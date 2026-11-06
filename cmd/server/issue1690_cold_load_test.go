package main

// Tests for issue #1690 — cold-load uses wrong time axis (first_seen instead
// of effective recency). Three tests live in this file:
//
//   Test1690_ColdLoad_TimeAxis  — long-lived transmissions (first_seen 30d
//                                  ago) with recent observations must load
//                                  under a 1h hotStartupHours window.
//   Test1690_BackgroundLoadHonesty — backgroundLoadComplete must NOT flip to
//                                     true when coverage is below threshold.
//   Test1690_PerfStats_NewFields — typed perf response must expose
//                                   retentionHours, oldestLoaded,
//                                   loadCoverageRatio.

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/pgtest"
)

// createTestDBWithLastSeen seeds rows with distinct first_seen and last_seen
// timestamps. Bulk SQL keeps the 5,000-row memory-budget test practical.
func createTestDBWithLastSeen(t *testing.T, numTx, obsPerTx int, nowSec int64, firstSeenAgo, lastSeenAgo time.Duration) string {
	t.Helper()
	dsn := pgtest.NewDatabase(t)
	ctx := t.Context()
	db, err := database.OpenReadWrite(ctx, dsn)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	defer db.Close()

	now := time.Unix(nowSec, 0).UTC()
	firstSeen := now.Add(-firstSeenAgo)
	lastSeen := now.Add(-lastSeenAgo)
	observerID := mustCreateTestObserver(t, ctx, db, "obs1", "Obs1", "", now)
	if _, err := db.SQL().ExecContext(ctx, `
		INSERT INTO transmissions
			(raw_hex, hash, first_seen, route_type, payload_type, payload_version, decoded_json, last_seen)
		SELECT 'aabb', 'cold' || lpad(i::text, 8, '0'), $1, 0, 4, 1, '{}', $2
		FROM generate_series(1, $3::int) AS i`, firstSeen, lastSeen, numTx); err != nil {
		t.Fatalf("seed transmissions: %v", err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
		INSERT INTO observations
			(transmission_id, observer_idx, direction, snr, rssi, score, timestamp)
		SELECT tx.id, $1, 'RX', -10.0, -80.0, 5,
		       tx.last_seen - (obs.n * interval '1 minute')
		FROM transmissions tx
		CROSS JOIN generate_series(0, $2::int - 1) AS obs(n)
		WHERE tx.hash LIKE 'cold%'`, observerID, obsPerTx); err != nil {
		t.Fatalf("seed observations: %v", err)
	}
	return dsn
}

// Test1690_ColdLoad_TimeAxis seeds 1000 transmissions whose hash *first
// appeared* 30 days ago but whose last observation was 30 minutes ago.
// With a 1h hotStartupHours, the pre-fix code (filtering on first_seen)
// loads zero rows; the post-fix code (filtering on last_seen) must load
// all 1000.
func Test1690_ColdLoad_TimeAxis(t *testing.T) {
	ctx := t.Context()

	nowSec := time.Now().UTC().Unix()
	dbPath := createTestDBWithLastSeen(t, 1000, 1, nowSec,
		30*24*time.Hour, // first_seen = 30d ago
		30*time.Minute)  // last_seen = 30min ago

	db, err := OpenDB(ctx, dbPath)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	store := NewPacketStore(db, &PacketStoreConfig{
		RetentionHours:  168,
		HotStartupHours: 1,
	})

	if err := store.LoadChunked(ctx, 0); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}

	loaded := len(store.packets)
	if loaded < 1000 {
		t.Fatalf("Test1690_ColdLoad_TimeAxis: expected ≥1000 transmissions loaded "+
			"(all 1000 fixture rows have last_seen within 1h), got %d. "+
			"Pre-fix behavior: chunked_load.go filters t.first_seen >= now-1h "+
			"which excludes all 30d-old rows.", loaded)
	}
}

// Test1690_BackgroundLoadHonesty seeds 1000 transmissions but caps the
// store's memory budget so it can only fit a fraction. After
// loadBackgroundChunks runs, backgroundLoadDone must be FALSE and
// backgroundLoadFailed must be TRUE because actual coverage is < 90%.
func Test1690_BackgroundLoadHonesty(t *testing.T) {
	ctx := t.Context()

	nowSec := time.Now().UTC().Unix()
	// 5000 rows; chunkSize=500 + maxMemoryMB=1 (→ maxPackets ≈ 1000) so
	// the load breaks at the end of the chunk that crosses the cap and
	// totalLoaded ≪ 5000.
	dbPath := createTestDBWithLastSeen(t, 5000, 1, nowSec,
		30*time.Minute, 30*time.Minute)

	db, err := OpenDB(ctx, dbPath)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	store := NewPacketStore(db, &PacketStoreConfig{
		RetentionHours:  168,
		HotStartupHours: 1,
		MaxMemoryMB:     1, // forces bounded load ≪ 5000 rows
	})
	if err := store.LoadChunked(ctx, 500); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}
	store.loadBackgroundChunks(ctx)

	if store.backgroundLoadDone.Load() {
		t.Errorf("backgroundLoadDone=true with only %d/5000 packets loaded; "+
			"must be false until coverage ≥ 90%%", len(store.packets))
	}
	if !store.backgroundLoadFailed.Load() {
		t.Errorf("backgroundLoadFailed=false despite under-coverage "+
			"(%d/5000 packets loaded); must be true with a reason", len(store.packets))
	}
	// The error message must mention a percentage so operators can see
	// the actual ratio surface in the perf endpoint.
	errMsg := store.BackgroundLoadError()
	if !strings.Contains(errMsg, "%") {
		t.Errorf("backgroundLoadError=%q; expected human-readable ratio "+
			"(e.g. 'loaded X%% of Y rows')", errMsg)
	}
}

// Test1690_PerfStats_NewFields asserts the typed perf payload exposes the
// retention/coverage fields needed for prod observability.
func Test1690_PerfStats_NewFields(t *testing.T) {
	ctx := t.Context()

	nowSec := time.Now().UTC().Unix()
	dbPath := createTestDBWithLastSeen(t, 10, 1, nowSec,
		30*time.Minute, 30*time.Minute)

	db, err := OpenDB(ctx, dbPath)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	store := NewPacketStore(db, &PacketStoreConfig{
		RetentionHours:  168,
		HotStartupHours: 1,
	})
	if err := store.LoadChunked(ctx, 0); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}

	ps := store.GetPerfStoreStatsTyped()
	buf, err := json.Marshal(ps)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var asMap map[string]interface{}
	if err := json.Unmarshal(buf, &asMap); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{"retentionHours", "oldestLoaded", "loadCoverageRatio"} {
		if _, ok := asMap[key]; !ok {
			t.Errorf("PerfPacketStoreStats missing %q field; payload=%s", key, string(buf))
		}
	}
}
