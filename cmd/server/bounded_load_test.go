package main

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
	"github.com/OKI-Mesh/CoreScope/internal/database/pgtest"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

func loadStoreFromDB(t *testing.T, db *database.DB, maxMemMB int) *PacketStore {
	ctx := t.Context()
	t.Helper()

	store := NewPacketStore(db, &PacketStoreConfig{MaxMemoryMB: maxMemMB})
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	return store
}

func TestBoundedLoad_LimitedMemory(t *testing.T) {
	db := setupTestDB(t)
	t.Cleanup(func() { db.Close() })
	dbtest.SeedTransmissions(t, db, 5000)

	// Use 1MB budget — should load far fewer than 5000 packets
	store := loadStoreFromDB(t, db, 1)

	loaded := len(store.packets)
	if loaded >= 5000 {
		t.Errorf("expected bounded load to limit packets, got %d/5000", loaded)
	}
	if loaded < 1000 {
		t.Errorf("expected at least 1000 packets (minimum), got %d", loaded)
	}
	t.Logf("Loaded %d/5000 packets with 1MB budget", loaded)
}

func TestBoundedLoad_NewestFirst(t *testing.T) {
	db := setupTestDB(t)
	t.Cleanup(func() { db.Close() })
	dbtest.SeedTransmissions(t, db, 5000)

	// Use 1MB budget — should load far fewer than 5000 packets
	store := loadStoreFromDB(t, db, 1)

	loaded := len(store.packets)
	if loaded >= 5000 {
		t.Skip("all packets loaded, can't verify newest-first")
	}

	// The newest packet in DB has first_seen based on minute 5000.
	// The loaded packets should be the newest ones.
	// Last packet in store (sorted ASC) should be the newest in DB.
	last := store.packets[loaded-1]
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	newestExpected := base.Add(5000 * time.Minute)
	if !last.FirstSeen.Equal(newestExpected) {
		t.Errorf("expected last packet to be newest (%s), got %s", newestExpected, last.FirstSeen)
	}

	// First packet should NOT be the oldest in the DB (minute 1)
	first := store.packets[0]
	oldestAll := base.Add(1 * time.Minute)
	if first.FirstSeen.Equal(oldestAll) {
		t.Errorf("first loaded packet should not be the absolute oldest when bounded")
	}
}

func TestBoundedLoad_OldestLoadedSet(t *testing.T) {
	db := setupTestDB(t)
	t.Cleanup(func() { db.Close() })
	dbtest.SeedTransmissions(t, db, 5000)

	store := loadStoreFromDB(t, db, 1)

	if store.oldestLoaded.IsZero() {
		t.Fatal("oldestLoaded should be set after bounded load")
	}
	if len(store.packets) > 0 && store.oldestLoaded != store.packets[0].FirstSeen {
		t.Errorf("oldestLoaded (%s) should match first packet (%s)", store.oldestLoaded, store.packets[0].FirstSeen)
	}
	t.Logf("oldestLoaded = %s", store.oldestLoaded)
}

func TestBoundedLoad_UnlimitedWithZero(t *testing.T) {
	db := setupTestDB(t)
	t.Cleanup(func() { db.Close() })
	dbtest.SeedTransmissions(t, db, 200)

	store := loadStoreFromDB(t, db, 0)

	if len(store.packets) != 200 {
		t.Errorf("expected all 200 packets with maxMemoryMB=0, got %d", len(store.packets))
	}
}

func TestBoundedLoad_AscendingOrder(t *testing.T) {
	db := setupTestDB(t)
	t.Cleanup(func() { db.Close() })
	dbtest.SeedTransmissions(t, db, 5000)

	store := loadStoreFromDB(t, db, 1)

	// Verify packets are in ascending first_seen order
	for i := 1; i < len(store.packets); i++ {
		if store.packets[i].FirstSeen.Before(store.packets[i-1].FirstSeen) {
			t.Fatalf("packets not in ascending order at index %d: %s < %s",
				i, store.packets[i].FirstSeen, store.packets[i-1].FirstSeen)
		}
	}
}

// loadStoreWithRetention creates a PacketStore with retentionHours set and loads it.
func loadStoreWithRetention(t *testing.T, db *database.DB, retentionHours float64) *PacketStore {
	ctx := t.Context()
	t.Helper()

	cfg := &PacketStoreConfig{RetentionHours: retentionHours}
	store := NewPacketStore(db, cfg)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	return store
}

// ensureObserver returns the id of the observer with the given pubkey,
// creating it if it does not exist yet.
func ensureObserver(tb testing.TB, db *database.DB, pubkey string) int64 {
	tb.Helper()
	var id int64
	err := db.SQL().QueryRowContext(tb.Context(), `
		INSERT INTO observers (pubkey, name, first_seen, last_seen)
		VALUES ($1, $1, now(), now())
		ON CONFLICT (pubkey) DO UPDATE SET pubkey = EXCLUDED.pubkey
		RETURNING id`, pubkey).Scan(&id)
	if err != nil {
		tb.Fatalf("ensure observer %s: %v", pubkey, err)
	}
	return id
}

func seedAgedPackets(tb testing.TB, db *database.DB, numRecent, numOld int) {
	tb.Helper()
	ctx := tb.Context()
	now := time.Now().UTC().Truncate(time.Second)

	observerIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        strings.Repeat("e3", 32),
		Name:      "Obs1",
		IATA:      "BRU",
		Meta:      nil,
		LastSeen:  now,
		IngestNow: now,
	})
	if err != nil {
		tb.Fatalf("seed observer: %v", err)
	}

	insert := func(hashPrefix, rawHex string, n int, start time.Time, step time.Duration) {
		tb.Helper()
		for i := 0; i < n; i++ {
			at := start.Add(time.Duration(i) * step)
			txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
				Hash:        fmt.Sprintf("%s%012x", hashPrefix, i),
				RawHex:      rawHex,
				RouteType:   types.RouteType(0),
				PayloadType: types.PayloadType(4),
				DecodedJSON: "{}",
			}, at)
			if err != nil {
				tb.Fatalf("insert %s transmission %d: %v", hashPrefix, i, err)
			}
			idx := observerIdx
			_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
				TransmissionID: txID,
				ObserverIdx:    &idx,
				SNR:            nil,
				RSSI:           nil,
				Timestamp:      at,
			}, nil, nil)
			if err != nil {
				tb.Fatalf("insert %s observation %d: %v", hashPrefix, i, err)
			}
			if !inserted {
				tb.Fatalf("expected observation to be inserted for %s/%d", hashPrefix, i)
			}
		}
	}
	insert("0d00", "aa", numOld, now.Add(-48*time.Hour), time.Second)
	insert("0e00", "bb", numRecent, now.Add(-30*time.Minute), time.Second)
}

func TestRetentionLoad_OnlyLoadsRecentPackets(t *testing.T) {
	db := setupTestDB(t)
	t.Cleanup(func() { db.Close() })
	seedAgedPackets(t, db, 50, 100)

	// retention = 2 hours — should load only the 50 recent packets, not the 100 old ones
	store := loadStoreWithRetention(t, db, 2)

	if len(store.packets) != 50 {
		t.Errorf("expected 50 recent packets, got %d (old packets should be excluded by retentionHours)", len(store.packets))
	}
}

func TestRetentionLoad_ZeroRetentionLoadsAll(t *testing.T) {

	db := setupTestDB(t)
	t.Cleanup(func() { db.Close() })
	seedAgedPackets(t, db, 50, 100)

	// retention = 0 (unlimited) — should load all 150 packets
	store := loadStoreWithRetention(t, db, 0)

	if len(store.packets) != 150 {
		t.Errorf("expected all 150 packets with retentionHours=0, got %d", len(store.packets))
	}
}

func TestEstimateStoreTxBytesTypical(t *testing.T) {
	est := estimateStoreTxBytesTypical(10)
	if est < 1000 {
		t.Errorf("typical estimate too low: %d", est)
	}
	// Should be roughly proportional to observation count
	est1 := estimateStoreTxBytesTypical(1)
	est20 := estimateStoreTxBytesTypical(20)
	if est20 <= est1 {
		t.Errorf("estimate should grow with observations: 1obs=%d, 20obs=%d", est1, est20)
	}
	t.Logf("Typical estimate: 1obs=%d, 10obs=%d, 20obs=%d bytes", est1, est, est20)
}

func BenchmarkLoad_Bounded(b *testing.B) {
	ctx := b.Context()

	db := setupTestDB(b)
	dbtest.SeedTransmissions(b, db, 5000)

	
	for b.Loop() {
		b.StopTimer()
		store := NewPacketStore(db, &PacketStoreConfig{MaxMemoryMB: 1})
		b.StartTimer()

		if err := store.LoadChunked(ctx, 1000); err != nil {
			b.Fatalf("LoadChunked(): %v", err)
		}
	}
}

func BenchmarkLoad_Unlimited(b *testing.B) {
	ctx := b.Context()

	db := setupTestDB(b)
	dbtest.SeedTransmissions(b, db, 5000)

	
	for b.Loop() {
		b.StopTimer()
		store := NewPacketStore(db, &PacketStoreConfig{MaxMemoryMB: 0})
		b.StartTimer()

		if err := store.LoadChunked(ctx, 1000); err != nil {
			b.Fatalf("LoadChunked(): %v", err)
		}
	}
}

// BenchmarkLoad_30K_Bounded benchmarks bounded Load() with 30K transmissions
// and realistic observation counts (1–5 per transmission).
func BenchmarkLoad_30K_Bounded(b *testing.B) {
	ctx := b.Context()

	db := setupTestDB(b)
	dbtest.SeedTransmissions(b, db, 30000)

	
	for b.Loop() {
		b.StopTimer()
		store := NewPacketStore(db, &PacketStoreConfig{MaxMemoryMB: 1})
		b.StartTimer()

		if err := store.LoadChunked(ctx, 1000); err != nil {
			b.Fatalf("LoadChunked(): %v", err)
		}
	}
}

// BenchmarkLoad_30K_Unlimited benchmarks unlimited Load() with 30K transmissions
// and realistic observation counts (1–5 per transmission).
func BenchmarkLoad_30K_Unlimited(b *testing.B) {
	ctx := b.Context()

	dsn := createTestDBWithObs(b, 30000)

	
	for b.Loop() {
		b.StopTimer()
		db, err := OpenDB(b.Context(), dsn)
		if err != nil {
			b.Fatalf("OpenDB: %v", err)
		}
		store := NewPacketStore(db, &PacketStoreConfig{MaxMemoryMB: 0})
		b.StartTimer()

		if err := store.LoadChunked(ctx, 1000); err != nil {
			b.Fatalf("LoadChunked(): %v", err)
		}

		b.StopTimer()
		db.Close()
		b.StartTimer()
	}
}

// createTestDBWithObs returns the connection string of a fresh migrated
// database with numTx transmissions and realistic observation counts (1-5 per
// transmission, from distinct observers so the dedup index is satisfied), two
// hops per observation. Timestamps ascend with i and end at "now", so the
// fixture does not age out of any retention window.
func createTestDBWithObs(tb testing.TB, numTx int) string {
	tb.Helper()
	ctx := tb.Context()
	dsn := pgtest.NewDatabase(tb)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		tb.Fatal(err)
	}
	conn := stdlib.OpenDBFromPool(pool)
	defer func() { conn.Close(); pool.Close() }()

	now := time.Now().UTC().Truncate(time.Second)
	if _, err := conn.ExecContext(ctx, `
		INSERT INTO observers (pubkey, name, first_seen, last_seen) VALUES
			('obs1', 'Alpha',   $1, $1),
			('obs2', 'Bravo',   $1, $1),
			('obs3', 'Charlie', $1, $1),
			('obs4', 'Delta',   $1, $1),
			('obs5', 'Echo',    $1, $1)`, now); err != nil {
		tb.Fatalf("seed observers: %v", err)
	}

	if _, err := conn.ExecContext(ctx, `
		WITH t AS (
			INSERT INTO transmissions
				(raw_hex, hash, first_seen, route_type, payload_type, payload_version, decoded_json, last_seen)
			SELECT 'aabb', 'h' || lpad(i::text, 6, '0'),
			       $1::timestamptz - ($2::int - i) * interval '1 minute', 0, 4, 1,
			       '{"pubKey":"pk' || lpad(i::text, 6, '0') || '"}',
			       $1::timestamptz - ($2::int - i) * interval '1 minute'
			FROM generate_series(1, $2::int) AS i
			RETURNING id, first_seen
		), o AS (
			INSERT INTO observations
				(transmission_id, observer_idx, direction, snr, rssi, score, timestamp)
			SELECT t.id, ob.id, 'RX',
			       -5.0 + j * 2.5,
			       -90.0 + j * 5.0,
			       5 - j,
			       t.first_seen
			FROM t
			CROSS JOIN LATERAL generate_series(0, (t.id % 5)::int) AS j   -- 1 to 5 observations
			JOIN observers ob ON ob.pubkey = 'obs' || (j + 1)
			RETURNING id
		)
		INSERT INTO observation_path_hops (observation_id, hop_index, prefix)
		SELECT o.id, h.idx, h.p
		FROM o CROSS JOIN (VALUES (0, 'aa'), (1, 'bb')) AS h(idx, p)`,
		now, numTx); err != nil {
		tb.Fatalf("seed %d transmissions: %v", numTx, err)
	}
	return dsn
}
