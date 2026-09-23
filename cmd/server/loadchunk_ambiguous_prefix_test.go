package main

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	_ "modernc.org/sqlite"
)

// createTestDBAmbiguousPrefix builds a fixture where TWO repeaters share the
// same 2-char hop prefix. An observation's path_json carries ONLY the
// ambiguous prefix (no longer prefix that would disambiguate). With no
// neighbor_edges seeded, the cold-load fallback in scanAndMergeChunk has
// nothing to anchor on — yet the current code resolves the prefix anyway
// (via observation_count_fallback or candidate[0]) and over-attributes the
// hop to ONE of the two repeaters. That is the time-travel bug munger
// flagged: the historical packet's actual relay is unknown, but the loader
// picks today's tier-4 winner against ~7-day-old observations.
func createTestDBAmbiguousPrefix(t *testing.T, relayA, relayB, hop, firstSeen string) string {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Bring the database fully under goose's control before inserting
	// data — OpenDB now asserts readiness via database.AssertReady.
	migrateConn, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.BaselineStampMigrate(migrateConn, nil); err != nil {
		t.Fatalf("migrating test db: %v", err)
	}
	migrateConn.Close()

	conn, err := sql.Open("sqlite", dbPath+"?_journal_mode=WAL")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	exec := func(s string, args ...interface{}) {
		if _, err := conn.Exec(s, args...); err != nil {
			t.Fatalf("setup exec failed: %v\nSQL: %s", err, s)
		}
	}

	// Real observers row so the LEFT JOIN observers ON obs.rowid =
	// o.observer_idx resolves.
	res, err := conn.Exec(`INSERT INTO observers (id, name) VALUES ('obs1', 'Obs1')`)
	if err != nil {
		t.Fatalf("insert observer: %v", err)
	}
	observerRowID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("observer rowid: %v", err)
	}

	// Two repeaters sharing the same 2-char prefix `hop`.
	// Different advert_counts so tier-4 tiebreak deterministically picks one
	// (proving the bug: it over-attributes to the higher-count node).
	exec(`INSERT INTO nodes (public_key, name, role, advert_count) VALUES (?,?,?,?)`,
		relayA, "Relay A", "repeater", 50)
	exec(`INSERT INTO nodes (public_key, name, role, advert_count) VALUES (?,?,?,?)`,
		relayB, "Relay B", "repeater", 10)

	// Aged 48h so it lands in the background window (loadChunk path).
	firstSeenTime, err := time.Parse(time.RFC3339, firstSeen)
	if err != nil {
		t.Fatalf("parsing firstSeen %q: %v", firstSeen, err)
	}
	lastSeenUnix := firstSeenTime.Unix()

	exec(`INSERT INTO transmissions
		(id, raw_hex, hash, first_seen, route_type, payload_type, payload_version, decoded_json, last_seen)
		VALUES (?,?,?,?,0,4,1,?,?)`,
		1, "aa", "hashamb_1", firstSeen, `{}`, lastSeenUnix)
	exec(`INSERT INTO observations
		(id, transmission_id, observer_idx, direction, snr, rssi, score, path_json, timestamp, resolved_path)
		VALUES (?,?,?,?,?,?,?,?,?,NULL)`,
		1, 1, observerRowID, "RX", -10.0, -80.0, 5, fmt.Sprintf(`[%q]`, hop), lastSeenUnix)

	return dbPath
}

// TestLoadChunk_AmbiguousPrefix_SkipsAttribution pins the fix for the
// time-travel attribution gate (munger R1 #1). When path_json carries an
// ambiguous prefix that matches multiple repeaters, the cold-load path
// MUST NOT pick a winner via affinity/observation-count tiebreak — today's
// affinity winner is not necessarily the historical hop. Safer to
// under-attribute (skip byNode for that hop) than to mis-attribute.
func TestLoadChunk_AmbiguousPrefix_SkipsAttribution(t *testing.T) {
	relayA := "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
	relayB := "aa1122334455667788990011223344556677889900112233445566778899aabb"
	hop := "aa" // 2-char prefix shared by both relayA and relayB

	aged := time.Now().UTC().Add(-48 * time.Hour).Format(time.RFC3339)
	dbPath := createTestDBAmbiguousPrefix(t, relayA, relayB, hop, aged)

	db, err := OpenDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.conn.Close()

	store := NewPacketStore(db, &PacketStoreConfig{
		RetentionHours:  72,
		HotStartupHours: 1, // hot load skips the 48h-old row → goes to loadChunk
	})
	// Empty graph: no neighbor-affinity tiebreak signal. Mirrors a freshly
	// restarted server whose only relay info is the prefix map.
	store.graph.Store(NewNeighborGraph())

	if err := store.LoadChunked(0); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}
	if got := len(store.byNode[relayA]) + len(store.byNode[relayB]); got != 0 {
		t.Fatalf("setup: hot load unexpectedly picked up 48h-old row "+
			"(byNode total=%d, want 0) — test would not exercise loadChunk", got)
	}

	chunkStart := time.Now().UTC().Add(-72 * time.Hour)
	chunkEnd := time.Now().UTC().Add(-1 * time.Hour)
	if err := store.loadChunk(chunkStart, chunkEnd); err != nil {
		t.Fatalf("loadChunk: %v", err)
	}

	// Neither repeater may be over-attributed. The hop is ambiguous → the
	// cold-load loader MUST NOT pick one as the byNode owner.
	if got := len(store.byNode[relayA]); got != 0 {
		t.Errorf("byNode[%s]: got %d transmissions, want 0 — ambiguous-prefix hop "+
			"was over-attributed to relayA (time-travel attribution bug)", relayA, got)
	}
	if got := len(store.byNode[relayB]); got != 0 {
		t.Errorf("byNode[%s]: got %d transmissions, want 0 — ambiguous-prefix hop "+
			"was over-attributed to relayB (time-travel attribution bug)", relayB, got)
	}
}

// TestLoad_AmbiguousPrefix_SkipsAttribution covers the hot-window Load()
// path. Same setup as the loadChunk test but the row falls inside the hot
// window so it is loaded by Load() / scanAndMergeChunk.
func TestLoad_AmbiguousPrefix_SkipsAttribution(t *testing.T) {
	relayA := "bbccddeeff00112233445566778899aabbccddeeff00112233445566778899aa"
	relayB := "bb112233445566778899001122334455667788990011223344556677889900aa"
	hop := "bb"

	ts := time.Now().UTC().Format(time.RFC3339)
	dbPath := createTestDBAmbiguousPrefix(t, relayA, relayB, hop, ts)

	db, err := OpenDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.conn.Close()

	store := NewPacketStore(db, &PacketStoreConfig{RetentionHours: 72})
	store.graph.Store(NewNeighborGraph())

	if err := store.LoadChunked(0); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}

	if got := len(store.byNode[relayA]); got != 0 {
		t.Errorf("byNode[%s]: got %d transmissions, want 0 — ambiguous-prefix hop "+
			"was over-attributed (hot Load path)", relayA, got)
	}
	if got := len(store.byNode[relayB]); got != 0 {
		t.Errorf("byNode[%s]: got %d transmissions, want 0 — ambiguous-prefix hop "+
			"was over-attributed (hot Load path)", relayB, got)
	}
}
