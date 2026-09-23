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

// createTestDBWithResolvedPath creates a fixture DB containing numTx old
// transmissions (48h ago, outside any default hot window) where each
// observation has a non-empty resolved_path JSON listing relay-hop pubkeys.
// Mirrors createTestDBWithAgedPackets shape but adds the resolved_path
// column so loadChunk's hasResolvedPath branch is exercised.
func createTestDBWithResolvedPath(t *testing.T, numTx int, relayPubkeys []string) string {
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

	// Build resolved_path JSON array of pubkey strings: ["pk1","pk2",...]
	rpJSON := "["
	for i, pk := range relayPubkeys {
		if i > 0 {
			rpJSON += ","
		}
		rpJSON += fmt.Sprintf("%q", pk)
	}
	rpJSON += "]"

	now := time.Now().UTC()
	for i := 0; i < numTx; i++ {
		txTime := now.Add(-48 * time.Hour).Add(time.Duration(i) * time.Second)
		ts := txTime.Format(time.RFC3339)
		tsUnix := txTime.Unix()
		hash := fmt.Sprintf("hash1558_%d", i)
		exec(`INSERT INTO transmissions
			(id, raw_hex, hash, first_seen, route_type, payload_type, payload_version, decoded_json, last_seen)
			VALUES (?,?,?,?,0,4,1,?,?)`,
			i+1, "aa", hash, ts, `{}`, tsUnix)
		exec(`INSERT INTO observations
			(id, transmission_id, observer_idx, direction, snr, rssi, score, path_json, timestamp, resolved_path)
			VALUES (?,?,?,?,?,?,?,?,?,?)`,
			i+1, i+1, observerRowID, "RX", -10.0, -80.0, 5, `[]`, tsUnix, rpJSON)
	}
	return dbPath
}

// TestLoadChunk_IndexesResolvedPathPubkeys_Issue1558 verifies the
// contract-violation fix from #1558:
//
//	`Load` (cmd/server/store.go:783-799) unmarshals each observation's
//	resolved_path column and feeds every relay-hop pubkey through
//	addToByNode / addResolvedPubkeysToPathHopIndex /
//	addToResolvedPubkeyIndex. `loadChunk` (cmd/server/store.go:937-1023)
//	scans the same column into resolvedPathStr but never feeds it
//	anywhere — so background-backfilled transmissions never appear under
//	their relay pubkeys in s.byNode, even though the same exact rows do
//	when they happen to fall inside the hot startup window.
//
// Symptom in production: Home page per-node `packetsToday` /
// `totalTransmissions` / observer counts collapse after a container
// restart for any node that primarily appears as a relay (rather than
// as the endpoint pubKey/destPubKey/srcPubKey of a packet), because the
// background backfill path silently drops the relay-hop indexing
// branch. See issue #1558 for the full trace + diagnosis.
//
// This test loads a fixture DB exclusively via loadChunk (skipping
// Load) and asserts that for each relay pubkey present in
// `resolved_path` of every observation, s.byNode contains the
// transmission.
func TestLoadChunk_IndexesResolvedPathPubkeys_Issue1558(t *testing.T) {
	// Two distinct relay pubkeys appear in every observation's resolved_path.
	// Neither is an endpoint pubkey in decoded_json — so the ONLY path
	// they can enter byNode through is the resolved_path branch.
	relayPK1 := "1111111111111111111111111111111111111111111111111111111111111111"
	relayPK2 := "2222222222222222222222222222222222222222222222222222222222222222"

	dbPath := createTestDBWithResolvedPath(t, 3, []string{relayPK1, relayPK2})

	db, err := OpenDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.conn.Close()

	if !db.hasResolvedPath {
		t.Fatalf("setup: fixture should expose resolved_path column; hasResolvedPath=false")
	}

	store := NewPacketStore(db, &PacketStoreConfig{
		RetentionHours:  72,
		HotStartupHours: 1, // initial Load should NOT pick up 48h-old fixture rows
	})
	if err := store.Load(); err != nil {
		t.Fatal(err)
	}
	// Confirm the fixture rows are outside the hot window — Load() must
	// not have already populated byNode for the relay pubkeys; otherwise
	// the test would not actually be exercising loadChunk.
	if len(store.byNode[relayPK1]) != 0 {
		t.Fatalf("setup: Load() unexpectedly picked up 48h-old rows; "+
			"byNode[relayPK1]=%d entries (expected 0)", len(store.byNode[relayPK1]))
	}

	// Trigger background backfill of the 48h-old window via loadChunk —
	// this is the code path under test.
	chunkStart := time.Now().UTC().Add(-72 * time.Hour)
	chunkEnd := time.Now().UTC().Add(-1 * time.Hour)
	if err := store.loadChunk(chunkStart, chunkEnd); err != nil {
		t.Fatalf("loadChunk failed: %v", err)
	}

	// Sanity: loadChunk did merge the transmissions into the slice.
	if len(store.packets) != 3 {
		t.Fatalf("loadChunk should have merged 3 transmissions; got %d", len(store.packets))
	}

	// THE ASSERTION: every relay pubkey listed in resolved_path must be
	// indexed in byNode for every transmission, because loadChunk's
	// per-row scan should mirror Load()'s 783-799 block.
	for _, relayPK := range []string{relayPK1, relayPK2} {
		got := len(store.byNode[relayPK])
		if got != 3 {
			t.Errorf("byNode[%s]: got %d transmissions, want 3 — "+
				"loadChunk dropped the resolved_path indexing branch "+
				"(issue #1558)",
				relayPK, got)
		}
	}
}
