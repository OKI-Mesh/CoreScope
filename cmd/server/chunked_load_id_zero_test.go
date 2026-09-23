package main

// Regression for PR #1596 / issue #1486 e2e: LoadChunked uses
// `cursorID = 0` with a `t2.id > cursorID` predicate, which silently
// excludes any transmission with id=0. The e2e seed for #1486 inserts
// the grouped-packet row with id=0 (so it sorts LAST in the default
// packets view), and the page deep-links to /packets?hash=<seed>.
// With the chunked loader skipping id=0, the in-memory store never
// learns about the row; QueryGroupedPackets returns 0; the page
// renders no `tr[data-hash]` and the e2e times out at 12s.
//
// Legacy Load() walked all transmissions unconditionally (no id
// cursor) and therefore included id=0. Restoring that semantic — by
// using a non-existent sentinel (-1) on the first iteration, or by
// switching the predicate to `>=` for the initial pass — fixes the
// regression.
//
// This test inserts a transmission with id=0 plus a handful of
// id>=1 transmissions and asserts that LoadChunked loads the id=0
// row into s.byHash.

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
)

func createTestDBWithIDZero(tb testing.TB, dbPath string, extraTx int) {
	tb.Helper()

	// Bring the database fully under goose's control before inserting
	// data — OpenDB now asserts readiness via database.AssertReady.
	migrateConn, err := sql.Open("sqlite", dbPath)
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := database.BaselineStampMigrate(migrateConn, nil); err != nil {
		tb.Fatalf("migrating test db: %v", err)
	}
	migrateConn.Close()

	conn, err := sql.Open("sqlite", dbPath+"?_journal_mode=WAL")
	if err != nil {
		tb.Fatal(err)
	}
	defer conn.Close()

	txStmt, err := conn.Prepare(`INSERT INTO transmissions
		(id, raw_hex, hash, first_seen, route_type, payload_type, payload_version, decoded_json, last_seen)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		tb.Fatalf("prepare transmissions insert: %v", err)
	}
	obsStmt, err := conn.Prepare(`INSERT INTO observations
		(id, transmission_id, observer_idx, direction, snr, rssi, score, path_json, timestamp)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		tb.Fatalf("prepare observations insert: %v", err)
	}
	defer txStmt.Close()
	defer obsStmt.Close()

	now := time.Now().UTC().Truncate(time.Second)

	// id=0: the #1486-style seed row, within retention window.
	seedTime := now.Add(-1 * time.Minute)
	txStmt.Exec(0, "1500", "fae0c9e6d357a814", seedTime.Format(time.RFC3339), 1, 5, 0, `{"type":"CHAN"}`, seedTime.Unix())
	obsStmt.Exec(0, 0, 1, "rx", 5.0, -95.0, 0, `["AA"]`, seedTime.Unix())

	for i := 1; i <= extraTx; i++ {
		txTime := now.Add(-time.Duration(i+1) * time.Minute)
		ts := txTime.Format(time.RFC3339)
		unixTs := txTime.Unix()
		hash := fmt.Sprintf("h%04d", i)
		txStmt.Exec(i, "aabb", hash, ts, 0, 4, 1, fmt.Sprintf(`{"pubKey":"pk%04d"}`, i), unixTs)
		obsStmt.Exec(i, i, 1, "rx", -10.0, -80.0, 5, `["aa","bb"]`, unixTs)
	}
}

// TestLoadChunked_IncludesIDZero: LoadChunked must load transmissions
// with id=0. The legacy Load() (since-replaced by LoadChunked) walked
// transmissions unconditionally; LoadChunked uses an id-cursor that
// starts at 0 with a strict `t2.id > cursorID` predicate, so id=0
// rows are silently dropped. This breaks the #1486 e2e fixture seed
// which uses id=0 to sort the grouped row last in the default view.
func TestLoadChunked_IncludesIDZero(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "idzero.db")
	createTestDBWithIDZero(t, dbPath, 10)

	db, err := OpenDB(dbPath)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	cfg := &PacketStoreConfig{}
	store := NewPacketStore(db, cfg)
	defer store.db.conn.Close()

	if err := store.LoadChunked(5); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}

	if _, ok := store.byHash["fae0c9e6d357a814"]; !ok {
		t.Fatalf("LoadChunked dropped the id=0 transmission: "+
			"byHash[fae0c9e6d357a814] missing; loaded %d packets total "+
			"(id-cursor starts at 0 with strict `t2.id > cursorID`, "+
			"so id=0 is excluded — this is the #1486 e2e regression)",
			len(store.packets))
	}
}
