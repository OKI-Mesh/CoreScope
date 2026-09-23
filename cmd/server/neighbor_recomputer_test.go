package main

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	_ "modernc.org/sqlite"
)

// TestNeighborGraphRecomputerLoadsSnapshot enforces #1287 Option 4:
// the server LOADS its in-memory neighbor graph from the SQLite
// snapshot the ingestor writes. After a write to neighbor_edges (here
// done synthetically), the recomputer's atomic-swap must reflect it.
func TestNeighborGraphRecomputerLoadsSnapshot(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "neighbor_recomp.db")

	// Bring the database fully under goose's control before inserting
	// data — OpenDB now asserts readiness via database.AssertReady.
	// neighbor_edges already exists via migration 26 — no manual
	// CREATE TABLE needed.
	migrateConn, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.BaselineStampMigrate(migrateConn, nil); err != nil {
		t.Fatalf("migrating test db: %v", err)
	}
	migrateConn.Close()

	rw, err := sql.Open("sqlite", "file:"+dbPath+"?_journal_mode=WAL")
	if err != nil {
		t.Fatal(err)
	}
	defer rw.Close()

	// Stage one edge.
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := rw.Exec(
		`INSERT INTO neighbor_edges (node_a, node_b, count, last_seen) VALUES (?, ?, ?, ?)`,
		"aaa", "bbb", 5, now,
	); err != nil {
		t.Fatal(err)
	}

	// Server opens read-only and refreshes via the recomputer.
	d, err := OpenDB(dbPath)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer d.conn.Close()
	store := &PacketStore{db: d}
	store.graph.Store(NewNeighborGraph())

	store.refreshNeighborGraphFromSnapshot()
	g := store.graph.Load()
	if g == nil {
		t.Fatal("graph nil after refresh")
	}
	if got := len(g.AllEdges()); got != 1 {
		t.Fatalf("expected 1 edge after first refresh, got %d", got)
	}

	// Add another row, refresh, assert the new total.
	if _, err := rw.Exec(
		`INSERT INTO neighbor_edges (node_a, node_b, count, last_seen) VALUES (?, ?, ?, ?)`,
		"ccc", "ddd", 2, now,
	); err != nil {
		t.Fatal(err)
	}
	store.refreshNeighborGraphFromSnapshot()
	g = store.graph.Load()
	if got := len(g.AllEdges()); got != 2 {
		t.Fatalf("expected 2 edges after second refresh, got %d", got)
	}
}

// TestServerStartupRequiresMigratedSchema enforces #1287: the server
// MUST refuse to start if the ingestor hasn't run schema migrations.
// AssertReady on a DB missing the required columns returns an error
// listing every missing surface; main.go then calls log.Fatalf.
func TestServerStartupRequiresMigratedSchema(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "unmigrated.db")

	// Bootstrap with ONLY transmissions/observations (the things
	// server tries to read) but WITHOUT the full goose migration
	// chain applied — simulating a database that was never brought
	// under goose's control.
	rw, err := sql.Open("sqlite", "file:"+dbPath+"?_journal_mode=WAL")
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{
		`CREATE TABLE transmissions (id INTEGER PRIMARY KEY, hash TEXT, payload_type INTEGER)`,
		`CREATE TABLE observations (id INTEGER PRIMARY KEY, transmission_id INTEGER)`,
		`CREATE TABLE observers (id TEXT PRIMARY KEY, name TEXT)`,
		`CREATE TABLE nodes (public_key TEXT PRIMARY KEY)`,
		`CREATE TABLE inactive_nodes (public_key TEXT PRIMARY KEY)`,
	} {
		if _, err := rw.Exec(s); err != nil {
			rw.Close()
			t.Fatal(err)
		}
	}
	rw.Close()

	// OpenDB itself must refuse — it now asserts readiness internally
	// (via database.AssertBaselined/AssertReady) before ever returning
	// a usable handle. This is the actual startup-safety guarantee:
	// the server never gets far enough to read from an unmigrated or
	// partially-migrated database.
	_, err = OpenDB(dbPath)
	if err == nil {
		t.Fatal("expected OpenDB to fail against an unmigrated DB; server would have started against an incomplete schema")
	}
	if !strings.Contains(err.Error(), "baseline") && !strings.Contains(err.Error(), "ready") {
		t.Errorf("expected error to indicate schema/readiness problem, got: %v", err)
	}
}

// assertReadyForTest is the same call main.go makes — declared here so
// the test stays decoupled from any future inlining or rename.
func assertReadyForTest(d *DB) error {
	return dbschemaAssertReadyShim(d)
}

// dbschemaAssertReadyShim wraps the package import so tests don't
// directly depend on the import being present (production wires it
// via main.go).
func dbschemaAssertReadyShim(d *DB) error { return database.AssertReady(d.conn) }
