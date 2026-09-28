package main

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	_ "modernc.org/sqlite"
)

// TestLoad_PanicsWhenGraphNotLoadedAndEdgesExist pins the startup-ordering
// invariant (munger R1 #2). Graph-load-before-packet-load is the entire
// premise of PR #1643's fix: without an in-memory neighbor graph, the
// path_json relay-hop fallback cannot resolve hops, so relay-node analytics
// history collapses. main.go currently does the right thing — but nothing
// asserts the ordering, so a future refactor could silently regress.
//
// Load() must panic when neighbor_edges has rows but s.graph.Load() returns
// nil. Fast-fail at startup beats silently-wrong attribution.
func TestLoad_PanicsWhenGraphNotLoadedAndEdgesExist(t *testing.T) {
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

	rw, err := sql.Open("sqlite", "file:"+dbPath+"?_journal_mode=WAL")
	if err != nil {
		t.Fatal(err)
	}
	defer rw.Close()

	exec := func(s string, args ...interface{}) {
		if _, err := rw.Exec(s, args...); err != nil {
			t.Fatalf("setup exec failed: %v\nSQL: %s", err, s)
		}
	}

	// neighbor_edges already exists via migration 26 — just seed a row so
	// the panic condition (rows exist, graph not loaded) is triggered.
	now := time.Now().UTC().Format(time.RFC3339)
	exec(`INSERT INTO neighbor_edges (node_a, node_b, count, last_seen) VALUES (?, ?, ?, ?)`,
		"aaa", "bbb", 5, now)

	d, err := OpenDB(dbPath)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer d.conn.Close()

	// Deliberately DO NOT call store.graph.Store(...). s.graph.Load() returns
	// nil → the bug condition the invariant guard must catch.
	store := NewPacketStore(d, &PacketStoreConfig{RetentionHours: 72})

	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("Load() must panic when neighbor_edges has rows but graph is nil; got no panic")
		}
	}()
	_ = store.Load()
}
