package main

import (
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/pgtest"
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
	ctx := t.Context()

	dsn := pgtest.NewDatabase(t)
	rw, err := database.OpenReadWrite(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}

	// neighbor_edges already exists via migration 26 — just seed a row so
	// the panic condition (rows exist, graph not loaded) is triggered.
	if _, err := rw.UpsertNeighborEdges(ctx, []database.NeighborEdgeInput{{
		NodeA: "aaa", NodeB: "bbb", Hits: 5, LastSeen: time.Now().UTC(),
	}}); err != nil {
		rw.Close()
		t.Fatalf("insert neighbor edge: %v", err)
	}
	rw.Close()

	d, err := OpenDB(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer d.Close()

	// Deliberately DO NOT call store.graph.Store(...). s.graph.Load() returns
	// nil → the bug condition the invariant guard must catch.
	store := NewPacketStore(d, &PacketStoreConfig{RetentionHours: 72})

	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("Load() must panic when neighbor_edges has rows but graph is nil; got no panic")
		}
	}()
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
}
