package main

import (
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/pgtest"
)

// TestNeighborGraphRecomputerLoadsSnapshot enforces #1287 Option 4:
// the server LOADS its in-memory neighbor graph from the SQLite
// snapshot the ingestor writes. After a write to neighbor_edges (here
// done synthetically), the recomputer's atomic-swap must reflect it.
func TestNeighborGraphRecomputerLoadsSnapshot(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	nodeA := "aaaa000000000001"
	nodeB := "bbbb000000000002"
	nodeC := "cccc000000000003"
	nodeD := "dddd000000000004"

	now := time.Now().UTC()
	for _, n := range []database.NodeInput{
		{PublicKey: nodeA, Name: "A", Role: "repeater", RxTime: now, IngestNow: now},
		{PublicKey: nodeB, Name: "B", Role: "repeater", RxTime: now, IngestNow: now},
		{PublicKey: nodeC, Name: "C", Role: "repeater", RxTime: now, IngestNow: now},
		{PublicKey: nodeD, Name: "D", Role: "repeater", RxTime: now, IngestNow: now},
	} {
		if err := db.CreateOrUpdateNode(ctx, n); err != nil {
			t.Fatalf("seed %s: %v", n.Name, err)
		}
	}

	ts := now.Truncate(time.Millisecond)

	// Stage one edge.
	if _, err := db.UpsertNeighborEdges(ctx, []database.NeighborEdgeInput{
		{NodeA: nodeA, NodeB: nodeB, Hits: 5, LastSeen: ts},
	}); err != nil {
		t.Fatalf("seed first edge: %v", err)
	}

	// The server refreshes via the recomputer.
	store := &PacketStore{db: db}
	store.graph.Store(NewNeighborGraph())

	store.refreshNeighborGraphFromSnapshot(ctx)
	g := store.graph.Load()
	if g == nil {
		t.Fatal("graph nil after refresh")
	}
	if got := len(g.AllEdges()); got != 1 {
		t.Fatalf("expected 1 edge after first refresh, got %d", got)
	}

	// Add another row, refresh, assert the new total.
	if _, err := db.UpsertNeighborEdges(ctx, []database.NeighborEdgeInput{
		{NodeA: nodeC, NodeB: nodeD, Hits: 2, LastSeen: ts},
	}); err != nil {
		t.Fatalf("seed second edge: %v", err)
	}
	store.refreshNeighborGraphFromSnapshot(ctx)
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
	ctx := t.Context()
	dsn := pgtest.NewEmptyDatabase(t)

	_, err := OpenDB(ctx, dsn)
	if err == nil {
		t.Fatal("expected OpenDB to fail against an unmigrated DB; server would have started against an incomplete schema")
	}
	if !strings.Contains(err.Error(), "baseline") && !strings.Contains(err.Error(), "ready") {
		t.Errorf("expected error to indicate schema/readiness problem, got: %v", err)
	}
}
