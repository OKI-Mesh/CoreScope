package main

import (
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
)

// TestNeighborPersist_LegacyEdgeInvariant (#1638 adv-#1): edges loaded from
// the persisted neighbor_edges snapshot have no per-hash-mode breakdown
// (the table stores only the flat Count). Loader MUST synthesize
// CountsByMode so the invariant sum(CountsByMode) == Count holds — all
// pre-existing observations land in bucket 0 (legacy/unknown, conservative
// weight in the JS confidence indicator).
func TestNeighborPersist_LegacyEdgeInvariant(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	// Canonical order: node_a <= node_b. Valid lowercase keys for the table's guards.
	nodeA := "aaaa000000000001"
	nodeB := "bbbb000000000002"

	// neighbor_edges may carry foreign keys to nodes (see the FK follow-up);
	// seeding both nodes keeps this test valid either way.
	now := time.Now().UTC()
	for _, n := range []database.NodeInput{
		{PublicKey: nodeA, Name: "A", Role: "repeater", RxTime: now, IngestNow: now},
		{PublicKey: nodeB, Name: "B", Role: "repeater", RxTime: now, IngestNow: now},
	} {
		if err := db.CreateOrUpdateNode(ctx, n); err != nil {
			t.Fatalf("seed %s: %v", n.Name, err)
		}
	}

	// A flat-count edge with no per-mode evidence: what a snapshot-only edge
	// looks like in the table.
	changed, err := db.UpsertNeighborEdges(ctx, []database.NeighborEdgeInput{
		{NodeA: nodeA, NodeB: nodeB, Hits: 7, LastSeen: now.Truncate(time.Millisecond)},
	})
	if err != nil {
		t.Fatalf("seed edge: %v", err)
	}
	if changed != 1 {
		t.Fatalf("UpsertNeighborEdges changed %d rows, want 1", changed)
	}

	g, err := loadNeighborEdgesFromDB(ctx, db)
	if err != nil {
		t.Fatalf("load neighbor graph: %v", err)
	}
	edges := g.AllEdges()
	if len(edges) != 1 {
		t.Fatalf("expected 1 edge, got %d", len(edges))
	}
	e := edges[0]
	if e.Count != 7 {
		t.Fatalf("expected Count=7, got %d", e.Count)
	}
	if e.CountsByMode == nil {
		t.Fatalf("expected CountsByMode synthesized for legacy edge, got nil")
	}
	// All flat-count observations must land in bucket 0 (legacy/unknown).
	if got := e.CountsByMode[0]; got != 7 {
		t.Errorf("CountsByMode[0] = %d, want 7 (all legacy count in bucket 0)", got)
	}
	// Buckets 1/2/3 must be empty: no real wire-mode evidence on a
	// snapshot-only edge.
	for _, m := range []int{1, 2, 3} {
		if got := e.CountsByMode[m]; got != 0 {
			t.Errorf("CountsByMode[%d] = %d, want 0", m, got)
		}
	}
	// Invariant: sum(CountsByMode) == Count.
	sum := 0
	for _, c := range e.CountsByMode {
		sum += c
	}
	if sum != e.Count {
		t.Errorf("invariant violated: sum(CountsByMode)=%d, Count=%d", sum, e.Count)
	}
}

// TestNeighborPersist_LegacyEdgeMergeOnReload covers the "row appears twice
// in the snapshot" path (loader's else-branch): subsequent counts must
// accumulate into bucket 0 too, preserving the invariant.
func TestNeighborPersist_RepeatedUpsertMergesOnReload(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	nodeA := "aaaa000000000001"
	nodeB := "bbbb000000000002"

	now := time.Now().UTC()
	for _, n := range []database.NodeInput{
		{PublicKey: nodeA, Name: "A", Role: "repeater", RxTime: now, IngestNow: now},
		{PublicKey: nodeB, Name: "B", Role: "repeater", RxTime: now, IngestNow: now},
	} {
		if err := db.CreateOrUpdateNode(ctx, n); err != nil {
			t.Fatalf("seed %s: %v", n.Name, err)
		}
	}

	// Two separate upserts for the same pair, as two builder ticks would do.
	ts := now.Truncate(time.Millisecond)
	for _, hits := range []int32{3, 4} {
		if _, err := db.UpsertNeighborEdges(ctx, []database.NeighborEdgeInput{
			{NodeA: nodeA, NodeB: nodeB, Hits: hits, LastSeen: ts},
		}); err != nil {
			t.Fatalf("upsert hits=%d: %v", hits, err)
		}
	}

	g, err := loadNeighborEdgesFromDB(ctx, db)
	if err != nil {
		t.Fatalf("load neighbor graph: %v", err)
	}
	edges := g.AllEdges()
	if len(edges) != 1 {
		t.Fatalf("expected 1 merged edge, got %d", len(edges))
	}
	e := edges[0]
	if e.Count != 7 {
		t.Fatalf("expected merged Count=7, got %d", e.Count)
	}
	if got := e.CountsByMode[0]; got != 7 {
		t.Errorf("CountsByMode[0] = %d, want 7 after merge", got)
	}
	sum := 0
	for _, c := range e.CountsByMode {
		sum += c
	}
	if sum != e.Count {
		t.Errorf("invariant violated after merge: sum(CountsByMode)=%d, Count=%d", sum, e.Count)
	}
}
