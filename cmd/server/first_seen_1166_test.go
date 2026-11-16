package main

import (
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/gorilla/mux"
)

// TestFirstSeen_1166_HandleNodesSurface pins issue #1166: the /api/nodes
// response carries a `first_seen` ISO timestamp per node so the frontend
// can show a sortable "First Seen" column.
// TestFirstSeen_1166_HandleNodesSurface pins issue #1166: the /api/nodes
// response carries a `first_seen` ISO timestamp per node so the frontend
// can show a sortable "First Seen" column.
func TestFirstSeen_1166_HandleNodesSurface(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	pk := "cccc000000000000000000000000000000000000000000000000000000000000"
	now := time.Now().UTC().Truncate(time.Millisecond)
	firstSeen := now.Add(-72 * time.Hour)
	if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: pk,
		Name:      "rpt",
		Role:      types.RoleRepeater,
		RxTime:    firstSeen,
		IngestNow: now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TouchNodeLastSeen(ctx, pk, now); err != nil {
		t.Fatal(err)
	}
	if err := db.IncrementNodeAdvertCount(ctx, pk); err != nil {
		t.Fatal(err)
	}

	store := newPacketStoreForTest(t, db)
	cfg := &Config{Port: 3000}
	hub := NewHub()
	srv := NewServer(db, cfg, hub)
	srv.store = store

	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	nodes := nodesByPubkey(t, router, "?limit=10")
	node, ok := nodes[pk]
	if !ok {
		t.Fatalf("node missing from /api/nodes response")
	}
	if node.FirstSeen == nil {
		t.Fatalf("first_seen absent from /api/nodes response (issue #1166)")
	}
	if want := firstSeen.Truncate(time.Second); !node.FirstSeen.Equal(want) {
		t.Errorf("first_seen = %s, want %s (whole seconds on the wire)", node.FirstSeen, want)
	}
}
