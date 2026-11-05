package main

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/gorilla/mux"
)

// TestFirstSeen_1166_HandleNodesSurface pins issue #1166: the /api/nodes
// response carries a `first_seen` ISO timestamp per node so the frontend
// can show a sortable "First Seen" column.
func TestFirstSeen_1166_HandleNodesSurface(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	pk := "cccc000000000000000000000000000000000000000000000000000000000000"
	now := time.Now().UTC().Truncate(time.Millisecond)
	firstSeen := now.Add(-72 * time.Hour)
	first := firstSeen.Format("2006-01-02T15:04:05.000Z")
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

	req := httptest.NewRequest("GET", "/api/nodes?limit=10", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("/api/nodes status: want 200, got %d body=%s", rr.Code, rr.Body.String())
	}

	var resp struct {
		Nodes []map[string]interface{} `json:"nodes"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v body=%s", err, rr.Body.String())
	}
	var got map[string]interface{}
	for _, n := range resp.Nodes {
		if k, _ := n["public_key"].(string); k == pk {
			got = n
			break
		}
	}
	if got == nil {
		t.Fatalf("node missing from /api/nodes response")
	}
	fs, hasFS := got["first_seen"]
	if !hasFS {
		t.Fatalf("first_seen absent from /api/nodes response (issue #1166)")
	}
	s, _ := fs.(string)
	if s == "" {
		t.Errorf("first_seen empty, want ISO timestamp, got %v", fs)
	}
	if s != first {
		t.Errorf("first_seen = %q, want %q", s, first)
	}
}
