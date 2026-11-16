package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/gorilla/mux"
)

func loadStoreForPaths(t *testing.T, db *database.DB) *PacketStore {
	t.Helper()
	ctx := t.Context()
	store := newPacketStoreForTest(t, db)
	store.RefreshNodeCache(ctx)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	deadline := time.After(5 * time.Second)
	for !store.PathHopIndexReady() {
		select {
		case <-deadline:
			t.Fatal("path-hop index not ready within 5s")
		case <-time.After(10 * time.Millisecond):
		}
	}
	return store
}

// TestHandleNodePaths_PrefixCollisionExclusion verifies that paths through a node
// sharing a 2-char prefix with another node are not returned as false positives
// when they have no resolved_path data (issue #929).
//
// Setup:
//   - nodeA (target): pubkey starts with "7a", no GPS
//   - nodeB (other):  pubkey starts with "7a", has GPS → "7a" resolves to nodeB
//   - tx1: path ["7a"], resolved_path NULL → false positive candidate, must be excluded
//   - tx2: path ["7a"], resolved_path contains nodeA pubkey → SQL-confirmed, must be included
func TestHandleNodePaths_PrefixCollisionExclusion(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	recent := time.Now().UTC().Add(-1 * time.Hour)

	nodeAPK := "7acb1111aaaabbbb"
	nodeBPK := "7aff2222ccccdddd" // same "7a" prefix, has GPS so resolveHop("7a") picks B

	latA, lonA := 0.0, 0.0
	latB, lonB := 37.5, -122.0
	mustCreateTestNode(t, ctx, db, database.NodeInput{
		PublicKey: nodeAPK, Name: "NodeA", Role: "repeater", Lat: &latA, Lon: &lonA, RxTime: recent, IngestNow: recent,
	})
	mustCreateTestNode(t, ctx, db, database.NodeInput{
		PublicKey: nodeBPK, Name: "NodeB", Role: "repeater", Lat: &latB, Lon: &lonB, RxTime: recent, IngestNow: recent,
	})

	// tx1: no resolved_path — should be excluded by hop-level check
	tx1ID := mustCreateTestTransmission(t, ctx, db, testTransmission(
		"AA", "hash_fp", "", types.RouteFlood, types.PayloadADVERT, "",
	), recent)
	mustInsertUnobservedTestObservation(t, ctx, db, tx1ID, recent, 0, 0, []string{"7a"}, nil)

	// tx2: resolved_path confirms nodeA — must be included
	tx2ID := mustCreateTestTransmission(t, ctx, db, testTransmission(
		"BB", "hash_tp", "", types.RouteFlood, types.PayloadADVERT, "",
	), recent)
	mustInsertUnobservedTestObservation(t, ctx, db, tx2ID, recent, 0, 0,
		[]string{"7a"}, []*string{&nodeAPK})

	srv := NewServer(db, &Config{Port: 3000}, NewHub())
	srv.store = loadStoreForPaths(t, db)
	router := mux.NewRouter()
	srv.RegisterRoutes(router)
	srv.RegisterRoutes(router)

	req := httptest.NewRequest("GET", "/api/nodes/"+nodeAPK+"/paths", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp api.NodePathsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Only the SQL-confirmed path (tx2) should be present; tx1 (false positive) must be excluded.
	// tx1 and tx2 share the same raw path ["7a"] so they collapse into 1 unique path group.
	// If tx1 were included, TotalTransmissions would be 2.
	if resp.TotalPaths != 1 {
		t.Errorf("expected 1 path group, got %d", resp.TotalPaths)
	}
	if resp.TotalTransmissions != 1 {
		t.Errorf("expected 1 transmission (false positive tx1 excluded), got %d", resp.TotalTransmissions)
	}
}
