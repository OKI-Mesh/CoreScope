package main

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
	"github.com/OKI-Mesh/CoreScope/internal/types"

	"github.com/gorilla/mux"
)

// TestRepro810 reproduces #810: when the longest-path observation has NULL
// resolved_path but a shorter-path observation has one, fetchResolvedPathForTxBest
// returns nil → /api/nodes/{pk}/health.recentPackets[].resolved_path is missing
// while /api/packets shows it.
func TestRepro810(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	now := time.Now().UTC()
	recent := now.Add(-1 * time.Hour)
	observer1 := mustCreateTestObserver(t, ctx, db, "obs1", "O1", "", recent)
	observer2 := mustCreateTestObserver(t, ctx, db, "obs2", "O2", "", recent)
	mustCreateTestNode(t, ctx, db, database.NodeInput{
		PublicKey: "aabbccdd11223344",
		Name:      "R",
		Role:      "repeater",
		RxTime:    recent,
		IngestNow: now,
	})
	mustCreateTestNode(t, ctx, db, database.NodeInput{
		PublicKey: "eeff00112233aabb",
		Name:      "Relay",
		Role:      "repeater",
		RxTime:    recent,
		IngestNow: now,
	})
	txID := mustCreateTestTransmission(t, ctx, db, testTransmission(
		"AABB", "testhash00000001", `{"pubKey":"aabbccdd11223344","type":"ADVERT"}`,
		types.RouteFlood, types.PayloadADVERT, "",
	), recent)
	// Longest-path obs WITHOUT resolved_path
	dbtest.Observation(t, db, txID, observer1, recent, 12.5, -90, []string{"aa", "bb", "cc"}...)
	// Shorter-path obs WITH resolved_path
	resolvedA := "aabbccdd11223344"
	resolvedB := "eeff00112233aabb"
	dbtest.Observation(t, db, txID, observer2, recent.Add(-100*time.Second), 8, -95, "aa:"+resolvedA, "bb:"+resolvedB)

	cfg := &Config{Port: 3000}
	hub := NewHub()
	srv := NewServer(db, cfg, hub)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	srv.store = store
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	// Sanity: /api/packets should show resolved_path for this tx.
	reqP := httptest.NewRequest("GET", "/api/packets?limit=10", nil)
	wP := httptest.NewRecorder()
	router.ServeHTTP(wP, reqP)
	var pktsBody map[string]interface{}
	json.Unmarshal(wP.Body.Bytes(), &pktsBody)
	pkts, _ := pktsBody["packets"].([]interface{})
	hasOnPackets := false
	for _, p := range pkts {
		pm := p.(map[string]interface{})
		if pm["hash"] == "testhash00000001" && pm["resolved_path"] != nil {
			hasOnPackets = true
		}
	}
	if !hasOnPackets {
		t.Fatal("precondition: /api/packets must report resolved_path for tx")
	}

	req := httptest.NewRequest("GET", "/api/nodes/aabbccdd11223344/health", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	rp, _ := body["recentPackets"].([]interface{})
	if len(rp) == 0 {
		t.Fatal("no recentPackets")
	}
	for _, p := range rp {
		pm := p.(map[string]interface{})
		if pm["hash"] == "testhash00000001" {
			if pm["resolved_path"] == nil {
				t.Fatal("BUG #810: /health.recentPackets resolved_path is nil despite /api/packets reporting it")
			}
			return
		}
	}
	t.Fatal("tx not found in recentPackets")
}
