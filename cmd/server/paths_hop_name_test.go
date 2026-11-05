package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/gorilla/mux"
)

// TestHandleNodePaths_HopName_CanonicalPathShowsTarget_1144 is the regression
// test for issue #1144.
//
// Bug: the biased hop resolver picked a GPS-having sibling over the actual target
// node when the target had no GPS coordinates, causing the wrong name in hop slots.
//
// Fix: the canonical-path branch (Option A) uses lookupNode(resolvedPK) with the
// full pubkey stored in resolved_path, bypassing the biased resolver entirely.
// This test verifies that when two nodes share a short prefix ("37"), the hop
// display uses the stored resolved_path pubkey and shows the correct target name.
func TestHandleNodePaths_HopName_CanonicalPathShowsTarget_1144(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	recent := time.Now().UTC().Add(-1 * time.Hour)

	targetPK := "37cf0832" + strings.Repeat("a", 56)
	siblingPK := "37bb0000" + strings.Repeat("b", 56)
	observerKey := scopeKey(20)

	if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: targetPK,
		Name:      "CJS SF Mission",
		Role:      "repeater",
		Lat:       nil,
		Lon:       nil,
		RxTime:    recent,
		IngestNow: recent,
	}); err != nil {
		t.Fatalf("seed target node: %v", err)
	}
	if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: siblingPK,
		Name:      "Templeton Hills",
		Role:      "repeater",
		Lat:       fptr(35.5),
		Lon:       fptr(-120.7),
		RxTime:    recent,
		IngestNow: recent,
	}); err != nil {
		t.Fatalf("seed sibling node: %v", err)
	}

	obsIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        observerKey,
		Name:      "Paths Observer",
		IATA:      "SFO",
		Meta:      nil,
		LastSeen:  recent,
		IngestNow: recent,
	})
	if err != nil {
		t.Fatalf("seed observer: %v", err)
	}

	txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		Hash:        "1144a1b2c3d4e5f6",
		RawHex:      "aa",
		RouteType:   1,
		PayloadType: 4,
		DecodedJSON: "{}",
	}, recent)
	if err != nil {
		t.Fatalf("seed transmission: %v", err)
	}

	_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: txID,
		ObserverIdx:    iptr64(obsIdx),
		SNR:            fptr(5.0),
		RSSI:           fptr(-90.0),
		Timestamp:      recent,
	}, []string{"37"}, []*string{sptr(targetPK)})
	if err != nil {
		t.Fatalf("seed observation: %v", err)
	}
	if !inserted {
		t.Fatalf("expected observation to be inserted for tx %d", txID)
	}

	cfg := &Config{Port: 3000}
	hub := NewHub()
	srv := NewServer(db, cfg, hub)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	srv.store = store
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache(): %v", err)
	}
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	req := httptest.NewRequest("GET", "/api/nodes/"+targetPK+"/paths", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /paths: code=%d body=%s", w.Code, w.Body.String())
	}
	var resp NodePathsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Paths) != 1 {
		t.Fatalf("expected 1 path, got %d", len(resp.Paths))
	}
	if len(resp.Paths[0].Hops) != 1 {
		t.Fatalf("expected 1 hop, got %d", len(resp.Paths[0].Hops))
	}
	hop := resp.Paths[0].Hops[0]
	if hop.Name != "CJS SF Mission" {
		if hop.Name == "Templeton Hills" {
			t.Errorf("hop name = %q (sibling mis-resolution #1144): canonical path must show target name %q", hop.Name, "CJS SF Mission")
		} else {
			t.Errorf("hop name = %q, want %q", hop.Name, "CJS SF Mission")
		}
	}
	if hop.Pubkey != targetPK {
		t.Errorf("hop pubkey = %q, want %q", hop.Pubkey, targetPK)
	}
}
