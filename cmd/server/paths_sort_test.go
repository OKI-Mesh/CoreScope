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

// TestHandleNodePaths_SortByRecency_1145 is the regression test for issue #1145.
//
// Prior to the fix, paths were returned in map-iteration order (non-deterministic).
// After the fix, paths are sorted by LastSeen descending (newest first), with
// Count as a tiebreaker (higher first).
//
// Setup: target node "aa..." is reached via three distinct paths.
//
//	Path A (via relay "11..."): 3 transmissions, last seen 2026-01-03 (oldest)
//	Path B (via relay "22..."): 1 transmission,  last seen 2026-05-01 (newest)
//	Path C (direct — "aa..." only): 2 transmissions, last seen 2026-03-02 (middle)
//
// Expected sort: B (newest) → C (middle) → A (oldest)
// Also covers: when LastSeen is equal, Count descending is the tiebreaker.
func TestHandleNodePaths_SortByRecency_1145(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)

	targetPK := "aabbccdd" + strings.Repeat("1", 56)
	relay1PK := "11" + strings.Repeat("0", 62)
	relay2PK := "22" + strings.Repeat("0", 62)
	observerKey := scopeKey(30)

	seenAt := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	for _, n := range []struct {
		pk   string
		name string
	}{
		{targetPK, "Target"},
		{relay1PK, "Relay One"},
		{relay2PK, "Relay Two"},
	} {
		if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
			PublicKey: n.pk,
			Name:      n.name,
			Role:      "repeater",
			Lat:       nil,
			Lon:       nil,
			RxTime:    seenAt,
			IngestNow: seenAt,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n.name, err)
		}
	}

	obsIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        observerKey,
		Name:      "Recency Observer",
		IATA:      "SFO",
		Meta:      nil,
		LastSeen:  seenAt,
		IngestNow: seenAt,
	})
	if err != nil {
		t.Fatalf("seed observer: %v", err)
	}

	packets := []struct {
		hash     string
		rawHex   string
		at       time.Time
		hops     []string
		resolved []*string
	}{
		{"a000000000000001", "aa", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), []string{"11", "aa"}, []*string{sptr(relay1PK), sptr(targetPK)}},
		{"a000000000000002", "aa", time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), []string{"11", "aa"}, []*string{sptr(relay1PK), sptr(targetPK)}},
		{"a000000000000003", "aa", time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC), []string{"11", "aa"}, []*string{sptr(relay1PK), sptr(targetPK)}},
		{"b000000000000001", "bb", time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC), []string{"22", "aa"}, []*string{sptr(relay2PK), sptr(targetPK)}},
		{"c000000000000001", "cc", time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), []string{"aa"}, []*string{sptr(targetPK)}},
		{"c000000000000002", "cc", time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC), []string{"aa"}, []*string{sptr(targetPK)}},
	}
	for _, p := range packets {
		txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
			Hash:        p.hash,
			RawHex:      p.rawHex,
			RouteType:   1,
			PayloadType: 4,
			DecodedJSON: "{}",
		}, p.at)
		if err != nil {
			t.Fatalf("seed transmission %s: %v", p.hash, err)
		}
		_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
			TransmissionID: txID,
			ObserverIdx:    iptr64(obsIdx),
			SNR:            fptr(5.0),
			RSSI:           fptr(-90.0),
			Timestamp:      p.at,
		}, p.hops, p.resolved)
		if err != nil {
			t.Fatalf("seed observation %s: %v", p.hash, err)
		}
		if !inserted {
			t.Fatalf("expected observation to be inserted for tx %d", txID)
		}
	}

	cfg := &Config{Port: 3000}
	hub := NewHub()
	srv := NewServer(db, cfg, hub)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache(): %v", err)
	}
	srv.store = store
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

	if len(resp.Paths) != 3 {
		t.Fatalf("expected 3 distinct paths, got %d: %+v", len(resp.Paths), resp.Paths)
	}
	if resp.TotalTransmissions != 6 {
		t.Errorf("expected TotalTransmissions=6, got %d", resp.TotalTransmissions)
	}

	wantCounts := []int{1, 2, 3}
	for i, want := range wantCounts {
		got := resp.Paths[i].Count
		if got != want {
			t.Errorf("Paths[%d].Count = %d, want %d (sort order wrong — paths must be newest-first)", i, got, want)
		}
	}
}

// TestHandleNodePaths_SortCountTiebreaker_1145 verifies that when two paths
// have identical LastSeen, the one with higher Count appears first.
func TestHandleNodePaths_SortCountTiebreaker_1145(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)

	targetPK := "ccddeeff" + strings.Repeat("1", 56)
	relay1PK := "aa" + strings.Repeat("1", 62)
	relay2PK := "bb" + strings.Repeat("2", 62)
	observerKey := scopeKey(31)
	sameTS := time.Date(2026, 4, 15, 12, 0, 0, 0, time.UTC)

	for _, n := range []struct {
		pk   string
		name string
	}{
		{targetPK, "Tgt"},
		{relay1PK, "Relay One"},
		{relay2PK, "Relay Two"},
	} {
		if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
			PublicKey: n.pk,
			Name:      n.name,
			Role:      "repeater",
			Lat:       nil,
			Lon:       nil,
			RxTime:    sameTS,
			IngestNow: sameTS,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n.name, err)
		}
	}

	obsIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        observerKey,
		Name:      "Tiebreaker Observer",
		IATA:      "SFO",
		Meta:      nil,
		LastSeen:  sameTS,
		IngestNow: sameTS,
	})
	if err != nil {
		t.Fatalf("seed observer: %v", err)
	}

	packets := []struct {
		hash     string
		rawHex   string
		at       time.Time
		hops     []string
		resolved []*string
	}{
		{"e000000000000010", "ee", time.Date(2026, 4, 15, 11, 0, 0, 0, time.UTC), []string{"aa", "cc"}, []*string{sptr(relay1PK), sptr(targetPK)}},
		{"e000000000000011", "ee", time.Date(2026, 4, 15, 11, 30, 0, 0, time.UTC), []string{"aa", "cc"}, []*string{sptr(relay1PK), sptr(targetPK)}},
		{"e000000000000012", "ee", sameTS, []string{"aa", "cc"}, []*string{sptr(relay1PK), sptr(targetPK)}},
		{"f000000000000020", "ff", sameTS, []string{"bb", "cc"}, []*string{sptr(relay2PK), sptr(targetPK)}},
	}
	for _, p := range packets {
		txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
			Hash:        p.hash,
			RawHex:      p.rawHex,
			RouteType:   1,
			PayloadType: 4,
			DecodedJSON: "{}",
		}, p.at)
		if err != nil {
			t.Fatalf("seed transmission %s: %v", p.hash, err)
		}
		_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
			TransmissionID: txID,
			ObserverIdx:    iptr64(obsIdx),
			SNR:            fptr(5.0),
			RSSI:           fptr(-90.0),
			Timestamp:      p.at,
		}, p.hops, p.resolved)
		if err != nil {
			t.Fatalf("seed observation %s: %v", p.hash, err)
		}
		if !inserted {
			t.Fatalf("expected observation to be inserted for tx %d", txID)
		}
	}

	cfg := &Config{Port: 3000}
	hub := NewHub()
	srv := NewServer(db, cfg, hub)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache(): %v", err)
	}
	srv.store = store
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
	if len(resp.Paths) != 2 {
		t.Fatalf("expected 2 paths, got %d", len(resp.Paths))
	}
	if resp.Paths[0].Count != 3 {
		t.Errorf("Paths[0].Count = %d, want 3 (higher-count path must sort first when LastSeen equal)", resp.Paths[0].Count)
	}
	if resp.Paths[1].Count != 1 {
		t.Errorf("Paths[1].Count = %d, want 1", resp.Paths[1].Count)
	}
}
