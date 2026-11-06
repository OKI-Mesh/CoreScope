package main

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/gorilla/mux"
)

func TestQueryCoverageRowsByPrefixAndBBox(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	rxKey := padKey("c0c0c0c0c0c0c0c0")

	for _, r := range []struct {
		heard string
		lat   float64
		lon   float64
	}{
		{"aabbcc", 51.05, 3.72},
		{"ddeeff", 51.06, 3.73},
	} {
		inserted, err := db.InsertClientReception(ctx, types.ClientReception{
			RxPubkey:    rxKey,
			HeardKey:    r.heard,
			HeardKeyLen: len(r.heard) / 2,
			SNR:         fptr(-6.0),
			RSSI:        nil,
			Lat:         r.lat,
			Lon:         r.lon,
			PosAccM:     nil,
			RxAt:        now,
			IngestedAt:  now,
			Src:         "rxlog",
		})
		if err != nil {
			t.Fatalf("seed reception %s: %v", r.heard, err)
		}
		if !inserted {
			t.Fatalf("reception %s was not inserted", r.heard)
		}
	}

	srv := &Server{db: db, cfg: &Config{ClientRxCoverage: &ClientRxCoverageConfig{Enabled: true}}}
	target := "aabbccddeeff0011" + strings.Repeat("0", 48)

	rows, err := srv.queryCoverageRows(ctx, target, bbox{MinLat: 50, MinLon: 3, MaxLat: 52, MaxLon: 4})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row by prefix, got %d", len(rows))
	}
	rows, err = srv.queryCoverageRows(ctx, target, bbox{MinLat: 0, MinLon: 0, MaxLat: 1, MaxLon: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("bbox filter failed, got %d", len(rows))
	}
}

func TestMobileRxStats(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	compA := padKey("c0a0c0a0c0a0c0a0")
	compB := padKey("c0b0c0b0c0b0c0b0")

	for _, r := range []struct {
		rx    string
		heard string
		snr   float64
		lat   float64
		lon   float64
		at    time.Time
	}{
		{compA, "aabbcc", -6, 51.05, 3.72, now.Add(-3 * time.Minute)},
		{compB, "aabbcc", -8, 51.06, 3.73, now.Add(-2 * time.Minute)},
		{compA, "ffeedd", -5, 51.07, 3.74, now.Add(-1 * time.Minute)},
	} {
		inserted, err := db.InsertClientReception(ctx, types.ClientReception{
			RxPubkey:    r.rx,
			HeardKey:    r.heard,
			HeardKeyLen: len(r.heard) / 2,
			SNR:         fptr(r.snr),
			RSSI:        nil,
			Lat:         r.lat,
			Lon:         r.lon,
			PosAccM:     nil,
			RxAt:        r.at,
			IngestedAt:  now,
			Src:         "rxlog",
		})
		if err != nil {
			t.Fatalf("seed reception from %s heard %s: %v", r.rx[:4], r.heard, err)
		}
		if !inserted {
			t.Fatalf("reception from %s heard %s was not inserted", r.rx[:4], r.heard)
		}
	}

	srv := &Server{db: db, cfg: &Config{ClientRxCoverage: &ClientRxCoverageConfig{Enabled: true}}}
	target := "aabbccddeeff0011" + strings.Repeat("0", 48)
	c, cl, err := srv.mobileRxStats(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	if c != 2 || cl != 2 {
		t.Fatalf("got count=%d clients=%d, want 2/2", c, cl)
	}
}

func serveRxCoverage(srv *Server, path string) *httptest.ResponseRecorder {
	router := mux.NewRouter()
	router.HandleFunc("/api/nodes/{pubkey}/rx-coverage", srv.handleNodeRxCoverage).Methods("GET")
	req := httptest.NewRequest("GET", path, nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	return rr
}

// nodePK is a full 64-hex pubkey whose 3-byte prefix is the seeded heard_key.
const nodePK = "aabbcc0000000000000000000000000000000000000000000000000000000000"

func TestRxCoverageEndpointGeoJSON(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	inserted, err := db.InsertClientReception(ctx, types.ClientReception{
		RxPubkey:    padKey("c0c0c0c0c0c0c0c0"),
		HeardKey:    "aabbcc",
		HeardKeyLen: 3,
		SNR:         fptr(-6.0),
		RSSI:        nil,
		Lat:         51.05,
		Lon:         3.72,
		PosAccM:     nil,
		RxAt:        now,
		IngestedAt:  now,
		Src:         "rxlog",
	})
	if err != nil {
		t.Fatalf("seed reception: %v", err)
	}
	if !inserted {
		t.Fatal("reception was not inserted")
	}
	srv := &Server{db: db, cfg: &Config{ClientRxCoverage: &ClientRxCoverageConfig{Enabled: true}}}

	rr := serveRxCoverage(srv, "/api/nodes/"+nodePK+"/rx-coverage?bbox=50,3,52,4&z=12")
	if rr.Code != 200 {
		t.Fatalf("status %d body %s", rr.Code, rr.Body.String())
	}
	var fc CoverageFeatureCollection
	if err := json.Unmarshal(rr.Body.Bytes(), &fc); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if fc.Type != "FeatureCollection" || len(fc.Features) != 1 {
		t.Fatalf("unexpected fc: %+v", fc)
	}
	if fc.MobileReceptions != 1 || fc.MobileClients != 1 {
		t.Fatalf("want mobile_receptions=1 mobile_clients=1, got %d/%d", fc.MobileReceptions, fc.MobileClients)
	}
	if serveRxCoverage(srv, "/api/nodes/"+nodePK+"/rx-coverage").Code != 400 {
		t.Fatal("missing bbox should be 400")
	}
	if serveRxCoverage(srv, "/api/nodes/nothex/rx-coverage?bbox=50,3,52,4").Code != 400 {
		t.Fatal("non-hex pubkey should be 400")
	}
}

// TestNodeRxCoverageHidesBlacklistedAndHidden verifies #1727 r2 must-fix #1: the
// per-node coverage endpoint must 404 for blacklisted or hidden-prefix nodes, so
// their GPS hex bins / counts aren't retrievable at a pubkey the rest of the API
// hides — not just the node name.
func TestNodeRxCoverageHidesBlacklistedAndHidden(t *testing.T) {
	ctx := t.Context()
	const hidPK = "ddee110000000000000000000000000000000000000000000000000000000000"
	db := setupTestDB(t)

	now := time.Now().UTC()
	inserted, err := db.InsertClientReception(ctx, types.ClientReception{
		RxPubkey:    padKey("c0c0c0c0c0c0c0c0"),
		HeardKey:    "aabbcc",
		HeardKeyLen: 3,
		SNR:         fptr(-6.0),
		RSSI:        nil,
		Lat:         51.05,
		Lon:         3.72,
		PosAccM:     nil,
		RxAt:        now,
		IngestedAt:  now,
		Src:         "rxlog",
	})
	if err != nil {
		t.Fatalf("seed reception: %v", err)
	}
	if !inserted {
		t.Fatal("reception was not inserted")
	}
	seedNode(t, db, hidPK, "🚫Secret", "repeater", nil, nil, now)

	srv := &Server{db: db, cfg: &Config{
		ClientRxCoverage:   &ClientRxCoverageConfig{Enabled: true},
		NodeBlacklist:      []string{nodePK},
		HiddenNamePrefixes: []string{"🚫"},
	}}

	if code := serveRxCoverage(srv, "/api/nodes/"+nodePK+"/rx-coverage?bbox=50,3,52,4").Code; code != 404 {
		t.Fatalf("blacklisted node coverage should be 404, got %d", code)
	}
	if code := serveRxCoverage(srv, "/api/nodes/"+hidPK+"/rx-coverage?bbox=50,3,52,4").Code; code != 404 {
		t.Fatalf("hidden-prefix node coverage should be 404, got %d", code)
	}
}
