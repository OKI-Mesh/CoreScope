package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/gorilla/mux"
)

// getAnalytics requests path, requires a 200 and decodes the body into out.
func getAnalytics(t *testing.T, router *mux.Router, path string, out any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET %s = %d, want 200: %s", path, w.Code, w.Body.String())
	}
	if err := json.Unmarshal(w.Body.Bytes(), out); err != nil {
		t.Fatalf("GET %s: decode: %v", path, err)
	}
}

// An unknown region or area gives an empty result, not unfiltered data. The
// unfiltered request is checked first so each test proves the fixture has data
// that the filter could have leaked.

func TestAnalyticsRF_UnknownRegionAndAreaAreEmpty(t *testing.T) {
	_, router := setupTestServer(t)

	var unfiltered api.RFAnalyticsResponse
	getAnalytics(t, router, "/api/analytics/rf", &unfiltered)
	if unfiltered.TotalPackets == 0 {
		t.Fatal("fixture has no packets, so the test proves nothing")
	}

	for _, query := range []string{"region=ZZZ", "area=NOPE", "region=ZZZ&area=NOPE"} {
		var got api.RFAnalyticsResponse
		getAnalytics(t, router, "/api/analytics/rf?"+query, &got)
		if got.TotalPackets != 0 || got.TotalTransmissions != 0 {
			t.Errorf("rf?%s: totalPackets=%d totalTransmissions=%d, want 0 and 0", query, got.TotalPackets, got.TotalTransmissions)
		}
	}
}

func TestAnalyticsDistance_UnknownRegionAndAreaAreEmpty(t *testing.T) {
	srv, router := setupTestServer(t)

	// The first distance request only starts the lazy index build (202), so
	// build it first and wait for it.
	srv.store.TriggerDistanceIndexBuild()
	deadline := time.Now().Add(5 * time.Second)
	for !srv.store.DistanceIndexBuilt() {
		if time.Now().After(deadline) {
			t.Fatal("distance index was not built within 5s")
		}
		time.Sleep(10 * time.Millisecond)
	}

	var unfiltered api.DistanceAnalyticsResponse
	getAnalytics(t, router, "/api/analytics/distance", &unfiltered)
	if unfiltered.Summary.TotalHops+unfiltered.Summary.TotalPaths == 0 {
		t.Skip("fixture has no resolvable hops, so the test proves nothing")
	}

	for _, query := range []string{"region=ZZZ", "area=NOPE", "region=ZZZ&area=NOPE"} {
		var got api.DistanceAnalyticsResponse
		getAnalytics(t, router, "/api/analytics/distance?"+query, &got)
		if got.Summary.TotalHops != 0 || got.Summary.TotalPaths != 0 {
			t.Errorf("distance?%s: totalHops=%d totalPaths=%d, want 0 and 0", query, got.Summary.TotalHops, got.Summary.TotalPaths)
		}
	}
}
func TestAnalyticsHashSizes_UnknownRegionAndAreaAreEmpty(t *testing.T) {
	_, router := setupTestServer(t)

	var unfiltered api.HashSizeAnalyticsResponse
	getAnalytics(t, router, "/api/analytics/hash-sizes", &unfiltered)
	if unfiltered.Total == 0 {
		t.Skip("fixture has no hash-size data, so the test proves nothing")
	}

	for _, query := range []string{"region=ZZZ", "area=NOPE", "region=ZZZ&area=NOPE"} {
		var got api.HashSizeAnalyticsResponse
		getAnalytics(t, router, "/api/analytics/hash-sizes?"+query, &got)
		if got.Total != 0 {
			t.Errorf("hash-sizes?%s: total=%d, want 0", query, got.Total)
		}
	}
}

// collisionNodes counts the nodes behind a hash-collisions response, over all
// prefix sizes.
func collisionNodes(r api.HashCollisionResponse) int {
	total := 0
	for _, size := range r.BySize {
		total += size.Stats.TotalNodes
	}
	return total
}

func TestAnalyticsHashCollisions_UnknownRegionAndAreaAreEmpty(t *testing.T) {
	_, router := setupTestServer(t)

	var unfiltered api.HashCollisionResponse
	getAnalytics(t, router, "/api/analytics/hash-collisions", &unfiltered)
	if collisionNodes(unfiltered) == 0 {
		t.Skip("fixture has no repeaters with a hash size, so the test proves nothing")
	}

	for _, query := range []string{"region=ZZZ", "area=NOPE", "region=ZZZ&area=NOPE"} {
		var got api.HashCollisionResponse
		getAnalytics(t, router, "/api/analytics/hash-collisions?"+query, &got)
		if n := collisionNodes(got); n != 0 {
			t.Errorf("hash-collisions?%s: total_nodes=%d, want 0", query, n)
		}
	}
}
