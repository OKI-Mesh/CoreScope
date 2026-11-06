package main

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
)

// ─── resolveWithContext unit tests ─────────────────────────────────────────────

func TestResolveWithContext_UniquePrefix(t *testing.T) {
	pm := buildPrefixMap([]nodeInfo{
		{Role: "repeater", PublicKey: "a1b2c3d4", Name: "Node-A", HasGPS: true, Lat: 1, Lon: 2},
	})
	ni, confidence, _ := pm.resolveWithContext("a1b2c3d4", nil, nil)
	if ni == nil || ni.Name != "Node-A" {
		t.Fatal("expected Node-A")
	}
	if confidence != "unique_prefix" {
		t.Fatalf("expected unique_prefix, got %s", confidence)
	}
}

func TestResolveWithContext_NoMatch(t *testing.T) {
	pm := buildPrefixMap([]nodeInfo{
		{Role: "repeater", PublicKey: "a1b2c3d4", Name: "Node-A"},
	})
	ni, confidence, _ := pm.resolveWithContext("ff", nil, nil)
	if ni != nil {
		t.Fatal("expected nil")
	}
	if confidence != "no_match" {
		t.Fatalf("expected no_match, got %s", confidence)
	}
}

func TestResolveWithContext_AffinityWins(t *testing.T) {
	pm := buildPrefixMap([]nodeInfo{
		{Role: "repeater", PublicKey: "a1aaaaaa", Name: "Node-A1"},
		{Role: "repeater", PublicKey: "a1bbbbbb", Name: "Node-A2"},
	})

	graph := NewNeighborGraph()
	for i := 0; i < 100; i++ {
		graph.upsertEdge("c0c0c0c0", "a1aaaaaa", "a1", "obs1", nil, time.Now())
	}

	ni, confidence, score := pm.resolveWithContext("a1", []string{"c0c0c0c0"}, graph)
	if ni == nil || ni.Name != "Node-A1" {
		t.Fatalf("expected Node-A1, got %v", ni)
	}
	if confidence != "neighbor_affinity" {
		t.Fatalf("expected neighbor_affinity, got %s", confidence)
	}
	if score <= 0 {
		t.Fatalf("expected positive score, got %f", score)
	}
}

func TestResolveWithContext_AffinityTooClose_FallsToGeo(t *testing.T) {
	pm := buildPrefixMap([]nodeInfo{
		{Role: "repeater", PublicKey: "a1aaaaaa", Name: "Node-A1", HasGPS: true, Lat: 10, Lon: 20},
		{Role: "repeater", PublicKey: "a1bbbbbb", Name: "Node-A2", HasGPS: true, Lat: 11, Lon: 21},
		{Role: "repeater", PublicKey: "c0c0c0c0", Name: "Ctx", HasGPS: true, Lat: 10.1, Lon: 20.1},
	})

	graph := NewNeighborGraph()
	for i := 0; i < 50; i++ {
		graph.upsertEdge("c0c0c0c0", "a1aaaaaa", "a1", "obs1", nil, time.Now())
		graph.upsertEdge("c0c0c0c0", "a1bbbbbb", "a1", "obs1", nil, time.Now())
	}

	ni, confidence, _ := pm.resolveWithContext("a1", []string{"c0c0c0c0"}, graph)
	if ni == nil {
		t.Fatal("expected a result")
	}
	if confidence != "geo_proximity" {
		t.Fatalf("expected geo_proximity, got %s", confidence)
	}
	if ni.Name != "Node-A1" {
		t.Fatalf("expected Node-A1 (closer to context), got %s", ni.Name)
	}
}

func TestResolveWithContext_GPSPreference(t *testing.T) {
	pm := buildPrefixMap([]nodeInfo{
		{Role: "repeater", PublicKey: "a1aaaaaa", Name: "NoGPS"},
		{Role: "repeater", PublicKey: "a1bbbbbb", Name: "HasGPS", HasGPS: true, Lat: 1, Lon: 2},
	})

	ni, confidence, _ := pm.resolveWithContext("a1", nil, nil)
	if ni == nil || ni.Name != "HasGPS" {
		t.Fatalf("expected HasGPS, got %v", ni)
	}
	if confidence != "gps_preference" {
		t.Fatalf("expected gps_preference, got %s", confidence)
	}
}

func TestResolveWithContext_FirstMatchFallback(t *testing.T) {
	pm := buildPrefixMap([]nodeInfo{
		{Role: "repeater", PublicKey: "a1aaaaaa", Name: "First"},
		{Role: "repeater", PublicKey: "a1bbbbbb", Name: "Second"},
	})

	ni, confidence, _ := pm.resolveWithContext("a1", nil, nil)
	if ni == nil || ni.Name != "First" {
		t.Fatalf("expected First, got %v", ni)
	}
	if confidence != "observation_count_fallback" {
		t.Fatalf("expected observation_count_fallback, got %s", confidence)
	}
}

func TestResolveWithContext_NilGraphFallsToGPS(t *testing.T) {
	pm := buildPrefixMap([]nodeInfo{
		{Role: "repeater", PublicKey: "a1aaaaaa", Name: "NoGPS"},
		{Role: "repeater", PublicKey: "a1bbbbbb", Name: "HasGPS", HasGPS: true, Lat: 1, Lon: 2},
	})

	ni, confidence, _ := pm.resolveWithContext("a1", []string{"someone"}, nil)
	if ni == nil || ni.Name != "HasGPS" {
		t.Fatalf("expected HasGPS, got %v", ni)
	}
	if confidence != "gps_preference" {
		t.Fatalf("expected gps_preference, got %s", confidence)
	}
}

func TestResolveWithContext_BackwardCompatResolve(t *testing.T) {
	// Verify original resolve() still works unchanged
	pm := buildPrefixMap([]nodeInfo{
		{Role: "repeater", PublicKey: "a1aaaaaa", Name: "NoGPS"},
		{Role: "repeater", PublicKey: "a1bbbbbb", Name: "HasGPS", HasGPS: true, Lat: 1, Lon: 2},
	})
	ni := pm.resolve("a1")
	if ni == nil || ni.Name != "HasGPS" {
		t.Fatalf("expected HasGPS from resolve(), got %v", ni)
	}
}

// ─── geoDistApprox ─────────────────────────────────────────────────────────────

func TestGeoDistApprox_SamePoint(t *testing.T) {
	d := geoDistApprox(37.0, -122.0, 37.0, -122.0)
	if d != 0 {
		t.Fatalf("expected 0, got %f", d)
	}
}

func TestGeoDistApprox_Ordering(t *testing.T) {
	d1 := geoDistApprox(37.0, -122.0, 37.01, -122.01)
	d2 := geoDistApprox(37.0, -122.0, 38.0, -121.0)
	if d1 >= d2 {
		t.Fatal("closer point should have smaller distance")
	}
}

// ─── handleResolveHops enhanced response (API tests) ───────────────────────────

func TestResolveHopsAPI_UniquePrefix(t *testing.T) {
	ctx := t.Context()
	srv, router := setupTestServer(t)

	now := time.Now().UTC()
	uniquePK := "ff1122" + strings.Repeat("0", 58)

	if err := srv.db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: uniquePK,
		Name:      "UniqueNode",
		Role:      "repeater",
		Lat:       fptr(37.0),
		Lon:       fptr(-122.0),
		RxTime:    now,
		IngestNow: now,
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}
	if err := srv.store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache(): %v", err)
	}

	req := httptest.NewRequest("GET", "/api/resolve-hops?hops=ff1122", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	var result ResolveHopsResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
		t.Fatalf("bad JSON: %v", err)
	}

	hr, ok := result.Resolved["ff1122"]
	if !ok {
		t.Fatal("expected hop in resolved map")
	}
	if hr.Confidence != "unique_prefix" {
		t.Fatalf("expected unique_prefix, got %s", hr.Confidence)
	}
}

func TestResolveHopsAPI_AmbiguousNoContext(t *testing.T) {
	ctx := t.Context()
	srv, router := setupTestServer(t)

	now := time.Now().UTC()
	for _, n := range []struct {
		pk       string
		name     string
		lat, lon float64
	}{
		{"ee1aaaaaaa" + strings.Repeat("0", 54), "Node-E1", 37.0, -122.0},
		{"ee1bbbbbbb" + strings.Repeat("0", 54), "Node-E2", 38.0, -121.0},
	} {
		if err := srv.db.CreateOrUpdateNode(ctx, database.NodeInput{
			PublicKey: n.pk,
			Name:      n.name,
			Role:      "repeater",
			Lat:       fptr(n.lat),
			Lon:       fptr(n.lon),
			RxTime:    now,
			IngestNow: now,
		}); err != nil {
			t.Fatalf("seed %s: %v", n.name, err)
		}
	}
	if err := srv.store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache(): %v", err)
	}

	req := httptest.NewRequest("GET", "/api/resolve-hops?hops=ee1", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	var result ResolveHopsResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
		t.Fatalf("bad JSON: %v", err)
	}

	hr := result.Resolved["ee1"]
	if hr == nil {
		t.Fatal("expected hop in resolved map")
	}
	if hr.Confidence != "gps_preference" {
		t.Fatalf("expected gps_preference, got %s", hr.Confidence)
	}
	if len(hr.Candidates) != 2 {
		t.Fatalf("expected 2 candidates, got %d", len(hr.Candidates))
	}
	for _, c := range hr.Candidates {
		if c.AffinityScore != nil {
			t.Fatal("expected nil affinity score without context")
		}
	}
}

func TestResolveHopsAPI_WithAffinityContext(t *testing.T) {
	ctx := t.Context()
	srv, router := setupTestServer(t)

	now := time.Now().UTC()
	d1PK := "dd1aaaaaaa" + strings.Repeat("0", 54)
	d2PK := "dd1bbbbbbb" + strings.Repeat("0", 54)
	contextPK := "c0c0c0c0c0" + strings.Repeat("0", 54)

	for _, n := range []struct {
		pk       string
		name     string
		lat, lon float64
	}{
		{d1PK, "Node-D1", 37.0, -122.0},
		{d2PK, "Node-D2", 38.0, -121.0},
		{contextPK, "Context", 37.1, -122.1},
	} {
		if err := srv.db.CreateOrUpdateNode(ctx, database.NodeInput{
			PublicKey: n.pk,
			Name:      n.name,
			Role:      "repeater",
			Lat:       fptr(n.lat),
			Lon:       fptr(n.lon),
			RxTime:    now,
			IngestNow: now,
		}); err != nil {
			t.Fatalf("seed %s: %v", n.name, err)
		}
	}
	if err := srv.store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache(): %v", err)
	}

	graph := NewNeighborGraph()
	for i := 0; i < 100; i++ {
		graph.upsertEdge(contextPK, d1PK, "dd1", scopeKey(72), nil, time.Now())
	}
	graph.builtAt = time.Now()
	srv.neighborMu.Lock()
	srv.neighborGraph = graph
	srv.neighborMu.Unlock()

	req := httptest.NewRequest("GET", "/api/resolve-hops?hops=dd1&from_node="+contextPK, nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	var result ResolveHopsResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
		t.Fatalf("bad JSON: %v", err)
	}

	hr := result.Resolved["dd1"]
	if hr == nil {
		t.Fatal("expected hop in resolved map")
	}
	if hr.Confidence != "neighbor_affinity" {
		t.Fatalf("expected neighbor_affinity, got %s", hr.Confidence)
	}
	if hr.BestCandidate == nil || *hr.BestCandidate != d1PK {
		t.Fatalf("expected bestCandidate %s, got %v", d1PK, hr.BestCandidate)
	}

	hasScore := false
	for _, c := range hr.Candidates {
		if c.AffinityScore != nil && *c.AffinityScore > 0 {
			hasScore = true
		}
	}
	if !hasScore {
		t.Fatal("expected at least one candidate with affinity score")
	}
}

func TestResolveHopsAPI_ResponseShape(t *testing.T) {
	ctx := t.Context()
	srv, router := setupTestServer(t)

	now := time.Now().UTC()
	if err := srv.db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: "bb1aaaaaaa" + strings.Repeat("0", 54),
		Name:      "Node-B1",
		Role:      "repeater",
		Lat:       fptr(37.0),
		Lon:       fptr(-122.0),
		RxTime:    now,
		IngestNow: now,
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}
	if err := srv.store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache(): %v", err)
	}

	req := httptest.NewRequest("GET", "/api/resolve-hops?hops=bb1a", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(rr.Body.Bytes(), &raw); err != nil {
		t.Fatalf("bad JSON: %v", err)
	}
	if _, ok := raw["resolved"]; !ok {
		t.Fatal("missing 'resolved' key")
	}

	var resolved map[string]map[string]json.RawMessage
	if err := json.Unmarshal(raw["resolved"], &resolved); err != nil {
		t.Fatalf("bad resolved JSON: %v", err)
	}
	if len(resolved) == 0 {
		t.Fatal("expected at least one resolved hop")
	}

	for _, hr := range resolved {
		if _, ok := hr["confidence"]; !ok {
			t.Error("missing 'confidence' field in HopResolution")
		}
		if _, ok := hr["candidates"]; !ok {
			t.Error("missing 'candidates' field")
		}
	}
}
