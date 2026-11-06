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

// collisionScenario captures the shared fixture state used by every #1352
// sub-test: 3 nodes sharing the 2-char "c0" prefix, plus a wired-up
// server + router ready to serve /api/nodes/{pk}/paths.
type collisionScenario struct {
	srv    *Server
	db     *database.DB
	router *mux.Router

	nodeAPK string
	nodeBPK string
	nodeCPK string

	recent      time.Time
	observerIdx int64
}

// setupCollisionScenario wires up the shared #1352 fixture: 3 "c0"-prefix
// nodes with configurable GPS, a Server + PacketStore + router. Caller
// inserts transmissions/observations and queries via s.query.
func setupCollisionScenario(t *testing.T, withGPS bool) *collisionScenario {
	t.Helper()
	ctx := t.Context()

	db := setupTestDB(t)
	recent := time.Now().UTC().Add(-1 * time.Hour)

	sc := &collisionScenario{
		db:      db,
		nodeAPK: "c0dedad42222aaaa" + strings.Repeat("0", 48),
		nodeBPK: "c0ffeec733333333" + strings.Repeat("0", 48),
		nodeCPK: "c0efb77f44444444" + strings.Repeat("0", 48),
		recent:  recent,
	}

	var aLat, aLon, cLat, cLon *float64
	bLat, bLon := fptr(37.79), fptr(-122.41)
	if withGPS {
		aLat, aLon = fptr(37.78), fptr(-122.40)
		cLat, cLon = fptr(37.50), fptr(-122.00)
	}

	for _, n := range []struct {
		pk       string
		name     string
		lat, lon *float64
	}{
		{sc.nodeAPK, "NodeA", aLat, aLon},
		{sc.nodeBPK, "NodeB", bLat, bLon},
		{sc.nodeCPK, "NodeC", cLat, cLon},
	} {
		if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
			PublicKey: n.pk,
			Name:      n.name,
			Role:      "repeater",
			Lat:       n.lat,
			Lon:       n.lon,
			RxTime:    recent,
			IngestNow: recent,
		}); err != nil {
			t.Fatalf("seed %s: %v", n.name, err)
		}
	}

	obsIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        scopeKey(40),
		Name:      "Collision Observer",
		IATA:      "SFO",
		Meta:      nil,
		LastSeen:  recent,
		IngestNow: recent,
	})
	if err != nil {
		t.Fatalf("seed observer: %v", err)
	}
	sc.observerIdx = obsIdx

	cfg := &Config{Port: 3000}
	hub := NewHub()
	srv := NewServer(db, cfg, hub)
	sc.srv = srv

	return sc
}

// reloadStore (re)builds the PacketStore from the current DB state. Must
// be called AFTER all transmissions/observations are inserted, otherwise
// the store snapshot is empty and queries return nothing.
func (sc *collisionScenario) reloadStore(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	store := newPacketStoreForTest(t, sc.db)
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
	sc.srv.store = store
	router := mux.NewRouter()
	sc.srv.RegisterRoutes(router)
	sc.router = router
}

// query issues GET /api/nodes/{pk}/paths and returns the decoded response.
func (sc *collisionScenario) query(t *testing.T, pk string) NodePathsResponse {
	t.Helper()
	req := httptest.NewRequest("GET", "/api/nodes/"+pk+"/paths", nil)
	w := httptest.NewRecorder()
	sc.router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /paths for %s: code=%d body=%s", pk, w.Code, w.Body.String())
	}
	var resp NodePathsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return resp
}

// TestHandleNodePaths_PrefixCollision_1352 reproduces issue #1352.
//
// Setup: 3 nodes share 2-char prefix "c0":
//
//	A = c0dedad4...  (no GPS)
//	B = c0ffeec7...  (HAS GPS @ SF)   — canonical relay per resolved_path
//	C = c0efb77f...  (no GPS)
//
// A packet observed with raw path ["c0"] has a CANONICAL resolved_path
// that names B (c0ffeec7…) — produced by the hop-disambiguator using
// observer context. The query for paths-through-X must use the canonical
// resolved_path to decide membership, NOT a naive prefix lookup.
//
// Only B is in the canonical resolved_path; only paths-through-B
// must include the tx. paths-through-A and paths-through-C must exclude it.
func TestHandleNodePaths_PrefixCollision_1352(t *testing.T) {
	ctx := t.Context()
	sc := setupCollisionScenario(t, false)

	txID, _, err := sc.db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		Hash:        "1352a00000000042",
		RawHex:      "dead",
		RouteType:   1,
		PayloadType: 4,
		DecodedJSON: "{}",
	}, sc.recent)
	if err != nil {
		t.Fatalf("seed transmission: %v", err)
	}
	_, inserted, err := sc.db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: txID,
		ObserverIdx:    iptr64(sc.observerIdx),
		SNR:            fptr(5.0),
		RSSI:           fptr(-90.0),
		Timestamp:      sc.recent,
	}, []string{"c0"}, []*string{sptr(sc.nodeBPK)})
	if err != nil {
		t.Fatalf("seed observation: %v", err)
	}
	if !inserted {
		t.Fatalf("expected observation to be inserted for tx %d", txID)
	}
	sc.reloadStore(t)

	respA := sc.query(t, sc.nodeAPK)
	respB := sc.query(t, sc.nodeBPK)
	respC := sc.query(t, sc.nodeCPK)

	if respA.TotalTransmissions != 0 {
		t.Errorf("nodeA (c0dedad…) paths-through: canonical resolved_path names B, not A — "+
			"expected 0 transmissions, got %d (wrong-node attribution #1352)",
			respA.TotalTransmissions)
	}
	if respC.TotalTransmissions != 0 {
		t.Errorf("nodeC (c0efb77…) paths-through: canonical resolved_path names B, not C — "+
			"expected 0 transmissions, got %d (wrong-node attribution #1352)",
			respC.TotalTransmissions)
	}
	if respB.TotalTransmissions != 1 {
		t.Errorf("nodeB (c0ffeec…) paths-through: B is canonical relay — "+
			"expected 1 transmission, got %d", respB.TotalTransmissions)
	}
}

// TestHandleNodePaths_PrefixCollision_1352_FallbackBranch covers the
// worse case: obs has NO persisted resolved_path. The OLD fallback branch
// invoked pm.resolveWithContext(hop, []string{lowerPK}, graph) — anchoring
// the resolver on the queried node. Tier-2 (geo_proximity) then picked
// the GPS candidate closest to the centroid of context (== the target
// itself when the target has GPS), causing every paths-through-X query
// that shared the prefix to return the tx with X attribution.
//
// Fix: with multiple "c0" candidates and no SQL/index pre-confirmation,
// the colliders must sum to AT MOST 1 (ideally 0). Old buggy code:
// all three = 3. Fixed: ≤1, and we tighten further to ≤1 explicitly.
func TestHandleNodePaths_PrefixCollision_1352_FallbackBranch(t *testing.T) {
	ctx := t.Context()
	sc := setupCollisionScenario(t, true)

	txID, _, err := sc.db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		Hash:        "1352b00000000043",
		RawHex:      "beef",
		RouteType:   1,
		PayloadType: 4,
		DecodedJSON: "{}",
	}, sc.recent)
	if err != nil {
		t.Fatalf("seed transmission: %v", err)
	}
	_, inserted, err := sc.db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: txID,
		ObserverIdx:    iptr64(sc.observerIdx),
		SNR:            fptr(5.0),
		RSSI:           fptr(-90.0),
		Timestamp:      sc.recent,
	}, []string{"c0"}, []*string{nil})
	if err != nil {
		t.Fatalf("seed observation: %v", err)
	}
	if !inserted {
		t.Fatalf("expected observation to be inserted for tx %d", txID)
	}
	sc.reloadStore(t)

	a := sc.query(t, sc.nodeAPK).TotalTransmissions
	b := sc.query(t, sc.nodeBPK).TotalTransmissions
	c := sc.query(t, sc.nodeCPK).TotalTransmissions

	sum := a + b + c
	if sum > 1 {
		t.Errorf("ambiguous-prefix tx with NULL resolved_path attributed to %d nodes total (A=%d B=%d C=%d); "+
			"expected sum ≤ 1 — paths-through must not return the same tx for multiple sibling prefix collisions (#1352)",
			sum, a, b, c)
	}
}

// TestHandleNodePaths_FallbackUniquePrefix_1352 is the POSITIVE companion
// to FallbackBranch: a hop prefix that has EXACTLY ONE candidate node MUST
// attribute the tx when that hop resolves to the queried target.
//
// Without this test, the "all zero" degenerate implementation passes the
// ≤1 fallback assertion vacuously. This locks in that the
// `len(pm.m[lowerHop]) <= 1` guard does NOT over-reject unique prefixes.
//
// Setup: only ONE node has the prefix "ab". NULL resolved_path so we take
// the fallback branch. paths-through-target MUST include exactly 1 tx.
func TestHandleNodePaths_FallbackUniquePrefix_1352(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	recent := time.Now().UTC().Add(-1 * time.Hour)
	pk := "abcdef0123456789" + strings.Repeat("0", 48)

	if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: pk,
		Name:      "UniqueNode",
		Role:      "repeater",
		Lat:       fptr(37.78),
		Lon:       fptr(-122.4),
		RxTime:    recent,
		IngestNow: recent,
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}
	obsIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        scopeKey(41),
		Name:      "Unique Observer",
		IATA:      "SFO",
		Meta:      nil,
		LastSeen:  recent,
		IngestNow: recent,
	})
	if err != nil {
		t.Fatalf("seed observer: %v", err)
	}
	txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		Hash:        "1352c00000000044",
		RawHex:      "cafe",
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
	}, []string{"ab"}, []*string{nil})
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
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache(): %v", err)
	}
	srv.store = store
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	req := httptest.NewRequest("GET", "/api/nodes/"+pk+"/paths", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /paths: code=%d body=%s", w.Code, w.Body.String())
	}
	var resp NodePathsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.TotalTransmissions != 1 {
		t.Errorf("unique-prefix hop with NULL resolved_path: target attribution "+
			"MUST be exactly 1, got %d — `len(pm.m[lowerHop]) <= 1` guard is "+
			"over-rejecting unambiguous prefixes (#1352)", resp.TotalTransmissions)
	}
}

// TestHandleNodePaths_FallbackPreconfirmed_1352 exercises the
// pre-confirmation path: when a tx is in confirmedByFullKey OR
// confirmedBySQL for the queried target, attribution MUST survive
// regardless of any sibling-prefix ambiguity.
//
// Mutation note (pushback recorded in PR body): in the current
// code shape, containsTarget is initialized to
// `confirmedByFullKey[tx.ID] || confirmedBySQL[tx.ID]` BEFORE the
// per-hop loop runs, and the loop only ever flips false→true. So
// removing the `preconfirmed ||` clause alone does not break this
// test — the preconfirmed tx is already attributed via the
// initialization. The `preconfirmed` snapshot is kept as a
// structural invariant (see routes.go comment): it documents the
// contract that the SQL/index signal must NEVER be silently
// overridden by a biased-resolver false-negative in a future edit
// that flips containsTarget back to false inside the loop. This
// test guards the BEHAVIOR ("preconfirmed survives ambiguous
// prefix") even if it can't currently mutation-detect every
// formulation of the structural guard.
func TestHandleNodePaths_FallbackPreconfirmed_1352(t *testing.T) {
	ctx := t.Context()
	sc := setupCollisionScenario(t, true)

	secondObsIdx, err := sc.db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        scopeKey(42),
		Name:      "Second Collision Observer",
		IATA:      "OAK",
		Meta:      nil,
		LastSeen:  sc.recent,
		IngestNow: sc.recent,
	})
	if err != nil {
		t.Fatalf("seed second observer: %v", err)
	}

	txID, _, err := sc.db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		Hash:        "1352d00000000050",
		RawHex:      "f00d",
		RouteType:   1,
		PayloadType: 4,
		DecodedJSON: "{}",
	}, sc.recent)
	if err != nil {
		t.Fatalf("seed transmission: %v", err)
	}
	_, inserted, err := sc.db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: txID,
		ObserverIdx:    iptr64(sc.observerIdx),
		SNR:            fptr(5.0),
		RSSI:           fptr(-90.0),
		Timestamp:      sc.recent,
	}, []string{"c0"}, []*string{nil})
	if err != nil {
		t.Fatalf("seed first observation: %v", err)
	}
	if !inserted {
		t.Fatalf("expected observation to be inserted for tx %d", txID)
	}
	_, inserted, err = sc.db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: txID,
		ObserverIdx:    iptr64(secondObsIdx),
		SNR:            fptr(6.0),
		RSSI:           fptr(-89.0),
		Timestamp:      sc.recent.Add(time.Second),
	}, []string{"c0"}, []*string{sptr(sc.nodeBPK)})
	if err != nil {
		t.Fatalf("seed second observation: %v", err)
	}
	if !inserted {
		t.Fatalf("expected observation to be inserted for tx %d", txID)
	}
	sc.reloadStore(t)

	respA := sc.query(t, sc.nodeAPK)
	respB := sc.query(t, sc.nodeBPK)
	respC := sc.query(t, sc.nodeCPK)

	if respB.TotalTransmissions != 1 {
		t.Errorf("nodeB preconfirmed via byPathHop/SQL: tx MUST attribute despite "+
			"multi-candidate `c0` prefix — got %d, expected 1. The SQL/index "+
			"pre-confirmation path is the documented contract for #1352. "+
			"If this fails, either the byPathHop full-pubkey index is not being "+
			"populated from persisted resolved_path, or containsTarget is being "+
			"reset inside the per-hop loop.", respB.TotalTransmissions)
	}
	if respA.TotalTransmissions != 0 {
		t.Errorf("nodeA not preconfirmed, prefix ambiguous: expected 0, got %d", respA.TotalTransmissions)
	}
	if respC.TotalTransmissions != 0 {
		t.Errorf("nodeC not preconfirmed, prefix ambiguous: expected 0, got %d", respC.TotalTransmissions)
	}
}

// TestHandleNodePaths_FallbackUnresolvableHop_1352 documents the
// behavior of the unresolvable-hop arm under multi-candidate prefix:
// when resolveHop returns nil (prefix not indexed by pm) AND the hop
// IS a prefix of the queried target, attribution must NOT happen
// without SQL/index pre-confirmation.
//
// Implementation reality (pushback recorded in PR body): the
// unresolvable arm is only reached when pm.m[lowerHop] is empty —
// resolveWithContext returns non-nil whenever len(candidates) >= 1.
// So in practice the arm's `len(pm.m[lowerHop]) <= 1` guard is
// always-true and structurally cannot be mutation-detected by a
// multi-candidate setup. This test instead asserts the BEHAVIOR
// (no attribution under an ambiguous + unresolvable scenario)
// and serves as a regression seat-belt for future edits to
// resolveWithContext that might start returning nil for len>=1.
func TestHandleNodePaths_FallbackUnresolvableHop_1352(t *testing.T) {
	ctx := t.Context()
	sc := setupCollisionScenario(t, false)

	txID, _, err := sc.db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		Hash:        "1352e00000000060",
		RawHex:      "feed",
		RouteType:   1,
		PayloadType: 4,
		DecodedJSON: "{}",
	}, sc.recent)
	if err != nil {
		t.Fatalf("seed transmission: %v", err)
	}
	_, inserted, err := sc.db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: txID,
		ObserverIdx:    iptr64(sc.observerIdx),
		SNR:            fptr(5.0),
		RSSI:           fptr(-90.0),
		Timestamp:      sc.recent,
	}, []string{"c0"}, []*string{nil})
	if err != nil {
		t.Fatalf("seed observation: %v", err)
	}
	if !inserted {
		t.Fatalf("expected observation to be inserted for tx %d", txID)
	}
	sc.reloadStore(t)

	respA := sc.query(t, sc.nodeAPK)
	if respA.TotalTransmissions != 0 {
		t.Errorf("nodeA (no GPS) with multi-candidate `c0` prefix + NULL resolved_path: "+
			"expected 0 attribution, got %d (#1352)", respA.TotalTransmissions)
	}
	respC := sc.query(t, sc.nodeCPK)
	if respC.TotalTransmissions != 0 {
		t.Errorf("nodeC (no GPS) with multi-candidate `c0` prefix + NULL resolved_path: "+
			"expected 0 attribution, got %d (#1352)", respC.TotalTransmissions)
	}
}
