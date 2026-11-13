package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/gorilla/mux"
)

func serveReach(srv *Server, path string) *httptest.ResponseRecorder {
	router := mux.NewRouter()
	router.HandleFunc("/api/nodes/{pubkey}/reach", srv.handleNodeReach).Methods("GET")
	req := httptest.NewRequest("GET", path, nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	return rr
}

// pk64 pads a short hex stem to a full 64-char lowercase pubkey.
func pk64(stem string) string { return stem + strings.Repeat("0", 64-len(stem)) }

// resetReachState clears the per-server reach caches so test order cannot
// leak observable state between handler tests (and restores after the test).
// Now operates on *Server (was package globals — Independent r2 #2); accepts
// a variadic *Server so existing call sites that didn't pass one still
// compile but the reset is a no-op (used by tests that build the Server
// fresh and don't need state cleared).
func resetReachState(t *testing.T, servers ...*Server) {
	t.Helper()
	clear := func() {
		for _, s := range servers {
			if s == nil {
				continue
			}
			s.reach.cacheMu.Lock()
			s.reach.cache = map[string]reachCacheEntry{}
			s.reach.cacheMu.Unlock()
			s.reach.degreeMu.Lock()
			s.reach.degreeSnap = nil
			s.reach.degreeMu.Unlock()
		}
	}
	clear()
	t.Cleanup(clear)
}

// newReachIntegrationDB builds a complete observer_idx-schema DB with a target
// node N, two neighbours A/B, and one observation on obsPath so the HTTP handler
// exercises real directional attribution. Pass a path that omits N's token to
// build the zero-reach case (identifiable node, no matching observations).
func newReachIntegrationDB(t *testing.T, obsHops []string) (*database.DB, string) {
	t.Helper()
	ctx := t.Context()
	db := setupTestDB(t)

	n := pk64("01fa")
	a := pk64("aabb")
	b := pk64("ccdd")
	now := time.Now().UTC()
	firstSeen := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	lastSeen := time.Date(2026, 6, 7, 0, 0, 0, 0, time.UTC)

	for _, node := range []struct {
		pubkey  string
		name    string
		lat     float64
		lon     float64
		adverts int
	}{
		{n, "N", 50.9, 5.4, 3},
		{a, "A", 51.0, 5.5, 1},
		{b, "B", 51.1, 5.6, 1},
	} {
		for i := 0; i < node.adverts; i++ {
			rx := firstSeen
			if i == node.adverts-1 {
				rx = lastSeen
			}
			if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
				PublicKey: node.pubkey,
				Name:      node.name,
				Role:      "repeater",
				Lat:       fptr(node.lat),
				Lon:       fptr(node.lon),
				RxTime:    rx,
				IngestNow: now,
			}); err != nil {
				t.Fatalf("seed node %s (advert %d): %v", node.name, i+1, err)
			}
		}
	}

	observerIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        "obs1",
		Name:      ptr("OBS1"),
		LastSeen:  now,
		IngestNow: now,
	})
	if err != nil {
		t.Fatalf("seed observer: %v", err)
	}

	txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		Hash:        "reach-integration-tx",
		RawHex:      "AA",
		RouteType:   types.RouteType(1),
		PayloadType: types.PayloadType(5),
	}, now)
	if err != nil {
		t.Fatalf("seed tx: %v", err)
	}
	_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: txID,
		ObserverIdx:    ptr(observerIdx),
		Direction:      ptr("RX"),
		SNR:            ptr(-7.0),
		Timestamp:      now,
	}, obsHops, nil)
	if err != nil {
		t.Fatalf("seed observation: %v", err)
	}
	if !inserted {
		t.Fatalf("expected observation to be inserted for tx %d", txID)
	}
	return db, n
}

func TestClampDays(t *testing.T) {
	cases := []struct{ in, want int }{{0, 1}, {-5, 1}, {1, 1}, {7, 7}, {30, 30}, {31, 30}, {999, 30}}
	for _, c := range cases {
		if got := clampDays(c.in); got != c.want {
			t.Errorf("clampDays(%d)=%d want %d", c.in, got, c.want)
		}
	}
}

func TestNodeReach_UnknownNode(t *testing.T) {
	srv := makeTestServer(makeTestGraph()) // no store/db wired → 404
	rr := serveReach(srv, "/api/nodes/"+pk64("deadbeef")+"/reach")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status=%d want 404", rr.Code)
	}
}

func TestNodeReach_InvalidPubkey(t *testing.T) {
	srv := makeTestServer(makeTestGraph())
	for _, bad := range []string{"deadbeef", "xyz", pk64("01") + "zz"} {
		rr := serveReach(srv, "/api/nodes/"+bad+"/reach")
		if rr.Code != http.StatusBadRequest {
			t.Errorf("pubkey %q: status=%d want 400", bad, rr.Code)
		}
	}
}

func TestNodeReach_ValidPubkeyNotInNodes(t *testing.T) {
	resetReachState(t)
	db := setupTestDB(t)
	cfg := &Config{}
	srv := &Server{store: newTestStoreWithDB(t, db, cfg), db: db, cfg: cfg, perfStats: NewPerfStats()}
	rr := serveReach(srv, "/api/nodes/"+pk64("beef")+"/reach")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status=%d want 404 (body=%s)", rr.Code, rr.Body.String())
	}
}

func TestNodeReach_BlacklistedReturns404(t *testing.T) {
	pk := pk64("01fa")
	cfg := &Config{NodeBlacklist: []string{pk}}
	srv := &Server{cfg: cfg}
	rr := serveReach(srv, "/api/nodes/"+pk+"/reach")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("blacklisted pubkey: status=%d want 404", rr.Code)
	}
}

func TestNodeReach_AttributionAndCacheHit(t *testing.T) {
	resetReachState(t)
	db, n := newReachIntegrationDB(t, []string{"aabb", "01fa", "ccdd"})
	cfg := &Config{}
	srv := &Server{store: newTestStoreWithDB(t, db, cfg), db: db, cfg: cfg, perfStats: NewPerfStats()}

	rr := serveReach(srv, "/api/nodes/"+n+"/reach?days=30")
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 (body=%s)", rr.Code, rr.Body.String())
	}
	var resp api.NodeReachResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	if resp.Importance.RelayObservations < 1 {
		t.Fatalf("expected ≥1 relay observation, got %d", resp.Importance.RelayObservations)
	}
	var weHearA, theyHearB bool
	for _, l := range resp.Links {
		if l.Name == "A" && l.WeHear >= 1 {
			weHearA = true
		}
		if l.Name == "B" && l.TheyHear >= 1 {
			theyHearB = true
		}
	}
	if !weHearA {
		t.Errorf("expected we_hear≥1 for neighbour A, links=%+v", resp.Links)
	}
	if !theyHearB {
		t.Errorf("expected they_hear≥1 for neighbour B, links=%+v", resp.Links)
	}

	// Cache hit: the key (now generation-suffixed, #1629) must be populated
	// and a second request must 200.
	wantKey := n + "|30|g" + strconv.FormatUint(srv.cfg.BlacklistGeneration(), 10)
	if _, ok := srv.reachCacheGet(wantKey); !ok {
		t.Fatalf("expected reach response to be cached under %q", wantKey)
	}
	rr2 := serveReach(srv, "/api/nodes/"+n+"/reach?days=30")
	if rr2.Code != http.StatusOK || rr2.Body.String() != rr.Body.String() {
		t.Fatalf("cache-hit response differs: code=%d", rr2.Code)
	}
}

// Zero-reach happy path: a node that IS identifiable (has reliable tokens) but
// whose observations contain none of its tokens must return 200 with empty
// arrays — NOT 404. A wrong implementation that 404s here passes every other
// test. (docs/api-spec.md contract.)
func TestNodeReach_ZeroReach(t *testing.T) {
	resetReachState(t)
	db, n := newReachIntegrationDB(t, []string{"aabb", "ccdd"}) // path omits N's "01fa" token
	cfg := &Config{}
	srv := &Server{store: newTestStoreWithDB(t, db, cfg), db: db, cfg: cfg, perfStats: NewPerfStats()}

	nodeMap := srv.buildNodeInfoMap(t.Context())
	_, inMap := nodeMap[n]
	t.Logf("n=%s inMap=%v hidden=%v blacklisted=%v", n, inMap, srv.isPubkeyHidden(t.Context(), n), cfg.IsBlacklisted(n))

	rr := serveReach(srv, "/api/nodes/"+n+"/reach?days=30")
	if rr.Code != http.StatusOK {
		t.Fatalf("zero-reach must be 200 not 404, got %d (body=%s)", rr.Code, rr.Body.String())
	}
	var resp api.NodeReachResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	if len(resp.ReliableTokens) == 0 {
		t.Fatalf("node should still be identifiable (reliable tokens present)")
	}
	if len(resp.Links) != 0 || len(resp.DirectObservers) != 0 || resp.Importance.RelayObservations != 0 {
		t.Fatalf("expected empty reach, got links=%d obs=%d relay=%d",
			len(resp.Links), len(resp.DirectObservers), resp.Importance.RelayObservations)
	}
}

func TestNodeReach_ShapeAndClamp(t *testing.T) {
	ctx := t.Context()
	resetReachState(t)
	db := setupTestDB(t)
	const pk = "01fa326b475800a31105abcb9e4cac000b3e5d9e2b5ba0739981ce8d5f3a6754"
	now := time.Now().UTC()

	for i, rx := range []time.Time{
		time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 6, 7, 0, 0, 0, 0, time.UTC),
	} {
		if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
			PublicKey: pk,
			Name:      "BE-Test",
			Role:      "repeater",
			Lat:       fptr(50.9),
			Lon:       fptr(5.4),
			RxTime:    rx,
			IngestNow: now,
		}); err != nil {
			t.Fatalf("seed node (advert %d): %v", i+1, err)
		}
	}

	cfg := &Config{}
	srv := &Server{store: newTestStoreWithDB(t, db, cfg), db: db, cfg: cfg, perfStats: NewPerfStats()}

	rr := serveReach(srv, "/api/nodes/"+pk+"/reach?days=999")
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 (body=%s)", rr.Code, rr.Body.String())
	}
	var resp api.NodeReachResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	if resp.Window.Days != 30 {
		t.Fatalf("days not clamped to 30: %d", resp.Window.Days)
	}
	if resp.Links == nil || resp.DirectObservers == nil || resp.ReliableTokens == nil {
		t.Fatalf("array fields must be non-nil (never null)")
	}
	if !contains(resp.ReliableTokens, "01FA") {
		t.Fatalf("expected 01FA reliable token, got %v", resp.ReliableTokens)
	}
	if resp.Node.FirstSeen == nil || !resp.Node.FirstSeen.Equal(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("first_seen not sourced from nodes table: %v", resp.Node.FirstSeen)
	}
}

// Issue #1631: a DB failure inside scanReachRows must surface as 500, not
// as a misleading "no reach" 200 or 404. We warm the integration DB, drop
// the observations table so the next reach scan query fails inside
// QueryContext, then assert the handler returns 500 (not 200 with empty
// arrays, which is the buggy current behavior — scanReachRows swallows the
// error and returns nil).
func TestNodeReach_ScanDBErrorReturns500(t *testing.T) {
	resetReachState(t)
	db, n := newReachIntegrationDB(t, []string{"aabb", "01fa", "ccdd"})
	cfg := &Config{}
	srv := &Server{store: newTestStoreWithDB(t, db, cfg), db: db, cfg: cfg, perfStats: NewPerfStats()}

	if rr := serveReach(srv, "/api/nodes/"+n+"/reach?days=30"); rr.Code != http.StatusOK {
		t.Fatalf("warm-up call: status=%d want 200 (body=%s)", rr.Code, rr.Body.String())
	}
	srv.reach.cacheMu.Lock()
	srv.reach.cache = map[string]reachCacheEntry{}
	srv.reach.cacheMu.Unlock()

	db.Close()

	rr := serveReach(srv, "/api/nodes/"+n+"/reach?days=30")
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on DB error inside scanReachRows, got %d (body=%s)", rr.Code, rr.Body.String())
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
