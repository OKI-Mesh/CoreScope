package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/gorilla/mux"
)

func TestAcquireScanSlot_BlocksAtLimitAndReleases(t *testing.T) {
	s := &Server{}
	releases := make([]func(), 0, reachMaxConcurrentScans)
	for i := 0; i < reachMaxConcurrentScans; i++ {
		release, err := s.acquireScanSlot(context.Background())
		if err != nil {
			t.Fatalf("slot %d: %v", i, err)
		}
		releases = append(releases, release)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := s.acquireScanSlot(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("full slots: err = %v, want DeadlineExceeded", err)
	}

	releases[0]()
	release, err := s.acquireScanSlot(context.Background())
	if err != nil {
		t.Fatalf("after release: %v", err)
	}
	release()
	releases[1]()
}

func TestAttributeDirections_LowercaseTokensAndPaths(t *testing.T) {
	ourTokens := map[string]bool{"1000": true}
	resolve := func(tok string) string {
		switch tok {
		case "aaaa":
			return "pk-a"
		case "bbbb":
			return "pk-b"
		}
		return ""
	}
	rows := []pathRow{
		{observerPK: "obs1", fromPubkey: "pk-from", payloadType: types.PayloadADVERT, path: []string{"aaaa", "1000", "bbbb"}},
		{observerPK: "obs1", fromPubkey: "pk-from", payloadType: types.PayloadADVERT, path: []string{"1000"}, snr: 5, snrValid: true},
		{observerPK: "obs2", fromPubkey: "pk-from", payloadType: types.PayloadADVERT, path: []string{"cccc"}},
	}

	d := attributeDirections(rows, ourTokens, "pk-self", resolve)

	if d.relay != 2 {
		t.Errorf("relay = %d, want 2", d.relay)
	}
	if d.we["pk-a"] != 1 || d.we["pk-from"] != 1 {
		t.Errorf("we = %v, want pk-a:1 and pk-from:1", d.we)
	}
	if d.they["pk-b"] != 1 || d.they["obs1"] != 1 {
		t.Errorf("they = %v, want pk-b:1 and obs1:1", d.they)
	}
	got := d.obs["obs1"]
	if got.count != 1 || got.snrN != 1 || got.snrSum != 5 {
		t.Errorf("obs[obs1] = %+v, want count 1, snrN 1, snrSum 5", got)
	}
	if _, ok := d.obs["obs2"]; ok {
		t.Errorf("obs2 appeared in obs though its path never touched our token")
	}
}

func TestScanReachRows_ReportsTruncationOnlyPastTheCap(t *testing.T) {
	defer func(orig int) { reachScanRowLimit = orig }(reachScanRowLimit)
	db := reachFixtureDB(t, 3, 1)
	srv := &Server{db: db}
	tokens := map[string]bool{"01fa": true}

	cases := []struct {
		limit         int
		wantRows      int
		wantTruncated bool
	}{
		{limit: 2, wantRows: 2, wantTruncated: true},
		{limit: 3, wantRows: 3, wantTruncated: false},
		{limit: 10, wantRows: 3, wantTruncated: false},
	}
	for _, c := range cases {
		reachScanRowLimit = c.limit
		rows, truncated, err := srv.scanReachRows(t.Context(), tokens, time.Time{})
		if err != nil {
			t.Fatalf("limit %d: %v", c.limit, err)
		}
		if len(rows) != c.wantRows || truncated != c.wantTruncated {
			t.Errorf("limit %d: got %d rows truncated=%v, want %d rows truncated=%v",
				c.limit, len(rows), truncated, c.wantRows, c.wantTruncated)
		}
	}
}

// seedReachObservations adds count more transmissions, each observed once with
// a path through the target node's 01fa token, so the reach scan matches more
// rows than the integration fixture's single observation.
func seedReachObservations(t *testing.T, db *database.DB, count int) {
	t.Helper()
	ctx := t.Context()
	now := time.Now().UTC()
	observerIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        "obs-extra",
		Name:      "obs-extra",
		LastSeen:  now,
		IngestNow: now,
	})
	if err != nil {
		t.Fatalf("seed observer: %v", err)
	}
	hashes := []string{"reach-extra-1", "reach-extra-2", "reach-extra-3", "reach-extra-4"}
	for _, hash := range hashes[:count] {
		txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
			Hash:        hash,
			RawHex:      "AA",
			RouteType:   types.RouteType(1),
			PayloadType: types.PayloadType(5),
		}, now)
		if err != nil {
			t.Fatalf("seed tx %s: %v", hash, err)
		}
		_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
			TransmissionID: txID,
			ObserverIdx:    ptr(observerIdx),
			Direction:      ptr("RX"),
			SNR:            ptr(-7.0),
			Timestamp:      now,
		}, []string{"aabb", "01fa", "ccdd"}, nil)
		if err != nil {
			t.Fatalf("seed observation %s: %v", hash, err)
		}
		if !inserted {
			t.Fatalf("expected observation for tx %d to be inserted", txID)
		}
	}
}

func reachBodyFor(t *testing.T, srv *Server, pubkey string) (api.ReachWindow, []byte) {
	t.Helper()
	rr := serveReach(srv, "/api/nodes/"+pubkey+"/reach?days=30")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", rr.Code, rr.Body.String())
	}
	var resp api.NodeReachResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return resp.Window, rr.Body.Bytes()
}

func TestNodeReach_WindowTruncatedFlag(t *testing.T) {
	defer func(orig int) { reachScanRowLimit = orig }(reachScanRowLimit)
	db, n := newReachIntegrationDB(t, []string{"aabb", "01fa", "ccdd"})
	seedReachObservations(t, db, 3)
	cfg := &Config{}
	srv := &Server{store: newTestStoreWithDB(t, db, cfg), db: db, cfg: cfg, perfStats: NewPerfStats()}

	cases := []struct {
		limit      int
		want       bool
		wantOnWire string
	}{
		{limit: 2, want: true, wantOnWire: "true"},
		{limit: 4, want: false, wantOnWire: "false"},
	}
	for _, c := range cases {
		reachScanRowLimit = c.limit
		resetReachState(t, srv)
		w, body := reachBodyFor(t, srv, n)
		if w.Truncated != c.want {
			t.Errorf("4 observations matched a cap of %d: Truncated = %v, want %v", c.limit, w.Truncated, c.want)
		}
		var raw struct {
			Window map[string]json.RawMessage `json:"window"`
		}
		if err := json.Unmarshal(body, &raw); err != nil {
			t.Fatalf("decode raw: %v", err)
		}
		if got := string(raw.Window["truncated"]); got != c.wantOnWire {
			t.Errorf("cap %d: window.truncated on the wire = %q, want %q (the key must always be present)", c.limit, got, c.wantOnWire)
		}
	}
}

func TestNodeReach_CancelledFirstCallerDoesNotFailOtherWaiters(t *testing.T) {
	db, n := newReachIntegrationDB(t, []string{"aabb", "01fa", "ccdd"})
	cfg := &Config{}
	srv := &Server{store: newTestStoreWithDB(t, db, cfg), db: db, cfg: cfg, perfStats: NewPerfStats()}
	resetReachState(t, srv)

	// Hold every scan slot so the shared computation blocks inside the scan.
	releases := make([]func(), 0, reachMaxConcurrentScans)
	for i := 0; i < reachMaxConcurrentScans; i++ {
		release, err := srv.acquireScanSlot(t.Context())
		if err != nil {
			t.Fatalf("hold slot %d: %v", i, err)
		}
		releases = append(releases, release)
	}

	router := mux.NewRouter()
	router.HandleFunc("/api/nodes/{pubkey}/reach", srv.handleNodeReach).Methods("GET")
	path := "/api/nodes/" + n + "/reach?days=30"

	firstCtx, cancelFirst := context.WithCancel(t.Context())
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", path, nil).WithContext(firstCtx))
	}()
	time.Sleep(200 * time.Millisecond)
	cancelFirst()
	select {
	case <-firstDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("the cancelled caller must return promptly instead of waiting for the scan")
	}

	second := httptest.NewRecorder()
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		router.ServeHTTP(second, httptest.NewRequest("GET", path, nil))
	}()
	time.Sleep(200 * time.Millisecond)
	for _, release := range releases {
		release()
	}
	select {
	case <-secondDone:
	case <-time.After(10 * time.Second):
		t.Fatalf("second caller never finished after the slots were released")
	}
	if second.Code != http.StatusOK {
		t.Fatalf("second caller status = %d, want 200 (the first caller's cancel must not fail it); body=%s",
			second.Code, second.Body.String())
	}
}

func TestGetDegreeSnapshot_ColdStartLoadsAndStores(t *testing.T) {
	db := setupTestDB(t)
	srv := &Server{db: db}

	snap := srv.getDegreeSnapshot(t.Context())
	if snap == nil {
		t.Fatalf("cold start must load a snapshot, got nil")
	}
	srv.reach.degreeMu.Lock()
	stored := srv.reach.degreeSnap
	srv.reach.degreeMu.Unlock()
	if stored != snap {
		t.Fatalf("the loaded snapshot must be the stored one")
	}
}

func waitForNewDegreeSnapshot(t *testing.T, srv *Server, old *degreeSnapshot) *degreeSnapshot {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		srv.reach.degreeMu.Lock()
		cur := srv.reach.degreeSnap
		srv.reach.degreeMu.Unlock()
		if cur != old {
			return cur
		}
		if time.Now().After(deadline) {
			t.Fatalf("snapshot was not replaced within 5s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestGetDegreeSnapshot_ServesStaleThenRefreshesInBackground(t *testing.T) {
	db := setupTestDB(t)
	srv := &Server{db: db}
	key := pk64("01fa")
	stale := &degreeSnapshot{
		at:         time.Now().Add(-2 * reachDegreeTTL),
		total:      1,
		deg:        map[string]int{key: 3},
		sortedDesc: []int{3},
	}
	srv.reach.degreeMu.Lock()
	srv.reach.degreeSnap = stale
	srv.reach.degreeMu.Unlock()

	if got := srv.getDegreeSnapshot(t.Context()); got != stale {
		t.Fatalf("a stale snapshot must be served immediately, not replaced inline")
	}

	fresh := waitForNewDegreeSnapshot(t, srv, stale)
	if time.Since(fresh.at) >= reachDegreeTTL {
		t.Errorf("refreshed snapshot is already stale: age %v", time.Since(fresh.at))
	}
	if fresh.deg[key] != 0 {
		t.Errorf("refreshed snapshot still holds the stale degree %d for %s", fresh.deg[key], key)
	}
}

func TestGetDegreeSnapshot_FailedRefreshKeepsStaleAndDelaysRetry(t *testing.T) {
	db := setupTestDB(t)
	db.Close()
	srv := &Server{db: db}
	key := pk64("01fa")
	stale := &degreeSnapshot{
		at:         time.Now().Add(-2 * reachDegreeTTL),
		total:      1,
		deg:        map[string]int{key: 3},
		sortedDesc: []int{3},
	}
	srv.reach.degreeMu.Lock()
	srv.reach.degreeSnap = stale
	srv.reach.degreeMu.Unlock()

	if got := srv.getDegreeSnapshot(t.Context()); got != stale {
		t.Fatalf("a stale snapshot must be served immediately")
	}

	kept := waitForNewDegreeSnapshot(t, srv, stale)
	if kept.deg[key] != 3 || kept.total != 1 {
		t.Errorf("failed refresh must keep the stale data, got total=%d deg=%d", kept.total, kept.deg[key])
	}
	age := time.Since(kept.at)
	if age >= reachDegreeTTL || age < reachDegreeTTL-reachDegreeRetry {
		t.Errorf("retry must be about %v away: age = %v, want between %v and %v",
			reachDegreeRetry, age, reachDegreeTTL-reachDegreeRetry, reachDegreeTTL)
	}
}
