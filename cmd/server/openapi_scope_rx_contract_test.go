package main

import (
	"net/http"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/gorilla/mux"
)

// rxEnabledServer builds a server with client RX coverage switched on and a
// few receptions in the database: two clients, three heard nodes, one of them
// a known node.
func rxEnabledServer(t *testing.T) (*Server, *mux.Router) {
	t.Helper()
	db := setupTestDB(t)
	now := time.Now().UTC()

	compA := padKey("c0a0000000000001")
	compB := padKey("c0b0000000000002")
	seedNode(t, db, nodePK, "Heard Node", "repeater", nil, nil, now)

	insRx(t, db, compA, "aabbcc", now, 51.05, 3.72)
	insRx(t, db, compA, "aabbcc", now.Add(-time.Minute), 51.05001, 3.72001)
	insRx(t, db, compA, "ddee00", now, 51.06, 3.73)
	insRx(t, db, compB, "aabbcc", now, 51.05, 3.72)
	insRx(t, db, compB, "ffeedd", now, 52.5, 4.5)

	cfg := &Config{Port: 3000, ClientRxCoverage: &ClientRxCoverageConfig{Enabled: true}}
	srv := NewServer(db, cfg, NewHub())
	router := mux.NewRouter()
	srv.RegisterRoutes(router)
	return srv, router
}

func TestRxEndpointsMatchSpec(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := rxEnabledServer(t)

	for _, path := range []string{
		"/api/rx-coverage?bbox=50,3,53,5&z=9",
		"/api/rx-coverage?bbox=50,3,53,5&z=12&days=30",
		"/api/rx-coverage?bbox=50,3,53,5&z=12&node=aabbcc",
		"/api/rx-coverage?bbox=0,0,1,1",
		"/api/nodes/" + nodePK + "/rx-coverage?bbox=50,3,53,5&z=12",
		"/api/rx-leaderboard",
		"/api/rx-leaderboard?days=30&limit=1",
	} {
		t.Run(path, func(t *testing.T) {
			checker.check(t, router, path)
		})
	}
}

func TestRxResponsesDecodeAsSpecTypes(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := rxEnabledServer(t)

	var fc api.CoverageFeatureCollection
	decodeInto(t, checker.checkStatus(t, router, "/api/rx-coverage?bbox=50,3,53,5&z=9", http.StatusOK).Body.Bytes(), &fc)
	if fc.Type != "FeatureCollection" || len(fc.Features) == 0 {
		t.Fatalf("coverage = %+v", fc)
	}
	if fc.MobileReceptions != nil || fc.MobileClients != nil {
		t.Errorf("the global route must not carry the per-node totals: %+v", fc)
	}
	f := fc.Features[0]
	if f.Type != "Feature" || f.Geometry.Type != "Polygon" || len(f.Geometry.Coordinates) != 1 || len(f.Geometry.Coordinates[0]) < 4 {
		t.Errorf("feature = %+v", f)
	}
	for _, pos := range f.Geometry.Coordinates[0] {
		if len(pos) != 2 {
			t.Fatalf("a GeoJSON position must be [lon, lat], got %v", pos)
		}
	}
	if f.Properties.Nodes == nil || f.Properties.BestSnr == nil || !f.Properties.HasSig {
		t.Errorf("properties = %+v", f.Properties)
	}

	// Nothing in view is an empty collection, not null.
	var empty api.CoverageFeatureCollection
	decodeInto(t, checker.checkStatus(t, router, "/api/rx-coverage?bbox=0,0,1,1", http.StatusOK).Body.Bytes(), &empty)
	if empty.Features == nil || len(empty.Features) != 0 {
		t.Errorf("empty coverage must be [] not null: %#v", empty.Features)
	}

	var perNode api.CoverageFeatureCollection
	decodeInto(t, checker.checkStatus(t, router, "/api/nodes/"+nodePK+"/rx-coverage?bbox=50,3,53,5&z=12", http.StatusOK).Body.Bytes(), &perNode)
	if perNode.MobileReceptions == nil || *perNode.MobileReceptions != 3 || perNode.MobileClients == nil || *perNode.MobileClients != 2 {
		t.Errorf("per-node totals = %v/%v, want 3/2", perNode.MobileReceptions, perNode.MobileClients)
	}

	var board api.RxLeaderboardResponse
	decodeInto(t, checker.checkStatus(t, router, "/api/rx-leaderboard?days=30", http.StatusOK).Body.Bytes(), &board)
	if board.Days != 30 || len(board.Observers) != 2 {
		t.Fatalf("leaderboard = %+v", board)
	}
	if board.Observers[0].Score < board.Observers[1].Score {
		t.Errorf("leaderboard is not ranked by score: %+v", board.Observers)
	}
}

func TestRxErrorsMatchSpec(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := rxEnabledServer(t)

	for path, status := range map[string]int{
		"/api/rx-coverage":                                 http.StatusBadRequest,
		"/api/rx-coverage?bbox=1,2,3":                      http.StatusBadRequest,
		"/api/nodes/not-a-pubkey/rx-coverage?bbox=1,2,3,4": http.StatusBadRequest,
		"/api/nodes/" + nodePK + "/rx-coverage":            http.StatusBadRequest,
	} {
		t.Run(path, func(t *testing.T) {
			checker.checkStatus(t, router, path, status)
		})
	}
}

// With the feature off, which is how it ships, every RX route answers 404
// with the standard JSON error.
func TestRxDisabledMatchesSpec(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := setupTestServer(t)

	for _, path := range []string{
		"/api/rx-coverage?bbox=50,3,53,5",
		"/api/rx-leaderboard",
		"/api/nodes/" + nodePK + "/rx-coverage?bbox=50,3,53,5",
	} {
		t.Run(path, func(t *testing.T) {
			rec := checker.checkStatus(t, router, path, http.StatusNotFound)
			var body api.ErrorResponse
			decodeInto(t, rec.Body.Bytes(), &body)
			if body.Error == "" {
				t.Errorf("404 body has no error message: %s", rec.Body.String())
			}
		})
	}
}

func TestScopeStatsMatchesSpec(t *testing.T) {
	checker := newSpecChecker(t)
	ctx := t.Context()
	db := setupTestDB(t)
	now := time.Now().UTC()
	for i, r := range []struct {
		hash  string
		scope *string
		route types.RouteType
	}{
		{"5c0e000000000001", sptr("#belgium"), 0},
		{"5c0e000000000002", sptr("#belgium"), 3},
		{"5c0e000000000003", sptr(""), 0},
		{"5c0e000000000004", nil, 0},
	} {
		if _, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
			Hash: r.hash, RawHex: "aa", RouteType: r.route, PayloadType: 5, DecodedJSON: "{}", ScopeName: r.scope,
		}, now); err != nil {
			t.Fatalf("seed row %d: %v", i, err)
		}
	}
	_, router := serverFor(t, db, loadedStore(t, db))

	for _, path := range []string{
		"/api/scope-stats",
		"/api/scope-stats?window=1h",
		"/api/scope-stats?window=24h",
		"/api/scope-stats?window=7d",
	} {
		t.Run(path, func(t *testing.T) {
			checker.check(t, router, path)
		})
	}
	checker.checkStatus(t, router, "/api/scope-stats?window=1y", http.StatusBadRequest)

	var stats api.ScopeStatsResponse
	decodeInto(t, checker.checkStatus(t, router, "/api/scope-stats?window=24h", http.StatusOK).Body.Bytes(), &stats)
	if stats.Window != "24h" || stats.Summary.TransportTotal != 4 || len(stats.ByRegion) != 1 {
		t.Errorf("stats = %+v", stats)
	}
	if stats.TimeSeries == nil || stats.AdvertsByRole == nil {
		t.Errorf("empty lists must be [] not null: %+v", stats)
	}
}

func TestScopeAuditMatchesSpec(t *testing.T) {
	checker := newSpecChecker(t)
	srv, router := setupScopeAuditServer(t)
	now := time.Now().UTC()
	recent := now.Add(-time.Minute)

	// One repeater with a gap in each direction: it declares be-vlg and never
	// forwards it, and forwards de-nw without declaring it. It also forwards a
	// plain flood while declaring no wildcard.
	pk := testFullPubkeyA
	insertDeclared(t, srv, pk, now, "be,be-vlg", 0)
	seedTransmissionRouteAt(t, srv.store, pk[:4], scopeMatched("#be"), types.RouteTransportFlood, recent)
	seedTransmissionRouteAt(t, srv.store, pk[:4], scopeMatched("#de-nw"), types.RouteTransportFlood, recent)
	seedTransmissionRouteAt(t, srv.store, pk[:4], scopeUnmatched(), types.RouteTransportFlood, recent)
	// A second one with nothing to report.
	pk2 := testFullPubkeyB
	insertDeclared(t, srv, pk2, now, "*", 0)

	for _, path := range []string{
		"/api/scope-audit",
		"/api/scope-audit?window=1h",
		"/api/scope-audit?window=7d",
	} {
		t.Run(path, func(t *testing.T) {
			checker.check(t, router, path)
		})
	}
	checker.checkStatus(t, router, "/api/scope-audit?window=1y", http.StatusBadRequest)

	var audit api.ScopeAuditResponse
	decodeInto(t, checker.checkStatus(t, router, "/api/scope-audit", http.StatusOK).Body.Bytes(), &audit)
	if audit.Window != "24h" || len(audit.Repeaters) != 2 {
		t.Fatalf("audit = %+v", audit)
	}
	row := findScopeAuditRow(t, audit.Repeaters, pk)
	if len(row.NotObserved) != 1 || row.NotObserved[0] != "be-vlg" {
		t.Errorf("notObserved = %v", row.NotObserved)
	}
	if len(row.UndeclaredObserved) != 1 || row.UndeclaredObserved[0].Scope != "de-nw" || row.UndeclaredObserved[0].Packets != 1 {
		t.Errorf("undeclaredObserved = %+v", row.UndeclaredObserved)
	}
	if row.UndeclaredObserved[0].FirstSeen.IsZero() || row.UndeclaredObserved[0].LastSeen.IsZero() {
		t.Errorf("firstSeen/lastSeen not decoded: %+v", row.UndeclaredObserved[0])
	}
	if row.ConfigState != api.ScopeConfigState(ScopeConfigNoUnscoped) || row.ObservedUnmatchedPackets != 1 || row.RegionEvidence == nil {
		t.Errorf("row = %+v", row)
	}
	row2 := findScopeAuditRow(t, audit.Repeaters, pk2)
	if row2.ConfigState != api.ScopeConfigState(ScopeConfigNoScopes) || row2.NotObserved == nil || row2.UndeclaredObserved == nil || row2.DeclaredRegions == nil {
		t.Errorf("empty lists must be [] not null: %+v", row2)
	}
	if audit.Since.After(now) || audit.Since.Before(now.Add(-25*time.Hour)) {
		t.Errorf("since = %v", audit.Since)
	}
}

// The production captures are a sample of the live /api/scope-stats and
// /api/scope-audit responses (the audit is cut to 14 of 149 repeaters, covering
// full, no-scopes and no-flood rows).
func TestProdScopeFixturesMatchSpec(t *testing.T) {
	checkProdFixtures(t, []prodFixture{
		{"scope_stats_1h_prod.json", "ScopeStatsResponse"},
		{"scope_stats_24h_prod.json", "ScopeStatsResponse"},
		{"scope_stats_7d_prod.json", "ScopeStatsResponse"},
		{"scope_audit_1h_prod.json", "ScopeAuditResponse"},
		{"scope_audit_24h_prod.json", "ScopeAuditResponse"},
		{"scope_audit_7d_prod.json", "ScopeAuditResponse"},
	})
}
