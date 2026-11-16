package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

func TestRFAndDistanceEndpointsMatchSpec(t *testing.T) {
	checker := newSpecChecker(t)
	srv, router := setupTestServer(t)
	waitDistanceIndex(t, srv.store)

	rfPaths := []string{
		"/api/analytics/rf",
		"/api/analytics/rf?region=SJC",
		"/api/analytics/rf?area=nowhere",
		"/api/analytics/rf?window=24h",
		"/api/analytics/rf?window=7d&region=SJC",
		"/api/analytics/rf?window=bogus",
		"/api/analytics/rf?from=2026-04-01T00:00:00Z&to=2026-04-08T00:00:00Z",
	}
	for _, path := range rfPaths {
		t.Run(path, func(t *testing.T) {
			checker.check(t, router, path)
		})
	}

	distancePaths := []string{
		"/api/analytics/distance",
		"/api/analytics/distance?region=SJC",
		"/api/analytics/distance?area=nowhere",
	}
	for _, path := range distancePaths {
		t.Run(path, func(t *testing.T) {
			checker.check(t, router, path)
		})
	}
}

// TestRFWarmupResponseMatchesSpec checks the 503 the unfiltered RF query
// answers until the background snapshot has finished its first pass.
func TestRFWarmupResponseMatchesSpec(t *testing.T) {
	ctx := t.Context()
	checker := newSpecChecker(t)

	db := setupTestDB(t)
	store := newPacketStoreForTest(t, db)
	block := make(chan struct{})
	defer close(block)
	store.installWarmupBlocker_1659(ctx, block)

	srv := NewServer(db, &Config{Port: 3000}, NewHub())
	srv.store = store
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	checker.checkStatus(t, router, "/api/analytics/rf", http.StatusServiceUnavailable)
	// A filtered query skips the gate and is served normally.
	checker.check(t, router, "/api/analytics/rf?window=24h")
}

// TestDistanceBuildingResponseMatchesSpec checks the 202 the first distance
// request answers while the lazy index build is under way. Only the first
// request is guaranteed to see the build pending, so the header is read from
// that same response.
func TestDistanceBuildingResponseMatchesSpec(t *testing.T) {
	ctx := t.Context()
	checker := newSpecChecker(t)

	db := setupRichTestDB(t)
	defer db.Close()
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}
	srv := NewServer(db, &Config{Port: 3000}, NewHub())
	srv.store = store
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	rec := checker.checkStatus(t, router, "/api/analytics/distance", http.StatusAccepted)
	if got := rec.Header().Get("Retry-After"); got != "5" {
		t.Errorf("Retry-After = %q, want 5", got)
	}
}

// TestRFAndDistanceOutputMatchesSpec checks the marshaled results for a
// populated store, for hops that carry SNR and hops that do not, and for an
// empty store, where the histograms and lists must still be present.
func TestRFAndDistanceOutputMatchesSpec(t *testing.T) {
	ctx := t.Context()
	doc := loadSpec(t)

	richDB := setupRichTestDB(t)
	defer richDB.Close()
	rich := newPacketStoreForTest(t, richDB)
	if err := rich.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}
	rf := rich.GetAnalyticsRF(ctx, "", "")
	if rf.TotalPackets == 0 || len(rf.PayloadTypes) == 0 || len(rf.PacketsPerHour) == 0 || len(rf.ScatterData) == 0 {
		t.Errorf("rich store left a section empty: totalPackets=%d payloadTypes=%d packetsPerHour=%d scatterData=%d",
			rf.TotalPackets, len(rf.PayloadTypes), len(rf.PacketsPerHour), len(rf.ScatterData))
	}

	// Hops injected straight into the index: with no region the compute never
	// reads the record's transmission.
	base := time.Date(2026, 11, 5, 15, 30, 0, 123456789, time.UTC)
	snr := 7.5
	rich.mu.Lock()
	rich.distHops = []distHopRecord{
		{FromName: "Repeater 01", FromPk: "aa01", ToName: "Repeater 02", ToPk: "bb02", Dist: 42.5, Type: "R↔R", SNR: &snr, Hash: "h1", Timestamp: base, HourBucket: "2026-11-05T15"},
		{FromName: "Repeater 01", FromPk: "aa01", ToName: "Companion 03", ToPk: "cc03", Dist: 12.25, Type: "C↔R", Hash: "h2", Timestamp: base, HourBucket: "2026-11-05T15"},
	}
	rich.distPaths = []distPathRecord{
		{Hash: "p1", TotalDist: 54.75, HopCount: 2, Timestamp: base, Hops: []distHopDetail{
			{FromName: "Repeater 01", FromPk: "aa01", ToName: "Repeater 02", ToPk: "bb02", Dist: 42.5},
			{FromName: "Repeater 02", FromPk: "bb02", ToName: "Companion 03", ToPk: "cc03", Dist: 12.25},
		}},
	}
	rich.mu.Unlock()
	distance := rich.computeAnalyticsDistance(ctx, "", "")
	if len(distance.TopHops) != 2 || len(distance.TopPaths) != 1 || len(distance.DistOverTime) != 1 {
		t.Errorf("injected hops left a section wrong: topHops=%d topPaths=%d distOverTime=%d",
			len(distance.TopHops), len(distance.TopPaths), len(distance.DistOverTime))
	}
	if distance.TopHops[0].BestSnr == nil || distance.TopHops[1].BestSnr != nil {
		t.Errorf("bestSnr should be set for the hop with SNR and null for the one without")
	}

	emptyDB := setupTestDB(t)
	empty := newPacketStoreForTest(t, emptyDB)
	if err := empty.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}

	responses := []struct {
		name   string
		schema string
		value  interface{}
	}{
		{"rf", "RFAnalyticsResponse", rf},
		{"rf for a region", "RFAnalyticsResponse", rich.GetAnalyticsRF(ctx, "SJC", "")},
		{"rf for a window", "RFAnalyticsResponse", rich.GetAnalyticsRFWithWindow(ctx, "", "", windowFromQuery(t, "24h"))},
		{"rf with no data", "RFAnalyticsResponse", empty.GetAnalyticsRF(ctx, "", "")},
		{"distance", "DistanceAnalyticsResponse", distance},
		{"distance with no data", "DistanceAnalyticsResponse", empty.computeAnalyticsDistance(ctx, "", "")},
	}
	for _, r := range responses {
		t.Run(r.name, func(t *testing.T) {
			schema, ok := doc.Components.Schemas[r.schema]
			if !ok {
				t.Fatalf("%s is not in the spec", r.schema)
			}
			raw, err := json.Marshal(r.value)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var decoded interface{}
			if err := json.Unmarshal(raw, &decoded); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if err := schema.Value.VisitJSON(decoded); err != nil {
				t.Errorf("response does not match %s: %v", r.schema, err)
			}
		})
	}
}

// windowFromQuery builds the window the handler derives from
// ?window=<name>.
func windowFromQuery(t *testing.T, name string) TimeWindow {
	t.Helper()
	return ParseTimeWindow(httptest.NewRequest(http.MethodGet, "/api/analytics/rf?window="+name, nil))
}

func TestProdRFAndDistanceFixturesMatchSpec(t *testing.T) {
	checkProdFixtures(t, []prodFixture{
		{"rf_prod.json", "RFAnalyticsResponse"},
		{"rf_window_prod.json", "RFAnalyticsResponse"},
		{"distance_prod.json", "DistanceAnalyticsResponse"},
	})
}
