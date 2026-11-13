package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
	"github.com/OKI-Mesh/CoreScope/internal/prunequeue"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/gorilla/mux"
)

// Section 1.6 round-trip tests. The fixtures are anonymized captures of the
// production (SQLite) responses taken from staging. Two kinds of check:
//
//   - every capture has to satisfy the spec schema, so the contract accepts
//     exactly what the live frontend receives today;
//   - the behaviors the captures revealed (the "0" hash-size bucket, empty
//     states, unknown observers, area order, prune timestamps) are pinned
//     against the Postgres-backed handlers so the cutover cannot change them.

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return raw
}

func fixtureJSON(t *testing.T, name string) any {
	t.Helper()
	var v any
	if err := json.Unmarshal(readFixture(t, name), &v); err != nil {
		t.Fatalf("decode %s: %v", name, err)
	}
	return v
}

// asJSON round-trips a handler body or struct into generic JSON so it can be
// compared with a fixture independent of key order and Go types.
func asJSON(t *testing.T, raw []byte) any {
	t.Helper()
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("decode response %q: %v", raw, err)
	}
	return v
}

func getBody(t *testing.T, router *mux.Router, path string, want int) []byte {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != want {
		t.Fatalf("GET %s: status %d, want %d; body=%s", path, rec.Code, want, rec.Body.String())
	}
	return rec.Body.Bytes()
}

// checkProdListFixtures validates every element of a captured array against
// the element schema.
func checkProdListFixtures(t *testing.T, fixtures []prodFixture) {
	t.Helper()
	doc := loadSpec(t)
	for _, f := range fixtures {
		t.Run(f.file, func(t *testing.T) {
			schema, ok := doc.Components.Schemas[f.schema]
			if !ok {
				t.Fatalf("%s is not in the spec", f.schema)
			}
			rows, ok := fixtureJSON(t, f.file).([]any)
			if !ok {
				t.Fatalf("%s is not a JSON array", f.file)
			}
			if len(rows) == 0 {
				t.Fatalf("%s is empty", f.file)
			}
			for i, row := range rows {
				if err := schema.Value.VisitJSON(row); err != nil {
					t.Errorf("row %d does not match %s: %v", i, f.schema, err)
				}
			}
		})
	}
}

func TestProdCapturesMatchSpec(t *testing.T) {
	checkProdFixtures(t, []prodFixture{
		{"hash_sizes_unfiltered_prod.json", "HashSizeAnalyticsResponse"},
		{"packet_detail_store_prod.json", "PacketDetailResponse"},
		{"packet_detail_fallback_prod.json", "PacketDetailResponse"},
		{"packet_observations_batch_prod.json", "ObservationBatchResponse"},
		{"observer_summary_prod.json", "MetricsSummaryResponse"},
		{"observer_metrics_empty_prod.json", "ObserverMetricsResponse"},
		{"observer_metrics_quiet_prod.json", "ObserverMetricsResponse"},
		{"observer_metrics_unknown_prod.json", "ObserverMetricsResponse"},
		{"observer_analytics_empty_prod.json", "ObserverAnalyticsResponse"},
		{"observer_analytics_quiet_prod.json", "ObserverAnalyticsResponse"},
		{"observer_analytics_unknown_prod.json", "ObserverAnalyticsResponse"},
		{"decode_advert_prod.json", "DecodeResponse"},
		{"decode_anon_req_prod.json", "DecodeResponse"},
		{"prune_preview_prod.json", "PruneGeoFilterPreview"},
		{"prune_accepted_prod.json", "PruneGeoFilterAccepted"},
		{"prune_status_prod.json", "PruneGeoFilterStatus"},
		{"prune_status_invalid_id_prod.json", "ErrorResponse"},
		// Existing files that this batch recaptured under the same name.
		{"distance_prod.json", "DistanceAnalyticsResponse"},
		{"hash_sizes_region_prod.json", "HashSizeAnalyticsResponse"},
		{"observer_analytics_prod.json", "ObserverAnalyticsResponse"},
		{"observer_metrics_prod.json", "ObserverMetricsResponse"},
	})
	checkProdListFixtures(t, []prodFixture{
		{"areas_prod.json", "AreaSummary"},
		{"polygons_prod.json", "AreaBoundary"},
	})
}

// The empty-distance capture is not validated against the spec: production
// sends distHistogram as [] where the contract says object (item 7). The
// difference is pinned by TestEmptyDistanceMatchesProductionExceptHistogram.

func TestProdNotFoundCaptureMatchesSpec(t *testing.T) {
	doc := loadSpec(t)
	raw := readFixture(t, "packet_notfound_prod.txt")
	status, body, ok := strings.Cut(string(raw), "\n")
	if !ok || strings.TrimSpace(status) != "status: 404" {
		t.Fatalf("unexpected status line %q", status)
	}
	var decoded any
	if err := json.Unmarshal([]byte(body), &decoded); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if err := doc.Components.Schemas["ErrorResponse"].Value.VisitJSON(decoded); err != nil {
		t.Errorf("not-found body does not match ErrorResponse: %v", err)
	}
}

func TestPacketNotFoundMatchesProduction(t *testing.T) {
	_, router := setupTestServer(t)
	got := asJSON(t, getBody(t, router, "/api/packets/ffffffffffffffff", http.StatusNotFound))
	want := map[string]any{"error": "Not found"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("body = %v, want %v", got, want)
	}
}

// Production sends a "0" key in distributionByRepeaters, with and without a
// region filter. It appears when a repeater's first advert is an undeclared
// zero-hop direct advert (path byte 0x00): the size bits say nothing, so the
// repeater is counted in bucket 0 until a later advert declares a size.
func TestHashSizesZeroBucketIsKept(t *testing.T) {
	ctx := t.Context()
	doc := loadSpec(t)

	db := setupTestDB(t)
	recent := time.Now().UTC().Add(-1 * time.Hour)
	const pk = "cc01cc02cc03cc04"
	dbtest.Node(t, db, pk, "Zero Hop Repeater", "repeater")
	obsIdx := dbtest.Observer(t, db, "obsz", "Obs Z", "CVG", recent, recent)
	// Header: ADVERT (4<<2) with the direct route bits; path byte 0x00.
	rawHex := fmt.Sprintf("%02x00aabbccdd", 0x10|int(types.RouteDirect))
	dbtest.Advert(t, db, obsIdx, "zerohop_01", rawHex, types.RouteDirect,
		fmt.Sprintf(`{"pubKey":%q,"name":"Zero Hop Repeater","type":"ADVERT"}`, pk), recent)

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache: %v", err)
	}

	for _, region := range []string{"", "CVG"} {
		t.Run("region="+region, func(t *testing.T) {
			resp := store.GetAnalyticsHashSizes(ctx, region, "")
			if got, ok := resp.DistributionByRepeaters["0"]; !ok || got != 1 {
				t.Errorf(`distributionByRepeaters["0"] = %d (present=%v), want 1; map=%v`, got, ok, resp.DistributionByRepeaters)
			}
			for _, k := range []string{"1", "2", "3"} {
				if _, ok := resp.DistributionByRepeaters[k]; !ok {
					t.Errorf("distributionByRepeaters lost key %q", k)
				}
			}
			raw, err := json.Marshal(resp)
			if err != nil {
				t.Fatal(err)
			}
			if err := doc.Components.Schemas["HashSizeAnalyticsResponse"].Value.VisitJSON(asJSON(t, raw)); err != nil {
				t.Errorf("response with a 0 bucket does not match the spec: %v", err)
			}
		})
	}
}

// newEmptyServer returns a server over a database with no data at all.
func newEmptyServer(t *testing.T) (*Server, *mux.Router) {
	t.Helper()
	ctx := t.Context()
	db := setupTestDB(t)
	cfg := &Config{Port: 3000, SnapshotDir: t.TempDir()}
	srv := NewServer(db, cfg, NewHub())
	store := newPacketStoreForTest(t, db)
	store.config = cfg
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache(): %v", err)
	}
	if !store.WaitIndexesReady(5 * time.Second) {
		t.Fatalf("background indexes never became ready")
	}
	srv.store = store
	router := mux.NewRouter()
	srv.RegisterRoutes(router)
	return srv, router
}

// With nothing to measure, production answers distHistogram: [] while the
// contract (item 7) says an object with empty bins, so the frontend can read
// .bins without a guard. Everything else has to match the capture exactly.
func TestEmptyDistanceMatchesProductionExceptHistogram(t *testing.T) {
	srv, router := newEmptyServer(t)
	waitDistanceIndex(t, srv.store)

	got, ok := asJSON(t, getBody(t, router, "/api/analytics/distance", http.StatusOK)).(map[string]any)
	if !ok {
		t.Fatal("response is not an object")
	}
	want := fixtureJSON(t, "distance_empty_prod.json").(map[string]any)

	if prodHist, ok := want["distHistogram"].([]any); !ok || len(prodHist) != 0 {
		t.Fatalf("capture no longer has distHistogram: [] (got %v); revisit item 7", want["distHistogram"])
	}
	hist, ok := got["distHistogram"].(map[string]any)
	if !ok {
		t.Fatalf("distHistogram = %v, want an object", got["distHistogram"])
	}
	if bins, ok := hist["bins"].([]any); !ok || len(bins) != 0 {
		t.Errorf("distHistogram.bins = %v, want []", hist["bins"])
	}

	delete(got, "distHistogram")
	delete(want, "distHistogram")
	if !reflect.DeepEqual(got, want) {
		gj, _ := json.MarshalIndent(got, "", "  ")
		wj, _ := json.MarshalIndent(want, "", "  ")
		t.Errorf("empty distance differs from production.\ngot:\n%s\nwant:\n%s", gj, wj)
	}
}

// An observer id the database has never seen is a 200 with empty bodies, not
// a 404: the frontend shows an empty chart for it.
func TestUnknownObserverReturnsEmptyBodies(t *testing.T) {
	_, router := setupTestServer(t)

	t.Run("metrics", func(t *testing.T) {
		got := asJSON(t, getBody(t, router, "/api/observers/unknown-observer/metrics", http.StatusOK))
		want := fixtureJSON(t, "observer_metrics_unknown_prod.json")
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
	t.Run("analytics", func(t *testing.T) {
		got := asJSON(t, getBody(t, router, "/api/observers/unknown-observer/analytics", http.StatusOK))
		want := fixtureJSON(t, "observer_analytics_unknown_prod.json")
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
}

// A known observer with no data in the window answers with empty arrays and
// keeps its id and name.
func TestObserverWithNoDataReturnsEmptyArrays(t *testing.T) {
	db := setupTestDB(t)
	old := time.Now().UTC().Add(-60 * 24 * time.Hour)
	dbtest.Observer(t, db, "neonhub", "Observer-016", "IND", old, old)

	cfg := &Config{Port: 3000, SnapshotDir: t.TempDir()}
	srv := NewServer(db, cfg, NewHub())
	store := newPacketStoreForTest(t, db)
	store.config = cfg
	if err := store.LoadChunked(t.Context(), 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	if !store.WaitIndexesReady(5 * time.Second) {
		t.Fatalf("background indexes never became ready")
	}
	srv.store = store
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	t.Run("metrics", func(t *testing.T) {
		got, ok := asJSON(t, getBody(t, router, "/api/observers/neonhub/metrics", http.StatusOK)).(map[string]any)
		if !ok {
			t.Fatal("not an object")
		}
		want := fixtureJSON(t, "observer_metrics_empty_prod.json").(map[string]any)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
	t.Run("analytics", func(t *testing.T) {
		got := asJSON(t, getBody(t, router, "/api/observers/neonhub/analytics", http.StatusOK))
		want := fixtureJSON(t, "observer_analytics_empty_prod.json")
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
}

// Production walks a map to build both lists, so the order is arbitrary and a
// client must not depend on it. The test compares by key.
func TestAreasAndPolygonsMatchProductionByKey(t *testing.T) {
	var captured []struct {
		Key     string       `json:"key"`
		Label   string       `json:"label"`
		Polygon [][2]float64 `json:"polygon"`
		LatMin  *float64     `json:"latMin"`
		LatMax  *float64     `json:"latMax"`
		LonMin  *float64     `json:"lonMin"`
		LonMax  *float64     `json:"lonMax"`
	}
	if err := json.Unmarshal(readFixture(t, "polygons_prod.json"), &captured); err != nil {
		t.Fatal(err)
	}
	if len(captured) < 2 {
		t.Fatalf("polygons fixture has %d areas, want at least 2", len(captured))
	}

	srv, router := setupTestServer(t)
	srv.cfg.Areas = make(map[string]AreaEntry, len(captured))
	for _, a := range captured {
		srv.cfg.Areas[a.Key] = AreaEntry{
			Label: a.Label, Polygon: a.Polygon,
			LatMin: a.LatMin, LatMax: a.LatMax, LonMin: a.LonMin, LonMax: a.LonMax,
		}
	}

	byKey := func(t *testing.T, v any) map[string]any {
		t.Helper()
		rows, ok := v.([]any)
		if !ok {
			t.Fatalf("not an array: %v", v)
		}
		out := make(map[string]any, len(rows))
		for _, r := range rows {
			row, ok := r.(map[string]any)
			if !ok {
				t.Fatalf("row is not an object: %v", r)
			}
			key, _ := row["key"].(string)
			if _, dup := out[key]; dup {
				t.Fatalf("duplicate key %q", key)
			}
			out[key] = row
		}
		return out
	}

	for _, c := range []struct{ name, path, fixture string }{
		{"areas", "/api/config/areas", "areas_prod.json"},
		{"polygons", "/api/config/areas/polygons", "polygons_prod.json"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := byKey(t, asJSON(t, getBody(t, router, c.path, http.StatusOK)))
			want := byKey(t, fixtureJSON(t, c.fixture))
			if !reflect.DeepEqual(got, want) {
				gk, wk := keysOf(got), keysOf(want)
				t.Errorf("areas differ by key.\ngot keys %v\nwant keys %v\ngot  %v\nwant %v", gk, wk, got, want)
			}
		})
	}
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Production reports prune times with nanosecond precision
// (2026-11-13T14:28:20.015215111Z). The result is read back from the queue
// file, so the handler must not truncate it.
func TestPruneStatusKeepsNanosecondTimestamps(t *testing.T) {
	checker := newSpecChecker(t)
	gf := &GeoFilterConfig{Polygon: [][2]float64{{37.0, -123.0}, {38.0, -123.0}, {38.0, -121.0}, {37.0, -121.0}}}
	srv, router := setupPruneGeoFilterServer(t, adminTestKey, gf)
	srv.cfg.PruneQueuePath = filepath.Join(t.TempDir(), "corescope.db")

	const id = "7b5ca089045d7b09"
	requested := time.Date(2026, 11, 13, 14, 28, 16, 756228663, time.UTC)
	completed := time.Date(2026, 11, 13, 14, 28, 20, 15215111, time.UTC)
	if err := prunequeue.WriteResult(srv.cfg.PruneQueuePath, prunequeue.Result{
		ID: id, RequestedAt: requested, CompletedAt: completed, Deleted: 1,
	}); err != nil {
		t.Fatalf("WriteResult: %v", err)
	}

	rec := checker.checkAsAdmin(t, router, http.MethodGet, "/api/admin/prune-geo-filter/status?id="+id, "", http.StatusOK)
	body := rec.Body.String()
	for _, want := range []string{`"2026-11-13T14:28:16.756228663Z"`, `"2026-11-13T14:28:20.015215111Z"`} {
		if !strings.Contains(body, want) {
			t.Errorf("body %s is missing %s", body, want)
		}
	}

	// The same keys as the capture, including an empty error string on success.
	got := asJSON(t, rec.Body.Bytes()).(map[string]any)
	prod := fixtureJSON(t, "prune_status_prod.json").(map[string]any)
	if !reflect.DeepEqual(keysOf(got), keysOf(prod)) {
		t.Errorf("keys = %v, production keys = %v", keysOf(got), keysOf(prod))
	}
	if e, ok := got["error"]; !ok || e != "" {
		t.Errorf(`error = %v (present=%v), want ""`, e, ok)
	}
	if got["status"] != "done" || got["deleted"] != float64(1) {
		t.Errorf("status/deleted = %v/%v", got["status"], got["deleted"])
	}
}

func TestPruneStatusPendingUnknownAndInvalid(t *testing.T) {
	checker := newSpecChecker(t)
	gf := &GeoFilterConfig{Polygon: [][2]float64{{37.0, -123.0}, {38.0, -123.0}, {38.0, -121.0}, {37.0, -121.0}}}
	srv, router := setupPruneGeoFilterServer(t, adminTestKey, gf)
	srv.cfg.PruneQueuePath = filepath.Join(t.TempDir(), "corescope.db")

	const pendingID = "00112233aabbccdd"
	if err := prunequeue.WriteRequest(srv.cfg.PruneQueuePath, prunequeue.Request{
		ID: pendingID, RequestedAt: time.Now().UTC(), Pubkeys: []string{padKey("aaaa111122223333")},
	}); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}

	t.Run("pending", func(t *testing.T) {
		rec := checker.checkAsAdmin(t, router, http.MethodGet, "/api/admin/prune-geo-filter/status?id="+pendingID, "", http.StatusOK)
		var status api.PruneGeoFilterStatus
		decodeInto(t, rec.Body.Bytes(), &status)
		if status.Status != "pending" || status.RequestId != pendingID || status.Deleted != nil || status.CompletedAt != nil {
			t.Errorf("status = %+v", status)
		}
	})
	t.Run("unknown id", func(t *testing.T) {
		checker.checkAsAdmin(t, router, http.MethodGet, "/api/admin/prune-geo-filter/status?id=deadbeefdeadbeef", "", http.StatusNotFound)
	})
	t.Run("invalid id matches production", func(t *testing.T) {
		rec := checker.checkAsAdmin(t, router, http.MethodGet, "/api/admin/prune-geo-filter/status?id=not-hex", "", http.StatusBadRequest)
		got := asJSON(t, rec.Body.Bytes())
		want := fixtureJSON(t, "prune_status_invalid_id_prod.json")
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
}
