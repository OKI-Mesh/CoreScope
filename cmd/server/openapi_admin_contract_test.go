package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/gorilla/mux"
)

const adminTestKey = "test-secret-key-strong-enough"

// checkAsAdmin sends the request with the API key, requires the given status,
// fails the test if the response does not match the spec, and returns the
// recorded response.
func (c specChecker) checkAsAdmin(t *testing.T, router *mux.Router, method, path, body string, want int) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("X-API-Key", adminTestKey)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != want {
		t.Fatalf("%s %s: status %d, want %d; body=%s", method, path, rec.Code, want, rec.Body.String())
	}

	route, pathParams, err := c.routes.FindRoute(req)
	if err != nil {
		t.Fatalf("%s %s: route is not in the spec: %v", method, path, err)
	}
	input := &openapi3filter.ResponseValidationInput{
		RequestValidationInput: &openapi3filter.RequestValidationInput{
			Request:    req,
			PathParams: pathParams,
			Route:      route,
		},
		Status:  rec.Code,
		Header:  rec.Header(),
		Options: &openapi3filter.Options{IncludeResponseStatus: true},
	}
	input.SetBodyBytes(rec.Body.Bytes())
	if err := openapi3filter.ValidateResponse(context.Background(), input); err != nil {
		t.Errorf("%s %s: status %d does not match the spec: %v", method, path, rec.Code, err)
	}
	return rec
}

func TestPerfResetMatchesSpec(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := setupTestServerWithAPIKey(t, adminTestKey)

	var resp api.OkResponse
	decodeInto(t, checker.checkAsAdmin(t, router, http.MethodPost, "/api/perf/reset", "", http.StatusOK).Body.Bytes(), &resp)
	if !resp.Ok {
		t.Error("ok = false")
	}
}

func TestDroppedPacketsMatchSpec(t *testing.T) {
	checker := newSpecChecker(t)
	srv, router := setupTestServerWithAPIKey(t, adminTestKey)

	// The database stamps dropped_at itself when it inserts a row.
	before := time.Now().UTC().Truncate(time.Second)
	for _, d := range []database.DroppedPacket{
		{Hash: "f8efa237271c1d25", RawHex: "11403f35", Reason: "invalid signature", ObserverID: "AABB", ObserverName: "Observer", NodePubkey: padKey("3f35"), NodeName: "Node"},
		{Hash: "", RawHex: "", Reason: "malformed", ObserverID: "", ObserverName: "", NodePubkey: "", NodeName: ""},
	} {
		if err := srv.db.InsertDroppedPacket(t.Context(), d); err != nil {
			t.Fatalf("InsertDroppedPacket(): %v", err)
		}
	}

	var resp []api.DroppedPacket
	decodeInto(t, checker.checkAsAdmin(t, router, http.MethodGet, "/api/dropped-packets", "", http.StatusOK).Body.Bytes(), &resp)
	if len(resp) != 2 {
		t.Fatalf("got %d packets, want 2", len(resp))
	}
	newest, oldest := resp[0], resp[1]
	if newest.Reason != "malformed" || newest.Hash != nil || newest.ObserverId != nil || newest.NodePubkey != nil || newest.RawHex != "" {
		t.Errorf("a packet with nothing known should carry nulls: %+v", newest)
	}
	if oldest.Hash == nil || *oldest.Hash != "f8efa237271c1d25" || oldest.RawHex != "11403f35" {
		t.Errorf("full packet = %+v", oldest)
	}
	if oldest.DroppedAt.Before(before) || oldest.DroppedAt.After(time.Now().UTC()) {
		t.Errorf("dropped_at = %v, want between %v and now", oldest.DroppedAt, before)
	}

	checker.checkAsAdmin(t, router, http.MethodGet, "/api/dropped-packets?limit=1&observer=AABB&pubkey="+padKey("3f35"), "", http.StatusOK)
}

func TestDebugAffinityMatchesSpec(t *testing.T) {
	checker := newSpecChecker(t)
	srv, router := setupTestServerWithAPIKey(t, adminTestKey)

	now := time.Now()
	resolvedA, resolvedB := padKey("aaaa1111"), padKey("bbbb2222")
	known := padKey("cccc3333")
	ambiguous := newAmbiguousEdge(known, "dd", []string{padKey("dddd0001"), padKey("dddd0002")}, 12, now)
	srv.neighborGraph = makeTestGraph(
		newEdge(resolvedA, resolvedB, "bb", 50, now),
		ambiguous,
	)

	var resp api.DebugAffinityResponse
	decodeInto(t, checker.checkAsAdmin(t, router, http.MethodGet, "/api/debug/affinity", "", http.StatusOK).Body.Bytes(), &resp)
	if len(resp.Edges) != 2 || resp.Stats.TotalEdges != 2 {
		t.Fatalf("edges = %d, stats = %+v", len(resp.Edges), resp.Stats)
	}
	if resp.Edges[0].Weight < resp.Edges[1].Weight {
		t.Error("edges should come heaviest first")
	}
	if len(resp.Resolutions) != 1 || resp.Resolutions[0].Confidence != "AMBIGUOUS" || len(resp.Resolutions[0].Candidates) != 2 {
		t.Errorf("resolutions = %+v", resp.Resolutions)
	}

	checker.checkAsAdmin(t, router, http.MethodGet, "/api/debug/affinity?prefix=bb", "", http.StatusOK)
	checker.checkAsAdmin(t, router, http.MethodGet, "/api/debug/affinity?node="+resolvedA, "", http.StatusOK)
}

func TestPruneGeoFilterMatchesSpec(t *testing.T) {
	checker := newSpecChecker(t)
	gf := &GeoFilterConfig{Polygon: [][2]float64{{37.0, -123.0}, {38.0, -123.0}, {38.0, -121.0}, {37.0, -121.0}}}
	srv, router := setupPruneGeoFilterServer(t, adminTestKey, gf)
	srv.cfg.PruneQueuePath = filepath.Join(t.TempDir(), "corescope.db")

	var preview api.PruneGeoFilterPreview
	decodeInto(t, checker.checkAsAdmin(t, router, http.MethodPost, "/api/admin/prune-geo-filter", "", http.StatusOK).Body.Bytes(), &preview)
	if !preview.DryRun || preview.Count != 1 || len(preview.Nodes) != 1 || preview.Nodes[0].Name != "OutsideNode" {
		t.Fatalf("preview = %+v", preview)
	}

	body, err := json.Marshal(api.PruneGeoFilterRequest{Pubkeys: &[]string{preview.Nodes[0].Pubkey}})
	if err != nil {
		t.Fatal(err)
	}
	var accepted api.PruneGeoFilterAccepted
	decodeInto(t, checker.checkAsAdmin(t, router, http.MethodPost, "/api/admin/prune-geo-filter?confirm=true", string(body), http.StatusAccepted).Body.Bytes(), &accepted)
	if accepted.DryRun || !accepted.Accepted || accepted.Count != 1 || accepted.RequestId == "" {
		t.Fatalf("accepted = %+v", accepted)
	}

	var status api.PruneGeoFilterStatus
	decodeInto(t, checker.checkAsAdmin(t, router, http.MethodGet, accepted.StatusUrl, "", http.StatusOK).Body.Bytes(), &status)
	if status.Status != "pending" || status.RequestId != accepted.RequestId || status.Deleted != nil {
		t.Errorf("status = %+v", status)
	}

	// A pubkey that was not in the preview is not queued, and an empty queue
	// still answers with an empty list rather than null.
	other, err := json.Marshal(api.PruneGeoFilterRequest{Pubkeys: &[]string{padKey("1111111122223333")}})
	if err != nil {
		t.Fatal(err)
	}
	var none api.PruneGeoFilterAccepted
	decodeInto(t, checker.checkAsAdmin(t, router, http.MethodPost, "/api/admin/prune-geo-filter?confirm=true", string(other), http.StatusAccepted).Body.Bytes(), &none)
	if none.Count != 0 || none.Nodes == nil {
		t.Errorf("none = %+v", none)
	}
}

func TestPruneGeoFilterErrorsMatchSpec(t *testing.T) {
	checker := newSpecChecker(t)
	gf := &GeoFilterConfig{Polygon: [][2]float64{{37.0, -123.0}, {38.0, -123.0}, {38.0, -121.0}, {37.0, -121.0}}}
	srv, router := setupPruneGeoFilterServer(t, adminTestKey, gf)
	srv.cfg.PruneQueuePath = filepath.Join(t.TempDir(), "corescope.db")

	checker.checkAsAdmin(t, router, http.MethodPost, "/api/admin/prune-geo-filter?confirm=true", "", http.StatusBadRequest)
	checker.checkAsAdmin(t, router, http.MethodGet, "/api/admin/prune-geo-filter/status", "", http.StatusBadRequest)
	checker.checkAsAdmin(t, router, http.MethodGet, "/api/admin/prune-geo-filter/status?id=deadbeefdeadbeef", "", http.StatusNotFound)

	_, unconfigured := setupPruneGeoFilterServer(t, adminTestKey, nil)
	checker.checkAsAdmin(t, unconfigured, http.MethodPost, "/api/admin/prune-geo-filter", "", http.StatusBadRequest)
}

func TestAdminRoutesRejectMissingKey(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := setupTestServerWithAPIKey(t, adminTestKey)

	for _, r := range []struct{ method, path string }{
		{http.MethodPost, "/api/perf/reset"},
		{http.MethodGet, "/api/dropped-packets"},
		{http.MethodGet, "/api/debug/affinity"},
		{http.MethodPost, "/api/admin/prune-geo-filter"},
		{http.MethodGet, "/api/admin/prune-geo-filter/status?id=x"},
	} {
		t.Run(r.method+" "+r.path, func(t *testing.T) {
			req := httptest.NewRequest(r.method, r.path, nil)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status %d, want 401", rec.Code)
			}
			route, pathParams, err := checker.routes.FindRoute(req)
			if err != nil {
				t.Fatal(err)
			}
			input := &openapi3filter.ResponseValidationInput{
				RequestValidationInput: &openapi3filter.RequestValidationInput{Request: req, PathParams: pathParams, Route: route},
				Status:                 rec.Code,
				Header:                 rec.Header(),
				Options:                &openapi3filter.Options{IncludeResponseStatus: true},
			}
			input.SetBodyBytes(rec.Body.Bytes())
			if err := openapi3filter.ValidateResponse(context.Background(), input); err != nil {
				t.Errorf("401 does not match the spec: %v", err)
			}
		})
	}
}

func TestProdAdminFixturesMatchSpec(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromData(api.Spec)
	if err != nil {
		t.Fatalf("load spec: %v", err)
	}
	fixtures := []struct{ file, schema string }{
		{"debug_affinity_prod.json", "DebugAffinityResponse"},
		{"dropped_packets_prod.json", "DroppedPacket"},
	}
	for _, f := range fixtures {
		t.Run(f.file, func(t *testing.T) {
			schema, ok := doc.Components.Schemas[f.schema]
			if !ok {
				t.Fatalf("%s is not in the spec", f.schema)
			}
			raw, err := os.ReadFile("testdata/" + f.file)
			if err != nil {
				t.Fatal(err)
			}
			var decoded interface{}
			if err := json.Unmarshal(raw, &decoded); err != nil {
				t.Fatalf("decode fixture: %v", err)
			}
			if rows, isList := decoded.([]interface{}); isList {
				for i, row := range rows {
					if err := schema.Value.VisitJSON(row); err != nil {
						t.Errorf("production row %d does not match %s: %v", i, f.schema, err)
					}
				}
				return
			}
			if err := schema.Value.VisitJSON(decoded); err != nil {
				t.Errorf("production response does not match %s: %v", f.schema, err)
			}
		})
	}
}
