package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/gorilla/mux"
)

// TestEveryRegisteredRouteIsInTheSpec fails when the server serves an /api
// operation that api/openapi.yaml does not document, so a handler cannot be
// added without describing it. TestSpecDescribesOnlyRegisteredRoutes checks the
// other direction.
func TestEveryRegisteredRouteIsInTheSpec(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromData(api.Spec)
	if err != nil {
		t.Fatalf("load spec: %v", err)
	}
	_, router := setupTestServer(t)

	err = router.Walk(func(route *mux.Route, _ *mux.Router, _ []*mux.Route) error {
		path, err := route.GetPathTemplate()
		if err != nil || !strings.HasPrefix(path, "/api/") {
			return nil
		}
		methods, err := route.GetMethods()
		if err != nil {
			t.Errorf("%s accepts every method; register it with Methods(...) so the spec can describe it", path)
			return nil
		}
		item := doc.Paths.Find(path)
		for _, method := range methods {
			if item == nil || item.GetOperation(method) == nil {
				t.Errorf("%s %s is served but is not in api/openapi.yaml", method, path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk routes: %v", err)
	}
}

// TestSpecEndpointServesCommittedDocument checks /api/spec returns the
// embedded api/openapi.yaml byte for byte.
func TestSpecEndpointServesCommittedDocument(t *testing.T) {
	_, router := setupTestServer(t)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/spec", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/yaml; charset=utf-8" {
		t.Errorf("Content-Type = %q", ct)
	}
	if origin := rec.Header().Get("Access-Control-Allow-Origin"); origin != "*" {
		t.Errorf("Access-Control-Allow-Origin = %q, want *", origin)
	}
	if !bytes.Equal(rec.Body.Bytes(), api.Spec) {
		t.Error("body differs from the embedded api/openapi.yaml")
	}
}

// TestDocsPageRendersSpec checks /api/docs serves the Swagger UI page and that
// it loads the document from /api/spec.
func TestDocsPageRendersSpec(t *testing.T) {
	_, router := setupTestServer(t)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/docs", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/html; charset=utf-8" {
		t.Errorf("Content-Type = %q", ct)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "swagger-ui") || !strings.Contains(body, "url: '/api/spec'") {
		t.Error("page does not load the Swagger UI bundle from /api/spec")
	}
}
