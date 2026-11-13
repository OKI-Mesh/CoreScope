package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/gorillamux"
	"github.com/gorilla/mux"
)

// specChecker validates real handler responses against the embedded
// api/openapi.yaml.
type specChecker struct {
	routes routers.Router
}

// loadSpec parses the embedded api/openapi.yaml.
func loadSpec(t *testing.T) *openapi3.T {
	t.Helper()
	doc, err := openapi3.NewLoader().LoadFromData(api.Spec)
	if err != nil {
		t.Fatalf("load spec: %v", err)
	}
	return doc
}

func newSpecChecker(t *testing.T) specChecker {
	t.Helper()
	doc := loadSpec(t)
	if err := doc.Validate(context.Background()); err != nil {
		t.Fatalf("spec is not valid OpenAPI: %v", err)
	}
	routes, err := gorillamux.NewRouter(doc)
	if err != nil {
		t.Fatalf("build spec router: %v", err)
	}
	return specChecker{routes: routes}
}

// check sends GET path through the server's router and fails the test if the
// 200 response does not match the spec.
func (c specChecker) check(t *testing.T, router *mux.Router, path string) {
	t.Helper()
	c.checkStatus(t, router, path, http.StatusOK)
}

// checkStatus sends GET path, requires the given status, and fails the test if
// the response does not match the spec. It returns the recorded response so a
// test can assert on headers of that same response.
func (c specChecker) checkStatus(t *testing.T, router *mux.Router, path string, want int) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != want {
		t.Fatalf("%s: status %d, want %d; body=%s", path, rec.Code, want, rec.Body.String())
	}

	c.validate(t, req, rec)
	return rec
}

// validate fails the test if the recorded response to req does not match the
// spec for its route and status.
func (c specChecker) validate(t *testing.T, req *http.Request, rec *httptest.ResponseRecorder) {
	t.Helper()
	route, pathParams, err := c.routes.FindRoute(req)
	if err != nil {
		t.Fatalf("%s %s: route is not in the spec: %v", req.Method, req.URL.RequestURI(), err)
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
		t.Errorf("%s %s: status %d does not match the spec: %v", req.Method, req.URL.RequestURI(), rec.Code, err)
	}
}

// prodFixture names an anonymized production response in testdata and the
// schema in api/openapi.yaml that it has to satisfy.
type prodFixture struct {
	file   string
	schema string
}

// checkProdFixtures validates each production capture against its schema. The
// contract has to accept exactly what the live frontend receives today, so a
// field the spec requires but production does not send fails here.
func checkProdFixtures(t *testing.T, fixtures []prodFixture) {
	t.Helper()
	doc := loadSpec(t)
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
			if err := schema.Value.VisitJSON(decoded); err != nil {
				t.Errorf("production response does not match %s: %v", f.schema, err)
			}
		})
	}
}

func TestInvalidRoleFilterMatchesSpec(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := setupTestServer(t)
	checker.checkStatus(t, router, "/api/analytics/neighbor-graph?role=bogus", http.StatusBadRequest)
}

// TestResponsesMatchCommittedSpec checks every documented endpoint against the
// fixture server. Add a path here in the same change that adds the endpoint to
// the spec.
func TestResponsesMatchCommittedSpec(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := setupTestServer(t)

	listRec := httptest.NewRecorder()
	router.ServeHTTP(listRec, httptest.NewRequest(http.MethodGet, "/api/nodes", nil))
	var list api.NodeListResponse
	if err := json.Unmarshal(listRec.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode /api/nodes: %v", err)
	}
	if len(list.Nodes) == 0 {
		t.Fatal("fixture has no nodes")
	}
	pubkey := list.Nodes[0].PublicKey

	paths := []string{
		"/api/nodes/" + pubkey + "/neighbors",
		"/api/nodes/" + pubkey + "/neighbors?min_count=1&min_score=0&include_ambiguous=false",
		"/api/analytics/neighbor-graph",
		"/api/analytics/neighbor-graph?min_count=1&min_score=0",
		"/api/analytics/neighbor-graph?min_count=2&min_score=0.5&role=repeater",
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			checker.check(t, router, path)
		})
	}
}

// TestNeighborGraphPopulatedResponseMatchesSpec injects a fully populated graph
// so the edge, node and stats schemas are exercised even when the fixture
// database produces none: a resolved edge with an SNR, and an ambiguous edge
// with a prefix target and a null SNR.
func TestNeighborGraphPopulatedResponseMatchesSpec(t *testing.T) {
	checker := newSpecChecker(t)
	srv, router := setupTestServer(t)
	name := "Ridge"
	nodeRole := types.RoleRepeater
	snr := 6.5
	srv.computeNeighborGraphResponseFn = func(_ int, _ float64, _ string, _ types.NodeRole) api.NeighborGraphResponse {
		return api.NeighborGraphResponse{
			Nodes: []api.GraphNode{
				{Pubkey: "aa11", Name: &name, Role: &nodeRole, NeighborCount: 2},
				{Pubkey: "bb22", Name: nil, Role: nil, NeighborCount: 1},
			},
			Edges: []api.GraphEdge{
				{Source: "aa11", Target: "bb22", Weight: 12, Score: 0.8, Bidirectional: true, AvgSnr: &snr, Ambiguous: false},
				{Source: "aa11", Target: "prefix:cc", Weight: 3, Score: 0.2, Bidirectional: true, AvgSnr: nil, Ambiguous: true},
			},
			Stats: api.GraphStats{
				TotalNodes:          2,
				TotalEdges:          2,
				AmbiguousEdges:      1,
				AvgClusterSize:      2,
				RejectedEdgesGeoFar: 4,
			},
		}
	}

	// min_count=2 keeps the request off the pre-marshaled snapshot paths.
	checker.check(t, router, "/api/analytics/neighbor-graph?min_count=2&min_score=0.5")
}

// TestSpecDescribesOnlyRegisteredRoutes fails when api/openapi.yaml documents an
// operation that no handler serves, so the spec cannot be pre-staged.
func TestSpecDescribesOnlyRegisteredRoutes(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromData(api.Spec)
	if err != nil {
		t.Fatalf("load spec: %v", err)
	}
	_, router := setupTestServer(t)

	registered := make(map[string]bool)
	err = router.Walk(func(route *mux.Route, _ *mux.Router, _ []*mux.Route) error {
		path, err := route.GetPathTemplate()
		if err != nil {
			return nil
		}
		methods, err := route.GetMethods()
		if err != nil {
			return nil
		}
		for _, method := range methods {
			registered[method+" "+path] = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk routes: %v", err)
	}

	for path, item := range doc.Paths.Map() {
		for method := range item.Operations() {
			if !registered[method+" "+path] {
				t.Errorf("spec documents %s %s but no handler is registered for it", method, path)
			}
		}
	}
}
