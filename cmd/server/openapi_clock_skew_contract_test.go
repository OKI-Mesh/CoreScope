package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gorilla/mux"
)

func TestClockSkewEndpointsMatchSpec(t *testing.T) {
	checker := newSpecChecker(t)

	t.Run("fleet and observers on the fixture server", func(t *testing.T) {
		_, router := setupTestServer(t)
		checker.check(t, router, "/api/nodes/clock-skew")
		checker.check(t, router, "/api/observers/clock-skew")
	})

	t.Run("node with bad samples", func(t *testing.T) {
		store, _, _ := seedIssue1094Repro(t)
		srv := &Server{store: store}
		router := mux.NewRouter()
		router.HandleFunc("/api/nodes/{pubkey}/clock-skew", srv.handleNodeClockSkew).Methods("GET")
		router.HandleFunc("/api/observers/clock-skew", srv.handleObserverClockSkew).Methods("GET")

		checker.check(t, router, "/api/nodes/BADTS1094/clock-skew")
		checker.check(t, router, "/api/observers/clock-skew")
		checker.checkStatus(t, router, "/api/nodes/NOSUCHNODE/clock-skew", http.StatusNotFound)
	})
}

// TestClockSkewRoutesAreNotShadowed goes through the full route table. If the
// clock-skew routes were registered after the parameterized ones, these paths
// would reach the node-detail and observer-detail handlers instead and return
// an error object, not the arrays asserted here.
func TestClockSkewRoutesAreNotShadowed(t *testing.T) {
	_, router := setupTestServer(t)
	for _, path := range []string{"/api/nodes/clock-skew", "/api/observers/clock-skew"} {
		t.Run(path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
			if rec.Code != http.StatusOK {
				t.Fatalf("%s: status %d, want 200; body=%s", path, rec.Code, rec.Body.String())
			}
			if !strings.HasPrefix(strings.TrimSpace(rec.Body.String()), "[") {
				t.Errorf("%s did not return an array, a parameterized route probably matched first: %s", path, rec.Body.String())
			}
		})
	}
}

func TestProdClockSkewFixturesMatchSpec(t *testing.T) {
	doc := loadSpec(t)
	for _, tc := range []struct{ name, file string }{
		{"NodeClockSkew", "node_clock_skew_prod.json"},
		{"FleetClockSkew", "fleet_clock_skew_prod.json"},
		{"ObserverCalibrations", "observer_clock_skew_prod.json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := os.ReadFile("testdata/" + tc.file)
			if err != nil {
				t.Fatal(err)
			}
			var decoded interface{}
			if err := json.Unmarshal(raw, &decoded); err != nil {
				t.Fatalf("decode fixture: %v", err)
			}
			schema := doc.Components.Schemas["NodeClockSkew"].Value
			switch tc.name {
			case "NodeClockSkew":
				if err := schema.VisitJSON(decoded); err != nil {
					t.Errorf("production response does not match NodeClockSkew: %v", err)
				}
			case "FleetClockSkew":
				for i, entry := range decoded.([]interface{}) {
					if err := schema.VisitJSON(entry); err != nil {
						t.Errorf("fleet entry %d does not match NodeClockSkew: %v", i, err)
					}
				}
			case "ObserverCalibrations":
				entry := doc.Components.Schemas["ObserverCalibration"].Value
				for i, row := range decoded.([]interface{}) {
					if err := entry.VisitJSON(row); err != nil {
						t.Errorf("observer entry %d does not match ObserverCalibration: %v", i, err)
					}
				}
			}
		})
	}
}
