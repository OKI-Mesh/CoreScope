package main

import (
	"encoding/json"
	"net/http"
	"os"
	"testing"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/gorilla/mux"
)

func TestNodeReachMatchesSpec(t *testing.T) {
	checker := newSpecChecker(t)

	serve := func(t *testing.T, hops []string) (*Server, *mux.Router, string) {
		t.Helper()
		resetReachState(t)
		db, node := newReachIntegrationDB(t, hops)
		cfg := &Config{}
		srv := &Server{store: newTestStoreWithDB(t, db, cfg), db: db, cfg: cfg, perfStats: NewPerfStats()}
		router := mux.NewRouter()
		router.HandleFunc("/api/nodes/{pubkey}/reach", srv.handleNodeReach).Methods("GET")
		return srv, router, node
	}

	t.Run("node with attributed links", func(t *testing.T) {
		_, router, node := serve(t, []string{"aabb", "01fa", "ccdd"})
		checker.check(t, router, "/api/nodes/"+node+"/reach?days=30")
	})
	t.Run("default window", func(t *testing.T) {
		_, router, node := serve(t, []string{"aabb", "01fa", "ccdd"})
		checker.check(t, router, "/api/nodes/"+node+"/reach")
	})
	t.Run("identifiable node with no reach", func(t *testing.T) {
		_, router, node := serve(t, []string{"aabb", "ccdd"})
		checker.check(t, router, "/api/nodes/"+node+"/reach?days=30")
	})
	t.Run("malformed pubkey is a 400", func(t *testing.T) {
		_, router, _ := serve(t, []string{"aabb", "01fa", "ccdd"})
		checker.checkStatus(t, router, "/api/nodes/not-a-pubkey/reach", http.StatusBadRequest)
	})
	t.Run("unknown node is a 404", func(t *testing.T) {
		_, router, _ := serve(t, []string{"aabb", "01fa", "ccdd"})
		checker.checkStatus(t, router, "/api/nodes/"+pk64("dead")+"/reach", http.StatusNotFound)
	})
}

func TestProdReachFixturesMatchSpec(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromData(api.Spec)
	if err != nil {
		t.Fatalf("load spec: %v", err)
	}
	schema, ok := doc.Components.Schemas["NodeReachResponse"]
	if !ok {
		t.Fatal("NodeReachResponse is not in the spec")
	}
	for _, file := range []string{"reach_busy_prod.json", "reach_quiet_prod.json"} {
		t.Run(file, func(t *testing.T) {
			raw, err := os.ReadFile("testdata/" + file)
			if err != nil {
				t.Fatal(err)
			}
			var decoded interface{}
			if err := json.Unmarshal(raw, &decoded); err != nil {
				t.Fatalf("decode fixture: %v", err)
			}
			if err := schema.Value.VisitJSON(decoded); err != nil {
				t.Errorf("production response does not match NodeReachResponse: %v", err)
			}
		})
	}
}
