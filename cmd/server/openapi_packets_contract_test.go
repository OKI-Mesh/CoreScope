package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/gorilla/mux"
)

// checkPost sends a POST with a JSON body through the server's router, requires
// the given status, and fails the test if the response does not match the spec.
func (c specChecker) checkPost(t *testing.T, router *mux.Router, path, body string, want int) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != want {
		t.Fatalf("POST %s: status %d, want %d; body=%s", path, rec.Code, want, rec.Body.String())
	}

	route, pathParams, err := c.routes.FindRoute(req)
	if err != nil {
		t.Fatalf("POST %s: route is not in the spec: %v", path, err)
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
		t.Errorf("POST %s: status %d does not match the spec: %v", path, rec.Code, err)
	}
}

func TestPacketEndpointsMatchSpec(t *testing.T) {
	checker := newSpecChecker(t)
	srv, router := setupTestServer(t)

	// abc123def4567890 is seeded with two observations and is in the store.
	const hash = "abc123def4567890"

	t.Run("flat list", func(t *testing.T) {
		checker.check(t, router, "/api/packets")
	})
	t.Run("flat list with observations", func(t *testing.T) {
		checker.check(t, router, "/api/packets?expand=observations")
	})
	t.Run("grouped list", func(t *testing.T) {
		checker.check(t, router, "/api/packets?groupByHash=true")
	})
	t.Run("filtered by type", func(t *testing.T) {
		checker.check(t, router, "/api/packets?type=4&order=asc")
	})
	t.Run("filtered by hash", func(t *testing.T) {
		checker.check(t, router, "/api/packets?hash="+hash)
	})
	t.Run("filtered by nodes", func(t *testing.T) {
		checker.check(t, router, "/api/packets?nodes=aabbccdd11223344")
	})
	t.Run("page past the end is an empty list", func(t *testing.T) {
		checker.check(t, router, "/api/packets?offset=100000")
	})
	t.Run("unparsable since is a 400", func(t *testing.T) {
		checker.checkStatus(t, router, "/api/packets?since=nope", http.StatusBadRequest)
	})

	t.Run("detail by hash", func(t *testing.T) {
		checker.check(t, router, "/api/packets/"+hash)
	})
	t.Run("detail by transmission id", func(t *testing.T) {
		checker.check(t, router, "/api/packets/1")
	})
	t.Run("detail by observation id", func(t *testing.T) {
		// An id that is an observation but not a transmission, so the lookup
		// falls through to the observation branch.
		obsID := 0
		for id := range srv.store.byObsID {
			if srv.store.byTxID[id] == nil {
				obsID = id
				break
			}
		}
		if obsID == 0 {
			t.Skip("every observation id in the fixture is also a transmission id")
		}
		path := "/api/packets/" + strconv.Itoa(obsID)
		checker.check(t, router, path)

		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		var body struct {
			Packet map[string]interface{} `json:"packet"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if _, ok := body.Packet["first_seen"]; ok {
			t.Errorf("an observation-id lookup returned the transmission shape: %v", body.Packet)
		}
	})
	t.Run("unknown hash is a 404", func(t *testing.T) {
		checker.checkStatus(t, router, "/api/packets/ffffffffffffffff", http.StatusNotFound)
	})
	t.Run("unknown id is a 404", func(t *testing.T) {
		checker.checkStatus(t, router, "/api/packets/99999999", http.StatusNotFound)
	})

	t.Run("timestamps", func(t *testing.T) {
		checker.check(t, router, "/api/packets/timestamps?since=2000-01-01T00:00:00Z")
	})
	t.Run("timestamps without since is a 400", func(t *testing.T) {
		checker.checkStatus(t, router, "/api/packets/timestamps", http.StatusBadRequest)
	})

	t.Run("batch observations", func(t *testing.T) {
		checker.checkPost(t, router, "/api/packets/observations", `{"hashes":["`+hash+`","0000000000000000"]}`, http.StatusOK)
	})
	t.Run("batch observations with no hashes", func(t *testing.T) {
		checker.checkPost(t, router, "/api/packets/observations", `{"hashes":[]}`, http.StatusOK)
	})
	t.Run("batch observations with a malformed body is a 400", func(t *testing.T) {
		checker.checkPost(t, router, "/api/packets/observations", `not json`, http.StatusBadRequest)
	})
}

// TestPacketRoutesAreNotShadowed goes through the full route table. If
// /api/packets/{id} were registered before the literal routes, these paths
// would reach the detail handler and return a 404 error object.
func TestPacketRoutesAreNotShadowed(t *testing.T) {
	_, router := setupTestServer(t)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/packets/timestamps?since=2000-01-01", nil))
	if rec.Code != http.StatusOK || !strings.HasPrefix(strings.TrimSpace(rec.Body.String()), "[") {
		t.Errorf("timestamps: status %d, body %s; want 200 and an array", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/packets/observations", strings.NewReader(`{"hashes":[]}`))
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"results"`) {
		t.Errorf("observations: status %d, body %s; want 200 and a results object", rec.Code, rec.Body.String())
	}
}

func TestProdPacketFixturesMatchSpec(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromData(api.Spec)
	if err != nil {
		t.Fatalf("load spec: %v", err)
	}

	fixtures := []struct {
		file   string
		schema string
	}{
		{"packets_list_prod.json", "PacketListResponse"},
		{"packets_expanded_prod.json", "PacketListResponse"},
		{"packets_grouped_prod.json", "GroupedPacketListResponse"},
		{"packet_detail_prod.json", "PacketDetailResponse"},
		{"packet_detail_observation_prod.json", "PacketDetailResponse"},
		{"observations_batch_prod.json", "ObservationBatchResponse"},
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
			if err := schema.Value.VisitJSON(decoded); err != nil {
				t.Errorf("production response does not match %s: %v", f.schema, err)
			}
		})
	}

	t.Run("packet_timestamps_prod.json", func(t *testing.T) {
		schema, ok := doc.Components.Schemas["TimestampSeconds"]
		if !ok {
			t.Fatal("TimestampSeconds is not in the spec")
		}
		raw, err := os.ReadFile("testdata/packet_timestamps_prod.json")
		if err != nil {
			t.Fatal(err)
		}
		var decoded []interface{}
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("the timestamps response is not a JSON array: %v", err)
		}
		if len(decoded) == 0 {
			t.Fatal("fixture is empty")
		}
		for _, ts := range decoded {
			if err := schema.Value.VisitJSON(ts); err != nil {
				t.Errorf("timestamp %v does not match TimestampSeconds: %v", ts, err)
			}
		}
	})
}
