package main

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/getkin/kin-openapi/openapi3"
)

// validateAgainstSchema marshals value the way a handler would and checks the
// JSON against the named component schema, so optional fields the fixture
// database never produces are still exercised.
func validateAgainstSchema[T any](t *testing.T, doc *openapi3.T, name string, value T) map[string]interface{} {
	t.Helper()
	schema, ok := doc.Components.Schemas[name]
	if !ok {
		t.Fatalf("schema %q is not in the spec", name)
	}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal %T: %v", value, err)
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal %T: %v", value, err)
	}
	if err := schema.Value.VisitJSON(decoded); err != nil {
		t.Errorf("%T does not match schema %s: %v\n%s", value, name, err, raw)
	}
	return decoded
}

func TestNodeMatchesSpec(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromData(api.Spec)
	if err != nil {
		t.Fatalf("load spec: %v", err)
	}

	heard := time.Date(2026, 11, 6, 13, 0, 0, 123456789, time.UTC)
	stamp := types.WireTimeSeconds{Time: heard}
	name := "Ridge"
	role := types.RoleRepeater
	lat, lon := 39.3, -84.4
	hashSize := 2
	battery := 4100
	temperature := 21.5
	scope := "us-midwest"
	flood := 12

	newBase := func(key string) api.Node {
		return api.Node{
			PublicKey:         key,
			Name:              &name,
			Role:              &role,
			Lat:               &lat,
			Lon:               &lon,
			LastSeen:          &stamp,
			FirstSeen:         &stamp,
			AdvertCount:       7,
			LastHeard:         &stamp,
			HashSize:          &hashSize,
			Foreign:           true,
			BatteryMv:         &battery,
			TemperatureC:      &temperature,
			DefaultScope:      &scope,
			ConfiguredScope:   &scope,
			ConfiguredScopeAt: &stamp,
		}
	}

	full := newBase("aa11")
	full.HashSizeInconsistent = true
	full.HashSizesSeen = []int{1, 2}
	full.FloodAdvertCount7d = &flood
	enrichNodeWithMultiByte(&full, &MultiByteCapEntry{Status: "confirmed", Evidence: "advert", MaxHashSize: 2})
	enrichNodeWithRelayInfo(&full, &RepeaterRelayInfo{
		LastRelayed:           heard,
		RelayActive:           true,
		WindowHours:           24,
		RelayCount1h:          3,
		RelayCount24h:         40,
		UnscopedRelayCount24h: 1,
		TransportedScopes:     []string{"oki"},
	})
	enrichNodeWithUsefulness(&full, &NodeUsefulness{
		TrafficShareScore: 0.4,
		BridgeScore:       0.5,
		CoverageScore:     0.6,
		RedundancyScore:   0.7,
		UsefulnessScore:   0.55,
		UsefulnessGrade:   "B",
	})
	enrichNodeWithDeclaredScope(&full, declaredAnswer{RegionsCSV: "oki", Truncated: true}, true, []string{"oki"})

	got := validateAgainstSchema(t, doc, "Node", full)
	for _, key := range []string{"last_relayed", "relay_active", "relay_count_24h", "transported_scopes", "multi_byte_status", "usefulness_grade", "scope_config_state", "declared_regions"} {
		if _, ok := got[key]; !ok {
			t.Errorf("full node is missing %q", key)
		}
	}

	t.Run("never-relayed repeater omits last_relayed", func(t *testing.T) {
		repeater := newBase("bb22")
		enrichNodeWithRelayInfo(&repeater, &RepeaterRelayInfo{WindowHours: 24})
		got := validateAgainstSchema(t, doc, "Node", repeater)
		if _, present := got["last_relayed"]; present {
			t.Error("last_relayed must be absent when the node has never relayed")
		}
	})

	t.Run("client with no groups", func(t *testing.T) {
		client := newBase("cc33")
		got := validateAgainstSchema(t, doc, "Node", client)
		for _, key := range []string{"relay_active", "usefulness_score", "scope_config_state", "multi_byte_status", "hash_sizes_seen"} {
			if _, present := got[key]; present {
				t.Errorf("%q must be absent on a node without that group", key)
			}
		}
	})
}

func TestNodeEndpointsMatchSpec(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := setupTestServer(t)

	for _, path := range []string{
		"/api/nodes",
		"/api/nodes?limit=1&offset=0&sortBy=name",
		"/api/nodes?role=repeater&lastHeard=7d",
		"/api/nodes/search?q=a",
		"/api/nodes/search",
	} {
		t.Run(path, func(t *testing.T) {
			checker.check(t, router, path)
		})
	}

	t.Run("unparseable before bound is a 400", func(t *testing.T) {
		checker.checkStatus(t, router, "/api/nodes?before=notadate", http.StatusBadRequest)
	})
}

func TestNodeDetailMatchesSpec(t *testing.T) {
	ctx := t.Context()
	checker := newSpecChecker(t)
	srv, router := setupScopeConfigStateServer(t)

	const (
		loneKey  = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
		twinOne  = "deadbeef00000000000000000000000000000000000000000000000000000001"
		twinTwo  = "deadbeef00000000000000000000000000000000000000000000000000000002"
		declared = "be,*"
	)
	scope := declared
	seedScopeConfigNode(t, ctx, srv.db, loneKey, "lone-rp", "repeater", 51.0, 4.0, &scope)
	seedScopeConfigNode(t, ctx, srv.db, twinOne, "twin-one", "repeater", 51.1, 4.1, nil)
	seedScopeConfigNode(t, ctx, srv.db, twinTwo, "twin-two", "repeater", 51.2, 4.2, nil)

	t.Run("full pubkey", func(t *testing.T) {
		checker.check(t, router, "/api/nodes/"+loneKey)
	})
	t.Run("unique prefix resolves to the node", func(t *testing.T) {
		checker.check(t, router, "/api/nodes/"+loneKey[:12])
	})
	t.Run("unknown pubkey is a 404", func(t *testing.T) {
		checker.checkStatus(t, router, "/api/nodes/ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", http.StatusNotFound)
	})
	t.Run("ambiguous prefix is a 409", func(t *testing.T) {
		checker.checkStatus(t, router, "/api/nodes/deadbeef", http.StatusConflict)
	})
}
