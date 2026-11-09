package main

import (
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/gorilla/mux"
)

// seededNode is the repeater seedTestData advertises twice, one hour and one
// day ago.
const seededNode = "aabbccdd11223344"

func TestNodeHealthMatchesSpec(t *testing.T) {
	checker := newSpecChecker(t)
	srv, router := setupTestServer(t)

	t.Run("node with packets", func(t *testing.T) {
		var resp api.NodeHealthResponse
		decodeInto(t, checker.checkStatus(t, router, "/api/nodes/"+seededNode+"/health", http.StatusOK).Body.Bytes(), &resp)
		if resp.Node.PublicKey != seededNode {
			t.Errorf("node = %q, want %q", resp.Node.PublicKey, seededNode)
		}
		if resp.Stats.TotalPackets != 2 || resp.Stats.TotalTransmissions != 2 {
			t.Errorf("stats = %+v, want 2 packets", resp.Stats)
		}
		if resp.Stats.LastHeard == nil || resp.Stats.AvgSnr == nil {
			t.Errorf("lastHeard=%v avgSnr=%v, want both set for a node with packets", resp.Stats.LastHeard, resp.Stats.AvgSnr)
		}
		if len(resp.RecentPackets) != 2 {
			t.Errorf("recentPackets = %d, want 2", len(resp.RecentPackets))
		}
	})

	t.Run("node with no packets has null lastHeard and avgSnr", func(t *testing.T) {
		quiet := padKey("5151")
		seedNode(t, srv.db, quiet, "Quiet", types.RoleRepeater, nil, nil, time.Now().UTC())
		var resp api.NodeHealthResponse
		decodeInto(t, checker.checkStatus(t, router, "/api/nodes/"+quiet+"/health", http.StatusOK).Body.Bytes(), &resp)
		if resp.Stats.LastHeard != nil || resp.Stats.AvgSnr != nil || resp.Stats.TotalPackets != 0 {
			t.Errorf("stats = %+v, want zero counts and null lastHeard and avgSnr", resp.Stats)
		}
		if resp.Observers == nil || resp.RecentPackets == nil {
			t.Error("observers and recentPackets must be empty arrays, not null")
		}
	})

	t.Run("unknown node is a 404", func(t *testing.T) {
		checker.checkStatus(t, router, "/api/nodes/"+padKey("ffff")+"/health", http.StatusNotFound)
	})
}

func TestNodePathsMatchSpec(t *testing.T) {
	ctx := t.Context()
	checker := newSpecChecker(t)
	db := setupTestDB(t)
	recent := time.Now().UTC().Add(-1 * time.Hour)

	target := padKey("7acb1111")
	lat, lon := 37.5, -122.0
	mustCreateTestNode(t, ctx, db, database.NodeInput{
		PublicKey: target, Name: "NodeA", Role: "repeater", Lat: &lat, Lon: &lon, RxTime: recent, IngestNow: recent,
	})

	// Two paths through the target: one with a resolved hop that has GPS
	// followed by a hop the resolver could not name, and a one-hop path.
	twoHops := mustCreateTestTransmission(t, ctx, db, testTransmission("AA", "hash_two", "", types.RouteFlood, types.PayloadADVERT, ""), recent)
	mustInsertUnobservedTestObservation(t, ctx, db, twoHops, recent, 0, 0, []string{"7a", "c3"}, []*string{&target, nil})
	oneHop := mustCreateTestTransmission(t, ctx, db, testTransmission("BB", "hash_one", "", types.RouteFlood, types.PayloadADVERT, ""), recent.Add(-2*time.Hour))
	mustInsertUnobservedTestObservation(t, ctx, db, oneHop, recent.Add(-2*time.Hour), 0, 0, []string{"7a"}, []*string{&target})

	srv := NewServer(db, &Config{Port: 3000}, NewHub())
	srv.store = loadStoreForPaths(t, db)
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	var resp api.NodePathsResponse
	decodeInto(t, checker.checkStatus(t, router, "/api/nodes/"+target+"/paths", http.StatusOK).Body.Bytes(), &resp)

	if resp.Node.PublicKey != target || resp.Node.Name == nil || *resp.Node.Name != "NodeA" || resp.Node.Lat == nil || *resp.Node.Lat != lat {
		t.Errorf("node = %+v, want %s named NodeA at %v", resp.Node, target, lat)
	}
	if resp.TotalPaths != 2 || resp.TotalTransmissions != 2 || len(resp.Paths) != 2 {
		t.Fatalf("totalPaths=%d totalTransmissions=%d paths=%d, want 2 of each", resp.TotalPaths, resp.TotalTransmissions, len(resp.Paths))
	}
	newest := resp.Paths[0]
	if len(newest.Hops) != 2 || newest.SampleHash != "hash_two" || newest.LastSeen == nil {
		t.Fatalf("most recent path = %+v, want the two-hop path first with lastSeen", newest)
	}
	named, unresolved := newest.Hops[0], newest.Hops[1]
	if named.Pubkey == nil || *named.Pubkey != target || named.Name != "NodeA" || named.Lat == nil || named.Lon == nil {
		t.Errorf("resolved hop = %+v, want the target with its name and position", named)
	}
	if unresolved.Pubkey != nil || unresolved.Lat != nil || unresolved.Lon != nil || unresolved.Name != "C3" || unresolved.Prefix != "C3" {
		t.Errorf("unresolved hop = %+v, want null pubkey and position and the prefix as its name", unresolved)
	}

	t.Run("unknown node is a 404", func(t *testing.T) {
		checker.checkStatus(t, router, "/api/nodes/"+padKey("ffff")+"/paths", http.StatusNotFound)
	})

	t.Run("no packet store is a 503 error", func(t *testing.T) {
		bare := NewServer(db, &Config{Port: 3000}, NewHub())
		bareRouter := mux.NewRouter()
		bare.RegisterRoutes(bareRouter)
		rec := checker.checkStatus(t, bareRouter, "/api/nodes/"+target+"/paths", http.StatusServiceUnavailable)
		var body api.ErrorResponse
		decodeInto(t, rec.Body.Bytes(), &body)
		if body.Error != "Packet store unavailable" {
			t.Errorf("error = %q", body.Error)
		}
		if rec.Header().Get("Retry-After") != "" {
			t.Error("an unavailable store is not retryable, so Retry-After must be absent")
		}
	})
}

func TestNodeAnalyticsMatchSpec(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := setupTestServer(t)

	var resp api.NodeAnalyticsResponse
	decodeInto(t, checker.checkStatus(t, router, "/api/nodes/"+seededNode+"/analytics?days=7", http.StatusOK).Body.Bytes(), &resp)

	if resp.TimeRange.Days != 7 {
		t.Errorf("timeRange.days = %d, want 7", resp.TimeRange.Days)
	}
	if resp.ComputedStats.TotalPackets != 2 || len(resp.SnrTrend) != 2 || len(resp.ActivityTimeline) != 2 {
		t.Fatalf("totalPackets=%d snrTrend=%d activityTimeline=%d, want 2 of each", resp.ComputedStats.TotalPackets, len(resp.SnrTrend), len(resp.ActivityTimeline))
	}
	// The two adverts are 23 hours apart, so that gap is the longest silence.
	if resp.ComputedStats.LongestSilenceMs != int((23*time.Hour).Milliseconds()) || resp.ComputedStats.LongestSilenceStart == nil {
		t.Errorf("longestSilence = %d ms from %v, want 23h with a start", resp.ComputedStats.LongestSilenceMs, resp.ComputedStats.LongestSilenceStart)
	}
	if resp.ComputedStats.UniqueObservers != len(resp.ObserverCoverage) || len(resp.ObserverCoverage) == 0 {
		t.Errorf("uniqueObservers=%d observerCoverage=%d", resp.ComputedStats.UniqueObservers, len(resp.ObserverCoverage))
	}

	t.Run("days is clamped to 1..365", func(t *testing.T) {
		for _, tc := range []struct {
			query string
			want  int
		}{{"?days=0", 1}, {"?days=999", 365}, {"", 7}} {
			var r api.NodeAnalyticsResponse
			decodeInto(t, checker.checkStatus(t, router, "/api/nodes/"+seededNode+"/analytics"+tc.query, http.StatusOK).Body.Bytes(), &r)
			if r.TimeRange.Days != tc.want {
				t.Errorf("%q: days = %d, want %d", tc.query, r.TimeRange.Days, tc.want)
			}
		}
	})

	t.Run("a node with no packets has empty arrays", func(t *testing.T) {
		quiet := padKey("5252")
		srv, quietRouter := setupTestServer(t)
		seedNode(t, srv.db, quiet, "Quiet", types.RoleRepeater, nil, nil, time.Now().UTC())
		var r api.NodeAnalyticsResponse
		decodeInto(t, checker.checkStatus(t, quietRouter, "/api/nodes/"+quiet+"/analytics", http.StatusOK).Body.Bytes(), &r)
		if r.ComputedStats.LongestSilenceStart != nil || r.ComputedStats.TotalPackets != 0 || r.ComputedStats.SignalGrade != "D" {
			t.Errorf("computedStats = %+v, want zero packets, null longestSilenceStart, grade D", r.ComputedStats)
		}
		if r.ActivityTimeline == nil || r.SnrTrend == nil || r.PacketTypeBreakdown == nil || r.ObserverCoverage == nil ||
			r.HopDistribution == nil || r.PeerInteractions == nil || r.UptimeHeatmap == nil {
			t.Error("every list must be an empty array, not null")
		}
	})

	t.Run("unknown node is a 404", func(t *testing.T) {
		checker.checkStatus(t, router, "/api/nodes/"+padKey("ffff")+"/analytics", http.StatusNotFound)
	})
}

func TestNodeHopAnalyticsMatchSpec(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := setupTestServer(t)

	var resp api.NodeHopAnalyticsResponse
	decodeInto(t, checker.checkStatus(t, router, "/api/nodes/"+seededNode+"/hop_analytics?days=0", http.StatusOK).Body.Bytes(), &resp)
	if resp.TimeRange.Days != 1 || resp.Packets == nil {
		t.Errorf("days = %d, packets nil = %v, want days clamped to 1 and an empty array", resp.TimeRange.Days, resp.Packets == nil)
	}

	t.Run("unknown node is a 404", func(t *testing.T) {
		checker.checkStatus(t, router, "/api/nodes/"+padKey("ffff")+"/hop_analytics", http.StatusNotFound)
	})
}

// TestNodeRoutesHideBlacklistedNodes sends each node route a blacklisted
// pubkey: all four answer the same 404 as an unknown node.
func TestNodeRoutesHideBlacklistedNodes(t *testing.T) {
	checker := newSpecChecker(t)
	db := setupTestDB(t)
	banned := padKey("bad0")
	seedNode(t, db, banned, "Banned", types.RoleRepeater, nil, nil, time.Now().UTC())
	srv := NewServer(db, &Config{Port: 3000, NodeBlacklist: []string{banned}}, NewHub())
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	for _, route := range []string{"health", "paths", "analytics", "hop_analytics"} {
		t.Run(route, func(t *testing.T) {
			checker.checkStatus(t, router, "/api/nodes/"+banned+"/"+route, http.StatusNotFound)
		})
	}
}

// TestNodeRouteSchemasAcceptOptionalValues marshals values the fixture
// database never produces, so every nullable and optional field of the node
// route schemas is exercised in both states.
func TestNodeRouteSchemasAcceptOptionalValues(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromData(api.Spec)
	if err != nil {
		t.Fatalf("load spec: %v", err)
	}
	at := types.WireTimeSeconds{Time: time.Date(2026, 11, 9, 19, 44, 50, 987654321, time.UTC)}
	snr, rssi, lat, lon := 9.5, -84.0, 39.1, -84.5
	pubkey, observer := padKey("c3c3"), "OBS"

	validateAgainstSchema(t, doc, "NodeHealthStats", api.NodeHealthStats{
		TotalTransmissions: 3, TotalObservations: 9, TotalPackets: 3, PacketsToday: 1, AvgSnr: &snr, AvgHops: 4, LastHeard: &at,
	})
	validateAgainstSchema(t, doc, "NodeHealthStats", api.NodeHealthStats{})
	validateAgainstSchema(t, doc, "NodePathsNode", api.NodePathsNode{PublicKey: pubkey})
	validateAgainstSchema(t, doc, "NodePathsNode", api.NodePathsNode{PublicKey: pubkey, Name: &observer, Lat: &lat, Lon: &lon})
	validateAgainstSchema(t, doc, "NodePathHop", api.NodePathHop{Prefix: "C3", Name: "C3"})
	validateAgainstSchema(t, doc, "NodePathHop", api.NodePathHop{Prefix: "C3", Name: "Ridge", Pubkey: &pubkey, Lat: &lat, Lon: &lon})
	validateAgainstSchema(t, doc, "NodePath", api.NodePath{Hops: []api.NodePathHop{}, SampleHash: "h"})
	validateAgainstSchema(t, doc, "NodePath", api.NodePath{Hops: []api.NodePathHop{}, Count: 2, LastSeen: &at, SampleHash: "h"})
	validateAgainstSchema(t, doc, "NodeSnrSample", api.NodeSnrSample{Timestamp: at, Snr: snr})
	validateAgainstSchema(t, doc, "NodeSnrSample", api.NodeSnrSample{Timestamp: at, Snr: snr, Rssi: &rssi, ObserverId: &observer, ObserverName: &observer})
	validateAgainstSchema(t, doc, "NodeObserverCoverage", api.NodeObserverCoverage{ObserverId: observer, FirstSeen: at, LastSeen: at})
	validateAgainstSchema(t, doc, "NodeObserverCoverage", api.NodeObserverCoverage{ObserverId: observer, ObserverName: "Obs", PacketCount: 4, AvgSnr: &snr, AvgRssi: &rssi, FirstSeen: at, LastSeen: at})
	validateAgainstSchema(t, doc, "NodeComputedStats", api.NodeComputedStats{SignalGrade: "D"})
	validateAgainstSchema(t, doc, "NodeComputedStats", api.NodeComputedStats{SignalGrade: "B+", LongestSilenceMs: 3600000, LongestSilenceStart: &at})
	validateAgainstSchema(t, doc, "NodePeerInteraction", api.NodePeerInteraction{PeerKey: pubkey, PeerName: "Peer", MessageCount: 2, LastContact: at})
	validateAgainstSchema(t, doc, "NodeHopAnalyticsResponse", api.NodeHopAnalyticsResponse{
		TimeRange: api.NodeTimeRange{From: at, To: at, Days: 7},
		Packets: []api.NodeHopPacket{
			{Hash: "h1", Timestamp: at, Hops: 2, Tags: []string{"flood", "unscoped"}},
			{Hash: "h2", Timestamp: at, Hops: 0, Tags: []string{"flood", "scoped", "advert"}},
		},
		Ambiguous: 3,
	})
}

// TestNodeRouteProdFixturesMatchSpec validates trimmed, anonymized copies of
// real production responses. The spec has to accept exactly what the live
// frontend receives, so a field it requires but production omits fails here.
func TestNodeRouteProdFixturesMatchSpec(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromData(api.Spec)
	if err != nil {
		t.Fatalf("load spec: %v", err)
	}
	for _, tc := range []struct{ schema, file string }{
		{"NodeHealthResponse", "node_health_prod.json"},
		{"NodePathsResponse", "node_paths_prod.json"},
		{"NodeAnalyticsResponse", "node_analytics_prod.json"},
		{"NodeHopAnalyticsResponse", "node_hop_analytics_prod.json"},
	} {
		t.Run(tc.schema, func(t *testing.T) {
			schema, ok := doc.Components.Schemas[tc.schema]
			if !ok {
				t.Fatalf("%s is not in the spec", tc.schema)
			}
			raw, err := os.ReadFile("testdata/" + tc.file)
			if err != nil {
				t.Fatal(err)
			}
			var decoded interface{}
			if err := json.Unmarshal(raw, &decoded); err != nil {
				t.Fatalf("decode fixture: %v", err)
			}
			if err := schema.Value.VisitJSON(decoded); err != nil {
				t.Errorf("production response does not match %s: %v", tc.schema, err)
			}
		})
	}
}
