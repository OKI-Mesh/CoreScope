package main

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/gorilla/mux"
)

func TestTopologyChannelsRelayAndSubpathEndpointsMatchSpec(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := setupTestServer(t)

	paths := []string{
		"/api/analytics/topology",
		"/api/analytics/topology?region=SJC",
		"/api/analytics/topology?area=nowhere",
		"/api/analytics/topology?window=24h",
		"/api/analytics/topology?window=bogus",
		"/api/analytics/topology?from=2026-04-01T00:00:00Z&to=2026-04-08T00:00:00Z",
		"/api/analytics/channels",
		"/api/analytics/channels?region=SJC",
		"/api/analytics/channels?window=24h",
		"/api/analytics/relay-airtime-share",
		"/api/analytics/relay-airtime-share?window=24h",
		"/api/analytics/retransmissions",
		"/api/analytics/retransmissions?bucket=15m",
		"/api/analytics/retransmissions?window=7d&region=SJC",
		"/api/analytics/subpaths",
		"/api/analytics/subpaths?minLen=2&maxLen=4&limit=10",
		"/api/analytics/subpaths?region=SJC",
		"/api/analytics/subpaths?window=24h",
		"/api/analytics/subpaths-bulk?groups=2-2:50,3-3:30,5-8:15",
		"/api/analytics/subpaths-bulk?groups=2-4:10&region=SJC",
		"/api/analytics/subpaths-bulk?groups=2-4:10&window=24h",
		"/api/analytics/subpath-detail?hops=aa,bb",
		"/api/analytics/subpath-detail?hops=eeff,0011",
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			checker.check(t, router, path)
		})
	}
}

func TestSubpathErrorResponsesMatchSpec(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := setupTestServer(t)

	for _, path := range []string{
		"/api/analytics/subpaths-bulk",
		"/api/analytics/subpaths-bulk?groups=bad",
		"/api/analytics/subpaths-bulk?groups=2-2",
		"/api/analytics/subpaths-bulk?groups=x-y:5",
		"/api/analytics/subpaths-bulk?groups=1-3:5",
		"/api/analytics/subpath-detail",
		"/api/analytics/subpath-detail?hops=aa",
	} {
		t.Run(path, func(t *testing.T) {
			checker.checkStatus(t, router, path, http.StatusBadRequest)
		})
	}
}

// TestBlacklistedSubpathDetailResponseMatchesSpec checks the 404 answered when
// a requested hop belongs to a blacklisted node.
func TestBlacklistedSubpathDetailResponseMatchesSpec(t *testing.T) {
	checker := newSpecChecker(t)
	db := setupTestDB(t)
	srv := NewServer(db, &Config{NodeBlacklist: []string{"badnode"}}, NewHub())
	router := setupTestRouter(srv)

	checker.checkStatus(t, router, "/api/analytics/subpath-detail?hops=badnode,othernode", http.StatusNotFound)
}

// TestSubpathIndexLoadingResponseMatchesSpec checks the 503 the subpath
// routes answer while the index is still being built.
func TestSubpathIndexLoadingResponseMatchesSpec(t *testing.T) {
	checker := newSpecChecker(t)
	srv, router := setupTestServer(t)
	srv.store.subpathReady.Store(false)

	for _, path := range []string{
		"/api/analytics/subpaths",
		"/api/analytics/subpaths-bulk?groups=2-3:10",
		"/api/analytics/subpath-detail?hops=aa,bb",
	} {
		t.Run(path, func(t *testing.T) {
			checker.checkStatus(t, router, path, http.StatusServiceUnavailable)
		})
	}
}

// TestAnalyticsWarmupResponsesMatchSpec checks the 503 the unfiltered topology,
// channels and retransmissions queries answer until their background snapshot
// has finished its first pass.
func TestAnalyticsWarmupResponsesMatchSpec(t *testing.T) {
	ctx := t.Context()
	checker := newSpecChecker(t)

	db := setupTestDB(t)
	store := newPacketStoreForTest(t, db)
	block := make(chan struct{})
	defer close(block)
	store.installWarmupBlocker_1659(ctx, block)
	store.analyticsRecomputerMu.Lock()
	store.recompRetransmissions = newAnalyticsRecomputer(ctx, "retransmissions-test-block", time.Hour,
		func(context.Context) interface{} {
			<-block
			return nil
		})
	store.analyticsRecomputerMu.Unlock()

	srv := NewServer(db, &Config{Port: 3000}, NewHub())
	srv.store = store
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	for _, path := range []string{
		"/api/analytics/topology",
		"/api/analytics/channels",
		"/api/analytics/retransmissions",
	} {
		t.Run(path, func(t *testing.T) {
			checker.checkStatus(t, router, path, http.StatusServiceUnavailable)
		})
	}
	// A filtered query skips the gate and is served normally.
	checker.check(t, router, "/api/analytics/topology?window=24h")
	checker.check(t, router, "/api/analytics/channels?window=24h")
	checker.check(t, router, "/api/analytics/retransmissions?window=24h")
}

// TestAnalyticsOutputMatchesSpec checks the marshaled results for a populated
// store, for unresolved and resolved hops, and for an empty store, where the
// lists must still be present.
func TestAnalyticsOutputMatchesSpec(t *testing.T) {
	ctx := t.Context()
	doc := loadSpec(t)

	richDB := setupRichTestDB(t)
	defer richDB.Close()
	rich := newPacketStoreForTest(t, richDB)
	if err := rich.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}
	if !rich.WaitIndexesReady(5 * time.Second) {
		t.Fatal("indexes never became ready")
	}
	topology := rich.GetAnalyticsTopology(ctx, "", "")
	if len(topology.HopDistribution) == 0 || len(topology.TopRepeaters) == 0 || len(topology.PerObserverReach) == 0 {
		t.Errorf("rich store left a section empty: hopDistribution=%d topRepeaters=%d perObserverReach=%d",
			len(topology.HopDistribution), len(topology.TopRepeaters), len(topology.PerObserverReach))
	}
	subpathDetail := rich.GetSubpathDetail([]string{"eeff", "0011"})
	if subpathDetail.TotalMatches == 0 || len(subpathDetail.Nodes) != 2 {
		t.Errorf("subpath detail left a section empty: totalMatches=%d nodes=%d", subpathDetail.TotalMatches, len(subpathDetail.Nodes))
	}

	emptyDB := setupTestDB(t)
	empty := newPacketStoreForTest(t, emptyDB)
	if err := empty.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}
	if !empty.WaitIndexesReady(5 * time.Second) {
		t.Fatal("indexes never became ready")
	}

	responses := []struct {
		name   string
		schema string
		value  interface{}
	}{
		{"topology", "TopologyResponse", topology},
		{"topology with no data", "TopologyResponse", empty.GetAnalyticsTopology(ctx, "", "")},
		{"channels", "ChannelAnalyticsResponse", rich.GetAnalyticsChannels(ctx, "", "")},
		{"channels with no data", "ChannelAnalyticsResponse", empty.GetAnalyticsChannels(ctx, "", "")},
		{"relay airtime share", "RelayAirtimeShareResponse", rich.GetRelayAirtimeShareWithWindow(TimeWindow{})},
		{"relay airtime share from the cache", "RelayAirtimeShareResponse", rich.GetRelayAirtimeShareWithWindow(TimeWindow{})},
		{"relay airtime share with no data", "RelayAirtimeShareResponse", empty.GetRelayAirtimeShareWithWindow(TimeWindow{})},
		{"retransmissions", "RetransmissionResponse", rich.GetRetransmissionPressure(ctx, "", TimeWindow{}, time.Hour)},
		{"retransmissions with no data", "RetransmissionResponse", empty.GetRetransmissionPressure(ctx, "", TimeWindow{}, time.Hour)},
		{"subpaths", "SubpathList", rich.GetAnalyticsSubpaths(ctx, "", 2, 8, 100)},
		{"subpaths for a region", "SubpathList", rich.GetAnalyticsSubpaths(ctx, "SJC", 2, 4, 50)},
		{"subpaths with no data", "SubpathList", empty.GetAnalyticsSubpaths(ctx, "", 2, 8, 100)},
		{"subpath detail", "SubpathDetailResponse", subpathDetail},
		{"subpath detail with no matches", "SubpathDetailResponse", rich.GetSubpathDetail([]string{"zzzz", "yyyy"})},
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

// TestPopulatedAnalyticsResponsesMatchSpec fills the sections the fixture
// database leaves empty: unresolved and resolved hops, a ring distance range,
// the relay preset and a channel timeline.
func TestPopulatedAnalyticsResponsesMatchSpec(t *testing.T) {
	doc := loadSpec(t)
	name := "Repeater 01"
	pubkey := "aa01"
	distRange := "1-3"
	lat, lon := 47.5, -122.25
	avgSnr, avgRssi := 6.5, -91.0

	topology := api.TopologyResponse{
		UniqueNodes:     2,
		AvgHops:         2.5,
		MedianHops:      2,
		MaxHops:         4,
		HopDistribution: []api.HopCount{{Hops: 1, Count: 3}, {Hops: 2, Count: 5}},
		TopRepeaters:    []api.TopRepeater{{Hop: "aa", Count: 8, Name: &name, Pubkey: &pubkey}, {Hop: "bb", Count: 2}},
		TopPairs:        []api.TopPair{{HopA: "aa", HopB: "bb", Count: 2, NameA: &name, PubkeyA: &pubkey}},
		HopsVsSnr:       []api.HopSnr{{Hops: 1, Count: 3, AvgSnr: 7.25}},
		Observers:       []api.TopologyObserver{{Id: "obs1", Name: "Observer 01"}},
		PerObserverReach: map[string]api.ObserverReach{
			"obs1": {ObserverName: "Observer 01", Rings: []api.ReachRing{{Hops: 1, Nodes: []api.TopologyReachNode{
				{Hop: "aa", Name: &name, Pubkey: &pubkey, Count: 8, DistRange: &distRange},
				{Hop: "bb", Count: 2},
			}}}},
		},
		MultiObsNodes: []api.MultiObsNode{{Hop: "aa", Name: &name, Pubkey: &pubkey, Observers: []api.MultiObsObserver{
			{ObserverId: "obs1", ObserverName: "Observer 01", MinDist: 1, Count: 5},
			{ObserverId: "obs2", ObserverName: "Observer 02", MinDist: 2, Count: 3},
		}}},
		BestPathList: []api.BestPathEntry{{Hop: "aa", Name: &name, Pubkey: &pubkey, MinDist: 1, ObserverId: "obs1", ObserverName: "Observer 01"}},
	}
	channels := api.ChannelAnalyticsResponse{
		ActiveChannels: 1,
		Decryptable:    1,
		Channels: []api.ChannelSummary{{
			Hash: "129", Name: "Channel 01", Messages: 4, Senders: 2, Encrypted: false,
			LastActivity: types.WireTimeSeconds{Time: time.Date(2026, 11, 5, 15, 30, 0, 123456789, time.UTC)},
		}},
		TopSenders:      []api.NameCount{{Name: "Sender 01", Count: 3}},
		ChannelTimeline: []api.ChannelTimelineEntry{{Hour: "2026-11-05T15", Channel: "Channel 01", Count: 4}},
		MsgLengths:      []int{12, 40},
	}
	relay := api.RelayAirtimeShareResponse{
		Rows: []api.RelayAirtimeRow{{
			PayloadType: "ADVERT", Type: types.PayloadADVERT, Count: 1, CountPct: 50, Score: 123456789, AirtimePct: 83.9,
		}},
		TotalCount: 2,
		TotalScore: 147000000,
		Preset:     relayPreset(defaultLoRaPreset()),
		Window:     "24h",
		Cached:     true,
	}
	retransmissions := api.RetransmissionResponse{
		BucketSeconds: 3600,
		Window:        "24h",
		Region:        "SJC",
		Summary:       api.RetransmissionSummary{Packets: 2, AvgRepeaters: 3.5, Observers: 2, OneBytePackets: 1, NoRepeaterPackets: 1},
		Buckets: []api.RetransmissionBucket{{
			Start: types.WireTimeSeconds{Time: time.Date(2026, 11, 5, 15, 0, 0, 0, time.UTC)}, Packets: 2, RepeaterSum: 7, AvgRepeaters: 3.5, Observers: 2,
		}},
	}
	detail := api.SubpathDetailResponse{
		Hops:         []string{"aa", "bb"},
		Nodes:        []api.SubpathNode{{Hop: "aa", Name: &name, Pubkey: &pubkey, Lat: &lat, Lon: &lon}, {Hop: "bb", Name: &name}},
		TotalMatches: 3,
		FirstSeen:    types.WireTimeSeconds{Time: time.Date(2026, 11, 5, 15, 30, 0, 0, time.UTC)},
		LastSeen:     types.WireTimeSeconds{Time: time.Date(2026, 11, 6, 15, 30, 0, 0, time.UTC)},
		Signal:       api.SubpathSignal{AvgSnr: &avgSnr, AvgRssi: &avgRssi, Samples: 3},
		HourDistribution: func() []int {
			h := make([]int, 24)
			h[15] = 3
			return h
		}(),
		ParentPaths: []api.PathCount{{Path: "Repeater 01 → Repeater 02 → cc", Count: 3}},
		Observers:   []api.NameCount{{Name: "Observer 01", Count: 3}},
	}

	responses := []struct {
		name   string
		schema string
		value  interface{}
	}{
		{"topology", "TopologyResponse", topology},
		{"channels", "ChannelAnalyticsResponse", channels},
		{"relay airtime share", "RelayAirtimeShareResponse", relay},
		{"retransmissions", "RetransmissionResponse", retransmissions},
		{"subpath detail", "SubpathDetailResponse", detail},
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

// TestTopologyFilterDropsHiddenNodes checks that blacklisted public keys and
// hidden name prefixes are removed from every list, and that the cached input
// is left as it was.
func TestTopologyFilterDropsHiddenNodes(t *testing.T) {
	cfg := &Config{NodeBlacklist: []string{"badkey"}, HiddenNamePrefixes: []string{"Hidden"}}
	db := setupTestDB(t)
	defer db.Close()
	srv := &Server{cfg: cfg, db: db}

	good, bad, hiddenName := "Good", "Bad", "Hidden One"
	goodKey, badKey, otherKey, dbHiddenKey := "goodkey", "badkey", "otherkey", padKey("dd01")
	// A node the best path list knows only by public key; its name is hidden
	// in the database.
	seedNode(t, db, dbHiddenKey, "Hidden Two", "repeater", nil, nil, time.Now().UTC().Truncate(time.Second))

	input := api.TopologyResponse{
		TopRepeaters: []api.TopRepeater{
			{Hop: "aa", Count: 3, Name: &good, Pubkey: &goodKey},
			{Hop: "bb", Count: 2, Name: &bad, Pubkey: &badKey},
			{Hop: "cc", Count: 1, Name: &hiddenName, Pubkey: &otherKey},
			{Hop: "dd", Count: 1},
		},
		TopPairs: []api.TopPair{
			{HopA: "aa", HopB: "dd", Count: 2, NameA: &good, PubkeyA: &goodKey},
			{HopA: "aa", HopB: "bb", Count: 2, NameA: &good, PubkeyA: &goodKey, NameB: &bad, PubkeyB: &badKey},
			{HopA: "cc", HopB: "dd", Count: 1, NameA: &hiddenName, PubkeyA: &otherKey},
		},
		BestPathList: []api.BestPathEntry{
			{Hop: "aa", Name: &good, Pubkey: &goodKey, MinDist: 1},
			{Hop: "bb", Name: &bad, Pubkey: &badKey, MinDist: 1},
			{Hop: "ee", Pubkey: &dbHiddenKey, MinDist: 2},
		},
		MultiObsNodes: []api.MultiObsNode{
			{Hop: "aa", Name: &good, Pubkey: &goodKey},
			{Hop: "bb", Name: &bad, Pubkey: &badKey},
		},
		PerObserverReach: map[string]api.ObserverReach{
			"obs1": {ObserverName: "Observer 01", Rings: []api.ReachRing{{Hops: 1, Nodes: []api.TopologyReachNode{
				{Hop: "aa", Name: &good, Pubkey: &goodKey, Count: 3},
				{Hop: "bb", Name: &bad, Pubkey: &badKey, Count: 2},
				{Hop: "cc", Name: &hiddenName, Pubkey: &otherKey, Count: 1},
			}}}},
		},
	}

	got := srv.filterHiddenFromTopology(t.Context(), input)

	if len(got.TopRepeaters) != 2 || got.TopRepeaters[0].Hop != "aa" || got.TopRepeaters[1].Hop != "dd" {
		t.Errorf("topRepeaters = %+v, want hops aa and dd", got.TopRepeaters)
	}
	if len(got.TopPairs) != 1 || got.TopPairs[0].HopA != "aa" || got.TopPairs[0].HopB != "dd" {
		t.Errorf("topPairs = %+v, want only the aa/dd pair", got.TopPairs)
	}
	if len(got.BestPathList) != 1 || got.BestPathList[0].Hop != "aa" {
		t.Errorf("bestPathList = %+v, want only hop aa", got.BestPathList)
	}
	if len(got.MultiObsNodes) != 1 || got.MultiObsNodes[0].Hop != "aa" {
		t.Errorf("multiObsNodes = %+v, want only hop aa", got.MultiObsNodes)
	}
	nodes := got.PerObserverReach["obs1"].Rings[0].Nodes
	if len(nodes) != 1 || nodes[0].Hop != "aa" {
		t.Errorf("perObserverReach nodes = %+v, want only hop aa", nodes)
	}

	// The input is a shared cache entry and must come back unchanged.
	if len(input.TopRepeaters) != 4 || len(input.TopPairs) != 3 || len(input.BestPathList) != 3 ||
		len(input.MultiObsNodes) != 2 || len(input.PerObserverReach["obs1"].Rings[0].Nodes) != 3 {
		t.Errorf("filter modified its input: %+v", input)
	}
}

func TestProdAnalyticsFixturesMatchSpec(t *testing.T) {
	checkProdFixtures(t, []prodFixture{
		{"topology_prod.json", "TopologyResponse"},
		{"channels_prod.json", "ChannelAnalyticsResponse"},
		{"relay_airtime_prod.json", "RelayAirtimeShareResponse"},
		{"relay_airtime_window_prod.json", "RelayAirtimeShareResponse"},
		{"retransmissions_prod.json", "RetransmissionResponse"},
		{"retransmissions_window_prod.json", "RetransmissionResponse"},
		{"subpaths_prod.json", "SubpathList"},
		{"subpaths_bulk_prod.json", "SubpathBulkResponse"},
		{"subpath_detail_prod.json", "SubpathDetailResponse"},
	})
}
