package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/gorilla/mux"
)

// registerAnalyticsRoutes registers the analytics endpoints whose responses
// are typed against the spec. The neighbor graph is registered in
// RegisterRoutes.
func (s *Server) registerAnalyticsRoutes(r *mux.Router) {
	r.HandleFunc("/api/analytics/roles", s.handleAnalyticsRoles).Methods("GET")
	r.HandleFunc("/api/analytics/hash-sizes", s.handleAnalyticsHashSizes).Methods("GET")
	r.HandleFunc("/api/analytics/hash-collisions", s.handleAnalyticsHashCollisions).Methods("GET")
	r.HandleFunc("/api/analytics/rf", s.handleAnalyticsRF).Methods("GET")
	r.HandleFunc("/api/analytics/distance", s.handleAnalyticsDistance).Methods("GET")
	r.HandleFunc("/api/analytics/topology", s.handleAnalyticsTopology).Methods("GET")
	r.HandleFunc("/api/analytics/channels", s.handleAnalyticsChannels).Methods("GET")
	r.HandleFunc("/api/analytics/relay-airtime-share", s.handleAnalyticsRelayAirtimeShare).Methods("GET")
	r.HandleFunc("/api/analytics/retransmissions", s.handleAnalyticsRetransmissions).Methods("GET")
	r.HandleFunc("/api/analytics/subpaths", s.handleAnalyticsSubpaths).Methods("GET")
	r.HandleFunc("/api/analytics/subpaths-bulk", s.handleAnalyticsSubpathsBulk).Methods("GET")
	r.HandleFunc("/api/analytics/subpath-detail", s.handleAnalyticsSubpathDetail).Methods("GET")
}

func (s *Server) handleAnalyticsHashSizes(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if s.store != nil {
		region := r.URL.Query().Get("region")
		area := r.URL.Query().Get("area")
		writeJSON(w, s.store.GetAnalyticsHashSizes(ctx, region, area))
		return
	}
	writeJSON(w, api.HashSizeAnalyticsResponse{
		AttributionMethod:       "observer",
		Total:                   0,
		Distribution:            map[string]int{"1": 0, "2": 0, "3": 0},
		DistributionByRepeaters: map[string]int{"1": 0, "2": 0, "3": 0},
		Hourly:                  []api.HashSizeHourly{},
		TopHops:                 []api.HashSizeHop{},
		MultiByteNodes:          []api.MultiByteNode{},
		MultiByteCapability:     []api.MultiByteCapability{},
	})
}

func (s *Server) handleAnalyticsHashCollisions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if s.store != nil {
		region := r.URL.Query().Get("region")
		area := r.URL.Query().Get("area")
		writeJSON(w, s.store.GetAnalyticsHashCollisions(ctx, region, area))
		return
	}
	writeJSON(w, api.HashCollisionResponse{
		BySize:            map[string]api.HashCollisionSize{},
		InconsistentNodes: []api.HashCollisionNode{},
	})
}

func (s *Server) handleAnalyticsRF(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	region := r.URL.Query().Get("region")
	area := r.URL.Query().Get("area")
	window := ParseTimeWindow(r)
	if s.store != nil {
		// Issue #1659: gate the default-shape request behind first-pass
		// completion of the RF recomputer. Otherwise the post-restart
		// in-RAM slice is served until the next recompute tick, and
		// the client pins it via CLIENT_TTL.analyticsRF.
		if region == "" && area == "" && window.IsZero() {
			s.store.analyticsRecomputerMu.RLock()
			rc := s.store.recompRF
			s.store.analyticsRecomputerMu.RUnlock()
			if rc != nil && rc.IsWarmingUp_1659() {
				writeAnalyticsWarmup503(w)
				return
			}
		}
		writeJSON(w, s.store.GetAnalyticsRFWithWindow(ctx, region, area, window))
		return
	}
	writeJSON(w, api.RFAnalyticsResponse{
		SnrValues:      api.Histogram{Bins: []api.HistogramBin{}},
		RssiValues:     api.Histogram{Bins: []api.HistogramBin{}},
		PacketSizes:    api.Histogram{Bins: []api.HistogramBin{}},
		PacketsPerHour: []api.HourlyCount{},
		PayloadTypes:   []api.PayloadTypeEntry{},
		SnrByType:      []api.PayloadTypeSignal{},
		SignalOverTime: []api.SignalOverTimeEntry{},
		ScatterData:    []api.ScatterPoint{},
	})
}

func (s *Server) handleAnalyticsDistance(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	region := r.URL.Query().Get("region")
	area := r.URL.Query().Get("area")
	if s.store != nil {
		// Lazy build (#1011): distance index is not built at startup.
		// First request triggers an async build and gets 202 +
		// Retry-After; concurrent requests during the build window
		// also get 202. Cached results (after the build completes)
		// are served as 200 from the analytics recomputer / TTL cache.
		if !s.store.DistanceIndexBuilt() {
			s.store.TriggerDistanceIndexBuild()
			w.Header().Set("Retry-After", "5")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(api.DistanceBuildingResponse{
				Status:            "building",
				RetryAfterSeconds: 5,
				Detail:            "distance index is being computed (lazy build, #1011). Retry after Retry-After seconds.",
			})
			return
		}
		writeJSON(w, s.store.GetAnalyticsDistance(ctx, region, area))
		return
	}
	writeJSON(w, api.DistanceAnalyticsResponse{
		TopHops:       []api.DistanceHop{},
		TopPaths:      []api.DistancePath{},
		CatStats:      map[string]api.CategoryDistStats{},
		DistHistogram: api.Histogram{Bins: []api.HistogramBin{}},
		DistOverTime:  []api.DistOverTimeEntry{},
	})
}

func (s *Server) handleAnalyticsTopology(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	region := r.URL.Query().Get("region")
	area := r.URL.Query().Get("area")
	window := ParseTimeWindow(r)
	if s.store == nil {
		writeJSON(w, api.TopologyResponse{
			HopDistribution:  []api.HopCount{},
			TopRepeaters:     []api.TopRepeater{},
			TopPairs:         []api.TopPair{},
			HopsVsSnr:        []api.HopSnr{},
			Observers:        []api.TopologyObserver{},
			PerObserverReach: map[string]api.ObserverReach{},
			MultiObsNodes:    []api.MultiObsNode{},
			BestPathList:     []api.BestPathEntry{},
		})
		return
	}
	// #1659 warmup gate (see handleAnalyticsRF for rationale).
	if region == "" && area == "" && window.IsZero() {
		s.store.analyticsRecomputerMu.RLock()
		rc := s.store.recompTopology
		s.store.analyticsRecomputerMu.RUnlock()
		if rc != nil && rc.IsWarmingUp_1659() {
			writeAnalyticsWarmup503(w)
			return
		}
	}
	data := s.store.GetAnalyticsTopologyWithWindow(ctx, region, area, window)
	if s.cfg != nil && (len(s.cfg.NodeBlacklist) > 0 || len(s.cfg.HiddenNamePrefixes) > 0) {
		data = s.filterHiddenFromTopology(ctx, data)
	}
	writeJSON(w, data)
}

// filterHiddenFromTopology removes blacklisted and hidden-prefix nodes (#1181)
// from the topology response. The input comes from a shared cache, so the
// result is built from copies and the input is never modified.
func (s *Server) filterHiddenFromTopology(ctx context.Context, data api.TopologyResponse) api.TopologyResponse {
	// hidden reports whether a node with this public key and name must not
	// be shown. Either may be nil for an unresolved hop.
	hidden := func(pubkey, name *string) bool {
		return (pubkey != nil && s.cfg.IsBlacklisted(*pubkey)) || (name != nil && s.cfg.IsNameHidden(*name))
	}

	repeaters := make([]api.TopRepeater, 0, len(data.TopRepeaters))
	for _, rep := range data.TopRepeaters {
		if !hidden(rep.Pubkey, rep.Name) {
			repeaters = append(repeaters, rep)
		}
	}
	data.TopRepeaters = repeaters

	pairs := make([]api.TopPair, 0, len(data.TopPairs))
	for _, p := range data.TopPairs {
		if !hidden(p.PubkeyA, p.NameA) && !hidden(p.PubkeyB, p.NameB) {
			pairs = append(pairs, p)
		}
	}
	data.TopPairs = pairs

	// The best path list also drops nodes the database marks as hidden.
	bestPaths := make([]api.BestPathEntry, 0, len(data.BestPathList))
	for _, p := range data.BestPathList {
		if p.Pubkey != nil && (s.cfg.IsBlacklisted(*p.Pubkey) || s.isPubkeyHidden(ctx, *p.Pubkey)) {
			continue
		}
		bestPaths = append(bestPaths, p)
	}
	data.BestPathList = bestPaths

	multi := make([]api.MultiObsNode, 0, len(data.MultiObsNodes))
	for _, n := range data.MultiObsNodes {
		if !hidden(n.Pubkey, n.Name) {
			multi = append(multi, n)
		}
	}
	data.MultiObsNodes = multi

	reach := make(map[string]api.ObserverReach, len(data.PerObserverReach))
	for id, obs := range data.PerObserverReach {
		rings := make([]api.ReachRing, 0, len(obs.Rings))
		for _, ring := range obs.Rings {
			nodes := make([]api.TopologyReachNode, 0, len(ring.Nodes))
			for _, n := range ring.Nodes {
				if !hidden(n.Pubkey, n.Name) {
					nodes = append(nodes, n)
				}
			}
			rings = append(rings, api.ReachRing{Hops: ring.Hops, Nodes: nodes})
		}
		reach[id] = api.ObserverReach{ObserverName: obs.ObserverName, Rings: rings}
	}
	data.PerObserverReach = reach
	return data
}

func (s *Server) handleAnalyticsChannels(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	region := r.URL.Query().Get("region")
	area := r.URL.Query().Get("area")
	window := ParseTimeWindow(r)
	if s.store == nil {
		writeJSON(w, api.ChannelAnalyticsResponse{
			Channels:        []api.ChannelSummary{},
			TopSenders:      []api.NameCount{},
			ChannelTimeline: []api.ChannelTimelineEntry{},
			MsgLengths:      []int{},
		})
		return
	}
	// #1659 warmup gate (see handleAnalyticsRF for rationale).
	if region == "" && area == "" && window.IsZero() {
		s.store.analyticsRecomputerMu.RLock()
		rc := s.store.recompChannels
		s.store.analyticsRecomputerMu.RUnlock()
		if rc != nil && rc.IsWarmingUp_1659() {
			writeAnalyticsWarmup503(w)
			return
		}
	}
	writeJSON(w, s.store.GetAnalyticsChannelsWithWindow(ctx, region, area, window))
}

func (s *Server) handleAnalyticsRelayAirtimeShare(w http.ResponseWriter, r *http.Request) {
	window := ParseTimeWindow(r)
	if s.store != nil {
		writeJSON(w, s.store.GetRelayAirtimeShareWithWindow(window))
		return
	}
	writeJSON(w, api.RelayAirtimeShareResponse{
		Rows:   []api.RelayAirtimeRow{},
		Preset: relayPreset(defaultLoRaPreset()),
	})
}

func (s *Server) handleAnalyticsRetransmissions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	region := r.URL.Query().Get("region")
	window := ParseTimeWindow(r)
	bucket := parseRetransmissionBucket(r.URL.Query().Get("bucket"))
	if s.store == nil {
		writeJSON(w, api.RetransmissionResponse{BucketSeconds: int(bucket / time.Second), Buckets: []api.RetransmissionBucket{}})
		return
	}
	// #1659 warmup gate (see handleAnalyticsRF for rationale).
	if isDefaultRetransmissionShape(region, window, bucket) {
		s.store.analyticsRecomputerMu.RLock()
		rc := s.store.recompRetransmissions
		s.store.analyticsRecomputerMu.RUnlock()
		if rc != nil && rc.IsWarmingUp_1659() {
			writeAnalyticsWarmup503(w)
			return
		}
	}
	writeJSON(w, s.store.GetRetransmissionPressure(ctx, region, window, bucket))
}

func (s *Server) handleAnalyticsSubpaths(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if s.store == nil {
		writeJSON(w, api.SubpathList{Subpaths: []api.SubpathEntry{}})
		return
	}
	if !s.store.SubpathIndexReady() {
		writeIndexLoading503(w)
		return
	}
	region := r.URL.Query().Get("region")
	minLen := queryInt(r, "minLen", 2)
	if minLen < 2 {
		minLen = 2
	}
	maxLen := queryInt(r, "maxLen", 8)
	limit := queryLimit(r, 100, s.cfg.ListLimits.AnalyticsMax)
	// Issue #1217: honor the Time window filter on Route Patterns.
	window := ParseTimeWindow(r)
	writeJSON(w, s.store.GetAnalyticsSubpathsWithWindow(ctx, region, minLen, maxLen, limit, window))
}

// subpathGroup defines a length-range + limit for the bulk subpaths endpoint.
type subpathGroup struct {
	MinLen, MaxLen, Limit int
}

// parseSubpathGroups reads the groups parameter of the bulk subpaths endpoint,
// comma-separated minLen-maxLen:limit entries such as 2-2:50,3-3:30.
func parseSubpathGroups(param string) ([]subpathGroup, error) {
	var groups []subpathGroup
	for _, g := range strings.Split(param, ",") {
		parts := strings.SplitN(g, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid group format: %s", g)
		}
		rangeParts := strings.SplitN(parts[0], "-", 2)
		if len(rangeParts) != 2 {
			return nil, fmt.Errorf("invalid range format: %s", parts[0])
		}
		mn, err1 := strconv.Atoi(rangeParts[0])
		mx, err2 := strconv.Atoi(rangeParts[1])
		lim, err3 := strconv.Atoi(parts[1])
		if err1 != nil || err2 != nil || err3 != nil || mn < 2 || mx < mn || lim < 1 {
			return nil, fmt.Errorf("invalid group: %s", g)
		}
		// Uniform clamp per audit-input-vulns-20260603 (MEDIUM): match the
		// /api/analytics/subpaths ceiling so a single bulk request can't
		// allocate more than the per-group endpoint allows.
		if lim > 200 {
			lim = 200
		}
		groups = append(groups, subpathGroup{mn, mx, lim})
	}
	return groups, nil
}

// handleAnalyticsSubpathsBulk returns multiple length-range buckets in a single
// response, avoiding repeated scans of the same packet data. Query format:
//
//	?groups=2-2:50,3-3:30,4-4:20,5-8:15   (minLen-maxLen:limit per group)
func (s *Server) handleAnalyticsSubpathsBulk(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if s.store != nil && !s.store.SubpathIndexReady() {
		writeIndexLoading503(w)
		return
	}
	region := r.URL.Query().Get("region")
	groupsParam := r.URL.Query().Get("groups")
	if groupsParam == "" {
		writeError(w, http.StatusBadRequest, "groups parameter required (e.g. groups=2-2:50,3-3:30)")
		return
	}
	groups, err := parseSubpathGroups(groupsParam)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if s.store == nil {
		results := make([]api.SubpathList, len(groups))
		for i := range results {
			results[i] = api.SubpathList{Subpaths: []api.SubpathEntry{}}
		}
		writeJSON(w, api.SubpathBulkResponse{Results: results})
		return
	}
	results := s.store.GetAnalyticsSubpathsBulkWithWindow(ctx, region, groups, ParseTimeWindow(r))
	writeJSON(w, api.SubpathBulkResponse{Results: results})
}

func (s *Server) handleAnalyticsSubpathDetail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	rawHops := strings.Split(r.URL.Query().Get("hops"), ",")
	if len(rawHops) < 2 {
		writeError(w, http.StatusBadRequest, "Need at least 2 hops")
		return
	}
	// Reject if any hop is a blacklisted or hidden-prefix node (#1181).
	if s.cfg != nil && (len(s.cfg.NodeBlacklist) > 0 || len(s.cfg.HiddenNamePrefixes) > 0) {
		for _, hop := range rawHops {
			if s.cfg.IsBlacklisted(hop) || s.isPubkeyHidden(ctx, hop) {
				writeError(w, http.StatusNotFound, "Not found")
				return
			}
		}
	}
	if s.store == nil {
		writeJSON(w, api.SubpathDetailResponse{
			Hops:             rawHops,
			Nodes:            []api.SubpathNode{},
			HourDistribution: make([]int, 24),
			ParentPaths:      []api.PathCount{},
			Observers:        []api.NameCount{},
		})
		return
	}
	if !s.store.SubpathIndexReady() {
		writeIndexLoading503(w)
		return
	}
	writeJSON(w, s.store.GetSubpathDetail(rawHops))
}
