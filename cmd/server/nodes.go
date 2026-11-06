package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/geofilter"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/gorilla/mux"
)

type NodeAdvertResponse struct {
	PacketResponse
	Observations []api.NodeAdvertObservation `json:"observations"`
}

type NodeHealthStats struct {
	TotalTransmissions int      `json:"totalTransmissions"`
	TotalObservations  int      `json:"totalObservations"`
	TotalPackets       int      `json:"totalPackets"`
	PacketsToday       int      `json:"packetsToday"`
	AvgSnr             *float64 `json:"avgSnr"` // null when no packet carried SNR
	AvgHops            int      `json:"avgHops"`
	LastHeard          *string  `json:"lastHeard"` // null when there are no packets
}

type NodeHealthResponse struct {
	Node               api.Node                 `json:"node"`      // becomes NodeResponse with the nodes conversion
	Observers          []map[string]interface{} `json:"observers"` // use the type buildDirectObserverRows returns
	RelayObserverCount int                      `json:"relayObserverCount"`
	Stats              NodeHealthStats          `json:"stats"`
	RecentPackets      []map[string]interface{} `json:"recentPackets"` // becomes []PacketResponse with the packet responses
}

func (s *Server) registerNodeRoutes(r *mux.Router) {
	// Fixed routes BEFORE the parameterized /api/nodes/{pubkey}.
	r.HandleFunc("/api/nodes/search", s.handleNodeSearch).Methods("GET")
	r.HandleFunc("/api/nodes/bulk-health", s.handleBulkHealth).Methods("GET")
	r.HandleFunc("/api/nodes/network-status", s.handleNetworkStatus).Methods("GET")
	r.HandleFunc("/api/nodes/clock-skew", s.handleFleetClockSkew).Methods("GET")
	r.HandleFunc("/api/nodes/resolve", s.handleResolvePrefix).Methods("GET")

	r.HandleFunc("/api/nodes/{pubkey}/health", s.handleNodeHealth).Methods("GET")
	r.HandleFunc("/api/nodes/{pubkey}/paths", s.handleNodePaths).Methods("GET")
	r.HandleFunc("/api/nodes/{pubkey}/analytics", s.handleNodeAnalytics).Methods("GET")
	r.HandleFunc("/api/nodes/{pubkey}/hop_analytics", s.handleNodeHopAnalytics).Methods("GET")
	r.HandleFunc("/api/nodes/{pubkey}/battery", s.handleNodeBattery).Methods("GET")
	r.HandleFunc("/api/nodes/{pubkey}/clock-skew", s.handleNodeClockSkew).Methods("GET")
	r.HandleFunc("/api/nodes/{pubkey}/reach", s.handleNodeReach).Methods("GET")
	// Coverage routes are always registered; each handler 404s when the opt-in
	// clientRxCoverage flag is off (a clean 404 rather than the SPA fallback that
	// an unregistered /api route would hit). See requireClientRxCoverage.
	r.HandleFunc("/api/nodes/{pubkey}/rx-coverage", s.handleNodeRxCoverage).Methods("GET")

	r.HandleFunc("/api/nodes/{pubkey}", s.handleNodeDetail).Methods("GET")
	r.HandleFunc("/api/nodes", s.handleNodes).Methods("GET")
}

func packetCore(p database.Packet) api.PacketCore {

	firstSeen := types.WireTimeSeconds{Time: p.FirstSeen}
	timestamp := firstSeen // the representative observation's time wins below

	r := api.PacketCore{
		Id:               int(p.ID),
		RawHex:           optString(p.RawHex),
		Hash:             optString(p.Hash),
		FirstSeen:        &firstSeen,
		Timestamp:        &timestamp,
		RouteType:        &p.RouteType,
		PayloadType:      &p.PayloadType,
		DecodedJson:      optString(p.DecodedJSON),
		ObservationCount: p.ObservationCount,
		ObserverId:       p.ObserverPubkey,
		ObserverName:     p.ObserverName,
		Snr:              p.SNR,
		Rssi:             p.RSSI,
		Score:            p.Score,
		Direction:        p.Direction,
		ScopeName:        p.ScopeName,
	}
	if p.ObsTimestamp != nil {
		r.Timestamp = types.NewWireTimeSecondsPtr(p.ObsTimestamp)
	}
	if p.ObserverPubkey != nil {
		pj := prefixesJSON(p.HopPrefixes)
		r.PathJson = &pj
	}
	return r
}

func nodeAdvertResponse(p database.Packet, observations []database.Observation) api.NodeAdvert {
	c := packetCore(p)
	out := make([]api.NodeAdvertObservation, len(observations))
	for i, o := range observations {
		out[i] = nodeAdvertObservation(o)
	}
	return api.NodeAdvert{
		Id:               c.Id,
		RawHex:           c.RawHex,
		Hash:             c.Hash,
		FirstSeen:        c.FirstSeen,
		Timestamp:        c.Timestamp,
		RouteType:        c.RouteType,
		PayloadType:      c.PayloadType,
		DecodedJson:      c.DecodedJson,
		ObservationCount: c.ObservationCount,
		ObserverId:       c.ObserverId,
		ObserverName:     c.ObserverName,
		ObserverIata:     c.ObserverIata,
		Snr:              c.Snr,
		Rssi:             c.Rssi,
		Score:            c.Score,
		PathJson:         c.PathJson,
		Direction:        c.Direction,
		ScopeName:        c.ScopeName,
		ResolvedPath:     c.ResolvedPath,
		Observations:     out,
	}
}

func roleCountsToResponse(c database.RoleCounts) api.RoleCounts {
	return api.RoleCounts{Repeaters: c.Repeaters, Rooms: c.Rooms, Companions: c.Companions, Sensors: c.Sensors}
}

// geoFilter converts the configured area into the geofilter shape.
func (e AreaEntry) geoFilter() *geofilter.Config {
	return &geofilter.Config{
		Polygon: e.Polygon,
		LatMin:  e.LatMin,
		LatMax:  e.LatMax,
		LonMin:  e.LonMin,
		LonMax:  e.LonMax,
	}
}

func nodeToResponse(n *database.Node) api.Node {
	out := api.Node{
		PublicKey:         n.PublicKey,
		Name:              n.Name,
		Role:              n.Role,
		Lat:               n.Lat,
		Lon:               n.Lon,
		LastSeen:          types.NewWireTimeSecondsPtr(n.LastSeen),
		FirstSeen:         types.NewWireTimeSecondsPtr(n.FirstSeen),
		AdvertCount:       int(n.AdvertCount),
		Foreign:           n.Foreign,
		BatteryMv:         n.BatteryMv,
		TemperatureC:      n.TemperatureC,
		DefaultScope:      n.DefaultScope,
		ConfiguredScope:   n.ConfiguredScope,
		ConfiguredScopeAt: types.NewWireTimeSecondsPtr(n.ConfiguredScopeAt),
		LastHeard:         types.NewWireTimeSecondsPtr(n.LastSeen), // same value as last_seen, as nodeMap had it
	}
	return out
}

var lastHeardWindows = map[string]time.Duration{
	"1h": time.Hour, "6h": 6 * time.Hour, "24h": 24 * time.Hour,
	"7d": 7 * 24 * time.Hour, "30d": 30 * 24 * time.Hour,
}

// parsePacketTimeBound parses a ?since= / ?until= style bound. An empty value
// means no bound (the zero time). Anything else must be RFC 3339, with or
// without fractional seconds.
func parsePacketTimeBound(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC(), nil
	}
	t, err := time.Parse(time.DateOnly, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid time bound %q: %w", s, err)
	}
	return t.UTC(), nil
}

// parseNodeListParams turns the /api/nodes query string into typed DB params.
// now is passed in so tests don't depend on the wall clock.
func parseNodeListParams(r *http.Request, now time.Time, nodesMax int) (database.NodeListParams, error) {
	q := r.URL.Query()
	p := database.NodeListParams{
		Limit:   queryLimit(r, 50, nodesMax),
		Offset:  queryInt(r, "offset", 0),
		Role:    q.Get("role"),
		Search:  q.Get("search"),
		Regions: database.NormalizeRegionCodes(q.Get("region")),
		Sort:    database.NodeSort(q.Get("sortBy")),
	}
	if d, ok := lastHeardWindows[q.Get("lastHeard")]; ok {
		since := now.Add(-d)
		p.HeardSince = &since
	}
	if s := q.Get("before"); s != "" {
		t, err := parsePacketTimeBound(s) // adjust to its real signature
		if err != nil {
			return database.NodeListParams{}, fmt.Errorf("invalid before: %w", err)
		}
		p.Before = &t
	}
	return p, nil
}

// nodePubkeysInArea returns the public keys of nodes whose GPS coordinates
// fall inside the given area polygon or bounding box.
func (s *Server) nodePubkeysInArea(ctx context.Context, entry AreaEntry) ([]string, error) {
	positions, err := s.db.ListNodePositions(ctx)
	if err != nil {
		return nil, err
	}
	return pubkeysInArea(positions, entry), nil
}

func (s *Server) handleNodeDetail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	pubkey := mux.Vars(r)["pubkey"]
	if s.cfg.IsBlacklisted(pubkey) {
		writeError(w, 404, "Not found")
		return
	}

	row, err := s.db.GetNodeByPubkey(ctx, pubkey)
	if errors.Is(err, database.ErrNotFound) {
		row, err = nil, nil
	}
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	// Issue #772: short-URL fallback. If exact pubkey lookup misses and the
	// path looks like a hex prefix (>=8 chars, <64), try prefix resolution.
	if row == nil && len(pubkey) >= 8 && len(pubkey) < 64 {
		resolved, ambiguous, perr := s.db.GetNodeByPrefix(ctx, pubkey, 8)
		if perr != nil && !errors.Is(perr, database.ErrNotFound) {
			writeError(w, 500, perr.Error())
			return
		}
		if ambiguous {
			writeError(w, http.StatusConflict, "Ambiguous prefix: multiple nodes match. Use a longer prefix.")
			return
		}
		if resolved != nil {
			if resolved.PublicKey != "" && s.cfg.IsBlacklisted(resolved.PublicKey) {
				writeError(w, 404, "Not found")
				return
			}
			row = resolved
		}
	}
	if row == nil {
		writeError(w, 404, "Not found")
		return
	}
	// Hide the node when its name matches an operator-configured prefix
	// (#1181). 404 mirrors the blacklist behaviour above.
	name := ""
	if row.Name != nil {
		name = *row.Name
	}
	if s.cfg.IsNameHidden(name) {
		writeError(w, 404, "Not found")
		return
	}
	if row.PublicKey != "" {
		pubkey = row.PublicKey
	}
	node := nodeToResponse(row)

	if s.store != nil {
		hashInfo := s.store.GetNodeHashSizeInfo()
		enrichNodeWithHashSize(&node, hashInfo[pubkey])
		mbEntry, _ := s.store.GetMultibyteCapFor(pubkey)
		enrichNodeWithMultiByte(&node, mbEntry)
		if node.Role.RelaysTraffic() {
			ht := s.cfg.GetHealthThresholds()
			info := s.store.GetRepeaterRelayInfo(pubkey, ht.RelayActiveHours)
			enrichNodeWithRelayInfo(&node, &info)
			usefulMap := s.store.GetRepeaterUsefulnessScoreMap()
			trafficRaw := lookupUsefulnessScore(usefulMap, pubkey)
			trafficNorm := 0.0

			if mx := maxFloat(usefulMap); mx > 0 {
				trafficNorm = trafficRaw / mx
			}
			u := computeNodeUsefulness(trafficRaw, usefulnessAxes{
				Traffic:    trafficNorm,
				Bridge:     s.store.GetBridgeScore(pubkey),
				Coverage:   s.store.GetCoverageScore(pubkey),
				Redundancy: s.store.GetRedundancyScore(pubkey),
			}, s.store.UsefulnessAxesComputed())
			enrichNodeWithUsefulness(&node, &u)

			// #1751: region scopes this repeater has transported.
			if len(info.TransportedScopes) > 0 {
				node.TransportedScopes = info.TransportedScopes
			}
			// #2001: same field, same rules as handleNodes.
			if declaredCSV, ok := s.declaredRegionsCSV(ctx); ok {
				answer, has := declaredCSV[strings.ToLower(pubkey)]
				enrichNodeWithDeclaredScope(&node, answer, has, info.TransportedScopes)
			}

		}
	}

	// #1143: strict exact-match on the indexed from_pubkey column.
	recentAdvertPackets, err := s.db.GetRecentTransmissionsForNode(ctx, pubkey, 20)
	if err != nil {
		log.Printf("WARN GetRecentTransmissionsForNode(%s): %v", pubkey, err)
	}
	txIDs := make([]int64, len(recentAdvertPackets))
	for i, packet := range recentAdvertPackets {
		txIDs[i] = packet.ID
	}
	observationsByTx, err := s.db.ListObservationsForTransmissions(ctx, txIDs)
	if err != nil {
		log.Printf("WARN ListObservationsForTransmissions(%s): %v", pubkey, err)
	}
	recentAdverts := make([]api.NodeAdvert, 0, len(recentAdvertPackets))
	for _, packet := range recentAdvertPackets {
		recentAdverts = append(recentAdverts, nodeAdvertResponse(packet, observationsByTx[packet.ID]))
	}

	// Windowed flood-advert count (7d), consumed by the ArcScope repeater advisor.
	since := time.Now().Add(-7 * 24 * time.Hour)
	if n, err := s.db.CountFloodAdvertsForNode(ctx, pubkey, since); err == nil {
		node.FloodAdvertCount7d = &n
	} else {
		log.Printf("WARN CountFloodAdvertsForNode(%s): %v", pubkey, err)
	}

	writeJSON(w, api.NodeDetailResponse{
		Node:          node,
		RecentAdverts: recentAdverts,
	})
}

func (s *Server) blacklistKeys() []string {
	keys := make([]string, 0, len(s.cfg.NodeBlacklist))
	for _, k := range s.cfg.NodeBlacklist {
		if k = strings.ToLower(strings.TrimSpace(k)); k != "" {
			keys = append(keys, k)
		}
	}
	return keys
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := queryLimit(r, 50, s.cfg.ListLimits.NodesMax)
	offset := queryInt(r, "offset", 0)
	ctx := r.Context()
	p, err := parseNodeListParams(r, time.Now().UTC(), limit)
	p.ExcludeKeys = s.blacklistKeys()
	p.ExcludeNamePrefixes = s.cfg.HiddenNamePrefixes
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	dbNodes, total64, err := s.db.ListNodes(ctx, p)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	roleCounts, err := s.db.GetAllRoleCounts(ctx) // all-time, as before
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	nodes := make([]api.Node, 0, len(dbNodes))
	for i := range dbNodes {
		nodes = append(nodes, nodeToResponse(&dbNodes[i]))
	}
	total := int(total64)
	counts := roleCountsToResponse(roleCounts)

	// has_more is decided here, against the raw SQL page and the real COUNT(*).
	// The blacklist and hidden-prefix filters now run in SQL, so total and the
	// page already agree for them. The geo-filter and area passes below still
	// drop rows from this page and rewrite total, so a short page is not
	// necessarily the last one.
	hasMore := offset+len(nodes) < total
	if s.store != nil {
		hashInfo := s.store.GetNodeHashSizeInfo()
		relayWindow := s.cfg.GetHealthThresholds().RelayActiveHours
		// #1257: bulk-compute relay info + usefulness scores ONCE per
		// request (cached 15s) instead of calling the per-node helpers
		// inside the loop. The per-node calls each grabbed their own
		// RLock and walked byPathHop[pk] + byPayloadType, blowing
		// /api/nodes up to 30+s on busy networks.
		var relayMap map[string]RepeaterRelayInfo
		var usefulMap map[string]float64
		needsRelay := false
		for i := range nodes {
			if nodes[i].Role.RelaysTraffic() {
				needsRelay = true
				break
			}
		}
		if needsRelay {
			relayMap = s.store.GetRepeaterRelayInfoMap(relayWindow)
			usefulMap = s.store.GetRepeaterUsefulnessScoreMap()
		}
		// #2001: declared region lists, once per request rather than per
		// node, so the map can colour every repeater by its scope-config
		// state. Two small queries (232 rows on a live instance) and no
		// window scan — the state is a pure function of the declared list,
		// see nodeScopeConfigState.
		var declaredCSV map[string]declaredAnswer
		declaredOK := false
		if needsRelay {
			declaredCSV, declaredOK = s.declaredRegionsCSV(ctx)
		}
		// Bridge axis (#672 axis 2 of 4). Snapshot is an atomic load
		// — safe to call regardless of needsRelay, and we want the
		// score on repeater rows specifically.
		bridgeMap := s.store.GetBridgeScoreMap()
		// Coverage + Redundancy axes (#672 axes 3 & 4). Atomic snapshots,
		// same discipline as the bridge map.
		coverageMap := s.store.GetCoverageScoreMap()
		redundancyMap := s.store.GetRedundancyScoreMap()
		// Whether the structural-axis recomputer has produced a snapshot yet:
		// distinguishes a genuinely isolated repeater (real "F") from cold
		// start (grade withheld) for the all-zero node (#1762 MAJOR-4).
		axesComputed := s.store.UsefulnessAxesComputed()
		// Population max of the raw Traffic axis, used to max-normalize
		// traffic into the composite (the other three axes are already
		// max-normalized; #1762 review).
		maxUseful := maxFloat(usefulMap)
		for i := range nodes {
			node := &nodes[i] // index, not a copy: the setters must change the slice element
			pk := node.PublicKey
			enrichNodeWithHashSize(node, hashInfo[pk])
			mbEntry, _ := s.store.GetMultibyteCapFor(pk)
			enrichNodeWithMultiByte(node, mbEntry)
			if !node.Role.RelaysTraffic() {
				continue
			}
			info, _ := lookupRelayInfo(relayMap, pk)
			info.WindowHours = relayWindow
			enrichNodeWithRelayInfo(node, &info)

			// #2001: how this repeater's region config reads. Omitted entirely
			// when the declared lookup failed; see declaredRegionsCSV.
			if declaredOK {
				answer, has := declaredCSV[strings.ToLower(pk)]
				enrichNodeWithDeclaredScope(node, answer, has, info.TransportedScopes)
			}
			// #672 4-axis usefulness. traffic_share_score keeps the raw
			// per-axis Traffic value (#1456); the composite uses the
			// max-normalized traffic so its 0.20 weight is real.
			trafficRaw := lookupUsefulnessScore(usefulMap, pk)
			trafficNorm := 0.0
			if maxUseful > 0 {
				trafficNorm = trafficRaw / maxUseful
			}
			u := computeNodeUsefulness(trafficRaw, usefulnessAxes{
				Traffic:    trafficNorm,
				Bridge:     lookupUsefulnessScore(bridgeMap, pk),
				Coverage:   lookupUsefulnessScore(coverageMap, pk),
				Redundancy: lookupUsefulnessScore(redundancyMap, pk),
			}, axesComputed)
			enrichNodeWithUsefulness(node, &u)
		}
	}
	if s.cfg.GeoFilter != nil {
		filtered := nodes[:0]
		for _, node := range nodes {
			// Foreign-flagged nodes (#730) are kept even when their GPS lies
			// outside the geofilter polygon: operators need to SEE bridged or
			// leaked nodes. The ingestor sets foreign_advert when its
			// configured geo_filter rejected the advert; the server must
			// surface those.
			if node.Foreign {
				filtered = append(filtered, node)
				continue
			}
			if NodePassesGeoFilter(node.Lat, node.Lon, s.cfg.GeoFilter) {
				filtered = append(filtered, node)
			}
		}
		total = len(filtered)
		nodes = filtered
	}

	// Filter by area
	if area := q.Get("area"); area != "" {
		var areaNodes map[string]bool
		if s.store != nil {
			areaNodes = s.store.resolveAreaNodes(ctx, area)
		} else if s.cfg != nil && s.cfg.Areas != nil {
			if entry, ok := s.cfg.Areas[area]; ok {
				pks, err := s.db.GetNodePubkeysInArea(ctx, entry.geoFilter())
				if err == nil {
					areaNodes = make(map[string]bool, len(pks))
					for _, pk := range pks {
						areaNodes[pk] = true
					}
				}
			}
		}
		if areaNodes != nil {
			filtered := make([]api.Node, 0, len(nodes))
			for _, n := range nodes {
				if areaNodes[n.PublicKey] {
					filtered = append(filtered, n)
				}
			}
			nodes = filtered
			total = len(filtered)
		}
	}
	writeJSON(w, api.NodeListResponse{Nodes: nodes, Total: total, Counts: counts, HasMore: hasMore})

}

func (s *Server) handleNodeSearch(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if q == "" {
		writeJSON(w, api.NodeSearchResponse{Nodes: []api.Node{}})
		return
	}
	found, err := s.db.SearchNodes(ctx, q, 10)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	// Filter blacklisted nodes and drop hidden-prefix nodes (#1181) from the results.
	nodes := make([]api.Node, 0, len(found))
	for i := range found {
		n := &found[i]
		if s.cfg.IsBlacklisted(n.PublicKey) {
			continue
		}
		name := ""
		if n.Name != nil {
			name = *n.Name
		}
		if s.cfg.IsNameHidden(name) {
			continue
		}
		nodes = append(nodes, nodeToResponse(n))
	}
	writeJSON(w, api.NodeSearchResponse{Nodes: nodes})
}

func (s *Server) handleNodeHealth(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	pubkey := mux.Vars(r)["pubkey"]
	if s.cfg.IsBlacklisted(pubkey) {
		writeError(w, 404, "Not found")
		return
	}
	if s.isPubkeyHidden(ctx, pubkey) {
		writeError(w, 404, "Not found")
		return
	}
	if s.store != nil {
		result, err := s.store.GetNodeHealth(ctx, pubkey)
		if err != nil || result == nil {
			writeError(w, 404, "Not found")
			return
		}
		writeJSON(w, result)
		return
	}
	writeError(w, 404, "Not found")
}

func (s *Server) handleBulkHealth(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	lim := queryLimit(r, 50, s.cfg.ListLimits.BulkHealthMax)

	if s.store != nil {
		region := r.URL.Query().Get("region")
		area := r.URL.Query().Get("area")
		results := s.store.GetBulkHealth(ctx, lim, region, area)
		// Filter blacklisted nodes + hidden-prefix nodes (#1181).
		needsBlacklist := len(s.cfg.NodeBlacklist) > 0
		needsHidden := len(s.cfg.HiddenNamePrefixes) > 0
		if needsBlacklist || needsHidden {
			filtered := make([]map[string]interface{}, 0, len(results))
			for _, entry := range results {
				if pk, ok := entry["public_key"].(string); ok && needsBlacklist && s.cfg.IsBlacklisted(pk) {
					continue
				}
				if needsHidden {
					name, _ := entry["name"].(string)
					if s.cfg.IsNameHidden(name) {
						continue
					}
				}
				filtered = append(filtered, entry)
			}
			writeJSON(w, filtered)
			return
		}
		writeJSON(w, results)
		return
	}

	writeJSON(w, []BulkHealthEntry{})
}

func (s *Server) handleNetworkStatus(w http.ResponseWriter, r *http.Request) {
	ht := s.cfg.GetHealthThresholds()
	nodes, err := s.db.ListNodeHealth(r.Context())
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, classifyNetworkStatus(nodes, ht, time.Now()))
}

// nodePathAgg groups the transmissions that took the same resolved path.
type nodePathAgg struct {
	Hops       []PathHopResp
	Count      int
	LastSeen   time.Time
	SampleHash string
}

// rankNodePaths returns up to limit groups: most recently seen first, then
// highest count, then sample hash so equal groups come out in a stable order.
func rankNodePaths(groups map[string]*nodePathAgg, limit int) []*nodePathAgg {
	out := make([]*nodePathAgg, 0, len(groups))
	for _, g := range groups {
		out = append(out, g)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if !a.LastSeen.Equal(b.LastSeen) {
			return a.LastSeen.After(b.LastSeen)
		}
		if a.Count != b.Count {
			return a.Count > b.Count
		}
		return a.SampleHash < b.SampleHash
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

func (s *Server) handleNodePaths(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	pubkey := mux.Vars(r)["pubkey"]
	if s.cfg.IsBlacklisted(pubkey) {
		writeError(w, 404, "Not found")
		return
	}
	if s.isPubkeyHidden(ctx, pubkey) {
		writeError(w, 404, "Not found")
		return
	}
	node, err := s.db.GetNodeByPubkey(ctx, pubkey)
	if errors.Is(err, database.ErrNotFound) {
		writeError(w, 404, "Not found")
		return
	}
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	if s.store == nil {
		writeError(w, 503, "Packet store unavailable")
		return
	}
	if !s.store.PathHopIndexReady() {
		writeIndexLoading503(w)
		return
	}

	// Use the precomputed byPathHop index instead of scanning all packets.
	// Look up by full pubkey (resolved hops) and by short prefixes (raw hops).
	lowerPK := strings.ToLower(pubkey)
	prefix2 := lowerPK
	if len(prefix2) > 4 {
		prefix2 = prefix2[:4]
	}
	prefix1 := lowerPK
	if len(prefix1) > 2 {
		prefix1 = prefix1[:2]
	}

	s.store.mu.RLock()
	_, pm := s.store.getCachedNodesAndPM()

	// Collect candidate transmissions from the index, deduplicating by tx ID.
	// confirmedByFullKey tracks TXs found via the full-pubkey index key — these are
	// already resolved_path-confirmed and bypass the hop-level check below.
	confirmedByFullKey := make(map[int]bool)
	seen := make(map[int]bool)
	var candidates []*StoreTx
	addCandidates := func(key string, confirmed bool) {
		for _, tx := range s.store.byPathHop[key] {
			if !seen[tx.ID] {
				seen[tx.ID] = true
				if confirmed {
					confirmedByFullKey[tx.ID] = true
				}
				candidates = append(candidates, tx)
			}
		}
	}
	addCandidates(lowerPK, true)  // full pubkey match (from resolved_path) → confirmed
	addCandidates(prefix1, false) // 2-char raw hop match
	addCandidates(prefix2, false) // 4-char raw hop match
	// Also check any raw hops that start with prefix2 (longer prefixes).
	// Raw hops are typically 2 chars, so iterate only keys with HasPrefix
	// on the small set of index keys rather than all packets.
	for key := range s.store.byPathHop {
		if len(key) > 4 && len(key) < len(lowerPK) && strings.HasPrefix(key, prefix2) {
			addCandidates(key, false)
		}
	}

	// Post-filter: verify target node actually appears in each candidate's resolved_path.
	// The byPathHop index uses short prefixes which can collide (e.g. "c0" matches multiple nodes).
	// We lean on resolved_path (from neighbor affinity graph) to disambiguate.
	//
	// Collect candidate IDs and index membership under the read lock, then release
	// the lock before running the batched SQL query (TransmissionIDsWithResolvedPubkey does disk I/O).
	type candidateCheck struct {
		tx         *StoreTx
		hasReverse bool
		inIndex    bool
	}
	checks := make([]candidateCheck, len(candidates))
	for i, tx := range candidates {
		cc := candidateCheck{tx: tx}
		if !s.store.useResolvedPathIndex {
			cc.inIndex = true // flag off — keep all
		} else if _, hasRev := s.store.resolvedPubkeyReverse[tx.ID]; !hasRev {
			cc.inIndex = true // no indexed pubkeys — keep (conservative)
		} else {
			h := resolvedPubkeyHash(lowerPK)
			for _, id := range s.store.resolvedPubkeyIndex[h] {
				if id == tx.ID {
					cc.hasReverse = true // needs SQL confirmation
					break
				}
			}
			// If not in index at all, it's a definite no
		}
		checks[i] = cc
	}
	s.store.mu.RUnlock()

	// Now run SQL checks outside the lock for candidates that need confirmation.
	var needConfirm []int
	for _, cc := range checks {
		if !cc.inIndex && cc.hasReverse {
			needConfirm = append(needConfirm, cc.tx.ID)
		}
	}
	confirmedIDs, err := s.db.TransmissionIDsWithResolvedPubkey(ctx, needConfirm, lowerPK)
	if err != nil {
		writeError(w, 500, "Internal error")
		return
	}
	confirmedBySQL := make(map[int]bool, len(confirmedIDs))
	for _, id := range confirmedIDs {
		confirmedBySQL[id] = true
	}
	filtered := candidates[:0]
	for _, cc := range checks {
		switch {
		case cc.inIndex:
			filtered = append(filtered, cc.tx)
		case cc.hasReverse && confirmedBySQL[cc.tx.ID]:
			filtered = append(filtered, cc.tx)
		}
		// else: not in the index, or in the index but not confirmed → exclude
	}
	candidates = filtered

	// #1278: Read the CANONICAL persisted resolved_path for each surviving
	// candidate OUTSIDE s.mu: the batched BestResolvedPaths query reads the
	// database, and no database read may happen while s.mu is held.
	//
	// Option A from the issue: the packets page renders each tx via
	// fetchResolvedPathForTxBest. For /api/nodes/{pk}/paths to stay
	// CONSISTENT with the packets page, BOTH the containsTarget membership
	// decision AND the displayed hop names must come from that same
	// canonical resolved_path — not a re-resolution biased by passing the
	// queried node as hopContext anchor.
	//
	// Falls back to biased re-resolve only when a tx has no persisted
	// resolved_path (older data / async backfill incomplete); in that case
	// there's no canonical answer to be consistent with.
	txIDs := make([]int, len(candidates))
	for i, tx := range candidates {
		txIDs[i] = tx.ID
	}
	canonicalRP, err := s.db.BestResolvedPaths(ctx, txIDs)
	if err != nil {
		writeError(w, 500, "Internal error")
		return
	}

	// Re-acquire read lock for the aggregation phase that reads store data.
	s.store.mu.RLock()

	pathGroups := map[string]*nodePathAgg{}
	totalTransmissions := 0
	hopCache := make(map[string]*nodeInfo)
	// Anchor the resolver with the node being queried so tier-1/2 hop-context
	// resolution lights up when a hop prefix matches the destination node
	// (handleNodePaths aggregates paths terminating at lowerPK). Passing nil
	// here re-introduced regression #1197 in production. See
	// resolve_context_callsites_test.go.
	//
	// NOTE (#1278): this biased resolver is only consulted for the FALLBACK
	// path — txs with no persisted resolved_path. Txs with a canonical
	// resolved_path use the persisted pubkeys directly (see canonicalRP),
	// which keeps results consistent with the packets page.
	hopContext := []string{lowerPK}
	resolveHop := func(hop string) *nodeInfo {
		if cached, ok := hopCache[hop]; ok {
			return cached
		}
		r, _, _ := pm.resolveWithContext(hop, hopContext, s.store.graph.Load())
		hopCache[hop] = r
		return r
	}
	// nodeByPK caches pubkey → *nodeInfo lookups when rendering canonical
	// resolved_path entries. Cheap O(1) hit against pm.m (the prefix map
	// stores the full pubkey as a key for pubkeys >= maxPrefixLen).
	nodeByPK := make(map[string]*nodeInfo)
	lookupNode := func(pk string) *nodeInfo {
		key := strings.ToLower(pk)
		if cached, ok := nodeByPK[key]; ok {
			return cached
		}
		// Use plain resolve(); we have the full pubkey, no ambiguity.
		n := pm.resolve(key)
		if n == nil || !strings.EqualFold(n.PublicKey, key) {
			// Full pubkey may not be present in pm (role filter, eviction).
			// Fall through with nil; caller renders prefix-only entry.
			nodeByPK[key] = nil
			return nil
		}
		nodeByPK[key] = n
		return n
	}
	for _, tx := range candidates {
		hops := txGetParsedPath(tx)
		resolvedHops := make([]PathHopResp, len(hops))
		sigParts := make([]string, len(hops))
		containsTarget := false

		if rp, ok := canonicalRP[tx.ID]; ok {
			// Option A: render hops + decide membership from the CANONICAL
			// persisted resolved_path. resolved_path is parallel to the
			// best-obs path_json which may be longer than tx.PathJSON used by
			// txGetParsedPath; align by the shorter length.
			rpLen := len(rp)
			for i, hop := range hops {
				entry := PathHopResp{Prefix: hop, Name: hop}
				var resolvedPK string
				if i < rpLen && rp[i] != nil {
					resolvedPK = strings.ToLower(*rp[i])
				}
				if resolvedPK != "" {
					if n := lookupNode(resolvedPK); n != nil {
						entry.Name = n.Name
						entry.Pubkey = n.PublicKey
						if n.HasGPS {
							entry.Lat = n.Lat
							entry.Lon = n.Lon
						}
						sigParts[i] = n.PublicKey
					} else {
						entry.Pubkey = resolvedPK
						sigParts[i] = resolvedPK
					}
					if resolvedPK == lowerPK {
						containsTarget = true
					}
				} else {
					sigParts[i] = hop
				}
				resolvedHops[i] = entry
			}
		} else {
			// Fallback: no canonical resolved_path persisted (older data /
			// async backfill incomplete). Use biased re-resolve and the
			// legacy containsTarget heuristics (preserves #1197 behavior
			// and the #929 prefix-collision exclusion test).
			//
			// #1352: When a hop prefix has MULTIPLE candidates (sibling
			// prefix collisions), the biased resolver — anchored on the
			// queried target via hopContext=[lowerPK] — will preferentially
			// resolve to the target via tier-2 geo / tier-3 GPS. This
			// causes the SAME tx to be attributed to every prefix sibling
			// when each is queried in turn. To prevent wrong-node
			// attribution, we ONLY accept a resolver match as evidence of
			// target membership when:
			//   (a) the tx was already pre-confirmed via
			//       confirmedByFullKey (resolved_path index hit) or
			//       confirmedBySQL (verified pubkey in resolved_path), OR
			//   (b) the hop's prefix candidate set is UNIQUE — no
			//       collision, so the resolver had no choice to bias.
			// Multi-candidate hops with no SQL/index confirmation are
			// treated as ambiguous and excluded from paths-through.
			containsTarget = confirmedByFullKey[tx.ID] || confirmedBySQL[tx.ID]
			// preconfirmed: SNAPSHOT of containsTarget BEFORE the per-hop
			// loop runs. Captures only the SQL/full-key index pre-confirmation
			// signal (independent of biased-resolver output). MUST NOT be
			// reassigned inside the loop — doing so would let a biased-
			// resolver match in hop[i] silently authorize a later ambiguous
			// hop[j], re-opening the #1352 wrong-node attribution path.
			//
			// Note: today the loop only ever transitions containsTarget
			// false → true, so the snapshot is functionally redundant for
			// the preconfirmed==true case (containsTarget is already true).
			// We keep the snapshot + the `preconfirmed ||` clauses below
			// as a structural invariant: future edits that flip
			// containsTarget back to false inside the loop (e.g. an
			// "exclude if last hop doesn't match" tweak) would otherwise
			// silently lose the SQL/index confirmation. The snapshot is
			// the documented contract.
			preconfirmed := containsTarget
			for i, hop := range hops {
				resolved := resolveHop(hop)
				entry := PathHopResp{Prefix: hop, Name: hop}
				lowerHop := strings.ToLower(hop)
				// #1352 guard helper. We treat as "unique/safe" when the
				// hop's prefix candidate set has EXACTLY ONE member: no
				// sibling collision, so the biased resolver had no choice
				// to bias. len(pm.m[lowerHop]) == 0 is also accepted as
				// safe-by-default in the resolvable arm because the
				// resolver returned a non-nil candidate from somewhere
				// (e.g. a full-pubkey hop longer than maxPrefixLen, or a
				// hop indexed under a different prefix length); there's
				// no collision to resolve away. In the unresolvable arm
				// below, len==0 is the ONLY reachable case (resolveHop
				// returns nil iff pm.m[lowerHop] is empty — see
				// resolveWithContext priority chain), so the guard there
				// is intentionally permissive on len==0 and the
				// `preconfirmed ||` clause is the meaningful gate.
				uniquePrefix := len(pm.m[lowerHop]) <= 1
				if resolved != nil {
					entry.Name = resolved.Name
					entry.Pubkey = resolved.PublicKey
					if resolved.HasGPS {
						entry.Lat = resolved.Lat
						entry.Lon = resolved.Lon
					}
					sigParts[i] = resolved.PublicKey
					if strings.ToLower(resolved.PublicKey) == lowerPK {
						// #1352: only attribute when unambiguous OR
						// already pre-confirmed via SQL/full-key index.
						if preconfirmed || uniquePrefix {
							containsTarget = true
						}
					}
				} else {
					sigParts[i] = hop
					// Unresolvable hop: keep conservative if prefix could
					// be the target AND there's no sibling collision.
					// If multiple candidates share this prefix, attribution
					// is ambiguous — don't claim membership without SQL
					// confirmation (#1352). See comment on uniquePrefix
					// above re: why len==0 is treated as safe here.
					if strings.HasPrefix(lowerPK, lowerHop) {
						if preconfirmed || uniquePrefix {
							containsTarget = true
						}
					}
				}
				resolvedHops[i] = entry
			}
		}

		if !containsTarget {
			continue
		}
		totalTransmissions++

		sig := strings.Join(sigParts, "→")
		agg := pathGroups[sig]
		if agg == nil {
			pathGroups[sig] = &nodePathAgg{
				Hops:       resolvedHops,
				Count:      1,
				LastSeen:   tx.FirstSeen,
				SampleHash: tx.Hash,
			}
			continue
		}
		agg.Count++
		if tx.FirstSeen.After(agg.LastSeen) {
			agg.LastSeen = tx.FirstSeen
			agg.SampleHash = tx.Hash
		}
	}
	s.store.mu.RUnlock()

	ranked := rankNodePaths(pathGroups, 50)
	paths := make([]PathEntryResp, 0, len(ranked))
	for _, agg := range ranked {
		var lastSeen interface{}
		if !agg.LastSeen.IsZero() {
			lastSeen = agg.LastSeen.UTC()
		}
		paths = append(paths, PathEntryResp{
			Hops:       agg.Hops,
			Count:      agg.Count,
			LastSeen:   lastSeen,
			SampleHash: agg.SampleHash,
		})
	}

	writeJSON(w, NodePathsResponse{
		Node: map[string]interface{}{
			"public_key": node.PublicKey,
			"name":       node.Name,
			"lat":        node.Lat,
			"lon":        node.Lon,
		},
		Paths:              paths,
		TotalPaths:         len(pathGroups),
		TotalTransmissions: totalTransmissions,
	})
}

func (s *Server) handleNodeAnalytics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	pubkey := mux.Vars(r)["pubkey"]
	if s.cfg.IsBlacklisted(pubkey) {
		writeError(w, 404, "Not found")
		return
	}
	if s.isPubkeyHidden(ctx, pubkey) {
		writeError(w, 404, "Not found")
		return
	}
	days := nodeAnalyticsDays(r)

	if s.store != nil {
		result, err := s.store.GetNodeAnalytics(ctx, pubkey, days)
		if err != nil || result == nil {
			writeError(w, 404, "Not found")
			return
		}
		writeJSON(w, result)
		return
	}

	writeError(w, 404, "Not found")
}

// nodeAnalyticsDays reads the node analytics range picker's ?days= (default 7,
// clamped to 1-365).
func nodeAnalyticsDays(r *http.Request) int {
	return min(max(queryInt(r, "days", 7), 1), 365)
}

// handleNodeHopAnalytics serves the hop count at this node for each flood
// packet it forwarded (issue #1812). Separate from /analytics so the hop
// scan does not slow down the main analytics response.
func (s *Server) handleNodeHopAnalytics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	pubkey := mux.Vars(r)["pubkey"]
	if s.cfg.IsBlacklisted(pubkey) || s.isPubkeyHidden(ctx, pubkey) || s.store == nil {
		writeError(w, 404, "Not found")
		return
	}
	result, err := s.store.GetNodeHopAnalytics(ctx, pubkey, nodeAnalyticsDays(r))
	if err != nil || result == nil {
		writeError(w, 404, "Not found")
		return
	}
	writeJSON(w, result)
}

func (s *Server) handleNodeClockSkew(w http.ResponseWriter, r *http.Request) {
	pubkey := mux.Vars(r)["pubkey"]
	if s.store == nil {
		writeError(w, 404, "Not found")
		return
	}
	result := s.store.GetNodeClockSkew(pubkey)
	if result == nil {
		writeError(w, 404, "No clock skew data for this node")
		return
	}
	writeJSON(w, result)
}

func (s *Server) handleObserverClockSkew(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeJSON(w, []ObserverCalibration{})
		return
	}
	writeJSON(w, s.store.GetObserverCalibrations())
}

func (s *Server) handleFleetClockSkew(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if s.store == nil {
		writeJSON(w, []*NodeClockSkew{})
		return
	}
	area := r.URL.Query().Get("area")
	writeJSON(w, s.store.GetFleetClockSkew(ctx, area))
}

// pubkeysInArea returns the public keys of positions that pass the area's
// polygon or bounding box.
func pubkeysInArea(positions []database.NodePosition, entry AreaEntry) []string {
	gf := &geofilter.Config{
		Polygon: entry.Polygon,
		LatMin:  entry.LatMin,
		LatMax:  entry.LatMax,
		LonMin:  entry.LonMin,
		LonMax:  entry.LonMax,
	}
	var result []string
	for _, p := range positions {
		if geofilter.PassesFilter(p.Lat, p.Lon, gf) {
			result = append(result, p.PublicKey)
		}
	}
	return result
}

// NetworkStatus mirrors the /api/network-status payload.
type NetworkStatus struct {
	Total      int                    `json:"total"`
	Active     int                    `json:"active"`
	Degraded   int                    `json:"degraded"`
	Silent     int                    `json:"silent"`
	RoleCounts map[types.NodeRole]int `json:"roleCounts"`
}

// classifyNetworkStatus buckets nodes by how long ago they were last seen, using the
// per-role degraded and silent thresholds. Never-seen nodes count as silent.
func classifyNetworkStatus(nodes []database.NodeHealth, th HealthThresholds, now time.Time) NetworkStatus {
	st := NetworkStatus{Total: len(nodes), RoleCounts: map[types.NodeRole]int{}}
	for _, n := range nodes {
		st.RoleCounts[n.Role]++

		degradedMs, silentMs := th.GetHealthMs(n.Role)
		if n.LastSeen == nil {
			st.Silent++
			continue
		}
		age := now.Sub(*n.LastSeen).Milliseconds()
		switch {
		case age < int64(degradedMs):
			st.Active++
		case age < int64(silentMs):
			st.Degraded++
		default:
			st.Silent++
		}
	}
	return st
}

// GetNodeHealth returns health info for a single node using in-memory data.
func (s *PacketStore) GetNodeHealth(ctx context.Context, pubkey string) (map[string]interface{}, error) {
	// Fetch node info from DB (fast single-row lookup)
	dbNode, err := s.db.GetNodeByPubkey(ctx, pubkey)
	var node api.Node
	switch {
	case err == nil:
		node = nodeToResponse(dbNode)
	case errors.Is(err, database.ErrNotFound):
		// The node isn't in the DB (e.g. companion that never advertised):
		// if we have any packet data for it, build a partial response.
		s.mu.RLock()
		hasPackets := len(s.byNode[pubkey]) > 0
		s.mu.RUnlock()
		if !hasPackets {
			return nil, nil
		}
		// Synthetic node stub so the rest of the function works.
		unknown := types.RoleUnknown
		node = api.Node{PublicKey: pubkey, Name: ptr("Unknown"), Role: &unknown}
	default:
		return nil, err
	}

	// Loaded before taking s.mu so the lock order stays s.mu →
	// analyticsRecomputerMu everywhere (computeDirectHeard takes s.mu).
	directHeard := s.loadDirectHeard()

	s.mu.RLock()

	packets := s.byNode[pubkey]
	todayStart := time.Now().UTC().Truncate(24 * time.Hour)
	var packetsToday int
	var snrSum float64
	var snrCount int
	var totalHops, hopCount int
	var lastHeard time.Time
	totalObservations := 0

	relayObservers := map[string]struct{}{}

	for _, pkt := range packets {
		totalObservations += pkt.ObservationCount
		if pkt.FirstSeen.After(todayStart) {
			packetsToday++
		}
		if pkt.SNR != nil {
			snrSum += *pkt.SNR
			snrCount++
		}
		if pkt.FirstSeen.After(lastHeard) {
			lastHeard = pkt.FirstSeen
		}
		hops := txGetParsedPath(pkt)
		if len(hops) > 0 {
			totalHops += len(hops)
			hopCount++
		}
		if pkt.ObserverID != "" {
			relayObservers[pkt.ObserverID] = struct{}{}
		}
	}

	nonRelaySet, seenSet := s.canRelaySets(ctx)
	directByObs := directHeard[strings.ToLower(pubkey)]
	observerRows := buildDirectObserverRows(directByObs, nonRelaySet, seenSet)
	relayObserverCount := relayOnlyObserverCount(relayObservers, directByObs)

	var avgSnr interface{}
	if snrCount > 0 {
		avgSnr = snrSum / float64(snrCount)
	}
	avgHops := 0
	if hopCount > 0 {
		avgHops = int(math.Round(float64(totalHops) / float64(hopCount)))
	}

	recentLimit := 20
	if len(packets) < recentLimit {
		recentLimit = len(packets)
	}
	recent := make([]*StoreTx, 0, recentLimit)
	for i := len(packets) - 1; i >= len(packets)-recentLimit; i-- {
		recent = append(recent, packets[i])
	}
	totalPackets := len(packets)
	txIDs, _ := pathIDs(recent, false)
	s.mu.RUnlock()

	paths, err := s.db.ResolvedPaths(ctx, txIDs, nil)
	if err != nil {
		return nil, err
	}

	s.mu.RLock()
	recentPackets := make([]map[string]interface{}, 0, len(recent))
	for _, tx := range recent {
		p := s.txToMapWithRP(tx, paths)
		delete(p, "observations")
		recentPackets = append(recentPackets, p)
	}
	s.mu.RUnlock()

	return map[string]interface{}{
		"node": node,
		// Direct-RF only: observers that received this node's own
		// transmission off the air.
		"observers": observerRows,
		// Observers that saw traffic through this node without hearing it.
		// The stats below count that relayed traffic too, so the card needs
		// the number to stay consistent with them.
		"relayObserverCount": relayObserverCount,
		"stats": map[string]interface{}{
			"totalTransmissions": totalPackets,
			"totalObservations":  totalObservations,
			"totalPackets":       totalPackets,
			"packetsToday":       packetsToday,
			"avgSnr":             avgSnr,
			"avgHops":            avgHops,
			"lastHeard":          formatTS(lastHeard),
		},
		"recentPackets": recentPackets,
	}, nil
}

func nodeAdvertObservation(o database.Observation) api.NodeAdvertObservation {
	hops := o.HopPrefixes()
	prefixes := make([]string, len(hops))
	for i, h := range hops {
		prefixes[i] = strings.ToUpper(h)
	}
	return api.NodeAdvertObservation{
		Id:             o.ID,
		TransmissionId: o.TransmissionID,
		ObserverId:     optString(o.ObserverID),
		ObserverName:   optString(o.ObserverName),
		ObserverIata:   o.ObserverIATA,
		Snr:            o.SNR,
		Rssi:           o.RSSI,
		Score:          o.Score,
		Direction:      optString(o.Direction),
		PathJson:       prefixesJSON(prefixes),
		Timestamp:      types.WireTimeMillis{Time: o.Timestamp},
	}
}
