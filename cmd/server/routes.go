package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"regexp"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/decoder"
	"github.com/OKI-Mesh/CoreScope/internal/prunequeue"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/gorilla/mux"
	"golang.org/x/sync/singleflight"
)

// memBreakdownNote is the static accounting caveat attached to the opt-in
// /api/perf?mem=1 store memory breakdown (api.PerfResponse.MemoryBreakdownNote).
const memBreakdownNote = "Per-component *MB count string content + one Go string header each and are a deliberate upper bound (the header is also in the *EstimatedMB base figures). struct/index/map overhead, the neighbor graph and analytics caches are excluded, so the totals sit below goHeapInuseMB. floodTxSharePct sizes the flood-forward multiplication: each flood hop is stored as its own transmission."

// Server holds shared state for route handlers.
type Server struct {
	db        *database.DB
	cfg       *Config
	hub       *Hub
	store     *PacketStore // in-memory packet store (nil = fallback to DB)
	configDir string       // directory containing config.json (for write-back via PUT /api/config/geo-filter)
	startedAt time.Time
	perfStats *PerfStats
	version   string
	commit    string
	buildTime string

	// Cached runtime.MemStats to avoid stop-the-world pauses on every health check
	memStatsMu       sync.Mutex
	memStatsCache    runtime.MemStats
	memStatsCachedAt time.Time

	// #2001: declared region lists for the scope_config_state field, cached
	// so /api/nodes does not re-run the merge query pair per request. The
	// cached map is read by concurrent requests and replaced, never mutated.
	declaredRegionsMu    sync.Mutex
	declaredRegionsCache map[string]declaredAnswer
	declaredRegionsAt    time.Time
	// Collapses the TTL-boundary herd so the query runs once, not once per
	// in-flight request, and never under declaredRegionsMu.
	declaredRegionsSF singleflight.Group
	// Counts executions of that query. Read by the test that pins the cache:
	// the enforceable perf characteristic here is "N requests, one query".
	declaredRegionsQueries int64

	// Cached /api/stats response — recomputed at most once every 10s
	statsMu       sync.Mutex
	statsCache    *api.StatsResponse
	statsCachedAt time.Time
	// #1910: collapses concurrent rebuilds. The cache check below releases
	// statsMu before building the response, so every request arriving while the
	// 10s window was expired used to rebuild it in full, each running its own DB
	// queries against a 4-connection pool.
	statsSF singleflight.Group

	// Guards s.cfg.GeoFilter — read by ingest/handler goroutines, written by PUT handler
	cfgMu sync.RWMutex

	// Serializes concurrent PUT /api/config/geo-filter disk writes so requests
	// can't race on the .tmp file or interleave disk/memory updates.
	saveMu sync.Mutex

	// Neighbor affinity graph (lazy-built, cached with TTL)
	neighborMu    sync.Mutex
	neighborGraph *NeighborGraph

	// Cached /api/scope-stats response — per-window, recomputed at most once every 30s
	scopeStatsMu       sync.Mutex
	scopeStatsCache    map[string]*api.ScopeStatsResponse
	scopeStatsCachedAt map[string]time.Time

	// #1975: cached /api/scope-audit response, per window, recomputed at most
	// once every 30s. Mirrors the scopeStats cache directly above it.
	scopeAuditMu       sync.Mutex
	scopeAuditCache    map[string]*api.ScopeAuditResponse
	scopeAuditCachedAt map[string]time.Time
	scopeAuditSF       singleflight.Group

	// #1975: /api/scope-audit window cache and its single-flight guard, so a
	// burst of viewers on a cold cache recomputes the network-wide scan once
	// rather than once per request. Lives in scope_audit.go.
	scopes scopesState

	// Router reference for OpenAPI spec generation
	router *mux.Router

	// Cached default (no-filter) /api/observers response, served from an
	// atomic-pointer snapshot. Refilled via singleflight on TTL boundary
	// to prevent thundering-herd SQL stampedes. Issue #1481 P0-3 +
	// #1483 follow-up (singleflight + monotonic time).
	observersCacheV2 observersCacheField

	channelsCache channelsCacheField // see channels_cache.go

	// Cached default-shape /api/analytics/neighbor-graph response,
	// recomputed every 5 min in a background goroutine. Issue #1481 P0-1.
	neighborGraphCache neighborGraphCacheField

	// Counter for rebuild-panic events on the neighbor-graph cache
	// background recomputer. Surfaced via /api/stats. #1483 follow-up.
	neighborGraphCacheRebuildFailures uint64

	// Test injection: when non-nil, replaces the real
	// computeNeighborGraphResponse pipeline so tests can assert the
	// bypass branch was exercised without standing up a full DB/store.
	// Production code MUST leave this nil. #1483 follow-up.
	computeNeighborGraphResponseFn func(minCount int, minScore float64, region string, role types.NodeRole) api.NeighborGraphResponse

	// Per-server state for /api/nodes/{pk}/reach: TTL cache + singleflight
	// + cached neighbor_edges degree snapshot. Lives on *Server (not as
	// package globals) so multiple instances don't share observable
	// state. Initialised lazily on first use; see node_reach.go.
	reach reachState

	// Known-channels catalogue cache (issue #1323). Nil until configured;
	// when nil the /api/known-channels endpoint returns an empty snapshot.
	knownChannels *knownChannelsCache
	channelCaches channelCaches
}

func newMetricsSummaryObserver(s database.MetricsSummary) api.MetricsSummaryObserver {
	var name *string
	if s.ObserverName != "" {
		name = &s.ObserverName
	}
	return api.MetricsSummaryObserver{
		ObserverId:        s.ObserverID,
		ObserverName:      name,
		Iata:              optString(s.IATA),
		CurrentNoiseFloor: s.CurrentNF,
		AvgNoiseFloor24h:  s.AvgNF,
		MaxNoiseFloor24h:  s.MaxNF,
		BatteryMv:         s.CurrentBattMv,
		SampleCount:       s.SampleCount,
		Sparkline:         s.Sparkline,
	}
}

// PerfStats tracks request performance.
type PerfStats struct {
	mu          sync.Mutex
	Requests    int64
	TotalMs     float64
	Endpoints   map[string]*EndpointPerf
	SlowQueries []api.SlowQuery
	StartedAt   time.Time
}

type EndpointPerf struct {
	Count   int
	TotalMs float64
	MaxMs   float64
	Recent  []float64
}

func relayReport(canRelay, seen bool) *bool {
	if !seen {
		return nil
	}
	return &canRelay
}

func NewPerfStats() *PerfStats {
	return &PerfStats{
		Endpoints:   make(map[string]*EndpointPerf),
		SlowQueries: make([]api.SlowQuery, 0),
		StartedAt:   time.Now(),
	}
}

func NewServer(db *database.DB, cfg *Config, hub *Hub) *Server {
	if cfg != nil {
		cfg.applyListLimitsDefaults()
	}
	return &Server{
		db:        db,
		cfg:       cfg,
		hub:       hub,
		startedAt: time.Now(),
		perfStats: NewPerfStats(),
		version:   resolveVersion(),
		commit:    resolveCommit(),
		buildTime: resolveBuildTime(),
	}
}

const memStatsTTL = 5 * time.Second

// getMemStats returns cached runtime.MemStats, refreshing at most every 5 seconds.
// runtime.ReadMemStats() stops the world; caching prevents per-request GC pauses.
func (s *Server) getMemStats() runtime.MemStats {
	s.memStatsMu.Lock()
	defer s.memStatsMu.Unlock()
	if time.Since(s.memStatsCachedAt) > memStatsTTL {
		runtime.ReadMemStats(&s.memStatsCache)
		s.memStatsCachedAt = time.Now()
	}
	return s.memStatsCache
}

// getGeoFilter returns a pointer to the current geo_filter config under read lock.
// Callers MUST NOT mutate the returned struct.
// isPubkeyHidden returns true if the node with the given pubkey has a name
// matching an operator-configured hidden prefix (#1181). Mirrors the
// IsBlacklisted check used at the top of per-pubkey handlers: per-pubkey
// endpoints should 404 on hidden nodes so callers learn nothing about
// whether the row exists. Returns false on DB error / missing row (the
// downstream handler's own 404 covers those).
func (s *Server) isPubkeyHidden(ctx context.Context, pubkey string) bool {
	if s == nil || s.cfg == nil || len(s.cfg.HiddenNamePrefixes) == 0 {
		return false
	}
	node, err := s.db.GetNodeByPubkey(ctx, pubkey)
	switch {
	case errors.Is(err, database.ErrNotFound):
		return false
	case err != nil:
		log.Printf("[hidden] node lookup failed for %.12s: %v", pubkey, err)
		return false
	}
	if node.Name == nil {
		return false
	}
	return s.cfg.IsNameHidden(*node.Name)
}

func (s *Server) getGeoFilter() *GeoFilterConfig {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	return s.cfg.GeoFilter
}

// setGeoFilter atomically swaps the geo_filter config; used by PUT /api/config/geo-filter.
func (s *Server) setGeoFilter(gf *GeoFilterConfig) {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.GeoFilter = gf
}

// RegisterRoutes sets up all HTTP routes on the given router.
func (s *Server) RegisterRoutes(r *mux.Router) {
	s.router = r
	// CORS middleware (must run before route handlers)
	r.Use(s.corsMiddleware)

	// Performance instrumentation middleware
	r.Use(s.perfMiddleware)

	// /api/* responses must not be cached by upstream CDNs (#1551).
	// Cloudflare/nginx/Varnish default zone policies cache
	// application/json for 15min–4h when no Cache-Control is set,
	// causing operators behind a CDN to serve stale observers/packets/
	// stats data. Scope: /api/ prefix only — static assets stay
	// CDN-cacheable (their headers are set by spaHandler).
	r.Use(noStoreAPIMiddleware)

	// Detect CDN-fronted deployments and warn the operator ONCE if
	// any CDN-typical header (CF-Ray, CF-Connecting-IP, etc.) is
	// observed. See #1561: no-store alone isn't sufficient on
	// Cloudflare zones with Cache Rules / Page Rules that ignore
	// origin Cache-Control. Operator must add a Bypass Cache rule
	// for /api/* — see docs/deployment-behind-cdn.md.
	r.Use(cdnDetectionMiddleware)

	// Config endpoints
	r.HandleFunc("/api/config/cache", s.handleConfigCache).Methods("GET")
	r.HandleFunc("/api/config/client", s.handleConfigClient).Methods("GET")
	r.HandleFunc("/api/config/regions", s.handleConfigRegions).Methods("GET")
	r.HandleFunc("/api/config/theme", s.handleConfigTheme).Methods("GET")
	r.HandleFunc("/api/config/map", s.handleConfigMap).Methods("GET")
	r.HandleFunc("/api/config/geo-filter", s.handleConfigGeoFilter).Methods("GET")
	r.HandleFunc("/api/config/areas", s.handleConfigAreas).Methods("GET")
	r.HandleFunc("/api/config/areas/polygons", s.handleConfigAreasPolygons).Methods("GET")
	r.Handle("/api/config/geo-filter", s.requireAPIKey(http.HandlerFunc(s.handlePutConfigGeoFilter))).Methods("PUT")

	// Readiness endpoint (gated on background init completion)
	r.HandleFunc("/api/healthz", s.handleHealthz).Methods("GET")

	// System endpoints
	r.HandleFunc("/api/health", s.handleHealth).Methods("GET")
	r.HandleFunc("/api/stats", s.handleStats).Methods("GET")
	r.HandleFunc("/api/scope-stats", s.handleScopeStats).Methods("GET")
	r.HandleFunc("/api/scope-audit", s.handleScopeAudit).Methods("GET") // #1975
	r.HandleFunc("/api/perf", s.handlePerf).Methods("GET")
	r.HandleFunc("/api/perf/io", s.handlePerfIO).Methods("GET")
	r.HandleFunc("/api/perf/write-sources", s.handlePerfWriteSources).Methods("GET")
	r.HandleFunc("/api/mqtt/status", s.handleMqttStatus).Methods("GET")
	r.Handle("/api/perf/reset", s.requireAPIKey(http.HandlerFunc(s.handlePerfReset))).Methods("POST")
	// /api/admin/prune removed in #1283 — pruning is owned by the
	// ingestor process (scheduled tickers + startup pass). Operators
	// who want an ad-hoc prune can restart the ingestor.
	//
	// /api/admin/prune-geo-filter (#669 M4 / PR #738): server enqueues a
	// marker file; the ingestor (which holds the writable DB handle)
	// runs the DELETE. /status reports completion.
	r.Handle("/api/admin/prune-geo-filter", s.requireAPIKey(http.HandlerFunc(s.handlePruneGeoFilter))).Methods("POST")
	r.Handle("/api/admin/prune-geo-filter/status", s.requireAPIKey(http.HandlerFunc(s.handlePruneGeoFilterStatus))).Methods("GET")
	r.Handle("/api/debug/affinity", s.requireAPIKey(http.HandlerFunc(s.handleDebugAffinity))).Methods("GET")
	r.Handle("/api/dropped-packets", s.requireAPIKey(http.HandlerFunc(s.handleDroppedPackets))).Methods("GET")

	// Packet endpoints
	s.registerPacketRoutes(r)

	// Decode endpoint
	r.HandleFunc("/api/decode", s.handleDecode).Methods("POST")

	// Neighbor Endpoints
	s.registerNeighborRoutes(r)

	// Clock-skew endpoints. These must be registered BEFORE the node routes:
	// /api/nodes/clock-skew would otherwise match /api/nodes/{pubkey} with
	// pubkey "clock-skew". /api/observers/clock-skew likewise has to precede
	// /api/observers/{id}.
	s.registerClockSkewRoutes(r)

	// Node endpoints — fixed routes BEFORE parameterized

	s.registerNodeRoutes(r)

	r.HandleFunc("/api/rx-coverage", s.handleRxCoverage).Methods("GET")
	r.HandleFunc("/api/rx-leaderboard", s.handleRxLeaderboard).Methods("GET")

	// Analytics endpoints
	s.registerAnalyticsRoutes(r)
	r.HandleFunc("/api/analytics/neighbor-graph", s.handleNeighborGraph).Methods("GET")

	// Other endpoints
	r.HandleFunc("/api/resolve-hops", s.handleResolveHops).Methods("GET")
	r.HandleFunc("/api/channels/{hash}/messages", s.handleChannelMessages).Methods("GET")
	r.HandleFunc("/api/channels", s.handleChannels).Methods("GET")
	r.HandleFunc("/api/known-channels", s.handleKnownChannels).Methods("GET")
	r.HandleFunc("/api/observers/metrics/summary", s.handleMetricsSummary).Methods("GET")
	r.HandleFunc("/api/observers/{id}/metrics", s.handleObserverMetrics).Methods("GET")
	r.HandleFunc("/api/observers/{id}/analytics", s.handleObserverAnalytics).Methods("GET")
	r.HandleFunc("/api/observers/{id}", s.handleObserverDetail).Methods("GET")
	r.HandleFunc("/api/observers", s.handleObservers).Methods("GET")
	r.HandleFunc("/api/traces/{hash}", s.handleTraces).Methods("GET")
	r.HandleFunc("/api/paths/inspect", s.handlePathInspect).Methods("POST")
	r.HandleFunc("/api/iata-coords", s.handleIATACoords).Methods("GET")
	r.HandleFunc("/api/audio-lab/buckets", s.handleAudioLabBuckets).Methods("GET")

	// OpenAPI spec + Swagger UI
	r.HandleFunc("/api/spec", s.handleOpenAPISpec).Methods("GET")
	r.HandleFunc("/api/docs", s.handleSwaggerUI).Methods("GET")
}

// noStoreAPIMiddleware sets Cache-Control: no-store on every response
// whose request path starts with /api/. See #1551 — CDNs cache JSON
// for minutes when no Cache-Control is present, which causes observers/
// packets/stats responses to go stale through Cloudflare/nginx/Varnish.
//
// Why no-store (not private,max-age=0):
//   - no-store is the most conservative directive; forbids ANY cache
//     (CDN, browser, intermediary) from storing the response.
//   - private,max-age=0 still permits short browser caches and some
//     intermediaries; we don't gain anything from it because the data
//     is fresh-on-every-request semantics by contract (WS pushes diff
//     against REST GETs).
//
// Scope: /api/ prefix only. Static assets (HTML/JS/CSS) keep their
// existing headers from spaHandler and remain CDN-cacheable on
// hashed URLs.
func noStoreAPIMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") {
			w.Header().Set("Cache-Control", "no-store")
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) perfMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/api/") {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		next.ServeHTTP(w, r)
		ms := float64(time.Since(start).Microseconds()) / 1000.0

		// Normalize key outside lock (no shared state needed)
		key := r.URL.Path
		if route := mux.CurrentRoute(r); route != nil {
			if tmpl, err := route.GetPathTemplate(); err == nil {
				key = muxBraceParam.ReplaceAllString(tmpl, ":$1")
			}
		}
		if key == r.URL.Path {
			key = perfHexFallback.ReplaceAllString(key, ":id")
		}

		s.perfStats.mu.Lock()
		s.perfStats.Requests++
		s.perfStats.TotalMs += ms

		if _, ok := s.perfStats.Endpoints[key]; !ok {
			s.perfStats.Endpoints[key] = &EndpointPerf{Recent: make([]float64, 0, 100)}
		}
		ep := s.perfStats.Endpoints[key]
		ep.Count++
		ep.TotalMs += ms
		if ms > ep.MaxMs {
			ep.MaxMs = ms
		}
		ep.Recent = append(ep.Recent, ms)
		if len(ep.Recent) > 100 {
			ep.Recent = ep.Recent[1:]
		}
		if ms > 100 {
			slow := api.SlowQuery{
				Path:   r.URL.Path,
				Ms:     round(ms, 1),
				Time:   types.WireTimeSeconds{Time: time.Now().UTC()},
				Status: 200,
			}
			s.perfStats.SlowQueries = append(s.perfStats.SlowQueries, slow)
			if len(s.perfStats.SlowQueries) > 50 {
				s.perfStats.SlowQueries = s.perfStats.SlowQueries[1:]
			}
		}
		s.perfStats.mu.Unlock()
	})
}

func (s *Server) requireAPIKey(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.cfg == nil || s.cfg.APIKey == "" {
			writeError(w, http.StatusForbidden, "write endpoints disabled — set apiKey in config.json")
			return
		}
		key := r.Header.Get("X-API-Key")
		if !constantTimeEqual(key, s.cfg.APIKey) {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		if IsWeakAPIKey(key) {
			writeError(w, http.StatusForbidden, "forbidden")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// --- Config Handlers ---

func (s *Server) handleConfigCache(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, numberMap(s.cfg.CacheTTL))
}

func (s *Server) handleConfigClient(w http.ResponseWriter, r *http.Request) {
	// #1508 — surface the operator-side customizer knobs. The frontend
	// (public/customize-v2.js _renderTabs) reads disabledTabs to hide
	// admin-only tabs from end users. Always return a non-nil slice so
	// the JSON shape is `[]` (not `null`) and the client can call
	// `.includes()` without an undefined guard.
	disabledTabs := []string{}
	if s.cfg.Customizer != nil && s.cfg.Customizer.DisabledTabs != nil {
		disabledTabs = s.cfg.Customizer.DisabledTabs
	}
	pathTrust := s.cfg.GetPathTrust()
	resp := api.ClientConfigResponse{
		Roles:               decodeOperatorBlock[api.ClientRoles](s.cfg.Roles, "roles"),
		HealthThresholds:    s.cfg.GetHealthThresholds().ToClientMs(),
		Map:                 clientMapConfig(s.cfg.Map),
		Tiles:               decodeOperatorBlock[api.LegacyTiles](s.cfg.Tiles, "tiles"),
		SnrThresholds:       decodeOperatorBlock[api.SnrThresholds](s.cfg.SnrThresholds, "snrThresholds"),
		DistThresholds:      decodeOperatorBlock[api.DistThresholds](s.cfg.DistThresholds, "distThresholds"),
		MaxHopDist:          s.cfg.MaxHopDist,
		Limits:              decodeOperatorBlock[api.ClientLimits](s.cfg.Limits, "limits"),
		PerfSlowMs:          s.cfg.PerfSlowMs,
		WsReconnectMs:       s.cfg.WsReconnectMs,
		CacheInvalidateMs:   s.cfg.CacheInvalidMs,
		ExternalUrls:        externalUrls(s.cfg.ExternalUrls),
		PropagationBufferMs: float64(s.cfg.PropagationBufferMs()),
		LiveMapMaxNodes:     s.cfg.LiveMapMaxNodes(),
		Timestamps:          s.cfg.GetTimestampConfig().ToAPI(),
		Customizer:          api.CustomizerClientConfig{DisabledTabs: disabledTabs},
		ClientRxCoverage:    s.cfg.ClientRxCoverageEnabled(),
		PathTrust:           &api.PathTrustConfig{},
	}
	if pathTrust.MinHashBytesForMapping != 0 {
		resp.PathTrust.MinHashBytesForMapping = &pathTrust.MinHashBytesForMapping
	}
	if s.cfg.DebugAffinity {
		resp.DebugAffinity = &s.cfg.DebugAffinity
	}
	resp.MapDarkTileProvider = optString(s.cfg.MapDarkTileProvider)
	writeJSON(w, resp)
}

func (s *Server) handleConfigAreas(w http.ResponseWriter, r *http.Request) {
	result := make([]api.AreaSummary, 0, len(s.cfg.Areas))
	for k, v := range s.cfg.Areas {
		if v.Label == "" {
			continue // skip comment/invalid entries (e.g. "_comment" keys in config)
		}
		result = append(result, api.AreaSummary{Key: k, Label: v.Label})
	}
	writeJSON(w, result)
}

func (s *Server) handleConfigAreasPolygons(w http.ResponseWriter, r *http.Request) {
	result := make([]api.AreaBoundary, 0, len(s.cfg.Areas))
	for k, v := range s.cfg.Areas {
		area := api.AreaBoundary{
			Key:    k,
			Label:  v.Label,
			LatMin: v.LatMin,
			LatMax: v.LatMax,
			LonMin: v.LonMin,
			LonMax: v.LonMax,
		}
		if len(v.Polygon) > 0 {
			polygon := latLonList(v.Polygon)
			area.Polygon = &polygon
		}
		result = append(result, area)
	}
	writeJSON(w, result)
}

func (s *Server) handleConfigRegions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	regions := make(api.RegionsResponse)
	for k, v := range s.cfg.Regions {
		regions[k] = v
	}
	codes, _ := s.db.GetDistinctIATAs(ctx)
	for _, c := range codes {
		if _, ok := regions[c]; !ok {
			regions[c] = c
		}
	}
	writeJSON(w, regions)
}

func (s *Server) handleConfigTheme(w http.ResponseWriter, r *http.Request) {
	theme := LoadTheme(".")

	branding := defaultThemeBranding()
	overlayOperatorBlocks(&branding, "branding", s.cfg.Branding, theme.Branding)

	themeColors := defaultLightThemeColors()
	overlayOperatorBlocks(&themeColors, "theme", s.cfg.Theme, theme.Theme)

	themeDark := defaultDarkThemeColors()
	overlayOperatorBlocks(&themeDark, "themeDark", s.cfg.ThemeDark, theme.ThemeDark)

	nodeColors := overlayStrings(defaultNodeColors(), s.cfg.NodeColors, theme.NodeColors)

	// #1799 PR #1804 r1 item 6: REQUEST→REQ rename is a BREAKING change to
	// the /api/config/theme shape. Compat policy for >=1 release cycle:
	//   - INCOMING: if an operator's config.json / theme.json carries the
	//     legacy "REQUEST" key, normalise it to "REQ" before the overlay so
	//     the override wins the canonical slot.
	//   - OUTGOING: dual-emit REQ AND REQUEST in the GET response so any
	//     consumer still reading the legacy key keeps working.
	typeColors := overlayStrings(defaultTypeColors(), normaliseTypeColorsLegacyKeys(s.cfg.TypeColors), normaliseTypeColorsLegacyKeys(theme.TypeColors))
	// Dual-emit REQUEST = REQ for back-compat (drop after >=1 release cycle).
	typeColors["REQUEST"] = typeColors["REQ"]

	home := defaultHomeConfig()
	for _, block := range []map[string]interface{}{s.cfg.Home, theme.Home} {
		if block == nil {
			continue
		}
		if overlay := decodeOperatorBlock[homeOverlay](block, "home"); overlay != nil {
			overlay.applyTo(&home)
		}
	}

	// #1488 — marker stroke overlay. Defaults mirror the :root values in
	// public/style.css so a fresh visitor with no config + no override
	// still gets the same painted outline as the static CSS fallback.
	markerStroke := defaultMarkerStroke()
	overlayOperatorBlocks(&markerStroke, "markerStroke", s.cfg.MarkerStroke, theme.MarkerStroke)

	writeJSON(w, api.ThemeResponse{
		Branding:     branding,
		Theme:        themeColors,
		ThemeDark:    themeDark,
		NodeColors:   nodeColors,
		TypeColors:   typeColors,
		Home:         home,
		MarkerStroke: markerStroke,
	})
}

func (s *Server) handleConfigMap(w http.ResponseWriter, r *http.Request) {
	center := s.cfg.MapDefaults.Center
	if len(center) == 0 {
		center = []float64{37.45, -122.0}
	}
	zoom := s.cfg.MapDefaults.Zoom
	if zoom == 0 {
		zoom = 9
	}
	writeJSON(w, api.MapConfigResponse{Center: center, Zoom: zoom})
}

func (s *Server) handleConfigGeoFilter(w http.ResponseWriter, r *http.Request) {
	gf := s.getGeoFilter()
	// writeEnabled signals to clients (e.g. the customizer UI) whether a
	// strong API key is configured. Low-sensitivity by design.
	writeEnabled := s.cfg != nil && s.cfg.APIKey != "" && !IsWeakAPIKey(s.cfg.APIKey)
	resp := geoFilterResponse(gf)
	resp.WriteEnabled = &writeEnabled
	writeJSON(w, resp)
}

// geoFilterResponse describes the filter in force. gf is nil when none is set.
func geoFilterResponse(gf *GeoFilterConfig) api.GeoFilterResponse {
	if gf == nil || len(gf.Polygon) == 0 {
		return api.GeoFilterResponse{}
	}
	polygon := latLonList(gf.Polygon)
	return api.GeoFilterResponse{Polygon: &polygon, BufferKm: gf.BufferKm}
}

// --- System Handlers ---

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	m := s.getMemStats()
	uptime := time.Since(s.startedAt).Seconds()

	wsClients := 0
	var wsDeny, wsRate, wsConnCap int64
	if s.hub != nil {
		wsClients = s.hub.ClientCount()
		wsDeny, wsRate, wsConnCap = s.hub.limits.counts() // #1794; nil-safe
	}

	// Real packet store stats
	pktCount := 0
	var pktEstMB float64
	var pktTrackedMB float64
	if s.store != nil {
		ps := s.store.GetPerfStoreStatsTyped()
		pktCount = ps.TotalLoaded
		pktEstMB = ps.EstimatedMB
		pktTrackedMB = ps.TrackedMB
	}

	// Real cache stats
	cs := api.CacheStats{}
	if s.store != nil {
		cs = s.store.GetCacheStatsTyped()
	}

	// Build eventLoop-equivalent from GC pause data (matches Node.js shape)
	var gcPauses []float64
	n := int(m.NumGC)
	if n > 256 {
		n = 256
	}
	for i := 0; i < n; i++ {
		idx := (int(m.NumGC) - n + i) % 256
		gcPauses = append(gcPauses, float64(m.PauseNs[idx])/1e6)
	}
	sortedPauses := sortedCopy(gcPauses)
	var lastPauseMs float64
	if m.NumGC > 0 {
		lastPauseMs = float64(m.PauseNs[(m.NumGC+255)%256]) / 1e6
	}

	// Build slow queries list (copy under lock)
	s.perfStats.mu.Lock()
	recentSlow := make([]api.SlowQuery, 0)
	sliceEnd := s.perfStats.SlowQueries
	if len(sliceEnd) > 5 {
		sliceEnd = sliceEnd[len(sliceEnd)-5:]
	}
	for _, sq := range sliceEnd {
		recentSlow = append(recentSlow, sq)
	}
	perfRequests := s.perfStats.Requests
	perfTotalMs := s.perfStats.TotalMs
	perfSlowCount := len(s.perfStats.SlowQueries)
	s.perfStats.mu.Unlock()

	ws := api.WebSocketStats{Clients: wsClients}
	if wsDeny != 0 {
		ws.RejectedDeny = &wsDeny
	}
	if wsRate != 0 {
		ws.RejectedRate = &wsRate
	}
	if wsConnCap != 0 {
		ws.RejectedConnCap = &wsConnCap
	}

	writeJSON(w, api.HealthResponse{
		Status:      "ok",
		Engine:      "go",
		Version:     s.version,
		Commit:      s.commit,
		BuildTime:   s.buildTime,
		Uptime:      int(uptime),
		UptimeHuman: fmt.Sprintf("%dh %dm", int(uptime)/3600, (int(uptime)%3600)/60),
		Memory: api.HealthMemory{
			Rss:       int(m.Sys / 1024 / 1024),
			HeapUsed:  int(m.HeapAlloc / 1024 / 1024),
			HeapTotal: int(m.HeapSys / 1024 / 1024),
			External:  0,
		},
		EventLoop: api.EventLoopStats{
			CurrentLagMs: round(lastPauseMs, 1),
			MaxLagMs:     round(percentile(sortedPauses, 1.0), 1),
			P50Ms:        round(percentile(sortedPauses, 0.5), 1),
			P95Ms:        round(percentile(sortedPauses, 0.95), 1),
			P99Ms:        round(percentile(sortedPauses, 0.99), 1),
		},
		Cache:     cs,
		Websocket: ws,
		PacketStore: api.HealthPacketStore{
			Packets:     pktCount,
			EstimatedMB: pktEstMB,
			TrackedMB:   pktTrackedMB,
		},
		Perf: api.HealthPerf{
			TotalRequests: int(perfRequests),
			AvgMs:         safeAvg(perfTotalMs, float64(perfRequests)),
			SlowQueries:   perfSlowCount,
			RecentSlow:    recentSlow,
		},
	})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	const statsTTL = 10 * time.Second

	s.statsMu.Lock()
	if s.statsCache != nil && time.Since(s.statsCachedAt) < statsTTL {
		cached := s.statsCache
		s.statsMu.Unlock()
		writeJSON(w, cached)
		return
	}
	s.statsMu.Unlock()

	// #1910: one rebuild per expiry, not one per request. Everything below runs
	// inside singleflight, so concurrent callers that miss the cache wait for the
	// first one's response instead of each running the same DB queries against a
	// 4-connection pool.
	built, sfErr, _ := s.statsSF.Do("stats", func() (interface{}, error) {

		// Re-check under the group: the winner may have just filled the cache.
		s.statsMu.Lock()
		if s.statsCache != nil && time.Since(s.statsCachedAt) < statsTTL {
			cached := s.statsCache
			s.statsMu.Unlock()
			return cached, nil
		}
		s.statsMu.Unlock()

		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 15*time.Second)
		defer cancel()
		now := time.Now()

		stats, err := s.store.GetStoreStats(ctx)
		if err != nil {
			return nil, err
		}
		counts, err := s.db.GetRoleCounts(ctx, now)
		if err != nil {
			return nil, err
		}

		mem := s.getMemorySnapshot(s.store.trackedMemoryMB())

		dropCount, err := s.db.CountDroppedPackets(ctx)
		if err != nil {
			log.Printf("count dropped packets: %v", err)
		}
		resp := &api.StatsResponse{
			TotalPackets:          stats.TotalPackets,
			TotalTransmissions:    stats.TotalTransmissions,
			TotalObservations:     stats.TotalObservations,
			TotalNodes:            stats.TotalNodes,
			TotalNodesAllTime:     stats.TotalNodesAllTime,
			TotalObservers:        stats.TotalObservers,
			PacketsLastHour:       stats.PacketsLastHour,
			PacketsLast24h:        stats.PacketsLast24h,
			Engine:                "go",
			Version:               s.version,
			Commit:                s.commit,
			BuildTime:             s.buildTime,
			Counts:                roleCountsToResponse(counts),
			SignatureDrops:        dropCount,
			HashMigrationComplete: s.store.hashMigrationComplete.Load(),

			TrackedMB:     mem.StoreDataMB, // deprecated alias
			StoreDataMB:   mem.StoreDataMB,
			ProcessRSSMB:  mem.ProcessRSSMB,
			GoHeapInuseMB: mem.GoHeapInuseMB,
			GoSysMB:       mem.GoSysMB,

			NeighborGraphCacheRebuildFailures: int64(atomic.LoadUint64(&s.neighborGraphCacheRebuildFailures)),
		}

		s.statsMu.Lock()
		s.statsCache = resp
		s.statsCachedAt = time.Now()
		s.statsMu.Unlock()

		return resp, nil
	})
	if sfErr != nil {
		writeError(w, 500, sfErr.Error())
		return
	}

	writeJSON(w, built.(*api.StatsResponse))
}

func (s *Server) handlePerf(w http.ResponseWriter, r *http.Request) {
	// Copy perfStats under lock to avoid data races
	s.perfStats.mu.Lock()
	type epSnapshot struct {
		path    string
		count   int
		totalMs float64
		maxMs   float64
		recent  []float64
	}
	epSnapshots := make([]epSnapshot, 0, len(s.perfStats.Endpoints))
	for path, ep := range s.perfStats.Endpoints {
		recentCopy := make([]float64, len(ep.Recent))
		copy(recentCopy, ep.Recent)
		epSnapshots = append(epSnapshots, epSnapshot{path, ep.Count, ep.TotalMs, ep.MaxMs, recentCopy})
	}
	uptimeSec := int(time.Since(s.perfStats.StartedAt).Seconds())
	totalRequests := s.perfStats.Requests
	totalMs := s.perfStats.TotalMs
	slowQueries := make([]api.SlowQuery, 0)
	sliceEnd := s.perfStats.SlowQueries
	if len(sliceEnd) > 20 {
		sliceEnd = sliceEnd[len(sliceEnd)-20:]
	}
	for _, sq := range sliceEnd {
		slowQueries = append(slowQueries, sq)
	}
	s.perfStats.mu.Unlock()

	// Process snapshots outside lock
	type epEntry struct {
		path string
		data *api.EndpointStats
	}
	var entries []epEntry
	for _, snap := range epSnapshots {
		sorted := sortedCopy(snap.recent)
		d := &api.EndpointStats{
			Count: snap.count,
			AvgMs: safeAvg(snap.totalMs, float64(snap.count)),
			P50Ms: round(percentile(sorted, 0.5), 1),
			P95Ms: round(percentile(sorted, 0.95), 1),
			MaxMs: round(snap.maxMs, 1),
		}
		entries = append(entries, epEntry{snap.path, d})
	}
	// Sort by total time spent (count * avg) descending, matching Node.js
	sort.Slice(entries, func(i, j int) bool {
		ti := float64(entries[i].data.Count) * entries[i].data.AvgMs
		tj := float64(entries[j].data.Count) * entries[j].data.AvgMs
		return ti > tj
	})
	summary := make(map[string]api.EndpointStats)
	for _, e := range entries {
		summary[e.path] = *e.data
	}

	// Cache stats from packet store
	var perfCS api.PerfCacheStats
	if s.store != nil {
		cs := s.store.GetCacheStatsTyped()
		perfCS = api.PerfCacheStats{
			Size:       cs.Entries,
			Hits:       cs.Hits,
			Misses:     cs.Misses,
			StaleHits:  cs.StaleHits,
			Recomputes: cs.Recomputes,
			HitRate:    cs.HitRate,
		}
	}

	// Packet store stats
	var pktStoreStats *api.PerfPacketStoreStats
	if s.store != nil {
		ps := s.store.GetPerfStoreStatsTyped()
		pktStoreStats = &ps
	}

	// Opt-in store memory diagnostic: an O(tx+obs) walk, only when explicitly
	// requested so the hot /api/perf path stays cheap.
	var memBreakdown *api.StoreMemoryBreakdown
	var breakdownNote string
	if s.store != nil && r.URL.Query().Get("mem") == "1" {
		memBreakdown = s.store.GetStoreMemoryBreakdown()
		breakdownNote = memBreakdownNote
	}

	// SQLite stats
	var dbStats *api.DbStats
	ss, err := s.db.GetDBSizeStats(r.Context())
	if err == nil {
		dbStats = dbSizeStatsToResponse(ss)
	}

	ms := s.getMemStats()
	resp := api.PerfResponse{
		Uptime:        uptimeSec,
		TotalRequests: totalRequests,
		AvgMs:         safeAvg(totalMs, float64(totalRequests)),
		Endpoints:     summary,
		SlowQueries:   slowQueries,
		Cache:         perfCS,
		PacketStore:   pktStoreStats,
		DbStats:       dbStats,
		GoRuntime: api.GoRuntimeStats{
			Goroutines:   runtime.NumGoroutine(),
			NumGC:        int64(ms.NumGC),
			PauseTotalMs: float64(ms.PauseTotalNs) / 1e6,
			LastPauseMs:  float64(ms.PauseNs[(ms.NumGC+255)%256]) / 1e6,
			HeapAllocMB:  float64(ms.HeapAlloc) / 1024 / 1024,
			HeapSysMB:    float64(ms.HeapSys) / 1024 / 1024,
			HeapInuseMB:  float64(ms.HeapInuse) / 1024 / 1024,
			HeapIdleMB:   float64(ms.HeapIdle) / 1024 / 1024,
			NumCPU:       runtime.NumCPU(),
		},
	}
	if memBreakdown != nil {
		resp.MemoryBreakdown = memBreakdown
		resp.MemoryBreakdownNote = optString(breakdownNote)
	}
	writeJSON(w, resp)
}

func (s *Server) handlePerfReset(w http.ResponseWriter, r *http.Request) {
	s.perfStats.mu.Lock()
	s.perfStats.Requests = 0
	s.perfStats.TotalMs = 0
	s.perfStats.Endpoints = make(map[string]*EndpointPerf)
	s.perfStats.SlowQueries = make([]api.SlowQuery, 0)
	s.perfStats.StartedAt = time.Now()
	s.perfStats.mu.Unlock()
	writeJSON(w, OkResp{Ok: true})
}

// splitObserverIDs turns "A, b,,C" into ["a", "b", "c"]: trimmed, lowercased,
// empty entries dropped. An empty or all-blank value returns nil (no filter).
func splitObserverIDs(raw string) []string {
	var out []string
	for _, id := range strings.Split(raw, ",") {
		if id = strings.ToLower(strings.TrimSpace(id)); id != "" {
			out = append(out, id)
		}
	}
	return out
}

// muxBraceParam matches {param} in gorilla/mux route templates for normalization.
var muxBraceParam = regexp.MustCompile(`\{([^}]+)\}`)

// perfHexFallback matches hex IDs for perf path normalization fallback.
var perfHexFallback = regexp.MustCompile(`[0-9a-f]{8,}`)

// handleBatchObservations returns observations for multiple hashes in a single request.
// POST /api/packets/observations with JSON body: {"hashes": ["abc123", "def456", ...]}
// Response: {"results": {"abc123": [...observations...], "def456": [...], ...}}
// Limited to 200 hashes per request to prevent abuse.
func (s *Server) handleDecode(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Hex string `json:"hex"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, 400, "invalid JSON body")
		return
	}
	hexStr := strings.TrimSpace(body.Hex)
	if hexStr == "" {
		writeError(w, 400, "hex is required")
		return
	}
	decoded, err := decoder.DecodePacket(hexStr, nil, true)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	writeJSON(w, DecodeResponse{
		Decoded: map[string]interface{}{
			"header":  decoded.Header,
			"path":    decoded.Path,
			"payload": decoded.Payload,
		},
	})
}

// --- Other Handlers ---

func (s *Server) handleResolveHops(w http.ResponseWriter, r *http.Request) {
	hopsParam := r.URL.Query().Get("hops")
	if hopsParam == "" {
		writeJSON(w, api.ResolveHopsResponse{Resolved: map[string]api.HopResolution{}})
		return
	}
	hops := strings.Split(hopsParam, ",")
	resolved := map[string]api.HopResolution{}

	// Context for affinity-based disambiguation.
	fromNode := r.URL.Query().Get("from_node")
	observer := r.URL.Query().Get("observer")
	var contextPubkeys []string
	if fromNode != "" {
		contextPubkeys = append(contextPubkeys, fromNode)
	}
	if observer != "" {
		contextPubkeys = append(contextPubkeys, observer)
	}

	// Get the neighbor graph for affinity scoring (may be nil).
	var graph *NeighborGraph
	if len(contextPubkeys) > 0 {
		graph = s.getNeighborGraph()
	}

	// Get the server's prefix map for resolveWithContext.
	var pm *prefixMap
	if s.store != nil {
		s.store.mu.RLock()
		_, pm = s.store.getCachedNodesAndPM()
		s.store.mu.RUnlock()
	}

	for _, hop := range hops {
		if hop == "" {
			continue
		}
		hopLower := strings.ToLower(hop)

		// Resolve candidates from the in-memory prefix map instead of
		// issuing per-hop DB queries (fixes N+1 pattern, see #369).
		var candidates []api.HopCandidate
		if pm != nil {
			if matched, ok := pm.m[hopLower]; ok {
				for _, ni := range matched {
					// Skip blacklisted nodes from resolution results.
					if s.cfg != nil && s.cfg.IsBlacklisted(ni.PublicKey) {
						continue
					}
					// #1181: skip hidden-prefix nodes too. We have the
					// name on ni so no extra DB lookup is needed.
					if s.cfg != nil && s.cfg.IsNameHidden(ni.Name) {
						continue
					}
					c := api.HopCandidate{Pubkey: ni.PublicKey, Name: optString(ni.Name)}
					if ni.HasGPS {
						lat, lon := ni.Lat, ni.Lon
						c.Lat, c.Lon = &lat, &lon
					}
					candidates = append(candidates, c)
				}
			}
		}

		if len(candidates) == 0 {
			resolved[hop] = api.HopResolution{Candidates: []api.HopCandidate{}, Conflicts: []api.HopCandidate{}, Confidence: api.HopConfidence("no_match")}
		} else if len(candidates) == 1 {
			resolved[hop] = api.HopResolution{
				Name: candidates[0].Name, Pubkey: &candidates[0].Pubkey,
				Candidates: candidates, Conflicts: []api.HopCandidate{},
				Confidence: api.HopConfidence("unique_prefix"),
			}
		} else {
			// Compute affinity scores for each candidate if we have context.
			if graph != nil && len(contextPubkeys) > 0 {
				now := time.Now()
				for i := range candidates {
					candPK := strings.ToLower(candidates[i].Pubkey)
					bestScore := 0.0
					for _, ctxPK := range contextPubkeys {
						edges := graph.Neighbors(strings.ToLower(ctxPK))
						for _, e := range edges {
							if e.Ambiguous {
								continue
							}
							otherPK := e.NodeA
							if strings.EqualFold(otherPK, ctxPK) {
								otherPK = e.NodeB
							}
							if strings.EqualFold(otherPK, candPK) {
								sc := e.Score(now)
								if sc > bestScore {
									bestScore = sc
								}
							}
						}
					}
					if bestScore > 0 {
						s := bestScore
						candidates[i].AffinityScore = &s
					}
				}
			}

			// Use resolveWithContext for 4-tier disambiguation.
			var best *nodeInfo
			var confidence string
			if pm != nil {
				best, confidence, _ = pm.resolveWithContext(hopLower, contextPubkeys, graph)
			}

			ambig := true
			hr := api.HopResolution{
				Name: candidates[0].Name, Pubkey: &candidates[0].Pubkey,
				Ambiguous: &ambig, Candidates: candidates, Conflicts: slices.Clone(candidates),
				Confidence: api.HopConfidence("ambiguous"),
			}

			// Use the resolved node as the default (best-effort pick).
			// Skip if the best pick is blacklisted or has a hidden-prefix
			// name (#1181).
			if best != nil && !(s.cfg != nil && (s.cfg.IsBlacklisted(best.PublicKey) || s.cfg.IsNameHidden(best.Name))) {
				hr.Name = optString(best.Name)
				hr.Pubkey = &best.PublicKey
			}

			// Only promote to bestCandidate when affinity is confident.
			if confidence == "neighbor_affinity" && best != nil {
				pk := best.PublicKey
				hr.BestCandidate = &pk
				hr.Confidence = api.HopConfidence("neighbor_affinity")
			} else if (confidence == "geo_proximity" || confidence == "gps_preference" || confidence == "observation_count_fallback") && best != nil {
				// Propagate lower-priority tiers so the API reflects the actual
				// resolution strategy used, rather than collapsing everything to "ambiguous".
				hr.Confidence = api.HopConfidence(confidence)
			}

			resolved[hop] = hr
		}
	}
	writeJSON(w, api.ResolveHopsResponse{Resolved: resolved})
}

func (s *Server) handleChannels(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	region := r.URL.Query().Get("region")
	includeEncrypted := r.URL.Query().Get("includeEncrypted") == "true"
	cacheKey := channelsCacheKey(region, includeEncrypted)

	if v, ok := s.channelsCache.entries.Load(cacheKey); ok {
		if e, ok := v.(*channelsCacheEntry); ok && e != nil && !s.isChannelsListCacheStale(e.at) {
			w.Header().Set("X-Cache-Age-Seconds", cacheAgeSecondsHeader(time.Since(e.at)))
			writeJSON(w, e.resp)
			return
		}
	}

	v, err, _ := s.channelsCache.sf.Do(cacheKey, func() (interface{}, error) {
		if v, ok := s.channelsCache.entries.Load(cacheKey); ok {
			if e, ok := v.(*channelsCacheEntry); ok && e != nil && !s.isChannelsListCacheStale(e.at) {
				return e, nil
			}
		}
		resp, fetchErr := s.fetchChannelsList(ctx, region, includeEncrypted)
		if fetchErr != nil {
			return nil, fetchErr
		}
		s.channelsCache.fillCount.Add(1)
		entry := &channelsCacheEntry{resp: resp, at: time.Now()}
		s.channelsCache.entries.Store(cacheKey, entry)
		return entry, nil
	})
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	entry := v.(*channelsCacheEntry)
	w.Header().Set("X-Cache-Age-Seconds", cacheAgeSecondsHeader(time.Since(entry.at)))
	writeJSON(w, entry.resp)
}

func (s *Server) handleChannelMessages(w http.ResponseWriter, r *http.Request) {
	hash := mux.Vars(r)["hash"]
	limit := queryLimit(r, 100, s.cfg.ListLimits.ChannelMessagesMax)
	offset := queryInt(r, "offset", 0)
	region := r.URL.Query().Get("region")
	// The database has the full history; the in-memory store has limited retention.
	messages, total, err := s.getChannelMessages(r.Context(), hash, false, limit, offset, region)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, api.ChannelMessagesResponse{Messages: messages, Total: total})
}

func (s *Server) handleObservers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	// #1481 P0-3 + #1483: serve from 30s atomic-pointer cache for the
	// default (no-filter) query shape. Refill is collapsed via
	// singleflight so concurrent TTL-boundary requests do not stampede
	// the 1.9M-row observations table.
	isDefault := r.URL.RawQuery == ""
	if isDefault {
		if e, ok := s.loadObserversCache(); ok && !s.observersCacheExpired(e.at) {
			w.Header().Set("X-Cache-Age-Seconds", cacheAgeSecondsHeader(time.Since(e.at)))
			writeJSON(w, e.resp)
			return
		}
	}

	if isDefault {
		v, err, _ := s.observersCacheV2.sf.Do(observersCacheFlightKey, func() (interface{}, error) {
			// Double-check inside the singleflight: another winner
			// may have just stored a fresh entry.
			if e, ok := s.loadObserversCache(); ok && !s.observersCacheExpired(e.at) {
				return e, nil
			}
			resp, herr := s.buildObserversDefaultResponse(ctx)
			if herr != nil {
				return nil, herr
			}
			s.observersCacheV2.fillCount.Add(1)
			entry := &observersCacheEntry{resp: resp, at: time.Now()}
			s.observersCacheV2.ptr.Store(entry)
			return entry, nil
		})
		if err != nil {
			writeError(w, 500, err.Error())
			return
		}
		entry := v.(*observersCacheEntry)
		w.Header().Set("X-Cache-Age-Seconds", cacheAgeSecondsHeader(time.Since(entry.at)))
		writeJSON(w, entry.resp)
		return
	}

	// Non-default queries bypass the cache entirely (filters not yet wired).
	resp, err := s.buildObserversDefaultResponse(ctx)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, resp)
}

// buildObserversDefaultResponse runs the underlying SQL pipeline for
// the default-shape /api/observers payload. Extracted so the cache
// refill path can be wrapped in singleflight and counted by tests.
// #1483 follow-up.
func (s *Server) buildObserversDefaultResponse(ctx context.Context) (api.ObserverListResponse, error) {
	observers, err := s.db.GetObservers(ctx)
	if err != nil {
		return api.ObserverListResponse{}, err
	}

	// Batch lookup: packetsLastHour per observer
	oneHourAgo := time.Now().Add(-1 * time.Hour)
	pktCounts, err := s.db.GetObserverPacketCounts(ctx, oneHourAgo)
	if err != nil {
		return api.ObserverListResponse{}, err
	}

	// Batch lookup: node locations only for observer IDs (not all nodes)
	observerIDs := make([]string, len(observers))
	for i, o := range observers {
		observerIDs[i] = o.Pubkey
	}
	nodeLocations, err := s.db.GetNodeLocationsByKeys(ctx, observerIDs)
	if err != nil {
		return api.ObserverListResponse{}, err
	}

	result := make([]api.Observer, 0, len(observers))
	now := time.Now().UTC()
	for i := range observers {
		o := &observers[i]
		// Defense in depth: skip observers that are in the blacklist
		if s.cfg != nil && s.cfg.IsObserverBlacklisted(o.Pubkey) {
			continue
		}
		resp := newObserver(o, pktCounts[o.Pubkey], now)
		if nodeLoc, ok := nodeLocations[strings.ToLower(o.Pubkey)]; ok {
			resp.Lat = nodeLoc.Lat
			resp.Lon = nodeLoc.Lon
			resp.NodeRole = nodeLoc.Role
		}
		result = append(result, resp)
	}
	return api.ObserverListResponse{
		Observers:  result,
		ServerTime: types.WireTimeSeconds{Time: now},
	}, nil
}

func (s *Server) handleObserverDetail(w http.ResponseWriter, r *http.Request) {
	id := strings.ToLower(mux.Vars(r)["id"]) // pubkeys are stored lowercase
	ctx := r.Context()

	// Defense in depth: reject blacklisted observer
	if s.cfg != nil && s.cfg.IsObserverBlacklisted(id) {
		writeError(w, 404, "Observer not found")
		return
	}

	obs, err := s.db.GetObserverByID(ctx, id)
	if errors.Is(err, database.ErrNotFound) {
		writeError(w, 404, "Observer not found")
		return
	}
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}

	// packetsLastHour from observations
	oneHourAgo := time.Now().Add(-1 * time.Hour)
	pktCounts, err := s.db.GetObserverPacketCounts(ctx, oneHourAgo)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}

	// A missing key means 0 observations in the window.
	writeJSON(w, newObserver(obs, pktCounts[id], time.Now().UTC()))
}

func (s *Server) handleObserverAnalytics(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	days := queryInt(r, "days", 7)
	if days < 1 {
		days = 1
	}
	if days > 365 {
		days = 365
	}
	if s.store == nil {
		writeError(w, 503, "Packet store unavailable")
		return
	}

	// #1828 Phase A: aggregate builders extracted into observer_analytics.go.
	// Single snapshot under RLock (#1481 P0-2), then five composable helpers
	// off the snapshot. No behavior change; JSON output is byte-identical.
	since := time.Now().Add(-time.Duration(days) * 24 * time.Hour)
	s.store.mu.RLock()
	obsList := s.store.byObserver[id]
	obsSnapshot := make([]*StoreObs, len(obsList))
	copy(obsSnapshot, obsList)
	// #134: resolve each observation's *StoreTx while we still hold the RLock.
	// enrichObs reads s.byTxID, which the ingestor writes concurrently; doing
	// that lookup after RUnlock is a fatal "concurrent map read and map write".
	// byTxID is keyed by transmission id, so shared transmissions dedupe here;
	// a missing id maps to nil, which enrichObsWithTx handles.
	//
	// This snapshot holds *pointers*. Closing only the map race is sound because
	// the store replaces index entries rather than mutating live objects in
	// place; reading tx fields after RUnlock relies on that lifecycle invariant,
	// not on this lookup. The one known in-place mutation (hash-migrate) is a
	// separate field-level race tracked in #142.
	txSnapshot := make(map[int]*StoreTx, len(obsSnapshot))
	for _, obs := range obsSnapshot {
		txSnapshot[obs.TransmissionID] = s.store.byTxID[obs.TransmissionID]
	}
	s.store.mu.RUnlock()
	filtered := make([]*StoreObs, 0, len(obsSnapshot))
	for _, obs := range obsSnapshot {
		if !obs.Timestamp.Before(since) {
			filtered = append(filtered, obs)
		}
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].Timestamp.After(filtered[j].Timestamp) })

	bucketDur := 24 * time.Hour
	if days <= 1 {
		bucketDur = time.Hour
	} else if days <= 7 {
		bucketDur = 4 * time.Hour
	}
	formatLabel := func(t time.Time) string {
		if days <= 1 {
			return t.UTC().Format("15:04")
		}
		if days <= 7 {
			return t.UTC().Format("Mon 15:04")
		}
		return t.UTC().Format("Jan 02")
	}

	packetTypes := map[types.PayloadType]int{}
	timelineCounts := map[int64]int{}
	nodeBucketSets := map[int64]map[string]struct{}{}
	snrBuckets := map[int]*api.SnrBucket{}

	for _, obs := range filtered {
		bucketStart := obs.Timestamp.UTC().Truncate(bucketDur).Unix()
		timelineCounts[bucketStart]++
		if nodeBucketSets[bucketStart] == nil {
			nodeBucketSets[bucketStart] = map[string]struct{}{}
		}

		if tx := txSnapshot[obs.TransmissionID]; tx != nil {
			if tx.PayloadType != nil {
				packetTypes[*tx.PayloadType]++ // map[types.PayloadType]int, as with buildPacketTypes
			}
			if tx.DecodedJSON != "" {
				var decoded map[string]interface{}
				if json.Unmarshal([]byte(tx.DecodedJSON), &decoded) == nil {
					for _, k := range []string{"pubKey", "srcHash", "destHash"} {
						if v, ok := decoded[k].(string); ok && v != "" {
							nodeBucketSets[bucketStart][v] = struct{}{}
						}
					}
				}
			}
		}
		for _, hop := range parsePathJSON(obs.PathJSON) {
			if hop != "" {
				nodeBucketSets[bucketStart][hop] = struct{}{}
			}
		}
		if obs.SNR != nil {
			bucket := int(*obs.SNR) / 2 * 2
			if *obs.SNR < 0 && int(*obs.SNR) != bucket {
				bucket -= 2
			}
			if snrBuckets[bucket] == nil {
				snrBuckets[bucket] = &api.SnrBucket{Range: fmt.Sprintf("%d to %d", bucket, bucket+2)}
			}
			snrBuckets[bucket].Count++
		}

	}
	// Recent packets (first 20 valid-timestamp observations, legacy index
	// semantics). Resolved paths are fetched in one batch outside the lock.
	selected := recentPacketObs(filtered, 20)
	selectedIDs := make([]int, len(selected))
	for i, o := range selected {
		selectedIDs[i] = o.ID
	}
	paths, err := s.db.ResolvedPaths(r.Context(), nil, selectedIDs)
	if err != nil {
		writeError(w, 500, "Internal error")
		return
	}
	recentPackets := buildRecentPackets(s.store, selected, txSnapshot, paths)
	// #1481 P0-2: RLock was released earlier after snapshotting the
	// observation pointer slice; no Unlock needed here.

	buildTimeline := func(counts map[int64]int) []api.TimelineBucket {
		keys := make([]int64, 0, len(counts))
		for k := range counts {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		out := make([]api.TimelineBucket, 0, len(keys))
		for _, k := range keys {
			out = append(out, api.TimelineBucket{Label: formatLabel(time.Unix(k, 0)), Count: counts[k]})
		}
		return out
	}

	nodeCounts := make(map[int64]int, len(nodeBucketSets))
	for k, nodes := range nodeBucketSets {
		nodeCounts[k] = len(nodes)
	}
	snrKeys := make([]int, 0, len(snrBuckets))
	for k := range snrBuckets {
		snrKeys = append(snrKeys, k)
	}
	sort.Ints(snrKeys)
	snrDistribution := make([]api.SnrBucket, 0, len(snrKeys))
	for _, k := range snrKeys {
		snrDistribution = append(snrDistribution, *snrBuckets[k])
	}

	writeJSON(w, api.ObserverAnalyticsResponse{
		Timeline:        buildTimeline(timelineCounts),
		PacketTypes:     packetTypes,
		NodesTimeline:   buildTimeline(nodeCounts),
		SnrDistribution: snrDistribution,
		RecentPackets:   recentPackets,
	})
}

func (s *Server) handleTraces(w http.ResponseWriter, r *http.Request) {
	hash := mux.Vars(r)["hash"]
	obs, err := s.db.GetObservationsForHash(r.Context(), hash)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, api.TraceResponse{Traces: traceEntries(obs)})
}

// traceEntries shapes observations (newest first, as the database returns
// them) into trace rows, oldest first. Never returns nil, so an unknown hash
// is "traces": [] and not null.
func traceEntries(obs []database.Observation) []api.TraceEntry {
	traces := make([]api.TraceEntry, 0, len(obs))
	for i := len(obs) - 1; i >= 0; i-- {
		o := obs[i]
		traces = append(traces, api.TraceEntry{
			Observer:     optString(o.ObserverID),
			ObserverName: optString(o.ObserverName),
			Time:         api.TimestampMillis{Time: o.Timestamp},
			Snr:          o.SNR,
			Rssi:         o.RSSI,
			PathJson:     hopsJSON(o.HopPrefixes()),
		})
	}
	return traces
}

// hopsJSON encodes hop prefixes as a JSON array string; no hops is "[]", never null.
func hopsJSON(hops []string) string {
	if hops == nil {
		hops = []string{}
	}
	b, _ := json.Marshal(hops) // []string cannot fail
	return string(b)
}

var iataCoords = map[string]IataCoord{
	"SJC": {Lat: 37.3626, Lon: -121.929},
	"SFO": {Lat: 37.6213, Lon: -122.379},
	"OAK": {Lat: 37.7213, Lon: -122.2208},
	"SEA": {Lat: 47.4502, Lon: -122.3088},
	"PDX": {Lat: 45.5898, Lon: -122.5951},
	"LAX": {Lat: 33.9425, Lon: -118.4081},
	"SAN": {Lat: 32.7338, Lon: -117.1933},
	"SMF": {Lat: 38.6954, Lon: -121.5908},
	"APC": {Lat: 38.2132, Lon: -122.2807}, // Napa County
	"STS": {Lat: 38.509, Lon: -122.8128},  // Charles M. Schulz–Sonoma County
	"MRY": {Lat: 36.587, Lon: -121.843},
	"EUG": {Lat: 44.1246, Lon: -123.2119},
	"RDD": {Lat: 40.509, Lon: -122.2934},
	"MFR": {Lat: 42.3742, Lon: -122.8735},
	"FAT": {Lat: 36.7762, Lon: -119.7181},
	"SBA": {Lat: 34.4262, Lon: -119.8405},
	"RNO": {Lat: 39.4991, Lon: -119.7681},
	"BOI": {Lat: 43.5644, Lon: -116.2228},
	"LAS": {Lat: 36.084, Lon: -115.1537},
	"PHX": {Lat: 33.4373, Lon: -112.0078},
	"SLC": {Lat: 40.7884, Lon: -111.9778},
	"DEN": {Lat: 39.8561, Lon: -104.6737},
	"DFW": {Lat: 32.8998, Lon: -97.0403},
	"IAH": {Lat: 29.9844, Lon: -95.3414},
	"AUS": {Lat: 30.1975, Lon: -97.6664},
	"MSP": {Lat: 44.8848, Lon: -93.2223},
	"ATL": {Lat: 33.6407, Lon: -84.4277},
	"ORD": {Lat: 41.9742, Lon: -87.9073},
	"JFK": {Lat: 40.6413, Lon: -73.7781},
	"EWR": {Lat: 40.6895, Lon: -74.1745},
	"BOS": {Lat: 42.3656, Lon: -71.0096},
	"MIA": {Lat: 25.7959, Lon: -80.287},
	"IAD": {Lat: 38.9531, Lon: -77.4565},
	"CLT": {Lat: 35.2144, Lon: -80.9473},
	"DTW": {Lat: 42.2124, Lon: -83.3534},
	"MCO": {Lat: 28.4312, Lon: -81.3081},
	"BNA": {Lat: 36.1263, Lon: -86.6774},
	"RDU": {Lat: 35.8801, Lon: -78.788},
	"YVR": {Lat: 49.1967, Lon: -123.1815},
	"YYZ": {Lat: 43.6777, Lon: -79.6248},
	"YYC": {Lat: 51.1215, Lon: -114.0076},
	"YEG": {Lat: 53.3097, Lon: -113.58},
	"YOW": {Lat: 45.3225, Lon: -75.6692},
	"LHR": {Lat: 51.47, Lon: -0.4543},
	"CDG": {Lat: 49.0097, Lon: 2.5479},
	"FRA": {Lat: 50.0379, Lon: 8.5622},
	"AMS": {Lat: 52.3105, Lon: 4.7683},
	"MUC": {Lat: 48.3537, Lon: 11.775},
	"SOF": {Lat: 42.6952, Lon: 23.4062},
	"NRT": {Lat: 35.772, Lon: 140.3929},
	"HND": {Lat: 35.5494, Lon: 139.7798},
	"ICN": {Lat: 37.4602, Lon: 126.4407},
	"SYD": {Lat: -33.9461, Lon: 151.1772},
	"MEL": {Lat: -37.669, Lon: 144.841},
}

func (s *Server) handleIATACoords(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, IataCoordsResponse{Coords: iataCoords})
}

func (s *Server) handleAudioLabBuckets(w http.ResponseWriter, r *http.Request) {
	buckets := map[string][]AudioLabPacket{}

	if s.store != nil {
		// Use in-memory store (matches Node.js pktStore.packets approach)
		s.store.mu.RLock()
		byType := map[string][]*StoreTx{}
		for _, tx := range s.store.packets {
			if tx.RawHex == "" {
				continue
			}
			typeName := "UNKNOWN"
			if tx.DecodedJSON != "" {
				if d := tx.ParsedDecoded(); d != nil {
					if t, ok := d["type"].(string); ok && t != "" {
						typeName = t
					}
				}
			}
			if typeName == "UNKNOWN" && tx.PayloadType != nil && tx.PayloadType.Valid() {
				typeName = tx.PayloadType.String()
			}
			byType[typeName] = append(byType[typeName], tx)
		}
		s.store.mu.RUnlock()

		for typeName, pkts := range byType {
			sort.Slice(pkts, func(i, j int) bool {
				return len(pkts[i].RawHex) < len(pkts[j].RawHex)
			})
			count := min(8, len(pkts))
			picked := make([]AudioLabPacket, 0, count)
			for i := 0; i < count; i++ {
				idx := (i * len(pkts)) / count
				tx := pkts[idx]
				pt := types.PayloadType(0)
				if tx.PayloadType != nil {
					pt = *tx.PayloadType
				}
				picked = append(picked, AudioLabPacket{
					Hash:             tx.Hash,
					RawHex:           tx.RawHex,
					DecodedJSON:      tx.DecodedJSON,
					ObservationCount: max(tx.ObservationCount, 1),
					PayloadType:      pt,
					PathJSON:         tx.PathJSON,
					ObserverID:       tx.ObserverID,
					Timestamp:        tx.FirstSeen,
				})
			}
			buckets[typeName] = picked
		}
	}

	writeJSON(w, AudioLabBucketsResponse{Buckets: buckets})
}

// --- Helpers ---

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[routes] JSON encode error: %v", err)
	}
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func queryInt(r *http.Request, key string, def int) int {
	v := r.URL.Query().Get(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

// normaliseTypeColorsLegacyKeys returns a copy of `m` with the legacy
// "REQUEST" key renamed to the canonical "REQ". If both keys are present
// the canonical wins (caller already chose the new name explicitly).
// #1799 PR #1804 r1 item 6: keeps stale operator config.json working
// after the REQUEST→REQ rename for >=1 release cycle. Returns nil for
// a nil input so mergeMap's nil-skip continues to work.
func normaliseTypeColorsLegacyKeys(m map[string]interface{}) map[string]interface{} {
	if m == nil {
		return nil
	}
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		out[k] = v
	}
	if legacy, ok := out["REQUEST"]; ok {
		if _, hasCanon := out["REQ"]; !hasCanon {
			out["REQ"] = legacy
		}
		delete(out, "REQUEST")
	}
	return out
}

// mergeMap returns a new map holding base overlaid with each overlay in order.
// The theme handler no longer uses it (it overlays typed structs now); it is
// kept for its own tests until they are retired with the old runtime spec.
func mergeMap(base map[string]interface{}, overlays ...map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{}, len(base))
	for k, v := range base {
		result[k] = v
	}
	for _, o := range overlays {
		for k, v := range o {
			result[k] = v
		}
	}
	return result
}

func safeAvg(total, count float64) float64 {
	if count == 0 {
		return 0
	}
	return round(total/count, 1)
}

func round(val float64, places int) float64 {
	m := 1.0
	for i := 0; i < places; i++ {
		m *= 10
	}
	return float64(int(val*m+0.5)) / m
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)) * p)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func sortedCopy(arr []float64) []float64 {
	cp := make([]float64, len(arr))
	copy(cp, arr)
	sort.Float64s(cp)
	return cp
}

func lastN(arr []map[string]interface{}, n int) []map[string]interface{} {
	if len(arr) <= n {
		return arr
	}
	return arr[len(arr)-n:]
}

func (s *Server) handleObserverMetrics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := strings.ToLower(mux.Vars(r)["id"]) // pubkeys are stored lowercase
	q := r.URL.Query()

	resolution, err := database.ParseMetricsResolution(q.Get("resolution"))
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}

	since, err := parsePacketTimeBound(q.Get("since"))
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	until, err := parsePacketTimeBound(q.Get("until"))
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	if since.IsZero() { // default to the last 24h
		since = time.Now().UTC().Add(-24 * time.Hour)
	}
	var untilPtr *time.Time
	if !until.IsZero() {
		untilPtr = &until
	}

	raw, err := s.db.GetObserverMetrics(ctx, id, &since, untilPtr, resolution)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	metrics, reboots := computeDeltas(raw, metricsBucketSeconds(resolution, 300))
	if metrics == nil {
		metrics = []api.MetricsSample{}
	}
	if reboots == nil {
		reboots = []api.TimestampSeconds{}
	}

	var name *string
	obs, err := s.db.GetObserverByID(ctx, id)
	switch {
	case errors.Is(err, database.ErrNotFound):
	case err != nil:
		writeError(w, 500, err.Error())
		return
	case obs != nil:
		name = obs.Name
	}

	writeJSON(w, api.ObserverMetricsResponse{
		ObserverId: id, ObserverName: name, Reboots: reboots, Metrics: metrics,
	})
}

func (s *Server) handleMetricsSummary(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	window := r.URL.Query().Get("window")
	if window == "" {
		window = "24h"
	}
	region := r.URL.Query().Get("region")

	dur, err := parseWindowDuration(window)
	if err != nil {
		writeError(w, 400, "invalid window: "+window)
		return
	}

	summary, err := s.db.GetMetricsSummary(ctx, time.Now().UTC().Add(-dur))
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}

	if region != "" {
		filtered := make([]database.MetricsSummary, 0, len(summary))
		for _, row := range summary {
			if strings.EqualFold(row.IATA, region) {
				filtered = append(filtered, row)
			}
		}
		summary = filtered
	}

	observers := make([]api.MetricsSummaryObserver, len(summary))
	for i, s := range summary {
		observers[i] = newMetricsSummaryObserver(s)
	}
	writeJSON(w, api.MetricsSummaryResponse{Observers: observers})
}

// parseWindowDuration parses strings like "24h", "3d", "7d", "30d".
func parseWindowDuration(window string) (time.Duration, error) {
	if strings.HasSuffix(window, "d") {
		daysStr := strings.TrimSuffix(window, "d")
		days, err := strconv.Atoi(daysStr)
		if err != nil || days <= 0 {
			return 0, fmt.Errorf("invalid days: %s", daysStr)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	return time.ParseDuration(window)
}

// handleAdminPrune was removed in #1283. Prune now runs in the ingestor
// process (server is read-only). The function and route are gone.

// constantTimeEqual compares two strings in constant time to prevent timing attacks.
func constantTimeEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// handleDroppedPackets returns recently dropped packets for investigation.
func (s *Server) handleDroppedPackets(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	limit := queryLimit(r, 100, s.cfg.ListLimits.PacketsMax)
	observerID := r.URL.Query().Get("observer")
	nodePubkey := r.URL.Query().Get("pubkey")

	results, err := s.db.ListDroppedPackets(ctx, limit, observerID, nodePubkey)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, results)
}

func (s *Server) handleScopeStats(w http.ResponseWriter, r *http.Request) {
	const scopeStatsTTL = 30 * time.Second

	window := r.URL.Query().Get("window")
	if window == "" {
		window = "24h"
	}
	if window != "1h" && window != "24h" && window != "7d" {
		writeError(w, 400, "window must be 1h, 24h, or 7d")
		return
	}

	s.scopeStatsMu.Lock()
	if s.scopeStatsCache != nil {
		if cached, ok := s.scopeStatsCache[window]; ok && time.Since(s.scopeStatsCachedAt[window]) < scopeStatsTTL {
			s.scopeStatsMu.Unlock()
			writeJSON(w, cached)
			return
		}
	}
	s.scopeStatsMu.Unlock()

	name, since, bucket := scopeStatsWindow(window, time.Now())
	st, err := s.db.GetScopeStats(r.Context(), since, bucket)
	if err != nil { /* your existing error response */
	}
	resp := scopeStatsToResponse(name, st)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}

	s.scopeStatsMu.Lock()
	if s.scopeStatsCache == nil {
		s.scopeStatsCache = make(map[string]*api.ScopeStatsResponse)
		s.scopeStatsCachedAt = make(map[string]time.Time)
	}
	s.scopeStatsCache[window] = resp
	s.scopeStatsCachedAt[window] = time.Now()
	s.scopeStatsMu.Unlock()

	writeJSON(w, resp)
}

// handlePruneGeoFilter identifies (dry_run=true, default) or enqueues (confirm=true)
// deletion of nodes whose GPS coordinates fall outside the currently configured
// geo_filter. Nodes with no GPS fix are always kept. Requires geo_filter to be
// configured.
//
// Since #1283/#1289 the server opens SQLite read-only, so the actual DELETE is
// performed by the ingestor. The server writes a request marker file (see
// internal/prunequeue); the ingestor's maintenance loop consumes it and writes a
// result marker. The confirm response is 202 Accepted with a request id;
// clients poll GET /api/admin/prune-geo-filter/status?id=<id> for completion.
//
// Confirm requires the pubkeys from the preview in the request body to prevent
// TOCTOU races: only nodes that were shown in preview AND are still outside the
// filter are enqueued.
func (s *Server) handlePruneGeoFilter(w http.ResponseWriter, r *http.Request) {
	gf := s.getGeoFilter()
	if gf == nil || len(gf.Polygon) < 3 {
		writeError(w, http.StatusBadRequest, "no geo_filter configured")
		return
	}

	nodes, err := s.db.ListNodesForGeoPrune(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db error")
		return
	}

	type nodeResult struct {
		PubKey string   `json:"pubkey"`
		Name   string   `json:"name"`
		Lat    *float64 `json:"lat"`
		Lon    *float64 `json:"lon"`
	}

	var outside []nodeResult
	for _, n := range nodes {
		if n.Lat == nil || n.Lon == nil {
			continue // no GPS — always keep
		}
		if !NodePassesGeoFilter(n.Lat, n.Lon, gf) {
			outside = append(outside, nodeResult{PubKey: n.Pubkey, Name: n.Name, Lat: n.Lat, Lon: n.Lon})
		}
	}

	if r.URL.Query().Get("confirm") != "true" {
		// Dry run — return preview without enqueueing anything.
		writeJSON(w, map[string]interface{}{
			"dryRun": true,
			"count":  len(outside),
			"nodes":  outside,
		})
		return
	}

	// Confirmed enqueue — require pubkeys from the preview to prevent TOCTOU:
	// only nodes that were shown in preview AND are still outside the filter
	// at this exact moment are scheduled for deletion. (The ingestor honors
	// the list verbatim; it does NOT re-evaluate geo_filter membership.)
	var body struct {
		Pubkeys []string `json:"pubkeys"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Pubkeys) == 0 {
		writeError(w, http.StatusBadRequest, "confirm requires pubkeys from preview in request body")
		return
	}
	allowed := make(map[string]bool, len(body.Pubkeys))
	for _, pk := range body.Pubkeys {
		allowed[pk] = true
	}

	var toDelete []nodeResult
	for _, n := range outside {
		if allowed[n.PubKey] {
			toDelete = append(toDelete, n)
		}
	}

	pubkeys := make([]string, 0, len(toDelete))
	for _, n := range toDelete {
		pubkeys = append(pubkeys, n.PubKey)
	}

	id := prunequeue.NewID()
	req := prunequeue.Request{
		ID:          id,
		RequestedAt: time.Now().UTC(),
		Reason:      "geo-prune",
		Pubkeys:     pubkeys,
	}
	if err := prunequeue.WriteRequest(s.cfg.PruneQueuePath, req); err != nil {
		log.Printf("[geo-prune] failed to enqueue request %s: %v", id, err)
		writeError(w, http.StatusInternalServerError, "failed to enqueue prune request")
		return
	}
	log.Printf("[geo-prune] enqueued request %s for %d node(s) (queue dir=%s)",
		id, len(pubkeys), prunequeue.QueueDir(s.cfg.PruneQueuePath))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"dryRun":    false,
		"accepted":  true,
		"requestId": id,
		"count":     len(pubkeys),
		"nodes":     toDelete,
		"statusUrl": "/api/admin/prune-geo-filter/status?id=" + id,
	})
}

// handlePruneGeoFilterStatus reports the state of a previously-enqueued
// geo-prune request. While the request marker is still present the response is
// {"status":"pending"}. Once the ingestor writes a result, the response is
// {"status":"done","deleted":N,"completedAt":...} (or "error" if the ingestor
// failed). Returns 404 when neither marker nor result is found.
func (s *Server) handlePruneGeoFilterStatus(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing id")
		return
	}

	res, err := prunequeue.ReadResult(s.cfg.PruneQueuePath, id)
	if err != nil {
		if strings.Contains(err.Error(), "invalid prune request id") {
			writeError(w, http.StatusBadRequest, "invalid id")
			return
		}
		writeError(w, http.StatusInternalServerError, "status read failed")
		return
	}
	if res != nil {
		status := "done"
		if res.Error != "" {
			status = "error"
		}
		writeJSON(w, map[string]interface{}{
			"requestId":   res.ID,
			"status":      status,
			"deleted":     res.Deleted,
			"requestedAt": res.RequestedAt,
			"completedAt": res.CompletedAt,
			"error":       res.Error,
		})
		return
	}

	pending, err := prunequeue.RequestExists(s.cfg.PruneQueuePath, id)
	if err != nil {
		if strings.Contains(err.Error(), "invalid prune request id") {
			writeError(w, http.StatusBadRequest, "invalid id")
			return
		}
		writeError(w, http.StatusInternalServerError, "status read failed")
		return
	}
	if pending {
		writeJSON(w, map[string]interface{}{
			"requestId": id,
			"status":    "pending",
		})
		return
	}
	writeError(w, http.StatusNotFound, "unknown request id")
}

// handlePutConfigGeoFilter writes the geo_filter config to disk and updates the
// in-memory pointer atomically. Empty/missing polygon clears the filter.
//
// Backstop validation: ≤1000 points, every point in [-90,90]/[-180,180], no
// NaN/Inf; bufferKm finite, non-negative, ≤ 20000 km. Concurrent PUTs are
// serialized via s.saveMu so they cannot race on the .tmp file.
func (s *Server) handlePutConfigGeoFilter(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MB cap

	var body api.GeoFilterRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	var polygon [][2]float64
	if body.Polygon != nil {
		polygon = make([][2]float64, 0, len(*body.Polygon))
		for _, pt := range *body.Polygon {
			if len(pt) != 2 {
				writeError(w, http.StatusBadRequest, "polygon points must be [lat, lon] pairs")
				return
			}
			polygon = append(polygon, [2]float64{pt[0], pt[1]})
		}
	}

	if len(polygon) > 0 && len(polygon) < 3 {
		writeError(w, http.StatusBadRequest, "polygon must have at least 3 points")
		return
	}
	if len(polygon) > 1000 {
		writeError(w, http.StatusBadRequest, "polygon must have at most 1000 points")
		return
	}
	for _, pt := range polygon {
		if math.IsNaN(pt[0]) || math.IsNaN(pt[1]) || math.IsInf(pt[0], 0) || math.IsInf(pt[1], 0) ||
			pt[0] < -90 || pt[0] > 90 || pt[1] < -180 || pt[1] > 180 {
			writeError(w, http.StatusBadRequest, "polygon point out of range: lat must be in [-90,90], lon in [-180,180]")
			return
		}
	}
	bufferKm := 0.0
	if body.BufferKm != nil {
		bufferKm = *body.BufferKm
	}
	if math.IsNaN(bufferKm) || math.IsInf(bufferKm, 0) ||
		bufferKm < 0 || bufferKm > 20000 {
		writeError(w, http.StatusBadRequest, "bufferKm must be a finite number in [0, 20000]")
		return
	}

	var gf *GeoFilterConfig
	if len(polygon) >= 3 {
		gf = &GeoFilterConfig{Polygon: polygon, BufferKm: bufferKm}
	}

	s.saveMu.Lock()
	if s.configDir != "" {
		if err := SaveGeoFilter(s.configDir, gf); err != nil {
			s.saveMu.Unlock()
			log.Printf("[geofilter] save failed: %v", err)
			writeError(w, http.StatusInternalServerError, "failed to save config")
			return
		}
	}
	s.setGeoFilter(gf)
	s.saveMu.Unlock()

	writeJSON(w, geoFilterResponse(gf))
}
