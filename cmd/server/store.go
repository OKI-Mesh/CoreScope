package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/decoder"
	"github.com/OKI-Mesh/CoreScope/internal/mbcapqueue"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"golang.org/x/sync/singleflight"
)

// pathIDs lists the transmission IDs (and, when withObs, the observation IDs)
// whose resolved paths a response build will need. Caller holds s.mu.
func pathIDs(txs []*StoreTx, withObs bool) (txIDs, obsIDs []int) {
	txIDs = make([]int, 0, len(txs))
	for _, tx := range txs {
		txIDs = append(txIDs, tx.ID)
		if withObs {
			for _, o := range tx.Observations {
				obsIDs = append(obsIDs, o.ID)
			}
		}
	}
	return txIDs, obsIDs
}

// Stats is the aggregate count block of /api/stats (matches the Node.js db.getStats shape).
type Stats struct {
	TotalPackets       int `json:"totalPackets"`
	TotalTransmissions int `json:"totalTransmissions"`
	TotalObservations  int `json:"totalObservations"`
	TotalNodes         int `json:"totalNodes"`
	TotalNodesAllTime  int `json:"totalNodesAllTime"`
	TotalObservers     int `json:"totalObservers"`
	PacketsLastHour    int `json:"packetsLastHour"`
	PacketsLast24h     int `json:"packetsLast24h"`
}

// StoreTx is an in-memory transmission with embedded observations.
type StoreTx struct {
	ID          int
	RawHex      string
	Hash        string
	FirstSeen   time.Time
	RouteType   *types.RouteType
	PayloadType *types.PayloadType
	DecodedJSON string
	// ScopeName is the transmission's region scope name (transmissions.scope_name,
	// #899). nil means the row is not transport-scoped (SQL NULL), or the schema
	// has no such column (db.hasScopeName=false); a pointer to "" means
	// transport-scoped with an unmatched region. Used to surface the set of region
	// scopes a repeater has transported (#1751) and the packet-detail Scope row.
	ScopeName        *string
	Observations     []*StoreObs
	ObservationCount int
	// Display fields from longest-path observation
	ObserverID          string
	ObserverName        string
	ObserverIATA        string
	SNR                 *float64
	RSSI                *float64
	PathJSON            string
	Direction           string
	LatestSeen          time.Time
	UniqueObserverCount int // cached count of distinct observer IDs
	// Cached parsed fields (set once, read many)
	parsedPath    []string               // cached parsePathJSON result
	pathParsed    bool                   // whether parsedPath has been set
	decodedOnce   sync.Once              // guards parsedDecoded
	parsedDecoded map[string]interface{} // cached json.Unmarshal of DecodedJSON
	// Dedup map: "observerID|pathJSON" → true for O(1) duplicate checks
	obsKeys     map[string]bool
	observerSet map[string]bool // unique observer IDs (for UniqueObserverCount)
	// accountedBytes is what trackedBytes was last charged for this tx
	// (observations excluded). See rechargeTx.
	accountedBytes int64
}

// StoreObs is a lean in-memory observation (no duplication of transmission fields).
type StoreObs struct {
	ID             int
	TransmissionID int
	ObserverID     string
	ObserverName   string
	ObserverIATA   string
	Direction      string
	SNR            *float64
	RSSI           *float64
	Score          *float64
	PathJSON       string
	RawHex         string
	Timestamp      time.Time

	// ParsedTime caches the typed timestamp and whether it is present.
	tsParseOnce sync.Once
	tsParsed    time.Time
	tsParsedOK  bool
}

// ParsedTime returns the observation timestamp and reports whether it is set.
func (o *StoreObs) ParsedTime() (time.Time, bool) {
	o.tsParseOnce.Do(func() {
		o.tsParsed = o.Timestamp
		o.tsParsedOK = !o.Timestamp.IsZero()
	})
	return o.tsParsed, o.tsParsedOK
}

// StartNodeCache loads the first node snapshot before returning, so the first
// request never sees an empty node list, then keeps it fresh in the background
// until ctx is cancelled.
func (s *PacketStore) StartNodeCache(ctx context.Context) error {
	if err := s.RefreshNodeCache(ctx); err != nil {
		return fmt.Errorf("initial node cache: %w", err)
	}
	go s.RunNodeCacheRefresher(ctx)
	return nil
}

// ParsedDecoded returns the parsed DecodedJSON map, caching the result.
// Thread-safe via sync.Once — the first call parses, subsequent calls return cached.
func (tx *StoreTx) ParsedDecoded() map[string]interface{} {
	tx.decodedOnce.Do(func() {
		if tx.DecodedJSON != "" {
			json.Unmarshal([]byte(tx.DecodedJSON), &tx.parsedDecoded)
		}
	})
	return tx.parsedDecoded
}

// PacketStore holds all transmissions in memory with indexes for fast queries.
//
// Lock ordering
// =============
// PacketStore uses several mutexes. To prevent deadlocks, locks MUST be
// acquired in the order listed below. Never acquire a higher-numbered lock
// while holding a lower-numbered one.
//
//  1. mu            (sync.RWMutex) — guards the core packet data: packets,
//     indexes (byHash, byTxID, byObsID, byObserver, byNode,
//     byPathHop, byPayloadType), counters, and loaded flag.
//
//  2. cacheMu       (sync.Mutex)  — guards analytics response caches:
//     rfCache, topoCache, hashCache, collisionCache, chanCache,
//     distCache, subpathCache, and their TTLs/hit counters.
//     Also guards rate-limited invalidation state
//     (lastInvalidated, pendingInv).
//
//  3. channelsCacheMu (sync.Mutex) — guards the short-lived GetChannels
//     cache (channelsCacheKey/Exp/Res).
//
//  4. groupedCacheMu (sync.Mutex)  — guards the short-lived
//     QueryGroupedPackets cache.
//
//  5. regionObsMu   (sync.Mutex)  — guards the region→observer mapping
//     cache (regionObsCache, regionObsCacheTime).
//
//  6. hashSizeInfoMu (sync.Mutex)  — guards the cached hash-size-info
//     result (hashSizeInfoCache). Acquired independently or
//     under mu (in EvictStale).
//
// Nesting that occurs today:
//   - IngestNew:               mu → cacheMu → channelsCacheMu  (1 → 2 → 3, OK)
//   - IngestObservations:      mu → cacheMu                    (1 → 2, OK)
//   - RunEviction/EvictStale:  mu → cacheMu → channelsCacheMu  (1 → 2 → 3, OK)
//   - RunEviction/EvictStale:  mu → hashSizeInfoMu             (1 → 6, OK)
//   - invalidateCachesFor:     cacheMu → channelsCacheMu       (2 → 3, OK)
//
// All other locks are acquired independently (no nesting).
// When adding new lock acquisitions, respect this ordering.
type PacketStore struct {
	mu            sync.RWMutex
	db            *database.DB
	packets       []*StoreTx                       // sorted by first_seen ASC (oldest first; newest at tail)
	byHash        map[string]*StoreTx              // hash → *StoreTx
	byTxID        map[int]*StoreTx                 // transmission_id → *StoreTx
	byObsID       map[int]*StoreObs                // observation_id → *StoreObs
	maxTxID       int                              // highest transmission_id in store
	maxObsID      int                              // highest observation_id in store
	byObserver    map[string][]*StoreObs           // observer_id → observations
	byNode        map[string][]*StoreTx            // pubkey → transmissions
	nodeHashes    map[string]map[string]bool       // pubkey → Set<hash>
	byPathHop     map[string][]*StoreTx            // lowercase hop/pubkey → transmissions with that hop in path
	byPayloadType map[types.PayloadType][]*StoreTx // payload_type → transmissions
	loaded        bool
	totalObs      int
	insertCount   int64
	queryCount    int64
	// Response caches (separate mutex to avoid contention with store RWMutex)
	cacheMu           sync.RWMutex
	rfCache           map[string]*cachedResult         // region → cached RF result
	topoCache         map[string]*cachedResult         // region → cached topology result
	hashCache         map[string]*cachedHashSizes      // region → cached hash-sizes result
	collisionCache    map[string]*cachedHashCollisions // cached hash-collisions result keyed by region ("" = global)
	chanCache         map[string]*cachedResult         // region → cached channels result
	distCache         map[string]*cachedResult         // region → cached distance result
	subpathCache      map[string]*cachedResult         // params → cached subpaths result
	rfCacheTTL        time.Duration
	collisionCacheTTL time.Duration

	// region|window|bucket → retransmission pressure (#1699). Typed, so it
	// cannot share the map[string]interface{} caches above; nil until first use.
	retransCache map[string]*retransmissionCacheEntry
	retransSF    singleflight.Group // collapses concurrent misses on one retransCache key

	// Steady-state analytics recomputers (issue #1240). Each holds the
	// latest snapshot for the default region="" / zero-window query of
	// an analytics endpoint in an atomic.Value, refreshed by a
	// background goroutine on a fixed interval. When set, the matching
	// GetAnalytics* function serves from Load() instead of running the
	// on-request compute path. Region/window variants still go through
	// the legacy TTL cache (compute-on-miss).
	analyticsRecomputerMu    sync.RWMutex
	recompTopology           *analyticsRecomputer
	recompRF                 *analyticsRecomputer
	recompDistance           *analyticsRecomputer
	recompChannels           *analyticsRecomputer
	recompHashCollisions     *analyticsRecomputer
	recompHashSizes          *analyticsRecomputer
	recompRoles              *analyticsRecomputer
	recompObserversClockSkew *analyticsRecomputer
	recompNodesClockSkew     *analyticsRecomputer
	recompRetransmissions    *analyticsRecomputer
	recompDirectHeard        *analyticsRecomputer
	// directHeardSnap holds the latest directHeardIndex published by
	// recompDirectHeard. Separate from the recomputer's own cache so
	// readers never touch analyticsRecomputerMu. See direct_heard.go.
	directHeardSnap atomic.Value
	cacheHits       int64
	cacheMisses     int64
	// Rate-limited invalidation (fixes #533: caches cleared faster than hit)
	lastInvalidated time.Time
	pendingInv      *cacheInvalidation // accumulated dirty flags during cooldown
	invCooldown     time.Duration      // minimum time between invalidations
	// Short-lived cache for QueryGroupedPackets (avoids repeated full sort)
	groupedCacheMu    sync.Mutex
	groupedCacheKey   string
	groupedCacheExp   time.Time
	groupedCacheTxs   []*StoreTx // sorted by LatestSeen DESC
	groupedCacheTotal int
	// Short-lived cache for GetChannels (avoids repeated full scan + JSON unmarshal)
	channelsCacheMu  sync.Mutex
	channelsCacheKey string
	channelsCacheExp time.Time
	channelsCacheRes []map[string]interface{}
	// Cached region → observer ID mapping (30s TTL, avoids repeated DB queries)
	regionObsMu        sync.Mutex
	regionObsCache     map[string]map[string]bool
	regionObsCacheTime time.Time
	// Cached area key → node pubkey set (30s per-key TTL)
	areaNodeMu         sync.RWMutex
	areaNodeCache      map[string]map[string]bool
	areaNodeCacheTimes map[string]time.Time
	// Full server config — needed for Areas map in resolveAreaNodes.
	config *Config
	// Cached node list + prefix map (rebuilt on demand, shared across analytics)
	nodeCache     []nodeInfo
	nodePM        *prefixMap
	nodeCacheTime time.Time
	// Precomputed subpath index: raw comma-joined hops → occurrence count.
	// Built during Load(), incrementally updated on ingest. Avoids full
	// packet iteration at query time (O(unique_subpaths) vs O(total_packets)).
	spIndex      map[string]int        // "hop1,hop2" → count
	spTxIndex    map[string][]*StoreTx // "hop1,hop2" → transmissions containing this subpath
	spTotalPaths int                   // transmissions with paths >= 2 hops
	// Background-build ready gates for spIndex/spTxIndex and byPathHop
	// (#1008). Flipped from false→true exactly once by the goroutine
	// kicked off in Load() (or synchronously by the background chunk
	// loader). Handlers gate reads via SubpathIndexReady() /
	// PathHopIndexReady(); while false, they respond 503 + Retry-After.
	subpathReady atomic.Bool
	pathHopReady atomic.Bool
	// indexReadyChan is closed exactly once when BOTH subpathReady
	// and pathHopReady are true (#1008 review m6). Replaces the
	// previous 2ms poll in WaitIndexesReady. Lazily allocated by
	// indexReadyCh / maybeCloseIndexReadyCh in index_ready_1008.go.
	indexReadyChMu sync.Mutex
	indexReadyChan chan struct{}
	// Precomputed distance analytics: hop distances and path totals.
	// Built LAZILY on first /api/analytics/distance request (#1011) —
	// previously eager in Load() at startup, which was O(n²) work for
	// operators who never visit the distance analytics page. After the
	// initial lazy build, the in-memory index is maintained
	// incrementally by updateDistanceIndexForTxs on ingest.
	distHops  []distHopRecord
	distPaths []distPathRecord

	// Lazy-build gate for the distance index (#1011). distLazyBuilt is
	// true once the first /api/analytics/distance request has completed
	// its build (or a debounced rebuild). distLazyBuilding is true
	// while a build is currently running — concurrent requests in this
	// window receive 202 + Retry-After rather than racing N parallel
	// O(n²) computations. distLazyOnce serialises the first build;
	// reset by the background loader (Load() chunked merge) and by the
	// debounced-rebuild policy so subsequent rebuilds can re-fire.
	distLazyMu        sync.Mutex
	distLazyOnce      sync.Once
	distLazyBuilt     bool
	distLazyBuilding  bool
	distLazyLastBuilt time.Time
	distLazyLastObs   int // totalObs at last build, for Δobs debounce
	// distanceBuildHook, if non-nil, runs at the start of the lazy build
	// goroutine (after distLazyBuilding is set, before any lock is held). Tests
	// use it to hold the build open so concurrent requests deterministically
	// observe the "building" window; nil (and zero overhead) in production.
	distanceBuildHook func()

	// Cached GetNodeHashSizeInfo result — recomputed at most once every 15s
	hashSizeInfoMu    sync.Mutex
	hashSizeInfoCache map[string]*hashSizeNodeInfo
	hashSizeInfoAt    time.Time

	// Cached relay stats batch result — recomputed at most once every 300s
	// or when byPathHop changes (see invalidateRelayStatsCache).
	relayStatsCacheMu     sync.Mutex
	relayStatsCache       map[string]RepeaterNodeStats
	relayStatsCacheAt     time.Time
	relayStatsCacheWindow float64
	relayStatsCacheSig    string

	// Snapshot from the last analytics cycle + O(1) index, both under cacheMu.
	// Populated by analytics + pre-populated from DB on Load (read-only path).
	// Persistence to the DB is owned by the ingestor (#1289/#1324): the
	// analytics cycle publishes a snapshot file via internal/mbcapqueue
	// and the ingestor's RunMultibyteCapPersist applies it.
	mbCapSnapshot []MultiByteCapEntry
	mbCapIndex    map[string]MultiByteCapEntry

	// Cached per-pubkey relay info + usefulness score maps (#1257). These
	// fold the previously per-node GetRepeaterRelayInfo /
	// GetRepeaterUsefulnessScore loop in handleNodes into one O(N) pass
	// per 15s TTL window — eliminating N RLock acquisitions and N×
	// timestamp parses of the same byPathHop entries per request.
	repeaterEnrichMu      sync.Mutex
	repeaterRelayCache    map[string]RepeaterRelayInfo
	repeaterRelayCacheWin float64
	repeaterRelayAt       time.Time
	repeaterUsefulCache   map[string]float64
	repeaterUsefulAt      time.Time

	// Steady-state recomputer for the two caches above (#1262). When
	// started, an initial sync compute prewarms the caches so the very
	// first /api/nodes?limit=2000 from live.js's SPA bootstrap hits a
	// populated cache instead of paying the 15.7s on-thread rebuild.
	repeaterEnrichRecompMu      sync.Mutex
	repeaterEnrichRecompStarted bool
	repeaterEnrichRecompStop    chan struct{}
	repeaterEnrichRecompDone    chan struct{}

	// Bridge axis (issue #672 axis 2 of 4): atomic snapshot of pubkey
	// → 0..1 betweenness-centrality score over the current neighbor
	// graph. Populated by the bridge recomputer (bridge_recomputer.go);
	// nil until the first compute lands. Read path is a single atomic
	// pointer load — no lock contention with the per-request enrichment
	// path in handleNodes (same discipline as #1248).
	bridgeScoreMap atomic.Pointer[map[string]float64]

	// Coverage + Redundancy axes (issue #672 axes 3 & 4 of 4): atomic
	// snapshots of pubkey → 0..1 score over the current neighbor graph.
	// Coverage = normalized harmonic reach centrality; Redundancy =
	// articulation-point fragmentation criticality. Populated by the
	// usefulness-axes recomputer (usefulness_axes_recomputer.go); nil until
	// the first compute lands. Read path is a single atomic pointer load,
	// matching the bridge axis.
	coverageScoreMap   atomic.Pointer[map[string]float64]
	redundancyScoreMap atomic.Pointer[map[string]float64]

	// Start-once latch for the usefulness-axes recomputer
	// (usefulness_axes_recomputer.go). Per-store rather than package-global so
	// independent PacketStores each run their own recomputer; guards the
	// idempotent StartUsefulnessAxesRecomputer (a second call no-ops).
	usefulnessAxesRecompMu      sync.Mutex
	usefulnessAxesRecompStarted bool

	// Precomputed distinct advert pubkey count (refcounted for eviction correctness).
	// Updated incrementally during Load/Ingest/Evict — avoids JSON parsing in GetPerfStoreStats.
	advertPubkeys map[string]int // pubkey → number of advert packets referencing it

	// Resolved path membership index: xxhash → []txID (forward) and txID → []hashes (reverse).
	// Replaces per-StoreTx/StoreObs ResolvedPath []*string field (#800).
	resolvedPubkeyIndex           map[uint64][]int // hash(pubkey) → []txID
	resolvedPubkeyReverse         map[int][]uint64 // txID → []hashes indexed under
	useResolvedPathIndex          bool             // feature flag (default true, off path = conservative)
	maxResolvedPubkeyIndexEntries int              // hard cap for size warning (0 = use default 5M)

	// Persisted neighbor graph for hop resolution at ingest time.
	// Accessed via atomic.Pointer because async rebuilds (path_inspect.go
	// ensureNeighborGraph) and ingest-time readers race on the pointer
	// (issue #1203 sub-fix).
	graph atomic.Pointer[NeighborGraph]

	// Singleflight state for ensureNeighborGraph. These were package-globals
	// in #1203 r0 — moved to per-store fields (PR #1208 review) so parallel
	// tests with independent *PacketStore values don't share rebuild state
	// (cross-store deadlock/skip risk under -race).
	rebuildMu    sync.Mutex
	rebuildInFlt chan struct{} // nil when no rebuild is in flight

	// Path inspector score cache (issue #944).
	inspectMu    sync.RWMutex
	inspectCache map[string]*inspectCachedResult

	// Clock skew detection engine.
	clockSkew *ClockSkewEngine

	// Bounded cold load: oldest packet timestamp loaded into memory.
	// Empty string means all data is in memory (no limit applied).
	oldestLoaded time.Time

	// Hot startup atomic gates — see contract below.
	//
	// Contract / ordering invariant (PR #1187):
	//   * hashMigrationComplete (set by migrateContentHashesAsync) gates
	//     content-hash–dependent code paths (e.g. dedup correctness on the
	//     write side). Set true ONLY after the migration loop finishes.
	//   * backgroundLoadDone is set true exactly once, after
	//     loadBackgroundChunks finishes its loop AND its post-load index
	//     rebuild. It gates "hot startup has finished filling
	//     retentionHours of data into memory" (used by /api/perf and the
	//     UI's hot-load banner). It says nothing about success — see
	//     backgroundLoadFailed for the success/failure signal.
	//   * backgroundLoadFailed is set true ONLY if at least one chunk
	//     errored during background load. /api/perf surfaces it so
	//     operators can distinguish "done & full" from "done & partial".
	//     Read order MUST be: load backgroundLoadDone first; only if true
	//     is backgroundLoadFailed meaningful.
	// 0 = disabled (current behavior). Background loader fills the rest.
	//
	// IMMUTABILITY: hotStartupHours is set ONCE in NewPacketStore (with
	// optional clamp against retentionHours) and is NEVER mutated
	// afterward. Readers (LoadChunked, RunStartupLoad,
	// loadBackgroundChunks, GetPerfStoreStats) intentionally read it
	// without s.mu. Do not add a write path without also adding the
	// lock to every reader — see #1809 / #1811.
	//
	// ENFORCEMENT (dij #5): immutability is enforced indirectly. A
	// mutation that left oldestLoaded inconsistent with the new
	// hotStartupHours would trip the loadBackgroundChunks invariant
	// (see store.go ~line 1442 — the invariantViolation() guard).
	// That guard is the runtime backstop; this comment is the
	// compile-time discipline. If you genuinely need to mutate
	// hotStartupHours at runtime, audit every reader above first.
	hotStartupHours        float64
	backgroundLoadDone     atomic.Bool
	backgroundLoadFailed   atomic.Bool
	backgroundLoadProgress atomic.Int64 // 0–100 percent complete

	// #1690: backgroundLoadError captures the human-readable reason when
	// backgroundLoadFailed flips true (e.g. "loaded 12.3% of 1000 rows").
	// Guarded by bgErrMu (RWMutex) so the perf endpoint can read it
	// without synchronising on s.mu (held by chunk-merge writers).
	// CONCURRENCY (dij #4): EVERY read and write of backgroundLoadErr
	// MUST hold bgErrMu (RLock for reads, Lock for writes). Do not
	// promote to atomic.Pointer without also auditing the two call
	// sites in chunked_load.go's RunStartupLoad and store.go's
	// loadBackgroundChunks + GetPerfStoreStats + BackgroundLoadError.
	bgErrMu           sync.RWMutex
	backgroundLoadErr string
	// loadCoverageRatio: totalLoaded / totalInDB (0.0–1.0). Updated by
	// loadBackgroundChunks after the chunk walk; surfaced via the typed
	// perf payload for prod observability.
	loadCoverageRatio float64

	// Async hash migration state: set after migrateContentHashesAsync completes.
	hashMigrationComplete atomic.Bool

	// Chunked startup load state (#1009). LoadChunked closes
	// firstChunkReady after the first chunk is merged so the HTTP
	// listener can bind. loadComplete flips true after all chunks have
	// been processed; loadProgressRows is updated per-chunk so
	// loadStatusMiddleware can emit progress.
	chunkInitOnce      sync.Once
	firstChunkReady    chan struct{}
	firstChunkSignaled atomic.Bool
	loadComplete       atomic.Bool
	loadProgressRows   atomic.Int64
	chunkCBMu          sync.Mutex
	chunkCallbacks     []func(rowsThisChunk, totalRows int)

	// startupLoadDone is closed when RunStartupLoad returns (hot window
	// plus background fill); see StartupLoadDone.
	startupLoadDone     chan struct{}
	startupLoadSignaled atomic.Bool

	// Eviction config and stats
	retentionHours  float64        // 0 = unlimited
	maxMemoryMB     int            // 0 = unlimited (packet store memory budget)
	evicted         int64          // total packets evicted
	trackedBytes    int64          // running total of estimated packet store memory
	memoryEstimator func() float64 // injectable for tests; nil = use runtime.ReadMemStats (stats only)

	// Per-store ReadMemStats cache (5s TTL). Fields (not package-level vars) so
	// that test helpers constructing &PacketStore{...} directly get independent
	// cache state, avoiding order-dependent test failures.
	estMemMu  sync.Mutex
	estMemVal float64
	estMemAt  time.Time

	// Short-lived cache for the observations aggregate in GetStoreStats (30s TTL).
	// Avoids a per-/api/stats full-table scan; values accurate to ~30s which is
	// sufficient for dashboard display.
	statsCacheMu   sync.Mutex
	statsCacheTime time.Time
	statsLastHour  int
	statsLast24h   int
	// #1910: collapses concurrent misses onto one query. Without it every
	// in-flight request ran the observations scan itself the moment the cache
	// expired, because the check above releases statsCacheMu before doing the
	// work. With SetMaxOpenConns(4) and a page that fires five endpoints at
	// once, that turned one scan into a queue of them.
	statsSF singleflight.Group

	// Test-only hook fired at the very start of loadBackgroundChunks
	// (after the #1809 invariant check). Nil in production. Used by
	// ordering tests to capture the bg-loader entry timestamp/signal
	// without polling. See runstartup_load_test.go.
	bgLoaderEntryHook func()
	nodeSnap          atomic.Pointer[nodeSnapshot] // latest nodes + prefix map; nil until the first refresh
	nodeRefreshCh     chan struct{}                // buffered 1: "refresh soon", from InvalidateNodeCache
}

// Precomputed distance records for fast analytics aggregation.
type distHopRecord struct {
	FromName   string
	FromPk     string
	ToName     string
	ToPk       string
	Dist       float64
	Type       string // "R↔R", "C↔R", "C↔C"
	SNR        *float64
	Hash       string
	Timestamp  time.Time
	HourBucket string
	tx         *StoreTx
}

// dedupeHopsByPair groups hops by unordered node-pair, keeps the max-distance
// record per pair, and computes obsCount / bestSnr / medianSnr.  limit caps the
// number of returned entries (sorted by distance descending).
func dedupeHopsByPair(hops []distHopRecord, limit int) []map[string]interface{} {
	type pairAgg struct {
		best     *distHopRecord
		obsCount int
		maxSNR   *float64
		snrs     []float64
	}
	pairMap := make(map[string]*pairAgg)
	for i := range hops {
		h := &hops[i]
		pk1, pk2 := h.FromPk, h.ToPk
		if pk1 > pk2 {
			pk1, pk2 = pk2, pk1
		}
		key := pk1 + "|" + pk2
		agg, ok := pairMap[key]
		if !ok {
			agg = &pairAgg{}
			pairMap[key] = agg
		}
		agg.obsCount++
		if h.SNR != nil {
			agg.snrs = append(agg.snrs, *h.SNR)
			if agg.maxSNR == nil || *h.SNR > *agg.maxSNR {
				v := *h.SNR
				agg.maxSNR = &v
			}
		}
		if agg.best == nil || h.Dist > agg.best.Dist {
			agg.best = h
		}
	}
	type pairEntry struct {
		key string
		agg *pairAgg
	}
	pairs := make([]pairEntry, 0, len(pairMap))
	for k, v := range pairMap {
		pairs = append(pairs, pairEntry{k, v})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].agg.best.Dist > pairs[j].agg.best.Dist })
	result := make([]map[string]interface{}, 0, min(limit, len(pairs)))
	for i, pe := range pairs {
		if i >= limit {
			break
		}
		h := pe.agg.best
		var medianSNR *float64
		if len(pe.agg.snrs) > 0 {
			sorted := make([]float64, len(pe.agg.snrs))
			copy(sorted, pe.agg.snrs)
			sort.Float64s(sorted)
			mid := len(sorted) / 2
			if len(sorted)%2 == 0 {
				v := (sorted[mid-1] + sorted[mid]) / 2
				medianSNR = &v
			} else {
				v := sorted[mid]
				medianSNR = &v
			}
		}
		result = append(result, map[string]interface{}{
			"fromName": h.FromName, "fromPk": h.FromPk,
			"toName": h.ToName, "toPk": h.ToPk,
			"dist": h.Dist, "type": h.Type,
			"bestSnr": floatPtrOrNil(pe.agg.maxSNR), "medianSnr": floatPtrOrNil(medianSNR),
			"obsCount": pe.agg.obsCount,
			"hash":     h.Hash, "timestamp": h.Timestamp,
		})
	}
	return result
}

type distPathRecord struct {
	Hash      string
	TotalDist float64
	HopCount  int
	Timestamp time.Time
	Hops      []distHopDetail
	tx        *StoreTx
}

type distHopDetail struct {
	FromName string
	FromPk   string
	ToName   string
	ToPk     string
	Dist     float64
}

type cachedResult struct {
	data      map[string]interface{}
	expiresAt time.Time
}

// cachedHashSizes is a hash-sizes result held in hashCache until it expires.
type cachedHashSizes struct {
	data      api.HashSizeAnalyticsResponse
	expiresAt time.Time
}

// cachedHashCollisions is a hash-collisions result held in collisionCache
// until it expires.
type cachedHashCollisions struct {
	data      api.HashCollisionResponse
	expiresAt time.Time
}

// cacheTTLSec extracts a duration from the cacheTTL config map.
// Values may be float64 (from JSON) or int. Returns false if key is missing or non-positive.
func cacheTTLSec(m map[string]interface{}, key string) (time.Duration, bool) {
	v, ok := m[key]
	if !ok {
		return 0, false
	}
	var sec float64
	switch n := v.(type) {
	case float64:
		sec = n
	case int:
		sec = float64(n)
	case int64:
		sec = float64(n)
	default:
		return 0, false
	}
	if sec <= 0 {
		return 0, false
	}
	return time.Duration(sec * float64(time.Second)), true
}

// NewPacketStore creates a new empty packet store backed by db.
// cacheTTLs is the optional cacheTTL map from config.json; keys are strings, values are seconds.
func NewPacketStore(db *database.DB, cfg *PacketStoreConfig, cacheTTLs ...map[string]interface{}) *PacketStore {
	ps := &PacketStore{
		db:            db,
		packets:       make([]*StoreTx, 0, 65536),
		byHash:        make(map[string]*StoreTx, 65536),
		byTxID:        make(map[int]*StoreTx, 65536),
		byObsID:       make(map[int]*StoreObs, 65536),
		byObserver:    make(map[string][]*StoreObs),
		byNode:        make(map[string][]*StoreTx),
		byPathHop:     make(map[string][]*StoreTx),
		nodeHashes:    make(map[string]map[string]bool),
		byPayloadType: make(map[types.PayloadType][]*StoreTx),
		rfCache:       make(map[string]*cachedResult),
		topoCache:     make(map[string]*cachedResult),
		hashCache:     make(map[string]*cachedHashSizes),

		collisionCache: make(map[string]*cachedHashCollisions),
		chanCache:      make(map[string]*cachedResult),
		distCache:      make(map[string]*cachedResult),
		subpathCache:   make(map[string]*cachedResult),
		// #1239: 60 seconds by default. rfCacheTTL is the shared TTL for
		// the RF, topology, distance, hash-sizes, subpath, and channel
		// analytics caches. Distance analytics IS viewed live during
		// active analysis sessions (operators won't tolerate a 5-min
		// lag), so the bump from 15s → 60s smooths the most-frequent
		// cold-miss churn during heavy ingest without freezing data.
		// Override via cacheTTL.analyticsRF in config.json (also
		// propagates to distance / topology / hash-sizes / etc.).
		rfCacheTTL:           60 * time.Second,
		collisionCacheTTL:    3600 * time.Second,
		invCooldown:          300 * time.Second,
		spIndex:              make(map[string]int, 4096),
		spTxIndex:            make(map[string][]*StoreTx, 4096),
		advertPubkeys:        make(map[string]int),
		clockSkew:            NewClockSkewEngine(),
		useResolvedPathIndex: true,
		areaNodeCache:        make(map[string]map[string]bool),
		areaNodeCacheTimes:   make(map[string]time.Time),
	}
	ps.initResolvedPathIndex()
	if cfg != nil {
		ps.retentionHours = cfg.RetentionHours
		ps.maxMemoryMB = cfg.MaxMemoryMB
		ps.maxResolvedPubkeyIndexEntries = cfg.MaxResolvedPubkeyIndexEntries
		if cfg.HotStartupHours > 0 {
			h := cfg.HotStartupHours
			if ps.retentionHours > 0 && h > ps.retentionHours {
				log.Printf("[store] warning: hotStartupHours (%g) > retentionHours (%g) — clamping", h, ps.retentionHours)
				h = ps.retentionHours
			}
			ps.hotStartupHours = h
		}
	}
	// Wire cacheTTL config values to server-side cache durations.
	if len(cacheTTLs) > 0 && cacheTTLs[0] != nil {
		ct := cacheTTLs[0]
		if v, ok := cacheTTLSec(ct, "analyticsHashSizes"); ok {
			ps.collisionCacheTTL = v
		}
		if v, ok := cacheTTLSec(ct, "analyticsRF"); ok {
			ps.rfCacheTTL = v
		}
		if v, ok := cacheTTLSec(ct, "invalidationDebounce"); ok {
			ps.invCooldown = v
		}
	}
	return ps
}

// newStoreObs builds the in-memory observation. pj is pathJSON(lo.Path),
// passed in because the caller already computed it for the dedup key.
func newStoreObs(lo database.Observation, txID int, pj string) *StoreObs {
	return &StoreObs{
		ID:             int(lo.ID),
		TransmissionID: txID,
		ObserverID:     lo.ObserverID,
		ObserverName:   lo.ObserverName,
		ObserverIATA:   lo.ObserverIATA,
		Direction:      lo.Direction,
		SNR:            lo.SNR,
		RSSI:           lo.RSSI,
		Score:          lo.Score,
		PathJSON:       pj,
		// obs.RawHex deliberately NOT stored: it duplicates tx.RawHex.
		Timestamp: lo.Timestamp.UTC(),
	}
}

// resolvedPubkeys is the resolved hop pubkeys, in hop order.
func resolvedPubkeys(hops []database.PathHop) []string {
	var out []string
	for _, h := range hops {
		if h.ResolvedPubkey != nil {
			out = append(out, *h.ResolvedPubkey)
		}
	}
	return out
}

func newStoreTx(lt database.LoadedTransmission) *StoreTx {
	return &StoreTx{
		ID:          int(lt.ID),
		RawHex:      lt.RawHex,
		Hash:        lt.Hash,
		FirstSeen:   lt.FirstSeen,
		LatestSeen:  lt.FirstSeen,
		RouteType:   lt.RouteType,
		PayloadType: lt.PayloadType,
		DecodedJSON: lt.DecodedJSON,
		ScopeName:   lt.ScopeName,
		obsKeys:     make(map[string]bool),
		observerSet: make(map[string]bool),
	}
}

// pathJSON is the hop prefixes as a JSON array, or "" when the observation
// has no hops (what a NULL path_json gave before).
func pathJSON(hops []database.PathHop) string {
	prefixes := make([]string, len(hops))
	for i, h := range hops {
		prefixes[i] = strings.ToUpper(h.Prefix)
	}
	return prefixesJSON(prefixes)
}

const bgChunkPage = 5000

func (s *PacketStore) loadChunk(ctx context.Context, from, to time.Time) error {
	fromStr := from.UTC().Format(time.RFC3339)
	toStr := to.UTC().Format(time.RFC3339)

	// Relay-hop fallback input, fetched once before the loop.
	s.mu.RLock()
	_, relayPM := s.getCachedNodesAndPM()
	s.mu.RUnlock()
	var coldLoadAmbiguousHopsSkipped int

	// Local data structures, built without holding the lock.
	localByHash := make(map[string]*StoreTx)
	localPackets := make([]*StoreTx, 0)
	localByTxID := make(map[int]*StoreTx)
	localByObsID := make(map[int]*StoreObs)
	localByObserver := make(map[string][]*StoreObs)
	// Issue #1558: persisted resolved-hop pubkeys per tx, accumulated and
	// deduplicated outside the lock; indexed via indexResolvedPathHops at merge.
	localResolvedPKsByTx := make(map[int][]string)
	localResolvedSeenByTx := make(map[int]map[string]bool)
	// Path fallback pubkeys (nothing persisted): indexed into byNode ONLY at
	// merge, kept separate because they must NOT enter the resolved-path /
	// path-hop indexes (#1352).
	localByNodePKsByTx := make(map[int][]string)
	localByNodeSeenByTx := make(map[int]map[string]bool)
	var localTotalObs int
	var localTrackedBytes int64
	var localMaxTxID int
	var localMaxObsID int

	cursor := firstChunkCursor
	for {
		page, err := s.db.ListTransmissionsChunk(ctx, database.ChunkQuery{
			AfterID: cursor, Since: &from, Until: &to, Limit: bgChunkPage,
		})
		if err != nil {
			return fmt.Errorf("transmissions: %w", err)
		}
		if len(page) == 0 {
			break
		}
		ids := make([]int64, len(page))
		for i, lt := range page {
			ids[i] = lt.ID
		}
		obsByTx, err := s.db.ListObservationsForTransmissions(ctx, ids)
		if err != nil {
			return fmt.Errorf("observations: %w", err)
		}

		for _, lt := range page {
			txID := int(lt.ID)
			tx := localByHash[lt.Hash]
			if tx == nil {
				tx = newStoreTx(lt)
				localByHash[lt.Hash] = tx
				localPackets = append(localPackets, tx)
				localByTxID[txID] = tx
				if txID > localMaxTxID {
					localMaxTxID = txID
				}
				localTrackedBytes += rechargeTx(tx)
			}

			for _, lo := range obsByTx[lt.ID] {
				pj := pathJSON(lo.Path)
				dk := lo.ObserverID + "|" + pj
				if tx.obsKeys[dk] {
					continue
				}
				obs := newStoreObs(lo, txID, pj)
				oid := obs.ID

				tx.Observations = append(tx.Observations, obs)
				tx.obsKeys[dk] = true
				if obs.ObserverID != "" && !tx.observerSet[obs.ObserverID] {
					tx.observerSet[obs.ObserverID] = true
					tx.UniqueObserverCount++
				}
				tx.ObservationCount++
				if obs.Timestamp.After(tx.LatestSeen) {
					tx.LatestSeen = obs.Timestamp
				}

				localByObsID[oid] = obs
				if oid > localMaxObsID {
					localMaxObsID = oid
				}
				if obs.ObserverID != "" {
					localByObserver[obs.ObserverID] = append(localByObserver[obs.ObserverID], obs)
				}
				localTotalObs++
				localTrackedBytes += estimateStoreObsBytes(obs)

				if pks := resolvedPubkeys(lo.Path); len(pks) > 0 {
					accumulateDedup(localResolvedPKsByTx, localResolvedSeenByTx, txID, pks)
				} else if relayPM != nil && pj != "" && pj != "[]" {
					// Nothing persisted: reconstruct relay hops from the path
					// so the older window keeps relay-node history across a
					// restart. byNode ONLY (#1352). PR #1643 R1 munger #1:
					// unique_prefix-only gate.
					rp := resolvePathForObsColdLoad(pj, obs.ObserverID, tx, relayPM, &coldLoadAmbiguousHopsSkipped)
					accumulateDedup(localByNodePKsByTx, localByNodeSeenByTx, txID, extractResolvedPubkeys(rp))
				}
			}
		}

		cursor = page[len(page)-1].ID
		if len(page) < bgChunkPage {
			break
		}
	}

	// Pick best observation for each local packet before merging.
	for _, tx := range localPackets {
		pickBestObservation(tx)
		localTrackedBytes += rechargeTx(tx)
	}

	if len(localPackets) == 0 {
		return nil
	}

	// PR #1187 r3 MUST-FIX 1: index<->slice consistency. Build the indexes
	// FIRST in bounded per-batch critical sections, then publish s.packets and
	// the counters in one final critical section, so the invariant "every tx
	// in s.packets is in s.byHash/s.byTxID" holds at every RLock instant.
	const mergeBatchSize = 500

	for batchStart := 0; batchStart < len(localPackets); batchStart += mergeBatchSize {
		batchEnd := batchStart + mergeBatchSize
		if batchEnd > len(localPackets) {
			batchEnd = len(localPackets)
		}
		batch := localPackets[batchStart:batchEnd]

		// Build the per-batch index views outside the lock so the critical
		// section below is O(batch), not O(total local).
		batchHashes := make(map[string]*StoreTx, len(batch))
		batchTxIDs := make(map[int]*StoreTx, len(batch))
		batchObsIDs := make(map[int]*StoreObs)
		batchByObserver := make(map[string][]*StoreObs)
		for _, tx := range batch {
			batchHashes[tx.Hash] = tx
			batchTxIDs[tx.ID] = tx
			for _, o := range tx.Observations {
				batchObsIDs[o.ID] = o
				if o.ObserverID != "" {
					batchByObserver[o.ObserverID] = append(batchByObserver[o.ObserverID], o)
				}
			}
		}

		s.mu.Lock()
		// Issue #1558: hopsSeen scratch map for indexResolvedPathHops,
		// allocated once per batch and cleared inside the helper on each call.
		hopsSeen := make(map[string]bool)
		newObsIDs := make(map[int]bool, len(batchObsIDs))
		for k := range batchObsIDs {
			if s.byObsID[k] == nil {
				newObsIDs[k] = true
			}
		}
		for k, v := range batchHashes {
			if s.byHash[k] == nil {
				s.byHash[k] = v
			}
		}
		for k, v := range batchTxIDs {
			if s.byTxID[k] == nil {
				s.byTxID[k] = v
			}
		}
		for k, v := range batchObsIDs {
			if newObsIDs[k] {
				s.byObsID[k] = v
			}
		}
		for observerID, obsList := range batchByObserver {
			for _, o := range obsList {
				if newObsIDs[o.ID] {
					s.byObserver[observerID] = append(s.byObserver[observerID], o)
				}
			}
		}
		for _, tx := range batch {
			s.indexByNode(tx)
			if tx.PayloadType != nil {
				pt := *tx.PayloadType
				s.byPayloadType[pt] = append(s.byPayloadType[pt], tx)
			}
			s.trackAdvertPubkey(tx)
			// Issue #1558: persisted resolved hops index under their relay
			// pubkeys; without this, per-node stats collapse after restart for
			// relay-heavy nodes. The pubkey union was built outside the lock.
			if pks := localResolvedPKsByTx[tx.ID]; len(pks) > 0 {
				s.indexResolvedPathHops(tx, pks, hopsSeen)
			}
			// Path fallback (nothing persisted): byNode ONLY (#1352).
			for _, pk := range localByNodePKsByTx[tx.ID] {
				s.addToByNode(tx, pk)
			}
		}
		s.mu.Unlock()
		runtime.Gosched()
	}

	// Final atomic step: every tx in localPackets is fully indexed, so publish
	// it into s.packets and bump counters in one short critical section.
	s.mu.Lock()
	s.packets = mergeChunkIntoPackets(localPackets, s.packets)
	s.totalObs += localTotalObs
	s.trackedBytes += localTrackedBytes
	if localMaxTxID > s.maxTxID {
		s.maxTxID = localMaxTxID
	}
	if localMaxObsID > s.maxObsID {
		s.maxObsID = localMaxObsID
	}
	s.mu.Unlock()

	log.Printf("[store] background chunk [%s, %s) merged: %d tx, %d obs", fromStr, toStr, len(localPackets), localTotalObs)
	if coldLoadAmbiguousHopsSkipped > 0 {
		log.Printf("[store] background chunk: skipped %d ambiguous-prefix relay hops (unique_prefix gate, PR #1643 R1)",
			coldLoadAmbiguousHopsSkipped)
	}
	return nil
}

// accumulateDedup appends pks to byTx[txID], deduplicating per-tx via the
// parallel seenByTx set. Used by loadChunk to build the per-tx relay-hop
// pubkey unions outside the merge critical section.
func accumulateDedup(byTx map[int][]string, seenByTx map[int]map[string]bool, txID int, pks []string) {
	if len(pks) == 0 {
		return
	}
	seen := seenByTx[txID]
	if seen == nil {
		seen = make(map[string]bool, len(pks))
		seenByTx[txID] = seen
	}
	for _, pk := range pks {
		if seen[pk] {
			continue
		}
		seen[pk] = true
		byTx[txID] = append(byTx[txID], pk)
	}
}

// loadBackgroundChunks fills the remaining retentionHours window by loading
// daily chunks from oldestLoaded back to the retention cutoff. After all
// chunks are merged it rebuilds analytics indexes once. Chunk errors are
// handled by advancing past the failed window so the loop always terminates.
func (s *PacketStore) loadBackgroundChunks(ctx context.Context) {
	// #1809 invariant: oldestLoaded MUST be set before the bg loader
	// runs whenever the in-memory store has packets. The original bug
	// was a parallel spawn that read a zero oldestLoaded and silently
	// bailed -> coverage gate trips -> backgroundLoadFailed=true. Encode
	// the precondition here so a future refactor that re-introduces
	// the race fails loudly instead of silently shipping the same
	// regression. Empty store + zero oldestLoaded is the legitimate
	// "empty DB" path and is allowed.
	s.mu.RLock()
	oldestAtEntry := s.oldestLoaded
	packetCountAtEntry := len(s.packets)
	s.mu.RUnlock()
	if oldestAtEntry.IsZero() && packetCountAtEntry > 0 {
		// adv #6 (PR #1811): log.Fatalf-style shutdown in prod, panic in
		// tests via the invariantViolation override.
		invariantViolation(fmt.Sprintf("loadBackgroundChunks: oldestLoaded is zero with %d packets in store — LoadChunked must run to completion first (#1809)", packetCountAtEntry))
		return
	}

	// Test-only entry hook. Production stores leave bgLoaderEntryHook
	// nil and pay the nil-check cost only.
	if s.bgLoaderEntryHook != nil {
		s.bgLoaderEntryHook()
	}

	if s.retentionHours <= 0 {
		s.backgroundLoadDone.Store(true)
		return
	}

	target := time.Now().UTC().Add(-time.Duration(s.retentionHours * float64(time.Hour)))
	// #1690: no `retentionHours - hotStartupHours <= 0` short-circuit; always
	// evaluate coverage against actual rows in the retention window.
	totalInDB := int64(-1)
	if n, err := s.db.CountTransmissionsSince(ctx, &target); err != nil {
		log.Printf("[store] background loader: count transmissions: %v", err)
	} else {
		totalInDB = n
	}

	var chunksLoaded float64
	var chunkErrors int
	totalHoursGap := s.retentionHours - s.hotStartupHours
	if totalHoursGap < 0 {
		totalHoursGap = 0
	}
	totalChunks := math.Ceil(totalHoursGap / 24)
	if totalChunks < 1 {
		totalChunks = 1
	}

	for {
		s.mu.RLock()
		chunkEnd := s.oldestLoaded
		s.mu.RUnlock()

		if chunkEnd.IsZero() || !chunkEnd.After(target) {
			break
		}

		chunkStart := chunkEnd.Add(-24 * time.Hour)
		if chunkStart.Before(target) {
			chunkStart = target
		}

		if err := s.loadChunk(ctx, chunkStart, chunkEnd); err != nil {
			chunkErrors++
			log.Printf("[store] background chunk [%s, %s) error: %v — advancing past it",
				chunkStart.Format(time.RFC3339), chunkEnd.Format(time.RFC3339), err)
		}
		// Always advance oldestLoaded to chunkStart so the loop terminates,
		// even when the chunk was empty (loadChunk skips the update when 0 packets).
		s.mu.Lock()
		if s.oldestLoaded.IsZero() || s.oldestLoaded.After(chunkStart) {
			s.oldestLoaded = chunkStart
		}
		s.mu.Unlock()

		chunksLoaded++
		if totalChunks > 0 {
			pct := int64(chunksLoaded / totalChunks * 100)
			if pct > 100 {
				pct = 100
			}
			s.backgroundLoadProgress.Store(pct)
		}

		// Yield between chunks so ingest goroutines can acquire s.mu.Lock()
		// without being starved. Background fill is best-effort and queries
		// fall back to the database while it runs.
		runtime.Gosched()
	}

	// Rebuild analytics indexes once after all chunks are merged.
	s.mu.Lock()
	s.buildSubpathIndex()
	s.buildPathHopIndex()
	// Distance index is lazy (#1011) — built on first
	// /api/analytics/distance request. If a previous request already
	// triggered the build, invalidate the gate so the next request rebuilds
	// against the fuller dataset.
	s.distLazyMu.Lock()
	s.distLazyBuilt = false
	s.distLazyOnce = sync.Once{}
	s.distLazyMu.Unlock()
	s.mu.Unlock()
	// #1008 review m3: flip the ready flags after the synchronous rebuild
	// for symmetry with startBackgroundIndexBuilds.
	s.markIndexesReadySync()

	// #1690: compute actual coverage ratio and gate backgroundLoadDone on it.
	// Threshold 0.90 mirrors the issue's acceptance criterion.
	s.mu.RLock()
	loadedCount := int64(len(s.packets))
	s.mu.RUnlock()

	var ratio float64
	if totalInDB > 0 {
		ratio = float64(loadedCount) / float64(totalInDB)
	} else if totalInDB == 0 {
		// Empty DB: nothing to load, treat as complete.
		ratio = 1.0
	}
	s.bgErrMu.Lock()
	s.loadCoverageRatio = ratio
	if totalInDB > 0 && ratio < 0.90 {
		s.backgroundLoadFailed.Store(true)
		s.backgroundLoadErr = fmt.Sprintf("loaded %.1f%% of %d rows (%d in memory)",
			ratio*100, totalInDB, loadedCount)
	} else if chunkErrors > 0 {
		s.backgroundLoadFailed.Store(true)
		s.backgroundLoadErr = fmt.Sprintf("%d chunk error(s) during background load", chunkErrors)
	} else {
		s.backgroundLoadErr = ""
	}
	bgErr := s.backgroundLoadErr
	s.bgErrMu.Unlock()

	// Only flip done=true when the store truly reflects the database
	// (the analytics warm-up gate, #1659, reads this).
	if !s.backgroundLoadFailed.Load() {
		s.backgroundLoadDone.Store(true)
	}
	s.backgroundLoadProgress.Store(100)

	s.mu.RLock()
	totalPkts := len(s.packets)
	oldest := s.oldestLoaded.Format(time.RFC3339)
	s.mu.RUnlock()
	if bgErr != "" {
		log.Printf("[store] background load FAILED (%s): %d packets in memory, oldestLoaded=%s", bgErr, totalPkts, oldest)
	} else {
		log.Printf("[store] background load complete: %d/%d packets in memory (coverage=%.1f%%), oldestLoaded=%s",
			totalPkts, totalInDB, ratio*100, oldest)
	}
}

// pickBestObservation selects the observation with the longest path
// and sets it as the transmission's display observation.
func pickBestObservation(tx *StoreTx) {
	if len(tx.Observations) == 0 {
		return
	}
	best := tx.Observations[0]
	bestLen := pathLen(best.PathJSON)
	for _, obs := range tx.Observations[1:] {
		l := pathLen(obs.PathJSON)
		if l > bestLen {
			best = obs
			bestLen = l
		}
	}
	tx.ObserverID = best.ObserverID
	tx.ObserverName = best.ObserverName
	tx.ObserverIATA = best.ObserverIATA
	tx.SNR = best.SNR
	tx.RSSI = best.RSSI
	tx.PathJSON = best.PathJSON
	tx.Direction = best.Direction
	tx.pathParsed = false // invalidate cached parsed path
}

// rechargeTx re-estimates tx and returns the change since it was last charged,
// for the caller to add to trackedBytes. A tx is first charged when it is
// created, before its observations are merged, so its path costs (byPathHop,
// spTxIndex) are unknown then: call this again once pickBestObservation has
// set the path. Eviction subtracts accountedBytes, so the running total stays
// consistent however often the path changed in between.
func rechargeTx(tx *StoreTx) int64 {
	est := estimateStoreTxBytes(tx)
	d := est - tx.accountedBytes
	tx.accountedBytes = est
	return d
}

func pathLen(pathJSON string) int {
	if pathJSON == "" {
		return 0
	}
	var hops []interface{}
	if json.Unmarshal([]byte(pathJSON), &hops) != nil {
		return 0
	}
	return len(hops)
}

// indexResolvedPathHops indexes a transmission under every relay-hop pubkey
// extracted from an observation's resolved_path, and refreshes the dependent
// resolved-pubkey + path-hop indexes. This is the single point of truth for
// the "feed decode-window consumers for resolved-path pubkeys" contract that
// must hold across every code path that materializes a transmission into the
// in-memory store: initial Load (cmd/server/store.go ~783-799), background
// chunk loads (loadChunk, see issue #1558), MQTT ingest (~2293-2306), and
// late-arriving-observation ingest (~2630-2643). Duplicating these three
// calls inline let loadChunk silently drop the branch and collapse per-node
// Home-page stats after restart for relay-heavy nodes — see #1558.
//
// Caller contract:
//   - Must hold s.mu write lock (addToByNode / addToResolvedPubkeyIndex /
//     addResolvedPubkeysToPathHopIndex all mutate store state).
//   - pks should be the output of extractResolvedPubkeys (no nils, no
//     empties); the helper is a no-op when pks is empty.
//   - hopsSeen is a reusable scratch map; addResolvedPubkeysToPathHopIndex
//     clear()s it on entry.
func (s *PacketStore) indexResolvedPathHops(tx *StoreTx, pks []string, hopsSeen map[string]bool) {
	if len(pks) == 0 {
		return
	}
	for _, pk := range pks {
		s.addToByNode(tx, pk)
	}
	s.addResolvedPubkeysToPathHopIndex(tx, pks, hopsSeen)
	s.addToResolvedPubkeyIndex(tx.ID, pks)
}

// indexByNode extracts pubkeys from decoded_json and indexes the transmission.
// indexByNode indexes a transmission under all pubkeys found in its decoded
// JSON. Resolved path pubkeys are handled separately via the decode-window.
// Returns true if any genuinely new node was discovered.
func (s *PacketStore) indexByNode(tx *StoreTx) bool {
	// Track which pubkeys have been indexed for this packet to avoid duplicates.
	indexed := make(map[string]bool)
	foundNew := false

	// Index by decoded JSON fields (pubKey, destPubKey, srcPubKey).
	if tx.DecodedJSON != "" && strings.Contains(tx.DecodedJSON, "ubKey") {
		if decoded := tx.ParsedDecoded(); decoded != nil {
			for _, field := range []string{"pubKey", "destPubKey", "srcPubKey"} {
				if v, ok := decoded[field].(string); ok && v != "" {
					if s.addToByNode(tx, v) {
						foundNew = true
					}
					indexed[v] = true
				}
			}
		}
	}

	return foundNew
}

// addToByNode adds tx to byNode[pubkey] with dedup via nodeHashes.
// Returns true if this is a genuinely new node (pubkey not seen before).
func (s *PacketStore) addToByNode(tx *StoreTx, pubkey string) bool {
	isNew := s.nodeHashes[pubkey] == nil
	if isNew {
		s.nodeHashes[pubkey] = make(map[string]bool)
	}
	if s.nodeHashes[pubkey][tx.Hash] {
		return false
	}
	s.nodeHashes[pubkey][tx.Hash] = true
	s.byNode[pubkey] = append(s.byNode[pubkey], tx)
	return isNew
}

// trackAdvertPubkey increments the advertPubkeys refcount for ADVERT packets.
// Must be called under s.mu write lock.
func (s *PacketStore) trackAdvertPubkey(tx *StoreTx) {
	if tx.PayloadType == nil || *tx.PayloadType != types.PayloadADVERT || tx.DecodedJSON == "" {
		return
	}
	d := tx.ParsedDecoded()
	if d == nil {
		return
	}
	pk := ""
	if v, ok := d["pubKey"].(string); ok {
		pk = v
	} else if v, ok := d["public_key"].(string); ok {
		pk = v
	}
	if pk != "" {
		s.advertPubkeys[pk]++
	}
}

// untrackAdvertPubkey decrements the advertPubkeys refcount for ADVERT packets.
// Must be called under s.mu write lock.
func (s *PacketStore) untrackAdvertPubkey(tx *StoreTx) {
	if tx.PayloadType == nil || *tx.PayloadType != types.PayloadADVERT || tx.DecodedJSON == "" {
		return
	}
	d := tx.ParsedDecoded()
	if d == nil {
		return
	}
	pk := ""
	if v, ok := d["pubKey"].(string); ok {
		pk = v
	} else if v, ok := d["public_key"].(string); ok {
		pk = v
	}
	if pk != "" {
		if s.advertPubkeys[pk] <= 1 {
			delete(s.advertPubkeys, pk)
		} else {
			s.advertPubkeys[pk]--
		}
	}
}

// queryPredates reports whether any time bound in q reaches before oldest,
// the start of the in-memory window. The old code compared the raw since/until
// strings with oldestLoaded; the typed bounds replace that.
func queryPredates(q database.PacketQuery, oldest time.Time) bool {
	for _, b := range []*time.Time{q.ObsSince, q.FirstSeenAfter, q.ObsUntil, q.FirstSeenBefore} {
		if b != nil && b.Before(oldest) {
			return true
		}
	}
	return false
}

func packetResultFromPackets(packets []database.Packet, total int64) *api.PacketListResponse {
	out := make([]api.Packet, len(packets))
	for i, p := range packets {
		out[i] = dbPacket(p)
	}
	return &api.PacketListResponse{Packets: out, Total: int(total)}
}

// QueryPackets returns filtered, paginated packets from memory.
// QueryPackets returns filtered, paginated packets from memory.
func (s *PacketStore) QueryPackets(ctx context.Context, q database.PacketQuery) (*api.PacketListResponse, error) {
	// SQL fallback: if the query window predates the in-memory window, delegate
	// to the DB layer which covers the full retention period.
	s.mu.RLock()
	oldest := s.oldestLoaded
	s.mu.RUnlock()
	if !oldest.IsZero() && queryPredates(q, oldest) {
		dbCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		packets, total, err := s.db.QueryPackets(dbCtx, q)
		cancel()
		if err == nil {
			return packetResultFromPackets(packets, total), nil
		}
		log.Printf("[store] QueryPackets SQL fallback failed: %v — using in-memory", err)
	}
	atomic.AddInt64(&s.queryCount, 1)

	if q.Limit <= 0 {
		q.Limit = 50
	}
	if q.Order == "" {
		q.Order = "DESC"
	}

	// Phase 1 (lock): filter, sort and pick the page.
	s.mu.RLock()
	results := s.filterPackets(ctx, q)
	total := len(results)

	// #1345: order by ingest id, not insertion-into-s.packets order (see the
	// earlier comment): sort so "page 0" is the most-recently-ingested.
	sortedByID := make([]*StoreTx, len(results))
	copy(sortedByID, results)
	sort.Slice(sortedByID, func(i, j int) bool {
		return sortedByID[i].ID < sortedByID[j].ID
	})
	results = sortedByID

	start := q.Offset
	if start >= total {
		s.mu.RUnlock()
		return &api.PacketListResponse{Packets: []api.Packet{}, Total: total}, nil
	}
	pageSize := q.Limit
	if start+pageSize > total {
		pageSize = total - start
	}

	// results is oldest-first (ASC). For DESC (default) read backwards from the tail.
	page := make([]*StoreTx, 0, pageSize)
	if q.Order == "ASC" {
		page = append(page, results[start:start+pageSize]...)
	} else {
		endIdx := total - start
		startIdx := endIdx - pageSize
		if startIdx < 0 {
			startIdx = 0
		}
		for i := endIdx - 1; i >= startIdx; i-- {
			page = append(page, results[i])
		}
	}
	s.mu.RUnlock()

	// Phases 2 and 3: resolved paths, then the responses.
	packets, err := s.responsesFor(ctx, page, q.ExpandObservations)
	if err != nil {
		return nil, err
	}
	return &api.PacketListResponse{Packets: packets, Total: total}, nil
}

// QueryGroupedPackets returns transmissions grouped by hash (already 1:1).
func (s *PacketStore) QueryGroupedPackets(ctx context.Context, q database.PacketQuery) (*api.GroupedPacketListResponse, error) {
	atomic.AddInt64(&s.queryCount, 1)

	if q.Limit <= 0 {
		q.Limit = 50
	}

	// Cache key covers all filter dimensions. Empty key = no filters.
	cacheKey := strings.Join([]string{
		timeKey(q.ObsSince), timeKey(q.ObsUntil), q.Region, q.Area, q.Node, q.Hash,
		strings.Join(q.Observers, ","), q.Channel, strconv.FormatBool(q.ChannelEncrypted),
	}, "|")
	if q.Type != nil {
		cacheKey += fmt.Sprintf("|t%d", *q.Type)
	}
	if q.Route != nil {
		cacheKey += fmt.Sprintf("|r%d", *q.Route)
	}

	// Return cached sorted list if still fresh (3s TTL)
	s.groupedCacheMu.Lock()
	if s.groupedCacheTxs != nil && s.groupedCacheKey == cacheKey && time.Now().Before(s.groupedCacheExp) {
		cachedTxs := s.groupedCacheTxs
		cachedTotal := s.groupedCacheTotal
		s.groupedCacheMu.Unlock()
		return groupedTxsToPage(cachedTxs, cachedTotal, q.Offset, q.Limit), nil
	}
	s.groupedCacheMu.Unlock()

	// Collect StoreTx pointers under read lock; sort outside it.
	s.mu.RLock()
	results := s.filterPackets(ctx, q)
	txs := make([]*StoreTx, len(results))
	copy(txs, results)
	s.mu.RUnlock()

	total := len(txs)

	// Full sort by LatestSeen DESC so the cached slice supports all page offsets.
	sort.Slice(txs, func(i, j int) bool {
		return txs[i].LatestSeen.After(txs[j].LatestSeen)
	})

	// Cache the sorted StoreTx slice (not response structs): lightweight and
	// reusable for any page.
	s.groupedCacheMu.Lock()
	s.groupedCacheTxs = txs
	s.groupedCacheTotal = total
	s.groupedCacheKey = cacheKey
	s.groupedCacheExp = time.Now().Add(3 * time.Second)
	s.groupedCacheMu.Unlock()

	return groupedTxsToPage(txs, total, q.Offset, q.Limit), nil
}

// timeKey is a stable cache-key form of an optional time bound.
func timeKey(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// groupedTxsToPage builds responses only for the requested page of sorted StoreTx pointers.
// This avoids allocating a response for all 30K+ transmissions when only 50 are needed.
func groupedTxsToPage(txs []*StoreTx, total, offset, limit int) *api.GroupedPacketListResponse {
	if offset >= len(txs) {
		return &api.GroupedPacketListResponse{Packets: []api.GroupedPacket{}, Total: total}
	}
	end := offset + limit
	if end > len(txs) {
		end = len(txs)
	}
	page := txs[offset:end]

	packets := make([]api.GroupedPacket, len(page))
	for i, tx := range page {
		packets[i] = groupedPacket(tx)
	}
	return &api.GroupedPacketListResponse{Packets: packets, Total: total}
}

// storeTxDistinctIatas (#1189 R2) returns a sorted, deduped list of observer
// IATA codes for a StoreTx, excluding empty values. Returns an empty
// (non-nil) []string when the tx has no IATA'd observations so JSON
// serialization stays consistent across the in-memory store and SQL
// fallback paths (db.go's parseDistinctIatasCSV does the same).
func storeTxDistinctIatas(tx *StoreTx) []string {
	if tx == nil {
		return []string{}
	}
	seen := make(map[string]bool)
	// Include the header observer's IATA (some hot-path StoreTx records the
	// chosen observer fields directly without re-populating Observations).
	if tx.ObserverIATA != "" {
		seen[tx.ObserverIATA] = true
	}
	for _, o := range tx.Observations {
		if o != nil && o.ObserverIATA != "" {
			seen[o.ObserverIATA] = true
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// GetStoreStats returns aggregate counts (packet data from memory, node/observer from the database).
func (s *PacketStore) GetStoreStats(ctx context.Context) (*Stats, error) {
	s.mu.RLock()
	txCount := len(s.packets)
	obsCount := s.totalObs
	s.mu.RUnlock()

	st := &Stats{
		TotalTransmissions: txCount,
		TotalPackets:       txCount,
		TotalObservations:  obsCount,
	}

	now := time.Now()
	activeSince := now.Add(-7 * 24 * time.Hour)

	// Observation counts (#1910). Three cases, and only the cold one makes the
	// caller wait:
	//   fresh  — serve the cache.
	//   stale  — serve the cache anyway and refresh once in the background. A
	//            count that is half a minute old is not worth a page that hangs.
	//   cold   — compute, but under singleflight, so N concurrent callers share
	//            one scan instead of each running their own.
	var obsFromCache bool
	s.statsCacheMu.Lock()
	haveObsCache := !s.statsCacheTime.IsZero()
	obsCacheFresh := haveObsCache && time.Since(s.statsCacheTime) < 30*time.Second
	if haveObsCache {
		st.PacketsLastHour = s.statsLastHour
		st.PacketsLast24h = s.statsLast24h
	}
	s.statsCacheMu.Unlock()

	switch {
	case obsCacheFresh:
		obsFromCache = true
	case haveObsCache:
		obsFromCache = true
		// The refresh outlives this request, so detach it from ctx's
		// cancellation: a cancelled request must not abort the cache refresh.
		go func() {
			bg, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			_, _, _ = s.refreshObsCounts(bg, now)
		}()
	}

	// Run node/observer counts and (if cold) observation counts concurrently.
	var wg sync.WaitGroup
	var nodeErr, obsErr error

	wg.Add(1)
	if !obsFromCache {
		wg.Add(1)
	}
	go func() {
		defer wg.Done()
		c, err := s.db.GetEntityCounts(ctx, activeSince)
		if err != nil {
			nodeErr = err
			return
		}
		st.TotalNodes = c.ActiveNodes
		st.TotalNodesAllTime = c.AllNodes
		st.TotalObservers = c.Observers
	}()
	if !obsFromCache {
		go func() {
			defer wg.Done()
			lastHour, last24h, err := s.refreshObsCounts(ctx, now)
			obsErr = err
			if err == nil {
				st.PacketsLastHour = lastHour
				st.PacketsLast24h = last24h
			}
		}()
	}
	wg.Wait()

	if nodeErr != nil {
		return st, nodeErr
	}
	if obsErr != nil {
		return st, obsErr
	}

	return st, nil
}

// refreshObsCounts runs the observations range scan for the last hour and the
// last 24h, and writes the result into the stats cache.
//
// #1910: wrapped in singleflight. The cache check in GetStoreStats releases
// statsCacheMu before doing the work, so every request that arrived while the
// cache was expired used to run this scan itself. With SetMaxOpenConns(4) and a
// page that fires stats, observers, nodes, channels and clock-skew at once, that
// turned one scan into a queue of them: /stats was measured at 10-17s under
// mixed load while staying under 70ms when it was the only endpoint being hit.
// Now the second and later callers wait for the first one's result.
func (s *PacketStore) refreshObsCounts(ctx context.Context, now time.Time) (int, int, error) {
	v, err, _ := s.statsSF.Do("obs-counts", func() (interface{}, error) {
		// This runs once for every caller waiting in the group, so it must not
		// be tied to the first caller's context: that caller disconnecting
		// would fail everyone waiting.
		qctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()

		c, qErr := s.db.CountRecentObservations(qctx, now)
		if qErr != nil {
			return nil, qErr
		}
		s.statsCacheMu.Lock()
		s.statsLastHour = c.LastHour
		s.statsLast24h = c.Last24h
		s.statsCacheTime = time.Now()
		s.statsCacheMu.Unlock()
		return [2]int{c.LastHour, c.Last24h}, nil
	})
	if err != nil {
		return 0, 0, err
	}
	pair := v.([2]int)
	return pair[0], pair[1], nil
}

// GetPerfStoreStats returns packet store statistics for /api/perf.
func (s *PacketStore) GetPerfStoreStats() map[string]interface{} {
	s.mu.RLock()
	totalLoaded := len(s.packets)
	totalObs := s.totalObs
	hashIdx := len(s.byHash)
	txIdx := len(s.byTxID)
	obsIdx := len(s.byObsID)
	observerIdx := len(s.byObserver)
	nodeIdx := len(s.byNode)
	pathHopIdx := len(s.byPathHop)
	ptIdx := len(s.byPayloadType)
	oldestLoaded := s.oldestLoaded
	retentionHours := s.retentionHours
	maxMemoryMB := s.maxMemoryMB
	hotStartupHours := s.hotStartupHours

	// Distinct advert pubkey count — precomputed incrementally (see trackAdvertPubkey).
	advertByObsCount := len(s.advertPubkeys)
	s.mu.RUnlock()

	estimatedMB := math.Round(s.estimatedMemoryMB()*10) / 10
	trackedMB := math.Round(s.trackedMemoryMB()*10) / 10

	evicted := atomic.LoadInt64(&s.evicted)

	return map[string]interface{}{
		"totalLoaded":            totalLoaded,
		"totalObservations":      totalObs,
		"evicted":                evicted,
		"inserts":                atomic.LoadInt64(&s.insertCount),
		"queries":                atomic.LoadInt64(&s.queryCount),
		"inMemory":               totalLoaded,
		"sqliteOnly":             false,
		"retentionHours":         retentionHours,
		"maxMemoryMB":            maxMemoryMB,
		"oldestLoaded":           oldestLoaded,
		"estimatedMB":            estimatedMB,
		"trackedMB":              trackedMB,
		"hotStartupHours":        hotStartupHours,
		"backgroundLoadComplete": s.backgroundLoadDone.Load(),
		"backgroundLoadFailed":   s.backgroundLoadFailed.Load(),
		"backgroundLoadProgress": s.backgroundLoadProgress.Load(),
		"indexes": map[string]interface{}{
			"byHash":           hashIdx,
			"byTxID":           txIdx,
			"byObsID":          obsIdx,
			"byObserver":       observerIdx,
			"byNode":           nodeIdx,
			"byPathHop":        pathHopIdx,
			"byPayloadType":    ptIdx,
			"advertByObserver": advertByObsCount,
		},
	}
}

// GetCacheStats returns RF cache hit/miss statistics.
func (s *PacketStore) GetCacheStats() map[string]interface{} {
	s.cacheMu.Lock()
	size := len(s.rfCache) + len(s.topoCache) + len(s.hashCache) + len(s.chanCache) + len(s.distCache) + len(s.subpathCache)
	hits := s.cacheHits
	misses := s.cacheMisses
	s.cacheMu.Unlock()

	var hitRate float64
	if hits+misses > 0 {
		hitRate = math.Round(float64(hits)/float64(hits+misses)*1000) / 10
	}

	return map[string]interface{}{
		"size":       size,
		"hits":       hits,
		"misses":     misses,
		"staleHits":  0,
		"recomputes": misses,
		"hitRate":    hitRate,
	}
}

// GetCacheStatsTyped returns cache stats as a typed struct.
func (s *PacketStore) GetCacheStatsTyped() CacheStats {
	s.cacheMu.Lock()
	size := len(s.rfCache) + len(s.topoCache) + len(s.hashCache) + len(s.chanCache) + len(s.distCache) + len(s.subpathCache)
	hits := s.cacheHits
	misses := s.cacheMisses
	s.cacheMu.Unlock()

	var hitRate float64
	if hits+misses > 0 {
		hitRate = math.Round(float64(hits)/float64(hits+misses)*1000) / 10
	}

	return CacheStats{
		Entries:    size,
		Hits:       hits,
		Misses:     misses,
		StaleHits:  0,
		Recomputes: misses,
		HitRate:    hitRate,
	}
}

// cacheInvalidation flags indicate what kind of data changed during ingestion.
// Used by invalidateCachesFor to selectively clear only affected caches.
type cacheInvalidation struct {
	hasNewObservations  bool // new SNR/RSSI data → rfCache
	hasNewPaths         bool // new/changed path data → topoCache, distCache, subpathCache
	hasNewTransmissions bool // new transmissions → hashCache
	hasNewNodes         bool // genuinely new node pubkey discovered → collisionCache
	hasChannelData      bool // new GRP_TXT (payload_type 5) → chanCache
	eviction            bool // data removed → all caches
}

// invalidateCachesFor selectively clears only the analytics caches affected
// by the kind of data that changed. To prevent continuous ingestion from
// defeating caching entirely (issue #533), invalidation is rate-limited:
// if called within invCooldown of the last invalidation, the flags are
// accumulated in pendingInv and applied on the next call after cooldown.
func (s *PacketStore) invalidateCachesFor(inv cacheInvalidation) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()

	// Eviction bypasses rate-limiting — data was removed, caches must clear.
	if inv.eviction {
		s.rfCache = make(map[string]*cachedResult)
		s.topoCache = make(map[string]*cachedResult)
		s.hashCache = make(map[string]*cachedHashSizes)
		s.collisionCache = make(map[string]*cachedHashCollisions)
		s.chanCache = make(map[string]*cachedResult)
		s.distCache = make(map[string]*cachedResult)
		s.subpathCache = make(map[string]*cachedResult)
		s.retransCache = nil
		s.channelsCacheMu.Lock()
		s.channelsCacheRes = nil
		s.channelsCacheMu.Unlock()
		s.lastInvalidated = time.Now()
		s.pendingInv = nil
		return
	}

	now := time.Now()
	if now.Sub(s.lastInvalidated) < s.invCooldown {
		// Within cooldown — accumulate dirty flags
		if s.pendingInv == nil {
			s.pendingInv = &cacheInvalidation{}
		}
		s.pendingInv.hasNewObservations = s.pendingInv.hasNewObservations || inv.hasNewObservations
		s.pendingInv.hasNewPaths = s.pendingInv.hasNewPaths || inv.hasNewPaths
		s.pendingInv.hasNewTransmissions = s.pendingInv.hasNewTransmissions || inv.hasNewTransmissions
		s.pendingInv.hasNewNodes = s.pendingInv.hasNewNodes || inv.hasNewNodes
		s.pendingInv.hasChannelData = s.pendingInv.hasChannelData || inv.hasChannelData
		return
	}

	// Cooldown expired — merge any pending flags and apply
	if s.pendingInv != nil {
		inv.hasNewObservations = inv.hasNewObservations || s.pendingInv.hasNewObservations
		inv.hasNewPaths = inv.hasNewPaths || s.pendingInv.hasNewPaths
		inv.hasNewTransmissions = inv.hasNewTransmissions || s.pendingInv.hasNewTransmissions
		inv.hasNewNodes = inv.hasNewNodes || s.pendingInv.hasNewNodes
		inv.hasChannelData = inv.hasChannelData || s.pendingInv.hasChannelData
		s.pendingInv = nil
	}

	s.applyCacheInvalidation(inv)
	s.lastInvalidated = now
}

// applyCacheInvalidation performs the actual cache clearing. Must be called
// with cacheMu held.
func (s *PacketStore) applyCacheInvalidation(inv cacheInvalidation) {
	if inv.hasNewObservations {
		s.rfCache = make(map[string]*cachedResult)
	}
	if inv.hasNewPaths {
		s.topoCache = make(map[string]*cachedResult)
		s.distCache = make(map[string]*cachedResult)
		s.subpathCache = make(map[string]*cachedResult)
		s.retransCache = nil
	}
	if inv.hasNewTransmissions {
		s.hashCache = make(map[string]*cachedHashSizes)
	}
	if inv.hasNewNodes {
		s.collisionCache = make(map[string]*cachedHashCollisions)
	}
	if inv.hasChannelData {
		s.chanCache = make(map[string]*cachedResult)
		s.channelsCacheMu.Lock()
		s.channelsCacheRes = nil
		s.channelsCacheMu.Unlock()
	}
}

// GetPerfStoreStatsTyped returns packet store stats as a typed struct.
func (s *PacketStore) GetPerfStoreStatsTyped() PerfPacketStoreStats {
	s.mu.RLock()
	totalLoaded := len(s.packets)
	totalObs := s.totalObs
	hashIdx := len(s.byHash)
	observerIdx := len(s.byObserver)
	nodeIdx := len(s.byNode)
	oldestLoaded := s.oldestLoaded
	retentionHours := s.retentionHours

	advertByObsCount := len(s.advertPubkeys)
	s.mu.RUnlock()

	s.bgErrMu.RLock()
	loadCov := s.loadCoverageRatio
	bgErr := s.backgroundLoadErr
	s.bgErrMu.RUnlock()

	estimatedMB := math.Round(s.estimatedMemoryMB()*10) / 10
	trackedMB := math.Round(s.trackedMemoryMB()*10) / 10

	var avgBytesPerPacket int64
	if totalLoaded > 0 {
		avgBytesPerPacket = s.trackedBytes / int64(totalLoaded)
	}

	return PerfPacketStoreStats{
		TotalLoaded:       totalLoaded,
		TotalObservations: totalObs,
		Evicted:           int(atomic.LoadInt64(&s.evicted)),
		Inserts:           atomic.LoadInt64(&s.insertCount),
		Queries:           atomic.LoadInt64(&s.queryCount),
		InMemory:          totalLoaded,
		SqliteOnly:        false,
		MaxPackets:        2386092,
		EstimatedMB:       estimatedMB,
		TrackedMB:         trackedMB,
		AvgBytesPerPacket: avgBytesPerPacket,
		MaxMB:             s.maxMemoryMB,
		Indexes: PacketStoreIndexes{
			ByHash:           hashIdx,
			ByObserver:       observerIdx,
			ByNode:           nodeIdx,
			AdvertByObserver: advertByObsCount,
		},
		HotStartupHours:        s.hotStartupHours,
		BackgroundLoadComplete: s.backgroundLoadDone.Load(),
		BackgroundLoadFailed:   s.backgroundLoadFailed.Load(),
		BackgroundLoadProgress: s.backgroundLoadProgress.Load(),
		BackgroundLoadError:    bgErr,
		RetentionHours:         retentionHours,
		OldestLoaded:           oldestLoaded,
		LoadCoverageRatio:      loadCov,
	}
}

// GetStoreMemoryBreakdown walks the whole store ONCE (under RLock) and returns
// the flood-forward (route_type 0/1) share of stored transmissions plus a
// per-component breakdown of the string bytes held in memory. O(tx + obs) — it
// touches every observation, so it is opt-in (/api/perf?mem=1) and must not be
// on the hot path.
//
// LOCK CONTENTION: this holds s.mu.RLock for the entire scan of the store. On a
// large store (millions of observations) the walk takes long enough to stall
// concurrent writers (ingest/eviction take the write lock) for the duration.
// Operators should poll this infrequently (e.g. on demand, not on a tight
// dashboard refresh) — do not wire it into a high-frequency scrape.
//
// The per-component *MB figures count string CONTENT plus one Go string header
// per field. For fields that are inline struct members (RawHex, DecodedJSON,
// PathJSON, observer strings) that header is ALSO part of storeTxBaseBytes /
// storeObsBaseBytes, so the per-component totals are a deliberate UPPER BOUND,
// not additive with TotalTxEstimatedMB. struct/index/map overhead, the neighbor
// graph and analytics caches are excluded entirely — which is why the total
// here sits below goHeapInuseMB.
func (s *PacketStore) GetStoreMemoryBreakdown() *StoreMemoryBreakdown {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := &StoreMemoryBreakdown{}
	var txRawHex, txDecodedJSON, txPathJSON int64
	var obsPathJSON, obsStrings int64
	var floodTxBytes, totalTxBytes int64

	for _, tx := range s.packets {
		if tx == nil {
			continue
		}
		out.TotalTx++
		isFlood := tx.RouteType != nil && (*tx.RouteType == 0 || *tx.RouteType == 1)
		if isFlood {
			out.FloodTx++
		}
		txRawHex += int64(len(tx.RawHex) + strHdr)
		txDecodedJSON += int64(len(tx.DecodedJSON) + strHdr)
		txPathJSON += int64(len(tx.PathJSON) + strHdr)
		b := estimateStoreTxBytes(tx)
		totalTxBytes += b
		if isFlood {
			floodTxBytes += b
		}
		for _, o := range tx.Observations {
			if o == nil {
				continue
			}
			out.Observations++
			obsPathJSON += int64(len(o.PathJSON) + strHdr)
			obsStrings += int64(len(o.ObserverID)+len(o.ObserverName)+len(o.ObserverIATA)+len(o.Direction)) + 4*strHdr
		}
	}

	// 2 decimal places so sub-0.1MB components don't round away to 0.
	mb := func(b int64) float64 { return math.Round(float64(b)/1048576*100) / 100 }
	out.TxRawHexMB = mb(txRawHex)
	out.TxDecodedJsonMB = mb(txDecodedJSON)
	out.TxPathJsonMB = mb(txPathJSON)
	out.ObsPathJsonMB = mb(obsPathJSON)
	out.ObsStringsMB = mb(obsStrings)
	out.FloodTxEstimatedMB = mb(floodTxBytes)
	out.TotalTxEstimatedMB = mb(totalTxBytes)
	if out.TotalTx > 0 {
		out.FloodTxSharePct = math.Round(float64(out.FloodTx)/float64(out.TotalTx)*1000) / 10
		out.ObsPerTx = math.Round(float64(out.Observations)/float64(out.TotalTx)*10) / 10
	}
	return out
}

// pageIDs lists the transmission ids of page and, when expand is set, the ids
// of their observations. Call with s.mu held.
func pageIDs(page []*StoreTx, expand bool) (txIDs, obsIDs []int) {
	txIDs = make([]int, len(page))
	for i, tx := range page {
		txIDs[i] = tx.ID
		if expand {
			for _, o := range tx.Observations {
				obsIDs = append(obsIDs, o.ID)
			}
		}
	}
	return txIDs, obsIDs
}

// responsesFor builds the responses for page. The caller must NOT hold s.mu:
// it takes the read lock to list ids, fetches the resolved paths with no lock
// held, then takes the read lock again to read the packets.
func (s *PacketStore) responsesFor(ctx context.Context, page []*StoreTx, expand bool) ([]api.Packet, error) {
	s.mu.RLock()
	txIDs, obsIDs := pageIDs(page, expand)
	s.mu.RUnlock()

	paths, err := s.db.ResolvedPaths(ctx, txIDs, obsIDs)
	if err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]api.Packet, len(page))
	for i, tx := range page {
		out[i] = txPacket(tx, paths, expand)
	}
	return out, nil
}

// txPacketByLookup resolves a transmission under s.mu and returns it with its
// observations. The resolved paths are fetched between two short read-lock
// phases, so no database read happens under s.mu. find must be called with
// s.mu held.
func (s *PacketStore) txPacketByLookup(ctx context.Context, find func() *StoreTx) (*api.Packet, error) {
	// Phase 1 (lock): find the packet and note which ids need resolved paths.
	s.mu.RLock()
	tx := find()
	if tx == nil {
		s.mu.RUnlock()
		return nil, nil
	}
	txID := tx.ID
	obsIDs := make([]int, len(tx.Observations))
	for i, o := range tx.Observations {
		obsIDs[i] = o.ID
	}
	s.mu.RUnlock()

	// Phase 2 (no lock): fetch the resolved paths.
	paths, err := s.db.ResolvedPaths(ctx, []int{txID}, obsIDs)
	if err != nil {
		return nil, err
	}

	// Phase 3 (lock): look the packet up again, since it may have been evicted
	// meanwhile, and build the response. An observation that arrived between the
	// phases simply has no resolved path in the response.
	s.mu.RLock()
	defer s.mu.RUnlock()
	tx = find()
	if tx == nil {
		return nil, nil
	}
	p := txPacket(tx, paths, true)
	return &p, nil
}

// GetTransmissionByID returns a transmission by its DB ID, or nil when it is not in memory.
func (s *PacketStore) GetTransmissionByID(ctx context.Context, id int) (*api.Packet, error) {
	return s.txPacketByLookup(ctx, func() *StoreTx { return s.byTxID[id] })
}

// GetPacketByHash returns a transmission by content hash, or nil when it is not in memory.
func (s *PacketStore) GetPacketByHash(ctx context.Context, hash string) (*api.Packet, error) {
	hash = strings.ToLower(hash)
	return s.txPacketByLookup(ctx, func() *StoreTx { return s.byHash[hash] })
}

// GetPacketByID returns an observation (with its transmission's payload fields)
// by observation ID, or nil when it is not in memory.
func (s *PacketStore) GetPacketByID(ctx context.Context, id int) (*api.ObservationPacket, error) {
	// Phase 1 (lock): confirm the observation exists.
	s.mu.RLock()
	obs := s.byObsID[id]
	s.mu.RUnlock()
	if obs == nil {
		return nil, nil
	}

	// Phase 2 (no lock): fetch its resolved path.
	paths, err := s.db.ResolvedPaths(ctx, nil, []int{id})
	if err != nil {
		return nil, err
	}

	// Phase 3 (lock): look it up again, since it may have been evicted, and
	// build the response with the transmission resolved under the same lock.
	s.mu.RLock()
	defer s.mu.RUnlock()
	obs = s.byObsID[id]
	if obs == nil {
		return nil, nil
	}
	p := observationPacket(obs, s.byTxID[obs.TransmissionID], paths.ByObs[id])
	return &p, nil
}

// GetObservationsForHash returns all observations for a hash with the packet's
// hash and frame. It is never nil.
func (s *PacketStore) GetObservationsForHash(ctx context.Context, hash string) ([]api.ObservationDetail, error) {
	// Copy the observations under the lock: the ingestor appends to the slice.
	s.mu.RLock()
	tx := s.byHash[strings.ToLower(hash)]
	var obsList []*StoreObs
	if tx != nil {
		obsList = append(obsList, tx.Observations...)
	}
	s.mu.RUnlock()

	if len(obsList) == 0 {
		return []api.ObservationDetail{}, nil
	}
	return s.observationDetails(ctx, obsList)
}

// GetTimestamps returns transmission first_seen timestamps after since, in ASC order.
//
// Production sends these as whole seconds, and the contract follows it. Clients
// send the last one back as `since`, but first_seen has sub-second precision, so
// a transmission that arrived in the same second as the last one returned is
// returned again on the next poll. TODO: send milliseconds in the next release
// (the comparison below is already at the millisecond).
func (s *PacketStore) GetTimestamps(since time.Time) []types.WireTimeSeconds {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Compare at the millisecond so a transmission whose first_seen is T+0.4ms
	// (microsecond precision in Postgres) is not returned again on every poll
	// by a client that sends back a millisecond timestamp.
	since = since.Truncate(time.Millisecond)

	// packets sorted oldest-first: scan from tail until we reach items not after since
	result := []types.WireTimeSeconds{}
	for i := len(s.packets) - 1; i >= 0; i-- {
		tx := s.packets[i]
		if !tx.FirstSeen.Truncate(time.Millisecond).After(since) {
			break
		}
		result = append(result, types.WireTimeSeconds{Time: tx.FirstSeen})
	}
	// result is currently newest-first; reverse to return ASC order
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	return result
}

// QueryMultiNodePackets filters packets matching any of the given pubkeys.
// QueryMultiNodePackets filters packets matching any of the given pubkeys or node names.
func (s *PacketStore) QueryMultiNodePackets(ctx context.Context, pubkeys []string, limit, offset int, order string, since, until time.Time) (*api.PacketListResponse, error) {
	if len(pubkeys) == 0 {
		return &api.PacketListResponse{Packets: []api.Packet{}}, nil
	}
	if limit <= 0 {
		limit = 50
	}

	// Resolve names to pubkeys before taking s.mu: this is a database read.
	resolved, err := s.db.ResolveNodePubkeys(ctx, pubkeys)
	if err != nil {
		return nil, err
	}
	since = since.Truncate(time.Millisecond)
	until = until.Truncate(time.Millisecond)

	// Phase 1 (lock): collect, order and page.
	s.mu.RLock()
	// Use the byNode index instead of scanning all packets.
	hashSet := make(map[string]bool)
	var filtered []*StoreTx
	for _, in := range pubkeys {
		for _, tx := range s.byNode[resolved[in]] {
			if hashSet[tx.Hash] {
				continue
			}
			first := tx.FirstSeen.Truncate(time.Millisecond)
			if !since.IsZero() && first.Before(since) {
				continue
			}
			if !until.IsZero() && first.After(until) {
				continue
			}
			hashSet[tx.Hash] = true
			filtered = append(filtered, tx)
		}
	}
	// #1345: sort by ingest id, not first_seen (=rxTime).
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].ID < filtered[j].ID
	})

	total := len(filtered)
	if offset >= total {
		s.mu.RUnlock()
		return &api.PacketListResponse{Packets: []api.Packet{}, Total: total}, nil
	}
	pageSize := limit
	if offset+pageSize > total {
		pageSize = total - offset
	}

	// filtered is oldest-first; apply the same DESC/ASC pagination as QueryPackets.
	page := make([]*StoreTx, 0, pageSize)
	if order == "ASC" {
		page = append(page, filtered[offset:offset+pageSize]...)
	} else {
		endIdx := total - offset
		startIdx := endIdx - pageSize
		if startIdx < 0 {
			startIdx = 0
		}
		for i := endIdx - 1; i >= startIdx; i-- {
			page = append(page, filtered[i])
		}
	}
	s.mu.RUnlock()

	// Phases 2 and 3: resolved paths, then the responses.
	packets, err := s.responsesFor(ctx, page, false)
	if err != nil {
		return nil, err
	}
	return &api.PacketListResponse{Packets: packets, Total: total}, nil
}

// BroadcastPacket is one live-feed message: the packet fields at the top level
// (live.js reads them there), the same fields nested under "packet" (packets.js
// reads them there), and the decoded payload.
type BroadcastPacket struct {
	LivePacket
	Decoded map[string]interface{} `json:"decoded"`
	Packet  LivePacket             `json:"packet"`
}

// broadcastPacketResponse is the packet as one observer saw it. rp is the
// freshly resolved path for that observation (nil when none).
func broadcastPacketResponse(tx *StoreTx, obs *StoreObs, rp []*string) LivePacket {
	return LivePacket{
		ID:               tx.ID,
		RawHex:           optString(tx.RawHex),
		Hash:             optString(tx.Hash),
		FirstSeen:        optTime(tx.FirstSeen),
		Timestamp:        optTime(tx.FirstSeen),
		RouteType:        tx.RouteType,
		PayloadType:      tx.PayloadType,
		DecodedJSON:      optString(tx.DecodedJSON),
		ObservationCount: tx.ObservationCount,
		ObserverID:       optString(obs.ObserverID),
		ObserverName:     optString(obs.ObserverName),
		ObserverIATA:     optString(obs.ObserverIATA),
		SNR:              obs.SNR,
		RSSI:             obs.RSSI,
		PathJSON:         optString(obs.PathJSON),
		Direction:        optString(obs.Direction),
		ScopeName:        tx.ScopeName,
		ResolvedPath:     rp,
	}
}

// IngestNewFromDB loads new transmissions from SQLite into memory and returns
// broadcast-ready maps plus the new max transmission ID.
// IngestNewFromDB loads transmissions newer than sinceID into memory and returns
// the broadcast messages plus the new max transmission id.
func (s *PacketStore) IngestNewFromDB(ctx context.Context, sinceID, limit int) ([]BroadcastPacket, int, error) {
	if limit <= 0 {
		limit = 100
	}

	// Database reads, no lock held.
	txs, err := s.db.ListTransmissionsChunk(ctx, database.ChunkQuery{AfterID: int64(sinceID), Limit: limit})
	if err != nil {
		return nil, sinceID, fmt.Errorf("ingest: list transmissions: %w", err)
	}
	if len(txs) == 0 {
		return nil, sinceID, nil
	}
	ids := make([]int64, len(txs))
	for i, lt := range txs {
		ids[i] = lt.ID
	}
	obsByTx, err := s.db.ListObservationsForTransmissions(ctx, ids)
	if err != nil {
		return nil, sinceID, fmt.Errorf("ingest: list observations: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	newMaxID := sinceID
	broadcastTxs := make(map[int]*StoreTx) // new transmissions, for broadcast
	var broadcastOrder []int
	hasNewNodes := false

	// Hoisted out of the per-observation loop.
	_, cachedPM := s.getCachedNodesAndPM()
	cachedGraph := s.graph.Load()

	// Fresh per-observation resolved paths, for the broadcast only.
	var broadcastRP map[int][]*string
	hopsSeen := make(map[string]bool) // reused across observations

	for _, lt := range txs {
		tx := s.byHash[lt.Hash]
		if tx == nil {
			tx = newStoreTx(lt)
			s.byHash[tx.Hash] = tx
			s.packets = append(s.packets, tx) // oldest-first; new items go to the tail
			s.byTxID[tx.ID] = tx
			if tx.ID > s.maxTxID {
				s.maxTxID = tx.ID
			}
			if s.indexByNode(tx) {
				hasNewNodes = true
			}
			if tx.PayloadType != nil {
				pt := *tx.PayloadType
				s.byPayloadType[pt] = append(s.byPayloadType[pt], tx)
			}
			s.trackAdvertPubkey(tx)
			s.trackedBytes += rechargeTx(tx)

			broadcastTxs[tx.ID] = tx
			broadcastOrder = append(broadcastOrder, tx.ID)
		}
		if tx.ID > newMaxID {
			newMaxID = tx.ID
		}

		for _, lo := range obsByTx[lt.ID] {
			pj := pathJSON(lo.Path)
			dk := lo.ObserverID + "|" + pj
			if tx.obsKeys == nil {
				tx.obsKeys = make(map[string]bool)
				tx.observerSet = make(map[string]bool)
			}
			if tx.obsKeys[dk] {
				continue
			}
			obs := newStoreObs(lo, tx.ID, pj)

			// Resolve the path now with the neighbor graph. The result goes to
			// the broadcast and the hop index, never onto the struct.
			var rp []*string
			if pj != "" && pj != "[]" && cachedPM != nil {
				rp = resolvePathForObs(pj, obs.ObserverID, tx, cachedPM, cachedGraph)
				s.indexResolvedPathHops(tx, extractResolvedPubkeys(rp), hopsSeen)
			}
			if rp != nil {
				if broadcastRP == nil {
					broadcastRP = make(map[int][]*string)
				}
				broadcastRP[obs.ID] = rp
			}

			tx.Observations = append(tx.Observations, obs)
			tx.obsKeys[dk] = true
			if obs.ObserverID != "" && !tx.observerSet[obs.ObserverID] {
				tx.observerSet[obs.ObserverID] = true
				tx.UniqueObserverCount++
			}
			tx.ObservationCount++
			if obs.Timestamp.After(tx.LatestSeen) {
				tx.LatestSeen = obs.Timestamp
			}
			s.byObsID[obs.ID] = obs
			if obs.ID > s.maxObsID {
				s.maxObsID = obs.ID
			}
			if obs.ObserverID != "" {
				s.byObserver[obs.ObserverID] = append(s.byObserver[obs.ObserverID], obs)
			}
			s.totalObs++
			s.trackedBytes += estimateStoreObsBytes(obs)
		}
	}

	// Pick best observation for new transmissions
	for _, tx := range broadcastTxs {
		pickBestObservation(tx)
		s.trackedBytes += rechargeTx(tx)
	}

	// Incrementally update precomputed subpath index with new transmissions
	for _, tx := range broadcastTxs {
		if addTxToSubpathIndexFull(s.spIndex, s.spTxIndex, tx) {
			s.spTotalPaths++
		}
		addTxToPathHopIndex(s.byPathHop, tx)
	}
	if len(broadcastTxs) > 0 {
		s.invalidateRelayStatsCache()
	}

	// Incrementally update precomputed distance index with new transmissions
	if len(broadcastTxs) > 0 {
		allNodes, pm := s.getCachedNodesAndPM()
		nodeByPk := make(map[string]*nodeInfo, len(allNodes))
		repeaterSet := make(map[string]bool)
		for i := range allNodes {
			n := &allNodes[i]
			nodeByPk[n.PublicKey] = n
			if n.Role == types.RoleRepeater {
				repeaterSet[n.PublicKey] = true
			}
		}
		// Per-tx hop resolver: cache reused across txs, context rebound per
		// tx via setContext (#1197 perf fix).
		resolveHop, setContext := s.hopResolverPerTx(pm)
		for _, tx := range broadcastTxs {
			// Per-tx context (sender + observer + unambiguous-prefix anchors)
			// so resolveWithContext tiers 1 and 2 light up. See #1197.
			setContext(buildHopContextPubkeys(tx, pm))
			txHops, txPath := computeDistancesForTx(tx, nodeByPk, repeaterSet, resolveHop)
			if len(txHops) > 0 {
				s.distHops = append(s.distHops, txHops...)
			}
			if txPath != nil {
				s.distPaths = append(s.distPaths, *txPath)
			}
		}
	}

	result := make([]BroadcastPacket, 0, len(broadcastOrder))
	for _, txID := range broadcastOrder {
		tx := broadcastTxs[txID]
		decoded := broadcastDecoded(tx)
		// TRACE: include path.hopsCompleted so the frontend can tell completed
		// from remaining hops (#683).
		if tx.PayloadType != nil && *tx.PayloadType == types.PayloadTRACE && tx.RawHex != "" {
			if dp, err := decoder.DecodePacket(tx.RawHex, nil, false); err == nil {
				decoded["path"] = dp.Path
			}
		}
		for _, obs := range tx.Observations {
			pkt := broadcastPacketResponse(tx, obs, broadcastRP[obs.ID])
			result = append(result, BroadcastPacket{LivePacket: pkt, Decoded: decoded, Packet: pkt})
		}
	}

	// Targeted cache invalidation: only clear caches affected by the ingested
	// data instead of wiping everything on every cycle (fixes #375).
	if len(result) > 0 {
		inv := cacheInvalidation{
			hasNewTransmissions: len(broadcastTxs) > 0,
			hasNewNodes:         hasNewNodes,
		}
		for _, tx := range broadcastTxs {
			if len(tx.Observations) > 0 {
				inv.hasNewObservations = true
			}
			if tx.PayloadType != nil && *tx.PayloadType == 5 {
				inv.hasChannelData = true
			}
			if tx.PathJSON != "" {
				inv.hasNewPaths = true
			}
			if inv.hasNewObservations && inv.hasChannelData && inv.hasNewPaths {
				break // all flags set, no need to continue
			}
		}
		s.invalidateCachesFor(inv)
	}

	// Per #1287 (Option 4): the server NEVER writes to the DB and
	// NEVER mutates the in-memory neighbor graph incrementally. The
	// ingestor owns neighbor_edges; recompNeighborGraph re-reads the
	// snapshot every 60s and atomic-swaps it into s.graph. We also no
	// longer persist resolved_path here — the ingestor (which already
	// sees every observation) owns that write too.
	_ = broadcastRP // resolved path is still computed in-memory (above) for live broadcast; no SQL write.

	return result, newMaxID, nil
}

// IngestNewObservations loads observations newer than sinceObsID for
// transmissions already in the store. It catches observations that arrive after
// IngestNewFromDB has advanced past the transmission's id (#174). It returns the
// broadcast packets and the cursor to pass on the next call.
func (s *PacketStore) IngestNewObservations(ctx context.Context, sinceObsID int64, limit int) ([]BroadcastPacket, int64, error) {
	if limit <= 0 {
		limit = 500
	}

	// Database read, no lock held.
	newRows, err := s.db.ListObservationsAfterID(ctx, sinceObsID, limit)
	if err != nil {
		return nil, sinceObsID, fmt.Errorf("ingest observations: %w", err)
	}
	if len(newRows) == 0 {
		return nil, sinceObsID, nil
	}

	// Rows are ordered by id ascending, so the last one is the highest. The cursor
	// advances to it even when rows below are skipped (duplicate, or the
	// transmission is not in the store yet), so one bad row cannot stall the poller.
	nextCursor := newRows[len(newRows)-1].ID

	s.mu.Lock()
	defer s.mu.Unlock()

	updatedTxs := make(map[int]*StoreTx)
	broadcastMaps := make([]BroadcastPacket, 0, len(newRows))
	// Only newly created observations are persisted, not all observations of
	// each updated tx (fixes edge count inflation).
	var newObs []*StoreObs
	var obsRPMap map[int][]*string // obsID → resolved path (decode-window)

	_, pm := s.getCachedNodesAndPM()
	graphRef := s.graph.Load()

	hopsSeen := make(map[string]bool) // reused across observations; cleared per use

	for _, no := range newRows {
		// Already ingested (e.g. by IngestNewFromDB in the same cycle).
		if _, exists := s.byObsID[int(no.ID)]; exists {
			continue
		}
		tx := s.byTxID[int(no.TransmissionID)]
		if tx == nil {
			continue // transmission not yet in store
		}

		pj := pathJSON(no.Path)
		dk := no.ObserverID + "|" + pj
		if tx.obsKeys == nil {
			tx.obsKeys = make(map[string]bool)
			tx.observerSet = make(map[string]bool)
		}
		if tx.obsKeys[dk] {
			continue
		}
		// obs.RawHex is deliberately not stored: it duplicates tx.RawHex.
		obs := newStoreObs(no, tx.ID, pj)

		// Resolve the path now for late-arriving observations. Decode-window
		// discipline: decode, feed consumers, don't store on the struct.
		var obsResolvedPath []*string
		if pj != "" && pj != "[]" && pm != nil {
			obsResolvedPath = resolvePathForObs(pj, obs.ObserverID, tx, pm, graphRef)
			s.indexResolvedPathHops(tx, extractResolvedPubkeys(obsResolvedPath), hopsSeen)
		}
		if obsResolvedPath != nil {
			if obsRPMap == nil {
				obsRPMap = make(map[int][]*string)
			}
			obsRPMap[obs.ID] = obsResolvedPath
		}

		tx.Observations = append(tx.Observations, obs)
		tx.obsKeys[dk] = true
		if obs.ObserverID != "" && !tx.observerSet[obs.ObserverID] {
			tx.observerSet[obs.ObserverID] = true
			tx.UniqueObserverCount++
		}
		tx.ObservationCount++
		newObs = append(newObs, obs)
		if obs.Timestamp.After(tx.LatestSeen) {
			tx.LatestSeen = obs.Timestamp
		}
		s.byObsID[obs.ID] = obs
		if obs.ID > s.maxObsID {
			s.maxObsID = obs.ID
		}
		if obs.ObserverID != "" {
			s.byObserver[obs.ObserverID] = append(s.byObserver[obs.ObserverID], obs)
		}
		s.totalObs++
		s.trackedBytes += estimateStoreObsBytes(obs)
		updatedTxs[tx.ID] = tx

		pkt := broadcastPacketResponse(tx, obs, obsRPMap[obs.ID])
		broadcastMaps = append(broadcastMaps, BroadcastPacket{
			LivePacket: pkt,
			Decoded:    broadcastDecoded(tx),
			Packet:     pkt,
		})
	}

	// Re-pick best observation for updated transmissions and update subpath index
	// if the path changed.
	oldPaths := make(map[int]string, len(updatedTxs))
	for txID, tx := range updatedTxs {
		oldPaths[txID] = tx.PathJSON
	}
	for _, tx := range updatedTxs {
		pickBestObservation(tx)
		s.trackedBytes += rechargeTx(tx)
	}
	pathHopMutated := false
	for txID, tx := range updatedTxs {
		if tx.PathJSON != oldPaths[txID] {
			// Path changed — remove old subpaths, add new ones.
			oldHops := parsePathJSON(oldPaths[txID])
			if len(oldHops) >= 2 {
				// Temporarily set parsedPath to old hops for removal.
				saved, savedFlag := tx.parsedPath, tx.pathParsed
				tx.parsedPath, tx.pathParsed = oldHops, true
				if removeTxFromSubpathIndexFull(s.spIndex, s.spTxIndex, tx) {
					s.spTotalPaths--
				}
				tx.parsedPath, tx.pathParsed = saved, savedFlag
			}
			// Remove old path-hop index entries using old hops.
			// Resolved pubkeys remain indexed independently of the best raw path.
			if len(oldHops) > 0 {
				saved, savedFlag := tx.parsedPath, tx.pathParsed
				tx.parsedPath, tx.pathParsed = oldHops, true
				removeTxFromPathHopIndex(s.byPathHop, tx)
				tx.parsedPath, tx.pathParsed = saved, savedFlag
			}
			// pickBestObservation already set pathParsed=false so
			// addTxToSubpathIndex will re-parse the new path.
			if addTxToSubpathIndexFull(s.spIndex, s.spTxIndex, tx) {
				s.spTotalPaths++
			}
			addTxToPathHopIndex(s.byPathHop, tx)
			// #1164: coalesce — one invalidate after the loop, not per-tx.
			pathHopMutated = true
		}
	}
	if pathHopMutated {
		s.invalidateRelayStatsCache()
	}

	// Check if any paths changed (used for distance update and cache invalidation).
	hasPathChanges := false
	var changedTxs []*StoreTx
	for txID, tx := range updatedTxs {
		if tx.PathJSON != oldPaths[txID] {
			hasPathChanges = true
			changedTxs = append(changedTxs, tx)
		}
	}
	if len(changedTxs) > 0 {
		s.updateDistanceIndexForTxs(changedTxs)
	}

	if len(updatedTxs) > 0 {
		// Targeted cache invalidation: new observations always affect RF
		// analytics; topology/distance/subpath caches only if paths changed.
		// Channel and hash caches are unaffected by observation-only ingestion.
		s.invalidateCachesFor(cacheInvalidation{
			hasNewObservations: true,
			hasNewPaths:        hasPathChanges,
		})
	}

	// Per #1287 (Option 4): server never writes to the DB and never
	// mutates the in-memory neighbor graph incrementally — the
	// ingestor owns both. recompNeighborGraph re-reads the snapshot
	// every 60s and atomic-swaps into s.graph.
	_ = obsRPMap // resolved path stays in-memory for broadcast; no SQL write.
	_ = newObs
	_ = pm
	_ = graphRef

	return broadcastMaps, nextCursor, nil
}

// MaxTransmissionID returns the highest transmission ID in the store.
func (s *PacketStore) MaxTransmissionID() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.maxTxID
}

// MaxObservationID returns the highest observation ID in the store.
func (s *PacketStore) MaxObservationID() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.maxObsID
}

// --- Internal filter/query helpers ---

// filterPackets applies PacketQuery filters to the in-memory packet list.
// The caller holds s.mu (read) and has already resolved q.Node to a pubkey.
// filterPackets applies PacketQuery filters to the in-memory packet list.
// The caller holds s.mu (read) and has already resolved q.Node to a pubkey.
func (s *PacketStore) filterPackets(ctx context.Context, q database.PacketQuery) []*StoreTx {
	hasObsRange := q.ObsSince != nil || q.ObsUntil != nil
	hasFirstSeenRange := q.FirstSeenAfter != nil || q.FirstSeenBefore != nil
	noTime := !hasObsRange && !hasFirstSeenRange

	// Fast path: single-key index lookups
	if q.Hash != "" && q.Type == nil && q.Route == nil && len(q.Observers) == 0 &&
		q.Region == "" && q.Area == "" && q.Node == "" && q.Channel == "" && noTime {
		tx := s.byHash[strings.ToLower(q.Hash)]
		if tx == nil {
			return nil
		}
		return []*StoreTx{tx}
	}
	if len(q.Observers) > 0 && q.Type == nil && q.Route == nil &&
		q.Region == "" && q.Area == "" && q.Node == "" && q.Channel == "" && q.Hash == "" && noTime {
		return s.transmissionsForObserver(strings.Join(q.Observers, ","), nil)
	}

	// Pre-compute filter parameters outside the hot loop.
	var (
		filterType  int
		hasType     bool
		filterRoute int
		hasRoute    bool
		filterHash  string
	)
	if q.Type != nil {
		hasType = true
		filterType = *q.Type
	}
	if q.Route != nil {
		hasRoute = true
		filterRoute = *q.Route
	}
	if q.Hash != "" {
		filterHash = strings.ToLower(q.Hash)
	}

	// Stored times are compared at millisecond precision, like the cursors the
	// client sends.
	trunc := func(t *time.Time) time.Time {
		if t == nil {
			return time.Time{}
		}
		return t.Truncate(time.Millisecond)
	}
	obsSince, obsUntil := trunc(q.ObsSince), trunc(q.ObsUntil)
	firstAfter, firstBefore := trunc(q.FirstSeenAfter), trunc(q.FirstSeenBefore)

	var observerSet map[string]bool
	if len(q.Observers) > 0 {
		observerSet = make(map[string]bool, len(q.Observers))
		for _, id := range q.Observers {
			observerSet[id] = true
		}
	}

	var regionObservers map[string]bool
	if q.Region != "" {
		regionObservers = s.resolveRegionObservers(ctx, q.Region)
		if len(regionObservers) == 0 {
			return nil
		}
	}

	var areaNodes map[string]bool
	if q.Area != "" {
		areaNodes = s.resolveAreaNodes(ctx, q.Area)
	}

	// Node filter: q.Node is already a pubkey (resolved before the lock).
	hasNode := q.Node != ""
	nodePK := q.Node
	var nodeHashSet map[string]bool
	if hasNode {
		txs := s.byNode[nodePK]
		nodeHashSet = make(map[string]bool, len(txs))
		for _, tx := range txs {
			nodeHashSet[tx.Hash] = true
		}
	}

	// Index-based source when only the node filter is active.
	source := s.packets
	if hasNode && !hasType && !hasRoute && len(q.Observers) == 0 &&
		filterHash == "" && noTime && q.Region == "" && q.Area == "" && q.Channel == "" {
		if indexed, ok := s.byNode[nodePK]; ok {
			return indexed
		}
	}

	// Single-pass filter: apply all predicates in one scan.
	return filterTxSlice(source, func(tx *StoreTx) bool {
		// Data integrity: exclude legacy rows missing hash or timestamp (#871)
		if tx.Hash == "" || tx.FirstSeen.IsZero() {
			return false
		}
		if hasType && (tx.PayloadType == nil || int(*tx.PayloadType) != filterType) {
			return false
		}
		if hasRoute && (tx.RouteType == nil || int(*tx.RouteType) != filterRoute) {
			return false
		}
		if filterHash != "" && tx.Hash != filterHash {
			return false
		}
		// first_seen bounds are strict (date-only input).
		if hasFirstSeenRange {
			first := tx.FirstSeen.Truncate(time.Millisecond)
			if q.FirstSeenAfter != nil && !first.After(firstAfter) {
				return false
			}
			if q.FirstSeenBefore != nil && !first.Before(firstBefore) {
				return false
			}
		}
		// Observation bounds are inclusive and match if any observation is in range (#1187 r2).
		if hasObsRange && !txHasObsInRange(tx, q.ObsSince != nil, obsSince, q.ObsUntil != nil, obsUntil) {
			return false
		}
		if observerSet != nil && !txSeenByAny(tx, observerSet) {
			return false
		}
		if regionObservers != nil && !txSeenByAny(tx, regionObservers) {
			return false
		}
		if areaNodes != nil {
			// Only ADVERT packets carry the originator pubkey (public_key/pubKey).
			// Other types have encrypted senders, so pk == "" and they are
			// excluded when an area filter is active.
			d := tx.ParsedDecoded()
			pk, _ := d["public_key"].(string)
			if pk == "" {
				pk, _ = d["pubKey"].(string)
			}
			if pk == "" || !areaNodes[pk] {
				return false
			}
		}
		if hasNode && !nodeHashSet[tx.Hash] {
			return false
		}
		if q.Channel != "" && !packetMatchesChannel(tx, channelFilterValue(q)) {
			return false
		}
		return true
	})
}

// txSeenByAny reports whether any observation of tx came from an observer in set.
func txSeenByAny(tx *StoreTx, set map[string]bool) bool {
	for _, obs := range tx.Observations {
		if set[obs.ObserverID] {
			return true
		}
	}
	return false
}

// channelFilterValue rebuilds the legacy channel string ("enc_" prefix for
// encrypted channels) that packetMatchesChannel expects. The handler splits it
// into Channel and ChannelEncrypted.
func channelFilterValue(q database.PacketQuery) string {
	if q.ChannelEncrypted {
		return "enc_" + q.Channel
	}
	return q.Channel
}

// txHasObsInRange reports whether any observation of tx has a timestamp within
// the given inclusive bounds. A bound is applied only when its has flag is set.
func txHasObsInRange(tx *StoreTx, hasSince bool, since time.Time, hasUntil bool, until time.Time) bool {
	for _, obs := range tx.Observations {
		ts := obs.Timestamp.Truncate(time.Millisecond)
		if hasSince && ts.Before(since) {
			continue
		}
		if hasUntil && ts.After(until) {
			continue
		}
		return true
	}
	return false
}

// packetMatchesChannel returns true if the transmission's decoded payload
// matches the requested channel filter (#812). The filter accepts either a
// plaintext channel name (e.g. "public", "#test") matching decoded.channel,
// or "enc_<HEX>" matching the channelHashHex of an undecryptable GRP_TXT.
func packetMatchesChannel(tx *StoreTx, filterChannel string) bool {
	if tx.PayloadType == nil || *tx.PayloadType != 5 {
		return false
	}
	if tx.DecodedJSON == "" {
		return false
	}
	d := tx.ParsedDecoded()
	if d == nil {
		return false
	}
	if ch, ok := d["channel"].(string); ok && ch != "" {
		if ch == filterChannel {
			return true
		}
	}
	if strings.HasPrefix(filterChannel, "enc_") {
		if hex, ok := d["channelHashHex"].(string); ok && hex != "" {
			if "enc_"+hex == filterChannel {
				return true
			}
		}
	}
	return false
}

// transmissionsForObserver returns unique transmissions for an observer.
func (s *PacketStore) transmissionsForObserver(observerIDs string, from []*StoreTx) []*StoreTx {
	ids := strings.Split(observerIDs, ",")
	idSet := make(map[string]bool, len(ids))
	for i, id := range ids {
		ids[i] = strings.TrimSpace(id)
		idSet[ids[i]] = true
	}
	if from != nil {
		return filterTxSlice(from, func(tx *StoreTx) bool {
			for _, obs := range tx.Observations {
				if idSet[obs.ObserverID] {
					return true
				}
			}
			return false
		})
	}
	// Use byObserver index: union transmissions for all IDs
	seen := make(map[int]bool)
	var result []*StoreTx
	for _, id := range ids {
		for _, obs := range s.byObserver[id] {
			if seen[obs.TransmissionID] {
				continue
			}
			seen[obs.TransmissionID] = true
			tx := s.byTxID[obs.TransmissionID]
			if tx != nil {
				result = append(result, tx)
			}
		}
	}
	return result
}

// resolveRegionObservers returns a set of observer IDs for a given IATA region.
// Results are cached for 30 seconds to avoid repeated DB queries.
// Uses its own mutex (regionObsMu) so callers holding s.mu won't deadlock.
func (s *PacketStore) resolveRegionObservers(ctx context.Context, region string) map[string]bool {
	s.regionObsMu.Lock()
	defer s.regionObsMu.Unlock()

	if s.regionObsCache != nil && time.Since(s.regionObsCacheTime) < 30*time.Second {
		if m, ok := s.regionObsCache[region]; ok {
			return m
		}
		return s.fetchAndCacheRegionObs(ctx, region)
	}
	// Cache expired — rebuild.
	s.regionObsCache = make(map[string]map[string]bool)
	s.regionObsCacheTime = time.Now()

	// Fetch for the requested region and cache it.
	return s.fetchAndCacheRegionObs(ctx, region)
}

// fetchAndCacheRegionObs fetches observer IDs for a region from the DB and stores in cache.
// Caller must hold regionObsMu.
func (s *PacketStore) fetchAndCacheRegionObs(ctx context.Context, region string) map[string]bool {
	if m, ok := s.regionObsCache[region]; ok {
		return m
	}
	ids, err := s.db.GetObserverIdsForRegion(ctx, region)
	if err != nil || len(ids) == 0 {
		s.regionObsCache[region] = nil
		return nil
	}
	m := make(map[string]bool, len(ids))
	for _, id := range ids {
		m[id] = true
	}
	s.regionObsCache[region] = m
	return m
}

// resolveAreaNodes returns a set of node pubkeys whose GPS coordinates fall
// inside the named area polygon. Returns nil if the area key is not in config.
// Results are cached per-key for 30 seconds. Uses its own RWMutex so callers
// holding s.mu won't deadlock.
func (s *PacketStore) resolveAreaNodes(ctx context.Context, areaKey string) map[string]bool {
	if s.config == nil || s.config.Areas == nil {
		return nil
	}
	entry, ok := s.config.Areas[areaKey]
	if !ok {
		return nil
	}

	// Fast path: serve from cache if the per-key TTL is still valid.
	s.areaNodeMu.RLock()
	if t, ok := s.areaNodeCacheTimes[areaKey]; ok && time.Since(t) < 30*time.Second {
		m := s.areaNodeCache[areaKey]
		s.areaNodeMu.RUnlock()
		return m
	}
	s.areaNodeMu.RUnlock()

	// Slow path: query the DB outside any lock, then write back under Lock.
	pks, err := s.db.GetNodePubkeysInArea(ctx, entry.geoFilter())
	var m map[string]bool
	if err == nil && len(pks) > 0 {
		m = make(map[string]bool, len(pks))
		for _, pk := range pks {
			m[pk] = true
		}
	}

	s.areaNodeMu.Lock()
	// Re-check in case another goroutine already refreshed while we queried.
	if t, ok := s.areaNodeCacheTimes[areaKey]; !ok || time.Since(t) >= 30*time.Second {
		s.areaNodeCache[areaKey] = m
		s.areaNodeCacheTimes[areaKey] = time.Now()
	} else {
		m = s.areaNodeCache[areaKey]
	}
	s.areaNodeMu.Unlock()
	return m
}

// iataMatchesRegion returns true if iata matches any of the comma-separated
// region codes in regionParam. Comparison is case-insensitive and trim-tolerant.
// Empty iata never matches; empty regionParam never matches.
//
// #804: shared helper used by analytics to attribute transmissions to a node's
// HOME region (derived from observers that hear its zero-hop direct adverts)
// rather than to the observer that happened to relay a packet.
func iataMatchesRegion(iata, regionParam string) bool {
	if iata == "" || regionParam == "" {
		return false
	}
	codes := database.NormalizeRegionCodes(regionParam)
	if len(codes) == 0 {
		return false
	}
	got := strings.TrimSpace(strings.ToUpper(iata))
	if got == "" {
		return false
	}
	for _, c := range codes {
		if c == got {
			return true
		}
	}
	return false
}

// computeNodeHomeRegions returns a pubkey → IATA map deriving each node's
// HOME region from zero-hop DIRECT adverts. A zero-hop direct advert is the
// most authoritative location signal because the path byte is set locally on
// the originating radio and the packet has not been relayed: the observer
// that hears it is necessarily within direct RF range of the originator.
//
// When a node has zero-hop direct adverts heard by observers from multiple
// regions, the most-frequently-observed region wins (geographic plurality).
//
// Caller must hold s.mu (read or write). Returns empty map (not nil) if no
// observers are loaded or no zero-hop direct adverts have been seen.
//
// #804: feeds analytics region-attribution so a multi-byte repeater whose
// flood adverts get relayed across regions is still attributed to its home.
func (s *PacketStore) computeNodeHomeRegions(obsIATA map[string]string) map[string]string {
	// obsIATA is expected to be pre-loaded by the caller (e.g., via loadObserverIATA).
	if len(obsIATA) == 0 {
		return map[string]string{}
	}

	// Tally zero-hop direct ADVERT region observations per pubkey.
	type tally struct {
		counts map[string]int
	}
	per := make(map[string]*tally, 256)

	for _, tx := range s.packets {
		if tx.RawHex == "" || len(tx.RawHex) < 4 {
			continue
		}
		if tx.PayloadType == nil || *tx.PayloadType != types.PayloadADVERT {
			continue
		}
		if tx.DecodedJSON == "" {
			continue
		}
		header, err := strconv.ParseUint(tx.RawHex[:2], 16, 8)
		if err != nil {
			continue
		}
		routeType := header & 0x03
		if routeType != uint64(types.RouteDirect) && routeType != uint64(types.RouteTransportDirect) {
			continue
		}
		// Path byte index — for direct/transport-direct it's at offset 1
		// (matches the analytics decoder's pathByteIdx logic).
		if len(tx.RawHex) < 4 {
			continue
		}
		pathByte, err := strconv.ParseUint(tx.RawHex[2:4], 16, 8)
		if err != nil {
			continue
		}
		hopCount := pathByte & 0x3F
		if hopCount != 0 {
			continue
		}

		d := tx.ParsedDecoded()
		if d == nil {
			continue
		}
		pk, _ := d["pubKey"].(string)
		if pk == "" {
			pk, _ = d["public_key"].(string)
		}
		if pk == "" {
			continue
		}

		for _, obs := range tx.Observations {
			iata := obsIATA[obs.ObserverID]
			if iata == "" {
				continue
			}
			t := per[pk]
			if t == nil {
				t = &tally{counts: map[string]int{}}
				per[pk] = t
			}
			t.counts[iata]++
		}
	}

	out := make(map[string]string, len(per))
	for pk, t := range per {
		var bestIATA string
		bestCount := 0
		for iata, n := range t.counts {
			if n > bestCount || (n == bestCount && iata < bestIATA) {
				bestCount = n
				bestIATA = iata
			}
		}
		if bestIATA != "" {
			out[pk] = bestIATA
		}
	}
	return out
}

// observationDetails builds the observation responses for obsList. The caller
// holds no lock: it resolves each transmission under a short read lock, fetches
// the resolved paths from the database, then builds the responses.
func (s *PacketStore) observationDetails(ctx context.Context, obsList []*StoreObs) ([]api.ObservationDetail, error) {
	// Phase 1 (lock): byTxID is written by the ingestor (#134).
	txs := make([]*StoreTx, len(obsList))
	obsIDs := make([]int, len(obsList))
	s.mu.RLock()
	for i, o := range obsList {
		txs[i] = s.byTxID[o.TransmissionID]
		obsIDs[i] = o.ID
	}
	s.mu.RUnlock()

	// Phase 2 (no lock): resolved paths.
	paths, err := s.db.ResolvedPaths(ctx, nil, obsIDs)
	if err != nil {
		return nil, err
	}

	// Phase 3 (no lock): build. Same as the analytics handler (#1481): it
	// reads fields of the *StoreTx it resolved under the lock.
	out := make([]api.ObservationDetail, len(obsList))
	for i, o := range obsList {
		out[i] = observationDetail(o, txs[i], paths.ByObs[o.ID])
	}
	return out, nil
}

// enrichObsWithTx is enrichObs with the transmission already resolved by the
// caller. It performs NO s.mu-guarded MAP lookup (no s.byTxID or other guarded
// map access), so the caller may release s.mu before calling it, provided it
// read byTxID under the lock. The on-demand resolved_path fetch uses its own
// lruMu, independent of s.mu. tx may be nil (unknown transmission), in which
// case the transmission-derived fields are omitted.
//
// Scope of the safety guarantee: this removes the unlocked *map* access only.
// Reading fields off the snapshotted *StoreObs / *StoreTx after s.mu is
// released is safe because the store replaces index entries rather than
// mutating live objects in place. That object-lifecycle invariant is separate
// from this change and is NOT established here; the one known in-place mutation
// (hash-migrate rewriting tx.Hash) is a distinct field-level race tracked in
// #142, not closed by this method.
func (s *PacketStore) enrichObsWithTx(obs *StoreObs, tx *StoreTx, rp []*string) map[string]interface{} {
	m := map[string]interface{}{
		"id":            obs.ID,
		"timestamp":     formatTS(obs.Timestamp),
		"observer_id":   strOrNil(obs.ObserverID),
		"observer_name": strOrNil(obs.ObserverName),
		"observer_iata": strOrNil(obs.ObserverIATA),
		"direction":     strOrNil(obs.Direction),
		"snr":           floatPtrOrNil(obs.SNR),
		"rssi":          floatPtrOrNil(obs.RSSI),
		"score":         floatPtrOrNil(obs.Score),
		"path_json":     strOrNil(obs.PathJSON),
	}
	if rp != nil {
		m["resolved_path"] = rp
	}
	if tx != nil {
		m["hash"] = strOrNil(tx.Hash)
		// raw_hex comes from the transmission (#881). obs.RawHex is no longer
		// retained when loading/ingesting the store: it duplicated this exact
		// value (same content hash => same frame) and at ~10.6 observations/tx
		// wasted ~98MB on a live ~1.7M-observation store. The obs.RawHex branch
		// stays for callers that build a StoreObs from a response map.
		if obs.RawHex != "" {
			m["raw_hex"] = obs.RawHex
		} else {
			m["raw_hex"] = strOrNil(tx.RawHex)
		}
		m["payload_type"] = tx.PayloadType
		m["route_type"] = tx.RouteType
		m["decoded_json"] = strOrNil(tx.DecodedJSON)
	}

	return m
}

// --- Conversion helpers ---

func strOrNil(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func strPtrOrNil(p *string) interface{} {
	if p == nil {
		return nil
	}
	return *p
}

func intPtrOrNil(p *int) interface{} {
	if p == nil {
		return nil
	}
	return *p
}

func floatPtrOrNil(p *float64) interface{} {
	if p == nil {
		return nil
	}
	return *p
}

// resolvePayloadTypeName returns the firmware-standard name for a payload_type.
func resolvePayloadTypeName(pt *types.PayloadType) string {
	if pt == nil {
		return "UNKNOWN"
	}
	if pt.Valid() {
		return pt.String()
	}
	return fmt.Sprintf("UNK(%d)", *pt)
}

// txGetParsedPath returns cached parsed path hops, parsing on first call.
func txGetParsedPath(tx *StoreTx) []string {
	if tx.pathParsed {
		return tx.parsedPath
	}
	tx.parsedPath = parsePathJSON(tx.PathJSON)
	tx.pathParsed = true
	return tx.parsedPath
}

// addTxToSubpathIndex extracts all raw subpaths (lengths 2–8) from tx and
// increments their counts in the index.  Returns true if the tx contributed
// (path had ≥ 2 hops).
func addTxToSubpathIndex(idx map[string]int, tx *StoreTx) bool {
	return addTxToSubpathIndexFull(idx, nil, tx)
}

// addTxToSubpathIndexFull is like addTxToSubpathIndex but also appends
// tx to txIdx for each subpath key (if txIdx is non-nil).
func addTxToSubpathIndexFull(idx map[string]int, txIdx map[string][]*StoreTx, tx *StoreTx) bool {
	hops := txGetParsedPath(tx)
	if len(hops) < 2 {
		return false
	}
	maxL := min(8, len(hops))
	for l := 2; l <= maxL; l++ {
		for start := 0; start <= len(hops)-l; start++ {
			key := strings.ToLower(strings.Join(hops[start:start+l], ","))
			idx[key]++
			if txIdx != nil {
				txIdx[key] = append(txIdx[key], tx)
			}
		}
	}
	return true
}

// removeTxFromSubpathIndexFull is like removeTxFromSubpathIndex but also
// removes tx from txIdx for each subpath key (if txIdx is non-nil).
func removeTxFromSubpathIndexFull(idx map[string]int, txIdx map[string][]*StoreTx, tx *StoreTx) bool {
	hops := txGetParsedPath(tx)
	if len(hops) < 2 {
		return false
	}
	maxL := min(8, len(hops))
	for l := 2; l <= maxL; l++ {
		for start := 0; start <= len(hops)-l; start++ {
			key := strings.ToLower(strings.Join(hops[start:start+l], ","))
			idx[key]--
			if idx[key] <= 0 {
				delete(idx, key)
			}
			if txIdx != nil {
				txs := txIdx[key]
				for i, t := range txs {
					if t == tx {
						txIdx[key] = append(txs[:i], txs[i+1:]...)
						break
					}
				}
				if len(txIdx[key]) == 0 {
					delete(txIdx, key)
				}
			}
		}
	}
	return true
}

// buildSubpathIndex scans all packets and populates spIndex + spTotalPaths.
// Must be called with s.mu held.
func (s *PacketStore) buildSubpathIndex() {
	s.spIndex = make(map[string]int, 4096)
	s.spTxIndex = make(map[string][]*StoreTx, 4096)
	s.spTotalPaths = 0
	for _, tx := range s.packets {
		if addTxToSubpathIndexFull(s.spIndex, s.spTxIndex, tx) {
			s.spTotalPaths++
		}
	}
	log.Printf("[store] Built subpath index: %d unique raw subpaths from %d paths",
		len(s.spIndex), s.spTotalPaths)
}

// buildPathHopIndex rebuilds byPathHop: raw wire hops from every packet's
// path_json, plus the resolved full-pubkey hops carried over from the
// previous index (see retainResolvedPathHops).
// Must be called with s.mu held.
func (s *PacketStore) buildPathHopIndex() {
	prev := s.byPathHop
	s.byPathHop = make(map[string][]*StoreTx, max(4096, len(prev)))
	for _, tx := range s.packets {
		addTxToPathHopIndex(s.byPathHop, tx)
	}
	retained := s.retainResolvedPathHops(prev)
	log.Printf("[store] Built path-hop index: %d unique keys (%d resolved-hop entries retained)",
		len(s.byPathHop), retained)
}

// retainResolvedPathHops re-merges the entries of a pre-rebuild byPathHop
// that the raw-hop pass above cannot reproduce: the resolved full-pubkey
// keys fed by indexResolvedPathHops. Their pubkey strings are retained
// nowhere — #800 dropped the per-StoreTx ResolvedPath field in favour of a
// hash-only membership index — so a plain rebuild silently discarded every
// resolved relay attribution, leaving relay counts and transported scopes
// empty after each cold load until live ingestion refilled them (#1904).
//
// Only transmissions still in s.packets are carried over. Eviction also
// prunes raw and resolved keys (#1908); this membership check prevents a
// rebuild from reintroducing stale entries from the previous index.
//
// Cost is O(entries in prev) with one reused scratch map, and it runs only
// where buildPathHopIndex already runs — cold load and background-fill
// completion — never on an ingest or request path.
//
// Returns the number of entries carried over. Must be called with s.mu held,
// after s.byPathHop has been rebuilt from raw hops.
func (s *PacketStore) retainResolvedPathHops(prev map[string][]*StoreTx) int {
	if len(prev) == 0 {
		return 0
	}
	live := make(map[*StoreTx]struct{}, len(s.packets))
	for _, tx := range s.packets {
		live[tx] = struct{}{}
	}

	// Reused across keys (cleared per key) so a large index does not churn
	// one map allocation per key. Guards against both a key that the raw
	// pass already produced and repeated appends of the same tx in prev —
	// indexResolvedPathHops dedups within a call, not across the several
	// observations of one transmission.
	seen := make(map[*StoreTx]struct{}, 16)
	retained := 0
	for key, list := range prev {
		if len(list) == 0 {
			continue
		}
		clear(seen)
		for _, tx := range s.byPathHop[key] {
			seen[tx] = struct{}{}
		}
		for _, tx := range list {
			if tx == nil {
				continue
			}
			if _, ok := live[tx]; !ok {
				continue
			}
			if _, dup := seen[tx]; dup {
				continue
			}
			seen[tx] = struct{}{}
			s.byPathHop[key] = append(s.byPathHop[key], tx)
			retained++
		}
	}
	return retained
}

// addTxToPathHopIndex indexes a transmission under each unique raw hop key.
// Resolved pubkey keys are handled by the decode-window via feedDecodeWindowConsumers.
func addTxToPathHopIndex(idx map[string][]*StoreTx, tx *StoreTx) {
	hops := txGetParsedPath(tx)
	if len(hops) == 0 {
		return
	}
	seen := make(map[string]bool, len(hops))
	for _, hop := range hops {
		key := strings.ToLower(hop)
		if !seen[key] {
			seen[key] = true
			idx[key] = append(idx[key], tx)
		}
	}
}

// addTxToRelayTimeIndex records the relay timestamp for each resolved pubkey.
// pubkeys is the pre-extracted list (use extractResolvedPubkeys on the decoded path).
// Maintains sorted ascending order for O(log n) window queries.
// Must be called with s.mu held (or during build before store is live).
func addTxToRelayTimeIndex(idx map[string][]int64, firstSeen string, pubkeys []string) {
	if len(pubkeys) == 0 {
		return
	}
	ms, err := time.Parse(time.RFC3339, firstSeen)
	if err != nil {
		return
	}
	millis := ms.UnixMilli()
	seen := make(map[string]bool, len(pubkeys))
	for _, pk := range pubkeys {
		pk = strings.ToLower(pk)
		if pk == "" || seen[pk] {
			continue
		}
		seen[pk] = true
		slice := idx[pk]
		i := sort.Search(len(slice), func(j int) bool { return slice[j] >= millis })
		if i < len(slice) && slice[i] == millis {
			continue // idempotent
		}
		slice = append(slice, 0)
		copy(slice[i+1:], slice[i:])
		slice[i] = millis
		idx[pk] = slice
	}
}

// removeFromRelayTimeIndex removes the relay timestamp for each resolved pubkey.
// Inverse of addTxToRelayTimeIndex.
func removeFromRelayTimeIndex(idx map[string][]int64, firstSeen string, pubkeys []string) {
	if len(pubkeys) == 0 {
		return
	}
	ms, err := time.Parse(time.RFC3339, firstSeen)
	if err != nil {
		return
	}
	millis := ms.UnixMilli()
	seen := make(map[string]bool, len(pubkeys))
	for _, pk := range pubkeys {
		pk = strings.ToLower(pk)
		if pk == "" || seen[pk] {
			continue
		}
		seen[pk] = true
		slice := idx[pk]
		i := sort.Search(len(slice), func(j int) bool { return slice[j] >= millis })
		if i < len(slice) && slice[i] == millis {
			newSlice := make([]int64, 0, len(slice)-1)
			newSlice = append(newSlice, slice[:i]...)
			newSlice = append(newSlice, slice[i+1:]...)
			idx[pk] = newSlice
			if len(newSlice) == 0 {
				delete(idx, pk)
			}
		}
	}
}

// relayMetrics computes relay_count_1h, relay_count_24h, and last_relayed from a
// sorted unix-millis slice. now is time.Now().UnixMilli(). O(log n).
func relayMetrics(times []int64, now int64) (count1h, count24h int, lastRelayed string) {
	if len(times) == 0 {
		return 0, 0, ""
	}
	i1h := sort.Search(len(times), func(i int) bool { return times[i] >= now-3600000 })
	i24h := sort.Search(len(times), func(i int) bool { return times[i] >= now-86400000 })
	count1h = len(times) - i1h
	count24h = len(times) - i24h
	lastRelayed = time.UnixMilli(times[len(times)-1]).UTC().Format(time.RFC3339)
	return
}

// removeTxFromPathHopIndex removes a transmission from all its raw path-hop index entries.
// Used when the best raw path changes; eviction filters all keys in one batch.
func removeTxFromPathHopIndex(idx map[string][]*StoreTx, tx *StoreTx) {
	hops := txGetParsedPath(tx)
	if len(hops) == 0 {
		return
	}
	seen := make(map[string]bool, len(hops))
	for _, hop := range hops {
		key := strings.ToLower(hop)
		if !seen[key] {
			seen[key] = true
			removeTxFromSlice(idx, key, tx)
		}
	}
}

// addResolvedPubkeysToPathHopIndex appends tx into byPathHop under each
// resolved pubkey key that isn't already present as a raw hop. Mutating
// byPathHop here MUST be paired with invalidateRelayStatsCache so the
// cached batch relay stats don't go stale for up to relayStatsCacheTTL.
// hopsSeen is a scratch map the caller can reuse across calls (it will
// be cleared on entry).
//
// Must be called with s.mu held.
func (s *PacketStore) addResolvedPubkeysToPathHopIndex(tx *StoreTx, pubkeys []string, hopsSeen map[string]bool) bool {
	if len(pubkeys) == 0 {
		return false
	}
	clear(hopsSeen)
	for _, hop := range txGetParsedPath(tx) {
		hopsSeen[strings.ToLower(hop)] = true
	}
	mutated := false
	for _, pk := range pubkeys {
		if !hopsSeen[pk] {
			hopsSeen[pk] = true
			s.byPathHop[pk] = append(s.byPathHop[pk], tx)
			mutated = true
		}
	}
	// Mutating byPathHop invalidates the batch relay-stats cache (#1164).
	if mutated {
		s.invalidateRelayStatsCache()
	}
	return mutated
}

// invalidateRelayStatsCache drops the cached batch relay-stats result so
// the next call to GetRepeaterNodeStatsBatchCached recomputes from scratch.
// Call this whenever byPathHop changes (add or remove). Safe to call while
// holding s.mu — relayStatsCacheMu is never acquired while holding relayStatsCacheMu,
// so there is no lock-ordering cycle.
func (s *PacketStore) invalidateRelayStatsCache() {
	s.relayStatsCacheMu.Lock()
	s.relayStatsCache = nil
	s.relayStatsCacheMu.Unlock()
}

// removeTxFromSlice removes tx from idx[key] by ID, deleting the key if empty.
func removeTxFromSlice(idx map[string][]*StoreTx, key string, tx *StoreTx) {
	list := idx[key]
	for i, t := range list {
		if t.ID == tx.ID {
			idx[key] = append(list[:i], list[i+1:]...)
			break
		}
	}
	if len(idx[key]) == 0 {
		delete(idx, key)
	}
}

// updateDistanceIndexForTxs removes old distance records for the given
// transmissions and recomputes them. Builds lookup maps once, amortising the
// cost across all changed txs in a single ingest cycle. Must be called with
// s.mu held.
func (s *PacketStore) updateDistanceIndexForTxs(txs []*StoreTx) {
	// Remove old records for all changed txs first.
	removeSet := make(map[*StoreTx]bool, len(txs))
	for _, tx := range txs {
		removeSet[tx] = true
	}
	n := 0
	for _, r := range s.distHops {
		if !removeSet[r.tx] {
			s.distHops[n] = r
			n++
		}
	}
	s.distHops = s.distHops[:n]
	n = 0
	for _, r := range s.distPaths {
		if !removeSet[r.tx] {
			s.distPaths[n] = r
			n++
		}
	}
	s.distPaths = s.distPaths[:n]

	// Build lookup maps once.
	allNodes, pm := s.getCachedNodesAndPM()
	nodeByPk := make(map[string]*nodeInfo, len(allNodes))
	repeaterSet := make(map[string]bool)
	for i := range allNodes {
		nd := &allNodes[i]
		nodeByPk[nd.PublicKey] = nd
		if nd.Role == types.RoleRepeater {
			repeaterSet[nd.PublicKey] = true
		}
	}
	// Per-tx hop resolver shared across the recompute loop (#1197 perf).
	resolveHop, setContext := s.hopResolverPerTx(pm)
	// Recompute distance records for each changed tx.
	for _, tx := range txs {
		// Per-tx context for hop disambiguation (#1197).
		setContext(buildHopContextPubkeys(tx, pm))
		txHops, txPath := computeDistancesForTx(tx, nodeByPk, repeaterSet, resolveHop)
		if len(txHops) > 0 {
			s.distHops = append(s.distHops, txHops...)
		}
		if txPath != nil {
			s.distPaths = append(s.distPaths, *txPath)
		}
	}
}

// DistanceIndexBuilt reports whether the distance analytics index has
// been constructed. Used by tests and /api/perf to verify the lazy
// build invariant from issue #1011 (eager Load() build removed).
func (s *PacketStore) DistanceIndexBuilt() bool {
	s.distLazyMu.Lock()
	defer s.distLazyMu.Unlock()
	return s.distLazyBuilt
}

// DistanceIndexBuilding reports whether a lazy distance-index build
// is currently in flight. Used by the /api/analytics/distance handler
// to serve 202 + Retry-After to concurrent first-window requests
// (#1011).
func (s *PacketStore) DistanceIndexBuilding() bool {
	s.distLazyMu.Lock()
	defer s.distLazyMu.Unlock()
	return s.distLazyBuilding
}

// TriggerDistanceIndexBuild kicks off a lazy build of the distance
// index in a background goroutine if one is not already running and
// the debounce policy permits. Returns immediately. Idempotent:
// concurrent callers see only one build, gated by sync.Once.
//
// Debounce policy (#1011 triage Fix path): rebuild if Δobs > 5% since
// the last build OR at most once per 5 minutes — whichever is more
// restrictive. The first-ever build always runs.
func (s *PacketStore) TriggerDistanceIndexBuild() {
	s.distLazyMu.Lock()
	if s.distLazyBuilding {
		s.distLazyMu.Unlock()
		return
	}
	// Debounce: if a build has already completed, suppress re-trigger
	// unless Δobs > 5% or >5min has elapsed.
	if s.distLazyBuilt {
		s.mu.RLock()
		curObs := s.totalObs
		s.mu.RUnlock()
		elapsed := time.Since(s.distLazyLastBuilt)
		deltaPct := 0.0
		if s.distLazyLastObs > 0 {
			deltaPct = float64(curObs-s.distLazyLastObs) / float64(s.distLazyLastObs)
		}
		if elapsed < 5*time.Minute && deltaPct < 0.05 {
			s.distLazyMu.Unlock()
			return
		}
		// Reset the gate so a new build can fire.
		s.distLazyOnce = sync.Once{}
		s.distLazyBuilt = false
	}
	s.distLazyMu.Unlock()

	// Fire-and-forget; sync.Once collapses concurrent goroutines into
	// a single build. The Once is reset above (under the mutex) before
	// each rebuild cycle.
	go s.distLazyOnce.Do(func() {
		s.distLazyMu.Lock()
		s.distLazyBuilding = true
		s.distLazyMu.Unlock()

		if s.distanceBuildHook != nil {
			s.distanceBuildHook() // test seam: hold the build window open
		}

		s.mu.Lock()
		s.buildDistanceIndex()
		obsAtBuild := s.totalObs
		s.mu.Unlock()

		// The distance recomputer's snapshot was computed from the index
		// as it was before this build. Refresh it before reporting the
		// index as built, so the handler does not go from 202 to serving
		// that older snapshot for up to one recompute interval.
		s.analyticsRecomputerMu.RLock()
		rc := s.recompDistance
		s.analyticsRecomputerMu.RUnlock()
		if rc != nil {
			rc.RecomputeNow()
		}

		s.distLazyMu.Lock()
		s.distLazyBuilding = false
		s.distLazyBuilt = true
		s.distLazyLastBuilt = time.Now()
		s.distLazyLastObs = obsAtBuild
		s.distLazyMu.Unlock()
	})
}

// buildDistanceIndex precomputes haversine distances for all packets.
// Must be called with s.mu held (Lock).
func (s *PacketStore) buildDistanceIndex() {
	allNodes, pm := s.getCachedNodesAndPM()
	nodeByPk := make(map[string]*nodeInfo, len(allNodes))
	repeaterSet := make(map[string]bool)
	for i := range allNodes {
		n := &allNodes[i]
		nodeByPk[n.PublicKey] = n
		if n.Role == types.RoleRepeater {
			repeaterSet[n.PublicKey] = true
		}
	}

	hops := make([]distHopRecord, 0, len(s.packets))
	paths := make([]distPathRecord, 0, len(s.packets)/2)

	// Per-tx hop resolver shared across the per-tx loop (#1197 perf).
	resolveHop, setContext := s.hopResolverPerTx(pm)
	for _, tx := range s.packets {
		// Per-tx context for hop disambiguation (#1197).
		setContext(buildHopContextPubkeys(tx, pm))
		txHops, txPath := computeDistancesForTx(tx, nodeByPk, repeaterSet, resolveHop)
		if len(txHops) > 0 {
			hops = append(hops, txHops...)
		}
		if txPath != nil {
			paths = append(paths, *txPath)
		}
	}

	s.distHops = hops
	s.distPaths = paths
	log.Printf("[store] Built distance index: %d hop records, %d path records",
		len(s.distHops), len(s.distPaths))
}

// Self-accounting memory estimation constants.
// These estimate the in-memory cost of StoreTx and StoreObs structs including
// map/index overhead. They don't need to be exact — just proportional to actual
// usage and independent of GC state.
//
// Issue #743: Previous estimates missed major per-packet allocations:
// - spTxIndex: O(path²) entries per tx (50-150MB at scale)
// - Per-tx maps: obsKeys, observerSet (~11MB at scale)
// - byPathHop index entries (20-40MB at scale)
// Note: ResolvedPath per-obs overhead eliminated by #800 refactor.
const (
	storeTxBaseBytes  = 384 // StoreTx struct fields + map headers + sync.Once + string headers
	storeObsBaseBytes = 192 // StoreObs struct fields + string headers
	strHdr            = 16  // Go string header (ptr + len) on a 64-bit build; used by GetStoreMemoryBreakdown
	indexEntryBytes   = 48  // average cost of one index map entry (key + pointer + bucket overhead)
	numIndexesPerTx   = 5   // byHash, byTxID, byNode, byPayloadType, nodeHashes entries
	numIndexesPerObs  = 2   // byObsID, byObserver entries

	// Per-tx map overhead (obsKeys + observerSet): map header + initial buckets
	perTxMapsBytes = 200

	// Per path hop: byPathHop index entry (pointer + map bucket)
	perPathHopBytes = 50

	// Per subpath entry in spTxIndex: string key + slice append + pointer
	perSubpathEntryBytes = 40

	// ParsedDecoded caches json.Unmarshal of DecodedJSON as a
	// map[string]interface{}: measured at about 4x the JSON length on a
	// production heap profile. Analytics touch every tx, so it is charged
	// up front rather than when the cache fills.
	decodedCacheFactor = 4

	// Per obs: the tx.obsKeys dedup entry ("observerID|pathJSON" key string
	// plus bucket slot); the key is built by concatenation, so its bytes are
	// a separate allocation from the obs fields.
	obsKeyEntryBytes = 16 + indexEntryBytes + 1
)

// estimateStoreTxBytes returns the estimated memory cost of a StoreTx (excluding observations).
// Includes per-tx maps (obsKeys, observerSet), byPathHop entries, and spTxIndex subpath entries.
func estimateStoreTxBytes(tx *StoreTx) int64 {
	base := int64(storeTxBaseBytes)
	base += int64(len(tx.RawHex) + len(tx.Hash) + len(tx.DecodedJSON) + len(tx.PathJSON))
	base += int64(numIndexesPerTx * indexEntryBytes)
	base += int64(decodedCacheFactor * len(tx.DecodedJSON))

	// Per-tx maps: obsKeys + observerSet
	base += perTxMapsBytes

	// Path-dependent costs
	hops := int64(len(txGetParsedPath(tx)))
	base += hops * perPathHopBytes

	// spTxIndex: O(path²) subpath combinations
	if hops > 1 {
		subpaths := hops * (hops - 1) / 2
		base += subpaths * perSubpathEntryBytes
	}

	return base
}

// estimateStoreTxBytesTypical returns the estimated memory cost of a typical
// transmission with the given number of observations. Used for budget
// calculation during bounded cold load (no actual StoreTx needed).
func estimateStoreTxBytesTypical(numObs int) int64 {
	// Typical tx: ~64 byte hash, ~200 byte decoded JSON, ~40 byte path, 3 hops
	base := int64(storeTxBaseBytes) + 64 + 200 + 40
	base += int64(numIndexesPerTx * indexEntryBytes)
	base += decodedCacheFactor * 200
	base += perTxMapsBytes
	hops := int64(3)
	base += hops * perPathHopBytes
	base += (hops * (hops - 1) / 2) * perSubpathEntryBytes
	// Add observation costs
	obsBase := int64(storeObsBaseBytes) + 30 + 30 + 60 + 25 // observer ID + name + path + timestamp
	obsBase += int64(numIndexesPerObs * indexEntryBytes)
	obsBase += obsKeyEntryBytes + 30 + 60 // dedup key: observer ID + path
	// No per-obs ResolvedPath overhead (#800)
	base += int64(numObs) * obsBase
	return base
}

// estimateStoreObsBytes returns the estimated memory cost of a StoreObs.
// ResolvedPath membership index overhead is tracked separately.
func estimateStoreObsBytes(obs *StoreObs) int64 {
	base := int64(storeObsBaseBytes)
	base += int64(len(obs.PathJSON) + len(obs.ObserverID) + len(obs.ObserverName) +
		len(obs.ObserverIATA) + len(obs.Direction) + len(obs.RawHex))
	base += int64(numIndexesPerObs * indexEntryBytes)
	base += int64(obsKeyEntryBytes + len(obs.ObserverID) + len(obs.PathJSON))
	// ResolvedPath field removed (#800) — no per-obs RP overhead
	return base
}

// estimatedMemoryMB returns current Go heap allocation in MB.
// Kept for stats/debug endpoints only — NOT used in eviction decisions.
// In tests, memoryEstimator can be set to inject a deterministic value.
// Caches the result for 5 seconds because runtime.ReadMemStats() stops the
// world and this is called from stats/debug endpoints that may be polled.
// The cache is per-store (not package-level) so that test helpers constructing
// &PacketStore{...} directly get independent cache state, avoiding
// order-dependent test failures from a shared global cache.
func (s *PacketStore) estimatedMemoryMB() float64 {
	if s.memoryEstimator != nil {
		return s.memoryEstimator()
	}
	s.estMemMu.Lock()
	defer s.estMemMu.Unlock()
	if time.Since(s.estMemAt) > 5*time.Second {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		s.estMemVal = float64(ms.HeapAlloc) / 1048576.0
		s.estMemAt = time.Now()
	}
	return s.estMemVal
}

// trackedMemoryMB returns the self-accounted packet store memory in MB.
func (s *PacketStore) trackedMemoryMB() float64 {
	return float64(s.trackedBytes) / 1048576.0
}

// EvictStale removes packets older than the retention window and/or exceeding
// the memory cap. Must be called with s.mu held (Lock). Returns the number of
// packets evicted.
// evictionCandidateTxIDs determines which tx IDs would be evicted and returns them.
// Must be called under s.mu.Lock (or RLock). Does NOT modify any state.
func (s *PacketStore) evictionCandidateTxIDs() []int {
	if s.retentionHours <= 0 && s.maxMemoryMB <= 0 {
		return nil
	}
	cutoffIdx := 0
	if s.retentionHours > 0 {
		cutoff := time.Now().UTC().Add(-time.Duration(s.retentionHours * float64(time.Hour)))
		for cutoffIdx < len(s.packets) && s.packets[cutoffIdx].FirstSeen.Before(cutoff) {
			cutoffIdx++
		}
	}
	if s.maxMemoryMB > 0 {
		highWatermark := int64(s.maxMemoryMB) * 1048576
		lowWatermark := int64(float64(highWatermark) * 0.85)
		if s.trackedBytes > highWatermark && len(s.packets) > 0 {
			var bytesToEvict int64
			memCutoff := cutoffIdx
			for memCutoff < len(s.packets) && (s.trackedBytes-bytesToEvict) > lowWatermark {
				tx := s.packets[memCutoff]
				bytesToEvict += tx.accountedBytes
				for _, obs := range tx.Observations {
					bytesToEvict += estimateStoreObsBytes(obs)
				}
				memCutoff++
			}
			maxEvict := len(s.packets) / 4
			if maxEvict < 1 {
				maxEvict = 1
			}
			if memCutoff > maxEvict {
				memCutoff = maxEvict
			}
			if memCutoff > cutoffIdx {
				cutoffIdx = memCutoff
			}
		}
	}
	if cutoffIdx == 0 || cutoffIdx > len(s.packets) {
		return nil
	}
	ids := make([]int, cutoffIdx)
	for i := 0; i < cutoffIdx; i++ {
		ids[i] = s.packets[i].ID
	}
	return ids
}

// EvictStaleWithRP runs eviction using pre-fetched resolved pubkeys.
// rpBatch may be nil (in which case resolved pubkey cleanup for byNode/nodeHashes is skipped).
// Must be called under s.mu.Lock.
func (s *PacketStore) EvictStaleWithRP(rpBatch map[int][]string) int {
	return s.evictStaleInternal(rpBatch)
}

// EvictStale runs eviction, fetching resolved pubkeys inline (SQL under lock).
// Prefer RunEviction() which batches the SQL outside the lock.
// Must be called under s.mu.Lock.
func (s *PacketStore) EvictStale() int {
	return s.evictStaleInternal(nil)
}

// mergeChunkIntoPackets merges a background chunk into the packet slice while
// keeping the invariant s.packets is declared with: "sorted by first_seen ASC
// (oldest first; newest at tail)". Retention eviction depends on it, walking
// from the head and stopping at the first transmission inside the window, so a
// slice that is out of order is silently under-evicted rather than noisily
// wrong.
//
// The chunk cannot simply be put in front. Chunks are selected by last_seen,
// so a transmission first heard weeks ago and heard again recently arrives in
// a recent chunk carrying its old FirstSeen. On a production database 2071 of
// the 236080 transmissions in a 14 day window have a first_seen more than a
// day older than their last_seen, 1848 of them more than a week.
//
// Linear on purpose: this runs under s.mu once per chunk, and re-sorting the
// whole slice there would mean sorting hundreds of thousands of packets while
// ingest waits. The chunk itself is sorted first, which is the only
// comparison sort involved and is bounded by one chunk. LoadChunked does its
// own sort once at the end of the initial load; this keeps that invariant true
// for every chunk merged afterwards.
func mergeChunkIntoPackets(chunk, existing []*StoreTx) []*StoreTx {
	less := func(i, j int) bool { return chunk[i].FirstSeen.Before(chunk[j].FirstSeen) }
	if !sort.SliceIsSorted(chunk, less) {
		sort.SliceStable(chunk, less)
	}

	out := make([]*StoreTx, 0, len(chunk)+len(existing))
	i, j := 0, 0
	for i < len(chunk) && j < len(existing) {
		if chunk[i].FirstSeen.Before(existing[j].FirstSeen) {
			out = append(out, chunk[i])
			i++
		} else {
			out = append(out, existing[j])
			j++
		}
	}
	out = append(out, chunk[i:]...)
	out = append(out, existing[j:]...)
	return out
}

func (s *PacketStore) evictStaleInternal(rpBatch map[int][]string) int {
	if s.retentionHours <= 0 && s.maxMemoryMB <= 0 {
		return 0
	}

	cutoffIdx := 0

	// Time-based eviction: find how many packets from the head are too old
	if s.retentionHours > 0 {
		cutoff := time.Now().UTC().Add(-time.Duration(s.retentionHours * float64(time.Hour)))
		for cutoffIdx < len(s.packets) && s.packets[cutoffIdx].FirstSeen.Before(cutoff) {
			cutoffIdx++
		}
	}

	// Memory-based eviction: use self-accounted trackedBytes with watermark hysteresis.
	// High watermark = maxMemoryMB (trigger), low watermark = 85% (stop).
	// Safety cap: never evict more than 25% of packets in a single pass.
	if s.maxMemoryMB > 0 {
		highWatermark := int64(s.maxMemoryMB) * 1048576
		lowWatermark := int64(float64(highWatermark) * 0.85)
		if s.trackedBytes > highWatermark && len(s.packets) > 0 {
			// Evict from head until trackedBytes would drop below low watermark
			var bytesToEvict int64
			memCutoff := cutoffIdx
			for memCutoff < len(s.packets) && (s.trackedBytes-bytesToEvict) > lowWatermark {
				tx := s.packets[memCutoff]
				bytesToEvict += tx.accountedBytes
				for _, obs := range tx.Observations {
					bytesToEvict += estimateStoreObsBytes(obs)
				}
				memCutoff++
			}
			// Safety cap: never evict more than 25% in a single pass
			maxEvict := len(s.packets) / 4
			if maxEvict < 1 {
				maxEvict = 1
			}
			if memCutoff > maxEvict {
				memCutoff = maxEvict
			}
			if memCutoff > cutoffIdx {
				cutoffIdx = memCutoff
			}
		}
	}

	if cutoffIdx == 0 {
		return 0
	}
	if cutoffIdx > len(s.packets) {
		cutoffIdx = len(s.packets)
	}

	evicting := s.packets[:cutoffIdx]
	evictedObs := 0
	var evictedBytes int64

	// Build sets of evicted IDs for batch removal from secondary indexes
	evictedTxIDs := make(map[int]struct{}, cutoffIdx)
	evictedObsIDs := make(map[int]struct{}, cutoffIdx*2)
	// Track which observer IDs and payload types need filtering
	affectedObservers := make(map[string]struct{})
	affectedPayloadTypes := make(map[types.PayloadType]struct{})
	affectedNodes := make(map[string]struct{})

	// First pass: remove from primary indexes (byHash, byTxID, byObsID),
	// collect IDs for batch secondary index cleanup, and handle non-index work
	for _, tx := range evicting {
		delete(s.byHash, tx.Hash)
		delete(s.byTxID, tx.ID)
		evictedTxIDs[tx.ID] = struct{}{}
		evictedBytes += tx.accountedBytes

		for _, obs := range tx.Observations {
			delete(s.byObsID, obs.ID)
			evictedObsIDs[obs.ID] = struct{}{}
			evictedBytes += estimateStoreObsBytes(obs)
			if obs.ObserverID != "" {
				affectedObservers[obs.ObserverID] = struct{}{}
			}
			evictedObs++
		}

		s.untrackAdvertPubkey(tx)
		if tx.PayloadType != nil {
			affectedPayloadTypes[*tx.PayloadType] = struct{}{}
		}

		// Remove from nodeHashes and collect affected node keys.
		// Must mirror indexByNode: process decoded JSON fields AND resolved_path pubkeys.
		evictedFromNode := make(map[string]bool)
		if tx.DecodedJSON != "" {
			if decoded := tx.ParsedDecoded(); decoded != nil {
				for _, field := range []string{"pubKey", "destPubKey", "srcPubKey"} {
					if v, ok := decoded[field].(string); ok && v != "" {
						if hashes, ok := s.nodeHashes[v]; ok {
							delete(hashes, tx.Hash)
							if len(hashes) == 0 {
								delete(s.nodeHashes, v)
							}
						}
						affectedNodes[v] = struct{}{}
						evictedFromNode[v] = true
					}
				}
			}
		}
		// Clean up resolved_path pubkeys from byNode/nodeHashes.
		// Uses pre-fetched batch data when available (no SQL under lock).
		var rpPubkeys []string
		if rpBatch != nil {
			rpPubkeys = rpBatch[tx.ID]
		}
		for _, pk := range rpPubkeys {
			if pk == "" || evictedFromNode[pk] {
				continue
			}
			if hashes, ok := s.nodeHashes[pk]; ok {
				delete(hashes, tx.Hash)
				if len(hashes) == 0 {
					delete(s.nodeHashes, pk)
				}
			}
			affectedNodes[pk] = struct{}{}
			evictedFromNode[pk] = true
		}

		// Remove from resolved pubkey index
		s.removeFromResolvedPubkeyIndex(tx.ID)

		// Remove from subpath index
		removeTxFromSubpathIndexFull(s.spIndex, s.spTxIndex, tx)
	}
	// Sweep raw AND resolved hop keys once per batch (#1908). The hash-only
	// membership index cannot recover full pubkey strings. Reuse the evicted
	// ID set for O(total path-hop entries) work, with no per-tx string storage
	// or repeated scans of shared buckets. DeleteFunc clears discarded pointer
	// slots so backing arrays cannot keep evicted transmissions alive.
	for key, list := range s.byPathHop {
		filtered := slices.DeleteFunc(list, func(tx *StoreTx) bool {
			_, evicted := evictedTxIDs[tx.ID]
			return evicted
		})
		if len(filtered) == 0 {
			delete(s.byPathHop, key)
		} else {
			s.byPathHop[key] = filtered
		}
	}
	s.invalidateRelayStatsCache()

	// Batch-remove from byObserver: single pass per affected observer slice
	for obsID := range affectedObservers {
		obsList := s.byObserver[obsID]
		filtered := obsList[:0]
		for _, o := range obsList {
			if _, evicted := evictedObsIDs[o.ID]; !evicted {
				filtered = append(filtered, o)
			}
		}
		if len(filtered) == 0 {
			delete(s.byObserver, obsID)
		} else {
			s.byObserver[obsID] = filtered
		}
	}

	// Batch-remove from byPayloadType: single pass per affected type slice
	for pt := range affectedPayloadTypes {
		ptList := s.byPayloadType[pt]
		filtered := ptList[:0]
		for _, t := range ptList {
			if _, evicted := evictedTxIDs[t.ID]; !evicted {
				filtered = append(filtered, t)
			}
		}
		if len(filtered) == 0 {
			delete(s.byPayloadType, pt)
		} else {
			s.byPayloadType[pt] = filtered
		}
	}

	// Batch-remove from byNode: single pass per affected node slice
	for nodeKey := range affectedNodes {
		nodeList := s.byNode[nodeKey]
		filtered := nodeList[:0]
		for _, t := range nodeList {
			if _, evicted := evictedTxIDs[t.ID]; !evicted {
				filtered = append(filtered, t)
			}
		}
		if len(filtered) == 0 {
			delete(s.byNode, nodeKey)
		} else {
			s.byNode[nodeKey] = filtered
		}
	}

	// Remove from distance indexes — filter out records referencing evicted txs
	evictedTxSet := make(map[*StoreTx]bool, cutoffIdx)
	for _, tx := range evicting {
		evictedTxSet[tx] = true
	}
	newDistHops := s.distHops[:0]
	for i := range s.distHops {
		if !evictedTxSet[s.distHops[i].tx] {
			newDistHops = append(newDistHops, s.distHops[i])
		}
	}
	s.distHops = newDistHops

	newDistPaths := s.distPaths[:0]
	for i := range s.distPaths {
		if !evictedTxSet[s.distPaths[i].tx] {
			newDistPaths = append(newDistPaths, s.distPaths[i])
		}
	}
	s.distPaths = newDistPaths

	// Trim packets slice
	n := copy(s.packets, s.packets[cutoffIdx:])
	s.packets = s.packets[:n]
	s.totalObs -= evictedObs

	evictCount := cutoffIdx
	atomic.AddInt64(&s.evicted, int64(evictCount))
	s.trackedBytes -= evictedBytes
	if s.trackedBytes < 0 {
		s.trackedBytes = 0
	}
	log.Printf("[store] Evicted %d packets (%d obs, freed ~%.1fMB, tracked ~%.1fMB)",
		evictCount, evictedObs, float64(evictedBytes)/1048576.0, s.trackedMemoryMB())

	// Eviction removes data — all caches may be affected
	s.invalidateCachesFor(cacheInvalidation{eviction: true})

	// Invalidate hash size cache
	s.hashSizeInfoMu.Lock()
	s.hashSizeInfoCache = nil
	s.hashSizeInfoMu.Unlock()

	// Compact resolved pubkey index after eviction sweep
	s.CompactResolvedPubkeyIndex()
	s.CheckResolvedPubkeyIndexSize()

	return evictCount
}

// RunEviction acquires the write lock and runs eviction. Safe to call from
// a goroutine. Returns evicted count.
// Uses a two-phase approach: determines eviction candidates under lock,
// releases lock for batch SQL fetch of resolved pubkeys, then re-acquires
// lock for the actual eviction pass.
func (s *PacketStore) RunEviction(ctx context.Context) int {
	// Phase 1: determine candidates under lock
	s.mu.Lock()
	txIDs := s.evictionCandidateTxIDs()
	s.mu.Unlock()

	// Phase 2: batch-fetch resolved pubkeys from the database (no lock held)
	var rpBatch map[int][]string
	if len(txIDs) > 0 {
		var err error
		rpBatch, err = s.db.ResolvedPubkeysForTransmissions(ctx, txIDs)
		if err != nil {
			if ctx.Err() != nil {
				return 0 // shutting down: skip this round
			}
			// Evict anyway: a nil map just means the index entries for this
			// batch are not cleaned up now (they are bounded and harmless).
			log.Printf("eviction: resolved-pubkey lookup failed, index cleanup skipped for %d transmissions: %v", len(txIDs), err)
			rpBatch = nil
		}
	}

	// Phase 3: actual eviction under write lock, using pre-fetched data
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.EvictStaleWithRP(rpBatch)
}

// StartEvictionTicker starts a background goroutine that runs eviction every
// minute. Returns a stop function.
func (s *PacketStore) StartEvictionTicker(ctx context.Context) func() {
	if s.retentionHours <= 0 && s.maxMemoryMB <= 0 {
		return func() {} // no-op
	}
	ticker := time.NewTicker(1 * time.Minute)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				s.RunEviction(ctx)
			case <-done:
				ticker.Stop()
				return
			}
		}
	}()
	return func() { close(done) }
}

// computeDistancesForTx computes distance records for a single transmission.
// buildHopContextPubkeys collects context pubkeys for hop disambiguation:
// the originator/sender pubkey plus any unambiguous-prefix anchors in the
// path (single-candidate prefixes — strong context). Used by callers of
// pm.resolveWithContext to light up tiers 1 and 2 of the resolver. See #1197.
//
// Returned pubkeys are de-duplicated and lowercased.
func buildHopContextPubkeys(tx *StoreTx, pm *prefixMap) []string {
	if tx == nil || pm == nil {
		return nil
	}
	seen := make(map[string]struct{}, 16)
	out := make([]string, 0, 16)
	add := func(pk string) {
		if pk == "" {
			return
		}
		l := strings.ToLower(pk)
		if _, ok := seen[l]; ok {
			return
		}
		seen[l] = struct{}{}
		out = append(out, l)
	}

	// Sender / originator pubkey from decoded payload. Use the cached
	// ParsedDecoded() (sync.Once-gated) instead of re-unmarshaling — the
	// helper is hot (3 distance sites + analytics topology, all 30k+ tx
	// loops). See #1197 (carmack/adversarial r1).
	if dec := tx.ParsedDecoded(); dec != nil {
		if pk, ok := dec["pubKey"].(string); ok {
			add(pk)
		}
	}

	// Observer pubkey, where available. ObserverID is the observers.id PRIMARY
	// KEY from the MQTT topic — it is NOT guaranteed to be a node pubkey hex
	// (some observers register with arbitrary string ids like "myobserver").
	// Guard against polluting the context with non-pubkey strings: include
	// only when it parses as hex AND is long enough to plausibly be a pubkey
	// prefix. The full prefix-map lookup would also be acceptable, but the
	// hex+length check is O(len) and avoids one map probe per tx on a hot
	// path. See #1197 (adversarial r1 #4).
	if obs := tx.ObserverID; len(obs) >= 4 {
		if _, err := hex.DecodeString(obs); err == nil {
			add(obs)
		}
	}

	// Unambiguous-prefix anchors: any hop in the path whose prefix has exactly
	// one candidate is a strong context signal.
	for _, hop := range txGetParsedPath(tx) {
		h := strings.ToLower(hop)
		if cands, ok := pm.m[h]; ok && len(cands) == 1 {
			add(cands[0].PublicKey)
		}
	}

	return out
}

// hasUpperASCII reports whether s contains any uppercase ASCII letter.
// Used by resolveWithContext tier-1 to skip the strings.ToLower allocation
// when the context pubkeys are already lowercased (the common case — see
// buildHopContextPubkeys / buildAggregateHopContextPubkeys, which lowercase
// on the way in). #1247.
func hasUpperASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			return true
		}
	}
	return false
}

// buildAggregateHopContextPubkeys gathers context across many txs for hot
// loops that resolve hops outside any per-tx scope (subpath/topology
// aggregations). Caller passes the slice of txs to consider; we union the
// per-tx contexts with de-dup. Used by call sites that read from precomputed
// indices (s.spIndex, s.spTxIndex) or that resolve user-supplied hops.
//
// Result is order-independent in semantics; iteration order is deterministic
// only modulo Go's map iteration (acceptable — the resolver's tier-2 averages
// GPS positions and tier-3 picks the lex-smallest pubkey on ties, so context
// order does not affect the chosen candidate).
func buildAggregateHopContextPubkeys(txs []*StoreTx, pm *prefixMap) []string {
	if len(txs) == 0 || pm == nil {
		return nil
	}
	seen := make(map[string]struct{}, 32)
	var out []string
	for _, tx := range txs {
		for _, pk := range buildHopContextPubkeys(tx, pm) {
			if _, ok := seen[pk]; ok {
				continue
			}
			seen[pk] = struct{}{}
			out = append(out, pk)
		}
	}
	return out
}

// hopResolverPerTx returns (resolveHop, setContext). The cache is allocated
// once and cleared between txs; setContext rebinds the per-tx context. Used
// by all per-tx distance/topology loops to avoid 4× duplicate closure
// definitions and per-tx map allocation. See #1197 (adversarial r1 #7,
// carmack r1 #3).
//
// CONCURRENCY: NOT safe for concurrent use. The returned closures share
// mutable captured state — `contextPubkeys` is reassigned by setContext and
// read by resolveHop, and `hopCache` is mutated by both (resolveHop writes
// on miss, setContext clears wholesale). Callers MUST invoke both functions
// from a single goroutine for the lifetime of the (resolveHop, setContext)
// pair. If a future caller fans out per-tx work across goroutines, allocate
// a fresh resolver pair per goroutine. See #1199 item 4.
func (s *PacketStore) hopResolverPerTx(pm *prefixMap) (resolveHop func(string) *nodeInfo, setContext func([]string)) {
	hopCache := make(map[string]*nodeInfo, 16)
	var contextPubkeys []string
	// Hoist the atomic graph.Load() out of the per-hop closure body so we do
	// one Load per resolver instance, not one per resolveHop call (PR #1208
	// carmack #1). Pair (resolveHop, setContext) is documented single-
	// goroutine, single-request scope — pinning the graph here matches that
	// lifetime.
	graph := s.graph.Load()
	resolveHop = func(hop string) *nodeInfo {
		if cached, ok := hopCache[hop]; ok {
			return cached
		}
		r, _, _ := pm.resolveWithContext(hop, contextPubkeys, graph)
		hopCache[hop] = r
		return r
	}
	setContext = func(ctx []string) {
		contextPubkeys = ctx
		clear(hopCache)
	}
	return resolveHop, setContext
}

func computeDistancesForTx(tx *StoreTx, nodeByPk map[string]*nodeInfo, repeaterSet map[string]bool, resolveHop func(string) *nodeInfo) ([]distHopRecord, *distPathRecord) {
	pathHops := txGetParsedPath(tx)
	if len(pathHops) == 0 {
		return nil, nil
	}

	resolved := make([]*nodeInfo, len(pathHops))
	for i, h := range pathHops {
		resolved[i] = resolveHop(h)
	}

	var senderNode *nodeInfo
	if tx.DecodedJSON != "" {
		if dec := tx.ParsedDecoded(); dec != nil {
			if pk, ok := dec["pubKey"].(string); ok && pk != "" {
				senderNode = nodeByPk[pk]
			}
		}
	}

	chain := make([]*nodeInfo, 0, len(pathHops)+1)
	if senderNode != nil && senderNode.HasGPS {
		chain = append(chain, senderNode)
	}
	for _, r := range resolved {
		if r != nil && r.HasGPS {
			chain = append(chain, r)
		}
	}
	if len(chain) < 2 {
		return nil, nil
	}

	hourBucket := ""
	if !tx.FirstSeen.IsZero() {
		hourBucket = tx.FirstSeen.UTC().Format("2006-01-02T15")
	}

	var hopRecords []distHopRecord
	var hopDetails []distHopDetail
	pathDist := 0.0

	for i := 0; i < len(chain)-1; i++ {
		a, b := chain[i], chain[i+1]
		dist := haversineKm(a.Lat, a.Lon, b.Lat, b.Lon)
		if dist > 300 {
			continue
		}

		aRep := repeaterSet[a.PublicKey]
		bRep := repeaterSet[b.PublicKey]
		var hopType string
		if aRep && bRep {
			hopType = "R↔R"
		} else if !aRep && !bRep {
			hopType = "C↔C"
		} else {
			hopType = "C↔R"
		}

		roundedDist := math.Round(dist*100) / 100
		hopRecords = append(hopRecords, distHopRecord{
			FromName: a.Name, FromPk: a.PublicKey,
			ToName: b.Name, ToPk: b.PublicKey,
			Dist: roundedDist, Type: hopType,
			SNR: tx.SNR, Hash: tx.Hash, Timestamp: tx.FirstSeen,
			HourBucket: hourBucket, tx: tx,
		})
		hopDetails = append(hopDetails, distHopDetail{
			FromName: a.Name, FromPk: a.PublicKey,
			ToName: b.Name, ToPk: b.PublicKey,
			Dist: roundedDist,
		})
		pathDist += dist
	}

	if len(hopRecords) == 0 {
		return nil, nil
	}

	pathRec := &distPathRecord{
		Hash: tx.Hash, TotalDist: math.Round(pathDist*100) / 100,
		HopCount: len(hopDetails), Timestamp: tx.FirstSeen,
		Hops: hopDetails, tx: tx,
	}
	return hopRecords, pathRec
}

func filterTxSlice(s []*StoreTx, fn func(*StoreTx) bool) []*StoreTx {
	var result []*StoreTx
	for _, tx := range s {
		if fn(tx) {
			result = append(result, tx)
		}
	}
	return result
}

// countNonPrintable counts characters that are non-printable (< 0x20 except \n, \t)
// or invalid UTF-8 replacement characters. Mirrors the heuristic from #197.
func countNonPrintable(s string) int {
	count := 0
	for _, r := range s {
		if r < 0x20 && r != '\n' && r != '\t' {
			count++
		} else if r == utf8.RuneError {
			count++
		}
	}
	return count
}

// hasGarbageChars returns true if the string contains garbage (non-printable) data.
func hasGarbageChars(s string) bool {
	return s != "" && (!utf8.ValidString(s) || countNonPrintable(s) > 2)
}

// GetChannels returns channel list from in-memory packets (payload_type 5, decoded type CHAN).
func (s *PacketStore) GetChannels(ctx context.Context, region string) []map[string]interface{} {
	cacheKey := region

	s.channelsCacheMu.Lock()
	if s.channelsCacheRes != nil && s.channelsCacheKey == cacheKey && time.Now().Before(s.channelsCacheExp) {
		res := s.channelsCacheRes
		s.channelsCacheMu.Unlock()
		return res
	}
	s.channelsCacheMu.Unlock()

	type txSnapshot struct {
		firstSeen   time.Time
		decodedJSON string
		hasRegion   bool
	}
	var regionObs map[string]bool
	if region != "" {
		regionObs = s.resolveRegionObservers(ctx, region)
	}

	// Copy only the fields needed — release the lock before JSON unmarshal.
	s.mu.RLock()
	grpTxts := s.byPayloadType[5]
	snapshots := make([]txSnapshot, 0, len(grpTxts))
	for _, tx := range grpTxts {
		inRegion := true
		if regionObs != nil {
			inRegion = false
			for _, obs := range tx.Observations {
				if regionObs[obs.ObserverID] {
					inRegion = true
					break
				}
			}
		}
		snapshots = append(snapshots, txSnapshot{
			firstSeen:   tx.FirstSeen,
			decodedJSON: tx.DecodedJSON,
			hasRegion:   inRegion,
		})
	}
	s.mu.RUnlock()

	// JSON unmarshal outside the lock.
	type chanInfo struct {
		Hash         string
		Name         string
		LastMessage  interface{}
		LastSender   interface{}
		MessageCount int
		LastActivity time.Time
	}
	type decodedGrp struct {
		Type    string `json:"type"`
		Channel string `json:"channel"`
		Text    string `json:"text"`
		Sender  string `json:"sender"`
	}
	channelMap := map[string]*chanInfo{}
	for _, snap := range snapshots {
		if !snap.hasRegion {
			continue
		}
		var decoded decodedGrp
		if json.Unmarshal([]byte(snap.decodedJSON), &decoded) != nil {
			continue
		}
		if decoded.Type != "CHAN" {
			continue
		}
		if hasGarbageChars(decoded.Channel) || hasGarbageChars(decoded.Text) {
			continue
		}
		channelName := decoded.Channel
		if channelName == "" {
			// Issue #1373: encrypted-no-key packets decode with channel="".
			// Previously we bucketed them under a literal "unknown" channel
			// which then leaked into /api/channels as a ghost entry next to
			// real channels (especially visible after the operator added a
			// PSK client-side). Skip them — they belong in encrypted-channels
			// analytics, not the user-facing channel list.
			continue
		}
		ch := channelMap[channelName]
		if ch == nil {
			ch = &chanInfo{Hash: channelName, Name: channelName, LastActivity: snap.firstSeen}
			channelMap[channelName] = ch
		}
		ch.MessageCount++
		if snap.firstSeen.After(ch.LastActivity) {
			ch.LastActivity = snap.firstSeen
			if decoded.Text != "" {
				idx := strings.Index(decoded.Text, ": ")
				if idx > 0 {
					ch.LastMessage = decoded.Text[idx+2:]
				} else {
					ch.LastMessage = decoded.Text
				}
				if decoded.Sender != "" {
					ch.LastSender = decoded.Sender
				}
			}
		}
	}

	channels := make([]map[string]interface{}, 0, len(channelMap))
	for _, ch := range channelMap {
		channels = append(channels, map[string]interface{}{
			"hash": ch.Hash, "name": ch.Name,
			"lastMessage": ch.LastMessage, "lastSender": ch.LastSender,
			"messageCount": ch.MessageCount, "lastActivity": ch.LastActivity,
		})
	}

	// #688: scan decoded message text for #hashtag mentions and surface any
	// previously-unseen channel names as discovered channels. We dedup against
	// channelMap (matched by name) so a channel that already has traffic does
	// NOT also appear as discovered.
	discovered := map[string]time.Time{} // name -> lastActivity
	for _, snap := range snapshots {
		if !snap.hasRegion {
			continue
		}
		var decoded decodedGrp
		if json.Unmarshal([]byte(snap.decodedJSON), &decoded) != nil {
			continue
		}
		if decoded.Type != "CHAN" || decoded.Text == "" {
			continue
		}
		if hasGarbageChars(decoded.Text) {
			continue
		}
		for _, tag := range extractHashtagsFromText(decoded.Text) {
			// Skip if already a known/decoded channel (by name with or without '#').
			bare := tag[1:]
			if _, ok := channelMap[tag]; ok {
				continue
			}
			if _, ok := channelMap[bare]; ok {
				continue
			}
			if existing, ok := discovered[tag]; !ok || snap.firstSeen.After(existing) {
				discovered[tag] = snap.firstSeen
			}
		}
	}
	for name, lastActivity := range discovered {
		channels = append(channels, map[string]interface{}{
			"hash":         name,
			"name":         name,
			"lastMessage":  nil,
			"lastSender":   nil,
			"messageCount": 0,
			"lastActivity": lastActivity,
			"discovered":   true,
		})
	}

	s.channelsCacheMu.Lock()
	s.channelsCacheRes = channels
	s.channelsCacheKey = cacheKey
	s.channelsCacheExp = time.Now().Add(15 * time.Second)
	s.channelsCacheMu.Unlock()

	return channels
}

// GetEncryptedChannels returns undecryptable GRP_TXT channels from in-memory packets.
func (s *PacketStore) GetEncryptedChannels(ctx context.Context, region string) []map[string]interface{} {
	var regionObs map[string]bool
	if region != "" {
		regionObs = s.resolveRegionObservers(ctx, region)
	}
	s.mu.RLock()
	grpTxts := s.byPayloadType[5]

	type encInfo struct {
		hash         string
		messageCount int
		lastActivity time.Time
	}
	type grpDec struct {
		Type             string      `json:"type"`
		ChannelHash      interface{} `json:"channelHash"`
		ChannelHashHex   string      `json:"channelHashHex"`
		DecryptionStatus string      `json:"decryptionStatus"`
	}
	channelMap := map[string]*encInfo{}

	for _, tx := range grpTxts {
		if regionObs != nil {
			match := false
			for _, obs := range tx.Observations {
				if regionObs[obs.ObserverID] {
					match = true
					break
				}
			}
			if !match {
				continue
			}
		}
		var decoded grpDec
		if json.Unmarshal([]byte(tx.DecodedJSON), &decoded) != nil {
			continue
		}
		if decoded.Type != "GRP_TXT" || decoded.DecryptionStatus != "no_key" {
			continue
		}
		chHash := decoded.ChannelHashHex
		if chHash == "" {
			if num, ok := decoded.ChannelHash.(float64); ok {
				chHash = fmt.Sprintf("%02X", int(num))
			}
		}
		if chHash == "" {
			chHash = "?"
		}
		ch := channelMap[chHash]
		if ch == nil {
			ch = &encInfo{hash: chHash, lastActivity: tx.FirstSeen}
			channelMap[chHash] = ch
		}
		ch.messageCount++
		if tx.FirstSeen.After(ch.lastActivity) {
			ch.lastActivity = tx.FirstSeen
		}
	}
	s.mu.RUnlock()

	channels := make([]map[string]interface{}, 0, len(channelMap))
	for _, ch := range channelMap {
		channels = append(channels, map[string]interface{}{
			"hash":         "enc_" + ch.hash,
			"name":         "Encrypted (0x" + ch.hash + ")",
			"lastMessage":  nil,
			"lastSender":   nil,
			"messageCount": ch.messageCount,
			"lastActivity": ch.lastActivity,
			"encrypted":    true,
		})
	}
	return channels
}

// GetChannelMessages returns deduplicated messages for a channel from in-memory packets.
func (s *PacketStore) GetChannelMessages(ctx context.Context, channelHash string, limit, offset int, region ...string) ([]map[string]interface{}, int) {
	regionParam := ""
	if len(region) > 0 {
		regionParam = region[0]
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if limit <= 0 {
		limit = 100
	}

	type msgEntry struct {
		Data      map[string]interface{}
		Repeats   int
		Observers []string
	}
	msgMap := map[string]*msgEntry{}
	var msgOrder []string

	regionObs := s.resolveRegionObservers(ctx, regionParam)

	// Iterate type-5 packets oldest-first (byPayloadType is ASC = oldest first)
	type decodedMsg struct {
		Type            string      `json:"type"`
		Channel         string      `json:"channel"`
		Text            string      `json:"text"`
		Sender          string      `json:"sender"`
		SenderTimestamp interface{} `json:"sender_timestamp"`
		PathLen         int         `json:"path_len"`
	}

	grpTxts := s.byPayloadType[5]
	for _, tx := range grpTxts {
		if regionObs != nil {
			match := false
			for _, obs := range tx.Observations {
				if regionObs[obs.ObserverID] {
					match = true
					break
				}
			}
			if !match {
				continue
			}
		}

		if tx.DecodedJSON == "" {
			continue
		}

		var decoded decodedMsg
		if json.Unmarshal([]byte(tx.DecodedJSON), &decoded) != nil {
			continue
		}
		if decoded.Type != "CHAN" {
			continue
		}
		ch := decoded.Channel
		if ch == "" {
			ch = "unknown"
		}
		if ch != channelHash {
			continue
		}

		text := decoded.Text
		sender := decoded.Sender
		if sender == "" && text != "" {
			idx := strings.Index(text, ": ")
			if idx > 0 && idx < 50 {
				sender = text[:idx]
			}
		}

		dedupeKey := sender + ":" + tx.Hash

		if existing, ok := msgMap[dedupeKey]; ok {
			existing.Repeats++
			existing.Data["repeats"] = existing.Repeats
			// Add observer if new
			obsName := tx.ObserverName
			if obsName == "" {
				obsName = tx.ObserverID
			}
			if obsName != "" {
				found := false
				for _, o := range existing.Observers {
					if o == obsName {
						found = true
						break
					}
				}
				if !found {
					existing.Observers = append(existing.Observers, obsName)
					existing.Data["observers"] = existing.Observers
				}
			}
		} else {
			displaySender := sender
			displayText := text
			if text != "" {
				idx := strings.Index(text, ": ")
				if idx > 0 && idx < 50 {
					displaySender = text[:idx]
					displayText = text[idx+2:]
				}
			}

			hops := pathLen(tx.PathJSON)

			var snrVal interface{}
			if tx.SNR != nil {
				snrVal = *tx.SNR
			}

			senderTs := decoded.SenderTimestamp

			// Issue #1366: emit tx.LatestSeen (max observation timestamp,
			// server UTC) as the rendered timestamp — NOT tx.FirstSeen,
			// which stays pinned at the first-ever observation of a hash
			// and lags reality for heartbeat-style retransmissions. Fall
			// back to FirstSeen only when LatestSeen is empty (no obs).
			// sender_timestamp from the decoded payload is NOT used as the
			// rendered field: client RTCs are unreliable. It remains in
			// the response for debug surfaces.
			displayTs := tx.LatestSeen
			if displayTs.IsZero() {
				displayTs = tx.FirstSeen
			}

			observers := make([]string, 0, len(tx.Observations))
			seenObservers := make(map[string]bool, len(tx.Observations))
			for _, o := range tx.Observations {
				name := o.ObserverName
				if name == "" {
					name = o.ObserverID
				}
				if name == "" || seenObservers[name] {
					continue
				}
				seenObservers[name] = true
				observers = append(observers, name)
			}
			if len(observers) == 0 {
				name := tx.ObserverName
				if name == "" {
					name = tx.ObserverID
				}
				if name != "" {
					observers = append(observers, name)
				}
			}
			repeats := len(tx.Observations)
			if repeats < 1 {
				repeats = 1
			}

			entry := &msgEntry{
				Data: map[string]interface{}{
					"sender":           displaySender,
					"text":             displayText,
					"timestamp":        UTCTime{displayTs},
					"first_seen":       UTCTime{tx.FirstSeen},
					"sender_timestamp": senderTs,
					"packetId":         tx.ID,
					"packetHash":       strOrNil(tx.Hash),
					"repeats":          repeats,
					"observers":        observers,
					"hops":             hops,
					"snr":              snrVal,
					"scope_name":       strPtrOrNil(tx.ScopeName),
				},
				Repeats:   repeats,
				Observers: observers,
			}
			msgMap[dedupeKey] = entry
			msgOrder = append(msgOrder, dedupeKey)
		}
	}

	// Issue #1366 follow-up: msgOrder is in tx insertion order
	// (≈ FirstSeen ascending). Re-sort by the rendered timestamp field
	// (= LatestSeen, set above) ascending, so the page tail = newest
	// LatestSeen. Without this, a long-running heartbeat with old
	// FirstSeen but fresh LatestSeen ends up at the head of msgOrder
	// and gets sliced off by the tail selection below.
	sort.SliceStable(msgOrder, func(i, j int) bool {
		ti, _ := msgMap[msgOrder[i]].Data["timestamp"].(UTCTime)
		tj, _ := msgMap[msgOrder[j]].Data["timestamp"].(UTCTime)
		return ti.Before(tj.Time)
	})

	total := len(msgOrder)
	// Return latest messages (tail)
	start := total - limit - offset
	if start < 0 {
		start = 0
	}
	end := total - offset
	if end < 0 {
		end = 0
	}
	if end > total {
		end = total
	}

	messages := make([]map[string]interface{}, 0, end-start)
	for i := start; i < end; i++ {
		messages = append(messages, msgMap[msgOrder[i]].Data)
	}
	return messages, total
}

// GetAnalyticsChannels returns full channel analytics computed from in-memory packets.
func (s *PacketStore) GetAnalyticsChannels(ctx context.Context, region, area string) map[string]interface{} {
	return s.GetAnalyticsChannelsWithWindow(ctx, region, area, TimeWindow{})
}

// GetAnalyticsChannelsWithWindow returns channel analytics for the given region,
// optionally bounded to a time window (issue #842). Zero TimeWindow = all data.
func (s *PacketStore) GetAnalyticsChannelsWithWindow(ctx context.Context, region, area string, window TimeWindow) map[string]interface{} {
	if region == "" && area == "" && window.IsZero() {
		s.analyticsRecomputerMu.RLock()
		rc := s.recompChannels
		s.analyticsRecomputerMu.RUnlock()
		if rc != nil {
			if v := rc.Load(); v != nil {
				if m, ok := v.(map[string]interface{}); ok {
					s.cacheMu.Lock()
					s.cacheHits++
					s.cacheMu.Unlock()
					return m
				}
			}
		}
	}
	cacheKey := region + "|" + area
	if !window.IsZero() {
		cacheKey += "|" + window.CacheKey()
	}
	s.cacheMu.Lock()
	if cached, ok := s.chanCache[cacheKey]; ok && time.Now().Before(cached.expiresAt) {
		s.cacheHits++
		s.cacheMu.Unlock()
		return cached.data
	}
	s.cacheMisses++
	s.cacheMu.Unlock()

	result := s.computeAnalyticsChannels(ctx, region, area, window)

	s.cacheMu.Lock()
	s.chanCache[cacheKey] = &cachedResult{data: result, expiresAt: time.Now().Add(s.rfCacheTTL)}
	s.cacheMu.Unlock()

	return result
}

// channelNameMatchesHash validates that a decrypted channel name hashes to the
// observed single-byte channel hash. This rejects rainbow-table mismatches where
// an observer's lookup table incorrectly maps a hash byte to the wrong name.
// Firmware invariant: channelHash = SHA256(SHA256("#name")[:16])[0]
func channelNameMatchesHash(name string, hashStr string) bool {
	expected, err := strconv.Atoi(hashStr)
	if err != nil {
		return false
	}
	chanName := name
	if !strings.HasPrefix(chanName, "#") {
		chanName = "#" + chanName
	}
	h1 := sha256.Sum256([]byte(chanName))
	h2 := sha256.Sum256(h1[:16])
	return int(h2[0]) == expected
}

// isPlaceholderName returns true if the name is a "chN" placeholder (not a real decrypted name).
func isPlaceholderName(name string) bool {
	if !strings.HasPrefix(name, "ch") {
		return false
	}
	_, err := strconv.Atoi(name[2:])
	return err == nil
}

func (s *PacketStore) computeAnalyticsChannels(ctx context.Context, region, area string, window TimeWindow) map[string]interface{} {
	var areaNodes map[string]bool
	if area != "" {
		areaNodes = s.resolveAreaNodes(ctx, area)
	}

	var regionObs map[string]bool
	if region != "" {
		regionObs = s.resolveRegionObservers(ctx, region)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	type decodedGrp struct {
		Type             string      `json:"type"`
		Channel          string      `json:"channel"`
		ChannelHash      interface{} `json:"channelHash"`
		ChannelHash2     string      `json:"channel_hash"`
		Text             string      `json:"text"`
		Sender           string      `json:"sender"`
		DecryptionStatus string      `json:"decryptionStatus,omitempty"`
	}

	// Convert channelHash (number or string in JSON) to string
	chHashStr := func(v interface{}) string {
		if v == nil {
			return ""
		}
		switch val := v.(type) {
		case string:
			return val
		case float64:
			return strconv.FormatFloat(val, 'f', -1, 64)
		default:
			return fmt.Sprintf("%v", val)
		}
	}

	type chanInfo struct {
		Hash         string
		Name         string
		Messages     int
		Senders      map[string]bool
		LastActivity time.Time
		Encrypted    bool
	}

	channelMap := map[string]*chanInfo{}
	senderCounts := map[string]int{}
	msgLengths := make([]int, 0)
	timeline := map[string]int{} // hour|channelName → count

	grpTxts := s.byPayloadType[5]
	for _, tx := range grpTxts {
		if !window.Includes(tx.FirstSeen) {
			continue
		}
		if regionObs != nil {
			match := false
			for _, obs := range tx.Observations {
				if regionObs[obs.ObserverID] {
					match = true
					break
				}
			}
			if !match {
				continue
			}
		}
		if areaNodes != nil {
			d := tx.ParsedDecoded()
			pk, _ := d["public_key"].(string)
			if pk == "" {
				pk, _ = d["pubKey"].(string)
			}
			if pk == "" || !areaNodes[pk] {
				continue
			}
		}

		var decoded decodedGrp
		if json.Unmarshal([]byte(tx.DecodedJSON), &decoded) != nil {
			continue
		}

		hash := chHashStr(decoded.ChannelHash)
		if hash == "" {
			hash = decoded.ChannelHash2
		}
		if hash == "" {
			hash = "?"
		}
		name := decoded.Channel
		if name == "" {
			name = "ch" + hash
		}
		encrypted := decoded.Text == "" && decoded.Sender == ""

		// Bug #978 fix: validate channel name against hash to reject rainbow-table
		// mismatches. If the claimed channel name doesn't hash to the observed
		// channelHash byte, discard it — UNLESS the ingestor marked the packet
		// `decryptionStatus:"decrypted"`. That flag means the name came from a
		// successful key-based (PSK) decryption, not a rainbow-table lookup, so
		// it is trustworthy regardless of the hashtag-derived hash check. This
		// keeps the firmware-default Public channel (0x11, key-derived hash
		// SHA256(key)[0]=17, not the hashtag scheme's 186) from being wrongly
		// discarded and rendered as "Encrypted (0x11)". See #1729.
		ingestorDecrypted := decoded.DecryptionStatus == "decrypted"
		if name != "" && name != "ch"+hash && !ingestorDecrypted && !channelNameMatchesHash(name, hash) {
			name = "ch" + hash
			encrypted = true
		}

		// Bug #978 fix: always group by hash byte alone — same physical channel,
		// regardless of which observer decrypted it.
		chKey := hash

		ch := channelMap[chKey]
		if ch == nil {
			ch = &chanInfo{Hash: hash, Name: name, Senders: map[string]bool{}, LastActivity: tx.FirstSeen, Encrypted: encrypted}
			channelMap[chKey] = ch
		} else {
			// Upgrade bucket name: if current is placeholder and we have a validated decrypted name
			if isPlaceholderName(ch.Name) && !isPlaceholderName(name) {
				ch.Name = name
			}
		}
		ch.Messages++
		ch.LastActivity = tx.FirstSeen
		if !encrypted {
			ch.Encrypted = false
		}

		if decoded.Sender != "" {
			ch.Senders[decoded.Sender] = true
			senderCounts[decoded.Sender]++
		}
		if decoded.Text != "" {
			msgLengths = append(msgLengths, len(decoded.Text))
		}

		// Timeline
		if !tx.FirstSeen.IsZero() {
			hr := tx.FirstSeen.UTC().Format("2006-01-02T15")
			key := hr + "|" + name
			timeline[key]++
		}
	}

	channelList := make([]map[string]interface{}, 0, len(channelMap))
	decryptable := 0
	for _, c := range channelMap {
		if !c.Encrypted {
			decryptable++
		}
		channelList = append(channelList, map[string]interface{}{
			"hash": c.Hash, "name": c.Name,
			"messages": c.Messages, "senders": len(c.Senders),
			"lastActivity": c.LastActivity, "encrypted": c.Encrypted,
		})
	}
	sort.Slice(channelList, func(i, j int) bool {
		return channelList[i]["messages"].(int) > channelList[j]["messages"].(int)
	})

	// Top senders
	type senderEntry struct {
		name  string
		count int
	}
	senderList := make([]senderEntry, 0, len(senderCounts))
	for n, c := range senderCounts {
		senderList = append(senderList, senderEntry{n, c})
	}
	sort.Slice(senderList, func(i, j int) bool { return senderList[i].count > senderList[j].count })
	topSenders := make([]map[string]interface{}, 0)
	for i, e := range senderList {
		if i >= 15 {
			break
		}
		topSenders = append(topSenders, map[string]interface{}{"name": e.name, "count": e.count})
	}

	// Channel timeline
	type tlEntry struct {
		hour, channel string
		count         int
	}
	var tlList []tlEntry
	for key, count := range timeline {
		parts := strings.SplitN(key, "|", 2)
		if len(parts) == 2 {
			tlList = append(tlList, tlEntry{parts[0], parts[1], count})
		}
	}
	sort.Slice(tlList, func(i, j int) bool { return tlList[i].hour < tlList[j].hour })
	channelTimeline := make([]map[string]interface{}, 0, len(tlList))
	for _, e := range tlList {
		channelTimeline = append(channelTimeline, map[string]interface{}{
			"hour": e.hour, "channel": e.channel, "count": e.count,
		})
	}

	return map[string]interface{}{
		"activeChannels":  len(channelList),
		"decryptable":     decryptable,
		"channels":        channelList,
		"topSenders":      topSenders,
		"channelTimeline": channelTimeline,
		"msgLengths":      msgLengths,
	}
}

// GetAnalyticsRF returns full RF analytics computed from in-memory observations.
func (s *PacketStore) GetAnalyticsRF(ctx context.Context, region, area string) map[string]interface{} {
	return s.GetAnalyticsRFWithWindow(ctx, region, area, TimeWindow{})
}

// GetAnalyticsRFWithWindow returns RF analytics bounded by an optional
// time window (issue #842). Zero TimeWindow = all data (backwards compatible).
func (s *PacketStore) GetAnalyticsRFWithWindow(ctx context.Context, region, area string, window TimeWindow) map[string]interface{} {
	if region == "" && area == "" && window.IsZero() {
		s.analyticsRecomputerMu.RLock()
		rc := s.recompRF
		s.analyticsRecomputerMu.RUnlock()
		if rc != nil {
			if v := rc.Load(); v != nil {
				if m, ok := v.(map[string]interface{}); ok {
					s.cacheMu.Lock()
					s.cacheHits++
					s.cacheMu.Unlock()
					return m
				}
			}
		}
	}
	cacheKey := region + "|" + area
	if !window.IsZero() {
		cacheKey += "|" + window.CacheKey()
	}
	s.cacheMu.Lock()
	if cached, ok := s.rfCache[cacheKey]; ok && time.Now().Before(cached.expiresAt) {
		s.cacheHits++
		s.cacheMu.Unlock()
		return cached.data
	}
	s.cacheMisses++
	s.cacheMu.Unlock()

	result := s.computeAnalyticsRF(ctx, region, area, window)

	s.cacheMu.Lock()
	s.rfCache[cacheKey] = &cachedResult{data: result, expiresAt: time.Now().Add(s.rfCacheTTL)}
	s.cacheMu.Unlock()

	return result
}

// hourBucket is the UTC hour of t as "2006-01-02T15", the key analytics group by.
func hourBucket(t time.Time) string {
	return t.UTC().Format("2006-01-02T15")
}

func (s *PacketStore) computeAnalyticsRF(ctx context.Context, region, area string, window TimeWindow) map[string]interface{} {
	var areaNodes map[string]bool
	if area != "" {
		areaNodes = s.resolveAreaNodes(ctx, area)
	}

	var regionObs map[string]bool
	if region != "" {
		regionObs = s.resolveRegionObservers(ctx, region)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Collect all observations matching the region
	estCap := s.totalObs
	if estCap > 2000000 {
		estCap = 2000000
	}
	snrVals := make([]float64, 0, estCap/2)
	rssiVals := make([]float64, 0, estCap/2)
	packetSizes := make([]int, 0, len(s.packets))
	seenSizeHashes := make(map[string]bool, len(s.packets))
	seenTypeHashes := make(map[string]bool, len(s.packets))
	typeBuckets := map[types.PayloadType]int{}
	hourBuckets := map[string]int{}
	seenHourHash := make(map[string]bool, len(s.packets)) // dedup packets-per-hour by hash+hour
	snrByType := map[string]*struct{ vals []float64 }{}
	sigTime := map[string]*struct {
		snrs  []float64
		count int
	}{}
	scatterAll := make([]struct{ snr, rssi float64 }, 0, estCap/4)
	totalObs := 0
	regionalHashes := make(map[string]bool, len(s.packets))
	var minTimestamp, maxTimestamp time.Time

	if regionObs != nil {
		// Regional: iterate observations from matching observers
		for obsID := range regionObs {
			obsList := s.byObserver[obsID]
			for _, obs := range obsList {
				if !window.Includes(obs.Timestamp) {
					continue
				}
				totalObs++
				tx := s.byTxID[obs.TransmissionID]
				if areaNodes != nil && tx != nil {
					d := tx.ParsedDecoded()
					pk, _ := d["public_key"].(string)
					if pk == "" {
						pk, _ = d["pubKey"].(string)
					}
					if pk == "" || !areaNodes[pk] {
						continue
					}
				}
				hash := ""
				if tx != nil {
					hash = tx.Hash
				}
				if hash != "" {
					regionalHashes[hash] = true
				}

				ts := obs.Timestamp
				if !ts.IsZero() {
					if minTimestamp.IsZero() || ts.Before(minTimestamp) {
						minTimestamp = ts
					}
					if ts.After(maxTimestamp) {
						maxTimestamp = ts
					}
				}

				// SNR/RSSI
				if obs.SNR != nil {
					snrVals = append(snrVals, *obs.SNR)
					typeName := "UNK"
					if tx != nil && tx.PayloadType != nil {
						if tx.PayloadType.Valid() {
							typeName = tx.PayloadType.String()
						} else {
							typeName = fmt.Sprintf("UNK(%d)", *tx.PayloadType)
						}
					}
					if snrByType[typeName] == nil {
						snrByType[typeName] = &struct{ vals []float64 }{}
					}
					snrByType[typeName].vals = append(snrByType[typeName].vals, *obs.SNR)

					if obs.RSSI != nil {
						scatterAll = append(scatterAll, struct{ snr, rssi float64 }{*obs.SNR, *obs.RSSI})
					}

					// Signal over time
					if !ts.IsZero() {
						hr := hourBucket(ts) // or ts.UTC().Format("2006-01-02T15")
						if sigTime[hr] == nil {
							sigTime[hr] = &struct {
								snrs  []float64
								count int
							}{}
						}
						sigTime[hr].snrs = append(sigTime[hr].snrs, *obs.SNR)
						sigTime[hr].count++
					}
				}
				if obs.RSSI != nil {
					rssiVals = append(rssiVals, *obs.RSSI)
				}

				// Packets per hour (unique by hash per hour)
				if !ts.IsZero() {
					hr := hourBucket(ts)
					hk := hash + "|" + hr
					if hash == "" || !seenHourHash[hk] {
						if hash != "" {
							seenHourHash[hk] = true
						}
						hourBuckets[hr]++
					}
				}

				// Packet sizes (unique by hash)
				if hash != "" && !seenSizeHashes[hash] && tx != nil && tx.RawHex != "" {
					seenSizeHashes[hash] = true
					packetSizes = append(packetSizes, len(tx.RawHex)/2)
				}

				// Payload type distribution (unique by hash)
				if hash != "" && !seenTypeHashes[hash] && tx != nil && tx.PayloadType != nil {
					seenTypeHashes[hash] = true
					typeBuckets[*tx.PayloadType]++
				}
			}
		}
	} else {
		// No region: iterate all transmissions and their observations
		for _, tx := range s.packets {
			// Window filter: skip transmissions outside the requested window.
			// We use tx.FirstSeen as the bounding timestamp; per-obs window
			// filter below handles cases where individual obs timestamps differ.
			if !window.Includes(tx.FirstSeen) {
				continue
			}
			if areaNodes != nil {
				d := tx.ParsedDecoded()
				pk, _ := d["public_key"].(string)
				if pk == "" {
					pk, _ = d["pubKey"].(string)
				}
				if pk == "" || !areaNodes[pk] {
					continue
				}
			}
			hash := tx.Hash
			if hash != "" {
				regionalHashes[hash] = true
				if !seenSizeHashes[hash] && tx.RawHex != "" {
					seenSizeHashes[hash] = true
					packetSizes = append(packetSizes, len(tx.RawHex)/2)
				}
				if !seenTypeHashes[hash] && tx.PayloadType != nil {
					seenTypeHashes[hash] = true
					typeBuckets[*tx.PayloadType]++
				}
			}

			// Pre-resolve type name once per transmission
			typeName := "UNK"
			if pt := tx.PayloadType; pt != nil {
				if pt.Valid() {
					typeName = pt.String()
				} else {
					typeName = fmt.Sprintf("UNK(%d)", *pt)
				}
			}

			if len(tx.Observations) > 0 {
				for _, obs := range tx.Observations {
					totalObs++
					ts := obs.Timestamp
					if !ts.IsZero() {
						if minTimestamp.IsZero() || ts.Before(minTimestamp) {
							minTimestamp = ts
						}
						if ts.After(maxTimestamp) {
							maxTimestamp = ts
						}
					}

					if obs.SNR != nil {
						snr := *obs.SNR
						snrVals = append(snrVals, snr)
						entry := snrByType[typeName]
						if entry == nil {
							entry = &struct{ vals []float64 }{}
							snrByType[typeName] = entry
						}
						entry.vals = append(entry.vals, snr)

						if obs.RSSI != nil {
							scatterAll = append(scatterAll, struct{ snr, rssi float64 }{snr, *obs.RSSI})
						}

						if !ts.IsZero() {
							hr := hourBucket(ts)
							st := sigTime[hr]
							if st == nil {
								st = &struct {
									snrs  []float64
									count int
								}{}
								sigTime[hr] = st
							}
							st.snrs = append(st.snrs, snr)
							st.count++
						}
					}
					if obs.RSSI != nil {
						rssiVals = append(rssiVals, *obs.RSSI)
					}

					if !ts.IsZero() {
						hr := hourBucket(ts)
						hk := hash + "|" + hr
						if hash == "" || !seenHourHash[hk] {
							if hash != "" {
								seenHourHash[hk] = true
							}
							hourBuckets[hr]++
						}
					}
				}
			} else {
				// Legacy: transmission without observations
				totalObs++
				if tx.SNR != nil {
					snrVals = append(snrVals, *tx.SNR)
				}
				if tx.RSSI != nil {
					rssiVals = append(rssiVals, *tx.RSSI)
				}
				ts := tx.FirstSeen
				if !ts.IsZero() {
					if minTimestamp.IsZero() || ts.Before(minTimestamp) {
						minTimestamp = ts
					}
					if ts.After(maxTimestamp) {
						maxTimestamp = ts
					}
					hourBuckets[hourBucket(ts)]++
				}
			}
		}
	}

	// Stats helpers
	stddevF64 := func(arr []float64, avg float64) float64 {
		if len(arr) == 0 {
			return 0
		}
		sum := 0.0
		for _, v := range arr {
			d := v - avg
			sum += d * d
		}
		return math.Sqrt(sum / float64(len(arr)))
	}
	minF64 := func(arr []float64) float64 {
		if len(arr) == 0 {
			return 0
		}
		m := arr[0]
		for _, v := range arr[1:] {
			if v < m {
				m = v
			}
		}
		return m
	}
	maxF64 := func(arr []float64) float64 {
		if len(arr) == 0 {
			return 0
		}
		m := arr[0]
		for _, v := range arr[1:] {
			if v > m {
				m = v
			}
		}
		return m
	}
	minInt := func(arr []int) int {
		if len(arr) == 0 {
			return 0
		}
		m := arr[0]
		for _, v := range arr[1:] {
			if v < m {
				m = v
			}
		}
		return m
	}
	maxInt := func(arr []int) int {
		if len(arr) == 0 {
			return 0
		}
		m := arr[0]
		for _, v := range arr[1:] {
			if v > m {
				m = v
			}
		}
		return m
	}

	// Sort snrVals and rssiVals once; reuse sorted order for min/max/median
	// instead of copying+sorting per stat call (#366).
	sort.Float64s(snrVals)
	sort.Float64s(rssiVals)

	snrAvg := 0.0
	if len(snrVals) > 0 {
		sum := 0.0
		for _, v := range snrVals {
			sum += v
		}
		snrAvg = sum / float64(len(snrVals))
	}
	rssiAvg := 0.0
	if len(rssiVals) > 0 {
		sum := 0.0
		for _, v := range rssiVals {
			sum += v
		}
		rssiAvg = sum / float64(len(rssiVals))
	}

	// Packets per hour
	type hourCount struct {
		Hour  string `json:"hour"`
		Count int    `json:"count"`
	}
	hourKeys := make([]string, 0, len(hourBuckets))
	for k := range hourBuckets {
		hourKeys = append(hourKeys, k)
	}
	sort.Strings(hourKeys)
	packetsPerHour := make([]hourCount, len(hourKeys))
	for i, k := range hourKeys {
		packetsPerHour[i] = hourCount{Hour: k, Count: hourBuckets[k]}
	}

	// Payload types
	type ptEntry struct {
		Type  types.PayloadType `json:"type"`
		Name  string            `json:"name"`
		Count int               `json:"count"`
	}
	payloadTypes := make([]ptEntry, 0, len(typeBuckets))
	for t, c := range typeBuckets {
		pt := t
		payloadTypes = append(payloadTypes, ptEntry{Type: t, Name: resolvePayloadTypeName(&pt), Count: c})
	}
	sort.Slice(payloadTypes, func(i, j int) bool {
		if payloadTypes[i].Count != payloadTypes[j].Count {
			return payloadTypes[i].Count > payloadTypes[j].Count
		}
		return payloadTypes[i].Type < payloadTypes[j].Type
	})

	// SNR by type
	type snrTypeEntry struct {
		Name  string  `json:"name"`
		Count int     `json:"count"`
		Avg   float64 `json:"avg"`
		Min   float64 `json:"min"`
		Max   float64 `json:"max"`
	}
	snrByTypeArr := make([]snrTypeEntry, 0, len(snrByType))
	for name, d := range snrByType {
		sum := 0.0
		for _, v := range d.vals {
			sum += v
		}
		snrByTypeArr = append(snrByTypeArr, snrTypeEntry{
			Name: name, Count: len(d.vals),
			Avg: sum / float64(len(d.vals)),
			Min: minF64(d.vals), Max: maxF64(d.vals),
		})
	}
	sort.Slice(snrByTypeArr, func(i, j int) bool { return snrByTypeArr[i].Count > snrByTypeArr[j].Count })

	// Signal over time
	type sigTimeEntry struct {
		Hour   string  `json:"hour"`
		Count  int     `json:"count"`
		AvgSnr float64 `json:"avgSnr"`
	}
	sigKeys := make([]string, 0, len(sigTime))
	for k := range sigTime {
		sigKeys = append(sigKeys, k)
	}
	sort.Strings(sigKeys)
	signalOverTime := make([]sigTimeEntry, len(sigKeys))
	for i, k := range sigKeys {
		d := sigTime[k]
		sum := 0.0
		for _, v := range d.snrs {
			sum += v
		}
		signalOverTime[i] = sigTimeEntry{Hour: k, Count: d.count, AvgSnr: sum / float64(d.count)}
	}

	// Scatter (downsample to 500)
	type scatterPoint struct {
		SNR  float64 `json:"snr"`
		RSSI float64 `json:"rssi"`
	}
	scatterStep := 1
	if len(scatterAll) > 500 {
		scatterStep = len(scatterAll) / 500
	}
	scatterData := make([]scatterPoint, 0, 500)
	for i, p := range scatterAll {
		if i%scatterStep == 0 {
			scatterData = append(scatterData, scatterPoint{SNR: p.snr, RSSI: p.rssi})
		}
	}

	// Histograms
	buildHistogramF64 := func(values []float64, bins int) map[string]interface{} {
		if len(values) == 0 {
			return map[string]interface{}{"bins": []interface{}{}, "min": 0, "max": 0}
		}
		mn, mx := minF64(values), maxF64(values)
		rng := mx - mn
		if rng == 0 {
			rng = 1
		}
		binWidth := rng / float64(bins)
		counts := make([]int, bins)
		for _, v := range values {
			idx := int((v - mn) / binWidth)
			if idx >= bins {
				idx = bins - 1
			}
			counts[idx]++
		}
		binArr := make([]map[string]interface{}, bins)
		for i, c := range counts {
			binArr[i] = map[string]interface{}{"x": mn + float64(i)*binWidth, "w": binWidth, "count": c}
		}
		return map[string]interface{}{"bins": binArr, "min": mn, "max": mx}
	}
	buildHistogramInt := func(values []int, bins int) map[string]interface{} {
		if len(values) == 0 {
			return map[string]interface{}{"bins": []interface{}{}, "min": 0, "max": 0}
		}
		mn, mx := float64(minInt(values)), float64(maxInt(values))
		rng := mx - mn
		if rng == 0 {
			rng = 1
		}
		binWidth := rng / float64(bins)
		counts := make([]int, bins)
		for _, v := range values {
			idx := int((float64(v) - mn) / binWidth)
			if idx >= bins {
				idx = bins - 1
			}
			counts[idx]++
		}
		binArr := make([]map[string]interface{}, bins)
		for i, c := range counts {
			binArr[i] = map[string]interface{}{"x": mn + float64(i)*binWidth, "w": binWidth, "count": c}
		}
		return map[string]interface{}{"bins": binArr, "min": mn, "max": mx}
	}

	snrHistogram := buildHistogramF64(snrVals, 20)
	rssiHistogram := buildHistogramF64(rssiVals, 20)
	sizeHistogram := buildHistogramInt(packetSizes, 25)

	// Time span from min/max timestamps tracked during first pass
	timeSpanHours := 0.0
	if !minTimestamp.IsZero() && maxTimestamp.After(minTimestamp) {
		timeSpanHours = float64(maxTimestamp.UnixMilli()-minTimestamp.UnixMilli()) / 3600000.0
	}

	// Avg packet size
	avgPktSize := 0
	if len(packetSizes) > 0 {
		sum := 0
		for _, v := range packetSizes {
			sum += v
		}
		avgPktSize = sum / len(packetSizes)
	}

	// snrVals and rssiVals are already sorted — read min/max/median directly.
	snrStats := map[string]interface{}{"min": 0.0, "max": 0.0, "avg": 0.0, "median": 0.0, "stddev": 0.0}
	if len(snrVals) > 0 {
		snrStats = map[string]interface{}{
			"min": snrVals[0], "max": snrVals[len(snrVals)-1],
			"avg": snrAvg, "median": snrVals[len(snrVals)/2],
			"stddev": stddevF64(snrVals, snrAvg),
		}
	}
	rssiStats := map[string]interface{}{"min": 0.0, "max": 0.0, "avg": 0.0, "median": 0.0, "stddev": 0.0}
	if len(rssiVals) > 0 {
		rssiStats = map[string]interface{}{
			"min": rssiVals[0], "max": rssiVals[len(rssiVals)-1],
			"avg": rssiAvg, "median": rssiVals[len(rssiVals)/2],
			"stddev": stddevF64(rssiVals, rssiAvg),
		}
	}

	return map[string]interface{}{
		"totalPackets":       len(snrVals),
		"totalAllPackets":    totalObs,
		"totalTransmissions": len(regionalHashes),
		"snr":                snrStats,
		"rssi":               rssiStats,
		"snrValues":          snrHistogram,
		"rssiValues":         rssiHistogram,
		"packetSizes":        sizeHistogram,
		"minPacketSize":      minInt(packetSizes),
		"maxPacketSize":      maxInt(packetSizes),
		"avgPacketSize":      avgPktSize,
		"packetsPerHour":     packetsPerHour,
		"payloadTypes":       payloadTypes,
		"snrByType":          snrByTypeArr,
		"signalOverTime":     signalOverTime,
		"scatterData":        scatterData,
		"timeSpanHours":      timeSpanHours,
	}
}

// --- Topology Analytics ---

type nodeInfo struct {
	PublicKey        string
	Name             string
	Role             types.NodeRole
	Lat              float64
	Lon              float64
	HasGPS           bool
	LastSeen         time.Time
	FirstSeen        time.Time // RFC3339; populated by buildNodeInfoMap for callers that need it (e.g. /api/nodes/{pk}/reach)
	ObservationCount int       // count of advertisements/observations; used for tier-3 tiebreak in resolveWithContext
}

// getAllNodes loads every node. It reads the database, so the caller must not
// hold s.mu.
func (s *PacketStore) getAllNodes(ctx context.Context) ([]nodeInfo, error) {
	rows, err := s.db.ListAllNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("load nodes: %w", err)
	}
	nodes := make([]nodeInfo, len(rows))
	for i, r := range rows {
		n := nodeInfo{
			PublicKey:        r.PublicKey,
			ObservationCount: r.AdvertCount,
		}
		if r.Name != nil {
			n.Name = *r.Name
		}
		if r.Role != nil {
			n.Role = *r.Role
		}
		if r.LastSeen != nil {
			n.LastSeen = r.LastSeen.UTC()
		}
		if r.FirstSeen != nil {
			n.FirstSeen = r.FirstSeen.UTC()
		}
		if r.Lat != nil && r.Lon != nil {
			n.Lat, n.Lon = *r.Lat, *r.Lon
			n.HasGPS = !(n.Lat == 0 && n.Lon == 0)
		}
		nodes[i] = n
	}
	return nodes, nil
}

type prefixMap struct {
	m map[string][]nodeInfo
	// nonRelay holds lowercase pubkeys of observer-known nodes that have
	// advertised `repeat:off` in their MQTT /status message (issue #1290).
	// Such nodes are pure listeners and must never be selected as a
	// path-hop candidate by resolveWithContext, since by firmware
	// contract they do not forward packets. nil/empty preserves the
	// pre-#1290 behavior (every prefix-matching node is a candidate).
	nonRelay map[string]struct{}
}

// markNonRelay registers a set of lowercase pubkeys as listener-only.
// Called by the server when wiring the prefix map after reading the
// observers table's can_relay column. Issue #1290.
func (pm *prefixMap) markNonRelay(pubkeys []string) {
	if pm == nil {
		return
	}
	if pm.nonRelay == nil {
		pm.nonRelay = make(map[string]struct{}, len(pubkeys))
	}
	for _, pk := range pubkeys {
		pm.nonRelay[strings.ToLower(pk)] = struct{}{}
	}
}

// maxPrefixLen caps prefix map entries. MeshCore path hops use 2–6 char
// prefixes; 8 gives comfortable headroom while cutting map size from ~31×N
// entries to ~7×N (+ 1 full-key entry per node for exact-match lookups).
const maxPrefixLen = 8

func buildPrefixMap(nodes []nodeInfo) *prefixMap {
	pm := &prefixMap{m: make(map[string][]nodeInfo, len(nodes)*(maxPrefixLen+1))}
	for _, n := range nodes {
		if !n.Role.RelaysTraffic() {
			continue
		}
		pk := strings.ToLower(n.PublicKey)
		maxLen := maxPrefixLen
		if maxLen > len(pk) {
			maxLen = len(pk)
		}
		for l := 2; l <= maxLen; l++ {
			pfx := pk[:l]
			pm.m[pfx] = append(pm.m[pfx], n)
		}
		// Always add full pubkey so exact-match lookups work.
		if len(pk) > maxPrefixLen {
			pm.m[pk] = append(pm.m[pk], n)
		}
	}
	return pm
}

// nodeSnapshot is an immutable node list with its prefix map. It is replaced
// whole, never modified, so readers need no lock.
type nodeSnapshot struct {
	nodes []nodeInfo
	pm    *prefixMap
}

var emptyNodePM = buildPrefixMap(nil)

// getCachedNodesAndPM returns the latest node list and prefix map. It does no
// I/O and takes no lock, so it is safe under s.mu.
func (s *PacketStore) getCachedNodesAndPM() ([]nodeInfo, *prefixMap) {
	if snap := s.nodeSnap.Load(); snap != nil {
		return snap.nodes, snap.pm
	}
	return nil, emptyNodePM
}

// RefreshNodeCache reloads the nodes and the non-relay observer set from the
// database and publishes a new snapshot. Call it with no lock held. On error
// the previous snapshot stays in place.
func (s *PacketStore) RefreshNodeCache(ctx context.Context) error {
	nodes, err := s.getAllNodes(ctx)
	if err != nil {
		return err
	}
	pm := buildPrefixMap(nodes)
	// Issue #1290: exclude observers that advertised `repeat:off` from the
	// path-hop candidates. A failure here is non-fatal: log it and proceed
	// with an empty filter, which is the pre-#1290 behavior.
	if pks, err := s.db.GetNonRelayObserverPubkeys(ctx); err == nil {
		pm.markNonRelay(pks)
	} else {
		log.Printf("[store] observers.can_relay read failed; path-hop disambiguator will not filter listener-only observers: %v", err)
	}
	s.nodeSnap.Store(&nodeSnapshot{nodes: nodes, pm: pm})
	return nil
}

// RunNodeCacheRefresher refreshes the snapshot every 30 seconds and whenever
// InvalidateNodeCache is called, until ctx is cancelled.
func (s *PacketStore) RunNodeCacheRefresher(ctx context.Context) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-s.nodeRefreshCh:
		}
		rctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		err := s.RefreshNodeCache(rctx)
		cancel()
		if err != nil && ctx.Err() == nil {
			log.Printf("[store] node cache refresh: %v", err)
		}
	}
}

// InvalidateNodeCache asks the refresher to reload soon. It never blocks and
// never touches the database, so it is safe from any caller.
func (s *PacketStore) InvalidateNodeCache() {
	select {
	case s.nodeRefreshCh <- struct{}{}:
	default: // a refresh is already pending
	}
}

// relayCandidates returns the nodes whose pubkey starts with hop and that may
// appear as a path hop.
//
// Issue #1290: observer-known listener-only nodes are dropped from the
// candidate set. By firmware contract a node that advertises `repeat:off` in
// its MQTT /status will never relay a packet, so it cannot legitimately be a
// hop in someone else's path. Filtering shrinks ambiguous candidate sets
// without affecting any upstream caller (only no_match becomes more likely
// when the only matching prefix belonged to a listener). Empty pm.nonRelay
// preserves the pre-#1290 behavior exactly (back-compat).
func (pm *prefixMap) relayCandidates(hop string) []nodeInfo {
	candidates := pm.m[strings.ToLower(hop)]
	if len(pm.nonRelay) == 0 || len(candidates) == 0 {
		return candidates
	}
	filtered := candidates[:0:0]
	for i := range candidates {
		if _, isListener := pm.nonRelay[strings.ToLower(candidates[i].PublicKey)]; isListener {
			continue
		}
		filtered = append(filtered, candidates[i])
	}
	return filtered
}

func (pm *prefixMap) resolve(hop string) *nodeInfo {
	h := strings.ToLower(hop)
	candidates := pm.m[h]
	if len(candidates) == 0 {
		return nil
	}
	if len(candidates) == 1 {
		return &candidates[0]
	}
	// Multiple candidates: prefer one with GPS
	for i := range candidates {
		if candidates[i].HasGPS {
			return &candidates[i]
		}
	}
	return &candidates[0]
}

// resolveWithContext resolves a hop prefix using the neighbor affinity graph
// for disambiguation when multiple candidates match. It applies a 4-tier
// priority:
//
//	(1) "neighbor_affinity"           — graph score vs context nodes,
//	                                    requires affinity ≥3× runner-up and
//	                                    affinityMinObservations
//	(2) "geo_proximity"               — geographic proximity to GPS context
//	                                    centroid (only fires when at least
//	                                    one context node has GPS)
//	(3) "gps_preference"              — among GPS-having candidates, pick
//	                                    highest ObservationCount; lex-pubkey
//	                                    tiebreak for determinism
//	(4) "observation_count_fallback"  — no GPS available; pick highest
//	                                    ObservationCount; lex-pubkey tiebreak
//
// (Pre-PR #1197/#1198 the tier-3 step was first-GPS-wins and tier-4 was
// first-slice-element. Both now use observation count + lex tiebreak; the
// returned method label was renamed accordingly.)
//
// contextPubkeys are pubkeys of nodes that provide context for disambiguation
// (e.g., the originator, observer, or adjacent hops in the path).
// graph may be nil, in which case tier-1 is skipped.
func (pm *prefixMap) resolveWithContext(hop string, contextPubkeys []string, graph *NeighborGraph) (*nodeInfo, string, float64) {
	candidates := pm.relayCandidates(hop)
	if len(candidates) == 0 {
		return nil, "no_match", 0
	}
	if len(candidates) == 1 {
		return &candidates[0], "unique_prefix", 1.0
	}

	// Priority 1: Affinity graph score
	//
	// NOTE: We use raw Score() (count × time-decay) here rather than Jaccard
	// similarity. Jaccard is used at the graph builder level (disambiguate() in
	// neighbor_graph.go) to resolve ambiguous edges by comparing neighbor-set
	// overlap. Here, edges are already resolved — we just need to pick the
	// highest-affinity candidate among them. Raw score is appropriate because
	// it reflects both observation frequency and recency, which are the right
	// signals for "which candidate is this hop most likely referring to."
	//
	// Issue #1229 (Option C): the raw score is further multiplied by
	// e.Confidence() — a source-diversity factor in (0,1] derived from the
	// number of distinct observers that contributed to the edge. Edges seen
	// by a single observer are discounted to 1/3 weight; edges seen by ≥3
	// observers saturate at full weight. This stacks with the geo-rejection
	// filter merged for #1228 to give two independent lines of defense
	// against cross-region prefix-collision pollution. Backward-compatible
	// with the persistence format: legacy edges with empty Observers sets
	// fall back to single-observer weight.
	if graph != nil && len(contextPubkeys) > 0 {
		type scored struct {
			idx   int
			score float64
			count int // observation count of the best-scoring edge
		}
		now := time.Now()
		// PERF (#1247): hoist per-context work out of the candidate loop.
		// The previous shape ran graph.Neighbors(ToLower(ctxPK)) and
		// re-lowercased candPK on every (cand, ctxPK) pair, then used
		// strings.EqualFold to compare two already-lowercased pubkeys.
		// At analytics scale (5k+ contextPubkeys, ~30k resolveHop calls)
		// this dominated computeAnalyticsTopology / computeAnalyticsRF
		// CPU time (37% / 55% of those endpoints respectively per
		// pprof). The new shape:
		//   1. Lowercases ctx pubkeys at most once per call (skipped
		//      entirely when the input is already lowercased — the
		//      common case for analytics callers that go through
		//      buildHopContextPubkeys).
		//   2. Lowercases candidate pubkeys at most once per call and
		//      uses raw == comparisons against NeighborEdge.NodeA/NodeB
		//      (which makeEdgeKey already lowercases).
		//   3. Loops outer-ctx / inner-edge / matched-cand-lookup. The
		//      previous shape was outer-cand / inner-ctx / inner-edge,
		//      which called graph.Neighbors(ctxPK) — taking the graph
		//      RLock each time — once per (cand, ctx) pair instead of
		//      once per ctx.
		lowerCtx := contextPubkeys
		needLower := false
		for _, p := range contextPubkeys {
			if hasUpperASCII(p) {
				needLower = true
				break
			}
		}
		if needLower {
			lowerCtx = make([]string, len(contextPubkeys))
			for i, p := range contextPubkeys {
				lowerCtx[i] = strings.ToLower(p)
			}
		}
		candPKs := make([]string, len(candidates))
		bestScores := make([]float64, len(candidates))
		bestCounts := make([]int, len(candidates))
		needLowerCand := false
		for i, c := range candidates {
			if hasUpperASCII(c.PublicKey) {
				needLowerCand = true
				break
			}
			candPKs[i] = c.PublicKey
		}
		if needLowerCand {
			for i, c := range candidates {
				candPKs[i] = strings.ToLower(c.PublicKey)
			}
		}
		candByPK := make(map[string]int, len(candidates))
		for i, pk := range candPKs {
			candByPK[pk] = i
		}
		for _, ctxPK := range lowerCtx {
			for _, e := range graph.Neighbors(ctxPK) {
				if e.Ambiguous {
					continue
				}
				otherPK := e.NodeA
				if otherPK == ctxPK {
					otherPK = e.NodeB
				}
				ci, ok := candByPK[otherPK]
				if !ok {
					continue
				}
				s := e.Score(now) * e.Confidence()
				if s > bestScores[ci] {
					bestScores[ci] = s
					bestCounts[ci] = e.Count
				}
			}
		}
		var scores []scored
		for i, s := range bestScores {
			if s > 0 {
				scores = append(scores, scored{i, s, bestCounts[i]})
			}
		}

		if len(scores) >= 1 {
			// Sort descending
			for i := 0; i < len(scores)-1; i++ {
				for j := i + 1; j < len(scores); j++ {
					if scores[j].score > scores[i].score {
						scores[i], scores[j] = scores[j], scores[i]
					}
				}
			}
			best := scores[0]
			// Require both score ratio ≥ 3× AND minimum observations (mirrors
			// disambiguate() in neighbor_graph.go which checks affinityMinObservations).
			if best.count >= affinityMinObservations &&
				(len(scores) == 1 || best.score >= affinityConfidenceRatio*scores[1].score) {
				return &candidates[best.idx], "neighbor_affinity", best.score
			}
			// Scores too close — fall through to lower-priority strategies
		}
	}

	// Priority 2: Geographic proximity (if context pubkeys have GPS and candidates have GPS)
	if len(contextPubkeys) > 0 {
		// Find GPS positions of context nodes from the prefix map or candidates
		// We need nodeInfo for context pubkeys — look them up
		var contextLat, contextLon float64
		var contextGPSCount int
		for _, ctxPK := range contextPubkeys {
			ctxLower := strings.ToLower(ctxPK)
			if infos, ok := pm.m[ctxLower]; ok && len(infos) == 1 && infos[0].HasGPS {
				contextLat += infos[0].Lat
				contextLon += infos[0].Lon
				contextGPSCount++
			}
		}
		if contextGPSCount > 0 {
			contextLat /= float64(contextGPSCount)
			contextLon /= float64(contextGPSCount)

			bestIdx := -1
			bestDist := math.MaxFloat64
			for i, cand := range candidates {
				if !cand.HasGPS {
					continue
				}
				d := geoDistApprox(contextLat, contextLon, cand.Lat, cand.Lon)
				if d < bestDist {
					bestDist = d
					bestIdx = i
				}
			}
			if bestIdx >= 0 {
				return &candidates[bestIdx], "geo_proximity", 0
			}
		}
	}

	// Priority 3: GPS preference. Among GPS-having candidates, prefer the one
	// with the highest observation count (recent/active evidence) rather than
	// slice/DB-insertion order. Ties on count are broken by lexicographically
	// smallest PublicKey for full determinism. See #1197.
	bestGPSIdx := -1
	for i := range candidates {
		if !candidates[i].HasGPS {
			continue
		}
		if bestGPSIdx < 0 || betterByObsCount(&candidates[i], &candidates[bestGPSIdx]) {
			bestGPSIdx = i
		}
	}
	if bestGPSIdx >= 0 {
		return &candidates[bestGPSIdx], "gps_preference", 0
	}

	// Priority 4: Fallback — pick the candidate with the highest observation
	// count (no GPS available on any candidate). Avoids slice-order
	// arbitrariness. Ties on count are broken by lexicographically smallest
	// PublicKey. Method label "observation_count_fallback" — the previous
	// "first_match" was misleading after the tier-4 algorithm changed in
	// PR #1198 (adversarial r1 #2).
	bestIdx := 0
	for i := 1; i < len(candidates); i++ {
		if betterByObsCount(&candidates[i], &candidates[bestIdx]) {
			bestIdx = i
		}
	}
	return &candidates[bestIdx], "observation_count_fallback", 0
}

// betterByObsCount reports whether candidate a should beat b under the
// tier-3/4 selection rule: higher ObservationCount wins; ties go to the
// lexicographically smaller PublicKey for determinism. Pointer receivers
// avoid value-copying nodeInfo (string + 2 floats + time.Time + int) on
// the hot resolve path. See #1197 (adversarial r1 #6, carmack r1 #4).
func betterByObsCount(a, b *nodeInfo) bool {
	if a.ObservationCount != b.ObservationCount {
		return a.ObservationCount > b.ObservationCount
	}
	return a.PublicKey < b.PublicKey
}

// geoDistApprox returns an approximate distance between two lat/lon points
// (equirectangular approximation, sufficient for relative comparison).
func geoDistApprox(lat1, lon1, lat2, lon2 float64) float64 {
	dLat := (lat2 - lat1) * math.Pi / 180
	dLon := (lon2 - lon1) * math.Pi / 180 * math.Cos((lat1+lat2)/2*math.Pi/180)
	return math.Sqrt(dLat*dLat + dLon*dLon)
}

func parsePathJSON(pathJSON string) []string {
	if pathJSON == "" || pathJSON == "[]" {
		return nil
	}
	var hops []string
	if json.Unmarshal([]byte(pathJSON), &hops) != nil {
		return nil
	}
	return hops
}

func (s *PacketStore) GetAnalyticsTopology(ctx context.Context, region, area string) map[string]interface{} {
	return s.GetAnalyticsTopologyWithWindow(ctx, region, area, TimeWindow{})
}

// GetAnalyticsTopologyWithWindow — see issue #842.
// For default (region="", area="", zero window), prefer the steady-state
// recomputer snapshot if registered (issue #1240).
func (s *PacketStore) GetAnalyticsTopologyWithWindow(ctx context.Context, region, area string, window TimeWindow) map[string]interface{} {
	if region == "" && area == "" && window.IsZero() {
		s.analyticsRecomputerMu.RLock()
		rc := s.recompTopology
		s.analyticsRecomputerMu.RUnlock()
		if rc != nil {
			if v := rc.Load(); v != nil {
				if m, ok := v.(map[string]interface{}); ok {
					s.cacheMu.Lock()
					s.cacheHits++
					s.cacheMu.Unlock()
					return m
				}
			}
		}
	}
	cacheKey := region + "|" + area
	if !window.IsZero() {
		cacheKey += "|" + window.CacheKey()
	}
	s.cacheMu.Lock()
	if cached, ok := s.topoCache[cacheKey]; ok && time.Now().Before(cached.expiresAt) {
		s.cacheHits++
		s.cacheMu.Unlock()
		return cached.data
	}
	s.cacheMisses++
	s.cacheMu.Unlock()

	result := s.computeAnalyticsTopology(ctx, region, area, window)

	s.cacheMu.Lock()
	s.topoCache[cacheKey] = &cachedResult{data: result, expiresAt: time.Now().Add(s.rfCacheTTL)}
	s.cacheMu.Unlock()

	return result
}

func (s *PacketStore) computeAnalyticsTopology(ctx context.Context, region, area string, window TimeWindow) map[string]interface{} {
	var areaNodes map[string]bool
	if area != "" {
		areaNodes = s.resolveAreaNodes(ctx, area)
	}
	var regionObs map[string]bool
	if region != "" {
		regionObs = s.resolveRegionObservers(ctx, region)
	}

	// Use DB 7-day active node count (matches /api/stats totalNodes).
	// Read before taking s.mu: no DB access under the lock.
	uniqueNodes := 0
	if s.db != nil {
		if stats, err := s.db.GetEntityCounts(ctx, time.Now().Add(-7*24*time.Hour)); err == nil {
			uniqueNodes = stats.ActiveNodes
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	allNodes, pm := s.getCachedNodesAndPM()
	_ = allNodes // only pm is needed for topology

	// Materialize the filtered tx slice ONCE — both the context-build pass
	// and the main aggregation pass need the same window+region predicate.
	// Two scans of s.packets re-running identical predicates is wasteful at
	// the 30k+ packet hot-path scale (#1199 item 2). One filter, two passes
	// over the result.
	filteredTxs := make([]*StoreTx, 0, len(s.packets))
	for _, tx := range s.packets {
		if !window.Includes(tx.FirstSeen) {
			continue
		}
		if regionObs != nil {
			match := false
			for _, obs := range tx.Observations {
				if regionObs[obs.ObserverID] {
					match = true
					break
				}
			}
			if !match {
				continue
			}
		}
		filteredTxs = append(filteredTxs, tx)
	}

	// Pre-pass: build the full hop-disambiguation context from all in-window
	// txs BEFORE any resolveHop call. The earlier shape — populating
	// contextPubkeys lazily during the main scan and reading it from a
	// closure — was correct only because the current code never calls
	// resolveHop inside the scan loop. A future maintainer who adds such a
	// call inside the loop would silently get partial context AND a
	// stale-cached result for any hop seen before the context grew. Two
	// explicit passes remove the hazard. See #1197 (carmack/adversarial r1).
	var contextPubkeys []string
	{
		seen := make(map[string]struct{}, 64)
		for _, tx := range filteredTxs {
			for _, pk := range buildHopContextPubkeys(tx, pm) {
				if _, ok := seen[pk]; ok {
					continue
				}
				seen[pk] = struct{}{}
				contextPubkeys = append(contextPubkeys, pk)
			}
		}
	}

	hopCache := make(map[string]*nodeInfo)
	graph := s.graph.Load() // hoist out of resolver closure (PR #1208 carmack #1)
	resolveHop := func(hop string) *nodeInfo {
		if cached, ok := hopCache[hop]; ok {
			return cached
		}
		r, _, _ := pm.resolveWithContext(hop, contextPubkeys, graph)
		hopCache[hop] = r
		return r
	}

	hopCounts := map[int]int{}
	var allHopsList []int
	hopSnr := map[int][]float64{}
	hopFreq := map[string]int{}
	pairFreq := map[string]int{}
	observerMap := map[string]string{} // observer_id → observer_name
	perObserver := map[string]map[string]*struct{ minDist, maxDist, count int }{}

	for _, tx := range filteredTxs {
		hops := txGetParsedPath(tx)
		if len(hops) == 0 {
			continue
		}

		// Area filter: skip this tx entirely from hop-count stats if none of its
		// hops belong to an area node — keeps hopCounts histogram consistent with
		// hopFreq, which already filters per-hop.
		if areaNodes != nil {
			hasAreaHop := false
			for _, h := range hops {
				r := resolveHop(h)
				if r != nil && areaNodes[r.PublicKey] {
					hasAreaHop = true
					break
				}
			}
			if !hasAreaHop {
				continue
			}
		}

		n := len(hops)
		hopCounts[n]++
		allHopsList = append(allHopsList, n)
		if tx.SNR != nil {
			hopSnr[n] = append(hopSnr[n], *tx.SNR)
		}
		for _, h := range hops {
			// Area filter: only count hops belonging to nodes in the area.
			if areaNodes != nil {
				r := resolveHop(h)
				if r == nil || !areaNodes[r.PublicKey] {
					continue
				}
			}
			hopFreq[h]++
		}
		for i := 0; i < len(hops)-1; i++ {
			a, b := hops[i], hops[i+1]
			// Area filter: only count pairs where both nodes are in the area.
			if areaNodes != nil {
				rA := resolveHop(a)
				rB := resolveHop(b)
				if rA == nil || !areaNodes[rA.PublicKey] || rB == nil || !areaNodes[rB.PublicKey] {
					continue
				}
			}
			if a > b {
				a, b = b, a
			}
			pairFreq[a+"|"+b]++
		}

		obsID := tx.ObserverID
		if obsID != "" {
			observerMap[obsID] = tx.ObserverName
		}
		if _, ok := perObserver[obsID]; !ok {
			perObserver[obsID] = map[string]*struct{ minDist, maxDist, count int }{}
		}
		for i, h := range hops {
			if areaNodes != nil {
				r := resolveHop(h)
				if r == nil || !areaNodes[r.PublicKey] {
					continue
				}
			}
			dist := n - i
			entry := perObserver[obsID][h]
			if entry == nil {
				entry = &struct{ minDist, maxDist, count int }{dist, dist, 0}
				perObserver[obsID][h] = entry
			}
			if dist < entry.minDist {
				entry.minDist = dist
			}
			if dist > entry.maxDist {
				entry.maxDist = dist
			}
			entry.count++
		}
	}

	// Hop distribution
	hopDist := make([]map[string]interface{}, 0)
	for h, c := range hopCounts {
		if h <= 25 {
			hopDist = append(hopDist, map[string]interface{}{"hops": h, "count": c})
		}
	}
	sort.Slice(hopDist, func(i, j int) bool {
		return hopDist[i]["hops"].(int) < hopDist[j]["hops"].(int)
	})

	avgHops := 0.0
	if len(allHopsList) > 0 {
		sum := 0
		for _, v := range allHopsList {
			sum += v
		}
		avgHops = float64(sum) / float64(len(allHopsList))
	}
	medianHops := 0
	if len(allHopsList) > 0 {
		sorted := make([]int, len(allHopsList))
		copy(sorted, allHopsList)
		sort.Ints(sorted)
		medianHops = sorted[len(sorted)/2]
	}
	maxHops := 0
	for _, v := range allHopsList {
		if v > maxHops {
			maxHops = v
		}
	}

	// pmLookup resolves a hop hex string to its prefix-map candidates,
	// applying the same truncation used during map construction.
	pmLookup := func(hop string) []nodeInfo {
		key := strings.ToLower(hop)
		if len(key) > maxPrefixLen {
			key = key[:maxPrefixLen]
		}
		return pm.m[key]
	}

	// --- Dedup pass: merge hop prefixes that resolve unambiguously to the same node ---
	// Only merge when pm.m[hop] has exactly 1 candidate (unique_prefix).
	// Ambiguous short prefixes (efiten's concern: 1-byte collisions) stay separate.
	{
		type dedupInfo struct {
			totalCount int
			longestHop string
		}
		byPubkey := map[string]*dedupInfo{} // pubkey → merged info
		ambiguous := map[string]int{}       // hop → count (kept as-is)
		for h, c := range hopFreq {
			candidates := pmLookup(h)
			if len(candidates) == 1 {
				pk := strings.ToLower(candidates[0].PublicKey)
				if info, ok := byPubkey[pk]; ok {
					info.totalCount += c
					if len(h) > len(info.longestHop) {
						info.longestHop = h
					}
				} else {
					byPubkey[pk] = &dedupInfo{totalCount: c, longestHop: h}
				}
			} else {
				ambiguous[h] = c
			}
		}
		// Rebuild hopFreq
		hopFreq = make(map[string]int, len(byPubkey)+len(ambiguous))
		for _, info := range byPubkey {
			hopFreq[info.longestHop] = info.totalCount
		}
		for h, c := range ambiguous {
			hopFreq[h] = c
		}
	}

	// --- Dedup pass for pairs: merge by resolved pubkey pair ---
	{
		type pairDedupInfo struct {
			totalCount int
			longestA   string
			longestB   string
		}
		byPubkeyPair := map[string]*pairDedupInfo{} // "pkA|pkB" (sorted) → merged info
		ambiguousPairs := map[string]int{}
		for p, c := range pairFreq {
			parts := strings.SplitN(p, "|", 2)
			candA := pmLookup(parts[0])
			candB := pmLookup(parts[1])
			if len(candA) == 1 && len(candB) == 1 {
				pkA := strings.ToLower(candA[0].PublicKey)
				pkB := strings.ToLower(candB[0].PublicKey)
				// Canonicalize by sorted pubkey
				if pkA > pkB {
					pkA, pkB = pkB, pkA
					parts[0], parts[1] = parts[1], parts[0]
				}
				key := pkA + "|" + pkB
				if info, ok := byPubkeyPair[key]; ok {
					info.totalCount += c
					if len(parts[0]) > len(info.longestA) {
						info.longestA = parts[0]
					}
					if len(parts[1]) > len(info.longestB) {
						info.longestB = parts[1]
					}
				} else {
					byPubkeyPair[key] = &pairDedupInfo{totalCount: c, longestA: parts[0], longestB: parts[1]}
				}
			} else {
				ambiguousPairs[p] = c
			}
		}
		// Rebuild pairFreq
		pairFreq = make(map[string]int, len(byPubkeyPair)+len(ambiguousPairs))
		for _, info := range byPubkeyPair {
			a, b := info.longestA, info.longestB
			if a > b {
				a, b = b, a
			}
			pairFreq[a+"|"+b] = info.totalCount
		}
		for p, c := range ambiguousPairs {
			pairFreq[p] = c
		}
	}

	// Top repeaters
	type freqEntry struct {
		hop   string
		count int
	}
	freqList := make([]freqEntry, 0, len(hopFreq))
	for h, c := range hopFreq {
		freqList = append(freqList, freqEntry{h, c})
	}
	sort.Slice(freqList, func(i, j int) bool { return freqList[i].count > freqList[j].count })
	topRepeaters := make([]map[string]interface{}, 0)
	for i, e := range freqList {
		if i >= 20 {
			break
		}
		r := resolveHop(e.hop)
		entry := map[string]interface{}{"hop": e.hop, "count": e.count, "name": nil, "pubkey": nil}
		if r != nil {
			entry["name"] = r.Name
			entry["pubkey"] = r.PublicKey
		}
		topRepeaters = append(topRepeaters, entry)
	}

	// Top pairs
	pairList := make([]freqEntry, 0, len(pairFreq))
	for p, c := range pairFreq {
		pairList = append(pairList, freqEntry{p, c})
	}
	sort.Slice(pairList, func(i, j int) bool { return pairList[i].count > pairList[j].count })
	topPairs := make([]map[string]interface{}, 0)
	for i, e := range pairList {
		if i >= 15 {
			break
		}
		parts := strings.SplitN(e.hop, "|", 2)
		rA := resolveHop(parts[0])
		rB := resolveHop(parts[1])
		entry := map[string]interface{}{
			"hopA": parts[0], "hopB": parts[1], "count": e.count,
			"nameA": nil, "nameB": nil, "pubkeyA": nil, "pubkeyB": nil,
		}
		if rA != nil {
			entry["nameA"] = rA.Name
			entry["pubkeyA"] = rA.PublicKey
		}
		if rB != nil {
			entry["nameB"] = rB.Name
			entry["pubkeyB"] = rB.PublicKey
		}
		topPairs = append(topPairs, entry)
	}

	// Hops vs SNR
	hopsVsSnr := make([]map[string]interface{}, 0)
	for h, snrs := range hopSnr {
		if h > 20 {
			continue
		}
		sum := 0.0
		for _, v := range snrs {
			sum += v
		}
		hopsVsSnr = append(hopsVsSnr, map[string]interface{}{
			"hops": h, "count": len(snrs), "avgSnr": sum / float64(len(snrs)),
		})
	}
	sort.Slice(hopsVsSnr, func(i, j int) bool {
		return hopsVsSnr[i]["hops"].(int) < hopsVsSnr[j]["hops"].(int)
	})

	// Observers list
	observers := make([]map[string]interface{}, 0)
	for id, name := range observerMap {
		n := name
		if n == "" {
			n = id
		}
		observers = append(observers, map[string]interface{}{"id": id, "name": n})
	}

	// Per-observer reachability
	perObserverReach := map[string]interface{}{}
	for obsID, nodes := range perObserver {
		obsName := observerMap[obsID]
		if obsName == "" {
			obsName = obsID
		}
		byDist := map[int][]map[string]interface{}{}
		for hop, data := range nodes {
			d := data.minDist
			if d > 15 {
				continue
			}
			r := resolveHop(hop)
			entry := map[string]interface{}{
				"hop": hop, "name": nil, "pubkey": nil,
				"count": data.count, "distRange": nil,
			}
			if r != nil {
				entry["name"] = r.Name
				entry["pubkey"] = r.PublicKey
			}
			if data.minDist != data.maxDist {
				entry["distRange"] = fmt.Sprintf("%d-%d", data.minDist, data.maxDist)
			}
			byDist[d] = append(byDist[d], entry)
		}
		rings := make([]map[string]interface{}, 0)
		for dist, nodeList := range byDist {
			sort.Slice(nodeList, func(i, j int) bool {
				return nodeList[i]["count"].(int) > nodeList[j]["count"].(int)
			})
			rings = append(rings, map[string]interface{}{"hops": dist, "nodes": nodeList})
		}
		sort.Slice(rings, func(i, j int) bool {
			return rings[i]["hops"].(int) < rings[j]["hops"].(int)
		})
		perObserverReach[obsID] = map[string]interface{}{
			"observer_name": obsName,
			"rings":         rings,
		}
	}

	// Cross-observer: build from perObserver
	crossObserver := map[string][]map[string]interface{}{}
	bestPath := map[string]map[string]interface{}{}
	for obsID, nodes := range perObserver {
		obsName := observerMap[obsID]
		if obsName == "" {
			obsName = obsID
		}
		for hop, data := range nodes {
			crossObserver[hop] = append(crossObserver[hop], map[string]interface{}{
				"observer_id": obsID, "observer_name": obsName,
				"minDist": data.minDist, "count": data.count,
			})
			if bp, ok := bestPath[hop]; !ok || data.minDist < bp["minDist"].(int) {
				bestPath[hop] = map[string]interface{}{
					"minDist": data.minDist, "observer_id": obsID, "observer_name": obsName,
				}
			}
		}
	}

	// Multi-observer nodes
	multiObsNodes := make([]map[string]interface{}, 0)
	for hop, obs := range crossObserver {
		if len(obs) <= 1 {
			continue
		}
		sort.Slice(obs, func(i, j int) bool {
			return obs[i]["minDist"].(int) < obs[j]["minDist"].(int)
		})
		r := resolveHop(hop)
		entry := map[string]interface{}{
			"hop": hop, "name": nil, "pubkey": nil, "observers": obs,
		}
		if r != nil {
			entry["name"] = r.Name
			entry["pubkey"] = r.PublicKey
		}
		multiObsNodes = append(multiObsNodes, entry)
	}
	sort.Slice(multiObsNodes, func(i, j int) bool {
		return len(multiObsNodes[i]["observers"].([]map[string]interface{})) >
			len(multiObsNodes[j]["observers"].([]map[string]interface{}))
	})
	if len(multiObsNodes) > 50 {
		multiObsNodes = multiObsNodes[:50]
	}

	// Best path list
	bestPathList := make([]map[string]interface{}, 0, len(bestPath))
	for hop, data := range bestPath {
		r := resolveHop(hop)
		entry := map[string]interface{}{
			"hop": hop, "name": nil, "pubkey": nil,
			"minDist": data["minDist"], "observer_id": data["observer_id"],
			"observer_name": data["observer_name"],
		}
		if r != nil {
			entry["name"] = r.Name
			entry["pubkey"] = r.PublicKey
		}
		bestPathList = append(bestPathList, entry)
	}
	sort.Slice(bestPathList, func(i, j int) bool {
		return bestPathList[i]["minDist"].(int) < bestPathList[j]["minDist"].(int)
	})
	if len(bestPathList) > 50 {
		bestPathList = bestPathList[:50]
	}

	return map[string]interface{}{
		"uniqueNodes":      uniqueNodes,
		"avgHops":          avgHops,
		"medianHops":       medianHops,
		"maxHops":          maxHops,
		"hopDistribution":  hopDist,
		"topRepeaters":     topRepeaters,
		"topPairs":         topPairs,
		"hopsVsSnr":        hopsVsSnr,
		"observers":        observers,
		"perObserverReach": perObserverReach,
		"multiObsNodes":    multiObsNodes,
		"bestPathList":     bestPathList,
	}
}

// --- Distance Analytics ---

func haversineKm(lat1, lon1, lat2, lon2 float64) float64 {
	const R = 6371.0
	dLat := (lat2 - lat1) * math.Pi / 180
	dLon := (lon2 - lon1) * math.Pi / 180
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*math.Pi/180)*math.Cos(lat2*math.Pi/180)*
			math.Sin(dLon/2)*math.Sin(dLon/2)
	return R * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

// GetAnalyticsDistance returns the distance analytics map. For the
// default (region="", area="") query, prefer the steady-state recomputer
// snapshot if one is registered (issue #1240). Region/area-keyed variants
// continue to use the legacy TTL cache + on-request compute.
func (s *PacketStore) GetAnalyticsDistance(ctx context.Context, region, area string) map[string]interface{} {
	if region == "" && area == "" {
		s.analyticsRecomputerMu.RLock()
		rc := s.recompDistance
		s.analyticsRecomputerMu.RUnlock()
		if rc != nil {
			if v := rc.Load(); v != nil {
				if m, ok := v.(map[string]interface{}); ok {
					s.cacheMu.Lock()
					s.cacheHits++
					s.cacheMu.Unlock()
					return m
				}
			}
		}
	}
	cacheKey := region + "|" + area
	s.cacheMu.Lock()
	if cached, ok := s.distCache[cacheKey]; ok && time.Now().Before(cached.expiresAt) {
		s.cacheHits++
		s.cacheMu.Unlock()
		return cached.data
	}
	s.cacheMisses++
	s.cacheMu.Unlock()

	result := s.computeAnalyticsDistance(ctx, region, area)

	s.cacheMu.Lock()
	s.distCache[cacheKey] = &cachedResult{data: result, expiresAt: time.Now().Add(s.rfCacheTTL)}
	s.cacheMu.Unlock()

	return result
}

func (s *PacketStore) computeAnalyticsDistance(ctx context.Context, region, area string) map[string]interface{} {
	// #1239: hold s.mu.RLock() only long enough to (a) snapshot the
	// distHops/distPaths slice headers and (b) build the region match
	// set (which reads tx.Observations, a field mutated by ingest under
	// s.mu.Lock). All filtering, sorting, deduping, histogram building
	// and category stats run on locally-captured slices OUTSIDE the
	// lock so concurrent ingest writers are not blocked by readers.
	//
	// Safety: snapshot slice headers are O(1). distHops/distPaths are
	// append-only via re-slice in buildDistanceIndex / updateDistanceIndexForTxs
	// under s.mu.Lock; if the backing array is reallocated after we
	// release the RLock, our snapshot still points at the prior backing
	// array (kept alive by GC) and observes the consistent length we
	// captured. The distHopRecord / distPathRecord values themselves
	// are value types (not pointers to live records) so we cannot read
	// torn writes from them post-release.
	var regionObs map[string]bool
	if region != "" {
		// resolveRegionObservers uses its own mutex (regionObsMu)
		// and is safe to call without s.mu held.
		regionObs = s.resolveRegionObservers(ctx, region)
	}
	var areaNodes map[string]bool
	if area != "" {
		areaNodes = s.resolveAreaNodes(ctx, area)
	}

	s.mu.RLock()
	hopsSnap := s.distHops
	pathsSnap := s.distPaths

	// Build region match set INSIDE the lock — touches tx.Observations
	// (slice header mutated by ingest). For non-region calls (the common
	// cold path) we skip this entirely and the RLock hold is microseconds.
	var matchSet map[*StoreTx]bool
	if regionObs != nil {
		matchSet = make(map[*StoreTx]bool)
		seen := make(map[*StoreTx]bool)
		for i := range hopsSnap {
			tx := hopsSnap[i].tx
			if tx == nil || seen[tx] {
				continue
			}
			seen[tx] = true
			for _, obs := range tx.Observations {
				if regionObs[obs.ObserverID] {
					matchSet[tx] = true
					break
				}
			}
		}
		for i := range pathsSnap {
			tx := pathsSnap[i].tx
			if tx == nil || seen[tx] {
				continue
			}
			seen[tx] = true
			for _, obs := range tx.Observations {
				if regionObs[obs.ObserverID] {
					matchSet[tx] = true
					break
				}
			}
		}
	}
	s.mu.RUnlock()

	// Everything below operates on hopsSnap / pathsSnap / matchSet —
	// no s.mu, no s.distHops / s.distPaths access. Safe to run while
	// ingest writers reallocate the underlying store-owned slices.

	// Additionally filter matchSet by area nodes
	if areaNodes != nil && matchSet != nil {
		for tx := range matchSet {
			d := tx.ParsedDecoded()
			pk, _ := d["public_key"].(string)
			if pk == "" {
				pk, _ = d["pubKey"].(string)
			}
			if pk == "" || !areaNodes[pk] {
				delete(matchSet, tx)
			}
		}
	} else if areaNodes != nil {
		// No region filter but area filter: build matchSet from area nodes
		matchSet = make(map[*StoreTx]bool)
		for i := range s.distHops {
			tx := s.distHops[i].tx
			if matchSet[tx] {
				continue
			}
			d := tx.ParsedDecoded()
			pk, _ := d["public_key"].(string)
			if pk == "" {
				pk, _ = d["pubKey"].(string)
			}
			if pk != "" && areaNodes[pk] {
				matchSet[tx] = true
			}
		}
		for i := range s.distPaths {
			tx := s.distPaths[i].tx
			if matchSet[tx] {
				continue
			}
			d := tx.ParsedDecoded()
			pk, _ := d["public_key"].(string)
			if pk == "" {
				pk, _ = d["pubKey"].(string)
			}
			if pk != "" && areaNodes[pk] {
				matchSet[tx] = true
			}
		}
	}

	// Filter precomputed hop records (copy to avoid mutating precomputed data during sort)
	filteredHops := make([]distHopRecord, 0, len(hopsSnap))
	for i := range hopsSnap {
		if matchSet == nil || matchSet[hopsSnap[i].tx] {
			filteredHops = append(filteredHops, hopsSnap[i])
		}
	}

	// Filter precomputed path records
	filteredPaths := make([]distPathRecord, 0, len(pathsSnap))
	for i := range pathsSnap {
		if matchSet == nil || matchSet[pathsSnap[i].tx] {
			filteredPaths = append(filteredPaths, pathsSnap[i])
		}
	}

	// Build category stats and time series from precomputed data
	catDists := map[string][]float64{"R↔R": {}, "C↔R": {}, "C↔C": {}}
	distByHour := map[string][]float64{}
	for i := range filteredHops {
		h := &filteredHops[i]
		catDists[h.Type] = append(catDists[h.Type], h.Dist)
		if h.HourBucket != "" {
			distByHour[h.HourBucket] = append(distByHour[h.HourBucket], h.Dist)
		}
	}

	topHops := dedupeHopsByPair(filteredHops, 20)

	// Sort and pick top paths
	sort.Slice(filteredPaths, func(i, j int) bool { return filteredPaths[i].TotalDist > filteredPaths[j].TotalDist })
	topPaths := make([]map[string]interface{}, 0)
	for i := range filteredPaths {
		if i >= 20 {
			break
		}
		p := &filteredPaths[i]
		hops := make([]map[string]interface{}, len(p.Hops))
		for j, hd := range p.Hops {
			hops[j] = map[string]interface{}{
				"fromName": hd.FromName, "fromPk": hd.FromPk,
				"toName": hd.ToName, "toPk": hd.ToPk,
				"dist": hd.Dist,
			}
		}
		topPaths = append(topPaths, map[string]interface{}{
			"hash": p.Hash, "totalDist": p.TotalDist,
			"hopCount": p.HopCount, "timestamp": p.Timestamp, "hops": hops,
		})
	}

	// Category stats
	medianF := func(arr []float64) float64 {
		if len(arr) == 0 {
			return 0
		}
		c := make([]float64, len(arr))
		copy(c, arr)
		sort.Float64s(c)
		return c[len(c)/2]
	}
	minF := func(arr []float64) float64 {
		if len(arr) == 0 {
			return 0
		}
		m := arr[0]
		for _, v := range arr[1:] {
			if v < m {
				m = v
			}
		}
		return m
	}
	maxF := func(arr []float64) float64 {
		if len(arr) == 0 {
			return 0
		}
		m := arr[0]
		for _, v := range arr[1:] {
			if v > m {
				m = v
			}
		}
		return m
	}

	catStats := map[string]interface{}{}
	for cat, dists := range catDists {
		if len(dists) == 0 {
			catStats[cat] = map[string]interface{}{"count": 0, "avg": 0, "median": 0, "min": 0, "max": 0}
			continue
		}
		sum := 0.0
		for _, v := range dists {
			sum += v
		}
		avg := sum / float64(len(dists))
		catStats[cat] = map[string]interface{}{
			"count":  len(dists),
			"avg":    math.Round(avg*100) / 100,
			"median": math.Round(medianF(dists)*100) / 100,
			"min":    math.Round(minF(dists)*100) / 100,
			"max":    math.Round(maxF(dists)*100) / 100,
		}
	}

	// Distance histogram
	var distHistogram interface{} = []interface{}{}
	allDists := make([]float64, len(filteredHops))
	for i := range filteredHops {
		allDists[i] = filteredHops[i].Dist
	}
	if len(allDists) > 0 {
		hMin, hMax := minF(allDists), maxF(allDists)
		binCount := 25
		binW := (hMax - hMin) / float64(binCount)
		if binW == 0 {
			binW = 1
		}
		bins := make([]int, binCount)
		for _, d := range allDists {
			idx := int(math.Floor((d - hMin) / binW))
			if idx >= binCount {
				idx = binCount - 1
			}
			if idx < 0 {
				idx = 0
			}
			bins[idx]++
		}
		binArr := make([]map[string]interface{}, binCount)
		for i, c := range bins {
			binArr[i] = map[string]interface{}{
				"x":     math.Round((hMin+float64(i)*binW)*10) / 10,
				"w":     math.Round(binW*10) / 10,
				"count": c,
			}
		}
		distHistogram = map[string]interface{}{"bins": binArr, "min": hMin, "max": hMax}
	}

	// Distance over time
	timeKeys := make([]string, 0, len(distByHour))
	for k := range distByHour {
		timeKeys = append(timeKeys, k)
	}
	sort.Strings(timeKeys)
	distOverTime := make([]map[string]interface{}, 0, len(timeKeys))
	for _, hour := range timeKeys {
		dists := distByHour[hour]
		sum := 0.0
		for _, v := range dists {
			sum += v
		}
		distOverTime = append(distOverTime, map[string]interface{}{
			"hour":  hour,
			"avg":   math.Round(sum/float64(len(dists))*100) / 100,
			"count": len(dists),
		})
	}

	// Summary
	summary := map[string]interface{}{
		"totalHops":  len(filteredHops),
		"totalPaths": len(filteredPaths),
		"avgDist":    0.0,
		"maxDist":    0.0,
	}
	if len(allDists) > 0 {
		sum := 0.0
		for _, v := range allDists {
			sum += v
		}
		summary["avgDist"] = math.Round(sum/float64(len(allDists))*100) / 100
		summary["maxDist"] = math.Round(maxF(allDists)*100) / 100
	}

	return map[string]interface{}{
		"summary":       summary,
		"topHops":       topHops,
		"topPaths":      topPaths,
		"catStats":      catStats,
		"distHistogram": distHistogram,
		"distOverTime":  distOverTime,
	}
}

// --- Hash Sizes Analytics ---

func (s *PacketStore) GetAnalyticsHashSizes(ctx context.Context, region, area string) api.HashSizeAnalyticsResponse {
	if region == "" && area == "" {
		s.analyticsRecomputerMu.RLock()
		rc := s.recompHashSizes
		s.analyticsRecomputerMu.RUnlock()
		if rc != nil {
			if v := rc.Load(); v != nil {
				if r, ok := v.(api.HashSizeAnalyticsResponse); ok {
					s.cacheMu.Lock()
					s.cacheHits++
					s.cacheMu.Unlock()
					return r
				}
			}
		}
	}
	cacheKey := region + "|" + area
	s.cacheMu.Lock()
	if cached, ok := s.hashCache[cacheKey]; ok && time.Now().Before(cached.expiresAt) {
		s.cacheHits++
		s.cacheMu.Unlock()
		return cached.data
	}
	s.cacheMisses++
	s.cacheMu.Unlock()

	result := s.computeAnalyticsHashSizesWithCapability(ctx, region, area)

	s.cacheMu.Lock()
	s.hashCache[cacheKey] = &cachedHashSizes{data: result, expiresAt: time.Now().Add(s.rfCacheTTL)}
	s.cacheMu.Unlock()

	return result
}

// computeAnalyticsHashSizesWithCapability runs computeAnalyticsHashSizes
// then layers in the multiByteCapability augmentation. Extracted so the
// steady-state recomputer (issue #1240) produces the same shape as the
// cached GetAnalyticsHashSizes call.
func (s *PacketStore) computeAnalyticsHashSizesWithCapability(ctx context.Context, region, area string) api.HashSizeAnalyticsResponse {
	result := s.computeAnalyticsHashSizes(ctx, region, area)

	// The adopters that decide a node's confirmed status are always the
	// global set. With a region filter the filtered result holds only part of
	// it, so compute the global set separately: a separate compute call, not
	// the cached path, avoids recursive locking on hashCache and keeps this
	// side-effect free.
	adopters := result.MultiByteNodes
	if region != "" {
		adopters = s.computeAnalyticsHashSizes(ctx, "", "").MultiByteNodes
	}
	globalAdopterHS := make(map[string]int, len(adopters))
	for _, n := range adopters {
		if n.Pubkey != "" && n.HashSize >= 2 {
			globalAdopterHS[n.Pubkey] = n.HashSize
		}
	}
	mbEntries := s.computeMultiByteCapability(globalAdopterHS)
	capabilities := make([]api.MultiByteCapability, 0, len(mbEntries))
	for _, e := range mbEntries {
		capabilities = append(capabilities, e.Capability())
	}
	result.MultiByteCapability = capabilities

	// Build the O(1) lookup index OUTSIDE the cache lock — at Cascadia
	// scale this is a ~2400-entry allocation + hash + insert per cycle.
	// Holding cacheMu while doing it blocks every API reader for the
	// duration. Swap the pointers in under a short write-lock.
	mbIdx := make(map[string]MultiByteCapEntry, len(mbEntries))
	for _, e := range mbEntries {
		mbIdx[e.PublicKey] = e
	}
	s.cacheMu.Lock()
	s.mbCapSnapshot = mbEntries
	s.mbCapIndex = mbIdx
	s.cacheMu.Unlock()
	// Publish snapshot to the on-disk handoff so the ingestor can
	// persist it (#1289/#1324: server is read-only; persistence is the
	// ingestor's job). Best-effort — a write failure here does not
	// affect serving (the in-memory index above is the read path).
	s.publishMultibyteCapSnapshot(s.config.SnapshotDir, mbEntries)

	return result
}

// loadObserverIATA reads observer→IATA from the DB. Call WITHOUT s.mu held.
func (s *PacketStore) loadObserverIATA(ctx context.Context) map[string]string {
	obsIATA := make(map[string]string, 64)
	if s.db == nil {
		return obsIATA
	}
	observers, err := s.db.GetObservers(ctx)
	if err != nil {
		return obsIATA
	}
	for _, o := range observers {
		if o.IATA != nil && *o.IATA != "" {
			obsIATA[o.Pubkey] = strings.TrimSpace(strings.ToUpper(*o.IATA))
		}
	}
	return obsIATA
}

func (s *PacketStore) computeAnalyticsHashSizes(ctx context.Context, region, area string) api.HashSizeAnalyticsResponse {

	var regionObs, areaNodes map[string]bool
	if region != "" {
		regionObs = s.resolveRegionObservers(ctx, region)
	}
	if area != "" {
		areaNodes = s.resolveAreaNodes(ctx, area)
	}
	obsIATA := s.loadObserverIATA(ctx)

	s.mu.RLock()
	defer s.mu.RUnlock()

	// #804: derive each node's HOME region from zero-hop direct adverts (the
	// most authoritative location signal — those packets cannot have been
	// relayed). When non-empty, multi-byte node attribution prefers this
	// over observer-region. Falls back to observer-region when unknown.
	nodeHomeRegion := s.computeNodeHomeRegions(obsIATA)
	attributionMethod := api.AttributionMethod("observer")
	if region != "" && len(nodeHomeRegion) > 0 {
		attributionMethod = "repeater"
	}

	allNodes, pm := s.getCachedNodesAndPM()

	// Build pubkey→role map for filtering by node type.
	nodeRoleByPK := make(map[string]types.NodeRole, len(allNodes))
	for _, n := range allNodes {
		nodeRoleByPK[n.PublicKey] = n.Role
	}

	distribution := map[string]int{"1": 0, "2": 0, "3": 0}
	byHour := map[string]map[string]int{}
	byNode := map[string]*hashSizeAdopter{}
	uniqueHops := map[string]*hopTally{}
	total := 0

	for _, tx := range s.packets {
		if tx.RawHex == "" {
			continue
		}

		// Parse header and path byte
		if len(tx.RawHex) < 4 {
			continue
		}
		header, err := strconv.ParseUint(tx.RawHex[:2], 16, 8)
		if err != nil {
			continue
		}
		routeType := header & 0x03
		pathByteIdx := 1
		if routeType == 0 || routeType == 3 {
			pathByteIdx = 5
		}
		hexStart := pathByteIdx * 2
		hexEnd := hexStart + 2
		if hexEnd > len(tx.RawHex) {
			continue
		}
		actualPathByte, err := strconv.ParseUint(tx.RawHex[hexStart:hexEnd], 16, 8)
		if err != nil {
			continue
		}

		hashSize := int((actualPathByte>>6)&0x3) + 1
		if hashSize > 3 {
			continue
		}

		// #804: pre-extract originator pubkey for ADVERT packets so we can
		// (a) relax observer-region filter when the originator's HOME region
		//     matches the requested region (a flood relay heard outside the
		//     home region must still attribute to the home), and
		// (b) reuse the parsed values below without re-parsing.
		var advertPK, advertName string
		var advertParsed bool
		if tx.PayloadType != nil && *tx.PayloadType == types.PayloadADVERT && tx.DecodedJSON != "" {
			if d := tx.ParsedDecoded(); d != nil {
				if v, ok := d["pubKey"].(string); ok {
					advertPK = v
				} else if v, ok := d["public_key"].(string); ok {
					advertPK = v
				}
				if n, ok := d["name"].(string); ok {
					advertName = n
				}
				advertParsed = advertPK != ""
			}
		}

		if regionObs != nil {
			match := false
			for _, obs := range tx.Observations {
				if regionObs[obs.ObserverID] {
					match = true
					break
				}
			}
			// #804: allow ADVERTs from a node whose HOME region matches the
			// requested region even if no observer in that region heard this
			// particular packet (e.g. flood relay heard only by an out-of-
			// region observer). Conservative: only ADVERTs (the source is
			// known by pubkey) and only when home is established.
			if !match && advertParsed {
				if home, ok := nodeHomeRegion[advertPK]; ok && iataMatchesRegion(home, region) {
					match = true
				}
			}
			if !match {
				continue
			}
		}

		// Area filter: skip ADVERT packets whose originator is not in the area.
		if areaNodes != nil && advertParsed && !areaNodes[advertPK] {
			continue
		}

		// Track originator from advert packets (including zero-hop adverts,
		// keyed by pubKey so same-name nodes don't merge).
		if advertParsed {
			pk := advertPK
			name := advertName
			if name == "" {
				if len(pk) >= 8 {
					name = pk[:8]
				} else {
					name = pk
				}
			}
			// Skip zero-hop direct adverts whose path byte is entirely zero —
			// there the size bits were wiped by the sender and say nothing.
			// A non-zero byte with a zero hop count (0x40 / 0x80) is a
			// deliberate size declaration; keep it. Same rule as
			// computeNodeHashSizeInfo. Skipped packets still count and still
			// update lastSeen.
			isUndeclaredZeroHop := (routeType == uint64(types.RouteDirect) || routeType == uint64(types.RouteTransportDirect)) && actualPathByte == 0x00
			adopter := byNode[pk]
			if adopter == nil {
				initHS := hashSize
				if isUndeclaredZeroHop {
					initHS = 0
				}
				// A pubkey that has advertised but is not in the nodes table
				// yet has no role. The wire format has a value for that.
				role, known := nodeRoleByPK[pk]
				if !known || role == "" {
					role = types.RoleUnknown
				}
				adopter = &hashSizeAdopter{
					name:     name,
					role:     role,
					hashSize: initHS,
				}
				byNode[pk] = adopter
			}
			adopter.packets++
			if !isUndeclaredZeroHop {
				adopter.hashSize = hashSize
			}
			adopter.lastSeen = tx.FirstSeen
		}

		// Distribution/hourly/uniqueHops only for packets with relay hops
		hops := txGetParsedPath(tx)
		if len(hops) == 0 {
			continue
		}
		total++

		sizeKey := strconv.Itoa(hashSize)
		distribution[sizeKey]++

		// Hourly buckets
		if !tx.FirstSeen.IsZero() {
			hour := hourBucket(tx.FirstSeen)
			if byHour[hour] == nil {
				byHour[hour] = map[string]int{"1": 0, "2": 0, "3": 0}
			}
			byHour[hour][sizeKey]++
		}

		// Track unique hops with their sizes
		for _, hop := range hops {
			tally := uniqueHops[hop]
			if tally == nil {
				tally = &hopTally{hex: hop, size: (len(hop) + 1) / 2}
				if candidates := pm.m[strings.ToLower(hop)]; len(candidates) > 0 {
					name := candidates[0].Name
					pubkey := candidates[0].PublicKey
					tally.name = &name
					tally.pubkey = &pubkey
				}
				uniqueHops[hop] = tally
			}
			tally.count++
		}
	}

	// Sort hourly data
	hourKeys := make([]string, 0, len(byHour))
	for k := range byHour {
		hourKeys = append(hourKeys, k)
	}
	sort.Strings(hourKeys)
	hourly := make([]api.HashSizeHourly, 0, len(hourKeys))
	for _, hour := range hourKeys {
		sizes := byHour[hour]
		hourly = append(hourly, api.HashSizeHourly{
			Hour: hour, N1: sizes["1"], N2: sizes["2"], N3: sizes["3"],
		})
	}

	// Top hops by frequency
	hopList := make([]*hopTally, 0, len(uniqueHops))
	for _, tally := range uniqueHops {
		hopList = append(hopList, tally)
	}
	sort.Slice(hopList, func(i, j int) bool {
		return hopList[i].count > hopList[j].count
	})
	topHops := make([]api.HashSizeHop, 0)
	for i, tally := range hopList {
		if i >= 50 {
			break
		}
		topHops = append(topHops, api.HashSizeHop{
			Hex: tally.hex, Size: tally.size, Count: tally.count,
			Name: tally.name, Pubkey: tally.pubkey,
		})
	}

	// Multi-byte nodes
	multiByteNodes := make([]api.MultiByteNode, 0)
	for pk, adopter := range byNode {
		// #804: when a region filter is active, prefer the repeater's HOME
		// region over the observer that happened to relay it. Falls back to
		// the (already-applied) observer-region filter when the node's home
		// region is unknown.
		if region != "" {
			if home, ok := nodeHomeRegion[pk]; ok && !iataMatchesRegion(home, region) {
				continue
			}
		}
		if adopter.hashSize > 1 {
			multiByteNodes = append(multiByteNodes, api.MultiByteNode{
				Name:     adopter.name,
				HashSize: adopter.hashSize,
				Packets:  adopter.packets,
				LastSeen: types.WireTimeSeconds{Time: adopter.lastSeen},
				Pubkey:   pk,
				Role:     adopter.role,
			})
		}
	}
	sort.Slice(multiByteNodes, func(i, j int) bool {
		return multiByteNodes[i].Packets > multiByteNodes[j].Packets
	})

	// Distribution by repeaters: count unique REPEATER nodes per hash size
	distributionByRepeaters := map[string]int{"1": 0, "2": 0, "3": 0}
	for pk, adopter := range byNode {
		if adopter.role != types.RoleRepeater {
			continue
		}
		// #804: same repeater-region preference as multiByteNodes.
		if region != "" {
			if home, ok := nodeHomeRegion[pk]; ok && !iataMatchesRegion(home, region) {
				continue
			}
		}
		distributionByRepeaters[strconv.Itoa(adopter.hashSize)]++
	}

	return api.HashSizeAnalyticsResponse{
		AttributionMethod:       attributionMethod,
		Total:                   total,
		Distribution:            distribution,
		DistributionByRepeaters: distributionByRepeaters,
		Hourly:                  hourly,
		TopHops:                 topHops,
		MultiByteNodes:          multiByteNodes,
		MultiByteCapability:     []api.MultiByteCapability{},
	}
}

// hashSizeAdopter tallies the adverts one node sent while hash sizes are
// counted.
type hashSizeAdopter struct {
	name     string
	role     types.NodeRole
	hashSize int
	packets  int
	lastSeen time.Time
}

// hopTally counts how many relayed packets carried one hop prefix.
type hopTally struct {
	hex    string
	size   int
	count  int
	name   *string
	pubkey *string
}

// hashSizeNodeInfo holds per-node hash size tracking data.
type hashSizeNodeInfo struct {
	HashSize     int
	AllSizes     map[int]bool
	Seq          []int
	Inconsistent bool
}

// --- Hash Collision Analytics ---

// GetAnalyticsHashCollisions returns pre-computed hash collision analysis.
// This moves the O(n²) distance computation from the frontend to the server.
func (s *PacketStore) GetAnalyticsHashCollisions(ctx context.Context, region, area string) api.HashCollisionResponse {
	if region == "" && area == "" {
		s.analyticsRecomputerMu.RLock()
		rc := s.recompHashCollisions
		s.analyticsRecomputerMu.RUnlock()
		if rc != nil {
			if v := rc.Load(); v != nil {
				if r, ok := v.(api.HashCollisionResponse); ok {
					s.cacheMu.Lock()
					s.cacheHits++
					s.cacheMu.Unlock()
					return r
				}
			}
		}
	}
	cacheKey := region + "|" + area
	s.cacheMu.Lock()
	if cached, ok := s.collisionCache[cacheKey]; ok && time.Now().Before(cached.expiresAt) {
		s.cacheHits++
		s.cacheMu.Unlock()
		return cached.data
	}
	s.cacheMisses++
	s.cacheMu.Unlock()

	result := s.computeHashCollisions(ctx, region, area)

	s.cacheMu.Lock()
	s.collisionCache[cacheKey] = &cachedHashCollisions{data: result, expiresAt: time.Now().Add(s.collisionCacheTTL)}
	s.cacheMu.Unlock()

	return result
}

func (s *PacketStore) computeHashCollisions(ctx context.Context, region, area string) api.HashCollisionResponse {
	// Get all nodes from DB
	nodes, _ := s.getAllNodes(ctx)
	hashInfo := s.GetNodeHashSizeInfo()

	// If region is specified, filter to only nodes seen by regional observers
	if region != "" {
		regionObs := s.resolveRegionObservers(ctx, region)
		if regionObs != nil {
			s.mu.RLock()
			regionNodePKs := make(map[string]bool)
			for _, tx := range s.packets {
				match := false
				for _, obs := range tx.Observations {
					if regionObs[obs.ObserverID] {
						match = true
						break
					}
				}
				if !match {
					continue
				}
				// Collect node public keys from advert packets
				if tx.DecodedJSON != "" {
					if d := tx.ParsedDecoded(); d != nil {
						if pk, ok := d["pubKey"].(string); ok && pk != "" {
							regionNodePKs[pk] = true
						}
						if pk, ok := d["public_key"].(string); ok && pk != "" {
							regionNodePKs[pk] = true
						}
					}
				}
				// Include observers themselves as nodes in the region
				for _, obs := range tx.Observations {
					if obs.ObserverID != "" {
						regionNodePKs[obs.ObserverID] = true
					}
				}
			}
			s.mu.RUnlock()

			// Filter nodes to only those seen in the region
			filtered := make([]nodeInfo, 0, len(regionNodePKs))
			for _, n := range nodes {
				if regionNodePKs[n.PublicKey] {
					filtered = append(filtered, n)
				}
			}
			nodes = filtered
		}
	}

	// If area is specified, filter to only nodes in the area
	if area != "" {
		areaNodes := s.resolveAreaNodes(ctx, area)
		if areaNodes != nil {
			filtered := make([]nodeInfo, 0, len(nodes))
			for _, n := range nodes {
				if areaNodes[n.PublicKey] {
					filtered = append(filtered, n)
				}
			}
			nodes = filtered
		}
	}

	// Build collision nodes with hash info
	var allCNodes []api.HashCollisionNode
	for _, n := range nodes {
		cn := api.HashCollisionNode{
			PublicKey: n.PublicKey,
			Name:      n.Name,
			Role:      n.Role,
			Lat:       n.Lat,
			Lon:       n.Lon,
		}
		if info, ok := hashInfo[n.PublicKey]; ok && info != nil {
			cn.HashSize = info.HashSize
			cn.HashSizeInconsistent = info.Inconsistent
			if len(info.AllSizes) > 1 {
				sizes := make([]int, 0, len(info.AllSizes))
				for sz := range info.AllSizes {
					sizes = append(sizes, sz)
				}
				sort.Ints(sizes)
				cn.HashSizesSeen = sizes
			}
		}
		allCNodes = append(allCNodes, cn)
	}

	// Inconsistent nodes
	var inconsistentNodes []api.HashCollisionNode
	for _, cn := range allCNodes {
		if cn.HashSizeInconsistent && (cn.Role == types.RoleRepeater || cn.Role == types.RoleRoom) {
			inconsistentNodes = append(inconsistentNodes, cn)
		}
	}
	if inconsistentNodes == nil {
		inconsistentNodes = make([]api.HashCollisionNode, 0)
	}

	// Compute collisions for each byte size (1, 2, 3)
	collisionsBySize := make(map[string]api.HashCollisionSize, 3)
	for _, bytes := range []int{1, 2, 3} {
		// Filter nodes relevant to this byte size.
		// - Exclude hash_size==0 nodes: no adverts seen, so actual hash
		//   size is unknown. Including them in every bucket inflates
		//   collision counts.
		// - Exclude companions: they are mobile/temporary and don't form
		//   the mesh backbone, so collisions with them aren't meaningful.
		// (Fixes #441)
		var nodesForByte []api.HashCollisionNode
		for _, cn := range allCNodes {
			if cn.HashSize == bytes && cn.Role == "repeater" {
				nodesForByte = append(nodesForByte, cn)
			}
		}

		// Build prefix map
		prefixMap := make(map[string][]api.HashCollisionNode)
		for _, cn := range nodesForByte {
			if len(cn.PublicKey) < bytes*2 {
				continue
			}
			prefix := strings.ToUpper(cn.PublicKey[:bytes*2])
			prefixMap[prefix] = append(prefixMap[prefix], cn)
		}

		// Compute collisions with pairwise distances
		var collisions []api.HashCollision
		for prefix, pnodes := range prefixMap {
			if len(pnodes) <= 1 {
				continue
			}
			// Pairwise distance
			var withCoords []api.HashCollisionNode
			for _, cn := range pnodes {
				if cn.Lat != 0 || cn.Lon != 0 {
					withCoords = append(withCoords, cn)
				}
			}
			var maxDistKm float64
			classification := api.CollisionClass("unknown")
			if len(withCoords) >= 2 {
				for i := 0; i < len(withCoords); i++ {
					for j := i + 1; j < len(withCoords); j++ {
						d := haversineKm(withCoords[i].Lat, withCoords[i].Lon, withCoords[j].Lat, withCoords[j].Lon)
						if d > maxDistKm {
							maxDistKm = d
						}
					}
				}
				if maxDistKm < 50 {
					classification = "local"
				} else if maxDistKm < 200 {
					classification = "regional"
				} else {
					classification = "distant"
				}
			} else {
				classification = "incomplete"
			}
			collisions = append(collisions, api.HashCollision{
				Prefix:         prefix,
				ByteSize:       bytes,
				Appearances:    len(pnodes),
				Nodes:          pnodes,
				MaxDistKm:      maxDistKm,
				Classification: classification,
				WithCoords:     len(withCoords),
			})
		}
		if collisions == nil {
			collisions = make([]api.HashCollision, 0)
		}

		// Sort: local first, then regional, distant, incomplete
		classOrder := map[api.CollisionClass]int{"local": 0, "regional": 1, "distant": 2, "incomplete": 3, "unknown": 4}
		sort.Slice(collisions, func(i, j int) bool {
			oi, oj := classOrder[collisions[i].Classification], classOrder[collisions[j].Classification]
			if oi != oj {
				return oi < oj
			}
			return collisions[i].Appearances > collisions[j].Appearances
		})

		// Stats
		nodeCount := len(nodesForByte)
		usingThisSize := 0
		for _, cn := range allCNodes {
			if cn.HashSize == bytes {
				usingThisSize++
			}
		}
		uniquePrefixes := len(prefixMap)
		collisionCount := len(collisions)
		var spaceSize int
		switch bytes {
		case 1:
			spaceSize = 256
		case 2:
			spaceSize = 65536
		case 3:
			spaceSize = 16777216
		}
		pctUsed := 0.0
		if spaceSize > 0 {
			pctUsed = float64(uniquePrefixes) / float64(spaceSize) * 100
		}

		// For 1-byte and 2-byte, include the full prefix cell data for matrix rendering
		var oneByteCells map[string][]api.HashCollisionNode
		var twoByteCells map[string]*api.TwoByteCell
		if bytes == 1 {
			oneByteCells = make(map[string][]api.HashCollisionNode)
			for i := 0; i < 256; i++ {
				hex := strings.ToUpper(fmt.Sprintf("%02x", i))
				oneByteCells[hex] = prefixMap[hex]
				if oneByteCells[hex] == nil {
					oneByteCells[hex] = make([]api.HashCollisionNode, 0)
				}
			}
			// Fix #1218: a repeater configured for a 2- or 3-byte hash still
			// occupies its first byte in the 1-byte hash space — any packet
			// routed by 1-byte path-matching collides on that first byte
			// regardless of the configured prefix size. Project all repeater
			// hashes to their first byte so the 1-byte view reflects real
			// conflicts in the 1-byte hash space.
			for _, cn := range allCNodes {
				if cn.Role != "repeater" {
					continue
				}
				if cn.HashSize != 2 && cn.HashSize != 3 {
					continue
				}
				if len(cn.PublicKey) < 2 {
					continue
				}
				hex := strings.ToUpper(cn.PublicKey[:2])
				if _, ok := oneByteCells[hex]; !ok {
					continue
				}
				oneByteCells[hex] = append(oneByteCells[hex], cn)
			}
		} else if bytes == 2 {
			twoByteCells = make(map[string]*api.TwoByteCell)
			for i := 0; i < 256; i++ {
				hex := strings.ToUpper(fmt.Sprintf("%02x", i))
				cell := &api.TwoByteCell{
					GroupNodes: make([]api.HashCollisionNode, 0),
					TwoByteMap: make(map[string][]api.HashCollisionNode),
				}
				twoByteCells[hex] = cell
			}
			for _, cn := range nodesForByte {
				if len(cn.PublicKey) < 4 {
					continue
				}
				firstHex := strings.ToUpper(cn.PublicKey[:2])
				twoHex := strings.ToUpper(cn.PublicKey[:4])
				cell := twoByteCells[firstHex]
				if cell == nil {
					continue
				}
				cell.GroupNodes = append(cell.GroupNodes, cn)
				cell.TwoByteMap[twoHex] = append(cell.TwoByteMap[twoHex], cn)
			}
			for _, cell := range twoByteCells {
				for _, ns := range cell.TwoByteMap {
					if len(ns) > 1 {
						cell.CollisionCount++
						if len(ns) > cell.MaxCollision {
							cell.MaxCollision = len(ns)
						}
					}
				}
			}
		}

		sizeData := api.HashCollisionSize{
			Stats: api.HashCollisionStats{
				TotalNodes:     len(allCNodes),
				NodesForByte:   nodeCount,
				UsingThisSize:  usingThisSize,
				UniquePrefixes: uniquePrefixes,
				CollisionCount: collisionCount,
				SpaceSize:      spaceSize,
				PctUsed:        pctUsed,
			},
			Collisions:   collisions,
			OneByteCells: oneByteCells,
		}
		if twoByteCells != nil {
			sizeData.TwoByteCells = make(map[string]api.TwoByteCell, len(twoByteCells))
			for hex, cell := range twoByteCells {
				sizeData.TwoByteCells[hex] = *cell
			}
		}
		collisionsBySize[strconv.Itoa(bytes)] = sizeData
	}

	return api.HashCollisionResponse{
		InconsistentNodes: inconsistentNodes,
		BySize:            collisionsBySize,
	}
}

// GetNodeHashSizeInfo returns cached per-node hash size data, recomputing at most every 15s.
func (s *PacketStore) GetNodeHashSizeInfo() map[string]*hashSizeNodeInfo {
	const ttl = 15 * time.Second
	s.hashSizeInfoMu.Lock()
	if s.hashSizeInfoCache != nil && time.Since(s.hashSizeInfoAt) < ttl {
		cached := s.hashSizeInfoCache
		s.hashSizeInfoMu.Unlock()
		return cached
	}
	s.hashSizeInfoMu.Unlock()
	result := s.computeNodeHashSizeInfo()
	s.hashSizeInfoMu.Lock()
	s.hashSizeInfoCache = result
	s.hashSizeInfoAt = time.Now()
	s.hashSizeInfoMu.Unlock()
	return result
}

// computeNodeHashSizeInfo scans advert packets to compute per-node hash size data.
// Only adverts from the last 7 days are considered so that legitimate config
// changes during testing don't create permanent false positives.
func (s *PacketStore) computeNodeHashSizeInfo() map[string]*hashSizeNodeInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Collect (timestamp, hashSize) per pubkey so we can order adverts
	// chronologically below. byPayloadType iteration is insertion order, which
	// is not guaranteed to be chronological (out-of-order MQTT ingest, chunked
	// cold-load), so we must sort by timestamp before reasoning about "latest"
	// or "most recent" adverts. We parse FirstSeen rather than string-compare it
	// so ordering is robust to timestamp-format differences (RFC3339 with or
	// without fractional seconds); an unparseable/empty FirstSeen sorts oldest
	// so it can never masquerade as the latest advert.
	type hsEntry struct {
		ts   time.Time
		size int
	}
	entries := make(map[string][]hsEntry)

	cutoff := time.Now().UTC().Add(-7 * 24 * time.Hour)

	adverts := s.byPayloadType[4]
	for _, tx := range adverts {
		// Skip adverts older than 7 days to avoid false positives from
		// historical config changes during testing.
		if !tx.FirstSeen.IsZero() && tx.FirstSeen.Before(cutoff) {
			continue
		}
		if tx.RawHex == "" || tx.DecodedJSON == "" {
			continue
		}
		if len(tx.RawHex) < 4 {
			continue
		}
		header, err := strconv.ParseUint(tx.RawHex[:2], 16, 8)
		if err != nil {
			continue
		}
		routeType := types.RouteType(header & 0x03)
		// Transport routes (0, 3) have 4 transport code bytes before the path
		// byte, so the path byte is at offset 5 instead of 1.
		pbOffset := 1
		if routeType == types.RouteTransportFlood || routeType == types.RouteTransportDirect {
			pbOffset = 5
		}
		if len(tx.RawHex) < (pbOffset+1)*2 {
			continue
		}
		pathByte, err := strconv.ParseUint(tx.RawHex[pbOffset*2:pbOffset*2+2], 16, 8)
		if err != nil {
			continue
		}
		// Direct zero-hop adverts carry no path, so the hop count is 0. Whether
		// the SIZE bits are meaningful depends on the sender: firmware that
		// predates meshcore-dev/MeshCore#3293 does `path_len = 0`, wiping the
		// whole byte including the two size bits, so 0x00 says nothing about
		// the node's path.hash.mode. A sender that writes the size through
		// setPathHashSizeAndCount() emits 0x40 / 0x80 with a zero hop count —
		// that is a deliberate declaration and the only way those bits can be
		// non-zero here. Skip on the byte's content, not on the route type.
		if (routeType == types.RouteDirect || routeType == types.RouteTransportDirect) && pathByte == 0x00 {
			continue
		}
		hs := int((pathByte>>6)&0x3) + 1

		d := tx.ParsedDecoded()
		if d == nil {
			continue
		}
		pk := ""
		if v, ok := d["pubKey"].(string); ok {
			pk = v
		} else if v, ok := d["public_key"].(string); ok {
			pk = v
		}
		if pk == "" {
			continue
		}

		// time.Parse(time.RFC3339, ...) accepts both the ingestor's no-fraction
		// form ("...05Z") and a fractional form ("...05.000Z"). Zero time on
		// parse failure → sorts oldest.
		entries[pk] = append(entries[pk], hsEntry{ts: tx.FirstSeen, size: hs})
	}

	info := make(map[string]*hashSizeNodeInfo)
	for pk, es := range entries {
		// Order adverts chronologically. Stable sort so that adverts with equal
		// (or unparseable) timestamps keep insertion order.
		sort.SliceStable(es, func(i, j int) bool { return es[i].ts.Before(es[j].ts) })

		ni := &hashSizeNodeInfo{AllSizes: make(map[int]bool), Seq: make([]int, len(es))}
		for i, e := range es {
			ni.Seq[i] = e.size
			ni.AllSizes[e.size] = true
		}
		info[pk] = ni

		// Use the most recent advert's hash size (last in chronological order).
		// The upstream firmware bug causing stale path bytes in flood adverts
		// was fixed (meshcore-dev/MeshCore#2154).
		ni.HashSize = ni.Seq[len(ni.Seq)-1]

		// Flip-flop (inconsistent) flag: need a minimum number of observations,
		// >= 2 unique sizes, and >= 2 transitions in the sequence.
		if len(ni.Seq) < hashSizeMinObservations || len(ni.AllSizes) < 2 {
			continue
		}
		transitions := 0
		for i := 1; i < len(ni.Seq); i++ {
			if ni.Seq[i] != ni.Seq[i-1] {
				transitions++
			}
		}
		if transitions < 2 {
			continue
		}
		// Recency decay (issue #1726): if the most recent adverts all agree on
		// a single size, the node has settled on its current hash mode (e.g. an
		// operator flipped path.hash.mode mid-flight, or a lone stale 1-byte
		// advert sits earlier in the 7-day window). Don't keep reporting "varies"
		// over older history once the node is consistent again. A node whose
		// recent adverts still disagree remains flagged.
		//
		// Known limitation: a node that flaps slowly (long stable stretches
		// between toggles) is not flagged during a stable stretch. This is
		// intentional — "varies" describes the node's *current* state — and the
		// full history stays visible via hash_sizes_seen / AllSizes.
		if recentAdvertsAgree(ni.Seq, hashSizeRecentAgreeCount) {
			continue
		}
		ni.Inconsistent = true
	}

	return info
}

// hashSizeMinObservations is the minimum number of non-zero-hop adverts in the
// window before a node is eligible to be flagged as flip-flopping at all.
const hashSizeMinObservations = 3

// hashSizeRecentAgreeCount is how many of the most recent non-zero-hop adverts
// must share a single hash size for a node to be considered "settled", clearing
// its flip-flop ("varies") flag.
const hashSizeRecentAgreeCount = 3

// recentAdvertsAgree reports whether the last n entries of a chronologically
// ordered hash-size sequence are all equal.
func recentAdvertsAgree(seq []int, n int) bool {
	if len(seq) < n {
		return false
	}
	last := seq[len(seq)-1]
	for i := len(seq) - n; i < len(seq)-1; i++ {
		if seq[i] != last {
			return false
		}
	}
	return true
}

// EnrichNodeWithHashSize populates hash_size, hash_size_inconsistent, and
// hash_sizes_seen on a node map using precomputed hash size info.
func EnrichNodeWithHashSize(node *api.Node, info *hashSizeNodeInfo) {
	if info == nil {
		return
	}
	hashSize := info.HashSize
	node.HashSize = &hashSize
	node.HashSizeInconsistent = info.Inconsistent
	if len(info.AllSizes) > 1 {
		sizes := make([]int, 0, len(info.AllSizes))
		for s := range info.AllSizes {
			sizes = append(sizes, s)
		}
		sort.Ints(sizes)
		node.HashSizesSeen = sizes
	}
}

// GetMultibyteCapFor returns the capability entry for a single pubkey via an O(1) map
// lookup into the snapshot rebuilt by each analytics cycle (and pre-populated from
// the DB on cold start). Returns false when the pubkey has no known capability.
func (s *PacketStore) GetMultibyteCapFor(pk string) (*MultiByteCapEntry, bool) {
	s.cacheMu.RLock()
	e, ok := s.mbCapIndex[pk]
	s.cacheMu.RUnlock()
	if !ok {
		return nil, false
	}
	return &e, true
}

// loadMultibyteCapFromDB pre-populates mbCapSnapshot and mbCapIndex from the
// nodes table so cold starts serve the last-known capability without waiting
// for the first analytics cycle (~15s). Best-effort: failures are logged.
func (s *PacketStore) loadMultibyteCapFromDB(ctx context.Context) {
	nodes, err := s.db.ListMultibyteCapNodes(ctx)
	if err != nil {
		log.Printf("[multibyte] loadFromDB: %v", err)
		return
	}
	if len(nodes) == 0 {
		return
	}

	entries := make([]MultiByteCapEntry, 0, len(nodes))
	idx := make(map[string]MultiByteCapEntry, len(nodes))
	for _, n := range nodes {
		status := "unknown"
		switch n.Sup {
		case 2:
			status = "confirmed"
		case 1:
			status = "suspected"
		}
		e := MultiByteCapEntry{
			PublicKey: n.Pubkey,
			Name:      n.Name,
			Role:      n.Role,
			Status:    status,
			Evidence:  n.Evidence,
			LastSeen:  n.LastSeen,
		}
		entries = append(entries, e)
		idx[e.PublicKey] = e
	}

	s.cacheMu.Lock()
	s.mbCapSnapshot = entries
	s.mbCapIndex = idx
	s.cacheMu.Unlock()
	log.Printf("[multibyte] loaded %d capability entries from DB", len(entries))
}

// publishMultibyteCapSnapshot writes the analytics-cycle output to the
// on-disk handoff (internal/mbcapqueue). The ingestor's
// RunMultibyteCapPersist consumes the file and writes confirmed /
// suspected entries to the DB.
//
// INVARIANT (#1289/#1324): the server is the read path and opens
// SQLite mode=ro. It MUST NOT execute any UPDATE on
// nodes.multibyte_* — see readonly_invariant_test.go. This helper is
// the only side-effect path for capability data leaving the server.
func (s *PacketStore) publishMultibyteCapSnapshot(path string, entries []MultiByteCapEntry) {
	if s.db == nil {
		return
	}
	out := make([]mbcapqueue.Entry, 0, len(entries))
	for _, e := range entries {
		out = append(out, mbcapqueue.Entry{
			PublicKey: e.PublicKey,
			Status:    e.Status,
			Evidence:  e.Evidence,
		})
	}
	if err := mbcapqueue.WriteSnapshot(path, mbcapqueue.Snapshot{Entries: out}); err != nil {
		log.Printf("[multibyte] publish snapshot: %v", err)
	}
}

// --- Multi-Byte Capability Inference ---

// MultiByteCapEntry represents a node's inferred multi-byte capability.
type MultiByteCapEntry struct {
	PublicKey   string         `json:"pubkey"`
	Name        string         `json:"name"`
	Role        types.NodeRole `json:"role"`
	Status      string         `json:"status"`   // "confirmed", "suspected", "unknown"
	Evidence    string         `json:"evidence"` // "advert", "path", ""
	MaxHashSize int            `json:"maxHashSize"`
	LastSeen    time.Time      `json:"lastSeen"`
}

// Capability is the entry as the hash-sizes endpoint serves it.
func (e MultiByteCapEntry) Capability() api.MultiByteCapability {
	return api.MultiByteCapability{
		Pubkey:      e.PublicKey,
		Name:        e.Name,
		Role:        e.Role,
		Status:      api.CapabilityStatus(e.Status),
		Evidence:    api.CapabilityEvidence(e.Evidence),
		MaxHashSize: e.MaxHashSize,
		LastSeen:    types.WireTimeSeconds{Time: e.LastSeen},
	}
}

// computeMultiByteCapability determines multi-byte capability for each
// node (repeaters, companions, rooms, sensors) using two methods:
//
//  1. Confirmed: the node has advertised with hash_size >= 2 (from advert
//     path byte). This is 100% reliable because the full public key is
//     received in adverts — no prefix collision ambiguity.
//
//  2. Suspected: the node's prefix appears as a hop in a packet whose path
//     header indicates hash_size >= 2. This is <100% reliable because
//     2-byte prefixes can collide — two different nodes may share the same
//     prefix. If one is confirmed multi-byte and the other is not, the
//     non-confirmed one could be a false positive.
//
//  3. Unknown: node has only been seen with 1-byte adverts and no
//     multi-byte path appearances. Could be pre-1.14 firmware or 1.14+
//     with default (1-byte) settings.
//
// Caller must hold NO locks — this method acquires mu.RLock internally.
func (s *PacketStore) computeMultiByteCapability(adopterHashSizes map[string]int) []MultiByteCapEntry {
	// Get hash size info from adverts (has its own locking)
	hashInfo := s.GetNodeHashSizeInfo()

	// Node name/role/last-seen lookup from the cached snapshot (no database read).
	allNodes, _ := s.getCachedNodesAndPM()
	nodeByPK := make(map[string]nodeInfo, len(allNodes))
	for _, n := range allNodes {
		nodeByPK[n.PublicKey] = n
	}

	// Build set of confirmed multi-byte pubkeys (advert hash_size >= 2)
	confirmed := make(map[string]int) // pubkey → max hash size from adverts
	for pk, info := range hashInfo {
		maxHS := 1
		for sz := range info.AllSizes {
			if sz > maxHS {
				maxHS = sz
			}
		}
		if maxHS >= 2 {
			confirmed[pk] = maxHS
		}
	}

	// Scan path-hop index for suspected multi-byte nodes.
	// For each repeater, check if any packet in byPathHop has that
	// node as a hop with hash_size >= 2 in the path header.
	s.mu.RLock()

	// Build prefix→pubkey mapping for repeaters
	type prefixEntry struct {
		pubkey string
		prefix string
	}
	nodePrefixes := make(map[string][]prefixEntry) // prefix → entries
	for pk := range nodeByPK {
		// Generate 1-byte, 2-byte, 3-byte prefixes
		pkLower := strings.ToLower(pk)
		for byteLen := 1; byteLen <= 3; byteLen++ {
			hexLen := byteLen * 2
			if len(pkLower) >= hexLen {
				pfx := pkLower[:hexLen]
				nodePrefixes[pfx] = append(nodePrefixes[pfx], prefixEntry{pk, pfx})
			}
		}
	}

	suspected := make(map[string]int) // pubkey → max hash size from path appearances
	for pfx, entries := range nodePrefixes {
		txList := s.byPathHop[pfx]
		for _, tx := range txList {
			if tx.RawHex == "" || len(tx.RawHex) < 4 {
				continue
			}
			// Skip TRACE packets (payload_type 8) — they carry hash size in
			// TRACE flags, not the repeater's compile-time PATH_HASH_SIZE.
			// Pre-1.14 repeaters can forward multi-byte TRACEs, creating
			// false positives for "suspected" capability. See #714.
			if tx.PayloadType != nil && *tx.PayloadType == types.PayloadTRACE {
				continue
			}
			header, err := strconv.ParseUint(tx.RawHex[:2], 16, 8)
			if err != nil {
				continue
			}
			routeType := header & 0x03
			pathByteIdx := 1
			if routeType == 0 || routeType == 3 {
				pathByteIdx = 5
			}
			hexStart := pathByteIdx * 2
			hexEnd := hexStart + 2
			if hexEnd > len(tx.RawHex) {
				continue
			}
			actualPathByte, err := strconv.ParseUint(tx.RawHex[hexStart:hexEnd], 16, 8)
			if err != nil {
				continue
			}
			hs := int((actualPathByte>>6)&0x3) + 1
			if hs < 2 {
				continue
			}
			// This packet uses multi-byte hashes and contains this prefix as a hop
			for _, e := range entries {
				if hs > suspected[e.pubkey] {
					suspected[e.pubkey] = hs
				}
			}
			break // one match is enough per prefix
		}
	}
	s.mu.RUnlock()

	// Build result for all nodes. Name, role and last seen come from the cached
	// snapshot, so no database read is needed here.
	result := make([]MultiByteCapEntry, 0, len(nodeByPK))
	for pk, n := range nodeByPK {
		entry := MultiByteCapEntry{
			PublicKey:   pk,
			Name:        n.Name,
			Role:        n.Role,
			MaxHashSize: 1,
		}
		if !n.LastSeen.IsZero() {
			entry.LastSeen = n.LastSeen
		}

		if maxHS, ok := confirmed[pk]; ok {
			entry.Status = "confirmed"
			entry.Evidence = "advert"
			entry.MaxHashSize = maxHS
		} else if maxHS, ok := adopterHashSizes[pk]; ok && maxHS >= 2 {
			// Adopter data (from computeAnalyticsHashSizes) shows hash_size >= 2
			// from advert analysis, so this is advert-based evidence: confirmed.
			entry.Status = "confirmed"
			entry.Evidence = "advert"
			entry.MaxHashSize = maxHS
		} else if maxHS, ok := suspected[pk]; ok {
			entry.Status = "suspected"
			entry.Evidence = "path"
			entry.MaxHashSize = maxHS
		} else {
			entry.Status = "unknown"
		}

		// Check advert hash info for max even if not confirmed multi-byte
		if info, ok := hashInfo[pk]; ok && entry.MaxHashSize == 1 {
			for sz := range info.AllSizes {
				if sz > entry.MaxHashSize {
					entry.MaxHashSize = sz
				}
			}
		}

		result = append(result, entry)
	}

	// Sort: confirmed first, then suspected, then unknown; within each group by
	// name, then pubkey so equal names keep a stable order (nodeByPK is a map).
	statusOrder := map[string]int{"confirmed": 0, "suspected": 1, "unknown": 2}
	sort.Slice(result, func(i, j int) bool {
		oi, oj := statusOrder[result[i].Status], statusOrder[result[j].Status]
		if oi != oj {
			return oi < oj
		}
		ni, nj := strings.ToLower(result[i].Name), strings.ToLower(result[j].Name)
		if ni != nj {
			return ni < nj
		}
		return result[i].PublicKey < result[j].PublicKey
	})

	return result
}

// --- Bulk Health (in-memory) ---

func (s *PacketStore) GetBulkHealth(ctx context.Context, limit int, region, area string) []api.BulkHealthEntry {
	// Everything that can touch the database happens before s.mu is taken.
	var areaNodes map[string]bool
	if area != "" {
		areaNodes = s.resolveAreaNodes(ctx, area)
	}
	var regionObs map[string]bool
	if region != "" {
		regionObs = s.resolveRegionObservers(ctx, region)
	}
	// Loaded before s.mu so the lock order stays s.mu → analyticsRecomputerMu.
	directHeard := s.loadDirectHeard()
	nonRelaySet, seenSet := s.canRelaySets(ctx)

	// Candidates: most recently heard first, from the cached node snapshot (no query).
	// Fetch more when filtering so we don't under-fill after exclusions.
	allNodes, _ := s.getCachedNodesAndPM()
	candidates := make([]nodeInfo, len(allNodes))
	copy(candidates, allNodes)
	sort.SliceStable(candidates, func(i, j int) bool {
		if !candidates[i].LastSeen.Equal(candidates[j].LastSeen) {
			return candidates[i].LastSeen.After(candidates[j].LastSeen)
		}
		return candidates[i].PublicKey < candidates[j].PublicKey
	})
	candidateLimit := limit
	if regionObs != nil || areaNodes != nil {
		candidateLimit = 10000
	}
	if len(candidates) > candidateLimit {
		candidates = candidates[:candidateLimit]
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Region filtering: nodes whose packets were seen by an observer in the region.
	var regionNodeKeys map[string]bool
	if regionObs != nil {
		regionalHashes := make(map[string]bool)
		for obsID := range regionObs {
			for _, o := range s.byObserver[obsID] {
				if tx := s.byTxID[o.TransmissionID]; tx != nil {
					regionalHashes[tx.Hash] = true
				}
			}
		}
		regionNodeKeys = make(map[string]bool)
		for pk, hashes := range s.nodeHashes {
			for h := range hashes {
				if regionalHashes[h] {
					regionNodeKeys[pk] = true
					break
				}
			}
		}
	}

	nodes := make([]nodeInfo, 0, len(candidates))
	for _, n := range candidates {
		if regionNodeKeys != nil && !regionNodeKeys[n.PublicKey] {
			continue
		}
		if areaNodes != nil && !areaNodes[n.PublicKey] {
			continue
		}
		nodes = append(nodes, n)
		if regionNodeKeys == nil && areaNodes == nil && len(nodes) >= limit {
			break
		}
	}
	// Only cap to limit in the global (no-filter) case; area/region returns the full filtered set.
	if regionNodeKeys != nil && areaNodes == nil && len(nodes) > limit {
		nodes = nodes[:limit]
	}

	todayStart := time.Now().UTC().Truncate(24 * time.Hour)
	results := make([]api.BulkHealthEntry, 0, len(nodes))

	for _, n := range nodes {
		packets := s.byNode[n.PublicKey]
		var packetsToday int
		var snrSum float64
		var snrCount int
		var lastHeard time.Time
		// See GetNodeHealth: this set is "saw traffic involving the node",
		// which is not "heard the node". Only the direct-RF rows below carry
		// signal numbers.
		relayObservers := map[string]struct{}{}
		totalObservations := 0

		for _, pkt := range packets {
			totalObservations += pkt.ObservationCount
			if totalObservations == 0 {
				totalObservations = 1
			}
			if pkt.FirstSeen.After(todayStart) {
				packetsToday++
			}
			if pkt.SNR != nil {
				snrSum += *pkt.SNR
				snrCount++
			}
			if lastHeard.IsZero() || pkt.FirstSeen.After(lastHeard) {
				lastHeard = pkt.FirstSeen
			}
			if pkt.ObserverID != "" {
				relayObservers[pkt.ObserverID] = struct{}{}
			}
		}

		directByObs := directHeard[strings.ToLower(n.PublicKey)]
		observerRows := buildDirectObserverRows(directByObs, nonRelaySet, seenSet)
		relayObserverCount := relayOnlyObserverCount(relayObservers, directByObs)

		var avgSnr *float64
		if snrCount > 0 {
			v := snrSum / float64(snrCount)
			avgSnr = &v
		}
		var lastHeardPtr *types.WireTimeSeconds
		if !lastHeard.IsZero() {
			lastHeardPtr = &types.WireTimeSeconds{Time: lastHeard}
		}
		var lat, lon *float64
		if n.HasGPS {
			lat, lon = ptr(n.Lat), ptr(n.Lon)
		}
		var name *string
		if n.Name != "" {
			name = ptr(n.Name)
		}

		results = append(results, api.BulkHealthEntry{
			PublicKey: n.PublicKey,
			Name:      name,
			Role:      string(n.Role),
			Lat:       lat,
			Lon:       lon,
			Stats: api.BulkHealthStats{
				TotalTransmissions: len(packets),
				TotalObservations:  totalObservations,
				TotalPackets:       len(packets),
				PacketsToday:       packetsToday,
				AvgSnr:             avgSnr,
				LastHeard:          lastHeardPtr,
			},
			Observers:          observerRows,
			RelayObserverCount: relayObserverCount,
		})
	}

	return results
}

// --- Subpaths Analytics ---

// nodeTxsSince returns the node's transmissions first seen after fromISO,
// from the byNode index (sender, recipient and resolved relay hops).
// Raw JSON text search is intentionally avoided: a GRP_TXT packet whose message
// text contains a node's pubkey is not a packet *for* that node.
// Must be called with s.mu held.
func (s *PacketStore) nodeTxsSince(pubkey string, fromISO time.Time) []*StoreTx {
	var packets []*StoreTx
	for _, p := range s.byNode[pubkey] {
		if p.FirstSeen.After(fromISO) {
			packets = append(packets, p)
		}
	}
	return packets
}

// GetNodeAnalytics computes analytics for a single node using in-memory byNode index.
func (s *PacketStore) GetNodeAnalytics(ctx context.Context, pubkey string, days int) (*NodeAnalyticsResponse, error) {
	dbNode, err := s.db.GetNodeByPubkey(ctx, pubkey)
	if errors.Is(err, database.ErrNotFound) {
		return nil, nil
	}
	if err != nil || dbNode == nil {
		return nil, err
	}
	node := nodeToResponse(dbNode)
	fromTime := time.Now().Add(-time.Duration(days) * 24 * time.Hour)
	toTime := time.Now().UTC()

	s.mu.RLock()
	defer s.mu.RUnlock()

	packets := s.nodeTxsSince(pubkey, fromTime)

	// Activity timeline (hourly buckets)
	timelineBuckets := map[string]int{}
	for _, p := range packets {
		if p.FirstSeen.IsZero() {
			continue
		}
		bucket := p.FirstSeen.UTC().Truncate(time.Hour).Format("2006-01-02T15:00:00Z")
		timelineBuckets[bucket]++
	}
	bucketKeys := make([]string, 0, len(timelineBuckets))
	for k := range timelineBuckets {
		bucketKeys = append(bucketKeys, k)
	}
	sort.Strings(bucketKeys)
	activityTimeline := make([]TimeBucket, 0, len(bucketKeys))
	for _, k := range bucketKeys {
		b := k
		activityTimeline = append(activityTimeline, TimeBucket{Bucket: &b, Count: timelineBuckets[k]})
	}

	// SNR trend
	snrTrend := make([]SnrTrendEntry, 0)
	for _, p := range packets {
		if p.SNR != nil {
			snrTrend = append(snrTrend, SnrTrendEntry{
				Timestamp:    p.FirstSeen,
				SNR:          floatPtrOrNil(p.SNR),
				RSSI:         floatPtrOrNil(p.RSSI),
				ObserverID:   strOrNil(p.ObserverID),
				ObserverName: strOrNil(p.ObserverName),
			})
		}
	}

	// Packet type breakdown
	typeBuckets := map[types.PayloadType]int{}
	for _, p := range packets {
		if p.PayloadType != nil {
			typeBuckets[*p.PayloadType]++
		}
	}
	packetTypeBreakdown := make([]PayloadTypeCount, 0, len(typeBuckets))
	for pt, cnt := range typeBuckets {
		packetTypeBreakdown = append(packetTypeBreakdown, PayloadTypeCount{PayloadType: pt, Count: cnt})
	}

	// Observer coverage
	type obsAccum struct {
		name                       string
		snrSum, rssiSum            float64
		snrCount, rssiCount, count int
		first, last                time.Time
	}
	obsMap := map[string]*obsAccum{}
	for _, p := range packets {
		if p.ObserverID == "" {
			continue
		}
		o := obsMap[p.ObserverID]
		if o == nil {
			o = &obsAccum{name: p.ObserverName, first: p.FirstSeen, last: p.FirstSeen}
			obsMap[p.ObserverID] = o
		}
		o.count++
		if p.SNR != nil {
			o.snrSum += *p.SNR
			o.snrCount++
		}
		if p.RSSI != nil {
			o.rssiSum += *p.RSSI
			o.rssiCount++
		}
		if p.FirstSeen.Before(o.first) {
			o.first = p.FirstSeen
		}
		if p.FirstSeen.After(o.last) {
			o.last = p.FirstSeen
		}
	}
	observerCoverage := make([]NodeObserverStatsResp, 0, len(obsMap))
	for id, o := range obsMap {
		var avgSnr, avgRssi interface{}
		if o.snrCount > 0 {
			avgSnr = o.snrSum / float64(o.snrCount)
		}
		if o.rssiCount > 0 {
			avgRssi = o.rssiSum / float64(o.rssiCount)
		}
		observerCoverage = append(observerCoverage, NodeObserverStatsResp{
			ObserverID:   id,
			ObserverName: o.name,
			PacketCount:  o.count,
			AvgSnr:       avgSnr,
			AvgRssi:      avgRssi,
			FirstSeen:    o.first,
			LastSeen:     o.last,
		})
	}
	sort.Slice(observerCoverage, func(i, j int) bool {
		return observerCoverage[i].PacketCount > observerCoverage[j].PacketCount
	})

	// Hop distribution
	hopCounts := map[string]int{}
	totalWithPath := 0
	relayedCount := 0
	for _, p := range packets {
		hops := txGetParsedPath(p)
		if len(hops) > 0 {
			key := fmt.Sprintf("%d", len(hops))
			if len(hops) >= 4 {
				key = "4+"
			}
			hopCounts[key]++
			totalWithPath++
			if len(hops) > 1 {
				relayedCount++
			}
		} else {
			hopCounts["0"]++
		}
	}
	hopDistribution := make([]HopDistEntry, 0)
	for _, h := range []string{"0", "1", "2", "3", "4+"} {
		if c, ok := hopCounts[h]; ok {
			hopDistribution = append(hopDistribution, HopDistEntry{Hops: h, Count: c})
		}
	}

	// Peer interactions
	type peerAccum struct {
		key, name   string
		count       int
		lastContact time.Time
	}
	peerMap := map[string]*peerAccum{}
	for _, p := range packets {
		if p.DecodedJSON == "" {
			continue
		}
		decoded := p.ParsedDecoded()
		if decoded == nil {
			continue
		}
		type candidate struct{ key, name string }
		var candidates []candidate
		if sk, ok := decoded["sender_key"].(string); ok && sk != "" && sk != pubkey {
			sn, _ := decoded["sender_name"].(string)
			if sn == "" {
				sn, _ = decoded["sender_short_name"].(string)
			}
			candidates = append(candidates, candidate{sk, sn})
		}
		if rk, ok := decoded["recipient_key"].(string); ok && rk != "" && rk != pubkey {
			rn, _ := decoded["recipient_name"].(string)
			if rn == "" {
				rn, _ = decoded["recipient_short_name"].(string)
			}
			candidates = append(candidates, candidate{rk, rn})
		}
		if pk, ok := decoded["pubkey"].(string); ok && pk != "" && pk != pubkey {
			nm, _ := decoded["name"].(string)
			candidates = append(candidates, candidate{pk, nm})
		}
		for _, c := range candidates {
			if c.key == "" {
				continue
			}
			pm := peerMap[c.key]
			if pm == nil {
				pn := c.name
				if pn == "" && len(c.key) >= 12 {
					pn = c.key[:12]
				}
				pm = &peerAccum{key: c.key, name: pn, lastContact: p.FirstSeen}
				peerMap[c.key] = pm
			}
			pm.count++
			if p.FirstSeen.After(pm.lastContact) {
				pm.lastContact = p.FirstSeen
			}
		}
	}
	peerSlice := make([]PeerInteraction, 0, len(peerMap))
	for _, pm := range peerMap {
		peerSlice = append(peerSlice, PeerInteraction{
			PeerKey: pm.key, PeerName: pm.name,
			MessageCount: pm.count, LastContact: pm.lastContact,
		})
	}
	sort.Slice(peerSlice, func(i, j int) bool {
		return peerSlice[i].MessageCount > peerSlice[j].MessageCount
	})
	if len(peerSlice) > 20 {
		peerSlice = peerSlice[:20]
	}

	// Uptime heatmap
	heatBuckets := map[string]*HeatmapCell{}
	for _, p := range packets {
		dow := int(p.FirstSeen.UTC().Weekday())
		hr := p.FirstSeen.UTC().Hour()
		k := fmt.Sprintf("%d:%d", dow, hr)
		if heatBuckets[k] == nil {
			heatBuckets[k] = &HeatmapCell{DayOfWeek: dow, Hour: hr}
		}
		heatBuckets[k].Count++
	}
	uptimeHeatmap := make([]HeatmapCell, 0, len(heatBuckets))
	for _, cell := range heatBuckets {
		uptimeHeatmap = append(uptimeHeatmap, *cell)
	}

	// Computed stats
	totalPackets := len(packets)
	distinctHours := len(activityTimeline)
	totalHours := float64(days) * 24
	availabilityPct := 0.0
	if totalHours > 0 {
		availabilityPct = round(float64(distinctHours)*100.0/totalHours, 1)
		if availabilityPct > 100 {
			availabilityPct = 100
		}
	}

	var avgPacketsPerDay float64
	if days > 0 {
		avgPacketsPerDay = round(float64(totalPackets)/float64(days), 1)
	}

	// Longest silence
	var longestSilenceMs int
	var longestSilenceStart interface{}
	if len(activityTimeline) >= 2 {
		for i := 1; i < len(activityTimeline); i++ {
			var t1Str, t2Str string
			if activityTimeline[i-1].Bucket != nil {
				t1Str = *activityTimeline[i-1].Bucket
			}
			if activityTimeline[i].Bucket != nil {
				t2Str = *activityTimeline[i].Bucket
			}
			t1, e1 := time.Parse(time.RFC3339, t1Str)
			t2, e2 := time.Parse(time.RFC3339, t2Str)
			if e1 == nil && e2 == nil {
				gap := int(t2.Sub(t1).Milliseconds())
				if gap > longestSilenceMs {
					longestSilenceMs = gap
					longestSilenceStart = t1Str
				}
			}
		}
	}

	// Signal grade & SNR stats
	var snrMean, snrStdDev float64
	if len(snrTrend) > 0 {
		var sum float64
		for _, e := range snrTrend {
			if v, ok := e.SNR.(float64); ok {
				sum += v
			}
		}
		snrMean = sum / float64(len(snrTrend))
		if len(snrTrend) > 1 {
			var sqSum float64
			for _, e := range snrTrend {
				if v, ok := e.SNR.(float64); ok {
					sqSum += (v - snrMean) * (v - snrMean)
				}
			}
			snrStdDev = math.Sqrt(sqSum / float64(len(snrTrend)))
		}
	}

	signalGrade := "D"
	if snrMean > 15 && snrStdDev < 2 {
		signalGrade = "A"
	} else if snrMean > 15 {
		signalGrade = "A-"
	} else if snrMean > 12 && snrStdDev < 3 {
		signalGrade = "B+"
	} else if snrMean > 8 {
		signalGrade = "B"
	} else if snrMean > 3 {
		signalGrade = "C"
	}

	var relayPct float64
	if totalWithPath > 0 {
		relayPct = round(float64(relayedCount)*100.0/float64(totalWithPath), 1)
	}

	// Compute clock skew (already under RLock).
	clockSkew := s.getNodeClockSkewLocked(pubkey)

	return &NodeAnalyticsResponse{
		Node:                node,
		TimeRange:           TimeRangeResp{From: fromTime, To: toTime, Days: days},
		ActivityTimeline:    activityTimeline,
		SnrTrend:            snrTrend,
		PacketTypeBreakdown: packetTypeBreakdown,
		ObserverCoverage:    observerCoverage,
		HopDistribution:     hopDistribution,
		PeerInteractions:    peerSlice,
		UptimeHeatmap:       uptimeHeatmap,
		ComputedStats: ComputedNodeStats{
			AvailabilityPct:     availabilityPct,
			LongestSilenceMs:    longestSilenceMs,
			LongestSilenceStart: longestSilenceStart,
			SignalGrade:         signalGrade,
			SnrMean:             round(snrMean, 1),
			SnrStdDev:           round(snrStdDev, 1),
			RelayPct:            relayPct,
			TotalPackets:        totalPackets,
			UniqueObservers:     len(observerCoverage),
			UniquePeers:         len(peerSlice),
			AvgPacketsPerDay:    avgPacketsPerDay,
		},
		ClockSkew: nodeClockSkewResponsePtr(clockSkew),
	}, nil
}

// GetAnalyticsSubpathsWithWindow is the window-aware variant of
// GetAnalyticsSubpaths. Issue #1217: the Route Patterns chart on /analytics
// ignored the Time window filter because callers always reached the unbounded
// path. With a zero TimeWindow this is byte-equivalent to GetAnalyticsSubpaths.
//
// For non-zero windows we iterate the packet list and filter on
// `tx.FirstSeen`. We deliberately do not consult the precomputed `spIndex`
// (which has no per-tx timestamp), so the windowed path is O(N_tx · path²);
// this matches the slow region-filtered path and keeps the fast unbounded
// hot path untouched. Results are cached by (region|area|window) so repeated
// renders of the same window don't re-scan.
func (s *PacketStore) GetAnalyticsSubpathsWithWindow(ctx context.Context, region string, minLen, maxLen, limit int, window TimeWindow) map[string]interface{} {
	if window.IsZero() {
		return s.GetAnalyticsSubpaths(ctx, region, minLen, maxLen, limit)
	}

	cacheKey := fmt.Sprintf("%s|%d|%d|%d|w=%s", region, minLen, maxLen, limit, window.CacheKey())
	s.cacheMu.Lock()
	if cached, ok := s.subpathCache[cacheKey]; ok && time.Now().Before(cached.expiresAt) {
		s.cacheHits++
		s.cacheMu.Unlock()
		return cached.data
	}
	s.cacheMisses++
	s.cacheMu.Unlock()

	result := s.computeAnalyticsSubpathsWindowed(ctx, region, minLen, maxLen, limit, window)

	s.cacheMu.Lock()
	s.subpathCache[cacheKey] = &cachedResult{data: result, expiresAt: time.Now().Add(s.rfCacheTTL)}
	s.cacheMu.Unlock()

	return result
}

// computeAnalyticsSubpathsWindowed iterates s.packets and bounds the result
// to transmissions whose FirstSeen falls inside `window`. Optionally also
// region-filters. Mirrors computeSubpathsSlow but with the window predicate
// inlined and without requiring a non-empty region.
func (s *PacketStore) computeAnalyticsSubpathsWindowed(ctx context.Context, region string, minLen, maxLen, limit int, window TimeWindow) map[string]interface{} {
	var regionObs map[string]bool
	if region != "" {
		regionObs = s.resolveRegionObservers(ctx, region)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	_, pm := s.getCachedNodesAndPM()
	contextPubkeys := buildAggregateHopContextPubkeys(s.packets, pm)
	hopCache := make(map[string]*nodeInfo)
	graph := s.graph.Load()
	resolveHop := func(hop string) string {
		if cached, ok := hopCache[hop]; ok {
			if cached != nil {
				return cached.Name
			}
			return hop
		}
		r, _, _ := pm.resolveWithContext(hop, contextPubkeys, graph)
		hopCache[hop] = r
		if r != nil {
			return r.Name
		}
		return hop
	}

	subpathCounts := make(map[string]*subpathAccum)
	totalPaths := 0

	for _, tx := range s.packets {
		if !window.Includes(tx.FirstSeen) {
			continue
		}
		hops := txGetParsedPath(tx)
		if len(hops) < 2 {
			continue
		}
		if regionObs != nil {
			match := false
			for _, obs := range tx.Observations {
				if regionObs[obs.ObserverID] {
					match = true
					break
				}
			}
			if !match {
				continue
			}
		}
		totalPaths++

		named := make([]string, len(hops))
		for i, h := range hops {
			named[i] = resolveHop(h)
		}

		for l := minLen; l <= maxLen && l <= len(named); l++ {
			for start := 0; start <= len(named)-l; start++ {
				sub := strings.Join(named[start:start+l], " → ")
				raw := strings.Join(hops[start:start+l], ",")
				entry := subpathCounts[sub]
				if entry == nil {
					entry = &subpathAccum{raw: raw}
					subpathCounts[sub] = entry
				}
				entry.count++
			}
		}
	}

	return s.rankSubpaths(subpathCounts, totalPaths, limit)
}

func (s *PacketStore) GetAnalyticsSubpaths(ctx context.Context, region string, minLen, maxLen, limit int) map[string]interface{} {
	cacheKey := fmt.Sprintf("%s|%d|%d|%d", region, minLen, maxLen, limit)

	s.cacheMu.Lock()
	if cached, ok := s.subpathCache[cacheKey]; ok && time.Now().Before(cached.expiresAt) {
		s.cacheHits++
		s.cacheMu.Unlock()
		return cached.data
	}
	s.cacheMisses++
	s.cacheMu.Unlock()

	result := s.computeAnalyticsSubpaths(ctx, region, minLen, maxLen, limit)

	s.cacheMu.Lock()
	s.subpathCache[cacheKey] = &cachedResult{data: result, expiresAt: time.Now().Add(s.rfCacheTTL)}
	s.cacheMu.Unlock()

	return result
}

// GetAnalyticsSubpathsBulkWithWindow is the window-aware variant. For a zero
// TimeWindow it is byte-equivalent to GetAnalyticsSubpathsBulk; for a non-
// zero window it delegates to GetAnalyticsSubpathsWithWindow per group so
// the `tx.FirstSeen` filter is honored (issue #1217).
func (s *PacketStore) GetAnalyticsSubpathsBulkWithWindow(ctx context.Context, region string, groups []subpathGroup, window TimeWindow) []map[string]interface{} {
	if window.IsZero() {
		return s.GetAnalyticsSubpathsBulk(ctx, region, groups)
	}
	results := make([]map[string]interface{}, len(groups))
	for i, g := range groups {
		results[i] = s.GetAnalyticsSubpathsWithWindow(ctx, region, g.MinLen, g.MaxLen, g.Limit, window)
	}
	return results
}

// GetAnalyticsSubpathsBulk returns multiple length-range buckets from a single
// scan of the subpath index, avoiding repeated iterations.
func (s *PacketStore) GetAnalyticsSubpathsBulk(ctx context.Context, region string, groups []subpathGroup) []map[string]interface{} {
	// For region queries or when there are few groups, fall back to individual calls
	// which benefit from per-key caching.
	if region != "" {
		results := make([]map[string]interface{}, len(groups))
		for i, g := range groups {
			results[i] = s.GetAnalyticsSubpaths(ctx, region, g.MinLen, g.MaxLen, g.Limit)
		}
		return results
	}

	// Check if all groups are cached.
	allCached := true
	cachedResults := make([]map[string]interface{}, len(groups))
	s.cacheMu.Lock()
	for i, g := range groups {
		cacheKey := fmt.Sprintf("|%d|%d|%d", g.MinLen, g.MaxLen, g.Limit)
		if cached, ok := s.subpathCache[cacheKey]; ok && time.Now().Before(cached.expiresAt) {
			cachedResults[i] = cached.data
		} else {
			allCached = false
			break
		}
	}
	if allCached {
		s.cacheHits += int64(len(groups))
		s.cacheMu.Unlock()
		return cachedResults
	}
	s.cacheMu.Unlock()

	// Single scan: bucket by hop length into per-group accumulators.
	s.mu.RLock()
	_, pm := s.getCachedNodesAndPM()
	// Aggregate hop-disambiguation context across all packets so the
	// resolver's tiers 1 and 2 light up even on this bulk-aggregate path
	// (the index iterates raw subpath strings, not per-tx). See #1197.
	contextPubkeys := buildAggregateHopContextPubkeys(s.packets, pm)
	hopCache := make(map[string]*nodeInfo)
	graph := s.graph.Load() // hoist out of resolver closure (PR #1208 carmack #1)
	resolveHop := func(hop string) string {
		if cached, ok := hopCache[hop]; ok {
			if cached != nil {
				return cached.Name
			}
			return hop
		}
		r, _, _ := pm.resolveWithContext(hop, contextPubkeys, graph)
		hopCache[hop] = r
		if r != nil {
			return r.Name
		}
		return hop
	}

	perGroup := make([]map[string]*subpathAccum, len(groups))
	for i := range groups {
		perGroup[i] = make(map[string]*subpathAccum)
	}

	for rawKey, count := range s.spIndex {
		hops := strings.Split(rawKey, ",")
		hopLen := len(hops)

		// Resolve hop names once, reuse across groups.
		var named []string
		var namedKey string
		resolved := false

		for gi, g := range groups {
			if hopLen < g.MinLen || hopLen > g.MaxLen {
				continue
			}
			if !resolved {
				named = make([]string, hopLen)
				for i, h := range hops {
					named[i] = resolveHop(h)
				}
				namedKey = strings.Join(named, " → ")
				resolved = true
			}
			entry := perGroup[gi][namedKey]
			if entry == nil {
				entry = &subpathAccum{raw: rawKey}
				perGroup[gi][namedKey] = entry
			}
			entry.count += count
		}
	}
	totalPaths := s.spTotalPaths
	s.mu.RUnlock()

	results := make([]map[string]interface{}, len(groups))
	for i, g := range groups {
		results[i] = s.rankSubpaths(perGroup[i], totalPaths, g.Limit)
	}

	// Cache individual results for future single-key lookups too.
	s.cacheMu.Lock()
	for i, g := range groups {
		cacheKey := fmt.Sprintf("|%d|%d|%d", g.MinLen, g.MaxLen, g.Limit)
		s.subpathCache[cacheKey] = &cachedResult{data: results[i], expiresAt: time.Now().Add(s.rfCacheTTL)}
	}
	s.cacheMu.Unlock()

	return results
}

// subpathAccum holds a running count for a single named subpath.
type subpathAccum struct {
	count int
	raw   string // first raw-hop key seen (used for rawHops in the API response)
}

func (s *PacketStore) computeAnalyticsSubpaths(ctx context.Context, region string, minLen, maxLen, limit int) map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, pm := s.getCachedNodesAndPM()
	// Aggregate hop-disambiguation context across all packets — bulk
	// aggregator over s.spIndex / per-tx fallback both need it. See #1197.
	contextPubkeys := buildAggregateHopContextPubkeys(s.packets, pm)
	hopCache := make(map[string]*nodeInfo)
	graph := s.graph.Load() // hoist out of resolver closure (PR #1208 carmack #1)
	resolveHop := func(hop string) string {
		if cached, ok := hopCache[hop]; ok {
			if cached != nil {
				return cached.Name
			}
			return hop
		}
		r, _, _ := pm.resolveWithContext(hop, contextPubkeys, graph)
		hopCache[hop] = r
		if r != nil {
			return r.Name
		}
		return hop
	}

	// For region queries fall back to packet iteration (region filtering
	// requires per-transmission observer checks).
	if region != "" {
		return s.computeSubpathsSlow(ctx, region, minLen, maxLen, limit, resolveHop)
	}

	// Fast path: read from precomputed raw-hop subpath index.
	// Resolve raw hop prefixes to names and merge counts.
	namedCounts := make(map[string]*subpathAccum, len(s.spIndex))
	for rawKey, count := range s.spIndex {
		hops := strings.Split(rawKey, ",")
		hopLen := len(hops)
		if hopLen < minLen || hopLen > maxLen {
			continue
		}
		named := make([]string, hopLen)
		for i, h := range hops {
			named[i] = resolveHop(h)
		}
		namedKey := strings.Join(named, " → ")
		entry := namedCounts[namedKey]
		if entry == nil {
			entry = &subpathAccum{raw: rawKey}
			namedCounts[namedKey] = entry
		}
		entry.count += count
	}

	return s.rankSubpaths(namedCounts, s.spTotalPaths, limit)
}

// computeSubpathsSlow is the original O(N) packet-iteration path, used only
// for region-filtered queries where we must check per-transmission observers.
func (s *PacketStore) computeSubpathsSlow(ctx context.Context, region string, minLen, maxLen, limit int, resolveHop func(string) string) map[string]interface{} {
	regionObs := s.resolveRegionObservers(ctx, region)

	subpathCounts := make(map[string]*subpathAccum)
	totalPaths := 0

	for _, tx := range s.packets {
		hops := txGetParsedPath(tx)
		if len(hops) < 2 {
			continue
		}
		if regionObs != nil {
			match := false
			for _, obs := range tx.Observations {
				if regionObs[obs.ObserverID] {
					match = true
					break
				}
			}
			if !match {
				continue
			}
		}
		totalPaths++

		named := make([]string, len(hops))
		for i, h := range hops {
			named[i] = resolveHop(h)
		}

		for l := minLen; l <= maxLen && l <= len(named); l++ {
			for start := 0; start <= len(named)-l; start++ {
				sub := strings.Join(named[start:start+l], " → ")
				raw := strings.Join(hops[start:start+l], ",")
				entry := subpathCounts[sub]
				if entry == nil {
					entry = &subpathAccum{raw: raw}
					subpathCounts[sub] = entry
				}
				entry.count++
			}
		}
	}

	return s.rankSubpaths(subpathCounts, totalPaths, limit)
}

// rankSubpaths sorts accumulated subpath counts by frequency, truncates to
// limit, and builds the API response map.
func (s *PacketStore) rankSubpaths(counts map[string]*subpathAccum, totalPaths, limit int) map[string]interface{} {
	type subpathEntry struct {
		path  string
		count int
		raw   string
	}
	ranked := make([]subpathEntry, 0, len(counts))
	for path, data := range counts {
		ranked = append(ranked, subpathEntry{path, data.count, data.raw})
	}
	sort.Slice(ranked, func(i, j int) bool { return ranked[i].count > ranked[j].count })
	if len(ranked) > limit {
		ranked = ranked[:limit]
	}

	subpaths := make([]map[string]interface{}, 0, len(ranked))
	for _, e := range ranked {
		pct := 0.0
		if totalPaths > 0 {
			pct = math.Round(float64(e.count)/float64(totalPaths)*1000) / 10
		}
		subpaths = append(subpaths, map[string]interface{}{
			"path":    e.path,
			"rawHops": strings.Split(e.raw, ","),
			"count":   e.count,
			"hops":    len(strings.Split(e.path, " → ")),
			"pct":     pct,
		})
	}

	return map[string]interface{}{
		"subpaths":   subpaths,
		"totalPaths": totalPaths,
	}
}

// --- Subpath Detail ---

func (s *PacketStore) GetSubpathDetail(rawHops []string) map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, pm := s.getCachedNodesAndPM()

	// Build the subpath key the same way the index does (lowercase, comma-joined)
	spKey := strings.ToLower(strings.Join(rawHops, ","))

	// Direct lookup instead of scanning all packets
	matchedTxs := s.spTxIndex[spKey]

	// Hop-disambiguation context: union over the matched txs that produced
	// this subpath. This is the right scope — those are the packets that
	// witnessed the requested hop sequence. See #1197.
	contextPubkeys := buildAggregateHopContextPubkeys(matchedTxs, pm)
	// Hoist atomic graph.Load() once for the whole request (PR #1208
	// carmack #1) — used by the rawHops loop AND the matched-tx loop below.
	graph := s.graph.Load()

	// Resolve the requested hops
	nodes := make([]map[string]interface{}, len(rawHops))
	for i, hop := range rawHops {
		r, _, _ := pm.resolveWithContext(hop, contextPubkeys, graph)
		entry := map[string]interface{}{"hop": hop, "name": hop, "lat": nil, "lon": nil, "pubkey": nil}
		if r != nil {
			entry["name"] = r.Name
			entry["pubkey"] = r.PublicKey
			if r.HasGPS {
				entry["lat"] = r.Lat
				entry["lon"] = r.Lon
			}
		}
		nodes[i] = entry
	}

	hourBuckets := make([]int, 24)
	var snrSum, rssiSum float64
	var snrCount, rssiCount int
	observers := map[string]int{}
	parentPaths := map[string]int{}
	matchCount := len(matchedTxs)
	var firstSeen, lastSeen time.Time

	for _, tx := range matchedTxs {
		ts := tx.FirstSeen
		if !ts.IsZero() {
			if firstSeen.IsZero() || ts.Before(firstSeen) {
				firstSeen = ts
			}
			if lastSeen.IsZero() || ts.After(lastSeen) {
				lastSeen = ts
			}
			hourBuckets[ts.UTC().Hour()]++
		}
		if tx.SNR != nil {
			snrSum += *tx.SNR
			snrCount++
		}
		if tx.RSSI != nil {
			rssiSum += *tx.RSSI
			rssiCount++
		}
		if tx.ObserverName != "" {
			observers[tx.ObserverName]++
		}

		// Full parent path (resolved). Per-tx context so the resolver picks
		// the right candidate when prefixes are ambiguous. See #1197.
		txCtx := buildHopContextPubkeys(tx, pm)
		hops := txGetParsedPath(tx)
		resolved := make([]string, len(hops))
		for i, h := range hops {
			r, _, _ := pm.resolveWithContext(h, txCtx, graph)
			if r != nil {
				resolved[i] = r.Name
			} else {
				resolved[i] = h
			}
		}
		fullPath := strings.Join(resolved, " → ")
		parentPaths[fullPath]++
	}

	var avgSnr, avgRssi interface{}
	if snrCount > 0 {
		avgSnr = snrSum / float64(snrCount)
	}
	if rssiCount > 0 {
		avgRssi = rssiSum / float64(rssiCount)
	}

	topParents := make([]map[string]interface{}, 0)
	for path, count := range parentPaths {
		topParents = append(topParents, map[string]interface{}{"path": path, "count": count})
	}
	sort.Slice(topParents, func(i, j int) bool {
		return topParents[i]["count"].(int) > topParents[j]["count"].(int)
	})
	if len(topParents) > 15 {
		topParents = topParents[:15]
	}

	topObs := make([]map[string]interface{}, 0)
	for name, count := range observers {
		topObs = append(topObs, map[string]interface{}{"name": name, "count": count})
	}
	sort.Slice(topObs, func(i, j int) bool {
		return topObs[i]["count"].(int) > topObs[j]["count"].(int)
	})
	if len(topObs) > 10 {
		topObs = topObs[:10]
	}

	return map[string]interface{}{
		"hops":             rawHops,
		"nodes":            nodes,
		"totalMatches":     matchCount,
		"firstSeen":        firstSeen,
		"lastSeen":         lastSeen,
		"signal":           map[string]interface{}{"avgSnr": avgSnr, "avgRssi": avgRssi, "samples": snrCount},
		"hourDistribution": hourBuckets,
		"parentPaths":      topParents,
		"observers":        topObs,
	}
}

// BackgroundLoadError returns the human-readable reason captured the last
// time backgroundLoadFailed flipped true. Empty string when no error has
// been recorded (either load is still running, succeeded, or has not run).
// See #1690 for the failure modes this surfaces.
func (s *PacketStore) BackgroundLoadError() string {
	s.bgErrMu.RLock()
	defer s.bgErrMu.RUnlock()
	return s.backgroundLoadErr
}

// LoadCoverageRatio returns the most recent totalLoaded / totalInDB ratio
// computed by loadBackgroundChunks. 0.0 until the background loader has
// run at least once. See #1690.
func (s *PacketStore) LoadCoverageRatio() float64 {
	s.bgErrMu.RLock()
	defer s.bgErrMu.RUnlock()
	return s.loadCoverageRatio
}
