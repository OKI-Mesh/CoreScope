package main

import (
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// Types generated from proto/ definitions for compile-time type safety.
// Every API response is a typed struct — no map[string]interface{}.

// ─── Common ────────────────────────────────────────────────────────────────────

type PaginationInfo struct {
	Total  int `json:"total"`
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
}

type ErrorResp struct {
	Error string `json:"error"`
}

type OkResp struct {
	Ok bool `json:"ok"`
}

type TimeBucket struct {
	Label  *string `json:"label,omitempty"`
	Count  int     `json:"count"`
	Bucket *string `json:"bucket,omitempty"`
}

// ─── Stats ─────────────────────────────────────────────────────────────────────

type StatsResponse struct {
	TotalPackets          int            `json:"totalPackets"`
	TotalTransmissions    *int           `json:"totalTransmissions"`
	TotalObservations     int            `json:"totalObservations"`
	TotalNodes            int            `json:"totalNodes"`
	TotalNodesAllTime     int            `json:"totalNodesAllTime"`
	TotalObservers        int            `json:"totalObservers"`
	PacketsLastHour       int            `json:"packetsLastHour"`
	PacketsLast24h        int            `json:"packetsLast24h"`
	Engine                string         `json:"engine"`
	Version               string         `json:"version"`
	Commit                string         `json:"commit"`
	BuildTime             string         `json:"buildTime"`
	Counts                api.RoleCounts `json:"counts"`
	SignatureDrops        int64          `json:"signatureDrops,omitempty"`
	HashMigrationComplete bool           `json:"hashMigrationComplete"`

	// Memory accounting (issue #832). All values in MB.
	//
	// StoreDataMB ("trackedMB" historically) is the in-store packet byte
	// estimate — useful packet bytes only. Subset of HeapInuse. Used as
	// the eviction watermark input. NOT a proxy for RSS; ops dashboards
	// should prefer ProcessRSSMB for capacity decisions.
	//
	// Old field name TrackedMB is retained for backward compatibility
	// with pre-v3.6 consumers; it carries the same value as StoreDataMB
	// and is deprecated.
	TrackedMB     float64 `json:"trackedMB"`     // deprecated alias for storeDataMB
	StoreDataMB   float64 `json:"storeDataMB"`   // in-store packet bytes (subset of heap)
	ProcessRSSMB  float64 `json:"processRSSMB"`  // process RSS from /proc (Linux) or runtime.Sys fallback
	GoHeapInuseMB float64 `json:"goHeapInuseMB"` // runtime.MemStats.HeapInuse
	GoSysMB       float64 `json:"goSysMB"`       // runtime.MemStats.Sys (total Go-managed)

	// NeighborGraphCacheRebuildFailures counts panic/marshal failures in the
	// background neighbor-graph cache recomputer. Non-zero = stale snapshot
	// being served indefinitely. Surfaced for operator visibility. #1483 follow-up.
	NeighborGraphCacheRebuildFailures uint64 `json:"neighborGraphCacheRebuildFailures"`
}

// ─── Scope Stats ───────────────────────────────────────────────────────────────

type ScopeStatsSummary struct {
	TransportTotal int `json:"transportTotal"`
	Scoped         int `json:"scoped"`
	Unscoped       int `json:"unscoped"`
	UnknownScope   int `json:"unknownScope"`
}

type ScopeRegionCount struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type ScopeTimePoint struct {
	T        string `json:"t"`
	Scoped   int    `json:"scoped"`
	Unscoped int    `json:"unscoped"`
}

// ScopeAdvertRoleCount is one row of the adverts-by-role breakdown (#1979):
// flood adverts sent by nodes of one role, split by the three scope_name
// states. Role is the sender's nodes.role, or "unknown" when the advert has
// no from_pubkey (legacy row not yet reached by the #1143 backfill), when the
// sender has no nodes row (also after MoveStaleNodes moved it to
// inactive_nodes, possible inside the 7d window only with
// retention.nodeDays < 7), or when its role is empty.
type ScopeAdvertRoleCount struct {
	Role         string `json:"role"`
	Unscoped     int    `json:"unscoped"`
	UnknownScope int    `json:"unknownScope"`
	Named        int    `json:"named"`
}

type ScopeStatsResponse struct {
	Window        string                 `json:"window"`
	Summary       ScopeStatsSummary      `json:"summary"`
	ByRegion      []ScopeRegionCount     `json:"byRegion"`
	TimeSeries    []ScopeTimePoint       `json:"timeSeries"`
	AdvertsByRole []ScopeAdvertRoleCount `json:"advertsByRole"`
}

// ─── Health ────────────────────────────────────────────────────────────────────

type MemoryStats struct {
	RSS       int `json:"rss"`
	HeapUsed  int `json:"heapUsed"`
	HeapTotal int `json:"heapTotal"`
	External  int `json:"external"`
}

type EventLoopStats struct {
	CurrentLagMs float64 `json:"currentLagMs"`
	MaxLagMs     float64 `json:"maxLagMs"`
	P50Ms        float64 `json:"p50Ms"`
	P95Ms        float64 `json:"p95Ms"`
	P99Ms        float64 `json:"p99Ms"`
}

type CacheStats struct {
	Entries    int     `json:"entries"`
	Hits       int64   `json:"hits"`
	Misses     int64   `json:"misses"`
	StaleHits  int     `json:"staleHits"`
	Recomputes int64   `json:"recomputes"`
	HitRate    float64 `json:"hitRate"`
}

// PerfCacheStats uses "size" key instead of "entries" (matching Node.js /api/perf shape).
type PerfCacheStats struct {
	Size       int     `json:"size"`
	Hits       int64   `json:"hits"`
	Misses     int64   `json:"misses"`
	StaleHits  int     `json:"staleHits"`
	Recomputes int64   `json:"recomputes"`
	HitRate    float64 `json:"hitRate"`
}

type WebSocketStatsResp struct {
	Clients int `json:"clients"`
	// #1794: upgrade rejections since boot, split by cause so an operator can
	// tell a deny-list hit from a client that is merely reconnecting too fast.
	// omitempty: an install with no limits configured sends none of these
	// rather than three permanent zeroes.
	RejectedDeny    int64 `json:"rejectedDeny,omitempty"`
	RejectedRate    int64 `json:"rejectedRate,omitempty"`
	RejectedConnCap int64 `json:"rejectedConnCap,omitempty"`
}

type HealthPacketStoreStats struct {
	Packets     int     `json:"packets"`
	EstimatedMB float64 `json:"estimatedMB"`
	TrackedMB   float64 `json:"trackedMB"`
}

type SlowQuery struct {
	Path   string  `json:"path"`
	Ms     float64 `json:"ms"`
	Time   string  `json:"time"`
	Status int     `json:"status"`
}

type HealthPerfStats struct {
	TotalRequests int         `json:"totalRequests"`
	AvgMs         float64     `json:"avgMs"`
	SlowQueries   int         `json:"slowQueries"`
	RecentSlow    []SlowQuery `json:"recentSlow"`
}

type HealthResponse struct {
	Status      string                 `json:"status"`
	Engine      string                 `json:"engine"`
	Version     string                 `json:"version"`
	Commit      string                 `json:"commit"`
	BuildTime   string                 `json:"buildTime"`
	Uptime      int                    `json:"uptime"`
	UptimeHuman string                 `json:"uptimeHuman"`
	Memory      MemoryStats            `json:"memory"`
	EventLoop   EventLoopStats         `json:"eventLoop"`
	Cache       CacheStats             `json:"cache"`
	WebSocket   WebSocketStatsResp     `json:"websocket"`
	PacketStore HealthPacketStoreStats `json:"packetStore"`
	Perf        HealthPerfStats        `json:"perf"`
}

// ─── Perf ──────────────────────────────────────────────────────────────────────

type EndpointStatsResp struct {
	Count int     `json:"count"`
	AvgMs float64 `json:"avgMs"`
	P50Ms float64 `json:"p50Ms"`
	P95Ms float64 `json:"p95Ms"`
	MaxMs float64 `json:"maxMs"`
}

type PacketStoreIndexes struct {
	ByHash           int `json:"byHash"`
	ByObserver       int `json:"byObserver"`
	ByNode           int `json:"byNode"`
	AdvertByObserver int `json:"advertByObserver"`
}

type PerfPacketStoreStats struct {
	TotalLoaded            int                `json:"totalLoaded"`
	TotalObservations      int                `json:"totalObservations"`
	Evicted                int                `json:"evicted"`
	Inserts                int64              `json:"inserts"`
	Queries                int64              `json:"queries"`
	InMemory               int                `json:"inMemory"`
	SqliteOnly             bool               `json:"sqliteOnly"`
	MaxPackets             int                `json:"maxPackets"`
	EstimatedMB            float64            `json:"estimatedMB"`
	TrackedMB              float64            `json:"trackedMB"`
	AvgBytesPerPacket      int64              `json:"avgBytesPerPacket"`
	MaxMB                  int                `json:"maxMB"`
	Indexes                PacketStoreIndexes `json:"indexes"`
	HotStartupHours        float64            `json:"hotStartupHours"`
	BackgroundLoadComplete bool               `json:"backgroundLoadComplete"`
	BackgroundLoadFailed   bool               `json:"backgroundLoadFailed"`
	BackgroundLoadProgress int64              `json:"backgroundLoadProgress"`
	BackgroundLoadError    string             `json:"backgroundLoadError,omitempty"`
	// #1690: surface retention + coverage so operators can see how much
	// of the on-disk DB the in-memory store currently reflects.
	RetentionHours    float64   `json:"retentionHours"`
	OldestLoaded      time.Time `json:"oldestLoaded"`
	LoadCoverageRatio float64   `json:"loadCoverageRatio"`
}

type CheckpointStats struct {
	Timed          int64 `json:"timed"`
	Requested      int64 `json:"requested"`
	BuffersWritten int64 `json:"buffersWritten"`
}

type TableRowCounts struct {
	Transmissions int64 `json:"transmissions"`
	Observations  int64 `json:"observations"`
	Nodes         int64 `json:"nodes"`
	Observers     int64 `json:"observers"`
}

type DbStats struct {
	DbSizeMB    float64          `json:"dbSizeMB"`
	WalSizeMB   float64          `json:"walSizeMB"`
	DeadTuples  int64            `json:"deadTuples"`
	Checkpoints *CheckpointStats `json:"checkpoints,omitempty"`
	Rows        TableRowCounts   `json:"rows"`
}

type PerfResponse struct {
	Uptime        int                           `json:"uptime"`
	TotalRequests int64                         `json:"totalRequests"`
	AvgMs         float64                       `json:"avgMs"`
	Endpoints     map[string]*EndpointStatsResp `json:"endpoints"`
	SlowQueries   []SlowQuery                   `json:"slowQueries"`
	Cache         PerfCacheStats                `json:"cache"`
	PacketStore   *PerfPacketStoreStats         `json:"packetStore"`
	DatabaseStats *DbStats                      `json:"dbStats"`
	GoRuntime     *GoRuntimeStats               `json:"goRuntime,omitempty"`
	// MemoryBreakdown is populated only for /api/perf?mem=1 (an O(tx+obs)
	// walk, opt-in so the normal hot endpoint stays cheap). It sizes the
	// flood-forward multiplication (store memory diagnostics) and where the
	// store's string bytes go.
	MemoryBreakdown *StoreMemoryBreakdown `json:"memoryBreakdown,omitempty"`
	// MemoryBreakdownNote documents the accounting scope of MemoryBreakdown.
	// It lives here (one occurrence) rather than repeating in every breakdown.
	MemoryBreakdownNote string `json:"memoryBreakdownNote,omitempty"`
}

// StoreMemoryBreakdown is the opt-in /api/perf?mem=1 diagnostic: the
// flood-forward (route_type 0/1) share of stored transmissions and a
// per-component breakdown of the string bytes held in the packet store.
type StoreMemoryBreakdown struct {
	TotalTx            int     `json:"totalTx"`
	FloodTx            int     `json:"floodTx"`         // route_type 0 or 1
	FloodTxSharePct    float64 `json:"floodTxSharePct"` // flood share of stored tx
	Observations       int     `json:"observations"`
	ObsPerTx           float64 `json:"obsPerTx"`
	TxRawHexMB         float64 `json:"txRawHexMB"`
	TxDecodedJsonMB    float64 `json:"txDecodedJsonMB"`
	TxPathJsonMB       float64 `json:"txPathJsonMB"`
	ObsPathJsonMB      float64 `json:"obsPathJsonMB"`
	ObsStringsMB       float64 `json:"obsStringsMB"` // observerID/name/iata/direction/timestamp
	FloodTxEstimatedMB float64 `json:"floodTxEstimatedMB"`
	TotalTxEstimatedMB float64 `json:"totalTxEstimatedMB"`
}

// GoRuntimeStats holds Go runtime metrics for the perf endpoint.
type GoRuntimeStats struct {
	Goroutines   int     `json:"goroutines"`
	NumGC        uint32  `json:"numGC"`
	PauseTotalMs float64 `json:"pauseTotalMs"`
	LastPauseMs  float64 `json:"lastPauseMs"`
	HeapAllocMB  float64 `json:"heapAllocMB"`
	HeapSysMB    float64 `json:"heapSysMB"`
	HeapInuseMB  float64 `json:"heapInuseMB"`
	HeapIdleMB   float64 `json:"heapIdleMB"`
	NumCPU       int     `json:"numCPU"`
}

type DecodeResponse struct {
	Decoded interface{} `json:"decoded"`
}

// ─── Nodes ─────────────────────────────────────────────────────────────────────

// type NodeDetailResponse struct {
// 	Node          api.Node             `json:"node"`
// 	RecentAdverts []api.NodeAdvert `json:"recentAdverts"`
// }

type NodeStatsResp struct {
	TotalTransmissions int         `json:"totalTransmissions"`
	TotalObservations  int         `json:"totalObservations"`
	TotalPackets       int         `json:"totalPackets"`
	PacketsToday       int         `json:"packetsToday"`
	AvgSnr             interface{} `json:"avgSnr"`
	LastHeard          interface{} `json:"lastHeard"`
	AvgHops            interface{} `json:"avgHops,omitempty"`
}

type NodeObserverStatsResp struct {
	ObserverID   interface{} `json:"observer_id"`
	ObserverName interface{} `json:"observer_name"`
	PacketCount  int         `json:"packetCount"`
	AvgSnr       interface{} `json:"avgSnr"`
	AvgRssi      interface{} `json:"avgRssi"`
	IATA         interface{} `json:"iata,omitempty"`
	FirstSeen    interface{} `json:"firstSeen,omitempty"`
	LastSeen     interface{} `json:"lastSeen,omitempty"`
}

type BulkHealthEntry struct {
	PublicKey string                  `json:"public_key"`
	Name      interface{}             `json:"name"`
	Role      interface{}             `json:"role"`
	Lat       interface{}             `json:"lat"`
	Lon       interface{}             `json:"lon"`
	Stats     NodeStatsResp           `json:"stats"`
	Observers []NodeObserverStatsResp `json:"observers"`
}

type NetworkStatusResponse struct {
	Total      int            `json:"total"`
	Active     int            `json:"active"`
	Degraded   int            `json:"degraded"`
	Silent     int            `json:"silent"`
	RoleCounts map[string]int `json:"roleCounts"`
}

// ─── Paths ─────────────────────────────────────────────────────────────────────

type PathHopResp struct {
	Prefix string      `json:"prefix"`
	Name   string      `json:"name"`
	Pubkey interface{} `json:"pubkey"`
	Lat    interface{} `json:"lat"`
	Lon    interface{} `json:"lon"`
}

type PathEntryResp struct {
	Hops       []PathHopResp `json:"hops"`
	Count      int           `json:"count"`
	LastSeen   interface{}   `json:"lastSeen"`
	SampleHash string        `json:"sampleHash"`
}

type NodePathsResponse struct {
	Node               map[string]interface{} `json:"node"`
	Paths              []PathEntryResp        `json:"paths"`
	TotalPaths         int                    `json:"totalPaths"`
	TotalTransmissions int                    `json:"totalTransmissions"`
}

// ─── Node Analytics ────────────────────────────────────────────────────────────

type TimeRangeResp struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
	Days int       `json:"days"`
}

type SnrTrendEntry struct {
	Timestamp    time.Time   `json:"timestamp"`
	SNR          interface{} `json:"snr"`
	RSSI         interface{} `json:"rssi"`
	ObserverID   interface{} `json:"observer_id"`
	ObserverName interface{} `json:"observer_name"`
}

type PayloadTypeCount struct {
	PayloadType types.PayloadType `json:"payload_type"`
	Count       int               `json:"count"`
}

type HopDistEntry struct {
	Hops  string `json:"hops"`
	Count int    `json:"count"`
}

type PeerInteraction struct {
	PeerKey      string    `json:"peer_key"`
	PeerName     string    `json:"peer_name"`
	MessageCount int       `json:"messageCount"`
	LastContact  time.Time `json:"lastContact"`
}

type HeatmapCell struct {
	DayOfWeek int `json:"dayOfWeek"`
	Hour      int `json:"hour"`
	Count     int `json:"count"`
}

type ComputedNodeStats struct {
	AvailabilityPct     float64     `json:"availabilityPct"`
	LongestSilenceMs    int         `json:"longestSilenceMs"`
	LongestSilenceStart interface{} `json:"longestSilenceStart"`
	SignalGrade         string      `json:"signalGrade"`
	SnrMean             float64     `json:"snrMean"`
	SnrStdDev           float64     `json:"snrStdDev"`
	RelayPct            float64     `json:"relayPct"`
	TotalPackets        int         `json:"totalPackets"`
	UniqueObservers     int         `json:"uniqueObservers"`
	UniquePeers         int         `json:"uniquePeers"`
	AvgPacketsPerDay    float64     `json:"avgPacketsPerDay"`
}

type NodeAnalyticsResponse struct {
	Node                api.Node                `json:"node"`
	TimeRange           TimeRangeResp           `json:"timeRange"`
	ActivityTimeline    []TimeBucket            `json:"activityTimeline"`
	SnrTrend            []SnrTrendEntry         `json:"snrTrend"`
	PacketTypeBreakdown []PayloadTypeCount      `json:"packetTypeBreakdown"`
	ObserverCoverage    []NodeObserverStatsResp `json:"observerCoverage"`
	HopDistribution     []HopDistEntry          `json:"hopDistribution"`
	PeerInteractions    []PeerInteraction       `json:"peerInteractions"`
	UptimeHeatmap       []HeatmapCell           `json:"uptimeHeatmap"`
	ComputedStats       ComputedNodeStats       `json:"computedStats"`
	ClockSkew           *api.NodeClockSkew      `json:"clockSkew,omitempty"`
}

// NodeHopPacket is one flood packet this node forwarded, with the hop count
// the node's flood.max check saw for it (issue #1812).
type NodeHopPacket struct {
	Hash      string   `json:"hash"`
	Timestamp UTCTime  `json:"timestamp"`
	Hops      int      `json:"hops"`
	Tags      []string `json:"tags"`
}

type NodeHopAnalyticsResponse struct {
	TimeRange TimeRangeResp   `json:"timeRange"`
	Packets   []NodeHopPacket `json:"packets"`
	Ambiguous int             `json:"ambiguous"`
}

// ─── Resolve Hops ──────────────────────────────────────────────────────────────

type HopCandidate struct {
	Name          interface{} `json:"name"`
	Pubkey        string      `json:"pubkey"`
	Lat           interface{} `json:"lat"`
	Lon           interface{} `json:"lon"`
	AffinityScore *float64    `json:"affinityScore"`
}

type HopResolution struct {
	Name          interface{}    `json:"name"`
	Pubkey        interface{}    `json:"pubkey,omitempty"`
	Ambiguous     *bool          `json:"ambiguous,omitempty"`
	Candidates    []HopCandidate `json:"candidates"`
	Conflicts     []interface{}  `json:"conflicts"`
	BestCandidate *string        `json:"bestCandidate,omitempty"`
	Confidence    string         `json:"confidence,omitempty"`
}

type ResolveHopsResponse struct {
	Resolved map[string]*HopResolution `json:"resolved"`
}

// ─── Config ────────────────────────────────────────────────────────────────────

type ThemeResponse struct {
	Branding   map[string]interface{} `json:"branding"`
	Theme      map[string]interface{} `json:"theme"`
	ThemeDark  map[string]interface{} `json:"themeDark"`
	NodeColors map[string]interface{} `json:"nodeColors"`
	TypeColors map[string]interface{} `json:"typeColors"`
	Home       interface{}            `json:"home"`
	// #1488 — marker stroke overlay so the frontend can apply server-side
	// defaults before the operator's localStorage override loads.
	MarkerStroke map[string]interface{} `json:"markerStroke,omitempty"`
}

type MapConfigResponse struct {
	Center []float64 `json:"center"`
	Zoom   int       `json:"zoom"`
}

type ClientConfigResponse struct {
	Roles               interface{}            `json:"roles"`
	HealthThresholds    interface{}            `json:"healthThresholds"`
	Map                 interface{}            `json:"map"`
	Tiles               interface{}            `json:"tiles,omitempty"` // deprecated
	SnrThresholds       interface{}            `json:"snrThresholds"`
	DistThresholds      interface{}            `json:"distThresholds"`
	MaxHopDist          interface{}            `json:"maxHopDist"`
	Limits              interface{}            `json:"limits"`
	PerfSlowMs          interface{}            `json:"perfSlowMs"`
	WsReconnectMs       interface{}            `json:"wsReconnectMs"`
	CacheInvalidateMs   interface{}            `json:"cacheInvalidateMs"`
	ExternalUrls        interface{}            `json:"externalUrls"`
	PropagationBufferMs float64                `json:"propagationBufferMs"`
	LiveMapMaxNodes     int                    `json:"liveMapMaxNodes"`
	Timestamps          TimestampConfig        `json:"timestamps"`
	DebugAffinity       bool                   `json:"debugAffinity,omitempty"`
	MapDarkTileProvider string                 `json:"mapDarkTileProvider,omitempty"` // deprecated. TODO: remove after v3.5.0
	Customizer          CustomizerClientConfig `json:"customizer"`
	ClientRxCoverage    bool                   `json:"clientRxCoverage"`
	PathTrust           *PathTrustConfig       `json:"pathTrust,omitempty"`
}

// CustomizerClientConfig is the operator-side customizer-modal knobs that
// /api/config/client surfaces to the frontend. Issue #1508. The field is
// always present (DisabledTabs defaults to an empty slice) so the frontend
// can blindly call `.disabledTabs.includes(...)` without an undefined guard.
type CustomizerClientConfig struct {
	DisabledTabs []string `json:"disabledTabs"`
}

// ─── IATA Coords ───────────────────────────────────────────────────────────────

type IataCoord struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type IataCoordsResponse struct {
	Coords map[string]IataCoord `json:"coords"`
}

// ─── Audio Lab ─────────────────────────────────────────────────────────────────

type AudioLabPacket struct {
	Hash             string            `json:"hash"`
	RawHex           string            `json:"raw_hex"`
	DecodedJSON      string            `json:"decoded_json"`
	ObservationCount int               `json:"observation_count"`
	PayloadType      types.PayloadType `json:"payload_type"`
	PathJSON         string            `json:"path_json"`
	ObserverID       string            `json:"observer_id"`
	Timestamp        time.Time         `json:"timestamp"`
}

type AudioLabBucketsResponse struct {
	Buckets map[string][]AudioLabPacket `json:"buckets"`
}

// ─── WebSocket ─────────────────────────────────────────────────────────────────

type WSMessage struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}
