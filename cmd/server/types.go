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

// ─── Config ────────────────────────────────────────────────────────────────────

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
