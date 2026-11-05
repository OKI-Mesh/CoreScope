package main

import (
	"sort"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

type NodeResponse struct {
	PublicKey            string          `json:"public_key"`
	Name                 *string         `json:"name"`
	Role                 *types.NodeRole `json:"role"`
	Lat                  *float64        `json:"lat"`
	Lon                  *float64        `json:"lon"`
	LastSeen             *string         `json:"last_seen"`
	FirstSeen            *string         `json:"first_seen"`
	AdvertCount          int             `json:"advert_count"`
	LastHeard            *string         `json:"last_heard"`
	HashSize             *int            `json:"hash_size"` // null until SetHashSize finds info
	HashSizeInconsistent bool            `json:"hash_size_inconsistent"`
	Foreign              bool            `json:"foreign"`
	BatteryMv            *int            `json:"battery_mv"`
	TemperatureC         *float64        `json:"temperature_c"`
	DefaultScope         *string         `json:"default_scope"`
	ConfiguredScope      *string         `json:"configured_scope"`
	ConfiguredScopeAt    *string         `json:"configured_scope_at"`
	HashSizesSeen        []int           `json:"hash_sizes_seen,omitempty"`
	FloodAdvertCount7d   *int            `json:"flood_advert_count_7d,omitempty"`

	// Optional groups: a nil pointer emits none of the group's keys.
	*NodeMultiByte     // any node with an entry
	*RepeaterRelayInfo // repeaters and rooms only
	*NodeUsefulness    // repeaters and rooms only
	*NodeDeclaredScope // repeaters and rooms only
}

// NodeListResponse is the /api/nodes page.
//
// HasMore reports whether rows exist past this page. It is computed from the
// raw SQL page before the post-LIMIT filters in handleNodes, which shorten the
// page and rewrite Total, so it is the only field a paginating client can
// trust to decide whether to ask for another page. It is always emitted (no
// omitempty): a client must be able to tell "false" from "old server".
type NodeListResponse struct {
	Nodes   []NodeResponse     `json:"nodes"`
	Total   int                `json:"total"`
	Counts  RoleCountsResponse `json:"counts"`
	HasMore bool               `json:"has_more"`
}

// NodeUsefulness holds the four #672 axes, the composite and the grade.
type NodeUsefulness struct {
	TrafficShareScore float64 `json:"traffic_share_score"`
	BridgeScore       float64 `json:"bridge_score"`
	CoverageScore     float64 `json:"coverage_score"`
	RedundancyScore   float64 `json:"redundancy_score"`
	UsefulnessScore   float64 `json:"usefulness_score"`
	UsefulnessGrade   string  `json:"usefulness_grade,omitempty"` // empty only on cold start
}

// NodeDeclaredScope is what enrichNodeDeclaredScope writes (#2001); relay roles only.
type NodeDeclaredScope struct {
	ScopeConfigState         string    `json:"scope_config_state"`
	DeclaredRegions          *[]string `json:"declared_regions,omitempty"`
	DeclaredRegionsTruncated bool      `json:"declared_regions_truncated,omitempty"` // only when true
}

// NodeMultiByte holds the multi-byte capability fields, present only for nodes with an entry.
type NodeMultiByte struct {
	Status      string `json:"multi_byte_status"`
	Evidence    string `json:"multi_byte_evidence"`
	MaxHashSize int    `json:"multi_byte_max_hash_size"`
}

func (n *NodeResponse) SetMultiByteCap(mb *database.MultiByteCap) {
	if mb == nil {
		return
	}
	evidence := ""
	if mb.Evidence != nil {
		evidence = string(*mb.Evidence)
	}
	n.NodeMultiByte = &NodeMultiByte{Status: mb.Status.String(), Evidence: evidence, MaxHashSize: mb.MaxHashSize}
}

// SetDeclaredScope replaces enrichNodeDeclaredScope for the typed response.
func (n *NodeResponse) SetDeclaredScope(answer declaredAnswer, hasDeclared bool, transportedScopes []string) {
	d := NodeDeclaredScope{
		ScopeConfigState: nodeScopeConfigState(answer.RegionsCSV, hasDeclared, transportedScopes),
	}
	if hasDeclared {
		named, _ := splitDeclaredRegions(answer.RegionsCSV)
		d.DeclaredRegions = declaredRegionsPtr(named)
		d.DeclaredRegionsTruncated = answer.Truncated
	}
	n.NodeDeclaredScope = &d
}

// SetMultiByte adds the capability fields from the store's entry. A nil entry leaves them absent.
func (n *NodeResponse) SetMultiByte(entry *MultiByteCapEntry) {
	if entry == nil {
		return
	}
	n.NodeMultiByte = &NodeMultiByte{
		Status:      entry.Status,
		Evidence:    entry.Evidence,
		MaxHashSize: entry.MaxHashSize,
	}
}

// SetHashSize adds hash_size, hash_size_inconsistent and hash_sizes_seen from precomputed info.
func (n *NodeResponse) SetHashSize(info *hashSizeNodeInfo) {
	if info == nil {
		return
	}
	hashSize := info.HashSize // copy; do not point into the store's data
	n.HashSize = &hashSize
	n.HashSizeInconsistent = info.Inconsistent
	if len(info.AllSizes) > 1 {
		sizes := make([]int, 0, len(info.AllSizes))
		for s := range info.AllSizes {
			sizes = append(sizes, s)
		}
		sort.Ints(sizes)
		n.HashSizesSeen = sizes
	}
}
