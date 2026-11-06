package main

import (
	"sort"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// NodeUsefulness holds the four #672 axes, the composite and the grade.
type NodeUsefulness struct {
	TrafficShareScore float64 `json:"traffic_share_score"`
	BridgeScore       float64 `json:"bridge_score"`
	CoverageScore     float64 `json:"coverage_score"`
	RedundancyScore   float64 `json:"redundancy_score"`
	UsefulnessScore   float64 `json:"usefulness_score"`
	UsefulnessGrade   string  `json:"usefulness_grade,omitempty"` // empty only on cold start
}

func applyMultiByteCap(n *api.Node, mb *database.MultiByteCap) {
	if mb == nil {
		return
	}
	n.MultiByteStatus = ptr(mb.Status.String())
	n.MultiByteMaxHashSize = ptr(mb.MaxHashSize)
	if mb.Evidence != nil {
		n.MultiByteEvidence = ptr(string(*mb.Evidence))
	}
}

func enrichNodeWithDeclaredScope(n *api.Node, answer declaredAnswer, hasDeclared bool, transportedScopes []string) {
	n.ScopeConfigState = ptr(api.ScopeConfigState(nodeScopeConfigState(answer.RegionsCSV, hasDeclared, transportedScopes)))
	if !hasDeclared {
		return
	}
	named, _ := splitDeclaredRegions(answer.RegionsCSV)
	n.DeclaredRegions = declaredRegionsPtr(named)
	if answer.Truncated {
		n.DeclaredRegionsTruncated = ptr(true)
	}
}

func enrichNodeWithMultiByte(n *api.Node, entry *MultiByteCapEntry) {
	if entry == nil {
		return
	}
	n.MultiByteStatus = ptr(entry.Status)
	n.MultiByteMaxHashSize = ptr(entry.MaxHashSize)
	if entry.Evidence != "" {
		n.MultiByteEvidence = ptr(entry.Evidence)
	}
}

// enrichNodeWithHashSize adds hash_size, hash_size_inconsistent and hash_sizes_seen from precomputed info.
func enrichNodeWithHashSize(n *api.Node, info *hashSizeNodeInfo) {
	if info == nil {
		return
	}
	n.HashSize = ptr(info.HashSize) // ptr copies the value; nothing points into the store's data
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

func enrichNodeWithRelayInfo(node *api.Node, info *RepeaterRelayInfo) {
	if info == nil {
		return
	}
	if !info.LastRelayed.IsZero() {
		node.LastRelayed = &types.WireTimeSeconds{Time: info.LastRelayed}
	}
	node.RelayActive = ptr(info.RelayActive)
	node.RelayWindowHours = ptr(info.WindowHours)
	node.RelayCount1h = ptr(info.RelayCount1h)
	node.RelayCount24h = ptr(info.RelayCount24h)
	node.UnscopedRelayCount24h = ptr(info.UnscopedRelayCount24h)
	node.TransportedScopes = info.TransportedScopes
}

// enrichNodeUsefulness writes the four #672 axes + composite + grade onto a
// node map (shared by the node-list and node-detail handlers). trafficRaw is
// the Traffic-axis share exposed verbatim as traffic_share_score; ax holds the
// composite inputs — its Traffic is the max-normalized share (across the
// repeater population) so all four axes contribute on a comparable [0,1]
// scale. axesComputed reports whether the structural-axis recomputers have
// produced a snapshot yet; it only matters for the all-zero node, where it
// distinguishes cold start (grade withheld) from a genuinely isolated repeater
// (real "F") — see compositeUsefulness. The usefulness_grade field is omitted
// only when the grade is empty (cold start).
func enrichNodeWithUsefulness(node *api.Node, u *NodeUsefulness) {
	if u == nil {
		return
	}
	node.TrafficShareScore = ptr(u.TrafficShareScore)
	node.BridgeScore = ptr(u.BridgeScore)
	node.CoverageScore = ptr(u.CoverageScore)
	node.RedundancyScore = ptr(u.RedundancyScore)
	node.UsefulnessScore = ptr(u.UsefulnessScore)
	if u.UsefulnessGrade != "" {
		node.UsefulnessGrade = ptr(api.UsefulnessGrade(u.UsefulnessGrade))
	}
}
