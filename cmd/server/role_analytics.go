package main

import (
	"context"
	"math"
	"net/http"
	"sort"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// computeRoleAnalytics groups nodes by role and aggregates clock-skew per
// role. Pure function: takes the node roster and the per-pubkey skew map and
// returns the response — no store / lock dependencies, easy to unit test.
//
// `nodesByPubkey` lists every known node (pubkey → role). `skewByPubkey`
// is the subset of pubkeys that have clock-skew data with their severity and
// most-recent corrected skew (in seconds, signed — we take |x| for averages).
func computeRoleAnalytics(nodesByPubkey map[string]types.NodeRole, skewByPubkey map[string]*NodeClockSkew) api.RoleAnalyticsResponse {
	type bucket struct {
		stats    api.RoleStats
		absSkews []float64
	}
	buckets := make(map[types.NodeRole]*bucket)
	for pk, role := range nodesByPubkey {
		if role == "" {
			role = types.RoleUnknown
		}
		b, ok := buckets[role]
		if !ok {
			b = &bucket{stats: api.RoleStats{Role: role}}
			buckets[role] = b
		}
		b.stats.NodeCount++
		cs, has := skewByPubkey[pk]
		if !has || cs == nil {
			continue
		}
		b.stats.WithSkew++
		abs := math.Abs(cs.RecentMedianSkewSec)
		if abs == 0 {
			abs = math.Abs(cs.LastSkewSec)
		}
		b.absSkews = append(b.absSkews, abs)
		switch cs.Severity {
		case types.SkewOK:
			b.stats.OkCount++
		case types.SkewWarning:
			b.stats.WarningCount++
		case types.SkewCritical:
			b.stats.CriticalCount++
		case types.SkewAbsurd:
			b.stats.AbsurdCount++
		case types.SkewNoClock:
			b.stats.NoClockCount++
		}
	}
	resp := api.RoleAnalyticsResponse{Roles: make([]api.RoleStats, 0, len(buckets))}
	for _, b := range buckets {
		if n := len(b.absSkews); n > 0 {
			sum := 0.0
			for _, v := range b.absSkews {
				sum += v
			}
			b.stats.MeanAbsSkewSec = round(sum/float64(n), 2)
			sorted := make([]float64, n)
			copy(sorted, b.absSkews)
			sort.Float64s(sorted)
			if n%2 == 1 {
				b.stats.MedianAbsSkewSec = round(sorted[n/2], 2)
			} else {
				b.stats.MedianAbsSkewSec = round((sorted[n/2-1]+sorted[n/2])/2, 2)
			}
		}
		resp.TotalNodes += b.stats.NodeCount
		resp.Roles = append(resp.Roles, b.stats)
	}
	// Sort: largest population first, then role name for stable output.
	sort.Slice(resp.Roles, func(i, j int) bool {
		if resp.Roles[i].NodeCount != resp.Roles[j].NodeCount {
			return resp.Roles[i].NodeCount > resp.Roles[j].NodeCount
		}
		return resp.Roles[i].Role < resp.Roles[j].Role
	})
	return resp
}

// handleAnalyticsRoles serves /api/analytics/roles. Reads from the
// steady-state recomputer snapshot (issue #1256) so the request never
// holds s.mu.RLock for a full clock-skew recompute over the advert
// transmissions — that path hung >60s on staging with 78k tx.
func (s *Server) handleAnalyticsRoles(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if s.store == nil {
		writeJSON(w, api.RoleAnalyticsResponse{Roles: []api.RoleStats{}})
		return
	}
	writeJSON(w, s.store.GetAnalyticsRoles(ctx))
}

// GetAnalyticsRoles returns the role-distribution analytics, preferring
// the steady-state recomputer snapshot (issue #1256). Falls back to an
// on-request compute path if the recomputer is not yet running (e.g.
// during the brief startup window before the initial compute completes
// — Start runs it synchronously, so this fallback is effectively only
// hit in tests that skip the recomputer entirely).
func (s *PacketStore) GetAnalyticsRoles(ctx context.Context) api.RoleAnalyticsResponse {
	s.analyticsRecomputerMu.RLock()
	rc := s.recompRoles
	s.analyticsRecomputerMu.RUnlock()
	if rc != nil {
		if v := rc.Load(); v != nil {
			if r, ok := v.(api.RoleAnalyticsResponse); ok {
				s.cacheMu.Lock()
				s.cacheHits++
				s.cacheMu.Unlock()
				return r
			}
		}
	}
	return s.computeAnalyticsRoles(ctx)
}

// computeAnalyticsRoles runs the actual role aggregation. Used by the
// background recomputer (issue #1256) and as a fallback for callers
// arriving before the snapshot is populated.
func (s *PacketStore) computeAnalyticsRoles(ctx context.Context) api.RoleAnalyticsResponse {
	nodes, _ := s.getCachedNodesAndPM()
	roles := make(map[string]types.NodeRole, len(nodes))
	for _, n := range nodes {
		roles[n.PublicKey] = n.Role
	}
	skewMap := make(map[string]*NodeClockSkew)
	for _, cs := range s.GetFleetClockSkew(ctx, "") {
		if cs == nil {
			continue
		}
		skewMap[cs.Pubkey] = cs
	}
	return computeRoleAnalytics(roles, skewMap)
}
