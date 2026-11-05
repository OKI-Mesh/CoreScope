package main

import (
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
)

// scopeStatsWindow maps the ?window= value to its name, start and time-series bucket.
// Unknown or empty values mean 24h.
func scopeStatsWindow(window string, now time.Time) (string, time.Time, time.Duration) {
	switch window {
	case "1h":
		return "1h", now.Add(-time.Hour), 5 * time.Minute
	case "7d":
		return "7d", now.Add(-7 * 24 * time.Hour), 6 * time.Hour
	default:
		return "24h", now.Add(-24 * time.Hour), time.Hour
	}
}

func scopeStatsToResponse(window string, st *database.ScopeStats) *ScopeStatsResponse {
	resp := &ScopeStatsResponse{Window: window}
	resp.Summary.TransportTotal = int(st.Summary.TransportTotal)
	resp.Summary.Scoped = int(st.Summary.Scoped)
	resp.Summary.Unscoped = int(st.Summary.Unscoped)
	resp.Summary.UnknownScope = int(st.Summary.UnknownScope)

	resp.ByRegion = make([]ScopeRegionCount, 0, len(st.ByRegion))
	for _, r := range st.ByRegion {
		resp.ByRegion = append(resp.ByRegion, ScopeRegionCount{Name: r.Name, Count: int(r.Count)})
	}
	resp.TimeSeries = make([]ScopeTimePoint, 0, len(st.TimeSeries))
	for _, p := range st.TimeSeries {
		resp.TimeSeries = append(resp.TimeSeries, ScopeTimePoint{T: formatTS(p.T), Scoped: int(p.Scoped), Unscoped: int(p.Unscoped)})
	}
	resp.AdvertsByRole = make([]ScopeAdvertRoleCount, 0, len(st.AdvertsByRole))
	for _, r := range st.AdvertsByRole {
		resp.AdvertsByRole = append(resp.AdvertsByRole, ScopeAdvertRoleCount{
			Role: r.Role, Unscoped: int(r.Unscoped), UnknownScope: int(r.UnknownScope), Named: int(r.Named),
		})
	}
	return resp
}
