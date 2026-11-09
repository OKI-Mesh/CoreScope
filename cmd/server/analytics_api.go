package main

import (
	"encoding/json"
	"net/http"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/gorilla/mux"
)

// registerAnalyticsRoutes registers the analytics endpoints whose responses
// are typed against the spec. The other analytics endpoints are still
// registered in RegisterRoutes and move here as they are converted.
func (s *Server) registerAnalyticsRoutes(r *mux.Router) {
	r.HandleFunc("/api/analytics/roles", s.handleAnalyticsRoles).Methods("GET")
	r.HandleFunc("/api/analytics/hash-sizes", s.handleAnalyticsHashSizes).Methods("GET")
	r.HandleFunc("/api/analytics/hash-collisions", s.handleAnalyticsHashCollisions).Methods("GET")
	r.HandleFunc("/api/analytics/rf", s.handleAnalyticsRF).Methods("GET")
	r.HandleFunc("/api/analytics/distance", s.handleAnalyticsDistance).Methods("GET")
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
