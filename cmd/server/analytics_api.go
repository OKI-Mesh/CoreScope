package main

import (
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
