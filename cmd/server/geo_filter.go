package main

import "github.com/OKI-Mesh/CoreScope/internal/geofilter"

// NodePassesGeoFilter returns true if the node should be included in responses.
// Nodes with no GPS coordinates are always allowed.
// lat and lon are interface{} because they come from DB row maps.
func NodePassesGeoFilter(lat, lon *float64, gf *GeoFilterConfig) bool {
	if gf == nil {
		return true
	}
	if lat == nil || lon == nil {
		return true
	}
	return geofilter.PassesFilter(*lat, *lon, gf)
}
