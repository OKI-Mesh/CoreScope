package packetpath

import "github.com/OKI-Mesh/CoreScope/internal/types"

// IsTransportRoute returns true for TRANSPORT_FLOOD (0) and TRANSPORT_DIRECT (3).
func IsTransportRoute(routeType types.RouteType) bool {
	return routeType == types.RouteTransportFlood || routeType == types.RouteTransportDirect
}

// PathBytesAreHops returns true when the raw_hex header path bytes represent
// route hop hashes (the normal case). Returns false for packet types where
// header path bytes are repurposed (e.g. TRACE uses them for SNR values).
func PathBytesAreHops(payloadType types.PayloadType) bool {
	return payloadType != types.PayloadTRACE
}
