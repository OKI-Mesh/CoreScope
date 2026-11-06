package packetpath

import (
	"strings"

	"github.com/OKI-Mesh/CoreScope/internal/types"
)

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

func LowerHops(hops []string) []string {
	out := make([]string, len(hops))
	for i, h := range hops {
		out[i] = strings.ToLower(h)
	}
	return out
}
