package types

import "fmt"

type RouteType int32

const (
	RouteTransportFlood  RouteType = 0
	RouteFlood           RouteType = 1
	RouteDirect          RouteType = 2
	RouteTransportDirect RouteType = 3
	RouteUnknown         RouteType = -1 // the DB sentinel for legacy NULLs
)

func (r RouteType) String() string {
	switch r {
	case RouteTransportFlood:
		return "TRANSPORT_FLOOD"
	case RouteFlood:
		return "FLOOD"
	case RouteDirect:
		return "DIRECT"
	case RouteTransportDirect:
		return "TRANSPORT_DIRECT"
	case RouteUnknown:
		return "UNKNOWN"
	default:
		return fmt.Sprintf("RouteType(%d)", int32(r))
	}
}

// IsTransport reports whether the route carries two transport codes after the
// header byte: TRANSPORT_FLOOD (0) and TRANSPORT_DIRECT (3).
func (r RouteType) IsTransport() bool {
	return r == RouteTransportFlood || r == RouteTransportDirect
}

// IsFlood reports whether the route is flooded. Each forwarder appends its own
// hash to the END of the path, so path[last] is the node that transmitted.
func (r RouteType) IsFlood() bool {
	return r == RouteTransportFlood || r == RouteFlood
}

// IsDirect reports whether the route is direct. Forwarders consume the next
// hop from the FRONT of the path, so path[last] is the far end of the route,
// not who transmitted.
func (r RouteType) IsDirect() bool {
	return r == RouteDirect || r == RouteTransportDirect
}
