package types

import "time"

// ClientReception is one mobile RX coverage point: a companion (RxPubkey)
// directly heard a node (HeardKey) at a GPS position. Hex binning is done
// server-side from Lat/Lon at query time, so no cell id is stored here.
type ClientReception struct {
	RxPubkey    string
	HeardKey    string
	HeardKeyLen int
	RSSI        *int
	SNR         *float64
	Lat         float64
	Lon         float64
	PosAccM     *float64
	RxAt        time.Time
	IngestedAt  time.Time
	Src         string
}

// ClientRfSample is one RF environment sample from a mobile client: the
// attached LoRa radio's own counters (noise floor, RX/TX airtime, CRC errors,
// packet totals), paired with the GPS point where the sample was taken. Every
// counter is an absolute cumulative value the radio reported; Task 5 derives
// deltas at query time. RecvErrors stays nil on firmware that cannot count
// CRC errors — never storing 0, which would read as a perfectly clean channel.
// Errors is the exception: per the firmware stats frame, it is an error-flags
// bitmask, not a counter, so it is deliberately excluded from ClientRfDeltas.
type ClientRfSample struct {
	RxPubkey                                                                           string
	SampledAt                                                                          time.Time
	IngestedAt                                                                         time.Time
	Lat, Lon                                                                           float64
	PosAccM                                                                            *float64
	Stationary                                                                         bool
	UptimeSecs                                                                         int64
	BatteryMV, QueueLen, Errors                                                        *int
	NoiseFloor, LastRSSI                                                               *int
	LastSNR                                                                            *float64
	TxAirSecs, RxAirSecs, Recv, Sent, FloodRx, DirectRx, FloodTx, DirectTx, RecvErrors *int64
}

// ClientRxObservation is one decodable packet a mobile client heard, whether or
// not it was attributable to a directly-heard node. Diagnostic only: this table
// is never a coverage source (see client_receptions for that).
type ClientRxObservation struct {
	RxPubkey    string
	RxAt        time.Time
	IngestedAt  time.Time
	PktHash     string
	RouteType   RouteType
	PayloadType PayloadType
	HashSize    int32
	HopCount    int32
	Code1       *string
	Code2       *string
	ScopeName   *string
	PathJSON    *string
	Forwarder   *string
	SNR         *float64
	RSSI        *int
	Lat         float64
	Lon         float64
	PosAccM     *float64
}

// ClientDeclaredRegions is one repeater's answer to a flood-allowed region
// list request (ANON_REQ_TYPE_REGIONS), relayed by a mobile companion. Every
// answer is a row — this table stores observations, never state. An empty
// RegionsCSV is a valid, deliberate answer ("nothing flood-allowed"); a
// repeater that did not answer produces no row at all, never a row with an
// empty list standing in for silence. Truncated is a hint, not a detection:
// the firmware's exportNamesTo skips names that do not fit and continues, so
// an overflowing reply has holes rather than a truncated prefix, with no
// marker distinguishing the two.
type ClientDeclaredRegions struct {
	Target        string
	RxPubkey      string
	ObservedAt    time.Time
	IngestedAt    time.Time
	RegionsCSV    string
	Truncated     bool
	Lat, Lon      *float64
	PosAccM       *float64
	RepeaterClock *int64
}

type ClientObserver struct {
	Pubkey   string
	Name     string
	LastSeen time.Time
}
