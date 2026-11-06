package main

import (
	"encoding/json"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// Transmission represents a row from the transmissions table.
type TransmissionResponse struct {
	ID             int     `json:"id"`
	RawHex         *string `json:"raw_hex"`
	Hash           string  `json:"hash"`
	FirstSeen      string  `json:"first_seen"`
	RouteType      *int    `json:"route_type"`
	PayloadType    *int    `json:"payload_type"`
	PayloadVersion *int    `json:"payload_version"`
	DecodedJSON    *string `json:"decoded_json"`
	CreatedAt      *string `json:"created_at"`
}

// PacketResult is one page of a packet list. T is PacketResponse for the flat
// list and GroupedPacketResponse for groupByHash=true.
type PacketResult[T any] struct {
	Packets []T `json:"packets"`
	Total   int `json:"total"`
	Limit   int `json:"limit"`
	Offset  int `json:"offset"`
}

// PacketResponse is one packet in the flat list, the multi-node list and the
// node-health recent packets. Pointer fields are null on the wire when unset.
type PacketResponse struct {
	ID               int                   `json:"id"`
	RawHex           *string               `json:"raw_hex"`
	Hash             *string               `json:"hash"`
	FirstSeen        *string               `json:"first_seen"`
	Timestamp        *string               `json:"timestamp"`
	RouteType        *types.RouteType      `json:"route_type"`
	PayloadType      *types.PayloadType    `json:"payload_type"`
	DecodedJSON      *string               `json:"decoded_json"`
	ObservationCount int                   `json:"observation_count"`
	ObserverID       *string               `json:"observer_id"`
	ObserverName     *string               `json:"observer_name"`
	ObserverIATA     *string               `json:"observer_iata"`
	SNR              *float64              `json:"snr"`
	RSSI             *float64              `json:"rssi"`
	Score            *float64              `json:"score,omitempty"`
	PathJSON         *string               `json:"path_json"`
	Direction        *string               `json:"direction"`
	ScopeName        *string               `json:"scope_name"`
	ParsedPath       []string              `json:"_parsedPath"`
	ResolvedPath     []*string             `json:"resolved_path,omitzero"`
	Observations     []ObservationResponse `json:"observations,omitzero"`
}

// GroupedPacketResponse is one row of the groupByHash=true list.
type GroupedPacketResponse struct {
	Hash             *string            `json:"hash"`
	FirstSeen        *string            `json:"first_seen"`
	Count            int                `json:"count"`
	ObserverCount    int                `json:"observer_count"`
	ObservationCount int                `json:"observation_count"`
	Latest           *string            `json:"latest"`
	ObserverID       *string            `json:"observer_id"`
	ObserverName     *string            `json:"observer_name"`
	ObserverIATA     *string            `json:"observer_iata"`
	DistinctIatas    []string           `json:"distinct_iatas"`
	PathJSON         *string            `json:"path_json"`
	PayloadType      *types.PayloadType `json:"payload_type"`
	RouteType        *types.RouteType   `json:"route_type"`
	RawHex           *string            `json:"raw_hex"`
	DecodedJSON      *string            `json:"decoded_json"`
	SNR              *float64           `json:"snr"`
	RSSI             *float64           `json:"rssi"`
	ScopeName        *string            `json:"scope_name"`
}

// optString is nil for "", so empty values are null on the wire.
func optString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// optTime is the fixed-format timestamp string, or nil for the zero time.
func optTime(t time.Time) *string {
	if t.IsZero() {
		return nil
	}
	s := formatTS(t)
	return &s
}

// prefixesJSON is the hop prefixes as a JSON array; "[]" when there are none.
func prefixesJSON(prefixes []string) string {
	if len(prefixes) == 0 {
		return "[]"
	}
	b, err := json.Marshal(prefixes)
	if err != nil {
		return "[]"
	}
	return string(b)
}

// txResponse is the in-memory packet as the API returns it.
func txResponse(tx *StoreTx, includeObservations bool) PacketResponse {
	r := PacketResponse{
		ID:               tx.ID,
		RawHex:           optString(tx.RawHex),
		Hash:             optString(tx.Hash),
		FirstSeen:        optTime(tx.FirstSeen),
		Timestamp:        optTime(tx.FirstSeen),
		RouteType:        tx.RouteType,
		PayloadType:      tx.PayloadType,
		DecodedJSON:      optString(tx.DecodedJSON),
		ObservationCount: tx.ObservationCount,
		ObserverID:       optString(tx.ObserverID),
		ObserverName:     optString(tx.ObserverName),
		ObserverIATA:     optString(tx.ObserverIATA),
		SNR:              tx.SNR,
		RSSI:             tx.RSSI,
		PathJSON:         optString(tx.PathJSON),
		Direction:        optString(tx.Direction),
		ScopeName:        tx.ScopeName,
	}
	if hops := txGetParsedPath(tx); len(hops) > 0 {
		r.ParsedPath = hops
	}
	// Only build observations when the caller asks (avoids allocations that get stripped).
	if includeObservations {
		r.Observations = make([]ObservationResponse, 0, len(tx.Observations))
		for _, o := range tx.Observations {
			r.Observations = append(r.Observations, ObservationResponse{
				ID:           o.ID,
				ObserverID:   optString(o.ObserverID),
				ObserverName: optString(o.ObserverName),
				ObserverIATA: optString(o.ObserverIATA),
				SNR:          o.SNR,
				RSSI:         o.RSSI,
				PathJSON:     optString(o.PathJSON),
				Timestamp:    optTime(o.Timestamp),
				Direction:    optString(o.Direction),
			})
		}
	}
	return r
}

// txResponseWithRP is txResponse plus the resolved paths the caller fetched
// with DB.ResolvedPaths. It never queries.
func txResponseWithRP(tx *StoreTx, paths database.ResolvedPaths, includeObservations bool) PacketResponse {
	r := txResponse(tx, includeObservations)
	r.ResolvedPath = paths.ByTx[tx.ID]
	if includeObservations {
		for i, o := range tx.Observations {
			r.Observations[i].ResolvedPath = paths.ByObs[o.ID]
		}
	}
	return r
}

// groupedPacketResponse is one grouped-list row. #1189 R2: DistinctIatas is
// the distinct IATA set across all observations, used by the frontend's
// collapsed view to show cross-region reception (groupedObserverIataBadgesHtml
// in public/packets.js). resolved_path is omitted (cold path).
func groupedPacketResponse(tx *StoreTx) GroupedPacketResponse {
	return GroupedPacketResponse{
		Hash:             optString(tx.Hash),
		FirstSeen:        optTime(tx.FirstSeen),
		Count:            tx.ObservationCount,
		ObserverCount:    tx.UniqueObserverCount,
		ObservationCount: tx.ObservationCount,
		Latest:           optTime(tx.LatestSeen),
		ObserverID:       optString(tx.ObserverID),
		ObserverName:     optString(tx.ObserverName),
		ObserverIATA:     optString(tx.ObserverIATA),
		DistinctIatas:    storeTxDistinctIatas(tx),
		PathJSON:         optString(tx.PathJSON),
		PayloadType:      tx.PayloadType,
		RouteType:        tx.RouteType,
		RawHex:           optString(tx.RawHex),
		DecodedJSON:      optString(tx.DecodedJSON),
		SNR:              tx.SNR,
		RSSI:             tx.RSSI,
		ScopeName:        tx.ScopeName,
	}
}

// packetResponse is a database-served packet as the API returns it. Fields the
// database path does not carry (observer_iata, _parsedPath) are null.
func packetResponse(p database.Packet) PacketResponse {
	r := PacketResponse{
		ID:               int(p.ID),
		RawHex:           optString(p.RawHex),
		Hash:             optString(p.Hash),
		FirstSeen:        optTime(p.FirstSeen),
		Timestamp:        optTime(p.FirstSeen), // the representative observation's time wins below
		RouteType:        &p.RouteType,
		PayloadType:      &p.PayloadType,
		DecodedJSON:      optString(p.DecodedJSON),
		ObservationCount: p.ObservationCount,
		ObserverID:       p.ObserverPubkey,
		ObserverName:     p.ObserverName,
		SNR:              p.SNR,
		RSSI:             p.RSSI,
		Score:            p.Score,
		Direction:        p.Direction,
		ScopeName:        p.ScopeName,
	}
	if p.ObsTimestamp != nil {
		r.Timestamp = optTime(*p.ObsTimestamp)
	}
	// path_json is rebuilt from the hop prefixes of the representative
	// observation: "[]" when it has none, null when there is no observation.
	if p.ObserverPubkey != nil {
		pj := prefixesJSON(p.HopPrefixes)
		r.PathJSON = &pj
	}
	return r
}
