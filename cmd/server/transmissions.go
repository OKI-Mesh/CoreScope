package main

import (
	"encoding/json"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// LivePacket is the packet as the WebSocket live feed sends it. It is not part
// of the HTTP contract: its timestamps are milliseconds, unlike the whole-second
// packet times /api/packets serves. Pointer fields are null on the wire when unset.
type LivePacket struct {
	ID               int                `json:"id"`
	RawHex           *string            `json:"raw_hex"`
	Hash             *string            `json:"hash"`
	FirstSeen        *string            `json:"first_seen"`
	Timestamp        *string            `json:"timestamp"`
	RouteType        *types.RouteType   `json:"route_type"`
	PayloadType      *types.PayloadType `json:"payload_type"`
	DecodedJSON      *string            `json:"decoded_json"`
	ObservationCount int                `json:"observation_count"`
	ObserverID       *string            `json:"observer_id"`
	ObserverName     *string            `json:"observer_name"`
	ObserverIATA     *string            `json:"observer_iata"`
	SNR              *float64           `json:"snr"`
	RSSI             *float64           `json:"rssi"`
	Score            *float64           `json:"score,omitempty"`
	PathJSON         *string            `json:"path_json"`
	Direction        *string            `json:"direction"`
	ScopeName        *string            `json:"scope_name"`
	ParsedPath       []string           `json:"_parsedPath"`
	ResolvedPath     []*string          `json:"resolved_path,omitzero"`
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

// txCore is the packet fields of an in-memory transmission that every packet
// response carries. Its timestamp is first_seen; the resolved path is added by
// the caller.
func txCore(tx *StoreTx) api.PacketCore {
	firstSeen := optionalWireSeconds(&tx.FirstSeen)
	timestamp := optionalWireSeconds(&tx.FirstSeen)
	return api.PacketCore{
		Id:               tx.ID,
		RawHex:           optString(tx.RawHex),
		Hash:             optString(tx.Hash),
		FirstSeen:        firstSeen,
		Timestamp:        timestamp,
		RouteType:        tx.RouteType,
		PayloadType:      tx.PayloadType,
		DecodedJson:      optString(tx.DecodedJSON),
		ObservationCount: tx.ObservationCount,
		ObserverId:       optString(tx.ObserverID),
		ObserverName:     optString(tx.ObserverName),
		ObserverIata:     optString(tx.ObserverIATA),
		Snr:              tx.SNR,
		Rssi:             tx.RSSI,
		PathJson:         optString(tx.PathJSON),
		Direction:        optString(tx.Direction),
		ScopeName:        tx.ScopeName,
	}
}

// packetFromCore adds what a full packet carries on top of the core fields.
// parsedPath is null on the wire when empty. observations is nil unless the
// caller asked for them; a non-nil empty slice is sent as [].
func packetFromCore(c api.PacketCore, parsedPath []string, observations []api.PacketObservation) api.Packet {
	p := api.Packet{
		Id:               c.Id,
		RawHex:           c.RawHex,
		Hash:             c.Hash,
		FirstSeen:        c.FirstSeen,
		Timestamp:        c.Timestamp,
		RouteType:        c.RouteType,
		PayloadType:      c.PayloadType,
		DecodedJson:      c.DecodedJson,
		ObservationCount: c.ObservationCount,
		ObserverId:       c.ObserverId,
		ObserverName:     c.ObserverName,
		ObserverIata:     c.ObserverIata,
		Snr:              c.Snr,
		Rssi:             c.Rssi,
		Score:            c.Score,
		PathJson:         c.PathJson,
		Direction:        c.Direction,
		ScopeName:        c.ScopeName,
		ResolvedPath:     c.ResolvedPath,
	}
	if len(parsedPath) > 0 {
		p.UnderscoreParsedPath = &parsedPath
	}
	if observations != nil {
		p.Observations = &observations
	}
	return p
}

// txPacket is the in-memory packet as the API returns it, with the resolved
// paths the caller fetched with DB.ResolvedPaths. It never queries.
func txPacket(tx *StoreTx, paths database.ResolvedPaths, includeObservations bool) api.Packet {
	c := txCore(tx)
	if rp := paths.ByTx[tx.ID]; rp != nil {
		c.ResolvedPath = &rp
	}
	// Only build observations when the caller asks (avoids allocations that get stripped).
	var observations []api.PacketObservation
	if includeObservations {
		observations = make([]api.PacketObservation, 0, len(tx.Observations))
		for _, o := range tx.Observations {
			observations = append(observations, packetObservation(o, paths.ByObs[o.ID]))
		}
	}
	return packetFromCore(c, txGetParsedPath(tx), observations)
}

// dbPacket is a database-served packet as the API returns it. Fields the
// database path does not carry (observer_iata, _parsedPath) are null.
func dbPacket(p database.Packet) api.Packet {
	return packetFromCore(packetCore(p), nil, nil)
}

// groupedPacket is one grouped-list row. #1189 R2: DistinctIatas is the
// distinct IATA set across all observations, used by the frontend's collapsed
// view to show cross-region reception (groupedObserverIataBadgesHtml in
// public/packets.js). resolved_path is omitted (cold path).
func groupedPacket(tx *StoreTx) api.GroupedPacket {
	return api.GroupedPacket{
		Hash:             optString(tx.Hash),
		FirstSeen:        optionalWireSeconds(&tx.FirstSeen),
		Count:            tx.ObservationCount,
		ObserverCount:    tx.UniqueObserverCount,
		ObservationCount: tx.ObservationCount,
		Latest:           optionalWireSeconds(&tx.LatestSeen),
		ObserverId:       optString(tx.ObserverID),
		ObserverName:     optString(tx.ObserverName),
		ObserverIata:     optString(tx.ObserverIATA),
		DistinctIatas:    storeTxDistinctIatas(tx),
		PathJson:         optString(tx.PathJSON),
		PayloadType:      tx.PayloadType,
		RouteType:        tx.RouteType,
		RawHex:           optString(tx.RawHex),
		DecodedJson:      optString(tx.DecodedJSON),
		Snr:              tx.SNR,
		Rssi:             tx.RSSI,
		ScopeName:        tx.ScopeName,
	}
}
