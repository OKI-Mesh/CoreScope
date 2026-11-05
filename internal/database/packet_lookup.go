package database

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// GetPacketByHash returns one transmission with its representative
// observation. ErrNotFound when there is none. Case-insensitive on hash.
func (d *DB) GetPacketByHash(ctx context.Context, hash string) (*Packet, error) {
	row, err := d.q.GetPacketByHash(ctx, strings.ToLower(hash))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get packet by hash: %w", err)
	}
	return packetFromLookup(row), nil
}

// GetTransmissionByID is GetPacketByHash for a transmission id.
func (d *DB) GetTransmissionByID(ctx context.Context, id int) (*Packet, error) {
	row, err := d.q.GetPacketByID(ctx, int64(id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get packet by id: %w", err)
	}
	// Both queries select identical columns, so sqlc generates two
	// identically shaped row types and Go can convert between them.
	return packetFromLookup(pgdb.GetPacketByHashRow(row)), nil
}

// GetObservationsForHash lists every observation of a transmission, newest
// first. An unknown hash is an empty slice, not an error.
func (d *DB) GetObservationsForHash(ctx context.Context, hash string) ([]Observation, error) {
	rows, err := d.q.ListObservationsForHash(ctx, strings.ToLower(hash))
	if err != nil {
		return nil, fmt.Errorf("list observations for hash: %w", err)
	}
	out := make([]Observation, len(rows))
	for i, r := range rows {
		out[i] = Observation{
			ID:             r.ID,
			TransmissionID: r.TransmissionID,
			ObserverPubkey: fromPgText(r.ObserverPubkey),
			ObserverName:   fromPgText(r.ObserverName),
			ObserverIATA:   r.ObserverIata,
			Direction:      fromPgText(r.Direction),
			SNR:            fromPgFloat8(r.Snr),
			RSSI:           fromPgFloat8(r.Rssi),
			Score:          fromPgFloat8(r.Score),
			Timestamp:      r.Timestamp,
			HopPrefixes:    r.HopPrefixes,
			HopResolved:    r.HopResolved,
		}
	}
	return out, nil
}

func packetFromLookup(r pgdb.GetPacketByHashRow) *Packet {
	return &Packet{
		ID:               int64(r.ID),
		RawHex:           types.RawFrame(r.RawHex).Hex(),
		Hash:             r.Hash,
		FirstSeen:        r.FirstSeen,
		RouteType:        r.RouteType,
		PayloadType:      r.PayloadType,
		DecodedJSON:      textValue(r.DecodedJson),
		ScopeName:        fromPgText(r.ScopeName),
		ObservationCount: int(r.ObservationCount),
		SNR:              fromPgFloat8(r.Snr),
		RSSI:             fromPgFloat8(r.Rssi),
		Direction:        fromPgText(r.Direction),
		ObserverPubkey:   fromPgText(r.ObserverID),
		ObserverName:     fromPgText(r.ObserverName),
		HopPrefixes:      r.HopPrefixes,
	}
}
