// internal/database/observations.go
package database

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// internal/database/observations.go
type ObservationInput struct {
	TransmissionID int64
	ObserverIdx    *int64
	Direction      *string
	SNR            *float64
	RSSI           *float64
	Score          *float64
	PathHash       string
	Timestamp      time.Time
	RawHex         string
}

// Observation is one observer's received copy of a transmission.
type Observation struct {
	ID             int64
	TransmissionID int64
	ObserverPubkey *string
	ObserverName   *string
	ObserverIATA   string // "" when there is no observer
	Direction      *string
	SNR, RSSI      *float64
	Score          *float64
	Timestamp      time.Time
	HopPrefixes    []string // ordered by hop_index
	HopResolved    []string // same length; "" = unresolved
}

func (db *DB) InsertObservationWithHops(ctx context.Context, obs ObservationInput, hops []string, resolved []*string) (obsID int64, inserted bool, err error) {
	hopPrefixes := hops
	if hopPrefixes == nil {
		hopPrefixes = []string{}
	}
	resolvedPubkeys := make([]string, len(hopPrefixes))
	var frame []byte
	if obs.RawHex != "" {
		f, err := types.ParseRawFrame(obs.RawHex)
		if err != nil {
			return 0, false, fmt.Errorf("observation raw hex: %w", err)
		}
		frame = f
	}
	anyResolved := false
	for i := range hopPrefixes {
		if i < len(resolved) && resolved[i] != nil && *resolved[i] != "" {
			resolvedPubkeys[i] = *resolved[i]
			anyResolved = true
		}
	}
	if !anyResolved {
		resolvedPubkeys = []string{}
	}

	err = pgx.BeginFunc(ctx, db.pool, func(tx pgx.Tx) error {
		qtx := db.q.WithTx(tx)

		row, err := qtx.CreateOrUpdateObservation(ctx, pgdb.CreateOrUpdateObservationParams{
			TransmissionID:  obs.TransmissionID,
			ObserverIdx:     toPgInt8Ptr(obs.ObserverIdx),
			Direction:       toPgTextPtr(obs.Direction),
			Snr:             toPgFloat8Ptr(obs.SNR),
			Rssi:            toPgFloat8Ptr(obs.RSSI),
			Score:           toPgFloat8Ptr(obs.Score),
			PathHash:        toPgTextIfNotEmpty(obs.PathHash),
			Timestamp:       obs.Timestamp,
			HopPrefixes:     hopPrefixes,
			ResolvedPubkeys: resolvedPubkeys,
		})
		if err != nil {
			return fmt.Errorf("observation insert: %w", err)
		}
		obsID, inserted = row.ID, row.Inserted

		if frame != nil {
			if _, err := qtx.InsertObservationFrame(ctx, pgdb.InsertObservationFrameParams{
				ObservationID: row.ID,
				Frame:         frame,
			}); err != nil {
				return fmt.Errorf("observation frame insert: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return 0, false, err
	}
	return obsID, inserted, nil
}

// RecordNaiveSkew stamps an observer's clock-skew columns. The count resets
// to 1 when the previous event is older than 24h before now, otherwise it
// increments. deltaSec is signed: negative means the observer clock is
// behind UTC.
func (db *DB) RecordNaiveSkew(ctx context.Context, pubkey string, deltaSec int32, now time.Time) error {
	return db.q.RecordNaiveSkew(ctx, pgdb.RecordNaiveSkewParams{
		Pubkey:           pubkey,
		ClockSkewSeconds: pgtype.Int4{Int32: deltaSec, Valid: true},
		ClockLastNaiveAt: pgtype.Timestamptz{Time: now, Valid: true},
		Cutoff:           pgtype.Timestamptz{Time: now.Add(-24 * time.Hour), Valid: true},
	})
}

func (db *DB) GetMaxObservationID(ctx context.Context) (int64, error) {
	id, err := db.q.GetMaxObservationID(ctx)
	if err != nil {
		return 0, fmt.Errorf("get max observation id: %w", err)
	}
	return id, nil
}

// ObservationRawHexForHash returns the stored wire bytes per observation id
// for one transmission, keyed by observations.id. The map is empty when
// nothing is stored.
//
// Why this is read on demand instead of held in memory (#1999): the store
// deliberately does not retain obs.RawHex. #881 measured ~98MB wasted on a
// ~1.7M-observation store, because at the time the frames were believed to be
// identical per transmission ("same content hash implies same frame"). They
// are not: the firmware hashes payload and type independently of the relay
// path, so observations of one transmission legitimately carry different
// bytes. Keeping the memory saving and paying one query on the packet-detail
// path, which is a single packet a human is looking at, is the trade this
// makes.
func (db *DB) ObservationRawHexForHash(ctx context.Context, hash string) (map[int64]string, error) {
	out := make(map[int64]string)
	if hash == "" {
		return out, nil
	}
	rows, err := db.q.ListObservationRawHexByHash(ctx, strings.ToLower(hash))
	if err != nil {
		return nil, fmt.Errorf("observation raw hex for %s: %w", hash, err)
	}
	for _, r := range rows {
		out[r.ID] = types.RawFrame(r.Frame).Hex()
	}
	return out, nil
}

// ObservationsForTransmissions returns the observations of the given
// transmissions, newest first, grouped by transmission id. Transmissions with
// no observations are absent from the map.
func (db *DB) ObservationsForTransmissions(ctx context.Context, txIDs []int64) (map[int64][]Observation, error) {
	out := make(map[int64][]Observation)
	if len(txIDs) == 0 {
		return out, nil
	}
	rows, err := db.q.ListObservationsByTransmissionIDs(ctx, txIDs)
	if err != nil {
		return nil, fmt.Errorf("list observations: %w", err)
	}
	for _, r := range rows {
		out[r.TransmissionID] = append(out[r.TransmissionID], Observation{
			ID:             r.ID,
			TransmissionID: r.TransmissionID,
			ObserverPubkey: fromPgText(r.ObserverPubkey),
			ObserverName:   fromPgText(r.ObserverName),
			ObserverIATA:   r.ObserverIata,
			Direction:      fromPgText(r.Direction),
			SNR:            fromPgFloat8(r.Snr),
			RSSI:           fromPgFloat8(r.Rssi),
			Score:          fromPgFloat8(r.Score),
			Timestamp:      r.Timestamp.UTC(), // adjust to the generated type
			HopPrefixes:    r.HopPrefixes,
			HopResolved:    r.HopResolved,
		})
	}
	return out, nil
}

// ObservationsForHash returns every observation of the transmission with the given hash,
// oldest first. An unknown hash yields an empty slice, not an error.
func (d *DB) ObservationsForHash(ctx context.Context, hash string) ([]Observation, error) {
	id, err := d.q.GetTransmissionIDByHash(ctx, strings.ToLower(hash))
	if errors.Is(err, pgx.ErrNoRows) {
		return []Observation{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get transmission id for hash %q: %w", hash, err)
	}
	byTx, err := d.ObservationsForTransmissions(ctx, []int64{id})
	if err != nil {
		return nil, err
	}
	obs := byTx[id] // newest first
	slices.Reverse(obs)
	if obs == nil {
		obs = []Observation{}
	}
	return obs, nil
}
