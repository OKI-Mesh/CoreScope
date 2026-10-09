// internal/database/observations.go
package database

import (
	"context"
	"fmt"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
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

func (db *DB) InsertObservationWithHops(ctx context.Context, obs ObservationInput, hops []string, resolved []*string) error {
	return pgx.BeginFunc(ctx, db.pool, func(tx pgx.Tx) error {
		qtx := db.q.WithTx(tx)

		obsID, err := qtx.CreateOrUpdateObservation(ctx, pgdb.CreateOrUpdateObservationParams{
			TransmissionID: obs.TransmissionID,
			ObserverIdx:    toPgInt8Ptr(obs.ObserverIdx),
			Direction:      toPgTextPtr(obs.Direction),
			Snr:            toPgFloat8Ptr(obs.SNR),
			Rssi:           toPgFloat8Ptr(obs.RSSI),
			Score:          toPgFloat8Ptr(obs.Score), // was toPgInt4Ptr — Score is float64, not int
			PathHash:       toPgTextIfNotEmpty(obs.PathHash),
			Timestamp:      obs.Timestamp,
			RawHex:         toPgTextIfNotEmpty(obs.RawHex),
		})
		if err != nil {
			return fmt.Errorf("observation insert: %w", err)
		}

		for i, hop := range hops {
			resolvedPK := ""
			if i < len(resolved) && resolved[i] != nil {
				resolvedPK = *resolved[i]
			}
			if err := qtx.InsertObservationPathHop(ctx, pgdb.InsertObservationPathHopParams{
				ObservationID:  obsID,
				HopIndex:       int32(i),
				Prefix:         hop,
				ResolvedPubkey: toPgTextIfNotEmpty(resolvedPK),
			}); err != nil {
				return fmt.Errorf("path hop insert (hop %d): %w", i, err)
			}
		}
		return nil
	})
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
