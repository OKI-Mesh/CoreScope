// internal/database/observers.go
package database

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/jackc/pgx/v5/pgtype"
)

type ObserverInput struct {
	ID        string
	Name      string
	IATA      string
	Meta      *types.ObserverMeta
	LastSeen  time.Time // envelope/receive time
	IngestNow time.Time // wall clock at ingest; caps how far last_seen may advance
}

type ObserverMetadataInput struct {
	ID   string
	Name string
	IATA string
	Meta *types.ObserverMeta
}

// ResolveObserverAndTouch looks up the observer's internal id by its
// real-world identifier (pubkey/station id) and, if found, touches its
// last_seen/last_packet_at to ingestNow. Returns nil if the observer is
// unknown or empty — callers treat that as "no observer resolved" and
// proceed without failing the surrounding insert.
//
// observer.last_seen and last_packet_at answer "when did the analyzer
// last hear from this observer" — both are ingest-time questions.
// Per-packet rxTime is stored separately on observations/transmissions
// using envelope time. See #1465.
func (db *DB) ResolveObserverAndTouch(ctx context.Context, observerID string, ingestNow time.Time) *int64 {
	if observerID == "" {
		return nil
	}
	id, err := db.q.GetObserverID(ctx, observerID)
	if err != nil {
		return nil
	}
	if err := db.q.TouchObserverLastSeen(ctx, pgdb.TouchObserverLastSeenParams{
		LastSeen: pgtype.Timestamptz{Time: ingestNow, Valid: true},
		ID:       id,
	}); err != nil {
		log.Printf("[db] touch observer last_seen (non-fatal): %v", err)
	}
	return &id
}

func (db *DB) CreateOrUpdateObserver(ctx context.Context, o ObserverInput) error {
	model, firmware, clientVersion, radio, batteryMv, uptimeSecs, noiseFloor, canRelay := metaToParams(o.Meta)
	o.ID = strings.ToLower(strings.TrimSpace(o.ID))
	if o.ID == "" {
		return errors.New("observer ID is empty")
	}

	return db.q.CreateOrUpdateObserver(ctx, pgdb.CreateOrUpdateObserverParams{
		Pubkey:        o.ID,
		Name:          toPgTextIfNotEmpty(o.Name),
		Iata:          toPgTextIfNotEmpty(o.IATA),
		LastSeen:      o.LastSeen,
		IngestNow:     o.IngestNow,
		Model:         model,
		Firmware:      firmware,
		ClientVersion: clientVersion,
		Radio:         radio,
		BatteryMv:     batteryMv,
		UptimeSecs:    uptimeSecs,
		NoiseFloor:    noiseFloor,
		CanRelay:      canRelay,
	})
}

// ReactivateObserver clears the inactive flag on an observer that was
// previously soft-deleted (e.g. by RemoveStaleObservers). A no-op if the
// observer is already active or does not exist.
func (db *DB) ReactivateObserver(ctx context.Context, pubkey string) error {
	return db.q.ReactivateObserver(ctx, pubkey)
}

// UpdateObserverMetadataIfRetained applies metadata from a RETAINED status
// message to an existing observer row. It never inserts, never advances
// last_seen, and never clears inactive: a retained message is not a sign
// of life.
func (db *DB) UpdateObserverMetadataIfRetained(ctx context.Context, in ObserverMetadataInput) error {
	model, firmware, clientVersion, radio, batteryMv, uptimeSecs, noiseFloor, canRelay := metaToParams(in.Meta)

	return db.q.UpdateObserverMetadataIfRetained(ctx, pgdb.UpdateObserverMetadataIfRetainedParams{
		Pubkey:        in.ID,
		Name:          toPgTextIfNotEmpty(in.Name),
		Iata:          toPgTextIfNotEmpty(in.IATA),
		Model:         model,
		Firmware:      firmware,
		ClientVersion: clientVersion,
		Radio:         radio,
		BatteryMv:     batteryMv,
		UptimeSecs:    uptimeSecs,
		NoiseFloor:    noiseFloor,
		CanRelay:      canRelay,
	})
}

func (db *DB) PurgeStaleObservers(ctx context.Context, cutoff time.Time) (int64, error) {
	return db.q.PurgeStaleObservers(ctx, cutoff)
}

func (db *DB) MarkStaleObserversInactive(ctx context.Context, cutoff time.Time) (int64, error) {
	return db.q.MarkStaleObserversInactive(ctx, cutoff)
}

func (db *DB) DeleteMetricsForInactiveObservers(ctx context.Context) (int64, error) {
	return db.q.DeleteMetricsForInactiveObservers(ctx)
}

func (db *DB) SoftDeleteBlacklistedObservers(ctx context.Context, blacklist []string) (int64, error) {
	keys := make([]string, 0, len(blacklist))
	for _, pk := range blacklist {
		if t := strings.ToLower(strings.TrimSpace(pk)); t != "" {
			keys = append(keys, t)
		}
	}
	if len(keys) == 0 {
		return 0, nil
	}
	return db.q.SoftDeleteBlacklistedObservers(ctx, keys)
}
