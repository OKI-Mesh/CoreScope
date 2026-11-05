package database

import (
	"context"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/jackc/pgx/v5/pgtype"
)

type ChunkQuery struct {
	AfterID int64
	Since   *time.Time // nil = no lower bound
	Until   *time.Time // nil = no upper bound; exclusive
	Limit   int
}

// LoadedTransmission is a transmission as the in-memory store loads it.
type LoadedTransmission struct {
	ID          int64
	Hash        string
	RawHex      string
	FirstSeen   time.Time
	RouteType   *types.RouteType   // nil = unknown (stored as -1)
	PayloadType *types.PayloadType // nil = unknown (stored as -1)
	DecodedJSON string
	ScopeName   *string // nil = unscoped, "" = scoped but unmatched
}

// PathHop is one hop of an observation's path.
type PathHop struct {
	Prefix         string  // hop prefix as transmitted
	ResolvedPubkey *string // nil if unresolved or ambiguous
}

// LoadedObservation is one observation of a loaded transmission.
type LoadedObservation struct {
	ID           int64
	ObserverID   string // observer pubkey; empty when observer_idx is NULL
	ObserverName string
	ObserverIATA string
	Direction    string
	SNR, RSSI    *float64
	Score        *float64
	Path         []PathHop // hop order
	Timestamp    time.Time
}

func sinceParam(t *time.Time) pgtype.Timestamptz {
	if t == nil {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: *t, Valid: true}
}

func routeTypeOrNil(v types.RouteType) *types.RouteType {
	if v < 0 {
		return nil
	}
	return &v
}

func payloadTypeOrNil(v types.PayloadType) *types.PayloadType {
	if v < 0 {
		return nil
	}
	return &v
}

func (db *DB) ListTransmissionsChunk(ctx context.Context, q ChunkQuery) ([]LoadedTransmission, error) {
	rows, err := db.q.ListTransmissionsChunk(ctx, pgdb.ListTransmissionsChunkParams{
		AfterID:  q.AfterID,
		Since:    sinceParam(q.Since),
		Until:    sinceParam(q.Until),
		RowLimit: int32(q.Limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]LoadedTransmission, 0, len(rows))
	for _, r := range rows {
		out = append(out, LoadedTransmission{
			ID:          int64(r.ID),
			Hash:        r.Hash,
			RawHex:      types.RawFrame(r.RawHex).Hex(),
			FirstSeen:   r.FirstSeen.UTC(),
			RouteType:   routeTypeOrNil(r.RouteType),
			PayloadType: payloadTypeOrNil(r.PayloadType),
			DecodedJSON: textValue(r.DecodedJson),
			ScopeName:   fromPgText(r.ScopeName),
		})
	}
	return out, nil
}

func (db *DB) CountTransmissionsSince(ctx context.Context, since *time.Time) (int64, error) {
	return db.q.CountTransmissionsSince(ctx, sinceParam(since))
}

// ListObservationsForTransmissions groups the observations of txIDs by
// transmission id, newest first within each, with their path hops attached.
func (db *DB) ListObservationsForTransmissions(ctx context.Context, txIDs []int64) (map[int64][]LoadedObservation, error) {
	rows, err := db.q.ListObservationsByTransmissionIDs(ctx, txIDs)
	if err != nil {
		return nil, err
	}

	out := make(map[int64][]LoadedObservation, len(txIDs))
	for _, r := range rows {
		var path []PathHop
		if len(r.HopPrefixes) > 0 {
			path = make([]PathHop, len(r.HopPrefixes))
			for i, prefix := range r.HopPrefixes {
				hop := PathHop{Prefix: prefix}
				if i < len(r.HopResolved) && r.HopResolved[i] != "" {
					pk := r.HopResolved[i]
					hop.ResolvedPubkey = &pk
				}
				path[i] = hop
			}
		}
		txID := int64(r.TransmissionID)
		out[txID] = append(out[txID], LoadedObservation{
			ID:           int64(r.ID),
			ObserverID:   textValue(r.ObserverPubkey),
			ObserverName: textValue(r.ObserverName),
			ObserverIATA: r.ObserverIata,
			Direction:    textValue(r.Direction),
			SNR:          fromPgFloat8(r.Snr),
			RSSI:         fromPgFloat8(r.Rssi),
			Score:        fromPgFloat8(r.Score),
			Path:         path,
			Timestamp:    r.Timestamp.UTC(),
		})
	}
	return out, nil
}

// textValue is the empty string for NULL.
func textValue(v pgtype.Text) string {
	if v.Valid {
		return v.String
	}
	return ""
}

// NewObservation is a LoadedObservation plus the transmission it belongs to.
type NewObservation struct {
	TransmissionID int64
	LoadedObservation
}

// ListObservationsAfterID returns up to limit observations with id > afterID, oldest first.
func (db *DB) ListObservationsAfterID(ctx context.Context, afterID int64, limit int) ([]NewObservation, error) {
	rows, err := db.q.ListObservationsAfterID(ctx, pgdb.ListObservationsAfterIDParams{
		AfterID:  afterID,
		RowLimit: int32(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]NewObservation, len(rows))
	for i, r := range rows {
		out[i] = NewObservation{
			TransmissionID: r.TransmissionID,
			LoadedObservation: LoadedObservation{
				ID:           r.ID,
				ObserverID:   r.ObserverID,
				ObserverName: r.ObserverName,
				ObserverIATA: r.ObserverIata,
				Direction:    r.Direction,
				SNR:          fromPgFloat8(r.Snr),
				RSSI:         fromPgFloat8(r.Rssi),
				Score:        fromPgFloat8(r.Score),
				Path:         pathHops(r.HopPrefixes, r.HopResolved),
				Timestamp:    r.Timestamp.UTC(),
			},
		}
	}
	return out, nil
}
