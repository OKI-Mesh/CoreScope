package database

import (
	"context"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/jackc/pgx/v5/pgtype"
)

// ChunkQuery pages through transmissions in id order. AfterID is exclusive.
// Since is inclusive and Until is exclusive, both compared against a
// transmission's last activity (last_seen, else first_seen); a nil bound is
// open. Limit must not be negative; zero returns nothing.
type ChunkQuery struct {
	AfterID int64
	Since   *time.Time
	Until   *time.Time
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
func (db *DB) ListObservationsForTransmissions(ctx context.Context, txIDs []int64) (map[int64][]Observation, error) {
	rows, err := db.q.ListObservationsByTransmissionIDs(ctx, txIDs)
	if err != nil {
		return nil, err
	}

	out := make(map[int64][]Observation, len(txIDs))
	for _, r := range rows {
		out[r.TransmissionID] = append(out[r.TransmissionID], Observation{
			ID:             r.ID,
			TransmissionID: r.TransmissionID,
			ObserverID:     r.ObserverID,
			ObserverName:   r.ObserverName,
			ObserverIATA:   r.ObserverIata,
			Direction:      r.Direction,
			SNR:            fromPgFloat8(r.Snr),
			RSSI:           fromPgFloat8(r.Rssi),
			Score:          fromPgFloat8(r.Score),
			Path:           pathHops(r.HopPrefixes, r.HopResolved),
			Timestamp:      r.Timestamp.UTC(),
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

// ListObservationsAfterID returns up to limit observations with id > afterID, oldest first.
func (db *DB) ListObservationsAfterID(ctx context.Context, afterID int64, limit int) ([]Observation, error) {
	rows, err := db.q.ListObservationsAfterID(ctx, pgdb.ListObservationsAfterIDParams{
		AfterID:  afterID,
		RowLimit: int32(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]Observation, len(rows))
	for i, r := range rows {
		out[i] = Observation{
			ID:             r.ID,
			TransmissionID: r.TransmissionID,
			ObserverID:     r.ObserverID,
			ObserverName:   r.ObserverName,
			ObserverIATA:   r.ObserverIata,
			Direction:      r.Direction,
			SNR:            fromPgFloat8(r.Snr),
			RSSI:           fromPgFloat8(r.Rssi),
			Score:          fromPgFloat8(r.Score),
			Path:           pathHops(r.HopPrefixes, r.HopResolved),
			Timestamp:      r.Timestamp.UTC(),
		}
	}
	return out, nil
}

// pathHops pairs the two parallel hop arrays; "" in resolved means unresolved.
func pathHops(prefixes, resolved []string) []PathHop {
	if len(prefixes) == 0 {
		return nil
	}
	hops := make([]PathHop, len(prefixes))
	for i, p := range prefixes {
		hops[i] = PathHop{Prefix: p}
		if i < len(resolved) && resolved[i] != "" {
			r := resolved[i]
			hops[i].ResolvedPubkey = &r
		}
	}
	return hops
}
