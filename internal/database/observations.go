// internal/database/observations.go
package database

import (
	"context"
	"errors"
	"fmt"
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
	ObserverID     string // observer pubkey; empty when observer_idx is NULL
	ObserverName   string
	ObserverIATA   string
	Direction      string
	SNR, RSSI      *float64
	Score          *float64
	Path           []PathHop // hop order
	Timestamp      time.Time
}

// HopPrefixes returns the hop prefixes in path order. It is never nil.
func (o Observation) HopPrefixes() []string {
	prefixes := make([]string, len(o.Path))
	for i, hop := range o.Path {
		prefixes[i] = hop.Prefix
	}
	return prefixes
}

func (db *DB) InsertObservationWithHops(ctx context.Context, obs ObservationInput, hops []string, resolved []*string) (obsID int64, inserted bool, err error) {
	hopPrefixes := make([]string, len(hops))
	for i, h := range hops {
		hopPrefixes[i] = normalizeKey(h)
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
		if i < len(resolved) && resolved[i] != nil {
			if key := normalizeKey(*resolved[i]); key != "" {
				resolvedPubkeys[i] = key
				anyResolved = true
			}
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

// ObserverReception is one observer's receipt of a transmission.
type ObserverReception struct {
	TransmissionID int64
	ObserverName   *string // nil when the observer is unknown or never sent a name
	SNR, RSSI      *float64
	ReceivedAt     time.Time
}

// ListObserverReceptions groups every observation of txIDs by transmission,
// oldest first, with the observer's name when it has one.
func (db *DB) ListObserverReceptions(ctx context.Context, txIDs []int64) (map[int64][]ObserverReception, error) {
	rows, err := db.q.ListObserverReceptions(ctx, txIDs)
	if err != nil {
		return nil, err
	}
	out := make(map[int64][]ObserverReception, len(txIDs))
	for _, r := range rows {
		txID := int64(r.TransmissionID)
		out[txID] = append(out[txID], ObserverReception{
			TransmissionID: txID,
			ObserverName:   fromPgText(r.ObserverName),
			SNR:            fromPgFloat8(r.Snr),
			RSSI:           fromPgFloat8(r.Rssi),
			ReceivedAt:     r.Timestamp.UTC(),
		})
	}
	return out, nil
}

// GetObservationsForHash lists every observation of a transmission, newest
// first. An unknown hash is an empty slice, not an error.
func (db *DB) GetObservationsForHash(ctx context.Context, hash string) ([]Observation, error) {
	id, err := db.q.GetTransmissionIDByHash(ctx, strings.ToLower(hash))
	if errors.Is(err, pgx.ErrNoRows) {
		return []Observation{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get transmission id for hash %q: %w", hash, err)
	}
	byTx, err := db.ListObservationsForTransmissions(ctx, []int64{id})
	if err != nil {
		return nil, err
	}
	obs := byTx[id]
	if obs == nil {
		obs = []Observation{}
	}
	return obs, nil
}

// TransmissionIDsWithResolvedPubkey returns the IDs from txIDs whose
// observations resolve a hop to pubkey. One query for the whole batch.
// An empty input returns nil without touching the database.
func (d *DB) TransmissionIDsWithResolvedPubkey(ctx context.Context, txIDs []int, pubkey string) ([]int, error) {
	if len(txIDs) == 0 {
		return nil, nil
	}
	ids := make([]int64, len(txIDs))
	for i, id := range txIDs {
		ids[i] = int64(id)
	}
	found, err := d.q.ListTransmissionIDsWithResolvedPubkey(ctx, pgdb.ListTransmissionIDsWithResolvedPubkeyParams{
		TransmissionIds: ids,
		Pubkey:          normalizeKey(pubkey),
	})
	if err != nil {
		return nil, fmt.Errorf("transmissions with resolved pubkey: %w", err)
	}
	out := make([]int, len(found))
	for i, id := range found {
		out[i] = int(id)
	}
	return out, nil
}

// BestResolvedPaths returns, per transmission, the resolved hop pubkeys of
// its best observation (see ListBestResolvedPaths). A nil element is a hop
// that was not resolved. Transmissions with no resolved hop are absent.
// One query for the whole batch.
func (d *DB) BestResolvedPaths(ctx context.Context, txIDs []int) (map[int][]*string, error) {
	out := make(map[int][]*string, len(txIDs))
	if len(txIDs) == 0 {
		return out, nil
	}
	ids := make([]int64, len(txIDs))
	for i, id := range txIDs {
		ids[i] = int64(id)
	}
	rows, err := d.q.ListBestResolvedPaths(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("best resolved paths: %w", err)
	}
	for _, r := range rows {
		rp := make([]*string, len(r.Resolved))
		for i := range r.Resolved {
			if r.Resolved[i] != "" {
				pk := r.Resolved[i]
				rp[i] = &pk
			}
		}
		out[int(r.TransmissionID)] = rp
	}
	return out, nil
}

// ResolvedPaths holds resolved hop pubkeys fetched ahead of building API
// responses. A nil element is a hop that was not resolved.
type ResolvedPaths struct {
	ByTx  map[int][]*string // transmission ID → its best observation's path
	ByObs map[int][]*string // observation ID → that observation's path
}

// ResolvedPaths fetches both views in two queries. Either ID list may be empty.
func (d *DB) ResolvedPaths(ctx context.Context, txIDs, obsIDs []int) (ResolvedPaths, error) {
	out := ResolvedPaths{ByTx: map[int][]*string{}, ByObs: map[int][]*string{}}

	if len(txIDs) > 0 {
		best, err := d.BestResolvedPaths(ctx, txIDs)
		if err != nil {
			return ResolvedPaths{}, err
		}
		out.ByTx = best
	}
	if len(obsIDs) > 0 {
		ids := make([]int64, len(obsIDs))
		for i, id := range obsIDs {
			ids[i] = int64(id)
		}
		rows, err := d.q.ListResolvedPathsForObservations(ctx, ids)
		if err != nil {
			return ResolvedPaths{}, fmt.Errorf("resolved paths for observations: %w", err)
		}
		for _, r := range rows {
			out.ByObs[int(r.ObservationID)] = resolvedFromText(r.Resolved)
		}
	}
	return out, nil
}

// resolvedFromText maps ” to nil so unresolved hops serialize as null.
func resolvedFromText(in []string) []*string {
	rp := make([]*string, len(in))
	for i := range in {
		if in[i] != "" {
			pk := in[i]
			rp[i] = &pk
		}
	}
	return rp
}

// ResolvedPubkeysForTransmissions returns, per transmission, the distinct
// resolved hop pubkeys across all of its observations. Transmissions that
// resolved nothing are absent. One query for the whole batch; the caller
// must not hold any store lock.
func (d *DB) ResolvedPubkeysForTransmissions(ctx context.Context, txIDs []int) (map[int][]string, error) {
	out := make(map[int][]string, len(txIDs))
	if len(txIDs) == 0 {
		return out, nil
	}
	ids := make([]int64, len(txIDs))
	for i, id := range txIDs {
		ids[i] = int64(id)
	}
	rows, err := d.q.ListResolvedPubkeysForTransmissions(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("resolved pubkeys for transmissions: %w", err)
	}
	for _, r := range rows {
		out[int(r.TransmissionID)] = r.Pubkeys
	}
	return out, nil
}
