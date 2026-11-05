package database

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// ObservedPath is one observation that passed through any of the given prefixes,
// with its full path (every hop prefix, in order).
type ObservedPath struct {
	ObserverPK  string
	FromPubkey  string
	PayloadType types.PayloadType
	Path        []string
	SNR         *float64
}

// ListReachRows returns observations since the given time that have a hop matching
// any of the tokens, capped at limit rows.
func (db *DB) ListObservedPathsThrough(ctx context.Context, tokens []string, since time.Time, limit int) ([]ObservedPath, error) {
	rows, err := db.q.ListObservedPathsThrough(ctx, pgdb.ListObservedPathsThroughParams{
		Since: since, Tokens: tokens, RowLimit: int32(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]ObservedPath, len(rows))
	for i, r := range rows {
		out[i] = ObservedPath{
			ObserverPK: r.ObserverPk, FromPubkey: r.FromPubkey, PayloadType: r.PayloadType,
			Path: r.Path, SNR: fromPgFloat8(r.Snr),
		}
	}
	return out, nil
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
		Pubkey:          strings.ToLower(pubkey),
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
