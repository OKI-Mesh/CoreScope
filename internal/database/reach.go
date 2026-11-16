package database

import (
	"context"
	"fmt"
	"math"
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

// ListObservedPathsThrough returns observations since the given time that have
// a hop matching any of the tokens, capped at limit rows, newest first. Tokens
// are matched case-insensitively. An empty token list matches nothing.
func (db *DB) ListObservedPathsThrough(ctx context.Context, tokens []string, since time.Time, limit int) ([]ObservedPath, error) {
	if limit < 0 || limit > math.MaxInt32 {
		return nil, fmt.Errorf("observed paths limit %d out of range", limit)
	}
	tokens = normalizeKeys(tokens)
	if len(tokens) == 0 {
		return []ObservedPath{}, nil
	}
	rows, err := db.q.ListObservedPathsThrough(ctx, pgdb.ListObservedPathsThroughParams{
		Since: since, Tokens: tokens, RowLimit: int32(limit),
	})
	if err != nil {
		return nil, fmt.Errorf("list observed paths: %w", err)
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
