package database

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
)

// ScopeMatchTotals is the persisted scope-match tally: one row, cumulative
// since Since.
type ScopeMatchTotals struct {
	Since               time.Time
	Unique              int64
	ExplicitOverDerived int64
	Ambiguous           int64
	None                int64
}

// GetScopeMatchTotals returns the stored row, or (nil, nil) when there is none
// yet.
func (db *DB) GetScopeMatchTotals(ctx context.Context) (*ScopeMatchTotals, error) {
	r, err := db.q.GetScopeMatchTotals(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &ScopeMatchTotals{
		Since:               r.CountingSince.UTC(),
		Unique:              r.UniqueMatches,
		ExplicitOverDerived: r.ExplicitOverDerived,
		Ambiguous:           r.Ambiguous,
		None:                r.NoneMatches,
	}, nil
}

// SaveScopeMatchTotals upserts the single row. now is the updated_at stamp.
func (db *DB) SaveScopeMatchTotals(ctx context.Context, t ScopeMatchTotals, now time.Time) error {
	return db.q.UpsertScopeMatchTotals(ctx, pgdb.UpsertScopeMatchTotalsParams{
		CountingSince:       t.Since,
		UniqueMatches:       t.Unique,
		ExplicitOverDerived: t.ExplicitOverDerived,
		Ambiguous:           t.Ambiguous,
		NoneMatches:         t.None,
		UpdatedAt:           now,
	})
}
