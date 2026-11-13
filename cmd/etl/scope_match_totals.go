package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// loadScopeMatchTotals copies the single counters row. since_unix and
// updated_unix become the timestamptz columns counting_since and updated_at.
func loadScopeMatchTotals(ctx context.Context, src *Source, tx pgx.Tx, report *Report) error {
	rows, err := src.db.QueryContext(ctx, `SELECT id, since_unix, unique_matches, explicit_over_derived,
		ambiguous, none_matches, updated_unix FROM scope_match_totals`)
	if err != nil {
		return fmt.Errorf("read scope_match_totals: %w", err)
	}
	defer rows.Close()

	var out [][]any
	for rows.Next() {
		var id, since, unique, explicit, ambiguous, none, updated int64
		if err := rows.Scan(&id, &since, &unique, &explicit, &ambiguous, &none, &updated); err != nil {
			return fmt.Errorf("scan scope_match_totals: %w", err)
		}
		out = append(out, []any{id, unixTimestamp(since), unique, explicit, ambiguous, none, unixTimestamp(updated)})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read scope_match_totals: %w", err)
	}
	report.Add("scope_match_totals: loaded", int64(len(out)))
	columns := []string{"id", "counting_since", "unique_matches", "explicit_over_derived", "ambiguous", "none_matches", "updated_at"}
	return copyRows(ctx, tx, "scope_match_totals", columns, out)
}
