package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// loadedTables are every table the ETL writes. The target must hold no rows in
// any of them, so a failed load restarts from scratch and a second run cannot
// double-load.
var loadedTables = []string{
	"nodes",
	"observers",
	"transmissions",
	"observations",
	"observation_frames",
	"observer_metrics",
	"dropped_packets",
	"neighbor_edges",
	"client_receptions",
	"client_observers",
	"client_rx_observations",
	"node_declared_regions",
	"client_rf_samples",
	"scope_match_totals",
}

func requireEmptyTarget(ctx context.Context, tx pgx.Tx) error {
	for _, table := range loadedTables {
		var populated bool
		query := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s)", pgx.Identifier{table}.Sanitize())
		if err := tx.QueryRow(ctx, query).Scan(&populated); err != nil {
			return fmt.Errorf("check %s is empty: %w", table, err)
		}
		if populated {
			return fmt.Errorf("target table %s already has rows; the ETL only loads an empty database", table)
		}
	}
	return nil
}

func copyRows(ctx context.Context, tx pgx.Tx, table string, columns []string, rows [][]any) error {
	copied, err := tx.CopyFrom(ctx, pgx.Identifier{table}, columns, pgx.CopyFromRows(rows))
	if err != nil {
		return fmt.Errorf("copy into %s: %w", table, err)
	}
	if copied != int64(len(rows)) {
		return fmt.Errorf("copy into %s: wrote %d of %d rows", table, copied, len(rows))
	}
	return nil
}

// resetSequence moves a table's id sequence past the ids the ETL inserted
// explicitly, so the next row the application inserts does not collide.
func resetSequence(ctx context.Context, tx pgx.Tx, table string) error {
	quoted := pgx.Identifier{table}.Sanitize()
	query := fmt.Sprintf(
		"SELECT setval(pg_get_serial_sequence($1, 'id'), COALESCE((SELECT max(id) FROM %s), 1), EXISTS (SELECT 1 FROM %s))",
		quoted, quoted)
	if _, err := tx.Exec(ctx, query, table); err != nil {
		return fmt.Errorf("reset %s id sequence: %w", table, err)
	}
	return nil
}

// copyStream loads rows produced one at a time, so a table too large for
// memory never has to be held whole. next returns a nil row and a nil error
// when the rows are exhausted.
func copyStream(ctx context.Context, tx pgx.Tx, table string, columns []string, next func() ([]any, error)) (int64, error) {
	copied, err := tx.CopyFrom(ctx, pgx.Identifier{table}, columns, pgx.CopyFromFunc(next))
	if err != nil {
		return 0, fmt.Errorf("copy into %s: %w", table, err)
	}
	return copied, nil
}
