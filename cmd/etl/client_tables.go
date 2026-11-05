package main

import (
	"context"
	"fmt"
)

// clientTables are empty in production, so the ETL does not load them. If a
// snapshot has rows in one, the load stops: silently dropping client data would
// be worse than writing the loader.
var clientTables = []string{
	"client_receptions",
	"client_observers",
	"client_rx_observations",
	"node_declared_regions",
	"client_rf_samples",
}

func requireEmptyClientTables(ctx context.Context, src *Source) error {
	for _, table := range clientTables {
		var populated bool
		if err := src.db.QueryRowContext(ctx, fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s)", table)).Scan(&populated); err != nil {
			return fmt.Errorf("check source %s is empty: %w", table, err)
		}
		if populated {
			return fmt.Errorf("source table %s has rows; the ETL has no loader for it", table)
		}
	}
	return nil
}
