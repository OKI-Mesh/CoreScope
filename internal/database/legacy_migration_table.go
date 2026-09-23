package database

import (
	"database/sql"
	"fmt"
)

// EnsureLegacyMigrationsTable creates the _migrations table if it
// doesn't already exist. This table tracks the legacy async/one-shot
// migration gates (BackfillPathJSONAsync, tx_last_seen_backfill_v1,
// etc.) — it is NOT part of the goose-managed schema, since these are
// long-running background jobs goose has no concept of running.
// Previously created as an incidental side effect of applySchema; now
// explicit since applySchema is retired.
//
// Called from the ingestor's boot sequence, alongside (not inside)
// RunMigrations.
func EnsureLegacyMigrationsTable(rw *sql.DB) error {
	if _, err := rw.Exec(`CREATE TABLE IF NOT EXISTS _migrations (name TEXT PRIMARY KEY)`); err != nil {
		return fmt.Errorf("ensure _migrations table: %w", err)
	}
	return nil
}
