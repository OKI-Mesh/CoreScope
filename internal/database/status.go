package database

import (
	"database/sql"
	"fmt"
)

// MigrationState describes the actual, unambiguous state of a
// database's schema relative to the goose migration chain.
type MigrationState int

const (
	// StateFresh: no core tables exist at all. Safe to run RunMigrations
	// — the full chain will apply cleanly from scratch.
	StateFresh MigrationState = iota

	// StateMigrated: goose_db_version reports a version > 0. Either
	// fully or partially migrated via goose; RunMigrations will apply
	// whatever's still pending, if anything.
	StateMigrated

	// StateUnstamped: core tables (nodes, transmissions) exist, but
	// goose_db_version reports 0. Real schema present, never touched by
	// goose — the one-time auto-detection/stamping recovery path
	// applies. See DetectAndStampSchema.
	StateUnstamped
)

func (s MigrationState) String() string {
	switch s {
	case StateFresh:
		return "fresh"
	case StateMigrated:
		return "migrated"
	case StateUnstamped:
		return "unstamped"
	default:
		return "unknown"
	}
}

// CurrentSchemaVersion returns the highest applied goose migration
// version for rw. Returns 0 for a database with no migrations applied
// yet (including a genuinely fresh database with no goose_db_version
// table at all).
func CurrentSchemaVersion(rw *sql.DB) (int64, error) {
	var version sql.NullInt64
	err := rw.QueryRow(`SELECT MAX(version_id) FROM goose_db_version WHERE is_applied = 1`).Scan(&version)
	if err != nil {
		// No goose_db_version table at all is expected on a fresh DB —
		// treat any query error here as "version 0", not fatal.
		return 0, nil
	}
	if !version.Valid {
		return 0, nil
	}
	return version.Int64, nil
}

// SchemaVersionApplied reports whether the given goose migration
// version has been applied to rw. A missing goose_db_version table
// (genuinely fresh/unmigrated database) is treated as "not applied",
// not an error — consistent with CurrentSchemaVersion's handling of
// the same case.
func SchemaVersionApplied(rw *sql.DB, version int64) (bool, error) {
	var applied int
	err := rw.QueryRow(`SELECT COUNT(*) FROM goose_db_version WHERE version_id = ? AND is_applied = 1`, version).Scan(&applied)
	if err != nil {
		return false, nil
	}
	return applied == 1, nil
}

// MigrationStatus reports rw's current MigrationState and, when
// StateMigrated, the current schema version (0 for StateFresh/
// StateUnstamped).
func MigrationStatus(rw *sql.DB) (MigrationState, int64, error) {
	version, err := CurrentSchemaVersion(rw)
	if err != nil {
		return 0, 0, fmt.Errorf("checking schema version: %w", err)
	}
	if version > 0 {
		return StateMigrated, version, nil
	}

	hasCoreTables, err := tablesExist(rw, "nodes", "transmissions")
	if err != nil {
		return 0, 0, fmt.Errorf("checking for existing schema: %w", err)
	}
	if hasCoreTables {
		return StateUnstamped, 0, nil
	}

	return StateFresh, 0, nil
}

func tablesExist(rw *sql.DB, names ...string) (bool, error) {
	for _, name := range names {
		var n string
		err := rw.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, name).Scan(&n)
		if err == sql.ErrNoRows {
			return false, nil
		}
		if err != nil {
			return false, err
		}
	}
	return true, nil
}
