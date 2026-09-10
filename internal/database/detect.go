package database

import (
	"database/sql"
	"fmt"
)

// migrationColumnMarkers maps each structural migration to the marker(s)
// that indicate it has been applied. Migrations touching multiple
// tables (e.g. node_telemetry_v1 alters both nodes AND inactive_nodes)
// list multiple entries with the SAME version — ALL must be present for
// that version to be considered detected, preventing false-positive
// stamps like a database having nodes.battery_mv but not
// inactive_nodes.battery_mv (which would mean the migration only
// partially applied, or inactive_nodes didn't exist yet at the time).
var migrationColumnMarkers = []struct {
	version int64
	table   string
	column  string
	index   string
}{
	{2, "observations", "", "idx_observations_dedup"},
	{6, "nodes", "battery_mv", ""},
	{6, "inactive_nodes", "battery_mv", ""},
	{8, "observer_metrics", "", ""},
	{10, "observers", "inactive", ""},
	{11, "observer_metrics", "packets_sent", ""},
	{12, "transmissions", "channel_hash", ""},
	{13, "dropped_packets", "", ""},
	{14, "observers", "last_packet_at", ""},
	{16, "nodes", "foreign_advert", ""},
	{16, "inactive_nodes", "foreign_advert", ""},
	{17, "transmissions", "from_pubkey", ""},
	{19, "transmissions", "scope_name", ""},
	{20, "nodes", "default_scope", ""},
	{20, "inactive_nodes", "default_scope", ""},
	{21, "observations", "raw_hex", ""},
	{22, "observers", "clock_skew_seconds", ""},
	{24, "neighbor_edges", "", ""},
	{25, "observations", "resolved_path", ""},
	{26, "observers", "iata", ""},
	{27, "nodes", "multibyte_sup", ""},
	{27, "inactive_nodes", "multibyte_sup", ""},
	{28, "observers", "can_relay", ""},
	{29, "transmissions", "last_seen", ""},
}

// DetectAndStampSchema inspects rw's actual table/column structure and
// stamps goose_db_version for every migration whose structural marker
// is present, WITHOUT executing any migration SQL. Migrations 0 and 1
// are stamped unconditionally if any marker matched (nothing else could
// exist without the base schema). Data-only migrations are intentionally
// left unstamped — they're cheap and idempotent, so RunMigrationsAllowingGaps
// just runs them regardless of whether they ran once under the old system.
//
// Real historical data can have gaps (e.g. a database with observers.iata
// present but observers.clock_skew_seconds absent) — this function stamps
// exactly what it finds, leaving gaps unstamped so a subsequent
// RunMigrationsAllowingGaps call fills them in out of order.
//
// Only call this after MigrationStatus reports StateUnstamped. Do not
// call it from tests simulating deliberately old-shaped fixtures —
// those should call RunMigrations directly and let the full chain
// replay from scratch.
func DetectAndStampSchema(rw *sql.DB) error {
	// Group markers by version so multi-table migrations require ALL
	// their markers present, not just one.
	byVersion := make(map[int64][]struct {
		table  string
		column string
		index  string
	})
	for _, m := range migrationColumnMarkers {
		byVersion[m.version] = append(byVersion[m.version], struct {
			table  string
			column string
			index  string
		}{m.table, m.column, m.index})
	}

	var matchedVersions []int64
	for version, markers := range byVersion {
		allPresent := true
		for _, mk := range markers {
			present, err := markerPresent(rw, mk.table, mk.column, mk.index)
			if err != nil {
				return fmt.Errorf("probing marker for v%d (%s.%s): %w", version, mk.table, mk.column, err)
			}
			if !present {
				allPresent = false
				break
			}
		}
		if allPresent {
			matchedVersions = append(matchedVersions, version)
		}
	}

	if len(matchedVersions) == 0 {
		// Nothing detected — do NOT create goose_db_version at all. Let
		// goose's own Up() initialize it fresh and correctly when
		// RunMigrations runs next (an empty-but-existing table confuses
		// goose differently than a genuinely absent one).
		return nil
	}

	if err := ensureGooseVersionTable(rw); err != nil {
		return fmt.Errorf("creating goose_db_version: %w", err)
	}

	for _, version := range matchedVersions {
		if _, err := rw.Exec(
			`INSERT OR IGNORE INTO goose_db_version (version_id, is_applied) VALUES (?, 1)`,
			version,
		); err != nil {
			return fmt.Errorf("stamping v%d: %w", version, err)
		}
	}

	if _, err := rw.Exec(`INSERT OR IGNORE INTO goose_db_version (version_id, is_applied) VALUES (0, 1)`); err != nil {
		return fmt.Errorf("stamping baseline version 0: %w", err)
	}
	if _, err := rw.Exec(`INSERT OR IGNORE INTO goose_db_version (version_id, is_applied) VALUES (1, 1)`); err != nil {
		return fmt.Errorf("stamping base schema version 1: %w", err)
	}

	return nil
}

func markerPresent(rw *sql.DB, table, column, index string) (bool, error) {
	if index != "" {
		var name string
		err := rw.QueryRow(`SELECT name FROM sqlite_master WHERE type='index' AND name=?`, index).Scan(&name)
		if err == sql.ErrNoRows {
			return false, nil
		}
		return err == nil, err
	}
	if column == "" {
		return tablesExist(rw, table)
	}
	return TableHasColumn(rw, table, column)
}

// TableHasColumn reports whether the given table has the given column.
// Exported because tests and the read-side need it without re-implementing.
func TableHasColumn(db *sql.DB, table, column string) (bool, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name string
		var ctype sql.NullString
		var notnull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

// ensureGooseVersionTable creates the goose_db_version table if it
// doesn't already exist, matching the schema goose itself creates.
// Needed because DetectAndStampSchema may run against a database that
// has real application schema but has never been touched by goose at
// all — the table won't exist yet.
func ensureGooseVersionTable(rw *sql.DB) error {
	_, err := rw.Exec(`CREATE TABLE IF NOT EXISTS goose_db_version (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		version_id INTEGER NOT NULL,
		is_applied INTEGER NOT NULL,
		tstamp TIMESTAMP DEFAULT (datetime('now'))
	)`)
	return err
}
