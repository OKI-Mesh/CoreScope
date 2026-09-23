package database

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/pressly/goose/v3"
)

// GooseAdoptionVersion is the first goose migration version created
// after CoreScope switched from raw-SQL/legacy migrations to goose. A
// database missing any version at or below this threshold was never
// (or was only partially) brought under goose's control by
// `migrate -baseline-and-stamp`, and may be in an unknown mixed state —
// neither the server nor the ingestor should attempt to reason about
// or migrate it directly.
const GooseAdoptionVersion = 31

// checkFunc reports whether a piece of expected schema (a table, an
// index, a column, a view) is present in db.
type checkFunc func(db *sql.DB) (bool, error)

// hasTable checks that a table exists and has (at least) the given
// columns with matching declared types. Extra columns beyond what's
// listed are ignored, since later migrations may have added more —
// this only verifies what THIS migration is expected to have added.
func hasTable(table string, cols map[string]string) checkFunc {
	return func(db *sql.DB) (bool, error) {
		exists, err := TableExists(db, table)
		if err != nil || !exists {
			return false, err
		}
		got, err := TableColumns(db, table)
		if err != nil {
			return false, err
		}
		for col, wantType := range cols {
			gotType, ok := got[col]
			if !ok || gotType != wantType {
				return false, nil
			}
		}
		return true, nil
	}
}

// hasColumn checks a single column exists with the given type on an
// already-existing table. Use this for ALTER TABLE ... ADD COLUMN
// migrations instead of re-listing the whole table via hasTable.
func hasColumn(table, col, wantType string) checkFunc {
	return func(db *sql.DB) (bool, error) {
		got, err := TableColumns(db, table)
		if err != nil {
			return false, err
		}
		gotType, ok := got[col]
		return ok && gotType == wantType, nil
	}
}

// hasIndex checks that a named index exists. Only use this for
// explicitly CREATE INDEX-named indexes — not SQLite's auto-generated
// indexes backing UNIQUE constraints (sqlite_autoindex_*), whose naming
// is an implementation detail, not something a migration declared.
func hasIndex(name string) checkFunc {
	return func(db *sql.DB) (bool, error) { return IndexExists(db, name) }
}

// hasView checks that a named view exists.
func hasView(name string) checkFunc {
	return func(db *sql.DB) (bool, error) { return ViewExists(db, name) }
}

// all combines checks with AND — use this when a single migration
// touched multiple tables, indexes, or views.
func all(checks ...checkFunc) checkFunc {
	return func(db *sql.DB) (bool, error) {
		for _, c := range checks {
			ok, err := c(db)
			if err != nil || !ok {
				return ok, err
			}
		}
		return true, nil
	}
}

type schemaCheck struct {
	Check      checkFunc
	LegacyName string
}

// schemaChecks is the sanity-check fallback used to confirm the schema
// a legacy-migration-name-detected version implies is actually present,
// before stamping. It does not need an entry for every one of the 29
// goose migrations — only enough to validate DetectSchemaVersion's
// answer is plausible. Entries are evaluated independently, not walked
// in order, so keep each one delta-only (what that migration added),
// not cumulative.
var schemaChecks = map[int64]schemaCheck{
	1: {Check: all(
		hasTable("nodes", map[string]string{
			"public_key": "TEXT", "name": "TEXT", "role": "TEXT",
			"lat": "REAL", "lon": "REAL",
			"last_seen": "TEXT", "first_seen": "TEXT",
			"advert_count": "INTEGER",
		}),
		hasTable("observers", map[string]string{
			"id": "TEXT", "name": "TEXT",
			"last_seen": "TEXT", "first_seen": "TEXT",
			"packet_count": "INTEGER", "model": "TEXT",
			"firmware": "TEXT", "client_version": "TEXT",
			"radio": "TEXT", "battery_mv": "INTEGER",
			"uptime_secs": "INTEGER", "noise_floor": "REAL",
		}),
		hasIndex("idx_nodes_last_seen"),
		hasIndex("idx_observers_last_seen"),

		hasTable("inactive_nodes", map[string]string{
			"public_key": "TEXT", "name": "TEXT", "role": "TEXT",
			"lat": "REAL", "lon": "REAL",
			"last_seen": "TEXT", "first_seen": "TEXT",
			"advert_count": "INTEGER",
		}),
		hasIndex("idx_inactive_nodes_last_seen"),

		hasTable("transmissions", map[string]string{
			"id": "INTEGER", "raw_hex": "TEXT", "hash": "TEXT",
			"first_seen": "TEXT", "route_type": "INTEGER",
			"payload_type": "INTEGER", "payload_version": "INTEGER",
			"decoded_json": "TEXT", "created_at": "TEXT",
		}),
		hasIndex("idx_transmissions_hash"),
		hasIndex("idx_transmissions_first_seen"),
		hasIndex("idx_transmissions_payload_type"),

		hasTable("client_receptions", map[string]string{
			"id": "INTEGER", "rx_pubkey": "TEXT", "heard_key": "TEXT",
			"heard_keylen": "INTEGER", "rssi": "INTEGER", "snr": "REAL",
			"lat": "REAL", "lon": "REAL", "pos_acc_m": "REAL",
			"rx_at": "TEXT", "ingested_at": "TEXT", "src": "TEXT",
		}),
		hasIndex("idx_client_recept_heard_geo"),
		hasIndex("idx_client_recept_latlon"),
		hasIndex("idx_client_recept_rxat"),

		hasTable("client_observers", map[string]string{
			"pubkey": "TEXT", "name": "TEXT", "last_seen": "TEXT",
		}),
	)},
	2: {Check: all(
		hasTable("observations", map[string]string{
			"transmission_id": "INTEGER", "observer_idx": "INTEGER", "direction": "TEXT",
			"snr": "REAL", "rssi": "REAL",
			"score": "INTEGER", "path_json": "TEXT",
			"timestamp": "INTEGER",
		}),
		hasIndex("idx_observations_transmission_id"),
		hasIndex("idx_observations_observer_idx"),
		hasIndex("idx_observations_timestamp"),
		hasIndex("idx_observations_dedup"),
		hasIndex("idx_observations_tx_ts"),
	)},
	3: {Check: hasView("packets_v")},
	4: {Check: all(hasColumn("transmissions", "from_pubkey", "TEXT"),
		hasIndex("idx_transmissions_from_pubkey"))},
	5: {LegacyName: "advert_count_unique_v1"},
	6: {LegacyName: "node_telemetry_v1"},
	7: {LegacyName: "noise_floor_real_v1"},
	8: {Check: hasIndex("idx_observations_timestamp")},
	9: {Check: hasTable("observer_metrics", map[string]string{
		"observer_id": "TEXT", "timestamp": "TEXT", "noise_floor": "REAL",
		"tx_air_secs": "INTEGER", "rx_air_secs": "INTEGER",
		"recv_errors": "INTEGER", "battery_mv": "INTEGER",
	})},
	10: {Check: hasIndex("idx_observer_metrics_timestamp")},
	11: {Check: all(hasColumn("observer_metrics", "packets_sent", "INTEGER"),
		hasColumn("observer_metrics", "packets_recv", "INTEGER"))},
	12: {Check: hasColumn("observers", "inactive", "INTEGER")},
	13: {Check: all(hasColumn("transmissions", "channel_hash", "TEXT"),
		hasIndex("idx_tx_channel_hash"),
	)},
	14: {LegacyName: "channel_hash_v1"},
	15: {Check: all(hasTable("dropped_packets", map[string]string{
		"id": "INTEGER", "hash": "TEXT", "raw_hex": "TEXT",
		"reason": "TEXT", "observer_id": "TEXT", "observer_name": "TEXT",
		"node_pubkey": "TEXT", "node_name": "TEXT", "dropped_at": "DATETIME"}),
		hasIndex("idx_dropped_observer"),
		hasIndex("idx_dropped_node"))},
	16: {Check: hasColumn("observers", "last_packet_at", "TEXT")},
	17: {LegacyName: "observers_last_packet_at_v1"},
	18: {LegacyName: "cleanup_legacy_null_hash_ts"},
	19: {Check: all(hasColumn("nodes", "foreign_advert", "INTEGER"),
		hasColumn("inactive_nodes", "foreign_advert", "INTEGER"),
		hasIndex("idx_nodes_foreign_advert"))},

	20: {LegacyName: "channel_hash_casing_v1"},
	21: {Check: all(hasColumn("transmissions", "scope_name", "TEXT"),
		hasIndex("idx_tx_scope_name"))},
	22: {Check: all(hasColumn("nodes", "default_scope", "TEXT"),
		hasColumn("inactive_nodes", "default_scope", "TEXT"))},
	23: {Check: hasColumn("observations", "raw_hex", "TEXT")},
	24: {Check: all(hasColumn("observers", "clock_skew_seconds", "INTEGER"),
		hasColumn("observers", "clock_skew_count_24h", "INTEGER"),
		hasColumn("observers", "clock_last_naive_at", "TEXT"))},
	25: {Check: hasIndex("idx_observations_observer_idx_timestamp")},
	26: {Check: hasTable("neighbor_edges", map[string]string{
		"node_a": "TEXT", "node_b": "TEXT", "count": "INTEGER", "last_seen": "TEXT"})},
	27: {Check: hasColumn("observations", "resolved_path", "TEXT")},
	28: {Check: hasColumn("observers", "iata", "TEXT")},
	29: {Check: all(hasColumn("nodes", "multibyte_sup", "INTEGER"),
		hasColumn("nodes", "multibyte_evidence", "TEXT"),
		hasColumn("inactive_nodes", "multibyte_sup", "INTEGER"),
		hasColumn("inactive_nodes", "multibyte_evidence", "TEXT"))},
	30: {Check: all(hasColumn("observers", "can_relay", "INTEGER"),
		hasColumn("observers", "can_relay_seen", "INTEGER"))},
	31: {Check: all(hasColumn("transmissions", "last_seen", "INTEGER"),
		hasIndex("idx_tx_last_seen_zero"))},
}

// loadLegacyAppliedNames returns the set of migration names recorded as
// applied in the old system's _migration table. Returns an empty set
// (not an error) if _migration doesn't exist.
func loadLegacyAppliedNames(db *sql.DB) (map[string]bool, error) {
	exists, err := TableExists(db, "_migrations")
	if err != nil {
		return nil, fmt.Errorf("checking for _migrations table: %w", err)
	}
	if !exists {
		return map[string]bool{}, nil
	}

	rows, err := db.Query(`SELECT name FROM _migrations`)
	if err != nil {
		return nil, fmt.Errorf("reading _migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scanning migration name: %w", err)
		}
		applied[name] = true
	}
	return applied, rows.Err()
}

// StampVersions marks each version in versions as applied in goose's
// version table, without executing any migration SQL. Safe to call
// repeatedly — already-stamped versions are skipped. Only the specific
// versions given are marked; any gaps between them are left pending for
// goose's own Up (with WithAllowOutofOrder) to fill in.
func StampVersions(db *sql.DB, versions []int64) error {
	provider, err := NewMigrationProvider(db, false, false)
	if err != nil {
		return fmt.Errorf("creating goose provider: %w", err)
	}
	if _, err := provider.Status(context.Background()); err != nil {
		return fmt.Errorf("ensuring version table: %w", err)
	}

	for _, v := range versions {
		var exists bool
		err := db.QueryRow(
			`SELECT EXISTS(SELECT 1 FROM goose_db_version WHERE version_id = ? AND is_applied = 1)`,
			v,
		).Scan(&exists)
		if err != nil {
			return fmt.Errorf("checking existing stamp for v%d: %w", v, err)
		}
		if exists {
			continue
		}
		if _, err := db.Exec(
			`INSERT INTO goose_db_version (version_id, is_applied, tstamp) VALUES (?, 1, CURRENT_TIMESTAMP)`,
			v,
		); err != nil {
			return fmt.Errorf("stamping v%d: %w", v, err)
		}
	}
	return nil
}

// BaselineResult reports what Baseline found on a database: everything
// already goose-tracked before Baseline ran, everything it newly
// detected and stamped, and everything still left pending afterward.
type BaselineResult struct {
	AlreadyTracked []int64 // was StateApplied before Baseline ran
	NewlyStamped   []int64 // detected via schemaChecks/legacy table and stamped this run
	StillPending   []int64 // could not be confirmed; left for Up to apply for real
}

// detectBaseline walks all migrations via goose's Status and determines
// which ones can be proven already applied — via schema shape
// (schemaChecks) or the old system's _migrations table — without
// writing anything to the  This is the shared core used by
// Baseline, BaselineAndStamp, and BaselineStampMigrate.
func detectBaseline(db *sql.DB, log func(format string, args ...any), disableVersioning bool) (result BaselineResult, toStamp []int64, err error) {
	if log == nil {
		log = func(string, ...any) {}
	}

	legacyApplied, err := loadLegacyAppliedNames(db)
	if err != nil {
		return BaselineResult{}, nil, err
	}

	provider, err := NewMigrationProvider(db, false, disableVersioning)
	if err != nil {
		return BaselineResult{}, nil, fmt.Errorf("creating goose provider: %w", err)
	}
	migrations, err := provider.Status(context.Background())
	if err != nil {
		return BaselineResult{}, nil, fmt.Errorf("checking migration status: %w", err)
	}

	for _, m := range migrations {
		v := m.Source.Version

		if m.State == goose.StateApplied {
			result.AlreadyTracked = append(result.AlreadyTracked, v)
			continue
		}

		sc, ok := schemaChecks[v]
		if !ok {
			result.StillPending = append(result.StillPending, v)
			continue
		}

		var satisfied bool
		switch {
		case sc.LegacyName != "":
			log("checking %s", sc.LegacyName)
			satisfied = legacyApplied[sc.LegacyName]
		case sc.Check != nil:
			satisfied, err = sc.Check(db)
			if err != nil {
				return BaselineResult{}, nil, fmt.Errorf("checking schema for migration v%d: %w", v, err)
			}
		default:
			return BaselineResult{}, nil, fmt.Errorf("schemaChecks[%d] has neither Check nor LegacyName set", v)
		}
		log("satisfied: %v", satisfied)

		if satisfied {
			toStamp = append(toStamp, v)
			result.NewlyStamped = append(result.NewlyStamped, v)
		} else {
			result.StillPending = append(result.StillPending, v)
		}
	}

	return result, toStamp, nil
}

// Baseline reports which individual goose migrations can be proven
// already applied to db, without writing anything to the  Use
// this to inspect what BaselineAndStamp would do before committing to
// it.
func Baseline(db *sql.DB, log func(format string, args ...any)) (BaselineResult, error) {
	result, _, err := detectBaseline(db, log, true)
	return result, err
}

// BaselineAndStamp detects which individual goose migrations can be
// proven already applied to db and stamps exactly those versions in
// goose_db_version, without running any migration SQL. Versions that
// can't be confirmed are left pending for a later Up (e.g. run by the
// ingestor at startup) to apply for real.
func BaselineAndStamp(db *sql.DB, log func(format string, args ...any)) (BaselineResult, error) {
	result, toStamp, err := detectBaseline(db, log, false)
	if err != nil {
		return BaselineResult{}, err
	}
	if len(toStamp) > 0 {
		if err := StampVersions(db, toStamp); err != nil {
			return BaselineResult{}, err
		}
	}
	return result, nil
}

// BaselineStampMigrate brings db fully up to the latest goose version
// in one pass: it stamps whichever individual migrations can be proven
// already applied, then runs goose Up (with out-of-order allowed) to
// apply everything still pending — including any gaps baselining left
// behind, and, for a genuinely fresh database, everything from scratch.
//
// On return, db is fully migrated to head.
func BaselineStampMigrate(db *sql.DB, log func(format string, args ...any)) (BaselineResult, error) {
	result, err := BaselineAndStamp(db, log)
	if err != nil {
		return BaselineResult{}, err
	}

	provider, err := NewMigrationProvider(db, true, false)
	if err != nil {
		return BaselineResult{}, fmt.Errorf("creating goose provider: %w", err)
	}
	if _, err := provider.Up(context.Background()); err != nil {
		return BaselineResult{}, fmt.Errorf("applying remaining migrations: %w", err)
	}
	return result, nil
}

// AssertBaselined refuses a database that has pre-existing schema (raw
// tables from before goose) but was never brought fully under goose's
// control via `migrate -baseline-and-stamp`. A genuinely fresh
// database (no core tables at all) is not an error — Up will apply the
// full chain from scratch. It returns an error naming the first gap
// found if any is missing — not just checking the overall max version,
// since a database can report a high max version while still having a
// gap somewhere below the adoption threshold (e.g. a partial or failed
// baseline run).
func AssertBaselined(db *sql.DB) error {
	hasNodes, err := TableExists(db, "nodes")
	if err != nil {
		return fmt.Errorf("checking for existing schema: %w", err)
	}
	hasTransmissions, err := TableExists(db, "transmissions")
	if err != nil {
		return fmt.Errorf("checking for existing schema: %w", err)
	}
	if !hasNodes && !hasTransmissions {
		return nil // genuinely fresh — nothing to baseline
	}

	provider, err := NewMigrationProvider(db, false, false)
	if err != nil {
		return fmt.Errorf("creating goose provider: %w", err)
	}

	statuses, err := provider.Status(context.Background())
	if err != nil {
		return fmt.Errorf("checking migration status: %w", err)
	}

	applied := make(map[int64]bool, len(statuses))
	for _, s := range statuses {
		if s.State == goose.StateApplied {
			applied[s.Source.Version] = true
		}
	}

	var missing []int64
	for v := int64(1); v <= GooseAdoptionVersion; v++ {
		if !applied[v] {
			missing = append(missing, v)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf(
			"database has pre-existing schema but is missing goose migrations %v (versions 1-%d must all be applied); "+
				"run `migrate -baseline-and-stamp` against it first", missing, GooseAdoptionVersion)
	}
	return nil
}

// migrationVersionPrefix extracts the numeric version from a goose
// migration filename like "00006_node_telemetry_v1.sql" -> 6.
// Returns false if the filename doesn't match the expected pattern.
func migrationVersionPrefix(name string) (int64, bool) {
	underscore := strings.IndexByte(name, '_')
	if underscore <= 0 {
		return 0, false
	}
	v, err := strconv.ParseInt(name[:underscore], 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}
