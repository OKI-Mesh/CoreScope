// internal/database/detect_test.go
package database

import (
	"testing"
)

// TestDetectAndStampSchema_FullyCurrentSchema builds a database with
// every structural marker present and confirms detection stamps every
// migration, leaving nothing for RunMigrationsAllowingGaps to add.
func TestDetectAndStampSchema_FullyCurrentSchema(t *testing.T) {
	db := openTestDB(t)

	// Build full current schema via RunMigrations, then wipe
	// goose_db_version to simulate "real schema, never stamped."
	if err := RunMigrations(db); err != nil {
		t.Fatalf("initial migrate: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM goose_db_version`); err != nil {
		t.Fatalf("wipe goose_db_version: %v", err)
	}

	state, _, err := MigrationStatus(db)
	if err != nil {
		t.Fatal(err)
	}
	if state != StateUnstamped {
		t.Fatalf("precondition failed: state = %v, want StateUnstamped", state)
	}

	if err := DetectAndStampSchema(db); err != nil {
		t.Fatalf("DetectAndStampSchema: %v", err)
	}

	version, err := CurrentSchemaVersion(db)
	if err != nil {
		t.Fatal(err)
	}
	if version != 29 {
		t.Errorf("expected detection to stamp up to v29 on fully current schema, got %d", version)
	}

	// RunMigrationsAllowingGaps should now be a clean no-op.
	if err := RunMigrationsAllowingGaps(db); err != nil {
		t.Fatalf("RunMigrationsAllowingGaps after full detection: %v", err)
	}
}

// TestDetectAndStampSchema_GapInHistory reproduces the real-world case
// found in e2e-fixture.db: a database with a LATER marker present
// (observers.iata, v26) but an EARLIER marker absent
// (observers.clock_skew_seconds, v22). Confirms detection stamps only
// what's actually present, and RunMigrationsAllowingGaps fills the gap
// out of order without erroring.
func TestDetectAndStampSchema_GapInHistory(t *testing.T) {
	db := openTestDB(t)

	if _, err := db.Exec(`CREATE TABLE nodes (
		public_key TEXT PRIMARY KEY,
		name TEXT,
		role TEXT,
		lat REAL,
		lon REAL,
		last_seen TEXT,
		first_seen TEXT,
		advert_count INTEGER DEFAULT 0
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE inactive_nodes (
		public_key TEXT PRIMARY KEY,
		name TEXT,
		role TEXT,
		lat REAL,
		lon REAL,
		last_seen TEXT,
		first_seen TEXT,
		advert_count INTEGER DEFAULT 0
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE transmissions (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		raw_hex TEXT NOT NULL,
		hash TEXT NOT NULL UNIQUE,
		first_seen TEXT NOT NULL,
		route_type INTEGER,
		payload_type INTEGER,
		payload_version INTEGER,
		decoded_json TEXT,
		created_at TEXT DEFAULT (datetime('now'))
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE observations (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		transmission_id INTEGER NOT NULL REFERENCES transmissions(id),
		observer_idx INTEGER,
		direction TEXT,
		snr REAL,
		rssi REAL,
		score INTEGER,
		path_json TEXT,
		timestamp INTEGER NOT NULL
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE observers (
		id TEXT PRIMARY KEY,
		name TEXT,
		iata TEXT,
		last_seen TEXT,
		first_seen TEXT,
		packet_count INTEGER DEFAULT 0,
		model TEXT,
		firmware TEXT,
		client_version TEXT,
		radio TEXT,
		battery_mv INTEGER,
		uptime_secs INTEGER,
		noise_floor REAL
	)`); err != nil {
		t.Fatal(err)
	}
	// NOTE: deliberately no clock_skew_seconds/clock_skew_count_24h/
	// clock_last_naive_at columns — this is the gap being tested.

	if err := DetectAndStampSchema(db); err != nil {
		t.Fatalf("DetectAndStampSchema: %v", err)
	}

	applied26, err := SchemaVersionApplied(db, 26)
	if err != nil {
		t.Fatal(err)
	}
	if !applied26 {
		t.Error("expected v26 (observers.iata) to be detected and stamped")
	}

	applied22, err := SchemaVersionApplied(db, 22)
	if err != nil {
		t.Fatal(err)
	}
	if applied22 {
		t.Error("expected v22 (observers.clock_skew_seconds) to NOT be stamped — column is absent")
	}

	if err := RunMigrationsAllowingGaps(db); err != nil {
		t.Fatalf("RunMigrationsAllowingGaps with gap in history: %v", err)
	}

	hasClockSkew, err := TableHasColumn(db, "observers", "clock_skew_seconds")
	if err != nil {
		t.Fatal(err)
	}
	if !hasClockSkew {
		t.Error("expected clock_skew_seconds column to exist after RunMigrationsAllowingGaps filled the v22 gap")
	}
}

// TestDetectAndStampSchema_NoMarkersPresent confirms detection is a
// safe no-op against a database with no structural markers at all
// (e.g. only the very base tables with no later columns) — nothing
// gets incorrectly stamped.
func TestDetectAndStampSchema_NoMarkersPresent(t *testing.T) {
	db := openTestDB(t)

	if _, err := db.Exec(`CREATE TABLE nodes (public_key TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE transmissions (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}

	if err := DetectAndStampSchema(db); err != nil {
		t.Fatalf("DetectAndStampSchema: %v", err)
	}

	version, err := CurrentSchemaVersion(db)
	if err != nil {
		t.Fatal(err)
	}
	if version != 0 {
		t.Errorf("expected no version stamped when no markers present, got %d", version)
	}
}
