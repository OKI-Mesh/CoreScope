package dbschema

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// minimalDB bootstraps a SQLite DB with just enough tables for the
// ensure_* helpers to run against, but WITHOUT any of the optional
// columns that dbschema.Apply is responsible for ensuring.
func minimalDB(t *testing.T) *sql.DB {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "schema.db")
	db, err := sql.Open("sqlite", "file:"+dbPath+"?_journal_mode=WAL")
	if err != nil {
		t.Fatal(err)
	}
	stmts := []string{
		// Bare-bones tables, mirroring the legacy/empty fixture shape
		// pre-migration. Intentionally omit columns we expect Apply to add.
		`CREATE TABLE transmissions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			raw_hex TEXT NOT NULL,
			hash TEXT NOT NULL UNIQUE,
			first_seen TEXT NOT NULL,
			route_type INTEGER,
			payload_type INTEGER,
			payload_version INTEGER,
			decoded_json TEXT
		)`,
		`CREATE TABLE observations (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			transmission_id INTEGER NOT NULL,
			observer_idx INTEGER,
			direction TEXT,
			snr REAL,
			rssi REAL,
			score INTEGER,
			path_json TEXT,
			timestamp INTEGER NOT NULL
		)`,
		`CREATE TABLE observers (
			id TEXT PRIMARY KEY,
			name TEXT
		)`,
		`CREATE TABLE nodes (
			public_key TEXT PRIMARY KEY,
			name TEXT
		)`,
		`CREATE TABLE inactive_nodes (
			public_key TEXT PRIMARY KEY,
			name TEXT,
			last_seen TEXT
		)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("bootstrap: %v", err)
		}
	}
	return db
}
