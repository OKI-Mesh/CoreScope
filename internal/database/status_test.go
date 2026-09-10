// internal/database/status_test.go
package database

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	conn, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestMigrationStatus_Fresh(t *testing.T) {
	db := openTestDB(t)

	state, version, err := MigrationStatus(db)
	if err != nil {
		t.Fatalf("MigrationStatus: %v", err)
	}
	if state != StateFresh {
		t.Errorf("state = %v, want StateFresh", state)
	}
	if version != 0 {
		t.Errorf("version = %d, want 0", version)
	}
}

func TestMigrationStatus_Migrated(t *testing.T) {
	db := openTestDB(t)

	if err := RunMigrations(db); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}

	state, version, err := MigrationStatus(db)
	if err != nil {
		t.Fatalf("MigrationStatus: %v", err)
	}
	if state != StateMigrated {
		t.Errorf("state = %v, want StateMigrated", state)
	}
	if version != 29 {
		t.Errorf("version = %d, want 29", version)
	}
}

func TestMigrationStatus_Unstamped(t *testing.T) {
	db := openTestDB(t)

	// Real schema present (core tables), but never touched by goose —
	// no goose_db_version table at all.
	if _, err := db.Exec(`CREATE TABLE nodes (public_key TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE transmissions (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}

	state, version, err := MigrationStatus(db)
	if err != nil {
		t.Fatalf("MigrationStatus: %v", err)
	}
	if state != StateUnstamped {
		t.Errorf("state = %v, want StateUnstamped", state)
	}
	if version != 0 {
		t.Errorf("version = %d, want 0", version)
	}
}

func TestMigrationStatus_PartialTablesNotUnstamped(t *testing.T) {
	db := openTestDB(t)

	// Only ONE of the two core tables — MigrationStatus requires BOTH
	// nodes and transmissions to classify as StateUnstamped.
	if _, err := db.Exec(`CREATE TABLE nodes (public_key TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}

	state, _, err := MigrationStatus(db)
	if err != nil {
		t.Fatalf("MigrationStatus: %v", err)
	}
	if state != StateFresh {
		t.Errorf("state = %v, want StateFresh (partial schema should not count as unstamped)", state)
	}
}

func TestCurrentSchemaVersion_NoTable(t *testing.T) {
	db := openTestDB(t)

	version, err := CurrentSchemaVersion(db)
	if err != nil {
		t.Fatalf("CurrentSchemaVersion: %v", err)
	}
	if version != 0 {
		t.Errorf("version = %d, want 0", version)
	}
}

func TestCurrentSchemaVersion_AfterMigration(t *testing.T) {
	db := openTestDB(t)

	if err := RunMigrations(db); err != nil {
		t.Fatal(err)
	}

	version, err := CurrentSchemaVersion(db)
	if err != nil {
		t.Fatalf("CurrentSchemaVersion: %v", err)
	}
	if version != 29 {
		t.Errorf("version = %d, want 29", version)
	}
}

func TestSchemaVersionApplied(t *testing.T) {
	db := openTestDB(t)

	if err := RunMigrations(db); err != nil {
		t.Fatal(err)
	}

	applied, err := SchemaVersionApplied(db, 6)
	if err != nil {
		t.Fatalf("SchemaVersionApplied(6): %v", err)
	}
	if !applied {
		t.Error("expected version 6 to be applied after full migration")
	}

	applied, err = SchemaVersionApplied(db, 999)
	if err != nil {
		t.Fatalf("SchemaVersionApplied(999): %v", err)
	}
	if applied {
		t.Error("expected version 999 to NOT be applied (doesn't exist)")
	}
}

func TestSchemaVersionApplied_UnmigratedDB(t *testing.T) {
	db := openTestDB(t)

	applied, err := SchemaVersionApplied(db, 1)
	if err != nil {
		t.Fatalf("SchemaVersionApplied on unmigrated DB: %v", err)
	}
	if applied {
		t.Error("expected version 1 to NOT be applied on a fresh, unmigrated DB")
	}
}
