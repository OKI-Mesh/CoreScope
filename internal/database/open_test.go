package database

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func tempDBPath(t *testing.T) string {
	t.Helper()
	name := strings.ReplaceAll(t.Name(), "/", "_")
	return filepath.Join(t.TempDir(), name+".db")
}

func newMemoryDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func execRaw(t *testing.T, db *sql.DB, sqlText string) {
	t.Helper()
	if _, err := db.Exec(sqlText); err != nil {
		t.Fatalf("exec raw sql: %v\nsql:\n%s", err, sqlText)
	}
}

func TestOpenReadWrite_CreatesFile(t *testing.T) {
	dbPath := tempDBPath(t)

	db, err := OpenReadWrite(dbPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Errorf("expected pingable connection, got: %v", err)
	}
	if _, err := os.Stat(dbPath); err != nil {
		t.Errorf("expected db file to be created, got: %v", err)
	}
}

func TestOpenReadWrite_CreatesParentDirectory(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "nested", "subdir", "test.db")

	db, err := OpenReadWrite(dbPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer db.Close()

	if _, err := os.Stat(filepath.Dir(dbPath)); err != nil {
		t.Errorf("expected parent directory to be created, got: %v", err)
	}
}

func TestOpenReadWrite_NoSchemaAssertion(t *testing.T) {
	// OpenReadWrite must NOT touch schema state at all — no migration,
	// no readiness check. A completely empty database should open
	// without error; asserting readiness is the caller's job (via
	// migrate.AssertBaselined), not this function's.
	dbPath := tempDBPath(t)

	db, err := OpenReadWrite(dbPath)
	if err != nil {
		t.Fatalf("expected OpenReadWrite to succeed on an empty database with no assertions, got: %v", err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master`).Scan(&count); err != nil {
		t.Fatalf("unexpected error querying sqlite_master: %v", err)
	}
	if count != 0 {
		t.Errorf("expected a genuinely empty database (no tables created), got %d objects", count)
	}
}

func TestOpenReadOnly_Succeeds(t *testing.T) {
	dbPath := tempDBPath(t)

	// Create the file first via a normal read-write connection.
	rw, err := OpenReadWrite(dbPath)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	rw.Close()

	ro, err := OpenReadOnly(dbPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer ro.Close()

	if err := ro.Ping(); err != nil {
		t.Errorf("expected pingable connection, got: %v", err)
	}
}

func TestOpenReadOnly_NoSchemaAssertion(t *testing.T) {
	// Same principle as OpenReadWrite — OpenReadOnly must not assert
	// readiness. It should succeed against any existing file,
	// regardless of migration state; that check is the caller's job.
	dbPath := tempDBPath(t)
	rw, err := OpenReadWrite(dbPath)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	rw.Close()

	ro, err := OpenReadOnly(dbPath)
	if err != nil {
		t.Fatalf("expected OpenReadOnly to succeed with no readiness assertion, got: %v", err)
	}
	ro.Close()
}

func TestOpenReadOnly_MissingFile(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "does_not_exist.db")

	// SQLite's mode=ro refuses to open (let alone create) a
	// nonexistent file — confirm that's still true through this
	// wrapper, since ro mode intentionally never creates files.
	if _, err := OpenReadOnly(dbPath); err == nil {
		t.Error("expected OpenReadOnly to fail against a nonexistent file")
	}
}
