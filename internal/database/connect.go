package database

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
)

// OpenReadWrite opens or creates a SQLite database at path with
// CoreScope's standard read-write pragma configuration. Does not touch
// schema state in any way — callers that need the database migrated
// or asserted-ready compose this with internal/database/migrate.
func OpenReadWrite(path string) (*sql.DB, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating data dir: %w", err)
	}
	db, err := sql.Open("sqlite", path+"?_pragma=auto_vacuum(INCREMENTAL)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("opening db: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("pinging db: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, nil
}

// OpenReadOnly opens path as a read-only SQLite connection. Does not
// assert readiness — callers needing that guarantee compose this with
// migrate.AssertReady.
func OpenReadOnly(path string) (*sql.DB, error) {
	conn, err := sql.Open("sqlite", "file:"+path+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("opening db: %w", err)
	}
	if err := conn.Ping(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("pinging db: %w", err)
	}
	// Belt-and-suspenders: enforce read-only at the SQLite level
	// directly, independent of whether the driver honored mode=ro in
	// the DSN. query_only rejects any write statement outright.
	if _, err := conn.Exec(`PRAGMA query_only = ON`); err != nil {
		conn.Close()
		return nil, fmt.Errorf("setting query_only pragma: %w", err)
	}
	return conn, nil
}
