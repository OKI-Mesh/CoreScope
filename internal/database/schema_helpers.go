package database

import (
	"database/sql"
	"errors"
	"fmt"
)

// ObjectExists reports whether a sqlite_master object of the given type
// (e.g. "table", "view", "index") and name exists in db.
func ObjectExists(db *sql.DB, objType, name string) (bool, error) {
	var got string
	err := db.QueryRow(
		`SELECT name FROM sqlite_master WHERE type = ? AND name = ?`, objType, name,
	).Scan(&got)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("checking %s %q: %w", objType, name, err)
	}
	return true, nil
}

// TableExists reports whether a table with the given name exists.
func TableExists(db *sql.DB, name string) (bool, error) {
	return ObjectExists(db, "table", name)
}

// ViewExists reports whether a view with the given name exists.
func ViewExists(db *sql.DB, name string) (bool, error) {
	return ObjectExists(db, "view", name)
}

// IndexExists reports whether an index with the given name exists.
func IndexExists(db *sql.DB, name string) (bool, error) {
	return ObjectExists(db, "index", name)
}

// TableColumns returns a map of column name -> declared SQLite type for
// the given table, via PRAGMA table_info. Returns an empty map (not an
// error) if the table doesn't exist — callers should check TableExists
// first if that distinction matters.
//
// Note: table names cannot be parameterized in PRAGMA statements, so
// callers must only pass fixed/known table names, never user input.
func TableColumns(db *sql.DB, table string) (map[string]string, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%q)", table))
	if err != nil {
		return nil, fmt.Errorf("reading table_info for %q: %w", table, err)
	}
	defer rows.Close()

	cols := make(map[string]string)
	for rows.Next() {
		var cid int
		var name, colType string
		var notnull, pk int
		var dfltValue sql.NullString
		if err := rows.Scan(&cid, &name, &colType, &notnull, &dfltValue, &pk); err != nil {
			return nil, fmt.Errorf("scanning table_info row for %q: %w", table, err)
		}
		cols[name] = colType
	}
	return cols, rows.Err()
}
