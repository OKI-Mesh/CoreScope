package database

import (
	"context"
	"database/sql"
	"fmt"
)

// ObjectExists reports whether a table or view with the given name exists
// in the public schema. objType is "table" or "view" (matching Postgres's
// own information_schema.tables.table_type values via the switch below —
// view checks go through information_schema.views instead, which doesn't
// use table_type at all).
func ObjectExists(ctx context.Context, db *sql.DB, objType, name string) (bool, error) {
	var query string
	switch objType {
	case "table":
		query = `SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = $1
		)`
	case "view":
		query = `SELECT EXISTS (
			SELECT 1 FROM information_schema.views
			WHERE table_schema = 'public' AND table_name = $1
		)`
	default:
		return false, fmt.Errorf("unsupported object type %q", objType)
	}
	var exists bool
	if err := db.QueryRow(query, name).Scan(&exists); err != nil {
		return false, fmt.Errorf("checking %s %q: %w", objType, name, err)
	}
	return exists, nil
}

func TableExists(ctx context.Context, db *sql.DB, name string) (bool, error) {
	return ObjectExists(ctx, db, "table", name)
}

// ViewExists reports whether a view with the given name exists.
func ViewExists(ctx context.Context, db *sql.DB, name string) (bool, error) {
	return ObjectExists(ctx, db, "view", name)
}

// IndexExists reports whether an index with the given name exists.
func IndexExists(ctx context.Context, db *sql.DB, name string) (bool, error) {
	return ObjectExists(ctx, db, "index", name)
}

// TableColumns returns a map of column name -> declared SQLite type for
// the given table, via PRAGMA table_info. Returns an empty map (not an
// error) if the table doesn't exist — callers should check TableExists
// first if that distinction matters.
//
// Note: table names cannot be parameterized in PRAGMA statements, so
// callers must only pass fixed/known table names, never user input.
func TableColumns(ctx context.Context, db *sql.DB, table string) (map[string]string, error) {
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
