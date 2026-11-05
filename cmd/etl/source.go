package main

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"

	_ "modernc.org/sqlite"
)

// Source is the SQLite snapshot being read. It is opened read-only.
type Source struct {
	db *sql.DB
}

func openSource(path string) (*Source, error) {
	if path == "" {
		return nil, errors.New("-sqlite is required")
	}
	if strings.ContainsAny(path, "?#") {
		return nil, fmt.Errorf("snapshot path %q contains ? or #", path)
	}
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("snapshot: %w", err)
	}
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		return nil, fmt.Errorf("open snapshot: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("open snapshot: %w", err)
	}
	return &Source{db: db}, nil
}

func (s *Source) Close() error {
	return s.db.Close()
}

// ptr returns a pointer to v when valid, and nil (SQL NULL) otherwise.
func ptr[T any](v T, valid bool) *T {
	if !valid {
		return nil
	}
	return &v
}

// textOrNil is ptr for a text column where the empty string also means NULL.
func textOrNil(s sql.NullString) *string {
	if !s.Valid || s.String == "" {
		return nil
	}
	return &s.String
}
