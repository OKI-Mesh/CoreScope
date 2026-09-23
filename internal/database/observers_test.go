// internal/database/observers_test.go
package database

import (
	"testing"
)

func TestSoftDeleteBlacklistedObservers(t *testing.T) {
	db := newMemoryDB(t)
	execRaw(t, db, `CREATE TABLE observers (id TEXT PRIMARY KEY, inactive INTEGER)`)
	execRaw(t, db, `INSERT INTO observers (id, inactive) VALUES
		('bad-observer-1', 0 ),
		('BAD-OBSERVER-2', 0),
		('good-observer', 0),
		('already-inactive', 1)`)

	n, err := SoftDeleteBlacklistedObservers(db, []string{"bad-observer-1", "bad-observer-2"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 2 {
		t.Errorf("expected 2 rows affected, got %d", n)
	}

	var inactive int
	if err := db.QueryRow(`SELECT inactive FROM observers WHERE id = 'bad-observer-1'`).Scan(&inactive); err != nil {
		t.Fatal(err)
	}
	if inactive != 1 {
		t.Errorf("bad-observer-1: expected inactive=1, got %d", inactive)
	}

	// Case-insensitive match: BAD-OBSERVER-2 should be caught by lowercase blacklist entry
	if err := db.QueryRow(`SELECT inactive FROM observers WHERE id = 'BAD-OBSERVER-2'`).Scan(&inactive); err != nil {
		t.Fatal(err)
	}
	if inactive != 1 {
		t.Errorf("BAD-OBSERVER-2: expected inactive=1 (case-insensitive match), got %d", inactive)
	}

	// Untouched rows
	if err := db.QueryRow(`SELECT inactive FROM observers WHERE id = 'good-observer'`).Scan(&inactive); err != nil {
		t.Fatal(err)
	}
	if inactive != 0 {
		t.Errorf("good-observer should remain untouched (NULL), got %d", inactive)
	}
}

func TestSoftDeleteBlacklistedObservers_EmptyBlacklist(t *testing.T) {
	db := newMemoryDB(t)
	execRaw(t, db, `CREATE TABLE observers (id TEXT PRIMARY KEY, inactive INTEGER)`)
	execRaw(t, db, `INSERT INTO observers (id, inactive) VALUES ('obs1', NULL)`)

	n, err := SoftDeleteBlacklistedObservers(db, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 rows affected for empty blacklist, got %d", n)
	}
}

func TestSoftDeleteBlacklistedObservers_BlankEntriesIgnored(t *testing.T) {
	db := newMemoryDB(t)
	execRaw(t, db, `CREATE TABLE observers (id TEXT PRIMARY KEY, inactive INTEGER)`)
	execRaw(t, db, `INSERT INTO observers (id, inactive) VALUES ('obs1', NULL)`)

	// Blacklist entries that are only whitespace should be skipped, not
	// error or match every row.
	n, err := SoftDeleteBlacklistedObservers(db, []string{"  ", "", "\t"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 rows affected when blacklist entries are all blank, got %d", n)
	}
}

func TestSoftDeleteBlacklistedObservers_AlreadyInactiveNotDoubleCounted(t *testing.T) {
	db := newMemoryDB(t)
	execRaw(t, db, `CREATE TABLE observers (id TEXT PRIMARY KEY, inactive INTEGER)`)
	execRaw(t, db, `INSERT INTO observers (id, inactive) VALUES ('obs1', 1)`)

	n, err := SoftDeleteBlacklistedObservers(db, []string{"obs1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 rows affected (already inactive, filtered by WHERE clause), got %d", n)
	}
}
