package database_test

import (
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
)

func TestSoftDeleteBlacklistedObservers(t *testing.T) {
	ctx := t.Context()
	db := dbtest.New(t)
	now := time.Now().UTC()
	dbtest.Observer(t, db, "bad-observer-1", "bad one", "", now, now)
	dbtest.Observer(t, db, "bad-observer-2", "bad two", "", now, now)
	dbtest.Observer(t, db, "good-observer", "good", "", now, now)

	n, err := db.SoftDeleteBlacklistedObservers(ctx, []string{" BAD-OBSERVER-1 ", "bad-observer-2"})
	if err != nil {
		t.Fatalf("SoftDeleteBlacklistedObservers(): %v", err)
	}
	if n != 2 {
		t.Errorf("expected 2 rows affected, got %d", n)
	}

	for _, pubkey := range []string{"bad-observer-1", "bad-observer-2"} {
		var inactive bool
		if err := db.Pool().QueryRow(ctx, `SELECT inactive FROM observers WHERE pubkey = $1`, pubkey).Scan(&inactive); err != nil {
			t.Fatalf("read inactive for %s: %v", pubkey, err)
		}
		if !inactive {
			t.Errorf("%s: expected inactive=true", pubkey)
		}
	}
	var inactive bool
	if err := db.Pool().QueryRow(ctx, `SELECT inactive FROM observers WHERE pubkey = 'good-observer'`).Scan(&inactive); err != nil {
		t.Fatalf("read inactive for good-observer: %v", err)
	}
	if inactive {
		t.Error("good-observer should remain active")
	}
}

func TestSoftDeleteBlacklistedObservers_EmptyBlacklist(t *testing.T) {
	ctx := t.Context()
	db := dbtest.New(t)
	now := time.Now().UTC()
	dbtest.Observer(t, db, "obs1", "observer", "", now, now)

	n, err := db.SoftDeleteBlacklistedObservers(ctx, nil)
	if err != nil {
		t.Fatalf("SoftDeleteBlacklistedObservers(): %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 rows affected for empty blacklist, got %d", n)
	}
}

func TestSoftDeleteBlacklistedObservers_BlankEntriesIgnored(t *testing.T) {
	ctx := t.Context()
	db := dbtest.New(t)
	now := time.Now().UTC()
	dbtest.Observer(t, db, "obs1", "observer", "", now, now)

	n, err := db.SoftDeleteBlacklistedObservers(ctx, []string{"  ", "", "\t"})
	if err != nil {
		t.Fatalf("SoftDeleteBlacklistedObservers(): %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 rows affected when blacklist entries are all blank, got %d", n)
	}
}

func TestSoftDeleteBlacklistedObservers_AlreadyInactiveNotDoubleCounted(t *testing.T) {
	ctx := t.Context()
	db := dbtest.New(t)
	now := time.Now().UTC()
	dbtest.Observer(t, db, "obs1", "observer", "", now, now)
	if _, err := db.SoftDeleteBlacklistedObservers(ctx, []string{"obs1"}); err != nil {
		t.Fatalf("first SoftDeleteBlacklistedObservers(): %v", err)
	}

	n, err := db.SoftDeleteBlacklistedObservers(ctx, []string{"obs1"})
	if err != nil {
		t.Fatalf("second SoftDeleteBlacklistedObservers(): %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 rows affected for already inactive observer, got %d", n)
	}
}
