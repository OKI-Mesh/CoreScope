package database_test

import (
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
)

func observerName(t *testing.T, db *database.DB, pubkey string) *string {
	t.Helper()
	var name *string
	if err := db.Pool().QueryRow(t.Context(), `SELECT name FROM observers WHERE pubkey = $1`, pubkey).Scan(&name); err != nil {
		t.Fatalf("read name for %s: %v", pubkey, err)
	}
	return name
}

func TestCreateOrUpdateObserver_NameNilKeepsBlankOverwrites(t *testing.T) {
	ctx := t.Context()
	db := newDB(t)
	now := time.Now().UTC()
	write := func(name *string) {
		t.Helper()
		if _, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
			ID: "obs-name", Name: name, LastSeen: now, IngestNow: now,
		}); err != nil {
			t.Fatalf("CreateOrUpdateObserver(%v): %v", name, err)
		}
	}

	write(nil)
	if got := observerName(t, db, "obs-name"); got != nil {
		t.Fatalf("first write with no name: want NULL, got %q", *got)
	}

	write(ptr("Hilltop"))
	if got := observerName(t, db, "obs-name"); got == nil || *got != "Hilltop" {
		t.Fatalf("named write: want Hilltop, got %v", got)
	}

	write(nil)
	if got := observerName(t, db, "obs-name"); got == nil || *got != "Hilltop" {
		t.Fatalf("write with no name must keep the stored name: want Hilltop, got %v", got)
	}

	write(ptr(""))
	if got := observerName(t, db, "obs-name"); got == nil || *got != "" {
		t.Fatalf("write with a blank name must overwrite: want blank, got %v", got)
	}
}

func TestUpdateObserverMetadataIfRetained_NameNilKeepsBlankOverwrites(t *testing.T) {
	ctx := t.Context()
	db := newDB(t)
	now := time.Now().UTC()
	if _, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID: "obs-retained", Name: ptr("Hilltop"), LastSeen: now, IngestNow: now,
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.UpdateObserverMetadataIfRetained(ctx, database.ObserverMetadataInput{ID: "obs-retained"}); err != nil {
		t.Fatal(err)
	}
	if got := observerName(t, db, "obs-retained"); got == nil || *got != "Hilltop" {
		t.Fatalf("retained update with no name must keep it: want Hilltop, got %v", got)
	}

	if err := db.UpdateObserverMetadataIfRetained(ctx, database.ObserverMetadataInput{ID: "obs-retained", Name: ptr("")}); err != nil {
		t.Fatal(err)
	}
	if got := observerName(t, db, "obs-retained"); got == nil || *got != "" {
		t.Fatalf("retained update with a blank name must overwrite: want blank, got %v", got)
	}
}
