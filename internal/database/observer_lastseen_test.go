package database_test

import (
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
)

func observerLastSeen(t *testing.T, db *database.DB, pubkey string) time.Time {
	t.Helper()
	var lastSeen time.Time
	if err := db.Pool().QueryRow(t.Context(), `SELECT last_seen FROM observers WHERE pubkey = $1`, pubkey).Scan(&lastSeen); err != nil {
		t.Fatalf("read last_seen for %s: %v", pubkey, err)
	}
	return lastSeen.UTC()
}

func TestCreateOrUpdateObserver_OlderLastSeenNeverMovesBack_DB(t *testing.T) {
	ctx := t.Context()
	db := newDB(t)
	recent := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	write := func(id string, lastSeen, ingestNow time.Time) {
		t.Helper()
		if _, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
			ID: id, IATA: "SJC", LastSeen: lastSeen, IngestNow: ingestNow,
		}); err != nil {
			t.Fatalf("CreateOrUpdateObserver(%s, last_seen %v, ingest_now %v): %v", id, lastSeen, ingestNow, err)
		}
	}
	expect := func(id string, want time.Time, why string) {
		t.Helper()
		if got := observerLastSeen(t, db, id); !got.Equal(want) {
			t.Errorf("%s: last_seen = %v, want %v", why, got, want)
		}
	}

	write("obs-mono", recent, recent)
	expect("obs-mono", recent, "first write")

	write("obs-mono", recent.Add(-time.Hour), recent)
	expect("obs-mono", recent, "an older last_seen must not move it back")

	write("obs-mono", recent.Add(time.Hour), recent.Add(time.Hour))
	expect("obs-mono", recent.Add(time.Hour), "a newer last_seen moves it forward")

	write("obs-mono", recent.Add(-2*time.Hour), recent.Add(2*time.Hour))
	expect("obs-mono", recent.Add(time.Hour), "an older last_seen must not move it back after it moved forward")

	write("obs-clamp", recent.Add(3*time.Hour), recent.Add(3*time.Hour))
	expect("obs-clamp", recent.Add(3*time.Hour), "first write of the clamp case")

	write("obs-clamp", recent.Add(-time.Hour), recent)
	expect("obs-clamp", recent, "a stored last_seen ahead of ingest_now is pulled back to ingest_now")
}
