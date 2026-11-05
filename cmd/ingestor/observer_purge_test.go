package main

import (
	"testing"
	"time"
)

// staleObserver inserts an observer already soft-deleted (inactive = TRUE) with
// last_seen 90 days in the past, and returns its id.
func staleObserver(t *testing.T, s *Store, pubkey string) int64 {
	t.Helper()
	ctx := t.Context()
	if err := s.UpsertObserver(ctx, pubkey, pubkey, "LAX", nil); err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().AddDate(0, 0, -90)
	if n := testExec(t, s, `UPDATE observers SET last_seen = $1, inactive = TRUE WHERE pubkey = $2`, old, pubkey); n != 1 {
		t.Fatalf("stale update touched %d rows, want 1", n)
	}
	var id int64
	mustScan(t, testRow(t, s, `SELECT id FROM observers WHERE pubkey = $1`, pubkey), &id)
	return id
}

func observerExists(t *testing.T, s *Store, pubkey string) bool {
	t.Helper()
	return countWhere(t, s, "observers", "pubkey = $1", pubkey) > 0
}

func TestPurgeStaleObserversDeletesUnreferencedRow(t *testing.T) {
	store := newTestStore(t)
	staleObserver(t, store, "obs-gone")
	ctx := t.Context()

	purged, err := store.PurgeStaleObservers(ctx, 30)
	if err != nil {
		t.Fatal(err)
	}
	if purged != 1 {
		t.Errorf("purged=%d, want 1", purged)
	}
	if observerExists(t, store, "obs-gone") {
		t.Error("obs-gone still present, want hard-deleted")
	}
}

// Regression: an observer whose rowid is still referenced by observations must
// survive the purge — deleting it orphans observations.observer_idx, which
// breaks the packets_v join and mis-attributes historical packets.
func TestPurgeStaleObserversKeepsObserverWithObservations(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	obsID := staleObserver(t, store, "obs-referenced")
	now := time.Now().UTC()

	var txID int64
	mustScan(t, testRow(t, store,
		`INSERT INTO transmissions (raw_hex, hash, first_seen) VALUES (decode('aa','hex'), 'h1', $1) RETURNING id`, now), &txID)
	if n := testExec(t, store,
		`INSERT INTO observations (transmission_id, observer_idx, timestamp) VALUES ($1, $2, $3)`,
		txID, obsID, now); n != 1 {
		t.Fatalf("inserted %d observations, want 1", n)
	}

	purged, err := store.PurgeStaleObservers(ctx, 30)
	if err != nil {
		t.Fatal(err)
	}
	if purged != 0 {
		t.Errorf("purged=%d, want 0 (row is referenced by observations)", purged)
	}
	if !observerExists(t, store, "obs-referenced") {
		t.Error("obs-referenced was deleted, want kept")
	}

	orphans := countWhere(t, store, "observations",
		"observer_idx IS NOT NULL AND observer_idx NOT IN (SELECT id FROM observers)")
	if orphans != 0 {
		t.Errorf("orphaned observations=%d, want 0", orphans)
	}
}

func TestPurgeStaleObserversKeepsObserverWithMetrics(t *testing.T) {
	store := newTestStore(t)
	staleObserver(t, store, "obs-metrics")
	ctx := t.Context()
	count := testExec(t, store, `INSERT INTO observer_metrics (observer_id, timestamp) VALUES ('obs-metrics', $1)`, time.Now().UTC())
	if count != 1 {
		t.Fatal("failed to insert observer metric")
	}

	purged, err := store.PurgeStaleObservers(ctx, 30)
	if err != nil {
		t.Fatal(err)
	}
	if purged != 0 {
		t.Errorf("purged=%d, want 0 (row is referenced by observer_metrics)", purged)
	}
	if !observerExists(t, store, "obs-metrics") {
		t.Error("obs-metrics was deleted, want kept")
	}
}

func TestPurgeStaleObserversKeepsObserverWithDroppedPackets(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	staleObserver(t, store, "obs-dropped")
	staleObserver(t, store, "obs-unreferenced") // purged, proves the referenced one didn't block it

	if n := testExec(t, store,
		`INSERT INTO dropped_packets (reason, observer_id, dropped_at) VALUES ('bad-sig', $1, $2)`,
		"obs-dropped", time.Now().UTC()); n != 1 {
		t.Fatalf("inserted %d dropped packets, want 1", n)
	}

	purged, err := store.PurgeStaleObservers(ctx, 30)
	if err != nil {
		t.Fatal(err)
	}
	if purged != 1 {
		t.Errorf("purged=%d, want 1 (only obs-unreferenced)", purged)
	}
	if !observerExists(t, store, "obs-dropped") {
		t.Error("obs-dropped was deleted, want kept (referenced by dropped_packets)")
	}
	if observerExists(t, store, "obs-unreferenced") {
		t.Error("obs-unreferenced was kept, want purged")
	}
}

// An observer old enough to purge but not yet soft-deleted must be left to
// RemoveStaleObservers — the hard purge only ever finalises an inactive row.
func TestPurgeStaleObserversKeepsActiveObserver(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	if err := store.UpsertObserver(ctx, "obs-active", "Active", "LAX", nil); err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().AddDate(0, 0, -90)
	count := testExec(t, store, `UPDATE observers SET last_seen = $1 WHERE pubkey = $2`, old, "obs-active")
	if count != 1 {
		t.Fatal("failed to update observer")
	}

	purged, err := store.PurgeStaleObservers(ctx, 30)
	if err != nil {
		t.Fatal(err)
	}
	if purged != 0 {
		t.Errorf("purged=%d, want 0 (inactive = 0)", purged)
	}
	if !observerExists(t, store, "obs-active") {
		t.Error("obs-active was deleted, want kept")
	}
}

func TestPurgeStaleObserversKeepsObserverInsideWindow(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	if err := store.UpsertObserver(ctx, "obs-recent", "Recent", "LAX", nil); err != nil {
		t.Fatal(err)
	}
	recent := time.Now().UTC().AddDate(0, 0, -20)
	count := testExec(t, store, `UPDATE observers SET last_seen = $1, inactive = TRUE WHERE pubkey = $2`, recent, "obs-recent")
	if count != 1 {
		t.Fatal("failed to update observer")
	}

	purged, err := store.PurgeStaleObservers(ctx, 30)
	if err != nil {
		t.Fatal(err)
	}
	if purged != 0 {
		t.Errorf("purged=%d, want 0 (last_seen inside the 30-day window)", purged)
	}
	if !observerExists(t, store, "obs-recent") {
		t.Error("obs-recent was deleted, want kept")
	}
}

func TestPurgeStaleObserversDisabled(t *testing.T) {
	store := newTestStore(t)
	staleObserver(t, store, "obs-kept")

	ctx := t.Context()
	for _, days := range []int{0, -1} {
		purged, err := store.PurgeStaleObservers(ctx, days)
		if err != nil {
			t.Fatal(err)
		}
		if purged != 0 {
			t.Errorf("PurgeStaleObservers(%d) purged=%d, want 0 (disabled)", days, purged)
		}
	}
	if !observerExists(t, store, "obs-kept") {
		t.Error("obs-kept was deleted, want kept while purge is disabled")
	}
}

func TestObserverPurgeDaysOrZero(t *testing.T) {
	tests := []struct {
		name string
		cfg  *Config
		want int
	}{
		{"nil retention", &Config{}, 0},
		{"unset", &Config{Retention: &RetentionConfig{}}, 0},
		{"configured", &Config{Retention: &RetentionConfig{ObserverPurgeDays: 30}}, 30},
		{"negative disables", &Config{Retention: &RetentionConfig{ObserverPurgeDays: -1}}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.ObserverPurgeDaysOrZero(); got != tt.want {
				t.Errorf("ObserverPurgeDaysOrZero() = %d, want %d", got, tt.want)
			}
		})
	}
}
