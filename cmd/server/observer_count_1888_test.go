package main

import (
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
)

// #1888: /api/stats.totalObservers and /api/observers disagreed because they
// counted different sets. GetObservers() (and therefore the Observers page)
// returns only rows the retention sweep has not soft-deleted
// (inactive IS NULL OR inactive = 0), while the store's stats query counted
// every row in the table. On a deployment with observerDays retention the two
// drift apart by exactly the number of soft-deleted observers — 79 vs 51 on the
// instance this was reproduced against.
//
// The two stats implementations did not even agree with each other: the DB
// fallback (DB.GetStats) already applied the inactive filter, the store path
// did not.

// seedObserversForCount inserts 4 live observers and 1 soft-deleted one.
func seedObserversForCount(t *testing.T, db *database.DB) {
	t.Helper()
	ctx := t.Context()
	now := time.Now().UTC()

	for _, id := range []string{"live1", "live2", "live3", "nullflag"} {
		if _, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
			ID:        id,
			Name:      ptr("Observer " + id),
			LastSeen:  now,
			IngestNow: now,
		}); err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
	}

	if _, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        "gone1",
		Name:      ptr("Gone One"),
		LastSeen:  now,
		IngestNow: now,
	}); err != nil {
		t.Fatalf("insert gone1: %v", err)
	}
	if err := db.MarkObserverInactive(ctx, "gone1"); err != nil {
		t.Fatalf("soft-delete gone1: %v", err)
	}
}

func TestStoreStatsTotalObserversExcludesSoftDeleted(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)

	seedObserversForCount(t, db)

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	st, err := store.GetStoreStats(ctx)
	if err != nil {
		t.Fatalf("GetStoreStats: %v", err)
	}
	if st.TotalObservers != 4 {
		t.Errorf("TotalObservers = %d, want 4 (3 live + 1 NULL flag, excluding 1 soft-deleted)", st.TotalObservers)
	}
}

// The count must equal what the Observers page actually lists — that is the
// whole point of the issue: three UI surfaces showing three numbers.
func TestStoreStatsTotalObserversMatchesObserverList(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)
	seedObserversForCount(t, db)

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	st, err := store.GetStoreStats(ctx)
	if err != nil {
		t.Fatalf("GetStoreStats: %v", err)
	}
	observers, err := db.GetObservers(ctx)
	if err != nil {
		t.Fatalf("GetObservers: %v", err)
	}
	if st.TotalObservers != len(observers) {
		t.Errorf("stats totalObservers = %d but /api/observers lists %d",
			st.TotalObservers, len(observers))
	}
}

// The store path and the DB fallback must not report different totals for the
// same database.
func TestStoreAndDBStatsAgreeOnTotalObservers(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)
	seedObserversForCount(t, db)

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	storeStats, err := store.GetStoreStats(ctx)
	if err != nil {
		t.Fatalf("GetStoreStats: %v", err)
	}
	dbStats, err := db.GetStats(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if storeStats.TotalObservers != dbStats.TotalObservers {
		t.Errorf("store path reports %d observers, DB fallback reports %d",
			storeStats.TotalObservers, dbStats.TotalObservers)
	}
}
