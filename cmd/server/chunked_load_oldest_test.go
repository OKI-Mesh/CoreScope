package main

// Regression for PR #1596 (issue #1009) chunked load: when transmission
// ids are anti-correlated with first_seen (e.g. id=1 has the NEWEST
// timestamp), LoadChunked walks id-ASC and the post-load
// `s.oldestLoaded = s.packets[0].FirstSeen` line set oldestLoaded to
// the NEWEST first_seen. QueryPackets then mis-routed any
// `since>=oldestLoaded` query to the SQL fallback, hiding fresh
// in-memory rows. This shows up in real life on the e2e fixture after
// tools/freshen-fixture.sh shifts timestamps so id=1 (originally
// loaded first) carries the most recent first_seen.
//
// The mobile e2e test test-observer-iata-1188-e2e.js fails as a
// result: with the default 15-minute time window, /api/packets returns
// 0 rows and the mobile DOM has no `tr[data-hash]` to tap.
//
// This test asserts the in-memory invariant: after LoadChunked,
// oldestLoaded must equal the actual oldest FirstSeen across loaded
// transmissions, not the FirstSeen of the first row in s.packets.

import (
	"fmt"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
	"github.com/OKI-Mesh/CoreScope/internal/database/pgtest"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// createTestDBReverseTime returns the connection string of a fresh migrated
// database with numTx transmissions whose ids run 1..numTx ascending while
// first_seen runs newest..oldest (id 1 = newest). Insertion order by id is the
// reverse of time order: this proves Load() sorts by time, not by id.
func createTestDBReverseTime(tb testing.TB, numTx int) string {
	tb.Helper()
	dsn := pgtest.NewDatabase(tb)

	db, err := database.OpenReadWrite(tb.Context(), dsn)
	if err != nil {
		tb.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC().Truncate(time.Second)
	observerID := dbtest.EnsureObserver(tb, db, "obs1")
	for i := 1; i <= numTx; i++ {
		dbtest.TxWithObservation(tb, db, observerID, database.TransmissionInput{
			RawHex:         "aabb",
			Hash:           fmt.Sprintf("h%04d", i),
			RouteType:      types.RouteType(0),
			PayloadType:    types.PayloadType(4),
			PayloadVersion: 1,
			DecodedJSON:    fmt.Sprintf(`{"pubKey":"pk%04d"}`, i),
		}, now.Add(-time.Duration(i-1)*time.Minute), -10.0, -80.0, "aa", "bb")
	}
	return dsn
}
func openReverseTimeStore(t *testing.T, numTx int) *PacketStore {
	t.Helper()
	dsn := createTestDBReverseTime(t, numTx) // ids ascend while first_seen descends

	db, err := OpenDB(t.Context(), dsn)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return NewPacketStore(db, &PacketStoreConfig{})
}

// TestLoadChunked_OldestLoadedIsActualOldest: when LoadChunked walks
// transmissions in id-ASC order but timestamps are anti-correlated
// with id (PR #1596 regression scenario), oldestLoaded MUST be the
// minimum FirstSeen across loaded packets, not the first row's
// FirstSeen. Otherwise QueryPackets routes "since=15min ago" to SQL
// fallback, hiding fresh rows.
func TestLoadChunked_OldestLoadedIsActualOldest(t *testing.T) {
	ctx := t.Context()
	store := openReverseTimeStore(t, 50)

	if err := store.LoadChunked(ctx, 20); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}

	// Compute the actual oldest first_seen across what got loaded.
	if len(store.packets) == 0 {
		t.Fatal("no packets loaded")
	}
	actualOldest := store.packets[0].FirstSeen
	for _, p := range store.packets {
		if p.FirstSeen.Before(actualOldest) {
			actualOldest = p.FirstSeen
		}
	}

	if store.oldestLoaded != actualOldest {
		t.Fatalf("oldestLoaded=%q must equal actual MIN(FirstSeen)=%q "+
			"(id-ordered chunk walk with anti-correlated timestamps "+
			"left oldestLoaded pointing at the newest row, which makes "+
			"QueryPackets mis-route since-windowed queries to SQL fallback "+
			"and the mobile e2e test renders 0 rows)",
			store.oldestLoaded, actualOldest)
	}
}

// TestLoadChunked_PacketsSortedByFirstSeenASC: QueryPackets and
// GetTimestamps both assume s.packets is "sorted oldest-first" (see
// store.go:2125 comment on GetTimestamps). LoadChunked walks rows
// id-ASC which only equals first_seen-ASC when ids and timestamps
// are correlated — not true after fixture freshen, not true after
// any out-of-order ingest. Assert the invariant directly.
func TestLoadChunked_PacketsSortedByFirstSeenASC(t *testing.T) {
	ctx := t.Context()
	store := openReverseTimeStore(t, 25)

	if err := store.LoadChunked(ctx, 10); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}
	for i := 1; i < len(store.packets); i++ {
		if store.packets[i-1].FirstSeen.After(store.packets[i].FirstSeen) {
			t.Fatalf("s.packets must be sorted by FirstSeen ASC; "+
				"packets[%d].FirstSeen=%q > packets[%d].FirstSeen=%q",
				i-1, store.packets[i-1].FirstSeen,
				i, store.packets[i].FirstSeen)
		}
	}
}
