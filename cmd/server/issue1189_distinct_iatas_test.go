package main

import (
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// TestQueryGroupedPacketsReturnsDistinctIATAs (#1189 R2):
// The default collapsed grouped view must already expose the DISTINCT set
// of observer IATA codes for each transmission — frontend can't compute it
// because p._children is empty until the user expands the row (or applies a
// non-default sort). Previously the cell showed a single IATA + "+N" of
// observer count, which conflates SAME-region redundancy with CROSS-region
// reception. R1 added a frontend helper but it only fired on the expanded
// view; this test gates the server-side fix.
//
// Seeds one transmission with observations from two IATAs (SJC, SFO) and
// asserts the grouped row carries distinct_iatas containing both codes.
func TestQueryGroupedPacketsReturnsDistinctIATAs(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	recent := now.Add(-1 * time.Hour)

	// Observers: SJC + SFO + a third with no IATA (should be excluded).
	observerA := mustCreateTestObserver(t, ctx, db, "obsA", "A", "SJC", now)
	observerB := mustCreateTestObserver(t, ctx, db, "obsB", "B", "SFO", now)
	observerC := mustCreateTestObserver(t, ctx, db, "obsC", "C", "", now)

	// One transmission with 3 observations (SJC, SFO, no-IATA).
	txID := mustCreateTestTransmission(t, ctx, db, testTransmission(
		"AABB", "deadbeefcafef00d", "{}", types.RouteFlood, types.PayloadADVERT, "",
	), now)
	dbtest.Observation(t, db, txID, observerA, recent, 12, -80, []string{"aa"}...)
	dbtest.Observation(t, db, txID, observerB, recent.Add(-30*time.Second), 8, -90, []string{"aa"}...)
	dbtest.Observation(t, db, txID, observerC, recent.Add(-60*time.Second), 5, -95, []string{"aa"}...)

	rows, total, err := db.QueryGroupedPackets(ctx, database.PacketQuery{Limit: 50})
	if err != nil {
		t.Fatalf("QueryGroupedPackets: %v", err)
	}
	if total != 1 {
		t.Fatalf("expected 1 grouped tx, got %d", total)
	}

	iatas := rows[0].DistinctIATAs
	sort.Strings(iatas)
	want := []string{"SFO", "SJC"}
	if strings.Join(iatas, ",") != strings.Join(want, ",") {
		t.Fatalf("distinct_iatas = %v, want %v (must exclude empty-IATA observers, dedupe)", iatas, want)
	}
}

// TestQueryGroupedPacketsDistinctIATAsEmptyWhenNoIATA (#1189 R2):
// Group whose observers all have no IATA → distinct_iatas should be empty
// (or absent / empty slice) — must NOT carry stale data from another group.
func TestQueryGroupedPacketsDistinctIATAsEmptyWhenNoIATA(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	recent := now.Add(-1 * time.Hour)

	observerID := mustCreateTestObserver(t, ctx, db, "obsX", "X", "", now)
	txID := mustCreateTestTransmission(t, ctx, db, testTransmission(
		"AA", "1111222233334444", "{}", types.RouteFlood, types.PayloadADVERT, "",
	), now)
	dbtest.Observation(t, db, txID, observerID, recent, 10, -85, nil...)

	rows, total, err := db.QueryGroupedPackets(ctx, database.PacketQuery{Limit: 50})
	if err != nil {
		t.Fatalf("QueryGroupedPackets: %v", err)
	}
	if total != 1 {
		t.Fatalf("expected 1 grouped tx, got %d", total)
	}
	iatas := rows[0].DistinctIATAs
	if len(iatas) != 0 {
		t.Fatalf("distinct_iatas should be empty for no-IATA group, got %v", iatas)
	}
}
