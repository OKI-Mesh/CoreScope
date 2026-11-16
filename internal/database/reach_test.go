package database_test

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
)

var reachBase = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

func seedReachPaths(tb testing.TB, db *database.DB) {
	tb.Helper()
	obs := dbtest.Observer(tb, db, "obsa", "Obs A", "CVG", reachBase, reachBase)
	one := dbtest.ChannelTx(tb, db, "reach_a", "3A", `{"k":1}`, reachBase)
	two := dbtest.ChannelTx(tb, db, "reach_b", "3A", `{"k":2}`, reachBase)
	three := dbtest.ChannelTx(tb, db, "reach_c", "3A", `{"k":3}`, reachBase)
	dbtest.Observation(tb, db, one, obs, reachBase.Add(time.Minute), 7.5, -90, "aa", "bb", "cc")
	dbtest.Observation(tb, db, two, obs, reachBase.Add(2*time.Minute), 6.0, -95, "dd", "bb")
	dbtest.Observation(tb, db, three, obs, reachBase.Add(3*time.Minute), 5.0, -99, "ee")
}

func joinedPaths(paths []database.ObservedPath) []string {
	out := make([]string, len(paths))
	for i, p := range paths {
		out[i] = strings.Join(p.Path, ",")
	}
	slices.Sort(out)
	return out
}

func TestListObservedPathsThrough_MatchesAnyHopAndReturnsFullPath_DB(t *testing.T) {
	db := newDB(t)
	seedReachPaths(t, db)

	got, err := db.ListObservedPathsThrough(t.Context(), []string{"bb"}, reachBase, 10)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"aa,bb,cc", "dd,bb"}; !slices.Equal(joinedPaths(got), want) {
		t.Fatalf("want the two paths through bb in full, got %v", joinedPaths(got))
	}
	for _, p := range got {
		if p.ObserverPK != "obsa" || p.FromPubkey != "" || p.PayloadType != 5 || p.SNR == nil {
			t.Errorf("want observer obsa, no sender, payload type 5 and an SNR, got %+v", p)
		}
	}

	got, err = db.ListObservedPathsThrough(t.Context(), []string{"AA", "Ee"}, reachBase, 10)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"aa,bb,cc", "ee"}; !slices.Equal(joinedPaths(got), want) {
		t.Errorf("several tokens, matched case-insensitively: want %v, got %v", want, joinedPaths(got))
	}
}

func TestListObservedPathsThrough_SinceExcludesOlderObservations_DB(t *testing.T) {
	db := newDB(t)
	seedReachPaths(t, db)

	got, err := db.ListObservedPathsThrough(t.Context(), []string{"bb"}, reachBase.Add(90*time.Second), 10)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"dd,bb"}; !slices.Equal(joinedPaths(got), want) {
		t.Errorf("want only the observation after since, got %v", joinedPaths(got))
	}
}

func TestListObservedPathsThrough_LimitKeepsTheNewestRows_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	seedReachPaths(t, db)

	for limit, want := range map[int][]string{
		1:  {"ee"},
		2:  {"dd,bb", "ee"},
		3:  {"aa,bb,cc", "dd,bb", "ee"},
		10: {"aa,bb,cc", "dd,bb", "ee"},
	} {
		got, err := db.ListObservedPathsThrough(ctx, []string{"bb", "ee"}, reachBase, limit)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(joinedPaths(got), want) {
			t.Errorf("limit %d: want the newest rows %v, got %v", limit, want, joinedPaths(got))
		}
	}

	if _, err := db.ListObservedPathsThrough(ctx, []string{"bb"}, reachBase, -1); err == nil {
		t.Error("a negative limit must be an error")
	}

	for name, tokens := range map[string][]string{"nil": nil, "empty": {}} {
		got, err := db.ListObservedPathsThrough(ctx, tokens, reachBase, 10)
		if err != nil || got == nil || len(got) != 0 {
			t.Errorf("%s tokens: want an empty non-nil slice, got %v (%v)", name, got, err)
		}
	}
}
