package database_test

import (
	"cmp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

var neighborBase = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

func upsertNeighbors(tb testing.TB, db *database.DB, edges ...database.NeighborEdgeInput) int64 {
	tb.Helper()
	written, err := db.UpsertNeighborEdges(tb.Context(), edges)
	if err != nil {
		tb.Fatal(err)
	}
	return written
}

func sortedNeighborEdges(tb testing.TB, db *database.DB) []database.NeighborEdge {
	tb.Helper()
	edges, err := db.ListNeighborEdges(tb.Context())
	if err != nil {
		tb.Fatal(err)
	}
	slices.SortFunc(edges, func(x, y database.NeighborEdge) int {
		return cmp.Or(cmp.Compare(x.NodeA, y.NodeA), cmp.Compare(x.NodeB, y.NodeB))
	})
	return edges
}

func TestUpsertNeighborEdges_AccumulatesHitsAndKeepsNewestLastSeen_DB(t *testing.T) {
	db := newDB(t)

	if n := upsertNeighbors(t, db, database.NeighborEdgeInput{NodeA: "aa", NodeB: "bb", Hits: 3, LastSeen: neighborBase}); n != 1 {
		t.Errorf("first upsert: want 1 pair written, got %d", n)
	}
	edges := sortedNeighborEdges(t, db)
	want := database.NeighborEdge{NodeA: "aa", NodeB: "bb", Count: 3, LastSeen: neighborBase}
	if len(edges) != 1 || edges[0] != want {
		t.Fatalf("want [%+v], got %+v", want, edges)
	}
	if edges[0].LastSeen.Location() != time.UTC {
		t.Errorf("want LastSeen in UTC, got %v", edges[0].LastSeen.Location())
	}

	upsertNeighbors(t, db, database.NeighborEdgeInput{NodeA: "aa", NodeB: "bb", Hits: 2, LastSeen: neighborBase.Add(time.Hour)})
	edges = sortedNeighborEdges(t, db)
	if edges[0].Count != 5 || !edges[0].LastSeen.Equal(neighborBase.Add(time.Hour)) {
		t.Errorf("after a newer upsert: want count 5 and last_seen +1h, got %+v", edges[0])
	}

	upsertNeighbors(t, db, database.NeighborEdgeInput{NodeA: "aa", NodeB: "bb", Hits: 1, LastSeen: neighborBase.Add(-time.Hour)})
	edges = sortedNeighborEdges(t, db)
	if edges[0].Count != 6 || !edges[0].LastSeen.Equal(neighborBase.Add(time.Hour)) {
		t.Errorf("an older upsert must add hits but never move last_seen back, got %+v", edges[0])
	}
}

func TestUpsertNeighborEdges_NormalizesCaseAndOrder_DB(t *testing.T) {
	db := newDB(t)

	upsertNeighbors(t, db, database.NeighborEdgeInput{NodeA: "bb", NodeB: "aa", Hits: 1, LastSeen: neighborBase})
	upsertNeighbors(t, db, database.NeighborEdgeInput{NodeA: "AA", NodeB: "BB", Hits: 2, LastSeen: neighborBase})
	upsertNeighbors(t, db, database.NeighborEdgeInput{NodeA: "Bb", NodeB: "aA", Hits: 4, LastSeen: neighborBase})

	edges := sortedNeighborEdges(t, db)
	want := database.NeighborEdge{NodeA: "aa", NodeB: "bb", Count: 7, LastSeen: neighborBase}
	if len(edges) != 1 || edges[0] != want {
		t.Errorf("want one canonical edge %+v, got %+v", want, edges)
	}
}

func TestUpsertNeighborEdges_MergesDuplicatePairsWithinOneCall_DB(t *testing.T) {
	db := newDB(t)

	written := upsertNeighbors(t, db,
		database.NeighborEdgeInput{NodeA: "aa", NodeB: "bb", Hits: 1, LastSeen: neighborBase},
		database.NeighborEdgeInput{NodeA: "BB", NodeB: "AA", Hits: 2, LastSeen: neighborBase.Add(time.Hour)},
		database.NeighborEdgeInput{NodeA: "aa", NodeB: "cc", Hits: 5, LastSeen: neighborBase},
	)

	if written != 2 {
		t.Errorf("want 2 distinct pairs written, got %d", written)
	}
	edges := sortedNeighborEdges(t, db)
	want := []database.NeighborEdge{
		{NodeA: "aa", NodeB: "bb", Count: 3, LastSeen: neighborBase.Add(time.Hour)},
		{NodeA: "aa", NodeB: "cc", Count: 5, LastSeen: neighborBase},
	}
	if !slices.Equal(edges, want) {
		t.Errorf("want %+v, got %+v", want, edges)
	}
}

func TestUpsertNeighborEdges_NothingToWrite_DB(t *testing.T) {
	db := newDB(t)

	for name, edges := range map[string][]database.NeighborEdgeInput{"nil": nil, "empty": {}} {
		written, err := db.UpsertNeighborEdges(t.Context(), edges)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if written != 0 {
			t.Errorf("%s: want 0 written, got %d", name, written)
		}
	}
	if edges := sortedNeighborEdges(t, db); len(edges) != 0 {
		t.Errorf("want no edges, got %+v", edges)
	}
}

func TestLatestNeighborEdgeTimeAndHasNeighborEdges_DB(t *testing.T) {
	db := newDB(t)

	latest, err := db.LatestNeighborEdgeTime(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !latest.IsZero() {
		t.Errorf("empty table: want the zero time, got %v", latest)
	}
	has, err := db.HasNeighborEdges(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if has {
		t.Error("empty table: want HasNeighborEdges false")
	}

	upsertNeighbors(t, db,
		database.NeighborEdgeInput{NodeA: "aa", NodeB: "bb", Hits: 1, LastSeen: neighborBase},
		database.NeighborEdgeInput{NodeA: "aa", NodeB: "cc", Hits: 1, LastSeen: neighborBase.Add(2 * time.Hour)},
		database.NeighborEdgeInput{NodeA: "bb", NodeB: "cc", Hits: 1, LastSeen: neighborBase.Add(time.Hour)},
	)

	latest, err = db.LatestNeighborEdgeTime(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !latest.Equal(neighborBase.Add(2*time.Hour)) || latest.Location() != time.UTC {
		t.Errorf("want the newest last_seen %v in UTC, got %v (%v)", neighborBase.Add(2*time.Hour), latest, latest.Location())
	}
	has, err = db.HasNeighborEdges(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Error("populated table: want HasNeighborEdges true")
	}
}

func TestPruneNeighborEdges_DeletesOnlyEdgesOlderThanCutoff_DB(t *testing.T) {
	db := newDB(t)
	upsertNeighbors(t, db,
		database.NeighborEdgeInput{NodeA: "aa", NodeB: "bb", Hits: 1, LastSeen: neighborBase.Add(-2 * time.Hour)},
		database.NeighborEdgeInput{NodeA: "aa", NodeB: "cc", Hits: 1, LastSeen: neighborBase},
		database.NeighborEdgeInput{NodeA: "bb", NodeB: "cc", Hits: 1, LastSeen: neighborBase.Add(time.Hour)},
	)

	removed, err := db.PruneNeighborEdges(t.Context(), neighborBase)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Errorf("want 1 edge removed (strictly older than the cutoff), got %d", removed)
	}
	edges := sortedNeighborEdges(t, db)
	if len(edges) != 2 || edges[0].NodeB != "cc" || edges[1].NodeA != "bb" {
		t.Errorf("want the two newer edges to remain, got %+v", edges)
	}

	removed, err = db.PruneNeighborEdges(t.Context(), neighborBase.Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if removed != 2 {
		t.Errorf("want the remaining 2 edges removed, got %d", removed)
	}
}

func TestListNeighborPairsAndNodeDegrees_DB(t *testing.T) {
	db := newDB(t)
	upsertNeighbors(t, db,
		database.NeighborEdgeInput{NodeA: "aa", NodeB: "bb", Hits: 9, LastSeen: neighborBase},
		database.NeighborEdgeInput{NodeA: "aa", NodeB: "cc", Hits: 1, LastSeen: neighborBase},
		database.NeighborEdgeInput{NodeA: "cc", NodeB: "dd", Hits: 1, LastSeen: neighborBase},
	)

	pairs, err := db.ListNeighborPairs(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(pairs, func(x, y database.NeighborPair) int {
		return cmp.Or(cmp.Compare(x.A, y.A), cmp.Compare(x.B, y.B))
	})
	wantPairs := []database.NeighborPair{{A: "aa", B: "bb"}, {A: "aa", B: "cc"}, {A: "cc", B: "dd"}}
	if !slices.Equal(pairs, wantPairs) {
		t.Errorf("want pairs %+v, got %+v", wantPairs, pairs)
	}

	degrees, err := db.ListNodeDegrees(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for _, d := range degrees {
		got[d.Pubkey] = d.Degree
	}
	wantDegrees := map[string]int{"aa": 2, "bb": 1, "cc": 2, "dd": 1}
	if len(got) != len(wantDegrees) {
		t.Fatalf("want degrees %v, got %v", wantDegrees, got)
	}
	for pubkey, degree := range wantDegrees {
		if got[pubkey] != degree {
			t.Errorf("%s: want degree %d, got %d", pubkey, degree, got[pubkey])
		}
	}
}

func TestListNeighborPairsAndNodeDegrees_EmptyTable_DB(t *testing.T) {
	db := newDB(t)

	pairs, err := db.ListNeighborPairs(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if pairs == nil || len(pairs) != 0 {
		t.Errorf("want an empty non-nil slice of pairs, got %v", pairs)
	}
	degrees, err := db.ListNodeDegrees(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if degrees == nil || len(degrees) != 0 {
		t.Errorf("want an empty non-nil slice of degrees, got %v", degrees)
	}
}

func TestListNodePublicKeys_DB(t *testing.T) {
	db := newDB(t)
	for _, pubkey := range []string{"bbbb", "aaaa"} {
		n := database.NodeInput{PublicKey: pubkey, Name: pubkey, Role: "repeater", RxTime: neighborBase, IngestNow: neighborBase}
		if err := db.CreateOrUpdateNode(t.Context(), n); err != nil {
			t.Fatal(err)
		}
	}

	keys, err := db.ListNodePublicKeys(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(keys)
	if !slices.Equal(keys, []string{"aaaa", "bbbb"}) {
		t.Errorf("want [aaaa bbbb], got %v", keys)
	}
}

type neighborObsFixture struct {
	base time.Time
	ids  []int64
}

func seedNeighborObservations(tb testing.TB, db *database.DB) neighborObsFixture {
	tb.Helper()
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	tx1 := dbtest.ChannelTx(tb, db, "scan_n1", "3A", `{"k":1}`, base)
	tx2 := dbtest.ChannelTx(tb, db, "scan_n2", "3A", `{"k":2}`, base)
	obs := dbtest.Observer(tb, db, "obsA_pub", "Obs A", "CVG", base, base)

	dbtest.Observation(tb, db, tx1, obs, base.Add(time.Minute), 7.5, -90, "a1", "b2", "c3")
	dbtest.Observation(tb, db, tx1, obs, base.Add(2*time.Minute), 3.0, -100)
	dbtest.Observation(tb, db, tx2, obs, base.Add(2*time.Minute), 1.0, -80, "d4")
	dbtest.Observation(tb, db, tx2, obs, base.Add(3*time.Minute), 2.0, -85, "e5", "f6")

	all, err := db.ScanNeighborObservations(tb.Context(), database.NeighborCursor{}, 10, nil)
	if err != nil {
		tb.Fatal(err)
	}
	ids := make([]int64, len(all))
	for i, o := range all {
		ids[i] = o.ID
	}
	return neighborObsFixture{base: base, ids: ids}
}

func TestScanNeighborObservations_FieldsAndHops_DB(t *testing.T) {
	db := newDB(t)
	f := seedNeighborObservations(t, db)

	got, err := db.ScanNeighborObservations(t.Context(), database.NeighborCursor{}, 10,
		[]types.PayloadType{5})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("want 4 observations, got %+v", got)
	}

	first := got[0]
	if first.PayloadType != 5 || first.FromPubkey != "" || first.Observer != "obsa_pub" {
		t.Errorf("want payload 5, no from_pubkey, observer obsa_pub, got %+v", first)
	}
	if strings.ReplaceAll(first.DecodedJSON, " ", "") != `{"k":1}` {
		t.Errorf("want decoded JSON for a requested payload type, got %q", first.DecodedJSON)
	}
	wantAt := f.base.Add(time.Minute)
	if !first.Timestamp.Equal(wantAt) || first.Timestamp.Location() != time.UTC {
		t.Errorf("want Timestamp %v in UTC, got %v (%v)", wantAt, first.Timestamp, first.Timestamp.Location())
	}
	if first.FirstHop == nil || *first.FirstHop != "a1" || first.LastHop == nil || *first.LastHop != "c3" {
		t.Errorf("want hops a1 .. c3, got %v .. %v", first.FirstHop, first.LastHop)
	}

	if got[1].FirstHop != nil || got[1].LastHop != nil {
		t.Errorf("an observation with no hops must have nil hops, got %v .. %v", got[1].FirstHop, got[1].LastHop)
	}
	single := got[2]
	if single.FirstHop == nil || *single.FirstHop != "d4" || single.LastHop == nil || *single.LastHop != "d4" {
		t.Errorf("a one-hop path has the same first and last hop, got %v .. %v", single.FirstHop, single.LastHop)
	}
}

func TestScanNeighborObservations_DecodedJSONOnlyForRequestedTypes_DB(t *testing.T) {
	db := newDB(t)
	seedNeighborObservations(t, db)

	cases := map[string][]types.PayloadType{
		"nil list":        nil,
		"empty list":      {},
		"other type only": {4},
	}
	for name, jsonTypes := range cases {
		got, err := db.ScanNeighborObservations(t.Context(), database.NeighborCursor{}, 10, jsonTypes)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(got) != 4 {
			t.Fatalf("%s: want 4 observations, got %d", name, len(got))
		}
		for _, o := range got {
			if o.DecodedJSON != "" {
				t.Errorf("%s: want no decoded JSON, got %q", name, o.DecodedJSON)
			}
		}
	}
}

func TestScanNeighborObservations_CursorPagingSkipsAndRepeatsNothing_DB(t *testing.T) {
	db := newDB(t)
	f := seedNeighborObservations(t, db)

	for _, pageSize := range []int{1, 2, 3, 10} {
		var seen []int64
		cursor := database.NeighborCursor{}
		for range 20 {
			page, err := db.ScanNeighborObservations(t.Context(), cursor, pageSize, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(page) == 0 {
				break
			}
			for _, o := range page {
				seen = append(seen, o.ID)
			}
			cursor = page[len(page)-1].Cursor()
		}
		if !slices.Equal(seen, f.ids) {
			t.Errorf("page size %d: want %v (two observations share a timestamp), got %v", pageSize, f.ids, seen)
		}
	}
}

func TestScanNeighborObservations_CursorBoundary_DB(t *testing.T) {
	db := newDB(t)
	f := seedNeighborObservations(t, db)

	at := database.NeighborCursor{Timestamp: f.base.Add(2 * time.Minute)}
	got, err := db.ScanNeighborObservations(t.Context(), at, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].ID != f.ids[1] {
		t.Errorf("a cursor with ID 0 must include the observations at its timestamp, got %+v", got)
	}

	exact := database.NeighborCursor{Timestamp: f.base.Add(2 * time.Minute), ID: f.ids[1]}
	got, err = db.ScanNeighborObservations(t.Context(), exact, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != f.ids[2] {
		t.Errorf("a full cursor is exclusive of its own row but keeps same-timestamp successors, got %+v", got)
	}

	end := database.NeighborCursor{Timestamp: f.base.Add(3 * time.Minute), ID: f.ids[3]}
	got, err = db.ScanNeighborObservations(t.Context(), end, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || len(got) != 0 {
		t.Errorf("past the last row: want an empty non-nil slice, got %v", got)
	}
}

func TestScanNeighborObservations_LimitEdges_DB(t *testing.T) {
	db := newDB(t)
	seedNeighborObservations(t, db)

	zero, err := db.ScanNeighborObservations(t.Context(), database.NeighborCursor{}, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(zero) != 0 {
		t.Errorf("zero limit: want nothing, got %+v", zero)
	}

	negative, err := db.ScanNeighborObservations(t.Context(), database.NeighborCursor{}, -1, nil)
	if err == nil {
		t.Fatal("want an error for a negative limit")
	}
	if negative != nil {
		t.Errorf("want nil on error, got %+v", negative)
	}
}

func TestCursorAfterTime_ExcludesObservationsAtThatInstant_DB(t *testing.T) {
	db := newDB(t)
	f := seedNeighborObservations(t, db)

	at := f.base.Add(2 * time.Minute)
	after, err := db.ScanNeighborObservations(t.Context(), database.CursorAfterTime(at), 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 1 || after[0].ID != f.ids[3] {
		t.Errorf("want only the observation after %v, got %+v", at, after)
	}

	atOrAfter, err := db.ScanNeighborObservations(t.Context(), database.NeighborCursor{Timestamp: at}, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(atOrAfter) != 3 {
		t.Errorf("a cursor with ID 0 is at-or-after, want 3, got %d", len(atOrAfter))
	}

	all, err := db.ScanNeighborObservations(t.Context(), database.CursorAfterTime(time.Time{}), 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 4 {
		t.Errorf("CursorAfterTime of the zero time must scan everything, want 4, got %d", len(all))
	}
}
