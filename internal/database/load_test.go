package database_test

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
)

func ptrTo[T any](v T) *T { return &v }

func seedLoadTransmissions(tb testing.TB, db *database.DB, base time.Time) []int64 {
	tb.Helper()
	ids := make([]int64, 4)
	for i := range ids {
		ids[i] = dbtest.ChannelTx(tb, db,
			fmt.Sprintf("load_hash_%d", i), "3A", fmt.Sprintf(`{"n":%d}`, i),
			base.Add(time.Duration(i)*time.Hour))
	}
	return ids
}

func loadedIDs(txs []database.LoadedTransmission) []int64 {
	ids := make([]int64, len(txs))
	for i, tx := range txs {
		ids[i] = tx.ID
	}
	return ids
}

type loadFixture struct {
	base time.Time
	txs  []int64
}

func seedLoadObservations(tb testing.TB, db *database.DB) loadFixture {
	tb.Helper()
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	txs := seedLoadTransmissions(tb, db, base)

	for _, n := range []database.NodeInput{
		{PublicKey: "aaaa", Name: "Hop A", Role: "repeater", RxTime: base, IngestNow: base},
		{PublicKey: "cccc", Name: "Hop C", Role: "repeater", RxTime: base, IngestNow: base},
	} {
		if err := db.CreateOrUpdateNode(tb.Context(), n); err != nil {
			tb.Fatal(err)
		}
	}

	obsA := dbtest.Observer(tb, db, "obsa_pub", "Obs A", "CVG", base, base)
	obsB := dbtest.Observer(tb, db, "obsb_pub", "Obs B", "DAY", base, base)

	dbtest.Observation(tb, db, txs[0], obsA, base.Add(time.Minute), 7.5, -90, "a1:aaaa", "b2")
	dbtest.Observation(tb, db, txs[0], obsB, base.Add(2*time.Minute), 3.0, -100)
	dbtest.Observation(tb, db, txs[1], obsA, base.Add(3*time.Minute), 1.0, -80, "c3:cccc")
	return loadFixture{base: base, txs: txs}
}

func TestListTransmissionsChunk_PagingAndBounds_DB(t *testing.T) {
	db := newDB(t)
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	ids := seedLoadTransmissions(t, db, base)

	at := func(h int) *time.Time { return ptr(base.Add(time.Duration(h) * time.Hour)) }
	cases := []struct {
		name string
		q    database.ChunkQuery
		want []int64
	}{
		{"everything in id order", database.ChunkQuery{Limit: 10}, ids},
		{"after id is exclusive", database.ChunkQuery{AfterID: ids[1], Limit: 10}, ids[2:]},
		{"limit caps from the start", database.ChunkQuery{Limit: 2}, ids[:2]},
		{"since is inclusive", database.ChunkQuery{Since: at(1), Limit: 10}, ids[1:]},
		{"until is exclusive", database.ChunkQuery{Until: at(3), Limit: 10}, ids[:3]},
		{"since and until together", database.ChunkQuery{Since: at(1), Until: at(3), Limit: 10}, ids[1:3]},
		{"after id with limit", database.ChunkQuery{AfterID: ids[0], Limit: 2}, ids[1:3]},
		{"zero limit returns nothing", database.ChunkQuery{Limit: 0}, []int64{}},
		{"after the last id", database.ChunkQuery{AfterID: ids[3], Limit: 10}, []int64{}},
	}
	for _, c := range cases {
		got, err := db.ListTransmissionsChunk(t.Context(), c.q)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got == nil {
			t.Errorf("%s: want an empty slice, not nil", c.name)
		}
		if !slices.Equal(loadedIDs(got), c.want) {
			t.Errorf("%s: want ids %v, got %v", c.name, c.want, loadedIDs(got))
		}
	}
}

func TestListTransmissionsChunk_NegativeLimitIsAnError_DB(t *testing.T) {
	db := newDB(t)
	got, err := db.ListTransmissionsChunk(t.Context(), database.ChunkQuery{Limit: -1})
	if err == nil {
		t.Fatal("want an error for a negative limit")
	}
	if got != nil {
		t.Errorf("want nil on error, got %+v", got)
	}
}

func TestListTransmissionsChunk_Fields_DB(t *testing.T) {
	db := newDB(t)
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	ids := seedLoadTransmissions(t, db, base)

	got, err := db.ListTransmissionsChunk(t.Context(), database.ChunkQuery{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 transmission, got %+v", got)
	}
	tx := got[0]

	if tx.ID != ids[0] || tx.Hash != "load_hash_0" {
		t.Errorf("want id %d and hash load_hash_0, got %d %q", ids[0], tx.ID, tx.Hash)
	}
	if tx.RawHex != "AABB" {
		t.Errorf("want RawHex AABB, got %q", tx.RawHex)
	}
	if !tx.FirstSeen.Equal(base) || tx.FirstSeen.Location() != time.UTC {
		t.Errorf("want FirstSeen %v in UTC, got %v (%v)", base, tx.FirstSeen, tx.FirstSeen.Location())
	}
	if tx.RouteType == nil || *tx.RouteType != 1 {
		t.Errorf("want RouteType 1, got %v", tx.RouteType)
	}
	if tx.PayloadType == nil || *tx.PayloadType != 5 {
		t.Errorf("want PayloadType 5, got %v", tx.PayloadType)
	}
	if strings.ReplaceAll(tx.DecodedJSON, " ", "") != `{"n":0}` {
		t.Errorf("want DecodedJSON {\"n\":0}, got %q", tx.DecodedJSON)
	}
	if tx.ScopeName != nil {
		t.Errorf("want nil ScopeName for an unscoped transmission, got %q", *tx.ScopeName)
	}
}

func TestListTransmissionsChunk_BoundsUseLastSeenWhenSet_DB(t *testing.T) {
	db := newDB(t)
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	ids := seedLoadTransmissions(t, db, base)

	if _, err := db.Pool().Exec(t.Context(), `UPDATE transmissions SET last_seen = first_seen`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool().Exec(t.Context(),
		`UPDATE transmissions SET last_seen = $1 WHERE id = $2`, base.Add(5*time.Hour), ids[0]); err != nil {
		t.Fatal(err)
	}

	since := base.Add(4 * time.Hour)
	got, err := db.ListTransmissionsChunk(t.Context(), database.ChunkQuery{Since: &since, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(loadedIDs(got), []int64{ids[0]}) {
		t.Errorf("since %v: want only the recently re-heard transmission %d, got %v", since, ids[0], loadedIDs(got))
	}

	until := base.Add(time.Hour)
	got, err = db.ListTransmissionsChunk(t.Context(), database.ChunkQuery{Until: &until, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("until %v: the re-heard transmission is no longer old enough, want none, got %v", until, loadedIDs(got))
	}

	count, err := db.CountTransmissionsSince(t.Context(), &since)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("CountTransmissionsSince must use the same last-activity rule, want 1, got %d", count)
	}
}

func TestCountTransmissionsSince_MatchesChunkBounds_DB(t *testing.T) {
	db := newDB(t)
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	seedLoadTransmissions(t, db, base)

	cutoffs := map[string]*time.Time{
		"nil counts all":        nil,
		"oldest is inclusive":   &base,
		"middle":                ptr(base.Add(2 * time.Hour)),
		"after the newest":      ptr(base.Add(24 * time.Hour)),
		"just after the oldest": ptr(base.Add(time.Microsecond)),
	}
	for name, since := range cutoffs {
		count, err := db.CountTransmissionsSince(t.Context(), since)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		chunk, err := db.ListTransmissionsChunk(t.Context(), database.ChunkQuery{Since: since, Limit: 100})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if count != int64(len(chunk)) {
			t.Errorf("%s: count %d does not match the %d transmissions the chunk returns", name, count, len(chunk))
		}
	}

	all, err := db.CountTransmissionsSince(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if all != 4 {
		t.Errorf("nil since: want 4, got %d", all)
	}
}

func TestListObservationsForTransmissions_GroupsNewestFirstWithPaths_DB(t *testing.T) {
	db := newDB(t)
	f := seedLoadObservations(t, db)

	got, err := db.ListObservationsForTransmissions(t.Context(), f.txs[:3])
	if err != nil {
		t.Fatal(err)
	}

	if len(got) != 2 {
		t.Fatalf("want entries for 2 transmissions (the third has no observations), got %v", got)
	}
	if _, ok := got[f.txs[2]]; ok {
		t.Errorf("a transmission without observations must be absent from the map")
	}

	first := got[f.txs[0]]
	if len(first) != 2 {
		t.Fatalf("want 2 observations for the first transmission, got %+v", first)
	}
	if first[0].ObserverID != "obsb_pub" || first[1].ObserverID != "obsa_pub" {
		t.Errorf("want newest first (obsb then obsa), got %q then %q", first[0].ObserverID, first[1].ObserverID)
	}

	newest, older := first[0], first[1]
	if newest.ObserverName != "Obs B" || newest.ObserverIATA != "DAY" {
		t.Errorf("want observer Obs B / DAY, got %q / %q", newest.ObserverName, newest.ObserverIATA)
	}
	if len(newest.Path) != 0 {
		t.Errorf("want an empty path, got %+v", newest.Path)
	}
	if newest.SNR == nil || *newest.SNR != 3.0 || newest.RSSI == nil || *newest.RSSI != -100 {
		t.Errorf("want SNR 3.0 and RSSI -100, got %v and %v", newest.SNR, newest.RSSI)
	}

	wantResolved := "aaaa"
	wantPath := []database.PathHop{
		{Prefix: "a1", ResolvedPubkey: &wantResolved},
		{Prefix: "b2", ResolvedPubkey: nil},
	}
	if !reflect.DeepEqual(older.Path, wantPath) {
		t.Errorf("want path %+v, got %+v", wantPath, older.Path)
	}
	wantAt := f.base.Add(time.Minute)
	if !older.Timestamp.Equal(wantAt) || older.Timestamp.Location() != time.UTC {
		t.Errorf("want Timestamp %v in UTC, got %v (%v)", wantAt, older.Timestamp, older.Timestamp.Location())
	}
}

func TestListObservationsForTransmissions_NoIDs_DB(t *testing.T) {
	db := newDB(t)
	seedLoadObservations(t, db)

	for name, ids := range map[string][]int64{"nil": nil, "empty": {}, "unknown": {999999}} {
		got, err := db.ListObservationsForTransmissions(t.Context(), ids)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got == nil || len(got) != 0 {
			t.Errorf("%s: want an empty non-nil map, got %v", name, got)
		}
	}
}

func TestListObservationsAfterID_OldestFirstPagedAndExclusive_DB(t *testing.T) {
	db := newDB(t)
	f := seedLoadObservations(t, db)

	all, err := db.ListObservationsAfterID(t.Context(), 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("want 3 observations, got %+v", all)
	}
	for i := 1; i < len(all); i++ {
		if all[i].ID <= all[i-1].ID {
			t.Errorf("want ascending ids, got %d then %d", all[i-1].ID, all[i].ID)
		}
	}
	wantTx := []int64{f.txs[0], f.txs[0], f.txs[1]}
	for i, o := range all {
		if o.TransmissionID != wantTx[i] {
			t.Errorf("observation %d: want transmission %d, got %d", i, wantTx[i], o.TransmissionID)
		}
	}

	rest, err := db.ListObservationsAfterID(t.Context(), all[0].ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 2 || rest[0].ID != all[1].ID {
		t.Errorf("after the first id: want the other 2 observations, got %+v", rest)
	}

	capped, err := db.ListObservationsAfterID(t.Context(), 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(capped) != 1 || capped[0].ID != all[0].ID {
		t.Errorf("limit 1: want only the oldest, got %+v", capped)
	}

	none, err := db.ListObservationsAfterID(t.Context(), all[2].ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if none == nil || len(none) != 0 {
		t.Errorf("after the last id: want an empty non-nil slice, got %v", none)
	}

	zero, err := db.ListObservationsAfterID(t.Context(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(zero) != 0 {
		t.Errorf("zero limit: want nothing, got %+v", zero)
	}
}

func TestListObservationsAfterID_NegativeLimitIsAnError_DB(t *testing.T) {
	db := newDB(t)
	if _, err := db.ListObservationsAfterID(t.Context(), 0, -1); err == nil {
		t.Fatal("want an error for a negative limit")
	}
}

func TestLoadedObservations_BothListingsAgree_DB(t *testing.T) {
	db := newDB(t)
	f := seedLoadObservations(t, db)

	byTx, err := db.ListObservationsForTransmissions(t.Context(), f.txs)
	if err != nil {
		t.Fatal(err)
	}
	after, err := db.ListObservationsAfterID(t.Context(), 0, 10)
	if err != nil {
		t.Fatal(err)
	}

	for _, n := range after {
		var match *database.Observation
		for i := range byTx[n.TransmissionID] {
			if byTx[n.TransmissionID][i].ID == n.ID {
				match = &byTx[n.TransmissionID][i]
			}
		}
		if match == nil {
			t.Fatalf("observation %d missing from the per-transmission listing", n.ID)
		}
		if !reflect.DeepEqual(*match, n) {
			t.Errorf("observation %d differs between listings:\n per-transmission %+v\n after-id         %+v", n.ID, *match, n)
		}
	}
}
