package database_test

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

func TestListGroupTextTransmissions_PagesByID_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()

	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	ids := make([]int64, 5)
	for i := range ids {
		tx := dbtest.ChannelTx(t, db, fmt.Sprintf("page_hash_%d", i), "#page",
			fmt.Sprintf(`{"type":"CHAN","channel":"#page","text":"S%d: hi","sender":"S%d"}`, i, i),
			base.Add(time.Duration(i)*time.Minute))
		ids[i] = tx
	}

	first, err := db.ListGroupTextTransmissions(ctx, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || first[0].ID != ids[0] || first[1].ID != ids[1] {
		t.Fatalf("page 1: want ids [%d %d], got %+v", ids[0], ids[1], first)
	}
	if first[0].Hash != "page_hash_0" || first[1].Hash != "page_hash_1" {
		t.Errorf("page 1: want hashes page_hash_0, page_hash_1, got %q, %q", first[0].Hash, first[1].Hash)
	}
	if got := first[0].Frame.Hex(); got != "AABB" {
		t.Errorf("page 1: want frame AABB, got %s", got)
	}

	second, err := db.ListGroupTextTransmissions(ctx, first[1].ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 2 || second[0].ID != ids[2] || second[1].ID != ids[3] {
		t.Fatalf("page 2: want ids [%d %d], got %+v", ids[2], ids[3], second)
	}

	third, err := db.ListGroupTextTransmissions(ctx, second[1].ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(third) != 1 || third[0].ID != ids[4] {
		t.Fatalf("page 3: want id [%d], got %+v", ids[4], third)
	}

	fourth, err := db.ListGroupTextTransmissions(ctx, third[0].ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(fourth) != 0 {
		t.Fatalf("page 4: want empty, got %+v", fourth)
	}
}

func TestListGroupTextTransmissions_Empty_DB(t *testing.T) {
	db := newDB(t)

	got, err := db.ListGroupTextTransmissions(t.Context(), 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("want no transmissions, got %+v", got)
	}
}

func TestQueryMultiNodePackets(t *testing.T) {
	ctx := t.Context()
	db := newDB(t)

	base := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	obsIdx := dbtest.Observer(t, db, "obsa", "Obs A", "SEA", base, base)

	const (
		nodeA = "aabbccdd11223344"
		nodeB = "eeff00112233aabb"
		nodeC = "0011223344556677"
	)
	seed := func(hash, from string, at time.Time) int64 {
		return dbtest.TxWithObservation(t, db, obsIdx, database.TransmissionInput{
			RawHex:      "0180" + hash,
			Hash:        hash,
			RouteType:   types.RouteType(1),
			PayloadType: types.PayloadType(4),
			DecodedJSON: `{}`,
			Advert:      &database.NodeInput{PublicKey: from, Name: "node-" + from, Role: "repeater", RxTime: at, IngestNow: at},
		}, at, 10.0, -90)
	}

	// A: three packets one hour apart; B: two; C: one.
	a1 := seed("a1", nodeA, base)
	a2 := seed("a2", nodeA, base.Add(1*time.Hour))
	a3 := seed("a3", nodeA, base.Add(2*time.Hour))
	b1 := seed("b1", nodeB, base.Add(3*time.Hour))
	b2 := seed("b2", nodeB, base.Add(4*time.Hour))
	seed("c1", nodeC, base.Add(5*time.Hour))

	ids := func(rows []database.Packet) []int64 {
		out := make([]int64, len(rows))
		for i, r := range rows {
			out[i] = r.ID
		}
		return out
	}

	t.Run("empty pubkeys", func(t *testing.T) {
		rows, total, err := db.QueryMultiNodePackets(ctx, database.MultiNodePacketParams{})
		if err != nil {
			t.Fatal(err)
		}
		if total != 0 || len(rows) != 0 {
			t.Errorf("got total=%d rows=%d, want 0/0", total, len(rows))
		}
	})

	t.Run("single pubkey, newest first by default", func(t *testing.T) {
		rows, total, err := db.QueryMultiNodePackets(ctx, database.MultiNodePacketParams{
			Pubkeys: []string{nodeA},
		})
		if err != nil {
			t.Fatal(err)
		}
		if total != 3 {
			t.Errorf("total = %d, want 3", total)
		}
		if got, want := ids(rows), []int64{a3, a2, a1}; !slices.Equal(got, want) {
			t.Errorf("ids = %v, want %v (DESC by id)", got, want)
		}
	})

	t.Run("ascending order", func(t *testing.T) {
		rows, _, err := db.QueryMultiNodePackets(ctx, database.MultiNodePacketParams{
			Pubkeys: []string{nodeA}, Asc: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if got, want := ids(rows), []int64{a1, a2, a3}; !slices.Equal(got, want) {
			t.Errorf("ids = %v, want %v", got, want)
		}
	})

	t.Run("multiple pubkeys", func(t *testing.T) {
		rows, total, err := db.QueryMultiNodePackets(ctx, database.MultiNodePacketParams{
			Pubkeys: []string{nodeA, nodeB},
		})
		if err != nil {
			t.Fatal(err)
		}
		if total != 5 {
			t.Errorf("total = %d, want 5", total)
		}
		if got, want := ids(rows), []int64{b2, b1, a3, a2, a1}; !slices.Equal(got, want) {
			t.Errorf("ids = %v, want %v", got, want)
		}
	})

	t.Run("time filters are inclusive", func(t *testing.T) {
		since := base.Add(1 * time.Hour) // a2's first_seen exactly
		until := base.Add(2 * time.Hour) // a3's first_seen exactly
		rows, total, err := db.QueryMultiNodePackets(ctx, database.MultiNodePacketParams{
			Pubkeys: []string{nodeA}, Asc: true, Since: &since, Until: &until,
		})
		if err != nil {
			t.Fatal(err)
		}
		if total != 2 {
			t.Errorf("total = %d, want 2", total)
		}
		if got, want := ids(rows), []int64{a2, a3}; !slices.Equal(got, want) {
			t.Errorf("ids = %v, want %v (both bounds inclusive)", got, want)
		}
	})

	t.Run("paging keeps the full total", func(t *testing.T) {
		rows, total, err := db.QueryMultiNodePackets(ctx, database.MultiNodePacketParams{
			Pubkeys: []string{nodeA}, Limit: 2, Offset: 2,
		})
		if err != nil {
			t.Fatal(err)
		}
		if total != 3 {
			t.Errorf("total = %d, want 3 (count ignores limit/offset)", total)
		}
		if got, want := ids(rows), []int64{a1}; !slices.Equal(got, want) {
			t.Errorf("ids = %v, want %v", got, want)
		}
	})

	t.Run("no match", func(t *testing.T) {
		rows, total, err := db.QueryMultiNodePackets(ctx, database.MultiNodePacketParams{
			Pubkeys: []string{"ffffffffffffffff"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if total != 0 || len(rows) != 0 {
			t.Errorf("got total=%d rows=%d, want 0/0", total, len(rows))
		}
	})
}

func TestGetMaxTransmissionID(t *testing.T) {
	ctx := t.Context()
	db := newDB(t)

	t.Run("empty table is zero", func(t *testing.T) {
		got, err := db.GetMaxTransmissionID(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got != 0 {
			t.Errorf("got %d, want 0", got)
		}
	})

	t.Run("returns the latest id", func(t *testing.T) {
		now := time.Now().UTC()
		obsIdx := dbtest.Observer(t, db, "obsa", "Obs A", "SEA", now, now)
		var last int64
		for _, hash := range []string{"AA01", "AA02", "AA03"} {
			last = dbtest.TxWithObservation(t, db, obsIdx, database.TransmissionInput{
				RawHex: hash, Hash: hash,
				RouteType: types.RouteType(1), PayloadType: types.PayloadType(4),
				DecodedJSON: `{}`,
			}, now, 10.0, -90)
		}

		got, err := db.GetMaxTransmissionID(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got != last {
			t.Errorf("got %d, want %d (the last inserted id)", got, last)
		}
	})
}

func TestListGroupTextTransmissions_ExcludesOtherPayloadTypes_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()

	seen := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	chat := dbtest.ChannelTx(t, db, "kind_hash_chat", "#kind",
		`{"type":"CHAN","channel":"#kind","text":"Alpha: hi","sender":"Alpha"}`, seen)

	_, isNew, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		RawHex:      "CCDD",
		Hash:        "kind_hash_other",
		RouteType:   types.RouteType(1),
		PayloadType: types.PayloadType(4),
		DecodedJSON: `{"type":"ADVERT"}`,
		ChannelHash: "",
	}, seen)
	if err != nil {
		t.Fatal(err)
	}
	if !isNew {
		t.Fatal("kind_hash_other already existed")
	}

	got, err := db.ListGroupTextTransmissions(ctx, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != chat || got[0].Hash != "kind_hash_chat" {
		t.Fatalf("want only the GRP_TXT transmission, got %+v", got)
	}
}

var lookupBase = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

func TestGetPacketByHash_ReturnsTransmissionWithItsObservation_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	obs := dbtest.Observer(t, db, "obsa", "Obs A", "CVG", lookupBase, lookupBase)
	tx := dbtest.ChannelTx(t, db, "lookup_hash", "3A", `{"k":1}`, lookupBase)
	dbtest.Observation(t, db, tx, obs, lookupBase.Add(time.Minute), 7.5, -90, "aa")

	got, err := db.GetPacketByHash(ctx, "lookup_hash")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != tx || got.Hash != "lookup_hash" || got.RawHex != "AABB" || got.DecodedJSON != `{"k":1}` {
		t.Errorf("want transmission %d with hash lookup_hash, raw hex AABB and its decoded JSON, got %+v", tx, got)
	}
	if got.RouteType != 1 || got.PayloadType != 5 {
		t.Errorf("want route type 1 and payload type 5, got %v and %v", got.RouteType, got.PayloadType)
	}
	if !got.FirstSeen.Equal(lookupBase) || got.FirstSeen.Location() != time.UTC {
		t.Errorf("want FirstSeen %v in UTC, got %v (%v)", lookupBase, got.FirstSeen, got.FirstSeen.Location())
	}
	if got.ScopeName != nil {
		t.Errorf("want no scope name, got %q", *got.ScopeName)
	}
	if got.ObservationCount != 1 {
		t.Errorf("want 1 observation, got %d", got.ObservationCount)
	}
	if got.ObserverPubkey == nil || *got.ObserverPubkey != "obsa" || got.ObserverName == nil || *got.ObserverName != "Obs A" {
		t.Errorf("want observer obsa / Obs A, got %v and %v", got.ObserverPubkey, got.ObserverName)
	}
	if got.SNR == nil || *got.SNR != 7.5 || got.RSSI == nil || *got.RSSI != -90 {
		t.Errorf("want SNR 7.5 and RSSI -90, got %v and %v", got.SNR, got.RSSI)
	}
	if !slices.Equal(got.HopPrefixes, []string{"aa"}) {
		t.Errorf("want hop prefixes [aa], got %v", got.HopPrefixes)
	}
}

func TestGetPacketByHash_IsCaseInsensitiveAndMatchesLookupByID_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	obs := dbtest.Observer(t, db, "obsa", "Obs A", "CVG", lookupBase, lookupBase)
	tx := dbtest.ChannelTx(t, db, "case_hash", "3A", `{"k":1}`, lookupBase)
	dbtest.Observation(t, db, tx, obs, lookupBase.Add(time.Minute), 7.5, -90, "aa")

	byHash, err := db.GetPacketByHash(ctx, "CASE_HASH")
	if err != nil {
		t.Fatal(err)
	}
	byID, err := db.GetPacketByID(ctx, tx)
	if err != nil {
		t.Fatal(err)
	}
	if byHash.ID != tx {
		t.Errorf("an upper-case hash must find transmission %d, got %d", tx, byHash.ID)
	}
	if !reflect.DeepEqual(byHash, byID) {
		t.Errorf("lookup by hash and by id must return the same packet:\n  hash: %+v\n  id:   %+v", byHash, byID)
	}
}

func TestGetPacketByHash_CountsEveryObservation_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	obsA := dbtest.Observer(t, db, "obsa", "Obs A", "CVG", lookupBase, lookupBase)
	obsB := dbtest.Observer(t, db, "obsb", "Obs B", "DAY", lookupBase, lookupBase)
	tx := dbtest.ChannelTx(t, db, "count_hash", "3A", `{"k":1}`, lookupBase)
	dbtest.Observation(t, db, tx, obsA, lookupBase.Add(time.Minute), 7.5, -90, "aa")
	dbtest.Observation(t, db, tx, obsB, lookupBase.Add(2*time.Minute), 6.0, -95, "bb")

	got, err := db.GetPacketByHash(ctx, "count_hash")
	if err != nil {
		t.Fatal(err)
	}
	if got.ObservationCount != 2 {
		t.Errorf("want 2 observations counted, got %d", got.ObservationCount)
	}
	if got.ObserverPubkey == nil {
		t.Errorf("want a representative observation, got none")
	}
}

func TestGetPacketByHash_TransmissionWithoutObservations_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	tx := dbtest.ChannelTx(t, db, "bare_hash", "3A", `{"k":1}`, lookupBase)

	got, err := db.GetPacketByHash(ctx, "bare_hash")
	if err != nil {
		t.Fatalf("a transmission nobody has observed yet must still be found: %v", err)
	}
	if got.ID != tx || got.ObservationCount != 0 || got.ObserverPubkey != nil || got.SNR != nil || got.HopPrefixes != nil && len(got.HopPrefixes) != 0 {
		t.Errorf("want the bare transmission with no observation data, got %+v", got)
	}
}

func TestPacketLookup_UnknownIsNotFound_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()

	if got, err := db.GetPacketByHash(ctx, "nope"); !errors.Is(err, database.ErrNotFound) || got != nil {
		t.Errorf("unknown hash: want ErrNotFound and no packet, got %+v (%v)", got, err)
	}
	if got, err := db.GetPacketByID(ctx, 987654); !errors.Is(err, database.ErrNotFound) || got != nil {
		t.Errorf("unknown id: want ErrNotFound and no packet, got %+v (%v)", got, err)
	}
}

var queryBase = time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)

const (
	queryNodeA = "aabbccdd11223344"
	queryNodeB = "eeff00112233aabb"
)

type queryPackets struct{ t1, t2, t3, t4 int64 }

// seedQueryPackets creates four packets:
//
//	t1 flood advert from node A, heard by obsa (SEA) at base
//	t2 flood channel message 3A, heard by obsa at base+1h
//	t3 direct advert from node B, heard by obsb (PDX) at base+2h
//	t4 flood encrypted channel message 3A, first heard by obsa at base+3h and again by obsb at base+10h
func seedQueryPackets(t *testing.T, db *database.DB) queryPackets {
	t.Helper()
	b := queryBase
	obsA := dbtest.Observer(t, db, "obsa", "Obs A", "SEA", b, b)
	obsB := dbtest.Observer(t, db, "obsb", "Obs B", "PDX", b, b)
	advert := func(key string, at time.Time) *database.NodeInput {
		return &database.NodeInput{PublicKey: key, Name: "node-" + key, Role: "repeater", RxTime: at, IngestNow: at}
	}

	var p queryPackets
	p.t1 = dbtest.TxWithObservation(t, db, obsA, database.TransmissionInput{
		RawHex: "0180e1", Hash: "e1", RouteType: types.RouteFlood, PayloadType: types.PayloadADVERT,
		DecodedJSON: `{}`, Advert: advert(queryNodeA, b),
	}, b, 10.0, -90)
	p.t2 = dbtest.TxWithObservation(t, db, obsA, database.TransmissionInput{
		RawHex: "0180e2", Hash: "e2", RouteType: types.RouteFlood, PayloadType: types.PayloadGRP_TXT,
		DecodedJSON: `{}`, ChannelHash: "3A",
	}, b.Add(time.Hour), 9.0, -91)
	p.t3 = dbtest.TxWithObservation(t, db, obsB, database.TransmissionInput{
		RawHex: "0180e3", Hash: "e3", RouteType: types.RouteDirect, PayloadType: types.PayloadADVERT,
		DecodedJSON: `{}`, Advert: advert(queryNodeB, b.Add(2*time.Hour)),
	}, b.Add(2*time.Hour), 8.0, -92)
	p.t4 = dbtest.TxWithObservation(t, db, obsA, database.TransmissionInput{
		RawHex: "0180e4", Hash: "e4", RouteType: types.RouteFlood, PayloadType: types.PayloadGRP_TXT,
		DecodedJSON: `{}`, ChannelHash: "3A", ChannelEncrypted: true,
	}, b.Add(3*time.Hour), 7.0, -93)
	dbtest.Observation(t, db, p.t4, obsB, b.Add(10*time.Hour), 6.0, -94)
	return p
}

func packetIDs(rows []database.Packet) []int64 {
	out := make([]int64, len(rows))
	for i, r := range rows {
		out[i] = r.ID
	}
	return out
}

func TestQueryPackets_Filters_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	p := seedQueryPackets(t, db)
	b := queryBase

	at := func(d time.Duration) *time.Time { v := b.Add(d); return &v }
	payload := func(v types.PayloadType) *int { i := int(v); return &i }
	route := func(v types.RouteType) *int { i := int(v); return &i }

	cases := []struct {
		name  string
		query database.PacketQuery
		want  []int64
	}{
		{"no filter, newest first", database.PacketQuery{}, []int64{p.t4, p.t3, p.t2, p.t1}},
		{"ascending", database.PacketQuery{Order: "asc"}, []int64{p.t1, p.t2, p.t3, p.t4}},
		{"payload type", database.PacketQuery{Type: payload(types.PayloadADVERT)}, []int64{p.t3, p.t1}},
		{"route type", database.PacketQuery{Route: route(types.RouteFlood)}, []int64{p.t4, p.t2, p.t1}},
		{"payload and route are ANDed", database.PacketQuery{Type: payload(types.PayloadADVERT), Route: route(types.RouteFlood)}, []int64{p.t1}},
		{"hash, any case", database.PacketQuery{Hash: " E2 "}, []int64{p.t2}},
		{"node by pubkey", database.PacketQuery{Node: queryNodeA}, []int64{p.t1}},
		{"observer", database.PacketQuery{Observers: []string{"obsb"}}, []int64{p.t4, p.t3}},
		{"observer, any case", database.PacketQuery{Observers: []string{" OBSB "}}, []int64{p.t4, p.t3}},
		{"region", database.PacketQuery{Region: "PDX"}, []int64{p.t4, p.t3}},
		{"region heard in two places", database.PacketQuery{Region: "SEA"}, []int64{p.t4, p.t2, p.t1}},
		{"channel", database.PacketQuery{Channel: "3A"}, []int64{p.t2}},
		{"encrypted channel", database.PacketQuery{Channel: "3A", ChannelEncrypted: true}, []int64{p.t4}},
		{"observation since is inclusive", database.PacketQuery{ObsSince: at(time.Hour)}, []int64{p.t4, p.t3, p.t2}},
		{"observation until is inclusive", database.PacketQuery{ObsUntil: at(time.Hour)}, []int64{p.t2, p.t1}},
		{"observation since matches a later re-observation", database.PacketQuery{ObsSince: at(9 * time.Hour)}, []int64{p.t4}},
		{"first seen after is strict", database.PacketQuery{FirstSeenAfter: at(time.Hour)}, []int64{p.t4, p.t3}},
		{"first seen before is strict", database.PacketQuery{FirstSeenBefore: at(2 * time.Hour)}, []int64{p.t2, p.t1}},
		{"first seen ignores a later re-observation", database.PacketQuery{FirstSeenAfter: at(9 * time.Hour)}, []int64{}},
		{"observer and channel are ANDed", database.PacketQuery{Observers: []string{"obsb"}, Channel: "3A", ChannelEncrypted: true}, []int64{p.t4}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rows, total, err := db.QueryPackets(ctx, c.query)
			if err != nil {
				t.Fatal(err)
			}
			if got := packetIDs(rows); !slices.Equal(got, c.want) {
				t.Errorf("ids = %v, want %v", got, c.want)
			}
			if total != int64(len(c.want)) {
				t.Errorf("total = %d, want %d", total, len(c.want))
			}
		})
	}
}

func TestQueryPackets_PagesAndKeepsTheFullTotal_DB(t *testing.T) {
	db := newDB(t)
	p := seedQueryPackets(t, db)

	rows, total, err := db.QueryPackets(t.Context(), database.PacketQuery{Limit: 2, Offset: 1})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := packetIDs(rows), []int64{p.t3, p.t2}; !slices.Equal(got, want) {
		t.Errorf("ids = %v, want %v", got, want)
	}
	if total != 4 {
		t.Errorf("total = %d, want 4 (the count ignores limit and offset)", total)
	}

	empty, total, err := db.QueryPackets(t.Context(), database.PacketQuery{Limit: 2, Offset: 10})
	if err != nil {
		t.Fatal(err)
	}
	if empty == nil || len(empty) != 0 || total != 4 {
		t.Errorf("past the end: want an empty non-nil page and total 4, got %#v and %d", empty, total)
	}
}

func TestQueryPackets_RejectsOutOfRangePaging_DB(t *testing.T) {
	db := newDB(t)
	for _, q := range []database.PacketQuery{{Offset: -1}, {Limit: math.MaxInt32 + 1}, {Offset: math.MaxInt32 + 1}} {
		if _, _, err := db.QueryPackets(t.Context(), q); err == nil {
			t.Errorf("limit %d offset %d: want an error", q.Limit, q.Offset)
		}
	}
}

func TestQueryGroupedPackets_RepresentativeCountsAndOrder_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	b := queryBase
	obsA := dbtest.Observer(t, db, "obsa", "Obs A", "SEA", b, b)
	obsB := dbtest.Observer(t, db, "obsb", "Obs B", "PDX", b, b)

	g1 := dbtest.ChannelTx(t, db, "g1", "3A", `{"k":1}`, b)
	g2 := dbtest.ChannelTx(t, db, "g2", "3A", `{"k":2}`, b.Add(time.Hour))
	dbtest.ChannelTx(t, db, "g3", "3A", `{"k":3}`, b.Add(2*time.Hour)) // never observed

	dbtest.Observation(t, db, g1, obsA, b.Add(time.Minute), 7.5, -90, "aa")
	dbtest.Observation(t, db, g1, obsB, b.Add(2*time.Minute), 6.0, -95, "aa", "bb")
	dbtest.Observation(t, db, g2, obsA, b.Add(time.Hour), 5.0, -100)
	if err := db.BumpTransmissionLastSeen(ctx, g1, b.Add(5*time.Hour)); err != nil {
		t.Fatal(err)
	}

	rows, total, err := db.QueryGroupedPackets(ctx, database.PacketQuery{})
	if err != nil {
		t.Fatal(err)
	}
	hashes := make([]string, len(rows))
	for i, r := range rows {
		hashes[i] = r.Hash
	}
	if want := []string{"g1", "g3", "g2"}; !slices.Equal(hashes, want) || total != 3 {
		t.Fatalf("want %v ordered by latest activity (g1 was bumped to base+5h) with total 3, got %v and %d", want, hashes, total)
	}

	first := rows[0]
	if first.Count != 2 || first.ObserverCount != 2 {
		t.Errorf("g1: want 2 observations from 2 observers, got %d and %d", first.Count, first.ObserverCount)
	}
	if first.ObserverID == nil || *first.ObserverID != "obsb" || first.ObserverName == nil || *first.ObserverName != "Obs B" || first.ObserverIATA != "PDX" {
		t.Errorf("g1: want the observation with the most hops (obsb / Obs B / PDX), got %v %v %q", first.ObserverID, first.ObserverName, first.ObserverIATA)
	}
	if first.SNR == nil || *first.SNR != 6.0 || first.RSSI == nil || *first.RSSI != -95 {
		t.Errorf("g1: want SNR 6 and RSSI -95 of that observation, got %v and %v", first.SNR, first.RSSI)
	}
	if !slices.Equal(first.HopPrefixes, []string{"aa", "bb"}) {
		t.Errorf("g1: want hop prefixes [aa bb], got %v", first.HopPrefixes)
	}
	if !slices.Equal(first.DistinctIATAs, []string{"PDX", "SEA"}) {
		t.Errorf("g1: want distinct IATAs [PDX SEA], got %v", first.DistinctIATAs)
	}
	if first.RawHex != "AABB" || first.DecodedJSON != `{"k":1}` {
		t.Errorf("g1: want raw hex AABB and its decoded JSON, got %q and %q", first.RawHex, first.DecodedJSON)
	}
	if first.PayloadType == nil || *first.PayloadType != 5 || first.RouteType == nil || *first.RouteType != 1 || first.ScopeName != nil {
		t.Errorf("g1: want payload 5, route 1 and no scope, got %v %v %v", first.PayloadType, first.RouteType, first.ScopeName)
	}
	if !first.FirstSeen.Equal(b) || first.FirstSeen.Location() != time.UTC || !first.Latest.Equal(b.Add(5*time.Hour)) || first.Latest.Location() != time.UTC {
		t.Errorf("g1: want FirstSeen %v and Latest %v, both in UTC, got %v and %v", b, b.Add(5*time.Hour), first.FirstSeen, first.Latest)
	}

	bare := rows[1]
	if bare.Count != 0 || bare.ObserverCount != 0 || bare.ObserverID != nil || bare.ObserverName != nil || bare.SNR != nil || bare.RSSI != nil {
		t.Errorf("g3: want no observation data, got %+v", bare)
	}
	if len(bare.HopPrefixes) != 0 || len(bare.DistinctIATAs) != 0 {
		t.Errorf("g3: want no hops and no IATAs, got %v and %v", bare.HopPrefixes, bare.DistinctIATAs)
	}
}

func TestQueryGroupedPackets_FiltersAndPages_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	b := queryBase
	obsA := dbtest.Observer(t, db, "obsa", "Obs A", "SEA", b, b)
	obsB := dbtest.Observer(t, db, "obsb", "Obs B", "PDX", b, b)
	g1 := dbtest.ChannelTx(t, db, "g1", "3A", `{}`, b)
	g2 := dbtest.ChannelTx(t, db, "g2", "3A", `{}`, b.Add(time.Hour))
	dbtest.Observation(t, db, g1, obsA, b, 7.5, -90)
	dbtest.Observation(t, db, g2, obsB, b.Add(time.Hour), 6.0, -95)

	rows, total, err := db.QueryGroupedPackets(ctx, database.PacketQuery{Observers: []string{"obsb"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Hash != "g2" || total != 1 {
		t.Errorf("observer filter: want only g2 with total 1, got %+v and %d", rows, total)
	}

	rows, total, err = db.QueryGroupedPackets(ctx, database.PacketQuery{Limit: 1, Offset: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Hash != "g1" || total != 2 {
		t.Errorf("paging: want the second page to be g1 with total 2, got %+v and %d", rows, total)
	}
}

func TestQueryMultiNodePackets_NormalizesKeysAndChecksPaging_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	p := seedQueryPackets(t, db)

	rows, total, err := db.QueryMultiNodePackets(ctx, database.MultiNodePacketParams{Pubkeys: []string{"  AABBCCDD11223344 ", ""}})
	if err != nil {
		t.Fatal(err)
	}
	if got := packetIDs(rows); !slices.Equal(got, []int64{p.t1}) || total != 1 {
		t.Errorf("want an upper-case, padded pubkey to find %d, got %v and %d", p.t1, got, total)
	}

	none, total, err := db.QueryMultiNodePackets(ctx, database.MultiNodePacketParams{Pubkeys: []string{"ffffffffffffffff"}})
	if err != nil {
		t.Fatal(err)
	}
	if none == nil || len(none) != 0 || total != 0 {
		t.Errorf("no match: want an empty non-nil slice and 0, got %#v and %d", none, total)
	}

	blank, total, err := db.QueryMultiNodePackets(ctx, database.MultiNodePacketParams{Pubkeys: []string{" "}})
	if err != nil {
		t.Fatal(err)
	}
	if blank == nil || len(blank) != 0 || total != 0 {
		t.Errorf("blank pubkey: want an empty non-nil slice and 0, got %#v and %d", blank, total)
	}

	if _, _, err := db.QueryMultiNodePackets(ctx, database.MultiNodePacketParams{Pubkeys: []string{queryNodeA}, Offset: -1}); err == nil {
		t.Error("a negative offset must be an error")
	}
}

func TestGetPacketByHash_MatchesTheListingRow_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	seedQueryPackets(t, db)

	byHash, err := db.GetPacketByHash(ctx, "e1")
	if err != nil {
		t.Fatal(err)
	}
	listed, _, err := db.QueryMultiNodePackets(ctx, database.MultiNodePacketParams{Pubkeys: []string{queryNodeA}})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || !reflect.DeepEqual(*byHash, listed[0]) {
		t.Errorf("a packet looked up by hash must equal its row in a listing (same fields, FromPubkey and ObserverIATA included):\n  lookup:  %+v\n  listing: %+v", byHash, listed)
	}
	if byHash.FromPubkey == nil || *byHash.FromPubkey != queryNodeA || byHash.ObserverIATA != "SEA" {
		t.Errorf("want FromPubkey %s and ObserverIATA SEA, got %v and %q", queryNodeA, byHash.FromPubkey, byHash.ObserverIATA)
	}

	multi, err := db.GetPacketByHash(ctx, "e4")
	if err != nil {
		t.Fatal(err)
	}
	rows, _, err := db.QueryPackets(ctx, database.PacketQuery{Hash: "e4"})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || !reflect.DeepEqual(*multi, rows[0]) {
		t.Errorf("a packet heard twice must also equal its listing row:\n  lookup:  %+v\n  listing: %+v", multi, rows)
	}
	if multi.ObservationCount != 2 {
		t.Errorf("want 2 observations, got %d", multi.ObservationCount)
	}
}

func TestCreateOrUpdateTransmission_DuplicateHashKeepsTheFirst_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	in := database.TransmissionInput{
		RawHex: "0180e1", Hash: "e1", RouteType: types.RouteFlood, PayloadType: types.PayloadGRP_TXT, DecodedJSON: `{}`,
	}

	first, isNew, err := db.CreateOrUpdateTransmission(ctx, in, queryBase)
	if err != nil || !isNew {
		t.Fatalf("first insert: want isNew and no error, got %v and %v", isNew, err)
	}
	again, isNew, err := db.CreateOrUpdateTransmission(ctx, in, queryBase.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if isNew || again != first {
		t.Errorf("same hash: want id %d with isNew false, got %d and %v", first, again, isNew)
	}
	got, err := db.GetPacketByHash(ctx, "e1")
	if err != nil {
		t.Fatal(err)
	}
	if !got.FirstSeen.Equal(queryBase) {
		t.Errorf("want FirstSeen to stay %v, got %v", queryBase, got.FirstSeen)
	}
	if n, err := db.CountTransmissions(ctx); err != nil || n != 1 {
		t.Errorf("want 1 transmission, got %d (%v)", n, err)
	}
}

func TestCreateOrUpdateTransmission_NormalizesHashAndAdvertKey_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	advert := &database.NodeInput{PublicKey: "AABBCCDD11223344", Name: "Node", Role: "repeater", RxTime: queryBase, IngestNow: queryBase}
	in := database.TransmissionInput{
		RawHex: "0180e1ab", Hash: " E1AB ", RouteType: types.RouteFlood, PayloadType: types.PayloadADVERT, DecodedJSON: `{}`, Advert: advert,
	}

	id, isNew, err := db.CreateOrUpdateTransmission(ctx, in, queryBase)
	if err != nil || !isNew {
		t.Fatalf("want a new transmission, got %v and %v", isNew, err)
	}
	got, err := db.GetPacketByHash(ctx, "E1AB")
	if err != nil {
		t.Fatalf("a stored hash must be findable in any case: %v", err)
	}
	if got.ID != id || got.Hash != "e1ab" {
		t.Errorf("want id %d with the hash stored as e1ab, got %d and %q", id, got.ID, got.Hash)
	}
	if got.FromPubkey == nil || *got.FromPubkey != "aabbccdd11223344" {
		t.Errorf("want FromPubkey aabbccdd11223344, got %v", got.FromPubkey)
	}
	rows, _, err := db.QueryMultiNodePackets(ctx, database.MultiNodePacketParams{Pubkeys: []string{queryNodeA}})
	if err != nil || len(rows) != 1 || rows[0].ID != id {
		t.Errorf("want the advert node's packets to include %d, got %+v (%v)", id, rows, err)
	}

	again, isNew, err := db.CreateOrUpdateTransmission(ctx, in, queryBase.Add(time.Minute))
	if err != nil || isNew || again != id {
		t.Errorf("the same hash in another case: want id %d and isNew false, got %d, %v, %v", id, again, isNew, err)
	}
}

func TestCreateOrUpdateTransmission_RejectsBadInput_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	good := database.TransmissionInput{
		RawHex: "0180e1", Hash: "e1", RouteType: types.RouteFlood, PayloadType: types.PayloadGRP_TXT, DecodedJSON: `{}`,
	}
	advert := &database.NodeInput{PublicKey: queryNodeA, Name: "Node", Role: "repeater", RxTime: queryBase, IngestNow: queryBase}

	cases := map[string]func(in *database.TransmissionInput){
		"not hex":               func(in *database.TransmissionInput) { in.RawHex = "ZZ" },
		"odd length hex":        func(in *database.TransmissionInput) { in.RawHex = "ABC" },
		"empty hash":            func(in *database.TransmissionInput) { in.Hash = "  " },
		"negative version":      func(in *database.TransmissionInput) { in.PayloadVersion = -1 },
		"bad frame with advert": func(in *database.TransmissionInput) { in.RawHex = "ZZ"; in.Advert = advert },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			in := good
			mutate(&in)
			if _, _, err := db.CreateOrUpdateTransmission(ctx, in, queryBase); err == nil {
				t.Fatal("want an error")
			}
		})
	}
	if n, err := db.CountTransmissions(ctx); err != nil || n != 0 {
		t.Errorf("nothing may be stored from rejected input, got %d transmissions (%v)", n, err)
	}
}

func TestCreateOrUpdateTransmission_ScopeNameKeepsUnknownDistinctFromNone_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	scopeTx(t, db, "sn_none", types.RouteTransportFlood, nil, queryBase, nil)
	scopeTx(t, db, "sn_unknown", types.RouteTransportFlood, ptr(""), queryBase, nil)
	scopeTx(t, db, "sn_named", types.RouteTransportFlood, ptr("#cvg"), queryBase, nil)

	none, err := db.GetPacketByHash(ctx, "sn_none")
	if err != nil {
		t.Fatal(err)
	}
	unknown, err := db.GetPacketByHash(ctx, "sn_unknown")
	if err != nil {
		t.Fatal(err)
	}
	named, err := db.GetPacketByHash(ctx, "sn_named")
	if err != nil {
		t.Fatal(err)
	}
	if none.ScopeName != nil {
		t.Errorf("unscoped: want nil, got %q", *none.ScopeName)
	}
	if unknown.ScopeName == nil || *unknown.ScopeName != "" {
		t.Errorf("scoped but unmatched: want an empty non-nil name, got %v", unknown.ScopeName)
	}
	if named.ScopeName == nil || *named.ScopeName != "#cvg" {
		t.Errorf("scoped: want #cvg, got %v", named.ScopeName)
	}
}

func TestCountTransmissions_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()

	if n, err := db.CountTransmissions(ctx); err != nil || n != 0 {
		t.Fatalf("empty: want 0, got %d (%v)", n, err)
	}
	for i := range 3 {
		dbtest.ChannelTx(t, db, fmt.Sprintf("count_%d", i), "3A", `{}`, queryBase.Add(time.Duration(i)*time.Minute))
	}
	if n, err := db.CountTransmissions(ctx); err != nil || n != 3 {
		t.Errorf("want 3, got %d (%v)", n, err)
	}
}

func TestPruneTransmissionsBatch_DeletesOldTransmissionsInBatches_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	b := queryBase
	for i := range 3 {
		dbtest.ChannelTx(t, db, fmt.Sprintf("old_%d", i), "3A", `{}`, b.Add(-48*time.Hour+time.Duration(i)*time.Minute))
	}
	dbtest.ChannelTx(t, db, "fresh", "3A", `{}`, b)
	cutoff := b.Add(-24 * time.Hour)

	for i, want := range []int64{2, 1, 0} {
		got, err := db.PruneTransmissionsBatch(ctx, cutoff, 2)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("batch %d: want %d deleted, got %d", i+1, want, got)
		}
	}
	if _, err := db.GetPacketByHash(ctx, "fresh"); err != nil {
		t.Errorf("the transmission newer than the cutoff must survive: %v", err)
	}
	if _, err := db.GetPacketByHash(ctx, "old_0"); !errors.Is(err, database.ErrNotFound) {
		t.Errorf("an old transmission must be gone, got %v", err)
	}
	if _, err := db.PruneTransmissionsBatch(ctx, cutoff, 0); err == nil {
		t.Error("a batch size of 0 would delete nothing forever; want an error")
	}
}

func TestCountFloodAdvertsForNode_CountsScopedAndUnscopedFloods_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	b := queryBase
	node := func(key string) *database.NodeInput {
		return &database.NodeInput{PublicKey: key, Name: "node-" + key, Role: "repeater", RxTime: b, IngestNow: b}
	}

	scopeTx(t, db, "fa_at_since", types.RouteFlood, nil, b, node(queryNodeA))                                   // not strictly after
	scopeTx(t, db, "fa_flood", types.RouteFlood, nil, b.Add(time.Minute), node(queryNodeA))                     // counted
	scopeTx(t, db, "fa_scoped", types.RouteTransportFlood, ptr("#cvg"), b.Add(2*time.Minute), node(queryNodeA)) // counted
	scopeTx(t, db, "fa_direct", types.RouteDirect, nil, b.Add(3*time.Minute), node(queryNodeA))                 // zero-hop
	scopeTx(t, db, "fa_transport_direct", types.RouteTransportDirect, nil, b.Add(4*time.Minute), node(queryNodeA))
	scopeTx(t, db, "fa_other", types.RouteFlood, nil, b.Add(5*time.Minute), node(queryNodeB)) // another node
	scopeTx(t, db, "fa_before", types.RouteFlood, nil, b.Add(-time.Hour), node(queryNodeA))

	n, err := db.CountFloodAdvertsForNode(ctx, " "+"AABBCCDD11223344", b)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("want 2 (the plain and the scoped flood after since, key in any case), got %d", n)
	}
}

func TestNthNewestTransmissionID_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	ids := make([]int64, 5)
	for i := range ids {
		ids[i] = dbtest.ChannelTx(t, db, fmt.Sprintf("nth_%d", i), "3A", `{}`, queryBase.Add(time.Duration(i)*time.Hour))
	}

	for n, want := range map[int64]int64{1: ids[4], 3: ids[2], 5: ids[0]} {
		got, err := db.NthNewestTransmissionID(ctx, nil, n)
		if err != nil || got != want {
			t.Errorf("n=%d: want %d, got %d (%v)", n, want, got, err)
		}
	}
	if _, err := db.NthNewestTransmissionID(ctx, nil, 6); !errors.Is(err, database.ErrNotFound) {
		t.Errorf("n=6 of 5: want ErrNotFound, got %v", err)
	}

	since := queryBase.Add(2 * time.Hour)
	if got, err := db.NthNewestTransmissionID(ctx, &since, 3); err != nil || got != ids[2] {
		t.Errorf("since the third, n=3: want %d, got %d (%v)", ids[2], got, err)
	}
	if _, err := db.NthNewestTransmissionID(ctx, &since, 4); !errors.Is(err, database.ErrNotFound) {
		t.Errorf("since the third, n=4: want ErrNotFound, got %v", err)
	}
	for _, n := range []int64{0, -1} {
		if _, err := db.NthNewestTransmissionID(ctx, nil, n); err == nil {
			t.Errorf("n=%d: want an error", n)
		}
	}
}

func TestListUnmatchedTransmissions_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	b := queryBase

	scopeTx(t, db, "um_before", types.RouteTransportFlood, ptr(""), b.Add(-time.Hour), nil)
	u1 := scopeTx(t, db, "um_1", types.RouteTransportFlood, ptr(""), b, nil)
	u2 := scopeTx(t, db, "um_2", types.RouteTransportDirect, ptr(""), b.Add(time.Hour), nil)
	u3 := scopeTx(t, db, "um_3", types.RouteTransportFlood, ptr(""), b.Add(2*time.Hour), nil)
	scopeTx(t, db, "um_none", types.RouteTransportFlood, nil, b.Add(3*time.Hour), nil)
	scopeTx(t, db, "um_named", types.RouteTransportFlood, ptr("#cvg"), b.Add(4*time.Hour), nil)

	ids := func(rows []database.UnmatchedTransmission) []int64 {
		out := make([]int64, len(rows))
		for i, r := range rows {
			out[i] = r.ID
		}
		return out
	}
	both := []int{int(types.RouteTransportFlood), int(types.RouteTransportDirect)}

	got, err := db.ListUnmatchedTransmissions(ctx, b, both, 10)
	if err != nil {
		t.Fatal(err)
	}
	if want := []int64{u3, u2, u1}; !slices.Equal(ids(got), want) {
		t.Errorf("want scoped-but-unmatched transmissions since base, newest first: %v, got %v", want, ids(got))
	}
	if len(got) > 0 && got[0].RawHex != "AABB" {
		t.Errorf("want raw hex AABB, got %q", got[0].RawHex)
	}

	got, err = db.ListUnmatchedTransmissions(ctx, b, []int{int(types.RouteTransportFlood)}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if want := []int64{u3, u1}; !slices.Equal(ids(got), want) {
		t.Errorf("one route type: want %v, got %v", want, ids(got))
	}

	got, err = db.ListUnmatchedTransmissions(ctx, b, both, 1)
	if err != nil {
		t.Fatal(err)
	}
	if want := []int64{u3}; !slices.Equal(ids(got), want) {
		t.Errorf("limit 1: want %v, got %v", want, ids(got))
	}

	if _, err := db.ListUnmatchedTransmissions(ctx, b, both, -1); err == nil {
		t.Error("a negative limit must be an error")
	}
}

func TestListGroupTextTransmissions_RejectsOutOfRangeLimit_DB(t *testing.T) {
	db := newDB(t)
	for _, limit := range []int{0, -1, math.MaxInt32 + 1} {
		if _, err := db.ListGroupTextTransmissions(t.Context(), 0, limit); err == nil {
			t.Errorf("limit %d: want an error", limit)
		}
	}
}
