package database_test

import (
	"slices"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

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
