package database_test

import (
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

func TestGetStats(t *testing.T) {
	ctx := t.Context()
	db := dbtest.New(t)

	now := time.Now().UTC()
	obsIdx := dbtest.Observer(t, db, "obsa", "Obs A", "SEA", now, now)

	// Nodes: one active, one last seen 10 days ago.
	dbtest.Node(t, db, "aabbccdd11223344", "Active", "repeater")
	dbtest.NodeWith(t, db, database.NodeInput{
		PublicKey: "eeff00112233aabb", Name: "Stale", Role: "repeater",
		RxTime: now.Add(-10 * 24 * time.Hour),
	})

	// Two transmissions; observations 10 minutes, 3 hours and 2 days old.
	tx := func(hash string, at time.Time) int64 {
		return dbtest.TxWithObservation(t, db, obsIdx, database.TransmissionInput{
			RawHex: hash, Hash: hash,
			RouteType: types.RouteType(1), PayloadType: types.PayloadType(4),
			DecodedJSON: `{}`,
		}, at, 10.0, -90)
	}
	tx("AA01", now.Add(-10*time.Minute))
	tx("AA02", now.Add(-3*time.Hour))
	tx("AA03", now.Add(-48*time.Hour))

	stats, err := db.GetStats(ctx, now)
	if err != nil {
		t.Fatalf("GetStats failed: %v", err)
	}

	want := map[string][2]int{ // name: {got, want}
		"TotalTransmissions": {stats.TotalTransmissions, 3},
		"TotalPackets":       {stats.TotalPackets, 3},
		"TotalObservations":  {stats.TotalObservations, 3},
		"TotalNodes":         {stats.TotalNodes, 1}, // 7-day active only
		"TotalNodesAllTime":  {stats.TotalNodesAllTime, 2},
		"TotalObservers":     {stats.TotalObservers, 1},
		"PacketsLastHour":    {stats.PacketsLastHour, 1},
		"PacketsLast24h":     {stats.PacketsLast24h, 2},
	}
	for name, v := range want {
		if v[0] != v[1] {
			t.Errorf("%s = %d, want %d", name, v[0], v[1])
		}
	}
}
