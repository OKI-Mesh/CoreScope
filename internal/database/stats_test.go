package database_test

import (
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

var statsNow = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

type statsFixture struct {
	obsA                   int64
	txRecent, txMid, txOld int64
}

// Two nodes (one heard an hour ago, one ten days ago), one observer, and three
// transmissions observed 10 minutes, 3 hours and 2 days before statsNow.
func seedStats(t *testing.T, db *database.DB) statsFixture {
	t.Helper()
	obsA := dbtest.Observer(t, db, "obsa", "Obs A", "SEA", statsNow, statsNow)

	dbtest.NodeWith(t, db, database.NodeInput{
		PublicKey: "aabbccdd11223344", Name: "Active", Role: "repeater",
		RxTime: statsNow.Add(-time.Hour), IngestNow: statsNow,
	})
	dbtest.NodeWith(t, db, database.NodeInput{
		PublicKey: "eeff00112233aabb", Name: "Stale", Role: "repeater",
		RxTime: statsNow.Add(-10 * 24 * time.Hour), IngestNow: statsNow,
	})

	tx := func(hash string, at time.Time) int64 {
		return dbtest.TxWithObservation(t, db, obsA, database.TransmissionInput{
			RawHex: hash, Hash: hash,
			RouteType: types.RouteType(1), PayloadType: types.PayloadType(4),
			DecodedJSON: `{}`,
		}, at, 10.0, -90)
	}
	return statsFixture{
		obsA:     obsA,
		txRecent: tx("AA01", statsNow.Add(-10*time.Minute)),
		txMid:    tx("AA02", statsNow.Add(-3*time.Hour)),
		txOld:    tx("AA03", statsNow.Add(-48*time.Hour)),
	}
}

func TestGetStats_CountsEverythingAndWindows_DB(t *testing.T) {
	db := newDB(t)
	seedStats(t, db)

	stats, err := db.GetStats(t.Context(), statsNow)
	if err != nil {
		t.Fatal(err)
	}

	for _, c := range []struct {
		name      string
		got, want int
	}{
		{"TotalTransmissions", stats.TotalTransmissions, 3},
		{"TotalPackets", stats.TotalPackets, 3},
		{"TotalObservations", stats.TotalObservations, 3},
		{"TotalNodes (heard in the last 7 days)", stats.TotalNodes, 1},
		{"TotalNodesAllTime", stats.TotalNodesAllTime, 2},
		{"TotalObservers", stats.TotalObservers, 1},
		{"PacketsLastHour", stats.PacketsLastHour, 1},
		{"PacketsLast24h", stats.PacketsLast24h, 2},
	} {
		if c.got != c.want {
			t.Errorf("%s: want %d, got %d", c.name, c.want, c.got)
		}
	}
}

func TestGetStats_EmptyDatabaseIsAllZero_DB(t *testing.T) {
	db := newDB(t)

	stats, err := db.GetStats(t.Context(), statsNow)
	if err != nil {
		t.Fatal(err)
	}
	if *stats != (database.Stats{}) {
		t.Errorf("want every count zero on an empty database, got %+v", *stats)
	}
}

func TestGetEntityCounts_ActiveWindowAndTotals_DB(t *testing.T) {
	db := newDB(t)
	f := seedStats(t, db)
	dbtest.Observer(t, db, "obsb", "Obs B", "DAY", statsNow, statsNow)

	got, err := db.GetEntityCounts(t.Context(), statsNow.Add(-7*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if want := (database.EntityCounts{ActiveNodes: 1, AllNodes: 2, Observers: 2}); got != want {
		t.Errorf("want %+v, got %+v", want, got)
	}

	narrow, err := db.GetEntityCounts(t.Context(), statsNow.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if narrow.ActiveNodes != 0 || narrow.AllNodes != 2 {
		t.Errorf("a window after every node was heard: want 0 active of 2, got %+v", narrow)
	}
	_ = f
}

func TestCountRecentObservations_CountsObservationsNotTransmissions_DB(t *testing.T) {
	db := newDB(t)
	f := seedStats(t, db)
	obsB := dbtest.Observer(t, db, "obsb", "Obs B", "DAY", statsNow, statsNow)
	dbtest.Observation(t, db, f.txRecent, obsB, statsNow.Add(-5*time.Minute), 6.0, -95, "aa")

	got, err := db.CountRecentObservations(t.Context(), statsNow)
	if err != nil {
		t.Fatal(err)
	}
	if want := (database.RecentObservationCounts{LastHour: 2, Last24h: 3}); got != want {
		t.Errorf("two observations of one transmission in the last hour, one more three hours ago: want %+v, got %+v", want, got)
	}

	empty, err := newDB(t).CountRecentObservations(t.Context(), statsNow)
	if err != nil {
		t.Fatal(err)
	}
	if empty != (database.RecentObservationCounts{}) {
		t.Errorf("empty database: want zero counts, got %+v", empty)
	}
}

func TestGetDBSizeStats_ReportsSizesAndRowCounts_DB(t *testing.T) {
	db := newDB(t)
	seedStats(t, db)

	got, err := db.GetDBSizeStats(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got.DBSizeBytes <= 0 {
		t.Errorf("want a positive database size, got %d", got.DBSizeBytes)
	}
	if want := (database.TableRowCounts{Transmissions: 3, Observations: 3, Nodes: 2, Observers: 1}); got.Rows != want {
		t.Errorf("row counts: want %+v, got %+v", want, got.Rows)
	}
	if got.WALSizeBytes == nil || *got.WALSizeBytes <= 0 {
		t.Errorf("the test role is a superuser, so want a positive WAL size, got %v", got.WALSizeBytes)
	}
	if got.Checkpoints == nil {
		t.Error("want checkpoint stats on Postgres 17 and later")
	}
	if got.DeadTuples < 0 {
		t.Errorf("want a non-negative dead tuple count, got %d", got.DeadTuples)
	}
}
