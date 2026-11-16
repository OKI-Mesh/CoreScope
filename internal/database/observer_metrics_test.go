package database_test

import (
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
)

var metricsBase = time.Date(2026, 3, 1, 12, 5, 0, 0, time.UTC)

func seedMetricsObservers(tb testing.TB, db *database.DB) {
	tb.Helper()
	dbtest.Observer(tb, db, "obs0", "Obs Zero", "CVG", metricsBase, metricsBase)
	dbtest.Observer(tb, db, "obs1", "Obs One", "CVG", metricsBase, metricsBase)
	dbtest.Observer(tb, db, "obs2", "Obs Two", "DAY", metricsBase, metricsBase)
	dbtest.Observer(tb, db, "obs3", "Obs Three", "LUK", metricsBase, metricsBase)
}

func storeMetrics(tb testing.TB, db *database.DB, inputs ...database.ObserverMetricsInput) {
	tb.Helper()
	for _, in := range inputs {
		if err := db.CreateOrUpdateObserverMetrics(tb.Context(), in); err != nil {
			tb.Fatal(err)
		}
	}
}

func rawMetrics(tb testing.TB, db *database.DB, pubkey string) []database.ObserverMetric {
	tb.Helper()
	got, err := db.GetObserverMetrics(tb.Context(), pubkey, nil, nil, database.MetricsRaw)
	if err != nil {
		tb.Fatal(err)
	}
	return got
}

func noiseFloors(metrics []database.ObserverMetric) []float64 {
	floors := make([]float64, len(metrics))
	for i, m := range metrics {
		floors[i] = *m.NoiseFloor
	}
	return floors
}

func TestParseMetricsResolution(t *testing.T) {
	for in, want := range map[string]database.MetricsResolution{
		"": database.MetricsRaw, "5m": database.MetricsRaw,
		"1h": database.MetricsHourly, "1d": database.MetricsDaily,
	} {
		got, err := database.ParseMetricsResolution(in)
		if err != nil || got != want {
			t.Errorf("%q: want %q, got %q (%v)", in, want, got, err)
		}
	}
	for _, in := range []string{"raw", "1H", "hourly", "15m", "1d "} {
		_, err := database.ParseMetricsResolution(in)
		if err == nil {
			t.Errorf("%q: want an error", in)
		} else if want := "invalid resolution: " + in + ". Must be 5m, 1h, or 1d"; err.Error() != want {
			t.Errorf("%q: want message %q, got %q", in, want, err.Error())
		}
	}
}

func TestGetObserverMetrics_RawRoundTripOldestFirst_DB(t *testing.T) {
	db := newDB(t)
	seedMetricsObservers(t, db)
	b := metricsBase

	storeMetrics(t, db,
		database.ObserverMetricsInput{ObserverID: "obs1", Timestamp: b.Add(20 * time.Minute),
			NoiseFloor: ptr(-99.5), TxAirSecs: ptr(30), RxAirSecs: ptr(40), RecvErrors: ptr(2),
			BatteryMv: ptr(3700), PacketsSent: ptr(50), PacketsRecv: ptr(60)},
		database.ObserverMetricsInput{ObserverID: "obs1", Timestamp: b,
			NoiseFloor: ptr(-101.25), TxAirSecs: ptr(10), RxAirSecs: ptr(20), RecvErrors: ptr(0),
			BatteryMv: ptr(3800), PacketsSent: ptr(5), PacketsRecv: ptr(6)},
		database.ObserverMetricsInput{ObserverID: "obs1", Timestamp: b.Add(10 * time.Minute)},
		database.ObserverMetricsInput{ObserverID: "obs2", Timestamp: b.Add(5 * time.Minute), NoiseFloor: ptr(-80.0)},
	)

	got := rawMetrics(t, db, "obs1")
	if len(got) != 3 {
		t.Fatalf("want 3 samples for obs1 only, got %+v", got)
	}
	for i, want := range []time.Time{b, b.Add(10 * time.Minute), b.Add(20 * time.Minute)} {
		if !got[i].Timestamp.Equal(want) || got[i].Timestamp.Location() != time.UTC {
			t.Errorf("sample %d: want %v in UTC, got %v (%v)", i, want, got[i].Timestamp, got[i].Timestamp.Location())
		}
	}

	first := got[0]
	if first.NoiseFloor == nil || *first.NoiseFloor != -101.25 ||
		first.TxAirSecs == nil || *first.TxAirSecs != 10 || first.RxAirSecs == nil || *first.RxAirSecs != 20 ||
		first.RecvErrors == nil || *first.RecvErrors != 0 || first.BatteryMv == nil || *first.BatteryMv != 3800 ||
		first.PacketsSent == nil || *first.PacketsSent != 5 || first.PacketsRecv == nil || *first.PacketsRecv != 6 {
		t.Errorf("first sample did not round trip: %+v", first)
	}

	empty := got[1]
	if empty.NoiseFloor != nil || empty.TxAirSecs != nil || empty.RxAirSecs != nil || empty.RecvErrors != nil ||
		empty.BatteryMv != nil || empty.PacketsSent != nil || empty.PacketsRecv != nil {
		t.Errorf("missing values must stay nil, got %+v", empty)
	}

	if none := rawMetrics(t, db, "nobody"); none == nil || len(none) != 0 {
		t.Errorf("unknown observer: want an empty non-nil slice, got %v", none)
	}
}

func TestCreateOrUpdateObserverMetrics_ReplacesSameTimestamp_DB(t *testing.T) {
	db := newDB(t)
	seedMetricsObservers(t, db)

	storeMetrics(t, db,
		database.ObserverMetricsInput{ObserverID: "obs1", Timestamp: metricsBase, NoiseFloor: ptr(-100.0), BatteryMv: ptr(3800)},
		database.ObserverMetricsInput{ObserverID: "obs1", Timestamp: metricsBase, NoiseFloor: ptr(-90.0), BatteryMv: ptr(3600)},
	)

	got := rawMetrics(t, db, "obs1")
	if len(got) != 1 {
		t.Fatalf("want one sample, got %+v", got)
	}
	if *got[0].NoiseFloor != -90.0 || *got[0].BatteryMv != 3600 {
		t.Errorf("want the second write to win, got %+v", got[0])
	}
}

func TestCreateOrUpdateObserverMetrics_RejectsCountersThatDoNotFitInt4_DB(t *testing.T) {
	db := newDB(t)
	seedMetricsObservers(t, db)

	err := db.CreateOrUpdateObserverMetrics(t.Context(), database.ObserverMetricsInput{
		ObserverID: "obs1", Timestamp: metricsBase, PacketsSent: ptr(3_000_000_000),
	})
	if err == nil {
		t.Fatal("want an error for a counter above the 32-bit range")
	}
	if got := rawMetrics(t, db, "obs1"); len(got) != 0 {
		t.Errorf("nothing must be stored for a rejected sample, got %+v", got)
	}
}

func TestGetObserverMetrics_SinceAndUntilAreInclusive_DB(t *testing.T) {
	db := newDB(t)
	seedMetricsObservers(t, db)
	b := metricsBase

	storeMetrics(t, db,
		database.ObserverMetricsInput{ObserverID: "obs1", Timestamp: b, NoiseFloor: ptr(-100.0)},
		database.ObserverMetricsInput{ObserverID: "obs1", Timestamp: b.Add(10 * time.Minute), NoiseFloor: ptr(-101.0)},
		database.ObserverMetricsInput{ObserverID: "obs1", Timestamp: b.Add(20 * time.Minute), NoiseFloor: ptr(-102.0)},
	)

	cases := []struct {
		name         string
		since, until *time.Time
		want         []float64
	}{
		{"no bounds", nil, nil, []float64{-100, -101, -102}},
		{"since only", ptr(b.Add(10 * time.Minute)), nil, []float64{-101, -102}},
		{"until only", nil, ptr(b.Add(10 * time.Minute)), []float64{-100, -101}},
		{"both inclusive", ptr(b.Add(10 * time.Minute)), ptr(b.Add(10 * time.Minute)), []float64{-101}},
		{"window with nothing", ptr(b.Add(time.Hour)), nil, []float64{}},
	}
	for _, c := range cases {
		got, err := db.GetObserverMetrics(t.Context(), "obs1", c.since, c.until, database.MetricsRaw)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if !slices.Equal(noiseFloors(got), c.want) {
			t.Errorf("%s: want %v, got %v", c.name, c.want, noiseFloors(got))
		}
	}
}

func TestGetObserverMetrics_BucketsKeepLastSamplePerUTCHourAndDay_DB(t *testing.T) {
	db := newDB(t)
	seedMetricsObservers(t, db)
	day := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	storeMetrics(t, db,
		database.ObserverMetricsInput{ObserverID: "obs1", Timestamp: day.Add(12*time.Hour + 5*time.Minute), NoiseFloor: ptr(-100.0)},
		database.ObserverMetricsInput{ObserverID: "obs1", Timestamp: day.Add(12*time.Hour + 35*time.Minute), NoiseFloor: ptr(-101.0)},
		database.ObserverMetricsInput{ObserverID: "obs1", Timestamp: day.Add(13*time.Hour + 10*time.Minute), NoiseFloor: ptr(-102.0)},
		database.ObserverMetricsInput{ObserverID: "obs1", Timestamp: day.Add(13*time.Hour + 50*time.Minute), NoiseFloor: ptr(-103.0)},
		database.ObserverMetricsInput{ObserverID: "obs1", Timestamp: day.Add(23*time.Hour + 30*time.Minute), NoiseFloor: ptr(-104.0)},
		database.ObserverMetricsInput{ObserverID: "obs1", Timestamp: day.Add(24*time.Hour + 30*time.Minute), NoiseFloor: ptr(-105.0)},
	)

	hourly, err := db.GetObserverMetrics(t.Context(), "obs1", nil, nil, database.MetricsHourly)
	if err != nil {
		t.Fatal(err)
	}
	wantHours := []time.Time{day.Add(12 * time.Hour), day.Add(13 * time.Hour), day.Add(23 * time.Hour), day.Add(24 * time.Hour)}
	if !slices.Equal(noiseFloors(hourly), []float64{-101, -103, -104, -105}) {
		t.Errorf("hourly: want the last sample per hour, got %v", noiseFloors(hourly))
	}
	for i, m := range hourly {
		if !m.Timestamp.Equal(wantHours[i]) || m.Timestamp.Location() != time.UTC {
			t.Errorf("hourly %d: want bucket start %v in UTC, got %v", i, wantHours[i], m.Timestamp)
		}
	}

	daily, err := db.GetObserverMetrics(t.Context(), "obs1", nil, nil, database.MetricsDaily)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(noiseFloors(daily), []float64{-104, -105}) {
		t.Errorf("daily: want the last sample per UTC day, got %v", noiseFloors(daily))
	}
	wantDays := []time.Time{day, day.Add(24 * time.Hour)}
	for i, m := range daily {
		if !m.Timestamp.Equal(wantDays[i]) {
			t.Errorf("daily %d: want bucket start %v, got %v", i, wantDays[i], m.Timestamp)
		}
	}
}

func TestGetMetricsSummary_RanksByMaxNoiseFloorThenPubkey_DB(t *testing.T) {
	db := newDB(t)
	seedMetricsObservers(t, db)
	b := metricsBase

	storeMetrics(t, db,
		database.ObserverMetricsInput{ObserverID: "obs1", Timestamp: b.Add(-time.Hour), NoiseFloor: ptr(-50.0), BatteryMv: ptr(4200)},
		database.ObserverMetricsInput{ObserverID: "obs2", Timestamp: b.Add(10 * time.Minute), NoiseFloor: ptr(-95.0), BatteryMv: ptr(3500)},
		database.ObserverMetricsInput{ObserverID: "obs1", Timestamp: b, NoiseFloor: ptr(-110.0), BatteryMv: ptr(4000)},
		database.ObserverMetricsInput{ObserverID: "obs0", Timestamp: b.Add(5 * time.Minute), NoiseFloor: ptr(-90.0)},
		database.ObserverMetricsInput{ObserverID: "obs1", Timestamp: b.Add(20 * time.Minute), BatteryMv: ptr(3800)},
		database.ObserverMetricsInput{ObserverID: "obs2", Timestamp: b, NoiseFloor: ptr(-90.0), BatteryMv: ptr(3600)},
		database.ObserverMetricsInput{ObserverID: "obs1", Timestamp: b.Add(10 * time.Minute), NoiseFloor: ptr(-100.0), BatteryMv: ptr(3900)},
		database.ObserverMetricsInput{ObserverID: "obs3", Timestamp: b, BatteryMv: ptr(3300)},
	)

	got, err := db.GetMetricsSummary(t.Context(), b)
	if err != nil {
		t.Fatal(err)
	}

	order := make([]string, len(got))
	for i, s := range got {
		order[i] = s.ObserverID
	}
	if !slices.Equal(order, []string{"obs0", "obs2", "obs1", "obs3"}) {
		t.Fatalf("want obs0 and obs2 tied at -90 (by pubkey), then obs1 at -100, then obs3 with no noise floor; got %v", order)
	}

	byID := map[string]database.MetricsSummary{}
	for _, s := range got {
		byID[s.ObserverID] = s
	}

	o1 := byID["obs1"]
	if o1.ObserverName != "Obs One" || o1.IATA != "CVG" || o1.SampleCount != 3 {
		t.Errorf("obs1: want name Obs One, CVG and 3 samples (the pre-window sample excluded), got %+v", o1)
	}
	if o1.CurrentNF != nil {
		t.Errorf("obs1: the latest sample has no noise floor, want nil CurrentNF, got %v", *o1.CurrentNF)
	}
	if o1.AvgNF == nil || *o1.AvgNF != -105 || o1.MaxNF == nil || *o1.MaxNF != -100 {
		t.Errorf("obs1: want avg -105 and max -100, got %v and %v", o1.AvgNF, o1.MaxNF)
	}
	if o1.CurrentBattMv == nil || *o1.CurrentBattMv != 3800 {
		t.Errorf("obs1: want current battery 3800, got %v", o1.CurrentBattMv)
	}
	if len(o1.Sparkline) != 3 || *o1.Sparkline[0] != -110 || *o1.Sparkline[1] != -100 || o1.Sparkline[2] != nil {
		t.Errorf("obs1: want sparkline [-110 -100 nil] oldest first, got %v", o1.Sparkline)
	}

	o2 := byID["obs2"]
	if o2.CurrentNF == nil || *o2.CurrentNF != -95 || *o2.AvgNF != -92.5 || *o2.MaxNF != -90 || o2.SampleCount != 2 {
		t.Errorf("obs2: want current -95, avg -92.5, max -90 over 2 samples, got %+v", o2)
	}

	o3 := byID["obs3"]
	if o3.AvgNF != nil || o3.MaxNF != nil || o3.CurrentNF != nil || o3.SampleCount != 1 {
		t.Errorf("obs3: want no noise statistics from one sample without a noise floor, got %+v", o3)
	}
	if o3.CurrentBattMv == nil || *o3.CurrentBattMv != 3300 {
		t.Errorf("obs3: want battery 3300, got %v", o3.CurrentBattMv)
	}
}

func TestGetMetricsSummary_NoSamplesInWindow_DB(t *testing.T) {
	db := newDB(t)
	seedMetricsObservers(t, db)
	storeMetrics(t, db, database.ObserverMetricsInput{ObserverID: "obs1", Timestamp: metricsBase, NoiseFloor: ptr(-100.0)})

	got, err := db.GetMetricsSummary(t.Context(), metricsBase.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || len(got) != 0 {
		t.Errorf("want an empty non-nil slice, got %v", got)
	}
}

func TestPruneObserverMetrics_DeletesOnlyOlderSamples_DB(t *testing.T) {
	db := newDB(t)
	seedMetricsObservers(t, db)
	b := metricsBase

	storeMetrics(t, db,
		database.ObserverMetricsInput{ObserverID: "obs1", Timestamp: b.Add(-2 * time.Hour), NoiseFloor: ptr(-100.0)},
		database.ObserverMetricsInput{ObserverID: "obs1", Timestamp: b, NoiseFloor: ptr(-101.0)},
		database.ObserverMetricsInput{ObserverID: "obs2", Timestamp: b.Add(time.Hour), NoiseFloor: ptr(-102.0)},
	)

	removed, err := db.PruneObserverMetrics(t.Context(), b)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Errorf("want 1 sample removed (strictly older than the cutoff), got %d", removed)
	}
	if got := rawMetrics(t, db, "obs1"); len(got) != 1 || *got[0].NoiseFloor != -101 {
		t.Errorf("obs1: want only the sample at the cutoff, got %+v", got)
	}
	if got := rawMetrics(t, db, "obs2"); len(got) != 1 {
		t.Errorf("obs2: want its sample kept, got %+v", got)
	}
}

func TestObserverMetrics_PubkeyIsCaseInsensitive_DB(t *testing.T) {
	db := newDB(t)
	seedMetricsObservers(t, db)

	storeMetrics(t, db, database.ObserverMetricsInput{ObserverID: "OBS1", Timestamp: metricsBase, NoiseFloor: ptr(-100.0)})

	for _, key := range []string{"obs1", "OBS1", " Obs1 "} {
		if got := rawMetrics(t, db, key); len(got) != 1 {
			t.Errorf("read with %q: want the one sample stored under obs1, got %+v", key, got)
		}
	}
}

func TestCreateOrUpdateObserverMetrics_UnknownObserverIsRejected_DB(t *testing.T) {
	db := newDB(t)
	seedMetricsObservers(t, db)

	err := db.CreateOrUpdateObserverMetrics(t.Context(), database.ObserverMetricsInput{
		ObserverID: "ghost", Timestamp: metricsBase, NoiseFloor: ptr(-100.0),
	})
	if err == nil {
		t.Fatal("want an error for a sample from an observer that does not exist")
	}
	if got := rawMetrics(t, db, "ghost"); len(got) != 0 {
		t.Errorf("nothing must be stored for an unknown observer, got %+v", got)
	}
}

func TestCreateOrUpdateObserverMetrics_EveryCounterIsRangeChecked_DB(t *testing.T) {
	db := newDB(t)
	seedMetricsObservers(t, db)

	tooBig := ptr(math.MaxInt32 + 1)
	tooSmall := ptr(math.MinInt32 - 1)
	cases := map[string]database.ObserverMetricsInput{
		"tx_air_secs":  {TxAirSecs: tooBig},
		"rx_air_secs":  {RxAirSecs: tooSmall},
		"recv_errors":  {RecvErrors: tooBig},
		"battery_mv":   {BatteryMv: tooSmall},
		"packets_sent": {PacketsSent: tooBig},
		"packets_recv": {PacketsRecv: tooSmall},
	}
	for field, in := range cases {
		in.ObserverID, in.Timestamp = "obs1", metricsBase
		err := db.CreateOrUpdateObserverMetrics(t.Context(), in)
		if err == nil || !strings.Contains(err.Error(), field) {
			t.Errorf("%s: want an error naming the field, got %v", field, err)
		}
	}
	if got := rawMetrics(t, db, "obs1"); len(got) != 0 {
		t.Errorf("nothing must be stored for rejected samples, got %+v", got)
	}

	storeMetrics(t, db, database.ObserverMetricsInput{
		ObserverID: "obs1", Timestamp: metricsBase, PacketsRecv: ptr(math.MaxInt32), PacketsSent: ptr(math.MinInt32),
	})
	got := rawMetrics(t, db, "obs1")
	if len(got) != 1 || got[0].PacketsRecv == nil || *got[0].PacketsRecv != math.MaxInt32 ||
		got[0].PacketsSent == nil || *got[0].PacketsSent != math.MinInt32 {
		t.Errorf("the 32-bit limits themselves must round trip, got %+v", got)
	}
}

func TestGetObserverMetrics_DailyBucketsAreUTCWhateverTheSessionTimezone_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	seedMetricsObservers(t, db)
	day := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	// 23:30 and 00:30 UTC are different UTC days but the same day in Los Angeles.
	storeMetrics(t, db,
		database.ObserverMetricsInput{ObserverID: "obs1", Timestamp: day.Add(23*time.Hour + 30*time.Minute), NoiseFloor: ptr(-104.0)},
		database.ObserverMetricsInput{ObserverID: "obs1", Timestamp: day.Add(24*time.Hour + 30*time.Minute), NoiseFloor: ptr(-105.0)},
	)

	if _, err := db.Pool().Exec(ctx, `DO $$ BEGIN
		EXECUTE format('ALTER DATABASE %I SET timezone TO %L', current_database(), 'America/Los_Angeles');
	END $$`); err != nil {
		t.Fatal(err)
	}
	db.Pool().Reset()

	var zone string
	if err := db.Pool().QueryRow(ctx, `SHOW timezone`).Scan(&zone); err != nil {
		t.Fatal(err)
	}
	if zone != "America/Los_Angeles" {
		t.Fatalf("precondition failed: want a new session in America/Los_Angeles, got %q", zone)
	}

	daily, err := db.GetObserverMetrics(ctx, "obs1", nil, nil, database.MetricsDaily)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(noiseFloors(daily), []float64{-104, -105}) {
		t.Fatalf("daily: want two UTC days, got %v", noiseFloors(daily))
	}
	for i, want := range []time.Time{day, day.Add(24 * time.Hour)} {
		if !daily[i].Timestamp.Equal(want) {
			t.Errorf("daily %d: want UTC bucket start %v, got %v", i, want, daily[i].Timestamp)
		}
	}
}
