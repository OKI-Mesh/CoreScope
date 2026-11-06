package database_test

import (
	"context"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

func clientPtr[T any](v T) *T { return &v }

func countClientRows(tb testing.TB, db *database.DB, table string) int {
	tb.Helper()
	var n int
	if err := db.SQL().QueryRowContext(tb.Context(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		tb.Fatalf("count %s: %v", table, err)
	}
	return n
}

func TestInsertClientReception_DedupesOnKey_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	first := types.ClientReception{
		RxPubkey: "rxa", HeardKey: "a1b2", HeardKeyLen: 2,
		RSSI: ptr(-90), SNR: ptr(7.5),
		Lat: 39.1, Lon: -84.5, PosAccM: ptr(5.0),
		RxAt: base, IngestedAt: base.Add(time.Second), Src: "ble",
	}

	insert := func(label string, r types.ClientReception, want bool) {
		t.Helper()
		got, err := db.InsertClientReception(ctx, r)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		if got != want {
			t.Errorf("%s: want inserted=%v, got %v", label, want, got)
		}
	}

	insert("first", first, true)

	sameKey := first
	sameKey.RSSI = ptr(-70)
	sameKey.Lat = 40.0
	insert("same key, other values", sameKey, false)

	otherTime := first
	otherTime.RxAt = base.Add(time.Second)
	insert("other rx_at", otherTime, true)

	otherHeard := first
	otherHeard.HeardKey = "c3d4"
	insert("other heard_key", otherHeard, true)

	otherRx := first
	otherRx.RxPubkey = "rxb"
	insert("other rx_pubkey", otherRx, true)

	if got := countClientRows(t, db, "client_receptions"); got != 4 {
		t.Errorf("want 4 rows, got %d", got)
	}

	var rssi int
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT rssi FROM client_receptions WHERE rx_pubkey = $1 AND heard_key = $2 AND rx_at = $3`,
		"rxa", "a1b2", base).Scan(&rssi); err != nil {
		t.Fatal(err)
	}
	if rssi != -90 {
		t.Errorf("a duplicate must not overwrite the first row: want rssi -90, got %d", rssi)
	}
}

func TestInsertClientReception_NullableSignalFields_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	inserted, err := db.InsertClientReception(ctx, types.ClientReception{
		RxPubkey: "rxa", HeardKey: "a1b2", HeardKeyLen: 2,
		RSSI: nil, SNR: nil,
		Lat: 39.1, Lon: -84.5, PosAccM: nil,
		RxAt: base, IngestedAt: base.Add(time.Second), Src: "ble",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("expected the row to be inserted")
	}

	var rssiNull, snrNull, accNull bool
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT rssi IS NULL, snr IS NULL, pos_acc_m IS NULL FROM client_receptions WHERE rx_pubkey = $1`,
		"rxa").Scan(&rssiNull, &snrNull, &accNull); err != nil {
		t.Fatal(err)
	}
	if !rssiNull || !snrNull || !accNull {
		t.Errorf("want NULL rssi, snr and pos_acc_m, got null flags %v %v %v", rssiNull, snrNull, accNull)
	}
}

func TestInsertClientRfSample_DedupesAndKeepsMissingCountersNull_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	sample := types.ClientRfSample{
		RxPubkey: "rxa", SampledAt: base, IngestedAt: base.Add(time.Second),
		Lat: 39.1, Lon: -84.5, PosAccM: ptr(4.0), Stationary: true,
		UptimeSecs: 1000,
		BatteryMV:  ptr(4100), QueueLen: ptr(0), Errors: ptr(0),
		NoiseFloor: ptr(-110), LastRSSI: ptr(-95), LastSNR: ptr(6.5),
		TxAirSecs: clientPtr[int64](10), RxAirSecs: clientPtr[int64](20),
		Recv: clientPtr[int64](300), Sent: clientPtr[int64](40),
		FloodRx: clientPtr[int64](250), DirectRx: clientPtr[int64](50),
		FloodTx: clientPtr[int64](30), DirectTx: clientPtr[int64](10),
		RecvErrors: nil,
	}

	inserted, err := db.InsertClientRfSample(ctx, &sample)
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("first sample: want inserted")
	}

	duplicate := sample
	duplicate.UptimeSecs = 2000
	inserted, err = db.InsertClientRfSample(ctx, &duplicate)
	if err != nil {
		t.Fatal(err)
	}
	if inserted {
		t.Error("same rx_pubkey and sampled_at: want not inserted")
	}

	later := sample
	later.SampledAt = base.Add(time.Minute)
	inserted, err = db.InsertClientRfSample(ctx, &later)
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Error("later sampled_at: want inserted")
	}

	if got := countClientRows(t, db, "client_rf_samples"); got != 2 {
		t.Errorf("want 2 rows, got %d", got)
	}

	var uptime int64
	var recvErrorsNull, errorsNull bool
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT uptime_secs, recv_errors IS NULL, errors IS NULL FROM client_rf_samples WHERE rx_pubkey = $1 AND sampled_at = $2`,
		"rxa", base).Scan(&uptime, &recvErrorsNull, &errorsNull); err != nil {
		t.Fatal(err)
	}
	if uptime != 1000 {
		t.Errorf("a duplicate must not overwrite the first row: want uptime 1000, got %d", uptime)
	}
	if !recvErrorsNull {
		t.Error("recv_errors must stay NULL when the radio cannot count CRC errors")
	}
	if errorsNull {
		t.Error("errors was 0 and must be stored as 0, not NULL")
	}
}

func TestInsertClientRxObservation_DedupesAndCounts_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	obs := types.ClientRxObservation{
		RxPubkey: "rxa", RxAt: base, IngestedAt: base.Add(time.Second),
		PktHash: "0123456789abcdef", RouteType: types.RouteType(1), PayloadType: types.PayloadType(4),
		HashSize: 1, HopCount: 0,
		Code1: nil, Code2: nil, ScopeName: nil, PathJSON: nil, Forwarder: nil,
		SNR: ptr(5.5), RSSI: ptr(-92),
		Lat: 39.1, Lon: -84.5, PosAccM: ptr(6.0),
	}

	insert := func(label string, o types.ClientRxObservation, want bool) {
		t.Helper()
		got, err := db.InsertClientRxObservation(ctx, &o)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		if got != want {
			t.Errorf("%s: want inserted=%v, got %v", label, want, got)
		}
	}

	insert("first", obs, true)

	sameKey := obs
	sameKey.SNR = ptr(9.0)
	insert("same key, other values", sameKey, false)

	otherHash := obs
	otherHash.PktHash = "fedcba9876543210"
	insert("other pkt_hash", otherHash, true)

	otherTime := obs
	otherTime.RxAt = base.Add(time.Second)
	insert("other rx_at", otherTime, true)

	count, err := db.GetClientRxObservationsCount(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("want count 3, got %d", count)
	}
}

func TestCreateOrUpdateClientObserver_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	if _, err := db.CreateOrUpdateClientObserver(ctx, types.ClientObserver{Pubkey: "rxa", Name: "Phone", LastSeen: time.Time{}}); err == nil {
		t.Error("a zero LastSeen must be rejected")
	}
	if got := countClientRows(t, db, "client_observers"); got != 0 {
		t.Fatalf("a rejected call must not insert, got %d rows", got)
	}

	if _, err := db.CreateOrUpdateClientObserver(ctx, types.ClientObserver{Pubkey: "rxa", Name: "Phone", LastSeen: base}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateOrUpdateClientObserver(ctx, types.ClientObserver{Pubkey: "rxa", Name: "Phone 2", LastSeen: base.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}

	if got := countClientRows(t, db, "client_observers"); got != 1 {
		t.Fatalf("want one row after an update, got %d", got)
	}

	var name string
	var lastSeen time.Time
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT name, last_seen FROM client_observers WHERE pubkey = $1`, "rxa").Scan(&name, &lastSeen); err != nil {
		t.Fatal(err)
	}
	if name != "Phone 2" || !lastSeen.Equal(base.Add(time.Hour)) {
		t.Errorf("want name Phone 2 and last_seen %v, got %q and %v", base.Add(time.Hour), name, lastSeen)
	}
}

func TestClientDeclaredRegions_CurrentIsNewestObservation_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	missing, err := db.GetCurrentDeclaredRegions(ctx, "unknown-target")
	if err != nil {
		t.Fatalf("an unknown target is not an error, got %v", err)
	}
	if missing != nil {
		t.Fatalf("an unknown target must return nil, got %+v", missing)
	}

	observations := []types.ClientDeclaredRegions{
		{Target: "t1", RxPubkey: "rxa", ObservedAt: base, IngestedAt: base.Add(time.Second),
			RegionsCSV: "old", Truncated: false},
		{Target: "t1", RxPubkey: "rxa", ObservedAt: base.Add(time.Hour), IngestedAt: base.Add(time.Hour + time.Second),
			RegionsCSV: "newest,second", Truncated: true,
			Lat: ptr(39.1), Lon: ptr(-84.5), PosAccM: ptr(5.0), RepeaterClock: clientPtr[int64](1700000000)},
		{Target: "t1", RxPubkey: "rxb", ObservedAt: base.Add(-time.Hour), IngestedAt: base.Add(-time.Hour + time.Second),
			RegionsCSV: "older", Truncated: false},
		{Target: "t2", RxPubkey: "rxa", ObservedAt: base.Add(2 * time.Hour), IngestedAt: base.Add(2*time.Hour + time.Second),
			RegionsCSV: "other target", Truncated: false},
	}
	for i := range observations {
		inserted, err := db.InsertClientDeclaredRegions(ctx, &observations[i])
		if err != nil {
			t.Fatal(err)
		}
		if !inserted {
			t.Fatalf("observation %d: want inserted", i)
		}
	}

	duplicate := observations[1]
	duplicate.RegionsCSV = "changed"
	inserted, err := db.InsertClientDeclaredRegions(ctx, &duplicate)
	if err != nil {
		t.Fatal(err)
	}
	if inserted {
		t.Error("same target, rx_pubkey and observed_at: want not inserted")
	}

	got, err := db.GetCurrentDeclaredRegions(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("want the newest observation for t1, got nil")
	}
	if got.RegionsCSV != "newest,second" || !got.Truncated || got.RxPubkey != "rxa" {
		t.Errorf("want the newest t1 observation, got %+v", got)
	}
	if !got.ObservedAt.Equal(base.Add(time.Hour)) || got.ObservedAt.Location() != time.UTC {
		t.Errorf("want ObservedAt %v in UTC, got %v (%v)", base.Add(time.Hour), got.ObservedAt, got.ObservedAt.Location())
	}
	if !got.IngestedAt.Equal(base.Add(time.Hour+time.Second)) || got.IngestedAt.Location() != time.UTC {
		t.Errorf("want IngestedAt in UTC, got %v (%v)", got.IngestedAt, got.IngestedAt.Location())
	}
	if got.Lat == nil || *got.Lat != 39.1 || got.Lon == nil || *got.Lon != -84.5 ||
		got.PosAccM == nil || *got.PosAccM != 5.0 || got.RepeaterClock == nil || *got.RepeaterClock != 1700000000 {
		t.Errorf("want position and repeater clock round-tripped, got %+v", got)
	}
}

func TestClientDeclaredRegions_EmptyListAndMissingFieldsRoundTrip_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	inserted, err := db.InsertClientDeclaredRegions(ctx, &types.ClientDeclaredRegions{
		Target: "t1", RxPubkey: "rxa", ObservedAt: base, IngestedAt: base.Add(time.Second),
		RegionsCSV: "", Truncated: false,
		Lat: nil, Lon: nil, PosAccM: nil, RepeaterClock: nil,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("an empty region list is a valid answer and must be stored")
	}

	got, err := db.GetCurrentDeclaredRegions(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.RegionsCSV != "" {
		t.Fatalf("want a stored empty list, got %+v", got)
	}
	if got.Lat != nil || got.Lon != nil || got.PosAccM != nil || got.RepeaterClock != nil {
		t.Errorf("want nil position and clock, got %+v", got)
	}
}

func TestClientPruneMethodsDeleteBeforeCutoff_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	cutoff := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	old := cutoff.Add(-time.Hour)
	fresh := cutoff.Add(time.Hour)

	for _, at := range []time.Time{old, fresh} {
		if _, err := db.InsertClientReception(ctx, types.ClientReception{
			RxPubkey: "rxa", HeardKey: "a1b2", HeardKeyLen: 2,
			RSSI: ptr(-90), SNR: ptr(7.5), Lat: 39.1, Lon: -84.5, PosAccM: nil,
			RxAt: at, IngestedAt: at, Src: "ble",
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := db.InsertClientRxObservation(ctx, &types.ClientRxObservation{
			RxPubkey: "rxa", RxAt: at, IngestedAt: at,
			PktHash: "0123456789abcdef", RouteType: types.RouteType(1), PayloadType: types.PayloadType(4),
			HashSize: 1, HopCount: 0, Lat: 39.1, Lon: -84.5,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := db.InsertClientRfSample(ctx, &types.ClientRfSample{
			RxPubkey: "rxa", SampledAt: at, IngestedAt: at, Lat: 39.1, Lon: -84.5, UptimeSecs: 1000,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := db.InsertClientDeclaredRegions(ctx, &types.ClientDeclaredRegions{
			Target: "t1", RxPubkey: "rxa", ObservedAt: at, IngestedAt: at, RegionsCSV: "r",
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.CreateOrUpdateClientObserver(ctx, types.ClientObserver{Pubkey: "stale", Name: "Stale", LastSeen: old}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateOrUpdateClientObserver(ctx, types.ClientObserver{Pubkey: "live", Name: "Live", LastSeen: fresh}); err != nil {
		t.Fatal(err)
	}

	receptions, err := db.PruneClientReceptions(ctx, cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if receptions != 1 {
		t.Errorf("PruneClientReceptions: want 1 reception deleted, got %d", receptions)
	}
	if got := countClientRows(t, db, "client_receptions"); got != 1 {
		t.Errorf("want 1 reception left, got %d", got)
	}
	if got := countClientRows(t, db, "client_observers"); got != 1 {
		t.Errorf("PruneClientReceptions must also remove stale client_observers: want 1 left, got %d", got)
	}

	for name, prune := range map[string]func(context.Context, time.Time) (int64, error){
		"PruneClientRxObservations": db.PruneClientRxObservations,
		"PruneClientRfSamples":      db.PruneClientRfSamples,
		"PruneDeclaredRegions":      db.PruneDeclaredRegions,
	} {
		deleted, err := prune(ctx, cutoff)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if deleted != 1 {
			t.Errorf("%s: want 1 row deleted, got %d", name, deleted)
		}
	}

	count, err := db.GetClientRxObservationsCount(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("want 1 rx observation left after the prune, got %d", count)
	}
	for _, table := range []string{"client_rf_samples", "node_declared_regions"} {
		if got := countClientRows(t, db, table); got != 1 {
			t.Errorf("%s: want 1 row left, got %d", table, got)
		}
	}
}

func TestListClientRfSamplesWithPrev_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	samples := []types.ClientRfSample{
		{RxPubkey: "rxa", SampledAt: base.Add(-time.Minute), IngestedAt: base, Lat: 39.0, Lon: -84.0,
			Stationary: false, UptimeSecs: 40,
			RxAirSecs: clientPtr[int64](4), TxAirSecs: clientPtr[int64](2), Recv: clientPtr[int64](20), RecvErrors: clientPtr[int64](1)},
		{RxPubkey: "rxa", SampledAt: base, IngestedAt: base, Lat: 39.1, Lon: -84.5,
			Stationary: true, UptimeSecs: 100,
			RxAirSecs: clientPtr[int64](10), TxAirSecs: clientPtr[int64](5), Recv: clientPtr[int64](50), RecvErrors: nil},
		{RxPubkey: "rxa", SampledAt: base.Add(time.Minute), IngestedAt: base, Lat: 39.2, Lon: -84.6,
			Stationary: false, UptimeSecs: 160,
			RxAirSecs: clientPtr[int64](16), TxAirSecs: clientPtr[int64](8), Recv: clientPtr[int64](80), RecvErrors: clientPtr[int64](2)},
		{RxPubkey: "rxa", SampledAt: base.Add(2 * time.Minute), IngestedAt: base, Lat: 39.3, Lon: -84.7,
			Stationary: true, UptimeSecs: 220,
			RxAirSecs: clientPtr[int64](25), TxAirSecs: clientPtr[int64](9), Recv: clientPtr[int64](120), RecvErrors: nil},
		{RxPubkey: "rxa", SampledAt: base.Add(3 * time.Minute), IngestedAt: base, Lat: 39.4, Lon: -84.8,
			Stationary: true, UptimeSecs: 280,
			RxAirSecs: clientPtr[int64](30), TxAirSecs: clientPtr[int64](10), Recv: clientPtr[int64](150), RecvErrors: clientPtr[int64](3)},
		{RxPubkey: "rxb", SampledAt: base.Add(30 * time.Second), IngestedAt: base, Lat: 40.0, Lon: -85.0,
			Stationary: true, UptimeSecs: 999,
			RxAirSecs: clientPtr[int64](99), TxAirSecs: clientPtr[int64](99), Recv: clientPtr[int64](999), RecvErrors: clientPtr[int64](9)},
	}
	for i := range samples {
		inserted, err := db.InsertClientRfSample(ctx, &samples[i])
		if err != nil {
			t.Fatal(err)
		}
		if !inserted {
			t.Fatalf("sample %d: want inserted", i)
		}
	}

	got, err := db.ListClientRfSamplesWithPrev(ctx, "rxa", base, base.Add(3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("want the 3 rxa samples in [base, base+3m), got %d: %+v", len(got), got)
	}

	wantInt := func(label string, got *int64, want int64) {
		t.Helper()
		if got == nil || *got != want {
			t.Errorf("%s: want %d, got %v", label, want, got)
		}
	}
	wantNil := func(label string, got *int64) {
		t.Helper()
		if got != nil {
			t.Errorf("%s: want nil, got %d", label, *got)
		}
	}

	first := got[0]
	if !first.SampledAt.Equal(base) || first.SampledAt.Location() != time.UTC {
		t.Errorf("row 0: want SampledAt %v in UTC, got %v (%v)", base, first.SampledAt, first.SampledAt.Location())
	}
	if first.Lat != 39.1 || first.Lon != -84.5 || !first.Stationary || first.UptimeSecs != 100 {
		t.Errorf("row 0: position, stationary or uptime wrong: %+v", first)
	}
	wantInt("row 0 RxAirSecs", first.RxAirSecs, 10)
	wantInt("row 0 TxAirSecs", first.TxAirSecs, 5)
	wantInt("row 0 Recv", first.Recv, 50)
	wantNil("row 0 RecvErrors", first.RecvErrors)
	if first.PrevAt != nil {
		t.Errorf("row 0: the sample before start is outside the window, want PrevAt nil, got %v", first.PrevAt)
	}
	wantNil("row 0 PrevUptime", first.PrevUptime)
	wantNil("row 0 PrevRx", first.PrevRx)
	wantNil("row 0 PrevTx", first.PrevTx)
	wantNil("row 0 PrevRecv", first.PrevRecv)
	wantNil("row 0 PrevErrs", first.PrevErrs)

	second := got[1]
	if !second.SampledAt.Equal(base.Add(time.Minute)) {
		t.Errorf("row 1: want SampledAt %v, got %v", base.Add(time.Minute), second.SampledAt)
	}
	if second.PrevAt == nil || !second.PrevAt.Equal(base) || second.PrevAt.Location() != time.UTC {
		t.Errorf("row 1: want PrevAt %v in UTC, got %v", base, second.PrevAt)
	}
	wantInt("row 1 PrevUptime", second.PrevUptime, 100)
	wantInt("row 1 PrevRx", second.PrevRx, 10)
	wantInt("row 1 PrevTx", second.PrevTx, 5)
	wantInt("row 1 PrevRecv", second.PrevRecv, 50)
	wantNil("row 1 PrevErrs", second.PrevErrs)
	wantInt("row 1 RecvErrors", second.RecvErrors, 2)

	third := got[2]
	if !third.SampledAt.Equal(base.Add(2 * time.Minute)) {
		t.Errorf("row 2: want SampledAt %v, got %v", base.Add(2*time.Minute), third.SampledAt)
	}
	if third.PrevAt == nil || !third.PrevAt.Equal(base.Add(time.Minute)) {
		t.Errorf("row 2: want PrevAt %v, got %v", base.Add(time.Minute), third.PrevAt)
	}
	wantInt("row 2 PrevUptime", third.PrevUptime, 160)
	wantInt("row 2 PrevRx", third.PrevRx, 16)
	wantInt("row 2 PrevTx", third.PrevTx, 8)
	wantInt("row 2 PrevRecv", third.PrevRecv, 80)
	wantInt("row 2 PrevErrs", third.PrevErrs, 2)
	wantNil("row 2 RecvErrors", third.RecvErrors)

	none, err := db.ListClientRfSamplesWithPrev(ctx, "nobody", base.Add(-time.Hour), base.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(none) != 0 {
		t.Errorf("an unknown receiver must return no rows, got %+v", none)
	}
}
