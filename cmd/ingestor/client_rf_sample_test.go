package main

import (
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// rfFixtureTime returns a sample timestamp offset seconds from a base two
// hours in the past, in the layout the handler round-trips.
//
// The times have to be anchored to now, not to a calendar date.
// resolveRxTimeCore (main.go:1527) replaces any timestamp more than 30 days
// old with the ingest time, so a fixed fixture date stops round-tripping 30
// days after it is written: the seeded rows land at time.Now() while the query
// window stays in the past, and every delta disappears. The 2026-08-17
// fixtures this file used to carry did exactly that on 2026-09-16T10:00Z,
// which turned all three delta tests red on master and on every open PR.
// The base is computed once for the whole test binary, not per call: two
// calls straddling a wall-clock second boundary would otherwise sit 16 s
// apart instead of 15 s, and TestRfDeltaBreaksOnReboot asserts the exact
// WallMillis.
var rfFixtureBase = time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Second)

// rfFixtureAt returns the sample instant `offset` after the fixture base.
func rfFixtureAt(offset time.Duration) time.Time {
	return rfFixtureBase.Add(offset)
}

// rfFixtureTS is the same instant as the envelope "timestamp" string the
// handler parses.
func rfFixtureTS(offset time.Duration) string {
	return rfFixtureAt(offset).Format(time.RFC3339Nano)
}

// rfFixtureWindow brackets the rfFixtureAt series: one hour either side of the
// base. ClientRfDeltas treats it as a half-open range, [from, to).
func rfFixtureWindow() (from, to time.Time) {
	return rfFixtureAt(-time.Hour), rfFixtureAt(time.Hour)
}

func TestHandleClientRfSample(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()
	// A non-zero millisecond offset, so the stored value can prove that
	// millisecond precision survives. At offset 0 it would also pass if the
	// handler had truncated to whole seconds.
	offset := 123 * time.Millisecond
	msg := map[string]interface{}{
		"type": "RF_SAMPLE", "timestamp": rfFixtureTS(offset),
		"gps":         map[string]interface{}{"lat": 51.2, "lon": 4.4, "acc_m": 8.0},
		"stationary":  false,
		"uptime_secs": 84213.0, "noise_floor": -119.0, "rx_air_secs": 20877.0,
	}
	handleClientRfSample(ctx, s, "test", "aa11", msg)

	if n := countRows(t, s, "client_rf_samples"); n != 1 {
		t.Fatalf("rows = %d, want 1", n)
	}

	var sampledAt time.Time
	var recvErrorsNull bool
	mustScan(t, testRow(t, s,
		`SELECT sampled_at, recv_errors IS NULL FROM client_rf_samples WHERE rx_pubkey = $1`, "aa11"),
		&sampledAt, &recvErrorsNull)

	// sampled_at is half of the unique key (rx_pubkey, sampled_at) and is
	// stored at millisecond precision.
	if want := rfFixtureAt(offset); !sampledAt.Equal(want) {
		t.Errorf("sampled_at = %v, want %v", sampledAt, want)
	}
	// The payload omits recv_errors; it must stay NULL, never default to 0
	// (which would read as a clean channel on firmware that can't count CRC
	// errors).
	if !recvErrorsNull {
		t.Error("recv_errors should be NULL when omitted from the payload")
	}
}

func TestHandleClientRfSampleRejects(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()
	base := func() map[string]interface{} {
		return map[string]interface{}{
			"timestamp":   rfFixtureTS(0),
			"gps":         map[string]interface{}{"lat": 51.2, "lon": 4.4},
			"uptime_secs": 1.0,
		}
	}

	handleClientRfSample(ctx, s, "test", "NOT-HEX!", base()) // bad topic pubkey

	noGPS := base()
	delete(noGPS, "gps")
	handleClientRfSample(ctx, s, "test", "aa11", noGPS)

	badGPS := base()
	badGPS["gps"] = map[string]interface{}{"lat": 999.0, "lon": 4.4}
	handleClientRfSample(ctx, s, "test", "aa11", badGPS)

	noUptime := base()
	delete(noUptime, "uptime_secs")
	handleClientRfSample(ctx, s, "test", "aa11", noUptime)

	n := countRows(t, s, "client_rf_samples")
	if n != 0 {
		t.Errorf("rows = %d, want 0 — all four must be rejected", n)
	}
}

// TestRfDeltaBreaksOnReboot seeds the first two samples through
// handleClientRfSample (not a direct InsertClientRfSample) so the
// handler-written rxTimeMillisLayout format is what ClientRfDeltas actually
// parses back — a regression that reverted the handler to second-resolution
// resolveRxTime would leave a direct-insert-seeded test green while breaking
// this round trip for real.
func TestRfDeltaBreaksOnReboot(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()
	seed := func(ts string, uptime, rxAir float64) {
		handleClientRfSample(ctx, s, "test", "aa11", map[string]interface{}{
			"timestamp":   ts,
			"gps":         map[string]interface{}{"lat": 51.2, "lon": 4.4},
			"uptime_secs": uptime,
			"rx_air_secs": rxAir,
		})
	}
	seed(rfFixtureTS(0), 1000, 500)
	seed(rfFixtureTS(15*time.Second), 1015, 512) // +12 s of RX air over 15 s
	seed(rfFixtureTS(30*time.Second), 10, 3)     // rebooted: uptime dropped

	from, to := rfFixtureWindow()
	deltas, err := s.ClientRfDeltas(ctx, "aa11", from, to)
	if err != nil {
		t.Fatalf("deltas: %v", err)
	}
	if len(deltas) != 1 {
		t.Fatalf("deltas = %d, want 1 (the reboot pair must be skipped)", len(deltas))
	}
	if deltas[0].RxAirDelta == nil || *deltas[0].RxAirDelta != 12 || deltas[0].WallMillis != 15000 {
		t.Errorf("delta = %v over %dms, want 12 over 15000ms", deltas[0].RxAirDelta, deltas[0].WallMillis)
	}
}

// TestRfDeltaNilWhenEitherEndpointMissingCounter proves the NULL-vs-zero
// distinction survives into the delta view: a counter unsupported by
// firmware (NULL on either endpoint) must produce a nil *int64 delta, never
// a 0 that a CRC-error-rate map would render as "clean channel".
func TestRfDeltaNilWhenEitherEndpointMissingCounter(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()

	seed := func(ts string, uptime float64, recvErrors *int64) {
		msg := map[string]interface{}{
			"timestamp":   ts,
			"gps":         map[string]interface{}{"lat": 51.2, "lon": 4.4},
			"uptime_secs": uptime,
		}
		if recvErrors != nil {
			msg["recv_errors"] = float64(*recvErrors)
		}
		handleClientRfSample(ctx, s, "test", "aa11", msg)
	}
	five := int64(5)
	seed(rfFixtureTS(0), 1000, nil) // firmware without the counter
	seed(rfFixtureTS(15*time.Second), 1015, &five)

	from, to := rfFixtureWindow()
	deltas, err := s.ClientRfDeltas(ctx, "aa11", from, to)
	if err != nil {
		t.Fatalf("deltas: %v", err)
	}
	if len(deltas) != 1 {
		t.Fatalf("deltas = %d, want 1", len(deltas))
	}
	if deltas[0].RecvErrDelta != nil {
		t.Errorf("RecvErrDelta = %v, want nil (prev recv_errors was NULL)", *deltas[0].RecvErrDelta)
	}
}

// TestClientRfDeltasNormalizesPubkeyCase proves ClientRfDeltas matches the
// stored lowercase rx_pubkey even when a caller passes uppercase hex —
// mirroring the strings.ToLower(rx) normalization in
// cmd/server/rx_dashboard.go's queryCoverageFiltered. Without it, a URL path
// pubkey pasted in uppercase silently returns zero rows.
func TestClientRfDeltasNormalizesPubkeyCase(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()
	seed := func(ts string, uptime, rxAir float64) {
		handleClientRfSample(ctx, s, "test", "aa11", map[string]interface{}{
			"timestamp":   ts,
			"gps":         map[string]interface{}{"lat": 51.2, "lon": 4.4},
			"uptime_secs": uptime,
			"rx_air_secs": rxAir,
		})
	}
	seed(rfFixtureTS(0), 1000, 500)
	seed(rfFixtureTS(15*time.Second), 1015, 512)

	from, to := rfFixtureWindow()
	deltas, err := s.ClientRfDeltas(ctx, "AA11", from, to)
	if err != nil {
		t.Fatalf("deltas: %v", err)
	}
	if len(deltas) != 1 {
		t.Fatalf("deltas = %d, want 1 for uppercase pubkey lookup", len(deltas))
	}
}

func TestPruneOldClientRfSamples(t *testing.T) {
	s := newTestStore(t)
	ctx := testCtx(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	recent, old := now.AddDate(0, 0, -1), now.AddDate(0, 0, -40)

	mk := func(at time.Time) *types.ClientRfSample {
		return &types.ClientRfSample{
			RxPubkey: "aa11", SampledAt: at, IngestedAt: at,
			Lat: 51.2, Lon: 4.4, UptimeSecs: 100,
		}
	}
	for _, at := range []time.Time{recent, old} {
		if _, err := s.InsertClientRfSample(ctx, mk(at)); err != nil {
			t.Fatal(err)
		}
	}

	if n, _ := s.PruneOldClientRfSamples(ctx, 0); n != 0 {
		t.Fatalf("days=0 must be a no-op, got %d", n)
	}
	if got := countRows(t, s, "client_rf_samples"); got != 2 {
		t.Fatalf("days=0 deleted rows: %d remain, want 2", got)
	}
	n, err := s.PruneOldClientRfSamples(ctx, 7)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 old sample pruned, got %d", n)
	}
	if got := countWhere(t, s, "client_rf_samples", "sampled_at = $1", recent); got != 1 {
		t.Fatalf("the recent sample must survive, got %d", got)
	}
}

// TestPruneClientRfSamplesCutoffBoundary pins which side of the cutoff survives,
// against a fixed instant so it cannot race the clock. The cutoff has a non-zero
// millisecond part on purpose: a row a millisecond newer must be kept, and one a
// millisecond older must go.
func TestPruneClientRfSamplesCutoffBoundary(t *testing.T) {
	s := newTestStore(t)
	ctx := testCtx(t)
	cutoff := time.Date(2026, 8, 4, 10, 0, 0, 400*int(time.Millisecond), time.UTC)
	before, boundary := cutoff.Add(-time.Millisecond), cutoff.Add(time.Millisecond)

	for _, at := range []time.Time{before, boundary} {
		if _, err := s.InsertClientRfSample(ctx, &types.ClientRfSample{
			RxPubkey: "aa11", SampledAt: at, IngestedAt: at, Lat: 51.2, Lon: 4.4, UptimeSecs: 100,
		}); err != nil {
			t.Fatal(err)
		}
	}

	n, err := s.pruneOldClientRfSamplesAt(ctx, cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("pruned %d, want 1 (only the row a millisecond before the cutoff)", n)
	}
	if got := countWhere(t, s, "client_rf_samples", "sampled_at = $1", boundary); got != 1 {
		t.Fatalf("the row a millisecond after the cutoff must survive, got %d", got)
	}
	if got := countWhere(t, s, "client_rf_samples", "sampled_at = $1", before); got != 0 {
		t.Fatalf("the row a millisecond before the cutoff must be deleted, got %d", got)
	}
}

func wantPtr[T comparable](t *testing.T, name string, got *T, want T) {
	t.Helper()
	if got == nil {
		t.Errorf("%s is NULL, want %v", name, want)
	} else if *got != want {
		t.Errorf("%s = %v, want %v", name, *got, want)
	}
}

func TestInsertClientRfSample(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()
	posAcc := 12.5
	snr := -6.75
	errs := 7
	nf := -119
	rssi := -92
	battery := 4021
	queueLen := 3
	txAir := int64(341)
	rxAir := int64(20877)
	recv := int64(18422)
	sent := int64(290)
	floodRx := int64(17110)
	directRx := int64(1312)
	floodTx := int64(212)
	directTx := int64(78)
	o := &types.ClientRfSample{
		RxPubkey: "aa11", SampledAt: time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC), IngestedAt: time.Date(2026, 8, 17, 10, 0, 1, 0, time.UTC),
		Lat: 51.2, Lon: 4.4, PosAccM: &posAcc, Stationary: true, UptimeSecs: 84213,
		BatteryMV: &battery, QueueLen: &queueLen, Errors: &errs, NoiseFloor: &nf, LastRSSI: &rssi,
		LastSNR: &snr, TxAirSecs: &txAir, RxAirSecs: &rxAir, Recv: &recv, Sent: &sent,
		FloodRx: &floodRx, DirectRx: &directRx, FloodTx: &floodTx, DirectTx: &directTx,
	}
	ins, err := s.InsertClientRfSample(ctx, o)
	if err != nil || !ins {
		t.Fatalf("insert: ins=%v err=%v", ins, err)
	}
	ins, err = s.InsertClientRfSample(ctx, o)
	if err != nil || ins {
		t.Fatalf("duplicate must be idempotent: ins=%v err=%v", ins, err)
	}

	// Every column gets a distinct, non-symmetric value and is read back, so a
	// positional transposition between any two same-typed neighbours in the
	// 23-column INSERT (e.g. flood_rx/direct_rx, or recv/sent) is detectable:
	// row-counting alone would pass either way.
	var gotLat, gotLon float64
	var gotStationary bool
	var gotUptimeSecs int64
	var gotPosAccM, gotLastSNR *float64
	var gotBatteryMV, gotQueueLen, gotErrors, gotNoiseFloor, gotLastRSSI *int32
	var gotTxAirSecs, gotRxAirSecs, gotRecv, gotSent, gotFloodRx, gotDirectRx, gotFloodTx, gotDirectTx, gotRecvErrors *int64
	mustScan(t, testRow(t, s, `
		SELECT lat, lon, pos_acc_m, stationary, uptime_secs, battery_mv, queue_len,
		       errors, noise_floor, last_rssi, last_snr, tx_air_secs, rx_air_secs,
		       recv, sent, flood_rx, direct_rx, flood_tx, direct_tx, recv_errors
		FROM client_rf_samples WHERE sampled_at = $1`, o.SampledAt),
		&gotLat, &gotLon, &gotPosAccM, &gotStationary, &gotUptimeSecs, &gotBatteryMV, &gotQueueLen,
		&gotErrors, &gotNoiseFloor, &gotLastRSSI, &gotLastSNR, &gotTxAirSecs, &gotRxAirSecs,
		&gotRecv, &gotSent, &gotFloodRx, &gotDirectRx, &gotFloodTx, &gotDirectTx, &gotRecvErrors)

	if gotLat != 51.2 || gotLon != 4.4 {
		t.Errorf("lat/lon = %v/%v, want 51.2/4.4 (a lat<->lon swap would pass with symmetric values)", gotLat, gotLon)
	}
	if !gotStationary {
		t.Error("stationary = false, want true")
	}
	if gotUptimeSecs != 84213 {
		t.Errorf("uptime_secs = %d, want 84213", gotUptimeSecs)
	}
	wantPtr(t, "pos_acc_m", gotPosAccM, 12.5)
	wantPtr(t, "battery_mv", gotBatteryMV, 4021)
	wantPtr(t, "queue_len", gotQueueLen, 3)
	wantPtr(t, "errors", gotErrors, 7)
	wantPtr(t, "noise_floor", gotNoiseFloor, -119)
	wantPtr(t, "last_rssi", gotLastRSSI, -92)
	wantPtr(t, "last_snr", gotLastSNR, -6.75)
	wantPtr(t, "tx_air_secs", gotTxAirSecs, 341)
	wantPtr(t, "rx_air_secs", gotRxAirSecs, 20877)
	wantPtr(t, "recv", gotRecv, 18422)
	wantPtr(t, "sent", gotSent, 290)
	wantPtr(t, "flood_rx", gotFloodRx, 17110)
	wantPtr(t, "direct_rx", gotDirectRx, 1312)
	wantPtr(t, "flood_tx", gotFloodTx, 212)
	wantPtr(t, "direct_tx", gotDirectTx, 78)
	if gotRecvErrors != nil {
		t.Errorf("recv_errors = %v, want NULL on firmware that cannot count them", *gotRecvErrors)
	}

	// A second sample, at a different sampled_at, proves recv_errors round-trips
	// non-NULL too: the first sample only covers the NULL direction.
	re := int64(55)
	o2 := &types.ClientRfSample{
		RxPubkey:   "aa11",
		SampledAt:  time.Date(2026, 8, 17, 10, 0, 15, 0, time.UTC),
		IngestedAt: time.Date(2026, 8, 17, 10, 0, 16, 0, time.UTC),
		Lat:        51.2, Lon: 4.4, Stationary: true, UptimeSecs: 84228, RecvErrors: &re,
	}
	if ins, err = s.InsertClientRfSample(ctx, o2); err != nil || !ins {
		t.Fatalf("insert second sample: ins=%v err=%v", ins, err)
	}
	var gotRecvErrors2 *int64
	mustScan(t, testRow(t, s, `SELECT recv_errors FROM client_rf_samples WHERE sampled_at = $1`, o2.SampledAt), &gotRecvErrors2)
	wantPtr(t, "recv_errors (second sample)", gotRecvErrors2, 55)

}
