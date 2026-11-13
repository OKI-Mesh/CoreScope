package main

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
)

func TestObserverEndpointsMatchSpec(t *testing.T) {
	ctx := t.Context()
	checker := newSpecChecker(t)
	srv, router := setupTestServer(t)

	// Counters that rise, then fall (a reboot), then a gap of more than two
	// intervals, so the samples carry rates, a reboot flag and null rates.
	at := func(minute int) time.Time {
		return time.Date(2026, time.April, 5, 0, minute, 0, 0, time.UTC)
	}
	sample := func(minute int, noise float64, battery, tx, rx, errs, sent, recv int) {
		mustCreateTestObserverMetrics(t, ctx, srv.db, database.ObserverMetricsInput{
			ObserverID: "obs1", Timestamp: at(minute),
			NoiseFloor: &noise, BatteryMv: &battery,
			TxAirSecs: &tx, RxAirSecs: &rx, RecvErrors: &errs, PacketsSent: &sent, PacketsRecv: &recv,
		})
	}
	sample(0, -112, 4100, 10, 30, 1, 5, 40)
	sample(5, -110, 4090, 20, 60, 3, 9, 80)
	sample(10, -111, 4080, 2, 6, 0, 1, 4)
	sample(50, -109, 4070, 12, 36, 1, 4, 44)

	paths := []string{
		"/api/observers",
		"/api/observers?region=SJC",
		"/api/observers/obs1",
		"/api/observers/OBS1",
		"/api/observers/obs1/analytics",
		"/api/observers/obs1/analytics?days=1",
		"/api/observers/obs1/analytics?days=30",
		"/api/observers/obs1/analytics?days=bogus",
		"/api/observers/nobody/analytics",
		"/api/observers/obs1/metrics?since=2026-04-01",
		"/api/observers/obs1/metrics?since=2026-04-01&until=2026-04-30T00:00:00Z",
		"/api/observers/obs1/metrics?since=2026-04-01&resolution=1h",
		"/api/observers/obs1/metrics?since=2026-04-01&resolution=1d",
		"/api/observers/obs1/metrics",
		"/api/observers/nobody/metrics",
		"/api/observers/metrics/summary",
		"/api/observers/metrics/summary?window=7d",
		"/api/observers/metrics/summary?window=36h&region=sjc",
		"/api/observers/metrics/summary?region=nowhere",
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			checker.check(t, router, path)
		})
	}

	errorPaths := []struct {
		path   string
		status int
	}{
		{"/api/observers/nobody", http.StatusNotFound},
		{"/api/observers/obs1/metrics?resolution=2h", http.StatusBadRequest},
		{"/api/observers/obs1/metrics?since=yesterday", http.StatusBadRequest},
		{"/api/observers/obs1/metrics?until=yesterday", http.StatusBadRequest},
		{"/api/observers/metrics/summary?window=soon", http.StatusBadRequest},
	}
	for _, e := range errorPaths {
		t.Run(e.path, func(t *testing.T) {
			checker.checkStatus(t, router, e.path, e.status)
		})
	}
}

// TestObserverMetricsSamplesMatchSpec decodes the metrics a seeded observer
// returns and checks the rates, the reboot flag and the null rates after a gap.
func TestObserverMetricsSamplesMatchSpec(t *testing.T) {
	ctx := t.Context()
	checker := newSpecChecker(t)
	srv, router := setupTestServer(t)

	at := func(minute int) time.Time {
		return time.Date(2026, time.April, 5, 0, minute, 0, 0, time.UTC)
	}
	sample := func(minute int, noise float64, battery, tx, rx, errs, sent, recv int) {
		mustCreateTestObserverMetrics(t, ctx, srv.db, database.ObserverMetricsInput{
			ObserverID: "obs1", Timestamp: at(minute),
			NoiseFloor: &noise, BatteryMv: &battery,
			TxAirSecs: &tx, RxAirSecs: &rx, RecvErrors: &errs, PacketsSent: &sent, PacketsRecv: &recv,
		})
	}
	sample(0, -112, 4100, 10, 30, 1, 5, 40)
	sample(5, -110, 4090, 20, 60, 3, 9, 80)
	sample(10, -111, 4080, 2, 6, 0, 1, 4)
	sample(50, -109, 4070, 12, 36, 1, 4, 44)

	rec := checker.checkStatus(t, router, "/api/observers/obs1/metrics?since=2026-04-01", http.StatusOK)
	var got api.ObserverMetricsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v body=%s", err, rec.Body.String())
	}
	if got.ObserverId != "obs1" || got.ObserverName == nil {
		t.Errorf("observer = %q, %v; want obs1 with a name", got.ObserverId, got.ObserverName)
	}
	if len(got.Metrics) != 4 {
		t.Fatalf("metrics = %d samples, want 4", len(got.Metrics))
	}

	// First sample: no previous one, so no rates.
	if got.Metrics[0].TxAirtimePct != nil {
		t.Errorf("first sample has a tx rate: %v", *got.Metrics[0].TxAirtimePct)
	}
	// 10 s of transmit over 300 s is 3.33 %, 30 s of receive is 10 %.
	if p := got.Metrics[1].TxAirtimePct; p == nil || *p != 3.33 {
		t.Errorf("second sample tx_airtime_pct = %v, want 3.33", p)
	}
	if p := got.Metrics[1].RxAirtimePct; p == nil || *p != 10 {
		t.Errorf("second sample rx_airtime_pct = %v, want 10", p)
	}
	// Counters fell: a reboot, with no rates.
	if r := got.Metrics[2].IsRebootSample; r == nil || !*r {
		t.Errorf("third sample is_reboot_sample = %v, want true", r)
	}
	if got.Metrics[2].TxAirtimePct != nil {
		t.Errorf("reboot sample has a tx rate: %v", *got.Metrics[2].TxAirtimePct)
	}
	if len(got.Reboots) != 1 || !got.Reboots[0].Time.Equal(at(10)) {
		t.Errorf("reboots = %v, want [%v]", got.Reboots, at(10))
	}
	// 40 minutes after the third sample is more than two intervals: no rates.
	if got.Metrics[3].TxAirtimePct != nil || got.Metrics[3].IsRebootSample != nil {
		t.Errorf("sample after a gap = %+v, want no rates and no reboot flag", got.Metrics[3])
	}
}

// TestObserverMetricsSummaryMatchesSpec seeds recent samples, one without a
// noise floor, and checks the summary the route computes from them.
func TestObserverMetricsSummaryMatchesSpec(t *testing.T) {
	ctx := t.Context()
	checker := newSpecChecker(t)
	srv, router := setupTestServer(t)

	now := time.Now().UTC().Truncate(time.Minute)
	battery := 4000
	for i, noise := range []float64{-110, -100, -90} {
		nf := noise
		mustCreateTestObserverMetrics(t, ctx, srv.db, database.ObserverMetricsInput{
			ObserverID: "obs1", Timestamp: now.Add(-time.Duration(3-i) * time.Hour),
			NoiseFloor: &nf, BatteryMv: &battery,
		})
	}
	mustCreateTestObserverMetrics(t, ctx, srv.db, database.ObserverMetricsInput{
		ObserverID: "obs2", Timestamp: now.Add(-time.Hour),
	})

	rec := checker.checkStatus(t, router, "/api/observers/metrics/summary", http.StatusOK)
	var got api.MetricsSummaryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v body=%s", err, rec.Body.String())
	}
	if len(got.Observers) != 2 {
		t.Fatalf("observers = %d, want 2", len(got.Observers))
	}
	// Highest maximum noise floor first; the observer without one last.
	first, last := got.Observers[0], got.Observers[1]
	if first.ObserverId != "obs1" || first.SampleCount != 3 {
		t.Errorf("first = %q with %d samples, want obs1 with 3", first.ObserverId, first.SampleCount)
	}
	if first.AvgNoiseFloor24h == nil || *first.AvgNoiseFloor24h != -100 || first.MaxNoiseFloor24h == nil || *first.MaxNoiseFloor24h != -90 {
		t.Errorf("first avg/max = %v/%v, want -100/-90", first.AvgNoiseFloor24h, first.MaxNoiseFloor24h)
	}
	if first.CurrentNoiseFloor == nil || *first.CurrentNoiseFloor != -90 || first.BatteryMv == nil || *first.BatteryMv != 4000 {
		t.Errorf("first current = %v, battery %v, want -90, 4000", first.CurrentNoiseFloor, first.BatteryMv)
	}
	if last.ObserverId != "obs2" || last.MaxNoiseFloor24h != nil || len(last.Sparkline) != 1 || last.Sparkline[0] != nil {
		t.Errorf("last = %+v, want obs2 with no noise floor and a one-entry null sparkline", last)
	}
}

// TestObserverDetailMatchesSpec decodes a seeded observer, including a naive
// clock, and checks the fields the list and the detail route share.
func TestObserverDetailMatchesSpec(t *testing.T) {
	ctx := t.Context()
	checker := newSpecChecker(t)
	srv, router := setupTestServer(t)
	now := time.Now().UTC()
	seedNaiveObserver(t, ctx, srv.db, "naive-obs", "Naive Pi", "SJC", now, 25200, 3, now.Add(-5*time.Minute))

	rec := checker.checkStatus(t, router, "/api/observers/naive-obs", http.StatusOK)
	var got api.Observer
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v body=%s", err, rec.Body.String())
	}
	if got.Id != "naive-obs" || got.Name == nil || *got.Name != "Naive Pi" {
		t.Errorf("observer = %q %v, want naive-obs Naive Pi", got.Id, got.Name)
	}
	if !got.ClockNaive || got.ClockSkewSeconds == nil || *got.ClockSkewSeconds != 25200 || got.ClockSkewCount24h != 3 {
		t.Errorf("clock = naive %v skew %v count %d, want true 25200 3", got.ClockNaive, got.ClockSkewSeconds, got.ClockSkewCount24h)
	}
	if got.ClockLastNaiveAt == nil {
		t.Error("clock_last_naive_at is null")
	}
}

// TestObserverListMatchesSpec checks that the list answers with every
// observer the fixture seeds, each with its status.
func TestObserverListMatchesSpec(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := setupTestServer(t)

	rec := checker.checkStatus(t, router, "/api/observers", http.StatusOK)
	var got api.ObserverListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v body=%s", err, rec.Body.String())
	}
	if len(got.Observers) != 2 {
		t.Fatalf("observers = %d, want 2", len(got.Observers))
	}
	if got.ServerTime.IsZero() {
		t.Error("server_time is zero")
	}
}

// TestObserverAnalyticsRecentPacketsMatchSpec checks the observations the
// analytics route lists: the transmission's fields and the resolved path.
func TestObserverAnalyticsRecentPacketsMatchSpec(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := setupTestServer(t)

	rec := checker.checkStatus(t, router, "/api/observers/obs1/analytics", http.StatusOK)
	var got api.ObserverAnalyticsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v body=%s", err, rec.Body.String())
	}
	if len(got.RecentPackets) == 0 {
		t.Fatal("no recent packets")
	}
	for i, p := range got.RecentPackets {
		if p.Hash == nil || p.PayloadType == nil {
			t.Errorf("recentPackets[%d] has no transmission fields: %+v", i, p)
		}
	}
	if len(got.PacketTypes) == 0 || len(got.Timeline) == 0 || len(got.NodesTimeline) == 0 {
		t.Errorf("empty aggregates: types %d, timeline %d, nodes %d",
			len(got.PacketTypes), len(got.Timeline), len(got.NodesTimeline))
	}
}

func TestProdObserverFixturesMatchSpec(t *testing.T) {
	checkProdFixtures(t, []prodFixture{
		{"observers_prod.json", "ObserverListResponse"},
		{"observer_detail_prod.json", "Observer"},
		{"observer_analytics_prod.json", "ObserverAnalyticsResponse"},
		{"observer_metrics_prod.json", "ObserverMetricsResponse"},
		{"metrics_summary_prod.json", "MetricsSummaryResponse"},
	})
}
