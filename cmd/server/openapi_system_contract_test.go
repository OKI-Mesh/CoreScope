package main

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// pointStatsFileAt makes the ingestor-backed endpoints read path, and drops
// the caches that remember an earlier file.
func pointStatsFileAt(t *testing.T, path string) {
	t.Helper()
	t.Setenv("CORESCOPE_INGESTOR_STATS", path)
	resetIngestorIOCache()
	resetSourceLivenessCache()
}

// TestHealthzNotReadyMatchesSpec checks the 503 /api/healthz answers while the
// server is still starting. TestSystemResponsesDecodeAsSpecTypes covers every
// system endpoint once it is ready.
func TestHealthzNotReadyMatchesSpec(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := setupTestServer(t)

	readiness.Store(0)
	t.Cleanup(func() { readiness.Store(0) })
	checker.checkStatus(t, router, "/api/healthz", http.StatusServiceUnavailable)
}

func TestSystemResponsesDecodeAsSpecTypes(t *testing.T) {
	checker := newSpecChecker(t)
	srv, router := setupTestServer(t)
	pointStatsFileAt(t, filepath.Join(t.TempDir(), "no-such-file.json"))
	readiness.Store(1)
	t.Cleanup(func() { readiness.Store(0) })

	// A slow request on record, so the slow-query lists have an entry.
	srv.perfStats.mu.Lock()
	srv.perfStats.SlowQueries = append(srv.perfStats.SlowQueries, api.SlowQuery{Path: "/api/packets", Ms: 250.5, Time: types.WireTimeSeconds{Time: time.Now().UTC()}, Status: 200})
	srv.perfStats.mu.Unlock()

	var stats api.StatsResponse
	decodeInto(t, checker.checkStatus(t, router, "/api/stats", http.StatusOK).Body.Bytes(), &stats)
	if stats.Engine != "go" || stats.TotalObservers < 1 {
		t.Errorf("stats = %+v", stats)
	}
	if stats.TrackedMB != stats.StoreDataMB {
		t.Errorf("trackedMB %v != storeDataMB %v", stats.TrackedMB, stats.StoreDataMB)
	}

	var health api.HealthResponse
	decodeInto(t, checker.checkStatus(t, router, "/api/health", http.StatusOK).Body.Bytes(), &health)
	if health.Status != "ok" || health.Websocket.RejectedDeny != nil {
		t.Errorf("health = %+v", health)
	}
	foundSlow := false
	for _, q := range health.Perf.RecentSlow {
		if q.Path == "/api/packets" && q.Ms == 250.5 {
			foundSlow = true
		}
	}
	if !foundSlow {
		t.Errorf("recentSlow = %+v, want the /api/packets entry", health.Perf.RecentSlow)
	}

	var healthz api.HealthzResponse
	decodeInto(t, checker.checkStatus(t, router, "/api/healthz", http.StatusOK).Body.Bytes(), &healthz)
	if !healthz.Ready || healthz.IngestLiveness != nil {
		t.Errorf("healthz = %+v", healthz)
	}

	var perf api.PerfResponse
	decodeInto(t, checker.checkStatus(t, router, "/api/perf", http.StatusOK).Body.Bytes(), &perf)
	if perf.PacketStore == nil || perf.DbStats == nil || perf.MemoryBreakdown != nil || perf.MemoryBreakdownNote != nil {
		t.Errorf("perf = %+v", perf)
	}
	if perf.GoRuntime.NumCPU == 0 {
		t.Errorf("goRuntime = %+v", perf.GoRuntime)
	}

	var withMem api.PerfResponse
	decodeInto(t, checker.checkStatus(t, router, "/api/perf?mem=1", http.StatusOK).Body.Bytes(), &withMem)
	if withMem.MemoryBreakdown == nil || withMem.MemoryBreakdownNote == nil {
		t.Errorf("perf?mem=1 has no memoryBreakdown")
	}

	var sources api.PerfWriteSourcesResponse
	decodeInto(t, checker.checkStatus(t, router, "/api/perf/write-sources", http.StatusOK).Body.Bytes(), &sources)
	if len(sources.Sources) != 0 || sources.SampleAt != nil {
		t.Errorf("write-sources without a stats file = %+v", sources)
	}

	var mqtt api.MqttStatusResponse
	decodeInto(t, checker.checkStatus(t, router, "/api/mqtt/status", http.StatusOK).Body.Bytes(), &mqtt)
	if len(mqtt.Sources) != 0 || mqtt.SampleAt != nil {
		t.Errorf("mqtt status without a stats file = %+v", mqtt)
	}
}

// TestIngestorStatusEndpointsMatchSpec serves the endpoints that read the
// ingestor's stats file from a populated file.
func TestIngestorStatusEndpointsMatchSpec(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := setupTestServer(t)
	readiness.Store(1)
	t.Cleanup(func() { readiness.Store(0) })

	sampled := time.Now().UTC().Truncate(time.Second)
	stamp := sampled.Format(time.RFC3339)
	stub := map[string]any{
		"sampledAt":          stamp,
		"tx_inserted":        10,
		"tx_dupes":           3,
		"obs_inserted":       25,
		"node_upserts":       4,
		"observer_upserts":   5,
		"write_errors":       0,
		"sig_drops":          2,
		"walCommits":         25,
		"groupCommitFlushes": 1,
		"backfillUpdates":    map[string]int{"path_json": 7},
		"procIO": map[string]any{
			"readBytesPerSec": 1.5, "writeBytesPerSec": 2.5, "cancelledWriteBytesPerSec": 0,
			"syscallsRead": 3, "syscallsWrite": 4, "sampledAt": stamp,
		},
		"source_liveness": map[string]any{
			"Broker A": map[string]any{"lastReceiptUnix": 1794229439, "lastMessageUnix": 1794229438},
			"Broker B": map[string]any{"lastReceiptUnix": 0, "lastMessageUnix": 0},
		},
		"source_statuses": []map[string]any{
			{
				"name": "Broker A", "broker": "mqtt://user:secret-password@broker.example.org:1883",
				"connected": true, "lastConnectUnix": 1794213400, "lastDisconnectUnix": 0, "lastPacketUnix": 1794229479,
				"connectCount": 2, "disconnectCount": 1, "packetsTotal": 1000, "packetsLast5m": 12,
				"lastError": "dial mqtt://user:secret-password@broker.example.org:1883: refused",
			},
			{
				"name": "Broker B", "broker": "wss://broker.example.net:9002",
				"connected": false, "lastConnectUnix": 0, "lastDisconnectUnix": 1794229000, "lastPacketUnix": 0,
				"connectCount": 0, "disconnectCount": 3, "packetsTotal": 0, "packetsLast5m": 0,
			},
		},
		"watchdogLastTickUnix": 1794229420,
		"watchdogPanicCount":   1,
		"watchdogLogDropCount": 2,
	}
	data, err := json.Marshal(stub)
	if err != nil {
		t.Fatal(err)
	}
	statsPath := filepath.Join(t.TempDir(), "ingestor-stats.json")
	if err := os.WriteFile(statsPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	pointStatsFileAt(t, statsPath)

	var sources api.PerfWriteSourcesResponse
	decodeInto(t, checker.checkStatus(t, router, "/api/perf/write-sources", http.StatusOK).Body.Bytes(), &sources)
	if sources.Sources["tx_inserted"] != 10 || sources.Sources["backfill_path_json"] != 7 || sources.Sources["sig_drops"] != 2 {
		t.Errorf("sources = %v", sources.Sources)
	}
	if sources.SampleAt == nil || !sources.SampleAt.Equal(sampled) {
		t.Errorf("sampleAt = %v, want %v", sources.SampleAt, sampled)
	}

	var mqtt api.MqttStatusResponse
	body := checker.checkStatus(t, router, "/api/mqtt/status", http.StatusOK).Body.Bytes()
	decodeInto(t, body, &mqtt)
	if len(mqtt.Sources) != 2 || mqtt.Sources[0].Broker != "mqtt://user:****@broker.example.org:1883" {
		t.Errorf("sources = %+v", mqtt.Sources)
	}
	if mqtt.Sources[0].LastError == nil || mqtt.Sources[1].LastError != nil {
		t.Errorf("lastError = %v, %v", mqtt.Sources[0].LastError, mqtt.Sources[1].LastError)
	}
	if mqtt.WatchdogLastTickUnix == nil || *mqtt.WatchdogLastTickUnix != 1794229420 || mqtt.WatchdogPanicCount == nil || mqtt.WatchdogLogDropCount == nil {
		t.Errorf("watchdog fields = %+v", mqtt)
	}

	var io api.PerfIOResponse
	decodeInto(t, checker.checkStatus(t, router, "/api/perf/io", http.StatusOK).Body.Bytes(), &io)
	if io.Ingestor == nil || io.Ingestor.ReadBytesPerSec != 1.5 || io.Ingestor.SampledAt == nil || !io.Ingestor.SampledAt.Equal(sampled) {
		t.Errorf("ingestor = %+v", io.Ingestor)
	}

	var healthz api.HealthzResponse
	decodeInto(t, checker.checkStatus(t, router, "/api/healthz", http.StatusOK).Body.Bytes(), &healthz)
	if healthz.IngestLiveness == nil || (*healthz.IngestLiveness)["Broker A"].LastMessageUnix != 1794229438 || len(*healthz.IngestLiveness) != 2 {
		t.Errorf("ingest_liveness = %v", healthz.IngestLiveness)
	}
}

func decodeInto(t *testing.T, body []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(body, v); err != nil {
		t.Fatalf("decode: %v body=%s", err, body)
	}
}

func TestProdSystemFixturesMatchSpec(t *testing.T) {
	checkProdFixtures(t, []prodFixture{
		{"stats_prod.json", "StatsResponse"},
		{"health_prod.json", "HealthResponse"},
		{"healthz_prod.json", "HealthzResponse"},
		{"perf_prod.json", "PerfResponse"},
		{"perf_mem_prod.json", "PerfResponse"},
		{"perf_io_prod.json", "PerfIOResponse"},
		{"perf_write_sources_prod.json", "PerfWriteSourcesResponse"},
		{"mqtt_status_prod.json", "MqttStatusResponse"},
	})
}
