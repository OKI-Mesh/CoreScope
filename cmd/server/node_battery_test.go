package main

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/gorilla/mux"
)

// TestGetNodeBatteryHistory_FromObserverMetrics validates that the DB layer
// can pull a node's battery_mv time-series from observer_metrics, joining
// observers.id (uppercase hex pubkey) to nodes.public_key (lowercase hex).
func TestGetNodeBatteryHistory_FromObserverMetrics(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	now := time.Now().UTC()

	// node + observer with matching pubkey (cases differ on purpose)
	pkLower := "deadbeefcafef00d11223344"
	idUpper := strings.ToUpper(pkLower)
	mustCreateTestNode(t, ctx, db, database.NodeInput{PublicKey: pkLower, Name: "BatNode", Role: "repeater", RxTime: now, IngestNow: now})
	mustCreateTestObserver(t, ctx, db, idUpper, "BatNode", "", now)

	// 3 metrics samples: 3700, 3500, 3200 mV
	for i, mv := range []int{3700, 3500, 3200} {
		ts := now.Add(time.Duration(-2+i) * time.Hour)
		if err := db.CreateOrUpdateObserverMetrics(ctx, database.ObserverMetricsInput{
			ObserverID: strings.ToLower(idUpper),
			Timestamp:  ts,
			BatteryMv:  &mv,
		}); err != nil {
			t.Fatalf("CreateOrUpdateObserverMetrics: %v", err)
		}
	}
	// One sample with NULL battery should be skipped
	if err := db.CreateOrUpdateObserverMetrics(ctx, database.ObserverMetricsInput{
		ObserverID: strings.ToLower(idUpper),
		Timestamp:  now.Add(-3 * time.Hour),
	}); err != nil {
		t.Fatalf("CreateOrUpdateObserverMetrics(null battery): %v", err)
	}

	samples, err := db.GetNodeBatteryHistory(ctx, pkLower, now.Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("GetNodeBatteryHistory: %v", err)
	}
	if len(samples) != 3 {
		t.Fatalf("expected 3 samples, got %d", len(samples))
	}
	if samples[0].BatteryMv != 3700 || samples[2].BatteryMv != 3200 {
		t.Errorf("samples=%+v", samples)
	}
}

// TestNodeBatteryEndpoint validates the /api/nodes/{pubkey}/battery endpoint
// returns time-series data plus configured thresholds and a status flag.
func TestNodeBatteryEndpoint(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)

	now := time.Now().UTC()
	pkLower := "aabbccdd11223344"
	idUpper := strings.ToUpper(pkLower)
	mustCreateTestObserver(t, ctx, db, idUpper, "TestRepeater", "", now)
	for i, mv := range []int{3800, 3600, 3200} {
		ts := now.Add(time.Duration(-2+i) * time.Hour)
		if err := db.CreateOrUpdateObserverMetrics(ctx, database.ObserverMetricsInput{
			ObserverID: strings.ToLower(idUpper),
			Timestamp:  ts,
			BatteryMv:  &mv,
		}); err != nil {
			t.Fatalf("CreateOrUpdateObserverMetrics: %v", err)
		}
	}

	cfg := &Config{Port: 3000}
	hub := NewHub()
	srv := NewServer(db, cfg, hub)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	srv.store = store
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	req := httptest.NewRequest("GET", "/api/nodes/"+pkLower+"/battery?days=7", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}
	var body api.NodeBatteryResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Samples) != 3 {
		t.Errorf("expected 3 samples, got %d", len(body.Samples))
	}
	if body.Thresholds.LowMv != 3300 {
		t.Errorf("default low_mv expected 3300, got %d", body.Thresholds.LowMv)
	}
	if body.Thresholds.CriticalMv != 3000 {
		t.Errorf("default critical_mv expected 3000, got %d", body.Thresholds.CriticalMv)
	}
	// latest 3200 -> "low" (below 3300, above 3000)
	if body.Status != api.BatteryStatus("low") {
		t.Errorf("expected status=low, got %v", body.Status)
	}
	if body.LatestMv == nil || *body.LatestMv != 3200 {
		t.Errorf("latest_mv expected 3200, got %v", body.LatestMv)
	}
	if body.LatestTs == nil || !body.LatestTs.Equal(body.Samples[2].Timestamp.Time) {
		t.Errorf("latest_ts must be the newest sample's timestamp, got %v", body.LatestTs)
	}
}

// TestNodeBatteryEndpoint_NoData returns 200 with empty samples and status="unknown".
func TestNodeBatteryEndpoint_NoData(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/nodes/aabbccdd11223344/battery", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body api.NodeBatteryResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Status != api.BatteryStatus("unknown") {
		t.Errorf("expected unknown when no samples, got %v", body.Status)
	}
	if body.LatestMv != nil || body.LatestTs != nil {
		t.Errorf("latest_mv/latest_ts must be null with no samples: %+v", body)
	}
	if body.Samples == nil || len(body.Samples) != 0 {
		t.Errorf("samples must be an empty list, got %#v", body.Samples)
	}
}

// TestNodeBatteryEndpoint_404 returns 404 for unknown node.
func TestNodeBatteryEndpoint_404(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/nodes/notarealnode00000000/battery", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 404 {
		t.Errorf("expected 404, got %d, body: %s", w.Code, w.Body.String())
	}
}

// TestBatteryThresholds_ConfigOverride confirms config overrides take effect.
func TestBatteryThresholds_ConfigOverride(t *testing.T) {
	cfg := &Config{
		BatteryThresholds: &BatteryThresholdsConfig{LowMv: 3500, CriticalMv: 3100},
	}
	if cfg.LowBatteryMv() != 3500 {
		t.Errorf("LowBatteryMv override failed: %d", cfg.LowBatteryMv())
	}
	if cfg.CriticalBatteryMv() != 3100 {
		t.Errorf("CriticalBatteryMv override failed: %d", cfg.CriticalBatteryMv())
	}

	empty := &Config{}
	if empty.LowBatteryMv() != 3300 {
		t.Errorf("default LowBatteryMv expected 3300, got %d", empty.LowBatteryMv())
	}
	if empty.CriticalBatteryMv() != 3000 {
		t.Errorf("default CriticalBatteryMv expected 3000, got %d", empty.CriticalBatteryMv())
	}
}
