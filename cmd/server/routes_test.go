package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/gorilla/mux"
)

type advertPacket struct {
	Hash        string
	RawHex      string
	RouteType   types.RouteType
	PayloadType types.PayloadType
	Name        string
	PubKey      string
	Role        types.NodeRole
	At          time.Time
	Hops        []string
}

type hashCollisionsBody struct {
	BySize map[string]struct {
		Stats struct {
			CollisionCount int `json:"collision_count"`
		} `json:"stats"`
		Collisions []struct {
			Prefix         string            `json:"prefix"`
			Classification string            `json:"classification"`
			Nodes          []json.RawMessage `json:"nodes"`
		} `json:"collisions"`
	} `json:"by_size"`
	InconsistentNodes []struct {
		Role string `json:"role"`
	} `json:"inconsistent_nodes"`
}

func padKey(prefix string) string {
	return prefix + strings.Repeat("0", 64-len(prefix))
}

func seedNode(t *testing.T, db *database.DB, pk string, name string, role types.NodeRole, lat, lon *float64, at time.Time) {
	t.Helper()
	if err := db.CreateOrUpdateNode(t.Context(), database.NodeInput{
		PublicKey: pk, Name: name, Role: role, Lat: lat, Lon: lon, RxTime: at, IngestNow: at,
	}); err != nil {
		t.Fatalf("seed node %q: %v", name, err)
	}
}

func seedObserver(t *testing.T, db *database.DB, id, iata string, at time.Time) int64 {
	t.Helper()
	idx, err := db.CreateOrUpdateObserver(t.Context(), database.ObserverInput{
		ID: id, Name: "observer-" + iata, IATA: iata, Meta: nil, LastSeen: at, IngestNow: at,
	})
	if err != nil {
		t.Fatalf("seed observer %s: %v", iata, err)
	}
	return idx
}

func loadedStore(t *testing.T, db *database.DB) *PacketStore {
	t.Helper()
	ctx := t.Context()
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	if !store.WaitIndexesReady(5 * time.Second) {
		t.Fatal("indexes never became ready")
	}
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache(): %v", err)
	}
	return store
}

func seedAdvertSeries(t *testing.T, db *database.DB, observerIdx int64, hashPrefix, pk, name string, role types.NodeRole, raws []string, start time.Time, step time.Duration) {
	t.Helper()
	for i, raw := range raws {
		header, err := strconv.ParseUint(raw[:2], 16, 8)
		if err != nil {
			t.Fatalf("raw %q: %v", raw, err)
		}
		insertAdvert(t, db, observerIdx, advertPacket{
			Role:        role,
			Hash:        fmt.Sprintf("%s%012x", hashPrefix, i),
			RawHex:      raw,
			RouteType:   types.RouteType(header & 0x03),
			PayloadType: types.PayloadType(4),
			Name:        name,
			PubKey:      pk,
			At:          start.Add(time.Duration(i) * step),
		})
	}
}

func seedNoNameNode(t *testing.T, db *database.DB, role types.NodeRole, lat, lon *float64) string {
	t.Helper()
	now := time.Now().UTC()
	pk := padKey("deadbeef12345678")
	seedNode(t, db, pk, "", role, lat, lon, now.Add(-1*time.Hour))
	obsIdx := seedObserver(t, db, scopeKey(102), "SFO", now)
	insertAdvert(t, db, obsIdx, advertPacket{
		Role:        types.RoleRepeater,
		Hash:        "de0b000000000001",
		RawHex:      "ddee",
		RouteType:   1,
		PayloadType: types.PayloadType(4),
		Name:        "",
		PubKey:      pk,
		At:          now.Add(-5 * time.Minute),
		Hops:        []string{"dd"},
	})
	return pk
}

func serverFor(t *testing.T, db *database.DB, store *PacketStore) (*Server, *mux.Router) {
	t.Helper()
	srv := NewServer(db, &Config{Port: 3000}, NewHub())
	srv.store = store
	router := mux.NewRouter()
	srv.RegisterRoutes(router)
	return srv, router
}

func insertAdvert(t *testing.T, db *database.DB, observerIdx int64, a advertPacket) int64 {
	t.Helper()
	ctx := t.Context()
	if !a.Role.Valid() {
		t.Fatalf("advert %s: invalid role %q", a.Hash, a.Role)
	}
	txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		Hash:        a.Hash,
		RawHex:      a.RawHex,
		RouteType:   a.RouteType,
		PayloadType: a.PayloadType,
		DecodedJSON: fmt.Sprintf(`{"name":%q,"pubKey":%q}`, a.Name, a.PubKey),
		Advert:      &database.NodeInput{PublicKey: a.PubKey, Name: a.Name, Role: a.Role, RxTime: a.At, IngestNow: a.At},
	}, a.At)
	if err != nil {
		t.Fatalf("seed transmission %s: %v", a.Hash, err)
	}
	_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: txID,
		ObserverIdx:    iptr64(observerIdx),
		SNR:            fptr(5.0),
		RSSI:           fptr(-90.0),
		Timestamp:      a.At,
	}, a.Hops, make([]*string, len(a.Hops)))
	if err != nil {
		t.Fatalf("seed observation %s: %v", a.Hash, err)
	}
	if !inserted {
		t.Fatalf("expected observation to be inserted for tx %d", txID)
	}
	return txID
}

func setupTestServer(t *testing.T) (*Server, *mux.Router) {
	t.Helper()
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)
	cfg := &Config{Port: 3000, SnapshotDir: t.TempDir()}
	hub := NewHub()
	srv := NewServer(db, cfg, hub)
	store := newPacketStoreForTest(t, db)
	store.config = cfg
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache(): %v", err)
	}
	if !store.WaitIndexesReady(5 * time.Second) {
		t.Fatalf("background indexes never became ready")
	}
	srv.store = store
	router := mux.NewRouter()
	srv.RegisterRoutes(router)
	return srv, router
}

func setupTestServerWithAPIKey(t *testing.T, apiKey string) (*Server, *mux.Router) {
	t.Helper()
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)
	cfg := &Config{Port: 3000, APIKey: apiKey, SnapshotDir: t.TempDir()}
	hub := NewHub()
	srv := NewServer(db, cfg, hub)
	store := newPacketStoreForTest(t, db)
	store.config = cfg
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache(): %v", err)
	}
	if !store.WaitIndexesReady(5 * time.Second) {
		t.Fatalf("background indexes never became ready")
	}
	srv.store = store
	router := mux.NewRouter()
	srv.RegisterRoutes(router)
	return srv, router
}

func TestWriteEndpointsRequireAPIKey(t *testing.T) {
	_, router := setupTestServerWithAPIKey(t, "test-secret-key-strong-enough")

	t.Run("missing key returns 401", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/api/perf/reset", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", w.Code)
		}
		var body map[string]interface{}
		_ = json.Unmarshal(w.Body.Bytes(), &body)
		if body["error"] != "unauthorized" {
			t.Fatalf("expected unauthorized error, got %v", body["error"])
		}
	})

	t.Run("wrong key returns 401", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/api/perf/reset", nil)
		req.Header.Set("X-API-Key", "wrong-secret-key-strong-enough")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", w.Code)
		}
	})

	t.Run("correct key passes", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/api/perf/reset", nil)
		req.Header.Set("X-API-Key", "test-secret-key-strong-enough")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
		}
	})

	t.Run("decode works without key", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/api/decode", bytes.NewBufferString(`{"hex":"0200"}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 for decode without key, got %d (body: %s)", w.Code, w.Body.String())
		}
	})
}

func TestWriteEndpointsBlockWhenAPIKeyEmpty(t *testing.T) {
	_, router := setupTestServerWithAPIKey(t, "")

	req := httptest.NewRequest("POST", "/api/perf/reset", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 with empty apiKey, got %d (body: %s)", w.Code, w.Body.String())
	}

	// decode should still work even with empty apiKey
	req2 := httptest.NewRequest("POST", "/api/decode", bytes.NewBufferString(`{"hex":"0200"}`))
	req2.Header.Set("Content-Type", "application/json")
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200 for decode with empty apiKey, got %d (body: %s)", w2.Code, w2.Body.String())
	}
}

func TestHealthEndpoint(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/health", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	if body["status"] != "ok" {
		t.Errorf("expected status ok, got %v", body["status"])
	}
	if body["engine"] != "go" {
		t.Errorf("expected engine go, got %v", body["engine"])
	}
	if _, ok := body["version"]; !ok {
		t.Error("expected version field in health response")
	}
	if _, ok := body["commit"]; !ok {
		t.Error("expected commit field in health response")
	}
	if bt, ok := body["buildTime"]; !ok || bt == nil {
		t.Error("expected non-nil buildTime field in health response")
	}

	// Verify memory has spec-defined fields (no heapMB or goRuntime per api-spec.md)
	mem, ok := body["memory"].(map[string]interface{})
	if !ok {
		t.Fatal("expected memory object in health response")
	}
	for _, field := range []string{"rss", "heapUsed", "heapTotal", "external"} {
		if _, ok := mem[field]; !ok {
			t.Errorf("expected %s in memory", field)
		}
	}
	if _, ok := mem["heapMB"]; ok {
		t.Error("heapMB should not be in memory (removed per api-spec.md)")
	}
	if _, ok := body["goRuntime"]; ok {
		t.Error("goRuntime should not be in health response (removed per api-spec.md)")
	}

	// Verify real packetStore stats (not zeros)
	pktStore, ok := body["packetStore"].(map[string]interface{})
	if !ok {
		t.Fatal("expected packetStore object in health response")
	}
	if _, ok := pktStore["packets"]; !ok {
		t.Error("expected packets in packetStore")
	}
	if _, ok := pktStore["estimatedMB"]; !ok {
		t.Error("expected estimatedMB in packetStore")
	}
	if _, ok := pktStore["trackedMB"]; !ok {
		t.Error("expected trackedMB in packetStore")
	}

	// Verify eventLoop (GC pause metrics matching Node.js shape)
	el, ok := body["eventLoop"].(map[string]interface{})
	if !ok {
		t.Fatal("expected eventLoop object in health response")
	}
	for _, field := range []string{"currentLagMs", "maxLagMs", "p50Ms", "p95Ms", "p99Ms"} {
		if _, ok := el[field]; !ok {
			t.Errorf("expected %s in eventLoop", field)
		}
	}

	// Verify cache has real structure
	cache, ok := body["cache"].(map[string]interface{})
	if !ok {
		t.Fatal("expected cache object in health response")
	}
	if _, ok := cache["entries"]; !ok {
		t.Error("expected entries in cache")
	}
	if _, ok := cache["hitRate"]; !ok {
		t.Error("expected hitRate in cache")
	}
}

func TestStatsEndpoint(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/stats", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	if body["totalTransmissions"] != float64(3) {
		t.Errorf("expected 3 transmissions, got %v", body["totalTransmissions"])
	}
	if body["totalNodes"] != float64(3) {
		t.Errorf("expected 3 nodes, got %v", body["totalNodes"])
	}
	if body["engine"] != "go" {
		t.Errorf("expected engine go, got %v", body["engine"])
	}
	if _, ok := body["version"]; !ok {
		t.Error("expected version field in stats response")
	}
	if _, ok := body["commit"]; !ok {
		t.Error("expected commit field in stats response")
	}
	if bt, ok := body["buildTime"]; !ok || bt == nil {
		t.Error("expected non-nil buildTime field in stats response")
	}
	// Regression (#1546): server-side async backfill was removed in #1289
	// (backfill moved to the ingestor). The dead "backfilling"/"backfillProgress"
	// fields had no writer and reported backfilling:true forever. They must not
	// reappear in the response, and the X-CoreScope-Status header is gone with them.
	if _, ok := body["backfilling"]; ok {
		t.Error("stats response must not carry the removed backfilling flag (#1546)")
	}
	if _, ok := body["backfillProgress"]; ok {
		t.Error("stats response must not carry the removed backfillProgress field (#1546)")
	}
	if got := w.Header().Get("X-CoreScope-Status"); got != "" {
		t.Errorf("X-CoreScope-Status header must not be set (#1546), got %q", got)
	}
}

func TestPacketsEndpoint(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/packets?limit=10", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	packets, ok := body["packets"].([]interface{})
	if !ok {
		t.Fatal("expected packets array")
	}
	if len(packets) != 3 {
		t.Errorf("expected 3 packets (transmissions), got %d", len(packets))
	}
}

func TestPacketsGrouped(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/packets?groupByHash=true&limit=10", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	packets, ok := body["packets"].([]interface{})
	if !ok {
		t.Fatal("expected packets array")
	}
	if len(packets) != 3 {
		t.Errorf("expected 3 grouped packets, got %d", len(packets))
	}
}

func TestNodesEndpoint(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/nodes?limit=50", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	nodes, ok := body["nodes"].([]interface{})
	if !ok {
		t.Fatal("expected nodes array")
	}
	if len(nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(nodes))
	}
	total := body["total"].(float64)
	if total != 3 {
		t.Errorf("expected total 3, got %v", total)
	}
}

func TestNodeDetailEndpoint(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/nodes/aabbccdd11223344", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	node, ok := body["node"].(map[string]interface{})
	if !ok {
		t.Fatal("expected node object")
	}
	if node["name"] != "TestRepeater" {
		t.Errorf("expected TestRepeater, got %v", node["name"])
	}
}

func TestNodeDetail404(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/nodes/nonexistent", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 404 {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestNodeSearchEndpoint(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/nodes/search?q=Repeater", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	nodes, ok := body["nodes"].([]interface{})
	if !ok {
		t.Fatal("expected nodes array")
	}
	if len(nodes) != 1 {
		t.Errorf("expected 1 node matching 'Repeater', got %d", len(nodes))
	}
}

func TestNetworkStatusEndpoint(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/nodes/network-status", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	if body["total"] != float64(3) {
		t.Errorf("expected 3 total, got %v", body["total"])
	}
}

func TestObserversEndpoint(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/observers", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	observers, ok := body["observers"].([]interface{})
	if !ok {
		t.Fatal("expected observers array")
	}
	if len(observers) != 2 {
		t.Errorf("expected 2 observers, got %d", len(observers))
	}
}

func TestObserverDetail404(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/observers/nonexistent", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 404 {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestChannelsEndpoint(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/channels", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	channels, ok := body["channels"].([]interface{})
	if !ok {
		t.Fatal("expected channels array")
	}
	if len(channels) != 1 {
		t.Errorf("expected 1 channel, got %d", len(channels))
	}
}

func TestTracesEndpoint(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/traces/abc123def4567890", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	traces, ok := body["traces"].([]interface{})
	if !ok {
		t.Fatal("expected traces array")
	}
	if len(traces) != 2 {
		t.Errorf("expected 2 traces, got %d", len(traces))
	}
}

func TestConfigCacheEndpoint(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/config/cache", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestConfigThemeEndpoint(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/config/theme", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	if body["branding"] == nil {
		t.Error("expected branding in theme response")
	}
}

func TestConfigMapEndpoint(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/config/map", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	if body["zoom"] == nil {
		t.Error("expected zoom in map response")
	}
}

func TestPerfEndpoint(t *testing.T) {
	_, router := setupTestServer(t)
	// Make a request first to generate perf data
	req1 := httptest.NewRequest("GET", "/api/health", nil)
	w1 := httptest.NewRecorder()
	router.ServeHTTP(w1, req1)

	req := httptest.NewRequest("GET", "/api/perf", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)

	// Verify goRuntime IS present with expected fields
	goRuntime, ok := body["goRuntime"].(map[string]interface{})
	if !ok {
		t.Fatal("expected goRuntime object in perf response")
	}
	for _, field := range []string{"goroutines", "numGC", "pauseTotalMs", "lastPauseMs", "heapAllocMB", "heapSysMB", "heapInuseMB", "heapIdleMB", "numCPU"} {
		if _, ok := goRuntime[field]; !ok {
			t.Errorf("expected %s in goRuntime", field)
		}
	}
	// Verify status, uptimeHuman, websocket are NOT present
	for _, removed := range []string{"status", "uptimeHuman", "websocket"} {
		if _, ok := body[removed]; ok {
			t.Errorf("%s should not be in perf response (removed per api-spec.md)", removed)
		}
	}

	// Verify cache stats (real, not hardcoded zeros)
	cache, ok := body["cache"].(map[string]interface{})
	if !ok {
		t.Fatal("expected cache object in perf response")
	}
	for _, field := range []string{"size", "hits", "misses", "hitRate"} {
		if _, ok := cache[field]; !ok {
			t.Errorf("expected %s in cache", field)
		}
	}

	// Verify packetStore stats
	if _, ok := body["packetStore"]; !ok {
		t.Error("expected packetStore in perf response")
	}

	// Verify dbStats
	dbStats, ok := body["dbStats"].(map[string]interface{})
	if !ok {
		t.Fatal("expected dbStats object in perf response")
	}
	for _, field := range []string{"dbSizeMB", "walSizeMB", "deadTuples", "rows"} {
		if _, ok := dbStats[field]; !ok {
			t.Errorf("expected %s in dbStats", field)
		}
	}

	// Verify standard fields still present
	if _, ok := body["uptime"]; !ok {
		t.Error("expected uptime in perf response")
	}
	if _, ok := body["endpoints"]; !ok {
		t.Error("expected endpoints in perf response")
	}
}

func TestAnalyticsRFEndpoint(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/analytics/rf", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestResolveHopsEndpoint(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/resolve-hops?hops=aabb,eeff", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	resolved, ok := body["resolved"].(map[string]interface{})
	if !ok {
		t.Fatal("expected resolved map")
	}
	// aabb should resolve to TestRepeater
	aabb, ok := resolved["aabb"].(map[string]interface{})
	if !ok {
		t.Fatal("expected aabb in resolved")
	}
	if aabb["name"] != "TestRepeater" {
		t.Errorf("expected TestRepeater for aabb, got %v", aabb["name"])
	}
}

func TestPacketTimestampsRequiresSince(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/packets/timestamps", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 400 {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestContentTypeJSON(t *testing.T) {
	_, router := setupTestServer(t)
	endpoints := []string{
		"/api/health", "/api/stats", "/api/nodes", "/api/packets",
		"/api/observers", "/api/channels",
	}
	for _, ep := range endpoints {
		req := httptest.NewRequest("GET", ep, nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		ct := w.Header().Get("Content-Type")
		if ct != "application/json" {
			t.Errorf("%s: expected application/json, got %s", ep, ct)
		}
	}
}

func TestPacketDetailByHash(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/packets/abc123def4567890", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	pkt, ok := body["packet"].(map[string]interface{})
	if !ok {
		t.Fatal("expected packet object")
	}
	if pkt["hash"] != "abc123def4567890" {
		t.Errorf("expected hash abc123def4567890, got %v", pkt["hash"])
	}
	if body["observation_count"] == nil {
		t.Error("expected observation_count")
	}
}

func TestPacketDetailByNumericID(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/packets/1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	if body["packet"] == nil {
		t.Error("expected packet object")
	}
}

func TestPacketDetailNotFound(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/packets/notahash12345678", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// "notahash12345678" is 16 hex chars, will try hash lookup first, then fail
	if w.Code != 404 {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestPacketDetailNumericNotFound(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/packets/99999", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 404 {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestPacketTimestampsWithSince(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/packets/timestamps?since=2020-01-01", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestPacketTimestampsAcceptsDateAndTimestamp(t *testing.T) {
	_, router := setupTestServer(t)
	for _, since := range []string{"2020-01-01", "2020-01-01T00:00:00Z", "2020-01-01T00:00:00.123Z"} {
		req := httptest.NewRequest("GET", "/api/packets/timestamps?since="+since, nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Errorf("since=%s: expected 200, got %d", since, w.Code)
		}
	}
	req := httptest.NewRequest("GET", "/api/packets/timestamps?since=yesterday", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 400 {
		t.Errorf("since=yesterday: expected 400, got %d", w.Code)
	}
}

func TestNodeDetailWithRecentAdverts(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/nodes/aabbccdd11223344", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	if body["recentAdverts"] == nil {
		t.Error("expected recentAdverts in response")
	}
	node, ok := body["node"].(map[string]interface{})
	if !ok {
		t.Fatal("expected node object")
	}
	if node["name"] != "TestRepeater" {
		t.Errorf("expected TestRepeater, got %v", node["name"])
	}
}

// TestNodeHealthPartialFromPackets verifies that a node with packets in the
// in-memory store but no DB entry returns a partial 200 response instead of 404.
// This is the fix for issue #665 (companion nodes without adverts).
func TestNodeHealthPartialFromPackets(t *testing.T) {
	srv, router := setupTestServer(t)

	// Inject a packet into byNode for a pubkey that doesn't exist in the nodes table
	ghostPubkey := "ghost_companion_no_advert"
	now := time.Now().UTC()
	snr := 5.0
	srv.store.mu.Lock()
	if srv.store.byNode == nil {
		srv.store.byNode = make(map[string][]*StoreTx)
	}
	if srv.store.nodeHashes == nil {
		srv.store.nodeHashes = make(map[string]map[string]bool)
	}
	srv.store.byNode[ghostPubkey] = []*StoreTx{
		{Hash: "abc123", FirstSeen: now, SNR: &snr, ObservationCount: 1},
	}
	srv.store.nodeHashes[ghostPubkey] = map[string]bool{"abc123": true}
	srv.store.mu.Unlock()

	req := httptest.NewRequest("GET", "/api/nodes/"+ghostPubkey+"/health", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200 for ghost companion, got %d (body: %s)", w.Code, w.Body.String())
	}

	var body map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("json unmarshal: %v", err)
	}

	// Should have a synthetic node stub
	node, ok := body["node"].(map[string]interface{})
	if !ok || node == nil {
		t.Fatal("expected node in response")
	}
	if node["role"] != "unknown" {
		t.Errorf("expected role=unknown, got %v", node["role"])
	}
	if node["public_key"] != ghostPubkey {
		t.Errorf("expected public_key=%s, got %v", ghostPubkey, node["public_key"])
	}

	// Should have stats from the packet
	stats, ok := body["stats"].(map[string]interface{})
	if !ok || stats == nil {
		t.Fatal("expected stats in response")
	}
	if stats["totalPackets"] != 1.0 { // JSON numbers are float64
		t.Errorf("expected totalPackets=1, got %v", stats["totalPackets"])
	}
	if stats["lastHeard"] == nil {
		t.Error("expected lastHeard to be set")
	}
}

func TestBulkHealthEndpoint(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/nodes/bulk-health?limit=10", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body []interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	if len(body) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(body))
	}
}

func TestBulkHealthLimitCap(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/nodes/bulk-health?limit=999", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestNodeAnalytics(t *testing.T) {
	_, router := setupTestServer(t)

	t.Run("default days", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/nodes/aabbccdd11223344/analytics", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != 200 {
			t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
		}
		var body map[string]interface{}
		json.Unmarshal(w.Body.Bytes(), &body)
		if body["timeRange"] == nil {
			t.Error("expected timeRange")
		}
		if body["activityTimeline"] == nil {
			t.Error("expected activityTimeline")
		}
	})

	t.Run("custom days", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/nodes/aabbccdd11223344/analytics?days=30", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})

	t.Run("clamp days below 1", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/nodes/aabbccdd11223344/analytics?days=0", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})

	t.Run("clamp days above 365", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/nodes/aabbccdd11223344/analytics?days=999", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})

	t.Run("not found", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/nodes/nonexistent/analytics", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != 404 {
			t.Errorf("expected 404, got %d", w.Code)
		}
	})
}

func TestObserverDetailFound(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/observers/obs1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	if body["id"] != "obs1" {
		t.Errorf("expected obs1, got %v", body["id"])
	}
}

func TestObserverAnalytics(t *testing.T) {
	_, router := setupTestServer(t)

	t.Run("default", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/observers/obs1/analytics", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
		var body map[string]interface{}
		json.Unmarshal(w.Body.Bytes(), &body)
		if body["packetTypes"] == nil {
			t.Error("expected packetTypes")
		}
		if body["recentPackets"] == nil {
			t.Error("expected recentPackets")
		}
		recent, ok := body["recentPackets"].([]interface{})
		if !ok || len(recent) == 0 {
			t.Errorf("expected non-empty recentPackets, got %v", body["recentPackets"])
		}

		// #1827: packetTypes must still reflect the real per-transmission
		// payload_type after switching the hot loop from enrichObs() to a
		// direct s.store.byTxID read. seedTestData gives obs1 three
		// observations: tx1 (payload_type=4), tx2 (payload_type=5), tx3
		// (payload_type=4) — i.e. counts {"4":2, "5":1}.
		pt, ok := body["packetTypes"].(map[string]interface{})
		if !ok {
			t.Fatalf("expected packetTypes to be an object, got %T", body["packetTypes"])
		}
		if pt["4"] != float64(2) {
			t.Errorf("expected packetTypes[4]=2, got %v", pt["4"])
		}
		if pt["5"] != float64(1) {
			t.Errorf("expected packetTypes[5]=1, got %v", pt["5"])
		}

		// recentPackets is still built via the unchanged enrichObs() path
		// and should retain resolved_path for observations that have one
		// (tx1's first observation, resolved via obs1).
		foundResolved := false
		for _, rp := range recent {
			m, ok := rp.(map[string]interface{})
			if !ok {
				continue
			}
			if m["resolved_path"] != nil {
				foundResolved = true
				break
			}
		}
		if !foundResolved {
			t.Error("expected at least one recentPackets entry to carry resolved_path")
		}
	})

	t.Run("custom days", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/observers/obs1/analytics?days=1", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})

	t.Run("days greater than 7", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/observers/obs1/analytics?days=30", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})
}

// TestObserverAnalytics_ConcurrentByTxIDMutation_1830 is the -race
// regression requested in #1830: handleObserverAnalytics reads
// s.store.byTxID (directly, and via enrichObsWithTx) for every observation
// in the window. Before the #1830 fix those reads happened after the
// snapshot RLock was released, racing with concurrent ingest/eviction
// writers to that same map. This test hammers the endpoint concurrently
// with goroutines that mutate byTxID exactly like ingest/eviction would,
// and must pass under `go test -race`.
func TestObserverAnalytics_ConcurrentByTxIDMutation_1830(t *testing.T) {
	srv, router := setupTestServer(t)

	stop := make(chan struct{})
	var writers sync.WaitGroup
	var readers sync.WaitGroup
	advertType := types.PayloadType(4)

	// Writer goroutines: mutate byTxID under s.store.mu, like ingest
	// (adds) and eviction (deletes) do in production. They run until
	// stop is closed, which happens once all readers are done below.
	for w := 0; w < 4; w++ {
		writers.Add(1)
		go func(base int) {
			defer writers.Done()
			n := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				srv.store.mu.Lock()
				id := base*100000 + n
				srv.store.byTxID[id] = &StoreTx{ID: id, PayloadType: &advertType}
				delete(srv.store.byTxID, id-1)
				srv.store.mu.Unlock()
				n++
			}
		}(w)
	}

	// Reader goroutines: repeatedly hit the endpoint under test.
	for r := 0; r < 8; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for i := 0; i < 50; i++ {
				req := httptest.NewRequest("GET", "/api/observers/obs1/analytics", nil)
				w := httptest.NewRecorder()
				router.ServeHTTP(w, req)
				if w.Code != 200 {
					t.Errorf("expected 200, got %d", w.Code)
					return
				}
			}
		}()
	}

	readers.Wait()
	close(stop)
	writers.Wait()
}

func TestChannelMessages(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/channels/%23test/messages", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	if body["messages"] == nil {
		t.Error("expected messages")
	}
}

func TestChannelMessagesWithRegion(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	sjcIdx := seedObserver(t, db, scopeKey(100), "SJC", now)
	sfoIdx := seedObserver(t, db, scopeKey(101), "SFO", now)

	for _, m := range []struct {
		hash    string
		text    string
		sender  string
		at      time.Time
		observe int64
	}{
		{"c0a0000000000001", "SJCUser: In region", "SJCUser", now.Add(-1 * time.Hour), sjcIdx},
		{"c0a0000000000002", "OtherUser: Cross region", "OtherUser", now.Add(-30 * time.Minute), sfoIdx},
	} {
		txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
			Hash:        m.hash,
			RawHex:      "eeff",
			RouteType:   1,
			PayloadType: 5,
			DecodedJSON: fmt.Sprintf(`{"type":"CHAN","channel":"#test","text":%q,"sender":%q}`, m.text, m.sender),
			ChannelHash: "#test",
		}, m.at)
		if err != nil {
			t.Fatalf("seed transmission %s: %v", m.hash, err)
		}
		_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
			TransmissionID: txID,
			ObserverIdx:    iptr64(m.observe),
			SNR:            fptr(11.0),
			RSSI:           fptr(-89.0),
			Timestamp:      m.at,
		}, nil, nil)
		if err != nil {
			t.Fatalf("seed observation %s: %v", m.hash, err)
		}
		if !inserted {
			t.Fatalf("expected observation to be inserted for tx %d", txID)
		}
	}

	store := loadedStore(t, db)
	_, router := serverFor(t, db, store)

	req := httptest.NewRequest("GET", "/api/channels/%23test/messages?region=SJC", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var body struct {
		Messages []struct {
			Sender string `json:"sender"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if len(body.Messages) == 0 {
		t.Fatalf("expected at least one regional message")
	}
	sawRegional := false
	for _, msg := range body.Messages {
		if msg.Sender == "OtherUser" {
			t.Fatalf("cross-region message should be excluded")
		}
		if msg.Sender == "SJCUser" {
			sawRegional = true
		}
	}
	if !sawRegional {
		t.Fatalf("expected the SJC message in the response")
	}
}

func TestAnalyticsTopology(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/analytics/topology", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	if body["uniqueNodes"] == nil {
		t.Error("expected uniqueNodes")
	}
}

func TestAnalyticsDistance(t *testing.T) {
	srv, router := setupTestServer(t)
	// #1011: lazy distance index — trigger build and wait for it
	// before asserting 200 shape.
	srv.store.TriggerDistanceIndexBuild()
	deadline := time.Now().Add(5 * time.Second)
	for !srv.store.DistanceIndexBuilt() {
		if time.Now().After(deadline) {
			t.Fatal("distance index did not finish building within 5s")
		}
		time.Sleep(10 * time.Millisecond)
	}
	req := httptest.NewRequest("GET", "/api/analytics/distance", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestAnalyticsHashSizes(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/analytics/hash-sizes", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestAnalyticsSubpaths(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/analytics/subpaths", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestAnalyticsSubpathsBulk(t *testing.T) {
	_, router := setupTestServer(t)

	// Valid request with multiple groups.
	req := httptest.NewRequest("GET", "/api/analytics/subpaths-bulk?groups=2-2:50,3-3:30,5-8:15", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	results, ok := body["results"].([]interface{})
	if !ok {
		t.Fatal("expected results array")
	}
	if len(results) != 3 {
		t.Errorf("expected 3 result groups, got %d", len(results))
	}
	// Each result should have subpaths and totalPaths.
	for i, r := range results {
		rm, ok := r.(map[string]interface{})
		if !ok {
			t.Fatalf("result %d not a map", i)
		}
		if _, ok := rm["subpaths"]; !ok {
			t.Errorf("result %d missing subpaths", i)
		}
		if _, ok := rm["totalPaths"]; !ok {
			t.Errorf("result %d missing totalPaths", i)
		}
	}

	// Missing groups param → 400 with an error body.
	req2 := httptest.NewRequest("GET", "/api/analytics/subpaths-bulk", nil)
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)
	if w2.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 with error body, got %d", w2.Code)
	}
	var errBody map[string]interface{}
	json.Unmarshal(w2.Body.Bytes(), &errBody)
	if _, ok := errBody["error"]; !ok {
		t.Error("expected error field for missing groups param")
	}

	// Invalid group format.
	req3 := httptest.NewRequest("GET", "/api/analytics/subpaths-bulk?groups=bad", nil)
	w3 := httptest.NewRecorder()
	router.ServeHTTP(w3, req3)
	if w3.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid group format, got %d", w3.Code)
	}
	var errBody3 map[string]interface{}
	json.Unmarshal(w3.Body.Bytes(), &errBody3)
	if _, ok := errBody3["error"]; !ok {
		t.Error("expected error for invalid group format")
	}
}

func TestAnalyticsSubpathDetailWithHops(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/analytics/subpath-detail?hops=aa,bb", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	hops, ok := body["hops"].([]interface{})
	if !ok {
		t.Fatal("expected hops array")
	}
	if len(hops) != 2 {
		t.Errorf("expected 2 hops, got %d", len(hops))
	}
}

func TestAnalyticsSubpathDetailNoHops(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/analytics/subpath-detail", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	if body["error"] == nil {
		t.Error("expected error message when no hops provided")
	}
}

func TestResolveHopsEmpty(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/resolve-hops", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	resolved, ok := body["resolved"].(map[string]interface{})
	if !ok {
		t.Fatal("expected resolved map")
	}
	if len(resolved) != 0 {
		t.Error("expected empty resolved map for no hops")
	}
}

func TestResolveHopsAmbiguous(t *testing.T) {
	db := setupTestDB(t)

	now := time.Now().UTC()
	seedNode(t, db, padKey("aabbccdd11223344"), "TestRepeater", "repeater", nil, nil, now)
	seedNode(t, db, padKey("aabb000000000000"), "AnotherNode", "repeater", nil, nil, now)
	store := loadedStore(t, db)
	_, router := serverFor(t, db, store)

	req := httptest.NewRequest("GET", "/api/resolve-hops?hops=aabb", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var body struct {
		Resolved map[string]struct {
			Ambiguous  bool              `json:"ambiguous"`
			Candidates []json.RawMessage `json:"candidates"`
		} `json:"resolved"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("bad JSON: %v", err)
	}
	aabb, ok := body.Resolved["aabb"]
	if !ok {
		t.Fatal("expected aabb in resolved map")
	}
	if !aabb.Ambiguous {
		t.Error("expected ambiguous=true when multiple candidates")
	}
	if len(aabb.Candidates) < 2 {
		t.Errorf("expected at least 2 candidates, got %d", len(aabb.Candidates))
	}
}

func TestResolveHopsNoMatch(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/resolve-hops?hops=zzzz", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	resolved := body["resolved"].(map[string]interface{})
	zzzz := resolved["zzzz"].(map[string]interface{})
	if zzzz["name"] != nil {
		t.Error("expected nil name for unresolved hop")
	}
}

func TestAudioLabBuckets(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/audio-lab/buckets", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	if body["buckets"] == nil {
		t.Error("expected buckets")
	}
}

func TestIATACoords(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/iata-coords", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	if body["coords"] == nil {
		t.Error("expected coords")
	}
}

func TestPerfMiddlewareRecording(t *testing.T) {
	_, router := setupTestServer(t)

	// Make several requests to generate perf data
	for i := 0; i < 5; i++ {
		req := httptest.NewRequest("GET", "/api/health", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
	}

	// Check perf endpoint
	req := httptest.NewRequest("GET", "/api/perf", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	totalReqs := body["totalRequests"].(float64)
	// At least 5 health requests + 1 perf request (but perf is also counted)
	if totalReqs < 5 {
		t.Errorf("expected at least 5 total requests, got %v", totalReqs)
	}
}

func TestPerfMiddlewareNonAPI(t *testing.T) {
	// Non-API paths should not be recorded
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/some/non/api/path", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	// No panic, no error — middleware just passes through
}

func TestPacketsWithOrderAsc(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/packets?limit=10&order=asc", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestPacketsWithTypeAndRouteFilter(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/packets?limit=10&type=4&route=1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestPacketsWithExpandObservations(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/packets?limit=10&expand=observations", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestConfigClientEndpoint(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/config/client", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	if body["propagationBufferMs"] == nil {
		t.Error("expected propagationBufferMs")
	}
	tsRaw, ok := body["timestamps"].(map[string]interface{})
	if !ok {
		t.Fatal("expected timestamps object")
	}
	if tsRaw["defaultMode"] != "ago" {
		t.Errorf("expected timestamps.defaultMode=ago, got %v", tsRaw["defaultMode"])
	}
	if tsRaw["timezone"] != "local" {
		t.Errorf("expected timestamps.timezone=local, got %v", tsRaw["timezone"])
	}
	if tsRaw["formatPreset"] != "iso" {
		t.Errorf("expected timestamps.formatPreset=iso, got %v", tsRaw["formatPreset"])
	}
}

func TestNodeSearchEmpty(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/nodes/search?q=", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	nodes := body["nodes"].([]interface{})
	if len(nodes) != 0 {
		t.Error("expected empty nodes for empty search")
	}
}

func TestNodeSearchWhitespace(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/nodes/search?q=%20%20", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	nodes := body["nodes"].([]interface{})
	if len(nodes) != 0 {
		t.Error("expected empty nodes for whitespace search")
	}
}

func TestNodeAnalyticsNoNameNode(t *testing.T) {
	db := setupTestDB(t)
	pk := seedNoNameNode(t, db, "", fptr(37.5), fptr(-122.0))
	store := loadedStore(t, db)
	_, router := serverFor(t, db, store)

	req := httptest.NewRequest("GET", "/api/nodes/"+pk+"/analytics?days=30", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}
	var body struct {
		Node json.RawMessage `json:"node"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("bad JSON: %v", err)
	}
	if len(body.Node) == 0 || string(body.Node) == "null" {
		t.Error("expected node in response")
	}
}

func TestNodeHealthForNoNameNode(t *testing.T) {
	db := setupTestDB(t)
	pk := seedNoNameNode(t, db, "repeater", nil, nil)
	store := loadedStore(t, db)
	_, router := serverFor(t, db, store)

	req := httptest.NewRequest("GET", "/api/nodes/"+pk+"/health", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}
}

func TestPacketsWithNodeFilter(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/packets?limit=10&node=TestRepeater", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestPacketsWithRegionFilter(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/packets?limit=10&region=SJC", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestPacketsWithHashFilter(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/packets?limit=10&hash=abc123def4567890", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestPacketsWithObserverFilter(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/packets?limit=10&observer=obs1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestPacketsWithSinceUntil(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/packets?limit=10&since=2020-01-01&until=2099-01-01", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestNodesWithRoleFilter(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/nodes?role=repeater&limit=10", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	total := body["total"].(float64)
	if total != 1 {
		t.Errorf("expected 1 repeater, got %v", total)
	}
}

func TestNodesWithSortAndSearch(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/nodes?search=Test&sortBy=name&limit=10", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestGroupedPacketsWithFilters(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/packets?groupByHash=true&limit=10&type=4", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestConfigThemeWithCustomConfig(t *testing.T) {
	db := setupTestDB(t)
	seedTestData(t, db)
	cfg := &Config{
		Port: 3000,
		Branding: map[string]interface{}{
			"siteName": "CustomSite",
		},
		Theme: map[string]interface{}{
			"accent": "#ff0000",
		},
		Home: map[string]interface{}{
			"title": "Welcome",
		},
	}
	hub := NewHub()
	srv := NewServer(db, cfg, hub)
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	req := httptest.NewRequest("GET", "/api/config/theme", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	branding := body["branding"].(map[string]interface{})
	if branding["siteName"] != "CustomSite" {
		t.Errorf("expected CustomSite, got %v", branding["siteName"])
	}
	if body["home"] == nil {
		t.Error("expected home in response")
	}
}

func TestConfigThemeHomeDefaults(t *testing.T) {
	// When no home config is set, server should return built-in defaults
	db := setupTestDB(t)
	seedTestData(t, db)
	cfg := &Config{Port: 3000} // no Home set
	hub := NewHub()
	srv := NewServer(db, cfg, hub)
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	req := httptest.NewRequest("GET", "/api/config/theme", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	home, ok := body["home"].(map[string]interface{})
	if !ok || home == nil {
		t.Fatal("expected non-null home object in theme response")
	}
	if home["heroTitle"] != "CoreScope" {
		t.Errorf("expected heroTitle=CoreScope, got %v", home["heroTitle"])
	}
	if home["heroSubtitle"] == nil {
		t.Error("expected heroSubtitle in home defaults")
	}
	steps, ok := home["steps"].([]interface{})
	if !ok || len(steps) == 0 {
		t.Error("expected non-empty steps array in home defaults")
	}
	footerLinks, ok := home["footerLinks"].([]interface{})
	if !ok || len(footerLinks) == 0 {
		t.Error("expected non-empty footerLinks array in home defaults")
	}
}

func TestConfigCacheWithCustomTTL(t *testing.T) {
	db := setupTestDB(t)
	seedTestData(t, db)
	cfg := &Config{
		Port: 3000,
		CacheTTL: map[string]interface{}{
			"nodes": 60000,
		},
	}
	hub := NewHub()
	srv := NewServer(db, cfg, hub)
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	req := httptest.NewRequest("GET", "/api/config/cache", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	if body["nodes"] != float64(60000) {
		t.Errorf("expected 60000, got %v", body["nodes"])
	}
}

func TestConfigRegionsWithCustomRegions(t *testing.T) {
	db := setupTestDB(t)
	seedTestData(t, db)
	cfg := &Config{
		Port: 3000,
		Regions: map[string]string{
			"LAX": "Los Angeles",
		},
	}
	hub := NewHub()
	srv := NewServer(db, cfg, hub)
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	req := httptest.NewRequest("GET", "/api/config/regions", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	if body["LAX"] != "Los Angeles" {
		t.Errorf("expected 'Los Angeles', got %v", body["LAX"])
	}
	// DB-sourced IATA codes should also appear
	if body["SJC"] == nil {
		t.Error("expected SJC from DB")
	}
}

func TestIataCoordsIncludesNapaAndSonoma(t *testing.T) {
	// Issue #1786: observers tagged APC (Napa County) or STS (Charles M.
	// Schulz–Sonoma County) rendered without lat/lon and did not pin on the map
	// because iataCoords lacked entries for them.
	cases := []struct {
		code     string
		lat, lon float64
	}{
		{"APC", 38.2132, -122.2807},
		{"STS", 38.509, -122.8128},
	}
	for _, c := range cases {
		coord, ok := iataCoords[c.code]
		if !ok {
			t.Errorf("iataCoords missing %q", c.code)
			continue
		}
		if coord.Lat != c.lat || coord.Lon != c.lon {
			t.Errorf("%s = {%v, %v}, want {%v, %v}", c.code, coord.Lat, coord.Lon, c.lat, c.lon)
		}
	}
}

func TestConfigMapWithCustomDefaults(t *testing.T) {
	db := setupTestDB(t)
	seedTestData(t, db)
	cfg := &Config{Port: 3000}
	cfg.MapDefaults.Center = []float64{40.0, -74.0}
	cfg.MapDefaults.Zoom = 12
	hub := NewHub()
	srv := NewServer(db, cfg, hub)
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	req := httptest.NewRequest("GET", "/api/config/map", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	if body["zoom"] != float64(12) {
		t.Errorf("expected zoom 12, got %v", body["zoom"])
	}
}

func TestHandlerErrorPathsWhenDatabaseClosed(t *testing.T) {
	db := setupTestDB(t)
	store := loadedStore(t, db)
	_, router := serverFor(t, db, store)
	db.Close()

	for _, tc := range []struct {
		name string
		path string
		want int
	}{
		{"stats", "/api/stats", 500},
		{"channels", "/api/channels", 500},
		{"traces", "/api/traces/abc123def4567890", 500},
		{"observers", "/api/observers", 500},
		{"nodes", "/api/nodes?limit=10", 500},
		{"network status", "/api/nodes/network-status", 500},
		{"packets", "/api/packets?limit=10", 200},
		{"packets predating memory", "/api/packets?limit=10&since=2000-01-01", 200},
		{"packets grouped", "/api/packets?limit=10&groupByHash=true", 200},
		{"node search", "/api/nodes/search?q=test", 500},
		{"channel messages", "/api/channels/%23test/messages", 500},
		{"bulk health", "/api/nodes/bulk-health", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tc.path, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != tc.want {
				t.Errorf("%s: expected %d, got %d", tc.path, tc.want, w.Code)
			}
		})
	}
}

func TestNodeHashSizeEnrichment(t *testing.T) {
	t.Run("nil info leaves defaults", func(t *testing.T) {
		node := api.Node{PublicKey: "abc123"}
		EnrichNodeWithHashSize(&node, nil)
		if node.HashSize != nil {
			t.Error("expected hash_size to remain nil with nil info")
		}
	})

	t.Run("enriches with computed data", func(t *testing.T) {
		node := api.Node{PublicKey: "abc123"}
		info := &hashSizeNodeInfo{
			HashSize:     2,
			AllSizes:     map[int]bool{1: true, 2: true},
			Seq:          []int{1, 2, 1, 2},
			Inconsistent: true,
		}
		EnrichNodeWithHashSize(&node, info)
		if node.HashSize == nil || *node.HashSize != 2 {
			t.Errorf("expected hash_size 2, got %v", node.HashSize)
		}
		if !node.HashSizeInconsistent {
			t.Error("expected hash_size_inconsistent true")
		}
		if len(node.HashSizesSeen) != 2 || node.HashSizesSeen[0] != 1 || node.HashSizesSeen[1] != 2 {
			t.Errorf("expected [1,2], got %v", node.HashSizesSeen)
		}
		info.HashSize = 9
		if *node.HashSize != 2 {
			t.Error("node.HashSize must not alias the cached info")
		}
	})

	t.Run("single size omits sizes_seen", func(t *testing.T) {
		node := api.Node{PublicKey: "abc123"}
		info := &hashSizeNodeInfo{
			HashSize: 3,
			AllSizes: map[int]bool{3: true},
			Seq:      []int{3, 3, 3},
		}
		EnrichNodeWithHashSize(&node, info)
		if node.HashSize == nil || *node.HashSize != 3 {
			t.Errorf("expected hash_size 3, got %v", node.HashSize)
		}
		if node.HashSizeInconsistent {
			t.Error("expected hash_size_inconsistent false")
		}
		if len(node.HashSizesSeen) != 0 {
			t.Error("hash_sizes_seen should not be set for single size")
		}
	})
}

const (
	raw1ByteFlood = "1101aabb"
	raw2ByteFlood = "1141aabb"
	raw3ByteFlood = "1181aabb"
)

func TestGetNodeHashSizeInfoFlipFlop(t *testing.T) {
	db := setupTestDB(t)
	now := time.Now().UTC()
	pk := "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	seedNode(t, db, pk, "TestNode", "repeater", nil, nil, now)
	obsIdx := seedObserver(t, db, scopeKey(110), "SFO", now)
	seedAdvertSeries(t, db, obsIdx, "f100", pk, "TestNode", types.RoleRepeater,
		[]string{raw1ByteFlood, raw2ByteFlood, raw1ByteFlood}, now.Add(-1*time.Hour), time.Minute)
	store := loadedStore(t, db)

	ni := store.GetNodeHashSizeInfo()[pk]
	if ni == nil {
		t.Fatal("expected hash info for test node")
	}
	if len(ni.AllSizes) != 2 {
		t.Errorf("expected 2 unique sizes, got %d", len(ni.AllSizes))
	}
	if !ni.Inconsistent {
		t.Error("expected inconsistent flag to be true for flip-flop pattern")
	}
}

func TestGetNodeHashSizeInfoDominant(t *testing.T) {
	db := setupTestDB(t)
	now := time.Now().UTC()
	pk := strings.Repeat("b", 64)
	seedNode(t, db, pk, "Repeater2B", "repeater", nil, nil, now)
	obsIdx := seedObserver(t, db, scopeKey(111), "SFO", now)
	seedAdvertSeries(t, db, obsIdx, "f200", pk, "Repeater2B", types.RoleRepeater,
		[]string{raw1ByteFlood, raw2ByteFlood, raw2ByteFlood, raw2ByteFlood, raw2ByteFlood},
		now.Add(-1*time.Hour), time.Minute)
	store := loadedStore(t, db)

	ni := store.GetNodeHashSizeInfo()[pk]
	if ni == nil {
		t.Fatal("expected hash info for test node")
	}
	if ni.HashSize != 2 {
		t.Errorf("HashSize=%d, want 2 (latest advert should determine hash size)", ni.HashSize)
	}
}

func TestGetNodeHashSizeInfoLatestWins(t *testing.T) {
	db := setupTestDB(t)
	now := time.Now().UTC()
	pk := strings.Repeat("c", 64)
	seedNode(t, db, pk, "LatestWins", "repeater", nil, nil, now)
	obsIdx := seedObserver(t, db, scopeKey(112), "SFO", now)
	seedAdvertSeries(t, db, obsIdx, "f300", pk, "LatestWins", types.RoleRepeater,
		[]string{raw1ByteFlood, raw1ByteFlood, raw1ByteFlood, raw1ByteFlood, raw2ByteFlood},
		now.Add(-1*time.Hour), time.Minute)
	store := loadedStore(t, db)

	ni := store.GetNodeHashSizeInfo()[pk]
	if ni == nil {
		t.Fatal("expected hash info for test node")
	}
	if ni.HashSize != 2 {
		t.Errorf("HashSize=%d, want 2 (latest advert should win over historical mode)", ni.HashSize)
	}
	if len(ni.AllSizes) != 2 {
		t.Errorf("AllSizes count=%d, want 2", len(ni.AllSizes))
	}
	if !ni.AllSizes[1] || !ni.AllSizes[2] {
		t.Error("AllSizes should contain both 1 and 2")
	}
}

func TestGetNodeHashSizeInfoIgnoreDirectZeroHop(t *testing.T) {
	db := setupTestDB(t)
	now := time.Now().UTC()
	pk := "dddd111122223333444455556666777788889999aaaabbbbccccddddeeee3333"
	seedNode(t, db, pk, "DirIgnore", "repeater", nil, nil, now)
	obsIdx := seedObserver(t, db, scopeKey(113), "SFO", now)
	const rawFlood2B = "1140aabb"
	const rawDirect0 = "1200aabb"
	seedAdvertSeries(t, db, obsIdx, "f400", pk, "DirIgnore", types.RoleRepeater,
		[]string{rawFlood2B, rawDirect0, rawFlood2B, rawDirect0, rawFlood2B},
		now.Add(-1*time.Hour), time.Minute)
	store := loadedStore(t, db)

	ni := store.GetNodeHashSizeInfo()[pk]
	if ni == nil {
		t.Fatal("expected hash info for test node")
	}
	if ni.HashSize != 2 {
		t.Errorf("HashSize=%d, want 2 (direct zero-hop adverts should be ignored)", ni.HashSize)
	}
	if ni.Inconsistent {
		t.Error("expected hash_size_inconsistent=false when direct zero-hop adverts are ignored")
	}
	if len(ni.AllSizes) != 1 || !ni.AllSizes[2] {
		t.Errorf("expected only 2-byte size in AllSizes, got %#v", ni.AllSizes)
	}
}

func TestGetNodeHashSizeInfoOnlyDirectZeroHopIgnored(t *testing.T) {
	db := setupTestDB(t)
	now := time.Now().UTC()
	pk := "eeee111122223333444455556666777788889999aaaabbbbccccddddeeee4444"
	seedNode(t, db, pk, "OnlyDirect", "repeater", nil, nil, now)
	obsIdx := seedObserver(t, db, scopeKey(114), "SFO", now)
	seedAdvertSeries(t, db, obsIdx, "f500", pk, "OnlyDirect", types.RoleRepeater,
		[]string{"1200aabb"}, now.Add(-1*time.Hour), time.Minute)
	store := loadedStore(t, db)

	if ni := store.GetNodeHashSizeInfo()[pk]; ni != nil {
		t.Errorf("expected nil hash info for direct zero-hop only node, got HashSize=%d", ni.HashSize)
	}
}

func TestGetNodeHashSizeInfoDirectNonZeroHopCounted(t *testing.T) {
	db := setupTestDB(t)
	now := time.Now().UTC()
	pk := "ffff111122223333444455556666777788889999aaaabbbbccccddddeeee5555"
	seedNode(t, db, pk, "DirNonZero", "repeater", nil, nil, now)
	obsIdx := seedObserver(t, db, scopeKey(115), "SFO", now)
	seedAdvertSeries(t, db, obsIdx, "f600", pk, "DirNonZero", types.RoleRepeater,
		[]string{"1241aabb"}, now.Add(-1*time.Hour), time.Minute)
	store := loadedStore(t, db)

	ni := store.GetNodeHashSizeInfo()[pk]
	if ni == nil {
		t.Fatal("expected hash info for DIRECT non-zero-hop node — it should NOT be skipped")
	}
	if ni.HashSize != 2 {
		t.Errorf("HashSize=%d, want 2 (DIRECT with hop count > 0 should be counted)", ni.HashSize)
	}
}
func TestGetNodeHashSizeInfoNoAdverts(t *testing.T) {
	db := setupTestDB(t)
	now := time.Now().UTC()
	pk := strings.Repeat("d", 64)
	seedNode(t, db, pk, "NoAdverts", "repeater", nil, nil, now)
	obsIdx := seedObserver(t, db, scopeKey(116), "SFO", now)
	insertAdvert(t, db, obsIdx, advertPacket{
		Role:        types.RoleRepeater,
		Hash:        "f700000000000000",
		RawHex:      "0440aabb",
		RouteType:   0,
		PayloadType: types.PayloadType(2),
		Name:        "NoAdverts",
		PubKey:      pk,
		At:          now,
	})
	store := loadedStore(t, db)

	if ni := store.GetNodeHashSizeInfo()[pk]; ni != nil {
		t.Errorf("expected nil hash info for node with no adverts, got HashSize=%d", ni.HashSize)
	}
}

func TestHashAnalyticsZeroHopAdvert(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	now := time.Now().UTC()
	pk := strings.Repeat("e", 64)
	seedNode(t, db, pk, "ZeroHop", "repeater", nil, nil, now)
	obsIdx := seedObserver(t, db, scopeKey(118), "SFO", now)
	seedAdvertSeries(t, db, obsIdx, "fa00", pk, "ZeroHop", types.RoleRepeater,
		[]string{"0500aabb"}, now.Add(-1*time.Hour), time.Minute)
	store := loadedStore(t, db)

	result := store.computeAnalyticsHashSizes(ctx, "", "")

	distByRepeaters := result.DistributionByRepeaters
	if distByRepeaters["1"] < 1 {
		t.Errorf("distributionByRepeaters[\"1\"]=%d, want >=1 (zero-hop advert should be tracked per-node)", distByRepeaters["1"])
	}
	total := result.Total
	dist := result.Distribution
	if total != 0 {
		t.Errorf("total=%d, want 0 (zero-hop adverts must not inflate total)", total)
	}
	if dist["1"] != 0 {
		t.Errorf("distribution[\"1\"]=%d, want 0 (zero-hop adverts must not inflate distribution)", dist["1"])
	}
}

func TestAnalyticsHashSizeSameNameDifferentPubkey(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	now := time.Now().UTC()
	pk1 := "aaaa111122223333444455556666777788889999aaaabbbbccccddddeeee1111"
	pk2 := "aaaa111122223333444455556666777788889999aaaabbbbccccddddeeee2222"
	seedNode(t, db, pk1, "SameName", "repeater", nil, nil, now)
	seedNode(t, db, pk2, "SameName", "repeater", nil, nil, now)
	obsIdx := seedObserver(t, db, scopeKey(119), "SFO", now)
	for i, pk := range []string{pk1, pk2} {
		insertAdvert(t, db, obsIdx, advertPacket{
			Role:        types.RoleRepeater,
			Hash:        fmt.Sprintf("fb00%012x", i),
			RawHex:      "0540aabb",
			RouteType:   1,
			PayloadType: types.PayloadType(4),
			Name:        "SameName",
			PubKey:      pk,
			At:          now.Add(-1 * time.Hour),
			Hops:        []string{"aabb"},
		})
	}
	store := loadedStore(t, db)

	result := store.GetAnalyticsHashSizes(ctx, "", "")
	distByRepeaters := result.DistributionByRepeaters
	if distByRepeaters["2"] < 2 {
		t.Errorf("distributionByRepeaters[\"2\"]=%d, want >=2 (same-name nodes with different pubkeys should be counted separately)", distByRepeaters["2"])
	}
}

func TestInconsistentNodesExcludesCompanions(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	now := time.Now().UTC()
	obsIdx := seedObserver(t, db, scopeKey(120), "SFO", now)

	for ni, n := range []struct {
		pk   string
		role types.NodeRole
	}{
		{"aa" + strings.Repeat("1", 62), types.RoleRepeater},
		{"bb" + strings.Repeat("2", 62), types.RoleRoom},
		{"cc" + strings.Repeat("3", 62), types.RoleCompanion},
	} {
		if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
			PublicKey: n.pk,
			Name:      "Node-" + string(n.role),
			Role:      n.role,
			Lat:       nil,
			Lon:       nil,
			RxTime:    now,
			IngestNow: now,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n.role, err)
		}
		seedAdvertSeries(t, db, obsIdx, fmt.Sprintf("1c%02x", ni), n.pk, "Node-"+string(n.role), n.role,
			[]string{raw2ByteFlood, raw3ByteFlood, raw2ByteFlood}, now.Add(-1*time.Hour), time.Minute)
	}
	store := loadedStore(t, db)
	_, router := serverFor(t, db, store)

	req := httptest.NewRequest("GET", "/api/analytics/hash-collisions", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body hashCollisionsBody
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("bad JSON: %v", err)
	}
	t.Logf("body: %s", w.Body.String())

	roles := make(map[types.NodeRole]bool)
	for _, node := range body.InconsistentNodes {
		if node.Role == string(types.RoleCompanion) {
			t.Error("companion node should be excluded from inconsistent_nodes")
		}
		roles[types.NodeRole(node.Role)] = true
	}
	if !roles[types.RoleRepeater] {
		t.Error("expected repeater in inconsistent_nodes")
	}
	if !roles[types.RoleRoom] {
		t.Error("expected room in inconsistent_nodes")
	}
}

func TestHashSizeInfoTimeWindow(t *testing.T) {
	db := setupTestDB(t)
	now := time.Now().UTC()
	oldPK := "dd" + strings.Repeat("4", 62)
	newPK := "ee" + strings.Repeat("5", 62)
	seedNode(t, db, oldPK, "OldNode", "repeater", nil, nil, now)
	seedNode(t, db, newPK, "NewNode", "repeater", nil, nil, now)
	obsIdx := seedObserver(t, db, scopeKey(117), "SFO", now)
	seedAdvertSeries(t, db, obsIdx, "f800", oldPK, "OldNode", types.RoleRepeater,
		[]string{raw2ByteFlood, raw3ByteFlood, raw2ByteFlood}, now.Add(-10*24*time.Hour), time.Minute)
	seedAdvertSeries(t, db, obsIdx, "f900", newPK, "NewNode", types.RoleRepeater,
		[]string{raw2ByteFlood, raw2ByteFlood, raw2ByteFlood}, now.Add(-1*time.Hour), time.Minute)
	store := loadedStore(t, db)

	info := store.GetNodeHashSizeInfo()
	if ni := info[oldPK]; ni != nil && ni.Inconsistent {
		t.Error("old adverts (>7 days) should be excluded; node should not be flagged as inconsistent")
	}
	if info[newPK] == nil {
		t.Error("recent adverts should be included in hash size info")
	}
}

func TestConfigGeoFilterEndpoint(t *testing.T) {
	ctx := t.Context()

	t.Run("no geo filter configured", func(t *testing.T) {
		_, router := setupTestServer(t)
		req := httptest.NewRequest("GET", "/api/config/geo-filter", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
		var body map[string]interface{}
		json.Unmarshal(w.Body.Bytes(), &body)
		if body["polygon"] != nil {
			t.Errorf("expected polygon to be nil when no geo filter configured, got %v", body["polygon"])
		}
	})

	t.Run("with polygon configured", func(t *testing.T) {
		db := setupTestDB(t)
		seedTestData(t, db)
		lat0, lat1 := 50.0, 51.5
		lon0, lon1 := 3.0, 5.5
		cfg := &Config{
			Port: 3000,
			GeoFilter: &GeoFilterConfig{
				Polygon:  [][2]float64{{lat0, lon0}, {lat1, lon0}, {lat1, lon1}, {lat0, lon1}},
				BufferKm: 20,
			},
		}
		hub := NewHub()
		srv := NewServer(db, cfg, hub)
		srv.store = newPacketStoreForTest(t, db)
		if err := srv.store.LoadChunked(ctx, 1000); err != nil {
			t.Fatalf("LoadChunked(): %v", err)
		}
		router := mux.NewRouter()
		srv.RegisterRoutes(router)

		req := httptest.NewRequest("GET", "/api/config/geo-filter", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
		var body map[string]interface{}
		json.Unmarshal(w.Body.Bytes(), &body)
		if body["polygon"] == nil {
			t.Error("expected polygon in response when geo filter is configured")
		}
		if body["bufferKm"] == nil {
			t.Error("expected bufferKm in response")
		}
		if _, ok := body["writeEnabled"]; !ok {
			t.Error("expected writeEnabled field in response")
		}
		// No apiKey configured → writeEnabled should be false
		if body["writeEnabled"] != false {
			t.Errorf("expected writeEnabled=false when no apiKey, got %v", body["writeEnabled"])
		}
	})

	t.Run("writeEnabled true when strong apiKey configured", func(t *testing.T) {
		db := setupTestDB(t)
		cfg := &Config{Port: 3000, APIKey: "a-strong-api-key-1234"}
		hub := NewHub()
		srv := NewServer(db, cfg, hub)
		srv.store = newPacketStoreForTest(t, db)
		if err := srv.store.LoadChunked(ctx, 1000); err != nil {
			t.Fatalf("LoadChunked(): %v", err)
		}
		router := mux.NewRouter()
		srv.RegisterRoutes(router)

		req := httptest.NewRequest("GET", "/api/config/geo-filter", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		var body map[string]interface{}
		json.Unmarshal(w.Body.Bytes(), &body)
		if body["writeEnabled"] != true {
			t.Errorf("expected writeEnabled=true when strong apiKey configured, got %v", body["writeEnabled"])
		}
	})
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// TestLatestSeenMaintained verifies that StoreTx.LatestSeen is populated after Load()
// and is >= FirstSeen for packets that have observations.
func TestLatestSeenMaintained(t *testing.T) {
	db := setupTestDB(t)
	now := time.Now().UTC()
	obs1 := seedObserver(t, db, scopeKey(130), "SFO", now)
	obs2 := seedObserver(t, db, scopeKey(131), "OAK", now)

	firstSeenA := now.Add(-2 * time.Hour)
	txA, _, err := db.CreateOrUpdateTransmission(t.Context(), database.TransmissionInput{
		Hash: "1a00000000000001", RawHex: "aa01", RouteType: 1, PayloadType: 2, DecodedJSON: "{}",
	}, firstSeenA)
	if err != nil {
		t.Fatalf("seed tx A: %v", err)
	}
	for _, o := range []struct {
		idx int64
		at  time.Time
	}{
		{obs1, firstSeenA},
		{obs2, firstSeenA.Add(10 * time.Minute)},
	} {
		_, inserted, err := db.InsertObservationWithHops(t.Context(), database.ObservationInput{
			TransmissionID: txA, ObserverIdx: iptr64(o.idx), SNR: fptr(5.0), RSSI: fptr(-90.0), Timestamp: o.at,
		}, nil, nil)
		if err != nil {
			t.Fatalf("seed obs A: %v", err)
		}
		if !inserted {
			t.Fatalf("expected observation to be inserted for tx %d", txA)
		}
	}
	insertRelayPacket(t, db, obs1, relayPacket{
		Hash: "1a00000000000002", RouteType: 1, PayloadType: types.PayloadType(2), At: now.Add(-1 * time.Hour),
		Hops: nil, Resolved: nil,
	})

	store := loadedStore(t, db)

	store.mu.RLock()
	defer store.mu.RUnlock()

	if len(store.packets) == 0 {
		t.Fatal("expected packets in store after Load")
	}
	for _, tx := range store.packets {
		if tx.LatestSeen.IsZero() {
			t.Errorf("packet %s has zero LatestSeen (FirstSeen=%s)", tx.Hash, tx.FirstSeen)
			continue
		}
		if tx.LatestSeen.Before(tx.FirstSeen) {
			t.Errorf("packet %s: LatestSeen %s < FirstSeen %s", tx.Hash, tx.LatestSeen, tx.FirstSeen)
		}
		for _, obs := range tx.Observations {
			if !obs.Timestamp.IsZero() && obs.Timestamp.After(tx.LatestSeen) {
				t.Errorf("packet %s: obs.Timestamp %s > LatestSeen %s", tx.Hash, obs.Timestamp, tx.LatestSeen)
			}
		}
	}
}

// TestQueryGroupedPacketsSortedByLatest verifies that QueryGroupedPackets returns packets
// sorted by LatestSeen DESC — i.e. the packet whose most-recent observation is newest
// comes first, even if its first_seen is older.
func TestQueryGroupedPacketsSortedByLatest(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	obsIdx := seedObserver(t, db, scopeKey(132), "TST", now)

	for _, p := range []struct {
		hash       string
		raw        string
		text       string
		firstSeen  time.Time
		observedAt time.Time
	}{
		{"5017000000000001", "aa01", "old first", now.Add(-48 * time.Hour), now.Add(-5 * time.Minute)},
		{"5017000000000002", "bb02", "new first", now.Add(-1 * time.Hour), now.Add(-72 * time.Hour)},
	} {
		txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
			Hash:        p.hash,
			RawHex:      p.raw,
			RouteType:   1,
			PayloadType: 2,
			DecodedJSON: fmt.Sprintf(`{"type":"TXT_MSG","text":%q}`, p.text),
		}, p.firstSeen)
		if err != nil {
			t.Fatalf("seed tx %s: %v", p.hash, err)
		}
		_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
			TransmissionID: txID, ObserverIdx: iptr64(obsIdx), SNR: fptr(10.0), RSSI: fptr(-90.0), Timestamp: p.observedAt,
		}, nil, nil)
		if err != nil {
			t.Fatalf("seed obs %s: %v", p.hash, err)
		}
		if !inserted {
			t.Fatalf("expected observation to be inserted for tx %d", txID)
		}
	}

	store := loadedStore(t, db)

	result, err := store.QueryGroupedPackets(ctx, database.PacketQuery{Limit: 50})
	if err != nil {
		t.Fatalf("QueryGroupedPackets(): %v", err)
	}
	if result.Total < 2 {
		t.Fatalf("expected at least 2 packets, got %d", result.Total)
	}

	firstHash := ""
	secondHash := ""
	for _, p := range result.Packets {
		if p.Hash == nil {
			continue
		}
		h := *p.Hash
		if h == "5017000000000001" || h == "5017000000000002" {
			if firstHash == "" {
				firstHash = h
			} else {
				secondHash = h
				break
			}
		}
	}
	if firstHash != "5017000000000001" {
		t.Errorf("expected the packet with the most recent observation first; got first=%q second=%q", firstHash, secondHash)
	}
}

// TestQueryGroupedPacketsCacheReturnsConsistentResult verifies that two rapid successive
// calls to QueryGroupedPackets return the same total count and first packet hash.
func TestQueryGroupedPacketsCacheReturnsConsistentResult(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	obsIdx := seedObserver(t, db, scopeKey(133), "SFO", now)
	for i := 0; i < 3; i++ {
		insertRelayPacket(t, db, obsIdx, relayPacket{
			Hash: fmt.Sprintf("cc00%012x", i), RouteType: 1, PayloadType: types.PayloadType(2),
			At: now.Add(-time.Duration(i+1) * time.Hour), Hops: nil, Resolved: nil,
		})
	}
	store := loadedStore(t, db)

	q := database.PacketQuery{Limit: 50}
	r1, err := store.QueryGroupedPackets(ctx, q)
	if err != nil {
		t.Fatalf("first QueryGroupedPackets(): %v", err)
	}
	r2, err := store.QueryGroupedPackets(ctx, q)
	if err != nil {
		t.Fatalf("second QueryGroupedPackets(): %v", err)
	}
	if r1.Total != r2.Total {
		t.Errorf("cache inconsistency: first call total=%d, second call total=%d", r1.Total, r2.Total)
	}
	if r1.Total == 0 {
		t.Fatal("expected non-zero results from QueryGroupedPackets")
	}
	if r1.Packets[0].Hash == nil || r2.Packets[0].Hash == nil {
		t.Fatal("expected hash on the first grouped packet")
	}
	h1 := *r1.Packets[0].Hash
	h2 := *r2.Packets[0].Hash
	if h1 != h2 {
		t.Errorf("cache inconsistency: first call first hash=%q, second call first hash=%q", h1, h2)
	}
}

// TestGetChannelsCacheReturnsConsistentResult verifies that two rapid successive calls
// to GetChannels return the same number of channels with the same names.
func TestGetChannelsCacheReturnsConsistentResult(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	store.WaitIndexesReady(5 * time.Second)

	r1 := store.GetChannels(ctx, "")
	r2 := store.GetChannels(ctx, "")

	if len(r1) != len(r2) {
		t.Errorf("cache inconsistency: first call len=%d, second call len=%d", len(r1), len(r2))
	}
	if len(r1) == 0 {
		t.Fatal("expected at least one channel from seedTestData")
	}

	names1 := make(map[string]bool)
	for _, ch := range r1 {
		if n, ok := ch["name"].(string); ok {
			names1[n] = true
		}
	}
	for _, ch := range r2 {
		if n, ok := ch["name"].(string); ok {
			if !names1[n] {
				t.Errorf("cache inconsistency: channel %q in second result but not first", n)
			}
		}
	}
}

// TestGetChannelsNotBlockedByLargeLock verifies that GetChannels returns correct channel
// data (count and messageCount) after observations have been added — i.e. the lock-copy
// pattern works correctly and the JSON unmarshal outside the lock produces valid results.
func TestGetChannelsNotBlockedByLargeLock(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	store.WaitIndexesReady(5 * time.Second)

	channels := store.GetChannels(ctx, "")

	// seedTestData inserts one GRP_TXT (payload_type=5) packet with channel "#test".
	if len(channels) != 1 {
		t.Fatalf("expected 1 channel, got %d", len(channels))
	}

	ch := channels[0]
	name, ok := ch["name"].(string)
	if !ok || name != "#test" {
		t.Errorf("expected channel name '#test', got %v", ch["name"])
	}

	// messageCount should be 1 (one CHAN packet for #test).
	msgCount, ok := ch["messageCount"].(int)
	if !ok {
		// JSON numbers may unmarshal as float64 — but GetChannels returns native Go values.
		t.Errorf("expected messageCount to be int, got %T (%v)", ch["messageCount"], ch["messageCount"])
	} else if msgCount != 1 {
		t.Errorf("expected messageCount=1, got %d", msgCount)
	}
}

// --- Tests for computeHashCollisions (Issue #416) ---

func TestAnalyticsHashCollisionsEndpoint(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/analytics/hash-collisions", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	// Must have top-level keys
	if _, ok := body["inconsistent_nodes"]; !ok {
		t.Error("missing inconsistent_nodes key")
	}
	if _, ok := body["by_size"]; !ok {
		t.Error("missing by_size key")
	}

	bySize, ok := body["by_size"].(map[string]interface{})
	if !ok {
		t.Fatal("by_size is not a map")
	}
	// Must have entries for 1, 2, 3 byte sizes
	for _, sz := range []string{"1", "2", "3"} {
		sizeData, ok := bySize[sz].(map[string]interface{})
		if !ok {
			t.Errorf("by_size[%s] is not a map", sz)
			continue
		}
		stats, ok := sizeData["stats"].(map[string]interface{})
		if !ok {
			t.Errorf("by_size[%s].stats is not a map", sz)
			continue
		}
		if _, ok := stats["total_nodes"]; !ok {
			t.Errorf("by_size[%s].stats missing total_nodes", sz)
		}
		if _, ok := stats["collision_count"]; !ok {
			t.Errorf("by_size[%s].stats missing collision_count", sz)
		}
		// collisions must be an array, not null
		collisions, ok := sizeData["collisions"].([]interface{})
		if !ok {
			t.Errorf("by_size[%s].collisions is not an array", sz)
		}
		_ = collisions
	}
}

func TestHashCollisionsRegionParam(t *testing.T) {
	// Issue #438: region param should be accepted and used for filtering.
	// With no region observers configured, results should be identical to global.
	_, router := setupTestServer(t)

	// Request without region
	req1 := httptest.NewRequest("GET", "/api/analytics/hash-collisions", nil)
	w1 := httptest.NewRecorder()
	router.ServeHTTP(w1, req1)
	if w1.Code != 200 {
		t.Fatalf("expected 200, got %d", w1.Code)
	}

	// Request with region param (no observers for this region, so falls back to global)
	req2 := httptest.NewRequest("GET", "/api/analytics/hash-collisions?region=us-west", nil)
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)
	if w2.Code != 200 {
		t.Fatalf("expected 200, got %d", w2.Code)
	}

	// With no region observers configured, both should return identical results
	if w1.Body.String() != w2.Body.String() {
		t.Error("responses differ with/without region param when no region observers configured")
	}
}

func TestHashCollisionsOneByteCells(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/analytics/hash-collisions", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	bySize := body["by_size"].(map[string]interface{})
	oneByteData := bySize["1"].(map[string]interface{})

	// 1-byte data should include one_byte_cells for matrix rendering
	cells, ok := oneByteData["one_byte_cells"].(map[string]interface{})
	if !ok {
		t.Fatal("1-byte data missing one_byte_cells")
	}
	// Should have 256 entries (00-FF)
	if len(cells) != 256 {
		t.Errorf("expected 256 one_byte_cells entries, got %d", len(cells))
	}
}

func TestHashCollisionsTwoByteCells(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/analytics/hash-collisions", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	bySize := body["by_size"].(map[string]interface{})
	twoByteData := bySize["2"].(map[string]interface{})

	// 2-byte data should include two_byte_cells for matrix rendering
	cells, ok := twoByteData["two_byte_cells"].(map[string]interface{})
	if !ok {
		t.Fatal("2-byte data missing two_byte_cells")
	}
	// Should have 256 entries (00-FF first-byte groups)
	if len(cells) != 256 {
		t.Errorf("expected 256 two_byte_cells entries, got %d", len(cells))
	}
}

func TestHashCollisionsThreeByteNoMatrix(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/analytics/hash-collisions", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	bySize := body["by_size"].(map[string]interface{})
	threeByteData := bySize["3"].(map[string]interface{})

	// 3-byte data should NOT have one_byte_cells or two_byte_cells
	if _, ok := threeByteData["one_byte_cells"]; ok {
		t.Error("3-byte data should not have one_byte_cells")
	}
	if _, ok := threeByteData["two_byte_cells"]; ok {
		t.Error("3-byte data should not have two_byte_cells")
	}
}

func TestHashCollisionsClassification(t *testing.T) {
	// Test with seed data — nodes have coordinates, so distance classification should work
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/analytics/hash-collisions", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	bySize := body["by_size"].(map[string]interface{})

	// Check that collision entries have required fields
	for _, sz := range []string{"1", "2", "3"} {
		sizeData := bySize[sz].(map[string]interface{})
		collisions := sizeData["collisions"].([]interface{})
		for i, c := range collisions {
			entry := c.(map[string]interface{})
			if _, ok := entry["prefix"]; !ok {
				t.Errorf("by_size[%s].collisions[%d] missing prefix", sz, i)
			}
			if _, ok := entry["classification"]; !ok {
				t.Errorf("by_size[%s].collisions[%d] missing classification", sz, i)
			}
			class := entry["classification"].(string)
			validClasses := map[string]bool{"local": true, "regional": true, "distant": true, "incomplete": true, "unknown": true}
			if !validClasses[class] {
				t.Errorf("by_size[%s].collisions[%d] invalid classification: %s", sz, i, class)
			}
			nodes, ok := entry["nodes"].([]interface{})
			if !ok {
				t.Errorf("by_size[%s].collisions[%d] missing nodes array", sz, i)
			}
			if len(nodes) < 2 {
				t.Errorf("by_size[%s].collisions[%d] has %d nodes, expected >=2", sz, i, len(nodes))
			}
		}
	}
}

func TestHashCollisionsCacheTTL(t *testing.T) {
	ctx := t.Context()

	// Issue #420: collision cache should use dedicated TTL, default 3600s (1 hour)
	db := setupTestDB(t)
	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	store.WaitIndexesReady(5 * time.Second)

	if store.collisionCacheTTL != 3600*time.Second {
		t.Errorf("expected collisionCacheTTL=3600s, got %v", store.collisionCacheTTL)
	}
	// #1239: default bumped 15s → 60s.
	if store.rfCacheTTL != 60*time.Second {
		t.Errorf("expected rfCacheTTL=60s, got %v", store.rfCacheTTL)
	}
}

func TestHashCollisionsStatsFields(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/analytics/hash-collisions", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	bySize := body["by_size"].(map[string]interface{})

	for _, sz := range []string{"1", "2", "3"} {
		sizeData := bySize[sz].(map[string]interface{})
		stats := sizeData["stats"].(map[string]interface{})

		requiredFields := []string{"total_nodes", "nodes_for_byte", "using_this_size", "unique_prefixes", "collision_count", "space_size", "pct_used"}
		for _, f := range requiredFields {
			if _, ok := stats[f]; !ok {
				t.Errorf("by_size[%s].stats missing field: %s", sz, f)
			}
		}
	}
}

func TestHashCollisionsEmptyStore(t *testing.T) {
	ctx := t.Context()

	// Test with no nodes seeded
	db := setupTestDB(t)
	// Don't call seedTestData — empty store
	cfg := &Config{Port: 3000}
	hub := NewHub()
	srv := NewServer(db, cfg, hub)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	store.WaitIndexesReady(5 * time.Second)
	srv.store = store
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	req := httptest.NewRequest("GET", "/api/analytics/hash-collisions", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)

	// With no nodes, inconsistent_nodes should be empty array
	incon := body["inconsistent_nodes"].([]interface{})
	if len(incon) != 0 {
		t.Errorf("expected 0 inconsistent nodes, got %d", len(incon))
	}

	// All collision lists should be empty
	bySize := body["by_size"].(map[string]interface{})
	for _, sz := range []string{"1", "2", "3"} {
		sizeData := bySize[sz].(map[string]interface{})
		collisions := sizeData["collisions"].([]interface{})
		if len(collisions) != 0 {
			t.Errorf("by_size[%s] expected 0 collisions with empty store, got %d", sz, len(collisions))
		}
	}
}

func TestHashCollisionsWithCollision(t *testing.T) {
	db := setupTestDB(t)
	now := time.Now().UTC().Add(-1 * time.Hour)
	pk1 := padKey("cc11223344556677")
	pk2 := padKey("cc99887766554433")
	seedNode(t, db, pk1, "Node1", "repeater", fptr(37.5), fptr(-122.0), now)
	seedNode(t, db, pk2, "Node2", "repeater", fptr(37.51), fptr(-122.01), now)
	store := loadedStore(t, db)

	store.hashSizeInfoMu.Lock()
	store.hashSizeInfoCache = map[string]*hashSizeNodeInfo{
		pk1: {HashSize: 1, AllSizes: map[int]bool{1: true}},
		pk2: {HashSize: 1, AllSizes: map[int]bool{1: true}},
	}
	store.hashSizeInfoAt = time.Now()
	store.hashSizeInfoMu.Unlock()
	_, router := serverFor(t, db, store)

	req := httptest.NewRequest("GET", "/api/analytics/hash-collisions", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	var body hashCollisionsBody
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("bad JSON: %v", err)
	}
	oneByte := body.BySize["1"]
	if oneByte.Stats.CollisionCount < 1 {
		t.Errorf("expected at least 1 collision (CC prefix), got %d", oneByte.Stats.CollisionCount)
	}

	found := false
	for _, c := range oneByte.Collisions {
		if c.Prefix == "CC" {
			found = true
			if len(c.Nodes) < 2 {
				t.Errorf("expected >=2 nodes for CC collision, got %d", len(c.Nodes))
			}
			if c.Classification != "local" {
				t.Errorf("expected 'local' classification for nearby nodes, got %s", c.Classification)
			}
		}
	}
	if !found {
		t.Error("expected collision entry with prefix 'CC'")
	}
}

func TestHashCollisionsMissingCoordinates(t *testing.T) {
	db := setupTestDB(t)
	now := time.Now().UTC().Add(-1 * time.Hour)
	pk1 := padKey("bb11223344556677")
	pk2 := padKey("bb99887766554433")
	seedNode(t, db, pk1, "NoCoords1", "repeater", nil, nil, now)
	seedNode(t, db, pk2, "NoCoords2", "repeater", nil, nil, now)
	store := loadedStore(t, db)

	store.hashSizeInfoMu.Lock()
	store.hashSizeInfoCache = map[string]*hashSizeNodeInfo{
		pk1: {HashSize: 1, AllSizes: map[int]bool{1: true}},
		pk2: {HashSize: 1, AllSizes: map[int]bool{1: true}},
	}
	store.hashSizeInfoAt = time.Now()
	store.hashSizeInfoMu.Unlock()
	_, router := serverFor(t, db, store)

	req := httptest.NewRequest("GET", "/api/analytics/hash-collisions", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	var body hashCollisionsBody
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("bad JSON: %v", err)
	}
	found := false
	for _, c := range body.BySize["1"].Collisions {
		if c.Prefix == "BB" {
			found = true
			if c.Classification != "incomplete" {
				t.Errorf("expected 'incomplete' for nodes without coords, got %s", c.Classification)
			}
		}
	}
	if !found {
		t.Error("expected collision entry with prefix 'BB'")
	}
}

// TestHashCollisionsOnlyRepeaters verifies that only repeater nodes
// are included in collision analysis. Companions, rooms, sensors, and
// hash_size==0 nodes are excluded — per firmware analysis, only repeaters
// forward packets and appear in path[] arrays. (#441)
func TestHashCollisionsOnlyRepeaters(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	now := time.Now().UTC()

	rep1 := padKey("aa11223344556677")
	rep2 := padKey("aa00112233445566")
	unknown := padKey("aa99887766554433")
	companion := padKey("aadeadbeefcafe01")
	room := padKey("aabbcc1122334455")
	sensor := padKey("aabbcc9988776655")
	seedNode(t, db, rep1, "Repeater1", "repeater", nil, nil, now)
	seedNode(t, db, rep2, "Repeater2", "repeater", nil, nil, now)
	seedNode(t, db, unknown, "UnknownNode", "repeater", nil, nil, now)
	seedNode(t, db, companion, "Companion1", "companion", nil, nil, now)
	seedNode(t, db, room, "Room1", "room", nil, nil, now)
	seedNode(t, db, sensor, "Sensor1", "sensor", nil, nil, now)
	store := loadedStore(t, db)

	store.hashSizeInfoMu.Lock()
	store.hashSizeInfoCache = map[string]*hashSizeNodeInfo{
		rep1:      {HashSize: 1, AllSizes: map[int]bool{1: true}},
		rep2:      {HashSize: 1, AllSizes: map[int]bool{1: true}},
		unknown:   {HashSize: 0, AllSizes: map[int]bool{}},
		companion: {HashSize: 1, AllSizes: map[int]bool{1: true}},
		room:      {HashSize: 1, AllSizes: map[int]bool{1: true}},
		sensor:    {HashSize: 1, AllSizes: map[int]bool{1: true}},
	}
	store.hashSizeInfoAt = time.Now()
	store.hashSizeInfoMu.Unlock()

	result := store.computeHashCollisions(ctx, "", "")

	size1, ok := result.BySize["1"]
	if !ok {
		t.Fatal("missing by_size[1]")
	}
	if nodesForByte := size1.Stats.NodesForByte; nodesForByte != 2 {
		t.Errorf("expected nodes_for_byte=2 (only repeaters with hash_size=1), got %v", nodesForByte)
	}
	collisions := size1.Collisions
	if len(collisions) != 1 {
		t.Errorf("expected 1 collision entry, got %d", len(collisions))
	}
	if len(collisions) == 1 && len(collisions[0].Nodes) != 2 {
		t.Errorf("expected 2 nodes in collision, got %d", len(collisions[0].Nodes))
	}
}

// TestHashCollisionsOneByteIncludesMultiBytePrefixRepeaters verifies that the
// 1-byte Hash Usage Matrix view includes the first byte of repeaters configured
// for 2-byte and 3-byte prefixes. In MeshCore, a multi-byte hash repeater still
// occupies its first byte in the 1-byte hash space, so any 1-byte path-matching
// collides on that first byte regardless of the configured prefix size. Omitting
// these under-reports real conflicts in the 1-byte space. (#1218)
func TestHashCollisionsOneByteIncludesMultiBytePrefixRepeaters(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	now := time.Now().UTC()

	rep1B := padKey("cc11223344556677")
	rep2B := padKey("cc22aabbccddeeff")
	rep3B := padKey("cc33bbddeeff0011")
	repDD := padKey("dd44556677889900")
	seedNode(t, db, rep1B, "Rep1B", "repeater", nil, nil, now)
	seedNode(t, db, rep2B, "Rep2B", "repeater", nil, nil, now)
	seedNode(t, db, rep3B, "Rep3B", "repeater", nil, nil, now)
	seedNode(t, db, repDD, "RepDD", "repeater", nil, nil, now)
	store := loadedStore(t, db)

	store.hashSizeInfoMu.Lock()
	store.hashSizeInfoCache = map[string]*hashSizeNodeInfo{
		rep1B: {HashSize: 1, AllSizes: map[int]bool{1: true}},
		rep2B: {HashSize: 2, AllSizes: map[int]bool{2: true}},
		rep3B: {HashSize: 3, AllSizes: map[int]bool{3: true}},
		repDD: {HashSize: 1, AllSizes: map[int]bool{1: true}},
	}
	store.hashSizeInfoAt = time.Now()
	store.hashSizeInfoMu.Unlock()

	result := store.computeHashCollisions(ctx, "", "")
	cells := result.BySize["1"].OneByteCells
	if cells == nil {
		t.Fatal("by_size[1] has no one_byte_cells")
	}
	ccNodes := cells["CC"]
	if len(ccNodes) != 3 {
		t.Errorf("expected 3 nodes in one_byte_cells[CC] (1B + 2B + 3B repeaters), got %d", len(ccNodes))
	}
	seen := map[string]bool{}
	for _, n := range ccNodes {
		seen[strings.ToLower(n.PublicKey)] = true
	}
	for _, pk := range []string{rep1B, rep2B, rep3B} {
		if !seen[pk] {
			t.Errorf("expected one_byte_cells[CC] to include %s, missing", pk)
		}
	}
	if seen[repDD] {
		t.Errorf("one_byte_cells[CC] unexpectedly contains DD repeater")
	}

	if n := result.BySize["2"].Stats.NodesForByte; n != 1 {
		t.Errorf("expected 2-byte view nodes_for_byte=1, got %v", n)
	}
}

func TestNodePathsEndpointUsesIndex(t *testing.T) {
	srv, router := setupTestServer(t)

	// Verify byPathHop index was built during Load
	srv.store.mu.RLock()
	hopKeys := len(srv.store.byPathHop)
	srv.store.mu.RUnlock()
	if hopKeys == 0 {
		t.Fatal("byPathHop index is empty after Load")
	}

	// Query paths for TestRepeater (pubkey aabbccdd11223344, prefix "aa")
	// Should find transmissions with hop "aa" in path
	req := httptest.NewRequest("GET", "/api/nodes/aabbccdd11223344/paths", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp struct {
		Paths              []json.RawMessage `json:"paths"`
		TotalTransmissions int               `json:"totalTransmissions"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("bad JSON: %v", err)
	}

	// Transmission 1 has path ["aa","bb"] which contains "aa" matching prefix of aabbccdd11223344
	if resp.TotalTransmissions == 0 {
		t.Error("expected at least 1 transmission matching node paths")
	}
	if len(resp.Paths) == 0 {
		t.Error("expected at least 1 path group")
	}
}

func TestNodePathsPrefixCollisionFilter(t *testing.T) {
	srv, router := setupTestServer(t)
	db := srv.db

	now := time.Now().UTC()
	recent := now.Add(-30 * time.Minute)
	targetPK := padKey("aabbccdd11223344")
	collisionPK := padKey("aacafe0000000000")
	farPK := padKey("eeff00112233aabb")
	seedNode(t, db, targetPK, "TestRepeater", "repeater", nil, nil, recent)
	seedNode(t, db, collisionPK, "CollisionNode", "repeater", nil, nil, recent)
	seedNode(t, db, farPK, "FarNode", "repeater", nil, nil, recent)
	obsIdx := seedObserver(t, db, scopeKey(103), "SFO", now)

	const collisionHash = "c011150000000001"
	insertRelayPacket(t, db, obsIdx, relayPacket{
		Hash: collisionHash, RouteType: 1, PayloadType: types.PayloadType(4), At: recent,
		Hops: []string{"aa", "bb"}, Resolved: []*string{sptr(collisionPK), sptr(farPK)},
	})

	srv.store = loadedStore(t, db)

	req := httptest.NewRequest("GET", "/api/nodes/"+targetPK+"/paths", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), collisionHash) {
		t.Error("collision packet should have been filtered out but appeared in response")
	}

	req2 := httptest.NewRequest("GET", "/api/nodes/"+collisionPK+"/paths", nil)
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)
	if w2.Code != 200 {
		t.Fatalf("expected 200 for CollisionNode, got %d: %s", w2.Code, w2.Body.String())
	}
	if !strings.Contains(w2.Body.String(), collisionHash) {
		t.Error("collision packet should appear for CollisionNode but was missing")
	}
}

func TestPathHopIndexIncrementalUpdate(t *testing.T) {
	// After #800, addTxToPathHopIndex only indexes raw hops (not resolved pubkeys).
	// Resolved pubkeys are handled by the resolved pubkey membership index.
	idx := make(map[string][]*StoreTx)

	tx1 := &StoreTx{
		ID:       1,
		PathJSON: `["ab","cd"]`,
	}

	addTxToPathHopIndex(idx, tx1)

	// Should be indexed under "ab" and "cd" only (no resolved pubkey)
	if len(idx["ab"]) != 1 {
		t.Errorf("expected 1 entry for 'ab', got %d", len(idx["ab"]))
	}
	if len(idx["cd"]) != 1 {
		t.Errorf("expected 1 entry for 'cd', got %d", len(idx["cd"]))
	}

	// Add another tx with overlapping hop
	tx2 := &StoreTx{
		ID:       2,
		PathJSON: `["ab","ef"]`,
	}
	addTxToPathHopIndex(idx, tx2)

	if len(idx["ab"]) != 2 {
		t.Errorf("expected 2 entries for 'ab', got %d", len(idx["ab"]))
	}
	if len(idx["ef"]) != 1 {
		t.Errorf("expected 1 entry for 'ef', got %d", len(idx["ef"]))
	}

	// Remove tx1
	removeTxFromPathHopIndex(idx, tx1)

	if len(idx["ab"]) != 1 {
		t.Errorf("expected 1 entry for 'ab' after removal, got %d", len(idx["ab"]))
	}
	if _, ok := idx["cd"]; ok {
		t.Error("expected 'cd' key to be deleted after removal")
	}
}

func TestMetricsAPIEndpoints(t *testing.T) {
	ctx := t.Context()
	srv, router := setupTestServer(t)

	now := time.Now().UTC()
	observerKey := scopeKey(105)
	seedObserver(t, srv.db, observerKey, "SFO", now)
	if err := srv.db.CreateOrUpdateObserverMetrics(ctx, database.ObserverMetricsInput{
		ObserverID: observerKey,
		Timestamp:  now.Add(-1 * time.Hour),
		NoiseFloor: fptr(-112.0),
	}); err != nil {
		t.Fatalf("seed metrics: %v", err)
	}

	req := httptest.NewRequest("GET", "/api/observers/"+observerKey+"/metrics", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("GET metrics = %d, want 200", w.Code)
	}
	var resp struct {
		Metrics []json.RawMessage `json:"metrics"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("bad JSON: %v", err)
	}
	if len(resp.Metrics) != 1 {
		t.Errorf("expected 1 metric in response, got %d", len(resp.Metrics))
	}

	req2 := httptest.NewRequest("GET", "/api/observers/metrics/summary?window=24h", nil)
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)
	if w2.Code != 200 {
		t.Fatalf("GET summary = %d, want 200", w2.Code)
	}
	var resp2 struct {
		Observers []json.RawMessage `json:"observers"`
	}
	if err := json.Unmarshal(w2.Body.Bytes(), &resp2); err != nil {
		t.Fatalf("bad JSON: %v", err)
	}
	if len(resp2.Observers) != 1 {
		t.Errorf("expected 1 observer in summary, got %d", len(resp2.Observers))
	}
}

// TestNodeHealth_RecentPackets_ResolvedPath verifies that recentPackets in the
// node health endpoint include resolved_path (regression for Codex review item #2).
func TestNodeHealth_RecentPackets_ResolvedPath(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/nodes/aabbccdd11223344/health", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}
	var body map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("json decode: %v", err)
	}
	rp, ok := body["recentPackets"].([]interface{})
	if !ok || len(rp) == 0 {
		t.Fatal("expected non-empty recentPackets")
	}
	// At least one packet should have resolved_path (tx 1 has observations with resolved_path)
	found := false
	for _, p := range rp {
		pm, ok := p.(map[string]interface{})
		if !ok {
			continue
		}
		if pm["resolved_path"] != nil {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected at least one recentPacket with resolved_path")
	}
}

// TestPacketsExpand_ResolvedPath verifies that expandObservations=true includes
// resolved_path on expanded observations (regression for Codex review item #3).
func TestPacketsExpand_ResolvedPath(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/packets?expand=observations&limit=10", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}
	var body map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("json decode: %v", err)
	}
	packets, ok := body["packets"].([]interface{})
	if !ok || len(packets) == 0 {
		t.Fatal("expected non-empty packets")
	}
	// Find a packet with observations that should have resolved_path
	found := false
	for _, p := range packets {
		pm, ok := p.(map[string]interface{})
		if !ok {
			continue
		}
		obs, ok := pm["observations"].([]interface{})
		if !ok {
			continue
		}
		for _, o := range obs {
			om, ok := o.(map[string]interface{})
			if !ok {
				continue
			}
			if om["resolved_path"] != nil {
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	if !found {
		t.Error("expected at least one expanded observation with resolved_path")
	}
}

// TestPacketDetailFallsBackToDBWhenStoreMisses verifies that handlePacketDetail
// serves transmissions present in the DB but absent from the in-memory store.
// This is the recentAdverts → "Not found" bug (#827).
func TestPacketDetailFallsBackToDBWhenStoreMisses(t *testing.T) {
	ctx := t.Context()
	srv, router := setupTestServer(t)
	db := srv.db

	const dbOnlyHash = "deadbeef00112233"
	now := time.Now().UTC()
	obsIdx := seedObserver(t, db, scopeKey(104), "SFO", now)
	insertRelayPacket(t, db, obsIdx, relayPacket{
		Hash: dbOnlyHash, RouteType: 1, PayloadType: types.PayloadType(4), At: now,
		Hops: nil, Resolved: nil,
	})

	if got, err := srv.store.GetPacketByHash(ctx, dbOnlyHash); err == nil && got != nil {
		t.Fatalf("test precondition failed: store unexpectedly has %s", dbOnlyHash)
	}

	req := httptest.NewRequest("GET", "/api/packets/"+dbOnlyHash, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}
	var body struct {
		Packet struct {
			Hash string `json:"hash"`
		} `json:"packet"`
		Observations []json.RawMessage `json:"observations"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Packet.Hash != dbOnlyHash {
		t.Errorf("expected hash %s, got %v", dbOnlyHash, body.Packet.Hash)
	}
	if len(body.Observations) == 0 {
		t.Errorf("expected DB observations to be returned, got 0")
	}
}

// TestPacketDetail404WhenAbsentFromBoth verifies that a hash present in
// neither store nor DB still returns 404 (no false positives from the fallback).
func TestPacketDetail404WhenAbsentFromBoth(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/packets/0011223344556677", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 404 {
		t.Errorf("expected 404, got %d (body: %s)", w.Code, w.Body.String())
	}
}

// TestPacketDetailPrefersStoreOverDB verifies the store result wins when the
// hash exists in both — the DB fallback must not double-fetch / overwrite.
func TestPacketDetailPrefersStoreOverDB(t *testing.T) {
	ctx := t.Context()
	srv, router := setupTestServer(t)
	// abc123def4567890 is seeded in both DB and (after Load) the store.
	const hash = "abc123def4567890"
	got, err := srv.store.GetPacketByHash(ctx, hash)
	if got == nil {
		t.Fatalf("test precondition failed: store should have %s", hash)
	}
	if err != nil {
		t.Fatalf("Failed to get Packet for Hash:,%s", hash)
	}

	req := httptest.NewRequest("GET", "/api/packets/"+hash, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	pkt, _ := body["packet"].(map[string]interface{})
	if pkt == nil || pkt["hash"] != hash {
		t.Fatalf("expected packet with hash %s, got %v", hash, pkt)
	}
	// observation_count comes from store observations (2 seeded for tx 1).
	if cnt, _ := body["observation_count"].(float64); cnt != 2 {
		t.Errorf("expected observation_count=2 (from store), got %v", body["observation_count"])
	}
}

func TestHandleScopeStats(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	rows := []struct {
		hash  string
		scope *string
		route types.RouteType
	}{
		{"5c0e000000000001", sptr("#belgium"), 0},
		{"5c0e000000000002", sptr("#belgium"), 3},
		{"5c0e000000000003", sptr(""), 0},
		{"5c0e000000000004", nil, 0},
	}
	for i, r := range rows {
		if _, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
			Hash:        r.hash,
			RawHex:      "aa",
			RouteType:   r.route,
			PayloadType: 5,
			DecodedJSON: "{}",
			ScopeName:   r.scope,
		}, now); err != nil {
			t.Fatalf("seed row %d: %v", i, err)
		}
	}

	store := loadedStore(t, db)
	srv, _ := serverFor(t, db, store)

	req := httptest.NewRequest("GET", "/api/scope-stats?window=24h", nil)
	w := httptest.NewRecorder()
	srv.handleScopeStats(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
	var resp api.ScopeStatsResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Window != "24h" {
		t.Errorf("window = %q, want 24h", resp.Window)
	}
	if resp.Summary.TransportTotal != 4 {
		t.Errorf("transportTotal = %d, want 4", resp.Summary.TransportTotal)
	}
	if resp.Summary.Scoped != 3 {
		t.Errorf("scoped = %d, want 3", resp.Summary.Scoped)
	}
	if resp.Summary.Unscoped != 1 {
		t.Errorf("unscoped = %d, want 1", resp.Summary.Unscoped)
	}
	if resp.Summary.UnknownScope != 1 {
		t.Errorf("unknownScope = %d, want 1", resp.Summary.UnknownScope)
	}
	if len(resp.ByRegion) != 1 || resp.ByRegion[0].Name != "#belgium" || resp.ByRegion[0].Count != 2 {
		t.Errorf("byRegion = %v, want [{#belgium 2}]", resp.ByRegion)
	}
	if resp.TimeSeries == nil {
		t.Error("timeSeries is nil")
	}
}

func TestHandleScopeStatsInvalidWindow(t *testing.T) {
	srv, _ := setupTestServer(t)

	req := httptest.NewRequest("GET", "/api/scope-stats?window=invalid", nil)
	w := httptest.NewRecorder()
	srv.handleScopeStats(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

// --- geo-filter write-back tests ---

func setupGeoFilterServer(t *testing.T, apiKey string) (*Server, *mux.Router, string) {
	ctx := t.Context()

	t.Helper()
	dir := t.TempDir()
	cfgJSON := `{"port":3000,"apiKey":"` + apiKey + `"}`
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(cfgJSON), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	db := setupTestDB(t)
	seedTestData(t, db)
	cfg := &Config{Port: 3000, APIKey: apiKey}
	hub := NewHub()
	srv := NewServer(db, cfg, hub)
	srv.configDir = dir
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	srv.store = store
	router := mux.NewRouter()
	srv.RegisterRoutes(router)
	return srv, router, dir
}

func TestPutConfigGeoFilter(t *testing.T) {
	const apiKey = "a-strong-api-key-for-testing"

	t.Run("saves valid polygon and updates in-memory config", func(t *testing.T) {
		srv, router, dir := setupGeoFilterServer(t, apiKey)

		body := `{"polygon":[[51.0,4.0],[51.0,5.0],[50.5,5.0],[50.5,4.0]],"bufferKm":15}`
		req := httptest.NewRequest("PUT", "/api/config/geo-filter", strings.NewReader(body))
		req.Header.Set("X-API-Key", apiKey)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != 200 {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		// In-memory config updated
		if srv.cfg.GeoFilter == nil {
			t.Fatal("expected in-memory GeoFilter to be set")
		}
		if len(srv.cfg.GeoFilter.Polygon) != 4 {
			t.Errorf("expected 4 polygon points, got %d", len(srv.cfg.GeoFilter.Polygon))
		}
		if srv.cfg.GeoFilter.BufferKm != 15 {
			t.Errorf("expected bufferKm=15, got %v", srv.cfg.GeoFilter.BufferKm)
		}

		// config.json updated on disk
		data, _ := os.ReadFile(filepath.Join(dir, "config.json"))
		if !bytes.Contains(data, []byte("geo_filter")) {
			t.Error("expected geo_filter key in saved config.json")
		}
	})

	t.Run("clears filter when polygon is empty", func(t *testing.T) {
		srv, router, dir := setupGeoFilterServer(t, apiKey)
		// Pre-set a filter so we can clear it
		srv.setGeoFilter(&GeoFilterConfig{Polygon: [][2]float64{{51.0, 4.0}, {51.0, 5.0}, {50.5, 4.0}}, BufferKm: 10})

		req := httptest.NewRequest("PUT", "/api/config/geo-filter", strings.NewReader(`{"polygon":null}`))
		req.Header.Set("X-API-Key", apiKey)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
		if srv.cfg.GeoFilter != nil {
			t.Error("expected in-memory GeoFilter to be cleared")
		}
		data, _ := os.ReadFile(filepath.Join(dir, "config.json"))
		if bytes.Contains(data, []byte("geo_filter")) {
			t.Error("expected geo_filter to be removed from config.json")
		}
	})

	t.Run("rejects polygon with fewer than 3 points", func(t *testing.T) {
		_, router, _ := setupGeoFilterServer(t, apiKey)

		body := `{"polygon":[[51.0,4.0],[51.0,5.0]],"bufferKm":0}`
		req := httptest.NewRequest("PUT", "/api/config/geo-filter", strings.NewReader(body))
		req.Header.Set("X-API-Key", apiKey)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", w.Code)
		}
	})

	t.Run("rejects out-of-range coordinates", func(t *testing.T) {
		_, router, _ := setupGeoFilterServer(t, apiKey)

		body := `{"polygon":[[91.0,4.0],[51.0,5.0],[50.5,4.0]],"bufferKm":0}`
		req := httptest.NewRequest("PUT", "/api/config/geo-filter", strings.NewReader(body))
		req.Header.Set("X-API-Key", apiKey)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for out-of-range lat, got %d", w.Code)
		}
	})

	t.Run("rejects polygon exceeding 1000 points", func(t *testing.T) {
		_, router, _ := setupGeoFilterServer(t, apiKey)

		pts := make([][2]float64, 1001)
		for i := range pts {
			pts[i] = [2]float64{51.0 + float64(i)*0.0001, 4.0}
		}
		b, _ := json.Marshal(map[string]interface{}{"polygon": pts, "bufferKm": 0})
		req := httptest.NewRequest("PUT", "/api/config/geo-filter", strings.NewReader(string(b)))
		req.Header.Set("X-API-Key", apiKey)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for oversized polygon, got %d", w.Code)
		}
	})

	t.Run("rejects missing API key", func(t *testing.T) {
		_, router, _ := setupGeoFilterServer(t, apiKey)

		body := `{"polygon":[[51.0,4.0],[51.0,5.0],[50.5,4.0]],"bufferKm":0}`
		req := httptest.NewRequest("PUT", "/api/config/geo-filter", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", w.Code)
		}
	})
}

func TestSaveGeoFilter(t *testing.T) {
	t.Run("saves and reads back", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(`{"port":3000}`), 0644); err != nil {
			t.Fatal(err)
		}
		gf := &GeoFilterConfig{
			Polygon:  [][2]float64{{51.0, 4.0}, {51.0, 5.0}, {50.5, 4.0}},
			BufferKm: 20,
		}
		if err := SaveGeoFilter(dir, gf); err != nil {
			t.Fatalf("SaveGeoFilter: %v", err)
		}
		data, _ := os.ReadFile(filepath.Join(dir, "config.json"))
		if !bytes.Contains(data, []byte("geo_filter")) {
			t.Error("expected geo_filter in saved config")
		}
		if !bytes.Contains(data, []byte(`"bufferKm"`)) {
			t.Error("expected bufferKm in saved config")
		}
	})

	t.Run("removes geo_filter key when gf is nil", func(t *testing.T) {
		dir := t.TempDir()
		initial := `{"port":3000,"geo_filter":{"polygon":[[1,2],[3,4],[5,6]],"bufferKm":5}}`
		if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(initial), 0644); err != nil {
			t.Fatal(err)
		}
		if err := SaveGeoFilter(dir, nil); err != nil {
			t.Fatalf("SaveGeoFilter: %v", err)
		}
		data, _ := os.ReadFile(filepath.Join(dir, "config.json"))
		if bytes.Contains(data, []byte("geo_filter")) {
			t.Error("expected geo_filter to be removed")
		}
	})

	t.Run("returns error when config.json not found", func(t *testing.T) {
		dir := t.TempDir()
		err := SaveGeoFilter(dir, nil)
		if err == nil {
			t.Error("expected error when config.json not found")
		}
	})
}

// --- prune-geo-filter endpoint tests ---

func setupPruneGeoFilterServer(t *testing.T, apiKey string, gf *GeoFilterConfig) (*Server, *mux.Router) {
	t.Helper()
	db := setupTestDB(t)

	now := time.Now().UTC().Add(-1 * time.Hour)
	seedNode(t, db, padKey("1111111122223333"), "InsideNode1", "repeater", fptr(37.5), fptr(-122.0), now)
	seedNode(t, db, padKey("2222111122223333"), "InsideNode2", "repeater", fptr(37.4), fptr(-121.9), now)
	seedNode(t, db, padKey("aaaa111122223333"), "OutsideNode", "repeater", fptr(51.5), fptr(4.5), now)
	seedNode(t, db, padKey("bbbb111122223333"), "NoGPSNode", "companion", nil, nil, now)

	store := loadedStore(t, db)
	srv := NewServer(db, &Config{Port: 3000, APIKey: apiKey, GeoFilter: gf}, NewHub())
	srv.store = store
	router := mux.NewRouter()
	srv.RegisterRoutes(router)
	return srv, router
}

func TestPruneGeoFilterEndpoint(t *testing.T) {
	const apiKey = "a-strong-api-key-for-testing"

	gf := &GeoFilterConfig{
		Polygon:  [][2]float64{{37.0, -123.0}, {38.0, -123.0}, {38.0, -121.0}, {37.0, -121.0}},
		BufferKm: 0,
	}

	t.Run("dry run returns outside nodes without deleting", func(t *testing.T) {
		_, router := setupPruneGeoFilterServer(t, apiKey, gf)

		req := httptest.NewRequest("POST", "/api/admin/prune-geo-filter", nil)
		req.Header.Set("X-API-Key", apiKey)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != 200 {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
		var body struct {
			DryRun bool `json:"dryRun"`
			Count  int  `json:"count"`
			Nodes  []struct {
				Name string `json:"name"`
			} `json:"nodes"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("bad JSON: %v", err)
		}
		if !body.DryRun {
			t.Error("expected dryRun=true")
		}
		if body.Count != 1 {
			t.Errorf("expected 1 outside node (OutsideNode), got %d", body.Count)
		}
		if len(body.Nodes) != 1 {
			t.Fatalf("expected 1 node in preview, got %d", len(body.Nodes))
		}
		if body.Nodes[0].Name != "OutsideNode" {
			t.Errorf("expected OutsideNode, got %v", body.Nodes[0].Name)
		}
	})

	t.Run("status endpoint returns 404 for unknown id", func(t *testing.T) {
		_, router := setupPruneGeoFilterServer(t, apiKey, gf)
		req := httptest.NewRequest("GET", "/api/admin/prune-geo-filter/status?id=deadbeefdeadbeef", nil)
		req.Header.Set("X-API-Key", apiKey)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d", w.Code)
		}
	})

	t.Run("status endpoint rejects path-traversal-looking id", func(t *testing.T) {
		_, router := setupPruneGeoFilterServer(t, apiKey, gf)
		req := httptest.NewRequest("GET", "/api/admin/prune-geo-filter/status?id=../../etc/passwd", nil)
		req.Header.Set("X-API-Key", apiKey)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", w.Code)
		}
	})

	t.Run("confirm=true without pubkeys body returns 400", func(t *testing.T) {
		_, router := setupPruneGeoFilterServer(t, apiKey, gf)

		req := httptest.NewRequest("POST", "/api/admin/prune-geo-filter?confirm=true", nil)
		req.Header.Set("X-API-Key", apiKey)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
		}
	})

	t.Run("returns 400 when no geo filter configured", func(t *testing.T) {
		_, router := setupPruneGeoFilterServer(t, apiKey, nil)

		req := httptest.NewRequest("POST", "/api/admin/prune-geo-filter", nil)
		req.Header.Set("X-API-Key", apiKey)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", w.Code)
		}
	})

	t.Run("returns 401 without API key", func(t *testing.T) {
		_, router := setupPruneGeoFilterServer(t, apiKey, gf)

		req := httptest.NewRequest("POST", "/api/admin/prune-geo-filter", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", w.Code)
		}
	})
}

func TestGetNodesForGeoPrune(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	nodes, err := db.ListNodesForGeoPrune(ctx)
	if err != nil {
		t.Fatalf("GetNodesForGeoPrune: %v", err)
	}
	if len(nodes) == 0 {
		t.Error("expected nodes to be returned")
	}
	// Check that nodes with lat/lon have non-nil fields
	for _, n := range nodes {
		if n.Pubkey == "" {
			t.Error("expected non-empty pubkey")
		}
	}
}

// TestDeleteNodesByPubkeys was removed in PR #738 follow-up: the DELETE has
// been relocated to the ingestor (cmd/ingestor/prune_geofilter.go). End-to-end
// TestListLimitsConfigurable verifies that list limits are driven by the configuration.
func TestListLimitsConfigurable(t *testing.T) {
	srv, router := setupTestServer(t)

	// Inject a custom config
	srv.cfg.ListLimits = &ListLimitsConfig{
		PacketsMax: 1234,
	}

	// Request with a limit larger than the configured max
	req := httptest.NewRequest("GET", "/api/packets?limit=999999", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("json decode: %v", err)
	}

	limit, ok := body["limit"].(float64)
	if !ok {
		t.Fatal("expected limit field in response")
	}

	if limit != 1234 {
		t.Errorf("expected limit to be capped at 1234, got %v", limit)
	}
}

// TestConfigThemeTypeColorsLegacyRequestKey verifies the REQUEST→REQ rename
// (#1799 PR #1804 r1 item 6) doesn't break operators whose config.json still
// carries the legacy `typeColors.REQUEST` key. The GET response must:
//   - accept a config that supplies "REQUEST" and have that value win the
//     mergeMap precedence for the corresponding logical slot
//   - emit BOTH "REQ" and "REQUEST" in typeColors for ≥1 release cycle so
//     downstream consumers reading the legacy key keep working
func TestConfigThemeTypeColorsLegacyRequestKey(t *testing.T) {
	db := setupTestDB(t)
	seedTestData(t, db)
	cfg := &Config{
		Port: 3000,
		TypeColors: map[string]interface{}{
			// Operator's stale config — legacy key only.
			"REQUEST": "#deadbe",
		},
	}
	hub := NewHub()
	srv := NewServer(db, cfg, hub)
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	req := httptest.NewRequest("GET", "/api/config/theme", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	tc, ok := body["typeColors"].(map[string]interface{})
	if !ok {
		t.Fatalf("typeColors missing or wrong shape: %v", body["typeColors"])
	}
	// Legacy operator override must propagate to the canonical REQ slot.
	if tc["REQ"] != "#deadbe" {
		t.Errorf("typeColors.REQ: want #deadbe (from legacy REQUEST override), got %v", tc["REQ"])
	}
	// Back-compat emission: REQUEST also present, equal to REQ.
	if tc["REQUEST"] != "#deadbe" {
		t.Errorf("typeColors.REQUEST: want #deadbe (back-compat dual-emit), got %v", tc["REQUEST"])
	}
}
