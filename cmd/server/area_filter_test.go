package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/gorilla/mux"
)

func mustExecDB(t *testing.T, db *database.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.SQL().ExecContext(t.Context(), q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

func TestAreaEntryParsing(t *testing.T) {
	raw := `{
		"port": 3000,
		"areas": {
			"BEL": {
				"label": "Belgium",
				"polygon": [[50.0, 2.5], [51.5, 2.5], [51.5, 6.4], [50.0, 6.4]]
			},
			"BOX": {
				"label": "Bounding Box Area",
				"latMin": 50.0, "latMax": 51.5, "lonMin": 2.5, "lonMax": 6.4
			}
		}
	}`
	var cfg Config
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(cfg.Areas) != 2 {
		t.Fatalf("want 2 areas, got %d", len(cfg.Areas))
	}
	bel := cfg.Areas["BEL"]
	if bel.Label != "Belgium" {
		t.Errorf("label: want Belgium, got %q", bel.Label)
	}
	if len(bel.Polygon) != 4 {
		t.Errorf("polygon: want 4 points, got %d", len(bel.Polygon))
	}
	box := cfg.Areas["BOX"]
	if box.LatMin == nil || *box.LatMin != 50.0 {
		t.Error("LatMin not parsed")
	}
}

func TestGetNodePubkeysInArea_Polygon(t *testing.T) {
	db := setupTestDB(t)
	s := &Server{db: db}
	t.Cleanup(func() { db.Close() })
	dbtest.NodeAt(t, db, "pk-inside", 50.85, 4.35)
	dbtest.NodeAt(t, db, "pk-outside", 48.0, 4.35)
	dbtest.Node(t, db, "pk-nogps", "", "")
	dbtest.NodeAt(t, db, "pk-zero", 0.0, 0.0)

	entry := AreaEntry{
		Label:   "Belgium",
		Polygon: [][2]float64{{50.0, 2.5}, {51.5, 2.5}, {51.5, 6.4}, {50.0, 6.4}},
	}
	pks, err := s.nodePubkeysInArea(t.Context(), entry)
	if err != nil {
		t.Fatalf("GetNodePubkeysInArea: %v", err)
	}
	if len(pks) != 1 || pks[0] != "pk-inside" {
		t.Errorf("want [pk-inside], got %v", pks)
	}
}

// newTestStoreWithDB builds a minimal PacketStore wired to the given DB and config.
func newTestStoreWithDB(t *testing.T, db *database.DB, cfg *Config) *PacketStore {
	t.Helper()
	ctx := t.Context()

	store := newPacketStoreForTest(t, db)
	if cfg != nil {
		if cfg.SnapshotDir == "" {
			cfg.SnapshotDir = t.TempDir()
		}
		store.config = cfg
	}
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache: %v", err)
	}
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	return store
}

func TestResolveAreaNodes_UnknownKey(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()
	t.Cleanup(func() { db.Close() })
	cfg := &Config{Areas: map[string]AreaEntry{
		"BEL": {Label: "Belgium", Polygon: [][2]float64{{50.0, 2.5}, {51.5, 2.5}, {51.5, 6.4}, {50.0, 6.4}}},
	}}
	s := newTestStoreWithDB(t, db, cfg)
	result := s.resolveAreaNodes(ctx, "UNKNOWN")
	if result != nil {
		t.Errorf("want nil for unknown area, got %v", result)
	}
}

func TestResolveAreaNodes_CacheHit(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()
	t.Cleanup(func() { db.Close() })
	mustExecDB(t, db, `INSERT INTO nodes (public_key, lat, lon, last_seen, first_seen) VALUES ('pk1', 50.85, 4.35, now(), now())`)

	cfg := &Config{Areas: map[string]AreaEntry{
		"BEL": {Label: "Belgium", Polygon: [][2]float64{{50.0, 2.5}, {51.5, 2.5}, {51.5, 6.4}, {50.0, 6.4}}},
	}}
	s := newTestStoreWithDB(t, db, cfg)

	r1 := s.resolveAreaNodes(ctx, "BEL")
	if !r1["pk1"] {
		t.Fatal("pk1 should be in area BEL on first call")
	}

	// Delete node so a live DB query would return nothing — second call must use cache.
	mustExecDB(t, db, `DELETE FROM nodes WHERE public_key = 'pk1'`)

	r2 := s.resolveAreaNodes(ctx, "BEL")
	if !r2["pk1"] {
		t.Fatal("cache hit should still return pk1 after DB delete")
	}
}

// ingestAdvert adds a synthetic ADVERT packet to the store's in-memory packet list.
func ingestAdvert(t *testing.T, s *PacketStore, hash, decodedJSON string) {
	t.Helper()
	pt := types.PayloadADVERT
	tx := &StoreTx{
		Hash:        hash,
		FirstSeen:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		PayloadType: &pt,
		DecodedJSON: decodedJSON,
	}
	s.mu.Lock()
	s.packets = append(s.packets, tx)
	s.byHash[hash] = tx
	s.byPayloadType[pt] = append(s.byPayloadType[pt], tx)
	s.mu.Unlock()
}

func TestFilterPacketsByArea(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()
	dbtest.NodeAt(t, db, "inside-node", 50.85, 4.35)
	dbtest.NodeAt(t, db, "outside-node", 48.0, 4.35)

	cfg := &Config{Areas: map[string]AreaEntry{
		"BEL": {Label: "Belgium", Polygon: [][2]float64{{50.0, 2.5}, {51.5, 2.5}, {51.5, 6.4}, {50.0, 6.4}}},
	}}
	s := newTestStoreWithDB(t, db, cfg)

	ingestAdvert(t, s, "hash-in", `{"public_key":"inside-node","name":"Inside"}`)
	ingestAdvert(t, s, "hash-out", `{"public_key":"outside-node","name":"Outside"}`)

	result, err := s.QueryPackets(ctx, database.PacketQuery{Limit: 50, Area: "BEL"})
	if err != nil {
		t.Fatalf("QueryPackets(): %v", err)
	}
	if result.Total != 1 {
		t.Fatalf("want 1 packet in area BEL, got %d (packets: %v)", result.Total, result.Packets)
	}
}

func TestAnalyticsRFAreaFilter(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()
	t.Cleanup(func() { db.Close() })
	mustExecDB(t, db, `INSERT INTO nodes (public_key, lat, lon, last_seen, first_seen) VALUES ('inside-node', 50.85, 4.35, now(), now())`)
	mustExecDB(t, db, `INSERT INTO nodes (public_key, lat, lon, last_seen, first_seen) VALUES ('outside-node', 48.0, 4.35, now(), now())`)

	cfg := &Config{Areas: map[string]AreaEntry{
		"BEL": {Label: "Belgium", Polygon: [][2]float64{{50.0, 2.5}, {51.5, 2.5}, {51.5, 6.4}, {50.0, 6.4}}},
	}}
	s := newTestStoreWithDB(t, db, cfg)

	ingestAdvert(t, s, "hash-rf-in", `{"public_key":"inside-node","name":"Inside"}`)
	ingestAdvert(t, s, "hash-rf-out", `{"public_key":"outside-node","name":"Outside"}`)

	result := s.GetAnalyticsRF(ctx, "", "BEL")
	if result.TotalTransmissions != 1 {
		t.Errorf("want totalTransmissions=1 for BEL, got %d", result.TotalTransmissions)
	}
}

// ingestChanMsg adds a synthetic GRP_TXT packet with the given sender pubkey and channel hash.
func ingestChanMsg(t *testing.T, s *PacketStore, hash, senderPK string, chanHash int) {
	t.Helper()
	pt := types.PayloadGRP_TXT
	decodedJSON := fmt.Sprintf(`{"public_key":%q,"channelHash":%d}`, senderPK, chanHash)
	tx := &StoreTx{
		Hash:        hash,
		FirstSeen:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		PayloadType: &pt,
		DecodedJSON: decodedJSON,
	}
	s.mu.Lock()
	s.packets = append(s.packets, tx)
	s.byHash[hash] = tx
	s.byPayloadType[pt] = append(s.byPayloadType[pt], tx)
	s.mu.Unlock()
}

func TestAnalyticsChannelsAreaFilter(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()
	t.Cleanup(func() { db.Close() })
	mustExecDB(t, db, `INSERT INTO nodes (public_key, lat, lon, last_seen, first_seen) VALUES ('inside-node', 50.85, 4.35, now(), now())`)
	mustExecDB(t, db, `INSERT INTO nodes (public_key, lat, lon, last_seen, first_seen) VALUES ('outside-node', 48.0, 4.35, now(), now())`)

	cfg := &Config{Areas: map[string]AreaEntry{
		"BEL": {Label: "Belgium", Polygon: [][2]float64{{50.0, 2.5}, {51.5, 2.5}, {51.5, 6.4}, {50.0, 6.4}}},
	}}
	s := newTestStoreWithDB(t, db, cfg)

	// inside-node sends on channel hash 42, outside-node on channel hash 99.
	ingestChanMsg(t, s, "ch-in", "inside-node", 42)
	ingestChanMsg(t, s, "ch-out", "outside-node", 99)

	unfiltered := s.GetAnalyticsChannels(ctx, "", "")
	filtered := s.GetAnalyticsChannels(ctx, "", "BEL")
	if unfiltered.ActiveChannels != 2 {
		t.Errorf("want 2 active channels unfiltered, got %d", unfiltered.ActiveChannels)
	}
	if filtered.ActiveChannels != 1 {
		t.Errorf("want 1 active channel for BEL, got %d", filtered.ActiveChannels)
	}
}

func TestGetNodePubkeysInArea_BoundingBox(t *testing.T) {
	db := setupTestDB(t)
	t.Cleanup(func() { db.Close() })
	s := &Server{db: db}
	mustExecDB(t, db, `INSERT INTO nodes (public_key, lat, lon, last_seen, first_seen) VALUES ('in', 50.5, 5.0, now(), now())`)
	mustExecDB(t, db, `INSERT INTO nodes (public_key, lat, lon, last_seen, first_seen) VALUES ('out', 52.0, 5.0, now(), now())`)

	minLat, maxLat, minLon, maxLon := 50.0, 51.5, 2.5, 6.4
	entry := AreaEntry{LatMin: &minLat, LatMax: &maxLat, LonMin: &minLon, LonMax: &maxLon}
	pks, err := s.nodePubkeysInArea(t.Context(), entry)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if len(pks) != 1 || pks[0] != "in" {
		t.Errorf("want [in], got %v", pks)
	}
}

func TestHandleConfigAreas(t *testing.T) {
	db := setupTestDB(t)
	t.Cleanup(func() { db.Close() })
	cfg := &Config{Areas: map[string]AreaEntry{
		"BEL": {Label: "Belgium", Polygon: [][2]float64{{50.0, 2.5}, {51.5, 2.5}, {51.5, 6.4}, {50.0, 6.4}}},
		"MST": {Label: "Maastricht"},
	}}

	r := mux.NewRouter()
	srv := &Server{db: db, cfg: cfg}
	r.HandleFunc("/api/config/areas", srv.handleConfigAreas).Methods("GET")

	req := httptest.NewRequest(http.MethodGet, "/api/config/areas", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("want 200, got %d", w.Code)
	}
	var result []map[string]string
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("want 2 areas, got %d", len(result))
	}
	keys := map[string]bool{}
	for _, entry := range result {
		keys[entry["key"]] = true
		if entry["label"] == "" {
			t.Errorf("missing label for key %q", entry["key"])
		}
	}
	if !keys["BEL"] || !keys["MST"] {
		t.Errorf("expected BEL and MST, got %v", keys)
	}
}

func TestHandleConfigAreasEmpty(t *testing.T) {
	db := setupTestDB(t)
	t.Cleanup(func() { db.Close() })
	cfg := &Config{}

	r := mux.NewRouter()
	srv := &Server{db: db, cfg: cfg}
	r.HandleFunc("/api/config/areas", srv.handleConfigAreas).Methods("GET")

	req := httptest.NewRequest(http.MethodGet, "/api/config/areas", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	var result []interface{}
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("want empty array, got %v", result)
	}
}

func TestResolveAreaNodes_CalledBeforeRLock(t *testing.T) {
	// Verify resolveAreaNodes doesn't deadlock when called concurrently with writes.
	// This test catches the anti-pattern where resolveAreaNodes (which does a DB
	// query) is called while holding s.mu.RLock().
	db := setupTestDB(t)
	ctx := t.Context()
	t.Cleanup(func() { db.Close() })
	mustExecDB(t, db, `INSERT INTO nodes (public_key, lat, lon, last_seen, first_seen) VALUES ('n1', 50.85, 4.35, now(), now())`)

	cfg := &Config{Areas: map[string]AreaEntry{
		"BEL": {Label: "Belgium", Polygon: [][2]float64{{50.0, 2.5}, {51.5, 2.5}, {51.5, 6.4}, {50.0, 6.4}}},
	}}
	s := newTestStoreWithDB(t, db, cfg)
	ingestAdvert(t, s, "h1", `{"public_key":"n1","name":"N1"}`)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.GetBulkHealth(ctx, 10, "", "BEL")
		}()
	}
	wg.Wait() // must not deadlock
}

func TestResolveAreaNodes_PerKeyTTL(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()
	t.Cleanup(func() { db.Close() })
	mustExecDB(t, db, `INSERT INTO nodes (public_key, lat, lon, last_seen, first_seen) VALUES ('bel-node', 50.85, 4.35, now(), now())`)
	mustExecDB(t, db, `INSERT INTO nodes (public_key, lat, lon, last_seen, first_seen) VALUES ('nl-node', 52.4, 4.9, now(), now())`)

	cfg := &Config{Areas: map[string]AreaEntry{
		"BEL": {Label: "Belgium", Polygon: [][2]float64{{50.0, 2.5}, {51.5, 2.5}, {51.5, 6.4}, {50.0, 6.4}}},
		"NL":  {Label: "Netherlands", Polygon: [][2]float64{{51.5, 3.4}, {53.6, 3.4}, {53.6, 7.2}, {51.5, 7.2}}},
	}}
	s := newTestStoreWithDB(t, db, cfg)

	// Populate both keys into cache.
	r1 := s.resolveAreaNodes(ctx, "BEL")
	if !r1["bel-node"] {
		t.Fatal("bel-node should be in BEL")
	}
	r2 := s.resolveAreaNodes(ctx, "NL")
	if !r2["nl-node"] {
		t.Fatal("nl-node should be in NL")
	}

	// Delete both nodes from DB to prove cache still serves them.
	mustExecDB(t, db, `DELETE FROM nodes`)

	// BEL cache should still be warm (not evicted by NL query).
	r3 := s.resolveAreaNodes(ctx, "BEL")
	if !r3["bel-node"] {
		t.Error("BEL cache was evicted by NL query (global TTL bug)")
	}
	// NL cache should still be warm too.
	r4 := s.resolveAreaNodes(ctx, "NL")
	if !r4["nl-node"] {
		t.Error("NL cache was evicted unexpectedly")
	}
}

func TestGetBulkHealth_AreaBypassesCap(t *testing.T) {
	db := setupTestDB(t)
	ctx := t.Context()
	now := time.Now().UTC()

	for i := 0; i < 510; i++ {
		if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
			PublicKey: fmt.Sprintf("%064x", i+1),
			Name:      fmt.Sprintf("node-%d", i),
			Role:      "repeater",
			Lat:       fptr(50.85),
			Lon:       fptr(4.35),
			RxTime:    now,
			IngestNow: now,
		}); err != nil {
			t.Fatalf("seed node %d: %v", i, err)
		}
	}

	cfg := &Config{Areas: map[string]AreaEntry{
		"BEL": {Label: "Belgium", Polygon: [][2]float64{{50.0, 2.5}, {51.5, 2.5}, {51.5, 6.4}, {50.0, 6.4}}},
	}}
	s := loadedStore(t, db)
	s.config = cfg

	result := s.GetBulkHealth(ctx, 10, "", "BEL")
	if len(result) != 510 {
		t.Errorf("want 510 nodes from area BEL, got %d", len(result))
	}
}
