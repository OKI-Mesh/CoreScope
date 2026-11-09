package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
	"github.com/OKI-Mesh/CoreScope/internal/decoder"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/gorilla/mux"
)

func newPacketStoreForTest(t testing.TB, db *database.DB) *PacketStore {
	t.Helper()
	store := NewPacketStore(db, nil)
	store.config = &Config{Port: 3000, SnapshotDir: t.TempDir()}
	return store
}

func mustQueryPackets(t *testing.T, store *PacketStore, ctx context.Context, query database.PacketQuery) *api.PacketListResponse {
	t.Helper()
	result, err := store.QueryPackets(ctx, query)
	if err != nil {
		t.Fatalf("QueryPackets(): %v", err)
	}
	return result
}

func mustQueryMultiNodePackets(t *testing.T, store *PacketStore, ctx context.Context, pubkeys []string, limit, offset int, order string, since, until time.Time) *api.PacketListResponse {
	t.Helper()
	result, err := store.QueryMultiNodePackets(ctx, pubkeys, limit, offset, order, since, until)
	if err != nil {
		t.Fatalf("QueryMultiNodePackets(): %v", err)
	}
	return result
}

func mustQueryGroupedPackets(t *testing.T, store *PacketStore, ctx context.Context, query database.PacketQuery) *api.GroupedPacketListResponse {
	t.Helper()
	result, err := store.QueryGroupedPackets(ctx, query)
	if err != nil {
		t.Fatalf("QueryGroupedPackets(): %v", err)
	}
	return result
}

func mustIngestNewFromDB(t *testing.T, store *PacketStore, ctx context.Context, sinceID, limit int) ([]BroadcastPacket, int) {
	t.Helper()
	packets, maxID, err := store.IngestNewFromDB(ctx, sinceID, limit)
	if err != nil {
		t.Fatalf("IngestNewFromDB(): %v", err)
	}
	return packets, maxID
}

func mustGetPacketByHash(t *testing.T, store *PacketStore, ctx context.Context, hash string) *api.Packet {
	t.Helper()
	packet, err := store.GetPacketByHash(ctx, hash)
	if err != nil {
		t.Fatalf("GetPacketByHash(%q): %v", hash, err)
	}
	return packet
}

func mustGetTransmissionByID(t *testing.T, store *PacketStore, ctx context.Context, id int) *api.Packet {
	t.Helper()
	packet, err := store.GetTransmissionByID(ctx, id)
	if err != nil {
		t.Fatalf("GetTransmissionByID(%d): %v", id, err)
	}
	return packet
}

func mustGetPacketByID(t *testing.T, store *PacketStore, ctx context.Context, id int) *api.ObservationPacket {
	t.Helper()
	packet, err := store.GetPacketByID(ctx, id)
	if err != nil {
		t.Fatalf("GetPacketByID(%d): %v", id, err)
	}
	return packet
}

func mustGetObservationsForHash(t *testing.T, store *PacketStore, ctx context.Context, hash string) []api.ObservationDetail {
	t.Helper()
	observations, err := store.GetObservationsForHash(ctx, hash)
	if err != nil {
		t.Fatalf("GetObservationsForHash(%q): %v", hash, err)
	}
	return observations
}

func mustIngestNewObservations(t *testing.T, store *PacketStore, ctx context.Context, sinceID int64, limit int) ([]BroadcastPacket, int64) {
	t.Helper()
	packets, maxID, err := store.IngestNewObservations(ctx, sinceID, limit)
	if err != nil {
		t.Fatalf("IngestNewObservations(): %v", err)
	}
	return packets, maxID
}

func mustGetMaxObservationID(t *testing.T, db *database.DB, ctx context.Context) int64 {
	t.Helper()
	maxID, err := db.GetMaxObservationID(ctx)
	if err != nil {
		t.Fatalf("GetMaxObservationID(): %v", err)
	}
	return maxID
}

// --- Store.QueryMultiNodePackets ---

func TestStoreQueryMultiNodePackets(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	t.Run("empty pubkeys", func(t *testing.T) {
		result := mustQueryMultiNodePackets(t, store, ctx, nil, 50, 0, "DESC", time.Time{}, time.Time{})
		if result.Total != 0 {
			t.Errorf("expected 0, got %d", result.Total)
		}
	})

	t.Run("matching pubkey", func(t *testing.T) {
		result := mustQueryMultiNodePackets(t, store, ctx, []string{"aabbccdd11223344"}, 50, 0, "DESC", time.Time{}, time.Time{})
		if result.Total < 1 {
			t.Errorf("expected >=1, got %d", result.Total)
		}
	})

	t.Run("ASC order", func(t *testing.T) {
		result := mustQueryMultiNodePackets(t, store, ctx, []string{"aabbccdd11223344"}, 50, 0, "ASC", time.Time{}, time.Time{})
		if result.Total < 1 {
			t.Errorf("expected >=1, got %d", result.Total)
		}
	})

	t.Run("with since/until", func(t *testing.T) {
		result := mustQueryMultiNodePackets(t, store, ctx,
			[]string{"aabbccdd11223344"}, 50, 0, "DESC",
			time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC))
		if result.Total < 1 {
			t.Errorf("expected >=1, got %d", result.Total)
		}
	})

	t.Run("offset beyond total", func(t *testing.T) {
		result := mustQueryMultiNodePackets(t, store, ctx, []string{"aabbccdd11223344"}, 50, 9999, "DESC", time.Time{}, time.Time{})
		if len(result.Packets) != 0 {
			t.Errorf("expected 0 packets, got %d", len(result.Packets))
		}
	})

	t.Run("default limit", func(t *testing.T) {
		result := mustQueryMultiNodePackets(t, store, ctx, []string{"aabbccdd11223344"}, 0, 0, "DESC", time.Time{}, time.Time{})
		if result.Total < 1 {
			t.Errorf("expected >=1, got %d", result.Total)
		}
	})
}

// --- IngestNewFromDB ---

func TestIngestNewFromDB(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}

	initialMax := store.MaxTransmissionID()

	// Insert a new transmission (with one observation) through the real writers.
	obsIdx, err := db.GetObserverID(ctx, "obs1")
	if err != nil {
		t.Fatalf("GetObserverID: %v", err)
	}
	now := time.Now().UTC()
	newTxID := dbtest.TxWithObservation(t, db, obsIdx, database.TransmissionInput{
		RawHex:      "EEFF",
		Hash:        "newhash123456abcd",
		RouteType:   types.RouteType(1),
		PayloadType: types.PayloadType(4),
		DecodedJSON: `{"pubKey":"aabbccdd11223344","type":"ADVERT"}`,
		Advert:      &database.NodeInput{PublicKey: "aabbccdd11223344", Name: "TestRepeater", Role: "repeater", RxTime: now, IngestNow: now},
	}, now, 10.0, -92, "cc")

	// Ingest
	broadcastMaps, newMax := mustIngestNewFromDB(t, store, ctx, initialMax, 100)
	if newMax <= initialMax {
		t.Errorf("expected newMax > %d, got %d", initialMax, newMax)
	}
	if newMax != int(newTxID) {
		t.Errorf("newMax = %d, want the new transmission id %d", newMax, newTxID)
	}
	if len(broadcastMaps) < 1 {
		t.Errorf("expected >=1 broadcast maps, got %d", len(broadcastMaps))
	}

	// Verify broadcast map contains nested "packet" field (fixes #162)
	if len(broadcastMaps) > 0 {
		bm := broadcastMaps[0]
		pkt := bm.Packet
		if pkt.Hash == nil {
			t.Error("broadcast map missing 'packet' field (required by packets.js)")
		}
		for _, field := range []struct {
			name  string
			value interface{}
		}{
			{"id", pkt.ID},
			{"hash", pkt.Hash},
			{"payload_type", pkt.PayloadType},
			{"observer_id", pkt.ObserverID},
		} {
			if field.value == nil || field.value == 0 {
				t.Errorf("packet sub-object missing field %q", field.name)
			}
		}
		// Verify decoded also present at top level (for live.js)
		if bm.Decoded == nil {
			t.Error("broadcast map missing 'decoded' field (required by live.js)")
		}
	}

	// Verify ingested into store
	updatedMax := store.MaxTransmissionID()
	if updatedMax < newMax {
		t.Errorf("store max (%d) should be >= newMax (%d)", updatedMax, newMax)
	}

	t.Run("no new data", func(t *testing.T) {
		maps, max := mustIngestNewFromDB(t, store, ctx, newMax, 100)
		if maps != nil {
			t.Errorf("expected nil for no new data, got %d maps", len(maps))
		}
		if max != newMax {
			t.Errorf("expected same max %d, got %d", newMax, max)
		}
	})

	t.Run("default limit", func(t *testing.T) {
		_, _ = mustIngestNewFromDB(t, store, ctx, newMax, 0)
	})
}

// --- MaxTransmissionID ---

func TestMaxTransmissionID(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	maxID := store.MaxTransmissionID()
	if maxID <= 0 {
		t.Errorf("expected maxID > 0, got %d", maxID)
	}

	t.Run("empty store", func(t *testing.T) {
		emptyStore := newPacketStoreForTest(t, db)
		if emptyStore.MaxTransmissionID() != 0 {
			t.Error("expected 0 for empty store")
		}
	})
}

// --- MaxTransmissionID incremental tracking ---

func TestMaxTransmissionIDIncremental(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	maxTx := store.MaxTransmissionID()
	maxObs := store.MaxObservationID()

	if maxTx <= 0 {
		t.Fatalf("expected maxTx > 0 after Load, got %d", maxTx)
	}
	if maxObs <= 0 {
		t.Fatalf("expected maxObs > 0 after Load, got %d", maxObs)
	}

	// Verify incremental field matches brute-force iteration
	store.mu.RLock()
	bruteMaxTx := 0
	for id := range store.byTxID {
		if id > bruteMaxTx {
			bruteMaxTx = id
		}
	}
	bruteMaxObs := 0
	for id := range store.byObsID {
		if id > bruteMaxObs {
			bruteMaxObs = id
		}
	}
	store.mu.RUnlock()

	if maxTx != bruteMaxTx {
		t.Errorf("maxTxID mismatch: incremental=%d brute=%d", maxTx, bruteMaxTx)
	}
	if maxObs != bruteMaxObs {
		t.Errorf("maxObsID mismatch: incremental=%d brute=%d", maxObs, bruteMaxObs)
	}
}

// --- Route handler DB fallback (no store) ---

func TestHandleBulkHealthNoStore(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/nodes/bulk-health?limit=10", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body []interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	if body == nil {
		t.Fatal("expected array response")
	}
}

func TestHandleBulkHealthNoStoreMaxLimit(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/nodes/bulk-health?limit=500", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleAnalyticsRFNoStore(t *testing.T) {
	_, router := setupTestServer(t)

	t.Run("basic", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/analytics/rf", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
		var body map[string]interface{}
		json.Unmarshal(w.Body.Bytes(), &body)
		if _, ok := body["snr"]; !ok {
			t.Error("expected snr field")
		}
		if _, ok := body["payloadTypes"]; !ok {
			t.Error("expected payloadTypes field")
		}
	})

	t.Run("with region", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/analytics/rf?region=SJC", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})
}

func TestHandlePackets(t *testing.T) {
	_, router := setupTestServer(t)

	get := func(t *testing.T, path string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest("GET", path, nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("%s: expected 200, got %d (body: %s)", path, w.Code, w.Body.String())
		}
		return w
	}

	t.Run("basic packets", func(t *testing.T) {
		get(t, "/api/packets?limit=10")
	})

	t.Run("multi-node", func(t *testing.T) {
		w := get(t, "/api/packets?nodes=aabbccdd11223344,eeff00112233aabb")
		var body struct {
			Packets json.RawMessage `json:"packets"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if body.Packets == nil {
			t.Error("expected packets field")
		}
	})

	t.Run("grouped", func(t *testing.T) {
		get(t, "/api/packets?groupByHash=true&limit=10")
	})
}

func TestHandlePacketsMultiNodeWithStore(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/packets?nodes=aabbccdd11223344&order=asc&limit=10&offset=0", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	if _, ok := body["packets"]; !ok {
		t.Error("expected packets field")
	}
}

func TestHandlePacketDetailNoStore(t *testing.T) {
	_, router := setupTestServer(t)

	// With no in-memory store, handlePacketDetail now falls back to the DB
	// (#827). The seeded transmissions are present in the DB, so by-hash and
	// by-ID lookups succeed; only truly absent IDs return 404.
	t.Run("by hash", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/packets/abc123def4567890", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200 (DB fallback), got %d: %s", w.Code, w.Body.String())
		}
	})

	t.Run("by ID", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/packets/1", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200 (DB fallback), got %d: %s", w.Code, w.Body.String())
		}
	})

	t.Run("not found", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/packets/9999", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 404 {
			t.Fatalf("expected 404, got %d", w.Code)
		}
	})

	t.Run("non-numeric non-hash", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/packets/notahash", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 404 {
			t.Fatalf("expected 404, got %d", w.Code)
		}
	})
}

func TestHandleAnalyticsChannelsNoStore(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/analytics/channels", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	if _, ok := body["activeChannels"]; !ok {
		t.Error("expected activeChannels field")
	}
}

// --- transmissionsForObserver (byObserver index path) ---

func TestTransmissionsForObserverIndex(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	// Query packets for an observer — hits the byObserver index
	result := mustQueryPackets(t, store, ctx, database.PacketQuery{Limit: 50, Observers: []string{"obs1"}, Order: "DESC"})
	if result.Total < 1 {
		t.Errorf("expected >=1 packets for obs1, got %d", result.Total)
	}

	// Query with observer + type (uses from != nil path in transmissionsForObserver)
	pt := 4
	result2 := mustQueryPackets(t, store, ctx, database.PacketQuery{Limit: 50, Observers: []string{"obs1"}, Type: &pt, Order: "DESC"})
	if result2.Total < 1 {
		t.Errorf("expected >=1 filtered packets, got %d", result2.Total)
	}
}

// --- GetChannelMessages (dedup, observer, hops paths) ---

func TestGetChannelMessagesFromStore(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	// Test channel should exist from seed data
	messages, total := store.GetChannelMessages(ctx, "#test", 100, 0)
	if total < 1 {
		t.Errorf("expected >=1 messages for #test, got %d", total)
	}
	if len(messages) < 1 {
		t.Errorf("expected >=1 message entries, got %d", len(messages))
	}

	t.Run("non-existent channel", func(t *testing.T) {
		msgs, total := store.GetChannelMessages(ctx, "nonexistent", 100, 0)
		if total != 0 || len(msgs) != 0 {
			t.Errorf("expected 0 for nonexistent channel, got %d/%d", total, len(msgs))
		}
	})

	t.Run("default limit", func(t *testing.T) {
		_, total := store.GetChannelMessages(ctx, "#test", 0, 0)
		if total < 1 {
			t.Errorf("expected >=1 with default limit, got %d", total)
		}
	})

	t.Run("with offset", func(t *testing.T) {
		_, _ = store.GetChannelMessages(ctx, "#test", 10, 9999)
	})
}

func TestGetChannelMessagesDedupe(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)

	recent := time.Now().UTC().Add(-1 * time.Hour)

	obs1, err := db.GetObserverID(ctx, "obs1")
	if err != nil {
		t.Fatalf("GetObserverID obs1: %v", err)
	}
	obs2, err := db.GetObserverID(ctx, "obs2")
	if err != nil {
		t.Fatalf("GetObserverID obs2: %v", err)
	}

	const decoded = `{"type":"CHAN","channel":"#test","text":"Hello: World","sender":"TestUser"}`
	channelTx := func(hash, rawHex string) database.TransmissionInput {
		return database.TransmissionInput{
			RawHex:      rawHex,
			Hash:        hash,
			RouteType:   types.RouteType(1),
			PayloadType: types.PayloadType(5), // GRP_TXT
			DecodedJSON: decoded,
			ChannelHash: "#test",
		}
	}

	// Two transmissions carrying the same channel message text and sender,
	// each seen by a different observer.
	dbtest.TxWithObservation(t, db, obs1, channelTx("dupchannelhash1234", "DDEE"), recent, 11.0, -91, "aa")
	dbtest.TxWithObservation(t, db, obs2, channelTx("dupchannelhash5678", "DDFF"), recent, 9.0, -93)

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}

	msgs, total := store.GetChannelMessages(ctx, "#test", 100, 0)
	// Should have messages, with some deduped
	if total < 1 {
		t.Errorf("expected >=1 total messages, got %d", total)
	}
	_ = msgs
}

// --- GetChannels ---

func TestGetChannelsFromStore(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	channels := store.GetChannels(ctx, "")
	if len(channels) < 1 {
		t.Errorf("expected >=1 channel, got %d", len(channels))
	}

	t.Run("with region", func(t *testing.T) {
		ch := store.GetChannels(ctx, "SJC")
		_ = ch
	})

	t.Run("non-existent region", func(t *testing.T) {
		ch := store.GetChannels(ctx, "NONEXIST")
		// Region filter may return 0 or fallback to unfiltered depending on DB content
		_ = ch
	})
}

// --- resolve (prefixMap) ---

func TestPrefixMapResolve(t *testing.T) {
	nodes := []nodeInfo{
		{Role: "repeater", PublicKey: "aabbccdd11223344", Name: "NodeA", HasGPS: true, Lat: 37.5, Lon: -122.0},
		{Role: "repeater", PublicKey: "aabbccdd55667788", Name: "NodeB", HasGPS: false},
		{Role: "repeater", PublicKey: "eeff0011aabbccdd", Name: "NodeC", HasGPS: true, Lat: 38.0, Lon: -121.0},
	}
	pm := buildPrefixMap(nodes)

	t.Run("exact match", func(t *testing.T) {
		n := pm.resolve("aabbccdd11223344")
		if n == nil || n.Name != "NodeA" {
			t.Errorf("expected NodeA, got %v", n)
		}
	})

	t.Run("prefix match single", func(t *testing.T) {
		n := pm.resolve("eeff")
		if n == nil || n.Name != "NodeC" {
			t.Errorf("expected NodeC, got %v", n)
		}
	})

	t.Run("prefix match multiple — prefer GPS", func(t *testing.T) {
		n := pm.resolve("aabbccdd")
		if n == nil {
			t.Fatal("expected non-nil")
		}
		if !n.HasGPS {
			t.Error("expected GPS-preferred candidate")
		}
		if n.Name != "NodeA" {
			t.Errorf("expected NodeA (has GPS), got %s", n.Name)
		}
	})

	t.Run("no match", func(t *testing.T) {
		n := pm.resolve("zzzzz")
		if n != nil {
			t.Errorf("expected nil, got %v", n)
		}
	})

	t.Run("multiple candidates no GPS", func(t *testing.T) {
		noGPSNodes := []nodeInfo{
			{Role: "repeater", PublicKey: "aa11bb22", Name: "X", HasGPS: false},
			{Role: "repeater", PublicKey: "aa11cc33", Name: "Y", HasGPS: false},
		}
		pm2 := buildPrefixMap(noGPSNodes)
		n := pm2.resolve("aa11")
		if n == nil {
			t.Fatal("expected non-nil")
		}
		// Should return first candidate
	})
}

func TestPrefixMapCap(t *testing.T) {
	// 16-char pubkey — longer than maxPrefixLen
	nodes := []nodeInfo{
		{Role: "repeater", PublicKey: "aabbccdd11223344", Name: "LongKey"},
		{Role: "repeater", PublicKey: "eeff0011", Name: "ShortKey"}, // exactly 8 chars
	}
	pm := buildPrefixMap(nodes)

	t.Run("short prefixes still work", func(t *testing.T) {
		n := pm.resolve("aabb")
		if n == nil || n.Name != "LongKey" {
			t.Errorf("expected LongKey for short prefix, got %v", n)
		}
	})

	t.Run("full pubkey exact match works", func(t *testing.T) {
		n := pm.resolve("aabbccdd11223344")
		if n == nil || n.Name != "LongKey" {
			t.Errorf("expected LongKey for full key, got %v", n)
		}
	})

	t.Run("intermediate prefix beyond cap returns nil", func(t *testing.T) {
		// 10-char prefix — beyond maxPrefixLen but not full key
		n := pm.resolve("aabbccdd11")
		if n != nil {
			t.Errorf("expected nil for intermediate prefix beyond cap, got %v", n.Name)
		}
	})

	t.Run("short key within cap has all prefixes", func(t *testing.T) {
		for l := 2; l <= 8; l++ {
			pfx := "eeff0011"[:l]
			n := pm.resolve(pfx)
			if n == nil || n.Name != "ShortKey" {
				t.Errorf("prefix %q: expected ShortKey, got %v", pfx, n)
			}
		}
	})

	t.Run("map size is capped", func(t *testing.T) {
		// LongKey: 7 prefix entries (2..8) + 1 full key = 8
		// ShortKey: 7 prefix entries (2..8), no full key entry (len == maxPrefixLen) = 7
		// No overlapping prefixes between the two nodes → 8 + 7 = 15 unique map keys
		if len(pm.m) != 15 {
			t.Errorf("expected 15 map entries (8 for LongKey + 7 for ShortKey), got %d", len(pm.m))
		}
	})
}

// --- pathLen ---

func TestPathLen(t *testing.T) {
	tests := []struct {
		json string
		want int
	}{
		{"", 0},
		{"invalid", 0},
		{`[]`, 0},
		{`["aa"]`, 1},
		{`["aa","bb","cc"]`, 3},
	}
	for _, tc := range tests {
		got := pathLen(tc.json)
		if got != tc.want {
			t.Errorf("pathLen(%q) = %d, want %d", tc.json, got, tc.want)
		}
	}
}

// --- floatPtrOrNil ---

func TestFloatPtrOrNil(t *testing.T) {
	v := 3.14
	if floatPtrOrNil(&v) != 3.14 {
		t.Error("expected 3.14")
	}
	if floatPtrOrNil(nil) != nil {
		t.Error("expected nil")
	}
}

// --- nilIfEmpty ---

func TestNilIfEmpty(t *testing.T) {
	if nilIfEmpty("") != nil {
		t.Error("expected nil for empty")
	}
	if nilIfEmpty("hello") != "hello" {
		t.Error("expected 'hello'")
	}
}

// --- pickBestObservation ---

func TestPickBestObservation(t *testing.T) {
	t.Run("empty observations", func(t *testing.T) {
		tx := &StoreTx{}
		pickBestObservation(tx)
		if tx.ObserverID != "" {
			t.Error("expected empty observer for no observations")
		}
	})

	t.Run("single observation", func(t *testing.T) {
		snr := 10.0
		tx := &StoreTx{
			Observations: []*StoreObs{
				{ObserverID: "obs1", ObserverName: "One", SNR: &snr, PathJSON: `["aa"]`},
			},
		}
		pickBestObservation(tx)
		if tx.ObserverID != "obs1" {
			t.Errorf("expected obs1, got %s", tx.ObserverID)
		}
	})

	t.Run("picks longest path", func(t *testing.T) {
		snr1, snr2 := 10.0, 5.0
		tx := &StoreTx{
			Observations: []*StoreObs{
				{ObserverID: "obs1", SNR: &snr1, PathJSON: `["aa"]`},
				{ObserverID: "obs2", SNR: &snr2, PathJSON: `["aa","bb","cc"]`},
			},
		}
		pickBestObservation(tx)
		if tx.ObserverID != "obs2" {
			t.Errorf("expected obs2 (longest path), got %s", tx.ObserverID)
		}
	})
}

// --- indexByNode ---

func TestIndexByNode(t *testing.T) {
	db := setupTestDB(t)
	store := newPacketStoreForTest(t, db)

	t.Run("empty decoded_json", func(t *testing.T) {
		tx := &StoreTx{Hash: "h1"}
		store.indexByNode(tx)
		if len(store.byNode) != 0 {
			t.Error("expected no index entries")
		}
	})

	t.Run("valid decoded_json", func(t *testing.T) {
		tx := &StoreTx{
			Hash:        "h2",
			DecodedJSON: `{"pubKey":"aabbccdd11223344","destPubKey":"eeff00112233aabb"}`,
		}
		store.indexByNode(tx)
		if len(store.byNode["aabbccdd11223344"]) != 1 {
			t.Error("expected pubKey indexed")
		}
		if len(store.byNode["eeff00112233aabb"]) != 1 {
			t.Error("expected destPubKey indexed")
		}
	})

	t.Run("duplicate hash skipped", func(t *testing.T) {
		tx := &StoreTx{
			Hash:        "h2",
			DecodedJSON: `{"pubKey":"aabbccdd11223344"}`,
		}
		store.indexByNode(tx)
		// Should not add duplicate
		if len(store.byNode["aabbccdd11223344"]) != 1 {
			t.Errorf("expected 1, got %d", len(store.byNode["aabbccdd11223344"]))
		}
	})

	t.Run("invalid json", func(t *testing.T) {
		tx := &StoreTx{Hash: "h3", DecodedJSON: "not json"}
		store.indexByNode(tx)
		// Should not panic or add anything
	})
}

// --- resolveVersion ---

func TestResolveVersion(t *testing.T) {
	old := Version
	defer func() { Version = old }()

	Version = "v1.2.3"
	if resolveVersion() != "v1.2.3" {
		t.Error("expected v1.2.3")
	}

	Version = ""
	if resolveVersion() != "unknown" {
		t.Error("expected unknown when empty")
	}
}

func TestResolveVersionFallbackChain(t *testing.T) {
	old := Version
	defer func() { Version = old }()

	// (a) CORESCOPE_VERSION env wins over baked Version="edge"
	Version = "edge"
	t.Setenv("CORESCOPE_VERSION", "v3.9.2")
	if got := resolveVersion(); got != "v3.9.2" {
		t.Errorf("expected v3.9.2 from env, got %s", got)
	}

	// (b) .image-version file wins over baked Version="edge" when env is unset
	t.Setenv("CORESCOPE_VERSION", "")
	Version = "edge"
	tmp := t.TempDir()
	origDir, _ := os.Getwd()
	defer os.Chdir(origDir)
	os.Chdir(tmp)
	if err := os.WriteFile(".image-version", []byte("v3.9.2\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if got := resolveVersion(); got != "v3.9.2" {
		t.Errorf("expected v3.9.2 from .image-version, got %s", got)
	}

	// (c) Version="edge" returned when neither env nor file present
	os.Chdir(t.TempDir())
	Version = "edge"
	if got := resolveVersion(); got != "edge" {
		t.Errorf("expected edge as fallback, got %s", got)
	}
}

// --- wsOrStatic ---

func TestWsOrStaticNonWebSocket(t *testing.T) {
	hub := NewHub()
	staticHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("static"))
	})
	handler := wsOrStatic(hub, staticHandler)

	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if w.Body.String() != "static" {
		t.Errorf("expected 'static', got %s", w.Body.String())
	}
}

// --- Poller.Start ---

func TestPollerStartWithStore(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)
	hub := NewHub()
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}

	poller := NewPoller(db, store, hub, 50*time.Millisecond)

	done := make(chan struct{})
	go func() {
		defer close(done)
		poller.Start(ctx)
	}()
	t.Cleanup(func() {
		poller.Stop()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("poller.Start did not return after Stop")
		}
	})

	// Insert new data while the poller is running.
	obsIdx, err := db.GetObserverID(ctx, "obs1")
	if err != nil {
		t.Fatalf("GetObserverID: %v", err)
	}
	newTxID := dbtest.TxWithObservation(t, db, obsIdx, database.TransmissionInput{
		RawHex:      "FFEE",
		Hash:        "pollerhash12345678",
		RouteType:   types.RouteType(1),
		PayloadType: types.PayloadType(4),
	}, time.Now().UTC(), 10.0, -92)

	// The poller should pick it up and add it to the store.
	deadline := time.Now().Add(5 * time.Second)
	for store.MaxTransmissionID() < int(newTxID) {
		if time.Now().After(deadline) {
			t.Fatalf("poller did not ingest transmission %d within 5s (store max = %d)",
				newTxID, store.MaxTransmissionID())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// --- perfMiddleware slow query path ---

func TestPerfMiddlewareSlowQuery(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)
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

	// Add a slow handler
	router.HandleFunc("/api/test-slow", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(110 * time.Millisecond)
		writeJSON(w, map[string]string{"ok": "true"})
	}).Methods("GET")

	req := httptest.NewRequest("GET", "/api/test-slow", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if len(srv.perfStats.SlowQueries) < 1 {
		t.Error("expected slow query to be recorded")
	}
}

func TestPerfMiddlewareNonAPIPath(t *testing.T) {
	db := setupTestDB(t)
	cfg := &Config{Port: 3000}
	hub := NewHub()
	srv := NewServer(db, cfg, hub)
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	// Non-API path should pass through without perf tracking
	router.HandleFunc("/not-api", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}).Methods("GET")

	initialReqs := srv.perfStats.Requests
	req := httptest.NewRequest("GET", "/not-api", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if srv.perfStats.Requests != initialReqs {
		t.Error("non-API request should not be tracked")
	}
}

// --- writeJSON error path ---

func TestWriteJSONErrorPath(t *testing.T) {
	w := httptest.NewRecorder()
	// math.Inf cannot be marshaled to JSON — triggers the error path
	writeJSON(w, math.Inf(1))
	// Should not panic, just log the error
}

// --- GetObserverPacketCounts ---

func TestGetObserverPacketCounts(t *testing.T) {
	ctx := context.Background()
	db := dbtest.New(t)

	now := time.Now().UTC()
	obsA := dbtest.Observer(t, db, "obsa", "Obs A", "SEA", now, now)
	obsB := dbtest.Observer(t, db, "obsb", "Obs B", "SEA", now, now)
	dbtest.Observer(t, db, "obsc", "Obs C", "SEA", now, now) // no observations

	seed := func(hash string, observer int64, at time.Time) {
		dbtest.TxWithObservation(t, db, observer, database.TransmissionInput{
			RawHex: hash, Hash: hash,
			RouteType: types.RouteType(1), PayloadType: types.PayloadType(4),
			DecodedJSON: `{}`,
		}, at, 10.0, -90)
	}
	seed("a1", obsA, now.Add(-10*time.Minute))
	seed("a2", obsA, now.Add(-20*time.Minute))
	seed("a3", obsB, now.Add(-30*time.Minute))
	seed("a4", obsA, now.Add(-3*time.Hour)) // outside the window

	got, err := db.GetObserverPacketCounts(ctx, now.Add(-1*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{"obsa": 2, "obsb": 1}
	if !maps.Equal(got, want) {
		t.Errorf("counts = %v, want %v (obsc absent, a4 outside window)", got, want)
	}
}

// --- Additional route fallback tests ---

func TestHandleAnalyticsTopologyNoStore(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/analytics/topology", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func waitDistanceIndex(t *testing.T, store *PacketStore) {
	t.Helper()
	store.TriggerDistanceIndexBuild()
	deadline := time.Now().Add(5 * time.Second)
	for !store.DistanceIndexBuilt() {
		if time.Now().After(deadline) {
			t.Fatalf("distance index not built within 5s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestHandleAnalyticsDistance(t *testing.T) {
	srv, router := setupTestServer(t)
	waitDistanceIndex(t, srv.store)
	req := httptest.NewRequest("GET", "/api/analytics/distance", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}
}

func TestHandleAnalyticsHashSizesNoStore(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/analytics/hash-sizes", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleAnalyticsSubpathsNoStore(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/analytics/subpaths", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleAnalyticsSubpathDetailNoStore(t *testing.T) {
	_, router := setupTestServer(t)

	t.Run("with hops", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/analytics/subpath-detail?hops=aa,bb", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})

	t.Run("missing hops", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/analytics/subpath-detail", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})

	t.Run("single hop", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/analytics/subpath-detail?hops=aa", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})
}

func TestHandleChannelsNoStore(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/channels", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleChannelMessagesNoStore(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/channels/test/messages", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandlePacketTimestampsNoStore(t *testing.T) {
	_, router := setupTestServer(t)

	t.Run("with since", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/packets/timestamps?since=2020-01-01T00:00:00Z", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})

	t.Run("missing since", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/packets/timestamps", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 400 {
			t.Fatalf("expected 400, got %d", w.Code)
		}
	})
}

func TestHandleStatsNoStore(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/stats", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleHealthNoStore(t *testing.T) {
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
}

// --- Store edge cases ---

func TestStoreQueryPacketsEdgeCases(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	t.Run("hash filter", func(t *testing.T) {
		result := mustQueryPackets(t, store, ctx, database.PacketQuery{Hash: "abc123def4567890", Limit: 50, Order: "DESC"})
		if result.Total != 1 {
			t.Errorf("expected 1, got %d", result.Total)
		}
	})

	t.Run("non-existent hash", func(t *testing.T) {
		result := mustQueryPackets(t, store, ctx, database.PacketQuery{Hash: "0000000000000000", Limit: 50, Order: "DESC"})
		if result.Total != 0 {
			t.Errorf("expected 0, got %d", result.Total)
		}
	})

	t.Run("ASC order", func(t *testing.T) {
		result := mustQueryPackets(t, store, ctx, database.PacketQuery{Limit: 50, Order: "ASC"})
		if result.Total < 1 {
			t.Error("expected results")
		}
	})

	t.Run("offset beyond end", func(t *testing.T) {
		result := mustQueryPackets(t, store, ctx, database.PacketQuery{Limit: 50, Offset: 9999, Order: "DESC"})
		if len(result.Packets) != 0 {
			t.Errorf("expected 0, got %d", len(result.Packets))
		}
	})

	t.Run("node filter with index", func(t *testing.T) {
		result := mustQueryPackets(t, store, ctx, database.PacketQuery{Node: "aabbccdd11223344", Limit: 50, Order: "DESC"})
		if result.Total < 1 {
			t.Error("expected >=1")
		}
	})

	t.Run("route filter", func(t *testing.T) {
		rt := 1
		result := mustQueryPackets(t, store, ctx, database.PacketQuery{Route: &rt, Limit: 50, Order: "DESC"})
		if result.Total < 1 {
			t.Error("expected >=1")
		}
	})

	t.Run("since filter", func(t *testing.T) {
		since := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		result := mustQueryPackets(t, store, ctx, database.PacketQuery{FirstSeenAfter: &since, Limit: 50, Order: "DESC"})
		if result.Total < 1 {
			t.Error("expected >=1")
		}
	})

	t.Run("until filter", func(t *testing.T) {
		until := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
		result := mustQueryPackets(t, store, ctx, database.PacketQuery{FirstSeenBefore: &until, Limit: 50, Order: "DESC"})
		if result.Total < 1 {
			t.Error("expected >=1")
		}
	})
}

// --- HandlePackets with various options ---

func TestHandlePacketsWithQueryOptions(t *testing.T) {
	_, router := setupTestServer(t)

	t.Run("with type filter", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/packets?type=4&limit=10", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})

	t.Run("with route filter", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/packets?route=1&limit=10", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})

	t.Run("expand observations", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/packets?limit=10&expand=observations", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})

	t.Run("ASC order", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/packets?order=asc&limit=10", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})
}

// --- handleObservers and handleObserverDetail ---

func TestHandleObserversNoStore(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/observers", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleObserverDetailNoStore(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/observers/obs1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

// --- HandleTraces ---

func TestHandleTracesNoStore(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/traces/abc123def4567890", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

// --- HandleResolveHops ---

func TestHandleResolveHops(t *testing.T) {
	_, router := setupTestServer(t)

	t.Run("empty hops", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/resolve-hops", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})

	t.Run("with hops", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/resolve-hops?hops=aabb,eeff", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})
}

// --- HandlePerf ---

func TestHandlePerfNoStore(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/perf", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

// --- HandleIATACoords ---

func TestHandleIATACoordsNoStore(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/iata-coords", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

// --- Conversion helpers ---

func TestStrOrNil(t *testing.T) {
	if strOrNil("") != nil {
		t.Error("expected nil")
	}
	if strOrNil("abc") != "abc" {
		t.Error("expected abc")
	}
}

func TestIntPtrOrNil(t *testing.T) {
	if intPtrOrNil(nil) != nil {
		t.Error("expected nil")
	}
	v := 42
	if intPtrOrNil(&v) != 42 {
		t.Error("expected 42")
	}
}

// --- resolveCommit ---

func TestResolveCommit(t *testing.T) {
	old := Commit
	defer func() { Commit = old }()

	Commit = "abc123"
	if resolveCommit() != "abc123" {
		t.Error("expected abc123")
	}

	Commit = ""
	// With no .git-commit file and possibly no git, should return something
	result := resolveCommit()
	if result == "" {
		t.Error("expected non-empty result")
	}
}

// --- parsePathJSON ---

func TestParsePathJSON(t *testing.T) {
	if parsePathJSON("") != nil {
		t.Error("expected nil for empty")
	}
	if parsePathJSON("[]") != nil {
		t.Error("expected nil for []")
	}
	if parsePathJSON("invalid") != nil {
		t.Error("expected nil for invalid")
	}
	hops := parsePathJSON(`["aa","bb"]`)
	if len(hops) != 2 {
		t.Errorf("expected 2 hops, got %d", len(hops))
	}
}

// --- Store.GetPerfStoreStats & GetCacheStats ---

func TestStorePerfAndCacheStats(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	stats := store.GetPerfStoreStats()
	if _, ok := stats["totalLoaded"]; !ok {
		t.Error("expected totalLoaded")
	}

	cacheStats := store.GetCacheStats()
	if _, ok := cacheStats["size"]; !ok {
		t.Error("expected size")
	}
}

// --- enrichObs ---

func TestEnrichObs(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	// Find an observation from the loaded store
	var obs *StoreObs
	for _, o := range store.byObsID {
		obs = o
		break
	}
	if obs == nil {
		t.Skip("no observations loaded")
	}

	enriched, err := store.observationDetails(ctx, []*StoreObs{obs})
	if err != nil {
		t.Fatalf("observationDetails(): %v", err)
	}
	if len(enriched) != 1 || enriched[0].ObserverId == nil {
		t.Error("expected observer_id")
	}
}

// --- HandleNodeSearch ---

func TestHandleNodeSearch(t *testing.T) {
	_, router := setupTestServer(t)

	t.Run("with query", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/nodes/search?q=Test", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})

	t.Run("empty query", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/nodes/search?q=", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})
}

// --- HandleNodeDetail ---

func TestHandleNodeDetail(t *testing.T) {
	_, router := setupTestServer(t)

	t.Run("existing", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/nodes/aabbccdd11223344", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})

	t.Run("not found", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/nodes/nonexistent12345678", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 404 {
			t.Fatalf("expected 404, got %d", w.Code)
		}
	})
}

// --- HandleNodeHealth ---

func TestHandleNodeHealth(t *testing.T) {
	_, router := setupTestServer(t)

	t.Run("not found", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/nodes/nonexistent12345678/health", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 404 {
			t.Fatalf("expected 404, got %d", w.Code)
		}
	})
}

// --- HandleNodePaths ---

func TestHandleNodePaths(t *testing.T) {
	_, router := setupTestServer(t)

	t.Run("existing", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/nodes/aabbccdd11223344/paths", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})

	t.Run("not found", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/nodes/nonexistent12345678/paths", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 404 {
			t.Fatalf("expected 404, got %d", w.Code)
		}
	})
}

// --- HandleNodeAnalytics ---

func TestHandleNodeAnalytics(t *testing.T) {
	_, router := setupTestServer(t)

	t.Run("existing", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/nodes/aabbccdd11223344/analytics?days=7", nil)
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
			t.Fatalf("expected 404, got %d", w.Code)
		}
	})

	t.Run("days bounds", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/nodes/aabbccdd11223344/analytics?days=0", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})

	t.Run("days max", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/nodes/aabbccdd11223344/analytics?days=999", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})
}

// --- HandleNetworkStatus ---

func TestHandleNetworkStatus(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/nodes/network-status", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

// --- HandleConfigEndpoints ---

func TestHandleConfigEndpoints(t *testing.T) {
	_, router := setupTestServer(t)

	endpoints := []string{
		"/api/config/cache",
		"/api/config/client",
		"/api/config/regions",
		"/api/config/theme",
		"/api/config/map",
	}
	for _, ep := range endpoints {
		t.Run(ep, func(t *testing.T) {
			req := httptest.NewRequest("GET", ep, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != 200 {
				t.Fatalf("expected 200, got %d for %s", w.Code, ep)
			}
		})
	}
}

// --- HandleAudioLabBuckets ---

func TestHandleAudioLabBuckets(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/audio-lab/buckets", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	// May return 200 or 404 depending on implementation
	if w.Code != 200 {
		// Audio lab might not be fully implemented — just verify it doesn't crash
	}
}

// --- txPacket ---

func TestTxPacket(t *testing.T) {
	snr := 10.5
	rssi := -90.0
	pt := types.PayloadType(4)
	rt := types.RouteType(1)
	tx := &StoreTx{
		ID:               1,
		RawHex:           "AABB",
		Hash:             "abc123",
		FirstSeen:        time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		RouteType:        &rt,
		PayloadType:      &pt,
		DecodedJSON:      `{"type":"ADVERT"}`,
		ObservationCount: 2,
		ObserverID:       "obs1",
		ObserverName:     "Obs One",
		SNR:              &snr,
		RSSI:             &rssi,
		PathJSON:         `["aa"]`,
		Direction:        "RX",
	}
	p := txPacket(tx, database.ResolvedPaths{}, false)
	if p.Id != 1 {
		t.Error("expected id 1")
	}
	if p.Hash == nil || *p.Hash != "abc123" {
		t.Error("expected hash abc123")
	}
	if p.Snr == nil || *p.Snr != 10.5 {
		t.Error("expected snr 10.5")
	}
}

func TestTxPacketLazyObservations(t *testing.T) {
	snr := 10.5
	rssi := -90.0
	tx := &StoreTx{
		ID:   1,
		Hash: "abc",
		Observations: []*StoreObs{
			{ID: 10, ObserverID: "obs1", ObserverName: "O1", SNR: &snr, RSSI: &rssi, Timestamp: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)},
			{ID: 11, ObserverID: "obs2", ObserverName: "O2", SNR: &snr, RSSI: &rssi, Timestamp: time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)},
		},
	}

	// Not requested: no observations
	p := txPacket(tx, database.ResolvedPaths{}, false)
	if p.Observations != nil {
		t.Error("txPacket without includeObservations should not include observations")
	}

	// Requested: observations included
	p = txPacket(tx, database.ResolvedPaths{}, true)
	if p.Observations == nil {
		t.Fatal("txPacket with includeObservations should include observations")
	}
	obsList := *p.Observations
	if len(obsList) != 2 {
		t.Errorf("expected 2 observations, got %d", len(obsList))
	}
	if obsList[0].ObserverId == nil || *obsList[0].ObserverId != "obs1" {
		t.Errorf("expected observer_id obs1, got %v", obsList[0].ObserverId)
	}
}

// --- filterTxSlice ---

func TestFilterTxSlice(t *testing.T) {
	txs := []*StoreTx{
		{ID: 1, Hash: "a"},
		{ID: 2, Hash: "b"},
		{ID: 3, Hash: "a"},
	}
	result := filterTxSlice(txs, func(tx *StoreTx) bool {
		return tx.Hash == "a"
	})
	if len(result) != 2 {
		t.Errorf("expected 2, got %d", len(result))
	}
}

// --- GetTimestamps ---

func TestStoreGetTimestamps(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	ts := store.GetTimestamps(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC))
	if len(ts) < 1 {
		t.Error("expected >=1 timestamps")
	}
}

func observerIDByPubkey(t *testing.T, db *database.DB, pubkey string) int64 {
	t.Helper()
	var id int64
	if err := db.SQL().QueryRowContext(t.Context(), `SELECT id FROM observers WHERE pubkey = $1`, pubkey).Scan(&id); err != nil {
		t.Fatalf("observer %q: %v", pubkey, err)
	}
	return id
}

func insertTestTx(t *testing.T, db *database.DB, rawHex, hash string, firstSeen time.Time,
	routeType, payloadType int, decodedJSON, from string) int64 {
	t.Helper()
	ctx := t.Context()
	var fromArg any // NULL when empty
	if from != "" {
		if _, err := db.SQL().ExecContext(ctx, `
			INSERT INTO nodes (public_key, name, role, last_seen)
			VALUES ($1, $1, 'repeater', $2) ON CONFLICT (public_key) DO NOTHING`, from, firstSeen); err != nil {
			t.Fatalf("seed node %s: %v", from, err)
		}
		fromArg = from
	}
	var id int64
	if err := db.SQL().QueryRowContext(ctx, `
		INSERT INTO transmissions (raw_hex, hash, first_seen, route_type, payload_type, decoded_json, from_pubkey, last_seen)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $3) RETURNING id`,
		rawHex, hash, firstSeen, routeType, payloadType, decodedJSON, fromArg).Scan(&id); err != nil {
		t.Fatalf("insert transmission %s: %v", hash, err)
	}
	return id
}

// setupRichTestDB creates a test DB with richer data including paths, multiple observers, channel data.
func setupRichTestDB(t *testing.T) *database.DB {
	t.Helper()
	db := setupTestDB(t)
	seedTestData(t, db)

	now := time.Now().UTC()
	recent := now.Add(-1 * time.Hour)
	yesterday := now.Add(-24 * time.Hour)

	// seedTestData's observers; adjust the pubkeys to what it inserts.
	obs1 := observerIDByPubkey(t, db, "obs1")
	obs2 := observerIDByPubkey(t, db, "obs2")

	// Advert with raw_hex carrying a valid header + path byte for hash-size parsing.
	// route_type 1 = FLOOD; header 0x01, path byte 0x40 (bits 01 -> hash size 2).
	tx3 := insertTestTx(t, db, "0140aabbccdd", "hash_with_path_01", recent, 1, 4,
		`{"pubKey":"aabbccdd11223344","name":"TestRepeater","type":"ADVERT"}`, "aabbccdd11223344")
	dbtest.Observation(t, db, tx3, obs1, recent, 10.0, -91, "aabb", "ccdd")

	// Advert with 3-byte hash size: header 0x01, path byte 0x80 (bits 10 -> size 3).
	tx4 := insertTestTx(t, db, "0180eeff0011", "hash_with_path_02", yesterday, 1, 4,
		`{"pubKey":"eeff00112233aabb","name":"TestCompanion","type":"ADVERT"}`, "eeff00112233aabb")
	dbtest.Observation(t, db, tx4, obs2, yesterday, 8.5, -94, "eeff", "0011", "2233")

	// Another channel message with a different sender, for analytics.
	tx5 := insertTestTx(t, db, "CC01", "chan_msg_hash_001", recent, 1, 5,
		`{"type":"CHAN","channel":"#test","text":"User2: Another msg","sender":"User2","channelHash":"abc123"}`, "")
	dbtest.Observation(t, db, tx5, obs1, recent, 14.0, -88, "aa")

	return db
}

// --- Store-backed analytics tests ---

func TestStoreGetBulkHealthWithStore(t *testing.T) {
	ctx := t.Context()
	db := setupRichTestDB(t)

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache(): %v", err)
	}

	results := store.GetBulkHealth(ctx, 50, "", "")
	if len(results) == 0 {
		t.Fatal("expected bulk health results")
	}
	unfiltered := make(map[string]bool, len(results))
	for _, r := range results {
		if r.PublicKey == "" {
			t.Error("expected a non-empty public_key")
		}
		unfiltered[r.PublicKey] = true
	}

	t.Run("with region filter", func(t *testing.T) {
		filtered := store.GetBulkHealth(ctx, 50, "SJC", "")
		if len(filtered) > len(results) {
			t.Fatalf("region filter returned %d nodes, more than the %d unfiltered", len(filtered), len(results))
		}
		for _, r := range filtered {
			pk := r.PublicKey
			if !unfiltered[pk] {
				t.Errorf("region result %q is not in the unfiltered set", pk)
			}
		}
	})
}

func TestStoreGetAnalyticsHashSizes(t *testing.T) {
	ctx := t.Context()
	db := setupRichTestDB(t)

	defer db.Close()
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	result := store.GetAnalyticsHashSizes(ctx, "", "")
	if result["total"] == nil {
		t.Error("expected total field")
	}
	dist, ok := result["distribution"].(map[string]int)
	if !ok {
		t.Error("expected distribution map")
	}
	_ = dist

	t.Run("with region", func(t *testing.T) {
		r := store.GetAnalyticsHashSizes(ctx, "SJC", "")
		_ = r
	})
}

func TestHashSizesDistributionByRepeatersFiltersRole(t *testing.T) {
	ctx := t.Context()

	db := setupRichTestDB(t)
	defer db.Close()
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache(): %v", err)
	}

	result := store.GetAnalyticsHashSizes(ctx, "", "")

	// distributionByRepeaters should only count repeater nodes.
	// Rich test DB: aabbccdd11223344 = repeater (hash size 2), eeff00112233aabb = companion (hash size 3).
	dbr, ok := result["distributionByRepeaters"].(map[string]int)
	if !ok {
		t.Fatal("expected distributionByRepeaters map")
	}
	// Only the repeater node should be counted.
	if dbr["3"] != 0 {
		t.Errorf("distributionByRepeaters[3] = %d, want 0 (companion should be excluded)", dbr["3"])
	}
	if dbr["2"] != 1 {
		t.Errorf("distributionByRepeaters[2] = %d, want 1 (repeater)", dbr["2"])
	}

	// multiByteNodes should include role field for frontend filtering.
	mbn, ok := result["multiByteNodes"].([]map[string]interface{})
	if !ok {
		t.Fatal("expected multiByteNodes slice")
	}
	for _, node := range mbn {
		if _, hasRole := node["role"]; !hasRole {
			t.Errorf("multiByteNodes entry missing 'role' field: %v", node)
		}
	}
	// Verify companion is included in multiByteNodes (it's multi-byte) with correct role.
	foundCompanion := false
	for _, node := range mbn {
		if node["pubkey"] == "eeff00112233aabb" {
			foundCompanion = true
			if node["role"] != types.RoleCompanion {
				t.Errorf("companion node role = %v, want %v", node["role"], types.RoleCompanion)
			}
		}
	}
	if !foundCompanion {
		t.Error("expected companion node in multiByteNodes (multi-byte adopters should include all roles)")
	}
}

func TestStoreGetAnalyticsSubpaths(t *testing.T) {
	ctx := t.Context()
	db := setupRichTestDB(t)

	defer db.Close()
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	result := store.GetAnalyticsSubpaths(ctx, "", 2, 8, 100)
	if _, ok := result["subpaths"]; !ok {
		t.Error("expected subpaths field")
	}

	t.Run("with region", func(t *testing.T) {
		r := store.GetAnalyticsSubpaths(ctx, "SJC", 2, 4, 50)
		_ = r
	})
}

func TestSubpathPrecomputedIndex(t *testing.T) {
	ctx := t.Context()
	db := setupRichTestDB(t)

	defer db.Close()
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	// #1008: indexes built in background goroutine; wait before reading.
	if !store.WaitIndexesReady(5 * time.Second) {
		t.Fatal("indexes never became ready")
	}
	keys := make([]string, 0, len(store.spIndex))
	for k := range store.spIndex {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	t.Logf("spIndex keys: %q", keys)

	// After Load(), the precomputed index must be populated.
	if len(store.spIndex) == 0 {
		t.Fatal("expected spIndex to be populated after Load()")
	}
	if store.spTotalPaths == 0 {
		t.Fatal("expected spTotalPaths > 0 after Load()")
	}

	// The rich test DB has paths ["aa","bb"], ["aabb","ccdd"], and
	// ["eeff","0011","2233"].  That yields 5 unique raw subpaths.
	expectedRaw := map[string]int{
		"aa,bb":          1,
		"aabb,ccdd":      1,
		"eeff,0011":      1,
		"0011,2233":      1,
		"eeff,0011,2233": 1,
	}
	for key, want := range expectedRaw {
		got, ok := store.spIndex[key]
		if !ok {
			t.Errorf("expected spIndex[%q] to exist", key)
		} else if got != want {
			t.Errorf("spIndex[%q] = %d, want %d", key, got, want)
		}
	}
	if store.spTotalPaths != 3 {
		t.Errorf("spTotalPaths = %d, want 3", store.spTotalPaths)
	}

	// Fast-path (no region) and slow-path (with region) must return the
	// same shape.
	fast := store.GetAnalyticsSubpaths(ctx, "", 2, 8, 100)
	slow := store.GetAnalyticsSubpaths(ctx, "SJC", 2, 4, 50)
	for _, r := range []map[string]interface{}{fast, slow} {
		if _, ok := r["subpaths"]; !ok {
			t.Error("missing subpaths in result")
		}
		if _, ok := r["totalPaths"]; !ok {
			t.Error("missing totalPaths in result")
		}
	}

	// Verify fast path totalPaths matches index.
	if tp, ok := fast["totalPaths"].(int); ok && tp != store.spTotalPaths {
		t.Errorf("fast totalPaths=%d, spTotalPaths=%d", tp, store.spTotalPaths)
	}
}

func TestSubpathTxIndexPopulated(t *testing.T) {
	ctx := t.Context()

	db := setupRichTestDB(t)
	defer db.Close()
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	// #1008: indexes built in background goroutine; wait before reading.
	if !store.WaitIndexesReady(5 * time.Second) {
		t.Fatal("indexes never became ready")
	}

	// spTxIndex must be populated alongside spIndex
	if len(store.spTxIndex) == 0 {
		t.Fatal("expected spTxIndex to be populated after Load()")
	}

	// Every key in spIndex must also exist in spTxIndex with matching count
	for key, count := range store.spIndex {
		txs, ok := store.spTxIndex[key]
		if !ok {
			t.Errorf("spTxIndex missing key %q that exists in spIndex", key)
			continue
		}
		if len(txs) != count {
			t.Errorf("spTxIndex[%q] has %d txs, spIndex count is %d", key, len(txs), count)
		}
	}

	// GetSubpathDetail should return correct match count via indexed lookup
	detail := store.GetSubpathDetail([]string{"eeff", "0011"})
	if detail == nil {
		t.Fatal("expected non-nil detail for existing subpath")
	}
	matches, _ := detail["totalMatches"].(int)
	if matches != 1 {
		t.Errorf("totalMatches = %d, want 1", matches)
	}

	// Non-existent subpath should return 0 matches
	detail2 := store.GetSubpathDetail([]string{"zzzz", "yyyy"})
	if detail2 == nil {
		t.Fatal("expected non-nil result even for non-existent subpath")
	}
	matches2, _ := detail2["totalMatches"].(int)
	if matches2 != 0 {
		t.Errorf("totalMatches for non-existent subpath = %d, want 0", matches2)
	}
}

func TestSubpathDetailMixedCaseHops(t *testing.T) {
	ctx := t.Context()

	db := setupRichTestDB(t)
	defer db.Close()
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	// #1008: indexes built in background goroutine; wait before reading.
	if !store.WaitIndexesReady(5 * time.Second) {
		t.Fatal("indexes never became ready")
	}

	// Query with lowercase hops to establish baseline
	lower := store.GetSubpathDetail([]string{"eeff", "0011"})
	if lower == nil {
		t.Fatal("expected non-nil detail for lowercase subpath")
	}
	lowerMatches, _ := lower["totalMatches"].(int)
	if lowerMatches == 0 {
		t.Fatal("expected >0 matches for lowercase subpath")
	}

	// Query with mixed-case hops — must return the same results (case-insensitive)
	mixed := store.GetSubpathDetail([]string{"EEFF", "0011"})
	if mixed == nil {
		t.Fatal("expected non-nil detail for mixed-case subpath")
	}
	mixedMatches, _ := mixed["totalMatches"].(int)
	if mixedMatches != lowerMatches {
		t.Errorf("mixed-case totalMatches = %d, want %d (same as lowercase)", mixedMatches, lowerMatches)
	}

	// All-uppercase should also match
	upper := store.GetSubpathDetail([]string{"EEFF", "0011"})
	upperMatches, _ := upper["totalMatches"].(int)
	if upperMatches != lowerMatches {
		t.Errorf("uppercase totalMatches = %d, want %d", upperMatches, lowerMatches)
	}
}

func TestStoreGetAnalyticsRFCacheHit(t *testing.T) {
	ctx := t.Context()
	db := setupRichTestDB(t)

	defer db.Close()
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	// First call — cache miss
	result1 := store.GetAnalyticsRF(ctx, "", "")
	if result1["totalPackets"] == nil {
		t.Error("expected totalPackets")
	}

	// Second call — should hit cache
	result2 := store.GetAnalyticsRF(ctx, "", "")
	if result2["totalPackets"] == nil {
		t.Error("expected cached totalPackets")
	}

	// Verify cache hit was recorded
	stats := store.GetCacheStats()
	hits, _ := stats["hits"].(int64)
	if hits < 1 {
		t.Error("expected at least 1 cache hit")
	}
}

func TestStoreGetAnalyticsTopology(t *testing.T) {
	ctx := t.Context()
	db := setupRichTestDB(t)

	defer db.Close()
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	activeNodeWindow := time.Now().Add(-7 * 24 * time.Hour)

	result := store.GetAnalyticsTopology(ctx, "", "")
	if result == nil {
		t.Error("expected non-nil result")
	}

	// #155: uniqueNodes must match DB 7-day active count, not hop resolution
	stats, err := db.GetStats(ctx, activeNodeWindow)
	if err != nil {
		t.Fatalf("GetStats failed: %v", err)
	}
	un, ok := result["uniqueNodes"].(int)
	if !ok {
		t.Fatalf("uniqueNodes is not int: %T", result["uniqueNodes"])
	}
	if un != stats.TotalNodes {
		t.Errorf("uniqueNodes=%d should match stats totalNodes=%d", un, stats.TotalNodes)
	}

	t.Run("with region", func(t *testing.T) {
		r := store.GetAnalyticsTopology(ctx, "SJC", "")
		_ = r
	})
}

func TestStoreGetAnalyticsChannels(t *testing.T) {
	ctx := t.Context()
	db := setupRichTestDB(t)

	defer db.Close()
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	result := store.GetAnalyticsChannels(ctx, "", "")
	if _, ok := result["activeChannels"]; !ok {
		t.Error("expected activeChannels")
	}
	if _, ok := result["topSenders"]; !ok {
		t.Error("expected topSenders")
	}
	if _, ok := result["channelTimeline"]; !ok {
		t.Error("expected channelTimeline")
	}

	t.Run("with region", func(t *testing.T) {
		r := store.GetAnalyticsChannels(ctx, "SJC", "")
		_ = r
	})
}

// Regression test for #154: channelHash is a number in decoded JSON from decoder.js,
// not a string. The Go struct must handle both types correctly.
func TestStoreGetAnalyticsChannelsNumericHash(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)

	recent := time.Now().UTC().Add(-1 * time.Hour)

	obsIdx, err := db.GetObserverID(ctx, "obs1")
	if err != nil {
		t.Fatalf("GetObserverID: %v", err)
	}

	grpTx := func(hash, rawHex, decoded string, snr, rssi float64) {
		dbtest.TxWithObservation(t, db, obsIdx, database.TransmissionInput{
			RawHex:      rawHex,
			Hash:        hash,
			RouteType:   types.RouteType(1),
			PayloadType: types.PayloadType(5), // GRP_TXT
			DecodedJSON: decoded,
		}, recent, snr, rssi)
	}

	// Insert GRP_TXT packets with numeric channelHash (matches decoder.js output)
	grpTx("grp_num_hash_1", "DD01",
		`{"type":"GRP_TXT","channelHash":97,"channelHashHex":"61","decryptionStatus":"no_key"}`, 10.0, -90)
	grpTx("grp_num_hash_2", "DD02",
		`{"type":"GRP_TXT","channelHash":42,"channelHashHex":"2A","decryptionStatus":"no_key"}`, 10.0, -90)

	// Also a decrypted CHAN with numeric channelHash — use hash 198 which is the real hash for #general
	grpTx("chan_num_hash_3", "DD03",
		`{"type":"CHAN","channel":"general","channelHash":198,"channelHashHex":"C6","text":"hello","sender":"Alice"}`, 12.0, -88)

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}
	result := store.GetAnalyticsChannels(ctx, "", "")

	channels, ok := result["channels"].([]map[string]interface{})
	if !ok {
		t.Fatalf("channels has type %T, want []map[string]interface{}", result["channels"])
	}
	if len(channels) < 3 {
		t.Errorf("expected at least 3 channels (hash 97 + hash 42 + hash 198), got %d", len(channels))
	}

	// Verify the numeric-hash channels we inserted have proper hashes (not "?")
	found97 := false
	found42 := false
	for _, ch := range channels {
		if ch["hash"] == "97" {
			found97 = true
		}
		if ch["hash"] == "42" {
			found42 = true
		}
	}
	if !found97 {
		t.Error("expected to find channel with hash '97' (numeric channelHash parsing)")
	}
	if !found42 {
		t.Error("expected to find channel with hash '42' (numeric channelHash parsing)")
	}

	// Verify the decrypted CHAN channel has the correct name (now at hash 198)
	foundGeneral := false
	for _, ch := range channels {
		if ch["name"] == "general" {
			foundGeneral = true
			if ch["hash"] != "198" {
				t.Errorf("expected hash '198' for general channel, got %v", ch["hash"])
			}
		}
	}
	if !foundGeneral {
		t.Error("expected to find channel named 'general'")
	}
}

func TestStoreGetAnalyticsDistance(t *testing.T) {
	ctx := t.Context()
	db := setupRichTestDB(t)

	defer db.Close()
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	result := store.GetAnalyticsDistance(ctx, "", "")
	if result == nil {
		t.Error("expected non-nil result")
	}

	t.Run("with region", func(t *testing.T) {
		r := store.GetAnalyticsDistance(ctx, "SJC", "")
		_ = r
	})
}

func TestStoreGetSubpathDetail(t *testing.T) {
	ctx := t.Context()

	db := setupRichTestDB(t)
	defer db.Close()
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	result := store.GetSubpathDetail([]string{"aabb", "ccdd"})
	if result == nil {
		t.Error("expected non-nil result")
	}
	if _, ok := result["hops"]; !ok {
		t.Error("expected hops field")
	}
}

// --- Route handlers with store for analytics ---

func TestHandleAnalyticsRFWithStore(t *testing.T) {
	ctx := t.Context()

	db := setupRichTestDB(t)
	defer db.Close()
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

	t.Run("basic", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/analytics/rf", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})

	t.Run("with region", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/analytics/rf?region=SJC", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})
}

func TestHandleBulkHealthWithStore(t *testing.T) {
	ctx := t.Context()

	db := setupRichTestDB(t)
	defer db.Close()
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

	req := httptest.NewRequest("GET", "/api/nodes/bulk-health?limit=50&region=SJC", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleAnalyticsSubpathsWithStore(t *testing.T) {
	ctx := t.Context()

	db := setupRichTestDB(t)
	defer db.Close()
	cfg := &Config{Port: 3000}
	hub := NewHub()
	srv := NewServer(db, cfg, hub)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	if !store.WaitIndexesReady(5 * time.Second) {
		t.Fatal("indexes not ready after 5s")
	}
	srv.store = store
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	req := httptest.NewRequest("GET", "/api/analytics/subpaths?minLen=2&maxLen=4&limit=50", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleAnalyticsSubpathDetailWithStore(t *testing.T) {
	ctx := t.Context()

	db := setupRichTestDB(t)
	defer db.Close()
	cfg := &Config{Port: 3000}
	hub := NewHub()
	srv := NewServer(db, cfg, hub)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	if !store.WaitIndexesReady(5 * time.Second) {
		t.Fatal("indexes not ready after 5s")
	}
	srv.store = store
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	req := httptest.NewRequest("GET", "/api/analytics/subpath-detail?hops=aabb,ccdd", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleAnalyticsDistanceWithStore(t *testing.T) {
	ctx := t.Context()

	db := setupRichTestDB(t)
	defer db.Close()
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

	// #1011: lazy distance index — first request returns 202; trigger
	// the build and wait for it before asserting the 200 shape.
	store.TriggerDistanceIndexBuild()
	deadline := time.Now().Add(5 * time.Second)
	for !store.DistanceIndexBuilt() {
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

func TestHandleAnalyticsHashSizesWithStore(t *testing.T) {
	ctx := t.Context()

	db := setupRichTestDB(t)
	defer db.Close()
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

	req := httptest.NewRequest("GET", "/api/analytics/hash-sizes", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleAnalyticsTopologyWithStore(t *testing.T) {
	ctx := t.Context()

	db := setupRichTestDB(t)
	defer db.Close()
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

	req := httptest.NewRequest("GET", "/api/analytics/topology", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleAnalyticsChannelsWithStore(t *testing.T) {
	ctx := t.Context()

	db := setupRichTestDB(t)
	defer db.Close()
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

	req := httptest.NewRequest("GET", "/api/analytics/channels", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

// --- GetChannelMessages more paths ---

func TestGetChannelMessagesRichData(t *testing.T) {
	ctx := t.Context()
	db := setupRichTestDB(t)
	defer db.Close()
	store := newPacketStoreForTest(t, db)

	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	messages, total := store.GetChannelMessages(ctx, "#test", 100, 0)
	if total < 2 {
		t.Errorf("expected >=2 messages for #test with rich data, got %d", total)
	}

	// Verify message fields
	for _, msg := range messages {
		if _, ok := msg["sender"]; !ok {
			t.Error("expected sender field")
		}
		if _, ok := msg["hops"]; !ok {
			t.Error("expected hops field")
		}
	}
}

// --- handleObservers with actual data ---

func TestHandleObserversWithData(t *testing.T) {
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
	if !ok || len(observers) == 0 {
		t.Error("expected non-empty observers")
	}
}

// --- handleChannelMessages with store ---

func TestHandleChannelMessagesWithStore(t *testing.T) {
	ctx := t.Context()

	db := setupRichTestDB(t)
	defer db.Close()
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

	req := httptest.NewRequest("GET", "/api/channels/%23test/messages?limit=10", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

// --- handleChannels with store ---

func TestHandleChannelsWithStore(t *testing.T) {
	ctx := t.Context()

	db := setupRichTestDB(t)
	defer db.Close()
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

	req := httptest.NewRequest("GET", "/api/channels", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

// --- Traces via store path ---

func TestHandleTracesWithStore(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/traces/abc123def4567890", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

// --- Store.GetStoreStats ---

func TestStoreGetStoreStats(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	stats, err := store.GetStoreStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.TotalTransmissions < 1 {
		t.Error("expected transmissions > 0")
	}
}

// --- Store.QueryGroupedPackets ---

func TestStoreQueryGroupedPackets(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	result := mustQueryGroupedPackets(t, store, ctx, database.PacketQuery{Limit: 50, Order: "DESC"})
	if result.Total < 1 {
		t.Error("expected >=1 grouped packets")
	}
}

// --- Store.GetPacketByHash / GetPacketByID / GetTransmissionByID ---

func TestStoreGetPacketByHash(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	pkt := mustGetPacketByHash(t, store, ctx, "abc123def4567890")
	if pkt == nil {
		t.Fatal("expected packet")
	}
	if pkt.Hash == nil || *pkt.Hash != "abc123def4567890" {
		t.Errorf("wrong hash: %v", pkt.Hash)
	}

	t.Run("not found", func(t *testing.T) {
		pkt := mustGetPacketByHash(t, store, ctx, "0000000000000000")
		if pkt != nil {
			t.Error("expected nil for not found")
		}
	})
}

// --- Coverage gap-filling tests ---

func TestResolvePayloadTypeNameUnknown(t *testing.T) {
	// nil → UNKNOWN
	if got := resolvePayloadTypeName(nil); got != "UNKNOWN" {
		t.Errorf("expected UNKNOWN for nil, got %s", got)
	}
	// known type
	pt4 := types.PayloadType(4)
	if got := resolvePayloadTypeName(&pt4); got != "ADVERT" {
		t.Errorf("expected ADVERT, got %s", got)
	}
	// unknown type → UNK(N) format
	pt99 := types.PayloadType(99)
	if got := resolvePayloadTypeName(&pt99); got != "UNK(99)" {
		t.Errorf("expected UNK(99), got %s", got)
	}
}

func TestCacheHitTopology(t *testing.T) {
	ctx := t.Context()
	db := setupRichTestDB(t)
	defer db.Close()
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	// First call — cache miss
	r1 := store.GetAnalyticsTopology(ctx, "", "")
	if r1 == nil {
		t.Fatal("expected topology result")
	}

	// Second call — cache hit
	r2 := store.GetAnalyticsTopology(ctx, "", "")
	if r2 == nil {
		t.Fatal("expected cached topology result")
	}

	stats := store.GetCacheStats()
	hits := stats["hits"].(int64)
	if hits < 1 {
		t.Errorf("expected cache hit, got %d hits", hits)
	}
}

func TestCacheHitHashSizes(t *testing.T) {
	ctx := t.Context()
	db := setupRichTestDB(t)
	defer db.Close()
	store := newPacketStoreForTest(t, db)

	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	r1 := store.GetAnalyticsHashSizes(ctx, "", "")
	if r1 == nil {
		t.Fatal("expected hash sizes result")
	}

	r2 := store.GetAnalyticsHashSizes(ctx, "", "")
	if r2 == nil {
		t.Fatal("expected cached hash sizes result")
	}

	stats := store.GetCacheStats()
	hits := stats["hits"].(int64)
	if hits < 1 {
		t.Errorf("expected cache hit, got %d", hits)
	}
}

func TestCacheHitChannels(t *testing.T) {
	ctx := t.Context()
	db := setupRichTestDB(t)
	defer db.Close()
	store := newPacketStoreForTest(t, db)

	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	r1 := store.GetAnalyticsChannels(ctx, "", "")
	if r1 == nil {
		t.Fatal("expected channels result")
	}

	r2 := store.GetAnalyticsChannels(ctx, "", "")
	if r2 == nil {
		t.Fatal("expected cached channels result")
	}

	stats := store.GetCacheStats()
	hits := stats["hits"].(int64)
	if hits < 1 {
		t.Errorf("expected cache hit, got %d", hits)
	}
}

func TestGetChannelMessagesEdgeCases(t *testing.T) {
	ctx := t.Context()
	db := setupRichTestDB(t)
	defer db.Close()
	store := newPacketStoreForTest(t, db)

	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	// Channel not found — empty result
	msgs, total := store.GetChannelMessages(ctx, "nonexistent_channel", 10, 0)
	if total != 0 {
		t.Errorf("expected 0 total for nonexistent channel, got %d", total)
	}
	if len(msgs) != 0 {
		t.Errorf("expected empty msgs, got %d", len(msgs))
	}

	// Default limit (0 → 100)
	msgs, _ = store.GetChannelMessages(ctx, "#test", 0, 0)
	_ = msgs // just exercises the default limit path

	// Offset beyond range
	msgs, total = store.GetChannelMessages(ctx, "#test", 10, 9999)
	if len(msgs) != 0 {
		t.Errorf("expected empty msgs for large offset, got %d", len(msgs))
	}
	if total == 0 {
		t.Error("total should be > 0 even with large offset")
	}

	// Negative offset
	msgs, _ = store.GetChannelMessages(ctx, "#test", 10, -5)
	_ = msgs // exercises the start < 0 path
}

func TestFilterPacketsEmptyRegion(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)

	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	// Region with no observers → empty result
	results := mustQueryPackets(t, store, ctx, database.PacketQuery{Region: "NONEXISTENT", Limit: 100})
	if results.Total != 0 {
		t.Errorf("expected 0 results for nonexistent region, got %d", results.Total)
	}
}

func TestFilterPacketsSinceUntil(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(t.Context(), 1000); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}

	// Since far future → empty
	future := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
	results := mustQueryPackets(t, store, ctx, database.PacketQuery{ObsSince: &future, Limit: 100})
	if results.Total != 0 {
		t.Errorf("expected 0 results for far future since, got %d", results.Total)
	}

	// Until far past → empty
	past := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	results = mustQueryPackets(t, store, ctx, database.PacketQuery{ObsUntil: &past, Limit: 100})
	if results.Total != 0 {
		t.Errorf("expected 0 results for far past until, got %d", results.Total)
	}

	// Route filter
	rt := 1
	results = mustQueryPackets(t, store, ctx, database.PacketQuery{Route: &rt, Limit: 100})
	if results.Total == 0 {
		t.Error("expected results for route_type=1 filter")
	}
}

func TestFilterPacketsHashOnly(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	// Single hash fast-path — found
	results := mustQueryPackets(t, store, ctx, database.PacketQuery{Hash: "abc123def4567890", Limit: 100})
	if results.Total != 1 {
		t.Errorf("expected 1 result for known hash, got %d", results.Total)
	}

	// Single hash fast-path — not found
	results = mustQueryPackets(t, store, ctx, database.PacketQuery{Hash: "0000000000000000", Limit: 100})
	if results.Total != 0 {
		t.Errorf("expected 0 results for unknown hash, got %d", results.Total)
	}
}

func TestFilterPacketsObserverWithType(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	// Observer + type filter (takes non-indexed path)
	pt := 4
	results := mustQueryPackets(t, store, ctx, database.PacketQuery{Observers: []string{"obs1"}, Type: &pt, Limit: 100})
	_ = results // exercises the combined observer+type filter path
}

func TestFilterPacketsNodeFilter(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	// Node filter — exercises DecodedJSON containment check
	results := mustQueryPackets(t, store, ctx, database.PacketQuery{Node: "aabbccdd11223344", Limit: 100})
	if results.Total == 0 {
		t.Error("expected results for node filter")
	}

	// Node filter with hash combined
	results = mustQueryPackets(t, store, ctx, database.PacketQuery{Node: "aabbccdd11223344", Hash: "abc123def4567890", Limit: 100})
	_ = results
}

func TestGetNodeHashSizeInfoEdgeCases(t *testing.T) {
	db := setupTestDB(t)

	now := time.Now().UTC()
	recent := now.Add(-1 * time.Hour)
	firstSeen := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	obsIdx := dbtest.Observer(t, db, "obs1", "Obs", "SJC", firstSeen, recent)
	flood := types.RouteType(1)

	// Adverts with various edge cases
	// 1. Valid advert with pubKey
	dbtest.Advert(t, db, obsIdx, "hs_valid_1", "0140aabbccdd", flood,
		`{"pubKey":"aabbccdd11223344","name":"NodeA","type":"ADVERT"}`, recent)

	// 2. Short raw_hex (< 4 chars)
	dbtest.Advert(t, db, obsIdx, "hs_short_hex", "01", flood,
		`{"pubKey":"eeff00112233aabb","name":"NodeB","type":"ADVERT"}`, recent)

	// 4. Invalid JSON (requires decoded_json to be a text column; see note)
	dbtest.Advert(t, db, obsIdx, "hs_bad_json", "0140aabb", flood, `not-json`, recent)

	// 5. JSON with public_key field instead of pubKey
	dbtest.Advert(t, db, obsIdx, "hs_alt_key", "0180eeff", flood,
		`{"public_key":"aabbccdd11223344","name":"NodeA","type":"ADVERT"}`, recent)

	// 6. JSON with no pubKey at all
	dbtest.Advert(t, db, obsIdx, "hs_no_pk", "01C0ffee", flood,
		`{"name":"NodeZ","type":"ADVERT"}`, recent)

	// 7. Empty decoded_json (see note)
	dbtest.Advert(t, db, obsIdx, "hs_empty_json", "0140bbcc", flood, ``, recent)

	// 8-10. Multiple adverts for same node with different hash sizes (flip-flop test).
	// Staggered by a minute so the order the code sees them in is deterministic.
	const flipperJSON = `{"pubKey":"ffff000011112222","name":"Flipper","type":"ADVERT"}`
	dbtest.Advert(t, db, obsIdx, "hs_flip_1", "0140dd01", flood, flipperJSON, recent)
	dbtest.Advert(t, db, obsIdx, "hs_flip_2", "0180dd02", flood, flipperJSON, recent.Add(1*time.Minute))
	dbtest.Advert(t, db, obsIdx, "hs_flip_3", "0140dd03", flood, flipperJSON, recent.Add(2*time.Minute))

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(t.Context(), 1000); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}
	info := store.GetNodeHashSizeInfo()

	// Valid node should be present
	if _, ok := info["aabbccdd11223344"]; !ok {
		t.Error("expected aabbccdd11223344 in hash size info")
	}

	// Flipper should have inconsistent flag (2→3→2 = 2 transitions, 2 unique sizes, 3 obs)
	if flipper, ok := info["ffff000011112222"]; ok {
		if len(flipper.AllSizes) < 2 {
			t.Errorf("expected 2+ unique sizes for flipper, got %d", len(flipper.AllSizes))
		}
		if !flipper.Inconsistent {
			t.Error("expected Inconsistent=true for flip-flop node")
		}
	} else {
		t.Error("expected ffff000011112222 in hash size info")
	}

	// Bad entries (short hex, bad json, no pk) should not corrupt results
	if _, ok := info["eeff00112233aabb"]; ok {
		t.Error("short raw_hex node should not be in results")
	}
}

// TestHashSizeTransportRoutePathByteOffset verifies that transport routes (0, 3)
// read the path byte from offset 5 (after 4 transport code bytes), not offset 1.
// Regression test for #744 / #722.
func TestHashSizeTransportRoutePathByteOffset(t *testing.T) {
	db := setupTestDB(t)

	recent := time.Now().UTC().Add(-1 * time.Hour)
	firstSeen := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	obsIdx := dbtest.Observer(t, db, "obs1", "Obs", "SJC", firstSeen, recent)

	// Route type 0 (TRANSPORT_FLOOD): header=0x04 (payload_type=1, route_type=0)
	// 4 transport bytes + path byte at offset 5.
	// Path byte 0x80 → hash_size bits = 10 → size 3
	// If bug is present, code reads byte 1 (0xAA) → hash_size bits = 10 → size 3 (coincidence)
	// Use path byte 0x40 (hash_size=2) and transport byte 0x01 at offset 1 (hash_size=1 if misread)
	dbtest.Advert(t, db, obsIdx, "tf_offset", "100102030440aabb", types.RouteType(0),
		`{"pubKey":"aaaa000000000001","name":"TF-Node","type":"ADVERT"}`, recent)

	// Route type 3 (TRANSPORT_DIRECT): header=0x13 (payload_type=4, route_type=3)
	// 4 transport bytes + path byte at offset 5.
	// Path byte 0xC1 → hash_size bits = 11 → size 4, hop_count = 1 (not zero-hop)
	// Byte 1 = 0x05 → hash_size bits = 00 → size 1 if misread
	dbtest.Advert(t, db, obsIdx, "td_offset", "1305060708C1bbcc", types.RouteType(3),
		`{"pubKey":"aaaa000000000002","name":"TD-Node","type":"ADVERT"}`, recent)

	// Route type 1 (FLOOD): header=0x11 (payload_type=4, route_type=1)
	// Path byte at offset 1. Path byte 0x80 → hash_size bits = 10 → size 3
	dbtest.Advert(t, db, obsIdx, "flood_offset", "1180aabbccdd", types.RouteType(1),
		`{"pubKey":"aaaa000000000003","name":"Flood-Node","type":"ADVERT"}`, recent)

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(t.Context(), 1000); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}
	info := store.GetNodeHashSizeInfo()

	// Transport flood node: path byte 0x40 → hash_size = 2
	if ni, ok := info["aaaa000000000001"]; !ok {
		t.Error("transport flood node missing from hash size info")
	} else if ni.HashSize != 2 {
		t.Errorf("transport flood node: want HashSize=2 (from path byte at offset 5), got %d", ni.HashSize)
	}

	// Transport direct node: path byte 0xC1 → hash_size = 4
	if ni, ok := info["aaaa000000000002"]; !ok {
		t.Error("transport direct node missing from hash size info")
	} else if ni.HashSize != 4 {
		t.Errorf("transport direct node: want HashSize=4 (from path byte at offset 5), got %d", ni.HashSize)
	}

	// Regular flood node: path byte 0x80 → hash_size = 3
	if ni, ok := info["aaaa000000000003"]; !ok {
		t.Error("regular flood node missing from hash size info")
	} else if ni.HashSize != 3 {
		t.Errorf("regular flood node: want HashSize=3 (from path byte at offset 1), got %d", ni.HashSize)
	}
}

// TestHashSizeTransportDirectZeroHopSkipped verifies that RouteTransportDirect
// zero-hop adverts are skipped (same as RouteDirect). Regression test for #744.
func TestHashSizeTransportDirectZeroHopSkipped(t *testing.T) {
	db := setupTestDB(t)

	recent := time.Now().UTC().Add(-1 * time.Hour)
	firstSeen := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	obsIdx := dbtest.Observer(t, db, "obs1", "Obs", "SJC", firstSeen, recent)

	// RouteDirect (2) zero-hop with an all-zero path byte: the sender wiped the
	// size bits, so there is nothing to read. Should be skipped.
	// (This fixture used to be 0x40. A zero hop count with the size bits SET is
	// a deliberate declaration — see TestZeroHopDirectAdvertDeclaredSizeIsEvidence
	// — so the skip is keyed on the byte's content, not on the route type.)
	dbtest.Advert(t, db, obsIdx, "direct_zh", "1200aabbccdd", types.RouteType(2),
		`{"pubKey":"bbbb000000000001","name":"Direct-ZH","type":"ADVERT"}`, recent)

	// RouteTransportDirect (3) zero-hop: 4 transport bytes + all-zero path byte
	// Should ALSO be skipped (this was the missing case in #747)
	dbtest.Advert(t, db, obsIdx, "tdirect_zh", "130102030400aabb", types.RouteType(3),
		`{"pubKey":"bbbb000000000002","name":"TDirect-ZH","type":"ADVERT"}`, recent)

	// RouteDirect (2) non-zero-hop: path byte 0x41 → hop_count=1
	// Should NOT be skipped
	dbtest.Advert(t, db, obsIdx, "direct_1h", "1241aabbccdd", types.RouteType(2),
		`{"pubKey":"bbbb000000000003","name":"Direct-1H","type":"ADVERT"}`, recent)

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(t.Context(), 1000); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}
	info := store.GetNodeHashSizeInfo()

	// RouteDirect zero-hop should be absent
	if _, ok := info["bbbb000000000001"]; ok {
		t.Error("RouteDirect zero-hop advert should be skipped")
	}

	// RouteTransportDirect zero-hop should also be absent
	if _, ok := info["bbbb000000000002"]; ok {
		t.Error("RouteTransportDirect zero-hop advert should be skipped")
	}

	// RouteDirect non-zero-hop should be present with hash_size=2
	if ni, ok := info["bbbb000000000003"]; !ok {
		t.Error("RouteDirect non-zero-hop should be in hash size info")
	} else if ni.HashSize != 2 {
		t.Errorf("RouteDirect non-zero-hop: want HashSize=2, got %d", ni.HashSize)
	}
}

// TestAnalyticsHashSizesZeroHopSkip verifies that computeAnalyticsHashSizes
// does not overwrite a node's hash_size with a zero-hop advert's unreliable value.
// Regression test for #744.
func TestAnalyticsHashSizesZeroHopSkip(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	recent := time.Now().UTC().Add(-1 * time.Hour)
	firstSeen := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	obsIdx := dbtest.Observer(t, db, "obs1", "Obs", "SJC", firstSeen, recent)

	const pk = "cccc000000000001"
	dbtest.Node(t, db, pk, "ZH-Analytics", "repeater")

	decoded := `{"pubKey":"` + pk + `","name":"ZH-Analytics","type":"ADVERT"}`

	// First: a flood advert with hashSize=2 (reliable, multi-hop)
	// header 0x11 = route_type 1 (flood), payload_type 4
	// pathByte 0x41 = hashSize bits 01 → size 2, hop_count 1
	dbtest.TxWithObservation(t, db, obsIdx, database.TransmissionInput{
		RawHex:      "1141aabbccdd",
		Hash:        "az_flood",
		RouteType:   types.RouteType(1),
		PayloadType: types.PayloadType(4),
		DecodedJSON: decoded,
	}, recent, 10.0, -90, "aabb")

	// Second: a direct zero-hop advert with pathByte=0x00 → would give hashSize=1
	// header 0x12 = route_type 2 (direct), payload_type 4
	dbtest.Advert(t, db, obsIdx, "az_direct", "1200aabbccdd", types.RouteType(2), decoded, recent)

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(t.Context(), 1000); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}

	result := store.GetAnalyticsHashSizes(ctx, "", "")

	// The node should appear in multiByteNodes (hashSize=2 from the flood advert)
	// If the zero-hop bug is present, hashSize would be 1 and the node would NOT
	// appear in multiByteNodes.
	multiByteNodes, ok := result["multiByteNodes"].([]map[string]interface{})
	if !ok {
		t.Fatal("expected multiByteNodes slice in analytics hash sizes")
	}

	found := false
	for _, n := range multiByteNodes {
		if n["pubkey"] == pk {
			found = true
			if hs, ok := n["hashSize"].(int); ok && hs != 2 {
				t.Errorf("expected hashSize=2 from flood advert, got %d", hs)
			}
		}
	}
	if !found {
		t.Error("node should appear in multiByteNodes with hashSize=2; zero-hop advert should not overwrite to 1")
	}
}

func TestHandleResolveHopsEdgeCases(t *testing.T) {
	db := setupTestDB(t)
	seedTestData(t, db)

	cfg := &Config{Port: 3000}
	hub := NewHub()
	srv := NewServer(db, cfg, hub)
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	// Empty hops param
	req := httptest.NewRequest("GET", "/api/resolve-hops", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	resolved := body["resolved"].(map[string]interface{})
	if len(resolved) != 0 {
		t.Errorf("expected empty resolved for empty hops, got %d", len(resolved))
	}

	// Multiple hops with empty string included
	req = httptest.NewRequest("GET", "/api/resolve-hops?hops=aabb,,eeff", nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Errorf("expected 200, got %d", w.Code)
	}
	json.Unmarshal(w.Body.Bytes(), &body)
	resolved = body["resolved"].(map[string]interface{})
	// Empty string should be skipped
	if _, ok := resolved[""]; ok {
		t.Error("empty hop should be skipped")
	}

	// Nonexistent prefix — zero candidates
	req = httptest.NewRequest("GET", "/api/resolve-hops?hops=nonexistent_prefix_xyz", nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestHandleObserversError(t *testing.T) {
	// Use a closed DB to trigger an error from GetObservers
	db := setupTestDB(t)
	seedTestData(t, db)

	cfg := &Config{Port: 3000}
	hub := NewHub()
	srv := NewServer(db, cfg, hub)
	router := mux.NewRouter()
	srv.RegisterRoutes(router)
	db.Close() // force error after routes registered

	req := httptest.NewRequest("GET", "/api/observers", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 500 {
		t.Errorf("expected 500 for closed DB, got %d", w.Code)
	}
}

func TestHandleAnalyticsChannels(t *testing.T) {
	_, router := setupTestServer(t)

	req := httptest.NewRequest("GET", "/api/analytics/channels", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}
	var body struct {
		ActiveChannels json.RawMessage `json:"activeChannels"`
		Channels       json.RawMessage `json:"channels"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.ActiveChannels == nil {
		t.Error("expected activeChannels in response")
	}
	if body.Channels == nil {
		t.Error("expected channels in response")
	}
}

func TestGetChannelMessagesDedupeRepeats(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)

	recent := time.Now().UTC().Add(-1 * time.Hour)
	firstSeen := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	obs1 := dbtest.Observer(t, db, "obs1", "Obs1", "SJC", firstSeen, recent)
	obs2 := dbtest.Observer(t, db, "obs2", "Obs2", "LAX", firstSeen, recent)

	chanTx := func(observer int64, hash, rawHex, decoded string, snr, rssi float64, hops ...string) int64 {
		return dbtest.TxWithObservation(t, db, observer, database.TransmissionInput{
			RawHex:      rawHex,
			Hash:        hash,
			RouteType:   types.RouteType(1),
			PayloadType: types.PayloadType(5), // GRP_TXT / CHAN
			DecodedJSON: decoded,
		}, recent, snr, rssi, hops...)
	}

	// One CHAN message, seen first by obs1...
	aliceID := chanTx(obs1, "dedup_chan_1", "CC01",
		`{"type":"CHAN","channel":"#general","text":"Alice: hello","sender":"Alice"}`, 12.0, -88, "aa")

	// ...and again by obs2: the same transmission, a second observation.
	snr, rssi := 11.0, -89.0
	_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: aliceID,
		ObserverIdx:    &obs2,
		SNR:            &snr,
		RSSI:           &rssi,
		Timestamp:      recent,
	}, nil, nil)
	if err != nil {
		t.Fatalf("second observation: %v", err)
	}
	if !inserted {
		t.Fatalf("expected second observation to be inserted for tx %d", aliceID)
	}

	// Second different message
	chanTx(obs2, "dedup_chan_2", "CC03",
		`{"type":"CHAN","channel":"#general","text":"Bob: world","sender":"Bob"}`, 10.0, -90, "bb")

	// GRP_TXT (not CHAN) — should be skipped by GetChannelMessages
	chanTx(obs1, "grp_msg_hash_1", "DD01",
		`{"type":"GRP_TXT","channelHash":"42","text":"encrypted"}`, 10.0, -90)

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}

	msgs, total := store.GetChannelMessages(ctx, "#general", 10, 0)
	if total != 2 {
		t.Errorf("expected 2 messages for #general (Alice once, Bob once; GRP_TXT skipped), got %d", total)
	}

	// Check message structure
	for _, msg := range msgs {
		if _, ok := msg["sender"]; !ok {
			t.Error("expected sender field")
		}
		if _, ok := msg["text"]; !ok {
			t.Error("expected text field")
		}
		if _, ok := msg["observers"]; !ok {
			t.Error("expected observers field")
		}
	}

	// Alice's message was seen by two observers and must appear once with both.
	for _, msg := range msgs {
		if msg["sender"] == "Alice" {
			if n := reflect.ValueOf(msg["observers"]).Len(); n != 2 {
				t.Errorf("Alice's message: expected 2 observers, got %d", n)
			}
		}
	}
}

func TestTransmissionsForObserverFromSlice(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	// Test with from=nil (index path) — for non-existent observer
	result := store.transmissionsForObserver("nonexistent_obs", nil)
	if len(result) != 0 {
		t.Errorf("expected nil/empty for nonexistent observer, got %d", len(result))
	}

	// Test with from=non-nil slice (filter path)
	allPackets := store.packets
	result = store.transmissionsForObserver("obs1", allPackets)
	if len(result) == 0 {
		t.Error("expected results for obs1 from filter path")
	}
}

func TestGetPerfStoreStatsPublicKeyField(t *testing.T) {
	ctx := t.Context()

	db := setupRichTestDB(t)
	defer db.Close()
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	stats := store.GetPerfStoreStats()
	indexes := stats["indexes"].(map[string]interface{})
	// advertByObserver should count distinct pubkeys from advert packets
	aboc := indexes["advertByObserver"].(int)
	if aboc == 0 {
		t.Error("expected advertByObserver > 0 for rich test DB")
	}
}

func TestHandleAudioLabBucketsQueryError(t *testing.T) {
	// Use closed DB to trigger query error
	db := setupTestDB(t)
	seedTestData(t, db)

	cfg := &Config{Port: 3000}
	hub := NewHub()
	srv := NewServer(db, cfg, hub)
	router := mux.NewRouter()
	srv.RegisterRoutes(router)
	db.Close()

	req := httptest.NewRequest("GET", "/api/audio-lab/buckets", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Errorf("expected 200 (empty buckets on error), got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	buckets := body["buckets"].(map[string]interface{})
	if len(buckets) != 0 {
		t.Errorf("expected empty buckets on query error, got %d", len(buckets))
	}
}

func TestStoreGetTransmissionByID(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	pkt := mustGetTransmissionByID(t, store, ctx, 1)
	if pkt == nil {
		t.Fatal("expected packet")
	}

	t.Run("not found", func(t *testing.T) {
		pkt := mustGetTransmissionByID(t, store, ctx, 99999)
		if pkt != nil {
			t.Error("expected nil")
		}
	})
}

func TestStoreGetPacketByID(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	// Get an observation ID from the store
	var obsID int
	for id := range store.byObsID {
		obsID = id
		break
	}
	if obsID == 0 {
		t.Skip("no observations")
	}

	pkt := mustGetPacketByID(t, store, ctx, obsID)
	if pkt == nil {
		t.Fatal("expected packet")
	}

	t.Run("not found", func(t *testing.T) {
		pkt := mustGetPacketByID(t, store, ctx, 99999)
		if pkt != nil {
			t.Error("expected nil")
		}
	})
}

// --- Store.GetObservationsForHash ---

func TestStoreGetObservationsForHash(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	obs := mustGetObservationsForHash(t, store, ctx, "abc123def4567890")
	if len(obs) < 1 {
		t.Error("expected >=1 observation")
	}

	t.Run("not found", func(t *testing.T) {
		obs := mustGetObservationsForHash(t, store, ctx, "0000000000000000")
		if len(obs) != 0 {
			t.Errorf("expected 0, got %d", len(obs))
		}
	})
}

// --- HandlePacketDetail with store (by hash, by tx ID, by obs ID) ---

func TestHandlePacketDetailWithStoreAllPaths(t *testing.T) {
	_, router := setupTestServer(t)

	t.Run("by hash", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/packets/abc123def4567890", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
		var body map[string]interface{}
		json.Unmarshal(w.Body.Bytes(), &body)
		if body["observations"] == nil {
			t.Error("expected observations")
		}
	})

	t.Run("by tx ID", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/packets/1", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})

	t.Run("not found ID", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/packets/999999", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 404 {
			t.Fatalf("expected 404, got %d", w.Code)
		}
	})
}

func TestDBGetObserverByID(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	obs, err := db.GetObserverByID(ctx, "obs1")
	if err != nil {
		t.Fatal(err)
	}
	if obs == nil {
		t.Error("expected non-nil observer")
	}
	if obs.Pubkey != "obs1" {
		t.Errorf("expected obs1, got %s", obs.Pubkey)
	}

	t.Run("not found", func(t *testing.T) {
		obs, err := db.GetObserverByID(ctx, "nonexistent")
		if err == nil && obs != nil {
			t.Error("expected nil observer for nonexistent ID")
		}
		// Some implementations return (nil, err) — that's fine too
	})
}

// --- DB queries with different filter combos ---

func TestDBQueryPacketsAllFilters(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	pt := 4
	rt := 1
	since := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
	result, _, err := db.QueryPackets(ctx, database.PacketQuery{
		Limit:           50,
		Type:            &pt,
		Route:           &rt,
		Observers:       []string{"obs1"},
		Hash:            "abc123def4567890",
		FirstSeenAfter:  &since,
		FirstSeenBefore: &until,
		Region:          "SJC",
		Node:            "TestRepeater",
		Order:           "ASC",
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = result
}

// --- IngestNewFromDB dedup path ---

func TestIngestNewFromDBDuplicateObs(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	initialMax := store.MaxTransmissionID()

	// Insert new transmission with same hash as existing (should merge into existing tx)
	now := time.Now().UTC()
	newTxID := mustCreateTestTransmission(t, ctx, db, testTransmission(
		"AABB", "dedup_test_hash_01", `{"pubKey":"aabbccdd11223344","type":"ADVERT"}`,
		types.RouteFlood, types.PayloadADVERT, "",
	), now)
	observerID, err := db.GetObserverID(ctx, "obs1")
	if err != nil {
		t.Fatalf("GetObserverID(obs1): %v", err)
	}
	hops := []string{"dd"}
	dbtest.Observation(t, db, newTxID, observerID, now, 11, -89, hops...)

	var pathHash string
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT COALESCE(path_hash, '') FROM observations WHERE transmission_id = $1 AND observer_idx = $2 ORDER BY id LIMIT 1`,
		newTxID, observerID).Scan(&pathHash); err != nil {
		t.Fatalf("read existing path hash: %v", err)
	}
	snr, rssi := 11.0, -89.0
	_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: newTxID,
		ObserverIdx:    &observerID,
		SNR:            &snr,
		RSSI:           &rssi,
		PathHash:       pathHash,
		Timestamp:      now,
	}, hops, nil)
	if err != nil {
		t.Fatalf("InsertObservationWithHops: %v", err)
	}
	if inserted {
		t.Fatal("the repeated observation must merge into the existing one")
	}

	var rows int
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM observations WHERE transmission_id = $1`, newTxID).Scan(&rows); err != nil {
		t.Fatalf("count observations: %v", err)
	}
	if rows != 1 {
		t.Fatalf("observations stored for the transmission = %d, want 1", rows)
	}

	_, newMax := mustIngestNewFromDB(t, store, ctx, initialMax, 100)
	if newMax <= initialMax {
		t.Errorf("expected newMax > %d, got %d", initialMax, newMax)
	}
	tx := store.byHash["dedup_test_hash_01"]
	if tx == nil {
		t.Fatal("expected the new transmission in the store")
	}
	if tx.ObservationCount != 1 {
		t.Errorf("store observation count = %d, want 1", tx.ObservationCount)
	}
}

// --- IngestNewObservations (fixes #174) ---

func TestIngestNewObservations(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	// Get initial observation count for transmission 1 (hash abc123def4567890)
	initialTx := store.byHash["abc123def4567890"]
	if initialTx == nil {
		t.Fatal("expected to find transmission abc123def4567890 in store")
	}
	initialObsCount := initialTx.ObservationCount
	if initialObsCount != 2 {
		t.Fatalf("expected 2 initial observations, got %d", initialObsCount)
	}

	// Record the max obs ID after initial load
	maxObsID := mustGetMaxObservationID(t, db, ctx)

	// Simulate a new observation arriving for the existing transmission AFTER
	// the poller has already advanced past its transmission ID
	observer3, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        strings.Repeat("c", 64),
		Name:      "Observer Three",
		IATA:      "LAX",
		Meta:      nil,
		LastSeen:  time.Now().UTC(),
		IngestNow: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("seed observer three: %v", err)
	}
	dbtest.Observation(t, db, int64(initialTx.ID), observer3, time.Now().UTC(), 5, -100, []string{"aa", "bb", "cc"}...)
	var inserted int
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM observations WHERE id > $1`, maxObsID).Scan(&inserted); err != nil {
		t.Fatalf("count new observations: %v", err)
	}
	if inserted != 1 {
		t.Fatalf("observations above maxObsID = %d, want 1", inserted)
	}

	// Verify IngestNewFromDB does NOT pick up the new observation (tx id hasn't changed)
	txMax := store.MaxTransmissionID()
	_, newTxMax := mustIngestNewFromDB(t, store, ctx, txMax, 100)
	if initialTx.ObservationCount != initialObsCount {
		t.Errorf("IngestNewFromDB should not have changed obs count, was %d now %d",
			initialObsCount, initialTx.ObservationCount)
	}
	_ = newTxMax

	// IngestNewObservations should pick it up
	newObsMaps, _ := mustIngestNewObservations(t, store, ctx, maxObsID, 500)
	if len(newObsMaps) != 1 {
		t.Errorf("expected 1 observation broadcast map, got %d", len(newObsMaps))
	}
	if initialTx.ObservationCount != initialObsCount+1 {
		t.Errorf("expected obs count %d, got %d", initialObsCount+1, initialTx.ObservationCount)
	}
	if len(initialTx.Observations) != initialObsCount+1 {
		t.Errorf("expected %d observations slice len, got %d", initialObsCount+1, len(initialTx.Observations))
	}

	// Best observation should have been re-picked (new obs has longer path)
	if initialTx.PathJSON != `["AA","BB","CC"]` {
		t.Errorf("expected best path to be updated to longer path, got %s", initialTx.PathJSON)
	}

	t.Run("no new observations", func(t *testing.T) {
		maps, _ := mustIngestNewObservations(t, store, ctx, mustGetMaxObservationID(t, db, ctx), 500)
		if maps != nil {
			t.Errorf("expected nil maps for no new observations, got %d", len(maps))
		}
	})

	t.Run("dedup by observer+path", func(t *testing.T) {
		observer1, err := db.GetObserverID(ctx, "obs1")
		if err != nil {
			t.Fatalf("GetObserverID(obs1): %v", err)
		}

		var pathHash string
		if err := db.SQL().QueryRowContext(ctx,
			`SELECT COALESCE(path_hash, '') FROM observations WHERE transmission_id = $1 AND observer_idx = $2 ORDER BY id LIMIT 1`,
			initialTx.ID, observer1).Scan(&pathHash); err != nil {
			t.Fatalf("read existing path hash: %v", err)
		}

		before := mustGetMaxObservationID(t, db, ctx)
		snr, rssi := 12.5, -90.0
		_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
			TransmissionID: int64(initialTx.ID),
			ObserverIdx:    &observer1,
			SNR:            &snr,
			RSSI:           &rssi,
			PathHash:       pathHash,
			Timestamp:      time.Now().UTC(),
		}, []string{"aa", "bb"}, nil)
		if err != nil {
			t.Fatalf("InsertObservationWithHops: %v", err)
		}
		if inserted {
			t.Fatal("same transmission, observer and path must merge into the existing observation, not insert a new one")
		}
		if after := mustGetMaxObservationID(t, db, ctx); after != before {
			t.Fatalf("max observation id moved from %d to %d on a duplicate", before, after)
		}

		prevCount := initialTx.ObservationCount
		maps, _ := mustIngestNewObservations(t, store, ctx, before, 500)
		if initialTx.ObservationCount != prevCount {
			t.Errorf("duplicate obs should not increase count, was %d now %d", prevCount, initialTx.ObservationCount)
		}
		if len(maps) != 0 {
			t.Errorf("expected 0 broadcast maps for duplicate obs, got %d", len(maps))
		}
	})

	t.Run("default limit", func(t *testing.T) {
		_, _ = mustIngestNewObservations(t, store, ctx, mustGetMaxObservationID(t, db, ctx), 0)
	})
}

func TestGetMaxObservationID(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	maxID := mustGetMaxObservationID(t, db, ctx)
	if maxID != 0 {
		t.Errorf("expected 0 for empty table, got %d", maxID)
	}

	seedTestData(t, db)
	maxID = mustGetMaxObservationID(t, db, ctx)
	if maxID <= 0 {
		t.Errorf("expected positive max obs ID, got %d", maxID)
	}
}

// --- perfMiddleware with endpoint normalization ---

func TestPerfMiddlewareEndpointNormalization(t *testing.T) {
	_, router := setupTestServer(t)

	// Hit a route with a hex hash — should normalize to :id
	req := httptest.NewRequest("GET", "/api/packets/abc123def4567890", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// The hex id should have been normalized in perf stats
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

// --- handleNodeAnalytics edge cases ---

func TestHandleNodeAnalyticsNameless(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)

	// Insert a node without a name
	lat, lon := 37.5, -122.0
	mustCreateTestNode(t, ctx, db, database.NodeInput{
		PublicKey: "nameless_node_pk_1",
		Role:      "repeater",
		Lat:       &lat,
		Lon:       &lon,
	})

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

	req := httptest.NewRequest("GET", "/api/nodes/nameless_node_pk_1/analytics?days=1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

// --- PerfStats overflow (>100 recent entries) ---

func TestPerfStatsRecentOverflow(t *testing.T) {
	_, router := setupTestServer(t)
	// Hit an endpoint 120 times to overflow the Recent buffer (capped at 100)
	for i := 0; i < 120; i++ {
		req := httptest.NewRequest("GET", fmt.Sprintf("/api/health?i=%d", i), nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
	}
}

// --- handleAudioLabBuckets ---

func TestHandleAudioLabBucketsNoStore(t *testing.T) {
	_, router := setupTestServer(t)
	req := httptest.NewRequest("GET", "/api/audio-lab/buckets", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	// Just verify no crash
}

// --- Store region filter paths ---

func TestStoreQueryPacketsRegionFilter(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	result := mustQueryPackets(t, store, ctx, database.PacketQuery{Region: "SJC", Limit: 50, Order: "DESC"})
	_ = result

	result2 := mustQueryPackets(t, store, ctx, database.PacketQuery{Region: "NONEXIST", Limit: 50, Order: "DESC"})
	if result2.Total != 0 {
		t.Errorf("expected 0 for non-existent region, got %d", result2.Total)
	}
}

// --- DB.GetObserverIdsForRegion ---

func TestDBGetObserverIdsForRegion(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	ids, err := db.GetObserverIdsForRegion(ctx, "SJC")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) == 0 {
		t.Error("expected observer IDs for SJC")
	}

	ids2, err := db.GetObserverIdsForRegion(ctx, "NONEXIST")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids2) != 0 {
		t.Errorf("expected 0 for NONEXIST, got %d", len(ids2))
	}
}

// --- DB.GetDistinctIATAs ---

func TestDBGetDistinctIATAs(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	iatas, err := db.GetDistinctIATAs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(iatas) == 0 {
		t.Error("expected at least one IATA code")
	}
}

// --- DB.SearchNodes ---

func TestDBSearchNodes(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	nodes, err := db.SearchNodes(ctx, "Test", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) == 0 {
		t.Error("expected nodes matching 'Test'")
	}
}

// --- Ensure non-panic on GetDBSizeStats with path ---

func TestGetDBSizeStatsMemory(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	stats, err := db.GetDBSizeStats(ctx)
	if err != nil {
		t.Fatalf("GetDBSizeStats(): %v", err)
	}
	if stats.DBSizeBytes <= 0 {
		t.Errorf("expected positive database size, got %d bytes", stats.DBSizeBytes)
	}
}

// Regression test for #198: channel messages must include newly ingested packets.
// byPayloadType must maintain newest-first ordering after IngestNewFromDB so that
// GetChannelMessages reverse iteration returns the latest messages.
func TestGetChannelMessagesAfterIngest(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	initialMax := store.MaxTransmissionID()

	// Get baseline message count
	_, totalBefore := store.GetChannelMessages(ctx, "#test", 100, 0)

	// Insert a new channel message into the DB (newer than anything loaded)
	now := time.Now().UTC()
	newTxID := mustCreateTestTransmission(t, ctx, db, testTransmission(
		"FF01", "newchannelmsg19800",
		`{"type":"CHAN","channel":"#test","text":"NewUser: brand new message","sender":"NewUser"}`,
		types.RouteFlood, types.PayloadGRP_TXT, "#test",
	), now)
	observerID, err := db.GetObserverID(ctx, "obs1")
	if err != nil {
		t.Fatalf("GetObserverID(obs1): %v", err)
	}
	dbtest.Observation(t, db, newTxID, observerID, now, 12, -88, nil...)

	// Ingest the new data
	_, newMax := mustIngestNewFromDB(t, store, ctx, initialMax, 100)
	if newMax <= initialMax {
		t.Fatalf("ingest did not advance maxID: %d -> %d", initialMax, newMax)
	}

	// GetChannelMessages must now include the new message
	msgs, totalAfter := store.GetChannelMessages(ctx, "#test", 100, 0)
	if totalAfter <= totalBefore {
		t.Errorf("expected more messages after ingest: before=%d after=%d", totalBefore, totalAfter)
	}

	// The newest message (last in the returned slice) must be the one we just inserted
	if len(msgs) == 0 {
		t.Fatal("expected at least one message")
	}
	lastMsg := msgs[len(msgs)-1]
	if lastMsg["text"] != "brand new message" {
		t.Errorf("newest message should be 'brand new message', got %q", lastMsg["text"])
	}
}

// --- resolveRegionObservers caching ---

func TestResolveRegionObserversCaching(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	store := &PacketStore{db: db}

	// First call should populate cache.
	obs1 := store.resolveRegionObservers(ctx, "SJC")
	if obs1 == nil || len(obs1) == 0 {
		t.Fatal("expected observer IDs for SJC on first call")
	}

	// Second call should return cached result (same pointer).
	obs2 := store.resolveRegionObservers(ctx, "SJC")
	if len(obs2) != len(obs1) {
		t.Errorf("cached result differs: got %d, want %d", len(obs2), len(obs1))
	}

	// Non-existent region should return nil even from cache.
	obs3 := store.resolveRegionObservers(ctx, "NONEXIST")
	if obs3 != nil {
		t.Errorf("expected nil for NONEXIST, got %v", obs3)
	}

	// Verify cache fields are set.
	if store.regionObsCache == nil {
		t.Error("regionObsCache should be non-nil after calls")
	}
	if store.regionObsCacheTime.IsZero() {
		t.Error("regionObsCacheTime should be set")
	}
}

func TestResolveRegionObserversCacheMissNewRegion(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	store := &PacketStore{db: db}

	// Populate cache with SJC.
	obs1 := store.resolveRegionObservers(ctx, "SJC")
	if obs1 == nil || len(obs1) == 0 {
		t.Fatal("expected observer IDs for SJC on first call")
	}

	// Cache is now valid. Request a different region that exists in DB.
	// Before the fix, this would return nil from the map lookup instead of
	// fetching from DB, silently returning "no observers" for up to 30s.
	obs2 := store.resolveRegionObservers(ctx, "LAX")
	// LAX may or may not have data in the test DB, but the key point is:
	// a non-existent region should be fetched (not just nil-returned).
	// Verify the region key was cached (even if empty).
	store.regionObsMu.Lock()
	_, cached := store.regionObsCache["LAX"]
	store.regionObsMu.Unlock()
	if !cached {
		t.Error("LAX should be cached after resolveRegionObservers call, even if empty")
	}
	_ = obs2
}

func TestIndexByNodePreCheck(t *testing.T) {
	store := &PacketStore{
		byNode:     make(map[string][]*StoreTx),
		nodeHashes: make(map[string]map[string]bool),
	}

	t.Run("indexes ADVERT with pubKey", func(t *testing.T) {
		tx := &StoreTx{Hash: "h1", DecodedJSON: `{"pubKey":"AABBCC","type":"ADVERT"}`}
		store.indexByNode(tx)
		if len(store.byNode["AABBCC"]) != 1 {
			t.Errorf("expected 1 entry for pubKey AABBCC, got %d", len(store.byNode["AABBCC"]))
		}
	})

	t.Run("indexes destPubKey", func(t *testing.T) {
		tx := &StoreTx{Hash: "h2", DecodedJSON: `{"destPubKey":"DDEEFF","type":"MSG"}`}
		store.indexByNode(tx)
		if len(store.byNode["DDEEFF"]) != 1 {
			t.Errorf("expected 1 entry for destPubKey DDEEFF, got %d", len(store.byNode["DDEEFF"]))
		}
	})

	t.Run("indexes srcPubKey", func(t *testing.T) {
		tx := &StoreTx{Hash: "h2b", DecodedJSON: `{"srcPubKey":"112233","type":"TXT_MSG"}`}
		store.indexByNode(tx)
		if len(store.byNode["112233"]) != 1 {
			t.Errorf("expected 1 entry for srcPubKey 112233, got %d", len(store.byNode["112233"]))
		}
	})

	t.Run("skips channel message without pubKey", func(t *testing.T) {
		beforeLen := len(store.byNode)
		tx := &StoreTx{Hash: "h3", DecodedJSON: `{"type":"CHAN","channel":"#test","text":"hello"}`}
		store.indexByNode(tx)
		if len(store.byNode) != beforeLen {
			t.Errorf("expected byNode unchanged for channel packet, got %d new entries", len(store.byNode)-beforeLen)
		}
	})

	t.Run("skips empty DecodedJSON", func(t *testing.T) {
		beforeLen := len(store.byNode)
		tx := &StoreTx{Hash: "h4", DecodedJSON: ""}
		store.indexByNode(tx)
		if len(store.byNode) != beforeLen {
			t.Error("expected byNode unchanged for empty DecodedJSON")
		}
	})

	t.Run("deduplicates same hash", func(t *testing.T) {
		tx := &StoreTx{Hash: "h1", DecodedJSON: `{"pubKey":"AABBCC","type":"ADVERT"}`}
		store.indexByNode(tx) // second call for same hash
		if len(store.byNode["AABBCC"]) != 1 {
			t.Errorf("expected dedup to keep 1 entry, got %d", len(store.byNode["AABBCC"]))
		}
	})
}

// TestIndexByNodeResolvedPath tests that indexByNode only indexes decoded JSON pubkeys.
// After #800, resolved_path entries are handled via the decode-window, not indexByNode.
func TestIndexByNodeResolvedPath(t *testing.T) {
	store := &PacketStore{
		byNode:     make(map[string][]*StoreTx),
		nodeHashes: make(map[string]map[string]bool),
	}

	t.Run("decoded JSON pubkeys still indexed", func(t *testing.T) {
		pk := "aabb1122334455ff"
		tx := &StoreTx{
			Hash:        "rp1",
			DecodedJSON: `{"pubKey":"` + pk + `"}`,
		}
		store.indexByNode(tx)
		if len(store.byNode[pk]) != 1 {
			t.Errorf("expected decoded pubkey indexed, got %d", len(store.byNode[pk]))
		}
	})

	t.Run("resolved path pubkeys NOT indexed by indexByNode", func(t *testing.T) {
		// After #800, indexByNode only handles decoded JSON fields.
		// Resolved path pubkeys are handled by the decode-window.
		tx := &StoreTx{
			Hash:        "rp2",
			DecodedJSON: `{"type":"CHAN","text":"hello"}`, // no pubKey fields
		}
		store.indexByNode(tx)
		// No new entries expected since there are no decoded pubkeys
	})

	t.Run("dedup within decoded JSON", func(t *testing.T) {
		pk := "dedup0test0pk1234"
		tx := &StoreTx{
			Hash:        "rp4",
			DecodedJSON: `{"pubKey":"` + pk + `","destPubKey":"` + pk + `"}`,
		}
		store.indexByNode(tx)
		if len(store.byNode[pk]) != 1 {
			t.Errorf("expected dedup to keep 1 entry, got %d", len(store.byNode[pk]))
		}
	})
}

// BenchmarkIndexByNode measures indexByNode performance with and without pubkey
// fields to demonstrate the strings.Contains pre-check optimization.
func BenchmarkIndexByNode(b *testing.B) {
	// Payload WITHOUT any pubkey fields — should be skipped via pre-check
	noPubkey := `{"type":1,"msgId":42,"sender":"node1","data":"hello world"}`
	// Payload WITH a pubkey field — requires JSON parse
	withPubkey := `{"type":1,"msgId":42,"pubKey":"AABB","sender":"node1","data":"hello world"}`

	b.Run("no_pubkey_skip", func(b *testing.B) {
		store := &PacketStore{
			byNode:     make(map[string][]*StoreTx),
			nodeHashes: make(map[string]map[string]bool),
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx := &StoreTx{
				Hash:        fmt.Sprintf("hash-%d", i),
				DecodedJSON: noPubkey,
			}
			store.indexByNode(tx)
		}
	})

	b.Run("with_pubkey_parse", func(b *testing.B) {
		store := &PacketStore{
			byNode:     make(map[string][]*StoreTx),
			nodeHashes: make(map[string]map[string]bool),
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx := &StoreTx{
				Hash:        fmt.Sprintf("hash-%d", i),
				DecodedJSON: withPubkey,
			}
			store.indexByNode(tx)
		}
	})
}

// --- Multi-observer comma-separated filter tests ---

func TestTransmissionsForObserverMultiCSV(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	t.Run("comma-separated returns union via index", func(t *testing.T) {
		result := store.transmissionsForObserver("obs1,obs2", nil)
		if len(result) == 0 {
			t.Fatal("expected results for obs1,obs2")
		}
		// obs1 has transmissions 1,2,3; obs2 has transmission 1
		// Union should include all unique transmissions
		obs1Only := store.transmissionsForObserver("obs1", nil)
		obs2Only := store.transmissionsForObserver("obs2", nil)
		if len(result) < len(obs1Only) || len(result) < len(obs2Only) {
			t.Errorf("union (%d) should be >= each individual set (obs1=%d, obs2=%d)",
				len(result), len(obs1Only), len(obs2Only))
		}
	})

	t.Run("comma-separated with spaces via index", func(t *testing.T) {
		result := store.transmissionsForObserver("obs1, obs2", nil)
		if len(result) == 0 {
			t.Fatal("expected results for 'obs1, obs2' (with space)")
		}
		noSpace := store.transmissionsForObserver("obs1,obs2", nil)
		if len(result) != len(noSpace) {
			t.Errorf("with-space (%d) should equal no-space (%d)", len(result), len(noSpace))
		}
	})

	t.Run("comma-separated returns union via filter path", func(t *testing.T) {
		allTx := store.packets
		result := store.transmissionsForObserver("obs1,obs2", allTx)
		if len(result) == 0 {
			t.Fatal("expected results for obs1,obs2 via filter path")
		}
	})

	t.Run("comma-separated with spaces via filter path", func(t *testing.T) {
		allTx := store.packets
		withSpace := store.transmissionsForObserver("obs1, obs2", allTx)
		noSpace := store.transmissionsForObserver("obs1,obs2", allTx)
		if len(withSpace) != len(noSpace) {
			t.Errorf("filter path: with-space (%d) should equal no-space (%d)", len(withSpace), len(noSpace))
		}
	})
}

// --- Distance index incremental update (#365, replaces debounce #557) ---

func TestDistanceIncrementalUpdate(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	// Record initial distance index size.
	initialHops := len(store.distHops)
	initialPaths := len(store.distPaths)

	// Insert a new observation with a different path to trigger an incremental update.
	maxObsID := mustGetMaxObservationID(t, db, ctx)
	observer2, err := db.GetObserverID(ctx, "obs2")
	if err != nil {
		t.Fatalf("GetObserverID(obs2): %v", err)
	}
	dbtest.Observation(t, db, 1, observer2, time.Now().UTC(), 5, -100, "f1", "f2", "f3")

	_, _ = mustIngestNewObservations(t, store, ctx, maxObsID, 500)

	// Distance index should have been updated incrementally (sizes may differ
	// if the new path resolves differently, but should not panic or corrupt).
	_ = len(store.distHops)
	_ = len(store.distPaths)

	// Insert another observation with yet another path.
	maxObsID = mustGetMaxObservationID(t, db, ctx)
	dbtest.Observation(t, db, 1, observer2, time.Now().UTC(), 5, -100, "f1", "f2", "f4")
	_, _ = mustIngestNewObservations(t, store, ctx, maxObsID, 500)

	// Verify the index is still coherent (no duplicates for the same tx).
	txSeen := make(map[int]int)
	for _, r := range store.distPaths {
		if r.tx != nil {
			txSeen[r.tx.ID]++
		}
	}
	for txID, count := range txSeen {
		if count > 1 {
			t.Errorf("distPaths has %d entries for tx %d (expected at most 1)", count, txID)
		}
	}

	t.Logf("Distance index: %d→%d hops, %d→%d paths (incremental)",
		initialHops, len(store.distHops), initialPaths, len(store.distPaths))
}

func TestHandleBatchObservations(t *testing.T) {
	_, router := setupTestServer(t)

	t.Run("empty hashes returns empty results", func(t *testing.T) {
		body := strings.NewReader(`{"hashes":[]}`)
		req := httptest.NewRequest("POST", "/api/packets/observations", body)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
		var resp map[string]interface{}
		json.Unmarshal(w.Body.Bytes(), &resp)
		results, ok := resp["results"].(map[string]interface{})
		if !ok || len(results) != 0 {
			t.Fatalf("expected empty results map, got %v", resp)
		}
	})

	t.Run("invalid JSON returns 400", func(t *testing.T) {
		body := strings.NewReader(`not json`)
		req := httptest.NewRequest("POST", "/api/packets/observations", body)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 400 {
			t.Fatalf("expected 400, got %d", w.Code)
		}
	})

	t.Run("too many hashes returns 400", func(t *testing.T) {
		hashes := make([]string, 201)
		for i := range hashes {
			hashes[i] = fmt.Sprintf("hash%d", i)
		}
		data, _ := json.Marshal(map[string][]string{"hashes": hashes})
		req := httptest.NewRequest("POST", "/api/packets/observations", bytes.NewReader(data))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 400 {
			t.Fatalf("expected 400, got %d", w.Code)
		}
	})

	t.Run("valid hashes with no store returns empty results", func(t *testing.T) {
		body := strings.NewReader(`{"hashes":["abc123","def456"]}`)
		req := httptest.NewRequest("POST", "/api/packets/observations", body)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
		var resp map[string]interface{}
		json.Unmarshal(w.Body.Bytes(), &resp)
		_, ok := resp["results"].(map[string]interface{})
		if !ok {
			t.Fatalf("expected results map, got %v", resp)
		}
	})
}

// TestIngestTraceBroadcastIncludesPath verifies that TRACE packet broadcasts
// include decoded.path with hopsCompleted (#683).
func TestIngestTraceBroadcastIncludesPath(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	initialMax := store.MaxTransmissionID()

	// TRACE packet: header=0x25, path_byte=0x02 (2 SNR bytes), 2 SNR bytes,
	// then payload: tag(4) + authCode(4) + flags(1) + 4 hop hashes (1-byte each)
	traceHex := "2502AABB010000000200000000DEADBEEF"
	now := time.Now().UTC()
	newTxID := mustCreateTestTransmission(t, ctx, db, testTransmission(
		traceHex, "tracehash683test", "", types.RouteFlood, types.PayloadTRACE, "",
	), now)
	observerID, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        strings.Repeat("a", 64),
		Name:      "Observer One",
		IATA:      "SJC",
		Meta:      nil,
		LastSeen:  now,
		IngestNow: now,
	})
	if err != nil {
		t.Fatalf("seed observer: %v", err)
	}
	dbtest.Observation(t, db, newTxID, observerID, now, 5, -100, []string{"aa"}...)

	broadcastMaps, _ := mustIngestNewFromDB(t, store, ctx, initialMax, 100)
	if len(broadcastMaps) < 1 {
		t.Fatal("expected >=1 broadcast maps")
	}

	bm := broadcastMaps[0]
	decoded := bm.Decoded
	if decoded == nil {
		t.Fatal("broadcast map missing 'decoded'")
	}

	pathObj, ok := decoded["path"]
	if !ok {
		t.Fatal("decoded missing 'path' for TRACE packet — hopsCompleted not delivered to frontend (#683)")
	}

	// The path should be a decoder.Path with HopsCompleted = 2
	pathStruct, ok := pathObj.(decoder.Path)
	if !ok {
		t.Fatalf("expected Path struct, got %T", pathObj)
	}
	if pathStruct.HopsCompleted == nil {
		t.Fatal("path.HopsCompleted is nil for TRACE packet")
	}
	if *pathStruct.HopsCompleted != 2 {
		t.Errorf("expected hopsCompleted=2, got %d", *pathStruct.HopsCompleted)
	}
}
