package main

import (
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// createTestDBPathJSONNoResolvedPath builds a fixture that mirrors the LIVE
// deployment state after #1287: observations carry a path_json hop list but
// observations.resolved_path is NULL (the ingestor no longer writes it; relay
// data is persisted as aggregate neighbor_edges instead). A single repeater
// node whose public_key starts with hopPrefix lets the in-memory prefix map
// resolve that hop unambiguously to relayPubkey.
//
// The transmission's decoded_json is empty ({}), so relayPubkey is NOT an
// endpoint (pubKey/destPubKey/srcPubKey). The ONLY way it can enter
// s.byNode is via path_json → resolvePathForObs relay-hop resolution.
func seedPathJSONNoResolvedPath(t *testing.T, db *database.DB, relayPubkey, hopPrefix string, firstSeen time.Time) {
	t.Helper()
	ctx := t.Context()
	now := time.Now().UTC()
	observerID := mustCreateTestObserver(t, ctx, db, "obs1", "Obs1", "", now)

	if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: relayPubkey, Name: "Relay One", Role: "repeater", RxTime: now, IngestNow: now,
	}); err != nil {
		t.Fatalf("create relay node: %v", err)
	}

	txID := mustCreateTestTransmission(t, ctx, db, database.TransmissionInput{
		RawHex: "aa", Hash: "hashpjf_1", RouteType: types.RouteFlood,
		PayloadType: types.PayloadADVERT, DecodedJSON: `{}`,
	}, firstSeen)
	dbtest.Observation(t, db, txID, observerID, firstSeen, -10, -80, []string{hopPrefix}...)
}

// TestLoadChunked_ResolvesRelayHopsFromPathJSON_WhenResolvedPathEmpty pins the
// fix for the "relay-node analytics empty after every restart" bug.
//
// On live, observations.resolved_path is 100% NULL (since #1287 the ingestor
// persists relay data as neighbor_edges, not per-observation resolved_path).
// The cold-load paths (Load / scanAndMergeChunk) indexed relay hops ONLY from
// resolved_path, so a relay node's path-hop attribution was never rebuilt on
// startup — it only re-accumulated from live traffic, collapsing the activity
// timeline to "just the hour the server restarted".
//
// The fix: when resolved_path is empty, fall back to resolving the hops from
// the persisted path_json using the in-memory prefix map + neighbor graph
// (exactly what the live ingest path already does), then index the relay hops.
func TestLoadChunked_ResolvesRelayHopsFromPathJSON_WhenResolvedPathEmpty(t *testing.T) {
	ctx := t.Context()
	relayPK := "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
	hop := "aa"

	db := setupTestDB(t)
	seedPathJSONNoResolvedPath(t, db, relayPK, hop, time.Now().UTC())

	store := newPacketStoreForTest(t, db)
	store.graph.Store(NewNeighborGraph())

	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache: %v", err)
	}
	if err := store.LoadChunked(ctx, 0); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}

	store.mu.RLock()
	got := len(store.byNode[relayPK])
	store.mu.RUnlock()
	if got != 1 {
		t.Errorf("byNode[%s]: got %d transmissions, want 1 — cold load did not "+
			"resolve relay hops from path_json when resolved_path was NULL "+
			"(relay history lost on restart)", relayPK, got)
	}
}

// TestLoadChunk_ResolvesRelayHopsFromPathJSON_WhenResolvedPathEmpty covers the
// background-window loader (loadBackgroundChunks → loadChunk), which on live
// loads everything older than hotStartupHours (24h) up to retentionHours
// (168h). Without the path_json fallback here, a relay node's analytics for
// the older 6 days would still vanish on every restart even with the hot
// window fixed.
func TestLoadChunk_ResolvesRelayHopsFromPathJSON_WhenResolvedPathEmpty(t *testing.T) {
	ctx := t.Context()
	relayPK := "ccddeeff00112233445566778899aabbccddeeff00112233445566778899aabb"
	hop := "cc"

	aged := time.Now().UTC().Add(-48 * time.Hour)

	db := setupTestDB(t)
	seedPathJSONNoResolvedPath(t, db, relayPK, hop, aged)

	store := newPacketStoreForTest(t, db)
	store.hotStartupHours = 1
	store.graph.Store(NewNeighborGraph())

	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache: %v", err)
	}
	if err := store.LoadChunked(ctx, 0); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}

	store.mu.RLock()
	hot := len(store.byNode[relayPK])
	store.mu.RUnlock()
	if hot != 0 {
		t.Fatalf("setup: hot load unexpectedly picked up 48h-old row; "+
			"byNode[relayPK]=%d (want 0) — test would not exercise loadChunk", hot)
	}

	chunkStart := time.Now().UTC().Add(-72 * time.Hour)
	chunkEnd := time.Now().UTC().Add(-1 * time.Hour)
	if err := store.loadChunk(ctx, chunkStart, chunkEnd); err != nil {
		t.Fatalf("loadChunk: %v", err)
	}

	store.mu.RLock()
	got := len(store.byNode[relayPK])
	store.mu.RUnlock()
	if got != 1 {
		t.Errorf("byNode[%s]: got %d transmissions, want 1 — background loadChunk "+
			"did not resolve relay hops from path_json when resolved_path was NULL "+
			"(relay history lost on restart for the older retention window)", relayPK, got)
	}
}
