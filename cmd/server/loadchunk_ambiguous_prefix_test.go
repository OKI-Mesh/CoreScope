package main

import (
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
	"github.com/OKI-Mesh/CoreScope/internal/database/pgtest"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// createTestDBAmbiguousPrefix builds a fixture where TWO repeaters share the
// same 2-char hop prefix. An observation's path_json carries ONLY the
// ambiguous prefix (no longer prefix that would disambiguate). With no
// neighbor_edges seeded, the cold-load fallback in scanAndMergeChunk has
// nothing to anchor on — yet the current code resolves the prefix anyway
// (via observation_count_fallback or candidate[0]) and over-attributes the
// hop to ONE of the two repeaters. That is the time-travel bug munger
// flagged: the historical packet's actual relay is unknown, but the loader
// picks today's tier-4 winner against ~7-day-old observations.
func createTestDBAmbiguousPrefix(t *testing.T, relayA, relayB, hop, firstSeen string) string {
	t.Helper()
	dsn := pgtest.NewDatabase(t)
	ctx := t.Context()
	db, err := database.OpenReadWrite(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().UTC()
	observerID := mustCreateTestObserver(t, ctx, db, "obs1", "Obs1", "", now)

	// Two repeaters sharing the same 2-char prefix `hop`.
	// Different advert_counts so tier-4 tiebreak deterministically picks one
	// (proving the bug: it over-attributes to the higher-count node).
	for _, node := range []struct {
		pubkey string
		name   string
		count  int
	}{
		{relayA, "Relay A", 50},
		{relayB, "Relay B", 10},
	} {
		if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
			PublicKey: node.pubkey, Name: node.name, Role: "repeater", RxTime: now, IngestNow: now,
		}); err != nil {
			t.Fatalf("create node %s: %v", node.pubkey, err)
		}
		mustIncrementTestNodeAdvertCount(t, ctx, db, node.pubkey, node.count)
	}

	// Aged 48h so it lands in the background window (loadChunk path).
	firstSeenTime, err := time.Parse(time.RFC3339, firstSeen)
	if err != nil {
		t.Fatalf("parsing firstSeen %q: %v", firstSeen, err)
	}
	txID := mustCreateTestTransmission(t, ctx, db, database.TransmissionInput{
		RawHex: "aa", Hash: "hashamb_1", RouteType: types.RouteFlood,
		PayloadType: types.PayloadADVERT, DecodedJSON: `{}`,
	}, firstSeenTime)
	dbtest.Observation(t, db, txID, observerID, firstSeenTime, -10, -80, []string{hop}...)
	return dsn
}

// TestLoadChunk_AmbiguousPrefix_SkipsAttribution pins the fix for the
// time-travel attribution gate (munger R1 #1). When path_json carries an
// ambiguous prefix that matches multiple repeaters, the cold-load path
// MUST NOT pick a winner via affinity/observation-count tiebreak — today's
// affinity winner is not necessarily the historical hop. Safer to
// under-attribute (skip byNode for that hop) than to mis-attribute.
func TestLoadChunk_AmbiguousPrefix_SkipsAttribution(t *testing.T) {
	ctx := t.Context()
	relayA := "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
	relayB := "aa1122334455667788990011223344556677889900112233445566778899aabb"
	hop := "aa" // 2-char prefix shared by both relayA and relayB

	aged := time.Now().UTC().Add(-48 * time.Hour).Format(time.RFC3339)
	dbPath := createTestDBAmbiguousPrefix(t, relayA, relayB, hop, aged)

	db, err := OpenDB(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewPacketStore(db, &PacketStoreConfig{
		RetentionHours:  72,
		HotStartupHours: 1, // hot load skips the 48h-old row → goes to loadChunk
	})
	// Empty graph: no neighbor-affinity tiebreak signal. Mirrors a freshly
	// restarted server whose only relay info is the prefix map.
	store.graph.Store(NewNeighborGraph())

	if err := store.LoadChunked(ctx, 0); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}
	if got := len(store.byNode[relayA]) + len(store.byNode[relayB]); got != 0 {
		t.Fatalf("setup: hot load unexpectedly picked up 48h-old row "+
			"(byNode total=%d, want 0) — test would not exercise loadChunk", got)
	}

	chunkStart := time.Now().UTC().Add(-72 * time.Hour)
	chunkEnd := time.Now().UTC().Add(-1 * time.Hour)
	if err := store.loadChunk(ctx, chunkStart, chunkEnd); err != nil {
		t.Fatalf("loadChunk: %v", err)
	}

	// Neither repeater may be over-attributed. The hop is ambiguous → the
	// cold-load loader MUST NOT pick one as the byNode owner.
	if got := len(store.byNode[relayA]); got != 0 {
		t.Errorf("byNode[%s]: got %d transmissions, want 0 — ambiguous-prefix hop "+
			"was over-attributed to relayA (time-travel attribution bug)", relayA, got)
	}
	if got := len(store.byNode[relayB]); got != 0 {
		t.Errorf("byNode[%s]: got %d transmissions, want 0 — ambiguous-prefix hop "+
			"was over-attributed to relayB (time-travel attribution bug)", relayB, got)
	}
}

// TestLoad_AmbiguousPrefix_SkipsAttribution covers the hot-window Load()
// path. Same setup as the loadChunk test but the row falls inside the hot
// window so it is loaded by Load() / scanAndMergeChunk.
func TestLoad_AmbiguousPrefix_SkipsAttribution(t *testing.T) {
	ctx := t.Context()
	relayA := "bbccddeeff00112233445566778899aabbccddeeff00112233445566778899aa"
	relayB := "bb112233445566778899001122334455667788990011223344556677889900aa"
	hop := "bb"

	ts := time.Now().UTC().Format(time.RFC3339)
	dbPath := createTestDBAmbiguousPrefix(t, relayA, relayB, hop, ts)

	db, err := OpenDB(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewPacketStore(db, &PacketStoreConfig{RetentionHours: 72})
	store.graph.Store(NewNeighborGraph())

	if err := store.LoadChunked(ctx, 0); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}

	if got := len(store.byNode[relayA]); got != 0 {
		t.Errorf("byNode[%s]: got %d transmissions, want 0 — ambiguous-prefix hop "+
			"was over-attributed (hot Load path)", relayA, got)
	}
	if got := len(store.byNode[relayB]); got != 0 {
		t.Errorf("byNode[%s]: got %d transmissions, want 0 — ambiguous-prefix hop "+
			"was over-attributed (hot Load path)", relayB, got)
	}
}
