package main

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
	"github.com/OKI-Mesh/CoreScope/internal/database/pgtest"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// createTestDBWithResolvedPath creates a fixture DB containing numTx old
// transmissions (48h ago, outside any default hot window) where each
// observation has a non-empty resolved_path JSON listing relay-hop pubkeys.
// Mirrors createTestDBWithAgedPackets shape but adds the resolved_path
// column so loadChunk's hasResolvedPath branch is exercised.
func createTestDBWithResolvedPath(t *testing.T, numTx int, relayPubkeys []string) string {
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
	for _, pubkey := range relayPubkeys {
		if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
			PublicKey: pubkey, Name: pubkey, Role: "repeater", RxTime: now, IngestNow: now,
		}); err != nil {
			t.Fatalf("create relay node: %v", err)
		}
	}

	for i := 0; i < numTx; i++ {
		txTime := now.Add(-48 * time.Hour).Add(time.Duration(i) * time.Second)
		hash := fmt.Sprintf("hash1558_%d", i)
		txID := mustCreateTestTransmission(t, ctx, db, database.TransmissionInput{
			RawHex: "aa", Hash: hash, RouteType: types.RouteFlood,
			PayloadType: types.PayloadADVERT, DecodedJSON: `{}`,
		}, txTime)
		hops := make([]string, len(relayPubkeys))
		resolved := make([]*string, len(relayPubkeys))
		for i, pubkey := range relayPubkeys {
			hops[i] = strings.ToLower(pubkey[:2])
			resolved[i] = &relayPubkeys[i]
		}
		merged := make([]string, len(hops))
		for i, h := range hops {
			merged[i] = h
			if i < len(resolved) && resolved[i] != nil {
				merged[i] = h + ":" + *resolved[i]
			}
		}
		dbtest.Observation(t, db, txID, observerID, txTime, -10, -80, merged...)
	}
	return dsn
}

// TestLoadChunk_IndexesResolvedPathPubkeys_Issue1558 verifies the
// contract-violation fix from #1558:
//
//	`Load` (cmd/server/store.go:783-799) unmarshals each observation's
//	resolved_path column and feeds every relay-hop pubkey through
//	addToByNode / addResolvedPubkeysToPathHopIndex /
//	addToResolvedPubkeyIndex. `loadChunk` (cmd/server/store.go:937-1023)
//	scans the same column into resolvedPathStr but never feeds it
//	anywhere — so background-backfilled transmissions never appear under
//	their relay pubkeys in s.byNode, even though the same exact rows do
//	when they happen to fall inside the hot startup window.
//
// Symptom in production: Home page per-node `packetsToday` /
// `totalTransmissions` / observer counts collapse after a container
// restart for any node that primarily appears as a relay (rather than
// as the endpoint pubKey/destPubKey/srcPubKey of a packet), because the
// background backfill path silently drops the relay-hop indexing
// branch. See issue #1558 for the full trace + diagnosis.
//
// This test loads a fixture DB exclusively via loadChunk (skipping
// Load) and asserts that for each relay pubkey present in
// `resolved_path` of every observation, s.byNode contains the
// transmission.
func TestLoadChunk_IndexesResolvedPathPubkeys_Issue1558(t *testing.T) {
	ctx := t.Context()

	// Two distinct relay pubkeys appear in every observation's resolved_path.
	// Neither is an endpoint pubkey in decoded_json — so the ONLY path
	// they can enter byNode through is the resolved_path branch.
	relayPK1 := "1111111111111111111111111111111111111111111111111111111111111111"
	relayPK2 := "2222222222222222222222222222222222222222222222222222222222222222"

	dbPath := createTestDBWithResolvedPath(t, 3, []string{relayPK1, relayPK2})

	db, err := OpenDB(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewPacketStore(db, &PacketStoreConfig{
		RetentionHours:  72,
		HotStartupHours: 1, // initial Load should NOT pick up 48h-old fixture rows
	})
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	// Confirm the fixture rows are outside the hot window — Load() must
	// not have already populated byNode for the relay pubkeys; otherwise
	// the test would not actually be exercising loadChunk.
	if len(store.byNode[relayPK1]) != 0 {
		t.Fatalf("setup: Load() unexpectedly picked up 48h-old rows; "+
			"byNode[relayPK1]=%d entries (expected 0)", len(store.byNode[relayPK1]))
	}

	// Trigger background backfill of the 48h-old window via loadChunk —
	// this is the code path under test.
	chunkStart := time.Now().UTC().Add(-72 * time.Hour)
	chunkEnd := time.Now().UTC().Add(-1 * time.Hour)
	if err := store.loadChunk(ctx, chunkStart, chunkEnd); err != nil {
		t.Fatalf("loadChunk failed: %v", err)
	}

	// Sanity: loadChunk did merge the transmissions into the slice.
	if len(store.packets) != 3 {
		t.Fatalf("loadChunk should have merged 3 transmissions; got %d", len(store.packets))
	}

	// THE ASSERTION: every relay pubkey listed in resolved_path must be
	// indexed in byNode for every transmission, because loadChunk's
	// per-row scan should mirror Load()'s 783-799 block.
	for _, relayPK := range []string{relayPK1, relayPK2} {
		got := len(store.byNode[relayPK])
		if got != 3 {
			t.Errorf("byNode[%s]: got %d transmissions, want 3 — "+
				"loadChunk dropped the resolved_path indexing branch "+
				"(issue #1558)",
				relayPK, got)
		}
	}
}
