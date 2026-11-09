package main

import (
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// TestMultiByteCapability_RegionFiltered_PreservesConfirmedStatus verifies
// that GetAnalyticsHashSizes returns a populated multiByteCapability list
// even when a region filter is applied. The frontend (analytics.js) merges
// this into the adopter table to render per-node "confirmed/suspected/unknown"
// badges. When the field is missing or empty under a region filter, every
// row falls back to "unknown" — see meshcore.meshat.se/#/analytics filtered
// by JKG showing 14 "unknown" while the unfiltered view shows 0.
//
// Multi-byte capability is a property of the NODE (advertised hash_size from
// its own adverts), not the observing region. Region filter should affect
// which nodes appear in the result list (multiByteNodes), not their cap status.
//
// Pre-fix behavior: multiByteCapability is only populated when region == "".
// This test fails because result["multiByteCapability"] is absent under
// region="JKG", so the lookup returns nil/false.
func TestMultiByteCapability_RegionFiltered_PreservesConfirmedStatus(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)

	now := time.Now().UTC()
	recentTime := now.Add(-1 * time.Hour)

	// Two observers in different regions.
	observerSJC := mustCreateTestObserver(t, ctx, db, "obs-sjc", "Obs SJC", "SJC", recentTime)
	observerJKG := mustCreateTestObserver(t, ctx, db, "obs-jkg", "Obs JKG", "JKG", recentTime)

	// Node A: a JKG-region repeater that advertises multi-byte (hash_size=2).
	// Its zero-hop direct advert is only heard by obs-SJC (e.g. an out-of-region
	// listener that happens to pick it up). Under the JKG region filter, the
	// computeAnalyticsHashSizes() pass will see a smaller advert dataset, but
	// the node's multi-byte capability is intrinsic and should still resolve
	// to "confirmed" via the global advert evidence.
	pkA := "aaa0000000000001"
	decodedA := `{"pubKey":"` + pkA + `","name":"Node-A","type":"ADVERT","flags":{"isRepeater":true}}`
	nodeA := &database.NodeInput{PublicKey: pkA, Name: "Node-A", Role: "repeater"}

	// Zero-hop direct advert (route_type=2, payload_type=4),
	// pathByte 0x40 → hash_size bits 01 → 2 bytes.
	// Heard by obs-SJC ONLY.
	directID := mustCreateTestTransmission(t, ctx, db, database.TransmissionInput{
		RawHex:      "1240aabbccdd",
		Hash:        "a_zh_direct",
		RouteType:   types.RouteDirect,
		PayloadType: types.PayloadADVERT,
		DecodedJSON: decodedA,
		Advert:      nodeA,
	}, recentTime)
	dbtest.Observation(t, db, directID, observerSJC, recentTime, 12, -85, nil...)

	// Node A also appears as a path hop in a JKG-observed packet, so it
	// shows up in the JKG region's node list.
	// route_type=1 (flood), payload_type=4, pathByte 0x41 (hs=2, hops=1)
	relayID := mustCreateTestTransmission(t, ctx, db, testTransmission(
		"1141aabbccdd", "a_jkg_relay", decodedA, types.RouteFlood, types.PayloadADVERT, "",
	), recentTime)
	resolvedA := pkA
	dbtest.Observation(t, db, relayID, observerJKG, recentTime, 8, -95, "aa:"+resolvedA)
	store := newPacketStoreForTest(t, db)
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache: %v", err)
	}
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	// Sanity: unfiltered view exposes the field.
	unfiltered := store.GetAnalyticsHashSizes(ctx, "", "")
	info := store.GetNodeHashSizeInfo()
	t.Logf("hashInfo[A] = %+v (present=%v)", info[pkA], info[pkA] != nil)
	store.mu.RLock()
	t.Logf("byPayloadType[4] len=%d", len(store.byPayloadType[4]))
	for _, tx := range store.byPayloadType[4] {
		t.Logf("advert %s raw=%q first_seen=%q route=%v decoded_empty=%v", tx.Hash, tx.RawHex, tx.FirstSeen, tx.RouteType, tx.DecodedJSON == "")
	}
	store.mu.RUnlock()
	if len(unfiltered.MultiByteCapability) == 0 {
		t.Fatal("unfiltered result missing multiByteCapability — test setup is wrong")
	}

	// The actual assertion: region-filtered view MUST also expose the field
	// AND must report Node A as "confirmed", not "unknown".
	result := store.GetAnalyticsHashSizes(ctx, "JKG", "")
	caps := result.MultiByteCapability
	if len(caps) == 0 {
		t.Fatal("expected multiByteCapability in region=JKG result")
	}

	var foundA *api.MultiByteCapability
	for i := range caps {
		if caps[i].Pubkey == pkA {
			foundA = &caps[i]
			break
		}
	}
	if foundA == nil {
		t.Fatalf("Node A missing from region=JKG multiByteCapability (have %d entries)", len(caps))
	}
	if foundA.Status != "confirmed" {
		t.Errorf("Node A status under region=JKG = %q, want %q (region filter wrongly downgraded multi-byte capability evidence)", foundA.Status, "confirmed")
	}
}
