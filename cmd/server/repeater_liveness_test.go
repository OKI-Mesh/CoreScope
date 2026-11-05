package main

import (
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// TestRepeaterRelayActivity_Active verifies that a repeater whose pubkey
// appears as a relay hop in a recent (non-advert) packet is reported with
// a non-zero lastRelayed timestamp and relayActive=true.
func TestRepeaterRelayActivity_Active(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	pubkey := "aabbccdd" + strings.Repeat("1", 56)
	observerKey := scopeKey(50)

	if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: pubkey,
		Name:      "RepActive",
		Role:      "repeater",
		Lat:       nil,
		Lon:       nil,
		RxTime:    now.Add(-1 * time.Hour),
		IngestNow: now.Add(-1 * time.Hour),
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	obsIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        observerKey,
		Name:      "Relay Observer",
		IATA:      "SFO",
		Meta:      nil,
		LastSeen:  now,
		IngestNow: now,
	})
	if err != nil {
		t.Fatalf("seed observer: %v", err)
	}

	relayedAt := now.Add(-2 * time.Hour)
	txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		Hash:        "a1e1a00000000001",
		RawHex:      "0100",
		RouteType:   1,
		PayloadType: 1,
		DecodedJSON: "{}",
	}, relayedAt)
	if err != nil {
		t.Fatalf("seed transmission: %v", err)
	}
	_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: txID,
		ObserverIdx:    iptr64(obsIdx),
		SNR:            fptr(5.0),
		RSSI:           fptr(-90.0),
		Timestamp:      relayedAt,
	}, []string{"aa"}, []*string{sptr(pubkey)})
	if err != nil {
		t.Fatalf("seed observation: %v", err)
	}
	if !inserted {
		t.Fatalf("expected observation to be inserted for tx %d", txID)
	}

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache(): %v", err)
	}

	info := store.GetRepeaterRelayInfo(pubkey, 24)
	if info.LastRelayed.IsZero() {
		t.Fatalf("expected non-empty LastRelayed for active relayer, got empty (RelayActive=%v)", info.RelayActive)
	}
	if !info.RelayActive {
		t.Errorf("expected RelayActive=true within 24h window, got false (LastRelayed=%s)", info.LastRelayed)
	}
	if info.RelayCount1h != 0 {
		t.Errorf("expected RelayCount1h=0 (relay was 2h ago, outside 1h window), got %d", info.RelayCount1h)
	}
	if info.RelayCount24h != 1 {
		t.Errorf("expected RelayCount24h=1 (relay was 2h ago, inside 24h window), got %d", info.RelayCount24h)
	}
}

// seedUnscopedRelayFixture builds a store in which pubkey appears as a relay
// hop on one FLOOD (unscoped) and one DIRECT tx - the shared fixture for the
// per-node and bulk UnscopedRelayCount24h tests, so the seeding pattern cannot
// drift between the two. hashPrefix keeps the packet hashes distinguishable.
func seedUnscopedRelayFixture(t *testing.T) (*PacketStore, string) {
	t.Helper()
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	pubkey := "aabbccdd" + strings.Repeat("1", 56)

	if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: pubkey,
		Name:      "RepUnscoped",
		Role:      "repeater",
		Lat:       nil,
		Lon:       nil,
		RxTime:    now.Add(-1 * time.Hour),
		IngestNow: now.Add(-1 * time.Hour),
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}
	obsIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        scopeKey(52),
		Name:      "Unscoped Observer",
		IATA:      "SFO",
		Meta:      nil,
		LastSeen:  now,
		IngestNow: now,
	})
	if err != nil {
		t.Fatalf("seed observer: %v", err)
	}

	relayedAt := now.Add(-2 * time.Hour)
	for _, p := range []struct {
		hash      string
		routeType types.RouteType
	}{
		{"a2e1000000000001", routeTypeFlood},
		{"a2e1000000000002", 2},
	} {
		txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
			Hash:        p.hash,
			RawHex:      "0100",
			RouteType:   p.routeType,
			PayloadType: 1,
			DecodedJSON: "{}",
		}, relayedAt)
		if err != nil {
			t.Fatalf("seed transmission %s: %v", p.hash, err)
		}
		_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
			TransmissionID: txID,
			ObserverIdx:    iptr64(obsIdx),
			SNR:            fptr(5.0),
			RSSI:           fptr(-90.0),
			Timestamp:      relayedAt,
		}, []string{"aa"}, []*string{sptr(pubkey)})
		if err != nil {
			t.Fatalf("seed observation %s: %v", p.hash, err)
		}
		if !inserted {
			t.Fatalf("expected observation to be inserted for tx %d", txID)
		}
	}

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache(): %v", err)
	}
	return store, pubkey
}

// assertUnscopedCounts pins the contract both lookups share: FLOOD hops count
// as unscoped, DIRECT hops only as plain relays.
func assertUnscopedCounts(t *testing.T, info RepeaterRelayInfo) {
	t.Helper()
	if info.RelayCount24h != 2 {
		t.Errorf("expected RelayCount24h=2 (both hops), got %d", info.RelayCount24h)
	}
	if info.UnscopedRelayCount24h != 1 {
		t.Errorf("expected UnscopedRelayCount24h=1 (only the FLOOD hop), got %d", info.UnscopedRelayCount24h)
	}
}

// TestRepeaterUnscopedRelayCount verifies that UnscopedRelayCount24h counts only
// route_type==FLOOD (unscoped) relay hops, as a subset of RelayCount24h.
func TestRepeaterUnscopedRelayCount(t *testing.T) {
	store, pubkey := seedUnscopedRelayFixture(t)
	assertUnscopedCounts(t, store.GetRepeaterRelayInfo(pubkey, 24))
}

// TestRepeaterRelayActivity_Idle verifies that a repeater whose pubkey
// has not appeared as a relay hop reports an empty LastRelayed and
// relayActive=false.
func TestRepeaterRelayActivity_Idle(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	pubkey := "ccddeeff" + strings.Repeat("5", 56)

	if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: pubkey,
		Name:      "RepIdle",
		Role:      "repeater",
		Lat:       nil,
		Lon:       nil,
		RxTime:    now.Add(-1 * time.Hour),
		IngestNow: now.Add(-1 * time.Hour),
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache(): %v", err)
	}

	info := store.GetRepeaterRelayInfo(pubkey, 24)
	if !info.LastRelayed.IsZero() {
		t.Errorf("expected zero LastRelayed for idle repeater, got %s", info.LastRelayed)
	}
	if info.RelayActive {
		t.Errorf("expected RelayActive=false for idle repeater, got true")
	}
	if info.RelayCount1h != 0 || info.RelayCount24h != 0 {
		t.Errorf("expected zero relay counts for idle repeater, got 1h=%d 24h=%d", info.RelayCount1h, info.RelayCount24h)
	}
}

// TestRepeaterRelayActivity_Stale verifies that a repeater whose only
// relay-hop appearances are older than the configured window reports
// a non-empty LastRelayed but relayActive=false.
func TestRepeaterRelayActivity_Stale(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	pubkey := "11223344" + strings.Repeat("5", 56)

	if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: pubkey,
		Name:      "RepStale",
		Role:      "repeater",
		Lat:       nil,
		Lon:       nil,
		RxTime:    now.Add(-1 * time.Hour),
		IngestNow: now.Add(-1 * time.Hour),
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}
	obsIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        scopeKey(53),
		Name:      "Stale Observer",
		IATA:      "SFO",
		Meta:      nil,
		LastSeen:  now,
		IngestNow: now,
	})
	if err != nil {
		t.Fatalf("seed observer: %v", err)
	}

	staleAt := now.Add(-48 * time.Hour).Truncate(time.Millisecond)
	txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		Hash:        "a3e1000000000001",
		RawHex:      "0100",
		RouteType:   1,
		PayloadType: 1,
		DecodedJSON: "{}",
	}, staleAt)
	if err != nil {
		t.Fatalf("seed transmission: %v", err)
	}
	_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: txID,
		ObserverIdx:    iptr64(obsIdx),
		SNR:            fptr(5.0),
		RSSI:           fptr(-90.0),
		Timestamp:      staleAt,
	}, []string{"11"}, []*string{sptr(pubkey)})
	if err != nil {
		t.Fatalf("seed observation: %v", err)
	}
	if !inserted {
		t.Fatalf("expected observation to be inserted for tx %d", txID)
	}

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache(): %v", err)
	}

	info := store.GetRepeaterRelayInfo(pubkey, 24)
	if want := staleAt; !info.LastRelayed.Equal(want) {
		t.Errorf("expected LastRelayed=%s (stale ts), got %s", want, info.LastRelayed)
	}
	if info.RelayActive {
		t.Errorf("expected RelayActive=false for relay older than window, got true")
	}
	if info.RelayCount1h != 0 || info.RelayCount24h != 0 {
		t.Errorf("expected zero relay counts for stale (>24h) repeater, got 1h=%d 24h=%d", info.RelayCount1h, info.RelayCount24h)
	}
}

// TestRepeaterRelayActivity_IgnoresAdverts verifies that adverts originated
// by the repeater itself (payload_type=4) are NOT counted as relay activity —
// adverts demonstrate liveness, not relaying.
func TestRepeaterRelayActivity_IgnoresAdverts(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	pubkey := "deadbeef" + strings.Repeat("0", 55) + "1"

	if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: pubkey,
		Name:      "RepAdvertOnly",
		Role:      "repeater",
		Lat:       nil,
		Lon:       nil,
		RxTime:    now.Add(-1 * time.Hour),
		IngestNow: now.Add(-1 * time.Hour),
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}
	obsIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        scopeKey(54),
		Name:      "Advert Observer",
		IATA:      "SFO",
		Meta:      nil,
		LastSeen:  now,
		IngestNow: now,
	})
	if err != nil {
		t.Fatalf("seed observer: %v", err)
	}

	advertAt := now.Add(-2 * time.Hour)
	txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		Hash:        "a4e1000000000001",
		RawHex:      "0140de",
		RouteType:   1,
		PayloadType: 4,
		DecodedJSON: "{}",
	}, advertAt)
	if err != nil {
		t.Fatalf("seed transmission: %v", err)
	}
	_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: txID,
		ObserverIdx:    iptr64(obsIdx),
		SNR:            fptr(5.0),
		RSSI:           fptr(-90.0),
		Timestamp:      advertAt,
	}, []string{"de"}, []*string{sptr(pubkey)})
	if err != nil {
		t.Fatalf("seed observation: %v", err)
	}
	if !inserted {
		t.Fatalf("expected observation to be inserted for tx %d", txID)
	}

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache(): %v", err)
	}

	info := store.GetRepeaterRelayInfo(pubkey, 24)
	if !info.LastRelayed.IsZero() {
		t.Errorf("expected zero LastRelayed (adverts ignored), got %s", info.LastRelayed)
	}
	if info.RelayActive {
		t.Errorf("expected RelayActive=false (adverts ignored), got true")
	}
	if info.RelayCount1h != 0 || info.RelayCount24h != 0 {
		t.Errorf("expected zero relay counts (adverts ignored), got 1h=%d 24h=%d", info.RelayCount1h, info.RelayCount24h)
	}
}

// TestRepeaterRelayActivity_PrefixHop verifies that GetRepeaterRelayInfo
// counts a non-advert packet whose path contains only the 1-byte raw hop
// prefix matching the target node (not the full resolved pubkey).
//
// Reality on prod/staging: many ingested packets only carry raw 1-byte
// path hops (e.g. ["a3"] from the wire) — resolution to a full pubkey
// happens later via neighbor affinity for the "Paths seen through node"
// view. The byPathHop index is populated under BOTH keys (raw hop AND
// resolved pubkey), but GetRepeaterRelayInfo only looks up the full
// pubkey, missing all raw-hop-only entries. This is the cause of the
// "never observed as relay hop" claim on nodes that clearly have paths
// shown through them. See https://analyzer-stg.00id.net/#/nodes/<pk>.
func TestRepeaterRelayActivity_PrefixHop(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	pubkey := "a36a21290d9c25a158130fe7c489541210d5f09f25fab997db5e942fb7680510"

	if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: pubkey,
		Name:      "RepPrefix",
		Role:      "repeater",
		Lat:       nil,
		Lon:       nil,
		RxTime:    now.Add(-1 * time.Hour),
		IngestNow: now.Add(-1 * time.Hour),
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}
	obsIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        scopeKey(55),
		Name:      "Prefix Observer",
		IATA:      "SFO",
		Meta:      nil,
		LastSeen:  now,
		IngestNow: now,
	})
	if err != nil {
		t.Fatalf("seed observer: %v", err)
	}

	relayedAt := now.Add(-2 * time.Hour)
	txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		Hash:        "a5e1000000000001",
		RawHex:      "0100",
		RouteType:   1,
		PayloadType: 1,
		DecodedJSON: "{}",
	}, relayedAt)
	if err != nil {
		t.Fatalf("seed transmission: %v", err)
	}
	_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: txID,
		ObserverIdx:    iptr64(obsIdx),
		SNR:            fptr(5.0),
		RSSI:           fptr(-90.0),
		Timestamp:      relayedAt,
	}, []string{"a3"}, []*string{nil})
	if err != nil {
		t.Fatalf("seed observation: %v", err)
	}
	if !inserted {
		t.Fatalf("expected observation to be inserted for tx %d", txID)
	}

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache(): %v", err)
	}

	info := store.GetRepeaterRelayInfo(pubkey, 24)
	if info.RelayCount24h < 1 {
		t.Fatalf("expected RelayCount24h>=1 for node with prefix-matched hop in path, got %d (LastRelayed=%q)",
			info.RelayCount24h, info.LastRelayed)
	}
	if info.LastRelayed.IsZero() {
		t.Errorf("expected non-empty LastRelayed when prefix hop matched, got empty")
	}
	if !info.RelayActive {
		t.Errorf("expected RelayActive=true within 24h window, got false (LastRelayed=%s)", info.LastRelayed)
	}
}

// TestRepeaterRelayActivity_DedupAcrossPrefixAndFullKey verifies that when
// the SAME packet is indexed in byPathHop under BOTH the full pubkey AND
// the raw 1-byte prefix, GetRepeaterRelayInfo counts it exactly once. This
// gates the `seen[tx.ID]` dedup map: without it, hop counts would double
// for any tx that resolved-path indexing recorded under both keys.
func TestRepeaterRelayActivity_DedupAcrossPrefixAndFullKey(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	pubkey := "a36a21290d9c25a158130fe7c489541210d5f09f25fab997db5e942fb7680510"

	if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: pubkey,
		Name:      "RepDedup",
		Role:      "repeater",
		Lat:       nil,
		Lon:       nil,
		RxTime:    now.Add(-1 * time.Hour),
		IngestNow: now.Add(-1 * time.Hour),
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}
	obsIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        scopeKey(56),
		Name:      "Dedup Observer",
		IATA:      "SFO",
		Meta:      nil,
		LastSeen:  now,
		IngestNow: now,
	})
	if err != nil {
		t.Fatalf("seed observer: %v", err)
	}

	relayedAt := now.Add(-2 * time.Hour)
	txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		Hash:        "a6e1000000000001",
		RawHex:      "0100",
		RouteType:   1,
		PayloadType: 1,
		DecodedJSON: "{}",
	}, relayedAt)
	if err != nil {
		t.Fatalf("seed transmission: %v", err)
	}
	_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: txID,
		ObserverIdx:    iptr64(obsIdx),
		SNR:            fptr(5.0),
		RSSI:           fptr(-90.0),
		Timestamp:      relayedAt,
	}, []string{"a3"}, []*string{sptr(pubkey)})
	if err != nil {
		t.Fatalf("seed observation: %v", err)
	}
	if !inserted {
		t.Fatalf("expected observation to be inserted for tx %d", txID)
	}

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache(): %v", err)
	}

	info := store.GetRepeaterRelayInfo(pubkey, 24)
	if info.RelayCount24h != 1 {
		t.Fatalf("expected RelayCount24h=1 (dedup across full+prefix indexing), got %d", info.RelayCount24h)
	}
	if info.RelayCount1h != 0 {
		t.Errorf("expected RelayCount1h=0 (relay was 2h ago, outside 1h window), got %d", info.RelayCount1h)
	}
	if !info.RelayActive {
		t.Errorf("expected RelayActive=true, got false (LastRelayed=%s)", info.LastRelayed)
	}
}

// TestRepeaterUnscopedRelayCount_Bulk verifies the bulk /api/nodes path
// (computeRepeaterRelayInfoMap) counts unscoped floods identically to the
// per-node path: only route_type==FLOOD hops, as a subset of RelayCount24h.
func TestRepeaterUnscopedRelayCount_Bulk(t *testing.T) {
	store, pubkey := seedUnscopedRelayFixture(t)
	assertUnscopedCounts(t, store.computeRepeaterRelayInfoMap(24)[pubkey])
}
