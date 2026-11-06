package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// recentTS returns a timestamp string N hours ago, ensuring test data
// stays within the 7-day advert window used by computeNodeHashSizeInfo.
func recentTS(hoursAgo int) time.Time {
	return time.Now().UTC().Add(-time.Duration(hoursAgo) * time.Hour)
}

// addTestPacket adds a StoreTx to the store's internal structures including
// the byPathHop index and byPayloadType index.
func addTestPacket(store *PacketStore, tx *StoreTx) {
	store.mu.Lock()
	defer store.mu.Unlock()
	tx.ID = len(store.packets) + 1
	if tx.Hash == "" {
		tx.Hash = fmt.Sprintf("test-hash-%d", tx.ID)
	}
	store.packets = append(store.packets, tx)
	store.byHash[tx.Hash] = tx
	store.byTxID[tx.ID] = tx
	if tx.PayloadType != nil {
		store.byPayloadType[*tx.PayloadType] = append(store.byPayloadType[*tx.PayloadType], tx)
	}
	addTxToPathHopIndex(store.byPathHop, tx)
}

// buildPathByte returns a 2-char hex string for the path byte with given
// hashSize (1-3) and hopCount.
func buildPathByte(hashSize, hopCount int) string {
	b := byte(((hashSize-1)&0x3)<<6) | byte(hopCount&0x3F)
	return fmt.Sprintf("%02x", b)
}

// makeTestAdvert creates a StoreTx representing a flood advert packet.
func makeTestAdvert(pubkey string, hashSize int) *StoreTx {
	decoded, _ := json.Marshal(map[string]interface{}{"pubKey": pubkey, "name": pubkey[:8]})
	pt := types.PayloadType(4)
	pathByte := buildPathByte(hashSize, 1)
	prefix := strings.ToLower(pubkey[:hashSize*2])
	rawHex := "01" + pathByte + prefix // flood header + path byte + hop prefix
	return &StoreTx{
		RawHex:      rawHex,
		PayloadType: &pt,
		DecodedJSON: string(decoded),
		PathJSON:    `["` + prefix + `"]`,
		FirstSeen:   time.Now().UTC().Add(-24 * time.Hour),
	}
}

// TestMultiByteCapability_Confirmed tests that a repeater advertising
// with hash_size >= 2 is classified as "confirmed".
func TestMultiByteCapability_Confirmed(t *testing.T) {
	db := setupTestDB(t)

	if err := db.CreateOrUpdateNode(t.Context(), database.NodeInput{
		PublicKey: "aabbccdd11223344",
		Name:      "RepA",
		Role:      "repeater",
		RxTime:    recentTS(24),
		IngestNow: recentTS(24),
	}); err != nil {
		t.Fatalf("seed RepA: %v", err)
	}

	store := newPacketStoreForTest(t, db)
	store.RefreshNodeCache(t.Context())
	addTestPacket(store, makeTestAdvert("aabbccdd11223344", 2))

	caps := store.computeMultiByteCapability(nil)
	if len(caps) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(caps))
	}
	if caps[0].Status != "confirmed" {
		t.Errorf("expected confirmed, got %s", caps[0].Status)
	}
	if caps[0].Evidence != "advert" {
		t.Errorf("expected advert evidence, got %s", caps[0].Evidence)
	}
	if caps[0].MaxHashSize != 2 {
		t.Errorf("expected maxHashSize 2, got %d", caps[0].MaxHashSize)
	}
}

// TestMultiByteCapability_Suspected tests that a repeater whose prefix
// appears in a multi-byte path is classified as "suspected".
func TestMultiByteCapability_Suspected(t *testing.T) {
	db := setupTestDB(t)

	if err := db.CreateOrUpdateNode(t.Context(), database.NodeInput{
		PublicKey: "aabbccdd11223344",
		Name:      "RepA",
		Role:      "repeater",
		RxTime:    recentTS(48),
		IngestNow: recentTS(48),
	}); err != nil {
		t.Fatalf("seed RepA: %v", err)
	}

	store := newPacketStoreForTest(t, db)
	store.RefreshNodeCache(t.Context())
	// Non-advert packet with 2-byte hash in path, hop prefix matching node
	pathByte := buildPathByte(2, 1)
	rawHex := "01" + pathByte + "aabb"
	pt := types.PayloadType(1)
	pkt := &StoreTx{
		RawHex:      rawHex,
		PayloadType: &pt,
		PathJSON:    `["aabb"]`,
		FirstSeen:   time.Now().UTC().Add(-48 * time.Hour),
	}
	addTestPacket(store, pkt)

	caps := store.computeMultiByteCapability(nil)
	if len(caps) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(caps))
	}
	if caps[0].Status != "suspected" {
		t.Errorf("expected suspected, got %s", caps[0].Status)
	}
	if caps[0].Evidence != "path" {
		t.Errorf("expected path evidence, got %s", caps[0].Evidence)
	}
	if caps[0].MaxHashSize != 2 {
		t.Errorf("expected maxHashSize 2, got %d", caps[0].MaxHashSize)
	}
}

// TestMultiByteCapability_Unknown tests that a repeater with only 1-byte
// adverts and no multi-byte path appearances is classified as "unknown".
func TestMultiByteCapability_Unknown(t *testing.T) {
	db := setupTestDB(t)

	if err := db.CreateOrUpdateNode(t.Context(), database.NodeInput{
		PublicKey: "aabbccdd11223344",
		Name:      "RepA",
		Role:      "repeater",
		RxTime:    recentTS(72),
		IngestNow: recentTS(72),
	}); err != nil {
		t.Fatalf("seed RepA: %v", err)
	}

	store := newPacketStoreForTest(t, db)
	store.RefreshNodeCache(t.Context())
	// Advert with 1-byte hash only
	addTestPacket(store, makeTestAdvert("aabbccdd11223344", 1))

	caps := store.computeMultiByteCapability(nil)
	if len(caps) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(caps))
	}
	if caps[0].Status != "unknown" {
		t.Errorf("expected unknown, got %s", caps[0].Status)
	}
	if caps[0].MaxHashSize != 1 {
		t.Errorf("expected maxHashSize 1, got %d", caps[0].MaxHashSize)
	}
}

// TestMultiByteCapability_PrefixCollision tests that when two repeaters
// share the same prefix, one confirmed via advert, the other gets
// suspected (not confirmed) from path data alone.
func TestMultiByteCapability_PrefixCollision(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	now := time.Now().UTC()

	// Two repeaters sharing 1-byte prefix "aa"
	for _, n := range []database.NodeInput{
		{PublicKey: "aabb000000000001", Name: "RepConfirmed", Role: "repeater", RxTime: recentTS(24), IngestNow: now},
		{PublicKey: "aacc000000000002", Name: "RepOther", Role: "repeater", RxTime: recentTS(24), IngestNow: now},
	} {
		if err := db.CreateOrUpdateNode(ctx, n); err != nil {
			t.Fatalf("seed %s: %v", n.Name, err)
		}
	}

	store := newPacketStoreForTest(t, db)
	store.RefreshNodeCache(ctx)

	// RepConfirmed has a 2-byte advert
	addTestPacket(store, makeTestAdvert("aabb000000000001", 2))

	// A packet with 2-byte path containing 1-byte hop "aa" — both share this prefix
	pathByte := buildPathByte(2, 1)
	rawHex := "01" + pathByte + "aa"
	pt := types.PayloadType(1)
	pkt := &StoreTx{
		RawHex:      rawHex,
		PayloadType: &pt,
		PathJSON:    `["aa"]`,
		FirstSeen:   time.Now().UTC().Add(-48 * time.Hour),
	}
	addTestPacket(store, pkt)

	caps := store.computeMultiByteCapability(nil)
	if len(caps) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(caps))
	}

	capByName := map[string]MultiByteCapEntry{}
	for _, c := range caps {
		capByName[c.Name] = c
	}

	if capByName["RepConfirmed"].Status != "confirmed" {
		t.Errorf("RepConfirmed expected confirmed, got %s", capByName["RepConfirmed"].Status)
	}
	if capByName["RepOther"].Status != "suspected" {
		t.Errorf("RepOther expected suspected, got %s", capByName["RepOther"].Status)
	}
}

func TestMultiByteCapability_TraceExcluded(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: "aabbccdd11223344",
		Name:      "RepTrace",
		Role:      "repeater",
		RxTime:    recentTS(48),
		IngestNow: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed RepTrace: %v", err)
	}

	store := newPacketStoreForTest(t, db)
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache: %v", err)
	}

	pathByte := buildPathByte(2, 1)
	rawHex := "01" + pathByte + "aabb"
	pt := types.PayloadTRACE
	pkt := &StoreTx{
		RawHex:      rawHex,
		PayloadType: &pt,
		PathJSON:    `["aabb"]`,
		FirstSeen:   time.Now().UTC().Add(-48 * time.Hour),
	}
	addTestPacket(store, pkt)

	caps := store.computeMultiByteCapability(nil)
	if len(caps) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(caps))
	}
	if caps[0].Status != "unknown" {
		t.Errorf("expected unknown (TRACE excluded), got %s", caps[0].Status)
	}
}

func TestMultiByteCapability_NonTraceStillSuspected(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: "aabbccdd11223344",
		Name:      "RepNonTrace",
		Role:      "repeater",
		RxTime:    recentTS(48),
		IngestNow: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed RepNonTrace: %v", err)
	}

	store := newPacketStoreForTest(t, db)
	store.RefreshNodeCache(ctx)

	// GRP_TXT packet (payload_type 1) with 2-byte hash in path
	pathByte := buildPathByte(2, 1)
	rawHex := "01" + pathByte + "aabb"
	pt := types.PayloadType(1)
	pkt := &StoreTx{
		RawHex:      rawHex,
		PayloadType: &pt,
		PathJSON:    `["aabb"]`,
		FirstSeen:   time.Now().UTC().Add(-48 * time.Hour),
	}
	addTestPacket(store, pkt)

	caps := store.computeMultiByteCapability(nil)
	if len(caps) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(caps))
	}
	if caps[0].Status != "suspected" {
		t.Errorf("expected suspected, got %s", caps[0].Status)
	}
}

func TestMultiByteCapability_ConfirmedUnaffectedByTraceExclusion(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: "aabbccdd11223344",
		Name:      "RepConfirmedTrace",
		Role:      "repeater",
		RxTime:    recentTS(24),
		IngestNow: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed RepConfirmedTrace: %v", err)
	}

	store := newPacketStoreForTest(t, db)
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache: %v", err)
	}

	addTestPacket(store, makeTestAdvert("aabbccdd11223344", 2))

	pathByte := buildPathByte(2, 1)
	rawHex := "01" + pathByte + "aabb"
	pt := types.PayloadTRACE
	pkt := &StoreTx{
		RawHex:      rawHex,
		PayloadType: &pt,
		PathJSON:    `["aabb"]`,
		FirstSeen:   time.Now().UTC().Add(-48 * time.Hour),
	}
	addTestPacket(store, pkt)

	caps := store.computeMultiByteCapability(nil)
	if len(caps) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(caps))
	}
	if caps[0].Status != "confirmed" {
		t.Errorf("expected confirmed (unaffected by TRACE), got %s", caps[0].Status)
	}
}

func TestMultiByteCapability_CompanionConfirmed(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: "aabbccdd11223344",
		Name:      "CompA",
		Role:      "companion",
		RxTime:    recentTS(24),
		IngestNow: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed CompA: %v", err)
	}

	store := newPacketStoreForTest(t, db)
	store.RefreshNodeCache(ctx)
	addTestPacket(store, makeTestAdvert("aabbccdd11223344", 2))

	caps := store.computeMultiByteCapability(nil)
	if len(caps) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(caps))
	}
	if caps[0].Status != "confirmed" {
		t.Errorf("expected confirmed for companion, got %s", caps[0].Status)
	}
	if caps[0].Role != "companion" {
		t.Errorf("expected role companion, got %s", caps[0].Role)
	}
	if caps[0].Evidence != "advert" {
		t.Errorf("expected advert evidence, got %s", caps[0].Evidence)
	}
}

func TestMultiByteCapability_RoleColumnPopulated(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	now := time.Now().UTC()

	for _, n := range []database.NodeInput{
		{PublicKey: "aabb000000000001", Name: "Rep1", Role: "repeater", RxTime: recentTS(24), IngestNow: now},
		{PublicKey: "ccdd000000000002", Name: "Comp1", Role: "companion", RxTime: recentTS(24), IngestNow: now},
		{PublicKey: "eeff000000000003", Name: "Room1", Role: "room", RxTime: recentTS(24), IngestNow: now},
	} {
		if err := db.CreateOrUpdateNode(ctx, n); err != nil {
			t.Fatalf("seed %s: %v", n.Name, err)
		}
	}

	store := newPacketStoreForTest(t, db)
	store.RefreshNodeCache(ctx)
	addTestPacket(store, makeTestAdvert("aabb000000000001", 2))
	addTestPacket(store, makeTestAdvert("ccdd000000000002", 2))
	addTestPacket(store, makeTestAdvert("eeff000000000003", 1))

	caps := store.computeMultiByteCapability(nil)
	if len(caps) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(caps))
	}

	roleByName := map[string]types.NodeRole{}
	for _, c := range caps {
		roleByName[c.Name] = c.Role
	}
	if roleByName["Rep1"] != types.RoleRepeater {
		t.Errorf("Rep1 role: expected repeater, got %s", roleByName["Rep1"])
	}
	if roleByName["Comp1"] != types.RoleCompanion {
		t.Errorf("Comp1 role: expected companion, got %s", roleByName["Comp1"])
	}
	if roleByName["Room1"] != types.RoleRoom {
		t.Errorf("Room1 role: expected room_server, got %s", roleByName["Room1"])
	}
}

func TestMultiByteCapability_AdopterEvidenceTakesPrecedence(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: "aabbccdd11223344",
		Name:      "RepAdopter",
		Role:      "repeater",
		RxTime:    recentTS(24),
		IngestNow: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed RepAdopter: %v", err)
	}

	store := newPacketStoreForTest(t, db)
	store.RefreshNodeCache(ctx)

	// Only a path-based packet (no advert) — would normally be "suspected"
	pathByte := buildPathByte(2, 1)
	rawHex := "01" + pathByte + "aabb"
	pt := types.PayloadType(1)
	pkt := &StoreTx{
		RawHex:      rawHex,
		PayloadType: &pt,
		PathJSON:    `["aabb"]`,
		FirstSeen:   time.Now().UTC().Add(-48 * time.Hour),
	}
	addTestPacket(store, pkt)

	// Without adopter data: should be suspected
	caps := store.computeMultiByteCapability(nil)
	capByName := map[string]MultiByteCapEntry{}
	for _, c := range caps {
		capByName[c.Name] = c
	}
	if capByName["RepAdopter"].Status != "suspected" {
		t.Errorf("without adopter data: expected suspected, got %s", capByName["RepAdopter"].Status)
	}

	// With adopter data showing hashSize 2: should be confirmed
	adopterHS := map[string]int{"aabbccdd11223344": 2}
	caps = store.computeMultiByteCapability(adopterHS)
	capByName = map[string]MultiByteCapEntry{}
	for _, c := range caps {
		capByName[c.Name] = c
	}
	if capByName["RepAdopter"].Status != "confirmed" {
		t.Errorf("with adopter data: expected confirmed, got %s", capByName["RepAdopter"].Status)
	}
	if capByName["RepAdopter"].Evidence != "advert" {
		t.Errorf("with adopter data: expected advert evidence, got %s", capByName["RepAdopter"].Evidence)
	}
}

func TestMultibyteCapGetMultibyteCapForO1(t *testing.T) {
	db := setupTestDB(t)
	store := newPacketStoreForTest(t, db)

	// Directly populate the index as the analytics cycle would.
	store.cacheMu.Lock()
	store.mbCapIndex = map[string]MultiByteCapEntry{
		"aabbccdd11223344": {PublicKey: "aabbccdd11223344", Status: "confirmed", Evidence: "advert"},
		"eeff001122334455": {PublicKey: "eeff001122334455", Status: "suspected", Evidence: "path"},
	}
	store.cacheMu.Unlock()

	e, ok := store.GetMultibyteCapFor("aabbccdd11223344")
	if !ok || e == nil {
		t.Fatal("expected entry for known pubkey, got none")
	}
	if e.Status != "confirmed" {
		t.Errorf("status = %q, want confirmed", e.Status)
	}

	_, ok = store.GetMultibyteCapFor("0000000000000000")
	if ok {
		t.Error("expected no entry for unknown pubkey")
	}
}

func TestMultibyteCapLoadFromDB(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	now := time.Now().UTC()
	rx := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	for _, n := range []database.NodeInput{
		{PublicKey: "aa11000000000001", Name: "A", Role: "repeater", RxTime: rx, IngestNow: now},
		{PublicKey: "bb22000000000002", Name: "B", Role: "repeater", RxTime: rx, IngestNow: now},
		{PublicKey: "cc33000000000003", Name: "C", Role: "repeater", RxTime: rx, IngestNow: now}, // unknown: must be skipped
	} {
		if err := db.CreateOrUpdateNode(ctx, n); err != nil {
			t.Fatalf("seed %s: %v", n.Name, err)
		}
	}
	// C is left at the column default (0 = unknown).
	caps := []database.NodeMultibyteCap{
		{PublicKey: "aa11000000000001", Sup: 2, Evidence: "advert"},
		{PublicKey: "bb22000000000002", Sup: 1, Evidence: "path"},
	}
	changed, err := db.UpdateNodeMultibyteCaps(ctx, caps)
	if err != nil {
		t.Fatalf("set multibyte caps: %v", err)
	}
	if changed != 2 {
		t.Fatalf("UpdateNodeMultibyteCaps changed %d rows, want 2 (a missing node is skipped silently)", changed)
	}

	// Applying the same values again must not rewrite anything.
	again, err := db.UpdateNodeMultibyteCaps(ctx, caps)
	if err != nil {
		t.Fatalf("repeat update: %v", err)
	}
	if again != 0 {
		t.Fatalf("repeat update changed %d rows, want 0 (stored values already match)", again)
	}

	store := newPacketStoreForTest(t, db)
	store.loadMultibyteCapFromDB(ctx)

	store.cacheMu.Lock()
	snap := store.mbCapSnapshot
	idx := store.mbCapIndex
	store.cacheMu.Unlock()

	if len(snap) != 2 {
		t.Fatalf("expected 2 entries (confirmed+suspected), got %d", len(snap))
	}
	if e, ok := idx["aa11000000000001"]; !ok || e.Status != "confirmed" {
		t.Errorf("aa11: expected confirmed, got %+v", e)
	}
	if e, ok := idx["bb22000000000002"]; !ok || e.Status != "suspected" {
		t.Errorf("bb22: expected suspected, got %+v", e)
	}
	if _, ok := idx["cc33000000000003"]; ok {
		t.Error("cc33 with sup=0 should not be in the index")
	}
}
