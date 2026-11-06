package main

import (
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// Zero-hop direct adverts carry no path, so the hop count is always 0. Whether
// the two SIZE bits next to it mean anything depends on the sender:
//
//   - Firmware predating meshcore-dev/MeshCore#3293 does `path_len = 0`, which
//     wipes the whole byte. 0x00 therefore says nothing about path.hash.mode
//     and must not be read as "1 byte".
//   - A sender that writes the size through setPathHashSizeAndCount() emits
//     0x40 (2 bytes) or 0x80 (3 bytes) with a zero hop count. Nothing else can
//     produce those bits here, so they are a deliberate declaration.
//
// Before this fix the skip was keyed on the route type and swallowed both
// cases. These tests pin the distinction. The two multi-byte fixtures are real
// packets off the Czech mesh (869.4 MHz), captured 2026-08-24/25.

func zeroHopStore(t *testing.T) (*database.DB, time.Time, int64) {
	t.Helper()
	ctx := t.Context()
	db := setupTestDB(t)
	now := time.Now().UTC().Add(-1 * time.Hour)
	observerID := mustCreateTestObserver(t, ctx, db, "obs1", "Obs", "PRG", now)
	return db, now, observerID
}

// TestZeroHopDirectAdvertDeclaredSizeIsEvidence: a zero-hop DIRECT advert whose
// path byte is 0x40 declares a 2-byte path hash and must be counted.
// Fixture: tth-hrebecna, 2026-08-25T09:29:40Z, header 0x12 (route 2, ADVERT).
func TestZeroHopDirectAdvertDeclaredSizeIsEvidence(t *testing.T) {
	ctx := t.Context()

	db, recent, observerID := zeroHopStore(t)
	defer db.Close()

	const hrebecnaPK = "5361aa08929b6248bb61cb1fab0fdb97d6dba8ae44f53a44427f63e7798a6b6b"
	insertAdvert(t, db, observerID, advertPacket{
		Role: types.RoleRepeater,
		Hash: "20a0000000000001",
		RawHex: "1240" + hrebecnaPK + "83608d6a3d38eeb187a94a745daf2ffb3191d1ae49fb82f30a083ba081ffbb20" +
			"266331879c931267408acd04bcb8c8aec558d106ab40e942c70ce8e214af68529faa580b9243bd000322a5c300" +
			"7474682d6872656265636e61",
		RouteType:   types.RouteDirect,
		PayloadType: types.PayloadType(4),
		Name:        "tth-hrebecna",
		PubKey:      hrebecnaPK,
		At:          recent,
		Hops:        nil,
	})

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	info := store.GetNodeHashSizeInfo()

	ni, ok := info[hrebecnaPK]
	if !ok {
		t.Fatal("node with a 0x40 zero-hop advert missing from hash size info — the declared size was dropped")
	}
	if ni.HashSize != 2 {
		t.Errorf("want HashSize=2 from path byte 0x40, got %d", ni.HashSize)
	}
	if ni.Inconsistent {
		t.Error("single advert must not be flagged inconsistent")
	}
}

// TestZeroHopDirectAdvertThreeByteDeclaration: same rule at 3 bytes (0x80).
// Fixture: TTH-L1, 2026-08-24T15:36:45Z.
func TestZeroHopDirectAdvertThreeByteDeclaration(t *testing.T) {
	ctx := t.Context()

	db, recent, observerID := zeroHopStore(t)
	defer db.Close()

	const pk = "e11e55684170982339efe2266a423c9040f909339646ee2adeea19c21ed2b02a"
	insertAdvert(t, db, observerID, advertPacket{
		Role: types.RoleRepeater,
		Hash: "20a0000000000002",
		RawHex: "1280" + pk + "0c658c6afcf586f0935c87b563dc0770f1ccb558126d16e4805eb4c556d511bf" +
			"c1f5d832a760bbe2f08d73b62c068c1a269425faadb2f575a69bcfc53ebae0cb65b71d0981" +
			"5454482d4c31",
		RouteType:   types.RouteDirect,
		PayloadType: types.PayloadType(4),
		Name:        "TTH-L1",
		PubKey:      pk,
		At:          recent,
		Hops:        nil,
	})

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	info := store.GetNodeHashSizeInfo()

	ni, ok := info[pk]
	if !ok {
		t.Fatal("node with a 0x80 zero-hop advert missing from hash size info")
	}
	if ni.HashSize != 3 {
		t.Errorf("want HashSize=3 from path byte 0x80, got %d", ni.HashSize)
	}
}

// TestZeroHopDirectAdvertAllZeroByteStillSkipped: 0x00 carries no information —
// the sender wiped the size bits — so the node must stay absent rather than be
// recorded as 1 byte. This is the behaviour #649 asked for and it must survive.
func TestZeroHopDirectAdvertAllZeroByteStillSkipped(t *testing.T) {
	ctx := t.Context()

	db, recent, observerID := zeroHopStore(t)
	defer db.Close()

	const pk = "aaaa000000000000000000000000000000000000000000000000000000000001"
	insertAdvert(t, db, observerID, advertPacket{
		Role:        types.RoleRepeater,
		Hash:        "20a0000000000003",
		RawHex:      "1200" + pk + "cafe",
		RouteType:   types.RouteDirect,
		PayloadType: types.PayloadType(4),
		Name:        "Legacy-Node",
		PubKey:      pk,
		At:          recent,
		Hops:        nil,
	})

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	if ni, ok := store.GetNodeHashSizeInfo()[pk]; ok {
		t.Errorf("node seen only via a 0x00 zero-hop advert must have no hash size evidence, got %d", ni.HashSize)
	}
}

// TestZeroHopTransportDirectDeclaredSize: route type 3 puts the path byte at
// offset 5, behind four transport code bytes. The same content rule applies.
func TestZeroHopTransportDirectDeclaredSize(t *testing.T) {
	ctx := t.Context()

	db, recent, observerID := zeroHopStore(t)
	defer db.Close()

	const declared = "aaaa000000000000000000000000000000000000000000000000000000000002"
	const wiped = "aaaa000000000000000000000000000000000000000000000000000000000003"

	// header 0x13 = payload_type 4 (ADVERT), route type 3; 4 transport bytes; path byte 0x40.
	insertAdvert(t, db, observerID, advertPacket{
		Role:        types.RoleRepeater,
		Hash:        "20a0000000000004",
		RawHex:      "1301020304" + "40" + declared + "cafe",
		RouteType:   types.RouteTransportDirect,
		PayloadType: types.PayloadType(4),
		Name:        "TD-Declared",
		PubKey:      declared,
		At:          recent,
		Hops:        nil,
	})
	// Same shape, path byte 0x00 → no evidence.
	insertAdvert(t, db, observerID, advertPacket{
		Role:        types.RoleRepeater,
		Hash:        "20a0000000000005",
		RawHex:      "1305060708" + "00" + wiped + "cafe",
		RouteType:   types.RouteTransportDirect,
		PayloadType: types.PayloadType(4),
		Name:        "TD-Wiped",
		PubKey:      wiped,
		At:          recent,
		Hops:        nil,
	})

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	info := store.GetNodeHashSizeInfo()

	if ni, ok := info[declared]; !ok {
		t.Error("transport-direct node with 0x40 at offset 5 missing from hash size info")
	} else if ni.HashSize != 2 {
		t.Errorf("transport-direct: want HashSize=2, got %d", ni.HashSize)
	}
	if _, ok := info[wiped]; ok {
		t.Error("transport-direct node with an all-zero path byte must have no evidence")
	}
}

// TestZeroHopDeclaredSizeReachesMultiByteCapability: the capability view derives
// "confirmed" from advert evidence, so a node heard only via zero-hop adverts
// should now be confirmed rather than left unknown. This is the user-visible
// half — it is what the map's multi-byte overlay reads.
func TestZeroHopDeclaredSizeReachesMultiByteCapability(t *testing.T) {
	ctx := t.Context()

	db, recent, observerID := zeroHopStore(t)
	defer db.Close()

	const pk = "5361aa08929b6248bb61cb1fab0fdb97d6dba8ae44f53a44427f63e7798a6b6b"
	insertAdvert(t, db, observerID, advertPacket{
		Role:        types.RoleRepeater,
		Hash:        "20a0000000000006",
		RawHex:      "1240" + pk + "cafe",
		RouteType:   types.RouteDirect,
		PayloadType: types.PayloadType(4),
		Name:        "tth-hrebecna",
		PubKey:      pk,
		At:          recent,
		Hops:        nil,
	})
	mustIncrementTestNodeAdvertCount(t, ctx, db, pk, 1)

	store := newPacketStoreForTest(t, db)
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache: %v", err)
	}
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	var found *MultiByteCapEntry
	for _, e := range store.computeMultiByteCapability(nil) {
		if e.PublicKey == pk {
			entry := e
			found = &entry
			break
		}
	}
	if found == nil {
		t.Fatal("node missing from multi-byte capability output")
	}
	if found.Status != "confirmed" {
		t.Errorf("want status=confirmed from a declared zero-hop advert, got %q (evidence %q)", found.Status, found.Evidence)
	}
	if found.MaxHashSize != 2 {
		t.Errorf("want MaxHashSize=2, got %d", found.MaxHashSize)
	}
}
