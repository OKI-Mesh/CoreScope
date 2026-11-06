package main

import (
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// seedCollisionAdvert inserts one ADVERT seen once by observerIdx with a
// single 3-byte hop "aabb11".
func seedCollisionAdvert(t *testing.T, db *database.DB, observerIdx int64, hash, rawHex, decodedJSON string, snr, rssi float64, rx time.Time) {
	t.Helper()
	dbtest.TxWithObservation(t, db, observerIdx, database.TransmissionInput{
		RawHex:      rawHex,
		Hash:        hash,
		RouteType:   types.RouteType(1),
		PayloadType: types.PayloadType(4), // ADVERT
		DecodedJSON: decodedJSON,
	}, rx, snr, rssi, "aabb11")
}

// collisionsForSize3 loads the store and returns the by_size[3] collision list.
func collisionsForSize3(t *testing.T, db *database.DB) []collisionEntry {
	ctx := t.Context()
	t.Helper()

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}

	result := store.GetAnalyticsHashCollisions(ctx, "", "")
	bySize, ok := result["by_size"].(map[string]interface{})
	if !ok {
		t.Fatal("expected by_size map")
	}
	size3, ok := bySize["3"].(map[string]interface{})
	if !ok {
		t.Fatal("expected by_size[3] map")
	}
	collisions, ok := size3["collisions"].([]collisionEntry)
	if !ok {
		t.Fatalf("expected collisions as []collisionEntry, got %T", size3["collisions"])
	}
	return collisions
}

// TestCollisionDetailsIncludeNodePairs verifies that collision details contain
// the correct prefix and matching node pairs (#757).
func TestCollisionDetailsIncludeNodePairs(t *testing.T) {
	db := setupTestDB(t)
	recent := time.Now().UTC().Add(-1 * time.Hour)

	// Two repeater nodes with the same 3-byte prefix.
	dbtest.Node(t, db, "aabb11ccdd001122", "Node Alpha", "repeater")
	dbtest.Node(t, db, "aabb11eeff334455", "Node Beta", "repeater")
	obsIdx := dbtest.Observer(t, db, "obsa", "Obs A", "SEA", recent, recent)

	// Advert transmissions with hash_size=3 path bytes (0x80 = bits 10 → size 3).
	seedCollisionAdvert(t, db, obsIdx, "col_hash_01", "0180aabb11ccdd",
		`{"pubKey":"aabb11ccdd001122","name":"Node Alpha","type":"ADVERT"}`, 10.0, -91, recent)
	seedCollisionAdvert(t, db, obsIdx, "col_hash_02", "0180aabb11eeff",
		`{"pubKey":"aabb11eeff334455","name":"Node Beta","type":"ADVERT"}`, 9.0, -93, recent)

	collisions := collisionsForSize3(t, db)

	var found *collisionEntry
	for i := range collisions {
		if collisions[i].Prefix == "AABB11" {
			found = &collisions[i]
			break
		}
	}
	if found == nil {
		t.Fatal("expected collision with prefix AABB11")
	}
	if found.Appearances != 2 {
		t.Errorf("expected 2 appearances, got %d", found.Appearances)
	}
	if len(found.Nodes) != 2 {
		t.Fatalf("expected 2 nodes in collision, got %d", len(found.Nodes))
	}

	pubkeys := map[string]bool{}
	names := map[string]bool{}
	for _, n := range found.Nodes {
		pubkeys[n.PublicKey] = true
		names[n.Name] = true
	}
	for _, pk := range []string{"aabb11ccdd001122", "aabb11eeff334455"} {
		if !pubkeys[pk] {
			t.Errorf("expected node %s in collision", pk)
		}
	}
	for _, name := range []string{"Node Alpha", "Node Beta"} {
		if !names[name] {
			t.Errorf("expected %s in collision", name)
		}
	}
}

// TestCollisionDetailsEmptyWhenNoCollisions verifies that collision details are
// empty when there are no collisions (#757).
func TestCollisionDetailsEmptyWhenNoCollisions(t *testing.T) {
	db := setupTestDB(t)
	recent := time.Now().UTC().Add(-1 * time.Hour)

	dbtest.Node(t, db, "aabb11ccdd001122", "Solo Node", "repeater")
	obsIdx := dbtest.Observer(t, db, "obsa", "Obs A", "SEA", recent, recent)
	seedCollisionAdvert(t, db, obsIdx, "solo_hash_01", "0180aabb11ccdd",
		`{"pubKey":"aabb11ccdd001122","name":"Solo Node","type":"ADVERT"}`, 10.0, -91, recent)

	if collisions := collisionsForSize3(t, db); len(collisions) != 0 {
		t.Errorf("expected 0 collisions, got %d", len(collisions))
	}
}
