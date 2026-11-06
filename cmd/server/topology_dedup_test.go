package main

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

var (
	topoAquaPK  = padKey("0735bc6dda4d1122aabbccdd")
	topoBetaPK  = padKey("99aabb001122334455667788")
	topoNodeAPK = padKey("ab11223344556677aabbccdd")
	topoNodeBPK = padKey("ab99887766554433aabbccdd")
)

func insertPathPackets(t *testing.T, db *database.DB, observerIdx int64, n *int, hops []string, resolved []*string, count int, base time.Time) {
	t.Helper()
	ctx := t.Context()
	for i := 0; i < count; i++ {
		*n++
		at := base.Add(time.Duration(*n) * time.Minute)
		txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
			Hash:        fmt.Sprintf("%016x", *n),
			RawHex:      "aabb",
			RouteType:   types.RouteType(0),
			PayloadType: types.PayloadType(4),
			DecodedJSON: fmt.Sprintf(`{"pubKey":"pk%04d"}`, *n),
		}, at)
		if err != nil {
			t.Fatalf("insert tx %d: %v", *n, err)
		}
		idx := observerIdx
		_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
			TransmissionID: txID,
			ObserverIdx:    &idx,
			SNR:            fptr(-10.0),
			RSSI:           fptr(-80.0),
			Timestamp:      at,
		}, hops, resolved)
		if err != nil {
			t.Fatalf("insert observation %d: %v", *n, err)
		}
		if !inserted {
			t.Fatalf("expected observation to be inserted for tx %d", txID)
		}
	}
}

// TestTopologyDedup_RepeatersMergeByPubkey verifies that topRepeaters
// merges entries whose hop prefixes resolve unambiguously to the same node.
func TestTopologyDedup_RepeatersMergeByPubkey(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)

	obsIdx := seedObserver(t, db, padKey("0b51"), "BRU", base)
	seedNode(t, db, topoAquaPK, "AQUA", "repeater", nil, nil, base)
	seedNode(t, db, topoBetaPK, "BETA", "repeater", nil, nil, base)

	n := 0
	insertPathPackets(t, db, obsIdx, &n, []string{"07", "99aa"}, []*string{sptr(topoAquaPK), sptr(topoBetaPK)}, 10, base)
	insertPathPackets(t, db, obsIdx, &n, []string{"0735bc", "99"}, []*string{sptr(topoAquaPK), sptr(topoBetaPK)}, 5, base)
	insertPathPackets(t, db, obsIdx, &n, []string{"0735bc6d", "99aabb"}, []*string{sptr(topoAquaPK), sptr(topoBetaPK)}, 3, base)

	store := loadedStore(t, db)

	result := store.computeAnalyticsTopology(ctx, "", "", TimeWindow{})
	topRepeaters := result["topRepeaters"].([]map[string]interface{})

	pubkeyCounts := map[string]int{}
	for _, entry := range topRepeaters {
		pk, _ := entry["pubkey"].(string)
		if pk == "" {
			continue
		}
		pubkeyCounts[pk] += entry["count"].(int)
	}

	aquaEntries := 0
	betaEntries := 0
	for _, entry := range topRepeaters {
		pk, _ := entry["pubkey"].(string)
		if pk == topoAquaPK {
			aquaEntries++
		}
		if pk == topoBetaPK {
			betaEntries++
		}
	}

	if aquaEntries != 1 {
		t.Errorf("AQUA should appear exactly once in topRepeaters after dedup, got %d entries", aquaEntries)
		for _, e := range topRepeaters {
			t.Logf("  entry: hop=%v name=%v pubkey=%v count=%v", e["hop"], e["name"], e["pubkey"], e["count"])
		}
	}
	if betaEntries != 1 {
		t.Errorf("BETA should appear exactly once in topRepeaters after dedup, got %d entries", betaEntries)
	}

	if c := pubkeyCounts[topoAquaPK]; c != 18 {
		t.Errorf("AQUA total count should be 18, got %d", c)
	}
	if c := pubkeyCounts[topoBetaPK]; c != 18 {
		t.Errorf("BETA total count should be 18, got %d", c)
	}
}

// TestTopologyDedup_AmbiguousPrefixNotMerged verifies that ambiguous short
// prefixes (matching multiple nodes) are NOT merged — they stay separate.
func TestTopologyDedup_AmbiguousPrefixNotMerged(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)

	obsIdx := seedObserver(t, db, padKey("0b51"), "BRU", base)
	seedNode(t, db, topoNodeAPK, "NODE_A", "repeater", nil, nil, base)
	seedNode(t, db, topoNodeBPK, "NODE_B", "repeater", nil, nil, base)

	n := 0
	insertPathPackets(t, db, obsIdx, &n, []string{"ab"}, []*string{nil}, 10, base)
	insertPathPackets(t, db, obsIdx, &n, []string{"ab1122"}, []*string{sptr(topoNodeAPK)}, 5, base)

	store := loadedStore(t, db)

	result := store.computeAnalyticsTopology(ctx, "", "", TimeWindow{})

	topRepeaters := result["topRepeaters"].([]map[string]interface{})

	for _, e := range topRepeaters {
		t.Logf("entry: %#v", e)
	}
	t.Logf("topoNodeAPK = %q, topoNodeBPK = %q", topoNodeAPK, topoNodeBPK)

	foundAb := false
	foundAb1122 := false
	for _, entry := range topRepeaters {
		hop := entry["hop"].(string)
		count := entry["count"].(int)
		if hop == strings.ToUpper("ab") {
			foundAb = true
			if count != 10 {
				t.Errorf("ambiguous hop 'AB' should have count=10, got %d", count)
			}
		}
		if hop == strings.ToUpper("ab1122") {
			foundAb1122 = true
			if count != 5 {
				t.Errorf("unambiguous hop 'AB1122' should have count=5, got %d", count)
			}
		}
	}
	if !foundAb {
		t.Error("ambiguous hop 'AB' should remain as separate entry")
	}
	if !foundAb1122 {
		t.Error("unambiguous hop 'AB1122' should remain as separate entry (not merged with ambiguous 'AB')")
	}
}

// TestTopologyDedup_PairsMergeByPubkey verifies that topPairs merges
// pair entries whose hops resolve unambiguously to the same node pair.
func TestTopologyDedup_PairsMergeByPubkey(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)

	obsIdx := seedObserver(t, db, padKey("0b51"), "BRU", base)
	seedNode(t, db, topoAquaPK, "AQUA", "repeater", nil, nil, base)
	seedNode(t, db, topoBetaPK, "BETA", "repeater", nil, nil, base)

	n := 0
	insertPathPackets(t, db, obsIdx, &n, []string{"07", "99aa"}, []*string{sptr(topoAquaPK), sptr(topoBetaPK)}, 10, base)
	insertPathPackets(t, db, obsIdx, &n, []string{"0735bc", "99"}, []*string{sptr(topoAquaPK), sptr(topoBetaPK)}, 5, base)

	store := loadedStore(t, db)

	result := store.computeAnalyticsTopology(ctx, "", "", TimeWindow{})
	topPairs := result["topPairs"].([]map[string]interface{})

	aquaBetaPairs := 0
	totalCount := 0
	for _, entry := range topPairs {
		pkA, _ := entry["pubkeyA"].(string)
		pkB, _ := entry["pubkeyB"].(string)
		if (pkA == topoAquaPK && pkB == topoBetaPK) || (pkA == topoBetaPK && pkB == topoAquaPK) {
			aquaBetaPairs++
			totalCount += entry["count"].(int)
		}
	}

	if aquaBetaPairs != 1 {
		t.Errorf("AQUA-BETA pair should appear exactly once after dedup, got %d entries", aquaBetaPairs)
		for _, e := range topPairs {
			t.Logf("  pair: hopA=%v hopB=%v count=%v pkA=%v pkB=%v", e["hopA"], e["hopB"], e["count"], e["pubkeyA"], e["pubkeyB"])
		}
	}
	if totalCount != 15 {
		t.Errorf("AQUA-BETA pair total count should be 15, got %d", totalCount)
	}
}
