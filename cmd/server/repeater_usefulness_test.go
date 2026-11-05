package main

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

type relayPacket struct {
	Hash        string
	RouteType   types.RouteType
	PayloadType types.PayloadType
	At          time.Time
	Hops        []string
	Resolved    []*string
}

// TestRepeaterUsefulness_BasicShare verifies that usefulness_score is
// relay_count_24h / total_non_advert_traffic_24h. With 1 of 4 relayed
// packets going through the repeater, score should be 0.25.
//
// Issue #672. We are intentionally implementing the *traffic share*
// dimension of the composite score from the issue body — bridge,
// coverage, redundancy are deferred to follow-up work. This is the
// "Traffic" axis of the table in #672.
func TestRepeaterUsefulness_BasicShare(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	pubkey := "aabbccdd" + strings.Repeat("1", 56)

	if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: pubkey, Name: "UsefulRepeater", Role: "repeater",
		Lat: nil, Lon: nil, RxTime: now, IngestNow: now,
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}
	obsIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID: scopeKey(60), Name: "Useful Observer", IATA: "SFO", Meta: nil, LastSeen: now, IngestNow: now,
	})
	if err != nil {
		t.Fatalf("seed observer: %v", err)
	}

	for i := 0; i < 4; i++ {
		p := relayPacket{
			Hash:        fmt.Sprintf("b1%014x", i),
			RouteType:   1,
			PayloadType: types.PayloadType(1),
			At:          now,
			Hops:        nil,
			Resolved:    nil,
		}
		if i == 0 {
			p.Hops = []string{"aa"}
			p.Resolved = []*string{sptr(pubkey)}
		}
		insertRelayPacket(t, db, obsIdx, p)
	}
	store := loadedStore(t, db)

	score := store.GetRepeaterUsefulnessScore(pubkey)
	if score < 0.24 || score > 0.26 {
		t.Errorf("expected usefulness ~0.25, got %f", score)
	}
}

// TestRepeaterUsefulness_NoTraffic verifies score is 0 when there is
// no non-advert traffic to share.
func TestRepeaterUsefulness_NoTraffic(t *testing.T) {
	db := setupTestDB(t)
	store := loadedStore(t, db)
	score := store.GetRepeaterUsefulnessScore("deadbeef" + strings.Repeat("c", 56))
	if score != 0 {
		t.Errorf("expected 0 for empty store, got %f", score)
	}
}

// TestRepeaterUsefulness_AdvertsExcluded verifies that ADVERT packets
// (payload_type=4) are excluded from both numerator and denominator —
// adverts don't count as forwarded traffic.
func TestRepeaterUsefulness_AdvertsExcluded(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	pubkey := "11aa22bb" + strings.Repeat("3", 56)

	if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: pubkey, Name: "AdvertExcluded", Role: "repeater",
		Lat: nil, Lon: nil, RxTime: now, IngestNow: now,
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}
	obsIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID: scopeKey(61), Name: "Advert Excluded Observer", IATA: "SFO", Meta: nil, LastSeen: now, IngestNow: now,
	})
	if err != nil {
		t.Fatalf("seed observer: %v", err)
	}

	for i := 0; i < 2; i++ {
		insertRelayPacket(t, db, obsIdx, relayPacket{
			Hash:        fmt.Sprintf("b2%014x", i),
			RouteType:   1,
			PayloadType: types.PayloadType(1),
			At:          now,
			Hops:        []string{"11"},
			Resolved:    []*string{sptr(pubkey)},
		})
	}
	for i := 0; i < 100; i++ {
		insertRelayPacket(t, db, obsIdx, relayPacket{
			Hash:        fmt.Sprintf("ad%014x", i),
			RouteType:   1,
			PayloadType: types.PayloadType(payloadTypeAdvert),
			At:          now,
			Hops:        nil,
			Resolved:    nil,
		})
	}
	store := loadedStore(t, db)

	score := store.GetRepeaterUsefulnessScore(pubkey)
	if score < 0.99 || score > 1.01 {
		t.Errorf("expected usefulness ~1.0 (adverts excluded), got %f", score)
	}
}

// TestGetRepeaterNodeStatsBatch_Parity verifies that GetRepeaterNodeStatsBatch
// returns the same usefulness score as GetRepeaterUsefulnessScore for a given
// pubkey, so both paths stay in sync.
func TestGetRepeaterNodeStatsBatch_Parity(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	pubkey := "aabbccdd" + strings.Repeat("1", 56)

	if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: pubkey, Name: "ParityRepeater", Role: "repeater",
		Lat: nil, Lon: nil, RxTime: now, IngestNow: now,
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}
	obsIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID: scopeKey(62), Name: "Parity Observer", IATA: "SFO", Meta: nil, LastSeen: now, IngestNow: now,
	})
	if err != nil {
		t.Fatalf("seed observer: %v", err)
	}

	for i := 0; i < 4; i++ {
		p := relayPacket{
			Hash:        fmt.Sprintf("b3%014x", i),
			RouteType:   1,
			PayloadType: types.PayloadType(1),
			At:          now,
			Hops:        nil,
			Resolved:    nil,
		}
		if i == 0 {
			p.Hops = []string{"aa"}
			p.Resolved = []*string{sptr(pubkey)}
		}
		insertRelayPacket(t, db, obsIdx, p)
	}
	store := loadedStore(t, db)

	want := store.GetRepeaterUsefulnessScore(pubkey)
	batch := store.GetRepeaterNodeStatsBatch([]string{pubkey}, 24)
	got := batch[pubkey].Score
	if got != want {
		t.Errorf("batch score %f != single score %f", got, want)
	}
}

// TestGetRepeaterNodeStatsBatch_EmptyInput verifies that an empty pubkey list
// returns an empty map without panicking.
func TestGetRepeaterNodeStatsBatch_EmptyInput(t *testing.T) {
	db := setupTestDB(t)
	store := loadedStore(t, db)
	result := store.GetRepeaterNodeStatsBatch([]string{}, 24)
	if len(result) != 0 {
		t.Errorf("expected empty map for empty input, got %d entries", len(result))
	}
}

// TestGetRepeaterNodeStatsBatch_MissingPubkey verifies that a pubkey with no
// indexed packets gets a zero score and empty relay info rather than panicking.
func TestGetRepeaterNodeStatsBatch_MissingPubkey(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	missingPK := "deadbeef" + strings.Repeat("c", 56)

	obsIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID: scopeKey(63), Name: "Missing Observer", IATA: "SFO", Meta: nil, LastSeen: now, IngestNow: now,
	})
	if err != nil {
		t.Fatalf("seed observer: %v", err)
	}
	insertRelayPacket(t, db, obsIdx, relayPacket{
		Hash:        "b4000000000000000"[:16],
		RouteType:   1,
		PayloadType: types.PayloadType(1),
		At:          now,
		Hops:        nil,
		Resolved:    nil,
	})
	store := loadedStore(t, db)

	result := store.GetRepeaterNodeStatsBatch([]string{missingPK}, 24)
	stats := result[missingPK]
	if stats.Score != 0 {
		t.Errorf("expected score 0 for unknown pubkey, got %f", stats.Score)
	}
	if stats.Info.RelayCount24h != 0 {
		t.Errorf("expected RelayCount24h 0 for unknown pubkey, got %d", stats.Info.RelayCount24h)
	}
}

// TestGetRepeaterNodeStatsBatch_Race verifies that concurrent calls to
// GetRepeaterNodeStatsBatch and GetRepeaterUsefulnessScore do not trigger
// the race detector — all StoreTx reads must happen under the store lock.
func TestGetRepeaterNodeStatsBatch_Race(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	pubkey := "aabbccdd" + strings.Repeat("1", 56)

	if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: pubkey, Name: "RaceRepeater", Role: "repeater",
		Lat: nil, Lon: nil, RxTime: now, IngestNow: now,
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}
	obsIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID: scopeKey(64), Name: "Race Observer", IATA: "SFO", Meta: nil, LastSeen: now, IngestNow: now,
	})
	if err != nil {
		t.Fatalf("seed observer: %v", err)
	}

	for i := 0; i < 10; i++ {
		insertRelayPacket(t, db, obsIdx, relayPacket{
			Hash:        fmt.Sprintf("b5%014x", i),
			RouteType:   1,
			PayloadType: types.PayloadType(1),
			At:          now,
			Hops:        []string{"aa"},
			Resolved:    []*string{sptr(pubkey)},
		})
	}
	store := loadedStore(t, db)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			store.GetRepeaterNodeStatsBatch([]string{pubkey}, 24)
		}()
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			store.GetRepeaterUsefulnessScore(pubkey)
		}()
	}
	wg.Wait()
}
