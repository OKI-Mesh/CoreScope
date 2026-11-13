package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// TestGetChannelMessagesPerfLargeChannel guards the in-memory channel message
// path with a realistic loaded-store size. The store holds 1500 transmissions
// and 75K observations; returning one 50-message page must stay below budget.
func TestGetChannelMessagesPerfLargeChannel(t *testing.T) {
	if testing.Short() {
		t.Skip("perf test")
	}
	ctx := t.Context()
	db := setupTestDB(t)

	const numTx = 1500
	const obsPerTx = 50

	store := newPacketStoreForTest(t, db)
	payload := types.PayloadGRP_TXT
	base := time.Now().UTC().Add(-24 * time.Hour)
	for i := 0; i < numTx; i++ {
		seen := base.Add(time.Duration(i) * time.Second)
		tx := &StoreTx{
			ID:           i + 1,
			Hash:         fmt.Sprintf("perfhash%08d", i),
			FirstSeen:    seen,
			LatestSeen:   seen,
			PayloadType:  &payload,
			DecodedJSON:  fmt.Sprintf(`{"type":"CHAN","channel":"#perf","text":"Sender%d: msg %d","sender":"Sender%d"}`, i%10, i, i%10),
			Observations: make([]*StoreObs, obsPerTx),
		}
		for j := range tx.Observations {
			tx.Observations[j] = &StoreObs{
				ID:             i*obsPerTx + j + 1,
				TransmissionID: tx.ID,
				ObserverID:     fmt.Sprintf("obs_perf_%d", j),
				Timestamp:      seen,
			}
		}
		store.byPayloadType[payload] = append(store.byPayloadType[payload], tx)
	}

	// Warm-up call to avoid first-use effects dominating the measurement.
	if _, total := store.GetChannelMessages(ctx, "#perf", 50, 0); total != numTx {
		t.Fatalf("warm-up total: got %d want %d", total, numTx)
	}

	start := time.Now()
	msgs, total := store.GetChannelMessages(ctx, "#perf", 50, 0)
	elapsed := time.Since(start)
	if total != numTx {
		t.Errorf("total: got %d want %d", total, numTx)
	}
	if len(msgs) != 50 {
		t.Errorf("page size: got %d want 50", len(msgs))
	}
	const budget = 1500 * time.Millisecond
	if elapsed > budget {
		t.Fatalf("GetChannelMessages too slow: %v (budget %v) on %d tx × %d obs",
			elapsed, budget, numTx, obsPerTx)
	}
}
