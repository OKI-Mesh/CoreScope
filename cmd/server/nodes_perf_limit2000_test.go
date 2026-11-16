package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// TestHandleNodesLimit2000ColdMiss is a regression guard for issue #1262.
//
// Background: PR #1260 added a 15s-TTL bulk-cache for repeater
// enrichment in handleNodes (GetRepeaterRelayInfoMap /
// GetRepeaterUsefulnessScoreMap). On warm hits the request is ~40ms.
// On the very first request after server startup (or after the 15s TTL
// expires) the cache rebuild runs on the request-serving goroutine and
// is O(byPathHop + parsed timestamps). On staging (75k tx, 600 nodes)
// the cold rebuild took 15.7s.
//
// /api/nodes?limit=2000 is the SPA's hop-resolver bootstrap call (see
// public/live.js) so EVERY cold SPA load eats the cold-rebuild cost.
//
// Acceptance: /api/nodes?limit=2000 must return in <2s on a
// realistic-shape fleet WITHOUT a prior warmup request — i.e. once the
// store has been initialized and the steady-state repeater-enrichment
// recomputer prewarm has run.
func TestHandleNodesLimit2000ColdMiss(t *testing.T) {
	if testing.Short() {
		t.Skip("perf test")
	}
	ctx := t.Context()
	srv, router := setupTestServer(t)
	now := time.Now().UTC()

	repKey := func(i int) string { return scopeKey(0xa0000 + i) }
	compKey := func(i int) string { return scopeKey(0xb0000 + i) }

	for i := 0; i < 50; i++ {
		if err := srv.db.CreateOrUpdateNode(ctx, database.NodeInput{
			PublicKey: repKey(i),
			Name:      fmt.Sprintf("rep%d", i),
			Role:      "repeater",
			RxTime:    now.Add(-time.Duration(i) * time.Minute),
			IngestNow: now,
		}); err != nil {
			t.Fatalf("seed repeater %d: %v", i, err)
		}
	}
	for i := 0; i < 550; i++ {
		if err := srv.db.CreateOrUpdateNode(ctx, database.NodeInput{
			PublicKey: compKey(i),
			Name:      fmt.Sprintf("comp%d", i),
			Role:      "companion",
			RxTime:    now.Add(-time.Duration(60+i) * time.Minute),
			IngestNow: now,
		}); err != nil {
			t.Fatalf("seed companion %d: %v", i, err)
		}
	}
	srv.store.RefreshNodeCache(ctx)

	const numTx = 150000
	const hopsPerTx = 6
	pt2 := types.PayloadType(2)
	store := srv.store
	for i := 0; i < numTx; i++ {
		txID := 100000 + i
		stx := &StoreTx{
			ID:          txID,
			Hash:        fmt.Sprintf("h%d", txID),
			FirstSeen:   now.Add(-time.Duration(i) * time.Second),
			PayloadType: &pt2,
		}
		store.byPayloadType[pt2] = append(store.byPayloadType[pt2], stx)
		store.byPathHop["pk"] = append(store.byPathHop["pk"], stx)
		for h := 0; h < hopsPerTx; h++ {
			pk := repKey((i + h) % 50)
			store.byPathHop[pk] = append(store.byPathHop[pk], stx)
		}
	}

	stop := store.StartRepeaterEnrichmentRecomputer(24, 5*time.Minute)
	defer stop()

	start := time.Now()
	req := httptest.NewRequest("GET", "/api/nodes?limit=2000", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	elapsed := time.Since(start)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}

	const budget = 2 * time.Second
	t.Logf("/api/nodes?limit=2000 elapsed=%v on %d nodes, %d tx", elapsed, 600, numTx)
	if elapsed > budget {
		t.Fatalf("/api/nodes?limit=2000 cold-miss too slow for #1262: %v (budget %v) on %d nodes, %d tx",
			elapsed, budget, 600, numTx)
	}
}
