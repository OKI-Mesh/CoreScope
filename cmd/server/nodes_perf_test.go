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

// TestHandleNodesPerfLargeFleet asserts the /api/nodes endpoint (no `limit`
// param — relying on the server-side default) returns in well under 2s on a
// realistic-shape fleet: 600 nodes, ~50 of them repeaters/rooms with rich
// path-hop activity, and a non-trivial byPayloadType + byPathHop index.
//
// Regression guard for issue #1257:
//
//	/api/nodes (no limit) → 32.9s, 30KB on staging (637 nodes)
//	/api/nodes?limit=2000 → 4.9s,  360KB
//
// Root cause class: per-repeater enrichment in handleNodes calls
// store.GetRepeaterRelayInfo + GetRepeaterUsefulnessScore separately for
// each node in the page. Each call takes its own RLock and walks
// byPathHop[pk] / byPayloadType, doing expensive timestamp parsing.
// For the default-page case (top-50 by last_seen, mostly hot repeaters)
// that is hundreds of thousands of timestamp parses per request.
//
// Budget: 2s. On the broken implementation with this fixture the
// endpoint blows the budget; with batched/cached per-page enrichment it
// completes in well under 500ms.
func TestHandleNodesPerfLargeFleet(t *testing.T) {
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

	{
		req := httptest.NewRequest("GET", "/api/nodes?limit=1", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("warmup status=%d body=%s", w.Code, w.Body.String())
		}
	}

	start := time.Now()
	req := httptest.NewRequest("GET", "/api/nodes", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	elapsed := time.Since(start)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}

	const budget = 2 * time.Second
	t.Logf("/api/nodes (no limit) elapsed=%v on %d nodes, %d tx", elapsed, 600, numTx)
	if elapsed > budget {
		t.Fatalf("/api/nodes (no limit) too slow for #1257: %v (budget %v) on %d nodes, %d tx",
			elapsed, budget, 600, numTx)
	}
}
