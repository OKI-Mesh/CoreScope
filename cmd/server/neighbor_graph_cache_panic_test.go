package main

import (
	"sync/atomic"
	"testing"

	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// #1483 follow-up: a panic inside recomputeNeighborGraphCache must NOT
// kill the goroutine but MUST increment the rebuild-failure counter so
// operators see the failure on /api/stats.
func TestNeighborGraphCacheRebuildPanicIncrementsCounter(t *testing.T) {
	ctx := t.Context()
	s := &Server{
		computeNeighborGraphResponseFn: func(minCount int, minScore float64, region string, role types.NodeRole) NeighborGraphResponse {
			panic("intentional test panic")
		},
	}
	before := atomic.LoadUint64(&s.neighborGraphCacheRebuildFailures)
	s.recomputeNeighborGraphCache(ctx)
	after := atomic.LoadUint64(&s.neighborGraphCacheRebuildFailures)
	if after != before+1 {
		t.Fatalf("expected rebuild-failure counter to increment by 1, before=%d after=%d", before, after)
	}
}
