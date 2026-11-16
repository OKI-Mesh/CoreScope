package main

import (
	"context"
	"fmt"
	"log"
	"time"
)

// pruneBatchTransmissions bounds how many transmissions (and their child
// observations) one prune transaction deletes before it commits.
//
// Batching keeps each transaction small: it bounds the WAL written per commit,
// how long the deleted rows stay locked, and how much dead space autovacuum has
// to clean up at once, so a retention-day-sized delete is never one long
// transaction.
//
// The bound is on transmissions, but the work scales with the rows actually
// deleted, and each transmission carries an unbounded number of observations.
// At ~16 observations per transmission a batch is ~4k row deletes; an instance
// with a denser observation ratio gets a proportionally larger transaction from
// the same batch size. 250 also keeps the commit count low (a 16k-transmission
// day is 64 transactions, not 16k).
const pruneBatchTransmissions = 250

// PruneNeighborEdges deletes rows older than maxAgeDays from neighbor_edges.
// Owned by the ingestor per #1287 (was in cmd/server). Returns DB rows deleted.
func (s *Store) PruneNeighborEdges(ctx context.Context, maxAgeDays int) (int64, error) {
	if maxAgeDays <= 0 {
		return 0, nil
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -maxAgeDays)
	n, err := s.db.PruneNeighborEdges(ctx, cutoff)
	if err != nil {
		return 0, fmt.Errorf("prune neighbor_edges: %w", err)
	}
	if n > 0 {
		log.Printf("[neighbor-prune] removed %d DB rows older than %d days", n, maxAgeDays)
	}
	return n, nil
}
