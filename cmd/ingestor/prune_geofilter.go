// Package main: ingestor-side processor for prune-request marker files
// written by the read-only server (see internal/prunequeue).
//
// The server cannot DELETE because it opens SQLite mode=ro (#1283/#1289).
// Instead, the server writes request-<id>.json under <dataDir>/prune-requests/
// and the ingestor consumes it here.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/prunequeue"
)

type pruneBatchFn func(ctx context.Context, cutoff time.Time, limit int32) (int64, error)

// pruneInBatches calls fn until a short batch proves nothing is left below the
// cutoff. On error it returns what the earlier batches already deleted, since
// those rows are gone and reporting 0 would be wrong.
func pruneInBatches(ctx context.Context, cutoff time.Time, batch int32, fn pruneBatchFn) (total int64, batches int, err error) {
	for {
		n, err := fn(ctx, cutoff, batch)
		if err != nil {
			return total, batches, err
		}
		batches++
		total += n
		if n < int64(batch) {
			return total, batches, nil
		}
	}
}

// DeleteNodesByPubkeys deletes nodes by public key and returns how many nodes
// were removed. Their neighbor_edges go with them, because edges reference
// nodes by foreign key. Observation and transmission history that mentions
// the pubkeys is retained for audit and ages out through the regular packet
// retention prune (observation_path_hops.resolved_pubkey is deliberately not
// cleared). Only the ingestor calls this (the server has no write handle).
func (s *Store) DeleteNodesByPubkeys(ctx context.Context, pubkeys []string) (int64, error) {
	if len(pubkeys) == 0 {
		return 0, nil
	}
	keys := make([]string, len(pubkeys))
	for i, pk := range pubkeys {
		keys[i] = strings.ToLower(pk) // nodes.public_key is stored lowercase
	}
	n, err := s.db.DeleteNodesByPubkeys(ctx, keys)
	if err != nil {
		return 0, fmt.Errorf("delete nodes: %w", err)
	}
	return n, nil
}

// RunPendingPruneRequests scans the prune-requests/ directory next to the
// SQLite database and processes any request-<id>.json markers written by
// the server. Each request is honored verbatim — the server is responsible
// for the TOCTOU snapshot (only pubkeys that were still outside the
// geofilter at confirm time). After running DELETE, the ingestor writes
// result-<id>.json and removes the request file (atomic, via os.Rename in
// prunequeue.WriteResult).
//
// Safe to call from a ticker — no-op when the queue is empty.
func (s *Store) RunPendingPruneRequests(ctx context.Context) {
	paths, err := prunequeue.ListPending(s.pruneQueuePath)
	if err != nil {
		log.Printf("[prune-queue] list pending failed: %v", err)
		return
	}
	if len(paths) == 0 {
		return
	}
	for _, p := range paths {
		req, err := prunequeue.ReadRequest(p)
		if err != nil {
			log.Printf("[prune-queue] read %s failed: %v — removing", p, err)
			_ = os.Remove(p)
			continue
		}
		log.Printf("[prune-queue] processing request %s: %d pubkey(s) (%s)",
			req.ID, len(req.Pubkeys), req.Reason)
		start := time.Now()
		deleted, derr := s.DeleteNodesByPubkeys(ctx, req.Pubkeys)
		res := prunequeue.Result{
			ID:          req.ID,
			RequestedAt: req.RequestedAt,
			CompletedAt: time.Now().UTC(),
			Deleted:     deleted,
		}
		if derr != nil {
			res.Error = derr.Error()
			log.Printf("[prune-queue] request %s FAILED after %s: %v", req.ID, time.Since(start), derr)
		} else {
			log.Printf("[prune-queue] request %s deleted %d node(s) in %s", req.ID, deleted, time.Since(start))
		}
		if werr := prunequeue.WriteResult(s.pruneQueuePath, res); werr != nil {
			log.Printf("[prune-queue] write result for %s failed: %v", req.ID, werr)
		}
	}
}

func (s *Store) PruneOldPackets(ctx context.Context, days int) (int64, error) {
	return s.pruneOldPackets(ctx, days, pruneBatchTransmissions)
}

// pruneOldPackets takes the batch size as a parameter so tests can cross several
// batches with a few hundred rows instead of tens of thousands.
func (s *Store) pruneOldPackets(ctx context.Context, days int, batch int32) (int64, error) {
	if days <= 0 {
		return 0, nil
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -days)
	total, _, err := pruneInBatches(ctx, cutoff, batch, s.db.PruneTransmissionsBatch)
	if err != nil {
		return total, err
	}
	if total > 0 {
		log.Printf("[prune] deleted %d transmissions older than %d days", total, days)
	}
	return total, nil
}
