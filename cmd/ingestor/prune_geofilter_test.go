package main

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/prunequeue"
)

func TestRunPendingPruneRequests(t *testing.T) {
	store := newTestStore(t)
	store.pruneQueuePath = filepath.Join(t.TempDir(), "prunequeue")
	defer store.Close()

	// Seed two nodes; one will be pruned, one will be kept.
	count := testExec(t, store, `INSERT INTO nodes (public_key, name, role, lat, lon, last_seen, first_seen)
		VALUES ('aaaa', 'gone', 'companion', 1.0, 1.0, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
		       ('bbbb', 'kept', 'companion', 2.0, 2.0, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`)
	if count != 2 {
		t.Fatalf("expected 2 nodes, got %d", count)
	}

	id := prunequeue.NewID()
	if err := prunequeue.WriteRequest(store.pruneQueuePath, prunequeue.Request{
		ID:          id,
		RequestedAt: time.Now().UTC(),
		Reason:      "geo-prune-test",
		Pubkeys:     []string{"aaaa"},
	}); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}

	store.RunPendingPruneRequests(t.Context())

	// Request file gone, result file present.
	if exists, _ := prunequeue.RequestExists(store.pruneQueuePath, id); exists {
		t.Error("request file should have been consumed")
	}
	res, err := prunequeue.ReadResult(store.pruneQueuePath, id)
	if err != nil || res == nil {
		t.Fatalf("ReadResult: res=%v err=%v", res, err)
	}
	if res.Deleted != 1 {
		t.Errorf("expected Deleted=1, got %d", res.Deleted)
	}
	if res.Error != "" {
		t.Errorf("unexpected error: %s", res.Error)
	}

	// Verify DB state: aaaa gone, bbbb kept.
	n := countWhere(t, store, "nodes", "public_key='aaaa'")
	if n != 0 {
		t.Errorf("expected 'aaaa' deleted, got count=%d", n)
	}
	n = countWhere(t, store, "nodes", "public_key='bbbb'")
	if n != 1 {
		t.Errorf("expected 'bbbb' kept, got count=%d", n)
	}
}

func TestRunPendingPruneRequests_EmptyQueueIsNoop(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	store.pruneQueuePath = filepath.Join(t.TempDir(), "prunequeue")
	defer store.Close()
	// Must not panic / error on empty queue.
	store.RunPendingPruneRequests(ctx)
}
