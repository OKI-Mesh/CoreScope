package main

// Issue #1009 follow-up tests for PR #1596:
//
//   (A) LoadChunked must flip subpath + pathHop index ready flags
//       after building those indexes. Otherwise WaitIndexesReady (used
//       by StartRepeaterEnrichmentRecomputer at boot) blocks the
//       caller for up to repeaterEnrichmentPrewarmWait (60s), which is
//       why CI's "Start Go server" step times out before /api/healthz
//       can answer within its 30s deadline.
//
//   (B) LoadChunked must NOT report LoadComplete()==true when it
//       returns an error. Today a defer unconditionally calls
//       s.loadComplete.Store(true), so a failed load appears "ready"
//       to probes and the load-status middleware.

import (
	"testing"
)

// (A) Indexes must be marked ready by LoadChunked.
func TestLoadChunked_MarksIndexesReady(t *testing.T) {
	ctx := t.Context()
	store := openChunkedTestStore(t, 100)

	if store.SubpathIndexReady() || store.PathHopIndexReady() {
		t.Fatal("indexes must start NOT ready")
	}

	if err := store.LoadChunked(ctx, 50); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}

	if !store.SubpathIndexReady() {
		t.Fatal("SubpathIndexReady() must be true after LoadChunked builds the index")
	}
	if !store.PathHopIndexReady() {
		t.Fatal("PathHopIndexReady() must be true after LoadChunked builds the index")
	}
}

// (B) LoadChunked errors must not flip LoadComplete=true.
func TestLoadChunked_ErrorDoesNotMarkComplete(t *testing.T) {
	ctx := t.Context()
	store := openChunkedTestStore(t, 100)

	// Close the pool so the very first chunk query fails.
	store.db.Close()

	if err := store.LoadChunked(ctx, 50); err == nil {
		t.Fatal("LoadChunked must return an error when the DB query fails")
	}

	if store.LoadComplete() {
		t.Fatal("LoadComplete() must remain false after LoadChunked returns an error")
	}
}
