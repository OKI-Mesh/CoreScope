package main

import (
	"testing"
	"time"
)

// TestIngestorPruneOldPackets enforces #1283: the writer for
// transmissions retention lives on the ingestor's *Store. Before the fix,
// this lived on cmd/server/*DB and raced with ingestor INSERTs. After
// the fix, ingestor owns it and runs it on its own write-locked handle.
func TestIngestorPruneOldPackets(t *testing.T) {
	store := newTestStore(t)
	defer store.Close()
	ctx := t.Context()

	old := time.Now().UTC().AddDate(0, 0, -10).Format(time.RFC3339)
	new := time.Now().UTC().Format(time.RFC3339)
	for i, ts := range []string{old, old, new} {

		rowcount := testExec(t, store,
			`INSERT INTO transmissions (raw_hex, hash, first_seen, route_type, payload_type, payload_version, decoded_json)
			 VALUES ($1, $2, $3, 0, 1, 1, '{}')`,
			"AA", "h"+string(rune('a'+i)), ts,
		)
		if rowcount != 1 {
			t.Fatalf("expected 1 row inserted, got %d", rowcount)
		}
	}

	n, err := store.PruneOldPackets(ctx, 5)
	if err != nil {
		t.Fatalf("PruneOldPackets: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 pruned, got %d", n)
	}

	remaining := countRows(t, store, "transmissions")
	if remaining != 1 {
		t.Fatalf("expected 1 transmission remaining, got %d", remaining)
	}
}
