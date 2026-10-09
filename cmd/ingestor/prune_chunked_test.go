package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// seedAgedTransmissions bulk-inserts n transmissions stamped ageDays in the
// past, each with obsPerTx observations that each carry one hop row. It uses
// generate_series, not per-row inserts: a batch-sized fixture is tens of
// thousands of rows. Kept local to this file so the batching tests own their
// fixture shape. Adjust the column lists if your schema has other NOT NULL
// columns.
func seedAgedTransmissions(t *testing.T, s *Store, n, obsPerTx, ageDays int) {
	t.Helper()
	at := time.Now().UTC().AddDate(0, 0, -ageDays)
	prefix := fmt.Sprintf("a%d-", ageDays) // unique per age, so two calls never collide on hash

	testExec(t, s, `INSERT INTO transmissions
			(raw_hex, hash, first_seen, route_type, payload_type, payload_version, decoded_json)
		SELECT 'AA', $1::text || g::text, $2::timestamptz, 0, 1, 1, '{}'
		FROM generate_series(1, $3::int) AS g`, prefix, at, n)

	// observer_idx stays NULL: it is a foreign key to observers, and NULLs never
	// collide in the dedup index, so several observations per transmission are fine.
	testExec(t, s, `INSERT INTO observations
			(transmission_id, observer_idx, direction, snr, rssi, score, timestamp)
		SELECT t.id, NULL, 'rx', 1.0, -100, 0, $2::timestamptz
		FROM transmissions t CROSS JOIN generate_series(1, $3::int) AS j
		WHERE t.hash LIKE $1::text || '%'`, prefix, at, obsPerTx)

	testExec(t, s, `INSERT INTO observation_path_hops (observation_id, hop_index, prefix)
		SELECT o.id, 0, 'aa'
		FROM observations o JOIN transmissions t ON t.id = o.transmission_id
		WHERE t.hash LIKE $1::text || '%'`, prefix)
}

// ---- The batching loop, with no database ----

// TestPruneInBatches pins the loop's contract. Batching still matters on
// Postgres: it bounds transaction size and WAL volume, and how long vacuum
// waits to reclaim the rows. The number of calls is the deterministic proxy:
// N calls means no single statement ever deleted more than one batch.
func TestPruneInBatches(t *testing.T) {
	const batch = 100

	// fake simulates a table holding `rows` aged rows: each call removes up to limit.
	fake := func(rows int64, calls *int) pruneBatchFn {
		return func(_ context.Context, _ time.Time, limit int32) (int64, error) {
			*calls++
			n := min(rows, int64(limit))
			rows -= n
			return n, nil
		}
	}

	for _, c := range []struct {
		name        string
		rows        int64
		wantBatches int
	}{
		{"nothing aged: one empty batch", 0, 1},
		{"less than a batch: one short batch ends the loop", 37, 1},
		{"exactly one batch: a second, empty batch proves nothing is left", batch, 2},
		{"two full batches plus a partial: no terminating empty batch", 2*batch + 37, 3},
		{"exact multiple: the last batch is full, so one more runs empty", 2 * batch, 3},
	} {
		t.Run(c.name, func(t *testing.T) {
			calls := 0
			total, batches, err := pruneInBatches(t.Context(), time.Now(), batch, fake(c.rows, &calls))
			if err != nil {
				t.Fatal(err)
			}
			if total != c.rows {
				t.Errorf("total = %d, want %d", total, c.rows)
			}
			if batches != c.wantBatches || calls != c.wantBatches {
				t.Errorf("batches = %d (calls %d), want %d", batches, calls, c.wantBatches)
			}
		})
	}

	t.Run("an error returns what the earlier batches already deleted", func(t *testing.T) {
		calls := 0
		total, batches, err := pruneInBatches(t.Context(), time.Now(), batch,
			func(context.Context, time.Time, int32) (int64, error) {
				calls++
				if calls == 2 {
					return 0, errors.New("boom")
				}
				return batch, nil
			})
		// Those rows are gone, so reporting 0 would be wrong.
		if err == nil || total != batch || batches != 1 {
			t.Fatalf("total/batches/err = %d/%d/%v, want %d/1/boom", total, batches, err, batch)
		}
	})
}

// ---- The real SQL ----

// TestPruneBatchesRunAgainstRealSQL runs the loop with the real batch statement
// and a small batch, so a fixture of a few hundred rows crosses several batches.
func TestPruneBatchesRunAgainstRealSQL(t *testing.T) {
	s := newTestStore(t)
	ctx := testCtx(t)
	const batch = 50
	const aged = batch*2 + 37 // two full batches and a partial one
	seedAgedTransmissions(t, s, aged, 2, 10)

	calls := 0
	total, batches, err := pruneInBatches(ctx, time.Now().UTC().AddDate(0, 0, -5), batch,
		func(ctx context.Context, cutoff time.Time, limit int32) (int64, error) {
			calls++
			return s.db.PruneTransmissionsBatch(ctx, cutoff, limit)
		})
	if err != nil {
		t.Fatal(err)
	}
	if total != aged || batches != 3 || calls != 3 {
		t.Fatalf("total/batches = %d/%d, want %d/3 (the delete must be chunked)", total, batches, aged)
	}
	// One statement removes a batch's hops, observations and transmissions together.
	for _, tbl := range []string{"transmissions", "observations", "observation_path_hops"} {
		if n := countRows(t, s, tbl); n != 0 {
			t.Errorf("%s has %d rows left, want 0", tbl, n)
		}
	}
}

// TestPruneOldPacketsSpansBatchesAndKeepsFreshRows covers the correctness risk
// the batching introduces: with a LIMIT on the batch, a cutoff that straddles
// several batches must still delete every aged row and no fresh one.
func TestPruneOldPacketsSpansBatchesAndKeepsFreshRows(t *testing.T) {
	s := newTestStore(t)
	ctx := testCtx(t)
	const batch = 50
	const aged = batch + 11
	const fresh = 17
	const obsPerTx = 3
	seedAgedTransmissions(t, s, aged, obsPerTx, 10)
	seedAgedTransmissions(t, s, fresh, obsPerTx, 0)

	n, err := s.pruneOldPackets(ctx, 5, batch)
	if err != nil {
		t.Fatal(err)
	}
	if n != aged {
		t.Fatalf("pruned %d, want %d", n, aged)
	}
	if got := countRows(t, s, "transmissions"); got != fresh {
		t.Errorf("transmissions = %d, want %d fresh kept", got, fresh)
	}
	if got := countRows(t, s, "observations"); got != fresh*obsPerTx {
		t.Errorf("observations = %d, want %d", got, fresh*obsPerTx)
	}
	// The foreign keys make orphans impossible: a statement that skipped the hop
	// delete would fail outright. The hop count proves the aged hops are gone and
	// the fresh ones are untouched.
	if got := countRows(t, s, "observation_path_hops"); got != fresh*obsPerTx {
		t.Errorf("observation_path_hops = %d, want %d", got, fresh*obsPerTx)
	}
}

// TestPruneOldPacketsDisabledDeletesNothing pins the 0/negative short-circuit.
// The rows are 10 days old: without the short-circuit, days=0 means a cutoff of
// now and days=-1 a cutoff in the future, so either would delete them.
func TestPruneOldPacketsDisabledDeletesNothing(t *testing.T) {
	s := newTestStore(t)
	seedAgedTransmissions(t, s, 5, 1, 10)

	for _, days := range []int{0, -1} {
		n, err := s.PruneOldPackets(testCtx(t), days)
		if err != nil {
			t.Fatalf("PruneOldPackets(%d): %v", days, err)
		}
		if n != 0 {
			t.Fatalf("PruneOldPackets(%d) = %d, want 0", days, n)
		}
	}
	if got := countRows(t, s, "transmissions"); got != 5 {
		t.Fatalf("transmissions = %d, want 5 untouched", got)
	}
}

// TestPruneOldPacketsNothingAgedKeepsEverything is the steady state when
// nothing has aged out yet: one empty batch, then the loop exits.
func TestPruneOldPacketsNothingAgedKeepsEverything(t *testing.T) {
	s := newTestStore(t)
	seedAgedTransmissions(t, s, 9, 2, 0)

	n, err := s.PruneOldPackets(testCtx(t), 5)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("pruned %d, want 0", n)
	}
	if got := countRows(t, s, "transmissions"); got != 9 {
		t.Fatalf("transmissions = %d, want 9 kept", got)
	}
}

// TestPruneBatchSelectCanUseFirstSeenIndex pins the plan of the batch select.
// The steady-state, nothing-left batch is cheap only if it walks
// idx_transmissions_first_seen instead of scanning the table. The ORDER BY
// includes id as a tiebreaker, so Postgres may add an incremental sort over the
// index for rows sharing a first_seen. That is expected and cheap with a LIMIT.
// A composite (first_seen, id) index would remove it, if a real plan ever
// shows a large sort.
//
// The SQL is a copy of the batch CTE's select in the .sql file, so keep the two
// in step. This proves the index CAN serve the query, not that the planner
// chooses it on production data (the post-cutover EXPLAIN ANALYZE covers that).
func TestPruneBatchSelectCanUseFirstSeenIndex(t *testing.T) {
	s := newTestStore(t)
	plan := explainPlan(t, s, `SELECT id FROM transmissions
		WHERE first_seen < '2026-01-01T00:00:00Z'::timestamptz
		ORDER BY first_seen, id LIMIT 10000`)
	if !strings.Contains(plan, "idx_transmissions_first_seen") {
		t.Errorf("batch select should be able to use idx_transmissions_first_seen, plan was:\n%s", plan)
	}
	if strings.Contains(plan, "Seq Scan on transmissions") {
		t.Errorf("batch select should not need a full scan, plan was:\n%s", plan)
	}
}
