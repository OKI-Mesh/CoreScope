package main

import (
	"testing"
	"time"
)

// setScopeMatchCounters overwrites the process-wide counters. They are shared
// by every test that calls matchScopeName, so a test that touches them must
// put back what it found.
func setScopeMatchCounters(since, unique, explicit, ambiguous, none int64) {
	c := &scopeMatchCounters
	c.sinceUnix.Store(since)
	c.unique.Store(unique)
	c.explicitOverDerived.Store(explicit)
	c.ambiguous.Store(ambiguous)
	c.none.Store(none)
}

func resetScopeMatchCounters(t *testing.T) {
	t.Helper()
	c := &scopeMatchCounters
	since, unique, explicit, ambiguous, none :=
		c.sinceUnix.Load(), c.unique.Load(), c.explicitOverDerived.Load(), c.ambiguous.Load(), c.none.Load()
	t.Cleanup(func() { setScopeMatchCounters(since, unique, explicit, ambiguous, none) })
	setScopeMatchCounters(0, 0, 0, 0, 0)
}

func TestScopeMatchTotalsSurviveRestart(t *testing.T) {
	resetScopeMatchCounters(t)
	s := newTestStore(t)
	ctx := testCtx(t)

	since := time.Now().Add(-72 * time.Hour).Unix()
	const big = 3_000_000_000 // above int32, so a 32-bit column would fail here
	setScopeMatchCounters(since, big, 7, 3, 42)

	if err := s.SaveScopeMatchTotals(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveScopeMatchTotals(ctx); err != nil { // an upsert, not an append
		t.Fatal(err)
	}
	if n := countRows(t, s, "scope_match_totals"); n != 1 {
		t.Fatalf("rows = %d, want 1", n)
	}

	// A restart: the process counters are gone.
	setScopeMatchCounters(0, 0, 0, 0, 0)
	if err := s.LoadScopeMatchTotals(ctx); err != nil {
		t.Fatal(err)
	}
	c := &scopeMatchCounters
	if c.sinceUnix.Load() != since || c.unique.Load() != big ||
		c.explicitOverDerived.Load() != 7 || c.ambiguous.Load() != 3 || c.none.Load() != 42 {
		t.Errorf("counters not restored: since=%d unique=%d explicit=%d ambiguous=%d none=%d",
			c.sinceUnix.Load(), c.unique.Load(), c.explicitOverDerived.Load(), c.ambiguous.Load(), c.none.Load())
	}
}

func TestScopeMatchTotalsFirstRunAnchorsTheWindow(t *testing.T) {
	resetScopeMatchCounters(t)
	s := newTestStore(t)

	before := time.Now().Unix()
	if err := s.LoadScopeMatchTotals(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	if got := scopeMatchCounters.sinceUnix.Load(); got < before {
		t.Errorf("since = %d, want it anchored at or after %d", got, before)
	}
	if n := countRows(t, s, "scope_match_totals"); n != 1 {
		t.Errorf("rows = %d, want 1 (the first load stores the anchored zeroes)", n)
	}
}
