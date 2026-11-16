package main

import (
	"context"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
)

// The scope-match tally lives in scopeMatchCounters, four atomics in this
// process and nowhere else. Every restart — a deploy, a crash, an operator
// bouncing the container — sets them back to zero and takes the container's
// log with it, so the previous count is not recoverable afterwards. That is
// how a finished 24h ambiguity measurement was lost.
//
// The numbers only say something over days: "ambiguous is 0.4% of matches"
// needs a denominator large enough to be stable, and the decision they gate
// (whether a collision tie-break is worth building) is not one to take on an
// hour of traffic. So they have to outlive the process doing the counting.
//
// scope_match_totals is one row. The ingestor reads it into the counters at
// startup and writes them back periodically and on shutdown, which makes the
// tally cumulative since counting_since instead of since boot. The server
// neither reads nor needs this table.
//
// The final save on shutdown must use a fresh, bounded context, not the root
// one: by the time shutdown runs the root context is already cancelled, so a
// save with it fails and loses everything since the last periodic save.

// LoadScopeMatchTotals seeds the in-process counters from the stored row so
// counting continues where the previous process stopped. On a DB that has no
// row yet it anchors `since` at now and stores the zeroes, so the very first
// window has a start time too — without it, a tally read after one restart
// would have no honest denominator.
func (s *Store) LoadScopeMatchTotals(ctx context.Context) error {
	t, err := s.db.GetScopeMatchTotals(ctx)
	if err != nil {
		return err
	}
	if t == nil {
		scopeMatchCounters.sinceUnix.Store(time.Now().Unix())
		return s.SaveScopeMatchTotals(ctx)
	}
	scopeMatchCounters.sinceUnix.Store(t.Since.Unix())
	scopeMatchCounters.unique.Store(t.Unique)
	scopeMatchCounters.explicitOverDerived.Store(t.ExplicitOverDerived)
	scopeMatchCounters.ambiguous.Store(t.Ambiguous)
	scopeMatchCounters.none.Store(t.None)
	return nil
}

// SaveScopeMatchTotals writes the current counters to the single row. Each
// atomic is loaded independently, so a save taken under live traffic can miss
// a match that lands mid-write; the next save picks it up. The tally is a
// long-window ratio, not an audit trail.
func (s *Store) SaveScopeMatchTotals(ctx context.Context) error {
	return s.db.SaveScopeMatchTotals(ctx, database.ScopeMatchTotals{
		Since:               time.Unix(scopeMatchCounters.sinceUnix.Load(), 0).UTC(),
		Unique:              scopeMatchCounters.unique.Load(),
		ExplicitOverDerived: scopeMatchCounters.explicitOverDerived.Load(),
		Ambiguous:           scopeMatchCounters.ambiguous.Load(),
		None:                scopeMatchCounters.none.Load(),
	}, time.Now().UTC())
}
