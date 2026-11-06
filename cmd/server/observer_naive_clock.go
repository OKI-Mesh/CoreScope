package main

import (
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
)

// observerNaiveClockWindow is the rolling window after which a recorded
// naive-clock skew event "decays" and the observer is no longer flagged in
// the UI. Read-time decay (no background sweep) keeps it cheap.
const observerNaiveClockWindow = 24 * time.Hour

// applyObserverNaiveClock populates the four clock_* fields on ObserverResp
// from the underlying Observer row, applying read-time decay: any event
// older than observerNaiveClockWindow is treated as absent so the chip and
// banner clear automatically without a background sweep.
//
// Issue #1478.
func applyObserverNaiveClock(resp *ObserverResponse, o *database.Observer, now time.Time) {
	if o.ClockLastNaiveAt == nil {
		return
	}
	if now.Sub(*o.ClockLastNaiveAt) > observerNaiveClockWindow {
		// Decayed — leave clock_naive=false and counters at zero. We
		// intentionally do NOT clear the underlying row here (server is
		// read-only; the next ingestor write or a future #1478 followup
		// vacuum can rewrite). The response just shows zero/null.
		return
	}
	resp.ClockNaive = true
	if o.ClockSkewSeconds != nil {
		v := int(*o.ClockSkewSeconds)
		resp.ClockSkewSeconds = &v
	}
	resp.ClockSkewCount24h = int(o.ClockSkewCount24h)
	ts := formatTS(*o.ClockLastNaiveAt)
	resp.ClockLastNaiveAt = &ts
}
