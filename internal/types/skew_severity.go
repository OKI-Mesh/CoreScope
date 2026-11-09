package types

// SkewSeverity classifies how far a node's clock is from real time.
type SkewSeverity string

const (
	SkewOK           SkewSeverity = "ok"            // < 5 min
	SkewWarning      SkewSeverity = "warning"       // 5 min – 1 hour
	SkewCritical     SkewSeverity = "critical"      // 1 hour – 30 days
	SkewAbsurd       SkewSeverity = "absurd"        // 30 days – 365 days
	SkewNoClock      SkewSeverity = "no_clock"      // >= 365 days — uninitialized RTC
	SkewBimodalClock SkewSeverity = "bimodal_clock" // mixed good+bad recent samples (flaky RTC)
)

const (
	skewThresholdWarnSec     = 5 * 60
	skewThresholdCriticalSec = 60 * 60
	skewThresholdAbsurdSec   = 30 * 24 * 3600
	skewThresholdNoClockSec  = 365 * 24 * 3600
)

// ClassifySkew maps absolute skew in seconds to a severity. It never returns
// SkewBimodalClock: that one comes from the mix of recent samples, not from a
// single skew value.
// Float64 comparison is safe: inputs are rounded to 1 decimal by the caller,
// and thresholds are integer multiples of 60 — no rounding artifacts.
func ClassifySkew(absSkewSec float64) SkewSeverity {
	switch {
	case absSkewSec >= skewThresholdNoClockSec:
		return SkewNoClock
	case absSkewSec >= skewThresholdAbsurdSec:
		return SkewAbsurd
	case absSkewSec >= skewThresholdCriticalSec:
		return SkewCritical
	case absSkewSec >= skewThresholdWarnSec:
		return SkewWarning
	default:
		return SkewOK
	}
}

// HasUnreliableClock reports whether the node's clock is so wrong or so
// inconsistent that drift and skew trends are meaningless.
func (s SkewSeverity) HasUnreliableClock() bool {
	return s == SkewNoClock || s == SkewBimodalClock
}
