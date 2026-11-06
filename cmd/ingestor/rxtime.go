package main

import (
	"log"
	"time"
)

// Limits for trusting an observer-supplied envelope timestamp.
const (
	// UTC+14 is the largest standard offset, so nothing valid is further ahead.
	rxMaxFuture = 14 * time.Hour
	// Older than this is an RTC-reset node reporting a factory date (e.g.
	// 2020-01-01). Letting it through would drag transmissions.first_seen
	// backwards (the LEAST in the transmission upsert). No legitimate buffered
	// upload is this stale.
	rxMaxAge = 30 * 24 * time.Hour
	// A zone-less (naive) timestamp is parsed as if UTC, which leaves a
	// residual error equal to the observer's UTC offset. Anything further than
	// this from server time is that offset (or a broken clock), not a real
	// reading (#1463).
	rxNaiveTolerance = 15 * time.Minute
)

// rxReason says why resolveRxTimeAt returned the instant it did.
type rxReason int

const (
	rxFromEnvelope rxReason = iota // the envelope timestamp was used as-is
	rxMissing                      // no timestamp in the message
	rxUnparseable                  // timestamp present but not in a known format
	rxTooFarAhead                  // more than rxMaxFuture ahead: clock error
	rxTooOld                       // older than rxMaxAge: RTC reset
	rxNaiveClamped                 // zone-less value off server time by more than rxNaiveTolerance
	rxClampedToNow                 // zone-aware value slightly ahead of now
)

func (r rxReason) String() string {
	switch r {
	case rxFromEnvelope:
		return "envelope"
	case rxMissing:
		return "missing"
	case rxUnparseable:
		return "unparseable"
	case rxTooFarAhead:
		return "future"
	case rxTooOld:
		return "stale"
	case rxNaiveClamped:
		return "naive-clamped"
	case rxClampedToNow:
		return "clamped-to-now"
	default:
		return "unknown"
	}
}

// rxResolution is the instant to record for a packet and why.
type rxResolution struct {
	At     time.Time
	Reason rxReason
	// SkewSec is the signed observer clock offset (negative = behind UTC,
	// positive = ahead). Nonzero only when Reason is rxNaiveClamped; callers
	// feed it to RecordNaiveSkew (#1478).
	SkewSec int64
}

// resolveRxTimeAt decides which instant to store as a packet's receive time.
// It is pure: now is injected and nothing is logged, so every boundary can be
// tested exactly. Rules apply in this order:
//
//  1. no timestamp                        -> now
//  2. unparseable                         -> now
//  3. more than rxMaxFuture ahead         -> now
//  4. older than rxMaxAge                 -> now
//  5. zone-less and off by > tolerance    -> now, with the signed skew reported
//  6. zone-aware but slightly ahead of now -> now (observer clock skew)
//  7. otherwise                           -> the envelope time, in UTC
//
// Zone-aware values (Z or an explicit offset) are never clamped by rule 5, so
// legitimate buffered uploads stay accurate whatever the skew.
func resolveRxTimeAt(msg map[string]interface{}, now time.Time) rxResolution {
	raw, _ := msg["timestamp"].(string)
	if raw == "" {
		return rxResolution{At: now, Reason: rxMissing}
	}
	t, naive, err := parseEnvelopeTime(raw)
	if err != nil {
		return rxResolution{At: now, Reason: rxUnparseable}
	}
	if t.After(now.Add(rxMaxFuture)) {
		return rxResolution{At: now, Reason: rxTooFarAhead}
	}
	if t.Before(now.Add(-rxMaxAge)) {
		return rxResolution{At: now, Reason: rxTooOld}
	}
	if naive {
		if signed := t.Sub(now); signed > rxNaiveTolerance || signed < -rxNaiveTolerance {
			return rxResolution{At: now, Reason: rxNaiveClamped, SkewSec: int64(signed / time.Second)}
		}
	}
	if t.After(now) {
		return rxResolution{At: now, Reason: rxClampedToNow}
	}
	return rxResolution{At: t.UTC(), Reason: rxFromEnvelope}
}

// resolveRxTimeCore keeps the existing (time, skew) signature for current
// callers. It owns the logging the pure function no longer does. The naive and
// soft-clamp cases stay silent: the UI chip and banner replace the per-message
// log (#1479).
func resolveRxTimeCore(msg map[string]interface{}, tag string) (time.Time, int64) {
	r := resolveRxTimeAt(msg, time.Now().UTC())
	switch r.Reason {
	case rxUnparseable, rxTooFarAhead, rxTooOld:
		log.Printf("MQTT [%s] %s timestamp %q, using ingest time", tag, r.Reason, msg["timestamp"])
	}
	return r.At, r.SkewSec
}
