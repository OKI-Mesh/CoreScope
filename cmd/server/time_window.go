package main

import (
	"net/http"
	"time"
)

// Label is a stable identifier for the user-requested window
// (e.g. "24h"). For relative windows it is the original alias; for
// absolute ranges it is empty (Since/Until are already stable).
// Used only for cache keying so that "?window=24h" produces a single
// cache entry instead of one per second.
type TimeWindow struct {
	Since time.Time // zero = unbounded
	Until time.Time // zero = unbounded
	Label string
}

// IsZero reports whether the window has no bounds at all.
func (w TimeWindow) IsZero() bool {
	return w.Since.IsZero() && w.Until.IsZero()
}

// CacheKey returns a deterministic key suitable for analytics caches.
// For relative windows the key is the alias label so that the cache
// remains stable across the wall-clock advancing.
func (w TimeWindow) CacheKey() string {
	if w.IsZero() {
		return ""
	}
	if w.Label != "" {
		return "rel:" + w.Label
	}
	return cacheKeyBound(w.Since) + "|" + cacheKeyBound(w.Until)
}

// cacheKeyBound renders one window bound for a cache key: empty when
// unbounded, otherwise UTC with full precision so two windows that differ
// by a millisecond never share a key.
func cacheKeyBound(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// Includes reports whether t falls inside the window: Since is inclusive,
// Until is exclusive, zero bounds are unbounded, and a zero timestamp is included.
func (w TimeWindow) Includes(t time.Time) bool {
	if t.IsZero() {
		return true
	}
	if !w.Since.IsZero() && t.Before(w.Since) {
		return false
	}
	if !w.Until.IsZero() && !t.Before(w.Until) {
		return false
	}
	return true
}

// ParseTimeWindow extracts a TimeWindow from query params.
//
// Supported parameters:
//
//	?window=1h | 24h | 7d | 30d   — relative window ending "now"
//	?from=<RFC3339>&to=<RFC3339>  — absolute custom range (either bound optional)
//
// When neither is set, returns the zero TimeWindow (unbounded; original behavior).
// Invalid values are silently ignored to preserve backwards compatibility.
func ParseTimeWindow(r *http.Request) TimeWindow {
	q := r.URL.Query()

	// Absolute range takes precedence if either bound is set.
	from := q.Get("from")
	to := q.Get("to")
	if from != "" || to != "" {
		w := TimeWindow{}
		if from != "" {
			if t, err := time.Parse(time.RFC3339, from); err == nil {
				w.Since = t.UTC().Truncate(time.Second)
			}
		}
		if to != "" {
			if t, err := time.Parse(time.RFC3339, to); err == nil {
				w.Until = t.UTC().Truncate(time.Second)
			}
		}
		return w
	}

	// Relative window.
	if win := q.Get("window"); win != "" {
		var d time.Duration
		switch win {
		case "1h":
			d = 1 * time.Hour
		case "24h", "1d":
			d = 24 * time.Hour
		case "3d":
			d = 3 * 24 * time.Hour
		case "7d", "1w":
			d = 7 * 24 * time.Hour
		case "30d":
			d = 30 * 24 * time.Hour
		default:
			// Unknown values are silently ignored — backwards compatible.
			return TimeWindow{}
		}
		since := time.Now().UTC().Add(-d).Truncate(time.Second)
		return TimeWindow{Since: since, Label: win}
	}

	return TimeWindow{}
}
