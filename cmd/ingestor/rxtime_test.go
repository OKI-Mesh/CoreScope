package main

import (
	"testing"
	"time"
)

func TestParseEnvelopeTime(t *testing.T) {
	cases := []struct {
		name      string
		in        string
		ok        bool
		wantNaive bool
	}{
		{"rfc3339 utc", "2026-05-16T10:00:00Z", true, false},
		{"rfc3339 offset", "2026-05-16T12:00:00+02:00", true, false},
		{"naive iso", "2026-05-16T10:00:00", true, true},
		{"naive iso micros", "2026-05-16T10:00:00.123456", true, true},
		{"garbage", "not-a-time", false, false},
		{"empty", "", false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, naive, err := parseEnvelopeTime(c.in)
			if (err == nil) != c.ok {
				t.Fatalf("parseEnvelopeTime(%q): want ok=%v, got err=%v", c.in, c.ok, err)
			}
			if err == nil && naive != c.wantNaive {
				t.Fatalf("parseEnvelopeTime(%q): want naive=%v, got %v", c.in, c.wantNaive, naive)
			}
		})
	}
}

func TestResolveRxTimeAt(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	zoned := func(d time.Duration) map[string]interface{} {
		return map[string]interface{}{"timestamp": now.Add(d).Format(time.RFC3339Nano)}
	}
	offsetZone := time.FixedZone("PDT", -7*3600)
	offset := func(d time.Duration) map[string]interface{} {
		return map[string]interface{}{"timestamp": now.Add(d).In(offsetZone).Format(time.RFC3339)}
	}
	naive := func(d time.Duration) map[string]interface{} {
		return map[string]interface{}{"timestamp": now.Add(d).Format("2006-01-02T15:04:05.000000")}
	}

	cases := []struct {
		name    string
		msg     map[string]interface{}
		reason  rxReason
		at      time.Time // zero means "expect now"
		skewSec int64
	}{
		{"missing", map[string]interface{}{}, rxMissing, time.Time{}, 0},
		{"garbage", map[string]interface{}{"timestamp": "not a time"}, rxUnparseable, time.Time{}, 0},
		{"zoned recent", zoned(-time.Minute), rxFromEnvelope, now.Add(-time.Minute), 0},
		{"zoned with offset", offset(-7 * time.Hour), rxFromEnvelope, now.Add(-7 * time.Hour), 0},
		{"zoned just under 30d", zoned(-rxMaxAge + time.Second), rxFromEnvelope, now.Add(-rxMaxAge + time.Second), 0},
		{"zoned just over 30d", zoned(-rxMaxAge - time.Second), rxTooOld, time.Time{}, 0},
		{"zoned just under 14h ahead", zoned(rxMaxFuture - time.Second), rxClampedToNow, time.Time{}, 0},
		{"zoned just over 14h ahead", zoned(rxMaxFuture + time.Second), rxTooFarAhead, time.Time{}, 0},
		{"zoned 2m ahead", zoned(2 * time.Minute), rxClampedToNow, time.Time{}, 0},
		{"naive 14m behind", naive(-14 * time.Minute), rxFromEnvelope, now.Add(-14 * time.Minute), 0},
		{"naive 16m behind", naive(-16 * time.Minute), rxNaiveClamped, time.Time{}, -960},
		{"naive 5h ahead (UTC+5)", naive(5 * time.Hour), rxNaiveClamped, time.Time{}, 18000},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := resolveRxTimeAt(c.msg, now)
			if got.Reason != c.reason {
				t.Fatalf("reason = %v, want %v", got.Reason, c.reason)
			}
			want := c.at
			if want.IsZero() {
				want = now
			}
			if !got.At.Equal(want) {
				t.Errorf("at = %v, want %v", got.At, want)
			}
			if got.At.Location() != time.UTC {
				t.Errorf("at is in %v, want UTC", got.At.Location())
			}
			if got.SkewSec != c.skewSec {
				t.Errorf("skew = %d, want %d", got.SkewSec, c.skewSec)
			}
		})
	}
}
