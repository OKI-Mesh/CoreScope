package main

import (
	"errors"
	"fmt"
	"time"
)

// naiveTimestampLayout matches MQTT envelope timestamps with no timezone
// indicator (assumed UTC) — e.g. "2026-10-02T22:23:30.000000".
const naiveTimestampLayout = "2006-01-02T15:04:05.000000"

// parseMQTTTimestamp parses the various timestamp formats seen across
// different observer/companion MQTT sources. Most carry an explicit
// timezone (RFC3339, with or without fractional seconds — "...Z" or
// "...+00:00"); some older sources omit the timezone entirely and are
// assumed UTC. An unrecognized format returns an error so the caller can
// log it and fall back to ingest time, rather than silently misparsing.
func parseMQTTTimestamp(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, errors.New("empty timestamp")
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t.UTC(), nil
	}
	if t, err := time.ParseInLocation(naiveTimestampLayout, raw, time.UTC); err == nil {
		return t.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("unrecognized timestamp format: %q", raw)
}
