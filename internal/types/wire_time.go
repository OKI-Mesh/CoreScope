package types

import (
	"encoding/json"
	"fmt"
	"time"
)

const (
	// WireTimeMillisLayout is UTC with exactly three fractional digits.
	WireTimeMillisLayout = "2006-01-02T15:04:05.000Z"
	// WireTimeSecondsLayout is UTC truncated to whole seconds.
	WireTimeSecondsLayout = "2006-01-02T15:04:05Z"
)

// FormatWireTimeMillis formats t the way WireTimeMillis serializes.
func FormatWireTimeMillis(t time.Time) string {
	return t.UTC().Format(WireTimeMillisLayout)
}

// FormatWireTimeSeconds formats t the way WireTimeSeconds serializes.
func FormatWireTimeSeconds(t time.Time) string {
	return t.UTC().Format(WireTimeSecondsLayout)
}

// WireTimeMillis is a time.Time that always serializes as millisecond UTC.
type WireTimeMillis struct{ time.Time }

func NewWireTimeMillisPtr(t *time.Time) *WireTimeMillis {
	if t == nil {
		return nil
	}
	return &WireTimeMillis{Time: *t}
}

func (t WireTimeMillis) MarshalJSON() ([]byte, error) {
	return json.Marshal(FormatWireTimeMillis(t.Time))
}

func (t *WireTimeMillis) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	parsed, err := time.Parse(WireTimeMillisLayout, s)
	if err != nil {
		return fmt.Errorf("parse millisecond timestamp %q: %w", s, err)
	}
	t.Time = parsed
	return nil
}

// WireTimeSeconds is a time.Time that always serializes as whole-second UTC.
// Sub-second digits are dropped, not rounded.
type WireTimeSeconds struct{ time.Time }

func NewWireTimeSecondsPtr(t *time.Time) *WireTimeSeconds {
	if t == nil {
		return nil
	}
	return &WireTimeSeconds{Time: *t}
}

func (t WireTimeSeconds) MarshalJSON() ([]byte, error) {
	return json.Marshal(FormatWireTimeSeconds(t.Time))
}

func (t *WireTimeSeconds) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	parsed, err := time.Parse(WireTimeSecondsLayout, s)
	if err != nil {
		return fmt.Errorf("parse whole-second timestamp %q: %w", s, err)
	}
	t.Time = parsed
	return nil
}
