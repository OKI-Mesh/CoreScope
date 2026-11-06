package types

import (
	"encoding/json"
	"time"
)

// WireTimeLayout is the one timestamp layout the API speaks: UTC with
// millisecond precision and a fixed ".000" width, for example
// 2026-11-06T13:00:00.123Z.
const WireTimeLayout = "2006-01-02T15:04:05.000Z"

// FormatWireTime renders t in WireTimeLayout, converted to UTC.
func FormatWireTime(t time.Time) string {
	return t.UTC().Format(WireTimeLayout)
}

// WireTime is a time.Time that marshals to WireTimeLayout. Domain code keeps
// plain time.Time values; this type is the boundary, so every JSON timestamp is
// formatted in exactly one place.
type WireTime struct {
	time.Time
}

// NewUTCTimePtr wraps an optional time. A nil input stays nil (JSON null).
func NewUTCTimePtr(t *time.Time) *WireTime {
	if t == nil {
		return nil
	}
	return &WireTime{Time: *t}
}

func (t WireTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(FormatWireTime(t.Time))
}

func (t *WireTime) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	parsed, err := time.Parse(WireTimeLayout, s)
	if err != nil {
		return err
	}
	t.Time = parsed
	return nil
}
