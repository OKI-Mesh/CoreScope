package types

import (
	"encoding/json"
	"testing"
	"time"
)

func TestUTCTimeMarshalsFixedWidthMilliseconds(t *testing.T) {
	est := time.FixedZone("EST", -5*60*60)
	cases := []struct {
		name string
		in   time.Time
		want string
	}{
		{"sub-millisecond precision is truncated", time.Date(2026, 11, 6, 13, 0, 0, 123456789, est), `"2026-11-06T18:00:00.123Z"`},
		{"whole seconds keep the .000 width", time.Date(2026, 11, 6, 13, 0, 0, 0, time.UTC), `"2026-11-06T13:00:00.000Z"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(WireTime{Time: tc.in})
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if string(raw) != tc.want {
				t.Errorf("marshaled as %s, want %s", raw, tc.want)
			}
		})
	}
}

func TestUTCTimeRoundTrip(t *testing.T) {
	in := WireTime{Time: time.Date(2026, 11, 6, 13, 0, 0, 123000000, time.UTC)}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out WireTime
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !out.Equal(in.Time) {
		t.Errorf("round trip changed the time: got %v, want %v", out.Time, in.Time)
	}
}

func TestNewWireTimePtrKeepsNil(t *testing.T) {
	if NewWireTimePtr(nil) != nil {
		t.Error("nil input must stay nil")
	}
	now := time.Now()
	if got := NewWireTimePtr(&now); got == nil || !got.Equal(now) {
		t.Errorf("NewWireTimePtr(&now) = %v, want %v", got, now)
	}
}
