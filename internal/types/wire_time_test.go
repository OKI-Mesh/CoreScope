package types

import (
	"encoding/json"
	"testing"
	"time"
)

func TestWireTimeMillisMarshalIsFixedWidth(t *testing.T) {
	cases := []struct {
		name string
		in   time.Time
		want string
	}{
		{"zero fraction keeps three digits", time.Date(2026, 11, 6, 2, 15, 52, 0, time.UTC), `"2026-11-06T02:15:52.000Z"`},
		{"milliseconds", time.Date(2026, 11, 6, 2, 15, 52, 56*int(time.Millisecond), time.UTC), `"2026-11-06T02:15:52.056Z"`},
		{"finer precision is truncated", time.Date(2026, 11, 6, 2, 15, 52, 123456789, time.UTC), `"2026-11-06T02:15:52.123Z"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b, err := json.Marshal(WireTimeMillis{Time: c.in})
			if err != nil {
				t.Fatal(err)
			}
			if string(b) != c.want {
				t.Errorf("marshal = %s, want %s", b, c.want)
			}
		})
	}
}

func TestWireTimeMillisConvertsToUTC(t *testing.T) {
	est := time.FixedZone("EST", -5*3600)
	b, err := json.Marshal(WireTimeMillis{Time: time.Date(2026, 11, 6, 14, 18, 9, 250*int(time.Millisecond), est)})
	if err != nil {
		t.Fatal(err)
	}
	if want := `"2026-11-06T19:18:09.250Z"`; string(b) != want {
		t.Errorf("marshal = %s, want %s", b, want)
	}
}

func TestWireTimeMillisRoundTrip(t *testing.T) {
	var got WireTimeMillis
	if err := json.Unmarshal([]byte(`"2026-11-06T02:15:52.056Z"`), &got); err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 11, 6, 2, 15, 52, 56*int(time.Millisecond), time.UTC); !got.Equal(want) {
		t.Errorf("unmarshal = %s, want %s", got, want)
	}
}

func TestWireTimeMillisRejectsOtherLayouts(t *testing.T) {
	for _, in := range []string{`"2026-11-06T02:15:52Z"`, `"2026-11-06T02:15:52.0561Z"`, `"not a time"`, `12`} {
		var got WireTimeMillis
		if err := json.Unmarshal([]byte(in), &got); err == nil {
			t.Errorf("unmarshal %s succeeded, want an error", in)
		}
	}
}

func TestNewWireTimeMillisPtr(t *testing.T) {
	if got := NewWireTimeMillisPtr(nil); got != nil {
		t.Errorf("NewWireTimeMillisPtr(nil) = %v, want nil", got)
	}
	in := time.Date(2026, 11, 6, 2, 15, 52, 0, time.UTC)
	got := NewWireTimeMillisPtr(&in)
	if got == nil || !got.Equal(in) {
		t.Errorf("NewWireTimeMillisPtr(&%s) = %v", in, got)
	}
}

func TestWireTimeSecondsMarshalDropsFraction(t *testing.T) {
	in := WireTimeSeconds{Time: time.Date(2026, 11, 6, 19, 18, 9, 987654321, time.UTC)}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if want := `"2026-11-06T19:18:09Z"`; string(b) != want {
		t.Errorf("marshal = %s, want %s (truncated, not rounded)", b, want)
	}
}

func TestWireTimeSecondsConvertsToUTC(t *testing.T) {
	est := time.FixedZone("EST", -5*3600)
	b, err := json.Marshal(WireTimeSeconds{Time: time.Date(2026, 11, 6, 14, 18, 9, 0, est)})
	if err != nil {
		t.Fatal(err)
	}
	if want := `"2026-11-06T19:18:09Z"`; string(b) != want {
		t.Errorf("marshal = %s, want %s", b, want)
	}
}

func TestWireTimeSecondsRoundTrip(t *testing.T) {
	var got WireTimeSeconds
	if err := json.Unmarshal([]byte(`"2026-11-06T19:18:09Z"`), &got); err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 11, 6, 19, 18, 9, 0, time.UTC); !got.Equal(want) {
		t.Errorf("unmarshal = %s, want %s", got, want)
	}
}

func TestWireTimeSecondsRejectsOtherLayouts(t *testing.T) {
	for _, in := range []string{`"2026-11-06"`, `"2026-11-06T19:18:09"`, `"not a time"`, `12`} {
		var got WireTimeSeconds
		if err := json.Unmarshal([]byte(in), &got); err == nil {
			t.Errorf("unmarshal %s succeeded, want an error", in)
		}
	}
}

func TestNewWireTimeSecondsPtr(t *testing.T) {
	if got := NewWireTimeSecondsPtr(nil); got != nil {
		t.Errorf("NewWireTimeSecondsPtr(nil) = %v, want nil", got)
	}
	in := time.Date(2026, 11, 6, 19, 18, 9, 0, time.UTC)
	got := NewWireTimeSecondsPtr(&in)
	if got == nil || !got.Equal(in) {
		t.Errorf("NewWireTimeSecondsPtr(&%s) = %v", in, got)
	}
}
