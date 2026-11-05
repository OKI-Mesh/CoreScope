package main

import (
	"testing"
	"time"
)

func TestParseTime(t *testing.T) {
	cases := []struct {
		name    string
		in      any
		want    time.Time
		wantOK  bool
		wantErr bool
	}{
		{"nil", nil, time.Time{}, false, false},
		{"empty string", "", time.Time{}, false, false},
		{"rfc3339", "2026-03-01T12:30:00Z", time.Date(2026, 3, 1, 12, 30, 0, 0, time.UTC), true, false},
		{"rfc3339 with fraction", "2026-03-01T12:30:00.5Z", time.Date(2026, 3, 1, 12, 30, 0, 500_000_000, time.UTC), true, false},
		{"rfc3339 with offset", "2026-03-01T12:30:00-05:00", time.Date(2026, 3, 1, 17, 30, 0, 0, time.UTC), true, false},
		{"datetime() text", "2026-03-01 12:30:00", time.Date(2026, 3, 1, 12, 30, 0, 0, time.UTC), true, false},
		{"epoch seconds", int64(1769904000), time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC), true, false},
		{"epoch seconds as text", "1769904000", time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC), true, false},
		{"epoch seconds as float", float64(1769904000), time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC), true, false},
		{"already a time", time.Date(2026, 3, 1, 12, 30, 0, 0, time.FixedZone("EST", -5*3600)), time.Date(2026, 3, 1, 17, 30, 0, 0, time.UTC), true, false},
		{"garbage", "not a time", time.Time{}, false, true},
		{"unsupported type", true, time.Time{}, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok, err := parseTime(c.in)
			if (err != nil) != c.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, c.wantErr)
			}
			if ok != c.wantOK {
				t.Fatalf("ok = %v, want %v", ok, c.wantOK)
			}
			if !got.Equal(c.want) {
				t.Errorf("time = %v, want %v", got, c.want)
			}
		})
	}
}
