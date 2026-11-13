package types

import "testing"

func TestClassifySkew(t *testing.T) {
	tests := []struct {
		absSkew  float64
		expected SkewSeverity
	}{
		{0, SkewOK},
		{60, SkewOK},                    // 1 min
		{299, SkewOK},                   // just under 5 min
		{300, SkewWarning},              // exactly 5 min
		{1800, SkewWarning},             // 30 min
		{3599, SkewWarning},             // just under 1 hour
		{3600, SkewCritical},            // exactly 1 hour
		{86400, SkewCritical},           // 1 day
		{2592000 - 1, SkewCritical},     // just under 30 days
		{2592000, SkewAbsurd},           // exactly 30 days
		{86400*365 - 1, SkewAbsurd},     // just under 365 days
		{86400 * 365, SkewNoClock},      // exactly 365 days
		{86400 * 365 * 10, SkewNoClock}, // 10 years (epoch-0 style)
	}
	for _, tc := range tests {
		if got := ClassifySkew(tc.absSkew); got != tc.expected {
			t.Errorf("ClassifySkew(%v) = %v, want %v", tc.absSkew, got, tc.expected)
		}
	}
}

func TestClassifySkewNeverReturnsBimodal(t *testing.T) {
	for _, v := range []float64{0, 299, 3600, 2592000, 86400 * 365 * 50} {
		if ClassifySkew(v) == SkewBimodalClock {
			t.Errorf("ClassifySkew(%v) returned bimodal_clock", v)
		}
	}
}

func TestSkewSeverityHasUnreliableClock(t *testing.T) {
	for severity, want := range map[SkewSeverity]bool{
		SkewOK:           false,
		SkewWarning:      false,
		SkewCritical:     false,
		SkewAbsurd:       false,
		SkewNoClock:      true,
		SkewBimodalClock: true,
	} {
		if got := severity.HasUnreliableClock(); got != want {
			t.Errorf("%s.HasUnreliableClock() = %v, want %v", severity, got, want)
		}
	}
}
