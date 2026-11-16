package types

import "testing"

func TestNewIATA(t *testing.T) {
	cases := []struct {
		raw  string
		want IATA
	}{
		{"CVG", "CVG"},
		{"cvg", "CVG"},
		{"  cvg\t", "CVG"},
		{"Cvg", "CVG"},
		{"", ""},
		{"   ", ""},
	}
	for _, c := range cases {
		if got := NewIATA(c.raw); got != c.want {
			t.Errorf("NewIATA(%q) = %q, want %q", c.raw, got, c.want)
		}
	}
}

func TestIATA_IsZero(t *testing.T) {
	if !NewIATA(" ").IsZero() {
		t.Error("blank code should be zero")
	}
	if NewIATA("cvg").IsZero() {
		t.Error("CVG should not be zero")
	}
}
