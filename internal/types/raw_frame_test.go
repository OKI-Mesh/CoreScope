package types

import "testing"

func TestParseRawFrameAcceptsEitherCaseAndFormatsUppercase(t *testing.T) {
	for _, in := range []string{"0a01ff", "0A01FF", "0a01FF"} {
		f, err := ParseRawFrame(in)
		if err != nil {
			t.Fatalf("ParseRawFrame(%q): %v", in, err)
		}
		if got := f.Hex(); got != "0A01FF" {
			t.Errorf("ParseRawFrame(%q).Hex() = %q, want 0A01FF", in, got)
		}
	}
}

func TestParseRawFrameRejectsInvalidHex(t *testing.T) {
	for _, in := range []string{"abc", "zz", "FRAME2HOPS"} {
		if _, err := ParseRawFrame(in); err == nil {
			t.Errorf("ParseRawFrame(%q) succeeded, want an error", in)
		}
	}
}

func TestParseRawFrameEmptyIsNonNil(t *testing.T) {
	f, err := ParseRawFrame("")
	if err != nil {
		t.Fatal(err)
	}
	if f == nil || len(f) != 0 {
		t.Errorf("ParseRawFrame(\"\") = %#v, want an empty non-nil frame", f)
	}
}
