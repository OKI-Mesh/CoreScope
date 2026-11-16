package decoder

import "testing"

func TestComputeContentHashIgnoresPath(t *testing.T) {
	const payload = "D69FD7A5A7"
	// Header 0x09 is a FLOOD route carrying payload type 2 (TXT_MSG). The path
	// byte carries the hash size in its top two bits and the hop count in the
	// low six, so 0x41 is one two-byte hop.
	cases := []struct {
		name string
		hex  string
	}{
		{"no path", "09" + "00" + payload},
		{"two one-byte hops", "09" + "02" + "AABB" + payload},
		{"one two-byte hop", "09" + "41" + "AABB" + payload},
		{"three one-byte hops", "09" + "03" + "AABBCC" + payload},
	}
	want := ComputeContentHash(cases[0].hex)
	for _, c := range cases[1:] {
		if got := ComputeContentHash(c.hex); got != want {
			t.Errorf("%s: hash %s, want %s (the hash of the same payload with no path)", c.name, got, want)
		}
	}

	other := ComputeContentHash("09" + "00" + "D69FD7A5A8")
	if other == want {
		t.Errorf("a different payload produced the same hash %s", other)
	}
}
