package types

import "testing"

func TestHopListHash(t *testing.T) {
	cases := []struct {
		name string
		hops HopList
		want string
	}{
		{"nil", nil, ""},
		{"empty", HopList{}, ""},
		{"one hop", HopList{"AA"}, "58bb119c35513a451d24dc20ef0e9031ec85b35bfc919d263e7e5d9868909cb5"},
		{"two hops", HopList{"AA", "BB"}, "8746e8fb2e649af11060f36b7f014ee6a0b8261808f1e37938f6bb7602653c6c"},
		{"two-byte hops", HopList{"7D1D", "DA2A", "EACF"}, "709ec6c7e53ec736bab5b2ae659f792363b2886624633a5c28715dfb74ea4c67"},
		{"case is significant", HopList{"aa"}, "961b6dd3ede3cb8ecbaacbd68de040cd78eb2ed5889130cceb4c49268ea4d506"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.hops.Hash(); got != c.want {
				t.Errorf("Hash() = %q, want %q", got, c.want)
			}
		})
	}
}
