package types_test

import (
	"testing"

	"github.com/OKI-Mesh/CoreScope/internal/types"
)

func TestRegionName_Key(t *testing.T) {
	cases := []struct{ name, want string }{
		{"cvg", "cvg"},
		{"#cvg", "cvg"},
		{"##cvg", "#cvg"},
		{"CVG", "CVG"},
		{"#CVG", "CVG"},
		{"$private", "$private"},
		{"*", "*"},
		{"#*", "*"},
		{"", ""},
		{"#", ""},
	}
	for _, c := range cases {
		if got := types.RegionName(c.name).Key(); got != c.want {
			t.Errorf("RegionName(%q).Key(): want %q, got %q", c.name, c.want, got)
		}
	}
}

func TestRegionName_SameRegionOnlyWhenKeysMatch(t *testing.T) {
	if types.RegionName("#cvg").Key() != types.RegionName("cvg").Key() {
		t.Error("#cvg and cvg are the same region")
	}
	if types.RegionName("CVG").Key() == types.RegionName("cvg").Key() {
		t.Error("CVG and cvg are different regions")
	}
}

func TestRegionName_IsWildcard(t *testing.T) {
	for name, want := range map[string]bool{"*": true, "#*": true, "cvg": false, "": false} {
		if got := types.RegionName(name).IsWildcard(); got != want {
			t.Errorf("RegionName(%q).IsWildcard(): want %v, got %v", name, want, got)
		}
	}
}

func TestRegionNameAcceptable(t *testing.T) {
	cases := []struct {
		name string
		want string
		ok   bool
		why  string
	}{
		{"be", "be", true, "a bare name from an OTA answer"},
		{"#be", "be", true, "the same region as nodes.configured_scope spells it — normalizeScopeList adds the '#'"},
		{"nl-li-nth", "nl-li-nth", true, "hyphens are ordinary in region names"},
		{"*", "", false, "the flood wildcard is not a region, and hashing it would invent one nobody declared"},
		{"#*", "", false, "the wildcard stays the wildcard however it is spelled"},
		{"", "", false, "empty"},
		{"#", "", false, "a bare prefix names nothing"},
		{"be,nl", "", false, "a comma would split the name on the next round-trip"},
		{"be#nl", "", false, "a second '#' cannot come from either source intact"},
		{"be nl", "", false, "whitespace would hash bytes nobody intended"},
		{"bé", "", false, "non-ASCII, same reason"},
		{"be" + string(rune(0)), "", false, "NUL is block-cipher padding a stale client failed to trim"},
		{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "", false, "33 chars, past maxRegionNameLen"},
	}
	for _, c := range cases {
		got, ok := types.RegionName(c.name).Canonical()
		if ok != c.ok || got != c.want {
			t.Errorf("RegionName(%q).Canonical() = (%q, %v), want (%q, %v) — %s", c.name, got, ok, c.want, c.ok, c.why)
		}
	}
}
