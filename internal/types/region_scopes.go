package types

import "strings"

// maxRegionNameLen bounds a derived region name. Firmware region names are
// short labels; anything longer is a malformed or hostile entry, and each
// accepted name costs an HMAC on every transport-scoped packet.
const MaxRegionNameLen = 32

// RegionName is a MeshCore region name as a node declared it. The firmware
// ignores one leading "#" when matching names, so "#cvg" and "cvg" are the same
// region, while case is significant: "CVG" is a different region.
type RegionName string

// Key is the identity of the region: the name without one leading "#".
func (n RegionName) Key() string {
	return strings.TrimPrefix(string(n), "#")
}

// IsWildcard reports whether the name is the flood wildcard "*", with or
// without the leading "#".
func (n RegionName) IsWildcard() bool {
	return n.Key() == "*"
}

// Canonical returns the name in the form a transport key is derived from, and
// whether the name may be a region name at all. The rules are structural, never
// about the name's meaning:
//
//   - "*" is the flood wildcard, not a region, and hashing it would invent a
//     region nobody declared
//   - a comma would split the name on the next round-trip through a
//     comma-separated column
//   - a second "#" cannot come from either source intact
//   - non-ASCII or whitespace would make the key a hash over bytes nobody
//     intended, silently mismatching the sender
//   - a NUL is block-cipher padding a stale client failed to trim
func (n RegionName) Canonical() (string, bool) {
	name := n.Key()
	if name == "" || name == "*" || len(name) > MaxRegionNameLen {
		return "", false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c <= ' ' || c >= 0x7F || c == ',' || c == '#' {
			return "", false
		}
	}
	return name, true
}
