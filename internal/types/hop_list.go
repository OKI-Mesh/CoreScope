package types

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// HopList is the ordered list of hop hashes a packet's path carried, exactly as
// written in path_json (uppercase hex, one entry per hop).
type HopList []string

// Hash returns the hex SHA-256 of the hops joined with commas, or "" for an
// empty list. The result is part of the observations dedup key, so it hashes
// the hops as stored: case is significant, and "AA" and "aa" hash differently.
func (h HopList) Hash() string {
	if len(h) == 0 {
		return ""
	}
	sum := sha256.Sum256([]byte(strings.Join(h, ",")))
	return hex.EncodeToString(sum[:])
}
