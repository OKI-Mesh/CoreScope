package types

import "strings"

// IATA is an observer's region code as stored: trimmed and upper-case, or
// empty when the observer has none. The database enforces the same rule with
// the observers_iata_normalized CHECK, so a writer that skips NewIATA fails
// loudly instead of storing a code the region filters cannot match.
type IATA string

// NewIATA trims surrounding whitespace and upper-cases the code. Any
// whitespace or case a status message arrived with is normalized here, once,
// at the boundary.
func NewIATA(raw string) IATA {
	return IATA(strings.ToUpper(strings.TrimSpace(raw)))
}

// IsZero reports whether there is no code.
func (i IATA) IsZero() bool {
	return i == ""
}

func (i IATA) String() string {
	return string(i)
}
