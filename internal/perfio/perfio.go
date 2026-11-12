// Package perfio holds the /proc/self/io key:value parser shared by the
// ingestor (which publishes /proc/self/io rate samples to its on-disk stats
// file) and the server (which samples its own I/O for /api/perf/io).
//
// The parser lives here (Carmack #1167 must-fix #7) so the two binaries do not
// carry divergent copies of it; past divergence already produced a real bug
// (see must-fix #6: the parsedAny empty-key gate was added on one side only).
// The rate sample the ingestor writes to its stats file is
// ingeststats.IOSample, with the rest of that file's contract.
package perfio

import (
	"bufio"
	"strconv"
	"strings"
)

// Counters is the raw /proc/self/io counter snapshot. Both the ingestor's
// procIOSnapshot and the server's procIOSample are thin wrappers around
// these fields plus a sampled-at timestamp; the parser populates Counters
// directly so there's exactly ONE implementation of the key:value walker.
type Counters struct {
	ReadBytes           int64
	WriteBytes          int64
	CancelledWriteBytes int64
	SyscR               int64
	SyscW               int64
}

// ParseProcIO reads /proc/self/io-shaped key:value lines from sc and
// populates c. Returns true iff at least one recognised key was
// successfully parsed (Carmack must-fix #6 — empty / no-known-keys input
// must NOT be treated as a valid sample, otherwise the next tick computes
// a phantom delta against zero counters).
func ParseProcIO(sc *bufio.Scanner, c *Counters) bool {
	parsedAny := false
	for sc.Scan() {
		parts := strings.SplitN(sc.Text(), ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		if err != nil {
			continue
		}
		switch key {
		case "read_bytes":
			c.ReadBytes = val
			parsedAny = true
		case "write_bytes":
			c.WriteBytes = val
			parsedAny = true
		case "cancelled_write_bytes":
			c.CancelledWriteBytes = val
			parsedAny = true
		case "syscr":
			c.SyscR = val
			parsedAny = true
		case "syscw":
			c.SyscW = val
			parsedAny = true
		}
	}
	return parsedAny
}
