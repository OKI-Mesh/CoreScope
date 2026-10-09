package main

import (
	"context"
	"errors"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/mbcapqueue"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// MultibyteCapPersistStats holds counts for /api/healthz exposure / logging.
type MultibyteCapPersistStats struct {
	ReadEntries     int   // entries read from snapshot
	UpdatedActive   int64 // rows updated in nodes
	UpdatedInactive int64 // rows updated in inactive_nodes
	Skipped         int   // entries skipped (status=="unknown")
	Updated         int64 // total rows updated (active + inactive)
	Invalid         int   // entries with unrecognized status
	Duplicates      int   // entries skipped due to duplicates
}

// RunMultibyteCapPersist consumes the latest multi-byte capability snapshot
// written by the server (internal/mbcapqueue) and persists it to nodes /
// inactive_nodes. Owned by the ingestor per #1287: the server is read-only
// since #1289 and cannot UPDATE these columns itself.
//
// INVARIANT (canonical owner): multibyte_sup / multibyte_evidence are
// derived/cached columns. The server COMPUTES the value during its
// analytics cycle (from observed packets) and writes a snapshot file;
// this function is the ONLY runtime path that mutates those columns
// (the schema itself is added by internal/dbschema). The server MUST
// NOT execute any UPDATE on nodes.multibyte_* — see
// cmd/server/readonly_invariant_test.go for the enforcement.
//
// Data-destruction guard: entries with Status=="unknown" (sup==0) are
// NEVER persisted — we never overwrite a previously confirmed/suspected
// DB value with a snapshot blank. Same guarantee the original
// server-side helper enforced before relocation.
//
// Safe to call from a ticker; no-op when no snapshot has been written
// (cold start), when the snapshot is empty, when the snapshot is
// malformed (#1386), or when running against a legacy DB that
// pre-dates the multibyte_sup migration (#1386).
func (s *Store) RunMultibyteCapPersist(ctx context.Context) (MultibyteCapPersistStats, error) {
	var stats MultibyteCapPersistStats
	if s.snapshotDir == "" {
		return stats, nil // misconfigured; main() warns once at startup, not on every tick
	}
	snap, err := mbcapqueue.ReadSnapshot(s.snapshotDir)
	if err != nil {
		// os.ErrNotExist is the steady state until the server's first analytics
		// cycle completes: a silent no-op. Anything else (malformed or unreadable)
		// is operator-actionable, but must not stop the maintenance ticker.
		if !errors.Is(err, os.ErrNotExist) {
			log.Printf("[multibyte-persist] read snapshot: %v (no-op)", err)
		}
		return stats, nil
	}
	stats.ReadEntries = len(snap.Entries)

	// One entry per node. The SQL is UPDATE ... FROM, which applies an arbitrary one of
	// several matching rows, so duplicates are resolved here, keeping the highest level.
	best := make(map[string]database.NodeMultibyteCap, len(snap.Entries))
	for _, e := range snap.Entries {
		st, ok := types.ParseMultibyteSupport(e.Status)
		if !ok {
			stats.Invalid++
			continue
		}
		if st == types.MultibyteUnknown { // never overwrite a confirmed/suspected value with a blank
			stats.Skipped++
			continue
		}
		key := strings.ToLower(e.PublicKey) // nodes.public_key is lowercase; dedupe on this form
		c := database.NodeMultibyteCap{PublicKey: key, Sup: int32(st), Evidence: e.Evidence}
		if cur, ok := best[key]; ok {
			stats.Duplicates++
			if c.Sup <= cur.Sup { // keep the highest; on a tie the first one wins
				continue
			}
		}
		best[key] = c
	}

	caps := make([]database.NodeMultibyteCap, 0, len(best))
	for _, c := range best {
		caps = append(caps, c)
	}
	sort.Slice(caps, func(i, j int) bool { return caps[i].PublicKey < caps[j].PublicKey })

	n, err := s.db.UpdateNodeMultibyteCaps(ctx, caps) // returns 0 for an empty slice

	if err != nil {
		return stats, err
	}
	stats.Updated = n
	if n > 0 {
		log.Printf("[multibyte-persist] applied snapshot: %d entries (%d skipped); updated %d nodes",
			stats.ReadEntries, stats.Skipped, n)
	}
	return stats, nil
}

// isMalformedSnapshotErr returns true if err looks like a JSON parse /
// IO-truncation failure surfaced by mbcapqueue.ReadSnapshot. The
// queue wraps errors with %w but mbcapqueue currently formats with
// %w only for "read:"/"unmarshal:" prefixes — we substring-match
// those so the operator-actionable log message is unambiguous.
func isMalformedSnapshotErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, frag := range []string{"unmarshal", "invalid character", "unexpected end of JSON"} {
		if containsCI(msg, frag) {
			return true
		}
	}
	return false
}

func containsCI(s, sub string) bool {
	if len(sub) == 0 {
		return true
	}
	// case-insensitive Contains without importing strings (already
	// imported in db.go, but keeping helper local to avoid widening
	// this file's imports).
	for i := 0; i+len(sub) <= len(s); i++ {
		match := true
		for j := 0; j < len(sub); j++ {
			a, b := s[i+j], sub[j]
			if a >= 'A' && a <= 'Z' {
				a += 32
			}
			if b >= 'A' && b <= 'Z' {
				b += 32
			}
			if a != b {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
