package main

import (
	"strings"
	"sync/atomic"
)

// Issue #1547 — resolved_path writer (ingestor-owned).
//
// Per the #1283 refactor (server is read-only; ingestor owns the
// neighbor graph + node directory), the writer that populated
// `observations.resolved_path` must live here in the ingestor. PR #1289
// removed the server-side writer without porting it — this restores it.
//
// Approach:
//   - `resolvePath` is a pure function: hop prefixes → full pubkeys
//     using the in-memory prefix index built from `nodes.public_key`.
//   - Unique-prefix hops resolve to the full pubkey; ambiguous or
//     unknown hops resolve to `nil`. The output shape is `[]*string`
//     (with nulls for unresolved positions), stored one row per hop in
//     observation_path_hops.
//   - The prefix index is rebuilt on startup and once per neighbor-
//     builder tick (60s) so new nodes start resolving within a minute
//     without blocking the MQTT ingest path.

// resolvePath maps each hop prefix to a full pubkey when the index
// has exactly one candidate; returns nil at that position otherwise.
// Returns nil for empty/no hops.
func resolvePath(hops []string, idx prefixIndex) []*string {
	if len(hops) == 0 {
		return nil
	}
	out := make([]*string, len(hops))
	if idx == nil {
		return out
	}
	for i, hop := range hops {
		h := strings.ToLower(hop)
		candidates := idx[h]
		if len(candidates) == 1 {
			pk := candidates[0]
			out[i] = &pk
		}
	}
	return out
}

// prefixIdxHolder caches the prefix index for the InsertTransmission
// hot path. atomic.Value lets the 60s rebuild happen without a lock on
// the read side.
type prefixIdxHolder struct {
	v atomic.Value // holds prefixIndex
}

func (h *prefixIdxHolder) load() prefixIndex {
	if v := h.v.Load(); v != nil {
		return v.(prefixIndex)
	}
	return nil
}

func (h *prefixIdxHolder) store(idx prefixIndex) {
	h.v.Store(idx)
}

// resolvedPubkeys flattens a resolved path to the non-nil pubkeys it
// contains, deduplicating repeats within the same path. Used by the
// relay-aware last_seen touch (#1598); nil entries are unresolved or
// ambiguous hops and are deliberately dropped.
func resolvedPubkeys(rp []*string) []string {
	if len(rp) == 0 {
		return nil
	}
	out := make([]string, 0, len(rp))
	seen := make(map[string]bool, len(rp))
	for _, p := range rp {
		if p == nil || *p == "" || seen[*p] {
			continue
		}
		seen[*p] = true
		out = append(out, *p)
	}
	return out
}
