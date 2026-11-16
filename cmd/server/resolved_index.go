package main

import (
	"hash/fnv"
	"log"
	"strings"
)

// resolvedPubkeyHash computes a fast 64-bit hash for membership index keying.
// Uses FNV-1a from stdlib — good distribution, no external dependency.
func resolvedPubkeyHash(pk string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(strings.ToLower(pk)))
	return h.Sum64()
}

// addToResolvedPubkeyIndex adds a txID under each resolved pubkey hash.
// Deduplicates both within a single call AND across calls — won't add the
// same (hash, txID) pair twice even when called multiple times for the same tx.
// Must be called under s.mu write lock.
func (s *PacketStore) addToResolvedPubkeyIndex(txID int, resolvedPubkeys []string) {
	if !s.useResolvedPathIndex {
		return
	}
	seen := make(map[uint64]bool, len(resolvedPubkeys))
	for _, pk := range resolvedPubkeys {
		if pk == "" {
			continue
		}
		h := resolvedPubkeyHash(pk)
		if seen[h] {
			continue
		}
		seen[h] = true

		// Cross-call dedup: check if (h, txID) already exists in forward index.
		existing := s.resolvedPubkeyIndex[h]
		alreadyPresent := false
		for _, id := range existing {
			if id == txID {
				alreadyPresent = true
				break
			}
		}
		if alreadyPresent {
			continue
		}

		s.resolvedPubkeyIndex[h] = append(existing, txID)
		s.resolvedPubkeyReverse[txID] = append(s.resolvedPubkeyReverse[txID], h)
	}
}

// removeFromResolvedPubkeyIndex removes all index entries for a txID using the reverse map.
// Must be called under s.mu write lock.
func (s *PacketStore) removeFromResolvedPubkeyIndex(txID int) {
	if !s.useResolvedPathIndex {
		return
	}
	hashes := s.resolvedPubkeyReverse[txID]
	for _, h := range hashes {
		list := s.resolvedPubkeyIndex[h]
		// Remove ALL occurrences of txID (not just the first) to prevent orphans.
		filtered := list[:0]
		for _, id := range list {
			if id != txID {
				filtered = append(filtered, id)
			}
		}
		if len(filtered) == 0 {
			delete(s.resolvedPubkeyIndex, h)
		} else {
			s.resolvedPubkeyIndex[h] = filtered
		}
	}
	delete(s.resolvedPubkeyReverse, txID)
}

// extractResolvedPubkeys extracts all non-nil, non-empty pubkeys from a resolved path.
func extractResolvedPubkeys(rp []*string) []string {
	if len(rp) == 0 {
		return nil
	}
	result := make([]string, 0, len(rp))
	for _, p := range rp {
		if p != nil && *p != "" {
			result = append(result, *p)
		}
	}
	return result
}

// mergeResolvedPubkeys collects unique non-empty pubkeys from multiple resolved paths.
func mergeResolvedPubkeys(paths ...[]*string) []string {
	seen := make(map[string]bool)
	var result []string
	for _, rp := range paths {
		for _, p := range rp {
			if p != nil && *p != "" && !seen[*p] {
				seen[*p] = true
				result = append(result, *p)
			}
		}
	}
	return result
}

// initResolvedPathIndex initializes the resolved path index data structures.
func (s *PacketStore) initResolvedPathIndex() {
	s.resolvedPubkeyIndex = make(map[uint64][]int, 4096)
	s.resolvedPubkeyReverse = make(map[int][]uint64, 4096)
}

// CompactResolvedPubkeyIndex reclaims memory from the resolved pubkey index maps
// after eviction. It removes empty forward-index entries (shouldn't exist if
// removeFromResolvedPubkeyIndex is correct, but defense in depth) and clips
// oversized slice backing arrays where cap > 2*len.
// Must be called under s.mu write lock.
func (s *PacketStore) CompactResolvedPubkeyIndex() {
	if !s.useResolvedPathIndex {
		return
	}
	for h, ids := range s.resolvedPubkeyIndex {
		if len(ids) == 0 {
			delete(s.resolvedPubkeyIndex, h)
			continue
		}
		// Clip oversized backing arrays: if cap > 2*len, reallocate.
		if cap(ids) > 2*len(ids)+8 {
			clipped := make([]int, len(ids))
			copy(clipped, ids)
			s.resolvedPubkeyIndex[h] = clipped
		}
	}
	for txID, hashes := range s.resolvedPubkeyReverse {
		if len(hashes) == 0 {
			delete(s.resolvedPubkeyReverse, txID)
			continue
		}
		if cap(hashes) > 2*len(hashes)+8 {
			clipped := make([]uint64, len(hashes))
			copy(clipped, hashes)
			s.resolvedPubkeyReverse[txID] = clipped
		}
	}
}

// defaultMaxResolvedPubkeyIndexEntries is the default hard cap for the forward
// index. When exceeded, a warning is logged. No auto-eviction — that's the
// eviction ticker's job.
const defaultMaxResolvedPubkeyIndexEntries = 5_000_000

// CheckResolvedPubkeyIndexSize logs a warning if the resolved pubkey forward
// index exceeds the configured maximum entries. Must be called under s.mu
// read lock at minimum.
func (s *PacketStore) CheckResolvedPubkeyIndexSize() {
	if !s.useResolvedPathIndex {
		return
	}
	maxEntries := s.maxResolvedPubkeyIndexEntries
	if maxEntries <= 0 {
		maxEntries = defaultMaxResolvedPubkeyIndexEntries
	}
	fwdLen := len(s.resolvedPubkeyIndex)
	revLen := len(s.resolvedPubkeyReverse)
	if fwdLen > maxEntries || revLen > maxEntries {
		log.Printf("[store] WARNING: resolvedPubkeyIndex size exceeds limit — forward=%d reverse=%d limit=%d",
			fwdLen, revLen, maxEntries)
	}
}
