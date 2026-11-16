package main

import (
	"context"
	"strings"
)

// hopIsHidden reports whether a path hop belongs to a node the operator has
// blacklisted or hidden by name prefix.
//
// A hop in a path is a short hex prefix of a pubkey (1 to 3 bytes), while the
// blacklist holds full pubkeys, so the hop alone never matches. The hop is
// therefore also resolved to every node it could stand for, and the hop counts
// as hidden when any of those candidates is blacklisted or hidden.
//
// This errs towards hiding: a hop that only shares a prefix with a blacklisted
// node is hidden as well. Filtering on resolved node ids would be exact (see
// the post-migration plan, section 1.6 item 10).
func (s *Server) hopIsHidden(ctx context.Context, hop string) bool {
	if s.cfg == nil {
		return false
	}
	if s.cfg.IsBlacklisted(hop) || s.isPubkeyHidden(ctx, hop) {
		return true
	}
	if s.store == nil {
		return false
	}
	_, pm := s.store.getCachedNodesAndPM()
	for _, n := range pm.m[strings.ToLower(strings.TrimSpace(hop))] {
		if s.cfg.IsBlacklisted(n.PublicKey) || s.cfg.IsNameHidden(n.Name) {
			return true
		}
	}
	return false
}
