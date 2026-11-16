package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
)

// handleResolvePrefix serves /api/nodes/resolve: it lets a client resolve a
// heard path prefix (or full pubkey) to a node name without fetching the whole
// node list. The reply is api.ResolvePrefixResponse. Read-only.
var hexPrefixRe = regexp.MustCompile(`^[0-9a-f]{2,64}$`)

// minResolvePrefixHex is the shortest accepted prefix. 1-byte (2 hex) keys are
// never stored — the ingestor rejects heard keys shorter than 2 bytes — so the
// floor matches the data model and, by ruling out the 256 two-char prefixes,
// blunts trivial enumeration of every node name through this endpoint (#15).
const minResolvePrefixHex = 4

func (s *Server) handleResolvePrefix(w http.ResponseWriter, r *http.Request) {
	pfx := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("prefix")))
	if !hexPrefixRe.MatchString(pfx) {
		http.Error(w, "prefix must be hex", http.StatusBadRequest)
		return
	}
	if len(pfx) < minResolvePrefixHex {
		http.Error(w, "prefix must be at least 4 hex chars", http.StatusBadRequest)
		return
	}
	if s.db == nil {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	// GetNodeByPrefix looks at two rows: enough to tell unique from ambiguous.
	node, ambiguous, err := s.db.GetNodeByPrefix(r.Context(), pfx, minResolvePrefixHex)
	if err != nil && !errors.Is(err, database.ErrNotFound) {
		http.Error(w, "query failed", http.StatusInternalServerError)
		return
	}

	resp := api.ResolvePrefixResponse{Prefix: pfx}
	switch {
	case ambiguous:
		resp.Ambiguous = true
	case node != nil:
		name := ""
		if node.Name != nil {
			name = *node.Name
		}
		// Parity with /api/nodes/search and /api/resolve-hops: never reveal the
		// identity of a blacklisted or hidden-prefix node (#1181). Report it as
		// not-found rather than leaking the name the rest of the API hides.
		if !s.cfg.IsBlacklisted(node.PublicKey) && !s.cfg.IsNameHidden(name) {
			resp.Pubkey = ptr(node.PublicKey)
			// A found node with no name reports its pubkey only.
			if name != "" {
				resp.Name = ptr(name)
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
