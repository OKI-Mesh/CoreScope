package main

// Issue #1975: the network-wide Scope Audit, GET /api/scope-audit.
//
// One row per repeater whose configured region list is known, answering a
// question no other view answers: you declare these regions, but were you
// actually seen forwarding them? default_scope says what a node's adverts were
// observed under and transported_scopes (#1751) says what it carried, but
// nothing lines the declared list up against observed forwarding, so a
// repeater configured for eight regions that only ever forwards one looks
// healthy everywhere else.
//
// Declared side: nodes.configured_scope, written by the observer /neighbors
// ingest from #1865/#1971. Observed side: forwarder hops in the window,
// aggregated per target. Both sides are compared through normScope, so the
// leading "#" that configured_scope carries and a bare region name are the
// same region.
//
// Ported from a long-running fork deployment. The measurements quoted in
// #1975 come from a 1179-repeater instance.

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"golang.org/x/sync/singleflight"
)

// ScopeObservation is one region scope this repeater has been observed
// forwarding traffic for, within the query window.
type ScopeObservation struct {
	Scope     string `json:"scope"` // matched region name (transmissions.scope_name)
	Packets   int64  `json:"packets"`
	FirstSeen string `json:"firstSeen"`
	LastSeen  string `json:"lastSeen"`
}

// RouteTypeMix is the route-type breakdown of packets this node was
// observed FORWARDING — i.e. packets on which this pubkey was the last hop
// of a FLOOD-family route (RouteTransportFlood, RouteFlood). It does NOT
// mean "packets in which this node appears anywhere in the path": a DIRECT
// or TRANSPORT_DIRECT packet's last path hop is the route's far end, never
// the transmitter, so crediting it here would attribute forwarding this node
// never did. Direct and TransportDirect are therefore always zero by
// construction — the forwarder join can never match those route types.

// minForwarderHopHexLen is the shortest path_json hop ScopeConformance will
// treat as an attribution: 4 hex chars = 2 bytes. Matches deriveHeardKey's
// floor (cmd/ingestor/client_reception.go: "exclude 1-byte (collision-prone),
// matching Reach") — a 1-byte/2-hex-char hash collides too often across a
// real fleet, and attributing a scope to the wrong node is worse than
// attributing none.
const minForwarderHopHexLen = 4

type DeclaredRegions struct {
	Regions    []string `json:"regions"`
	ObservedAt string   `json:"observedAt"`
	Truncated  bool     `json:"truncated"`
}

// splitRegionsCSV parses regions_csv (comma-separated, "#" prefixes already
// stripped by the firmware) into a slice, always non-nil.
func splitRegionsCSV(csv string) []string {
	regions := []string{}
	if csv == "" {
		return regions
	}
	for _, part := range strings.Split(csv, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			regions = append(regions, part)
		}
	}
	return regions
}

// normScope strips a leading '#' so the two sides of the declared/observed
// comparison can be matched at all — mirrors public/node-scopes.js's
// normScope exactly. transmissions.scope_name keeps the '#' (region keys
// are configured as hashRegions: ["#belgium", "#eu"]); regions_csv arrives
// from the firmware with the prefix already stripped. Comparing raw
// silently inverts the whole comparison while looking entirely plausible.

func normScope(s string) string {
	if strings.HasPrefix(s, "#") {
		return s[1:]
	}
	return s
}

// scopeAuditTargetAgg is one declared target's forwarding aggregate for the
// GET /api/scope-audit scan window: named scopes it forwarded (key =
// normScope'd name) plus a count of the plain-FLOOD (unscoped) packets it
// forwarded — the signal the wildcard-contradiction check needs, since '*'
// governs exactly those packets, not any named scope.

type scopeAuditTargetAgg struct {
	scopes          map[string]*ScopeObservation
	unscopedPackets int64
	// unmatchedPackets counts packets this target was observed forwarding
	// that carried a transport scope no configured region key matched
	// (transmissions.scope_name = the empty string). Deliberately NOT folded
	// into unscopedPackets: those two are opposites. Unscoped means the packet
	// carried no scope at all (scope_name SQL NULL) and is what '*' governs;
	// unmatched means it IS scoped and this instance simply holds no key for
	// that region, so '*' says nothing about it. See scopeNameForDB in the
	// ingestor for the encoding.
	//
	// A non-zero value is a caveat on this target's notObserved entries: any
	// of them may be a region this instance cannot name rather than one the
	// repeater is not forwarding.
	unmatchedPackets int64

	// unmatchedTxIDs are the transmissions behind unmatchedPackets, kept so
	// declared-region verification can test this target's own declarations
	// against this target's own unnameable traffic (scope_verify.go). The same
	// (target, txID) de-duplication that guards unmatchedPackets guards this,
	// so one packet reaching a target by two hops cannot corroborate twice.
	//
	// Bounded by scopeVerifyMaxPacketsPerTarget, which unmatchedPackets is NOT:
	// the count stays the honest total, this is the working set.
	unmatchedTxIDs []int64

	// ambiguousHops counts forwarder-hop observations in the window whose
	// truncated hash prefix matched this target AND at least one other
	// declared target — see ScopeAuditForwarding's doc comment for why
	// those hops are attributed to neither candidate instead of both.
	ambiguousHops int64
}

// scopeAuditPrefixIndex builds, for every even hex length from
// minForwarderHopHexLen up to a full 64-char pubkey, a lowercase prefix ->
// []target map. A forwarder hop of any valid truncated-hash length then
// resolves to its matching declared target(s) via a single map lookup,
// instead of a per-target SQL join — this is what keeps
// ScopeAuditForwarding's underlying scan a single query independent of how
// many repeaters have declared a region list.
func scopeAuditPrefixIndex(targets []string) map[int]map[string][]string {
	byLen := make(map[int]map[string][]string)
	for _, t := range targets {
		t = strings.ToLower(t)
		for l := minForwarderHopHexLen; l <= len(t); l += 2 {
			m, ok := byLen[l]
			if !ok {
				m = map[string][]string{}
				byLen[l] = m
			}
			prefix := t[:l]
			m[prefix] = append(m[prefix], t)
		}
	}
	return byLen
}

// scopeAuditSeenKey identifies one (target, transmission) pair for the
// de-duplication below. A struct key rather than the string it used to be
// built into: the hop scan reaches millions of rows on a 7d window, and every
// candidate hop was allocating a fresh "<target>|<txID>" string to ask a
// question that a comparable struct answers without allocating.

// scopeAuditSeenKey identifies one (target, transmission) pair for the
// de-duplication below. A struct key rather than the string it used to be
// built into: the hop scan reaches millions of rows on a 7d window, and every
// candidate hop was allocating a fresh "<target>|<txID>" string to ask a
// question that a comparable struct answers without allocating.
type scopeAuditSeenKey struct {
	target string
	txID   int64
}

// scopeForwarderRouteTypes are the FLOOD-family route types counted as
// forwarded (the SQL fragment scopeConformanceForwarderRouteTypesSQL spells
// the same set for the queries not yet converted).
var scopeForwarderRouteTypes = []int{0, 1}

// ScopeAuditForwarding makes one query for the whole window and attributes
// every forwarder hop it finds to targets, by the same truncated-hash prefix
// match ScopeConformance uses for a single pubkey.
//
// A hop is attributed only when its prefix matches EXACTLY ONE declared
// target. This endpoint exists to find a repeater that declares a region and
// is not actually forwarding it — crediting a hop to every target sharing
// its prefix would let a colliding neighbour's traffic silently paper over a
// real gap, and crediting nobody (the alternative of dropping the hop
// entirely) would invent failures for targets that simply share a collision-
// prone prefix. Instead, an ambiguous hop is credited to NEITHER candidate,
// and every candidate's ambiguousHops counter is incremented instead, so the
// row can say "this notObserved might just be a prefix collision" rather
// than presenting it as a confirmed finding. See scopeAuditTargetAgg's
// ambiguousHops field and ScopeAuditRow.AmbiguousHops.
//
// Each (target, transmission) pair is counted at most once even if seen via
// several observations or several hops of one path (a routing loop, or two
// hops colliding on the same truncated prefix), for both the attributed and
// the ambiguous count. The database returns each (transmission, hop) once;
// the seen set below removes the remaining repeats across hops.
// TestScopeAuditForwardingAttribution pins it.
func (s *PacketStore) ScopeAuditForwarding(ctx context.Context, since time.Time, targets []string) (map[string]*scopeAuditTargetAgg, error) {
	byLen := scopeAuditPrefixIndex(targets)
	result := make(map[string]*scopeAuditTargetAgg, len(targets))

	var prefixes []string
	for _, byPrefix := range byLen {
		for p := range byPrefix {
			prefixes = append(prefixes, p)
		}
	}
	hops, err := s.db.ListScopeAuditHops(ctx, database.ScopeAuditQuery{
		Since:      since,
		Prefixes:   prefixes,
		MinHopLen:  minForwarderHopHexLen,
		RouteTypes: scopeForwarderRouteTypes,
	})
	if err != nil {
		return nil, fmt.Errorf("scope audit forwarder scan: %w", err)
	}

	getAgg := func(target string) *scopeAuditTargetAgg {
		agg, ok := result[target]
		if !ok {
			agg = &scopeAuditTargetAgg{scopes: map[string]*ScopeObservation{}}
			result[target] = agg
		}
		return agg
	}

	seen := make(map[scopeAuditSeenKey]bool) // (target, txID) already counted (attributed or ambiguous)
	for _, h := range hops {
		candidates := byLen[len(h.Hop)][h.Hop]
		if len(candidates) == 0 {
			continue
		}

		if len(candidates) > 1 {
			for _, target := range candidates {
				key := scopeAuditSeenKey{target: target, txID: h.TxID}
				if seen[key] {
					continue
				}
				seen[key] = true
				getAgg(target).ambiguousHops++
			}
			continue
		}

		firstSeen := formatTS(h.FirstSeen)
		for _, target := range candidates {
			key := scopeAuditSeenKey{target: target, txID: h.TxID}
			if seen[key] {
				continue
			}
			seen[key] = true

			agg := getAgg(target)
			if h.ScopeName == nil {
				agg.unscopedPackets++
				continue
			}
			if *h.ScopeName == "" {
				// Unmatched: transport-scoped, but no configured region key
				// matched code1. Still not part of the declared/observed
				// comparison — it names no region, so it can never satisfy a
				// declaration — but it is the evidence that a notObserved
				// finding on this row may be a gap in this instance's
				// hashRegions rather than in the repeater's forwarding.
				agg.unmatchedPackets++
				if len(agg.unmatchedTxIDs) < scopeVerifyMaxPacketsPerTarget {
					agg.unmatchedTxIDs = append(agg.unmatchedTxIDs, h.TxID)
				}
				continue
			}
			name := normScope(*h.ScopeName)
			so, ok := agg.scopes[name]
			if !ok {
				so = &ScopeObservation{Scope: name, FirstSeen: firstSeen, LastSeen: firstSeen}
				agg.scopes[name] = so
			}
			so.Packets++
			if firstSeen < so.FirstSeen {
				so.FirstSeen = firstSeen
			}
			if firstSeen > so.LastSeen {
				so.LastSeen = firstSeen
			}
		}
	}
	return result, nil
}

// Scope audit configuration-state values — see ScopeAuditRow.ConfigState and
// scopeAuditConfigState.

const (
	ScopeConfigFull       = "full"        // named regions AND '*'
	ScopeConfigNoScopes   = "no-scopes"   // '*' only, no named regions
	ScopeConfigNoUnscoped = "no-unscoped" // named regions, no '*'
	ScopeConfigNoFlood    = "no-flood"    // neither named regions nor '*'
)

// scopeAuditConfigState classifies a repeater's declared-regions answer into
// one of four configuration states, from ONLY the two fields the caller has
// already parsed out of the raw declared list (namedRegions and wildcard) —
// never re-derived from regions_csv, which splitRegionsCSV/normScope have
// already reduced to exactly these two facts.
//
// The firmware exports the FLOOD-allowed set (region_map.exportNamesTo(...,
// REGION_DENY_FLOOD) in examples/simple_repeater/MyMesh.cpp), so "no named
// regions in the export" does not strictly prove "no regions defined": a
// repeater with regions defined but every one of them marked deny-flood
// exports exactly the same list as one with no region tree at all, and the
// two are indistinguishable from this data alone. That caveat lands on
// ScopeConfigNoScopes and ScopeConfigNoFlood, both of which read "zero named
// regions in the export". It does NOT land on the wildcard half of the
// classification: '*' absent from the export is exact, not an inference —
// it means the wildcard denies flooding, i.e. plain unscoped floods are not
// forwarded, full stop. That exactness is why ScopeConfigNoUnscoped needs no
// caveat, and why ScopeConfigNoFlood's "no unscoped forwarding" half is
// exact even though its "no named regions" half is not.
func scopeAuditConfigState(namedRegions []string, wildcard bool) string {
	switch {
	case len(namedRegions) > 0 && wildcard:
		return ScopeConfigFull
	case len(namedRegions) == 0 && wildcard:
		return ScopeConfigNoScopes
	case len(namedRegions) > 0 && !wildcard:
		return ScopeConfigNoUnscoped
	default:
		return ScopeConfigNoFlood
	}
}

// ScopeAuditRow is one repeater's declared-vs-observed comparison for
// GET /api/scope-audit — the network-wide answer to "which repeaters
// declare a region they are not actually forwarding". Every field is
// already normalised (no leading '#') and '*' is never present in
// DeclaredRegions/NotObserved/UndeclaredObserved — see DeclaredWildcard and
// WildcardContradiction for its counterpart.

type ScopeAuditRow struct {
	PublicKey string `json:"publicKey"`
	// Name/Role are pointers so "we hold no nodes row for this target" serialises
	// as null rather than "". A declared-regions row can name a repeater this
	// instance has never recorded, and an empty string would make that
	// indistinguishable from a node we DO know that simply has no name — the
	// same absent-is-not-empty rule the declared side already follows.
	Name *string         `json:"name"`
	Role *types.NodeRole `json:"role"`

	DeclaredRegions  []string `json:"declaredRegions"`  // '*' excluded — see DeclaredWildcard
	DeclaredWildcard bool     `json:"declaredWildcard"` // '*' present in the raw declared list
	// ConfigState is scopeAuditConfigState(DeclaredRegions, DeclaredWildcard)
	// — one of ScopeConfigFull/NoScopes/NoUnscoped/NoFlood, the config-state
	// reading of those same two fields so a caller does not have to
	// re-derive it from them. See scopeAuditConfigState's doc comment for
	// the caveat on the "no named regions" half of NoScopes/NoFlood.
	ConfigState string    `json:"configState"`
	DeclaredAt  time.Time `json:"declaredAt"` // ISO — age of the declared answer, not the window
	Truncated   bool      `json:"truncated"`  // declared list may have had entries silently dropped

	// NotObserved is declared regions with zero matched-forwarding observed
	// in the window — the headline this endpoint exists to surface.
	NotObserved []string `json:"notObserved"`
	// UndeclaredObserved is scopes this repeater was observed forwarding
	// that are absent from its declared list.
	UndeclaredObserved []ScopeObservation `json:"undeclaredObserved"`

	// ObservedUnscopedPackets is plain-FLOOD (no transport scope) packets
	// this repeater was observed forwarding in the window — the '*'
	// counterpart, per DeclaredWildcard's doc comment.
	ObservedUnscopedPackets int64 `json:"observedUnscopedPackets"`
	// WildcardContradiction is true when this repeater was observed
	// forwarding unscoped floods but its declared list omits '*' — it says
	// it does NOT forward them, and the traffic says otherwise.
	WildcardContradiction bool `json:"wildcardContradiction"`

	// AmbiguousHops counts forwarder-hop observations in the window whose
	// truncated hash prefix matched this target's pubkey AND at least one
	// other declared target's — see ScopeAuditForwarding's doc comment for
	// why those hops are attributed to neither and instead counted here on
	// every candidate. A non-zero value is a caveat, not a finding: any
	// NotObserved entry on this row could be explained by a colliding
	// neighbour's traffic rather than a real absence, and any
	// UndeclaredObserved entry is unaffected by it (ambiguous hops are never
	// attributed to a scope at all).
	AmbiguousHops int64 `json:"ambiguousHops"`

	// ObservedUnmatchedPackets counts packets this repeater was observed
	// forwarding whose transport scope matched no region key this instance
	// holds. Like AmbiguousHops it is a caveat rather than a finding, but the
	// two have different causes and different fixes: AmbiguousHops is a
	// pubkey-prefix collision between two repeaters and nobody's fault,
	// ObservedUnmatchedPackets is a missing entry in this instance's own
	// hashRegions and the reader can act on it. A non-zero value means any
	// NotObserved entry on this row may name a region this instance cannot
	// name rather than one the repeater is not forwarding.
	//
	// It says nothing about DeclaredWildcard: unmatched traffic IS scoped, so
	// it never feeds WildcardContradiction, which counts only plain unscoped
	// floods.
	ObservedUnmatchedPackets int64 `json:"observedUnmatchedPackets"`

	// ObservedUnmatchedSampled is how many of those packets verification could
	// actually look at: the per-target list is capped at
	// scopeVerifyMaxPacketsPerTarget and the window sample at
	// scopeVerifyMaxWindowPackets, while ObservedUnmatchedPackets keeps
	// counting past both.
	//
	// It exists because a client cannot otherwise subtract the two fields
	// honestly. RegionEvidence can only ever count packets inside this sample,
	// so on a row where this is smaller than ObservedUnmatchedPackets the
	// difference between them is an upper bound on the unexplained traffic, not
	// a figure. Equal values mean the subtraction is exact.
	ObservedUnmatchedSampled int64 `json:"observedUnmatchedSampled"`

	// RegionEvidence maps a declared region to how many of this repeater's own
	// unmatched forwarded packets derive to it — see scope_verify.go. A region
	// reaching scopeVerifyMinCorroboration is removed from NotObserved, so this
	// field is NOT what decides the chip's colour; NotObserved remains the sole
	// source of that. This exists so a client can say HOW a region was
	// established, and can explain a region that got exactly one hit and
	// therefore stayed in NotObserved.
	//
	// Absent regions simply had no matching traffic. Never nil in the response
	// — an empty object and a missing key mean the same thing, and an empty map
	// is the cheaper thing for a client to iterate.
	RegionEvidence map[string]int `json:"regionEvidence"`
}

// ScopeAuditResponse is the payload for GET /api/scope-audit. Only
// repeaters with at least one declared-regions answer are included — a
// repeater never successfully asked is absent, not shown declaring nothing
// (see AllCurrentDeclaredRegions).
type ScopeAuditResponse struct {
	Window    string          `json:"window"`
	Since     time.Time       `json:"since"` // ISO — start of the observed-forwarding window
	Repeaters []ScopeAuditRow `json:"repeaters"`
}

// NodeScopesResponse is the payload for GET /api/nodes/{pubkey}/scopes: the
// observed-forwarding side (ScopeConformance, embedded BY VALUE so its three
// scope states sit at the JSON top level — unmatched and unscoped are
// separate counts and a matched scope never has an empty name) plus the
// declared side (DeclaredRegions), returned together so the UI needs only
// one request per node rather than a second per-item call.
//
// ScopeConformance is embedded by value rather than as *ScopeConformance:
// encoding/json silently skips fields promoted through a nil embedded
// pointer instead of erroring, which would drop observed/unmatched/unscoped
// from the body entirely rather than surfacing the failure.

type scopesState struct {
	cacheMu sync.RWMutex
	cache   map[string]scopesCacheEntry
	sf      singleflight.Group

	// lastSeenBlacklistGen mirrors reachState's field: when the live
	// blacklist generation advances past this value, the cache is purged
	// wholesale on the next request. The 404 path itself is never cached
	// (the handler returns before a cache key is even computed), so this
	// only guards a pre-existing successful entry from outliving a
	// blacklist/hide edit made after it was filled.
	lastSeenBlacklistGen atomic.Uint64
}

type scopesCacheEntry struct {
	at  time.Time
	raw []byte
}

// nodeScopesWindowLookback maps the ?window= vocabulary to a lookback
// duration. Matches the sibling /api/scope-stats endpoint's vocabulary
// exactly (1h, 24h, 7d) rather than the broader ParseTimeWindow alias set
// (which also accepts 1d/3d/1w/30d) used by unrelated analytics endpoints.

func nodeScopesWindowLookback(window string) (time.Duration, bool) {
	switch window {
	case "1h":
		return time.Hour, true
	case "24h":
		return 24 * time.Hour, true
	case "7d":
		return 7 * 24 * time.Hour, true
	default:
		return 0, false
	}
}

// handleNodeScopes serves GET /api/nodes/{pubkey}/scopes?window=1h|24h|7d.
//
// A pubkey never heard forwarding anything is a valid question with an
// empty answer (200), not a 404 — ScopeConformance already treats it that
// way, and this handler performs no node-existence lookup that would
// override it.

// handleScopeAudit serves GET /api/scope-audit?window=1h|24h|7d: the
// network-wide answer to "which repeaters declare a region they are not
// actually forwarding". Unlike the per-repeater /api/nodes/{pubkey}/scopes,
// this compares every repeater that has ever declared a region list in one
// pass — see scopes.go's AllCurrentDeclaredRegions and ScopeAuditForwarding
// for why that stays a single scan rather than one query per repeater.
func (s *Server) handleScopeAudit(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	window := r.URL.Query().Get("window")
	if window == "" {
		window = "24h"
	}
	lookback, ok := nodeScopesWindowLookback(window)
	if !ok {
		writeError(w, 400, "window must be 1h, 24h, or 7d")
		return
	}

	sinceISO := time.Now().Add(-lookback).UTC()

	if cached, ok := s.scopeAuditCached(window); ok {
		writeJSON(w, cached)
		return
	}

	// singleflight: the compute below runs outside the cache mutex, so without
	// this every request arriving on a cold window ran its own full scan
	// concurrently. Reading every hop makes that scan seconds of work over
	// millions of rows, which is the shape that turns a thundering herd from
	// wasteful into expensive. Same treatment /api/observers and
	// /api/nodes/{pubkey}/reach already have.
	v, err, _ := s.scopeAuditSF.Do(window, func() (interface{}, error) {
		// Waiters that arrive while a scan is in flight are served by that
		// scan's result; this second look is for the caller that acquires the
		// group right after a winner stored one.
		if cached, ok := s.scopeAuditCached(window); ok {
			return cached, nil
		}
		resp, cErr := s.computeScopeAudit(ctx, window, sinceISO)
		if cErr != nil {
			return nil, cErr
		}
		s.scopeAuditStore(window, resp)
		return resp, nil
	})
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, v.(*ScopeAuditResponse))
}

// scopeAuditTTLFor is how long one window's computed audit stays fresh.
//
// 7d is not 30s because it does not cost what the others cost. Measured on a
// live-shaped database with 206 declared repeaters and 965k transmissions:
// 16.7s cold for 7d against 4.0s for 24h and 0.15s for 1h, with the 7d scan
// reading 3,470,188 hop rows. At a 30s TTL a single reader with that window
// open keeps the instance recomputing more than half the time, for an
// aggregate that moves at the pace of a week of traffic. Five minutes of
// staleness on a seven-day window is not a fact the reader can act on
// differently.
func scopeAuditTTLFor(window string) time.Duration {
	if window == "7d" {
		return 5 * time.Minute
	}
	return 30 * time.Second
}

// scopeAuditCached returns the cached response for a window while it is within
// that window's TTL.
func (s *Server) scopeAuditCached(window string) (*ScopeAuditResponse, bool) {
	s.scopeAuditMu.Lock()
	defer s.scopeAuditMu.Unlock()
	if s.scopeAuditCache == nil {
		return nil, false
	}
	cached, ok := s.scopeAuditCache[window]
	if !ok || time.Since(s.scopeAuditCachedAt[window]) >= scopeAuditTTLFor(window) {
		return nil, false
	}
	return cached, true
}

// scopeAuditStore publishes a freshly computed response for a window.
func (s *Server) scopeAuditStore(window string, resp *ScopeAuditResponse) {
	s.scopeAuditMu.Lock()
	defer s.scopeAuditMu.Unlock()
	if s.scopeAuditCache == nil {
		s.scopeAuditCache = make(map[string]*ScopeAuditResponse)
		s.scopeAuditCachedAt = make(map[string]time.Time)
	}
	s.scopeAuditCache[window] = resp
	s.scopeAuditCachedAt[window] = time.Now()
}

// computeScopeAudit builds one window's audit response: the declared lists and
// the forwarding evidence attributed to them. Split out of the handler so the
// cache and its singleflight wrap a plain function instead of a request.
func (s *Server) computeScopeAudit(ctx context.Context, window string, sinceISO time.Time) (*ScopeAuditResponse, error) {
	declared, err := s.db.AllCurrentDeclaredRegions(ctx)
	if err != nil {
		return nil, err
	}

	targets := make([]string, 0, len(declared))
	for _, d := range declared {
		targets = append(targets, strings.ToLower(d.Target))
	}

	forwarding := map[string]*scopeAuditTargetAgg{}
	if s.store != nil {
		forwarding, err = s.store.ScopeAuditForwarding(ctx, sinceISO, targets)
		if err != nil {
			return nil, err
		}
	}

	// Declared-region verification: a region this instance holds no key for is
	// unnameable, not absent, and the audit can settle which by deriving the key
	// from the repeater's own declaration and testing it against that
	// repeater's own unnameable traffic. One verifier serves every row so each
	// (region, transmission) pair is derived at most once — see scope_verify.go
	// for why that memo is what keeps this affordable.
	//
	// A failure here degrades to "no verification" rather than failing the
	// request: the audit was useful before this existed and must stay useful if
	// the extra query errors.
	var verifier *scopeVerifier
	if s.store != nil {
		unmatchedRows, truncated, uErr := s.store.unmatchedTransmissionsInWindow(ctx, sinceISO)
		if uErr != nil {
			log.Printf("[scope-audit] declared-region verification unavailable: %v", uErr)
		} else {
			if truncated {
				// Said out loud rather than absorbed: past the cap a region can
				// hold evidence this refresh did not look at, so a grey chip
				// means "not corroborated in this sample", not "not forwarded".
				log.Printf("[scope-audit] window %s holds more than %d unnameable packets; verification used the most recent %d and may under-report evidence",
					window, scopeVerifyMaxWindowPackets, scopeVerifyMaxWindowPackets)
			}
			verifier = newScopeVerifier(unmatchedRows)
		}
	}

	identities, err := s.db.ListNodeIdentities(ctx, targets)

	resp := &ScopeAuditResponse{Window: window, Since: sinceISO, Repeaters: []ScopeAuditRow{}}
	for _, d := range declared {
		pk := strings.ToLower(d.Target)
		if s.cfg != nil && s.cfg.IsBlacklisted(pk) {
			continue
		}
		id := identities[pk]
		// A target with no nodes row has no name to match a hidden-prefix rule
		// against; it cannot be hidden by name, so only check when we have one.
		if id.Name != nil && s.cfg != nil && s.cfg.IsNameHidden(*id.Name) {
			continue
		}

		// splitDeclaredRegions is shared with /api/nodes (scope_config_state
		// and declared_regions), so the map and this page read one declared
		// list the same way. It uses isScopeWildcard rather than a bare "*"
		// compare (#2006 review), and normScope's the named regions to mirror
		// the observed side (scopes.go: agg.scopes keys are already
		// normScope'd): regions_csv is not guaranteed to arrive with '#'
		// already stripped.
		declaredNamed, declaredWildcard := splitDeclaredRegions(d.RegionsCSV)
		declaredSet := make(map[string]bool, len(declaredNamed))
		for _, rgn := range declaredNamed {
			declaredSet[rgn] = true
		}

		agg := forwarding[pk]

		// Verify the declared regions this repeater has no NAMED evidence for,
		// against its own unmatched traffic. Regions already observed by name
		// need no verification and are not tested — that keeps the candidate
		// set to exactly the open questions, which is also what keeps the
		// verifier's work proportional to the problem rather than to the fleet.
		unnamed := []string{}
		for _, rgn := range declaredNamed {
			if agg == nil || agg.scopes[rgn] == nil {
				unnamed = append(unnamed, rgn)
			}
		}
		regionEvidence := map[string]int{}
		verifiedSet := map[string]bool{}
		if verifier != nil && agg != nil && len(unnamed) > 0 {
			// capVerifyRegions bounds the per-target half of the verifier's
			// work. The declared list arrives from a collector and its LENGTH
			// is not validated anywhere on the way in, while each distinct name
			// costs a full pass over the packet set.
			regionEvidence = verifier.evidence(agg.unmatchedTxIDs, capVerifyRegions(unnamed))
			for _, rgn := range verifier.verified(regionEvidence) {
				verifiedSet[rgn] = true
			}
		}
		notObserved := []string{}
		for _, rgn := range unnamed {
			if !verifiedSet[rgn] {
				notObserved = append(notObserved, rgn)
			}
		}

		undeclared := []ScopeObservation{}
		if agg != nil {
			names := make([]string, 0, len(agg.scopes))
			for name := range agg.scopes {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, name := range names {
				if !declaredSet[name] {
					undeclared = append(undeclared, *agg.scopes[name])
				}
			}
		}

		var unscopedPackets, ambiguousHops, unmatchedPackets, unmatchedSampled int64
		if agg != nil {
			unscopedPackets = agg.unscopedPackets
			ambiguousHops = agg.ambiguousHops
			unmatchedPackets = agg.unmatchedPackets
			// What verification could actually see, against what was counted.
			// The list is capped; the count is not.
			unmatchedSampled = int64(len(agg.unmatchedTxIDs))
		}

		resp.Repeaters = append(resp.Repeaters, ScopeAuditRow{
			PublicKey:                pk,
			Name:                     id.Name,
			Role:                     id.Role,
			DeclaredRegions:          declaredNamed,
			DeclaredWildcard:         declaredWildcard,
			ConfigState:              scopeAuditConfigState(declaredNamed, declaredWildcard),
			DeclaredAt:               d.ObservedAt,
			Truncated:                d.Truncated,
			NotObserved:              notObserved,
			UndeclaredObserved:       undeclared,
			ObservedUnscopedPackets:  unscopedPackets,
			WildcardContradiction:    unscopedPackets > 0 && !declaredWildcard,
			AmbiguousHops:            ambiguousHops,
			ObservedUnmatchedPackets: unmatchedPackets,
			ObservedUnmatchedSampled: unmatchedSampled,
			RegionEvidence:           regionEvidence,
		})
	}

	// Interesting rows first: a repeater silently not forwarding a declared
	// region is the headline this endpoint exists to surface, ranked by how
	// many declared regions it's missing. The wildcard contradiction and
	// undeclared-observed counts are secondary tie-breaks — flags on the
	// row, not a separate ranking. Full agreement (no issues at all) sorts
	// to the bottom, alphabetically, so it doesn't crowd out the rows that
	// matter.
	sort.Slice(resp.Repeaters, func(i, j int) bool {
		a, b := resp.Repeaters[i], resp.Repeaters[j]
		if len(a.NotObserved) != len(b.NotObserved) {
			return len(a.NotObserved) > len(b.NotObserved)
		}
		if a.WildcardContradiction != b.WildcardContradiction {
			return a.WildcardContradiction
		}
		if len(a.UndeclaredObserved) != len(b.UndeclaredObserved) {
			return len(a.UndeclaredObserved) > len(b.UndeclaredObserved)
		}
		// Fall back to the pubkey for an unnamed or unknown node so the order is
		// still total and stable rather than grouping every nameless row together.
		an, bn := a.PublicKey, b.PublicKey
		if a.Name != nil && *a.Name != "" {
			an = *a.Name
		}
		if b.Name != nil && *b.Name != "" {
			bn = *b.Name
		}
		return an < bn
	})

	return resp, nil
}
