package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/gorilla/mux"
)

// Full 64-hex pubkeys for fixtures. path_json hops are truncated hashes of
// 1 to 4 bytes, so the forwarder join matches a hop as a PREFIX of the full
// pubkey rather than by equality; the tests need a real full-length key to
// exercise that.
var (
	testFullPubkeyA = "1a2b" + strings.Repeat("11", 30) // 64 hex chars
	testFullPubkeyB = "bbbb" + strings.Repeat("22", 30) // 64 hex chars
)

// newScopeTestStore wires a *PacketStore to a freshly seeded scope-conformance
// schema. ScopeConformance is a pure SQL read against s.db, so no other
// PacketStore field needs to be populated.
func newScopeTestStore(t *testing.T) *PacketStore {
	t.Helper()
	db := setupTestDB(t)
	return newBareStore(db)
}

// scopeSeed carries the code1/scope_name pair for one of the three states a
// seeded transmission can be in. scopeName mirrors scopeNameForDB's own
// encoding: nil means the packet carried no scope at all, a non-nil pointer
// to an empty string means transport-scoped but unmatched, and a non-nil
// pointer to a name means matched.
type scopeSeed struct {
	code1     string
	scopeName *string
}

func scopeMatched(name string) scopeSeed {
	n := name
	return scopeSeed{code1: "1234", scopeName: &n}
}

func scopeUnmatched() scopeSeed {
	empty := ""
	return scopeSeed{code1: "1234", scopeName: &empty}
}

func scopeUnscoped() scopeSeed {
	return scopeSeed{code1: "0000", scopeName: nil}
}

var scopeSeedCounter int

// seedTransmissionRouteAt is seedTransmissionRoute with an explicit
// first_seen, for tests (e.g. the handler tests below) that need a
// transmission to fall inside a real-wall-clock ?window= lookback rather
// than the fixed date the ScopeConformance unit tests above use.
func seedTransmissionRouteAt(t *testing.T, s *PacketStore, forwarder string, seed scopeSeed, routeType types.RouteType, firstSeen time.Time) {
	t.Helper()
	ctx := t.Context()
	scopeSeedCounter++

	txID, _, err := s.db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		Hash:        fmt.Sprintf("%016x", scopeSeedCounter),
		RawHex:      "AA",
		RouteType:   routeType,
		PayloadType: types.PayloadType(1),
		DecodedJSON: "{}",
		ScopeName:   seed.scopeName,
	}, firstSeen)
	if err != nil {
		t.Fatalf("seed transmission: %v", err)
	}

	_, inserted, err := s.db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: txID,
		ObserverIdx:    nil,
		SNR:            nil,
		RSSI:           nil,
		Timestamp:      firstSeen,
	}, []string{strings.ToLower(forwarder)}, []*string{nil})
	if err != nil {
		t.Fatalf("seed observation: %v", err)
	}
	if !inserted {
		t.Fatalf("expected observation to be inserted for tx %d", txID)
	}
}

func TestScopeAuditForwardingAttributesUnambiguousHop(t *testing.T) {
	ctx := t.Context()
	s := newScopeTestStore(t)
	hop := testFullPubkeyA[:4]
	recent := time.Now().UTC().Add(-time.Minute)
	seedTransmissionRouteAt(t, s, hop, scopeMatched("#be"), types.RouteTransportFlood, recent)

	got, err := s.ScopeAuditForwarding(ctx, time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC), []string{testFullPubkeyA})
	if err != nil {
		t.Fatal(err)
	}
	agg := got[testFullPubkeyA]
	if agg == nil || agg.scopes["be"] == nil || agg.scopes["be"].Packets != 1 {
		t.Fatalf("want the hop attributed to the sole matching target, got %+v", got)
	}
	if agg.ambiguousHops != 0 {
		t.Errorf("ambiguousHops = %d, want 0 — nothing here is ambiguous", agg.ambiguousHops)
	}
}

// TestScopeAuditForwardingAmbiguousHopCreditsNeitherTarget is FIX 1's core
// case: two declared targets share a 2-byte prefix. A hop at that prefix
// must be attributed to NEITHER of them (crediting both would let a
// colliding neighbour's traffic silently paper over a real notObserved
// finding), and BOTH candidates' ambiguousHops counters must increment (so
// the row can surface the caveat regardless of which candidate is being
// looked at).
func TestScopeAuditForwardingAmbiguousHopCreditsNeitherTarget(t *testing.T) {
	ctx := t.Context()
	s := newScopeTestStore(t)
	pkA := "1a2b" + strings.Repeat("11", 30)
	pkB := "1a2b" + strings.Repeat("22", 30) // shares pkA's first 4 hex chars
	hop := pkA[:4]
	recent := time.Now().UTC().Add(-time.Minute)
	seedTransmissionRouteAt(t, s, hop, scopeMatched("#be"), types.RouteTransportFlood, recent)

	targets := []string{pkA, pkB}
	got, err := s.ScopeAuditForwarding(ctx, time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC), targets)
	if err != nil {
		t.Fatal(err)
	}
	for _, pk := range targets {
		agg := got[pk]
		if agg == nil {
			t.Fatalf("%s: want an agg present to carry the ambiguity counter, got none (result = %+v)", pk, got)
		}
		if len(agg.scopes) != 0 {
			t.Errorf("%s: scopes = %+v, want empty — an ambiguous hop must not be attributed to either candidate", pk, agg.scopes)
		}
		if agg.unscopedPackets != 0 {
			t.Errorf("%s: unscopedPackets = %d, want 0", pk, agg.unscopedPackets)
		}
		if agg.ambiguousHops != 1 {
			t.Errorf("%s: ambiguousHops = %d, want 1", pk, agg.ambiguousHops)
		}
	}
}

// --- GET /api/scope-audit handler tests ---

// setupScopeAuditServer builds a *Server over the ScopeConformance schema
// plus a nodes table carrying the confirmed-scope columns from #1865/#1971,
// which is where this endpoint reads its declared side. detectSchema must run
// after the columns exist so hasConfiguredScope is picked up; without it the
// handler correctly serves an empty audit and every assertion below would pass
// vacuously.
func setupScopeAuditServer(t *testing.T) (*Server, *mux.Router) {
	t.Helper()
	db := setupTestDB(t)

	cfg := &Config{Port: 3000}
	hub := NewHub()
	srv := NewServer(db, cfg, hub)
	srv.store = newBareStore(db)
	router := mux.NewRouter()
	srv.RegisterRoutes(router)
	return srv, router
}

// insertDeclared records a node's confirmed region list. Keeps the original
// signature so the ported cases read unchanged, but writes to the upstream
// source: nodes.configured_scope, which an observer /neighbors report fills
// (#1865/#1971).
//
// truncated is accepted and ignored. The old source recorded whether an answer
// was cut short; /neighbors has its own size cap but does not report whether it
// fired, so the audit cannot claim either way and the page omits the caveat
// rather than showing a wrong one. Kept in the signature so the ported tests
// still document which fixtures meant "this answer was incomplete".
func insertDeclared(t *testing.T, srv *Server, target string, observedAt time.Time, regionsCSV string, truncated int) {
	t.Helper()
	_ = truncated
	ctx := t.Context()
	if err := srv.db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: target,
		Name:      "",
		Role:      "repeater",
		Lat:       nil,
		Lon:       nil,
		RxTime:    observedAt,
		IngestNow: observedAt,
	}); err != nil {
		t.Fatal(err)
	}
	if err := srv.db.UpdateNodeConfiguredScope(ctx, target, regionsCSV, &observedAt); err != nil {
		t.Fatal(err)
	}
}

func getScopeAudit(t *testing.T, router *mux.Router, query string) api.ScopeAuditResponse {
	t.Helper()
	req := httptest.NewRequest("GET", "/api/scope-audit"+query, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var got api.ScopeAuditResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return got
}

// TestHandleScopeAuditNormalisesHashPrefix pins trap 1: transmissions.scope_name
// keeps the '#' (hashRegions config), regions_csv arrives from the firmware
// with it already stripped. Declared "be-van" and observed "#be-van" must be
// recognised as the same scope, not reported as both missing and undeclared.
func TestHandleScopeAuditNormalisesHashPrefix(t *testing.T) {
	srv, router := setupScopeAuditServer(t)
	pk := testFullPubkeyA
	hop := pk[:4]
	recent := time.Now().UTC().Add(-time.Minute)
	insertDeclared(t, srv, pk, time.Now().UTC(), "be-van", 0)
	seedTransmissionRouteAt(t, srv.store, hop, scopeMatched("#be-van"), types.RouteTransportFlood, recent)

	// FIX 2: the DECLARED side must be normalised too, not just observed.
	// regions_csv is not guaranteed to arrive with '#' already stripped (a
	// firmware variant, an operator-seeded row, or a future collector could
	// leave it on) — declaring "#be-van" and observing "#be-van" (which the
	// observed side always normalises to "be-van") must still match. Without
	// the fix, declaredNamed/declaredSet keep the raw "#be-van", so this
	// second repeater's row would show BOTH "#be-van" in notObserved (the
	// declared value never matches the normalised agg.scopes key) AND
	// "be-van" in undeclaredObserved (the normalised observed name isn't in
	// declaredSet) — the exact trap normScope exists to prevent, reappearing
	// on the other side of the comparison. Seeded alongside pk (not as a
	// separate getScopeAudit call) so both land in one response and neither
	// is masked by the 30s response cache.
	pk2 := testFullPubkeyB
	hop2 := pk2[:4]
	insertDeclared(t, srv, pk2, time.Now().UTC(), "#be-van", 0)
	seedTransmissionRouteAt(t, srv.store, hop2, scopeMatched("#be-van"), types.RouteTransportFlood, recent)

	got := getScopeAudit(t, router, "")
	if len(got.Repeaters) != 2 {
		t.Fatalf("repeaters = %+v, want exactly 2", got.Repeaters)
	}

	row := findScopeAuditRow(t, got.Repeaters, pk)
	if len(row.NotObserved) != 0 {
		t.Errorf("notObserved = %v, want empty — declared \"be-van\" observed as \"#be-van\" must match after normalisation", row.NotObserved)
	}
	if len(row.UndeclaredObserved) != 0 {
		t.Errorf("undeclaredObserved = %+v, want empty", row.UndeclaredObserved)
	}

	row2 := findScopeAuditRow(t, got.Repeaters, pk2)
	if len(row2.DeclaredRegions) != 1 || row2.DeclaredRegions[0] != "be-van" {
		t.Errorf("declaredRegions = %v, want [\"be-van\"] — the leading '#' must be stripped from the declared side too", row2.DeclaredRegions)
	}
	if len(row2.NotObserved) != 0 {
		t.Errorf("notObserved = %v, want empty — declared \"#be-van\" observed as \"#be-van\" must match after normalising BOTH sides", row2.NotObserved)
	}
	if len(row2.UndeclaredObserved) != 0 {
		t.Errorf("undeclaredObserved = %+v, want empty", row2.UndeclaredObserved)
	}
}

// findScopeAuditRow locates the row for pk, failing the test if absent —
// used wherever more than one repeater is seeded in the same response, since
// sort order is not by pubkey and cannot be relied on to pick the right row.
func findScopeAuditRow(t *testing.T, rows []api.ScopeAuditRow, pk string) api.ScopeAuditRow {
	t.Helper()
	for _, r := range rows {
		if r.PublicKey == pk {
			return r
		}
	}
	t.Fatalf("no row for pubkey %s in %+v", pk, rows)
	return api.ScopeAuditRow{}
}

// TestHandleScopeAuditExcludesWildcardFromComparison pins trap 2: '*' is the
// root of the region tree (governs plain FLOOD), not a scope. It must never
// appear in declaredRegions (or its count) or in notObserved/undeclaredObserved.
func TestHandleScopeAuditExcludesWildcardFromComparison(t *testing.T) {
	srv, router := setupScopeAuditServer(t)
	pk := testFullPubkeyA
	insertDeclared(t, srv, pk, time.Now().UTC(), "*,be", 0)

	got := getScopeAudit(t, router, "")
	if len(got.Repeaters) != 1 {
		t.Fatalf("repeaters = %+v, want 1", got.Repeaters)
	}
	row := got.Repeaters[0]
	if !row.DeclaredWildcard {
		t.Error("declaredWildcard = false, want true")
	}
	if len(row.DeclaredRegions) != 1 || row.DeclaredRegions[0] != "be" {
		t.Errorf("declaredRegions = %v, want [\"be\"] — '*' must be excluded from the region list and its count", row.DeclaredRegions)
	}
	for _, r := range row.NotObserved {
		if r == "*" {
			t.Error("notObserved contains '*' — it must never be treated as a scope")
		}
	}
}

// TestHandleScopeAuditOmitsRepeaterWithNoDeclaredRow pins trap 3: a repeater
// that was never successfully asked is absent from the response entirely —
// not shown as a row declaring nothing, which is a distinct, meaningful fact.
func TestHandleScopeAuditOmitsRepeaterWithNoDeclaredRow(t *testing.T) {
	srv, router := setupScopeAuditServer(t)
	now := time.Now().UTC()
	declaredPk := testFullPubkeyA
	neverAskedPk := testFullPubkeyB
	insertDeclared(t, srv, declaredPk, now, "be", 0)
	seedNode(t, srv.db, neverAskedPk, "never-asked", "repeater", nil, nil, now)

	got := getScopeAudit(t, router, "")
	if len(got.Repeaters) != 1 || got.Repeaters[0].PublicKey != declaredPk {
		t.Fatalf("repeaters = %+v, want exactly the one repeater that has declared", got.Repeaters)
	}
}

// TestHandleScopeAuditUnknownNodeNameIsNullNotEmpty proves a declared target
// this instance holds no nodes row for serialises name/role as null, not "".
// A declared-regions answer can name a repeater the network has never recorded,
// and "" would make that indistinguishable from a node we DO know that simply
// has no name — the same absent-is-not-empty rule the declared side follows.
func TestHandleScopeAuditUnknownNodeNameIsNullNotEmpty(t *testing.T) {
	srv, router := setupScopeAuditServer(t)
	pk := testFullPubkeyA
	insertDeclared(t, srv, pk, time.Now().UTC(), "be", 0)
	// deliberately NO nodes row for pk

	req := httptest.NewRequest("GET", "/api/scope-audit", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	if !strings.Contains(body, `"name":null`) {
		t.Errorf(`body must carry "name":null for a target with no nodes row, got: %s`, body)
	}
	if strings.Contains(body, `"name":""`) {
		t.Error(`"name":"" collapses "unknown node" into "node with no name"`)
	}

	var got api.ScopeAuditResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Repeaters) != 1 {
		t.Fatalf("repeaters = %d, want 1", len(got.Repeaters))
	}
	if got.Repeaters[0].Name != nil {
		t.Errorf("Name = %q, want nil", *got.Repeaters[0].Name)
	}
}

// TestHandleScopeAuditWildcardContradiction: observed forwarding unscoped
// (plain-FLOOD) traffic while the declared list omits '*' is the wildcard
// contradiction this endpoint must flag — the repeater says it does NOT
// forward those packets, and the traffic says otherwise.
func TestHandleScopeAuditWildcardContradiction(t *testing.T) {
	srv, router := setupScopeAuditServer(t)
	pk := testFullPubkeyA
	hop := pk[:4]
	insertDeclared(t, srv, pk, time.Now().UTC(), "be", 0) // no '*'
	recent := time.Now().UTC().Add(-time.Minute)
	seedTransmissionRouteAt(t, srv.store, hop, scopeUnscoped(), types.RouteFlood, recent)

	got := getScopeAudit(t, router, "")
	if len(got.Repeaters) != 1 {
		t.Fatalf("repeaters = %+v, want 1", got.Repeaters)
	}
	row := got.Repeaters[0]
	if !row.WildcardContradiction {
		t.Error("wildcardContradiction = false, want true — observed unscoped forwarding but '*' not declared")
	}
	if row.ObservedUnscopedPackets != 1 {
		t.Errorf("observedUnscopedPackets = %d, want 1", row.ObservedUnscopedPackets)
	}
}

// TestHandleScopeAuditWildcardDeclaredIsNotAContradiction is the other half:
// the same observed unscoped traffic is expected, not a contradiction, once
// '*' is declared.
func TestHandleScopeAuditWildcardDeclaredIsNotAContradiction(t *testing.T) {
	srv, router := setupScopeAuditServer(t)
	pk := testFullPubkeyA
	hop := pk[:4]
	insertDeclared(t, srv, pk, time.Now().UTC(), "*,be", 0)
	recent := time.Now().UTC().Add(-time.Minute)
	seedTransmissionRouteAt(t, srv.store, hop, scopeUnscoped(), types.RouteFlood, recent)

	got := getScopeAudit(t, router, "")
	if got.Repeaters[0].WildcardContradiction {
		t.Error("wildcardContradiction = true, want false — '*' IS declared, so unscoped forwarding is expected")
	}
}

// TestHandleScopeAuditNotObservedAndUndeclaredObserved exercises both
// comparison directions in one repeater: a declared region with zero
// observed forwarding, and an observed scope absent from the declared list.
func TestHandleScopeAuditNotObservedAndUndeclaredObserved(t *testing.T) {
	srv, router := setupScopeAuditServer(t)
	pk := testFullPubkeyA
	hop := pk[:4]
	insertDeclared(t, srv, pk, time.Now().UTC(), "be,be-vlg", 0)
	recent := time.Now().UTC().Add(-time.Minute)
	seedTransmissionRouteAt(t, srv.store, hop, scopeMatched("#be"), types.RouteTransportFlood, recent)
	seedTransmissionRouteAt(t, srv.store, hop, scopeMatched("#de-nw"), types.RouteTransportFlood, recent)

	got := getScopeAudit(t, router, "")
	row := got.Repeaters[0]
	if len(row.NotObserved) != 1 || row.NotObserved[0] != "be-vlg" {
		t.Errorf("notObserved = %v, want [\"be-vlg\"]", row.NotObserved)
	}
	if len(row.UndeclaredObserved) != 1 || row.UndeclaredObserved[0].Scope != "de-nw" {
		t.Errorf("undeclaredObserved = %+v, want exactly \"de-nw\"", row.UndeclaredObserved)
	}
}

// TestHandleScopeAuditSurfacesAmbiguousHops is FIX 1's handler-level case:
// two repeaters both declare "be" and share a 2-byte pubkey prefix. The one
// hop seen in the window can't be attributed to either, so both rows must
// still show "be" as notObserved (an ambiguous hop invents no attribution,
// so it cannot silently satisfy the declared region) AND both rows must
// carry ambiguousHops=1, the caveat that the notObserved finding might be a
// prefix collision rather than a real gap.
func TestHandleScopeAuditSurfacesAmbiguousHops(t *testing.T) {
	srv, router := setupScopeAuditServer(t)
	pkA := "1a2b" + strings.Repeat("11", 30)
	pkB := "1a2b" + strings.Repeat("22", 30)
	hop := pkA[:4]
	insertDeclared(t, srv, pkA, time.Now().UTC(), "be", 0)
	insertDeclared(t, srv, pkB, time.Now().UTC(), "be", 0)
	recent := time.Now().UTC().Add(-time.Minute)
	seedTransmissionRouteAt(t, srv.store, hop, scopeMatched("#be"), types.RouteTransportFlood, recent)

	got := getScopeAudit(t, router, "")
	if len(got.Repeaters) != 2 {
		t.Fatalf("repeaters = %+v, want 2", got.Repeaters)
	}
	for _, row := range got.Repeaters {
		if row.AmbiguousHops != 1 {
			t.Errorf("%s: ambiguousHops = %d, want 1", row.PublicKey, row.AmbiguousHops)
		}
		if len(row.NotObserved) != 1 || row.NotObserved[0] != "be" {
			t.Errorf("%s: notObserved = %v, want [\"be\"] — an ambiguous hop must not silently satisfy the declared region", row.PublicKey, row.NotObserved)
		}
	}
}

// TestHandleScopeAuditSortsMissingRegionsFirst: the repeater with a declared
// region it is not forwarding must rank above a repeater in full agreement —
// that's the headline this endpoint exists to surface, not the boring majority.
func TestHandleScopeAuditSortsMissingRegionsFirst(t *testing.T) {
	srv, router := setupScopeAuditServer(t)
	agreePk := testFullPubkeyA
	missingPk := testFullPubkeyB
	agreeHop := agreePk[:4]
	missingHop := missingPk[:4]
	insertDeclared(t, srv, agreePk, time.Now().UTC(), "be", 0)
	insertDeclared(t, srv, missingPk, time.Now().UTC(), "be,be-vlg", 0)
	recent := time.Now().UTC().Add(-time.Minute)
	seedTransmissionRouteAt(t, srv.store, agreeHop, scopeMatched("#be"), types.RouteTransportFlood, recent)
	seedTransmissionRouteAt(t, srv.store, missingHop, scopeMatched("#be"), types.RouteTransportFlood, recent)
	// missingPk never forwards be-vlg -> 1 missing declared region; agreePk has 0.

	got := getScopeAudit(t, router, "")
	if len(got.Repeaters) != 2 {
		t.Fatalf("repeaters = %+v, want 2", got.Repeaters)
	}
	if got.Repeaters[0].PublicKey != missingPk {
		t.Errorf("repeaters[0] = %s, want %s (the row missing a declared region) ranked first", got.Repeaters[0].PublicKey, missingPk)
	}
}

// TestHandleScopeAuditWindowVocabulary confirms this endpoint matches the
// vocabulary used by the sibling /api/scope-stats and /api/nodes/{pubkey}/scopes
// endpoints (1h, 24h, 7d), not the broader ParseTimeWindow alias set.
func TestHandleScopeAuditWindowVocabulary(t *testing.T) {
	_, router := setupScopeAuditServer(t)
	req := httptest.NewRequest("GET", "/api/scope-audit?window=30d", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("window=30d: status = %d, want 400 (not part of this endpoint's vocabulary)", w.Code)
	}
}

// TestHandleScopeAuditFiltersBlacklistedNode confirms a blacklisted repeater
// is dropped from the audit even though it has declared a region list,
// mirroring the blacklist filtering applied by other multi-node endpoints.
func TestHandleScopeAuditFiltersBlacklistedNode(t *testing.T) {
	srv, router := setupScopeAuditServer(t)
	pk := testFullPubkeyA
	insertDeclared(t, srv, pk, time.Now().UTC(), "be", 0)
	srv.cfg.NodeBlacklist = []string{pk}

	got := getScopeAudit(t, router, "")
	if len(got.Repeaters) != 0 {
		t.Errorf("repeaters = %+v, want empty — blacklisted node must be excluded", got.Repeaters)
	}
}

// TestHandleScopeAuditFiltersHiddenNamePrefix is the scope-audit twin of
// TestHandleScopeAuditFiltersBlacklistedNode, covering FIX 5: a repeater
// whose known name matches an operator-configured hidden-name prefix is
// excluded. It also pins the subtler half of the `id.Name != nil` guard in
// handleScopeAudit — a declared target this instance holds NO nodes row for
// has no name to test a hidden-prefix rule against, so it must NOT be
// filtered merely for lacking a name; only a KNOWN, matching name is ever
// grounds for hiding.
func TestHandleScopeAuditFiltersHiddenNamePrefix(t *testing.T) {
	srv, router := setupScopeAuditServer(t)
	now := time.Now().UTC()
	hiddenPk := testFullPubkeyA
	unknownPk := testFullPubkeyB
	insertDeclared(t, srv, hiddenPk, now, "be", 0)
	insertDeclared(t, srv, unknownPk, now, "be", 0)
	// The declared list lives ON the nodes row (configured_scope), so
	// insertDeclared has already created it. The name is set afterwards so
	// insertDeclared's empty name cannot overwrite it.
	seedNode(t, srv.db, hiddenPk, "🚫 ban me", "repeater", nil, nil, now)
	// unknownPk deliberately gets NO name: the row exists but its name stays
	// empty. A node whose name we do not know must never be hidden by a
	// name-prefix rule.
	srv.cfg.SetHiddenNamePrefixes([]string{"🚫"})

	got := getScopeAudit(t, router, "")
	if len(got.Repeaters) != 1 || got.Repeaters[0].PublicKey != unknownPk {
		t.Fatalf("repeaters = %+v, want exactly the unknown-name target — the hidden-name-prefixed repeater must be excluded, and the nameless one must NOT be excluded merely for having no name", got.Repeaters)
	}
}

// --- Scope audit config-state classification ---

// TestScopeAuditConfigStateClassification is a table-driven test of the pure
// classification function scopeAuditConfigState — the derivation is a plain
// switch over (namedRegions, wildcard), so each of the four cells is worth
// pinning directly rather than only indirectly via the handler.
func TestScopeAuditConfigStateClassification(t *testing.T) {
	tests := []struct {
		name         string
		namedRegions []string
		wildcard     bool
		want         string
	}{
		{"named and wildcard", []string{"be"}, true, ScopeConfigFull},
		{"wildcard only", nil, true, ScopeConfigNoScopes},
		{"named only", []string{"be"}, false, ScopeConfigNoUnscoped},
		{"neither", nil, false, ScopeConfigNoFlood},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scopeAuditConfigState(tt.namedRegions, tt.wildcard)
			if got != tt.want {
				t.Errorf("scopeAuditConfigState(%v, %v) = %q, want %q", tt.namedRegions, tt.wildcard, got, tt.want)
			}
		})
	}
}

// TestHandleScopeAuditConfigStateAllFourShapes seeds one repeater per shape
// of the (declaredRegions, declaredWildcard) pair — including the fourth,
// "answered but empty" shape (no named regions AND no '*') that sits outside
// the three named states — and confirms both that each row's ConfigState
// lands correctly AND that tallying ConfigState across the response (the
// same arithmetic the frontend summary line performs over d.repeaters)
// reproduces the exact per-state counts seeded here. That second check is
// what pins "the counts in the summary match the rows": if
// scopeAuditConfigState mis-tags even one row, the tally computed from the
// response no longer matches what was seeded here, and the test fails.
func TestHandleScopeAuditConfigStateAllFourShapes(t *testing.T) {
	srv, router := setupScopeAuditServer(t)
	now := time.Now().UTC()

	fullPk := strings.Repeat("11", 32)
	noScopesPk := strings.Repeat("22", 32)
	noUnscopedPk := strings.Repeat("33", 32)
	noFloodPk := strings.Repeat("44", 32)

	insertDeclared(t, srv, fullPk, now, "*,be", 0)
	insertDeclared(t, srv, noScopesPk, now, "*", 0)
	insertDeclared(t, srv, noUnscopedPk, now, "be", 0)
	insertDeclared(t, srv, noFloodPk, now, "", 0)

	got := getScopeAudit(t, router, "")
	if len(got.Repeaters) != 4 {
		t.Fatalf("repeaters = %+v, want exactly 4", got.Repeaters)
	}

	want := map[string]api.ScopeConfigState{
		fullPk:       ScopeConfigFull,
		noScopesPk:   ScopeConfigNoScopes,
		noUnscopedPk: ScopeConfigNoUnscoped,
		noFloodPk:    ScopeConfigNoFlood,
	}
	for pk, wantState := range want {
		row := findScopeAuditRow(t, got.Repeaters, pk)
		if row.ConfigState != wantState {
			t.Errorf("pk %s: configState = %q, want %q", pk, row.ConfigState, wantState)
		}
	}

	counts := map[api.ScopeConfigState]int{}
	for _, row := range got.Repeaters {
		counts[row.ConfigState]++
	}
	for _, state := range []api.ScopeConfigState{ScopeConfigFull, ScopeConfigNoScopes, ScopeConfigNoUnscoped, ScopeConfigNoFlood} {
		if counts[state] != 1 {
			t.Errorf("count of state %q across repeaters = %d, want 1 (summary tally must match the seeded rows)", state, counts[state])
		}
	}
}

// TestHandleNodeScopesDifferentWindowIsSeparateCacheEntry confirms the cache
// key includes window: a request for a different window must recompute
// rather than reuse another window's cached entry.

// --- Merging confirmed-scope sources (#1975) ---

func declaredFor(t *testing.T, srv *Server, target string) (database.DeclaredRegions, bool) {
	t.Helper()
	rows, err := srv.db.AllCurrentDeclaredRegions(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if r.Target == target {
			return r, true
		}
	}
	return database.DeclaredRegions{}, false
}

func TestDeclaredRegionsMergeUsesConfiguredScopeAlone(t *testing.T) {
	srv, _ := setupScopeAuditServer(t)
	insertDeclared(t, srv, testFullPubkeyA, time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), "#be,#eu", 0)
	got, ok := declaredFor(t, srv, testFullPubkeyA)
	if !ok || got.RegionsCSV != "#be,#eu" {
		t.Fatalf("got %+v (found=%v), want the configured_scope answer", got, ok)
	}
}

func TestDeclaredRegionsOlderReportDoesNotOverwriteNewer(t *testing.T) {
	srv, _ := setupScopeAuditServer(t)

	insertDeclared(t, srv, testFullPubkeyA, time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC), "#newer", 0)
	insertDeclared(t, srv, testFullPubkeyA, time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), "#old", 0)
	got, ok := declaredFor(t, srv, testFullPubkeyA)
	if !ok || got.RegionsCSV != "#newer" {
		t.Fatalf("got %+v (found=%v), want the newer report to survive an out-of-order older one", got, ok)
	}

	insertDeclared(t, srv, testFullPubkeyA, time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC), "#latest", 0)
	got, _ = declaredFor(t, srv, testFullPubkeyA)
	if got.RegionsCSV != "#latest" {
		t.Errorf("regions = %q, want a later report to replace the stored one", got.RegionsCSV)
	}
}
func TestDeclaredRegionsMergeWithNoSourcesIsEmptyNotAnError(t *testing.T) {
	db := dbtest.New(t)
	rows, err := db.AllCurrentDeclaredRegions(t.Context())
	if err != nil {
		t.Fatalf("no sources must not be an error: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("rows = %+v, want empty", rows)
	}
}

// --- Every-hop forwarder attribution and the scan that pays for it ---
//
// These cover the change from crediting path[last] to crediting every hop of a
// flood-family route, and the two-column scan plus per-window TTL that keeps
// the wider scan affordable.

// seedTransmissionPathAt seeds one transmission whose single observation
// carries a MULTI-hop path. A one-hop seed cannot tell the two reasons a node
// gets attributed apart — it is simultaneously path[0] and path[last] — so the
// mid-path cases below need a path with something after the target on it.
//
// Hops are upper-cased for the same reason seedTransmissionRoute does it: the
// decoder writes them that way (packetpath.DecodePathFromRawHex), and the join
// has to cope with that rather than with a lowercase convenience fiction.
func seedTransmissionPathAt(t *testing.T, s *PacketStore, hops []string, seed scopeSeed, routeType types.RouteType, firstSeen time.Time) {
	t.Helper()
	ctx := t.Context()
	scopeSeedCounter++

	txID, _, err := s.db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		Hash:        fmt.Sprintf("%016x", scopeSeedCounter),
		RawHex:      "AA",
		RouteType:   routeType,
		PayloadType: types.PayloadType(1),
		ScopeName:   seed.scopeName,
	}, firstSeen)
	if err != nil {
		t.Fatalf("seed transmission: %v", err)
	}

	lowered := make([]string, len(hops))
	resolved := make([]*string, len(hops))
	for i, h := range hops {
		lowered[i] = strings.ToLower(h)
	}

	_, inserted, err := s.db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: txID,
		ObserverIdx:    nil,
		SNR:            nil,
		RSSI:           nil,
		Timestamp:      time.Now().UTC(),
	}, lowered, resolved)
	if err != nil {
		t.Fatalf("seed observation: %v", err)
	}
	if !inserted {
		t.Fatalf("expected observation to be inserted for tx %d", txID)
	}
}

// seedTransmission seeds a FLOOD packet (route_type=1) — path[last] is the
// actual transmitter, so forwarder is attributable.

// TestScopeAuditForwardingAttributesMidPathHop is the fleet-wide half of the
// mid-path attribution fix. The audit runs a different query from
// ScopeConformance — one full-window scan instead of one EXISTS per pubkey — so
// the two share the rule but not the code, and both need pinning.
//
// This is the case behind the audit's 65% blind spot: a declared target that
// forwards steadily but is never the hop an observer hears directly had every
// region it declares reported as notObserved.
func TestScopeAuditForwardingAttributesMidPathHop(t *testing.T) {
	ctx := t.Context()
	s := newScopeTestStore(t)
	recent := time.Now().UTC()
	seedTransmissionPathAt(t, s, []string{testFullPubkeyA[:4], "AAAA", "BBBB"}, scopeMatched("#be"), types.RouteTransportFlood, recent)

	got, err := s.ScopeAuditForwarding(ctx, time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC), []string{testFullPubkeyA})
	if err != nil {
		t.Fatal(err)
	}
	agg := got[testFullPubkeyA]
	if agg == nil || agg.scopes["be"] == nil || agg.scopes["be"].Packets != 1 {
		t.Fatalf("want the mid-path hop attributed to its sole matching target, got %+v", got)
	}
	if agg.ambiguousHops != 0 {
		t.Errorf("ambiguousHops = %d, want 0 — one target matches this hop", agg.ambiguousHops)
	}
}

// TestScopeAuditForwardingIgnoresDirectRoutes pins the route-type filter on the
// audit's own query. With the last-hop restriction gone it is the only guard
// against crediting a DIRECT route's remaining path plan as forwarding — and a
// DIRECT packet's hops are frequently the declared targets this audit judges.

// TestScopeAuditForwardingIgnoresDirectRoutes pins the route-type filter on the
// audit's own query. With the last-hop restriction gone it is the only guard
// against crediting a DIRECT route's remaining path plan as forwarding — and a
// DIRECT packet's hops are frequently the declared targets this audit judges.
func TestScopeAuditForwardingIgnoresDirectRoutes(t *testing.T) {
	ctx := t.Context()
	s := newScopeTestStore(t)
	recent := time.Now().UTC().Add(-time.Minute)
	seedTransmissionPathAt(t, s, []string{"AAAA", testFullPubkeyA[:4], "BBBB"}, scopeMatched("#be"), types.RouteTransportDirect, recent)
	seedTransmissionPathAt(t, s, []string{"AAAA", "BBBB", testFullPubkeyA[:4]}, scopeUnscoped(), types.RouteDirect, recent)

	got, err := s.ScopeAuditForwarding(ctx, time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC), []string{testFullPubkeyA})
	if err != nil {
		t.Fatal(err)
	}
	if agg := got[testFullPubkeyA]; agg != nil && (len(agg.scopes) != 0 || agg.unscopedPackets != 0 || agg.ambiguousHops != 0) {
		t.Errorf("agg = %+v, want no attribution from DIRECT routes", agg)
	}
}

// TestScopeAuditForwardingCountsOneTransmissionOncePerTarget pins that the
// existing "<target>|<txID>" de-duplication also absorbs the same target
// matching several hops of ONE path — which could not happen while only
// path[last] was read, and now can (a routing loop, or two hops colliding on the
// same truncated prefix). Without it a looping packet would inflate a target's
// packet count and quietly make a quiet region look busy.

// TestScopeAuditForwardingCountsOneTransmissionOncePerTarget pins that the
// existing "<target>|<txID>" de-duplication also absorbs the same target
// matching several hops of ONE path — which could not happen while only
// path[last] was read, and now can (a routing loop, or two hops colliding on the
// same truncated prefix). Without it a looping packet would inflate a target's
// packet count and quietly make a quiet region look busy.
func TestScopeAuditForwardingCountsOneTransmissionOncePerTarget(t *testing.T) {
	ctx := t.Context()
	s := newScopeTestStore(t)
	recent := time.Now().UTC().Add(-time.Minute)
	seedTransmissionPathAt(t, s, []string{testFullPubkeyA[:4], "AAAA", testFullPubkeyA[:4]}, scopeMatched("#be"), types.RouteTransportFlood, recent)

	got, err := s.ScopeAuditForwarding(ctx, time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC), []string{testFullPubkeyA})
	if err != nil {
		t.Fatal(err)
	}
	agg := got[testFullPubkeyA]
	if agg == nil || agg.scopes["be"] == nil {
		t.Fatalf("want #be attributed, got %+v", got)
	}
	if agg.scopes["be"].Packets != 1 {
		t.Errorf("Packets = %d, want 1 — one transmission, matched on two of its hops", agg.scopes["be"].Packets)
	}
}

// TestScopeAuditForwardingAttributesLongerHopByItsOwnLength pins the
// length-indexed half of scopeAuditPrefixIndex, which every other test in this
// file leaves untested: they all seed 4-char hops, so a lookup that ignored hop
// length entirely would still pass them.
//
// pkOther shares the first 4 hex chars with testFullPubkeyA and diverges after
// that, so an 8-char hop has exactly one candidate while a 4-char hop would
// have two. Attribution must therefore key on the hop's OWN length: at 8 chars
// this is an unambiguous attribution, not an ambiguousHops row.

// TestScopeAuditForwardingAttributesLongerHopByItsOwnLength pins the
// length-indexed half of scopeAuditPrefixIndex, which every other test in this
// file leaves untested: they all seed 4-char hops, so a lookup that ignored hop
// length entirely would still pass them.
//
// pkOther shares the first 4 hex chars with testFullPubkeyA and diverges after
// that, so an 8-char hop has exactly one candidate while a 4-char hop would
// have two. Attribution must therefore key on the hop's OWN length: at 8 chars
// this is an unambiguous attribution, not an ambiguousHops row.
func TestScopeAuditForwardingAttributesLongerHopByItsOwnLength(t *testing.T) {
	ctx := t.Context()
	s := newScopeTestStore(t)
	pkOther := testFullPubkeyA[:4] + strings.Repeat("33", 30)
	hop := testFullPubkeyA[:8]
	recent := time.Now().UTC().Add(-time.Minute)
	seedTransmissionPathAt(t, s, []string{hop, "AAAA"}, scopeMatched("#be"), types.RouteTransportFlood, recent)

	got, err := s.ScopeAuditForwarding(ctx, time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC), []string{testFullPubkeyA, pkOther})
	if err != nil {
		t.Fatal(err)
	}
	agg := got[testFullPubkeyA]
	if agg == nil || agg.scopes["be"] == nil || agg.scopes["be"].Packets != 1 {
		t.Fatalf("want the 8-char hop attributed to its sole matching target, got %+v", got)
	}
	if agg.ambiguousHops != 0 {
		t.Errorf("ambiguousHops = %d, want 0 — the two targets diverge before hop length 8", agg.ambiguousHops)
	}
	if other := got[pkOther]; other != nil && (len(other.scopes) != 0 || other.ambiguousHops != 0) {
		t.Errorf("pkOther = %+v, want no attribution and no ambiguity — the hop is not its prefix", other)
	}
}

// TestScopeAuditForwardingCountsUnmatchedPackets: a transport-scoped packet
// whose code1 matched no configured region key is stored with scope_name = ""
// (scopeNameForDB's "transport-scoped but unnameable" state). It is not a
// named scope, so it must not enter agg.scopes, and it is not an unscoped
// plain flood either, so it must not enter unscopedPackets. It is its own
// fact: this instance saw the target forward traffic it holds no key for.
//
// Without this counter the audit reports the declared region as "not
// observed", which reads as a finding about the repeater when it is really a
// gap in this instance's own hashRegions.

// TestScopeAuditTTLForSevenDayWindow pins the per-window TTL. The 7d window
// costs a different order of magnitude than the others (16.7s against 4.0s and
// 0.15s, measured on the live-shaped staging database on 2026-09-07), so it is
// deliberately not on the 30s the other two share. A future edit that collapses
// this back to one constant should have to delete a test that says why.
func TestScopeAuditTTLForSevenDayWindow(t *testing.T) {
	if got := scopeAuditTTLFor("7d"); got != 5*time.Minute {
		t.Errorf("scopeAuditTTLFor(7d) = %s, want 5m", got)
	}
	for _, w := range []string{"1h", "24h", ""} {
		if got := scopeAuditTTLFor(w); got != 30*time.Second {
			t.Errorf("scopeAuditTTLFor(%q) = %s, want 30s", w, got)
		}
	}
}

// TestHandleScopeAuditServesSecondRequestFromCache pins the cache path itself,
// which the singleflight rewrite moved out of the handler and into
// scopeAuditCached/scopeAuditStore. A declared row inserted between two
// requests inside the TTL must NOT appear in the second response: if it does,
// the response was recomputed and the cache is not being consulted.

// TestHandleScopeAuditServesSecondRequestFromCache pins the cache path itself,
// which the singleflight rewrite moved out of the handler and into
// scopeAuditCached/scopeAuditStore. A declared row inserted between two
// requests inside the TTL must NOT appear in the second response: if it does,
// the response was recomputed and the cache is not being consulted.
func TestHandleScopeAuditServesSecondRequestFromCache(t *testing.T) {
	srv, router := setupScopeAuditServer(t)
	now := time.Now().UTC()
	insertDeclared(t, srv, testFullPubkeyA, now, "be", 0)

	first := getScopeAudit(t, router, "")
	if len(first.Repeaters) != 1 {
		t.Fatalf("first call repeaters = %d, want 1", len(first.Repeaters))
	}

	insertDeclared(t, srv, testFullPubkeyB, now, "be", 0)
	second := getScopeAudit(t, router, "")
	if len(second.Repeaters) != 1 {
		t.Errorf("second call repeaters = %d, want 1 — the row added after the first call proves the cache was bypassed", len(second.Repeaters))
	}
}

// TestHandleScopeAuditNormalisesHashPrefix pins trap 1: transmissions.scope_name
// keeps the '#' (hashRegions config), regions_csv arrives from the firmware
// with it already stripped. Declared "be-van" and observed "#be-van" must be
// recognised as the same scope, not reported as both missing and undeclared.

// TestScopeAuditForwardingCountsUnmatchedPackets: a transport-scoped packet
// whose code1 matched no configured region key is stored with scope_name = ""
// (scopeNameForDB's "transport-scoped but unnameable" state). It is not a
// named scope, so it must not enter agg.scopes, and it is not an unscoped
// plain flood either, so it must not enter unscopedPackets. It is its own
// fact: this instance saw the target forward traffic it holds no key for.
//
// Without this counter the audit reports the declared region as "not
// observed", which reads as a finding about the repeater when it is really a
// gap in this instance's own hashRegions.
func TestScopeAuditForwardingCountsUnmatchedPackets(t *testing.T) {
	ctx := t.Context()
	s := newScopeTestStore(t)
	hop := testFullPubkeyA[:4]
	recent := time.Now().UTC().Add(-time.Minute)
	seedTransmissionRouteAt(t, s, hop, scopeUnmatched(), types.RouteTransportFlood, recent)

	got, err := s.ScopeAuditForwarding(ctx, time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC), []string{testFullPubkeyA})
	if err != nil {
		t.Fatal(err)
	}
	agg := got[testFullPubkeyA]
	if agg == nil {
		t.Fatalf("want an agg for the target, got none (result = %+v)", got)
	}
	if agg.unmatchedPackets != 1 {
		t.Errorf("unmatchedPackets = %d, want 1", agg.unmatchedPackets)
	}
	if len(agg.scopes) != 0 {
		t.Errorf("scopes = %+v, want empty — an unmatched packet names no region", agg.scopes)
	}
	if agg.unscopedPackets != 0 {
		t.Errorf("unscopedPackets = %d, want 0 — unmatched is not the same as unscoped", agg.unscopedPackets)
	}
}

// TestScopeAuditForwardingCountsUnmatchedOnMidPathHop is the post-M0 case that
// carries almost all of this counter's real volume: before M0 only a last hop
// was attributed, so a repeater deep in a flood path contributed nothing at
// all. Now every hop counts, and the same de-duplication that protects the
// named-scope tally must protect this one — a target appearing twice in one
// path is still one packet, not two.
func TestScopeAuditForwardingCountsUnmatchedOnMidPathHop(t *testing.T) {
	ctx := t.Context()
	s := newScopeTestStore(t)
	hop := testFullPubkeyA[:4]
	recent := time.Now().UTC().Add(-time.Minute)
	seedTransmissionPathAt(t, s, []string{"AAAA", hop, "BBBB", hop}, scopeUnmatched(), types.RouteTransportFlood, recent)

	got, err := s.ScopeAuditForwarding(ctx, time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC), []string{testFullPubkeyA})
	if err != nil {
		t.Fatal(err)
	}
	agg := got[testFullPubkeyA]
	if agg == nil {
		t.Fatalf("want an agg for the mid-path target, got none (result = %+v)", got)
	}
	if agg.unmatchedPackets != 1 {
		t.Errorf("unmatchedPackets = %d, want 1 — one transmission, matched on two of its hops", agg.unmatchedPackets)
	}
}

// TestHandleScopeAuditSurfacesUnmatchedPackets: a repeater declares "behss",
// and this instance sees it forward transport-scoped traffic it cannot name.
// The row must still list "behss" as notObserved — an unmatched packet names
// no region, so it cannot satisfy the declaration — but it must also carry
// observedUnmatchedPackets, so a client can say the finding might be a missing
// region key rather than a silent repeater.
func TestHandleScopeAuditSurfacesUnmatchedPackets(t *testing.T) {
	srv, router := setupScopeAuditServer(t)
	pk := testFullPubkeyA
	insertDeclared(t, srv, pk, time.Now().UTC(), "behss", 0)
	recent := time.Now().UTC().Add(-time.Minute)
	seedTransmissionRouteAt(t, srv.store, pk[:4], scopeUnmatched(), types.RouteTransportFlood, recent)

	got := getScopeAudit(t, router, "")
	if len(got.Repeaters) != 1 {
		t.Fatalf("repeaters = %+v, want 1", got.Repeaters)
	}
	row := got.Repeaters[0]
	if row.ObservedUnmatchedPackets != 1 {
		t.Errorf("observedUnmatchedPackets = %d, want 1", row.ObservedUnmatchedPackets)
	}
	if len(row.NotObserved) != 1 || row.NotObserved[0] != "behss" {
		t.Errorf("notObserved = %v, want [\"behss\"] — an unmatched packet names no region and cannot satisfy a declaration", row.NotObserved)
	}
	if row.WildcardContradiction {
		t.Error("wildcardContradiction = true, want false — unmatched traffic is scoped, so it says nothing about '*'")
	}
}

// TestHandleScopeAuditVerifiesDeclaredRegion is the case this milestone exists
// for, built from the real packet that started the investigation. A repeater
// declares "fm-112"; this instance holds no key for it, so both packets it
// forwarded are stored unmatched. Verification derives the key from the
// repeater's own declaration, finds two corroborating packets, and the region
// must leave notObserved with its evidence count reported.
// transportFloodPacketFor builds a TRANSPORT_FLOOD packet whose code1 is the
// code `region` derives over this payload, so the verifier will match it. The
// shape mirrors realTransportFloodPacket: header 0x14 (route 0, payload type
// 5), the two transport codes, path byte 0x41 (hash size 2, one hop), the hop,
// then the payload.
//
// It exists because corroboration has to come from DIFFERENT payloads. Two
// copies of one packet derive the same code by construction, so they are one
// observation counted twice, not the two independent matches the threshold
// argument rests on.
func transportFloodPacketFor(region string, payload []byte) string {
	code1 := regionCode(region, 5, payload)
	return "14" + code1 + "0000" + "41" + "E3D3" + strings.ToUpper(hex.EncodeToString(payload))
}

func TestHandleScopeAuditVerifiesDeclaredRegion(t *testing.T) {
	srv, router := setupScopeAuditServer(t)
	pk := testFullPubkeyA
	insertDeclared(t, srv, pk, time.Now().UTC(), "fm-112,behss", 0)
	recent := time.Now().UTC().Add(-time.Minute)
	// Two DIFFERENT payloads, each deriving to #fm-112 on its own. The first is
	// the real packet captured from a live instance; the second is built for
	// this test. Seeding the same packet twice would prove only that the
	// verifier counts rows.
	seedUnmatchedRawAt(t, srv.store, pk[:4], realTransportFloodPacket, types.RouteTransportFlood, recent)
	seedUnmatchedRawAt(t, srv.store, pk[:4], transportFloodPacketFor("fm-112", []byte{0x51, 0x52, 0x53, 0x54, 0x55}), types.RouteTransportFlood, recent)

	got := getScopeAudit(t, router, "")
	if len(got.Repeaters) != 1 {
		t.Fatalf("repeaters = %+v, want 1", got.Repeaters)
	}
	row := got.Repeaters[0]
	if row.RegionEvidence["fm-112"] != 2 {
		t.Errorf("regionEvidence = %v, want fm-112:2", row.RegionEvidence)
	}
	for _, n := range row.NotObserved {
		if n == "fm-112" {
			t.Errorf("notObserved = %v, must not contain fm-112 - two corroborating packets prove it is forwarded", row.NotObserved)
		}
	}
	if len(row.NotObserved) != 1 || row.NotObserved[0] != "behss" {
		t.Errorf("notObserved = %v, want [behss] - that region has no corroborating traffic here", row.NotObserved)
	}
}

// TestHandleScopeAuditReportsTheVerificationSampleSize pins the field a client
// needs to subtract RegionEvidence from ObservedUnmatchedPackets honestly.
// RegionEvidence can only ever count packets inside the sample, so a client
// that subtracts it from an uncapped total overstates what is unexplained. The
// two are equal here, which is the case that says "this subtraction is exact".

// TestHandleScopeAuditDoesNotVerifyOnOnePacket: a single match is 1-in-65536
// and must leave the region in notObserved, with its count still reported so a
// client can say "one hit, not enough".
func TestHandleScopeAuditDoesNotVerifyOnOnePacket(t *testing.T) {
	srv, router := setupScopeAuditServer(t)
	pk := testFullPubkeyA
	insertDeclared(t, srv, pk, time.Now().UTC(), "fm-112", 0)
	recent := time.Now().UTC().Add(-time.Minute)
	seedUnmatchedRawAt(t, srv.store, pk[:4], realTransportFloodPacket, types.RouteTransportFlood, recent)

	got := getScopeAudit(t, router, "")
	row := got.Repeaters[0]
	if row.RegionEvidence["fm-112"] != 1 {
		t.Errorf("regionEvidence = %v, want fm-112:1", row.RegionEvidence)
	}
	if len(row.NotObserved) != 1 || row.NotObserved[0] != "fm-112" {
		t.Errorf("notObserved = %v, want [fm-112] - one corroborating packet is not evidence", row.NotObserved)
	}
}

// TestHandleScopeAuditLeavesCleanRowsAlone: a repeater whose declared regions
// are all observed by name, with no unmatched traffic at all, must be untouched
// by verification - no evidence, no change to notObserved, and an empty (not
// null) regionEvidence so a client can iterate it without a guard.

// TestHandleScopeAuditReportsTheVerificationSampleSize pins the field a client
// needs to subtract RegionEvidence from ObservedUnmatchedPackets honestly.
// RegionEvidence can only ever count packets inside the sample, so a client
// that subtracts it from an uncapped total overstates what is unexplained. The
// two are equal here, which is the case that says "this subtraction is exact".
func TestHandleScopeAuditReportsTheVerificationSampleSize(t *testing.T) {
	srv, router := setupScopeAuditServer(t)
	pk := testFullPubkeyA
	insertDeclared(t, srv, pk, time.Now().UTC(), "fm-112", 0)
	recent := time.Now().UTC().Add(-time.Minute)
	seedUnmatchedRawAt(t, srv.store, pk[:4], realTransportFloodPacket, types.RouteTransportFlood, recent)
	seedUnmatchedRawAt(t, srv.store, pk[:4], realTransportFloodPacket, types.RouteTransportFlood, recent)

	row := getScopeAudit(t, router, "").Repeaters[0]
	if row.ObservedUnmatchedPackets != 2 {
		t.Fatalf("observedUnmatchedPackets = %d, want 2", row.ObservedUnmatchedPackets)
	}
	if row.ObservedUnmatchedSampled != row.ObservedUnmatchedPackets {
		t.Errorf("observedUnmatchedSampled = %d, want %d — nothing was capped away at this size, and a client can only tell from this field",
			row.ObservedUnmatchedSampled, row.ObservedUnmatchedPackets)
	}
}

// TestHandleScopeAuditDoesNotVerifyOnOnePacket: a single match is 1-in-65536
// and must leave the region in notObserved, with its count still reported so a
// client can say "one hit, not enough".

// seedUnmatchedRawAt seeds one unmatched transmission carrying a real raw_hex,
// attributed to forwarder. Distinct from seedTransmissionRouteAt, which seeds
// raw_hex 'AA' - fine for tests that never parse it, useless here.
func seedUnmatchedRawAt(t *testing.T, s *PacketStore, forwarder, rawHex string, routeType types.RouteType, firstSeen time.Time) {
	t.Helper()
	ctx := t.Context()
	scopeSeedCounter++

	unmatched := ""
	txID, _, err := s.db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		Hash:        fmt.Sprintf("%016x", scopeSeedCounter),
		RawHex:      rawHex,
		RouteType:   routeType,
		PayloadType: types.PayloadType(5),
		DecodedJSON: "{}",
		ScopeName:   &unmatched,
	}, firstSeen)
	if err != nil {
		t.Fatalf("seed unmatched transmission: %v", err)
	}

	_, inserted, err := s.db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: txID,
		ObserverIdx:    nil,
		SNR:            nil,
		RSSI:           nil,
		Timestamp:      firstSeen,
	}, []string{strings.ToLower(forwarder)}, []*string{nil})
	if err != nil {
		t.Fatalf("seed unmatched observation: %v", err)
	}
	if !inserted {
		t.Fatalf("expected observation to be inserted for tx %d", txID)
	}
}

// TestHandleNodeScopesDifferentWindowIsSeparateCacheEntry confirms the cache
// key includes window: a request for a different window must recompute
// rather than reuse another window's cached entry.

// TestHandleScopeAuditNamedRegionNeedsNoVerification pins the interaction
// between naming a packet at ingest and verifying it at read time, which are
// built separately and had no test together.
//
// Deriving region keys from what nodes declare means a packet that used to be
// stored unnameable now arrives with a name. The audit must then report that
// region as observed by the ordinary route: present in agg.scopes, absent from
// notObserved, and NOT claimed by regionEvidence, which exists to explain
// regions that could only be established by verification.
//
// Getting this wrong is not loud. A region would still be green, so the page
// looks right while the reason underneath it is wrong, and a reader chasing
// "how do we know this" is told the wrong story.
func TestHandleScopeAuditNamedRegionNeedsNoVerification(t *testing.T) {
	srv, router := setupScopeAuditServer(t)
	pk := testFullPubkeyA
	insertDeclared(t, srv, pk, time.Now().UTC(), "fm-112", 0)
	recent := time.Now().UTC().Add(-time.Minute)
	// scopeMatched is the state the ingestor writes once it holds a key for
	// the region, whether that key was configured by hand or derived.
	seedTransmissionRouteAt(t, srv.store, pk[:4], scopeMatched("#fm-112"), types.RouteTransportFlood, recent)

	row := getScopeAudit(t, router, "").Repeaters[0]
	for _, rgn := range row.NotObserved {
		if rgn == "fm-112" {
			t.Errorf("notObserved = %v, must not contain fm-112 — it was observed under its own name", row.NotObserved)
		}
	}
	if n, ok := row.RegionEvidence["fm-112"]; ok {
		t.Errorf("regionEvidence[fm-112] = %d, want absent — verification explains regions that could not be named, and this one was", n)
	}
	if row.ObservedUnmatchedPackets != 0 {
		t.Errorf("observedUnmatchedPackets = %d, want 0 — a named packet is not unnameable traffic", row.ObservedUnmatchedPackets)
	}
}
