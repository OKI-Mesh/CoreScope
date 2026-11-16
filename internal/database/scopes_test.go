package database_test

import (
	"cmp"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

var scopeBase = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

func scopeTx(t *testing.T, db *database.DB, hash string, route types.RouteType, scope *string, at time.Time, advert *database.NodeInput) int64 {
	t.Helper()
	in := database.TransmissionInput{
		RawHex: "AABB", Hash: hash, RouteType: route, PayloadType: types.PayloadADVERT,
		DecodedJSON: `{}`, ScopeName: scope, Advert: advert,
	}
	if route.IsTransport() {
		in.Code1, in.Code2 = "AAAA", "BBBB"
	}
	id, _, err := db.CreateOrUpdateTransmission(t.Context(), in, at)
	if err != nil {
		t.Fatalf("seed transmission %s: %v", hash, err)
	}
	return id
}

func declare(t *testing.T, db *database.DB, target, rx, csv string, truncated bool, at time.Time) {
	t.Helper()
	inserted, err := db.InsertClientDeclaredRegions(t.Context(), &types.ClientDeclaredRegions{
		Target: target, RxPubkey: rx, ObservedAt: at, IngestedAt: at,
		RegionsCSV: csv, Truncated: truncated,
	})
	if err != nil {
		t.Fatalf("seed declared regions for %s: %v", target, err)
	}
	if !inserted {
		t.Fatalf("seed declared regions for %s: row was not inserted", target)
	}
}

func describeDeclared(rs []database.DeclaredRegions) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = fmt.Sprintf("%s|%s|%v|%s", r.Target, r.RegionsCSV, r.Truncated, r.ObservedAt.Format(time.RFC3339))
	}
	return out
}

func TestScopeMatchTotals_RoundTrip_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()

	empty, err := db.GetScopeMatchTotals(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if empty != nil {
		t.Fatalf("want nil before anything is saved, got %+v", empty)
	}

	first := database.ScopeMatchTotals{Since: scopeBase, Unique: 5, ExplicitOverDerived: 2, Ambiguous: 1, None: 7}
	if err := db.SaveScopeMatchTotals(ctx, first, scopeBase.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	got, err := db.GetScopeMatchTotals(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || *got != first || got.Since.Location() != time.UTC {
		t.Errorf("want %+v with Since in UTC, got %+v", first, got)
	}

	second := database.ScopeMatchTotals{Since: scopeBase.Add(-time.Hour), Unique: 9, ExplicitOverDerived: 3, Ambiguous: 4, None: 8}
	if err := db.SaveScopeMatchTotals(ctx, second, scopeBase.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	got, err = db.GetScopeMatchTotals(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || *got != second {
		t.Errorf("a second save replaces the single row; want %+v, got %+v", second, got)
	}
}

func TestGetScopeStats_RejectsBucketsThatAreNotWholeSeconds_DB(t *testing.T) {
	db := newDB(t)
	for _, bucket := range []time.Duration{0, -time.Second, 500 * time.Millisecond, 1500 * time.Millisecond} {
		if _, err := db.GetScopeStats(t.Context(), scopeBase, bucket); err == nil {
			t.Errorf("bucket %v: want an error, got none", bucket)
		}
	}
	if _, err := db.GetScopeStats(t.Context(), scopeBase, time.Second); err != nil {
		t.Errorf("a one-second bucket is valid: %v", err)
	}
}

func TestGetScopeStats_EmptyDatabase_DB(t *testing.T) {
	db := newDB(t)
	got, err := db.GetScopeStats(t.Context(), scopeBase, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if got.Summary != (database.ScopeSummary{}) {
		t.Errorf("want a zero summary, got %+v", got.Summary)
	}
	if got.ByRegion == nil || len(got.ByRegion) != 0 {
		t.Errorf("want an empty non-nil ByRegion, got %#v", got.ByRegion)
	}
	if got.TimeSeries == nil || len(got.TimeSeries) != 0 {
		t.Errorf("want an empty non-nil TimeSeries, got %#v", got.TimeSeries)
	}
	if got.AdvertsByRole == nil || len(got.AdvertsByRole) != 0 {
		t.Errorf("want an empty non-nil AdvertsByRole, got %#v", got.AdvertsByRole)
	}
}

func TestGetScopeStats_CountsRoutesScopesAndBuckets_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	b := scopeBase

	scopeTx(t, db, "st_1", types.RouteTransportFlood, ptr("#cvg"), b, nil) // exactly at since
	scopeTx(t, db, "st_2", types.RouteTransportFlood, ptr("#cvg"), b.Add(2*time.Minute), nil)
	scopeTx(t, db, "st_3", types.RouteTransportFlood, ptr("#dayton"), b.Add(2*time.Hour), nil)
	scopeTx(t, db, "st_4", types.RouteTransportFlood, nil, b.Add(2*time.Minute), nil)
	scopeTx(t, db, "st_5", types.RouteTransportDirect, ptr(""), b.Add(3*time.Hour), nil)   // scoped but unmatched
	scopeTx(t, db, "st_6", types.RouteFlood, nil, b.Add(4*time.Hour), nil)                 // non-transport
	scopeTx(t, db, "st_7", types.RouteDirect, nil, b.Add(5*time.Hour), nil)                // non-transport
	scopeTx(t, db, "st_8", types.RouteTransportFlood, ptr("#old"), b.Add(-time.Hour), nil) // before since

	got, err := db.GetScopeStats(ctx, b, time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	if want := (database.ScopeSummary{TransportTotal: 5, Scoped: 4, Unscoped: 3, UnknownScope: 1}); got.Summary != want {
		t.Errorf("summary: want %+v (non-transport routes folded into Unscoped, unknown scope counted in Scoped), got %+v", want, got.Summary)
	}

	if want := []database.ScopeRegionCount{{Name: "#cvg", Count: 2}, {Name: "#dayton", Count: 1}}; !slices.Equal(got.ByRegion, want) {
		t.Errorf("by region: want %v, got %v", want, got.ByRegion)
	}

	hourly := []struct {
		at               time.Time
		scoped, unscoped int64
	}{
		{b, 2, 1},
		{b.Add(2 * time.Hour), 1, 0},
		{b.Add(3 * time.Hour), 1, 0},
	}
	if len(got.TimeSeries) != len(hourly) {
		t.Fatalf("hourly series: want %d points, got %+v", len(hourly), got.TimeSeries)
	}
	for i, w := range hourly {
		p := got.TimeSeries[i]
		if !p.T.Equal(w.at) || p.T.Location() != time.UTC || p.Scoped != w.scoped || p.Unscoped != w.unscoped {
			t.Errorf("hourly point %d: want %v scoped=%d unscoped=%d, got %+v", i, w.at, w.scoped, w.unscoped, p)
		}
	}

	daily, err := db.GetScopeStats(ctx, b, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	dayStart := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	if len(daily.TimeSeries) != 1 || !daily.TimeSeries[0].T.Equal(dayStart) || daily.TimeSeries[0].Scoped != 4 || daily.TimeSeries[0].Unscoped != 1 {
		t.Errorf("daily series: want one UTC-aligned point %v scoped=4 unscoped=1, got %+v", dayStart, daily.TimeSeries)
	}
}

func TestGetScopeStats_AdvertsByRoleSplitByScopeState_DB(t *testing.T) {
	db := newDB(t)
	b := scopeBase
	repeater := &database.NodeInput{PublicKey: "r1", Name: "Rep", Role: "repeater", RxTime: b, IngestNow: b}
	companion := &database.NodeInput{PublicKey: "c1", Name: "Comp", Role: "companion", RxTime: b, IngestNow: b}

	scopeTx(t, db, "adv_1", types.RouteFlood, nil, b.Add(time.Minute), repeater)                // repeater, unscoped
	scopeTx(t, db, "adv_2", types.RouteTransportFlood, ptr(""), b.Add(2*time.Minute), repeater) // repeater, unknown scope
	scopeTx(t, db, "adv_3", types.RouteTransportFlood, ptr("#cvg"), b.Add(3*time.Minute), repeater)
	scopeTx(t, db, "adv_4", types.RouteFlood, nil, b.Add(4*time.Minute), companion)
	scopeTx(t, db, "adv_5", types.RouteDirect, nil, b.Add(5*time.Minute), repeater) // not flooded
	scopeTx(t, db, "adv_6", types.RouteFlood, nil, b.Add(6*time.Minute), nil)       // sender unknown
	scopeTx(t, db, "adv_7", types.RouteFlood, nil, b.Add(-time.Hour), companion)    // before since

	got, err := db.GetScopeStats(t.Context(), b, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	want := []database.ScopeAdvertRoleCount{
		{Role: "repeater", Unscoped: 1, UnknownScope: 1, Named: 1},
		{Role: "companion", Unscoped: 1},
		{Role: "unknown", Unscoped: 1},
	}
	if !slices.Equal(got.AdvertsByRole, want) {
		t.Errorf("want flood adverts by role (most first, ties by role) %+v, got %+v", want, got.AdvertsByRole)
	}
}

func TestListScopeAuditHops_DistinctForwarderHopsInWindow_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	b := scopeBase
	obsA := dbtest.Observer(t, db, "obsa", "Obs A", "CVG", b, b)
	obsB := dbtest.Observer(t, db, "obsb", "Obs B", "DAY", b, b)

	first := scopeTx(t, db, "audit_a", types.RouteTransportFlood, nil, b.Add(time.Minute), nil)
	second := scopeTx(t, db, "audit_b", types.RouteTransportFlood, ptr("#cvg"), b.Add(2*time.Minute), nil)
	direct := scopeTx(t, db, "audit_c", types.RouteDirect, nil, b.Add(3*time.Minute), nil)
	before := scopeTx(t, db, "audit_d", types.RouteTransportFlood, nil, b.Add(-time.Hour), nil)

	dbtest.Observation(t, db, first, obsA, b.Add(time.Minute), 7.5, -90, "aa", "cc")
	dbtest.Observation(t, db, first, obsB, b.Add(time.Minute), 6.0, -95, "bbbb")
	dbtest.Observation(t, db, second, obsA, b.Add(2*time.Minute), 7.5, -90, "bbbb", "dddd")
	dbtest.Observation(t, db, direct, obsA, b.Add(3*time.Minute), 7.5, -90, "bbbb")
	dbtest.Observation(t, db, before, obsA, b.Add(-time.Hour), 7.5, -90, "bbbb")

	describe := func(hops []database.ScopeAuditHop) []string {
		out := make([]string, len(hops))
		for i, h := range hops {
			scope := "unscoped"
			if h.ScopeName != nil {
				scope = "scope=" + *h.ScopeName
			}
			out[i] = fmt.Sprintf("%d %s %s", h.TxID, h.Hop, scope)
		}
		return out
	}
	query := database.ScopeAuditQuery{
		Since: b, Prefixes: []string{"BBBB", "aa"}, MinHopLen: 2,
		RouteTypes: []int{int(types.RouteTransportFlood), int(types.RouteFlood)},
	}

	got, err := db.ListScopeAuditHops(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		fmt.Sprintf("%d aa unscoped", first),
		fmt.Sprintf("%d bbbb unscoped", first),
		fmt.Sprintf("%d bbbb scope=#cvg", second),
	}
	if !slices.Equal(describe(got), want) {
		t.Errorf("want one row per distinct (transmission, hop), upper-case prefixes matched, direct and older transmissions excluded:\n  want %v\n  got  %v", want, describe(got))
	}
	if len(got) > 0 && (!got[0].FirstSeen.Equal(b.Add(time.Minute)) || got[0].FirstSeen.Location() != time.UTC) {
		t.Errorf("want FirstSeen %v in UTC, got %v", b.Add(time.Minute), got[0].FirstSeen)
	}

	query.MinHopLen = 4
	got, err = db.ListScopeAuditHops(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	want = []string{
		fmt.Sprintf("%d bbbb unscoped", first),
		fmt.Sprintf("%d bbbb scope=#cvg", second),
	}
	if !slices.Equal(describe(got), want) {
		t.Errorf("a minimum hop length of 4 drops the two-character hop:\n  want %v\n  got  %v", want, describe(got))
	}
}

func TestListScopeAuditHops_EmptyPrefixesReturnNothing_DB(t *testing.T) {
	db := newDB(t)
	for _, prefixes := range [][]string{nil, {}, {"", "  "}} {
		got, err := db.ListScopeAuditHops(t.Context(), database.ScopeAuditQuery{
			Since: scopeBase, Prefixes: prefixes, MinHopLen: 2, RouteTypes: []int{int(types.RouteFlood)},
		})
		if err != nil {
			t.Fatalf("prefixes %q: %v", prefixes, err)
		}
		if got == nil || len(got) != 0 {
			t.Errorf("prefixes %q: want an empty non-nil slice, got %#v", prefixes, got)
		}
	}
}

func TestListConfiguredScopes_OnlyNodesWithAScopeAndTheirTarget_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	seedNodes(t, db,
		nodeAt("aaaa", "A", "repeater", nodesNow),
		nodeAt("bbbb", "B", "repeater", nodesNow),
		nodeAt("cccc", "C", "repeater", nodesNow),
	)
	reportedA := nodesNow.Add(time.Minute)
	reportedB := nodesNow.Add(2 * time.Minute)
	if err := db.UpdateNodeConfiguredScope(ctx, "aaaa", "#cvg,#dayton", &reportedA); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateNodeConfiguredScope(ctx, "bbbb", "#lex", &reportedB); err != nil {
		t.Fatal(err)
	}

	got, err := db.ListConfiguredScopes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(got, func(a, b database.DeclaredRegions) int { return cmp.Compare(a.Target, b.Target) })
	if len(got) != 2 {
		t.Fatalf("want the two nodes that have a configured scope (not cccc), got %+v", got)
	}
	if got[0].Target != "aaaa" || got[0].RegionsCSV != "#cvg,#dayton" || !got[0].ObservedAt.Equal(reportedA) || got[0].ObservedAt.Location() != time.UTC || got[0].Truncated {
		t.Errorf("aaaa: want its scope, report time in UTC and Truncated false, got %+v", got[0])
	}
	if got[1].Target != "bbbb" || got[1].RegionsCSV != "#lex" || !got[1].ObservedAt.Equal(reportedB) {
		t.Errorf("bbbb: got %+v", got[1])
	}
}

func TestAllCurrentDeclaredRegions_NewestAnswerWinsPerTarget_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	seedNodes(t, db,
		nodeAt("aaaa", "A", "repeater", nodesNow),
		nodeAt("bbbb", "B", "repeater", nodesNow),
	)
	t1 := nodesNow.Add(time.Minute)
	t3 := nodesNow.Add(3 * time.Minute)

	if err := db.UpdateNodeConfiguredScope(ctx, "aaaa", "#cvg", &t1); err != nil {
		t.Fatal(err)
	}
	declare(t, db, "aaaa", "rx1", "cvg,dayton", true, t3)

	if err := db.UpdateNodeConfiguredScope(ctx, "bbbb", "#lex", &t3); err != nil {
		t.Fatal(err)
	}
	declare(t, db, "bbbb", "rx1", "old", false, t1)

	got, err := db.AllCurrentDeclaredRegions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"aaaa|cvg,dayton|true|" + t3.UTC().Format(time.RFC3339),
		"bbbb|#lex|false|" + t3.UTC().Format(time.RFC3339),
	}
	if !slices.Equal(describeDeclared(got), want) {
		t.Errorf("want the newer source per target (declared for aaaa with Truncated kept, configured for bbbb):\n  want %v\n  got  %v", want, describeDeclared(got))
	}
}

func TestAllCurrentDeclaredRegions_TieKeepsConfiguredScope_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	seedNodes(t, db, nodeAt("aaaa", "A", "repeater", nodesNow))
	at := nodesNow.Add(time.Minute)

	if err := db.UpdateNodeConfiguredScope(ctx, "aaaa", "#cvg", &at); err != nil {
		t.Fatal(err)
	}
	declare(t, db, "aaaa", "rx1", "other", true, at)

	got, err := db.AllCurrentDeclaredRegions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"aaaa|#cvg|false|" + at.UTC().Format(time.RFC3339)}
	if !slices.Equal(describeDeclared(got), want) {
		t.Errorf("on equal timestamps the configured scope wins:\n  want %v\n  got  %v", want, describeDeclared(got))
	}
}

func TestAllCurrentDeclaredRegions_TargetsMergeCaseInsensitively_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	seedNodes(t, db, nodeAt("aaaa", "A", "repeater", nodesNow))
	older := nodesNow.Add(time.Minute)
	newer := nodesNow.Add(2 * time.Minute)

	if err := db.UpdateNodeConfiguredScope(ctx, "aaaa", "#cvg", &older); err != nil {
		t.Fatal(err)
	}
	declare(t, db, "AAAA", "rx1", "dayton", false, newer)

	got, err := db.AllCurrentDeclaredRegions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"aaaa|dayton|false|" + newer.UTC().Format(time.RFC3339)}
	if !slices.Equal(describeDeclared(got), want) {
		t.Errorf("a collector's upper-case target is the same node as the lower-case one; want one entry, newest wins:\n  want %v\n  got  %v", want, describeDeclared(got))
	}
}

func TestAllCurrentDeclaredRegions_UnionSortedByTargetAndNeverNil_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()

	empty, err := db.AllCurrentDeclaredRegions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if empty == nil || len(empty) != 0 {
		t.Fatalf("want an empty non-nil slice with no data, got %#v", empty)
	}

	seedNodes(t, db,
		nodeAt("cccc", "C", "repeater", nodesNow),
		nodeAt("aaaa", "A", "repeater", nodesNow),
	)
	at := nodesNow.Add(time.Minute)
	if err := db.UpdateNodeConfiguredScope(ctx, "cccc", "#cvg", &at); err != nil {
		t.Fatal(err)
	}
	declare(t, db, "bbbb", "rx1", "lex", false, at)
	declare(t, db, "aaaa", "rx1", "dayton", false, at)

	got, err := db.AllCurrentDeclaredRegions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"aaaa|dayton|false|" + at.UTC().Format(time.RFC3339),
		"bbbb|lex|false|" + at.UTC().Format(time.RFC3339),
		"cccc|#cvg|false|" + at.UTC().Format(time.RFC3339),
	}
	if !slices.Equal(describeDeclared(got), want) {
		t.Errorf("want the union of both sources sorted by target:\n  want %v\n  got  %v", want, describeDeclared(got))
	}
}

func TestAllCurrentDeclaredRegions_RegionNamesStayVerbatim_DB(t *testing.T) {
	db := newDB(t)
	at := nodesNow.Add(time.Minute)
	declare(t, db, "aaaa", "rx1", "CVG,#cvg,cvg", false, at)

	got, err := db.AllCurrentDeclaredRegions(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"aaaa|CVG,#cvg,cvg|false|" + at.UTC().Format(time.RFC3339)}
	if !slices.Equal(describeDeclared(got), want) {
		t.Errorf("the database layer returns the region list exactly as declared, with no folding of case or the leading #:\n  want %v\n  got  %v", want, describeDeclared(got))
	}
}
