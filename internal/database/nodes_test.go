package database_test

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

func ptr[T any](v T) *T { return &v }

var nodesNow = time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)

func TestGetNodeLocationsByKeys(t *testing.T) {
	ctx := t.Context()
	db := dbtest.New(t)

	const pk = "aabbccdd11223344"
	dbtest.NodeWith(t, db, database.NodeInput{
		PublicKey: pk,
		Name:      "Test Repeater",
		Role:      "repeater",
		Lat:       ptr(37.77),
		Lon:       ptr(-122.42),
	})
	dbtest.Node(t, db, "eeff00112233aabb", "No Location", "repeater")

	t.Run("known key, mixed-case input", func(t *testing.T) {
		locs, err := db.GetNodeLocationsByKeys(ctx, []string{strings.ToUpper(pk)})
		if err != nil {
			t.Fatalf("GetNodeLocationsByKeys: %v", err)
		}
		if len(locs) != 1 {
			t.Fatalf("expected 1 location, got %d: %v", len(locs), locs)
		}
		entry, ok := locs[pk]
		if !ok {
			t.Fatalf("expected node location for test repeater, got keys %v", locs)
		}
		if entry.Lat == nil || *entry.Lat != 37.77 {
			t.Errorf("lat = %v, want 37.77", entry.Lat)
		}
		if entry.Lon == nil || *entry.Lon != -122.42 {
			t.Errorf("lon = %v, want -122.42", entry.Lon)
		}
		if entry.Role == nil || *entry.Role != "repeater" {
			t.Errorf("role = %v, want repeater", entry.Role)
		}
	})

	t.Run("node without coordinates is present with nil lat/lon", func(t *testing.T) {
		locs, err := db.GetNodeLocationsByKeys(ctx, []string{"eeff00112233aabb"})
		if err != nil {
			t.Fatalf("GetNodeLocationsByKeys: %v", err)
		}
		entry, ok := locs["eeff00112233aabb"]
		if !ok {
			t.Fatal("node without coordinates should still be returned")
		}
		if entry.Lat != nil || entry.Lon != nil {
			t.Errorf("lat/lon = %v/%v, want nil/nil", entry.Lat, entry.Lon)
		}
	})

	t.Run("no keys returns empty map", func(t *testing.T) {
		empty, err := db.GetNodeLocationsByKeys(ctx, []string{})
		if err != nil {
			t.Fatalf("GetNodeLocationsByKeys: %v", err)
		}
		if len(empty) != 0 {
			t.Errorf("expected 0 locations for empty keys, got %d", len(empty))
		}
	})

	t.Run("unknown key returns empty map", func(t *testing.T) {
		unknown, err := db.GetNodeLocationsByKeys(ctx, []string{"nonexistent"})
		if err != nil {
			t.Fatalf("GetNodeLocationsByKeys: %v", err)
		}
		if len(unknown) != 0 {
			t.Errorf("expected 0 locations for unknown key, got %d", len(unknown))
		}
	})
}

func nodeAt(pubkey, name string, role types.NodeRole, at time.Time) database.NodeInput {
	return database.NodeInput{PublicKey: pubkey, Name: name, Role: role, RxTime: at, IngestNow: at}
}

func seedNodes(tb testing.TB, db *database.DB, nodes ...database.NodeInput) {
	tb.Helper()
	for _, n := range nodes {
		if err := db.CreateOrUpdateNode(tb.Context(), n); err != nil {
			tb.Fatal(err)
		}
	}
}

func nodeKeys(nodes []database.Node) []string {
	keys := make([]string, len(nodes))
	for i, n := range nodes {
		keys[i] = n.PublicKey
	}
	return keys
}

func mustGetNode(tb testing.TB, db *database.DB, pubkey string) *database.Node {
	tb.Helper()
	n, err := db.GetNodeByPubkey(tb.Context(), pubkey)
	if err != nil {
		tb.Fatal(err)
	}
	return n
}

func TestCreateOrUpdateNode_NormalizesKeyAndStoresOptionalFields_DB(t *testing.T) {
	db := newDB(t)

	seedNodes(t, db,
		nodeAt("  AA01BB  ", "Named", "repeater", nodesNow),
		nodeAt("aa02bb", "", "", nodesNow),
	)

	named := mustGetNode(t, db, "aa01bb")
	if named.Name == nil || *named.Name != "Named" || named.Role == nil || *named.Role != "repeater" {
		t.Errorf("want name Named and role repeater under the lowercase key, got %+v", named)
	}
	bare := mustGetNode(t, db, "aa02bb")
	if bare.Name != nil || bare.Role != nil {
		t.Errorf("an empty name and role must be stored as NULL, got %+v", bare)
	}

	for _, key := range []string{"", "   "} {
		if err := db.CreateOrUpdateNode(t.Context(), nodeAt(key, "x", "repeater", nodesNow)); err == nil {
			t.Errorf("key %q: want an error", key)
		}
	}
}

func TestCreateOrUpdateNode_LastSeenCapAndMonotonicity_DB(t *testing.T) {
	db := newDB(t)

	t.Run("insert caps a receive time ahead of ingest time", func(t *testing.T) {
		seedNodes(t, db, database.NodeInput{
			PublicKey: "aa01", Name: "N", Role: "repeater", RxTime: nodesNow.Add(10 * time.Hour), IngestNow: nodesNow,
		})
		if got := mustGetNode(t, db, "aa01"); !got.LastSeen.Equal(nodesNow) {
			t.Errorf("want last_seen capped at %v, got %v", nodesNow, got.LastSeen)
		}
	})
	t.Run("update caps a receive time ahead of ingest time", func(t *testing.T) {
		seedNodes(t, db, database.NodeInput{
			PublicKey: "aa02", Name: "N", Role: "repeater", RxTime: nodesNow.Add(-2 * time.Hour), IngestNow: nodesNow.Add(-2 * time.Hour),
		})
		seedNodes(t, db, database.NodeInput{
			PublicKey: "aa02", Name: "N", Role: "repeater", RxTime: nodesNow.Add(10 * time.Hour), IngestNow: nodesNow,
		})
		if got := mustGetNode(t, db, "aa02"); !got.LastSeen.Equal(nodesNow) {
			t.Errorf("want last_seen capped at %v, got %v", nodesNow, got.LastSeen)
		}
	})

	t.Run("an older receive time never moves last_seen back", func(t *testing.T) {
		seedNodes(t, db, nodeAt("aa03", "N", "repeater", nodesNow))
		seedNodes(t, db, database.NodeInput{
			PublicKey: "aa03", Name: "N", Role: "repeater", RxTime: nodesNow.Add(-time.Hour), IngestNow: nodesNow,
		})
		got := mustGetNode(t, db, "aa03")
		if !got.LastSeen.Equal(nodesNow) || got.LastSeen.Location() != time.UTC {
			t.Errorf("want last_seen to stay %v in UTC, got %v", nodesNow, got.LastSeen)
		}
	})
}

func TestNodeWrites_AcceptMixedCaseKeys_DB(t *testing.T) {
	db := newDB(t)
	created := nodesNow.Add(-2 * time.Hour)
	seedNodes(t, db, nodeAt("aa02", "Target", "repeater", created))
	before := mustGetNode(t, db, "aa02")

	if err := db.IncrementNodeAdvertCount(t.Context(), "AA02"); err != nil {
		t.Fatal(err)
	}
	touched, err := db.TouchNodeLastSeen(t.Context(), "AA02", nodesNow)
	if err != nil {
		t.Fatal(err)
	}
	if touched != 1 {
		t.Errorf("TouchNodeLastSeen with an uppercase key: want 1 row, got %d", touched)
	}
	if err := db.UpdateBatteryVoltageAndTempC(t.Context(), "AA02", ptr(3700), ptr(21.5)); err != nil {
		t.Fatal(err)
	}
	if err := db.SetForeignAdvert(t.Context(), "AA02"); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateNodeDefaultScope(t.Context(), "AA02", "#oki"); err != nil {
		t.Fatal(err)
	}
	reported := nodesNow.Add(-time.Minute)
	if err := db.UpdateNodeConfiguredScope(t.Context(), "AA02", "#cin", &reported); err != nil {
		t.Fatal(err)
	}

	got := mustGetNode(t, db, "aa02")
	if got.AdvertCount != before.AdvertCount+1 {
		t.Errorf("advert count: want %d, got %d", before.AdvertCount+1, got.AdvertCount)
	}
	if !got.LastSeen.Equal(nodesNow) {
		t.Errorf("last_seen: want %v, got %v", nodesNow, got.LastSeen)
	}
	if got.BatteryMv == nil || *got.BatteryMv != 3700 || got.TemperatureC == nil || *got.TemperatureC != 21.5 {
		t.Errorf("want battery 3700 and 21.5 C, got %v and %v", got.BatteryMv, got.TemperatureC)
	}
	if !got.Foreign {
		t.Error("want the node marked foreign")
	}
	if got.DefaultScope == nil || *got.DefaultScope != "#oki" {
		t.Errorf("want default scope #oki, got %v", got.DefaultScope)
	}
	if got.ConfiguredScope == nil || *got.ConfiguredScope != "#cin" ||
		got.ConfiguredScopeAt == nil || !got.ConfiguredScopeAt.Equal(reported) || got.ConfiguredScopeAt.Location() != time.UTC {
		t.Errorf("want configured scope #cin at %v in UTC, got %v at %v", reported, got.ConfiguredScope, got.ConfiguredScopeAt)
	}
}

func TestGetNodeByPubkey_FoundAndNotFound_DB(t *testing.T) {
	db := newDB(t)
	seedNodes(t, db, nodeAt("aa01", "Alpha", "repeater", nodesNow))

	got, err := db.GetNodeByPubkey(t.Context(), "AA01")
	if err != nil || got.PublicKey != "aa01" {
		t.Errorf("want the node for a mixed-case key, got %+v (%v)", got, err)
	}
	missing, err := db.GetNodeByPubkey(t.Context(), "ffff")
	if !errors.Is(err, database.ErrNotFound) || missing != nil {
		t.Errorf("want ErrNotFound and no node, got %+v (%v)", missing, err)
	}
}

func TestGetNodeByPrefix_UniqueAmbiguousAndInvalid_DB(t *testing.T) {
	db := newDB(t)
	seedNodes(t, db,
		nodeAt("aabbccdd11", "One", "repeater", nodesNow),
		nodeAt("aabbccdd22", "Two", "repeater", nodesNow),
		nodeAt("11223344aa", "Three", "repeater", nodesNow),
	)

	cases := []struct {
		name      string
		prefix    string
		wantKey   string
		ambiguous bool
	}{
		{"unique, mixed case", "AABBCCDD1", "aabbccdd11", false},
		{"unique, full key", "11223344aa", "11223344aa", false},
		{"shared prefix is ambiguous", "aabbccdd", "", true},
		{"shorter than the minimum", "1122334", "", false},
		{"not hex", "zzzzzzzz", "", false},
		{"no such node", "ffffffff", "", false},
	}
	for _, c := range cases {
		node, ambiguous, err := db.GetNodeByPrefix(t.Context(), c.prefix, 8)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if ambiguous != c.ambiguous {
			t.Errorf("%s: want ambiguous=%v, got %v", c.name, c.ambiguous, ambiguous)
		}
		if (node == nil) != (c.wantKey == "") || (node != nil && node.PublicKey != c.wantKey) {
			t.Errorf("%s: want node %q, got %+v", c.name, c.wantKey, node)
		}
	}
}

func seedListNodesFixture(tb testing.TB, db *database.DB) {
	tb.Helper()
	seedNodes(tb, db,
		nodeAt("aa01", "Alpha Repeater", "repeater", nodesNow.Add(-5*time.Hour)),
		nodeAt("aa02", "Bravo Room", "room", nodesNow.Add(-4*time.Hour)),
		nodeAt("aa03", "Charlie Repeater", "repeater", nodesNow.Add(-3*time.Hour)),
		nodeAt("aa04", "Delta Sensor", "sensor", nodesNow.Add(-2*time.Hour)),
		nodeAt("aa05", "[test] Hidden", "repeater", nodesNow.Add(-time.Hour)),
	)
}

func TestListNodes_FiltersPagingAndTotal_DB(t *testing.T) {
	db := newDB(t)
	seedListNodesFixture(t, db)

	cases := []struct {
		name      string
		params    database.NodeListParams
		want      []string
		wantTotal int64
	}{
		{"default: newest last_seen first", database.NodeListParams{}, []string{"aa05", "aa04", "aa03", "aa02", "aa01"}, 5},
		{"role", database.NodeListParams{Role: "repeater"}, []string{"aa05", "aa03", "aa01"}, 3},
		{"search is a case-insensitive substring", database.NodeListParams{Search: "REPEATER"}, []string{"aa03", "aa01"}, 2},
		{"before bounds first_seen inclusively", database.NodeListParams{Before: ptr(nodesNow.Add(-3 * time.Hour))}, []string{"aa03", "aa02", "aa01"}, 3},
		{"heard since is exclusive", database.NodeListParams{HeardSince: ptr(nodesNow.Add(-3 * time.Hour))}, []string{"aa05", "aa04"}, 2},
		{"exclude keys, any case", database.NodeListParams{ExcludeKeys: []string{"AA04"}}, []string{"aa05", "aa03", "aa02", "aa01"}, 4},
		{"exclude name prefixes", database.NodeListParams{ExcludeNamePrefixes: []string{"[test]", "  "}}, []string{"aa04", "aa03", "aa02", "aa01"}, 4},
		{"limit and offset page, total ignores them", database.NodeListParams{Limit: 2, Offset: 1}, []string{"aa04", "aa03"}, 5},
		{"limit with a filter", database.NodeListParams{Role: "repeater", Limit: 1}, []string{"aa05"}, 3},
		{"offset past the end", database.NodeListParams{Offset: 99}, []string{}, 5},
		{"negative offset counts as zero", database.NodeListParams{Limit: 1, Offset: -3}, []string{"aa05"}, 5},
	}
	for _, c := range cases {
		nodes, total, err := db.ListNodes(t.Context(), c.params)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if nodes == nil {
			t.Errorf("%s: want an empty slice, not nil", c.name)
		}
		if !slices.Equal(nodeKeys(nodes), c.want) || total != c.wantTotal {
			t.Errorf("%s: want %v (total %d), got %v (total %d)", c.name, c.want, c.wantTotal, nodeKeys(nodes), total)
		}
	}
}

func TestListNodes_SortByName_DB(t *testing.T) {
	db := newDB(t)
	seedNodes(t, db,
		nodeAt("aa11", "gamma", "repeater", nodesNow.Add(-3*time.Hour)),
		nodeAt("aa12", "alpha", "repeater", nodesNow.Add(-2*time.Hour)),
		nodeAt("aa13", "beta", "repeater", nodesNow.Add(-time.Hour)),
	)

	nodes, _, err := db.ListNodes(t.Context(), database.NodeListParams{Sort: database.NodeSortName})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"aa12", "aa13", "aa11"}; !slices.Equal(nodeKeys(nodes), want) {
		t.Errorf("want ascending by name %v, got %v", want, nodeKeys(nodes))
	}

	nodes, _, err = db.ListNodes(t.Context(), database.NodeListParams{Sort: "bogus"})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"aa13", "aa12", "aa11"}; !slices.Equal(nodeKeys(nodes), want) {
		t.Errorf("an unknown sort falls back to last_seen, want %v, got %v", want, nodeKeys(nodes))
	}
}

func TestRoleCounts_ActiveWindowAndAll_DB(t *testing.T) {
	db := newDB(t)
	seedNodes(t, db,
		nodeAt("aa01", "r-active", "repeater", nodesNow.Add(-24*time.Hour)),
		nodeAt("aa02", "r-stale", "repeater", nodesNow.Add(-8*24*time.Hour)),
		nodeAt("aa03", "room", "room", nodesNow.Add(-time.Hour)),
		nodeAt("aa04", "companion", "companion", nodesNow.Add(-time.Hour)),
		nodeAt("aa05", "sensor-stale", "sensor", nodesNow.Add(-30*24*time.Hour)),
	)

	active, err := db.GetRoleCounts(t.Context(), nodesNow)
	if err != nil {
		t.Fatal(err)
	}
	if want := (database.RoleCounts{Repeaters: 1, Rooms: 1, Companions: 1, Sensors: 0}); active != want {
		t.Errorf("active window: want %+v, got %+v", want, active)
	}

	all, err := db.GetAllRoleCounts(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if want := (database.RoleCounts{Repeaters: 2, Rooms: 1, Companions: 1, Sensors: 1}); all != want {
		t.Errorf("all nodes: want %+v, got %+v", want, all)
	}
}

func TestSearchNodes_ByNameOrKeyPrefixNewestFirst_DB(t *testing.T) {
	db := newDB(t)
	seedNodes(t, db,
		nodeAt("aa01", "Alpha Repeater", "repeater", nodesNow.Add(-2*time.Hour)),
		nodeAt("aa02", "Alpine Room", "room", nodesNow.Add(-time.Hour)),
		nodeAt("bb03", "Other", "repeater", nodesNow),
	)

	byName, err := db.SearchNodes(t.Context(), "ALP", 10)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"aa02", "aa01"}; !slices.Equal(nodeKeys(byName), want) {
		t.Errorf("name search: want %v newest first, got %v", want, nodeKeys(byName))
	}
	limited, err := db.SearchNodes(t.Context(), "alp", 1)
	if err != nil || len(limited) != 1 || limited[0].PublicKey != "aa02" {
		t.Errorf("limit 1: want only the newest match, got %v (%v)", nodeKeys(limited), err)
	}
	byKey, err := db.SearchNodes(t.Context(), "bb0", 10)
	if err != nil || !slices.Equal(nodeKeys(byKey), []string{"bb03"}) {
		t.Errorf("key prefix search: want [bb03], got %v (%v)", nodeKeys(byKey), err)
	}
	none, err := db.SearchNodes(t.Context(), "zzz", 10)
	if err != nil || none == nil || len(none) != 0 {
		t.Errorf("no match: want an empty non-nil slice, got %v (%v)", none, err)
	}
}

func TestMatchNodesByPrefixAndIdentitiesAndResolve_DB(t *testing.T) {
	db := newDB(t)
	seedNodes(t, db,
		nodeAt("aa01", "Alpha", "repeater", nodesNow),
		nodeAt("aa02", "Bravo", "room", nodesNow),
		nodeAt("bb03", "", "", nodesNow),
	)

	matches, err := db.MatchNodesByPrefix(t.Context(), []string{"aa01", "aa", "zz"})
	if err != nil {
		t.Fatal(err)
	}
	if m := matches["aa01"]; m.Matches != 1 || m.Pubkey != "aa01" || m.Name != "Alpha" {
		t.Errorf("aa01: want one match named Alpha, got %+v", m)
	}
	if m := matches["aa"]; m.Matches != 2 {
		t.Errorf("aa: want 2 matches, got %+v", m)
	}
	if _, ok := matches["zz"]; ok {
		t.Error("a prefix with no node must be absent")
	}

	identities, err := db.ListNodeIdentities(t.Context(), []string{"AA01", "bb03", "ffff"})
	if err != nil {
		t.Fatal(err)
	}
	if id := identities["aa01"]; id.Name == nil || *id.Name != "Alpha" || id.Role == nil || *id.Role != "repeater" {
		t.Errorf("aa01: want name Alpha and role repeater, got %+v", id)
	}
	if id := identities["bb03"]; id.Name != nil || id.Role != nil {
		t.Errorf("bb03: want nil name and role, got %+v", id)
	}
	if _, ok := identities["ffff"]; ok {
		t.Error("an unknown key must be absent")
	}
	if empty, err := db.ListNodeIdentities(t.Context(), nil); err != nil || empty == nil || len(empty) != 0 {
		t.Errorf("no keys: want an empty non-nil map, got %v (%v)", empty, err)
	}

	resolved, err := db.ResolveNodePubkeys(t.Context(), []string{"Alpha", "aa02", "nobody"})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"Alpha": "aa01", "aa02": "aa02", "nobody": "nobody"}
	for in, key := range want {
		if resolved[in] != key {
			t.Errorf("%q: want %q, got %q", in, key, resolved[in])
		}
	}
	one, err := db.ResolveNodePubkey(t.Context(), "Bravo")
	if err != nil || one != "aa02" {
		t.Errorf("single resolve: want aa02, got %q (%v)", one, err)
	}
}

func TestUpdateNodeMultibyteCapsAndList_DB(t *testing.T) {
	db := newDB(t)
	seedNodes(t, db,
		nodeAt("aa01", "One", "repeater", nodesNow),
		nodeAt("aa02", "Two", "repeater", nodesNow),
		nodeAt("aa03", "Three", "repeater", nodesNow),
	)

	caps := []database.NodeMultibyteCap{
		{PublicKey: "AA01", Sup: 2, Evidence: "advert"},
		{PublicKey: "aa02", Sup: 1, Evidence: "path"},
		{PublicKey: "ffff", Sup: 2, Evidence: "ghost"},
	}
	changed, err := db.UpdateNodeMultibyteCaps(t.Context(), caps)
	if err != nil {
		t.Fatal(err)
	}
	if changed != 2 {
		t.Errorf("want 2 rows changed (the unknown node is skipped), got %d", changed)
	}
	again, err := db.UpdateNodeMultibyteCaps(t.Context(), caps)
	if err != nil {
		t.Fatal(err)
	}
	if again != 0 {
		t.Errorf("re-applying identical capabilities must change nothing, got %d", again)
	}

	listed, err := db.ListMultibyteCapNodes(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(listed, func(a, b database.MultibyteCapNode) int {
		return compareStrings(a.Pubkey, b.Pubkey)
	})
	if len(listed) != 2 || listed[0].Pubkey != "aa01" || listed[0].Sup != 2 || listed[0].Evidence != "advert" ||
		listed[0].Name != "One" || listed[0].Role != "repeater" || listed[0].LastSeen.Location() != time.UTC ||
		listed[1].Pubkey != "aa02" || listed[1].Sup != 1 {
		t.Errorf("want aa01 (sup 2, advert) and aa02 (sup 1) only, got %+v", listed)
	}

	if none, err := db.UpdateNodeMultibyteCaps(t.Context(), nil); err != nil || none != 0 {
		t.Errorf("no caps: want 0 changed, got %d (%v)", none, err)
	}
}

func compareStrings(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func TestDeleteNodesByPubkeys_DB(t *testing.T) {
	db := newDB(t)
	seedNodes(t, db,
		nodeAt("aa01", "One", "repeater", nodesNow),
		nodeAt("aa02", "Two", "repeater", nodesNow),
		nodeAt("aa03", "Three", "repeater", nodesNow),
	)

	removed, err := db.DeleteNodesByPubkeys(t.Context(), []string{"AA01", "aa02", "ffff"})
	if err != nil {
		t.Fatal(err)
	}
	if removed != 2 {
		t.Errorf("want 2 nodes removed (mixed case accepted, unknown ignored), got %d", removed)
	}
	remaining, err := db.ListAllNodes(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(nodeKeys(remaining), []string{"aa03"}) {
		t.Errorf("want only aa03 left, got %v", nodeKeys(remaining))
	}
	if none, err := db.DeleteNodesByPubkeys(t.Context(), nil); err != nil || none != 0 {
		t.Errorf("no keys: want 0 removed, got %d (%v)", none, err)
	}
}

func TestListAllNodesAndLocatedNodesAndHealth_DB(t *testing.T) {
	db := newDB(t)
	seedNodes(t, db,
		database.NodeInput{PublicKey: "bb02", Name: "Placed", Role: "repeater", Lat: ptr(39.1), Lon: ptr(-84.5), RxTime: nodesNow, IngestNow: nodesNow},
		nodeAt("aa01", "", "", nodesNow),
	)

	all, err := db.ListAllNodes(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(nodeKeys(all), []string{"aa01", "bb02"}) {
		t.Errorf("want every node ordered by public key, got %v", nodeKeys(all))
	}

	located, err := db.ListLocatedNodes(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(located) != 1 || located[0].PublicKey != "bb02" || located[0].Lat != 39.1 || located[0].Lon != -84.5 {
		t.Errorf("want only the node with coordinates, got %+v", located)
	}
	positions, err := db.ListNodePositions(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(positions) != 1 || positions[0].PublicKey != "bb02" {
		t.Errorf("want only the node with a position, got %+v", positions)
	}

	prune, err := db.ListNodesForGeoPrune(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(prune) != 2 {
		t.Fatalf("want both nodes for geo pruning, got %+v", prune)
	}

	health, err := db.ListNodeHealth(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	roles := map[types.NodeRole]int{}
	for _, h := range health {
		roles[h.Role]++
	}
	if roles[types.RoleUnknown] != 1 || roles["repeater"] != 1 {
		t.Errorf("want one repeater and one unknown-role node, got %v", roles)
	}
}
