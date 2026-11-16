package database_test

import (
	"slices"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

func seedLeaderboard(tb testing.TB, db *database.DB, base time.Time) {
	tb.Helper()
	ctx := tb.Context()

	if _, err := db.CreateOrUpdateClientObserver(ctx, types.ClientObserver{
		Pubkey: "rxa", Name: "Alice Phone", LastSeen: base,
	}); err != nil {
		tb.Fatal(err)
	}
	if _, err := db.CreateOrUpdateClientObserver(ctx, types.ClientObserver{
		Pubkey: "rxc", Name: "Carol Phone", LastSeen: base,
	}); err != nil {
		tb.Fatal(err)
	}
	if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: "rxc", Name: "Hill Repeater",
	}); err != nil {
		tb.Fatal(err)
	}

	receptions := []types.ClientReception{
		{RxPubkey: "rxa", HeardKey: "a1b2", HeardKeyLen: 2, Lat: 39.10, Lon: -84.50, RxAt: base, IngestedAt: base, Src: "ble"},
		{RxPubkey: "rxb", HeardKey: "a1b2", HeardKeyLen: 2, Lat: 39.20, Lon: -84.40, RxAt: base.Add(time.Hour), IngestedAt: base, Src: "ble"},
		{RxPubkey: "rxc", HeardKey: "a1b2", HeardKeyLen: 2, Lat: 39.30, Lon: -84.30, RxAt: base.Add(2 * time.Hour), IngestedAt: base, Src: "ble"},
		{RxPubkey: "rxa", HeardKey: "c3d4", HeardKeyLen: 2, Lat: 39.40, Lon: -84.20, RxAt: base.Add(2 * time.Hour), IngestedAt: base, Src: "ble"},
	}
	for i := range receptions {
		inserted, err := db.InsertClientReception(ctx, receptions[i])
		if err != nil {
			tb.Fatal(err)
		}
		if !inserted {
			tb.Fatalf("reception %d: want inserted", i)
		}
	}
}

func collectLeaderboard(tb testing.TB, db *database.DB, since time.Time, limit int) []database.LeaderboardSample {
	tb.Helper()
	samples, err := db.ListLeaderboardSamples(tb.Context(), since, limit)
	if err != nil {
		tb.Fatal(err)
	}
	return samples
}

func leaderboardKeys(samples []database.LeaderboardSample) []string {
	keys := make([]string, len(samples))
	for i, s := range samples {
		keys[i] = s.RxPubkey + "/" + s.HeardKey
	}
	return keys
}

func TestEachLeaderboardSample_NewestFirstWithDeterministicTies_DB(t *testing.T) {
	db := newDB(t)
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	seedLeaderboard(t, db, base)

	got := collectLeaderboard(t, db, base, 10)

	want := []string{"rxa/c3d4", "rxc/a1b2", "rxb/a1b2", "rxa/a1b2"}
	if !slices.Equal(leaderboardKeys(got), want) {
		t.Errorf("want %v (newest first, tie by receiver), got %v", want, leaderboardKeys(got))
	}
}

func TestEachLeaderboardSample_NamePrefersNodeThenCompanionThenEmpty_DB(t *testing.T) {
	db := newDB(t)
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	seedLeaderboard(t, db, base)

	names := map[string]string{}
	for _, s := range collectLeaderboard(t, db, base, 10) {
		names[s.RxPubkey] = s.Name
	}

	want := map[string]string{"rxa": "Alice Phone", "rxb": "", "rxc": "Hill Repeater"}
	for pubkey, name := range want {
		if names[pubkey] != name {
			t.Errorf("%s: want name %q, got %q", pubkey, name, names[pubkey])
		}
	}
}

func TestEachLeaderboardSample_CarriesPositionAndHeardKey_DB(t *testing.T) {
	db := newDB(t)
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	seedLeaderboard(t, db, base)

	got := collectLeaderboard(t, db, base, 1)

	if len(got) != 1 {
		t.Fatalf("want 1 sample, got %+v", got)
	}
	want := database.LeaderboardSample{
		RxPubkey: "rxa", Name: "Alice Phone", Lat: 39.40, Lon: -84.20, HeardKey: "c3d4",
	}
	if got[0] != want {
		t.Errorf("want %+v, got %+v", want, got[0])
	}
}

func TestListLeaderboardSamples_SinceIsInclusiveAndLimitCaps_DB(t *testing.T) {
	db := newDB(t)
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	seedLeaderboard(t, db, base)

	cases := []struct {
		name  string
		since time.Time
		limit int
		want  []string
	}{
		{"since equal to the oldest keeps it", base, 10, []string{"rxa/c3d4", "rxc/a1b2", "rxb/a1b2", "rxa/a1b2"}},
		{"since one microsecond after the oldest drops it", base.Add(time.Microsecond), 10, []string{"rxa/c3d4", "rxc/a1b2", "rxb/a1b2"}},
		{"since equal to the newest keeps the tied pair", base.Add(2 * time.Hour), 10, []string{"rxa/c3d4", "rxc/a1b2"}},
		{"since after everything", base.Add(24 * time.Hour), 10, nil},
		{"limit caps the newest", base, 2, []string{"rxa/c3d4", "rxc/a1b2"}},
		{"zero limit returns nothing", base, 0, nil},
	}
	for _, c := range cases {
		got := leaderboardKeys(collectLeaderboard(t, db, c.since, c.limit))
		if !slices.Equal(got, c.want) && !(len(got) == 0 && len(c.want) == 0) {
			t.Errorf("%s: want %v, got %v", c.name, c.want, got)
		}
	}
}

func TestListLeaderboardSamples_NegativeLimitIsAnError_DB(t *testing.T) {
	db := newDB(t)
	samples, err := db.ListLeaderboardSamples(t.Context(), time.Time{}, -1)
	if err == nil {
		t.Fatal("want an error for a negative limit")
	}
	if samples != nil {
		t.Errorf("want nil samples on error, got %+v", samples)
	}
}

func TestListLeaderboardSamples_EmptyDatabaseStreamsNothing_DB(t *testing.T) {
	db := newDB(t)
	if got := collectLeaderboard(t, db, time.Time{}, 10); len(got) != 0 {
		t.Errorf("want no samples, got %+v", got)
	}
}
