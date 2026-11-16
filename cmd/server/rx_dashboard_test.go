package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// TestRequireClientRxCoverageNilSafe verifies the #4 fix: coverage routes are
// registered unconditionally, so a nil server cfg (or nil *Config receiver)
// must 404 rather than panic.
func TestRequireClientRxCoverageNilSafe(t *testing.T) {
	var nilCfg *Config
	if nilCfg.ClientRxCoverageEnabled() {
		t.Fatal("nil *Config must report disabled")
	}
	req := func(srv *Server) int {
		rr := httptest.NewRecorder()
		srv.handleRxCoverage(rr, httptest.NewRequest("GET", "/api/rx-coverage?bbox=50,3,52,4", nil))
		return rr.Code
	}
	if code := req(&Server{}); code != http.StatusNotFound { // cfg nil → would panic without the guard
		t.Fatalf("nil cfg: want 404, got %d", code)
	}
	if code := req(&Server{cfg: &Config{}}); code != http.StatusNotFound { // feature disabled
		t.Fatalf("disabled: want 404, got %d", code)
	}
}

func insRx(t *testing.T, db *database.DB, rx, hk string, at time.Time, lat, lon float64) {
	t.Helper()
	inserted, err := db.InsertClientReception(t.Context(), types.ClientReception{
		RxPubkey:    rx,
		HeardKey:    hk,
		HeardKeyLen: len(hk) / 2,
		SNR:         fptr(-6.0),
		RSSI:        nil,
		Lat:         lat,
		Lon:         lon,
		PosAccM:     nil,
		RxAt:        at,
		IngestedAt:  at,
		Src:         "rxlog",
	})
	if err != nil {
		t.Fatalf("insert reception: %v", err)
	}
	if !inserted {
		t.Fatalf("reception for %s/%s at %s was not inserted", rx, hk, at)
	}
}

func TestQueryCoverageFiltered(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	now := time.Now().UTC()
	recent := now
	old := now.AddDate(0, 0, -40)

	compA := padKey("c0a0000000000001")
	compB := padKey("c0b0000000000002")

	insRx(t, db, compA, "aabbcc", recent, 51.05, 3.72)
	insRx(t, db, compB, "ffeedd", recent, 51.06, 3.73)
	insRx(t, db, compA, "aabbcc", old, 51.05, 3.72)

	srv := &Server{db: db}
	all, err := srv.queryCoverageFiltered(ctx, "", "", 0, bbox{MinLat: -90, MinLon: -180, MaxLat: 90, MaxLon: 180})
	t.Logf("world bbox, all time: %d rows, err=%v", len(all), err)
	bb := bbox{MinLat: 50, MinLon: 3, MaxLat: 52, MaxLon: 4}

	rows, err := srv.queryCoverageFiltered(ctx, "", "", 7, bb)
	if err != nil {
		t.Fatalf("global 7d: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("global 7d: want 2, got %d", len(rows))
	}

	rows, err = srv.queryCoverageFiltered(ctx, "", compA, 7, bb)
	if err != nil {
		t.Fatalf("observer compA 7d: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("observer compA 7d: want 1, got %d", len(rows))
	}

	rows, err = srv.queryCoverageFiltered(ctx, "", "", 0, bb)
	if err != nil {
		t.Fatalf("global all-time: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("global all-time: want 3, got %d", len(rows))
	}
}

func TestRxLeaderboard(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	now := time.Now().UTC()

	compA := padKey("c0a0000000000001")
	compB := padKey("c0b0000000000002")
	compC := padKey("c0c0000000000003")

	seedNode(t, db, compA, "MyCompanion", "companion", nil, nil, now)

	_, err := db.CreateOrUpdateClientObserver(ctx, types.ClientObserver{
		Pubkey:   compC,
		Name:     "MobOnly",
		LastSeen: now,
	})
	if err != nil {
		t.Fatalf("seed client observer: %v", err)
	}

	for i := 0; i < 3; i++ {
		insRx(t, db, compA, fmt.Sprintf("aabb%02d", i), now, 51.05, 3.72)
	}
	insRx(t, db, compC, "ddee00", now, 51.05, 3.72)
	insRx(t, db, compC, "ddee01", now, 51.05, 3.72)
	insRx(t, db, compB, "aabbcc", now, 51.05, 3.72)

	srv := &Server{db: db}

	obs, err := srv.rxLeaderboard(ctx, 7, 10)
	if err != nil {
		t.Fatal(err)
	}
	byPk := map[string]api.RxLeaderboardObserver{}
	for _, o := range obs {
		byPk[o.Pubkey] = o
	}
	if byPk[compA].Name != "MyCompanion" || byPk[compA].Receptions != 3 {
		t.Fatalf("compA (nodes name): %+v", byPk[compA])
	}
	if byPk[compC].Name != "MobOnly" || byPk[compC].Receptions != 2 {
		t.Fatalf("compC (client_observers fallback): %+v", byPk[compC])
	}
	if byPk[compB].Name != "" {
		t.Fatalf("compB should have no name: %+v", byPk[compB])
	}
}

func TestBatchResolveHeardKeys(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	now := time.Now().UTC()

	alicePK := padKey("aabbccdd11223344")
	bobPK := padKey("aabbcc99887766aa")
	hiddenPK := padKey("ddee110000000000")
	seedNode(t, db, alicePK, "Alice", "repeater", nil, nil, now)
	seedNode(t, db, bobPK, "Bob", "repeater", nil, nil, now)
	seedNode(t, db, hiddenPK, "🚫Hidden", "repeater", nil, nil, now)

	srv := &Server{db: db, cfg: &Config{HiddenNamePrefixes: []string{"🚫"}}}

	got := srv.batchResolveHeardKeys(ctx, []string{"aabbccdd", "aabbcc", "ffff", "ddee11", "aabbccdd"})
	cases := map[string][2]string{
		"aabbccdd": {alicePK, "Alice"},
		"aabbcc":   {"aabbcc", ""},
		"ffff":     {"ffff", ""},
		"ddee11":   {"ddee11", ""},
	}
	for k, want := range cases {
		if got[k] != want {
			t.Errorf("batchResolveHeardKeys[%q] = %v, want %v", k, got[k], want)
		}
	}
}

// TestRxLeaderboardHidesBlacklistedAndHidden verifies #1727 r2 must-fix #2: the
// leaderboard must drop observer-blacklisted contributors and blank the name of
// node-blacklisted or hidden-prefix identities (pre-PR / post-blacklist rows).
func TestRxLeaderboardHidesBlacklistedAndHidden(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	now := time.Now().UTC()

	goodPK := padKey("aa01")
	blockedObsPK := padKey("bb02")
	badNodePK := padKey("cc03")
	hiddenPK := padKey("dd04")

	seedNode(t, db, goodPK, "GoodGuy", "companion", nil, nil, now)
	seedNode(t, db, badNodePK, "BadNode", "companion", nil, nil, now)
	seedNode(t, db, hiddenPK, "🚫Hidden", "companion", nil, nil, now)

	insRx(t, db, goodPK, "aabb01", now, 51.05, 3.72)
	insRx(t, db, blockedObsPK, "aabb02", now, 51.05, 3.72)
	insRx(t, db, badNodePK, "aabb03", now, 51.05, 3.72)
	insRx(t, db, hiddenPK, "aabb04", now, 51.05, 3.72)

	srv := &Server{db: db, cfg: &Config{
		ObserverBlacklist:  []string{blockedObsPK},
		NodeBlacklist:      []string{badNodePK},
		HiddenNamePrefixes: []string{"🚫"},
	}}

	obs, err := srv.rxLeaderboard(ctx, 7, 100)
	if err != nil {
		t.Fatal(err)
	}
	byPk := map[string]api.RxLeaderboardObserver{}
	for _, o := range obs {
		byPk[o.Pubkey] = o
	}
	if _, ok := byPk[blockedObsPK]; ok {
		t.Fatalf("observer-blacklisted contributor must be dropped, got %+v", byPk[blockedObsPK])
	}
	if byPk[goodPK].Name != "GoodGuy" {
		t.Fatalf("normal contributor name should be kept: %+v", byPk[goodPK])
	}
	if _, ok := byPk[badNodePK]; !ok || byPk[badNodePK].Name != "" {
		t.Fatalf("node-blacklisted contributor should remain with a blanked name: %+v", byPk[badNodePK])
	}
	if _, ok := byPk[hiddenPK]; !ok || byPk[hiddenPK].Name != "" {
		t.Fatalf("hidden-prefix contributor should remain with a blanked name: %+v", byPk[hiddenPK])
	}
}

// TestRxLeaderboardLimitSurvivesBlacklistDrop verifies #1727 r2 must-fix #2: the
// SQL LIMIT runs before the Go-side observer-blacklist drop, so the leaderboard
// must over-fetch and still return `limit` non-blacklisted rows even when the
// top contributors are blacklisted (not limit-minus-dropped).
func TestRxLeaderboardLimitSurvivesBlacklistDrop(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	now := time.Now().UTC()

	bk1 := padKey("bb01")
	bk2 := padKey("bb02")
	g1 := padKey("a001")
	g2 := padKey("a002")
	g3 := padKey("a003")
	g4 := padKey("a004")
	g5 := padKey("a005")

	counts := []struct {
		pk string
		n  int
	}{
		{bk1, 10}, {bk2, 9},
		{g1, 8}, {g2, 7}, {g3, 6}, {g4, 5}, {g5, 4},
	}
	for idx, c := range counts {
		for i := 0; i < c.n; i++ {
			insRx(t, db, c.pk, fmt.Sprintf("%02x%04x", idx, i), now, 51.05, 3.72)
		}
	}

	srv := &Server{db: db, cfg: &Config{ObserverBlacklist: []string{bk1, bk2}}}

	obs, err := srv.rxLeaderboard(ctx, 7, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(obs) != 3 {
		t.Fatalf("expected exactly 3 rows after dropping 2 blacklisted from the top, got %d: %+v", len(obs), obs)
	}
	want := []string{g1, g2, g3}
	for i, o := range obs {
		if o.Pubkey == bk1 || o.Pubkey == bk2 {
			t.Fatalf("blacklisted observer %q leaked into the leaderboard", o.Pubkey)
		}
		if o.Pubkey != want[i] {
			t.Fatalf("row %d = %q, want %q (top-3 non-blacklisted by count)", i, o.Pubkey, want[i])
		}
	}
}

// TestRxLeaderboardFrontierScore verifies the leaderboard ranks by frontier-
// weighted cell coverage, not raw reception count: a roaming observer with FEWER
// receptions but MORE distinct cells outranks a stationary spammer, a cell only
// one observer covers weighs 1.0, and a cell shared by N observers weighs 1/N.
func TestRxLeaderboardFrontierScore(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	now := time.Now().UTC()

	park := padKey("a0a0")
	roam := padKey("b0b0")

	for i := 0; i < 5; i++ {
		insRx(t, db, park, fmt.Sprintf("a1%04x", i), now, 51.05, 3.72)
	}
	insRx(t, db, roam, "b20001", now, 51.05, 3.72)
	insRx(t, db, roam, "b20002", now, 51.06, 3.72)
	insRx(t, db, roam, "b20003", now, 51.07, 3.72)

	srv := &Server{db: db}

	obs, err := srv.rxLeaderboard(ctx, 7, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(obs) != 2 {
		t.Fatalf("want 2 observers, got %d: %+v", len(obs), obs)
	}
	if obs[0].Pubkey != roam || obs[1].Pubkey != park {
		t.Fatalf("ranking: want [roam park], got [%s %s]", obs[0].Pubkey, obs[1].Pubkey)
	}
	byPk := map[string]api.RxLeaderboardObserver{}
	for _, o := range obs {
		byPk[o.Pubkey] = o
	}
	if byPk[park].Cells != 1 || byPk[park].Receptions != 5 {
		t.Fatalf("park: %+v", byPk[park])
	}
	if d := byPk[park].Score - 0.5; d > 1e-9 || d < -1e-9 {
		t.Fatalf("park score: want 0.5, got %v", byPk[park].Score)
	}
	if byPk[roam].Cells != 3 {
		t.Fatalf("roam cells: want 3, got %d", byPk[roam].Cells)
	}
	if d := byPk[roam].Score - 2.5; d > 1e-9 || d < -1e-9 {
		t.Fatalf("roam score: want 2.5, got %v", byPk[roam].Score)
	}
}

// TestRxLeaderboardScoreNotDilutedByBlacklisted verifies the #review-r2 fix: a
// blacklisted observer sharing a cell must NOT dilute a legitimate observer's
// frontier score. Without excluding blacklisted pubkeys from the per-cell count,
// the legit observer's only cell would weigh 1/2 = 0.5 instead of 1.0.
func TestRxLeaderboardScoreNotDilutedByBlacklisted(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	now := time.Now().UTC()

	good := padKey("a0d0")
	bad := padKey("bad0")

	insRx(t, db, good, "aabb01", now, 51.05, 3.72)
	insRx(t, db, bad, "aabb02", now, 51.05, 3.72)

	srv := &Server{db: db, cfg: &Config{ObserverBlacklist: []string{bad}}}

	obs, err := srv.rxLeaderboard(ctx, 7, 100)
	if err != nil {
		t.Fatal(err)
	}
	byPk := map[string]api.RxLeaderboardObserver{}
	for _, o := range obs {
		byPk[o.Pubkey] = o
	}
	if _, leaked := byPk[bad]; leaked {
		t.Fatalf("blacklisted observer must not appear: %+v", byPk[bad])
	}
	if d := byPk[good].Score - 1.0; d > 1e-9 || d < -1e-9 {
		t.Fatalf("blacklisted observer diluted the score: got %v, want 1.0", byPk[good].Score)
	}
}
