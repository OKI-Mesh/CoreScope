package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// serverWithRepeaters seeds one repeater per (pubkey, name) pair and returns a
// server whose store has loaded them, with cfg applied.
func serverWithRepeaters(t *testing.T, cfg *Config, nodes [][2]string) *Server {
	t.Helper()
	ctx := t.Context()
	db := setupTestDB(t)
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	for _, n := range nodes {
		seedNode(t, db, n[0], n[1], types.RoleRepeater, nil, nil, at)
	}
	cfg.SnapshotDir = t.TempDir()
	srv := NewServer(db, cfg, NewHub())
	store := newPacketStoreForTest(t, db)
	store.config = cfg
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache(): %v", err)
	}
	srv.store = store
	srv.RegisterRoutes(setupTestRouter(srv))
	return srv
}

func subpathDetailStatus(srv *Server, hops string) int {
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/analytics/subpath-detail?hops="+hops, nil))
	return w.Code
}

func TestSubpathDetail_BlacklistedNodeIsHiddenByHopPrefix(t *testing.T) {
	blacklisted := "ab12" + strings.Repeat("0", 60)
	other := "cd34" + strings.Repeat("0", 60)
	srv := serverWithRepeaters(t, &Config{NodeBlacklist: []string{blacklisted}}, [][2]string{
		{blacklisted, "Blocked"},
		{other, "Fine"},
	})

	// A path hop is the first one to three bytes of a pubkey, never the whole key.
	for _, hops := range []string{"ab,cd34", "cd34,ab12", "cd34,ab1200"} {
		if got := subpathDetailStatus(srv, hops); got != http.StatusNotFound {
			t.Errorf("hops=%s: status = %d, want 404 for a hop that stands for a blacklisted node", hops, got)
		}
	}
	// The full key still works too.
	if got := subpathDetailStatus(srv, "cd34,"+blacklisted); got != http.StatusNotFound {
		t.Errorf("full blacklisted key: status = %d, want 404", got)
	}
	if got := subpathDetailStatus(srv, "cd34,ef56"); got == http.StatusNotFound {
		t.Errorf("hops with no blacklisted node: status = 404, want the normal answer")
	}
}

func TestSubpathDetail_HiddenNamePrefixIsHiddenByHopPrefix(t *testing.T) {
	hidden := "ab12" + strings.Repeat("0", 60)
	other := "cd34" + strings.Repeat("0", 60)
	srv := serverWithRepeaters(t, &Config{HiddenNamePrefixes: []string{"zz-"}}, [][2]string{
		{hidden, "zz-secret"},
		{other, "Fine"},
	})

	if got := subpathDetailStatus(srv, "cd34,ab12"); got != http.StatusNotFound {
		t.Errorf("hop standing for a hidden-name node: status = %d, want 404", got)
	}
	if got := subpathDetailStatus(srv, "cd34,ef56"); got == http.StatusNotFound {
		t.Errorf("hops with no hidden node: status = 404, want the normal answer")
	}
}

// A prefix shared with a blacklisted node hides the hop too (option A in the
// plan). This test records that on purpose: when the filter moves to resolved
// node ids, change the expectation to 200 here.
func TestSubpathDetail_SharedPrefixIsHiddenForNow(t *testing.T) {
	blacklisted := "ab12" + strings.Repeat("0", 60)
	sibling := "ab99" + strings.Repeat("0", 60)
	srv := serverWithRepeaters(t, &Config{NodeBlacklist: []string{blacklisted}}, [][2]string{
		{blacklisted, "Blocked"},
		{sibling, "Sibling"},
	})

	if got := subpathDetailStatus(srv, "cd34,ab"); got != http.StatusNotFound {
		t.Errorf("one-byte hop shared by a blacklisted node and a sibling: status = %d, want 404 for now", got)
	}
	if got := subpathDetailStatus(srv, "cd34,ab99"); got == http.StatusNotFound {
		t.Errorf("two-byte hop that only the sibling matches: status = 404, want the normal answer")
	}
}
