package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/gorilla/mux"
)

// TestNodesHasMoreSurvivesPostLimitFiltering pins the contract that lets a
// client paginate /api/nodes safely.
//
// handleNodes applies the geo-filter, blacklist, hidden-prefix and area passes
// AFTER the SQL LIMIT/OFFSET, and rewrites Total to the filtered length. So a
// page that loses a row is short without being the last page, and neither the
// page length nor Total can tell a client whether to ask for another page.
// has_more is computed from the raw SQL page against the real COUNT(*), before
// those passes run, and must therefore stay true on a page that filtering
// shortened.
//
// Anti-tautology: move the hasMore assignment below the filter block (or
// compute it from the filtered slice) and the page-1 assertion fails — that is
// exactly the arrangement that stranded every node behind a filtered row.
func TestNodesHasMoreSurvivesPostLimitFiltering(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	srv := NewServer(db, &Config{Port: 3000}, NewHub())
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	now := time.Now().UTC()
	for i := 0; i < 7; i++ {
		name := fmt.Sprintf("visible-%d", i)
		lat, lon := 45.0, -85.0
		switch i {
		case 1:
			name = "outside-geo-1"
			lat, lon = 10.0, 10.0
		case 4:
			name = "🚫 hidden-4"
		}
		if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
			PublicKey: scopeKey(0xdead0000 + i),
			Name:      name,
			Role:      "repeater",
			Lat:       fptr(lat),
			Lon:       fptr(lon),
			RxTime:    time.Date(2026, 6, 7-i, 0, 0, 0, 0, time.UTC),
			IngestNow: now,
		}); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	srv.cfg.SetHiddenNamePrefixes([]string{"🚫"})
	srv.cfg.GeoFilter = &GeoFilterConfig{
		LatMin: fptr(40), LatMax: fptr(50),
		LonMin: fptr(-90), LonMax: fptr(-80),
	}

	page := func(offset int) api.NodeListResponse {
		req := httptest.NewRequest("GET", fmt.Sprintf("/api/nodes?limit=3&offset=%d", offset), nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("offset %d: status %d body=%s", offset, w.Code, w.Body.String())
		}
		var got api.NodeListResponse
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatalf("offset %d: decode: %v", offset, err)
		}
		return got
	}

	first := page(0)
	if len(first.Nodes) != 2 {
		t.Fatalf("page 1: expected 2 rows after the geo-filter dropped one, got %d", len(first.Nodes))
	}
	if !first.HasMore {
		t.Fatalf("page 1: has_more must stay true on a page shortened by the geo-filter " +
			"(3 more nodes are waiting), a client stopping here strands them")
	}

	seen := map[string]bool{}
	for offset, more := 0, true; more && offset < 100; offset += 3 {
		p := page(offset)
		for _, n := range p.Nodes {
			seen[n.PublicKey] = true
		}
		more = p.HasMore
	}
	if len(seen) != 5 {
		t.Fatalf("expected the 5 nodes that pass both filters across the walk, got %d", len(seen))
	}
	for _, i := range []int{1, 4} {
		if seen[scopeKey(0xdead0000+i)] {
			t.Errorf("node %d must not appear: it is hidden or outside the geo-filter", i)
		}
	}

	if last := page(6); last.HasMore {
		t.Fatalf("final page: has_more must be false, got true")
	}
}
