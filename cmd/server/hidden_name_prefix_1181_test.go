package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
)

// TestHiddenNamePrefix_1181 verifies operator-configurable name-prefix hiding
// for nodes (issue #1181). When the operator configures HiddenNamePrefixes,
// nodes whose name begins with any configured prefix are omitted from API
// responses (list, search, detail). DB rows are preserved — filtering happens
// at the API layer only.
func TestHiddenNamePrefix_1181_NodesList(t *testing.T) {
	srv, router := setupTestServer(t)

	// Insert a node whose name starts with the configured 🚫 prefix.
	now := time.Now().UTC()
	mustCreateTestNode(t, t.Context(), srv.db, database.NodeInput{
		PublicKey: "deadbeef00001181", Name: "🚫 ban me", Role: "companion", RxTime: now, IngestNow: now,
	})

	get := func() []map[string]interface{} {
		req := httptest.NewRequest("GET", "/api/nodes?limit=2000", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
		}
		var resp struct {
			Nodes []map[string]interface{} `json:"nodes"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal: %v body=%s", err, w.Body.String())
		}
		return resp.Nodes
	}

	hasName := func(nodes []map[string]interface{}, substr string) bool {
		for _, n := range nodes {
			if name, _ := n["name"].(string); strings.Contains(name, substr) {
				return true
			}
		}
		return false
	}

	// Empty prefix list: node MUST be present.
	srv.cfg.SetHiddenNamePrefixes(nil)
	if !hasName(get(), "ban me") {
		t.Fatalf("with empty HiddenNamePrefixes, node should be present in /api/nodes")
	}

	// Configured 🚫 prefix: node MUST be omitted.
	srv.cfg.SetHiddenNamePrefixes([]string{"🚫"})
	if hasName(get(), "ban me") {
		t.Fatalf("with HiddenNamePrefixes=[\"🚫\"], node 🚫 ban me should be hidden from /api/nodes")
	}
}

// TestHiddenNamePrefix_1181_Search ensures hidden nodes are also filtered
// from /api/nodes/search.
func TestHiddenNamePrefix_1181_Search(t *testing.T) {
	srv, router := setupTestServer(t)

	now := time.Now().UTC()
	mustCreateTestNode(t, t.Context(), srv.db, database.NodeInput{
		PublicKey: "deadbeef00001182", Name: "🚫 search me", Role: "companion", RxTime: now, IngestNow: now,
	})

	srv.cfg.SetHiddenNamePrefixes([]string{"🚫"})

	req := httptest.NewRequest("GET", "/api/nodes/search?q=search", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		Nodes []map[string]interface{} `json:"nodes"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, n := range resp.Nodes {
		if name, _ := n["name"].(string); strings.Contains(name, "search me") {
			t.Fatalf("hidden node leaked through /api/nodes/search: %v", n)
		}
	}
}

// TestHiddenNamePrefix_1181_Detail ensures /api/nodes/{pubkey} returns 404
// for a node whose name starts with a hidden prefix — mirroring the
// blacklist behaviour so callers learn nothing about whether the row exists.
func TestHiddenNamePrefix_1181_Detail(t *testing.T) {
	srv, router := setupTestServer(t)

	pk := "deadbeef00001183"
	now := time.Now().UTC()
	mustCreateTestNode(t, t.Context(), srv.db, database.NodeInput{
		PublicKey: pk, Name: "🚫 detail me", Role: "companion", RxTime: now, IngestNow: now,
	})

	get := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", "/api/nodes/"+pk, nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w
	}

	// Empty prefix list: detail MUST be reachable (200 with the name).
	srv.cfg.SetHiddenNamePrefixes(nil)
	w := get()
	if w.Code != http.StatusOK {
		t.Fatalf("baseline: expected 200, got %d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "detail me") {
		t.Fatalf("baseline: response missing node name; body=%s", w.Body.String())
	}

	// Configured 🚫 prefix: detail MUST 404 — no name, no fields, nothing.
	srv.cfg.SetHiddenNamePrefixes([]string{"🚫"})
	w = get()
	if w.Code != http.StatusNotFound {
		t.Fatalf("hidden: expected 404, got %d body=%s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "detail me") {
		t.Fatalf("hidden: name leaked in 404 body: %s", w.Body.String())
	}
}
