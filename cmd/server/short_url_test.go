package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/OKI-Mesh/CoreScope/internal/database"
)

// Issue #772 — shortened URL for easier sending over the mesh.
//
// Public keys are 64 hex chars. Operators want to share node URLs over a
// mesh radio link where every byte counts. We allow truncating the pubkey
// in the URL down to a minimum 8-hex-char prefix; the server resolves the
// prefix back to the full pubkey when (and only when) it is unambiguous.

func TestResolveNodePrefix_Unique(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	// "aabbccdd" uniquely identifies the seeded TestRepeater (pubkey aabbccdd11223344).
	node, ambiguous, err := db.GetNodeByPrefix(ctx, "aabbccdd", 8)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ambiguous {
		t.Fatalf("expected unambiguous match, got ambiguous=true")
	}
	if node == nil {
		t.Fatalf("expected node, got nil")
	}
	if got := node.PublicKey; got != "aabbccdd11223344" {
		t.Errorf("expected public_key aabbccdd11223344, got %q", got)
	}
}

func TestResolveNodePrefix_Ambiguous(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	// Insert a second node sharing the 8-char prefix "aabbccdd".
	mustCreateTestNode(t, ctx, db, database.NodeInput{
		PublicKey: "aabbccdd99887766",
		Name:      "OtherNode",
		Role:      "companion",
	})

	node, ambiguous, err := db.GetNodeByPrefix(ctx, "aabbccdd", 8)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !ambiguous {
		t.Fatalf("expected ambiguous=true for shared prefix, got false (node=%v)", node)
	}
	if node != nil {
		t.Errorf("expected nil node when ambiguous, got %v", node.PublicKey)
	}
}

func TestResolveNodePrefix_TooShort(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	// <8 hex chars must NOT resolve, even if it would be unique.
	node, _, err := db.GetNodeByPrefix(ctx, "aabbccd", 8)
	if err == nil && node != nil {
		t.Errorf("expected nil/error for 7-char prefix, got node %v", node.PublicKey)
	}
}

// Route-level: GET /api/nodes/<8-char-prefix> resolves to the full node.
func TestNodeDetailRoute_PrefixResolves(t *testing.T) {
	_, router := setupTestServer(t)

	req := httptest.NewRequest("GET", "/api/nodes/aabbccdd", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for unique 8-char prefix, got %d body=%s", w.Code, w.Body.String())
	}
	var body NodeDetailResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	pk := body.Node.PublicKey
	if pk != "aabbccdd11223344" {
		t.Errorf("expected resolved pubkey aabbccdd11223344, got %q", pk)
	}
}

// Route-level: GET /api/nodes/<ambiguous-prefix> returns 409 with a hint.
func TestNodeDetailRoute_PrefixAmbiguous(t *testing.T) {
	srv, router := setupTestServer(t)
	mustCreateTestNode(t, t.Context(), srv.db, database.NodeInput{
		PublicKey: "aabbccdd99887766",
		Name:      "OtherNode",
		Role:      "companion",
	})

	req := httptest.NewRequest("GET", "/api/nodes/aabbccdd", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409 for ambiguous prefix, got %d body=%s", w.Code, w.Body.String())
	}
}
