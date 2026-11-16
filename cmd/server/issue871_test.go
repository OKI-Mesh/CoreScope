package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/gorilla/mux"
)

// TestIssue871_NoNullHashOrTimestamp verifies that /api/packets never returns
// packets with null/empty hash or null timestamp (issue #871).
func TestIssue871_NoNullHashOrTimestamp(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)

	// The typed database API still permits an empty legacy hash, but
	// first_seen is NOT NULL and CreateOrUpdateTransmission rejects an empty hash,
	// so neither a NULL nor an empty hash or timestamp can reach /api/packets
	// through the typed database API. This keeps the response-level check.
	now := time.Now().UTC().Add(-30 * time.Minute)
	mustCreateTestTransmission(t, ctx, db, testTransmission(
		"DEAD", "dead0001", "{}", types.RouteFlood, types.PayloadADVERT, "",
	), now)

	cfg := &Config{Port: 3000}
	hub := NewHub()
	srv := NewServer(db, cfg, hub)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	srv.store = store
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	req := httptest.NewRequest(http.MethodGet, "/api/packets?limit=200", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp struct {
		Packets []map[string]interface{} `json:"packets"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	for i, p := range resp.Packets {
		hash, _ := p["hash"]
		ts, _ := p["timestamp"]
		if hash == nil || hash == "" {
			t.Errorf("packet[%d] has null/empty hash: %v", i, p)
		}
		if ts == nil || ts == "" {
			t.Errorf("packet[%d] has null/empty timestamp: %v", i, p)
		}
	}
}
