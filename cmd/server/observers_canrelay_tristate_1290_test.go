package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

func bptr(b bool) *bool { return &b }

// Issue #1290 (MAJOR-2, adversarial review of PR #1624) — tri-state badge.
//
// The badge surface needs to distinguish three states:
//  1. legacy observer (never sent `repeat` field) → unknown → no badge
//  2. firmware confirmed `repeat:on`              → "Repeater"
//  3. firmware confirmed `repeat:off`             → "Listener"
//
// Previously `CanRelay bool` defaulted to false in Go even when the row
// was the legacy DEFAULT 1, conflating "confirmed repeater" with
// "unknown". This pins the API surface to *bool + JSON omitempty so the
// frontend tri-state render works.
type observerTriStateResponse struct {
	ID       string `json:"id"`
	CanRelay *bool  `json:"can_relay"`
}

func TestObservers_CanRelayTriState_Issue1290(t *testing.T) {
	srv, router := setupTestServer(t)
	ctx := t.Context()

	now := time.Now().UTC()
	legacyID := scopeKey(1)
	repeaterID := scopeKey(2)
	listenerID := scopeKey(3)

	if _, err := srv.db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        legacyID,
		Name:      "Legacy",
		IATA:      "SJC",
		Meta:      nil,
		LastSeen:  now,
		IngestNow: now,
	}); err != nil {
		t.Fatalf("seed legacy: %v", err)
	}
	if _, err := srv.db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        repeaterID,
		Name:      "Repeater",
		IATA:      "SFO",
		Meta:      &types.ObserverMeta{CanRelay: bptr(true)},
		LastSeen:  now,
		IngestNow: now,
	}); err != nil {
		t.Fatalf("seed repeater: %v", err)
	}
	if _, err := srv.db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        listenerID,
		Name:      "Listener",
		IATA:      "OAK",
		Meta:      &types.ObserverMeta{CanRelay: bptr(false)},
		LastSeen:  now,
		IngestNow: now,
	}); err != nil {
		t.Fatalf("seed listener: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/observers?nocache=1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}

	var body struct {
		Observers []observerTriStateResponse `json:"observers"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("json: %v", err)
	}

	byID := make(map[string]observerTriStateResponse, len(body.Observers))
	for _, o := range body.Observers {
		byID[o.ID] = o
	}

	legacy, ok := byID[legacyID]
	if !ok {
		ids := make([]string, 0, len(byID))
		for id := range byID {
			ids = append(ids, id)
		}
		t.Fatalf("legacy observer missing from response; got ids: %v", ids)
	}
	if legacy.CanRelay != nil {
		t.Errorf("legacy observer (never sent repeat) should have can_relay omitted (unknown); got can_relay=%v", *legacy.CanRelay)
	}

	repeater, ok := byID[repeaterID]
	if !ok {
		t.Fatalf("repeater observer missing from response")
	}
	if repeater.CanRelay == nil || !*repeater.CanRelay {
		t.Errorf("repeater observer: expected can_relay=true, got %v", repeater.CanRelay)
	}

	listener, ok := byID[listenerID]
	if !ok {
		t.Fatalf("listener observer missing from response")
	}
	if listener.CanRelay == nil || *listener.CanRelay {
		t.Errorf("listener observer: expected can_relay=false, got %v", listener.CanRelay)
	}

	raw := w.Body.String()
	idx := strings.Index(raw, `"id":"`+legacyID+`"`)
	if idx < 0 {
		t.Fatalf("legacy observer id not found in raw JSON")
	}
	end := strings.Index(raw[idx:], "}")
	if end <= 0 {
		t.Fatalf("could not find end of legacy observer object in raw JSON")
	}
	rowStr := raw[idx : idx+end]
	if strings.Contains(rowStr, `"can_relay"`) {
		t.Errorf("legacy observer raw JSON unexpectedly contains can_relay key: %s", rowStr)
	}
}
