package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
)

func TestConfigIsObserverBlacklisted(t *testing.T) {
	cfg := &Config{
		ObserverBlacklist: []string{"OBS1", "obs2", "  Obs3  "},
	}

	tests := []struct {
		id   string
		want bool
	}{
		{"OBS1", true},
		{"obs1", true}, // case-insensitive
		{"OBS2", true},
		{"Obs3", true}, // whitespace trimmed
		{"obs4", false},
		{"", false},
	}

	for _, tt := range tests {
		got := cfg.IsObserverBlacklisted(tt.id)
		if got != tt.want {
			t.Errorf("IsObserverBlacklisted(%q) = %v, want %v", tt.id, got, tt.want)
		}
	}
}

func TestConfigIsObserverBlacklistedEmpty(t *testing.T) {
	cfg := &Config{}
	if cfg.IsObserverBlacklisted("anything") {
		t.Error("empty blacklist should not match anything")
	}
}

func TestConfigIsObserverBlacklistedNil(t *testing.T) {
	var cfg *Config
	if cfg.IsObserverBlacklisted("anything") {
		t.Error("nil config should not match anything")
	}
}

func TestObserverBlacklistFiltersHandleObservers(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	now := time.Now().UTC()

	for _, o := range []database.ObserverInput{
		{ID: "goodobs", Name: "GoodObs", IATA: "SFO", LastSeen: now, IngestNow: now},
		{ID: "badobs", Name: "BadObs", IATA: "LAX", LastSeen: now, IngestNow: now},
	} {
		if _, err := db.CreateOrUpdateObserver(ctx, o); err != nil {
			t.Fatalf("seed observer %s: %v", o.ID, err)
		}
	}

	cfg := &Config{
		ObserverBlacklist: []string{"badobs"},
	}
	srv := NewServer(db, cfg, NewHub())
	srv.RegisterRoutes(setupTestRouter(srv))

	req := httptest.NewRequest("GET", "/api/observers", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp api.ObserverListResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	for _, obs := range resp.Observers {
		if obs.Id == "badobs" {
			t.Error("blacklisted observer should not appear in observers list")
		}
	}

	foundGood := false
	for _, obs := range resp.Observers {
		if obs.Id == "goodobs" {
			foundGood = true
		}
	}
	if !foundGood {
		t.Error("non-blacklisted observer should appear in observers list")
	}
}

func TestObserverBlacklistFiltersObserverDetail(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	now := time.Now().UTC()

	if _, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID: "badobs", Name: "BadObs", IATA: "LAX", LastSeen: now, IngestNow: now,
	}); err != nil {
		t.Fatalf("seed observer: %v", err)
	}

	cfg := &Config{
		ObserverBlacklist: []string{"badobs"},
	}
	srv := NewServer(db, cfg, NewHub())
	srv.RegisterRoutes(setupTestRouter(srv))

	req := httptest.NewRequest("GET", "/api/observers/badobs", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404 for blacklisted observer detail, got %d", w.Code)
	}
}

func TestNoObserverBlacklistPassesAll(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	now := time.Now().UTC()

	if _, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID: "someobs", Name: "SomeObs", IATA: "SFO", LastSeen: now, IngestNow: now,
	}); err != nil {
		t.Fatalf("seed observer: %v", err)
	}

	cfg := &Config{}
	srv := NewServer(db, cfg, NewHub())
	srv.RegisterRoutes(setupTestRouter(srv))

	req := httptest.NewRequest("GET", "/api/observers", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp api.ObserverListResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	foundSome := false
	for _, obs := range resp.Observers {
		if obs.Id == "someobs" {
			foundSome = true
		}
	}
	if !foundSome {
		t.Error("without blacklist, observer should appear")
	}
}

func TestObserverBlacklistConcurrent(t *testing.T) {
	cfg := &Config{
		ObserverBlacklist: []string{"AA", "BB", "CC"},
	}

	done := make(chan struct{})
	for i := 0; i < 50; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 100; j++ {
				cfg.IsObserverBlacklisted("AA")
				cfg.IsObserverBlacklisted("DD")
			}
		}()
	}
	for i := 0; i < 50; i++ {
		<-done
	}
}
