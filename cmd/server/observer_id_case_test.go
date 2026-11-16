package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Observer pubkeys are stored lowercase, so every observer route that takes an
// id lowercases it. The id is requested in mixed case and must give the same
// answer as the lowercase one.
func TestObserverRoutes_IDIsCaseInsensitive(t *testing.T) {
	_, router := setupTestServer(t)

	get := func(path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		return w
	}

	for _, route := range []struct {
		name   string
		suffix string
		// compareBody is false where the body can move with the clock.
		compareBody bool
	}{
		{"detail", "", true},
		{"metrics", "/metrics", false},
		{"analytics", "/analytics", false},
	} {
		t.Run(route.name, func(t *testing.T) {
			lower := get("/api/observers/obs1" + route.suffix)
			mixed := get("/api/observers/ObS1" + route.suffix)
			upper := get("/api/observers/OBS1" + route.suffix)
			if lower.Code != http.StatusOK {
				t.Fatalf("lowercase id = %d, want 200: %s", lower.Code, lower.Body.String())
			}
			for name, got := range map[string]*httptest.ResponseRecorder{"mixed case": mixed, "upper case": upper} {
				if got.Code != lower.Code {
					t.Errorf("%s id = %d, want %d: %s", name, got.Code, lower.Code, got.Body.String())
				}
				if route.compareBody && got.Body.String() != lower.Body.String() {
					t.Errorf("%s id body differs from the lowercase id body", name)
				}
			}
		})
	}
}

// The analytics aggregate reads the store's per-observer list, which is keyed by
// the lowercase id, so a mixed-case id must still find the observer's packets.
func TestObserverAnalytics_MixedCaseIDFindsPackets(t *testing.T) {
	_, router := setupTestServer(t)

	for _, id := range []string{"obs1", "OBS1", "ObS1"} {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/observers/"+id+"/analytics", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("id %q: status = %d, want 200: %s", id, w.Code, w.Body.String())
		}
		var body struct {
			PacketTypes   map[string]int `json:"packetTypes"`
			RecentPackets []any          `json:"recentPackets"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("id %q: decode: %v", id, err)
		}
		if len(body.RecentPackets) == 0 {
			t.Errorf("id %q: recentPackets is empty, want the observer's packets", id)
		}
		if body.PacketTypes["4"] != 2 || body.PacketTypes["5"] != 1 {
			t.Errorf("id %q: packetTypes = %v, want 4:2 and 5:1\nbody: %.600s", id, body.PacketTypes, w.Body.String())
		}
	}
}
