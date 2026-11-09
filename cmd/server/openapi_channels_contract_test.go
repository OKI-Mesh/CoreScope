package main

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/gorilla/mux"
)

// setupChannelScopeServer serves the seeded #scopetest channel through the
// real router.
func setupChannelScopeServer(t *testing.T) (*Server, *mux.Router) {
	t.Helper()
	ctx := t.Context()
	db := setupChannelScopeDB(t)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	srv := NewServer(db, &Config{Port: 3000}, NewHub())
	srv.store = store
	router := mux.NewRouter()
	srv.RegisterRoutes(router)
	return srv, router
}

func TestChannelEndpointsMatchSpec(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := setupChannelScopeServer(t)

	for _, path := range []string{
		"/api/channels",
		"/api/channels?includeEncrypted=true",
		"/api/channels?region=BRU",
		"/api/channels/%23scopetest/messages",
		"/api/channels/%23scopetest/messages?limit=1&offset=1",
		"/api/channels/%23scopetest/messages?region=BRU",
		"/api/channels/%23nobody/messages",
	} {
		t.Run(path, func(t *testing.T) {
			checker.check(t, router, path)
		})
	}
}

func TestChannelMessagesDecodeAsSpecTypes(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := setupChannelScopeServer(t)

	rec := checker.checkStatus(t, router, "/api/channels/%23scopetest/messages?limit=50", http.StatusOK)
	var got api.ChannelMessagesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v body=%s", err, rec.Body.String())
	}
	assertChannelMessageScopes(t, got.Messages)
	for _, m := range got.Messages {
		if m.Timestamp.IsZero() || m.FirstSeen.IsZero() {
			t.Errorf("%s: zero timestamp or first_seen", m.PacketHash)
		}
	}
}

func TestKnownChannelsMatchSpec(t *testing.T) {
	checker := newSpecChecker(t)
	srv, router := setupTestServer(t)

	// Empty cache, as production serves it today.
	checker.check(t, router, "/api/known-channels")

	snap, err := parseKnownChannelsJSON([]byte(knownChannelsFixture), "fixture://test", time.Unix(1700000000, 0))
	if err != nil {
		t.Fatal(err)
	}
	cache := newKnownChannelsCache("", time.Hour)
	cache.ptr.Store(snap)
	srv.knownChannels = cache

	for _, path := range []string{"/api/known-channels", "/api/known-channels?region=BE", "/api/known-channels?region=ZZ"} {
		t.Run(path, func(t *testing.T) {
			checker.check(t, router, path)
		})
	}

	rec := checker.checkStatus(t, router, "/api/known-channels", http.StatusOK)
	var got api.KnownChannelsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Entries) != 3 || got.Source != "fixture://test" || !got.FetchedAt.Equal(time.Unix(1700000000, 0)) {
		t.Errorf("response = %+v", got)
	}
}

func TestResolveHopsMatchSpec(t *testing.T) {
	ctx := t.Context()
	checker := newSpecChecker(t)
	srv, router := setupTestServer(t)

	now := time.Now().UTC()
	for _, n := range []struct {
		pk       string
		name     string
		lat, lon float64
	}{
		{"ee1aaaaaaa" + strings.Repeat("0", 54), "Node-E1", 37.0, -122.0},
		{"ee1bbbbbbb" + strings.Repeat("0", 54), "Node-E2", 38.0, -121.0},
		{"dd1ccccccc" + strings.Repeat("0", 54), "Node-D1", 37.5, -122.5},
	} {
		if err := srv.db.CreateOrUpdateNode(ctx, database.NodeInput{
			PublicKey: n.pk, Name: n.name, Role: "repeater",
			Lat: fptr(n.lat), Lon: fptr(n.lon), RxTime: now, IngestNow: now,
		}); err != nil {
			t.Fatalf("seed %s: %v", n.name, err)
		}
	}
	if err := srv.store.RefreshNodeCache(ctx); err != nil {
		t.Fatal(err)
	}

	for _, path := range []string{
		"/api/resolve-hops",
		"/api/resolve-hops?hops=ee1",
		"/api/resolve-hops?hops=dd1",
		"/api/resolve-hops?hops=ff9",
		"/api/resolve-hops?hops=ee1,dd1,ff9",
		"/api/resolve-hops?hops=ee1&from_node=" + "dd1ccccccc" + strings.Repeat("0", 54),
		"/api/resolve-hops?hops=ee1&observer=obs1",
	} {
		t.Run(path, func(t *testing.T) {
			checker.check(t, router, path)
		})
	}

	rec := checker.checkStatus(t, router, "/api/resolve-hops?hops=ee1,dd1,ff9", http.StatusOK)
	var got api.ResolveHopsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if c := got.Resolved["dd1"].Confidence; c != "unique_prefix" {
		t.Errorf("dd1 confidence = %q, want unique_prefix", c)
	}
	if c := got.Resolved["ff9"].Confidence; c != "no_match" {
		t.Errorf("ff9 confidence = %q, want no_match", c)
	}
	if n := len(got.Resolved["ee1"].Candidates); n != 2 {
		t.Errorf("ee1 candidates = %d, want 2", n)
	}
}

func TestTracesMatchSpec(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := setupTestServer(t)

	checker.check(t, router, "/api/traces/abc123def4567890")
	checker.check(t, router, "/api/traces/0000000000000000")

	rec := checker.checkStatus(t, router, "/api/traces/abc123def4567890", http.StatusOK)
	var got api.TraceResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Traces) != 2 {
		t.Errorf("traces = %d, want 2", len(got.Traces))
	}
}

func TestProdChannelFixturesMatchSpec(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromData(api.Spec)
	if err != nil {
		t.Fatalf("load spec: %v", err)
	}
	fixtures := []struct{ file, schema string }{
		{"channel_list_prod.json", "ChannelListResponse"},
		{"channel_messages_prod.json", "ChannelMessagesResponse"},
		{"resolve_hops_prod.json", "ResolveHopsResponse"},
		{"traces_prod.json", "TraceResponse"},
		{"known_channels_prod.json", "KnownChannelsResponse"},
	}
	for _, f := range fixtures {
		t.Run(f.file, func(t *testing.T) {
			schema, ok := doc.Components.Schemas[f.schema]
			if !ok {
				t.Fatalf("%s is not in the spec", f.schema)
			}
			raw, err := os.ReadFile("testdata/" + f.file)
			if err != nil {
				t.Fatal(err)
			}
			var decoded interface{}
			if err := json.Unmarshal(raw, &decoded); err != nil {
				t.Fatalf("decode fixture: %v", err)
			}
			if err := schema.Value.VisitJSON(decoded); err != nil {
				t.Errorf("production response does not match %s: %v", f.schema, err)
			}
		})
	}
}
