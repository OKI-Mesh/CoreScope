package main

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

func TestChannelsCacheServesWarmEntry(t *testing.T) {
	s := &Server{}
	resp := ChannelListResponse{
		Channels: []ChannelResponse{
			{Hash: "enc_SENTINEL", Encrypted: true, Name: "enc_SENTINEL"},
		},
	}
	cacheKey := channelsCacheKey("", true)
	s.channelsCache.entries.Store(cacheKey, &channelsCacheEntry{resp: resp, at: time.Now()})

	req := httptest.NewRequest("GET", "/api/channels?includeEncrypted=true", nil)
	w := httptest.NewRecorder()
	s.handleChannels(w, req)

	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "enc_SENTINEL") {
		t.Fatalf("expected cached sentinel in body, got: %s", w.Body.String())
	}
	if h := w.Header().Get("X-Cache-Age-Seconds"); h == "" {
		t.Error("expected X-Cache-Age-Seconds header on cached response")
	}
	if got := s.channelsCache.fillCount.Load(); got != 0 {
		t.Fatalf("warm cache must not increment fillCount, got %d", got)
	}
}

func TestChannelsCacheTTLBoundary(t *testing.T) {
	db := setupTestDB(t)
	s := NewServer(db, &Config{Port: 3000}, NewHub())

	if !s.isChannelsListCacheStale(time.Time{}) {
		t.Error("zero time should be expired")
	}
	if s.isChannelsListCacheStale(time.Now()) {
		t.Error("just-now should not be expired")
	}
	if !s.isChannelsListCacheStale(time.Now().Add(-channelsCacheTTL - time.Second)) {
		t.Error("entry older than TTL should be expired")
	}

	cacheKey := channelsCacheKey("", false)
	fresh := ChannelListResponse{
		Channels: []ChannelResponse{{Hash: "fresh-sentinel", Name: "fresh-sentinel"}},
	}
	s.channelsCache.entries.Store(cacheKey, &channelsCacheEntry{resp: fresh, at: time.Now()})
	req := httptest.NewRequest("GET", "/api/channels", nil)
	w := httptest.NewRecorder()
	s.handleChannels(w, req)
	if !strings.Contains(w.Body.String(), "fresh-sentinel") {
		t.Fatalf("fresh cache should be served, body=%s", w.Body.String())
	}

	stale := ChannelListResponse{
		Channels: []ChannelResponse{{Hash: "stale-sentinel", Name: "stale-sentinel"}},
	}
	s.channelsCache.entries.Store(cacheKey, &channelsCacheEntry{resp: stale, at: time.Now().Add(-channelsCacheTTL - time.Second)})
	w2 := httptest.NewRecorder()
	s.handleChannels(w2, req)
	if strings.Contains(w2.Body.String(), "stale-sentinel") {
		t.Fatalf("stale cache must not be served, body=%s", w2.Body.String())
	}
}

func TestChannelsCacheEncryptedSecondCallDoesNotRefill(t *testing.T) {
	srv, router := setupTestServer(t)
	seedEncryptedChannelData(t, srv.db)

	var enc, total int
	if err := srv.db.SQL().QueryRowContext(t.Context(),
		`SELECT COUNT(*) FILTER (WHERE channel_encrypted), COUNT(*) FROM transmissions WHERE payload_type = 5`).Scan(&enc, &total); err != nil {
		t.Fatalf("count: %v", err)
	}
	t.Logf("encrypted=%d total=%d", enc, total)

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("GET", "/api/channels?includeEncrypted=true", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("call %d: status=%d body=%s", i+1, w.Code, w.Body.String())
		}
		var body ChannelListResponse
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("call %d: decode: %v", i+1, err)
		}
		foundEnc := false
		for _, ch := range body.Channels {
			if ch.Encrypted {
				foundEnc = true
				break
			}
		}
		if !foundEnc {
			t.Fatalf("call %d: expected encrypted channel in response, body=%s", i+1, w.Body.String())
		}
	}
	if got := srv.channelsCache.fillCount.Load(); got != 1 {
		t.Fatalf("expected 1 fill for 2 sequential encrypted requests, got %d", got)
	}
}

func TestChannelsCacheSingleflightCollapsesStampede(t *testing.T) {
	srv, router := setupTestServer(t)
	seedEncryptedChannelData(t, srv.db)

	const N = 50
	var wg sync.WaitGroup
	wg.Add(N)
	start := make(chan struct{})
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			<-start
			req := httptest.NewRequest("GET", "/api/channels?includeEncrypted=true", nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != 200 {
				t.Errorf("status=%d", w.Code)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := srv.channelsCache.fillCount.Load(); got != 1 {
		t.Fatalf("singleflight must collapse %d concurrent requests into 1 fill, got %d", N, got)
	}
}

func TestChannelsCacheKeysSeparateEncrypted(t *testing.T) {
	srv, router := setupTestServer(t)
	seedEncryptedChannelData(t, srv.db)

	reqPlain := httptest.NewRequest("GET", "/api/channels", nil)
	wPlain := httptest.NewRecorder()
	router.ServeHTTP(wPlain, reqPlain)
	if wPlain.Code != 200 {
		t.Fatalf("plain status=%d", wPlain.Code)
	}

	reqEnc := httptest.NewRequest("GET", "/api/channels?includeEncrypted=true", nil)
	wEnc := httptest.NewRecorder()
	router.ServeHTTP(wEnc, reqEnc)
	if wEnc.Code != 200 {
		t.Fatalf("enc status=%d", wEnc.Code)
	}

	if got := srv.channelsCache.fillCount.Load(); got != 2 {
		t.Fatalf("plain + encrypted shapes should each fill once, got %d", got)
	}

	var plain, enc ChannelListResponse
	_ = json.Unmarshal(wPlain.Body.Bytes(), &plain)
	_ = json.Unmarshal(wEnc.Body.Bytes(), &enc)
	if len(enc.Channels) <= len(plain.Channels) {
		t.Fatalf("encrypted response should include extra channels; plain=%d enc=%d",
			len(plain.Channels), len(enc.Channels))
	}
}

func TestChannelsCacheDoesNotCacheOnEncryptedQueryError(t *testing.T) {
	db := setupTestDB(t)
	seedTestData(t, db)
	cfg := &Config{Port: 3000}
	srv := NewServer(db, cfg, NewHub())
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	// Warm the query-level channel list cache so only the encrypted query
	// hits the database after the table is gone.
	if _, err := srv.getChannels(t.Context(), ""); err != nil {
		t.Fatalf("warm channels cache: %v", err)
	}
	mustExecDB(t, db, `DROP TABLE transmissions CASCADE`)

	req := httptest.NewRequest("GET", "/api/channels?includeEncrypted=true", nil)
	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 500 {
			t.Fatalf("call %d: want 500 got %d body=%s", i+1, w.Code, w.Body.String())
		}
	}
	if got := srv.channelsCache.fillCount.Load(); got != 0 {
		t.Fatalf("failed encrypted merge must not cache, fillCount=%d", got)
	}
}
