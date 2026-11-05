package main

import (
	"encoding/json"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func sentinelChannels(name string) ChannelListResponse {
	return ChannelListResponse{Channels: []map[string]interface{}{{"hash": name, "name": name}}}
}

func getChannelsBody(t *testing.T, s *Server, query string) string {
	t.Helper()
	w := httptest.NewRecorder()
	s.handleChannels(w, httptest.NewRequest("GET", "/api/channels"+query, nil))
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	return w.Body.String()
}

// The point of #225: a copy kept warm by the background refresh is served past
// the old 90 s expiry, so no request ever waits for a cold rebuild.
func TestChannelsRefresh_BackgroundEntryServedPastTTL(t *testing.T) {
	s := &Server{}
	age := channelsCacheTTL + 3*time.Minute
	s.channelsCache.entries.Store(channelsCacheKey("", false),
		&channelsCacheEntry{resp: sentinelChannels("bg-sentinel"), at: time.Now().Add(-age), background: true})

	if body := getChannelsBody(t, s, ""); !strings.Contains(body, "bg-sentinel") {
		t.Fatalf("background entry %s old should be served, body=%s", age, body)
	}
	if got := s.channelsCache.fillCount.Load(); got != 0 {
		t.Fatalf("serving a background entry must not rebuild, fillCount=%d", got)
	}
}

// Entries filled on request keep today's rule: stale after the TTL.
func TestChannelsRefresh_RequestFilledEntryKeepsTTL(t *testing.T) {
	s := &Server{}
	s.channelsCache.entries.Store(channelsCacheKey("", false),
		&channelsCacheEntry{resp: sentinelChannels("req-sentinel"), at: time.Now().Add(-channelsCacheTTL - time.Second)})

	if body := getChannelsBody(t, s, ""); strings.Contains(body, "req-sentinel") {
		t.Fatalf("a request-filled entry past the TTL must not be served, body=%s", body)
	}
}

// A refresher that has stopped must not leave ancient data in place forever.
func TestChannelsRefresh_BackgroundEntryPastMaxAgeFallsBack(t *testing.T) {
	s := &Server{}
	s.channelsCache.entries.Store(channelsCacheKey("", true),
		&channelsCacheEntry{resp: sentinelChannels("old-bg"), at: time.Now().Add(-channelsRefreshMaxAge - time.Second), background: true})

	if body := getChannelsBody(t, s, "?includeEncrypted=true"); strings.Contains(body, "old-bg") {
		t.Fatalf("a background entry past the hard cap must not be served, body=%s", body)
	}
	if got := s.channelsCache.fillCount.Load(); got != 1 {
		t.Fatalf("past the cap the request path should rebuild once, fillCount=%d", got)
	}
}

// Region-filtered variants are untouched: never written by the refresh, and an
// entry for one still follows the TTL.
func TestChannelsRefresh_RegionVariantsUnchanged(t *testing.T) {
	srv, _ := setupTestServer(t)
	seedEncryptedChannelData(t, srv.db)

	r := srv.newChannelsRefresher(time.Hour)
	r.runOnce()

	var keys []string
	srv.channelsCache.entries.Range(func(k, _ interface{}) bool {
		keys = append(keys, k.(string))
		return true
	})
	want := map[string]bool{channelsCacheKey("", false): true, channelsCacheKey("", true): true}
	if len(keys) != len(want) {
		t.Fatalf("refresh wrote keys %v, want exactly the All Regions variants", keys)
	}
	for _, k := range keys {
		if !want[k] {
			t.Fatalf("refresh wrote unexpected key %q", k)
		}
	}

	key := channelsCacheKey("SJC", false)
	srv.channelsCache.entries.Store(key, &channelsCacheEntry{resp: sentinelChannels("region-old"), at: time.Now().Add(-channelsCacheTTL - time.Second)})
	if body := getChannelsBody(t, srv, "?region=SJC"); strings.Contains(body, "region-old") {
		t.Fatalf("an expired region entry must be rebuilt as today, body=%s", body)
	}
}

// What the refresh stores must equal what the current on-request path returns,
// for both variants, and requests must then be served without rebuilding.
func TestChannelsRefresh_StoresSameResultAsRequestPath(t *testing.T) {
	srv, _ := setupTestServer(t)
	seedEncryptedChannelData(t, srv.db)

	for _, enc := range []bool{false, true} {
		want, err := srv.fetchChannelsList("", enc)
		if err != nil {
			t.Fatal(err)
		}
		r := srv.newChannelsRefresher(time.Hour)
		r.runOnce()
		v, ok := srv.channelsCache.entries.Load(channelsCacheKey("", enc))
		if !ok {
			t.Fatalf("includeEncrypted=%v: no entry after refresh", enc)
		}
		e := v.(*channelsCacheEntry)
		if !e.background {
			t.Fatalf("includeEncrypted=%v: refreshed entry not marked background", enc)
		}
		if !reflect.DeepEqual(e.resp, want) {
			t.Fatalf("includeEncrypted=%v: refreshed result differs from request path\n got %+v\nwant %+v", enc, e.resp, want)
		}
	}

	before := srv.channelsCache.fillCount.Load()
	for _, q := range []string{"", "?includeEncrypted=true"} {
		var resp ChannelListResponse
		if err := json.Unmarshal([]byte(getChannelsBody(t, srv, q)), &resp); err != nil {
			t.Fatal(err)
		}
	}
	if got := srv.channelsCache.fillCount.Load(); got != before {
		t.Fatalf("requests after a refresh rebuilt %d time(s), want 0", got-before)
	}
}

func TestChannelsRefresh_FailedRefreshKeepsPreviousCopy(t *testing.T) {
	s := &Server{}
	r := s.newChannelsRefresher(time.Hour)
	calls := 0
	r.fetch = func(enc bool) (ChannelListResponse, error) {
		calls++
		switch {
		case calls <= 2:
			return sentinelChannels("good"), nil
		case calls <= 4:
			return ChannelListResponse{}, errors.New("simulated failure")
		default:
			panic("simulated panic")
		}
	}
	r.runOnce() // good, both variants
	at := map[bool]time.Time{}
	for _, enc := range []bool{false, true} {
		v, _ := s.channelsCache.entries.Load(channelsCacheKey("", enc))
		at[enc] = v.(*channelsCacheEntry).at
	}
	r.runOnce() // errors
	r.runOnce() // panics
	for _, enc := range []bool{false, true} {
		v, _ := s.channelsCache.entries.Load(channelsCacheKey("", enc))
		e := v.(*channelsCacheEntry)
		if !strings.Contains(e.resp.Channels[0]["name"].(string), "good") || !e.at.Equal(at[enc]) {
			t.Fatalf("includeEncrypted=%v: a failed refresh replaced the previous copy", enc)
		}
	}
}

// One variant failing must not stop the other from refreshing.
func TestChannelsRefresh_VariantsRefreshIndependently(t *testing.T) {
	s := &Server{}
	r := s.newChannelsRefresher(time.Hour)
	r.fetch = func(enc bool) (ChannelListResponse, error) {
		if !enc {
			panic("plain variant panics")
		}
		return sentinelChannels("enc-ok"), nil
	}
	r.runOnce()
	if _, ok := s.channelsCache.entries.Load(channelsCacheKey("", true)); !ok {
		t.Fatal("encrypted variant was not refreshed after the plain variant panicked")
	}
}

// Each pass must read current data. Without dropping the DB-level cache, a
// pass inside its 90 s TTL would re-store the previous result under a fresh
// timestamp and a new channel would stay invisible.
func TestChannelsRefresh_PassSeesDataAddedSinceLastPass(t *testing.T) {
	srv, _ := setupTestServer(t)
	seedEncryptedChannelData(t, srv.db)

	r := srv.newChannelsRefresher(time.Hour)
	r.runOnce()

	if _, err := srv.db.conn.Exec(`INSERT INTO transmissions (raw_hex, hash, first_seen, route_type, payload_type, decoded_json, channel_hash)
		VALUES ('FEED', 'refreshfresh0001', ?, 1, 5, '{"type":"CHAN","channel":"#refresh-new","text":"me: hello"}', '#refresh-new')`,
		time.Now().UTC().Format(time.RFC3339)); err != nil {
		t.Fatal(err)
	}

	r.runOnce()
	v, _ := srv.channelsCache.entries.Load(channelsCacheKey("", false))
	found := false
	for _, ch := range v.(*channelsCacheEntry).resp.Channels {
		if ch["hash"] == "#refresh-new" {
			found = true
		}
	}
	if !found {
		t.Fatal("a channel added after the previous pass is missing: the pass re-used the DB-level cache")
	}
}

// A rebuild cannot be cancelled mid-query, so Stop must not wait on it
// indefinitely.
func TestChannelsRefresh_StopIsBoundedWhileRebuildHangs(t *testing.T) {
	s := &Server{}
	r := s.newChannelsRefresher(time.Hour)
	r.stopWait = 100 * time.Millisecond
	entered := make(chan struct{})
	release := make(chan struct{})
	var once atomic.Bool
	r.fetch = func(bool) (ChannelListResponse, error) {
		if once.CompareAndSwap(false, true) {
			close(entered)
		}
		<-release
		return sentinelChannels("x"), nil
	}
	defer close(release)
	r.Start()
	<-entered
	done := make(chan struct{})
	go func() { r.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop waited on a hung rebuild past its bound")
	}
}

func TestChannelsRefresh_StartStop(t *testing.T) {
	t.Run("stop before start, then start is a no-op", func(t *testing.T) {
		s := &Server{}
		r := s.newChannelsRefresher(time.Hour)
		var calls atomic.Int32
		r.fetch = func(bool) (ChannelListResponse, error) { calls.Add(1); return sentinelChannels("x"), nil }
		r.Stop()
		r.Start()
		r.Stop()
		time.Sleep(50 * time.Millisecond)
		if calls.Load() != 0 {
			t.Fatal("a stopped refresher refreshed")
		}
	})

	t.Run("start refreshes immediately, stop waits for the loop", func(t *testing.T) {
		s := &Server{}
		r := s.newChannelsRefresher(time.Hour)
		r.fetch = func(bool) (ChannelListResponse, error) { return sentinelChannels("x"), nil }
		r.Start()
		deadline := time.Now().Add(5 * time.Second)
		for {
			if _, ok := s.channelsCache.entries.Load(channelsCacheKey("", true)); ok {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("no refresh within 5s of Start")
			}
			time.Sleep(10 * time.Millisecond)
		}
		done := make(chan struct{})
		go func() { r.Stop(); r.Stop(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("Stop did not return")
		}
	})
}
