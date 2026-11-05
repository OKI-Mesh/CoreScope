package main

// Background refresh of the All Regions channel list (#225, resolving #224).
//
// The channel list cache in channels_cache.go refills on the request that
// finds it expired. That refill reads channel history from a database far
// larger than the page cache, so a cold refill took 33 to 68 s on prod and
// could cross the server's 60 s write timeout, leaving the Channels page
// empty or failed.
//
// This loop rebuilds the variants the page requests with All Regions selected
// (region "", with and without encrypted channels) on a fixed interval and
// stores them marked as background copies. handleChannels serves a background
// copy at any age up to channelsRefreshMaxAge, so those requests never wait
// for a rebuild. Region-filtered variants are not refreshed here and keep the
// on-request behavior.
//
// main starts it just before the HTTP listener binds, after the startup
// warm-ups, so its first rebuild stays out of the startup window (#223).

import (
	"log"
	"sync"
	"time"
)

// channelsRefreshInterval is the gap between the end of one rebuild and the
// start of the next. Shorter than channelsCacheTTL so the refreshed copy is
// normally younger than an on-request copy would be.
const channelsRefreshInterval = 60 * time.Second

// channelsRefreshMaxAge caps how old a background copy may be and still be
// served. If the refresher stops or keeps failing, requests fall back to the
// on-request path rather than showing ancient data indefinitely.
const channelsRefreshMaxAge = 10 * time.Minute

// channelsRefreshStopWait bounds how long Stop waits for a rebuild in
// progress. A rebuild cannot be cancelled mid-query, and prod's container is
// killed 10 s after a stop signal anyway, so waiting longer would only hold
// up the rest of an orderly shutdown. Same bound as analyticsRecomputer.Stop.
const channelsRefreshStopWait = 5 * time.Second

// channelsRefreshVariants are the includeEncrypted values refreshed for
// region "".
var channelsRefreshVariants = []bool{false, true}

type channelsRefresher struct {
	s        *Server
	interval time.Duration
	stopWait time.Duration
	fetch    func(includeEncrypted bool) (ChannelListResponse, error)
	// beforeRun runs at the start of each rebuild pass. By default it drops
	// the DB-level channel caches for region "", so the pass reads current
	// data instead of re-storing a cached result under a fresh timestamp.
	beforeRun func()

	stop chan struct{}
	done chan struct{}

	mu      sync.Mutex
	started bool
	stopped bool
}

func (s *Server) newChannelsRefresher(interval time.Duration) *channelsRefresher {
	if interval <= 0 {
		interval = channelsRefreshInterval
	}
	return &channelsRefresher{
		s:        s,
		interval: interval,
		stopWait: channelsRefreshStopWait,
		fetch: func(includeEncrypted bool) (ChannelListResponse, error) {
			return s.fetchChannelsList("", includeEncrypted)
		},
		beforeRun: func() {
			if s.db != nil {
				s.db.dropChannelsCaches("")
			}
		},
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
}

// Start begins refreshing immediately, then every interval. A no-op if
// already started or already stopped.
func (r *channelsRefresher) Start() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started || r.stopped {
		return
	}
	r.started = true
	go r.loop()
}

// Stop ends the loop and waits up to stopWait for a rebuild in progress to
// return. Safe before Start and when repeated.
func (r *channelsRefresher) Stop() {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	started := r.started
	close(r.stop)
	r.mu.Unlock()

	if started {
		select {
		case <-r.done:
		case <-time.After(r.stopWait):
			log.Printf("[channels-refresh] stop: rebuild still running after %s, not waiting further", r.stopWait)
		}
	}
}

// loop rebuilds, then waits for the next tick, so rebuilds never overlap.
func (r *channelsRefresher) loop() {
	defer close(r.done)
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		r.runOnce()
		select {
		case <-t.C:
		case <-r.stop:
			return
		}
	}
}

// runOnce rebuilds each variant independently. A failed or panicking rebuild
// leaves that variant's previous copy in place.
func (r *channelsRefresher) runOnce() {
	if r.beforeRun != nil {
		r.beforeRun()
	}
	for _, enc := range channelsRefreshVariants {
		r.refreshVariant(enc)
	}
}

func (r *channelsRefresher) refreshVariant(includeEncrypted bool) {
	defer func() {
		if p := recover(); p != nil {
			log.Printf("[channels-refresh] includeEncrypted=%v panic recovered, keeping previous copy: %v", includeEncrypted, p)
		}
	}()
	t0 := time.Now()
	resp, err := r.fetch(includeEncrypted)
	if err != nil {
		log.Printf("[channels-refresh] includeEncrypted=%v failed after %s, keeping previous copy: %v", includeEncrypted, time.Since(t0).Round(time.Millisecond), err)
		return
	}
	r.s.channelsCache.entries.Store(channelsCacheKey("", includeEncrypted),
		&channelsCacheEntry{resp: resp, at: time.Now(), background: true})
	log.Printf("[channels-refresh] includeEncrypted=%v: %d channels in %s", includeEncrypted, len(resp.Channels), time.Since(t0).Round(time.Millisecond))
}
