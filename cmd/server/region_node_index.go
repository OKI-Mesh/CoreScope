// Package main: region node index (CoreScope#214).
//
// A region-filtered node list needs the set of nodes that have advertised
// through an observer in the requested regions. Working that out per request
// joins every advert ever recorded against its observations, so the cost grows
// with history rather than with traffic, and GetNodes used to pay it twice per
// call (once for the count, once for the page).
//
// This keeps the answer in memory instead. One background pass builds the
// membership for every region at once; the loop rebuilds it on an interval.
// GetNodes reads the snapshot when there is one. Before the first build
// finishes, it falls back to the SQL subquery: run once per call, shared
// between concurrent callers for the same regions, and limited to one at a
// time so the fallback can never occupy more than one read connection.
//
// Same pattern as analyticsRecomputer (#1240), with one deliberate
// difference: the first build is asynchronous. It takes tens of seconds on a
// large database, which must not land inside startup or the deploy health
// window, so main starts the index only after readiness.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"
)

// regionNodeIndexInterval is how often the snapshot is rebuilt. A node first
// heard in a region can take up to this long to appear under that region's
// filter.
const regionNodeIndexInterval = 10 * time.Minute

// regionNodeSnapshot maps an observer region code, normalized the same way
// the region subquery compared it (UPPER(TRIM(iata))), to the public keys of
// nodes that have advertised through an observer in that region.
type regionNodeSnapshot struct {
	byRegion map[string]map[string]struct{}
	pairs    int
}

// pubkeys returns the union of the nodes for codes. Several codes mean OR,
// matching the subquery's IN (codes). Never nil, so it encodes as [] rather
// than null.
func (s *regionNodeSnapshot) pubkeys(codes []string) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0)
	for _, code := range codes {
		for pk := range s.byRegion[code] {
			if _, dup := seen[pk]; !dup {
				seen[pk] = struct{}{}
				out = append(out, pk)
			}
		}
	}
	return out
}

// regionJoinCond is the observer join the region subquery has always used for
// each schema version.
func (db *DB) regionJoinCond() string {
	if db.isV3 {
		return "obs.rowid = o.observer_idx"
	}
	return "obs.id = o.observer_id"
}

// buildRegionNodeSnapshot runs the region subquery's join once with no region
// filter, collecting every (region, node) pair. The walk over adverts is the
// same whether one region or all are wanted, so this costs about what a single
// region-filtered request used to.
func (db *DB) buildRegionNodeSnapshot(ctx context.Context) (*regionNodeSnapshot, error) {
	rows, err := db.conn.QueryContext(ctx, fmt.Sprintf(`
		SELECT DISTINCT UPPER(TRIM(obs.iata)), t.from_pubkey
		FROM transmissions t
		JOIN observations o ON o.transmission_id = t.id
		JOIN observers obs ON %s
		WHERE t.payload_type = 4
		  AND t.from_pubkey IS NOT NULL
		  AND obs.iata IS NOT NULL`, db.regionJoinCond()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	snap := &regionNodeSnapshot{byRegion: make(map[string]map[string]struct{})}
	for rows.Next() {
		var region, pk string
		if err := rows.Scan(&region, &pk); err != nil {
			return nil, err
		}
		set := snap.byRegion[region]
		if set == nil {
			set = make(map[string]struct{})
			snap.byRegion[region] = set
		}
		set[pk] = struct{}{}
		snap.pairs++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return snap, nil
}

// queryRegionPubkeys is the region subquery as GetNodes ran it before the
// index, standalone. A NULL from_pubkey is skipped: inside IN (subquery) it
// could never match a node, so the result is unchanged.
func (db *DB) queryRegionPubkeys(codes []string) ([]string, error) {
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(codes)), ",")
	args := make([]interface{}, len(codes))
	for i, c := range codes {
		args[i] = c
	}
	// #1143: from_pubkey is a dedicated, indexed column populated at ingest
	// (and backfilled) for ADVERT rows specifically so pubkey lookups don't
	// need to JSON_EXTRACT + parse decoded_json per row.
	rows, err := db.conn.Query(fmt.Sprintf(`
		SELECT DISTINCT t.from_pubkey
		FROM transmissions t
		JOIN observations o ON o.transmission_id = t.id
		JOIN observers obs ON %s
		WHERE t.payload_type = 4
		AND UPPER(TRIM(obs.iata)) IN (%s)`, db.regionJoinCond(), placeholders), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]string, 0)
	for rows.Next() {
		var pk sql.NullString
		if err := rows.Scan(&pk); err != nil {
			return nil, err
		}
		if pk.Valid {
			out = append(out, pk.String)
		}
	}
	return out, rows.Err()
}

// regionPubkeys returns the public keys of nodes that have advertised through
// an observer in any of codes, from the snapshot when one exists and from the
// gated fallback query otherwise. codes must already be normalized.
func (db *DB) regionPubkeys(codes []string) ([]string, error) {
	if snap := db.regionSnap.Load(); snap != nil {
		return snap.pubkeys(codes), nil
	}
	v, err, shared := db.regionColdSF.Do(regionCodeSetKey(codes), func() (interface{}, error) {
		db.regionColdMu.Lock()
		defer db.regionColdMu.Unlock()
		// The first build may have finished while this caller waited.
		if snap := db.regionSnap.Load(); snap != nil {
			return snap.pubkeys(codes), nil
		}
		if db.regionColdHook != nil {
			db.regionColdHook()
		}
		return db.queryRegionPubkeys(codes)
	})
	if err != nil {
		return nil, err
	}
	keys := v.([]string)
	if shared {
		// Every caller that joined the flight holds the same slice; give each
		// its own so no caller can affect another's.
		keys = append([]string(nil), keys...)
	}
	return keys, nil
}

// regionCodeSetKey identifies a set of codes regardless of order or
// repetition, so "SJC,SFO", "SFO,SJC" and "SJC,SFO,SJC" share one fallback
// query.
func regionCodeSetKey(codes []string) string {
	set := append([]string(nil), codes...)
	sort.Strings(set)
	out := set[:0]
	for i, c := range set {
		if i == 0 || c != set[i-1] {
			out = append(out, c)
		}
	}
	return strings.Join(out, ",")
}

// regionNodeIndex rebuilds the DB's region snapshot in the background.
type regionNodeIndex struct {
	db       *DB
	interval time.Duration
	build    func(ctx context.Context) (*regionNodeSnapshot, error)

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu      sync.Mutex
	started bool
	stopped bool
}

// newRegionNodeIndex constructs an unstarted index that publishes into db.
func (db *DB) newRegionNodeIndex(interval time.Duration) *regionNodeIndex {
	if interval <= 0 {
		interval = regionNodeIndexInterval
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &regionNodeIndex{
		db:       db,
		interval: interval,
		build:    db.buildRegionNodeSnapshot,
		ctx:      ctx,
		cancel:   cancel,
		done:     make(chan struct{}),
	}
}

// Start builds the first snapshot in the background, then rebuilds on the
// interval. A no-op if already started or already stopped.
func (ix *regionNodeIndex) Start() {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if ix.started || ix.stopped {
		return
	}
	ix.started = true
	go ix.loop()
}

// Stop cancels any build in progress and waits for the loop to exit, so a
// caller can close the database afterwards. Safe to call more than once, and
// before Start.
func (ix *regionNodeIndex) Stop() {
	ix.mu.Lock()
	if ix.stopped {
		ix.mu.Unlock()
		return
	}
	ix.stopped = true
	started := ix.started
	ix.mu.Unlock()

	ix.cancel()
	if started {
		<-ix.done
	}
}

// loop builds, then waits for the next tick. A build never overlaps another:
// a slow build only delays the next one.
func (ix *regionNodeIndex) loop() {
	defer close(ix.done)
	t := time.NewTicker(ix.interval)
	defer t.Stop()
	for {
		ix.runOnce()
		select {
		case <-t.C:
		case <-ix.ctx.Done():
			return
		}
	}
}

// runOnce builds and publishes a snapshot. A failed or panicking build is
// logged and leaves the previous snapshot, or the fallback if there has never
// been one, in place.
func (ix *regionNodeIndex) runOnce() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[region-index] build panic recovered, keeping previous snapshot: %v", r)
		}
	}()
	t0 := time.Now()
	snap, err := ix.build(ix.ctx)
	if err != nil {
		if ix.ctx.Err() == nil {
			log.Printf("[region-index] build failed after %s, keeping previous snapshot: %v", time.Since(t0).Round(time.Millisecond), err)
		}
		return
	}
	ix.db.regionSnap.Store(snap)
	log.Printf("[region-index] built %d regions, %d node-region pairs in %s", len(snap.byRegion), snap.pairs, time.Since(t0).Round(time.Millisecond))
}
