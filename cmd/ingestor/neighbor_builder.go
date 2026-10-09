package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/packetpath"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// NeighborEdgesBuilderInterval is how often the ingestor rescans observations
// and refreshes neighbor_edges. The server reads with the same 60s cadence
// (see cmd/server/neighbor_recomputer.go), so a 60s pulse keeps the snapshot
// fresh.
const NeighborEdgesBuilderInterval = 60 * time.Second

// neighborBuilderMaxBatch caps how many observation rows one build call scans
// (#1339). It bounds memory and per-tick time; a backlog drains over
// successive ticks or warm-up iterations, 50k rows at a time.
const neighborBuilderMaxBatch = 50000

// neighborBuilderSlowTickThreshold is the per-tick wallclock budget. Exceeding
// it is logged loudly so a regression of #1339 is noticed quickly.
const neighborBuilderSlowTickThreshold = 5 * time.Second

// neighborOriginatorTypes are the payload types whose sender is identified by
// a FULL public key on the wire, so originator-to-first-hop is a trustworthy
// edge. ANON_REQ qualifies (#1777): it carries the sender's full key, unlike
// REQ/RESP/PATH/TXT, which carry only a 1-byte hash (about 1 in 256 collision
// odds, not enough to seed an edge). Other types are deliberately excluded.
var neighborOriginatorTypes = []types.PayloadType{types.PayloadADVERT, types.PayloadANON_REQ}

func hasFullOriginator(pt types.PayloadType) bool {
	for _, t := range neighborOriginatorTypes {
		if pt == t {
			return true
		}
	}
	return false
}

// edgeRow is one edge candidate. (a, b) is already canonical-ordered (a <= b).
type edgeRow struct {
	a, b string
	ts   time.Time
}

// prefixIndex maps a hop prefix (lowercase) to all full pubkeys that start
// with it. Prefixes with more than one candidate are ambiguous and skipped.
type prefixIndex map[string][]string

// newPrefixIndex builds the index from a list of public keys, for every prefix
// length the firmware uses (1, 2, 3, 4, 6, 8 bytes = 2, 4, 6, 8, 12, 16 hex
// chars). Memory is O(nodes x prefix lengths).
func newPrefixIndex(keys []string) prefixIndex {
	idx := make(prefixIndex, len(keys)*2)
	for _, pk := range keys {
		pk = strings.ToLower(pk)
		for _, n := range []int{2, 4, 6, 8, 12, 16} {
			if len(pk) >= n {
				idx[pk[:n]] = append(idx[pk[:n]], pk)
			}
		}
	}
	return idx
}

// resolvePrefix returns the single resolved pubkey if exactly one candidate
// matches, otherwise ok=false (the conservative rule shared with the server's
// resolver).
func resolvePrefix(idx prefixIndex, hop string, trust *packetpath.TrustConfig) (string, bool) {
	// #1784: gate on prefix length before consulting the index. A hop hash
	// below the operator's trust threshold is not mapping evidence even when
	// it resolves to exactly one candidate today: uniqueness is relative to
	// the nodes we currently know about, so an unknown or newly joined
	// repeater sharing that prefix silently turns it into a wrong edge.
	// Gating here means every consumer of the resolver inherits the threshold.
	if !packetpath.MeetsPathTrust(len(hop)/2, trust) {
		return "", false
	}
	candidates := idx[strings.ToLower(hop)]
	if len(candidates) != 1 {
		return "", false
	}
	return candidates[0], true
}

// canonEdge orders the pair so a <= b (the schema convention).
func canonEdge(a, b string, ts time.Time) edgeRow {
	if a > b {
		a, b = b, a
	}
	return edgeRow{a, b, ts}
}

// extractEdges turns scanned observations into edge candidates:
//   - originator to first hop, for payload types that carry a full originator
//     key (ADVERT, ANON_REQ), and
//   - observer to last hop, for every type.
//
// Zero-hop observations (FirstHop == nil) mean the observer heard the
// originator directly, which yields an originator-to-observer edge. It is pure:
// no database, so it is tested with plain tables.
func extractEdges(rows []database.NeighborObservation, idx prefixIndex, trust *packetpath.TrustConfig) []edgeRow {
	var edges []edgeRow
	for _, r := range rows {
		fullOriginator := hasFullOriginator(r.PayloadType)

		fromNode := strings.ToLower(r.FromPubkey)
		if fromNode == "" {
			switch r.PayloadType {
			case types.PayloadADVERT:
				fromNode = strings.ToLower(extractPubkeyFromAdvertJSON(r.DecodedJSON))
			case types.PayloadANON_REQ:
				fromNode = strings.ToLower(extractPubkeyFromAnonReqJSON(r.DecodedJSON))
			}
		}
		observer := strings.ToLower(r.Observer)

		if r.FirstHop == nil {
			if fullOriginator && fromNode != "" && observer != "" && fromNode != observer {
				edges = append(edges, canonEdge(fromNode, observer, r.Timestamp))
			}
			continue
		}
		if fullOriginator && fromNode != "" {
			if resolved, ok := resolvePrefix(idx, *r.FirstHop, trust); ok && resolved != fromNode {
				edges = append(edges, canonEdge(fromNode, resolved, r.Timestamp))
			}
		}
		if observer != "" && r.LastHop != nil {
			if resolved, ok := resolvePrefix(idx, *r.LastHop, trust); ok && resolved != observer {
				edges = append(edges, canonEdge(observer, resolved, r.Timestamp))
			}
		}
	}
	return edges
}

// aggregateEdges collapses duplicate pairs into one row each, summing hits and
// keeping the newest timestamp. This is required, not an optimization: a
// single INSERT ... ON CONFLICT DO UPDATE cannot touch the same row twice.
func aggregateEdges(edges []edgeRow) []database.NeighborEdge {
	type key struct{ a, b string }
	byPair := make(map[key]*database.NeighborEdge, len(edges))
	for _, e := range edges {
		k := key{e.a, e.b}
		if x, ok := byPair[k]; ok {
			x.Hits++
			if e.ts.After(x.LastSeen) {
				x.LastSeen = e.ts
			}
			continue
		}
		byPair[k] = &database.NeighborEdge{NodeA: e.a, NodeB: e.b, Hits: 1, LastSeen: e.ts}
	}
	out := make([]database.NeighborEdge, 0, len(byPair))
	for _, x := range byPair {
		out = append(out, *x)
	}
	return out
}

// neighborEdgeStore is the slice of the database layer the builder needs.
// Declared here so tests can supply a fake.
type neighborEdgeStore interface {
	ListNodePublicKeys(ctx context.Context) ([]string, error)
	LatestNeighborEdgeTime(ctx context.Context) (time.Time, error)
	ScanNeighborObservations(ctx context.Context, since time.Time, limit int32, jsonTypes []types.PayloadType) ([]database.NeighborObservation, error)
	UpsertNeighborEdges(ctx context.Context, edges []database.NeighborEdge) (int64, error)
}

// neighborBuilder periodically derives neighbor_edges from observations. It is
// the only writer to neighbor_edges (#1287).
type neighborBuilder struct {
	db    neighborEdgeStore
	trust *packetpath.TrustConfig

	// Snapshots shared with the ingest path (#1547, #1560): the prefix index is
	// refreshed before a build and the graph after, so new nodes and edges are
	// visible to the resolver within one tick.
	refreshPrefixIndex func(context.Context) error
	refreshGraph       func(context.Context) error

	// since is the in-memory watermark: observations at or before it have been
	// processed. Zero means a full warm-up scan. WarmUp seeds it from the newest
	// neighbor_edges row, so a restart resumes from what the table reflects.
	//
	// Trade-off, inherited from the SQLite version: an observation that arrives
	// with a timestamp already behind the watermark is skipped, and rows that
	// tie the last scanned timestamp at a batch boundary are too. Acceptable for
	// an approximate neighbor graph. A periodic full rebuild can close it later.
	since    time.Time
	maxBatch int // 0 means neighborBuilderMaxBatch; tests set it small
}

func (b *neighborBuilder) batchSize() int {
	if b.maxBatch > 0 {
		return b.maxBatch
	}
	return neighborBuilderMaxBatch
}

// build processes one batch. It returns how many observations it SCANNED
// (which decides whether a backlog remains) and how many edge pairs it upserted.
func (b *neighborBuilder) build(ctx context.Context) (scanned, edges int, err error) {
	keys, err := b.db.ListNodePublicKeys(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("list nodes: %w", err)
	}
	rows, err := b.db.ScanNeighborObservations(ctx, b.since, int32(b.batchSize()), neighborOriginatorTypes)
	if err != nil {
		return 0, 0, fmt.Errorf("scan observations: %w", err)
	}
	if len(rows) == 0 {
		return 0, 0, nil
	}

	agg := aggregateEdges(extractEdges(rows, newPrefixIndex(keys), b.trust))
	if len(agg) > 0 {
		if _, err := b.db.UpsertNeighborEdges(ctx, agg); err != nil {
			return len(rows), 0, fmt.Errorf("upsert edges: %w", err)
		}
	}
	// Advance past everything scanned, including rows that produced no edge, so
	// a batch of edge-less rows can't be rescanned forever.
	b.since = rows[len(rows)-1].Timestamp
	return len(rows), len(agg), nil
}

// WarmUp runs synchronously before Run, so the server's first load sees a
// populated table instead of an empty one. It loops until a short batch proves
// the backlog is drained.
func (b *neighborBuilder) WarmUp(ctx context.Context) {
	start := time.Now()

	since, err := b.db.LatestNeighborEdgeTime(ctx) // zero time when the table is empty
	if err != nil {
		log.Printf("[neighbor-build] initial watermark error: %v", err)
		return
	}
	b.since = since

	// Prime the prefix index (#1547) and neighbor graph (#1560) so the very
	// first InsertTransmission after startup can resolve hop prefixes.
	if err := b.refreshPrefixIndex(ctx); err != nil {
		log.Printf("[neighbor-build] initial prefix-index refresh error: %v", err)
	}
	if err := b.refreshGraph(ctx); err != nil {
		log.Printf("[neighbor-build] initial neighbor-graph refresh error: %v", err)
	}

	var total int
	for {
		scanned, edges, err := b.build(ctx)
		if err != nil {
			log.Printf("[neighbor-build] initial build error: %v", err)
			break
		}
		total += edges
		if scanned < b.batchSize() {
			break
		}
	}
	log.Printf("[neighbor-build] initial build: %d edges upserted in %s", total, time.Since(start))
}

// Run ticks until ctx is cancelled.
func (b *neighborBuilder) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = NeighborEdgesBuilderInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			start := time.Now()
			// Refresh the prefix index alongside the edges build (#1547) so new
			// nodes become resolvable within a tick.
			if err := b.refreshPrefixIndex(ctx); err != nil {
				log.Printf("[neighbor-build] prefix-index refresh error: %v", err)
			}
			_, edges, err := b.build(ctx)
			// Refresh the graph snapshot after the build (#1560) so the
			// context-aware resolver sees newly persisted adjacencies.
			if gerr := b.refreshGraph(ctx); gerr != nil {
				log.Printf("[neighbor-build] neighbor-graph refresh error: %v", gerr)
			}
			dur := time.Since(start)
			switch {
			case err != nil:
				log.Printf("[neighbor-build] tick error after %s: %v", dur, err)
			case edges > 0:
				log.Printf("[neighbor-build] tick: %d edges in %s", edges, dur)
			}
			if dur > neighborBuilderSlowTickThreshold {
				log.Printf("[neighbor-build] SLOW tick: %s", dur)
			}
		}
	}
}

// startNeighborEdgesBuilder warms up synchronously (so the server's first load
// sees real data), starts the ticker, and returns a stop function.
func startNeighborEdgesBuilder(ctx context.Context, store *Store, interval time.Duration,
	trust *packetpath.TrustConfig) (stop func()) {
	nb := &neighborBuilder{
		db:                 store.db,
		trust:              trust,
		refreshPrefixIndex: store.RefreshPrefixIndex,
		refreshGraph:       store.RefreshNeighborGraph,
	}
	nb.WarmUp(ctx)

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		nb.Run(runCtx, interval)
	}()
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second): // same bound the old stop closure had
		}
	}
}

func (s *Store) RefreshPrefixIndex(ctx context.Context) error {
	keys, err := s.db.ListNodePublicKeys(ctx)
	if err != nil {
		return err
	}
	s.prefixIdx.store(newPrefixIndex(keys))
	return nil
}

// RefreshNeighborGraph loads the latest neighbor_edges snapshot and
// publishes it atomically. Called on startup and once per neighbor-
// edges builder tick (60s) alongside RefreshPrefixIndex.
func (s *Store) RefreshNeighborGraph(ctx context.Context) error {
	g, err := loadNeighborGraph(ctx, s.db)
	if err != nil {
		return err // the previous snapshot stays published
	}
	s.neighborGraph.store(g)
	return nil
}

func loadNeighborGraph(ctx context.Context, db *database.DB) (*NeighborGraph, error) {
	pairs, err := db.ListNeighborPairs(ctx)
	if err != nil {
		return nil, err
	}
	g := NewNeighborGraph()
	for _, p := range pairs {
		g.AddEdge(p.A, p.B)
	}
	return g, nil
}
