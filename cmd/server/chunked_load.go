package main

// Chunked startup load + early HTTP readiness for issue #1009.
//
// Design:
//   * LoadChunked paginates transmissions in id-ordered chunks of
//     `chunkSize` (default 10000 via Config.DBLoadChunkSize). After the
//     first chunk is merged into the store, FirstChunkReady is closed.
//     main.go binds the HTTP listener on that signal and serves
//     partial data while remaining chunks stream in the background.
//   * RunStartupLoad is the orchestrator: it runs LoadChunked
//     synchronously, then on success runs loadBackgroundChunks
//     synchronously so s.oldestLoaded is guaranteed set before the
//     background loader reads it (#1809). main.go typically invokes
//     RunStartupLoad inside its own goroutine and waits on
//     FirstChunkReady() in parallel to bind the HTTP listener.
//   * loadStatusMiddleware stamps X-CoreScope-Load-Status on every
//     response: "loading; progress=<rows>" until LoadComplete()
//     reports true, then "ready". Dashboards and probes can read the
//     header without parsing JSON.
//   * OnChunkLoaded registers a per-chunk callback for progress
//     logging / tests.
//
// Concurrency: each chunk acquires s.mu.Lock() ONLY while merging the
// chunk's rows into store-shared maps. SQLite reads run lock-free so
// HTTP handlers (which take s.mu.RLock) stay responsive.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/dbconfig"
	"math"
)

const firstChunkCursor int64 = math.MinInt64

// dbLoadConfig is the server-package alias for dbconfig.LoadConfig (#1009).
type dbLoadConfig = dbconfig.LoadConfig

// invariantViolation is the production-time handler for #1809-class
// invariant failures (see loadBackgroundChunks). In prod it logs a
// FATAL line and calls os.Exit(1) via log.Fatalf — a clean shutdown
// with no goroutine-stack noise. Tests override it to panic so they
// can recover() and assert the invariant message without crashing the
// test runner. adv #6 (PR #1811).
var invariantViolation = func(msg string) {
	log.Fatalf("invariant violation: %s", msg)
}

// DBLoadChunkSize returns the configured chunk size for chunked
// startup load (config: db.load.chunkSize), or 10000 default (#1009).
func (c *Config) DBLoadChunkSize() int {
	return c.DB.GetLoadChunkSize()
}

// chunkedLoadState holds the runtime gates for LoadChunked. It lives
// on PacketStore via embedded fields — see store.go additions in the
// same commit.

// FirstChunkReady returns a channel closed once the first chunk has
// been merged into the store, signalling the HTTP listener can bind.
func (s *PacketStore) FirstChunkReady() <-chan struct{} {
	s.chunkedLoadInit()
	return s.firstChunkReady
}

// LoadComplete reports whether LoadChunked has finished all chunks.
func (s *PacketStore) LoadComplete() bool {
	return s.loadComplete.Load()
}

// LoadProgress reports the number of transmission rows processed by
// the in-flight (or completed) LoadChunked call.
func (s *PacketStore) LoadProgress() int64 {
	return s.loadProgressRows.Load()
}

// OnChunkLoaded registers a callback fired once per chunk after that
// chunk has been merged into the store. The callback receives the
// number of transmission rows in that chunk and the running total.
// Multiple registrations chain.
func (s *PacketStore) OnChunkLoaded(fn func(rowsThisChunk, totalRows int)) {
	s.chunkedLoadInit()
	s.chunkCBMu.Lock()
	defer s.chunkCBMu.Unlock()
	s.chunkCallbacks = append(s.chunkCallbacks, fn)
}

// chunkedLoadInit lazily initialises the readiness channel + callback
// list under a mutex so concurrent first callers don't race.
func (s *PacketStore) chunkedLoadInit() {
	s.chunkInitOnce.Do(func() {
		s.firstChunkReady = make(chan struct{})
		s.startupLoadDone = make(chan struct{})
	})
}

// StartupLoadDone returns a channel closed once RunStartupLoad has
// returned: LoadChunked AND the background fill loader are finished,
// whether they succeeded or not. Nothing more is loaded from SQLite
// after it closes. LoadComplete() is not a substitute: it flips at the
// end of the hot window, before the background fill starts.
func (s *PacketStore) StartupLoadDone() <-chan struct{} {
	s.chunkedLoadInit()
	return s.startupLoadDone
}

func (s *PacketStore) signalStartupLoadDone() {
	s.chunkedLoadInit()
	if !s.startupLoadSignaled.CompareAndSwap(false, true) {
		return
	}
	// The analytics recomputes this signal triggers read these TTL
	// caches. Drop what was computed from the partial store so they see
	// the loaded data now rather than after the TTL.
	s.hashSizeInfoMu.Lock()
	s.hashSizeInfoCache = nil
	s.hashSizeInfoMu.Unlock()
	s.clockSkew.Invalidate()
	close(s.startupLoadDone)
}

func (s *PacketStore) signalFirstChunk() {
	if s.firstChunkSignaled.CompareAndSwap(false, true) {
		close(s.firstChunkReady)
	}
}

func (s *PacketStore) fireChunkCallbacks(rowsThisChunk, totalRows int) {
	s.chunkCBMu.Lock()
	cbs := append([]func(int, int){}, s.chunkCallbacks...)
	s.chunkCBMu.Unlock()
	for _, cb := range cbs {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[store] OnChunkLoaded callback panic: %v", r)
				}
			}()
			cb(rowsThisChunk, totalRows)
		}()
	}
}

// RunStartupLoad orchestrates the startup load sequence:
//  1. run LoadChunked synchronously (FirstChunkReady is signaled
//     internally so callers waiting on it in parallel can bind HTTP)
//  2. on success, run loadBackgroundChunks synchronously so
//     s.oldestLoaded is guaranteed set before the bg loader reads
//     it (#1809).
//
// chunkSize=0 uses the LoadChunked default. The function blocks until
// LoadChunked AND any background loader have finished. Callers that
// want to bind the HTTP listener at FirstChunkReady should run this
// in a goroutine and wait on FirstChunkReady() themselves.
//
// SYNCHRONOUS-CALL DEADLOCK WARNING (adv #10): callers that block on
// FirstChunkReady() from the SAME goroutine that calls RunStartupLoad
// will deadlock — LoadChunked only closes firstChunkReady from within
// this call, and RunStartupLoad does not return until ALL chunks
// (including the background loader) have finished. main.go's pattern
// is correct: spawn RunStartupLoad in a goroutine and wait on
// FirstChunkReady() on the parent stack. Do not call RunStartupLoad
// inline.
//
// SINGLE-CALL INVARIANT (adv #7): RunStartupLoad is invoked exactly
// once per process from main.go boot. Tests open a fresh store per
// test. Re-entry would observe a stale backgroundLoadErr from a
// previous failed call until the new LoadChunked or
// loadBackgroundChunks rewrote it. We clear backgroundLoadErr on
// entry to make repeat invocations in tests safe; a panic on
// reentrant concurrent calls is not added because the production
// invariant (one call per boot) is already documented above
// LoadChunked.
//
// Steady-state contracts post-return:
//   - StartupLoadDone() is closed, on every path below.
//   - LoadChunked error: backgroundLoadFailed=true, backgroundLoadDone
//     is also set true (terminal observable state — see dij #1).
//     backgroundLoadErr non-empty. Returns the error.
//   - hotStartupHours == 0: backgroundLoadDone=true, failed=false
//     (no background work was needed). Coverage is computed against
//     row count so /api/perf does not report ratio=0.0 (dij #2).
//   - hotStartupHours > 0 success: terminal state is whatever
//     loadBackgroundChunks set (done=true on full coverage,
//     failed=true on partial / chunk errors — see #1690).
//
// Issue #1809 root cause: previously main.go spawned loadBackgroundChunks
// at FirstChunkReady while LoadChunked was still merging the remainder
// of the hot window. s.oldestLoaded is only assigned at the end of
// LoadChunked, so the bg loader read "" and bailed → coverage gate
// trips → backgroundLoadFailed=true. Running the bg loader after
// LoadChunked returns preserves the FirstChunkReady HTTP-bind
// parallelism while ensuring oldestLoaded has a valid floor when the
// bg loader starts.
func (s *PacketStore) RunStartupLoad(ctx context.Context, chunkSize int) error {
	defer s.signalStartupLoadDone()
	// Clear any stale error from a previous invocation (single-call
	// invariant — see godoc above). Production never re-enters but
	// test fixtures may construct fresh stores that share no state;
	// keeping this explicit avoids surprising "ghost error" reads
	// across test runs that share a store.
	s.bgErrMu.Lock()
	s.backgroundLoadErr = ""
	s.bgErrMu.Unlock()

	if err := s.LoadChunked(ctx, chunkSize); err != nil {
		// Pick a terminal state on the error path so /api/healthz +
		// backgroundLoadComplete don't stay undefined forever. Both
		// done AND failed are set true: the LoadChunked read-protocol
		// contract documented in store.go says `done` is the primary
		// observable terminal signal and `failed` qualifies it (read
		// done first, then check failed). See dij #1.
		s.bgErrMu.Lock()
		s.backgroundLoadErr = fmt.Sprintf("LoadChunked failed: %v", err)
		s.bgErrMu.Unlock()
		s.backgroundLoadFailed.Store(true)
		s.backgroundLoadDone.Store(true)
		return err
	}
	if s.hotStartupHours <= 0 {
		// No bg work required → terminal steady state is done=true,
		// failed=false. Without this the healthz probe would see
		// backgroundLoadComplete=false indefinitely. dij #2: compute
		// coverage so /api/perf doesn't report ratio=0 on the
		// "no-work" path — empty DB is 1.0, otherwise everything in
		// the DB is what we already loaded into memory.
		s.mu.RLock()
		loadedCount := int64(len(s.packets))
		s.mu.RUnlock()
		var totalInDB int64
		cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		n, err := s.db.CountTransmissions(cctx)
		cancel()
		if err != nil {
			log.Printf("[store] RunStartupLoad: count transmissions failed: %v", err)
			totalInDB = -1
		} else {
			totalInDB = n
		}
		var ratio float64
		switch {
		case totalInDB <= 0:
			// Empty DB (0) or count failed (-1) → treat as fully covered.
			ratio = 1.0
		case loadedCount >= totalInDB:
			ratio = 1.0
		default:
			ratio = float64(loadedCount) / float64(totalInDB)
		}
		s.bgErrMu.Lock()
		s.loadCoverageRatio = ratio
		s.bgErrMu.Unlock()
		log.Printf("[store] RunStartupLoad: hotStartupHours=0 — background fill loader SKIPPED (coverage=%.1f%%, loaded=%d, totalInDB=%d)",
			ratio*100, loadedCount, totalInDB)
		s.backgroundLoadDone.Store(true)
		s.backgroundLoadProgress.Store(100)
		return nil
	}
	// INFO signal between LoadChunked completion and the bg loader
	// kick-off. The post-mortem of #1809 needed exactly this line to
	// confirm the bg loader actually started after oldestLoaded was set.
	log.Printf("[store] LoadChunked complete (oldestLoaded=%q) — starting background fill loader (retentionHours=%gh, hotStartupHours=%gh)",
		s.oldestLoaded, s.retentionHours, s.hotStartupHours)
	s.loadBackgroundChunks(ctx)
	return nil
}

// LoadChunked streams transmissions + observations from SQLite into
// the in-memory store in id-ordered chunks of `chunkSize` rows. Pass
// 0 to use the default (10000).
//
// After the first chunk is merged, FirstChunkReady is closed and the
// HTTP listener may bind. Remaining chunks stream while handlers run
// against partially-populated data; loadStatusMiddleware advertises
// loading status until LoadComplete() returns true.
//
// Re-entrancy: LoadChunked is NOT safe to call concurrently with
// itself on the same PacketStore — it resets loadComplete /
// loadProgressRows and mutates store-shared maps under s.mu. In
// production it is invoked exactly once from main.go boot. Tests that
// open a fresh store per test are also safe. If a future caller needs
// repeat or concurrent loads, add a top-level mutex first.
func (s *PacketStore) LoadChunked(ctx context.Context, chunkSize int) error {
	if chunkSize <= 0 {
		chunkSize = 10000
	}
	s.chunkedLoadInit()
	// Reset state for repeat calls in tests.
	s.loadComplete.Store(false)
	s.loadProgressRows.Store(0)

	// On any return — error OR success — unblock listeners that gate on
	// the readiness signal so an empty/failed DB does not deadlock the
	// caller. Note: loadComplete is set on the success path only (see
	// the end of this function) so probes do NOT see ready=true after a
	// failed load.
	defer s.signalFirstChunk()

	// Startup-ordering invariant (PR #1643 R1 munger #2). Mirror the
	// guard in Load() so the production async path also fast-fails when
	// neighbor_edges has rows but the graph is missing. See Load() for
	// the full rationale.
	hasEdges, err := s.db.HasNeighborEdges(ctx)
	if err != nil {
		return fmt.Errorf("check neighbor_edges: %w", err)
	}
	if hasEdges && s.graph.Load() == nil {
		panic("packet store LoadChunked(): neighbor_edges table has rows but s.graph is nil — graph must be loaded before packet load (see main.go #1643 invariant)")
	}

	t0 := time.Now()

	// One cutoff, used for the query window, the count and oldestLoaded
	// (PR #1187 r2 #7: two time.Now() calls skewed at chunk borders).
	hotCutoffHours := s.retentionHours
	if s.hotStartupHours > 0 {
		hotCutoffHours = s.hotStartupHours
	}
	var since *time.Time
	if hotCutoffHours > 0 {
		// #1690: filter on last_seen (effective recency, first_seen when not
		// yet backfilled), not first_seen, so long-lived hashes with recent
		// traffic load on cold start.
		cutoff := time.Now().UTC().Add(-time.Duration(hotCutoffHours * float64(time.Hour)))
		since = &cutoff
	}

	// COUNT honours the same retention/hot-startup filter the chunk loop
	// applies, so the logged "DB total" matches the rows the loop will
	// actually walk. Log-only: on failure it stays -1.
	totalInDB := int64(-1)
	if n, err := s.db.CountTransmissionsSince(ctx, since); err != nil {
		log.Printf("[store] LoadChunked: count failed: %v", err)
	} else {
		totalInDB = n
	}

	// Memory cap honoured by clamping the maximum cursor walk.
	var maxPackets int64
	if s.maxMemoryMB > 0 {
		avgBytes := int64(1000)
		if sample := estimateStoreTxBytesTypical(10); sample > avgBytes {
			avgBytes = sample
		}
		maxPackets = (int64(s.maxMemoryMB) * 1048576) / avgBytes
		if maxPackets < 1000 {
			maxPackets = 1000
		}
	}

	chunkIdx := 0
	totalLoaded := 0
	// Start below every id so the first chunk includes id=0 (issue #1486
	// fixture seeds the grouped-packet row with id=0).
	var cursorID int64 = firstChunkCursor

	if maxPackets > 0 {
		nthID, err := s.db.NthNewestTransmissionID(ctx, since, maxPackets)
		switch {
		case err == nil:
			cursorID = nthID - 1
		case errors.Is(err, database.ErrNotFound):
		default:
			return fmt.Errorf("find newest-window start: %w", err)
		}
	}

	// Relay-hop fallback input, fetched ONCE before the chunk loop. Hops the
	// ingestor persisted (observation_path_hops.resolved_pubkey) are indexed
	// directly; for observations with none, mergeChunk re-resolves relay hops
	// from the hop prefixes using this snapshot.
	// PR #1643 R1 munger #1: cold load uses the unique_prefix-only gate, so
	// the neighbor graph is not consulted here (affinity-tier resolution
	// against <=168h-old observations would silently mis-attribute).
	s.mu.RLock()
	_, relayPM := s.getCachedNodesAndPM()
	s.mu.RUnlock()
	var coldLoadAmbiguousHopsSkipped int

	for {
		// Phase 1, no lock: read the chunk.
		txs, err := s.db.ListTransmissionsChunk(ctx, database.ChunkQuery{
			AfterID: cursorID,
			Since:   since,
			Limit:   chunkSize,
		})
		if err != nil {
			return fmt.Errorf("chunk %d: transmissions: %w", chunkIdx, err)
		}
		if len(txs) == 0 {
			break
		}
		ids := make([]int64, len(txs))
		for i, t := range txs {
			ids[i] = t.ID
		}
		obsByTx, err := s.db.ListObservationsForTransmissions(ctx, ids)
		if err != nil {
			return fmt.Errorf("chunk %d: observations: %w", chunkIdx, err)
		}

		// Phase 2, lock: merge it. No database call under s.mu.
		s.mergeChunk(txs, obsByTx, relayPM, &coldLoadAmbiguousHopsSkipped)

		cursorID = txs[len(txs)-1].ID
		totalLoaded += len(txs)
		chunkIdx++
		s.loadProgressRows.Store(int64(totalLoaded))
		s.signalFirstChunk()
		s.fireChunkCallbacks(len(txs), totalLoaded)

		if maxPackets > 0 && int64(totalLoaded) >= maxPackets {
			break
		}
		if len(txs) < chunkSize {
			break
		}
	}

	// Post-load: pick best observation, build indexes — same shape as
	// legacy Load().
	s.mu.Lock()
	for _, tx := range s.packets {
		pickBestObservation(tx)
		s.trackedBytes += rechargeTx(tx)
		s.indexByNode(tx)
	}
	// Restore the "s.packets sorted oldest-first by FirstSeen" invariant
	// that legacy Load() got for free from "ORDER BY t.first_seen ASC".
	// LoadChunked walks chunks in id-ASC order so the slice ends up
	// id-ordered, which only equals first_seen-ordered when ids and
	// timestamps are correlated. After tools/freshen-fixture.sh (or any
	// real-world out-of-order ingest) they're not, leaving
	// s.packets[0].FirstSeen pointing at the newest row — which then
	// poisons oldestLoaded below and routes legitimate in-memory queries
	// to the SQL fallback. GetTimestamps (store.go) and QueryPackets
	// both rely on this invariant. See PR #1596 / mobile e2e regression.
	sort.SliceStable(s.packets, func(i, j int) bool {
		return s.packets[i].FirstSeen.Before(s.packets[j].FirstSeen)
	})
	s.buildSubpathIndex()
	s.buildPathHopIndex()
	s.buildDistanceIndex()
	if s.hotStartupHours > 0 && since != nil {
		s.oldestLoaded = *since
	} else if len(s.packets) > 0 {
		s.oldestLoaded = s.packets[0].FirstSeen
	}
	s.loaded = true
	s.mu.Unlock()

	// #1009 / PR #1596: flip the subpath + pathHop ready flags now that
	// the chunk loader has built both indexes synchronously above.
	// Without this, WaitIndexesReady (used by
	// StartRepeaterEnrichmentRecomputer at boot) blocks for up to
	// repeaterEnrichmentPrewarmWait (60s), delaying HTTP listener bind
	// past CI's 30s /api/healthz deadline.
	s.markIndexesReadySync()

	elapsed := time.Since(t0)
	log.Printf("[store] LoadChunked: %d transmissions (%d observations) across %d chunk(s) in %v (chunkSize=%d, DB total=%d)",
		totalLoaded, s.totalObs, chunkIdx, elapsed, chunkSize, totalInDB)
	if coldLoadAmbiguousHopsSkipped > 0 {
		log.Printf("[store] LoadChunked: skipped %d ambiguous-prefix relay hops (unique_prefix gate, PR #1643 R1)",
			coldLoadAmbiguousHopsSkipped)
	}
	s.loadMultibyteCapFromDB(ctx)
	// Mark complete on the success path only — see the function-level
	// defer above for why this is NOT in a deferred call. Probes that
	// read LoadComplete()==true after a failed load would otherwise
	// see ready=true for a half-loaded store.
	s.loadComplete.Store(true)
	return nil
}

// mergeChunk merges one chunk into the store under s.mu.Lock. It makes no
// database calls.
func (s *PacketStore) mergeChunk(txs []database.LoadedTransmission, obsByTx map[int64][]database.LoadedObservation, relayPM *prefixMap, coldLoadAmbiguousHopsSkipped *int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Scratch map for indexResolvedPathHops, reused across observations
	// (cleared inside the helper; see #1558).
	hopsSeen := make(map[string]bool)

	for _, lt := range txs {
		txID := int(lt.ID)
		tx := s.byHash[lt.Hash]
		if tx == nil {
			tx = newStoreTx(lt)
			s.byHash[lt.Hash] = tx
			s.packets = append(s.packets, tx)
			s.byTxID[txID] = tx
			if txID > s.maxTxID {
				s.maxTxID = txID
			}
			s.indexByNode(tx)
			if tx.PayloadType != nil {
				pt := *tx.PayloadType
				s.byPayloadType[pt] = append(s.byPayloadType[pt], tx)
			}
			s.trackAdvertPubkey(tx)
			s.trackedBytes += rechargeTx(tx)
		}

		for _, lo := range obsByTx[lt.ID] {
			pj := pathJSON(lo.Path)
			dk := lo.ObserverID + "|" + pj
			if tx.obsKeys[dk] {
				continue
			}
			obs := newStoreObs(lo, txID, pj)
			oid := obs.ID

			// Decode-window: index the persisted resolved hops. Single point
			// of truth, see indexResolvedPathHops and #1558.
			if pks := resolvedPubkeys(lo.Path); len(pks) > 0 {
				s.indexResolvedPathHops(tx, pks, hopsSeen)
			} else if relayPM != nil && pj != "" && pj != "[]" {
				// Nothing persisted: reconstruct relay hops from the path.
				// byNode ONLY (#1352): the resolved-path and path-hop
				// indexes are cross-checked by handleNodePaths against the
				// persisted hops. unique_prefix gate (PR #1643 R1 munger #1).
				rp := resolvePathForObsColdLoad(pj, lo.ObserverID, tx, relayPM, coldLoadAmbiguousHopsSkipped)
				for _, pk := range extractResolvedPubkeys(rp) {
					s.addToByNode(tx, pk)
				}
			}

			tx.Observations = append(tx.Observations, obs)
			tx.obsKeys[dk] = true
			if obs.ObserverID != "" && !tx.observerSet[obs.ObserverID] {
				tx.observerSet[obs.ObserverID] = true
				tx.UniqueObserverCount++
			}
			tx.ObservationCount++
			if obs.Timestamp.After(tx.LatestSeen) {
				tx.LatestSeen = obs.Timestamp
			}

			s.byObsID[oid] = obs
			if oid > s.maxObsID {
				s.maxObsID = oid
			}
			if obs.ObserverID != "" {
				s.byObserver[obs.ObserverID] = append(s.byObserver[obs.ObserverID], obs)
			}
			s.totalObs++
			s.trackedBytes += estimateStoreObsBytes(obs)
		}
	}
}

func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// loadStatusMiddleware sets X-CoreScope-Load-Status on every response.
// While LoadChunked is in flight the header reports
// "loading; progress=<rows>"; after completion it reports "ready".
// The header is set BEFORE calling the next handler so probes can
// observe it on any response (including streaming bodies).
func loadStatusMiddleware(s *PacketStore, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s != nil && s.LoadComplete() {
			w.Header().Set("X-CoreScope-Load-Status", "ready")
		} else if s != nil {
			w.Header().Set("X-CoreScope-Load-Status",
				fmt.Sprintf("loading; progress=%d", s.LoadProgress()))
		} else {
			w.Header().Set("X-CoreScope-Load-Status", "loading")
		}
		next.ServeHTTP(w, r)
	})
}

// --- runtime state stitched into PacketStore via store_chunked.go ---

// Forward declarations of the new PacketStore fields used above. The
// actual struct fields live in store.go; placing them here as a
// reminder keeps the chunked-load surface easy to audit.
var _ = sync.Once{}
var _ atomic.Bool
