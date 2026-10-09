package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func pathHash(hops []string) string {
	if len(hops) == 0 {
		return ""
	}
	h := sha256.Sum256([]byte(strings.Join(hops, ",")))
	return hex.EncodeToString(h[:])
}

// Store wraps the database for packet ingestion.
type Store struct {
	db             *database.DB
	snapshotDir    string
	pruneQueuePath string
	Stats          IngestStats

	sampleIntervalSec int
	backfillWg        sync.WaitGroup

	// prefixIdx holds the prefix → pubkey index used by the
	// resolved_path writer (#1547). Rebuilt on startup and once per
	// neighbor-edges builder tick (60s).
	prefixIdx prefixIdxHolder

	// neighborGraph holds the in-memory NeighborGraph snapshot used
	// by the context-aware resolver (#1560). Rebuilt on startup and
	// once per neighbor-edges builder tick (60s).
	neighborGraph neighborGraphHolder

	// relayTouched is the debounce map for touchRelayNodesLocked:
	// pubkey -> rxTime of the last last_seen write (#1598). Guarded by
	// writerMu, which InsertTransmission holds for its whole body.
	relayTouched map[string]time.Time
}

// relayTouchDebounce is the minimum interval between two last_seen writes
// for the same relay node. A backbone repeater appears in thousands of
// paths per hour; without this the ingest path would issue one UPDATE per
// observation for no added freshness.
const relayTouchDebounce = 5 * time.Minute

// relayTouchedMaxEntries caps the debounce map. One entry per node ever
// seen relaying — ~10^3-10^4 on real deployments — but a long-lived
// process on a large mesh should not grow it without bound. On overflow
// we drop entries older than two debounce windows, which can only cause
// an extra UPDATE, never a missed one.
const relayTouchedMaxEntries = 50000

// OpenStore opens or creates a SQLite DB at the given path, applying the
// v3 schema that is compatible with the Node.js server.
func OpenStore(ctx context.Context, connStr string) (*Store, error) {
	return OpenStoreWithInterval(ctx, connStr, 300)
}

// OpenStoreWithInterval opens or creates a SQLite DB with a configurable sample interval.
func OpenStoreWithInterval(ctx context.Context, connStr string, sampleIntervalSec int) (*Store, error) {
	db, err := database.OpenReadWrite(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	s := &Store{db: db, sampleIntervalSec: sampleIntervalSec}
	// Seed the in-process scope-match counters from the persisted running
	// total so a restart doesn't zero the ratio. Synchronous: it's one row.
	if err := s.LoadScopeMatchTotals(ctx); err != nil {
		log.Printf("[scope-match] failed to restore totals: %v", err)
	}

	return s, nil
}

// Close checkpoints the WAL and closes the database.
func (s *Store) Close() error {
	s.backfillWg.Wait()
	s.db.Close()
	return nil
}

// RoundToInterval rounds a time to the nearest sample interval boundary.
func RoundToInterval(t time.Time, intervalSec int) time.Time {
	if intervalSec <= 0 {
		intervalSec = 300
	}
	epoch := t.Unix()
	half := int64(intervalSec) / 2
	rounded := ((epoch + half) / int64(intervalSec)) * int64(intervalSec)
	return time.Unix(rounded, 0).UTC()
}

// MetricsData holds the fields to insert into observer_metrics.
type MetricsData struct {
	ObserverID  string
	NoiseFloor  *float64
	TxAirSecs   *int
	RxAirSecs   *int
	RecvErrors  *int
	BatteryMv   *int
	PacketsSent *int
	PacketsRecv *int
}

// TODO: MOVE TO ETL program
// backfillTransportCodes does the actual batched decode-and-update work,
// returning the number of rows updated. Uses keyset pagination on id rather
// than counting decoded items per page: a page can hit the LIMIT while
// containing undecodable rows, so "items returned < batchSize" does NOT mean
// "no rows remain" — that conflation previously caused the backfill to record
// its completion migration after silently truncating on the first page with
// an undecodable row. Termination is driven by how many rows the page
// actually scanned (seen), not by how many decoded successfully; lastID
// advances past undecodable rows too, so they are stepped over rather than
// re-selected forever.
//
// Invariant: the caller records the backfill_transport_codes_v1 guard row iff
// this returns a nil error, i.e. iff the loop ran to genuine exhaustion of all
// matching rows. Every error path below returns non-nil immediately, without
// finishing the run, so the guard is withheld and the next startup retries.
// func (s *Store) backfillTransportCodes(batchSize int) (int, error) {
// 	var total int
// 	var lastID int64
// 	for {
// 		rows, err := s.db.Query(`
// 			SELECT id, raw_hex FROM transmissions
// 			WHERE id > ? AND code1 IS NULL AND route_type IN (0, 3)
// 			ORDER BY id
// 			LIMIT ?`, lastID, batchSize)
// 		if err != nil {
// 			return total, fmt.Errorf("transport_codes select: %w", err)
// 		}
// 		type item struct {
// 			id           int64
// 			code1, code2 string
// 		}
// 		var items []item
// 		var seen int
// 		for rows.Next() {
// 			var id int64
// 			var raw string
// 			if err := rows.Scan(&id, &raw); err != nil {
// 				rows.Close()
// 				return total, fmt.Errorf("transport_codes scan: %w", err)
// 			}
// 			seen++
// 			lastID = id // advance past undecodable rows too, or they are re-selected forever
// 			decoded, err := DecodePacket(raw, nil, false)
// 			if err != nil || decoded.TransportCodes == nil {
// 				continue
// 			}
// 			items = append(items, item{id, decoded.TransportCodes.Code1, decoded.TransportCodes.Code2})
// 		}
// 		rows.Close()
// 		if seen == 0 {
// 			break // no rows left matching the filter
// 		}

// 		if len(items) > 0 {
// 			tx, err := s.db.Begin()
// 			if err != nil {
// 				return total, fmt.Errorf("transport_codes begin: %w", err)
// 			}
// 			stmt, err := tx.Prepare(`UPDATE transmissions SET code1 = ?, code2 = ? WHERE id = ?`)
// 			if err != nil {
// 				tx.Rollback()
// 				return total, fmt.Errorf("transport_codes prepare: %w", err)
// 			}
// 			for _, it := range items {
// 				if _, err := stmt.Exec(it.code1, it.code2, it.id); err != nil {
// 					stmt.Close()
// 					tx.Rollback()
// 					return total, fmt.Errorf("transport_codes update id=%d: %w", it.id, err)
// 				}
// 			}
// 			stmt.Close()
// 			if err := tx.Commit(); err != nil {
// 				return total, fmt.Errorf("transport_codes commit: %w", err)
// 			}
// 			total += len(items)
// 		}
// 		if seen < batchSize {
// 			break // last partial page
// 		}
// 	}
// 	return total, nil
// }

// LogStats logs current operational metrics.
func (s *Store) LogStats() {
	log.Printf("[stats] tx_inserted=%d tx_dupes=%d obs_inserted=%d node_upserts=%d observer_upserts=%d write_errors=%d sig_drops=%d",
		s.Stats.TransmissionsInserted.Load(),
		s.Stats.DuplicateTransmissions.Load(),
		s.Stats.ObservationsInserted.Load(),
		s.Stats.NodeUpserts.Load(),
		s.Stats.ObserverUpserts.Load(),
		s.Stats.WriteErrors.Load(),
		s.Stats.SignatureDrops.Load(),
	)
}

// compactRelayTouched drops debounce entries older than two windows.
// Caller must hold writerMu.
func (s *Store) compactRelayTouched(now time.Time) {
	cutoff := now.Add(-2 * relayTouchDebounce)
	for pk, t := range s.relayTouched {
		if t.Before(cutoff) {
			delete(s.relayTouched, pk)
		}
	}
}

// nilIfEmpty returns nil for empty strings (for nullable DB columns).
func nilIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

// boolToInt converts a bool to SQLite's 0/1 INTEGER representation.
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// scopeNameForDB encodes PacketData scope semantics for DB storage:
// non-transport-scoped → nil (SQL NULL); transport-scoped → pointer to ScopeName
// (may be "" for unknown region, "#name" for matched region).
func scopeNameForDB(data *PacketData) *string {
	if !data.IsTransportScoped {
		return nil
	}
	s := data.ScopeName
	return &s
}

// normalizeReportTS parses an observer report timestamp and returns it in
// canonical UTC RFC3339 form ("2006-01-02T15:04:05Z"). It accepts both the
// firmware's fractional/offset form (e.g. "2026-07-26T09:43:48.000000+00:00")
// and plain "Z"/offset variants. An empty or unparseable input returns "" so
// the caller writes an empty configured_scope_at and skips the ordering guard;
// this keeps every stored timestamp either canonical or empty, never a mix of
// offset/precision formats that would break lexicographic last-write-wins.
func normalizeReportTS(raw string) string {
	if raw == "" {
		return ""
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC().Format(time.RFC3339)
		}
	}
	return ""
}

// normalizeScopeList canonicalises the comma-separated region-scope string an
// observer reports for a node, so configured_scope is directly comparable with
// default_scope instead of only looking similar (#1865, raised by @cwichura).
//
// The OTA scope query returns bare region names ("dk,eu,dk-aarhus"), while
// everything else in CoreScope carries the leading "#": every stored
// default_scope value on a live 1425-node instance starts with one (361 of
// 361), and the hashRegions/hashChannels config paths already prefix a missing
// "#" before deriving the channel key from it (main.go). The "#" is therefore
// part of the region's name here, not decoration, so a bare "dk" and a stored
// "#dk" are the same region and must be written the same way.
//
// Rules, in order: entries are trimmed; empty entries are dropped; "*" is a
// wildcard rather than a region name and is passed through untouched; anything
// else gains a leading "#" if it lacks one. Order is preserved and duplicates
// are NOT collapsed, because the observer's ordering carries meaning we do not
// interpret here. Case is left alone: region keys are derived as
// sha256("#name"), so folding case would silently retarget a scope.
//
// An empty or all-empty input returns "", which UpdateNodeConfiguredScope
// stores as a valid "responded, no scopes configured" statement.
func normalizeScopeList(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if p != "*" && !strings.HasPrefix(p, "#") {
			p = "#" + p
		}
		out = append(out, p)
	}
	return strings.Join(out, ",")
}

// MQTTPacketMessage is the JSON payload from an MQTT raw packet message.
type MQTTPacketMessage struct {
	Raw       string   `json:"raw"`
	SNR       *float64 `json:"SNR"`
	RSSI      *float64 `json:"RSSI"`
	Score     *float64 `json:"score"`
	Direction *string  `json:"direction"`
	Origin    string   `json:"origin"`
	Region    string   `json:"region,omitempty"`    // optional region override (#788)
	Timestamp string   `json:"timestamp,omitempty"` // observer receive time, resolved by handler
}

// ─── Writer-lock instrumentation (issue #1340) ────────────────────────────
//
// Make SQLite writer-lock starvation visible to operators. Per-component
// wait_ms / hold_ms / contention_total histograms, surfaced via
// /api/perf/write-sources under the "writer_perf" key. Component tags:
// neighbor_builder, mqtt_handler, prune_packets, prune_observers,
// prune_metrics, mbcap_persist (deferred — see PR body), vacuum.
//
// The single writer connection (SetMaxOpenConns(1)) means writes serialise
// inside the driver and the wait is invisible to Go. writerMu measures the
// wait Go can see (everyone queueing behind the current holder) by gating
// every wrapped call site through the same package-level mutex.

// WriterStatsSnapshot is a per-component wait/hold latency snapshot
// surfaced via /api/perf to make SQLite writer-lock starvation visible
// to operators (issue #1340). Times are in milliseconds.
type WriterStatsSnapshot struct {
	Count           int64   `json:"count"`
	ContentionTotal int64   `json:"contention_total"`
	WaitMsP50       float64 `json:"wait_ms_p50"`
	WaitMsP95       float64 `json:"wait_ms_p95"`
	WaitMsP99       float64 `json:"wait_ms_p99"`
	WaitMsMax       float64 `json:"wait_ms_max"`
	HoldMsP50       float64 `json:"hold_ms_p50"`
	HoldMsP95       float64 `json:"hold_ms_p95"`
	HoldMsP99       float64 `json:"hold_ms_p99"`
	HoldMsMax       float64 `json:"hold_ms_max"`
}

const (
	// writerSampleWindow bounds the per-component rolling window so a
	// long-running ingestor doesn't grow this unbounded.
	writerSampleWindow = 1024
	// contentionThresholdMs: wait_ms above this counts as a "contended"
	// write (per #1340 spec).
	contentionThresholdMs = 100.0
	defaultSlowWriterMs   = 500.0
)

// slowWriterThresholdMsAtomic — hold_ms threshold above which writes
// emit a [db-slow-writer] log line. Read on the hot path; written once
// at startup by SetSlowWriterThresholdMs.
var slowWriterThresholdMsAtomic atomic.Uint64

// SetSlowWriterThresholdMs sets the [db-slow-writer] log threshold.
// ms<=0 restores the 500ms default. Operators can also set
// CORESCOPE_DB_SLOW_WRITER_MS at process start — see initSlowWriterFromEnv.
func SetSlowWriterThresholdMs(ms float64) {
	if ms <= 0 {
		ms = defaultSlowWriterMs
	}
	slowWriterThresholdMsAtomic.Store(uint64(ms))
}

func getSlowWriterThresholdMs() float64 {
	v := slowWriterThresholdMsAtomic.Load()
	if v == 0 {
		return defaultSlowWriterMs
	}
	return float64(v)
}

// initSlowWriterFromEnv is called once from package init so operators can
// override the threshold via CORESCOPE_DB_SLOW_WRITER_MS without a
// Go-side Config change.
func initSlowWriterFromEnv() {
	v := os.Getenv("CORESCOPE_DB_SLOW_WRITER_MS")
	if v == "" {
		return
	}
	var ms float64
	if _, err := fmt.Sscanf(v, "%f", &ms); err == nil && ms > 0 {
		SetSlowWriterThresholdMs(ms)
	}
}

func init() { initSlowWriterFromEnv() }

type writerComponentStats struct {
	mu              sync.Mutex
	count           int64
	contentionTotal int64
	waitMs          []float64
	holdMs          []float64
	waitMax         float64
	holdMax         float64
}

func (c *writerComponentStats) record(waitMs, holdMs float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.count++
	if waitMs > contentionThresholdMs {
		c.contentionTotal++
	}
	if waitMs > c.waitMax {
		c.waitMax = waitMs
	}
	if holdMs > c.holdMax {
		c.holdMax = holdMs
	}
	c.waitMs = appendBoundedFloat(c.waitMs, waitMs, writerSampleWindow)
	c.holdMs = appendBoundedFloat(c.holdMs, holdMs, writerSampleWindow)
}

func appendBoundedFloat(s []float64, v float64, max int) []float64 {
	if len(s) < max {
		return append(s, v)
	}
	copy(s, s[1:])
	s[len(s)-1] = v
	return s
}

func (c *writerComponentStats) snapshot() WriterStatsSnapshot {
	c.mu.Lock()
	wait := append([]float64(nil), c.waitMs...)
	hold := append([]float64(nil), c.holdMs...)
	snap := WriterStatsSnapshot{
		Count:           c.count,
		ContentionTotal: c.contentionTotal,
		WaitMsMax:       c.waitMax,
		HoldMsMax:       c.holdMax,
	}
	c.mu.Unlock()
	sort.Float64s(wait)
	sort.Float64s(hold)
	snap.WaitMsP50 = nearestRankPercentile(wait, 0.50)
	snap.WaitMsP95 = nearestRankPercentile(wait, 0.95)
	snap.WaitMsP99 = nearestRankPercentile(wait, 0.99)
	snap.HoldMsP50 = nearestRankPercentile(hold, 0.50)
	snap.HoldMsP95 = nearestRankPercentile(hold, 0.95)
	snap.HoldMsP99 = nearestRankPercentile(hold, 0.99)
	return snap
}

func nearestRankPercentile(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n == 1 {
		return sorted[0]
	}
	idx := int(p*float64(n-1) + 0.5)
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return sorted[idx]
}

type writerStatsAggregator struct {
	mu         sync.Mutex
	components map[string]*writerComponentStats
}

var writerStatsAgg = &writerStatsAggregator{
	components: make(map[string]*writerComponentStats),
}

func (a *writerStatsAggregator) get(component string) *writerComponentStats {
	a.mu.Lock()
	defer a.mu.Unlock()
	c, ok := a.components[component]
	if !ok {
		c = &writerComponentStats{}
		a.components[component] = c
	}
	return c
}

// reset clears all per-component samples. Test-only: lets a single
// scenario assert against a clean aggregator without prior-test noise
// in the same package run (TestWriterStarvationVisibleInPerf would
// otherwise mix this run's 5 starved samples with thousands of fast
// InsertTransmission samples from earlier tests and the p99 would
// collapse below the 50s threshold).
func (a *writerStatsAggregator) reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.components = make(map[string]*writerComponentStats)
}

// ResetWriterStatsForTest wipes the per-component writer stats
// aggregator. Test-only; not safe to call from production code paths.
func ResetWriterStatsForTest() { writerStatsAgg.reset() }

func (a *writerStatsAggregator) snapshot() map[string]WriterStatsSnapshot {
	a.mu.Lock()
	keys := make([]string, 0, len(a.components))
	stats := make([]*writerComponentStats, 0, len(a.components))
	for k, v := range a.components {
		keys = append(keys, k)
		stats = append(stats, v)
	}
	a.mu.Unlock()
	out := make(map[string]WriterStatsSnapshot, len(keys))
	for i, k := range keys {
		out[k] = stats[i].snapshot()
	}
	return out
}

// WriterStatsSnapshot returns a per-component wait/hold/contention
// snapshot for exposure on /api/perf/write-sources (issue #1340).
func (s *Store) WriterStatsSnapshot() map[string]WriterStatsSnapshot {
	return writerStatsAgg.snapshot()
}

// recordWriterTiming aggregates a single sample under component and
// emits [db-slow-writer] if hold_ms > configured threshold (default
// 500ms). queryForLog is truncated to 200 chars.
func recordWriterTiming(component string, wait, hold time.Duration, queryForLog string) {
	waitMs := float64(wait.Nanoseconds()) / 1e6
	holdMs := float64(hold.Nanoseconds()) / 1e6
	writerStatsAgg.get(component).record(waitMs, holdMs)
	if holdMs > getSlowWriterThresholdMs() {
		q := queryForLog
		if len(q) > 200 {
			q = q[:200]
		}
		log.Printf("[db-slow-writer] component=%s duration=%.1fms query=%s", component, holdMs, q)
	}
}
