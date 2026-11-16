package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// ── Clock Skew Constants ───────────────────────────────────────────────────────

var (
	skewMinAdvert = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	skewMaxAdvert = time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
)

const (
	// minDriftSamples is the minimum number of advert transmissions needed
	// to compute a meaningful linear drift rate.
	minDriftSamples = 5

	// maxReasonableDriftPerDay caps drift display. Physically impossible
	// drift rates (> 1 day/day) indicate insufficient or outlier samples.
	maxReasonableDriftPerDay = 86400.0

	// recentSkewWindowCount is the number of most-recent advert samples
	// used to derive the "current" skew for severity classification (see
	// issue #789). The all-time median is poisoned by historical bad
	// samples (e.g. a node that was off and then GPS-corrected); severity
	// must reflect current health, not lifetime statistics.
	recentSkewWindowCount = 5

	// recentSkewWindowSec bounds the recent-window in time as well: only
	// samples from the last N seconds count as "recent" for severity.
	// The effective window is min(recentSkewWindowCount, samples in 1h).
	recentSkewWindowSec = 3600

	// bimodalSkewThresholdSec is the absolute skew threshold (1 hour)
	// above which a sample is considered "bad" — likely firmware emitting
	// a nonsense timestamp from an uninitialized RTC, not real drift.
	// Chosen to match the warning/critical severity boundary: real clock
	// drift rarely exceeds 1 hour, while epoch-0 RTCs produce ~1.7B sec.
	bimodalSkewThresholdSec = 3600.0

	// rtcResetOutlierThresholdSec is the absolute skew above which a
	// sample is treated as obvious sensor garbage — an RTC-reset advert
	// where the firmware emitted its factory timestamp (typically off by
	// months/years). These samples are excluded from the recent-window
	// "good/bad" split (bug #1285 — single RTC-reset advert among 30
	// healthy adverts must not flip a node to bimodal_clock) and from the
	// per-hash evidence median (a 700-day median is not actionable for
	// operators). They remain in the raw sample stream and the RTC-reset
	// badge logic which surfaces them separately. 24h is a generous floor:
	// real drift is fractions of a sec/advert, real clock-skew tops out
	// in the hours range; anything above a day is structurally not a
	// drift signal.
	rtcResetOutlierThresholdSec = 24 * 3600.0

	// maxPlausibleSkewJumpSec is the largest skew change between
	// consecutive samples that we treat as physical drift. Anything larger
	// (e.g. a GPS sync that jumps the clock by minutes/days) is rejected
	// as an outlier when computing drift. Real microcontroller drift is
	// fractions of a second per advert; 60s is a generous safety factor.
	maxPlausibleSkewJumpSec = 60.0

	// theilSenMaxPoints caps the number of points fed to Theil-Sen
	// regression (O(n²) in pairs). For nodes with thousands of samples we
	// keep the most-recent points, which are also the most relevant for
	// current drift.
	theilSenMaxPoints = 200
)

// ── Data Types ─────────────────────────────────────────────────────────────────

// skewSample is a single raw skew measurement from one advert observation.
type skewSample struct {
	advertTime   time.Time // what the node's clock said (from the payload)
	observedTime time.Time // when the observer heard it (from the DB)
	observerID   string
	hash         string
}

func (s skewSample) rawSkewSec() float64 {
	return s.advertTime.Sub(s.observedTime).Seconds()
}

// ObserverCalibration holds the computed clock offset for an observer.
type ObserverCalibration struct {
	ObserverID string
	OffsetSec  float64 // positive = observer clock ahead
	Samples    int     // number of multi-observer packets used
}

// NodeClockSkew is the engine's clock skew result for a single node. It is
// not serialized: handlers convert it to api.NodeClockSkew, which fixes the
// wire names and formats.
type NodeClockSkew struct {
	Pubkey               string
	MeanSkewSec          float64 // corrected mean skew (positive = node ahead)
	MedianSkewSec        float64 // corrected median skew
	LastSkewSec          float64 // most recent corrected skew
	RecentMedianSkewSec  float64 // median across most-recent samples (drives severity, see #789)
	DriftPerDaySec       float64 // linear drift rate (sec/day)
	Severity             types.SkewSeverity
	SampleCount          int
	Calibrated           bool // true if observer calibration was applied
	LastAdvertTime       time.Time
	LastObservedTime     time.Time
	Samples              []SkewSample // time-series for sparklines
	GoodFraction         float64      // fraction of recent samples with |skew| <= 1h
	RecentBadSampleCount int          // count of recent samples with |skew| > 1h
	RecentBadSamples     []BadSample  // #1094: per-bad-sample evidence (hash + bad advertTime)
	RecentSampleCount    int          // total recent samples in window
	RecentHashEvidence   []HashEvidence
	CalibrationSummary   *CalibrationSummary
	NodeName             string         // populated in fleet results
	NodeRole             types.NodeRole // populated in fleet results
}

// SkewSample is a single (time, skew) point for sparkline rendering.
type SkewSample struct {
	Time    time.Time // when the observation was heard
	SkewSec float64   // corrected skew in seconds
}

// BadSample is a single recent advert flagged as having a nonsense timestamp
// (|corrected skew| in the bimodal-bad band — > 1h, <= 24h). #1094: surfaced
// so the UI can link each offender to its packet detail page.
type BadSample struct {
	Hash       string    // transmission hash for packet-detail deep-link
	AdvertTime time.Time // what the node's clock said in the offending advert
	SkewSec    float64   // corrected skew vs observer at observation time
}

// HashEvidenceObserver is one observer's contribution to a per-hash evidence entry.
type HashEvidenceObserver struct {
	ObserverID        string
	ObserverName      string
	RawSkewSec        float64
	CorrectedSkewSec  float64
	ObserverOffsetSec float64
	Calibrated        bool
}

// HashEvidence is per-hash clock skew evidence showing individual observer contributions.
type HashEvidence struct {
	Hash                   string
	Observers              []HashEvidenceObserver
	MedianCorrectedSkewSec float64
	Time                   time.Time // when the newest observation of this hash was heard
}

// CalibrationSummary counts how many samples were corrected via observer calibration.
type CalibrationSummary struct {
	TotalSamples        int
	CalibratedSamples   int
	UncalibratedSamples int
}

// txSkew is the skew statistics of one advert transmission, computed across
// the observers that heard it. The store later maps transmission hash to node
// pubkey when building the per-node view.
type txSkew struct {
	MeanSkewSec   float64
	MedianSkewSec float64
	LastSkewSec   float64
	Severity      types.SkewSeverity
	SampleCount   int
	Calibrated    bool
	LastAdvert    time.Time // what the node's clock said
	LastObserved  time.Time // newest observation, truncated to whole seconds
}

// txSkewResult maps tx hash → per-transmission skew stats. This is an
// intermediate result keyed by hash (not pubkey).
type txSkewResult = map[string]*txSkew

// ── Clock Skew Engine ──────────────────────────────────────────────────────────

// ClockSkewEngine computes and caches clock skew data for nodes and observers.
type ClockSkewEngine struct {
	mu              sync.RWMutex
	observerOffsets map[string]float64 // observerID → calibrated offset (seconds)
	observerSamples map[string]int     // observerID → number of multi-observer packets used
	nodeSkew        txSkewResult
	hashEvidence    map[string][]hashEvidenceEntry // hash → per-observer raw/corrected data
	lastComputed    time.Time
	computeInterval time.Duration
}

// hashEvidenceEntry stores raw evidence per observer per hash, cached during Recompute.
type hashEvidenceEntry struct {
	observerID   string
	rawSkew      float64
	corrected    float64
	offset       float64
	calibrated   bool
	observedTime time.Time
}

func NewClockSkewEngine() *ClockSkewEngine {
	return &ClockSkewEngine{
		observerOffsets: make(map[string]float64),
		observerSamples: make(map[string]int),
		nodeSkew:        make(txSkewResult),
		hashEvidence:    make(map[string][]hashEvidenceEntry),
		computeInterval: 30 * time.Second,
	}
}

// Invalidate makes the next Recompute run even if the last one is
// younger than computeInterval.
func (e *ClockSkewEngine) Invalidate() {
	e.mu.Lock()
	e.lastComputed = time.Time{}
	e.mu.Unlock()
}

// Recompute recalculates all clock skew data from the packet store.
// Called periodically or on demand. Holds store RLock externally.
// Uses read-copy-update: heavy computation runs outside the write lock,
// then results are swapped in under a brief lock.
func (e *ClockSkewEngine) Recompute(store *PacketStore) {
	// Fast path: check under read lock if recompute is needed.
	e.mu.RLock()
	fresh := time.Since(e.lastComputed) < e.computeInterval
	e.mu.RUnlock()
	if fresh {
		return
	}

	// Phase 1: Collect skew samples from ADVERT packets (store RLock held by caller).
	samples := collectSamples(store)

	// Phase 2–3: Compute outside the write lock.
	var newOffsets map[string]float64
	var newSamples map[string]int
	var newNodeSkew txSkewResult
	var newHashEvidence map[string][]hashEvidenceEntry

	if len(samples) > 0 {
		newOffsets, newSamples = calibrateObservers(samples)
		newNodeSkew, newHashEvidence = computeNodeSkew(samples, newOffsets)
	} else {
		newOffsets = make(map[string]float64)
		newSamples = make(map[string]int)
		newNodeSkew = make(txSkewResult)
		newHashEvidence = make(map[string][]hashEvidenceEntry)
	}

	// Swap results under brief write lock.
	e.mu.Lock()
	// Re-check: another goroutine may have computed while we were working.
	if time.Since(e.lastComputed) < e.computeInterval {
		e.mu.Unlock()
		return
	}
	e.observerOffsets = newOffsets
	e.observerSamples = newSamples
	e.nodeSkew = newNodeSkew
	e.hashEvidence = newHashEvidence
	e.lastComputed = time.Now()
	e.mu.Unlock()
}

// collectSamples extracts skew samples from ADVERT packets in the store.
// Must be called with store.mu held (at least RLock).
// collectSamples extracts skew samples from ADVERT packets in the store.
// Must be called with store.mu held (at least RLock).
func collectSamples(store *PacketStore) []skewSample {
	adverts := store.byPayloadType[types.PayloadADVERT]
	if len(adverts) == 0 {
		return nil
	}

	samples := make([]skewSample, 0, len(adverts)*2)
	for _, tx := range adverts {
		decoded := tx.ParsedDecoded()
		if decoded == nil {
			continue
		}
		// Extract advert timestamp (Unix seconds) from decoded JSON.
		advertTime := extractTimestamp(decoded)
		if advertTime.IsZero() {
			continue
		}
		// Sanity: skip timestamps before year 2020 or after year 2100.
		// Sanity: skip timestamps before year 2020 or after year 2100.
		if advertTime.Before(skewMinAdvert) || advertTime.After(skewMaxAdvert) {
			continue
		}

		for _, obs := range tx.Observations {
			if obs.Timestamp.IsZero() {
				continue
			}
			samples = append(samples, skewSample{
				advertTime:   advertTime,
				observedTime: obs.Timestamp,
				observerID:   obs.ObserverID,
				hash:         tx.Hash,
			})
		}
	}
	return samples
}

// extractTimestamp gets the Unix timestamp from a decoded ADVERT payload.
func extractTimestamp(decoded map[string]interface{}) time.Time {
	// Try payload.timestamp first (nested in "payload" key).
	if payload, ok := decoded["payload"]; ok {
		if pm, ok := payload.(map[string]interface{}); ok {
			if ts := jsonNumber(pm, "timestamp"); ts > 0 {
				return time.Unix(ts, 0).UTC()
			}
		}
	}
	// Fallback: top-level timestamp.
	if ts := jsonNumber(decoded, "timestamp"); ts > 0 {
		return time.Unix(ts, 0).UTC()
	}
	return time.Time{}
}

// jsonNumber extracts an int64 from a JSON-parsed map (handles float64 and json.Number).
func jsonNumber(m map[string]interface{}, key string) int64 {
	v, ok := m[key]
	if !ok || v == nil {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return i
		}
		if f, err := n.Float64(); err == nil {
			return int64(f)
		}
	}
	return 0
}

// parseISO parses an ISO 8601 timestamp string to Unix seconds.
func parseISO(s string) int64 {
	if s == "" {
		return 0
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		// Try with fractional seconds.
		t, err = time.Parse("2006-01-02T15:04:05.999999999Z07:00", s)
		if err != nil {
			return 0
		}
	}
	return t.Unix()
}

// ── Phase 2: Observer Calibration ──────────────────────────────────────────────

// calibrateObservers computes each observer's clock offset using multi-observer
// packets. Returns offset map and sample count map.
func calibrateObservers(samples []skewSample) (map[string]float64, map[string]int) {
	// Group observations by packet hash.
	byHash := make(map[string][]skewSample)
	for _, s := range samples {
		byHash[s.hash] = append(byHash[s.hash], s)
	}

	// For each multi-observer packet, compute per-observer deviation from median.
	deviations := make(map[string][]float64) // observerID → list of deviations (seconds)
	for _, group := range byHash {
		if len(group) < 2 {
			continue // single-observer packet, can't calibrate
		}
		// Work in seconds relative to the group's first observation. A
		// float64 holds ~15-16 significant digits, so absolute Unix
		// seconds with sub-second parts would lose precision; relative
		// offsets stay exact.
		ref := group[0].observedTime
		rel := make([]float64, len(group))
		for i, s := range group {
			rel[i] = s.observedTime.Sub(ref).Seconds()
		}
		medianRel := median(rel)
		for i, s := range group {
			deviations[s.observerID] = append(deviations[s.observerID], rel[i]-medianRel)
		}
	}

	// Each observer's offset = median of its deviations.
	offsets := make(map[string]float64, len(deviations))
	counts := make(map[string]int, len(deviations))
	for obsID, devs := range deviations {
		offsets[obsID] = median(devs)
		counts[obsID] = len(devs)
	}
	return offsets, counts
}

// ── Phase 3: Per-Node Skew ─────────────────────────────────────────────────────

// computeNodeSkew calculates corrected skew statistics for each node.
func computeNodeSkew(samples []skewSample, obsOffsets map[string]float64) (txSkewResult, map[string][]hashEvidenceEntry) {
	// Compute corrected skew per sample, grouped by hash (each hash = one
	// node's advert transmission). The caller maps hash → pubkey via byNode.
	type correctedSample struct {
		skew         float64
		observedTime time.Time
		calibrated   bool
	}

	byHash := make(map[string][]correctedSample)
	hashAdvertTime := make(map[string]time.Time)     // was map[string]int64
	evidence := make(map[string][]hashEvidenceEntry) // hash → per-observer evidence

	for _, s := range samples {
		obsOffset, hasCal := obsOffsets[s.observerID]
		rawSkew := s.rawSkewSec()
		corrected := rawSkew
		if hasCal {
			// Observer offset = obs_ts - median(all_obs_ts). If observer is ahead,
			// its obs_ts is inflated, making raw_skew too low. Add offset to correct.
			corrected = rawSkew + obsOffset
		}
		byHash[s.hash] = append(byHash[s.hash], correctedSample{
			skew:         corrected,
			observedTime: s.observedTime,
			calibrated:   hasCal,
		})
		hashAdvertTime[s.hash] = s.advertTime
		evidence[s.hash] = append(evidence[s.hash], hashEvidenceEntry{
			observerID:   s.observerID,
			rawSkew:      round(rawSkew, 1),
			corrected:    round(corrected, 1),
			offset:       round(obsOffset, 1),
			calibrated:   hasCal,
			observedTime: s.observedTime,
		})
	}

	// Each hash represents one advert from one node. Compute median corrected
	// skew per hash (across multiple observers).

	result := make(txSkewResult) // keyed by hash for now
	for hash, cs := range byHash {
		skews := make([]float64, len(cs))
		for i, c := range cs {
			skews[i] = c.skew
		}
		medSkew := median(skews)
		meanSkew := mean(skews)

		// Find latest observation.
		var latestObs time.Time
		var anyCal bool
		for _, c := range cs {
			if c.observedTime.After(latestObs) {
				latestObs = c.observedTime
			}
			if c.calibrated {
				anyCal = true
			}
		}

		absMedian := math.Abs(medSkew)
		result[hash] = &txSkew{
			MeanSkewSec:   round(meanSkew, 1),
			MedianSkewSec: round(medSkew, 1),
			LastSkewSec:   round(cs[len(cs)-1].skew, 1),
			Severity:      types.ClassifySkew(absMedian),
			SampleCount:   len(cs),
			Calibrated:    anyCal,
			LastAdvert:    time.Unix(hashAdvertTime[hash].Unix(), 0).UTC(),
			LastObserved:  time.Unix(latestObs.Unix(), 0).UTC(),
		}
	}
	return result, evidence
}

// ── Integration with PacketStore ───────────────────────────────────────────────

// GetNodeClockSkew returns the clock skew data for a specific node (acquires RLock).
func (s *PacketStore) GetNodeClockSkew(pubkey string) *NodeClockSkew {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getNodeClockSkewLocked(pubkey)
}

// txOriginatedBy reports whether tx is an ADVERT self-signed by pubkey,
// as opposed to a transmission merely indexed under pubkey because it was
// resolved as a relay hop. byNode is an involvement index that
// intentionally includes relay-hop transmissions for the activity
// timeline (store.go:1696-1705, indexResolvedPathHops, #1558/#1352).
// Clock-skew computation must restrict to self-originated adverts only:
// ADVERTs are self-signed, so decoded["pubKey"] is the originator per
// protocol (safe key, does not need path-hop resolution). Without this
// guard, every relay that forwards a broken-clock node's advert inherits
// that node's skew, producing fleet-wide false no_clock/bimodal
// classifications and bit-identical skew "clusters" across unrelated
// relays (#1816), including cases where a node's own adverts are healthy
// but a couple of relayed adverts from a broken-clock originator dominate
// its small recent-window sample (#1818). Comparison is case-insensitive
// as a defensive measure; pubkeys are lowercase hex today (decoder.go),
// but nothing enforces that at this boundary.
func txOriginatedBy(tx *StoreTx, pubkey string) bool {
	decoded := tx.ParsedDecoded()
	if decoded == nil {
		return false
	}
	pk, ok := decoded["pubKey"].(string)
	if !ok || pk == "" {
		return false
	}
	return strings.EqualFold(pk, pubkey)
}

// getNodeClockSkewLocked returns clock skew for a node.
// Must be called with s.mu held (at least RLock).
func (s *PacketStore) getNodeClockSkewLocked(pubkey string) *NodeClockSkew {
	s.clockSkew.Recompute(s)

	txs := s.byNode[pubkey]
	if len(txs) == 0 {
		return nil
	}

	s.clockSkew.mu.RLock()
	defer s.clockSkew.mu.RUnlock()

	var allSkews []float64
	var lastSkew float64
	var lastObserved, lastAdvert time.Time
	var totalSamples int
	var anyCal bool
	var tsSkews []tsSkewPair

	for _, tx := range txs {
		if tx.PayloadType == nil || *tx.PayloadType != types.PayloadADVERT {
			continue
		}
		if !txOriginatedBy(tx, pubkey) {
			// Skip adverts this node merely relayed (byNode is an
			// involvement index, not an originator index) — see
			// txOriginatedBy and #1816/#1818.
			continue
		}
		cs, ok := s.clockSkew.nodeSkew[tx.Hash]
		if !ok {
			continue
		}
		allSkews = append(allSkews, cs.MedianSkewSec)
		totalSamples += cs.SampleCount
		if cs.Calibrated {
			anyCal = true
		}
		if cs.LastObserved.After(lastObserved) {
			lastObserved = cs.LastObserved
			lastSkew = cs.LastSkewSec
			lastAdvert = cs.LastAdvert
		}
		tsSkews = append(tsSkews, tsSkewPair{ts: cs.LastObserved.Unix(), skew: cs.MedianSkewSec, hash: tx.Hash, advertTime: cs.LastAdvert})
	}

	if len(allSkews) == 0 {
		return nil
	}

	medSkew := median(allSkews)
	meanSkew := mean(allSkews)

	// Severity is derived from RECENT samples only (issue #789). The
	// all-time median is poisoned by historical bad data — a node that
	// was off for hours and then GPS-corrected can have median = -59M sec
	// while its current skew is -0.8s. Operators need severity to reflect
	// current health, so they trust the dashboard.
	//
	// Sort tsSkews by time and take the last recentSkewWindowCount samples
	// (or all samples within recentSkewWindowSec of the latest, whichever
	// gives FEWER samples — we want the more-current view; a chatty node
	// can fit dozens of samples in 1h, in which case the count cap wins).
	sort.Slice(tsSkews, func(i, j int) bool { return tsSkews[i].ts < tsSkews[j].ts })

	recentSkew := lastSkew
	var recentVals []float64
	var recentPairs []tsSkewPair
	if n := len(tsSkews); n > 0 {
		latestTS := tsSkews[n-1].ts
		// Index-based window: last K samples.
		startByCount := n - recentSkewWindowCount
		if startByCount < 0 {
			startByCount = 0
		}
		// Time-based window: samples newer than latestTS - windowSec.
		startByTime := n - 1
		for i := n - 1; i >= 0; i-- {
			if latestTS-tsSkews[i].ts <= recentSkewWindowSec {
				startByTime = i
			} else {
				break
			}
		}
		// Pick the narrower (larger-index) of the two windows — the most
		// current view of the node's clock health.
		start := startByCount
		if startByTime > start {
			start = startByTime
		}
		recentVals = make([]float64, 0, n-start)
		recentPairs = tsSkews[start:n]
		for i := start; i < n; i++ {
			recentVals = append(recentVals, tsSkews[i].skew)
		}
		if len(recentVals) > 0 {
			recentSkew = median(recentVals)
		}
	}

	// ── Bimodal detection (#845) ─────────────────────────────────────────
	// Split recent samples into "good" (|skew| <= 1h, real clock) and
	// "bad" (|skew| > 1h, firmware nonsense from uninitialized RTC).
	// Classification order (first match wins):
	//   no_clock       — goodFraction < 0.10 (essentially no real clock)
	//   bimodal_clock  — 0.10 <= goodFraction < 0.80 AND badCount > 0
	//   ok/warn/etc.   — goodFraction >= 0.80 (normal, outliers filtered)
	//
	// RTC-reset outliers (|skew| > 24h — single advert where the firmware
	// emitted its factory timestamp) are EXCLUDED from this split (bug
	// #1285): they're not "bimodal-bad real-but-large skew" but obvious
	// sensor garbage, surfaced separately via the RTC-reset badge. Counting
	// them as bimodal-bad produces a false-alarm warning ("3 of last 5
	// adverts had nonsense timestamps") on otherwise-healthy nodes.
	var goodSamples []float64
	var rtcResetCount int
	var recentBadSamples []BadSample // #1094: per-bad-sample evidence (hash + advertTS)
	for i, v := range recentVals {
		absV := math.Abs(v)
		switch {
		case absV > rtcResetOutlierThresholdSec:
			rtcResetCount++ // ignored for good/bad classification
		case absV <= bimodalSkewThresholdSec:
			goodSamples = append(goodSamples, v)
		default:
			// Bimodal-bad: 1h < |skew| <= 24h. Capture hash + advertTS so
			// the UI can link each offender to its packet detail page
			// instead of showing a count without evidence (#1094).
			if i < len(recentPairs) && recentPairs[i].hash != "" {
				recentBadSamples = append(recentBadSamples, BadSample{
					Hash:       recentPairs[i].hash,
					AdvertTime: recentPairs[i].advertTime,
					SkewSec:    round(v, 1),
				})
			}
		}
	}
	recentSampleCount := len(recentVals) - rtcResetCount
	recentBadCount := recentSampleCount - len(goodSamples)
	var goodFraction float64
	if recentSampleCount > 0 {
		goodFraction = float64(len(goodSamples)) / float64(recentSampleCount)
	}

	var severity types.SkewSeverity
	if goodFraction < 0.10 {
		// Essentially no real clock — classify as no_clock regardless
		// of the raw skew magnitude.
		severity = types.SkewNoClock
	} else if goodFraction < 0.80 && recentBadCount > 0 {
		// Bimodal: use median of GOOD samples as the "real" skew.
		severity = types.SkewBimodalClock
		if len(goodSamples) > 0 {
			recentSkew = median(goodSamples)
		}
	} else {
		// Normal path: if there are good samples, use their median
		// (filters out rare outliers in ≥80% good case, and rejects
		// RTC-reset outliers regardless of bimodal/bad counts — #1285).
		if len(goodSamples) > 0 {
			recentSkew = median(goodSamples)
		}
		severity = types.ClassifySkew(math.Abs(recentSkew))
	}

	// For no_clock / bimodal_clock nodes, skip drift when data is unreliable.
	var drift float64
	if !severity.HasUnreliableClock() && len(tsSkews) >= minDriftSamples {
		drift = computeDrift(tsSkews)
		// Cap physically impossible drift rates.
		if math.Abs(drift) > maxReasonableDriftPerDay {
			drift = 0
		}
	}

	// Build sparkline samples from tsSkews (already sorted by time above).
	samples := make([]SkewSample, len(tsSkews))
	for i, p := range tsSkews {
		samples[i] = SkewSample{Time: time.Unix(p.ts, 0).UTC(), SkewSec: round(p.skew, 1)}
	}

	// Build per-hash evidence (most recent 10 hashes with ≥1 observer).
	// Observer name lookup from store observations.
	obsNameMap := make(map[string]string)
	type hashMeta struct {
		hash string
		ts   int64
	}
	var evidenceHashes []hashMeta
	for _, tx := range txs {
		if tx.PayloadType == nil || *tx.PayloadType != types.PayloadADVERT {
			continue
		}
		if !txOriginatedBy(tx, pubkey) {
			// Keep evidence consistent with the self-only skew stream
			// above — don't show relayed adverts as this node's evidence.
			continue
		}
		ev, ok := s.clockSkew.hashEvidence[tx.Hash]
		if !ok || len(ev) == 0 {
			continue
		}
		// Collect observer names from tx observations.
		for _, obs := range tx.Observations {
			if obs.ObserverID != "" && obs.ObserverName != "" {
				obsNameMap[obs.ObserverID] = obs.ObserverName
			}
		}
		evidenceHashes = append(evidenceHashes, hashMeta{hash: tx.Hash, ts: ev[0].observedTime.Unix()})
	}
	// Sort by timestamp descending, take most recent 10.
	sort.Slice(evidenceHashes, func(i, j int) bool { return evidenceHashes[i].ts > evidenceHashes[j].ts })
	if len(evidenceHashes) > 10 {
		evidenceHashes = evidenceHashes[:10]
	}
	var recentEvidence []HashEvidence
	var calSummary CalibrationSummary
	for _, eh := range evidenceHashes {
		entries := s.clockSkew.hashEvidence[eh.hash]
		var observers []HashEvidenceObserver
		var corrSkews []float64
		for _, e := range entries {
			name := obsNameMap[e.observerID]
			if name == "" {
				name = e.observerID
			}
			observers = append(observers, HashEvidenceObserver{
				ObserverID:        e.observerID,
				ObserverName:      name,
				RawSkewSec:        e.rawSkew,
				CorrectedSkewSec:  e.corrected,
				ObserverOffsetSec: e.offset,
				Calibrated:        e.calibrated,
			})
			corrSkews = append(corrSkews, e.corrected)
			calSummary.TotalSamples++
			if e.calibrated {
				calSummary.CalibratedSamples++
			} else {
				calSummary.UncalibratedSamples++
			}
		}
		recentEvidence = append(recentEvidence, HashEvidence{
			Hash:                   eh.hash,
			Observers:              observers,
			MedianCorrectedSkewSec: round(hashEvidenceMedian(corrSkews), 1),
			Time:                   time.Unix(eh.ts, 0).UTC(),
		})
	}

	return &NodeClockSkew{
		Pubkey:               pubkey,
		MeanSkewSec:          round(meanSkew, 1),
		MedianSkewSec:        round(medSkew, 1),
		LastSkewSec:          round(lastSkew, 1),
		RecentMedianSkewSec:  round(recentSkew, 1),
		DriftPerDaySec:       round(drift, 2),
		Severity:             severity,
		SampleCount:          totalSamples,
		Calibrated:           anyCal,
		LastAdvertTime:       lastAdvert,
		LastObservedTime:     lastObserved,
		Samples:              samples,
		GoodFraction:         round(goodFraction, 2),
		RecentBadSampleCount: recentBadCount,
		RecentBadSamples:     recentBadSamples,
		RecentSampleCount:    recentSampleCount,
		RecentHashEvidence:   recentEvidence,
		CalibrationSummary:   &calSummary,
	}
}

// GetFleetClockSkew returns clock skew data for all nodes, optionally
// filtered to area. With no area, prefers the steady-state recomputer
// snapshot (issue #1265). Must NOT be called with s.mu held.
func (s *PacketStore) GetFleetClockSkew(ctx context.Context, area string) []*NodeClockSkew {
	if area == "" {
		s.analyticsRecomputerMu.RLock()
		rc := s.recompNodesClockSkew
		s.analyticsRecomputerMu.RUnlock()
		if rc != nil {
			if v := rc.Load(); v != nil {
				if r, ok := v.([]*NodeClockSkew); ok {
					return r
				}
			}
		}
	}
	return s.computeFleetClockSkewForArea(ctx, area)
}

// computeFleetClockSkew wraps computeFleetClockSkewForArea with no area
// filter; called by the steady-state recomputer. Must NOT be called with
// s.mu held.
func (s *PacketStore) computeFleetClockSkew(ctx context.Context) []*NodeClockSkew {
	return s.computeFleetClockSkewForArea(ctx, "")
}

// computeFleetClockSkewForArea is the underlying compute. Must NOT be
// called with s.mu held.
func (s *PacketStore) computeFleetClockSkewForArea(ctx context.Context, area string) []*NodeClockSkew {
	var areaNodes map[string]bool
	if area != "" {
		areaNodes = s.resolveAreaNodes(ctx, area)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Build name/role lookup from DB cache (requires s.mu held).
	allNodes, _ := s.getCachedNodesAndPM()
	nameMap := make(map[string]nodeInfo, len(allNodes))
	for _, ni := range allNodes {
		nameMap[ni.PublicKey] = ni
	}

	var results = []*NodeClockSkew{}
	for pubkey := range s.byNode {
		if areaNodes != nil && !areaNodes[pubkey] {
			continue
		}
		cs := s.getNodeClockSkewLocked(pubkey)
		if cs == nil {
			continue
		}
		// Enrich with node name/role.
		if ni, ok := nameMap[pubkey]; ok {
			cs.NodeName = ni.Name
			cs.NodeRole = ni.Role
		}
		// Omit samples and evidence in fleet response (too much data).
		cs.Samples = nil
		cs.RecentHashEvidence = nil
		cs.CalibrationSummary = nil
		results = append(results, cs)
	}
	return results
}

// GetObserverCalibrations returns the current observer clock offsets,
// preferring the steady-state recomputer snapshot (issue #1265). Falls
// back to an on-request compute when the recomputer is not running.
func (s *PacketStore) GetObserverCalibrations() []ObserverCalibration {
	s.analyticsRecomputerMu.RLock()
	rc := s.recompObserversClockSkew
	s.analyticsRecomputerMu.RUnlock()
	if rc != nil {
		if v := rc.Load(); v != nil {
			if r, ok := v.([]ObserverCalibration); ok {
				return r
			}
		}
	}
	return s.computeObserverCalibrations()
}

// computeObserverCalibrations is the underlying compute used by the
// recomputer and on-request fallback. Must NOT be called with s.mu held.
func (s *PacketStore) computeObserverCalibrations() []ObserverCalibration {
	s.mu.RLock()
	defer s.mu.RUnlock()

	s.clockSkew.Recompute(s)

	s.clockSkew.mu.RLock()
	defer s.clockSkew.mu.RUnlock()

	result := make([]ObserverCalibration, 0, len(s.clockSkew.observerOffsets))
	for obsID, offset := range s.clockSkew.observerOffsets {
		result = append(result, ObserverCalibration{
			ObserverID: obsID,
			OffsetSec:  round(offset, 1),
			Samples:    s.clockSkew.observerSamples[obsID],
		})
	}
	// Sort by absolute offset descending.
	sort.Slice(result, func(i, j int) bool {
		return math.Abs(result[i].OffsetSec) > math.Abs(result[j].OffsetSec)
	})
	return result
}

// ── Math Helpers ───────────────────────────────────────────────────────────────

func median(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sorted := make([]float64, len(vals))
	copy(sorted, vals)
	sort.Float64s(sorted)
	n := len(sorted)
	if n%2 == 0 {
		return (sorted[n/2-1] + sorted[n/2]) / 2
	}
	return sorted[n/2]
}

// hashEvidenceMedian returns the median corrected skew for a single
// transmission hash, filtering out RTC-reset outliers (|skew| > 24h —
// firmware emitting factory timestamp). Issue #1285: a single outlier
// observer was dragging the displayed median to ~-704d on an otherwise
// healthy node. If filtering leaves zero usable samples (every observer
// of this hash saw a reset-shaped advert), return 0 so the UI can render
// "insufficient data" rather than the garbage outlier value.
func hashEvidenceMedian(vals []float64) float64 {
	clean := vals[:0:0]
	for _, v := range vals {
		if math.Abs(v) <= rtcResetOutlierThresholdSec {
			clean = append(clean, v)
		}
	}
	return median(clean)
}

func mean(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

// tsSkewPair is a (timestamp, skew) pair for drift estimation. Also carries
// the source hash + advertTime so callers building per-sample evidence (e.g.
// recentBadSamples for #1094) can identify the offending packet without a
// second pass. Drift code reads only ts/skew; the extra fields are inert
// there.
type tsSkewPair struct {
	ts         int64
	skew       float64
	hash       string
	advertTime time.Time
}

// computeDrift estimates linear drift in seconds per day from time-ordered
// (timestamp, skew) pairs. Issue #789: a single GPS-correction event (huge
// skew jump in seconds) used to dominate ordinary least squares and produce
// absurd drift like 1.7M sec/day. We now:
//
//  1. Drop pairs whose consecutive skew jump exceeds maxPlausibleSkewJumpSec
//     (clock corrections, not physical drift). This protects both OLS-style
//     consumers and Theil-Sen.
//  2. Use Theil-Sen regression — the slope is the median of all pairwise
//     slopes, naturally robust to remaining outliers (breakdown point ~29%).
//
// For very small samples after filtering we fall back to a simple slope
// between first and last calibrated samples.
func computeDrift(pairs []tsSkewPair) float64 {
	if len(pairs) < 2 {
		return 0
	}
	// Sort by timestamp.
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].ts < pairs[j].ts
	})

	// Time span too short? Skip.
	spanSec := float64(pairs[len(pairs)-1].ts - pairs[0].ts)
	if spanSec < 3600 { // need at least 1 hour of data
		return 0
	}

	// Outlier filter: drop samples where the skew jumps more than
	// maxPlausibleSkewJumpSec from the running "stable" baseline.
	// We anchor on the first sample, then accept each subsequent point
	// that's within the threshold of the most recent accepted point —
	// this preserves a slow drift while rejecting correction events.
	filtered := make([]tsSkewPair, 0, len(pairs))
	filtered = append(filtered, pairs[0])
	for i := 1; i < len(pairs); i++ {
		prev := filtered[len(filtered)-1]
		if math.Abs(pairs[i].skew-prev.skew) <= maxPlausibleSkewJumpSec {
			filtered = append(filtered, pairs[i])
		}
	}
	// If the filter killed too much (e.g. unstable node), fall back to the
	// raw series so we at least produce *something* — it'll be capped by
	// maxReasonableDriftPerDay downstream.
	if len(filtered) < 2 || float64(filtered[len(filtered)-1].ts-filtered[0].ts) < 3600 {
		filtered = pairs
	}

	// Cap point count for Theil-Sen (O(n²) on pairs). Keep most-recent.
	if len(filtered) > theilSenMaxPoints {
		filtered = filtered[len(filtered)-theilSenMaxPoints:]
	}

	return theilSenSlope(filtered) * 86400 // sec/sec → sec/day
}

// theilSenSlope returns the Theil-Sen estimator: median of all pairwise
// slopes (yj - yi) / (tj - ti) for i < j. Naturally robust to outliers.
// Pairs must be sorted by timestamp ascending.
func theilSenSlope(pairs []tsSkewPair) float64 {
	n := len(pairs)
	if n < 2 {
		return 0
	}
	// Pre-allocate: n*(n-1)/2 pairs.
	slopes := make([]float64, 0, n*(n-1)/2)
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			dt := float64(pairs[j].ts - pairs[i].ts)
			if dt <= 0 {
				continue
			}
			slopes = append(slopes, (pairs[j].skew-pairs[i].skew)/dt)
		}
	}
	if len(slopes) == 0 {
		return 0
	}
	return median(slopes)
}

// Regression test for #1094: the bimodal-clock warning currently exposes only
// RecentBadSampleCount, leaving the UI to render "⚠️ N of M adverts had
// nonsense timestamps" without telling the operator WHICH packets were bad.
//
// This test pins the additive API contract: alongside the count, the response
// must expose RecentBadSamples — a slice of (hash, advertTS, skewSec) — so the
// frontend can render each offending hash as a clickable link with its bad
// timestamp.
// Seeds 5 recent adverts: 3 healthy (~-20s skew) and 2 with a "nonsense"
// bimodal-bad timestamp (|skew| in (1h, 24h]). The recent window is exactly
// 5 samples, so all five are inside it.
func seedIssue1094Repro(t *testing.T) (*PacketStore, []string, []int64) {
	t.Helper()
	ps := NewPacketStore(nil, nil)
	pt := types.PayloadADVERT

	const pubkey = "BADTS1094"
	baseObs := time.Date(2026, 5, 17, 6, 40, 0, 0, time.UTC) // equivalent to the old 1779000000

	var txs []*StoreTx
	var badHashes []string
	var badAdvertTSs []int64

	// 3 healthy adverts (skew = -20s).
	for i := 0; i < 3; i++ {
		obsTime := baseObs.Add(time.Duration(i) * time.Minute)
		advTS := obsTime.Add(-20 * time.Second).Unix()
		txs = append(txs, &StoreTx{
			Hash:        "healthy-1094-" + formatInt64(int64(i)),
			PayloadType: &pt,
			DecodedJSON: `{"payload":{"timestamp":` + formatInt64(advTS) + `},"pubKey":"BADTS1094"}`,
			Observations: []*StoreObs{
				{ObserverID: "obs1", Timestamp: obsTime},
			},
		})
	}

	// 2 nonsense-timestamp adverts (skew = -7200s = -2h — bimodal-bad,
	// below the 24h RTC-reset exclusion so they DO count in recentBadCount).
	for i := 0; i < 2; i++ {
		obsTS := baseObs.Add(time.Duration(3+i) * 60)
		advTS := obsTS.Add(-2 * time.Hour).Unix()
		hash := "bad-1094-" + formatInt64(int64(i))
		txs = append(txs, &StoreTx{
			Hash:        hash,
			PayloadType: &pt,
			DecodedJSON: `{"payload":{"timestamp":` + formatInt64(advTS) + `},"pubKey":"BADTS1094"}`,
			Observations: []*StoreObs{
				{ObserverID: "obs1", Timestamp: obsTS},
			},
		})
		badHashes = append(badHashes, hash)
		badAdvertTSs = append(badAdvertTSs, advTS)
	}

	ps.mu.Lock()
	ps.byNode[pubkey] = txs
	for _, tx := range txs {
		ps.byPayloadType[4] = append(ps.byPayloadType[4], tx)
	}
	ps.clockSkew.computeInterval = 0
	ps.mu.Unlock()
	return ps, badHashes, badAdvertTSs
}

func TestIssue1094_RecentBadSamples_ExposesHashAndTimestamp(t *testing.T) {
	ps, wantHashes, wantAdvertTSs := seedIssue1094Repro(t)
	r := ps.GetNodeClockSkew("BADTS1094")
	if r == nil {
		t.Fatal("expected clock skew result")
	}

	// Pre-condition: count must already be 2 (gates the test against the
	// existing field — if this drops we'd be measuring the wrong thing).
	if r.RecentBadSampleCount != 2 {
		t.Fatalf("RecentBadSampleCount = %d, want 2 (seed bug, not the field-under-test)",
			r.RecentBadSampleCount)
	}

	if len(r.RecentBadSamples) != 2 {
		t.Fatalf("RecentBadSamples len = %d, want 2 — operators need to see which "+
			"adverts had nonsense timestamps, not just the count",
			len(r.RecentBadSamples))
	}

	gotByHash := map[string]int64{}
	for _, bs := range r.RecentBadSamples {
		gotByHash[bs.Hash] = bs.AdvertTime.Unix()
	}
	for i, h := range wantHashes {
		ts, ok := gotByHash[h]
		if !ok {
			t.Errorf("RecentBadSamples missing hash %q", h)
			continue
		}
		if ts != wantAdvertTSs[i] {
			t.Errorf("RecentBadSamples[%q].AdvertTime = %d, want %d (the bad advert time)",
				h, ts, wantAdvertTSs[i])
		}
	}
}

// Regression tests for #1285:
//
// Bug A: per-hash evidence's MedianCorrectedSkewSec includes 700-day RTC-reset
//        outliers, dragging the displayed median into "−704d 18h" garbage even
//        though every recent sample is small (< 30s).
//
// Bug B: RecentBadSampleCount counts samples outside the *displayed* recent
//        window (or counts against raw skew, not corrected) → "3 of last 5
//        adverts had nonsense timestamps" warning fires on healthy nodes.
//
// Both must pass without disturbing the existing bimodal / no-clock logic.

// Synthesizes the repro from issue #1285:
//
//	30 healthy adverts (skew ~−20s) + 1 historical advert with an RTC reset
//	(advertTS = 2024-06-13, observed today → −705d skew).
//
// Returns the populated store.
func seedIssue1285Repro(t *testing.T) *PacketStore {
	t.Helper()
	ps := NewPacketStore(nil, nil)
	pt := types.PayloadADVERT

	const pubkey = "RTCRESET"
	const skew = -20 * time.Second // node clock is 20s BEHIND wall-clock

	baseObs := time.Unix(1779000000, 0).UTC() // ~mid-2026
	rtcResetAdv := int64(1718281640)          // 2024-06-13 (from the issue repro); payload value, stays unix seconds

	var txs []*StoreTx

	// 30 healthy adverts spanning the older end of the recent window.
	for i := 0; i < 30; i++ {
		obsTime := baseObs.Add(time.Duration(i) * time.Minute)
		advTS := obsTime.Add(skew).Unix()
		tx := &StoreTx{
			Hash:        "healthy-" + formatInt64(int64(i)),
			PayloadType: &pt,
			DecodedJSON: `{"payload":{"timestamp":` + formatInt64(advTS) + `},"pubKey":"RTCRESET"}`,
			Observations: []*StoreObs{
				{ObserverID: "obs1", Timestamp: obsTime},
			},
		}
		txs = append(txs, tx)
	}

	// One RTC-reset packet observed MOST RECENTLY (so it sits in the
	// per-hash evidence list AND is included in the recent-window count
	// on master). Its advertTS is from 2024 → corrected skew ≈ -60M sec.
	rtcResetObs := baseObs.Add(31 * time.Minute) // 30*60 + 60 seconds after base
	rtcTx := &StoreTx{
		Hash:        "rtc-reset-0001",
		PayloadType: &pt,
		DecodedJSON: `{"payload":{"timestamp":` + formatInt64(rtcResetAdv) + `},"pubKey":"RTCRESET"}`,
		Observations: []*StoreObs{
			{ObserverID: "obs1", Timestamp: rtcResetObs},
		},
	}
	txs = append(txs, rtcTx)

	ps.mu.Lock()
	ps.byNode[pubkey] = txs
	for _, tx := range txs {
		ps.byPayloadType[4] = append(ps.byPayloadType[4], tx)
	}
	ps.clockSkew.computeInterval = 0
	ps.mu.Unlock()
	return ps
}

// Bug A — per-hash evidence median must EXCLUDE the 705-day RTC-reset outlier.
// On master this asserts on the RTC-reset hash's MedianCorrectedSkewSec being
// ≈ −60M sec ("−704d 18h"); after the fix the field is suppressed (0) or
// otherwise marked insufficient, never displayed as the garbage value.
func TestIssue1285_HashEvidence_OutlierExcludedFromMedian(t *testing.T) {
	ps := seedIssue1285Repro(t)
	r := ps.GetNodeClockSkew("RTCRESET")
	if r == nil {
		t.Fatal("expected clock skew result")
	}

	// The recent-hash evidence list is the source of the "median corrected:
	// −704d 18h" string in the UI. After the fix, NO entry in this list
	// should report a |median| above the 24h sanity threshold — the fix is
	// to drop outlier samples (or flag the hash as insufficient-data) before
	// publishing the median.
	const maxSaneAbsSec = float64(24 * 3600)
	for _, ev := range r.RecentHashEvidence {
		if abs(ev.MedianCorrectedSkewSec) > maxSaneAbsSec {
			t.Errorf("hash %s exposes outlier-dominated median %.0fs (~%.1fd); "+
				"expected entry to be filtered out or marked insufficient (|median| <= %.0fs)",
				ev.Hash, ev.MedianCorrectedSkewSec,
				ev.MedianCorrectedSkewSec/86400, maxSaneAbsSec)
		}
	}
}

// Bug B — RecentBadSampleCount must be 0 when every sample in the recent
// window is healthy (<30s |corrected skew|). On master this fires because
// "recent" is computed over the wrong set (or against raw skew).
func TestIssue1285_RecentBadCount_NotPollutedByOldOutlier(t *testing.T) {
	ps := seedIssue1285Repro(t)
	r := ps.GetNodeClockSkew("RTCRESET")
	if r == nil {
		t.Fatal("expected clock skew result")
	}
	if r.RecentBadSampleCount != 0 {
		t.Errorf("RecentBadSampleCount = %d, want 0 — recent samples are all "+
			"~−20s (healthy); the historical RTC-reset outlier is outside the "+
			"recent window and must not be counted", r.RecentBadSampleCount)
	}
	if r.Severity == types.SkewBimodalClock || r.Severity == types.SkewNoClock {
		t.Errorf("severity = %v, want ok/warning — recent samples are all "+
			"healthy (~−20s skew), one historical outlier must not flip the node "+
			"to bimodal/no-clock", r.Severity)
	}
}

func abs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

// Regression tests for #1816 / #1818:
//
// getNodeClockSkewLocked() iterated every ADVERT transaction indexed under
// a pubkey in s.byNode WITHOUT checking whether the node actually
// originated (self-signed) that advert. byNode is an involvement index
// (store.go:1696-1705, indexResolvedPathHops, #1558/#1352): a transmission
// is also indexed under every relay-hop pubkey extracted from an
// observation's resolved_path. So a relay that merely forwarded a
// broken-clock node's advert inherited that node's skew as if it were its
// own — producing:
//
//   - Fleet-wide false no_clock / bimodal_clock classifications on healthy
//     relays whose only "bad" samples are adverts they relayed (#1816).
//   - Bit-identical RecentMedianSkewSec "clusters" across unrelated relays
//     that all forwarded the same broken-clock originator (#1816).
//   - A single relay showing a nonsense multi-day skew even though its own
//     self-adverts are perfectly healthy, because 1-2 relayed adverts from
//     a broken originator landed in the tail of its small recent-window
//     sample (#1818 — the "island" repro from cwichura).
//
// The fix (txOriginatedBy in clock_skew.go) restricts the ADVERT filter in
// getNodeClockSkewLocked to self-originated adverts only (decoded pubKey
// == pubkey), leaving byNode itself untouched (#1558/#1352 still depend on
// the broader involvement index for other consumers).
// ── txOriginatedBy ───────────────────────────────────────────────────────────

func TestTxOriginatedBy(t *testing.T) {
	pt := types.PayloadADVERT
	selfTx := &StoreTx{
		Hash:        "self-1",
		PayloadType: &pt,
		DecodedJSON: `{"payload":{"timestamp":1700000000},"pubKey":"AABBCC"}`,
	}
	foreignTx := &StoreTx{
		Hash:        "foreign-1",
		PayloadType: &pt,
		DecodedJSON: `{"payload":{"timestamp":1700000000},"pubKey":"DDEEFF"}`,
	}
	noPubkeyTx := &StoreTx{
		Hash:        "nopk-1",
		PayloadType: &pt,
		DecodedJSON: `{"payload":{"timestamp":1700000000}}`,
	}
	mixedCaseTx := &StoreTx{
		Hash:        "case-1",
		PayloadType: &pt,
		DecodedJSON: `{"payload":{"timestamp":1700000000},"pubKey":"aabbcc"}`,
	}

	if !txOriginatedBy(selfTx, "AABBCC") {
		t.Error("expected self-signed advert to match its own pubkey")
	}
	if txOriginatedBy(foreignTx, "AABBCC") {
		t.Error("expected advert from a different originator NOT to match")
	}
	if txOriginatedBy(noPubkeyTx, "AABBCC") {
		t.Error("expected advert with no decoded pubKey NOT to match anything")
	}
	if !txOriginatedBy(mixedCaseTx, "AABBCC") {
		t.Error("expected case-insensitive pubkey match to succeed")
	}
}

// ── #1816: relay must not inherit the originator's skew ───────────────────────

// A relay ("RELAY001") only ever transmits healthy self-adverts (skew ~0s).
// It also forwards adverts from a broken-clock originator ("BROKENORIG",
// skew ~+100,500s, matching the #1816 report's +100.5k band) — those
// transmissions are indexed under RELAY001 in byNode too, exactly as
// indexResolvedPathHops does for real relay-hop traffic. Before the fix,
// these foreign adverts polluted RELAY001's skew stream and could flip its
// severity to no_clock/bimodal even though every advert RELAY001 itself
// sent is fine.
func TestIssue1816_RelayDoesNotInheritOriginatorSkew(t *testing.T) {
	ps := NewPacketStore(nil, nil)
	pt := types.PayloadADVERT
	baseObs := time.Unix(1700000000, 0).UTC()

	var relayTxs []*StoreTx

	// RELAY001's own healthy adverts: skew ~0s.
	for i := 0; i < 5; i++ {
		obsTime := baseObs.Add(time.Duration(i) * 5 * time.Minute)
		advTS := obsTime.Add(-2 * time.Second).Unix() // 2s behind, trivially healthy
		tx := &StoreTx{
			Hash:        "relay-self-" + formatInt64(int64(i)),
			PayloadType: &pt,
			DecodedJSON: `{"payload":{"timestamp":` + formatInt64(advTS) + `},"pubKey":"RELAY001"}`,
			Observations: []*StoreObs{
				{ObserverID: "obs1", Timestamp: obsTime},
			},
		}
		relayTxs = append(relayTxs, tx)
	}

	// BROKENORIG's adverts: skew ~+100,500s (matches the #1816 band).
	// These are indexed under RELAY001 too (simulating indexResolvedPathHops
	// adding the transmission under the relay-hop pubkey), but their
	// decoded pubKey is BROKENORIG, not RELAY001.
	for i := 0; i < 5; i++ {
		obsTime := baseObs.Add(time.Duration(100+i) * 5 * time.Minute)
		advTS := obsTime.Add(100500 * time.Second).Unix()
		tx := &StoreTx{
			Hash:        "relayed-foreign-" + formatInt64(int64(i)),
			PayloadType: &pt,
			DecodedJSON: `{"payload":{"timestamp":` + formatInt64(advTS) + `},"pubKey":"BROKENORIG"}`,
			Observations: []*StoreObs{
				{ObserverID: "obs1", Timestamp: obsTime},
			},
		}
		relayTxs = append(relayTxs, tx)
	}

	ps.mu.Lock()
	ps.byNode["RELAY001"] = relayTxs
	for _, tx := range relayTxs {
		ps.byPayloadType[4] = append(ps.byPayloadType[4], tx)
	}
	ps.clockSkew.computeInterval = 0
	ps.mu.Unlock()

	r := ps.GetNodeClockSkew("RELAY001")
	if r == nil {
		t.Fatal("expected clock skew result for RELAY001")
	}
	if r.Severity != types.SkewOK {
		t.Errorf("severity = %v, want ok — relay's own adverts are healthy; "+
			"relayed foreign adverts must not be attributed to it", r.Severity)
	}
	if abs(r.RecentMedianSkewSec) > 5 {
		t.Errorf("recentMedianSkewSec = %v, want ~0 (only RELAY001's own adverts "+
			"should count, not the relayed +100.5k s originator adverts)", r.RecentMedianSkewSec)
	}
	// Sample count must reflect only the 5 self-originated adverts.
	if r.RecentSampleCount != 5 {
		t.Errorf("recentSampleCount = %d, want 5 (relayed adverts must be excluded)", r.RecentSampleCount)
	}
}

// ── #1816: bit-identical clusters across relays disappear ─────────────────────

// Five distinct relay pubkeys never self-advert at all — they only forward
// a single broken-clock originator's advert. Before the fix, each relay's
// GetNodeClockSkew would report the SAME bit-identical skew as the
// originator (a false "cluster"). After the fix, a node with zero
// self-originated adverts must report no clock-skew data at all (nil),
// since it never actually said anything about its own clock.
func TestIssue1816_PureRelaysReportNoSkew_NoBitIdenticalCluster(t *testing.T) {
	ps := NewPacketStore(nil, nil)
	pt := types.PayloadADVERT
	baseObs := time.Unix(1700000000, 0).UTC()

	const originator = "BROKENORIG2"
	obsTime := baseObs
	advTS := obsTime.Add(201000 * time.Second).Unix() // matches the #1816 report's +201k s band

	originatorTx := &StoreTx{
		Hash:        "originator-advert-1",
		PayloadType: &pt,
		DecodedJSON: `{"payload":{"timestamp":` + formatInt64(advTS) + `},"pubKey":"` + originator + `"}`,
		Observations: []*StoreObs{
			{ObserverID: "obs1", Timestamp: obsTime},
		},
	}

	relayKeys := []string{"RLY-A", "RLY-B", "RLY-C", "RLY-D", "RLY-E"}

	ps.mu.Lock()
	// The originator's own byNode entry (legitimate — it self-signed this advert).
	ps.byNode[originator] = []*StoreTx{originatorTx}
	// Each relay is ALSO indexed under this same transmission (as
	// indexResolvedPathHops does for every relay hop in the resolved path),
	// but none of them ever self-advert.
	for _, rk := range relayKeys {
		ps.byNode[rk] = []*StoreTx{originatorTx}
	}
	ps.byPayloadType[4] = []*StoreTx{originatorTx}
	ps.clockSkew.computeInterval = 0
	ps.mu.Unlock()

	// The originator itself should still report its own (bad) skew.
	origResult := ps.GetNodeClockSkew(originator)
	if origResult == nil {
		t.Fatal("expected clock skew result for the originator")
	}
	if abs(origResult.RecentMedianSkewSec-201000) > 5 {
		t.Errorf("originator recentMedianSkewSec = %v, want ~201000", origResult.RecentMedianSkewSec)
	}

	// None of the pure relays ever said anything about their own clock —
	// they must report nil, not a copy of the originator's skew.
	for _, rk := range relayKeys {
		r := ps.GetNodeClockSkew(rk)
		if r != nil {
			t.Errorf("relay %s: expected nil (no self-originated adverts), "+
				"got skew=%v — this is the bit-identical false cluster from #1816",
				rk, r.RecentMedianSkewSec)
		}
	}
}

// ── #1818: two foreign adverts must not poison an otherwise-healthy node ──────

// Reproduces the cwichura "island" report: a node's own adverts are all
// healthy (~0s skew), but two adverts from a broken-clock originator
// (skew ~10 days) are present in its byNode entry — exactly the volume
// described in the issue ("Only 2 adverts out of all the adverts seen are
// from other nodes"). Because getNodeClockSkewLocked sorts by time and
// takes only the last recentSkewWindowCount samples, even 2 catastrophic
// foreign samples can dominate the tail of a small window. After the fix,
// they must be excluded entirely since they aren't self-originated.
func TestIssue1818_TwoForeignAdvertsDoNotPoisonIslandNode(t *testing.T) {
	ps := NewPacketStore(nil, nil)
	pt := types.PayloadType(4) // ADVERT
	baseObs := time.Unix(1700000000, 0).UTC()

	const island = "ISLAND001"
	var txs []*StoreTx

	// Plenty of healthy self-adverts (island node mostly talks to itself).
	for i := 0; i < 8; i++ {
		obsTime := baseObs.Add(time.Duration(i) * 5 * time.Minute)
		advTS := obsTime.Add(-1 * time.Second).Unix() // ~0s skew
		tx := &StoreTx{
			Hash:        "island-self-" + formatInt64(int64(i)),
			PayloadType: &pt,
			DecodedJSON: `{"payload":{"timestamp":` + formatInt64(advTS) + `},"pubKey":"` + island + `"}`,
			Observations: []*StoreObs{
				{ObserverID: "obs1", Timestamp: obsTime},
			},
		}
		txs = append(txs, tx)
	}

	// Two foreign adverts from a broken-clock originator, landing at the
	// tail of the recent window (most recent timestamps), skew ~10 days —
	// matching the report's "off by nearly ten days".
	for i := 0; i < 2; i++ {
		obsTime := baseObs.Add(time.Duration(100+i) * 5 * time.Minute)
		advTS := obsTime.Add(10 * 24 * time.Hour).Unix()
		tx := &StoreTx{
			Hash:        "island-foreign-" + formatInt64(int64(i)),
			PayloadType: &pt,
			DecodedJSON: `{"payload":{"timestamp":` + formatInt64(advTS) + `},"pubKey":"OFFCLOCKORIG"}`,
			Observations: []*StoreObs{
				{ObserverID: "obs1", Timestamp: obsTime},
			},
		}
		txs = append(txs, tx)
	}

	ps.mu.Lock()
	ps.byNode[island] = txs
	for _, tx := range txs {
		ps.byPayloadType[4] = append(ps.byPayloadType[4], tx)
	}
	ps.clockSkew.computeInterval = 0
	ps.mu.Unlock()

	r := ps.GetNodeClockSkew(island)
	if r == nil {
		t.Fatal("expected clock skew result for island node")
	}
	if r.Severity == types.SkewNoClock || r.Severity == types.SkewBimodalClock {
		t.Errorf("severity = %v, want ok/warning — the node's own adverts are "+
			"healthy; the two foreign adverts must not be attributed to it (#1818)",
			r.Severity)
	}
	if abs(r.RecentMedianSkewSec) > 5 {
		t.Errorf("recentMedianSkewSec = %v, want ~0 (foreign adverts excluded)", r.RecentMedianSkewSec)
	}
	// recentSkewWindowCount caps the window at 5 most-recent samples; with
	// the 2 foreign adverts excluded, those 5 must all come from the
	// island node's own healthy stream.
	if r.RecentSampleCount != recentSkewWindowCount {
		t.Errorf("recentSampleCount = %d, want %d (only self-originated adverts, "+
			"windowed)", r.RecentSampleCount, recentSkewWindowCount)
	}
}

// formatInt64 is a test helper to format int64 as string for JSON embedding.
func formatInt64(n int64) string {
	return fmt.Sprintf("%d", n)
}
