package main

import (
	"context"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
)

// heardKeyResolverFor builds a nodeResolver for exactly the distinct heard_keys
// present in rows, resolving them all in one batched query instead of one query
// per key (the previous per-key resolver was N+1 on the read connection). Maps a
// heard_key to (pubkey, name) on a unique, non-hidden match; to (heardKey, "")
// otherwise. nil when there's no DB.
func (s *Server) heardKeyResolverFor(ctx context.Context, rows []coverageRow) nodeResolver {
	if s.db == nil {
		return nil
	}
	keys := make([]string, 0, len(rows))
	seen := map[string]bool{}
	for _, r := range rows {
		if r.HeardKey != "" && !seen[r.HeardKey] {
			seen[r.HeardKey] = true
			keys = append(keys, r.HeardKey)
		}
	}
	resolved := s.batchResolveHeardKeys(ctx, keys)
	return func(heardKey string) (string, string) {
		if v, ok := resolved[heardKey]; ok {
			return v[0], v[1]
		}
		return heardKey, ""
	}
}

// batchResolveHeardKeys resolves many heard_keys (2-3 byte prefixes or full
// pubkeys) to their canonical (pubkey, name) in a single round-trip per chunk: a
// UNION ALL of one LIMIT-2 prefix lookup each, so per-key work stays bounded
// (2 rows) and the whole set costs one query, not N. A unique match returns
// [pubkey, name]; unknown / ambiguous / blacklisted / hidden-prefix keys (#15,
// #1181) return [heardKey, ""].
func (s *Server) batchResolveHeardKeys(ctx context.Context, keys []string) map[string][2]string {
	res := make(map[string][2]string, len(keys))
	valid := make([]string, 0, len(keys))
	seen := map[string]bool{}
	for _, k := range keys {
		if k == "" || seen[k] {
			continue
		}
		seen[k] = true
		if !hexPrefixRe.MatchString(k) {
			res[k] = [2]string{k, ""}
			continue
		}
		valid = append(valid, strings.ToLower(k)) // LIKE is case-sensitive in Postgres
	}
	if len(valid) == 0 {
		return res
	}

	matches, err := s.db.MatchNodesByPrefix(ctx, valid)
	if err != nil {
		// Don't fail the request, but don't fail silently either: a swallowed
		// error here presents as "every name is ambiguous" with no signal.
		log.Printf("WARN batchResolveHeardKeys: %v", err)
		for _, k := range keys {
			if k != "" {
				res[k] = [2]string{k, ""}
			}
		}
		return res
	}
	for _, k := range keys {
		if k == "" || res[k] != ([2]string{}) {
			continue
		}
		m, ok := matches[strings.ToLower(k)]
		if ok && m.Matches == 1 && !s.cfg.IsBlacklisted(m.Pubkey) && !s.cfg.IsNameHidden(m.Name) {
			res[k] = [2]string{m.Pubkey, m.Name}
		} else {
			res[k] = [2]string{k, ""}
		}
	}
	return res
}

// resolveHeardKey resolves a single heard_key (2-3 byte prefix or full pubkey)
// to its canonical (pubkey, name), or (heardKey, "") when unknown / ambiguous /
// hidden. Thin wrapper over batchResolveHeardKeys so there is one code path.
func (s *Server) resolveHeardKey(ctx context.Context, heardKey string) (string, string) {
	v := s.batchResolveHeardKeys(ctx, []string{heardKey})[heardKey]
	if v[0] == "" && v[1] == "" {
		return heardKey, "" // empty/unresolved → echo the key
	}
	return v[0], v[1]
}

func (s *Server) coverageRows(ctx context.Context, q database.CoverageQuery) ([]coverageRow, error) {
	rows, err := s.db.ListCoverageRows(ctx, q)
	if err != nil {
		return nil, err
	}
	out := make([]coverageRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, coverageRow{Lat: r.Lat, Lon: r.Lon, SNR: r.SNR, RSSI: r.RSSI, HeardKey: r.HeardKey, RxAt: formatTS(r.RxAt)})
	}
	return out, nil
}

// queryCoverageFiltered returns coverage rows within a bbox, optionally filtered
// by heard node (prefix/pubkey), contributing client (rx_pubkey), and time window
// (days; 0 = all time). Powers the global and per-observer coverage maps.
func (s *Server) queryCoverageFiltered(ctx context.Context, node, rx string, days int, b bbox) ([]coverageRow, error) {
	q := database.CoverageQuery{MinLat: b.MinLat, MaxLat: b.MaxLat, MinLon: b.MinLon, MaxLon: b.MaxLon}
	if node != "" {
		q.HeardKeys = coverageHeardKeyCandidates(node)
	}
	if rx != "" {
		q.RxPubkey = rx
	}
	if days > 0 {
		t := time.Now().UTC().AddDate(0, 0, -days)
		q.Since = &t
	}
	return s.coverageRows(ctx, q)
}

// handleRxCoverage serves global (or per-observer via ?rx=) coverage as GeoJSON
// hexbins, over a time window. ?node= also works (same as the per-node endpoint).
// requireClientRxCoverage writes a 404 and returns false when the opt-in
// client-RX coverage feature is disabled, so the coverage endpoints read as
// "not found" instead of serving data on deployments that haven't enabled it.
func (s *Server) requireClientRxCoverage(w http.ResponseWriter, r *http.Request) bool {
	// Routes are registered unconditionally, so guard against a nil server/cfg
	// (e.g. handlers exercised in isolation) rather than panicking (#4).
	// ClientRxCoverageEnabled is itself nil-receiver-safe.
	if s == nil || s.cfg == nil || !s.cfg.ClientRxCoverageEnabled() {
		writeError(w, http.StatusNotFound, "not found")
		return false
	}
	return true
}

func (s *Server) handleRxCoverage(w http.ResponseWriter, r *http.Request) {
	if !s.requireClientRxCoverage(w, r) {
		return
	}
	b, ok := parseBBox(r.URL.Query().Get("bbox"))
	if !ok {
		writeError(w, http.StatusBadRequest, "bbox required as minLat,minLon,maxLat,maxLon")
		return
	}
	days := clampDays(atoiDefault(r.URL.Query().Get("days"), 7))
	z, _ := strconv.Atoi(r.URL.Query().Get("z"))
	rows, err := s.queryCoverageFiltered(r.Context(), r.URL.Query().Get("node"), r.URL.Query().Get("rx"), days, b)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "query failed")
		return
	}
	fc := aggregateCoverage(rows, zoomToHexRes(z), s.heardKeyResolverFor(r.Context(), rows))
	writeJSON(w, fc)
}

// --- Leaderboard (top mobile observers) ---

// leaderboardHexRes is the fixed hex resolution used to bucket receptions into
// "cells visited" for the frontier-weighted score. ~150 m ground cells at our
// latitude: coarse enough that a parked node's GPS jitter stays in one cell,
// fine enough that real driving paints many. Independent of the coverage map's
// zoom-dependent render resolution so the ranking is stable across views.
const leaderboardHexRes = 13

// leaderboardScanCap bounds how many rows the leaderboard aggregates in memory.
// The endpoint is unauthenticated (only requireClientRxCoverage), and the Go-side
// rarity weighting can't push the GROUP BY into SQLite, so without a cap a wide
// window on a busy network would stream the whole table into maps. At the cap we
// log and return a partial (best-effort) ranking rather than OOM (#review r2).
const leaderboardScanCap = 500000

// rxLeaderboard ranks mobile observers by frontier-weighted cell coverage over
// the time window. Each distinct cell an observer covers contributes
// 1/(observers covering that cell): a cell only they reached weighs 1.0, a cell
// shared by N observers weighs 1/N. This rewards expanding the map's edge and is
// spam-proof — a stationary node covers exactly one cell regardless of how many
// receptions it logs. Bucketing + the rarity weight can't be expressed in SQL,
// so we aggregate the window's rows in Go (bounded by leaderboardScanCap).
func (s *Server) rxLeaderboard(ctx context.Context, days, limit int) ([]api.RxLeaderboardObserver, error) {
	since := time.Now().UTC().AddDate(0, 0, -days)

	type agg struct {
		name       string
		receptions int
		cells      map[string]struct{}
		nodes      map[string]struct{}
	}
	obsAgg := map[string]*agg{}
	cellObservers := map[string]map[string]struct{}{} // cell -> set of rx_pubkey

	samples, err := s.db.ListLeaderboardSamples(ctx, since, leaderboardScanCap)
	if err != nil {
		return nil, err
	}
	if len(samples) >= leaderboardScanCap {
		log.Printf("[rx-leaderboard] scan hit cap %d over %dd window; ranking is partial (most-recent rows)", leaderboardScanCap, days)
	}
	for _, r := range samples {
		a := obsAgg[r.RxPubkey]
		if a == nil {
			a = &agg{name: r.Name, cells: map[string]struct{}{}, nodes: map[string]struct{}{}}
			obsAgg[r.RxPubkey] = a
		}
		a.receptions++
		a.nodes[r.HeardKey] = struct{}{}
		cell := hexCellAt(r.Lat, r.Lon, leaderboardHexRes)
		a.cells[cell] = struct{}{}
		set := cellObservers[cell]
		if set == nil {
			set = map[string]struct{}{}
			cellObservers[cell] = set
		}
		set[r.RxPubkey] = struct{}{}
	}

	// Per-cell observer counts EXCLUDING blacklisted contributors, so an operator
	// of a blacklisted node parked in a cell can't silently dilute everyone else's
	// frontier weight (#review r2). Name-hidden (not blacklisted) observers are
	// legitimate contributors and still count.
	cellCount := make(map[string]int, len(cellObservers))
	for cell, set := range cellObservers {
		n := 0
		for pk := range set {
			if !s.cfg.IsObserverBlacklisted(pk) && !s.cfg.IsBlacklisted(pk) {
				n++
			}
		}
		cellCount[cell] = n
	}

	out := make([]api.RxLeaderboardObserver, 0, len(obsAgg))
	for pk, a := range obsAgg {
		var score float64
		for cell := range a.cells {
			if c := cellCount[cell]; c > 0 {
				score += 1.0 / float64(c)
			}
		}
		out = append(out, api.RxLeaderboardObserver{
			Pubkey:     pk,
			Name:       a.name,
			Receptions: a.receptions,
			Nodes:      len(a.nodes),
			Cells:      len(a.cells),
			Score:      score,
		})
	}

	// Rank by frontier score; ties broken by raw receptions then pubkey so the
	// order is deterministic (keeps same-location fixtures stable).
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		if out[i].Receptions != out[j].Receptions {
			return out[i].Receptions > out[j].Receptions
		}
		return out[i].Pubkey < out[j].Pubkey
	})

	// Identity hiding parity (#1727 r2): drop observer-blacklisted contributors,
	// blank node-blacklisted / hidden-prefix names, cap at limit. nil cfg ⇒ no-ops.
	filtered := make([]api.RxLeaderboardObserver, 0, limit)
	for _, o := range out {
		if s.cfg.IsObserverBlacklisted(o.Pubkey) {
			continue
		}
		if s.cfg.IsBlacklisted(o.Pubkey) || s.cfg.IsNameHidden(o.Name) {
			o.Name = ""
		}
		filtered = append(filtered, o)
		if len(filtered) >= limit {
			break
		}
	}
	return filtered, nil
}

func (s *Server) handleRxLeaderboard(w http.ResponseWriter, r *http.Request) {
	if !s.requireClientRxCoverage(w, r) {
		return
	}
	days := clampDays(atoiDefault(r.URL.Query().Get("days"), 7))
	limit := atoiDefault(r.URL.Query().Get("limit"), 20)
	if limit < 1 || limit > 100 {
		limit = 20
	}
	obs, err := s.rxLeaderboard(r.Context(), days, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "query failed")
		return
	}
	writeJSON(w, api.RxLeaderboardResponse{Days: days, Observers: obs})
}

func atoiDefault(s string, d int) int {
	if n, err := strconv.Atoi(strings.TrimSpace(s)); err == nil {
		return n
	}
	return d
}
