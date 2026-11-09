package main

import (
	"context"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/gorilla/mux"
)

// coverageRow is one raw reception read from client_receptions.
type coverageRow struct {
	Lat, Lon float64
	SNR      *float64
	RSSI     *int
	HeardKey string // directly-heard node key (2-3 byte prefix or full pubkey), lowercase
	RxAt     string // reception time (RFC3339); used to pick the latest SNR per node
}

// coverageFeatureCap bounds the number of hex cells returned in one response.
// A wide bbox at high zoom over the 30-day window could otherwise emit multi-MB
// GeoJSON; when more cells exist the densest are kept and Truncated is set (#12).
const coverageFeatureCap = 5000

// coverageCellNodeCap bounds the per-cell node breakdown shipped on the wire
// (the client only renders the top ~10). NodesTruncated flags that more were
// heard than returned (#11).
const coverageCellNodeCap = 25

type covAgg struct {
	count   int
	bestSNR *float64
	hasSig  bool
	nodes   map[string]*covNodeAgg
}

// covNodeAgg tracks, per directly-heard node within a cell, its reception count and
// the SNR of its most recent reception (by rx_at). name/prefix are the resolved node
// name (when known) and a display prefix fallback. nameKeyLen records the heard_key
// length that set the current name, so the chosen identity is the most specific one
// regardless of row order (#20).
type covNodeAgg struct {
	count      int
	latestAt   string
	latestSNR  *float64
	name       string
	nameKeyLen int
	prefix     string
}

// nodeResolver maps a heard_key (2-3 byte prefix or full pubkey) to a canonical
// identity key and a display name. A unique match returns (pubkey, name) so the same
// node heard under different prefix lengths collapses into one bucket; unknown or
// ambiguous keys return (heardKey, "") and stay distinct. nil disables resolution.
type nodeResolver func(heardKey string) (key, name string)

// aggregateCoverage bins raw rows into display-resolution hex cells, keeping the
// best (max) SNR per cell, and emits GeoJSON polygons. resolve (may be nil) collapses
// per-node receptions by resolved node identity.
func aggregateCoverage(rows []coverageRow, res int, resolve nodeResolver) api.CoverageFeatureCollection {
	byCell := map[string]*covAgg{}
	for _, row := range rows {
		cell := hexCellAt(row.Lat, row.Lon, res)
		a := byCell[cell]
		if a == nil {
			a = &covAgg{}
			byCell[cell] = a
		}
		a.count++
		if row.SNR != nil {
			a.hasSig = true
			if a.bestSNR == nil || *row.SNR > *a.bestSNR {
				v := *row.SNR
				a.bestSNR = &v
			}
		}
		if row.HeardKey != "" {
			if a.nodes == nil {
				a.nodes = map[string]*covNodeAgg{}
			}
			key, name := row.HeardKey, ""
			if resolve != nil {
				if k, n := resolve(row.HeardKey); k != "" {
					key, name = k, n
				}
			}
			na := a.nodes[key]
			if na == nil {
				na = &covNodeAgg{prefix: row.HeardKey}
				a.nodes[key] = na
			}
			// Lock the display identity to the MOST SPECIFIC (longest) heard_key
			// that resolved to a non-empty name, tie-broken lexicographically, so
			// the name no longer flaps with row/map order (#20). A full-pubkey
			// reception thus outranks a short-prefix one for the same node.
			if name != "" && (na.name == "" || len(row.HeardKey) > na.nameKeyLen ||
				(len(row.HeardKey) == na.nameKeyLen && name < na.name)) {
				na.name = name
				na.nameKeyLen = len(row.HeardKey)
			}
			// Display-prefix fallback (shown when name is empty): same precedence so
			// it is also order-independent.
			if len(row.HeardKey) > len(na.prefix) ||
				(len(row.HeardKey) == len(na.prefix) && row.HeardKey < na.prefix) {
				na.prefix = row.HeardKey
			}
			na.count++
			// rx_at is RFC3339, so lexical >= is chronological; keep the latest
			// SNR. The first row always wins (latestAt starts "", and any value
			// >= ""), so no separate count==1 guard is needed.
			if row.RxAt >= na.latestAt {
				na.latestAt = row.RxAt
				na.latestSNR = row.SNR
			}
		}
	}
	fc := api.CoverageFeatureCollection{Type: "FeatureCollection", Features: []api.CoverageFeature{}}
	for cell, a := range byCell {
		ring := hexBoundary(cell)
		if ring == nil {
			continue
		}
		nodes, nodesTrunc := sortedCoverageNodes(a.nodes)
		positions := make(api.CoverageRing, len(ring))
		for i, pt := range ring {
			positions[i] = api.GeoPosition{pt[0], pt[1]}
		}
		props := api.CoverageProperties{
			Cell: cell, Count: a.count, BestSnr: a.bestSNR, HasSig: a.hasSig, Nodes: nodes,
		}
		if nodesTrunc {
			props.NodesTruncated = &nodesTrunc
		}
		fc.Features = append(fc.Features, api.CoverageFeature{
			Type:       "Feature",
			Geometry:   api.CoveragePolygon{Type: "Polygon", Coordinates: []api.CoverageRing{positions}},
			Properties: props,
		})
	}
	// Bound the response: when more cells exist than coverageFeatureCap, keep the
	// densest (highest count) and flag the truncation, so a wide/zoomed-out query
	// can't emit unbounded multi-MB GeoJSON (#12).
	if len(fc.Features) > coverageFeatureCap {
		sort.Slice(fc.Features, func(i, j int) bool {
			ci, cj := fc.Features[i].Properties.Count, fc.Features[j].Properties.Count
			if ci != cj {
				return ci > cj // densest first
			}
			return fc.Features[i].Properties.Cell < fc.Features[j].Properties.Cell // deterministic tie-break
		})
		fc.Features = fc.Features[:coverageFeatureCap]
		truncated := true
		fc.Truncated = &truncated
	}
	// Map iteration is randomized, so sort features by cell for a deterministic
	// payload — stable ETag/caching and a non-flaky "first feature" in e2e (#8).
	sort.Slice(fc.Features, func(i, j int) bool {
		return fc.Features[i].Properties.Cell < fc.Features[j].Properties.Cell
	})
	return fc
}

// sortedCoverageNodes flattens the per-node aggregates into a slice sorted by latest
// SNR descending (nodes heard without a signal sort last), tie-broken by count then
// prefix for a stable order. The slice is capped at coverageCellNodeCap; truncated
// reports whether more nodes were heard in the cell than returned (#11).
func sortedCoverageNodes(m map[string]*covNodeAgg) (nodes []api.CoverageNode, truncated bool) {
	out := make([]api.CoverageNode, 0, len(m))
	for _, na := range m {
		out = append(out, api.CoverageNode{Prefix: na.prefix, Name: optString(na.name), Snr: na.latestSNR, Count: na.count})
	}
	sort.Slice(out, func(i, j int) bool {
		si, sj := out[i].Snr, out[j].Snr
		if (si == nil) != (sj == nil) {
			return si != nil // signal before no-signal
		}
		if si != nil && *si != *sj {
			return *si > *sj
		}
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Prefix < out[j].Prefix
	})
	if len(out) > coverageCellNodeCap {
		return out[:coverageCellNodeCap], true
	}
	return out, false
}

type bbox struct{ MinLat, MinLon, MaxLat, MaxLon float64 }

// coverageHeardKeyCandidates returns the exact heard_key values that identify a
// node: its full pubkey (stored with heard_keylen 32) and the 2-byte (4 hex) and
// 3-byte (6 hex) prefixes a relay logs. Matching heard_key IN (these) is
// equivalent to the old "heard_keylen=32 AND heard_key=? OR heard_keylen IN (2,3)
// AND substr(?,1,keylen*2)=heard_key", but sargable — so the (heard_key, …)
// composite index seeks the few matching rows instead of scanning the bbox (#5).
func coverageHeardKeyCandidates(pubkey string) []string {
	pk := strings.ToLower(pubkey)
	seen := map[string]bool{}
	out := make([]string, 0, 3)
	for _, c := range []string{pk, prefixOrEmpty(pk, 6), prefixOrEmpty(pk, 4)} {
		if c != "" && !seen[c] {
			seen[c] = true
			out = append(out, c)
		}
	}
	return out
}

func prefixOrEmpty(s string, n int) string {
	if len(s) >= n {
		return s[:n]
	}
	return ""
}

func (s *Server) queryCoverageRows(ctx context.Context, pubkey string, b bbox) ([]coverageRow, error) {
	rows, err := s.db.ListCoverageRows(ctx, database.CoverageQuery{
		HeardKeys: coverageHeardKeyCandidates(pubkey),
		MinLat:    b.MinLat, MaxLat: b.MaxLat, MinLon: b.MinLon, MaxLon: b.MaxLon,
	})
	if err != nil {
		return nil, err
	}
	out := make([]coverageRow, 0, len(rows))
	for _, r := range rows {
		// formatTS keeps the fixed-width UTC string the aggregation compares.
		out = append(out, coverageRow{Lat: r.Lat, Lon: r.Lon, SNR: r.SNR, RSSI: r.RSSI, HeardKey: r.HeardKey, RxAt: formatTS(r.RxAt)})
	}
	return out, nil
}

// mobileRxStats returns the node-wide reception and distinct-client totals.
func (s *Server) mobileRxStats(ctx context.Context, pubkey string) (count, clients int, err error) {
	return s.db.CountMobileReceptions(ctx, coverageHeardKeyCandidates(pubkey))
}

// zoomToHexRes maps a Leaflet zoom level to the display resolution used for hex
// binning. Resolution == zoom (clamped to a sane range) so hex size tracks the map
// scale 1:1 and renders at a constant ~hexTargetPx (see hexSizeForRes). The clamp also
// guards the missing-param case (z parses to 0).
func zoomToHexRes(z int) int {
	switch {
	case z < 3:
		return 3
	case z > 18:
		return 18
	default:
		return z
	}
}

func parseBBox(s string) (bbox, bool) {
	p := strings.Split(s, ",")
	if len(p) != 4 {
		return bbox{}, false
	}
	v := make([]float64, 4)
	for i := range p {
		f, err := strconv.ParseFloat(strings.TrimSpace(p[i]), 64)
		if err != nil {
			return bbox{}, false
		}
		v[i] = f
	}
	return bbox{MinLat: v[0], MinLon: v[1], MaxLat: v[2], MaxLon: v[3]}, true
}

// handleNodeRxCoverage serves per-node mobile RX coverage as a GeoJSON hex grid.
func (s *Server) handleNodeRxCoverage(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if !s.requireClientRxCoverage(w, r) {
		return
	}
	pubkey := strings.ToLower(mux.Vars(r)["pubkey"])
	// Mirror handleNodeReach's gate at this same {pubkey}: reject malformed keys,
	// and 404 blacklisted / hidden-prefix nodes. Hiding only the node *name* (via
	// heardKeyResolver) still leaked the GPS hex bins and mobile_receptions /
	// mobile_clients counts for a node the rest of the API hides (#1727 r2).
	if !isHexPubkey(pubkey) {
		writeError(w, http.StatusBadRequest, "invalid pubkey: expected 64 hex chars")
		return
	}
	if (s.cfg != nil && s.cfg.IsBlacklisted(pubkey)) || s.isPubkeyHidden(ctx, pubkey) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	b, ok := parseBBox(r.URL.Query().Get("bbox"))
	if !ok {
		writeError(w, http.StatusBadRequest, "bbox required as minLat,minLon,maxLat,maxLon")
		return
	}
	if s.db == nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable")
		return
	}
	z, _ := strconv.Atoi(r.URL.Query().Get("z"))
	rows, err := s.queryCoverageRows(ctx, pubkey, b)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "query failed")
		return
	}
	fc := aggregateCoverage(rows, zoomToHexRes(z), s.heardKeyResolverFor(ctx, rows))
	// Attach the node-wide reception/contributor totals (#3): the bbox limits the
	// hex features to the current view, but these summarise all of this node's
	// mobile coverage so the UI can show "heard by N clients" regardless of pan.
	count, clients, err := s.mobileRxStats(r.Context(), pubkey)
	if err != nil {
		log.Printf("[coverage] mobile stats for %s: %v", pubkey, err)
	}
	// Absent when zero (the wire format has always omitted them).
	if count != 0 {
		fc.MobileReceptions = &count
	}
	if clients != 0 {
		fc.MobileClients = &clients
	}
	writeJSON(w, fc)
}
