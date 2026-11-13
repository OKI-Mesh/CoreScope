package database

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// Route lists for the scope queries. Transport routes can carry transport_code_1;
// flood routes append hops (zero-hop adverts go out as DIRECT and are not flooded).
var (
	transportRoutes    = []int32{int32(types.RouteTransportFlood), int32(types.RouteTransportDirect)}
	nonTransportRoutes = []int32{int32(types.RouteFlood), int32(types.RouteDirect)}
	floodRoutes        = []int32{int32(types.RouteTransportFlood), int32(types.RouteFlood)}
)

// DeclaredRegions is the newest declared-regions answer for one target.
// RegionsCSV is stored and returned verbatim: region names are case sensitive,
// and MeshCore treats "#cvg" and "cvg" as the same region, so callers compare
// names by their key with the leading "#" removed.
type DeclaredRegions struct {
	Target     string
	ObservedAt time.Time // zero when the source has no timestamp
	RegionsCSV string
	Truncated  bool
}

// ScopeMatchTotals is the persisted scope-match tally: one row, cumulative
// since Since.
type ScopeMatchTotals struct {
	Since               time.Time
	Unique              int64
	ExplicitOverDerived int64
	Ambiguous           int64
	None                int64
}

type ScopeSummary struct {
	TransportTotal int64
	Scoped         int64 // transport transmissions with a scope name, including UnknownScope
	Unscoped       int64 // includes every non-transport transmission (#1838)
	UnknownScope   int64 // scoped, but the name did not match a known region
}

type ScopeRegionCount struct {
	Name  string
	Count int64
}

type ScopeTimePoint struct {
	T        time.Time // bucket start, UTC
	Scoped   int64
	Unscoped int64
}

type ScopeAdvertRoleCount struct {
	Role         string
	Unscoped     int64
	UnknownScope int64
	Named        int64
}

type ScopeStats struct {
	Summary       ScopeSummary
	ByRegion      []ScopeRegionCount
	TimeSeries    []ScopeTimePoint
	AdvertsByRole []ScopeAdvertRoleCount
}

// GetScopeMatchTotals returns the stored row, or (nil, nil) when there is none
// yet.
func (db *DB) GetScopeMatchTotals(ctx context.Context) (*ScopeMatchTotals, error) {
	r, err := db.q.GetScopeMatchTotals(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get scope match totals: %w", err)
	}
	return &ScopeMatchTotals{
		Since:               r.CountingSince.UTC(),
		Unique:              r.UniqueMatches,
		ExplicitOverDerived: r.ExplicitOverDerived,
		Ambiguous:           r.Ambiguous,
		None:                r.NoneMatches,
	}, nil
}

// SaveScopeMatchTotals upserts the single row. now is the updated_at stamp.
func (db *DB) SaveScopeMatchTotals(ctx context.Context, t ScopeMatchTotals, now time.Time) error {
	err := db.q.UpsertScopeMatchTotals(ctx, pgdb.UpsertScopeMatchTotalsParams{
		CountingSince:       t.Since,
		UniqueMatches:       t.Unique,
		ExplicitOverDerived: t.ExplicitOverDerived,
		Ambiguous:           t.Ambiguous,
		NoneMatches:         t.None,
		UpdatedAt:           now,
	})
	if err != nil {
		return fmt.Errorf("save scope match totals: %w", err)
	}
	return nil
}

// ListConfiguredScopes returns the region sets that nodes.configured_scope
// holds, one row per node that has one, with the node's public key as Target.
// Truncated is always false: the /neighbors report has a size cap but does not
// say whether it fired, so claiming false would invent a fact only in the sense
// of "not flagged".
func (db *DB) ListConfiguredScopes(ctx context.Context) ([]DeclaredRegions, error) {
	rows, err := db.q.ListConfiguredScopes(ctx)
	if err != nil {
		return nil, fmt.Errorf("list configured scopes: %w", err)
	}
	out := make([]DeclaredRegions, len(rows))
	for i, r := range rows {
		var at time.Time
		if p := fromPgTimestamptz(r.ConfiguredScopeAt); p != nil {
			at = p.UTC()
		}
		out[i] = DeclaredRegions{Target: r.PublicKey, RegionsCSV: r.ConfiguredScope, ObservedAt: at}
	}
	return out, nil
}

// ListLatestDeclaredRegions returns the newest answer per target from
// node_declared_regions.
func (db *DB) ListLatestDeclaredRegions(ctx context.Context) ([]DeclaredRegions, error) {
	rows, err := db.q.ListLatestDeclaredRegions(ctx)
	if err != nil {
		return nil, fmt.Errorf("list latest declared regions: %w", err)
	}
	out := make([]DeclaredRegions, len(rows))
	for i, r := range rows {
		out[i] = DeclaredRegions{
			Target:     r.Target,
			RegionsCSV: r.RegionsCsv,
			ObservedAt: r.ObservedAt.UTC(),
			Truncated:  r.Truncated,
		}
	}
	return out, nil
}

// AllCurrentDeclaredRegions merges configured scopes and declared answers into
// one row per node, keyed by lowercase target because node_declared_regions is
// filled by an external collector whose casing is not controlled. The newer
// ObservedAt wins and a tie keeps the configured scope. Rows without a target
// are dropped. The result is sorted by target and is never nil.
func (db *DB) AllCurrentDeclaredRegions(ctx context.Context) ([]DeclaredRegions, error) {
	configured, err := db.ListConfiguredScopes(ctx)
	if err != nil {
		return nil, err
	}
	latest, err := db.ListLatestDeclaredRegions(ctx)
	if err != nil {
		return nil, err
	}

	byTarget := make(map[string]DeclaredRegions, len(configured)+len(latest))
	for _, r := range configured {
		r.Target = normalizeKey(r.Target)
		if r.Target == "" {
			continue
		}
		byTarget[r.Target] = r
	}
	for _, r := range latest {
		r.Target = normalizeKey(r.Target)
		if r.Target == "" {
			continue
		}
		if cur, ok := byTarget[r.Target]; ok && !r.ObservedAt.After(cur.ObservedAt) {
			continue
		}
		byTarget[r.Target] = r
	}

	out := make([]DeclaredRegions, 0, len(byTarget))
	for _, r := range byTarget {
		out = append(out, r)
	}
	slices.SortFunc(out, func(a, b DeclaredRegions) int { return cmp.Compare(a.Target, b.Target) })
	return out, nil
}

// GetScopeStats returns scope statistics for transmissions first seen at or after since.
// bucket is the time-series bucket width: a whole number of seconds, at least one.
func (db *DB) GetScopeStats(ctx context.Context, since time.Time, bucket time.Duration) (*ScopeStats, error) {
	if bucket < time.Second || bucket%time.Second != 0 || bucket/time.Second > math.MaxInt32 {
		return nil, fmt.Errorf("scope stats: bucket %v must be a whole number of seconds, at least one", bucket)
	}

	sum, err := db.q.GetScopeSummary(ctx, pgdb.GetScopeSummaryParams{
		TransportRoutes:    transportRoutes,
		NonTransportRoutes: nonTransportRoutes,
		Since:              since,
	})
	if err != nil {
		return nil, fmt.Errorf("scope summary query: %w", err)
	}
	st := &ScopeStats{
		Summary: ScopeSummary{
			TransportTotal: sum.TransportTotal,
			Scoped:         sum.Scoped,
			Unscoped:       sum.Unscoped + sum.NonTransport,
			UnknownScope:   sum.UnknownScope,
		},
	}

	regions, err := db.q.ListScopeRegionCounts(ctx, pgdb.ListScopeRegionCountsParams{
		TransportRoutes: transportRoutes,
		Since:           since,
	})
	if err != nil {
		return nil, fmt.Errorf("scope byRegion query: %w", err)
	}
	st.ByRegion = make([]ScopeRegionCount, 0, len(regions))
	for _, r := range regions {
		st.ByRegion = append(st.ByRegion, ScopeRegionCount{Name: r.Name, Count: r.Cnt})
	}

	points, err := db.q.ListScopeTimeSeries(ctx, pgdb.ListScopeTimeSeriesParams{
		BucketSecs:      int32(bucket / time.Second),
		TransportRoutes: transportRoutes,
		Since:           since,
	})
	if err != nil {
		return nil, fmt.Errorf("scope timeseries query: %w", err)
	}
	st.TimeSeries = make([]ScopeTimePoint, 0, len(points))
	for _, p := range points {
		st.TimeSeries = append(st.TimeSeries, ScopeTimePoint{T: p.Bucket.UTC(), Scoped: p.Scoped, Unscoped: p.Unscoped})
	}

	roles, err := db.q.ListScopeAdvertsByRole(ctx, pgdb.ListScopeAdvertsByRoleParams{
		PayloadType: int32(types.PayloadADVERT),
		FloodRoutes: floodRoutes,
		Since:       since,
	})
	if err != nil {
		return nil, fmt.Errorf("scope advertsByRole query: %w", err)
	}
	st.AdvertsByRole = make([]ScopeAdvertRoleCount, 0, len(roles))
	for _, r := range roles {
		st.AdvertsByRole = append(st.AdvertsByRole, ScopeAdvertRoleCount{
			Role: r.Role, Unscoped: r.Unscoped, UnknownScope: r.UnknownScope, Named: r.Named,
		})
	}
	return st, nil
}

type ScopeAuditQuery struct {
	Since      time.Time
	Prefixes   []string // hop prefixes to look for, any case; lowercased before matching
	MinHopLen  int
	RouteTypes []int
}

// ScopeAuditHop is one distinct (transmission, forwarder hop) match.
type ScopeAuditHop struct {
	TxID      int64
	Hop       string  // lowercase
	ScopeName *string // nil = unscoped, "" = scoped but unmatched
	FirstSeen time.Time
}

// ListScopeAuditHops returns one row per distinct (transmission, hop) where a hop
// of at least MinHopLen characters equals one of the prefixes, for transmissions
// first seen at or after Since on the given route types.
func (db *DB) ListScopeAuditHops(ctx context.Context, q ScopeAuditQuery) ([]ScopeAuditHop, error) {
	if q.MinHopLen < 0 || q.MinHopLen > math.MaxInt32 {
		return nil, fmt.Errorf("scope audit min hop length %d out of range", q.MinHopLen)
	}
	prefixes := make([]string, 0, len(q.Prefixes))
	for _, p := range q.Prefixes {
		if p = normalizeKey(p); p != "" {
			prefixes = append(prefixes, p)
		}
	}
	if len(prefixes) == 0 {
		return []ScopeAuditHop{}, nil
	}
	routeTypes := make([]int32, len(q.RouteTypes))
	for i, rt := range q.RouteTypes {
		routeTypes[i] = int32(rt)
	}
	rows, err := db.q.ListScopeAuditHops(ctx, pgdb.ListScopeAuditHopsParams{
		Since:      q.Since,
		RouteTypes: routeTypes,
		MinHopLen:  int32(q.MinHopLen),
		Prefixes:   prefixes,
	})
	if err != nil {
		return nil, fmt.Errorf("list scope audit hops: %w", err)
	}
	out := make([]ScopeAuditHop, 0, len(rows))
	for _, r := range rows {
		out = append(out, ScopeAuditHop{
			TxID:      int64(r.TransmissionID),
			Hop:       r.Hop,
			ScopeName: fromPgText(r.ScopeName),
			FirstSeen: r.FirstSeen.UTC(),
		})
	}
	return out, nil
}
