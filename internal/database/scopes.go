package database

import (
	"context"
	"errors"
	"fmt"
	"sort"
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
	Scoped         int64
	Unscoped       int64 // includes every non-transport transmission (#1838)
	UnknownScope   int64
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
		return nil, err
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
	return db.q.UpsertScopeMatchTotals(ctx, pgdb.UpsertScopeMatchTotalsParams{
		CountingSince:       t.Since,
		UniqueMatches:       t.Unique,
		ExplicitOverDerived: t.ExplicitOverDerived,
		Ambiguous:           t.Ambiguous,
		NoneMatches:         t.None,
		UpdatedAt:           now,
	})
}

// ListConfiguredScopes returns the region sets that nodes.configured_scope
// holds, one row per node that has one. Names carry the leading "#".
func (db *DB) ListConfiguredScopes(ctx context.Context) ([]DeclaredRegions, error) {
	rows, err := db.q.ListConfiguredScopes(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]DeclaredRegions, len(rows))
	for i, r := range rows {
		out[i] = DeclaredRegions{RegionsCSV: r.ConfiguredScope, ObservedAt: r.ConfiguredScopeAt.Time.UTC()}
	}
	return out, nil
}

// ListLatestDeclaredRegions returns the newest answer per target from
// node_declared_regions. Names are bare, without the "#".
func (db *DB) ListLatestDeclaredRegions(ctx context.Context) ([]DeclaredRegions, error) {
	rows, err := db.q.ListLatestDeclaredRegions(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]DeclaredRegions, len(rows))
	for i, r := range rows {
		out[i] = DeclaredRegions{Target: r.Target, RegionsCSV: r.RegionsCsv, ObservedAt: r.ObservedAt.UTC(), Truncated: r.Truncated}
	}
	return out, nil
}

// DeclaredRegions is the newest declared-regions answer for one target.
type DeclaredRegions struct {
	Target     string
	ObservedAt time.Time // zero when the source has no timestamp
	RegionsCSV string
	Truncated  bool
}

func (d *DB) AllCurrentDeclaredRegions(ctx context.Context) ([]DeclaredRegions, error) {
	merged := map[string]DeclaredRegions{}
	keep := func(r DeclaredRegions) {
		if r.Target == "" {
			return
		}
		if cur, ok := merged[r.Target]; ok && !r.ObservedAt.After(cur.ObservedAt) {
			return // ties keep the first source seen (configured_scope)
		}
		merged[r.Target] = r
	}

	scopes, err := d.q.ListConfiguredScopes(ctx)
	if err != nil {
		return nil, fmt.Errorf("declared regions from configured_scope: %w", err)
	}
	for _, s := range scopes {
		// Truncated has no equivalent on this source: the /neighbors report has a size
		// cap but does not say whether it fired, so claiming false would invent a fact.
		var at time.Time
		if p := fromPgTimestamptz(s.ConfiguredScopeAt); p != nil { // GUESS: configured_scope_at is timestamptz
			at = p.UTC()
		}
		keep(DeclaredRegions{Target: s.PublicKey, ObservedAt: at, RegionsCSV: s.ConfiguredScope})
	}

	latest, err := d.q.ListLatestDeclaredRegions(ctx)
	if err != nil {
		return nil, fmt.Errorf("declared regions from node_declared_regions: %w", err)
	}
	for _, r := range latest {
		keep(DeclaredRegions{
			Target:     r.Target,
			ObservedAt: r.ObservedAt.UTC(), // GUESS: observed_at timestamptz NOT NULL
			RegionsCSV: r.RegionsCsv,
			Truncated:  r.Truncated, // GUESS: boolean (was int 0/1)
		})
	}

	if len(merged) == 0 {
		return nil, nil
	}
	out := make([]DeclaredRegions, 0, len(merged))
	for _, r := range merged {
		out = append(out, r)
	}
	// Stable output: the handler ranks rows itself, but a deterministic order keeps
	// responses comparable between calls on unchanged data.
	sort.Slice(out, func(i, j int) bool { return out[i].Target < out[j].Target })
	return out, nil
}

// GetScopeStats returns scope statistics for transmissions first seen at or after since.
// bucket is the time-series bucket width (at least one second).
func (db *DB) GetScopeStats(ctx context.Context, since time.Time, bucket time.Duration) (*ScopeStats, error) {
	if bucket < time.Second {
		return nil, fmt.Errorf("scope stats: bucket %v is shorter than one second", bucket)
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
	Prefixes   []string // lowercase hop prefixes to look for
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

func (db *DB) ListScopeAuditHops(ctx context.Context, q ScopeAuditQuery) ([]ScopeAuditHop, error) {
	if len(q.Prefixes) == 0 {
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
		Prefixes:   q.Prefixes,
	})
	if err != nil {
		return nil, err
	}
	out := make([]ScopeAuditHop, 0, len(rows))
	for _, r := range rows {
		out = append(out, ScopeAuditHop{
			TxID:      int64(r.TransmissionID),
			Hop:       r.Hop,
			ScopeName: fromPgText(r.ScopeName),
			FirstSeen: r.FirstSeen,
		})
	}
	return out, nil
}
