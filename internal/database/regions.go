package database

import (
	"context"
	"time"
)

// DeclaredRegionsRow is one node's declared region set: the comma-separated
// names and when they were last answered.
type DeclaredRegionsRow struct {
	CSV    string
	SeenAt time.Time // the epoch when the source has no timestamp
}

// ListConfiguredScopes returns the region sets that nodes.configured_scope
// holds, one row per node that has one. Names carry the leading "#".
func (db *DB) ListConfiguredScopes(ctx context.Context) ([]DeclaredRegionsRow, error) {
	rows, err := db.q.ListConfiguredScopes(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]DeclaredRegionsRow, len(rows))
	for i, r := range rows {
		out[i] = DeclaredRegionsRow{CSV: r.Csv, SeenAt: r.SeenAt.UTC()}
	}
	return out, nil
}

// ListLatestDeclaredRegions returns the newest answer per target from
// node_declared_regions. Names are bare, without the "#".
func (db *DB) ListLatestDeclaredRegions(ctx context.Context) ([]DeclaredRegionsRow, error) {
	rows, err := db.q.ListLatestDeclaredRegions(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]DeclaredRegionsRow, len(rows))
	for i, r := range rows {
		out[i] = DeclaredRegionsRow{CSV: r.Csv, SeenAt: r.SeenAt.UTC()}
	}
	return out, nil
}
