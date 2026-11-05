package main

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// epoch stands in for a timestamp the source never recorded where the target
// requires one. An edge or drop dated 1970 is plainly "unknown" and ages out
// of every retention window.
var epoch = time.Unix(0, 0).UTC()

type edgeKey struct{ a, b string }

type edge struct {
	count    int64
	lastSeen time.Time
}

// loadNeighborEdges lowercases both endpoints. Two rows that only differed by
// case become one edge: the counts add and the later last_seen wins.
func loadNeighborEdges(ctx context.Context, src *Source, tx pgx.Tx, nodes *LoadedNodes, report *Report) error {
	rows, err := src.db.QueryContext(ctx, `SELECT node_a, node_b, count, last_seen FROM neighbor_edges ORDER BY node_a, node_b`)
	if err != nil {
		return fmt.Errorf("read neighbor_edges: %w", err)
	}
	defer rows.Close()

	edges := map[edgeKey]edge{}
	for rows.Next() {
		var (
			a, b     string
			count    sql.NullInt64
			lastSeen any
		)
		if err := rows.Scan(&a, &b, &count, &lastSeen); err != nil {
			return fmt.Errorf("scan neighbor_edges: %w", err)
		}
		key := edgeKey{strings.ToLower(strings.TrimSpace(a)), strings.ToLower(strings.TrimSpace(b))}
		for _, endpoint := range []string{key.a, key.b} {
			if _, found := nodes.IDs[endpoint]; !found {
				report.Problem("neighbor_edges: endpoint has no node row (loaded anyway)", endpoint)
			}
		}
		current := edge{count: 1, lastSeen: epoch}
		if count.Valid {
			current.count = count.Int64
		}
		if seen := report.Time("neighbor_edges.last_seen", lastSeen); seen != nil {
			current.lastSeen = *seen
		} else {
			report.Add("neighbor_edges: no last_seen, set to the epoch", 1)
		}
		if existing, collided := edges[key]; collided {
			current.count += existing.count
			if existing.lastSeen.After(current.lastSeen) {
				current.lastSeen = existing.lastSeen
			}
			report.Add("neighbor_edges: merged, keys collided after lowercasing", 1)
		}
		edges[key] = current
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read neighbor_edges: %w", err)
	}

	keys := make([]edgeKey, 0, len(edges))
	for key := range edges {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].a != keys[j].a {
			return keys[i].a < keys[j].a
		}
		return keys[i].b < keys[j].b
	})
	out := make([][]any, 0, len(keys))
	for _, key := range keys {
		out = append(out, []any{key.a, key.b, edges[key].count, edges[key].lastSeen})
	}
	report.Add("neighbor_edges: loaded", int64(len(out)))
	return copyRows(ctx, tx, "neighbor_edges", []string{"node_a", "node_b", "count", "last_seen"}, out)
}
