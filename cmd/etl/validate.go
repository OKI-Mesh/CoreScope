package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// reconciliation says how many rows a source table should have turned into:
// every source row is either loaded or counted under one of the other names.
type reconciliation struct {
	source string
	// loaded names the report counters that add up to the rows in the target.
	loaded []string
	target string
	// accounted names the counters for rows that were dropped, skipped or merged.
	accounted []string
	// subtract names counters that take rows out of the source total before it
	// is compared (rows that are two source rows becoming one target row).
	subtract []string
}

var reconciliations = []reconciliation{
	{
		source: "nodes", target: "nodes",
		loaded:   []string{"nodes: loaded active", "nodes: loaded inactive"},
		subtract: []string{"nodes: skipped, type-N role", "nodes: key in both nodes and inactive_nodes, nodes row wins", "inactive_nodes: skipped, type-N role"},
	},
	{source: "observers", target: "observers", loaded: []string{"observers: loaded"}},
	{source: "transmissions", target: "transmissions", loaded: []string{"transmissions: loaded"}},
	{
		source: "observations", target: "observations",
		loaded: []string{"observations: loaded"},
		accounted: []string{
			"observations: duplicate of a lower id after normalising, dropped",
			"observations: skipped, transmission missing",
			"observations: skipped, no usable timestamp",
		},
	},
	{
		source: "neighbor_edges", target: "neighbor_edges",
		loaded:    []string{"neighbor_edges: loaded"},
		accounted: []string{"neighbor_edges: merged, keys collided after lowercasing"},
	},
	{
		source: "observer_metrics", target: "observer_metrics",
		loaded: []string{"observer_metrics: loaded"},
		accounted: []string{
			"observer_metrics: skipped, observer not loaded",
			"observer_metrics: skipped, unusable timestamp",
			"observer_metrics: duplicate after lowercasing, dropped",
		},
	},
	{source: "dropped_packets", target: "dropped_packets", loaded: []string{"dropped_packets: loaded"}},
	{source: "scope_match_totals", target: "scope_match_totals", loaded: []string{"scope_match_totals: loaded"}},
}

func sum(report *Report, names []string) int64 {
	var total int64
	for _, name := range names {
		total += report.Count(name)
	}
	return total
}

// validate checks that the target holds what the report says was loaded, that
// every source row is accounted for, and that observation_frames matches its
// counter. It then analyzes every loaded table so the planner has statistics
// the moment the application starts.
func validate(ctx context.Context, src *Source, tx pgx.Tx, report *Report) error {
	for _, r := range reconciliations {
		var sourceRows int64
		sourceTables := []string{r.source}
		if r.source == "nodes" {
			sourceTables = append(sourceTables, "inactive_nodes")
		}
		for _, table := range sourceTables {
			var n int64
			if err := src.db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s", table)).Scan(&n); err != nil {
				return fmt.Errorf("validate: count source %s: %w", table, err)
			}
			sourceRows += n
		}
		sourceRows -= sum(report, r.subtract)

		var targetRows int64
		if err := tx.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", pgx.Identifier{r.target}.Sanitize())).Scan(&targetRows); err != nil {
			return fmt.Errorf("validate: count target %s: %w", r.target, err)
		}
		if loaded := sum(report, r.loaded); targetRows != loaded {
			return fmt.Errorf("validate: %s holds %d rows but the load reported %d", r.target, targetRows, loaded)
		}
		if accounted := targetRows + sum(report, r.accounted); accounted != sourceRows {
			return fmt.Errorf("validate: %s has %d source rows but %d are loaded or accounted for", r.source, sourceRows, accounted)
		}
	}

	var frames int64
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM observation_frames`).Scan(&frames); err != nil {
		return fmt.Errorf("validate: count observation_frames: %w", err)
	}
	if reported := report.Count("observations: frame stored (differs from the transmission)"); frames != reported {
		return fmt.Errorf("validate: observation_frames holds %d rows but the load reported %d", frames, reported)
	}

	for _, table := range loadedTables {
		if _, err := tx.Exec(ctx, "ANALYZE "+pgx.Identifier{table}.Sanitize()); err != nil {
			return fmt.Errorf("validate: analyze %s: %w", table, err)
		}
	}
	report.Add("validation: tables reconciled with the source", int64(len(reconciliations)))
	return nil
}
