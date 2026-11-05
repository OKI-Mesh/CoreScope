package database

import (
	"context"
	"fmt"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
)

// Stats holds system statistics.
type Stats struct {
	TotalPackets       int `json:"totalPackets"`
	TotalTransmissions int `json:"totalTransmissions"`
	TotalObservations  int `json:"totalObservations"`
	TotalNodes         int `json:"totalNodes"`
	TotalNodesAllTime  int `json:"totalNodesAllTime"`
	TotalObservers     int `json:"totalObservers"`
	PacketsLastHour    int `json:"packetsLastHour"`
	PacketsLast24h     int `json:"packetsLast24h"`
}

type CheckpointStats struct {
	Timed          int64 // checkpoints triggered by checkpoint_timeout
	Requested      int64 // checkpoints triggered by WAL volume or manual request
	BuffersWritten int64
}

type TableRowCounts struct {
	Transmissions, Observations, Nodes, Observers int64
}

type DBSizeStats struct {
	DBSizeBytes  int64
	WALSizeBytes *int64           // nil when the role cannot call pg_ls_waldir()
	DeadTuples   int64            // closest analogue to the SQLite freelist
	Checkpoints  *CheckpointStats // nil if pg_stat_checkpointer is unavailable
	Rows         TableRowCounts   // transmissions, observations, nodes, observers
}

// GetStats returns aggregate counts. totalNodes counts nodes seen in the
// last 7 days; now is a parameter so tests control the windows.
func (d *DB) GetStats(ctx context.Context, now time.Time) (*Stats, error) {
	row, err := d.q.GetStats(ctx, pgdb.GetStatsParams{
		ActiveSince: now.Add(-7 * 24 * time.Hour),
		HourAgo:     now.Add(-time.Hour),
		DayAgo:      now.Add(-24 * time.Hour),
	})
	if err != nil {
		return nil, fmt.Errorf("get stats: %w", err)
	}
	return &Stats{
		TotalPackets:       int(row.TotalTransmissions),
		TotalTransmissions: int(row.TotalTransmissions),
		TotalObservations:  int(row.TotalObservations),
		TotalNodes:         int(row.TotalNodes),
		TotalNodesAllTime:  int(row.TotalNodesAllTime),
		TotalObservers:     int(row.TotalObservers),
		PacketsLastHour:    int(row.PacketsLastHour),
		PacketsLast24h:     int(row.PacketsLast24h),
	}, nil
}

// GetDBSizeStats returns database size, WAL, bloat and row-count figures
// for the /api/perf endpoint.
func (db *DB) GetDBSizeStats(ctx context.Context) (DBSizeStats, error) {
	base, err := db.q.GetDBSizeStats(ctx)
	if err != nil {
		return DBSizeStats{}, fmt.Errorf("db size stats: %w", err)
	}

	out := DBSizeStats{
		DBSizeBytes: base.DbSizeBytes,
		Rows: TableRowCounts{
			Transmissions: base.Transmissions,
			Observations:  base.Observations,
			Nodes:         base.Nodes,
			Observers:     base.Observers,
		},
	}

	// Best-effort extras: a missing privilege must not fail the whole endpoint.
	var wal int64
	if err := db.pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(size), 0)::bigint FROM pg_ls_waldir()`).Scan(&wal); err == nil {
		out.WALSizeBytes = &wal
	}

	if err := db.pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(n_dead_tup), 0)::bigint FROM pg_stat_user_tables`).Scan(&out.DeadTuples); err != nil {
		return DBSizeStats{}, fmt.Errorf("dead tuples: %w", err)
	}

	var cp CheckpointStats
	if err := db.pool.QueryRow(ctx,
		`SELECT num_timed::bigint, num_requested::bigint, buffers_written::bigint
		   FROM pg_stat_checkpointer`).Scan(&cp.Timed, &cp.Requested, &cp.BuffersWritten); err == nil {
		out.Checkpoints = &cp
	}

	return out, nil
}
