package database

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
	"github.com/jackc/pgx/v5/pgconn"
)

// sqlstateInsufficientPrivilege is the code Postgres returns when the role may
// not call a function such as pg_ls_waldir().
const sqlstateInsufficientPrivilege = "42501"

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
	Checkpoints  *CheckpointStats // always set on Postgres 17 and later
	Rows         TableRowCounts   // transmissions, observations, nodes, observers
}

// EntityCounts are the node and observer totals in the store stats.
type EntityCounts struct {
	ActiveNodes int
	AllNodes    int
	Observers   int
}

// RecentObservationCounts are the observation totals for the last hour and day.
type RecentObservationCounts struct {
	LastHour int
	Last24h  int
}

// GetStats returns aggregate counts. totalNodes counts nodes seen in the
// last 7 days; now is a parameter so tests control the windows.
func (db *DB) GetStats(ctx context.Context, now time.Time) (*Stats, error) {
	row, err := db.q.GetStats(ctx, pgdb.GetStatsParams{
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

// GetEntityCounts counts nodes heard after activeSince, all nodes, and
// observers that are not inactive.
func (db *DB) GetEntityCounts(ctx context.Context, activeSince time.Time) (EntityCounts, error) {
	r, err := db.q.GetEntityCounts(ctx, activeSince)
	if err != nil {
		return EntityCounts{}, fmt.Errorf("get entity counts: %w", err)
	}
	return EntityCounts{
		ActiveNodes: int(r.ActiveNodes),
		AllNodes:    int(r.AllNodes),
		Observers:   int(r.Observers),
	}, nil
}

// CountRecentObservations counts observations in the hour and the day before now.
func (db *DB) CountRecentObservations(ctx context.Context, now time.Time) (RecentObservationCounts, error) {
	r, err := db.q.CountRecentObservations(ctx, pgdb.CountRecentObservationsParams{
		HourAgo: now.Add(-time.Hour),
		DayAgo:  now.Add(-24 * time.Hour),
	})
	if err != nil {
		return RecentObservationCounts{}, fmt.Errorf("count recent observations: %w", err)
	}
	return RecentObservationCounts{LastHour: int(r.LastHour), Last24h: int(r.LastDay)}, nil
}

// GetDBSizeStats returns database size, WAL, bloat and row-count figures
// for the /api/perf endpoint. The WAL size is omitted, not an error, when the
// role lacks the privilege to list the WAL directory.
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

	var wal int64
	err = db.pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(size), 0)::bigint FROM pg_ls_waldir()`).Scan(&wal)
	var pgErr *pgconn.PgError
	switch {
	case err == nil:
		out.WALSizeBytes = &wal
	case errors.As(err, &pgErr) && pgErr.Code == sqlstateInsufficientPrivilege:
	default:
		return DBSizeStats{}, fmt.Errorf("wal size: %w", err)
	}

	if err := db.pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(n_dead_tup), 0)::bigint FROM pg_stat_user_tables`).Scan(&out.DeadTuples); err != nil {
		return DBSizeStats{}, fmt.Errorf("dead tuples: %w", err)
	}

	var cp CheckpointStats
	if err := db.pool.QueryRow(ctx,
		`SELECT num_timed::bigint, num_requested::bigint, buffers_written::bigint
		   FROM pg_stat_checkpointer`).Scan(&cp.Timed, &cp.Requested, &cp.BuffersWritten); err != nil {
		return DBSizeStats{}, fmt.Errorf("checkpoint stats: %w", err)
	}
	out.Checkpoints = &cp

	return out, nil
}
