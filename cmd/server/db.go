package main

import (
	"context"
	"fmt"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
)

func ptr[T any](v T) *T { return &v }

// OpenDB opens the server's read-only Postgres handle and refuses to start
// unless the schema is fully migrated (AssertReady).
func OpenDB(ctx context.Context, dsn string) (*database.DB, error) {
	pg, err := database.OpenReadOnly(ctx, dsn)
	if err != nil {
		return nil, err
	}
	sqlDB := pg.SQL()
	if err := database.AssertReady(ctx, sqlDB); err != nil {
		sqlDB.Close()
		pg.Close()
		return nil, fmt.Errorf("database not ready: %w", err)
	}
	return pg, nil
}

func bytesToMB(b int64) float64 {
	return roundMB(float64(b) / 1048576)
}

func dbSizeStatsToResponse(s database.DBSizeStats) *api.DbStats {
	resp := &api.DbStats{
		DbSizeMB:   bytesToMB(s.DBSizeBytes),
		DeadTuples: s.DeadTuples,
		Rows: api.TableRowCounts{
			Transmissions: s.Rows.Transmissions,
			Observations:  s.Rows.Observations,
			Nodes:         s.Rows.Nodes,
			Observers:     s.Rows.Observers,
		},
	}
	if s.WALSizeBytes != nil {
		resp.WalSizeMB = bytesToMB(*s.WALSizeBytes)
	}
	if s.Checkpoints != nil {
		resp.Checkpoints = &api.CheckpointStats{
			Timed:          s.Checkpoints.Timed,
			Requested:      s.Checkpoints.Requested,
			BuffersWritten: s.Checkpoints.BuffersWritten,
		}
	}
	return resp
}

// --- Helpers ---

func nilIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

// nonNil returns s, or an empty slice if s is nil, so it marshals to [] instead of null.
func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}
