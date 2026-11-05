package database

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool sizes. Each process opens one pool. The ingestor runs one ingest worker
// plus prune, neighbor-builder, metrics and stats writers; the server fans out
// about five requests per page. Both stay well under Postgres max_connections
// (default 100). Revisit after the cutover rehearsal.
const (
	maxConnsReadWrite = 10
	maxConnsReadOnly  = 10
)

// OpenReadWrite opens a connection pool to the Postgres database at connStr
// with CoreScope's standard read-write configuration. Does not touch schema
// state in any way — callers that need the database migrated or
// asserted-ready compose this with internal/database/migrate.
func OpenReadWrite(ctx context.Context, connStr string) (*DB, error) {
	cfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("parsing connection string: %w", err)
	}
	cfg.MaxConns = maxConnsReadWrite

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("opening db: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging db: %w", err)
	}
	return New(pool), nil
}

// OpenReadOnly opens connStr as a read-only Postgres connection pool. Does
// not assert readiness — callers needing that guarantee compose this with
// migrate.AssertReady.
func OpenReadOnly(ctx context.Context, connStr string) (*DB, error) {
	cfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("parsing connection string: %w", err)
	}
	cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	cfg.MaxConns = maxConnsReadOnly

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("opening db: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging db: %w", err)
	}
	return New(pool), nil
}
