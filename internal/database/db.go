package database

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

// Pool sizes. Each process opens one pool. The ingestor runs one ingest worker
// plus prune, neighbor-builder, metrics and stats writers; the server fans out
// about five requests per page. Both stay well under Postgres max_connections
// (default 100). Revisit after the cutover rehearsal.
const (
	maxConnsReadWrite = 10
	maxConnsReadOnly  = 10
)

type DB struct {
	pool *pgxpool.Pool
	q    *pgdb.Queries
}

func New(pool *pgxpool.Pool) *DB {
	return &DB{
		pool: pool,
		q:    pgdb.New(pool),
	}
}

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

func (db *DB) Close() {
	db.pool.Close()
}

func (db *DB) Pool() *pgxpool.Pool { return db.pool }

// SQL returns a database/sql handle backed by the same pool. It exists for
// goose and other database/sql-only code. Closing it does not close the pool.
func (db *DB) SQL() *sql.DB {
	return stdlib.OpenDBFromPool(db.pool)
}

type ConnectionTarget struct {
	User     string
	Host     string
	Port     uint16
	Database string
}

func ParseConnectionTarget(dsn string) (ConnectionTarget, error) {
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return ConnectionTarget{}, err
	}
	return ConnectionTarget{
		User:     cfg.User,
		Host:     cfg.Host,
		Port:     cfg.Port,
		Database: cfg.Database,
	}, nil
}
