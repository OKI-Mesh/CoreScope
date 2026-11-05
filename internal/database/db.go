package database

import (
	"database/sql"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
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
