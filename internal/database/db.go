package database

import (
	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
	"github.com/jackc/pgx/v5/pgxpool"
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
