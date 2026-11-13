// internal/database/tx.go
package database

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// inTx runs fn in one transaction. tx is a *DB whose queries are bound to that
// transaction, so every existing method works inside it unchanged. It commits
// if fn returns nil and rolls back on an error or a panic.
func (db *DB) inTx(ctx context.Context, fn func(tx *DB) error) error {
	return pgx.BeginFunc(ctx, db.pool, func(t pgx.Tx) error {
		return fn(&DB{
			pool: nil, // methods that try to open their own transaction or use Pool() fail loudly
			q:    db.q.WithTx(t),
		})
	})
}
