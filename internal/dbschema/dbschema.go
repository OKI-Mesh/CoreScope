// Package dbschema centralizes schema migrations and read-side schema
// assertions for the CoreScope SQLite DB. Per issue #1287 the writer
// (cmd/ingestor) owns ALL CREATE/ALTER/INSERT/UPDATE/DELETE on schema
// objects; the server (cmd/server) only ASSERTS that the schema is in
// the expected shape and refuses to start otherwise.
//
// Apply(rw, log) runs from the ingestor at startup BEFORE subscribing to
// MQTT. AssertReady(ro) runs from the server at startup and returns an
// error listing every missing column/index/table.
//
// INVARIANT (#1321): Any optional column the SERVER detects via PRAGMA
// (e.g. cmd/server/db.go detectSchema → hasScopeName, hasDefaultScope,
// hasObsRawHex, hasResolvedPath) MUST be added here, in Apply, AND
// asserted in AssertReady. Adding it only to cmd/ingestor/db.go
// applySchema reintroduces the startup-race bug from #1321: the
// server's PRAGMA can run before the ingestor finishes applySchema,
// the boolean caches `false`, and feature endpoints permanently 500
// until the server restarts. Source of truth lives here — full stop.
package dbschema

import (
	"database/sql"
	"strings"
)

// Logger is the minimal logging surface used by Apply. Both cmd/server
// and cmd/ingestor satisfy this with the stdlib `log` package's Printf
// (passed as a closure to avoid an indirect log dependency here).
type Logger func(format string, args ...interface{})

// Querier is the read surface shared by *sql.DB and *sql.Tx.
//
// Not *sql.Conn: it exposes only QueryContext/QueryRowContext, so it does not
// satisfy this. Widen the interface to the Context variants if a caller ever
// needs one.
//
// It exists so schema probes can run on whichever handle the caller already
// holds. Taking *sql.DB unconditionally is a deadlock waiting to happen: a
// caller inside a transaction has the connection checked out, and on a pool
// capped at one connection — which is what cmd/ingestor runs — a probe against
// the pool waits forever for the connection its own transaction is holding.
type Querier interface {
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

// ─── ensure_* helpers (writer side) ────────────────────────────────────────

// SoftDeleteBlacklistedObservers marks the given observer IDs as
// inactive=1 (case-insensitive match). Returns count affected.
// Writer-side helper; ingestor calls it at startup with the operator
// blacklist (read from config).
func SoftDeleteBlacklistedObservers(rw *sql.DB, blacklist []string) (int64, error) {
	placeholders := make([]string, 0, len(blacklist))
	args := make([]interface{}, 0, len(blacklist))
	for _, pk := range blacklist {
		t := strings.TrimSpace(pk)
		if t == "" {
			continue
		}
		placeholders = append(placeholders, "LOWER(?)")
		args = append(args, t)
	}
	if len(placeholders) == 0 {
		return 0, nil
	}
	q := "UPDATE observers SET inactive = 1 WHERE LOWER(id) IN (" +
		strings.Join(placeholders, ",") + ") AND (inactive IS NULL OR inactive = 0)"
	res, err := rw.Exec(q, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}
