package dbtest

import (
	"context"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/OKI-Mesh/CoreScope/internal/database"
)

// Raw SQL for test assertions and seeding. Production queries go through sqlc
// and internal/database; these helpers exist so tests can look at the tables
// directly without adding methods to the production query set.

var identifier = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// Ago returns the time d before now, in UTC. Fixtures that go through
// time-window logic (the 7-day advert window, the 30-day envelope cutoff) need
// recent timestamps: a hardcoded date eventually stops testing what it was
// written to test.
func Ago(d time.Duration) time.Time {
	return time.Now().UTC().Add(-d)
}

// TestCtx returns a per-test context that also fails a hung query instead of
// stalling the whole suite. Use context.Background() inside Cleanup funcs,
// because tb.Context() is cancelled before cleanups run.
func TestCtx(tb testing.TB) context.Context {
	tb.Helper()
	ctx, cancel := context.WithTimeout(tb.Context(), 30*time.Second)
	tb.Cleanup(cancel)
	return ctx
}

// CountRows returns COUNT(*) for a table.
func CountRows(tb testing.TB, db *database.DB, table string) int {
	tb.Helper()
	return CountWhere(tb, db, table, "")
}

// CountWhere returns COUNT(*) for a table with an optional WHERE clause.
// table and where must be test-authored literals; values go through $N args.
func CountWhere(tb testing.TB, db *database.DB, table, where string, args ...any) int {
	tb.Helper()
	if !identifier.MatchString(table) {
		tb.Fatalf("CountWhere: bad table name %q", table)
	}
	q := "SELECT COUNT(*) FROM " + table
	if where != "" {
		q += " WHERE " + where
	}
	var n int
	MustScan(tb, QueryRow(tb, db, q, args...), &n)
	return n
}

// QueryRow runs a query expected to return one row. Pair it with MustScan.
func QueryRow(tb testing.TB, db *database.DB, query string, args ...any) pgx.Row {
	tb.Helper()
	return db.Pool().QueryRow(TestCtx(tb), query, args...)
}

// MustScan scans a row and fails the test on any error, including no rows.
// Assertions that ignore Scan errors turn real failures into zero values.
func MustScan(tb testing.TB, row pgx.Row, dest ...any) {
	tb.Helper()
	for i, d := range dest {
		if v := reflect.ValueOf(d); v.Kind() != reflect.Ptr || v.IsNil() {
			tb.Fatalf("MustScan: dest[%d] is %T, want a non-nil pointer (missing &?)", i, d)
		}
	}
	if err := row.Scan(dest...); err != nil {
		tb.Fatalf("scan: %v", err)
	}
}

// Exec runs a statement for seeding or cleanup and returns rows affected.
func Exec(tb testing.TB, db *database.DB, query string, args ...any) int64 {
	tb.Helper()
	tag, err := db.Pool().Exec(TestCtx(tb), query, args...)
	if err != nil {
		tb.Fatalf("exec %q: %v", query, err)
	}
	return tag.RowsAffected()
}

// Collect runs a multi-row query. For a single column use pgx.RowTo[string]
// (or the matching type) as fn.
func Collect[T any](tb testing.TB, db *database.DB, query string, fn pgx.RowToFunc[T], args ...any) []T {
	tb.Helper()
	rows, err := db.Pool().Query(TestCtx(tb), query, args...)
	if err != nil {
		tb.Fatalf("query %q: %v", query, err)
	}
	out, err := pgx.CollectRows(rows, fn)
	if err != nil {
		tb.Fatalf("collect %q: %v", query, err)
	}
	return out
}

// ExplainPlan returns the plan for a statement with sequential scans disabled,
// so it shows whether an index CAN serve the query. EXPLAIN without ANALYZE
// executes nothing, and SET LOCAL is scoped to this rolled-back transaction.
func ExplainPlan(tb testing.TB, db *database.DB, stmt string) string {
	tb.Helper()
	ctx := TestCtx(tb)
	tx, err := db.Pool().Begin(ctx)
	if err != nil {
		tb.Fatal(err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `SET LOCAL enable_seqscan = off`); err != nil {
		tb.Fatal(err)
	}
	rows, err := tx.Query(ctx, "EXPLAIN "+stmt)
	if err != nil {
		tb.Fatal(err)
	}
	lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		tb.Fatal(err)
	}
	return strings.Join(lines, "\n")
}

// TableColumns returns column name -> data_type for a table in the public schema.
func TableColumns(tb testing.TB, db *database.DB, table string) map[string]string {
	tb.Helper()
	rows := Collect(tb, db, `
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1`,
		func(row pgx.CollectableRow) ([2]string, error) {
			var kv [2]string
			err := row.Scan(&kv[0], &kv[1])
			return kv, err
		}, table)
	cols := make(map[string]string, len(rows))
	for _, kv := range rows {
		cols[kv[0]] = kv[1]
	}
	return cols
}
