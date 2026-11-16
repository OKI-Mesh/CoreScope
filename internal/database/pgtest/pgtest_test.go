package pgtest_test

import (
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgtest"
)

func TestMain(m *testing.M) { os.Exit(pgtest.Run(m)) }

func connect(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatalf("pgxpool.New(): %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func count(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(t.Context(), query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

const tableExists = `SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1`

func TestNewDatabaseIsMigratedWithPostGIS(t *testing.T) {
	pool := connect(t, pgtest.NewDatabase(t))

	for _, table := range []string{"nodes", "observers", "transmissions", "observations"} {
		if count(t, pool, tableExists, table) != 1 {
			t.Errorf("table %q is missing from a database cloned from the template", table)
		}
	}
	if count(t, pool, `SELECT count(*) FROM pg_extension WHERE extname = 'postgis'`) != 1 {
		t.Error("postgis extension is not installed")
	}
}

func TestNewDatabaseIsIsolated(t *testing.T) {
	first := connect(t, pgtest.NewDatabase(t))
	second := connect(t, pgtest.NewDatabase(t))

	if _, err := first.Exec(t.Context(), `CREATE TABLE only_in_first (id integer)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if count(t, first, tableExists, "only_in_first") != 1 {
		t.Fatal("the table was not created in the first database")
	}
	if count(t, second, tableExists, "only_in_first") != 0 {
		t.Error("a second test database can see a table created in the first")
	}

	third := connect(t, pgtest.NewDatabase(t))
	if count(t, third, tableExists, "only_in_first") != 0 {
		t.Error("a table created in a test database leaked into the template")
	}
}

func TestNewEmptyDatabaseHasNoSchema(t *testing.T) {
	pool := connect(t, pgtest.NewEmptyDatabase(t))
	if got := count(t, pool, `SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'`); got != 0 {
		t.Errorf("empty database has %d tables, want 0", got)
	}
}

func TestDatabaseIsDroppedWhenTestEnds(t *testing.T) {
	var dsn string
	t.Run("create", func(t *testing.T) {
		dsn = pgtest.NewDatabase(t)
	})

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	name := strings.TrimPrefix(u.Path, "/")

	admin := connect(t, pgtest.WithDatabase(dsn, "postgres"))
	if got := count(t, admin, `SELECT count(*) FROM pg_database WHERE datname = $1`, name); got != 0 {
		t.Errorf("database %q still exists after its test ended", name)
	}
}
