// Package pgtest gives test packages a migrated Postgres without sharing state
// between tests.
//
// Call Run from TestMain and return its result:
//
//	func TestMain(m *testing.M) { os.Exit(pgtest.Run(m)) }
//
// Run starts one Postgres 18 + PostGIS container per test binary (or uses the
// server named by TEST_PG_DSN), migrates a template database once, and
// terminates the container when the tests finish. NewDatabase clones the
// template for each test, so every test starts from the migrated schema and
// cannot see another test's tables or rows.
package pgtest

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/OKI-Mesh/CoreScope/internal/database"
)

// Image is the one Postgres image constant that TestMain, Compose and CI share.
const Image = "imresamu/postgis:18-3.6"

var (
	adminDSN     string
	adminPool    *pgxpool.Pool
	templateName string
	seq          atomic.Int64
)

// Run starts Postgres, migrates a template database and runs the tests. It
// returns the exit code for os.Exit. A setup failure returns 1 without running
// any test, so callers must pass the result to os.Exit: ignoring it would make
// a missing Docker look like a passing run.
func Run(m *testing.M) int {
	ctx := context.Background()

	if dsn := os.Getenv("TEST_PG_DSN"); dsn != "" { // CI service container: no Docker-in-Docker
		adminDSN = dsn
	} else {
		ctr, err := postgres.Run(ctx, Image,
			postgres.WithDatabase("postgres"), postgres.WithUsername("test"),
			postgres.WithPassword("test"), postgres.BasicWaitStrategies())
		if err != nil {
			fmt.Fprintln(os.Stderr, "pgtest: Docker is required:", err)
			return 1
		}
		defer testcontainers.TerminateContainer(ctr)
		adminDSN, err = ctr.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			fmt.Fprintln(os.Stderr, "pgtest: connection string:", err)
			return 1
		}
	}

	var err error
	adminPool, err = pgxpool.New(ctx, adminDSN)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pgtest:", err)
		return 1
	}
	defer adminPool.Close()

	// go test runs packages in parallel, and with TEST_PG_DSN they all share one
	// server, so the template is named per process like the test databases.
	templateName = fmt.Sprintf("template_migrated_%d", os.Getpid())
	if _, err := adminPool.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %s`, templateName)); err != nil {
		fmt.Fprintln(os.Stderr, "pgtest: create template:", err)
		return 1
	}
	defer func() {
		_, _ = adminPool.Exec(context.Background(), fmt.Sprintf(`DROP DATABASE IF EXISTS %s WITH (FORCE)`, templateName))
	}()
	if err := database.Migrate(ctx, WithDatabase(adminDSN, templateName)); err != nil {
		fmt.Fprintln(os.Stderr, "pgtest: migrate template:", err)
		return 1
	}
	// CREATE DATABASE ... TEMPLATE needs the template to have no other sessions.
	_, _ = adminPool.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1`, templateName)
	return m.Run()
}

// NewDatabase returns the connection string of a new database cloned from the
// migrated template. It is dropped when the test ends.
func NewDatabase(tb testing.TB) string {
	tb.Helper()
	return createDatabase(tb, templateName)
}

// NewEmptyDatabase returns the connection string of a new database with no
// schema at all, for tests of the migration path itself. It is dropped when
// the test ends.
func NewEmptyDatabase(tb testing.TB) string {
	tb.Helper()
	return createDatabase(tb, "")
}

func createDatabase(tb testing.TB, template string) string {
	tb.Helper()
	if adminPool == nil {
		tb.Fatal("pgtest: Run was not called; use func TestMain(m *testing.M) { os.Exit(pgtest.Run(m)) }")
	}
	ctx := tb.Context()
	name := fmt.Sprintf("t_%d_%d", os.Getpid(), seq.Add(1))

	stmt := fmt.Sprintf(`CREATE DATABASE %s`, name)
	if template != "" {
		stmt += fmt.Sprintf(` TEMPLATE %s`, template)
	}
	if _, err := adminPool.Exec(ctx, stmt); err != nil {
		tb.Fatalf("create database %q from template %q: %v", name, template, err)
	}
	tb.Cleanup(func() {
		_, _ = adminPool.Exec(context.Background(), fmt.Sprintf(`DROP DATABASE IF EXISTS %s WITH (FORCE)`, name))
	})
	return WithDatabase(adminDSN, name)
}

// WithDatabase returns dsn with its database name replaced.
func WithDatabase(dsn, name string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		panic(fmt.Sprintf("pgtest: invalid DSN %q: %v", dsn, err))
	}
	u.Path = "/" + name
	return u.String()
}
