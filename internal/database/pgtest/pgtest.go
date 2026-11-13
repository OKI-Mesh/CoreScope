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

const Image = "imresamu/postgis:18-3.6" // the one image constant TestMain, Compose and CI share

var (
	adminDSN string
	seq      atomic.Int64
)

// Run starts Postgres, migrates a template database and runs the tests.
// Use it from TestMain:  func TestMain(m *testing.M) { os.Exit(testpg.Run(m)) }
func Run(m *testing.M) int {
	ctx := context.Background()

	if dsn := os.Getenv("TEST_PG_DSN"); dsn != "" { // CI service container: no Docker-in-Docker
		adminDSN = dsn
	} else {
		ctr, err := postgres.Run(ctx, Image,
			postgres.WithDatabase("postgres"), postgres.WithUsername("test"),
			postgres.WithPassword("test"), postgres.BasicWaitStrategies())
		if err != nil {
			fmt.Fprintln(os.Stderr, "testpg: Docker is required:", err)
			return 1
		}
		defer testcontainers.TerminateContainer(ctr)
		adminDSN, _ = ctr.ConnectionString(ctx, "sslmode=disable")
	}

	admin, err := pgxpool.New(ctx, adminDSN)
	if err != nil {
		fmt.Fprintln(os.Stderr, "testpg:", err)
		return 1
	}
	defer admin.Close()
	if _, err := admin.Exec(ctx, `CREATE DATABASE template_migrated`); err != nil {
		fmt.Fprintln(os.Stderr, "testpg: create template:", err)
		return 1
	}
	if err := database.Migrate(ctx, WithDatabase(adminDSN, "template_migrated")); err != nil {
		fmt.Fprintln(os.Stderr, "testpg: migrate template:", err)
		return 1
	}
	_, _ = admin.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'template_migrated'`)
	return m.Run()
}

func NewDatabase(tb testing.TB) string {
	tb.Helper()
	return createDatabase(tb, "template_migrated")
}

func NewEmptyDatabase(tb testing.TB) string {
	tb.Helper()
	return createDatabase(tb, "")
}

func createDatabase(tb testing.TB, template string) string {
	tb.Helper()
	ctx := tb.Context()
	name := fmt.Sprintf("t_%d_%d", os.Getpid(), seq.Add(1))

	admin, err := pgxpool.New(ctx, adminDSN)
	if err != nil {
		tb.Fatal(err)
	}
	defer admin.Close()

	stmt := fmt.Sprintf(`CREATE DATABASE %s`, name)
	if template != "" {
		stmt += fmt.Sprintf(` TEMPLATE %s`, template)
	}
	if _, err := admin.Exec(ctx, stmt); err != nil {
		tb.Fatalf("create database %q from template %q: %v", name, template, err)
	}
	tb.Cleanup(func() {
		bg := context.Background()
		a, err := pgxpool.New(bg, adminDSN)
		if err != nil {
			return
		}
		defer a.Close()
		_, _ = a.Exec(bg, fmt.Sprintf(`DROP DATABASE IF EXISTS %s WITH (FORCE)`, name))
	})
	return WithDatabase(adminDSN, name)
}

func WithDatabase(dsn, name string) string {
	u, _ := url.Parse(dsn)
	u.Path = "/" + name
	return u.String()
}
