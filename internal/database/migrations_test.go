package database_test

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/pgtest"
)

func openSQL(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open sql handle: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestMigrateFreshDatabaseReachesLatest(t *testing.T) {
	ctx := t.Context()
	dsn := pgtest.NewEmptyDatabase(t)

	if err := database.Migrate(ctx, dsn); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	db := openSQL(t, dsn)
	current, err := database.CurrentSchemaVersion(ctx, db)
	if err != nil {
		t.Fatalf("CurrentSchemaVersion: %v", err)
	}
	latest, err := database.LatestSchemaVersion(ctx, db)
	if err != nil {
		t.Fatalf("LatestSchemaVersion: %v", err)
	}
	if current == 0 || current != latest {
		t.Errorf("current = %d, latest = %d, want equal and non-zero", current, latest)
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	ctx := t.Context()
	dsn := pgtest.NewEmptyDatabase(t)

	if err := database.Migrate(ctx, dsn); err != nil {
		t.Fatalf("first Migrate: %v", err)
	}
	if err := database.Migrate(ctx, dsn); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
}

func TestMigrateReportsUnreachableDatabase(t *testing.T) {
	dsn := "postgres://nobody:nothing@127.0.0.1:1/none?sslmode=disable&connect_timeout=2"

	err := database.Migrate(t.Context(), dsn)
	if err == nil || !strings.Contains(err.Error(), "cannot reach database") {
		t.Fatalf("want a cannot-reach-database error, got %v", err)
	}
}

func TestAssertReadyFailsOnUnmigratedDatabase(t *testing.T) {
	ctx := t.Context()
	db := openSQL(t, pgtest.NewEmptyDatabase(t))

	if err := database.AssertReady(ctx, db); err == nil {
		t.Fatal("expected AssertReady to fail on an unmigrated database")
	}
}

func TestAssertReadyPassesOnMigratedDatabase(t *testing.T) {
	ctx := t.Context()
	db := openSQL(t, pgtest.NewDatabase(t))

	if err := database.AssertReady(ctx, db); err != nil {
		t.Fatalf("expected migrated database to pass AssertReady, got: %v", err)
	}
}

func TestAssertReadyPassesOverReadOnlyConnection(t *testing.T) {
	ctx := t.Context()
	dsn := pgtest.NewDatabase(t)

	ro, err := database.OpenReadOnly(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ro.Close() })

	handle := ro.SQL()
	t.Cleanup(func() { handle.Close() })
	if err := database.AssertReady(ctx, handle); err != nil {
		t.Fatalf("AssertReady must work without write access, got: %v", err)
	}
}

func TestAssertReadyFailsWhenNewestMigrationUnrecorded(t *testing.T) {
	ctx := t.Context()
	db := openSQL(t, pgtest.NewDatabase(t))

	if _, err := db.ExecContext(ctx,
		`DELETE FROM goose_db_version WHERE version_id = (SELECT max(version_id) FROM goose_db_version)`); err != nil {
		t.Fatalf("unrecord newest migration: %v", err)
	}
	err := database.AssertReady(ctx, db)
	if err == nil || !strings.Contains(err.Error(), "pending migrations") {
		t.Fatalf("want a pending-migrations error, got %v", err)
	}
}

func TestAssertReadyFailsOnSchemaGap(t *testing.T) {
	ctx := t.Context()
	db := openSQL(t, pgtest.NewDatabase(t))

	latest, err := database.LatestSchemaVersion(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if latest < 2 {
		t.Skip("a schema gap needs at least two migrations")
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM goose_db_version WHERE version_id = 1`); err != nil {
		t.Fatalf("unrecord first migration: %v", err)
	}
	err = database.AssertReady(ctx, db)
	if err == nil || !strings.Contains(err.Error(), "schema gap") {
		t.Fatalf("want a schema-gap error, got %v", err)
	}
}
