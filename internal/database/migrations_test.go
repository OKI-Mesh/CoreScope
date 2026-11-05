package database_test

import (
	"database/sql"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

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

func TestAssertReadyFailsWhenMigrationUnrecorded(t *testing.T) {
	ctx := t.Context()
	db := openSQL(t, pgtest.NewDatabase(t))

	if _, err := db.ExecContext(ctx, `DELETE FROM goose_db_version WHERE version_id = 1`); err != nil {
		t.Fatalf("unrecord migration: %v", err)
	}
	if err := database.AssertReady(ctx, db); err == nil {
		t.Fatal("expected AssertReady to fail when a migration is no longer recorded as applied")
	}
}
