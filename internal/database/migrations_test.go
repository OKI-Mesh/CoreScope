package database

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewMigrationProvider(t *testing.T) {
	db := newMemoryDB(t)

	provider, err := NewMigrationProvider(db, false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(provider.ListSources()) == 0 {
		t.Error("expected at least one embedded migration to be found")
	}
}

func TestRunMigrations_FreshDatabase(t *testing.T) {
	db := newMemoryDB(t)

	if err := RunMigrations(db); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ver, err := CurrentSchemaVersion(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ver == 0 {
		t.Error("expected a nonzero schema version after running migrations")
	}
	if err := AssertReady(db); err != nil {
		t.Errorf("expected db ready after RunMigrations, got: %v", err)
	}
}

func TestRunMigrations_Idempotent(t *testing.T) {
	db := newMemoryDB(t)

	if err := RunMigrations(db); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if err := RunMigrations(db); err != nil {
		t.Fatalf("second run should be a no-op, got error: %v", err)
	}
}

func TestRunMigrations_RefusesRealGap(t *testing.T) {
	db := newMemoryDB(t)
	// Stamp v1 and v3, skipping v2 — a genuine out-of-order gap.
	// Strict RunMigrations (allowOutOfOrder=false) must refuse rather
	// than silently apply 2 out of sequence.
	if err := StampVersions(db, []int64{1}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Can't stamp 3 without 2 existing as a migration source issue in
	// practice, so instead: stamp 1, then attempt Up and confirm it
	// still succeeds normally (no real gap yet) — the more meaningful
	// gap test is via RunMigrationsAllowingGaps below, since
	// constructing a genuine goose-refused gap requires stamping a
	// later version while skipping an earlier one that still has a
	// migration file, which StampVersions alone models correctly:
	if err := StampVersions(db, []int64{3}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err := RunMigrations(db)
	if err == nil {
		t.Error("expected RunMigrations to refuse applying migrations with an unstamped gap below the max stamped version")
	}
}

func TestRunMigrationsAllowingGaps_FillsGap(t *testing.T) {
	db := newMemoryDB(t)

	// Build REAL schema through v3 first (so migration 2's actual
	// effects exist, even though we're about to pretend it's
	// unapplied) — StampVersions alone only touches goose_db_version
	// bookkeeping, it doesn't run any migration SQL.
	if err := SeedUnstampedSchema(db, 3); err != nil {
		t.Fatalf("seeding: %v", err)
	}

	// Now stamp only 1 and 3 as applied, leaving 2 as a real gap in
	// goose's eyes — even though its SQL already ran during seeding.
	if err := StampVersions(db, []int64{1, 3}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := RunMigrationsAllowingGaps(db); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := AssertReady(db); err != nil {
		t.Errorf("expected db ready after filling the gap, got: %v", err)
	}
}

func TestAssertReady_FreshUnmigratedDatabase(t *testing.T) {
	db := newMemoryDB(t)
	if err := AssertReady(db); err == nil {
		t.Error("expected AssertReady to fail on an unmigrated database")
	}
}

func TestAssertReady_AfterRunMigrations(t *testing.T) {
	db := newMemoryDB(t)
	if err := RunMigrations(db); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := AssertReady(db); err != nil {
		t.Errorf("expected ready, got: %v", err)
	}
}

func TestAssertReady_PartialMigrationStillPending(t *testing.T) {
	db := newMemoryDB(t)
	// Stamp only an early subset — AssertReady must report pending,
	// not silently pass.
	if err := StampVersions(db, []int64{1, 2}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := AssertReady(db); err == nil {
		t.Error("expected AssertReady to fail when migrations remain pending")
	}
}

func TestBackupBeforeMigration(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	if err := os.WriteFile(path, []byte("fake sqlite content"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := BackupBeforeMigration(path); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	backupPath := path + ".pre-migration-backup"
	got, err := os.ReadFile(backupPath)
	if err != nil {
		t.Fatalf("expected backup file to exist: %v", err)
	}
	if string(got) != "fake sqlite content" {
		t.Errorf("backup content = %q, want %q", got, "fake sqlite content")
	}
}

func TestBackupBeforeMigration_DoesNotOverwriteExistingBackup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	backupPath := path + ".pre-migration-backup"

	if err := os.WriteFile(path, []byte("current content"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(backupPath, []byte("original backup"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := BackupBeforeMigration(path); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := os.ReadFile(backupPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "original backup" {
		t.Errorf("existing backup was overwritten: got %q, want %q (preserved)", got, "original backup")
	}
}

func TestBackupBeforeMigration_MissingSourceFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "does_not_exist.db")

	if err := BackupBeforeMigration(path); err == nil {
		t.Error("expected an error when the source database file doesn't exist")
	}
}
