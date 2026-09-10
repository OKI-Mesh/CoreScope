package database

import (
	"database/sql"
	"embed"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// minRequiredSchemaVersion is the goose migration version the SERVER
// requires to operate correctly. Bump this whenever server code starts
// depending on a column/table introduced in a new migration. The
// ingestor is the only writer of schema — see RunMigrations, called
// only from cmd/ingestor.
const minRequiredSchemaVersion = 29

func init() {
	goose.SetBaseFS(migrationsFS)
	if err := goose.SetDialect("sqlite3"); err != nil {
		panic(fmt.Sprintf("database: set goose dialect: %v", err))
	}
}

// RunMigrations applies all pending goose migrations, in strict order.
// No pre-flight checks — callers that need to distinguish a fresh DB
// from an unstamped-but-populated one should call MigrationStatus first
// and branch accordingly (see cmd/ingestor's boot sequence).
func RunMigrations(rw *sql.DB) error {
	if err := goose.Up(rw, "migrations"); err != nil {
		return fmt.Errorf("goose up: %w", err)
	}
	return nil
}

// RunMigrationsAllowingGaps behaves like RunMigrations but permits
// out-of-order application — i.e. it applies migration N even if some
// earlier migration hasn't been stamped, rather than refusing. Intended
// ONLY for use immediately after DetectAndStampSchema, to fill in
// whatever gaps that best-effort stamping left behind.
func RunMigrationsAllowingGaps(rw *sql.DB) error {
	if err := goose.Up(rw, "migrations", goose.WithAllowMissing()); err != nil {
		return fmt.Errorf("goose up (allow-missing): %w", err)
	}
	return nil
}

// AssertReady verifies the schema has been migrated to at least
// minRequiredSchemaVersion. The server calls this at startup and refuses
// to start if the ingestor hasn't migrated the database yet — the server
// never runs migrations itself.
func AssertReady(ro *sql.DB) error {
	current, err := CurrentSchemaVersion(ro)
	if err != nil {
		return fmt.Errorf("checking schema version: %w", err)
	}
	if current < minRequiredSchemaVersion {
		return fmt.Errorf(
			"schema not migrated by ingestor (at v%d, need v%d); restart ingestor first",
			current, minRequiredSchemaVersion)
	}
	return nil
}

// backupBeforeMigration copies path to path+".pre-migration-backup"
// before any migration/stamping runs, so a failed or unexpected
// migration never destroys the user's only copy of pre-existing data.
// Skipped if a backup already exists (avoids overwriting a good backup
// with a possibly-already-migrated file on a restart).
func BackupBeforeMigration(path string) error {
	backupPath := path + ".pre-migration-backup"
	if _, err := os.Stat(backupPath); err == nil {
		return nil // backup already exists, don't overwrite it
	}
	in, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening db for backup: %w", err)
	}
	defer in.Close()
	out, err := os.Create(backupPath)
	if err != nil {
		return fmt.Errorf("creating backup file: %w", err)
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return fmt.Errorf("copying db to backup: %w", err)
	}
	log.Printf("[migrate] backed up pre-migration database to %s", backupPath)
	return nil
}
