package database

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path"

	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

func NewMigrationProvider(db *sql.DB, allowOutOfOrder bool, disableVersioning bool) (*goose.Provider, error) {
	sub, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("creating migrations sub filesystem: %w", err)
	}
	opts := []goose.ProviderOption{}
	if allowOutOfOrder {
		opts = append(opts, goose.WithAllowOutofOrder(true))
	}
	if disableVersioning {
		opts = append(opts, goose.WithDisableVersioning(true))
	}
	return goose.NewProvider(goose.DialectSQLite3, db, sub, opts...)
}

// RunMigrations applies all pending goose migrations, in strict order.
// No pre-flight checks — callers that need to distinguish a fresh DB
// from an unstamped-but-populated one should call MigrationStatus first
// and branch accordingly (see cmd/ingestor's boot sequence).
func RunMigrations(rw *sql.DB) error {

	provider, err := NewMigrationProvider(rw, false, false)
	if err != nil {
		return fmt.Errorf("creating goose provider: %w", err)
	}
	_, err = provider.Up(context.Background())
	if err != nil {
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
	provider, err := NewMigrationProvider(rw, true, false)
	if err != nil {
		return fmt.Errorf("creating goose provider: %w", err)
	}
	_, err = provider.Up(context.Background())
	if err != nil {
		return fmt.Errorf("goose up: %w", err)
	}
	return nil
}

// AssertReady verifies the schema has been migrated. The server calls this at startup and refuses
// to start if the ingestor hasn't migrated the database yet — the server
// never runs migrations itself. If migration is missing, the server will exit with a clear error message instructing the user to restart the ingestor first. If the schema is out of order, it will error out with a message instructing the user to run the ingestor with --allow-missing to fill in the gaps. This is a safety measure to prevent the server from running against an incompatible schema.
func AssertReady(ro *sql.DB) error {
	provider, err := NewMigrationProvider(ro, false, false)
	if err != nil {
		return fmt.Errorf("creating goose provider: %w", err)
	}
	pending, err := provider.HasPending(context.Background())
	if err != nil {
		// HasPending errors either on a genuine DB/connectivity problem, or when it
		// detects out-of-order migrations (a gap below the current max version) —
		// goose doesn't distinguish these with a typed error. The out-of-order case
		// is the common one in practice: run the ingestor with --allow-missing to
		// fill the gap. If the DB itself is unreachable, that will also surface here.
		return fmt.Errorf(
			"checking migration status (possible schema gap — restart the ingestor to apply missing schema changes): %w", err)
	}
	if pending {
		return errors.New("database has pending migrations; restart the ingestor to apply them before starting the server")
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

// CurrentSchemaVersion returns the highest applied goose migration
// version for db. Returns 0 for a database with no migrations applied
// yet (including a genuinely fresh database with no goose_db_version
// table at all).
func CurrentSchemaVersion(db *sql.DB) (int64, error) {
	provider, err := NewMigrationProvider(db, false, false)
	if err != nil {
		return 0, fmt.Errorf("creating goose provider: %w", err)
	}
	ver, err := provider.GetDBVersion(context.Background())
	if err != nil {
		return 0, fmt.Errorf("getting goose version: %w", err)
	}
	return ver, nil
}

// MigrationApplied reports whether the given goose migration version
// has been applied to db, by checking goose's own Status rather than
// querying goose_db_version directly.
func MigrationApplied(db *sql.DB, version int64) (bool, error) {
	provider, err := NewMigrationProvider(db, false, false)
	if err != nil {
		return false, fmt.Errorf("creating goose provider: %w", err)
	}
	statuses, err := provider.Status(context.Background())
	if err != nil {
		return false, fmt.Errorf("checking migration status: %w", err)
	}
	for _, s := range statuses {
		if s.Source.Version == version {
			return s.State == goose.StateApplied, nil
		}
	}
	return false, nil // no migration with that version number exists
}

func SeedUnstampedSchema(db *sql.DB, version int64) error {
	sub, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("migrations sub filesystem: %w", err)
	}
	filtered := filteredMigrationsFS{FS: sub, maxVersion: version}

	provider, err := goose.NewProvider(
		goose.DialectSQLite3, db, filtered,
		goose.WithDisableVersioning(true),
	)
	if err != nil {
		return fmt.Errorf("creating unversioned provider: %w", err)
	}
	if _, err := provider.Up(context.Background()); err != nil {
		return fmt.Errorf("applying migrations up to v%d (unstamped): %w", version, err)
	}
	return nil
}

// filteredMigrationsFS wraps fsys, hiding any migration file whose
// version prefix exceeds maxVersion. Used by SeedUnstampedSchema to
// work around goose's UpTo not respecting a version ceiling when
// WithDisableVersioning is set (it applies every file it finds,
// since there's no tracking table to consult for "already applied").
type filteredMigrationsFS struct {
	fs.FS
	maxVersion int64
}

func (f filteredMigrationsFS) ReadDir(name string) ([]fs.DirEntry, error) {
	entries, err := fs.ReadDir(f.FS, name)
	if err != nil {
		return nil, err
	}
	var out []fs.DirEntry
	for _, e := range entries {
		v, ok := migrationVersionPrefix(e.Name())
		if !ok || v <= f.maxVersion {
			out = append(out, e)
		}
	}
	return out, nil
}

func (f filteredMigrationsFS) Open(name string) (fs.File, error) {
	base := path.Base(name)
	if v, ok := migrationVersionPrefix(base); ok && v > f.maxVersion {
		return nil, fs.ErrNotExist
	}
	return f.FS.Open(name)
}
