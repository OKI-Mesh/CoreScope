package database

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"

	"github.com/pressly/goose/v3"
)

//go:embed postgres/migrations/*.sql
var migrationsFS embed.FS

func newMigrationProvider(db *sql.DB) (*goose.Provider, error) {
	sub, err := fs.Sub(migrationsFS, "postgres/migrations")
	if err != nil {
		return nil, fmt.Errorf("creating migrations sub filesystem: %w", err)
	}
	return goose.NewProvider(goose.DialectPostgres, db, sub)
}

// Migrate applies every pending migration to the database at connStr.
func Migrate(ctx context.Context, connStr string) error {
	migDB, err := sql.Open("pgx", connStr)
	if err != nil {
		return fmt.Errorf("opening migration handle: %w", err)
	}
	defer migDB.Close()
	if err := migDB.PingContext(ctx); err != nil {
		return fmt.Errorf("cannot reach database: %w", err)
	}
	provider, err := newMigrationProvider(migDB)
	if err != nil {
		return fmt.Errorf("creating migration provider: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("running migrations: %w", err)
	}
	return nil
}

// AssertReady fails unless every embedded migration is recorded as applied.
// It only reads, so it is safe on a read-only connection.
func AssertReady(ctx context.Context, ro *sql.DB) error {
	provider, err := newMigrationProvider(ro)
	if err != nil {
		return fmt.Errorf("creating goose provider: %w", err)
	}
	pending, err := provider.HasPending(ctx)
	if err != nil {
		return fmt.Errorf(
			"checking migration status (possible schema gap — restart the ingestor to apply missing schema changes): %w", err)
	}
	if pending {
		return errors.New("database has pending migrations; restart the ingestor to apply them before starting the server")
	}
	return nil
}

// CurrentSchemaVersion is the newest migration version recorded as applied.
func CurrentSchemaVersion(ctx context.Context, db *sql.DB) (int64, error) {
	provider, err := newMigrationProvider(db)
	if err != nil {
		return 0, fmt.Errorf("creating goose provider: %w", err)
	}
	ver, err := provider.GetDBVersion(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting goose version: %w", err)
	}
	return ver, nil
}

// LatestSchemaVersion is the newest migration version embedded in the binary.
func LatestSchemaVersion(ctx context.Context, db *sql.DB) (int64, error) {
	provider, err := newMigrationProvider(db)
	if err != nil {
		return 0, fmt.Errorf("creating goose provider: %w", err)
	}
	_, latest, err := provider.GetVersions(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting goose versions: %w", err)
	}
	return latest, nil
}
