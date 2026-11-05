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

func NewMigrationProvider(ctx context.Context, db *sql.DB, allowOutOfOrder bool, disableVersioning bool) (*goose.Provider, error) {
	sub, err := fs.Sub(migrationsFS, "postgres/migrations")
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
	return goose.NewProvider(goose.DialectPostgres, db, sub, opts...)
}

func withMigrationHandle(ctx context.Context, connStr string, fn func(*sql.DB) error) error {
	migDB, err := sql.Open("pgx", connStr)
	if err != nil {
		return fmt.Errorf("opening migration handle: %w", err)
	}
	defer migDB.Close()
	if err := migDB.PingContext(ctx); err != nil {
		return fmt.Errorf("cannot reach database: %w", err)
	}
	return fn(migDB)
}

func Migrate(ctx context.Context, connStr string) error {
	return withMigrationHandle(ctx, connStr, func(migDB *sql.DB) error {
		provider, err := NewMigrationProvider(ctx, migDB, false, false)
		if err != nil {
			return fmt.Errorf("creating migration provider: %w", err)
		}
		if _, err := provider.Up(ctx); err != nil {
			return fmt.Errorf("running migrations: %w", err)
		}
		return nil
	})
}

func AssertReady(ctx context.Context, ro *sql.DB) error {
	provider, err := NewMigrationProvider(ctx, ro, false, false)
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

func CurrentSchemaVersion(ctx context.Context, db *sql.DB) (int64, error) {
	provider, err := NewMigrationProvider(ctx, db, false, false)
	if err != nil {
		return 0, fmt.Errorf("creating goose provider: %w", err)
	}
	ver, err := provider.GetDBVersion(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting goose version: %w", err)
	}
	return ver, nil
}

func LatestSchemaVersion(ctx context.Context, db *sql.DB) (int64, error) {
	provider, err := NewMigrationProvider(ctx, db, false, false)
	if err != nil {
		return 0, fmt.Errorf("creating goose provider: %w", err)
	}
	_, latest, err := provider.GetVersions(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting goose versions: %w", err)
	}
	return latest, nil
}
