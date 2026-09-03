package database

import (
	"database/sql"
	"embed"
	"fmt"

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
		panic(fmt.Sprintf("dbschema: set goose dialect: %v", err))
	}
}

// RunMigrations applies all pending goose migrations. Intended to be
// called ONLY from the ingestor's startup path — the ingestor is the
// sole schema writer. The server never calls this; see AssertReady for
// the server's read-only readiness gate.
func RunMigrations(rw *sql.DB) error {
	if err := goose.Up(rw, "migrations"); err != nil {
		return fmt.Errorf("goose up: %w", err)
	}
	return nil
}

// AssertReady verifies the schema has been migrated to at least
// minRequiredSchemaVersion. The server calls this at startup and refuses
// to start if the ingestor hasn't migrated the database yet — the server
// never runs migrations itself.
func AssertReady(ro *sql.DB) error {
	current, err := goose.GetDBVersion(ro)
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
