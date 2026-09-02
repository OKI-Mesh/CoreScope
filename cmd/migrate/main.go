// Command migrate runs all dbschema migrations against a SQLite
// CoreScope database and exits. Used by CI / one-shot tooling to bring
// an unmigrated fixture (or a fresh DB) up to the schema shape the
// read-only server (cmd/server) requires via dbschema.AssertReady.
//
// In production the ingestor (cmd/ingestor) runs dbschema.Apply at
// startup before subscribing to MQTT — this binary exists so CI's E2E
// job can migrate the e2e-fixture.db without booting the full ingestor
// (which needs MQTT brokers).
//
// Usage:
//
//	migrate -db path/to/file.db
package main

import (
	"database/sql"
	"flag"
	"log"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	_ "modernc.org/sqlite"
)

// cmd/migrate/main.go
func main() {
	dbPath := flag.String("db", "", "path to SQLite database to migrate (required)")
	flag.Parse()

	if *dbPath == "" {
		log.Fatalf("[migrate] -db is required")
	}

	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[migrate] ")

	db, err := sql.Open("sqlite", *dbPath)
	if err != nil {
		log.Fatalf("open %s: %v", *dbPath, err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("ping %s: %v", *dbPath, err)
	}

	if err := database.RunMigrations(db); err != nil {
		log.Fatalf("run migrations: %v", err)
	}

	if err := database.NormalizePublicKeyCasing(db); err != nil {
		log.Fatalf("normalize public_key casing: %v", err)
	}

	if err := database.AssertReady(db); err != nil {
		log.Fatalf("database not ready even after migrations: %v (this indicates a bug in the migration set)", err)
	}

	log.Printf("OK: %s is migrated and ready", *dbPath)
}
