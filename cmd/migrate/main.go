package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/OKI-Mesh/CoreScope/internal/database"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[migrate] ")

	// The DSN carries a password, so the default comes from the environment and
	// the flag exists only for local use (it shows up in `ps`).
	dsn := flag.String("dsn", os.Getenv("DATABASE_URL"), "Postgres connection string (default: $DATABASE_URL)")
	status := flag.Bool("status", false, "print the current and latest schema version and exit; makes no changes")
	flag.Parse()

	if *dsn == "" {
		log.Fatalf("a connection string is required (-dsn or $DATABASE_URL)")
	}

	// Ctrl-C / SIGTERM cancels a long migration cleanly. There is deliberately
	// no timeout: index builds on large tables can run for minutes.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := database.OpenReadWrite(ctx, *dsn)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer db.Close()

	sqlDB := db.SQL() // database/sql handle over the same pool, for the migrate calls
	defer sqlDB.Close()

	current, err := database.CurrentSchemaVersion(ctx, sqlDB)
	if err != nil {
		log.Fatalf("current version: %v", err)
	}
	latest, err := database.LatestSchemaVersion(ctx, sqlDB)
	if err != nil {
		log.Fatalf("latest version: %v", err)
	}
	log.Printf("schema version: current=%d latest=%d", current, latest)

	if *status {
		if current != latest {
			os.Exit(1) // lets CI and the compose healthcheck test "is it migrated"
		}
		return
	}

	if err := database.Migrate(ctx, *dsn); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	after, err := database.CurrentSchemaVersion(ctx, sqlDB)
	if err != nil {
		log.Fatalf("version after migrate: %v", err)
	}
	if after != latest {
		log.Fatalf("migrated, but version is %d, want %d", after, latest)
	}
	log.Printf("OK: schema at version %d", after)
}
