package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5"
)

func main() {
	sqlitePath := flag.String("sqlite", "", "path to the SQLite snapshot (a .backup file, never the live WAL-mode file)")
	dsn := flag.String("dsn", os.Getenv("DATABASE_URL"), "Postgres connection string (default: $DATABASE_URL)")
	commit := flag.Bool("commit", false, "commit the load; without it the load runs in full and is rolled back")
	flag.Parse()

	if err := run(context.Background(), *sqlitePath, *dsn, *commit); err != nil {
		log.Fatalf("etl: %v", err)
	}
}

func run(ctx context.Context, sqlitePath, dsn string, commit bool) error {
	if dsn == "" {
		return fmt.Errorf("-dsn or $DATABASE_URL is required")
	}
	src, err := openSource(sqlitePath)
	if err != nil {
		return err
	}
	defer src.Close()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect to Postgres: %w", err)
	}
	defer conn.Close(ctx)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	report := newReport()
	if err := load(ctx, src, tx, report); err != nil {
		return err
	}
	fmt.Print(report)

	if !commit {
		fmt.Println("dry run: rolled back (pass -commit to keep the load)")
		return nil
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	fmt.Println("committed")
	return nil
}

func load(ctx context.Context, src *Source, tx pgx.Tx, report *Report) error {
	if err := requireEmptyTarget(ctx, tx); err != nil {
		return err
	}
	if err := requireEmptyClientTables(ctx, src); err != nil {
		return err
	}
	nodes, err := loadNodes(ctx, src, tx, report)
	if err != nil {
		return err
	}
	observers, err := loadObservers(ctx, src, tx, report)
	if err != nil {
		return err
	}
	transmissions, err := loadTransmissions(ctx, src, tx, nodes, report)
	if err != nil {
		return err
	}
	if err := loadObservations(ctx, src, tx, nodes, observers, transmissions, report); err != nil {
		return err
	}
	if err := loadNeighborEdges(ctx, src, tx, nodes, report); err != nil {
		return err
	}
	if err := loadObserverMetrics(ctx, src, tx, observers, report); err != nil {
		return err
	}
	if err := loadDroppedPackets(ctx, src, tx, report); err != nil {
		return err
	}
	if err := loadScopeMatchTotals(ctx, src, tx, report); err != nil {
		return err
	}
	return validate(ctx, src, tx, report)
}
