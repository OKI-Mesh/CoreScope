package main

import (
	"database/sql"
	"flag"
	"log"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	_ "modernc.org/sqlite"
)

func main() {
	dbPath := flag.String("db", "", "path to SQLite database (required)")
	baseline := flag.Bool("baseline", false,
		"report which migrations can be proven already applied; makes no changes to the database")
	baselineAndStamp := flag.Bool("baseline-and-stamp", false,
		"detect and stamp already-applied migrations; does not run any migrations")
	baselineStampMigrate := flag.Bool("baseline-stamp-migrate", false,
		"detect and stamp already-applied migrations, then run all remaining migrations")
	flag.Parse()

	if *dbPath == "" {
		log.Fatalf("[migrate] -db is required")
	}

	count := 0
	for _, b := range []bool{*baseline, *baselineAndStamp, *baselineStampMigrate} {
		if b {
			count++
		}
	}
	if count != 1 {
		log.Fatalf("[migrate] exactly one of -baseline, -baseline-and-stamp, or -baseline-stamp-migrate is required")
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

	switch {
	case *baseline:
		runBaseline(db, *dbPath)
	case *baselineAndStamp:
		runBaselineAndStamp(db, *dbPath)
	case *baselineStampMigrate:
		runBaselineStampMigrate(db, *dbPath)
	}
}

// runBaseline reports what's detectable without changing anything —
// dry-run mode.
func runBaseline(db *sql.DB, dbPath string) {
	result, err := database.Baseline(db, log.Printf)
	if err != nil {
		log.Fatalf("baseline %s: %v", dbPath, err)
	}
	printBaselineResult(dbPath, result)
	log.Printf("dry run only — nothing was written to %s", dbPath)
}

// runBaselineAndStamp detects and stamps whichever individual
// migrations can be proven already applied. Does not run migrations —
// the database is not expected to be fully migrated afterward. The
// ingestor finishes that at its own startup.
func runBaselineAndStamp(db *sql.DB, dbPath string) {
	result, err := database.BaselineAndStamp(db, log.Printf)
	if err != nil {
		log.Fatalf("baseline-and-stamp %s: %v", dbPath, err)
	}
	printBaselineResult(dbPath, result)
}

// runBaselineStampMigrate stamps whatever's detectable, then runs the
// remaining migrations for real, leaving db fully at head.
func runBaselineStampMigrate(db *sql.DB, dbPath string) {
	result, err := database.BaselineStampMigrate(db, log.Printf)
	if err != nil {
		log.Fatalf("baseline-stamp-migrate %s: %v", dbPath, err)
	}
	printBaselineResult(dbPath, result)

	if err := database.NormalizePublicKeyCasing(db); err != nil {
		log.Fatalf("normalize public_key casing: %v", err)
	}
	if err := database.AssertBaselined(db); err != nil {
		log.Fatalf("database not baselined: %v", err)
	}
	log.Printf("OK: %s is migrated and ready", dbPath)
}

func printBaselineResult(dbPath string, r database.BaselineResult) {
	log.Printf("%s:", dbPath)
	log.Printf("  already tracked (%d): %v", len(r.AlreadyTracked), r.AlreadyTracked)
	log.Printf("  newly stamped   (%d): %v", len(r.NewlyStamped), r.NewlyStamped)
	log.Printf("  still pending   (%d): %v", len(r.StillPending), r.StillPending)
}
