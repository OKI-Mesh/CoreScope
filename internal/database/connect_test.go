package database_test

import (
	"testing"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/pgtest"
)

func TestOpenReadWrite_Connects(t *testing.T) {
	ctx := t.Context()
	db, err := database.OpenReadWrite(ctx, pgtest.NewDatabase(t))
	if err != nil {
		t.Fatalf("OpenReadWrite(): %v", err)
	}
	defer db.Close()

	if err := db.Pool().Ping(ctx); err != nil {
		t.Errorf("expected pingable connection, got: %v", err)
	}
}

func TestOpenReadOnly_Connects(t *testing.T) {
	ctx := t.Context()
	dsn := pgtest.NewDatabase(t)
	rw, err := database.OpenReadWrite(ctx, dsn)
	if err != nil {
		t.Fatalf("setup OpenReadWrite(): %v", err)
	}
	rw.Close()

	ro, err := database.OpenReadOnly(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenReadOnly(): %v", err)
	}
	defer ro.Close()

	if err := ro.Pool().Ping(ctx); err != nil {
		t.Errorf("expected pingable connection, got: %v", err)
	}
}

func TestOpenReadOnly_MissingDatabase(t *testing.T) {
	ctx := t.Context()
	dsn := pgtest.WithDatabase(pgtest.NewDatabase(t), "does_not_exist")
	ro, err := database.OpenReadOnly(ctx, dsn)
	if err != nil {
		return
	}
	ro.Close()
	t.Fatal("expected OpenReadOnly to fail against a nonexistent database")
}
