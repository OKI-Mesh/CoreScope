package main

import (
	"os"
	"testing"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgtest"
)

func TestMain(m *testing.M) { os.Exit(pgtest.Run(m)) }

func newTarget(t *testing.T) string {
	t.Helper()
	return pgtest.NewDatabase(t)
}
