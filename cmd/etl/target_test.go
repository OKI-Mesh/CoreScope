package main

import (
	"testing"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgtest"
)

func TestMain(m *testing.M) {
	pgtest.Run(m)
}

func newTarget(t *testing.T) string {
	t.Helper()
	return pgtest.NewDatabase(t)
}
