package database_test

import (
	"testing"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
	"github.com/OKI-Mesh/CoreScope/internal/database/pgtest"
)

func TestMain(m *testing.M) { pgtest.Run(m) }

func newDB(t *testing.T) *database.DB { return dbtest.New(t) }
