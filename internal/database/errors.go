package database

import (
	"errors"

	"github.com/jackc/pgx/v5"
)

// ErrNotFound is returned when a single-row lookup finds nothing. Callers test
// for it with errors.Is; the driver's error type never leaves this package.
var ErrNotFound = errors.New("not found")

// notFound maps the driver's "no rows" error to ErrNotFound and passes every
// other error through unchanged.
func notFound(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return err
}
