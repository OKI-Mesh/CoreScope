package database

import (
	"database/sql"
	"fmt"
	"log"
)

// NormalizePublicKeyCasing lowercases any nodes.public_key rows that
// aren't already lowercase. #1483: the server's GetNodeLocationsByKeys
// lookup dropped LOWER(public_key) for perf and now relies on stored
// keys being lowercase. Runs unconditionally on every boot (not gated
// by _migrations or goose — this was applySchema's original design and
// is preserved as-is) — cheap once normalized, since subsequent passes
// match zero rows.
func NormalizePublicKeyCasing(rw *sql.DB) error {
	var n int64
	if err := rw.QueryRow("SELECT COUNT(*) FROM nodes WHERE public_key != lower(public_key)").Scan(&n); err != nil {
		return fmt.Errorf("count uppercase public_key rows: %w", err)
	}
	if n == 0 {
		return nil
	}
	log.Printf("[migration] Normalizing %d nodes.public_key row(s) to lowercase (#1483)...", n)
	if _, err := rw.Exec(`UPDATE nodes SET public_key = lower(public_key) WHERE public_key != lower(public_key)`); err != nil {
		return fmt.Errorf("normalize public_key casing: %w", err)
	}
	log.Printf("[migration] public_key lowercase normalize complete (%d rows)", n)
	return nil
}
