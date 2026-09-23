// internal/database/observers.go
package database

import (
	"database/sql"
	"strings"
)

// SoftDeleteBlacklistedObservers marks the given observer IDs as
// inactive=1 (case-insensitive match). Returns the count of rows
// affected. Writer-side helper; the ingestor calls this at startup
// with the operator's configured blacklist.
func SoftDeleteBlacklistedObservers(rw *sql.DB, blacklist []string) (int64, error) {
	placeholders := make([]string, 0, len(blacklist))
	args := make([]interface{}, 0, len(blacklist))
	for _, pk := range blacklist {
		t := strings.TrimSpace(pk)
		if t == "" {
			continue
		}
		placeholders = append(placeholders, "LOWER(?)")
		args = append(args, t)
	}
	if len(placeholders) == 0 {
		return 0, nil
	}
	q := "UPDATE observers SET inactive = 1 WHERE LOWER(id) IN (" +
		strings.Join(placeholders, ",") + ") AND (inactive IS NULL OR inactive = 0)"
	res, err := rw.Exec(q, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}
