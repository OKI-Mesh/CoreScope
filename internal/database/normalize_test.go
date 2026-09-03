package database

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

// TestNormalizePublicKeyCasing_NormalizesUppercaseRows confirms that
// any nodes.public_key rows with uppercase characters are lowercased.
func TestNormalizePublicKeyCasing_NormalizesUppercaseRows(t *testing.T) {
	conn := setupNodesFixture(t)

	if _, err := conn.Exec(
		`INSERT INTO nodes (public_key, name) VALUES ('AABBCCDDEEFF11223344', 'mixed-case-node')`,
	); err != nil {
		t.Fatalf("seed uppercase row: %v", err)
	}
	if _, err := conn.Exec(
		`INSERT INTO nodes (public_key, name) VALUES ('aabbccddeeff11223344aa', 'already-lowercase-node')`,
	); err != nil {
		t.Fatalf("seed lowercase row: %v", err)
	}

	if err := NormalizePublicKeyCasing(conn); err != nil {
		t.Fatalf("NormalizePublicKeyCasing: %v", err)
	}

	var uppercaseCount int
	if err := conn.QueryRow(`SELECT COUNT(*) FROM nodes WHERE public_key = 'AABBCCDDEEFF11223344'`).Scan(&uppercaseCount); err != nil {
		t.Fatal(err)
	}
	if uppercaseCount != 0 {
		t.Errorf("expected 0 uppercase rows after normalization, got %d", uppercaseCount)
	}

	var lowercaseCount int
	if err := conn.QueryRow(`SELECT COUNT(*) FROM nodes WHERE public_key = 'aabbccddeeff11223344'`).Scan(&lowercaseCount); err != nil {
		t.Fatal(err)
	}
	if lowercaseCount != 1 {
		t.Errorf("expected 1 row with normalized lowercase key, got %d", lowercaseCount)
	}
}

// TestNormalizePublicKeyCasing_NoOpWhenAlreadyLowercase confirms that
// running normalization against an already-clean table doesn't error
// or mutate anything.
func TestNormalizePublicKeyCasing_NoOpWhenAlreadyLowercase(t *testing.T) {
	conn := setupNodesFixture(t)

	if _, err := conn.Exec(
		`INSERT INTO nodes (public_key, name) VALUES ('aabbccddeeff11223344', 'clean-node')`,
	); err != nil {
		t.Fatalf("seed row: %v", err)
	}

	if err := NormalizePublicKeyCasing(conn); err != nil {
		t.Fatalf("NormalizePublicKeyCasing (first run): %v", err)
	}
	if err := NormalizePublicKeyCasing(conn); err != nil {
		t.Fatalf("NormalizePublicKeyCasing (second run, should be no-op): %v", err)
	}

	var count int
	if err := conn.QueryRow(`SELECT COUNT(*) FROM nodes`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected exactly 1 row (no duplication/loss), got %d", count)
	}
}

// TestNormalizePublicKeyCasing_EmptyTable confirms no error against a
// table with zero rows.
func TestNormalizePublicKeyCasing_EmptyTable(t *testing.T) {
	conn := setupNodesFixture(t)

	if err := NormalizePublicKeyCasing(conn); err != nil {
		t.Fatalf("NormalizePublicKeyCasing on empty table: %v", err)
	}
}

// setupNodesFixture opens a temp SQLite DB with a bare nodes table —
// only the columns NormalizePublicKeyCasing actually touches.
func setupNodesFixture(t *testing.T) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "normalize_test.db")
	conn, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	if _, err := conn.Exec(`CREATE TABLE nodes (public_key TEXT PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatal(err)
	}
	return conn
}
