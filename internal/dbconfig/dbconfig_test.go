package dbconfig

import "testing"

func TestDSN(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://env/db")

	var missing *DBConfig
	if got := missing.DSN(); got != "postgres://env/db" {
		t.Errorf("nil config DSN = %q, want the DATABASE_URL value", got)
	}
	if got := (&DBConfig{}).DSN(); got != "postgres://env/db" {
		t.Errorf("empty config DSN = %q, want the DATABASE_URL value", got)
	}
	if got := (&DBConfig{ConnectionString: "postgres://cfg/db"}).DSN(); got != "postgres://cfg/db" {
		t.Errorf("configured DSN = %q, want the connectionString value", got)
	}

	t.Setenv("DATABASE_URL", "")
	if got := missing.DSN(); got != "" {
		t.Errorf("nil config with no env DSN = %q, want empty", got)
	}
}
