package database

import (
	"database/sql"
	"os"
	"sort"
	"testing"
)

var excludedFromSchemaComparison = []string{"goose_db_version", "_async_migrations", "_migrations"}

func isExcludedTable(name string) bool {
	for _, ex := range excludedFromSchemaComparison {
		if name == ex {
			return true
		}
	}
	return false
}

// TestMigratedSchemaMatchesProd (extended) — also compares indexes
// and views, not just tables/columns.
func TestMigratedSchemaMatchesProd(t *testing.T) {
	const prodDumpPath = "test-fixtures/prod_schema.sql"

	prod := newMemoryDB(t)
	prodSQL, err := os.ReadFile(prodDumpPath)
	if err != nil {
		t.Skipf("no prod schema fixture at %s, skipping: %v", prodDumpPath, err)
	}
	execRaw(t, prod, string(prodSQL))

	fresh := newMemoryDB(t)
	if _, err := BaselineStampMigrate(fresh, nil); err != nil {
		t.Fatalf("migrating fresh db: %v", err)
	}

	compareTablesAndColumns(t, prod, fresh)
	compareIndexes(t, prod, fresh)
	compareViews(t, prod, fresh)
}

func compareTablesAndColumns(t *testing.T, prod *sql.DB, fresh *sql.DB) {

	prodTables := allTableNames(t, prod)
	freshTables := allTableNames(t, fresh)

	prodSet := toSet(prodTables)
	freshSet := toSet(freshTables)

	for _, tbl := range prodTables {
		if !freshSet[tbl] {
			t.Errorf("table %q exists in prod but not in migrated schema", tbl)
		}
	}
	for _, tbl := range freshTables {
		if !prodSet[tbl] {
			t.Errorf("table %q exists in migrated schema but not in prod", tbl)
		}
	}

	for _, tbl := range prodTables {
		if !freshSet[tbl] {
			continue // already reported
		}
		prodCols, err := TableColumns(prod, tbl)
		if err != nil {
			t.Errorf("table %q: reading prod columns: %v", tbl, err)
			continue
		}
		freshCols, err := TableColumns(fresh, tbl)
		if err != nil {
			t.Errorf("table %q: reading fresh columns: %v", tbl, err)
			continue
		}
		for col, prodType := range prodCols {
			freshType, ok := freshCols[col]
			if !ok {
				t.Errorf("table %q: column %q exists in prod but not in migrated schema", tbl, col)
				continue
			}
			if freshType != prodType {
				t.Errorf("table %q: column %q type mismatch: prod=%q migrated=%q", tbl, col, prodType, freshType)
			}
		}
		for col := range freshCols {
			if _, ok := prodCols[col]; !ok {
				t.Errorf("table %q: column %q exists in migrated schema but not in prod", tbl, col)
			}
		}
	}
}

func allTableNames(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		t.Fatalf("listing tables: %v", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		if isExcludedTable(n) {
			continue
		}
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func toSet(items []string) map[string]bool {
	m := make(map[string]bool, len(items))
	for _, i := range items {
		m[i] = true
	}
	return m
}

// compareIndexes compares the set of explicitly-named indexes per
// table. sqlite_autoindex_* (auto-generated for UNIQUE constraints) are
// excluded — their numeric suffix depends on declaration order and
// isn't something a migration controls directly; the UNIQUE constraint
// itself is already covered by compareTablesAndColumns if it affects
// column definitions, and by comparing CREATE TABLE text isn't done
// here, so a UNIQUE constraint added/removed without a matching
// explicit index name change could slip past this specific check —
// see note below.
func compareIndexes(t *testing.T, prod, fresh *sql.DB) {
	t.Helper()
	prodIdx := namedIndexesByTable(t, prod)
	freshIdx := namedIndexesByTable(t, fresh)

	allTables := map[string]bool{}
	for tbl := range prodIdx {
		allTables[tbl] = true
	}
	for tbl := range freshIdx {
		allTables[tbl] = true
	}

	for tbl := range allTables {
		p := prodIdx[tbl]
		f := freshIdx[tbl]
		pSet := toSet(p)
		fSet := toSet(f)
		for _, idx := range p {
			if !fSet[idx] {
				t.Errorf("table %q: index %q exists in prod but not in migrated schema", tbl, idx)
			}
		}
		for _, idx := range f {
			if !pSet[idx] {
				t.Errorf("table %q: index %q exists in migrated schema but not in prod", tbl, idx)
			}
		}
	}
}

// namedIndexesByTable returns table name -> sorted list of explicitly
// named index names (excluding sqlite_autoindex_*).
func namedIndexesByTable(t *testing.T, db *sql.DB) map[string][]string {
	t.Helper()
	rows, err := db.Query(`
		SELECT tbl_name, name FROM sqlite_master
		WHERE type = 'index' AND name NOT LIKE 'sqlite_autoindex_%'`)
	if err != nil {
		t.Fatalf("listing indexes: %v", err)
	}
	defer rows.Close()

	out := map[string][]string{}
	for rows.Next() {
		var tbl, name string
		if err := rows.Scan(&tbl, &name); err != nil {
			t.Fatal(err)
		}
		out[tbl] = append(out[tbl], name)
	}
	for tbl := range out {
		sort.Strings(out[tbl])
	}
	return out
}

// compareViews compares the set of view names (existence only — not
// their SELECT body, since two views can be written differently but
// produce equivalent output; a text diff there would be noisy and
// isn't usually what you want to catch here).
func compareViews(t *testing.T, prod, fresh *sql.DB) {
	t.Helper()
	prodViews := toSet(viewNames(t, prod))
	freshViews := toSet(viewNames(t, fresh))

	for v := range prodViews {
		if !freshViews[v] {
			t.Errorf("view %q exists in prod but not in migrated schema", v)
		}
	}
	for v := range freshViews {
		if !prodViews[v] {
			t.Errorf("view %q exists in migrated schema but not in prod", v)
		}
	}
}

func viewNames(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type = 'view'`)
	if err != nil {
		t.Fatalf("listing views: %v", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		names = append(names, n)
	}
	return names
}
