package database

import (
	"database/sql"
	"testing"
)

// --- checkFunc building blocks ---

func TestHasTable(t *testing.T) {
	db := newMemoryDB(t)
	execRaw(t, db, `CREATE TABLE nodes (public_key TEXT, lat REAL)`)

	ok, err := hasTable("nodes", map[string]string{"public_key": "TEXT", "lat": "REAL"})(db)
	if err != nil || !ok {
		t.Fatalf("expected pass, got ok=%v err=%v", ok, err)
	}
	ok, err = hasTable("nodes", map[string]string{"public_key": "INTEGER"})(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("expected fail on type mismatch")
	}
	ok, err = hasTable("ghost", map[string]string{"x": "TEXT"})(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("expected fail for missing table")
	}
}

func TestHasColumn(t *testing.T) {
	db := newMemoryDB(t)
	execRaw(t, db, `CREATE TABLE observers (id TEXT)`)

	ok, err := hasColumn("observers", "id", "TEXT")(db)
	if err != nil || !ok {
		t.Fatalf("expected pass, got ok=%v err=%v", ok, err)
	}
	ok, err = hasColumn("observers", "cpu_temp_c", "REAL")(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("expected fail for missing column")
	}
}

func TestHasIndex(t *testing.T) {
	db := newMemoryDB(t)
	execRaw(t, db, `CREATE TABLE nodes (last_seen TEXT)`)
	execRaw(t, db, `CREATE INDEX idx_nodes_last_seen ON nodes(last_seen)`)

	ok, err := hasIndex("idx_nodes_last_seen")(db)
	if err != nil || !ok {
		t.Fatalf("expected pass, got ok=%v err=%v", ok, err)
	}
	ok, err = hasIndex("idx_ghost")(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("expected fail for missing index")
	}
}

func TestHasView(t *testing.T) {
	db := newMemoryDB(t)
	execRaw(t, db, `CREATE TABLE nodes (public_key TEXT)`)
	execRaw(t, db, `CREATE VIEW packets_v AS SELECT * FROM nodes`)

	ok, err := hasView("packets_v")(db)
	if err != nil || !ok {
		t.Fatalf("expected pass, got ok=%v err=%v", ok, err)
	}
}

func TestAll(t *testing.T) {
	db := newMemoryDB(t)
	execRaw(t, db, `CREATE TABLE nodes (public_key TEXT)`)
	execRaw(t, db, `CREATE INDEX idx_nodes_pk ON nodes(public_key)`)

	ok, err := all(
		hasTable("nodes", map[string]string{"public_key": "TEXT"}),
		hasIndex("idx_nodes_pk"),
	)(db)
	if err != nil || !ok {
		t.Fatalf("expected pass, got ok=%v err=%v", ok, err)
	}
	ok, err = all(
		hasTable("nodes", map[string]string{"public_key": "TEXT"}),
		hasIndex("idx_ghost"),
	)(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("expected fail when one check fails")
	}
}

// --- schemaChecks well-formedness ---

func TestSchemaChecksWellFormed(t *testing.T) {
	for v, sc := range schemaChecks {
		hasCheck := sc.Check != nil
		hasLegacy := sc.LegacyName != ""
		if !hasCheck && !hasLegacy {
			t.Errorf("schemaChecks[%d]: neither Check nor LegacyName set", v)
		}
		if hasCheck && hasLegacy {
			t.Errorf("schemaChecks[%d]: both Check and LegacyName set, expected exactly one", v)
		}
	}
}

// TestSchemaChecksCoverAdoptionRange ensures every version from 1
// through gooseAdoptionVersion has an entry — a gap here would cause
// AssertBaselined to report versions as "missing" that were simply
// never given a check, rather than genuinely absent.
func TestSchemaChecksCoverAdoptionRange(t *testing.T) {
	for v := int64(1); v <= GooseAdoptionVersion; v++ {
		if _, ok := schemaChecks[v]; !ok {
			t.Errorf("schemaChecks missing entry for version %d (required through %d)", v, GooseAdoptionVersion)
		}
	}
}

// --- loadLegacyAppliedNames ---

func TestLoadLegacyAppliedNames(t *testing.T) {
	db := newMemoryDB(t)
	seedLegacyMigrations(t, db, "advert_count_unique_v1", "noise_floor_real_v1")

	applied, err := loadLegacyAppliedNames(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !applied["advert_count_unique_v1"] {
		t.Error("expected advert_count_unique_v1 to be applied")
	}
	if applied["never_ran"] {
		t.Error("expected unrun migration to be false")
	}
}

func TestLoadLegacyAppliedNames_NoTable(t *testing.T) {
	db := newMemoryDB(t)
	applied, err := loadLegacyAppliedNames(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(applied) != 0 {
		t.Errorf("expected empty set, got %v", applied)
	}
}

// --- StampVersions ---

func TestStampVersions(t *testing.T) {
	db := newMemoryDB(t)
	if err := StampVersions(db, []int64{1, 3, 5}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, v := range []int64{1, 3, 5} {
		var applied bool
		if err := db.QueryRow(`SELECT is_applied FROM goose_db_version WHERE version_id = ?`, v).Scan(&applied); err != nil {
			t.Fatalf("v%d: %v", v, err)
		}
		if !applied {
			t.Errorf("v%d: expected is_applied = true", v)
		}
	}
	for _, v := range []int64{2, 4} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM goose_db_version WHERE version_id = ?`, v).Scan(&count); err != nil {
			t.Fatalf("v%d: %v", v, err)
		}
		if count != 0 {
			t.Errorf("v%d: expected gap preserved, found a row", v)
		}
	}
}

func TestStampVersions_Idempotent(t *testing.T) {
	db := newMemoryDB(t)
	if err := StampVersions(db, []int64{1, 2}); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := StampVersions(db, []int64{1, 2, 3}); err != nil {
		t.Fatalf("second: %v", err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM goose_db_version WHERE version_id = 1`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected exactly one row for v1, got %d", count)
	}
}

// --- Baseline (dry run) ---

func TestBaseline_FreshDatabase_NoSideEffects(t *testing.T) {
	db := newMemoryDB(t)

	result, err := Baseline(db, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.NewlyStamped) != 0 {
		t.Errorf("expected nothing detected on fresh db, got %v", result.NewlyStamped)
	}
	ver, err := CurrentSchemaVersion(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ver != 0 {
		t.Errorf("Baseline must not write anything; expected version 0, got %d", ver)
	}
}

func TestBaseline_DetectsV1ViaSchema(t *testing.T) {
	db := newMemoryDB(t)
	SeedUnstampedSchema(db, 1)

	result, err := Baseline(db, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !containsVersion(result.NewlyStamped, 1) {
		t.Errorf("expected v1 detected, got %v", result.NewlyStamped)
	}
	ver, _ := CurrentSchemaVersion(db)
	if ver != 0 {
		t.Errorf("Baseline must not write anything; expected version 0, got %d", ver)
	}
}

func TestBaseline_DetectsLegacyDataOnlyMigrationIndependently(t *testing.T) {
	db := newMemoryDB(t)
	seedLegacyMigrations(t, db, "advert_count_unique_v1")

	result, err := Baseline(db, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !containsVersion(result.NewlyStamped, 5) {
		t.Errorf("expected v5 detected via legacy table, got %v", result.NewlyStamped)
	}
}

// --- BaselineAndStamp ---

func TestBaselineAndStamp_WritesStampedVersions(t *testing.T) {
	db := newMemoryDB(t)
	SeedUnstampedSchema(db, 1)

	result, err := BaselineAndStamp(db, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.NewlyStamped) == 0 {
		t.Fatal("expected at least v1 detected")
	}
	ver, err := CurrentSchemaVersion(db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ver == 0 {
		t.Error("expected goose_db_version to reflect the stamp; got version 0")
	}

	result2, err := BaselineAndStamp(db, nil)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if len(result2.NewlyStamped) != 0 {
		t.Errorf("expected nothing newly stamped on second call, got %v", result2.NewlyStamped)
	}
	if len(result2.AlreadyTracked) == 0 {
		t.Error("expected previously stamped versions to show as AlreadyTracked on second call")
	}
}

// --- BaselineStampMigrate ---

func TestBaselineStampMigrate_FreshDatabase(t *testing.T) {
	db := newMemoryDB(t)

	result, err := BaselineStampMigrate(db, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.NewlyStamped) != 0 {
		t.Errorf("expected nothing pre-stamped on a fresh db, got %v", result.NewlyStamped)
	}
	if err := AssertReady(db); err != nil {
		t.Errorf("expected db fully migrated and ready, got: %v", err)
	}
}

func TestBaselineStampMigrate_RawSQLMatchingV1(t *testing.T) {
	db := newMemoryDB(t)
	if err := SeedUnstampedSchema(db, 1); err != nil {
		t.Fatalf("seeding unstamped v1 schema: %v", err)
	}

	result, err := BaselineStampMigrate(db, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !containsVersion(result.NewlyStamped, 1) {
		t.Errorf("expected v1 detected and stamped, got %v", result.NewlyStamped)
	}
	if err := AssertReady(db); err != nil {
		t.Errorf("expected db fully migrated after baseline, got: %v", err)
	}
}

func TestBaselineStampMigrate_GapFilledOutOfOrder(t *testing.T) {
	db := newMemoryDB(t)
	SeedUnstampedSchema(db, 1)
	seedLegacyMigrations(t, db, "advert_count_unique_v1")

	if _, err := BaselineStampMigrate(db, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := AssertReady(db); err != nil {
		t.Errorf("expected db fully migrated despite the gap, got: %v", err)
	}
}

// --- AssertBaselined ---

func TestAssertBaselined_FreshDatabase(t *testing.T) {
	db := newMemoryDB(t)
	if err := AssertBaselined(db); err != nil {
		t.Errorf("expected fresh database to pass AssertBaselined, got: %v", err)
	}
}

func TestAssertBaselined_PreexistingSchemaUnstamped(t *testing.T) {
	db := newMemoryDB(t)
	SeedUnstampedSchema(db, 1)

	if err := AssertBaselined(db); err == nil {
		t.Error("expected AssertBaselined to fail on pre-existing, unstamped schema")
	}
}

func TestAssertBaselined_FullyBaselined(t *testing.T) {
	db := newMemoryDB(t)
	if _, err := BaselineStampMigrate(db, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := AssertBaselined(db); err != nil {
		t.Errorf("expected fully migrated database to pass, got: %v", err)
	}
}

func TestAssertBaselined_PartialGapBelowThreshold(t *testing.T) {
	db := newMemoryDB(t)
	SeedUnstampedSchema(db, 1)
	// Stamp only v1 — leave 2 through 31 as gaps despite the table
	// existing, simulating a partial/failed baseline run.
	if err := StampVersions(db, []int64{1}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := AssertBaselined(db); err == nil {
		t.Error("expected AssertBaselined to fail with a partial gap below the adoption threshold")
	}
}

func TestSeedUnstampedSchema_StopsAtRequestedVersion(t *testing.T) {
	db := newMemoryDB(t)
	if err := SeedUnstampedSchema(db, 1); err != nil {
		t.Fatalf("seeding: %v", err)
	}
	// Migration 6 adds nodes.battery_mv — must NOT exist if seeding
	// genuinely stopped at version 1.
	cols, err := TableColumns(db, "nodes")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := cols["battery_mv"]; ok {
		t.Error("SeedUnstampedSchema(db, 1) applied migration 6's battery_mv column — UpTo did not stop at version 1")
	}
}

// --- shared fixtures ---

func containsVersion(versions []int64, target int64) bool {
	for _, v := range versions {
		if v == target {
			return true
		}
	}
	return false
}

// SeedLegacyMigrations creates the old system's _migrations table and
// inserts the given names, simulating a database the old system had
// already migrated.
func seedLegacyMigrations(t *testing.T, db *sql.DB, names ...string) {
	t.Helper()
	execRaw(t, db, `CREATE TABLE IF NOT EXISTS _migrations (name TEXT PRIMARY KEY)`)
	for _, n := range names {
		if _, err := db.Exec(`INSERT INTO _migrations (name) VALUES (?)`, n); err != nil {
			t.Fatalf("seed legacy migration %q: %v", n, err)
		}
	}
}
