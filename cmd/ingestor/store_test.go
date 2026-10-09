package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/decoder"
	"github.com/OKI-Mesh/CoreScope/internal/packetpath"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const templateDB = "corescope_template"

func TestMain(m *testing.M) { os.Exit(runTests(m)) }

var (
	adminPool   *pgxpool.Pool // connected to the maintenance database, used to create and drop test databases
	baseConnStr string
	dbSeq       atomic.Int64
	storeConn   sync.Map // *Store -> connection string, for testConnStr
)

// The image the tests run against. It defaults to the prod tag. On Apple
// Silicon the official postgis/postgis has no arm64 build, so set
// TEST_POSTGRES_IMAGE to an arm64-capable build of the same versions.
var postgresImage = envOr("TEST_POSTGRES_IMAGE", "postgis/postgis:18-3.6")

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// runTests exists so the deferred cleanups run before os.Exit.
func runTests(m *testing.M) int {
	ctx := context.Background()

	ctr, err := postgres.Run(ctx, postgresImage,
		postgres.WithDatabase("postgres"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(90*time.Second)),
	)
	if err != nil {
		// Fail loudly. Skipping every database test when Docker is missing would hide everything in CI.
		fmt.Fprintf(os.Stderr, "starting postgres container: %v\n", err)
		return 1
	}
	defer ctr.Terminate(ctx)

	if baseConnStr, err = ctr.ConnectionString(ctx, "sslmode=disable"); err != nil {
		fmt.Fprintf(os.Stderr, "connection string: %v\n", err)
		return 1
	}
	if adminPool, err = pgxpool.New(ctx, baseConnStr); err != nil {
		fmt.Fprintf(os.Stderr, "admin pool: %v\n", err)
		return 1
	}
	defer adminPool.Close()

	// Migrate once, into the template, through the real goose path.
	if _, err := adminPool.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{templateDB}.Sanitize()); err != nil {
		fmt.Fprintf(os.Stderr, "create template: %v\n", err)
		return 1
	}
	if err := database.Migrate(ctx, connStrFor(templateDB)); err != nil {
		fmt.Fprintf(os.Stderr, "migrating template: %v\n", err)
		return 1
	}
	// A template can't be cloned while anything is connected to it.
	if _, err := adminPool.Exec(ctx,
		`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1`, templateDB); err != nil {
		fmt.Fprintf(os.Stderr, "releasing template: %v\n", err)
		return 1
	}

	return m.Run()
}

// newTestStore returns a Store backed by its own copy of the migrated schema.
// It starts no background goroutines. If the constructor ever does, tests that
// capture the global logger can race them again (#2065), so keep tickers and
// backfills in main().
func newTestStore(t testing.TB) *Store {
	t.Helper()
	ctx := context.Background() // t.Context() is already cancelled when cleanups run

	name := fmt.Sprintf("t_%d", dbSeq.Add(1))
	quoted := pgx.Identifier{name}.Sanitize()
	if _, err := adminPool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s TEMPLATE %s",
		quoted, pgx.Identifier{templateDB}.Sanitize())); err != nil {
		t.Fatalf("cloning test database: %v", err)
	}

	connStr := connStrFor(name)
	s, err := OpenStore(ctx, connStr) // whatever opens a Store from a connection string; migrations are already in the template
	if err != nil {
		t.Fatal(err)
	}
	storeConn.Store(s, connStr)

	t.Cleanup(func() {
		s.Close()
		storeConn.Delete(s)
		if _, err := adminPool.Exec(context.Background(),
			"DROP DATABASE IF EXISTS "+quoted+" WITH (FORCE)"); err != nil {
			t.Logf("dropping %s: %v", name, err)
		}
	})
	return s
}

// connStrFor returns baseConnStr pointed at another database in the container.
func connStrFor(db string) string {
	u, err := url.Parse(baseConnStr)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + db
	return u.String()
}

// testConnStr returns the connection string for a test store's database.
func testConnStr(t testing.TB, s *Store) string {
	t.Helper()
	v, ok := storeConn.Load(s)
	if !ok {
		t.Fatal("testConnStr: store was not created by newTestStore")
	}
	return v.(string)
}

// newTestDatabase clones the migrated template into a fresh database and
// returns its connection string. The database is dropped when the test ends.
func newTestDatabase(t testing.TB) string {
	t.Helper()
	if adminPool == nil {
		t.Fatal("no test container: this package needs the harness TestMain (adminPool is nil)")
	}
	name := fmt.Sprintf("t_%d", dbSeq.Add(1))
	quoted := pgx.Identifier{name}.Sanitize()
	if _, err := adminPool.Exec(t.Context(), fmt.Sprintf("CREATE DATABASE %s TEMPLATE %s",
		quoted, pgx.Identifier{templateDB}.Sanitize())); err != nil {
		t.Fatalf("cloning test database: %v", err)
	}
	t.Cleanup(func() {
		if _, err := adminPool.Exec(context.Background(),
			"DROP DATABASE IF EXISTS "+quoted+" WITH (FORCE)"); err != nil {
			t.Logf("dropping %s: %v", name, err)
		}
	})
	return connStrFor(name)
}

func TestOpenStore(t *testing.T) {
	// A fresh clone of the migrated template, but no Store yet:
	// this test is about OpenStore itself.
	connStr := newTestDatabase(t)

	s, err := OpenStore(t.Context(), connStr)
	if err != nil {
		t.Fatal(err)
	}
	// Runs before the database drop registered by newTestDatabase, so the
	// pool is closed by the time the clone is removed.
	defer s.Close()

	// Table and view existence are covered by TestGooseMigrationsAppliedOnFreshDB.
	if err := s.db.Pool().Ping(t.Context()); err != nil {
		t.Fatalf("store opened but the pool can't reach the database: %v", err)
	}
}
func TestPacketsViewQueryable(t *testing.T) {
	s := newTestStore(t)
	defer s.Close()
	ctx := t.Context()

	// Insert observer so the LEFT JOIN resolves
	if err := s.UpsertObserver(ctx, "obs1", "TestObserver", "SJC", nil); err != nil {
		t.Fatal(err)
	}

	snr := 3.5
	rssi := -95.0
	data := &PacketData{
		RawHex:      "AABB",
		Timestamp:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ObserverID:  "obs1",
		Hash:        "viewtesthash",
		RouteType:   1,
		PayloadType: 4,
		PathJSON:    "[]",
		DecodedJSON: `{"type":"ADVERT"}`,
		SNR:         &snr,
		RSSI:        &rssi,
	}
	if _, err := s.InsertTransmission(ctx, data); err != nil {
		t.Fatal(err)
	}

	// Query through packets_v — the view the Go server relies on
	var obsID, obsName sql.NullString
	var hash string
	mustScan(t, testRow(t, s, "SELECT observer_id, observer_name, hash FROM packets_v LIMIT 1"), &obsID, &obsName, &hash)
	if hash != "viewtesthash" {
		t.Errorf("hash=%s, want viewtesthash", hash)
	}
	if !obsID.Valid || obsID.String != "obs1" {
		t.Errorf("observer_id=%v, want obs1", obsID)
	}
	if !obsName.Valid || obsName.String != "TestObserver" {
		t.Errorf("observer_name=%v, want TestObserver", obsName)
	}
}

func TestUpsertNode(t *testing.T) {
	s := newTestStore(t)
	defer s.Close()
	ctx := t.Context()

	lat := 37.0
	lon := -122.0
	if err := s.UpsertNode(ctx, "aabbccdd", "TestNode", "repeater", &lat, &lon, time.Date(2026, 3, 25, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}

	var name, role string
	mustScan(t, testRow(t, s, "SELECT name, role FROM nodes WHERE public_key = 'aabbccdd'"), &name, &role)
	if name != "TestNode" {
		t.Errorf("name=%s, want TestNode", name)
	}
	if role != "repeater" {
		t.Errorf("role=%s, want repeater", role)
	}

	// Upsert again — should update
	if err := s.UpsertNode(ctx, "aabbccdd", "UpdatedNode", "repeater", &lat, &lon, time.Date(2026, 3, 25, 1, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}

	mustScan(t, testRow(t, s, "SELECT name FROM nodes WHERE public_key = 'aabbccdd'"), &name)
	if name != "UpdatedNode" {
		t.Errorf("after upsert name=%s, want UpdatedNode", name)
	}

	// UpsertNode does not modify advert_count (IncrementAdvertCount is separate)
	var count int
	mustScan(t, testRow(t, s, "SELECT advert_count FROM nodes WHERE public_key = 'aabbccdd'"), &count)
	if count != 0 {
		t.Errorf("advert_count=%d, want 0 (UpsertNode does not increment)", count)
	}
}

func TestUpsertObserver(t *testing.T) {
	s := newTestStore(t)
	defer s.Close()
	ctx := t.Context()
	if err := s.UpsertObserver(ctx, "obs1", "Observer1", " sjc ", nil); err != nil {
		t.Fatal(err)
	}

	var name, iata string
	mustScan(t, testRow(t, s, "SELECT name, iata FROM observers WHERE id = 'obs1'"), &name, &iata)
	if name != "Observer1" {
		t.Errorf("name=%s, want Observer1", name)
	}
	if iata != "SJC" {
		t.Errorf("iata=%s, want SJC", iata)
	}
}

func TestUpsertObserverWithMeta(t *testing.T) {
	s := newTestStore(t)
	defer s.Close()
	ctx := t.Context()

	battery := 3500
	uptime := int64(86400)
	noise := -115.5
	model := "L1"
	firmware := "v1.2.3"
	clientVersion := "2.4.1"
	radio := "SX1262"
	meta := &types.ObserverMeta{
		Model:         &model,
		Firmware:      &firmware,
		ClientVersion: &clientVersion,
		Radio:         &radio,
		BatteryMv:     &battery,
		UptimeSecs:    &uptime,
		NoiseFloor:    &noise,
	}

	if err := s.UpsertObserver(ctx, "obs1", "Observer1", "SJC", meta); err != nil {
		t.Fatal(err)
	}

	// Verify correct types in DB
	var batteryMv int
	var uptimeSecs int64
	var noiseFloor float64
	var gotModel, gotFirmware, gotClientVersion, gotRadio string
	mustScan(t, testRow(t, s, "SELECT model, firmware, client_version, radio, battery_mv, uptime_secs, noise_floor FROM observers WHERE id = 'obs1'"), &gotModel, &gotFirmware, &gotClientVersion, &gotRadio, &batteryMv, &uptimeSecs, &noiseFloor)
	if gotModel != model {
		t.Errorf("model=%s, want %s", gotModel, model)
	}
	if gotFirmware != firmware {
		t.Errorf("firmware=%s, want %s", gotFirmware, firmware)
	}
	if gotClientVersion != clientVersion {
		t.Errorf("client_version=%s, want %s", gotClientVersion, clientVersion)
	}
	if gotRadio != radio {
		t.Errorf("radio=%s, want %s", gotRadio, radio)
	}
	if batteryMv != 3500 {
		t.Errorf("battery_mv=%d, want 3500", batteryMv)
	}
	if uptimeSecs != 86400 {
		t.Errorf("uptime_secs=%d, want 86400", uptimeSecs)
	}
	if noiseFloor != -115.5 {
		t.Errorf("noise_floor=%f, want -115.5", noiseFloor)
	}

	// Verify typeof returns correct SQLite types
	var typBattery, typUptime, typNoise string
	mustScan(t, testRow(t, s, "SELECT typeof(battery_mv), typeof(uptime_secs), typeof(noise_floor) FROM observers WHERE id = 'obs1'"), &typBattery, &typUptime, &typNoise)
	if typBattery != "integer" {
		t.Errorf("typeof(battery_mv)=%s, want integer", typBattery)
	}
	if typUptime != "integer" {
		t.Errorf("typeof(uptime_secs)=%s, want integer", typUptime)
	}
	if typNoise != "real" {
		t.Errorf("typeof(noise_floor)=%s, want real", typNoise)
	}
}

func TestUpsertObserverMetaPreservesExisting(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()

	// First upsert with metadata
	battery := 3500
	noise := -115.5
	model := "L1"
	firmware := "v1.2.3"
	clientVersion := "2.4.1"
	radio := "SX1262"
	meta := &types.ObserverMeta{
		Model:         &model,
		Firmware:      &firmware,
		ClientVersion: &clientVersion,
		Radio:         &radio,
		BatteryMv:     &battery,
		NoiseFloor:    &noise,
	}
	if err := s.UpsertObserver(ctx, "obs1", "Observer1", "SJC", meta); err != nil {
		t.Fatal(err)
	}

	// Second upsert without metadata — should preserve existing values
	if err := s.UpsertObserver(ctx, "obs1", "Observer1", "SJC", nil); err != nil {
		t.Fatal(err)
	}

	var batteryMv int
	var noiseFloor float64
	var gotModel, gotFirmware, gotClientVersion, gotRadio string
	mustScan(t, testRow(t, s, "SELECT model, firmware, client_version, radio, battery_mv, noise_floor FROM observers WHERE id = 'obs1'"), &gotModel, &gotFirmware, &gotClientVersion, &gotRadio, &batteryMv, &noiseFloor)
	if gotModel != model {
		t.Errorf("model=%s after nil-meta upsert, want %s (preserved)", gotModel, model)
	}
	if gotFirmware != firmware {
		t.Errorf("firmware=%s after nil-meta upsert, want %s (preserved)", gotFirmware, firmware)
	}
	if gotClientVersion != clientVersion {
		t.Errorf("client_version=%s after nil-meta upsert, want %s (preserved)", gotClientVersion, clientVersion)
	}
	if gotRadio != radio {
		t.Errorf("radio=%s after nil-meta upsert, want %s (preserved)", gotRadio, radio)
	}
	if batteryMv != 3500 {
		t.Errorf("battery_mv=%d after nil-meta upsert, want 3500 (preserved)", batteryMv)
	}
	if noiseFloor != -115.5 {
		t.Errorf("noise_floor=%f after nil-meta upsert, want -115.5 (preserved)", noiseFloor)
	}
}

func TestExtractObserverMeta(t *testing.T) {
	// Float values from JSON (typical MQTT payload)
	msg := map[string]interface{}{
		"model":            "L1",
		"firmware_version": "v1.2.3",
		"clientVersion":    "2.4.1",
		"radio":            "SX1262",
		"battery_mv":       3500.0,
		"uptime_secs":      86400.0,
		"noise_floor":      -115.5,
	}
	meta := extractObserverMeta(msg)
	if meta == nil {
		t.Fatal("expected non-nil meta")
	}
	if meta.Model == nil || *meta.Model != "L1" {
		t.Errorf("Model=%v, want L1", meta.Model)
	}
	if meta.Firmware == nil || *meta.Firmware != "v1.2.3" {
		t.Errorf("Firmware=%v, want v1.2.3", meta.Firmware)
	}
	if meta.ClientVersion == nil || *meta.ClientVersion != "2.4.1" {
		t.Errorf("ClientVersion=%v, want 2.4.1", meta.ClientVersion)
	}
	if meta.Radio == nil || *meta.Radio != "SX1262" {
		t.Errorf("Radio=%v, want SX1262", meta.Radio)
	}
	if meta.BatteryMv == nil || *meta.BatteryMv != 3500 {
		t.Errorf("BatteryMv=%v, want 3500", meta.BatteryMv)
	}
	if meta.UptimeSecs == nil || *meta.UptimeSecs != 86400 {
		t.Errorf("UptimeSecs=%v, want 86400", meta.UptimeSecs)
	}
	if meta.NoiseFloor == nil || *meta.NoiseFloor != -115.5 {
		t.Errorf("NoiseFloor=%v, want -115.5", meta.NoiseFloor)
	}

	// Battery with fractional part should round
	msg2 := map[string]interface{}{
		"battery_mv": 3500.7,
	}
	meta2 := extractObserverMeta(msg2)
	if meta2 == nil || meta2.BatteryMv == nil || *meta2.BatteryMv != 3501 {
		t.Errorf("battery_mv rounding: got %v, want 3501", meta2)
	}

	// Empty message → nil
	meta3 := extractObserverMeta(map[string]interface{}{})
	if meta3 != nil {
		t.Errorf("expected nil for empty message, got %v", meta3)
	}

	// firmware/client snake_case fields should be captured too
	msg4 := map[string]interface{}{
		"firmware":       "v9.9.9",
		"client_version": "3.0.0",
	}
	meta4 := extractObserverMeta(msg4)
	if meta4 == nil || meta4.Firmware == nil || *meta4.Firmware != "v9.9.9" {
		t.Errorf("Firmware=%v, want v9.9.9", meta4)
	}
	if meta4 == nil || meta4.ClientVersion == nil || *meta4.ClientVersion != "3.0.0" {
		t.Errorf("ClientVersion=%v, want 3.0.0", meta4)
	}

	// When both keys are present, explicit compatibility fields win due extraction order:
	// firmware_version overrides firmware and clientVersion overrides client_version.
	msg5 := map[string]interface{}{
		"firmware":         "v1-legacy",
		"firmware_version": "v2-canonical",
		"client_version":   "1.0.0-legacy",
		"clientVersion":    "2.0.0-canonical",
	}
	meta5 := extractObserverMeta(msg5)
	if meta5 == nil {
		t.Fatal("expected non-nil meta for dual-key payload")
	}
	if meta5.Firmware == nil || *meta5.Firmware != "v2-canonical" {
		t.Errorf("Firmware precedence mismatch: got %v, want v2-canonical from firmware_version", meta5.Firmware)
	}
	if meta5.ClientVersion == nil || *meta5.ClientVersion != "2.0.0-canonical" {
		t.Errorf("ClientVersion precedence mismatch: got %v, want 2.0.0-canonical from clientVersion", meta5.ClientVersion)
	}
}

func TestInsertTransmissionWithObserver(t *testing.T) {
	ctx := t.Context()
	s := newTestStore(t)
	defer s.Close()

	// Insert observer first
	if err := s.UpsertObserver(ctx, "obs1", "Observer1", "SJC", nil); err != nil {
		t.Fatal(err)
	}

	data := &PacketData{
		RawHex:      "0A00D69F",
		Timestamp:   time.Date(2026, 3, 25, 0, 0, 0, 0, time.UTC),
		ObserverID:  "obs1",
		Hash:        "test1234567890ab",
		RouteType:   2,
		PayloadType: 2,
		PathJSON:    "[]",
		DecodedJSON: `{"type":"TXT_MSG"}`,
	}
	if _, err := s.InsertTransmission(ctx, data); err != nil {
		t.Fatal(err)
	}

	// Verify observer_idx was resolved
	var wantIdx int64
	mustScan(t, testRow(t, s, `SELECT id FROM observers WHERE pubkey = $1`, "obs1"), &wantIdx)

	var observerIdx *int64
	mustScan(t, testRow(t, s, `SELECT observer_idx FROM observations ORDER BY id LIMIT 1`), &observerIdx)
	if observerIdx == nil {
		t.Fatal("observer_idx should be set when observer exists")
	}
	if *observerIdx != wantIdx {
		t.Errorf("observer_idx = %d, want %d", *observerIdx, wantIdx)
	}
}

// #463: Verify that inserting a packet updates the observer's last_seen,
// so low-traffic observers don't incorrectly appear offline.
func TestInsertTransmissionUpdatesObserverLastSeen(t *testing.T) {
	ctx := t.Context()
	s := newTestStore(t)
	defer s.Close()

	// Insert observer with an old last_seen
	if err := s.UpsertObserver(ctx, "obs1", "Observer1", "SJC", nil); err != nil {
		t.Fatal(err)
	}
	// Backdate last_seen to 2 hours ago
	oldTime := time.Date(2026, 3, 24, 22, 0, 0, 0, time.UTC)
	testExec(t, s, "UPDATE observers SET last_seen = $1 WHERE id = $2", oldTime, "obs1")

	// Verify it was backdated
	var lastSeenBefore time.Time
	mustScan(t, testRow(t, s, `SELECT last_seen FROM observers WHERE id = $1`, "obs1"), &lastSeenBefore)
	if !lastSeenBefore.Equal(oldTime) {
		t.Fatalf("expected last_seen=%s, got %s", oldTime, lastSeenBefore)
	}

	// Insert a packet from this observer
	data := &PacketData{
		RawHex:      "0A00D69F",
		Timestamp:   time.Date(2026, 3, 25, 1, 0, 0, 0, time.UTC),
		ObserverID:  "obs1",
		Hash:        "lastseentest123456",
		RouteType:   2,
		PayloadType: 2,
		PathJSON:    "[]",
		DecodedJSON: `{"type":"TXT_MSG"}`,
	}
	before := time.Now().Unix()
	if _, err := s.InsertTransmission(ctx, data); err != nil {
		t.Fatal(err)
	}
	after := time.Now().Unix()

	// Verify last_seen was updated to INGEST time, not envelope time (#1465).
	var lastSeenAfter time.Time
	mustScan(t, testRow(t, s, `SELECT last_seen FROM observers WHERE id = $1`, "obs1"), &lastSeenAfter)
	if lastSeenAfter.Equal(oldTime) {
		t.Error("observer last_seen was NOT updated after packet insertion — low-traffic observers will appear offline")
	}
	ls := lastSeenAfter
	if ls.Unix() < before-5 || ls.Unix() > after+5 {
		t.Errorf("expected last_seen ≈ server now (in [%d, %d]), got %s (epoch %d). "+
			"observer.last_seen must use ingest time, not envelope time (#1465).",
			before, after, lastSeenAfter, ls.Unix())
	}
}

func TestLastPacketAtUpdatedOnPacketOnly(t *testing.T) {
	s := newTestStore(t)
	defer s.Close()
	ctx := t.Context()

	// Insert observer via status path — last_packet_at should be NULL
	if err := s.UpsertObserver(ctx, "obs1", "Observer1", "SJC", nil); err != nil {
		t.Fatal(err)
	}

	var lastPacketAt *time.Time
	mustScan(t, testRow(t, s, `SELECT last_packet_at FROM observers WHERE pubkey = $1`, "obs1"), &lastPacketAt)
	if lastPacketAt != nil {
		t.Fatalf("expected last_packet_at to be NULL after UpsertObserver, got %s", lastPacketAt)
	}

	// Insert a packet from this observer — last_packet_at should be set
	data := &PacketData{
		RawHex:      "0A00D69F",
		Timestamp:   time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC),
		ObserverID:  "obs1",
		Hash:        "lastpackettest123456",
		RouteType:   2,
		PayloadType: 2,
		PathJSON:    "[]",
		DecodedJSON: `{"type":"TXT_MSG"}`,
	}
	before := time.Now().Unix()
	if _, err := s.InsertTransmission(ctx, data); err != nil {
		t.Fatal(err)
	}
	after := time.Now().Unix()

	mustScan(t, testRow(t, s, `SELECT last_packet_at FROM observers WHERE id = $1`, "obs1"), &lastPacketAt)
	if lastPacketAt == nil {
		t.Fatal("expected last_packet_at to be non-NULL after InsertTransmission")
	}
	// last_packet_at, like last_seen, is "when did the analyzer last receive a
	// packet from this observer" — an ingest-time question, independent of the
	// envelope timestamp. See #1465.
	lp := *lastPacketAt
	if lp.Unix() < before-5 || lp.Unix() > after+5 {
		t.Errorf("expected last_packet_at ≈ server now (in [%d, %d]), got %s (epoch %d)",
			before, after, lastPacketAt, lp.Unix())
	}

	// UpsertObserver again (status path) — last_packet_at should NOT change
	if err := s.UpsertObserver(ctx, "obs1", "Observer1", "SJC", nil); err != nil {
		t.Fatal(err)
	}

	var lastPacketAtAfterStatus *time.Time
	mustScan(t, testRow(t, s, `SELECT last_packet_at FROM observers WHERE pubkey = $1`, "obs1"), &lastPacketAtAfterStatus)
	if lastPacketAtAfterStatus == nil || !lastPacketAtAfterStatus.Equal(*lastPacketAt) {
		t.Errorf("UpsertObserver should not change last_packet_at; expected %s, got %v", lastPacketAt, lastPacketAtAfterStatus)
	}
}

func TestEndToEndIngest(t *testing.T) {
	ctx := t.Context()
	s := newTestStore(t)
	defer s.Close()

	// Simulate full pipeline: decode + insert
	rawHex := "120046D62DE27D4C5194D7821FC5A34A45565DCC2537B300B9AB6275255CEFB65D840CE5C169C94C9AED39E8BCB6CB6EB0335497A198B33A1A610CD3B03D8DCFC160900E5244280323EE0B44CACAB8F02B5B38B91CFA18BD067B0B5E63E94CFC85F758A8530B9240933402E0E6B8F84D5252322D52"

	decoded, err := decoder.DecodePacket(rawHex, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	msg := &MQTTPacketMessage{
		Raw: rawHex,
	}
	pktData := BuildPacketData(msg, decoded, "obs1", "SJC", nil)
	if _, err := s.InsertTransmission(ctx, pktData); err != nil {
		t.Fatal(err)
	}

	// Process advert node upsert
	if decoded.Payload.Type == "ADVERT" && decoded.Payload.PubKey != "" {
		ok, _ := decoder.ValidateAdvert(&decoded.Payload)
		if ok {
			role := decoder.AdvertRole(decoded.Payload.Flags)
			err := s.UpsertNode(ctx, decoded.Payload.PubKey, decoded.Payload.Name, role, decoded.Payload.Lat, decoded.Payload.Lon, pktData.Timestamp)
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	// Verify node was created
	var nodeName string
	mustScan(t, testRow(t, s, `SELECT name FROM nodes WHERE public_key = $1`, decoded.Payload.PubKey), &nodeName)
	if !strings.Contains(nodeName, "MRR2-R") {
		t.Errorf("node name=%s, want MRR2-R", nodeName)
	}
}

func TestInsertTransmissionEmptyHash(t *testing.T) {
	s := newTestStore(t)
	defer s.Close()
	ctx := t.Context()

	data := &PacketData{
		RawHex:    "0A00",
		Timestamp: time.Date(2026, 3, 25, 0, 0, 0, 0, time.UTC),
		Hash:      "", // empty hash → should return nil
	}
	_, err := s.InsertTransmission(ctx, data)
	if err != nil {
		t.Errorf("empty hash should return nil, got %v", err)
	}

	var count int
	count = countRows(t, s, "SELECT COUNT(*) FROM transmissions")
	if count != 0 {
		t.Errorf("no transmission should be inserted for empty hash, got count=%d", count)
	}
}

func TestInsertTransmissionEmptyTimestamp(t *testing.T) {
	s := newTestStore(t)
	defer s.Close()
	ctx := t.Context()

	data := &PacketData{
		RawHex:    "0A00D69F",
		Timestamp: time.Time{}, // empty → uses current time
		Hash:      "emptyts123456789",
		RouteType: 2,
	}
	_, err := s.InsertTransmission(ctx, data)
	if err != nil {
		t.Fatal(err)
	}

	var firstSeen string
	mustScan(t, testRow(t, s, `SELECT first_seen FROM transmissions WHERE hash = $1`, data.Hash), &firstSeen)
	if firstSeen == "" {
		t.Error("first_seen should be set even with empty timestamp")
	}
}

func TestInsertTransmissionEarlierFirstSeen(t *testing.T) {
	s := newTestStore(t)
	defer s.Close()
	ctx := t.Context()

	// Insert with later timestamp
	data := &PacketData{
		RawHex:    "0A00D69F",
		Timestamp: time.Date(2026, 3, 25, 12, 0, 0, 0, time.UTC),
		Hash:      "firstseen12345678",
		RouteType: 2,
	}
	if _, err := s.InsertTransmission(ctx, data); err != nil {
		t.Fatal(err)
	}

	// Insert again with earlier timestamp
	data2 := &PacketData{
		RawHex:    "0A00D69F",
		Timestamp: time.Date(2026, 3, 25, 6, 0, 0, 0, time.UTC), // earlier
		Hash:      "firstseen12345678",                          // same hash
		RouteType: 2,
	}
	if _, err := s.InsertTransmission(ctx, data2); err != nil {
		t.Fatal(err)
	}

	var firstSeen string
	mustScan(t, testRow(t, s, `SELECT first_seen FROM transmissions WHERE hash = $1`, data.Hash), &firstSeen)
	if firstSeen != "2026-03-25 06:00:00 +0000 UTC" {
		t.Errorf("first_seen=%s, want 2026-03-25 06:00:00 +0000 UTC (earlier timestamp)", firstSeen)
	}
}

func TestInsertTransmissionLaterFirstSeenNotUpdated(t *testing.T) {
	s := newTestStore(t)
	defer s.Close()
	ctx := t.Context()

	data := &PacketData{
		RawHex:    "0A00D69F",
		Timestamp: time.Date(2026, 3, 25, 6, 0, 0, 0, time.UTC),
		Hash:      "notupdated1234567",
		RouteType: 2,
	}
	if _, err := s.InsertTransmission(ctx, data); err != nil {
		t.Fatal(err)
	}

	// Insert with later timestamp — should NOT update first_seen
	data2 := &PacketData{
		RawHex:    "0A00D69F",
		Timestamp: time.Date(2026, 3, 25, 18, 0, 0, 0, time.UTC),
		Hash:      "notupdated1234567",
		RouteType: 2,
	}
	if _, err := s.InsertTransmission(ctx, data2); err != nil {
		t.Fatal(err)
	}

	var firstSeen time.Time
	mustScan(t, testRow(t, s, "SELECT first_seen FROM transmissions WHERE hash = $1", data.Hash), &firstSeen)
	if !firstSeen.Equal(time.Date(2026, 3, 25, 6, 0, 0, 0, time.UTC)) {
		t.Errorf("first_seen=%s should not change to later time", firstSeen)
	}
}

func TestInsertTransmissionNilSNRRSSI(t *testing.T) {
	s := newTestStore(t)
	defer s.Close()
	ctx := t.Context()

	data := &PacketData{
		RawHex:    "0A00D69F",
		Timestamp: time.Date(2026, 3, 25, 0, 0, 0, 0, time.UTC),
		Hash:      "nilsnrrssi1234567",
		RouteType: 2,
		SNR:       nil,
		RSSI:      nil,
	}
	_, err := s.InsertTransmission(ctx, data)
	if err != nil {
		t.Fatal(err)
	}

	var snr, rssi *float64
	mustScan(t, testRow(t, s, "SELECT snr, rssi FROM observations LIMIT 1"), &snr, &rssi)
	if snr != nil {
		t.Errorf("snr should be nil, got %v", snr)
	}
	if rssi != nil {
		t.Errorf("rssi should be nil, got %v", rssi)
	}
}

func TestBuildPacketData(t *testing.T) {
	rawHex := "0A00D69FD7A5A7475DB07337749AE61FA53A4788E976"
	decoded, err := decoder.DecodePacket(rawHex, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	snr := 5.0
	rssi := -100.0
	msg := &MQTTPacketMessage{
		Raw:       rawHex,
		SNR:       &snr,
		RSSI:      &rssi,
		Origin:    "test-observer",
		Timestamp: "2026-05-16T10:00:00Z",
	}

	pkt := BuildPacketData(msg, decoded, "obs123", "SJC", nil)

	if pkt.RawHex != rawHex {
		t.Errorf("rawHex mismatch")
	}
	if pkt.ObserverID != "obs123" {
		t.Errorf("observerID=%s, want obs123", pkt.ObserverID)
	}
	if pkt.ObserverName != "test-observer" {
		t.Errorf("observerName=%s", pkt.ObserverName)
	}
	if pkt.SNR == nil || *pkt.SNR != 5.0 {
		t.Errorf("SNR=%v", pkt.SNR)
	}
	if pkt.RSSI == nil || *pkt.RSSI != -100.0 {
		t.Errorf("RSSI=%v", pkt.RSSI)
	}
	if pkt.Hash == "" {
		t.Error("hash should not be empty")
	}
	if len(pkt.Hash) != 16 {
		t.Errorf("hash length=%d, want 16", len(pkt.Hash))
	}
	if pkt.RouteType != decoded.Header.RouteType {
		t.Errorf("routeType mismatch")
	}
	if pkt.PayloadType != decoded.Header.PayloadType {
		t.Errorf("payloadType mismatch")
	}
	if pkt.Timestamp.IsZero() {
		t.Errorf("timestamp must be populated (server ingest time, #1370 reverts #1233)")
	}
	if pkt.Timestamp == time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC) {
		t.Errorf("timestamp=%s; must NOT be the envelope value (#1370 reverts #1233's "+
			"premise that envelope timestamp is trustworthy — buggy client clocks poison ordering)", pkt.Timestamp)
	}
	if pkt.DecodedJSON == "" || pkt.DecodedJSON == "{}" {
		t.Error("decodedJSON should be populated")
	}
}

func TestBuildPacketDataWithHops(t *testing.T) {
	// A packet with actual hops in the path
	raw := "0505AABBCCDDEE" + strings.Repeat("00", 10)
	decoded, err := decoder.DecodePacket(raw, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	msg := &MQTTPacketMessage{Raw: raw}
	pkt := BuildPacketData(msg, decoded, "", "", nil)

	if pkt.PathJSON == "[]" {
		t.Error("pathJSON should contain hops")
	}
	if !strings.Contains(pkt.PathJSON, "AA") {
		t.Errorf("pathJSON should contain hop AA: %s", pkt.PathJSON)
	}
}

func TestBuildPacketDataNilSNRRSSI(t *testing.T) {
	decoded, _ := decoder.DecodePacket("0A00"+strings.Repeat("00", 10), nil, false)
	msg := &MQTTPacketMessage{Raw: "0A00" + strings.Repeat("00", 10)}
	pkt := BuildPacketData(msg, decoded, "", "", nil)

	if pkt.SNR != nil {
		t.Errorf("SNR should be nil")
	}
	if pkt.RSSI != nil {
		t.Errorf("RSSI should be nil")
	}
}

func TestUpsertNodeEmptyLastSeen(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()

	lat := 37.0
	lon := -122.0
	// Empty lastSeen → should use current time
	if err := s.UpsertNode(ctx, "aabbccdd", "TestNode", "repeater", &lat, &lon, time.Time{}); err != nil {
		t.Fatal(err)
	}

	var lastSeen string
	mustScan(t, testRow(t, s, "SELECT last_seen FROM nodes WHERE public_key = 'aabbccdd'"), &lastSeen)
	if lastSeen == "" {
		t.Error("last_seen should be set even with empty input")
	}
}

func TestRestartAgainstMigratedDatabase(t *testing.T) {
	// A restart runs Migrate and opens the store again on a database that
	// already has its schema. Migrate must be a no-op the second time.
	connStr := newTestDatabase(t) // cloned from the migrated template
	ctx := testCtx(t)

	s1, err := OpenStore(ctx, connStr)
	if err != nil {
		t.Fatal(err)
	}
	applied := countRows(t, s1, "goose_db_version")
	s1.Close()

	// Second boot.
	if err := database.Migrate(ctx, connStr); err != nil {
		t.Fatalf("second Migrate on an already-migrated database: %v", err)
	}
	s2, err := OpenStore(ctx, connStr)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	if got := countRows(t, s2, "goose_db_version"); got != applied {
		t.Errorf("goose_db_version rows = %d, was %d: the second Migrate applied or recorded something", got, applied)
	}
	if n := countRows(t, s2, "observations"); n != 0 {
		t.Errorf("expected 0 observations, got %d", n)
	}
}

func TestInsertTransmissionDedupObservation(t *testing.T) {
	ctx := t.Context()
	s := newTestStore(t)
	defer s.Close()

	// First insert
	data := &PacketData{
		RawHex:    "0A00D69F",
		Timestamp: time.Date(2026, 3, 25, 0, 0, 0, 0, time.UTC),
		Hash:      "dedupobs12345678",
		RouteType: 2,
		PathJSON:  "[]",
	}
	if _, err := s.InsertTransmission(ctx, data); err != nil {
		t.Fatal(err)
	}

	// Insert same hash again with same observer (no observerID) —
	// the UNIQUE constraint on observations dedup should handle it
	if _, err := s.InsertTransmission(ctx, data); err != nil {
		t.Fatal(err)
	}

	var count int
	count = countRows(t, s, "observations")
	// Should have 2 observations (no observer_idx means both have NULL)
	// Actually INSERT OR IGNORE — may be 1 due to dedup index
	if count < 1 {
		t.Errorf("should have at least 1 observation, got %d", count)
	}
}

func TestConcurrentWrites(t *testing.T) {
	s := newTestStore(t)
	// Generous: 1000 packets, each with an observation plus a node and an observer upsert.
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	// Pre-create an observer for observer_idx resolution.
	if err := s.UpsertObserver(ctx, "obs1", "Observer1", "SJC", nil); err != nil {
		t.Fatal(err)
	}

	const goroutines = 20
	const writesPerGoroutine = 50

	errCh := make(chan error, goroutines*writesPerGoroutine)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gIdx int) {
			defer wg.Done()
			for i := 0; i < writesPerGoroutine; i++ {
				snr, rssi := 5.0, -100.0
				data := &PacketData{
					RawHex:      "0A00D69F",
					Timestamp:   time.Now().UTC(),
					ObserverID:  "obs1",
					Hash:        fmt.Sprintf("concurrent_%02d_%03d", gIdx, i),
					RouteType:   types.RouteDirect,
					PayloadType: types.PayloadADVERT,
					PathJSON:    "[]",
					DecodedJSON: `{"type":"ADVERT"}`,
					SNR:         &snr,
					RSSI:        &rssi,
				}
				if _, err := s.InsertTransmission(ctx, data); err != nil {
					errCh <- fmt.Errorf("goroutine %d write %d: %w", gIdx, i, err)
					return
				}
				// Also do node + observer upserts to simulate the full pipeline.
				lat, lon := 37.0, -122.0
				if err := s.UpsertNode(ctx, fmt.Sprintf("node_%02d_%03d", gIdx, i), "Node", "repeater", &lat, &lon, data.Timestamp); err != nil {
					errCh <- fmt.Errorf("goroutine %d node upsert %d: %w", gIdx, i, err)
					return
				}
				if err := s.UpsertObserver(ctx, fmt.Sprintf("obs_%02d_%03d", gIdx, i), "Obs", "SJC", nil); err != nil {
					errCh <- fmt.Errorf("goroutine %d observer upsert %d: %w", gIdx, i, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		t.Errorf("got %d errors from %d concurrent writers (first: %v)", len(errs), goroutines, errs[0])
	}

	// Verify data integrity. The old test counted nodes and observers too but never asserted on them.
	expected := goroutines * writesPerGoroutine
	for table, want := range map[string]int{
		"transmissions": expected,
		"observations":  expected,
		"nodes":         expected,
		"observers":     expected + 1, // plus obs1
	} {
		if got := countRows(t, s, table); got != want {
			t.Errorf("%s count=%d, want %d", table, got, want)
		}
	}

	t.Logf("Concurrent write test: %d goroutines × %d writes = %d total, %d errors",
		goroutines, writesPerGoroutine, expected, len(errs))
	t.Logf("Stats: tx_inserted=%d tx_dupes=%d obs_inserted=%d write_errors=%d",
		s.Stats.TransmissionsInserted.Load(),
		s.Stats.DuplicateTransmissions.Load(),
		s.Stats.ObservationsInserted.Load(),
		s.Stats.WriteErrors.Load(),
	)
}

func TestDBStats(t *testing.T) {
	s := newTestStore(t)
	ctx := testCtx(t)

	// Initial stats should be zero. They belong to this store, so no reset is needed.
	if s.Stats.TransmissionsInserted.Load() != 0 {
		t.Error("initial TransmissionsInserted should be 0")
	}
	if s.Stats.WriteErrors.Load() != 0 {
		t.Error("initial WriteErrors should be 0")
	}

	// Insert a transmission
	rxTime := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	data := &PacketData{
		RawHex:    "0A00D69F",
		Timestamp: rxTime,
		Hash:      "stats_test_12345",
		RouteType: types.RouteDirect,
		PathJSON:  "[]",
	}
	if _, err := s.InsertTransmission(ctx, data); err != nil {
		t.Fatal(err)
	}
	if s.Stats.TransmissionsInserted.Load() != 1 {
		t.Errorf("TransmissionsInserted=%d, want 1", s.Stats.TransmissionsInserted.Load())
	}
	if s.Stats.ObservationsInserted.Load() != 1 {
		t.Errorf("ObservationsInserted=%d, want 1", s.Stats.ObservationsInserted.Load())
	}

	// Insert duplicate. Counted exactly once: a double increment of this
	// counter slipped into InsertTransmission before, and this is the assertion
	// that catches it.
	if _, err := s.InsertTransmission(ctx, data); err != nil {
		t.Fatal(err)
	}
	if s.Stats.DuplicateTransmissions.Load() != 1 {
		t.Errorf("DuplicateTransmissions=%d, want 1", s.Stats.DuplicateTransmissions.Load())
	}

	// Node upsert
	lat, lon := 37.0, -122.0
	if err := s.UpsertNode(ctx, "pk1", "Node1", "repeater", &lat, &lon, rxTime); err != nil {
		t.Fatal(err)
	}
	if s.Stats.NodeUpserts.Load() != 1 {
		t.Errorf("NodeUpserts=%d, want 1", s.Stats.NodeUpserts.Load())
	}

	// Observer upsert
	if err := s.UpsertObserver(ctx, "obs1", "Obs1", "SJC", nil); err != nil {
		t.Fatal(err)
	}
	if s.Stats.ObserverUpserts.Load() != 1 {
		t.Errorf("ObserverUpserts=%d, want 1", s.Stats.ObserverUpserts.Load())
	}

	// None of the above should have failed a write. The old test only checked
	// that the counter started at zero.
	if got := s.Stats.WriteErrors.Load(); got != 0 {
		t.Errorf("WriteErrors=%d after a clean run, want 0", got)
	}

	// LogStats should not panic
	s.LogStats()
}

func TestLoadTestThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("load test; skipped in -short")
	}
	s := newTestStore(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	// Pre-create observer
	if err := s.UpsertObserver(ctx, "obs1", "Observer1", "SJC", nil); err != nil {
		t.Fatal(err)
	}

	const totalMessages = 1000
	const goroutines = 20
	const perGoroutine = totalMessages / goroutines

	// Simulate the full pipeline: InsertTransmission + UpsertNode + IncrementAdvertCount + UpsertObserver.
	// This matches the real handleMessage write pattern for ADVERT packets.
	latencies := make([]time.Duration, totalMessages)
	var mu sync.Mutex
	var failures int
	var firstErr error
	fail := func(what string, err error) {
		mu.Lock()
		defer mu.Unlock()
		failures++
		if firstErr == nil {
			firstErr = fmt.Errorf("%s: %w", what, err)
		}
	}

	var wg sync.WaitGroup
	start := time.Now()
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gIdx int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				msgStart := time.Now()
				id := fmt.Sprintf("%04d_%04d", gIdx, i)
				snr, rssi := 5.0, -100.0
				rxTime := time.Now().UTC()

				data := &PacketData{
					RawHex:      "0A00D69F",
					Timestamp:   rxTime,
					ObserverID:  "obs1",
					Hash:        "load_" + id,
					RouteType:   types.RouteDirect,
					PayloadType: types.PayloadADVERT,
					PathJSON:    "[]",
					DecodedJSON: `{"type":"ADVERT","pubKey":"load_` + id + `"}`,
					SNR:         &snr,
					RSSI:        &rssi,
				}
				if _, err := s.InsertTransmission(ctx, data); err != nil {
					fail("insert transmission", err)
					continue
				}

				lat := 37.0 + float64(gIdx)*0.001
				lon := -122.0 + float64(i)*0.001
				pubKey := "node_" + id
				if err := s.UpsertNode(ctx, pubKey, "Node", "repeater", &lat, &lon, rxTime); err != nil {
					fail("upsert node", err)
				}
				if err := s.IncrementAdvertCount(ctx, pubKey); err != nil {
					fail("increment advert count", err)
				}
				if err := s.UpsertObserver(ctx, "obs_"+id, "Obs", "SJC", nil); err != nil {
					fail("upsert observer", err)
				}
				latencies[gIdx*perGoroutine+i] = time.Since(msgStart)
			}
		}(g)
	}
	wg.Wait()
	elapsed := time.Since(start)

	valid := make([]time.Duration, 0, totalMessages)
	for _, l := range latencies {
		if l > 0 {
			valid = append(valid, l)
		}
	}
	if len(valid) == 0 {
		t.Fatalf("no message completed; first error: %v", firstErr)
	}
	slices.Sort(valid)
	pct := func(p int) time.Duration { return valid[len(valid)*p/100] }
	msgsPerSec := float64(totalMessages) / elapsed.Seconds()

	t.Logf("=== LOAD TEST RESULTS ===")
	t.Logf("Messages:     %d (%d goroutines × %d each)", totalMessages, goroutines, perGoroutine)
	t.Logf("Writes/msg:   4 (InsertTx + UpsertNode + IncrAdvertCount + UpsertObserver)")
	t.Logf("Pool:         max %d connections", s.db.Pool().Config().MaxConns)
	t.Logf("Duration:     %s", elapsed.Round(time.Millisecond))
	t.Logf("Throughput:   %.1f msgs/sec (%.1f writes/sec)", msgsPerSec, msgsPerSec*4)
	t.Logf("Latency p50:  %s", pct(50).Round(time.Microsecond))
	t.Logf("Latency p95:  %s", pct(95).Round(time.Microsecond))
	t.Logf("Latency p99:  %s", pct(99).Round(time.Microsecond))
	t.Logf("Total errors: %d", failures)
	t.Logf("Stats: tx=%d dupes=%d obs=%d nodes=%d observers=%d write_err=%d",
		s.Stats.TransmissionsInserted.Load(),
		s.Stats.DuplicateTransmissions.Load(),
		s.Stats.ObservationsInserted.Load(),
		s.Stats.NodeUpserts.Load(),
		s.Stats.ObserverUpserts.Load(),
		s.Stats.WriteErrors.Load(),
	)

	// Hard assertions. Throughput and latency are logged, not asserted: they depend
	// on the machine and would make the test flaky.
	if failures > 0 {
		t.Errorf("%d write errors (expected 0), first: %v", failures, firstErr)
	}
	if got := s.Stats.WriteErrors.Load(); got != 0 {
		t.Errorf("WriteErrors=%d, want 0", got)
	}
	for table, want := range map[string]int{
		"transmissions": totalMessages,
		"observations":  totalMessages,
		"nodes":         totalMessages,
		"observers":     totalMessages + 1, // plus obs1
	} {
		if got := countRows(t, s, table); got != want {
			t.Errorf("%s=%d, want %d", table, got, want)
		}
	}
}

func TestUpdateNodeTelemetry(t *testing.T) {
	s := newTestStore(t)
	defer s.Close()
	ctx := t.Context()

	lat := 37.0
	lon := -122.0
	if err := s.UpsertNode(ctx, "telem1", "TelemetryNode", "sensor", &lat, &lon, time.Date(2026, 3, 25, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}

	battery := 3700
	temp := 28.5
	if err := s.UpdateNodeTelemetry(ctx, "telem1", &battery, &temp); err != nil {
		t.Fatal(err)
	}

	var bv int
	var tc float64
	mustScan(t, testRow(t, s, "SELECT battery_mv, temperature_c FROM nodes WHERE public_key = 'telem1'"), &bv, &tc)
	if bv != 3700 {
		t.Errorf("battery_mv=%d, want 3700", bv)
	}
	if tc != 28.5 {
		t.Errorf("temperature_c=%f, want 28.5", tc)
	}

	newTemp := -5.0
	if err := s.UpdateNodeTelemetry(ctx, "telem1", nil, &newTemp); err != nil {
		t.Fatal(err)
	}
	mustScan(t, testRow(t, s, "SELECT battery_mv, temperature_c FROM nodes WHERE public_key = 'telem1'"), &bv, &tc)
	if bv != 3700 {
		t.Errorf("battery_mv after nil update=%d, want 3700 (preserved)", bv)
	}
	if tc != -5.0 {
		t.Errorf("temperature_c after update=%f, want -5.0", tc)
	}
}

// --- Bug #320: Observer metadata nested stats ---

func TestExtractObserverMetaNestedStats(t *testing.T) {
	// Real-world MQTT status payload: stats fields nested under "stats"
	msg := map[string]interface{}{
		"status":           "online",
		"origin":           "ObserverName",
		"model":            "Heltec V3",
		"firmware_version": "v1.14.0-9f1a3ea",
		"stats": map[string]interface{}{
			"battery_mv":  4174.0,
			"uptime_secs": 80277.0,
			"noise_floor": -110.0,
		},
	}
	meta := extractObserverMeta(msg)
	if meta == nil {
		t.Fatal("expected non-nil meta")
	}
	if meta.Model == nil || *meta.Model != "Heltec V3" {
		t.Errorf("Model=%v, want Heltec V3", meta.Model)
	}
	if meta.Firmware == nil || *meta.Firmware != "v1.14.0-9f1a3ea" {
		t.Errorf("Firmware=%v, want v1.14.0-9f1a3ea", meta.Firmware)
	}
	if meta.BatteryMv == nil || *meta.BatteryMv != 4174 {
		t.Errorf("BatteryMv=%v, want 4174", meta.BatteryMv)
	}
	if meta.UptimeSecs == nil || *meta.UptimeSecs != 80277 {
		t.Errorf("UptimeSecs=%v, want 80277", meta.UptimeSecs)
	}
	if meta.NoiseFloor == nil || *meta.NoiseFloor != -110.0 {
		t.Errorf("NoiseFloor=%v, want -110", meta.NoiseFloor)
	}
}

func TestExtractObserverMetaNestedStatsPrecedence(t *testing.T) {
	// If stats has a value AND top-level has a value, nested wins
	msg := map[string]interface{}{
		"battery_mv":  9999.0, // top-level (stale/wrong)
		"noise_floor": -120.0, // top-level (stale/wrong)
		"stats": map[string]interface{}{
			"battery_mv":  4174.0, // nested (correct)
			"noise_floor": -110.5, // nested (correct)
		},
	}
	meta := extractObserverMeta(msg)
	if meta == nil {
		t.Fatal("expected non-nil meta")
	}
	if meta.BatteryMv == nil || *meta.BatteryMv != 4174 {
		t.Errorf("BatteryMv=%v, want 4174 (nested should win over top-level)", meta.BatteryMv)
	}
	if meta.NoiseFloor == nil || *meta.NoiseFloor != -110.5 {
		t.Errorf("NoiseFloor=%v, want -110.5 (nested should win over top-level)", meta.NoiseFloor)
	}
}

func TestExtractObserverMetaFlatFallback(t *testing.T) {
	// Backward compatibility: flat structure (no stats object) still works
	msg := map[string]interface{}{
		"battery_mv":  3500.0,
		"uptime_secs": 86400.0,
		"noise_floor": -115.5,
	}
	meta := extractObserverMeta(msg)
	if meta == nil {
		t.Fatal("expected non-nil meta for flat structure")
	}
	if meta.BatteryMv == nil || *meta.BatteryMv != 3500 {
		t.Errorf("BatteryMv=%v, want 3500", meta.BatteryMv)
	}
	if meta.UptimeSecs == nil || *meta.UptimeSecs != 86400 {
		t.Errorf("UptimeSecs=%v, want 86400", meta.UptimeSecs)
	}
	if meta.NoiseFloor == nil || *meta.NoiseFloor != -115.5 {
		t.Errorf("NoiseFloor=%v, want -115.5", meta.NoiseFloor)
	}
}

func TestExtractObserverMetaEmptyStats(t *testing.T) {
	// Empty stats object should not crash, top-level fallback still applies
	msg := map[string]interface{}{
		"model": "T-Beam",
		"stats": map[string]interface{}{},
	}
	meta := extractObserverMeta(msg)
	if meta == nil {
		t.Fatal("expected non-nil meta (model is present)")
	}
	if meta.Model == nil || *meta.Model != "T-Beam" {
		t.Errorf("Model=%v, want T-Beam", meta.Model)
	}
	if meta.BatteryMv != nil {
		t.Errorf("BatteryMv should be nil, got %v", *meta.BatteryMv)
	}
}

func TestExtractObserverMetaStatsNotAMap(t *testing.T) {
	// stats field is not a map (e.g., string) — should not crash, fall back to top-level
	msg := map[string]interface{}{
		"stats":      "invalid",
		"battery_mv": 3700.0,
	}
	meta := extractObserverMeta(msg)
	if meta == nil {
		t.Fatal("expected non-nil meta")
	}
	if meta.BatteryMv == nil || *meta.BatteryMv != 3700 {
		t.Errorf("BatteryMv=%v, want 3700 (top-level fallback when stats is not a map)", meta.BatteryMv)
	}
}

func TestExtractObserverMetaNoiseFloorFloat(t *testing.T) {
	// noise_floor migrated to REAL — verify float precision preserved
	msg := map[string]interface{}{
		"stats": map[string]interface{}{
			"noise_floor": -108.75,
		},
	}
	meta := extractObserverMeta(msg)
	if meta == nil {
		t.Fatal("expected non-nil meta")
	}
	if meta.NoiseFloor == nil || *meta.NoiseFloor != -108.75 {
		t.Errorf("NoiseFloor=%v, want -108.75", meta.NoiseFloor)
	}
}

func TestExtractObserverMetaNestedNilSkipsTopLevel(t *testing.T) {
	// JSON {"stats": {"battery_mv": null}} decodes to nil value in the map.
	// Nested nil should suppress top-level fallback (nested wins semantics).
	msg := map[string]interface{}{
		"battery_mv": 3700.0,
		"stats": map[string]interface{}{
			"battery_mv": nil,
		},
	}
	meta := extractObserverMeta(msg)
	if meta != nil && meta.BatteryMv != nil {
		t.Error("nested nil should suppress top-level fallback")
	}
}

// TestRequiredIndexesExist pins the indexes that queries and prunes depend on.
// A missing one doesn't break a build or a unit test, it just makes a hot query
// scan the table, so existence is worth asserting directly.
func TestRequiredIndexesExist(t *testing.T) {
	s := newTestStore(t)
	for _, c := range []struct{ table, index string }{
		{"observations", "idx_observations_timestamp"}, // the neighbor builder's watermark scan
		{"observations", "idx_observations_dedup"},
		{"transmissions", "idx_transmissions_first_seen"}, // the packet prune's batch select
		{"transmissions", "idx_tx_channel_hash"},          // the channel list
		{"client_receptions", "idx_client_recept_rxat"},
		{"client_rx_observations", "idx_cro_prune"},
		{"client_rf_samples", "idx_crf_sampled_at"},
		{"node_declared_regions", "idx_ndr_prune"},
		{"dropped_packets", "idx_dropped_dropped_at"},
	} {
		t.Run(c.index, func(t *testing.T) {
			n := countWhere(t, s, "pg_indexes",
				"schemaname = 'public' AND tablename = $1 AND indexname = $2", c.table, c.index)
			if n != 1 {
				t.Errorf("index %s on %s is missing", c.index, c.table)
			}
		})
	}
}

func TestBuildPacketDataScoreAndDirection(t *testing.T) {
	rawHex := "0A00D69FD7A5A7475DB07337749AE61FA53A4788E976"
	decoded, err := decoder.DecodePacket(rawHex, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	score := 42.0
	dir := "incoming"
	msg := &MQTTPacketMessage{
		Raw:       rawHex,
		Score:     &score,
		Direction: &dir,
	}

	pkt := BuildPacketData(msg, decoded, "obs1", "SJC", nil)
	if pkt.Score == nil || *pkt.Score != 42.0 {
		t.Errorf("Score=%v, want 42.0", pkt.Score)
	}
	if pkt.Direction == nil || *pkt.Direction != "incoming" {
		t.Errorf("Direction=%v, want incoming", pkt.Direction)
	}
}

func TestBuildPacketDataNilScoreDirection(t *testing.T) {
	decoded, _ := decoder.DecodePacket("0A00"+strings.Repeat("00", 10), nil, false)
	msg := &MQTTPacketMessage{Raw: "0A00" + strings.Repeat("00", 10)}
	pkt := BuildPacketData(msg, decoded, "", "", nil)

	if pkt.Score != nil {
		t.Errorf("Score should be nil, got %v", *pkt.Score)
	}
	if pkt.Direction != nil {
		t.Errorf("Direction should be nil, got %v", *pkt.Direction)
	}
}

func TestInsertTransmissionWithScoreAndDirection(t *testing.T) {
	s := newTestStore(t)
	defer s.Close()
	ctx := t.Context()

	score := 7.5
	dir := "outgoing"
	data := &PacketData{
		RawHex:    "AABB",
		Timestamp: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		SNR:       ptrFloat(5.0),
		RSSI:      ptrFloat(-90.0),
		Score:     &score,
		Direction: &dir,
		Hash:      "abc123",
		PathJSON:  "[]",
	}

	isNew, err := s.InsertTransmission(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	if !isNew {
		t.Error("expected new transmission")
	}

	// Verify the observation was stored with score and direction
	var gotDir sql.NullString
	var gotScore sql.NullFloat64
	mustScan(t, testRow(t, s, "SELECT direction, score FROM observations LIMIT 1"), &gotDir, &gotScore)
	if !gotDir.Valid || gotDir.String != "outgoing" {
		t.Errorf("direction=%v, want outgoing", gotDir)
	}
	if !gotScore.Valid || gotScore.Float64 != 7.5 {
		t.Errorf("score=%v, want 7.5", gotScore)
	}
}

func ptrFloat(f float64) *float64 { return &f }
func ptrInt(i int) *int           { return &i }

func TestRoundToInterval(t *testing.T) {
	tests := []struct {
		input    time.Time
		interval int
		want     time.Time
	}{
		{time.Date(2026, 4, 5, 10, 2, 0, 0, time.UTC), 300, time.Date(2026, 4, 5, 10, 0, 0, 0, time.UTC)},
		{time.Date(2026, 4, 5, 10, 3, 0, 0, time.UTC), 300, time.Date(2026, 4, 5, 10, 5, 0, 0, time.UTC)},
		{time.Date(2026, 4, 5, 10, 2, 30, 0, time.UTC), 300, time.Date(2026, 4, 5, 10, 5, 0, 0, time.UTC)},
		{time.Date(2026, 4, 5, 10, 5, 0, 0, time.UTC), 300, time.Date(2026, 4, 5, 10, 5, 0, 0, time.UTC)},
		{time.Date(2026, 4, 5, 10, 7, 29, 0, time.UTC), 300, time.Date(2026, 4, 5, 10, 5, 0, 0, time.UTC)},
	}
	for _, tc := range tests {
		got := RoundToInterval(tc.input, tc.interval)
		if !got.Equal(tc.want) {
			t.Errorf("RoundToInterval(%v, %d) = %v, want %v", tc.input, tc.interval, got, tc.want)
		}
	}
}

func TestInsertMetrics(t *testing.T) {
	s := newTestStore(t)
	defer s.Close()
	ctx := t.Context()

	nf := -112.5
	txAir := 100
	rxAir := 500
	recvErr := 3
	batt := 3720
	data := &MetricsData{
		ObserverID: "obs1",
		NoiseFloor: &nf,
		TxAirSecs:  &txAir,
		RxAirSecs:  &rxAir,
		RecvErrors: &recvErr,
		BatteryMv:  &batt,
	}

	if err := s.InsertMetrics(ctx, data); err != nil {
		t.Fatalf("InsertMetrics: %v", err)
	}

	// Verify insertion
	var count int
	count = countWhere(t, s, "observer_metrics", "observer_id = 'obs1'")
	if count != 1 {
		t.Errorf("expected 1 row, got %d", count)
	}

	// Verify values
	var gotNF float64
	var gotTx, gotRx, gotErr, gotBatt int
	mustScan(t, testRow(t, s, "SELECT noise_floor, tx_air_secs, rx_air_secs, recv_errors, battery_mv FROM observer_metrics WHERE observer_id = 'obs1'"), &gotNF, &gotTx, &gotRx, &gotErr, &gotBatt)
	if gotNF != -112.5 {
		t.Errorf("noise_floor = %v, want -112.5", gotNF)
	}
	if gotTx != 100 {
		t.Errorf("tx_air_secs = %d, want 100", gotTx)
	}
}

func TestInsertMetricsIdempotent(t *testing.T) {
	store := newTestStore(t)
	defer store.Close()
	ctx := t.Context()

	nf := -110.0
	data := &MetricsData{ObserverID: "obs1", NoiseFloor: &nf}

	// Insert twice — should result in 1 row (INSERT OR REPLACE)
	store.InsertMetrics(ctx, data)
	nf2 := -108.0
	data.NoiseFloor = &nf2
	store.InsertMetrics(ctx, data)

	var count int
	count = countWhere(t, store, "observer_metrics", "observer_id = 'obs1'")
	if count != 1 {
		t.Errorf("expected 1 row (idempotent), got %d", count)
	}

	// Verify the value was replaced
	var gotNF float64
	mustScan(t, testRow(t, store, "SELECT noise_floor FROM observer_metrics WHERE observer_id = 'obs1'"), &gotNF)
	if gotNF != -108.0 {
		t.Errorf("noise_floor = %v, want -108.0 (replaced)", gotNF)
	}
}

func TestInsertMetricsNullFields(t *testing.T) {
	store := newTestStore(t)
	defer store.Close()
	ctx := t.Context()

	nf := -115.0
	data := &MetricsData{
		ObserverID: "obs1",
		NoiseFloor: &nf,
		// All other fields nil
	}

	if err := store.InsertMetrics(ctx, data); err != nil {
		t.Fatalf("InsertMetrics with nulls: %v", err)
	}

	var gotNF sql.NullFloat64
	var gotTx sql.NullInt64
	mustScan(t, testRow(t, store, "SELECT noise_floor, tx_air_secs FROM observer_metrics WHERE observer_id = 'obs1'"), &gotNF, &gotTx)
	if !gotNF.Valid || gotNF.Float64 != -115.0 {
		t.Errorf("noise_floor = %v, want -115.0", gotNF)
	}
	if gotTx.Valid {
		t.Errorf("tx_air_secs should be NULL, got %v", gotTx.Int64)
	}
}

func TestPruneOldMetrics(t *testing.T) {
	store := newTestStore(t)
	defer store.Close()
	ctx := t.Context()

	// Insert old and new metrics directly
	oldTs := time.Now().UTC().AddDate(0, 0, -40)
	newTs := time.Now().UTC()
	testExec(t, store, "INSERT INTO observer_metrics (observer_id, timestamp, noise_floor) VALUES (?, ?, ?)", "obs1", oldTs, -110.0)
	testExec(t, store, "INSERT INTO observer_metrics (observer_id, timestamp, noise_floor) VALUES (?, ?, ?)", "obs1", newTs, -112.0)

	n, err := store.PruneOldMetrics(ctx, 30)
	if err != nil {
		t.Fatalf("PruneOldMetrics: %v", err)
	}
	if n != 1 {
		t.Errorf("pruned %d rows, want 1", n)
	}

	var count int
	count = countRows(t, store, "observer_metrics")
	if count != 1 {
		t.Errorf("expected 1 row remaining, got %d", count)
	}
}

func TestExtractObserverMetaNewFields(t *testing.T) {
	msg := map[string]interface{}{
		"model": "L1",
		"stats": map[string]interface{}{
			"noise_floor": -112.5,
			"battery_mv":  3720.0,
			"uptime_secs": 86400.0,
			"tx_air_secs": 100.0,
			"rx_air_secs": 500.0,
			"recv_errors": 3.0,
		},
	}
	meta := extractObserverMeta(msg)
	if meta == nil {
		t.Fatal("expected non-nil meta")
	}
	if meta.TxAirSecs == nil || *meta.TxAirSecs != 100 {
		t.Errorf("TxAirSecs = %v, want 100", meta.TxAirSecs)
	}
	if meta.RxAirSecs == nil || *meta.RxAirSecs != 500 {
		t.Errorf("RxAirSecs = %v, want 500", meta.RxAirSecs)
	}
	if meta.RecvErrors == nil || *meta.RecvErrors != 3 {
		t.Errorf("RecvErrors = %v, want 3", meta.RecvErrors)
	}
}

// TestInsertObservationSNRFillIn verifies that when the same observation is
// received twice — first without SNR, then with SNR — the SNR is filled in
// rather than silently discarded. The unique dedup index is
// (transmission_id, observer_idx, COALESCE(path_json, ”)); observer_idx must
// be non-NULL for the conflict to fire (SQLite treats NULL != NULL).
//
// (Blank line above is deliberate: gofmt's Go-1.19+ doc-comment formatter
// rewrites a doubled straight-quote pair like '' into a single curly “ once
// this comment is attached as a doc comment to the func below — detaching it
// keeps the literal '' matching the real index expression in the
// idx_observations_dedup CREATE UNIQUE INDEX statements elsewhere in this file.)

func TestInsertObservationSNRFillIn(t *testing.T) {
	s := newTestStore(t)
	defer s.Close()
	ctx := t.Context()

	// Register the observer so observer_idx is non-NULL (required for dedup).
	if err := s.UpsertObserver(ctx, "pymc-obs1", "PyMC Observer", "SJC", nil); err != nil {
		t.Fatal(err)
	}

	// First arrival: same observer, no SNR/RSSI (e.g. broker replay without RF fields).
	data1 := &PacketData{
		RawHex:     "0A00D69FD7A5A7475DB07337749AE61FA53A4788E976",
		Timestamp:  time.Date(2026, 4, 20, 0, 0, 0, 0, time.UTC),
		Hash:       "snrfillin0001hash",
		RouteType:  1,
		ObserverID: "pymc-obs1",
		SNR:        nil,
		RSSI:       nil,
	}
	if _, err := s.InsertTransmission(ctx, data1); err != nil {
		t.Fatal(err)
	}

	var snr1, rssi1 *float64

	testRow(t, s, "SELECT snr, rssi FROM observations LIMIT 1").Scan(&snr1, &rssi1)
	if snr1 != nil || rssi1 != nil {
		t.Fatalf("precondition: first insert should have nil SNR/RSSI, got snr=%v rssi=%v", snr1, rssi1)
	}

	// Second arrival: same packet, same observer, now WITH SNR/RSSI.
	snr := 10.5
	rssi := -88.0
	data2 := &PacketData{
		RawHex:     data1.RawHex,
		Timestamp:  data1.Timestamp,
		Hash:       data1.Hash,
		RouteType:  data1.RouteType,
		ObserverID: "pymc-obs1",
		SNR:        &snr,
		RSSI:       &rssi,
	}
	if _, err := s.InsertTransmission(ctx, data2); err != nil {
		t.Fatal(err)
	}

	var snr2, rssi2 *float64
	testRow(t, s, "SELECT snr, rssi FROM observations LIMIT 1").Scan(&snr2, &rssi2)
	if snr2 == nil || *snr2 != snr {
		t.Errorf("SNR not filled in by second arrival: got %v, want %v", snr2, snr)
	}
	if rssi2 == nil || *rssi2 != rssi {
		t.Errorf("RSSI not filled in by second arrival: got %v, want %v", rssi2, rssi)
	}

	// Third arrival: same packet again, SNR absent — must NOT overwrite existing SNR.
	data3 := &PacketData{
		RawHex:     data1.RawHex,
		Timestamp:  data1.Timestamp,
		Hash:       data1.Hash,
		RouteType:  data1.RouteType,
		ObserverID: "pymc-obs1",
		SNR:        nil,
		RSSI:       nil,
	}
	if _, err := s.InsertTransmission(ctx, data3); err != nil {
		t.Fatal(err)
	}

	var snr3, rssi3 *float64
	testRow(t, s, "SELECT snr, rssi FROM observations LIMIT 1").Scan(&snr3, &rssi3)
	if snr3 == nil || *snr3 != snr {
		t.Errorf("SNR overwritten by null arrival: got %v, want %v", snr3, snr)
	}
	if rssi3 == nil || *rssi3 != rssi {
		t.Errorf("RSSI overwritten by null arrival: got %v, want %v", rssi3, rssi)
	}
}

// TestPerObservationRawHex verifies that two MQTT packets for the same hash
// from different observers store distinct raw_hex per observation (#881).
func TestPerObservationRawHex(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()

	// Register two observers
	s.UpsertObserver(ctx, "obs-A", "Observer A", "", nil)
	s.UpsertObserver(ctx, "obs-B", "Observer B", "", nil)

	hash := "abc123def456"
	rawA := "c0ffee01"
	rawB := "c0ffee0201aa"
	dir := "RX"

	// First observation from observer A
	pdA := &PacketData{
		RawHex:     rawA,
		Hash:       hash,
		Timestamp:  time.Date(2026, 4, 21, 10, 0, 0, 0, time.UTC),
		ObserverID: "obs-A",
		Direction:  &dir,
		PathJSON:   "[]",
	}
	isNew, err := s.InsertTransmission(ctx, pdA)
	if err != nil {
		t.Fatalf("insert A: %v", err)
	}
	if !isNew {
		t.Fatal("expected new transmission")
	}

	// Second observation from observer B (same hash, different raw bytes)
	pdB := &PacketData{
		RawHex:     rawB,
		Hash:       hash,
		Timestamp:  time.Date(2026, 4, 21, 10, 0, 1, 0, time.UTC),
		ObserverID: "obs-B",
		Direction:  &dir,
		PathJSON:   `["aabb"]`,
	}
	isNew2, err := s.InsertTransmission(ctx, pdB)
	if err != nil {
		t.Fatalf("insert B: %v", err)
	}
	if isNew2 {
		t.Fatal("expected duplicate transmission")
	}

	// Query observations and verify per-observation raw_hex
	type obsRow struct {
		RawHex   *string // NULL when the observation stored no raw_hex
		Observer *string // NULL when the LEFT JOIN finds no observer
	}
	got := testCollect(t, s, `
	SELECT o.raw_hex, obs.pubkey
	FROM observations o
	LEFT JOIN observers obs ON obs.id = o.observer_idx
	ORDER BY o.id ASC`,
		func(r pgx.CollectableRow) (obsRow, error) {
			var x obsRow
			err := r.Scan(&x.RawHex, &x.Observer)
			return x, err
		})

	// NULL reads as "" here, as the old sql.NullString.String did.
	str := func(p *string) string {
		if p == nil {
			return ""
		}
		return *p
	}
	type obsResult struct {
		rawHex     string
		observerID string
	}
	results := make([]obsResult, len(got))
	for i, r := range got {
		results[i] = obsResult{rawHex: str(r.RawHex), observerID: str(r.Observer)}
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 observations, got %d", len(results))
	}
	if results[0].rawHex != rawA {
		t.Errorf("obs A raw_hex: got %q, want %q", results[0].rawHex, rawA)
	}
	if results[1].rawHex != rawB {
		t.Errorf("obs B raw_hex: got %q, want %q", results[1].rawHex, rawB)
	}
	if results[0].rawHex == results[1].rawHex {
		t.Error("both observations have same raw_hex — should differ")
	}
}

// TestBuildPacketData_TraceUsesPayloadHops verifies that TRACE packets use
// payload-decoded route hops in path_json (NOT the raw_hex header SNR bytes).
// Issue #886 / #887.
func TestBuildPacketData_TraceUsesPayloadHops(t *testing.T) {
	// TRACE packet: header path has SNR bytes [30,2D,0D,23], but decoded.Path.Hops
	// is overwritten to payload hops [67,33,D6,33,67].
	rawHex := "2604302D0D2359FEE7B100000000006733D63367"
	decoded, err := decoder.DecodePacket(rawHex, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	// decoded.Path.Hops should be the TRACE-replaced hops (payload hops)
	if len(decoded.Path.Hops) != 5 {
		t.Fatalf("expected 5 decoded hops, got %d", len(decoded.Path.Hops))
	}

	msg := &MQTTPacketMessage{Raw: rawHex}
	pd := BuildPacketData(msg, decoded, "test-obs", "TST", nil)

	// For TRACE: path_json MUST be the payload-decoded route hops, NOT the SNR bytes
	expectedPathJSON := `["67","33","D6","33","67"]`
	if pd.PathJSON != expectedPathJSON {
		t.Errorf("path_json = %s, want %s (TRACE must use payload hops)", pd.PathJSON, expectedPathJSON)
	}

	// Verify that DecodePathFromRawHex returns the SNR bytes (header path) which differ
	headerHops, herr := packetpath.DecodePathFromRawHex(rawHex)
	if herr != nil {
		t.Fatal(herr)
	}
	headerJSON, _ := json.Marshal(headerHops)
	if string(headerJSON) == expectedPathJSON {
		t.Error("header path (SNR) should differ from payload hops for TRACE")
	}
}

// TestBuildPacketData_NonTracePathJSON verifies non-TRACE packets also derive path from raw_hex.
func TestBuildPacketData_NonTracePathJSON(t *testing.T) {
	// A simple ADVERT packet (payload type 0) with 2 hops, hash_size 1
	// Header 0x09 = FLOOD(1), ADVERT(2), version 0
	// Path byte 0x02 = hash_size 1, hash_count 2
	// Path bytes: AA BB
	rawHex := "0902AABB" + "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
	decoded, err := decoder.DecodePacket(rawHex, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	msg := &MQTTPacketMessage{Raw: rawHex}
	pd := BuildPacketData(msg, decoded, "obs1", "TST", nil)

	expectedPathJSON := `["AA","BB"]`
	if pd.PathJSON != expectedPathJSON {
		t.Errorf("path_json = %s, want %s", pd.PathJSON, expectedPathJSON)
	}
}

// TestScopeNameThreeStates pins how transmissions.scope_name encodes a packet's
// region: NULL (not scoped), ” (scoped, no region could be named), or a name.
func TestScopeNameThreeStates(t *testing.T) {
	s := newTestStore(t)
	ctx := testCtx(t)
	sp := func(v string) *string { return &v }
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	for _, c := range []struct {
		name   string
		hash   string
		scoped bool
		scope  string
		want   *string // nil means NULL
	}{
		{"scoped and matched", "hash-matched", true, "#belgium", sp("#belgium")},
		{"scoped, no region named", "hash-unmatched", true, "", sp("")},
		{"not scoped", "hash-unscoped", false, "", nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			data := &PacketData{
				RawHex: "aabb", Timestamp: at, Hash: c.hash,
				RouteType: types.RouteFlood, PayloadType: types.PayloadGRP_TXT,
				PathJSON: "[]", DecodedJSON: "{}",
				IsTransportScoped: c.scoped, ScopeName: c.scope,
			}
			if c.scoped {
				data.RouteType = types.RouteTransportFlood
				data.Code1, data.Code2 = "2AB5", "AABB"
			}
			if _, err := s.InsertTransmission(ctx, data); err != nil {
				t.Fatal(err)
			}

			var got *string
			mustScan(t, testRow(t, s, `SELECT scope_name FROM transmissions WHERE hash = $1`, c.hash), &got)
			switch {
			case c.want == nil && got != nil:
				t.Errorf("scope_name = %q, want NULL", *got)
			case c.want != nil && got == nil:
				t.Errorf("scope_name is NULL, want %q (an empty string must stay '' and not collapse to NULL)", *c.want)
			case c.want != nil && *got != *c.want:
				t.Errorf("scope_name = %q, want %q", *got, *c.want)
			}
		})
	}
}
func TestTransportCodesStored(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()

	// Byte order matters: Code1 is the RAW wire hex of the two bytes,
	// uppercased, with NO little-endian swap (decoder.go:954). Bytes AA BB
	// therefore read back as "AABB". Layout, per firmware packet_format.md:
	// header + transport_codes(4) + path_len + path + payload.
	// header 0x14 = route_type 0 (TRANSPORT_FLOOD), payload_type 5 (GRP_TXT).
	// payload_type 5 keeps the advert validation path out of this test.
	rawScoped := "14" + "AABB" + "CCDD" + "00" + strings.Repeat("11", 32)
	// header 0x15 = route_type 1 (FLOOD), payload_type 5 — no transport codes.
	rawUnscoped := "15" + "00" + strings.Repeat("22", 32)
	// header 0x14 = route_type 0 (TRANSPORT_FLOOD) with all-zero transport
	// codes. "0000" is stored literally (NOT NULL) — it is the meaningful
	// "explicitly unscoped transport packet" case and must not collapse
	// into the same NULL a non-transport packet gets.
	rawZeroCodes := "14" + "0000" + "0000" + "00" + strings.Repeat("33", 32)

	for _, raw := range []string{rawScoped, rawUnscoped, rawZeroCodes} {
		pd := &PacketData{
			RawHex: raw, Hash: decoder.ComputeContentHash(raw), Timestamp: time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC),
			ObserverID: "obs1", RouteType: 0, PayloadType: 5,
		}
		if raw == rawUnscoped {
			pd.RouteType = 1
		}
		if raw == rawScoped {
			pd.Code1, pd.Code2 = "AABB", "CCDD"
		}
		if raw == rawZeroCodes {
			pd.Code1, pd.Code2 = "0000", "0000"
		}
		if _, err := s.InsertTransmission(ctx, pd); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	var c1, c2 *string
	mustScan(t, testRow(t, s, `SELECT code1, code2 FROM transmissions WHERE hash = $1`, decoder.ComputeContentHash(rawScoped)), &c1, &c2)
	if c1 == nil || *c1 != "AABB" || c2 == nil || *c2 != "CCDD" {
		t.Errorf("scoped codes = %v/%v, want AABB/CCDD", c1, c2)
	}

	mustScan(t, testRow(t, s, `SELECT code1, code2 FROM transmissions WHERE hash = $1`, decoder.ComputeContentHash(rawUnscoped)), &c1, &c2)
	if c1 != nil || c2 != nil {
		t.Errorf("unscoped codes = %v/%v, want NULL/NULL", c1, c2)
	}

	mustScan(t, testRow(t, s, `SELECT code1, code2 FROM transmissions WHERE hash = $1`, decoder.ComputeContentHash(rawZeroCodes)), &c1, &c2)
	if c1 == nil || *c1 != "0000" || c2 == nil || *c2 != "0000" {
		t.Errorf("zero-codes codes = %v/%v, want literal \"0000\"/\"0000\" (not NULL)", c1, c2)
	}
}

// --- Feature 3: default_scope column on nodes (#899) ---

func TestUpdateNodeDefaultScope(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	// Insert a node into nodes and inactive_nodes so both tables can be updated.
	testExec(t, store, `INSERT INTO nodes (public_key, name, active) VALUES ('pk1', 'Node1', TRUE)`)
	testExec(t, store, `INSERT INTO nodes (public_key, name) VALUES ('pk2', 'Node2', FALSE)`)
	// First call: writes scope to both tables.
	if err := store.UpdateNodeDefaultScope(ctx, "pk1", "#belgium"); err != nil {
		t.Fatalf("UpdateNodeDefaultScope: %v", err)
	}
	var got string
	mustScan(t, testRow(t, store, `SELECT default_scope FROM nodes WHERE public_key = 'pk1'`), &got)
	if got != "#belgium" {
		t.Errorf("nodes.default_scope = %q, want #belgium", got)
	}
	var gotInactive string
	mustScan(t, testRow(t, store, `SELECT default_scope FROM nodes WHERE public_key = 'pk2'`), &gotInactive)
	if gotInactive != "#belgium" {
		t.Errorf("inactive_nodes.default_scope = %q, want #belgium", gotInactive)
	}

	// Second call with same value: short-circuit, no redundant UPDATE (verify no error and value stable).
	if err := store.UpdateNodeDefaultScope(ctx, "pk1", "#belgium"); err != nil {
		t.Fatalf("UpdateNodeDefaultScope short-circuit: %v", err)
	}
	mustScan(t, testRow(t, store, `SELECT default_scope FROM nodes WHERE public_key = 'pk1'`), &got)
	if got != "#belgium" {
		t.Errorf("after short-circuit nodes.default_scope = %q, want #belgium", got)
	}

	// Third call with different value: updates both tables.
	if err := store.UpdateNodeDefaultScope(ctx, "pk1", "#eu"); err != nil {
		t.Fatalf("UpdateNodeDefaultScope update: %v", err)
	}
	mustScan(t, testRow(t, store, `SELECT default_scope FROM nodes WHERE public_key = 'pk1'`), &got)
	if got != "#eu" {
		t.Errorf("after update nodes.default_scope = %q, want #eu", got)
	}
	if err := store.UpdateNodeDefaultScope(ctx, "pk2", "#eu"); err != nil {
		t.Fatalf("UpdateNodeDefaultScope update: %v", err)
	}
	mustScan(t, testRow(t, store, `SELECT default_scope FROM inactive_nodes WHERE public_key = 'pk1'`), &gotInactive)
	if gotInactive != "#eu" {
		t.Errorf("after update inactive_nodes.default_scope = %q, want #eu", gotInactive)
	}
}

// // --- Issue #888: Backfill path_json from raw_hex ---

// func TestBackfillPathJsonFromRawHex(t *testing.T) {
// 	dbPath := testfixtures.TempDBPath(t)
// 	s, err := OpenStore(dbPath)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	// Insert a transmission with payload_type != TRACE (e.g. 0x01)
// 	// raw_hex: header 0x05 (route FLOOD, payload 0x01), path byte 0x42 (hash_size=2, count=2),
// 	// hops: AABB, CCDD, then some payload bytes
// 	rawHex := "0542AABBCCDD0000000000000000000000000000"
// 	s.db.Exec(`INSERT INTO transmissions (raw_hex, hash, first_seen, payload_type) VALUES (?, 'h1', '2025-01-01T00:00:00Z', 1)`, rawHex)

// 	// Insert observation with raw_hex but empty path_json
// 	s.db.Exec(`INSERT INTO observations (transmission_id, timestamp, raw_hex, path_json) VALUES (1, 1000, ?, '[]')`, rawHex)
// 	// Insert observation with raw_hex and NULL path_json
// 	s.db.Exec(`INSERT INTO observations (transmission_id, timestamp, raw_hex, path_json) VALUES (1, 1001, ?, NULL)`, rawHex)
// 	// Insert observation with existing path_json (should NOT be overwritten)
// 	s.db.Exec(`INSERT INTO observations (transmission_id, timestamp, raw_hex, path_json) VALUES (1, 1002, ?, '["XX","YY"]')`, rawHex)

// 	// Insert a TRACE transmission (payload_type = 0x09) — should be skipped
// 	traceRaw := "2604302D0D2359FEE7B100000000006733D63367"
// 	s.db.Exec(`INSERT INTO transmissions (raw_hex, hash, first_seen, payload_type) VALUES (?, 'h2', '2025-01-01T00:00:00Z', 9)`, traceRaw)
// 	s.db.Exec(`INSERT INTO observations (transmission_id, timestamp, raw_hex, path_json) VALUES (2, 1003, ?, '[]')`, traceRaw)

// 	// Remove the migration marker so it runs again on reopen
// 	s.db.Exec(`DELETE FROM _migrations WHERE name = 'backfill_path_json_from_raw_hex_v1'`)
// 	s.Close()

// 	// Reopen — backfill is now async, must trigger explicitly
// 	s2, err := OpenStore(dbPath)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer s2.Close()

// 	// Trigger async backfill and wait for completion
// 	s2.BackfillPathJSONAsync()
// 	deadline := time.Now().Add(10 * time.Second)
// 	var migCount int
// 	for time.Now().Before(deadline) {
// 		s2.db.QueryRow("SELECT COUNT(*) FROM _migrations WHERE name = 'backfill_path_json_from_raw_hex_v1'").Scan(&migCount)
// 		if migCount == 1 {
// 			break
// 		}
// 		time.Sleep(50 * time.Millisecond)
// 	}
// 	if migCount != 1 {
// 		t.Fatalf("migration not recorded")
// 	}

// 	// Row 1 (was '[]') is NOT re-processed by the backfill — '[]' means
// 	// "already attempted, no hops" and is excluded by the WHERE to avoid the
// 	// infinite-loop bug fixed in #1119. It must remain '[]'.
// 	var pj1 string
// 	s2.db.QueryRow("SELECT path_json FROM observations WHERE id = 1").Scan(&pj1)
// 	if pj1 != "[]" {
// 		t.Errorf("row 1 path_json = %q, want %q (must not re-process '[]' rows after #1119)", pj1, "[]")
// 	}

// 	// Row 2 (was NULL) should now have decoded hops
// 	var pj2 string
// 	s2.db.QueryRow("SELECT path_json FROM observations WHERE id = 2").Scan(&pj2)
// 	if pj2 != `["AABB","CCDD"]` {
// 		t.Errorf("row 2 path_json = %q, want %q", pj2, `["AABB","CCDD"]`)
// 	}

// 	// Row 3 (had existing data) should NOT be overwritten
// 	var pj3 string
// 	s2.db.QueryRow("SELECT path_json FROM observations WHERE id = 3").Scan(&pj3)
// 	if pj3 != `["XX","YY"]` {
// 		t.Errorf("row 3 path_json = %q, want %q (should not be overwritten)", pj3, `["XX","YY"]`)
// 	}

// 	// Row 4 (TRACE) should NOT be updated
// 	var pj4 string
// 	s2.db.QueryRow("SELECT path_json FROM observations WHERE id = 4").Scan(&pj4)
// 	if pj4 != "[]" {
// 		t.Errorf("row 4 (TRACE) path_json = %q, want %q (should be skipped)", pj4, "[]")
// 	}
// }

// func TestCleanupLegacyNullHashTimestamp(t *testing.T) {
// 	path := testfixtures.TempDBPath(t)

// 	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	db.Exec(`CREATE TABLE IF NOT EXISTS transmissions (
// 		id INTEGER PRIMARY KEY AUTOINCREMENT,
// 		raw_hex TEXT NOT NULL,
// 		hash TEXT NOT NULL,
// 		first_seen TEXT NOT NULL,
// 		route_type INTEGER,
// 		payload_type INTEGER,
// 		payload_version INTEGER,
// 		decoded_json TEXT,
// 		created_at TEXT DEFAULT (datetime('now'))
// 	)`)
// 	db.Exec(`CREATE TABLE IF NOT EXISTS observations (
// 		id INTEGER PRIMARY KEY AUTOINCREMENT,
// 		transmission_id INTEGER NOT NULL REFERENCES transmissions(id),
// 		observer_idx INTEGER,
// 		direction TEXT,
// 		snr REAL,
// 		rssi REAL,
// 		score INTEGER,
// 		path_json TEXT,
// 		timestamp INTEGER NOT NULL
// 	)`)
// 	db.Exec(`CREATE TABLE IF NOT EXISTS nodes (
// 		public_key TEXT PRIMARY KEY, name TEXT, role TEXT, lat REAL, lon REAL,
// 		last_seen TEXT, first_seen TEXT, advert_count INTEGER DEFAULT 0
// 	)`)
// 	db.Exec(`CREATE TABLE IF NOT EXISTS inactive_nodes (
// 		public_key TEXT PRIMARY KEY, name TEXT, role TEXT, lat REAL, lon REAL,
// 		last_seen TEXT, first_seen TEXT, advert_count INTEGER DEFAULT 0
// 	)`)
// 	db.Exec(`CREATE TABLE IF NOT EXISTS observers (
// 		id TEXT PRIMARY KEY, name TEXT, last_seen TEXT, first_seen TEXT,
// 		packet_count INTEGER DEFAULT 0, model TEXT, firmware TEXT,
// 		client_version TEXT, radio TEXT, battery_mv INTEGER, uptime_secs INTEGER,
// 		noise_floor REAL
// 	)`)

// 	db.Exec(`INSERT INTO transmissions (id, raw_hex, hash, first_seen) VALUES (1, 'aabb', 'abc123', '2024-01-01T00:00:00Z')`)
// 	db.Exec(`INSERT INTO observations (transmission_id, observer_idx, timestamp) VALUES (1, 1, 1704067200)`)

// 	db.Exec(`INSERT INTO transmissions (id, raw_hex, hash, first_seen) VALUES (2, 'ccdd', '', '2024-01-01T00:00:00Z')`)
// 	db.Exec(`INSERT INTO observations (transmission_id, observer_idx, timestamp) VALUES (2, 1, 1704067200)`)

// 	db.Exec(`INSERT INTO transmissions (id, raw_hex, hash, first_seen) VALUES (3, 'eeff', 'def456', '')`)
// 	db.Exec(`INSERT INTO observations (transmission_id, observer_idx, timestamp) VALUES (3, 2, 1704067200)`)

// 	db.Close()

// 	// NEW: bring the raw schema under goose's control before OpenStore.
// 	baselineConn, err := sql.Open("sqlite", path)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	if _, err := database.BaselineStampMigrate(baselineConn, nil); err != nil {
// 		t.Fatalf("baseline: %v", err)
// 	}
// 	baselineConn.Close()

// 	s, err := OpenStore(path)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer s.Close()

// 	var count int
// 	s.db.QueryRow("SELECT COUNT(*) FROM transmissions WHERE id = 1").Scan(&count)
// 	if count != 1 {
// 		t.Error("good transmission should not be deleted")
// 	}

// 	s.db.QueryRow("SELECT COUNT(*) FROM transmissions WHERE id = 2").Scan(&count)
// 	if count != 0 {
// 		t.Errorf("transmission with empty hash should be deleted, got count=%d", count)
// 	}
// 	s.db.QueryRow("SELECT COUNT(*) FROM transmissions WHERE id = 3").Scan(&count)
// 	if count != 0 {
// 		t.Errorf("transmission with empty first_seen should be deleted, got count=%d", count)
// 	}

// 	s.db.QueryRow("SELECT COUNT(*) FROM observations WHERE transmission_id IN (2, 3)").Scan(&count)
// 	if count != 0 {
// 		t.Errorf("observations for bad transmissions should be deleted, got count=%d", count)
// 	}

// 	s.db.QueryRow("SELECT COUNT(*) FROM observations WHERE transmission_id = 1").Scan(&count)
// 	if count != 1 {
// 		t.Error("observation for good transmission should remain")
// 	}

// 	applied, err := database.MigrationApplied(s.db, 17)
// 	if err != nil {
// 		t.Fatalf("checking goose version 17: %v", err)
// 	}
// 	if !applied {
// 		t.Error("goose migration 00017_cleanup_legacy_null_hash_ts should be applied")
// 	}

// 	s.Close()
// 	s2, err := OpenStore(path)
// 	if err != nil {
// 		t.Fatal("second open should not fail:", err)
// 	}
// 	s2.Close()
// }

func TestBuildPacketDataRegionFromPayload(t *testing.T) {
	msg := &MQTTPacketMessage{Raw: "0102030405060708", Region: "PDX"}
	decoded := &decoder.DecodedPacket{
		Header: decoder.Header{RouteType: 1, PayloadType: 3},
	}
	pkt := BuildPacketData(msg, decoded, "obs1", "SJC", nil)
	// When payload has region, it should override the topic-derived region
	if pkt.Region != "PDX" {
		t.Fatalf("expected region PDX from payload, got %q", pkt.Region)
	}
}

func TestBuildPacketDataRegionFallsBackToTopic(t *testing.T) {
	msg := &MQTTPacketMessage{Raw: "0102030405060708"}
	decoded := &decoder.DecodedPacket{
		Header: decoder.Header{RouteType: 1, PayloadType: 3},
	}
	pkt := BuildPacketData(msg, decoded, "obs1", "SJC", nil)
	if pkt.Region != "SJC" {
		t.Fatalf("expected region SJC from topic, got %q", pkt.Region)
	}
}

// // TestBackfillPathJSONAsync verifies that the path_json backfill does NOT block
// // OpenStore from returning. MQTT connect happens immediately after OpenStore;
// // if the backfill is synchronous, MQTT would be delayed indefinitely on large DBs.
// // This test creates pending backfill rows, opens the store, and asserts that
// // OpenStore returns before the migration is recorded — proving async execution.
// func TestBackfillPathJSONAsync(t *testing.T) {
// 	dir := t.TempDir()
// 	dbPath := filepath.Join(dir, "async_test.db")

// 	db, err := sql.Open("sqlite", dbPath+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	_, err = db.Exec(`
// 		CREATE TABLE _migrations (name TEXT PRIMARY KEY);
// 		CREATE TABLE transmissions (
// 			id INTEGER PRIMARY KEY AUTOINCREMENT,
// 			raw_hex TEXT NOT NULL,
// 			hash TEXT NOT NULL UNIQUE,
// 			first_seen TEXT NOT NULL,
// 			route_type INTEGER,
// 			payload_type INTEGER,
// 			payload_version INTEGER,
// 			decoded_json TEXT,
// 			created_at TEXT DEFAULT (datetime('now'))
// 		);
// 		CREATE TABLE observers (
// 			id TEXT PRIMARY KEY,
// 			name TEXT,
// 			iata TEXT,
// 			last_seen TEXT,
// 			first_seen TEXT,
// 			packet_count INTEGER DEFAULT 0,
// 			model TEXT,
// 			firmware TEXT,
// 			client_version TEXT,
// 			radio TEXT,
// 			battery_mv INTEGER,
// 			uptime_secs INTEGER,
// 			noise_floor REAL,
// 			last_packet_at TEXT
// 		);
// 		CREATE TABLE nodes (
// 			public_key TEXT PRIMARY KEY,
// 			name TEXT, role TEXT, lat REAL, lon REAL,
// 			last_seen TEXT, first_seen TEXT, advert_count INTEGER DEFAULT 0,
// 			battery_mv INTEGER, temperature_c REAL
// 		);
// 		CREATE TABLE inactive_nodes (
// 			public_key TEXT PRIMARY KEY,
// 			name TEXT, role TEXT, lat REAL, lon REAL,
// 			last_seen TEXT, first_seen TEXT, advert_count INTEGER DEFAULT 0,
// 			battery_mv INTEGER, temperature_c REAL
// 		);
// 		CREATE TABLE observations (
// 			id INTEGER PRIMARY KEY AUTOINCREMENT,
// 			transmission_id INTEGER NOT NULL REFERENCES transmissions(id),
// 			observer_idx INTEGER,
// 			direction TEXT,
// 			snr REAL, rssi REAL, score INTEGER,
// 			path_json TEXT,
// 			timestamp INTEGER NOT NULL,
// 			raw_hex TEXT
// 		);
// 		CREATE UNIQUE INDEX idx_observations_dedup ON observations(transmission_id, observer_idx, COALESCE(path_json, ''));
// 		CREATE INDEX idx_observations_transmission_id ON observations(transmission_id);
// 		CREATE INDEX idx_observations_observer_idx ON observations(observer_idx);
// 		CREATE INDEX idx_observations_timestamp ON observations(timestamp);
// 		CREATE TABLE observer_metrics (
// 			observer_id TEXT NOT NULL,
// 			timestamp TEXT NOT NULL,
// 			noise_floor REAL, tx_air_secs INTEGER, rx_air_secs INTEGER,
// 			recv_errors INTEGER, battery_mv INTEGER,
// 			PRIMARY KEY (observer_id, timestamp)
// 		);
// 		CREATE TABLE dropped_packets (
// 			id INTEGER PRIMARY KEY AUTOINCREMENT,
// 			hash TEXT, raw_hex TEXT, reason TEXT NOT NULL,
// 			observer_id TEXT, observer_name TEXT,
// 			node_pubkey TEXT, node_name TEXT,
// 			dropped_at DATETIME DEFAULT CURRENT_TIMESTAMP
// 		);
// 	`)
// 	if err != nil {
// 		t.Fatal("bootstrap schema:", err)
// 	}

// 	// Mark all migrations as done EXCEPT the path_json backfill
// 	for _, m := range []string{
// 		"advert_count_unique_v1", "noise_floor_real_v1", "node_telemetry_v1",
// 		"obs_timestamp_index_v1", "observer_metrics_v1", "observer_metrics_ts_idx",
// 		"observers_inactive_v1", "observer_metrics_packets_v1", "channel_hash_v1",
// 		"dropped_packets_v1", "observations_raw_hex_v1", "observers_last_packet_at_v1",
// 		"cleanup_legacy_null_hash_ts",
// 	} {
// 		db.Exec(`INSERT INTO _migrations (name) VALUES (?)`, m)
// 	}

// 	rawHex := "41020304AABBCCDD05060708"
// 	_, err = db.Exec(`INSERT INTO transmissions (raw_hex, hash, first_seen, payload_type) VALUES (?, 'hash1', '2025-01-01T00:00:00Z', 4)`, rawHex)
// 	if err != nil {
// 		t.Fatal("insert tx:", err)
// 	}
// 	for i := 0; i < 100; i++ {
// 		_, err = db.Exec(`INSERT INTO observations (transmission_id, observer_idx, timestamp, raw_hex, path_json) VALUES (1, ?, ?, ?, NULL)`,
// 			i+1, 1700000000+i, rawHex)
// 		if err != nil {
// 			t.Fatalf("insert obs %d: %v", i, err)
// 		}
// 	}
// 	db.Close()

// 	// Bring the pre-existing schema fully under goose's control before
// 	// opening via the real ingestor path — matches production, where
// 	// `migrate -baseline-and-stamp` (plus filling any gaps) runs before
// 	// the ingestor ever starts. OpenStore itself only asserts readiness.
// 	baselineConn, err := sql.Open("sqlite", dbPath)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	if _, err := database.BaselineStampMigrate(baselineConn, nil); err != nil {
// 		t.Fatalf("baseline+migrate: %v", err)
// 	}
// 	baselineConn.Close()

// 	// Now open store via OpenStore — this must return QUICKLY (non-blocking)
// 	start := time.Now()
// 	store, err := OpenStoreWithInterval(dbPath, 300)
// 	elapsed := time.Since(start)
// 	if err != nil {
// 		t.Fatal("OpenStore:", err)
// 	}
// 	defer store.Close()

// 	// OpenStore must return in under 2 seconds (backfill is no longer in applySchema)
// 	if elapsed > 2*time.Second {
// 		t.Fatalf("OpenStore blocked for %v — backfill must not run in applySchema", elapsed)
// 	}

// 	// Backfill must NOT be recorded yet — it hasn't been triggered
// 	var done int
// 	err = store.db.QueryRow("SELECT 1 FROM _migrations WHERE name = 'backfill_path_json_from_raw_hex_v1'").Scan(&done)
// 	if err == nil {
// 		t.Fatal("migration recorded during OpenStore — backfill must be async via BackfillPathJSONAsync()")
// 	}

// 	// Now trigger the async backfill (simulates what main.go does after OpenStore)
// 	store.BackfillPathJSONAsync()

// 	// Wait for backfill to complete (should be very fast with 100 rows)
// 	deadline := time.Now().Add(10 * time.Second)
// 	for time.Now().Before(deadline) {
// 		err = store.db.QueryRow("SELECT 1 FROM _migrations WHERE name = 'backfill_path_json_from_raw_hex_v1'").Scan(&done)
// 		if err == nil {
// 			break
// 		}
// 		time.Sleep(100 * time.Millisecond)
// 	}
// 	if err != nil {
// 		t.Fatal("backfill never completed within 10s")
// 	}

// 	// Verify backfill actually worked — observations should have non-NULL path_json
// 	var nullCount int
// 	store.db.QueryRow("SELECT COUNT(*) FROM observations WHERE path_json IS NULL").Scan(&nullCount)
// 	if nullCount > 0 {
// 		t.Errorf("backfill left %d observations with NULL path_json", nullCount)
// 	}
// }

// // TestBackfillPathJSONAsyncMethodExists verifies the async backfill API surface
// // exists — BackfillPathJSONAsync must be callable independently from OpenStore.
// func TestBackfillPathJSONAsyncMethodExists(t *testing.T) {
// 	dir := t.TempDir()
// 	dbPath := filepath.Join(dir, "method_test.db")
// 	store, err := OpenStoreWithInterval(dbPath, 300)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer store.Close()

// 	// BackfillPathJSONAsync must exist as a method on *Store
// 	// This is a compile-time check — if the method doesn't exist, the test won't compile.
// 	store.BackfillPathJSONAsync()
// }

// // TestBackfillPathJSONAsync_BracketRowsTerminate exercises the infinite-loop bug
// // from issue #1119. Observations whose path_json is already '[]' (meaning a prior
// // backfill pass attempted to decode them and found no hops) must NOT be re-selected
// // by the WHERE clause — otherwise the loop rewrites the same '[]' value forever
// // and never records the migration marker.
// //
// // This test seeds N rows with path_json='[]' and a raw_hex that DecodePathFromRawHex
// // resolves to zero hops. With the bug, the backfill loops infinitely re-UPDATEing
// // the same rows back to '[]', batch is never empty, migration marker is never
// // written. With the fix, no rows match → the very first batch is empty → migration
// // is recorded immediately.
// func TestBackfillPathJSONAsync_BracketRowsTerminate(t *testing.T) {
// 	dir := t.TempDir()
// 	dbPath := filepath.Join(dir, "bracket_terminate.db")

// 	db, err := sql.Open("sqlite", dbPath+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	_, err = db.Exec(`
// 		CREATE TABLE _migrations (name TEXT PRIMARY KEY);
// 		CREATE TABLE transmissions (
// 			id INTEGER PRIMARY KEY AUTOINCREMENT,
// 			raw_hex TEXT NOT NULL,
// 			hash TEXT NOT NULL UNIQUE,
// 			first_seen TEXT NOT NULL,
// 			route_type INTEGER,
// 			payload_type INTEGER,
// 			payload_version INTEGER,
// 			decoded_json TEXT,
// 			created_at TEXT DEFAULT (datetime('now'))
// 		);
// 		CREATE TABLE observers (
// 			id TEXT PRIMARY KEY, name TEXT, iata TEXT,
// 			last_seen TEXT, first_seen TEXT, packet_count INTEGER DEFAULT 0,
// 			model TEXT, firmware TEXT, client_version TEXT, radio TEXT,
// 			battery_mv INTEGER, uptime_secs INTEGER, noise_floor REAL,
// 			inactive INTEGER DEFAULT 0, last_packet_at TEXT
// 		);
// 		CREATE TABLE nodes (
// 			public_key TEXT PRIMARY KEY, name TEXT, role TEXT,
// 			lat REAL, lon REAL, last_seen TEXT, first_seen TEXT,
// 			advert_count INTEGER DEFAULT 0, battery_mv INTEGER, temperature_c REAL
// 		);
// 		CREATE TABLE inactive_nodes (
// 			public_key TEXT PRIMARY KEY, name TEXT, role TEXT,
// 			lat REAL, lon REAL, last_seen TEXT, first_seen TEXT,
// 			advert_count INTEGER DEFAULT 0, battery_mv INTEGER, temperature_c REAL
// 		);
// 		CREATE TABLE observations (
// 			id INTEGER PRIMARY KEY AUTOINCREMENT,
// 			transmission_id INTEGER NOT NULL REFERENCES transmissions(id),
// 			observer_idx INTEGER, direction TEXT,
// 			snr REAL, rssi REAL, score INTEGER,
// 			path_json TEXT,
// 			timestamp INTEGER NOT NULL,
// 			raw_hex TEXT
// 		);
// 		CREATE UNIQUE INDEX idx_observations_dedup ON observations(transmission_id, observer_idx, COALESCE(path_json, ''));
// 		CREATE INDEX idx_observations_transmission_id ON observations(transmission_id);
// 		CREATE INDEX idx_observations_observer_idx ON observations(observer_idx);
// 		CREATE INDEX idx_observations_timestamp ON observations(timestamp);
// 		CREATE TABLE observer_metrics (
// 			observer_id TEXT NOT NULL, timestamp TEXT NOT NULL,
// 			noise_floor REAL, tx_air_secs INTEGER, rx_air_secs INTEGER,
// 			recv_errors INTEGER, battery_mv INTEGER,
// 			packets_sent INTEGER, packets_recv INTEGER,
// 			PRIMARY KEY (observer_id, timestamp)
// 		);
// 		CREATE TABLE dropped_packets (
// 			id INTEGER PRIMARY KEY AUTOINCREMENT,
// 			hash TEXT, raw_hex TEXT, reason TEXT NOT NULL,
// 			observer_id TEXT, observer_name TEXT,
// 			node_pubkey TEXT, node_name TEXT,
// 			dropped_at DATETIME DEFAULT CURRENT_TIMESTAMP
// 		);
// 	`)
// 	if err != nil {
// 		t.Fatal("bootstrap schema:", err)
// 	}

// 	// Mark all migrations done EXCEPT backfill_path_json_from_raw_hex_v1.
// 	for _, m := range []string{
// 		"advert_count_unique_v1", "noise_floor_real_v1", "node_telemetry_v1",
// 		"obs_timestamp_index_v1", "observer_metrics_v1", "observer_metrics_ts_idx",
// 		"observers_inactive_v1", "observer_metrics_packets_v1", "channel_hash_v1",
// 		"dropped_packets_v1", "observations_raw_hex_v1", "observers_last_packet_at_v1",
// 		"cleanup_legacy_null_hash_ts",
// 	} {
// 		db.Exec(`INSERT INTO _migrations (name) VALUES (?)`, m)
// 	}

// 	rawHex := "0A00DEADBEEF"
// 	_, err = db.Exec(`INSERT INTO transmissions (raw_hex, hash, first_seen, payload_type) VALUES (?, 'h_brackets', '2025-01-01T00:00:00Z', 2)`, rawHex)
// 	if err != nil {
// 		t.Fatal("insert tx:", err)
// 	}
// 	const seedCount = 100
// 	for i := 0; i < seedCount; i++ {
// 		_, err = db.Exec(`INSERT INTO observations (transmission_id, observer_idx, timestamp, raw_hex, path_json) VALUES (1, ?, ?, ?, '[]')`,
// 			i+1, 1700000000+i, rawHex)
// 		if err != nil {
// 			t.Fatalf("insert obs %d: %v", i, err)
// 		}
// 	}
// 	db.Close()

// 	// Bring the pre-existing schema fully under goose's control before
// 	// opening via the real ingestor path.
// 	baselineConn, err := sql.Open("sqlite", dbPath)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	if _, err := database.BaselineStampMigrate(baselineConn, nil); err != nil {
// 		t.Fatalf("baseline+migrate: %v", err)
// 	}
// 	baselineConn.Close()

// 	store, err := OpenStoreWithInterval(dbPath, 300)
// 	if err != nil {
// 		t.Fatal("OpenStore:", err)
// 	}
// 	defer store.Close()

// 	// Trigger backfill. With the bug, every iteration re-fetches all 100 rows
// 	// (because '[]' matches the WHERE), rewrites them to '[]', sleeps 50ms, repeats.
// 	// The loop never terminates and the migration marker is never written.
// 	store.BackfillPathJSONAsync()

// 	// Generous deadline: with the fix the marker is written essentially immediately.
// 	// With the bug the marker is never written within any bounded time.
// 	deadline := time.Now().Add(5 * time.Second)
// 	var done int
// 	for time.Now().Before(deadline) {
// 		err = store.db.QueryRow("SELECT 1 FROM _migrations WHERE name = 'backfill_path_json_from_raw_hex_v1'").Scan(&done)
// 		if err == nil {
// 			break
// 		}
// 		time.Sleep(50 * time.Millisecond)
// 	}
// 	if err != nil {
// 		t.Fatalf("issue #1119: backfill never recorded migration marker within 5s — infinite loop on path_json='[]' rows")
// 	}

// 	// Verify the seeded '[]' rows still have '[]' (sanity — neither bug nor fix
// 	// should change their value), and that there are no NULL/empty path_json rows
// 	// the backfill should have processed.
// 	var bracketCount int
// 	store.db.QueryRow("SELECT COUNT(*) FROM observations WHERE path_json = '[]'").Scan(&bracketCount)
// 	if bracketCount != seedCount {
// 		t.Errorf("expected %d rows with path_json='[]', got %d", seedCount, bracketCount)
// 	}
// }

// // TestSchemaMultibyteSupColumns verifies that the multibyte_sup_v1 migration adds
// // the expected columns and is idempotent across multiple OpenStore calls.
// func TestSchemaMultibyteSupColumns(t *testing.T) {
// 	dir := t.TempDir()
// 	dbPath := filepath.Join(dir, "test.db")

// 	store, err := OpenStore(dbPath)
// 	if err != nil {
// 		t.Fatalf("OpenStore: %v", err)
// 	}
// 	defer store.Close()

// 	for _, table := range []string{"nodes", "inactive_nodes"} {
// 		rows, err := store.db.Query("PRAGMA table_info(" + table + ")")
// 		if err != nil {
// 			t.Fatalf("PRAGMA table_info(%s): %v", table, err)
// 		}
// 		var foundSup, foundEvid bool
// 		for rows.Next() {
// 			var cid int
// 			var name, colType string
// 			var notNull, pk int
// 			var dflt interface{}
// 			if rows.Scan(&cid, &name, &colType, &notNull, &dflt, &pk) == nil {
// 				if name == "multibyte_sup" {
// 					foundSup = true
// 				}
// 				if name == "multibyte_evidence" {
// 					foundEvid = true
// 				}
// 			}
// 		}
// 		rows.Close()
// 		if !foundSup {
// 			t.Errorf("table %s: multibyte_sup column missing", table)
// 		}
// 		if !foundEvid {
// 			t.Errorf("table %s: multibyte_evidence column missing", table)
// 		}
// 	}

// 	// Verify migration is present. As of #1324 follow-up the migration
// 	// lives in internal/dbschema (column-probe + idempotent ALTER), not
// 	// in the legacy _migrations marker table — so we just re-assert the
// 	// columns exist and the second OpenStore is a no-op.
// 	store.Close()
// 	store2, err := OpenStore(dbPath)
// 	if err != nil {
// 		t.Fatalf("OpenStore (second open): %v", err)
// 	}
// 	store2.Close()
// }

// TestUpdateNodeDefaultScope_EmptyScopeIsNoop is the DB-layer defense-in-depth
// regression test for #1534. Even if the call-site guard at main.go:720 is
// later removed or refactored, the DB function MUST refuse to overwrite a
// previously-correct default_scope with the empty string. This is the
// belt-and-braces guard recommended by adversarial review (MAJOR-2) and
// dijkstra review (MINOR-2).
func TestUpdateNodeDefaultScope_EmptyScopeIsNoop(t *testing.T) {
	s := newTestStore(t)
	ctx := testCtx(t)

	scopeOf := func(pk string) *string {
		var v *string
		mustScan(t, testRow(t, s, `SELECT default_scope FROM nodes WHERE public_key = $1`, pk), &v)
		return v
	}

	// One active node and one inactive node (the old inactive_nodes mirror is the
	// same table now). The guard must hold for both.
	for _, pk := range []string{"pk1", "pk2"} {
		if err := s.UpsertNode(ctx, pk, "Node-"+pk, "repeater", nil, nil, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		if err := s.UpdateNodeDefaultScope(ctx, pk, "#belgium"); err != nil {
			t.Fatal(err)
		}
	}
	testExec(t, s, `UPDATE nodes SET inactive = true WHERE public_key = 'pk2'`)

	// Positive control: a non-empty scope was written, so the checks below can
	// only pass because the empty call did nothing.
	for _, pk := range []string{"pk1", "pk2"} {
		if got := scopeOf(pk); got == nil || *got != "#belgium" {
			t.Fatalf("%s: setup default_scope = %v, want #belgium", pk, got)
		}
	}

	// Empty-scope call must be a silent no-op (return nil), NOT overwrite.
	for _, pk := range []string{"pk1", "pk2"} {
		if err := s.UpdateNodeDefaultScope(ctx, pk, ""); err != nil {
			t.Fatalf("UpdateNodeDefaultScope(%q, \"\") returned error: %v (want nil)", pk, err)
		}
		if got := scopeOf(pk); got == nil || *got != "#belgium" {
			t.Errorf("%s: default_scope after empty-scope call = %v, want #belgium (guard missing, #1534)", pk, got)
		}
	}
}

// The update writes only when the value changes. Without that, the hot ingest
// path rewrites the row on every packet.
func TestUpdateNodeDefaultScope_WritesOnlyWhenChanged(t *testing.T) {
	s := newTestStore(t)
	ctx := testCtx(t)
	if err := s.UpsertNode(ctx, "pk1", "Node1", "repeater", nil, nil, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	scope := func() *string {
		var v *string
		mustScan(t, testRow(t, s, `SELECT default_scope FROM nodes WHERE public_key = 'pk1'`), &v)
		return v
	}
	rowVersion := func() string { // xmin changes whenever the row is rewritten
		var x string
		mustScan(t, testRow(t, s, `SELECT xmin::text FROM nodes WHERE public_key = 'pk1'`), &x)
		return x
	}
	update := func(v string) {
		if err := s.UpdateNodeDefaultScope(ctx, "pk1", v); err != nil {
			t.Fatal(err)
		}
	}

	if scope() != nil {
		t.Fatal("a new node should have no default_scope")
	}
	// NULL to a value must write. This is the case a plain `!=` gets wrong,
	// because NULL != 'x' is NULL, so the first scope would never be stored.
	update("#belgium")
	if got := scope(); got == nil || *got != "#belgium" {
		t.Fatalf("default_scope = %v after the first write, want #belgium", got)
	}
	v1 := rowVersion()

	update("#belgium") // same value: no new row version
	if rowVersion() != v1 {
		t.Error("writing the same scope rewrote the row")
	}

	update("#france") // a different value is written
	if got := scope(); got == nil || *got != "#france" {
		t.Errorf("default_scope = %v, want #france", got)
	}
	if rowVersion() == v1 {
		t.Error("a changed scope did not rewrite the row")
	}
}

// --- Startup failure behavior (goose adoption, #????) ---

// TestOpenStore_RefusesUnbaselinedPreexistingSchema verifies that a
// database with real, pre-existing raw-SQL schema — but never run
// through `migrate -baseline-and-stamp` — is refused at startup with a
// clear, actionable error, rather than silently auto-detected and
// fixed (that auto-fix behavior moved out of the ingestor entirely).
// func TestOpenStore_RefusesUnbaselinedPreexistingSchema(t *testing.T) {
// 	path := testfixtures.TempDBPath(t)

// 	db, err := sql.Open("sqlite", path)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	database.SeedUnstampedSchema(db, 1)

// 	db.Close()

// 	s, err := OpenStore(path)
// 	if err == nil {
// 		s.Close()
// 		t.Fatal("expected OpenStore to fail against a pre-existing, unbaselined schema")
// 	}
// 	if !strings.Contains(err.Error(), "baseline") {
// 		t.Errorf("expected error to mention baselining as the fix, got: %v", err)
// 	}
// }

// TestOpenStore_SucceedsAfterBaselineAndStamp is the positive-path
// companion to the refusal tests above — confirms the documented fix
// (running migrate -baseline-and-stamp first) actually resolves the
// // failure.
// func TestOpenStore_SucceedsAfterBaselineAndStamp(t *testing.T) {
// 	path := testfixtures.TempDBPath(t)

// 	db, err := sql.Open("sqlite", path)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	database.SeedUnstampedSchema(db, 1)
// 	db.Close()

// 	if s, err := OpenStore(path); err == nil {
// 		s.Close()
// 		t.Fatal("precondition failed: expected OpenStore to fail before baselining")
// 	}

// 	baselineConn, err := sql.Open("sqlite", path)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	if _, err := database.BaselineStampMigrate(baselineConn, nil); err != nil {
// 		t.Fatalf("baseline+migrate: %v", err)
// 	}
// 	baselineConn.Close()

// 	s, err := OpenStore(path)
// 	if err != nil {
// 		t.Fatalf("expected OpenStore to succeed after baselining, got: %v", err)
// 	}
// 	defer s.Close()

// }
func TestInsertClientRxObservation(t *testing.T) {
	s := newTestStore(t)
	ctx := testCtx(t)
	code1, fwd := "AABB", "1a2b"
	snr, rssi, posAccM := -7.5, -92, 8.5
	rxAt := time.Date(2026, 8, 17, 10, 0, 0, 123*int(time.Millisecond), time.UTC)
	o := &types.ClientRxObservation{
		RxPubkey: "aa11", RxAt: rxAt, IngestedAt: time.Date(2026, 8, 17, 10, 0, 1, 0, time.UTC),
		PktHash: "0123456789abcdef", RouteType: types.RouteTransportFlood, PayloadType: types.PayloadADVERT,
		HashSize: 2, HopCount: 1, Code1: &code1, Forwarder: &fwd,
		Lat: 51.2, Lon: 4.4, SNR: &snr, RSSI: &rssi, PosAccM: &posAccM,
	}
	ins, err := s.InsertClientRxObservation(ctx, o)
	if err != nil || !ins {
		t.Fatalf("insert: ins=%v err=%v", ins, err)
	}

	// Geometry/signal round-trip: lat/lon/snr/rssi/pos_acc_m are never read back
	// anywhere else in the suite, so a field swap in the wrapper's mapping (e.g.
	// lat<->lon) would pass green. lat and lon are deliberately unequal so a swap
	// is detectable.
	var gotLat, gotLon float64
	var gotSNR, gotPosAccM *float64
	var gotRSSI *int32
	var gotRoute types.RouteType
	var gotPayload types.PayloadType
	var gotHashSize, gotHops int32
	var gotRxAt time.Time
	var gotCode1, gotCode2, gotFwd *string
	mustScan(t, testRow(t, s, `SELECT lat, lon, snr, rssi, pos_acc_m, route_type, payload_type,
			hash_size, hop_count, rx_at, code1, code2, forwarder
		FROM client_rx_observations WHERE pkt_hash = $1`, o.PktHash),
		&gotLat, &gotLon, &gotSNR, &gotRSSI, &gotPosAccM, &gotRoute, &gotPayload,
		&gotHashSize, &gotHops, &gotRxAt, &gotCode1, &gotCode2, &gotFwd)

	if gotLat != 51.2 || gotLon != 4.4 {
		t.Errorf("lat/lon = %v/%v, want 51.2/4.4 (a lat<->lon swap would pass with symmetric values)", gotLat, gotLon)
	}
	if gotSNR == nil || *gotSNR != -7.5 {
		t.Errorf("snr = %v, want -7.5", gotSNR)
	}
	if gotRSSI == nil || *gotRSSI != -92 {
		t.Errorf("rssi = %v, want -92", gotRSSI)
	}
	if gotPosAccM == nil || *gotPosAccM != 8.5 {
		t.Errorf("pos_acc_m = %v, want 8.5", gotPosAccM)
	}
	// The enums round-trip through the sqlc column overrides.
	if gotRoute != types.RouteTransportFlood || gotPayload != types.PayloadADVERT {
		t.Errorf("route/payload = %v/%v, want %v/%v", gotRoute, gotPayload, types.RouteTransportFlood, types.PayloadADVERT)
	}
	if gotHashSize != 2 || gotHops != 1 {
		t.Errorf("hash_size/hop_count = %d/%d, want 2/1", gotHashSize, gotHops)
	}
	// rx_at keeps millisecond precision: it is half of the unique key, and the
	// second copy below depends on it.
	if !gotRxAt.Equal(rxAt) {
		t.Errorf("rx_at = %v, want %v", gotRxAt, rxAt)
	}
	if gotCode1 == nil || *gotCode1 != "AABB" || gotFwd == nil || *gotFwd != "1a2b" {
		t.Errorf("code1/forwarder = %v/%v, want AABB/1a2b", gotCode1, gotFwd)
	}
	// A field the fixture never set stays NULL, never an empty string.
	if gotCode2 != nil {
		t.Errorf("code2 = %q, want NULL (never set)", *gotCode2)
	}

	// Same (rx_pubkey, pkt_hash, rx_at) → idempotent, no second row.
	ins, err = s.InsertClientRxObservation(ctx, o)
	if err != nil || ins {
		t.Fatalf("re-insert: ins=%v err=%v (want false, nil)", ins, err)
	}

	// A second copy of the same flood from another forwarder, 40 ms later,
	// MUST create its own row: that multiplicity is the amplification signal.
	fwd2 := "3c4d"
	o2 := *o
	o2.RxAt, o2.Forwarder = rxAt.Add(40*time.Millisecond), &fwd2
	if ins, err = s.InsertClientRxObservation(ctx, &o2); err != nil || !ins {
		t.Fatalf("second copy: ins=%v err=%v", ins, err)
	}
	if n := countWhere(t, s, "client_rx_observations", "pkt_hash = $1", o.PktHash); n != 2 {
		t.Errorf("rows for pkt_hash = %d, want 2", n)
	}
}

// NOTE: a TestTransportCodesV1MigrationFailsClosed regression test (forcing a
// genuine ALTER failure to prove the migration doesn't record its guard row
// on error) was attempted and removed — see final-fix-report.md for why the
// failure mode isn't reachable from a black-box test of applySchema without
// contorting production code. The fix itself (db.go, transport_codes_v1
// block) still stands, matching the observers_clock_naive_v1 pattern.

// TestPruneClientRfSamplesUsesIndex verifies the RF-sample reaper's
// DELETE ... WHERE sampled_at < ? seeks idx_crf_prune rather than
// full-scanning under the writer lock, mirroring
// TestPruneClientRxObservationsUsesIndex for the sibling table. May pass
// immediately since idx_crf_prune already exists (Task 4) — in that case
// this is a regression guard.
func TestPruneDeletesCanUseTheirIndex(t *testing.T) {
	cases := []struct{ table, col, index string }{
		{"client_receptions", "rx_at", "idx_client_recept_rxat"},
		{"client_rx_observations", "rx_at", "idx_cro_prune"},
		{"client_rf_samples", "sampled_at", "idx_crf_prune"}, // your existing name
		{"node_declared_regions", "observed_at", "idx_ndr_prune"},
	}
	s := newTestStore(t)
	for _, c := range cases {
		t.Run(c.table, func(t *testing.T) {
			plan := explainPlan(t, s, fmt.Sprintf(
				`DELETE FROM %s WHERE %s < '2026-01-01T00:00:00Z'::timestamptz`, c.table, c.col))
			if !strings.Contains(plan, c.index) {
				t.Fatalf("prune DELETE should be able to use %s, plan was:\n%s", c.index, plan)
			}
		})
	}
}

// Raw SQL for test assertions and seeding. Production queries go through
// sqlc and internal/database; these helpers exist so tests can look at the
// tables directly without adding methods to the production query set.

var testIdent = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// testCtx returns a per-test context that also fails a hung query instead of
// stalling the whole suite. Use context.Background() inside t.Cleanup funcs,
// because t.Context() is cancelled before cleanups run.
func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// countRows returns COUNT(*) for a table.
func countRows(t *testing.T, s *Store, table string) int {
	t.Helper()
	return countWhere(t, s, table, "")
}

// countWhere returns COUNT(*) for a table with an optional WHERE clause.
// table and where must be test-authored literals; values go through $N args.
func countWhere(t *testing.T, s *Store, table, where string, args ...any) int {
	t.Helper()
	if !testIdent.MatchString(table) {
		t.Fatalf("countWhere: bad table name %q", table)
	}
	q := "SELECT COUNT(*) FROM " + table
	if where != "" {
		q += " WHERE " + where
	}
	var n int
	mustScan(t, testRow(t, s, q, args...), &n)
	return n
}

// testRow runs a query expected to return one row. Pair it with mustScan.
func testRow(t *testing.T, s *Store, query string, args ...any) pgx.Row {
	t.Helper()
	return s.db.Pool().QueryRow(testCtx(t), query, args...)
}

// mustScan scans a row and fails the test on any error, including no rows.
// The old assertions ignored Scan errors, which turned real failures into
// zero values.
func mustScan(t *testing.T, row pgx.Row, dest ...any) {
	t.Helper()
	for i, d := range dest {
		if v := reflect.ValueOf(d); v.Kind() != reflect.Ptr || v.IsNil() {
			t.Fatalf("mustScan: dest[%d] is %T, want a non-nil pointer (missing &?)", i, d)
		}
	}
	if err := row.Scan(dest...); err != nil {
		t.Fatalf("scan: %v", err)
	}
}

// testExec runs a statement for seeding or cleanup and returns rows affected.
func testExec(t *testing.T, s *Store, query string, args ...any) int64 {
	t.Helper()
	tag, err := s.db.Pool().Exec(testCtx(t), query, args...)
	if err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
	return tag.RowsAffected()
}

// testCollect runs a multi-row query. For a single column use
// pgx.RowTo[string] (or the matching type) as fn.
func testCollect[T any](t *testing.T, s *Store, query string, fn pgx.RowToFunc[T], args ...any) []T {
	t.Helper()
	rows, err := s.db.Pool().Query(testCtx(t), query, args...)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	out, err := pgx.CollectRows(rows, fn)
	if err != nil {
		t.Fatalf("collect %q: %v", query, err)
	}
	return out
}

// explainPlan returns the plan for a statement with sequential scans disabled,
// so it shows whether an index CAN serve the query. EXPLAIN without ANALYZE
// executes nothing, and SET LOCAL is scoped to this rolled-back transaction.
func explainPlan(t *testing.T, s *Store, stmt string) string {
	t.Helper()
	ctx := testCtx(t)
	tx, err := s.db.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `SET LOCAL enable_seqscan = off`); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(ctx, "EXPLAIN "+stmt)
	if err != nil {
		t.Fatal(err)
	}
	lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return strings.Join(lines, "\n")
}

// tableColumns returns column name -> data_type for a table in the public schema.
func tableColumns(t *testing.T, s *Store, table string) map[string]string {
	t.Helper()
	rows := testCollect(t, s, `
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1`,
		func(row pgx.CollectableRow) ([2]string, error) {
			var kv [2]string
			err := row.Scan(&kv[0], &kv[1])
			return kv, err
		}, table)
	cols := make(map[string]string, len(rows))
	for _, kv := range rows {
		cols[kv[0]] = kv[1]
	}
	return cols
}

// recentTS returns an RFC3339 timestamp `ago` before now. Fixtures that go
// through resolveRxTimeCore need this: it replaces envelope times older than
// 30 days with ingest time, so a hardcoded date eventually stops testing
// what it was written to test.
func recentTS(ago time.Duration) string {
	return time.Now().UTC().Add(-ago).Format(time.RFC3339Nano)
}
