package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
)

// legacyRegionKeys runs the region subquery exactly as GetNodes ran it before
// CoreScope#214, on its own, as the membership oracle the new paths must match.
func legacyRegionKeys(t *testing.T, db *DB, codes []string) map[string]bool {
	t.Helper()
	joinCond := "obs.rowid = o.observer_idx"
	if !db.isV3 {
		joinCond = "obs.id = o.observer_id"
	}
	ph := strings.TrimSuffix(strings.Repeat("?,", len(codes)), ",")
	args := make([]interface{}, len(codes))
	for i, c := range codes {
		args[i] = c
	}
	rows, err := db.conn.Query(fmt.Sprintf(`SELECT public_key FROM nodes WHERE public_key IN (
		SELECT DISTINCT t.from_pubkey
		FROM transmissions t
		JOIN observations o ON o.transmission_id = t.id
		JOIN observers obs ON %s
		WHERE t.payload_type = 4
		AND UPPER(TRIM(obs.iata)) IN (%s)
	)`, joinCond, ph), args...)
	if err != nil {
		t.Fatalf("legacy region query: %v", err)
	}
	defer rows.Close()
	keys := map[string]bool{}
	for rows.Next() {
		var pk string
		if err := rows.Scan(&pk); err != nil {
			t.Fatal(err)
		}
		keys[pk] = true
	}
	return keys
}

// seedRegionExtras widens seedTestData so regions differ: SJC has one node,
// SFO two, DAY one, CMH one via a padded lower-case IATA, and an advert with
// no from_pubkey is heard in SJC.
func seedRegionExtras(t *testing.T, db *DB) {
	t.Helper()
	now := time.Now().UTC()
	recent := now.Add(-1 * time.Hour).Format(time.RFC3339)
	epoch := now.Add(-1 * time.Hour).Unix()
	for _, q := range []string{
		`INSERT INTO observers (id, name, iata, last_seen, first_seen, packet_count) VALUES ('obs3', 'Observer Three', 'DAY', '` + recent + `', '2026-01-01T00:00:00Z', 5)`,
		`INSERT INTO observers (id, name, iata, last_seen, first_seen, packet_count) VALUES ('obs4', 'Observer Four', ' cmh ', '` + recent + `', '2026-01-01T00:00:00Z', 5)`,
		`INSERT INTO transmissions (raw_hex, hash, first_seen, route_type, payload_type, decoded_json, from_pubkey) VALUES ('EE01', 'regionx0000000001', '` + recent + `', 1, 4, '{"pubKey":"eeff00112233aabb","type":"ADVERT"}', 'eeff00112233aabb')`,
		`INSERT INTO transmissions (raw_hex, hash, first_seen, route_type, payload_type, decoded_json, from_pubkey) VALUES ('EE02', 'regionx0000000002', '` + recent + `', 1, 4, '{"pubKey":"1122334455667788","type":"ADVERT"}', '1122334455667788')`,
		`INSERT INTO transmissions (raw_hex, hash, first_seen, route_type, payload_type, decoded_json) VALUES ('EE03', 'regionx0000000003', '` + recent + `', 1, 4, '{"type":"ADVERT"}')`,
	} {
		if _, err := db.conn.Exec(q); err != nil {
			t.Fatalf("seed: %v\n%s", err, q)
		}
	}
	// Transmissions 4, 5, 6 above; observers 3 (DAY) and 4 (cmh) by rowid.
	for _, o := range [][2]int{{4, 3}, {5, 2}, {5, 4}, {6, 1}} {
		if _, err := db.conn.Exec(`INSERT INTO observations (transmission_id, observer_idx, snr, rssi, path_json, timestamp) VALUES (?, ?, 1, -100, '[]', ?)`, o[0], o[1], epoch); err != nil {
			t.Fatalf("seed observation %v: %v", o, err)
		}
	}
}

func warmRegionIndex(t *testing.T, db *DB) {
	t.Helper()
	snap, err := db.buildRegionNodeSnapshot(context.Background())
	if err != nil {
		t.Fatalf("build snapshot: %v", err)
	}
	db.regionSnap.Store(snap)
}

func nodeKeys(nodes []map[string]interface{}) []string {
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, fmt.Sprint(n["public_key"]))
	}
	return out
}

// assertRegionEquivalent checks every filter combination GetNodes composes
// with a region: the region result must equal the unfiltered result (an
// unchanged code path) restricted to the legacy oracle's members, in the same
// order, with the same total, page by page.
func assertRegionEquivalent(t *testing.T, db *DB, mode string) {
	t.Helper()
	regions := []string{"SJC", "SFO", "DAY", "CMH", "SJC,SFO", "sjc", " SJC ", "SJC,SJC", "SFO,DAY,CMH", "AMS"}
	for _, region := range regions {
		member := legacyRegionKeys(t, db, normalizeRegionCodes(region))
		for _, role := range []string{"", "repeater", "companion", "room"} {
			for _, search := range []string{"", "Test", "Room"} {
				for _, sortBy := range []string{"", "name", "lastSeen", "packetCount"} {
					all, _, _, err := db.GetNodes(1000, 0, role, search, "", "", sortBy, "")
					if err != nil {
						t.Fatal(err)
					}
					var want []string
					for _, k := range nodeKeys(all) {
						if member[k] {
							want = append(want, k)
						}
					}
					got, total, _, err := db.GetNodes(1000, 0, role, search, "", "", sortBy, region)
					if err != nil {
						t.Fatalf("[%s] region=%q: %v", mode, region, err)
					}
					label := fmt.Sprintf("[%s] region=%q role=%q search=%q sort=%q", mode, region, role, search, sortBy)
					if total != len(want) {
						t.Errorf("%s: total %d, want %d", label, total, len(want))
					}
					if g := strings.Join(nodeKeys(got), ","); g != strings.Join(want, ",") {
						t.Errorf("%s: rows %s, want %s", label, g, strings.Join(want, ","))
					}
					for off := 0; off < len(want); off++ {
						page, ptotal, _, err := db.GetNodes(1, off, role, search, "", "", sortBy, region)
						if err != nil {
							t.Fatal(err)
						}
						if ptotal != len(want) || len(page) != 1 || nodeKeys(page)[0] != want[off] {
							t.Errorf("%s: page offset %d got %v (total %d), want %s (total %d)", label, off, nodeKeys(page), ptotal, want[off], len(want))
						}
					}
				}
			}
		}
	}
}

func TestGetNodesRegion_ColdMatchesLegacy(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	seedTestData(t, db)
	seedRegionExtras(t, db)
	assertRegionEquivalent(t, db, "cold")
}

func TestGetNodesRegion_WarmMatchesLegacy(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	seedTestData(t, db)
	seedRegionExtras(t, db)
	warmRegionIndex(t, db)
	assertRegionEquivalent(t, db, "warm")
}

// The defining property of the index: once warm, a region request is answered
// from the snapshot and never re-joins observations. Deleting every
// observation proves it, because the live join would now match nothing.
func TestGetNodesRegion_WarmReadsSnapshotNotDatabase(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	seedTestData(t, db)
	seedRegionExtras(t, db)
	warmRegionIndex(t, db)

	if _, err := db.conn.Exec(`DELETE FROM observations`); err != nil {
		t.Fatal(err)
	}
	_, total, _, err := db.GetNodes(50, 0, "", "", "", "", "", "SFO")
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 {
		t.Fatalf("warm SFO total %d, want 2 from the snapshot", total)
	}

	db.regionSnap.Store(nil)
	_, total, _, err = db.GetNodes(50, 0, "", "", "", "", "", "SFO")
	if err != nil {
		t.Fatal(err)
	}
	if total != 0 {
		t.Fatalf("cold SFO total %d after deleting observations, want 0 (live query)", total)
	}
}

func TestGetNodesRegion_NoUsableCodesAppliesNoFilter(t *testing.T) {
	for _, mode := range []string{"cold", "warm"} {
		db := setupTestDB(t)
		seedTestData(t, db)
		seedRegionExtras(t, db)
		if mode == "warm" {
			warmRegionIndex(t, db)
		}
		_, all, _, err := db.GetNodes(50, 0, "", "", "", "", "", "")
		if err != nil {
			t.Fatal(err)
		}
		for _, region := range []string{"ALL", "all", ",", " , ", "ALL,"} {
			_, total, _, err := db.GetNodes(50, 0, "", "", "", "", "", region)
			if err != nil {
				t.Fatal(err)
			}
			if total != all {
				t.Errorf("[%s] region=%q total %d, want %d (no filter)", mode, region, total, all)
			}
		}
		db.Close()
	}
}

func TestGetNodesRegion_LegacySchema(t *testing.T) {
	for _, mode := range []string{"cold", "warm"} {
		db := setupTestDBV2(t)
		now := time.Now().UTC()
		recent := now.Add(-1 * time.Hour).Format(time.RFC3339)
		db.conn.Exec(`INSERT INTO observers (id, name, iata, last_seen, first_seen, packet_count) VALUES ('obs-v2-1', 'V2 Observer', 'LAX', ?, '2026-01-01T00:00:00Z', 10)`, recent)
		db.conn.Exec(`INSERT INTO nodes (public_key, name, role, lat, lon, last_seen, first_seen, advert_count) VALUES ('v2pubkey11223344', 'V2Node', 'repeater', 34.0, -118.0, ?, '2026-01-01T00:00:00Z', 5)`, recent)
		db.conn.Exec(`INSERT INTO transmissions (raw_hex, hash, first_seen, route_type, payload_type, decoded_json) VALUES ('AABB', 'v2hash0001', ?, 1, 4, '{"pubKey":"v2pubkey11223344","name":"V2Node","type":"ADVERT"}')`, recent)
		db.conn.Exec(`INSERT INTO observations (transmission_id, observer_id, observer_name, snr, rssi, path_json, timestamp) VALUES (1, 'obs-v2-1', 'V2 Observer', 10.0, -90, '[]', ?)`, now.Unix())
		if mode == "warm" {
			warmRegionIndex(t, db)
		}
		for region, want := range map[string]int{"LAX": 1, "JFK": 0, "lax,JFK": 1} {
			_, total, _, err := db.GetNodes(50, 0, "", "", "", "", "", region)
			if err != nil {
				t.Fatal(err)
			}
			if total != want {
				t.Errorf("[%s] v2 region=%q total %d, want %d", mode, region, total, want)
			}
		}
		db.Close()
	}
}

// Before the first build, concurrent region requests must never run more than
// one fallback query at a time, whatever regions they ask for.
func TestRegionPubkeys_ColdFallbackUsesOneConnection(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	seedTestData(t, db)
	seedRegionExtras(t, db)

	var inFlight, maxInFlight int32
	db.regionColdHook = func() {
		n := atomic.AddInt32(&inFlight, 1)
		for {
			m := atomic.LoadInt32(&maxInFlight)
			if n <= m || atomic.CompareAndSwapInt32(&maxInFlight, m, n) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		atomic.AddInt32(&inFlight, -1)
	}

	var wg sync.WaitGroup
	for _, codes := range [][]string{{"SJC"}, {"SFO"}, {"DAY"}, {"CMH"}, {"SJC", "SFO"}, {"AMS"}} {
		wg.Add(1)
		go func(codes []string) {
			defer wg.Done()
			if _, err := db.regionPubkeys(codes); err != nil {
				t.Error(err)
			}
		}(codes)
	}
	wg.Wait()
	if maxInFlight != 1 {
		t.Fatalf("fallback queries in flight at once: %d, want 1", maxInFlight)
	}
}

// Concurrent requests for the same regions share one fallback query.
func TestRegionPubkeys_ColdFallbackSharedAcrossCallers(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	seedTestData(t, db)

	var runs int32
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	db.regionColdHook = func() {
		atomic.AddInt32(&runs, 1)
		once.Do(func() { close(entered) })
		<-release
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			keys, err := db.regionPubkeys([]string{"SJC"})
			if err != nil || len(keys) != 1 {
				t.Errorf("keys %v err %v", keys, err)
			}
		}()
	}
	<-entered
	time.Sleep(100 * time.Millisecond) // let the other callers join the in-flight query
	close(release)
	wg.Wait()
	if runs != 1 {
		t.Fatalf("fallback query ran %d times for 8 concurrent callers, want 1", runs)
	}
}

func TestRegionCodeSetKey_IgnoresOrderAndRepeats(t *testing.T) {
	a := regionCodeSetKey([]string{"SJC", "SFO"})
	for _, codes := range [][]string{{"SFO", "SJC"}, {"SJC", "SFO", "SJC"}, {"SFO", "SFO", "SJC"}} {
		if k := regionCodeSetKey(codes); k != a {
			t.Errorf("key(%v) = %q, want %q", codes, k, a)
		}
	}
	if regionCodeSetKey([]string{"SJC"}) == a {
		t.Error("different code sets share a key")
	}
	in := []string{"SJC", "SFO"}
	regionCodeSetKey(in)
	if in[0] != "SJC" || in[1] != "SFO" {
		t.Errorf("regionCodeSetKey reordered its input: %v", in)
	}
}

// The same region set in a different order coalesces onto one fallback query,
// and each caller gets its own copy of the shared result.
func TestRegionPubkeys_ColdFallbackSharedAcrossOrder(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	seedTestData(t, db)
	seedRegionExtras(t, db)

	var runs int32
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	db.regionColdHook = func() {
		atomic.AddInt32(&runs, 1)
		once.Do(func() { close(entered) })
		<-release
	}

	results := make([][]string, 6)
	var wg sync.WaitGroup
	for i := range results {
		codes := []string{"SJC", "SFO"}
		if i%2 == 1 {
			codes = []string{"SFO", "SJC", "SFO"}
		}
		wg.Add(1)
		go func(i int, codes []string) {
			defer wg.Done()
			keys, err := db.regionPubkeys(codes)
			if err != nil {
				t.Error(err)
			}
			results[i] = keys
		}(i, codes)
	}
	<-entered
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()
	if runs != 1 {
		t.Fatalf("fallback ran %d times for one region set in two orders, want 1", runs)
	}
	results[0][0] = "mutated"
	for i := 1; i < len(results); i++ {
		if results[i][0] == "mutated" {
			t.Fatal("callers that shared a fallback query share one slice")
		}
	}
}

// setupFileTestDB is setupTestDB on a file with a real multi-connection pool,
// the shape production runs with, so pool contention can actually occur.
func setupFileTestDB(t *testing.T, maxConns int) *DB {
	t.Helper()
	conn, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "region.db"))
	if err != nil {
		t.Fatal(err)
	}
	conn.SetMaxOpenConns(maxConns)
	if _, err := database.BaselineStampMigrate(conn, nil); err != nil {
		t.Fatalf("migrating test db: %v", err)
	}
	db := &DB{conn: conn}
	if err := db.detectSchema(context.Background(), conn); err != nil {
		t.Fatalf("detectSchema: %v", err)
	}
	return db
}

// The fallback holds regionColdMu while it waits for a connection. That is
// only a deadlock if something holding a connection also waits on the mutex,
// and GetNodes takes no connection until after regionPubkeys returns. Prove it
// on a contended pool: mixed region and non-region requests must all finish,
// and every region answer must still match the oracle.
func TestGetNodesRegion_ColdFallbackOnContendedPool(t *testing.T) {
	db := setupFileTestDB(t, 2)
	defer db.Close()
	seedTestData(t, db)
	seedRegionExtras(t, db)
	db.regionColdHook = func() { time.Sleep(20 * time.Millisecond) }

	regions := []string{"SJC", "SFO", "DAY", "CMH", "SFO,SJC", "AMS"}
	want := map[string]int{}
	for _, r := range regions {
		want[r] = len(legacyRegionKeys(t, db, normalizeRegionCodes(r)))
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 48; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				if _, _, _, err := db.GetNodes(50, 0, "", "", "", "", "", ""); err != nil {
					t.Error(err)
				}
				return
			}
			r := regions[i%len(regions)]
			_, total, _, err := db.GetNodes(50, 0, "", "", "", "", "", r)
			if err != nil {
				t.Error(err)
				return
			}
			if total != want[r] {
				t.Errorf("region=%q total %d, want %d", r, total, want[r])
			}
		}(i)
	}
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("requests did not all finish within 30s on a 2-connection pool")
	}
}

func TestRegionNodeIndex_FailedBuildKeepsSnapshot(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	seedTestData(t, db)

	good, err := db.buildRegionNodeSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	ix := db.newRegionNodeIndex(time.Hour)
	ix.build = func(ctx context.Context) (*regionNodeSnapshot, error) {
		calls++
		switch calls {
		case 1:
			return good, nil
		case 2:
			return nil, errors.New("simulated build failure")
		default:
			panic("simulated build panic")
		}
	}
	ix.runOnce()
	ix.runOnce()
	ix.runOnce()
	if db.regionSnap.Load() != good {
		t.Fatal("a failed or panicking build replaced the previous snapshot")
	}
}

func TestRegionNodeIndex_StartStop(t *testing.T) {
	t.Run("stop before start returns and start is then a no-op", func(t *testing.T) {
		db := setupTestDB(t)
		defer db.Close()
		ix := db.newRegionNodeIndex(time.Hour)
		ix.Stop()
		ix.Start()
		ix.Stop()
		if db.regionSnap.Load() != nil {
			t.Fatal("a stopped index built a snapshot")
		}
	})

	t.Run("start builds immediately", func(t *testing.T) {
		db := setupTestDB(t)
		defer db.Close()
		seedTestData(t, db)
		ix := db.newRegionNodeIndex(time.Hour)
		ix.Start()
		defer ix.Stop()
		deadline := time.Now().Add(5 * time.Second)
		for db.regionSnap.Load() == nil {
			if time.Now().After(deadline) {
				t.Fatal("no snapshot within 5s of Start")
			}
			time.Sleep(10 * time.Millisecond)
		}
	})

	t.Run("stop cancels an in-flight build", func(t *testing.T) {
		db := setupTestDB(t)
		defer db.Close()
		ix := db.newRegionNodeIndex(time.Hour)
		entered := make(chan struct{})
		ix.build = func(ctx context.Context) (*regionNodeSnapshot, error) {
			close(entered)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		ix.Start()
		<-entered
		stopped := make(chan struct{})
		go func() { ix.Stop(); close(stopped) }()
		select {
		case <-stopped:
		case <-time.After(2 * time.Second):
			t.Fatal("Stop did not cancel the in-flight build within 2s")
		}
	})
}
