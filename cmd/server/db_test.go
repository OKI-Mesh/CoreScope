package main

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
	"github.com/OKI-Mesh/CoreScope/internal/database/pgtest"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

func TestMain(m *testing.M) { os.Exit(pgtest.Run(m)) }

// setupTestDB returns a read-write pool on a fresh, fully migrated clone.
func setupTestDB(tb testing.TB) *database.DB {
	tb.Helper()
	db, err := database.OpenReadWrite(tb.Context(), pgtest.NewDatabase(tb))
	if err != nil {
		tb.Fatalf("open test db: %v", err)
	}
	tb.Cleanup(func() { db.Close() }) // runs before the clone is dropped
	return db
}

func seedTestData(t *testing.T, db *database.DB) {
	t.Helper()
	ctx := t.Context()
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := db.SQL().ExecContext(ctx, query, args...); err != nil {
			t.Fatalf("seed: %v\nSQL: %s", err, query)
		}
	}

	now := time.Now().UTC()
	recent := now.Add(-1 * time.Hour)
	yesterday := now.Add(-24 * time.Hour)
	twoDaysAgo := now.Add(-48 * time.Hour)
	jan1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	obs1 := dbtest.Observer(t, db, "obs1", "Observer One", "SJC", jan1, recent)
	obs2 := dbtest.Observer(t, db, "obs2", "Observer Two", "SFO", jan1, yesterday)

	dbtest.Node(t, db, "eeff00112233aabb", "TestCompanion", "companion")
	dbtest.Node(t, db, "1122334455667788", "TestRoom", "room")

	const repeaterAdvert1 = `{"pubKey":"aabbccdd11223344","name":"TestRepeater","type":"ADVERT","timestamp":1700000000,"timestampISO":"2023-11-14T22:13:20.000Z","signature":"abcdef","flags":{"isRepeater":true},"lat":37.5,"lon":-122.0}`
	const repeaterAdvert2 = `{"pubKey":"aabbccdd11223344","name":"TestRepeater","type":"ADVERT","timestamp":1700000100,"timestampISO":"2023-11-14T22:14:40.000Z","signature":"fedcba","flags":{"isRepeater":true},"lat":37.5,"lon":-122.0}`

	tx1, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		RawHex:      "AABB",
		Hash:        "abc123def4567890",
		RouteType:   types.RouteType(1),
		PayloadType: types.PayloadType(4),
		DecodedJSON: repeaterAdvert1,
		ChannelHash: "#test",
		Advert:      &database.NodeInput{PublicKey: "aabbccdd11223344", Name: "TestRepeater", Role: "repeater", RxTime: recent, IngestNow: now},
	}, recent)
	if err != nil {
		t.Fatalf("seed tx1: %v", err)
	}
	dbtest.Observation(t, db, tx1, obs1, recent, 12.5, -90, "aa:aabbccdd11223344", "bb:eeff00112233aabb")
	dbtest.Observation(t, db, tx1, obs2, recent.Add(-100*time.Second), 8.0, -95, "aa:aabbccdd11223344")

	dbtest.TxWithObservation(t, db, obs1, database.TransmissionInput{
		RawHex:      "CCDD",
		Hash:        "1234567890abcdef",
		RouteType:   types.RouteType(1),
		PayloadType: types.PayloadType(5),
		DecodedJSON: `{"type":"CHAN","channel":"#test","text":"Hello: World","sender":"TestUser"}`,
		ChannelHash: "#test",
	}, yesterday, 15.0, -85)

	tx3, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		RawHex:      "AA1F",
		Hash:        "def456abc1230099",
		RouteType:   types.RouteType(1),
		PayloadType: types.PayloadType(4),
		DecodedJSON: repeaterAdvert2,
		Advert:      &database.NodeInput{PublicKey: "aabbccdd11223344", Name: "TestRepeater", Role: "repeater", RxTime: yesterday, IngestNow: now},
	}, yesterday)
	if err != nil {
		t.Fatalf("seed tx3: %v", err)
	}
	dbtest.Observation(t, db, tx3, obs1, yesterday, 10.0, -92, "cc:1122334455667788")

	exec(`UPDATE observers SET packet_count = 100 WHERE pubkey = 'obs1'`)
	exec(`UPDATE observers SET packet_count = 50 WHERE pubkey = 'obs2'`)
	exec(`UPDATE nodes SET lat = 37.5, lon = -122.0, advert_count = 50, first_seen = $1, last_seen = $2 WHERE public_key = 'aabbccdd11223344'`, jan1, recent)
	exec(`UPDATE nodes SET lat = 37.6, lon = -122.1, advert_count = 10, first_seen = $1, last_seen = $2 WHERE public_key = 'eeff00112233aabb'`, jan1, yesterday)
	exec(`UPDATE nodes SET lat = 37.4, lon = -121.9, advert_count = 5, first_seen = $1, last_seen = $2 WHERE public_key = '1122334455667788'`, jan1, twoDaysAgo)
}

func TestGetStats(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	stats, err := db.GetStats(ctx, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}

	if stats.TotalTransmissions != 3 {
		t.Errorf("expected 3 transmissions, got %d", stats.TotalTransmissions)
	}
	if stats.TotalNodes != 3 {
		t.Errorf("expected 3 nodes, got %d", stats.TotalNodes)
	}
	if stats.TotalObservers != 2 {
		t.Errorf("expected 2 observers, got %d", stats.TotalObservers)
	}
	if stats.TotalObservations != 4 {
		t.Errorf("expected 4 observations, got %d", stats.TotalObservations)
	}
}

func TestGetRoleCounts(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	counts, err := db.GetRoleCounts(ctx, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if counts.Repeaters != 1 {
		t.Errorf("expected 1 repeater, got %d", counts.Repeaters)
	}
	if counts.Companions != 1 {
		t.Errorf("expected 1 companion, got %d", counts.Companions)
	}
	if counts.Rooms != 1 {
		t.Errorf("expected 1 room, got %d", counts.Rooms)
	}
}

func TestGetDBSizeStats(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	stats, err := db.GetDBSizeStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.DBSizeBytes <= 0 {
		t.Errorf("expected positive database size, got %d", stats.DBSizeBytes)
	}
	if stats.Rows.Transmissions != 3 {
		t.Errorf("expected 3 transmissions rows, got %d", stats.Rows.Transmissions)
	}
	if stats.Rows.Observations != 4 {
		t.Errorf("expected 4 observations rows, got %d", stats.Rows.Observations)
	}
	if stats.Rows.Nodes != 3 {
		t.Errorf("expected 3 nodes rows, got %d", stats.Rows.Nodes)
	}
	if stats.Rows.Observers != 2 {
		t.Errorf("expected 2 observers rows, got %d", stats.Rows.Observers)
	}
	if stats.DeadTuples < 0 {
		t.Errorf("expected non-negative dead tuple count, got %d", stats.DeadTuples)
	}
}

func TestQueryPackets(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	packets, count, err := db.QueryPackets(ctx, database.PacketQuery{Limit: 50, Order: "DESC"})
	if err != nil {
		t.Fatal(err)
	}
	// Transmission-centric: 3 unique transmissions (not 4 observations)
	if count != 3 {
		t.Errorf("expected 3 total transmissions, got %d", count)
	}
	if len(packets) != 3 {
		t.Errorf("expected 3 packets, got %d", len(packets))
	}
	// Verify transmission shape has required fields
	if len(packets) > 0 {
		p := packets[0]
		if p.FirstSeen.IsZero() {
			t.Error("expected first_seen in packet")
		}
		if p.ObservationCount < 1 {
			t.Error("expected observation_count in packet")
		}
		if p.ObsTimestamp == nil {
			t.Error("expected representative observation timestamp in packet")
		}
	}
}

func TestQueryPacketsWithTypeFilter(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	pt := 4
	_, count, err := db.QueryPackets(ctx, database.PacketQuery{Limit: 50, Type: &pt, Order: "DESC"})
	if err != nil {
		t.Fatal(err)
	}
	// 2 transmissions with payload_type=4 (ADVERT)
	if count != 2 {
		t.Errorf("expected 2 ADVERT transmissions, got %d", count)
	}
}

func TestQueryGroupedPackets(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	_, count, err := db.QueryGroupedPackets(ctx, database.PacketQuery{Limit: 50})
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("expected 3 grouped packets (unique hashes), got %d", count)
	}
}

func TestGetNodeByPubkey(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	node, err := db.GetNodeByPubkey(ctx, "aabbccdd11223344")
	if err != nil {
		t.Fatal(err)
	}
	if node == nil {
		t.Fatal("expected node, got nil")
	}
	if node.Name == nil || *node.Name != "TestRepeater" {
		t.Errorf("expected TestRepeater, got %v", node.Name)
	}
}

func TestGetNodeByPubkeyNotFound(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	node, _ := db.GetNodeByPubkey(ctx, "nonexistent")
	if node != nil {
		t.Error("expected nil for nonexistent node")
	}
}

func TestSearchNodes(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	nodes, err := db.SearchNodes(ctx, "Test", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 3 {
		t.Errorf("expected 3 nodes matching 'Test', got %d", len(nodes))
	}
}

func TestGetObservers(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	observers, err := db.GetObservers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(observers) != 2 {
		t.Errorf("expected 2 observers, got %d", len(observers))
	}
	if observers[0].Pubkey != "obs1" {
		t.Errorf("expected obs1 first (most recent), got %s", observers[0].Pubkey)
	}
	// last_packet_at should be nil since seedTestData doesn't set it
	if observers[0].LastPacketAt != nil {
		t.Errorf("expected nil LastPacketAt for obs1 from seed, got %v", *observers[0].LastPacketAt)
	}
}

// Regression: GetObservers must exclude soft-deleted (inactive=1) rows.
// Stale observers were appearing in /api/observers despite the auto-prune
// marking them inactive, because the SELECT query had no WHERE filter.
func TestGetObservers_ExcludesInactive(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)
	// Mark obs2 inactive — soft delete simulating a stale-observer prune.
	if _, err := db.SQL().ExecContext(ctx, `UPDATE observers SET inactive = TRUE WHERE pubkey = $1`, "obs2"); err != nil {
		t.Fatalf("update inactive: %v", err)
	}
	observers, err := db.GetObservers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(observers) != 1 {
		t.Errorf("expected 1 observer (obs1) after marking obs2 inactive, got %d", len(observers))
	}
	for _, o := range observers {
		if o.Pubkey == "obs2" {
			t.Errorf("inactive observer obs2 should be excluded")
		}
	}
}

func TestGetObserverByID(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	obs, err := db.GetObserverByID(ctx, "obs1")
	if err != nil {
		t.Fatal(err)
	}
	if obs.Pubkey != "obs1" {
		t.Errorf("expected obs1, got %s", obs.Pubkey)
	}
	// Verify last_packet_at is nil by default
	if obs.LastPacketAt != nil {
		t.Errorf("expected nil LastPacketAt, got %v", *obs.LastPacketAt)
	}
}

func TestGetObserverLastPacketAt(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	// Set last_packet_at for obs1
	ts := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	if _, err := db.SQL().ExecContext(ctx, `UPDATE observers SET last_packet_at = $1 WHERE pubkey = $2`, ts, "obs1"); err != nil {
		t.Fatal(err)
	}

	// Verify via GetObservers
	observers, err := db.GetObservers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var obs1 *database.Observer
	for i := range observers {
		if observers[i].Pubkey == "obs1" {
			obs1 = &observers[i]
			break
		}
	}
	if obs1 == nil {
		t.Fatal("obs1 not found")
	}
	if obs1.LastPacketAt == nil || !obs1.LastPacketAt.Equal(ts) {
		t.Errorf("expected LastPacketAt=%s via GetObservers, got %v", ts, obs1.LastPacketAt)
	}

	// Verify via GetObserverByID
	obs, err := db.GetObserverByID(ctx, "obs1")
	if err != nil {
		t.Fatal(err)
	}
	if obs.LastPacketAt == nil || !obs.LastPacketAt.Equal(ts) {
		t.Errorf("expected LastPacketAt=%s via GetObserverByID, got %v", ts, obs.LastPacketAt)
	}
}

func TestGetObserverByIDNotFound(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	_, err := db.GetObserverByID(ctx, "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent observer")
	}
}

func TestObserverTypeConsistency(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	// Insert observer with typed metadata matching ingestor writes
	_, err := db.SQL().ExecContext(ctx, `INSERT INTO observers (pubkey, name, iata, last_seen, first_seen, packet_count, battery_mv, uptime_secs, noise_floor)
		VALUES ($1, $2, $3, $4, $4, $5, $6, $7, $8)`,
		"obs_typed", "TypedObs", "SJC", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), 10, 3500, 86400, -115.5)
	if err != nil {
		t.Fatal(err)
	}

	obs, err := db.GetObserverByID(ctx, "obs_typed")
	if err != nil {
		t.Fatal(err)
	}

	// battery_mv should be *int
	if obs.BatteryMv == nil {
		t.Fatal("BatteryMv should not be nil")
	}
	if *obs.BatteryMv != 3500 {
		t.Errorf("BatteryMv=%d, want 3500", *obs.BatteryMv)
	}

	// uptime_secs should be *int64
	if obs.UptimeSecs == nil {
		t.Fatal("UptimeSecs should not be nil")
	}
	if *obs.UptimeSecs != 86400 {
		t.Errorf("UptimeSecs=%d, want 86400", *obs.UptimeSecs)
	}

	// noise_floor should be *float64
	if obs.NoiseFloor == nil {
		t.Fatal("NoiseFloor should not be nil")
	}
	if *obs.NoiseFloor != -115.5 {
		t.Errorf("NoiseFloor=%f, want -115.5", *obs.NoiseFloor)
	}

	// Verify NULL handling: observer without metadata
	_, err = db.SQL().ExecContext(ctx, `INSERT INTO observers (pubkey, name, iata, last_seen, first_seen, packet_count)
		VALUES ($1, $2, $3, $4, $4, $5)`,
		"obs_null", "NullObs", "SFO", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), 5)
	if err != nil {
		t.Fatal(err)
	}

	obsNull, err := db.GetObserverByID(ctx, "obs_null")
	if err != nil {
		t.Fatal(err)
	}
	if obsNull.BatteryMv != nil {
		t.Errorf("BatteryMv should be nil for observer without metadata, got %d", *obsNull.BatteryMv)
	}
	if obsNull.UptimeSecs != nil {
		t.Errorf("UptimeSecs should be nil for observer without metadata, got %d", *obsNull.UptimeSecs)
	}
	if obsNull.NoiseFloor != nil {
		t.Errorf("NoiseFloor should be nil for observer without metadata, got %f", *obsNull.NoiseFloor)
	}
}

func TestObserverTypesInGetObservers(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	_, err := db.SQL().ExecContext(ctx, `INSERT INTO observers (pubkey, name, iata, last_seen, first_seen, packet_count, battery_mv, uptime_secs, noise_floor)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		"obs1", "Obs1", "SJC", time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), 10, 4200, 172800, -110.3)
	if err != nil {
		t.Fatal(err)
	}

	observers, err := db.GetObservers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(observers) != 1 {
		t.Fatalf("expected 1 observer, got %d", len(observers))
	}
	o := observers[0]
	if o.BatteryMv == nil || *o.BatteryMv != 4200 {
		t.Errorf("BatteryMv=%v, want 4200", o.BatteryMv)
	}
	if o.UptimeSecs == nil || *o.UptimeSecs != 172800 {
		t.Errorf("UptimeSecs=%v, want 172800", o.UptimeSecs)
	}
	if o.NoiseFloor == nil || *o.NoiseFloor != -110.3 {
		t.Errorf("NoiseFloor=%v, want -110.3", o.NoiseFloor)
	}
}

func TestGetDistinctIATAs(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	codes, err := db.GetDistinctIATAs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(codes) != 2 {
		t.Errorf("expected 2 IATA codes, got %d", len(codes))
	}
}

func TestGetPacketByHash(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	pkt, err := db.GetPacketByHash(ctx, "abc123def4567890")
	if err != nil {
		t.Fatal(err)
	}
	if pkt == nil {
		t.Fatal("expected packet, got nil")
	}
	if pkt.Hash != "abc123def4567890" {
		t.Errorf("expected hash abc123def4567890, got %v", pkt.Hash)
	}
}

func TestGetTraces(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	traces, err := db.GetObservationsForHash(ctx, "abc123def4567890")
	if err != nil {
		t.Fatal(err)
	}
	if len(traces) != 2 {
		t.Errorf("expected 2 traces, got %d", len(traces))
	}
}

func TestGetChannels(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	channels, err := db.ListChannelSummaries(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(channels) != 1 {
		t.Errorf("expected 1 channel, got %d", len(channels))
	}
	if channels[0].ChannelHash != "#test" {
		t.Errorf("expected #test channel, got %v", channels[0].ChannelHash)
	}
}

func TestGetMaxTransmissionID(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	maxID, err := db.GetMaxTransmissionID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if maxID != 3 {
		t.Errorf("expected max ID 3, got %d", maxID)
	}
}

func TestGetTransmissionByIDFound(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	tx, err := db.GetPacketByID(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if tx == nil {
		t.Fatal("expected packet, got nil")
	}
	if tx.Hash != "abc123def4567890" {
		t.Errorf("expected hash abc123def4567890, got %v", tx.Hash)
	}
	if tx.RawHex != "AABB" {
		t.Errorf("expected raw_hex AABB, got %v", tx.RawHex)
	}
}

func TestGetPacketByIDNotFound(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	result, err := db.GetPacketByID(ctx, 9999)
	if err == nil || result != nil {
		t.Error("expected not-found error and nil result for nonexistent packet")
	}
}

func TestGetPacketByHashNotFound(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	result, err := db.GetPacketByHash(ctx, "nonexistenthash1")
	if err == nil || result != nil {
		t.Error("expected not-found error and nil result for nonexistent hash")
	}
}

func TestGetChannelMessages(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	t.Run("matching channel", func(t *testing.T) {
		messages, err := db.ListChannelMessages(ctx, "#test", false, nil, 100, 0)
		if err != nil {
			t.Fatal(err)
		}
		total, err := db.CountChannelMessages(ctx, "#test", false, nil)
		if err != nil {
			t.Fatal(err)
		}
		if total == 0 {
			t.Error("expected at least 1 message for #test")
		}
		if len(messages) == 0 {
			t.Error("expected non-empty messages")
		}
	})

	t.Run("non-matching channel", func(t *testing.T) {
		messages, err := db.ListChannelMessages(ctx, "#nonexistent", false, nil, 100, 0)
		if err != nil {
			t.Fatal(err)
		}
		total, err := db.CountChannelMessages(ctx, "#nonexistent", false, nil)
		if err != nil {
			t.Fatal(err)
		}
		if total != 0 {
			t.Errorf("expected 0 messages, got %d", total)
		}
		if len(messages) != 0 {
			t.Errorf("expected empty messages, got %d", len(messages))
		}
	})

	t.Run("default limit", func(t *testing.T) {
		messages, err := db.ListChannelMessages(ctx, "#test", false, nil, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if messages == nil {
			t.Error("expected non-nil result")
		}
	})
}

func TestGetChannelMessagesRegionFiltering(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)
	msgsSJC, err := db.ListChannelMessages(ctx, "#test", false, []string{"SJC"}, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	totalSJC, err := db.CountChannelMessages(ctx, "#test", false, []string{"SJC"})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgsSJC) == 0 || totalSJC == 0 {
		t.Fatal("expected channel messages received in SJC")
	}
	msgsUnknown, err := db.ListChannelMessages(ctx, "#test", false, []string{"ZZZ"}, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	totalUnknown, err := db.CountChannelMessages(ctx, "#test", false, []string{"ZZZ"})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgsUnknown) != 0 || totalUnknown != 0 {
		t.Fatalf("expected no messages for unknown region, count=%d len=%d", totalUnknown, len(msgsUnknown))
	}
}

func TestResolveNodePubkey(t *testing.T) {
	db := setupTestDB(t)
	seedTestData(t, db)

	t.Run("by pubkey", func(t *testing.T) {
		pk, err := db.ResolveNodePubkey(t.Context(), "aabbccdd11223344")
		if err != nil {
			t.Fatal(err)
		}
		if pk != "aabbccdd11223344" {
			t.Errorf("expected aabbccdd11223344, got %s", pk)
		}
	})

	t.Run("by name", func(t *testing.T) {
		pk, err := db.ResolveNodePubkey(t.Context(), "TestRepeater")
		if err != nil {
			t.Fatal(err)
		}
		if pk != "aabbccdd11223344" {
			t.Errorf("expected aabbccdd11223344, got %s", pk)
		}
	})

	t.Run("not found returns input", func(t *testing.T) {
		pk, err := db.ResolveNodePubkey(t.Context(), "nonexistent")
		if err != nil {
			t.Fatal(err)
		}
		if pk != "nonexistent" {
			t.Errorf("expected 'nonexistent' back, got %s", pk)
		}
	})
}

func TestGetNodesFiltering(t *testing.T) {
	db := setupTestDB(t)
	seedTestData(t, db)

	t.Run("role filter", func(t *testing.T) {
		nodes, total, err := db.ListNodes(t.Context(), database.NodeListParams{Limit: 50, Role: "repeater"})
		if err != nil {
			t.Fatal(err)
		}
		if total != 1 {
			t.Errorf("expected 1 repeater, got %d", total)
		}
		if len(nodes) != 1 {
			t.Errorf("expected 1 node, got %d", len(nodes))
		}
	})

	t.Run("search filter", func(t *testing.T) {
		nodes, _, err := db.ListNodes(t.Context(), database.NodeListParams{Limit: 50, Search: "Companion"})
		if err != nil {
			t.Fatal(err)
		}
		if len(nodes) != 1 {
			t.Errorf("expected 1 companion, got %d", len(nodes))
		}
	})

	t.Run("sort by name", func(t *testing.T) {
		nodes, _, err := db.ListNodes(t.Context(), database.NodeListParams{Limit: 50, Sort: database.NodeSortName})
		if err != nil {
			t.Fatal(err)
		}
		if len(nodes) == 0 {
			t.Error("expected nodes")
		}
	})

	t.Run("sort by packetCount", func(t *testing.T) {
		nodes, _, err := db.ListNodes(t.Context(), database.NodeListParams{Limit: 50, Sort: database.NodeSortPacketCount})
		if err != nil {
			t.Fatal(err)
		}
		if len(nodes) == 0 {
			t.Error("expected nodes")
		}
	})

	t.Run("sort by lastSeen", func(t *testing.T) {
		nodes, _, err := db.ListNodes(t.Context(), database.NodeListParams{Limit: 50, Sort: database.NodeSortLastSeen})
		if err != nil {
			t.Fatal(err)
		}
		if len(nodes) == 0 {
			t.Error("expected nodes")
		}
	})

	t.Run("lastHeard filter 30d", func(t *testing.T) {
		since := time.Now().UTC().Add(-30 * 24 * time.Hour)
		_, _, err := db.ListNodes(t.Context(), database.NodeListParams{Limit: 50, HeardSince: &since})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("lastHeard filter various", func(t *testing.T) {
		since := time.Now().UTC().Add(-24 * time.Hour)
		if _, _, err := db.ListNodes(t.Context(), database.NodeListParams{Limit: 50, HeardSince: &since}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("default limit", func(t *testing.T) {
		nodes, _, err := db.ListNodes(t.Context(), database.NodeListParams{})
		if err != nil {
			t.Fatal(err)
		}
		if len(nodes) == 0 {
			t.Error("expected nodes with default limit")
		}
	})

	t.Run("before filter", func(t *testing.T) {
		before := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
		_, total, err := db.ListNodes(t.Context(), database.NodeListParams{Limit: 50, Before: &before})
		if err != nil {
			t.Fatal(err)
		}
		if total != 3 {
			t.Errorf("expected 3 nodes with first_seen <= 2026-01-02, got %d", total)
		}
	})

	t.Run("offset", func(t *testing.T) {
		nodes, total, err := db.ListNodes(t.Context(), database.NodeListParams{Limit: 1, Offset: 1})
		if err != nil {
			t.Fatal(err)
		}
		if total != 3 {
			t.Errorf("expected 3 total, got %d", total)
		}
		if len(nodes) != 1 {
			t.Errorf("expected 1 node with offset, got %d", len(nodes))
		}
	})

	t.Run("region filter SJC", func(t *testing.T) {
		nodes, total, err := db.ListNodes(t.Context(), database.NodeListParams{Limit: 50, Regions: []string{"SJC"}})
		if err != nil {
			t.Fatal(err)
		}
		if total != 1 {
			t.Errorf("expected 1 node for SJC region, got %d", total)
		}
		if len(nodes) != 1 {
			t.Fatalf("expected 1 node, got %d", len(nodes))
		}
		if nodes[0].PublicKey != "aabbccdd11223344" {
			t.Errorf("expected TestRepeater, got %v", nodes[0].PublicKey)
		}
	})

	t.Run("region filter SFO", func(t *testing.T) {
		_, total, err := db.ListNodes(t.Context(), database.NodeListParams{Limit: 50, Regions: []string{"SFO"}})
		if err != nil {
			t.Fatal(err)
		}
		if total != 1 {
			t.Errorf("expected 1 node for SFO region, got %d", total)
		}
	})

	t.Run("region filter multi", func(t *testing.T) {
		_, total, err := db.ListNodes(t.Context(), database.NodeListParams{Limit: 50, Regions: []string{"SJC", "SFO"}})
		if err != nil {
			t.Fatal(err)
		}
		if total != 1 {
			t.Errorf("expected 1 node for SJC,SFO region, got %d", total)
		}
	})

	t.Run("region filter unknown", func(t *testing.T) {
		_, total, err := db.ListNodes(t.Context(), database.NodeListParams{Limit: 50, Regions: []string{"AMS"}})
		if err != nil {
			t.Fatal(err)
		}
		if total != 0 {
			t.Errorf("expected 0 nodes for unknown region, got %d", total)
		}
	})
}

func TestOpenDBValid(t *testing.T) {
	ctx := t.Context()
	dsn := pgtest.NewDatabase(t)
	db, err := OpenDB(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer db.Close()

	maxID, err := db.GetMaxTransmissionID(ctx)
	if err != nil {
		t.Fatalf("GetMaxTransmissionID: %v", err)
	}
	if maxID != 0 {
		t.Errorf("expected 0, got %d", maxID)
	}
}

func TestOpenDBInvalidPath(t *testing.T) {
	ctx := t.Context()
	_, err := OpenDB(ctx, "not a postgres connection string")
	if err == nil {
		t.Error("expected error for invalid path")
	}
}

func TestQueryGroupedPacketsWithFilters(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	rt := 1
	_, total, err := db.QueryGroupedPackets(ctx, database.PacketQuery{Limit: 50, Route: &rt})
	if err != nil {
		t.Fatal(err)
	}
	if total == 0 {
		t.Error("expected results for grouped with route filter")
	}
}

func TestNodeTelemetryFields(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	mustCreateTestNode(t, ctx, db, database.NodeInput{
		PublicKey: "pk_telem1", Name: "SensorNode", Role: "sensor", RxTime: now, IngestNow: now,
	})
	battery, temperature := 3700, 28.5
	if err := db.UpdateBatteryVoltageAndTempC(ctx, "pk_telem1", &battery, &temperature); err != nil {
		t.Fatalf("UpdateBatteryVoltageAndTempC(): %v", err)
	}

	// Test via GetNodeByPubkey
	node, err := db.GetNodeByPubkey(ctx, "pk_telem1")
	if err != nil {
		t.Fatal(err)
	}
	if node == nil {
		t.Fatal("expected node, got nil")
	}
	if node.BatteryMv == nil || *node.BatteryMv != battery {
		t.Errorf("battery_mv=%v, want %d", node.BatteryMv, battery)
	}
	if node.TemperatureC == nil || *node.TemperatureC != temperature {
		t.Errorf("temperature_c=%v, want %v", node.TemperatureC, temperature)
	}

	// Test via the typed node list API.
	nodes, _, err := db.ListNodes(ctx, database.NodeListParams{Limit: 50, Role: "sensor"})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 sensor node, got %d", len(nodes))
	}
	if nodes[0].BatteryMv == nil || *nodes[0].BatteryMv != battery {
		t.Errorf("ListNodes battery_mv=%v, want %d", nodes[0].BatteryMv, battery)
	}

	// Test node without telemetry — fields should be nil
	mustCreateTestNode(t, ctx, db, database.NodeInput{
		PublicKey: "pk_notelem", Name: "PlainNode", Role: "repeater", RxTime: now, IngestNow: now,
	})
	node2, err := db.GetNodeByPubkey(ctx, "pk_notelem")
	if err != nil {
		t.Fatalf("GetNodeByPubkey(pk_notelem): %v", err)
	}
	if node2.BatteryMv != nil {
		t.Errorf("expected nil battery_mv for node without telemetry, got %v", node2.BatteryMv)
	}
	if node2.TemperatureC != nil {
		t.Errorf("expected nil temperature_c for node without telemetry, got %v", node2.TemperatureC)
	}
}

func TestGetObserverMetrics(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	now := time.Now().UTC()
	nf1, nf2, nf3, nf4 := -112.5, -110.0, -108.0, -115.0
	tx1, tx2, tx3 := 100, 200, 300
	rx1, rx2, rx3 := 500, 800, 1100
	errors1, errors2, errors3, battery := 3, 5, 8, 3720
	mustCreateTestObserverMetrics(t, ctx, db, database.ObserverMetricsInput{
		ObserverID: "obs1", Timestamp: now.Add(-2 * time.Hour), NoiseFloor: &nf1,
		TxAirSecs: &tx1, RxAirSecs: &rx1, RecvErrors: &errors1, BatteryMv: &battery,
	})
	mustCreateTestObserverMetrics(t, ctx, db, database.ObserverMetricsInput{
		ObserverID: "obs1", Timestamp: now.Add(-time.Hour), NoiseFloor: &nf2,
		TxAirSecs: &tx2, RxAirSecs: &rx2, RecvErrors: &errors2,
	})
	mustCreateTestObserverMetrics(t, ctx, db, database.ObserverMetricsInput{
		ObserverID: "obs1", Timestamp: now, NoiseFloor: &nf3,
		TxAirSecs: &tx3, RxAirSecs: &rx3, RecvErrors: &errors3,
	})
	mustCreateTestObserverMetrics(t, ctx, db, database.ObserverMetricsInput{
		ObserverID: "obs2", Timestamp: now.Add(-2 * time.Hour), NoiseFloor: &nf4,
	})

	// Query all for obs1
	since := now.Add(-3 * time.Hour)
	srv := &Server{db: db}
	metrics, reboots, err := srv.observerMetrics(ctx, "obs1", &since, nil, "5m", 3600)
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics) != 3 {
		t.Errorf("expected 3 metrics, got %d", len(metrics))
	}
	if len(reboots) != 0 {
		t.Errorf("expected 0 reboots, got %d", len(reboots))
	}

	// Verify first row has noise_floor
	if metrics[0].NoiseFloor == nil || *metrics[0].NoiseFloor != -112.5 {
		t.Errorf("first noise_floor = %v, want -112.5", metrics[0].NoiseFloor)
	}
	// First row: no delta possible (first sample)
	if metrics[0].TxAirtimePct != nil {
		t.Errorf("first sample should have nil tx_airtime_pct, got %v", *metrics[0].TxAirtimePct)
	}

	// Second row should have computed deltas
	// TX: (200-100) / 3600 * 100 ≈ 2.78%
	if metrics[1].TxAirtimePct == nil {
		t.Errorf("second sample tx_airtime_pct should not be nil")
	} else if *metrics[1].TxAirtimePct < 2.0 || *metrics[1].TxAirtimePct > 3.5 {
		t.Errorf("second sample tx_airtime_pct = %v, want ~2.78", *metrics[1].TxAirtimePct)
	}

	// Query with until filter
	until := now.Add(-time.Hour)
	metrics2, _, err := srv.observerMetrics(ctx, "obs1", &since, &until, "5m", 3600)
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics2) != 2 {
		t.Errorf("expected 2 metrics with until filter, got %d", len(metrics2))
	}
}

func TestGetMetricsSummary(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	now := time.Now().UTC()
	nf1, nf2, nf3 := -112.0, -108.0, -115.0
	battery := 3720
	mustCreateTestObserverMetrics(t, ctx, db, database.ObserverMetricsInput{
		ObserverID: "obs1", Timestamp: now.Add(-2 * time.Hour), NoiseFloor: &nf1, BatteryMv: &battery,
	})
	mustCreateTestObserverMetrics(t, ctx, db, database.ObserverMetricsInput{
		ObserverID: "obs1", Timestamp: now.Add(-time.Hour), NoiseFloor: &nf2,
	})
	mustCreateTestObserverMetrics(t, ctx, db, database.ObserverMetricsInput{
		ObserverID: "obs2", Timestamp: now.Add(-2 * time.Hour), NoiseFloor: &nf3,
	})

	since := now.Add(-24 * time.Hour)
	summary, err := db.GetMetricsSummary(ctx, since)
	if err != nil {
		t.Fatal(err)
	}
	if len(summary) != 2 {
		t.Fatalf("expected 2 observers in summary, got %d", len(summary))
	}

	// Results sorted by max_nf DESC
	// obs1 has max -108, obs2 has max -115
	if summary[0].ObserverID != "obs1" {
		t.Errorf("first observer should be obs1 (highest max NF), got %s", summary[0].ObserverID)
	}
	if summary[0].CurrentNF == nil || *summary[0].CurrentNF != -108.0 {
		t.Errorf("obs1 current NF = %v, want -108.0", summary[0].CurrentNF)
	}
	if summary[0].SampleCount != 2 {
		t.Errorf("obs1 sample count = %d, want 2", summary[0].SampleCount)
	}
	// Verify sparkline data is included
	if len(summary[0].Sparkline) != 2 {
		t.Errorf("obs1 sparkline length = %d, want 2", len(summary[0].Sparkline))
	}
	if len(summary[1].Sparkline) != 1 {
		t.Errorf("obs2 sparkline length = %d, want 1", len(summary[1].Sparkline))
	}
	// Sparkline should be ordered by timestamp ASC
	if summary[0].Sparkline[0] != nil && *summary[0].Sparkline[0] != -112.0 {
		t.Errorf("obs1 sparkline[0] = %v, want -112.0", *summary[0].Sparkline[0])
	}
	if summary[0].Sparkline[1] != nil && *summary[0].Sparkline[1] != -108.0 {
		t.Errorf("obs1 sparkline[1] = %v, want -108.0", *summary[0].Sparkline[1])
	}
}

func TestObserverMetricsAPIEndpoints(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)

	now := time.Now().UTC()
	nf := -112.0
	mustCreateTestObserverMetrics(t, ctx, db, database.ObserverMetricsInput{
		ObserverID: "obs1", Timestamp: now.Add(-time.Hour), NoiseFloor: &nf,
	})

	// Query directly to verify
	metrics, _, err := (&Server{db: db}).observerMetrics(ctx, "obs1", nil, nil, "5m", 300)
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics) != 1 {
		t.Errorf("expected 1 metric, got %d", len(metrics))
	}
}

func TestComputeDeltas(t *testing.T) {
	int64Ptr := func(v int64) *int64 { return &v }
	floatPtr := func(v float64) *float64 { return &v }
	at := func(minute int) time.Time {
		return time.Date(2026, time.April, 5, 0, minute, 0, 0, time.UTC)
	}

	t.Run("empty input", func(t *testing.T) {
		result, reboots := computeDeltas(nil, 300)
		if result != nil {
			t.Errorf("expected nil, got %v", result)
		}
		if reboots != nil {
			t.Errorf("expected nil reboots, got %v", reboots)
		}
	})

	t.Run("normal delta computation", func(t *testing.T) {
		raw := []database.ObserverMetric{
			{Timestamp: at(0), NoiseFloor: floatPtr(-112), TxAirSecs: int64Ptr(100), RxAirSecs: int64Ptr(500), RecvErrors: int64Ptr(3), PacketsRecv: int64Ptr(1000)},
			{Timestamp: at(5), NoiseFloor: floatPtr(-110), TxAirSecs: int64Ptr(115), RxAirSecs: int64Ptr(525), RecvErrors: int64Ptr(5), PacketsRecv: int64Ptr(1100)},
		}
		result, reboots := computeDeltas(raw, 300)
		if len(result) != 2 {
			t.Fatalf("expected 2 results, got %d", len(result))
		}
		if len(reboots) != 0 {
			t.Errorf("expected 0 reboots, got %d", len(reboots))
		}
		// First sample: no deltas
		if result[0].TxAirtimePct != nil {
			t.Errorf("first sample should have nil tx_airtime_pct")
		}
		// Second sample: TX delta = 15 secs / 300 secs * 100 = 5%
		if result[1].TxAirtimePct == nil {
			t.Fatal("second sample tx_airtime_pct should not be nil")
		}
		if *result[1].TxAirtimePct != 5.0 {
			t.Errorf("tx_airtime_pct = %v, want 5.0", *result[1].TxAirtimePct)
		}
		// RX delta = 25 secs / 300 secs * 100 ≈ 8.33%
		if result[1].RxAirtimePct == nil {
			t.Fatal("second sample rx_airtime_pct should not be nil")
		}
		if *result[1].RxAirtimePct < 8.3 || *result[1].RxAirtimePct > 8.4 {
			t.Errorf("rx_airtime_pct = %v, want ~8.33", *result[1].RxAirtimePct)
		}
		// Error rate: delta_errors=2, delta_recv=100, rate = 2/(100+2)*100 ≈ 1.96%
		if result[1].RecvErrorRate == nil {
			t.Fatal("second sample recv_error_rate should not be nil")
		}
		if *result[1].RecvErrorRate < 1.9 || *result[1].RecvErrorRate > 2.0 {
			t.Errorf("recv_error_rate = %v, want ~1.96", *result[1].RecvErrorRate)
		}
	})

	t.Run("reboot detection", func(t *testing.T) {
		raw := []database.ObserverMetric{
			{Timestamp: at(0), TxAirSecs: int64Ptr(1000), RxAirSecs: int64Ptr(5000)},
			{Timestamp: at(5), TxAirSecs: int64Ptr(10), RxAirSecs: int64Ptr(20)}, // reboot!
			{Timestamp: at(10), TxAirSecs: int64Ptr(25), RxAirSecs: int64Ptr(45)},
		}
		result, reboots := computeDeltas(raw, 300)
		if len(reboots) != 1 {
			t.Fatalf("expected 1 reboot, got %d", len(reboots))
		}
		if got := types.FormatWireTimeSeconds(reboots[0].Time); got != "2026-04-05T00:05:00Z" {
			t.Errorf("reboot timestamp = %s", got)
		}
		if result[1].IsRebootSample == nil || !*result[1].IsRebootSample {
			t.Error("second sample should be marked as reboot")
		}
		// Reboot sample should have nil deltas
		if result[1].TxAirtimePct != nil {
			t.Error("reboot sample should have nil tx_airtime_pct")
		}
		// Third sample should have valid deltas from post-reboot baseline
		if result[2].TxAirtimePct == nil {
			t.Fatal("third sample tx_airtime_pct should not be nil")
		}
		if *result[2].TxAirtimePct != 5.0 { // 15/300*100
			t.Errorf("third sample tx_airtime_pct = %v, want 5.0", *result[2].TxAirtimePct)
		}
	})

	t.Run("gap detection", func(t *testing.T) {
		raw := []database.ObserverMetric{
			{Timestamp: at(0), TxAirSecs: int64Ptr(100)},
			{Timestamp: at(15), TxAirSecs: int64Ptr(200)}, // 15min gap > 2*300s
		}
		result, _ := computeDeltas(raw, 300)
		// Gap sample should have nil deltas
		if result[1].TxAirtimePct != nil {
			t.Error("gap sample should have nil tx_airtime_pct")
		}
	})
}

func TestGetObserverMetricsResolution(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)
	srv := &Server{db: db}
	since := time.Date(2026, time.April, 4, 0, 0, 0, 0, time.UTC)

	metricAt := func(hour, minute int, noise, tx int) {
		nf := float64(noise)
		mustCreateTestObserverMetrics(t, ctx, db, database.ObserverMetricsInput{
			ObserverID: "obs1", Timestamp: time.Date(2026, time.April, 5, hour, minute, 0, 0, time.UTC),
			NoiseFloor: &nf, TxAirSecs: &tx,
		})
	}
	metricAt(0, 0, -112, 100)
	metricAt(0, 5, -110, 200)
	metricAt(1, 0, -108, 500)
	metricAt(1, 5, -106, 600)

	// 5m resolution: all 4 rows
	m5, _, err := srv.observerMetrics(ctx, "obs1", &since, nil, "5m", 300)
	if err != nil {
		t.Fatal(err)
	}
	if len(m5) != 4 {
		t.Errorf("5m resolution: expected 4 rows, got %d", len(m5))
	}

	// 1h resolution: 2 buckets
	m1h, _, err := srv.observerMetrics(ctx, "obs1", &since, nil, "1h", 300)
	if err != nil {
		t.Fatal(err)
	}
	if len(m1h) != 2 {
		t.Errorf("1h resolution: expected 2 rows, got %d", len(m1h))
	}

	// 1d resolution: 1 bucket
	m1d, _, err := srv.observerMetrics(ctx, "obs1", &since, nil, "1d", 300)
	if err != nil {
		t.Fatal(err)
	}
	if len(m1d) != 1 {
		t.Errorf("1d resolution: expected 1 row, got %d", len(m1d))
	}
}

func TestHourlyResolutionDeltasNotNull(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)
	srv := &Server{db: db}
	since := time.Date(2026, time.April, 4, 0, 0, 0, 0, time.UTC)

	observerPubkey := strings.Repeat("d", 64)
	if _, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        observerPubkey,
		Name:      "Hourly",
		IATA:      "SJC",
		Meta:      nil,
		LastSeen:  time.Now().UTC(),
		IngestNow: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed observer: %v", err)
	}

	metricAt := func(hour int, noise float64, tx, rx, recvErrors, sent, received int) {
		mustCreateTestObserverMetrics(t, ctx, db, database.ObserverMetricsInput{
			ObserverID: observerPubkey, Timestamp: time.Date(2026, time.April, 5, hour, 0, 0, 0, time.UTC),
			NoiseFloor: &noise, TxAirSecs: &tx, RxAirSecs: &rx, RecvErrors: &recvErrors,
			PacketsSent: &sent, PacketsRecv: &received,
		})
	}
	metricAt(10, -110, 100, 200, 5, 50, 100)
	metricAt(11, -108, 200, 400, 10, 80, 200)

	m, _, err := srv.observerMetrics(ctx, observerPubkey, &since, nil, "1h", 300)
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(m))
	}
	if m[1].TxAirtimePct == nil {
		t.Error("1h resolution: tx_airtime_pct should not be nil — gap threshold must scale with resolution")
	}
}

func TestLastValuePreservesReboot(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)
	srv := &Server{db: db}
	since := time.Date(2026, time.April, 4, 0, 0, 0, 0, time.UTC)

	observerPubkey := strings.Repeat("e", 64)
	if _, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        observerPubkey,
		Name:      "Reboot",
		IATA:      "SJC",
		Meta:      nil,
		LastSeen:  time.Now().UTC(),
		IngestNow: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed observer: %v", err)
	}

	metricAt := func(hour, minute int, noise float64, tx, rx, recvErrors, sent, received int) {
		mustCreateTestObserverMetrics(t, ctx, db, database.ObserverMetricsInput{
			ObserverID: observerPubkey, Timestamp: time.Date(2026, time.April, 5, hour, minute, 0, 0, time.UTC),
			NoiseFloor: &noise, TxAirSecs: &tx, RxAirSecs: &rx, RecvErrors: &recvErrors,
			PacketsSent: &sent, PacketsRecv: &received,
		})
	}
	metricAt(10, 0, -110, 1000, 2000, 500, 400, 800)
	metricAt(10, 20, -110, 5000, 6000, 900, 700, 1200)
	metricAt(10, 40, -110, 10, 20, 1, 5, 10)
	metricAt(11, 0, -108, 100, 120, 5, 20, 50)

	m, reboots, err := srv.observerMetrics(ctx, observerPubkey, &since, nil, "1h", 300)
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(m))
	}
	if m[1].IsRebootSample != nil {
		t.Error("second bucket should NOT be flagged as reboot with LAST-value aggregation")
	}
	if m[1].TxAirtimePct == nil {
		t.Error("second bucket should have non-nil tx_airtime_pct")
	}
	_ = reboots
}

func TestParseWindowDuration(t *testing.T) {
	tests := []struct {
		input string
		want  time.Duration
		err   bool
	}{
		{"1h", time.Hour, false},
		{"24h", 24 * time.Hour, false},
		{"3d", 3 * 24 * time.Hour, false},
		{"30d", 30 * 24 * time.Hour, false},
		{"invalid", 0, true},
	}
	for _, tc := range tests {
		got, err := parseWindowDuration(tc.input)
		if tc.err && err == nil {
			t.Errorf("parseWindowDuration(%q) expected error", tc.input)
		}
		if !tc.err && got != tc.want {
			t.Errorf("parseWindowDuration(%q) = %v, want %v", tc.input, got, tc.want)
		}
	}
}

func TestGetScopeStats(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	now := time.Now().UTC()
	since := now.Add(-24 * time.Hour)
	create := func(hash string, route types.RouteType, scope *string) {
		t.Helper()
		mustCreateTestTransmission(t, ctx, db, database.TransmissionInput{
			Hash: hash, RawHex: "AA01", RouteType: route, PayloadType: types.PayloadType(5), ScopeName: scope,
		}, now)
	}
	named, unknown := "#belgium", ""
	create("scope-named", types.RouteTransportFlood, &named)
	create("scope-unknown", types.RouteTransportFlood, &unknown)
	create("scope-null", types.RouteTransportFlood, nil)
	create("scope-flood", types.RouteFlood, nil)
	create("scope-direct", types.RouteDirect, nil)

	stats, err := db.GetScopeStats(ctx, since, time.Hour)
	if err != nil {
		t.Fatalf("GetScopeStats: %v", err)
	}
	// TransportTotal still counts only routes 0+3 (transport-code-carrying).
	if stats.Summary.TransportTotal != 3 {
		t.Errorf("TransportTotal = %d, want 3", stats.Summary.TransportTotal)
	}
	if stats.Summary.Scoped != 2 {
		t.Errorf("Scoped = %d, want 2", stats.Summary.Scoped)
	}
	// Unscoped now folds in non-transport (routes 1+2) — see #1838.
	// 1 transport-NULL + 2 non-transport (routes 1 and 2) = 3.
	if stats.Summary.Unscoped != 3 {
		t.Errorf("Unscoped = %d, want 3 (1 transport-null + 2 non-transport)", stats.Summary.Unscoped)
	}
	if stats.Summary.UnknownScope != 1 {
		t.Errorf("UnknownScope = %d, want 1", stats.Summary.UnknownScope)
	}
	if len(stats.ByRegion) != 1 || stats.ByRegion[0].Name != "#belgium" || stats.ByRegion[0].Count != 1 {
		t.Errorf("ByRegion = %+v, want [{#belgium 1}]", stats.ByRegion)
	}
}

// TestGetScopeStatsAdvertsByRole covers the #1979 breakdown: flood adverts in
// the window, grouped by the sender's nodes.role, split by the three
// scope_name states (NULL is unscoped, an empty string is unknown scope, a
// name is a named scope).
func TestGetScopeStatsAdvertsByRole(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	now := time.Now().UTC()
	since := now.Add(-24 * time.Hour)
	create := func(hash string, route types.RouteType, payload types.PayloadType, scope *string, pubkey string, role types.NodeRole) {
		t.Helper()
		var advert *database.NodeInput
		if pubkey != "" {
			advert = &database.NodeInput{PublicKey: pubkey, Name: pubkey, Role: role, RxTime: now, IngestNow: now}
		}
		mustCreateTestTransmission(t, ctx, db, database.TransmissionInput{
			Hash: hash, RawHex: "AA01", RouteType: route, PayloadType: payload, ScopeName: scope, Advert: advert,
		}, now)
	}
	named, unknown := "#belgium", ""
	for _, row := range []struct {
		hash  string
		route types.RouteType
		scope *string
		key   string
		role  types.NodeRole
	}{
		{"r1", types.RouteFlood, nil, "rpt", "repeater"},
		{"r2", types.RouteTransportFlood, &unknown, "rpt", "repeater"},
		{"r3", types.RouteTransportFlood, &named, "rpt", "repeater"},
		{"r4", types.RouteDirect, nil, "rpt", "repeater"},
		{"r5", types.RouteTransportDirect, &named, "rpt", "repeater"},
		{"r6", types.RouteTransportFlood, &named, "rpt", "repeater"},
		{"c1", types.RouteFlood, nil, "cmp", "companion"},
		{"c3", types.RouteTransportFlood, &named, "cmp", "companion"},
		{"s1", types.RouteFlood, nil, "sns", "sensor"},
		{"s2", types.RouteTransportFlood, &unknown, "sns", "sensor"},
		{"u1", types.RouteFlood, nil, "stranger", ""},
		{"u2", types.RouteTransportFlood, &unknown, "norole", ""},
		{"u3", types.RouteTransportFlood, &named, "", ""},
	} {
		create(row.hash, row.route, types.PayloadADVERT, row.scope, row.key, row.role)
	}
	old := now.Add(-48 * time.Hour)
	mustCreateTestTransmission(t, ctx, db, database.TransmissionInput{
		Hash: "r7", RawHex: "AA01", RouteType: types.RouteFlood, PayloadType: types.PayloadADVERT,
	}, old)
	create("r6-non-advert", types.RouteTransportFlood, types.PayloadType(5), &named, "rpt", "repeater")

	stats, err := db.GetScopeStats(ctx, since, time.Hour)
	if err != nil {
		t.Fatalf("GetScopeStats: %v", err)
	}
	// Ordered by total adverts descending, then role name.
	want := []database.ScopeAdvertRoleCount{
		{Role: "repeater", Unscoped: 1, UnknownScope: 1, Named: 2},
		{Role: "unknown", Unscoped: 1, UnknownScope: 1, Named: 1},
		{Role: "companion", Unscoped: 1, UnknownScope: 0, Named: 1},
		{Role: "sensor", Unscoped: 1, UnknownScope: 1, Named: 0},
	}
	if !reflect.DeepEqual(stats.AdvertsByRole, want) {
		t.Errorf("AdvertsByRole = %+v, want %+v", stats.AdvertsByRole, want)
	}
}

func TestGetScopeStatsAdvertsByRoleEmpty(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	stats, err := db.GetScopeStats(ctx, time.Now().Add(-time.Hour), time.Hour)
	if err != nil {
		t.Fatalf("GetScopeStats: %v", err)
	}
	// Array fields are [] when empty, never null (docs/api-spec.md conventions).
	if stats.AdvertsByRole == nil || len(stats.AdvertsByRole) != 0 {
		t.Errorf("AdvertsByRole = %#v, want empty non-nil slice", stats.AdvertsByRole)
	}
}

// TestLoadIndexesRelayHopsFromResolvedPath verifies that after Load(), relay
// nodes that appear only in resolved_path (not in decoded_json) are indexed
// in byNode. Regression for #692: indexByNode was called before observations
// were appended, so tx.ResolvedPath was nil at index time — #806 fixed this
// by indexing inline during the scan, this test locks it in.
func TestLoadIndexesRelayHopsFromResolvedPath(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	now := time.Now().UTC().Add(-time.Hour)

	// Insert a node whose pubkey does NOT appear in any decoded_json —
	// it only relays traffic (appears in resolved_path of other packets).
	const relayPubkey = "relay000aabbccddeeff0011"
	const senderPubkey = "sender00112233445566"

	transmissionID := mustCreateTestTransmission(t, ctx, db, database.TransmissionInput{
		RawHex:      "FF01",
		Hash:        "relaytest0001hash",
		RouteType:   types.RouteFlood,
		PayloadType: types.PayloadADVERT,
		DecodedJSON: `{"pubKey":"` + senderPubkey + `","name":"Sender","type":"ADVERT"}`,
		Advert: &database.NodeInput{
			PublicKey: senderPubkey,
			Name:      "Sender",
			Role:      "repeater",
			RxTime:    now,
			IngestNow: now,
		},
	}, now)
	resolvedRelay := relayPubkey
	if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: relayPubkey,
		Name:      "Relay",
		Role:      "repeater",
		Lat:       nil,
		Lon:       nil,
		RxTime:    now,
		IngestNow: now,
	}); err != nil {
		t.Fatalf("seed relay node: %v", err)
	}
	mustInsertUnobservedTestObservation(t, ctx, db, transmissionID, now, 10, -90, []string{"ee"}, []*string{&resolvedRelay})

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	// The sender should be in byNode via decoded_json.
	if len(store.byNode[senderPubkey]) == 0 {
		t.Errorf("sender not indexed in byNode via decoded_json")
	}

	// The relay node must be in byNode via resolved_path — this was the bug.
	if len(store.byNode[relayPubkey]) == 0 {
		t.Errorf("relay node not indexed in byNode after Load() — resolved_path indexing broken")
	}
	if store.byNode[relayPubkey][0].Hash != "relaytest0001hash" {
		t.Errorf("relay byNode entry has wrong hash: %s", store.byNode[relayPubkey][0].Hash)
	}
}

// TestGetChannelsPreviewRespectsRegionFilter verifies channel previews are
// sampled from messages heard in the requested observer region.
func TestGetChannelsPreviewRespectsRegionFilter(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	now := time.Now().UTC()
	obsSJC := mustCreateTestObserver(t, ctx, db, "obs-sjc", "Observer SJC", "SJC", now)
	obsSFO := mustCreateTestObserver(t, ctx, db, "obs-sfo", "Observer SFO", "SFO", now)
	createMessage := func(hash, sender, text string, firstSeen time.Time, observerID int64) {
		t.Helper()
		decoded := `{"type":"CHAN","channel":"#shared","text":"` + sender + `: ` + text + `","sender":"` + sender + `"}`
		id := mustCreateTestTransmission(t, ctx, db, database.TransmissionInput{
			RawHex:      hash,
			Hash:        hash,
			RouteType:   types.RouteFlood,
			PayloadType: types.PayloadGRP_TXT,
			DecodedJSON: decoded,
			ChannelHash: "#shared",
		}, firstSeen)
		dbtest.Observation(t, db, id, observerID, firstSeen, 12, -90, nil...)
	}
	createMessage("AA01", "Alice", "heard in SJC", now.Add(-10*time.Minute), obsSJC)
	createMessage("AA02", "Bob", "heard in SFO", now.Add(-5*time.Minute), obsSFO)

	srv := &Server{db: db}
	sjc, err := srv.getChannels(ctx, "SJC")
	if err != nil {
		t.Fatal(err)
	}
	if len(sjc) != 1 {
		t.Fatalf("expected 1 channel for SJC, got %d", len(sjc))
	}
	if got := sjc[0].LastSender; got == nil || *got != "Alice" {
		t.Errorf("preview sender = %v, want Alice: SJC must not be shown Bob's message, "+
			"which only SFO heard", got)
	}
	if got := sjc[0].LastMessage; got == nil || *got != "heard in SJC" {
		t.Errorf("preview message = %v, want \"heard in SJC\"", got)
	}

	// The other direction, so the test cannot pass by always picking the oldest.
	sfo, err := srv.getChannels(ctx, "SFO")
	if err != nil {
		t.Fatal(err)
	}
	if len(sfo) != 1 {
		t.Fatalf("expected 1 channel for SFO, got %d", len(sfo))
	}
	if got := sfo[0].LastSender; got == nil || *got != "Bob" {
		t.Errorf("preview sender = %v, want Bob for SFO", got)
	}

	// Unscoped still shows the globally newest, which was never in question.
	all, err := srv.getChannels(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Fatalf("expected 1 unfiltered channel, got %d", len(all))
	}
	if got := all[0].LastSender; got == nil || *got != "Bob" {
		t.Errorf("unfiltered preview sender = %v, want Bob (the newest message)", got)
	}
}
