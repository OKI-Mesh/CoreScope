package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// newTestContext extracts the repeated newTestStore + MQTTSource boilerplate.
func newTestContext(t *testing.T) (*Store, MQTTSource) {
	t.Helper()
	return newTestStore(t), MQTTSource{Name: "test"}
}

// --- config.go: NodeDaysOrDefault (0% coverage) ---

func TestNodeDaysOrDefault(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want int
	}{
		{"nil retention", Config{}, 7},
		{"zero nodeDays", Config{Retention: &RetentionConfig{NodeDays: 0}}, 7},
		{"negative nodeDays", Config{Retention: &RetentionConfig{NodeDays: -1}}, 7},
		{"custom nodeDays", Config{Retention: &RetentionConfig{NodeDays: 14}}, 14},
		{"one day", Config{Retention: &RetentionConfig{NodeDays: 1}}, 1},
	}
	for i := range tests {
		tt := &tests[i]
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.NodeDaysOrDefault()
			if got != tt.want {
				t.Errorf("NodeDaysOrDefault() = %d, want %d", got, tt.want)
			}
		})
	}
}

// --- config.go: ResolvedSources broker scheme normalization (71.4% → 100%) ---

func TestResolvedSourcesBrokerScheme(t *testing.T) {
	cfg := &Config{
		MQTTSources: []MQTTSource{
			{Name: "mqtt", Broker: "mqtt://broker:1883"},
			{Name: "mqtts", Broker: "mqtts://broker:8883"},
			{Name: "tcp", Broker: "tcp://broker:1883"},
		},
	}
	sources := cfg.ResolvedSources()
	if sources[0].Broker != "tcp://broker:1883" {
		t.Errorf("mqtt:// should become tcp://, got %s", sources[0].Broker)
	}
	if sources[1].Broker != "ssl://broker:8883" {
		t.Errorf("mqtts:// should become ssl://, got %s", sources[1].Broker)
	}
	if sources[2].Broker != "tcp://broker:1883" {
		t.Errorf("tcp:// should stay, got %s", sources[2].Broker)
	}
}

// --- db.go: MoveStaleNodes (0% coverage) ---

func TestMoveStaleNodes(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	// Insert a node with last_seen 30 days ago
	err := store.UpsertNode(ctx, "deadbeef1234567890abcdef12345678", "OldNode", "companion", nil, nil, time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}

	// Insert a recent node
	err = store.UpsertNode(ctx, "aabbccdd1234567890abcdef12345678", "NewNode", "repeater", nil, nil, time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}

	moved, err := store.MoveStaleNodes(ctx, 7)
	if err != nil {
		t.Fatal(err)
	}
	if moved != 1 {
		t.Errorf("moved=%d, want 1", moved)
	}

	count := countWhere(t, store, "nodes", "inactive=TRUE")
	if count != 1 {
		t.Errorf("inactive_nodes count=%d, want 1", count)
	}

	count = countWhere(t, store, "nodes", "inactive=FALSE")
	if count != 1 {
		t.Errorf("nodes count=%d, want 1", count)
	}
}

func TestMoveStaleNodesNoneToMove(t *testing.T) {
	store := newTestStore(t)
	moved, err := store.MoveStaleNodes(t.Context(), 7)
	if err != nil {
		t.Fatal(err)
	}
	if moved != 0 {
		t.Errorf("moved=%d, want 0", moved)
	}
}

// --- geo_filter.go: NodePassesGeoFilter (40% → 100%) ---

func TestNodePassesGeoFilterAllBranches(t *testing.T) {
	lat, lon := 37.0, -122.0
	outLat, outLon := 50.0, 10.0

	latMin, latMax := 36.0, 38.0
	lonMin, lonMax := -123.0, -121.0
	gf := &GeoFilterConfig{
		LatMin: &latMin, LatMax: &latMax,
		LonMin: &lonMin, LonMax: &lonMax,
	}

	if !NodePassesGeoFilter(nil, nil, gf) {
		t.Error("nil coords should pass")
	}
	if !NodePassesGeoFilter(&lat, nil, gf) {
		t.Error("nil lon should pass")
	}
	if !NodePassesGeoFilter(nil, &lon, gf) {
		t.Error("nil lat should pass")
	}
	if !NodePassesGeoFilter(&lat, &lon, gf) {
		t.Error("inside filter should pass")
	}
	if NodePassesGeoFilter(&outLat, &outLon, gf) {
		t.Error("outside filter should fail")
	}
}

// --- main.go: handleMessage channel messages (41.4% → higher) ---

func TestHandleMessageChannelMessage(t *testing.T) {
	store, source := newTestContext(t)
	ctx := t.Context()

	payload := []byte(`{"text":"Alice: Hello everyone","channel_idx":3,"SNR":5.0,"RSSI":-95,"score":10,"direction":"rx","sender_timestamp":1700000000}`)
	msg := &mockMessage{topic: "meshcore/message/channel/2", payload: payload}

	handleMessage(ctx, store, "test", source, msg, nil, nil, &Config{})

	count := countRows(t, store, "transmissions")
	if count != 1 {
		t.Errorf("transmissions count=%d, want 1", count)
	}

	// Verify stored transmission values
	var decodedJSON *string
	mustScan(t, testRow(t, store, "SELECT decoded_json FROM transmissions ORDER BY id LIMIT 1"), &decodedJSON)
	if decodedJSON == nil {
		t.Fatal("decoded_json is NULL")
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal([]byte(*decodedJSON), &decoded); err != nil {
		t.Fatalf("decoded_json unmarshal: %v", err)
	}
	if decoded["type"] != "CHAN" {
		t.Errorf("type=%v, want CHAN", decoded["type"])
	}
	if decoded["text"] != "Alice: Hello everyone" {
		t.Errorf("text=%v, want 'Alice: Hello everyone'", decoded["text"])
	}
	if decoded["sender"] != "Alice" {
		t.Errorf("sender=%v, want Alice", decoded["sender"])
	}
	if decoded["channel"] != "ch3" {
		t.Errorf("channel=%v, want ch3", decoded["channel"])
	}

	// Verify observation values
	var snr, rssi *float64
	var score *int32
	var direction *string
	mustScan(t, testRow(t, store, "SELECT snr, rssi, score, direction FROM observations ORDER BY id LIMIT 1"),
		&snr, &rssi, &score, &direction)
	if snr == nil || *snr != 5.0 {
		t.Errorf("snr=%v, want 5.0", snr)
	}
	if direction == nil || *direction != "rx" {
		t.Errorf("direction=%v, want rx", direction)
	}

	// Sender node should NOT be created (see issue #665: synthetic "sender-" keys
	// are unreachable from the claiming/health flow)
	count = countRows(t, store, "nodes")
	if count != 0 {
		t.Errorf("nodes count=%d, want 0 (no phantom sender node)", count)
	}
}

func TestHandleMessageChannelMessageEmptyText(t *testing.T) {
	store, source := newTestContext(t)
	ctx := context.Background()

	msg := &mockMessage{topic: "meshcore/message/channel/1", payload: []byte(`{"text":""}`)}
	handleMessage(ctx, store, "test", source, msg, nil, nil, &Config{})

	count := countRows(t, store, "transmissions")
	if count != 0 {
		t.Error("empty text should not insert")
	}
}

func TestHandleMessageChannelNoSender(t *testing.T) {
	store, source := newTestContext(t)
	ctx := t.Context()

	msg := &mockMessage{topic: "meshcore/message/channel/1", payload: []byte(`{"text":"no sender here"}`)}
	handleMessage(ctx, store, "test", source, msg, nil, nil, &Config{})

	count := countRows(t, store, "nodes")
	if count != 0 {
		t.Error("no sender should mean no node")
	}
}

func TestHandleMessageDirectMessage(t *testing.T) {
	store, source := newTestContext(t)
	ctx := t.Context()

	payload := []byte(`{"text":"Bob: Hey there","sender_timestamp":1700000000,"SNR":3.0,"rssi":-100,"Score":8,"Direction":"tx"}`)
	msg := &mockMessage{topic: "meshcore/message/direct/abc123", payload: payload}

	handleMessage(ctx, store, "test", source, msg, nil, nil, &Config{})

	count := countRows(t, store, "transmissions")
	if count != 1 {
		t.Errorf("transmissions count=%d, want 1", count)
	}

	// Verify stored decoded values
	var decodedJSON string
	mustScan(t, testRow(t, store, "select decoded_json from transmissions limit 1"), &decodedJSON)
	var decoded map[string]interface{}
	if err := json.Unmarshal([]byte(decodedJSON), &decoded); err != nil {
		t.Fatalf("decoded_json unmarshal: %v", err)
	}
	if decoded["type"] != "DM" {
		t.Errorf("type=%v, want DM", decoded["type"])
	}
	if decoded["sender"] != "Bob" {
		t.Errorf("sender=%v, want Bob", decoded["sender"])
	}

	// Verify observation score=8 and direction=tx
	var score *float64
	var direction *string
	mustScan(t, testRow(t, store, "SELECT score, direction FROM observations LIMIT 1"), &score, &direction)
	if score == nil || *score != 8.0 {
		t.Errorf("score=%v, want 8.0", score)
	}
	if direction == nil || *direction != "tx" {
		t.Errorf("direction=%v, want tx", direction)
	}
}

func TestHandleMessageDirectMessageEmptyText(t *testing.T) {
	store, source := newTestContext(t)
	ctx := t.Context()

	msg := &mockMessage{topic: "meshcore/message/direct/abc", payload: []byte(`{"text":""}`)}
	handleMessage(ctx, store, "test", source, msg, nil, nil, &Config{})

	count := countRows(t, store, "transmissions")
	if count != 0 {
		t.Error("empty text DM should not insert")
	}
}

func TestHandleMessageDirectNoSender(t *testing.T) {
	store, source := newTestContext(t)
	ctx := t.Context()

	msg := &mockMessage{topic: "meshcore/message/direct/xyz", payload: []byte(`{"text":"message with no colon"}`)}
	handleMessage(ctx, store, "test", source, msg, nil, nil, &Config{})

	count := countRows(t, store, "transmissions")
	if count != 1 {
		t.Errorf("count=%d, want 1", count)
	}
}

// Test Score/Direction case-insensitive handling in raw packets
func TestHandleMessageUppercaseScoreDirection(t *testing.T) {
	store, source := newTestContext(t)
	ctx := t.Context()

	rawHex := "0A00D69FD7A5A7475DB07337749AE61FA53A4788E976"
	payload := []byte(`{"raw":"` + rawHex + `","Score":9.0,"Direction":"tx"}`)
	msg := &mockMessage{topic: "meshcore/SJC/obs1/packets", payload: payload}

	handleMessage(ctx, store, "test", source, msg, nil, nil, &Config{})

	var score *float64
	var direction *string
	mustScan(t, testRow(t, store, "SELECT score, direction FROM observations LIMIT 1"), &score, &direction)
	if score == nil || *score != 9.0 {
		t.Errorf("score=%v, want 9.0", score)
	}
	if direction == nil || *direction != "tx" {
		t.Errorf("direction=%v, want tx", direction)
	}
}

// Test channel messages with lowercase snr/rssi/Score/Direction
func TestHandleMessageChannelLowercaseFields(t *testing.T) {
	store, source := newTestContext(t)
	ctx := t.Context()

	payload := []byte(`{"text":"Test: msg","snr":3.0,"rssi":-90,"Score":5,"Direction":"rx"}`)
	msg := &mockMessage{topic: "meshcore/message/channel/0", payload: payload}
	handleMessage(ctx, store, "test", source, msg, nil, nil, &Config{})

	count := countRows(t, store, "transmissions")
	if count != 1 {
		t.Errorf("count=%d, want 1", count)
	}
}

func TestHandleMessageDirectLowercaseFields(t *testing.T) {
	store, source := newTestContext(t)
	ctx := t.Context()

	payload := []byte(`{"text":"Test: msg","snr":2.0,"rssi":-85,"score":7,"direction":"tx"}`)
	msg := &mockMessage{topic: "meshcore/message/direct/xyz", payload: payload}
	handleMessage(ctx, store, "test", source, msg, nil, nil, &Config{})

	count := countRows(t, store, "transmissions")
	if count != 1 {
		t.Errorf("count=%d, want 1", count)
	}
}

// --- main.go: handleMessage advert with telemetry ---

func TestHandleMessageAdvertWithTelemetry(t *testing.T) {
	store, source := newTestContext(t)
	ctx := t.Context()

	// Use a known ADVERT hex
	rawHex := "120046D62DE27D4C5194D7821FC5A34A45565DCC2537B300B9AB6275255CEFB65D840CE5C169C94C9AED39E8BCB6CB6EB0335497A198B33A1A610CD3B03D8DCFC160900E5244280323EE0B44CACAB8F02B5B38B91CFA18BD067B0B5E63E94CFC85F758A8530B9240933402E0E6B8F84D5252322D52"
	msg := &mockMessage{
		topic:   "meshcore/SJC/obs1/packets",
		payload: []byte(`{"raw":"` + rawHex + `"}`),
	}

	handleMessage(ctx, store, "test", source, msg, nil, nil, &Config{})

	// Should have created transmission, node, and observer
	var txCount, nodeCount, obsCount int
	txCount = countRows(t, store, "transmissions")
	nodeCount = countRows(t, store, "nodes")
	obsCount = countRows(t, store, "observers")
	if txCount != 1 {
		t.Errorf("transmissions=%d, want 1", txCount)
	}
	if nodeCount != 1 {
		t.Errorf("nodes=%d, want 1", nodeCount)
	}
	if obsCount != 1 {
		t.Errorf("observers=%d, want 1", obsCount)
	}
}

// --- main.go: handleMessage geo filter on advert ---

func TestHandleMessageAdvertGeoFiltered(t *testing.T) {
	store, source := newTestContext(t)
	ctx := t.Context()

	rawHex := "120046D62DE27D4C5194D7821FC5A34A45565DCC2537B300B9AB6275255CEFB65D840CE5C169C94C9AED39E8BCB6CB6EB0335497A198B33A1A610CD3B03D8DCFC160900E5244280323EE0B44CACAB8F02B5B38B91CFA18BD067B0B5E63E94CFC85F758A8530B9240933402E0E6B8F84D5252322D52"

	latMin, latMax := -1.0, 1.0
	lonMin, lonMax := -1.0, 1.0
	gf := &GeoFilterConfig{
		LatMin: &latMin, LatMax: &latMax,
		LonMin: &lonMin, LonMax: &lonMax,
	}

	msg := &mockMessage{
		topic:   "meshcore/SJC/obs1/packets",
		payload: []byte(`{"raw":"` + rawHex + `"}`),
	}
	// Legacy silent-drop behavior is now opt-in via ForeignAdverts.Mode="drop"
	// (#730). The new default — flag — is covered by foreign_advert_test.go.
	handleMessage(ctx, store, "test", source, msg, nil, nil, &Config{
		GeoFilter:      gf,
		ForeignAdverts: &ForeignAdvertConfig{Mode: "drop"},
	})

	// Geo-filtered adverts should not create nodes
	nodeCount := countRows(t, store, "nodes")

	if nodeCount != 0 {
		t.Errorf("nodes=%d, want 0 (geo-filtered advert in drop mode should not create node)", nodeCount)
	}
}

// --- db.go: Checkpoint and LogStats ---

func TestLogStatsDoesNotPanic(t *testing.T) {
	store := newTestStore(t)
	store.Stats.TransmissionsInserted.Add(5)
	store.LogStats()
}

// --- main.go: handleMessage advert that fails ValidateAdvert ---

func TestHandleMessageCorruptedAdvertNoNode(t *testing.T) {
	store, source := newTestContext(t)
	ctx := t.Context()

	// Build an ADVERT packet with all-zero pubkey (fails ValidateAdvert)
	// header: 0x12 = FLOOD + ADVERT, path: 0x00
	// Then 100+ bytes of zeros (pubkey all zeros)
	rawHex := "1200"
	for i := 0; i < 110; i++ {
		rawHex += "00"
	}
	msg := &mockMessage{
		topic:   "meshcore/SJC/obs1/packets",
		payload: []byte(`{"raw":"` + rawHex + `"}`),
	}
	handleMessage(ctx, store, "test", source, msg, nil, nil, &Config{})

	count := countRows(t, store, "nodes")

	if count != 0 {
		t.Error("all-zero pubkey advert should not create a node")
	}
}

// --- main.go: handleMessage non-advert packet (else branch) ---

func TestHandleMessageNonAdvertPacket(t *testing.T) {
	store, source := newTestContext(t)

	// ACK packet: header 0x0E = DIRECT (route 2) | ACK (payload 3) << 2
	rawHex := "0E00DEADBEEF"
	msg := &mockMessage{
		topic:   "meshcore/SJC/obs1/packets",
		payload: []byte(`{"raw":"` + rawHex + `"}`),
	}
	handleMessage(t.Context(), store, "test", source, msg, nil, nil, &Config{})

	if n := countRows(t, store, "transmissions"); n != 1 {
		t.Fatalf("non-advert should insert transmission, got %d", n)
	}

	var routeType types.RouteType
	var payloadType types.PayloadType
	mustScan(t, testRow(t, store,
		"SELECT route_type, payload_type FROM transmissions ORDER BY id LIMIT 1"),
		&routeType, &payloadType)
	if payloadType != types.PayloadACK {
		t.Errorf("payload_type=%v, want %v", payloadType, types.PayloadACK)
	}
	if routeType != types.RouteDirect {
		t.Errorf("route_type=%v, want %v", routeType, types.RouteDirect)
	}

	if n := countRows(t, store, "nodes"); n != 0 {
		t.Error("non-advert should not create nodes")
	}
}

// --- db.go: InsertTransmission default timestamp ---

func TestInsertTransmissionDefaultTimestamp(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	data := &PacketData{
		RawHex:    "AABB",
		Hash:      "default_ts_test1",
		Timestamp: time.Time{}, // zero → should use ingest time
	}
	before := time.Now().UTC().Add(-time.Second)
	isNew, err := store.InsertTransmission(ctx, data)
	after := time.Now().UTC().Add(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !isNew {
		t.Error("should be new")
	}

	var firstSeen time.Time
	mustScan(t, testRow(t, store,
		`SELECT first_seen FROM transmissions WHERE hash = $1`, "default_ts_test1"), &firstSeen)
	if firstSeen.Before(before) || firstSeen.After(after) {
		t.Errorf("first_seen = %v, want within [%v, %v] (ingest time)", firstSeen, before, after)
	}
}

// --- db.go: UpsertNode default timestamp ---

func TestUpsertNodeDefaultTimestamp(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	const pk = "pk_default_ts_00000000000000000000000000000001"

	before := time.Now().UTC().Add(-time.Second)
	// Zero lastSeen → uses ingest time
	if err := store.UpsertNode(ctx, pk, "Node", "companion", nil, nil, time.Time{}); err != nil {
		t.Fatal(err)
	}
	after := time.Now().UTC().Add(time.Second)

	var lastSeen, firstSeen *time.Time
	mustScan(t, testRow(t, store,
		`SELECT last_seen, first_seen FROM nodes WHERE public_key = $1`, pk), &lastSeen, &firstSeen)
	if lastSeen == nil || lastSeen.Before(before) || lastSeen.After(after) {
		t.Errorf("last_seen = %v, want within [%v, %v] (ingest time)", lastSeen, before, after)
	}
	if firstSeen == nil || !firstSeen.Equal(*lastSeen) {
		t.Errorf("first_seen = %v, want it equal to last_seen on first insert", firstSeen)
	}
}

// --- db.go: UpsertNode with lat/lon ---

func TestUpsertNodeWithLatLon(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	lat, lon := 37.0, -122.0
	err := store.UpsertNode(ctx, "pk_latlon_0000000000000000000000000000000001", "Node", "repeater", &lat, &lon, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	var gotLat, gotLon float64
	mustScan(t, testRow(t, store, "SELECT lat, lon from nodes WHERE public_key = 'pk_latlon_0000000000000000000000000000000001'"), &gotLat, &gotLon)
	if gotLat != 37.0 || gotLon != -122.0 {
		t.Errorf("lat=%f lon=%f", gotLat, gotLon)
	}
}

// --- main.go: handleMessage channel with very long sender (>50 chars = no extraction) ---

func TestHandleMessageChannelLongSender(t *testing.T) {
	store, source := newTestContext(t)
	ctx := t.Context()

	// Colon at index > 50 — should not extract sender
	longText := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA: msg"
	payload := []byte(`{"text":"` + longText + `"}`)
	msg := &mockMessage{topic: "meshcore/message/channel/1", payload: payload}
	handleMessage(ctx, store, "test", source, msg, nil, nil, &Config{})

	count := countRows(t, store, "nodes")
	if count != 0 {
		t.Error("long sender should not extract")
	}
}

// --- main.go: handleMessage DM with long sender ---

func TestHandleMessageDirectLongSender(t *testing.T) {
	store, source := newTestContext(t)
	ctx := t.Context()

	longText := "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB: msg"
	payload := []byte(`{"text":"` + longText + `"}`)
	msg := &mockMessage{topic: "meshcore/message/direct/abc", payload: payload}
	handleMessage(ctx, store, "test", source, msg, nil, nil, &Config{})

	count := countRows(t, store, "transmissions")
	if count != 1 {
		t.Errorf("count=%d, want 1", count)
	}
}

// DM with uppercase Score and Direction (fallback branches)
func TestHandleMessageDirectUppercaseScoreDirection(t *testing.T) {
	store, source := newTestContext(t)
	ctx := t.Context()

	payload := []byte(`{"text":"X: hi","Score":6,"Direction":"rx"}`)
	msg := &mockMessage{topic: "meshcore/message/direct/d1", payload: payload}
	handleMessage(ctx, store, "test", source, msg, nil, nil, &Config{})

	count := countRows(t, store, "transmissions")
	if count != 1 {
		t.Errorf("count=%d, want 1", count)
	}

	// Verify uppercase Score/Direction were picked up
	var score *float64
	var direction *string
	mustScan(t, testRow(t, store, "select score, direction from observations limit 1"), &score, &direction)
	if score == nil || *score != 6.0 {
		t.Errorf("score=%v, want 6.0", score)
	}
	if direction == nil || *direction != "rx" {
		t.Errorf("direction=%v, want rx", direction)
	}
}

// Channel with uppercase Score and Direction
func TestHandleMessageChannelUppercaseScoreDirection(t *testing.T) {
	store, source := newTestContext(t)
	ctx := t.Context()

	payload := []byte(`{"text":"Y: hi","Score":4,"Direction":"tx"}`)
	msg := &mockMessage{topic: "meshcore/message/channel/5", payload: payload}
	handleMessage(ctx, store, "test", source, msg, nil, nil, &Config{})

	count := countRows(t, store, "transmissions")
	if count != 1 {
		t.Errorf("count=%d, want 1", count)
	}

	// Verify uppercase Score/Direction were picked up
	var score *float64
	var direction *string
	mustScan(t, testRow(t, store, "select score, direction from observations limit 1"), &score, &direction)
	if score == nil || *score != 4.0 {
		t.Errorf("score=%v, want 4.0", score)
	}
	if direction == nil || *direction != "tx" {
		t.Errorf("direction=%v, want tx", direction)
	}
}

// Raw packet with only lowercase score (no uppercase Score present)
func TestHandleMessageRawLowercaseScore(t *testing.T) {
	store, source := newTestContext(t)
	ctx := t.Context()

	rawHex := "0A00D69FD7A5A7475DB07337749AE61FA53A4788E976"
	payload := []byte(`{"raw":"` + rawHex + `","score":3.5}`)
	msg := &mockMessage{topic: "meshcore/SJC/obs1/packets", payload: payload}
	handleMessage(ctx, store, "test", source, msg, nil, nil, &Config{})

	var score *float64
	mustScan(t, testRow(t, store, "select score from observations limit 1"), &score)
	if score == nil || *score != 3.5 {
		t.Errorf("score=%v, want 3.5", score)
	}
}

// Test handleMessage status without origin (log fallback)
func TestHandleMessageStatusNoOrigin(t *testing.T) {
	store, source := newTestContext(t)
	ctx := t.Context()

	msg := &mockMessage{
		topic:   "meshcore/LAX/obs5/status",
		payload: []byte(`{"model":"L1"}`),
	}
	handleMessage(ctx, store, "test", source, msg, nil, nil, &Config{})

	if n := countWhere(t, store, "observers", "pubkey = $1", "obs5"); n != 1 {
		t.Errorf("observer count=%d, want 1", n)
	}

	// Verify fallback behavior: origin is "" (no "origin" key in payload), so name
	// is stored as NULL (blank means "unknown" and never overwrites a real name),
	// and IATA should be "LAX".
	var name, iata *string
	mustScan(t, testRow(t, store,
		`SELECT name, iata FROM observers WHERE pubkey = $1`, "obs5"), &name, &iata)
	if name != nil {
		t.Errorf("name=%q, want NULL (no origin provided)", *name)
	}
	if iata == nil || *iata != "LAX" {
		t.Errorf("iata=%v, want LAX", iata)
	}
}

// --- db.go: applySchema migrations run on fresh DB ---

func TestGooseMigrationsAppliedOnFreshDB(t *testing.T) {
	// newTestStore builds the schema through the real migration path; verify
	// that it ran to completion and produced the expected objects.
	store := newTestStore(t)
	ctx := testCtx(t)

	db, err := sql.Open("pgx", store.db.Pool().Config().ConnString()) // import _ "github.com/jackc/pgx/v5/stdlib"
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	current, err := database.CurrentSchemaVersion(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	latest, err := database.LatestSchemaVersion(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if latest == 0 {
		t.Fatal("no embedded migrations found; the check below would pass vacuously")
	}
	if current != latest {
		t.Errorf("goose version = %d, want %d (latest embedded)", current, latest)
	}

	for _, tbl := range []string{"nodes", "observers", "transmissions", "observations", "observation_path_hops"} {
		if n := countWhere(t, store, "pg_tables", "schemaname = 'public' AND tablename = $1", tbl); n != 1 {
			t.Errorf("table %s should exist", tbl)
		}
	}
	if n := countWhere(t, store, "pg_tables", "schemaname = 'public' AND tablename = $1", "inactive_nodes"); n != 0 {
		t.Error("inactive_nodes should not exist; nodes carries an inactive flag")
	}
	if n := countWhere(t, store, "pg_views", "schemaname = 'public' AND viewname = $1", "packets_v"); n != 1 {
		t.Error("packets_v view should exist")
	}
}

// Test MoveStaleNodes with existing inactive nodes (REPLACE behavior)
func TestMoveStaleNodesMarksInactive(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	now := time.Now().UTC()
	const stale = "stale_node_0000000000000000000000000000000000000001"
	const fresh = "fresh_node_0000000000000000000000000000000000000001"

	if err := store.UpsertNode(ctx, stale, "StaleNode", "repeater", nil, nil, now.AddDate(0, 0, -40)); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertNode(ctx, fresh, "FreshNode", "repeater", nil, nil, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}

	moved, err := store.MoveStaleNodes(ctx, 7)
	if err != nil {
		t.Fatal(err)
	}
	if moved != 1 {
		t.Errorf("moved=%d, want 1", moved)
	}

	if n := countWhere(t, store, "nodes", "public_key = $1 AND inactive", stale); n != 1 {
		t.Error("stale node should be marked inactive")
	}
	if n := countWhere(t, store, "nodes", "public_key = $1 AND NOT inactive", fresh); n != 1 {
		t.Error("fresh node should stay active")
	}
	// Nothing is deleted or copied: both rows are still in nodes. This is what
	// keeps neighbor_edges and observation_path_hops references valid.
	if n := countRows(t, store, "nodes"); n != 2 {
		t.Errorf("nodes rows = %d, want 2", n)
	}

	// Idempotent: the stale node is already inactive, so a second run changes nothing.
	if moved, err := store.MoveStaleNodes(ctx, 7); err != nil || moved != 0 {
		t.Errorf("second run: moved=%d err=%v, want 0 and nil", moved, err)
	}
}

// --- db.go: RemoveStaleObservers (soft-delete) ---

func TestRemoveStaleObservers(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	now := time.Now().UTC()

	// obs-old is first seen 30 days ago; obs-new is current.
	if err := store.UpsertObserverAt(ctx, "obs-old", "OldObserver", "LAX", nil, now.AddDate(0, 0, -30)); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertObserverAt(ctx, "obs-new", "NewObserver", "NYC", nil, now); err != nil {
		t.Fatal(err)
	}
	// Metrics rows for both: the prune deletes metrics for observers it deactivates.
	// observer_metrics.observer_id holds the pubkey text, not the surrogate id.
	for _, pk := range []string{"obs-old", "obs-new"} {
		testExec(t, store, `INSERT INTO observer_metrics (observer_id, timestamp) VALUES ($1, $2)`, pk, now)
	}

	removed, err := store.RemoveStaleObservers(ctx, 14)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Errorf("removed=%d, want 1", removed)
	}

	// Soft delete: both rows are still there.
	if n := countRows(t, store, "observers"); n != 2 {
		t.Errorf("observers count=%d, want 2 (soft-delete preserves row)", n)
	}
	if n := countWhere(t, store, "observers", "pubkey = $1 AND inactive", "obs-old"); n != 1 {
		t.Error("obs-old should be marked inactive")
	}
	if n := countWhere(t, store, "observers", "pubkey = $1 AND NOT inactive", "obs-new"); n != 1 {
		t.Error("obs-new should stay active")
	}

	// The metrics of the deactivated observer are gone; the active one keeps its own.
	if n := countWhere(t, store, "observer_metrics", "observer_id = $1", "obs-old"); n != 0 {
		t.Errorf("obs-old metrics = %d, want 0 after deactivation", n)
	}
	if n := countWhere(t, store, "observer_metrics", "observer_id = $1", "obs-new"); n != 1 {
		t.Errorf("obs-new metrics = %d, want 1", n)
	}

	// Idempotent: obs-old is already inactive, so a second run changes nothing.
	if removed, err := store.RemoveStaleObservers(ctx, 14); err != nil || removed != 0 {
		t.Errorf("second run: removed=%d err=%v, want 0 and nil", removed, err)
	}
}

func TestRemoveStaleObserversNone(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	removed, err := store.RemoveStaleObservers(ctx, 14)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 {
		t.Errorf("removed=%d, want 0", removed)
	}
}

func TestRemoveStaleObserversKeepForever(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	// An observer last seen a year ago, created that way through the real write path.
	if err := store.UpsertObserverAt(ctx, "obs-ancient", "AncientObserver", "LAX", nil,
		time.Now().UTC().AddDate(0, 0, -365)); err != nil {
		t.Fatal(err)
	}

	// observerDays = -1 means keep forever
	removed, err := store.RemoveStaleObservers(ctx, -1)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 {
		t.Errorf("removed=%d, want 0 (keep forever)", removed)
	}
	if n := countRows(t, store, "observers"); n != 1 {
		t.Errorf("observers count=%d, want 1 (keep forever)", n)
	}
	if n := countWhere(t, store, "observers", "pubkey = $1 AND NOT inactive", "obs-ancient"); n != 1 {
		t.Error("obs-ancient should NOT be marked inactive (keep forever)")
	}

	// Positive control: the same observer IS removed under a finite window, so
	// the zero above came from the -1 rule and not from a fixture that was
	// never stale.
	if removed, err := store.RemoveStaleObservers(ctx, 14); err != nil || removed != 1 {
		t.Fatalf("control: removed=%d err=%v, want 1 and nil — the fixture isn't stale enough to prove anything", removed, err)
	}
}

func TestRemoveStaleObserversReactivation(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	// Created already stale, through the real write path.
	if err := store.UpsertObserverAt(ctx, "obs-test", "TestObserver", "LAX", nil,
		time.Now().UTC().AddDate(0, 0, -30)); err != nil {
		t.Fatal(err)
	}

	removed, err := store.RemoveStaleObservers(ctx, 14)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Errorf("removed=%d, want 1", removed)
	}
	if n := countWhere(t, store, "observers", "pubkey = $1 AND inactive", "obs-test"); n != 1 {
		t.Fatal("obs-test should be inactive after soft-delete; the reactivation check below proves nothing otherwise")
	}

	// A live message from the same observer reactivates it.
	if err := store.UpsertObserver(ctx, "obs-test", "TestObserver", "LAX", nil); err != nil {
		t.Fatal(err)
	}
	if n := countWhere(t, store, "observers", "pubkey = $1 AND NOT inactive", "obs-test"); n != 1 {
		t.Error("obs-test should be active again after a live upsert")
	}
}
func TestObserverDaysOrDefault(t *testing.T) {
	tests := []struct {
		name string
		cfg  *Config
		want int
	}{
		{"nil retention", &Config{}, 14},
		{"zero observer days", &Config{Retention: &RetentionConfig{ObserverDays: 0}}, 14},
		{"positive value", &Config{Retention: &RetentionConfig{ObserverDays: 30}}, 30},
		{"keep forever", &Config{Retention: &RetentionConfig{ObserverDays: -1}}, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.ObserverDaysOrDefault()
			if got != tt.want {
				t.Errorf("ObserverDaysOrDefault() = %d, want %d", got, tt.want)
			}
		})
	}
}
