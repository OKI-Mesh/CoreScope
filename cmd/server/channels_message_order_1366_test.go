package main

// Regression tests for issue #1366: Channel view shows stale timestamps
// because GetChannelMessages emits tx.FirstSeen (first-observation time)
// when the operator-visible expectation is the latest observation time
// (tx.LatestSeen). For repeated heartbeat-style messages whose tx.Hash is
// stable, FirstSeen stays pinned to the very first observation while the
// real-world transmission keeps repeating, producing a multi-hour gap
// between the channel view and the operator's live MeshCore client.
//
// Server-side UTC clocks are trusted; client-reported sender_timestamp
// is NOT (firmware lacks reliable wall-clock on many builds). Therefore
// the fix uses tx.LatestSeen (== max observation timestamp), NOT
// sender_timestamp. sender_timestamp remains exposed in the response
// for debug surfaces but MUST NOT be the rendered field.

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// TestChannelMessages_TimestampUsesLatestSeen: a CHAN tx with multiple
// observations spanning hours must render with the LATEST observation
// timestamp, not the first-seen ingest time.
func TestChannelMessages_TimestampUsesLatestSeen(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	firstSeenAt := now.Add(-7 * time.Hour)
	laterAt := now.Add(-5 * time.Minute)
	firstSeenEpoch := firstSeenAt.Unix()
	laterEpoch := laterAt.Unix()

	obsA, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID: "obsa", Name: "ObsA", IATA: "SJC",
		LastSeen: firstSeenAt, IngestNow: firstSeenAt,
	})
	if err != nil {
		t.Fatalf("create observer obsa: %v", err)
	}
	obsB, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID: "obsb", Name: "ObsB", IATA: "LAX",
		LastSeen: firstSeenAt, IngestNow: firstSeenAt,
	})
	if err != nil {
		t.Fatalf("create observer obsb: %v", err)
	}

	txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		RawHex:      "AA01",
		Hash:        "hash_repeated_msg",
		RouteType:   types.RouteType(1),
		PayloadType: types.PayloadType(5),
		DecodedJSON: `{"type":"CHAN","channel":"#test","text":"Heartbeat: ping","sender":"Heartbeat","sender_timestamp":` +
			strconv.FormatInt(firstSeenEpoch, 10) + `}`,
		ChannelHash: "#test",
	}, firstSeenAt)
	if err != nil {
		t.Fatalf("insert tx: %v", err)
	}
	for _, o := range []struct {
		observer  int64
		snr, rssi float64
		at        time.Time
		hop       string
	}{
		{obsA, 10.0, -90, firstSeenAt, "aa"},
		{obsB, 11.0, -88, laterAt, "bb"},
	} {
		_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
			TransmissionID: txID,
			ObserverIdx:    ptr(o.observer),
			Direction:      ptr("RX"),
			SNR:            ptr(o.snr),
			RSSI:           ptr(o.rssi),
			Timestamp:      o.at,
		}, []string{o.hop}, nil)
		if err != nil {
			t.Fatalf("insert observation (hop %s): %v", o.hop, err)
		}
		if !inserted {
			t.Fatalf("expected observation to be inserted for tx %d (hop %s)", txID, o.hop)
		}
	}

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	msgs, total := store.GetChannelMessages(ctx, "#test", 10, 0)
	t.Logf("msg0=%#v", msgs[0])
	if total != 1 {
		t.Fatalf("want 1 msg, got %d (msgs=%+v)", total, msgs)
	}
	ts, ok := msgs[0]["timestamp"].(UTCTime)
	if !ok {
		t.Fatalf("timestamp has type %T, want UTCTime", msgs[0]["timestamp"])
	}
	if delta := ts.Unix() - laterEpoch; delta < -1 || delta > 1 {
		t.Errorf("timestamp: want ~%s (LatestSeen, observation at T-5m), got %s (Δ=%ds, likely FirstSeen, issue #1366)",
			laterAt.Format(time.RFC3339), formatTS(ts.Time), delta)
	}

	fs, ok := msgs[0]["first_seen"].(UTCTime)
	if !ok || fs.IsZero() {
		t.Fatalf("first_seen must be exposed alongside timestamp; got %#v", msgs[0]["first_seen"])
	}
	if fs.Equal(ts.Time) {
		t.Errorf("first_seen should differ from latest-seen timestamp (both = %s)", formatTS(ts.Time))
	}
}

// TestChannelMessages_TimestampNotSenderTimestamp: a CHAN tx whose
// decoded sender_timestamp is wildly off (e.g. client with bad RTC)
// must NOT cause the rendered timestamp to drift. Rendered timestamp
// must remain server UTC (LatestSeen/FirstSeen), regardless of what
// the client claimed.
func TestChannelMessages_TimestampNotSenderTimestamp(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	firstSeenAt := now.Add(-10 * time.Minute)

	badSenderTs := int64(946684800)

	obsX, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID: "obsx", Name: "ObsX", IATA: "SJC",
		LastSeen: firstSeenAt, IngestNow: firstSeenAt,
	})
	if err != nil {
		t.Fatalf("create observer: %v", err)
	}

	txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		RawHex:      "BB01",
		Hash:        "hash_bad_clock",
		RouteType:   types.RouteType(1),
		PayloadType: types.PayloadType(5),
		DecodedJSON: `{"type":"CHAN","channel":"#bad","text":"Alice: ping","sender":"Alice","sender_timestamp":` +
			strconv.FormatInt(badSenderTs, 10) + `}`,
		ChannelHash: "#bad",
	}, firstSeenAt)
	if err != nil {
		t.Fatalf("insert tx: %v", err)
	}
	_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: txID,
		ObserverIdx:    ptr(obsX),
		Direction:      ptr("RX"),
		SNR:            ptr(10.0),
		RSSI:           ptr(-90.0),
		Timestamp:      firstSeenAt,
	}, []string{"aa"}, nil)
	if err != nil {
		t.Fatalf("insert observation: %v", err)
	}
	if !inserted {
		t.Fatalf("expected observation to be inserted for tx %d", txID)
	}

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	msgs, total := store.GetChannelMessages(ctx, "#bad", 10, 0)
	if total != 1 {
		t.Fatalf("want 1 msg, got %d", total)
	}
	ts, ok := msgs[0]["timestamp"].(UTCTime)
	if !ok {
		t.Fatalf("timestamp has type %T, want UTCTime", msgs[0]["timestamp"])
	}
	if ts.Year() < now.Year() {
		t.Errorf("rendered timestamp %s took on the client's bad sender_timestamp (year %d) instead of server UTC",
			formatTS(ts.Time), ts.Year())
	}
	if delta := ts.Sub(firstSeenAt); delta < -time.Second || delta > time.Second {
		t.Errorf("timestamp = %s, want about %s (the observation time)",
			formatTS(ts.Time), firstSeenAt.Format(time.RFC3339))
	}
}

// TestChannelMessages_TimestampIsUTCZ: rendered timestamp MUST end with
// 'Z' (or +00:00) so the browser does NOT interpret it as a local-zone
// string and shift by the operator's TZ offset.
func TestChannelMessages_TimestampIsUTCZ(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	fs := now.Add(-30 * time.Minute)

	obsZ, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        "obsz",
		Name:      "ObsZ",
		IATA:      "SJC",
		LastSeen:  fs,
		IngestNow: now,
	})
	if err != nil {
		t.Fatalf("create observer: %v", err)
	}

	txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		RawHex:      "AA01",
		Hash:        "hash_zone_check",
		RouteType:   types.RouteType(1),
		PayloadType: types.PayloadType(5),
		DecodedJSON: `{"type":"CHAN","channel":"#zone","text":"Carol: ping","sender":"Carol"}`,
		ChannelHash: "#zone",
	}, fs)
	if err != nil {
		t.Fatalf("insert tx: %v", err)
	}
	_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: txID,
		ObserverIdx:    ptr(obsZ),
		Direction:      ptr("RX"),
		SNR:            ptr(11.0),
		RSSI:           ptr(-89.0),
		Timestamp:      fs,
	}, []string{"aa"}, nil)
	if err != nil {
		t.Fatalf("insert observation: %v", err)
	}
	if !inserted {
		t.Fatalf("expected observation to be inserted for tx %d", txID)
	}

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	msgs, _ := store.GetChannelMessages(ctx, "#zone", 10, 0)
	if len(msgs) != 1 {
		t.Fatalf("want 1 msg, got %d", len(msgs))
	}
	encoded, err := json.Marshal(msgs[0])
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var wire struct {
		Timestamp string `json:"timestamp"`
		FirstSeen string `json:"first_seen"`
	}
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for name, value := range map[string]string{"timestamp": wire.Timestamp, "first_seen": wire.FirstSeen} {
		if value == "" {
			t.Fatalf("%s is empty in %s", name, encoded)
		}
		if !(strings.HasSuffix(value, "Z") || strings.HasSuffix(value, "+00:00")) {
			t.Errorf("%s not UTC-suffixed (Z/+00:00): %q", name, value)
		}
	}
}

// TestChannelMessages_OrderedByLatestSeen: adversarial follow-up to #1366
// (PR #1368). The earlier fix only adjusted the rendered `timestamp`
// field; page SELECTION and SORT ORDER on both the in-memory and DB
// paths still used FirstSeen. This test pins the contract:
//
//   - tx-A: FirstSeen 24h ago, LatestSeen NOW (via a fresh observation).
//   - tx-B: FirstSeen 1h ago, LatestSeen 1h ago (single observation).
//
// Both paths MUST:
//  1. Return BOTH transmissions in a small (limit=10) page — tx-A must
//     not be excluded because its FirstSeen is old.
//  2. Return tx-A AFTER tx-B (newest-LatestSeen-LAST), matching the
//     tail-of-msgOrder convention used by the rest of the API and
//     the frontend's scrollToBottom().
func TestChannelMessages_OrderedByLatestSeen_InMemory(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	tOld := now.Add(-24 * time.Hour)
	tMid := now.Add(-1 * time.Hour)
	tNewest := now.Add(-30 * time.Minute)
	tFresh := now.Add(-1 * time.Minute)
	obsO, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID: "obso", Name: "ObsO", IATA: "SJC",
		LastSeen: tOld, IngestNow: now,
	})
	if err != nil {
		t.Fatalf("create observer obso: %v", err)
	}
	obsP, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID: "obsp", Name: "ObsP", IATA: "SJC",
		LastSeen: tOld, IngestNow: now,
	})
	if err != nil {
		t.Fatalf("create observer obsp: %v", err)
	}

	addTx := func(raw, hash, sender, text string, firstSeen time.Time) int64 {
		t.Helper()
		id, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
			RawHex:      raw,
			Hash:        hash,
			RouteType:   types.RouteType(1),
			PayloadType: types.PayloadType(5),
			DecodedJSON: `{"type":"CHAN","channel":"#ord","text":"` + sender + `: ` + text +
				`","sender":"` + sender + `"}`,
			ChannelHash: "#ord",
		}, firstSeen)
		if err != nil {
			t.Fatalf("insert tx %s: %v", hash, err)
		}
		return id
	}
	addObs := func(txID, observer int64, snr, rssi float64, at time.Time, hop string) {
		t.Helper()
		_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
			TransmissionID: txID,
			ObserverIdx:    ptr(observer),
			Direction:      ptr("RX"),
			SNR:            ptr(snr),
			RSSI:           ptr(rssi),
			Timestamp:      at,
		}, []string{hop}, nil)
		if err != nil {
			t.Fatalf("insert observation (hop %s): %v", hop, err)
		}
		if !inserted {
			t.Fatalf("expected observation to be inserted for tx %d (hop %s)", txID, hop)
		}
	}

	// tx-A: FirstSeen 24h ago, LatestSeen NOW (T-1m). Old insertion order.
	txA := addTx("AAAA", "order_hash_a", "Alpha", "hb", tOld)
	addObs(txA, obsO, 10.0, -90, tOld, "aa")
	addObs(txA, obsP, 11.0, -88, tFresh, "aa")

	// tx-B: FirstSeen 1h ago, LatestSeen 1h ago. OLDEST.
	txB := addTx("BBBB", "order_hash_b", "Bravo", "msg", tMid)
	addObs(txB, obsO, 9.0, -91, tMid, "bb")

	// tx-C: FirstSeen 30m ago, LatestSeen 30m ago. Middle.
	txC := addTx("CCCC", "order_hash_c", "Charlie", "msg", tNewest)
	addObs(txC, obsO, 9.0, -91, tNewest, "cc")

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	// TEMP DIAGNOSTIC
	for _, tx := range store.byPayloadType[5] {
		t.Logf("tx hash=%s FirstSeen=%s LatestSeen=%s", tx.Hash, tx.FirstSeen, tx.LatestSeen)
	}

	// Full-page: ordering check (fix #1 gates this — without sort,
	// msgOrder is insertion order and Alpha lands FIRST, not LAST).
	msgsAll, totalAll := store.GetChannelMessages(ctx, "#ord", 10, 0)
	if totalAll != 3 {
		t.Fatalf("in-memory: want total=3, got %d", totalAll)
	}
	if len(msgsAll) != 3 {
		t.Fatalf("in-memory: want 3 msgs, got %d", len(msgsAll))
	}
	wantOrder := []string{"Bravo", "Charlie", "Alpha"}
	for i, want := range wantOrder {
		got, _ := msgsAll[i]["sender"].(string)
		if got != want {
			t.Errorf("in-memory: msg[%d] want sender=%q, got %q (LatestSeen ASC, fix #1)", i, want, got)
		}
	}

	// Small page (limit=2): tx-A (Alpha) MUST be included because its
	// LatestSeen is freshest, even though FirstSeen is oldest. Without
	// fix #1, the in-memory path takes msgOrder[total-2:] which would
	// drop Alpha (it sits at msgOrder[0] by insertion order).
	msgsPage, _ := store.GetChannelMessages(ctx, "#ord", 2, 0)
	if len(msgsPage) != 2 {
		t.Fatalf("in-memory: want 2 msgs at limit=2, got %d", len(msgsPage))
	}
	hasAlpha := false
	for _, m := range msgsPage {
		if s, _ := m["sender"].(string); s == "Alpha" {
			hasAlpha = true
		}
	}
	if !hasAlpha {
		t.Errorf("in-memory: tx-A (Alpha) excluded from limit=2 page — FirstSeen-based tail selection bug (fix #1 reverted?)")
	}
}
