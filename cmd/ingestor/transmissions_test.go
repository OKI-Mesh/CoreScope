package main

import (
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/types"
)

func TestInsertTransmission(t *testing.T) {
	s := newTestStore(t)
	ctx := t.Context()

	// The observer has to exist before its packets arrive (the real ingest path
	// upserts it first). Without it observer_idx is NULL, and NULLs never
	// conflict in a unique index, so re-inserting the same packet would add a
	// second observation instead of updating the first.
	if err := s.UpsertObserver(ctx, "obs1", "Observer1", "SJC", nil); err != nil {
		t.Fatal(err)
	}

	snr := 5.5
	rssi := -100.0
	rxTime := time.Date(2026, 3, 25, 0, 0, 0, 0, time.UTC)
	data := &PacketData{
		RawHex:         "0A00D69FD7A5A7475DB07337749AE61FA53A4788E976",
		Timestamp:      rxTime,
		ObserverID:     "obs1",
		Hash:           "abcdef1234567890",
		RouteType:      types.RouteDirect,
		PayloadType:    types.PayloadTXT_MSG,
		PayloadVersion: 0,
		PathJSON:       "[]",
		DecodedJSON:    `{"type":"TXT_MSG"}`,
		SNR:            &snr,
		RSSI:           &rssi,
	}

	isNew, err := s.InsertTransmission(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	if !isNew {
		t.Error("first insert should report a new transmission")
	}

	if n := countRows(t, s, "transmissions"); n != 1 {
		t.Errorf("transmissions count=%d, want 1", n)
	}
	if n := countRows(t, s, "observations"); n != 1 {
		t.Errorf("observations count=%d, want 1", n)
	}

	// Read back what was stored. This also round-trips the RouteType and
	// PayloadType sqlc overrides.
	var routeType types.RouteType
	var payloadType types.PayloadType
	var firstSeen time.Time
	mustScan(t, testRow(t, s,
		`SELECT route_type, payload_type, first_seen FROM transmissions WHERE hash = $1`, data.Hash),
		&routeType, &payloadType, &firstSeen)
	if routeType != types.RouteDirect || payloadType != types.PayloadTXT_MSG {
		t.Errorf("route/payload = %v/%v, want %v/%v", routeType, payloadType, types.RouteDirect, types.PayloadTXT_MSG)
	}
	if !firstSeen.Equal(rxTime) {
		t.Errorf("first_seen = %v, want %v", firstSeen, rxTime)
	}
	var gotSNR, gotRSSI *float64
	mustScan(t, testRow(t, s,
		`SELECT snr, rssi FROM observations
		 WHERE transmission_id = (SELECT id FROM transmissions WHERE hash = $1)`, data.Hash),
		&gotSNR, &gotRSSI)
	if gotSNR == nil || *gotSNR != snr || gotRSSI == nil || *gotRSSI != rssi {
		t.Errorf("snr/rssi = %v/%v, want %v/%v", gotSNR, gotRSSI, snr, rssi)
	}

	// Verify hash dedup: the same hash must not create a new transmission, and
	// the same observer + path updates its observation instead of adding one.
	isNew, err = s.InsertTransmission(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	if isNew {
		t.Error("same hash must not report a new transmission")
	}
	if n := countRows(t, s, "transmissions"); n != 1 {
		t.Errorf("transmissions count after dedup=%d, want 1", n)
	}
	if n := countRows(t, s, "observations"); n != 1 {
		t.Errorf("observations count after dedup=%d, want 1", n)
	}
	// Counted exactly once (a double increment slipped into this path before).
	if d := s.Stats.DuplicateTransmissions.Load(); d != 1 {
		t.Errorf("DuplicateTransmissions = %d, want 1", d)
	}
}

func TestIssue1690_LastSeenUpdatedOnObservation(t *testing.T) {
	s := newTestStore(t)
	defer s.Close()
	ctx := t.Context()

	obs1 := strings.Repeat("e1", 32)
	obs2 := strings.Repeat("e2", 32)
	seedObserver(t, s, obs1, 0)
	seedObserver(t, s, obs2, 0)

	hash := "1690cafebabe0001"
	weekAgo := time.Now().UTC().Add(-7 * 24 * time.Hour).Truncate(time.Second)
	snr, rssi := 5.5, -100.0

	first := &PacketData{
		RawHex:         "0A00",
		Timestamp:      weekAgo,
		ObserverID:     obs1,
		Hash:           hash,
		RouteType:      2,
		PayloadType:    2,
		PayloadVersion: 0,
		PathJSON:       "[]",
		DecodedJSON:    `{"type":"TXT_MSG"}`,
		SNR:            &snr,
		RSSI:           &rssi,
	}
	if _, err := s.InsertTransmission(ctx, first); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	var seededLastSeen time.Time
	mustScan(t, testRow(t, s, "SELECT last_seen FROM transmissions WHERE hash = $1", hash), &seededLastSeen)
	if !seededLastSeen.Equal(weekAgo) {
		t.Logf("seed last_seen=%v expected %v (allowed for fresh column)", seededLastSeen, weekAgo)
	}

	nowSec := time.Now().UTC()
	second := &PacketData{
		RawHex:         "0A00",
		Timestamp:      nowSec,
		ObserverID:     obs2,
		Hash:           hash,
		RouteType:      2,
		PayloadType:    2,
		PayloadVersion: 0,
		PathJSON:       "[]",
		DecodedJSON:    `{"type":"TXT_MSG"}`,
		SNR:            &snr,
		RSSI:           &rssi,
	}
	if _, err := s.InsertTransmission(ctx, second); err != nil {
		t.Fatalf("second insert: %v", err)
	}

	var ls time.Time
	mustScan(t, testRow(t, s, "SELECT last_seen FROM transmissions WHERE hash = $1", hash), &ls)
	if ls.Before(nowSec.Add(-2 * time.Second)) {
		t.Errorf("transmissions.last_seen=%v after fresh observation; expected ≥ %v (a recent unix-second). "+
			"Pre-fix the column is never updated on re-observation — the original cold-load bug (#1690).",
			ls, nowSec)
	}
}
