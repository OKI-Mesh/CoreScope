package main

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/gorilla/mux"
)

// Issue #1999: the packet-detail API returned the transmission's canonical
// frame for every observation, so the browser's hex view could not show the
// selected observation's actual bytes and could contradict the path_json shown
// beside it.
//
// The store drops observations.raw_hex on purpose (#881, ~98MB on a 1.7M
// observation store) on the assumption that one content hash means one frame.
// The firmware hashes payload and type independently of the relay path, so
// observations of a single transmission legitimately differ. Measured on a
// production DB: of the 3000 most recent transmissions, 1977 had more than one
// observation and 1844 of those held genuinely different frames, up to 51 for
// one packet.
//
// The fix reads them back on the detail path only, one query per request.

// seedDistinctFrames inserts one transmission and three observations whose
// stored frames differ in their path bytes, plus one with no stored frame at
// all so the canonical fallback stays covered. Returns the hash and the
// observation ids in insertion order.
func seedDistinctFrames(t *testing.T, db *database.DB, hash string) (string, []int64) {
	t.Helper()
	ctx := t.Context()
	now := time.Now().UTC().Truncate(time.Millisecond)

	observerIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        "frames-observer",
		Name:      ptr("frames-observer"),
		LastSeen:  now,
		IngestNow: now,
	})
	if err != nil {
		t.Fatalf("seed observer: %v", err)
	}

	txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		RawHex:      "CA000000",
		Hash:        hash,
		RouteType:   types.RouteType(1),
		PayloadType: types.PayloadType(5),
		DecodedJSON: `{"type":"CHAN"}`,
	}, now)
	if err != nil {
		t.Fatalf("insert transmission: %v", err)
	}

	frames := []struct {
		hops   []string
		rawHex string
	}{
		{[]string{"aa", "bb"}, "AA0201"},
		{[]string{"aa", "bb", "cc"}, "AA030102"},
		{[]string{"aa"}, "AA01"},
		{[]string{"dd"}, ""},
	}
	for i, f := range frames {
		_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
			TransmissionID: txID,
			ObserverIdx:    ptr(observerIdx),
			Direction:      ptr("RX"),
			SNR:            ptr(5.0),
			RSSI:           ptr(-90.0),
			PathHash:       strings.Join(f.hops, ","),
			Timestamp:      now.Add(-time.Duration(i) * time.Second),
			RawHex:         f.rawHex,
		}, f.hops, nil)
		if err != nil {
			t.Fatalf("insert observation %d: %v", i, err)
		}
		if !inserted {
			t.Fatalf("expected observation to be inserted for tx %d", txID)
		}
	}

	stored, err := db.ListObservationsAfterID(ctx, 0, 100)
	if err != nil {
		t.Fatalf("read back observations: %v", err)
	}
	var ids []int64
	for _, o := range stored {
		if o.TransmissionID == txID {
			ids = append(ids, o.ID)
		}
	}
	if len(ids) != len(frames) {
		t.Fatalf("read back %d observations for tx %d, want %d", len(ids), txID, len(frames))
	}
	return hash, ids
}

func TestObservationRawHexForHash(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	hash, ids := seedDistinctFrames(t, db, "1999aaaabbbbcccc")

	got, err := db.ObservationRawHexForHash(ctx, hash)
	if err != nil {
		t.Fatalf("ObservationRawHexForHash: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d frames, want 3 (the fourth observation stores none): %v", len(got), got)
	}
	want := map[int64]string{ids[0]: "AA0201", ids[1]: "AA030102", ids[2]: "AA01"}
	for id, wantHex := range want {
		if got[id] != wantHex {
			t.Errorf("observation %d: got %q, want %q", id, got[id], wantHex)
		}
	}
	if _, present := got[ids[3]]; present {
		t.Errorf("observation %d has no stored frame and must not appear in the map", ids[3])
	}

	none, err := db.ObservationRawHexForHash(ctx, "0000000000000000")
	if err != nil {
		t.Fatalf("unknown hash must not error: %v", err)
	}
	if len(none) != 0 {
		t.Errorf("unknown hash returned %d frames, want 0", len(none))
	}
}

// TestPacketDetailExposesPerObservationFrames is the regression the issue asks
// for: the detail response must give each observation its own bytes, and fall
// back to the transmission's only where none are stored.
func TestPacketDetailExposesPerObservationFrames(t *testing.T) {
	srv, router := setupTestServer(t)

	// Inserted after the store load, so this transmission is DB-only and the
	// handler takes its DB-fallback path. Both paths converge on the same
	// backfill, and the DB path is the one that can be set up deterministically.
	hash, ids := seedDistinctFrames(t, srv.db, "1999112233445566")

	req := httptest.NewRequest("GET", "/api/packets/"+hash, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("got %d, want 200 (body: %s)", w.Code, w.Body.String())
	}

	var body map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	obsList, _ := body["observations"].([]interface{})
	if len(obsList) != 4 {
		t.Fatalf("got %d observations, want 4", len(obsList))
	}

	byID := map[int64]string{}
	for _, raw := range obsList {
		m, ok := raw.(map[string]interface{})
		if !ok {
			t.Fatalf("observation is not an object: %T", raw)
		}
		idF, ok := m["id"].(float64) // JSON numbers decode as float64
		if !ok {
			t.Fatalf("observation has no numeric id: %v", m["id"])
		}
		hx, _ := m["raw_hex"].(string)
		byID[int64(idF)] = hx
	}

	for i, wantHex := range []string{"AA0201", "AA030102", "AA01"} {
		if got := byID[ids[i]]; got != wantHex {
			t.Errorf("observation %d: raw_hex = %q, want its own frame %q", ids[i], got, wantHex)
		}
	}
	// The one without stored bytes gets the transmission's canonical frame. On
	// this path that is new: the DB observation query selects no raw_hex, so
	// before the fix the field was missing from those observations entirely.
	if got := byID[ids[3]]; got != "CA000000" {
		t.Errorf("observation %d: raw_hex = %q, want the canonical fallback %q", ids[3], got, "CANON0000")
	}

	// The whole point: the frames are not all the same value.
	distinct := map[string]bool{}
	for _, hx := range byID {
		distinct[hx] = true
	}
	if len(distinct) < 3 {
		t.Errorf("only %d distinct frames across 4 observations (%v) — the canonical frame is still being repeated", len(distinct), byID)
	}
}

// TestPacketDetailExposesPerObservationFramesFromStore covers the path almost
// every real request takes: the transmission IS in the in-memory store.
//
// This is the case a DB-fallback-only test misses. enrichObsWithTx already puts
// the transmission's canonical frame into every observation map, so a backfill
// that skipped observations which "already have" raw_hex would skip all of them
// and leave the defect untouched on the main path. A stored frame has to win
// over that placeholder.
func TestPacketDetailExposesPerObservationFramesFromStore(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)

	// Seed BEFORE the store loads, so the store holds this transmission and the
	// handler never reaches its DB fallback.
	hash, ids := seedDistinctFrames(t, db, "1999aabbccdd0011")

	cfg := &Config{Port: 3000}
	srv := NewServer(db, cfg, NewHub())
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	if !store.WaitIndexesReady(5 * time.Second) {
		t.Fatal("background indexes never became ready")
	}
	srv.store = store
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	got, err := srv.store.GetPacketByHash(ctx, hash)
	if err != nil {
		t.Fatalf("GetPacketByHash: %v", err)
	}
	if got == nil {
		t.Fatal("precondition failed: the store does not hold the seeded transmission")
	}

	req := httptest.NewRequest("GET", "/api/packets/"+hash, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("got %d, want 200 (body: %s)", w.Code, w.Body.String())
	}
	var body map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	obsList, _ := body["observations"].([]interface{})
	if len(obsList) != 4 {
		t.Fatalf("got %d observations, want 4", len(obsList))
	}

	byID := map[int64]string{}
	for _, raw := range obsList {
		m, _ := raw.(map[string]interface{})
		idF, ok := m["id"].(float64)
		if !ok {
			t.Fatalf("observation has no numeric id: %v", m["id"])
		}
		hx, _ := m["raw_hex"].(string)
		byID[int64(idF)] = hx
	}

	for i, wantHex := range []string{"AA0201", "AA030102", "AA01"} {
		if got := byID[ids[i]]; got != wantHex {
			t.Errorf("store path, observation %d: raw_hex = %q, want its own frame %q (a canonical value here means the backfill was skipped)", ids[i], got, wantHex)
		}
	}
	if got := byID[ids[3]]; got != "CA000000" {
		t.Errorf("store path, observation %d: raw_hex = %q, want the canonical fallback %q", ids[3], got, "CANON0000")
	}
}
