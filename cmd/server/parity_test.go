package main

// parity_test.go — behaviours the OpenAPI contract tests cannot express:
// the health endpoint's engine value and the multi-observer WebSocket
// broadcast. Response shapes are covered by the openapi_*_contract_test.go
// files, which validate against api/openapi.yaml.

import (
	"encoding/json"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
)

func iptr64(i int64) *int64 { return &i }

// TestParityHealthEngine verifies Go health endpoint declares engine=go
// while Node declares engine=node (or omits it). The Go server must always
// identify itself.
func TestParityHealthEngine(t *testing.T) {
	_, router := setupTestServer(t)

	req := httptest.NewRequest("GET", "/api/health", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)

	engine, ok := body["engine"]
	if !ok {
		t.Error("health response missing 'engine' field (Go server must include engine=go)")
	} else if engine != "go" {
		t.Errorf("health engine=%v, expected 'go'", engine)
	}
}

type wsPacketMessage struct {
	Type string `json:"type"`
	Data struct {
		Hash       string `json:"hash"`
		PathJSON   string `json:"path_json"`
		ObserverID string `json:"observer_id"`
	} `json:"data"`
}

func TestParityWSMultiObserverGolden(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	seedTestData(t, db)
	hub := NewHub()
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	poller := NewPoller(db, store, hub, 50*time.Millisecond)
	poller.store = store

	client := &Client{send: make(chan []byte, 256)}
	hub.Register(client)
	defer hub.Unregister(client)

	go poller.Start(ctx)
	defer poller.Stop()

	time.Sleep(100 * time.Millisecond)

	now := time.Now().UTC()
	senderKey := scopeKey(10)
	obs1Key := scopeKey(11)
	obs2Key := scopeKey(12)

	if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
		PublicKey: senderKey,
		Name:      "Golden Sender",
		Role:      "repeater",
		Lat:       nil,
		Lon:       nil,
		RxTime:    now,
		IngestNow: now,
	}); err != nil {
		t.Fatalf("seed sender node: %v", err)
	}

	obs1Idx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID: obs1Key, Name: ptr("Golden Obs 1"), IATA: "SJC", Meta: nil, LastSeen: now, IngestNow: now,
	})
	if err != nil {
		t.Fatalf("seed observer 1: %v", err)
	}
	obs2Idx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID: obs2Key, Name: ptr("Golden Obs 2"), IATA: "SFO", Meta: nil, LastSeen: now, IngestNow: now,
	})
	if err != nil {
		t.Fatalf("seed observer 2: %v", err)
	}

	const goldenHash = "a1b2c3d4e5f60718"
	txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		Hash:        goldenHash,
		RawHex:      "beef",
		RouteType:   1,
		PayloadType: 4,
		DecodedJSON: `{"pubKey":"` + senderKey + `","type":"ADVERT"}`,
		Advert:      &database.NodeInput{PublicKey: senderKey, Name: "sender", Role: "repeater", RxTime: now, IngestNow: now},
	}, now)
	if err != nil {
		t.Fatalf("seed transmission: %v", err)
	}

	observations := []struct {
		observerIdx int64
		snr         float64
		rssi        float64
		at          time.Time
		hops        []string
		pathHash    string
	}{
		{obs1Idx, 11.0, -88, now, []string{"aa"}, "ph1"},
		{obs2Idx, 9.0, -92, now.Add(time.Second), []string{"aa", "bb"}, "ph2"},
		{obs1Idx, 7.0, -96, now.Add(2 * time.Second), []string{"aa", "bb", "cc"}, "ph3"},
	}
	for i, o := range observations {
		_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
			TransmissionID: txID,
			ObserverIdx:    iptr64(o.observerIdx),
			SNR:            fptr(o.snr),
			RSSI:           fptr(o.rssi),
			PathHash:       o.pathHash,
			Timestamp:      o.at,
		}, o.hops, nil)
		if err != nil {
			t.Fatalf("seed observation %d: %v", i, err)
		}
		if !inserted {
			t.Fatalf("expected observation to be inserted for tx %d", txID)
		}
	}

	type golden struct {
		Hash        string
		Count       int
		Paths       []string
		ObserverIDs []string
	}
	expected := golden{
		Hash:        goldenHash,
		Count:       3,
		Paths:       []string{`["AA"]`, `["AA","BB"]`, `["AA","BB","CC"]`},
		ObserverIDs: []string{obs1Key, obs2Key},
	}

	gotPaths := make([]string, 0, expected.Count)
	gotObservers := make(map[string]bool)
	deadline := time.After(2 * time.Second)
	for len(gotPaths) < expected.Count {
		select {
		case raw := <-client.send:
			var msg wsPacketMessage
			if err := json.Unmarshal(raw, &msg); err != nil {
				t.Fatalf("unmarshal ws message failed: %v", err)
			}
			if msg.Type != "packet" || msg.Data.Hash != expected.Hash {
				continue
			}
			if msg.Data.PathJSON != "" {
				gotPaths = append(gotPaths, msg.Data.PathJSON)
			}
			if msg.Data.ObserverID != "" {
				gotObservers[msg.Data.ObserverID] = true
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %d ws messages, got %d", expected.Count, len(gotPaths))
		}
	}

	sort.Strings(gotPaths)
	sort.Strings(expected.Paths)
	if len(gotPaths) != len(expected.Paths) {
		t.Fatalf("path count mismatch: got %d want %d", len(gotPaths), len(expected.Paths))
	}
	for i := range expected.Paths {
		if gotPaths[i] != expected.Paths[i] {
			t.Fatalf("path mismatch at %d: got %q want %q", i, gotPaths[i], expected.Paths[i])
		}
	}
	for _, oid := range expected.ObserverIDs {
		if !gotObservers[oid] {
			t.Fatalf("missing expected observer %q in ws messages", oid)
		}
	}
}
