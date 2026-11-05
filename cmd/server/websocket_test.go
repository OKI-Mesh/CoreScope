package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/gorilla/websocket"
)

func TestHubBroadcast(t *testing.T) {
	hub := NewHub()

	if hub.ClientCount() != 0 {
		t.Errorf("expected 0 clients, got %d", hub.ClientCount())
	}

	// Create a test server with WebSocket endpoint
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hub.ServeWS(w, r)
	}))
	defer srv.Close()

	// Connect a WebSocket client
	wsURL := "ws" + srv.URL[4:] // replace http with ws
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	defer conn.Close()

	// Wait for registration
	time.Sleep(50 * time.Millisecond)

	if hub.ClientCount() != 1 {
		t.Errorf("expected 1 client, got %d", hub.ClientCount())
	}

	// Broadcast a message
	hub.Broadcast(map[string]interface{}{
		"type": "packet",
		"data": map[string]interface{}{"id": 1, "hash": "test123"},
	})

	// Read the message
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if len(msg) == 0 {
		t.Error("expected non-empty message")
	}

	// Disconnect
	conn.Close()
	time.Sleep(100 * time.Millisecond)
}

func TestPollerCreation(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)
	store := newPacketStoreForTest(t, db)
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache: %v", err)
	}
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}
	hub := NewHub()

	poller := NewPoller(db, store, hub, 100*time.Millisecond)
	if poller == nil {
		t.Fatal("expected poller")
	}

	done := make(chan struct{})
	go func() {
		poller.Start(ctx)
		close(done)
	}()
	time.Sleep(200 * time.Millisecond)
	poller.Stop()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("poller did not stop")
	}
}

func TestHubMultipleClients(t *testing.T) {
	hub := NewHub()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hub.ServeWS(w, r)
	}))
	defer srv.Close()

	wsURL := "ws" + srv.URL[4:]

	// Connect two clients
	conn1, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	defer conn1.Close()

	conn2, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	defer conn2.Close()

	time.Sleep(100 * time.Millisecond)

	if hub.ClientCount() != 2 {
		t.Errorf("expected 2 clients, got %d", hub.ClientCount())
	}

	// Broadcast and both should receive
	hub.Broadcast(map[string]interface{}{"type": "test", "data": "hello"})

	conn1.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, msg1, err := conn1.ReadMessage()
	if err != nil {
		t.Fatalf("conn1 read error: %v", err)
	}
	if len(msg1) == 0 {
		t.Error("expected non-empty message on conn1")
	}

	conn2.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, msg2, err := conn2.ReadMessage()
	if err != nil {
		t.Fatalf("conn2 read error: %v", err)
	}
	if len(msg2) == 0 {
		t.Error("expected non-empty message on conn2")
	}

	// Disconnect one
	conn1.Close()
	time.Sleep(100 * time.Millisecond)

	// Remaining client should still work
	hub.Broadcast(map[string]interface{}{"type": "test2"})

	conn2.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, msg3, err := conn2.ReadMessage()
	if err != nil {
		t.Fatalf("conn2 read error after disconnect: %v", err)
	}
	if len(msg3) == 0 {
		t.Error("expected non-empty message")
	}
}

func TestBroadcastFullBuffer(t *testing.T) {
	hub := NewHub()

	// Create a client with tiny buffer (1)
	client := &Client{
		send: make(chan []byte, 1),
	}
	hub.mu.Lock()
	hub.clients[client] = true
	hub.mu.Unlock()

	// Fill the buffer
	client.send <- []byte("first")

	// This broadcast should drop the message (buffer full)
	hub.Broadcast(map[string]interface{}{"type": "dropped"})

	// Channel should still only have the first message
	select {
	case msg := <-client.send:
		if string(msg) != "first" {
			t.Errorf("expected 'first', got %s", string(msg))
		}
	default:
		t.Error("expected message in channel")
	}

	// Clean up
	hub.mu.Lock()
	delete(hub.clients, client)
	hub.mu.Unlock()
}

func TestBroadcastMarshalError(t *testing.T) {
	hub := NewHub()

	// Marshal error: functions can't be marshaled to JSON
	hub.Broadcast(map[string]interface{}{"bad": func() {}})
	// Should not panic — just log and return
}

func TestPollerBroadcastsNewData(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	hub := NewHub()

	client := &Client{
		send: make(chan []byte, 256),
	}
	hub.mu.Lock()
	hub.clients[client] = true
	hub.mu.Unlock()
	defer func() {
		hub.mu.Lock()
		delete(hub.clients, client)
		hub.mu.Unlock()
	}()

	now := time.Now().UTC().Truncate(time.Second)
	obsIdx := seedObserver(t, db, padKey("0b51"), "BRU", now)

	store := newPacketStoreForTest(t, db)
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache: %v", err)
	}
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}

	poller := NewPoller(db, store, hub, 50*time.Millisecond)
	go poller.Start(ctx)
	defer poller.Stop()
	time.Sleep(100 * time.Millisecond)

	txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		Hash:        "ee11ff22aabb3344",
		RawHex:      "EEFF",
		RouteType:   types.RouteFlood,
		PayloadType: types.PayloadType(4),
	}, now)
	if err != nil {
		t.Fatalf("insert tx: %v", err)
	}
	_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: txID,
		ObserverIdx:    &obsIdx,
		SNR:            fptr(5.0),
		RSSI:           fptr(-90.0),
		Timestamp:      now,
	}, []string{"aa"}, []*string{nil})
	if err != nil {
		t.Fatalf("insert observation: %v", err)
	}
	if !inserted {
		t.Fatalf("expected observation to be inserted for tx %d", txID)
	}

	type broadcast struct {
		Type string `json:"type"`
		Data struct {
			Packet map[string]json.RawMessage `json:"packet"`
		} `json:"data"`
	}

	select {
	case msg := <-client.send:
		if len(msg) == 0 {
			t.Fatal("expected non-empty broadcast message")
		}
		var parsed broadcast
		if err := json.Unmarshal(msg, &parsed); err != nil {
			t.Fatalf("failed to parse broadcast: %v", err)
		}
		if parsed.Type != "packet" {
			t.Errorf("expected type=packet, got %v", parsed.Type)
		}
		if parsed.Data.Packet == nil {
			t.Fatal("expected data.packet to exist (required by packets.js WS handler)")
		}
		for _, field := range []string{"id", "hash", "payload_type", "timestamp"} {
			if _, exists := parsed.Data.Packet[field]; !exists {
				t.Errorf("expected data.packet.%s to exist", field)
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for broadcast")
	}
}

func TestPollerBroadcastsMultipleObservations(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	hub := NewHub()

	client := &Client{
		send: make(chan []byte, 256),
	}
	hub.mu.Lock()
	hub.clients[client] = true
	hub.mu.Unlock()
	defer func() {
		hub.mu.Lock()
		delete(hub.clients, client)
		hub.mu.Unlock()
	}()

	now := time.Now().UTC().Truncate(time.Second)
	obsIdx1 := seedObserver(t, db, padKey("0b51"), "BRU", now)
	obsIdx2 := seedObserver(t, db, padKey("0b52"), "GNT", now)

	store := newPacketStoreForTest(t, db)
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache: %v", err)
	}
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}

	poller := NewPoller(db, store, hub, 50*time.Millisecond)
	go poller.Start(ctx)
	defer poller.Stop()

	time.Sleep(100 * time.Millisecond)

	const hash = "57a2b0000000237a"
	txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		Hash:        hash,
		RawHex:      "FACE",
		RouteType:   types.RouteFlood,
		PayloadType: types.PayloadType(4),
		DecodedJSON: `{"pubKey":"` + padKey("aabbccdd11223344") + `","type":"ADVERT"}`,
	}, now)
	if err != nil {
		t.Fatalf("insert tx failed: %v", err)
	}

	type pollerPacketMessage struct {
		Type string `json:"type"`
		Data struct {
			Hash       string `json:"hash"`
			PathJSON   string `json:"path_json"`
			ObserverID string `json:"observer_id"`
		} `json:"data"`
	}

	waitForPath := func(wantPath string) pollerPacketMessage {
		t.Helper()
		deadline := time.After(2 * time.Second)
		for {
			select {
			case raw := <-client.send:
				var parsed pollerPacketMessage
				if err := json.Unmarshal(raw, &parsed); err != nil {
					t.Fatalf("unmarshal ws msg failed: %v", err)
				}
				t.Logf("ws msg: type=%s hash=%s path=%q observer=%q raw=%.300s", parsed.Type, parsed.Data.Hash, parsed.Data.PathJSON, parsed.Data.ObserverID, raw)
				if parsed.Type == "packet" && parsed.Data.Hash == hash && parsed.Data.PathJSON == wantPath {
					return parsed
				}
			case <-deadline:
				t.Fatalf("timed out waiting for a broadcast with path %s", wantPath)
			}
		}
	}

	observations := []struct {
		observerIdx int64
		snr         float64
		rssi        float64
		hops        []string
		pathHash    string
		wantPath    string
		at          time.Time
	}{
		{obsIdx1, 14.0, -82, []string{"aa"}, "h1", `["AA"]`, now},
		{obsIdx2, 10.5, -90, []string{"aa", "bb"}, "h2", `["AA","BB"]`, now.Add(time.Second)},
		{obsIdx1, 7.0, -96, []string{"aa", "bb", "cc"}, "h3", `["AA","BB","CC"]`, now.Add(2 * time.Second)},
	}

	observers := make(map[string]bool)
	for _, o := range observations {
		idx := o.observerIdx
		_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
			TransmissionID: txID,
			ObserverIdx:    &idx,
			SNR:            fptr(o.snr),
			RSSI:           fptr(o.rssi),
			PathHash:       o.pathHash,
			Timestamp:      o.at,
		}, o.hops, make([]*string, len(o.hops)))
		if err != nil {
			t.Fatalf("insert observation failed: %v", err)
		}
		if !inserted {
			t.Fatalf("expected observation to be inserted for tx %d", txID)
		}
		msg := waitForPath(o.wantPath)
		if msg.Data.ObserverID != "" {
			observers[msg.Data.ObserverID] = true
		}
	}

	if len(observers) < 2 {
		t.Fatalf("expected observations from >=2 observers, got %d", len(observers))
	}
}

func TestIngestNewObservationsBroadcast(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	obsIdx1 := seedObserver(t, db, padKey("0b51"), "BRU", now)
	obsIdx2 := seedObserver(t, db, padKey("0b52"), "GNT", now)

	const hash = "abc123def4567890"
	txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		Hash:        hash,
		RawHex:      "0a1b2c",
		RouteType:   types.RouteFlood,
		PayloadType: types.PayloadType(4),
		DecodedJSON: `{"type":"ADVERT"}`,
	}, now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("insert tx: %v", err)
	}
	idx1 := obsIdx1
	_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: txID,
		ObserverIdx:    &idx1,
		SNR:            fptr(12.5),
		RSSI:           fptr(-90),
		PathHash:       "h-initial",
		Timestamp:      now.Add(-time.Hour),
	}, []string{"aa"}, []*string{nil})
	if err != nil {
		t.Fatalf("insert initial observation: %v", err)
	}
	if !inserted {
		t.Fatalf("expected initial observation to be inserted for tx %d", txID)
	}

	store := newPacketStoreForTest(t, db)
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache: %v", err)
	}
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}

	maxObs, err := db.GetMaxObservationID(ctx)
	if err != nil {
		t.Fatalf("GetMaxObservationID(): %v", err)
	}

	idx2 := obsIdx2
	_, inserted, err = db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: txID,
		ObserverIdx:    &idx2,
		SNR:            fptr(6.0),
		RSSI:           fptr(-100),
		PathHash:       "h-new-1",
		Timestamp:      now,
	}, []string{"aa", "bb"}, []*string{nil, nil})
	if err != nil {
		t.Fatalf("insert new observation 1: %v", err)
	}
	if !inserted {
		t.Fatalf("expected new observation 1 to be inserted for tx %d", txID)
	}
	_, inserted, err = db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: txID,
		ObserverIdx:    &idx1,
		SNR:            fptr(5.0),
		RSSI:           fptr(-101),
		PathHash:       "h-new-2",
		Timestamp:      now.Add(time.Second),
	}, []string{"aa", "cc"}, []*string{nil, nil})
	if err != nil {
		t.Fatalf("insert new observation 2: %v", err)
	}
	if !inserted {
		t.Fatalf("expected new observation 2 to be inserted for tx %d", txID)
	}

	packets, _, err := store.IngestNewObservations(ctx, maxObs, 500)
	if err != nil {
		t.Fatalf("IngestNewObservations(): %v", err)
	}
	if len(packets) != 2 {
		t.Fatalf("expected 2 broadcast packets, got %d", len(packets))
	}
	for _, packet := range packets {
		if packet.Hash == nil || *packet.Hash != hash {
			t.Fatalf("unexpected hash in packet: %v", packet.Hash)
		}
		if packet.PathJSON == nil || *packet.PathJSON == "" {
			t.Fatalf("missing path_json in packet: %#v", packet)
		}
		if packet.ObserverID == nil {
			t.Fatalf("missing observer_id in packet: %#v", packet)
		}
	}
	gotPaths := map[string]bool{}
	for _, packet := range packets {
		gotPaths[*packet.PathJSON] = true
	}
	for _, want := range []string{`["AA","BB"]`, `["AA","CC"]`} {
		if !gotPaths[want] {
			t.Errorf("missing broadcast with path %s (got %v)", want, gotPaths)
		}
	}
}

func TestHubRegisterUnregister(t *testing.T) {
	hub := NewHub()

	client := &Client{
		send: make(chan []byte, 256),
	}

	hub.Register(client)
	if hub.ClientCount() != 1 {
		t.Errorf("expected 1 client after register, got %d", hub.ClientCount())
	}

	hub.Unregister(client)
	if hub.ClientCount() != 0 {
		t.Errorf("expected 0 clients after unregister, got %d", hub.ClientCount())
	}

	// Unregister again should be safe
	hub.Unregister(client)
	if hub.ClientCount() != 0 {
		t.Errorf("expected 0 clients, got %d", hub.ClientCount())
	}
}

// #1074: browsers cannot see protocol-level pings, so a page cannot tell a
// quiet mesh from a dead socket unless the server also sends something the
// page receives. The heartbeat rides the same ticker as the ping.
func TestWritePumpSendsAppHeartbeat(t *testing.T) {
	hub := NewHub()
	hub.pingInterval = 20 * time.Millisecond

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hub.ServeWS(w, r)
	}))
	defer srv.Close()

	conn, _, err := websocket.DefaultDialer.Dial("ws"+srv.URL[4:], nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	kind, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("expected a heartbeat with no broadcast traffic, got read error: %v", err)
	}
	if kind != websocket.TextMessage {
		t.Fatalf("heartbeat must be a text frame so page JS receives it, got frame type %d", kind)
	}
	// public/app.js matches these exact bytes to keep heartbeats away from
	// page listeners; changing them breaks that match.
	if string(msg) != `{"type":"heartbeat"}` {
		t.Fatalf("heartbeat bytes changed: %s", msg)
	}
}

// The client's stale threshold (WS_STALE_MS in public/app.js, 75s) assumes
// a heartbeat at least every 30s.
func TestHubDefaultPingInterval(t *testing.T) {
	if got := NewHub().pingInterval; got != 30*time.Second {
		t.Fatalf("pingInterval = %v, want 30s (public/app.js WS_STALE_MS depends on it)", got)
	}
}
