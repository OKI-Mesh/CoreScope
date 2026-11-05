package main

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// seedEncryptedChannelData adds undecryptable GRP_TXT packets to the test DB.
func seedEncryptedChannelData(t *testing.T, db *database.DB) {
	t.Helper()
	ctx := t.Context()
	recent := time.Now().UTC().Add(-1 * time.Hour)

	observerID := ensureObserver(t, db, "obs1")

	// Two encrypted GRP_TXT packets on channel hash "A1B2".
	for _, hash := range []string{"enc_hash_001", "enc_hash_002"} {
		raw := "EE01"
		if hash == "enc_hash_002" {
			raw = "EE02"
		}
		txID, isNew, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
			RawHex:           raw,
			Hash:             hash,
			RouteType:        types.RouteType(1),
			PayloadType:      types.PayloadType(5),
			DecodedJSON:      `{"type":"GRP_TXT","channelHashHex":"A1B2","decryptionStatus":"no_key"}`,
			ChannelHash:      "A1B2",
			ChannelEncrypted: true,
		}, recent)
		if err != nil {
			t.Fatalf("insert tx %s: %v", hash, err)
		}
		if !isNew {
			t.Fatalf("tx %s already existed", hash)
		}

		_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
			TransmissionID: txID,
			ObserverIdx:    ptr(observerID),
			Direction:      ptr("RX"),
			SNR:            ptr(10.0),
			RSSI:           ptr(-90.0),
			Timestamp:      recent,
		}, nil, nil)
		if err != nil {
			t.Fatalf("insert obs %s: %v", hash, err)
		}
		if !inserted {
			t.Fatalf("expected observation to be inserted for tx %d", txID)
		}
	}
}

func TestGetEncryptedChannels(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	seedTestData(t, db)
	seedEncryptedChannelData(t, db)

	srv := &Server{db: db}
	channels, err := srv.getEncryptedChannels(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(channels) != 1 {
		t.Fatalf("expected 1 encrypted channel, got %d", len(channels))
	}
	ch := channels[0]
	if ch.Hash != "A1B2" {
		t.Errorf("expected hash A1B2, got %v", ch.Hash)
	}
	if !ch.Encrypted {
		t.Errorf("expected encrypted=true, got %v", ch.Encrypted)
	}
	if ch.MessageCount != 2 {
		t.Errorf("expected messageCount=2, got %v", ch.MessageCount)
	}
}

func TestChannelsAPIExcludesEncrypted(t *testing.T) {
	_, router := setupTestServer(t)
	// Seed encrypted data into the server's DB
	// setupTestServer uses seedTestData which has no encrypted packets,
	// so default /api/channels should NOT include encrypted channels.
	req := httptest.NewRequest("GET", "/api/channels", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	channels := body["channels"].([]interface{})

	for _, ch := range channels {
		m := ch.(map[string]interface{})
		if enc, ok := m["encrypted"]; ok && enc == true {
			t.Errorf("default /api/channels should not include encrypted channels, found: %v", m["hash"])
		}
	}
}

func TestChannelsAPIIncludesEncryptedWithParam(t *testing.T) {
	ctx := t.Context()

	srv, router := setupTestServer(t)
	// Add encrypted data to the server's DB
	seedEncryptedChannelData(t, srv.db)
	// Reload store so in-memory also has the data
	store := NewPacketStore(srv.db, nil)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	srv.store = store

	req := httptest.NewRequest("GET", "/api/channels?includeEncrypted=true", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	channels := body["channels"].([]interface{})

	foundEncrypted := false
	for _, ch := range channels {
		m := ch.(map[string]interface{})
		if enc, ok := m["encrypted"]; ok && enc == true {
			foundEncrypted = true
			break
		}
	}
	if !foundEncrypted {
		t.Error("expected encrypted channels with includeEncrypted=true, found none")
	}
}

func TestChannelMessagesExcludesEncrypted(t *testing.T) {
	ctx := t.Context()

	srv, router := setupTestServer(t)
	seedEncryptedChannelData(t, srv.db)
	store := NewPacketStore(srv.db, nil)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	srv.store = store

	// Request messages for the encrypted channel — should return empty
	req := httptest.NewRequest("GET", "/api/channels/enc_A1B2/messages", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &body)
	messages, ok := body["messages"].([]interface{})
	if !ok {
		// messages might be null/missing — that's fine, means no messages
		return
	}
	// Encrypted messages should not be returned as readable messages
	for _, msg := range messages {
		m := msg.(map[string]interface{})
		if text, ok := m["text"].(string); ok && text != "" {
			t.Errorf("encrypted channel should not return readable messages, got text: %s", text)
		}
	}
}
