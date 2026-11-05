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

// #1851: the Channels view shows the region scope a message was sent under.
// transmissions.scope_name encodes three states (see StoreTx.ScopeName):
// SQL NULL = not transport-scoped, "" = transport-scoped but no configured
// region key matched, "#name" = matched region. /api/channels/{hash}/messages
// is served by the DB query when a DB is attached and by the in-memory store
// otherwise, and live messages arrive over the WebSocket broadcast, so all
// three paths must carry the same three states.

const (
	chScopeHashUnscoped = "cccccccccccccc01"
	chScopeHashUnknown  = "cccccccccccccc02"
	chScopeHashMatched  = "cccccccccccccc03"
)

var chScopeWant = map[string]*string{
	chScopeHashUnscoped: nil,
	chScopeHashUnknown:  ptr(""),
	chScopeHashMatched:  ptr("#belgium"),
}

// setupChannelScopeDB seeds one channel with three messages, one per scope
// state, each heard by one observation.
func setupChannelScopeDB(t *testing.T) *database.DB {
	t.Helper()
	db := setupTestDB(t)
	ctx := t.Context()
	now := time.Now().UTC()

	observerIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        "obs1",
		Name:      "Observer One",
		IATA:      "BRU",
		LastSeen:  now,
		IngestNow: now,
	})
	if err != nil {
		t.Fatalf("create observer: %v", err)
	}

	rows := []struct {
		hash      string
		routeType types.RouteType
		scope     *string // nil = NULL (unscoped), "" = unknown region
		text      string
	}{
		{chScopeHashUnscoped, 1, nil, "Alice: plain flood"},
		{chScopeHashUnknown, 0, ptr(""), "Bob: unknown region"},
		{chScopeHashMatched, 0, ptr("#belgium"), "Carol: matched region"},
	}

	for i, r := range rows {
		ts := now.Add(time.Duration(i-3) * time.Minute)

		txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
			RawHex:      "AABB",
			Hash:        r.hash,
			RouteType:   r.routeType,
			PayloadType: types.PayloadType(5),
			DecodedJSON: `{"type":"CHAN","channel":"#scopetest","text":"` + r.text + `"}`,
			ChannelHash: "#scopetest",
			ScopeName:   r.scope,
		}, ts)
		if err != nil {
			t.Fatalf("insert tx %s: %v", r.hash, err)
		}

		_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
			TransmissionID: txID,
			ObserverIdx:    ptr(observerIdx),
			Direction:      ptr("RX"),
			SNR:            ptr(9.5),
			RSSI:           ptr(-90.0),
			Timestamp:      ts,
		}, nil, nil)
		if err != nil {
			t.Fatalf("insert obs %s: %v", r.hash, err)
		}
		if !inserted {
			t.Fatalf("expected observation to be inserted for %s", r.hash)
		}
	}
	return db
}

func assertChannelMessageScopes(t *testing.T, messages []ChannelMessageResponse) {
	t.Helper()
	if len(messages) != len(chScopeWant) {
		t.Fatalf("expected %d messages, got %d", len(chScopeWant), len(messages))
	}
	for _, m := range messages {
		h := m.PacketHash
		want, known := chScopeWant[h]
		if !known {
			t.Errorf("unexpected packetHash %q", h)
			continue
		}
		if m.ScopeName == nil && want == nil {
			continue
		}
		if m.ScopeName == nil || want == nil || *m.ScopeName != *want {
			t.Errorf("%s: scope_name = %v, want %v", h, m.ScopeName, want)
		}
	}
}

func TestChannelMessagesCarryScopeName(t *testing.T) {
	db := setupChannelScopeDB(t)
	s := &Server{db: db}

	messages, _, err := s.getChannelMessages(t.Context(), "#scopetest", false, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	assertChannelMessageScopes(t, messages)
}

func TestStoreGetChannelMessagesCarriesScopeName(t *testing.T) {
	ctx := t.Context()
	db := setupChannelScopeDB(t)

	defer db.Close()
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	messages, _ := store.GetChannelMessages(ctx, "#scopetest", 100, 0)
	responses := make([]ChannelMessageResponse, 0, len(messages))
	for _, message := range messages {
		encoded, err := json.Marshal(message)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var response ChannelMessageResponse
		if err := json.Unmarshal(encoded, &response); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		responses = append(responses, response)
	}
	assertChannelMessageScopes(t, responses)
}

func TestChannelMessagesEndpointExposesScopeName(t *testing.T) {
	ctx := t.Context()

	db := setupChannelScopeDB(t)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	srv := NewServer(db, &Config{Port: 3000}, NewHub())
	srv.store = store
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	req := httptest.NewRequest("GET", "/api/channels/%23scopetest/messages?limit=50", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}
	var body struct {
		Messages []ChannelMessageResponse `json:"messages"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	assertChannelMessageScopes(t, body.Messages)

}

// Live messages in the Channels view are built from the WebSocket broadcast,
// not from the REST response, so both broadcast builders must carry scope_name
// or a new message shows no region until the next REST refresh replaces it.
func TestBroadcastMapsCarryScopeName(t *testing.T) {
	ctx := t.Context()

	db := setupChannelScopeDB(t)
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	observerID := observerIDByPubkey(t, db, "obs1")

	assertBroadcast := func(t *testing.T, packets []BroadcastPacket, hash string, want *string) {
		t.Helper()
		found := false
		for _, packet := range packets {
			if packet.Hash == nil || *packet.Hash != hash {
				continue
			}
			found = true
			if !sameOptionalString(packet.ScopeName, want) {
				t.Errorf("top-level scope_name = %v, want %v", packet.ScopeName, want)
			}
			if !sameOptionalString(packet.Packet.ScopeName, want) {
				t.Errorf("packet.scope_name = %v, want %v", packet.Packet.ScopeName, want)
			}
		}
		if !found {
			t.Fatalf("no broadcast map for %s", hash)
		}
	}

	t.Run("IngestNewFromDB", func(t *testing.T) {
		ctx := t.Context()
		txMax := store.MaxTransmissionID()
		now := time.Now().UTC()

		txID, isNew, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
			RawHex:      "AABB",
			Hash:        "cccccccccccccc04",
			RouteType:   types.RouteType(0),
			PayloadType: types.PayloadType(5),
			DecodedJSON: `{"type":"CHAN","channel":"#scopetest","text":"Dave: live"}`,
			ChannelHash: "#scopetest",
			ScopeName:   ptr("#belgium"),
		}, now)
		if err != nil {
			t.Fatalf("insert tx: %v", err)
		}
		if !isNew {
			t.Fatalf("hash cccccccccccccc04 already existed")
		}
		_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
			TransmissionID: txID,
			ObserverIdx:    ptr(observerID),
			Direction:      ptr("RX"),
			SNR:            ptr(7.0),
			RSSI:           ptr(-95.0),
			Timestamp:      now,
		}, nil, nil)
		if err != nil {
			t.Fatalf("insert obs: %v", err)
		}
		if !inserted {
			t.Fatalf("expected observation to be inserted for tx %d", txID)
		}

		maps, _, err := store.IngestNewFromDB(ctx, txMax, 100)
		if err != nil {
			t.Fatalf("IngestNewFromDB(): %v", err)
		}
		assertBroadcast(t, maps, "cccccccccccccc04", ptr("#belgium"))
	})

	t.Run("IngestNewObservations", func(t *testing.T) {
		ctx := t.Context()
		obsMax, err := db.GetMaxObservationID(ctx)
		if err != nil {
			t.Fatalf("max observation id: %v", err)
		}

		freshID, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
			ID:        strings.Repeat("e", 64),
			Name:      "scope-fresh",
			IATA:      "LAX",
			Meta:      nil,
			LastSeen:  time.Now().UTC(),
			IngestNow: time.Now().UTC(),
		})
		if err != nil {
			t.Fatalf("seed fresh observer: %v", err)
		}

		var txID int64
		if err := db.SQL().QueryRowContext(ctx,
			`SELECT id FROM transmissions WHERE hash = $1`, chScopeHashUnknown).Scan(&txID); err != nil {
			t.Fatalf("lookup tx: %v", err)
		}
		_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
			TransmissionID: txID,
			ObserverIdx:    ptr(freshID),
			Direction:      ptr("RX"),
			SNR:            ptr(3.0),
			RSSI:           ptr(-110.0),
			Timestamp:      time.Now().UTC(),
		}, []string{"aa"}, nil)
		if err != nil {
			t.Fatalf("insert obs: %v", err)
		}
		if !inserted {
			t.Fatalf("expected observation to be inserted for tx %d", txID)
		}

		var n int
		if err := db.SQL().QueryRowContext(ctx,
			`SELECT COUNT(*) FROM observations WHERE id > $1`, obsMax).Scan(&n); err != nil {
			t.Fatalf("count new obs: %v", err)
		}
		if n != 1 {
			t.Fatalf("observations above obsMax = %d, want 1", n)
		}

		maps, _, err := store.IngestNewObservations(ctx, obsMax, 100)
		if err != nil {
			t.Fatalf("IngestNewObservations(): %v", err)
		}
		assertBroadcast(t, maps, chScopeHashUnknown, ptr(""))
	})
}

func sameOptionalString(got, want *string) bool {
	if got == nil || want == nil {
		return got == nil && want == nil
	}
	return *got == *want
}
