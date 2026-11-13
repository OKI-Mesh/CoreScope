package main

import (
	"encoding/hex"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// insertAdvertTx seeds one advert through the real ingest write path. The
// originating node is created by the Advert input, so from_pubkey satisfies
// its foreign key without a separate node insert. The frame is a header byte
// (route type in the low two bits, payload type above it), a zero path length
// byte and a zeroed payload.
// insertAdvertTx seeds one advert through the real ingest write path. The
// originating node is created by the Advert input, so from_pubkey satisfies
// its foreign key without a separate node insert. The frame is a header byte
// (route type in the low two bits, payload type above it), the four transport
// code bytes for transport routes, a zero path length byte and a zeroed
// payload. Transport routes store their two 2-byte codes as hex in code1/code2.
func insertAdvertTx(t *testing.T, db *database.DB, pubkey, hash string, rt types.RouteType, ts time.Time) {
	t.Helper()
	header := byte(rt) | byte(types.PayloadADVERT)<<2
	frame := []byte{header}
	var code1, code2 string
	if rt.IsTransport() {
		frame = append(frame, 0xce, 0x6f, 0x00, 0x00)
		code1, code2 = "CE6F", "0000"
	}
	frame = append(frame, 0x00)
	frame = append(frame, make([]byte, 100)...)

	_, _, err := db.CreateOrUpdateTransmission(t.Context(), database.TransmissionInput{
		RawHex:         hex.EncodeToString(frame),
		Hash:           hash,
		RouteType:      rt,
		PayloadType:    types.PayloadADVERT,
		PayloadVersion: 0,
		Code1:          code1,
		Code2:          code2,
		Advert: &database.NodeInput{
			PublicKey: pubkey,
			Name:      pubkey,
			Role:      types.RoleRepeater,
			RxTime:    ts,
			IngestNow: time.Now().UTC(),
		},
	}, ts)
	if err != nil {
		t.Fatalf("insert advert transmission %s: %v", hash, err)
	}
}

// The wire contract: GET /api/nodes/{pubkey} carries flood_advert_count_7d,
// counting recent FLOOD adverts only (direct and out-of-window excluded).
func TestNodeDetailIncludesFloodAdvertCount(t *testing.T) {
	srv, router := setupTestServer(t)
	now := time.Now().UTC()
	ins := func(hash string, rt types.RouteType, ts time.Time) {
		insertAdvertTx(t, srv.db, "aabbccdd11223344", hash, rt, ts)
	}
	// The shared fixture may already seed adverts for this node, so assert the
	// DELTA our inserts cause rather than an absolute count.
	fetch := func() float64 {
		t.Helper()
		req := httptest.NewRequest("GET", "/api/nodes/aabbccdd11223344", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("expected 200, got %d", w.Code)
		}
		var body map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("bad JSON: %v", err)
		}
		node, ok := body["node"].(map[string]interface{})
		if !ok {
			t.Fatal("expected node object")
		}
		got, ok := node["flood_advert_count_7d"].(float64)
		if !ok {
			t.Fatalf("flood_advert_count_7d missing or not a number: %v", node["flood_advert_count_7d"])
		}
		return got
	}
	before := fetch()

	ins("fa1", types.RouteFlood, now.Add(-2*time.Hour))
	ins("fa2", types.RouteFlood, now.Add(-30*time.Hour))
	ins("za1", types.RouteDirect, now.Add(-1*time.Hour))             // zero-hop (direct): excluded
	ins("fa3", types.RouteFlood, now.Add(-9*24*time.Hour))           // well outside the 7d window
	ins("fa4", types.RouteFlood, now.Add(-7*24*time.Hour-time.Hour)) // just outside the 7d cutoff

	if got := fetch(); got != before+2 {
		t.Fatalf("want flood_advert_count_7d = %v+2, got %v", before, got)
	}
}
