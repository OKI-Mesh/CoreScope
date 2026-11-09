package main

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/gorilla/mux"
)

// The packet detail panel (public/packets.js) renders a "Scope" row gated on
// `pkt.scope_name != null`, distinguishing three states that transmissions.scope_name
// encodes: SQL NULL (not transport-scoped, row hidden), "" (transport-scoped but the
// region did not match a configured key → "unknown scope") and "#name" (matched
// region). txPacket is the shape /api/packets and /api/packets/{id} serve from the
// in-memory store, so it must carry all three states through.

func TestTxPacketScopeNameMatchedRegion(t *testing.T) {
	scope := "#belgium"
	p := txPacket(&StoreTx{ID: 1, Hash: "aa", ScopeName: &scope}, database.ResolvedPaths{}, false)
	if p.ScopeName == nil || *p.ScopeName != "#belgium" {
		t.Errorf("scope_name = %#v, want %q", p.ScopeName, "#belgium")
	}
}

func TestTxPacketScopeNameUnknownScope(t *testing.T) {
	scope := ""
	p := txPacket(&StoreTx{ID: 1, Hash: "aa", ScopeName: &scope}, database.ResolvedPaths{}, false)
	if p.ScopeName == nil {
		t.Fatal("scope_name missing for a transport-scoped packet with an unmatched region")
	}
	if *p.ScopeName != "" {
		t.Errorf("scope_name = %#v, want %q (frontend renders this as 'unknown scope')", *p.ScopeName, "")
	}
}

func TestTxPacketScopeNameNotTransportScoped(t *testing.T) {
	p := txPacket(&StoreTx{ID: 1, Hash: "aa", ScopeName: nil}, database.ResolvedPaths{}, false)
	if p.ScopeName != nil {
		t.Errorf("scope_name = %#v, want nil for a non-transport-scoped packet", p.ScopeName)
	}
	// Assert the JSON the browser actually receives: the key is present and null.
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	v, present := decoded["scope_name"]
	if !present || v != nil {
		t.Errorf("JSON scope_name = %#v (present %v), want null", v, present)
	}
}

// TestPacketDetailExposesScopeName is the end-to-end guard: the packet-detail
// endpoint is served from the in-memory store, so scope_name must survive the
// SQL scan (nullStrPtr) and the response conversion (txPacket) with all three states
// intact. Before this test, the conversion dropped the field entirely and the Scope row
// only ever rendered for packets old enough to fall through to the DB.
func TestPacketDetailExposesScopeName(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)

	now := time.Now().UTC()
	// route_type 1 = FLOOD (never transport-scoped → NULL); 0 = TRANSPORT_FLOOD.
	mustCreateTestTransmission(t, ctx, db, testTransmission(
		"AABB", "aaaaaaaaaaaaaaa1", "", types.RouteFlood, types.PayloadADVERT, "",
	), now)
	unknownScope := ""
	unknownTx := testTransmission("AABB", "aaaaaaaaaaaaaaa2", "", types.RouteTransportFlood, types.PayloadADVERT, "")
	unknownTx.ScopeName = &unknownScope
	mustCreateTestTransmission(t, ctx, db, unknownTx, now)
	matchedScope := "#belgium"
	matchedTx := testTransmission("AABB", "aaaaaaaaaaaaaaa3", "", types.RouteTransportFlood, types.PayloadADVERT, "")
	matchedTx.ScopeName = &matchedScope
	mustCreateTestTransmission(t, ctx, db, matchedTx, now)

	srv := NewServer(db, &Config{Port: 3000}, NewHub())
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

	cases := []struct {
		name string
		hash string
		want interface{}
	}{
		{"not transport-scoped", "aaaaaaaaaaaaaaa1", nil},
		{"transport-scoped, region unmatched", "aaaaaaaaaaaaaaa2", ""},
		{"transport-scoped, region matched", "aaaaaaaaaaaaaaa3", "#belgium"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			packet, err := store.GetPacketByHash(ctx, tc.hash)
			if err != nil {
				t.Fatalf("GetPacketByHash(%q): %v", tc.hash, err)
			}
			if packet == nil {
				t.Fatalf("precondition: %s not in store (would hit the DB fallback)", tc.hash)
			}
			req := httptest.NewRequest("GET", "/api/packets/"+tc.hash, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != 200 {
				t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
			}
			var body map[string]interface{}
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			pkt, ok := body["packet"].(map[string]interface{})
			if !ok {
				t.Fatal("expected packet object")
			}
			if got := pkt["scope_name"]; got != tc.want {
				t.Errorf("scope_name = %#v, want %#v", got, tc.want)
			}
		})
	}
}

// --- grouped view ---
//
// The Packets tab defaults to "Group by Hash", which is served by a separate
// mapper (groupedTxsToPage in the store, a dedicated query in the DB fallback).
// The Scope column reads scope_name off those rows, so both paths must carry it.

func TestGroupedTxsToPageCarriesScopeName(t *testing.T) {
	matched := "#belgium"
	unmatched := ""
	txs := []*StoreTx{
		{ID: 1, Hash: "aa", ScopeName: &matched},
		{ID: 2, Hash: "bb", ScopeName: &unmatched},
		{ID: 3, Hash: "cc", ScopeName: nil},
	}
	res := groupedTxsToPage(txs, len(txs), 0, len(txs))
	want := []interface{}{"#belgium", "", nil}
	for i, w := range want {
		var got interface{}
		if res.Packets[i].ScopeName != nil {
			got = *res.Packets[i].ScopeName
		}
		if got != w {
			t.Errorf("packet %d: scope_name = %#v, want %#v", i, got, w)
		}
	}
}

func TestGroupedPacketsEndpointExposesScopeName(t *testing.T) {
	ctx := t.Context()

	db := setupTestDB(t)
	now := time.Now().UTC()
	mustCreateTestTransmission(t, ctx, db, testTransmission(
		"AABB", "bbbbbbbbbbbbbbb1", "", types.RouteFlood, types.PayloadADVERT, "",
	), now)
	unknownScope := ""
	unknownTx := testTransmission("AABB", "bbbbbbbbbbbbbbb2", "", types.RouteTransportFlood, types.PayloadADVERT, "")
	unknownTx.ScopeName = &unknownScope
	mustCreateTestTransmission(t, ctx, db, unknownTx, now)
	matchedScope := "#belgium"
	matchedTx := testTransmission("AABB", "bbbbbbbbbbbbbbb3", "", types.RouteTransportFlood, types.PayloadADVERT, "")
	matchedTx.ScopeName = &matchedScope
	mustCreateTestTransmission(t, ctx, db, matchedTx, now)

	want := map[string]*string{
		"bbbbbbbbbbbbbbb1": nil,
		"bbbbbbbbbbbbbbb2": &unknownScope,
		"bbbbbbbbbbbbbbb3": &matchedScope,
	}

	srv := NewServer(db, &Config{Port: 3000}, NewHub())
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

	req := httptest.NewRequest("GET", "/api/packets?groupByHash=true&limit=50", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}
	var body struct {
		Packets []struct {
			Hash      string  `json:"hash"`
			ScopeName *string `json:"scope_name"`
		} `json:"packets"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(body.Packets) != 3 {
		t.Fatalf("expected 3 grouped packets, got %d", len(body.Packets))
	}
	for _, p := range body.Packets {
		expected, known := want[p.Hash]
		if !known {
			t.Errorf("unexpected packet %s", p.Hash)
			continue
		}
		switch {
		case expected == nil && p.ScopeName != nil:
			t.Errorf("%s: scope_name = %q, want null", p.Hash, *p.ScopeName)
		case expected != nil && p.ScopeName == nil:
			t.Errorf("%s: scope_name = null, want %q", p.Hash, *expected)
		case expected != nil && *p.ScopeName != *expected:
			t.Errorf("%s: scope_name = %q, want %q", p.Hash, *p.ScopeName, *expected)
		}
	}
}
