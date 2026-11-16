package main

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/gorilla/mux"
)

func serveResolve(srv *Server, path string) *httptest.ResponseRecorder {
	router := mux.NewRouter()
	router.HandleFunc("/api/nodes/resolve", srv.handleResolvePrefix).Methods("GET")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, httptest.NewRequest("GET", path, nil))
	return rr
}

func TestResolvePrefix(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	now := time.Now().UTC()

	for _, n := range []database.NodeInput{
		{PublicKey: "efef7943505052b47f1809488ea4b4d3942d4ed72d2b1953b90a9f5e62a65fb5", Name: "NodeUnique", Role: "repeater", RxTime: now, IngestNow: now},
		{PublicKey: "aabb110000000000000000000000000000000000000000000000000000000000", Name: "NodeA", Role: "repeater", RxTime: now, IngestNow: now},
		{PublicKey: "aabb220000000000000000000000000000000000000000000000000000000000", Name: "NodeB", Role: "repeater", RxTime: now, IngestNow: now},
	} {
		if err := db.CreateOrUpdateNode(ctx, n); err != nil {
			t.Fatalf("seed %s: %v", n.Name, err)
		}
	}
	srv := &Server{db: db}

	var r1 api.ResolvePrefixResponse
	if err := json.Unmarshal(serveResolve(srv, "/api/nodes/resolve?prefix=efef79").Body.Bytes(), &r1); err != nil {
		t.Fatalf("decode unique: %v", err)
	}
	if r1.Name == nil || *r1.Name != "NodeUnique" || r1.Ambiguous {
		t.Fatalf("unique: %+v", r1)
	}
	var r2 api.ResolvePrefixResponse
	if err := json.Unmarshal(serveResolve(srv, "/api/nodes/resolve?prefix=aabb").Body.Bytes(), &r2); err != nil {
		t.Fatalf("decode ambiguous: %v", err)
	}
	if !r2.Ambiguous || r2.Name != nil || r2.Pubkey != nil {
		t.Fatalf("ambiguous: %+v", r2)
	}
	var r3 api.ResolvePrefixResponse
	if err := json.Unmarshal(serveResolve(srv, "/api/nodes/resolve?prefix=dead").Body.Bytes(), &r3); err != nil {
		t.Fatalf("decode notfound: %v", err)
	}
	if r3.Name != nil || r3.Pubkey != nil || r3.Ambiguous {
		t.Fatalf("notfound: %+v", r3)
	}
	if serveResolve(srv, "/api/nodes/resolve?prefix=xyz").Code != 400 {
		t.Fatal("non-hex prefix should be 400")
	}
	for _, short := range []string{"a", "aa", "abc"} {
		if code := serveResolve(srv, "/api/nodes/resolve?prefix="+short).Code; code != 400 {
			t.Fatalf("prefix %q (<4 hex) should be 400, got %d", short, code)
		}
	}
}

func TestResolvePrefixHidesBlacklistedAndHidden(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	now := time.Now().UTC()
	const blPK = "bbcc110000000000000000000000000000000000000000000000000000000000"
	const hidPK = "ddee220000000000000000000000000000000000000000000000000000000000"

	for _, n := range []database.NodeInput{
		{PublicKey: blPK, Name: "BlacklistedNode", Role: "repeater", RxTime: now, IngestNow: now},
		{PublicKey: hidPK, Name: "🚫HiddenNode", Role: "repeater", RxTime: now, IngestNow: now},
	} {
		if err := db.CreateOrUpdateNode(ctx, n); err != nil {
			t.Fatalf("seed %s: %v", n.Name, err)
		}
	}
	srv := &Server{db: db, cfg: &Config{
		NodeBlacklist:      []string{blPK},
		HiddenNamePrefixes: []string{"🚫"},
	}}

	var rb api.ResolvePrefixResponse
	if err := json.Unmarshal(serveResolve(srv, "/api/nodes/resolve?prefix=bbcc11").Body.Bytes(), &rb); err != nil {
		t.Fatalf("decode blacklisted: %v", err)
	}
	if rb.Name != nil || rb.Pubkey != nil || rb.Ambiguous {
		t.Fatalf("blacklisted node must resolve as not-found: %+v", rb)
	}
	var rh api.ResolvePrefixResponse
	if err := json.Unmarshal(serveResolve(srv, "/api/nodes/resolve?prefix=ddee22").Body.Bytes(), &rh); err != nil {
		t.Fatalf("decode hidden: %v", err)
	}
	if rh.Name != nil || rh.Pubkey != nil || rh.Ambiguous {
		t.Fatalf("hidden-prefix node must resolve as not-found: %+v", rh)
	}
}
