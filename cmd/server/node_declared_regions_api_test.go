package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
)

func declaredRegionsOf(node api.Node) ([]string, bool) {
	if node.DeclaredRegions == nil {
		return nil, false
	}
	return *node.DeclaredRegions, true
}

// scopeKey builds a valid 64-char lowercase hex pubkey from a small number,
// so fixtures satisfy the nodes.public_key guards.
func scopeKey(n int) string { return fmt.Sprintf("%064x", n) }

func sptr(s string) *string       { return &s }
func tptr(t time.Time) *time.Time { return &t }

// scopedNode is a node plus the answer it gave to the scope question.
// Scope nil means never asked (UpdateNodeConfiguredScope is not called);
// sptr("") means it answered with nothing, which is a different thing.
type scopedNode struct {
	Node    database.NodeInput
	Scope   *string
	ScopeAt *time.Time // nil skips the last-write-wins guard
}

func seedScopedNodes(t *testing.T, db *database.DB, fixtures []scopedNode) {
	t.Helper()
	ctx := t.Context()
	for _, f := range fixtures {
		if err := db.CreateOrUpdateNode(ctx, f.Node); err != nil {
			t.Fatalf("seed %s: %v", f.Node.Name, err)
		}
		if f.Scope != nil {
			if err := db.UpdateNodeConfiguredScope(ctx, f.Node.PublicKey, *f.Scope, f.ScopeAt); err != nil {
				t.Fatalf("set scope %s: %v", f.Node.Name, err)
			}
		}
	}
}

var (
	keyNamed     = scopeKey(0xa1)
	keyStarOnly  = scopeKey(0xa2)
	keyNever     = scopeKey(0xa3)
	keyCompanion = scopeKey(0xa4)
	keyDetail    = scopeKey(0xa5)
)

func TestHandleNodesExposesDeclaredRegions(t *testing.T) {
	srv, router := setupScopeConfigStateServer(t)
	now := time.Now().UTC()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	seedScopedNodes(t, srv.db, []scopedNode{
		{
			Node:    database.NodeInput{PublicKey: keyNamed, Name: "named-rp", Role: "repeater", Lat: fptr(51.0), Lon: fptr(4.0), RxTime: at, IngestNow: now},
			Scope:   sptr("#be,*,be-bru"),
			ScopeAt: tptr(at),
		},
		{
			Node:    database.NodeInput{PublicKey: keyStarOnly, Name: "star-rp", Role: "repeater", Lat: fptr(51.1), Lon: fptr(4.1), RxTime: at, IngestNow: now},
			Scope:   sptr("*"),
			ScopeAt: tptr(at),
		},
		{
			// Never asked: no Scope, so UpdateNodeConfiguredScope is not called.
			Node: database.NodeInput{PublicKey: keyNever, Name: "never-rp", Role: "repeater", Lat: fptr(51.2), Lon: fptr(4.2), RxTime: at, IngestNow: now},
		},
		{
			Node:    database.NodeInput{PublicKey: keyCompanion, Name: "phone", Role: "companion", Lat: fptr(51.3), Lon: fptr(4.3), RxTime: at, IngestNow: now},
			Scope:   sptr("be,*"),
			ScopeAt: tptr(at),
		},
	})

	nodes := nodesByPubkey(t, router, "?limit=200")

	got, present := declaredRegionsOf(nodes[keyNamed])
	if !present {
		t.Fatalf("declared repeater has no declared_regions field: %+v", nodes[keyNamed])
	}
	if want := []string{"be", "be-bru"}; !reflect.DeepEqual(got, want) {
		t.Errorf("declared_regions = %v, want %v ('#' stripped, '*' left out, list order kept)", got, want)
	}

	// Answered with only the wildcard: the answer exists and names no region.
	// That is an empty list, not an absent field.
	got, present = declaredRegionsOf(nodes[keyStarOnly])
	if !present {
		t.Errorf("wildcard-only repeater has no declared_regions field, want an empty list")
	} else if len(got) != 0 {
		t.Errorf("wildcard-only repeater declared_regions = %v, want []", got)
	}

	// Never asked: no answer to report.
	if v, present := declaredRegionsOf(nodes[keyNever]); present {
		t.Errorf("never-asked repeater carries declared_regions = %v, want the field absent", v)
	}

	// A companion does not forward, so a declared list on it is not something
	// the region filter should read, same rule as scope_config_state.
	if v, present := declaredRegionsOf(nodes[keyCompanion]); present {
		t.Errorf("companion carries declared_regions = %v, want the field absent", v)
	}
}

func TestHandleNodeDetailExposesDeclaredRegions(t *testing.T) {
	srv, router := setupScopeConfigStateServer(t)
	now := time.Now().UTC()
	jan := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	seedScopedNodes(t, srv.db, []scopedNode{
		{
			Node: database.NodeInput{
				PublicKey: keyDetail, Name: "detail-rp", Role: "repeater",
				Lat: fptr(51.3), Lon: fptr(4.3),
				RxTime: jan, IngestNow: now,
			},
			Scope:   sptr("nl,#be"),
			ScopeAt: tptr(jan),
		},
	})

	req := httptest.NewRequest("GET", "/api/nodes/"+keyDetail, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp struct {
		Node api.Node `json:"node"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	got, present := declaredRegionsOf(resp.Node)
	if !present {
		t.Fatalf("detail endpoint has no declared_regions field: %+v", resp.Node)
	}
	if want := []string{"nl", "be"}; !reflect.DeepEqual(got, want) {
		t.Errorf("declared_regions = %v, want %v", got, want)
	}
}

func TestScopeAuditAndNodesAgreeOnDeclaredRegions(t *testing.T) {
	srv, router := setupScopeConfigStateServer(t)
	now := time.Now().UTC()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	cases := []struct{ label, key, csv string }{
		{"HASHSTAR", scopeKey(0xb1), "#be,#*"},
		{"BARESTAR", scopeKey(0xb2), "be,*,nl-li"},
		{"STARONLY", scopeKey(0xb3), "#*"},
		{"NAMEDONLY", scopeKey(0xb4), "#be, de"},
		{"EMPTY", scopeKey(0xb5), ""},
	}
	fixtures := make([]scopedNode, 0, len(cases))
	for _, c := range cases {
		fixtures = append(fixtures, scopedNode{
			Node: database.NodeInput{
				PublicKey: c.key, Name: "rp-" + c.label, Role: "repeater",
				Lat: fptr(51.0), Lon: fptr(4.0),
				RxTime: at, IngestNow: now,
			},
			Scope:   sptr(c.csv),
			ScopeAt: tptr(at),
		})
	}
	seedScopedNodes(t, srv.db, fixtures)

	audit := getScopeAudit(t, router, "?window=24h")
	auditRegions := map[string][]string{}
	for _, row := range audit.Repeaters {
		auditRegions[strings.ToLower(row.PublicKey)] = row.DeclaredRegions
	}
	nodes := nodesByPubkey(t, router, "?limit=200")

	for _, c := range cases {
		want, ok := auditRegions[c.key]
		if !ok {
			t.Errorf("%s (%q): the scope audit does not list the repeater at all", c.label, c.csv)
			continue
		}
		node, ok := nodes[c.key]
		if !ok {
			t.Errorf("%s (%q): the node list does not contain the repeater", c.label, c.csv)
			continue
		}
		got, present := declaredRegionsOf(node)
		if !present {
			t.Errorf("regions_csv %q: the scope audit lists %v, the node has no declared_regions field", c.csv, want)
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("regions_csv %q: the scope audit lists %v, the node lists %v", c.csv, want, got)
		}
	}
}

func TestNodesNeverCarryDeclaredRegionsTruncated(t *testing.T) {
	srv, router := setupScopeConfigStateServer(t)
	now := time.Now().UTC()
	jan := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	keyCfg := scopeKey(0xc4)
	seedScopedNodes(t, srv.db, []scopedNode{
		{
			Node:    database.NodeInput{PublicKey: keyCfg, Name: "cfg-rp", Role: "repeater", Lat: fptr(51.3), Lon: fptr(4.3), RxTime: jan, IngestNow: now},
			Scope:   sptr("be"),
			ScopeAt: tptr(jan),
		},
	})

	nodes := nodesByPubkey(t, router, "?limit=200")
	node, ok := nodes[keyCfg]
	if !ok {
		t.Fatalf("%s: missing from the node list", keyCfg)
	}
	if _, present := declaredRegionsOf(node); !present {
		t.Errorf("%s: no declared_regions field", keyCfg)
	}
	if node.DeclaredRegionsTruncated != nil {
		t.Errorf("%s: declared_regions_truncated = %v, want the field absent", keyCfg, *node.DeclaredRegionsTruncated)
	}

	req := httptest.NewRequest("GET", "/api/nodes/"+keyCfg, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("detail status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp struct {
		Node api.Node `json:"node"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Node.DeclaredRegionsTruncated != nil {
		t.Errorf("detail endpoint declared_regions_truncated = %v, want the field absent", *resp.Node.DeclaredRegionsTruncated)
	}
}
