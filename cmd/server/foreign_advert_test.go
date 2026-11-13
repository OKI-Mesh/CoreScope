package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
)

// TestHandleNodes_ExposesForeignAdvertField asserts the /api/nodes response
// surfaces the foreign_advert column as a boolean `foreign` field on each
// node, so operators can see bridged/leaked nodes (#730).
func TestHandleNodes_ExposesForeignAdvertField(t *testing.T) {
	srv, router := setupTestServer(t)
	ctx := t.Context()
	now := time.Now().UTC()
	for _, node := range []struct {
		pubkey  string
		name    string
		foreign bool
	}{
		{"pk_local", "local-node", false},
		{"pk_foreign", "foreign-node", true},
	} {
		if err := srv.db.CreateOrUpdateNode(ctx, database.NodeInput{
			PublicKey: node.pubkey,
			Name:      node.name,
			Role:      "companion",
			Lat:       ptr(37.0),
			Lon:       ptr(-122.0),
			RxTime:    now,
			IngestNow: now,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := srv.db.SQL().ExecContext(ctx, `UPDATE nodes SET foreign_advert = $1 WHERE public_key = $2`, node.foreign, node.pubkey); err != nil {
			t.Fatal(err)
		}
	}

	req := httptest.NewRequest("GET", "/api/nodes?limit=100", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}

	var resp struct {
		Nodes []map[string]interface{} `json:"nodes"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}

	got := map[string]bool{}
	for _, n := range resp.Nodes {
		pk, _ := n["public_key"].(string)
		f, ok := n["foreign"].(bool)
		if !ok {
			t.Errorf("node %s: missing/non-bool 'foreign' field, got %T %v", pk, n["foreign"], n["foreign"])
			continue
		}
		got[pk] = f
	}
	if got["pk_local"] {
		t.Errorf("PK_LOCAL foreign=%v, want false", got["PK_LOCAL"])
	}
	if !got["pk_foreign"] {
		t.Errorf("PK_FOREIGN foreign=%v, want true", got["PK_FOREIGN"])
	}
}
