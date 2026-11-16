package main

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

// TestUsefulnessAxes_HandleNodesSurface drives the line graph A-B-C-D
// through the full pipeline and verifies /api/nodes surfaces the #672
// axes 3 & 4 (coverage_score, redundancy_score) plus the composite
// (usefulness_score) and letter grade (usefulness_grade) on repeater rows.
// Mirrors TestBridgeScore_HandleNodesSurface.
func TestUsefulnessAxes_HandleNodesSurface(t *testing.T) {
	db := setupTestDB(t)
	now := time.Now().UTC()

	pks := []string{
		"aaaa000000000000000000000000000000000000000000000000000000000000",
		"bbbb000000000000000000000000000000000000000000000000000000000000",
		"cccc000000000000000000000000000000000000000000000000000000000000",
		"dddd000000000000000000000000000000000000000000000000000000000000",
	}
	for _, pk := range pks {
		seedNode(t, db, pk, "node-"+pk[:4], "repeater", fptr(37.5), fptr(-122.0), now)
	}
	clientPK := "eeee000000000000000000000000000000000000000000000000000000000000"
	seedNode(t, db, clientPK, "client-eeee", "companion", fptr(37.5), fptr(-122.0), now)

	store := loadedStore(t, db)
	g := NewNeighborGraph()
	obs := "obs-test"
	snr := 5.0
	for i := 0; i < 10; i++ {
		g.upsertEdge(pks[0], pks[1], "aa", obs, &snr, now)
		g.upsertEdge(pks[1], pks[2], "bb", obs, &snr, now)
		g.upsertEdge(pks[2], pks[3], "cc", obs, &snr, now)
	}
	store.graph.Store(g)
	store.recomputeUsefulnessAxes()

	cov := store.GetCoverageScoreMap()
	red := store.GetRedundancyScoreMap()
	if len(cov) == 0 || len(red) == 0 {
		t.Fatalf("expected non-empty coverage/redundancy snapshots, got cov=%d red=%d", len(cov), len(red))
	}
	if red[pks[1]] <= 0 || red[pks[2]] <= 0 {
		t.Errorf("middle redundancy should be > 0: b=%v c=%v", red[pks[1]], red[pks[2]])
	}
	if red[pks[0]] != 0 || red[pks[3]] != 0 {
		t.Errorf("leaf redundancy should be 0: a=%v d=%v", red[pks[0]], red[pks[3]])
	}
	if cov[pks[0]] <= 0 || cov[pks[1]] <= 0 {
		t.Errorf("coverage should be > 0 for connected nodes: a=%v b=%v", cov[pks[0]], cov[pks[1]])
	}

	cfg := &Config{Port: 3000}
	hub := NewHub()
	srv := NewServer(db, cfg, hub)
	srv.store = store
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	req := httptest.NewRequest("GET", "/api/nodes?limit=100", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("handleNodes status: want 200, got %d body=%s", rr.Code, rr.Body.String())
	}

	type nodeAxes struct {
		PublicKey         string   `json:"public_key"`
		CoverageScore     *float64 `json:"coverage_score"`
		RedundancyScore   *float64 `json:"redundancy_score"`
		BridgeScore       *float64 `json:"bridge_score"`
		TrafficShareScore *float64 `json:"traffic_share_score"`
		UsefulnessScore   *float64 `json:"usefulness_score"`
		UsefulnessGrade   *string  `json:"usefulness_grade"`
	}
	var resp struct {
		Nodes []nodeAxes `json:"nodes"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v body=%s", err, rr.Body.String())
	}
	gotBy := map[string]nodeAxes{}
	for _, n := range resp.Nodes {
		if n.PublicKey != "" {
			gotBy[n.PublicKey] = n
		}
	}
	val := func(p *float64) float64 {
		if p == nil {
			return 0
		}
		return *p
	}

	for _, pk := range pks {
		n, ok := gotBy[pk]
		if !ok {
			t.Errorf("node %s missing from response", pk[:4])
			continue
		}
		if n.CoverageScore == nil {
			t.Errorf("node %s: coverage_score field absent from response", pk[:4])
		}
		if n.RedundancyScore == nil {
			t.Errorf("node %s: redundancy_score field absent from response", pk[:4])
		}
		if n.UsefulnessScore == nil {
			t.Errorf("node %s: usefulness_score field absent from response", pk[:4])
		}
		if n.UsefulnessGrade == nil {
			t.Errorf("node %s: usefulness_grade field absent from response", pk[:4])
		}
	}

	b := gotBy[pks[1]]
	a := gotBy[pks[0]]
	if v := val(b.CoverageScore); v <= 0 {
		t.Errorf("middle B coverage_score should be > 0, got %v", v)
	}
	if v := val(b.RedundancyScore); v <= 0 {
		t.Errorf("middle B redundancy_score should be > 0, got %v", v)
	}
	if v := val(a.RedundancyScore); v != 0 {
		t.Errorf("leaf A redundancy_score should be 0, got %v", v)
	}
	if v := val(b.UsefulnessScore); v <= 0 {
		t.Errorf("middle B usefulness_score (composite) should be > 0, got %v", v)
	}
	if ts := val(b.TrafficShareScore); ts != 0 {
		t.Errorf("middle B traffic_share_score should be 0 (no relayed traffic), got %v", ts)
	}
	if us := val(b.UsefulnessScore); us == 0 {
		t.Error("middle B composite must differ from its zero traffic_share_score")
	}
	grade := ""
	if b.UsefulnessGrade != nil {
		grade = *b.UsefulnessGrade
	}
	if grade == "" || len(grade) != 1 || grade[0] < 'A' || grade[0] > 'F' {
		t.Errorf("middle B usefulness_grade should be a single A–F letter, got %q", grade)
	}

	client, ok := gotBy[clientPK]
	if !ok {
		t.Fatalf("client node %s missing from response", clientPK[:4])
	}
	for name, p := range map[string]*float64{
		"coverage_score":      client.CoverageScore,
		"redundancy_score":    client.RedundancyScore,
		"bridge_score":        client.BridgeScore,
		"traffic_share_score": client.TrafficShareScore,
		"usefulness_score":    client.UsefulnessScore,
	} {
		if p != nil {
			t.Errorf("non-repeater node must not carry %q, got %v", name, *p)
		}
	}
	if client.UsefulnessGrade != nil {
		t.Errorf("non-repeater node must not carry %q, got %v", "usefulness_grade", *client.UsefulnessGrade)
	}
}
