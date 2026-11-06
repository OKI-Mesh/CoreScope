package main

import (
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
)

func TestEnrichNodeWithMultiByte(t *testing.T) {
	assertMultiByte := func(t *testing.T, node api.Node, status string, evidence string, maxHashSize int) {
		t.Helper()
		if node.MultiByteStatus == nil || *node.MultiByteStatus != status {
			t.Errorf("multi_byte_status = %v, want %q", node.MultiByteStatus, status)
		}
		if node.MultiByteEvidence == nil || *node.MultiByteEvidence != evidence {
			t.Errorf("multi_byte_evidence = %v, want %q", node.MultiByteEvidence, evidence)
		}
		if node.MultiByteMaxHashSize == nil || *node.MultiByteMaxHashSize != maxHashSize {
			t.Errorf("multi_byte_max_hash_size = %v, want %d", node.MultiByteMaxHashSize, maxHashSize)
		}
	}

	t.Run("nil entry leaves no fields", func(t *testing.T) {
		node := api.Node{PublicKey: "abc123"}
		enrichNodeWithMultiByte(&node, nil)
		if node.MultiByteStatus != nil || node.MultiByteEvidence != nil || node.MultiByteMaxHashSize != nil {
			t.Errorf("expected no multi-byte fields with nil entry, got status=%v evidence=%v max=%v",
				node.MultiByteStatus, node.MultiByteEvidence, node.MultiByteMaxHashSize)
		}
	})

	t.Run("confirmed entry sets fields", func(t *testing.T) {
		node := api.Node{PublicKey: "abc123"}
		enrichNodeWithMultiByte(&node, &MultiByteCapEntry{Status: "confirmed", Evidence: "advert", MaxHashSize: 2})
		assertMultiByte(t, node, "confirmed", "advert", 2)
	})

	t.Run("suspected entry sets fields", func(t *testing.T) {
		node := api.Node{PublicKey: "abc123"}
		enrichNodeWithMultiByte(&node, &MultiByteCapEntry{Status: "suspected", Evidence: "path", MaxHashSize: 2})
		assertMultiByte(t, node, "suspected", "path", 2)
	})

	t.Run("unknown entry sets status unknown", func(t *testing.T) {
		node := api.Node{PublicKey: "abc123"}
		enrichNodeWithMultiByte(&node, &MultiByteCapEntry{Status: "unknown", MaxHashSize: 1})
		if node.MultiByteStatus == nil || *node.MultiByteStatus != "unknown" {
			t.Errorf("multi_byte_status = %v, want unknown", node.MultiByteStatus)
		}
		if node.MultiByteMaxHashSize == nil || *node.MultiByteMaxHashSize != 1 {
			t.Errorf("multi_byte_max_hash_size = %v, want 1", node.MultiByteMaxHashSize)
		}
	})

	t.Run("JSON keys are unchanged", func(t *testing.T) {
		type wireKeys struct {
			Status      json.RawMessage `json:"multi_byte_status"`
			Evidence    json.RawMessage `json:"multi_byte_evidence"`
			MaxHashSize json.RawMessage `json:"multi_byte_max_hash_size"`
		}
		decode := func(node api.Node) wireKeys {
			b, err := json.Marshal(node)
			if err != nil {
				t.Fatal(err)
			}
			var got wireKeys
			if err := json.Unmarshal(b, &got); err != nil {
				t.Fatal(err)
			}
			return got
		}

		node := api.Node{PublicKey: "abc123"}
		enrichNodeWithMultiByte(&node, &MultiByteCapEntry{Status: "confirmed", Evidence: "advert", MaxHashSize: 2})
		got := decode(node)
		if string(got.Status) != `"confirmed"` {
			t.Errorf("multi_byte_status = %s, want \"confirmed\"", got.Status)
		}
		if string(got.Evidence) != `"advert"` {
			t.Errorf("multi_byte_evidence = %s, want \"advert\"", got.Evidence)
		}
		if string(got.MaxHashSize) != "2" {
			t.Errorf("multi_byte_max_hash_size = %s, want 2", got.MaxHashSize)
		}

		bare := api.Node{PublicKey: "abc123"}
		enrichNodeWithMultiByte(&bare, nil)
		got = decode(bare)
		if len(got.Status) != 0 || len(got.Evidence) != 0 || len(got.MaxHashSize) != 0 {
			t.Errorf("keys present with nil entry (status=%s evidence=%s max=%s); want all omitted, not null",
				got.Status, got.Evidence, got.MaxHashSize)
		}
	})
}

func TestEnrichNodeWithDeclaredScope(t *testing.T) {
	t.Run("not declared sets state only", func(t *testing.T) {
		node := api.Node{PublicKey: "abc123"}
		scopes := []string{"oki"}
		answer := declaredAnswer{}
		enrichNodeWithDeclaredScope(&node, answer, false, scopes)

		want := api.ScopeConfigState(nodeScopeConfigState(answer.RegionsCSV, false, scopes))
		if node.ScopeConfigState == nil || *node.ScopeConfigState != want {
			t.Errorf("scope_config_state = %v, want %q", node.ScopeConfigState, want)
		}
		if node.DeclaredRegions != nil {
			t.Errorf("declared_regions = %v, want nil when not declared", *node.DeclaredRegions)
		}
		if node.DeclaredRegionsTruncated != nil {
			t.Errorf("declared_regions_truncated = %v, want nil when not declared", *node.DeclaredRegionsTruncated)
		}
	})

	t.Run("declared sets regions", func(t *testing.T) {
		node := api.Node{PublicKey: "abc123"}
		scopes := []string{"oki"}
		answer := declaredAnswer{RegionsCSV: "oki,cin"}
		enrichNodeWithDeclaredScope(&node, answer, true, scopes)

		want := api.ScopeConfigState(nodeScopeConfigState(answer.RegionsCSV, true, scopes))
		if node.ScopeConfigState == nil || *node.ScopeConfigState != want {
			t.Errorf("scope_config_state = %v, want %q", node.ScopeConfigState, want)
		}
		if node.DeclaredRegions == nil {
			t.Fatal("declared_regions is nil, want the declared list")
		}
		named, _ := splitDeclaredRegions(answer.RegionsCSV)
		if !slices.Equal(*node.DeclaredRegions, named) {
			t.Errorf("declared_regions = %v, want %v", *node.DeclaredRegions, named)
		}
		if node.DeclaredRegionsTruncated != nil {
			t.Errorf("declared_regions_truncated = %v, want nil when not truncated", *node.DeclaredRegionsTruncated)
		}
	})

	t.Run("truncated flag is set only when true", func(t *testing.T) {
		node := api.Node{PublicKey: "abc123"}
		enrichNodeWithDeclaredScope(&node, declaredAnswer{RegionsCSV: "oki", Truncated: true}, true, nil)
		if node.DeclaredRegionsTruncated == nil || !*node.DeclaredRegionsTruncated {
			t.Errorf("declared_regions_truncated = %v, want true", node.DeclaredRegionsTruncated)
		}
	})

	t.Run("JSON keys are unchanged", func(t *testing.T) {
		type wireKeys struct {
			State     json.RawMessage `json:"scope_config_state"`
			Regions   json.RawMessage `json:"declared_regions"`
			Truncated json.RawMessage `json:"declared_regions_truncated"`
		}
		decode := func(node api.Node) wireKeys {
			b, err := json.Marshal(node)
			if err != nil {
				t.Fatal(err)
			}
			var got wireKeys
			if err := json.Unmarshal(b, &got); err != nil {
				t.Fatal(err)
			}
			return got
		}

		declared := api.Node{PublicKey: "abc123"}
		enrichNodeWithDeclaredScope(&declared, declaredAnswer{RegionsCSV: "oki", Truncated: true}, true, nil)
		got := decode(declared)
		if len(got.State) == 0 {
			t.Error("scope_config_state missing on a declared node")
		}
		if string(got.Regions) != `["oki"]` {
			t.Errorf("declared_regions = %s, want [\"oki\"]", got.Regions)
		}
		if string(got.Truncated) != "true" {
			t.Errorf("declared_regions_truncated = %s, want true", got.Truncated)
		}

		undeclared := api.Node{PublicKey: "abc123"}
		enrichNodeWithDeclaredScope(&undeclared, declaredAnswer{}, false, nil)
		got = decode(undeclared)
		if len(got.State) == 0 {
			t.Error("scope_config_state missing on an undeclared node")
		}
		if len(got.Regions) != 0 || len(got.Truncated) != 0 {
			t.Errorf("declared_regions=%s declared_regions_truncated=%s, want both omitted", got.Regions, got.Truncated)
		}
	})
}

func TestEnrichNodeWithRelayInfo(t *testing.T) {
	relayed := time.Date(2026, 11, 6, 13, 0, 0, 123000000, time.UTC)

	t.Run("nil info leaves no fields", func(t *testing.T) {
		node := api.Node{PublicKey: "abc123"}
		enrichNodeWithRelayInfo(&node, nil)
		if node.LastRelayed != nil || node.RelayActive != nil || node.RelayWindowHours != nil ||
			node.RelayCount1h != nil || node.RelayCount24h != nil ||
			node.UnscopedRelayCount24h != nil || node.TransportedScopes != nil {
			t.Errorf("expected no relay fields with nil info, got %+v", node)
		}
	})

	t.Run("full info sets every field", func(t *testing.T) {
		node := api.Node{PublicKey: "abc123"}
		enrichNodeWithRelayInfo(&node, &RepeaterRelayInfo{
			LastRelayed:           relayed,
			RelayActive:           true,
			WindowHours:           24,
			RelayCount1h:          3,
			RelayCount24h:         40,
			UnscopedRelayCount24h: 1,
			TransportedScopes:     []string{"cin", "oki"},
		})
		if node.LastRelayed == nil || !node.LastRelayed.Equal(relayed) {
			t.Errorf("last_relayed = %v, want %s", node.LastRelayed, relayed)
		}
		if node.RelayActive == nil || !*node.RelayActive {
			t.Errorf("relay_active = %v, want true", node.RelayActive)
		}
		if node.RelayWindowHours == nil || *node.RelayWindowHours != 24 {
			t.Errorf("relay_window_hours = %v, want 24", node.RelayWindowHours)
		}
		if node.RelayCount1h == nil || *node.RelayCount1h != 3 {
			t.Errorf("relay_count_1h = %v, want 3", node.RelayCount1h)
		}
		if node.RelayCount24h == nil || *node.RelayCount24h != 40 {
			t.Errorf("relay_count_24h = %v, want 40", node.RelayCount24h)
		}
		if node.UnscopedRelayCount24h == nil || *node.UnscopedRelayCount24h != 1 {
			t.Errorf("unscoped_relay_count_24h = %v, want 1", node.UnscopedRelayCount24h)
		}
		if !slices.Equal(node.TransportedScopes, []string{"cin", "oki"}) {
			t.Errorf("transported_scopes = %v, want [cin oki]", node.TransportedScopes)
		}
	})

	t.Run("never-relayed repeater omits last_relayed", func(t *testing.T) {
		node := api.Node{PublicKey: "abc123"}
		enrichNodeWithRelayInfo(&node, &RepeaterRelayInfo{WindowHours: 24})
		if node.LastRelayed != nil {
			t.Errorf("last_relayed = %v, want nil for a zero LastRelayed", node.LastRelayed)
		}
		if node.RelayActive == nil || *node.RelayActive {
			t.Errorf("relay_active = %v, want false", node.RelayActive)
		}
		if node.RelayCount24h == nil || *node.RelayCount24h != 0 {
			t.Errorf("relay_count_24h = %v, want 0 (present, not omitted)", node.RelayCount24h)
		}
	})

	t.Run("JSON keys are snake_case", func(t *testing.T) {
		type wireKeys struct {
			LastRelayed           json.RawMessage `json:"last_relayed"`
			RelayActive           json.RawMessage `json:"relay_active"`
			RelayWindowHours      json.RawMessage `json:"relay_window_hours"`
			RelayCount1h          json.RawMessage `json:"relay_count_1h"`
			RelayCount24h         json.RawMessage `json:"relay_count_24h"`
			UnscopedRelayCount24h json.RawMessage `json:"unscoped_relay_count_24h"`
			TransportedScopes     json.RawMessage `json:"transported_scopes"`
		}
		decode := func(node api.Node) wireKeys {
			b, err := json.Marshal(node)
			if err != nil {
				t.Fatal(err)
			}
			var got wireKeys
			if err := json.Unmarshal(b, &got); err != nil {
				t.Fatal(err)
			}
			return got
		}

		node := api.Node{PublicKey: "abc123"}
		enrichNodeWithRelayInfo(&node, &RepeaterRelayInfo{
			LastRelayed:           relayed,
			RelayActive:           true,
			WindowHours:           24,
			RelayCount1h:          3,
			RelayCount24h:         40,
			UnscopedRelayCount24h: 1,
			TransportedScopes:     []string{"oki"},
		})
		got := decode(node)
		if string(got.LastRelayed) != `"2026-11-06T13:00:00Z"` {
			t.Errorf("last_relayed = %s, want \"2026-11-06T13:00:00Z\"", got.LastRelayed)
		}
		if string(got.RelayActive) != "true" {
			t.Errorf("relay_active = %s, want true", got.RelayActive)
		}
		if string(got.RelayWindowHours) != "24" {
			t.Errorf("relay_window_hours = %s, want 24", got.RelayWindowHours)
		}
		if string(got.RelayCount1h) != "3" {
			t.Errorf("relay_count_1h = %s, want 3", got.RelayCount1h)
		}
		if string(got.RelayCount24h) != "40" {
			t.Errorf("relay_count_24h = %s, want 40", got.RelayCount24h)
		}
		if string(got.UnscopedRelayCount24h) != "1" {
			t.Errorf("unscoped_relay_count_24h = %s, want 1", got.UnscopedRelayCount24h)
		}
		if string(got.TransportedScopes) != `["oki"]` {
			t.Errorf("transported_scopes = %s, want [\"oki\"]", got.TransportedScopes)
		}

		bare := api.Node{PublicKey: "abc123"}
		enrichNodeWithRelayInfo(&bare, nil)
		got = decode(bare)
		if len(got.LastRelayed) != 0 || len(got.RelayActive) != 0 || len(got.RelayCount24h) != 0 || len(got.TransportedScopes) != 0 {
			t.Errorf("relay keys present with nil info (last_relayed=%s relay_active=%s relay_count_24h=%s transported_scopes=%s); want all omitted",
				got.LastRelayed, got.RelayActive, got.RelayCount24h, got.TransportedScopes)
		}

		noScopes := api.Node{PublicKey: "abc123"}
		enrichNodeWithRelayInfo(&noScopes, &RepeaterRelayInfo{WindowHours: 24})
		got = decode(noScopes)
		if len(got.TransportedScopes) != 0 {
			t.Errorf("transported_scopes = %s, want omitted when there are none", got.TransportedScopes)
		}
	})
}

func TestEnrichNodeWithUsefulness(t *testing.T) {
	t.Run("nil usefulness leaves no fields", func(t *testing.T) {
		node := api.Node{PublicKey: "abc123"}
		enrichNodeWithUsefulness(&node, nil)
		if node.TrafficShareScore != nil || node.BridgeScore != nil || node.CoverageScore != nil ||
			node.RedundancyScore != nil || node.UsefulnessScore != nil || node.UsefulnessGrade != nil {
			t.Errorf("expected no usefulness fields with nil input, got %+v", node)
		}
	})

	t.Run("full usefulness sets every field", func(t *testing.T) {
		node := api.Node{PublicKey: "abc123"}
		enrichNodeWithUsefulness(&node, &NodeUsefulness{
			TrafficShareScore: 0.4,
			BridgeScore:       0.5,
			CoverageScore:     0.6,
			RedundancyScore:   0.7,
			UsefulnessScore:   0.55,
			UsefulnessGrade:   "B",
		})
		if node.TrafficShareScore == nil || *node.TrafficShareScore != 0.4 {
			t.Errorf("traffic_share_score = %v, want 0.4", node.TrafficShareScore)
		}
		if node.BridgeScore == nil || *node.BridgeScore != 0.5 {
			t.Errorf("bridge_score = %v, want 0.5", node.BridgeScore)
		}
		if node.CoverageScore == nil || *node.CoverageScore != 0.6 {
			t.Errorf("coverage_score = %v, want 0.6", node.CoverageScore)
		}
		if node.RedundancyScore == nil || *node.RedundancyScore != 0.7 {
			t.Errorf("redundancy_score = %v, want 0.7", node.RedundancyScore)
		}
		if node.UsefulnessScore == nil || *node.UsefulnessScore != 0.55 {
			t.Errorf("usefulness_score = %v, want 0.55", node.UsefulnessScore)
		}
		if node.UsefulnessGrade == nil || *node.UsefulnessGrade != "B" {
			t.Errorf("usefulness_grade = %v, want B", node.UsefulnessGrade)
		}
	})

	t.Run("cold start omits the grade but keeps the scores", func(t *testing.T) {
		node := api.Node{PublicKey: "abc123"}
		enrichNodeWithUsefulness(&node, &NodeUsefulness{})
		if node.UsefulnessGrade != nil {
			t.Errorf("usefulness_grade = %v, want nil when the grade is empty", *node.UsefulnessGrade)
		}
		if node.UsefulnessScore == nil || *node.UsefulnessScore != 0 {
			t.Errorf("usefulness_score = %v, want 0 (present, not omitted)", node.UsefulnessScore)
		}
	})

	t.Run("JSON keys are unchanged", func(t *testing.T) {
		type wireKeys struct {
			TrafficShare json.RawMessage `json:"traffic_share_score"`
			Bridge       json.RawMessage `json:"bridge_score"`
			Coverage     json.RawMessage `json:"coverage_score"`
			Redundancy   json.RawMessage `json:"redundancy_score"`
			Score        json.RawMessage `json:"usefulness_score"`
			Grade        json.RawMessage `json:"usefulness_grade"`
		}
		decode := func(node api.Node) wireKeys {
			b, err := json.Marshal(node)
			if err != nil {
				t.Fatal(err)
			}
			var got wireKeys
			if err := json.Unmarshal(b, &got); err != nil {
				t.Fatal(err)
			}
			return got
		}

		node := api.Node{PublicKey: "abc123"}
		enrichNodeWithUsefulness(&node, &NodeUsefulness{
			TrafficShareScore: 0.4,
			BridgeScore:       0.5,
			CoverageScore:     0.6,
			RedundancyScore:   0.7,
			UsefulnessScore:   0.55,
			UsefulnessGrade:   "B",
		})
		got := decode(node)
		if string(got.TrafficShare) != "0.4" {
			t.Errorf("traffic_share_score = %s, want 0.4", got.TrafficShare)
		}
		if string(got.Bridge) != "0.5" {
			t.Errorf("bridge_score = %s, want 0.5", got.Bridge)
		}
		if string(got.Coverage) != "0.6" {
			t.Errorf("coverage_score = %s, want 0.6", got.Coverage)
		}
		if string(got.Redundancy) != "0.7" {
			t.Errorf("redundancy_score = %s, want 0.7", got.Redundancy)
		}
		if string(got.Score) != "0.55" {
			t.Errorf("usefulness_score = %s, want 0.55", got.Score)
		}
		if string(got.Grade) != `"B"` {
			t.Errorf("usefulness_grade = %s, want \"B\"", got.Grade)
		}

		bare := api.Node{PublicKey: "abc123"}
		enrichNodeWithUsefulness(&bare, nil)
		got = decode(bare)
		if len(got.TrafficShare) != 0 || len(got.Score) != 0 || len(got.Grade) != 0 {
			t.Errorf("usefulness keys present with nil input (traffic_share_score=%s usefulness_score=%s usefulness_grade=%s); want all omitted",
				got.TrafficShare, got.Score, got.Grade)
		}
	})
}
