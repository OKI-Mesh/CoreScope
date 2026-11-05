package main

import (
	"encoding/json"
	"testing"
)

func TestEnrichNodeWithMultiByte(t *testing.T) {
	t.Run("nil entry leaves no fields", func(t *testing.T) {
		node := NodeResponse{PublicKey: "abc123"}
		EnrichNodeWithMultiByte(&node, nil)
		if node.NodeMultiByte != nil {
			t.Errorf("expected no multi-byte group with nil entry, got %+v", node.NodeMultiByte)
		}
	})

	t.Run("confirmed entry sets fields", func(t *testing.T) {
		node := NodeResponse{PublicKey: "abc123"}
		entry := &MultiByteCapEntry{
			Status:      "confirmed",
			Evidence:    "advert",
			MaxHashSize: 2,
		}
		EnrichNodeWithMultiByte(&node, entry)
		if node.NodeMultiByte == nil {
			t.Fatal("expected multi-byte group to be set")
		}
		if node.NodeMultiByte.Status != "confirmed" {
			t.Errorf("expected confirmed, got %v", node.NodeMultiByte.Status)
		}
		if node.NodeMultiByte.Evidence != "advert" {
			t.Errorf("expected advert, got %v", node.NodeMultiByte.Evidence)
		}
		if node.NodeMultiByte.MaxHashSize != 2 {
			t.Errorf("expected 2, got %v", node.NodeMultiByte.MaxHashSize)
		}
	})

	t.Run("suspected entry sets fields", func(t *testing.T) {
		node := NodeResponse{PublicKey: "abc123"}
		entry := &MultiByteCapEntry{
			Status:      "suspected",
			Evidence:    "path",
			MaxHashSize: 2,
		}
		EnrichNodeWithMultiByte(&node, entry)
		if node.NodeMultiByte == nil || node.NodeMultiByte.Status != "suspected" {
			t.Errorf("expected suspected, got %+v", node.NodeMultiByte)
		}
	})

	t.Run("unknown entry sets status unknown", func(t *testing.T) {
		node := NodeResponse{PublicKey: "abc123"}
		entry := &MultiByteCapEntry{
			Status:      "unknown",
			MaxHashSize: 1,
		}
		EnrichNodeWithMultiByte(&node, entry)
		if node.NodeMultiByte == nil || node.NodeMultiByte.Status != "unknown" {
			t.Errorf("expected unknown, got %+v", node.NodeMultiByte)
		}
	})

	// The old test asserted the JSON key names through the map. Keep that
	// guarantee: the frontend reads these exact keys.
	t.Run("JSON keys are unchanged", func(t *testing.T) {
		node := NodeResponse{PublicKey: "abc123"}
		EnrichNodeWithMultiByte(&node, &MultiByteCapEntry{Status: "confirmed", Evidence: "advert", MaxHashSize: 2})
		b, err := json.Marshal(node)
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]interface{}
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatal(err)
		}
		if got["multi_byte_status"] != "confirmed" {
			t.Errorf("multi_byte_status = %v, want confirmed", got["multi_byte_status"])
		}
		if got["multi_byte_evidence"] != "advert" {
			t.Errorf("multi_byte_evidence = %v, want advert", got["multi_byte_evidence"])
		}
		if got["multi_byte_max_hash_size"] != float64(2) { // JSON numbers decode as float64
			t.Errorf("multi_byte_max_hash_size = %v, want 2", got["multi_byte_max_hash_size"])
		}

		// With a nil entry the keys must be absent, not null.
		bare := NodeResponse{PublicKey: "abc123"}
		EnrichNodeWithMultiByte(&bare, nil)
		b, _ = json.Marshal(bare)
		got = nil
		_ = json.Unmarshal(b, &got)
		if _, ok := got["multi_byte_status"]; ok {
			t.Error("multi_byte_status present with nil entry; want the key omitted")
		}
	})
}
