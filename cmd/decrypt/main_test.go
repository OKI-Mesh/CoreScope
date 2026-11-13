package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/OKI-Mesh/CoreScope/internal/channel"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

func TestSplitGroupText(t *testing.T) {
	// header: route=FLOOD(1), payload=GRP_TXT(5), version=0 → (5<<2)|1 = 0x15
	// path byte 0x00: 0 hops, 1-byte hashes
	frame := types.RawFrame{0x15, 0x00, 0x81, 0x12, 0x34} // channel_hash + mac + data
	path, payload, err := splitGroupText(frame)
	if err != nil {
		t.Fatal(err)
	}
	if len(path) != 0 {
		t.Fatalf("path = %v, want none", path)
	}
	if len(payload) != 3 || payload[0] != 0x81 {
		t.Fatalf("payload mismatch: %x", payload)
	}
}

func TestSplitGroupTextWithPath(t *testing.T) {
	// path byte 0x42: 2-byte hashes (0x42>>6 = 1), 2 hops (0x42&0x3F = 2)
	frame := types.RawFrame{0x15, 0x42, 0xAA, 0xBB, 0xCC, 0xDD, 0x81, 0x12, 0x34}
	path, payload, err := splitGroupText(frame)
	if err != nil {
		t.Fatal(err)
	}
	if len(path) != 2 || path[0] != "AABB" || path[1] != "CCDD" {
		t.Fatalf("path = %v, want [AABB CCDD]", path)
	}
	if len(payload) != 3 || payload[0] != 0x81 {
		t.Fatalf("payload mismatch: %x", payload)
	}
}

func TestSplitGroupTextTransport(t *testing.T) {
	// Transport flood: route=0, 4 bytes transport codes BEFORE path byte
	// header: (5<<2)|0 = 0x14
	frame := types.RawFrame{0x14, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0xAA, 0xBB, 0xCC}
	_, payload, err := splitGroupText(frame)
	if err != nil {
		t.Fatal(err)
	}
	if payload[0] != 0xAA {
		t.Fatalf("expected AA, got %02X", payload[0])
	}
}

func TestSplitGroupTextNotGRP(t *testing.T) {
	// payload type = ADVERT (4): (4<<2)|1 = 0x11
	if _, _, err := splitGroupText(types.RawFrame{0x11, 0x00, 0x01, 0x02}); err == nil {
		t.Fatal("expected error for non-GRP_TXT")
	}
}

func TestSplitGroupTextRejectsTruncatedPath(t *testing.T) {
	// path byte 0x42 promises 4 hop bytes, but only 2 follow and there is no payload
	if _, _, err := splitGroupText(types.RawFrame{0x15, 0x42, 0xAA, 0xBB}); err == nil {
		t.Fatal("expected error for a frame shorter than its path")
	}
}

func TestKeyDerivationConsistency(t *testing.T) {
	// Verify key derivation matches what the ingestor expects
	key := channel.DeriveKey("#wardriving")
	if len(key) != 16 {
		t.Fatalf("key len %d", len(key))
	}
	ch := channel.ChannelHash(key)
	if ch != 0x81 {
		// We know from fixture data that #wardriving has channelHashHex "81"
		t.Fatalf("channel hash %02X, expected 81", ch)
	}
}

func TestRenderIRC(t *testing.T) {
	msgs := []ChannelMessage{
		{Timestamp: "2026-04-12T03:45:12Z", Sender: "NodeA", Message: "Hello"},
		{Timestamp: "2026-04-12T03:46:01Z", Sender: "", Message: "No sender"},
	}
	out := string(renderIRC(msgs))
	if !strings.Contains(out, "[2026-04-12 03:45:12] <NodeA> Hello") {
		t.Fatalf("IRC output missing expected line: %s", out)
	}
	if !strings.Contains(out, "<???> No sender") {
		t.Fatalf("IRC output should use ??? for empty sender: %s", out)
	}
}

func TestRenderHTMLValid(t *testing.T) {
	msgs := []ChannelMessage{
		{Hash: "abc", Timestamp: "2026-04-12T00:00:00Z", Sender: "X", Message: "test", Channel: "#test"},
	}
	out := string(renderHTML(msgs, "#test"))
	if !strings.Contains(out, "<!DOCTYPE html>") {
		t.Fatal("not valid HTML")
	}
	if !strings.Contains(out, "#test") {
		t.Fatal("channel name missing")
	}
	if !strings.Contains(out, "</html>") {
		t.Fatal("HTML not closed")
	}
}

func TestJSONOutputParseable(t *testing.T) {
	msgs := []ChannelMessage{
		{Hash: "abc", Timestamp: "2026-04-12T00:00:00Z", Sender: "X", Message: "hi", Channel: "#test"},
	}
	data, err := json.MarshalIndent(msgs, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	var parsed []ChannelMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("JSON not parseable: %v", err)
	}
	if len(parsed) != 1 || parsed[0].Sender != "X" {
		t.Fatalf("parsed mismatch: %+v", parsed)
	}
}
