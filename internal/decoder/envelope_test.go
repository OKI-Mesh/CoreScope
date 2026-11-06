package decoder

import (
	"bytes"
	"strings"
	"testing"

	"github.com/OKI-Mesh/CoreScope/internal/types"
)

func TestParseEnvelopeFlood(t *testing.T) {
	// header 0x15: FLOOD, GRP_TXT. Path byte 0x02: two 1-byte hops.
	frame := []byte{0x15, 0x02, 0xAA, 0xBB, 0x81, 0x12, 0x34}
	env, err := ParseEnvelope(frame)
	if err != nil {
		t.Fatal(err)
	}
	if env.Header.PayloadType != types.PayloadGRP_TXT || env.Header.RouteType != types.RouteFlood {
		t.Errorf("header = %+v", env.Header)
	}
	if env.TransportCodes != nil {
		t.Errorf("transport codes = %+v, want none", env.TransportCodes)
	}
	if len(env.Path.Hops) != 2 || env.Path.Hops[0] != "AA" || env.Path.Hops[1] != "BB" {
		t.Errorf("hops = %v, want [AA BB]", env.Path.Hops)
	}
	if env.PathByte != 0x02 {
		t.Errorf("path byte = %#x, want 0x02", env.PathByte)
	}
	if !bytes.Equal(env.Payload, []byte{0x81, 0x12, 0x34}) {
		t.Errorf("payload = %x", env.Payload)
	}
}

func TestParseEnvelopeTransport(t *testing.T) {
	// header 0x14: TRANSPORT_FLOOD, GRP_TXT. Codes AABB CCDD, then path byte 0, then payload.
	frame := []byte{0x14, 0xAA, 0xBB, 0xCC, 0xDD, 0x00, 0x81, 0x12, 0x34}
	env, err := ParseEnvelope(frame)
	if err != nil {
		t.Fatal(err)
	}
	if env.TransportCodes == nil || env.TransportCodes.Code1 != "AABB" || env.TransportCodes.Code2 != "CCDD" {
		t.Fatalf("transport codes = %+v, want AABB/CCDD", env.TransportCodes)
	}
	if !bytes.Equal(env.Payload, []byte{0x81, 0x12, 0x34}) {
		t.Errorf("payload = %x", env.Payload)
	}
}

func TestParseEnvelopeEmptyPayloadIsValid(t *testing.T) {
	env, err := ParseEnvelope([]byte{0x15, 0x00})
	if err != nil {
		t.Fatal(err)
	}
	if len(env.Payload) != 0 {
		t.Errorf("payload = %x, want empty", env.Payload)
	}
}

func TestParseEnvelopePayloadCap(t *testing.T) {
	atCap := append([]byte{0x15, 0x00}, make([]byte, maxPacketPayload)...)
	if _, err := ParseEnvelope(atCap); err != nil {
		t.Errorf("payload of exactly %d bytes rejected: %v", maxPacketPayload, err)
	}
	overCap := append([]byte{0x15, 0x00}, make([]byte, maxPacketPayload+1)...)
	_, err := ParseEnvelope(overCap)
	if err == nil || !strings.Contains(err.Error(), "MAX_PACKET_PAYLOAD") {
		t.Errorf("payload over the cap: err = %v, want a MAX_PACKET_PAYLOAD error", err)
	}
}

func TestParseEnvelopeRejectsMalformed(t *testing.T) {
	cases := []struct {
		name  string
		frame []byte
	}{
		{"empty", nil},
		{"header only", []byte{0x15}},
		{"transport codes cut short", []byte{0x14, 0xAA}},
		{"no path byte after transport codes", []byte{0x14, 0xAA, 0xBB, 0xCC, 0xDD}},
		{"path claims more than the buffer holds", []byte{0x15, 0x42, 0xAA, 0xBB}},
		{"reserved hash size 4", []byte{0x15, 0xC0, 0xAA}},
	}
	for _, c := range cases {
		if _, err := ParseEnvelope(c.frame); err == nil {
			t.Errorf("%s: accepted %x, want an error", c.name, c.frame)
		}
	}
}
