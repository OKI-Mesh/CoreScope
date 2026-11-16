package decoder

import (
	"strings"
	"testing"
)

func TestDecodeRawCustomPayloads(t *testing.T) {
	cases := []struct {
		name    string
		buf     []byte
		wantLen int
		wantTag string
		wantHex string
	}{
		{"nil payload", nil, 0, "", ""},
		{"empty payload", []byte{}, 0, "", ""},
		{"one byte", []byte{0x05}, 1, "05", "05"},
		{"tag is uppercase, raw hex is lowercase", []byte{0xAB, 0xCD}, 2, "AB", "abcd"},
		{"zero first byte keeps two digits", []byte{0x00, 0x01}, 2, "00", "0001"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := decodeRawCustom(c.buf)
			if p.Type != "RAW_CUSTOM" {
				t.Errorf("Type = %q, want RAW_CUSTOM", p.Type)
			}
			if p.RawLength == nil {
				t.Fatalf("RawLength is nil, want %d", c.wantLen)
			}
			if *p.RawLength != c.wantLen {
				t.Errorf("RawLength = %d, want %d", *p.RawLength, c.wantLen)
			}
			if p.FirstByteTag != c.wantTag {
				t.Errorf("FirstByteTag = %q, want %q", p.FirstByteTag, c.wantTag)
			}
			if p.RawHex != c.wantHex {
				t.Errorf("RawHex = %q, want %q", p.RawHex, c.wantHex)
			}
		})
	}
}

func TestDecodePacketRawCustomPayloadSizeLimit(t *testing.T) {
	atLimit := "7D00" + strings.Repeat("AB", 184)
	pkt, err := DecodePacket(atLimit, nil, false)
	if err != nil {
		t.Fatalf("184-byte payload: %v", err)
	}
	if pkt.Payload.RawLength == nil || *pkt.Payload.RawLength != 184 {
		t.Errorf("RawLength = %v, want 184", pkt.Payload.RawLength)
	}
	if pkt.Payload.FirstByteTag != "AB" {
		t.Errorf("FirstByteTag = %q, want AB", pkt.Payload.FirstByteTag)
	}

	overLimit := "7D00" + strings.Repeat("AB", 185)
	if pkt, err := DecodePacket(overLimit, nil, false); err == nil {
		t.Errorf("185-byte payload: expected an error, got pkt=%+v", pkt)
	}
}
