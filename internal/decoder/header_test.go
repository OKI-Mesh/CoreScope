package decoder

import (
	"testing"

	"github.com/OKI-Mesh/CoreScope/internal/types"
)

func TestDecodeHeader_TransportFlood(t *testing.T) {
	// Route type 0 = TRANSPORT_FLOOD, payload type 5 = GRP_TXT, version 0
	// Header byte: (0 << 6) | (5 << 2) | 0 = 0x14
	h := decodeHeader(0x14)
	if h.RouteType != types.RouteTransportFlood {
		t.Errorf("expected RouteTransportFlood (0), got %d", h.RouteType)
	}
	if h.RouteTypeName != "TRANSPORT_FLOOD" {
		t.Errorf("expected TRANSPORT_FLOOD, got %s", h.RouteTypeName)
	}
	if h.PayloadType != types.PayloadGRP_TXT {
		t.Errorf("expected PayloadGRP_TXT (5), got %d", h.PayloadType)
	}
}

func TestDecodeHeader_TransportDirect(t *testing.T) {
	// Route type 3 = TRANSPORT_DIRECT, payload type 2 = TXT_MSG, version 0
	// Header byte: (0 << 6) | (2 << 2) | 3 = 0x0B
	h := decodeHeader(0x0B)
	if h.RouteType != types.RouteTransportDirect {
		t.Errorf("expected RouteTransportDirect (3), got %d", h.RouteType)
	}
	if h.RouteTypeName != "TRANSPORT_DIRECT" {
		t.Errorf("expected TRANSPORT_DIRECT, got %s", h.RouteTypeName)
	}
}

func TestDecodeHeader_Flood(t *testing.T) {
	// Route type 1 = FLOOD, payload type 4 = ADVERT
	// Header byte: (0 << 6) | (4 << 2) | 1 = 0x11
	h := decodeHeader(0x11)
	if h.RouteType != types.RouteFlood {
		t.Errorf("expected RouteFlood (1), got %d", h.RouteType)
	}
	if h.RouteTypeName != "FLOOD" {
		t.Errorf("expected FLOOD, got %s", h.RouteTypeName)
	}
}

func TestDecodePacket_TransportFloodHasCodes(t *testing.T) {
	// Minimal TRANSPORT_FLOOD packet:
	// Header 0x14 (route=0/T_FLOOD, payload=5/GRP_TXT)
	// Transport codes: AABB CCDD (4 bytes)
	// Path byte: 0x00 (hashSize=1, hashCount=0)
	// Payload: some bytes for GRP_TXT
	frame := "14AABBCCDD00112233445566778899"
	pkt, err := DecodePacket(frame, nil, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pkt.TransportCodes == nil {
		t.Fatal("expected transport codes to be present")
	}
	if pkt.TransportCodes.Code1 != "AABB" {
		t.Errorf("expected Code1=AABB, got %s", pkt.TransportCodes.Code1)
	}
	if pkt.TransportCodes.Code2 != "CCDD" {
		t.Errorf("expected Code2=CCDD, got %s", pkt.TransportCodes.Code2)
	}
}

func TestDecodePacket_FloodHasNoCodes(t *testing.T) {
	// Header 0x11 (route=1/FLOOD, payload=4/ADVERT)
	// Path byte: 0x00 (no hops), then some payload bytes
	frame := "110011223344556677889900AABBCCDD"
	pkt, err := DecodePacket(frame, nil, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pkt.TransportCodes != nil {
		t.Error("expected no transport codes for FLOOD route")
	}
}
