package decoder

import "testing"

func TestDecodePacket_TraceHopsCompleted(t *testing.T) {
	// header: route=FLOOD(1), payload=TRACE(9), version=0 → (0<<6)|(9<<2)|1 = 0x25
	// path_length: hash_size bits=0b00 (1-byte), hash_count=2 (2 SNR bytes) → 0x02
	// path: 2 SNR bytes: 0xAA, 0xBB
	// payload: tag(4 LE) + authCode(4 LE) + flags(1) + 4 hop hashes (1 byte each)
	frame := "2502AABB" + // header + path_length + 2 SNR bytes
		"01000000" + // tag = 1
		"02000000" + // authCode = 2
		"00" + // flags = 0
		"DEADBEEF" // 4 hops (1-byte hash each)

	pkt, err := DecodePacket(frame, nil, false)
	if err != nil {
		t.Fatalf("DecodePacket error: %v", err)
	}
	if pkt.Payload.Type != "TRACE" {
		t.Fatalf("expected TRACE, got %s", pkt.Payload.Type)
	}
	// Full intended route = 4 hops from payload
	if len(pkt.Path.Hops) != 4 {
		t.Errorf("expected 4 hops, got %d: %v", len(pkt.Path.Hops), pkt.Path.Hops)
	}
	// HopsCompleted = 2 (from header path SNR count)
	if pkt.Path.HopsCompleted == nil {
		t.Fatal("expected HopsCompleted to be set")
	}
	if *pkt.Path.HopsCompleted != 2 {
		t.Errorf("expected HopsCompleted=2, got %d", *pkt.Path.HopsCompleted)
	}
	// FLOOD routing for TRACE is anomalous
	if pkt.Anomaly == "" {
		t.Error("expected anomaly flag for FLOOD-routed TRACE")
	}
}

func TestDecodePacket_TraceNoSNR(t *testing.T) {
	// TRACE with 0 SNR bytes (trace hasn't been forwarded yet)
	// path_length: hash_size=0b00 (1-byte), hash_count=0 → 0x00
	frame := "2500" + // header + path_length (0 hops in header)
		"01000000" + // tag
		"02000000" + // authCode
		"00" + // flags
		"AABBCC" // 3 hops intended

	pkt, err := DecodePacket(frame, nil, false)
	if err != nil {
		t.Fatalf("DecodePacket error: %v", err)
	}
	if pkt.Path.HopsCompleted == nil {
		t.Fatal("expected HopsCompleted to be set")
	}
	if *pkt.Path.HopsCompleted != 0 {
		t.Errorf("expected HopsCompleted=0, got %d", *pkt.Path.HopsCompleted)
	}
	if len(pkt.Path.Hops) != 3 {
		t.Errorf("expected 3 hops, got %d", len(pkt.Path.Hops))
	}
}

func TestDecodePacket_TraceFullyCompleted(t *testing.T) {
	// TRACE where all hops completed (SNR count = hop count)
	// path_length: hash_size=0b00 (1-byte), hash_count=3 → 0x03
	frame := "2503AABBCC" + // header + path_length + 3 SNR bytes
		"01000000" + // tag
		"02000000" + // authCode
		"00" + // flags
		"DDEEFF" // 3 hops intended

	pkt, err := DecodePacket(frame, nil, false)
	if err != nil {
		t.Fatalf("DecodePacket error: %v", err)
	}
	if pkt.Path.HopsCompleted == nil {
		t.Fatal("expected HopsCompleted to be set")
	}
	if *pkt.Path.HopsCompleted != 3 {
		t.Errorf("expected HopsCompleted=3, got %d", *pkt.Path.HopsCompleted)
	}
	if len(pkt.Path.Hops) != 3 {
		t.Errorf("expected 3 hops, got %d", len(pkt.Path.Hops))
	}
}

func TestDecodePacket_TraceFlags1_TwoBytePathSz(t *testing.T) {
	// TRACE with flags=1 → path_sz = 1 << (1 & 0x03) = 2-byte hashes
	// Firmware always sends TRACE as DIRECT (route_type=2), so header byte =
	// (0<<6)|(9<<2)|2 = 0x26. path_length 0x00 = 0 SNR bytes.
	frame := "2600" + // header (DIRECT+TRACE) + path_length (0 SNR)
		"01000000" + // tag
		"02000000" + // authCode
		"01" + // flags = 1 → path_sz = 2
		"AABBCCDD" // 4 bytes = 2 hops of 2-byte each

	pkt, err := DecodePacket(frame, nil, false)
	if err != nil {
		t.Fatalf("DecodePacket error: %v", err)
	}
	if len(pkt.Path.Hops) != 2 {
		t.Errorf("expected 2 hops (2-byte path_sz), got %d: %v", len(pkt.Path.Hops), pkt.Path.Hops)
	}
	if pkt.Path.HashSize != 2 {
		t.Errorf("expected HashSize=2, got %d", pkt.Path.HashSize)
	}
	if pkt.Anomaly != "" {
		t.Errorf("expected no anomaly for DIRECT TRACE, got %q", pkt.Anomaly)
	}
}

func TestDecodePacket_TraceFlags2_FourBytePathSz(t *testing.T) {
	// TRACE with flags=2 → path_sz = 1 << (2 & 0x03) = 4-byte hashes
	// DIRECT route_type (0x26)
	frame := "2600" + // header (DIRECT+TRACE) + path_length (0 SNR)
		"01000000" + // tag
		"02000000" + // authCode
		"02" + // flags = 2 → path_sz = 4
		"AABBCCDD11223344" // 8 bytes = 2 hops of 4-byte each

	pkt, err := DecodePacket(frame, nil, false)
	if err != nil {
		t.Fatalf("DecodePacket error: %v", err)
	}
	if len(pkt.Path.Hops) != 2 {
		t.Errorf("expected 2 hops (4-byte path_sz), got %d: %v", len(pkt.Path.Hops), pkt.Path.Hops)
	}
	if pkt.Path.HashSize != 4 {
		t.Errorf("expected HashSize=4, got %d", pkt.Path.HashSize)
	}
}

func TestDecodePacket_TracePathSzUnevenPayload(t *testing.T) {
	// TRACE with flags=1 → path_sz=2, but 5 bytes of path data (not evenly divisible)
	// Should produce 2 hops (4 bytes) and ignore the trailing byte
	frame := "2600" + // header (DIRECT+TRACE) + path_length (0 SNR)
		"01000000" + // tag
		"02000000" + // authCode
		"01" + // flags = 1 → path_sz = 2
		"AABBCCDDEE" // 5 bytes → 2 hops, 1 byte remainder ignored

	pkt, err := DecodePacket(frame, nil, false)
	if err != nil {
		t.Fatalf("DecodePacket error: %v", err)
	}
	if len(pkt.Path.Hops) != 2 {
		t.Errorf("expected 2 hops (trailing byte ignored), got %d: %v", len(pkt.Path.Hops), pkt.Path.Hops)
	}
}

func TestDecodePacket_TraceTransportDirect(t *testing.T) {
	// TRACE via TRANSPORT_DIRECT (route_type=3) — includes 4 transport code bytes
	// header: (0<<6)|(9<<2)|3 = 0x27
	frame := "27" + // header (TRANSPORT_DIRECT+TRACE)
		"AABB" + "CCDD" + // transport codes (2+2 bytes)
		"02" + // path_length: hash_count=2 SNR bytes
		"EEFF" + // 2 SNR bytes
		"01000000" + // tag
		"02000000" + // authCode
		"00" + // flags = 0 → path_sz = 1
		"112233" // 3 hops (1-byte each)

	pkt, err := DecodePacket(frame, nil, false)
	if err != nil {
		t.Fatalf("DecodePacket error: %v", err)
	}
	if pkt.TransportCodes == nil {
		t.Fatal("expected transport codes for TRANSPORT_DIRECT")
	}
	if pkt.TransportCodes.Code1 != "AABB" {
		t.Errorf("expected Code1=AABB, got %s", pkt.TransportCodes.Code1)
	}
	if len(pkt.Path.Hops) != 3 {
		t.Errorf("expected 3 hops, got %d: %v", len(pkt.Path.Hops), pkt.Path.Hops)
	}
	if pkt.Path.HopsCompleted == nil || *pkt.Path.HopsCompleted != 2 {
		t.Errorf("expected HopsCompleted=2, got %v", pkt.Path.HopsCompleted)
	}
	if pkt.Anomaly != "" {
		t.Errorf("expected no anomaly for TRANSPORT_DIRECT TRACE, got %q", pkt.Anomaly)
	}
}

func TestDecodePacket_TraceFloodRouteAnomaly(t *testing.T) {
	// TRACE via FLOOD (route_type=1) — anomalous per firmware (firmware only
	// sends TRACE as DIRECT). Should still parse but flag the anomaly.
	frame := "2500" + // header (FLOOD+TRACE) + path_length (0 SNR)
		"01000000" + // tag
		"02000000" + // authCode
		"01" + // flags = 1 → path_sz = 2
		"AABBCCDD" // 4 bytes = 2 hops of 2-byte each

	pkt, err := DecodePacket(frame, nil, false)
	if err != nil {
		t.Fatalf("should not crash on anomalous FLOOD+TRACE: %v", err)
	}
	if len(pkt.Path.Hops) != 2 {
		t.Errorf("expected 2 hops even for anomalous FLOOD route, got %d", len(pkt.Path.Hops))
	}
	if pkt.Anomaly == "" {
		t.Error("expected anomaly flag for FLOOD-routed TRACE, got empty string")
	}
}

func TestDecodePacket_TraceSNRValues(t *testing.T) {
	// TRACE packet with 3 SNR bytes in header path:
	// SNR byte 0: 0x14 = int8(20) → 20/4.0 = 5.0 dB
	// SNR byte 1: 0xF4 = int8(-12) → -12/4.0 = -3.0 dB
	// SNR byte 2: 0x08 = int8(8) → 8/4.0 = 2.0 dB
	// header: DIRECT+TRACE = (0<<6)|(9<<2)|2 = 0x26
	// path_length: hash_size=0b00 (1-byte), hash_count=3 → 0x03
	frame := "2603" + "14F408" + // header + path_byte + 3 SNR bytes
		"01000000" + // tag
		"02000000" + // authCode
		"00" + // flags=0 → path_sz=1
		"AABBCCDD" // 4 route hops (1-byte each)

	pkt, err := DecodePacket(frame, nil, false)
	if err != nil {
		t.Fatalf("DecodePacket error: %v", err)
	}
	if pkt.Payload.SNRValues == nil {
		t.Fatal("expected SNRValues to be populated")
	}
	if len(pkt.Payload.SNRValues) != 3 {
		t.Fatalf("expected 3 SNR values, got %d", len(pkt.Payload.SNRValues))
	}
	expected := []float64{5.0, -3.0, 2.0}
	for i, want := range expected {
		if pkt.Payload.SNRValues[i] != want {
			t.Errorf("SNRValues[%d] = %v, want %v", i, pkt.Payload.SNRValues[i], want)
		}
	}
}

func TestDecodePacket_TraceNoSNRValues(t *testing.T) {
	// TRACE with 0 SNR bytes → SNRValues should be nil/empty
	frame := "2600" + // header + path_byte (0 hops)
		"01000000" + // tag
		"02000000" + // authCode
		"00" + // flags
		"AABB" // 2 route hops

	pkt, err := DecodePacket(frame, nil, false)
	if err != nil {
		t.Fatalf("DecodePacket error: %v", err)
	}
	if len(pkt.Payload.SNRValues) != 0 {
		t.Errorf("expected empty SNRValues, got %v", pkt.Payload.SNRValues)
	}
}
