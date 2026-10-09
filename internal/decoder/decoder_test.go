package decoder

import (
	"crypto/aes"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"
	"strings"
	"testing"

	"github.com/OKI-Mesh/CoreScope/internal/packetpath"
	"github.com/OKI-Mesh/CoreScope/internal/sigvalidate"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

func TestNormalizeChannelName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		// Known channel: "public" should be normalized to "Public"
		{"public", "Public"},
		{"Public", "Public"},
		{"PUBLIC", "Public"},
		// Hashtag channels should be left untouched
		{"#LongFast", "#LongFast"},
		{"#wardrive", "#wardrive"},
		// Custom/unknown channels should be left untouched
		{"myChannel", "myChannel"},
		{"testchannel", "testchannel"},
		// Empty string
		{"", ""},
	}

	for _, tt := range tests {
		got := NormalizeChannelName(tt.input)
		if got != tt.expected {
			t.Errorf("NormalizeChannelName(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}
func TestAdvertRole(t *testing.T) {
	// advertRole now keys off AdvertFlags.Type (firmware ADV_TYPE_*) — see
	// firmware/src/helpers/AdvertDataHelpers.h:7-12 and issue #1279 P1 #3.
	tests := []struct {
		name  string
		flags *AdvertFlags
		want  string
	}{
		{"none (type 0)", &AdvertFlags{Type: 0}, "none"},
		{"companion (type 1)", &AdvertFlags{Type: 1, Chat: true}, "companion"},
		{"repeater (type 2)", &AdvertFlags{Type: 2, Repeater: true}, "repeater"},
		{"room (type 3)", &AdvertFlags{Type: 3, Room: true}, "room"},
		{"sensor (type 4)", &AdvertFlags{Type: 4, Sensor: true}, "sensor"},
		{"future type-5", &AdvertFlags{Type: 5}, "type-5"},
		{"nil flags falls back to companion", nil, "companion"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AdvertRole(tt.flags)
			if got != tt.want {
				t.Errorf("AdvertRole(%+v) = %s, want %s", tt.flags, got, tt.want)
			}
		})
	}
}

// hmacSHA256 computes HMAC-SHA256 for test use.
func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func TestDecodeHeaderRouteTypes(t *testing.T) {
	tests := []struct {
		b    byte
		rt   types.RouteType
		name string
	}{
		{0x00, types.RouteTransportFlood, "TRANSPORT_FLOOD"},
		{0x01, types.RouteFlood, "FLOOD"},
		{0x02, types.RouteDirect, "DIRECT"},
		{0x03, types.RouteTransportDirect, "TRANSPORT_DIRECT"},
	}
	for _, tt := range tests {
		h := decodeHeader(tt.b)
		if h.RouteType != tt.rt {
			t.Errorf("header 0x%02X: routeType=%v, want %v", tt.b, h.RouteType, tt.rt)
		}
		if h.RouteTypeName != tt.name {
			t.Errorf("header 0x%02X: routeTypeName=%s, want %s", tt.b, h.RouteTypeName, tt.name)
		}
	}
}

func TestCountNonPrintableRuneError(t *testing.T) {
	// Invalid UTF-8 bytes
	got := countNonPrintable(string([]byte{0xff, 0xfe}))
	if got != 2 {
		t.Errorf("countNonPrintable invalid UTF-8 = %d, want 2", got)
	}

	// Normal text
	if countNonPrintable("hello") != 0 {
		t.Error("normal text should have 0 non-printable")
	}
}
func TestDecryptChannelMessageEmptyCiphertext(t *testing.T) {
	_, err := decryptChannelMessage("", "0011", "00112233445566778899aabbccddeeff")
	if err == nil {
		t.Error("empty ciphertext should error")
	}
}

func TestDecryptChannelMessageMACFailsBeforeAlignment(t *testing.T) {
	// 15 bytes of ciphertext — not aligned to AES block size (16).
	// However, decryptChannelMessage checks MAC before alignment, so with
	// random ciphertext the HMAC won't match and it errors on MAC first.
	_, err := decryptChannelMessage("00112233445566778899aabbccddee", "0011", "00112233445566778899aabbccddeeff")
	if err == nil {
		t.Error("should error (MAC mismatch)")
	}
	if err.Error() != "MAC verification failed" {
		t.Errorf("expected MAC error, got: %v", err)
	}
}

func TestDecryptChannelMessageNotAligned(t *testing.T) {
	// To actually exercise the alignment branch, we need ciphertext whose
	// HMAC-SHA256 first 2 bytes match the provided MAC, but whose length
	// is not a multiple of 16. We craft this by computing the real MAC.
	key := "00112233445566778899aabbccddeeff"
	// 15 bytes of ciphertext (not aligned to 16)
	ciphertextBytes := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}
	ciphertextHex := hex.EncodeToString(ciphertextBytes)

	// Compute real HMAC to pass the MAC check
	keyBytes, _ := hex.DecodeString(key)
	channelSecret := make([]byte, 32)
	copy(channelSecret, keyBytes)

	h := hmacSHA256(channelSecret, ciphertextBytes)
	macHex := hex.EncodeToString(h[:2])

	_, err := decryptChannelMessage(ciphertextHex, macHex, key)
	if err == nil {
		t.Error("unaligned ciphertext should error")
	}
	if err.Error() != "ciphertext not aligned to AES block size" {
		t.Errorf("expected alignment error, got: %v", err)
	}
}

// --- decoder.go: ComputeContentHash with transport route short after transport codes ---

func TestComputeContentHashTransportShortAfterCodes(t *testing.T) {
	// Transport route (0x00), 4 bytes transport codes, but then nothing (no path byte).
	// offset after transport codes = 5, which equals len(buf), so fallback fires.
	// rawHex is 10 chars (< 16), so fallback returns rawHex itself.
	rawHex := "00AABBCCDD"
	got := ComputeContentHash(rawHex)
	if got != rawHex {
		t.Errorf("got=%s, want fallback=%s", got, rawHex)
	}
}

// --- decoder.go: DecodePacket no path byte after transport ---

func TestDecodePacketNoPathByteAfterHeader(t *testing.T) {
	// Non-transport route, but only header byte (no path byte)
	// Actually 0A alone = 1 byte, but we need >= 2
	// Header + exactly at offset boundary
	_, err := DecodePacket("0A", nil, false)
	if err == nil {
		t.Error("should error - too short")
	}
}

// --- decoder.go: ValidateAdvert name too long ---

// Test ValidateAdvert pubkey too short
func TestValidateAdvertPubkeyTooShort(t *testing.T) {
	p := &Payload{PubKey: "aabb"}
	ok, _ := ValidateAdvert(p)
	if ok {
		t.Error("short pubkey should fail")
	}
}

// --- decoder.go: decodeTrace with extra path data ---

func TestDecodeTraceWithPath(t *testing.T) {
	buf := make([]byte, 15)
	// tag (4) + authCode (4) + flags (1) + path data (6)
	buf[0] = 0x01 // tag
	buf[4] = 0x02 // authCode
	buf[8] = 0x03 // flags
	buf[9] = 0xAA
	buf[10] = 0xBB
	buf[11] = 0xCC
	buf[12] = 0xDD
	buf[13] = 0xEE
	buf[14] = 0xFF

	p := decodeTrace(buf)
	if p.PathData == "" {
		t.Error("should have path data")
	}
	if p.TraceFlags == nil || *p.TraceFlags != 3 {
		t.Errorf("flags=%v, want 3", p.TraceFlags)
	}
}

// --- decoder.go: decodeAdvert with name but no null terminator ---

func TestValidateAdvertNameTooLong(t *testing.T) {
	longName := ""
	for i := 0; i < 65; i++ {
		longName += "A"
	}
	p := &Payload{PubKey: "aabbccdd00112233445566778899aabb", Name: longName}
	ok, reason := ValidateAdvert(p)
	if ok {
		t.Error("name >64 chars should fail")
	}
	if reason == "" {
		t.Error("should have reason")
	}
}
func TestDecodeAdvertNameNoNull(t *testing.T) {
	buf := make([]byte, 115)
	for i := 0; i < 32; i++ {
		buf[i] = byte(i + 1)
	}
	for i := 36; i < 100; i++ {
		buf[i] = 0xCC
	}
	// flags: hasName(0x80) | type=1 = 0x81
	buf[100] = 0x81
	// Name without null terminator — goes to end of buffer
	copy(buf[101:], []byte("LongNameNoNull"))

	p := decodeAdvert(buf[:115], false)
	if p.Name != "LongNameNoNull" {
		t.Errorf("name=%q, want LongNameNoNull", p.Name)
	}
}
func TestDecodeAdvertSensorNoName(t *testing.T) {
	buf := make([]byte, 108)
	for i := 0; i < 32; i++ {
		buf[i] = byte(i + 1)
	}
	for i := 36; i < 100; i++ {
		buf[i] = 0xCC
	}
	// flags: sensor(4) = 0x04 (no hasName)
	buf[100] = 0x04
	// telemetry right after flags: battery=3700 (0x0E74), temp=2500 (25.00°C)
	buf[101] = 0x74
	buf[102] = 0x0E
	buf[103] = 0xC4
	buf[104] = 0x09

	p := decodeAdvert(buf[:105], false)
	if p.Error != "" {
		t.Fatalf("error: %s", p.Error)
	}
	if p.Name != "" {
		t.Errorf("name=%q, want empty", p.Name)
	}
	if p.BatteryMv == nil || *p.BatteryMv != 3700 {
		t.Errorf("battery_mv=%v, want 3700", p.BatteryMv)
	}
}

func TestComputeContentHashPathOverflow(t *testing.T) {
	// path byte 0xFF = hashSize=4, hashCount=63 = 252 bytes needed, but only a few available
	// payloadStart (2 + 252 = 254) > len(buf) (6), so fallback fires.
	// rawHex is 12 chars (< 16), so fallback returns rawHex itself.
	rawHex := "0AFF" + "AABBCCDD"
	got := ComputeContentHash(rawHex)
	if got != rawHex {
		t.Errorf("got=%s, want fallback=%s", got, rawHex)
	}
}

func TestDecryptChannelMessageBadMACHex(t *testing.T) {
	_, err := decryptChannelMessage("00112233445566778899aabbccddeeff", "ZZ", "00112233445566778899aabbccddeeff")
	if err == nil {
		t.Error("invalid MAC hex should error")
	}
}

func TestDecryptChannelMessageBadCiphertextHex(t *testing.T) {
	_, err := decryptChannelMessage("ZZZZ", "0011", "00112233445566778899aabbccddeeff")
	if err == nil {
		t.Error("invalid ciphertext hex should error")
	}
}

func TestDecodeAdvertSensorBadTelemetry(t *testing.T) {
	buf := make([]byte, 112)
	// Bytes 0-31: public key
	for i := 0; i < 32; i++ {
		buf[i] = byte(i + 1)
	}
	// Bytes 32-35: reserved (4 bytes)
	// Bytes 36-99: padding (64 bytes of 0xCC)
	for i := 36; i < 100; i++ {
		buf[i] = 0xCC
	}
	// Byte 100: flags — sensor(4) | hasName(0x80) = 0x84
	//   off starts at 101 after flags byte
	buf[100] = 0x84

	// Bytes 101-102: name "S" + null terminator
	//   Name parsing reads from off=101, finds null at 102, advances off to 103
	copy(buf[101:], []byte("S\x00"))

	// Bytes 103-104: battery_mv (uint16 LE) = 0 (out of range, should be skipped)
	//   off=103 after name parsing
	buf[103] = 0x00
	buf[104] = 0x00

	// Bytes 105-106: temperature (uint16 LE) = 0x4E20 = 20000 raw (200.00°C, out of range)
	//   off=105 after battery
	buf[105] = 0x20
	buf[106] = 0x4E

	p := decodeAdvert(buf[:107], false)
	if p.BatteryMv != nil {
		t.Error("battery_mv=0 should be nil")
	}
	if p.TemperatureC != nil {
		t.Error("out-of-range temp should be nil")
	}
}
func TestDecodeAdvertFeat2Truncated(t *testing.T) {
	buf := make([]byte, 104)
	for i := 0; i < 32; i++ {
		buf[i] = byte(i + 1)
	}
	for i := 36; i < 100; i++ {
		buf[i] = 0xCC
	}
	// flags: hasFeat1(0x20) | hasFeat2(0x40) | type=1 = 0x61
	buf[100] = 0x61
	// feat1: 2 bytes
	buf[101] = 0x01
	buf[102] = 0x00
	// Only 1 byte left — not enough for feat2

	p := decodeAdvert(buf[:104], false)
	if p.Feat1 == nil {
		t.Error("feat1 should be set")
	}
	if p.Feat2 != nil {
		t.Error("feat2 should be nil with truncated data")
	}
}

func TestDecodeAdvertFeat1Truncated(t *testing.T) {
	buf := make([]byte, 102)
	for i := 0; i < 32; i++ {
		buf[i] = byte(i + 1)
	}
	for i := 36; i < 100; i++ {
		buf[i] = 0xCC
	}
	// flags: hasFeat1(0x20) | type=1 = 0x21
	buf[100] = 0x21
	// Only 1 byte after flags — not enough for feat1 (needs 2)

	p := decodeAdvert(buf[:102], false)
	if p.Feat1 != nil {
		t.Error("feat1 should be nil with truncated data")
	}
}

func TestDecodeAdvertLocationTruncated(t *testing.T) {
	buf := make([]byte, 105)
	for i := 0; i < 32; i++ {
		buf[i] = byte(i + 1)
	}
	for i := 36; i < 100; i++ {
		buf[i] = 0xCC
	}
	// flags: hasLocation(0x10) | type=1(chat) = 0x11
	buf[100] = 0x11
	// Only 4 bytes after flags — not enough for full location (needs 8)

	p := decodeAdvert(buf[:105], false)
	if p.Error != "" {
		t.Fatalf("error: %s", p.Error)
	}
	// Location should not be set (not enough data)
	if p.Lat != nil {
		t.Error("lat should be nil with truncated location data")
	}
}

func TestDecodeHeaderPayloadTypes(t *testing.T) {
	// 0x11 = 0b00_0100_01 → routeType=1(FLOOD), payloadType=4(ADVERT), version=0
	h := decodeHeader(0x11)
	if h.RouteType != 1 {
		t.Errorf("0x11: routeType=%d, want 1", h.RouteType)
	}
	if h.PayloadType != 4 {
		t.Errorf("0x11: payloadType=%d, want 4", h.PayloadType)
	}
	if h.PayloadVersion != 0 {
		t.Errorf("0x11: payloadVersion=%d, want 0", h.PayloadVersion)
	}
	if h.RouteTypeName != "FLOOD" {
		t.Errorf("0x11: routeTypeName=%s, want FLOOD", h.RouteTypeName)
	}
	if h.PayloadTypeName != "ADVERT" {
		t.Errorf("0x11: payloadTypeName=%s, want ADVERT", h.PayloadTypeName)
	}
}

func TestDecodePathZeroHops(t *testing.T) {
	// 0x00: 0 hops, 1-byte hashes
	pkt, err := DecodePacket("0500"+strings.Repeat("00", 10), nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if pkt.Path.HashCount != 0 {
		t.Errorf("hashCount=%d, want 0", pkt.Path.HashCount)
	}
	if pkt.Path.HashSize != 1 {
		t.Errorf("hashSize=%d, want 1", pkt.Path.HashSize)
	}
	if len(pkt.Path.Hops) != 0 {
		t.Errorf("hops=%d, want 0", len(pkt.Path.Hops))
	}
}

func TestDecodePath1ByteHashes(t *testing.T) {
	// 0x05: 5 hops, 1-byte hashes → 5 path bytes
	pkt, err := DecodePacket("0505"+"AABBCCDDEE"+strings.Repeat("00", 10), nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if pkt.Path.HashCount != 5 {
		t.Errorf("hashCount=%d, want 5", pkt.Path.HashCount)
	}
	if pkt.Path.HashSize != 1 {
		t.Errorf("hashSize=%d, want 1", pkt.Path.HashSize)
	}
	if len(pkt.Path.Hops) != 5 {
		t.Fatalf("hops=%d, want 5", len(pkt.Path.Hops))
	}
	if pkt.Path.Hops[0] != "AA" {
		t.Errorf("hop[0]=%s, want AA", pkt.Path.Hops[0])
	}
	if pkt.Path.Hops[4] != "EE" {
		t.Errorf("hop[4]=%s, want EE", pkt.Path.Hops[4])
	}
}

func TestDecodePath2ByteHashes(t *testing.T) {
	// 0x45: 5 hops, 2-byte hashes
	pkt, err := DecodePacket("0545"+"AA11BB22CC33DD44EE55"+strings.Repeat("00", 10), nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if pkt.Path.HashCount != 5 {
		t.Errorf("hashCount=%d, want 5", pkt.Path.HashCount)
	}
	if pkt.Path.HashSize != 2 {
		t.Errorf("hashSize=%d, want 2", pkt.Path.HashSize)
	}
	if pkt.Path.Hops[0] != "AA11" {
		t.Errorf("hop[0]=%s, want AA11", pkt.Path.Hops[0])
	}
}

func TestDecodePath3ByteHashes(t *testing.T) {
	// 0x8A: 10 hops, 3-byte hashes
	pkt, err := DecodePacket("058A"+strings.Repeat("AA11FF", 10)+strings.Repeat("00", 10), nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if pkt.Path.HashCount != 10 {
		t.Errorf("hashCount=%d, want 10", pkt.Path.HashCount)
	}
	if pkt.Path.HashSize != 3 {
		t.Errorf("hashSize=%d, want 3", pkt.Path.HashSize)
	}
	if len(pkt.Path.Hops) != 10 {
		t.Errorf("hops=%d, want 10", len(pkt.Path.Hops))
	}
}

func TestTransportCodes(t *testing.T) {
	// Route type 0 (TRANSPORT_FLOOD) should have transport codes
	// Firmware order: header + transport_codes(4) + path_len + path + payload
	hex := "14" + "AABB" + "CCDD" + "00" + strings.Repeat("00", 10)
	pkt, err := DecodePacket(hex, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if pkt.Header.RouteType != 0 {
		t.Errorf("routeType=%d, want 0", pkt.Header.RouteType)
	}
	if pkt.TransportCodes == nil {
		t.Fatal("transportCodes should not be nil for TRANSPORT_FLOOD")
	}
	if pkt.TransportCodes.Code1 != "AABB" {
		t.Errorf("code1=%s, want AABB", pkt.TransportCodes.Code1)
	}
	if pkt.TransportCodes.Code2 != "CCDD" {
		t.Errorf("code2=%s, want CCDD", pkt.TransportCodes.Code2)
	}

	// Route type 1 (FLOOD) should NOT have transport codes
	pkt2, err := DecodePacket("0500"+strings.Repeat("00", 10), nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if pkt2.TransportCodes != nil {
		t.Error("FLOOD should not have transport codes")
	}
}

func TestDecodeAdvertFull(t *testing.T) {
	pubkey := strings.Repeat("AA", 32)
	timestamp := "78563412" // 0x12345678 LE
	signature := strings.Repeat("BB", 64)
	// flags: 0x92 = repeater(2) | hasLocation(0x10) | hasName(0x80)
	flags := "92"
	lat := "40933402"          // ~37.0
	lon := "E0E6B8F8"          // ~-122.1
	name := "546573744E6F6465" // "TestNode"

	hex := "1200" + pubkey + timestamp + signature + flags + lat + lon + name
	pkt, err := DecodePacket(hex, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	if pkt.Payload.Type != "ADVERT" {
		t.Errorf("type=%s, want ADVERT", pkt.Payload.Type)
	}
	if pkt.Payload.PubKey != strings.ToLower(pubkey) {
		t.Errorf("pubkey mismatch")
	}
	if pkt.Payload.Timestamp != 0x12345678 {
		t.Errorf("timestamp=%d, want %d", pkt.Payload.Timestamp, 0x12345678)
	}

	if pkt.Payload.Flags == nil {
		t.Fatal("flags should not be nil")
	}
	if pkt.Payload.Flags.Raw != 0x92 {
		t.Errorf("flags.raw=%d, want 0x92", pkt.Payload.Flags.Raw)
	}
	if pkt.Payload.Flags.Type != 2 {
		t.Errorf("flags.type=%d, want 2", pkt.Payload.Flags.Type)
	}
	if !pkt.Payload.Flags.Repeater {
		t.Error("flags.repeater should be true")
	}
	if pkt.Payload.Flags.Room {
		t.Error("flags.room should be false")
	}
	if !pkt.Payload.Flags.HasLocation {
		t.Error("flags.hasLocation should be true")
	}
	if !pkt.Payload.Flags.HasName {
		t.Error("flags.hasName should be true")
	}

	if pkt.Payload.Lat == nil {
		t.Fatal("lat should not be nil")
	}
	if math.Abs(*pkt.Payload.Lat-37.0) > 0.001 {
		t.Errorf("lat=%f, want ~37.0", *pkt.Payload.Lat)
	}
	if pkt.Payload.Lon == nil {
		t.Fatal("lon should not be nil")
	}
	if math.Abs(*pkt.Payload.Lon-(-122.1)) > 0.001 {
		t.Errorf("lon=%f, want ~-122.1", *pkt.Payload.Lon)
	}
	if pkt.Payload.Name != "TestNode" {
		t.Errorf("name=%s, want TestNode", pkt.Payload.Name)
	}
}

func TestDecodeAdvertTypeEnums(t *testing.T) {
	makeAdvert := func(flagsByte byte) *DecodedPacket {
		hex := "1200" + strings.Repeat("AA", 32) + "00000000" + strings.Repeat("BB", 64) +
			strings.ToUpper(string([]byte{hexDigit(flagsByte >> 4), hexDigit(flagsByte & 0x0f)}))
		pkt, err := DecodePacket(hex, nil, false)
		if err != nil {
			t.Fatal(err)
		}
		return pkt
	}

	// type 1 = chat/companion
	p1 := makeAdvert(0x01)
	if p1.Payload.Flags.Type != 1 {
		t.Errorf("type 1: flags.type=%d", p1.Payload.Flags.Type)
	}
	if !p1.Payload.Flags.Chat {
		t.Error("type 1: chat should be true")
	}

	// type 2 = repeater
	p2 := makeAdvert(0x02)
	if !p2.Payload.Flags.Repeater {
		t.Error("type 2: repeater should be true")
	}

	// type 3 = room
	p3 := makeAdvert(0x03)
	if !p3.Payload.Flags.Room {
		t.Error("type 3: room should be true")
	}

	// type 4 = sensor
	p4 := makeAdvert(0x04)
	if !p4.Payload.Flags.Sensor {
		t.Error("type 4: sensor should be true")
	}
}

func hexDigit(v byte) byte {
	v = v & 0x0f
	if v < 10 {
		return '0' + v
	}
	return 'a' + v - 10
}

func TestDecodeAdvertNoLocationNoName(t *testing.T) {
	hex := "1200" + strings.Repeat("CC", 32) + "00000000" + strings.Repeat("DD", 64) + "02"
	pkt, err := DecodePacket(hex, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if pkt.Payload.Flags.HasLocation {
		t.Error("hasLocation should be false")
	}
	if pkt.Payload.Flags.HasName {
		t.Error("hasName should be false")
	}
	if pkt.Payload.Lat != nil {
		t.Error("lat should be nil")
	}
	if pkt.Payload.Name != "" {
		t.Errorf("name should be empty, got %s", pkt.Payload.Name)
	}
}

func TestGoldenFixtureTxtMsg(t *testing.T) {
	pkt, err := DecodePacket("0A00D69FD7A5A7475DB07337749AE61FA53A4788E976", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if pkt.Header.PayloadType != types.PayloadTXT_MSG {
		t.Errorf("payloadType=%d, want %d", pkt.Header.PayloadType, types.PayloadTXT_MSG)
	}
	if pkt.Header.RouteType != types.RouteDirect {
		t.Errorf("routeType=%d, want %d", pkt.Header.RouteType, types.RouteDirect)
	}
	if pkt.Path.HashCount != 0 {
		t.Errorf("hashCount=%d, want 0", pkt.Path.HashCount)
	}
	if pkt.Payload.DestHash != "d6" {
		t.Errorf("destHash=%s, want d6", pkt.Payload.DestHash)
	}
	if pkt.Payload.SrcHash != "9f" {
		t.Errorf("srcHash=%s, want 9f", pkt.Payload.SrcHash)
	}
}

func TestGoldenFixtureAdvert(t *testing.T) {
	rawHex := "120046D62DE27D4C5194D7821FC5A34A45565DCC2537B300B9AB6275255CEFB65D840CE5C169C94C9AED39E8BCB6CB6EB0335497A198B33A1A610CD3B03D8DCFC160900E5244280323EE0B44CACAB8F02B5B38B91CFA18BD067B0B5E63E94CFC85F758A8530B9240933402E0E6B8F84D5252322D52"
	pkt, err := DecodePacket(rawHex, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if pkt.Payload.Type != "ADVERT" {
		t.Errorf("type=%s, want ADVERT", pkt.Payload.Type)
	}
	if pkt.Payload.PubKey != "46d62de27d4c5194d7821fc5a34a45565dcc2537b300b9ab6275255cefb65d84" {
		t.Errorf("pubKey mismatch: %s", pkt.Payload.PubKey)
	}
	if pkt.Payload.Flags == nil || !pkt.Payload.Flags.Repeater {
		t.Error("should be repeater")
	}
	if math.Abs(*pkt.Payload.Lat-37.0) > 0.001 {
		t.Errorf("lat=%f, want ~37.0", *pkt.Payload.Lat)
	}
	if pkt.Payload.Name != "MRR2-R" {
		t.Errorf("name=%s, want MRR2-R", pkt.Payload.Name)
	}
}

func TestGoldenFixtureUnicodeAdvert(t *testing.T) {
	rawHex := "120073CFF971E1CB5754A742C152B2D2E0EB108A19B246D663ED8898A72C4A5AD86EA6768E66694B025EDF6939D5C44CFF719C5D5520E5F06B20680A83AD9C2C61C3227BBB977A85EE462F3553445FECF8EDD05C234ECE217272E503F14D6DF2B1B9B133890C923CDF3002F8FDC1F85045414BF09F8CB3"
	pkt, err := DecodePacket(rawHex, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if pkt.Payload.Type != "ADVERT" {
		t.Errorf("type=%s, want ADVERT", pkt.Payload.Type)
	}
	if !pkt.Payload.Flags.Repeater {
		t.Error("should be repeater")
	}
	// Name contains emoji: PEAK🌳
	if !strings.HasPrefix(pkt.Payload.Name, "PEAK") {
		t.Errorf("name=%s, expected to start with PEAK", pkt.Payload.Name)
	}
}

func TestDecodePacketTooShort(t *testing.T) {
	_, err := DecodePacket("FF", nil, false)
	if err == nil {
		t.Error("expected error for 1-byte packet")
	}
}

func TestDecodePacketInvalidHex(t *testing.T) {
	_, err := DecodePacket("ZZZZ", nil, false)
	if err == nil {
		t.Error("expected error for invalid hex")
	}
}

func TestComputeContentHash(t *testing.T) {
	hash := ComputeContentHash("0A00D69FD7A5A7475DB07337749AE61FA53A4788E976")
	if len(hash) != 16 {
		t.Errorf("hash length=%d, want 16", len(hash))
	}
	// Same content with different path should produce same hash
	// (path bytes are stripped, only header + payload hashed)

	// Verify consistency
	hash2 := ComputeContentHash("0A00D69FD7A5A7475DB07337749AE61FA53A4788E976")
	if hash != hash2 {
		t.Error("content hash not deterministic")
	}
}

func TestValidateAdvert(t *testing.T) {
	goodPk := strings.Repeat("aa", 32)

	// Good advert
	good := &Payload{PubKey: goodPk, Flags: &AdvertFlags{Repeater: true}}
	ok, _ := ValidateAdvert(good)
	if !ok {
		t.Error("good advert should validate")
	}

	// Nil
	ok, _ = ValidateAdvert(nil)
	if ok {
		t.Error("nil should fail")
	}

	// Error payload
	ok, _ = ValidateAdvert(&Payload{Error: "bad"})
	if ok {
		t.Error("error payload should fail")
	}

	// Short pubkey
	ok, _ = ValidateAdvert(&Payload{PubKey: "aa"})
	if ok {
		t.Error("short pubkey should fail")
	}

	// All-zero pubkey
	ok, _ = ValidateAdvert(&Payload{PubKey: strings.Repeat("0", 64)})
	if ok {
		t.Error("all-zero pubkey should fail")
	}

	// Invalid lat
	badLat := 999.0
	ok, _ = ValidateAdvert(&Payload{PubKey: goodPk, Lat: &badLat})
	if ok {
		t.Error("invalid lat should fail")
	}

	// Invalid lon
	badLon := -999.0
	ok, _ = ValidateAdvert(&Payload{PubKey: goodPk, Lon: &badLon})
	if ok {
		t.Error("invalid lon should fail")
	}

	// Control chars in name
	ok, _ = ValidateAdvert(&Payload{PubKey: goodPk, Name: "test\x00name"})
	if ok {
		t.Error("control chars in name should fail")
	}

	// Name too long
	ok, _ = ValidateAdvert(&Payload{PubKey: goodPk, Name: strings.Repeat("x", 65)})
	if ok {
		t.Error("long name should fail")
	}
}

func TestDecodePacketPayloadRaw(t *testing.T) {
	// Build a minimal TRANSPORT_FLOOD packet (route_type=0):
	// header(1) + transport_codes(4) + path_len(1) + payload(N)
	// Header 0x00 = route_type=TRANSPORT_FLOOD, payload_type=0, version=0
	// Code1=9A52, Code2=0000, path_len=0x00 (0 hops, hash_size=1)
	payload := []byte("hello")
	raw := []byte{0x00, 0x9A, 0x52, 0x00, 0x00, 0x00}
	raw = append(raw, payload...)
	hexStr := strings.ToUpper(hex.EncodeToString(raw))

	decoded, err := DecodePacket(hexStr, nil, false)
	if err != nil {
		t.Fatalf("DecodePacket: %v", err)
	}
	if decoded.TransportCodes == nil {
		t.Fatal("expected TransportCodes, got nil")
	}
	if string(decoded.PayloadRaw) != string(payload) {
		t.Errorf("payloadRaw = %v, want %v", decoded.PayloadRaw, payload)
	}
}

func TestDecodeGrpTxtShort(t *testing.T) {
	p := decodeGrpTxt([]byte{0x01, 0x02}, nil)
	if p.Error != "too short" {
		t.Errorf("expected 'too short' error, got %q", p.Error)
	}
	if p.Type != "GRP_TXT" {
		t.Errorf("type=%s, want GRP_TXT", p.Type)
	}
}

func TestDecodeGrpTxtValid(t *testing.T) {
	p := decodeGrpTxt([]byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE}, nil)
	if p.Error != "" {
		t.Errorf("unexpected error: %s", p.Error)
	}
	if p.ChannelHash != 0xAA {
		t.Errorf("channelHash=%d, want 0xAA", p.ChannelHash)
	}
	if p.MAC != "bbcc" {
		t.Errorf("mac=%s, want bbcc", p.MAC)
	}
	if p.EncryptedData != "ddee" {
		t.Errorf("encryptedData=%s, want ddee", p.EncryptedData)
	}
}

func TestDecodeAnonReqShort(t *testing.T) {
	p := decodeAnonReq(make([]byte, 10))
	if p.Error != "too short" {
		t.Errorf("expected 'too short' error, got %q", p.Error)
	}
	if p.Type != "ANON_REQ" {
		t.Errorf("type=%s, want ANON_REQ", p.Type)
	}
}

func TestDecodeAnonReqValid(t *testing.T) {
	buf := make([]byte, 40)
	buf[0] = 0xFF // destHash
	for i := 1; i < 33; i++ {
		buf[i] = byte(i)
	}
	buf[33] = 0xAA
	buf[34] = 0xBB
	p := decodeAnonReq(buf)
	if p.Error != "" {
		t.Errorf("unexpected error: %s", p.Error)
	}
	if p.DestHash != "ff" {
		t.Errorf("destHash=%s, want ff", p.DestHash)
	}
	if p.MAC != "aabb" {
		t.Errorf("mac=%s, want aabb", p.MAC)
	}
	// #1864: ANON_REQ must capture the FULL 32-byte source pubkey (buf[1:33]),
	// not a 1-byte srcHash. buf[i]=i for i in 1..32 -> 0102..20.
	wantPK := "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
	if p.SrcPubKey != wantPK {
		t.Errorf("srcPubKey=%s, want %s", p.SrcPubKey, wantPK)
	}
}

func TestDecodePathPayloadShort(t *testing.T) {
	p := decodePathPayload([]byte{0x01, 0x02, 0x03})
	if p.Error != "too short" {
		t.Errorf("expected 'too short' error, got %q", p.Error)
	}
	if p.Type != "PATH" {
		t.Errorf("type=%s, want PATH", p.Type)
	}
}

func TestDecodePathPayloadValid(t *testing.T) {
	buf := []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF}
	p := decodePathPayload(buf)
	if p.Error != "" {
		t.Errorf("unexpected error: %s", p.Error)
	}
	if p.DestHash != "aa" {
		t.Errorf("destHash=%s, want aa", p.DestHash)
	}
	if p.SrcHash != "bb" {
		t.Errorf("srcHash=%s, want bb", p.SrcHash)
	}
	if p.PathData != "eeff" {
		t.Errorf("pathData=%s, want eeff", p.PathData)
	}
}

func TestDecodeTraceShort(t *testing.T) {
	p := decodeTrace(make([]byte, 5))
	if p.Error != "too short" {
		t.Errorf("expected 'too short' error, got %q", p.Error)
	}
	if p.Type != "TRACE" {
		t.Errorf("type=%s, want TRACE", p.Type)
	}
}

func TestDecodeTraceValid(t *testing.T) {
	buf := make([]byte, 16)
	// tag(4) + authCode(4) + flags(1) + pathData
	binary.LittleEndian.PutUint32(buf[0:4], 1)          // tag = 1
	binary.LittleEndian.PutUint32(buf[4:8], 0xDEADBEEF) // authCode
	buf[8] = 0x02                                       // flags
	buf[9] = 0xAA                                       // path data
	p := decodeTrace(buf)
	if p.Error != "" {
		t.Errorf("unexpected error: %s", p.Error)
	}
	if p.Tag != 1 {
		t.Errorf("tag=%d, want 1", p.Tag)
	}
	if p.AuthCode != 0xDEADBEEF {
		t.Errorf("authCode=%d, want 0xDEADBEEF", p.AuthCode)
	}
	if p.TraceFlags == nil || *p.TraceFlags != 2 {
		t.Errorf("traceFlags=%v, want 2", p.TraceFlags)
	}
	if p.Type != "TRACE" {
		t.Errorf("type=%s, want TRACE", p.Type)
	}
	if p.PathData == "" {
		t.Error("pathData should not be empty")
	}
}

func TestDecodeTracePathParsing(t *testing.T) {
	// Packet from issue #276: 260001807dca00000000007d547d
	// Path byte 0x00 → hashSize=1, hops in payload at buf[9:] = 7d 54 7d
	// Expected path: ["7D", "54", "7D"]
	pkt, err := DecodePacket("260001807dca00000000007d547d", nil, false)
	if err != nil {
		t.Fatalf("DecodePacket error: %v", err)
	}
	if pkt.Payload.Type != "TRACE" {
		t.Errorf("payload type=%s, want TRACE", pkt.Payload.Type)
	}
	want := []string{"7D", "54", "7D"}
	if len(pkt.Path.Hops) != len(want) {
		t.Fatalf("hops=%v, want %v", pkt.Path.Hops, want)
	}
	for i, h := range want {
		if pkt.Path.Hops[i] != h {
			t.Errorf("hops[%d]=%s, want %s", i, pkt.Path.Hops[i], h)
		}
	}
	if pkt.Path.HashCount != 3 {
		t.Errorf("hashCount=%d, want 3", pkt.Path.HashCount)
	}
}

func TestDecodeAdvertShort(t *testing.T) {
	p := decodeAdvert(make([]byte, 50), false)
	if p.Error != "too short for advert" {
		t.Errorf("expected 'too short for advert' error, got %q", p.Error)
	}
}

func TestDecodeEncryptedPayloadShort(t *testing.T) {
	p := decodeEncryptedPayload("REQ", []byte{0x01, 0x02})
	if p.Error != "too short" {
		t.Errorf("expected 'too short' error, got %q", p.Error)
	}
	if p.Type != "REQ" {
		t.Errorf("type=%s, want REQ", p.Type)
	}
}

func TestDecodeEncryptedPayloadValid(t *testing.T) {
	buf := []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF}
	p := decodeEncryptedPayload("RESPONSE", buf)
	if p.Error != "" {
		t.Errorf("unexpected error: %s", p.Error)
	}
	if p.DestHash != "aa" {
		t.Errorf("destHash=%s, want aa", p.DestHash)
	}
	if p.SrcHash != "bb" {
		t.Errorf("srcHash=%s, want bb", p.SrcHash)
	}
	if p.MAC != "ccdd" {
		t.Errorf("mac=%s, want ccdd", p.MAC)
	}
	if p.EncryptedData != "eeff" {
		t.Errorf("encryptedData=%s, want eeff", p.EncryptedData)
	}
}

func TestDecodePayloadGRPData(t *testing.T) {
	// GRP_DATA (0x06) decoder added for #1279 P0 #1 — envelope only when no
	// channel key matches (firmware/src/helpers/BaseChatMesh.cpp:500).
	buf := []byte{0x01, 0x02, 0x03}
	p := decodePayload(types.PayloadGRP_DATA, buf, nil, false)
	if p.Type != "GRP_DATA" {
		t.Errorf("type=%s, want GRP_DATA", p.Type)
	}
}

func TestDecodePayloadRAWCustom(t *testing.T) {
	// #1279 P2 #5: RAW_CUSTOM (0x0F) now exposes envelope shape (length +
	// first-byte tag) per firmware/src/Mesh.cpp:577 (createRawData).
	buf := []byte{0xFF, 0xFE}
	p := decodePayload(types.PayloadRAW_CUSTOM, buf, nil, false)
	if p.Type != "RAW_CUSTOM" {
		t.Errorf("type=%s, want RAW_CUSTOM", p.Type)
	}
	if p.RawLength == nil || *p.RawLength != 2 {
		t.Errorf("rawLength missing or wrong, want 2")
	}
	if p.FirstByteTag != "FF" {
		t.Errorf("firstByteTag=%q, want FF", p.FirstByteTag)
	}
}

// buildMultipartAckByte0: remaining<<4 | PayloadACK (0x02).
func buildMultipartAckByte0(remaining int) byte {
	return byte((remaining<<4)&0xF0) | byte(types.PayloadACK&0x0F)
}

func TestDecodeMultipartAck5Byte(t *testing.T) {
	// v1.16: byte0 + 4-byte CRC + 1-byte attempt → payload_len = 6.
	buf := []byte{buildMultipartAckByte0(1), 0xAA, 0xBB, 0xCC, 0xDD, 0x09}
	p := decodeMultipart(buf)
	if p.InnerAckCrc != "ddccbbaa" {
		t.Errorf("innerAckCrc=%q want ddccbbaa", p.InnerAckCrc)
	}
	if p.InnerAckLen == nil || *p.InnerAckLen != 5 {
		t.Errorf("innerAckLen=%v want 5", p.InnerAckLen)
	}
	if p.InnerAckAttempt == nil || *p.InnerAckAttempt != 9 {
		t.Errorf("innerAckAttempt=%v want 9", p.InnerAckAttempt)
	}
	if p.InnerAckRand != nil {
		t.Errorf("innerAckRand=%v want nil for 5-byte inner ACK", *p.InnerAckRand)
	}
}

func TestDecodeMultipartAck6Byte(t *testing.T) {
	// v1.16: byte0 + 4-byte CRC + 1-byte attempt + 1-byte RNG → payload_len = 7.
	buf := []byte{buildMultipartAckByte0(0), 0xAA, 0xBB, 0xCC, 0xDD, 0x04, 0xC3}
	p := decodeMultipart(buf)
	if p.InnerAckCrc != "ddccbbaa" {
		t.Errorf("innerAckCrc=%q want ddccbbaa", p.InnerAckCrc)
	}
	if p.InnerAckLen == nil || *p.InnerAckLen != 6 {
		t.Errorf("innerAckLen=%v want 6", p.InnerAckLen)
	}
	if p.InnerAckAttempt == nil || *p.InnerAckAttempt != 4 {
		t.Errorf("innerAckAttempt=%v want 4", p.InnerAckAttempt)
	}
	if p.InnerAckRand == nil || *p.InnerAckRand != 0xC3 {
		t.Errorf("innerAckRand=%v want 195", p.InnerAckRand)
	}
}

func TestDecodeMultipartAck4ByteLegacy(t *testing.T) {
	// Pre-1.16 inner ACK is 4 bytes → ackLen=4, attempt/rand nil.
	buf := []byte{buildMultipartAckByte0(3), 0xAA, 0xBB, 0xCC, 0xDD}
	p := decodeMultipart(buf)
	if p.InnerAckCrc != "ddccbbaa" {
		t.Errorf("innerAckCrc=%q want ddccbbaa", p.InnerAckCrc)
	}
	if p.InnerAckLen == nil || *p.InnerAckLen != 4 {
		t.Errorf("innerAckLen=%v want 4", p.InnerAckLen)
	}
	if p.InnerAckAttempt != nil {
		t.Errorf("innerAckAttempt=%v want nil", *p.InnerAckAttempt)
	}
	if p.InnerAckRand != nil {
		t.Errorf("innerAckRand=%v want nil", *p.InnerAckRand)
	}
}
func TestDecodeAck6ByteExtended(t *testing.T) {
	// v1.16 sender (commit a130a95a): 4-byte CRC + 1-byte attempt + 1-byte RNG.
	buf := []byte{0xAA, 0xBB, 0xCC, 0xDD, 0x02, 0x5A}
	p := decodeAck(buf)
	if p.ExtraHash != "ddccbbaa" {
		t.Errorf("extraHash=%q want ddccbbaa", p.ExtraHash)
	}
	if p.AckLen == nil || *p.AckLen != 6 {
		t.Errorf("ackLen=%v want 6", p.AckLen)
	}
	if p.AckAttempt == nil || *p.AckAttempt != 2 {
		t.Errorf("ackAttempt=%v want 2", p.AckAttempt)
	}
	if p.AckRand == nil || *p.AckRand != 0x5A {
		t.Errorf("ackRand=%v want 90", p.AckRand)
	}
}

func TestDecodeAck5ByteExtended(t *testing.T) {
	// v1.16 sender (commit f6e6fdaa): 4-byte CRC + 1-byte attempt.
	buf := []byte{0xAA, 0xBB, 0xCC, 0xDD, 0x07}
	p := decodeAck(buf)
	if p.ExtraHash != "ddccbbaa" {
		t.Errorf("extraHash=%q want ddccbbaa", p.ExtraHash)
	}
	if p.AckLen == nil || *p.AckLen != 5 {
		t.Errorf("ackLen=%v want 5", p.AckLen)
	}
	if p.AckAttempt == nil || *p.AckAttempt != 7 {
		t.Errorf("ackAttempt=%v want 7", p.AckAttempt)
	}
	if p.AckRand != nil {
		t.Errorf("ackRand=%v want nil for 5-byte ACK", *p.AckRand)
	}
}

func TestDecodeAckLegacy4Byte(t *testing.T) {
	// Backwards-compat: 4-byte ACK leaves the new optional fields nil.
	buf := []byte{0xAA, 0xBB, 0xCC, 0xDD}
	p := decodeAck(buf)
	if p.ExtraHash != "ddccbbaa" {
		t.Errorf("extraHash=%q want ddccbbaa", p.ExtraHash)
	}
	if p.AckLen == nil || *p.AckLen != 4 {
		t.Errorf("ackLen=%v want 4", p.AckLen)
	}
	if p.AckAttempt != nil {
		t.Errorf("ackAttempt=%v want nil for legacy 4-byte ACK", *p.AckAttempt)
	}
	if p.AckRand != nil {
		t.Errorf("ackRand=%v want nil for legacy 4-byte ACK", *p.AckRand)
	}
}

func TestDecodePayloadAllTypes(t *testing.T) {
	// REQ
	p := decodePayload(types.PayloadREQ, make([]byte, 10), nil, false)
	if p.Type != "REQ" {
		t.Errorf("REQ: type=%s", p.Type)
	}

	// RESPONSE
	p = decodePayload(types.PayloadRESPONSE, make([]byte, 10), nil, false)
	if p.Type != "RESPONSE" {
		t.Errorf("RESPONSE: type=%s", p.Type)
	}

	// TXT_MSG
	p = decodePayload(types.PayloadTXT_MSG, make([]byte, 10), nil, false)
	if p.Type != "TXT_MSG" {
		t.Errorf("TXT_MSG: type=%s", p.Type)
	}

	// ACK
	p = decodePayload(types.PayloadACK, make([]byte, 10), nil, false)
	if p.Type != "ACK" {
		t.Errorf("ACK: type=%s", p.Type)
	}

	// GRP_TXT
	p = decodePayload(types.PayloadGRP_TXT, make([]byte, 10), nil, false)
	if p.Type != "GRP_TXT" {
		t.Errorf("GRP_TXT: type=%s", p.Type)
	}

	// ANON_REQ
	p = decodePayload(types.PayloadANON_REQ, make([]byte, 40), nil, false)
	if p.Type != "ANON_REQ" {
		t.Errorf("ANON_REQ: type=%s", p.Type)
	}

	// PATH
	p = decodePayload(types.PayloadPATH, make([]byte, 10), nil, false)
	if p.Type != "PATH" {
		t.Errorf("PATH: type=%s", p.Type)
	}

	// TRACE
	p = decodePayload(types.PayloadTRACE, make([]byte, 20), nil, false)
	if p.Type != "TRACE" {
		t.Errorf("TRACE: type=%s", p.Type)
	}
}

func TestPayloadJSON(t *testing.T) {
	p := &Payload{Type: "TEST", Name: "hello"}
	j := PayloadJSON(p)
	if j == "" || j == "{}" {
		t.Errorf("PayloadJSON returned empty: %s", j)
	}
	if !strings.Contains(j, `"type":"TEST"`) {
		t.Errorf("PayloadJSON missing type: %s", j)
	}
	if !strings.Contains(j, `"name":"hello"`) {
		t.Errorf("PayloadJSON missing name: %s", j)
	}
}

func TestPayloadJSONNil(t *testing.T) {
	// nil should not panic
	j := PayloadJSON(nil)
	if j != "null" && j != "{}" {
		// json.Marshal(nil) returns "null"
		t.Logf("PayloadJSON(nil) = %s", j)
	}
}

func TestValidateAdvertNaNLat(t *testing.T) {
	goodPk := strings.Repeat("aa", 32)
	nanVal := math.NaN()
	ok, reason := ValidateAdvert(&Payload{PubKey: goodPk, Lat: &nanVal})
	if ok {
		t.Error("NaN lat should fail")
	}
	if !strings.Contains(reason, "lat") {
		t.Errorf("reason should mention lat: %s", reason)
	}
}

func TestValidateAdvertInfLon(t *testing.T) {
	goodPk := strings.Repeat("aa", 32)
	infVal := math.Inf(1)
	ok, reason := ValidateAdvert(&Payload{PubKey: goodPk, Lon: &infVal})
	if ok {
		t.Error("Inf lon should fail")
	}
	if !strings.Contains(reason, "lon") {
		t.Errorf("reason should mention lon: %s", reason)
	}
}

func TestValidateAdvertNegInfLat(t *testing.T) {
	goodPk := strings.Repeat("aa", 32)
	negInf := math.Inf(-1)
	ok, _ := ValidateAdvert(&Payload{PubKey: goodPk, Lat: &negInf})
	if ok {
		t.Error("-Inf lat should fail")
	}
}

func TestValidateAdvertNaNLon(t *testing.T) {
	goodPk := strings.Repeat("aa", 32)
	nan := math.NaN()
	ok, _ := ValidateAdvert(&Payload{PubKey: goodPk, Lon: &nan})
	if ok {
		t.Error("NaN lon should fail")
	}
}

func TestValidateAdvertControlChars(t *testing.T) {
	goodPk := strings.Repeat("aa", 32)
	tests := []struct {
		name string
		char string
	}{
		{"null", "\x00"},
		{"bell", "\x07"},
		{"backspace", "\x08"},
		{"vtab", "\x0b"},
		{"formfeed", "\x0c"},
		{"shift out", "\x0e"},
		{"unit sep", "\x1f"},
		{"delete", "\x7f"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ok, _ := ValidateAdvert(&Payload{PubKey: goodPk, Name: "test" + tt.char + "name"})
			if ok {
				t.Errorf("control char %q in name should fail", tt.char)
			}
		})
	}
}

func TestValidateAdvertAllowedCharsInName(t *testing.T) {
	goodPk := strings.Repeat("aa", 32)
	// Tab (\t = 0x09), newline (\n = 0x0a), carriage return (\r = 0x0d) are NOT blocked
	ok, reason := ValidateAdvert(&Payload{PubKey: goodPk, Name: "hello\tworld", Flags: &AdvertFlags{Repeater: true}})
	if !ok {
		t.Errorf("tab in name should be allowed, got reason: %s", reason)
	}
}

func TestValidateAdvertUnknownRole(t *testing.T) {
	goodPk := strings.Repeat("aa", 32)
	// type=0 maps to companion via Chat=false, Repeater=false, Room=false, Sensor=false → companion
	// type=5 (unknown) → companion (default), which IS a valid role
	// But if all booleans are false AND type is 0, advertRole returns "companion" which is valid
	// To get "unknown", we'd need a flags combo that doesn't match any valid role
	// Actually advertRole always returns companion as default — so let's just test the validation path
	flags := &AdvertFlags{Type: 5, Chat: false, Repeater: false, Room: false, Sensor: false}
	ok, reason := ValidateAdvert(&Payload{PubKey: goodPk, Flags: flags})
	// advertRole returns "companion" for this, which is valid
	if !ok {
		t.Errorf("default companion role should be valid, got: %s", reason)
	}
}

func TestValidateAdvertValidLocation(t *testing.T) {
	goodPk := strings.Repeat("aa", 32)
	lat := 45.0
	lon := -90.0
	ok, _ := ValidateAdvert(&Payload{PubKey: goodPk, Lat: &lat, Lon: &lon, Flags: &AdvertFlags{Repeater: true}})
	if !ok {
		t.Error("valid lat/lon should pass")
	}
}

func TestValidateAdvertBoundaryLat(t *testing.T) {
	goodPk := strings.Repeat("aa", 32)
	// Exactly at boundary
	lat90 := 90.0
	ok, _ := ValidateAdvert(&Payload{PubKey: goodPk, Lat: &lat90})
	if !ok {
		t.Error("lat=90 should pass")
	}
	latNeg90 := -90.0
	ok, _ = ValidateAdvert(&Payload{PubKey: goodPk, Lat: &latNeg90})
	if !ok {
		t.Error("lat=-90 should pass")
	}
	// Just over
	lat91 := 90.001
	ok, _ = ValidateAdvert(&Payload{PubKey: goodPk, Lat: &lat91})
	if ok {
		t.Error("lat=90.001 should fail")
	}
}

func TestValidateAdvertBoundaryLon(t *testing.T) {
	goodPk := strings.Repeat("aa", 32)
	lon180 := 180.0
	ok, _ := ValidateAdvert(&Payload{PubKey: goodPk, Lon: &lon180})
	if !ok {
		t.Error("lon=180 should pass")
	}
	lonNeg180 := -180.0
	ok, _ = ValidateAdvert(&Payload{PubKey: goodPk, Lon: &lonNeg180})
	if !ok {
		t.Error("lon=-180 should pass")
	}
}

func TestComputeContentHashShortHex(t *testing.T) {
	// Less than 16 hex chars and invalid hex
	hash := ComputeContentHash("AB")
	if hash != "AB" {
		t.Errorf("short hex hash=%s, want AB", hash)
	}

	// Exactly 16 chars invalid hex
	hash = ComputeContentHash("ZZZZZZZZZZZZZZZZ")
	if len(hash) != 16 {
		t.Errorf("invalid hex hash length=%d, want 16", len(hash))
	}
}

func TestComputeContentHashTransportRoute(t *testing.T) {
	// Route type 0 (TRANSPORT_FLOOD) with transport codes then path=0x00 (0 hops)
	// header=0x14 (TRANSPORT_FLOOD, ADVERT), transport(4), path=0x00
	hex := "14" + "AABBCCDD" + "00" + strings.Repeat("EE", 10)
	hash := ComputeContentHash(hex)
	if len(hash) != 16 {
		t.Errorf("hash length=%d, want 16", len(hash))
	}
}

func TestComputeContentHashPayloadBeyondBuffer(t *testing.T) {
	// path claims more bytes than buffer has → fallback
	// header=0x05 (FLOOD, REQ), pathByte=0x3F (63 hops of 1 byte = 63 path bytes)
	// but total buffer is only 4 bytes
	hex := "053F" + "AABB"
	hash := ComputeContentHash(hex)
	// payloadStart = 2 + 63 = 65, but buffer is only 4 bytes
	// Should fallback — rawHex is 8 chars (< 16), so returns rawHex
	if hash != hex {
		t.Errorf("hash=%s, want %s", hash, hex)
	}
}

func TestComputeContentHashPayloadBeyondBufferLongHex(t *testing.T) {
	// Same as above but with rawHex >= 16 chars → returns first 16
	hex := "053F" + strings.Repeat("AA", 20) // 44 chars total, but pathByte claims 63 hops
	hash := ComputeContentHash(hex)
	if len(hash) != 16 {
		t.Errorf("hash length=%d, want 16", len(hash))
	}
	if hash != hex[:16] {
		t.Errorf("hash=%s, want %s", hash, hex[:16])
	}
}

func TestComputeContentHashTransportBeyondBuffer(t *testing.T) {
	// Transport route (0x00 = TRANSPORT_FLOOD) with path claiming some bytes
	// header=0x00, transport(4), pathByte=0x02 (2 hops, 1-byte hash)
	// offset=1+4+1+2=8, buffer needs to be >= 8
	hex := "00" + "AABB" + "CCDD" + "02" + strings.Repeat("CC", 6) // 20 chars = 10 bytes
	hash := ComputeContentHash(hex)
	if len(hash) != 16 {
		t.Errorf("hash length=%d, want 16", len(hash))
	}
}

func TestComputeContentHashLongFallback(t *testing.T) {
	// Long rawHex (>= 16) but invalid → returns first 16 chars
	longInvalid := "ZZZZZZZZZZZZZZZZZZZZZZZZ"
	hash := ComputeContentHash(longInvalid)
	if hash != longInvalid[:16] {
		t.Errorf("hash=%s, want first 16 of input", hash)
	}
}

// TestComputeContentHashRouteTypeIndependence verifies that the same logical
// packet produces the same content hash regardless of route type (issue #786).
func TestComputeContentHashRouteTypeIndependence(t *testing.T) {
	// Same payload type (TXT_MSG=2, bits 2-5) with different route types.
	// Header 0x08 = route_type 0 (TRANSPORT_FLOOD), payload_type 2
	// Header 0x0A = route_type 2 (DIRECT), payload_type 2
	// Header 0x09 = route_type 1 (FLOOD), payload_type 2
	// pathByte=0x00, payload=D69FD7A5A7
	payloadHex := "D69FD7A5A7"

	// FLOOD: header=0x09 (route_type 1), pathByte=0x00
	floodHex := "09" + "00" + payloadHex
	// DIRECT: header=0x0A (route_type 2), pathByte=0x00
	directHex := "0A" + "00" + payloadHex

	hashFlood := ComputeContentHash(floodHex)
	hashDirect := ComputeContentHash(directHex)
	if hashFlood != hashDirect {
		t.Errorf("same payload with different route types produced different hashes: flood=%s direct=%s", hashFlood, hashDirect)
	}
}

// TestComputeContentHashTraceIncludesPathLen verifies TRACE packets include
// path_len in the hash (matching firmware behavior).
func TestComputeContentHashTraceIncludesPathLen(t *testing.T) {
	// TRACE = payload_type 0x09, so header bits 2-5 = 0x09 → header = 0x09<<2 | route=2 = 0x26
	// pathByte=0x01 (1 hop, 1-byte hash) → 1 path byte
	traceHeader1 := "26" // route=2, payload_type=9
	pathByte1 := "01"
	pathData1 := "AA"
	payload := "DEADBEEF"
	hex1 := traceHeader1 + pathByte1 + pathData1 + payload

	// Same but pathByte=0x02 (2 hops) → 2 path bytes
	pathByte2 := "02"
	pathData2 := "AABB"
	hex2 := traceHeader1 + pathByte2 + pathData2 + payload

	hash1 := ComputeContentHash(hex1)
	hash2 := ComputeContentHash(hex2)
	if hash1 == hash2 {
		t.Error("TRACE packets with different path_len should produce different hashes (path_len is part of hash input)")
	}
}

// TestComputeContentHashMatchesFirmware verifies hash output matches what the
// firmware would compute: SHA256(payload_type_byte + payload)[:16hex].
func TestComputeContentHashMatchesFirmware(t *testing.T) {
	// header=0x0A → payload_type = (0x0A >> 2) & 0x0F = 2
	// pathByte=0x00, payload = D69FD7A5A7475DB07337749AE61FA53A4788E976
	rawHex := "0A00D69FD7A5A7475DB07337749AE61FA53A4788E976"
	hash := ComputeContentHash(rawHex)

	// Manually compute expected: SHA256(0x02 + payload_bytes)
	payloadBytes, _ := hex.DecodeString("D69FD7A5A7475DB07337749AE61FA53A4788E976")
	toHash := append([]byte{0x02}, payloadBytes...)
	expected := sha256.Sum256(toHash)
	expectedHex := hex.EncodeToString(expected[:])[:16]
	if hash != expectedHex {
		t.Errorf("hash=%s, want %s (firmware-compatible)", hash, expectedHex)
	}
}

// TestComputeContentHashTraceGoldenValue is a golden-value test that locks down
// the 2-byte path_len (uint16 LE) behavior for TRACE hashing. If anyone removes
// the 0x00 byte from the hash input, this test breaks.
//
// Packet: header=0x25 (FLOOD route=1, payload_type=TRACE=0x09), pathByte=0x02
// (2 hops, 1-byte hash), path=[AA,BB], payload=[DE,AD,BE,EF].
// Hash input: [0x09, 0x02, 0x00, 0xDE, 0xAD, 0xBE, 0xEF]
//
//	→ SHA256 = b1baaf3bf0d0726c2672b1ec9e2665dc...
//	→ first 16 hex chars = "b1baaf3bf0d0726c"
func TestComputeContentHashTraceGoldenValue(t *testing.T) {
	// TRACE packet: header byte 0x25 = payload_type 9 (TRACE), route_type 1 (FLOOD)
	// pathByte 0x02 = hash_size 1, hash_count 2
	// 2 path bytes (AA, BB), then payload DEADBEEF
	rawHex := "2502AABBDEADBEEF"
	hash := ComputeContentHash(rawHex)

	// Pre-computed: SHA256(0x09 0x02 0x00 0xDE 0xAD 0xBE 0xEF)[:16hex]
	// The 0x00 is the high byte of uint16_t path_len (little-endian).
	const golden = "b1baaf3bf0d0726c"
	if hash != golden {
		t.Errorf("TRACE golden hash = %s, want %s (2-byte path_len encoding)", hash, golden)
	}
}

func TestDecodePacketWithWhitespace(t *testing.T) {
	raw := "0A 00 D6 9F D7 A5 A7 47 5D B0 73 37 74 9A E6 1F A5 3A 47 88 E9 76"
	pkt, err := DecodePacket(raw, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if pkt.Header.PayloadType != types.PayloadTXT_MSG {
		t.Errorf("payloadType=%d, want %d", pkt.Header.PayloadType, types.PayloadTXT_MSG)
	}
}

func TestDecodePacketWithNewlines(t *testing.T) {
	raw := "0A00\nD69F\r\nD7A5A7475DB07337749AE61FA53A4788E976"
	pkt, err := DecodePacket(raw, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if pkt.Payload.Type != "TXT_MSG" {
		t.Errorf("type=%s, want TXT_MSG", pkt.Payload.Type)
	}
}

func TestDecodePacketTransportRouteTooShort(t *testing.T) {
	// TRANSPORT_FLOOD (route=0) but only 2 bytes total → too short for transport codes
	_, err := DecodePacket("1400", nil, false)
	if err == nil {
		t.Error("expected error for transport route with too-short buffer")
	}
	if !strings.Contains(err.Error(), "transport codes") {
		t.Errorf("error should mention transport codes: %v", err)
	}
}

func TestDecodeAckShort(t *testing.T) {
	p := decodeAck([]byte{0x01, 0x02, 0x03})
	if p.Error != "too short" {
		t.Errorf("expected 'too short', got %q", p.Error)
	}
}

func TestDecodeAckValid(t *testing.T) {
	buf := []byte{0xAA, 0xBB, 0xCC, 0xDD}
	p := decodeAck(buf)
	if p.Error != "" {
		t.Errorf("unexpected error: %s", p.Error)
	}
	if p.ExtraHash != "ddccbbaa" {
		t.Errorf("extraHash=%s, want ddccbbaa", p.ExtraHash)
	}
	if p.DestHash != "" {
		t.Errorf("destHash should be empty, got %s", p.DestHash)
	}
	if p.SrcHash != "" {
		t.Errorf("srcHash should be empty, got %s", p.SrcHash)
	}
}

func TestIsTransportRoute(t *testing.T) {
	if !isTransportRoute(types.RouteTransportFlood) {
		t.Error("RouteTransportFlood should be transport")
	}
	if !isTransportRoute(types.RouteTransportDirect) {
		t.Error("RouteTransportDirect should be transport")
	}
	if isTransportRoute(types.RouteFlood) {
		t.Error("RouteFlood should not be transport")
	}
	if isTransportRoute(types.RouteDirect) {
		t.Error("RouteDirect should not be transport")
	}
}

func TestDecodeHeaderUnknownTypes(t *testing.T) {
	// Payload type that doesn't map to any known name
	// bits 5-2 = 0x0C (12) is CONTROL but 0x0D (13) would be unknown
	// byte = 0b00_1101_01 = 0x35 → routeType=1, payloadType=0x0D(13), version=0
	h := decodeHeader(0x35)
	if h.PayloadTypeName != "UNKNOWN" {
		t.Errorf("payloadTypeName=%s, want UNKNOWN for type 13", h.PayloadTypeName)
	}
}

func TestDecodePayloadMultipart(t *testing.T) {
	// MULTIPART (0x0A) now decoded — #1279 P0 #2 (firmware/src/Mesh.cpp:289).
	p := decodePayload(types.PayloadMULTIPART, []byte{0x01, 0x02}, nil, false)
	if p.Type != "MULTIPART" {
		t.Errorf("MULTIPART type=%s, want MULTIPART", p.Type)
	}
}

func TestDecodePayloadControl(t *testing.T) {
	// CONTROL (0x0B) now decoded — #1279 P1 #4 (firmware/src/Mesh.cpp:69).
	p := decodePayload(types.PayloadCONTROL, []byte{0x01, 0x02}, nil, false)
	if p.Type != "CONTROL" {
		t.Errorf("CONTROL type=%s, want CONTROL", p.Type)
	}
}

func TestDecodePathTruncatedBuffer(t *testing.T) {
	// path byte claims 5 hops of 2 bytes = 10 bytes, but only 4 available
	path, consumed, _ := decodePath(0x45, []byte{0xAA, 0x11, 0xBB, 0x22}, 0)
	if path.HashCount != 5 {
		t.Errorf("hashCount=%d, want 5", path.HashCount)
	}
	// Should only decode 2 hops (4 bytes / 2 bytes per hop)
	if len(path.Hops) != 2 {
		t.Errorf("hops=%d, want 2 (truncated)", len(path.Hops))
	}
	if consumed != 10 {
		t.Errorf("consumed=%d, want 10 (full claimed size)", consumed)
	}
}

func TestDecodeFloodAdvert5Hops(t *testing.T) {
	// From test-decoder.js Test 1
	raw := "11451000D818206D3AAC152C8A91F89957E6D30CA51F36E28790228971C473B755F244F718754CF5EE4A2FD58D944466E42CDED140C66D0CC590183E32BAF40F112BE8F3F2BDF6012B4B2793C52F1D36F69EE054D9A05593286F78453E56C0EC4A3EB95DDA2A7543FCCC00B939CACC009278603902FC12BCF84B706120526F6F6620536F6C6172"
	pkt, err := DecodePacket(raw, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if pkt.Header.RouteTypeName != "FLOOD" {
		t.Errorf("route=%s, want FLOOD", pkt.Header.RouteTypeName)
	}
	if pkt.Header.PayloadTypeName != "ADVERT" {
		t.Errorf("payload=%s, want ADVERT", pkt.Header.PayloadTypeName)
	}
	if pkt.Path.HashSize != 2 {
		t.Errorf("hashSize=%d, want 2", pkt.Path.HashSize)
	}
	if pkt.Path.HashCount != 5 {
		t.Errorf("hashCount=%d, want 5", pkt.Path.HashCount)
	}
	if pkt.Path.Hops[0] != "1000" {
		t.Errorf("hop[0]=%s, want 1000", pkt.Path.Hops[0])
	}
	if pkt.Path.Hops[1] != "D818" {
		t.Errorf("hop[1]=%s, want D818", pkt.Path.Hops[1])
	}
	if pkt.TransportCodes != nil {
		t.Error("FLOOD should have no transport codes")
	}
}

// --- Channel decryption tests ---

// buildTestCiphertext creates a valid AES-128-ECB encrypted GRP_TXT payload
// with a matching HMAC-SHA256 MAC for testing.
func buildTestCiphertext(channelKeyHex, senderMsg string, timestamp uint32) (ciphertextHex, macHex string) {
	channelKey, _ := hex.DecodeString(channelKeyHex)

	// Build plaintext: timestamp(4 LE) + flags(1) + message
	plain := make([]byte, 4+1+len(senderMsg))
	binary.LittleEndian.PutUint32(plain[0:4], timestamp)
	plain[4] = 0x00 // flags
	copy(plain[5:], senderMsg)

	// Pad to AES block boundary
	pad := aes.BlockSize - (len(plain) % aes.BlockSize)
	if pad != aes.BlockSize {
		plain = append(plain, make([]byte, pad)...)
	}

	// AES-128-ECB encrypt
	block, _ := aes.NewCipher(channelKey)
	ct := make([]byte, len(plain))
	for i := 0; i < len(plain); i += aes.BlockSize {
		block.Encrypt(ct[i:i+aes.BlockSize], plain[i:i+aes.BlockSize])
	}

	// HMAC-SHA256 MAC (first 2 bytes)
	secret := make([]byte, 32)
	copy(secret, channelKey)
	h := hmac.New(sha256.New, secret)
	h.Write(ct)
	mac := h.Sum(nil)

	return hex.EncodeToString(ct), hex.EncodeToString(mac[:2])
}

func TestDecryptChannelMessageValid(t *testing.T) {
	key := "2cc3d22840e086105ad73443da2cacb8"
	ctHex, macHex := buildTestCiphertext(key, "Alice: Hello world", 1700000000)

	result, err := decryptChannelMessage(ctHex, macHex, key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Sender != "Alice" {
		t.Errorf("sender=%q, want Alice", result.Sender)
	}
	if result.Message != "Hello world" {
		t.Errorf("message=%q, want 'Hello world'", result.Message)
	}
	if result.Timestamp != 1700000000 {
		t.Errorf("timestamp=%d, want 1700000000", result.Timestamp)
	}
}

func TestDecryptChannelMessageMACFail(t *testing.T) {
	key := "2cc3d22840e086105ad73443da2cacb8"
	ctHex, _ := buildTestCiphertext(key, "Alice: Hello", 100)
	wrongMac := "ffff"

	_, err := decryptChannelMessage(ctHex, wrongMac, key)
	if err == nil {
		t.Fatal("expected MAC verification failure")
	}
	if !strings.Contains(err.Error(), "MAC") {
		t.Errorf("error should mention MAC: %v", err)
	}
}

func TestDecryptChannelMessageWrongKey(t *testing.T) {
	key := "2cc3d22840e086105ad73443da2cacb8"
	ctHex, macHex := buildTestCiphertext(key, "Alice: Hello", 100)
	wrongKey := "deadbeefdeadbeefdeadbeefdeadbeef"

	_, err := decryptChannelMessage(ctHex, macHex, wrongKey)
	if err == nil {
		t.Fatal("expected error with wrong key")
	}
}

func TestDecryptChannelMessageNoSender(t *testing.T) {
	key := "aaaabbbbccccddddaaaabbbbccccdddd"
	ctHex, macHex := buildTestCiphertext(key, "Just a message", 500)

	result, err := decryptChannelMessage(ctHex, macHex, key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Sender != "" {
		t.Errorf("sender=%q, want empty", result.Sender)
	}
	if result.Message != "Just a message" {
		t.Errorf("message=%q, want 'Just a message'", result.Message)
	}
}

func TestDecryptChannelMessageSenderWithBrackets(t *testing.T) {
	key := "aaaabbbbccccddddaaaabbbbccccdddd"
	ctHex, macHex := buildTestCiphertext(key, "[admin]: Not a sender", 500)

	result, err := decryptChannelMessage(ctHex, macHex, key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Sender != "" {
		t.Errorf("sender=%q, want empty (brackets disqualify)", result.Sender)
	}
	if result.Message != "[admin]: Not a sender" {
		t.Errorf("message=%q", result.Message)
	}
}

func TestDecryptChannelMessageInvalidKey(t *testing.T) {
	_, err := decryptChannelMessage("aabb", "cc", "ZZZZ")
	if err == nil {
		t.Fatal("expected error for invalid key hex")
	}
}

func TestDecryptChannelMessageShortKey(t *testing.T) {
	_, err := decryptChannelMessage("aabb", "cc", "aabb")
	if err == nil {
		t.Fatal("expected error for short key")
	}
}

func TestDecodeGrpTxtWithDecryption(t *testing.T) {
	key := "2cc3d22840e086105ad73443da2cacb8"
	ctHex, macHex := buildTestCiphertext(key, "Bob: Testing 123", 1700000000)
	macBytes, _ := hex.DecodeString(macHex)
	ctBytes, _ := hex.DecodeString(ctHex)

	// Build GRP_TXT payload: channelHash(1) + MAC(2) + encrypted
	buf := []byte{0xAA}
	buf = append(buf, macBytes...)
	buf = append(buf, ctBytes...)

	keys := map[string]string{"#test": key}
	p := decodeGrpTxt(buf, keys)

	if p.Type != "CHAN" {
		t.Errorf("type=%s, want CHAN", p.Type)
	}
	if p.DecryptionStatus != "decrypted" {
		t.Errorf("decryptionStatus=%s, want decrypted", p.DecryptionStatus)
	}
	if p.Channel != "#test" {
		t.Errorf("channel=%s, want #test", p.Channel)
	}
	if p.Sender != "Bob" {
		t.Errorf("sender=%q, want Bob", p.Sender)
	}
	if p.Text != "Bob: Testing 123" {
		t.Errorf("text=%q, want 'Bob: Testing 123'", p.Text)
	}
	if p.ChannelHash != 0xAA {
		t.Errorf("channelHash=%d, want 0xAA", p.ChannelHash)
	}
	if p.ChannelHashHex != "AA" {
		t.Errorf("channelHashHex=%s, want AA", p.ChannelHashHex)
	}
	if p.SenderTimestamp != 1700000000 {
		t.Errorf("senderTimestamp=%d, want 1700000000", p.SenderTimestamp)
	}
}

func TestDecodeGrpTxtDecryptionFailed(t *testing.T) {
	key := "2cc3d22840e086105ad73443da2cacb8"
	ctHex, macHex := buildTestCiphertext(key, "Hello", 100)
	macBytes, _ := hex.DecodeString(macHex)
	ctBytes, _ := hex.DecodeString(ctHex)

	buf := []byte{0xFF}
	buf = append(buf, macBytes...)
	buf = append(buf, ctBytes...)

	wrongKeys := map[string]string{"#wrong": "deadbeefdeadbeefdeadbeefdeadbeef"}
	p := decodeGrpTxt(buf, wrongKeys)

	if p.Type != "GRP_TXT" {
		t.Errorf("type=%s, want GRP_TXT", p.Type)
	}
	if p.DecryptionStatus != "decryption_failed" {
		t.Errorf("decryptionStatus=%s, want decryption_failed", p.DecryptionStatus)
	}
	if p.ChannelHashHex != "FF" {
		t.Errorf("channelHashHex=%s, want FF", p.ChannelHashHex)
	}
}

func TestDecodeGrpTxtNoKey(t *testing.T) {
	buf := []byte{0x03, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22}
	p := decodeGrpTxt(buf, nil)

	if p.Type != "GRP_TXT" {
		t.Errorf("type=%s, want GRP_TXT", p.Type)
	}
	if p.DecryptionStatus != "no_key" {
		t.Errorf("decryptionStatus=%s, want no_key", p.DecryptionStatus)
	}
	if p.ChannelHashHex != "03" {
		t.Errorf("channelHashHex=%s, want 03", p.ChannelHashHex)
	}
}

func TestDecodeGrpTxtEmptyKeys(t *testing.T) {
	buf := []byte{0xFF, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22}
	p := decodeGrpTxt(buf, map[string]string{})

	if p.DecryptionStatus != "no_key" {
		t.Errorf("decryptionStatus=%s, want no_key", p.DecryptionStatus)
	}
}

func TestDecodeGrpTxtShortEncryptedNoDecryptAttempt(t *testing.T) {
	// encryptedData < 5 bytes (10 hex chars) → should not attempt decryption
	buf := []byte{0xFF, 0xAA, 0xBB, 0xCC, 0xDD}
	keys := map[string]string{"#test": "2cc3d22840e086105ad73443da2cacb8"}
	p := decodeGrpTxt(buf, keys)

	if p.DecryptionStatus != "no_key" {
		t.Errorf("decryptionStatus=%s, want no_key (too short for decryption)", p.DecryptionStatus)
	}
}

func TestDecodeGrpTxtMultipleKeysTriesAll(t *testing.T) {
	correctKey := "2cc3d22840e086105ad73443da2cacb8"
	ctHex, macHex := buildTestCiphertext(correctKey, "Eve: Found it", 999)
	macBytes, _ := hex.DecodeString(macHex)
	ctBytes, _ := hex.DecodeString(ctHex)

	buf := []byte{0x01}
	buf = append(buf, macBytes...)
	buf = append(buf, ctBytes...)

	keys := map[string]string{
		"#wrong1":  "deadbeefdeadbeefdeadbeefdeadbeef",
		"#correct": correctKey,
		"#wrong2":  "11111111111111111111111111111111",
	}
	p := decodeGrpTxt(buf, keys)

	if p.Type != "CHAN" {
		t.Errorf("type=%s, want CHAN", p.Type)
	}
	if p.Channel != "#correct" {
		t.Errorf("channel=%s, want #correct", p.Channel)
	}
	if p.Sender != "Eve" {
		t.Errorf("sender=%q, want Eve", p.Sender)
	}
}

func TestDecodeGrpTxtChannelHashHexZeroPad(t *testing.T) {
	buf := []byte{0x03, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE}
	p := decodeGrpTxt(buf, nil)
	if p.ChannelHashHex != "03" {
		t.Errorf("channelHashHex=%s, want 03 (zero-padded)", p.ChannelHashHex)
	}
}

func TestDecodeGrpTxtChannelHashHexFF(t *testing.T) {
	buf := []byte{0xFF, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE}
	p := decodeGrpTxt(buf, nil)
	if p.ChannelHashHex != "FF" {
		t.Errorf("channelHashHex=%s, want FF", p.ChannelHashHex)
	}
}

// --- Garbage text detection (fixes #197) ---

func TestDecryptChannelMessageGarbageText(t *testing.T) {
	// Build ciphertext with binary garbage as the message
	key := "2cc3d22840e086105ad73443da2cacb8"
	garbage := "\x01\x02\x03\x80\x81"
	ctHex, macHex := buildTestCiphertext(key, garbage, 1700000000)

	_, err := decryptChannelMessage(ctHex, macHex, key)
	if err == nil {
		t.Fatal("expected error for garbage text, got nil")
	}
	if !strings.Contains(err.Error(), "non-printable") {
		t.Errorf("error should mention non-printable: %v", err)
	}
}

func TestDecryptChannelMessageValidText(t *testing.T) {
	key := "2cc3d22840e086105ad73443da2cacb8"
	ctHex, macHex := buildTestCiphertext(key, "Alice: Hello\nworld", 1700000000)

	result, err := decryptChannelMessage(ctHex, macHex, key)
	if err != nil {
		t.Fatalf("unexpected error for valid text: %v", err)
	}
	if result.Sender != "Alice" {
		t.Errorf("sender=%q, want Alice", result.Sender)
	}
	if result.Message != "Hello\nworld" {
		t.Errorf("message=%q, want 'Hello\\nworld'", result.Message)
	}
}

func TestDecodeGrpTxtGarbageMarkedFailed(t *testing.T) {
	key := "2cc3d22840e086105ad73443da2cacb8"
	garbage := "\x01\x02\x03\x04\x05"
	ctHex, macHex := buildTestCiphertext(key, garbage, 1700000000)

	macBytes, _ := hex.DecodeString(macHex)
	ctBytes, _ := hex.DecodeString(ctHex)
	buf := make([]byte, 1+2+len(ctBytes))
	buf[0] = 0xFF // channel hash
	buf[1] = macBytes[0]
	buf[2] = macBytes[1]
	copy(buf[3:], ctBytes)

	keys := map[string]string{"#general": key}
	p := decodeGrpTxt(buf, keys)

	if p.DecryptionStatus != "decryption_failed" {
		t.Errorf("decryptionStatus=%s, want decryption_failed", p.DecryptionStatus)
	}
	if p.Type != "GRP_TXT" {
		t.Errorf("type=%s, want GRP_TXT", p.Type)
	}
}

func TestDecodeAdvertWithTelemetry(t *testing.T) {
	pubkey := strings.Repeat("AA", 32)
	timestamp := "78563412"
	signature := strings.Repeat("BB", 64)
	flags := "94" // sensor(4) | hasLocation(0x10) | hasName(0x80)
	lat := "40933402"
	lon := "E0E6B8F8"
	name := hex.EncodeToString([]byte("Sensor1"))
	nullTerm := "00"
	batteryLE := make([]byte, 2)
	binary.LittleEndian.PutUint16(batteryLE, 3700)
	tempLE := make([]byte, 2)
	binary.LittleEndian.PutUint16(tempLE, uint16(int16(2850)))

	hexStr := "1200" + pubkey + timestamp + signature + flags + lat + lon +
		name + nullTerm +
		hex.EncodeToString(batteryLE) + hex.EncodeToString(tempLE)

	pkt, err := DecodePacket(hexStr, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	if pkt.Payload.Name != "Sensor1" {
		t.Errorf("name=%s, want Sensor1", pkt.Payload.Name)
	}
	if pkt.Payload.BatteryMv == nil {
		t.Fatal("battery_mv should not be nil")
	}
	if *pkt.Payload.BatteryMv != 3700 {
		t.Errorf("battery_mv=%d, want 3700", *pkt.Payload.BatteryMv)
	}
	if pkt.Payload.TemperatureC == nil {
		t.Fatal("temperature_c should not be nil")
	}
	if math.Abs(*pkt.Payload.TemperatureC-28.50) > 0.01 {
		t.Errorf("temperature_c=%f, want 28.50", *pkt.Payload.TemperatureC)
	}
}

func TestDecodeAdvertWithTelemetryNegativeTemp(t *testing.T) {
	pubkey := strings.Repeat("CC", 32)
	timestamp := "00000000"
	signature := strings.Repeat("DD", 64)
	flags := "84" // sensor(4) | hasName(0x80), no location
	name := hex.EncodeToString([]byte("Cold"))
	nullTerm := "00"
	batteryLE := make([]byte, 2)
	binary.LittleEndian.PutUint16(batteryLE, 4200)
	tempLE := make([]byte, 2)
	var negTemp int16 = -550
	binary.LittleEndian.PutUint16(tempLE, uint16(negTemp))

	hexStr := "1200" + pubkey + timestamp + signature + flags +
		name + nullTerm +
		hex.EncodeToString(batteryLE) + hex.EncodeToString(tempLE)

	pkt, err := DecodePacket(hexStr, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	if pkt.Payload.Name != "Cold" {
		t.Errorf("name=%s, want Cold", pkt.Payload.Name)
	}
	if pkt.Payload.BatteryMv == nil || *pkt.Payload.BatteryMv != 4200 {
		t.Errorf("battery_mv=%v, want 4200", pkt.Payload.BatteryMv)
	}
	if pkt.Payload.TemperatureC == nil {
		t.Fatal("temperature_c should not be nil")
	}
	if math.Abs(*pkt.Payload.TemperatureC-(-5.50)) > 0.01 {
		t.Errorf("temperature_c=%f, want -5.50", *pkt.Payload.TemperatureC)
	}
}

func TestDecodeAdvertWithoutTelemetry(t *testing.T) {
	pubkey := strings.Repeat("EE", 32)
	timestamp := "00000000"
	signature := strings.Repeat("FF", 64)
	flags := "82" // repeater(2) | hasName(0x80)
	name := hex.EncodeToString([]byte("Node1"))

	hexStr := "1200" + pubkey + timestamp + signature + flags + name
	pkt, err := DecodePacket(hexStr, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	if pkt.Payload.Name != "Node1" {
		t.Errorf("name=%s, want Node1", pkt.Payload.Name)
	}
	if pkt.Payload.BatteryMv != nil {
		t.Errorf("battery_mv should be nil for advert without telemetry, got %d", *pkt.Payload.BatteryMv)
	}
	if pkt.Payload.TemperatureC != nil {
		t.Errorf("temperature_c should be nil for advert without telemetry, got %f", *pkt.Payload.TemperatureC)
	}
}

func TestDecodeAdvertNonSensorIgnoresTelemetryBytes(t *testing.T) {
	// A repeater node with 4 trailing bytes after the name should NOT decode telemetry.
	pubkey := strings.Repeat("AB", 32)
	timestamp := "00000000"
	signature := strings.Repeat("CD", 64)
	flags := "82" // repeater(2) | hasName(0x80)
	name := hex.EncodeToString([]byte("Rptr"))
	nullTerm := "00"
	extraBytes := "B40ED403" // battery-like and temp-like bytes

	hexStr := "1200" + pubkey + timestamp + signature + flags + name + nullTerm + extraBytes
	pkt, err := DecodePacket(hexStr, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if pkt.Payload.BatteryMv != nil {
		t.Errorf("battery_mv should be nil for non-sensor node, got %d", *pkt.Payload.BatteryMv)
	}
	if pkt.Payload.TemperatureC != nil {
		t.Errorf("temperature_c should be nil for non-sensor node, got %f", *pkt.Payload.TemperatureC)
	}
}

func TestDecodeAdvertTelemetryZeroTemp(t *testing.T) {
	// 0°C is a valid temperature and must be emitted.
	pubkey := strings.Repeat("12", 32)
	timestamp := "00000000"
	signature := strings.Repeat("34", 64)
	flags := "84" // sensor(4) | hasName(0x80)
	name := hex.EncodeToString([]byte("FreezeSensor"))
	nullTerm := "00"
	batteryLE := make([]byte, 2)
	binary.LittleEndian.PutUint16(batteryLE, 3600)
	tempLE := make([]byte, 2) // tempRaw=0 → 0°C

	hexStr := "1200" + pubkey + timestamp + signature + flags +
		name + nullTerm +
		hex.EncodeToString(batteryLE) + hex.EncodeToString(tempLE)

	pkt, err := DecodePacket(hexStr, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if pkt.Payload.TemperatureC == nil {
		t.Fatal("temperature_c should not be nil for 0°C")
	}
	if *pkt.Payload.TemperatureC != 0.0 {
		t.Errorf("temperature_c=%f, want 0.0", *pkt.Payload.TemperatureC)
	}
}

func repeatHex(byteHex string, n int) string {
	s := ""
	for i := 0; i < n; i++ {
		s += byteHex
	}
	return s
}

func TestZeroHopDirectHashSize(t *testing.T) {
	// DIRECT (RouteType=2) + REQ (PayloadType=0) → header byte = 0x02
	// pathByte=0x00 → hash_count=0, hash_size bits=0 → should get HashSize=0
	hex := "02" + "00" + repeatHex("AA", 20)
	pkt, err := DecodePacket(hex, nil, false)
	if err != nil {
		t.Fatalf("DecodePacket failed: %v", err)
	}
	if pkt.Path.HashSize != 0 {
		t.Errorf("DIRECT zero-hop: want HashSize=0, got %d", pkt.Path.HashSize)
	}
}

func TestZeroHopDirectHashSizeWithNonZeroUpperBits(t *testing.T) {
	// DIRECT (RouteType=2) + REQ (PayloadType=0) → header byte = 0x02
	// pathByte=0x40 → hash_count=0, hash_size bits=01 → should still get HashSize=0
	hex := "02" + "40" + repeatHex("AA", 20)
	pkt, err := DecodePacket(hex, nil, false)
	if err != nil {
		t.Fatalf("DecodePacket failed: %v", err)
	}
	if pkt.Path.HashSize != 0 {
		t.Errorf("DIRECT zero-hop with hash_size bits set: want HashSize=0, got %d", pkt.Path.HashSize)
	}
}

func TestNonDirectZeroPathByteKeepsHashSize(t *testing.T) {
	// FLOOD (RouteType=1) + REQ (PayloadType=0) → header byte = 0x01
	// pathByte=0x00 → non-DIRECT should keep HashSize=1
	hex := "01" + "00" + repeatHex("AA", 20)
	pkt, err := DecodePacket(hex, nil, false)
	if err != nil {
		t.Fatalf("DecodePacket failed: %v", err)
	}
	if pkt.Path.HashSize != 1 {
		t.Errorf("FLOOD zero pathByte: want HashSize=1, got %d", pkt.Path.HashSize)
	}
}

func TestDirectNonZeroHopKeepsHashSize(t *testing.T) {
	// DIRECT (RouteType=2) + REQ (PayloadType=0) → header byte = 0x02
	// pathByte=0x01 → hash_count=1, hash_size=1 → should keep HashSize=1
	hex := "02" + "01" + repeatHex("BB", 21)
	pkt, err := DecodePacket(hex, nil, false)
	if err != nil {
		t.Fatalf("DecodePacket failed: %v", err)
	}
	if pkt.Path.HashSize != 1 {
		t.Errorf("DIRECT with 1 hop: want HashSize=1, got %d", pkt.Path.HashSize)
	}
}

func TestZeroHopTransportDirectHashSize(t *testing.T) {
	// TRANSPORT_DIRECT (RouteType=3) + REQ (PayloadType=0) → header byte = 0x03
	// 4 bytes transport codes + pathByte=0x00 → hash_count=0 → should get HashSize=0
	hex := "03" + "11223344" + "00" + repeatHex("AA", 20)
	pkt, err := DecodePacket(hex, nil, false)
	if err != nil {
		t.Fatalf("DecodePacket failed: %v", err)
	}
	if pkt.Path.HashSize != 0 {
		t.Errorf("TRANSPORT_DIRECT zero-hop: want HashSize=0, got %d", pkt.Path.HashSize)
	}
}

func TestZeroHopTransportDirectHashSizeWithNonZeroUpperBits(t *testing.T) {
	// pathByte=0xC0 → hash_size bits=11 (4, reserved per firmware Packet.cpp:13-18).
	// Firmware Packet::isValidPathLen rejects this regardless of hash_count,
	// because hash_size==4 is reserved. Go decoder must mirror that — even
	// when hash_count==0, an attacker-emitted 0xC0 byte should not be
	// silently accepted; firmware never emits hash_size==4.
	hex := "03" + "11223344" + "C0" + repeatHex("AA", 20)
	_, err := DecodePacket(hex, nil, false)
	if err == nil {
		t.Fatalf("DecodePacket(pathByte=0xC0) succeeded; want error mirroring firmware Packet.cpp:13-18 (hash_size==4 reserved)")
	}
}

func TestValidateAdvertSignature(t *testing.T) {
	// Generate a real ed25519 key pair
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}

	var timestamp uint32 = 1234567890
	appdata := []byte{0x02, 0x11, 0x22} // flags + some data

	// Build the signed message: pubKey + timestamp(LE) + appdata
	message := make([]byte, 32+4+len(appdata))
	copy(message[0:32], pub)
	binary.LittleEndian.PutUint32(message[32:36], timestamp)
	copy(message[36:], appdata)

	sig := ed25519.Sign(priv, message)

	// Valid signature
	valid, err := sigvalidate.ValidateAdvert([]byte(pub), sig, timestamp, appdata)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !valid {
		t.Error("expected valid signature")
	}

	// Tampered appdata → invalid
	badAppdata := []byte{0x03, 0x11, 0x22}
	valid, err = sigvalidate.ValidateAdvert([]byte(pub), sig, timestamp, badAppdata)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if valid {
		t.Error("expected invalid signature with tampered appdata")
	}

	// Wrong timestamp → invalid
	valid, err = sigvalidate.ValidateAdvert([]byte(pub), sig, timestamp+1, appdata)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if valid {
		t.Error("expected invalid signature with wrong timestamp")
	}

	// Wrong length pubkey
	_, err = sigvalidate.ValidateAdvert([]byte{0xAA, 0xBB}, sig, timestamp, appdata)
	if err == nil {
		t.Error("expected error for short pubkey")
	}

	// Wrong length signature
	_, err = sigvalidate.ValidateAdvert([]byte(pub), []byte{0xAA, 0xBB}, timestamp, appdata)
	if err == nil {
		t.Error("expected error for short signature")
	}
}

func TestDecodeAdvertWithSignatureValidation(t *testing.T) {
	// Generate key pair
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}

	var timestamp uint32 = 1000000
	appdata := []byte{0x02} // repeater type, no location

	// Build signed message
	message := make([]byte, 32+4+len(appdata))
	copy(message[0:32], pub)
	binary.LittleEndian.PutUint32(message[32:36], timestamp)
	copy(message[36:], appdata)
	sig := ed25519.Sign(priv, message)

	// Build advert buffer: pubkey(32) + timestamp(4) + signature(64) + appdata
	buf := make([]byte, 0, 101)
	buf = append(buf, pub...)
	ts := make([]byte, 4)
	binary.LittleEndian.PutUint32(ts, timestamp)
	buf = append(buf, ts...)
	buf = append(buf, sig...)
	buf = append(buf, appdata...)

	// With validation enabled
	p := decodeAdvert(buf, true)
	if p.Error != "" {
		t.Fatalf("decode error: %s", p.Error)
	}
	if p.SignatureValid == nil {
		t.Fatal("SignatureValid should be set when validation enabled")
	}
	if !*p.SignatureValid {
		t.Error("expected valid signature")
	}

	// Without validation
	p2 := decodeAdvert(buf, false)
	if p2.SignatureValid != nil {
		t.Error("SignatureValid should be nil when validation disabled")
	}
}

// === Tests for DecodePathFromRawHex (issue #886) ===

func TestDecodePathFromRawHex_HashSize1(t *testing.T) {
	// Header byte 0x26 = route_type DIRECT, payload TRACE
	// Path byte 0x04 = hash_size 1 (bits 7-6 = 00 → 0+1=1), hash_count 4
	// Path bytes: 30 2D 0D 23
	raw := "2604302D0D2359FEE7B100000000006733D63367"
	hops, err := packetpath.DecodePathFromRawHex(raw)
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{"30", "2D", "0D", "23"}
	if len(hops) != len(expected) {
		t.Fatalf("got %d hops, want %d", len(hops), len(expected))
	}
	for i, h := range hops {
		if h != expected[i] {
			t.Errorf("hop[%d] = %s, want %s", i, h, expected[i])
		}
	}
}

func TestDecodePathFromRawHex_HashSize2(t *testing.T) {
	// Path byte 0x42 = hash_size 2 (bits 7-6 = 01 → 1+1=2), hash_count 2
	// Header 0x09 = FLOOD route (rt=1), payload ADVERT (pt=2)
	// Path bytes: AABB CCDD (4 bytes = 2 hops * 2 bytes)
	raw := "0942AABBCCDD" + "00000000000000"
	hops, err := packetpath.DecodePathFromRawHex(raw)
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{"AABB", "CCDD"}
	if len(hops) != len(expected) {
		t.Fatalf("got %d hops, want %d", len(hops), len(expected))
	}
	for i, h := range hops {
		if h != expected[i] {
			t.Errorf("hop[%d] = %s, want %s", i, h, expected[i])
		}
	}
}

func TestDecodePathFromRawHex_HashSize3(t *testing.T) {
	// Path byte 0x81 = hash_size 3 (bits 7-6 = 10 → 2+1=3), hash_count 1
	// Header 0x09 = FLOOD route (rt=1), payload ADVERT
	raw := "0981AABBCC" + "0000000000"
	hops, err := packetpath.DecodePathFromRawHex(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(hops) != 1 || hops[0] != "AABBCC" {
		t.Fatalf("got %v, want [AABBCC]", hops)
	}
}

func TestDecodePathFromRawHex_HashSize4(t *testing.T) {
	// Path byte 0xC1 = hash_size 4 (bits 7-6 = 11 → 3+1=4), hash_count 1
	// Header 0x09 = FLOOD route (rt=1)
	raw := "09C1AABBCCDD" + "0000000000"
	hops, err := packetpath.DecodePathFromRawHex(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(hops) != 1 || hops[0] != "AABBCCDD" {
		t.Fatalf("got %v, want [AABBCCDD]", hops)
	}
}

func TestDecodePathFromRawHex_DirectZeroHops(t *testing.T) {
	// Path byte 0x00 = hash_size 1, hash_count 0
	// Header 0x0A = DIRECT route (rt=2), payload ADVERT
	raw := "0A00" + "0000000000"
	hops, err := packetpath.DecodePathFromRawHex(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(hops) != 0 {
		t.Fatalf("got %d hops, want 0", len(hops))
	}
}

func TestDecodePathFromRawHex_Transport(t *testing.T) {
	// Route type 3 = TRANSPORT_DIRECT → 4 transport code bytes before path byte
	// Header 0x27 = route_type 3, payload TRACE
	// Transport codes: 1122 3344
	// Path byte 0x02 = hash_size 1, hash_count 2
	// Path bytes: AA BB
	raw := "2711223344" + "02AABB" + "0000000000"
	hops, err := packetpath.DecodePathFromRawHex(raw)
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{"AA", "BB"}
	if len(hops) != len(expected) {
		t.Fatalf("got %d hops, want %d", len(hops), len(expected))
	}
	for i, h := range hops {
		if h != expected[i] {
			t.Errorf("hop[%d] = %s, want %s", i, h, expected[i])
		}
	}
}

func TestDecodeTracePayloadFailSetsAnomaly(t *testing.T) {
	// Issue #889: TRACE packet with payload too short to decode (< 9 bytes)
	// should still return a DecodedPacket (observation stored) but with Anomaly
	// set to warn operators that the decode was degraded.
	// Packet: header 0x26 (TRACE+DIRECT), pathByte 0x00, payload 4 bytes (too short).
	pkt, err := DecodePacket("2600aabbccdd", nil, false)
	if err != nil {
		t.Fatalf("DecodePacket error: %v", err)
	}
	if pkt.Payload.Type != "TRACE" {
		t.Fatalf("payload type=%s, want TRACE", pkt.Payload.Type)
	}
	if pkt.Payload.Error == "" {
		t.Fatal("expected payload.Error to indicate decode failure")
	}
	// The key assertion: Anomaly must be set when TRACE decode fails
	if pkt.Anomaly == "" {
		t.Error("expected Anomaly to be set when TRACE payload decode fails but observation is stored")
	}
}

// TestDecodeTraceExtractsSNRValues verifies that for TRACE packets, the header
// path bytes are interpreted as int8 SNR values (quarter-dB) and exposed via
// payload.SNRValues. Mirrors logic in cmd/server/decoder.go (issue: SNR values
// extracted by server but never written into decoded_json by ingestor).
//
// Packet 26022FF8116A23A80000000001C0DE1000DEDE:
//
//	header  0x26 → TRACE (pt=9), DIRECT (rt=2)
//	pathByte 0x02 → hash_size=1, hash_count=2
//	header path: 2F F8 → SNR = [int8(0x2F)/4, int8(0xF8)/4] = [11.75, -2.0]
//	payload (15B): tag=116A23A8 auth=00000000 flags=0x01 pathData=C0DE1000DEDE
func TestDecodeTraceExtractsSNRValues(t *testing.T) {
	pkt, err := DecodePacket("26022FF8116A23A80000000001C0DE1000DEDE", nil, false)
	if err != nil {
		t.Fatalf("DecodePacket error: %v", err)
	}
	if pkt.Payload.Type != "TRACE" {
		t.Fatalf("payload type=%s, want TRACE", pkt.Payload.Type)
	}
	if len(pkt.Payload.SNRValues) != 2 {
		t.Fatalf("len(SNRValues)=%d, want 2 (got %v)", len(pkt.Payload.SNRValues), pkt.Payload.SNRValues)
	}
	if pkt.Payload.SNRValues[0] != 11.75 {
		t.Errorf("SNRValues[0]=%v, want 11.75", pkt.Payload.SNRValues[0])
	}
	if pkt.Payload.SNRValues[1] != -2.0 {
		t.Errorf("SNRValues[1]=%v, want -2.0", pkt.Payload.SNRValues[1])
	}
}

// TestDecodePacketBoundsFromWire — regression for issue #1211.
//
// A malformed packet on the wire claimed pathByte=0xF6 (hash_size=4, hash_count=54
// → 216 path bytes) inside a 15-byte buffer. decodePath() returned bytesConsumed=216
// without bounds-check, causing the outer slice `payloadBuf := buf[offset:]` to
// blow up with `slice bounds out of range [218:15]`.
//
// Expected behaviour: DecodePacket MUST NOT panic on any input. If the path
// length claimed by the wire byte exceeds the buffer, it should return a
// clean error.
func TestDecodePacketBoundsFromWire_Issue1211(t *testing.T) {
	// 15-byte buffer: header=0x12 (rt=DIRECT, pt=ADVERT), pathByte=0xF6
	// (hash_size=4, hash_count=54 → claims 216 path bytes), + 13 garbage bytes.
	raw := "12F6" + strings.Repeat("AA", 13)
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("DecodePacket panicked on malformed input: %v", r)
		}
	}()
	pkt, err := DecodePacket(raw, nil, false)
	if err == nil {
		t.Fatalf("expected error for malformed packet (path claims 216 bytes in 15-byte buf), got nil; pkt=%+v", pkt)
	}
}

// TestDecodePacketFuzzTruncated — sweep the decoder with truncated payloads.
// Zero panics is the acceptance bar.
//
// Adv M2: the original loop ran 256*256*20 = 1.3M iterations on every
// `go test` (in both packages, so 2.6M total). That is not "fuzzing" — it
// is an expensive deterministic sweep that runs in the default unit-test
// path with no opt-in. We now:
//
//   - gate the exhaustive sweep on !testing.Short() so `go test -short`
//     skips it (CI's unit gate runs short)
//   - keep the full sweep under `go test ./...` to preserve coverage
//   - prefer `go test -fuzz=FuzzDecodePacketTruncated` for actual
//     randomized fuzzing (see FuzzDecodePacketTruncated below)
func TestDecodePacketFuzzTruncated_Issue1211(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("DecodePacket panicked during fuzz: %v", r)
		}
	}()
	if testing.Short() {
		t.Skip("skipping exhaustive sweep in -short mode; use FuzzDecodePacketTruncated")
	}
	// Sweep every pathByte value with a short tail.
	for hdr := 0; hdr < 256; hdr++ {
		for pb := 0; pb < 256; pb++ {
			for tail := 0; tail < 20; tail++ {
				raw := hex.EncodeToString([]byte{byte(hdr), byte(pb)}) + strings.Repeat("00", tail)
				_, _ = DecodePacket(raw, nil, false)
			}
		}
	}
}

// FuzzDecodePacketTruncated — native go fuzz target. Run with:
//
//	go test -fuzz=FuzzDecodePacketTruncated -fuzztime=30s ./cmd/ingestor
//
// Zero panics regardless of input is the acceptance bar.
func FuzzDecodePacketTruncated(f *testing.F) {
	seeds := [][]byte{
		{0x12, 0xF6, 0xAA, 0xAA, 0xAA},
		{0x12, 0x00},
		{0x03, 0x11, 0x22, 0x33, 0x44, 0xC0, 0xAA, 0xAA, 0xAA},
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("DecodePacket panicked on input %x: %v", data, r)
			}
		}()
		_, _ = DecodePacket(hex.EncodeToString(data), nil, false)
	})
}

// TestDecodeAdvertOversizedNameTruncated asserts decodeAdvert truncates the
// advert name to firmware's MAX_ADVERT_DATA_SIZE=32 (firmware/src/MeshCore.h:11).
// Firmware writes the node name into a 32-byte buffer, so any on-wire advert
// carrying >32 bytes of name data is adversarial — the Go decoder must not
// surface attacker-controlled bytes beyond what firmware would ever emit.
func TestDecodeAdvertOversizedNameTruncated(t *testing.T) {
	pubkey := repeatHex("AA", 32)
	timestamp := "78563412"
	signature := repeatHex("BB", 64)
	flags := "81" // chat(1) | hasName(0x80), no location, no feat1/2
	// 64-byte ASCII 'X' name with no null terminator (firmware buffer is 32 bytes).
	name := repeatHex("58", 64)
	hex := "1200" + pubkey + timestamp + signature + flags + name
	pkt, err := DecodePacket(hex, nil, false)
	if err != nil {
		t.Fatalf("DecodePacket: %v", err)
	}
	if got := len(pkt.Payload.Name); got > 32 {
		t.Errorf("name length=%d, want <=32 (MAX_ADVERT_DATA_SIZE firmware/src/MeshCore.h:11)", got)
	}
}

func TestDecodeGrpDataNoKey(t *testing.T) {
	// Envelope alone (no key in store).
	buf := []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11}
	p := decodeGrpData(buf, nil)
	if p.Type != "GRP_DATA" {
		t.Fatalf("type=%q want GRP_DATA", p.Type)
	}
	if p.ChannelHash != 0xAA {
		t.Errorf("channelHash=%d want 170", p.ChannelHash)
	}
	if p.ChannelHashHex != "AA" {
		t.Errorf("channelHashHex=%q want AA", p.ChannelHashHex)
	}
	if p.MAC != "bbcc" {
		t.Errorf("mac=%q want bbcc", p.MAC)
	}
	if p.EncryptedData != "ddeeff11" {
		t.Errorf("encryptedData=%q want ddeeff11", p.EncryptedData)
	}
	if p.DecryptionStatus != "no_key" {
		t.Errorf("decryptionStatus=%q want no_key", p.DecryptionStatus)
	}
}

func TestDecodeGrpDataDecryptedInner(t *testing.T) {
	// Inner per BaseChatMesh.cpp:382-385: data_type(uint16 LE) + data_len(1) + blob.
	key := "2cc3d22840e086105ad73443da2cacb8"
	blob := []byte{0x10, 0x20, 0x30, 0x40, 0x50}
	inner := []byte{0x34, 0x12, byte(len(blob))} // data_type = 0x1234
	inner = append(inner, blob...)
	ctHex, macHex := buildChannelEncrypted(key, inner)

	buf := []byte{0xAB}
	mb, _ := hex.DecodeString(macHex)
	buf = append(buf, mb...)
	cb, _ := hex.DecodeString(ctHex)
	buf = append(buf, cb...)

	p := decodeGrpData(buf, map[string]string{"test": key})
	if p.Type != "GRP_DATA" {
		t.Fatalf("type=%q want GRP_DATA", p.Type)
	}
	if p.DecryptionStatus != "decrypted" {
		t.Fatalf("decryptionStatus=%q want decrypted", p.DecryptionStatus)
	}
	if p.DataType == nil || *p.DataType != 0x1234 {
		t.Errorf("dataType=%v want 0x1234", p.DataType)
	}
	if p.DataLen == nil || *p.DataLen != 5 {
		t.Errorf("dataLen=%v want 5", p.DataLen)
	}
	if p.DecryptedBlob != hex.EncodeToString(blob) {
		t.Errorf("decryptedBlob=%q want %q", p.DecryptedBlob, hex.EncodeToString(blob))
	}
	if p.Channel != "test" {
		t.Errorf("channel=%q want test", p.Channel)
	}
}

func buildChannelEncrypted(channelKeyHex string, inner []byte) (ctHex, macHex string) {
	key, _ := hex.DecodeString(channelKeyHex)
	plain := append([]byte{}, inner...)
	pad := aes.BlockSize - (len(plain) % aes.BlockSize)
	if pad != aes.BlockSize {
		plain = append(plain, make([]byte, pad)...)
	}
	block, _ := aes.NewCipher(key)
	ct := make([]byte, len(plain))
	for i := 0; i < len(plain); i += aes.BlockSize {
		block.Encrypt(ct[i:i+aes.BlockSize], plain[i:i+aes.BlockSize])
	}
	secret := make([]byte, 32)
	copy(secret, key)
	h := hmac.New(sha256.New, secret)
	h.Write(ct)
	mac := h.Sum(nil)
	return hex.EncodeToString(ct), hex.EncodeToString(mac[:2])
}

func TestDecodeMultipartAck(t *testing.T) {
	// remaining=3, inner_type=PAYLOAD_TYPE_ACK(0x03), ack_crc=0xDEADBEEF.
	// byte0 = (3<<4) | 3 = 0x33; next 4 bytes are LE crc.
	buf := []byte{0x33, 0xEF, 0xBE, 0xAD, 0xDE}
	p := decodeMultipart(buf)
	if p.Type != "MULTIPART" {
		t.Fatalf("type=%q want MULTIPART", p.Type)
	}
	if p.Remaining == nil || *p.Remaining != 3 {
		t.Errorf("remaining=%v want 3", p.Remaining)
	}
	if p.InnerType == nil || *p.InnerType != 0x03 {
		t.Errorf("innerType=%v want 3", p.InnerType)
	}
	if p.InnerTypeName != "ACK" {
		t.Errorf("innerTypeName=%q want ACK", p.InnerTypeName)
	}
	if p.InnerAckCrc != "deadbeef" {
		t.Errorf("innerAckCrc=%q want deadbeef", p.InnerAckCrc)
	}
}

func TestDecodeMultipartNonAck(t *testing.T) {
	// remaining=2, inner_type=0x02 (TXT_MSG), arbitrary inner payload.
	buf := []byte{0x22, 0x01, 0x02, 0x03}
	p := decodeMultipart(buf)
	if p.Remaining == nil || *p.Remaining != 2 {
		t.Errorf("remaining=%v want 2", p.Remaining)
	}
	if p.InnerType == nil || *p.InnerType != 0x02 {
		t.Errorf("innerType=%v want 2", p.InnerType)
	}
	if p.InnerTypeName != "TXT_MSG" {
		t.Errorf("innerTypeName=%q want TXT_MSG", p.InnerTypeName)
	}
	if p.InnerPayload != "010203" {
		t.Errorf("innerPayload=%q want 010203", p.InnerPayload)
	}
	if p.InnerAckCrc != "" {
		t.Errorf("non-ACK should not surface innerAckCrc, got %q", p.InnerAckCrc)
	}
}

func TestAdvertRoleLabelsRawType(t *testing.T) {
	// Firmware: ADV_TYPE_NONE=0, CHAT=1, REPEATER=2, ROOM=3, SENSOR=4, 5..15 FUTURE.
	cases := []struct {
		typ  int
		want string
	}{
		{0, "none"},
		{1, "companion"},
		{2, "repeater"},
		{3, "room"},
		{4, "sensor"},
		{5, "type-5"},
		{15, "type-15"},
	}
	for _, tc := range cases {
		got := AdvertRole(&AdvertFlags{Type: tc.typ, Repeater: tc.typ == 2, Room: tc.typ == 3, Sensor: tc.typ == 4})
		if got != tc.want {
			t.Errorf("advertRole(type=%d) = %q, want %q", tc.typ, got, tc.want)
		}
	}
}

func TestDecodeControlZeroHop(t *testing.T) {
	// byte0 = 0x81 (high-bit set ⇒ zero-hop), followed by 3 app bytes.
	buf := []byte{0x81, 0xAA, 0xBB, 0xCC}
	p := decodeControl(buf)
	if p.Type != "CONTROL" {
		t.Fatalf("type=%q want CONTROL", p.Type)
	}
	if p.CtrlFlags != "81" {
		t.Errorf("ctrlFlags=%q want 81", p.CtrlFlags)
	}
	if p.CtrlZeroHop == nil || !*p.CtrlZeroHop {
		t.Errorf("ctrlZeroHop=%v want true", p.CtrlZeroHop)
	}
	if p.CtrlLength == nil || *p.CtrlLength != 4 {
		t.Errorf("ctrlLength=%v want 4", p.CtrlLength)
	}
}

func TestDecodeControlMultiHop(t *testing.T) {
	// byte0 = 0x01 (high-bit clear ⇒ not zero-hop subset).
	buf := []byte{0x01, 0x42}
	p := decodeControl(buf)
	if p.CtrlFlags != "01" {
		t.Errorf("ctrlFlags=%q want 01", p.CtrlFlags)
	}
	if p.CtrlZeroHop == nil || *p.CtrlZeroHop {
		t.Errorf("ctrlZeroHop=%v want false", p.CtrlZeroHop)
	}
	if p.CtrlLength == nil || *p.CtrlLength != 2 {
		t.Errorf("ctrlLength=%v want 2", p.CtrlLength)
	}
}

func TestDecodeControl_DiscoverReq_FullBody(t *testing.T) {
	// byte0 = 0x80 (DISCOVER_REQ), filter=0x02, tag=0xDEADBEEF, since=0x11223344.
	buf := []byte{0x80, 0x02, 0xEF, 0xBE, 0xAD, 0xDE, 0x44, 0x33, 0x22, 0x11}
	p := decodeControl(buf)
	if p.Type != "CONTROL" {
		t.Fatalf("type=%q want CONTROL", p.Type)
	}
	if p.CtrlSubtype != "DISCOVER_REQ" {
		t.Errorf("ctrlSubtype=%q want DISCOVER_REQ", p.CtrlSubtype)
	}
	if p.CtrlFilter == nil || *p.CtrlFilter != 0x02 {
		t.Errorf("ctrlFilter=%v want 2", p.CtrlFilter)
	}
	if p.CtrlTag == nil || *p.CtrlTag != 0xDEADBEEF {
		t.Errorf("ctrlTag=%v want 0xDEADBEEF", p.CtrlTag)
	}
	if p.CtrlSince == nil || *p.CtrlSince != 0x11223344 {
		t.Errorf("ctrlSince=%v want 0x11223344", p.CtrlSince)
	}
}

func TestDecodeControl_DiscoverReq_NoSince(t *testing.T) {
	// 6-byte body (no optional since): byte0=0x80, filter, tag.
	buf := []byte{0x80, 0x04, 0x01, 0x02, 0x03, 0x04}
	p := decodeControl(buf)
	if p.CtrlSubtype != "DISCOVER_REQ" {
		t.Errorf("ctrlSubtype=%q want DISCOVER_REQ", p.CtrlSubtype)
	}
	if p.CtrlFilter == nil || *p.CtrlFilter != 0x04 {
		t.Errorf("ctrlFilter=%v want 4", p.CtrlFilter)
	}
	wantTag := binary.LittleEndian.Uint32([]byte{0x01, 0x02, 0x03, 0x04})
	if p.CtrlTag == nil || *p.CtrlTag != wantTag {
		t.Errorf("ctrlTag=%v want %#x", p.CtrlTag, wantTag)
	}
	if p.CtrlSince != nil {
		t.Errorf("ctrlSince should be nil for 6B REQ body, got %v", p.CtrlSince)
	}
}

func TestDecodeControl_DiscoverResp_FullPubKey(t *testing.T) {
	// byte0 = 0x90 | 0x02 (node_type=2, REPEATER), snr=0x10, tag, 32B pubkey.
	body := []byte{0x92, 0x10, 0x44, 0x33, 0x22, 0x11}
	for i := 0; i < 32; i++ {
		body = append(body, byte(i))
	}
	p := decodeControl(body)
	if p.CtrlSubtype != "DISCOVER_RESP" {
		t.Errorf("ctrlSubtype=%q want DISCOVER_RESP", p.CtrlSubtype)
	}
	if p.CtrlNodeType == nil || *p.CtrlNodeType != 2 {
		t.Errorf("ctrlNodeType=%v want 2", p.CtrlNodeType)
	}
	if p.CtrlSNR == nil || *p.CtrlSNR != 0x10 {
		t.Errorf("ctrlSNR=%v want 16", p.CtrlSNR)
	}
	wantTag := binary.LittleEndian.Uint32([]byte{0x44, 0x33, 0x22, 0x11})
	if p.CtrlTag == nil || *p.CtrlTag != wantTag {
		t.Errorf("ctrlTag=%v want %#x", p.CtrlTag, wantTag)
	}
	if len(p.CtrlPubKey) != 64 {
		t.Fatalf("ctrlPubKey hex len=%d want 64 (32B)", len(p.CtrlPubKey))
	}
	if p.CtrlPubKey[:4] != "0001" {
		t.Errorf("ctrlPubKey prefix=%q want 0001…", p.CtrlPubKey[:4])
	}
}

func TestDecodeControl_DiscoverResp_PrefixPubKey(t *testing.T) {
	// 6 header bytes + 8 pubkey bytes only (prefix_only path).
	body := []byte{0x91, 0xF0, 0xAA, 0xBB, 0xCC, 0xDD, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88}
	p := decodeControl(body)
	if p.CtrlSubtype != "DISCOVER_RESP" {
		t.Errorf("ctrlSubtype=%q want DISCOVER_RESP", p.CtrlSubtype)
	}
	if p.CtrlNodeType == nil || *p.CtrlNodeType != 1 {
		t.Errorf("ctrlNodeType=%v want 1", p.CtrlNodeType)
	}
	// snr = signed 0xF0 = -16
	if p.CtrlSNR == nil || *p.CtrlSNR != -16 {
		t.Errorf("ctrlSNR=%v want -16", p.CtrlSNR)
	}
	if len(p.CtrlPubKey) != 16 {
		t.Errorf("ctrlPubKey hex len=%d want 16 (8B)", len(p.CtrlPubKey))
	}
}

func TestDecodeControl_DiscoverResp_TruncatedPubKey(t *testing.T) {
	// 6 header bytes + 4 pubkey bytes — neither 8 nor 32. Must NOT panic;
	// subtype emitted, pubkey omitted.
	body := []byte{0x91, 0x05, 0xAA, 0xBB, 0xCC, 0xDD, 0x11, 0x22, 0x33, 0x44}
	p := decodeControl(body)
	if p.CtrlSubtype != "DISCOVER_RESP" {
		t.Errorf("ctrlSubtype=%q want DISCOVER_RESP", p.CtrlSubtype)
	}
	if p.CtrlPubKey != "" {
		t.Errorf("ctrlPubKey=%q want empty for 4B pubkey blob", p.CtrlPubKey)
	}
}

func TestDecodeControl_ShortBody_NoSubtypeFields(t *testing.T) {
	// byte0=0x80 only (no body). Must emit subtype, no panic, no body fields.
	buf := []byte{0x80}
	p := decodeControl(buf)
	if p.CtrlSubtype != "DISCOVER_REQ" {
		t.Errorf("ctrlSubtype=%q want DISCOVER_REQ", p.CtrlSubtype)
	}
	if p.CtrlFilter != nil {
		t.Errorf("ctrlFilter should be nil, got %v", p.CtrlFilter)
	}
	if p.CtrlTag != nil {
		t.Errorf("ctrlTag should be nil, got %v", p.CtrlTag)
	}
}

func TestDecodeControl_UnknownSubtype(t *testing.T) {
	// byte0 = 0xA0 — not DISCOVER_REQ/RESP. Subtype = "UNKNOWN".
	buf := []byte{0xA0, 0x11, 0x22}
	p := decodeControl(buf)
	if p.CtrlSubtype != "UNKNOWN" {
		t.Errorf("ctrlSubtype=%q want UNKNOWN", p.CtrlSubtype)
	}
}

func TestEpochToISO(t *testing.T) {
	for epoch, want := range map[uint32]string{
		0:          "1970-01-01T00:00:00.000Z", // a node whose clock was never set
		1767225600: "2026-01-01T00:00:00.000Z",
		4294967295: "2106-02-07T06:28:15.000Z", // the largest value a uint32 can carry
	} {
		if got := epochToISO(epoch); got != want {
			t.Errorf("epochToISO(%d) = %q, want %q", epoch, got, want)
		}
	}
}
