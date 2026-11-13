package decoder

import (
	"crypto/ed25519"
	"encoding/binary"
	"encoding/hex"
	"testing"
)

func TestDecodeAdvertSignatureValidation(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}

	var timestamp uint32 = 1234567890
	appdata := []byte{0x02} // flags: repeater, no extras

	// Build signed message: pubKey(32) + timestamp(4 LE) + appdata
	msg := make([]byte, 32+4+len(appdata))
	copy(msg[0:32], pub)
	binary.LittleEndian.PutUint32(msg[32:36], timestamp)
	copy(msg[36:], appdata)
	sig := ed25519.Sign(priv, msg)

	// Build a raw advert buffer: pubKey(32) + timestamp(4) + signature(64) + appdata
	buf := make([]byte, 100+len(appdata))
	copy(buf[0:32], pub)
	binary.LittleEndian.PutUint32(buf[32:36], timestamp)
	copy(buf[36:100], sig)
	copy(buf[100:], appdata)

	// With validation enabled
	p := decodeAdvert(buf, true)
	if p.SignatureValid == nil {
		t.Fatal("expected SignatureValid to be set")
	}
	if !*p.SignatureValid {
		t.Error("expected valid signature")
	}
	if p.PubKey != hex.EncodeToString(pub) {
		t.Errorf("pubkey mismatch: got %s", p.PubKey)
	}

	// Tamper with signature → invalid
	buf[40] ^= 0xFF
	p = decodeAdvert(buf, true)
	if p.SignatureValid == nil {
		t.Fatal("expected SignatureValid to be set")
	}
	if *p.SignatureValid {
		t.Error("expected invalid signature after tampering")
	}

	// Without validation → SignatureValid should be nil
	p = decodeAdvert(buf, false)
	if p.SignatureValid != nil {
		t.Error("expected SignatureValid to be nil when validation disabled")
	}
}
