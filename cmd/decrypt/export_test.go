package main

import (
	"context"
	"crypto/aes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/channel"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/pgtest"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

func TestMain(m *testing.M) { os.Exit(pgtest.Run(m)) }

// encryptGroupText builds the GRP_TXT payload (channel hash byte, 2-byte MAC,
// ECB ciphertext) for text sent on channelName at unix time ts. The plaintext
// is the 4-byte little-endian timestamp, a flags byte, then "sender: message"
// and a NUL, zero-padded to a whole AES block.
func encryptGroupText(t *testing.T, channelName string, ts uint32, text string) []byte {
	t.Helper()
	key := channel.DeriveKey(channelName)

	plaintext := make([]byte, 5, 64)
	binary.LittleEndian.PutUint32(plaintext, ts)
	plaintext = append(plaintext, text...)
	plaintext = append(plaintext, 0)
	for len(plaintext)%aes.BlockSize != 0 {
		plaintext = append(plaintext, 0)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	ciphertext := make([]byte, len(plaintext))
	for i := 0; i < len(plaintext); i += aes.BlockSize {
		block.Encrypt(ciphertext[i:i+aes.BlockSize], plaintext[i:i+aes.BlockSize])
	}

	secret := make([]byte, 32)
	copy(secret, key)
	h := hmac.New(sha256.New, secret)
	h.Write(ciphertext)
	mac := h.Sum(nil)[:2]

	payload := []byte{channel.ChannelHash(key), mac[0], mac[1]}
	return append(payload, ciphertext...)
}

// groupTextFrame wraps payload in a FLOOD frame of the given payload type with
// an empty path.
func groupTextFrame(payloadType types.PayloadType, payload []byte) types.RawFrame {
	header := byte(types.RouteFlood) | byte(payloadType)<<2
	return append(types.RawFrame{header, 0x00}, payload...)
}

func seedTransmission(ctx context.Context, t *testing.T, db *database.DB, hash string, payloadType types.PayloadType, payload []byte, rx time.Time) (int64, types.RawFrame) {
	t.Helper()
	frame := groupTextFrame(payloadType, payload)
	txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		RawHex:           frame.Hex(),
		Hash:             hash,
		RouteType:        types.RouteFlood,
		PayloadType:      payloadType,
		PayloadVersion:   0,
		DecodedJSON:      "{}",
		ChannelHash:      fmt.Sprintf("%02X", payload[0]),
		ChannelEncrypted: true,
	}, rx)
	if err != nil {
		t.Fatalf("seed transmission %s: %v", hash, err)
	}
	return txID, frame
}

func TestExportChannel(t *testing.T) {
	ctx := t.Context()
	db, err := database.OpenReadWrite(ctx, pgtest.NewDatabase(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	base := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	sent := uint32(base.Unix())

	firstID, firstFrame := seedTransmission(ctx, t, db, "aa00000000000001", types.PayloadGRP_TXT,
		encryptGroupText(t, "#wardriving", sent, "Alice: first"), base)
	seedTransmission(ctx, t, db, "aa00000000000002", types.PayloadGRP_TXT,
		encryptGroupText(t, "#wardriving", sent+60, "Bob: second"), base.Add(time.Minute))
	// Another channel's message: same payload type, different key.
	seedTransmission(ctx, t, db, "bb00000000000001", types.PayloadGRP_TXT,
		encryptGroupText(t, "#other", sent+120, "Carol: elsewhere"), base.Add(2*time.Minute))
	// Right key, wrong payload type: must never be read.
	seedTransmission(ctx, t, db, "cc00000000000001", types.PayloadGRP_DATA,
		encryptGroupText(t, "#wardriving", sent+180, "Dave: not text"), base.Add(3*time.Minute))
	seedTransmission(ctx, t, db, "aa00000000000003", types.PayloadGRP_TXT,
		encryptGroupText(t, "#wardriving", sent+240, "Eve: third"), base.Add(4*time.Minute))

	observerID, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        "0f0f0f0f",
		Name:      "Hilltop",
		IATA:      "CVG",
		LastSeen:  base,
		IngestNow: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	snr, rssi := 5.5, -92.0
	if _, _, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: firstID,
		ObserverIdx:    &observerID,
		SNR:            &snr,
		RSSI:           &rssi,
		Timestamp:      base.Add(time.Second),
	}, nil, nil); err != nil {
		t.Fatal(err)
	}
	// A second copy with no observer and no signal values.
	if _, _, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: firstID,
		Timestamp:      base.Add(2 * time.Second),
	}, nil, nil); err != nil {
		t.Fatal(err)
	}

	// A page size of 2 over 4 GRP_TXT rows forces a second page.
	messages, total, decrypted, err := exportChannel(ctx, db, "#wardriving", 2)
	if err != nil {
		t.Fatal(err)
	}
	if total != 4 {
		t.Errorf("scanned %d GRP_TXT transmissions, want 4 (the GRP_DATA row must not be read)", total)
	}
	if decrypted != 3 {
		t.Errorf("decrypted %d, want 3", decrypted)
	}
	if len(messages) != 3 {
		t.Fatalf("got %d messages, want 3: %+v", len(messages), messages)
	}

	first := messages[0]
	if first.Hash != "aa00000000000001" || first.Sender != "Alice" || first.Message != "first" || first.Channel != "#wardriving" {
		t.Errorf("first message = %+v", first)
	}
	if want := base.Format(time.RFC3339); first.Timestamp != want {
		t.Errorf("first timestamp = %q, want %q", first.Timestamp, want)
	}
	if first.RawHex != firstFrame.Hex() {
		t.Errorf("first raw hex = %q, want %q", first.RawHex, firstFrame.Hex())
	}
	if len(first.Path) != 0 {
		t.Errorf("first path = %v, want none", first.Path)
	}
	if len(first.Observers) != 2 {
		t.Fatalf("first has %d observers, want 2: %+v", len(first.Observers), first.Observers)
	}
	if got, want := first.Observers[0], (Observer{Name: "Hilltop", SNR: 5.5, RSSI: -92, Timestamp: base.Add(time.Second).Format(time.RFC3339)}); got != want {
		t.Errorf("observer 0 = %+v, want %+v", got, want)
	}
	if got, want := first.Observers[1], (Observer{Timestamp: base.Add(2 * time.Second).Format(time.RFC3339)}); got != want {
		t.Errorf("observer 1 = %+v, want %+v", got, want)
	}

	if second := messages[1]; second.Sender != "Bob" || second.Message != "second" || len(second.Observers) != 0 {
		t.Errorf("second message = %+v", second)
	}
	if third := messages[2]; third.Sender != "Eve" || third.Message != "third" {
		t.Errorf("third message = %+v", third)
	}

	t.Run("channel with no messages", func(t *testing.T) {
		none, total, decrypted, err := exportChannel(ctx, db, "#nobody", 2)
		if err != nil {
			t.Fatal(err)
		}
		if none == nil || len(none) != 0 {
			t.Errorf("messages = %#v, want an empty non-nil slice", none)
		}
		if total != 4 || decrypted != 0 {
			t.Errorf("total=%d decrypted=%d, want 4 and 0", total, decrypted)
		}
	})
}
