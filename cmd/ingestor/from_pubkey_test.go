package main

// Tests for #1143: ingestor must populate transmissions.from_pubkey at
// write time (cheap — already parsing decoded_json) so attribution queries
// don't rely on JSON substring matches.

import (
	"database/sql"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/decoder"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

func TestInsertTransmission_FromPubkeyPopulatedForAdvert(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	const pk = "f7181c468dfe7c55aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	data := &PacketData{
		RawHex:         "AABBCC",
		Timestamp:      ts,
		ObserverID:     "obs1",
		Hash:           "advert_hash_1143",
		RouteType:      1,
		PayloadType:    4, // ADVERT
		PayloadVersion: 0,
		PathJSON:       "[]",
		DecodedJSON:    `{"type":"ADVERT","pubKey":"` + pk + `","name":"X"}`,
		FromPubkey:     pk, // still used as the hop-resolution anchor
		Advert: &database.NodeInput{ // use whatever type TransmissionInput.Advert has now
			PublicKey: pk, Name: "X", Role: "repeater", RxTime: ts,
		},
	}
	if _, err := store.InsertTransmission(ctx, data); err != nil {
		t.Fatal(err)
	}

	var got *string
	mustScan(t, testRow(t, store, "SELECT from_pubkey FROM transmissions WHERE hash = $1", data.Hash), &got)
	if got == nil || *got != pk {
		t.Fatalf("from_pubkey = %v, want %q", got, pk)
	}
	if n := countWhere(t, store, "nodes", "public_key = $1", pk); n != 1 {
		t.Errorf("nodes with key = %d, want 1 (written in the same transaction)", n)
	}
}

func TestInsertTransmission_FromPubkeyNullForNonAdvert(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	data := &PacketData{
		RawHex:         "AA",
		Timestamp:      time.Date(2026, 3, 25, 0, 0, 0, 0, time.UTC),
		ObserverID:     "obs1",
		Hash:           "txt_hash_1143",
		RouteType:      1,
		PayloadType:    2, // TXT_MSG
		PayloadVersion: 0,
		PathJSON:       "[]",
		DecodedJSON:    `{"type":"TXT_MSG"}`,
		// FromPubkey deliberately empty — non-ADVERTs don't carry one.
	}
	if _, err := store.InsertTransmission(ctx, data); err != nil {
		t.Fatal(err)
	}

	var got sql.NullString
	mustScan(t, testRow(t, store, "SELECT from_pubkey FROM transmissions WHERE hash = $1", data.Hash), &got)
	if got.Valid {
		t.Fatalf("from_pubkey for non-ADVERT must be NULL, got %q", got.String)
	}
}

func TestBuildPacketData_PopulatesFromPubkey(t *testing.T) {
	const pk = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
	msg := &MQTTPacketMessage{Raw: "AA", Origin: "obs"}
	decoded := &decoder.DecodedPacket{
		Header:  decoder.Header{PayloadType: types.PayloadADVERT},
		Payload: decoder.Payload{Type: "ADVERT", PubKey: pk},
	}
	pd := BuildPacketData(msg, decoded, "obs", "", nil)
	if pd.FromPubkey != pk {
		t.Fatalf("BuildPacketData FromPubkey = %q, want %q", pd.FromPubkey, pk)
	}

	// Non-ADVERT: must not carry a pubkey.
	decoded2 := &decoder.DecodedPacket{
		Header:  decoder.Header{PayloadType: types.PayloadTXT_MSG},
		Payload: decoder.Payload{Type: "TXT_MSG"},
	}
	pd2 := BuildPacketData(msg, decoded2, "obs", "", nil)
	if pd2.FromPubkey != "" {
		t.Fatalf("BuildPacketData FromPubkey for non-ADVERT = %q, want empty", pd2.FromPubkey)
	}
}
