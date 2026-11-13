package main

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// TestInsertTransmissionMapsEveryPacketDataField sets every PacketData field and
// reads each stored value back, so a field that stops being written fails here.
// The fixture is checked for zero values, so a field added to PacketData fails
// until it is given a value here and an assertion below.
func TestInsertTransmissionMapsEveryPacketDataField(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	given := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	advertiser := strings.Repeat("ab", 32)

	if err := dbtest.Observer(t, store.db, "obs-map", "Mapper", "SJC", given, given); err == 0 {
		t.Fatal("expected an error from dbtest.Observer, got nil")
	}

	snr, rssi, score, direction := 5.5, -92.0, 0.75, "RX"
	data := &PacketData{
		RawHex:            "14AABBCCDD00DEADBEEF",
		Timestamp:         given,
		ObserverID:        "obs-map",
		SNR:               &snr,
		RSSI:              &rssi,
		Score:             &score,
		Direction:         &direction,
		Hash:              "0123456789abcdef",
		RouteType:         types.RouteTransportDirect,
		PayloadType:       types.PayloadGRP_TXT,
		PayloadVersion:    1,
		PathJSON:          `["aa","bb"]`,
		DecodedJSON:       `{"type":"CHAN","channel":"#mapping","text":"A: hi","sender":"A"}`,
		ChannelHash:       "#mapping",
		ScopeName:         "#region",
		IsTransportScoped: true,
		Code1:             "AABB",
		Code2:             "CCDD",
		FromPubkey:        advertiser,
		Advert: &database.NodeInput{
			PublicKey: advertiser,
			Name:      "Advertiser",
			Role:      "repeater",
			RxTime:    given,
		},
		ChannelEncrypted: true,
	}
	fixture := reflect.ValueOf(*data)
	for i := 0; i < fixture.NumField(); i++ {
		if fixture.Field(i).IsZero() {
			t.Errorf("fixture leaves PacketData.%s at its zero value", fixture.Type().Field(i).Name)
		}
	}

	isNew, err := store.InsertTransmission(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	if !isNew {
		t.Fatal("want a new transmission")
	}

	var (
		txID                    int64
		rawHex, hash            string
		route, payload, version int
		decoded, channel        *string
		encrypted               bool
		scope, code1, code2     *string
		fromPubkey              *string
		firstSeen, lastSeen     time.Time
	)
	mustScan(t, testRow(t, store, `SELECT id, encode(raw_hex, 'hex'), hash, route_type, payload_type, payload_version,
		decoded_json, channel_hash, channel_encrypted, scope_name, code1, code2, from_pubkey, first_seen, last_seen
		FROM transmissions WHERE hash = $1`, "0123456789abcdef"),
		&txID, &rawHex, &hash, &route, &payload, &version,
		&decoded, &channel, &encrypted, &scope, &code1, &code2, &fromPubkey, &firstSeen, &lastSeen)

	if rawHex != "14aabbccdd00deadbeef" {
		t.Errorf("raw_hex = %q", rawHex)
	}
	if route != int(types.RouteTransportDirect) || payload != int(types.PayloadGRP_TXT) || version != 1 {
		t.Errorf("route/payload/version = %d/%d/%d, want %d/%d/1", route, payload, version, types.RouteTransportDirect, types.PayloadGRP_TXT)
	}
	if decoded == nil || *decoded != data.DecodedJSON {
		t.Errorf("decoded_json = %v, want %q", decoded, data.DecodedJSON)
	}
	if channel == nil || *channel != "#mapping" || !encrypted {
		t.Errorf("channel_hash/channel_encrypted = %v/%v, want #mapping/true", channel, encrypted)
	}
	if scope == nil || *scope != "#region" {
		t.Errorf("scope_name = %v, want #region", scope)
	}
	if code1 == nil || *code1 != "AABB" || code2 == nil || *code2 != "CCDD" {
		t.Errorf("code1/code2 = %v/%v, want AABB/CCDD", code1, code2)
	}
	if fromPubkey == nil || *fromPubkey != advertiser {
		t.Errorf("from_pubkey = %v, want the advert's key %s", fromPubkey, advertiser)
	}
	if !firstSeen.Equal(given) || !lastSeen.Equal(given) {
		t.Errorf("first_seen/last_seen = %v/%v, want both %v", firstSeen, lastSeen, given)
	}

	var advertName string
	mustScan(t, testRow(t, store, `SELECT name FROM nodes WHERE public_key = $1`, advertiser), &advertName)
	if advertName != "Advertiser" {
		t.Errorf("advert node name = %q, want Advertiser", advertName)
	}

	var (
		obsDirection              *string
		obsSNR, obsRSSI, obsScore *float64
		pathHash                  *string
		obsTime                   time.Time
		hops                      []string
		frame                     *string
		observerPubkey            string
	)
	mustScan(t, testRow(t, store, `SELECT o.direction, o.snr, o.rssi, o.score, o.path_hash, o.timestamp, o.hop_prefixes,
		encode(f.frame, 'hex'), ob.pubkey
		FROM observations o
		LEFT JOIN observation_frames f ON f.observation_id = o.id
		JOIN observers ob ON ob.id = o.observer_idx
		WHERE o.transmission_id = $1`, txID),
		&obsDirection, &obsSNR, &obsRSSI, &obsScore, &pathHash, &obsTime, &hops, &frame, &observerPubkey)

	if obsDirection == nil || *obsDirection != "RX" {
		t.Errorf("direction = %v, want RX", obsDirection)
	}
	if obsSNR == nil || *obsSNR != 5.5 || obsRSSI == nil || *obsRSSI != -92.0 || obsScore == nil || *obsScore != 0.75 {
		t.Errorf("snr/rssi/score = %v/%v/%v, want 5.5/-92/0.75", obsSNR, obsRSSI, obsScore)
	}
	wantPathHash := types.HopList([]string{"aa", "bb"}).Hash()
	if pathHash == nil || *pathHash != wantPathHash {
		t.Errorf("path_hash = %v, want %q", pathHash, wantPathHash)
	}
	if !obsTime.Equal(given) {
		t.Errorf("observation timestamp = %v, want %v", obsTime, given)
	}
	if !reflect.DeepEqual(hops, []string{"aa", "bb"}) {
		t.Errorf("hop_prefixes = %v, want [aa bb]", hops)
	}
	if frame != nil {
		t.Errorf("observation frame = %q, want none stored: it equals the transmission's raw_hex", *frame)
	}
	if observerPubkey != "obs-map" {
		t.Errorf("observer = %q, want obs-map", observerPubkey)
	}
}
