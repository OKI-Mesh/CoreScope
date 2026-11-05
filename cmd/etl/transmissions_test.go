package main

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/OKI-Mesh/CoreScope/internal/decoder"
)

func TestSplitChannelHash(t *testing.T) {
	cases := []struct {
		name          string
		stored        sql.NullString
		wantHash      *string
		wantEncrypted bool
	}{
		{"NULL", sql.NullString{}, nil, false},
		{"empty", sql.NullString{Valid: true, String: ""}, nil, false},
		{"decrypted channel keeps its name", sql.NullString{Valid: true, String: "#wardriving"}, text("#wardriving"), false},
		{"bare hash byte stays as is", sql.NullString{Valid: true, String: "3A"}, text("3A"), false},
		{"enc_ prefix is stripped and flagged", sql.NullString{Valid: true, String: "enc_3A"}, text("3A"), true},
		{"enc_ with nothing after it", sql.NullString{Valid: true, String: "enc_"}, nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			hash, encrypted := splitChannelHash(c.stored)
			if !reflect.DeepEqual(hash, c.wantHash) || encrypted != c.wantEncrypted {
				t.Errorf("splitChannelHash = (%v, %v), want (%v, %v)", deref(hash), encrypted, deref(c.wantHash), c.wantEncrypted)
			}
		})
	}
}

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}

type loadedTransmission struct {
	ID               int64
	RawHex           string
	FromPubkey       *string
	RouteType        int
	PayloadType      int
	ChannelHash      *string
	ChannelEncrypted bool
	Code1            *string
	Code2            *string
	LastSeen         *string
}

func TestLoadTransmissions(t *testing.T) {
	ctx := context.Background()
	matchingHash := decoder.ComputeContentHash("0100AA")

	src := newSnapshot(t,
		`INSERT INTO nodes (public_key, name, role) VALUES ('aa01', 'Alpha', 'repeater'), ('bb02', 'Beta', 'repeater'), ('cc03', 'Spam', 'type-7')`,
		fmt.Sprintf(`INSERT INTO transmissions (id, raw_hex, hash, first_seen, route_type, payload_type, payload_version, decoded_json, from_pubkey, channel_hash, last_seen, code1, code2) VALUES
			(1, '0100AA', '%s', '2026-03-01T00:00:00Z', 1, 4, 0, '{"type":"ADVERT","pubKey":"aa01"}', 'AA01', NULL, 0, NULL, NULL),
			(2, '0100AB', 'h2', '2026-03-01T00:00:01Z', 1, 4, 0, '{"type":"ADVERT","pubKey":"BB02"}', NULL, NULL, 0, NULL, NULL),
			(3, '0100AC', 'h3', '2026-03-01T00:00:02Z', 1, 4, 0, '{"type":"ADVERT"}', 'cc03', NULL, 0, NULL, NULL),
			(4, '0100AD', 'h4', '2026-03-01T00:00:03Z', 1, 4, 0, '{"type":"ADVERT"}', 'ffff', NULL, 0, NULL, NULL),
			(5, '0100AE', 'h5', '2026-03-01T00:00:04Z', 1, 5, 0, '{"type":"GRP_TXT"}', NULL, 'enc_3A', 0, NULL, NULL),
			(6, '0100AF', 'h6', '2026-03-01T00:00:05Z', 1, 5, 0, '{"type":"CHAN"}', NULL, '#wardriving', 0, NULL, NULL),
			(7, '0100B0', 'h7', '2026-03-01T00:00:06Z', NULL, NULL, NULL, NULL, '', NULL, 0, NULL, NULL),
			(8, '0100B1', 'h8', '2026-03-01T00:00:07Z', 1, 2, 0, NULL, NULL, NULL, 1773532800, NULL, NULL),
			(9, '08CE6F000000AABBCC', 'h9', '2026-03-01T00:00:08Z', 0, 2, 0, NULL, NULL, NULL, 0, NULL, NULL),
			(11, '0100B2', 'h11', '2026-03-01T00:00:09Z', 1, 2, 0, NULL, NULL, NULL, 0, NULL, NULL)`, matchingHash),
		`INSERT INTO observations (transmission_id, observer_idx, timestamp) VALUES
			(7, 1, 0),
			(7, 2, 1769904000),
			(7, 3, 1770681600),
			(8, 1, 1773964800)`)
	tx := beginTx(t, newTarget(t))
	report := newReport()

	nodes, err := loadNodes(ctx, src, tx, report)
	if err != nil {
		t.Fatalf("loadNodes: %v", err)
	}
	loaded, err := loadTransmissions(ctx, src, tx, nodes, report)
	if err != nil {
		t.Fatalf("loadTransmissions: %v", err)
	}

	for _, id := range []int64{1, 5, 9, 11} {
		if !loaded.Has(id) {
			t.Errorf("Has(%d) = false, want true", id)
		}
	}
	for _, id := range []int64{0, 10, 12} {
		if loaded.Has(id) {
			t.Errorf("Has(%d) = true, want false", id)
		}
	}

	rows, err := tx.Query(ctx, `SELECT id, encode(raw_hex, 'hex'), from_pubkey, route_type, payload_type,
		channel_hash, channel_encrypted, code1, code2,
		to_char(last_seen AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
		FROM transmissions ORDER BY id`)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	defer rows.Close()
	var got []loadedTransmission
	for rows.Next() {
		var row loadedTransmission
		if err := rows.Scan(&row.ID, &row.RawHex, &row.FromPubkey, &row.RouteType, &row.PayloadType,
			&row.ChannelHash, &row.ChannelEncrypted, &row.Code1, &row.Code2, &row.LastSeen); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, row)
	}
	rows.Close()
	want := []loadedTransmission{
		{ID: 1, RawHex: "0100aa", FromPubkey: text("aa01"), RouteType: 1, PayloadType: 4},
		{ID: 2, RawHex: "0100ab", FromPubkey: text("bb02"), RouteType: 1, PayloadType: 4},
		{ID: 3, RawHex: "0100ac", RouteType: 1, PayloadType: 4},
		{ID: 4, RawHex: "0100ad", RouteType: 1, PayloadType: 4},
		{ID: 5, RawHex: "0100ae", RouteType: 1, PayloadType: 5, ChannelHash: text("3A"), ChannelEncrypted: true},
		{ID: 6, RawHex: "0100af", RouteType: 1, PayloadType: 5, ChannelHash: text("#wardriving")},
		{ID: 7, RawHex: "0100b0", RouteType: -1, PayloadType: -1, LastSeen: text("2026-02-10T00:00:00Z")},
		{ID: 8, RawHex: "0100b1", RouteType: 1, PayloadType: 2, LastSeen: text("2026-03-15T00:00:00Z")},
		{ID: 9, RawHex: "08ce6f000000aabbcc", RouteType: 0, PayloadType: 2, Code1: text("CE6F"), Code2: text("0000")},
		{ID: 11, RawHex: "0100b2", RouteType: 1, PayloadType: 2},
	}
	if !reflect.DeepEqual(got, want) {
		for i := range got {
			if i < len(want) && !reflect.DeepEqual(got[i], want[i]) {
				t.Errorf("transmission %d = %+v\nwant           %+v", got[i].ID, got[i], want[i])
			}
		}
		if len(got) != len(want) {
			t.Errorf("loaded %d transmissions, want %d", len(got), len(want))
		}
	}

	var next int64
	if err := tx.QueryRow(ctx, `INSERT INTO transmissions (raw_hex, hash, first_seen) VALUES ('\x00', 'next', now()) RETURNING id`).Scan(&next); err != nil {
		t.Fatalf("insert after load: %v", err)
	}
	if next != 12 {
		t.Errorf("next transmission id = %d, want 12 (sequence not reset)", next)
	}

	counts := map[string]int64{
		"transmissions: loaded": 10,
		"transmissions: stored hash differs from ComputeContentHash":      9,
		"transmissions: from_pubkey taken from decoded_json":              1,
		"transmissions: from_pubkey set NULL, node skipped (type-N role)": 1,
		"transmissions: from_pubkey set NULL, no node row":                1,
		"transmissions: enc_ prefix stripped, channel_encrypted set":      1,
		"transmissions: last_seen computed from observations":             1,
		"transmissions: transport codes decoded from raw_hex":             1,
		"transmissions: transport codes undecodable":                      0,
		"transmissions.first_seen: unparseable timestamp, loaded as NULL": 0,
	}
	for name, want := range counts {
		if got := report.Count(name); got != want {
			t.Errorf("report %q = %d, want %d", name, got, want)
		}
	}
}

func TestLoadTransmissionsStopsOnMalformedRawHex(t *testing.T) {
	src := newSnapshot(t,
		`INSERT INTO transmissions (id, raw_hex, hash, first_seen) VALUES (1, 'zz', 'h1', '2026-03-01T00:00:00Z')`)
	tx := beginTx(t, newTarget(t))

	_, err := loadTransmissions(context.Background(), src, tx, &LoadedNodes{}, newReport())
	if err == nil || !strings.Contains(err.Error(), "transmission 1") {
		t.Fatalf("err = %v, want one naming transmission 1", err)
	}
}

func TestLoadTransmissionsStopsWithoutFirstSeen(t *testing.T) {
	src := newSnapshot(t,
		`INSERT INTO transmissions (id, raw_hex, hash, first_seen) VALUES (1, '0100AA', 'h1', 'not a time')`)
	tx := beginTx(t, newTarget(t))

	_, err := loadTransmissions(context.Background(), src, tx, &LoadedNodes{}, newReport())
	if err == nil || !strings.Contains(err.Error(), "first_seen") {
		t.Fatalf("err = %v, want one naming first_seen", err)
	}
}
