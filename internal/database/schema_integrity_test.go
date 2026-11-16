package database_test

import (
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// TestSchemaColumnTypes pins the column types and nullability that the
// Postgres migration is meant to guarantee. Every last_seen is NOT NULL: it is
// the "updated at" of its row and only ever moves forward.
func TestSchemaColumnTypes(t *testing.T) {
	db := dbtest.New(t)
	ctx := t.Context()

	want := map[string]string{
		"observers.last_seen":             "timestamp with time zone NOT NULL",
		"observers.first_seen":            "timestamp with time zone NOT NULL",
		"observers.packet_count":          "bigint NOT NULL",
		"observers.name":                  "text NULL",
		"observers.iata":                  "text NULL",
		"observers.battery_mv":            "integer NULL",
		"observers.uptime_secs":           "bigint NULL",
		"observers.noise_floor":           "double precision NULL",
		"nodes.last_seen":                 "timestamp with time zone NOT NULL",
		"nodes.first_seen":                "timestamp with time zone NOT NULL",
		"transmissions.last_seen":         "timestamp with time zone NOT NULL",
		"transmissions.first_seen":        "timestamp with time zone NOT NULL",
		"transmissions.channel_encrypted": "boolean NOT NULL",
		"neighbor_edges.last_seen":        "timestamp with time zone NOT NULL",
		"client_observers.last_seen":      "timestamp with time zone NOT NULL",
		"observations.score":              "double precision NULL",
		"observations.path_hash":          "text NULL",
	}

	rows, err := db.Pool().Query(ctx, `
		SELECT table_name || '.' || column_name,
		       data_type || CASE is_nullable WHEN 'NO' THEN ' NOT NULL' ELSE ' NULL' END
		FROM information_schema.columns
		WHERE table_schema = current_schema()`)
	if err != nil {
		t.Fatalf("read information_schema: %v", err)
	}
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var column, shape string
		if err := rows.Scan(&column, &shape); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[column] = shape
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read information_schema: %v", err)
	}

	for column, shape := range want {
		if got[column] != shape {
			t.Errorf("%s = %q, want %q", column, got[column], shape)
		}
	}
	if shape, ok := got["nodes.multibyte_max_hash_size"]; ok {
		t.Errorf("nodes.multibyte_max_hash_size should not exist, found %q", shape)
	}
}

func TestObserversIATACheck_DatabaseRejectsUnnormalizedCodes(t *testing.T) {
	db := dbtest.New(t)
	ctx := t.Context()

	for _, bad := range []string{"cvg", " CVG", "CVG ", ""} {
		_, err := db.Pool().Exec(ctx,
			`INSERT INTO observers (pubkey, last_seen, first_seen, iata) VALUES ('aa01', now(), now(), $1)`, bad)
		if err == nil || !strings.Contains(err.Error(), "observers_iata_normalized") {
			t.Errorf("iata %q: err = %v, want a violation of observers_iata_normalized", bad, err)
		}
	}
	for i, good := range []*string{nil, ptrTo("CVG")} {
		_, err := db.Pool().Exec(ctx,
			`INSERT INTO observers (pubkey, last_seen, first_seen, iata) VALUES ($1, now(), now(), $2)`,
			[]string{"bb01", "bb02"}[i], good)
		if err != nil {
			t.Errorf("iata %v: unexpected error %v", good, err)
		}
	}
}

func TestCreateOrUpdateObserver_NormalizesIATA_DB(t *testing.T) {
	db := dbtest.New(t)
	ctx := t.Context()
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	dbtest.Observer(t, db, "aa01", "Lower", "cvg", now, now)
	dbtest.Observer(t, db, "aa02", "Padded", "  day\t", now, now)
	dbtest.Observer(t, db, "aa03", "Blank", "   ", now, now)

	want := map[string]*string{"aa01": ptrTo("CVG"), "aa02": ptrTo("DAY"), "aa03": nil}
	for pubkey, wantIATA := range want {
		var got *string
		if err := db.Pool().QueryRow(ctx, `SELECT iata FROM observers WHERE pubkey = $1`, pubkey).Scan(&got); err != nil {
			t.Fatalf("read %s: %v", pubkey, err)
		}
		switch {
		case wantIATA == nil && got != nil:
			t.Errorf("%s: iata = %q, want NULL", pubkey, *got)
		case wantIATA != nil && (got == nil || *got != *wantIATA):
			t.Errorf("%s: iata = %v, want %q", pubkey, got, *wantIATA)
		}
	}
}

func TestUpdateObserverMetadataIfRetained_NormalizesIATA_DB(t *testing.T) {
	db := dbtest.New(t)
	ctx := t.Context()
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	dbtest.Observer(t, db, "aa01", "Alpha", "", now, now)

	if err := db.UpdateObserverMetadataIfRetained(ctx, database.ObserverMetadataInput{ID: "aa01", Name: ptr("Alpha"), IATA: " luk "}); err != nil {
		t.Fatalf("UpdateObserverMetadataIfRetained: %v", err)
	}
	var got *string
	if err := db.Pool().QueryRow(ctx, `SELECT iata FROM observers WHERE pubkey = 'aa01'`).Scan(&got); err != nil {
		t.Fatalf("read: %v", err)
	}
	if got == nil || *got != "LUK" {
		t.Errorf("iata = %v, want LUK", got)
	}
}

func TestCreateOrUpdateObserver_ZeroLastSeenUsesIngestNow_DB(t *testing.T) {
	db := dbtest.New(t)
	ctx := t.Context()
	ingest := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	if _, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{ID: "aa01", Name: ptr("Alpha"), IngestNow: ingest}); err != nil {
		t.Fatalf("CreateOrUpdateObserver: %v", err)
	}
	var first, last time.Time
	if err := db.Pool().QueryRow(ctx, `SELECT first_seen, last_seen FROM observers WHERE pubkey = 'aa01'`).Scan(&first, &last); err != nil {
		t.Fatalf("read: %v", err)
	}
	if !first.Equal(ingest) || !last.Equal(ingest) {
		t.Errorf("first_seen = %v, last_seen = %v, want both %v", first, last, ingest)
	}
}

func TestCreateOrUpdateObserver_NoTimeAtAllIsAnError_DB(t *testing.T) {
	db := dbtest.New(t)

	_, err := db.CreateOrUpdateObserver(t.Context(), database.ObserverInput{ID: "aa01", Name: ptr("Alpha")})
	if err == nil || !strings.Contains(err.Error(), "neither LastSeen nor IngestNow") {
		t.Fatalf("err = %v, want one saying the observer has neither LastSeen nor IngestNow", err)
	}
	var n int
	if err := db.Pool().QueryRow(t.Context(), `SELECT count(*) FROM observers`).Scan(&n); err != nil || n != 0 {
		t.Errorf("observers stored = %d (%v), want 0", n, err)
	}
}

type storedTransmission struct {
	ChannelHash      *string
	ChannelEncrypted bool
	DecodedJSON      *string
	FirstSeen        time.Time
	LastSeen         time.Time
}

func readStoredTransmission(t *testing.T, db *database.DB, hash string) storedTransmission {
	t.Helper()
	var s storedTransmission
	err := db.Pool().QueryRow(t.Context(),
		`SELECT channel_hash, channel_encrypted, decoded_json, first_seen, last_seen FROM transmissions WHERE hash = $1`, hash).
		Scan(&s.ChannelHash, &s.ChannelEncrypted, &s.DecodedJSON, &s.FirstSeen, &s.LastSeen)
	if err != nil {
		t.Fatalf("read transmission %s: %v", hash, err)
	}
	return s
}

func channelTransmission(hash, decodedJSON, channelHash string, encrypted bool) database.TransmissionInput {
	return database.TransmissionInput{
		RawHex:           "0180e1",
		Hash:             hash,
		RouteType:        types.RouteFlood,
		PayloadType:      types.PayloadGRP_TXT,
		DecodedJSON:      decodedJSON,
		ChannelHash:      channelHash,
		ChannelEncrypted: encrypted,
	}
}

func TestCreateOrUpdateTransmission_LateKeyDecryptsStoredRow_DB(t *testing.T) {
	db := dbtest.New(t)
	ctx := t.Context()
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	locked := channelTransmission("c1", `{"type":"GRP_TXT","channelHashHex":"3a"}`, "3a", true)
	unlocked := channelTransmission("c1", `{"type":"CHAN","channel":"#wardriving","text":"hi"}`, "#wardriving", false)

	id, isNew, err := db.CreateOrUpdateTransmission(ctx, locked, base)
	if err != nil || !isNew {
		t.Fatalf("first insert: isNew = %v, err = %v", isNew, err)
	}
	again, isNew, err := db.CreateOrUpdateTransmission(ctx, unlocked, base.Add(time.Minute))
	if err != nil || isNew || again != id {
		t.Fatalf("decrypted copy: id = %d (want %d), isNew = %v, err = %v", again, id, isNew, err)
	}

	got := readStoredTransmission(t, db, "c1")
	if got.ChannelEncrypted {
		t.Error("channel_encrypted stayed true after a copy that decrypted")
	}
	if got.ChannelHash == nil || *got.ChannelHash != "#wardriving" {
		t.Errorf("channel_hash = %v, want #wardriving", got.ChannelHash)
	}
	if got.DecodedJSON == nil || *got.DecodedJSON != unlocked.DecodedJSON {
		t.Errorf("decoded_json = %v, want %s", got.DecodedJSON, unlocked.DecodedJSON)
	}
}

func TestCreateOrUpdateTransmission_EncryptedCopyNeverUndoesDecryption_DB(t *testing.T) {
	db := dbtest.New(t)
	ctx := t.Context()
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	locked := channelTransmission("c2", `{"type":"GRP_TXT","channelHashHex":"3a"}`, "3a", true)
	unlocked := channelTransmission("c2", `{"type":"CHAN","channel":"#wardriving","text":"hi"}`, "#wardriving", false)

	if _, _, err := db.CreateOrUpdateTransmission(ctx, unlocked, base); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.CreateOrUpdateTransmission(ctx, locked, base.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	got := readStoredTransmission(t, db, "c2")
	if got.ChannelEncrypted || got.ChannelHash == nil || *got.ChannelHash != "#wardriving" ||
		got.DecodedJSON == nil || *got.DecodedJSON != unlocked.DecodedJSON {
		t.Errorf("row after an encrypted repeat = %+v, want the decrypted row unchanged", got)
	}
}

func TestCreateOrUpdateTransmission_RepeatMovesLastSeenForwardOnly_DB(t *testing.T) {
	db := dbtest.New(t)
	ctx := t.Context()
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	in := channelTransmission("c3", `{"type":"CHAN","channel":"#wardriving","text":"hi"}`, "#wardriving", false)

	if _, _, err := db.CreateOrUpdateTransmission(ctx, in, base.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.CreateOrUpdateTransmission(ctx, in, base); err != nil {
		t.Fatal(err)
	}
	got := readStoredTransmission(t, db, "c3")
	if !got.FirstSeen.Equal(base) || !got.LastSeen.Equal(base.Add(time.Hour)) {
		t.Errorf("first_seen = %v, last_seen = %v, want %v and %v", got.FirstSeen, got.LastSeen, base, base.Add(time.Hour))
	}

	if _, _, err := db.CreateOrUpdateTransmission(ctx, in, base.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if got := readStoredTransmission(t, db, "c3"); !got.LastSeen.Equal(base.Add(2 * time.Hour)) {
		t.Errorf("last_seen = %v, want %v", got.LastSeen, base.Add(2*time.Hour))
	}
}
