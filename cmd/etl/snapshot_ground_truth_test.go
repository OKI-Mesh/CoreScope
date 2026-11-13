package main

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"

	"github.com/OKI-Mesh/CoreScope/internal/decoder"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

const groundTruthRows = 20000

const maxReportedMismatches = 10

// These tests read a real snapshot, which is too large to commit. They skip
// unless ETL_SNAPSHOT points at one, and fail if the snapshot has nothing to
// compare, so an empty or wrong file cannot pass silently.

func TestTransportCodes_MatchRowsTheOldBackfillPopulated(t *testing.T) {
	path := os.Getenv("ETL_SNAPSHOT")
	if path == "" {
		t.Skip("ETL_SNAPSHOT is not set")
	}
	src, err := openSource(path)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	rows, err := src.db.QueryContext(context.Background(), `SELECT id, raw_hex, route_type, code1, code2
		FROM transmissions WHERE route_type IN (0, 3) AND code1 IS NOT NULL ORDER BY id LIMIT ?`, groundTruthRows)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	compared, mismatches := 0, 0
	for rows.Next() {
		var (
			id           int64
			rawHex       string
			route        int64
			code1, code2 sql.NullString
		)
		if err := rows.Scan(&id, &rawHex, &route, &code1, &code2); err != nil {
			t.Fatal(err)
		}
		compared++

		var problem string
		frame, err := types.ParseRawFrame(rawHex)
		if err != nil {
			problem = "raw_hex: " + err.Error()
		} else if got1, got2, err := transportCodes(frame, types.RouteType(route)); err != nil {
			problem = "decode: " + err.Error()
		} else if got1 == nil || got2 == nil {
			problem = "decoded no codes"
		} else if *got1 != code1.String || *got2 != code2.String {
			problem = "decoded " + *got1 + "/" + *got2 + ", stored " + code1.String + "/" + code2.String
		}
		if problem != "" {
			mismatches++
			if mismatches <= maxReportedMismatches {
				t.Errorf("transmission %d (route %d): %s", id, route, problem)
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if compared == 0 {
		t.Fatal("the snapshot has no transport-route rows with stored codes to compare")
	}
	if mismatches > maxReportedMismatches {
		t.Errorf("%d of %d rows mismatched (first %d reported)", mismatches, compared, maxReportedMismatches)
	}
	t.Logf("compared %d rows, %d mismatched", compared, mismatches)
}

func TestAdvertPubKey_MatchesLiveDecode(t *testing.T) {
	path := os.Getenv("ETL_SNAPSHOT")
	if path == "" {
		t.Skip("ETL_SNAPSHOT is not set")
	}
	src, err := openSource(path)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	rows, err := src.db.QueryContext(context.Background(), `SELECT id, raw_hex, decoded_json
		FROM transmissions WHERE payload_type = 4 AND decoded_json IS NOT NULL ORDER BY id DESC LIMIT ?`, groundTruthRows)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	compared, undecodable, mismatches := 0, 0, 0
	for rows.Next() {
		var (
			id          int64
			rawHex      string
			decodedJSON string
		)
		if err := rows.Scan(&id, &rawHex, &decodedJSON); err != nil {
			t.Fatal(err)
		}
		live, err := decoder.DecodePacket(rawHex, nil, false)
		if err != nil {
			undecodable++
			continue
		}
		compared++
		fromLive := strings.ToLower(live.Payload.PubKey)
		fromETL := strings.ToLower(advertPubKey(&decodedJSON))
		if fromLive != fromETL {
			mismatches++
			if mismatches <= maxReportedMismatches {
				t.Errorf("transmission %d: live decode gives %q, the ETL reads %q from decoded_json", id, fromLive, fromETL)
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if compared == 0 {
		t.Fatal("the snapshot has no ADVERT rows with decoded_json to compare")
	}
	if mismatches > maxReportedMismatches {
		t.Errorf("%d of %d rows mismatched (first %d reported)", mismatches, compared, maxReportedMismatches)
	}
	t.Logf("compared %d ADVERT rows, %d mismatched, %d no longer decode", compared, mismatches, undecodable)
}
