package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/decoder"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/jackc/pgx/v5"
)

const encryptedChannelPrefix = "enc_"

const transmissionColumnsQuery = `SELECT t.id, t.raw_hex, t.hash, t.first_seen, t.route_type, t.payload_type,
	t.payload_version, t.decoded_json, t.from_pubkey, t.created_at, t.channel_hash, t.scope_name,
	t.last_seen, t.code1, t.code2,
	(SELECT MAX(o.timestamp) FROM observations o WHERE o.transmission_id = t.id AND o.timestamp > 0)
	FROM transmissions t ORDER BY t.id`

var transmissionColumns = []string{
	"id", "raw_hex", "hash", "first_seen", "route_type", "payload_type", "payload_version", "decoded_json",
	"created_at", "from_pubkey", "channel_hash", "channel_encrypted", "scope_name", "last_seen", "code1", "code2",
}

// LoadedTransmissions is what later stages need to know about the transmissions
// load. Transmissions keep their SQLite id, so observations.transmission_id
// needs no remapping, but SQLite never enforced that foreign key.
type LoadedTransmissions struct {
	ids []int64
}

func (l *LoadedTransmissions) Has(id int64) bool {
	i := sort.Search(len(l.ids), func(i int) bool { return l.ids[i] >= id })
	return i < len(l.ids) && l.ids[i] == id
}

// splitChannelHash turns the SQLite channel_hash into the Postgres pair: the
// bare identifier, and whether the payload could not be decrypted. SQLite
// marked an undecrypted channel by prefixing its hash with "enc_".
func splitChannelHash(stored sql.NullString) (hash *string, encrypted bool) {
	if !stored.Valid || stored.String == "" {
		return nil, false
	}
	if bare, found := strings.CutPrefix(stored.String, encryptedChannelPrefix); found {
		if bare == "" {
			return nil, false
		}
		return &bare, true
	}
	value := stored.String
	return &value, false
}

// advertPubKey reads the originating node's key out of a stored decoded_json,
// the same field the ingestor uses for an ADVERT.
func advertPubKey(decodedJSON *string) string {
	if decodedJSON == nil {
		return ""
	}
	var payload decoder.Payload
	if json.Unmarshal([]byte(*decodedJSON), &payload) != nil {
		return ""
	}
	return payload.PubKey
}

// transportCodes decodes the two transport codes of a transport-route frame.
// Any other route carries none, and so does a transport frame the decoder
// finds no codes in.
func transportCodes(frame types.RawFrame, routeType types.RouteType) (code1, code2 *string, err error) {
	if !routeType.IsTransport() {
		return nil, nil, nil
	}
	envelope, err := decoder.ParseEnvelope(frame)
	if err != nil {
		return nil, nil, err
	}
	if envelope.TransportCodes == nil {
		return nil, nil, nil
	}
	first, second := envelope.TransportCodes.Code1, envelope.TransportCodes.Code2
	return &first, &second, nil
}

func unixTime(seconds int64) *time.Time {
	t := time.Unix(seconds, 0).UTC()
	return &t
}

func loadTransmissions(ctx context.Context, src *Source, tx pgx.Tx, nodes *LoadedNodes, report *Report) (*LoadedTransmissions, error) {
	rows, err := src.db.QueryContext(ctx, transmissionColumnsQuery)
	if err != nil {
		return nil, fmt.Errorf("read transmissions: %w", err)
	}
	defer rows.Close()

	loaded := &LoadedTransmissions{}
	next := func() ([]any, error) {
		if !rows.Next() {
			return nil, rows.Err()
		}
		var (
			id                                                                    int64
			rawHex, hash                                                          string
			firstSeen, createdAt                                                  any
			route, payload, version, storedLastSeen, observedLastSeen             sql.NullInt64
			decoded, fromPubkey, channelHash, scopeName, storedCode1, storedCode2 sql.NullString
		)
		if err := rows.Scan(&id, &rawHex, &hash, &firstSeen, &route, &payload, &version, &decoded, &fromPubkey,
			&createdAt, &channelHash, &scopeName, &storedLastSeen, &storedCode1, &storedCode2, &observedLastSeen); err != nil {
			return nil, fmt.Errorf("scan transmissions: %w", err)
		}

		frame, err := types.ParseRawFrame(rawHex)
		if err != nil {
			return nil, fmt.Errorf("transmission %d (%s): %w", id, hash, err)
		}
		if decoder.ComputeContentHash(rawHex) != hash {
			report.Problem("transmissions: stored hash differs from ComputeContentHash", fmt.Sprintf("id %d hash %s", id, hash))
		}
		first := report.Time("transmissions.first_seen", firstSeen)
		if first == nil {
			return nil, fmt.Errorf("transmission %d (%s) has no usable first_seen", id, hash)
		}
		created := report.Time("transmissions.created_at", createdAt)
		if created == nil {
			created = first
		}

		routeType, payloadType, payloadVersion := int64(-1), int64(-1), int64(0)
		if route.Valid {
			routeType = route.Int64
		}
		if payload.Valid {
			payloadType = payload.Int64
		}
		if version.Valid {
			payloadVersion = version.Int64
		}
		decodedJSON := cleanText(report, "transmissions.decoded_json", fmt.Sprintf("id %d", id), ptr(decoded.String, decoded.Valid))

		key := fromPubkey.String
		if strings.TrimSpace(key) == "" && types.PayloadType(payloadType) == types.PayloadADVERT {
			if key = advertPubKey(decodedJSON); key != "" {
				report.Add("transmissions: from_pubkey taken from decoded_json", 1)
			}
		}
		reference, dropped := nodes.Reference(key)
		if dropped != "" {
			report.Add("transmissions: "+dropped, 1)
		}

		channel, encrypted := splitChannelHash(channelHash)
		if encrypted {
			report.Add("transmissions: enc_ prefix stripped, channel_encrypted set", 1)
		}

		var lastSeen *time.Time
		switch {
		case storedLastSeen.Valid && storedLastSeen.Int64 > 0:
			lastSeen = unixTime(storedLastSeen.Int64)
		case observedLastSeen.Valid:
			lastSeen = unixTime(observedLastSeen.Int64)
			report.Add("transmissions: last_seen computed from observations", 1)
		}

		code1, code2 := ptr(storedCode1.String, storedCode1.Valid), ptr(storedCode2.String, storedCode2.Valid)
		if code1 == nil {
			decoded1, decoded2, err := transportCodes(frame, types.RouteType(routeType))
			switch {
			case err != nil:
				report.Problem("transmissions: transport codes undecodable", fmt.Sprintf("id %d: %v", id, err))
			case decoded1 != nil:
				code1, code2 = decoded1, decoded2
				report.Add("transmissions: transport codes decoded from raw_hex", 1)
			}
		}

		loaded.ids = append(loaded.ids, id)
		return []any{
			id, []byte(frame), hash, first, routeType, payloadType, payloadVersion, decodedJSON,
			created, reference, channel, encrypted, cleanText(report, "transmissions.scope_name", fmt.Sprintf("id %d", id), ptr(scopeName.String, scopeName.Valid)), lastSeen, code1, code2,
		}, nil
	}

	copied, err := copyStream(ctx, tx, "transmissions", transmissionColumns, next)
	if err != nil {
		return nil, err
	}
	report.Add("transmissions: loaded", copied)
	if err := resetSequence(ctx, tx, "transmissions"); err != nil {
		return nil, err
	}
	return loaded, nil
}
