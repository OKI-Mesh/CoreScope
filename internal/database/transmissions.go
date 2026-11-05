package database

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// PacketQuery holds filter params for packet listing.
type PacketQuery struct {
	Limit  int
	Offset int
	Order  string // "ASC" or "DESC"; anything else is treated as DESC

	Type  *int
	Route *int
	Hash  string
	Node  string // node name or pubkey; resolved by the DB layer
	Area  string // area key; filters by transmitting node's GPS position (store-only, ignored by the DB queries)

	Observers []string // observer pubkeys, trimmed and lowercased by the handler (was a comma-separated Observer string)
	Region    string

	// Channel is the channel_hash filter (#812). The handler strips the legacy
	// "enc_" prefix from the request value and sets ChannelEncrypted instead,
	// matching the channel_encrypted column.
	Channel          string
	ChannelEncrypted bool

	// Time bounds, parsed by the handler (see parsePacketTimeBound). RFC3339
	// input matches any observation in range (#1187 r2, inclusive); date-only
	// input compares t.first_seen strictly.
	ObsSince, ObsUntil              *time.Time
	FirstSeenAfter, FirstSeenBefore *time.Time

	ExpandObservations bool // store-only: include observation sub-maps in txToMap output
}

type GroupedPacketRow struct {
	Hash          string    `db:"hash"`
	FirstSeen     time.Time `db:"first_seen"`
	RawHex        string    `db:"raw_hex"`
	DecodedJSON   string    `db:"decoded_json"`
	PayloadType   *int      `db:"payload_type"`
	RouteType     *int      `db:"route_type"`
	ScopeName     *string   `db:"scope_name"`
	Count         int       `db:"count"`
	ObserverCount int       `db:"observer_count"`
	Latest        time.Time `db:"latest"`
	ObserverID    *string   `db:"observer_id"` // observer pubkey
	ObserverName  *string   `db:"observer_name"`
	ObserverIATA  string    `db:"observer_iata"`
	SNR           *float64  `db:"snr"`
	RSSI          *float64  `db:"rssi"`
	HopPrefixes   []string  `db:"hop_prefixes"` // of the representative observation
	DistinctIATAs []string  `db:"distinct_iatas"`
}

type MultiNodePacketParams struct {
	Pubkeys []string // already resolved, lowercase
	Limit   int
	Offset  int
	Asc     bool
	Since   *time.Time
	Until   *time.Time
}

type Packet struct {
	ID               int64
	RawHex           string
	Hash             string
	FirstSeen        time.Time
	RouteType        types.RouteType
	PayloadType      types.PayloadType
	DecodedJSON      string
	ScopeName        *string
	FromPubkey       *string
	ObservationCount int

	// Representative observation (most path hops, ties by lowest id); nil fields when the transmission has none.
	ObserverPubkey *string
	ObserverName   *string
	ObserverIATA   string
	Direction      *string
	SNR, RSSI      *float64
	Score          *float64
	ObsTimestamp   *time.Time
	HopPrefixes    []string
}

type PacketResult struct {
	Packets []Packet
	Count   int
	Limit   int
	Offset  int
}

// Transmission is a message: one row of the transmissions table.
type Transmission struct {
	ID             int64
	RawHex         string
	Hash           string
	FirstSeen      time.Time
	RouteType      types.RouteType
	PayloadType    types.PayloadType
	PayloadVersion int
	DecodedJSON    string
	ScopeName      *string
	FromPubkey     *string
}

// TransmissionSummary is a list row: the message, how many copies were heard,
// and the representative copy (most path hops, ties by lowest id; nil when
// there are none).
type TransmissionSummary struct {
	Transmission
	ObservationCount int
	Representative   *Observation
}

type TransmissionInput struct {
	RawHex           string
	Hash             string
	RouteType        types.RouteType
	PayloadType      types.PayloadType
	PayloadVersion   int
	DecodedJSON      string
	ChannelHash      string
	ScopeName        *string
	Code1            string
	Code2            string
	Advert           *NodeInput
	ChannelEncrypted bool
}

// PacketWithObservations is a packet row plus every observation of it.
type PacketWithObservations struct {
	Packet
	Observations []Observation
}

func (db *DB) CreateOrUpdateTransmission(ctx context.Context, data TransmissionInput, rxTime time.Time) (txID int64, isNew bool, err error) {
	frame, err := types.ParseRawFrame(data.RawHex)
	if err != nil {
		return 0, false, fmt.Errorf("transmission %s: %w", data.Hash, err)
	}
	err = db.inTx(ctx, func(tx *DB) error {
		var from pgtype.Text
		if data.Advert != nil {
			n := *data.Advert
			n.PublicKey = strings.ToLower(n.PublicKey)
			n.IngestNow = time.Now().UTC()
			if err := tx.CreateOrUpdateNode(ctx, n); err != nil {
				return fmt.Errorf("upsert advert node: %w", err)
			}
			from = pgtype.Text{String: n.PublicKey, Valid: true}
		}
		row, err := tx.q.InsertTransmission(ctx, pgdb.InsertTransmissionParams{
			RawHex:           frame,
			Hash:             data.Hash,
			FirstSeen:        rxTime,
			RouteType:        data.RouteType,
			PayloadType:      data.PayloadType,
			PayloadVersion:   int32(data.PayloadVersion),
			DecodedJson:      toPgText(data.DecodedJSON),
			ChannelHash:      toPgTextIfNotEmpty(data.ChannelHash),
			ScopeName:        toPgTextPtr(data.ScopeName),
			FromPubkey:       from,
			LastSeen:         pgtype.Timestamptz{Time: rxTime, Valid: true},
			Code1:            toPgTextIfNotEmpty(data.Code1),
			Code2:            toPgTextIfNotEmpty(data.Code2),
			ChannelEncrypted: data.ChannelEncrypted,
		})
		if err != nil {
			return fmt.Errorf("insert transmission: %w", err)
		}
		txID, isNew = row.ID, row.Inserted
		return nil
	})
	return txID, isNew, err
}

func (db *DB) BumpTransmissionLastSeen(ctx context.Context, txID int64, rxTime time.Time) error {
	return db.q.BumpTxLastSeen(ctx, pgdb.BumpTxLastSeenParams{
		LastSeen: pgtype.Timestamptz{Time: rxTime, Valid: true},
		ID:       txID,
	})
}

func (db *DB) PruneTransmissionsBatch(ctx context.Context, cutoff time.Time, batchSize int32) (int64, error) {
	return db.q.PruneTransmissionsBatch(ctx, pgdb.PruneTransmissionsBatchParams{
		Cutoff: cutoff, BatchSize: batchSize,
	})
}

// CountFloodAdvertsForNode returns how many FLOOD adverts pubkey originated
// strictly after since. DIRECT (zero-hop) adverts are excluded.
func (db *DB) CountFloodAdvertsForNode(ctx context.Context, pubkey string, since time.Time) (int, error) {
	n, err := db.q.CountFloodAdvertsForNode(ctx, pgdb.CountFloodAdvertsForNodeParams{
		Pubkey:      pgtype.Text{String: strings.ToLower(pubkey), Valid: true},
		PayloadType: types.PayloadADVERT,
		RouteType:   types.RouteFlood,
		Cutoff:      since,
	})
	if err != nil {
		return 0, fmt.Errorf("count flood adverts: %w", err)
	}
	return int(n), nil
}

func (db *DB) GetTransmissionIDByHash(ctx context.Context, hash string) (int64, error) {
	id, err := db.q.GetTransmissionbyHash(ctx, hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("get transmission id %q: %w", hash, err)
	}
	return id.ID, nil
}

func (db *DB) CountTransmissions(ctx context.Context) (int64, error) {
	n, err := db.q.CountTransmissions(ctx)
	if err != nil {
		return 0, fmt.Errorf("count transmissions: %w", err)
	}
	return n, nil
}

func (db *DB) QueryMultiNodePackets(ctx context.Context, p MultiNodePacketParams) ([]Packet, int64, error) {
	if len(p.Pubkeys) == 0 {
		return nil, 0, nil
	}
	if p.Limit <= 0 {
		p.Limit = 50
	}

	total, err := db.q.CountTransmissionsByFromPubkeys(ctx, pgdb.CountTransmissionsByFromPubkeysParams{
		Pubkeys: nonNil(p.Pubkeys),
		Since:   toPgTimestamptzPtr(p.Since),
		Until:   toPgTimestamptzPtr(p.Until),
	})
	if err != nil {
		return nil, 0, fmt.Errorf("count: %w", err)
	}

	// page ids (Desc or Asc query), then hydrate with the shared by-ids query
	ids, err := db.pageIDs(ctx, p)
	if err != nil {
		return nil, 0, err
	}
	rows, err := db.listPacketsByIDs(ctx, ids) // keeps the page's order
	if err != nil {
		return nil, 0, err
	}
	return rows, total, nil
}

func (db *DB) pageIDs(ctx context.Context, p MultiNodePacketParams) ([]int64, error) {
	if p.Asc {
		return db.q.PageTransmissionIDsByFromPubkeysAsc(ctx, pgdb.PageTransmissionIDsByFromPubkeysAscParams{
			Pubkeys:   nonNil(p.Pubkeys),
			Since:     toPgTimestamptzPtr(p.Since),
			Until:     toPgTimestamptzPtr(p.Until),
			RowLimit:  int32(p.Limit),
			RowOffset: int32(p.Offset),
		})
	}
	return db.q.PageTransmissionIDsByFromPubkeysDesc(ctx, pgdb.PageTransmissionIDsByFromPubkeysDescParams{
		Pubkeys:   nonNil(p.Pubkeys),
		Since:     toPgTimestamptzPtr(p.Since),
		Until:     toPgTimestamptzPtr(p.Until),
		RowLimit:  int32(p.Limit),
		RowOffset: int32(p.Offset),
	})
}

// listPacketsByIDs hydrates ids into packets, preserving the order of ids.
func (db *DB) listPacketsByIDs(ctx context.Context, ids []int64) ([]Packet, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := db.q.ListPacketsByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	out := make([]Packet, len(rows))
	for i, r := range rows {
		out[i] = Packet{
			ID:               r.ID,
			RawHex:           types.RawFrame(r.RawHex).Hex(),
			Hash:             r.Hash,
			FirstSeen:        r.FirstSeen.UTC(),
			RouteType:        r.RouteType,
			PayloadType:      r.PayloadType,
			DecodedJSON:      r.DecodedJson,
			ScopeName:        fromPgText(r.ScopeName),
			FromPubkey:       fromPgText(r.FromPubkey),
			ObservationCount: int(r.ObservationCount),
			ObserverPubkey:   fromPgText(r.ObserverID),
			ObserverName:     fromPgText(r.ObserverName),
			ObserverIATA:     r.ObserverIata,
			Direction:        fromPgText(r.Direction),
			SNR:              fromPgFloat8(r.Snr),
			RSSI:             fromPgFloat8(r.Rssi),
			Score:            fromPgFloat8(r.Score),
			ObsTimestamp:     utcPtr(fromPgTimestamptz(r.ObsTimestamp)),
			HopPrefixes:      r.HopPrefixes,
		}
	}
	return out, nil
}

func (q *PacketQuery) normalize() (order string) {
	if q.Limit <= 0 {
		q.Limit = 50
	}
	if strings.EqualFold(q.Order, "ASC") {
		return "ASC"
	}
	return "DESC" // default, and the only other legal value
}

// transmissionWhere resolves q.Node and builds the WHERE clause text and args.
func (db *DB) transmissionWhere(ctx context.Context, q PacketQuery) (clause string, args []any, err error) {
	nodePK := ""
	if q.Node != "" {
		nodePK, err = db.ResolveNodePubkey(ctx, q.Node)
		if err != nil {
			return "", nil, fmt.Errorf("resolve node %q: %w", q.Node, err)
		}
	}
	where, args := buildTransmissionWhere(q, nodePK)
	if len(where) > 0 {
		clause = "WHERE " + strings.Join(where, " AND ")
	}
	return clause, args, nil
}

func (db *DB) countTransmissionsWhere(ctx context.Context, clause string, args []any) (int64, error) {
	if clause == "" {
		return db.CountTransmissions(ctx) // the sqlc count
	}
	var total int64
	err := db.pool.QueryRow(ctx, "SELECT COUNT(*) FROM transmissions t "+clause, args...).Scan(&total)
	return total, err
}

func (db *DB) QueryPackets(ctx context.Context, q PacketQuery) ([]Packet, int64, error) {
	order := q.normalize()
	clause, args, err := db.transmissionWhere(ctx, q)
	if err != nil {
		return nil, 0, err
	}

	total, err := db.countTransmissionsWhere(ctx, clause, args)
	if err != nil {
		return nil, 0, fmt.Errorf("count packets: %w", err)
	}

	// #1345: order by ingest id, NOT first_seen. PR #1233 made first_seen=rxTime,
	// so buffered-then-uploaded observer packets with hours-old rxTime were
	// sorting to the top/middle and hiding fresh ingest. Ordering by id keeps
	// "latest activity" semantically equal to "what we ingested last".
	idSQL := fmt.Sprintf("SELECT t.id FROM transmissions t %s ORDER BY t.id %s LIMIT $%d OFFSET $%d",
		clause, order, len(args)+1, len(args)+2)
	rows, err := db.pool.Query(ctx, idSQL, append(args, q.Limit, q.Offset)...)
	if err != nil {
		return nil, 0, fmt.Errorf("page packet ids: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return nil, 0, fmt.Errorf("scan packet ids: %w", err)
	}

	packets, err := db.listPacketsByIDs(ctx, ids)
	if err != nil {
		return nil, 0, fmt.Errorf("hydrate packets: %w", err)
	}
	return packets, total, nil
}

func (db *DB) QueryGroupedPackets(ctx context.Context, q PacketQuery) ([]GroupedPacketRow, int64, error) {
	q.normalize()
	clause, args, err := db.transmissionWhere(ctx, q)
	if err != nil {
		return nil, 0, err
	}

	total, err := db.countTransmissionsWhere(ctx, clause, args)
	if err != nil {
		return nil, 0, fmt.Errorf("count grouped packets: %w", err)
	}

	querySQL := fmt.Sprintf(`
SELECT t.hash, t.first_seen, upper(encode(t.raw_hex, 'hex'))::text AS raw_hex,
       COALESCE(t.decoded_json, '')::text AS decoded_json,
       t.payload_type, t.route_type, t.scope_name,
       (SELECT COUNT(*) FROM observations oi WHERE oi.transmission_id = t.id)::int AS count,
       (SELECT COUNT(DISTINCT oi.observer_idx) FROM observations oi WHERE oi.transmission_id = t.id)::int AS observer_count,
       COALESCE(t.last_seen, t.first_seen) AS latest,
       obs.pubkey AS observer_id, obs.name AS observer_name,
       COALESCE(obs.iata, '')::text AS observer_iata,
       o.snr, o.rssi,
       COALESCE(o.hop_prefixes, '{}')::text[] AS hop_prefixes,
       COALESCE((SELECT array_agg(DISTINCT TRIM(obi.iata) ORDER BY TRIM(obi.iata))
               FROM observations oi JOIN observers obi ON obi.id = oi.observer_idx
              WHERE oi.transmission_id = t.id
                AND obi.iata IS NOT NULL AND TRIM(obi.iata) <> ''), '{}')::text[] AS distinct_iatas
FROM transmissions t
LEFT JOIN LATERAL (
    SELECT oo.id, oo.observer_idx, oo.snr, oo.rssi, oo.hop_prefixes
    FROM observations oo
    WHERE oo.transmission_id = t.id
    ORDER BY cardinality(oo.hop_prefixes) DESC, oo.id ASC
    LIMIT 1
) o ON true
LEFT JOIN observers obs ON obs.id = o.observer_idx
%s
ORDER BY COALESCE(t.last_seen, t.first_seen) DESC, t.id DESC
LIMIT $%d OFFSET $%d`, clause, len(args)+1, len(args)+2)

	rows, err := db.pool.Query(ctx, querySQL, append(args, q.Limit, q.Offset)...)
	if err != nil {
		return nil, 0, fmt.Errorf("query grouped packets: %w", err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowToStructByName[GroupedPacketRow])
	if err != nil {
		return nil, 0, fmt.Errorf("scan grouped packets: %w", err)
	}
	return out, total, nil
}

// packetWhere accumulates WHERE clauses and numbered args. Every clause takes
// exactly one argument, written as "?" and rewritten to $n.
type packetWhere struct {
	clauses []string
	args    []any
}

func (w *packetWhere) add(clause string, arg any) {
	w.args = append(w.args, arg)
	w.clauses = append(w.clauses, strings.Replace(clause, "?", fmt.Sprintf("$%d", len(w.args)), 1))
}

// buildTransmissionWhere builds WHERE clauses for transmission-centric queries.
// Uses t. prefix for transmission columns and EXISTS subqueries for observation filters.
// nodePubkey is the already-resolved pubkey for q.Node (resolved by the caller,
// which owns the DB). Area and ExpandObservations are store-only and ignored here.
func buildTransmissionWhere(q PacketQuery, nodePubkey string) ([]string, []any) {
	var w packetWhere

	if q.Type != nil {
		w.add("t.payload_type = ?", *q.Type)
	}
	if q.Route != nil {
		w.add("t.route_type = ?", *q.Route)
	}
	if q.Hash != "" {
		w.add("t.hash = ?", strings.ToLower(q.Hash))
	}

	// RFC3339 since/until match any observation in range, so re-observed
	// packets (t.first_seen older than the window but with observations
	// inside it) are still included (PR #1187 r2). Inclusive.
	if q.ObsSince != nil {
		w.add("EXISTS (SELECT 1 FROM observations oi WHERE oi.transmission_id = t.id AND oi.timestamp >= ?)", *q.ObsSince)
	}
	if q.ObsUntil != nil {
		w.add("EXISTS (SELECT 1 FROM observations oi WHERE oi.transmission_id = t.id AND oi.timestamp <= ?)", *q.ObsUntil)
	}
	// Date-only since/until compare t.first_seen, strictly.
	if q.FirstSeenAfter != nil {
		w.add("t.first_seen > ?", *q.FirstSeenAfter)
	}
	if q.FirstSeenBefore != nil {
		w.add("t.first_seen < ?", *q.FirstSeenBefore)
	}

	if q.Node != "" {
		// #1143: exact-match on the dedicated from_pubkey column instead of
		// LIKE-on-JSON substring (adversarial spoof + same-name false positives).
		w.add("t.from_pubkey = ?", nodePubkey)
	}
	if q.Channel != "" {
		// channel_hash is indexed for payload_type = 5; exact match. The
		// encrypted flag replaces the old "enc_" prefix on the value.
		w.add("t.channel_hash = ?", q.Channel)
		w.add("t.channel_encrypted = ?", q.ChannelEncrypted)
	}
	if len(q.Observers) > 0 {
		// observers.pubkey is stored lowercase; the handler lowercases the input.
		w.add("EXISTS (SELECT 1 FROM observations oi JOIN observers obi ON obi.id = oi.observer_idx "+
			"WHERE oi.transmission_id = t.id AND obi.pubkey = ANY(?::text[]))", q.Observers)
	}
	if q.Region != "" {
		w.add("EXISTS (SELECT 1 FROM observations oi JOIN observers obi ON obi.id = oi.observer_idx "+
			"WHERE oi.transmission_id = t.id AND obi.iata = ?)", q.Region)
	}
	return w.clauses, w.args
}

// GetMaxTransmissionID returns the highest transmission id, or 0 if there are none.
func (db *DB) GetMaxTransmissionID(ctx context.Context) (int64, error) {
	return db.q.GetMaxTransmissionID(ctx)
}

// GetNewTransmissionsSince returns transmissions with id > lastID, oldest first,
// for WebSocket polling. limit <= 0 defaults to 100.
func (db *DB) GetNewTransmissionsSince(ctx context.Context, lastID int64, limit int) ([]NewTransmission, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := db.q.ListTransmissionsAfterID(ctx, pgdb.ListTransmissionsAfterIDParams{
		AfterID:  lastID,
		RowLimit: int32(limit),
	})
	if err != nil {
		return nil, fmt.Errorf("list transmissions after id %d: %w", lastID, err)
	}

	out := make([]NewTransmission, 0, len(rows))
	for _, r := range rows {
		rawHex := types.RawFrame(r.RawHex).Hex()
		out = append(out, NewTransmission{
			ID:             r.ID,
			RawHex:         &rawHex,
			Hash:           &r.Hash,
			FirstSeen:      &r.FirstSeen,
			RouteType:      &r.RouteType,
			PayloadType:    &r.PayloadType,
			PayloadVersion: &r.PayloadVersion,
			DecodedJSON:    &r.DecodedJson,
		})
	}
	return out, nil
}

// GetRecentTransmissionsForNode returns recent transmissions originated by a
// node, identified by exact pubkey match on the indexed from_pubkey column
// (#1143). The legacy `name` substring fallback was removed: it produced
// same-name false positives and an adversarial spoof path where any node
// could attribute its transmissions to a victim by naming itself with the
// victim's pubkey. Pubkey is unique by design — that's the whole point.
func (db *DB) GetRecentTransmissionsForNode(ctx context.Context, pubkey string, limit int) ([]Packet, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, _, err := db.QueryMultiNodePackets(ctx, MultiNodePacketParams{
		Pubkeys: []string{strings.ToLower(pubkey)},
		Limit:   limit,
	})
	return rows, err
}

// UnmatchedTransmission is a transport-scoped transmission that no configured
// region key matched. RawHex is empty when the column is NULL.
type UnmatchedTransmission struct {
	ID     int64
	RawHex string
}

// ListUnmatchedTransmissions returns up to limit transmissions with an empty
// scope_name at or after since, newest first.
func (db *DB) ListUnmatchedTransmissions(ctx context.Context, since time.Time, routeTypes []int, limit int) ([]UnmatchedTransmission, error) {
	rt := make([]int32, len(routeTypes))
	for i, v := range routeTypes {
		rt[i] = int32(v)
	}
	rows, err := db.q.ListUnmatchedTransmissions(ctx, pgdb.ListUnmatchedTransmissionsParams{
		Since:      since,
		RouteTypes: rt,
		RowLimit:   int32(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]UnmatchedTransmission, 0, len(rows))
	for _, r := range rows {
		out = append(out, UnmatchedTransmission{ID: r.ID, RawHex: types.RawFrame(r.RawHex).Hex()})
	}
	return out, nil
}

// pathHops pairs the two parallel hop arrays; "" in resolved means unresolved.
func pathHops(prefixes, resolved []string) []PathHop {
	if len(prefixes) == 0 {
		return nil
	}
	hops := make([]PathHop, len(prefixes))
	for i, p := range prefixes {
		hops[i] = PathHop{Prefix: p}
		if i < len(resolved) && resolved[i] != "" {
			r := resolved[i]
			hops[i].ResolvedPubkey = &r
		}
	}
	return hops
}

func (db *DB) NthNewestTransmissionID(ctx context.Context, since *time.Time, n int64) (int64, error) {
	var pgSince pgtype.Timestamptz
	if since != nil {
		pgSince = pgtype.Timestamptz{Time: *since, Valid: true}
	}
	id, err := db.q.NthNewestTransmissionID(ctx, pgdb.NthNewestTransmissionIDParams{
		Since: pgSince,
		Skip:  n - 1,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("nth newest transmission id: %w", err)
	}
	return id, nil
}
