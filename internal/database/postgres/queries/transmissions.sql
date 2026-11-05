-- name: GetTransmissionbyHash :one
SELECT id, first_seen FROM transmissions WHERE hash = $1;

-- name: InsertTransmission :one
-- name: InsertTransmission :one
INSERT INTO transmissions (
    raw_hex, hash, first_seen, route_type, payload_type, payload_version,
    decoded_json, channel_hash, scope_name, from_pubkey, last_seen, code1, code2, channel_encrypted
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
)
ON CONFLICT (hash) DO UPDATE SET
    first_seen = LEAST(transmissions.first_seen, EXCLUDED.first_seen)
RETURNING id, (xmax = 0) AS inserted;

-- name: UpdateTransmissionFirstSeen :one
UPDATE transmissions SET first_seen = $1 WHERE id = $2 RETURNING id;

-- name: BumpTxLastSeen :exec
UPDATE transmissions
SET last_seen = $1
WHERE id = $2 AND (last_seen IS NULL OR last_seen < $1);

-- name: PruneTransmissionsBatch :execrows
WITH batch AS (
    SELECT id FROM transmissions
    WHERE first_seen < sqlc.arg(cutoff)::timestamptz
    ORDER BY first_seen, id
    LIMIT sqlc.arg(batch_size)::int
)
DELETE FROM transmissions WHERE id IN (SELECT id FROM batch);

-- name: CountFloodAdvertsForNode :one
SELECT count(*)::bigint
FROM transmissions
WHERE from_pubkey   = sqlc.arg(pubkey)
  AND payload_type  = sqlc.arg(payload_type)
  AND route_type    = sqlc.arg(route_type)
  AND first_seen    > sqlc.arg(cutoff);

-- name: GetTransmissionIDByHash :one
SELECT id FROM transmissions WHERE hash = $1;

-- name: CountTransmissions :one
SELECT COUNT(*)::bigint FROM transmissions;

-- name: CountTransmissionsSince :one
-- since NULL = no retention filter.
SELECT COUNT(*)::bigint
FROM transmissions t2
WHERE (sqlc.narg(since)::timestamptz IS NULL OR t2.last_seen >= sqlc.narg(since)::timestamptz);

-- name: ListTransmissionsChunk :many
SELECT id, hash, raw_hex, first_seen, route_type, payload_type,
       payload_version, decoded_json, scope_name
FROM transmissions
WHERE id > sqlc.arg(after_id)
  AND (sqlc.narg(since)::timestamptz IS NULL OR COALESCE(last_seen, first_seen) >= sqlc.narg(since)::timestamptz)
  AND (sqlc.narg(until)::timestamptz IS NULL OR COALESCE(last_seen, first_seen) <  sqlc.narg(until)::timestamptz)
ORDER BY id ASC
LIMIT sqlc.arg(row_limit)::int;

-- name: CountTransmissionsByFromPubkeys :one
SELECT COUNT(*)::bigint
FROM transmissions t
WHERE t.from_pubkey = ANY(sqlc.arg(pubkeys)::text[])
  AND (sqlc.narg(since)::timestamptz IS NULL OR t.first_seen >= sqlc.narg(since)::timestamptz)
  AND (sqlc.narg(until)::timestamptz IS NULL OR t.first_seen <= sqlc.narg(until)::timestamptz);

-- name: PageTransmissionIDsByFromPubkeysDesc :many
SELECT t.id
FROM transmissions t
WHERE t.from_pubkey = ANY(sqlc.arg(pubkeys)::text[])
  AND (sqlc.narg(since)::timestamptz IS NULL OR t.first_seen >= sqlc.narg(since)::timestamptz)
  AND (sqlc.narg(until)::timestamptz IS NULL OR t.first_seen <= sqlc.narg(until)::timestamptz)
ORDER BY t.id DESC
LIMIT sqlc.arg(row_limit)::int OFFSET sqlc.arg(row_offset)::int;

-- name: PageTransmissionIDsByFromPubkeysAsc :many
SELECT t.id
FROM transmissions t
WHERE t.from_pubkey = ANY(sqlc.arg(pubkeys)::text[])
  AND (sqlc.narg(since)::timestamptz IS NULL OR t.first_seen >= sqlc.narg(since)::timestamptz)
  AND (sqlc.narg(until)::timestamptz IS NULL OR t.first_seen <= sqlc.narg(until)::timestamptz)
ORDER BY t.id ASC
LIMIT sqlc.arg(row_limit)::int OFFSET sqlc.arg(row_offset)::int;

-- name: ListPacketsByIDs :many
SELECT
    t.id,
    t.raw_hex,
    t.hash,
    t.first_seen,
    t.route_type,
    t.payload_type,
    t.decoded_json::text AS decoded_json,
    t.from_pubkey,
    (SELECT COUNT(*) FROM observations oc WHERE oc.transmission_id = t.id)::bigint AS observation_count,
    obs.pubkey                      AS observer_id,
    obs.name                        AS observer_name,
    COALESCE(obs.iata, '')::text    AS observer_iata,
    rep.id                          AS observation_id,
    rep.snr,
    rep.rssi,
    rep.score,
    rep.direction,
    rep.timestamp AS obs_timestamp,
    t.scope_name,
    COALESCE(rep.hop_prefixes, '{}')::text[] AS hop_prefixes
FROM transmissions t
LEFT JOIN observations rep ON rep.id = (
    SELECT o.id
    FROM observations o
    WHERE o.transmission_id = t.id
    ORDER BY cardinality(o.hop_prefixes) DESC,
             o.id ASC
    LIMIT 1
)
LEFT JOIN observers obs ON obs.id = rep.observer_idx
WHERE t.id = ANY(@ids::bigint[])
ORDER BY array_position(@ids::bigint[], t.id);

-- name: GetMaxTransmissionID :one
SELECT COALESCE(MAX(id), 0)::bigint FROM transmissions;

-- name: ListTransmissionsAfterID :many
SELECT
    id,
    raw_hex,
    hash,
    first_seen,
    route_type,
    payload_type,
    payload_version,
    decoded_json::text AS decoded_json
FROM transmissions
WHERE id > @after_id
ORDER BY id ASC
LIMIT @row_limit;

-- name: ListUnmatchedTransmissions :many
SELECT t.id, t.raw_hex
FROM transmissions t
WHERE t.first_seen >= sqlc.arg(since)
  AND t.scope_name = ''
  AND t.route_type = ANY(sqlc.arg(route_types)::int[])
ORDER BY t.first_seen DESC, t.id DESC
LIMIT sqlc.arg(row_limit)::int;

-- name: NthNewestTransmissionID :one
SELECT id
FROM transmissions
WHERE (sqlc.narg(since)::timestamptz IS NULL OR COALESCE(last_seen, first_seen) >= sqlc.narg(since)::timestamptz)
ORDER BY id DESC
OFFSET sqlc.arg(skip)::bigint
LIMIT 1;

-- name: CountFloodAdverts :one
SELECT COUNT(*)::bigint
FROM (
    SELECT 1
    FROM transmissions
    WHERE from_pubkey = sqlc.arg(pubkey)
      AND payload_type = sqlc.arg(payload_type)
      AND route_type = sqlc.arg(route_type)
      AND first_seen > sqlc.arg(since)::timestamptz
    LIMIT sqlc.arg(row_cap)::int
) AS capped;
