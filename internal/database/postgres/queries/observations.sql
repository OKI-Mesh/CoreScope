-- name: CreateOrUpdateObservation :one
INSERT INTO observations (transmission_id, observer_idx, direction, snr, rssi, score, path_hash, timestamp, hop_prefixes, hop_nodes)
VALUES (
    sqlc.arg(transmission_id),
    sqlc.arg(observer_idx),
    sqlc.arg(direction),
    sqlc.arg(snr),
    sqlc.arg(rssi),
    sqlc.arg(score),
    sqlc.arg(path_hash),
    sqlc.arg(timestamp),
    sqlc.arg(hop_prefixes)::text[],
    ARRAY(SELECT COALESCE(n.id, 0)
          FROM unnest(sqlc.arg(resolved_pubkeys)::text[]) WITH ORDINALITY AS r(pubkey, ord)
          LEFT JOIN nodes n ON n.public_key = r.pubkey
          ORDER BY r.ord)::int4[]
)
ON CONFLICT (transmission_id, observer_idx, COALESCE(path_hash, '')) DO UPDATE SET
    snr       = COALESCE(EXCLUDED.snr,   observations.snr),
    rssi      = COALESCE(EXCLUDED.rssi,  observations.rssi),
    score     = COALESCE(EXCLUDED.score, observations.score),
    hop_nodes = CASE WHEN cardinality(EXCLUDED.hop_nodes) > 0
                      AND cardinality(EXCLUDED.hop_nodes) = cardinality(observations.hop_prefixes)
                     THEN EXCLUDED.hop_nodes
                     ELSE observations.hop_nodes END
RETURNING id, (xmax = 0) AS inserted;

-- name: InsertObservationFrame :execrows
INSERT INTO observation_frames (observation_id, frame)
SELECT o.id, sqlc.arg(frame)::bytea
FROM observations o
JOIN transmissions t ON t.id = o.transmission_id
WHERE o.id = sqlc.arg(observation_id)::bigint
  AND t.raw_hex <> sqlc.arg(frame)::bytea
ON CONFLICT (observation_id) DO UPDATE SET frame = EXCLUDED.frame;

-- name: GetMaxObservationID :one
SELECT COALESCE(MAX(id), 0)::bigint FROM observations;

-- name: CountObservationsByObserverSince :many
-- Observations per observer since a cutoff. Observers with none are absent.
SELECT obs.pubkey, COUNT(*)::bigint AS cnt
FROM observations o
JOIN observers obs ON obs.id = o.observer_idx
WHERE o.timestamp > sqlc.arg(since)::timestamptz
GROUP BY obs.pubkey;

-- name: GetObservationCount :one
SELECT COUNT(*) FROM observations;

-- name: CountObservationsSince :one
SELECT COUNT(*) FROM observations WHERE timestamp > sqlc.arg(since)::timestamptz;

-- name: ListObservationRawHexByHash :many
SELECT o.id, f.frame
FROM transmissions t
JOIN observations o ON o.transmission_id = t.id
JOIN observation_frames f ON f.observation_id = o.id
WHERE t.hash = sqlc.arg(hash)::text;

-- name: ListObservationsByTransmissionIDs :many
SELECT o.transmission_id,
       o.id,
       COALESCE(obs.pubkey, '')::text  AS observer_id,
       COALESCE(obs.name, '')::text    AS observer_name,
       COALESCE(obs.iata, '')::text    AS observer_iata,
       COALESCE(o.direction, '')::text AS direction,
       o.snr, o.rssi, o.score, o.timestamp,
       o.hop_prefixes::text[] AS hop_prefixes,
       ARRAY(SELECT COALESCE(n.public_key, '')
             FROM unnest(o.hop_prefixes) WITH ORDINALITY AS p(prefix, ord)
             LEFT JOIN nodes n ON n.id = o.hop_nodes[p.ord::int]
             ORDER BY p.ord)::text[] AS hop_resolved
FROM observations o
LEFT JOIN observers obs ON obs.id = o.observer_idx
WHERE o.transmission_id = ANY(sqlc.arg(transmission_ids)::bigint[])
ORDER BY o.timestamp DESC, o.id DESC;

-- name: ListObserverReceptions :many
SELECT o.transmission_id,
       ob.name AS observer_name,
       o.snr,
       o.rssi,
       o.timestamp
FROM observations o
LEFT JOIN observers ob ON ob.id = o.observer_idx
WHERE o.transmission_id = ANY(sqlc.arg(transmission_ids)::bigint[])
ORDER BY o.transmission_id, o.timestamp, o.id;
