-- name: GetTransmissionbyHash :one
SELECT id, first_seen FROM transmissions WHERE hash = $1;

-- name: InsertTransmission :one
INSERT INTO transmissions (
    raw_hex, hash, first_seen, route_type, payload_type, payload_version,
    decoded_json, channel_hash, scope_name, from_pubkey, last_seen, code1, code2
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
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
),
del_hops AS (
    DELETE FROM observation_path_hops
    WHERE observation_id IN (
        SELECT o.id FROM observations o JOIN batch b ON o.transmission_id = b.id)
),
del_obs AS (
    DELETE FROM observations WHERE transmission_id IN (SELECT id FROM batch)
)
DELETE FROM transmissions WHERE id IN (SELECT id FROM batch);