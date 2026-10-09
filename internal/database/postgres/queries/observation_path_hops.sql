-- name: InsertObservationPathHop :exec
INSERT INTO observation_path_hops (observation_id, hop_index, prefix, resolved_pubkey)
VALUES ($1, $2, $3, $4)
ON CONFLICT (observation_id, hop_index) DO UPDATE SET
    resolved_pubkey = COALESCE(EXCLUDED.resolved_pubkey, observation_path_hops.resolved_pubkey);

-- name: GetObservationPathHops :many
SELECT hop_index, prefix, resolved_pubkey
FROM observation_path_hops
WHERE observation_id = $1
ORDER BY hop_index;

-- name: ScanReachRows :many
SELECT LOWER(COALESCE(obs.pubkey, '')) AS observer_pk,
       LOWER(COALESCE(t.from_pubkey, '')) AS from_pubkey,
       COALESCE(t.payload_type, 0) AS payload_type,
       o.snr
FROM observations o
JOIN transmissions t ON t.id = o.transmission_id
JOIN observation_path_hops h ON h.observation_id = o.id
LEFT JOIN observers obs ON obs.id = o.observer_idx
WHERE o.timestamp >= $1
  AND h.prefix = ANY($2::text[])
LIMIT $3;