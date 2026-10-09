-- name: CreateOrUpdateObservation :one
INSERT INTO observations (transmission_id, observer_idx, direction, snr, rssi, score, path_hash, timestamp, raw_hex)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (transmission_id, observer_idx, COALESCE(path_hash, '')) DO UPDATE SET
    snr     = COALESCE(EXCLUDED.snr,     observations.snr),
    rssi    = COALESCE(EXCLUDED.rssi,    observations.rssi),
    score   = COALESCE(EXCLUDED.score,   observations.score),
    raw_hex = COALESCE(EXCLUDED.raw_hex, observations.raw_hex)
RETURNING id;