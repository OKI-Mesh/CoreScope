-- name: CreateOrUpdateClientObserver :exec
INSERT INTO client_observers (pubkey, name, last_seen)
VALUES (sqlc.arg(pubkey), sqlc.arg(name), sqlc.arg(last_seen))
ON CONFLICT (pubkey) DO UPDATE SET name = EXCLUDED.name, last_seen = EXCLUDED.last_seen;

-- name: InsertClientReception :execrows
INSERT INTO client_receptions (
    rx_pubkey, heard_key, heard_keylen, rssi, snr, lat, lon, pos_acc_m,
    rx_at, ingested_at, src)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (rx_pubkey, heard_key, rx_at) DO NOTHING;

-- name: InsertClientRfSample :execrows
INSERT INTO client_rf_samples (
    rx_pubkey, sampled_at, ingested_at, lat, lon, pos_acc_m, stationary,
    uptime_secs, battery_mv, queue_len, errors, noise_floor, last_rssi,
    last_snr, tx_air_secs, rx_air_secs, recv, sent, flood_rx, direct_rx,
    flood_tx, direct_tx, recv_errors)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
        $16, $17, $18, $19, $20, $21, $22, $23)
ON CONFLICT (rx_pubkey, sampled_at) DO NOTHING;

-- name: InsertClientRxObservation :execrows
INSERT INTO client_rx_observations (
    rx_pubkey, rx_at, ingested_at, pkt_hash, route_type, payload_type,
    code1, code2, scope_name, hash_size, hop_count, path_json, forwarder,
    snr, rssi, lat, lon, pos_acc_m)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
ON CONFLICT (rx_pubkey, pkt_hash, rx_at) DO NOTHING;

-- name: InsertClientDeclaredRegions :execrows
INSERT INTO node_declared_regions (
    target, rx_pubkey, observed_at, ingested_at, regions_csv, truncated,
    lat, lon, pos_acc_m, repeater_clock)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (target, rx_pubkey, observed_at) DO NOTHING;

-- name: GetCurrentDeclaredRegions :one
SELECT target, rx_pubkey, observed_at, ingested_at, regions_csv, truncated,
       lat, lon, pos_acc_m, repeater_clock
FROM node_declared_regions
WHERE target = $1
ORDER BY observed_at DESC
LIMIT 1;

-- name: PruneClientReceptions :execrows
DELETE FROM client_receptions WHERE rx_at < sqlc.arg(cutoff)::timestamptz;

-- name: PruneClientObservers :execrows
DELETE FROM client_observers WHERE last_seen < sqlc.arg(cutoff)::timestamptz;

-- name: PruneClientRxObservations :execrows
DELETE FROM client_rx_observations WHERE rx_at < sqlc.arg(cutoff)::timestamptz;

-- name: PruneDeclaredRegions :execrows
DELETE FROM node_declared_regions WHERE observed_at < sqlc.arg(cutoff)::timestamptz;

-- name: GetClientRxObservationsCount :one
SELECT COUNT(*) FROM client_rx_observations;

-- name: PruneClientRfSamples :execrows
DELETE FROM client_rf_samples WHERE sampled_at < sqlc.arg(cutoff)::timestamptz;

-- name: ListCoverageRows :many
SELECT lat, lon, snr, rssi, heard_key, rx_at
FROM client_receptions
WHERE heard_key = ANY(sqlc.arg(heard_keys)::text[])
  AND lat BETWEEN sqlc.arg(min_lat) AND sqlc.arg(max_lat)
  AND lon BETWEEN sqlc.arg(min_lon) AND sqlc.arg(max_lon)
  AND (sqlc.narg(rx_pubkey)::text IS NULL OR rx_pubkey = sqlc.narg(rx_pubkey)::text)
  AND (sqlc.narg(since)::timestamptz IS NULL OR rx_at >= sqlc.narg(since)::timestamptz);

-- name: ListCoverageRowsAnyNode :many
SELECT lat, lon, snr, rssi, heard_key, rx_at
FROM client_receptions
WHERE lat BETWEEN sqlc.arg(min_lat) AND sqlc.arg(max_lat)
  AND lon BETWEEN sqlc.arg(min_lon) AND sqlc.arg(max_lon)
  AND (sqlc.narg(rx_pubkey)::text IS NULL OR rx_pubkey = sqlc.narg(rx_pubkey)::text)
  AND (sqlc.narg(since)::timestamptz IS NULL OR rx_at >= sqlc.narg(since)::timestamptz);

-- name: CountMobileReceptions :one
SELECT COUNT(*)::bigint AS receptions,
       COUNT(DISTINCT rx_pubkey)::bigint AS clients
FROM client_receptions
WHERE heard_key = ANY(sqlc.arg(heard_keys)::text[]);

-- name: ListClientRfSamplesWithPrev :many
WITH s AS (
    SELECT sampled_at, lat, lon, stationary, uptime_secs,
           rx_air_secs, tx_air_secs, recv, recv_errors,
           LAG(sampled_at) OVER (ORDER BY sampled_at) AS prev_sampled_at
    FROM client_rf_samples
    WHERE rx_pubkey = sqlc.arg(rx_pubkey)
      AND sampled_at >= sqlc.arg(start_at)::timestamptz
      AND sampled_at <  sqlc.arg(end_at)::timestamptz
)
SELECT s.sampled_at, s.lat, s.lon, s.stationary, s.uptime_secs,
       s.rx_air_secs, s.tx_air_secs, s.recv, s.recv_errors,
       p.sampled_at  AS prev_at,
       p.uptime_secs AS prev_uptime,
       p.rx_air_secs AS prev_rx,
       p.tx_air_secs AS prev_tx,
       p.recv        AS prev_recv,
       p.recv_errors AS prev_errs
FROM s
LEFT JOIN client_rf_samples p
       ON p.rx_pubkey = sqlc.arg(rx_pubkey)
      AND p.sampled_at = s.prev_sampled_at
ORDER BY s.sampled_at;
