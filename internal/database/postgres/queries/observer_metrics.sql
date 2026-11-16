-- name: CreateOrUpdateObserverMetrics :exec
INSERT INTO observer_metrics (
    observer_id, timestamp, noise_floor, tx_air_secs, rx_air_secs,
    recv_errors, battery_mv, packets_sent, packets_recv
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (observer_id, timestamp) DO UPDATE SET
    noise_floor  = EXCLUDED.noise_floor,
    tx_air_secs  = EXCLUDED.tx_air_secs,
    rx_air_secs  = EXCLUDED.rx_air_secs,
    recv_errors  = EXCLUDED.recv_errors,
    battery_mv   = EXCLUDED.battery_mv,
    packets_sent = EXCLUDED.packets_sent,
    packets_recv = EXCLUDED.packets_recv;

-- name: PruneObserverMetrics :execrows
DELETE FROM observer_metrics WHERE timestamp < sqlc.arg(cutoff);

-- name: DeleteMetricsForInactiveObservers :execrows
DELETE FROM observer_metrics
WHERE observer_id IN (SELECT pubkey FROM observers WHERE inactive = true);

-- name: ListObserverMetrics :many
SELECT m.timestamp, m.noise_floor, m.tx_air_secs, m.rx_air_secs, m.recv_errors,
       m.battery_mv, m.packets_sent, m.packets_recv
FROM observer_metrics m
WHERE m.observer_id = sqlc.arg(observer_id)
  AND (sqlc.narg(since)::timestamptz IS NULL OR m.timestamp >= sqlc.narg(since)::timestamptz)
  AND (sqlc.narg(until)::timestamptz IS NULL OR m.timestamp <= sqlc.narg(until)::timestamptz)
ORDER BY m.timestamp ASC;

-- name: ListObserverMetricsBucketed :many
-- Last sample per bucket (not MAX): if a device reboots mid-bucket, the last sample is the
-- post-reboot baseline, not the pre-reboot high-water mark. bucket is 'hour' or 'day' (UTC).
SELECT DISTINCT ON (date_trunc(sqlc.arg(bucket)::text, m.timestamp AT TIME ZONE 'UTC'))
       (date_trunc(sqlc.arg(bucket)::text, m.timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'UTC')::timestamptz AS timestamp,
       m.noise_floor, m.tx_air_secs, m.rx_air_secs, m.recv_errors,
       m.battery_mv, m.packets_sent, m.packets_recv
FROM observer_metrics m
WHERE m.observer_id = sqlc.arg(observer_id)
  AND (sqlc.narg(since)::timestamptz IS NULL OR m.timestamp >= sqlc.narg(since)::timestamptz)
  AND (sqlc.narg(until)::timestamptz IS NULL OR m.timestamp <= sqlc.narg(until)::timestamptz)
ORDER BY date_trunc(sqlc.arg(bucket)::text, m.timestamp AT TIME ZONE 'UTC'), m.timestamp DESC;

-- name: ListMetricsSummarySamples :many
SELECT
    m.observer_id,
    COALESCE(o.name, '')::text AS name,
    COALESCE(o.iata, '')::text AS iata,
    m.noise_floor,
    m.battery_mv
FROM observer_metrics m
JOIN observers o ON o.pubkey = m.observer_id
WHERE m.timestamp >= sqlc.arg(since)::timestamptz
ORDER BY m.observer_id, m.timestamp ASC;

-- name: ListMetricsSince :many
SELECT m.observer_id, o.name, o.iata, m.noise_floor, m.battery_mv
FROM observer_metrics m
JOIN observers o ON o.pubkey = m.observer_id
WHERE m.timestamp >= $1
ORDER BY m.observer_id, m.timestamp ASC;