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