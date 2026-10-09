-- name: ListClientRfSamplesWithPrev :many
SELECT sampled_at, lat, lon, stationary, uptime_secs,
       rx_air_secs, tx_air_secs, recv, recv_errors,
       LAG(sampled_at)  OVER w AS prev_at,
       LAG(uptime_secs) OVER w AS prev_uptime,
       LAG(rx_air_secs) OVER w AS prev_rx,
       LAG(tx_air_secs) OVER w AS prev_tx,
       LAG(recv)        OVER w AS prev_recv,
       LAG(recv_errors) OVER w AS prev_errs
FROM client_rf_samples
WHERE rx_pubkey = sqlc.arg(rx_pubkey)
  AND sampled_at >= sqlc.arg(start_at)::timestamptz
  AND sampled_at <  sqlc.arg(end_at)::timestamptz
WINDOW w AS (ORDER BY sampled_at)
ORDER BY sampled_at;
