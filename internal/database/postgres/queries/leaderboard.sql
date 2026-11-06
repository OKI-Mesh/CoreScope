-- name: ListLeaderboardSamples :many
SELECT cr.rx_pubkey,
       COALESCE(NULLIF(n.name, ''), NULLIF(co.name, ''), '')::text AS name,
       cr.lat, cr.lon, cr.heard_key
FROM client_receptions cr
LEFT JOIN nodes n ON n.public_key = cr.rx_pubkey
LEFT JOIN client_observers co ON co.pubkey = cr.rx_pubkey
WHERE cr.rx_at >= sqlc.arg(since)::timestamptz
ORDER BY cr.rx_at DESC, cr.rx_pubkey, cr.heard_key
LIMIT sqlc.arg(max_rows)::int;