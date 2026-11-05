-- name: ListGroupTextTransmissions :many
SELECT id, hash, raw_hex
FROM transmissions
WHERE payload_type = sqlc.arg(payload_type)
  AND id > sqlc.arg(after_id)
ORDER BY id
LIMIT sqlc.arg(row_limit);

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