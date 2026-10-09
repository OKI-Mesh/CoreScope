-- name: InsertDroppedPacket :exec
INSERT INTO dropped_packets (
    hash, raw_hex, reason, observer_id, observer_name, node_pubkey, node_name
)
VALUES (
    sqlc.narg(hash), sqlc.narg(raw_hex), sqlc.arg(reason),
    sqlc.narg(observer_id), sqlc.narg(observer_name),
    sqlc.narg(node_pubkey), sqlc.narg(node_name)
);

-- name: PruneDroppedPackets :execrows
DELETE FROM dropped_packets WHERE dropped_at < sqlc.arg(cutoff)::timestamptz;