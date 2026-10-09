-- name: PruneNeighborEdges :execrows
DELETE FROM neighbor_edges WHERE last_seen < sqlc.arg(cutoff)::timestamptz;

-- name: LatestNeighborEdgeTime :one
SELECT COALESCE(MAX(last_seen), 'epoch'::timestamptz)::timestamptz AS latest FROM neighbor_edges;

-- name: ScanNeighborObservations :many
SELECT t.payload_type,
       (CASE WHEN t.from_pubkey IS NULL AND t.payload_type = ANY(sqlc.arg(json_types)::int[])
             THEN COALESCE(t.decoded_json, '') ELSE '' END)::text AS decoded_json,
       COALESCE(t.from_pubkey, '')::text   AS from_pubkey,
       COALESCE(obs.pubkey, '')::text      AS observer_pubkey,
       o.timestamp,
       COALESCE(fh.prefix, '')::text       AS first_hop,
       COALESCE(lh.prefix, '')::text       AS last_hop
FROM observations o
JOIN transmissions t ON t.id = o.transmission_id
LEFT JOIN observers obs ON obs.id = o.observer_idx
LEFT JOIN LATERAL (SELECT prefix FROM observation_path_hops
                   WHERE observation_id = o.id ORDER BY hop_index ASC LIMIT 1) fh ON true
LEFT JOIN LATERAL (SELECT prefix FROM observation_path_hops
                   WHERE observation_id = o.id ORDER BY hop_index DESC LIMIT 1) lh ON true
WHERE o.timestamp > sqlc.arg(since)::timestamptz
ORDER BY o.timestamp
LIMIT sqlc.arg(batch_size)::int;

-- name: UpsertNeighborEdges :execrows
WITH v AS (
    SELECT unnest(sqlc.arg(node_a)::text[])          AS node_a,
           unnest(sqlc.arg(node_b)::text[])          AS node_b,
           unnest(sqlc.arg(hits)::int[])             AS hits,
           unnest(sqlc.arg(seen_at)::timestamptz[])  AS seen_at
)
INSERT INTO neighbor_edges (node_a, node_b, count, last_seen)
SELECT node_a, node_b, hits, seen_at FROM v
ON CONFLICT (node_a, node_b) DO UPDATE SET
    count     = neighbor_edges.count + EXCLUDED.count,
    last_seen = GREATEST(neighbor_edges.last_seen, EXCLUDED.last_seen);

-- name: ListNeighborPairs :many
SELECT node_a, node_b FROM neighbor_edges;