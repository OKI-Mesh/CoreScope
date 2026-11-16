-- name: PruneNeighborEdges :execrows
DELETE FROM neighbor_edges WHERE last_seen < sqlc.arg(cutoff)::timestamptz;

-- name: LatestNeighborEdgeTime :one
SELECT COALESCE(MAX(last_seen), 'epoch'::timestamptz)::timestamptz AS latest FROM neighbor_edges;

-- name: ScanNeighborObservations :many
SELECT o.id,
       t.payload_type,
       (CASE WHEN t.from_pubkey IS NULL AND t.payload_type = ANY(sqlc.arg(json_types)::int[])
             THEN COALESCE(t.decoded_json, '') ELSE '' END)::text AS decoded_json,
       COALESCE(t.from_pubkey, '')::text   AS from_pubkey,
       COALESCE(obs.pubkey, '')::text      AS observer_pubkey,
       o.timestamp,
       COALESCE(o.hop_prefixes[1], '')::text                          AS first_hop,
       COALESCE(o.hop_prefixes[cardinality(o.hop_prefixes)], '')::text AS last_hop
FROM observations o
JOIN transmissions t ON t.id = o.transmission_id
LEFT JOIN observers obs ON obs.id = o.observer_idx
WHERE (o.timestamp, o.id) > (sqlc.arg(after_timestamp)::timestamptz, sqlc.arg(after_id)::bigint)
ORDER BY o.timestamp, o.id
LIMIT sqlc.arg(batch_size)::int;

-- name: UpsertNeighborEdges :execrows
WITH v AS (
    SELECT unnest(sqlc.arg(node_a)::text[])          AS node_a,
           unnest(sqlc.arg(node_b)::text[])          AS node_b,
           unnest(sqlc.arg(hits)::int[])             AS hits,
           unnest(sqlc.arg(seen_at)::timestamptz[])  AS seen_at
),
agg AS (
    SELECT node_a, node_b,
           sum(hits)::int AS hits,
           max(seen_at)   AS seen_at
    FROM v
    GROUP BY node_a, node_b
    ORDER BY node_a, node_b -- consistent lock order across concurrent batches
)
INSERT INTO neighbor_edges (node_a, node_b, count, last_seen)
SELECT node_a, node_b, hits, seen_at FROM agg
ON CONFLICT (node_a, node_b) DO UPDATE SET
    count     = neighbor_edges.count + EXCLUDED.count,
    last_seen = GREATEST(neighbor_edges.last_seen, EXCLUDED.last_seen);

-- name: ListNeighborPairs :many
SELECT node_a, node_b FROM neighbor_edges;

-- name: HasNeighborEdges :one
SELECT EXISTS (SELECT 1 FROM neighbor_edges);

-- name: ListNeighborEdges :many
SELECT node_a, node_b, count, last_seen
FROM neighbor_edges
ORDER BY node_a, node_b;

-- name: ListNodeDegrees :many
SELECT pk::text AS pubkey, count(*)::int AS degree
FROM (
    SELECT node_a AS pk FROM neighbor_edges
    UNION ALL
    SELECT node_b FROM neighbor_edges
) AS edge_ends
GROUP BY pk;