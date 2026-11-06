-- name: ListObservedPathsThrough :many
SELECT coalesce(obs.pubkey, '')::text       AS observer_pk,
       coalesce(t.from_pubkey, '')::text    AS from_pubkey,
       t.payload_type,
       o.hop_prefixes::text[]               AS path,
       o.snr
FROM observations o
JOIN transmissions t ON t.id = o.transmission_id
LEFT JOIN observers obs ON obs.id = o.observer_idx
WHERE o.timestamp >= sqlc.arg(since)::timestamptz
  AND o.hop_prefixes && sqlc.arg(tokens)::text[]
ORDER BY o.timestamp DESC, o.id DESC
LIMIT sqlc.arg(row_limit)::int;

-- name: ListTransmissionIDsWithResolvedPubkey :many
SELECT DISTINCT o.transmission_id
FROM observations o
WHERE o.transmission_id = ANY(@transmission_ids::bigint[])
  AND (SELECT n.id FROM nodes n WHERE n.public_key = @pubkey::text) = ANY(o.hop_nodes);


-- name: ListBestResolvedPaths :many
SELECT DISTINCT ON (o.transmission_id)
       o.transmission_id,
       ARRAY(SELECT COALESCE(n.public_key, '')
             FROM unnest(o.hop_prefixes) WITH ORDINALITY AS p(prefix, ord)
             LEFT JOIN nodes n ON n.id = o.hop_nodes[p.ord::int]
             ORDER BY p.ord)::text[] AS resolved
FROM observations o
WHERE o.transmission_id = ANY(@transmission_ids::bigint[])
  AND EXISTS (SELECT 1 FROM unnest(o.hop_nodes) AS x(node_id) WHERE x.node_id > 0)
ORDER BY o.transmission_id, cardinality(o.hop_prefixes) DESC, o.id;

-- name: ListResolvedPathsForObservations :many
SELECT o.id AS observation_id,
       ARRAY(SELECT COALESCE(n.public_key, '')
             FROM unnest(o.hop_prefixes) WITH ORDINALITY AS p(prefix, ord)
             LEFT JOIN nodes n ON n.id = o.hop_nodes[p.ord::int]
             ORDER BY p.ord)::text[] AS resolved
FROM observations o
WHERE o.id = ANY(@observation_ids::bigint[])
  AND EXISTS (SELECT 1 FROM unnest(o.hop_nodes) AS x(node_id) WHERE x.node_id > 0);

-- name: ListResolvedPubkeysForTransmissions :many
SELECT o.transmission_id,
       array_agg(DISTINCT n.public_key ORDER BY n.public_key)::text[] AS pubkeys
FROM observations o
CROSS JOIN LATERAL unnest(o.hop_nodes) AS h(node_id)
JOIN nodes n ON n.id = h.node_id
WHERE o.transmission_id = ANY(@transmission_ids::bigint[])
GROUP BY o.transmission_id;