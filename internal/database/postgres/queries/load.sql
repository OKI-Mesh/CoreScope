-- Observations newer than after_id, oldest first. Catches observations that
-- arrive after their transmission was already ingested (#174).
-- name: ListObservationsAfterID :many
SELECT o.id,
       o.transmission_id,
       COALESCE(obs.pubkey, '')::text AS observer_id,
       COALESCE(obs.name, '')::text   AS observer_name,
       COALESCE(obs.iata, '')::text   AS observer_iata,
       COALESCE(o.direction, '')::text AS direction,
       o.snr, o.rssi, o.score, o.timestamp,
       o.hop_prefixes::text[] AS hop_prefixes,
       ARRAY(SELECT COALESCE(n.public_key, '')
             FROM unnest(o.hop_prefixes) WITH ORDINALITY AS p(prefix, ord)
             LEFT JOIN nodes n ON n.id = o.hop_nodes[p.ord::int]
             ORDER BY p.ord)::text[] AS hop_resolved
FROM observations o
LEFT JOIN observers obs ON obs.id = o.observer_idx
WHERE o.id > @after_id
ORDER BY o.id ASC
LIMIT @row_limit;