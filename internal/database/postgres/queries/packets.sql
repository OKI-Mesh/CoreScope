-- name: GetPacketByHash :one
SELECT
    t.id, t.raw_hex, t.hash, t.first_seen, t.route_type, t.payload_type,
    t.decoded_json, t.scope_name,
    (SELECT count(*) FROM observations WHERE transmission_id = t.id)::bigint AS observation_count,
    rep.snr, rep.rssi, rep.direction,
    obs.pubkey AS observer_id,
    obs.name AS observer_name,
    coalesce(obs.iata, '')::text AS observer_iata,
    coalesce(rep.hop_prefixes, '{}')::text[] AS hop_prefixes
FROM transmissions t
LEFT JOIN LATERAL (
    SELECT o.id, o.observer_idx, o.snr, o.rssi, o.direction, o.hop_prefixes
    FROM observations o
    WHERE o.transmission_id = t.id
    ORDER BY cardinality(o.hop_prefixes) DESC, o.id
    LIMIT 1
) rep ON true
LEFT JOIN observers obs ON obs.id = rep.observer_idx
WHERE t.hash = @hash::text;

-- name: GetPacketByID :one
SELECT
    t.id, t.raw_hex, t.hash, t.first_seen, t.route_type, t.payload_type,
    t.decoded_json, t.scope_name,
    (SELECT count(*) FROM observations WHERE transmission_id = t.id)::bigint AS observation_count,
    rep.snr, rep.rssi, rep.direction,
    obs.pubkey AS observer_id,
    obs.name AS observer_name,
    coalesce(obs.iata, '')::text AS observer_iata,
    coalesce(rep.hop_prefixes, '{}')::text[] AS hop_prefixes
FROM transmissions t
LEFT JOIN LATERAL (
    SELECT o.id, o.observer_idx, o.snr, o.rssi, o.direction, o.hop_prefixes
    FROM observations o
    WHERE o.transmission_id = t.id
    ORDER BY cardinality(o.hop_prefixes) DESC, o.id
    LIMIT 1
) rep ON true
LEFT JOIN observers obs ON obs.id = rep.observer_idx
WHERE t.id = @id::bigint;

-- name: ListObservationsForHash :many
SELECT
    o.id, o.transmission_id, o.direction, o.snr, o.rssi, o.score, o.timestamp,
    obs.pubkey AS observer_pubkey,
    obs.name AS observer_name,
    coalesce(obs.iata, '')::text AS observer_iata,
    o.hop_prefixes::text[] AS hop_prefixes,
    ARRAY(SELECT COALESCE(n.public_key, '')
          FROM unnest(o.hop_prefixes) WITH ORDINALITY AS p(prefix, ord)
          LEFT JOIN nodes n ON n.id = o.hop_nodes[p.ord::int]
          ORDER BY p.ord)::text[] AS hop_resolved
FROM transmissions t
JOIN observations o ON o.transmission_id = t.id
LEFT JOIN observers obs ON obs.id = o.observer_idx
WHERE t.hash = @hash::text
ORDER BY o.timestamp DESC, o.id;
