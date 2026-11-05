-- name: CountChannelMessages :one
SELECT COUNT(*)::bigint
FROM transmissions t
WHERE t.channel_hash = sqlc.arg(channel_hash)::text
  AND t.payload_type = 5
  AND t.channel_encrypted = sqlc.arg(encrypted)::bool
  AND (
    COALESCE(cardinality(sqlc.arg(regions)::text[]), 0) = 0
    OR EXISTS (
      SELECT 1
      FROM observations o
      JOIN observers obs ON obs.id = o.observer_idx
      WHERE o.transmission_id = t.id
        AND upper(btrim(obs.iata)) = ANY(sqlc.arg(regions)::text[])
    )
  );

-- name: ListChannelMessages :many
-- Page = newest LIMIT messages by latest observation time (issue #1366), then
-- emitted oldest-first (newest LAST) to match the in-memory path's contract.
WITH page AS (
  SELECT t.id, a.latest_seen
  FROM transmissions t
  CROSS JOIN LATERAL (
    SELECT MAX(o.timestamp)::timestamptz AS latest_seen
    FROM observations o
    WHERE o.transmission_id = t.id
  ) a
  WHERE t.channel_hash = sqlc.arg(channel_hash)::text
    AND t.channel_encrypted = sqlc.arg(encrypted)::bool
    AND t.payload_type = 5
    AND t.decoded_json IS NOT NULL
    AND a.latest_seen IS NOT NULL
    AND (
      COALESCE(cardinality(sqlc.arg(regions)::text[]), 0) = 0
      OR EXISTS (
        SELECT 1
        FROM observations o
        JOIN observers obs ON obs.id = o.observer_idx
        WHERE o.transmission_id = t.id
          AND upper(btrim(obs.iata)) = ANY(sqlc.arg(regions)::text[])
      )
    )
  ORDER BY a.latest_seen DESC, t.id DESC
  LIMIT sqlc.arg(page_limit)::int OFFSET sqlc.arg(page_offset)::int
)
SELECT t.id AS transmission_id,
       t.hash,
       t.decoded_json::text AS decoded_json,
       t.first_seen,
       t.scope_name,
       p.latest_seen,
       c.repeats,
       f.snr,
       f.observer_pubkey,
       f.observer_name,
       f.hop_count
FROM page p
JOIN transmissions t ON t.id = p.id
CROSS JOIN LATERAL (
  SELECT COUNT(*)::int AS repeats
  FROM observations o
  WHERE o.transmission_id = t.id
) c
LEFT JOIN LATERAL (
  SELECT o.snr,
         ob.pubkey AS observer_pubkey,
         ob.name   AS observer_name,
         cardinality(o.hop_prefixes)::int AS hop_count
  FROM observations o
  LEFT JOIN observers ob ON ob.id = o.observer_idx
  WHERE o.transmission_id = t.id
  ORDER BY o.id
  LIMIT 1
) f ON true
WHERE t.decoded_json IS NOT NULL
  AND c.repeats > 0
ORDER BY p.latest_seen ASC NULLS FIRST, t.id ASC;

-- name: ListChannelSummaries :many
SELECT t.channel_hash::text           AS channel_hash,
       COUNT(*)::bigint               AS msg_count,
       MAX(t.first_seen)::timestamptz AS last_activity,
       (SELECT t2.decoded_json
                 FROM transmissions t2
                 WHERE t2.channel_hash = t.channel_hash
                   AND t2.payload_type = 5
                   AND (COALESCE(cardinality(sqlc.arg(regions)::text[]), 0) = 0
                        OR EXISTS (SELECT 1 FROM observations o2
                                   JOIN observers obs2 ON obs2.id = o2.observer_idx
                                   WHERE o2.transmission_id = t2.id
                                     AND upper(btrim(obs2.iata)) = ANY(sqlc.arg(regions)::text[])))
                 ORDER BY t2.first_seen DESC, t2.id DESC
                 LIMIT 1) AS sample_json
FROM transmissions t
WHERE t.payload_type = 5
  AND t.channel_hash IS NOT NULL
  AND t.channel_hash <> ''
  AND NOT t.channel_encrypted
  AND (COALESCE(cardinality(sqlc.arg(regions)::text[]), 0) = 0
       OR EXISTS (SELECT 1 FROM observations o
                  JOIN observers obs ON obs.id = o.observer_idx
                  WHERE o.transmission_id = t.id
                    AND upper(btrim(obs.iata)) = ANY(sqlc.arg(regions)::text[])))
GROUP BY t.channel_hash
ORDER BY last_activity DESC, t.channel_hash;

-- name: ListEncryptedChannelSummaries :many
SELECT t.channel_hash::text                AS channel_hash,
       COUNT(*)::bigint                    AS msg_count,
       MAX(t.first_seen)::timestamptz      AS last_activity
FROM transmissions t
WHERE t.payload_type = 5
  AND t.channel_encrypted                  -- the CHECK guarantees channel_hash is set
  AND (COALESCE(cardinality(sqlc.arg(regions)::text[]), 0) = 0
       OR EXISTS (SELECT 1 FROM observations o
                  JOIN observers obs ON obs.id = o.observer_idx
                  WHERE o.transmission_id = t.id
                    AND upper(btrim(obs.iata)) = ANY(sqlc.arg(regions)::text[])))
GROUP BY t.channel_hash
ORDER BY last_activity DESC, t.channel_hash;