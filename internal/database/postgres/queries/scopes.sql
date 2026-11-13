-- name: GetScopeMatchTotals :one
SELECT counting_since, unique_matches, explicit_over_derived, ambiguous, none_matches
FROM scope_match_totals
WHERE id = 1;

-- name: UpsertScopeMatchTotals :exec
INSERT INTO scope_match_totals
    (id, counting_since, unique_matches, explicit_over_derived, ambiguous, none_matches, updated_at)
VALUES (1,
        sqlc.arg(counting_since)::timestamptz,
        sqlc.arg(unique_matches)::bigint,
        sqlc.arg(explicit_over_derived)::bigint,
        sqlc.arg(ambiguous)::bigint,
        sqlc.arg(none_matches)::bigint,
        sqlc.arg(updated_at)::timestamptz)
ON CONFLICT (id) DO UPDATE SET
    counting_since        = EXCLUDED.counting_since,
    unique_matches        = EXCLUDED.unique_matches,
    explicit_over_derived = EXCLUDED.explicit_over_derived,
    ambiguous             = EXCLUDED.ambiguous,
    none_matches          = EXCLUDED.none_matches,
    updated_at            = EXCLUDED.updated_at;

-- name: ListLatestDeclaredRegions :many
SELECT DISTINCT ON (target) target, observed_at, regions_csv, truncated
FROM node_declared_regions
ORDER BY target, observed_at DESC;

-- name: ListConfiguredScopes :many
SELECT public_key, configured_scope_at, configured_scope::text AS configured_scope
FROM nodes
WHERE configured_scope IS NOT NULL;

-- name: GetScopeSummary :one
-- One scan of the window. Non-transport routes never carry transport_code_1, so
-- they are inherently unscoped (#1838); the DB layer folds them into Unscoped.
SELECT
    count(*) FILTER (WHERE route_type = ANY(sqlc.arg(transport_routes)::int[])) AS transport_total,
    count(scope_name) FILTER (WHERE route_type = ANY(sqlc.arg(transport_routes)::int[])) AS scoped,
    count(*) FILTER (WHERE route_type = ANY(sqlc.arg(transport_routes)::int[]) AND scope_name IS NULL) AS unscoped,
    count(*) FILTER (WHERE route_type = ANY(sqlc.arg(transport_routes)::int[]) AND scope_name = '') AS unknown_scope,
    count(*) FILTER (WHERE route_type = ANY(sqlc.arg(non_transport_routes)::int[])) AS non_transport
FROM transmissions
WHERE first_seen >= sqlc.arg(since)::timestamptz;

-- name: ListScopeRegionCounts :many
SELECT scope_name::text AS name, count(*) AS cnt
FROM transmissions
WHERE route_type = ANY(sqlc.arg(transport_routes)::int[])
  AND scope_name IS NOT NULL AND scope_name <> ''
  AND first_seen >= sqlc.arg(since)::timestamptz
GROUP BY scope_name
ORDER BY cnt DESC, scope_name;

-- name: ListScopeTimeSeries :many
-- Buckets are aligned to the Unix epoch in UTC, whatever the session time zone.
SELECT
    date_bin(sqlc.arg(bucket_secs)::int * interval '1 second', first_seen, TIMESTAMPTZ '1970-01-01 00:00:00+00')::timestamptz AS bucket,
    count(scope_name) AS scoped,
    count(*) FILTER (WHERE scope_name IS NULL) AS unscoped
FROM transmissions
WHERE route_type = ANY(sqlc.arg(transport_routes)::int[])
  AND first_seen >= sqlc.arg(since)::timestamptz
GROUP BY 1
ORDER BY 1;

-- name: ListScopeAdvertsByRole :many
-- #1979: flood adverts by sender role, split by the three scope_name states.
SELECT
    COALESCE(NULLIF(n.role, ''), 'unknown')::text AS role,
    count(*) FILTER (WHERE t.scope_name IS NULL) AS unscoped,
    count(*) FILTER (WHERE t.scope_name = '') AS unknown_scope,
    count(*) FILTER (WHERE t.scope_name <> '') AS named
FROM transmissions t
LEFT JOIN nodes n ON n.public_key = t.from_pubkey
WHERE t.payload_type = sqlc.arg(payload_type)::int
  AND t.route_type = ANY(sqlc.arg(flood_routes)::int[])
  AND t.first_seen >= sqlc.arg(since)::timestamptz
GROUP BY 1
ORDER BY count(*) DESC, 1;

-- name: ListScopeAuditHops :many
SELECT DISTINCT t.id AS transmission_id,
       h.hop::text AS hop,
       t.scope_name,
       t.first_seen
FROM transmissions t
JOIN observations o ON o.transmission_id = t.id
CROSS JOIN LATERAL unnest(o.hop_prefixes) AS h(hop)
WHERE t.first_seen >= sqlc.arg(since)
  AND t.route_type = ANY(sqlc.arg(route_types)::int[])
  AND o.hop_prefixes && sqlc.arg(prefixes)::text[]
  AND length(h.hop) >= sqlc.arg(min_hop_len)::int
  AND h.hop = ANY(sqlc.arg(prefixes)::text[])
  ORDER BY transmission_id, hop;