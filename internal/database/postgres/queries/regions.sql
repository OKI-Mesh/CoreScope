-- name: ListConfiguredScopes :many
SELECT COALESCE(configured_scope, '')::text AS csv,
       COALESCE(configured_scope_at, 'epoch'::timestamptz)::timestamptz AS seen_at
FROM nodes
WHERE configured_scope IS NOT NULL AND configured_scope <> '';

-- name: ListLatestDeclaredRegions :many
SELECT DISTINCT ON (target) regions_csv AS csv, observed_at AS seen_at
FROM node_declared_regions
ORDER BY target, observed_at DESC;