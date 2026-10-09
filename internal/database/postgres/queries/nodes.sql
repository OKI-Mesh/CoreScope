-- -- name: GetObserverByPubkey :one
-- SELECT * FROM observers WHERE pubkey = $1;

-- name: UpdateNodeLastSeen :execrows
UPDATE nodes
SET last_seen = sqlc.arg(last_seen)
WHERE public_key = sqlc.arg(public_key)
  AND (last_seen IS NULL OR last_seen < sqlc.arg(last_seen));


-- name: CreateOrUpdateNode :exec
INSERT INTO nodes (public_key, name, role, lat, lon, last_seen, first_seen)
VALUES (
    sqlc.arg(public_key),
    sqlc.narg(name),
    sqlc.narg(role),
    sqlc.narg(lat),
    sqlc.narg(lon),
    sqlc.arg(rx_time)::timestamptz,
    sqlc.arg(rx_time)::timestamptz
)
ON CONFLICT (public_key) DO UPDATE SET
    name      = COALESCE(sqlc.narg(name), nodes.name),
    role      = COALESCE(sqlc.narg(role), nodes.role),
    lat       = COALESCE(sqlc.narg(lat), nodes.lat),
    lon       = COALESCE(sqlc.narg(lon), nodes.lon),
    last_seen = GREATEST(
                    LEAST(COALESCE(nodes.last_seen, 'epoch'::timestamptz), sqlc.arg(ingest_now)::timestamptz),
                    sqlc.arg(rx_time)::timestamptz
                );

-- name: IncrementNodeAdvertCount :exec
UPDATE nodes SET advert_count = advert_count + 1 WHERE public_key = sqlc.arg(public_key);

-- name: MarkStaleNodesInactive :execrows
UPDATE nodes SET inactive = true WHERE last_seen < $1 AND inactive = false;

-- name: UpdateBatteryVoltageAndTempC :exec
UPDATE nodes SET battery_mv = COALESCE($1, battery_mv),
			temperature_c = COALESCE($2, temperature_c)
		WHERE public_key = $3;

-- name: SetForeignAdvert :exec
UPDATE nodes SET foreign_advert = true WHERE public_key = $1;

-- name: UpdateNodeConfiguredScope :exec
UPDATE nodes
SET configured_scope = sqlc.arg(scope)::text,
    configured_scope_at = sqlc.narg(reported_at)::timestamptz
WHERE public_key = sqlc.arg(public_key)
  AND (sqlc.narg(reported_at)::timestamptz IS NULL
       OR configured_scope_at IS NULL
       OR configured_scope_at < sqlc.narg(reported_at)::timestamptz);

-- name: UpdateNodeDefaultScope :exec
UPDATE nodes
SET default_scope = sqlc.arg(scope)::text
WHERE public_key = sqlc.arg(public_key)
  AND default_scope IS DISTINCT FROM sqlc.arg(scope)::text;

-- name: UpdateNodeMultibyteCaps :execrows
WITH v AS (
    SELECT unnest(sqlc.arg(pubkeys)::text[])  AS public_key,
           unnest(sqlc.arg(sups)::int[])      AS sup,
           unnest(sqlc.arg(evidence)::text[]) AS evidence
)
UPDATE nodes n
SET multibyte_sup      = v.sup,
    multibyte_evidence = NULLIF(v.evidence, '')
FROM v
WHERE n.public_key = v.public_key
  AND n.multibyte_sup <= v.sup  -- never downgrade: confirmed (2) stays above suspected (1)
  AND (n.multibyte_sup, n.multibyte_evidence)
      IS DISTINCT FROM (v.sup, NULLIF(v.evidence, ''));

-- name: DeleteNodesByPubkeys :execrows
WITH gone AS (
    DELETE FROM neighbor_edges
    WHERE node_a = ANY(sqlc.arg(pubkeys)::text[])
       OR node_b = ANY(sqlc.arg(pubkeys)::text[])
)
DELETE FROM nodes WHERE public_key = ANY(sqlc.arg(pubkeys)::text[]);

-- name: ListNodePublicKeys :many
SELECT public_key FROM nodes;