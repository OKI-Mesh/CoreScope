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

-- name: ListLocatedNodes :many
SELECT public_key, lat::float8 AS lat, lon::float8 AS lon
FROM nodes
WHERE lat IS NOT NULL AND lon IS NOT NULL;

-- name: ListMultibyteCapNodes :many
SELECT public_key,
       COALESCE(name, '')::text               AS name,
       role,
       last_seen,
       multibyte_sup::int                     AS multibyte_sup,
       COALESCE(multibyte_evidence, '')::text AS multibyte_evidence
FROM nodes
WHERE multibyte_sup > 0;

-- name: ListNodeLocationsByKeys :many
SELECT public_key, lat, lon, role
FROM nodes
WHERE public_key = ANY(sqlc.arg(keys)::text[]);

-- name: CountNodesByRole :many
-- active_since NULL = all nodes, no time filter.
SELECT role::text AS role, count(*)::bigint AS n
FROM nodes
WHERE sqlc.narg('active_since')::timestamptz IS NULL
   OR last_seen > sqlc.narg('active_since')::timestamptz
GROUP BY role;

-- name: CountActiveNodes :one
SELECT COUNT(*) FROM nodes WHERE NOT inactive;

-- name: LookupNode :one
SELECT public_key
FROM nodes
WHERE public_key = sqlc.narg(pubkey)::text
   OR name = sqlc.narg(name)::text
ORDER BY (public_key = sqlc.narg(pubkey)::text) DESC, public_key
LIMIT 1;

-- name: ListNodes :many
-- Filtered, sorted, paged node list. NULL filter = not applied.
-- sort: 'name', 'packetCount', anything else = last_seen.
-- NULLS clauses reproduce SQLite's ordering (NULLs smallest).
SELECT n.*
FROM nodes n
WHERE (sqlc.narg(role)::text IS NULL OR n.role = sqlc.narg(role)::text)
  AND (sqlc.narg(search)::text IS NULL
       OR strpos(lower(n.name), lower(sqlc.narg(search)::text)) > 0)
  AND (sqlc.narg(before)::timestamptz IS NULL OR n.first_seen <= sqlc.narg(before)::timestamptz)
  AND (sqlc.narg(heard_since)::timestamptz IS NULL OR n.last_seen > sqlc.narg(heard_since)::timestamptz)
  AND (COALESCE(cardinality(sqlc.narg(regions)::text[]), 0) = 0
       OR EXISTS (
            -- #1143: from_pubkey is the indexed column set at ingest for ADVERT rows
            SELECT 1
            FROM transmissions t
            JOIN observations o ON o.transmission_id = t.id
            JOIN observers obs ON obs.id = o.observer_idx
            WHERE t.from_pubkey = n.public_key
              AND t.payload_type = 4
              AND UPPER(TRIM(obs.iata)) = ANY(sqlc.narg(regions)::text[])
       ))
  AND n.public_key <> ALL(COALESCE(sqlc.narg(exclude_keys)::text[], '{}'))
  AND NOT EXISTS (
        SELECT 1
        FROM unnest(COALESCE(sqlc.narg(hidden_prefixes)::text[], '{}')) AS hp(prefix)
        WHERE starts_with(lower(COALESCE(n.name, '')), lower(hp.prefix))
  )
ORDER BY
    CASE WHEN @sort::text = 'name' THEN n.name END ASC NULLS FIRST,
    CASE WHEN @sort::text = 'packetCount' THEN n.advert_count END DESC NULLS LAST,
    CASE WHEN @sort::text NOT IN ('name', 'packetCount') THEN n.last_seen END DESC NULLS LAST,
    n.public_key ASC
LIMIT @row_limit OFFSET @row_offset;

-- name: CountNodes :one
-- Same filters as ListNodes. Keep the two WHERE clauses in sync.
SELECT COUNT(*)::bigint
FROM nodes n
WHERE (sqlc.narg(role)::text IS NULL OR n.role = sqlc.narg(role)::text)
  AND (sqlc.narg(search)::text IS NULL
       OR strpos(lower(n.name), lower(sqlc.narg(search)::text)) > 0)
  AND (sqlc.narg(before)::timestamptz IS NULL OR n.first_seen <= sqlc.narg(before)::timestamptz)
  AND (sqlc.narg(heard_since)::timestamptz IS NULL OR n.last_seen > sqlc.narg(heard_since)::timestamptz)
  AND n.public_key <> ALL(COALESCE(sqlc.narg(exclude_keys)::text[], '{}'))
  AND NOT EXISTS (
        SELECT 1
        FROM unnest(COALESCE(sqlc.narg(hidden_prefixes)::text[], '{}')) AS hp(prefix)
        WHERE starts_with(lower(COALESCE(n.name, '')), lower(hp.prefix))
  )
  AND (COALESCE(cardinality(sqlc.narg(regions)::text[]), 0) = 0
       OR EXISTS (
            SELECT 1
            FROM transmissions t
            JOIN observations o ON o.transmission_id = t.id
            JOIN observers obs ON obs.id = o.observer_idx
            WHERE t.from_pubkey = n.public_key
              AND t.payload_type = 4
              AND UPPER(TRIM(obs.iata)) = ANY(sqlc.narg(regions)::text[])
       ));

-- name: ListNodeCoordinates :many
SELECT public_key, lat::float8 AS lat, lon::float8 AS lon
FROM nodes
WHERE lat IS NOT NULL
  AND lon IS NOT NULL
  AND NOT (lat = 0 AND lon = 0); 

-- name: SearchNodes :many
-- Nodes whose name contains the query (case-insensitive) or whose public key
-- starts with it. The query is a literal: % and _ have no special meaning.
SELECT *
FROM nodes
WHERE strpos(lower(name), lower(@query::text)) > 0
   OR starts_with(public_key, lower(@query::text))
ORDER BY last_seen DESC NULLS LAST, public_key ASC
LIMIT @row_limit;

-- name: FindNodesByKeyPrefix :many
-- At most two nodes whose public key starts with the prefix; two is enough to
-- tell "unique" from "ambiguous". The prefix is validated as hex by the
-- caller, so LIKE has no wildcard characters to worry about.
SELECT *
FROM nodes
WHERE public_key LIKE @prefix::text || '%'
ORDER BY public_key
LIMIT 2;

-- name: GetNodeByPublicKey :one
SELECT *
FROM nodes
WHERE public_key = @public_key::text;

-- name: ListNodeHealth :many
SELECT role, last_seen
FROM nodes;

-- name: ListNodesForGeoPrune :many
SELECT public_key, COALESCE(name, '')::text AS name, lat, lon
FROM nodes
ORDER BY name NULLS FIRST, public_key;

-- name: MatchNodesByPrefix :many
SELECT p.prefix::text AS prefix,
       count(DISTINCT n.public_key)::bigint AS matches,
       min(n.public_key)::text AS public_key,
       min(COALESCE(n.name, ''))::text AS name
FROM (SELECT unnest(sqlc.arg(prefixes)::text[]) AS prefix) AS p
JOIN nodes n ON n.public_key LIKE p.prefix || '%'
GROUP BY p.prefix;

-- name: ListNodeIdentities :many
SELECT public_key, name, role
FROM nodes
WHERE public_key = ANY(sqlc.arg(pubkeys)::text[]);

-- name: ResolveNodePubkeys :many
-- Maps each input (a pubkey or a node name) to a node's public key. Inputs that
-- match nothing are absent from the result. An exact pubkey match wins over a
-- name match, and among name matches the lowest pubkey wins, so the answer is
-- stable. (SQLite's LIMIT 1 had no ORDER BY, so it was arbitrary.)
SELECT DISTINCT ON (i.input) i.input::text AS input, n.public_key
FROM (SELECT unnest(@inputs::text[]) AS input) AS i
JOIN nodes n ON n.public_key = lower(i.input) OR n.name = i.input
ORDER BY i.input, (n.public_key = lower(i.input)) DESC, n.public_key;

-- name: ListAllNodes :many
-- Every node, for the resolver cache. No filters or paging.
SELECT * FROM nodes ORDER BY public_key;