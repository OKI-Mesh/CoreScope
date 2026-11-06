-- name: GetStats :one
SELECT
    (SELECT count(*) FROM transmissions)::bigint AS total_transmissions,
    (SELECT count(*) FROM observations)::bigint AS total_observations,
    (SELECT count(*) FROM nodes WHERE last_seen > @active_since::timestamptz)::bigint AS total_nodes,
    (SELECT count(*) FROM nodes)::bigint AS total_nodes_all_time,
    (SELECT count(*) FROM observers WHERE inactive IS NOT TRUE)::bigint AS total_observers,
    (SELECT count(*) FROM observations WHERE timestamp > @hour_ago::timestamptz)::bigint AS packets_last_hour,
    (SELECT count(*) FROM observations WHERE timestamp > @day_ago::timestamptz)::bigint AS packets_last_24h;

-- name: GetDBSizeStats :one
SELECT
    pg_database_size(current_database())::bigint AS db_size_bytes,
    (SELECT COUNT(*) FROM transmissions) AS transmissions,
    (SELECT COUNT(*) FROM observations)  AS observations,
    (SELECT COUNT(*) FROM nodes)         AS nodes,
    (SELECT COUNT(*) FROM observers)     AS observers;

-- name: GetEntityCounts :one
-- Active nodes (heard after @active_since), all nodes, and active observers.
SELECT
    (SELECT count(*) FROM nodes WHERE last_seen > @active_since::timestamptz) AS active_nodes,
    (SELECT count(*) FROM nodes)                                               AS all_nodes,
    (SELECT count(*) FROM observers WHERE NOT inactive)                        AS observers;

-- name: CountRecentObservations :one
-- Observations in the last hour and the last day, in one scan of the
-- timestamp index.
SELECT count(*) FILTER (WHERE timestamp > @hour_ago::timestamptz) AS last_hour,
       count(*)                                                    AS last_day
FROM observations
WHERE timestamp > @day_ago::timestamptz;