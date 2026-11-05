-- name: GetObserverByPubkey :one
SELECT * FROM observers WHERE pubkey = $1;

-- name: GetObserverID :one
SELECT id FROM observers WHERE pubkey = $1;

-- name: TouchObserverLastSeen :exec
UPDATE observers SET
    last_seen      = $1,
    last_packet_at = $1
WHERE id = $2;

-- name: CreateOrUpdateObserver :one
INSERT INTO observers (
    pubkey, name, iata, last_seen, first_seen, packet_count,
    model, firmware, client_version, radio, battery_mv, uptime_secs,
    noise_floor, can_relay, can_relay_seen
)
VALUES (
    sqlc.arg(pubkey),
    sqlc.narg(name),
    sqlc.narg(iata),
    sqlc.arg(last_seen)::timestamptz,
    sqlc.arg(last_seen)::timestamptz,
    1,
    sqlc.narg(model),
    sqlc.narg(firmware),
    sqlc.narg(client_version),
    sqlc.narg(radio),
    sqlc.narg(battery_mv),
    sqlc.narg(uptime_secs),
    sqlc.narg(noise_floor),
    COALESCE(sqlc.narg(can_relay)::boolean, true),
    sqlc.narg(can_relay)::boolean IS NOT NULL
)
ON CONFLICT (pubkey) DO UPDATE SET
    name           = COALESCE(sqlc.narg(name), observers.name),
    iata           = COALESCE(sqlc.narg(iata), observers.iata),
    last_seen      = GREATEST(
                         LEAST(COALESCE(observers.last_seen, 'epoch'::timestamptz), sqlc.arg(ingest_now)::timestamptz),
                         sqlc.arg(last_seen)::timestamptz
                     ),
    packet_count   = observers.packet_count + 1,
    model          = COALESCE(sqlc.narg(model), observers.model),
    firmware       = COALESCE(sqlc.narg(firmware), observers.firmware),
    client_version = COALESCE(sqlc.narg(client_version), observers.client_version),
    radio          = COALESCE(sqlc.narg(radio), observers.radio),
    battery_mv     = COALESCE(sqlc.narg(battery_mv), observers.battery_mv),
    uptime_secs    = COALESCE(sqlc.narg(uptime_secs), observers.uptime_secs),
    noise_floor    = COALESCE(sqlc.narg(noise_floor), observers.noise_floor),
    can_relay      = COALESCE(sqlc.narg(can_relay)::boolean, observers.can_relay),
    can_relay_seen = CASE WHEN sqlc.narg(can_relay)::boolean IS NULL
                          THEN observers.can_relay_seen ELSE true END
    RETURNING id;

-- name: ReactivateObserver :exec
UPDATE observers SET inactive = false WHERE pubkey = $1 AND inactive = true;

-- name: RecordNaiveSkew :exec
UPDATE observers SET
    clock_skew_seconds   = sqlc.arg(clock_skew_seconds),
    clock_last_naive_at  = sqlc.arg(clock_last_naive_at),
    clock_skew_count_24h = CASE
        WHEN clock_last_naive_at IS NULL OR clock_last_naive_at < sqlc.arg(cutoff)
            THEN 1
        ELSE COALESCE(clock_skew_count_24h, 0) + 1
    END
WHERE pubkey = sqlc.arg(pubkey);

-- name: UpdateObserverMetadataIfRetained :exec
UPDATE observers SET
    name           = COALESCE(sqlc.narg(name), name),
    iata           = COALESCE(sqlc.narg(iata), iata),
    model          = COALESCE(sqlc.narg(model), model),
    firmware       = COALESCE(sqlc.narg(firmware), firmware),
    client_version = COALESCE(sqlc.narg(client_version), client_version),
    radio          = COALESCE(sqlc.narg(radio), radio),
    battery_mv     = COALESCE(sqlc.narg(battery_mv), battery_mv),
    uptime_secs    = COALESCE(sqlc.narg(uptime_secs), uptime_secs),
    noise_floor    = COALESCE(sqlc.narg(noise_floor), noise_floor),
    can_relay      = COALESCE(sqlc.narg(can_relay)::boolean, can_relay),
    can_relay_seen = CASE WHEN sqlc.narg(can_relay)::boolean IS NULL THEN can_relay_seen ELSE true END
WHERE pubkey = sqlc.arg(pubkey);

-- name: PurgeStaleObservers :execrows
DELETE FROM observers
WHERE inactive = true
  AND last_seen < sqlc.arg(cutoff)::timestamptz
  AND NOT EXISTS (SELECT 1 FROM observations o WHERE o.observer_idx = observers.id)
  AND NOT EXISTS (SELECT 1 FROM observer_metrics m WHERE m.observer_id = observers.pubkey)
  AND NOT EXISTS (SELECT 1 FROM dropped_packets d WHERE d.observer_id = observers.pubkey)
;

-- name: MarkStaleObserversInactive :execrows
UPDATE observers
SET inactive = true
WHERE last_seen < sqlc.arg(cutoff)::timestamptz
  AND inactive = false;

-- name: SoftDeleteBlacklistedObservers :execrows
UPDATE observers
SET inactive = true
WHERE lower(pubkey) = ANY(sqlc.arg(pubkeys)::text[])
  AND inactive = false;

-- name: CountActiveObservers :one
SELECT COUNT(*)::bigint
FROM observers
WHERE NOT COALESCE(inactive, false);

-- name: ListActiveObservers :many
-- Active (not soft-deleted) observers, most recently seen first.
-- can_relay is tri-state: NULL unless the observer ever reported the firmware
-- 1.16 `repeat` flag (can_relay_seen), so legacy rows don't masquerade as
-- confirmed repeaters (#1290).
-- name: ListActiveObservers :many
SELECT * FROM observers WHERE NOT inactive ORDER BY last_seen DESC, id ASC;

-- name: ListReportedRelayObservers :many
SELECT lower(pubkey)::text AS pubkey, can_relay
FROM observers
WHERE can_relay_seen AND NOT inactive
ORDER BY pubkey;

-- name: ListObserverPubkeysByIATA :many
SELECT pubkey
FROM observers
WHERE upper(btrim(iata)) = ANY(sqlc.arg(codes)::text[])
ORDER BY pubkey;

-- name: ListDistinctObserverIATAs :many
SELECT DISTINCT iata::text AS iata
FROM observers
WHERE iata IS NOT NULL
ORDER BY iata;

-- name: ListObserverNames :many
-- Every observer, active or not: the node-info map enriches names for any observer key.
SELECT pubkey, COALESCE(name, '')::text AS name
FROM observers
ORDER BY pubkey;

-- name: ListNodeBatteryHistory :many
-- battery_mv is cast because the IS NOT NULL filter does not narrow the column type for sqlc.
SELECT timestamp, battery_mv::int AS battery_mv
FROM observer_metrics
WHERE observer_id = sqlc.arg(pubkey)::text
  AND battery_mv IS NOT NULL
  AND timestamp >= sqlc.arg(since)::timestamptz
ORDER BY timestamp ASC;

-- name: ListNonRelayObserverPubkeys :many
SELECT pubkey
FROM observers
WHERE NOT can_relay AND NOT inactive
ORDER BY pubkey;

-- name: MarkObserverInactive :exec
UPDATE observers SET inactive = true
WHERE pubkey = sqlc.arg(pubkey);

-- name: GetObserverLastSeen :one
SELECT last_seen FROM observers WHERE pubkey = $1;