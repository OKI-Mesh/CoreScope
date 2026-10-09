-- name: GetObserverByPubkey :one
SELECT * FROM observers WHERE pubkey = $1;

-- name: GetObserverID :one
SELECT id FROM observers WHERE pubkey = $1;

-- name: TouchObserverLastSeen :exec
UPDATE observers SET
    last_seen      = $1,
    last_packet_at = $1
WHERE id = $2;

-- name: CreateOrUpdateObserver :exec
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
                          THEN observers.can_relay_seen ELSE true END;

-- name: ReactivateObserver :exec
UPDATE observers SET inactive = false WHERE pubkey = $1 AND inactive = true;

-- name: RecordNaiveSkew :exec
INSERT INTO observers (pubkey, clock_skew_seconds, clock_skew_count_24h, clock_last_naive_at)
VALUES ($1, $2, 1, $3)
ON CONFLICT (pubkey) DO UPDATE SET
    clock_skew_seconds   = EXCLUDED.clock_skew_seconds,
    clock_last_naive_at  = EXCLUDED.clock_last_naive_at,
    clock_skew_count_24h = CASE
        WHEN observers.clock_last_naive_at IS NULL OR observers.clock_last_naive_at < sqlc.arg(cutoff)
            THEN 1
        ELSE COALESCE(observers.clock_skew_count_24h, 0) + 1
    END;

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