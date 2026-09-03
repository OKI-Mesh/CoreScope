-- +goose Up
DROP VIEW IF EXISTS packets_v;
CREATE VIEW packets_v AS
	SELECT o.id, COALESCE(o.raw_hex, t.raw_hex) AS raw_hex,
	       datetime(o.timestamp, 'unixepoch') AS timestamp,
	       obs.id AS observer_id, obs.name AS observer_name,
	       o.direction, o.snr, o.rssi, o.score, t.hash, t.route_type,
	       t.payload_type, t.payload_version, o.path_json, t.decoded_json,
	       t.created_at
	FROM observations o
	JOIN transmissions t ON t.id = o.transmission_id
	LEFT JOIN observers obs ON obs.rowid = o.observer_idx AND (obs.inactive IS NULL OR obs.inactive = 0);

-- +goose Down
DROP VIEW IF EXISTS packets_v;