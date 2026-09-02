-- +goose Up
CREATE TABLE IF NOT EXISTS observations (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	transmission_id INTEGER NOT NULL REFERENCES transmissions(id),
	observer_idx INTEGER,
	direction TEXT,
	snr REAL,
	rssi REAL,
	score INTEGER,
	path_json TEXT,
	timestamp INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_observations_transmission_id ON observations(transmission_id);
CREATE INDEX IF NOT EXISTS idx_observations_observer_idx ON observations(observer_idx);
CREATE INDEX IF NOT EXISTS idx_observations_timestamp ON observations(timestamp);

DELETE FROM observations
WHERE id NOT IN (
	SELECT MIN(id)
	FROM observations
	GROUP BY transmission_id, observer_idx, COALESCE(path_json, '')
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_observations_dedup ON observations(transmission_id, observer_idx, COALESCE(path_json, ''));

-- +goose Down
DROP TABLE IF EXISTS observations;