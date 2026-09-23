-- 00008_observer_metrics_v1.sql
-- +goose Up
CREATE TABLE IF NOT EXISTS observer_metrics (
	observer_id TEXT NOT NULL,
	timestamp TEXT NOT NULL,
	noise_floor REAL,
	tx_air_secs INTEGER,
	rx_air_secs INTEGER,
	recv_errors INTEGER,
	battery_mv INTEGER,
	PRIMARY KEY (observer_id, timestamp)
);

-- +goose Down
DROP TABLE IF EXISTS observer_metrics;