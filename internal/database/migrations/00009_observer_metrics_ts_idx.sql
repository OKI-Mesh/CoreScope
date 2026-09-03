-- +goose Up
CREATE INDEX IF NOT EXISTS idx_observer_metrics_timestamp ON observer_metrics(timestamp);

-- +goose Down
DROP INDEX IF EXISTS idx_observer_metrics_timestamp;