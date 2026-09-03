-- +goose Up
-- #1481 P0-3: covering index for GetObserverPacketCounts (GROUP BY
-- observer_idx with a timestamp range filter).
CREATE INDEX IF NOT EXISTS idx_observations_observer_idx_timestamp ON observations(observer_idx, timestamp);

-- +goose Down
DROP INDEX IF EXISTS idx_observations_observer_idx_timestamp;