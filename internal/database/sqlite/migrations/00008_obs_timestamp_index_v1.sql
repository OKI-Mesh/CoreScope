-- +goose Up
-- Historically added to fix full-table-scan /api/stats queries on
-- older DBs created before 00002 included this index. Idempotent
-- (IF NOT EXISTS) against DBs that already have it from 00002.
CREATE INDEX IF NOT EXISTS idx_observations_timestamp ON observations(timestamp);

-- +goose Down
-- No-op: index already required by 00002's Down (table drop).