-- +goose Up
-- #1478: per-observer naive-clock skew tracking, written by the ingestor
-- when resolveRxTime clamps a zone-less local-time envelope timestamp.
ALTER TABLE observers ADD COLUMN clock_skew_seconds INTEGER DEFAULT NULL;
ALTER TABLE observers ADD COLUMN clock_skew_count_24h INTEGER DEFAULT 0;
ALTER TABLE observers ADD COLUMN clock_last_naive_at TEXT DEFAULT NULL;

-- +goose Down
-- SQLite DROP COLUMN requires 3.35+; columns left as documented no-op.