-- +goose Up
-- #1690: denormalized "most recent observation timestamp" for cold-load
-- hot-window filtering. Backfill (MAX(observations.timestamp) per tx) is
-- NOT part of this migration — it runs as a separate async job scheduled
-- by the ingestor via Store.RunAsyncMigration, per the async-migration
-- policy (must not block boot on prod-scale tables).
ALTER TABLE transmissions ADD COLUMN last_seen INTEGER NOT NULL DEFAULT 0;

-- #1740 step (a): partial index on the un-backfilled hot subset.
CREATE INDEX IF NOT EXISTS idx_tx_last_seen_zero ON transmissions(id) WHERE last_seen=0;

-- #1740 step (b): drop the legacy full index, if present.
DROP INDEX IF EXISTS idx_tx_last_seen;

-- +goose Down
DROP INDEX IF EXISTS idx_tx_last_seen_zero;
-- SQLite DROP COLUMN requires 3.35+; column left as documented no-op.