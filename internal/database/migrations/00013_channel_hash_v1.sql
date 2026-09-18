-- +goose Up
-- #762
ALTER TABLE transmissions ADD COLUMN channel_hash TEXT DEFAULT NULL;
CREATE INDEX IF NOT EXISTS idx_tx_channel_hash ON transmissions(channel_hash) WHERE payload_type = 5;

-- +goose Down
DROP INDEX IF EXISTS idx_tx_channel_hash;
-- SQLite DROP COLUMN requires 3.35+; column left as documented no-op.