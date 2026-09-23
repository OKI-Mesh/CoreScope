-- +goose Up
ALTER TABLE observers ADD COLUMN last_packet_at TEXT DEFAULT NULL;

-- +goose Down
-- SQLite DROP COLUMN requires 3.35+; left as documented no-op.