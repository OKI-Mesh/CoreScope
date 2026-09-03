-- +goose Up
-- #1143: replaces the unsound `decoded_json LIKE '%pubkey%'` attribution
-- path with an exact-match indexed column. Row-level backfill historically
-- ran async from the server side (cmd/server/from_pubkey_migration.go) —
-- not part of this schema migration.
ALTER TABLE transmissions ADD COLUMN from_pubkey TEXT;
CREATE INDEX IF NOT EXISTS idx_transmissions_from_pubkey ON transmissions(from_pubkey);

-- +goose Down
DROP INDEX IF EXISTS idx_transmissions_from_pubkey;
-- SQLite DROP COLUMN requires 3.35+; column left as documented no-op.