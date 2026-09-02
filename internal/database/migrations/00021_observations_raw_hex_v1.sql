-- +goose Up
-- #881. Ownership later consolidated into dbschema.Apply per #1321;
-- server PRAGMA-detects this as hasObsRawHex.
ALTER TABLE observations ADD COLUMN raw_hex TEXT;

-- +goose Down
-- SQLite DROP COLUMN requires 3.35+; column left as documented no-op.