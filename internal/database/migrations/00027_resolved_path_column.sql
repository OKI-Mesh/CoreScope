-- +goose Up
-- Never tracked in the legacy _migrations table; position best-effort.
ALTER TABLE observations ADD COLUMN resolved_path TEXT;

-- +goose Down
-- SQLite DROP COLUMN requires 3.35+; column left as documented no-op.