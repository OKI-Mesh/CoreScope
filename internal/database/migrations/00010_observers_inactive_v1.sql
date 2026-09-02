-- +goose Up
ALTER TABLE observers ADD COLUMN inactive INTEGER DEFAULT 0;

-- +goose Down
-- SQLite DROP COLUMN requires 3.35+; left as documented no-op.