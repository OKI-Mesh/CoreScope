-- +goose Up
ALTER TABLE nodes ADD COLUMN configured_scope TEXT DEFAULT NULL;
ALTER TABLE nodes ADD COLUMN configured_scope_at TEXT DEFAULT NULL;
ALTER TABLE inactive_nodes ADD COLUMN configured_scope TEXT DEFAULT NULL;
ALTER TABLE inactive_nodes ADD COLUMN configured_scope_at TEXT DEFAULT NULL;
-- +goose Down
