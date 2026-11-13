-- +goose Up

ALTER TABLE nodes ADD COLUMN configured_scope TEXT;
ALTER TABLE nodes ADD COLUMN configured_scope_at TEXT;
ALTER TABLE inactive_nodes ADD COLUMN configured_scope TEXT;
ALTER TABLE inactive_nodes ADD COLUMN configured_scope_at TEXT;

-- +goose Down

ALTER TABLE nodes DROP COLUMN configured_scope;
ALTER TABLE nodes DROP COLUMN configured_scope_at;
ALTER TABLE inactive_nodes DROP COLUMN configured_scope;
ALTER TABLE inactive_nodes DROP COLUMN configured_scope_at;
