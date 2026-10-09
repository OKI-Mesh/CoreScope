-- +goose Up
-- #899 Feature 3. Ownership later consolidated into dbschema.Apply per #1321.
ALTER TABLE nodes ADD COLUMN default_scope TEXT DEFAULT NULL;
ALTER TABLE inactive_nodes ADD COLUMN default_scope TEXT DEFAULT NULL;

-- +goose Down
-- SQLite DROP COLUMN requires 3.35+; columns left as documented no-op.