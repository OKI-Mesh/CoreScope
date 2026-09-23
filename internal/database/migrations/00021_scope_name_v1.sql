-- +goose Up
-- #899. Ownership later consolidated into dbschema.Apply per #1321;
-- schema effect is identical.
ALTER TABLE transmissions ADD COLUMN scope_name TEXT DEFAULT NULL;
CREATE INDEX IF NOT EXISTS idx_tx_scope_name ON transmissions(scope_name) WHERE scope_name IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS idx_tx_scope_name;
-- SQLite DROP COLUMN requires 3.35+; column left as documented no-op.