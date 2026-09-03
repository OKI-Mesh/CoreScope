-- +goose Up
-- #1324 follow-up (PR #903 surface). Populated by the ingestor's
-- RunMultibyteCapPersist; server is read-only for these columns.
ALTER TABLE nodes ADD COLUMN multibyte_sup INTEGER NOT NULL DEFAULT 0;
ALTER TABLE nodes ADD COLUMN multibyte_evidence TEXT;
ALTER TABLE inactive_nodes ADD COLUMN multibyte_sup INTEGER NOT NULL DEFAULT 0;
ALTER TABLE inactive_nodes ADD COLUMN multibyte_evidence TEXT;

-- +goose Down
-- SQLite DROP COLUMN requires 3.35+; columns left as documented no-op.