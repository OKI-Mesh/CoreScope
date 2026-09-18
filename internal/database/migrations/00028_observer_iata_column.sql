-- +goose Up
-- Never tracked in the legacy _migrations table; position best-effort.
ALTER TABLE observers ADD COLUMN iata TEXT;

CREATE INDEX IF NOT EXISTS idx_observers_iata_norm ON observers(UPPER(TRIM(iata)));

-- +goose Down
-- SQLite DROP COLUMN requires 3.35+; column left as documented no-op.