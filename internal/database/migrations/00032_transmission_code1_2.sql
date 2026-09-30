-- +goose Up

ALTER TABLE transmissions ADD COLUMN code1 TEXT;
ALTER TABLE transmissions ADD COLUMN code2 TEXT;

CREATE INDEX IF NOT EXISTS idx_tx_code1 ON transmissions(code1) WHERE code1 IS NOT NULL;
-- +goose Down

DROP INDEX IF EXISTS idx_tx_code1;
ALTER TABLE transmissions DROP COLUMN code1;
ALTER TABLE transmissions DROP COLUMN code2;

