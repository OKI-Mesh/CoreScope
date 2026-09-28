-- +goose Up
ALTER TABLE transmissions ADD COLUMN code1 TEXT DEFAULT NULL;
ALTER TABLE transmissions ADD COLUMN code2 TEXT DEFAULT NULL;

CREATE INDEX IF NOT EXISTS idx_tx_code1 ON transmissions(code1) WHERE code1 IS NOT NULL;

-- +goose Down
