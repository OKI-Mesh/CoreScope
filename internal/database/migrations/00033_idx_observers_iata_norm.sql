-- +goose Up
CREATE INDEX IF NOT EXISTS idx_observers_iata_norm ON observers(UPPER(TRIM(iata)));

-- +goose Down
DROP INDEX IF EXISTS idx_observers_iata_norm;


