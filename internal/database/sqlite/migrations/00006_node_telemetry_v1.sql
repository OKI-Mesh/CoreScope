-- +goose Up
ALTER TABLE nodes ADD COLUMN battery_mv INTEGER;
ALTER TABLE nodes ADD COLUMN temperature_c REAL;
ALTER TABLE inactive_nodes ADD COLUMN battery_mv INTEGER;
ALTER TABLE inactive_nodes ADD COLUMN temperature_c REAL;

-- +goose Down
-- SQLite DROP COLUMN requires 3.35+; left as documented no-op.