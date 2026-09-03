-- +goose Up
ALTER TABLE observer_metrics ADD COLUMN packets_sent INTEGER;
ALTER TABLE observer_metrics ADD COLUMN packets_recv INTEGER;

-- +goose Down
-- SQLite DROP COLUMN requires 3.35+; left as documented no-op.