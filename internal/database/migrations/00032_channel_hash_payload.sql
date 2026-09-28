-- +goose Up
CREATE INDEX IF NOT EXISTS idx_transmissions_channel_hash_payload ON transmissions(channel_hash, payload_type, first_seen);

-- +goose Down
DROP INDEX IF EXISTS idx_transmissions_channel_hash_payload;
