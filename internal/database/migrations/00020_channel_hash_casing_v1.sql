-- +goose Up
-- Normalizes channel_hash="public" (pre-PR) to "Public" (post-PR) so
-- channel grouping doesn't split into two buckets across the boundary.
UPDATE transmissions SET channel_hash = 'Public' WHERE channel_hash = 'public' AND payload_type = 5;

-- +goose Down
-- Data-normalization migration; no meaningful rollback.