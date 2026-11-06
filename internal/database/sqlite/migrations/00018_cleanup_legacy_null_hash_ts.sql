-- +goose Up
-- #994: one-time cleanup of legacy packets with empty hash or first_seen.
DELETE FROM observations WHERE transmission_id IN (
	SELECT id FROM transmissions WHERE hash = '' OR first_seen = ''
);
DELETE FROM transmissions WHERE hash = '' OR first_seen = '';

-- +goose Down
-- Deletion migration; no meaningful rollback.