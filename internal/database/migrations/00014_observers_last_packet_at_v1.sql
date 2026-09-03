-- +goose Up
ALTER TABLE observers ADD COLUMN last_packet_at TEXT DEFAULT NULL;

-- Backfill: only for observers that actually have observation rows
-- (packet_count alone is unreliable — INSERT sets it to 1 even for
-- status-only observers).
UPDATE observers SET last_packet_at = last_seen
	WHERE last_packet_at IS NULL
	AND rowid IN (SELECT DISTINCT observer_idx FROM observations WHERE observer_idx IS NOT NULL);

-- +goose Down
-- SQLite DROP COLUMN requires 3.35+; left as documented no-op.