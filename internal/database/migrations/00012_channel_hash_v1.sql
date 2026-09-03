-- +goose Up
-- #762
ALTER TABLE transmissions ADD COLUMN channel_hash TEXT DEFAULT NULL;
CREATE INDEX IF NOT EXISTS idx_tx_channel_hash ON transmissions(channel_hash) WHERE payload_type = 5;

-- Backfill: extract channel name for decrypted (CHAN) packets.
UPDATE transmissions SET channel_hash = json_extract(decoded_json, '$.channel')
	WHERE payload_type = 5 AND channel_hash IS NULL
	  AND json_extract(decoded_json, '$.type') = 'CHAN';

-- Backfill: extract channelHashHex for encrypted (GRP_TXT) packets, prefixed 'enc_'.
UPDATE transmissions SET channel_hash = 'enc_' || json_extract(decoded_json, '$.channelHashHex')
	WHERE payload_type = 5 AND channel_hash IS NULL
	  AND json_extract(decoded_json, '$.type') = 'GRP_TXT';

-- +goose Down
DROP INDEX IF EXISTS idx_tx_channel_hash;
-- SQLite DROP COLUMN requires 3.35+; column left as documented no-op.