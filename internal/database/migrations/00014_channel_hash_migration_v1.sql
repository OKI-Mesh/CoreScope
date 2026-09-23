-- +goose Up

-- Backfill: extract channel name for decrypted (CHAN) packets.
UPDATE transmissions SET channel_hash = json_extract(decoded_json, '$.channel')
	WHERE payload_type = 5 AND channel_hash IS NULL
	  AND json_extract(decoded_json, '$.type') = 'CHAN';

-- Backfill: extract channelHashHex for encrypted (GRP_TXT) packets, prefixed 'enc_'.
UPDATE transmissions SET channel_hash = 'enc_' || json_extract(decoded_json, '$.channelHashHex')
	WHERE payload_type = 5 AND channel_hash IS NULL
	  AND json_extract(decoded_json, '$.type') = 'GRP_TXT';