-- 00004_advert_count_unique_v1.sql
-- +goose Up
-- NOTE: reconstructed. Current applySchema source uses
-- `t.from_pubkey = nodes.public_key`, but from_pubkey didn't exist yet
-- when this migration originally ran (confirmed via _migrations
-- ordering — predates from_pubkey_v1). Reconstructed per from_pubkey_v1's
-- own comment describing what it replaced ("unsound decoded_json LIKE
-- '%pubkey%' attribution path"). Verify against source history if exact
-- original WHERE clause matters.
UPDATE nodes SET advert_count = (
	SELECT COUNT(*) FROM transmissions t
	WHERE t.payload_type = 4
	  AND t.decoded_json LIKE '%' || nodes.public_key || '%'
);

-- +goose Down
-- Data-recalculation migration; no meaningful rollback.