-- +goose Up
-- #1290: firmware 1.16 `repeat: on|off` flag. #1624 MAJOR-2 follow-up:
-- can_relay_seen distinguishes "confirmed" from "legacy, field never sent".
ALTER TABLE observers ADD COLUMN can_relay INTEGER DEFAULT 1;
ALTER TABLE observers ADD COLUMN can_relay_seen INTEGER DEFAULT 0;

-- +goose Down
-- SQLite DROP COLUMN requires 3.35+; columns left as documented no-op.