-- +goose Up
-- #793: signature validation failures
CREATE TABLE IF NOT EXISTS dropped_packets (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	hash TEXT,
	raw_hex TEXT,
	reason TEXT NOT NULL,
	observer_id TEXT,
	observer_name TEXT,
	node_pubkey TEXT,
	node_name TEXT,
	dropped_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_dropped_observer ON dropped_packets(observer_id);
CREATE INDEX IF NOT EXISTS idx_dropped_node ON dropped_packets(node_pubkey);

-- +goose Down
DROP INDEX IF EXISTS idx_dropped_observer;
DROP INDEX IF EXISTS idx_dropped_node;
DROP TABLE IF EXISTS dropped_packets;
