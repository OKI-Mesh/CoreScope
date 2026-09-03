-- +goose Up
-- Never tracked in the legacy _migrations table (gated only by
-- CREATE TABLE IF NOT EXISTS); position here is best-effort, not
-- verified against real historical ordering.
CREATE TABLE IF NOT EXISTS neighbor_edges (
	node_a TEXT NOT NULL,
	node_b TEXT NOT NULL,
	count INTEGER DEFAULT 1,
	last_seen TEXT,
	PRIMARY KEY (node_a, node_b)
);

-- +goose Down
DROP TABLE IF EXISTS neighbor_edges;