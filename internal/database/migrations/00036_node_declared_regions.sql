-- +goose Up
-- Declared region lists reported by repeaters via ANON_REQ_TYPE_REGIONS.
-- Observations, never state: "current" is the greatest observed_at for a
-- target, NOT the greatest ingested_at — a drive buffered offline can
-- arrive days late and must not overwrite a fresher reading.
CREATE TABLE IF NOT EXISTS node_declared_regions (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    target         TEXT NOT NULL,
    rx_pubkey      TEXT NOT NULL,
    observed_at    TEXT NOT NULL,
    ingested_at    TEXT NOT NULL,
    regions_csv    TEXT NOT NULL,
    truncated      INTEGER NOT NULL DEFAULT 0,
    lat            REAL,
    lon            REAL,
    pos_acc_m      REAL,
    repeater_clock INTEGER,
    UNIQUE(target, rx_pubkey, observed_at)
);
CREATE INDEX IF NOT EXISTS idx_ndr_target ON node_declared_regions(target, observed_at);
CREATE INDEX IF NOT EXISTS idx_ndr_prune  ON node_declared_regions(observed_at);

-- +goose Down
DROP INDEX idx_ndr_target;
DROP INDEX idx_ndr_prune;
DROP TABLE node_declared_regions;
