-- +goose Up
-- Diagnostic RF observations from mobile clients. Unlike client_receptions
-- this holds EVERY decodable packet, attributable or not, so it must never
-- be used for coverage. pkt_hash is ComputeContentHash() — identical to
-- transmissions.hash — so dark-traffic queries are a plain equality join.
CREATE TABLE IF NOT EXISTS client_rx_observations (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    rx_pubkey     TEXT NOT NULL,
    rx_at         TEXT NOT NULL,
    ingested_at   TEXT NOT NULL,
    pkt_hash      TEXT NOT NULL,
    route_type    INTEGER NOT NULL,
    payload_type  INTEGER NOT NULL,
    code1         TEXT,
    code2         TEXT,
    scope_name    TEXT,
    hash_size     INTEGER NOT NULL,
    hop_count     INTEGER NOT NULL,
    path_json     TEXT,
    forwarder     TEXT,
    snr           REAL,
    rssi          INTEGER,
    lat           REAL NOT NULL,
    lon           REAL NOT NULL,
    pos_acc_m     REAL,
    UNIQUE(rx_pubkey, pkt_hash, rx_at)
);
CREATE INDEX IF NOT EXISTS idx_cro_prune     ON client_rx_observations(rx_at);
CREATE INDEX IF NOT EXISTS idx_cro_hash      ON client_rx_observations(pkt_hash, rx_at);
CREATE INDEX IF NOT EXISTS idx_cro_forwarder ON client_rx_observations(forwarder, rx_at);
CREATE INDEX IF NOT EXISTS idx_cro_scope     ON client_rx_observations(scope_name, rx_at);

-- +goose Down
DROP INDEX IF EXISTS idx_cro_prune;
DROP INDEX IF EXISTS idx_cro_hash;
DROP INDEX IF EXISTS idx_cro_forwarder;
DROP INDEX IF EXISTS idx_cro_scope;
DROP TABLE IF EXISTS client_rx_observations;