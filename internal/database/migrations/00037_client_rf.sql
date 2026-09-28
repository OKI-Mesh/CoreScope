-- +goose Up
-- RF environment samples from mobile clients: radio counters paired with
-- a GPS point. Absolutes only — deltas are computed at query time, and a
-- decrease in uptime_secs (reboot) or any counter (wrap) breaks the chain.
CREATE TABLE IF NOT EXISTS client_rf_samples (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    rx_pubkey    TEXT NOT NULL,
    sampled_at   TEXT NOT NULL,
    ingested_at  TEXT NOT NULL,
    lat          REAL NOT NULL,
    lon          REAL NOT NULL,
    pos_acc_m    REAL,
    stationary   INTEGER NOT NULL DEFAULT 0,
    uptime_secs  INTEGER NOT NULL,
    battery_mv   INTEGER,
    queue_len    INTEGER,
    errors       INTEGER,
    noise_floor  INTEGER,
    last_rssi    INTEGER,
    last_snr     REAL,
    tx_air_secs  INTEGER,
    rx_air_secs  INTEGER,
    recv         INTEGER,
    sent         INTEGER,
    flood_rx     INTEGER,
    direct_rx    INTEGER,
    flood_tx     INTEGER,
    direct_tx    INTEGER,
    recv_errors  INTEGER,
    UNIQUE(rx_pubkey, sampled_at)
);
CREATE INDEX IF NOT EXISTS idx_crf_prune ON client_rf_samples(sampled_at);
CREATE INDEX IF NOT EXISTS idx_crf_track ON client_rf_samples(rx_pubkey, sampled_at);

-- +goose Down
DROP INDEX idx_crf_prune;
DROP INDEX idx_crf_track;
DROP TABLE client_rf_samples;
