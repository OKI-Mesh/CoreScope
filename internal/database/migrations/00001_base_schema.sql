-- +goose Up
CREATE TABLE IF NOT EXISTS nodes (
	public_key TEXT PRIMARY KEY,
	name TEXT,
	role TEXT,
	lat REAL,
	lon REAL,
	last_seen TEXT,
	first_seen TEXT,
	advert_count INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS observers (
	id TEXT PRIMARY KEY,
	name TEXT,
	last_seen TEXT,
	first_seen TEXT,
	packet_count INTEGER DEFAULT 0,
	model TEXT,
	firmware TEXT,
	client_version TEXT,
	radio TEXT,
	battery_mv INTEGER,
	uptime_secs INTEGER,
	noise_floor REAL
);
CREATE INDEX IF NOT EXISTS idx_nodes_last_seen ON nodes(last_seen);
CREATE INDEX IF NOT EXISTS idx_observers_last_seen ON observers(last_seen);

CREATE TABLE IF NOT EXISTS inactive_nodes (
	public_key TEXT PRIMARY KEY,
	name TEXT,
	role TEXT,
	lat REAL,
	lon REAL,
	last_seen TEXT,
	first_seen TEXT,
	advert_count INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_inactive_nodes_last_seen ON inactive_nodes(last_seen);

CREATE TABLE IF NOT EXISTS transmissions (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	raw_hex TEXT NOT NULL,
	hash TEXT NOT NULL UNIQUE,
	first_seen TEXT NOT NULL,
	route_type INTEGER,
	payload_type INTEGER,
	payload_version INTEGER,
	decoded_json TEXT,
	created_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_transmissions_hash ON transmissions(hash);
CREATE INDEX IF NOT EXISTS idx_transmissions_first_seen ON transmissions(first_seen);
CREATE INDEX IF NOT EXISTS idx_transmissions_payload_type ON transmissions(payload_type);

-- Mobile client RX coverage: a roaming companion = a mobile observer with a
-- moving GPS position, so it gets its own table rather than observations.
CREATE TABLE IF NOT EXISTS client_receptions (
	id            INTEGER PRIMARY KEY AUTOINCREMENT,
	rx_pubkey     TEXT NOT NULL,
	heard_key     TEXT NOT NULL,
	heard_keylen  INTEGER NOT NULL,
	rssi          INTEGER,
	snr           REAL,
	lat           REAL NOT NULL,
	lon           REAL NOT NULL,
	pos_acc_m     REAL,
	rx_at         TEXT NOT NULL,
	ingested_at   TEXT NOT NULL,
	src           TEXT NOT NULL,
	UNIQUE(rx_pubkey, heard_key, rx_at)
);
CREATE INDEX IF NOT EXISTS idx_client_recept_heard_geo ON client_receptions(heard_key, heard_keylen, lat, lon);
CREATE INDEX IF NOT EXISTS idx_client_recept_latlon ON client_receptions(lat, lon);
CREATE INDEX IF NOT EXISTS idx_client_recept_rxat ON client_receptions(rx_at);

-- Self-reported name of each mobile client (companion), from SELF_INFO.
CREATE TABLE IF NOT EXISTS client_observers (
	pubkey    TEXT PRIMARY KEY,
	name      TEXT,
	last_seen TEXT
);

-- +goose Down
DROP TABLE IF EXISTS client_observers;
DROP TABLE IF EXISTS client_receptions;
DROP TABLE IF EXISTS transmissions;
DROP TABLE IF EXISTS inactive_nodes;
DROP TABLE IF EXISTS observers;
DROP TABLE IF EXISTS nodes;