CREATE TABLE nodes (
			public_key TEXT PRIMARY KEY,
			name TEXT,
			role TEXT,
			lat REAL,
			lon REAL,
			last_seen TEXT,
			first_seen TEXT,
			advert_count INTEGER DEFAULT 0,
			battery_mv INTEGER,
			temperature_c REAL,
			foreign_advert INTEGER DEFAULT 0
		, default_scope TEXT DEFAULT NULL, multibyte_sup INTEGER NOT NULL DEFAULT 0, multibyte_evidence TEXT);
CREATE TABLE observers (
			id TEXT PRIMARY KEY,
			name TEXT,
			iata TEXT,
			last_seen TEXT,
			first_seen TEXT,
			packet_count INTEGER DEFAULT 0,
			model TEXT,
			firmware TEXT,
			client_version TEXT,
			radio TEXT,
			battery_mv INTEGER,
			uptime_secs INTEGER,
			noise_floor REAL,
			inactive INTEGER DEFAULT 0,
			last_packet_at TEXT DEFAULT NULL
		, clock_skew_seconds INTEGER DEFAULT NULL, clock_skew_count_24h INTEGER DEFAULT 0, clock_last_naive_at TEXT DEFAULT NULL, can_relay INTEGER DEFAULT 1, can_relay_seen INTEGER DEFAULT 0);
CREATE INDEX idx_nodes_last_seen ON nodes(last_seen);
CREATE INDEX idx_observers_last_seen ON observers(last_seen);
CREATE TABLE inactive_nodes (
			public_key TEXT PRIMARY KEY,
			name TEXT,
			role TEXT,
			lat REAL,
			lon REAL,
			last_seen TEXT,
			first_seen TEXT,
			advert_count INTEGER DEFAULT 0,
			battery_mv INTEGER,
			temperature_c REAL,
			foreign_advert INTEGER DEFAULT 0
		, default_scope TEXT DEFAULT NULL, multibyte_sup INTEGER NOT NULL DEFAULT 0, multibyte_evidence TEXT);
CREATE INDEX idx_inactive_nodes_last_seen ON inactive_nodes(last_seen);
CREATE TABLE transmissions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			raw_hex TEXT NOT NULL,
			hash TEXT NOT NULL UNIQUE,
			first_seen TEXT NOT NULL,
			route_type INTEGER,
			payload_type INTEGER,
			payload_version INTEGER,
			decoded_json TEXT,
			from_pubkey TEXT,
			created_at TEXT DEFAULT (datetime('now'))
		, channel_hash TEXT DEFAULT NULL, scope_name TEXT DEFAULT NULL, last_seen INTEGER NOT NULL DEFAULT 0);
CREATE INDEX idx_transmissions_hash ON transmissions(hash);
CREATE INDEX idx_transmissions_first_seen ON transmissions(first_seen);
CREATE INDEX idx_transmissions_payload_type ON transmissions(payload_type);
CREATE TABLE observations (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				transmission_id INTEGER NOT NULL REFERENCES transmissions(id),
				observer_idx INTEGER,
				direction TEXT,
				snr REAL,
				rssi REAL,
				score INTEGER,
				path_json TEXT,
				timestamp INTEGER NOT NULL
			, resolved_path TEXT, raw_hex TEXT);
CREATE INDEX idx_observations_transmission_id ON observations(transmission_id);
CREATE INDEX idx_observations_observer_idx ON observations(observer_idx);
CREATE INDEX idx_observations_timestamp ON observations(timestamp);
CREATE UNIQUE INDEX idx_observations_dedup ON observations(transmission_id, observer_idx, COALESCE(path_json, ''));
CREATE TABLE _migrations (name TEXT PRIMARY KEY);
CREATE TABLE observer_metrics (
				observer_id TEXT NOT NULL,
				timestamp TEXT NOT NULL,
				noise_floor REAL,
				tx_air_secs INTEGER,
				rx_air_secs INTEGER,
				recv_errors INTEGER,
				battery_mv INTEGER, packets_sent INTEGER, packets_recv INTEGER,
				PRIMARY KEY (observer_id, timestamp)
			);
CREATE INDEX idx_observer_metrics_timestamp ON observer_metrics(timestamp);
CREATE INDEX idx_tx_channel_hash ON transmissions(channel_hash) WHERE payload_type = 5;
CREATE TABLE dropped_packets (
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
CREATE INDEX idx_dropped_observer ON dropped_packets(observer_id);
CREATE INDEX idx_dropped_node ON dropped_packets(node_pubkey);
CREATE INDEX idx_nodes_foreign_advert ON nodes(foreign_advert) WHERE foreign_advert = 1;
CREATE INDEX idx_transmissions_from_pubkey ON transmissions(from_pubkey);
CREATE INDEX idx_observations_tx_ts ON observations(transmission_id, timestamp);
CREATE TABLE neighbor_edges (
		node_a TEXT NOT NULL,
		node_b TEXT NOT NULL,
		count INTEGER DEFAULT 1,
		last_seen TEXT,
		PRIMARY KEY (node_a, node_b)
	);
CREATE INDEX idx_tx_scope_name ON transmissions(scope_name) WHERE scope_name IS NOT NULL;
CREATE TABLE _async_migrations (
			name       TEXT PRIMARY KEY,
			status     TEXT NOT NULL,             -- pending_async | done | failed
			started_at TEXT NOT NULL DEFAULT (datetime('now')),
			ended_at   TEXT,
			error      TEXT
		);
CREATE INDEX idx_observations_observer_idx_timestamp ON observations(observer_idx, timestamp);
CREATE TABLE client_receptions (
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
CREATE INDEX idx_client_recept_heard_geo ON client_receptions(heard_key, heard_keylen, lat, lon);
CREATE INDEX idx_client_recept_latlon ON client_receptions(lat, lon);
CREATE INDEX idx_client_recept_rxat ON client_receptions(rx_at);
CREATE TABLE client_observers (
			pubkey    TEXT PRIMARY KEY,
			name      TEXT,
			last_seen TEXT
		);
CREATE INDEX idx_tx_last_seen_zero ON transmissions(id) WHERE last_seen=0;
CREATE VIEW packets_v AS
			SELECT o.id, COALESCE(o.raw_hex, t.raw_hex) AS raw_hex,
				   datetime(o.timestamp, 'unixepoch') AS timestamp,
				   obs.id AS observer_id, obs.name AS observer_name,
				   o.direction, o.snr, o.rssi, o.score, t.hash, t.route_type,
				   t.payload_type, t.payload_version, o.path_json, t.decoded_json,
				   t.created_at
			FROM observations o
			JOIN transmissions t ON t.id = o.transmission_id
			LEFT JOIN observers obs ON obs.rowid = o.observer_idx AND (obs.inactive IS NULL OR obs.inactive = 0)
/* packets_v(id,raw_hex,timestamp,observer_id,observer_name,direction,snr,rssi,score,hash,route_type,payload_type,payload_version,path_json,decoded_json,created_at) */;
CREATE TABLE goose_db_version (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		version_id INTEGER NOT NULL,
		is_applied INTEGER NOT NULL,
		tstamp TIMESTAMP DEFAULT (datetime('now'))
	);
