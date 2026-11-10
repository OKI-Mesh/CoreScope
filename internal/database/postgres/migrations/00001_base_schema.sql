-- +goose Up
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS nodes (
    id                      INTEGER GENERATED ALWAYS AS IDENTITY UNIQUE,
    public_key              TEXT PRIMARY KEY CONSTRAINT nodes_public_key_lowercase CHECK (public_key = lower(public_key)),
    name                    TEXT,
    role                    TEXT CONSTRAINT nodes_role_valid CHECK (role IN ('repeater', 'companion', 'room', 'sensor')),
    lat                     DOUBLE PRECISION,
    lon                     DOUBLE PRECISION,
    last_seen               TIMESTAMPTZ NOT NULL,
    first_seen              TIMESTAMPTZ NOT NULL,
    advert_count            INTEGER NOT NULL DEFAULT 0,
    battery_mv              INTEGER,
    temperature_c           DOUBLE PRECISION,
    foreign_advert          BOOLEAN NOT NULL DEFAULT false,
    default_scope           TEXT,
    multibyte_sup           INTEGER NOT NULL DEFAULT 0 CONSTRAINT nodes_multibyte_sup_valid CHECK (multibyte_sup IN (0,1,2)),
    multibyte_evidence      TEXT,
    configured_scope        TEXT,
    configured_scope_at     TIMESTAMPTZ,
    inactive                BOOLEAN NOT NULL DEFAULT false
);

-- Surrogate id replaces SQLite's implicit rowid (which observer_idx used to
-- reference). Sequence-backed, so a deleted observer's id is never reissued.
-- pubkey is the real-world identifier (was `id TEXT PRIMARY KEY` in SQLite).
CREATE TABLE IF NOT EXISTS observers (
    id                     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pubkey                 TEXT NOT NULL UNIQUE CHECK (pubkey = lower(pubkey) AND pubkey <> ''),
    name                   TEXT,
    last_seen              TIMESTAMPTZ NOT NULL,
    first_seen             TIMESTAMPTZ NOT NULL,
    packet_count           BIGINT NOT NULL DEFAULT 0,
    model                  TEXT,
    firmware               TEXT,
    client_version         TEXT,
    radio                  TEXT,
    battery_mv             INTEGER,
    uptime_secs            BIGINT,
    noise_floor            DOUBLE PRECISION,
    inactive               BOOLEAN NOT NULL DEFAULT false,
    last_packet_at         TIMESTAMPTZ,
    raw_hex                TEXT,
    clock_skew_seconds     INTEGER,
    clock_skew_count_24h   INTEGER NOT NULL DEFAULT 0,
    clock_last_naive_at    TIMESTAMPTZ,
    iata                   TEXT CONSTRAINT observers_iata_normalized CHECK (iata = upper(btrim(iata)) AND iata <> ''),
    can_relay              BOOLEAN NOT NULL DEFAULT true,
    can_relay_seen         BOOLEAN NOT NULL DEFAULT false
);

-- transmission table and related indexes
-- route_type/payload_type: -1 is the explicit "unknown" sentinel (was NULL on
-- a *int in Go). from_pubkey is the full key of the originating node, set only
-- when the payload carries one; it goes NULL if that node is deleted.
-- last_seen is when the transmission was last heard and is never NULL: it
-- starts at first_seen and only moves forward.
CREATE TABLE IF NOT EXISTS transmissions (
    id                 BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    raw_hex            BYTEA NOT NULL,
    hash               TEXT NOT NULL UNIQUE,
    first_seen         TIMESTAMPTZ NOT NULL,
    route_type         INTEGER NOT NULL DEFAULT -1 CHECK (route_type IN (-1,0,1,2,3)),
    payload_type       INTEGER NOT NULL DEFAULT -1,
    payload_version    INTEGER NOT NULL DEFAULT 0,
    decoded_json       TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    from_pubkey        TEXT REFERENCES nodes(public_key) ON DELETE SET NULL,
    channel_hash       TEXT,
    channel_encrypted  BOOLEAN NOT NULL DEFAULT false,
    scope_name         TEXT,
    last_seen          TIMESTAMPTZ NOT NULL,
    code1              TEXT,
    code2              TEXT,
    CONSTRAINT tx_channel_encrypted_needs_hash CHECK (NOT channel_encrypted OR channel_hash IS NOT NULL),
    CONSTRAINT tx_scope_needs_transport_route CHECK (scope_name IS NULL OR route_type IN (0, 3))
);

CREATE INDEX IF NOT EXISTS idx_transmissions_first_seen ON transmissions (first_seen);
CREATE INDEX IF NOT EXISTS idx_transmissions_type_time  ON transmissions (payload_type, first_seen DESC);
CREATE INDEX IF NOT EXISTS idx_transmissions_from_pubkey_time ON transmissions (from_pubkey, first_seen DESC) WHERE from_pubkey IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_tx_channel ON transmissions (channel_hash, channel_encrypted, first_seen DESC) WHERE payload_type = 5;
CREATE INDEX IF NOT EXISTS idx_tx_scope_name   ON transmissions (scope_name)   WHERE scope_name IS NOT NULL;

-- Mobile client RX coverage: a roaming companion = a mobile observer with a
-- moving GPS position, so it gets its own table rather than observations.
CREATE TABLE IF NOT EXISTS client_receptions (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    rx_pubkey     TEXT NOT NULL,
    heard_key     TEXT NOT NULL,
    heard_keylen  INTEGER NOT NULL,
    rssi          INTEGER,
    snr           DOUBLE PRECISION,
    lat           DOUBLE PRECISION NOT NULL,
    lon           DOUBLE PRECISION NOT NULL,
    pos_acc_m     DOUBLE PRECISION,
    rx_at         TIMESTAMPTZ NOT NULL,
    ingested_at   TIMESTAMPTZ NOT NULL,
    src           TEXT NOT NULL,
    UNIQUE (rx_pubkey, heard_key, rx_at)
);

CREATE TABLE IF NOT EXISTS client_observers (
    pubkey     TEXT PRIMARY KEY,
    name       TEXT,
    last_seen  TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS observations (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transmission_id  BIGINT NOT NULL REFERENCES transmissions(id) ON DELETE CASCADE,
    observer_idx     BIGINT REFERENCES observers(id),
    direction        TEXT,
    snr              DOUBLE PRECISION,
    rssi             DOUBLE PRECISION,
    score            DOUBLE PRECISION,
    path_hash        TEXT,
    timestamp        TIMESTAMPTZ NOT NULL,
    hop_prefixes     TEXT[] NOT NULL DEFAULT '{}'
        CONSTRAINT observations_hop_prefixes_hex_lowercase CHECK (hop_prefixes::text ~ '^\{([0-9a-f]+(,[0-9a-f]+)*)?\}$'),
    hop_nodes        INTEGER[] NOT NULL DEFAULT '{}'
        CONSTRAINT observations_hop_nodes_length CHECK (cardinality(hop_nodes) IN (0, cardinality(hop_prefixes)))
);

CREATE TABLE IF NOT EXISTS observation_frames (
    observation_id  BIGINT PRIMARY KEY REFERENCES observations(id) ON DELETE CASCADE,
    frame           BYTEA NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_observations_hop_prefixes ON observations USING GIN (hop_prefixes);

CREATE INDEX IF NOT EXISTS idx_observations_timestamp
    ON observations (timestamp);
CREATE INDEX IF NOT EXISTS idx_observations_tx_ts
    ON observations (transmission_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_observations_observer_idx_timestamp
    ON observations (observer_idx, timestamp);
CREATE UNIQUE INDEX IF NOT EXISTS idx_observations_dedup
    ON observations (transmission_id, observer_idx, COALESCE(path_hash, '')) NULLS NOT DISTINCT;

CREATE TABLE IF NOT EXISTS observer_metrics (
    observer_id    TEXT NOT NULL REFERENCES observers (pubkey) ON DELETE CASCADE,
    timestamp      TIMESTAMPTZ NOT NULL,
    noise_floor    DOUBLE PRECISION,
    tx_air_secs    INTEGER,
    rx_air_secs    INTEGER,
    recv_errors    INTEGER,
    battery_mv     INTEGER,
    packets_sent   INTEGER,
    packets_recv   INTEGER,
    PRIMARY KEY (observer_id, timestamp)
);

CREATE TABLE IF NOT EXISTS dropped_packets (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    hash           TEXT,
    raw_hex        TEXT,
    reason         TEXT NOT NULL,
    observer_id    TEXT,
    observer_name  TEXT,
    node_pubkey    TEXT,
    node_name      TEXT,
    dropped_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dropped_packets_dropped_at ON dropped_packets (dropped_at DESC, id DESC);

-- Directed: A hearing B is tracked independently of B hearing A, since LoRa
-- links are routinely asymmetric.
CREATE TABLE IF NOT EXISTS neighbor_edges (
    node_a     TEXT NOT NULL,
    node_b     TEXT NOT NULL,
    count      INTEGER NOT NULL DEFAULT 1,
    last_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (node_a, node_b)
);

CREATE TABLE IF NOT EXISTS client_rx_observations (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    rx_pubkey      TEXT NOT NULL,
    rx_at          TIMESTAMPTZ NOT NULL,
    ingested_at    TIMESTAMPTZ NOT NULL,
    pkt_hash       TEXT NOT NULL,
    route_type     INTEGER NOT NULL,
    payload_type   INTEGER NOT NULL,
    code1          TEXT,
    code2          TEXT,
    scope_name     TEXT,
    hash_size      INTEGER NOT NULL,
    hop_count      INTEGER NOT NULL,
    path_json      TEXT,
    forwarder      TEXT,
    snr            DOUBLE PRECISION,
    rssi           INTEGER,
    lat            DOUBLE PRECISION NOT NULL,
    lon            DOUBLE PRECISION NOT NULL,
    pos_acc_m      DOUBLE PRECISION,
    UNIQUE (rx_pubkey, pkt_hash, rx_at)
);

CREATE TABLE IF NOT EXISTS node_declared_regions (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    target          TEXT NOT NULL,
    rx_pubkey       TEXT NOT NULL,
    observed_at     TIMESTAMPTZ NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL,
    regions_csv     TEXT NOT NULL,
    truncated       BOOLEAN NOT NULL DEFAULT false,
    lat             DOUBLE PRECISION,
    lon             DOUBLE PRECISION,
    pos_acc_m       DOUBLE PRECISION,
    repeater_clock  BIGINT,
    UNIQUE (target, rx_pubkey, observed_at)
);

-- One row per polygon. A region may be disjoint (multiple rows, same iata).
-- Uploaded as one GeoJSON per region; an upload replaces all rows for that
-- iata and is rejected if the new boundary overlaps another region's.
CREATE TABLE IF NOT EXISTS iata_boundaries (
    id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    iata      TEXT NOT NULL,
    boundary  geography(POLYGON, 4326) NOT NULL
);

CREATE TABLE IF NOT EXISTS client_rf_samples (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    rx_pubkey    TEXT NOT NULL,
    sampled_at   TIMESTAMPTZ NOT NULL,
    ingested_at  TIMESTAMPTZ NOT NULL,
    lat          DOUBLE PRECISION NOT NULL,
    lon          DOUBLE PRECISION NOT NULL,
    pos_acc_m    DOUBLE PRECISION,
    stationary   BOOLEAN NOT NULL DEFAULT false,
    uptime_secs  BIGINT NOT NULL,
    battery_mv   INTEGER, queue_len INTEGER, errors INTEGER,
    noise_floor  INTEGER, last_rssi INTEGER, last_snr DOUBLE PRECISION,
    tx_air_secs  BIGINT, rx_air_secs BIGINT, recv BIGINT, sent BIGINT,
    flood_rx     BIGINT, direct_rx BIGINT, flood_tx BIGINT, direct_tx BIGINT,
    recv_errors  BIGINT,
    UNIQUE (rx_pubkey, sampled_at)
);

CREATE TABLE IF NOT EXISTS scope_match_totals (
    id                    INTEGER PRIMARY KEY CHECK (id = 1),
    counting_since        TIMESTAMPTZ NOT NULL,
    unique_matches        BIGINT NOT NULL,
    explicit_over_derived BIGINT NOT NULL,
    ambiguous             BIGINT NOT NULL,
    none_matches          BIGINT NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_nodes_last_seen ON nodes(last_seen);
CREATE INDEX IF NOT EXISTS idx_observers_last_seen ON observers(last_seen);
CREATE INDEX IF NOT EXISTS idx_tx_activity ON transmissions (last_seen);
CREATE INDEX IF NOT EXISTS idx_client_recept_heard_geo ON client_receptions(heard_key, heard_keylen, lat, lon);
CREATE INDEX IF NOT EXISTS idx_client_recept_latlon ON client_receptions(lat, lon);
CREATE INDEX IF NOT EXISTS idx_client_recept_rxat ON client_receptions(rx_at);

CREATE INDEX IF NOT EXISTS idx_observer_metrics_timestamp ON observer_metrics(timestamp);
CREATE INDEX IF NOT EXISTS idx_dropped_observer ON dropped_packets(observer_id);
CREATE INDEX IF NOT EXISTS idx_dropped_node ON dropped_packets(node_pubkey);
CREATE INDEX IF NOT EXISTS idx_nodes_foreign_advert ON nodes(foreign_advert) WHERE foreign_advert = true;

CREATE INDEX IF NOT EXISTS idx_cro_prune ON client_rx_observations(rx_at);
CREATE INDEX IF NOT EXISTS idx_cro_hash ON client_rx_observations(pkt_hash, rx_at);
CREATE INDEX IF NOT EXISTS idx_cro_forwarder ON client_rx_observations(forwarder, rx_at);
CREATE INDEX IF NOT EXISTS idx_cro_scope ON client_rx_observations(scope_name, rx_at);
CREATE INDEX IF NOT EXISTS idx_ndr_target ON node_declared_regions(target, observed_at);
CREATE INDEX IF NOT EXISTS idx_ndr_prune ON node_declared_regions(observed_at);
CREATE INDEX IF NOT EXISTS idx_neighbor_edges_node_b ON neighbor_edges(node_b);
CREATE INDEX IF NOT EXISTS idx_neighbor_edges_last_seen ON neighbor_edges(last_seen);
CREATE INDEX IF NOT EXISTS idx_observers_iata ON observers(iata) WHERE iata IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_iata_boundaries_iata ON iata_boundaries(iata);
CREATE INDEX IF NOT EXISTS idx_iata_boundaries_boundary ON iata_boundaries USING GIST (boundary);
CREATE INDEX idx_crf_sampled_at ON client_rf_samples(sampled_at);


-- +goose Down
DROP TABLE IF EXISTS observation_frames;
DROP TABLE IF EXISTS observations;
DROP TABLE IF EXISTS observer_metrics;
DROP TABLE IF EXISTS dropped_packets;
DROP TABLE IF EXISTS neighbor_edges;
DROP TABLE IF EXISTS client_rx_observations;
DROP TABLE IF EXISTS client_rf_samples;
DROP TABLE IF EXISTS client_receptions;
DROP TABLE IF EXISTS client_observers;
DROP TABLE IF EXISTS node_declared_regions;
DROP TABLE IF EXISTS iata_boundaries;
DROP TABLE IF EXISTS scope_match_totals;
DROP TABLE IF EXISTS transmissions;
DROP TABLE IF EXISTS observers;
DROP TABLE IF EXISTS nodes;