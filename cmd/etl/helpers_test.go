package main

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5"
)

// sourceSchema is the part of the production SQLite schema the stage 1 loaders read.
const sourceSchema = `
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
	foreign_advert INTEGER DEFAULT 0,
	default_scope TEXT DEFAULT NULL,
	multibyte_sup INTEGER NOT NULL DEFAULT 0,
	multibyte_evidence TEXT,
	configured_scope TEXT,
	configured_scope_at TEXT
);
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
	foreign_advert INTEGER DEFAULT 0,
	default_scope TEXT DEFAULT NULL,
	multibyte_sup INTEGER NOT NULL DEFAULT 0,
	multibyte_evidence TEXT,
	configured_scope TEXT,
	configured_scope_at TEXT
);
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
	last_packet_at TEXT DEFAULT NULL,
	clock_skew_seconds INTEGER DEFAULT NULL,
	clock_skew_count_24h INTEGER DEFAULT 0,
	clock_last_naive_at TEXT DEFAULT NULL,
	can_relay INTEGER DEFAULT 1,
	can_relay_seen INTEGER DEFAULT 0
);
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
	created_at TEXT DEFAULT (datetime('now')),
	channel_hash TEXT DEFAULT NULL,
	scope_name TEXT DEFAULT NULL,
	last_seen INTEGER NOT NULL DEFAULT 0,
	code1 TEXT,
	code2 TEXT
);
CREATE TABLE observations (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	transmission_id INTEGER NOT NULL,
	observer_idx INTEGER,
	direction TEXT,
	snr REAL,
	rssi REAL,
	score INTEGER,
	path_json TEXT,
	timestamp INTEGER NOT NULL,
	resolved_path TEXT,
	raw_hex TEXT
);
CREATE TABLE neighbor_edges (
	node_a TEXT NOT NULL,
	node_b TEXT NOT NULL,
	count INTEGER DEFAULT 1,
	last_seen TEXT,
	PRIMARY KEY (node_a, node_b)
);
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
CREATE TABLE scope_match_totals (
	id INTEGER PRIMARY KEY CHECK (id = 1),
	since_unix INTEGER NOT NULL,
	unique_matches INTEGER NOT NULL,
	explicit_over_derived INTEGER NOT NULL,
	ambiguous INTEGER NOT NULL,
	none_matches INTEGER NOT NULL,
	updated_unix INTEGER NOT NULL
);
CREATE TABLE client_receptions (id INTEGER PRIMARY KEY, rx_pubkey TEXT);
CREATE TABLE client_observers (pubkey TEXT PRIMARY KEY, name TEXT);
CREATE TABLE client_rx_observations (id INTEGER PRIMARY KEY, rx_pubkey TEXT);
CREATE TABLE node_declared_regions (id INTEGER PRIMARY KEY, target TEXT);
CREATE TABLE client_rf_samples (id INTEGER PRIMARY KEY, rx_pubkey TEXT);
`

// newSnapshot builds a SQLite file with the production schema, runs the given
// INSERT statements against it, and opens it the way the ETL does.
func newSnapshot(t *testing.T, inserts ...string) *Source {
	t.Helper()
	path := filepath.Join(t.TempDir(), "snapshot.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	if _, err := db.Exec(sourceSchema); err != nil {
		t.Fatalf("snapshot schema: %v", err)
	}
	for _, statement := range inserts {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("seed snapshot: %v\n%s", err, statement)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close snapshot: %v", err)
	}
	src, err := openSource(path)
	if err != nil {
		t.Fatalf("open snapshot: %v", err)
	}
	t.Cleanup(func() { src.Close() })
	return src
}

// beginTx opens a transaction on the target and rolls it back when the test ends.
func beginTx(t *testing.T, dsn string) pgx.Tx {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to target: %v", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		conn.Close(ctx)
		t.Fatalf("begin: %v", err)
	}
	t.Cleanup(func() {
		tx.Rollback(ctx)
		conn.Close(ctx)
	})
	return tx
}
