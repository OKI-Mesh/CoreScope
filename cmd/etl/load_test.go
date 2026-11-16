package main

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"
)

func seedEverything(t *testing.T) *Source {
	t.Helper()
	return newSnapshot(t,
		`INSERT INTO nodes (public_key, name, role) VALUES ('aa01', 'Alpha', 'repeater'), ('bb02', 'Beta', 'repeater')`,
		`INSERT INTO observers (rowid, id, name, iata, last_seen, first_seen) VALUES
			(1, 'OBS1', 'One', 'CVG', '2026-04-01T00:00:00Z', '2026-01-01T00:00:00Z'),
			(2, 'obs2', 'Two', 'DAY', '2026-04-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		`INSERT INTO transmissions (id, raw_hex, hash, first_seen, route_type, payload_type) VALUES
			(1, '010099', 'h1', '2026-03-01T00:00:00Z', 1, 0)`,
		`INSERT INTO observations (id, transmission_id, observer_idx, path_json, timestamp, raw_hex) VALUES
			(1, 1, 1, '["AA"]', 1773532800, '010199AA99'),
			(2, 1, 2, '[]', 1773532801, NULL)`,
		`INSERT INTO neighbor_edges (node_a, node_b, count, last_seen) VALUES
			('AA01', 'BB02', 3, '2026-03-01T00:00:00Z'),
			('aa01', 'bb02', 2, '2026-03-02T00:00:00Z'),
			('bb02', 'ffff', NULL, NULL)`,
		`INSERT INTO observer_metrics (observer_id, timestamp, noise_floor, battery_mv) VALUES
			('OBS1', '2026-03-01T00:00:00Z', -110.5, 4000),
			('obs1', '2026-03-01T00:00:00Z', -111, 4001),
			('OBS2', '2026-03-01T00:05:00Z', NULL, NULL),
			('ghost', '2026-03-01T00:00:00Z', NULL, NULL),
			('obs1', 'not a time', NULL, NULL)`,
		`INSERT INTO dropped_packets (id, hash, raw_hex, reason, observer_id, observer_name, node_pubkey, node_name, dropped_at) VALUES
			(5, '', NULL, 'bad', 'OBSX', '', 'AA01', '', '2026-03-01 00:00:00'),
			(9, 'h9', '0100', 'worse', NULL, 'Name', NULL, CAST(x'4162EFBF' AS TEXT), NULL)`,
		`INSERT INTO scope_match_totals VALUES (1, 1773532800, 5, 2, 1, 7, 1773619200)`)
}

func TestLoadEndToEnd(t *testing.T) {
	ctx := context.Background()
	src := seedEverything(t)
	tx := beginTx(t, newTarget(t))
	report := newReport()

	if err := load(ctx, src, tx, report); err != nil {
		t.Fatalf("load: %v", err)
	}

	counts := map[string]int64{
		"neighbor_edges: loaded":                                         2,
		"neighbor_edges: merged, keys collided after lowercasing":        1,
		"neighbor_edges: no last_seen, set to the epoch":                 1,
		"observer_metrics: loaded":                                       2,
		"observer_metrics: duplicate after lowercasing, dropped":         1,
		"observer_metrics: skipped, observer not loaded":                 1,
		"observer_metrics: skipped, unusable timestamp":                  1,
		"dropped_packets: loaded":                                        2,
		"dropped_packets: no dropped_at, set to the epoch":               1,
		"text cleaned (NUL or invalid UTF-8): dropped_packets.node_name": 1,
		"scope_match_totals: loaded":                                     1,
		"validation: tables reconciled with the source":                  int64(len(reconciliations)),
	}
	for name, want := range counts {
		if got := report.Count(name); got != want {
			t.Errorf("report %q = %d, want %d", name, got, want)
		}
	}

	var edgeCount int64
	var edgeSeen time.Time
	if err := tx.QueryRow(ctx, `SELECT count, last_seen FROM neighbor_edges WHERE node_a = 'aa01' AND node_b = 'bb02'`).Scan(&edgeCount, &edgeSeen); err != nil {
		t.Fatalf("merged edge: %v", err)
	}
	if edgeCount != 5 || !edgeSeen.Equal(time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("merged edge = (%d, %v), want (5, 2026-03-02)", edgeCount, edgeSeen)
	}
	var unknownSeen time.Time
	var unknownCount int64
	if err := tx.QueryRow(ctx, `SELECT count, last_seen FROM neighbor_edges WHERE node_b = 'ffff'`).Scan(&unknownCount, &unknownSeen); err != nil {
		t.Fatalf("unknown-endpoint edge: %v", err)
	}
	if unknownCount != 1 || !unknownSeen.Equal(epoch) {
		t.Errorf("edge with no data = (%d, %v), want (1, epoch)", unknownCount, unknownSeen)
	}

	type drop struct {
		ID                                     int64
		Hash, RawHex, ObserverID, ObserverName *string
		NodePubkey, NodeName                   *string
		Reason                                 string
	}
	rows, err := tx.Query(ctx, `SELECT id, hash, raw_hex, reason, observer_id, observer_name, node_pubkey, node_name FROM dropped_packets ORDER BY id`)
	if err != nil {
		t.Fatalf("dropped_packets: %v", err)
	}
	defer rows.Close()
	var drops []drop
	for rows.Next() {
		var d drop
		if err := rows.Scan(&d.ID, &d.Hash, &d.RawHex, &d.Reason, &d.ObserverID, &d.ObserverName, &d.NodePubkey, &d.NodeName); err != nil {
			t.Fatalf("scan drop: %v", err)
		}
		drops = append(drops, d)
	}
	rows.Close()
	wantDrops := []drop{
		{ID: 5, Reason: "bad", ObserverID: text("obsx"), NodePubkey: text("aa01")},
		{ID: 9, Hash: text("h9"), RawHex: text("0100"), Reason: "worse", ObserverName: text("Name"), NodeName: text("Ab\uFFFD")},
	}
	if !reflect.DeepEqual(drops, wantDrops) {
		t.Errorf("dropped_packets = %+v\nwant              %+v", drops, wantDrops)
	}
	var nextDrop int64
	if err := tx.QueryRow(ctx, `INSERT INTO dropped_packets (reason) VALUES ('x') RETURNING id`).Scan(&nextDrop); err != nil {
		t.Fatalf("insert after load: %v", err)
	}
	if nextDrop != 10 {
		t.Errorf("next dropped_packets id = %d, want 10", nextDrop)
	}

	var since, updated time.Time
	var other int64
	if err := tx.QueryRow(ctx, `SELECT counting_since, updated_at, none_matches FROM scope_match_totals`).Scan(&since, &updated, &other); err != nil {
		t.Fatalf("scope_match_totals: %v", err)
	}
	if !since.Equal(time.Unix(1773532800, 0)) || !updated.Equal(time.Unix(1773619200, 0)) || other != 7 {
		t.Errorf("scope_match_totals = (%v, %v, %d)", since, updated, other)
	}
}

func TestValidateCatchesMissingRows(t *testing.T) {
	ctx := context.Background()
	src := seedEverything(t)
	tx := beginTx(t, newTarget(t))
	report := newReport()
	if err := load(ctx, src, tx, report); err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM neighbor_edges WHERE node_b = 'ffff'`); err != nil {
		t.Fatalf("tamper: %v", err)
	}
	err := validate(ctx, src, tx, report)
	if err == nil || !strings.Contains(err.Error(), "neighbor_edges") {
		t.Fatalf("validate = %v, want an error naming neighbor_edges", err)
	}
}

func TestLoadStopsOnClientRows(t *testing.T) {
	src := newSnapshot(t, `INSERT INTO client_rf_samples (id, rx_pubkey) VALUES (1, 'aa')`)
	tx := beginTx(t, newTarget(t))

	err := load(context.Background(), src, tx, newReport())
	if err == nil || !strings.Contains(err.Error(), "client_rf_samples") {
		t.Fatalf("load = %v, want an error naming client_rf_samples", err)
	}
}
