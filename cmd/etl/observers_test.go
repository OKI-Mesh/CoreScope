package main

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

type loadedObserver struct {
	ID           int64
	Pubkey       string
	Name         *string
	IATA         *string
	FirstSeen    string
	LastSeen     string
	Inactive     bool
	CanRelay     bool
	CanRelaySeen bool
}

func TestLoadObservers(t *testing.T) {
	ctx := context.Background()
	src := newSnapshot(t,
		`INSERT INTO observers (rowid, id, name, iata, last_seen, first_seen, packet_count, inactive, can_relay, can_relay_seen) VALUES
			(1, 'ABCD', 'Alpha', 'CVG', '2026-04-01T00:00:00Z', '2026-01-01T00:00:00Z', 10, 0, 1, 0),
			(2, 'ef01', '', 'DAY', '2026-04-02T00:00:00Z', NULL, 0, NULL, NULL, 1),
			(7, '2345', 'Gamma', NULL, NULL, NULL, 3, 1, 0, 0)`,
		`INSERT INTO observations (transmission_id, observer_idx, timestamp) VALUES
			(1, 2, 1769904000),
			(2, 2, 1773532800),
			(3, 7, 0),
			(4, 7, 1770681600),
			(5, 7, 1773964800)`)
	tx := beginTx(t, newTarget(t))
	report := newReport()

	loaded, err := loadObservers(ctx, src, tx, report)
	if err != nil {
		t.Fatalf("loadObservers: %v", err)
	}
	wantPubkeys := map[int64]string{1: "abcd", 2: "ef01", 7: "2345"}
	if !reflect.DeepEqual(loaded.Pubkeys, wantPubkeys) {
		t.Errorf("Pubkeys = %v, want %v", loaded.Pubkeys, wantPubkeys)
	}

	rows, err := tx.Query(ctx, `SELECT id, pubkey, name, iata,
		to_char(first_seen AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
		to_char(last_seen AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
		inactive, can_relay, can_relay_seen
		FROM observers ORDER BY id`)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	defer rows.Close()
	var got []loadedObserver
	for rows.Next() {
		var o loadedObserver
		if err := rows.Scan(&o.ID, &o.Pubkey, &o.Name, &o.IATA, &o.FirstSeen, &o.LastSeen,
			&o.Inactive, &o.CanRelay, &o.CanRelaySeen); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, o)
	}
	rows.Close()
	want := []loadedObserver{
		{1, "abcd", text("Alpha"), text("CVG"), "2026-01-01T00:00:00Z", "2026-04-01T00:00:00Z", false, true, false},
		{2, "ef01", nil, text("DAY"), "2026-02-01T00:00:00Z", "2026-04-02T00:00:00Z", false, true, true},
		{7, "2345", text("Gamma"), nil, "2026-02-10T00:00:00Z", "2026-03-20T00:00:00Z", true, false, false},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("observers = %+v\nwant      %+v", got, want)
	}

	var next int64
	err = tx.QueryRow(ctx, `INSERT INTO observers (pubkey, last_seen, first_seen) VALUES ('new', now(), now()) RETURNING id`).Scan(&next)
	if err != nil {
		t.Fatalf("insert after load: %v", err)
	}
	if next != 8 {
		t.Errorf("next observer id = %d, want 8 (sequence not reset past rowid 7)", next)
	}

	counts := map[string]int64{
		"observers: loaded":        3,
		"observers: id lowercased": 1,
		"observers: NULL first_seen filled from earliest observation": 2,
		"observers: NULL last_seen filled from latest observation":    1,
	}
	for name, want := range counts {
		if got := report.Count(name); got != want {
			t.Errorf("report %q = %d, want %d", name, got, want)
		}
	}
}

func TestLoadObserversStopsOnCaseCollision(t *testing.T) {
	src := newSnapshot(t,
		`INSERT INTO observers (id, last_seen, first_seen) VALUES ('AB', '2026-04-01T00:00:00Z', '2026-04-01T00:00:00Z'), ('ab', '2026-04-01T00:00:00Z', '2026-04-01T00:00:00Z')`)
	tx := beginTx(t, newTarget(t))

	_, err := loadObservers(context.Background(), src, tx, newReport())
	if err == nil || !strings.Contains(err.Error(), "same pubkey") {
		t.Fatalf("err = %v, want a case-collision error", err)
	}
}

func TestLoadObserversStopsOnEmptyID(t *testing.T) {
	src := newSnapshot(t,
		`INSERT INTO observers (id, last_seen, first_seen) VALUES ('', '2026-04-01T00:00:00Z', '2026-04-01T00:00:00Z')`)
	tx := beginTx(t, newTarget(t))

	_, err := loadObservers(context.Background(), src, tx, newReport())
	if err == nil || !strings.Contains(err.Error(), "empty id") {
		t.Fatalf("err = %v, want an empty-id error", err)
	}
}

func TestLoadObserversStopsWhenTimesCannotBeInferred(t *testing.T) {
	src := newSnapshot(t,
		`INSERT INTO observers (id) VALUES ('ab12')`)
	tx := beginTx(t, newTarget(t))

	_, err := loadObservers(context.Background(), src, tx, newReport())
	if err == nil || !strings.Contains(err.Error(), "no first_seen or last_seen") {
		t.Fatalf("err = %v, want an uninferable-times error", err)
	}
}

func TestRequireEmptyTarget(t *testing.T) {
	ctx := context.Background()
	tx := beginTx(t, newTarget(t))

	if err := requireEmptyTarget(ctx, tx); err != nil {
		t.Fatalf("empty target rejected: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO nodes (public_key) VALUES ('aa01')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	err := requireEmptyTarget(ctx, tx)
	if err == nil || !strings.Contains(err.Error(), "nodes") {
		t.Fatalf("err = %v, want one naming the nodes table", err)
	}
}
