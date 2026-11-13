package main

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func text(s string) *string {
	return &s
}

type loadedNode struct {
	ID            int
	PublicKey     string
	Name          *string
	Inactive      bool
	ForeignAdvert bool
	AdvertCount   int
	MultibyteSup  int
	FirstSeen     *string
	LastSeen      *string
}

func TestLoadNodes(t *testing.T) {
	ctx := context.Background()
	src := newSnapshot(t,
		`INSERT INTO nodes (public_key, name, role, last_seen, first_seen, advert_count, foreign_advert, multibyte_sup) VALUES
			('aa01', 'Alpha', 'repeater', '2026-03-01T00:00:00Z', '2026-01-02T00:00:00Z', 5, 0, 1),
			('bb02', NULL, NULL, '2026-03-02T00:00:00Z', 'not a time', 1, 1, 0),
			('CC03', 'Spam', 'type-7', '2026-03-03T00:00:00Z', '2026-03-03T00:00:00Z', 1, 0, 0)`,
		`INSERT INTO inactive_nodes (public_key, name, role, last_seen, first_seen, advert_count, foreign_advert, multibyte_sup) VALUES
			('aa01', 'Alpha (old)', 'repeater', '2025-12-01T00:00:00Z', '2026-01-01T00:00:00Z', 9, 0, 0),
			('dd04', 'Dormant', 'companion', '2025-11-01T00:00:00Z', '2025-10-01T00:00:00Z', 2, 0, 0),
			('ee05', NULL, 'type-3', '2025-11-01T00:00:00Z', '2025-10-01T00:00:00Z', 1, 0, 0)`)
	tx := beginTx(t, newTarget(t))
	report := newReport()

	loaded, err := loadNodes(ctx, src, tx, report)
	if err != nil {
		t.Fatalf("loadNodes: %v", err)
	}

	wantIDs := map[string]int32{"aa01": 1, "bb02": 2, "dd04": 3}
	if !reflect.DeepEqual(loaded.IDs, wantIDs) {
		t.Errorf("IDs = %v, want %v", loaded.IDs, wantIDs)
	}
	wantTypeN := map[string]bool{"cc03": true, "ee05": true}
	if !reflect.DeepEqual(loaded.TypeN, wantTypeN) {
		t.Errorf("TypeN = %v, want %v", loaded.TypeN, wantTypeN)
	}

	rows, err := tx.Query(ctx, `SELECT id, public_key, name, inactive, foreign_advert, advert_count, multibyte_sup,
		to_char(first_seen AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
		to_char(last_seen AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
		FROM nodes ORDER BY id`)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	defer rows.Close()
	var got []loadedNode
	for rows.Next() {
		var n loadedNode
		if err := rows.Scan(&n.ID, &n.PublicKey, &n.Name, &n.Inactive, &n.ForeignAdvert, &n.AdvertCount,
			&n.MultibyteSup, &n.FirstSeen, &n.LastSeen); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, n)
	}
	rows.Close()
	want := []loadedNode{
		{1, "aa01", text("Alpha"), false, false, 9, 1, text("2026-01-01T00:00:00Z"), text("2026-03-01T00:00:00Z")},
		{2, "bb02", nil, false, true, 1, 0, text("2026-03-02T00:00:00Z"), text("2026-03-02T00:00:00Z")},
		{3, "dd04", text("Dormant"), true, false, 2, 0, text("2025-10-01T00:00:00Z"), text("2025-11-01T00:00:00Z")},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("nodes = %+v\nwant   %+v", got, want)
	}

	var next int
	if err := tx.QueryRow(ctx, `INSERT INTO nodes (public_key, last_seen, first_seen) VALUES ('zz99', now(), now()) RETURNING id`).Scan(&next); err != nil {
		t.Fatalf("insert after load: %v", err)
	}
	if next != 4 {
		t.Errorf("next node id = %d, want 4 (sequence not reset)", next)
	}

	counts := map[string]int64{
		"nodes: key in both nodes and inactive_nodes, nodes row wins": 1,
		"nodes: skipped, type-N role":                                 1,
		"inactive_nodes: skipped, type-N role":                        1,
		"nodes: loaded active":                                        2,
		"nodes: loaded inactive":                                      1,
		"nodes.first_seen: unparseable timestamp, loaded as NULL":     1,
		"nodes: no first_seen, set to last_seen":                      1,
	}
	for name, want := range counts {
		if got := report.Count(name); got != want {
			t.Errorf("report %q = %d, want %d", name, got, want)
		}
	}
}

func TestLoadNodesStopsOnUnknownRole(t *testing.T) {
	src := newSnapshot(t,
		`INSERT INTO nodes (public_key, name, role) VALUES ('aa01', 'Odd', 'wizard')`)
	tx := beginTx(t, newTarget(t))

	_, err := loadNodes(context.Background(), src, tx, newReport())
	if err == nil || !strings.Contains(err.Error(), "nodes_role_valid") {
		t.Fatalf("err = %v, want one naming nodes_role_valid", err)
	}
}

func TestLoadNodesStopsOnKeysThatCollideAfterLowercasing(t *testing.T) {
	src := newSnapshot(t,
		`INSERT INTO nodes (public_key, name, role) VALUES ('AB12', 'Upper', 'repeater'), ('ab12', 'Lower', 'repeater')`)
	tx := beginTx(t, newTarget(t))

	_, err := loadNodes(context.Background(), src, tx, newReport())
	if err == nil || !strings.Contains(err.Error(), "appears twice") {
		t.Fatalf("err = %v, want a duplicate-key error", err)
	}
}

func TestLoadNodesFillsMissingTimes(t *testing.T) {
	ctx := context.Background()
	src := newSnapshot(t,
		`INSERT INTO nodes (public_key, name, role, last_seen, first_seen, advert_count, foreign_advert, multibyte_sup) VALUES
			('aa01', 'NoLast', 'repeater', NULL, '2026-01-02T00:00:00Z', 1, 0, 0),
			('bb02', 'NoFirst', 'repeater', '2026-03-04T00:00:00Z', NULL, 1, 0, 0),
			('cc03', 'Neither', 'repeater', NULL, NULL, 1, 0, 0)`)
	tx := beginTx(t, newTarget(t))
	report := newReport()

	if _, err := loadNodes(ctx, src, tx, report); err != nil {
		t.Fatalf("loadNodes: %v", err)
	}

	type times struct{ First, Last string }
	got := map[string]times{}
	rows, err := tx.Query(ctx, `SELECT public_key,
		to_char(first_seen AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
		to_char(last_seen AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') FROM nodes`)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		var seen times
		if err := rows.Scan(&key, &seen.First, &seen.Last); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[key] = seen
	}
	want := map[string]times{
		"aa01": {"2026-01-02T00:00:00Z", "2026-01-02T00:00:00Z"},
		"bb02": {"2026-03-04T00:00:00Z", "2026-03-04T00:00:00Z"},
		"cc03": {"1970-01-01T00:00:00Z", "1970-01-01T00:00:00Z"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("times = %v, want %v", got, want)
	}
	for name, wantCount := range map[string]int64{
		"nodes: no last_seen, set to first_seen":                   1,
		"nodes: no first_seen, set to last_seen":                   1,
		"nodes: no first_seen or last_seen, both set to the epoch": 1,
	} {
		if n := report.Count(name); n != wantCount {
			t.Errorf("report %q = %d, want %d", name, n, wantCount)
		}
	}
}
