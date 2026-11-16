package main

import (
	"context"
	"reflect"
	"testing"

	"github.com/OKI-Mesh/CoreScope/internal/types"
)

func TestParseHops(t *testing.T) {
	cases := []struct {
		name   string
		input  string
		want   types.HopList
		wantOK bool
	}{
		{"uppercase is lowercased", `["3F21","CAC7"]`, types.HopList{"3f21", "cac7"}, true},
		{"already lowercase", `["aa"]`, types.HopList{"aa"}, true},
		{"empty array", `[]`, types.HopList{}, true},
		{"not hex", `["ZZ"]`, nil, false},
		{"empty hop", `["aa",""]`, nil, false},
		{"not an array", `{"a":1}`, nil, false},
		{"not json", `aa,bb`, nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := parseHops(c.input)
			if ok != c.wantOK || !reflect.DeepEqual(got, c.want) {
				t.Errorf("parseHops(%s) = (%v, %v), want (%v, %v)", c.input, got, ok, c.want, c.wantOK)
			}
		})
	}
}

func TestHopNodeIDs(t *testing.T) {
	nodes := &LoadedNodes{IDs: map[string]int32{"aa01": 1, "bb02": 2}}
	cases := []struct {
		name        string
		resolved    string
		hops        int
		want        []int32
		wantProblem bool
	}{
		{"empty", "", 2, nil, false},
		{"some resolved", `["AA01",null]`, 2, []int32{1, 0}, false},
		{"unknown key counts as unresolved", `["ffff","bb02"]`, 2, []int32{0, 2}, false},
		{"nothing resolved", `[null,"ffff"]`, 2, nil, false},
		{"length differs", `["aa01"]`, 2, nil, true},
		{"not json", `aa01`, 1, nil, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, problem := hopNodeIDs(c.resolved, c.hops, nodes)
			if !reflect.DeepEqual(got, c.want) || (problem != "") != c.wantProblem {
				t.Errorf("hopNodeIDs = (%v, %q), want (%v, problem %v)", got, problem, c.want, c.wantProblem)
			}
		})
	}
}

func TestLoadObservations(t *testing.T) {
	ctx := context.Background()
	src := newSnapshot(t,
		`INSERT INTO nodes (public_key, name, role) VALUES ('aa01', 'Alpha', 'repeater'), ('bb02', 'Beta', 'repeater')`,
		`INSERT INTO observers (rowid, id, name, iata, last_seen, first_seen) VALUES
			(1, 'OBS1', 'One', 'CVG', '2026-04-01T00:00:00Z', '2026-01-01T00:00:00Z'),
			(2, 'obs2', 'Two', 'DAY', '2026-04-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		`INSERT INTO transmissions (id, raw_hex, hash, first_seen, route_type, payload_type) VALUES
			(1, '010099', 'h1', '2026-03-01T00:00:00Z', 1, 0),
			(2, '010099', 'h2', '2026-03-01T00:00:01Z', 1, 0)`,
		`INSERT INTO observations (id, transmission_id, observer_idx, direction, snr, rssi, score, path_json, timestamp, resolved_path, raw_hex) VALUES
			(1, 1, 1, 'RX', 5.5, -90, 3, '["AA","BB"]', 1773532800, '["aa01",null]', '0102AABB99'),
			(2, 1, 1, NULL, NULL, NULL, NULL, '["aa","bb"]', 1773532801, NULL, '0102AABB99'),
			(3, 1, 2, NULL, NULL, NULL, NULL, '[]', 1773532802, NULL, '0102CCDD99'),
			(4, 1, NULL, NULL, NULL, NULL, NULL, NULL, 1773532803, NULL, '010099'),
			(5, 2, 99, NULL, NULL, NULL, NULL, '["ZZ"]', 1773532804, NULL, NULL),
			(6, 77, 1, NULL, NULL, NULL, NULL, '[]', 1773532805, NULL, NULL),
			(7, 2, 1, NULL, NULL, NULL, NULL, '[]', 0, NULL, NULL),
			(8, 2, 1, NULL, NULL, NULL, NULL, '["aa"]', 1773532806, '["bb02","aa01"]', NULL),
			(9, 2, 2, NULL, NULL, NULL, NULL, '["aa"]', 1773532807, '["bb02"]', NULL)`)
	tx := beginTx(t, newTarget(t))
	report := newReport()

	nodes, err := loadNodes(ctx, src, tx, report)
	if err != nil {
		t.Fatalf("loadNodes: %v", err)
	}
	observers, err := loadObservers(ctx, src, tx, report)
	if err != nil {
		t.Fatalf("loadObservers: %v", err)
	}
	transmissions, err := loadTransmissions(ctx, src, tx, nodes, report)
	if err != nil {
		t.Fatalf("loadTransmissions: %v", err)
	}
	if err := loadObservations(ctx, src, tx, nodes, observers, transmissions, report); err != nil {
		t.Fatalf("loadObservations: %v", err)
	}

	type loaded struct {
		ID, TransmissionID int64
		Observer           *int64
		HopPrefixes        []string
		HopNodes           []int32
		HasPathHash        bool
	}
	rows, err := tx.Query(ctx, `SELECT id, transmission_id, observer_idx, hop_prefixes, hop_nodes, path_hash IS NOT NULL
		FROM observations ORDER BY id`)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	defer rows.Close()
	var got []loaded
	for rows.Next() {
		var o loaded
		if err := rows.Scan(&o.ID, &o.TransmissionID, &o.Observer, &o.HopPrefixes, &o.HopNodes, &o.HasPathHash); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, o)
	}
	rows.Close()
	one, two := int64(1), int64(2)
	want := []loaded{
		{1, 1, &one, []string{"aa", "bb"}, []int32{1, 0}, true},
		{3, 1, &two, []string{"cc", "dd"}, []int32{}, true},
		{4, 1, nil, []string{}, []int32{}, false},
		{5, 2, nil, []string{}, []int32{}, false},
		{8, 2, &one, []string{"aa"}, []int32{}, true},
		{9, 2, &two, []string{"aa"}, []int32{2}, true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("observations = %+v\nwant          %+v", got, want)
	}

	var hash string
	if err := tx.QueryRow(ctx, `SELECT path_hash FROM observations WHERE id = 1`).Scan(&hash); err != nil {
		t.Fatalf("path_hash: %v", err)
	}
	if want := (types.HopList{"aa", "bb"}).Hash(); hash != want {
		t.Errorf("path_hash = %s, want %s", hash, want)
	}

	var frames []int64
	frameRows, err := tx.Query(ctx, `SELECT observation_id FROM observation_frames ORDER BY 1`)
	if err != nil {
		t.Fatalf("frames: %v", err)
	}
	for frameRows.Next() {
		var id int64
		if err := frameRows.Scan(&id); err != nil {
			t.Fatalf("scan frame: %v", err)
		}
		frames = append(frames, id)
	}
	frameRows.Close()
	if !reflect.DeepEqual(frames, []int64{1, 3}) {
		t.Errorf("frames for observations %v, want [1 3]", frames)
	}

	var next int64
	if err := tx.QueryRow(ctx, `SELECT nextval(pg_get_serial_sequence('observations', 'id'))`).Scan(&next); err != nil {
		t.Fatalf("sequence: %v", err)
	}
	if next != 10 {
		t.Errorf("next observation id = %d, want 10", next)
	}

	counts := map[string]int64{
		"observations: loaded": 6,
		"observations: duplicate of a lower id after normalising, dropped":               1,
		"observations: skipped, transmission missing":                                    1,
		"observations: skipped, no usable timestamp":                                     1,
		"observations: observer_idx matches no observer, set NULL":                       1,
		"observations: path_json unusable, loaded with no hops":                          1,
		"observations: resolved_path length differs from the path, hop_nodes left empty": 1,
		"observations: path derived from raw_hex":                                        1,
		"observations: frame stored (differs from the transmission)":                     2,
	}
	for name, want := range counts {
		if got := report.Count(name); got != want {
			t.Errorf("report %q = %d, want %d", name, got, want)
		}
	}
}
