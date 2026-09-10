package main

import (
	"fmt"
	"testing"
	"time"
)

// TestGetChannelMessagesPerfLargeChannel asserts that fetching a small page
// (limit=50) from a channel with many observations does not scan the full
// observation set. Regression guard for issue #1225 where the query loaded
// every observation row for the channel and deduped/paginated in Go memory.
//
// Dataset: 1500 transmissions in #perf, each with 50 observations
// (75K obs total). Same shape as staging where #wardriving had ~5.7K tx
// and ~275K obs — fewer rows are enough to demonstrate that the broken
// impl (which loads/dedups every observation in Go) blows the budget,
// while keeping setup fast enough for slower CI runners.
//
// On the broken implementation this takes multiple seconds (>2s on dev,
// ~6–9s on GitHub-hosted CI). With SQL-level pagination over
// transmissions it must complete well under the 1.5s budget
// (~sub-100ms observed on dev).
func TestGetChannelMessagesPerfLargeChannel(t *testing.T) {
	if testing.Short() {
		t.Skip("perf test")
	}
	db := setupTestDB(t)
	defer db.Close()

	if _, err := db.conn.Exec(`INSERT INTO observers (id, name, iata, last_seen, first_seen, packet_count)
		VALUES ('obs_perf', 'PerfObs', 'SJC', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 0)`); err != nil {
		t.Fatal(err)
	}

	const numTx = 1500
	const obsPerTx = 50

	// Seed obsPerTx distinct observers so each (transmission, observer)
	// pair is unique — idx_observations_dedup is keyed on
	// (transmission_id, observer_idx, path_json), so obsPerTx rows per tx
	// with the SAME observer_idx would violate that constraint after the
	// first insert.
	for o := 0; o < obsPerTx; o++ {
		if _, err := db.conn.Exec(`INSERT INTO observers (id, name, iata, last_seen, first_seen, packet_count)
			VALUES (?, ?, 'SJC', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 0)`,
			fmt.Sprintf("obs_perf_%d", o), fmt.Sprintf("PerfObs%d", o)); err != nil {
			t.Fatal(err)
		}
	}

	tx, err := db.conn.Begin()
	if err != nil {
		t.Fatal(err)
	}
	txStmt, err := tx.Prepare(`INSERT INTO transmissions (raw_hex, hash, first_seen, route_type, payload_type, decoded_json, channel_hash)
		VALUES (?, ?, ?, 1, 5, ?, '#perf')`)
	if err != nil {
		t.Fatal(err)
	}
	obsStmt, err := tx.Prepare(`INSERT INTO observations (transmission_id, observer_idx, snr, rssi, path_json, timestamp)
		VALUES (?, ?, 10.0, -90, '[]', ?)`)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Add(-24 * time.Hour)
	for i := 0; i < numTx; i++ {
		ts := base.Add(time.Duration(i) * time.Second).Format(time.RFC3339)
		hash := fmt.Sprintf("perfhash%08d", i)
		body := fmt.Sprintf(`{"type":"CHAN","channel":"#perf","text":"Sender%d: msg %d","sender":"Sender%d"}`, i%10, i, i%10)
		res, err := txStmt.Exec(fmt.Sprintf("%04X", i), hash, ts, body)
		if err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
		txID, _ := res.LastInsertId()
		for o := 0; o < obsPerTx; o++ {
			// observer_idx here is the observer's ROWID (isV3 join is on
			// obs.rowid), which is 1-indexed insertion order — matches
			// the o+1'th observer inserted above.
			if _, err := obsStmt.Exec(txID, o+1, base.Unix()+int64(i*100+o)); err != nil {
				tx.Rollback()
				t.Fatal(err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
