package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type metricKey struct {
	observer string
	at       time.Time
}

// loadObserverMetrics streams the table, which is large. observer_id is
// lowercased to match observers.pubkey, and a row whose observer was not loaded
// is skipped because the foreign key would reject it.
func loadObserverMetrics(ctx context.Context, src *Source, tx pgx.Tx, observers *LoadedObservers, report *Report) error {
	known := make(map[string]bool, len(observers.Pubkeys))
	for _, pubkey := range observers.Pubkeys {
		known[pubkey] = true
	}

	rows, err := src.db.QueryContext(ctx, `SELECT observer_id, timestamp, noise_floor, tx_air_secs, rx_air_secs,
		recv_errors, battery_mv, packets_sent, packets_recv
		FROM observer_metrics ORDER BY rowid`)
	if err != nil {
		return fmt.Errorf("read observer_metrics: %w", err)
	}
	defer rows.Close()

	seen := map[metricKey]bool{}
	next := func() ([]any, error) {
		for rows.Next() {
			var (
				observerID                                                  string
				timestamp                                                   any
				noise                                                       sql.NullFloat64
				txAir, rxAir, recvErrors, battery, packetsSent, packetsRecv sql.NullInt64
			)
			if err := rows.Scan(&observerID, &timestamp, &noise, &txAir, &rxAir, &recvErrors, &battery, &packetsSent, &packetsRecv); err != nil {
				return nil, fmt.Errorf("scan observer_metrics: %w", err)
			}
			id := strings.ToLower(strings.TrimSpace(observerID))
			if !known[id] {
				report.Problem("observer_metrics: skipped, observer not loaded", observerID)
				continue
			}
			at := report.Time("observer_metrics.timestamp", timestamp)
			if at == nil {
				report.Add("observer_metrics: skipped, unusable timestamp", 1)
				continue
			}
			key := metricKey{id, *at}
			if seen[key] {
				report.Add("observer_metrics: duplicate after lowercasing, dropped", 1)
				continue
			}
			seen[key] = true
			report.Add("observer_metrics: loaded", 1)
			return []any{id, *at, ptr(noise.Float64, noise.Valid),
				ptr(txAir.Int64, txAir.Valid), ptr(rxAir.Int64, rxAir.Valid), ptr(recvErrors.Int64, recvErrors.Valid),
				ptr(battery.Int64, battery.Valid), ptr(packetsSent.Int64, packetsSent.Valid), ptr(packetsRecv.Int64, packetsRecv.Valid)}, nil
		}
		return nil, rows.Err()
	}

	columns := []string{"observer_id", "timestamp", "noise_floor", "tx_air_secs", "rx_air_secs",
		"recv_errors", "battery_mv", "packets_sent", "packets_recv"}
	_, err = copyStream(ctx, tx, "observer_metrics", columns, next)
	return err
}
