package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
)

// InsertMetrics inserts a metrics sample for an observer using ingestor wall clock.
func (s *Store) InsertMetrics(ctx context.Context, data *MetricsData) error {
	ts := RoundToInterval(time.Now().UTC(), s.sampleIntervalSec)

	err := s.db.CreateOrUpdateObserverMetrics(ctx, database.ObserverMetricsInput{
		ObserverID:  data.ObserverID,
		Timestamp:   ts,
		NoiseFloor:  data.NoiseFloor,
		TxAirSecs:   data.TxAirSecs,
		RxAirSecs:   data.RxAirSecs,
		RecvErrors:  data.RecvErrors,
		BatteryMv:   data.BatteryMv,
		PacketsSent: data.PacketsSent,
		PacketsRecv: data.PacketsRecv,
	})
	if err != nil {
		s.Stats.WriteErrors.Add(1)
		return fmt.Errorf("insert metrics: %w", err)
	}
	return nil
}

// PruneOldMetrics deletes observer_metrics rows older than retentionDays.
func (s *Store) PruneOldMetrics(ctx context.Context, retentionDays int) (int64, error) {
	cutoff := time.Now().UTC().AddDate(0, 0, -retentionDays)
	n, err := s.db.PruneObserverMetrics(ctx, cutoff)
	if err != nil {
		return 0, fmt.Errorf("prune metrics: %w", err)
	}
	if n > 0 {
		log.Printf("[metrics] Pruned %d rows older than %d days", n, retentionDays)
	}
	return n, nil
}
