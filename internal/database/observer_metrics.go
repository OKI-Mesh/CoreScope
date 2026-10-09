package database

import (
	"context"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
)

type ObserverMetricsInput struct {
	ObserverID  string
	Timestamp   time.Time
	NoiseFloor  *float64
	TxAirSecs   *int
	RxAirSecs   *int
	RecvErrors  *int
	BatteryMv   *int
	PacketsSent *int
	PacketsRecv *int
}

func (db *DB) CreateOrUpdateObserverMetrics(ctx context.Context, input ObserverMetricsInput) error {
	return db.q.CreateOrUpdateObserverMetrics(ctx, pgdb.CreateOrUpdateObserverMetricsParams{
		ObserverID:  input.ObserverID,
		Timestamp:   input.Timestamp,
		NoiseFloor:  toPgFloat8Ptr(input.NoiseFloor),
		TxAirSecs:   toPgInt4Ptr(input.TxAirSecs),
		RxAirSecs:   toPgInt4Ptr(input.RxAirSecs),
		RecvErrors:  toPgInt4Ptr(input.RecvErrors),
		BatteryMv:   toPgInt4Ptr(input.BatteryMv),
		PacketsSent: toPgInt4Ptr(input.PacketsSent),
		PacketsRecv: toPgInt4Ptr(input.PacketsRecv),
	})
}

func (db *DB) PruneObserverMetrics(ctx context.Context, cutoff time.Time) (int64, error) {
	return db.q.PruneObserverMetrics(ctx, cutoff)
}
