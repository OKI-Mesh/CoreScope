package database

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
	"github.com/jackc/pgx/v5/pgtype"
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

// ObserverMetric is one observer_metrics sample. Counters are cumulative since boot.
type ObserverMetric struct {
	Timestamp   time.Time // UTC; for bucketed queries, the bucket start
	NoiseFloor  *float64
	TxAirSecs   *int64
	RxAirSecs   *int64
	RecvErrors  *int64
	BatteryMv   *int
	PacketsSent *int64
	PacketsRecv *int64
}

// MetricsSummary is one observer's noise-floor and battery summary over a window.
type MetricsSummary struct {
	ObserverID    string
	ObserverName  *string
	IATA          string
	CurrentNF     *float64 // latest sample in the window; nil if that sample has no noise floor
	AvgNF         *float64 // nil if no sample has a noise floor
	MaxNF         *float64
	CurrentBattMv *int
	SampleCount   int
	Sparkline     []*float64 // noise floor per sample, oldest first
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

// GetObserverMetrics returns the observer's samples oldest first. resolution is "1h" or
// "1d" for the last sample per UTC hour/day; anything else returns the raw samples.
// since and until are inclusive and optional.
func (d *DB) GetObserverMetrics(ctx context.Context, pubkey string, since, until *time.Time, resolution string) ([]ObserverMetric, error) {
	var out []ObserverMetric
	switch resolution {
	case "1h", "1d":
		bucket := "hour"
		if resolution == "1d" {
			bucket = "day"
		}
		rows, err := d.q.ListObserverMetricsBucketed(ctx, pgdb.ListObserverMetricsBucketedParams{
			Bucket: bucket, ObserverID: pubkey, Since: toPgTimestamptzPtr(since), Until: toPgTimestamptzPtr(until),
		})
		if err != nil {
			return nil, fmt.Errorf("list observer metrics (%s): %w", resolution, err)
		}
		out = make([]ObserverMetric, len(rows))
		for i, r := range rows {
			out[i] = observerMetric(r.Timestamp, r.NoiseFloor, r.TxAirSecs, r.RxAirSecs, r.RecvErrors, r.BatteryMv, r.PacketsSent, r.PacketsRecv)
		}
	default:
		rows, err := d.q.ListObserverMetrics(ctx, pgdb.ListObserverMetricsParams{
			ObserverID: pubkey, Since: toPgTimestamptzPtr(since), Until: toPgTimestamptzPtr(until),
		})
		if err != nil {
			return nil, fmt.Errorf("list observer metrics: %w", err)
		}
		out = make([]ObserverMetric, len(rows))
		for i, r := range rows {
			out[i] = observerMetric(r.Timestamp, r.NoiseFloor, r.TxAirSecs, r.RxAirSecs, r.RecvErrors, r.BatteryMv, r.PacketsSent, r.PacketsRecv)
		}
	}
	return out, nil
}

func observerMetric(ts time.Time, noise pgtype.Float8, tx, rx, errs, battery, sent, recv pgtype.Int4) ObserverMetric {
	m := ObserverMetric{
		Timestamp: ts.UTC(), NoiseFloor: fromPgFloat8(noise),
		TxAirSecs: fromPgInt4To64(tx), RxAirSecs: fromPgInt4To64(rx), RecvErrors: fromPgInt4To64(errs),
		PacketsSent: fromPgInt4To64(sent), PacketsRecv: fromPgInt4To64(recv),
	}
	if battery.Valid {
		v := int(battery.Int32)
		m.BatteryMv = &v
	}
	return m
}

// GetMetricsSummary summarizes every observer with metrics at or after since,
// ordered by max noise floor descending (NULLs last), then pubkey.
func (db *DB) GetMetricsSummary(ctx context.Context, since time.Time) ([]MetricsSummary, error) {
	rows, err := db.q.ListMetricsSummarySamples(ctx, since)
	if err != nil {
		return nil, err
	}

	out := []MetricsSummary{}
	// Rows arrive grouped by observer, oldest sample first.
	for start := 0; start < len(rows); {
		end := start
		for end < len(rows) && rows[end].ObserverID == rows[start].ObserverID {
			end++
		}
		out = append(out, summarizeMetrics(rows[start:end]))
		start = end
	}

	slices.SortFunc(out, func(a, b MetricsSummary) int {
		switch {
		case a.MaxNF == nil && b.MaxNF == nil:
			return strings.Compare(a.ObserverID, b.ObserverID)
		case a.MaxNF == nil:
			return 1
		case b.MaxNF == nil:
			return -1
		case *a.MaxNF != *b.MaxNF:
			return cmp.Compare(*b.MaxNF, *a.MaxNF)
		}
		return strings.Compare(a.ObserverID, b.ObserverID)
	})
	return out, nil
}

// summarizeMetrics reduces one observer's samples (oldest first, never empty).
func summarizeMetrics(g []pgdb.ListMetricsSummarySamplesRow) MetricsSummary {
	last := g[len(g)-1]
	s := MetricsSummary{
		ObserverID:    last.ObserverID,
		ObserverName:  &last.Name,
		IATA:          last.Iata,
		CurrentNF:     fromPgFloat8(last.NoiseFloor),
		CurrentBattMv: fromPgInt4(last.BatteryMv),
		SampleCount:   len(g),
		Sparkline:     make([]*float64, 0, len(g)),
	}

	var sum, maxNF float64
	n := 0
	for _, r := range g {
		nf := fromPgFloat8(r.NoiseFloor)
		s.Sparkline = append(s.Sparkline, nf)
		if nf == nil {
			continue
		}
		sum += *nf
		if n == 0 || *nf > maxNF {
			maxNF = *nf
		}
		n++
	}
	if n > 0 {
		avg := sum / float64(n)
		s.AvgNF, s.MaxNF = &avg, &maxNF
	}
	return s
}
