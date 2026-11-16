package database

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/pgdb"
	"github.com/jackc/pgx/v5/pgtype"
)

// MetricsResolution selects how GetObserverMetrics aggregates samples.
type MetricsResolution string

const (
	MetricsRaw    MetricsResolution = "5m"
	MetricsHourly MetricsResolution = "1h"
	MetricsDaily  MetricsResolution = "1d"
)

// ObserverMetricsInput is one sample to store. The optional counters are
// cumulative since boot and must fit a 32-bit integer.
type ObserverMetricsInput struct {
	ObserverID  string // observer pubkey
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
	ObserverName  string
	IATA          string
	CurrentNF     *float64
	AvgNF         *float64
	MaxNF         *float64
	CurrentBattMv *int
	SampleCount   int
	Sparkline     []*float64
}

func int4Fits(field string, v *int) error {
	if v != nil && (*v < math.MinInt32 || *v > math.MaxInt32) {
		return fmt.Errorf("observer metric %s %d does not fit a 32-bit integer", field, *v)
	}
	return nil
}

// ParseMetricsResolution turns an API resolution into a MetricsResolution.
// "5m" (the default, also the empty string) means the unaggregated samples.
func ParseMetricsResolution(s string) (MetricsResolution, error) {
	switch s {
	case "", "5m":
		return MetricsRaw, nil
	case "1h":
		return MetricsHourly, nil
	case "1d":
		return MetricsDaily, nil
	}
	return "", fmt.Errorf("invalid resolution: %s. Must be 5m, 1h, or 1d", s)
}

// truncUnit is the date_trunc unit of a bucketed resolution, "" for raw.
func (r MetricsResolution) truncUnit() string {
	switch r {
	case MetricsHourly:
		return "hour"
	case MetricsDaily:
		return "day"
	}
	return ""
}

// CreateOrUpdateObserverMetrics stores a sample, replacing the stored one with
// the same observer and timestamp.
func (db *DB) CreateOrUpdateObserverMetrics(ctx context.Context, input ObserverMetricsInput) error {
	for field, v := range map[string]*int{
		"tx_air_secs": input.TxAirSecs, "rx_air_secs": input.RxAirSecs, "recv_errors": input.RecvErrors,
		"battery_mv": input.BatteryMv, "packets_sent": input.PacketsSent, "packets_recv": input.PacketsRecv,
	} {
		if err := int4Fits(field, v); err != nil {
			return err
		}
	}
	err := db.q.CreateOrUpdateObserverMetrics(ctx, pgdb.CreateOrUpdateObserverMetricsParams{
		ObserverID:  normalizeKey(input.ObserverID),
		Timestamp:   input.Timestamp,
		NoiseFloor:  toPgFloat8Ptr(input.NoiseFloor),
		TxAirSecs:   toPgInt4Ptr(input.TxAirSecs),
		RxAirSecs:   toPgInt4Ptr(input.RxAirSecs),
		RecvErrors:  toPgInt4Ptr(input.RecvErrors),
		BatteryMv:   toPgInt4Ptr(input.BatteryMv),
		PacketsSent: toPgInt4Ptr(input.PacketsSent),
		PacketsRecv: toPgInt4Ptr(input.PacketsRecv),
	})
	if err != nil {
		return fmt.Errorf("store observer metrics for %s: %w", input.ObserverID, err)
	}
	return nil
}

// PruneObserverMetrics deletes samples older than cutoff and returns how many
// were removed.
func (db *DB) PruneObserverMetrics(ctx context.Context, cutoff time.Time) (int64, error) {
	removed, err := db.q.PruneObserverMetrics(ctx, cutoff)
	if err != nil {
		return 0, fmt.Errorf("prune observer metrics: %w", err)
	}
	return removed, nil
}

// GetObserverMetrics returns the observer's samples oldest first. Hourly and
// daily resolutions return the last sample per UTC hour or day, stamped with
// the bucket start; raw returns every sample. since and until are inclusive and
// optional.
func (db *DB) GetObserverMetrics(ctx context.Context, pubkey string, since, until *time.Time, resolution MetricsResolution) ([]ObserverMetric, error) {
	pubkey = normalizeKey(pubkey)
	if unit := resolution.truncUnit(); unit != "" {
		rows, err := db.q.ListObserverMetricsBucketed(ctx, pgdb.ListObserverMetricsBucketedParams{
			Bucket: unit, ObserverID: pubkey, Since: toPgTimestamptzPtr(since), Until: toPgTimestamptzPtr(until),
		})
		if err != nil {
			return nil, fmt.Errorf("list observer metrics (%s): %w", resolution, err)
		}
		out := make([]ObserverMetric, len(rows))
		for i, r := range rows {
			out[i] = observerMetric(r.Timestamp, r.NoiseFloor, r.TxAirSecs, r.RxAirSecs, r.RecvErrors, r.BatteryMv, r.PacketsSent, r.PacketsRecv)
		}
		return out, nil
	}
	rows, err := db.q.ListObserverMetrics(ctx, pgdb.ListObserverMetricsParams{
		ObserverID: pubkey, Since: toPgTimestamptzPtr(since), Until: toPgTimestamptzPtr(until),
	})
	if err != nil {
		return nil, fmt.Errorf("list observer metrics: %w", err)
	}
	out := make([]ObserverMetric, len(rows))
	for i, r := range rows {
		out[i] = observerMetric(r.Timestamp, r.NoiseFloor, r.TxAirSecs, r.RxAirSecs, r.RecvErrors, r.BatteryMv, r.PacketsSent, r.PacketsRecv)
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
		return nil, fmt.Errorf("list metrics summary samples: %w", err)
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
		ObserverName:  last.Name,
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
