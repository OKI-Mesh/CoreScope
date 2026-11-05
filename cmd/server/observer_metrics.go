package main

import (
	"context"
	"math"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
)

// MetricsSample is one observer metrics row with server-computed deltas.
// The raw cumulative counters are not sent; they were always omitted
// (omitempty on never-set fields), so they are dropped from the struct.
type MetricsSample struct {
	Timestamp     string   `json:"timestamp"`
	NoiseFloor    *float64 `json:"noise_floor"`
	BatteryMv     *int     `json:"battery_mv"`
	TxAirtimePct  *float64 `json:"tx_airtime_pct"`
	RxAirtimePct  *float64 `json:"rx_airtime_pct"`
	RecvErrorRate *float64 `json:"recv_error_rate"`
	IsReboot      bool     `json:"is_reboot_sample,omitempty"`
}

type ObserverMetricsResponse struct {
	ObserverID   string          `json:"observer_id"`
	ObserverName *string         `json:"observer_name"`
	Reboots      []string        `json:"reboots"`
	Metrics      []MetricsSample `json:"metrics"`
}

func (s *Server) observerMetrics(ctx context.Context, pubkey string, since, until *time.Time, resolution string, sampleIntervalSec int) ([]MetricsSample, []string, error) {
	raw, err := s.db.GetObserverMetrics(ctx, pubkey, since, until, resolution)
	if err != nil {
		return nil, nil, err
	}
	samples, reboots := computeDeltas(raw, metricsBucketSeconds(resolution, sampleIntervalSec))
	return samples, reboots, nil
}

// metricsBucketSeconds is the expected spacing between samples; a gap larger
// than twice this is not interpolated.
func metricsBucketSeconds(resolution string, sampleIntervalSec int) int {
	switch resolution {
	case "1h":
		return 3600
	case "1d":
		return 86400
	}
	if sampleIntervalSec <= 0 {
		return 300
	}
	return sampleIntervalSec
}

func computeDeltas(raw []database.ObserverMetric, bucketSizeSec int) ([]MetricsSample, []string) {
	if len(raw) == 0 {
		return nil, nil
	}
	gapThreshold := float64(bucketSizeSec) * 2.0
	result := make([]MetricsSample, 0, len(raw))
	var reboots []string

	for i, cur := range raw {
		s := MetricsSample{
			Timestamp:  formatTS(cur.Timestamp),
			NoiseFloor: cur.NoiseFloor,
			BatteryMv:  cur.BatteryMv,
		}
		if i == 0 {
			result = append(result, s) // first sample: no delta possible
			continue
		}
		prev := raw[i-1]

		interval := cur.Timestamp.Sub(prev.Timestamp).Seconds()
		if interval > gapThreshold || interval <= 0 {
			result = append(result, s) // gap or clock oddity: null deltas, no interpolation
			continue
		}

		// Reboot: any cumulative counter went down. The sample becomes the new baseline.
		if decreased(cur.TxAirSecs, prev.TxAirSecs) || decreased(cur.RxAirSecs, prev.RxAirSecs) ||
			decreased(cur.RecvErrors, prev.RecvErrors) || decreased(cur.PacketsSent, prev.PacketsSent) ||
			decreased(cur.PacketsRecv, prev.PacketsRecv) {
			s.IsReboot = true
			reboots = append(reboots, s.Timestamp)
			result = append(result, s)
			continue
		}

		s.TxAirtimePct = airtimePct(cur.TxAirSecs, prev.TxAirSecs, interval)
		s.RxAirtimePct = airtimePct(cur.RxAirSecs, prev.RxAirSecs, interval)
		s.RecvErrorRate = recvErrorRate(cur, prev)
		result = append(result, s)
	}
	return result, reboots
}

func decreased(cur, prev *int64) bool {
	return cur != nil && prev != nil && *cur < *prev
}

// airtimePct is the share of the interval spent transmitting/receiving,
// clamped to [0, 100] and rounded to two decimals.
func airtimePct(cur, prev *int64, intervalSecs float64) *float64 {
	if cur == nil || prev == nil {
		return nil
	}
	pct := float64(*cur-*prev) / intervalSecs * 100.0
	pct = math.Max(0, math.Min(100, pct))
	r := math.Round(pct*100) / 100
	return &r
}

// recvErrorRate is errors / (received + errors) over the interval, in percent.
func recvErrorRate(cur, prev database.ObserverMetric) *float64 {
	if cur.RecvErrors == nil || prev.RecvErrors == nil || cur.PacketsRecv == nil || prev.PacketsRecv == nil {
		return nil
	}
	dErr := float64(*cur.RecvErrors - *prev.RecvErrors)
	dRecv := float64(*cur.PacketsRecv - *prev.PacketsRecv)
	total := dRecv + dErr
	if total <= 0 {
		return nil
	}
	r := math.Round(dErr/total*100.0*100) / 100
	return &r
}
