package main

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/gorilla/mux"
)

// BatteryThresholdsConfig: voltage cutoffs for low-battery alerts (#663).
// All values in millivolts. When a node's most-recent battery sample falls
// below LowMv it is flagged "low"; below CriticalMv it is flagged "critical".
type BatteryThresholdsConfig struct {
	LowMv      int `json:"lowMv"`
	CriticalMv int `json:"criticalMv"`
}

// LowBatteryMv returns the configured low-battery threshold or the default 3300mV.
func (c *Config) LowBatteryMv() int {
	if c.BatteryThresholds != nil && c.BatteryThresholds.LowMv > 0 {
		return c.BatteryThresholds.LowMv
	}
	return 3300
}

// CriticalBatteryMv returns the configured critical-battery threshold or the default 3000mV.
func (c *Config) CriticalBatteryMv() int {
	if c.BatteryThresholds != nil && c.BatteryThresholds.CriticalMv > 0 {
		return c.BatteryThresholds.CriticalMv
	}
	return 3000
}

func nodeBatteryToResponse(rows []database.NodeBatterySample) []api.BatterySample {
	out := make([]api.BatterySample, 0, len(rows))
	for _, r := range rows {
		out = append(out, api.BatterySample{Timestamp: types.WireTimeMillis{Time: r.Timestamp}, BatteryMv: r.BatteryMv})
	}
	return out
}

// handleNodeBattery serves GET /api/nodes/{pubkey}/battery?days=N (#663).
//
// Returns voltage time-series for a node and a status flag based on the most
// recent sample evaluated against configured thresholds:
//   - "critical" : latest_mv < CriticalBatteryMv
//   - "low"      : latest_mv < LowBatteryMv
//   - "ok"       : latest_mv >= LowBatteryMv
//   - "unknown"  : no samples in window
func (s *Server) handleNodeBattery(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	pubkey := mux.Vars(r)["pubkey"]
	if pubkey == "" {
		writeError(w, 400, "missing pubkey")
		return
	}

	// 404 if node unknown — keeps URL space tidy and matches /health behavior.
	_, err := s.db.GetNodeByPubkey(ctx, pubkey)
	if errors.Is(err, database.ErrNotFound) {
		writeError(w, 404, "node not found")
		return
	}
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}

	days := 7
	if d, _ := strconv.Atoi(r.URL.Query().Get("days")); d > 0 && d <= 365 {
		days = d
	}
	since := time.Now().UTC().Add(-time.Duration(days) * 24 * time.Hour)

	rows, err := s.db.GetNodeBatteryHistory(ctx, pubkey, since)
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	samples := nodeBatteryToResponse(rows)

	low := s.cfg.LowBatteryMv()
	crit := s.cfg.CriticalBatteryMv()

	status := api.BatteryStatus("unknown")
	var latestMv *int
	var latestTs *types.WireTimeMillis
	if n := len(samples); n > 0 {
		last := samples[n-1]
		latestMv = ptr(last.BatteryMv)
		latestTs = ptr(last.Timestamp)
		switch {
		case last.BatteryMv < crit:
			status = "critical"
		case last.BatteryMv < low:
			status = "low"
		default:
			status = "ok"
		}
	}

	writeJSON(w, api.NodeBatteryResponse{
		PublicKey:  strings.ToLower(pubkey),
		Days:       days,
		Samples:    samples,
		LatestMv:   latestMv,
		LatestTs:   latestTs,
		Status:     status,
		Thresholds: api.BatteryThresholds{LowMv: low, CriticalMv: crit},
	})
}
