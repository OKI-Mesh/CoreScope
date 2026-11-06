package main

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
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

type NodeBatterySampleResponse struct {
	Timestamp string `json:"timestamp"`
	BatteryMv int    `json:"battery_mv"`
}

func nodeBatteryToResponse(rows []database.NodeBatterySample) []NodeBatterySampleResponse {
	out := make([]NodeBatterySampleResponse, 0, len(rows))
	for _, r := range rows {
		out = append(out, NodeBatterySampleResponse{Timestamp: formatTS(r.Timestamp), BatteryMv: r.BatteryMv})
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

	status := "unknown"
	var latestMv interface{}
	var latestTs interface{}
	if n := len(samples); n > 0 {
		mv := samples[n-1].BatteryMv
		latestMv = mv
		latestTs = samples[n-1].Timestamp
		switch {
		case mv < crit:
			status = "critical"
		case mv < low:
			status = "low"
		default:
			status = "ok"
		}
	}

	writeJSON(w, map[string]interface{}{
		"public_key": strings.ToLower(pubkey),
		"days":       days,
		"samples":    samples,
		"latest_mv":  latestMv,
		"latest_ts":  latestTs,
		"status":     status,
		"thresholds": map[string]interface{}{
			"low_mv":      low,
			"critical_mv": crit,
		},
	})
}
