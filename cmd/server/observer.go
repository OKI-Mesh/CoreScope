package main

import (
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// Observer represents a row from the observers table.
type ObserverResponse struct {
	ID              string          `json:"id"`
	Name            *string         `json:"name"`
	IATA            *string         `json:"iata"`
	LastSeen        *time.Time      `json:"last_seen"`
	FirstSeen       *time.Time      `json:"first_seen"`
	PacketCount     int             `json:"packet_count"`
	Model           *string         `json:"model"`
	Firmware        *string         `json:"firmware"`
	ClientVersion   *string         `json:"client_version"`
	Radio           *string         `json:"radio"`
	BatteryMv       *int            `json:"battery_mv"`
	UptimeSecs      *int            `json:"uptime_secs"`
	NoiseFloor      *float64        `json:"noise_floor"`
	LastPacketAt    *time.Time      `json:"last_packet_at"`
	PacketsLastHour int             `json:"packetsLastHour"`
	Lat             *float64        `json:"lat"`
	Lon             *float64        `json:"lon"`
	NodeRole        *types.NodeRole `json:"nodeRole"`
	// Issue #1478: surface naive-clock observers to the UI.
	// `clock_naive` is derived from clock_last_naive_at being within the
	// last 24h; once decayed, all three skew fields read as zero/null so the
	// chip and banner clear automatically.
	ClockNaive        bool    `json:"clock_naive"`
	ClockSkewSeconds  *int    `json:"clock_skew_seconds"`
	ClockSkewCount24h int     `json:"clock_skew_count_24h"`
	ClockLastNaiveAt  *string `json:"clock_last_naive_at"`
	// Issue #1290: firmware 1.16 `repeat` flag — true=repeater,
	// false=listener-only, nil=unknown (legacy observer never sent the
	// field). UI tri-state badge renders nothing when nil so legacy
	// rows don't masquerade as confirmed repeaters (PR #1624 MAJOR-2).
	CanRelay *bool `json:"can_relay,omitempty"`
}
