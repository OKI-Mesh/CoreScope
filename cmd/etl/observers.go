package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/jackc/pgx/v5"
)

type observer struct {
	ID                int64
	Pubkey            string
	Name              *string
	IATA              *string
	LastSeen          *time.Time
	FirstSeen         *time.Time
	PacketCount       int64
	Model             *string
	Firmware          *string
	ClientVersion     *string
	Radio             *string
	BatteryMv         *int64
	UptimeSecs        *int64
	NoiseFloor        *float64
	Inactive          bool
	LastPacketAt      *time.Time
	ClockSkewSeconds  *int64
	ClockSkewCount24h int64
	ClockLastNaiveAt  *time.Time
	CanRelay          bool
	CanRelaySeen      bool
}

// LoadedObservers is what later stages need to know about the observers load.
// Observers keep their SQLite rowid as id, so observations.observer_idx needs
// no remapping.
type LoadedObservers struct {
	// Pubkeys maps observers.id (the SQLite rowid) to the lowercase pubkey.
	Pubkeys map[int64]string
}

func readObservers(ctx context.Context, src *Source, report *Report) ([]observer, error) {
	rows, err := src.db.QueryContext(ctx, `SELECT rowid, id, name, iata, last_seen, first_seen, packet_count,
		model, firmware, client_version, radio, battery_mv, uptime_secs, noise_floor, inactive,
		last_packet_at, clock_skew_seconds, clock_skew_count_24h, clock_last_naive_at,
		can_relay, can_relay_seen
		FROM observers ORDER BY rowid`)
	if err != nil {
		return nil, fmt.Errorf("read observers: %w", err)
	}
	defer rows.Close()

	var observers []observer
	for rows.Next() {
		var (
			rowID, id                                  = int64(0), ""
			name, iata, model, firmware, clientVersion sql.NullString
			radio                                      sql.NullString
			lastSeen, firstSeen, lastPacketAt          any
			clockLastNaiveAt                           any
			packetCount, battery, uptime               sql.NullInt64
			inactive, skewSeconds, skewCount           sql.NullInt64
			canRelay, canRelaySeen                     sql.NullInt64
			noiseFloor                                 sql.NullFloat64
		)
		if err := rows.Scan(&rowID, &id, &name, &iata, &lastSeen, &firstSeen, &packetCount,
			&model, &firmware, &clientVersion, &radio, &battery, &uptime, &noiseFloor, &inactive,
			&lastPacketAt, &skewSeconds, &skewCount, &clockLastNaiveAt,
			&canRelay, &canRelaySeen); err != nil {
			return nil, fmt.Errorf("scan observers: %w", err)
		}
		pubkey := strings.ToLower(id)
		if pubkey == "" {
			return nil, fmt.Errorf("observers: rowid %d has an empty id", rowID)
		}
		if pubkey != id {
			report.Add("observers: id lowercased", 1)
		}
		var normalizedIATA *string
		if iata.Valid {
			if code := types.NewIATA(iata.String); !code.IsZero() {
				value := code.String()
				normalizedIATA = &value
				if value != iata.String {
					report.Add("observers: iata trimmed and upper-cased", 1)
				}
			} else {
				report.Add("observers: blank iata set to NULL", 1)
			}
		}
		observers = append(observers, observer{
			ID:                rowID,
			Pubkey:            pubkey,
			Name:              cleanText(report, "observers.name", id, textOrNil(name)),
			IATA:              normalizedIATA,
			LastSeen:          report.Time("observers.last_seen", lastSeen),
			FirstSeen:         report.Time("observers.first_seen", firstSeen),
			PacketCount:       packetCount.Int64,
			Model:             cleanText(report, "observers.model", id, ptr(model.String, model.Valid)),
			Firmware:          cleanText(report, "observers.firmware", id, ptr(firmware.String, firmware.Valid)),
			ClientVersion:     cleanText(report, "observers.client_version", id, ptr(clientVersion.String, clientVersion.Valid)),
			Radio:             cleanText(report, "observers.radio", id, ptr(radio.String, radio.Valid)),
			BatteryMv:         ptr(battery.Int64, battery.Valid),
			UptimeSecs:        ptr(uptime.Int64, uptime.Valid),
			NoiseFloor:        ptr(noiseFloor.Float64, noiseFloor.Valid),
			Inactive:          inactive.Int64 != 0,
			LastPacketAt:      report.Time("observers.last_packet_at", lastPacketAt),
			ClockSkewSeconds:  ptr(skewSeconds.Int64, skewSeconds.Valid),
			ClockSkewCount24h: skewCount.Int64,
			ClockLastNaiveAt:  report.Time("observers.clock_last_naive_at", clockLastNaiveAt),
			CanRelay:          !canRelay.Valid || canRelay.Int64 != 0,
			CanRelaySeen:      canRelaySeen.Int64 != 0,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read observers: %w", err)
	}
	return observers, nil
}

// observationSpan returns the earliest and latest real observation time for an
// observer. Timestamp 0 marks an observation with no time and is ignored.
func observationSpan(ctx context.Context, src *Source, observerID int64) (first, last *time.Time, err error) {
	var minTS, maxTS sql.NullInt64
	err = src.db.QueryRowContext(ctx,
		`SELECT MIN(timestamp), MAX(timestamp) FROM observations WHERE observer_idx = ? AND timestamp > 0`,
		observerID).Scan(&minTS, &maxTS)
	if err != nil {
		return nil, nil, fmt.Errorf("observation span for observer %d: %w", observerID, err)
	}
	if minTS.Valid {
		t := time.Unix(minTS.Int64, 0).UTC()
		first = &t
	}
	if maxTS.Valid {
		t := time.Unix(maxTS.Int64, 0).UTC()
		last = &t
	}
	return first, last, nil
}

// fillTimes gives an observer both first_seen and last_seen, which are NOT NULL
// in Postgres. A missing first_seen comes from the observer's earliest
// observation, else its last_seen; a missing last_seen from its latest
// observation, else its first_seen.
func (o *observer) fillTimes(ctx context.Context, src *Source, report *Report) error {
	if o.FirstSeen != nil && o.LastSeen != nil {
		return nil
	}
	first, last, err := observationSpan(ctx, src, o.ID)
	if err != nil {
		return err
	}
	if o.FirstSeen == nil {
		switch {
		case first != nil:
			o.FirstSeen = first
			report.Add("observers: NULL first_seen filled from earliest observation", 1)
		case o.LastSeen != nil:
			o.FirstSeen = o.LastSeen
			report.Add("observers: NULL first_seen filled from last_seen", 1)
		}
	}
	if o.LastSeen == nil {
		switch {
		case last != nil:
			o.LastSeen = last
			report.Add("observers: NULL last_seen filled from latest observation", 1)
		case o.FirstSeen != nil:
			o.LastSeen = o.FirstSeen
			report.Add("observers: NULL last_seen filled from first_seen", 1)
		}
	}
	if o.FirstSeen == nil || o.LastSeen == nil {
		return fmt.Errorf("observer %s (rowid %d) has no first_seen or last_seen and no observations to infer them from", o.Pubkey, o.ID)
	}
	return nil
}

func loadObservers(ctx context.Context, src *Source, tx pgx.Tx, report *Report) (*LoadedObservers, error) {
	observers, err := readObservers(ctx, src, report)
	if err != nil {
		return nil, err
	}

	pubkeys := make(map[int64]string, len(observers))
	byPubkey := make(map[string]int64, len(observers))
	rows := make([][]any, 0, len(observers))
	for i := range observers {
		o := &observers[i]
		if first, dup := byPubkey[o.Pubkey]; dup {
			return nil, fmt.Errorf("observers: rowids %d and %d are the same pubkey %s after lowercasing", first, o.ID, o.Pubkey)
		}
		byPubkey[o.Pubkey] = o.ID
		if err := o.fillTimes(ctx, src, report); err != nil {
			return nil, err
		}
		pubkeys[o.ID] = o.Pubkey
		rows = append(rows, []any{
			o.ID, o.Pubkey, o.Name, o.LastSeen, o.FirstSeen, o.PacketCount, o.Model, o.Firmware,
			o.ClientVersion, o.Radio, o.BatteryMv, o.UptimeSecs, o.NoiseFloor, o.Inactive, o.LastPacketAt,
			o.ClockSkewSeconds, o.ClockSkewCount24h, o.ClockLastNaiveAt, o.IATA, o.CanRelay, o.CanRelaySeen,
		})
	}
	report.Add("observers: loaded", int64(len(rows)))

	columns := []string{"id", "pubkey", "name", "last_seen", "first_seen", "packet_count", "model", "firmware",
		"client_version", "radio", "battery_mv", "uptime_secs", "noise_floor", "inactive", "last_packet_at",
		"clock_skew_seconds", "clock_skew_count_24h", "clock_last_naive_at", "iata", "can_relay", "can_relay_seen"}
	if err := copyRows(ctx, tx, "observers", columns, rows); err != nil {
		return nil, err
	}
	if err := resetSequence(ctx, tx, "observers"); err != nil {
		return nil, err
	}
	return &LoadedObservers{Pubkeys: pubkeys}, nil
}
