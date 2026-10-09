package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// UpsertObserver inserts or updates an observer using the current wall-clock
// time as last_seen. Use UpsertObserverAt when the message envelope provides
// an observer receive-time (e.g. MQTT status and data packet handlers).
func (s *Store) UpsertObserver(ctx context.Context, id, name, iata string, meta *types.ObserverMeta) error {
	return s.UpsertObserverAt(ctx, id, name, iata, meta, time.Now().UTC())
}

// UpsertObserverAt inserts or updates an observer with an explicit lastSeen
// timestamp (typically the observer receive-time from the MQTT envelope). The
// SQL uses GREATEST so last_seen never moves backwards — a retained or
// replayed message whose rxTime pre-dates the existing last_seen is a no-op
// for that field, preventing offline observers from flashing as Online on
// reconnect.
func (s *Store) UpsertObserverAt(ctx context.Context, id, name, iata string, meta *types.ObserverMeta, lastSeen time.Time) error {
	ingestNow := time.Now().UTC()
	if lastSeen.IsZero() {
		lastSeen = ingestNow
	}

	err := s.db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        id,
		Name:      name,
		IATA:      strings.TrimSpace(strings.ToUpper(iata)),
		Meta:      meta,
		LastSeen:  lastSeen,
		IngestNow: ingestNow,
	})
	if err != nil {
		s.Stats.WriteErrors.Add(1)
		return err
	}
	s.Stats.ObserverUpserts.Add(1)

	return s.db.ReactivateObserver(ctx, id)
}

// UpsertObserverRetained applies the metadata from a RETAINED status message
// without treating it as a sign of life. The broker replays retained messages
// on every subscribe, so an ingestor restart would otherwise stamp last_seen
// with the restart time for every observer that ever published one — making
// dead observers permanently un-ageable by RemoveStaleObservers, and undoing
// any inactive flag it did manage to set.
//
// Deliberately narrower than UpsertObserverAt: it updates metadata columns on
// an existing row only. It does not advance last_seen, does not clear
// inactive, does not bump packet_count, and does not INSERT — a retained-only
// observer the analyzer has never heard from live describes a past that may be
// months old and does not belong in the list. A live message from the same
// observer arrives moments later and creates the row through the normal path.
func (s *Store) UpsertObserverRetained(ctx context.Context, id, name, iata string, meta *types.ObserverMeta) error {
	err := s.db.UpdateObserverMetadataIfRetained(ctx, database.ObserverMetadataInput{
		ID:   id,
		Name: name,
		IATA: strings.TrimSpace(strings.ToUpper(iata)),
		Meta: meta,
	})
	if err != nil {
		s.Stats.WriteErrors.Add(1)
		return err
	}
	return nil
}

// RemoveStaleObservers marks observers that have not actively sent data in observerDays
// as inactive (soft-delete). This preserves JOIN integrity for observations.observer_idx
// and observer_metrics.observer_id — historical data still references the correct observer.
// An observer must actively send data to stay listed — being seen by another node does not count.
// observerDays <= -1 means never remove (keep forever).
func (s *Store) RemoveStaleObservers(ctx context.Context, observerDays int) (int64, error) {
	if observerDays <= -1 {
		return 0, nil // keep forever
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -observerDays)
	removed, err := s.db.MarkStaleObserversInactive(ctx, cutoff)
	if err != nil {
		return 0, fmt.Errorf("mark stale observers inactive: %w", err)
	}
	if removed > 0 {
		// Clean up metrics for now-inactive observers (best-effort).
		if _, err := s.db.DeleteMetricsForInactiveObservers(ctx); err != nil {
			log.Printf("[prune] observer metrics cleanup (non-fatal): %v", err)
		}
		log.Printf("Marked %d observer(s) as inactive (not seen in %d days)", removed, observerDays)
	}
	return removed, nil
}

// PurgeStaleObservers hard-deletes rows RemoveStaleObservers already soft-deleted,
// once they are older than purgeDays and nothing references them any more. It is
// the second stage of observer retention: the soft-delete hides the observer,
// this reclaims the row after its packets have aged out via packetDays.
//
// observations.observer_idx is a bare rowid with no foreign key, so deleting a
// still-referenced observer silently orphans history — packets_v stops resolving
// the observer and the packets are mis-attributed. The three NOT EXISTS guards
// are what make the delete safe; they are correctness, not defensive padding.
// Each is an index seek per candidate row (observers is O(100)), so the whole
// statement stays cheap enough for the daily retention tick.
//
// purgeDays <= 0 disables the purge (the default).
func (s *Store) PurgeStaleObservers(ctx context.Context, purgeDays int) (int64, error) {
	if purgeDays <= 0 {
		return 0, nil // disabled
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -purgeDays)
	purged, err := s.db.PurgeStaleObservers(ctx, cutoff)
	if err != nil {
		return 0, fmt.Errorf("purge stale observers: %w", err)
	}
	if purged > 0 {
		log.Printf("Purged %d inactive observer(s) with no remaining data (not seen in %d days)", purged, purgeDays)
	}
	return purged, nil
}

// RecordNaiveSkew is called when resolveRxTime() clamps a packet's envelope
// timestamp because the observer is emitting a zone-less local-time string
// off from UTC by more than 15 min (issue #1478). Stamps the observer's
// clock_skew_seconds / clock_skew_count_24h / clock_last_naive_at so the
// server can surface a ⚠️ chip + banner in the UI.
//
// The count is reset to 1 (not incremented) if no event has been recorded in
// the past 24h, otherwise incremented. deltaSec is signed: negative = observer
// clock is behind UTC, positive = ahead.
// TODO: Move out of the store
func (s *Store) RecordNaiveSkew(ctx context.Context, observerID string, deltaSec int64, now time.Time) error {
	if observerID == "" {
		return nil
	}
	return s.db.RecordNaiveSkew(ctx, observerID, int32(deltaSec), now)
}

// SoftDeleteBlacklistedObservers marks observers in the blacklist as
// inactive=1 so they are hidden from API responses. Owned by ingestor
// per #1287. Runs once at startup.
func (s *Store) SoftDeleteBlacklistedObservers(ctx context.Context, blacklist []string) {
	n, err := s.db.SoftDeleteBlacklistedObservers(ctx, blacklist)
	if err != nil {
		log.Printf("[observer-blacklist] warning: soft-delete failed: %v", err)
		return
	}
	if n > 0 {
		log.Printf("[observer-blacklist] soft-deleted %d blacklisted observer(s)", n)
	}
}
