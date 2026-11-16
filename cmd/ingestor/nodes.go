package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/decoder"
)

// IncrementAdvertCount increments advert_count for a node by public key.
func (s *Store) IncrementAdvertCount(ctx context.Context, pubKey string) error {
	err := s.db.IncrementNodeAdvertCount(ctx, pubKey)
	return err
}

// MarkNodeForeign sets foreign_advert=1 on the node row identified by pubKey.
// Used when an ADVERT arrives whose GPS lies outside the configured geofilter
// polygon (#730). Idempotent — safe to call repeatedly. No-op if pubKey is
// empty.
func (s *Store) MarkNodeForeign(ctx context.Context, pubKey string) error {
	if pubKey == "" {
		return nil
	}
	err := s.db.SetForeignAdvert(ctx, pubKey)
	if err != nil {
		s.Stats.WriteErrors.Add(1)
	}
	return err
}

// UpdateNodeTelemetry updates battery and temperature for a node.
func (s *Store) UpdateNodeTelemetry(ctx context.Context, pubKey string, batteryMv *int, temperatureC *float64) error {
	if err := s.db.UpdateBatteryVoltageAndTempC(ctx, pubKey, batteryMv, temperatureC); err != nil {
		s.Stats.WriteErrors.Add(1)
		return err
	}
	return nil

}

// touchRelayNodes refreshes nodes.last_seen for nodes observed as
// relay hops, so a node that forwards traffic stays fresh even when it
// adverts rarely or not at all.
//
// Callers must not run it concurrently: relayTouched has no lock of its own.
// Its only caller is InsertTransmission, which runs on the ingest buffer's
// single worker goroutine.
//
// Ownership (#1283/#1287/#1289): nodes is written by the ingestor only; the
// server connection is read-only.
//
// Callers pass the resolved pubkeys already computed for
// observations.hop_nodes (#1547/#1560). Only unambiguously resolved hops
// reach this function, so a 1-byte prefix collision cannot keep a silent node
// alive. resolvedPubkeys already dedups, so no second pass here.
//
// Never inserts: the UPDATE matches an existing row or does nothing.
// Never rewinds: the last_seen guard makes out-of-order ingest a no-op.
// CreateOrUpdateNode is monotonic in the same direction, so an ADVERT cannot
// undo a touch.
func (s *Store) touchRelayNodes(ctx context.Context, pubkeys []string, rxTime time.Time) {
	if len(pubkeys) == 0 {
		return
	}

	if s.relayTouched == nil {
		s.relayTouched = make(map[string]time.Time)
	}
	if len(s.relayTouched) >= relayTouchedMaxEntries {
		s.compactRelayTouched(rxTime)
	}
	for _, pk := range pubkeys {
		if pk == "" {
			continue
		}
		if last, ok := s.relayTouched[pk]; ok && rxTime.Sub(last) < relayTouchDebounce {
			continue
		}
		n, err := s.db.TouchNodeLastSeen(ctx, pk, rxTime)
		if err != nil {
			log.Printf("[db] touch node last_seen (non-fatal): %v", err)
		}

		s.relayTouched[pk] = rxTime
		if n > 0 {
			s.Stats.RelayTouches.Add(n)
		}
	}
}

// MoveStaleNodes moves nodes not seen in nodeDays to the inactive_nodes table.
// Returns the number of nodes moved.
func (s *Store) MoveStaleNodes(ctx context.Context, nodeDays int) (int64, error) {
	cutoff := time.Now().UTC().AddDate(0, 0, -nodeDays)
	moved, err := s.db.MarkStaleNodesInactive(ctx, cutoff)
	if err != nil {
		return 0, fmt.Errorf("mark stale nodes inactive: %w", err)
	}
	if moved > 0 {
		log.Printf("Marked %d node(s) inactive (not seen in %d days)", moved, nodeDays)
	}
	return moved, nil
}

// UpdateNodeDefaultScope records the most-recently observed region scope for a
// node. Skips the UPDATE when the stored value already matches to avoid
// redundant writes on the hot MQTT ingest path. Updates both nodes and
// inactive_nodes to stay consistent.
//
// Defense-in-depth (#1534): an empty scope is treated as a no-op. The call
// site at handleMessage is the primary guard (shouldUpdateDefaultScope),
// but this layer refuses the invalid write so a future caller cannot
// reintroduce the bug by passing "" directly.
func (s *Store) UpdateNodeDefaultScope(ctx context.Context, pubkey, scope string) error {
	if scope == "" {
		return nil // #1534: refuse the invalid write; shouldUpdateDefaultScope is the primary guard
	}
	return s.db.UpdateNodeDefaultScope(ctx, pubkey, scope)
}

// UpdateNodeConfiguredScope records the region scopes a node has CONFIGURED,
// as concrete evidence from an observer /neighbors report (#1865). Unlike
// UpdateNodeDefaultScope (inferred, overwritten on every observation), this is
// only called for the observer's own `self` scopes and for neighbors whose OTA
// scope query returned status="responded" — so the caller, not this method,
// gates on status. An empty scope IS a valid "responded, no scopes configured"
// statement and is stored; a timeout must simply not reach this method.
//
// reportedAt is the report envelope timestamp (ISO-8601). It is stored in
// configured_scope_at and used for last-write-wins: an out-of-order older
// report must not clobber a newer confirmed value. A blank reportedAt skips
// the ordering guard (always writes).
func (s *Store) UpdateNodeConfiguredScope(ctx context.Context, pubkey, scope string, reportedAt time.Time) error {
	if pubkey == "" {
		return nil
	}
	scope = normalizeScopeList(scope)

	var at *time.Time
	if !reportedAt.IsZero() {
		at = &reportedAt
	}
	return s.db.UpdateNodeConfiguredScope(ctx, pubkey, scope, at)
}

func advertNode(payload *decoder.Payload, at time.Time) *database.NodeInput {
	role, ok := decoder.AdvertRole(payload.Flags)
	if !ok {
		return nil
	}
	return &database.NodeInput{
		PublicKey: payload.PubKey,
		Name:      payload.Name,
		Role:      role,
		Lat:       payload.Lat,
		Lon:       payload.Lon,
		RxTime:    at,
	}
}
