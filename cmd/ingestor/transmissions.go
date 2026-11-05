package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/packetpath"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// InsertTransmission inserts a decoded packet into transmissions + observations.
// Returns true if a new transmission was created (not a duplicate hash).
func (s *Store) InsertTransmission(ctx context.Context, data *PacketData) (bool, error) {
	hash := data.Hash
	if hash == "" {
		return false, nil
	}

	ingestNow := time.Now().UTC()

	// PacketData.Timestamp is server time, set in BuildPacketData (#1370), so there
	// is nothing to parse. The zero value only appears when a caller didn't set one.
	rxTime := data.Timestamp.UTC()
	if rxTime.IsZero() {
		rxTime = ingestNow
	}

	txID, isNew, err := s.db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		RawHex:           data.RawHex,
		Hash:             hash,
		RouteType:        data.RouteType,
		PayloadType:      data.PayloadType,
		PayloadVersion:   data.PayloadVersion,
		DecodedJSON:      data.DecodedJSON,
		ChannelHash:      data.ChannelHash,
		ChannelEncrypted: data.ChannelEncrypted,
		ScopeName:        scopeNameForDB(data),
		Code1:            data.Code1,
		Code2:            data.Code2,
		Advert:           data.Advert,
	}, rxTime)
	if err != nil {
		s.Stats.WriteErrors.Add(1)
		return false, fmt.Errorf("insert transmission: %w", err)
	}
	if isNew {
		s.Stats.TransmissionsInserted.Add(1)
	} else {
		s.Stats.DuplicateTransmissions.Add(1)
	}
	if data.Advert != nil {
		s.Stats.NodeUpserts.Add(1)
	}

	// Resolve observer_idx and update last_seen
	observerIdx := s.db.ResolveObserverAndTouch(ctx, data.ObserverID, ingestNow)

	// Resolve hop prefixes to full pubkeys for `observations.resolved_path`.
	// Per #1547: this writer was lost in the #1289 refactor and lives in
	// the ingestor now. Per #1560: use the context-aware resolver so
	// 1-byte prefix collisions are disambiguated via NeighborGraph
	// adjacency (anchored on from_pubkey for ADVERTs, previous hop
	// otherwise). Empty resolved JSON → NULL via nilIfEmpty.
	hops := parsePathArray(data.PathJSON)
	resolved := resolvePathWithContext(
		hops,
		strings.ToLower(data.FromPubkey),
		s.neighborGraph.load(),
		s.prefixIdx.load(),
	)
	obs := database.ObservationInput{
		TransmissionID: txID,
		ObserverIdx:    observerIdx,
		Direction:      data.Direction,
		SNR:            data.SNR,
		RSSI:           data.RSSI,
		Score:          data.Score,
		PathHash:       types.HopList(hops).Hash(),
		Timestamp:      rxTime,
		RawHex:         data.RawHex,
	}

	_, inserted, err := s.db.InsertObservationWithHops(ctx, obs, hops, resolved)
	if err != nil {
		s.Stats.WriteErrors.Add(1)
		log.Printf("[db] observation+hops insert failed, rolled back: %v", err)
		return false, fmt.Errorf("observation insert: %w", err)
	}
	if inserted {
		s.Stats.ObservationsInserted.Add(1)
	}
	s.touchRelayNodes(ctx, resolvedPubkeys(resolved), rxTime)
	if err := s.db.BumpTransmissionLastSeen(ctx, txID, rxTime); err != nil {
		log.Printf("[db] tx last_seen bump (non-fatal): %v", err)
	}

	return isNew, nil
}

// parsePathArray returns the hop strings from a path_json blob.
// Defensive against missing/invalid JSON.
func parsePathArray(s string) []string {
	if s == "" || s == "[]" {
		return nil
	}
	var arr []string
	if json.Unmarshal([]byte(s), &arr) != nil {
		return nil
	}
	return packetpath.LowerHops(arr)
}
