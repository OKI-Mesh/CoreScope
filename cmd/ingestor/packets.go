package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/decoder"
	"github.com/OKI-Mesh/CoreScope/internal/packetpath"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// BuildPacketData constructs a PacketData from a decoded packet and MQTT message.
// path_json is derived directly from raw_hex header bytes (not decoded.Path.Hops)
// to guarantee the stored path always matches the raw bytes. This matters for
// TRACE packets where decoded.Path.Hops is overwritten with payload hops (#886).
//
// Timestamp is server ingest time (time.Now()), NOT msg.Timestamp (#1370):
// PR #1233 (commit 498fbc03) routed the envelope timestamp into
// PacketData.Timestamp on the premise that uploader-stamped envelope time
// was trustworthy. Issue #1370 disproved that premise — observers with
// broken client clocks (staging Voodoo3 tx 304114: 4/5 obs stamped 18:42
// while genuine receive was 01:42) poisoned transmissions.first_seen /
// observations.timestamp and dragged the /api/channels lastActivity 7h
// into the past. Packet ordering is owned by the server clock; client
// clocks are untrusted. msg.Timestamp still flows into observer.last_seen
// via UpsertObserverAt — that's #1233's MAX/MIN guarded path and is fine.
func BuildPacketData(msg *MQTTPacketMessage, decoded *decoder.DecodedPacket, observerID, region string, regionSet *regionKeySet) *PacketData {
	pathJSON := "[]"
	// For TRACE packets, path_json must be the payload-decoded route hops
	// (decoded.Path.Hops), NOT the raw_hex header bytes which are SNR values.
	// For all other packet types, derive path from raw_hex (#886).
	if !packetpath.PathBytesAreHops(decoded.Header.PayloadType) {
		if len(decoded.Path.Hops) > 0 {
			b, _ := json.Marshal(decoded.Path.Hops)
			pathJSON = string(b)
		}
	} else if hops, err := packetpath.DecodePathFromRawHex(msg.Raw); err == nil && len(hops) > 0 {
		b, _ := json.Marshal(hops)
		pathJSON = string(b)
	}

	pd := &PacketData{
		RawHex:           msg.Raw,
		Timestamp:        time.Now().UTC(), // #1370 (counters #1233)
		ObserverID:       observerID,
		ObserverName:     msg.Origin,
		SNR:              msg.SNR,
		RSSI:             msg.RSSI,
		Score:            msg.Score,
		Direction:        msg.Direction,
		Hash:             decoder.ComputeContentHash(msg.Raw),
		RouteType:        decoded.Header.RouteType,
		PayloadType:      decoded.Header.PayloadType,
		PayloadVersion:   decoded.Header.PayloadVersion,
		PathJSON:         pathJSON,
		DecodedJSON:      decoder.PayloadJSON(&decoded.Payload),
		ChannelEncrypted: false,
	}

	// Region priority: payload field > topic-derived parameter (#788)
	if msg.Region != "" {
		pd.Region = msg.Region
	} else {
		pd.Region = region
	}

	// Populate channel_hash for fast channel queries (#762)
	if decoded.Header.PayloadType == types.PayloadGRP_TXT {
		pd.ChannelHash, pd.ChannelEncrypted = decoded.Payload.ChannelReference()
	}

	if decoded.TransportCodes != nil {
		pd.Code1 = decoded.TransportCodes.Code1
		pd.Code2 = decoded.TransportCodes.Code2
		if decoded.TransportCodes.Code1 != "0000" {
			pd.IsTransportScoped = true
			pd.ScopeName = regionSet.matchScopeName(decoded.Header.PayloadType, decoded.PayloadRaw, decoded.TransportCodes.Code1)
		}
	}

	// Populate from_pubkey at write time (#1143). ADVERTs carry the
	// originating node's pubkey directly; other packet types stay NULL
	// (downstream attribution queries handle NULL gracefully).
	if decoded.Header.PayloadType == types.PayloadADVERT && decoded.Payload.PubKey != "" {
		pd.FromPubkey = decoded.Payload.PubKey
	}

	return pd
}

// InsertDroppedPacket records a rejected packet in the dropped_packets table.
func (s *Store) InsertDroppedPacket(ctx context.Context, dp *database.DroppedPacket) error {
	err := s.db.InsertDroppedPacket(ctx, *dp)
	if err != nil {
		s.Stats.WriteErrors.Add(1)
		return fmt.Errorf("insert dropped packet: %w", err)
	}
	s.Stats.SignatureDrops.Add(1)
	return nil
}

// PruneDroppedPackets removes dropped_packets older than retentionDays.
func (s *Store) PruneDroppedPackets(ctx context.Context, retentionDays int) (int64, error) {
	if retentionDays <= 0 {
		return 0, nil
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -retentionDays)
	n, err := s.db.PruneDroppedPackets(ctx, cutoff)
	if err != nil {
		return 0, fmt.Errorf("prune dropped packets: %w", err)
	}
	if n > 0 {
		log.Printf("Pruned %d dropped packet(s) older than %d days", n, retentionDays)
	}
	return n, nil
}

// PacketData holds the data needed to insert a packet into the DB.
type PacketData struct {
	RawHex            string
	Timestamp         time.Time
	ObserverID        string
	ObserverName      string
	SNR               *float64
	RSSI              *float64
	Score             *float64
	Direction         *string
	Hash              string
	RouteType         types.RouteType
	PayloadType       types.PayloadType
	PayloadVersion    int
	PathJSON          string
	DecodedJSON       string
	ChannelHash       string // grouping key for channel queries (#762)
	ScopeName         string // matched region name, or "" for unknown-scoped
	IsTransportScoped bool   // true when route_type IN (0,3) AND Code1 ≠ "0000"
	Code1             string // transport code 1 (scope), "" when the route carries none
	Code2             string // transport code 2 (return-region hint), "" when absent
	Region            string // observer region: payload > topic > source config (#788)
	Foreign           bool   // true when ADVERT GPS lies outside configured geofilter (#730)
	FromPubkey        string // pubkey of the originating node, for exact-match attribution (#1143)
	Advert            *database.NodeInput
	ChannelEncrypted  bool
}
