package main

import (
	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// packetObservation is one observation nested inside a packet. rp is the
// resolved path the caller fetched with DB.ResolvedPaths (nil when none).
func packetObservation(o *StoreObs, rp []*string) api.PacketObservation {
	po := api.PacketObservation{
		Id:           o.ID,
		ObserverId:   optString(o.ObserverID),
		ObserverName: optString(o.ObserverName),
		ObserverIata: optString(o.ObserverIATA),
		Direction:    optString(o.Direction),
		Snr:          o.SNR,
		Rssi:         o.RSSI,
		PathJson:     optString(o.PathJSON),
		Timestamp:    types.WireTimeMillis{Time: o.Timestamp},
	}
	if rp != nil {
		po.ResolvedPath = &rp
	}
	return po
}

// observationDetail is an in-memory observation with its packet's hash and
// frame. tx may be nil (the packet was evicted), in which case hash and raw_hex
// are omitted. raw_hex comes from the transmission (#881): obs.RawHex is not
// retained when loading or ingesting the store, since it duplicates the
// transmission's frame (same content hash, same frame).
func observationDetail(o *StoreObs, tx *StoreTx, rp []*string) api.ObservationDetail {
	d := api.ObservationDetail{
		Id:           o.ID,
		ObserverId:   optString(o.ObserverID),
		ObserverName: optString(o.ObserverName),
		ObserverIata: optString(o.ObserverIATA),
		Direction:    optString(o.Direction),
		Snr:          o.SNR,
		Rssi:         o.RSSI,
		PathJson:     optString(o.PathJSON),
		Timestamp:    types.WireTimeMillis{Time: o.Timestamp},
	}
	if rp != nil {
		d.ResolvedPath = &rp
	}
	if tx != nil {
		d.Hash = optString(tx.Hash)
		if o.RawHex != "" {
			d.RawHex = optString(o.RawHex)
		} else {
			d.RawHex = optString(tx.RawHex)
		}
	}
	return d
}

// observationPacket is an in-memory observation with its packet's payload
// fields, the shape /api/packets/{id} returns for an observation id. tx may be
// nil (the packet was evicted), in which case the packet fields are omitted.
func observationPacket(o *StoreObs, tx *StoreTx, rp []*string) api.ObservationPacket {
	p := api.ObservationPacket{
		Id:           o.ID,
		ObserverId:   optString(o.ObserverID),
		ObserverName: optString(o.ObserverName),
		ObserverIata: optString(o.ObserverIATA),
		Direction:    optString(o.Direction),
		Snr:          o.SNR,
		Rssi:         o.RSSI,
		Score:        o.Score,
		PathJson:     optString(o.PathJSON),
		Timestamp:    types.WireTimeMillis{Time: o.Timestamp},
	}
	if rp != nil {
		p.ResolvedPath = &rp
	}
	if tx != nil {
		p.Hash = optString(tx.Hash)
		if o.RawHex != "" {
			p.RawHex = optString(o.RawHex)
		} else {
			p.RawHex = optString(tx.RawHex)
		}
		p.PayloadType = tx.PayloadType
		p.RouteType = tx.RouteType
		p.DecodedJson = optString(tx.DecodedJSON)
	}
	return p
}

// dbObservationDetail is a database observation as the batch and detail
// routes return it. The database path has no per-observation frame, so raw_hex
// is filled in by the caller. The observation's transmission id is not sent,
// matching the in-memory path.
func dbObservationDetail(o database.Observation) api.ObservationDetail {
	d := api.ObservationDetail{
		Id:           int(o.ID),
		ObserverId:   optString(o.ObserverID),
		ObserverName: optString(o.ObserverName),
		ObserverIata: optString(o.ObserverIATA),
		Direction:    optString(o.Direction),
		Snr:          o.SNR,
		Rssi:         o.RSSI,
		PathJson:     optString(hopsJSON(o.HopPrefixes())),
		Timestamp:    types.WireTimeMillis{Time: o.Timestamp},
	}
	// resolved_path only when at least one hop resolved, as on the store path.
	rp := make([]*string, len(o.Path))
	resolved := false
	for i, hop := range o.Path {
		if hop.ResolvedPubkey != nil && *hop.ResolvedPubkey != "" {
			rp[i] = hop.ResolvedPubkey
			resolved = true
		}
	}
	if resolved {
		d.ResolvedPath = &rp
	}
	return d
}
