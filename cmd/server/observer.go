package main

import (
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// newObserver builds the wire form of an observer. Its position and role come
// from the node with the same key, so the caller fills Lat, Lon and NodeRole
// when there is one.
func newObserver(o *database.Observer, packetsLastHour int, now time.Time) api.Observer {
	obs := api.Observer{
		Id:              o.Pubkey,
		Name:            o.Name,
		Iata:            o.IATA,
		LastSeen:        types.WireTimeSeconds{Time: o.LastSeen},
		PacketCount:     o.PacketCount,
		Model:           o.Model,
		Firmware:        o.Firmware,
		ClientVersion:   o.ClientVersion,
		Radio:           o.Radio,
		BatteryMv:       o.BatteryMv,
		UptimeSecs:      o.UptimeSecs,
		NoiseFloor:      o.NoiseFloor,
		LastPacketAt:    types.NewWireTimeSecondsPtr(o.LastPacketAt),
		PacketsLastHour: packetsLastHour,
		CanRelay:        relayReport(o.CanRelay, o.CanRelaySeen),
	}
	// A zero first_seen is an observer whose first sighting was never recorded.
	if !o.FirstSeen.IsZero() {
		obs.FirstSeen = &types.WireTimeSeconds{Time: o.FirstSeen}
	}
	applyObserverNaiveClock(&obs, o, now)
	return obs
}
