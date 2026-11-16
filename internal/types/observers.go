package types

// ObserverMeta holds optional observer hardware metadata.
type ObserverMeta struct {
	Model         *string  // e.g., L1
	Firmware      *string  // firmware version string
	ClientVersion *string  // client app version string
	Radio         *string  // radio chipset/platform string
	BatteryMv     *int     // millivolts, always integer
	UptimeSecs    *int64   // seconds, always integer
	NoiseFloor    *float64 // dBm, may have decimals
	TxAirSecs     *int     // cumulative TX seconds since boot
	RxAirSecs     *int     // cumulative RX seconds since boot
	RecvErrors    *int     // cumulative CRC/decode failures since boot
	PacketsSent   *int     // cumulative packets sent since boot
	PacketsRecv   *int     // cumulative packets received since boot
	// CanRelay reflects the firmware 1.16 /status `repeat` flag (#1290).
	// nil means the firmware did not send the field — caller must
	// preserve the existing observers.can_relay value (default 1).
	// true → relay-capable (`repeat:on`); false → listener-only
	// (`repeat:off`), which causes the server-side disambiguator to
	// exclude this observer's pubkey from path-hop candidate sets.
	CanRelay *bool
}
