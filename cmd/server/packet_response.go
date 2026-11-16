package main

import (
	"github.com/OKI-Mesh/CoreScope/internal/decoder"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// broadcastDecoded is the "decoded" part of a live message: header, payload, and
// for TRACE the path with hopsCompleted (#683).
func broadcastDecoded(tx *StoreTx) map[string]interface{} {
	decoded := map[string]interface{}{
		"header": map[string]interface{}{
			"payloadTypeName": tx.PayloadType.String(),
		},
	}
	if tx.DecodedJSON != "" {
		if payload := tx.ParsedDecoded(); payload != nil {
			// Copy the cached map so consumers cannot mutate the shared cache (#1871).
			cp := make(map[string]interface{}, len(payload))
			for k, v := range payload {
				cp[k] = v
			}
			decoded["payload"] = cp
		}
	}
	if tx.PayloadType != nil && *tx.PayloadType == types.PayloadTRACE && tx.RawHex != "" {
		if dp, err := decoder.DecodePacket(tx.RawHex, nil, false); err == nil {
			decoded["path"] = dp.Path
		}
	}
	return decoded
}
