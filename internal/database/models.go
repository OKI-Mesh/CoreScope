package database

import (
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// NewTransmission is a transmission as seen by WebSocket polling.
type NewTransmission struct {
	ID             int64
	RawHex         *string
	Hash           *string
	FirstSeen      *time.Time
	RouteType      *types.RouteType
	PayloadType    *types.PayloadType
	PayloadVersion *int32
	DecodedJSON    *string
}
