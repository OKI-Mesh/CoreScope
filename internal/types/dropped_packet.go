// internal/types/dropped_packet.go
package types

// DroppedPacket holds data for a packet rejected during ingest.
type DroppedPacket struct {
	Hash         string
	RawHex       string
	Reason       string
	ObserverID   string
	ObserverName string
	NodePubKey   string
	NodeName     string
}
