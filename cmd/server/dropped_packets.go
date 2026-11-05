package main

type DroppedPacketResponse struct {
	ID           int64  `json:"id"`
	Hash         any    `json:"hash"`
	RawHex       any    `json:"raw_hex,omitempty"`
	Reason       string `json:"reason"`
	ObserverID   any    `json:"observer_id"`
	ObserverName any    `json:"observer_name"`
	NodePubkey   any    `json:"node_pubkey"`
	NodeName     any    `json:"node_name"`
	DroppedAt    string `json:"dropped_at"`
}
