package main

// Observation (observation-level data).
type ObservationResponse struct {
	ID           int       `json:"id"`
	RawHex       *string   `json:"raw_hex"`
	Timestamp    *string   `json:"timestamp"`
	ObserverID   *string   `json:"observer_id"`
	ObserverName *string   `json:"observer_name"`
	Direction    *string   `json:"direction"`
	SNR          *float64  `json:"snr"`
	RSSI         *float64  `json:"rssi"`
	Score        *int      `json:"score"`
	Hash         *string   `json:"hash"`
	RouteType    *int      `json:"route_type"`
	PayloadType  *int      `json:"payload_type"`
	PayloadVer   *int      `json:"payload_version"`
	PathJSON     *string   `json:"path_json"`
	DecodedJSON  *string   `json:"decoded_json"`
	CreatedAt    *string   `json:"created_at"`
	ObserverIATA *string   `json:"observer_iata"`
	ResolvedPath []*string `json:"resolved_path,omitzero"`
}
