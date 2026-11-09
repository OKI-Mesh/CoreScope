package main

// Types generated from proto/ definitions for compile-time type safety.
// Every API response is a typed struct — no map[string]interface{}.

// ─── Common ────────────────────────────────────────────────────────────────────

type PaginationInfo struct {
	Total  int `json:"total"`
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
}

type ErrorResp struct {
	Error string `json:"error"`
}

// ─── Nodes ─────────────────────────────────────────────────────────────────────

// type NodeDetailResponse struct {
// 	Node          api.Node             `json:"node"`
// 	RecentAdverts []api.NodeAdvert `json:"recentAdverts"`
// }

type NetworkStatusResponse struct {
	Total      int            `json:"total"`
	Active     int            `json:"active"`
	Degraded   int            `json:"degraded"`
	Silent     int            `json:"silent"`
	RoleCounts map[string]int `json:"roleCounts"`
}

// ─── WebSocket ─────────────────────────────────────────────────────────────────

type WSMessage struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}
