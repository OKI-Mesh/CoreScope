package main

// WSMessage is the envelope for every message sent over the WebSocket.
type WSMessage struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}
