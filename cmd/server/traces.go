package main

type TraceEntry struct {
	Observer     *string  `json:"observer"`
	ObserverName *string  `json:"observer_name"`
	Time         string   `json:"time"`
	SNR          *float64 `json:"snr"`
	RSSI         *float64 `json:"rssi"`
	PathJSON     string   `json:"path_json"`
}

type TraceResponse struct {
	Traces []TraceEntry `json:"traces"`
}
