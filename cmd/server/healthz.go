package main

import (
	"encoding/json"
	"net/http"
	"sync/atomic"

	"github.com/OKI-Mesh/CoreScope/api"
)

// readiness tracks whether background init goroutines have completed.
// Set to 1 once store.Load, pickBestObservation, and neighbor graph build are done.
var readiness atomic.Int32

// handleHealthz returns 200 when the server is ready to serve queries,
// or 503 while background initialization is still running.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if readiness.Load() == 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(api.HealthzLoadingResponse{Ready: false, Reason: "loading"})
		return
	}

	var loadedTx, loadedObs int
	if s.store != nil {
		s.store.mu.RLock()
		loadedTx = len(s.store.packets)
		for _, p := range s.store.packets {
			loadedObs += len(p.Observations)
		}
		s.store.mu.RUnlock()
	}

	w.WriteHeader(http.StatusOK)
	resp := api.HealthzResponse{
		Ready:     true,
		LoadedTx:  loadedTx,
		LoadedObs: loadedObs,
		// The from_pubkey backfill is an ETL step now, so there is never
		// progress to report. The field stays on the wire because the
		// warm-up banner (public/warmup-banner.js) waits for done == true.
		FromPubkeyBackfill: api.FromPubkeyBackfill{
			Total:     0,
			Processed: 0,
			Done:      true,
		},
	}
	// PR #1609 M1: surface per-MQTT-source receipt vs write-path
	// liveness so operators can distinguish "broker alive, write
	// path stuck" (lastReceiptUnix recent, lastMessageUnix stale)
	// from "everything stalled" (both stale). Additive — older
	// ingestor builds simply produce no entry and the field is
	// omitted. Schema-compatible with prior /healthz consumers.
	if liveness := readIngestorSourceLiveness(); len(liveness) > 0 {
		sources := make(map[string]api.SourceLiveness, len(liveness))
		for name, l := range liveness {
			sources[name] = api.SourceLiveness{LastReceiptUnix: l.LastReceiptUnix, LastMessageUnix: l.LastMessageUnix}
		}
		resp.IngestLiveness = &sources
	}
	json.NewEncoder(w).Encode(resp)
}
