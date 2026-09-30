package main

import (
	"encoding/json"
	"net/http/httptest"
	"sync"
	"testing"
)

// TestObserverAnalyticsByTxIDRace is the regression guard for #134.
//
// handleObserverAnalytics snapshots byObserver under the store RLock and
// releases it immediately (#1481), then calls enrichObs in the unlocked loop.
// enrichObs read s.byTxID, so that map read happened with no lock held while
// the ingestor was writing byTxID under s.mu.Lock — a fatal "concurrent map
// read and map write" (the observed prod crash). The fix resolves each
// observation's *StoreTx under the snapshot RLock and passes it to
// enrichObsWithTx, so byTxID is only ever touched with s.mu held.
//
// Run under -race: before the fix this reports a data race on s.byTxID; after
// it, byTxID access is fully synchronized and the test is clean. The requests
// must also keep returning 200 (the enrichment still produces valid output).
func TestObserverAnalyticsByTxIDRace(t *testing.T) {
	srv, router := setupTestServer(t)
	store := srv.store

	stop := make(chan struct{})

	// Writer: churn byTxID under the write lock, mimicking ingest + eviction.
	// Keys are disjoint from the seeded transmission ids; the race detector
	// flags any unsynchronized read of the same map regardless of key.
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		k := 1 << 20
		for {
			select {
			case <-stop:
				return
			default:
			}
			store.mu.Lock()
			store.byTxID[k] = &StoreTx{ID: k}
			delete(store.byTxID, k-1)
			store.mu.Unlock()
			k++
		}
	}()

	// Readers: hammer the analytics handler that enriches obs1's observations.
	var readers sync.WaitGroup
	for r := 0; r < 8; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for i := 0; i < 50; i++ {
				req := httptest.NewRequest("GET", "/api/observers/obs1/analytics", nil)
				w := httptest.NewRecorder()
				router.ServeHTTP(w, req)
				if w.Code != 200 {
					t.Errorf("analytics returned %d, want 200", w.Code)
					return
				}
			}
		}()
	}

	readers.Wait()
	close(stop)
	<-writerDone

	// #141 review (Jeymz): a 200 alone would pass even if enrichment silently
	// stopped resolving the transmission. Assert a transmission-derived field
	// survives — packetTypes is keyed by tx.PayloadType, so a non-empty map
	// proves enrichObsWithTx still resolved the tx for obs1's seeded packets.
	// Run this after the writer stops, so it's a deterministic quiescent check.
	req := httptest.NewRequest("GET", "/api/observers/obs1/analytics", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("final analytics request returned %d, want 200", w.Code)
	}
	var body map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode analytics response: %v", err)
	}
	pt, ok := body["packetTypes"].(map[string]interface{})
	if !ok || len(pt) == 0 {
		t.Errorf("packetTypes empty: enrichment no longer resolves the transmission (got %v)", body["packetTypes"])
	}
}
