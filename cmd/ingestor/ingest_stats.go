package main

import (
	"sync"
	"sync/atomic"
)

// IngestStats tracks operational metrics for the ingestor database.
type IngestStats struct {
	TransmissionsInserted  atomic.Int64
	ObservationsInserted   atomic.Int64
	DuplicateTransmissions atomic.Int64
	NodeUpserts            atomic.Int64
	ObserverUpserts        atomic.Int64
	WriteErrors            atomic.Int64
	SignatureDrops         atomic.Int64
	// RelayTouches counts nodes.last_seen refreshes driven by relay
	// participation rather than an ADVERT (#1598).
	RelayTouches atomic.Int64
	// WALCommits tracks every successful tx.Commit() that may have flushed
	// WAL pages.
	WALCommits atomic.Int64
	// BackfillUpdates tracks per-named-backfill row write counts so an
	// infinite-loop backfill (cf #1119) is obvious from the perf page.
	BackfillUpdates sync.Map // name (string) -> *atomic.Int64
}

// IncBackfill increments the backfill counter for the given name, allocating
// the counter on first use.
func (s *IngestStats) IncBackfill(name string) {
	v, ok := s.BackfillUpdates.Load(name)
	if !ok {
		nc := new(atomic.Int64)
		actual, loaded := s.BackfillUpdates.LoadOrStore(name, nc)
		if loaded {
			v = actual
		} else {
			v = nc
		}
	}
	v.(*atomic.Int64).Add(1)
}

// SnapshotBackfills returns a name->count copy of all backfill counters.
func (s *IngestStats) SnapshotBackfills() map[string]int64 {
	out := make(map[string]int64)
	s.BackfillUpdates.Range(func(k, v interface{}) bool {
		out[k.(string)] = v.(*atomic.Int64).Load()
		return true
	})
	return out
}
