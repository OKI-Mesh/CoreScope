package main

// Issue #1009: chunked Load with early HTTP readiness.
//
// These tests gate three behaviors:
//   (a) FirstChunkReady() unblocks BEFORE LoadChunked returns, so the
//       HTTP listener can bind after the first chunk completes while
//       remaining rows continue loading in the background.
//   (b) loadStatusMiddleware stamps an X-CoreScope-Load-Status header
//       with "loading" + progress while a load is in flight, flipping
//       to "ready" once LoadComplete() reports true.
//   (c) LoadChunked honors the configured chunkSize: the per-chunk
//       progress callback fires once per chunk, so a 2500-row DB with
//       chunkSize=1000 must yield 3 callbacks (1000 + 1000 + 500).
//
// Each subtest fails on an assertion (not a build error) when the
// production code is absent — that is the red-commit contract.

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
)

func openChunkedTestStore(t *testing.T, numTx int) *PacketStore {
	t.Helper()
	db := dbtest.New(t)
	dbtest.SeedTransmissions(t, db, numTx)
	return NewPacketStore(db, &PacketStoreConfig{})
}

// (a) FirstChunkReady fires before LoadChunked returns.
func TestLoadChunked_FirstChunkReadyBeforeComplete(t *testing.T) {
	ctx := t.Context()
	store := openChunkedTestStore(t, 2500)

	doneCh := make(chan error, 1)
	go func() { doneCh <- store.LoadChunked(ctx, 500) }()

	drained := false // true once doneCh's single value has been received
	select {
	case <-store.FirstChunkReady():
		// Good: first chunk signaled. Load may or may not have completed
		// for tiny test DBs, but the gate must have fired without
		// requiring the full load.
	case err := <-doneCh:
		drained = true
		// If load completed before we could observe the signal, the
		// signal still must be closed.
		if err != nil {
			t.Fatalf("LoadChunked: %v", err)
		}
		select {
		case <-store.FirstChunkReady():
		default:
			t.Fatal("FirstChunkReady channel must be closed after LoadChunked completes")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("FirstChunkReady did not fire within 10s — listener would never bind")
	}

	// Drain background completion (unless the first select already did).
	if !drained {
		select {
		case err := <-doneCh:
			if err != nil {
				t.Fatalf("LoadChunked returned error: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("LoadChunked never returned")
		}
	}

	if !store.LoadComplete() {
		t.Fatal("LoadComplete() must report true after LoadChunked returns")
	}
}

// (b) Middleware stamps X-CoreScope-Load-Status correctly across the
//
//	loading→ready transition.
func TestLoadStatusMiddleware_HeaderTransition(t *testing.T) {
	ctx := t.Context()
	store := openChunkedTestStore(t, 100)

	handler := loadStatusMiddleware(store, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Pre-load: header must report "loading".
	req := httptest.NewRequest("GET", "/api/healthz", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if got := w.Header().Get("X-CoreScope-Load-Status"); got == "" || got == "ready" {
		t.Fatalf("expected loading status header before Load, got %q", got)
	}

	if err := store.LoadChunked(ctx, 50); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}

	// Post-load: header must report "ready".
	req2 := httptest.NewRequest("GET", "/api/healthz", nil)
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, req2)
	if got := w2.Header().Get("X-CoreScope-Load-Status"); got != "ready" {
		t.Fatalf("expected X-CoreScope-Load-Status=ready after load, got %q", got)
	}
}

// (c) LoadChunked honors the chunkSize argument — progress callback
//
//	fires once per chunk.
func TestLoadChunked_ChunkSizeHonored(t *testing.T) {
	ctx := t.Context()

	store := openChunkedTestStore(t, 2500)

	var chunks []int
	store.OnChunkLoaded(func(rowsThisChunk, totalRows int) {
		chunks = append(chunks, rowsThisChunk)
	})

	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}

	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks for 2500 rows @ chunkSize=1000, got %d (sizes=%v)", len(chunks), chunks)
	}
	if chunks[0] != 1000 || chunks[1] != 1000 || chunks[2] != 500 {
		t.Fatalf("expected chunk sizes [1000,1000,500], got %v", chunks)
	}
}

// (d) Config plumbing: DB.Load.ChunkSize threads through.
func TestConfig_DBLoadChunkSize(t *testing.T) {
	c := &Config{}
	if got := c.DBLoadChunkSize(); got != 10000 {
		t.Fatalf("DBLoadChunkSize() default = %d, want 10000", got)
	}
	c.DB = &DBConfig{Load: &dbLoadConfig{ChunkSize: 2500}}
	if got := c.DBLoadChunkSize(); got != 2500 {
		t.Fatalf("DBLoadChunkSize() configured = %d, want 2500", got)
	}
}

// TestLoadChunked_LoadsRowsAtOrBelowZero guards the first chunk's cursor.
// Production ids come from an identity column and start at 1, but the E2E fixture
// holds rows with id 0 and a negative sentinel (#1791). A start cursor of -1 left
// the sentinel in the database and out of the store.
//
// The rows are inserted with OVERRIDING SYSTEM VALUE because the id column is
// GENERATED ALWAYS and no insert method can set it.
func TestLoadChunked_LoadsRowsAtOrBelowZero(t *testing.T) {
	db := setupTestDB(t)
	t.Cleanup(func() { db.Close() })
	ctx := t.Context()

	firstSeen := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ids := []int64{-1000000, 0, 1}
	for i, id := range ids {
		if _, err := db.Pool().Exec(ctx, `
			INSERT INTO transmissions (id, raw_hex, hash, first_seen, route_type, payload_type, payload_version)
			OVERRIDING SYSTEM VALUE
			VALUES ($1, $2, $3, $4, 1, 6, 0)`,
			id, []byte{0x19, 0x00, 0x01, byte(i)}, fmt.Sprintf("cursor%016d", i), firstSeen.Add(time.Duration(i)*time.Minute),
		); err != nil {
			t.Fatalf("insert transmission %d: %v", id, err)
		}
		if _, err := db.Pool().Exec(ctx, `
			INSERT INTO observations (transmission_id, observer_idx, direction, snr, rssi, score, path_hash, timestamp, hop_prefixes, hop_nodes)
			VALUES ($1, NULL, 'rx', 7.0, -88, 0, NULL, $2, '{}', '{}')`,
			id, firstSeen.Add(time.Duration(i)*time.Minute),
		); err != nil {
			t.Fatalf("insert observation for %d: %v", id, err)
		}
	}

	store := loadStoreFromDB(t, db, 512)

	loaded := map[int64]bool{}
	for _, packet := range store.packets {
		loaded[int64(packet.ID)] = true
	}
	for _, id := range ids {
		if !loaded[id] {
			t.Errorf("transmission %d is in the database but was not loaded into the store", id)
		}
	}
}
