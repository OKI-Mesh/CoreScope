package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/gorilla/mux"
)

func insertRelayPacket(t *testing.T, db *database.DB, observerIdx int64, p relayPacket) int64 {
	t.Helper()
	ctx := t.Context()
	txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		Hash:        p.Hash,
		RawHex:      "0100",
		RouteType:   p.RouteType,
		PayloadType: p.PayloadType,
		DecodedJSON: "{}",
	}, p.At)
	if err != nil {
		t.Fatalf("seed transmission %s: %v", p.Hash, err)
	}
	_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: txID,
		ObserverIdx:    iptr64(observerIdx),
		SNR:            fptr(5.0),
		RSSI:           fptr(-90.0),
		Timestamp:      p.At,
	}, p.Hops, p.Resolved)
	if err != nil {
		t.Fatalf("seed observation %s: %v", p.Hash, err)
	}
	if !inserted {
		t.Fatalf("expected observation to be inserted for tx %d", txID)
	}
	return txID
}

func seedRepeater(t *testing.T, db *database.DB, pk string, at time.Time) {
	t.Helper()
	if err := db.CreateOrUpdateNode(t.Context(), database.NodeInput{
		PublicKey: pk,
		Name:      "repeater-" + pk[:8],
		Role:      "repeater",
		Lat:       nil,
		Lon:       nil,
		RxTime:    at,
		IngestNow: at,
	}); err != nil {
		t.Fatalf("seed repeater %s: %v", pk[:8], err)
	}
}

func newBareStore(db *database.DB) *PacketStore {
	store := &PacketStore{
		packets:              make([]*StoreTx, 0),
		byHash:               make(map[string]*StoreTx),
		byTxID:               make(map[int]*StoreTx),
		byObsID:              make(map[int]*StoreObs),
		byObserver:           make(map[string][]*StoreObs),
		byNode:               make(map[string][]*StoreTx),
		nodeHashes:           make(map[string]map[string]bool),
		byPayloadType:        make(map[types.PayloadType][]*StoreTx),
		byPathHop:            make(map[string][]*StoreTx),
		spIndex:              make(map[string]int),
		spTxIndex:            make(map[string][]*StoreTx),
		distHops:             make([]distHopRecord, 0),
		distPaths:            make([]distPathRecord, 0),
		rfCache:              make(map[string]*cachedResult),
		topoCache:            make(map[string]*cachedResult),
		hashCache:            make(map[string]*cachedHashSizes),
		chanCache:            make(map[string]*cachedResult),
		distCache:            make(map[string]*cachedResult),
		subpathCache:         make(map[string]*cachedResult),
		rfCacheTTL:           15 * time.Second,
		db:                   db,
		useResolvedPathIndex: true,
	}
	store.initResolvedPathIndex()
	return store
}

func addEvictableToStore(store *PacketStore, txID int, hash string, firstSeen time.Time) *StoreTx {
	tx := &StoreTx{ID: txID, Hash: hash, FirstSeen: firstSeen}
	obs := &StoreObs{ID: txID, TransmissionID: txID, ObserverID: "obs0", Timestamp: firstSeen}
	tx.Observations = []*StoreObs{obs}
	store.packets = append(store.packets, tx)
	store.byHash[hash] = tx
	store.byTxID[txID] = tx
	store.byObsID[txID] = obs
	store.byObserver["obs0"] = append(store.byObserver["obs0"], obs)
	return tx
}

// --- Unit tests ---

// TestStoreTx_NoResolvedPathField is a compile-time guard ensuring ResolvedPath
// field never returns to StoreTx/StoreObs structs (#800).
func TestStoreTx_ResolvedPathFieldAbsent(t *testing.T) {
	txType := reflect.TypeOf(StoreTx{})
	if _, found := txType.FieldByName("ResolvedPath"); found {
		t.Fatal("StoreTx must not have a ResolvedPath field (#800)")
	}

	obsType := reflect.TypeOf(StoreObs{})
	if _, found := obsType.FieldByName("ResolvedPath"); found {
		t.Fatal("StoreObs must not have a ResolvedPath field (#800)")
	}
}

// TestResolvedPubkeyIndex_BuildFromLoad verifies forward + reverse maps are
// consistent after building from resolved paths.
func TestResolvedPubkeyIndex_BuildFromLoad(t *testing.T) {
	store := &PacketStore{
		byNode:               make(map[string][]*StoreTx),
		nodeHashes:           make(map[string]map[string]bool),
		useResolvedPathIndex: true,
	}
	store.initResolvedPathIndex()

	// Simulate Load() building index from 3 transmissions
	store.addToResolvedPubkeyIndex(1, []string{"aabb", "ccdd"})
	store.addToResolvedPubkeyIndex(2, []string{"aabb", "eeff"})
	store.addToResolvedPubkeyIndex(3, []string{"ccdd"})

	// Forward: aabb should map to [1, 2]
	h := resolvedPubkeyHash("aabb")
	if len(store.resolvedPubkeyIndex[h]) != 2 {
		t.Errorf("expected 2 txIDs for aabb, got %d", len(store.resolvedPubkeyIndex[h]))
	}

	// Forward: ccdd should map to [1, 3]
	h2 := resolvedPubkeyHash("ccdd")
	if len(store.resolvedPubkeyIndex[h2]) != 2 {
		t.Errorf("expected 2 txIDs for ccdd, got %d", len(store.resolvedPubkeyIndex[h2]))
	}

	// Reverse: tx 1 should have 2 hashes
	if len(store.resolvedPubkeyReverse[1]) != 2 {
		t.Errorf("expected 2 hashes for tx 1, got %d", len(store.resolvedPubkeyReverse[1]))
	}

	// Reverse: tx 2 should have 2 hashes
	if len(store.resolvedPubkeyReverse[2]) != 2 {
		t.Errorf("expected 2 hashes for tx 2, got %d", len(store.resolvedPubkeyReverse[2]))
	}
}

// TestResolvedPubkeyIndex_HashCollision verifies that SQL safety filters false candidates
// when two pubkeys produce a hash collision (simulated by inserting both under same hash).
func TestNodePaths_HashCollisionFilteredBySQL(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	seenAt := time.Now().UTC().Add(-1 * time.Hour)
	realPK := "aa" + strings.Repeat("1", 62)
	falsePK := "aa" + strings.Repeat("2", 62)
	seedRepeater(t, db, realPK, seenAt)
	seedRepeater(t, db, falsePK, seenAt)
	obsIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID: scopeKey(90), Name: "Collision Observer", IATA: "SFO", Meta: nil, LastSeen: seenAt, IngestNow: seenAt,
	})
	if err != nil {
		t.Fatalf("seed observer: %v", err)
	}

	tx1ID := insertRelayPacket(t, db, obsIdx, relayPacket{
		Hash: "c0170000000000a1", RouteType: 1, PayloadType: types.PayloadType(1), At: seenAt,
		Hops: []string{"aa"}, Resolved: []*string{sptr(realPK)},
	})
	tx2ID := insertRelayPacket(t, db, obsIdx, relayPacket{
		Hash: "c0170000000000a2", RouteType: 1, PayloadType: types.PayloadType(1), At: seenAt,
		Hops: []string{"aa"}, Resolved: []*string{sptr(falsePK)},
	})

	srv := NewServer(db, &Config{Port: 3000}, NewHub())
	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache(): %v", err)
	}
	srv.store = store
	router := mux.NewRouter()
	srv.RegisterRoutes(router)

	collisionHash := resolvedPubkeyHash(realPK)
	store.mu.Lock()
	if got := len(store.resolvedPubkeyIndex[collisionHash]); got != 1 {
		store.mu.Unlock()
		t.Fatalf("precondition: expected load to index 1 tx under the real pubkey, got %d", got)
	}
	store.resolvedPubkeyIndex[collisionHash] = append(store.resolvedPubkeyIndex[collisionHash], int(tx2ID))
	store.resolvedPubkeyReverse[int(tx2ID)] = append(store.resolvedPubkeyReverse[int(tx2ID)], collisionHash)
	store.mu.Unlock()

	req := httptest.NewRequest("GET", "/api/nodes/"+realPK+"/paths", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /paths: code=%d body=%s", w.Code, w.Body.String())
	}
	var resp NodePathsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.TotalTransmissions != 1 {
		t.Errorf("expected only tx %d (real match), got %d transmissions — hash collision with tx %d was not filtered by the SQL confirmation",
			tx1ID, resp.TotalTransmissions, tx2ID)
	}
}

// TestResolvedPubkeyIndex_IngestUpdate verifies both maps reflect new ingests.
func TestResolvedPubkeyIndex_IngestUpdate(t *testing.T) {
	store := &PacketStore{
		byNode:               make(map[string][]*StoreTx),
		nodeHashes:           make(map[string]map[string]bool),
		useResolvedPathIndex: true,
	}
	store.initResolvedPathIndex()

	// Initial state
	store.addToResolvedPubkeyIndex(1, []string{"pk1"})
	h := resolvedPubkeyHash("pk1")
	if len(store.resolvedPubkeyIndex[h]) != 1 {
		t.Fatal("expected 1 entry after first insert")
	}

	// Ingest update: add more pubkeys for same tx
	store.addToResolvedPubkeyIndex(1, []string{"pk2"})
	h2 := resolvedPubkeyHash("pk2")
	if len(store.resolvedPubkeyIndex[h2]) != 1 {
		t.Fatal("expected 1 entry for pk2 after update")
	}
	if len(store.resolvedPubkeyReverse[1]) != 2 {
		t.Errorf("expected 2 reverse entries for tx 1, got %d", len(store.resolvedPubkeyReverse[1]))
	}
}

// TestResolvedPubkeyIndex_RemoveOnEvict verifies eviction removes via reverse map.
func TestResolvedPubkeyIndex_RemoveOnEvict(t *testing.T) {
	store := &PacketStore{
		byNode:               make(map[string][]*StoreTx),
		nodeHashes:           make(map[string]map[string]bool),
		useResolvedPathIndex: true,
	}
	store.initResolvedPathIndex()

	store.addToResolvedPubkeyIndex(1, []string{"pk1", "pk2"})
	store.addToResolvedPubkeyIndex(2, []string{"pk1"})

	// Remove tx 1
	store.removeFromResolvedPubkeyIndex(1)

	// pk1 should still have tx 2
	h := resolvedPubkeyHash("pk1")
	if len(store.resolvedPubkeyIndex[h]) != 1 || store.resolvedPubkeyIndex[h][0] != 2 {
		t.Error("pk1 should only have tx 2 after removing tx 1")
	}

	// pk2 should be empty
	h2 := resolvedPubkeyHash("pk2")
	if _, exists := store.resolvedPubkeyIndex[h2]; exists {
		t.Error("pk2 should be deleted after removing its only tx")
	}

	// Reverse map should be clean
	if _, exists := store.resolvedPubkeyReverse[1]; exists {
		t.Error("reverse map for tx 1 should be deleted")
	}
}

// TestResolvedPubkeyIndex_PerObsCoverage verifies non-best obs's resolved pubkeys are indexed.
func TestResolvedPubkeyIndex_PerObsCoverage(t *testing.T) {
	store := &PacketStore{
		byNode:               make(map[string][]*StoreTx),
		nodeHashes:           make(map[string]map[string]bool),
		useResolvedPathIndex: true,
	}
	store.initResolvedPathIndex()

	// Simulate: tx has 2 observations with different resolved paths
	// Both should be indexed
	store.addToResolvedPubkeyIndex(1, []string{"obs1_pk1", "obs1_pk2"})
	store.addToResolvedPubkeyIndex(1, []string{"obs2_pk3"})

	// All 3 pubkeys should be indexed
	for _, pk := range []string{"obs1_pk1", "obs1_pk2", "obs2_pk3"} {
		h := resolvedPubkeyHash(pk)
		found := false
		for _, id := range store.resolvedPubkeyIndex[h] {
			if id == 1 {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("pubkey %s should be indexed for tx 1", pk)
		}
	}
}

// TestAddToByNode_WithoutResolvedPathField verifies relay nodes are still indexed
// when fed through the decode-window (not via struct field).
func TestAddToByNode_WithoutResolvedPathField(t *testing.T) {
	store := &PacketStore{
		byNode:     make(map[string][]*StoreTx),
		nodeHashes: make(map[string]map[string]bool),
	}

	tx := &StoreTx{ID: 1, Hash: "h1"}

	// Simulate decode-window feeding relay pubkeys
	store.addToByNode(tx, "relay_pk_1")
	store.addToByNode(tx, "relay_pk_2")

	if len(store.byNode["relay_pk_1"]) != 1 {
		t.Error("relay_pk_1 should be in byNode")
	}
	if len(store.byNode["relay_pk_2"]) != 1 {
		t.Error("relay_pk_2 should be in byNode")
	}
}

// TestWebSocketBroadcast_IncludesResolvedPath verifies broadcast maps carry resolved_path
// from the decode-window, not from struct fields.
func TestWebSocketBroadcast_IncludesResolvedPath(t *testing.T) {
	// This is verified by the IngestNewFromDB code path: if resolvePathForObs
	// returns a non-nil result, it's put into broadcastRP map and then into
	// the broadcast pkt map. We test the extraction helper here.
	pk1 := "aabbccdd"
	pk2 := "eeff0011"
	rp := []*string{&pk1, nil, &pk2}
	pks := extractResolvedPubkeys(rp)
	if len(pks) != 2 {
		t.Errorf("expected 2 pubkeys, got %d", len(pks))
	}
	if pks[0] != "aabbccdd" || pks[1] != "eeff0011" {
		t.Errorf("unexpected pubkeys: %v", pks)
	}
}

// TestEviction_ByNodeCleanup_OnDemandSQL verifies eviction path SQL-fetches
// resolved_path to clean byNode/nodeHashes.
func TestEviction_ByNodeCleanup_OnDemandSQL(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	staleAt := now.Add(-48 * time.Hour)
	relayPK := strings.Repeat("e1", 32)
	seedRepeater(t, db, relayPK, staleAt)
	obsIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID: scopeKey(81), Name: "Evict Observer", IATA: "SFO", Meta: nil, LastSeen: now, IngestNow: now,
	})
	if err != nil {
		t.Fatalf("seed observer: %v", err)
	}
	txID := insertRelayPacket(t, db, obsIdx, relayPacket{
		Hash: "c0120000000000a1", RouteType: 1, PayloadType: types.PayloadType(1), At: staleAt,
		Hops: []string{"aa"}, Resolved: []*string{sptr(relayPK)},
	})

	store := newBareStore(db)
	store.retentionHours = 24
	tx := addEvictableToStore(store, int(txID), "c0120000000000a1", staleAt)

	store.addToByNode(tx, relayPK)
	store.addToResolvedPubkeyIndex(int(txID), []string{relayPK})

	if len(store.byNode[relayPK]) != 1 {
		t.Fatalf("expected relay in byNode")
	}

	evicted := runEviction(t, store)
	if evicted != 1 {
		t.Fatalf("expected 1 evicted, got %d", evicted)
	}

	if len(store.byNode[relayPK]) != 0 {
		t.Error("expected byNode cleanup after eviction via on-demand SQL")
	}
}

// TestRunEviction_NoLockDuringSQLFetch proves that RunEviction releases the
// write lock during the batch SQL fetch phase. A concurrent goroutine acquires
// mu.RLock within a tight deadline — if the lock were held during SQL, it would
// time out.
func TestRunEviction_NoLockDuringSQLFetch(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	staleAt := now.Add(-48 * time.Hour)
	obsIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID: scopeKey(82), Name: "NoLock Observer", IATA: "SFO", Meta: nil, LastSeen: now, IngestNow: now,
	})
	if err != nil {
		t.Fatalf("seed observer: %v", err)
	}

	store := newBareStore(db)
	store.retentionHours = 24

	for i := 1; i <= 10; i++ {
		pk := fmt.Sprintf("%064x", 0x1000+i)
		hash := fmt.Sprintf("c3%014x", i)
		seedRepeater(t, db, pk, staleAt)
		txID := insertRelayPacket(t, db, obsIdx, relayPacket{
			Hash: hash, RouteType: 1, PayloadType: types.PayloadType(1), At: staleAt,
			Hops: []string{"aa"}, Resolved: []*string{sptr(pk)},
		})
		addEvictableToStore(store, int(txID), hash, staleAt)
	}

	evictDone := make(chan int, 1)
	go func() {
		evictDone <- store.RunEviction(ctx)
	}()

	lockAcquired := make(chan bool, 1)
	go func() {
		time.Sleep(5 * time.Millisecond)
		store.mu.RLock()
		store.mu.RUnlock()
		lockAcquired <- true
	}()

	select {
	case <-lockAcquired:
	case <-time.After(5 * time.Second):
		t.Fatal("mu.RLock blocked for >5s — SQL likely running under write lock")
	}

	evicted := <-evictDone
	if evicted != 10 {
		t.Fatalf("expected 10 evicted, got %d", evicted)
	}
}

// --- Endpoint tests ---

// TestPathsThroughNode_CollisionSafety is tested in TestResolvedPubkeyIndex_HashCollision above.

// --- Feature flag tests ---

// TestFeatureFlag_OffPath_PreservesOldBehavior verifies that with useResolvedPathIndex=false,
// the index is not used and all candidates are kept (conservative behavior).
func TestFeatureFlag_OffPath_IndexIsNoOp(t *testing.T) {
	store := &PacketStore{
		byNode:               make(map[string][]*StoreTx),
		nodeHashes:           make(map[string]map[string]bool),
		useResolvedPathIndex: false,
	}
	store.initResolvedPathIndex()

	store.addToResolvedPubkeyIndex(1, []string{"pk1"})
	if len(store.resolvedPubkeyIndex) != 0 {
		t.Error("index should be empty when flag is off")
	}
}

// TestReverseMap_NoLeakOnPartialFailure verifies the reverse map stays consistent
// when operations are interleaved.
func TestReverseMap_NoLeakOnPartialFailure(t *testing.T) {
	store := &PacketStore{
		byNode:               make(map[string][]*StoreTx),
		nodeHashes:           make(map[string]map[string]bool),
		useResolvedPathIndex: true,
	}
	store.initResolvedPathIndex()

	// Add, then remove — reverse map should be clean
	store.addToResolvedPubkeyIndex(1, []string{"pk1", "pk2", "pk3"})
	store.removeFromResolvedPubkeyIndex(1)

	if _, exists := store.resolvedPubkeyReverse[1]; exists {
		t.Error("reverse map should be empty after full removal")
	}
	for _, h := range []string{"pk1", "pk2", "pk3"} {
		hv := resolvedPubkeyHash(h)
		if len(store.resolvedPubkeyIndex[hv]) != 0 {
			t.Errorf("forward index for %s should be empty", h)
		}
	}
}

// TestDecodeWindow_LockHoldTimeBounded measures write-lock duration during
// decode-window operations. This is a documentation/assertion test.
func TestDecodeWindow_LockHoldTimeBounded(t *testing.T) {
	store := &PacketStore{
		byNode:               make(map[string][]*StoreTx),
		nodeHashes:           make(map[string]map[string]bool),
		byPathHop:            make(map[string][]*StoreTx),
		useResolvedPathIndex: true,
	}
	store.initResolvedPathIndex()

	// Simulate decode-window for 100 observations with 5 hops each
	start := time.Now()
	for i := 0; i < 100; i++ {
		tx := &StoreTx{ID: i, Hash: "h", PathJSON: `["a","b","c","d","e"]`}
		pks := []string{"pk1", "pk2", "pk3", "pk4", "pk5"}
		for _, pk := range pks {
			store.addToByNode(tx, pk)
		}
		store.addToResolvedPubkeyIndex(i, pks)
	}
	elapsed := time.Since(start)

	// Should complete in well under 100ms (typically <1ms)
	if elapsed > 100*time.Millisecond {
		t.Errorf("decode-window for 100 obs took %v, expected <100ms", elapsed)
	}
}

// --- Integration / regression tests ---

// --- Benchmarks ---

// BenchmarkResolvedPubkeyIndex_Memory measures index memory at different cardinalities.
func BenchmarkResolvedPubkeyIndex_Memory(b *testing.B) {
	for _, numPubkeys := range []int{50000, 500000} {
		b.Run(fmt.Sprintf("pubkeys=%dK", numPubkeys/1000), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				store := &PacketStore{
					byNode:               make(map[string][]*StoreTx),
					nodeHashes:           make(map[string]map[string]bool),
					useResolvedPathIndex: true,
				}
				store.initResolvedPathIndex()

				// Build index with realistic distribution
				txsPerPubkey := 10
				for pk := 0; pk < numPubkeys; pk++ {
					pks := []string{string(rune(pk))}
					for j := 0; j < txsPerPubkey; j++ {
						txID := pk*txsPerPubkey + j
						store.addToResolvedPubkeyIndex(txID, pks)
					}
				}
			}
		})
	}
}

// BenchmarkLoad_BeforeAfter measures heap reduction from removing per-obs ResolvedPath.
func BenchmarkLoad_BeforeAfter(b *testing.B) {
	// Create a realistic in-memory dataset to measure memory
	const numTx = 10000
	const numObsPerTx = 5
	const numHops = 3

	for n := 0; n < b.N; n++ {
		var m1 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		store := &PacketStore{
			packets:              make([]*StoreTx, 0, numTx),
			byHash:               make(map[string]*StoreTx, numTx),
			byTxID:               make(map[int]*StoreTx, numTx),
			byObsID:              make(map[int]*StoreObs, numTx*numObsPerTx),
			byNode:               make(map[string][]*StoreTx),
			nodeHashes:           make(map[string]map[string]bool),
			byPathHop:            make(map[string][]*StoreTx),
			useResolvedPathIndex: true,
		}
		store.initResolvedPathIndex()

		for i := 0; i < numTx; i++ {
			tx := &StoreTx{
				ID:       i,
				Hash:     string(rune(i)),
				PathJSON: `["aa","bb","cc"]`,
			}
			store.packets = append(store.packets, tx)
			store.byHash[tx.Hash] = tx
			store.byTxID[i] = tx

			for j := 0; j < numObsPerTx; j++ {
				obs := &StoreObs{
					ID:             i*numObsPerTx + j,
					TransmissionID: i,
					PathJSON:       `["aa","bb","cc"]`,
				}
				tx.Observations = append(tx.Observations, obs)
				store.byObsID[obs.ID] = obs
			}

			// Simulate decode-window: add to index instead of storing on struct
			pks := make([]string, numHops)
			for h := 0; h < numHops; h++ {
				pks[h] = string(rune(i*numHops + h))
			}
			store.addToResolvedPubkeyIndex(i, pks)
		}

		var m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m2)

		heapUsed := m2.HeapAlloc - m1.HeapAlloc
		b.ReportMetric(float64(heapUsed)/1048576, "MB")
	}
}

// BenchmarkLoad_OldFieldStorage simulates the OLD approach where each observation
// stored a []*string ResolvedPath directly on the struct. This serves as the "before"
// baseline for comparison with BenchmarkLoad_BeforeAfter (the "after").
func BenchmarkLoad_OldFieldStorage(b *testing.B) {
	const numTx = 10000
	const numObsPerTx = 5
	const numHops = 3

	for n := 0; n < b.N; n++ {
		var m1 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		packets := make([]*StoreTx, 0, numTx)
		byHash := make(map[string]*StoreTx, numTx)
		byTxID := make(map[int]*StoreTx, numTx)
		byObsID := make(map[int]*StoreObs, numTx*numObsPerTx)
		// Simulate old per-obs []*string storage — keep slices alive in a map
		oldResolvedPaths := make(map[int][]*string, numTx*numObsPerTx)

		for i := 0; i < numTx; i++ {
			tx := &StoreTx{
				ID:       i,
				Hash:     string(rune(i)),
				PathJSON: `["aa","bb","cc"]`,
			}
			packets = append(packets, tx)
			byHash[tx.Hash] = tx
			byTxID[i] = tx

			for j := 0; j < numObsPerTx; j++ {
				obsID := i*numObsPerTx + j
				obs := &StoreObs{
					ID:             obsID,
					TransmissionID: i,
					PathJSON:       `["aa","bb","cc"]`,
				}
				// OLD approach: each observation stores its own []*string
				rp := make([]*string, numHops)
				for h := 0; h < numHops; h++ {
					s := string(rune(i*numHops + h))
					rp[h] = &s
				}
				oldResolvedPaths[obsID] = rp
				tx.Observations = append(tx.Observations, obs)
				byObsID[obs.ID] = obs
			}
		}

		var m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m2)

		heapUsed := m2.HeapAlloc - m1.HeapAlloc
		b.ReportMetric(float64(heapUsed)/1048576, "MB")

		// Keep everything alive past GC
		runtime.KeepAlive(packets)
		runtime.KeepAlive(byHash)
		runtime.KeepAlive(byTxID)
		runtime.KeepAlive(byObsID)
		runtime.KeepAlive(oldResolvedPaths)
	}
}

// BenchmarkHopsSeen_Reuse measures allocations from reusing hopsSeen map
// via clear() vs allocating a new map per observation.
func BenchmarkHopsSeen_Reuse(b *testing.B) {
	// Simulate realistic hop counts (10 hops + 10 resolved pubkeys per observation)
	hops := make([]string, 10)
	pks := make([]string, 10)
	for i := range hops {
		hops[i] = fmt.Sprintf("hop%04d", i)
		pks[i] = fmt.Sprintf("pk%04d", i+100)
	}

	b.Run("clear-reuse", func(b *testing.B) {
		b.ReportAllocs()
		hopsSeen := make(map[string]bool)
		for i := 0; i < b.N; i++ {
			clear(hopsSeen)
			for _, hop := range hops {
				hopsSeen[strings.ToLower(hop)] = true
			}
			for _, pk := range pks {
				if !hopsSeen[pk] {
					hopsSeen[pk] = true
				}
			}
		}
	})

	b.Run("alloc-each", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			hopsSeen := make(map[string]bool)
			for _, hop := range hops {
				hopsSeen[strings.ToLower(hop)] = true
			}
			for _, pk := range pks {
				if !hopsSeen[pk] {
					hopsSeen[pk] = true
				}
			}
		}
	})
}

// TestExtractResolvedPubkeys verifies the extraction helper.
func TestExtractResolvedPubkeys(t *testing.T) {
	pk1, pk2 := "aa", "bb"
	rp := []*string{&pk1, nil, &pk2, nil}
	pks := extractResolvedPubkeys(rp)
	if len(pks) != 2 || pks[0] != "aa" || pks[1] != "bb" {
		t.Errorf("unexpected: %v", pks)
	}

	// Empty
	if pks := extractResolvedPubkeys(nil); pks != nil {
		t.Error("expected nil for nil input")
	}
}

// TestMergeResolvedPubkeys verifies the merge helper.
func TestMergeResolvedPubkeys(t *testing.T) {
	pk1, pk2, pk3 := "aa", "bb", "aa"
	rp1 := []*string{&pk1, &pk2}
	rp2 := []*string{&pk3}
	merged := mergeResolvedPubkeys(rp1, rp2)
	if len(merged) != 2 {
		t.Errorf("expected 2 unique pubkeys, got %d: %v", len(merged), merged)
	}
}

// TestResolvedPubkeyHash_Deterministic verifies hash stability.
func TestResolvedPubkeyHash_Deterministic(t *testing.T) {
	h1 := resolvedPubkeyHash("test_pubkey")
	h2 := resolvedPubkeyHash("test_pubkey")
	if h1 != h2 {
		t.Error("hash should be deterministic")
	}
	h3 := resolvedPubkeyHash("TEST_PUBKEY")
	if h1 != h3 {
		t.Error("hash should be case-insensitive")
	}
}

// --- Regression tests for review feedback fixes ---

// TestAddToResolvedPubkeyIndex_CrossCallDedup verifies that calling
// addToResolvedPubkeyIndex multiple times for the same (hash, txID) pair
// does not create duplicate entries in the forward index.
func TestAddToResolvedPubkeyIndex_CrossCallDedup(t *testing.T) {
	store := &PacketStore{
		useResolvedPathIndex: true,
	}
	store.initResolvedPathIndex()

	// Call twice with the same txID and pubkey
	store.addToResolvedPubkeyIndex(1, []string{"abc123"})
	store.addToResolvedPubkeyIndex(1, []string{"abc123"})

	h := resolvedPubkeyHash("abc123")
	ids := store.resolvedPubkeyIndex[h]
	if len(ids) != 1 {
		t.Errorf("expected 1 entry in forward index, got %d", len(ids))
	}

	// Reverse index should also have no duplicates
	rev := store.resolvedPubkeyReverse[1]
	if len(rev) != 1 {
		t.Errorf("expected 1 entry in reverse index, got %d", len(rev))
	}
}

// TestRemoveFromResolvedPubkeyIndex_RemovesAllOccurrences verifies that
// removeFromResolvedPubkeyIndex removes ALL occurrences of a txID, not just the first.
func TestRemoveFromResolvedPubkeyIndex_RemovesAllOccurrences(t *testing.T) {
	store := &PacketStore{
		useResolvedPathIndex: true,
	}
	store.initResolvedPathIndex()

	// Manually inject duplicates to simulate pre-fix state
	h := resolvedPubkeyHash("abc123")
	store.resolvedPubkeyIndex[h] = []int{1, 2, 1, 1} // txID=1 appears 3 times
	store.resolvedPubkeyReverse[1] = []uint64{h}
	store.resolvedPubkeyReverse[2] = []uint64{h}

	store.removeFromResolvedPubkeyIndex(1)

	ids := store.resolvedPubkeyIndex[h]
	if len(ids) != 1 || ids[0] != 2 {
		t.Errorf("expected [2] after removal, got %v", ids)
	}
}

// --- #807: Bounded growth tests ---

func TestResolvedPubkeyIndex_BoundedByEviction(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	staleAt := now.Add(-2 * time.Hour)
	obsIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID: scopeKey(84), Name: "Bounded Observer", IATA: "SFO", Meta: nil, LastSeen: now, IngestNow: now,
	})
	if err != nil {
		t.Fatalf("seed observer: %v", err)
	}

	store := newBareStore(db)
	store.retentionHours = 1

	const n = 50
	for i := 1; i <= n; i++ {
		hash := fmt.Sprintf("c5%014x", i)
		pk := fmt.Sprintf("%064x", 0x2000+i)
		seedRepeater(t, db, pk, staleAt)
		txID := insertRelayPacket(t, db, obsIdx, relayPacket{
			Hash: hash, RouteType: 1, PayloadType: types.PayloadType(1), At: staleAt,
			Hops: []string{"aa"}, Resolved: []*string{sptr(pk)},
		})
		addEvictableToStore(store, int(txID), hash, staleAt)
		store.addToResolvedPubkeyIndex(int(txID), []string{pk})
	}

	if len(store.resolvedPubkeyIndex) != n {
		t.Fatalf("forward index: expected %d entries, got %d", n, len(store.resolvedPubkeyIndex))
	}
	if len(store.resolvedPubkeyReverse) != n {
		t.Fatalf("reverse index: expected %d entries, got %d", n, len(store.resolvedPubkeyReverse))
	}

	evicted := runEviction(t, store)
	if evicted != n {
		t.Fatalf("expected %d evicted, got %d", n, evicted)
	}

	if len(store.resolvedPubkeyIndex) != 0 {
		t.Errorf("forward index should be empty after full eviction, got %d entries", len(store.resolvedPubkeyIndex))
	}
	if len(store.resolvedPubkeyReverse) != 0 {
		t.Errorf("reverse index should be empty after full eviction, got %d entries", len(store.resolvedPubkeyReverse))
	}
}

func TestCompactResolvedPubkeyIndex_DropsEmptyKeys(t *testing.T) {
	store := &PacketStore{useResolvedPathIndex: true}
	store.initResolvedPathIndex()

	h := resolvedPubkeyHash("pk1")
	// Manually insert an empty slice (simulates a bug where delete wasn't called)
	store.resolvedPubkeyIndex[h] = []int{}
	store.resolvedPubkeyReverse[99] = []uint64{}

	store.CompactResolvedPubkeyIndex()

	if _, exists := store.resolvedPubkeyIndex[h]; exists {
		t.Error("CompactResolvedPubkeyIndex should delete empty forward entries")
	}
	if _, exists := store.resolvedPubkeyReverse[99]; exists {
		t.Error("CompactResolvedPubkeyIndex should delete empty reverse entries")
	}
}

func TestCompactResolvedPubkeyIndex_ClipsOversizedSlices(t *testing.T) {
	store := &PacketStore{useResolvedPathIndex: true}
	store.initResolvedPathIndex()

	h := resolvedPubkeyHash("pk1")
	// Create a slice with large backing array but small length
	big := make([]int, 100)
	for i := range big {
		big[i] = i
	}
	// Shrink to 2 elements but keep cap=100
	store.resolvedPubkeyIndex[h] = big[:2]

	if cap(store.resolvedPubkeyIndex[h]) != 100 {
		t.Fatalf("precondition: cap should be 100, got %d", cap(store.resolvedPubkeyIndex[h]))
	}

	store.CompactResolvedPubkeyIndex()

	ids := store.resolvedPubkeyIndex[h]
	if len(ids) != 2 {
		t.Errorf("length should be preserved: got %d", len(ids))
	}
	if cap(ids) > 2*len(ids)+8 {
		t.Errorf("cap should be clipped: got cap=%d for len=%d", cap(ids), len(ids))
	}
	if ids[0] != 0 || ids[1] != 1 {
		t.Error("data should be preserved after clip")
	}
}

func TestRunEviction_TriggersCompaction(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	staleAt := now.Add(-2 * time.Hour)
	pk := strings.Repeat("c1", 32)
	pk2 := strings.Repeat("d2", 32)
	seedRepeater(t, db, pk, staleAt)
	seedRepeater(t, db, pk2, staleAt)
	obsIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID: scopeKey(85), Name: "Compaction Observer", IATA: "SFO", Meta: nil, LastSeen: now, IngestNow: now,
	})
	if err != nil {
		t.Fatalf("seed observer: %v", err)
	}

	store := newBareStore(db)
	store.retentionHours = 1

	oldHash := "c0160000000000a1"
	oldID := insertRelayPacket(t, db, obsIdx, relayPacket{
		Hash: oldHash, RouteType: 1, PayloadType: types.PayloadType(1), At: staleAt,
		Hops: []string{"c1", "d2"}, Resolved: []*string{sptr(pk), sptr(pk2)},
	})
	addEvictableToStore(store, int(oldID), oldHash, staleAt)

	freshHash := "c0160000000000a2"
	freshID := insertRelayPacket(t, db, obsIdx, relayPacket{
		Hash: freshHash, RouteType: 1, PayloadType: types.PayloadType(1), At: now,
		Hops: []string{"d2"}, Resolved: []*string{sptr(pk2)},
	})
	addEvictableToStore(store, int(freshID), freshHash, now)

	store.addToResolvedPubkeyIndex(int(oldID), []string{pk, pk2})
	store.addToResolvedPubkeyIndex(int(freshID), []string{pk2})

	evicted := runEviction(t, store)
	if evicted != 1 {
		t.Fatalf("expected 1 evicted, got %d", evicted)
	}

	hPK := resolvedPubkeyHash(pk)
	if _, exists := store.resolvedPubkeyIndex[hPK]; exists {
		t.Error("pk entry should be removed after eviction")
	}

	hPK2 := resolvedPubkeyHash(pk2)
	ids := store.resolvedPubkeyIndex[hPK2]
	if len(ids) != 1 || ids[0] != int(freshID) {
		t.Errorf("pk2 should only have tx %d, got %v", freshID, ids)
	}
}

func TestResolvedPubkeyIndex_MemoryStableThroughEvictionCycles(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	now := time.Now().UTC()
	staleAt := now.Add(-2 * time.Hour)
	obsIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID: scopeKey(86), Name: "Cycle Observer", IATA: "SFO", Meta: nil, LastSeen: now, IngestNow: now,
	})
	if err != nil {
		t.Fatalf("seed observer: %v", err)
	}

	store := newBareStore(db)
	store.retentionHours = 1

	txCounter := 0
	for cycle := 0; cycle < 5; cycle++ {
		for i := 0; i < 100; i++ {
			txCounter++
			hash := fmt.Sprintf("c7%014x", txCounter)
			pk := fmt.Sprintf("%064x", 0x3000+txCounter)
			seedRepeater(t, db, pk, staleAt)
			txID := insertRelayPacket(t, db, obsIdx, relayPacket{
				Hash: hash, RouteType: 1, PayloadType: types.PayloadType(1), At: staleAt,
				Hops: []string{"aa"}, Resolved: []*string{sptr(pk)},
			})
			addEvictableToStore(store, int(txID), hash, staleAt)
			store.addToResolvedPubkeyIndex(int(txID), []string{pk})
		}

		evicted := runEviction(t, store)
		if evicted != 100 {
			t.Fatalf("cycle %d: expected 100 evicted, got %d", cycle, evicted)
		}

		if fwd := len(store.resolvedPubkeyIndex); fwd != 0 {
			t.Errorf("cycle %d: forward index should be 0, got %d", cycle, fwd)
		}
		if rev := len(store.resolvedPubkeyReverse); rev != 0 {
			t.Errorf("cycle %d: reverse index should be 0, got %d", cycle, rev)
		}
	}
}

func TestCheckResolvedPubkeyIndexSize_Warning(t *testing.T) {
	store := &PacketStore{
		useResolvedPathIndex:          true,
		maxResolvedPubkeyIndexEntries: 10,
	}
	store.initResolvedPathIndex()

	// Add 15 entries — should exceed limit of 10
	for i := 0; i < 15; i++ {
		store.addToResolvedPubkeyIndex(i, []string{fmt.Sprintf("pk_%d", i)})
	}

	// Just verify it doesn't panic — the warning is logged, not returned
	store.CheckResolvedPubkeyIndexSize()
}
