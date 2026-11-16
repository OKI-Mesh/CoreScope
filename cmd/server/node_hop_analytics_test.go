package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
	"github.com/OKI-Mesh/CoreScope/internal/types"
	"github.com/gorilla/mux"
)

// Issue #1812: per-node hop count, as the repeater's own flood.max check sees
// it. The firmware compares getPathHashCount() (the number of hashes already in
// the path when the packet arrives) against the limits before appending its own
// hash, so the target's zero-based index in an observed flood path is exactly
// that count.

const (
	hopTarget   = "ab12cd34ef567890"
	hopOther    = "77aa88bb99cc0011"
	hopCollider = "ab99ffee00112233" // shares the 1-byte prefix "ab" with hopTarget
	hopListener = "ab12ffff00000000" // shares the 2-byte prefix "ab12", listener only
	hopBridge   = "88cc000000000000" // neighbor of hopCollider only
	hopBoth     = "55dd000000000000" // neighbor of hopTarget and hopCollider
	hopTwinA    = "99aa000000000000" // shares the 1-byte prefix "99" with hopTwinB
	hopTwinB    = "99bb000000000000"
)

var hopTimestamp = time.Now().UTC().Add(-time.Hour).Truncate(time.Second)

func hopTx(id int, hash string, routeType types.RouteType, payloadType types.PayloadType, paths ...string) *StoreTx {
	rt, pt := routeType, payloadType
	tx := &StoreTx{ID: id, Hash: hash, FirstSeen: hopTimestamp, RouteType: &rt, PayloadType: &pt}
	for i, p := range paths {
		tx.Observations = append(tx.Observations, &StoreObs{ID: id*10 + i, TransmissionID: id, PathJSON: p})
	}
	return tx
}

func hopPM() *prefixMap {
	return buildPrefixMap([]nodeInfo{
		{PublicKey: hopTarget, Role: "repeater"},
		{PublicKey: hopOther, Role: "repeater"},
		{PublicKey: hopCollider, Role: "repeater"},
		{PublicKey: hopBridge, Role: "repeater"},
		{PublicKey: hopBoth, Role: "repeater"},
		{PublicKey: hopTwinA, Role: "repeater"},
		{PublicKey: hopTwinB, Role: "repeater"},
	})
}

// hopGraph: hopOther and hopTwinB neighbor hopTarget, hopBridge neighbors
// hopCollider, hopBoth neighbors both, and hopOther and hopBoth neighbor both
// twins.
func hopGraph() *NeighborGraph {
	g := NewNeighborGraph()
	now := time.Now()
	for _, e := range [][2]string{
		{hopOther, hopTarget},
		{hopTwinB, hopTarget},
		{hopBridge, hopCollider},
		{hopBoth, hopTarget},
		{hopBoth, hopCollider},
		{hopOther, hopTwinA},
		{hopOther, hopTwinB},
		{hopBoth, hopTwinA},
		{hopBoth, hopTwinB},
	} {
		g.upsertEdge(e[0], e[1], "", "obs", nil, now)
	}
	return g
}

func hopsOf(packets []api.NodeHopPacket) []int {
	out := make([]int, 0, len(packets))
	for _, p := range packets {
		out = append(out, p.Hops)
	}
	return out
}

func hashesOf(packets []api.NodeHopPacket) []string {
	out := make([]string, 0, len(packets))
	for _, p := range packets {
		out = append(out, p.Hash)
	}
	return out
}

func TestNodeHopPackets_IndexIsHopCountAtDifferentPositions(t *testing.T) {
	txs := []*StoreTx{
		hopTx(1, "h1", types.RouteFlood, types.PayloadGRP_TXT, `["AB12"]`),
		hopTx(2, "h2", types.RouteFlood, types.PayloadGRP_TXT, `["77AA","AB12"]`),
		hopTx(3, "h3", types.RouteTransportFlood, types.PayloadGRP_TXT, `["77AA","77AA","77AA","AB12","77AA"]`),
	}
	packets, ambiguous := computeNodeHopPackets(hopTarget, txs, time.Time{}, hopPM(), nil)
	if got, want := hopsOf(packets), []int{0, 1, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("hops = %v, want %v (zero-based index of the node in the path, no +1)", got, want)
	}
	if ambiguous != 0 {
		t.Errorf("ambiguous = %d, want 0", ambiguous)
	}
	if packets[1].Hash != "h2" || !packets[1].Timestamp.Equal(hopTimestamp) {
		t.Errorf("packet[1] = %+v, want hash h2 and timestamp %s", packets[1], hopTimestamp.Format(time.RFC3339))
	}
}

func TestNodeHopPackets_Tags(t *testing.T) {
	txs := []*StoreTx{
		hopTx(1, "unscoped", types.RouteFlood, types.PayloadGRP_TXT, `["AB12"]`),
		hopTx(2, "scoped", types.RouteTransportFlood, types.PayloadGRP_TXT, `["AB12"]`),
		hopTx(3, "advert", types.RouteFlood, types.PayloadADVERT, `["AB12"]`),
		hopTx(4, "scoped-advert", types.RouteTransportFlood, types.PayloadADVERT, `["AB12"]`),
	}
	packets, _ := computeNodeHopPackets(hopTarget, txs, time.Time{}, hopPM(), nil)
	want := map[string][]string{
		"unscoped":      {"flood", "unscoped"},
		"scoped":        {"flood", "scoped"},
		"advert":        {"flood", "unscoped", "advert"},
		"scoped-advert": {"flood", "scoped", "advert"},
	}
	if len(packets) != len(want) {
		t.Fatalf("got %d packets, want %d", len(packets), len(want))
	}
	for _, p := range packets {
		if !reflect.DeepEqual(p.Tags, want[p.Hash]) {
			t.Errorf("%s tags = %v, want %v", p.Hash, p.Tags, want[p.Hash])
		}
	}
}

func TestNodeHopPackets_DirectRoutesExcluded(t *testing.T) {
	txs := []*StoreTx{
		hopTx(1, "direct", types.RouteDirect, types.PayloadTXT_MSG, `["77AA","AB12"]`),
		hopTx(2, "tdirect", types.RouteTransportDirect, types.PayloadTXT_MSG, `["AB12"]`),
	}
	packets, ambiguous := computeNodeHopPackets(hopTarget, txs, time.Time{}, hopPM(), nil)
	if len(packets) != 0 || ambiguous != 0 {
		t.Fatalf("direct packets must be skipped: packets=%v ambiguous=%d", packets, ambiguous)
	}
}

func TestNodeHopPackets_OneEventPerPacketAcrossObservations(t *testing.T) {
	txs := []*StoreTx{
		hopTx(1, "h1", types.RouteFlood, types.PayloadGRP_TXT, `["77AA","AB12"]`, `["77AA","AB12","77AA"]`, `["77AA"]`),
	}
	packets, ambiguous := computeNodeHopPackets(hopTarget, txs, time.Time{}, hopPM(), nil)
	if got := hopsOf(packets); !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("hops = %v, want [1] (one event per packet hash)", got)
	}
	if ambiguous != 0 {
		t.Errorf("ambiguous = %d, want 0", ambiguous)
	}
}

func TestNodeHopPackets_NotInPathIsNeitherCountedNorAmbiguous(t *testing.T) {
	txs := []*StoreTx{hopTx(1, "h1", types.RouteFlood, types.PayloadGRP_TXT, `["77AA"]`, `[]`)}
	packets, ambiguous := computeNodeHopPackets(hopTarget, txs, time.Time{}, hopPM(), nil)
	if len(packets) != 0 || ambiguous != 0 {
		t.Fatalf("packets=%v ambiguous=%d, want none", packets, ambiguous)
	}
}

func TestNodeHopPackets_OriginatorNeverForwardsOwnFlood(t *testing.T) {
	tx := hopTx(1, "own-advert", types.RouteFlood, types.PayloadADVERT, `["AB12"]`)
	tx.DecodedJSON = `{"pubKey":"` + hopTarget + `"}`
	packets, ambiguous := computeNodeHopPackets(hopTarget, []*StoreTx{tx}, time.Time{}, hopPM(), hopGraph())
	if len(packets) != 0 || ambiguous != 0 {
		t.Fatalf("own advert: packets=%v ambiguous=%d, want none", packets, ambiguous)
	}
}

// A colliding prefix counts for the node only under the ingestor's strict rule
// (cmd/ingestor/path_resolver.go resolvePathWithContext): the previous hop is
// itself identified without a tiebreak, and exactly one candidate is its
// graph neighbor. The server resolver's pick (affinity, GPS, advert count)
// plays no part.
func TestNodeHopPackets_CollisionNeedsStrictNeighborAttribution(t *testing.T) {
	advert := func(id int, hash, origin string, path string) *StoreTx {
		tx := hopTx(id, hash, types.RouteFlood, types.PayloadADVERT, path)
		tx.DecodedJSON = `{"pubKey":"` + origin + `"}`
		return tx
	}
	txs := []*StoreTx{
		hopTx(1, "after-neighbor", types.RouteFlood, types.PayloadGRP_TXT, `["77","AB"]`),
		hopTx(2, "no-previous-hop", types.RouteFlood, types.PayloadGRP_TXT, `["AB","77"]`),
		hopTx(3, "after-collider-neighbor", types.RouteFlood, types.PayloadGRP_TXT, `["88","AB"]`),
		hopTx(4, "after-common-neighbor", types.RouteFlood, types.PayloadGRP_TXT, `["55","AB"]`),
		hopTx(5, "previous-hop-ambiguous", types.RouteFlood, types.PayloadGRP_TXT, `["99","AB"]`),
		hopTx(6, "previous-hop-repeats", types.RouteFlood, types.PayloadGRP_TXT, `["77","88","77","AB"]`),
		advert(7, "advert-from-neighbor", hopOther, `["AB"]`),
		advert(8, "advert-from-stranger", hopBridge, `["AB"]`),
		hopTx(9, "observations-disagree", types.RouteFlood, types.PayloadGRP_TXT, `["77","AB"]`, `["88","AB"]`),
		hopTx(10, "one-observation-unresolved", types.RouteFlood, types.PayloadGRP_TXT, `["77","AB"]`, `["99","AB"]`),
		hopTx(11, "observations-without-node", types.RouteFlood, types.PayloadGRP_TXT, `["77","88"]`, `["77"]`, `["77","AB"]`),
		hopTx(12, "chain-broken", types.RouteFlood, types.PayloadGRP_TXT, `["77","99","AB"]`),
		hopTx(13, "origin-of-non-advert", types.RouteFlood, types.PayloadTXT_MSG, `["AB"]`),
		advert(14, "originator-excluded-later", hopTwinA, `["55","99","AB"]`),
	}
	txs[12].DecodedJSON = `{"pubKey":"` + hopOther + `"}`
	packets, ambiguous := computeNodeHopPackets(hopTarget, txs, time.Time{}, hopPM(), hopGraph())
	got := map[string]int{}
	for _, p := range packets {
		got[p.Hash] = p.Hops
	}
	want := map[string]int{"after-neighbor": 1, "advert-from-neighbor": 0, "one-observation-unresolved": 1,
		"observations-without-node": 1, "originator-excluded-later": 2}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("attributed = %v, want %v", got, want)
	}
	if ambiguous != 9 {
		t.Errorf("ambiguous = %d, want 9", ambiguous)
	}
}

func TestNodeHopPackets_CollisionWithoutGraphIsAmbiguous(t *testing.T) {
	txs := []*StoreTx{hopTx(1, "h1", types.RouteFlood, types.PayloadGRP_TXT, `["77","AB"]`)}
	packets, ambiguous := computeNodeHopPackets(hopTarget, txs, time.Time{}, hopPM(), nil)
	if len(packets) != 0 || ambiguous != 1 {
		t.Fatalf("packets=%v ambiguous=%d, want none and 1", packets, ambiguous)
	}
}

func TestNodeHopPackets_PrefixAtSeveralPositionsIsAmbiguous(t *testing.T) {
	txs := []*StoreTx{
		hopTx(1, "same-path", types.RouteFlood, types.PayloadGRP_TXT, `["AB","77","AB"]`),
		hopTx(2, "across-obs", types.RouteFlood, types.PayloadGRP_TXT, `["AB"]`, `["77","AB"]`),
	}
	packets, ambiguous := computeNodeHopPackets(hopTarget, txs, time.Time{}, hopPM(), hopGraph())
	if len(packets) != 0 {
		t.Fatalf("packets = %+v, want none: a node forwards a flood once, so two positions mean a collision", packets)
	}
	if ambiguous != 2 {
		t.Errorf("ambiguous = %d, want 2", ambiguous)
	}
}

func TestNodeHopPackets_ListenerDoesNotMakePrefixAmbiguous(t *testing.T) {
	pm := buildPrefixMap([]nodeInfo{
		{PublicKey: hopTarget, Role: "repeater"},
		{PublicKey: hopListener, Role: "repeater"},
	})
	pm.markNonRelay([]string{hopListener})
	txs := []*StoreTx{hopTx(1, "h1", types.RouteFlood, types.PayloadGRP_TXT, `["AB12","77AA"]`)}
	packets, ambiguous := computeNodeHopPackets(hopTarget, txs, time.Time{}, pm, nil)
	if got := hopsOf(packets); !reflect.DeepEqual(got, []int{0}) || ambiguous != 0 {
		t.Fatalf("hops=%v ambiguous=%d, want [0] and 0", got, ambiguous)
	}
}

// When the node itself is a listener, the only relay candidate for its prefix
// is another node: nothing may be attributed to it.
func TestNodeHopPackets_OnlyRelayCandidateIsAnotherNode(t *testing.T) {
	pm := buildPrefixMap([]nodeInfo{
		{PublicKey: hopTarget, Role: "repeater"},
		{PublicKey: hopCollider, Role: "repeater"},
	})
	pm.markNonRelay([]string{hopTarget})
	txs := []*StoreTx{hopTx(1, "h1", types.RouteFlood, types.PayloadGRP_TXT, `["AB"]`)}
	packets, ambiguous := computeNodeHopPackets(hopTarget, txs, time.Time{}, pm, nil)
	if len(packets) != 0 || ambiguous != 1 {
		t.Fatalf("packets=%v ambiguous=%d, want none and 1", packets, ambiguous)
	}
}

// Uniqueness is per prefix: "ab" collides with hopCollider, "ab12" does not.
func TestNodeHopPackets_UniquenessIsPerPrefixLength(t *testing.T) {
	txs := []*StoreTx{
		hopTx(1, "two-byte", types.RouteFlood, types.PayloadGRP_TXT, `["AB12"]`),
		hopTx(2, "one-byte", types.RouteFlood, types.PayloadGRP_TXT, `["AB"]`),
		hopTx(3, "two-byte-again", types.RouteFlood, types.PayloadGRP_TXT, `["AB12"]`),
		hopTx(4, "one-byte-again", types.RouteFlood, types.PayloadGRP_TXT, `["AB"]`),
	}
	packets, ambiguous := computeNodeHopPackets(hopTarget, txs, time.Time{}, hopPM(), nil)
	if got, want := hashesOf(packets), []string{"two-byte", "two-byte-again"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("attributed = %v, want %v", got, want)
	}
	if ambiguous != 2 {
		t.Errorf("ambiguous = %d, want 2", ambiguous)
	}
}

// ─── Store level: live ingest and cold load ────────────────────────────────

func hopInsertNode(t *testing.T, db *database.DB, pk, name string, adverts int) {
	t.Helper()
	ctx := t.Context()
	now := time.Now().UTC()

	for i := 0; i < adverts; i++ {
		if err := db.CreateOrUpdateNode(ctx, database.NodeInput{
			PublicKey: pk,
			Name:      name,
			Role:      "repeater",
			RxTime:    now,
			IngestNow: now,
		}); err != nil {
			t.Fatalf("insert node %s (advert %d): %v", name, i+1, err)
		}
	}
}

// hopInsertTx inserts one transmission with a single observation whose path is
// pathJSON, and returns the id the database assigned.
func hopInsertTx(t *testing.T, db *database.DB, hash string, route types.RouteType, payload types.PayloadType, paths ...string) int {
	t.Helper()
	ctx := t.Context()

	id, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
		Hash:   hash,
		RawHex: "0100", RouteType: route,
		PayloadType: payload,
	}, hopTimestamp)
	if err != nil {
		t.Fatalf("insert tx %s: %v", hash, err)
	}

	for i, p := range paths {
		var hops []string
		if err := json.Unmarshal([]byte(p), &hops); err != nil {
			t.Fatalf("path %q for tx %s is not a JSON array of strings: %v", p, hash, err)
		}
		observerID := mustCreateTestObserver(t, ctx, db, fmt.Sprintf("hopobs%d", i), fmt.Sprintf("Hop Observer %d", i), "", hopTimestamp)
		dbtest.Observation(t, db, id, observerID, hopTimestamp, 10, -90, hops...)
	}
	return int(id)

}

func hopLoadedStore(t *testing.T, db *database.DB) *PacketStore {
	t.Helper()
	ctx := t.Context()

	store := newPacketStoreForTest(t, db)
	g, err := loadNeighborEdgesFromDB(ctx, db)
	if err != nil {
		t.Fatalf("loadNeighborEdgesFromDB(): %v", err)
	}
	store.graph.Store(g)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked(): %v", err)
	}
	if !store.WaitIndexesReady(10 * time.Second) {
		t.Fatal("indexes not ready")
	}
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache: %v", err)
	}
	return store
}

func hopAnalytics(t *testing.T, store *PacketStore, pk string) *api.NodeHopAnalyticsResponse {
	t.Helper()
	ctx := t.Context()
	resp, err := store.GetNodeHopAnalytics(ctx, pk, 7)
	if err != nil || resp == nil {
		t.Fatalf("GetNodeHopAnalytics(%s): resp=%v err=%v", pk, resp, err)
	}
	sort.Slice(resp.Packets, func(i, j int) bool { return resp.Packets[i].Hash < resp.Packets[j].Hash })
	return resp
}

// Review repro: live ingest resolves a colliding hop with a tiebreak (here the
// advert count). That pick must not become a hop count.
func TestNodeHopAnalytics_LiveIngestDoesNotTrustResolverPick(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	hopInsertNode(t, db, hopTarget, "Target", 500)
	hopInsertNode(t, db, hopCollider, "Collider", 1)
	store := hopLoadedStore(t, db)

	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache: %v", err)
	}

	txID := hopInsertTx(t, db, "collides", types.RouteFlood, types.PayloadGRP_TXT, `["ab"]`)
	if _, maxID, err := store.IngestNewFromDB(ctx, 0, 100); err != nil {
		t.Fatalf("IngestNewFromDB(): %v", err)
	} else if maxID != txID {
		t.Fatalf("IngestNewFromDB maxID = %d, want %d", maxID, txID)
	}

	for _, pk := range []string{hopTarget, hopCollider} {
		resp := hopAnalytics(t, store, pk)
		if len(resp.Packets) != 0 || resp.Ambiguous != 1 {
			t.Errorf("%s: packets=%+v ambiguous=%d, want none and 1", pk, resp.Packets, resp.Ambiguous)
		}
	}
}

// Live ingest and a cold load of the same DB must give the same answer: hops
// are read from every observation's raw path and attributed from the prefix
// map and neighbor graph, not from what each load path indexed.
func TestNodeHopAnalytics_SameResultAfterRestart(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)

	hopInsertNode(t, db, hopTarget, "Target", 1)
	hopInsertNode(t, db, hopCollider, "Collider", 500)
	hopInsertNode(t, db, hopOther, "Other", 1)

	nodeA, nodeB := hopOther, hopTarget
	if nodeA > nodeB {
		nodeA, nodeB = nodeB, nodeA
	}
	if _, err := db.UpsertNeighborEdges(ctx, []database.NeighborEdgeInput{
		{NodeA: nodeA, NodeB: nodeB, Hits: 50, LastSeen: time.Now().UTC().Truncate(time.Millisecond)},
	}); err != nil {
		t.Fatalf("seed neighbor edge: %v", err)
	}
	live := hopLoadedStore(t, db)
	if err := live.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache: %v", err)
	}

	hopInsertTx(t, db, "after-neighbor", types.RouteFlood, types.PayloadGRP_TXT, `["77","ab"]`, `["77","ab","99"]`)
	hopInsertTx(t, db, "no-previous-hop", types.RouteFlood, types.PayloadGRP_TXT, `["ab"]`)
	hopInsertTx(t, db, "two-byte", types.RouteTransportFlood, types.PayloadGRP_TXT, `["77aa","ab12"]`)
	hopInsertTx(t, db, "three-byte", types.RouteFlood, types.PayloadGRP_TXT, `["77aa88","77aa88","ab12cd"]`)
	hopInsertTx(t, db, "not-on-longest-path", types.RouteFlood, types.PayloadGRP_TXT, `["77aa","77aa","77aa"]`, `["77aa","ab12"]`)
	if _, _, err := live.IngestNewFromDB(ctx, 0, 100); err != nil {
		t.Fatalf("IngestNewFromDB(): %v", err)
	}
	fromLive := hopAnalytics(t, live, hopTarget)

	restarted := hopAnalytics(t, hopLoadedStore(t, db), hopTarget)

	want := []api.NodeHopPacket{
		{Hash: "after-neighbor", Hops: 1, Tags: hopTagsUnscoped},
		{Hash: "not-on-longest-path", Hops: 1, Tags: hopTagsUnscoped},
		{Hash: "three-byte", Hops: 2, Tags: hopTagsUnscoped},
		{Hash: "two-byte", Hops: 1, Tags: hopTagsScoped},
	}
	for name, resp := range map[string]*api.NodeHopAnalyticsResponse{"live ingest": fromLive, "cold load": restarted} {
		got := make([]api.NodeHopPacket, len(resp.Packets))
		for i, p := range resp.Packets {
			got[i] = api.NodeHopPacket{Hash: p.Hash, Hops: p.Hops, Tags: p.Tags}
		}
		if !reflect.DeepEqual(got, want) || resp.Ambiguous != 1 {
			t.Errorf("%s: packets=%+v ambiguous=%d, want %+v and 1", name, got, resp.Ambiguous, want)
		}
	}
	if !reflect.DeepEqual(fromLive.Packets, restarted.Packets) || fromLive.Ambiguous != restarted.Ambiguous {
		t.Errorf("live ingest %+v/%d differs from cold load %+v/%d",
			fromLive.Packets, fromLive.Ambiguous, restarted.Packets, restarted.Ambiguous)
	}
}

// End to end through the route: days window, response shape, 404.
func TestHandleNodeHopAnalytics(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	recent := time.Now().Add(-1 * time.Hour).UTC().Truncate(time.Second)
	old := time.Now().Add(-3 * 24 * time.Hour).UTC().Truncate(time.Millisecond)

	hopInsertNode(t, db, hopTarget, "Target", 1)
	hopInsertNode(t, db, hopOther, "Other", 1)
	hopInsertNode(t, db, hopCollider, "Collider", 1)

	observerIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        scopeKey(0xd1),
		Name:      ptr("hop-observer"),
		IATA:      "CVG",
		LastSeen:  recent,
		IngestNow: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("seed observer: %v", err)
	}

	seeds := []struct {
		hash    string
		rawHex  string
		seen    time.Time
		route   types.RouteType
		payload types.PayloadType
		hops    []string
	}{
		{"hop_recent_2", "AA", recent, types.RouteFlood, types.PayloadGRP_TXT, []string{"77aa", "77aa", "ab12"}},
		{"hop_recent_1byte", "BB", recent, types.RouteTransportFlood, types.PayloadADVERT, []string{"ab"}},
		{"hop_scoped_advert", "CC", recent, types.RouteTransportFlood, types.PayloadADVERT, []string{"ab12"}},
		{"hop_old", "DD", old, types.RouteFlood, types.PayloadGRP_TXT, []string{"ab12"}},
	}
	for _, s := range seeds {
		txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
			Hash:        s.hash,
			RawHex:      s.rawHex,
			RouteType:   s.route,
			PayloadType: s.payload,
		}, s.seen)
		if err != nil {
			t.Fatalf("seed tx %s: %v", s.hash, err)
		}
		_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
			TransmissionID: txID,
			ObserverIdx:    ptr(observerIdx),
			Direction:      ptr("RX"),
			Timestamp:      s.seen,
		}, s.hops, nil)
		if err != nil {
			t.Fatalf("seed observation %s: %v", s.hash, err)
		}
		if !inserted {
			t.Fatalf("expected observation to be inserted for tx %d", txID)
		}
	}

	srv := NewServer(db, &Config{Port: 3000}, NewHub())
	router := mux.NewRouter()
	srv.RegisterRoutes(router)
	get := func(url string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest("GET", url, nil))
		return w
	}

	srv.store = hopLoadedStore(t, db)

	w := get("/api/nodes/" + hopTarget + "/hop_analytics?days=1")
	if w.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
	}
	var resp api.NodeHopAnalyticsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	byHash := map[string]api.NodeHopPacket{}
	for _, p := range resp.Packets {
		byHash[p.Hash] = p
	}
	if len(resp.Packets) != 2 {
		t.Fatalf("packets = %+v ambiguous=%d, want the two attributable packets inside the 1-day window", resp.Packets, resp.Ambiguous)
	}
	if p := byHash["hop_recent_2"]; p.Hops != 2 || !p.Timestamp.Equal(recent) || !reflect.DeepEqual(p.Tags, []string{"flood", "unscoped"}) {
		t.Errorf("hop_recent_2 = %+v, want hops 2, timestamp %s, tags [flood unscoped]", p, recent)
	}
	if p := byHash["hop_scoped_advert"]; p.Hops != 0 || !reflect.DeepEqual(p.Tags, []string{"flood", "scoped", "advert"}) {
		t.Errorf("hop_scoped_advert = %+v, want hops 0 tags [flood scoped advert]", p)
	}
	if resp.TimeRange.Days != 1 || resp.Ambiguous != 1 {
		t.Errorf("timeRange.days=%d ambiguous=%d, want 1 and 1 (colliding 1-byte prefix, no neighbor graph)", resp.TimeRange.Days, resp.Ambiguous)
	}

	if w := get("/api/nodes/" + hopTarget + "/hop_analytics?days=7"); w.Code != http.StatusOK || !json.Valid(w.Body.Bytes()) {
		t.Fatalf("days=7: code=%d", w.Code)
	} else {
		var wide api.NodeHopAnalyticsResponse
		_ = json.Unmarshal(w.Body.Bytes(), &wide)
		if len(wide.Packets) != 3 {
			t.Errorf("days=7 packets = %d, want 3", len(wide.Packets))
		}
	}

	if w := get("/api/nodes/ffffffffffffffff/hop_analytics"); w.Code != http.StatusNotFound {
		t.Errorf("unknown node: code=%d, want 404", w.Code)
	}
}

// BenchmarkNodeHopPackets sizes one request for a busy repeater over 7 days,
// as counted in a 1,669-node mesh DB on 2026-09-13: 73,782 flood packets
// with 1,430,280 observations (19 per packet). A third of the packets pass the
// node under a unique 2-byte prefix, a sixth under a 1-byte prefix shared with
// another relay (strict neighbor walk), the rest do not pass it; in each, half
// of the observations contain the node. Observation slices are shared between
// packets to keep the fixture small; the scan only reads them.
func BenchmarkNodeHopPackets(b *testing.B) {
	obsOf := func(self, other string) []*StoreObs {
		through := `["` + other + `","` + self + `","` + other + `","` + other + `","` + other + `","` + other + `"]`
		elsewhere := `["` + other + `","` + other + `","` + other + `","` + other + `","` + other + `","` + other + `"]`
		obs := make([]*StoreObs, 19)
		for j := range obs {
			obs[j] = &StoreObs{PathJSON: elsewhere}
			if j%2 == 0 {
				obs[j].PathJSON = through
			}
		}
		return obs
	}
	unique, colliding, elsewhere := obsOf("AB12", "77AA"), obsOf("AB", "77"), obsOf("88CC", "77AA")
	rt, pt := types.RouteFlood, types.PayloadGRP_TXT
	txs := make([]*StoreTx, 73782)
	for i := range txs {
		txs[i] = &StoreTx{ID: i, Hash: "h", FirstSeen: hopTimestamp, RouteType: &rt, PayloadType: &pt, Observations: elsewhere}
		switch i % 6 {
		case 0, 1:
			txs[i].Observations = unique
		case 2:
			txs[i].Observations = colliding
		}
	}
	pm, graph := hopPM(), hopGraph()
	b.ReportAllocs()

	for b.Loop() {
		computeNodeHopPackets(hopTarget, txs, time.Time{}, pm, graph)
	}
}
