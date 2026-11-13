package main

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/packetpath"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

const (
	nbNodeA    = "aaaaaaaaaa" // the originator
	nbNodeB    = "bbbbbbbbbb" // the node the hop prefix "bb" resolves to
	nbObserver = "obs-1"      // the observer's pubkey
)

func seedNeighborObservation(t *testing.T, s *Store, obsID int64, hash string, at time.Time,
	from string, pt types.PayloadType, hops ...string) {
	t.Helper()
	ctx := testCtx(t)

	in := database.TransmissionInput{
		Hash:        hash,
		RouteType:   types.RouteFlood,
		PayloadType: pt,
		DecodedJSON: "{}",
	}
	if from != "" {
		in.Advert = &database.NodeInput{
			PublicKey: from,
			Name:      from,
			Role:      types.RoleRepeater,
		}
	}
	txID, _, err := s.db.CreateOrUpdateTransmission(ctx, in, at)
	if err != nil {
		t.Fatalf("seed transmission %s: %v", hash, err)
	}

	var observer *int64
	if obsID != 0 {
		observer = &obsID
	}
	_, inserted, err := s.db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: txID,
		ObserverIdx:    observer,
		Timestamp:      at,
	}, hops, nil)
	if err != nil {
		t.Fatalf("seed observation for %s: %v", hash, err)
	}
	if !inserted {
		t.Fatalf("expected observation to be inserted for %s", hash)
	}
}

func noRefresh(context.Context) error { return nil }

func nbHop(s string) *string { return &s }

// trustAllPrefixes returns the pre-#1784 threshold, where 1-byte hop hashes
// still count as mapping evidence. Tests that exercise edge *shape* use 1-byte
// fixtures, so they pin the legacy threshold; the gate itself is covered by
// explicit-threshold cases in TestExtractEdges.
func trustAllPrefixes() *packetpath.TrustConfig {
	return &packetpath.TrustConfig{MinHashBytesForMapping: 1}
}

// ---- Pure tests: no database ----

// TestExtractEdges replaces the SQLite-seeded shape tests (builder upserts,
// ANON_REQ #1777, other types excluded, path trust #1784). Edge extraction is
// a pure function now, so each rule is one table row.
func TestExtractEdges(t *testing.T) {
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	trust1 := trustAllPrefixes()
	// Passed explicitly rather than leaning on the package default: the gate is
	// about what happens AT threshold 2, and coupling to the default made this
	// fail the moment the default moved to 1 (#1929).
	trust2 := &packetpath.TrustConfig{MinHashBytesForMapping: 2}
	idx := newPrefixIndex([]string{nbNodeA, nbNodeB})
	ambiguous := newPrefixIndex([]string{nbNodeA, nbNodeB, "bbbbbbbbbc"}) // "bb" now has two candidates

	pair := func(x, y string) string { // canonical "a|b" form
		if x > y {
			x, y = y, x
		}
		return x + "|" + y
	}
	advert := database.NeighborObservation{
		PayloadType: types.PayloadADVERT, FromPubkey: nbNodeA, Observer: nbObserver,
		Timestamp: ts, FirstHop: nbHop("bb"), LastHop: nbHop("bb"),
	}
	with := func(base database.NeighborObservation, f func(*database.NeighborObservation)) database.NeighborObservation {
		f(&base)
		return base
	}

	cases := []struct {
		name  string
		obs   database.NeighborObservation
		trust *packetpath.TrustConfig
		idx   prefixIndex
		want  []string
	}{
		{"relayed advert: originator to first hop, observer to last hop",
			advert, trust1, idx,
			[]string{pair(nbNodeA, nbNodeB), pair(nbObserver, nbNodeB)}},
		{"zero-hop advert: the observer heard the originator directly",
			with(advert, func(o *database.NeighborObservation) { o.FirstHop, o.LastHop = nil, nil }),
			trust1, idx,
			[]string{pair(nbNodeA, nbObserver)}},
		{"ANON_REQ: the originator comes from decoded_json (#1777)",
			with(advert, func(o *database.NeighborObservation) {
				o.PayloadType, o.FromPubkey = types.PayloadANON_REQ, ""
				o.DecodedJSON = `{"ephemeralPubKey":"` + nbNodeA + `"}`
			}),
			trust1, idx,
			[]string{pair(nbNodeA, nbNodeB), pair(nbObserver, nbNodeB)}},
		{"REQ: a 1-byte source hash never seeds an originator edge (#1777)",
			with(advert, func(o *database.NeighborObservation) { o.PayloadType = types.PayloadREQ }),
			trust1, idx,
			[]string{pair(nbObserver, nbNodeB)}},
		{"1-byte hop under a 2-byte threshold: no edge at all (#1784)",
			advert, trust2, idx, nil},
		{"2-byte hop under a 2-byte threshold: edges as normal",
			with(advert, func(o *database.NeighborObservation) { o.FirstHop, o.LastHop = nbHop("bbbb"), nbHop("bbbb") }),
			trust2, idx,
			[]string{pair(nbNodeA, nbNodeB), pair(nbObserver, nbNodeB)}},
		{"ambiguous prefix: two candidates, no edge",
			advert, trust1, ambiguous, nil},
		{"originator is the resolved hop: no self-edge",
			with(advert, func(o *database.NeighborObservation) { o.FromPubkey = nbNodeB }),
			trust1, idx,
			[]string{pair(nbObserver, nbNodeB)}},
		{"zero hops and no observer: nothing to connect",
			with(advert, func(o *database.NeighborObservation) { o.FirstHop, o.LastHop, o.Observer = nil, nil, "" }),
			trust1, idx, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var got []string
			for _, e := range extractEdges([]database.NeighborObservation{c.obs}, c.idx, c.trust) {
				if e.a > e.b {
					t.Errorf("edge %s|%s is not in canonical order", e.a, e.b)
				}
				got = append(got, e.a+"|"+e.b)
			}
			slices.Sort(got)
			want := slices.Clone(c.want)
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Errorf("edges = %v, want %v", got, want)
			}
		})
	}
}

func TestAggregateEdges(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	got := aggregateEdges([]edgeRow{
		{nbNodeA, nbNodeB, t0},
		{nbNodeA, nbNodeB, t0.Add(time.Minute)},
		{nbNodeA, nbNodeB, t0.Add(-time.Hour)},
		{nbNodeA, "cccccccccc", t0},
	})
	byPair := map[string]database.NeighborEdgeInput{}
	for _, e := range got {
		byPair[e.NodeA+"|"+e.NodeB] = e
	}
	if len(byPair) != 2 {
		t.Fatalf("got %d pairs, want 2 (duplicates must collapse): %v", len(byPair), got)
	}
	if ab := byPair[nbNodeA+"|"+nbNodeB]; ab.Hits != 3 || !ab.LastSeen.Equal(t0.Add(time.Minute)) {
		t.Errorf("a|b = %+v, want 3 hits with the newest timestamp", ab)
	}
	if ac := byPair[nbNodeA+"|cccccccccc"]; ac.Hits != 1 {
		t.Errorf("a|c = %+v, want 1 hit", ac)
	}
	// Output order is deterministic (sorted by NodeA, then NodeB).
	if got[0].NodeB != nbNodeB && got[0].NodeB != "cccccccccc" {
		t.Errorf("unexpected first row: %+v", got[0])
	}
}

// ---- Warm-up termination, against a fake store (no database) ----

// fakeNeighborStore serves rows ordered by (timestamp, id), honoring the cursor
// and limit parameters the way the real query does. served records the id of
// every row it hands out, in order.
type fakeNeighborStore struct {
	rows    []database.NeighborObservation
	calls   int
	upserts int
	served  []int64
}

func (f *fakeNeighborStore) ListNodePublicKeys(context.Context) ([]string, error) { return nil, nil }
func (f *fakeNeighborStore) LatestNeighborEdgeTime(context.Context) (time.Time, error) {
	return time.Time{}, nil
}
func (f *fakeNeighborStore) ScanNeighborObservations(_ context.Context, after database.NeighborCursor, limit int,
	_ []types.PayloadType) ([]database.NeighborObservation, error) {
	f.calls++
	var out []database.NeighborObservation
	for _, r := range f.rows {
		if r.Timestamp.After(after.Timestamp) || (r.Timestamp.Equal(after.Timestamp) && r.ID > after.ID) {
			out = append(out, r)
			f.served = append(f.served, r.ID)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}
func (f *fakeNeighborStore) UpsertNeighborEdges(_ context.Context, edges []database.NeighborEdgeInput) (int64, error) {
	f.upserts += len(edges)
	return int64(len(edges)), nil
}

// TestWarmUpTerminatesWhenRowsProduceNoEdges is the regression test for the
// drain loop. The loop must end on rows SCANNED, and the watermark must
// advance past rows that produced no edge; otherwise a full batch of edge-less
// rows is rescanned forever.
func TestWarmUpTerminatesWhenRowsProduceNoEdges(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, c := range []struct{ rows, wantCalls int }{
		{0, 1},  // nothing to do
		{5, 1},  // one short batch
		{10, 2}, // exactly one full batch: a second scan proves it is drained
		{20, 3},
		{25, 3}, // 10 + 10 + 5
	} {
		t.Run(fmt.Sprintf("%d rows", c.rows), func(t *testing.T) {
			f := &fakeNeighborStore{}
			for i := 0; i < c.rows; i++ {
				f.rows = append(f.rows, database.NeighborObservation{
					PayloadType: types.PayloadADVERT, FromPubkey: nbNodeA, Observer: nbObserver,
					Timestamp: base.Add(time.Duration(i) * time.Second),
					FirstHop:  nbHop("zz"), LastHop: nbHop("zz"), // unresolvable: no edges
				})
			}
			b := &neighborBuilder{db: f, trust: trustAllPrefixes(), maxBatch: 10,
				refreshPrefixIndex: noRefresh, refreshGraph: noRefresh}

			done := make(chan struct{})
			go func() { b.WarmUp(t.Context()); close(done) }()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("WarmUp did not terminate: rows that produce no edge were rescanned forever")
			}

			if f.calls != c.wantCalls {
				t.Errorf("scan calls = %d, want %d", f.calls, c.wantCalls)
			}
			if f.upserts != 0 {
				t.Errorf("upserts = %d, want 0 (nothing resolves)", f.upserts)
			}
			if c.rows > 0 {
				if want := base.Add(time.Duration(c.rows-1) * time.Second); !b.cursor.Timestamp.Equal(want) {
					t.Errorf("watermark = %v, want %v (past every scanned row)", b.cursor.Timestamp, want)
				}
			}
		})
	}
}

// ---- Database-backed tests: the real SQL ----

func newTestNeighborBuilder(s *Store, batch int) *neighborBuilder {
	return &neighborBuilder{db: s.db, trust: trustAllPrefixes(), maxBatch: batch,
		refreshPrefixIndex: noRefresh, refreshGraph: noRefresh}
}

func seedNeighborNode(t *testing.T, s *Store, pk string) {
	t.Helper()
	if err := upsertNode(testCtx(t), s, pk, "node-"+pk[:2], "repeater", nil, nil, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
}

// seedNeighborObserver creates the observer and returns its surrogate id.
func seedNeighborObserver(t *testing.T, s *Store) int64 {
	t.Helper()
	if err := s.UpsertObserver(testCtx(t), nbObserver, ptr("observer-1"), "SJC", nil); err != nil {
		t.Fatal(err)
	}
	var id int64
	mustScan(t, testRow(t, s, `SELECT id FROM observers WHERE pubkey = $1`, nbObserver), &id)
	return id
}

func TestNeighborBuilderPersistsEdges(t *testing.T) {
	s := newTestStore(t)
	ctx := testCtx(t)
	for _, pk := range []string{nbNodeA, nbNodeB, nbObserver} {
		seedNeighborNode(t, s, pk)
	}
	obsID := seedNeighborObserver(t, s)
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	seedNeighborObservation(t, s, obsID, "h1", at, nbNodeA, types.PayloadADVERT, "bb")

	scanned, edges, err := newTestNeighborBuilder(s, 0).build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if scanned != 1 || edges != 2 {
		t.Fatalf("scanned/edges = %d/%d, want 1/2 (originator-hop and observer-hop)", scanned, edges)
	}

	var count int32
	var lastSeen time.Time
	mustScan(t, testRow(t, s,
		`SELECT count, last_seen FROM neighbor_edges WHERE node_a = $1 AND node_b = $2`, nbNodeA, nbNodeB),
		&count, &lastSeen)
	if count != 1 || !lastSeen.Equal(at) {
		t.Errorf("a|b edge: count=%d last_seen=%v, want 1 and %v", count, lastSeen, at)
	}
}

// TestScanNeighborObservationsHandlesNulls runs the real scan query over the
// shapes that produce NULLs: zero hops, no observer, no from_pubkey. A scan
// into a non-null Go type fails at runtime, not at compile time, so only real
// rows catch it.
func TestScanNeighborObservationsHandlesNulls(t *testing.T) {
	s := newTestStore(t)
	ctx := testCtx(t)
	obsID := seedNeighborObserver(t, s)
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)

	seedNeighborObservation(t, s, obsID, "direct", at, nbNodeA, types.PayloadADVERT) // zero hops
	seedNeighborObservation(t, s, obsID, "relayed", at.Add(1*time.Second), nbNodeA, types.PayloadADVERT, "aa", "bb")
	seedNeighborObservation(t, s, 0, "no-observer", at.Add(2*time.Second), "", types.PayloadREQ, "cc")
	seedNeighborObservation(t, s, obsID, "advert-no-from", at.Add(3*time.Second), "", types.PayloadADVERT, "dd")

	rows, err := s.db.ScanNeighborObservations(ctx, database.NeighborCursor{}, 100, neighborOriginatorTypes)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 {
		t.Fatalf("got %d rows, want 4", len(rows))
	}
	if rows[0].FirstHop != nil || rows[0].LastHop != nil {
		t.Errorf("zero-hop row has hops: %v / %v", rows[0].FirstHop, rows[0].LastHop)
	}
	if rows[1].FirstHop == nil || *rows[1].FirstHop != "aa" || rows[1].LastHop == nil || *rows[1].LastHop != "bb" {
		t.Errorf("relayed row first/last hop = %v / %v, want aa / bb", rows[1].FirstHop, rows[1].LastHop)
	}
	if rows[2].Observer != "" || rows[2].FromPubkey != "" {
		t.Errorf("no-observer row = observer %q from %q, want both empty", rows[2].Observer, rows[2].FromPubkey)
	}
	// decoded_json is only returned for the requested payload types when from_pubkey is NULL.
	if rows[0].DecodedJSON != "" {
		t.Errorf("row with from_pubkey set returned decoded_json %q", rows[0].DecodedJSON)
	}
	if rows[2].DecodedJSON != "" {
		t.Errorf("REQ row returned decoded_json %q, but REQ was not requested", rows[2].DecodedJSON)
	}
	if rows[3].DecodedJSON != "{}" {
		t.Errorf("ADVERT row with NULL from_pubkey: decoded_json = %q, want {}", rows[3].DecodedJSON)
	}
}

// TestWarmUpScansRowsSharingATimestampAcrossBatchesExactlyOnce is the
// regression test for the batch-boundary skip: with a watermark of timestamp
// alone, the rows that tie the last scanned timestamp were never read again.
func TestWarmUpScansRowsSharingATimestampAcrossBatchesExactlyOnce(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f := &fakeNeighborStore{}
	for id := int64(1); id <= 7; id++ {
		f.rows = append(f.rows, database.NeighborObservation{
			ID: id, PayloadType: types.PayloadADVERT, FromPubkey: nbNodeA, Observer: nbObserver,
			Timestamp: at,
			FirstHop:  nbHop("zz"), LastHop: nbHop("zz"), // unresolvable: no edges
		})
	}
	b := &neighborBuilder{db: f, trust: trustAllPrefixes(), maxBatch: 3,
		refreshPrefixIndex: noRefresh, refreshGraph: noRefresh}

	done := make(chan struct{})
	go func() { b.WarmUp(t.Context()); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("WarmUp did not terminate")
	}

	if want := []int64{1, 2, 3, 4, 5, 6, 7}; !slices.Equal(f.served, want) {
		t.Errorf("rows served = %v, want each of %v exactly once, in order", f.served, want)
	}
	if f.calls != 3 {
		t.Errorf("scan calls = %d, want 3 (3 + 3 + 1)", f.calls)
	}
	if !b.cursor.Timestamp.Equal(at) || b.cursor.ID != 7 {
		t.Errorf("cursor = %+v, want timestamp %v and id 7", b.cursor, at)
	}
}

// TestNeighborBuilderDeltaScan replaces the 100k-row timing test (#1339). Wall
// clock pinned SQLite lock behavior and is flaky; what matters is how many rows
// each tick SCANS, which is deterministic.
func TestNeighborBuilderDeltaScan(t *testing.T) {
	s := newTestStore(t)
	ctx := testCtx(t)
	for _, pk := range []string{nbNodeA, nbNodeB, nbObserver} {
		seedNeighborNode(t, s, pk)
	}
	obsID := seedNeighborObserver(t, s)
	base := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Second)

	seed := func(from, to int) {
		for i := from; i < to; i++ {
			seedNeighborObservation(t, s, obsID, fmt.Sprintf("h%d", i),
				base.Add(time.Duration(i)*time.Second), nbNodeA, types.PayloadADVERT, "bb")
		}
	}
	pairCount := func() int32 {
		var n int32
		mustScan(t, testRow(t, s,
			`SELECT count FROM neighbor_edges WHERE node_a = $1 AND node_b = $2`, nbNodeA, nbNodeB), &n)
		return n
	}

	// Warm-up drains 25 rows in three scans of at most 10.
	seed(0, 25)
	b := newTestNeighborBuilder(s, 10)
	b.WarmUp(ctx)
	if got := pairCount(); got != 25 {
		t.Fatalf("after warm-up the a|b edge counts %d observations, want 25 (no batch skipped or double counted)", got)
	}
	if want := base.Add(24 * time.Second); !b.cursor.Timestamp.Equal(want) {
		t.Fatalf("watermark = %v, want %v", b.cursor.Timestamp, want)
	}

	// A tick with nothing new scans nothing.
	scanned, edges, err := b.build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if scanned != 0 || edges != 0 {
		t.Fatalf("empty-delta tick scanned/edges = %d/%d, want 0/0 (#1339)", scanned, edges)
	}

	// New observations are the only rows the next tick scans.
	seed(25, 30)
	scanned, _, err = b.build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if scanned != 5 {
		t.Fatalf("delta tick scanned %d rows, want 5: the watermark must exclude everything already processed (#1339)", scanned)
	}
	if got := pairCount(); got != 30 {
		t.Fatalf("a|b edge counts %d observations, want 30", got)
	}

	// A restarted builder resumes from the table, not from scratch.
	b2 := newTestNeighborBuilder(s, 10)
	b2.WarmUp(ctx)
	if got := pairCount(); got != 30 {
		t.Fatalf("after restart a|b edge counts %d, want 30 (resumed, did not recount)", got)
	}
}

// TestNeighborBuilderEdgeToObserverWithoutNodeRow pins the decision on the
// neighbor_edges foreign keys (plan item 5.8). Observers often have no nodes
// row, and a single violation fails the whole batched upsert. While the table
// has FKs to nodes this test fails. Either drop the FKs (this assertion is
// written for that choice), or filter such edges in the builder and flip it.
func TestNeighborBuilderEdgeToObserverWithoutNodeRow(t *testing.T) {
	s := newTestStore(t)
	seedNeighborNode(t, s, nbNodeA)
	seedNeighborNode(t, s, nbNodeB) // the observer is deliberately NOT a node
	obsID := seedNeighborObserver(t, s)
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	seedNeighborObservation(t, s, obsID, "h1", at, nbNodeA, types.PayloadADVERT, "bb")

	if _, _, err := newTestNeighborBuilder(s, 0).build(testCtx(t)); err != nil {
		t.Fatalf("an observer without a nodes row broke the whole batch: %v", err)
	}
	if n := countWhere(t, s, "neighbor_edges", "node_b = $1", nbObserver); n != 1 {
		t.Errorf("observer edge rows = %d, want 1", n)
	}
}
