package database_test

import (
	"slices"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
)

func TestListObserverReceptions_GroupedAndOrdered_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()

	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seenFirst := time.Date(2026, 3, 1, 12, 1, 0, 0, time.UTC)
	seenSecond := time.Date(2026, 3, 1, 12, 2, 0, 0, time.UTC)
	seenOther := time.Date(2026, 3, 1, 12, 3, 0, 0, time.UTC)

	obsD := dbtest.Observer(t, db, "obsd", "Obsd", "SJC", epoch, seenFirst)
	obsE := dbtest.Observer(t, db, "obse", "ObsE", "SJC", epoch, seenFirst)

	a := dbtest.ChannelTx(t, db, "recv_hash_a", "#recv",
		`{"type":"CHAN","channel":"#recv","text":"Alpha: hi","sender":"Alpha"}`, seenFirst)
	b := dbtest.ChannelTx(t, db, "recv_hash_b", "#recv",
		`{"type":"CHAN","channel":"#recv","text":"Bravo: hi","sender":"Bravo"}`, seenOther)

	dbtest.Observation(t, db, a, obsE, seenSecond, 11.0, -88, "aa")
	dbtest.Observation(t, db, a, obsD, seenFirst, 10.0, -90, "aa")
	dbtest.Observation(t, db, b, obsD, seenOther, 9.0, -91, "bb")

	got, err := db.ListObserverReceptions(ctx, []int64{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 transmissions in the map, got %d: %+v", len(got), got)
	}

	recvA := got[a]
	if len(recvA) != 2 {
		t.Fatalf("tx a: want 2 receptions, got %+v", recvA)
	}
	if recvA[0].ObserverName == nil || *recvA[0].ObserverName != "Obsd" {
		t.Errorf("tx a [0]: want observer Obsd (earliest), got %v", recvA[0].ObserverName)
	}
	if recvA[1].ObserverName == nil || *recvA[1].ObserverName != "ObsE" {
		t.Errorf("tx a [1]: want observer ObsE (latest), got %v", recvA[1].ObserverName)
	}
	if recvA[0].TransmissionID != a || recvA[1].TransmissionID != a {
		t.Errorf("tx a: want TransmissionID %d on both receptions, got %d and %d",
			a, recvA[0].TransmissionID, recvA[1].TransmissionID)
	}
	if recvA[0].SNR == nil || *recvA[0].SNR != 10.0 || recvA[0].RSSI == nil || *recvA[0].RSSI != -90 {
		t.Errorf("tx a [0]: want SNR 10 and RSSI -90, got %v and %v", recvA[0].SNR, recvA[0].RSSI)
	}
	if !recvA[0].ReceivedAt.Equal(seenFirst) || !recvA[1].ReceivedAt.Equal(seenSecond) {
		t.Errorf("tx a: want ReceivedAt %v then %v, got %v then %v",
			seenFirst, seenSecond, recvA[0].ReceivedAt, recvA[1].ReceivedAt)
	}
	if recvA[0].ReceivedAt.Location() != time.UTC {
		t.Errorf("tx a [0]: want ReceivedAt in UTC, got %v", recvA[0].ReceivedAt.Location())
	}

	recvB := got[b]
	if len(recvB) != 1 || recvB[0].ObserverName == nil || *recvB[0].ObserverName != "Obsd" {
		t.Fatalf("tx b: want one reception from Obsd, got %+v", recvB)
	}
}

func TestListObserverReceptions_OnlyRequestedIDs_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()

	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seen := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	obs := dbtest.Observer(t, db, "obsd", "Obsd", "SJC", epoch, seen)

	a := dbtest.ChannelTx(t, db, "only_hash_a", "#only",
		`{"type":"CHAN","channel":"#only","text":"Alpha: hi","sender":"Alpha"}`, seen)
	b := dbtest.ChannelTx(t, db, "only_hash_b", "#only",
		`{"type":"CHAN","channel":"#only","text":"Bravo: hi","sender":"Bravo"}`, seen)
	unseen := dbtest.ChannelTx(t, db, "only_hash_c", "#only",
		`{"type":"CHAN","channel":"#only","text":"Charlie: hi","sender":"Charlie"}`, seen)

	dbtest.Observation(t, db, a, obs, seen, 10.0, -90, "aa")
	dbtest.Observation(t, db, b, obs, seen, 9.0, -91, "bb")

	got, err := db.ListObserverReceptions(ctx, []int64{a, unseen})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want only tx a in the map (b not requested, unseen has no observations), got %+v", got)
	}
	if len(got[a]) != 1 {
		t.Fatalf("tx a: want 1 reception, got %+v", got[a])
	}
	if _, ok := got[unseen]; ok {
		t.Errorf("tx with no observations must be absent from the map")
	}
}

func TestListObserverReceptions_NoIDs_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()

	for name, ids := range map[string][]int64{"nil": nil, "empty": {}} {
		got, err := db.ListObserverReceptions(ctx, ids)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(got) != 0 {
			t.Errorf("%s: want an empty map, got %+v", name, got)
		}
	}
}

func TestListObserverReceptions_MissingSignalValues_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()

	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seen := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	obs := dbtest.Observer(t, db, "obsd", "Obsd", "SJC", epoch, seen)
	tx := dbtest.ChannelTx(t, db, "nosig_hash", "#nosig",
		`{"type":"CHAN","channel":"#nosig","text":"Alpha: hi","sender":"Alpha"}`, seen)

	direction := "RX"
	_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: tx,
		ObserverIdx:    &obs,
		Direction:      &direction,
		SNR:            nil,
		RSSI:           nil,
		PathHash:       "",
		Timestamp:      seen,
	}, []string{}, []*string{})
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatalf("expected observation to be inserted for tx %d", tx)
	}

	got, err := db.ListObserverReceptions(ctx, []int64{tx})
	if err != nil {
		t.Fatal(err)
	}
	if len(got[tx]) != 1 {
		t.Fatalf("want 1 reception, got %+v", got[tx])
	}
	if got[tx][0].SNR != nil || got[tx][0].RSSI != nil {
		t.Errorf("want nil SNR and RSSI, got %v and %v", got[tx][0].SNR, got[tx][0].RSSI)
	}
	if got[tx][0].ObserverName == nil || *got[tx][0].ObserverName != "Obsd" {
		t.Errorf("want observer Obsd, got %v", got[tx][0].ObserverName)
	}
}

func TestListObserverReceptions_NilObserverName_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()

	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seen := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	direction := "RX"
	snr := 10.0
	rssi := -90.0

	unnamed, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        "obsnoname",
		Name:      "",
		IATA:      "SJC",
		LastSeen:  seen,
		IngestNow: epoch,
	})
	if err != nil {
		t.Fatal(err)
	}

	withObserver := dbtest.ChannelTx(t, db, "noname_hash_a", "#noname",
		`{"type":"CHAN","channel":"#noname","text":"Alpha: hi","sender":"Alpha"}`, seen)
	_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: withObserver,
		ObserverIdx:    &unnamed,
		Direction:      &direction,
		SNR:            &snr,
		RSSI:           &rssi,
		PathHash:       "",
		Timestamp:      seen,
	}, []string{}, []*string{})
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatalf("expected observation to be inserted for tx %d", withObserver)
	}

	withoutObserver := dbtest.ChannelTx(t, db, "noname_hash_b", "#noname",
		`{"type":"CHAN","channel":"#noname","text":"Bravo: hi","sender":"Bravo"}`, seen)
	_, inserted, err = db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: withoutObserver,
		ObserverIdx:    nil,
		Direction:      &direction,
		SNR:            &snr,
		RSSI:           &rssi,
		PathHash:       "",
		Timestamp:      seen,
	}, []string{}, []*string{})
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatalf("expected observation to be inserted for tx %d", withoutObserver)
	}

	got, err := db.ListObserverReceptions(ctx, []int64{withObserver, withoutObserver})
	if err != nil {
		t.Fatal(err)
	}

	if len(got[withoutObserver]) != 1 {
		t.Fatalf("observation with no observer: want 1 reception, got %+v", got[withoutObserver])
	}
	if got[withoutObserver][0].ObserverName != nil {
		t.Errorf("observation with no observer: want nil ObserverName, got %q", *got[withoutObserver][0].ObserverName)
	}

	if len(got[withObserver]) != 1 {
		t.Fatalf("observer with empty name: want 1 reception, got %+v", got[withObserver])
	}
	if got[withObserver][0].ObserverName != nil {
		t.Errorf("observer with empty name: want nil ObserverName, got %q", *got[withObserver][0].ObserverName)
	}
}

func TestInsertObservationWithHops_StoresNormalizedPathAndFrame_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	seedNodes(t, db, nodeAt("aaaa", "Hop A", "repeater", nodesNow))
	obs := dbtest.Observer(t, db, "obsa", "Obs A", "CVG", nodesNow, nodesNow)
	tx := dbtest.ChannelTx(t, db, "path_hash", "3A", `{"k":1}`, nodesNow)

	id, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: tx, ObserverIdx: &obs, Direction: ptr("RX"),
		SNR: ptr(7.5), RSSI: ptr(-90.0), Timestamp: nodesNow.Add(time.Minute), RawHex: "CCDD",
	}, []string{"AA", "Bb", "cc"}, []*string{ptr("AAAA"), nil})
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("want the observation inserted")
	}

	byTx, err := db.ListObservationsForTransmissions(ctx, []int64{tx})
	if err != nil {
		t.Fatal(err)
	}
	if len(byTx[tx]) != 1 || byTx[tx][0].ID != id {
		t.Fatalf("want the one observation %d, got %+v", id, byTx[tx])
	}
	got := byTx[tx][0]
	if got.ObserverID != "obsa" || got.Direction != "RX" {
		t.Errorf("want observer obsa and direction RX, got %q and %q", got.ObserverID, got.Direction)
	}
	if got.SNR == nil || *got.SNR != 7.5 || got.RSSI == nil || *got.RSSI != -90 {
		t.Errorf("want SNR 7.5 and RSSI -90, got %v and %v", got.SNR, got.RSSI)
	}
	if !slices.Equal(got.HopPrefixes(), []string{"aa", "bb", "cc"}) {
		t.Errorf("hop prefixes are stored lowercase, want [aa bb cc], got %v", got.HopPrefixes())
	}
	resolved := make([]string, len(got.Path))
	for i, hop := range got.Path {
		if hop.ResolvedPubkey != nil {
			resolved[i] = *hop.ResolvedPubkey
		}
	}
	if !slices.Equal(resolved, []string{"aaaa", "", ""}) {
		t.Errorf("want the resolved key lowercased and missing ones empty, got %v", resolved)
	}

	frames, err := db.ObservationRawHexForHash(ctx, "PATH_HASH")
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 || frames[id] != "CCDD" {
		t.Errorf("want frame CCDD for observation %d (hash lookup is case-insensitive), got %v", id, frames)
	}
}

func TestInsertObservationWithHops_BadFrameStoresNothing_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	obs := dbtest.Observer(t, db, "obsa", "Obs A", "CVG", nodesNow, nodesNow)
	tx := dbtest.ChannelTx(t, db, "bad_frame_hash", "3A", `{"k":1}`, nodesNow)

	_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: tx, ObserverIdx: &obs, Timestamp: nodesNow, RawHex: "ZZ",
	}, nil, nil)
	if err == nil || inserted {
		t.Fatalf("want an error and nothing inserted for an invalid frame, got inserted=%v err=%v", inserted, err)
	}

	byTx, err := db.ListObservationsForTransmissions(ctx, []int64{tx})
	if err != nil {
		t.Fatal(err)
	}
	if len(byTx) != 0 {
		t.Errorf("no observation may be stored when the frame is rejected, got %+v", byTx)
	}
}

func TestInsertObservationWithHops_DuplicateIsNotInsertedTwice_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	obs := dbtest.Observer(t, db, "obsa", "Obs A", "CVG", nodesNow, nodesNow)
	tx := dbtest.ChannelTx(t, db, "dup_hash", "3A", `{"k":1}`, nodesNow)
	in := database.ObservationInput{
		TransmissionID: tx, ObserverIdx: &obs, Direction: ptr("RX"),
		SNR: ptr(7.5), RSSI: ptr(-90.0), Timestamp: nodesNow, RawHex: "AABB",
	}

	first, inserted, err := db.InsertObservationWithHops(ctx, in, []string{"aa"}, nil)
	if err != nil || !inserted {
		t.Fatalf("first insert: want inserted, got %v (%v)", inserted, err)
	}
	second, inserted, err := db.InsertObservationWithHops(ctx, in, []string{"aa"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if inserted || second != first {
		t.Errorf("a repeated observation must report not inserted with the same id, got id %d (first %d) inserted=%v", second, first, inserted)
	}
	byTx, err := db.ListObservationsForTransmissions(ctx, []int64{tx})
	if err != nil {
		t.Fatal(err)
	}
	if len(byTx[tx]) != 1 {
		t.Errorf("want one stored observation, got %+v", byTx[tx])
	}
}

func TestObservationRawHexForHash_StoresOnlyFramesThatDifferFromTheTransmission_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	obsA := dbtest.Observer(t, db, "obsa", "Obs A", "CVG", nodesNow, nodesNow)
	obsB := dbtest.Observer(t, db, "obsb", "Obs B", "DAY", nodesNow, nodesNow)
	obsC := dbtest.Observer(t, db, "obsc", "Obs C", "LUK", nodesNow, nodesNow)
	tx := dbtest.ChannelTx(t, db, "frames_hash", "3A", `{"k":1}`, nodesNow)

	idSame, _, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: tx, ObserverIdx: &obsA, Timestamp: nodesNow, RawHex: "AABB",
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	idB, _, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: tx, ObserverIdx: &obsB, Timestamp: nodesNow.Add(time.Second), RawHex: "CCDD",
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	idC, _, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: tx, ObserverIdx: &obsC, Timestamp: nodesNow.Add(2 * time.Second), RawHex: "1122",
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	frames, err := db.ObservationRawHexForHash(ctx, "frames_hash")
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 2 || frames[idB] != "CCDD" || frames[idC] != "1122" {
		t.Errorf("want each differing observation's own frame, got %v", frames)
	}
	if _, stored := frames[idSame]; stored {
		t.Errorf("observation %d carries the transmission's own frame, so none may be stored for it", idSame)
	}

	for name, hash := range map[string]string{"empty hash": "", "unknown hash": "nope"} {
		got, err := db.ObservationRawHexForHash(ctx, hash)
		if err != nil || got == nil || len(got) != 0 {
			t.Errorf("%s: want an empty non-nil map, got %v (%v)", name, got, err)
		}
	}
}

func TestListObservations_NewestFirstAndEmpties_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	obsA := dbtest.Observer(t, db, "obsa", "Obs A", "CVG", nodesNow, nodesNow)
	obsB := dbtest.Observer(t, db, "obsb", "Obs B", "DAY", nodesNow, nodesNow)
	tx := dbtest.ChannelTx(t, db, "order_hash", "3A", `{"k":1}`, nodesNow)

	idOld, _, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: tx, ObserverIdx: &obsA, Timestamp: nodesNow.Add(time.Minute),
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	idNew, _, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: tx, ObserverIdx: &obsB, Timestamp: nodesNow.Add(2 * time.Minute),
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	byTx, err := db.ListObservationsForTransmissions(ctx, []int64{tx})
	if err != nil {
		t.Fatal(err)
	}
	if len(byTx[tx]) != 2 || byTx[tx][0].ID != idNew || byTx[tx][1].ID != idOld {
		t.Errorf("ListObservationsForTransmissions: want newest first (%d, %d), got %+v", idNew, idOld, byTx[tx])
	}
	if byTx[tx][0].Timestamp.Location() != time.UTC {
		t.Errorf("want Timestamp in UTC, got %v", byTx[tx][0].Timestamp.Location())
	}

	byHash, err := db.GetObservationsForHash(ctx, "ORDER_HASH")
	if err != nil {
		t.Fatal(err)
	}
	if len(byHash) != 2 || byHash[0].ID != idNew || byHash[1].ID != idOld {
		t.Errorf("GetObservationsForHash: want newest first (%d, %d), got %+v", idNew, idOld, byHash)
	}
	for _, o := range byHash {
		if o.TransmissionID != tx {
			t.Errorf("observation %d: want TransmissionID %d, got %d", o.ID, tx, o.TransmissionID)
		}
	}

	unknown, err := db.GetObservationsForHash(ctx, "nope")
	if err != nil || unknown == nil || len(unknown) != 0 {
		t.Errorf("unknown hash: want an empty non-nil slice, got %v (%v)", unknown, err)
	}
	for name, ids := range map[string][]int64{"nil": nil, "empty": {}} {
		got, err := db.ListObservationsForTransmissions(ctx, ids)
		if err != nil || len(got) != 0 {
			t.Errorf("%s ids: want nothing, got %v (%v)", name, got, err)
		}
	}
}

func TestGetMaxObservationID_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()

	empty, err := db.GetMaxObservationID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if empty != 0 {
		t.Errorf("empty table: want 0, got %d", empty)
	}

	obs := dbtest.Observer(t, db, "obsa", "Obs A", "CVG", nodesNow, nodesNow)
	tx := dbtest.ChannelTx(t, db, "max_hash", "3A", `{"k":1}`, nodesNow)
	var last int64
	for i, pathHash := range []string{"a001", "a002", "a003"} {
		id, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
			TransmissionID: tx, ObserverIdx: &obs, PathHash: pathHash,
			Timestamp: nodesNow.Add(time.Duration(i) * time.Minute),
		}, nil, nil)
		if err != nil || !inserted {
			t.Fatalf("observation %d: inserted=%v err=%v", i, inserted, err)
		}
		last = max(last, id)
	}
	if last != 3 {
		t.Fatalf("want three distinct observations with ids up to 3, got max id %d", last)
	}

	got, err := db.GetMaxObservationID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got != last {
		t.Errorf("want the newest observation id %d, got %d", last, got)
	}
}

type resolvedFixture struct {
	txA, txB, txC, txD int64
	obsA               int64
}

func TestBestResolvedPaths_KeepsUnresolvedHopsAsNilAndOmitsBareTransmissions_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	f := seedResolvedPaths(t, db)

	got, err := db.BestResolvedPaths(ctx, []int{int(f.txA), int(f.txB), int(f.txC)})
	if err != nil {
		t.Fatal(err)
	}
	pathA := got[int(f.txA)]
	if len(pathA) != 3 || pathA[1] != nil || !slices.Equal(resolvedStrings(pathA), []string{"aaaa", "", "bbbb"}) {
		t.Errorf("txA: want [aaaa nil bbbb], got %v", resolvedStrings(pathA))
	}
	if pathB := got[int(f.txB)]; len(pathB) == 0 || pathB[0] == nil || *pathB[0] != "aaaa" {
		t.Errorf("txB: want a path starting with aaaa, got %v", resolvedStrings(pathB))
	}
	if _, present := got[int(f.txC)]; present {
		t.Errorf("txC resolved nothing and must be absent, got %v", got[int(f.txC)])
	}

	empty, err := db.BestResolvedPaths(ctx, nil)
	if err != nil || empty == nil || len(empty) != 0 {
		t.Errorf("empty input: want an empty non-nil map, got %v (%v)", empty, err)
	}
}

func TestResolvedPubkeysForTransmissions_DistinctAcrossObservations_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	f := seedResolvedPaths(t, db)

	got, err := db.ResolvedPubkeysForTransmissions(ctx, []int{int(f.txA), int(f.txB), int(f.txC)})
	if err != nil {
		t.Fatal(err)
	}
	pubkeysA := slices.Clone(got[int(f.txA)])
	slices.Sort(pubkeysA)
	if !slices.Equal(pubkeysA, []string{"aaaa", "bbbb"}) {
		t.Errorf("txA: want [aaaa bbbb], got %v", got[int(f.txA)])
	}
	if !slices.Equal(got[int(f.txB)], []string{"aaaa"}) {
		t.Errorf("txB: two observations resolve aaaa, want it once, got %v", got[int(f.txB)])
	}
	if _, present := got[int(f.txC)]; present {
		t.Errorf("txC resolved nothing and must be absent, got %v", got[int(f.txC)])
	}

	empty, err := db.ResolvedPubkeysForTransmissions(ctx, nil)
	if err != nil || empty == nil || len(empty) != 0 {
		t.Errorf("empty input: want an empty non-nil map, got %v (%v)", empty, err)
	}
}

// txA has one observation resolving aaaa and bbbb around an unresolved middle hop.
// txB has two observations that both resolve aaaa; the second has the longer path.
// txC resolves nothing.
// txD has two observations of equal length that resolve different hops.
func seedResolvedPaths(t *testing.T, db *database.DB) resolvedFixture {
	t.Helper()
	ctx := t.Context()
	seedNodes(t, db, nodeAt("aaaa", "Hop A", "repeater", nodesNow), nodeAt("bbbb", "Hop B", "repeater", nodesNow))
	obs := dbtest.Observer(t, db, "obsa", "Obs A", "CVG", nodesNow, nodesNow)

	f := resolvedFixture{
		txA: dbtest.ChannelTx(t, db, "res_a", "3A", `{"k":1}`, nodesNow),
		txB: dbtest.ChannelTx(t, db, "res_b", "3A", `{"k":2}`, nodesNow),
		txC: dbtest.ChannelTx(t, db, "res_c", "3A", `{"k":3}`, nodesNow),
		txD: dbtest.ChannelTx(t, db, "res_d", "3A", `{"k":4}`, nodesNow),
	}
	insert := func(tx int64, pathHash string, at time.Time, hops []string, resolved []*string) int64 {
		id, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
			TransmissionID: tx, ObserverIdx: &obs, PathHash: pathHash, Timestamp: at,
		}, hops, resolved)
		if err != nil || !inserted {
			t.Fatalf("seed observation for tx %d: inserted=%v err=%v", tx, inserted, err)
		}
		return id
	}
	f.obsA = insert(f.txA, "a001", nodesNow.Add(time.Minute), []string{"aa", "bb", "cc"}, []*string{ptr("aaaa"), nil, ptr("bbbb")})
	insert(f.txB, "b001", nodesNow.Add(2*time.Minute), []string{"aa"}, []*string{ptr("aaaa")})
	insert(f.txB, "b002", nodesNow.Add(3*time.Minute), []string{"aa", "dd"}, []*string{ptr("aaaa"), nil})
	insert(f.txC, "c001", nodesNow.Add(4*time.Minute), []string{"dd"}, nil)
	insert(f.txD, "d001", nodesNow.Add(5*time.Minute), []string{"aa", "bb"}, []*string{ptr("aaaa"), nil})
	insert(f.txD, "d002", nodesNow.Add(6*time.Minute), []string{"aa", "bb"}, []*string{nil, ptr("bbbb")})
	return f
}

func resolvedStrings(path []*string) []string {
	out := make([]string, len(path))
	for i, pk := range path {
		if pk != nil {
			out[i] = *pk
		}
	}
	return out
}

func sortedInts(ids []int) []int {
	out := slices.Clone(ids)
	slices.Sort(out)
	return out
}

func TestTransmissionIDsWithResolvedPubkey_OnlyRequestedIDsAndCaseInsensitive_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	f := seedResolvedPaths(t, db)
	all := []int{int(f.txA), int(f.txB), int(f.txC)}

	got, err := db.TransmissionIDsWithResolvedPubkey(ctx, all, "BBBB")
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{int(f.txA)}; !slices.Equal(sortedInts(got), want) {
		t.Errorf("bbbb (upper-case lookup): want %v, got %v", want, got)
	}

	got, err = db.TransmissionIDsWithResolvedPubkey(ctx, all, "aaaa")
	if err != nil {
		t.Fatal(err)
	}
	if want := sortedInts([]int{int(f.txA), int(f.txB)}); !slices.Equal(sortedInts(got), want) {
		t.Errorf("aaaa: want %v, got %v", want, got)
	}

	got, err = db.TransmissionIDsWithResolvedPubkey(ctx, []int{int(f.txB), int(f.txC)}, "bbbb")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("a transmission outside the requested ids must not appear, got %v", got)
	}

	got, err = db.TransmissionIDsWithResolvedPubkey(ctx, nil, "aaaa")
	if err != nil || len(got) != 0 {
		t.Errorf("empty input: want nothing, got %v (%v)", got, err)
	}
}

func TestBestResolvedPaths_LongestPathWinsThenLowestID_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	f := seedResolvedPaths(t, db)

	got, err := db.BestResolvedPaths(ctx, []int{int(f.txA), int(f.txB), int(f.txC), int(f.txD)})
	if err != nil {
		t.Fatal(err)
	}

	pathA := got[int(f.txA)]
	if len(pathA) != 3 || pathA[1] != nil || !slices.Equal(resolvedStrings(pathA), []string{"aaaa", "", "bbbb"}) {
		t.Errorf("txA: want [aaaa nil bbbb] with a nil middle hop, got %v", resolvedStrings(pathA))
	}
	if want := []string{"aaaa", ""}; !slices.Equal(resolvedStrings(got[int(f.txB)]), want) {
		t.Errorf("txB: the longer observation wins, want %v, got %v", want, resolvedStrings(got[int(f.txB)]))
	}
	if want := []string{"aaaa", ""}; !slices.Equal(resolvedStrings(got[int(f.txD)]), want) {
		t.Errorf("txD: equal lengths are broken by the lowest observation id, want %v, got %v", want, resolvedStrings(got[int(f.txD)]))
	}
	if _, present := got[int(f.txC)]; present {
		t.Errorf("txC resolved nothing and must be absent, got %v", got[int(f.txC)])
	}

	empty, err := db.BestResolvedPaths(ctx, nil)
	if err != nil || empty == nil || len(empty) != 0 {
		t.Errorf("empty input: want an empty non-nil map, got %v (%v)", empty, err)
	}
}

func TestResolvedPaths_ByTransmissionAndByObservation_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	f := seedResolvedPaths(t, db)

	got, err := db.ResolvedPaths(ctx, []int{int(f.txA)}, []int{int(f.obsA)})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"aaaa", "", "bbbb"}; !slices.Equal(resolvedStrings(got.ByTx[int(f.txA)]), want) {
		t.Errorf("ByTx[txA]: want %v, got %v", want, resolvedStrings(got.ByTx[int(f.txA)]))
	}
	byObs := got.ByObs[int(f.obsA)]
	if want := []string{"aaaa", "", "bbbb"}; !slices.Equal(resolvedStrings(byObs), want) || byObs[1] != nil {
		t.Errorf("ByObs[obsA]: want %v with a nil middle hop, got %v", want, resolvedStrings(byObs))
	}

	onlyTx, err := db.ResolvedPaths(ctx, []int{int(f.txA)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(onlyTx.ByTx) != 1 || onlyTx.ByObs == nil || len(onlyTx.ByObs) != 0 {
		t.Errorf("transmissions only: want ByTx filled and an empty non-nil ByObs, got %+v", onlyTx)
	}

	none, err := db.ResolvedPaths(ctx, nil, nil)
	if err != nil || none.ByTx == nil || none.ByObs == nil || len(none.ByTx) != 0 || len(none.ByObs) != 0 {
		t.Errorf("no ids: want two empty non-nil maps, got %+v (%v)", none, err)
	}
}

func TestResolvedPubkeysForTransmissions_DistinctAndSorted_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	f := seedResolvedPaths(t, db)

	got, err := db.ResolvedPubkeysForTransmissions(ctx, []int{int(f.txA), int(f.txB), int(f.txC)})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got[int(f.txA)], []string{"aaaa", "bbbb"}) {
		t.Errorf("txA: want [aaaa bbbb] sorted, got %v", got[int(f.txA)])
	}
	if !slices.Equal(got[int(f.txB)], []string{"aaaa"}) {
		t.Errorf("txB: two observations resolve aaaa, want it once, got %v", got[int(f.txB)])
	}
	if _, present := got[int(f.txC)]; present {
		t.Errorf("txC resolved nothing and must be absent, got %v", got[int(f.txC)])
	}

	empty, err := db.ResolvedPubkeysForTransmissions(ctx, nil)
	if err != nil || empty == nil || len(empty) != 0 {
		t.Errorf("empty input: want an empty non-nil map, got %v (%v)", empty, err)
	}
}

func TestResolvedPaths_AResolvedPubkeyWithoutANodeStaysUnresolved_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	obs := dbtest.Observer(t, db, "obsa", "Obs A", "CVG", nodesNow, nodesNow)
	tx := dbtest.ChannelTx(t, db, "ghost_hash", "3A", `{"k":1}`, nodesNow)

	obsID, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: tx, ObserverIdx: &obs, Timestamp: nodesNow.Add(time.Minute),
	}, []string{"ff"}, []*string{ptr("ffff")})
	if err != nil || !inserted {
		t.Fatalf("want the observation inserted even though node ffff does not exist: inserted=%v err=%v", inserted, err)
	}

	paths, err := db.ResolvedPaths(ctx, []int{int(tx)}, []int{int(obsID)})
	if err != nil {
		t.Fatal(err)
	}
	if len(paths.ByTx) != 0 || len(paths.ByObs) != 0 {
		t.Errorf("a pubkey with no node row must not resolve, got %+v", paths)
	}

	confirmed, err := db.TransmissionIDsWithResolvedPubkey(ctx, []int{int(tx)}, "ffff")
	if err != nil || len(confirmed) != 0 {
		t.Errorf("want no transmission confirmed for an unknown node, got %v (%v)", confirmed, err)
	}
}
