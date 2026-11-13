package main

import (
	"sync"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// TestObserverAnalyticsBucketDur pins the bucket-size table copied from the
// legacy handler (#1828 Phase A).
func TestObserverAnalyticsBucketDur(t *testing.T) {
	cases := []struct {
		days int
		want time.Duration
	}{
		{1, time.Hour},
		{7, 4 * time.Hour},
		{8, 24 * time.Hour},
		{30, 24 * time.Hour},
	}
	for _, c := range cases {
		if got := observerAnalyticsBucketDur(c.days); got != c.want {
			t.Errorf("bucketDur(%d) = %v, want %v", c.days, got, c.want)
		}
	}
}

// TestObserverAnalyticsFormatLabel pins the label formatting per day range.
func TestObserverAnalyticsFormatLabel(t *testing.T) {
	ts := time.Date(2026, 3, 4, 15, 30, 0, 0, time.UTC)
	if got := observerAnalyticsFormatLabel(1)(ts); got != "15:30" {
		t.Errorf("days=1 label = %q, want %q", got, "15:30")
	}
	if got := observerAnalyticsFormatLabel(7)(ts); got != "Wed 15:30" {
		t.Errorf("days=7 label = %q, want %q", got, "Wed 15:30")
	}
	if got := observerAnalyticsFormatLabel(30)(ts); got != "Mar 04" {
		t.Errorf("days=30 label = %q, want %q", got, "Mar 04")
	}
}

// buildObsForTest constructs a StoreObs with the parsed-time cache primed so
// tests don't depend on time.Parse in the helpers.
func buildObsForTest(txID int, ts time.Time, snr *float64, pathJSON string) *StoreObs {
	o := &StoreObs{
		TransmissionID: txID,
		Timestamp:      ts.UTC(),
		SNR:            snr,
		PathJSON:       pathJSON,
	}
	return o
}

func newStoreForAnalyticsTest() *PacketStore {
	return &PacketStore{
		mu:     sync.RWMutex{},
		byTxID: map[int]*StoreTx{},
	}
}

// TestBuildPacketTypesDirectRead verifies the packet-type histogram builds
// from store.byTxID, and skips obs whose tx or tx.PayloadType is missing.
func TestBuildPacketTypesDirectRead(t *testing.T) {
	store := newStoreForAnalyticsTest()
	pt3, pt5 := types.PayloadType(3), types.PayloadType(5)
	store.byTxID[1] = &StoreTx{ID: 1, PayloadType: &pt3}
	store.byTxID[2] = &StoreTx{ID: 2, PayloadType: &pt5}
	store.byTxID[3] = &StoreTx{ID: 3, PayloadType: nil}

	now := time.Date(2026, 3, 4, 12, 0, 0, 0, time.UTC)
	filtered := []*StoreObs{
		buildObsForTest(1, now, nil, "[]"),
		buildObsForTest(1, now, nil, "[]"),
		buildObsForTest(2, now, nil, "[]"),
		buildObsForTest(3, now, nil, "[]"),
		buildObsForTest(4, now, nil, "[]"),
	}
	got := buildPacketTypes(filtered, store.byTxID)
	if got[pt3] != 2 {
		t.Errorf("packetTypes[3] = %d, want 2", got[pt3])
	}
	if got[pt5] != 1 {
		t.Errorf("packetTypes[5] = %d, want 1", got[pt5])
	}
	if _, ok := got[types.PayloadType(0)]; ok {
		t.Errorf("packetTypes should not contain missing-type entries: %v", got)
	}
	if len(got) != 2 {
		t.Errorf("packetTypes has %d keys, want 2 (got %v)", len(got), got)
	}
}

// TestBuildTimelineBuckets asserts that the timeline aggregates by bucketDur
// and returns entries sorted by bucket time.
func TestBuildTimelineBuckets(t *testing.T) {
	base := time.Date(2026, 3, 4, 12, 0, 0, 0, time.UTC)
	filtered := []*StoreObs{
		buildObsForTest(1, base, nil, "[]"),
		buildObsForTest(1, base.Add(30*time.Minute), nil, "[]"),
		buildObsForTest(1, base.Add(2*time.Hour), nil, "[]"),
	}
	// days=1 → 1-hour buckets → two buckets, counts 2 and 1
	got := buildTimeline(filtered, 1)
	if len(got) != 2 {
		t.Fatalf("timeline entries = %d, want 2 (got %+v)", len(got), got)
	}
	if got[0].Count != 2 || got[1].Count != 1 {
		t.Errorf("timeline counts = [%d, %d], want [2, 1]", got[0].Count, got[1].Count)
	}
}

// TestBuildSnrDistribution asserts 2-unit floor bucketing over SNR values.
func TestBuildSnrDistribution(t *testing.T) {
	f := func(v float64) *float64 { return &v }
	now := time.Date(2026, 3, 4, 12, 0, 0, 0, time.UTC)
	filtered := []*StoreObs{
		buildObsForTest(1, now, f(12.5), "[]"), // bucket 12
		buildObsForTest(1, now, f(13.9), "[]"), // bucket 12
		buildObsForTest(1, now, f(-1.0), "[]"), // bucket -2
		buildObsForTest(1, now, nil, "[]"),     // no SNR → skip
	}
	got := buildSnrDistribution(filtered)
	if len(got) != 2 {
		t.Fatalf("snr entries = %d, want 2 (got %+v)", len(got), got)
	}
	// sorted ascending by bucket
	if got[0].Range != "-2 to 0" || got[0].Count != 1 {
		t.Errorf("snr[0] = %+v, want {Range:'-2 to 0', Count:1}", got[0])
	}
	if got[1].Range != "12 to 14" || got[1].Count != 2 {
		t.Errorf("snr[1] = %+v, want {Range:'12 to 14', Count:2}", got[1])
	}
}

// TestBuildRecentPacketsLimit asserts that recentPackets returns at most
// `limit` entries taken from the head of `filtered`.
func TestBuildRecentPacketsOneEntryPerSelectedObservation(t *testing.T) {
	store := newStoreForAnalyticsTest()
	pt := types.PayloadType(3)
	store.byTxID[1] = &StoreTx{ID: 1, PayloadType: &pt}
	base := time.Date(2026, 3, 4, 12, 0, 0, 0, time.UTC)
	selected := make([]*StoreObs, 0, 20)
	for i := 0; i < 20; i++ {
		selected = append(selected, buildObsForTest(1, base.Add(-time.Duration(i)*time.Minute), nil, "[]"))
	}
	got := buildRecentPackets(store, selected, store.byTxID, database.ResolvedPaths{})
	if len(got) != 20 {
		t.Errorf("recentPackets len = %d, want 20", len(got))
	}
}

// TestBuildRecentPacketsSkipsZeroTimestamp asserts observations with a zero
// Timestamp never become entries. The top-N selection lives in the caller now,
// so this only pins the per-observation skip (regression guard for #1839).
func TestBuildRecentPacketsSkipsZeroTimestamp(t *testing.T) {
	store := newStoreForAnalyticsTest()
	pt := types.PayloadType(3)
	store.byTxID[1] = &StoreTx{ID: 1, PayloadType: &pt}
	base := time.Date(2026, 3, 4, 12, 0, 0, 0, time.UTC)

	selected := make([]*StoreObs, 0, 23)
	for i := 0; i < 3; i++ {
		selected = append(selected, &StoreObs{
			TransmissionID: 1,
			Timestamp:      time.Time{},
			PathJSON:       "[]",
		})
	}
	for i := 0; i < 20; i++ {
		selected = append(selected, buildObsForTest(1, base.Add(-time.Duration(i)*time.Minute), nil, "[]"))
	}

	got := buildRecentPackets(store, selected, store.byTxID, database.ResolvedPaths{})

	if len(got) != 20 {
		t.Errorf("recentPackets len = %d, want 20 (the 3 zero-time observations must be skipped)", len(got))
	}
	for i, e := range got {
		if e.Timestamp.IsZero() {
			t.Errorf("recentPackets[%d] contains a zero-time observation (timestamp=%v)", i, e.Timestamp)
		}
	}
}

// TestBuildNodesTimelineDistinct asserts that nodes-timeline counts distinct
// nodes per bucket (path hops + decoded pubKey/srcHash/destHash).
func TestBuildNodesTimelineDistinct(t *testing.T) {
	store := newStoreForAnalyticsTest()
	pt := types.PayloadType(4)
	// tx=1 has decoded_json with a pubKey
	store.byTxID[1] = &StoreTx{
		ID:          1,
		PayloadType: &pt,
		DecodedJSON: `{"pubKey":"aaaa"}`,
	}
	base := time.Date(2026, 3, 4, 12, 0, 0, 0, time.UTC)
	filtered := []*StoreObs{
		buildObsForTest(1, base, nil, `["bb","cc"]`),                // bucket A: nodes {aaaa, bb, cc}
		buildObsForTest(1, base.Add(10*time.Minute), nil, `["bb"]`), // same bucket: dedup
	}
	got := buildNodesTimeline(filtered, 1, store.byTxID)
	if len(got) != 1 {
		t.Fatalf("nodes timeline entries = %d, want 1 (got %+v)", len(got), got)
	}
	if got[0].Count != 3 {
		t.Errorf("nodes timeline count = %d, want 3 (distinct: aaaa, bb, cc)", got[0].Count)
	}
}
