package main

import (
	"testing"
	"time"
)

func f64(v float64) *float64 { return &v }

func TestDedupeTopHopsByPair(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	hops := []distHopRecord{
		{FromPk: "AAA", ToPk: "BBB", FromName: "A", ToName: "B", Dist: 100, Type: "R↔R", SNR: f64(5.0), Hash: "h1", Timestamp: base.Add(1 * time.Second)},
		{FromPk: "AAA", ToPk: "BBB", FromName: "A", ToName: "B", Dist: 90, Type: "R↔R", SNR: f64(8.0), Hash: "h2", Timestamp: base.Add(2 * time.Second)},
		{FromPk: "BBB", ToPk: "AAA", FromName: "B", ToName: "A", Dist: 80, Type: "R↔R", SNR: f64(3.0), Hash: "h3", Timestamp: base.Add(3 * time.Second)},
		{FromPk: "AAA", ToPk: "BBB", FromName: "A", ToName: "B", Dist: 70, Type: "R↔R", SNR: f64(6.0), Hash: "h4", Timestamp: base.Add(4 * time.Second)},
		{FromPk: "AAA", ToPk: "BBB", FromName: "A", ToName: "B", Dist: 60, Type: "R↔R", SNR: f64(4.0), Hash: "h5", Timestamp: base.Add(5 * time.Second)},
		{FromPk: "CCC", ToPk: "DDD", FromName: "C", ToName: "D", Dist: 50, Type: "C↔R", SNR: f64(7.0), Hash: "h6", Timestamp: base.Add(6 * time.Second)},
	}

	result := dedupeHopsByPair(hops, 20)

	if len(result) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(result))
	}

	// First entry: A↔B pair, max distance = 100, obsCount = 5
	ab := result[0]
	if ab.Dist != 100 {
		t.Errorf("expected dist 100, got %v", ab.Dist)
	}
	if ab.ObsCount != 5 {
		t.Errorf("expected obsCount 5, got %v", ab.ObsCount)
	}
	if ab.Hash != "h1" {
		t.Errorf("expected hash h1 (from max-dist record), got %v", ab.Hash)
	}
	if ab.BestSnr == nil || *ab.BestSnr != 8.0 {
		t.Errorf("expected bestSnr 8.0, got %v", ab.BestSnr)
	}
	// medianSnr of [3,4,5,6,8] = 5.0
	if ab.MedianSnr == nil || *ab.MedianSnr != 5.0 {
		t.Errorf("expected medianSnr 5.0, got %v", ab.MedianSnr)
	}
	if want := base.Add(1 * time.Second); !ab.Timestamp.Time.Equal(want) {
		t.Errorf("expected timestamp of the max-dist record %v, got %v", want, ab.Timestamp.Time)
	}

	// Second entry: C↔D pair
	cd := result[1]
	if cd.Dist != 50 {
		t.Errorf("expected dist 50, got %v", cd.Dist)
	}
	if cd.ObsCount != 1 {
		t.Errorf("expected obsCount 1, got %v", cd.ObsCount)
	}
}

func TestDedupeTopHopsReversePairMerges(t *testing.T) {
	hops := []distHopRecord{
		{FromPk: "BBB", ToPk: "AAA", FromName: "B", ToName: "A", Dist: 50, Type: "R↔R", Hash: "h1"},
		{FromPk: "AAA", ToPk: "BBB", FromName: "A", ToName: "B", Dist: 80, Type: "R↔R", Hash: "h2"},
	}
	result := dedupeHopsByPair(hops, 20)
	if len(result) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(result))
	}
	if result[0].ObsCount != 2 {
		t.Errorf("expected obsCount 2, got %v", result[0].ObsCount)
	}
	if result[0].Dist != 80 {
		t.Errorf("expected dist 80, got %v", result[0].Dist)
	}
}

func TestDedupeTopHopsNilSNR(t *testing.T) {
	hops := []distHopRecord{
		{FromPk: "AAA", ToPk: "BBB", FromName: "A", ToName: "B", Dist: 100, Type: "R↔R", SNR: nil, Hash: "h1"},
		{FromPk: "AAA", ToPk: "BBB", FromName: "A", ToName: "B", Dist: 90, Type: "R↔R", SNR: nil, Hash: "h2"},
	}
	result := dedupeHopsByPair(hops, 20)
	if len(result) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(result))
	}
	if result[0].BestSnr != nil {
		t.Errorf("expected bestSnr nil, got %v", *result[0].BestSnr)
	}
	if result[0].MedianSnr != nil {
		t.Errorf("expected medianSnr nil, got %v", *result[0].MedianSnr)
	}
}

func TestDedupeTopHopsLimit(t *testing.T) {
	// Generate 25 unique pairs, verify limit=20 caps output
	hops := make([]distHopRecord, 25)
	for i := range hops {
		hops[i] = distHopRecord{
			FromPk: "A", ToPk: string(rune('a' + i)),
			Dist: float64(i), Type: "R↔R", Hash: "h",
		}
	}
	result := dedupeHopsByPair(hops, 20)
	if len(result) != 20 {
		t.Errorf("expected 20 entries, got %d", len(result))
	}
}

func TestDedupeTopHopsEvenMedian(t *testing.T) {
	// Even count: median = avg of two middle values
	hops := []distHopRecord{
		{FromPk: "A", ToPk: "B", Dist: 10, Type: "R↔R", SNR: f64(2.0), Hash: "h1"},
		{FromPk: "A", ToPk: "B", Dist: 20, Type: "R↔R", SNR: f64(4.0), Hash: "h2"},
		{FromPk: "A", ToPk: "B", Dist: 30, Type: "R↔R", SNR: f64(6.0), Hash: "h3"},
		{FromPk: "A", ToPk: "B", Dist: 40, Type: "R↔R", SNR: f64(8.0), Hash: "h4"},
	}
	result := dedupeHopsByPair(hops, 20)
	// sorted SNR: [2,4,6,8], median = (4+6)/2 = 5.0
	if result[0].MedianSnr == nil || *result[0].MedianSnr != 5.0 {
		t.Errorf("expected medianSnr 5.0, got %v", result[0].MedianSnr)
	}
}
