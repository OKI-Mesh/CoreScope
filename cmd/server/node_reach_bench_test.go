package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// reachFixtureDB seeds the test database with nObs observations of one
// transmission each. matchEvery controls the path mix: 1 means every row's
// path contains the "01fa" hop (the worst case for the scan), 2 means every
// other row matches and the rest carry an unrelated path, and so on. That lets
// benchmarks measure the scan over a realistic mix, not just all-matching.
// Hops are stored lowercase, as the schema requires.
func reachFixtureDB(tb testing.TB, nObs, matchEvery int) *database.DB {
	tb.Helper()
	if matchEvery < 1 {
		matchEvery = 1
	}
	ctx := tb.Context()
	db := setupTestDB(tb)
	seen := time.Date(2026, 6, 7, 0, 0, 0, 0, time.UTC)

	observerIdx, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        "obs",
		Name:      "o",
		LastSeen:  seen,
		IngestNow: seen,
	})
	if err != nil {
		tb.Fatalf("seed observer: %v", err)
	}

	matchHops := []string{"aa", "01fa", "bb"}
	fillerHops := []string{"aa", "cc", "bb"}

	for i := 0; i < nObs; i++ {
		txID, _, err := db.CreateOrUpdateTransmission(ctx, database.TransmissionInput{
			Hash:        fmt.Sprintf("h%d", i),
			RawHex:      "AA",
			RouteType:   types.RouteType(1),
			PayloadType: types.PayloadType(5),
		}, seen)
		if err != nil {
			tb.Fatalf("seed tx %d: %v", i, err)
		}
		hops := fillerHops
		if i%matchEvery == 0 {
			hops = matchHops
		}
		_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
			TransmissionID: txID,
			ObserverIdx:    ptr(observerIdx),
			Direction:      ptr("RX"),
			SNR:            ptr(-7.0),
			Timestamp:      seen,
		}, hops, nil)
		if err != nil {
			tb.Fatalf("seed observation %d: %v", i, err)
		}
		if !inserted {
			tb.Fatalf("expected observation to be inserted for tx %d", txID)
		}
	}
	return db
}

// BenchmarkNodeReachScan measures the windowed scan + path-decode at increasing
// scale, all-matching (worst case for memory/allocs).
func BenchmarkNodeReachScan(b *testing.B) {
	tokens := map[string]bool{"01FA": true}
	for _, n := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			db := reachFixtureDB(b, n, 1)
			srv := &Server{db: db}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, _ := srv.scanReachRows(context.Background(), tokens, time.Time{})
				if len(rows) == 0 {
					b.Fatal("expected rows")
				}
			}
		})
	}
}

// BenchmarkNodeReachScanMixed measures the scan when only half the windowed
// rows actually contain the token — closer to production path mixes.
func BenchmarkNodeReachScanMixed(b *testing.B) {
	tokens := map[string]bool{"01FA": true}
	db := reachFixtureDB(b, 100000, 2)
	srv := &Server{db: db}
	b.ReportAllocs()
	
	for b.Loop() {
		rows, _ := srv.scanReachRows(context.Background(), tokens, time.Time{})
		if len(rows) == 0 {
			b.Fatal("expected rows")
		}
	}
}

// BenchmarkNodeReachScanLowerCase measures the worst case for path decoding:
// lowercase hops force parsePathTokens' ToUpper to allocate a new string per
// hop (production path_json is uppercase, where ToUpper is a no-op). Publishing
// this alongside the all-uppercase numbers keeps the perf claims honest.
func BenchmarkNodeReachScanLowerCase(b *testing.B) {
	tokens := map[string]bool{"01FA": true}
	db := reachFixtureDB(b, 100000, 1)
	srv := &Server{db: db}
	b.ReportAllocs()
	
	for b.Loop() {
		rows, _ := srv.scanReachRows(context.Background(), tokens, time.Time{})
		if len(rows) == 0 {
			b.Fatal("expected rows")
		}
	}
}

// BenchmarkNodeReachAttribute measures the directional attribution pass over an
// already-scanned row set (the in-memory hot loop + map building), isolated
// from DB I/O.
func BenchmarkNodeReachAttribute(b *testing.B) {
	tokens := map[string]bool{"01FA": true}
	db := reachFixtureDB(b, 100000, 1)
	srv := &Server{db: db}
	rows, _ := srv.scanReachRows(context.Background(), tokens, time.Time{})
	if len(rows) == 0 {
		b.Fatal("expected rows")
	}
	resolve := func(tok string) string {
		switch tok {
		case "AA":
			return "aa00000000000000"
		case "BB":
			return "bb00000000000000"
		}
		return ""
	}
	b.ReportAllocs()
	
	for b.Loop() {
		d := attributeDirections(rows, tokens, "01fa326b", resolve)
		if d.relay == 0 {
			b.Fatal("expected relay hits")
		}
	}
}

// TestScanReachRows_ErrorReturn anchors the new ([]pathRow, error) signature
// at the unit-level (issue #1631). Passing a Server whose db.conn is closed
// must surface an error, not a swallowed nil. Lives in this file because
// the bench callers in the same file rely on the same signature.
func TestScanReachRows_ErrorReturn(t *testing.T) {
	db := setupTestDB(t)
	db.Close()

	srv := &Server{db: db}
	rows, err := srv.scanReachRows(t.Context(), map[string]bool{"01fa": true}, time.Time{})
	if err == nil {
		t.Fatalf("expected error from closed DB, got nil (rows=%d)", len(rows))
	}
	if rows != nil {
		t.Fatalf("expected nil rows on error, got %d", len(rows))
	}
}
