package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

func mustCreateTestNode(tb testing.TB, ctx context.Context, db *database.DB, input database.NodeInput) {
	tb.Helper()
	if err := db.CreateOrUpdateNode(ctx, input); err != nil {
		tb.Fatalf("CreateOrUpdateNode(%q): %v", input.PublicKey, err)
	}
}

func mustIncrementTestNodeAdvertCount(tb testing.TB, ctx context.Context, db *database.DB, pubkey string, count int) {
	tb.Helper()
	for i := 0; i < count; i++ {
		if err := db.IncrementNodeAdvertCount(ctx, pubkey); err != nil {
			tb.Fatalf("IncrementNodeAdvertCount(%q): %v", pubkey, err)
		}
	}
}

func mustCreateTestObserver(tb testing.TB, ctx context.Context, db *database.DB, id, name, iata string, seen time.Time) int64 {
	tb.Helper()
	_, err := db.CreateOrUpdateObserver(ctx, database.ObserverInput{
		ID:        id,
		Name:      name,
		IATA:      iata,
		LastSeen:  seen,
		IngestNow: seen,
	})
	if err != nil {
		tb.Fatalf("CreateOrUpdateObserver(%q): %v", id, err)
	}
	observerID, err := db.GetObserverID(ctx, id)
	if err != nil {
		tb.Fatalf("GetObserverID(%q): %v", id, err)
	}
	return observerID
}

func mustCreateTestObserverMetrics(tb testing.TB, ctx context.Context, db *database.DB, input database.ObserverMetricsInput) {
	tb.Helper()
	if err := db.CreateOrUpdateObserverMetrics(ctx, input); err != nil {
		tb.Fatalf("CreateOrUpdateObserverMetrics(%q at %s): %v", input.ObserverID, input.Timestamp, err)
	}
}

func mustCreateTestTransmission(tb testing.TB, ctx context.Context, db *database.DB, input database.TransmissionInput, seen time.Time) int64 {
	tb.Helper()
	id, _, err := db.CreateOrUpdateTransmission(ctx, input, seen)
	if err != nil {
		tb.Fatalf("CreateOrUpdateTransmission(%q): %v", input.Hash, err)
	}
	return id
}

func mustInsertUnobservedTestObservation(tb testing.TB, ctx context.Context, db *database.DB, transmissionID int64, seen time.Time, snr, rssi float64, hops []string, resolved []*string) {
	tb.Helper()
	_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: transmissionID,
		SNR:            &snr,
		RSSI:           &rssi,
		PathHash:       strings.Join(hops, ","),
		Timestamp:      seen,
	}, hops, resolved)
	if err != nil {
		tb.Fatalf("InsertObservationWithHops(tx=%d, no observer): %v", transmissionID, err)
	}
	if !inserted {
		tb.Fatalf("expected observation to be inserted for tx %d", transmissionID)
	}
}

func testTransmission(rawHex, hash, decoded string, route types.RouteType, payload types.PayloadType, channel string) database.TransmissionInput {
	return database.TransmissionInput{
		RawHex:      rawHex,
		Hash:        hash,
		RouteType:   route,
		PayloadType: payload,
		DecodedJSON: decoded,
		ChannelHash: channel,
	}
}
