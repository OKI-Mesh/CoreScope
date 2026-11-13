// Package dbtest builds test data for packages that use database.DB.
//
// Builders go through the real writers, return ids and fail the test on error.
// The bulk seeders (SeedTransmissions, SeedAgedPackets) use raw SQL on purpose:
// per-row writer calls would make 30K-row benchmarks measure the writer.
//
// dbtest imports database and types only; it must never import cmd/*.
package dbtest

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/pgtest"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

func ptr[T any](v T) *T { return &v }

// New returns a fresh, migrated database cloned from the package template.
// The pool is closed and the database dropped when the test ends, so callers
// must not close it themselves.
func New(tb testing.TB) *database.DB {
	tb.Helper()
	dsn := pgtest.NewDatabase(tb) // registers its own drop-database cleanup first
	db, err := database.OpenReadWrite(tb.Context(), dsn)
	if err != nil {
		tb.Fatalf("open database: %v", err)
	}
	tb.Cleanup(func() { db.Close() }) // LIFO: pool closes before the database is dropped
	return db
}

// Observer creates (or updates) an observer through the real writer and
// returns its row id. The name defaults to the pubkey, which must be lowercase.
// firstSeen is passed as IngestNow, so it becomes observers.first_seen on creation.
func Observer(tb testing.TB, db *database.DB, pubkey, name, iata string, firstSeen, lastSeen time.Time) int64 {
	tb.Helper()
	var storedName *string
	if name != "" {
		storedName = &name
	}
	id, err := db.CreateOrUpdateObserver(tb.Context(), database.ObserverInput{
		ID:        pubkey,
		Name:      storedName,
		IATA:      iata,
		LastSeen:  lastSeen,
		IngestNow: firstSeen,
	})
	if err != nil {
		tb.Fatalf("create observer %s: %v", pubkey, err)
	}
	return id
}

// EnsureObserver returns the id of the observer with this pubkey, creating it
// (dated now) if it does not exist. Safe to call repeatedly.
func EnsureObserver(tb testing.TB, db *database.DB, pubkey string) int64 {
	tb.Helper()
	now := time.Now().UTC()
	return Observer(tb, db, pubkey, pubkey, "", now, now)
}

// ChannelTx creates a type-5 (channel) transmission first heard at firstSeen
// and returns its id. channel is the bare channel hash ("#name" or "3A").
func ChannelTx(tb testing.TB, db *database.DB, hash, channel, decodedJSON string, firstSeen time.Time) int64 {
	tb.Helper()
	id, isNew, err := db.CreateOrUpdateTransmission(tb.Context(), database.TransmissionInput{
		RawHex:      "AABB",
		Hash:        hash,
		RouteType:   types.RouteType(1),
		PayloadType: types.PayloadType(5),
		DecodedJSON: decodedJSON,
		ChannelHash: channel,
	}, firstSeen)
	if err != nil {
		tb.Fatalf("insert tx %s: %v", hash, err)
	}
	if !isNew {
		tb.Fatalf("tx %s already existed", hash)
	}
	return id
}

// EncryptedChannelTx is ChannelTx for a channel the ingestor could not decrypt.
func EncryptedChannelTx(tb testing.TB, db *database.DB, hash, channel, rawHex string, firstSeen time.Time) int64 {
	tb.Helper()
	id, isNew, err := db.CreateOrUpdateTransmission(tb.Context(), database.TransmissionInput{
		RawHex:           rawHex,
		Hash:             hash,
		RouteType:        types.RouteType(1),
		PayloadType:      types.PayloadType(5),
		DecodedJSON:      `{"type":"GRP_TXT","channelHashHex":"` + channel + `","decryptionStatus":"no_key"}`,
		ChannelHash:      channel,
		ChannelEncrypted: true,
	}, firstSeen)
	if err != nil {
		tb.Fatalf("insert encrypted tx %s: %v", hash, err)
	}
	if !isNew {
		tb.Fatalf("tx %s already existed", hash)
	}
	return id
}

// Observation records one RX observation of txID by observerID at `at`, with
// the given hops (none = zero-hop). A hop is either "prefix", which leaves it
// unresolved, or "prefix:pubkey", which resolves it to that node. The pubkey
// must belong to an existing node, otherwise the hop is stored as unresolved.
// Prefixes must already be lowercase hex.
func Observation(tb testing.TB, db *database.DB, txID, observerID int64, at time.Time, snr, rssi float64, hops ...string) {
	tb.Helper()
	prefixes := make([]string, len(hops))
	resolved := make([]*string, len(hops))
	for i, h := range hops {
		prefix, pubkey, hasResolved := strings.Cut(h, ":")
		prefixes[i] = prefix
		if hasResolved {
			pk := pubkey
			resolved[i] = &pk
		}
	}
	_, inserted, err := db.InsertObservationWithHops(tb.Context(), database.ObservationInput{
		TransmissionID: txID,
		ObserverIdx:    ptr(observerID),
		Direction:      ptr("RX"),
		SNR:            ptr(snr),
		RSSI:           ptr(rssi),
		PathHash:       strings.Join(prefixes, ","),
		Timestamp:      at,
	}, prefixes, resolved)
	if err != nil {
		tb.Fatalf("insert observation: %v", err)
	}
	if !inserted {
		tb.Fatalf("expected observation to be inserted for tx %d", txID)
	}
}

// SeedTransmissions inserts numTx advert transmissions (hashes h00000001..),
// each with one observation by observer "obs1" and two hops, in one statement.
// Call it at most once per database: hashes are unique.
func SeedTransmissions(tb testing.TB, db *database.DB, numTx int) {
	tb.Helper()
	observerID := EnsureObserver(tb, db, "obs1")
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 1; i <= numTx; i++ {
		TxWithObservation(tb, db, observerID, database.TransmissionInput{
			RawHex:         "aabb",
			Hash:           fmt.Sprintf("h%08d", i),
			RouteType:      types.RouteType(0),
			PayloadType:    types.PayloadType(4),
			PayloadVersion: 1,
			DecodedJSON:    fmt.Sprintf(`{"pubKey":"pk%08d"}`, i),
		}, base.Add(time.Duration(i)*time.Minute), -10.0, -80.0, "aa", "bb")
	}
}

// SeedAgedPackets inserts numOld transmissions heard 48h ago (hashes old0..)
// and numRecent heard 30 minutes ago (hashes new0..), each with one observation
// by observer "obs1". For numRecent above 1800 the recent rows run into the future.
func SeedAgedPackets(tb testing.TB, db *database.DB, numRecent, numOld int) {
	tb.Helper()
	observerID := EnsureObserver(tb, db, "obs1")
	now := time.Now().UTC()

	insert := func(hashPrefix, rawHex string, n int, start time.Time) {
		tb.Helper()
		for i := 0; i < n; i++ {
			TxWithObservation(tb, db, observerID, database.TransmissionInput{
				RawHex:         rawHex,
				Hash:           hashPrefix + strconv.Itoa(i),
				RouteType:      types.RouteType(0),
				PayloadType:    types.PayloadType(4),
				PayloadVersion: 1,
				DecodedJSON:    "{}",
			}, start.Add(time.Duration(i)*time.Second), -10.0, -80.0)
		}
	}
	insert("old", "aa", numOld, now.Add(-48*time.Hour))
	insert("new", "bb", numRecent, now.Add(-30*time.Minute))
}

// TxWithObservation inserts a transmission plus one observation by observerIdx
// carrying the given hop prefixes (all unresolved). It returns the transmission id.
func TxWithObservation(tb testing.TB, db *database.DB, observerIdx int64, in database.TransmissionInput, rx time.Time, snr, rssi float64, hops ...string) int64 {
	tb.Helper()
	ctx := context.Background()

	txID, _, err := db.CreateOrUpdateTransmission(ctx, in, rx)
	if err != nil {
		tb.Fatalf("dbtest: insert transmission %s: %v", in.Hash, err)
	}
	_, inserted, err := db.InsertObservationWithHops(ctx, database.ObservationInput{
		TransmissionID: txID,
		ObserverIdx:    &observerIdx,
		SNR:            &snr,
		RSSI:           &rssi,
		Timestamp:      rx,
	}, hops, make([]*string, len(hops)))
	if err != nil {
		tb.Fatalf("dbtest: insert observation for %s: %v", in.Hash, err)
	}
	if !inserted {
		tb.Fatalf("dbtest: expected observation to be inserted for tx %d", txID)
	}
	return txID
}

// Node inserts a node through the real writer with last_seen = now.
func Node(tb testing.TB, db *database.DB, pubkey string, name string, role types.NodeRole) {
	tb.Helper()
	NodeWith(tb, db, database.NodeInput{PublicKey: pubkey, Name: name, Role: role})
}

// NodeWith inserts a node from a full NodeInput, for tests that need
// coordinates (Lat/Lon) or a specific RxTime.
func NodeWith(tb testing.TB, db *database.DB, in database.NodeInput) {
	tb.Helper()
	if err := db.CreateOrUpdateNode(context.Background(), in); err != nil {
		tb.Fatalf("dbtest: create node %s: %v", in.PublicKey, err)
	}
}

// NodeAt inserts a node with coordinates through the real writer.
func NodeAt(tb testing.TB, db *database.DB, pubkey string, lat, lon float64) {
	tb.Helper()
	NodeWith(tb, db, database.NodeInput{
		PublicKey: pubkey,
		Lat:       &lat,
		Lon:       &lon,
	})
}

// Advert inserts one ADVERT transmission (payload type 4) seen once by
// observerIdx, through the real writers. Fixed SNR/RSSI; use TxWithObservation
// directly when a test cares about those or needs hops.
func Advert(tb testing.TB, db *database.DB, observerIdx int64, hash, rawHex string, route types.RouteType, decodedJSON string, at time.Time) int64 {
	tb.Helper()
	return TxWithObservation(tb, db, observerIdx, database.TransmissionInput{
		RawHex:      rawHex,
		Hash:        hash,
		RouteType:   route,
		PayloadType: types.PayloadType(4), // ADVERT
		DecodedJSON: decodedJSON,
	}, at, 10.0, -90)
}
