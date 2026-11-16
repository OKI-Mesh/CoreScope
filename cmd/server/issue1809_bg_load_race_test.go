package main

// Test for issue #1809 — background load fails almost immediately because
// `loadBackgroundChunks` is spawned at FirstChunkReady (chunk #1 merged)
// while `LoadChunked` is still merging the remainder of the hot window.
// At that moment `s.oldestLoaded` is still "" (only set at the end of
// LoadChunked), so the bg loader sees empty oldest → breaks immediately →
// coverage = 0 → `backgroundLoadFailed=true`.
//
// The fix extracts a `RunStartupLoad` helper that runs LoadChunked first
// and only then spawns the background loader. This test calls the helper
// directly and asserts the post-load state.
//
// PR #1811 round-1 fixture rewrite (B1 — tautology trap): the original
// fixture put all 100 rows inside the 1h hot window, so LoadChunked alone
// produced coverage=1.0 and the test passed even if loadBackgroundChunks
// was a no-op. We now spread rows across 14 days with hotStartupHours=24,
// so a no-op bg loader leaves a deliberately incomplete store and the
// assertions fail. The original red-commit assertions
// (oldestLoaded != "", !backgroundLoadFailed, backgroundLoadDone) are
// kept intact; we add the coverage assertions on top.

import (
	"fmt"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
	"github.com/OKI-Mesh/CoreScope/internal/database/pgtest"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// Test1809_StartupLoad_BgLoaderSeesOldestLoaded confirms that after
// RunStartupLoad returns, oldestLoaded is set and backgroundLoadFailed
// is false. The pre-fix code (spawn bg loader at FirstChunkReady)
// produces backgroundLoadFailed=true deterministically because the bg
// loader reads oldestLoaded="" and bails.
func Test1809_StartupLoad_BgLoaderSeesOldestLoaded(t *testing.T) {
	ctx := t.Context()

	nowSec := time.Now().UTC().Unix()
	// Seed 100 rows spread over 14 days with last_seen also spread, so
	// hotStartupHours=24 picks up only ~24/(14*24) ≈ 7 rows in the hot
	// window. The remaining ~93 rows MUST be loaded by the background
	// loader; if it is a no-op the post-load len(packets) and
	// oldestLoaded will betray the regression.
	const totalRows = 100
	const spanDays = 14
	dbPath := createTestDBSpreadOverDays(t, totalRows, spanDays, nowSec)

	db, err := OpenDB(ctx, dbPath)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}

	defer db.Close()

	store := NewPacketStore(db, &PacketStoreConfig{
		RetentionHours:  float64(spanDays * 24), // cover the full seed span
		HotStartupHours: 24,                     // hot window = 1 day
	})

	if err := store.RunStartupLoad(ctx, 500); err != nil {
		t.Fatalf("RunStartupLoad: %v", err)
	}

	// --- original red-commit assertions: KEEP INTACT ---
	if store.oldestLoaded.IsZero() {
		t.Fatalf("oldestLoaded is empty after RunStartupLoad; bg loader would bail")
	}
	if store.backgroundLoadFailed.Load() {
		t.Fatalf("backgroundLoadFailed=true after RunStartupLoad; "+
			"bg loader fired before LoadChunked set oldestLoaded "+
			"(error=%q, loaded=%d, oldest=%q)",
			store.BackgroundLoadError(), len(store.packets), store.oldestLoaded)
	}
	if !store.backgroundLoadDone.Load() {
		t.Fatalf("backgroundLoadDone=false after RunStartupLoad; expected true on success")
	}

	// --- B1 anti-tautology assertions: bg loader actually did work ---

	// Bound the rows the hot window alone could have loaded. The seeder
	// places rows evenly across spanDays so the hot window (24h) catches
	// at most ~totalRows/spanDays + a small fudge for boundary edge.
	hotOnlyMax := (totalRows/spanDays)*2 + 5
	if len(store.packets) <= hotOnlyMax {
		t.Fatalf("len(packets)=%d after RunStartupLoad — bg loader appears to be a no-op "+
			"(hot window alone caps at ~%d rows for %d rows spread over %d days). "+
			"Coverage = backgroundLoadDone may have flipped without bg work.",
			len(store.packets), hotOnlyMax, totalRows, spanDays)
	}

	// oldestLoaded must be older than the hot cutoff after the bg loader
	// has retreated through the retention window. Pre-fix it would equal
	// the hot cutoff (or empty), proving bg loader never advanced.
	oldest := store.oldestLoaded
	hotCutoff := time.Unix(nowSec, 0).UTC().Add(-24 * time.Hour)
	// Allow a small margin since the bg loader chunks daily; oldest
	// should be at least one full day before the hot cutoff.
	if !oldest.Before(hotCutoff.Add(-12 * time.Hour)) {
		t.Fatalf("oldestLoaded=%s is not materially older than hot cutoff %s — "+
			"bg loader did not advance through the retention window",
			oldest.Format(time.RFC3339), hotCutoff.Format(time.RFC3339))
	}
}

func createTestDBSpreadOverDays(t *testing.T, numTx, spanDays int, nowSec int64) string {
	t.Helper()
	dsn := pgtest.NewDatabase(t)
	ctx := t.Context()
	db, err := database.OpenReadWrite(ctx, dsn)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	defer db.Close()

	now := time.Unix(nowSec, 0).UTC()
	observerID := mustCreateTestObserver(t, ctx, db, "obs1", "Obs1", "", now)
	for i := 0; i < numTx; i++ {
		age := time.Duration(numTx-i) * time.Duration(spanDays) * 24 * time.Hour / time.Duration(numTx+1)
		seen := now.Add(-age)
		hash := fmt.Sprintf("spread%010d", i)
		txID := mustCreateTestTransmission(t, ctx, db, database.TransmissionInput{
			RawHex: "aabb", Hash: hash, RouteType: types.RouteTransportFlood,
			PayloadType: types.PayloadADVERT, DecodedJSON: `{}`,
		}, seen)
		dbtest.Observation(t, db, txID, observerID, seen, -10, -80, nil...)
	}
	return dsn
}
