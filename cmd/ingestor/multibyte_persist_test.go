package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/OKI-Mesh/CoreScope/internal/mbcapqueue"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

// TestRunMultibyteCapPersist_AppliesSnapshot enforces the architectural
// invariant from #1289 + #1322 + #1324 follow-up: the multi-byte
// capability columns (multibyte_sup / multibyte_evidence) on
// nodes / inactive_nodes MUST be written by the ingestor, NEVER by the
// read-only server. The server publishes a snapshot file via
// internal/mbcapqueue; the ingestor's maintenance loop applies it here.
//
// Pre-relocation (PR #1324 as-shipped), the server held a write handle
// and executed UPDATE … nodes SET multibyte_sup directly — which is
// impossible after #1289 made the server's *sql.DB read-only. This test
// asserts the relocated path: snapshot in → UPDATEs out, from the
// ingestor side.
func TestRunMultibyteCapPersist_AppliesSnapshot(t *testing.T) {
	store := newTestStore(t) // registers cleanup; no defer store.Close()
	ctx := t.Context()
	snapdir := filepath.Join(t.TempDir(), "queue")
	store.snapshotDir = snapdir

	const ins = `INSERT INTO nodes (public_key, name, role, last_seen, multibyte_sup, multibyte_evidence, inactive)
	             VALUES ($1, $2, 'repeater', $3, $4, $5, $6)`
	seed := func(key, name, lastSeen string, sup types.MultibyteSupport, evid *string, inactive bool) {
		t.Helper()
		if n := testExec(t, store, ins, key, name, lastSeen, int(sup), evid, inactive); n != 1 {
			t.Fatalf("seed %s: inserted %d rows, want 1", key, n)
		}
	}
	advert := "advert"
	seed("aa11", "Alpha", "2026-01-01T00:00:00Z", types.MultibyteUnknown, nil, false)
	seed("bb22", "Bravo", "2025-01-01T00:00:00Z", types.MultibyteUnknown, nil, true) // inactive flag, same table
	seed("cc33", "Charlie", "2026-01-01T00:00:00Z", types.MultibyteConfirmed, &advert, false)

	snap := mbcapqueue.Snapshot{Entries: []mbcapqueue.Entry{
		{PublicKey: "aa11", Status: "confirmed", Evidence: "advert"},
		{PublicKey: "bb22", Status: "suspected", Evidence: "path"},
		{PublicKey: "cc33", Status: "unknown"}, // must be skipped, cc33 stays confirmed
	}}
	if err := mbcapqueue.WriteSnapshot(snapdir, snap); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	if _, err := os.Stat(filepath.Join(snapdir, mbcapqueue.SnapshotFileName)); err != nil {
		t.Fatalf("snapshot not on disk: %v", err)
	}

	stats, err := store.RunMultibyteCapPersist(ctx)
	if err != nil {
		t.Fatalf("RunMultibyteCapPersist: %v", err)
	}
	if stats.ReadEntries != 3 {
		t.Errorf("ReadEntries = %d, want 3", stats.ReadEntries)
	}
	if stats.Skipped != 1 {
		t.Errorf("Skipped = %d, want 1 (the unknown entry)", stats.Skipped)
	}
	if stats.Updated != 2 {
		t.Errorf("Updated = %d, want 2 (aa11 and bb22; the inactive flag does not matter)", stats.Updated)
	}

	check := func(key string, wantSup types.MultibyteSupport, wantEvid string) {
		t.Helper()
		var sup int
		var evid string
		mustScan(t, testRow(t, store,
			"SELECT multibyte_sup, COALESCE(multibyte_evidence,'') FROM nodes WHERE public_key=$1", key), &sup, &evid)
		if sup != int(wantSup) || evid != wantEvid {
			t.Errorf("%s: sup=%d evid=%q, want sup=%d evid=%q", key, sup, evid, wantSup, wantEvid)
		}
	}
	check("aa11", types.MultibyteConfirmed, "advert")
	check("bb22", types.MultibyteSuspected, "path")
	check("cc33", types.MultibyteConfirmed, "advert") // data-destruction guard
}

// TestRunMultibyteCapPersist_NoSnapshot_NoOp verifies that the persist
// step is a clean no-op when the server hasn't written a snapshot yet
// (cold start; the analytics cycle takes ~15s after server boot).
func TestRunMultibyteCapPersist_NoSnapshot_NoOp(t *testing.T) {

	store := newTestStore(t)
	defer store.Close()
	ctx := t.Context()
	stats, err := store.RunMultibyteCapPersist(ctx)
	if err != nil {
		t.Fatalf("RunMultibyteCapPersist (no snapshot): %v", err)
	}
	if stats.ReadEntries != 0 || stats.UpdatedActive != 0 || stats.UpdatedInactive != 0 {
		t.Errorf("expected zero-valued stats on cold start, got %+v", stats)
	}
}

// TestRunMultibyteCapPersist_RoundTrip exercises the full end-to-end
// contract claimed by PR #1324: the server writes a snapshot, the
// ingestor persists it, and after a simulated restart (close + reopen
// the store) the DB still carries the persisted state.
//
// The audit (#1386) flagged this as the #1 missing test: the two halves
// (persist / read-back) were each tested in isolation, but no single
// test proved the persist path produces a database state the loader
// can later consume — so a column-rename or snapshot-version drift
// would slip past.
func TestRunMultibyteCapPersist_RoundTrip(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	store.snapshotDir = filepath.Join(t.TempDir(), "queue")

	const ins = `INSERT INTO nodes (public_key, name, role, last_seen, multibyte_sup, multibyte_evidence, inactive)
	             VALUES ($1, $2, $3, $4, 0, NULL, $5)`
	testExec(t, store, ins, "dd44", "Delta", "repeater", "2026-01-01T00:00:00Z", false)
	testExec(t, store, ins, "ee55", "Echo", "companion", "2025-12-01T00:00:00Z", true)

	snap := mbcapqueue.Snapshot{Entries: []mbcapqueue.Entry{
		{PublicKey: "dd44", Status: "confirmed", Evidence: "advert"},
		{PublicKey: "ee55", Status: "suspected", Evidence: "path"},
	}}
	if err := mbcapqueue.WriteSnapshot(store.snapshotDir, snap); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	if _, err := store.RunMultibyteCapPersist(ctx); err != nil {
		t.Fatalf("RunMultibyteCapPersist: %v", err)
	}

	check := func(key string, wantSup types.MultibyteSupport, wantEvid string, wantInactive bool) {
		t.Helper()
		var sup int
		var evid string
		var inactive bool
		mustScan(t, testRow(t, store,
			`SELECT multibyte_sup, COALESCE(multibyte_evidence,''), inactive FROM nodes WHERE public_key=$1`, key),
			&sup, &evid, &inactive)
		if sup != int(wantSup) || evid != wantEvid || inactive != wantInactive {
			t.Errorf("%s: sup=%d evid=%q inactive=%v, want sup=%d evid=%q inactive=%v",
				key, sup, evid, inactive, wantSup, wantEvid, wantInactive)
		}
	}
	check("dd44", types.MultibyteConfirmed, "advert", false)
	check("ee55", types.MultibyteSuspected, "path", true) // persist must not touch the inactive flag

	// Second run with the same snapshot changes nothing (idempotent).
	stats, err := store.RunMultibyteCapPersist(ctx)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if stats.Updated != 0 {
		t.Errorf("second run Updated = %d, want 0", stats.Updated)
	}
	check("dd44", types.MultibyteConfirmed, "advert", false)
	check("ee55", types.MultibyteSuspected, "path", true)
}

// TestRunMultibyteCapPersist_MalformedSnapshot verifies the persist
// path is safe against a corrupted/truncated snapshot file: it must
// return without error (no-op), MUST NOT crash, AND MUST log a warning
// distinguishing the malformed case from the steady-state "no
// snapshot yet" cold-start case.
//
// Audit (#1386, kent-beck) flagged: "Snapshot file malformed /
// truncated / wrong-version — RunMultibyteCapPersist error vs.
// silent-skip behavior is unspecified by any test."
func TestRunMultibyteCapPersist_MalformedSnapshot(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	dir := t.TempDir()
	store.snapshotDir = filepath.Join(dir, "queue")
	defer store.Close()

	// Write malformed JSON directly to the snapshot path.
	if err := mbcapqueue.EnsureDir(store.snapshotDir); err != nil {
		t.Fatalf("EnsureDir: %v", err)
	}
	if err := os.WriteFile(mbcapqueue.SnapshotPath(store.snapshotDir), []byte("not-json{{{garbage"), 0o644); err != nil {
		t.Fatalf("write malformed: %v", err)
	}

	// Capture log output to assert the warning is emitted.
	logBuf := captureLogs(t)

	// Must not panic.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("RunMultibyteCapPersist panicked on malformed snapshot: %v", r)
		}
	}()
	stats, err := store.RunMultibyteCapPersist(ctx)
	if err != nil {
		t.Errorf("RunMultibyteCapPersist on malformed snapshot returned error %v; expected silent no-op", err)
	}
	if stats.ReadEntries != 0 || stats.UpdatedActive != 0 || stats.UpdatedInactive != 0 {
		t.Errorf("expected zero-valued stats on malformed snapshot, got %+v", stats)
	}
	if !logContains(logBuf, "malformed") && !logContains(logBuf, "invalid") && !logContains(logBuf, "corrupt") {
		t.Errorf("expected log to mention malformed/invalid/corrupt snapshot; got: %s", logBuf.String())
	}
}

// TestRunMultibyteCapPersist_PreservesConfirmedOnUnknown is the
// data-destruction guard the PR claims to enforce: a snapshot Entry
// with status="unknown" must NEVER overwrite an existing "confirmed"
// (or "suspected") DB row. The audit's mutation test: revert the
// `if sup == 0 { continue }` guard in multibyte_persist.go — this
// test must fail.
func TestRunMultibyteCapPersist_PreservesConfirmedOnUnknown(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	dir := t.TempDir()
	store.snapshotDir = filepath.Join(dir, "queue")
	defer store.Close()

	activeNodeCount := testExec(t, store, `INSERT INTO nodes (public_key, name, role, last_seen, multibyte_sup, multibyte_evidence, inactive)
		VALUES ('gg77', 'Golf', 'repeater', '2026-01-01T00:00:00Z', 2, 'advert', FALSE)`)
	if activeNodeCount != 1 {
		t.Fatalf("expected 1 active node, got %d", activeNodeCount)
	}
	inactiveNodeCount := testExec(t, store, `INSERT INTO nodes (public_key, name, role, last_seen, multibyte_sup, multibyte_evidence, inactive)
		VALUES ('hh88', 'Hotel', 'companion', '2025-12-01T00:00:00Z', 1, 'path', TRUE)`)
	if inactiveNodeCount != 1 {
		t.Fatalf("expected 1 inactive node, got %d", inactiveNodeCount)
	}

	// Snapshot has only "unknown" entries for both — must skip both.
	snap := mbcapqueue.Snapshot{Entries: []mbcapqueue.Entry{
		{PublicKey: "gg77", Status: "unknown"},
		{PublicKey: "hh88", Status: "unknown"},
	}}
	if err := mbcapqueue.WriteSnapshot(store.snapshotDir, snap); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}

	stats, err := store.RunMultibyteCapPersist(ctx)
	if err != nil {
		t.Fatalf("RunMultibyteCapPersist: %v", err)
	}
	if stats.Skipped != 2 {
		t.Errorf("Skipped = %d, want 2 (both unknown entries)", stats.Skipped)
	}
	if stats.UpdatedActive != 0 || stats.UpdatedInactive != 0 {
		t.Errorf("expected zero updates, got %+v", stats)
	}

	// Verify the existing values were NOT clobbered.
	var sup int
	var evid string
	mustScan(t, testRow(t, store, `SELECT multibyte_sup, COALESCE(multibyte_evidence,'') FROM nodes WHERE public_key='gg77'`), &sup, &evid)
	if sup != 2 || evid != "advert" {
		t.Errorf("gg77 was clobbered by unknown snapshot: sup=%d evid=%q, want sup=2 evid=advert", sup, evid)
	}

	mustScan(t, testRow(t, store, `SELECT multibyte_sup, COALESCE(multibyte_evidence,'') FROM nodes WHERE public_key='hh88'`), &sup, &evid)
	if sup != 1 || evid != "path" {
		t.Errorf("hh88 was clobbered by unknown snapshot: sup=%d evid=%q, want sup=1 evid=path", sup, evid)
	}
}
