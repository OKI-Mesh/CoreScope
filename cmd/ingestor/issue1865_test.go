package main

// Tests for #1865: ingest the observer /neighbors report as concrete evidence
// of configured region scopes. Covers handleNeighborsReport dispatch semantics
// and the UpdateNodeConfiguredScope store method (status gating, case folding,
// last-write-wins, and the "absence/timeout is not a signal" contract).

import (
	"database/sql"
	"testing"
	"time"
)

// seedNode inserts an active node (lowercase key).
func seedNode(t *testing.T, store *Store, pubkey string) {
	t.Helper()
	if n := testExec(t, store,
		`INSERT INTO nodes (public_key, name, inactive) VALUES ($1, $2, FALSE)`,
		pubkey, "n_"+pubkey[:4]); n < 1 {
		t.Fatalf("failed to insert node %s", pubkey)
	}
}

func seedInactiveNode(t *testing.T, store *Store, pubkey string) {
	t.Helper()
	testExec(t, store, `INSERT INTO nodes (public_key, name, inactive) VALUES ($1, $2, TRUE)`, pubkey, "n_"+pubkey[:4])
}

func configuredScope(t *testing.T, store *Store, pubkey string) (sql.NullString, *time.Time) {
	t.Helper()
	var sc sql.NullString
	var at *time.Time
	mustScan(t, testRow(t, store, "SELECT configured_scope, configured_scope_at FROM nodes WHERE public_key = $1", pubkey), &sc, &at)
	return sc, at
}

func openNeighborsStore(t *testing.T) *Store {
	t.Helper()

	store := newTestStore(t)
	t.Cleanup(func() { store.Close() })
	return store
}

func TestHandleNeighborsReportWritesSelfAndResponded(t *testing.T) {
	store := openNeighborsStore(t)
	ctx := t.Context()

	// Report keys are UPPERCASE; nodes.public_key is lowercase hex.
	const originUpper = "FEEDCA4AD4E2AE615AAAB3CB73FAEC6EF0C7AF4D410F5C58A70FC0F724B7C933"
	const respUpper = "B0D17C59FCF580592F8FB78B67D2F0CE9E9187EF3483A765BDFF1D7947A5109C"
	const timeoutUpper = "0CE5EA7CFA3AB01D11810EF56B73DD899CD6C58644D6A6832A5C1AE89AFC5E25"
	originLower := "feedca4ad4e2ae615aaab3cb73faec6ef0c7af4d410f5c58a70fc0f724b7c933"
	respLower := "b0d17c59fcf580592f8fb78b67d2f0ce9e9187ef3483a765bdff1d7947a5109c"
	timeoutLower := "0ce5ea7cfa3ab01d11810ef56b73dd899cd6c58644d6a6832a5c1ae89afc5e25"

	seedNode(t, store, originLower)
	seedNode(t, store, respLower)
	seedNode(t, store, timeoutLower)
	// Pre-existing confirmed scope on the timeout node — a timeout must NOT clobber it.
	if err := store.UpdateNodeConfiguredScope(ctx, timeoutLower, "eu", time.Date(2026, 7, 24, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}

	report := map[string]interface{}{
		"timestamp": "2026-07-25T10:46:14.000000+00:00",
		"origin_id": originUpper,
		"self":      map[string]interface{}{"scopes": "*"},
		"neighbors": []interface{}{
			map[string]interface{}{"pubkey": timeoutUpper, "scopes": "", "status": "timeout"},
			map[string]interface{}{"pubkey": respUpper, "scopes": "de,eu", "status": "responded"},
		},
	}
	handleNeighborsReport(ctx, store, "test", "obs-topic-id", report)

	// self scopes written under the lowercased origin_id.
	if sc, _ := configuredScope(t, store, originLower); !sc.Valid || sc.String != "*" {
		t.Errorf("self configured_scope = %v, want '*'", sc)
	}
	// responded neighbor written, lowercased.
	if sc, _ := configuredScope(t, store, respLower); !sc.Valid || sc.String != "#de,#eu" {
		t.Errorf("responded configured_scope = %v, want '#de,#eu'", sc)
	}
	// timeout neighbor untouched — prior confirmed value survives.
	if sc, _ := configuredScope(t, store, timeoutLower); !sc.Valid || sc.String != "#eu" {
		t.Errorf("timeout node configured_scope = %v, want prior '#eu' (must not be cleared)", sc)
	}
}

func TestHandleNeighborsReportUnknownNeighborIsNoop(t *testing.T) {
	store := openNeighborsStore(t)
	ctx := t.Context()
	// No node seeded for this pubkey — UPDATE must match no row (no insert, no error).
	report := map[string]interface{}{
		"timestamp": "2026-07-25T10:46:14Z",
		"neighbors": []interface{}{
			map[string]interface{}{"pubkey": "aa" + "00000000000000000000000000000000000000000000000000000000000000"[2:], "scopes": "de", "status": "responded"},
		},
	}
	handleNeighborsReport(ctx, store, "test", "obs", report)
	n := countRows(t, store, "nodes")

	if n != 0 {
		t.Errorf("nodes count = %d, want 0 (report must not create nodes)", n)
	}
}

func TestHandleNeighborsReportRespondedEmptyScopeIsStored(t *testing.T) {
	var reportTS = time.Date(2026, 7, 25, 10, 46, 14, 0, time.UTC)

	store := openNeighborsStore(t)
	ctx := t.Context()
	pk := "cc00000000000000000000000000000000000000000000000000000000000001"
	seedNode(t, store, pk)
	// A responded query with empty scopes is a valid "no scopes configured".
	report := map[string]interface{}{
		"timestamp": reportTS.Format(time.RFC3339),
		"neighbors": []interface{}{
			map[string]interface{}{"pubkey": pk, "scopes": "", "status": "responded"},
		},
	}
	handleNeighborsReport(ctx, store, "test", "obs", report)
	sc, at := configuredScope(t, store, pk)
	if !sc.Valid || sc.String != "" {
		t.Errorf("responded-empty configured_scope = %v, want stored empty string", sc)
	}
	if !at.Equal(reportTS) {
		t.Errorf("configured_scope_at = %v, want the report timestamp", at)
	}
}

func TestUpdateNodeConfiguredScopeAppliesToInactiveNode(t *testing.T) {
	store := openNeighborsStore(t)
	ctx := t.Context()
	pk := "dd00000000000000000000000000000000000000000000000000000000000002"
	seedInactiveNode(t, store, pk)

	if err := store.UpdateNodeConfiguredScope(ctx, pk, "de", time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if sc, _ := configuredScope(t, store, pk); sc.String != "#de" {
		t.Errorf("configured_scope = %q, want '#de' (inactive nodes still record scope reports)", sc.String)
	}
	var inactive bool
	mustScan(t, testRow(t, store, "SELECT inactive FROM nodes WHERE public_key = $1", pk), &inactive)
	if !inactive {
		t.Error("a scope report must not reactivate the node")
	}
}

// TestUpdateNodeConfiguredScopeNormalizesAndOrdersByInstant proves the
// last-write-wins guard orders by chronological instant, not by raw string.
// A "+02:00" report that is lexicographically "greater" but chronologically
// EARLIER than the stored UTC value must not win, and stored timestamps are
// canonicalized to UTC "Z" form regardless of the incoming offset/precision.
func TestUpdateNodeConfiguredScopeNormalizesAndOrdersByInstant(t *testing.T) {
	store := openNeighborsStore(t)
	ctx := t.Context()
	pk := "ee00000000000000000000000000000000000000000000000000000000000001"
	seedNode(t, store, pk)

	// Baseline: noon UTC.
	baseline := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	if err := store.UpdateNodeConfiguredScope(ctx, pk, "eu", baseline); err != nil {
		t.Fatal(err)
	}
	if _, at := configuredScope(t, store, pk); !at.Equal(baseline) {
		t.Fatalf("stored at = %v, want %v", at, baseline)
	}

	// Tie: same instant as the baseline, different scope. Pins > vs >=.
	if err := store.UpdateNodeConfiguredScope(ctx, pk, "tie", time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if sc, _ := configuredScope(t, store, pk); sc.String != "#eu" { // "#tie" if the guard is >=
		t.Errorf("tie: configured_scope = %q, want '#eu' (same-instant report must not overwrite)", sc.String)
	}

	// Earlier instant (08:00 EDT == 12:00Z is a tie; 07:00 EDT == 11:00Z is older): must not overwrite.
	older := time.Date(2026, 7, 25, 7, 0, 0, 0, time.FixedZone("EDT", -4*3600))
	if err := store.UpdateNodeConfiguredScope(ctx, pk, "stale", older); err != nil {
		t.Fatal(err)
	}
	if sc, _ := configuredScope(t, store, pk); sc.String != "#eu" {
		t.Errorf("older report overwrote: configured_scope = %q, want '#eu'", sc.String)
	}

	// "2026-07-25T13:30:00+02:00" == 11:30Z, chronologically EARLIER than 12:00Z,
	// but lexicographically GREATER ("13:30..." > "12:00...Z"). Must be skipped.
	if err := store.UpdateNodeConfiguredScope(ctx, pk, "stale", time.Date(2026, 7, 25, 13, 30, 0, 0, time.FixedZone("", 2*60*60))); err != nil {
		t.Fatal(err)
	}
	if sc, _ := configuredScope(t, store, pk); sc.String != "#eu" {
		t.Errorf("configured_scope = %q, want '#eu' (earlier +02:00 report must not win)", sc.String)
	}

	// 15:00+02:00 == 13:00Z, chronologically LATER than the 12:00Z baseline. Must update.
	later, err := time.Parse(time.RFC3339, "2026-07-25T15:00:00+02:00")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateNodeConfiguredScope(ctx, pk, "de", later); err != nil {
		t.Fatal(err)
	}
	sc, at := configuredScope(t, store, pk)
	if sc.String != "#de" {
		t.Errorf("configured_scope = %q, want '#de' (later +02:00 report should win)", sc.String)
	}
	want := time.Date(2026, 7, 25, 13, 0, 0, 0, time.UTC)
	if !at.Equal(want) {
		t.Errorf("stored at = %v, want the instant %v", at, want)
	}

	got := reportTime(map[string]interface{}{"timestamp": "2026-07-26T09:43:48.000000+00:00"}, time.Time{})
	want = time.Date(2026, 7, 26, 9, 43, 48, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("reportTime = %v, want %v", got, want) // a zero value here means the parse failed
	}
	if got.Location() != time.UTC {
		t.Errorf("reportTime location = %v, want UTC (canonicalized at the parse boundary)", got.Location())
	}

	if err := store.UpdateNodeConfiguredScope(ctx, pk, "dk", got); err != nil {
		t.Fatal(err)
	}
	sc, at = configuredScope(t, store, pk)
	if sc.String != "#dk" {
		t.Errorf("configured_scope = %q, want '#dk' (later report should win)", sc.String)
	}
	if !at.Equal(want) {
		t.Errorf("stored at = %v, want %v", at, want)
	}

}

// #1865: the OTA scope query returns bare region names while every other
// stored scope in CoreScope carries the leading "#". normalizeScopeList makes
// configured_scope comparable with default_scope; these cases pin the parts
// that are easy to get wrong when someone later "simplifies" it.
func TestNormalizeScopeList(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"empty stays empty (responded, no scopes)", "", ""},
		{"whitespace only is empty, not \"#\"", "   ", ""},
		{"bare region gains the hash", "dk", "#dk"},
		{"already hashed is untouched", "#dk", "#dk"},
		{"wildcard is not a region name", "*", "*"},
		{"real observer report, mixed", "*,dk,eu,europe,dk-aarhus", "*,#dk,#eu,#europe,#dk-aarhus"},
		{"already-hashed report is idempotent", "*,#de,#de-sl", "*,#de,#de-sl"},
		{"mixed hashed and bare in one report", "#de,sl", "#de,#sl"},
		{"surrounding spaces are trimmed", " dk , eu ", "#dk,#eu"},
		{"empty entries are dropped, not turned into \"#\"", "dk,,eu,", "#dk,#eu"},
		{"order is preserved", "eu,dk", "#eu,#dk"},
		{"duplicates are kept, not collapsed", "dk,dk", "#dk,#dk"},
		{"case is left alone (keys are sha256 of the name)", "DK", "#DK"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := normalizeScopeList(c.in); got != c.want {
				t.Errorf("normalizeScopeList(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
	// Applying it twice must not add a second "#": the ingest path can see the
	// same report again after a reconnect.
	once := normalizeScopeList("*,dk,#eu")
	if twice := normalizeScopeList(once); twice != once {
		t.Errorf("not idempotent: %q then %q", once, twice)
	}
}
