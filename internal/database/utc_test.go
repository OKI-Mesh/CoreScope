package database_test

import (
	"errors"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
)

func TestPacketLookups_ReturnUTC_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seen := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	obs := dbtest.Observer(t, db, "obsd", "Obsd", "SJC", epoch, seen)
	tx := dbtest.ChannelTx(t, db, "utc_lookup_hash", "#utc",
		`{"type":"CHAN","channel":"#utc","text":"Alice: hi","sender":"Alice"}`, seen)
	dbtest.Observation(t, db, tx, obs, seen.Add(time.Minute), 10.0, -90, "aa")

	byHash, err := db.GetPacketByHash(ctx, "UTC_LOOKUP_HASH")
	if err != nil {
		t.Fatal(err)
	}
	if byHash.Hash != "utc_lookup_hash" || byHash.ID != tx {
		t.Errorf("GetPacketByHash: want %d utc_lookup_hash, got %d %s", tx, byHash.ID, byHash.Hash)
	}
	if !byHash.FirstSeen.Equal(seen) || byHash.FirstSeen.Location() != time.UTC {
		t.Errorf("GetPacketByHash: want FirstSeen %v in UTC, got %v (%v)", seen, byHash.FirstSeen, byHash.FirstSeen.Location())
	}

	byID, err := db.GetPacketByID(ctx, int64(tx))
	if err != nil {
		t.Fatal(err)
	}
	if byID.Hash != "utc_lookup_hash" {
		t.Errorf("GetPacketByID: want utc_lookup_hash, got %s", byID.Hash)
	}
	if !byID.FirstSeen.Equal(seen) || byID.FirstSeen.Location() != time.UTC {
		t.Errorf("GetPacketByID: want FirstSeen %v in UTC, got %v (%v)", seen, byID.FirstSeen, byID.FirstSeen.Location())
	}

	observations, err := db.GetObservationsForHash(ctx, "utc_lookup_hash")
	if err != nil {
		t.Fatal(err)
	}
	if len(observations) != 1 {
		t.Fatalf("want 1 observation, got %d", len(observations))
	}
	if !observations[0].Timestamp.Equal(seen.Add(time.Minute)) || observations[0].Timestamp.Location() != time.UTC {
		t.Errorf("GetObservationsForHash: want Timestamp %v in UTC, got %v (%v)",
			seen.Add(time.Minute), observations[0].Timestamp, observations[0].Timestamp.Location())
	}
}

func TestPacketLookups_UnknownHash_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()

	if _, err := db.GetPacketByHash(ctx, "no_such_hash"); !errors.Is(err, database.ErrNotFound) {
		t.Errorf("GetPacketByHash: want ErrNotFound, got %v", err)
	}
	if _, err := db.GetPacketByID(ctx, 987654321); !errors.Is(err, database.ErrNotFound) {
		t.Errorf("GetPacketByID: want ErrNotFound, got %v", err)
	}
	observations, err := db.GetObservationsForHash(ctx, "no_such_hash")
	if err != nil {
		t.Fatalf("an unknown hash is an empty slice, not an error: %v", err)
	}
	if len(observations) != 0 {
		t.Errorf("want no observations, got %+v", observations)
	}
}

func TestListScopeAuditHops_FiltersAndReturnsUTC_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seen := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	obsD := dbtest.Observer(t, db, "obsd", "Obsd", "SJC", epoch, seen)
	obsE := dbtest.Observer(t, db, "obse", "ObsE", "SJC", epoch, seen)

	current := dbtest.ChannelTx(t, db, "audit_current", "#audit",
		`{"type":"CHAN","channel":"#audit","text":"Alice: hi","sender":"Alice"}`, seen)
	dbtest.Observation(t, db, current, obsD, seen, 10.0, -90, "aa", "aabb", "cc")
	dbtest.Observation(t, db, current, obsE, seen, 9.0, -91, "aa")

	old := dbtest.ChannelTx(t, db, "audit_old", "#audit",
		`{"type":"CHAN","channel":"#audit","text":"Bob: hi","sender":"Bob"}`, seen.Add(-2*time.Hour))
	dbtest.Observation(t, db, old, obsD, seen.Add(-2*time.Hour), 10.0, -90, "aa")

	query := database.ScopeAuditQuery{
		Since:      seen.Add(-time.Hour),
		Prefixes:   []string{"aa", "aabb"},
		MinHopLen:  1,
		RouteTypes: []int{1},
	}

	got, err := db.ListScopeAuditHops(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 rows (aa and aabb of the current transmission, deduplicated across observers), got %+v", got)
	}
	for i, wantHop := range []string{"aa", "aabb"} {
		if got[i].TxID != current || got[i].Hop != wantHop {
			t.Errorf("row %d: want tx %d hop %s, got tx %d hop %s", i, current, wantHop, got[i].TxID, got[i].Hop)
		}
		if got[i].ScopeName != nil {
			t.Errorf("row %d: want nil ScopeName for an unscoped transmission, got %q", i, *got[i].ScopeName)
		}
		if !got[i].FirstSeen.Equal(seen) || got[i].FirstSeen.Location() != time.UTC {
			t.Errorf("row %d: want FirstSeen %v in UTC, got %v (%v)", i, seen, got[i].FirstSeen, got[i].FirstSeen.Location())
		}
	}

	longOnly := query
	longOnly.MinHopLen = 4
	got, err = db.ListScopeAuditHops(ctx, longOnly)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Hop != "aabb" {
		t.Errorf("MinHopLen 4: want only aabb, got %+v", got)
	}

	otherRoute := query
	otherRoute.RouteTypes = []int{2}
	got, err = db.ListScopeAuditHops(ctx, otherRoute)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("route type 2: want none, got %+v", got)
	}

	noPrefixes := query
	noPrefixes.Prefixes = []string{}
	got, err = db.ListScopeAuditHops(ctx, noPrefixes)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("no prefixes: want none, got %+v", got)
	}

	wide := query
	wide.Since = seen.Add(-3 * time.Hour)
	got, err = db.ListScopeAuditHops(ctx, wide)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Errorf("a wider window should add the old transmission's aa: want 3 rows, got %+v", got)
	}
}
