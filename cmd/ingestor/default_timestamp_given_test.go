package main

import (
	"testing"
	"time"
)

func TestInsertTransmissionGivenTimestamp(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	given := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)

	isNew, err := store.InsertTransmission(ctx, &PacketData{
		RawHex:    "AABB",
		Hash:      "given_ts_test1",
		Timestamp: given,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !isNew {
		t.Error("should be new")
	}

	var firstSeen, lastSeen time.Time
	mustScan(t, testRow(t, store,
		`SELECT first_seen, last_seen FROM transmissions WHERE hash = $1`, "given_ts_test1"), &firstSeen, &lastSeen)
	if !firstSeen.Equal(given) {
		t.Errorf("first_seen = %v, want the given %v", firstSeen, given)
	}
	if !lastSeen.Equal(given) {
		t.Errorf("last_seen = %v, want the given %v", lastSeen, given)
	}
}

func TestUpsertNodeGivenTimestamp(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()
	const pk = "pk_given_ts_000000000000000000000000000000001"
	given := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)

	if err := upsertNode(ctx, store, pk, "Node", "companion", nil, nil, given); err != nil {
		t.Fatal(err)
	}

	var lastSeen, firstSeen time.Time
	mustScan(t, testRow(t, store,
		`SELECT last_seen, first_seen FROM nodes WHERE public_key = $1`, pk), &lastSeen, &firstSeen)
	if !lastSeen.Equal(given) {
		t.Errorf("last_seen = %v, want the given %v", lastSeen, given)
	}
	if !firstSeen.Equal(given) {
		t.Errorf("first_seen = %v, want the given %v", firstSeen, given)
	}
}
