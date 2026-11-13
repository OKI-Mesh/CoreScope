package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
)

func TestStartNodeCache_PopulatesSnapshotBeforeReturning(t *testing.T) {
	ctx := t.Context()
	db := setupTestDB(t)
	t.Cleanup(func() { db.Close() })

	seen := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	pubkeyA := "a1b2" + strings.Repeat("0", 60)
	pubkeyB := "c3d4" + strings.Repeat("0", 60)
	for _, n := range []database.NodeInput{
		{PublicKey: pubkeyA, Name: "Alpha", Role: "repeater", Lat: nil, Lon: nil, RxTime: seen, IngestNow: seen},
		{PublicKey: pubkeyB, Name: "Bravo", Role: "repeater", Lat: nil, Lon: nil, RxTime: seen, IngestNow: seen},
	} {
		if err := db.CreateOrUpdateNode(ctx, n); err != nil {
			t.Fatal(err)
		}
	}

	store := NewPacketStore(db, &PacketStoreConfig{RetentionHours: 24, HotStartupHours: 1})
	if store.nodeSnap.Load() != nil {
		t.Fatal("a new store must have no node snapshot")
	}

	cacheCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	if err := store.StartNodeCache(cacheCtx); err != nil {
		t.Fatal(err)
	}

	nodes, pm := store.getCachedNodesAndPM()
	if len(nodes) != 2 {
		t.Fatalf("want 2 nodes as soon as StartNodeCache returns, got %d", len(nodes))
	}
	for _, prefix := range []string{"a1", "a1b2"} {
		got := pm.m[prefix]
		if len(got) != 1 || got[0].PublicKey != pubkeyA {
			t.Errorf("prefix %s: want only %s, got %+v", prefix, pubkeyA, got)
		}
	}
	if got := pm.m["c3"]; len(got) != 1 || got[0].PublicKey != pubkeyB {
		t.Errorf("prefix c3: want only %s, got %+v", pubkeyB, got)
	}
}

func TestRefreshNodeCache_EmptyDatabaseStillPublishesSnapshot(t *testing.T) {
	db := setupTestDB(t)
	t.Cleanup(func() { db.Close() })

	store := NewPacketStore(db, &PacketStoreConfig{RetentionHours: 24, HotStartupHours: 1})
	if err := store.RefreshNodeCache(t.Context()); err != nil {
		t.Fatal(err)
	}
	if store.nodeSnap.Load() == nil {
		t.Fatal("a refresh that finds no nodes must still publish a snapshot")
	}
	if nodes, _ := store.getCachedNodesAndPM(); len(nodes) != 0 {
		t.Fatalf("want no nodes, got %d", len(nodes))
	}
}
