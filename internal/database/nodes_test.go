package database_test

import (
	"strings"
	"testing"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
)

func ptr[T any](v T) *T { return &v }

func TestGetNodeLocationsByKeys(t *testing.T) {
	ctx := t.Context()
	db := dbtest.New(t)

	const pk = "aabbccdd11223344"
	dbtest.NodeWith(t, db, database.NodeInput{
		PublicKey: pk,
		Name:      "Test Repeater",
		Role:      "repeater",
		Lat:       ptr(37.77),
		Lon:       ptr(-122.42),
	})
	dbtest.Node(t, db, "eeff00112233aabb", "No Location", "repeater")

	t.Run("known key, mixed-case input", func(t *testing.T) {
		locs, err := db.GetNodeLocationsByKeys(ctx, []string{strings.ToUpper(pk)})
		if err != nil {
			t.Fatalf("GetNodeLocationsByKeys: %v", err)
		}
		if len(locs) != 1 {
			t.Fatalf("expected 1 location, got %d: %v", len(locs), locs)
		}
		entry, ok := locs[pk]
		if !ok {
			t.Fatalf("expected node location for test repeater, got keys %v", locs)
		}
		if entry.Lat == nil || *entry.Lat != 37.77 {
			t.Errorf("lat = %v, want 37.77", entry.Lat)
		}
		if entry.Lon == nil || *entry.Lon != -122.42 {
			t.Errorf("lon = %v, want -122.42", entry.Lon)
		}
		if entry.Role == nil || *entry.Role != "repeater" {
			t.Errorf("role = %v, want repeater", entry.Role)
		}
	})

	t.Run("node without coordinates is present with nil lat/lon", func(t *testing.T) {
		locs, err := db.GetNodeLocationsByKeys(ctx, []string{"eeff00112233aabb"})
		if err != nil {
			t.Fatalf("GetNodeLocationsByKeys: %v", err)
		}
		entry, ok := locs["eeff00112233aabb"]
		if !ok {
			t.Fatal("node without coordinates should still be returned")
		}
		if entry.Lat != nil || entry.Lon != nil {
			t.Errorf("lat/lon = %v/%v, want nil/nil", entry.Lat, entry.Lon)
		}
	})

	t.Run("no keys returns empty map", func(t *testing.T) {
		empty, err := db.GetNodeLocationsByKeys(ctx, []string{})
		if err != nil {
			t.Fatalf("GetNodeLocationsByKeys: %v", err)
		}
		if len(empty) != 0 {
			t.Errorf("expected 0 locations for empty keys, got %d", len(empty))
		}
	})

	t.Run("unknown key returns empty map", func(t *testing.T) {
		unknown, err := db.GetNodeLocationsByKeys(ctx, []string{"nonexistent"})
		if err != nil {
			t.Fatalf("GetNodeLocationsByKeys: %v", err)
		}
		if len(unknown) != 0 {
			t.Errorf("expected 0 locations for unknown key, got %d", len(unknown))
		}
	})
}
