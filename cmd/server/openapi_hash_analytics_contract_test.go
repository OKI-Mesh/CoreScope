package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database/dbtest"
)

func TestHashAnalyticsEndpointsMatchSpec(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := setupTestServer(t)

	t.Run("hash sizes", func(t *testing.T) {
		checker.check(t, router, "/api/analytics/hash-sizes")
	})
	t.Run("hash sizes for a region", func(t *testing.T) {
		checker.check(t, router, "/api/analytics/hash-sizes?region=SJC")
	})
	t.Run("hash sizes for an area", func(t *testing.T) {
		checker.check(t, router, "/api/analytics/hash-sizes?area=nowhere")
	})
	t.Run("hash collisions", func(t *testing.T) {
		checker.check(t, router, "/api/analytics/hash-collisions")
	})
	t.Run("hash collisions for a region", func(t *testing.T) {
		checker.check(t, router, "/api/analytics/hash-collisions?region=SJC")
	})
	t.Run("hash collisions for an area", func(t *testing.T) {
		checker.check(t, router, "/api/analytics/hash-collisions?area=nowhere")
	})
	t.Run("roles", func(t *testing.T) {
		checker.check(t, router, "/api/analytics/roles")
	})
}

// TestHashAnalyticsOutputMatchesSpec seeds two repeaters that share a 3-byte
// prefix, so the responses carry multi-byte nodes, hops, capability entries
// and a collision, and checks the marshaled responses against the spec.
func TestHashAnalyticsOutputMatchesSpec(t *testing.T) {
	ctx := t.Context()
	doc := loadSpec(t)

	db := setupTestDB(t)
	recent := time.Now().UTC().Add(-1 * time.Hour)
	dbtest.Node(t, db, "aabb11ccdd001122", "Node Alpha", "repeater")
	dbtest.Node(t, db, "aabb11eeff334455", "Node Beta", "repeater")
	obsIdx := dbtest.Observer(t, db, "obsa", "Obs A", "SEA", recent, recent)
	seedCollisionAdvert(t, db, obsIdx, "hashan_01", "0180aabb11ccdd",
		`{"pubKey":"aabb11ccdd001122","name":"Node Alpha","type":"ADVERT"}`, 10.0, -91, recent)
	seedCollisionAdvert(t, db, obsIdx, "hashan_02", "0180aabb11eeff",
		`{"pubKey":"aabb11eeff334455","name":"Node Beta","type":"ADVERT"}`, 9.0, -93, recent)

	store := newPacketStoreForTest(t, db)
	if err := store.LoadChunked(ctx, 1000); err != nil {
		t.Fatalf("LoadChunked: %v", err)
	}
	if err := store.RefreshNodeCache(ctx); err != nil {
		t.Fatalf("RefreshNodeCache: %v", err)
	}

	sizes := store.GetAnalyticsHashSizes(ctx, "", "")
	if len(sizes.MultiByteNodes) != 2 {
		t.Errorf("multiByteNodes has %d entries, want 2", len(sizes.MultiByteNodes))
	}
	if len(sizes.TopHops) == 0 || len(sizes.Hourly) == 0 || len(sizes.MultiByteCapability) == 0 {
		t.Errorf("seeded store left a section empty: topHops=%d hourly=%d capability=%d",
			len(sizes.TopHops), len(sizes.Hourly), len(sizes.MultiByteCapability))
	}
	collisions := store.GetAnalyticsHashCollisions(ctx, "", "")
	if got := len(collisions.BySize["3"].Collisions); got != 1 {
		t.Errorf("by_size[3] has %d collisions, want 1", got)
	}

	responses := []struct {
		name   string
		schema string
		value  interface{}
	}{
		{"hash sizes", "HashSizeAnalyticsResponse", sizes},
		{"hash sizes for a region", "HashSizeAnalyticsResponse", store.GetAnalyticsHashSizes(ctx, "SEA", "")},
		{"hash collisions", "HashCollisionResponse", collisions},
		{"hash collisions for a region", "HashCollisionResponse", store.GetAnalyticsHashCollisions(ctx, "SEA", "")},
		{"roles", "RoleAnalyticsResponse", store.GetAnalyticsRoles(ctx)},
	}
	for _, r := range responses {
		t.Run(r.name, func(t *testing.T) {
			schema, ok := doc.Components.Schemas[r.schema]
			if !ok {
				t.Fatalf("%s is not in the spec", r.schema)
			}
			raw, err := json.Marshal(r.value)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var decoded interface{}
			if err := json.Unmarshal(raw, &decoded); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if err := schema.Value.VisitJSON(decoded); err != nil {
				t.Errorf("response does not match %s: %v", r.schema, err)
			}
		})
	}
}

func TestProdHashAnalyticsFixturesMatchSpec(t *testing.T) {
	checkProdFixtures(t, []prodFixture{
		{"hash_sizes_prod.json", "HashSizeAnalyticsResponse"},
		{"hash_sizes_region_prod.json", "HashSizeAnalyticsResponse"},
		{"hash_collisions_prod.json", "HashCollisionResponse"},
		{"roles_prod.json", "RoleAnalyticsResponse"},
	})
}
