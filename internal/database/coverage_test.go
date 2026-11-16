package database_test

import (
	"slices"
	"testing"
	"time"

	"github.com/OKI-Mesh/CoreScope/internal/database"
	"github.com/OKI-Mesh/CoreScope/internal/types"
)

func seedCoverage(tb testing.TB, db *database.DB, base time.Time) {
	tb.Helper()
	receptions := []types.ClientReception{
		{RxPubkey: "rxa", HeardKey: "a1b2", HeardKeyLen: 2, RSSI: ptr(-90), SNR: ptr(7.5),
			Lat: 39.10, Lon: -84.50, PosAccM: nil, RxAt: base, IngestedAt: base, Src: "ble"},
		{RxPubkey: "rxb", HeardKey: "a1b2", HeardKeyLen: 2, RSSI: nil, SNR: nil,
			Lat: 39.20, Lon: -84.40, PosAccM: nil, RxAt: base.Add(time.Hour), IngestedAt: base, Src: "ble"},
		{RxPubkey: "rxa", HeardKey: "c3d4", HeardKeyLen: 2, RSSI: ptr(-80), SNR: ptr(3.0),
			Lat: 39.30, Lon: -84.30, PosAccM: nil, RxAt: base.Add(2 * time.Hour), IngestedAt: base, Src: "ble"},
		{RxPubkey: "rxa", HeardKey: "a1b2", HeardKeyLen: 2, RSSI: ptr(-95), SNR: ptr(1.0),
			Lat: 41.00, Lon: -84.50, PosAccM: nil, RxAt: base.Add(10 * time.Minute), IngestedAt: base, Src: "ble"},
		{RxPubkey: "rxa", HeardKey: "a1b2", HeardKeyLen: 2, RSSI: ptr(-85), SNR: ptr(5.0),
			Lat: 40.00, Lon: -84.00, PosAccM: nil, RxAt: base.Add(3 * time.Hour), IngestedAt: base, Src: "ble"},
	}
	for i := range receptions {
		inserted, err := db.InsertClientReception(tb.Context(), receptions[i])
		if err != nil {
			tb.Fatal(err)
		}
		if !inserted {
			tb.Fatalf("reception %d: want inserted", i)
		}
	}
}

func latsOf(points []database.CoveragePoint) []float64 {
	lats := make([]float64, len(points))
	for i, p := range points {
		lats[i] = p.Lat
	}
	slices.Sort(lats)
	return lats
}

var coverageBox = database.CoverageQuery{MinLat: 39, MaxLat: 40, MinLon: -85, MaxLon: -84}

func TestListCoverageRows_FiltersByHeardKeyAndBox_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	seedCoverage(t, db, base)

	q := coverageBox
	q.HeardKeys = []string{"a1b2"}
	got, err := db.ListCoverageRows(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if want := []float64{39.10, 39.20, 40.00}; !slices.Equal(latsOf(got), want) {
		t.Errorf("a1b2 in the box (edges inclusive, 41.0 outside): want lats %v, got %v", want, latsOf(got))
	}

	for _, p := range got {
		if p.RxAt.Location() != time.UTC {
			t.Errorf("point %v: want RxAt in UTC, got %v", p.Lat, p.RxAt.Location())
		}
		if p.HeardKey != "a1b2" {
			t.Errorf("point %v: want HeardKey a1b2, got %s", p.Lat, p.HeardKey)
		}
		switch p.Lat {
		case 39.10:
			if p.SNR == nil || *p.SNR != 7.5 || p.RSSI == nil || *p.RSSI != -90 || !p.RxAt.Equal(base) || p.Lon != -84.50 {
				t.Errorf("point 39.10: wrong values %+v", p)
			}
		case 39.20:
			if p.SNR != nil || p.RSSI != nil {
				t.Errorf("point 39.20: want nil SNR and RSSI, got %+v", p)
			}
		}
	}

	q.HeardKeys = []string{"a1b2", "c3d4"}
	got, err = db.ListCoverageRows(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if want := []float64{39.10, 39.20, 39.30, 40.00}; !slices.Equal(latsOf(got), want) {
		t.Errorf("two keys: want lats %v, got %v", want, latsOf(got))
	}
}

func TestListCoverageRows_NoHeardKeysMeansAnyNode_DB(t *testing.T) {
	db := newDB(t)
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	seedCoverage(t, db, base)

	for name, keys := range map[string][]string{"nil": nil, "empty": {}} {
		q := coverageBox
		q.HeardKeys = keys
		got, err := db.ListCoverageRows(t.Context(), q)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if want := []float64{39.10, 39.20, 39.30, 40.00}; !slices.Equal(latsOf(got), want) {
			t.Errorf("%s: want every node in the box, lats %v, got %v", name, want, latsOf(got))
		}
	}
}

func TestListCoverageRows_FiltersByReceiverAndSince_DB(t *testing.T) {
	db := newDB(t)
	ctx := t.Context()
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	seedCoverage(t, db, base)

	q := coverageBox
	q.HeardKeys = []string{"a1b2"}

	q.RxPubkey = "rxa"
	got, err := db.ListCoverageRows(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if want := []float64{39.10, 40.00}; !slices.Equal(latsOf(got), want) {
		t.Errorf("receiver rxa: want lats %v, got %v", want, latsOf(got))
	}

	q.RxPubkey = "rxb"
	got, err = db.ListCoverageRows(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if want := []float64{39.20}; !slices.Equal(latsOf(got), want) {
		t.Errorf("receiver rxb: want lats %v, got %v", want, latsOf(got))
	}

	q.RxPubkey = ""
	since := base.Add(time.Hour)
	q.Since = &since
	got, err = db.ListCoverageRows(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if want := []float64{39.20, 40.00}; !slices.Equal(latsOf(got), want) {
		t.Errorf("since is inclusive: want lats %v, got %v", want, latsOf(got))
	}

	later := base.Add(time.Hour + time.Second)
	q.Since = &later
	got, err = db.ListCoverageRows(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if want := []float64{40.00}; !slices.Equal(latsOf(got), want) {
		t.Errorf("one second later: want lats %v, got %v", want, latsOf(got))
	}
}

func TestListCoverageRows_InvertedLongitudeRangeIsEmpty_DB(t *testing.T) {
	db := newDB(t)
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	seedCoverage(t, db, base)

	q := coverageBox
	q.HeardKeys = []string{"a1b2"}
	q.MinLon, q.MaxLon = -84, -85
	got, err := db.ListCoverageRows(t.Context(), q)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("a box with MinLon > MaxLon does not wrap the antimeridian: want none, got %+v", got)
	}
}

func TestListCoverageRows_HeardKeysAreCaseInsensitive_DB(t *testing.T) {
	db := newDB(t)
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	seedCoverage(t, db, base)

	q := coverageBox
	q.HeardKeys = []string{"A1B2"}
	got, err := db.ListCoverageRows(t.Context(), q)
	if err != nil {
		t.Fatal(err)
	}
	if want := []float64{39.10, 39.20, 40.00}; !slices.Equal(latsOf(got), want) {
		t.Errorf("uppercase key: want lats %v, got %v", want, latsOf(got))
	}

	receptions, clients, err := db.CountMobileReceptions(t.Context(), []string{"A1B2"})
	if err != nil {
		t.Fatal(err)
	}
	if receptions != 4 || clients != 2 {
		t.Errorf("CountMobileReceptions with an uppercase key: want 4 and 2, got %d and %d", receptions, clients)
	}
}

func TestCountMobileReceptions_DB(t *testing.T) {
	db := newDB(t)
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	seedCoverage(t, db, base)

	cases := []struct {
		name          string
		keys          []string
		wantReception int
		wantClients   int
	}{
		{"one key, every receiver and location counted", []string{"a1b2"}, 4, 2},
		{"other key", []string{"c3d4"}, 1, 1},
		{"both keys", []string{"a1b2", "c3d4"}, 5, 2},
		{"unknown key", []string{"ffff"}, 0, 0},
		{"no keys counts nothing", []string{}, 0, 0},
		{"nil keys counts nothing", nil, 0, 0},
	}
	for _, c := range cases {
		receptions, clients, err := db.CountMobileReceptions(t.Context(), c.keys)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if receptions != c.wantReception || clients != c.wantClients {
			t.Errorf("%s: want %d receptions and %d clients, got %d and %d",
				c.name, c.wantReception, c.wantClients, receptions, clients)
		}
	}
}
