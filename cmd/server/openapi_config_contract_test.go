package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/OKI-Mesh/CoreScope/api"
	"github.com/gorilla/mux"
)

const configTestAPIKey = "a-very-strong-key-1234"

var configGetPaths = []string{
	"/api/config/cache",
	"/api/config/client",
	"/api/config/regions",
	"/api/config/theme",
	"/api/config/map",
	"/api/config/geo-filter",
	"/api/config/areas",
	"/api/config/areas/polygons",
}

func TestConfigEndpointsMatchSpec(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := setupTestServer(t)
	for _, path := range configGetPaths {
		t.Run(path, func(t *testing.T) {
			checker.check(t, router, path)
		})
	}
}

// newConfiguredServer builds a server whose config has every operator knob
// that /api/config/* surfaces filled in.
func newConfiguredServer(t *testing.T) (*Server, *mux.Router) {
	t.Helper()
	db := setupTestDB(t)
	seedTestData(t, db)
	maxHop := 1.5
	perfSlow, wsReconnect, cacheInvalid := 250, 3000, 20
	cfg := &Config{
		Port:   3000,
		APIKey: configTestAPIKey,
		Branding: map[string]interface{}{
			"siteName": "Test Mesh",
			"logoUrl":  "https://example.org/logo.svg",
			"unknown":  "dropped",
		},
		Theme:     map[string]interface{}{"accent": "#123456", "font": "Inter"},
		ThemeDark: map[string]interface{}{"accent": "#654321"},
		NodeColors: map[string]interface{}{
			"repeater": "#aa0000",
			"custom":   "#00aa00",
		},
		TypeColors: map[string]interface{}{"REQUEST": "#deadbe"},
		Home: map[string]interface{}{
			"heroTitle": "Hello",
			"steps":     []interface{}{map[string]interface{}{"title": "One", "emoji": "x", "description": "first"}},
			"checklist": []interface{}{map[string]interface{}{"title": "Two", "emoji": "y", "description": "second"}},
		},
		MarkerStroke: map[string]interface{}{"color": "#000", "width": 2.0},
		Roles: map[string]interface{}{
			"colors": map[string]interface{}{"repeater": "#ff0000"},
			"labels": map[string]interface{}{"repeater": "Repeater"},
			"style":  map[string]interface{}{"repeater": map[string]interface{}{"color": "#ff0000"}},
			"emoji":  map[string]interface{}{"repeater": "R"},
			"sort":   []interface{}{"repeater", "companion"},
		},
		Map: map[string]interface{}{
			"tiles": map[string]interface{}{
				"darkDefault": "carto",
				"providers": map[string]interface{}{
					"_comment_providers": "not a provider",
					"carto":              map[string]interface{}{"enabled": true, "key": "k", "token": "t"},
				},
			},
		},
		Tiles:          map[string]interface{}{"dark": "https://d/{z}", "light": "https://l/{z}"},
		SnrThresholds:  map[string]interface{}{"excellent": 10.5, "good": 5.0},
		DistThresholds: map[string]interface{}{"local": 5.0, "regional": 50.0},
		Limits:         map[string]interface{}{"topNodes": 20.0, "feedMax": 500.0},
		ExternalUrls:   map[string]interface{}{"flasher": "https://flasher.example.org"},
		CacheTTL:       map[string]interface{}{"nodes": 60000.0},
		Regions:        map[string]string{"TST": "Test Region"},
		Areas: map[string]AreaEntry{
			"north": {Label: "North", Polygon: [][2]float64{{1, 2}, {3, 4}, {5, 6}}},
			"box":   {Label: "Box", LatMin: fptr(1), LatMax: fptr(2), LonMin: fptr(3), LonMax: fptr(4)},
			"_c":    {},
		},
		MaxHopDist:     &maxHop,
		PerfSlowMs:     &perfSlow,
		WsReconnectMs:  &wsReconnect,
		CacheInvalidMs: &cacheInvalid,
		DebugAffinity:  true,
		Customizer:     &CustomizerConfig{DisabledTabs: []string{"theme"}},
		PathTrust:      &PathTrustConfig{MinHashBytesForMapping: 2},
	}
	cfg.MapDefaults.Center = []float64{10, 20}
	cfg.MapDefaults.Zoom = 5
	srv := NewServer(db, cfg, NewHub())
	srv.setGeoFilter(&GeoFilterConfig{Polygon: [][2]float64{{1, 1}, {1, 2}, {2, 2}}, BufferKm: 3})
	router := mux.NewRouter()
	srv.RegisterRoutes(router)
	return srv, router
}

func TestConfiguredEndpointsMatchSpec(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := newConfiguredServer(t)
	for _, path := range configGetPaths {
		t.Run(path, func(t *testing.T) {
			checker.check(t, router, path)
		})
	}
}

func TestConfiguredResponsesDecodeAsSpecTypes(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := newConfiguredServer(t)
	get := func(path string) []byte {
		return checker.checkStatus(t, router, path, http.StatusOK).Body.Bytes()
	}

	var client api.ClientConfigResponse
	decodeInto(t, get("/api/config/client"), &client)
	if client.Roles == nil || client.Roles.Sort == nil || len(*client.Roles.Sort) != 2 {
		t.Errorf("roles = %+v", client.Roles)
	}
	if client.Map == nil || client.Map.Tiles == nil || client.Map.Tiles.Providers == nil || len(*client.Map.Tiles.Providers) != 1 {
		t.Errorf("map = %+v (the _comment_ provider must be filtered out)", client.Map)
	}
	if client.SnrThresholds == nil || client.DistThresholds == nil || client.Limits == nil || client.Tiles == nil || client.ExternalUrls == nil {
		t.Errorf("operator blocks missing: %+v", client)
	}
	if client.MaxHopDist == nil || *client.MaxHopDist != 1.5 || client.PerfSlowMs == nil || *client.PerfSlowMs != 250 {
		t.Errorf("scalars = %+v", client)
	}
	if client.DebugAffinity == nil || !*client.DebugAffinity {
		t.Errorf("debugAffinity = %v", client.DebugAffinity)
	}
	if client.PathTrust == nil || client.PathTrust.MinHashBytesForMapping == nil || *client.PathTrust.MinHashBytesForMapping != 2 {
		t.Errorf("pathTrust = %+v", client.PathTrust)
	}
	if len(client.Customizer.DisabledTabs) != 1 {
		t.Errorf("customizer = %+v", client.Customizer)
	}

	var theme api.ThemeResponse
	decodeInto(t, get("/api/config/theme"), &theme)
	if theme.Branding.SiteName != "Test Mesh" || theme.Branding.Tagline == "" {
		t.Errorf("branding = %+v (siteName overridden, tagline defaulted)", theme.Branding)
	}
	if theme.NodeColors["custom"] != "#00aa00" || theme.NodeColors["repeater"] != "#aa0000" {
		t.Errorf("nodeColors = %v", theme.NodeColors)
	}
	if theme.TypeColors["REQ"] != "#deadbe" || theme.TypeColors["REQUEST"] != "#deadbe" {
		t.Errorf("typeColors REQ/REQUEST = %v/%v", theme.TypeColors["REQ"], theme.TypeColors["REQUEST"])
	}
	if theme.Home.HeroTitle != "Hello" || len(theme.Home.Steps) != 1 || theme.Home.Checklist == nil || len(*theme.Home.Checklist) != 1 {
		t.Errorf("home = %+v", theme.Home)
	}
	if theme.Home.HeroSubtitle == "" || len(theme.Home.FooterLinks) == 0 {
		t.Errorf("home defaults lost: %+v", theme.Home)
	}
	if theme.MarkerStroke.Width != 2 {
		t.Errorf("markerStroke = %+v", theme.MarkerStroke)
	}

	var m api.MapConfigResponse
	decodeInto(t, get("/api/config/map"), &m)
	if len(m.Center) != 2 || m.Center[0] != 10 || m.Zoom != 5 {
		t.Errorf("map = %+v", m)
	}

	var gf api.GeoFilterResponse
	decodeInto(t, get("/api/config/geo-filter"), &gf)
	if gf.Polygon == nil || len(*gf.Polygon) != 3 || gf.BufferKm != 3 || gf.WriteEnabled == nil || !*gf.WriteEnabled {
		t.Errorf("geo-filter = %+v", gf)
	}

	var areas []api.AreaSummary
	decodeInto(t, get("/api/config/areas"), &areas)
	if len(areas) != 2 {
		t.Errorf("areas = %+v (the label-less entry must be skipped)", areas)
	}

	var bounds []api.AreaBoundary
	decodeInto(t, get("/api/config/areas/polygons"), &bounds)
	if len(bounds) != 3 {
		t.Errorf("polygons = %+v", bounds)
	}

	var regions api.RegionsResponse
	decodeInto(t, get("/api/config/regions"), &regions)
	if regions["TST"] != "Test Region" {
		t.Errorf("regions = %v", regions)
	}

	var cache api.CacheTTLConfig
	decodeInto(t, get("/api/config/cache"), &cache)
	if cache["nodes"] != 60000 {
		t.Errorf("cache = %v", cache)
	}
}

func TestConfigDefaultsDecodeAsSpecTypes(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := setupTestServer(t)

	// An unconfigured server answers with nulls and defaults, not errors.
	var client api.ClientConfigResponse
	decodeInto(t, checker.checkStatus(t, router, "/api/config/client", http.StatusOK).Body.Bytes(), &client)
	if client.Roles != nil || client.Map != nil || client.ExternalUrls != nil || client.MaxHopDist != nil {
		t.Errorf("unconfigured client = %+v", client)
	}
	if client.Customizer.DisabledTabs == nil || len(client.Customizer.DisabledTabs) != 0 {
		t.Errorf("disabledTabs must be an empty array, got %#v", client.Customizer.DisabledTabs)
	}

	var gf api.GeoFilterResponse
	decodeInto(t, checker.checkStatus(t, router, "/api/config/geo-filter", http.StatusOK).Body.Bytes(), &gf)
	if gf.Polygon != nil || gf.BufferKm != 0 {
		t.Errorf("unconfigured geo-filter = %+v", gf)
	}

	var theme api.ThemeResponse
	decodeInto(t, checker.checkStatus(t, router, "/api/config/theme", http.StatusOK).Body.Bytes(), &theme)
	if theme.TypeColors["REQ"] == "" || theme.TypeColors["REQ"] != theme.TypeColors["REQUEST"] {
		t.Errorf("typeColors must dual-emit REQ and REQUEST: %v", theme.TypeColors)
	}
}

// putGeoFilter sends PUT /api/config/geo-filter and checks the response
// against the spec for whatever status comes back.
func putGeoFilter(t *testing.T, checker specChecker, router *mux.Router, key, body string, wantStatus int) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, "/api/config/geo-filter", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("X-API-Key", key)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != wantStatus {
		t.Fatalf("PUT %s: status %d, want %d; body=%s", body, rec.Code, wantStatus, rec.Body.String())
	}
	checker.validate(t, req, rec)
	return rec
}

func TestPutGeoFilterMatchesSpec(t *testing.T) {
	checker := newSpecChecker(t)
	srv, router := setupTestServerWithAPIKey(t, configTestAPIKey)

	rec := putGeoFilter(t, checker, router, configTestAPIKey,
		`{"polygon":[[1,1],[1,2],[2,2]],"bufferKm":7.5}`, http.StatusOK)
	var saved api.GeoFilterResponse
	decodeInto(t, rec.Body.Bytes(), &saved)
	if saved.Polygon == nil || len(*saved.Polygon) != 3 || saved.BufferKm != 7.5 || saved.WriteEnabled != nil {
		t.Errorf("PUT response = %+v", saved)
	}
	if gf := srv.getGeoFilter(); gf == nil || len(gf.Polygon) != 3 || gf.BufferKm != 7.5 {
		t.Errorf("filter in force = %+v", gf)
	}

	// The GET now reports what was saved.
	checker.check(t, router, "/api/config/geo-filter")

	// Clearing: empty polygon, and null polygon.
	for _, body := range []string{`{"polygon":[],"bufferKm":0}`, `{"polygon":null,"bufferKm":0}`} {
		rec = putGeoFilter(t, checker, router, configTestAPIKey, body, http.StatusOK)
		var cleared api.GeoFilterResponse
		decodeInto(t, rec.Body.Bytes(), &cleared)
		if cleared.Polygon != nil || cleared.BufferKm != 0 {
			t.Errorf("%s: PUT response = %+v", body, cleared)
		}
		if gf := srv.getGeoFilter(); gf != nil {
			t.Errorf("%s: filter still in force: %+v", body, gf)
		}
	}
}

func TestPutGeoFilterErrorsMatchSpec(t *testing.T) {
	checker := newSpecChecker(t)
	_, router := setupTestServerWithAPIKey(t, configTestAPIKey)

	for name, body := range map[string]string{
		"not json":         `{`,
		"two points":       `{"polygon":[[1,1],[2,2]],"bufferKm":0}`,
		"lat out of range": `{"polygon":[[91,1],[1,2],[2,2]],"bufferKm":0}`,
		"lon out of range": `{"polygon":[[1,181],[1,2],[2,2]],"bufferKm":0}`,
		"negative buffer":  `{"polygon":[[1,1],[1,2],[2,2]],"bufferKm":-1}`,
		"huge buffer":      `{"polygon":[[1,1],[1,2],[2,2]],"bufferKm":20001}`,
		"short point":      `{"polygon":[[1],[1,2],[2,2]],"bufferKm":0}`,
		"long point":       `{"polygon":[[1,1,1],[1,2],[2,2]],"bufferKm":0}`,
	} {
		t.Run(name, func(t *testing.T) {
			putGeoFilter(t, checker, router, configTestAPIKey, body, http.StatusBadRequest)
		})
	}

	t.Run("wrong key", func(t *testing.T) {
		putGeoFilter(t, checker, router, "not-the-key-at-all-12345", `{"polygon":null,"bufferKm":0}`, http.StatusUnauthorized)
	})
	t.Run("no key", func(t *testing.T) {
		putGeoFilter(t, checker, router, "", `{"polygon":null,"bufferKm":0}`, http.StatusUnauthorized)
	})
	t.Run("too many points", func(t *testing.T) {
		var buf bytes.Buffer
		buf.WriteString(`{"polygon":[`)
		for i := 0; i < 1001; i++ {
			if i > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(`[1,1]`)
		}
		buf.WriteString(`],"bufferKm":0}`)
		putGeoFilter(t, checker, router, configTestAPIKey, buf.String(), http.StatusBadRequest)
	})
}

func TestPutGeoFilterDisabledMatchesSpec(t *testing.T) {
	checker := newSpecChecker(t)
	for name, key := range map[string]string{"no key configured": "", "weak key": "test"} {
		t.Run(name, func(t *testing.T) {
			_, router := setupTestServerWithAPIKey(t, key)
			putGeoFilter(t, checker, router, key, `{"polygon":null,"bufferKm":0}`, http.StatusForbidden)
		})
	}
}

func TestProdConfigFixturesMatchSpec(t *testing.T) {
	checkProdFixtures(t, []prodFixture{
		{"config_cache_prod.json", "CacheTTLConfig"},
		{"config_client_prod.json", "ClientConfigResponse"},
		{"config_regions_prod.json", "RegionsResponse"},
		{"config_theme_prod.json", "ThemeResponse"},
		{"config_map_prod.json", "MapConfigResponse"},
		{"config_geo_filter_prod.json", "GeoFilterResponse"},
	})
}
