/**
 * Unit tests for public/region-def-core.js (Region Def tool) plus static wiring
 * checks for the page. Pure Node: no jsdom, no network, no browser.
 */
'use strict';
const fs = require('fs');
const path = require('path');
const assert = require('assert');

const ROOT = path.join(__dirname, '..', '..');
const read = (p) => fs.readFileSync(path.join(ROOT, p), 'utf8');
const R = require(path.join(ROOT, 'public', 'region-def-core.js'));

let passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('  ok  ' + name); }
  catch (e) { failed++; console.error('  FAIL ' + name + ': ' + e.message); }
}

// square(lon0, lat0, lon1, lat1) -> closed ring
const sq = (x0, y0, x1, y1) => [[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]];
const poly = (props, ...rings) => ({ type: 'Feature', properties: props, geometry: { type: 'Polygon', coordinates: rings } });
const fc = (...features) => ({ type: 'FeatureCollection', features });

// ---- point in polygon -------------------------------------------------------
test('inside / outside simple square', () => {
  const g = poly({}, sq(0, 0, 10, 10)).geometry;
  assert.strictEqual(R.geometryContains(g, 5, 5), true);
  assert.strictEqual(R.geometryContains(g, 11, 5), false);
  assert.strictEqual(R.geometryContains(g, 5, -0.001), false);
});

test('lon/lat order: [lon, lat] (lat 39, lon -84 is Cincinnati-ish)', () => {
  const g = poly({}, sq(-85, 38, -83, 40)).geometry;
  assert.strictEqual(R.geometryContains(g, -84, 39), true);
  assert.strictEqual(R.geometryContains(g, 39, -84), false); // swapped args must miss
});

test('boundary counts as inside (edge and vertex)', () => {
  const g = poly({}, sq(0, 0, 10, 10)).geometry;
  assert.strictEqual(R.geometryContains(g, 0, 5), true);
  assert.strictEqual(R.geometryContains(g, 10, 10), true);
  assert.strictEqual(R.geometryContains(g, 5, 10), true);
});

test('hole is outside, hole edge is outside, ring between is inside', () => {
  const g = poly({}, sq(0, 0, 10, 10), sq(4, 4, 6, 6)).geometry;
  assert.strictEqual(R.geometryContains(g, 5, 5), false);
  assert.strictEqual(R.geometryContains(g, 4, 5), false);
  assert.strictEqual(R.geometryContains(g, 2, 2), true);
});

test('MultiPolygon: either part matches', () => {
  const g = { type: 'MultiPolygon', coordinates: [[sq(0, 0, 1, 1)], [sq(10, 10, 11, 11)]] };
  assert.strictEqual(R.geometryContains(g, 0.5, 0.5), true);
  assert.strictEqual(R.geometryContains(g, 10.5, 10.5), true);
  assert.strictEqual(R.geometryContains(g, 5, 5), false);
});

test('bbox covers all parts', () => {
  const g = { type: 'MultiPolygon', coordinates: [[sq(0, 0, 1, 1)], [sq(10, 20, 11, 21)]] };
  assert.deepStrictEqual(R.geometryBBox(g), [0, 0, 11, 21]);
});

// ---- normalize --------------------------------------------------------------
test('normalizeGeoJSON: FeatureCollection, Feature, bare geometry; drops points/garbage', () => {
  const p = poly({ name: 'A' }, sq(0, 0, 1, 1));
  assert.strictEqual(R.normalizeGeoJSON(fc(p, { type: 'Feature', properties: {}, geometry: { type: 'Point', coordinates: [0, 0] } })).length, 1);
  assert.strictEqual(R.normalizeGeoJSON(p).length, 1);
  assert.strictEqual(R.normalizeGeoJSON(p.geometry).length, 1);
  assert.strictEqual(R.normalizeGeoJSON(null).length, 0);
  assert.strictEqual(R.normalizeGeoJSON('x').length, 0);
  assert.strictEqual(R.normalizeGeoJSON({ type: 'FeatureCollection' }).length, 0);
});

// ---- slug -------------------------------------------------------------------
test('slug', () => {
  assert.strictEqual(R.slug('  US Midwest '), 'us-midwest');
  assert.strictEqual(R.slug('St. Louis!'), 'st-louis');
  assert.strictEqual(R.slug('CVG'), 'cvg');
  assert.strictEqual(R.slug(null), '');
  assert.strictEqual(R.slug(42), '42');
  assert.strictEqual(R.slug('a_b-c'), 'a_b-c');
});

// ---- grouping / merging -----------------------------------------------------
test('groupBy merges features sharing a property and drops those without it', () => {
  const feats = R.normalizeGeoJSON(fc(
    poly({ shapeName: 'Indiana', mesh: 'oki' }, sq(0, 0, 1, 1)),
    poly({ shapeName: 'Ohio', mesh: 'oki' }, sq(1, 0, 2, 1)),
    poly({ shapeName: 'Texas', mesh: 'tx' }, sq(5, 5, 6, 6)),
    poly({ shapeName: 'Nowhere' }, sq(8, 8, 9, 9))
  ));
  const g = R.groupFeatures(feats, 'mesh');
  assert.deepStrictEqual(g.map(f => f.properties.__name), ['oki', 'tx']);
  assert.strictEqual(g[0].geometry.type, 'MultiPolygon');
  assert.strictEqual(g[0].geometry.coordinates.length, 2);
});

test('buildLayer: per-file name merges the whole file into one region', () => {
  const layer = R.buildLayer({ id: 'macro', label: 'Macro', url: ['a', 'b'] }, [
    { name: 'US-Midwest', data: fc(poly({ x: 1 }, sq(0, 0, 1, 1)), poly({ x: 2 }, sq(1, 0, 2, 1))) },
    { name: 'US-Southeast', data: fc(poly({}, sq(0, -5, 2, -1))) }
  ]);
  assert.strictEqual(layer.features.length, 2);
  assert.strictEqual(R.slug(layer.features[0].name), 'us-midwest');
  assert.strictEqual(R.matchLayer(layer, 0.5, 0.5).matches[0].code, 'us-midwest');
  assert.strictEqual(R.matchLayer(layer, 0.5, 1.5).matches[0].code, 'us-midwest');
  assert.strictEqual(R.matchLayer(layer, -3, 1).matches[0].code, 'us-southeast');
});

test('nameProperty array falls back to the first non-empty property', () => {
  const layer = R.buildLayer({ id: 's', nameProperty: ['shapeName', 'name'] }, [
    { data: fc(poly({ shapeName: '', name: 'Ohio' }, sq(0, 0, 1, 1))) }
  ]);
  assert.strictEqual(R.matchLayer(layer, 0.5, 0.5).matches[0].code, 'ohio');
});

// ---- region def -------------------------------------------------------------
function layers() {
  return [
    R.buildLayer({ id: 'macro', label: 'Macro region' }, [{ name: 'US-Midwest', data: fc(poly({}, sq(0, 0, 20, 20))) }]),
    R.buildLayer({ id: 'mesh', label: 'Mesh region', nameProperty: 'name' }, [{ data: fc(poly({ name: 'OKI' }, sq(0, 0, 10, 10))) }]),
    R.buildLayer({ id: 'state', label: 'State', nameProperty: 'name' }, [{ data: fc(poly({ name: 'Ohio' }, sq(0, 0, 5, 10)), poly({ name: 'Indiana' }, sq(5, 0, 10, 10))) }]),
    R.buildLayer({ id: 'metro', label: 'Metro', nameProperty: 'iata' }, [{ data: fc(poly({ iata: 'CVG' }, sq(1, 1, 3, 3)), poly({ iata: 'DAY' }, sq(1, 6, 3, 8))) }])
  ];
}

test('regionDef: top-to-bottom order, one token per layer', () => {
  const r = R.buildRegionDef(layers(), 2, 2);
  assert.strictEqual(r.value, 'us-midwest oki ohio cvg');
  assert.deepStrictEqual(r.warnings, []);
});

test('regionDef: sibling metros do not collide; no metro match leaves the token out', () => {
  assert.strictEqual(R.buildRegionDef(layers(), 7, 2).value, 'us-midwest oki ohio day');
  assert.strictEqual(R.buildRegionDef(layers(), 4, 4.5).value, 'us-midwest oki ohio');
  assert.strictEqual(R.buildRegionDef(layers(), 7, 8).value, 'us-midwest oki indiana');
});

test('regionDef: outside everything is empty', () => {
  const r = R.buildRegionDef(layers(), 50, 50);
  assert.strictEqual(r.value, '');
  assert.strictEqual(R.repeaterCommands(r.value), '');
});

test('regionDef: overlap uses first match and warns', () => {
  const L = [R.buildLayer({ id: 'metro', label: 'Metro', nameProperty: 'iata' }, [
    { data: fc(poly({ iata: 'AAA' }, sq(0, 0, 5, 5)), poly({ iata: 'BBB' }, sq(3, 3, 8, 8))) }])];
  const r = R.buildRegionDef(L, 4, 4);
  assert.strictEqual(r.value, 'aaa');
  assert.strictEqual(r.warnings.length, 1);
  assert.ok(/aaa, bbb/.test(r.warnings[0]));
});

test('regionDef: unnamed polygon never produces a token, warns instead', () => {
  const L = [R.buildLayer({ id: 'm', label: 'Metro', nameProperty: 'iata' }, [{ data: fc(poly({}, sq(0, 0, 5, 5))) }])];
  const r = R.buildRegionDef(L, 2, 2);
  assert.strictEqual(r.value, '');
  assert.strictEqual(r.warnings.length, 1);
});

test('repeaterCommands: exact three-line block', () => {
  assert.strictEqual(R.repeaterCommands('us-midwest oki ohio cvg'),
    'region def us-midwest oki ohio cvg\nregion save\nregion allowf *');
});

// ---- input parsing ----------------------------------------------------------
test('parseLatLon', () => {
  assert.deepStrictEqual(R.parseLatLon('39.1, -84.5'), { lat: 39.1, lon: -84.5 });
  assert.deepStrictEqual(R.parseLatLon(' 39.1 -84.5 '), { lat: 39.1, lon: -84.5 });
  assert.strictEqual(R.parseLatLon('91, 0'), null);
  assert.strictEqual(R.parseLatLon('0, 181'), null);
  assert.strictEqual(R.parseLatLon('123 Main St'), null);
  assert.strictEqual(R.parseLatLon('1,2,3'), null);
  assert.strictEqual(R.parseLatLon(null), null);
});

test('featureColorOverride only applies with colorBy=feature and a sane value', () => {
  const f = { properties: { color: '#db2777' } };
  assert.strictEqual(R.featureColorOverride({ colorBy: 'feature' }, f), '#db2777');
  assert.strictEqual(R.featureColorOverride({}, f), null);
  assert.strictEqual(R.featureColorOverride({ colorBy: 'feature' }, { properties: { color: 'red;x:y' } }), null);
});

// ---- static wiring ----------------------------------------------------------
test('wiring: index.html loads css + core before page script', () => {
  const html = read('public/index.html');
  assert.ok(/region-def\.css\?v=__BUST__/.test(html), 'css link');
  const a = html.indexOf('region-def-core.js?v=__BUST__');
  const b = html.indexOf('region-def.js?v=__BUST__');
  assert.ok(a > 0 && b > a, 'core must load before page');
});

test('wiring: app.js routes #/tools/region-def and highlights Tools nav', () => {
  const app = read('public/app.js');
  assert.ok(/#\/tools\/region-def/.test(app), 'landing card');
  assert.ok(/routeParam === 'region-def'/.test(app), 'sub-route');
  assert.ok(/basePage === 'region-def'/.test(app), 'nav active');
});

test('wiring: page registers and only the static skeleton uses innerHTML', () => {
  const js = read('public/region-def.js');
  assert.ok(/registerPage\('region-def'/.test(js));
  const hits = js.match(/innerHTML/g) || [];
  assert.strictEqual(hits.length, 1, 'exactly one innerHTML (static skeleton); all dynamic data via textContent');
  assert.ok(/app\.innerHTML\s*=\s*\n?\s*'<div class="rd-page">/.test(js));
});

test('theming: region-def.css has no hex colours outside :root / [data-theme]', () => {
  const css = read('public/region-def.css').replace(/\/\*[^]*?\*\//g, '');
  const blocks = css.split('}');
  blocks.forEach((b) => {
    const sel = b.split('{')[0];
    if (/:root|\[data-theme/.test(sel)) return;
    assert.ok(!/#[0-9a-fA-F]{3,8}\b/.test(b), 'hex colour in rule: ' + sel.trim());
  });
});

test('no emoji in new frontend files (#1648 policy)', () => {
  const re = /[\u{1F300}-\u{1FAFF}\u{2600}-\u{27BF}]/u;
  ['public/region-def.js', 'public/region-def-core.js', 'public/region-def.css'].forEach((f) => {
    assert.ok(!re.test(read(f)), f);
  });
});

console.log('\n' + passed + ' passed, ' + failed + ' failed');
if (failed) process.exit(1);