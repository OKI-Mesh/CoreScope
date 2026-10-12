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

test('repeaterCommands: exact three-line block, region save last (allowf does not persist on its own)', () => {
  assert.strictEqual(R.repeaterCommands('us-midwest oki ohio cvg'),
    'region def us-midwest oki ohio cvg\nregion allowf *\nregion save');
});

// ---- firmware limits (MeshCore RegionMap/CommonCLI): name[31], 160-byte CLI line ----------------
const code30 = 'a'.repeat(30), code31 = 'a'.repeat(31);

test('CODE_RE: 30 characters is the longest accepted code; 31 and 64 are rejected', () => {
  assert.strictEqual(R.FW_MAX_NAME_LEN, 30);
  assert.ok(R.CODE_RE.test(code30));
  assert.ok(!R.CODE_RE.test(code31));
  assert.ok(!R.CODE_RE.test('a'.repeat(64)));
});

test('CODE_RE: still rejects uppercase, spaces, separators and the firmware-special $ and #', () => {
  ['US-OH', 'a b', 'a,b', 'a|b', '$priv', '#oki', '-lead', '', 'a.b'].forEach((c) => assert.ok(!R.CODE_RE.test(c), c));
});

test('featureCode: an over-long explicit or slugged code is invalid, never truncated', () => {
  const explicit = R.featureCode({ properties: { __code: code31 } }, {});
  assert.deepStrictEqual(explicit, { code: null, reason: 'invalid' });
  const slugged = R.featureCode({ properties: { name: code31 } }, { nameProperty: 'name' });
  assert.deepStrictEqual(slugged, { code: null, reason: 'invalid' });
  assert.strictEqual(R.featureCode({ properties: { __code: code30 } }, {}).code, code30);
});

test('validateManifest: a 31-character canonical code or ignored code is an error', () => {
  const m = (extra) => ({ version: 1, layers: [Object.assign({ id: 'a', url: 'a.geojson' }, extra)] });
  assert.ok(R.validateManifest(m({ codes: [code31] })).errors.length > 0);
  assert.ok(R.validateManifest(m({ ignoreCodes: [code31] })).errors.length > 0);
  assert.strictEqual(R.validateManifest(m({ codes: [code30] })).errors.length, 0);
});

test('commandProblems: the 158-character line limit is exact', () => {
  // 'region def ' is 11 characters; 147 more fill the line to exactly 158.
  const fit = [code30, code30, code30, code30, 'b'.repeat(147 - 4 * 30 - 4)].join(' ');
  assert.strictEqual(('region def ' + fit).length, 158);
  assert.deepStrictEqual(R.commandProblems(fit), []);
  assert.ok(R.repeaterCommands(fit).length > 0);
  const over = fit + 'b';
  assert.strictEqual(('region def ' + over).length, 159);
  assert.ok(R.commandProblems(over).some((p) => /159 characters/.test(p)));
  assert.strictEqual(R.repeaterCommands(over), '');
});

test('commandProblems: more regions than a repeater can hold, and invalid tokens, are rejected', () => {
  const many = Array.from({ length: 33 }, (_, i) => 'r' + i).join(' ');
  assert.ok(R.commandProblems(many).some((p) => /33 regions/.test(p)));
  assert.ok(R.commandProblems('oki US-OH').some((p) => /"US-OH"/.test(p)));
  assert.ok(R.commandProblems(code31).length > 0);
  assert.strictEqual(R.repeaterCommands(code31), '');
  assert.deepStrictEqual(R.commandProblems(''), []);
});

test('buildRegionDef: a command over the line limit is withheld with the reason, not returned as valid', () => {
  const names = ['a', 'b', 'c', 'd', 'e', 'f'].map((ch) => ch.repeat(30));
  const L = names.map((n, i) => R.buildLayer({ id: 'l' + i, label: 'L' + i, nameProperty: 'name' }, [{ data: fc(poly({ name: n }, sq(0, 0, 10, 10))) }]));
  const r = R.buildRegionDef(L, 5, 5);
  assert.strictEqual(r.tokens.length, 6);
  assert.strictEqual(r.value, '');
  assert.ok(r.blocked.some((p) => /characters; the repeater accepts at most 158/.test(p)), r.blocked.join('\n'));
  assert.strictEqual(R.repeaterCommands(r.value), '');
});

test('buildRegionDef: a normal result has no blocked reasons', () => {
  assert.deepStrictEqual(R.buildRegionDef(layers(), 2, 2).blocked, []);
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

// ---- layer path allowlist / sizing -------------------------------------------
test('isSafeLayerPath: accepts plain relative paths, rejects everything else', () => {
  ['a.geojson', 'geojson/states.geojson', 'x/y/z-1_2.json'].forEach((p) => assert.strictEqual(R.isSafeLayerPath(p), true, p));
  ['../x.json', 'a/../b.json', 'a/./b.json', '/a.json', 'a//b.json', 'http://x/a.json', '//x/a.json',
   'a.json?x=1', 'a.json#f', 'a%2e%2e/b.json', 'a\\b.json', '.hidden.json', 'a.txt', '', null, 42, './a.json',
   'a/b/' + 'x'.repeat(200) + '.json'].forEach((p) => assert.strictEqual(R.isSafeLayerPath(p), false, String(p)));
});

test('buildLayer reports vertex count and unnamed polygons', () => {
  const L = R.buildLayer({ id: 'm', nameProperty: 'iata' }, [{ data: fc(poly({ iata: 'A' }, sq(0, 0, 1, 1)), poly({}, sq(2, 2, 3, 3))) }]);
  assert.strictEqual(L.vertices, 10);
  assert.strictEqual(L.unnamed, 1);
  assert.strictEqual(R.countVertices({ type: 'MultiPolygon', coordinates: [[sq(0, 0, 1, 1), sq(0.2, 0.2, 0.4, 0.4)]] }), 10);
});

test('buildRegionDef exposes per-layer unnamed hits so callers can withhold the command', () => {
  const L = [R.buildLayer({ id: 'm', label: 'Metro', nameProperty: 'iata' }, [{ data: fc(poly({}, sq(0, 0, 5, 5))) }])];
  const r = R.buildRegionDef(L, 2, 2);
  assert.strictEqual(r.perLayer[0].unnamed, 1);
});

// ---- canonical codes ------------------------------------------------------------
test('codeProperty is used verbatim and validated, never slugged', () => {
  const L = R.buildLayer({ id: 's', nameProperty: 'shapeName', codeProperty: 'abbr' }, [{ data: fc(
    poly({ shapeName: 'Ohio', abbr: 'oh' }, sq(0, 0, 1, 1)),
    poly({ shapeName: 'Indiana', abbr: 'IN' }, sq(2, 2, 3, 3)),       // uppercase: invalid, not lowercased for you
    poly({ shapeName: 'Kentucky', abbr: 'k y' }, sq(4, 4, 5, 5)),     // space: invalid
    poly({ shapeName: 'Michigan' }, sq(6, 6, 7, 7))) }]);            // no abbr: falls back to slug(name)
  assert.strictEqual(R.matchLayer(L, 0.5, 0.5).matches[0].code, 'oh');
  assert.strictEqual(R.matchLayer(L, 2.5, 2.5).matches.length, 0);
  assert.strictEqual(R.matchLayer(L, 4.5, 4.5).matches.length, 0);
  assert.strictEqual(R.matchLayer(L, 6.5, 6.5).matches[0].code, 'michigan');
  assert.strictEqual(L.codeProblems.invalid, 2);
});

test('codes allowlist: a code outside the canonical list is unusable', () => {
  const L = R.buildLayer({ id: 'mesh', nameProperty: 'name', codes: ['oki', 'swoh'] }, [{ data: fc(
    poly({ name: 'OKI' }, sq(0, 0, 1, 1)), poly({ name: 'Typo' }, sq(2, 2, 3, 3))) }]);
  assert.strictEqual(R.matchLayer(L, 0.5, 0.5).matches[0].code, 'oki');
  assert.strictEqual(R.matchLayer(L, 2.5, 2.5).matches.length, 0);
  assert.strictEqual(L.codeProblems['not-allowed'], 1);
  const r = R.buildRegionDef([L], 2.5, 2.5);
  assert.strictEqual(r.perLayer[0].unnamed, 1);
});

test('file-level code and groupBy+codeProperty', () => {
  const macro = R.buildLayer({ id: 'macro' }, [{ name: 'US Midwest', code: 'us-midwest', data: fc(poly({}, sq(0, 0, 1, 1))) }]);
  assert.strictEqual(R.matchLayer(macro, 0.5, 0.5).matches[0].code, 'us-midwest');
  assert.strictEqual(R.matchLayer(macro, 0.5, 0.5).matches[0].name, 'US Midwest');
  const badFile = R.buildLayer({ id: 'macro' }, [{ name: 'X', code: 'Bad Code', data: fc(poly({}, sq(0, 0, 1, 1))) }]);
  assert.strictEqual(R.matchLayer(badFile, 0.5, 0.5).matches.length, 0);
  const mesh = R.buildLayer({ id: 'mesh', groupBy: 'grp', codeProperty: 'abbr' }, [{ data: fc(
    poly({ grp: 'Ohio-Kentucky-Indiana', abbr: 'oki' }, sq(0, 0, 1, 1)),
    poly({ grp: 'Ohio-Kentucky-Indiana', abbr: 'oki' }, sq(1, 0, 2, 1)))}]);
  assert.strictEqual(mesh.features.length, 1);
  const m = R.matchLayer(mesh, 0.5, 1.5).matches[0];
  assert.strictEqual(m.code, 'oki');                       // from codeProperty, not the group value
  assert.strictEqual(m.name, 'Ohio-Kentucky-Indiana');
});

test('groupBy merges one region across several files of the same layer', () => {
  const west = fc(poly({ mesh: 'oki' }, sq(0, 0, 1, 1)), poly({ mesh: 'swoh' }, sq(5, 5, 6, 6)));
  const east = fc(poly({ mesh: 'oki' }, sq(1, 0, 2, 1)));
  const L = R.buildLayer({ id: 'mesh', groupBy: 'mesh' }, [{ data: west }, { data: east }]);
  assert.strictEqual(L.features.length, 2, 'one polygon per mesh region, not one per file');
  assert.strictEqual(L.features.filter((e) => e.code === 'oki').length, 1);
  assert.strictEqual(R.matchLayer(L, 0.5, 0.5).matches[0].code, 'oki');   // in the west file
  assert.strictEqual(R.matchLayer(L, 0.5, 1.5).matches[0].code, 'oki');   // in the east file
  assert.strictEqual(R.matchLayer(L, 5.5, 5.5).matches[0].code, 'swoh');
});

test('ignoreCodes drops deliberately excluded regions: not drawn, not matched, not a problem', () => {
  const data = fc(poly({ iso: 'US-OH' }, sq(0, 0, 1, 1)), poly({ iso: 'US-PA' }, sq(1, 0, 2, 1)), poly({ iso: 'US-WV' }, sq(2, 0, 3, 1)));
  const def = { id: 's', nameProperty: 'iso', codes: ['us-oh'] };
  const strict = R.buildLayer(def, [{ data }]);
  assert.strictEqual(strict.unnamed, 2, 'without ignoreCodes the extra states are reported');
  const L = R.buildLayer(Object.assign({}, def, { ignoreCodes: ['us-pa', 'us-wv'] }), [{ data }]);
  assert.strictEqual(L.unnamed, 0);
  assert.strictEqual(L.features.length, 1);
  assert.strictEqual(R.matchLayer(L, 0.5, 0.5).matches[0].code, 'us-oh');
  assert.strictEqual(R.matchLayer(L, 0.5, 1.5).matches.length, 0);
  assert.strictEqual(R.matchLayer(L, 0.5, 2.5).matches.length, 0);
  // an ignored code does not need to be in `codes`, and a code in both is a manifest error
  const bad = R.validateManifest({ version: 1, layers: [{ id: 'a', url: 'a.geojson', codes: ['us-oh'], ignoreCodes: ['us-oh'] }] });
  assert.ok(bad.errors.some((e) => /both "codes" and "ignoreCodes"/.test(e)), bad.errors.join('\n'));
  const bad2 = R.validateManifest({ version: 1, layers: [{ id: 'a', url: 'a.geojson', ignoreCodes: ['US-PA'] }] });
  assert.ok(bad2.errors.some((e) => /ignored code/.test(e)));
  const ok = R.validateManifest({ version: 1, layers: [{ id: 'a', url: 'a.geojson', codes: ['us-oh'], ignoreCodes: ['us-pa'] }] });
  assert.deepStrictEqual(ok.errors, []);
  assert.ok(!ok.warnings.some((w) => /unknown/i.test(w)), ok.warnings.join('\n'));
});

test('coverage:"full" turns a miss into a data gap only when another layer matched', () => {
  const mk = (cov) => [
    R.buildLayer({ id: 'state', label: 'State', nameProperty: 'name' }, [{ data: fc(poly({ name: 'Ohio' }, sq(0, 0, 10, 10))) }]),
    R.buildLayer({ id: 'mesh', label: 'Mesh', nameProperty: 'name', coverage: cov }, [{ data: fc(poly({ name: 'OKI' }, sq(0, 0, 5, 10))) }])];
  const gap = R.buildRegionDef(mk('full'), 5, 8);        // in Ohio, outside OKI
  assert.strictEqual(gap.perLayer[1].gap, true);
  assert.ok(gap.warnings.some((w) => /data gap/.test(w)));
  assert.strictEqual(R.buildRegionDef(mk('partial'), 5, 8).perLayer[1].gap, false);   // valid exclusion
  assert.strictEqual(R.buildRegionDef(mk('full'), 50, 50).perLayer[1].gap, false);    // outside everything
  assert.strictEqual(R.buildRegionDef(mk('full'), 2, 2).perLayer[1].gap, false);      // covered
});

test('duplicate code across layers is flagged', () => {
  const a = R.buildLayer({ id: 'a', label: 'A', nameProperty: 'n' }, [{ data: fc(poly({ n: 'oki' }, sq(0, 0, 5, 5))) }]);
  const b = R.buildLayer({ id: 'b', label: 'B', nameProperty: 'n' }, [{ data: fc(poly({ n: 'oki' }, sq(0, 0, 5, 5))) }]);
  assert.ok(R.buildRegionDef([a, b], 2, 2).warnings.some((w) => /repeat/.test(w)));
});

// ---- manifest validation ----------------------------------------------------------
const goodManifest = () => ({ version: 1, layers: [
  { id: 'macro', label: 'Macro', url: [{ url: 'a.geojson', name: 'US-Midwest', code: 'us-midwest' }], codes: ['us-midwest'] },
  { id: 'metro', url: 'metros.geojson', nameProperty: 'iata', optional: true }] });

test('validateManifest: good manifest has no errors or warnings', () => {
  const v = R.validateManifest(goodManifest());
  assert.deepStrictEqual(v.errors, []);
  assert.deepStrictEqual(v.warnings, []);
});

test('validateManifest: version handling', () => {
  const m = goodManifest(); delete m.version;
  const v = R.validateManifest(m);
  assert.strictEqual(v.errors.length, 0);
  assert.ok(v.warnings.some((w) => /no "version"/.test(w)));
  m.version = 2; assert.ok(R.validateManifest(m).errors.some((e) => /newer than this page/.test(e)));
  m.version = '1'; assert.ok(R.validateManifest(m).errors.length > 0);
  m.version = 0; assert.ok(R.validateManifest(m).errors.length > 0);
});

test('validateManifest: rejects structural problems', () => {
  const bad = (mut) => { const m = goodManifest(); mut(m); return R.validateManifest(m).errors; };
  assert.ok(bad((m) => { m.layers = []; }).length);
  assert.ok(bad((m) => { m.layers[1].id = 'macro'; }).some((e) => /duplicate id/.test(e)));
  assert.ok(bad((m) => { m.layers[0].id = '1bad'; }).length);
  assert.ok(bad((m) => { m.layers[0].url = '../x.json'; }).some((e) => /unsafe/.test(e)));
  assert.ok(bad((m) => { delete m.layers[0].url; }).some((e) => /required/.test(e)));
  assert.ok(bad((m) => { m.layers[0].codes = ['OKI']; }).some((e) => /invalid/.test(e)));
  assert.ok(bad((m) => { m.layers[0].codes = ['oki', 'oki']; }).some((e) => /twice/.test(e)));
  assert.ok(bad((m) => { m.layers[0].coverage = 'all'; }).some((e) => /coverage/.test(e)));
  assert.ok(bad((m) => { m.layers[0].optional = 'yes'; }).some((e) => /optional/.test(e)));
  assert.ok(bad((m) => { m.layers[0].url[0].code = 'Bad'; }).some((e) => /not a valid code/.test(e)));
  assert.ok(bad((m) => { m.layers = new Array(17).fill(0).map((_, i) => ({ id: 'l' + i, url: 'a.json' })); }).some((e) => /At most/.test(e)));
  assert.ok(R.validateManifest(null).errors.length && R.validateManifest([]).errors.length);
});

test('validateManifest: unknown fields are warned about (catches typos like "optionnal")', () => {
  const m = goodManifest(); m.layers[1].optionnal = true;
  const v = R.validateManifest(m);
  assert.deepStrictEqual(v.errors, []);
  assert.ok(v.warnings.some((w) => /optionnal/.test(w)));
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

test('wiring: loader dedupes requests, uses the path allowlist, withholds incomplete commands', () => {
  const js = read('public/region-def.js');
  assert.ok(/cache\[url\]\s*\|\|\s*\(cache\[url\]\s*=/.test(js), 'per-URL request cache');
  assert.ok(/RegionDef\.isSafeLayerPath/.test(js), 'allowlist used');
  assert.ok(/contains no valid polygons/.test(js), 'empty layers must count as failed');
  assert.ok(/Command withheld/.test(js) && /optional === true/.test(js), 'withhold / optional logic');
  assert.ok(/preferCanvas:\s*true/.test(js), 'canvas renderer');
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
