/**
 * Tests for scripts/check-region-data.js: the integrity check must catch each class of bad
 * region data (and exit nonzero), and must not cry wolf on legitimate data.
 */
'use strict';
const fs = require('fs');
const os = require('os');
const path = require('path');
const assert = require('assert');
const { spawnSync } = require('child_process');
const { checkRegionData } = require('../../scripts/check-region-data.js');

const SCRIPT = path.join(__dirname, '..', '..', 'scripts', 'check-region-data.js');
const FIXTURE = path.join(__dirname, '..', 'fixtures', 'region-layers');

let passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('  ok  ' + name); }
  catch (e) { failed++; console.error('  FAIL ' + name + ': ' + e.message); }
}

function tmpCopy() {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'rd-check-'));
  fs.readdirSync(FIXTURE).forEach((f) => { if (f !== 'README.md') fs.copyFileSync(path.join(FIXTURE, f), path.join(d, f)); });
  return d;
}
const rd = (d, f) => JSON.parse(fs.readFileSync(path.join(d, f), 'utf8'));
const wr = (d, f, o) => fs.writeFileSync(path.join(d, f), typeof o === 'string' ? o : JSON.stringify(o));
const box = (x0, y0, x1, y1) => [[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]];
const metro = (iata, ring) => ({ type: 'Feature', properties: { iata: iata }, geometry: { type: 'Polygon', coordinates: [ring] } });
function mutate(fn) { const d = tmpCopy(); fn(d); const r = checkRegionData(d); fs.rmSync(d, { recursive: true, force: true }); return r; }
const has = (res, re) => res.errors.some((e) => re.test(e));

test('the fixture is clean (no errors, no warnings)', () => {
  const r = checkRegionData(FIXTURE);
  assert.deepStrictEqual(r.errors, []);
  assert.deepStrictEqual(r.warnings, []);
  assert.ok(r.known.length >= 25);
});

test('no layers.json: nothing to check, not an error', () => {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'rd-empty-'));
  const r = checkRegionData(d);
  assert.strictEqual(r.checked, false);
  assert.deepStrictEqual(r.errors, []);
  assert.strictEqual(spawnSync('node', [SCRIPT, '--dir', d]).status, 0);
});

test('malformed GeoJSON file (invalid JSON) is reported with the file name', () => {
  const r = mutate((d) => wr(d, 'metros.geojson', '{"type":"FeatureCollection","features":[{oops'));
  assert.ok(has(r, /metros\.geojson: invalid JSON/), r.errors.join('\n'));
});

test('missing boundary file is reported', () => {
  const r = mutate((d) => fs.unlinkSync(path.join(d, 'metros.geojson')));
  assert.ok(has(r, /metros\.geojson: cannot read file/), r.errors.join('\n'));
});

test('valid JSON that is not polygon GeoJSON is reported', () => {
  const r = mutate((d) => wr(d, 'metros.geojson', { hello: 'world' }));
  assert.ok(has(r, /no Polygon\/MultiPolygon features/), r.errors.join('\n'));
});

test('swapped [lat, lon] coordinates: caught by range when |lon| > 90, otherwise by the known points', () => {
  // San Francisco-ish, swapped: "latitude" -122 is impossible
  const west = mutate((d) => wr(d, 'metros.geojson', { type: 'FeatureCollection', features: [metro('CVG', box(37.5, -122.6, 37.9, -122.2))] }));
  assert.ok(has(west, /latitude -122\.?\d* out of range \(coordinates must be \[lon, lat\]/), west.errors.join('\n'));
  // Cincinnati-ish, swapped: (-84.9) is a legal latitude, so range checks cannot see it; known-points.json does
  const swap = (f) => f.map((q) => ({ type: 'Feature', properties: q.properties, geometry: { type: 'Polygon', coordinates: [q.geometry.coordinates[0].map((c) => [c[1], c[0]])] } }));
  const mid = mutate((d) => { const m = rd(d, 'metros.geojson'); m.features = swap(m.features); wr(d, 'metros.geojson', m); });
  assert.ok(has(mid, /known point "Cincinnati OH": expected "us-midwest oki us-oh cvg" but got "us-midwest oki us-oh"/), mid.errors.join('\n'));
});

test('unclosed ring and too-short ring are caught', () => {
  let r = mutate((d) => wr(d, 'metros.geojson', { type: 'FeatureCollection', features: [metro('CVG', box(-85, 38, -84, 39).slice(0, 4))] }));
  assert.ok(has(r, /not closed/), r.errors.join('\n'));
  r = mutate((d) => wr(d, 'metros.geojson', { type: 'FeatureCollection', features: [metro('CVG', [[0, 0], [1, 1], [0, 0]])] }));
  assert.ok(has(r, /fewer than 4 positions/), r.errors.join('\n'));
});

test('duplicate code inside a layer is caught', () => {
  const r = mutate((d) => { const m = rd(d, 'metros.geojson'); m.features.push(metro('CVG', box(-80, 35, -79, 36))); wr(d, 'metros.geojson', m); });
  assert.ok(has(r, /code "cvg" is produced by more than one polygon/), r.errors.join('\n'));
});

test('overlapping polygons in a layer are caught; allowOverlap opts out', () => {
  const addOverlap = (d, allow) => {
    const m = rd(d, 'metros.geojson'); m.features.push(metro('LUK', box(-84.8, 38.95, -84.3, 39.25))); wr(d, 'metros.geojson', m);
    const l = rd(d, 'layers.json'); l.layers[3].codes.push('luk'); if (allow) l.layers[3].allowOverlap = true; wr(d, 'layers.json', l);
  };
  const r = mutate((d) => addOverlap(d, false));
  assert.ok(has(r, /"cvg" and "luk" overlap/), r.errors.join('\n'));
  const r2 = mutate((d) => addOverlap(d, true));
  assert.deepStrictEqual(r2.errors.filter((e) => /overlap \(up to/.test(e)), [], 'allowOverlap must silence the static overlap check');
  // at runtime the overlap is still visible: known points inside it warn (first match wins)
  assert.ok(r2.errors.some((e) => /known point .*overlapping regions \(cvg, luk\)/.test(e)));
});

test('polygons that only share an edge, or a sliver within tolerance, are NOT overlaps', () => {
  const r = mutate((d) => {
    const m = rd(d, 'metros.geojson');
    m.features.push(metro('LUK', box(-84.15, 38.85, -83.9, 39.40)));        // shares CVG's east edge exactly
    m.features.push(metro('DAY', box(-84.9501, 40.0, -84.5, 40.3)));        // far away
    m.features.push(metro('SLV', box(-84.15, 39.40, -84.1, 39.5)));         // touches at a corner
    wr(d, 'metros.geojson', m);
    const l = rd(d, 'layers.json'); l.layers[3].codes.push('luk', 'day', 'slv'); wr(d, 'layers.json', l);
  });
  assert.deepStrictEqual(r.errors.filter((e) => /overlap \(up to/.test(e)), []);
  const tiny = mutate((d) => {                                              // 0.0002 deg (~20 m) sliver into CVG
    const m = rd(d, 'metros.geojson'); m.features.push(metro('LUK', box(-84.1502, 38.85, -83.9, 39.40))); wr(d, 'metros.geojson', m);
    const l = rd(d, 'layers.json'); l.layers[3].codes.push('luk'); wr(d, 'layers.json', l);
  });
  assert.deepStrictEqual(tiny.errors.filter((e) => /overlap \(up to/.test(e)), []);
});

test('invalid canonical code (uppercase from codeProperty) is caught', () => {
  const r = mutate((d) => { const l = rd(d, 'layers.json'); delete l.layers[2].nameProperty; l.layers[2].codeProperty = 'shapeISO'; wr(d, 'layers.json', l); });
  assert.ok(has(r, /cannot produce a valid canonical code/), r.errors.join('\n'));
});

test('declared canonical code with no polygon (a missing region) is caught', () => {
  const r = mutate((d) => { const s = rd(d, 'states.geojson'); s.features = s.features.filter((f) => f.properties.shapeISO !== 'US-MI'); wr(d, 'states.geojson', s); });
  assert.ok(has(r, /canonical code "us-mi" is declared but no polygon produces it/), r.errors.join('\n'));
});

test('a typo in the data (code outside the declared list) is caught', () => {
  const r = mutate((d) => { const s = rd(d, 'states.geojson'); s.features[0].properties.meshregion = 'OKl'; wr(d, 'states.geojson', s); });
  assert.ok(has(r, /cannot produce a valid canonical code/), r.errors.join('\n'));
});

test('known-point mismatch is caught, naming the point, expected and actual', () => {
  const r = mutate((d) => { const k = rd(d, 'known-points.json'); k[0].expect = 'us-midwest oki us-oh'; wr(d, 'known-points.json', k); });
  assert.ok(has(r, /known point "Cincinnati OH": expected "us-midwest oki us-oh" but got "us-midwest oki us-oh cvg"/), r.errors.join('\n'));
});

test('manifest problems surface as errors: unsupported version, missing version, unsafe path', () => {
  assert.ok(has(mutate((d) => { const l = rd(d, 'layers.json'); l.version = 9; wr(d, 'layers.json', l); }), /newer than this page supports/));
  assert.ok(has(mutate((d) => { const l = rd(d, 'layers.json'); delete l.version; wr(d, 'layers.json', l); }), /set "version"/));
  assert.ok(has(mutate((d) => { const l = rd(d, 'layers.json'); l.layers[3].url = '../metros.geojson'; wr(d, 'layers.json', l); }), /unsafe or invalid file path/));
  assert.ok(has(mutate((d) => wr(d, 'layers.json', '{ nope')), /layers\.json: invalid JSON/));
});

test('the same code in two layers is a warning, and --strict turns it into a failure', () => {
  const d = tmpCopy();
  const l = rd(d, 'layers.json');
  l.layers.push({ id: 'dup', label: 'Dup', url: 'metros.geojson', nameProperty: 'iata' });
  wr(d, 'layers.json', l);
  fs.unlinkSync(path.join(d, 'known-points.json'));   // the duplicate layer legitimately changes the command
  const r = checkRegionData(d);
  assert.ok(r.warnings.some((w) => /code "cvg" is used by both layer "metro" and "dup"/.test(w)), r.warnings.join('\n'));
  assert.strictEqual(spawnSync('node', [SCRIPT, '--dir', d]).status, 0);
  assert.strictEqual(spawnSync('node', [SCRIPT, '--dir', d, '--strict']).status, 1);
  fs.rmSync(d, { recursive: true, force: true });
});

test('CLI exit codes: 0 on the fixture, 1 on broken data; --point prints the repeater block', () => {
  assert.strictEqual(spawnSync('node', [SCRIPT, '--dir', FIXTURE]).status, 0);
  const d = tmpCopy(); wr(d, 'metros.geojson', '{ broken');
  const bad = spawnSync('node', [SCRIPT, '--dir', d]);
  assert.strictEqual(bad.status, 1);
  assert.ok(/ERROR .*invalid JSON/.test(bad.stdout.toString()));
  fs.rmSync(d, { recursive: true, force: true });
  const p = spawnSync('node', [SCRIPT, '--dir', FIXTURE, '--point', '39.1031,-84.5120']);
  assert.ok(/region def us-midwest oki us-oh cvg\n\s+region save\n\s+region allowf \*/.test(p.stdout.toString()), p.stdout.toString());
});

console.log('\n' + passed + ' passed, ' + failed + ' failed');
if (failed) process.exit(1);
