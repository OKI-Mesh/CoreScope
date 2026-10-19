/**
 * Unit tests for the "regions within reach of a point" part of public/region-def-core.js:
 * distanceToGeometry, matchNear, and the tree-shaped `region def` command built from the layer keys
 * root / parentProperty / nearMiles. Pure Node: no jsdom, no network, no browser.
 *
 * `region def` semantics (MeshCore docs/cli_commands.md): a plain token is created as a child of the
 * cursor and moves the cursor to itself; `name|jump` creates name, then moves the cursor to `jump`.
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
const near = (actual, expected, tol, what) =>
  assert.ok(Math.abs(actual - expected) <= tol, (what || 'value') + ': expected ' + expected + ' +/- ' + tol + ' but got ' + actual);

const sq = (x0, y0, x1, y1) => [[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]];
const poly = (props, ...rings) => ({ type: 'Feature', properties: props, geometry: { type: 'Polygon', coordinates: rings } });
const fc = (...features) => ({ type: 'FeatureCollection', features });
const MI = R.MILE_M;
const M_PER_DEG = 111195;
const kx = (lat) => M_PER_DEG * Math.cos(lat * Math.PI / 180);

// ---- distanceToGeometry ---------------------------------------------------------------------
test('distance is 0 inside and on the edge', () => {
  const g = poly({}, sq(0, 0, 1, 1)).geometry;
  assert.strictEqual(R.distanceToGeometry(g, 0.5, 0.5), 0);
  assert.strictEqual(R.distanceToGeometry(g, 0, 0.5), 0);
  assert.strictEqual(R.distanceToGeometry(g, 1, 1), 0);
});

test('distance straight north of an edge is degrees of latitude', () => {
  const g = poly({}, sq(0, 0, 1, 1)).geometry;
  near(R.distanceToGeometry(g, 0.5, 1.1), 0.1 * M_PER_DEG, 1, 'north');
});

test('distance east of an edge shrinks with cos(latitude)', () => {
  const g = poly({}, sq(0, 39, 1, 41)).geometry;
  near(R.distanceToGeometry(g, 1.2, 40), 0.2 * kx(40), 1, 'east at 40N');
});

test('distance off a corner is the straight line to the corner', () => {
  const g = poly({}, sq(0, 40, 1, 41)).geometry;
  const lat = 41.1;
  const expected = Math.sqrt(Math.pow(0.1 * kx(lat), 2) + Math.pow(0.1 * M_PER_DEG, 2));
  near(R.distanceToGeometry(g, 1.1, lat), expected, 1, 'corner');
});

test('a point inside a hole is measured to the hole edge', () => {
  const g = poly({}, sq(0, 0, 10, 10), sq(4, 4, 6, 6)).geometry;
  near(R.distanceToGeometry(g, 5, 5), 1 * kx(5), 5, 'hole'); // left/right edges are the nearest (cos 5 deg < 1)
});

test('MultiPolygon takes the nearest part', () => {
  const g = { type: 'MultiPolygon', coordinates: [[sq(0, 0, 1, 1)], [sq(10, 0, 11, 1)]] };
  near(R.distanceToGeometry(g, 9.9, 0.5), 0.1 * kx(0.5), 1, 'nearest part');
});

test('one mile is 1609.344 m', () => { assert.strictEqual(R.MILE_M, 1609.344); });

// ---- matchNear ------------------------------------------------------------------------------
function plainLayer(features, def) {
  return R.buildLayer(Object.assign({ id: 'l', label: 'L', nameProperty: 'name' }, def || {}), [{ data: fc(...features) }]);
}

test('matchNear: containing first, then nearest first; radius is inclusive', () => {
  const layer = plainLayer([
    poly({ name: 'far' }, sq(0, 0.30, 1, 0.40)),
    poly({ name: 'close' }, sq(0, 0.12, 1, 0.20)),
    poly({ name: 'in' }, sq(0, -1, 1, 0.1))
  ]);
  const hit = R.matchNear(layer, 0.05, 0.5, 50 * MI);
  assert.deepStrictEqual(hit.map((m) => m.code), ['in', 'close', 'far']);
  assert.strictEqual(hit[0].distance, 0);
  const dClose = hit[1].distance;
  assert.deepStrictEqual(R.matchNear(layer, 0.05, 0.5, dClose).map((m) => m.code).slice(0, 2), ['in', 'close']); // exactly at the radius
  assert.ok(R.matchNear(layer, 0.05, 0.5, dClose - 1).every((m) => m.code !== 'close'));
});

test('matchNear: regions with no usable code are left out', () => {
  const layer = plainLayer([poly({ name: 'ok' }, sq(0, 0, 1, 1)), poly({}, sq(0, 0, 1, 1))]);
  assert.deepStrictEqual(R.matchNear(layer, 0.5, 0.5, 10 * MI).map((m) => m.code), ['ok']);
});

test('matchNear: a region outside the widened bounding box is never measured', () => {
  const layer = plainLayer([poly({ name: 'x' }, sq(5, 5, 6, 6))]);
  assert.deepStrictEqual(R.matchNear(layer, 0, 0, 10 * MI), []);
});

// ---- the command: geography ------------------------------------------------------------------
// Ohio (north), Kentucky (south of Ohio), Indiana (west of both). 1 deg latitude = 69.1 miles.
//   Ohio     lon -85..-82, lat 39.1..41       Kentucky lon -85..-82, lat 37..39.1
//   Indiana  lon -88..-85, lat 37..41
//   cvg      lon -84.9..-84.0, lat 38.8..39.4 (spans the Ohio/Kentucky line)
//   day      lon -84.6..-83.8, lat 39.4..40.0 (touches cvg on its north edge)
const OH = sq(-85, 39.1, -82, 41), KY = sq(-85, 37, -82, 39.1), IN = sq(-88, 37, -85, 41);
const CVG = sq(-84.9, 38.8, -84.0, 39.4), DAY = sq(-84.6, 39.4, -83.8, 40.0);

function layers(opts) {
  const o = Object.assign({ tree: true }, opts || {});
  const macro = R.buildLayer(Object.assign({ id: 'macro', label: 'Macro' }, o.tree ? { root: true } : {}), [
    { name: 'US Midwest', code: 'us-midwest', data: fc(poly({}, OH), poly({}, IN)) },
    { name: 'US Southeast', code: 'us-southeast', data: fc(poly({}, KY)) }
  ]);
  const mesh = R.buildLayer(Object.assign({ id: 'mesh', label: 'Mesh' }, o.tree ? { root: true } : {}), [
    { name: 'oki', code: 'oki', data: fc(poly({}, OH), poly({}, IN), poly({}, KY)) }
  ]);
  const state = R.buildLayer(Object.assign({ id: 'state', label: 'State', nameProperty: 'shapeISO' },
    o.tree ? { parentProperty: 'region', nearMiles: 10 } : {}), [{
    data: fc(poly({ shapeISO: 'US-OH', region: o.noRegion ? undefined : 'us-midwest' }, OH),
      poly({ shapeISO: 'US-IN', region: 'us-midwest' }, IN),
      poly({ shapeISO: 'US-KY', region: o.badParent || 'us-southeast' }, KY))
  }]);
  const metro = R.buildLayer(Object.assign({ id: 'metro', label: 'Metro', nameProperty: 'iata' }, o.tree ? { nearMiles: 10 } : {}), [
    { data: fc(poly({ iata: 'CVG' }, CVG), poly({ iata: 'DAY' }, DAY)) }
  ]);
  return o.tree ? [mesh, macro, state, metro] : [macro, mesh, state, metro];
}
const cmd = (ls, lat, lon) => R.buildRegionDef(ls, lat, lon);

// ---- a near region hangs under the region it overlaps ----------------------------------------------
// IND: wholly in Indiana, just west of the Ohio line. SPAN: straddles the Ohio/Indiana line.
const IND = sq(-86.0, 39.5, -85.1, 40.0), SPAN = sq(-85.3, 40.2, -84.7, 40.7);
function layersWithIndiana() {
  const ls = layers();
  ls[3] = R.buildLayer({ id: 'metro', label: 'Metro', nameProperty: 'iata', nearMiles: 10 }, [
    { data: fc(poly({ iata: 'CVG' }, CVG), poly({ iata: 'IND' }, IND), poly({ iata: 'SPAN' }, SPAN)) }]);
  return ls;
}

test('geometriesTouch: overlap, containment, crossing edges, and apart', () => {
  const g = (r) => poly({}, r).geometry;
  assert.strictEqual(R.geometriesTouch(g(sq(0, 0, 2, 2)), g(sq(1, 1, 3, 3))), true);
  assert.strictEqual(R.geometriesTouch(g(sq(0, 0, 4, 4)), g(sq(1, 1, 2, 2))), true);
  assert.strictEqual(R.geometriesTouch(g(sq(0, 1, 4, 2)), g(sq(1, 0, 2, 4))), true);  // plus sign: only edges cross
  assert.strictEqual(R.geometriesTouch(g(sq(0, 0, 1, 1)), g(sq(2, 2, 3, 3))), false);
});

test('a neighbouring Indiana-only metro goes under Indiana, not under the repeater\'s Ohio', () => {
  const r = cmd(layersWithIndiana(), 39.75, -84.95);
  assert.strictEqual(r.value, 'oki|* us-midwest us-oh|us-midwest us-in ind');
  assert.deepStrictEqual(r.warnings, []);
});

test('the metro containing the point stays under the repeater\'s own state, even if it spans states', () => {
  assert.strictEqual(cmd(layersWithIndiana(), 39.39, -84.3).value, 'oki|* us-midwest us-oh cvg');
  assert.strictEqual(cmd(layersWithIndiana(), 38.9, -84.3).value, 'oki|* us-southeast us-ky cvg');
});

test('a neighbouring metro that overlaps the repeater\'s state stays under that state', () => {
  // Ohio repeater 5 miles south of SPAN, which straddles Ohio and Indiana: under Ohio (home), not Indiana.
  const r = cmd(layersWithIndiana(), 40.12, -84.9);
  assert.strictEqual(r.value, 'oki|* us-midwest us-oh span|us-midwest us-in');
});

test('deep inside Ohio: oki at the root, the macro region beside it, the state under the macro', () => {
  const r = cmd(layers(), 40.5, -83.2);
  assert.strictEqual(r.value, 'oki|* us-midwest us-oh');
  assert.deepStrictEqual(r.warnings, []);
  assert.deepStrictEqual(r.blocked, []);
});

test('Ohio side of the Kentucky line, inside cvg: Kentucky and its macro join as a second branch', () => {
  const r = cmd(layers(), 39.2, -84.5); // Kentucky is 0.1 deg (6.9 mi) south; Indiana 27 mi west; day 13.8 mi north
  assert.strictEqual(r.value, 'oki|* us-midwest us-oh cvg|* us-southeast us-ky');
  assert.deepStrictEqual(r.warnings, []);
});

test('inside cvg and within reach of day: both metros are siblings under the home state', () => {
  const r = cmd(layers(), 39.3, -84.2); // day is 0.1 deg (6.9 mi) north
  assert.strictEqual(r.value, 'oki|* us-midwest us-oh cvg|us-oh day');
  assert.deepStrictEqual(r.warnings, []);
});

test('Kentucky side of the line, near Ohio and Indiana: home branch first, neighbours nearest first', () => {
  const r = cmd(layers(), 39.0, -84.85); // Ohio 6.9 mi north, Indiana 8.1 mi west
  assert.strictEqual(r.value, 'oki|* us-southeast us-ky cvg|* us-midwest us-oh|us-midwest us-in');
  assert.deepStrictEqual(r.warnings, []);
});

test('the per-layer record keeps the containing region and the near ones apart', () => {
  const r = cmd(layers(), 39.0, -84.85);
  const state = r.perLayer.filter((p) => p.id === 'state')[0];
  assert.strictEqual(state.match.code, 'us-ky');
  assert.deepStrictEqual(state.near.map((m) => m.code), ['us-oh', 'us-in']);
  near(state.near[0].distance / MI, 6.9, 0.1, 'Ohio distance in miles');
  near(state.near[1].distance / MI, 8.05, 0.1, 'Indiana distance in miles');
});

test('outside every region: no command', () => {
  const r = cmd(layers(), 35, -70);
  assert.strictEqual(r.value, '');
  assert.deepStrictEqual(r.tokens, []);
});

test('a root layer works wherever it sits in the list', () => {
  const [mesh, macro, state, metro] = layers();
  assert.strictEqual(cmd([macro, state, metro, mesh], 40.5, -83.2).value, 'us-midwest us-oh|* oki');
});

test('without root / parentProperty / nearMiles the layers stay a plain chain, no "|" anywhere', () => {
  const r = cmd(layers({ tree: false }), 39.2, -84.5);
  assert.strictEqual(r.value, 'us-midwest oki us-oh cvg');
  assert.ok(r.tokens.every((t) => t.indexOf('|') < 0));
});

test('a state with no parent code attaches to the wildcard and says so', () => {
  const r = cmd(layers({ noRegion: true }), 40.5, -83.2);
  assert.strictEqual(r.value, 'oki|* us-midwest|* us-oh');
  assert.ok(r.warnings.some((w) => /State: "US-OH" has no usable parent code in "region"; attached to the wildcard/.test(w)), r.warnings.join(' | '));
});

test('a parent that is not a region of any layer above attaches to the wildcard and says so', () => {
  const r = cmd(layers({ badParent: 'nowhere' }), 39.0, -84.85);
  assert.ok(r.warnings.some((w) => /parent "nowhere" is not a region of any layer above/.test(w)), r.warnings.join(' | '));
  assert.ok(r.value.indexOf('nowhere') >= 0 && r.blocked.length === 0);
});

test('a code that two layers both produce is defined once, with a warning when the layer has no nearMiles', () => {
  const a = R.buildLayer({ id: 'a', label: 'A', root: true }, [{ name: 'x', code: 'dup', data: fc(poly({}, sq(0, 0, 1, 1))) }]);
  const b = R.buildLayer({ id: 'b', label: 'B' }, [{ name: 'x', code: 'dup', data: fc(poly({}, sq(0, 0, 1, 1))) }]);
  const r = R.buildRegionDef([a, b], 0.5, 0.5);
  assert.strictEqual(r.value, 'dup');
  assert.ok(r.warnings.some((w) => /already used by a higher layer/.test(w)));
});

// ---- overlap handling ----------------------------------------------------------------------------
test('nearMiles layer: a point inside two overlapping regions gets both codes, with no overlap warning', () => {
  const l = R.buildLayer({ id: 'm', label: 'Metro', nameProperty: 'iata', nearMiles: 1 }, [
    { data: fc(poly({ iata: 'CVG' }, sq(0, 0, 2, 2)), poly({ iata: 'LUK' }, sq(1, 1, 3, 3))) }]);
  const r = R.buildRegionDef([l], 1.5, 1.5);
  assert.strictEqual(r.value, 'cvg|* luk');
  assert.deepStrictEqual(r.warnings, []);
});

test('layer without nearMiles: first region wins and the overlap is still warned about', () => {
  const l = R.buildLayer({ id: 'm', label: 'Metro', nameProperty: 'iata' }, [
    { data: fc(poly({ iata: 'CVG' }, sq(0, 0, 2, 2)), poly({ iata: 'LUK' }, sq(1, 1, 3, 3))) }]);
  const r = R.buildRegionDef([l], 1.5, 1.5);
  assert.strictEqual(r.value, 'cvg');
  assert.deepStrictEqual(r.warnings, ['Metro: point is in 2 overlapping regions (cvg, luk); using cvg.']);
});

// ---- commandProblems -------------------------------------------------------------------------------
test('commandProblems accepts name and name|jump tokens', () => {
  assert.deepStrictEqual(R.commandProblems('oki|* us-midwest us-oh cvg|* us-southeast us-ky'), []);
  assert.deepStrictEqual(R.commandProblems('a b|a c'), []);
});

test('commandProblems: jump target must be * or created earlier in the command', () => {
  assert.strictEqual(R.commandProblems('a|nope b').length, 1);
  assert.strictEqual(R.commandProblems('a b|c c').length, 1); // c is created after the jump to it
  assert.strictEqual(R.commandProblems('a|a b').length, 0);   // jumping to the region just created is fine
});

test('commandProblems: at most one "|" per token, and the name part is still validated', () => {
  assert.strictEqual(R.commandProblems('a|b|c').length >= 1, true);
  assert.ok(R.commandProblems('A|*')[0].indexOf('"A" is not a valid region name') === 0);
  assert.ok(R.commandProblems('|*')[0].indexOf('"" is not a valid region name') === 0);
});

test('commandProblems: the 158 character and 32 region limits still apply with jump tokens', () => {
  const long = Array.from({ length: 12 }, (_, i) => 'region-number-' + i + '|*').join(' ');
  assert.ok(R.commandProblems(long).some((p) => /characters; the repeater accepts at most 158/.test(p)));
  const many = Array.from({ length: 33 }, (_, i) => 'r' + i + '|*').join(' ');
  assert.ok(R.commandProblems(many).some((p) => /33 regions/.test(p)));
});

test('repeaterCommands wraps a jump command and withholds an invalid one', () => {
  assert.strictEqual(R.repeaterCommands('oki|* us-midwest'), 'region def oki|* us-midwest\nregion allowf *\nregion save');
  assert.strictEqual(R.repeaterCommands('a|nope'), '');
});

// ---- manifest ---------------------------------------------------------------------------------------
const manifest = (layer) => ({ version: 1, layers: [Object.assign({ id: 'l', url: 'l.geojson' }, layer)] });

test('manifest accepts root, parentProperty and nearMiles without an unknown-field warning', () => {
  [{ root: true, nearMiles: 10 }, { parentProperty: 'region', nearMiles: 10 }, { parentProperty: ['region', 'macro'] }].forEach((layer) => {
    const v = R.validateManifest(manifest(layer));
    assert.deepStrictEqual(v.errors, [], JSON.stringify(layer));
    assert.deepStrictEqual(v.warnings, [], JSON.stringify(layer));
  });
});

test('manifest rejects a bad nearMiles, root or parentProperty', () => {
  [0, -1, 101, '10', NaN, null].forEach((v) => {
    assert.strictEqual(R.validateManifest(manifest({ nearMiles: v })).errors.length, 1, 'nearMiles ' + String(v));
  });
  assert.strictEqual(R.validateManifest(manifest({ root: 'yes' })).errors.length, 1);
  assert.strictEqual(R.validateManifest(manifest({ parentProperty: 5 })).errors.length, 1);
  assert.strictEqual(R.validateManifest(manifest({ nearMiles: 100 })).errors.length, 0);
});

test('manifest warns when root and parentProperty are both set, or parentProperty meets groupBy', () => {
  assert.strictEqual(R.validateManifest(manifest({ root: true, parentProperty: 'region' })).warnings.length, 1);
  assert.strictEqual(R.validateManifest(manifest({ parentProperty: 'region', groupBy: 'g' })).warnings.length, 1);
});

test('buildLayer carries the new keys and defaults them off', () => {
  const l = R.buildLayer({ id: 'a', label: 'A', root: true, parentProperty: 'p', nearMiles: 7 }, [{ data: fc(poly({ name: 'x' }, sq(0, 0, 1, 1))) }]);
  assert.strictEqual(l.root, true); assert.strictEqual(l.parentProperty, 'p'); assert.strictEqual(l.nearMiles, 7);
  const d = R.buildLayer({ id: 'a' }, [{ data: fc(poly({ name: 'x' }, sq(0, 0, 1, 1))) }]);
  assert.strictEqual(d.root, false); assert.strictEqual(d.parentProperty, null); assert.strictEqual(d.nearMiles, 0);
});

// ---- performance ------------------------------------------------------------------------------------
test('perf: a point against 2,500 regions of 64 vertices each stays under 50 ms (bounding-box prefilter)', () => {
  const feats = [];
  for (let gx = 0; gx < 50; gx++) {
    for (let gy = 0; gy < 50; gy++) {
      const ring = [];
      for (let k = 0; k < 64; k++) {
        const a = (k / 64) * 2 * Math.PI;
        ring.push([-90 + gx * 0.1 + 0.03 * Math.cos(a), 30 + gy * 0.1 + 0.03 * Math.sin(a)]);
      }
      ring.push(ring[0]);
      feats.push(poly({ name: 'r' + gx + '-' + gy }, ring));
    }
  }
  const big = R.buildLayer({ id: 'big', label: 'Big', nameProperty: 'name', nearMiles: 10 }, [{ data: fc(...feats) }]);
  const t0 = process.hrtime.bigint();
  let r;
  for (let i = 0; i < 20; i++) r = R.buildRegionDef([big], 32.52, -87.48);
  const ms = Number(process.hrtime.bigint() - t0) / 1e6 / 20;
  console.log('       2,500 regions, one lookup: ' + ms.toFixed(2) + ' ms');
  assert.ok(r.perLayer[0].near.length + r.perLayer[0].matches.length >= 1);
  assert.ok(ms < 50, 'took ' + ms.toFixed(1) + ' ms');
});

// ---- page wiring (static) -------------------------------------------------------------------------------
test('the page draws the ring with Leaflet and uses the shared matcher', () => {
  const js = read('public/region-def.js');
  assert.ok(/L\.circle\(/.test(js), 'ring');
  assert.ok(/RegionDef\.matchNear\(/.test(js), 'matchNear');
  assert.ok(/id="rd-ring"/.test(js), 'ring input');
  assert.ok(/rd-b-near/.test(read('public/region-def.css')), 'NEAR badge style');
});

console.log('\n' + passed + ' passed, ' + failed + ' failed');
if (failed) process.exit(1);
