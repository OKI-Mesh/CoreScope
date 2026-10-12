/**
 * Known-point regression tests for the Region Def lookup, driven by files on disk.
 *
 *  1. tests/fixtures/region-layers/  (always): a coarse synthetic deployment (see its README) with
 *     a table of real cities and of points just either side of shared borders. This guards the
 *     lookup, grouping, ordering and exclusion logic.
 *  2. public/region-layers/          (when layers.json is committed): the deployed manifest and
 *     boundary files, checked by the same integrity routine plus the operator's own
 *     known-points.json. That is the test that verifies the files actually shipped.
 */
'use strict';
const fs = require('fs');
const path = require('path');
const assert = require('assert');
const R = require(path.join(__dirname, '..', '..', 'public', 'region-def-core.js'));
const { checkRegionData } = require(path.join(__dirname, '..', '..', 'scripts', 'check-region-data.js'));

const ROOT = path.join(__dirname, '..', '..');
const FIXTURE = path.join(ROOT, 'tests', 'fixtures', 'region-layers');
const PROD = path.join(ROOT, 'public', 'region-layers');

let passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('  ok  ' + name); }
  catch (e) { failed++; console.error('  FAIL ' + name + ': ' + e.message); }
}

function suite(label, dir) {
  const res = checkRegionData(dir);
  test(label + ': manifest and boundary files pass the integrity check', () => {
    assert.deepStrictEqual(res.errors, []);
    assert.deepStrictEqual(res.warnings, []);
  });
  const layers = res.layers.map((x) => x.layer);
  const kpFile = path.join(dir, 'known-points.json');
  const points = fs.existsSync(kpFile) ? JSON.parse(fs.readFileSync(kpFile, 'utf8')) : [];
  points.forEach((p) => {
    test(label + ': ' + p.name + ' -> "' + p.expect + '"', () => {
      const r = R.buildRegionDef(layers, p.lat, p.lon);
      assert.strictEqual(r.value, p.expect);
      if (!p.allowWarnings) assert.deepStrictEqual(r.warnings, []);
      assert.ok(!r.perLayer.some((x) => x.unnamed || x.gap));
    });
  });
  test(label + ': every code any polygon can produce is a valid repeater identifier', () => {
    res.layers.forEach(({ def, layer }) => layer.features.forEach((e) => {
      assert.ok(e.code && R.CODE_RE.test(e.code), def.id + ': ' + JSON.stringify(e.name) + ' -> ' + e.code);
    }));
  });
  return { res: res, layers: layers, points: points };
}

// ---- 1. fixture ------------------------------------------------------------------------------
const fx = suite('fixture', FIXTURE);
const lookup = (lat, lon) => R.buildRegionDef(fx.layers, lat, lon);

test('fixture: table covers Ohio, Indiana, Kentucky, non-mesh states, borders and an empty result', () => {
  const names = fx.points.map((p) => p.name).join(' | ');
  ['Cincinnati OH', 'Indianapolis IN', 'Covington KY', 'Detroit MI', 'Chicago IL', 'Charleston WV', 'Pittsburgh PA', 'OH/IN line', 'IN/IL line', 'OH/PA line', 'Ohio River']
    .forEach((n) => assert.ok(names.indexOf(n) >= 0, 'missing ' + n));
  assert.ok(fx.points.some((p) => p.expect === ''), 'needs an outside-everything point');
  assert.ok(fx.points.length >= 25);
});

test('Indiana regression: an Indiana point yields the macro region AND the state (not the macro region alone)', () => {
  const t = lookup(39.7684, -86.1581).tokens;
  assert.ok(t.indexOf('us-midwest') === 0 && t.indexOf('us-in') > 0, t.join(' '));
});

test('mesh states always carry oki between macro and state; non-mesh states never do', () => {
  fx.points.forEach((p) => {
    const t = lookup(p.lat, p.lon).tokens;
    const isMesh = ['us-oh', 'us-in', 'us-ky'].some((s) => t.indexOf(s) >= 0);
    if (isMesh) assert.strictEqual(t[1], 'oki', p.name + ': ' + t.join(' '));
    else assert.ok(t.indexOf('oki') < 0, p.name + ': ' + t.join(' '));
  });
});

test('output order is always macro, mesh, state, metro', () => {
  const rank = (c) => (c.indexOf('us-midwest') === 0 || c === 'us-southeast' ? 0 : c === 'oki' ? 1 : /^(cvg|cmh|ind)$/.test(c) ? 3 : 2);
  fx.points.forEach((p) => {
    const ranks = lookup(p.lat, p.lon).tokens.map(rank);
    assert.deepStrictEqual(ranks, ranks.slice().sort((a, b) => a - b), p.name);
  });
});

test('a point exactly on the Ohio/Indiana border is deterministic: first state in the file wins, with a warning', () => {
  const r = lookup(40.0, -84.82);
  assert.strictEqual(r.perLayer[2].match.code, 'us-oh');
  assert.ok(r.warnings.some((w) => /overlapping regions \(us-oh, us-in\)/.test(w)), r.warnings.join(' | '));
  assert.strictEqual(r.perLayer[0].matches.length, 1, 'both states are in the same macro region: one match');
});

test('every state has an interior point that resolves to its own state (no hole in the fixture)', () => {
  const inside = { 'us-oh': [40.3, -82.8], 'us-in': [40.0, -86.2], 'us-ky': [37.5, -85.5], 'us-mi': [43.3, -84.5], 'us-il': [40.0, -89.0],
    'us-pa': [40.9, -77.5], 'us-wv': [38.6, -80.6] };
  Object.keys(inside).forEach((code) => {
    const r = lookup(inside[code][0], inside[code][1]);
    assert.strictEqual(r.perLayer[2].match && r.perLayer[2].match.code, code);
  });
});

test('sweeping a line across the Ohio/Indiana border flips state exactly once', () => {
  let flips = 0, prev = null;
  for (let lon = -85.2; lon <= -84.4; lon += 0.01) {
    const code = lookup(40.5, Math.round(lon * 100) / 100).perLayer[2].match.code;
    if (prev && code !== prev) flips++;
    prev = code;
  }
  assert.strictEqual(flips, 1);
});

// ---- 2. the deployed files, when committed --------------------------------------------------------
if (!fs.existsSync(path.join(PROD, 'layers.json'))) {
  console.log('  skip deployed-data checks (public/region-layers/layers.json not committed)');
} else {
  suite('deployed', PROD);
  if (!fs.existsSync(path.join(PROD, 'known-points.json'))) {
    test('deployed: known-points.json exists (add at least one point per mapped state and one on a shared border)', () => assert.fail('missing public/region-layers/known-points.json'));
  }
}

console.log('\n' + passed + ' passed, ' + failed + ' failed');
if (failed) process.exit(1);
