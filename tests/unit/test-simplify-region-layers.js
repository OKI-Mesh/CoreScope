/**
 * Tests for scripts/simplify-region-layers.js
 */
'use strict';
const fs = require('fs');
const os = require('os');
const path = require('path');
const assert = require('assert');
const { spawnSync } = require('child_process');
const { run, simplifyRing } = require('../../scripts/simplify-region-layers.js');
const { checkRegionData } = require('../../scripts/check-region-data.js');

const SCRIPT = path.join(__dirname, '..', '..', 'scripts', 'simplify-region-layers.js');
const FIXTURE = path.join(__dirname, '..', 'fixtures', 'region-layers');

let passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('  ok  ' + name); }
  catch (e) { failed++; console.error('  FAIL ' + name + ': ' + e.message); }
}
const tmp = (p) => fs.mkdtempSync(path.join(os.tmpdir(), p));

// A dense copy of the fixture: every edge subdivided into 40 points with sub-tolerance noise,
// coordinates over-precise, as a real boundary download would be.
function denseFixture() {
  const d = tmp('rd-dense-');
  let seed = 7; const rnd = () => (seed = (seed * 16807) % 2147483647) / 2147483647 - 0.5;
  const dense = (ring) => {
    const out = [];
    for (let i = 1; i < ring.length; i++) {
      const [x0, y0] = ring[i - 1], [x1, y1] = ring[i];
      for (let s = 0; s < 40; s++) {
        const t = s / 40;
        const noise = (s === 0) ? 0 : rnd() * 0.0001;           // far below the 0.0005 tolerance, vertices of the original stay exact
        out.push([x0 + (x1 - x0) * t + noise, y0 + (y1 - y0) * t + noise]);
      }
    }
    out.push(ring[ring.length - 1]);
    return out;
  };
  fs.readdirSync(FIXTURE).forEach((f) => {
    if (f === 'README.md') return;
    const src = fs.readFileSync(path.join(FIXTURE, f), 'utf8');
    if (!/\.geojson$/.test(f)) { fs.writeFileSync(path.join(d, f), src); return; }
    const g = JSON.parse(src);
    g.features.forEach((ft) => { ft.geometry.coordinates = ft.geometry.coordinates.map((r) => dense(r)); });
    fs.writeFileSync(path.join(d, f), JSON.stringify(g, null, 2));
  });
  return d;
}

test('simplifyRing: removes collinear points, keeps corners, stays closed with >= 4 positions', () => {
  const ring = [[0, 0], [1, 0], [2, 0], [3, 0], [3, 1], [3, 2], [3, 3], [0, 3], [0, 2], [0, 1], [0, 0]];
  const s = simplifyRing(ring, 0.001, 5);
  assert.deepStrictEqual(s, [[0, 0], [3, 0], [3, 3], [0, 3], [0, 0]]);
});

test('simplifyRing: a ring that collapses is reported (null) so the caller keeps the original', () => {
  assert.strictEqual(simplifyRing([[0, 0], [0.00001, 0], [0.00001, 0.00001], [0, 0.00001], [0, 0]], 1, 5), null);
});

test('dense data: big vertex and byte reduction, output passes the integrity check, known points unchanged', () => {
  const inDir = denseFixture(), outDir = tmp('rd-simp-');
  assert.deepStrictEqual(checkRegionData(inDir, { overlapTolerance: 0.001 }).errors, [], 'dense input should itself be valid');
  const r = run(inDir, outDir, { tolerance: 0.0005, precision: 5 });
  assert.deepStrictEqual(r.errors, []);
  assert.ok(r.ok);
  const sum = (k) => r.files.reduce((s, f) => s + f[k], 0);
  assert.ok(sum('vertsAfter') < sum('vertsBefore') / 10, 'vertices ' + sum('vertsBefore') + ' -> ' + sum('vertsAfter'));
  assert.ok(sum('bytesAfter') < sum('bytesBefore') / 10, 'bytes ' + sum('bytesBefore') + ' -> ' + sum('bytesAfter'));
  const after = checkRegionData(outDir);
  assert.deepStrictEqual(after.errors, []);
  assert.strictEqual(after.known.length, 26);
  assert.ok(after.known.every((k) => k.got === k.expect));
});

test('too coarse a tolerance is refused (output would change answers) with a clear error', () => {
  const inDir = denseFixture(), outDir = tmp('rd-simp-');
  const r = run(inDir, outDir, { tolerance: 3, precision: 5 });
  assert.strictEqual(r.ok, false);
  assert.ok(/fails the integrity check/.test(r.errors[0]), r.errors.join('\n'));
});

test('input that already fails the integrity check is refused', () => {
  const inDir = tmp('rd-bad-'); fs.writeFileSync(path.join(inDir, 'layers.json'), '{ nope');
  const r = run(inDir, tmp('rd-simp-'), {});
  assert.strictEqual(r.ok, false);
  assert.ok(/input fails the integrity check/.test(r.errors[0]));
});

test('--in and --out must differ; CLI exit codes', () => {
  assert.strictEqual(run(FIXTURE, FIXTURE, {}).ok, false);
  const out = tmp('rd-simp-');
  assert.strictEqual(spawnSync('node', [SCRIPT, '--in', FIXTURE, '--out', out]).status, 0);
  assert.strictEqual(spawnSync('node', [SCRIPT, '--in', FIXTURE, '--out', FIXTURE]).status, 1);
  assert.strictEqual(spawnSync('node', [SCRIPT]).status, 2);
});

console.log('\n' + passed + ' passed, ' + failed + ' failed');
if (failed) process.exit(1);
