/**
 * Manifest checks for public/region-layers/.
 *
 *  - layers.json.example (the documented format) must always validate cleanly.
 *  - It must use the same conventions as the test fixture, which is what the integrity and
 *    known-point tests run against (guards the documentation drifting away from what is tested:
 *    same layer ids, same name/code/group properties, same file shapes).
 *  - known-points.json.example must match the fixture's table.
 *  - If a real public/region-layers/layers.json is committed it is validated, and the full
 *    deployed-data checks live in test-region-def-known-points.js / scripts/check-region-data.js.
 */
'use strict';
const fs = require('fs');
const path = require('path');
const assert = require('assert');

const ROOT = path.join(__dirname, '..', '..');
const DIR = path.join(ROOT, 'public', 'region-layers');
const FIX = path.join(ROOT, 'tests', 'fixtures', 'region-layers');
const R = require(path.join(ROOT, 'public', 'region-def-core.js'));
const json = (f) => JSON.parse(fs.readFileSync(f, 'utf8'));

let passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('  ok  ' + name); }
  catch (e) { failed++; console.error('  FAIL ' + name + ': ' + e.message); }
}

const example = json(path.join(DIR, 'layers.json.example'));

test('layers.json.example validates cleanly and carries the schema version', () => {
  const v = R.validateManifest(example);
  assert.deepStrictEqual(v.errors, []);
  assert.deepStrictEqual(v.warnings, []);
  assert.strictEqual(example.version, R.MANIFEST_VERSION);
});

test('layers.json.example uses the same conventions as the tested fixture', () => {
  const shape = (m) => m.layers.map((l) => ({
    id: l.id, nameProperty: l.nameProperty || null, codeProperty: l.codeProperty || null, groupBy: l.groupBy || null,
    files: (Array.isArray(l.url) ? l.url : [l.url]).map((u) => (typeof u === 'object' ? { name: u.name, code: u.code } : 'plain'))
  }));
  assert.deepStrictEqual(shape(example), shape(json(path.join(FIX, 'layers.json'))));
});

test('known-points.json.example matches the fixture table (verified against the fixture by the known-points test)', () => {
  assert.deepStrictEqual(json(path.join(DIR, 'known-points.json.example')), json(path.join(FIX, 'known-points.json')));
});

test('the fixture manifest validates cleanly', () => {
  const v = R.validateManifest(json(path.join(FIX, 'layers.json')));
  assert.deepStrictEqual(v.errors.concat(v.warnings), []);
});

const prod = path.join(DIR, 'layers.json');
if (!fs.existsSync(prod)) {
  console.log('  skip deployed manifest check (public/region-layers/layers.json not committed)');
} else {
  test('deployed layers.json validates and sets "version"', () => {
    const m = json(prod);
    const v = R.validateManifest(m);
    assert.deepStrictEqual(v.errors, []);
    assert.strictEqual(m.version, R.MANIFEST_VERSION);
  });
}

console.log('\n' + passed + ' passed, ' + failed + ' failed');
if (failed) process.exit(1);
