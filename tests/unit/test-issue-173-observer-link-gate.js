/* test-issue-173-observer-link-gate.js — behavioral test for the observer-link
 * gate shipped for OKI-Mesh/CoreScope#173 (bug OKI-Mesh/CoreScope#170).
 *
 * NOTE ON ISSUE NUMBERING: #173 / #170 here are OKI-Mesh/CoreScope issues, not
 * the upstream 4-digit numbers most suites in this directory reference.
 *
 * The node-detail "Observer ->" link used to render for every node. An observer
 * record exists for only 71 of 1,436 nodes on prod, so it 404'd on 95% of node
 * pages. It is now gated behind OBSERVER_LINK_ENABLED, which ships `false`.
 *
 * Why this suite exists even though #1825 already greps for the href:
 *   test-issue-1825-observer-node-cross-links.js asserts the observer href is
 *   PRESENT in nodes.js source. That is still correct — the markup is retained
 *   on purpose so re-enabling is a flag flip — but it passes whether the link
 *   is gated or not, so it cannot catch a regression here. Nothing else asserts
 *   the link is actually off.
 *
 * Strategy (mirrors test-issue-1470-node-tile-helper.js): extract the button-row
 * template source out of public/nodes.js by regex and evaluate THAT SOURCE in a
 * vm sandbox with the flag forced both ways. The assertions run against the
 * shipped expression, not a copy of it.
 *
 * What breaks this suite:
 *   - restoring the unconditional anchor (drops the ternary)   -> test 2 fails
 *   - flipping the shipped default to true                     -> test 1 fails
 *   - deleting the markup instead of gating it                 -> test 4 fails
 *   - gating the Analytics/Reach links by accident             -> test 3 fails
 */
'use strict';
const REPO_ROOT = require('path').resolve(__dirname, '..', '..');

const vm = require('vm');
const fs = require('fs');
const path = require('path');
const assert = require('assert');

let passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('  ✅ ' + name); }
  catch (e) { failed++; console.log('  ❌ ' + name + ': ' + e.message); }
}

console.log('── Observer link gate (#173) ──');

const nodesSrc = fs.readFileSync(path.join(REPO_ROOT, 'public', 'nodes.js'), 'utf8');

// ── Pull the shipped default out of the source ────────────────────────────
const flagDecl = /\bconst\s+OBSERVER_LINK_ENABLED\s*=\s*(true|false)\s*;/.exec(nodesSrc);

// ── Pull the button row out of the source ─────────────────────────────────
// The row is the flex div that holds copyUrlBtn through the observer anchor.
// Anchored on copyUrlBtn so it cannot drift onto a different flex container.
function extractButtonRow(src) {
  const lines = src.split('\n');
  const btnIdx = lines.findIndex((l) => l.includes('id="copyUrlBtn"'));
  assert.ok(btnIdx > 0, 'could not find the copyUrlBtn line in public/nodes.js');
  let open = -1;
  for (let i = btnIdx; i >= 0; i--) {
    if (lines[i].includes('display:flex;flex-wrap:wrap;gap:6px')) { open = i; break; }
  }
  assert.ok(open >= 0, 'could not find the opening flex div above copyUrlBtn');
  let close = -1;
  for (let i = btnIdx; i < lines.length; i++) {
    if (lines[i].trim() === '</div>') { close = i; break; }
  }
  assert.ok(close > open, 'could not find the closing </div> of the button row');
  return lines.slice(open, close + 1).join('\n');
}

const rowSrc = extractButtonRow(nodesSrc);

// The extracted text is a fragment of a template literal, so wrapping it in
// backticks reproduces the original nesting (the observer anchor is itself a
// nested template literal inside `${...}` — valid, and exactly how it ships).
function render(flagValue) {
  const sandbox = {
    n: { public_key: 'abc123def456' },
    OBSERVER_LINK_ENABLED: flagValue,
    encodeURIComponent,
  };
  return vm.runInNewContext('`' + rowSrc + '`', sandbox);
}

// ── 1. The shipped default is off ─────────────────────────────────────────
test('OBSERVER_LINK_ENABLED ships as false', () => {
  assert.ok(flagDecl, 'no `const OBSERVER_LINK_ENABLED = <bool>;` found in public/nodes.js');
  assert.strictEqual(
    flagDecl[1], 'false',
    'OBSERVER_LINK_ENABLED must ship false. Re-enabling it needs a real "does an '
    + 'observer exist for this pubkey" condition, which depends on the node-identity '
    + 'decision in the Postgres migration (#19, Feature #14) — not a flag flip.'
  );
});

// ── 2. With the flag off, no observer link is rendered ────────────────────
test('flag off renders no #/observers/ link', () => {
  const html = render(false);
  assert.ok(
    !html.includes('#/observers/'),
    'the button row still emits an #/observers/ href with the gate off — the '
    + 'anchor is being rendered unconditionally again'
  );
  assert.ok(!/Observer\s*→/.test(html), 'the "Observer ->" label is still rendered with the gate off');
});

// ── 3. The gate did not take the rest of the row with it ─────────────────
test('flag off keeps Analytics and Reach', () => {
  const html = render(false);
  assert.ok(html.includes('#/nodes/abc123def456/analytics'), 'Analytics link missing');
  assert.ok(html.includes('#/nodes/abc123def456/reach'), 'Reach link missing');
  assert.ok(html.includes('id="copyUrlBtn"'), 'Copy URL button missing');
  assert.ok(html.includes('id="copyShortUrlBtn"'), 'Copy short URL button missing');
});

// ── 4. The markup is retained and the gate actually controls it ───────────
test('flag on restores the link, uppercased', () => {
  const html = render(true);
  assert.ok(
    html.includes('#/observers/ABC123DEF456'),
    'flipping the gate on did not produce the observer href — the markup has been '
    + 'deleted rather than gated, or the pubkey is no longer uppercased (#1836)'
  );
});

// ── 5. The off branch leaves no interpolation artifact ───────────────────
test('flag off emits no undefined/null artifact', () => {
  const html = render(false);
  assert.ok(!/undefined|null/.test(html), 'off branch interpolated a non-empty value: ' + html.slice(0, 200));
});

console.log('\n' + (failed === 0 ? '✅' : '❌') + ` ${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
