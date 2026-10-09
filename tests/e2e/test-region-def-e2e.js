#!/usr/bin/env node
/* Region Def (#/tools/region-def) E2E.
 *
 * Mocked part: the page reads operator-supplied files from /region-layers/. Those requests are
 * intercepted and answered from tests/fixtures/region-layers/ (a synthetic multi-state
 * deployment), with per-test mutations for failure modes. Nominatim is intercepted too: no test
 * ever touches the real service.
 *
 * Production smoke (last step): if the server under test really serves /region-layers/layers.json,
 * the page is loaded WITHOUT interception and the deployed files are exercised through the real
 * UI (every point in the deployed known-points.json must produce its exact command). A server
 * with no region data deployed skips that step.
 *
 * Assertions are on exact text (commands, messages, badges), not just on elements existing.
 * CHROMIUM_REQUIRE=1 makes Chromium-launch failure a HARD FAIL. Exit code is nonzero on any failure.
 */
'use strict';
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');

const BASE = process.env.BASE_URL || 'http://localhost:13581';
const FIX = path.join(__dirname, '..', 'fixtures', 'region-layers');
const CMD_TAIL = '\nregion save\nregion allowf *';

let passed = 0, failed = 0;
async function step(name, fn) {
  try { await fn(); passed++; console.log('  ok  ' + name); }
  catch (e) { failed++; console.error('  FAIL ' + name + ': ' + e.message); }
}
function eq(actual, expected, what) {
  if (actual !== expected) throw new Error((what || 'value') + ': expected ' + JSON.stringify(expected) + ' but got ' + JSON.stringify(actual));
}
function ok(c, m) { if (!c) throw new Error(m || 'assertion failed'); }

const load = (name) => JSON.parse(fs.readFileSync(path.join(FIX, name), 'utf8'));
function freshFiles() {
  const f = {};
  fs.readdirSync(FIX).filter((n) => n !== 'README.md').forEach((n) => { f[n] = load(n); });
  return f;
}
const KNOWN = load('known-points.json');
const point = (name) => KNOWN.find((p) => p.name.indexOf(name) === 0);
const cmd = (value) => 'region def ' + value + CMD_TAIL;
const box = (x0, y0, x1, y1) => [[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]];
const polyFeature = (props, ring) => ({ type: 'Feature', properties: props, geometry: { type: 'Polygon', coordinates: [ring] } });

let ctx;

/* Open a page whose /region-layers/ and Nominatim traffic is mocked.
 *   mutate(files)   edit the fixture files (deep copies) before serving
 *   respond         { 'name': { status, body, contentType, hang, abort } } per-file overrides
 *   nominatim       (route) => ...  custom geocoder handler (default: one Cincinnati result)
 *   config          window.RegionDefConfig (timeouts)
 *   viewport        { width, height }
 *   fn(page, hits)  the test body; hits counts requests per layer file
 */
async function withPage(opts, fn) {
  const files = freshFiles();
  if (opts.mutate) opts.mutate(files);
  const pg = await ctx.newPage();
  if (opts.viewport) await pg.setViewportSize(opts.viewport);
  if (opts.config) await pg.addInitScript((c) => { window.RegionDefConfig = c; }, opts.config);
  const hits = {};
  const respond = opts.respond || {};
  const pageErrors = [];
  pg.on('pageerror', (e) => pageErrors.push(e.message));
  await pg.route('**/region-layers/**', (route) => {
    const name = route.request().url().split('/region-layers/')[1].split('?')[0];
    hits[name] = (hits[name] || 0) + 1;
    const o = respond[name];
    if (o && o.hang) return; // never answered: exercises the timeout
    if (o && o.abort) return route.abort('failed');
    if (o) return route.fulfill({ status: o.status || 200, contentType: o.contentType || 'application/json', body: typeof o.body === 'string' ? o.body : JSON.stringify(o.body) });
    if (!(name in files)) return route.fulfill({ status: 404, body: 'not found' });
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(files[name]) });
  });
  await pg.route('https://nominatim.openstreetmap.org/**', (route) => {
    if (opts.nominatim) return opts.nominatim(route);
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify([{ lat: '39.1031', lon: '-84.5120', display_name: 'Cincinnati, Ohio, USA' }]) });
  });
  try { await fn(pg, hits); }
  finally { await pg.close(); }
  if (pageErrors.length) throw new Error('uncaught page error: ' + pageErrors[0]);
}

const goto = (pg, hash) => pg.goto(BASE + '/#/tools/region-def' + (hash || ''), { waitUntil: 'domcontentloaded' });
const text = async (pg, sel) => (await pg.textContent(sel)) || '';
async function rows(pg) {
  return pg.$$eval('.rd-row', (els) => els.map((e) => ({ label: e.querySelector('.rd-row-label').textContent, badge: e.querySelector('.rd-badge').textContent, name: (e.querySelector('.rd-row-name') || {}).textContent || '' })));
}
async function expectCommand(pg, value) {
  await pg.waitForSelector('.rd-cmd');
  eq(await text(pg, '.rd-cmd'), cmd(value), 'repeater command');
  eq(await text(pg, '.rd-cmd-head'), 'Type this into your repeater', 'header');
  ok(!(await pg.$('.rd-incomplete')), 'unexpected incomplete banner');
  eq(await text(pg, '#rd-msg'), '', 'status line');
}
async function waitMsg(pg, re) {
  await pg.waitForFunction((src) => new RegExp(src).test((document.querySelector('#rd-msg') || {}).textContent || ''), re.source, { timeout: 8000 });
  return text(pg, '#rd-msg');
}
async function typeAndGo(pg, value) { await pg.fill('#rd-input', value); await pg.press('#rd-input', 'Enter'); }

(async () => {
  const requireChromium = process.env.CHROMIUM_REQUIRE === '1';
  let browser;
  try {
    browser = await chromium.launch({ headless: true, executablePath: process.env.CHROMIUM_PATH || undefined,
      args: ['--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage'] });
  } catch (err) {
    if (requireChromium) { console.error('test-region-def-e2e.js: FAIL - Chromium required but unavailable: ' + err.message); process.exit(1); }
    console.log('test-region-def-e2e.js: SKIP (Chromium unavailable)'); process.exit(0);
  }
  ctx = await browser.newContext({ viewport: { width: 1400, height: 900 } });
  ctx.setDefaultTimeout(15000);

  console.log('\n=== region-def E2E against ' + BASE + ' ===');

  // ---------------------------------------------------------------- navigation and happy path
  await step('Tools landing links to Region Def', async () => {
    const pg = await ctx.newPage();
    await pg.goto(BASE + '/#/tools', { waitUntil: 'domcontentloaded' });
    await pg.waitForSelector('.tools-card');
    const hrefs = await pg.$$eval('.tools-card', (a) => a.map((x) => x.getAttribute('href')));
    ok(hrefs.indexOf('#/tools/region-def') >= 0, 'card missing, got ' + hrefs.join(','));
    await pg.close();
  });

  await step('page loads all four layers and the map', async () => {
    await withPage({}, async (pg) => {
      await goto(pg);
      await pg.waitForSelector('.rd-toggle');
      eq(JSON.stringify(await pg.$$eval('.rd-toggle', (l) => l.map((x) => x.textContent))), JSON.stringify(['Macro region', 'Mesh region', 'State', 'Metro']), 'layer toggles');
      ok(await pg.$('#rd-map.leaflet-container'), 'leaflet map missing');
      eq(await text(pg, '#rd-msg'), '', 'status line');
    });
  });

  for (const name of ['Cincinnati OH', 'Indianapolis IN', 'Covington KY', 'Detroit MI', 'Pittsburgh PA', 'OH/IN line, Indiana side, lat 40.5', 'OH/PA line, Pennsylvania side']) {
    await step('exact command for ' + name, async () => {
      const p = point(name);
      await withPage({}, async (pg) => {
        await goto(pg, '?lat=' + p.lat + '&lon=' + p.lon);
        await expectCommand(pg, p.expect);
        eq((await rows(pg)).length, 4, 'one result row per layer');
      });
    });
  }

  await step('result rows give INSIDE/OUTSIDE per layer with the matched name and code', async () => {
    await withPage({}, async (pg) => {
      const p = point('Cincinnati OH');
      await goto(pg, '?lat=' + p.lat + '&lon=' + p.lon);
      await expectCommand(pg, p.expect);
      eq(JSON.stringify(await rows(pg)), JSON.stringify([
        { label: 'Macro region', badge: 'INSIDE', name: 'US Midwest (us-midwest)' },
        { label: 'Mesh region', badge: 'INSIDE', name: 'OKI (oki)' },
        { label: 'State', badge: 'INSIDE', name: 'US-OH (us-oh)' },
        { label: 'Metro', badge: 'INSIDE', name: 'CVG (cvg)' }]), 'rows');
    });
    await withPage({}, async (pg) => {
      const p = point('Detroit MI');
      await goto(pg, '?lat=' + p.lat + '&lon=' + p.lon);
      await expectCommand(pg, p.expect);
      eq(JSON.stringify((await rows(pg)).map((r) => r.label + ':' + r.badge)), JSON.stringify(['Macro region:INSIDE', 'Mesh region:OUTSIDE', 'State:INSIDE', 'Metro:OUTSIDE']), 'non-mesh state rows');
    });
  });

  await step('a point outside every region shows the exact empty message and no command', async () => {
    await withPage({}, async (pg) => {
      await goto(pg, '?lat=35&lon=-70');
      await pg.waitForSelector('.rd-none');
      eq(await text(pg, '.rd-none'), 'This location is not inside any configured region.', 'message');
      ok(!(await pg.$('.rd-cmd')), 'command must not render');
      eq(JSON.stringify((await rows(pg)).map((r) => r.badge)), JSON.stringify(['OUTSIDE', 'OUTSIDE', 'OUTSIDE', 'OUTSIDE']), 'badges');
    });
  });

  await step('clicking the map looks up that spot, fills the input and the deep link', async () => {
    await withPage({}, async (pg) => {
      await goto(pg);
      await pg.waitForSelector('.rd-toggle');
      const box_ = await (await pg.$('#rd-map')).boundingBox();
      await pg.mouse.click(box_.x + box_.width / 2, box_.y + box_.height / 2);
      await pg.waitForFunction(() => /^-?\d+\.\d+, -?\d+\.\d+$/.test(document.querySelector('#rd-input').value));
      ok(/lat=-?\d+\.\d+&lon=-?\d+\.\d+/.test(pg.url()), 'deep link: ' + pg.url());
    });
  });

  await step('empty input asks for an address or coordinates', async () => {
    await withPage({}, async (pg) => {
      await goto(pg);
      await pg.waitForSelector('.rd-toggle');
      await pg.click('#rd-go');
      eq(await text(pg, '#rd-msg'), 'Enter an address or a "lat, lon" pair.', 'message');
    });
  });

  // ---------------------------------------------------------------- geocoder
  await step('address lookup: one result gives the exact command and a lat/lon deep link', async () => {
    await withPage({}, async (pg) => {
      await goto(pg);
      await pg.waitForSelector('.rd-toggle');
      await typeAndGo(pg, '1 Main St Cincinnati');
      await expectCommand(pg, 'us-midwest oki us-oh cvg');
      ok(/lat=39\.1031.*lon=-84\.5120/.test(pg.url()), pg.url());
    });
  });

  await step('address lookup: several results are offered as buttons showing the exact names; picking one resolves it', async () => {
    const list = [{ lat: '39.1031', lon: '-84.5120', display_name: 'Cincinnati, Ohio, USA' }, { lat: '39.7684', lon: '-86.1581', display_name: 'Indianapolis, Indiana, USA' }];
    await withPage({ nominatim: (r) => r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(list) }) }, async (pg) => {
      await goto(pg);
      await pg.waitForSelector('.rd-toggle');
      await typeAndGo(pg, 'springfield');
      await pg.waitForSelector('.rd-cand');
      eq(await text(pg, '#rd-msg'), 'Several matches; pick one:', 'message');
      eq(JSON.stringify(await pg.$$eval('.rd-cand', (b) => b.map((x) => x.textContent))), JSON.stringify(['Cincinnati, Ohio, USA', 'Indianapolis, Indiana, USA']), 'candidates');
      await (await pg.$$('.rd-cand'))[1].click();
      await expectCommand(pg, 'us-midwest oki us-in ind');
      eq((await pg.$$('.rd-cand')).length, 0, 'candidates should be cleared');
    });
  });

  const geocoderFailures = [
    ['HTTP 500', { nominatim: (r) => r.fulfill({ status: 500, body: 'boom' }) }, 'Address lookup failed: the geocoder returned HTTP 500'],
    ['HTTP 429 (rate limited)', { nominatim: (r) => r.fulfill({ status: 429, body: 'slow down' }) }, 'Address lookup failed: the geocoder is rate limiting requests; wait a moment and try again'],
    ['network failure', { nominatim: (r) => r.abort('failed') }, 'Address lookup failed: network error (offline, or the geocoder is blocked?)'],
    ['unreadable reply', { nominatim: (r) => r.fulfill({ status: 200, contentType: 'application/json', body: '<html>not json' }) }, 'Address lookup failed: the geocoder sent an unreadable reply'],
    ['timeout', { nominatim: () => {}, config: { geocodeTimeoutMs: 300 } }, 'Address lookup failed: the geocoder did not answer in time'],
  ];
  for (const [label, o, expected] of geocoderFailures) {
    await step('geocoder failure (' + label + ') shows a plain message, no command, and the form stays usable', async () => {
      await withPage(o, async (pg) => {
        await goto(pg);
        await pg.waitForSelector('.rd-toggle');
        await typeAndGo(pg, 'anywhere');
        eq(await waitMsg(pg, new RegExp('^' + expected.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '$')), expected, 'message');
        ok(!(await pg.$('.rd-cmd')), 'no command on failure');
        ok(!/nominatim|anywhere/i.test(await text(pg, '#rd-msg')), 'message must not echo the request URL or address');
        await pg.waitForFunction(() => !document.querySelector('#rd-go').disabled);
      });
    });
  }

  await step('geocoder returning no results says so exactly', async () => {
    await withPage({ nominatim: (r) => r.fulfill({ status: 200, contentType: 'application/json', body: '[]' }) }, async (pg) => {
      await goto(pg);
      await pg.waitForSelector('.rd-toggle');
      await typeAndGo(pg, 'zzzz nowhere');
      eq(await waitMsg(pg, /No match found/), 'No match found for that address.', 'message');
    });
  });

  // ---------------------------------------------------------------- layer loading failures
  const STATE_404 = { 'states.geojson': { status: 404, body: 'nope' } };
  await step('required layer 404: command withheld, message names the layer and file', async () => {
    await withPage({ respond: STATE_404 }, async (pg) => {
      const p = point('Cincinnati OH');
      await goto(pg, '?lat=' + p.lat + '&lon=' + p.lon);
      await pg.waitForSelector('.rd-incomplete');
      const t = await text(pg, '.rd-incomplete');
      ok(t.indexOf('Command withheld: the result would be incomplete.') === 0, t);
      ok(t.indexOf('Mesh region: HTTP 404 for region-layers/states.geojson') >= 0, t);
      ok(t.indexOf('State: HTTP 404 for region-layers/states.geojson') >= 0, t);
      ok(!(await pg.$('.rd-cmd')), 'command must not render');
    });
  });

  await step('optional layer 404: command still shown, flagged incomplete, missing code named', async () => {
    await withPage({ mutate: (f) => { f['layers.json'].layers[3].optional = true; }, respond: { 'metros.geojson': { status: 404, body: 'nope' } } }, async (pg) => {
      const p = point('Cincinnati OH');
      await goto(pg, '?lat=' + p.lat + '&lon=' + p.lon);
      await pg.waitForSelector('.rd-cmd');
      eq(await text(pg, '.rd-cmd'), cmd('us-midwest oki us-oh'), 'command without the metro');
      eq(await text(pg, '.rd-cmd-head'), 'Type this into your repeater (incomplete)', 'header');
      const t = await text(pg, '.rd-incomplete');
      ok(t.indexOf('Incomplete result: this command is missing codes for optional layers.') === 0 && t.indexOf('Metro: HTTP 404 for region-layers/metros.geojson') >= 0, t);
    });
  });

  await step('malformed GeoJSON file (invalid JSON) is reported by file name', async () => {
    await withPage({ respond: { 'metros.geojson': { status: 200, body: '{"type":"FeatureCollection","features":[{oops' } } }, async (pg) => {
      const p = point('Cincinnati OH');
      await goto(pg, '?lat=' + p.lat + '&lon=' + p.lon);
      await pg.waitForSelector('.rd-incomplete');
      ok((await text(pg, '.rd-incomplete')).indexOf('Metro: invalid JSON in region-layers/metros.geojson') >= 0, await text(pg, '.rd-incomplete'));
      ok(!(await pg.$('.rd-cmd')));
    });
  });

  await step('valid JSON that is not polygon GeoJSON counts as a failed layer, not a loaded one', async () => {
    await withPage({ respond: { 'metros.geojson': { body: { hello: 'world' } } } }, async (pg) => {
      const p = point('Cincinnati OH');
      await goto(pg, '?lat=' + p.lat + '&lon=' + p.lon);
      await pg.waitForSelector('.rd-incomplete');
      ok((await text(pg, '.rd-incomplete')).indexOf('Metro: loaded but contains no valid polygons') >= 0, await text(pg, '.rd-incomplete'));
    });
  });

  await step('layer request timeout: reported as a timeout, command withheld', async () => {
    await withPage({ config: { layerTimeoutMs: 300 }, respond: { 'metros.geojson': { hang: true } } }, async (pg) => {
      const p = point('Cincinnati OH');
      await goto(pg, '?lat=' + p.lat + '&lon=' + p.lon);
      await pg.waitForSelector('.rd-incomplete');
      ok(/Metro: timed out after 300 ms: region-layers\/metros\.geojson/.test(await text(pg, '.rd-incomplete')), await text(pg, '.rd-incomplete'));
      ok(!(await pg.$('.rd-cmd')));
    });
  });

  await step('layers.json problems stop the page with an exact message: 404, invalid JSON, timeout, newer version, unsafe path', async () => {
    const cases = [
      [{ respond: { 'layers.json': { status: 404, body: 'nope' } } }, 'Could not load region layers: HTTP 404 for region-layers/layers.json'],
      [{ respond: { 'layers.json': { status: 200, body: '{ nope' } } }, 'Could not load region layers: invalid JSON in region-layers/layers.json'],
      [{ config: { layerTimeoutMs: 300 }, respond: { 'layers.json': { hang: true } } }, 'Could not load region layers: timed out after 300 ms: region-layers/layers.json'],
      [{ mutate: (f) => { f['layers.json'].version = 99; } }, 'Could not load region layers: layers.json is invalid: layers.json version 99 is newer than this page supports (1).'],
      [{ mutate: (f) => { f['layers.json'].layers[3].url = '../secret.json'; } }, null],
    ];
    for (const [o, expected] of cases) {
      await withPage(o, async (pg, hits) => {
        await goto(pg);
        if (expected) eq(await waitMsg(pg, new RegExp('^' + expected.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '$')), expected, 'message');
        else {
          const m = await waitMsg(pg, /unsafe or invalid file path/);
          ok(/^Could not load region layers: layers\.json is invalid: layer 4 \(metro\): unsafe or invalid file path "\.\.\/secret\.json"\.$/.test(m), m);
          ok(!hits['../secret.json'] && !hits['secret.json'], 'traversal path must never be requested');
        }
        ok(await pg.$eval('#rd-go', (b) => b.disabled), 'lookup must stay disabled when nothing loaded');
        ok(!(await pg.$('.rd-cmd')));
      });
    }
  });

  await step('a file used by several layers is fetched exactly once', async () => {
    await withPage({}, async (pg, hits) => {
      await goto(pg, '?lat=39.1031&lon=-84.5120');
      await pg.waitForSelector('.rd-cmd');
      eq(hits['states.geojson'], 1, 'states.geojson requests (used by the mesh and state layers)');
    });
  });

  // ---------------------------------------------------------------- overlaps, gaps, escaping
  await step('overlapping regions: first wins, the warning names both, output stays complete', async () => {
    await withPage({ mutate: (f) => {
      f['metros.geojson'].features.push(polyFeature({ iata: 'LUK' }, box(-84.8, 38.95, -84.3, 39.25)));
      f['layers.json'].layers[3].codes.push('luk'); f['layers.json'].layers[3].allowOverlap = true;
    } }, async (pg) => {
      const p = point('Cincinnati OH');
      await goto(pg, '?lat=' + p.lat + '&lon=' + p.lon);
      await pg.waitForSelector('.rd-cmd');
      eq(await text(pg, '.rd-cmd'), cmd(p.expect), 'first match wins');
      eq(await text(pg, '.rd-warnings'), 'Metro: point is in 2 overlapping regions (cvg, luk); using cvg.', 'warning');
      eq((await rows(pg))[3].name, 'CVG (cvg) - overlaps 1 more', 'row text');
    });
  });

  await step('coverage:"full" mesh layer: a state outside the mesh region is a data gap and the command is withheld', async () => {
    await withPage({ mutate: (f) => { f['layers.json'].layers[1].coverage = 'full'; } }, async (pg) => {
      const p = point('Detroit MI');
      await goto(pg, '?lat=' + p.lat + '&lon=' + p.lon);
      await pg.waitForSelector('.rd-incomplete');
      ok((await text(pg, '.rd-incomplete')).indexOf('Mesh region: no region covers this point, but the layer declares full coverage (data gap)') >= 0, await text(pg, '.rd-incomplete'));
      ok(!(await pg.$('.rd-cmd')));
    });
  });

  await step('HTML in geocoder names and in boundary data is displayed as literal text, never parsed', async () => {
    const evil = [{ lat: '39.1031', lon: '-84.5120', display_name: '<img src=x onerror="window.__xss=1">Evil St' }, { lat: '39.7684', lon: '-86.1581', display_name: '<b>bold</b> & <script>window.__xss=2</script>' }];
    await withPage({ nominatim: (r) => r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(evil) }),
      mutate: (f) => { f['metros.geojson'].features[0].properties.iata = '<i>x</i>'; delete f['layers.json'].layers[3].codes; f['known-points.json'] = []; } }, async (pg) => {
      await goto(pg);
      await pg.waitForSelector('.rd-toggle');
      await typeAndGo(pg, 'evil');
      await pg.waitForSelector('.rd-cand');
      eq(JSON.stringify(await pg.$$eval('.rd-cand', (b) => b.map((x) => x.textContent))), JSON.stringify(evil.map((e) => e.display_name)), 'candidate text is the literal string');
      eq(await pg.$$eval('.rd-cand img, .rd-cand b, .rd-cand script', (n) => n.length), 0, 'no elements created from candidate text');
      await (await pg.$$('.rd-cand'))[0].click();
      await pg.waitForSelector('.rd-cmd');
      eq(await text(pg, '.rd-where'), evil[0].display_name, 'result heading is literal');
      eq(await pg.$$eval('.rd-where img, .rd-row-name i', (n) => n.length), 0, 'no elements created from the heading or boundary names');
      ok((await rows(pg))[3].name.indexOf('<i>x</i>') === 0, 'boundary name shown literally: ' + (await rows(pg))[3].name);
      eq(await pg.evaluate(() => window.__xss), undefined, 'no injected script ran');
    });
  });

  // ---------------------------------------------------------------- mobile layouts
  for (const vp of [{ width: 320, height: 640 }, { width: 375, height: 667 }, { width: 768, height: 1024 }]) {
    await step('mobile layout at ' + vp.width + 'x' + vp.height + ': no overflow, command fully on screen, map usable, details collapsed', async () => {
      await withPage({ viewport: vp }, async (pg) => {
        const p = point('Cincinnati OH');
        await goto(pg, '?lat=' + p.lat + '&lon=' + p.lon);
        await expectCommand(pg, p.expect);
        const m = await pg.evaluate(() => {
          const r = document.querySelector('.rd-cmd').getBoundingClientRect();
          const map = document.querySelector('#rd-map').getBoundingClientRect();
          return { sw: document.documentElement.scrollWidth, vw: innerWidth, vh: innerHeight, left: r.left, right: r.right, top: r.top, bottom: r.bottom,
            mapH: map.height, mapW: map.width, details: document.querySelector('.rd-details').open,
            smallTargets: [].slice.call(document.querySelectorAll('.rd-input-row .btn, .rd-details > summary')).filter((e) => e.getBoundingClientRect().height < 44).length };
        });
        ok(m.sw <= m.vw, 'horizontal overflow ' + m.sw + ' > ' + m.vw);
        ok(m.left >= 0 && m.right <= m.vw, 'command overflows horizontally: ' + m.left + '..' + m.right);
        ok(m.top >= 0 && m.bottom <= m.vh, 'command not fully visible without scrolling: ' + m.top + '..' + m.bottom + ' of ' + m.vh);
        ok(m.mapH >= 280 && m.mapW >= vp.width - 40, 'map too small: ' + m.mapW + 'x' + m.mapH);
        eq(m.details, false, 'details start collapsed on narrow screens');
        eq(m.smallTargets, 0, 'touch targets under 44px');
      });
    });
  }

  await step('desktop layout on a small laptop (1024x600): panel scrolls, no page overflow, map visible', async () => {
    await withPage({ viewport: { width: 1024, height: 600 } }, async (pg) => {
      const p = point('Cincinnati OH');
      await goto(pg, '?lat=' + p.lat + '&lon=' + p.lon);
      await expectCommand(pg, p.expect);
      const m = await pg.evaluate(() => ({ sw: document.documentElement.scrollWidth, vw: innerWidth, mapH: document.querySelector('#rd-map').getBoundingClientRect().height,
        panelW: document.querySelector('.rd-panel').getBoundingClientRect().width, open: document.querySelector('.rd-details').open }));
      ok(m.sw <= m.vw, 'horizontal overflow');
      ok(m.mapH >= 400, 'map height ' + m.mapH);
      ok(m.panelW >= 300 && m.panelW <= 380, 'panel width ' + m.panelW);
      eq(m.open, true, 'details open on desktop');
    });
  });

  // ---------------------------------------------------------------- production-data smoke
  await step('production smoke: deployed region data (if any) loads cleanly and every known point yields its exact command', async () => {
    const probe = await ctx.request.get(BASE + '/region-layers/layers.json');
    if (probe.status() !== 200) { console.log('       (no region data deployed at ' + BASE + '/region-layers/: skipped)'); return; }
    const manifest = await probe.json();
    const kp = await ctx.request.get(BASE + '/region-layers/known-points.json');
    ok(kp.status() === 200, 'region data is deployed but known-points.json is not served: add at least one point per mapped state and one on a shared border');
    const points = await kp.json();
    ok(Array.isArray(points) && points.length > 0, 'known-points.json is empty');
    const pg = await ctx.newPage();
    const errs = [];
    pg.on('pageerror', (e) => errs.push(e.message));
    for (const p of points) {
      await goto(pg, '?lat=' + p.lat + '&lon=' + p.lon);
      await pg.reload();
      await pg.waitForSelector('.rd-toggle', { timeout: 20000 });
      eq((await pg.$$('.rd-toggle')).length, manifest.layers.length, 'layer toggles (' + p.name + ')');
      if (p.expect === '') { await pg.waitForSelector('.rd-none'); ok(!(await pg.$('.rd-cmd')), p.name + ': expected no command'); }
      else { await pg.waitForSelector('.rd-cmd'); eq(await text(pg, '.rd-cmd'), cmd(p.expect), p.name); }
      ok(!(await pg.$('.rd-incomplete')), p.name + ': deployed data produced an incomplete result: ' + ((await pg.$('.rd-incomplete')) ? await text(pg, '.rd-incomplete') : ''));
      if (!p.allowWarnings) ok(!(await pg.$('.rd-warnings')), p.name + ': unexpected warnings: ' + ((await pg.$('.rd-warnings')) ? await text(pg, '.rd-warnings') : ''));
    }
    await pg.close();
    ok(errs.length === 0, 'uncaught page error: ' + errs[0]);
  });

  await browser.close();
  console.log('\n' + passed + ' passed, ' + failed + ' failed');
  process.exit(failed ? 1 : 0);
})().catch((e) => { console.error('test-region-def-e2e.js: crashed: ' + (e && e.stack || e)); process.exit(1); });
