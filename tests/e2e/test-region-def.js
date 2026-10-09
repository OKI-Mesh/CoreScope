#!/usr/bin/env node
/* Region Def (#/tools/region-def) E2E.
 *
 * The page reads operator-supplied files from /region-layers/. This suite
 * intercepts those requests with small fixtures, so it does not depend on what
 * (if anything) the deployment ships there. Nominatim is intercepted too: no test
 * ever touches the real service.
 *
 * CHROMIUM_REQUIRE=1 makes Chromium-launch failure a HARD FAIL.
 */
'use strict';
const { chromium } = require('playwright');

const BASE = process.env.BASE_URL || 'http://localhost:13581';
let passed = 0, failed = 0;
async function step(name, fn) {
  try { await fn(); passed++; console.log('  ok  ' + name); }
  catch (e) { failed++; console.error('  FAIL ' + name + ': ' + e.message); }
}
function assert(c, m) { if (!c) throw new Error(m || 'assertion failed'); }

const sq = (x0, y0, x1, y1) => [[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]];
const fc = (...f) => ({ type: 'FeatureCollection', features: f });
const feat = (props, ring) => ({ type: 'Feature', properties: props, geometry: { type: 'Polygon', coordinates: [ring] } });

// Cincinnati is about (39.10, -84.51)
const FIXTURES = {
  'layers.json': { layers: [
    { id: 'macro', label: 'Macro region', url: [{ url: 'macro.geojson', name: 'US-Midwest' }] },
    { id: 'state', label: 'State', url: 'states.geojson', nameProperty: 'name' },
    { id: 'metro', label: 'Metro', url: 'metros.geojson', nameProperty: 'iata', colorBy: 'feature' },
  ] },
  'macro.geojson': fc(feat({}, sq(-90, 35, -80, 45))),
  'states.geojson': fc(feat({ name: 'Ohio' }, sq(-85, 38, -80, 42)), feat({ name: 'Indiana' }, sq(-90, 38, -85, 42))),
  'metros.geojson': fc(feat({ iata: 'CVG', color: '#db2777' }, sq(-85, 38.5, -84, 39.5))),
};

async function routeFixtures(page) {
  await page.route('**/region-layers/**', (route) => {
    const name = route.request().url().split('/region-layers/')[1].split('?')[0];
    if (FIXTURES[name]) return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(FIXTURES[name]) });
    return route.fulfill({ status: 404, body: 'nope' });
  });
  await page.route('https://nominatim.openstreetmap.org/**', (route) =>
    route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify([{ lat: '39.10', lon: '-84.51', display_name: 'Cincinnati, Ohio, USA' }]) }));
}

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
  const ctx = await browser.newContext({ viewport: { width: 1400, height: 900 } });
  const page = await ctx.newPage();
  page.setDefaultTimeout(15000);
  page.on('pageerror', (e) => console.error('[pageerror]', e.message));
  await routeFixtures(page);

  console.log('\n=== region-def E2E against ' + BASE + ' ===');

  await step('Tools landing links to Region Def', async () => {
    await page.goto(BASE + '/#/tools', { waitUntil: 'domcontentloaded' });
    await page.waitForSelector('.tools-card');
    assert(await page.$('.tools-card[href="#/tools/region-def"]'), 'card missing');
  });

  await step('page renders and loads layers', async () => {
    await page.goto(BASE + '/#/tools/region-def', { waitUntil: 'domcontentloaded' });
    await page.waitForSelector('.rd-page');
    await page.waitForSelector('.rd-toggle');
    assert((await page.$$('.rd-toggle')).length === 3, 'expected 3 layer toggles');
    assert(await page.$('#rd-map .leaflet-container, #rd-map.leaflet-container'), 'leaflet map missing');
  });

  await step('lat/lon lookup yields ordered region def block', async () => {
    await page.fill('#rd-input', '39.10, -84.51');
    await page.click('#rd-go');
    await page.waitForSelector('.rd-cmd');
    const txt = await page.textContent('.rd-cmd');
    assert(txt === 'region def us-midwest ohio cvg\nregion save\nregion allowf *', 'got: ' + JSON.stringify(txt));
    assert(/Type this into your repeater/.test(await page.textContent('.rd-cmd-head')), 'header missing');
    assert(/lat=39\.1/.test(page.url()), 'deep link not updated: ' + page.url());
  });

  await step('address lookup (mocked geocoder) fills the same result', async () => {
    await page.goto(BASE + '/#/tools/region-def', { waitUntil: 'domcontentloaded' });
    await page.waitForSelector('.rd-toggle');
    await page.fill('#rd-input', '1 Main St Cincinnati');
    await page.press('#rd-input', 'Enter');
    await page.waitForSelector('.rd-cmd');
    assert(/ohio cvg/.test(await page.textContent('.rd-cmd')), 'expected ohio cvg');
  });

  await step('deep link ?lat=&lon= auto-runs', async () => {
    await page.goto(BASE + '/#/tools/region-def?lat=39.1&lon=-87', { waitUntil: 'domcontentloaded' });
    await page.waitForSelector('.rd-cmd');
    assert((await page.textContent('.rd-cmd')).startsWith('region def us-midwest indiana\n'), 'expected indiana, no metro');
  });

  await step('outside every region shows the empty message and no command', async () => {
    await page.goto(BASE + '/#/tools/region-def?lat=10&lon=10', { waitUntil: 'domcontentloaded' });
    await page.waitForSelector('.rd-none');
    assert(!(await page.$('.rd-cmd')), 'command block must not render');
  });

  await step('address text is never interpreted as HTML', async () => {
    await page.goto(BASE + '/#/tools/region-def?q=%3Cimg%20src%3Dx%20onerror%3Dwindow.__xss%3D1%3E', { waitUntil: 'domcontentloaded' });
    await page.waitForSelector('.rd-toggle');
    await page.waitForTimeout(1500);
    assert(!(await page.evaluate(() => window.__xss)), 'script ran');
  });

  await browser.close();
  console.log('\n' + passed + ' passed, ' + failed + ' failed');
  process.exit(failed ? 1 : 0);
})();