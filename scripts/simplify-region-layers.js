#!/usr/bin/env node
/* Simplify Region Def boundary files so the page downloads and draws less.
 *
 *   node scripts/simplify-region-layers.js --in DIR --out DIR [--tolerance 0.0005] [--precision 5]
 *
 * Each polygon ring is reduced with Douglas-Peucker (tolerance in degrees; 0.0005 is about
 * 55 m) and coordinates are rounded to `precision` decimals (5 is about 1 m) and written without
 * whitespace. layers.json and known-points.json are copied unchanged.
 *
 * Safety net: the output directory must pass scripts/check-region-data.js (including every
 * known point producing exactly the same command) or the script exits 1. Input that already
 * fails the check is refused: fix the data first, then simplify.
 *
 * Rings are simplified independently, so two neighbours can drift apart by up to the tolerance
 * along a shared border. The check ignores overlaps shallower than 2x the tolerance, and a
 * point inside such a sliver can resolve to either neighbour. Pick a tolerance well below the
 * accuracy you need; this is about page weight, not about surveying.
 */
'use strict';
const fs = require('fs');
const path = require('path');
const { checkRegionData } = require('./check-region-data.js');

function dp(points, tol, k) {
  // points: open path of [lon, lat]; returns kept points (first and last always kept)
  const n = points.length;
  if (n <= 2) return points.slice();
  const keep = new Uint8Array(n);
  keep[0] = keep[n - 1] = 1;
  const stack = [[0, n - 1]];
  while (stack.length) {
    const [a, b] = stack.pop();
    let best = -1, bi = -1;
    const ax = points[a][0] * k, ay = points[a][1], bx = points[b][0] * k, by = points[b][1];
    const dx = bx - ax, dy = by - ay, len2 = dx * dx + dy * dy;
    for (let i = a + 1; i < b; i++) {
      const px = points[i][0] * k, py = points[i][1];
      let d;
      if (len2 === 0) d = Math.hypot(px - ax, py - ay);
      else {
        let t = ((px - ax) * dx + (py - ay) * dy) / len2;
        t = Math.max(0, Math.min(1, t));
        d = Math.hypot(px - (ax + t * dx), py - (ay + t * dy));
      }
      if (d > best) { best = d; bi = i; }
    }
    if (best > tol) { keep[bi] = 1; stack.push([a, bi], [bi, b]); }
  }
  return points.filter((_, i) => keep[i]);
}

function round(v, p) { const m = Math.pow(10, p); return Math.round(v * m) / m; }

function simplifyRing(ring, tol, precision) {
  const open = ring.slice(0, -1);
  const meanLat = open.reduce((s, c) => s + c[1], 0) / open.length;
  const k = Math.cos(meanLat * Math.PI / 180);
  // anchor the ring at its two farthest-apart vertices so the closing seam is simplified like any other edge
  let ia = 0, ib = 0, far = -1;
  for (let i = 1; i < open.length; i++) {
    const d = Math.hypot((open[i][0] - open[0][0]) * k, open[i][1] - open[0][1]);
    if (d > far) { far = d; ib = i; }
  }
  const rot = open.slice(ia).concat(open.slice(0, ia));
  const half1 = dp(rot.slice(0, ib + 1), tol, k);
  const half2 = dp(rot.slice(ib).concat([rot[0]]), tol, k);
  let pts = half1.slice(0, -1).concat(half2);
  pts = pts.map((c) => [round(c[0], precision), round(c[1], precision)]);
  const dedup = [];
  pts.forEach((c) => { const l = dedup[dedup.length - 1]; if (!l || l[0] !== c[0] || l[1] !== c[1]) dedup.push(c); });
  if (dedup[0][0] !== dedup[dedup.length - 1][0] || dedup[0][1] !== dedup[dedup.length - 1][1]) dedup.push(dedup[0].slice());
  return dedup.length >= 4 ? dedup : null; // collapsed: caller keeps the original
}

function simplifyGeometry(geom, tol, precision, stats) {
  const polys = geom.type === 'Polygon' ? [geom.coordinates] : geom.coordinates;
  const out = polys.map((poly) => poly.map((ring) => {
    stats.before += ring.length;
    const s = simplifyRing(ring, tol, precision);
    if (!s) { stats.kept++; stats.after += ring.length; return ring.map((c) => [round(c[0], precision), round(c[1], precision)]); }
    stats.after += s.length;
    return s;
  }));
  return geom.type === 'Polygon' ? { type: 'Polygon', coordinates: out[0] } : { type: 'MultiPolygon', coordinates: out };
}

function simplifyGeoJSON(gj, tol, precision, stats) {
  const feats = gj.type === 'FeatureCollection' ? gj.features : gj.type === 'Feature' ? [gj] : [{ type: 'Feature', properties: {}, geometry: gj }];
  return {
    type: 'FeatureCollection',
    features: feats.map((f) => {
      if (!f || !f.geometry || (f.geometry.type !== 'Polygon' && f.geometry.type !== 'MultiPolygon')) return f;
      return { type: 'Feature', properties: f.properties || {}, geometry: simplifyGeometry(f.geometry, tol, precision, stats) };
    })
  };
}

function run(inDir, outDir, opts) {
  opts = opts || {};
  const tol = opts.tolerance === undefined ? 0.0005 : opts.tolerance;
  const precision = opts.precision === undefined ? 5 : opts.precision;
  const log = opts.log || function () {};
  const result = { ok: false, errors: [], files: [] };
  if (path.resolve(inDir) === path.resolve(outDir)) { result.errors.push('--in and --out must be different directories'); return result; }
  const before = checkRegionData(inDir);
  if (!before.checked) { result.errors.push('no layers.json in ' + inDir); return result; }
  if (before.errors.length) { result.errors = ['input fails the integrity check, fix it first:'].concat(before.errors); return result; }
  fs.mkdirSync(outDir, { recursive: true });
  const names = {};
  before.manifest.layers.forEach((l) => (Array.isArray(l.url) ? l.url : [l.url]).forEach((u) => { names[typeof u === 'object' ? u.url : u] = true; }));
  Object.keys(names).forEach((f) => {
    const src = fs.readFileSync(path.join(inDir, f), 'utf8');
    const stats = { before: 0, after: 0, kept: 0 };
    fs.mkdirSync(path.dirname(path.join(outDir, f)), { recursive: true });
    const outTxt = JSON.stringify(simplifyGeoJSON(JSON.parse(src), tol, precision, stats));
    fs.writeFileSync(path.join(outDir, f), outTxt + '\n');
    result.files.push({ file: f, bytesBefore: Buffer.byteLength(src), bytesAfter: Buffer.byteLength(outTxt) + 1, vertsBefore: stats.before, vertsAfter: stats.after, collapsedKept: stats.kept });
  });
  ['layers.json', 'known-points.json'].forEach((f) => { if (fs.existsSync(path.join(inDir, f))) fs.copyFileSync(path.join(inDir, f), path.join(outDir, f)); });
  const after = checkRegionData(outDir, { overlapTolerance: tol * 2 });
  result.warnings = after.warnings;
  if (after.errors.length) {
    result.errors = ['simplified output fails the integrity check (tolerance ' + tol + ' is too coarse for this data, or a known point moved):'].concat(after.errors);
    return result;
  }
  result.ok = true;
  return result;
}

function main(argv) {
  let inDir, outDir, tolerance, precision;
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--in') inDir = path.resolve(argv[++i]);
    else if (argv[i] === '--out') outDir = path.resolve(argv[++i]);
    else if (argv[i] === '--tolerance') tolerance = parseFloat(argv[++i]);
    else if (argv[i] === '--precision') precision = parseInt(argv[++i], 10);
    else { console.error('unknown argument ' + argv[i]); return 2; }
  }
  if (!inDir || !outDir) { console.error('usage: simplify-region-layers.js --in DIR --out DIR [--tolerance DEG] [--precision N]'); return 2; }
  const r = run(inDir, outDir, { tolerance: tolerance, precision: precision });
  r.files.forEach((f) => console.log('  ' + f.file + ': ' + f.vertsBefore + ' -> ' + f.vertsAfter + ' vertices, ' + f.bytesBefore + ' -> ' + f.bytesAfter + ' bytes'));
  (r.warnings || []).forEach((w) => console.log('WARN  ' + w));
  r.errors.forEach((e) => console.log('ERROR ' + e));
  console.log(r.ok ? 'simplify-region-layers: OK (output passes check-region-data, known points unchanged)' : 'simplify-region-layers: FAIL');
  return r.ok ? 0 : 1;
}

module.exports = { run: run, simplifyRing: simplifyRing };
if (require.main === module) process.exit(main(process.argv.slice(2)));
