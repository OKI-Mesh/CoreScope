#!/usr/bin/env node
/* Region Def data integrity check.
 *
 * Validates the manifest and the boundary files that are actually deployed in
 * public/region-layers/ (or --dir), the same files the page loads:
 *
 *   - layers.json is valid (schema version, ids, safe paths, codes ...)
 *   - every referenced file exists and is well-formed GeoJSON with polygons
 *     (closed rings, finite [lon, lat] numbers, in range: swapped [lat, lon] is caught)
 *   - every polygon produces a valid canonical repeater code, inside the layer's
 *     declared "codes" list, and every declared code is produced by some polygon
 *   - no code appears twice inside a layer
 *   - polygons of one layer do not overlap (shared edges are fine; sliver overlaps
 *     under --overlap-tolerance are ignored). Opt out per layer with "allowOverlap": true
 *   - the same code is not reused by two layers (the repeater command would repeat it)
 *   - known-points.json (if present) produces exactly the expected command, with no warnings
 *
 * Usage:
 *   node scripts/check-region-data.js [--dir DIR] [--strict] [--overlap-tolerance DEG]
 *                                     [--point LAT,LON]...
 *   --strict   also fail on warnings
 *   --point    print the `region def` block for a coordinate (use it to produce verified
 *              examples for docs and known-points.json)
 *
 * With no layers.json in DIR there is nothing to check: exits 0 (a deployment may serve
 * its data from elsewhere).
 *
 * Exports checkRegionData(dir, opts) for tests.
 */
'use strict';
const fs = require('fs');
const path = require('path');
const R = require('../public/region-def-core.js');

const DEFAULT_DIR = path.join(__dirname, '..', 'public', 'region-layers');
const DEFAULT_TOL = 0.0005; // degrees, about 55 m: ignore misalignment slivers along shared borders

function readJSON(file, errors, label) {
  let txt;
  try { txt = fs.readFileSync(file, 'utf8'); } catch (e) { errors.push(label + ': cannot read file (' + e.code + ')'); return undefined; }
  try { return JSON.parse(txt); } catch (e) { errors.push(label + ': invalid JSON (' + e.message + ')'); return undefined; }
}

// Structural geometry problems the lookup would silently turn into wrong answers.
function checkGeometry(feature, where, errors) {
  const g = feature.geometry;
  const polys = g.type === 'Polygon' ? [g.coordinates] : g.coordinates;
  for (let p = 0; p < polys.length; p++) {
    const rings = polys[p];
    if (!Array.isArray(rings) || !rings.length) { errors.push(where + ': polygon without rings'); return; }
    for (let r = 0; r < rings.length; r++) {
      const ring = rings[r];
      if (!Array.isArray(ring) || ring.length < 4) { errors.push(where + ': ring with fewer than 4 positions'); return; }
      const a = ring[0], b = ring[ring.length - 1];
      if (a[0] !== b[0] || a[1] !== b[1]) { errors.push(where + ': ring is not closed'); return; }
      for (let i = 0; i < ring.length; i++) {
        const x = ring[i][0], y = ring[i][1];
        if (typeof x !== 'number' || typeof y !== 'number' || !isFinite(x) || !isFinite(y)) { errors.push(where + ': non-numeric coordinate'); return; }
        if (y < -90 || y > 90) { errors.push(where + ': latitude ' + y + ' out of range (coordinates must be [lon, lat], these look swapped)'); return; }
        if (x < -180 || x > 180) { errors.push(where + ': longitude ' + x + ' out of range'); return; }
      }
    }
  }
}

function strictlyInside(geom, lon, lat) {
  const polys = geom.type === 'Polygon' ? [geom.coordinates] : geom.coordinates;
  for (let i = 0; i < polys.length; i++) {
    const poly = polys[i];
    if (R.ringLocate(lon, lat, poly[0]) !== 1) continue;
    let inHole = false;
    for (let h = 1; h < poly.length; h++) if (R.ringLocate(lon, lat, poly[h]) !== 0) { inHole = true; break; }
    if (!inHole) return true;
  }
  return false;
}

function distToRingsDeg(geom, lon, lat) {
  const k = Math.cos(lat * Math.PI / 180);
  let best = Infinity;
  const polys = geom.type === 'Polygon' ? [geom.coordinates] : geom.coordinates;
  polys.forEach((poly) => poly.forEach((ring) => {
    for (let i = 1; i < ring.length; i++) {
      const ax = ring[i - 1][0] * k, ay = ring[i - 1][1], bx = ring[i][0] * k, by = ring[i][1], px = lon * k, py = lat;
      const dx = bx - ax, dy = by - ay, len2 = dx * dx + dy * dy;
      let t = len2 ? ((px - ax) * dx + (py - ay) * dy) / len2 : 0;
      t = Math.max(0, Math.min(1, t));
      const d = Math.hypot(px - (ax + t * dx), py - (ay + t * dy));
      if (d < best) best = d;
    }
  }));
  return best;
}

// Vertices and edge midpoints of A, thinned to at most `max` samples.
function samples(geom, max) {
  const pts = [];
  const polys = geom.type === 'Polygon' ? [geom.coordinates] : geom.coordinates;
  polys.forEach((poly) => poly.forEach((ring) => {
    for (let i = 1; i < ring.length; i++) {
      pts.push(ring[i]);
      pts.push([(ring[i - 1][0] + ring[i][0]) / 2, (ring[i - 1][1] + ring[i][1]) / 2]);
    }
  }));
  const step = Math.max(1, Math.ceil(pts.length / max));
  return pts.filter((_, i) => i % step === 0);
}

function overlapDepth(a, b, tol) {
  // is a sample of a deeper than `tol` inside b?
  const ba = a.bbox, bb = b.bbox;
  if (ba[0] > bb[2] || ba[2] < bb[0] || ba[1] > bb[3] || ba[3] < bb[1]) return 0;
  let checked = 0, deepest = 0;
  const pts = samples(a.feature.geometry, 3000);
  for (let i = 0; i < pts.length; i++) {
    const x = pts[i][0], y = pts[i][1];
    if (x < bb[0] || x > bb[2] || y < bb[1] || y > bb[3]) continue;
    if (!strictlyInside(b.feature.geometry, x, y)) continue;
    if (++checked > 200) break;
    const d = distToRingsDeg(b.feature.geometry, x, y);
    if (d > tol && d > deepest) deepest = d;
  }
  return deepest;
}

function checkRegionData(dir, opts) {
  opts = opts || {};
  const tol = typeof opts.overlapTolerance === 'number' ? opts.overlapTolerance : DEFAULT_TOL;
  const errors = [], warnings = [], summary = [];
  const out = { dir: dir, errors: errors, warnings: warnings, summary: summary, layers: [], manifest: null, known: [], checked: false };

  const manifestFile = path.join(dir, opts.manifestName || 'layers.json');
  if (!fs.existsSync(manifestFile)) return out;
  out.checked = true;
  const manifest = readJSON(manifestFile, errors, 'layers.json');
  if (manifest === undefined) return out;
  out.manifest = manifest;
  const v = R.validateManifest(manifest);
  v.errors.forEach((e) => errors.push('layers.json: ' + e));
  v.warnings.forEach((w) => warnings.push('layers.json: ' + w));
  if (v.errors.length) return out;
  if (manifest.version === undefined) errors.push('layers.json: set "version": ' + R.MANIFEST_VERSION + ' explicitly');

  const cache = {};
  const codeOwner = {};
  manifest.layers.forEach((def) => {
    const at = 'layer "' + def.id + '"';
    const raw = Array.isArray(def.url) ? def.url : [def.url];
    const files = [];
    let ok = true;
    raw.forEach((u) => {
      const f = (u && typeof u === 'object') ? u : { url: u };
      if (!(f.url in cache)) cache[f.url] = readJSON(path.join(dir, f.url), errors, at + ' file ' + f.url);
      const data = cache[f.url];
      if (data === undefined) { ok = false; return; }
      const feats = R.normalizeGeoJSON(data);
      if (!feats.length) { errors.push(at + ' file ' + f.url + ': no Polygon/MultiPolygon features'); ok = false; return; }
      feats.forEach((ft, i) => checkGeometry(ft, at + ' file ' + f.url + ' feature ' + (i + 1), errors));
      files.push({ name: f.name || null, code: f.code || null, data: data });
    });
    if (!ok) return;
    const layer = R.buildLayer(def, files);
    out.layers.push({ def: def, layer: layer });
    if (!layer.features.length) { errors.push(at + ': no usable polygons'); return; }
    if (layer.unnamed) {
      const names = layer.features.filter((e) => !e.code).slice(0, 5).map((e) => JSON.stringify(e.name || '(unnamed)')).join(', ');
      errors.push(at + ': ' + layer.unnamed + ' polygon(s) cannot produce a valid canonical code [' + JSON.stringify(layer.codeProblems) + '] e.g. ' + names +
        '. Codes are lowercase a-z 0-9 _ - and must be in the layer\'s "codes" list when one is declared.');
    }
    const seen = {};
    layer.features.forEach((e) => {
      if (!e.code) return;
      if (seen[e.code]) errors.push(at + ': code "' + e.code + '" is produced by more than one polygon (merge them with groupBy or a file "name")');
      seen[e.code] = true;
      if (codeOwner[e.code] && codeOwner[e.code] !== def.id) warnings.push('code "' + e.code + '" is used by both layer "' + codeOwner[e.code] + '" and "' + def.id + '"; region def would repeat it');
      codeOwner[e.code] = codeOwner[e.code] || def.id;
    });
    if (Array.isArray(def.codes)) {
      def.codes.forEach((c) => { if (!seen[c]) errors.push(at + ': canonical code "' + c + '" is declared but no polygon produces it (missing region or wrong property?)'); });
    }
    if (!def.allowOverlap) {
      const withCode = layer.features.filter((e) => e.code);
      for (let i = 0; i < withCode.length; i++) {
        for (let j = i + 1; j < withCode.length; j++) {
          const d = Math.max(overlapDepth(withCode[i], withCode[j], tol), overlapDepth(withCode[j], withCode[i], tol));
          if (d > 0) errors.push(at + ': "' + withCode[i].code + '" and "' + withCode[j].code + '" overlap (up to ~' + Math.round(d * 111000) + ' m deep). Fix the data or set "allowOverlap": true if intended');
        }
      }
    }
    summary.push(def.id + ': ' + layer.features.length + ' region(s), ' + layer.vertices + ' vertices, codes: ' + layer.features.map((e) => e.code || '(none)').join(' '));
  });

  const kp = path.join(dir, 'known-points.json');
  if (fs.existsSync(kp) && out.layers.length === manifest.layers.length && !errors.length) {
    const points = readJSON(kp, errors, 'known-points.json');
    if (Array.isArray(points)) {
      points.forEach((p) => {
        if (!p || typeof p.name !== 'string' || typeof p.expect !== 'string' || !R.parseLatLon(p.lat + ',' + p.lon)) { errors.push('known-points.json: malformed entry ' + JSON.stringify(p)); return; }
        const r = R.buildRegionDef(out.layers.map((x) => x.layer), p.lat, p.lon);
        out.known.push({ name: p.name, expect: p.expect, got: r.value, warnings: r.warnings });
        if (r.value !== p.expect) errors.push('known point "' + p.name + '": expected "' + p.expect + '" but got "' + r.value + '"');
        else if (r.warnings.length && !p.allowWarnings) errors.push('known point "' + p.name + '": unexpected warning: ' + r.warnings.join(' | '));
      });
    } else if (points !== undefined) errors.push('known-points.json: must be an array');
  }
  return out;
}

function main(argv) {
  let dir = DEFAULT_DIR, strict = false, tol = DEFAULT_TOL;
  const points = [];
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--dir') dir = path.resolve(argv[++i]);
    else if (argv[i] === '--strict') strict = true;
    else if (argv[i] === '--overlap-tolerance') tol = parseFloat(argv[++i]);
    else if (argv[i] === '--point') points.push(argv[++i]);
    else { console.error('unknown argument ' + argv[i]); return 2; }
  }
  const res = checkRegionData(dir, { overlapTolerance: tol });
  if (!res.checked) { console.log('check-region-data: no layers.json in ' + dir + ' - nothing to check'); return 0; }
  res.summary.forEach((s) => console.log('  ' + s));
  res.warnings.forEach((w) => console.log('WARN  ' + w));
  res.errors.forEach((e) => console.log('ERROR ' + e));
  points.forEach((s) => {
    const ll = R.parseLatLon(s);
    if (!ll) { console.log('--point ' + s + ': not a lat,lon pair'); return; }
    const r = R.buildRegionDef(res.layers.map((x) => x.layer), ll.lat, ll.lon);
    console.log('\npoint ' + ll.lat + ', ' + ll.lon + (r.value ? '' : '  (not inside any region)'));
    if (r.value) console.log(R.repeaterCommands(r.value).split('\n').map((l) => '  ' + l).join('\n'));
    r.warnings.forEach((w) => console.log('  warning: ' + w));
  });
  const bad = res.errors.length || (strict && res.warnings.length);
  console.log('\ncheck-region-data: ' + (bad ? 'FAIL' : 'OK') + ' - ' + res.errors.length + ' error(s), ' + res.warnings.length + ' warning(s), ' + res.known.length + ' known point(s)');
  return bad ? 1 : 0;
}

module.exports = { checkRegionData: checkRegionData, DEFAULT_TOL: DEFAULT_TOL };
if (require.main === module) process.exit(main(process.argv.slice(2)));
