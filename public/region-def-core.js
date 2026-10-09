/* region-def-core.js — pure logic for the Region Def tool (#/tools/region-def).
 *
 * Given a set of GeoJSON "layers" (macro region, mesh region, state, metro, ...)
 * and a lat/lon, work out which polygon of each layer contains the point and
 * build the repeater command `region def <code> <code> ...` (layer order = output
 * order, top to bottom).
 *
 * No DOM, no network, no globals read. Loaded in the browser as window.RegionDef
 * and in Node via require() for unit tests.
 *
 * GeoJSON coordinate order is [lon, lat]. A point exactly on a polygon edge counts
 * as inside; a point inside a hole is outside.
 */
(function (root) {
  'use strict';

  // ---- GeoJSON normalisation -------------------------------------------------

  // Accepts FeatureCollection | Feature | bare Polygon/MultiPolygon geometry and
  // returns a flat array of Polygon/MultiPolygon features. Anything else is dropped.
  function normalizeGeoJSON(gj) {
    var out = [];
    if (!gj || typeof gj !== 'object') return out;
    function push(f) {
      if (!f || !f.geometry) return;
      var t = f.geometry.type;
      if (t === 'Polygon' || t === 'MultiPolygon') {
        out.push({ type: 'Feature', properties: f.properties || {}, geometry: f.geometry });
      } else if (t === 'GeometryCollection' && Array.isArray(f.geometry.geometries)) {
        f.geometry.geometries.forEach(function (g) { push({ properties: f.properties, geometry: g }); });
      }
    }
    if (gj.type === 'FeatureCollection' && Array.isArray(gj.features)) gj.features.forEach(push);
    else if (gj.type === 'Feature') push(gj);
    else if (gj.type === 'Polygon' || gj.type === 'MultiPolygon') push({ properties: {}, geometry: gj });
    return out;
  }

  function polysOf(geom) {
    return geom.type === 'Polygon' ? [geom.coordinates] : geom.coordinates;
  }

  function geometryBBox(geom) {
    var b = [Infinity, Infinity, -Infinity, -Infinity]; // minLon, minLat, maxLon, maxLat
    polysOf(geom).forEach(function (poly) {
      var ring = poly[0] || [];
      for (var i = 0; i < ring.length; i++) {
        var p = ring[i];
        if (p[0] < b[0]) b[0] = p[0];
        if (p[1] < b[1]) b[1] = p[1];
        if (p[0] > b[2]) b[2] = p[0];
        if (p[1] > b[3]) b[3] = p[1];
      }
    });
    return b;
  }

  // ---- point in polygon ------------------------------------------------------

  var EPS = 1e-12;

  function onSegment(px, py, ax, ay, bx, by) {
    var cross = (px - ax) * (by - ay) - (py - ay) * (bx - ax);
    if (Math.abs(cross) > EPS * Math.max(1, Math.abs(bx - ax) + Math.abs(by - ay))) return false;
    return px >= Math.min(ax, bx) - EPS && px <= Math.max(ax, bx) + EPS &&
           py >= Math.min(ay, by) - EPS && py <= Math.max(ay, by) + EPS;
  }

  // 0 = outside, 1 = inside, 2 = on boundary
  function ringLocate(x, y, ring) {
    var inside = false;
    for (var i = 0, j = ring.length - 1; i < ring.length; j = i++) {
      var xi = ring[i][0], yi = ring[i][1], xj = ring[j][0], yj = ring[j][1];
      if (onSegment(x, y, xi, yi, xj, yj)) return 2;
      if (((yi > y) !== (yj > y)) && (x < (xj - xi) * (y - yi) / (yj - yi) + xi)) inside = !inside;
    }
    return inside ? 1 : 0;
  }

  // poly = [outerRing, hole, hole, ...]
  function polygonContains(poly, x, y) {
    if (!poly || !poly.length) return false;
    var outer = ringLocate(x, y, poly[0]);
    if (outer === 0) return false;
    if (outer === 2) return true; // on the outer edge
    for (var h = 1; h < poly.length; h++) {
      if (ringLocate(x, y, poly[h]) !== 0) return false; // in hole or on hole edge
    }
    return true;
  }

  function geometryContains(geom, lon, lat) {
    var polys = polysOf(geom);
    for (var i = 0; i < polys.length; i++) {
      if (polygonContains(polys[i], lon, lat)) return true;
    }
    return false;
  }

  // ---- naming / grouping -----------------------------------------------------

  // Region code: trim, lowercase, whitespace -> hyphen, strip anything outside [a-z0-9_-].
  function slug(s) {
    if (s === null || s === undefined) return '';
    return String(s).trim().toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9_-]/g, '');
  }

  function pick(props, nameProperty) {
    var names = Array.isArray(nameProperty) ? nameProperty : [nameProperty];
    for (var i = 0; i < names.length; i++) {
      var v = props && props[names[i]];
      if (v !== undefined && v !== null && String(v).trim() !== '') return v;
    }
    return null;
  }

  // Name of a feature (merged features carry __name). null when it has no usable name.
  function featureName(feature, nameProperty) {
    var props = feature.properties || {};
    if (props.__name) return String(props.__name);
    return pick(props, nameProperty || 'name');
  }

  // Merge many features into ONE MultiPolygon feature named `name`.
  function mergeFeatures(feats, name) {
    var coords = [];
    var color = null;
    feats.forEach(function (f) {
      polysOf(f.geometry).forEach(function (p) { coords.push(p); });
      if (!color && f.properties && f.properties.color) color = f.properties.color;
    });
    var props = { __name: name };
    if (color) props.color = color;
    return { type: 'Feature', properties: props, geometry: { type: 'MultiPolygon', coordinates: coords } };
  }

  // Merge features that share the value of `prop`. Features without the property are dropped.
  function groupFeatures(features, prop) {
    var order = [], buckets = Object.create(null);
    features.forEach(function (f) {
      var v = f.properties && f.properties[prop];
      if (v === undefined || v === null || String(v).trim() === '') return;
      var k = String(v);
      if (!buckets[k]) { buckets[k] = []; order.push(k); }
      buckets[k].push(f);
    });
    return order.map(function (k) { return mergeFeatures(buckets[k], k); });
  }

  // ---- layers ----------------------------------------------------------------

  // def   : one entry of layers.json  { id, label, url, nameProperty, groupBy, colorBy, color, highlight }
  // files : array of { name?: string, data: <GeoJSON> } in the same order as the def's url list
  // returns { id, label, nameProperty, color, colorBy, highlight, features:[{feature,bbox,name}] }
  function buildLayer(def, files) {
    var features = [];
    (files || []).forEach(function (file) {
      var feats = normalizeGeoJSON(file.data);
      if (file.name && feats.length) feats = [mergeFeatures(feats, file.name)];
      else if (def.groupBy) feats = groupFeatures(feats, def.groupBy);
      features = features.concat(feats);
    });
    return {
      id: def.id,
      label: def.label || def.id,
      nameProperty: def.nameProperty || 'name',
      color: def.color || null,
      colorBy: def.colorBy || null,
      highlight: def.highlight !== false,
      features: features.map(function (f) {
        return { feature: f, bbox: geometryBBox(f.geometry), name: featureName(f, def.nameProperty || 'name') };
      })
    };
  }

  // Layer-level match for a point: { matches:[{feature,name,code}], unnamed:n }
  function matchLayer(layer, lat, lon) {
    var matches = [], unnamed = 0;
    layer.features.forEach(function (e) {
      var b = e.bbox;
      if (lon < b[0] || lon > b[2] || lat < b[1] || lat > b[3]) return;
      if (!geometryContains(e.feature.geometry, lon, lat)) return;
      var code = slug(e.name);
      if (!code) { unnamed++; return; }
      matches.push({ feature: e.feature, name: String(e.name), code: code });
    });
    return { matches: matches, unnamed: unnamed };
  }

  // layers: result of buildLayer() in output order.
  // returns { tokens:[code...], value:'a b c', perLayer:[{id,label,match,matches}], warnings:[string] }
  function buildRegionDef(layers, lat, lon) {
    var tokens = [], perLayer = [], warnings = [];
    layers.forEach(function (layer) {
      var r = matchLayer(layer, lat, lon);
      var first = r.matches.length ? r.matches[0] : null;
      if (r.matches.length > 1) {
        warnings.push(layer.label + ': point is in ' + r.matches.length + ' overlapping regions (' +
          r.matches.map(function (m) { return m.code; }).join(', ') + '); using ' + first.code + '.');
      }
      if (r.unnamed) {
        warnings.push(layer.label + ': point is in ' + r.unnamed + ' polygon(s) with no usable name property; skipped.');
      }
      if (first) tokens.push(first.code);
      perLayer.push({ id: layer.id, label: layer.label, match: first, matches: r.matches });
    });
    return { tokens: tokens, value: tokens.join(' '), perLayer: perLayer, warnings: warnings };
  }

  // The block the operator pastes into the repeater console.
  function repeaterCommands(value) {
    if (!value) return '';
    return 'region def ' + value + '\nregion save\nregion allowf *';
  }

  // "39.1, -84.5" | "39.1 -84.5" -> {lat, lon} or null (range validated).
  function parseLatLon(s) {
    if (typeof s !== 'string') return null;
    var m = /^\s*(-?\d+(?:\.\d+)?)\s*[,\s]\s*(-?\d+(?:\.\d+)?)\s*$/.exec(s);
    if (!m) return null;
    var lat = parseFloat(m[1]), lon = parseFloat(m[2]);
    if (!(lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180)) return null;
    return { lat: lat, lon: lon };
  }

  // Per-feature colour override (only meaningful when layer.colorBy === 'feature').
  function featureColorOverride(layer, feature) {
    if (layer.colorBy !== 'feature') return null;
    var c = feature && feature.properties && feature.properties.color;
    return (typeof c === 'string' && /^#[0-9a-fA-F]{3,8}$|^[a-zA-Z]+$/.test(c)) ? c : null;
  }

  var api = {
    normalizeGeoJSON: normalizeGeoJSON, geometryBBox: geometryBBox, onSegment: onSegment,
    ringLocate: ringLocate, polygonContains: polygonContains, geometryContains: geometryContains,
    mergeFeatures: mergeFeatures, groupFeatures: groupFeatures, slug: slug, featureName: featureName,
    buildLayer: buildLayer, matchLayer: matchLayer, buildRegionDef: buildRegionDef,
    repeaterCommands: repeaterCommands, parseLatLon: parseLatLon, featureColorOverride: featureColorOverride
  };

  root.RegionDef = api;
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
})(typeof window !== 'undefined' ? window : globalThis);
