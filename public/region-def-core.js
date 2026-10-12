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

  function countVertices(geom) {
    var n = 0;
    polysOf(geom).forEach(function (poly) { poly.forEach(function (ring) { n += ring.length; }); });
    return n;
  }

  // Strict allowlist for layer file paths from layers.json: relative, [A-Za-z0-9_.-] segments
  // joined by single '/', no '.'/'..' segments, must end in .geojson or .json. Rejects schemes,
  // protocol-relative URLs, backslashes, %-escapes, queries and fragments.
  function isSafeLayerPath(p) {
    if (typeof p !== 'string' || p.length === 0 || p.length > 200) return false;
    if (!/^[A-Za-z0-9_-][A-Za-z0-9_.\/-]*\.(?:geojson|json)$/.test(p)) return false;
    var segs = p.split('/');
    for (var i = 0; i < segs.length; i++) {
      if (segs[i] === '' || segs[i] === '.' || segs[i] === '..') return false;
    }
    return true;
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

  // Canonical region codes are written to repeaters, so they are validated, never "fixed up":
  // lowercase letters, digits, '_' and '-', starting with a letter or digit.
  //
  // Firmware limits (MeshCore src/helpers/RegionMap.h/.cpp, CommonCLI.cpp, examples/simple_repeater/main.cpp):
  //   - RegionEntry.name is `char name[31]`: at most 30 characters. A longer name is silently truncated
  //     by the repeater, so it would not match what the operator meant. FW_MAX_NAME_LEN
  //   - RegionMap::is_name_char accepts '-', '$', '#', digits and every byte >= 'A'. The characters
  //     allowed here are a strict subset; '$' (private region) and '#' are deliberately never emitted.
  //   - The serial CLI line buffer is `char command[160]`, one byte of which is the terminating CR, so a
  //     `region def ...` line longer than 158 characters is cut off. FW_MAX_LINE_LEN
  //   - A repeater holds at most MAX_REGION_ENTRIES (32 by default) regions in total. FW_MAX_REGIONS
  var FW_MAX_NAME_LEN = 30;
  var FW_MAX_LINE_LEN = 158;
  var FW_MAX_REGIONS = 32;
  var CODE_RE = /^[a-z0-9][a-z0-9_-]{0,29}$/;

  // The code a feature contributes to `region def`, and why not if it has none.
  //   explicit  : feature.properties.__code (file/group level) or def.codeProperty -> used verbatim
  //   otherwise : slug(name)
  // Either way the result must match CODE_RE and, when def.codes is set, be one of those codes.
  // returns { code: string|null, reason: null|'missing'|'invalid'|'not-allowed' }
  function featureCode(feature, def) {
    var props = feature.properties || {};
    var raw = null, explicit = false;
    if (props.__code !== undefined && props.__code !== null) { raw = String(props.__code); explicit = true; }
    else if (def && def.codeProperty) {
      var v = pick(props, def.codeProperty);
      if (v !== null) { raw = String(v).trim(); explicit = true; }
    }
    var code;
    if (explicit) {
      code = raw;
      if (!CODE_RE.test(code)) return { code: null, reason: 'invalid' };
    } else {
      code = slug(featureName(feature, (def && def.nameProperty) || 'name'));
      if (!code) return { code: null, reason: 'missing' };
      if (!CODE_RE.test(code)) return { code: null, reason: 'invalid' };
    }
    if (def && Array.isArray(def.codes) && def.codes.indexOf(code) < 0) return { code: null, reason: 'not-allowed' };
    return { code: code, reason: null };
  }

  // Merge many features into ONE MultiPolygon feature named `name` (optionally with explicit `code`).
  function mergeFeatures(feats, name, code) {
    var coords = [];
    var color = null;
    feats.forEach(function (f) {
      polysOf(f.geometry).forEach(function (p) { coords.push(p); });
      if (!color && f.properties && f.properties.color) color = f.properties.color;
    });
    var props = { __name: name };
    if (code !== undefined && code !== null) props.__code = code;
    if (color) props.color = color;
    return { type: 'Feature', properties: props, geometry: { type: 'MultiPolygon', coordinates: coords } };
  }

  // Merge features that share the value of `prop`. Features without the property are dropped.
  function groupFeatures(features, prop, codeProp) {
    var order = [], buckets = Object.create(null);
    features.forEach(function (f) {
      var v = f.properties && f.properties[prop];
      if (v === undefined || v === null || String(v).trim() === '') return;
      var k = String(v);
      if (!buckets[k]) { buckets[k] = []; order.push(k); }
      buckets[k].push(f);
    });
    return order.map(function (k) {
      var code = null;
      if (codeProp) { var cv = pick(buckets[k][0].properties || {}, codeProp); if (cv !== null) code = String(cv).trim(); }
      return mergeFeatures(buckets[k], k, code);
    });
  }

  // ---- layers ----------------------------------------------------------------

  // def   : one entry of layers.json  { id, label, url, nameProperty, groupBy, colorBy, color, highlight }
  // files : array of { name?: string, data: <GeoJSON> } in the same order as the def's url list
  // returns { id, label, nameProperty, color, colorBy, highlight, features:[{feature,bbox,name}] }
  function buildLayer(def, files) {
    // groupBy merges features that share a property value across ALL the layer's files
    // (a mesh region can straddle two state/macro files). Grouped features take the place
    // of the first file that contributed to the pool, so output order stays predictable.
    var features = [], pool = [], poolSlot = -1;
    (files || []).forEach(function (file) {
      var feats = normalizeGeoJSON(file.data);
      if (file.name && feats.length) { features.push(mergeFeatures(feats, file.name, file.code)); return; }
      if (def.groupBy) {
        if (poolSlot < 0) { poolSlot = features.length; features.push(null); }
        pool = pool.concat(feats);
        return;
      }
      features = features.concat(feats);
    });
    if (poolSlot >= 0) {
      var grouped = groupFeatures(pool, def.groupBy, def.codeProperty);
      features = features.slice(0, poolSlot).concat(grouped, features.slice(poolSlot + 1));
    }
    // ignoreCodes: regions deliberately left out of this layer (for example states that
    // exist in the boundary file but are not part of any macro region). They are dropped
    // before anything else, so they are neither drawn, matched, nor reported as problems.
    if (Array.isArray(def.ignoreCodes) && def.ignoreCodes.length) {
      var open = {}; Object.keys(def).forEach(function (k) { if (k !== 'codes') open[k] = def[k]; });
      features = features.filter(function (f) {
        var c = featureCode(f, open).code;
        return !(c && def.ignoreCodes.indexOf(c) >= 0);
      });
    }
    var built = features.map(function (f) {
      var fc = featureCode(f, def);
      return { feature: f, bbox: geometryBBox(f.geometry), name: featureName(f, def.nameProperty || 'name'), code: fc.code, codeProblem: fc.reason };
    });
    var vertices = 0, unnamed = 0, problems = { missing: 0, invalid: 0, 'not-allowed': 0 };
    features.forEach(function (f) { vertices += countVertices(f.geometry); });
    built.forEach(function (e) { if (!e.code) { unnamed++; problems[e.codeProblem]++; } });
    return {
      id: def.id,
      vertices: vertices,
      unnamed: unnamed,
      codeProblems: problems,
      coverage: def.coverage === 'full' ? 'full' : 'partial',
      label: def.label || def.id,
      nameProperty: def.nameProperty || 'name',
      color: def.color || null,
      colorBy: def.colorBy || null,
      highlight: def.highlight !== false,
      features: built
    };
  }

  // Layer-level match for a point: { matches:[{feature,name,code}], unnamed:n }
  function matchLayer(layer, lat, lon) {
    var matches = [], unnamed = 0;
    layer.features.forEach(function (e) {
      var b = e.bbox;
      if (lon < b[0] || lon > b[2] || lat < b[1] || lat > b[3]) return;
      if (!geometryContains(e.feature.geometry, lon, lat)) return;
      if (!e.code) { unnamed++; return; }
      matches.push({ feature: e.feature, name: String(e.name || e.code), code: e.code });
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
        warnings.push(layer.label + ': point is in ' + r.unnamed + ' polygon(s) with no usable code; skipped.');
      }
      if (first) {
        if (tokens.indexOf(first.code) >= 0) {
          warnings.push(layer.label + ': code "' + first.code + '" is already used by a higher layer; region def would repeat it.');
        }
        tokens.push(first.code);
      }
      perLayer.push({ id: layer.id, label: layer.label, match: first, matches: r.matches, unnamed: r.unnamed, gap: false });
    });
    // coverage:"full" layers promise to cover every place the other layers cover. A miss there
    // while some other layer matched is a hole in the data, not a legitimate exclusion.
    layers.forEach(function (layer, i) {
      if (layer.coverage !== 'full' || perLayer[i].match) return;
      var other = perLayer.some(function (p, k) { return k !== i && p.match; });
      if (other) {
        perLayer[i].gap = true;
        warnings.push(layer.label + ': no region covers this point, but the layer declares full coverage (data gap).');
      }
    });
    var value = tokens.join(' ');
    // A command the repeater would truncate or reject is withheld, not shown as if it were valid.
    var blocked = commandProblems(value);
    if (blocked.length) value = '';
    return { tokens: tokens, value: value, blocked: blocked, perLayer: perLayer, warnings: warnings };
  }

  // Reasons `region def <value>` cannot be pasted into a repeater as-is (empty list = fine).
  function commandProblems(value) {
    var problems = [];
    if (!value) return problems;
    var tokens = value.split(' ');
    tokens.forEach(function (t) {
      if (!CODE_RE.test(t)) problems.push('"' + t + '" is not a valid region name for the repeater (lowercase a-z 0-9 _ -, at most ' + FW_MAX_NAME_LEN + ' characters).');
    });
    var line = 'region def ' + value;
    if (line.length > FW_MAX_LINE_LEN) problems.push('The command line is ' + line.length + ' characters; the repeater accepts at most ' + FW_MAX_LINE_LEN + ' and would cut it off.');
    if (tokens.length > FW_MAX_REGIONS) problems.push('The command defines ' + tokens.length + ' regions; a repeater holds at most ' + FW_MAX_REGIONS + ' in total.');
    return problems;
  }

  // The block the operator pastes into the repeater console. Order matters: `region save` is the
  // only step that persists, and `region allowf` does not save by itself, so it must come first.
  // Returns '' for a value that would not be a valid repeater configuration.
  function repeaterCommands(value) {
    if (!value || commandProblems(value).length) return '';
    return 'region def ' + value + '\nregion allowf *\nregion save';
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

  // ---- manifest validation -----------------------------------------------------

  var MANIFEST_VERSION = 1;
  var MAX_LAYERS = 16;
  var LAYER_KEYS = ['id', 'label', 'url', 'nameProperty', 'codeProperty', 'codes', 'groupBy', 'coverage',
                    'color', 'colorBy', 'highlight', 'optional', 'allowOverlap', 'ignoreCodes'];

  function isStrOrStrArray(v) {
    return typeof v === 'string' ? v.length > 0 :
      (Array.isArray(v) && v.length > 0 && v.every(function (s) { return typeof s === 'string' && s.length > 0; }));
  }

  // Structural check of layers.json. Pure; returns { errors:[], warnings:[], version }.
  // errors  -> the page refuses to load the manifest
  // warnings-> shown to the visitor, loading continues
  function validateManifest(m) {
    var errors = [], warnings = [];
    if (!m || typeof m !== 'object' || Array.isArray(m)) return { errors: ['layers.json must be an object'], warnings: warnings, version: null };
    var version = m.version;
    if (version === undefined) warnings.push('layers.json has no "version" field; assuming version ' + MANIFEST_VERSION + '.');
    else if (typeof version !== 'number' || Math.floor(version) !== version || version < 1) errors.push('"version" must be a positive integer.');
    else if (version > MANIFEST_VERSION) errors.push('layers.json version ' + version + ' is newer than this page supports (' + MANIFEST_VERSION + ').');
    if (!Array.isArray(m.layers) || !m.layers.length) { errors.push('"layers" must be a non-empty array.'); return { errors: errors, warnings: warnings, version: version }; }
    if (m.layers.length > MAX_LAYERS) errors.push('At most ' + MAX_LAYERS + ' layers are supported.');
    var ids = Object.create(null);
    m.layers.forEach(function (def, i) {
      var at = 'layer ' + (i + 1) + (def && def.id ? ' (' + def.id + ')' : '');
      if (!def || typeof def !== 'object' || Array.isArray(def)) { errors.push(at + ': must be an object.'); return; }
      if (typeof def.id !== 'string' || !/^[A-Za-z][A-Za-z0-9_-]{0,31}$/.test(def.id)) errors.push(at + ': "id" must be 1-32 chars of letters, digits, "_" or "-", starting with a letter.');
      else if (ids[def.id]) errors.push(at + ': duplicate id.');
      else ids[def.id] = true;
      if (def.label !== undefined && typeof def.label !== 'string') errors.push(at + ': "label" must be a string.');
      var files = Array.isArray(def.url) ? def.url : [def.url];
      if (def.url === undefined || !files.length) errors.push(at + ': "url" is required.');
      else files.forEach(function (f) {
        var path = (f && typeof f === 'object') ? f.url : f;
        if (!isSafeLayerPath(path)) errors.push(at + ': unsafe or invalid file path ' + JSON.stringify(String(path).slice(0, 60)) + '.');
        if (f && typeof f === 'object') {
          if (f.name !== undefined && (typeof f.name !== 'string' || !f.name.trim())) errors.push(at + ': file "name" must be a non-empty string.');
          if (f.code !== undefined && !CODE_RE.test(String(f.code))) errors.push(at + ': file "code" ' + JSON.stringify(String(f.code)) + ' is not a valid code (lowercase a-z 0-9 _ -, at most 30 characters).');
          if (f.code !== undefined && f.name === undefined) errors.push(at + ': file "code" needs a file "name" (it merges the file into one region).');
        }
      });
      ['nameProperty', 'codeProperty'].forEach(function (k) {
        if (def[k] !== undefined && !isStrOrStrArray(def[k])) errors.push(at + ': "' + k + '" must be a string or array of strings.');
      });
      if (def.groupBy !== undefined && (typeof def.groupBy !== 'string' || !def.groupBy)) errors.push(at + ': "groupBy" must be a property name.');
      if (def.codes !== undefined) {
        if (!Array.isArray(def.codes) || !def.codes.length) errors.push(at + ': "codes" must be a non-empty array.');
        else {
          var seen = Object.create(null);
          def.codes.forEach(function (c) {
            if (typeof c !== 'string' || !CODE_RE.test(c)) errors.push(at + ': canonical code ' + JSON.stringify(c) + ' is invalid (lowercase a-z 0-9 _ -, at most 30 characters).');
            else if (seen[c]) errors.push(at + ': canonical code "' + c + '" is listed twice.');
            seen[c] = true;
          });
        }
      }
      if (def.ignoreCodes !== undefined) {
        if (!Array.isArray(def.ignoreCodes) || !def.ignoreCodes.length) errors.push(at + ': "ignoreCodes" must be a non-empty array.');
        else def.ignoreCodes.forEach(function (c) {
          if (typeof c !== 'string' || !CODE_RE.test(c)) errors.push(at + ': ignored code ' + JSON.stringify(c) + ' is invalid (lowercase a-z 0-9 _ -, at most 30 characters).');
          else if (Array.isArray(def.codes) && def.codes.indexOf(c) >= 0) errors.push(at + ': code "' + c + '" is in both "codes" and "ignoreCodes".');
        });
      }
      if (def.coverage !== undefined && def.coverage !== 'partial' && def.coverage !== 'full') errors.push(at + ': "coverage" must be "partial" or "full".');
      if (def.colorBy !== undefined && def.colorBy !== 'feature') errors.push(at + ': "colorBy" can only be "feature".');
      if (def.color !== undefined && (typeof def.color !== 'string' || !/^#[0-9a-fA-F]{3,8}$|^[a-zA-Z]+$/.test(def.color))) errors.push(at + ': "color" must be a hex or named colour.');
      ['highlight', 'optional', 'allowOverlap'].forEach(function (k) {
        if (def[k] !== undefined && typeof def[k] !== 'boolean') errors.push(at + ': "' + k + '" must be true or false.');
      });
      Object.keys(def).forEach(function (k) {
        if (LAYER_KEYS.indexOf(k) < 0) warnings.push(at + ': unknown field "' + k + '" ignored (typo?).');
      });
    });
    return { errors: errors, warnings: warnings, version: version };
  }

  var api = {
    featureCode: featureCode, commandProblems: commandProblems, FW_MAX_NAME_LEN: FW_MAX_NAME_LEN, FW_MAX_LINE_LEN: FW_MAX_LINE_LEN, FW_MAX_REGIONS: FW_MAX_REGIONS, validateManifest: validateManifest, CODE_RE: CODE_RE, MANIFEST_VERSION: MANIFEST_VERSION,
    normalizeGeoJSON: normalizeGeoJSON, countVertices: countVertices, isSafeLayerPath: isSafeLayerPath, geometryBBox: geometryBBox, onSegment: onSegment,
    ringLocate: ringLocate, polygonContains: polygonContains, geometryContains: geometryContains,
    mergeFeatures: mergeFeatures, groupFeatures: groupFeatures, slug: slug, featureName: featureName,
    buildLayer: buildLayer, matchLayer: matchLayer, buildRegionDef: buildRegionDef,
    repeaterCommands: repeaterCommands, parseLatLon: parseLatLon, featureColorOverride: featureColorOverride
  };

  root.RegionDef = api;
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
})(typeof window !== 'undefined' ? window : globalThis);
