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

  // True when two geometries share any area: a vertex of one lies in the other, or two edges cross.
  // Used to decide which neighbouring region a near region belongs under (see buildRegionDef).
  function segmentsCross(a, b, c, d) {
    function orient(p, q, r) { return (q[0] - p[0]) * (r[1] - p[1]) - (q[1] - p[1]) * (r[0] - p[0]); }
    var o1 = orient(a, b, c), o2 = orient(a, b, d), o3 = orient(c, d, a), o4 = orient(c, d, b);
    return ((o1 > 0) !== (o2 > 0)) && o1 !== 0 && o2 !== 0 && ((o3 > 0) !== (o4 > 0)) && o3 !== 0 && o4 !== 0;
  }
  function geometriesTouch(g1, g2) {
    var b1 = geometryBBox(g1), b2 = geometryBBox(g2);
    if (b1[0] > b2[2] || b2[0] > b1[2] || b1[1] > b2[3] || b2[1] > b1[3]) return false;
    var p1 = polysOf(g1), p2 = polysOf(g2), i, j, k, m;
    function anyVertexIn(polysA, polysB) {
      for (var a = 0; a < polysA.length; a++) {
        var ring = polysA[a][0] || [];
        for (var v = 0; v < ring.length; v++) {
          for (var b = 0; b < polysB.length; b++) if (polygonContains(polysB[b], ring[v][0], ring[v][1])) return true;
        }
      }
      return false;
    }
    if (anyVertexIn(p1, p2) || anyVertexIn(p2, p1)) return true;
    for (i = 0; i < p1.length; i++) {
      var r1 = p1[i][0] || [];
      for (j = 0; j < p2.length; j++) {
        var r2 = p2[j][0] || [];
        for (k = 1; k < r1.length; k++) {
          for (m = 1; m < r2.length; m++) if (segmentsCross(r1[k - 1], r1[k], r2[m - 1], r2[m])) return true;
        }
      }
    }
    return false;
  }

  // Centre of a region: the area centroid of its largest polygon (outer ring only). A metro that spills a
  // little across a state line has its centre in the state it belongs to.
  function geometryAnchor(geom) {
    var best = null, bestArea = -1;
    polysOf(geom).forEach(function (poly) {
      var ring = poly[0] || [], a = 0, cx = 0, cy = 0;
      for (var i = 0, j = ring.length - 1; i < ring.length; j = i++) {
        var f = ring[j][0] * ring[i][1] - ring[i][0] * ring[j][1];
        a += f; cx += (ring[j][0] + ring[i][0]) * f; cy += (ring[j][1] + ring[i][1]) * f;
      }
      var area = Math.abs(a) / 2;
      if (area > bestArea && a !== 0) { bestArea = area; best = [cx / (3 * a), cy / (3 * a)]; }
    });
    if (best) return best;
    var b = geometryBBox(geom);
    return [(b[0] + b[2]) / 2, (b[1] + b[3]) / 2];
  }

  // ---- distance ---------------------------------------------------------------



  // Short distances only (tens of miles): positions are flattened around the query point
  // (longitude scaled by cos(latitude)) and measured in metres. Over 10 miles that is accurate to
  // a few metres; it is not meant for continental distances.
  var M_PER_DEG = 111195;   // metres per degree of latitude (spherical earth, R = 6371.0088 km)
  var MILE_M = 1609.344;

  // Metres from (lon, lat) to the nearest point of the geometry; 0 inside or on its edge.
  function distanceToGeometry(geom, lon, lat) {
    if (geometryContains(geom, lon, lat)) return 0;
    var kx = M_PER_DEG * Math.cos(lat * Math.PI / 180), ky = M_PER_DEG;
    var best = Infinity;
    polysOf(geom).forEach(function (poly) {
      poly.forEach(function (ring) { // outer ring and holes: a point inside a hole is outside the polygon
        for (var i = 1; i < ring.length; i++) {
          var ax = (ring[i - 1][0] - lon) * kx, ay = (ring[i - 1][1] - lat) * ky;
          var bx = (ring[i][0] - lon) * kx, by = (ring[i][1] - lat) * ky;
          var dx = bx - ax, dy = by - ay, len2 = dx * dx + dy * dy;
          var t = len2 > 0 ? Math.max(0, Math.min(1, -(ax * dx + ay * dy) / len2)) : 0;
          var px = ax + t * dx, py = ay + t * dy;
          var d2 = px * px + py * py;
          if (d2 < best) best = d2;
        }
      });
    });
    return Math.sqrt(best);
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
      nearMiles: (typeof def.nearMiles === 'number' && isFinite(def.nearMiles) && def.nearMiles > 0) ? def.nearMiles : 0,
      root: def.root === true,
      parentProperty: def.parentProperty || null,
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

  // Every named region of the layer that contains the point or lies within `meters` of it:
  // [{ feature, name, code, distance (metres, 0 = contains), order }], containing first, then nearest first.
  // The bounding box (widened by the radius) is the cheap test; the exact distance runs only for survivors.
  function matchNear(layer, lat, lon, meters) {
    var dLat = meters / M_PER_DEG;
    var dLon = dLat / Math.max(Math.cos(lat * Math.PI / 180), 0.01);
    var out = [];
    layer.features.forEach(function (e, idx) {
      var b = e.bbox;
      if (lon < b[0] - dLon || lon > b[2] + dLon || lat < b[1] - dLat || lat > b[3] + dLat) return;
      if (!e.code) return;
      var d = distanceToGeometry(e.feature.geometry, lon, lat);
      if (d <= meters) out.push({ feature: e.feature, name: String(e.name || e.code), code: e.code, distance: d, order: idx });
    });
    out.sort(function (a, b) { return (a.distance - b.distance) || (a.order - b.order); });
    return out;
  }

  // layers: result of buildLayer() in output order.
  // returns { tokens:[token...], value:'a b c', perLayer:[{id,label,match,matches,near,unnamed,gap}],
  //           warnings:[string], blocked:[string] }
  //
  // `region def` builds a tree (MeshCore docs/cli_commands.md): a plain token is created as a child of the
  // cursor and the cursor moves to it; `name|jump` creates name, then moves the cursor to `jump`.
  // With none of the layer keys below the layers form a plain chain, layer 1 > layer 2 > ..., and the value
  // is the codes joined by spaces. With any of them the value is serialised from a tree instead:
  //   root: true           the layer's regions hang off the wildcard `*`
  //   parentProperty: 'p'  a region's parent is the code stored in its feature property p
  //                        (for example a state carries the code of its macro region)
  //   nearMiles: n         every region of the layer within n miles of the point is included, not only the
  //                        one containing it: the containing region first, then the nearest first
  // A layer with none of these hangs off the containing region of the nearest layer above that had one;
  // its near regions (those not containing the point) hang off the region of that layer they are centred in.
  function buildRegionDef(layers, lat, lon) {
    var tokens = [], perLayer = [], warnings = [];
    var treeMode = layers.some(function (l) { return l.root || l.parentProperty || l.nearMiles; });
    var nodes = Object.create(null), order = [];
    var cursor = null, cursorBefore = [], cursorLayer = -1, cursorLayerBefore = [], picksByLayer = [];

    // A near region that does not contain the point hangs under the region of the layer above that it is
    // centred in (a Fort Wayne metro seen from an Ohio repeater goes under Indiana even if its outline spills
    // into Ohio). Failing that: the point's own region if it overlaps it, else the nearest one it overlaps.
    // A region that contains the point always hangs under the point's own region.
    function homeParent(li, feature, distance) {
      var home = cursorBefore[li] || '*';
      var above = cursorLayerBefore[li];
      if (!(distance > 0) || above < 0 || !picksByLayer[above]) return home;
      var cands = picksByLayer[above];
      // 1. the region of the layer above that the near region is centred in
      var anchor = geometryAnchor(feature.geometry);
      for (var a = 0; a < cands.length; a++) {
        if (geometryContains(cands[a].feature.geometry, anchor[0], anchor[1])) return cands[a].code;
      }
      // 2. else the point's own region if the near region overlaps it, else any region it overlaps
      for (var c = 0; c < cands.length; c++) {
        if (cands[c].code === home && geometriesTouch(feature.geometry, cands[c].feature.geometry)) return home;
      }
      for (var d = 0; d < cands.length; d++) {
        if (geometriesTouch(feature.geometry, cands[d].feature.geometry)) return cands[d].code;
      }
      return home;
    }
    function parentOf(li, feature, distance) {
      var layer = layers[li];
      if (layer.root) return '*';
      if (layer.parentProperty) {
        var v = pick(feature.properties || {}, layer.parentProperty);
        v = v === null ? '' : String(v).trim();
        if (CODE_RE.test(v)) return v;
        warnings.push(layer.label + ': "' + featureName(feature, layer.nameProperty) + '" has no usable parent code in "' +
          [].concat(layer.parentProperty).join('" / "') + '"; attached to the wildcard.');
        return '*';
      }
      return homeParent(li, feature, distance);
    }
    function addNode(code, li, feature, distance) {
      var p = parentOf(li, feature, distance);
      if (p !== '*' && !nodes[p]) addAncestor(p, li);
      nodes[code] = { code: code, parent: p, layer: li };
      order.push(nodes[code]);
    }
    // The parent of a region that is not itself part of the result: take it from the layers above.
    function addAncestor(code, belowLi) {
      for (var j = belowLi - 1; j >= 0; j--) {
        var feats = layers[j].features;
        for (var k = 0; k < feats.length; k++) {
          if (feats[k].code === code) { addNode(code, j, feats[k].feature); return; }
        }
      }
      warnings.push(layers[belowLi].label + ': parent "' + code + '" is not a region of any layer above; attached to the wildcard.');
      nodes[code] = { code: code, parent: '*', layer: -1 };
      order.push(nodes[code]);
    }

    layers.forEach(function (layer, i) {
      var r = matchLayer(layer, lat, lon);
      var first = r.matches.length ? r.matches[0] : null;
      var picks = layer.nearMiles ? matchNear(layer, lat, lon, layer.nearMiles * MILE_M) : (first ? [first] : []);
      if (r.matches.length > 1 && !layer.nearMiles) {
        warnings.push(layer.label + ': point is in ' + r.matches.length + ' overlapping regions (' +
          r.matches.map(function (m) { return m.code; }).join(', ') + '); using ' + first.code + '.');
      }
      if (r.unnamed) {
        warnings.push(layer.label + ': point is in ' + r.unnamed + ' polygon(s) with no usable code; skipped.');
      }
      cursorBefore[i] = cursor;
      cursorLayerBefore[i] = cursorLayer;
      picksByLayer[i] = picks;
      if (treeMode) {
        picks.forEach(function (m) {
          if (nodes[m.code]) {
            if (!layer.nearMiles && nodes[m.code].layer !== i) {
              warnings.push(layer.label + ': code "' + m.code + '" is already used by a higher layer; it is only defined once.');
            }
            return;
          }
          addNode(m.code, i, m.feature, m.distance);
        });
      } else if (first) {
        if (tokens.indexOf(first.code) >= 0) {
          warnings.push(layer.label + ': code "' + first.code + '" is already used by a higher layer; region def would repeat it.');
        }
        tokens.push(first.code);
      }
      if (first) { cursor = first.code; cursorLayer = i; }
      perLayer.push({
        id: layer.id, label: layer.label, match: first, matches: r.matches,
        near: picks.filter(function (m) { return m.distance > 0; }),
        unnamed: r.unnamed, gap: false
      });
    });

    if (treeMode) {
      // Pre-order walk of the tree; a token takes `|<parent of the next token>` whenever the next token is
      // not its own child, which moves the cursor back to where the next token belongs.
      var kids = Object.create(null);
      order.forEach(function (n) { (kids[n.parent] = kids[n.parent] || []).push(n); });
      var pre = [];
      (function walk(parent) {
        (kids[parent] || []).forEach(function (n) { pre.push(n); walk(n.code); });
      })('*');
      pre.forEach(function (n, k) {
        var next = pre[k + 1];
        tokens.push(next && next.parent !== n.code ? n.code + '|' + next.parent : n.code);
      });
    }

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
  // A token is `name` or `name|jump`; the repeater splits a token at most once on '|', and the jump
  // target must be `*` or a name created earlier in the same command.
  function commandProblems(value) {
    var problems = [];
    if (!value) return problems;
    var tokens = value.split(' ');
    var defined = Object.create(null), regions = 0;
    tokens.forEach(function (t) {
      var parts = t.split('|');
      if (parts.length > 2) { problems.push('"' + t + '" has more than one "|"; the repeater splits a token at most once.'); return; }
      var name = parts[0], jump = parts.length === 2 ? parts[1] : null;
      if (!CODE_RE.test(name)) problems.push('"' + name + '" is not a valid region name for the repeater (lowercase a-z 0-9 _ -, at most ' + FW_MAX_NAME_LEN + ' characters).');
      regions++;
      defined[name] = true;
      if (jump !== null && jump !== '*' && !defined[jump]) problems.push('"' + t + '": the jump target "' + jump + '" is not defined earlier in the command.');
    });
    var line = 'region def ' + value;
    if (line.length > FW_MAX_LINE_LEN) problems.push('The command line is ' + line.length + ' characters; the repeater accepts at most ' + FW_MAX_LINE_LEN + ' and would cut it off.');
    if (regions > FW_MAX_REGIONS) problems.push('The command defines ' + regions + ' regions; a repeater holds at most ' + FW_MAX_REGIONS + ' in total.');
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
  var MAX_NEAR_MILES = 100;
  var LAYER_KEYS = ['id', 'label', 'url', 'nameProperty', 'codeProperty', 'codes', 'groupBy', 'coverage',
                    'color', 'colorBy', 'highlight', 'optional', 'allowOverlap', 'ignoreCodes',
                    'root', 'parentProperty', 'nearMiles'];

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
      if (def.nearMiles !== undefined && (typeof def.nearMiles !== 'number' || !isFinite(def.nearMiles) || def.nearMiles <= 0 || def.nearMiles > MAX_NEAR_MILES)) {
        errors.push(at + ': "nearMiles" must be a number above 0 and at most ' + MAX_NEAR_MILES + '.');
      }
      if (def.parentProperty !== undefined && !isStrOrStrArray(def.parentProperty)) errors.push(at + ': "parentProperty" must be a string or array of strings.');
      if (def.root === true && def.parentProperty !== undefined) warnings.push(at + ': "root" and "parentProperty" are both set; "root" wins and the layer hangs off the wildcard.');
      if (def.parentProperty !== undefined && def.groupBy !== undefined) warnings.push(at + ': "parentProperty" has no effect together with "groupBy" (grouped regions do not keep their feature properties).');
      ['highlight', 'optional', 'allowOverlap', 'root'].forEach(function (k) {
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
    repeaterCommands: repeaterCommands, parseLatLon: parseLatLon, featureColorOverride: featureColorOverride,
    distanceToGeometry: distanceToGeometry, geometriesTouch: geometriesTouch, geometryAnchor: geometryAnchor, matchNear: matchNear, MILE_M: MILE_M, MAX_NEAR_MILES: MAX_NEAR_MILES
  };

  root.RegionDef = api;
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
})(typeof window !== 'undefined' ? window : globalThis);
