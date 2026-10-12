// Region Def — address -> `region def ...` repeater command (#/tools/region-def).
// Pure logic lives in region-def-core.js (window.RegionDef); this file is the page.
//
// Data: public/region-layers/layers.json + the GeoJSON files it lists (operator-supplied,
// served as static files). See docs/region-def.md for the manifest format.
// Geocoding: the typed address is sent from the visitor's browser to Nominatim.
(function () {
  'use strict';

  var LAYER_DIR = 'region-layers/';
  var NOMINATIM = 'https://nominatim.openstreetmap.org/search';
  var MIN_GEOCODE_GAP_MS = 1100; // Nominatim usage policy: max 1 request/second
  var OSM_FALLBACK = 'https://tile.openstreetmap.org/{z}/{x}/{y}.png';
  var PALETTE_SIZE = 8;
  // Line patterns repeat every 4 layers; with the colour they let layers be told apart without relying on hue alone.
  var DASHES = [null, '8 6', '2 6', '12 4 2 4'];
  var SWATCH_STYLES = ['solid', 'dashed', 'dotted', 'double'];

  var HEAVY_VERTICES = 50000; // layers above this start hidden: drawing them is the expensive part on phones
  var GEOCODE_KEY = 'rd-last-geocode';
  // Request timeouts. window.RegionDefConfig = { layerTimeoutMs, geocodeTimeoutMs } overrides them
  // (used by the E2E tests; an operator could raise them for a slow link).
  var DEFAULT_LAYER_TIMEOUT_MS = 20000;
  var DEFAULT_GEOCODE_TIMEOUT_MS = 10000;
  function fmtMs(ms) { return ms >= 1000 ? Math.round(ms / 1000) + ' s' : ms + ' ms'; }
  function cfgMs(key, dflt) {
    var c = window.RegionDefConfig;
    return (c && typeof c[key] === 'number' && c[key] > 0) ? c[key] : dflt;
  }

  var S = null; // per-visit state; null when the page is not mounted
  var lastGeocode = 0; // module-level so it survives leaving and re-entering the page (SPA)

  function el(tag, cls, text) {
    var n = document.createElement(tag);
    if (cls) n.className = cls;
    if (text !== undefined && text !== null) n.textContent = text;
    return n;
  }

  function hashParams() {
    return new URLSearchParams((location.hash.split('?')[1]) || '');
  }

  function cssVar(name, fallback) {
    try {
      var v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
      return v || fallback;
    } catch (_) { return fallback; }
  }

  // ---- manifest + layer loading ---------------------------------------------

  // fetch + parse with a timeout and errors that say what went wrong and where.
  function fetchJSON(url, timeoutMs, init) {
    var ctl = (typeof AbortController === 'function') ? new AbortController() : null;
    var timer = null, timedOut = false;
    if (ctl) timer = setTimeout(function () { timedOut = true; ctl.abort(); }, timeoutMs);
    var opts = init || {};
    if (ctl) opts.signal = ctl.signal;
    return fetch(url, opts).then(function (r) {
      if (!r.ok) throw new Error('HTTP ' + r.status + ' for ' + url);
      return r.json().catch(function () {
        if (timedOut) throw new Error('timed out after ' + fmtMs(timeoutMs) + ' reading ' + url);
        throw new Error('invalid JSON in ' + url);
      });
    }).catch(function (err) {
      if (timedOut) throw new Error('timed out after ' + fmtMs(timeoutMs) + ': ' + url);
      if (err && err.name === 'TypeError') throw new Error('network error fetching ' + url);
      throw err;
    }).then(function (v) { clearTimeout(timer); return v; }, function (e) { clearTimeout(timer); throw e; });
  }

  // def.url: string | [string | {url, name}]  ->  [{url, name}]
  function fileList(def) {
    var raw = Array.isArray(def.url) ? def.url : [def.url];
    return raw.map(function (u) {
      return (u && typeof u === 'object') ? { url: u.url, name: u.name || null, code: u.code || null } : { url: u, name: null, code: null };
    });
  }

  // One request per distinct file, however many layers reference it. Parsed GeoJSON is shared
  // read-only between layers (nothing in region-def-core.js mutates it).
  function loadLayer(def, fetchCached) {
    var files = fileList(def);
    return Promise.all(files.map(function (f) {
      if (!window.RegionDef.isSafeLayerPath(f.url)) {
        return Promise.reject(new Error('invalid layer path ' + JSON.stringify(String(f.url).slice(0, 60))));
      }
      return fetchCached(LAYER_DIR + f.url).then(function (data) { return { name: f.name, code: f.code, data: data }; });
    })).then(function (loaded) {
      var layer = window.RegionDef.buildLayer(def, loaded);
      var warnings = [];
      if (!layer.features.length) throw new Error('loaded but contains no valid polygons');
      if (layer.unnamed) {
        var cp = layer.codeProblems, why = [];
        if (cp.missing) why.push(cp.missing + ' with no name/code');
        if (cp.invalid) why.push(cp.invalid + ' with an invalid code (lowercase a-z 0-9 _ - only)');
        if (cp['not-allowed']) why.push(cp['not-allowed'] + ' not in the layer\'s canonical "codes" list');
        warnings.push(layer.label + ': ' + layer.unnamed + ' polygon(s) can never produce a code (' + why.join('; ') + ').');
      }
      if (layer.vertices > HEAVY_VERTICES) {
        warnings.push(layer.label + ': ' + layer.vertices + ' vertices; shown only when ticked. Simplify the file (e.g. mapshaper) for a lighter page.');
      }
      return { layer: layer, warnings: warnings };
    });
  }

  function loadAll() {
    var cache = Object.create(null);
    var layerMs = cfgMs('layerTimeoutMs', DEFAULT_LAYER_TIMEOUT_MS);
    function fetchCached(url) { return cache[url] || (cache[url] = fetchJSON(url, layerMs)); }
    return fetchJSON(LAYER_DIR + 'layers.json', layerMs).then(function (manifest) {
      var check = window.RegionDef.validateManifest(manifest);
      if (check.errors.length) throw new Error('layers.json is invalid: ' + check.errors.join(' '));
      var defs = manifest.layers;
      return Promise.all(defs.map(function (def) {
        return loadLayer(def, fetchCached).then(function (r) {
          return { layer: r.layer, def: def, error: null, warnings: r.warnings };
        }, function (err) {
          return { layer: null, def: def, error: (def.label || def.id) + ': ' + (err && err.message ? err.message : 'failed to load'), warnings: [] };
        });
      })).then(function (entries) { return { entries: entries, warnings: check.warnings }; });
    });
  }

  // ---- map ---------------------------------------------------------------------

  function isDarkTheme() {
    var attr = document.documentElement.getAttribute('data-theme');
    if (attr === 'dark') return true;
    if (attr === 'light') return false;
    try { return !!(window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches); } catch (_) { return false; }
  }

  // Page-local theme-synced basemap built on the shared tile registry (map-tile-providers.js).
  // Follow-up: extract map.js _syncDarkTiles into a shared helper and use it here.
  function syncTiles() {
    if (!S || !S.map) return;
    var dark = isDarkTheme();
    var reg = window.MC_TILE_PROVIDERS || {};
    var id = null;
    try { id = dark ? window.MC_getDarkTileProvider() : window.MC_getLightTileProvider(); } catch (_) {}
    var p = id && reg[id] ? reg[id] : null;
    var url = (typeof window.MC_tileUrlById === 'function' && p) ? window.MC_tileUrlById(id, OSM_FALLBACK) : OSM_FALLBACK;
    var opts = { attribution: p ? (p.attribution || '') : '&copy; OpenStreetMap contributors', maxZoom: p ? (p.maxZoom || 19) : 19 };
    var layers = [L.tileLayer(url, opts)];
    if (p && p.refUrl) layers.push(L.tileLayer(p.refUrl, opts));
    if (S.tiles) S.map.removeLayer(S.tiles);
    S.tiles = L.layerGroup(layers).addTo(S.map);
    var pane = S.map.getPane('tilePane');
    if (pane) pane.style.filter = (dark && p && p.invertFilter) ? p.invertFilter : '';
    if (S.tileStatus) {
      S.tileStatus.textContent = p ? '' : 'Tile provider not configured; using OpenStreetMap.';
    }
  }

  function layerColor(layer, index) {
    if (layer.color) return layer.color;
    return cssVar('--region-layer-' + ((index % PALETTE_SIZE) + 1), '#3388ff');
  }

  function featureStyle(layer, index, feature, matched) {
    var color = window.RegionDef.featureColorOverride(layer, feature) || layerColor(layer, index);
    return {
      color: color,
      weight: matched ? 3 : 1,
      opacity: matched ? 1 : 0.7,
      fillColor: color,
      fillOpacity: matched ? 0.25 : 0.06,
      dashArray: DASHES[index % DASHES.length]
    };
  }

  // One L.geoJSON per layer (canvas-rendered). Built on first show, so a heavy layer that
  // starts hidden costs nothing until the user ticks it.
  function ensureGroup(entry) {
    if (entry.group) return entry.group;
    var layer = entry.layer, i = entry.index;
    entry.group = L.geoJSON(layer.features.map(function (e) { return e.feature; }), {
      style: function (f) { return featureStyle(layer, i, f, false); },
      onEachFeature: function (feature, lyr) {
        lyr._rdFeature = feature;
        // DOM node, not an HTML string: feature names are data and must never be parsed as markup.
        var nm = window.RegionDef.featureName(feature, layer.nameProperty);
        lyr.bindTooltip(el('span', null, layer.label + ': ' + (nm || 'unnamed')), { sticky: true });
      }
    });
    if (S.lastResult) restyleEntry(entry, S.lastResult);
    return entry.group;
  }

  function drawLayers() {
    S.loaded.forEach(function (entry, i) {
      if (!entry.layer) return;
      entry.index = i;
      entry.visible = entry.layer.vertices <= HEAVY_VERTICES;
      if (entry.visible) ensureGroup(entry).addTo(S.map);
    });
  }

  function restyleEntry(entry, result) {
    var per = null;
    for (var k = 0; k < result.perLayer.length; k++) if (result.perLayer[k].id === entry.layer.id) per = result.perLayer[k];
    var hit = per && per.match ? per.match.feature : null;
    entry.group.eachLayer(function (gj) {
      var matched = !!hit && gj._rdFeature === hit && entry.layer.highlight;
      gj.setStyle(featureStyle(entry.layer, entry.index, gj._rdFeature, matched));
      if (matched && gj.bringToFront) gj.bringToFront();
    });
  }

  function restyle(result) {
    S.lastResult = result;
    S.loaded.forEach(function (entry) {
      if (entry.layer && entry.group) restyleEntry(entry, result);
    });
  }

  function fitSmart(lat, lon) {
    S.map.setView([lat, lon], Math.max(S.map.getZoom(), 9));
  }

  // ---- results -------------------------------------------------------------------

  function renderResult(result, label) {
    var out = S.out;
    while (out.firstChild) out.removeChild(out.firstChild);

    if (label) out.appendChild(el('p', 'rd-where', label));

    // A command missing a layer's code would be a wrong `region def` if pasted, so say so loudly.
    // Required layers (the default) withhold the command; `"optional": true` layers only flag it.
    var problems = [];
    S.loaded.forEach(function (e) {
      if (!e.layer) problems.push({ text: e.error, optional: e.def.optional === true });
    });
    result.perLayer.forEach(function (row, idx) {
      if (!row.unnamed) return;
      var def = S.usableDefs[idx];
      problems.push({ text: row.label + ': the point is inside a polygon with no usable code', optional: !!(def && def.optional === true) });
    });
    result.perLayer.forEach(function (row, idx) {
      if (!row.gap) return;
      var def = S.usableDefs[idx];
      problems.push({ text: row.label + ': no region covers this point, but the layer declares full coverage (data gap)', optional: !!(def && def.optional === true) });
    });
    (result.blocked || []).forEach(function (text) { problems.push({ text: text, optional: false }); });
    var blocking = problems.filter(function (p) { return !p.optional; });
    var optionalOnly = problems.filter(function (p) { return p.optional; });

    if (blocking.length) {
      var alertBox = el('div', 'rd-incomplete');
      alertBox.setAttribute('role', 'alert');
      alertBox.appendChild(el('strong', null, 'Command withheld: the result would be incomplete.'));
      var bl = el('ul');
      blocking.forEach(function (p) { bl.appendChild(el('li', null, p.text)); });
      alertBox.appendChild(bl);
      out.appendChild(alertBox);
    } else if (!result.value) {
      out.appendChild(el('p', 'rd-none', 'This location is not inside any configured region.'));
    } else {
      if (optionalOnly.length) {
        var warn = el('div', 'rd-incomplete');
        warn.setAttribute('role', 'alert');
        warn.appendChild(el('strong', null, 'Incomplete result: this command is missing codes for optional layers.'));
        var wl = el('ul');
        optionalOnly.forEach(function (p) { wl.appendChild(el('li', null, p.text)); });
        warn.appendChild(wl);
        out.appendChild(warn);
      }
      out.appendChild(el('h3', 'rd-cmd-head', optionalOnly.length ? 'Type this into your repeater (incomplete)' : 'Type this into your repeater'));
      var pre = el('pre', 'rd-cmd', window.RegionDef.repeaterCommands(result.value));
      pre.setAttribute('tabindex', '0');
      pre.setAttribute('aria-label', 'Repeater commands');
      out.appendChild(pre);

      // What these commands do to a repeater that already has regions (see docs/region-def.md).
      var apply = el('div', 'rd-apply');
      apply.appendChild(el('strong', null, 'Before you apply this'));
      var al = el('ul');
      [
        '`region def` adds to the repeater\'s existing regions; it does not clear them. Run `region` first to see what is configured, and remove obsolete entries with `region remove <name>` (children before parents).',
        '`region allowf *` lets the repeater forward unscoped (wildcard) flood traffic. Leave that line out if this repeater should only forward region-scoped floods.',
        '`region save` is last on purpose: it is the only command that writes to flash.',
        'Afterwards run `region` to check the tree, and `region list allowed` to see where flooding is allowed.'
      ].forEach(function (t) { al.appendChild(el('li', null, t.replace(/`/g, ''))); });
      apply.appendChild(al);
      out.appendChild(apply);
    }

    // Warnings + per-layer rows live in a <details> so the command stays on screen on phones.
    var details = el('details', 'rd-details');
    var narrow = false;
    try { narrow = !!(window.matchMedia && window.matchMedia('(max-width: 800px)').matches); } catch (_) {}
    if (!narrow) details.open = true;
    details.appendChild(el('summary', null, 'Layer details'));

    var warnings = result.warnings.concat(S.loadWarnings);
    if (warnings.length) {
      var ul = el('ul', 'rd-warnings');
      warnings.forEach(function (w) { ul.appendChild(el('li', null, w)); });
      details.appendChild(ul);
    }

    var rows = el('ul', 'rd-rows');
    result.perLayer.forEach(function (row) {
      var li = el('li', 'rd-row');
      li.appendChild(el('span', 'rd-row-label', row.label));
      var inside = !!row.match;
      li.appendChild(el('span', 'rd-badge ' + (inside ? 'rd-b-in' : 'rd-b-out'), inside ? 'INSIDE' : 'OUTSIDE'));
      if (inside) {
        var txt = row.match.name + ' (' + row.match.code + ')';
        if (row.matches.length > 1) txt += ' - overlaps ' + (row.matches.length - 1) + ' more';
        li.appendChild(el('span', 'rd-row-name', txt));
      }
      rows.appendChild(li);
    });
    details.appendChild(rows);
    out.appendChild(details);
  }

  function lookup(lat, lon, label) {
    if (!S) return;
    var usableEntries = S.loaded.filter(function (e) { return e.layer; });
    var usable = usableEntries.map(function (e) { return e.layer; });
    S.usableDefs = usableEntries.map(function (e) { return e.def; });
    var result = window.RegionDef.buildRegionDef(usable, lat, lon);
    if (S.marker) S.map.removeLayer(S.marker);
    S.marker = L.circleMarker([lat, lon], {
      radius: 7, weight: 2, color: cssVar('--accent', '#3388ff'), fillColor: cssVar('--accent', '#3388ff'), fillOpacity: 0.8
    }).addTo(S.map);
    restyle(result);
    fitSmart(lat, lon);
    renderResult(result, label || (lat.toFixed(5) + ', ' + lon.toFixed(5)));
    S.latlon = { lat: lat, lon: lon };
  }

  function setUrl(params) {
    var qs = new URLSearchParams();
    Object.keys(params).forEach(function (k) { if (params[k] !== null && params[k] !== undefined) qs.set(k, params[k]); });
    var s = qs.toString();
    try { history.replaceState(null, '', '#/tools/region-def' + (s ? '?' + s : '')); } catch (_) {}
  }

  // ---- geocoding ------------------------------------------------------------------

  function showMessage(text, isError) {
    S.msg.textContent = text || '';
    S.msg.className = 'rd-msg' + (isError ? ' rd-msg-err' : '');
  }

  // Plain-language geocoder failures (never echo the request URL: it contains the typed address).
  function explainGeocodeError(err) {
    var m = (err && err.message) || '';
    if (/HTTP 429/.test(m)) return 'the geocoder is rate limiting requests; wait a moment and try again';
    var h = /HTTP (\d+)/.exec(m);
    if (h) return 'the geocoder returned HTTP ' + h[1];
    if (/timed out/.test(m)) return 'the geocoder did not answer in time';
    if (/invalid JSON/.test(m)) return 'the geocoder sent an unreadable reply';
    return 'network error (offline, or the geocoder is blocked?)';
  }

  function geocode(q) {
    var last = lastGeocode;
    try { last = Math.max(last, parseInt(sessionStorage.getItem(GEOCODE_KEY), 10) || 0); } catch (_) {}
    var wait = Math.max(0, last + MIN_GEOCODE_GAP_MS - Date.now());
    return new Promise(function (r) { setTimeout(r, wait); }).then(function () {
      lastGeocode = Date.now();
      try { sessionStorage.setItem(GEOCODE_KEY, String(lastGeocode)); } catch (_) {}
      var url = NOMINATIM + '?format=jsonv2&limit=5&q=' + encodeURIComponent(q);
      return fetchJSON(url, cfgMs('geocodeTimeoutMs', DEFAULT_GEOCODE_TIMEOUT_MS), { headers: { 'Accept': 'application/json' } });
    });
  }

  function showCandidates(list) {
    var box = S.cands;
    while (box.firstChild) box.removeChild(box.firstChild);
    list.forEach(function (c) {
      var b = el('button', 'btn rd-cand', c.display_name);
      b.type = 'button';
      b.addEventListener('click', function () { pickCandidate(c); });
      box.appendChild(b);
    });
  }

  function pickCandidate(c) {
    var lat = parseFloat(c.lat), lon = parseFloat(c.lon);
    if (!isFinite(lat) || !isFinite(lon)) { showMessage('Geocoder returned an unusable location.', true); return; }
    showCandidates([]);
    showMessage('');
    lookup(lat, lon, c.display_name);
    setUrl({ lat: lat.toFixed(5), lon: lon.toFixed(5) });
  }

  function submit(raw) {
    if (!S) return;
    var text = (raw || '').trim();
    showMessage('');
    showCandidates([]);
    if (!text) { showMessage('Enter an address or a "lat, lon" pair.', true); return; }
    var ll = window.RegionDef.parseLatLon(text);
    if (ll) {
      lookup(ll.lat, ll.lon);
      setUrl({ lat: ll.lat, lon: ll.lon });
      return;
    }
    if (S.busy) return;
    S.busy = true;
    S.btn.disabled = true;
    showMessage('Searching...');
    var mine = S;
    geocode(text).then(function (list) {
      if (S !== mine) return;
      if (!Array.isArray(list) || !list.length) { showMessage('No match found for that address.', true); return; }
      showMessage('');
      setUrl({ q: text });
      if (list.length === 1) pickCandidate(list[0]);
      else { showMessage('Several matches; pick one:'); showCandidates(list); }
    }).catch(function (err) {
      if (S === mine) showMessage('Address lookup failed: ' + explainGeocodeError(err), true);
    }).then(function () {
      if (S === mine) { S.busy = false; S.btn.disabled = false; }
    });
  }

  // ---- layer toggles ------------------------------------------------------------------

  function renderToggles() {
    var box = S.toggles;
    S.loaded.forEach(function (entry, i) {
      if (!entry.layer) return;
      var lab = el('label', 'rd-toggle');
      var cb = document.createElement('input');
      cb.type = 'checkbox';
      cb.checked = !!entry.visible;
      cb.addEventListener('change', function () {
        if (cb.checked) ensureGroup(entry).addTo(S.map); else if (entry.group) S.map.removeLayer(entry.group);
      });
      var sw = el('span', 'rd-swatch');
      sw.style.borderTopColor = layerColor(entry.layer, i);
      sw.style.borderTopStyle = SWATCH_STYLES[i % SWATCH_STYLES.length];
      lab.appendChild(cb);
      lab.appendChild(sw);
      lab.appendChild(el('span', null, entry.layer.label));
      box.appendChild(lab);
    });
  }

  // ---- lifecycle ------------------------------------------------------------------------

  function init(app) {
    app.innerHTML =
      '<div class="rd-page">' +
        '<div class="rd-panel">' +
          '<h2>Region Def</h2>' +
          '<p class="help-text">Enter the repeater\'s address (or <code>lat, lon</code>) or click the map. ' +
            'The address is sent to OpenStreetMap Nominatim to find its coordinates.</p>' +
          '<div class="rd-input-row">' +
            '<label class="rd-sr" for="rd-input">Address or coordinates</label>' +
            '<input type="text" id="rd-input" class="input" autocomplete="off" placeholder="123 Main St, Cincinnati OH  or  39.10, -84.51">' +
            '<button type="button" id="rd-go" class="btn btn-primary">Look up</button>' +
          '</div>' +
          '<div id="rd-msg" class="rd-msg" role="status" aria-live="polite"></div>' +
          '<div id="rd-cands" class="rd-cands"></div>' +
          '<div id="rd-out" class="rd-out" aria-live="polite"></div>' +
          '<div id="rd-toggles" class="rd-toggles"></div>' +
          '<div id="rd-tilestatus" class="help-text"></div>' +
        '</div>' +
        '<div id="rd-map" class="rd-map" role="application" aria-label="Map"></div>' +
      '</div>';

    S = {
      map: null, tiles: null, marker: null, loaded: [], loadWarnings: [], latlon: null,
      busy: false, lastResult: null, observer: null, onTheme: null,
      input: app.querySelector('#rd-input'), btn: app.querySelector('#rd-go'),
      msg: app.querySelector('#rd-msg'), cands: app.querySelector('#rd-cands'),
      out: app.querySelector('#rd-out'), toggles: app.querySelector('#rd-toggles'),
      tileStatus: app.querySelector('#rd-tilestatus')
    };
    var mine = S;

    S.map = L.map('rd-map', { zoomControl: true, preferCanvas: true }).setView([39.5, -98.35], 4);
    syncTiles();
    S.onTheme = function () { syncTiles(); };
    window.addEventListener('mc-tile-provider-changed', S.onTheme);
    window.addEventListener('theme-changed', S.onTheme);
    if (typeof MutationObserver === 'function') {
      S.observer = new MutationObserver(S.onTheme);
      S.observer.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
    }
    setTimeout(function () { if (S === mine && S.map) S.map.invalidateSize(); }, 0);

    S.btn.addEventListener('click', function () { submit(S.input.value); });
    S.input.addEventListener('keydown', function (e) { if (e.key === 'Enter') submit(S.input.value); });
    S.map.on('click', function (e) {
      if (!S || !S.loaded.length) return;
      var lat = e.latlng.lat, lon = ((e.latlng.lng + 540) % 360) - 180; // wrap panned-around worlds
      S.input.value = lat.toFixed(5) + ', ' + lon.toFixed(5);
      lookup(lat, lon);
      setUrl({ lat: lat.toFixed(5), lon: lon.toFixed(5) });
    });

    showMessage('Loading region layers...');
    S.btn.disabled = true;
    loadAll().then(function (res) {
      if (S !== mine) return;
      var loaded = res.entries;
      S.loaded = loaded;
      S.loadWarnings = res.warnings.slice();
      loaded.forEach(function (e) {
        if (e.error) S.loadWarnings.push(e.error);
        e.warnings.forEach(function (w) { S.loadWarnings.push(w); });
      });
      if (!loaded.some(function (e) { return e.layer; })) {
        showMessage('No region layers could be loaded.', true);
        S.out.appendChild(el('p', 'rd-none', S.loadWarnings.join(' ')));
        return;
      }
      showMessage('');
      S.btn.disabled = false;
      drawLayers();
      renderToggles();
      var p = hashParams();
      var ll = window.RegionDef.parseLatLon((p.get('lat') || '') + ',' + (p.get('lon') || ''));
      if (ll) { S.input.value = ll.lat + ', ' + ll.lon; lookup(ll.lat, ll.lon); }
      else if (p.get('q')) { S.input.value = p.get('q'); submit(p.get('q')); }
    }, function (err) {
      if (S === mine) showMessage('Could not load region layers: ' + (err && err.message ? err.message : 'error'), true);
    });
  }

  function destroy() {
    if (!S) return;
    window.removeEventListener('mc-tile-provider-changed', S.onTheme);
    window.removeEventListener('theme-changed', S.onTheme);
    if (S.observer) S.observer.disconnect();
    if (S.map) S.map.remove();
    S = null;
  }

  if (typeof registerPage === 'function') registerPage('region-def', { init: init, destroy: destroy });
})();
