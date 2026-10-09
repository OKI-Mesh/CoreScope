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
  var MAX_LAYERS = 16;
  var OSM_FALLBACK = 'https://tile.openstreetmap.org/{z}/{x}/{y}.png';
  var PALETTE_SIZE = 8;

  var S = null; // per-visit state; null when the page is not mounted

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

  function isLocalPath(u) {
    return typeof u === 'string' && u.length > 0 && !/^[a-z][a-z0-9+.-]*:/i.test(u) && u.indexOf('//') !== 0;
  }

  function fetchJSON(url) {
    return fetch(url, { cache: 'no-cache' }).then(function (r) {
      if (!r.ok) throw new Error('HTTP ' + r.status + ' for ' + url);
      return r.json();
    });
  }

  // def.url: string | [string | {url, name}]  ->  [{url, name}]
  function fileList(def) {
    var raw = Array.isArray(def.url) ? def.url : [def.url];
    return raw.map(function (u) {
      return (u && typeof u === 'object') ? { url: u.url, name: u.name || null } : { url: u, name: null };
    });
  }

  function loadLayer(def) {
    var files = fileList(def);
    return Promise.all(files.map(function (f) {
      if (!isLocalPath(f.url)) return Promise.reject(new Error('only same-site relative URLs are allowed'));
      return fetchJSON(LAYER_DIR + f.url).then(function (data) { return { name: f.name, data: data }; });
    })).then(function (loaded) {
      return window.RegionDef.buildLayer(def, loaded);
    });
  }

  function loadAll() {
    return fetchJSON(LAYER_DIR + 'layers.json').then(function (manifest) {
      var defs = (manifest && Array.isArray(manifest.layers)) ? manifest.layers.slice(0, MAX_LAYERS) : [];
      if (!defs.length) throw new Error('layers.json has no layers');
      return Promise.all(defs.map(function (def, i) {
        def = def || {};
        if (!def.id) def.id = 'layer' + (i + 1);
        return loadLayer(def).then(function (layer) {
          layer.def = def;
          return { layer: layer, error: null };
        }, function (err) {
          return { layer: null, def: def, error: (def.label || def.id) + ': ' + (err && err.message ? err.message : 'failed to load') };
        });
      }));
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
      fillOpacity: matched ? 0.25 : 0.06
    };
  }

  function drawLayers() {
    S.loaded.forEach(function (entry, i) {
      var layer = entry.layer;
      if (!layer) return;
      var group = L.layerGroup();
      layer.features.forEach(function (e) {
        var gj = L.geoJSON(e.feature, { style: function (f) { return featureStyle(layer, i, f, false); } });
        gj._rdFeature = e.feature;
        group.addLayer(gj);
      });
      entry.group = group;
      entry.index = i;
      group.addTo(S.map);
    });
  }

  function restyle(result) {
    S.loaded.forEach(function (entry, i) {
      if (!entry.layer || !entry.group) return;
      var per = null;
      for (var k = 0; k < result.perLayer.length; k++) if (result.perLayer[k].id === entry.layer.id) per = result.perLayer[k];
      var hit = per && per.match ? per.match.feature : null;
      entry.group.eachLayer(function (gj) {
        var matched = !!hit && gj._rdFeature === hit && entry.layer.highlight;
        gj.setStyle(featureStyle(entry.layer, i, gj._rdFeature, matched));
        if (matched && gj.bringToFront) gj.bringToFront();
      });
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

    if (!result.value) {
      out.appendChild(el('p', 'rd-none', 'This location is not inside any configured region.'));
    } else {
      out.appendChild(el('h3', 'rd-cmd-head', 'Type this into your repeater'));
      var pre = el('pre', 'rd-cmd', window.RegionDef.repeaterCommands(result.value));
      pre.setAttribute('tabindex', '0');
      pre.setAttribute('aria-label', 'Repeater commands');
      out.appendChild(pre);
    }

    var warnings = result.warnings.concat(S.loadWarnings);
    if (warnings.length) {
      var ul = el('ul', 'rd-warnings');
      warnings.forEach(function (w) { ul.appendChild(el('li', null, w)); });
      out.appendChild(ul);
    }

    var rows = el('ul', 'rd-rows');
    result.perLayer.forEach(function (row) {
      var li = el('li', 'rd-row');
      li.appendChild(el('span', 'rd-row-label', row.label));
      var inside = !!row.match;
      li.appendChild(el('span', 'rd-badge ' + (inside ? 'rd-b-in' : 'rd-b-out'), inside ? 'INSIDE' : 'OUTSIDE'));
      if (inside) li.appendChild(el('span', 'rd-row-name', row.match.name + ' (' + row.match.code + ')'));
      rows.appendChild(li);
    });
    out.appendChild(rows);
  }

  function lookup(lat, lon, label) {
    if (!S) return;
    var usable = S.loaded.filter(function (e) { return e.layer; }).map(function (e) { return e.layer; });
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

  function geocode(q) {
    var wait = Math.max(0, S.lastGeocode + MIN_GEOCODE_GAP_MS - Date.now());
    return new Promise(function (r) { setTimeout(r, wait); }).then(function () {
      S.lastGeocode = Date.now();
      var url = NOMINATIM + '?format=jsonv2&limit=5&q=' + encodeURIComponent(q);
      return fetch(url, { headers: { 'Accept': 'application/json' } });
    }).then(function (r) {
      if (!r.ok) throw new Error('Geocoder returned HTTP ' + r.status);
      return r.json();
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
      if (S === mine) showMessage('Address lookup failed: ' + (err && err.message ? err.message : 'network error'), true);
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
      cb.checked = true;
      cb.addEventListener('change', function () {
        if (cb.checked) entry.group.addTo(S.map); else S.map.removeLayer(entry.group);
      });
      var sw = el('span', 'rd-swatch');
      sw.style.background = layerColor(entry.layer, i);
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
      busy: false, lastGeocode: 0, observer: null, onTheme: null,
      input: app.querySelector('#rd-input'), btn: app.querySelector('#rd-go'),
      msg: app.querySelector('#rd-msg'), cands: app.querySelector('#rd-cands'),
      out: app.querySelector('#rd-out'), toggles: app.querySelector('#rd-toggles'),
      tileStatus: app.querySelector('#rd-tilestatus')
    };
    var mine = S;

    S.map = L.map('rd-map', { zoomControl: true }).setView([39.5, -98.35], 4);
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
    loadAll().then(function (loaded) {
      if (S !== mine) return;
      S.loaded = loaded;
      S.loadWarnings = loaded.filter(function (e) { return e.error; }).map(function (e) { return e.error; });
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