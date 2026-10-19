#!/usr/bin/env node
// Small dev server for the CoreScope front end (no dependencies, Node 14+).
//
//   node scripts/dev-server.js                       -> http://0.0.0.0:13581/#/tools/region-def
//
// Environment:
//   PORT            listen port (default 13581)
//   HOST            bind address (default 0.0.0.0, needed inside Docker)
//   PUBLIC_DIR      front end files (default ./public)
//   REGION_LAYERS   folder served at /region-layers/ (default: public/region-layers if it has a layers.json,
//                   otherwise tests/fixtures/region-layers)
//   API_TARGET      e.g. http://localhost:3000 - forward /api/* there; unset answers /api/* with {}
//
// Files are re-read on every request and sent with no-cache, so edit and refresh.
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = Number(process.env.PORT || 13581);
const HOST = process.env.HOST || '0.0.0.0';
const ROOT = path.resolve(process.env.PUBLIC_DIR || 'public');
const own = path.join(ROOT, 'region-layers');
const LAYERS = path.resolve(process.env.REGION_LAYERS ||
  (fs.existsSync(path.join(own, 'layers.json')) ? own : path.join('tests', 'fixtures', 'region-layers')));
const API = process.env.API_TARGET ? new URL(process.env.API_TARGET) : null;

const TYPES = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8',
  '.json': 'application/json', '.geojson': 'application/json', '.svg': 'image/svg+xml', '.png': 'image/png',
  '.jpg': 'image/jpeg', '.ico': 'image/x-icon', '.woff2': 'font/woff2', '.map': 'application/json'
};

function inside(base, rel) {
  const f = path.resolve(base, '.' + path.sep + rel);
  return f === base || f.startsWith(base + path.sep) ? f : null;
}
function send(res, code, type, body) {
  res.writeHead(code, { 'content-type': type, 'cache-control': 'no-store' });
  res.end(body);
}
function sendFile(res, file) {
  fs.readFile(file, (err, data) => {
    if (err) return send(res, 404, 'text/plain', 'not found');
    const type = TYPES[path.extname(file).toLowerCase()] || 'application/octet-stream';
    if (file.endsWith('index.html')) data = Buffer.from(data.toString().replace(/__BUST__/g, String(Date.now())));
    send(res, 200, type, data);
  });
}

http.createServer((req, res) => {
  let url;
  try { url = decodeURIComponent(req.url.split('?')[0]); } catch (_) { return send(res, 400, 'text/plain', 'bad url'); }

  if (url.startsWith('/api/')) {
    if (!API) return send(res, 200, 'application/json', '{}');
    const up = http.request({ host: API.hostname, port: API.port, path: req.url, method: req.method, headers: Object.assign({}, req.headers, { host: API.host }) },
      (r) => { res.writeHead(r.statusCode, r.headers); r.pipe(res); });
    up.on('error', (e) => send(res, 502, 'text/plain', 'API_TARGET unreachable: ' + e.message));
    return req.pipe(up);
  }

  if (url.startsWith('/region-layers/')) {
    const f = inside(LAYERS, url.slice('/region-layers/'.length));
    return f ? sendFile(res, f) : send(res, 404, 'text/plain', 'not found');
  }

  let f = inside(ROOT, url === '/' ? 'index.html' : url);
  if (!f || !fs.existsSync(f) || fs.statSync(f).isDirectory()) f = path.join(ROOT, 'index.html'); // SPA fallback
  sendFile(res, f);
}).listen(PORT, HOST, () => {
  console.log('CoreScope dev server  http://localhost:' + PORT + '/#/tools/region-def');
  console.log('  front end     ' + ROOT);
  console.log('  region layers ' + LAYERS);
  console.log('  /api          ' + (API ? 'proxied to ' + API.href : 'stubbed with {}'));
});
