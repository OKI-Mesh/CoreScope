# Region Def tool

`#/tools/region-def` turns an address (or `lat, lon`, or a map click) into the repeater
commands that set its regions:

```
region def us-midwest oki us-oh cvg
region save
region allowf *
```

Each configured layer contributes at most one code, in manifest order (top to bottom).

## Rollout scope

The tool is deliberately regional. It only knows the areas whose boundary files you publish in
`public/region-layers/`; today that is the Ohio/Kentucky/Indiana (OKI) part of the US Midwest and
its neighbours. A point in a state you have not mapped returns the plain message "not inside any
configured region" and no command. That is expected, not a bug. To extend coverage, add the new
boundaries to the existing files (or new files) and add known points for them (see
[Known-point tests](#known-point-tests)). Where a layer is meant to tile the whole mapped area,
mark it `"coverage": "full"` so a hole in the data cannot pass as "no region".

## Operating it

### When a layer is missing or failed

| Situation | What the page does | What you do |
|---|---|---|
| A required layer 404s, times out, or is not valid JSON/GeoJSON | Shows an alert naming the layer and file, and **withholds** the command | Fix or restore the file; reload |
| A required layer loaded but has no polygons | Same as above | Check the file is real GeoJSON with Polygon/MultiPolygon features |
| A polygon matched but has no valid code | Withholds the command and names the layer | Fix the property in the data (see canonical codes) |
| `coverage: "full"` layer misses a point that another layer covers | Withholds the command, warns of a data gap | Add the missing boundary, or drop `full` if the exclusion is real |
| An `optional: true` layer fails | Shows the command flagged "(incomplete)" and names the missing layer | Fine for metros and other nice-to-haves; fix when convenient |
| `layers.json` itself is missing, invalid, unsafe, or newer than the page | Stops with a message saying which | Fix the manifest |

Warnings that do not block: two regions in one layer overlap (the first wins, both are named), or
the same code is produced by two layers.

### Troubleshooting

- **"not inside any configured region" for a place you expected.** Open the "Details" section: each
  layer shows INSIDE/OUTSIDE. If every layer says OUTSIDE the area is not mapped, or the
  coordinates are swapped. Run
  `node scripts/check-region-data.js --point LAT,LON` to see what the data says without a browser.
- **A boundary is missing on the map.** Layers over 50,000 vertices start unticked; tick them in the
  toggles under the map. Otherwise the layer failed to load; the page lists it.
- **Command withheld / "incomplete".** See the table above. The message names the layer and
  the file.
- **"Could not load <file>".** Open `/region-layers/<file>` in the browser. 404: wrong name or not
  deployed. Not JSON: the server returned an HTML error page or a truncated file. Timeout: the file
  is large; simplify it.
- **A code looks wrong (`us-oh` vs `ohio`).** Codes come from the property named in
  `nameProperty` (slugged) or `codeProperty` (verbatim). See below.
- **Geocoder messages.** "rate limited" means Nominatim returned 429; wait a minute. Network or
  timeout errors do not affect coordinate or map-click lookups, which work offline of the geocoder.
- **Swapped coordinates.** Typing `lon, lat` is only caught automatically when a value falls
  outside its valid range (|latitude| above 90). A swap that stays in range, such as `-85, 39`
  for a Midwest point, is a valid coordinate somewhere else, so the page accepts it and finds
  nothing; check the "where" line under the map. Known-point tests catch a code-level swap.

## Layer files

Put static files in `public/region-layers/` (served at `/region-layers/`):

```
public/region-layers/
  layers.json
  macro-midwest.geojson
  mesh-regions.geojson
  states.geojson
  metros.geojson
```

Only publish boundary data you are happy to make public; the repo and the site are public.

### layers.json

```json
{
  "version": 1,
  "layers": [
    { "id": "macro", "label": "Macro region",
      "url": [ { "url": "macro-midwest.geojson", "name": "US-Midwest", "code": "us-midwest" },
               { "url": "macro-southeast.geojson", "name": "US-Southeast", "code": "us-southeast" } ],
      "codes": ["us-midwest", "us-southeast"], "coverage": "full" },
    { "id": "mesh",  "label": "Mesh region", "url": "states.geojson",
      "nameProperty": "shapeName", "groupBy": "meshregion", "codeProperty": "meshregion",
      "codes": ["oki", "swoh"] },
    { "id": "state", "label": "State", "url": "states.geojson",
      "nameProperty": "shapeName", "codeProperty": "stateCode" },
    { "id": "metro", "label": "Metro", "url": "metros.geojson",
      "nameProperty": "iata", "colorBy": "feature", "optional": true }
  ]
}
```

`version` is the manifest schema version (currently `1`). A missing version is accepted with a
warning; a version newer than the page understands is refused. Unknown layer fields produce a
warning (to catch typos such as `optionnal`); structural errors refuse the manifest and the page
says why.

| Field | Meaning |
|---|---|
| `id`, `label` | identifier (unique, letters/digits/`_`/`-`) and the name shown in the results list |
| `url` | file path relative to `layers.json`, or an array of paths / `{url, name, code}` objects. Strict allowlist: letters, digits, `_ - .`, `/` between folders, must end `.geojson` or `.json`; no `..`, no leading `/`, no URLs. Each distinct file is fetched once even if several layers use it |
| `{url, name, code}` | all polygons in that file are merged into one region. `name` is the display name; `code` is its canonical code (defaults to the slug of `name`) |
| `nameProperty` | feature property holding the display name; an array means "first non-empty" |
| `codeProperty` | feature property holding the canonical code. Used verbatim, never altered |
| `groupBy` | merge features sharing this property value into one region; features lacking it are dropped |
| `codes` | the canonical codes this layer may produce. A polygon whose code is not in the list can never produce a code |
| `coverage` | `"partial"` (default) or `"full"`; see below |
| `color` | fixed layer colour (otherwise the theme palette `--region-layer-1..8`) |
| `colorBy: "feature"` | use each feature's own `properties.color` |
| `highlight` | `false` stops the matched polygon being emphasised on the map |
| `optional` | `true` lets the page still show a command when this layer is unavailable (flagged as incomplete). Default: a layer that fails to load, has no valid polygons, hits a polygon with no usable code, or breaks its coverage promise makes the page withhold the command, because pasting a command with a missing code would set the wrong regions |

### Canonical codes

The codes go straight into `region def`, so they are validated rather than corrected:
lowercase letters, digits, `_` and `-`, starting with a letter or digit, at most 64 characters.
A code comes from, in order: a file's `code`, the layer's `codeProperty`, else the slug of the
display name (trimmed, lowercased, spaces to `-`, other characters removed). An explicit code that
is not valid is not slugged for you: that polygon simply cannot produce a code, and the page says so.
The canonical identifier format is therefore `^[a-z0-9][a-z0-9_-]{0,63}$`, for example
`us-midwest`, `oki`, `us-oh`, `cvg`. The tool never emits anything else.

Which property to use matters. With a geoBoundaries-style dataset the state has
`shapeName: "Ohio"` and `shapeISO: "US-OH"`:

| Config | Code |
|---|---|
| `"nameProperty": "shapeName"` | `ohio` |
| `"nameProperty": "shapeISO"` | `us-oh` (the slug lowercases `US-OH`) |
| `"codeProperty": "shapeISO"` | **invalid**: `US-OH` is used verbatim and uppercase is not allowed, so every polygon is flagged |

Pick the one that matches what your repeaters expect and keep `layers.json.example`, your shipped
`layers.json` and this documentation in agreement (the example uses `shapeISO` so the commands
read `us-oh`). Declaring `codes` gives you a canonical list to check against; a typo in the data (`"OKl"`,
`"swoh "`) then shows up instead of silently becoming a new region.

### Missing regions: exclusion or gap?

By default a layer that does not contain the point just contributes no code. That is correct for
layers that only cover part of the map (a metro layer, say). For a layer that is supposed to tile
the whole area (macro regions, mesh regions) it hides data holes, so declare `"coverage": "full"`:
if the point is inside any other layer but outside this one, the page treats it as a data gap,
shows a warning, and withholds the command (unless the layer is also `optional`). A point outside
every layer still gets the plain "not inside any configured region" message.

## Performance

Polygons are drawn on a canvas, one object per layer. A layer with more than 50,000 vertices
starts unticked and is only built when ticked, and the page warns you at load. For big
boundary sets, simplify the files before publishing. The lookup uses the same geometry as the
drawing, so simplification moves the lookup edge as well; keep the tolerance small and re-run the
known-point tests afterwards.

```
node scripts/simplify-region-layers.js --in raw/ --out public/region-layers/ --tolerance 0.0005 --precision 5
node scripts/check-region-data.js --strict
```

`--tolerance` is in degrees (0.0005 is roughly 55 m) and `--precision` is decimal places. Rings
are simplified independently, so neighbours can drift apart by up to the tolerance along a shared
border; points within that sliver may resolve to either side. The script refuses to write output
that fails the integrity check or changes any known-point command. Serve the output as `.geojson` or `.json`; if your server
compresses by content type, make sure `application/geo+json` is on its list (or name the files
`.json`).

## Privacy and limits

- The typed address is sent from the visitor's browser to OpenStreetMap Nominatim. The page
  spaces requests at least 1.1 s apart, including across navigation within the session
  (their usage policy asks for at most 1 request/second; check their current policy).
  That spacing is per browser: it cannot coordinate between visitors. Visitors have separate
  IPs, which is how a browser-side tool is normally used, but if traffic grows, put an
  application-controlled geocoding endpoint (with caching) in front of it instead.
  Coordinates and map clicks are never sent to a geocoder or to CoreScope; but the map itself
  still requests tiles for the area on screen from the configured tile provider, which can infer
  roughly where the visitor is looking.
- Basemap tiles use the existing tile configuration (`map.tiles` in `config.json`,
  `map-tile-providers.js`), so the Carto key is handled exactly as on the Map page.

## Dataset sources and updates

Record where each file came from in a `SOURCES.md` next to `layers.json` (name, publisher, URL,
licence, retrieval date, any processing). The `shapeName`/`shapeISO` property names used in the
examples follow the geoBoundaries convention, but confirm the source of your own files and its
licence before publishing. To update:

1. Download the new release and put it in a working folder (not in `public/`).
2. Simplify (see Performance) and write the result to `public/region-layers/`.
3. Run `node scripts/check-region-data.js --strict` and `node tests/unit/test-region-def-known-points.js`.
4. Review the map visually at `#/tools/region-def`, spot-check border points, and commit data,
   `SOURCES.md` and any new known points together.

## Verified example

Produced with `node scripts/check-region-data.js --dir tests/fixtures/region-layers --point 39.1031,-84.5120`
against the **synthetic test fixture** (Cincinnati). Real boundary data should give the same
string; if it does not, one of them is wrong:

```
region def us-midwest oki us-oh cvg
region save
region allowf *
```

For your own deployment, generate the block with `--point`, then paste it into
`known-points.json` so it is checked on every CI run.

## Region data integrity in CI

`node scripts/check-region-data.js --strict` runs as its own step in `deploy.yml` right after the
unit tests (it passes with a note when no region data is deployed). It validates the manifest,
every referenced file, canonical codes, polygon validity, shared-edge overlap between regions in
a layer, and `known-points.json`. The browser tests (`tests/e2e/test-region-def-e2e.js`) mock
degraded states with fixtures and also run a smoke test against the deployed
`/region-layers/` when present.

## Tests

- `tests/unit/test-region-def.js`: geometry, codes, coverage, manifest validation, wiring.
- `tests/unit/test-region-layers-manifest.js`: `layers.json.example` must always validate. If you
  commit a real `public/region-layers/layers.json` it is checked too: every file exists and
  parses, every layer has polygons, every polygon has a canonical code (in the layer's `codes`
  list if one is declared), every declared code is produced by some polygon, and no code repeats
  inside a layer. Run it in CI so boundary-data edits cannot break region defs unnoticed.

- `tests/unit/test-check-region-data.js`, `tests/unit/test-simplify-region-layers.js`: the integrity
  script and the simplifier, including failure cases.
- `tests/e2e/test-region-def-e2e.js`: exact commands and messages, geocoder and layer failure modes,
  timeouts, overlap, coverage gaps, escaping of hostile text, mobile (320/375/768) and small-laptop
  layouts, and a production-data smoke test. CI fails if this script exits non-zero (the suite runs
  under `bash -o pipefail`, so `| tee` does not hide failures).

### Known-point tests

`tests/unit/test-region-def-known-points.js` always runs a table of real cities (Ohio, Indiana,
Kentucky, and states outside the mesh region such as Michigan and Pennsylvania) against a coarse
built-in fixture, which guards the lookup, grouping and ordering logic. To guard your actual
boundary data, copy `public/region-layers/known-points.json.example` to
`public/region-layers/known-points.json`, edit the expected strings to what your data should
produce, and commit it next to `layers.json`; the same test then evaluates every row against your
real files and fails on any difference or unexpected warning (`"allowWarnings": true` per row
opts out, e.g. for a point deliberately on a shared border). Include points just either side of
borders you care about, and at least one point that should produce no mesh code.

## Follow-up

The page re-implements theme-synced tile switching that `map.js` (`_syncDarkTiles`) also
does. Extracting that into a shared helper is the cleaner end state.
