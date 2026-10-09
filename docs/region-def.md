# Region Def tool

`#/tools/region-def` turns an address (or `lat, lon`, or a map click) into the repeater
commands that set its regions:

```
region def us-midwest oki ohio cvg
region save
region allowf *
```

Each configured layer contributes at most one code, in manifest order (top to bottom).

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
  "layers": [
    { "id": "macro", "label": "Macro region",
      "url": [ { "url": "macro-midwest.geojson", "name": "US-Midwest" },
               { "url": "macro-southeast.geojson", "name": "US-Southeast" } ] },
    { "id": "mesh",  "label": "Mesh region", "url": "states.geojson",
      "nameProperty": "shapeName", "groupBy": "meshregion" },
    { "id": "state", "label": "State", "url": "states.geojson", "nameProperty": "shapeName" },
    { "id": "metro", "label": "Metro", "url": "metros.geojson",
      "nameProperty": "iata", "colorBy": "feature", "highlight": true }
  ]
}
```

| Field | Meaning |
|---|---|
| `id`, `label` | identifier and the name shown in the results list |
| `url` | file path relative to `layers.json` (same site only), or an array of paths / `{url, name}` objects |
| `{url, name}` | all polygons in that file are merged into one region called `name` |
| `nameProperty` | feature property holding the name; an array means "first non-empty" |
| `groupBy` | merge features sharing this property value into one region named by that value; features lacking it are dropped |
| `color` | fixed layer colour (otherwise the theme palette `--region-layer-1..8`) |
| `colorBy: "feature"` | use each feature's own `properties.color` |
| `highlight` | `false` stops the matched polygon being emphasised on the map |

Codes are lowercased, whitespace becomes `-`, and anything outside `a-z 0-9 _ -` is removed.
If several polygons of one layer contain the point, the first is used and a warning is shown.
A layer that fails to load produces a warning; the others still work. A point on a polygon
edge counts as inside; a point in a hole is outside. GeoJSON order is `[lon, lat]`.

## Privacy and limits

- The typed address is sent from the visitor's browser to OpenStreetMap Nominatim. The page
  spaces requests at least 1.1 s apart (their usage policy is 1 request/second).
  Coordinates and map clicks are never sent anywhere.
- Basemap tiles use the existing tile configuration (`map.tiles` in `config.json`,
  `map-tile-providers.js`), so the Carto key is handled exactly as on the Map page.

## Follow-up

The page re-implements theme-synced tile switching that `map.js` (`_syncDarkTiles`) also
does. Extracting that into a shared helper is the cleaner end state.