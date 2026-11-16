# MeshCore MQTT Ingestor (Go)

Standalone MQTT ingestion service for CoreScope. Connects to MQTT brokers, decodes raw MeshCore packets, and writes them to the Postgres database the web server reads.

## Architecture

```
MQTT Broker(s)  →  Go Ingestor  →  Postgres  ←  Go Web Server
                    (this binary)    (shared)
```

- **Single static binary** — no runtime dependencies, no CGO
- **Postgres** via `github.com/jackc/pgx/v5`; the schema is owned by the goose migrations in `internal/database/postgres/migrations`, which the ingestor applies on start
- **MQTT** via `github.com/eclipse/paho.mqtt.golang`
- Runs **alongside** the Go server; they share the database
- Does NOT serve HTTP/WebSocket — that stays in the server

## Build

Requires the Go version in the root `go.mod`.

```bash
go build -o corescope-ingestor ./cmd/ingestor
```

The binary is pure Go, so cross-compiling needs no C toolchain:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o corescope-ingestor ./cmd/ingestor
```

## Run

```bash
./corescope-ingestor -config /path/to/config.json
```

The ingestor reads `config.json`: the `mqttSources` array (or legacy `mqtt` object) and the `db` block.

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | Postgres connection string, used when `db.connectionString` is not set | — |
| `MQTT_BROKER` | Single MQTT broker URL (overrides config) | — |
| `MQTT_TOPIC` | MQTT topic (used with `MQTT_BROKER`) | `meshcore/#` |
| `CORESCOPE_INGESTOR_STATS` | Path to the per-second stats JSON file consumed by the server's `/api/perf/io` and `/api/perf/write-sources` endpoints (#1120) | `/tmp/corescope-ingestor-stats.json` |

### Stats file (`CORESCOPE_INGESTOR_STATS`)

Every second the ingestor publishes a JSON snapshot of its counters
(`tx_inserted`, `obs_inserted`, etc.) plus
a `procIO` block sampled from `/proc/self/io` (read/write/cancelled bytes per
second + syscall counts). The server reads this file and surfaces the data on
the Perf page so operators can self-diagnose write-volume anomalies.

The writer uses `O_NOFOLLOW | O_CREAT | O_TRUNC` mode `0o600`, so a
pre-planted symlink at the path cannot be used to clobber an arbitrary file.

**Security note:** the default lives in `/tmp`, which is world-writable on
most hosts (sticky bit only protects deletion, not creation). On
shared/multi-tenant hosts, override `CORESCOPE_INGESTOR_STATS` to point at a
private directory (e.g. `/var/lib/corescope/ingestor-stats.json`) that only
the corescope user can write to.

### Minimal Config

```json
{
  "db": { "connectionString": "postgres://corescope:PASSWORD@localhost:5432/corescope" },
  "mqttSources": [
    {
      "name": "local",
      "broker": "mqtt://localhost:1883",
      "topics": ["meshcore/#"]
    }
  ]
}
```

### Full Config

The ingestor reads these fields from `config.json`:

- `mqttSources[]` — array of MQTT broker connections
  - `name` — display name for logging
  - `broker` — MQTT URL (`mqtt://`, `mqtts://`)
  - `username` / `password` — auth credentials
  - `topics` — array of topic patterns to subscribe
  - `iataFilter` — optional regional filter
  - `clientId`: optional MQTT ClientID. Default `corescope-<name>-<random>`, new suffix per ingestor start. Two running ingestors must not share a value on one broker
- `mqtt` — legacy single-broker config (auto-converted to `mqttSources`)
- `db.connectionString` — Postgres DSN (falls back to `$DATABASE_URL`)

## Test

```bash
cd cmd/ingestor
go test -v ./...
```

## What It Does

1. Connects to configured MQTT brokers with auto-reconnect
2. Subscribes to mesh packet topics (e.g., `meshcore/+/+/packets`)
3. Receives raw hex packets via JSON messages (`{ "raw": "...", "SNR": ..., "RSSI": ... }`)
4. Decodes MeshCore packet headers, paths, and payloads (ported from `decoder.js`)
5. Computes content hashes (path-independent, SHA-256-based)
6. Writes to Postgres: `transmissions` + `observations` tables
7. Upserts `nodes` from decoded ADVERT packets (with validation)
8. Upserts `observers` from MQTT topic metadata

## Schema

The tables are defined by the goose migrations in `internal/database/postgres/migrations`. The ingestor applies pending migrations when it starts, before it subscribes to MQTT; `corescope-migrate` (`cmd/migrate`) runs them by hand. The main tables:

- `transmissions` — deduplicated by content hash
- `observations` — per-observer sightings, referencing `observers(id)`
- `nodes` — mesh nodes discovered from adverts
- `observers` — MQTT feed sources

The server and the ingestor are separate processes writing to one Postgres database; Postgres handles the concurrency.

## Files

`main.go` is the entry point and MQTT message handler; `store.go`, `transmissions.go`, `observers.go`, `nodes.go` and `observer_metrics.go` write to the database; `maintenance.go` and `prune_geofilter.go` do retention; `ingest_buffer.go` is the startup buffer; `config.go` holds the config struct and loader. `ls cmd/ingestor` has the rest.
