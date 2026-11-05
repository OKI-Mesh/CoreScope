# corescope-decrypt

Standalone CLI tool to decrypt and export MeshCore hashtag channel messages from a CoreScope Postgres database.

## Why

MeshCore hashtag channels use symmetric encryption where the key is derived deterministically from the channel name. The CoreScope ingestor stores **all** `GRP_TXT` packets in the database, including those it cannot decrypt at ingest time.

This tool enables:

- **Retroactive decryption** — decrypt historical messages for any channel whose name you learn after the fact
- **Forensics & analysis** — export channel traffic for offline review
- **Bulk export** — dump an entire channel's history as JSON, HTML, or plain text

## Installation

### From Docker image

The binary is included in the CoreScope Docker image at `/app/corescope-decrypt`. It reads the database connection string from `$DATABASE_URL`, so inside the CoreScope container no extra flag is needed:

```bash
docker exec corescope-prod /app/corescope-decrypt --channel "#wardriving"
```

### From GitHub release

Download the static binary from the [Releases](https://github.com/Kpa-clawbot/CoreScope/releases) page:

```bash
# Linux amd64
curl -LO https://github.com/Kpa-clawbot/CoreScope/releases/latest/download/corescope-decrypt-linux-amd64
chmod +x corescope-decrypt-linux-amd64
./corescope-decrypt-linux-amd64 --help
```

### Build from source

```bash
cd cmd/decrypt
CGO_ENABLED=0 go build -ldflags="-s -w" -o corescope-decrypt .
```

The binary is statically linked — no dependencies, runs on any Linux.

## Connecting to the database

The tool needs a Postgres connection string. It reads `$DATABASE_URL` by default, or you can pass `--dsn`:

```bash
export DATABASE_URL='postgres://corescope:secret@db.example.com:5432/corescope'
corescope-decrypt --channel "#wardriving"
```

Prefer the environment variable: the connection string contains a password, and a command-line flag shows up in the process list.

The connection is read-only. The tool never modifies the database.

## Usage

```
corescope-decrypt --channel NAME [--dsn URL] [--format FORMAT] [--output FILE]
```

Run `corescope-decrypt --help` for full flag documentation.

### JSON output (default)

Machine-readable, includes all metadata (observers, path hops, raw hex):

```bash
corescope-decrypt --channel "#wardriving"
```

```json
[
  {
    "hash": "a1b2c3...",
    "timestamp": "2026-04-12T17:19:09Z",
    "sender": "XMD Tag 1",
    "message": "@[MapperBot] 37.76985, -122.40525 [0.3w]",
    "channel": "#wardriving",
    "raw_hex": "150206...",
    "path": ["A3", "B0"],
    "observers": [
      {"name": "Observer1", "snr": 9.5, "rssi": -56, "timestamp": "2026-04-12T17:19:10Z"}
    ]
  }
]
```

`path` is read from the stored frame: the hop hashes the packet carried, as uppercase hex. `observers` lists every observer that heard the transmission, oldest first. An observer with no name, or a reception with no SNR or RSSI, shows an empty name or `0`.

### HTML output

Self-contained interactive viewer — search, sortable columns, expandable detail rows:

```bash
corescope-decrypt --channel "#wardriving" --format html --output wardriving.html
open wardriving.html
```

No external dependencies. The JSON data is embedded directly in the HTML file.

### IRC / log output

Plain-text, one line per message — ideal for `grep`, `awk`, and piping:

```bash
corescope-decrypt --channel "#wardriving" --format irc
```

```
[2026-04-12 17:19:09] <XMD Tag 1> @[MapperBot] 37.76985, -122.40525 [0.3w]
[2026-04-12 17:20:25] <XMD Tag 1> @[MapperBot] 37.78075, -122.39774 [0.3w]
[2026-04-12 17:25:30] <mk 🤠> @[MapperBot] 35.32444, -120.62077
```

```bash
# Find all messages from a specific sender
corescope-decrypt --channel "#wardriving" --format irc | grep "KE6QR"
```

## How channel encryption works

MeshCore hashtag channels derive their encryption key from the channel name:

1. **Key derivation**: `AES-128 key = SHA-256("#channelname")[:16]` (first 16 bytes)
2. **Channel hash**: `SHA-256(key)[0]` — 1-byte identifier in the packet header, used for fast filtering
3. **Encryption**: AES-128-ECB
4. **MAC**: HMAC-SHA256 with a 32-byte secret (key + 16 zero bytes), truncated to 2 bytes
5. **Plaintext format**: `timestamp(4 LE) + flags(1) + "sender: message\0"`

See the firmware source at `firmware/src/helpers/BaseChatMesh.cpp` for the canonical implementation.

## Testing

The tests start a throwaway Postgres container, so Docker must be running:

```bash
cd cmd/decrypt
go test ./...
```

The export test encrypts real `GRP_TXT` frames, stores them through the database layer, and checks that the tool recovers them with the right sender, path and observers.

The shared crypto library also has independent tests:

```bash
cd internal/channel
go test -v ./...
```

## Limitations

- **Hashtag channels only.** Only channels where the key is derived from `SHA-256("#name")` are supported. Custom PSK channels require the raw key (not implemented).
- **No DM decryption.** Direct messages (`TXT_MSG`) use per-peer asymmetric encryption and cannot be decrypted by this tool.
- **Retention.** Retroactive decryption works only while the encrypted payload is still stored. Once a message has aged out of the retention window and its payload has been scrubbed, it cannot be recovered. Database backups made earlier still contain it.
- **Read-only.** The tool opens a read-only connection and never modifies the database.
- **Timestamps are UTC.** The sender's embedded timestamp is used when available, displayed in UTC.
