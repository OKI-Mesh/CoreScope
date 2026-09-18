# Database Migrations

CoreScope's SQLite schema is managed by [goose](https://github.com/pressly/goose), version-controlled starting at migration 1. All migration logic — running migrations, detecting/baselining pre-goose databases, and asserting a database is ready — lives in `internal/database`.

## Architecture

- **The ingestor is the only writer.** It's the only process that ever calls `Up` against a database. At startup it asserts the database is baselined (see below), then applies any pending migrations, using `--allow-missing` so it can fill gaps left by a partial baseline.
- **The server is read-only and never migrates.** It opens the database in `mode=ro`, and additionally enforces `PRAGMA query_only = ON` so a write is rejected at the SQLite level even if application code tries one. At startup it asserts the database is fully migrated (`AssertReady`) and refuses to start otherwise.
- **Neither binary silently fixes a bad database.** If a database predates goose, or is only partially migrated, both refuse to start with an error telling the operator to run `cmd/migrate` first. This is deliberate: a database in a mixed/unknown state should never be guessed at by a running service.

```
database.OpenReadWrite(path)   // ingestor: raw connection, no schema assertions
database.OpenReadOnly(path)    // server: raw connection, no schema assertions

database.AssertBaselined(db)   // refuses if pre-existing schema isn't fully stamped through the adoption version
database.AssertReady(db)       // refuses if any migration is still pending
```

`OpenReadWrite`/`OpenReadOnly` deliberately do **not** assert anything about schema state — that's composed explicitly by each caller, so it's always visible at the call site which checks are actually happening.

## The adoption boundary

`gooseAdoptionVersion` (in `internal/database/baseline.go`) marks the first goose migration written after CoreScope switched from raw SQL / a legacy migration system to goose. A database is considered **baselined** once every version from 1 through this boundary is applied — not just the highest version, since a database can report a high max version while still having a gap somewhere below the boundary (e.g. a partial or failed baseline run).

`AssertBaselined` checks this explicitly, version by version, via goose's own `Status()` — not by trusting `CurrentSchemaVersion`'s max-version number alone.

A genuinely fresh database (no pre-existing `nodes`/`transmissions` tables at all) is not required to be pre-baselined — `Up` applies the full chain from scratch in that case.

## Bringing a pre-existing database under goose's control

Pre-goose databases — raw SQL, or tracked by the old system's `_migrations` table — are brought under goose's control by `cmd/migrate`, run once, manually, before the ingestor is ever pointed at that database.

```
migrate -db path/to/file.db -baseline
migrate -db path/to/file.db -baseline-and-stamp
migrate -db path/to/file.db -baseline-stamp-migrate
```

| Flag | Does | Use when |
|---|---|---|
| `-baseline` | Reports which migrations can be proven already applied. Makes no changes. | Inspecting a database before committing to anything — dry run. |
| `-baseline-and-stamp` | Detects and stamps whichever individual migrations can be proven already applied, via schema shape or the legacy `_migrations` table. Does not run any migration SQL. | You want the database stamped but will let the ingestor run the remaining migrations itself at its own startup (`--allow-missing`). |
| `-baseline-stamp-migrate` | Stamps whatever's detectable, then runs all remaining migrations for real. On return, the database is fully at head. | The standard path: run this once, standalone, before the ingestor ever starts against this database. |

Detection works per-version, independently — not as a single "walk until the first failure" pass. Each pending version is checked against `schemaChecks[version]`, which is either:

- **`Check`** — a schema-shape probe (table/column/index/view existence) for migrations that alter structure.
- **`LegacyName`** — a lookup against the old system's `_migrations` table, for migrations that only transform data and leave no schema signature (e.g. `NormalizePublicKeyCasing`).

A version with neither confirmable is left pending — `-baseline-stamp-migrate`'s subsequent `Up` (with out-of-order allowed) applies it for real. Nothing is guessed; an unconfirmed version is simply migrated for real rather than assumed.

**Every migration from 1 through `gooseAdoptionVersion` must have a `schemaChecks` entry.** A version missing from this map is treated as "cannot confirm," not "assume satisfied" — so a gap in `schemaChecks` degrades to always running that migration for real (safe), never to a false positive (unsafe). `TestSchemaChecksCoverAdoptionRange` in `baseline_test.go` guards against silently forgetting an entry.

## Adding a new migration

1. Add the `.sql` file under `internal/database/migrations/`, following goose's naming convention.
2. If the migration is expected to ever need baselining against a pre-existing database (i.e. it's at or below `gooseAdoptionVersion`, or you're extending that boundary), add a corresponding `schemaChecks` entry describing what it added.
3. Migrations above `gooseAdoptionVersion` don't need a `schemaChecks` entry — every database that could reach that version was already brought under goose's control by definition, so there's nothing to detect.

## The legacy `_migrations` table

Separate from goose entirely. Tracks long-running, one-shot **background jobs** (`BackfillPathJSONAsync`, `tx_last_seen_backfill_v1`, etc.) that goose has no concept of running. `EnsureLegacyMigrationsTable` creates it if missing; it's not being retired.

`_migrations` also served a second purpose during the goose transition: its row names are what `LegacyName`-based `schemaChecks` entries look up, for detecting old data-only migrations during baselining.

## Testing

- `internal/database`'s own test files (`*_test.go`, `package database`) use small local unexported helpers (`newMemoryDB`, `execRaw`, etc.) defined in `testhelpers_test.go`. They must never import `internal/testfixtures` — since `testfixtures` imports `database`, that would be a cycle.
- `internal/testfixtures` is for `cmd/ingestor` and `cmd/server` tests only. It provides `NewDB`, `Observer`, `Transmission`, `Observation`, `InsertNode` — each takes a plain path string and opens its own short-lived connection, so callers don't need to juggle a shared `*sql.DB`.
- `database.SeedUnstampedSchema(db, version)` applies real migration SQL up to and including `version`, without touching `goose_db_version` — simulating a pre-existing raw-SQL database for baseline-detection tests. It filters the embedded migrations filesystem to only files at or below `version` before running, since goose's `Up`/`UpTo` do not respect a version ceiling when `WithDisableVersioning` is set (with no tracking table to consult, it has no way to know what's "already applied," so it runs every file it finds unless the filesystem itself is filtered).

## Troubleshooting

**"database has pre-existing schema but is missing goose migrations [...]; run `migrate -baseline-stamp-migrate` against it first"**
The database has tables but wasn't (fully) brought under goose's control. Run `cmd/migrate` against it directly.

**"database has pending migrations; restart the ingestor to apply them before starting the server"**
The database is baselined but not fully migrated to head. Start (or restart) the ingestor — it owns running `Up`.

**"checking migration status (possible schema gap — restart the ingestor to apply missing schema changes)"**
`HasPending` returned an error rather than a clean pending/not-pending answer. Usually means an out-of-order gap above the adoption boundary (normal operational gap, not a baselining problem) — restarting the ingestor with its `--allow-missing` behavior resolves it. Could also indicate a genuine DB connectivity problem; goose doesn't distinguish the two with a typed error.