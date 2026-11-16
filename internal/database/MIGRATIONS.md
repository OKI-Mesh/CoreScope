# Database Migrations

CoreScope's Postgres schema is managed by [goose](https://github.com/pressly/goose). The migrations are `.sql` files in `internal/database/postgres/migrations/`, embedded into every binary, and the code that runs and checks them lives in `internal/database/migrations.go`.

## Architecture

- **The ingestor is the only process that migrates.** At startup it calls `database.Migrate`, which applies every pending migration (`provider.Up`), and only then connects to MQTT.
- **`corescope-migrate` does the same thing by hand.** `cmd/migrate` runs `Migrate` against `-dsn` (or `$DATABASE_URL`), so a deploy step can migrate before any service restarts. `corescope-migrate -status` prints the current and latest version and exits non-zero when they differ, and it changes nothing.
- **The server never migrates and cannot write.** It opens its pool with `database.OpenReadOnly`, which sets `default_transaction_read_only = on`, so Postgres rejects a write even if application code tries one. At startup the server calls `AssertReady` and refuses to start while any migration is pending.

```
database.Migrate(ctx, dsn)                 // ingestor and cmd/migrate: apply pending migrations
database.OpenReadWrite(ctx, dsn)           // ingestor: pool, no schema assertions
database.OpenReadOnly(ctx, dsn)            // server: pool with default_transaction_read_only = on
database.AssertReady(ctx, sqlDB)           // server: refuses if any migration is pending
database.CurrentSchemaVersion / LatestSchemaVersion   // used by corescope-migrate -status
```

`OpenReadWrite` and `OpenReadOnly` deliberately do not assert anything about schema state. Each caller composes the checks it needs, so it is visible at the call site which checks are happening.

## Adding a new migration

1. Add `NNNNN_short_description.sql` to `internal/database/postgres/migrations/`, numbered after the newest file. Start it with `-- +goose Up`, and add a `-- +goose Down` section when the change can be reversed.
2. Read [MIGRATIONS.md](../../MIGRATIONS.md) at the repo root for what is safe on a large table. The ingestor runs migrations before it starts ingesting, so a slow one delays ingest.
3. If queries change, edit the files in `internal/database/postgres/queries/` and run `sqlc generate` (see `sqlc.yaml`). The generated code is in `internal/database/pgdb/`; do not edit it by hand.
4. Run `go test ./internal/database/... -count=1`. The tests build a template database from the embedded migrations, so a broken migration fails every database test.

## Testing

- `internal/database/pgtest` starts a throwaway Postgres (or uses `TEST_PG_DSN`), migrates a template database once per test process, and hands each test its own copy. A test package's `TestMain` must call `os.Exit(pgtest.Run(m))`.
- `internal/database/dbtest` holds the shared helpers for seeding and querying (`CountRows`, `QueryRow`, `Exec`, `ExplainPlan`, and others). Seed rows by calling the real database methods with explicit values, so the CHECK and foreign-key constraints fail the test when a fixture is wrong.

## Troubleshooting

**"database has pending migrations; restart the ingestor to apply them before starting the server"**
The server's `AssertReady` found migrations that are embedded in the binary but not applied. Start (or restart) the ingestor, or run `corescope-migrate -dsn "$DATABASE_URL"`, then start the server.

**"checking migration status (possible schema gap — restart the ingestor to apply missing schema changes)"**
`HasPending` returned an error instead of a clean answer. It usually means goose could not read its version table, which is most often a connectivity or permissions problem. Check that the server's role can read `goose_db_version`.

**"cannot reach database"**
`Migrate` could not ping Postgres. Check the connection string and that the PostGIS-capable Postgres 18 server is up.
