# MIGRATIONS — keeping schema changes cheap on a large database

CoreScope's schema is a chain of goose migrations in
`internal/database/postgres/migrations/`, described in
[internal/database/MIGRATIONS.md](internal/database/MIGRATIONS.md). This page is
the policy for writing them.

## Why scale matters

The ingestor applies pending migrations when it starts, before it connects to
MQTT (`database.Migrate` in `cmd/ingestor/main.go`). A migration that takes
minutes keeps the ingestor from ingesting for minutes, and the MQTT messages
published in that window are lost because they are QoS 0 and nothing is
subscribed yet. The ingest buffer does not help: it only covers startup
maintenance that runs after the subscription is up. The server also refuses to
start until the migration has finished.

On a developer database with a few hundred rows every migration takes
milliseconds, so this failure only shows up in production (millions of
observations). The rules below are about making a migration safe at that size.

## The rules

**Adding a column.** `ALTER TABLE ... ADD COLUMN` with no default, or with a
constant default, only changes the catalog and is instant. A `NOT NULL` column
with a volatile default, or a type change, rewrites the whole table. Add the
column nullable, fill it in batches (see below), then add the constraint.

**Adding a constraint.** `ADD CONSTRAINT ... CHECK/FOREIGN KEY` scans the table
under a lock. Add it `NOT VALID`, then run `VALIDATE CONSTRAINT` in a later
migration (the validation only takes a light lock).

**Adding an index.** A plain `CREATE INDEX` blocks writes to the table for the
whole build. On `transmissions`, `observations` or any other large table, put
`-- +goose NO TRANSACTION` as the first line of the migration and use
`CREATE INDEX CONCURRENTLY IF NOT EXISTS`. It cannot run inside a transaction,
which is why the migration must opt out of goose's transaction. If a concurrent
build fails it leaves an invalid index behind; drop it before retrying.

**Rewriting data.** Do not put a large `UPDATE` or `DELETE` in a migration. Add
the new column or table in the migration, and backfill from the ingestor in
bounded batches, the way the retention prunes work (`pruneBatchTransmissions`),
so one transaction never touches the whole table.

## The budget

A migration should finish in under 30 seconds at production scale. If it cannot,
say so in the PR with the measured time and row count, and run it as a deploy
step before the ingestor restarts:

```bash
corescope-migrate -dsn "$DATABASE_URL"
```

The ingestor then finds nothing pending and starts ingesting immediately.
`corescope-migrate -status` shows whether anything is pending.

## Review checklist

- Does it touch a table with more than about 100K rows? If so, which of the rules above applies?
- Is a plain `CREATE INDEX` or a table rewrite hiding in it?
- Is there a `-- +goose Down` for anything that can be reversed?
- Do the database tests pass? They build a database from the embedded migrations, so a broken migration fails all of them.
