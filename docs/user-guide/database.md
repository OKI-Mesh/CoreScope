# Database

CoreScope stores everything in PostgreSQL 18 with the PostGIS extension available. The ingestor writes; the server reads.

## Connection

Both processes read the same setting. In `config.json`:

```json
{
  "db": {
    "connectionString": "postgres://corescope:change-me@localhost:5432/corescope"
  }
}
```

If `db.connectionString` is empty, the `DATABASE_URL` environment variable is used. The server also accepts a `-dsn` flag, which overrides both. The connection string carries the password, so CoreScope never logs it.

## Migrations

The ingestor applies pending schema migrations when it starts. The server does not migrate: it checks that the database is up to date and refuses to start otherwise, so start the ingestor first after an upgrade.

You can also run migrations by hand, for example from a deploy step:

```bash
corescope-migrate -dsn "$DATABASE_URL"            # apply pending migrations
corescope-migrate -dsn "$DATABASE_URL" -status    # show the applied and pending versions
```

The PostGIS extension must be available on the Postgres server.

## Maintenance

Postgres reclaims space with autovacuum, so there are no vacuum settings in CoreScope and no one-time maintenance step. Retention (`retention.*` in `config.json`) deletes old rows from the ingestor; autovacuum then makes that space reusable.

## Backups

Use Postgres tooling:

```bash
pg_dump --format=custom --file=corescope.dump "$DATABASE_URL"
pg_restore --dbname="$TARGET_DATABASE_URL" corescope.dump
```

## Moving from SQLite

An existing SQLite database is converted with `cmd/etl`. It loads into an empty, migrated Postgres database in a single transaction and rolls back by default (a dry run); pass `-commit` to keep the result. Run `go run ./cmd/etl -h` for the flags.
