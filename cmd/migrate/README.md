# migrate

One-shot CLI that applies the schema migrations to a CoreScope Postgres database.

The server and the ingestor do not migrate at startup, and both refuse to start
against a database that has not been migrated, so run this first: on a new
database, on deploy, and after pulling new migrations. See
`internal/database/MIGRATIONS.md` for the architecture.

## Usage

    migrate -dsn postgres://user:password@host:5432/corescope   # migrate to the latest version
    migrate -dsn postgres://user:password@host:5432/corescope -status   # print current and latest version, change nothing

`-dsn` defaults to `$DATABASE_URL`. The server must have the PostGIS extension available.
