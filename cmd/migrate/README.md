# migrate

One-shot CLI for baselining and migrating a CoreScope SQLite database.
See `internal/database/MIGRATIONS.md` for the full architecture.

## Usage

    migrate -db path/to/file.db -baseline               # dry run — report only
    migrate -db path/to/file.db -baseline-and-stamp      # stamp what's detectable, run nothing
    migrate -db path/to/file.db -baseline-stamp-migrate  # bring fully to head (standard path)