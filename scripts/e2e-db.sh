#!/usr/bin/env bash
# Shared helpers for the E2E scripts; source it, it runs nothing by itself.
#
# The E2E database is a copy of a template database that holds the migrated
# schema, the captured fixture and the seed rows. A copy takes well under a
# second, so every suite attempt gets a pristine database.
#
# Connection:
#   E2E_PG_URL        server URL without a database name or query string
#                     (default postgres://postgres:postgres@localhost:5432)
#   E2E_PG_CONTAINER  when set, psql runs inside that docker container instead of
#                     on this machine (for a laptop with no psql installed);
#                     E2E_PG_USER is its role (default postgres)
#   E2E_TEMPLATE_DB / E2E_DB_NAME   database names to use
# Run from the repo root.

E2E_PG_URL="${E2E_PG_URL:-postgres://postgres:postgres@localhost:5432}"
E2E_TEMPLATE_DB="${E2E_TEMPLATE_DB:-corescope_e2e_template}"
E2E_DB_NAME="${E2E_DB_NAME:-corescope_e2e}"

# e2e_psql DATABASE [psql args...]; SQL on stdin is read with -f -.
e2e_psql() {
  local db="$1"; shift
  if [ -n "${E2E_PG_CONTAINER:-}" ]; then
    docker exec -i "$E2E_PG_CONTAINER" psql -U "${E2E_PG_USER:-postgres}" -d "$db" -v ON_ERROR_STOP=1 -q -X "$@"
  else
    psql "$E2E_PG_URL/$db" -v ON_ERROR_STOP=1 -q -X "$@"
  fi
}

# e2e_dsn DATABASE prints the DSN the server and the migrate tool connect with.
e2e_dsn() { echo "$E2E_PG_URL/$1?sslmode=disable"; }

e2e_template_exists() {
  [ "$(e2e_psql postgres -tA -c "SELECT 1 FROM pg_database WHERE datname = '$E2E_TEMPLATE_DB'")" = "1" ]
}

# e2e_build_template MIGRATE_BINARY (re)creates the template: schema, fixture,
# freshened to now, then the seed rows (which are relative to the freshened data).
e2e_build_template() {
  local migrate="$1"
  e2e_psql postgres -c "DROP DATABASE IF EXISTS $E2E_TEMPLATE_DB WITH (FORCE)"
  e2e_psql postgres -c "CREATE DATABASE $E2E_TEMPLATE_DB"
  e2e_psql "$E2E_TEMPLATE_DB" -c 'CREATE EXTENSION IF NOT EXISTS postgis'
  "$migrate" -dsn "$(e2e_dsn "$E2E_TEMPLATE_DB")"
  e2e_psql "$E2E_TEMPLATE_DB" -f - < test-fixtures/e2e-fixture.sql
  e2e_psql "$E2E_TEMPLATE_DB" -f - < tools/freshen-fixture.sql
  e2e_psql "$E2E_TEMPLATE_DB" -f - < test-fixtures/e2e-seed.sql
}

# e2e_new_db replaces the E2E database with a fresh copy of the template and
# freshens its timestamps. Stop any server using it first.
e2e_new_db() {
  e2e_psql postgres -c "DROP DATABASE IF EXISTS $E2E_DB_NAME WITH (FORCE)"
  e2e_psql postgres -c "CREATE DATABASE $E2E_DB_NAME TEMPLATE $E2E_TEMPLATE_DB"
  e2e_psql "$E2E_DB_NAME" -f - < tools/freshen-fixture.sql
}
