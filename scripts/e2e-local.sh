#!/usr/bin/env bash
# usage: bash scripts/e2e-local.sh tests/e2e/<test>.js [more tests...]
#
# Runs E2E tests against a throwaway database on a local Postgres with PostGIS.
# Point it at one with E2E_PG_URL (see scripts/e2e-db.sh), or, if Postgres runs in
# docker and this machine has no psql, with E2E_PG_CONTAINER:
#   E2E_PG_CONTAINER=corescope-etl-pg E2E_PG_URL=postgres://postgres:etl@localhost:5433 \
#     bash scripts/e2e-local.sh tests/e2e/test-issue-1486-collapse-reopens-detail-e2e.js
set -euo pipefail
cd "$(dirname "$0")/.."

PORT="${E2E_PORT:-13581}"
WORK="$(mktemp -d)"
SERVER_PID=""

# Names that cannot clash with the CI ones or another run's.
export E2E_TEMPLATE_DB="${E2E_TEMPLATE_DB:-corescope_e2e_local_template}"
export E2E_DB_NAME="${E2E_DB_NAME:-corescope_e2e_local}"
# shellcheck source=scripts/e2e-db.sh
source scripts/e2e-db.sh

cleanup() {
  [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null || true
  e2e_psql postgres -c "DROP DATABASE IF EXISTS $E2E_DB_NAME WITH (FORCE)" >/dev/null 2>&1 || true
  e2e_psql postgres -c "DROP DATABASE IF EXISTS $E2E_TEMPLATE_DB WITH (FORCE)" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

[ "$#" -ge 1 ] || { echo "usage: $0 tests/e2e/<test>.js [more tests...]" >&2; exit 2; }
for t in "$@"; do [ -f "$t" ] || { echo "no such test: $t" >&2; exit 2; }; done
if [ -z "${E2E_PG_CONTAINER:-}" ]; then
  command -v psql >/dev/null || { echo "psql is required (or set E2E_PG_CONTAINER)" >&2; exit 2; }
fi

go build -o "$WORK/corescope-server" ./cmd/server
go build -o "$WORK/corescope-migrate" ./cmd/migrate

e2e_build_template "$WORK/corescope-migrate"
e2e_new_db

fuser -k "$PORT"/tcp 2>/dev/null || true
"$WORK/corescope-server" -port "$PORT" -dsn "$(e2e_dsn "$E2E_DB_NAME")" -public public >"$WORK/server.log" 2>&1 &
SERVER_PID=$!
for i in $(seq 1 120); do
  if curl -sf "http://localhost:$PORT/api/healthz" 2>/dev/null | grep -q '"ready":true'; then echo "server ready after ${i}s"; break; fi
  kill -0 "$SERVER_PID" 2>/dev/null || { echo "server exited:"; tail -50 "$WORK/server.log"; exit 1; }
  [ "$i" -eq 120 ] && { echo "server not ready after 120s:"; tail -50 "$WORK/server.log"; exit 1; }
  sleep 1
done

rc=0
for t in "$@"; do
  echo "== $t"
  CHROMIUM_REQUIRE=1 BASE_URL="http://localhost:$PORT" node "$t" || rc=1
done
[ "$rc" -eq 0 ] || { echo "== server log tail"; tail -30 "$WORK/server.log"; }
exit "$rc"
