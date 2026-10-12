#!/usr/bin/env bash
# usage: bash scripts/e2e-local.sh tests/e2e/<test>.js [more tests...]
set -euo pipefail
cd "$(dirname "$0")/.."

PORT="${E2E_PORT:-13581}"
WORK="$(mktemp -d)"
DB="$WORK/e2e-fixture.db"
SERVER_PID=""
cleanup() { [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null || true; rm -rf "$WORK"; }
trap cleanup EXIT

[ "$#" -ge 1 ] || { echo "usage: $0 tests/e2e/<test>.js [more tests...]" >&2; exit 2; }
for t in "$@"; do [ -f "$t" ] || { echo "no such test: $t" >&2; exit 2; }; done
command -v sqlite3 >/dev/null || { echo "sqlite3 CLI is required" >&2; exit 2; }

go build -o "$WORK/corescope-server" ./cmd/server
go build -o "$WORK/corescope-migrate" ./cmd/migrate

cp test-fixtures/e2e-fixture.db "$DB"
bash tools/freshen-fixture.sh "$DB"
"$WORK/corescope-migrate" -db "$DB" -baseline-stamp-migrate

WF=.github/workflows/deploy.yml
grep -E "sqlite3 test-fixtures/e2e-fixture.db \"UPDATE observers" "$WF" \
  | sed -E 's/^ *run: *//; s#test-fixtures/e2e-fixture.db#'"$DB"'#' | bash
awk '/sqlite3 test-fixtures\/e2e-fixture.db <<.SQL.$/{f=1;next} /^ *SQL$/{f=0} f' "$WF" | sqlite3 "$DB"

fuser -k "$PORT"/tcp 2>/dev/null || true
"$WORK/corescope-server" -port "$PORT" -db "$DB" -public public >"$WORK/server.log" 2>&1 &
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