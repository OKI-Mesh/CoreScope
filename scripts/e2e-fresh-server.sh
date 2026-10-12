#!/usr/bin/env bash
# Restore the pristine E2E fixture, freshen its timestamps, and (re)start the
# server. The Packets page defaults to a 15-minute window, so fixture data goes
# stale ~15 min after freshen; every suite attempt needs its own fresh start.
# Run from the repo root.
set -euo pipefail

PORT="${E2E_PORT:-13581}"
DB="${E2E_DB:-test-fixtures/e2e-fixture.db}"
PRISTINE="${E2E_PRISTINE:-/tmp/e2e-fixture.pristine.db}"
PUBLIC_DIR="${E2E_PUBLIC:-public-instrumented}"
LOG="${E2E_SERVER_LOG:-server.log}"

# First call: snapshot the seeded fixture before the server ever opens it.
if [ ! -f "$PRISTINE" ]; then
  sqlite3 "$DB" "PRAGMA wal_checkpoint(TRUNCATE);" >/dev/null 2>&1 || true
  cp "$DB" "$PRISTINE"
fi

if [ -f .server.pid ]; then kill "$(cat .server.pid)" 2>/dev/null || true; fi
fuser -k "$PORT"/tcp 2>/dev/null || true
sleep 1

rm -f "$DB-wal" "$DB-shm"
cp "$PRISTINE" "$DB"
bash tools/freshen-fixture.sh "$DB"

./corescope-server -port "$PORT" -db "$DB" -public "$PUBLIC_DIR" >"$LOG" 2>&1 &
echo $! > .server.pid

for i in $(seq 1 180); do
  if curl -sf "http://localhost:$PORT/api/healthz" >/dev/null 2>&1; then
    echo "Server ready after ${i}s"
    exit 0
  fi
  sleep 1
done
echo "Server failed to start within 180s" >&2
tail -n 50 "$LOG" >&2 || true
exit 1