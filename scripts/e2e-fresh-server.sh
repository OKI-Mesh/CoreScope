#!/usr/bin/env bash
# Give the E2E suite a pristine database and (re)start the server on it. The
# Packets page defaults to a 15-minute window, so fixture data goes stale ~15 min
# after freshen; every suite attempt needs its own fresh start.
# Needs a Postgres reachable as described in scripts/e2e-db.sh, and the built
# ./corescope-server and ./corescope-migrate. Run from the repo root.
set -euo pipefail

PORT="${E2E_PORT:-13581}"
PUBLIC_DIR="${E2E_PUBLIC:-public-instrumented}"
LOG="${E2E_SERVER_LOG:-server.log}"
MIGRATE="${E2E_MIGRATE:-./corescope-migrate}"

# shellcheck source=scripts/e2e-db.sh
source "$(dirname "$0")/e2e-db.sh"

if ! e2e_template_exists; then
  e2e_build_template "$MIGRATE"
fi

if [ -f .server.pid ]; then kill "$(cat .server.pid)" 2>/dev/null || true; fi
fuser -k "$PORT"/tcp 2>/dev/null || true
sleep 1

e2e_new_db

./corescope-server -port "$PORT" -dsn "$(e2e_dsn "$E2E_DB_NAME")" -public "$PUBLIC_DIR" >"$LOG" 2>&1 &
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
