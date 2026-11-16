#!/usr/bin/env bash
# blacklist-test.sh — verify nodeBlacklist hides a pubkey from API surface
# while retaining its packets in the DB. Implements QA plan §10.1 + §10.2.
#
# Usage:
#   blacklist-test.sh BASELINE_URL TARGET_URL
#
# BASELINE_URL is currently unused for assertions but kept as a positional
# arg for parity with other qa-suite scripts (always called with two URLs).
#
# Required env (target host control + test data):
#   TEST_NODE_PUBKEY      — hex pubkey of a real, currently-visible node on TARGET_URL
#   TARGET_SSH_HOST       — e.g. runner@example
#   TARGET_SSH_KEY        — path to ssh private key (default: /root/.ssh/id_ed25519)
#   TARGET_CONFIG_PATH    — absolute path to config.json on the target
#   TARGET_CONTAINER      — docker container name on the target
# Optional env:
#   TARGET_DB_CONTAINER   — docker container running Postgres on the target
#                            (default: corescope-postgres; used by the §10.2 probe)
#   TARGET_DB_USER        — Postgres role (default: corescope)
#   TARGET_DB_NAME        — Postgres database (default: corescope)
#   CURL_TIMEOUT          — per-request curl timeout, seconds (default 60)
#   RESTART_WAIT_S        — max wait for /api/stats after restart (default 120)
#
# Distinguishes:
#   ssh-failed     → cannot reach/control target
#   restart-stuck  → /api/stats not 200 within RESTART_WAIT_S
#   hide-failed    → blacklisted pubkey still surfaced via API (§10.1 fail)
#   retain-failed  → no transmissions.from_pubkey rows (ADVERTs) for the
#                    blacklisted pubkey in the DB (§10.2 fail), or the
#                    §10.2 probe could not run at all — psql in the Postgres
#                    container did not answer. The message names what is needed;
#                    there is no fallback to interpolated SQL.
#   teardown-failed→ post-test removal did not restore listing
#
# Exit code = number of failures (0 = pass).
# PUBLIC repo: zero PII — no real pubkeys, IPs, or hostnames as defaults.
#
# Structure: helpers live at top level and the imperative body lives in main(),
# so test-blacklist-sql.sh can source this file and exercise individual helpers
# without running the suite. Same idiom as scripts/staging/disk-monitor.sh.

set -uo pipefail

SSH_OPTS=()  # populated by main() from TARGET_SSH_KEY
ssh_t() { ssh "${SSH_OPTS[@]}" "$TARGET_SSH_HOST" "$@"; }

# -----------------------------------------------------------------------------
# Teardown — MANDATORY in all exit paths.
# -----------------------------------------------------------------------------
teardown() {
  local rc=$?
  if [[ "$TEARDOWN_DONE" == "1" ]]; then rm -rf "$TMP"; exit "$rc"; fi
  TEARDOWN_DONE=1
  echo "=== teardown: removing $TEST_PUBKEY from nodeBlacklist ==="
  if remove_from_blacklist && restart_target && wait_for_stats; then
    if node_visible; then
      echo "  ✅ teardown ok — node returned to listings"
    else
      echo "  ❌ teardown-failed: node still hidden after removal"
      rc=$((rc + 1))
    fi
  else
    echo "  ❌ teardown-failed: could not restore config / restart / stats"
    rc=$((rc + 1))
  fi
  rm -rf "$TMP"
  exit "$rc"
}

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
fetch_code() {
  local url="$1" out="$2"
  curl -s -m "$CURL_TIMEOUT" -o "$out" -w "%{http_code}" "$url" 2>/dev/null || echo "000"
}

wait_for_stats() {
  local deadline code
  echo "  waiting up to ${RESTART_WAIT_S}s for $TARGET_URL/api/stats ..."
  deadline=$(( $(date +%s) + RESTART_WAIT_S ))
  while (( $(date +%s) < deadline )); do
    code=$(fetch_code "$TARGET_URL/api/stats" "$TMP/stats.json")
    if [[ "$code" == "200" ]]; then echo "  stats OK"; return 0; fi
    sleep 3
  done
  echo "  ❌ restart-stuck: /api/stats never returned 200"
  return 1
}

restart_target() {
  echo "  restarting container $TARGET_CONTAINER ..."
  # TARGET_CONTAINER is validated above; still quote defensively.
  if ! ssh_t "docker restart $(printf %q "$TARGET_CONTAINER")" >/dev/null; then
    echo "  ❌ ssh-failed: docker restart failed"
    return 1
  fi
  return 0
}

# Mutate config.json on target. Values pass via env (printf %q + single-quoted
# heredoc) so $TEST_PUBKEY etc. never enter the remote shell as code.
set_blacklist_state() {
  local mode="$1"  # add | remove
  ssh_t "CFG=$(printf %q "$TARGET_CONFIG_PATH") PK=$(printf %q "$TEST_PUBKEY") MODE=$(printf %q "$mode") bash -s" <<'REMOTE'
set -euo pipefail
TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT
if command -v jq >/dev/null; then
  if [ "$MODE" = "add" ]; then
    jq --arg pk "$PK" '.nodeBlacklist = ((.nodeBlacklist // []) + [$pk] | unique)' "$CFG" > "$TMP"
  else
    jq --arg pk "$PK" '.nodeBlacklist = ((.nodeBlacklist // []) - [$pk])' "$CFG" > "$TMP"
  fi
else
  python3 - "$CFG" "$PK" "$MODE" "$TMP" <<'PY'
import json, sys
cfg, pk, mode, out = sys.argv[1:]
with open(cfg) as f: d = json.load(f)
bl = list(dict.fromkeys(d.get("nodeBlacklist") or []))
if mode == "add":
    if pk not in bl: bl.append(pk)
else:
    bl = [x for x in bl if x != pk]
d["nodeBlacklist"] = bl
with open(out, "w") as f: json.dump(d, f, indent=2)
PY
fi
# Preserve mode and ownership; mv across same FS is atomic.
chmod --reference="$CFG" "$TMP" 2>/dev/null || true
chown --reference="$CFG" "$TMP" 2>/dev/null || true
mv "$TMP" "$CFG"
trap - EXIT
REMOTE
  local rc=$?
  if (( rc != 0 )); then
    echo "  ❌ ssh-failed: could not edit $TARGET_CONFIG_PATH ($mode)"
    return 1
  fi
  return 0
}

add_to_blacklist()      { set_blacklist_state add; }
remove_from_blacklist() { set_blacklist_state remove; }

node_visible() {
  # Returns 0 if the pubkey is currently visible via API.
  local code
  code=$(fetch_code "$TARGET_URL/api/nodes/$TEST_PUBKEY" "$TMP/node.json")
  if [[ "$code" == "200" ]]; then return 0; fi
  fetch_code "$TARGET_URL/api/nodes?limit=10000" "$TMP/nodes.json" >/dev/null
  if grep -qF -- "\"$TEST_PUBKEY\"" "$TMP/nodes.json" 2>/dev/null; then
    return 0
  fi
  return 1
}

# -----------------------------------------------------------------------------
# §10.2 DB probe — bind the pubkey, do not interpolate it (issue #1977)
# -----------------------------------------------------------------------------
# psql batch flags, all in service of "the count is parseable and errors are
# visible":
#   -X                      ignore the operator's ~/.psqlrc
#   -q -A -t                quiet, unaligned, tuples only: stdout is exactly the number
#   -v ON_ERROR_STOP=1      a SQL error ends the run with a non-zero exit
PSQL_ARGS=(-X -q -A -t -v ON_ERROR_STOP=1)
# Round-trip probe token. The value is arbitrary; it only has to come back intact.
PSQL_PROBE_TOKEN="corescope-probe-ok"
RETAIN_COUNT=""    # set by read_retain_count

# Hex-encode a value as a single-quoted SQL string of hex digits.
#
# Why hex rather than quoting: the output alphabet is [0-9a-f], so no byte the
# caller passes can terminate a string literal or change the statement. That
# holds for arbitrary input, which is the point — the SQL layer stops depending
# on main()'s hex gate in order to be safe.
#
# `od -v` is load-bearing: without it od collapses runs of identical lines to
# '*' and long repetitive values encode wrongly.
pg_hex_literal() {
  printf "'%s'" "$(printf '%s' "$1" | od -An -v -tx1 | tr -d ' \n')"
}

# The original value as a text expression, decoded server-side from its hex.
pg_text_from_hex() {
  printf "convert_from(decode(%s, 'hex'), 'UTF8')" "$(pg_hex_literal "$1")"
}

# SQL for the §10.2 count, fed to psql on stdin. The SELECT text is a constant;
# the pubkey arrives only as hex digits that Postgres decodes.
#
# transmissions.from_pubkey is filled only for ADVERTs, with lowercase hex. The
# script's hex gate and the server's nodeBlacklist both accept any case, so the
# value is lowercased inside SQL. There is no from_node column: querying it
# errors on every real database.
transmission_count_sql() {
  printf 'SELECT COUNT(*) FROM transmissions WHERE from_pubkey = lower(%s);\n' "$(pg_text_from_hex "$1")"
}

# Capability probe: send a known value through the same decode path and read it
# back. It proves the container, role and database are reachable and that the
# statement round-trips, before the count is trusted.
pg_probe_sql() {
  printf 'SELECT %s;\n' "$(pg_text_from_hex "$PSQL_PROBE_TOKEN")"
}

# Run SQL from stdin through psql inside the Postgres container. Stderr is left
# alone so the caller can capture it, and the exit status is psql's. The SQL
# crosses on stdin, so only the container, role and database names are on the
# remote command line, and those are validated and printf %q-quoted. docker exec
# needs -i to attach stdin.
run_psql() {
  ssh_t "docker exec -i $(printf %q "$TARGET_DB_CONTAINER") psql ${PSQL_ARGS[*]} -U $(printf %q "$TARGET_DB_USER") -d $(printf %q "$TARGET_DB_NAME")"
}

# Read the retained-transmission count into RETAIN_COUNT. Prints a classified
# "retain-failed" line and returns 1 on failure, so §10.2 has exactly one place
# that increments $fails.
read_retain_count() {
  RETAIN_COUNT=""
  if [[ -z "$TARGET_DB_CONTAINER" ]]; then
    echo "  ❌ retain-failed: TARGET_DB_CONTAINER unset — cannot probe"
    return 1
  fi
  local out
  if ! out=$(run_psql <<<"$(pg_probe_sql)" 2>"$TMP/psql-probe.err") || [[ "$out" != "$PSQL_PROBE_TOKEN" ]]; then
    echo "  ❌ retain-failed: psql in container $TARGET_DB_CONTAINER did not answer the probe"
    echo "     tried: docker exec -i $TARGET_DB_CONTAINER psql -U $TARGET_DB_USER -d $TARGET_DB_NAME over ssh to $TARGET_SSH_HOST"
    cat "$TMP/psql-probe.err" >&2
    return 1
  fi
  if ! RETAIN_COUNT=$(run_psql <<<"$(transmission_count_sql "$TEST_PUBKEY")" 2>"$TMP/psql.err"); then
    echo "  ❌ retain-failed: psql query failed in container $TARGET_DB_CONTAINER"
    cat "$TMP/psql.err" >&2
    RETAIN_COUNT=""
    return 1
  fi
  return 0
}

# -----------------------------------------------------------------------------
# main
# -----------------------------------------------------------------------------
main() {
  BASELINE_URL="${1:-}"
  TARGET_URL="${2:-}"
  if [[ -z "$BASELINE_URL" || -z "$TARGET_URL" ]]; then
    echo "usage: $0 BASELINE_URL TARGET_URL  (TEST_NODE_PUBKEY+TARGET_* via env)" >&2
    exit 2
  fi

  TEST_PUBKEY="${TEST_NODE_PUBKEY:-}"
  TARGET_SSH_HOST="${TARGET_SSH_HOST:-}"
  TARGET_SSH_KEY="${TARGET_SSH_KEY:-/root/.ssh/id_ed25519}"
  TARGET_CONFIG_PATH="${TARGET_CONFIG_PATH:-}"
  TARGET_CONTAINER="${TARGET_CONTAINER:-}"
  TARGET_DB_CONTAINER="${TARGET_DB_CONTAINER:-corescope-postgres}"
  TARGET_DB_USER="${TARGET_DB_USER:-corescope}"
  TARGET_DB_NAME="${TARGET_DB_NAME:-corescope}"

  if [[ -z "$TEST_PUBKEY" || -z "$TARGET_SSH_HOST" || -z "$TARGET_CONFIG_PATH" || -z "$TARGET_CONTAINER" ]]; then
    echo "error: TEST_NODE_PUBKEY, TARGET_SSH_HOST, TARGET_CONFIG_PATH, TARGET_CONTAINER are required" >&2
    exit 2
  fi

  # Hard input validation — these strings are interpolated into the remote shell.
  # §10.2's SQL binds TEST_PUBKEY as a parameter rather than interpolating it, so
  # for the SQL layer this gate is defence in depth rather than the only guard
  # (issue #1977). Keep it: redundant is not the same as wrong.
  # Pubkey must be hex (MeshCore pubkeys are hex-encoded ed25519 prefixes).
  if ! [[ "$TEST_PUBKEY" =~ ^[0-9a-fA-F]+$ ]]; then
    echo "error: TEST_NODE_PUBKEY must be hex (got: redacted)" >&2
    exit 2
  fi
  # Container name must match docker's allowed chars: [a-zA-Z0-9][a-zA-Z0-9_.-]*
  if ! [[ "$TARGET_CONTAINER" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]*$ ]]; then
    echo "error: TARGET_CONTAINER has illegal chars" >&2
    exit 2
  fi
  # Config path must be an absolute, sane path (no spaces, quotes, $, ;, etc.).
  if ! [[ "$TARGET_CONFIG_PATH" =~ ^/[A-Za-z0-9_./-]+$ ]]; then
    echo "error: TARGET_CONFIG_PATH must be a sane absolute path" >&2
    exit 2
  fi
  if ! [[ "$TARGET_DB_CONTAINER" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]*$ ]]; then
    echo "error: TARGET_DB_CONTAINER has illegal chars" >&2
    exit 2
  fi
  if ! [[ "$TARGET_DB_USER" =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ && "$TARGET_DB_NAME" =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ ]]; then
    echo "error: TARGET_DB_USER and TARGET_DB_NAME must be plain identifiers" >&2
    exit 2
  fi

  CURL_TIMEOUT="${CURL_TIMEOUT:-60}"
  RESTART_WAIT_S="${RESTART_WAIT_S:-120}"

  SSH_OPTS=(-i "$TARGET_SSH_KEY" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 -o BatchMode=yes)

  TMP=$(mktemp -d)
  fails=0
  TEARDOWN_DONE=0
  trap teardown EXIT INT TERM

  # ---------------------------------------------------------------------------
  # §10.1 — hide
  # ---------------------------------------------------------------------------
  echo "=== §10.1 add $TEST_PUBKEY to nodeBlacklist ==="
  if ! add_to_blacklist; then fails=$((fails+1)); exit "$fails"; fi
  if ! restart_target;    then fails=$((fails+1)); exit "$fails"; fi
  if ! wait_for_stats;    then fails=$((fails+1)); exit "$fails"; fi

  detail_code=$(fetch_code "$TARGET_URL/api/nodes/$TEST_PUBKEY" "$TMP/detail.json")
  list_code=$(fetch_code "$TARGET_URL/api/nodes?limit=10000" "$TMP/list.json")
  in_list=0
  if [[ "$list_code" == "200" ]] && grep -qF -- "\"$TEST_PUBKEY\"" "$TMP/list.json"; then
    in_list=1
  fi
  if [[ "$detail_code" == "404" || "$in_list" == "0" ]]; then
    echo "  ✅ hide ok: detail=$detail_code in_list=$in_list"
  else
    echo "  ❌ hide-failed: detail=$detail_code in_list=$in_list — pubkey still surfaced"
    fails=$((fails+1))
  fi

  topo_code=$(fetch_code "$TARGET_URL/api/topology" "$TMP/topo.json")
  if [[ "$topo_code" != "200" ]]; then
    echo "  ⚠️  /api/topology HTTP $topo_code — skipping topology assertion"
  elif grep -qF -- "$TEST_PUBKEY" "$TMP/topo.json"; then
    echo "  ❌ hide-failed: /api/topology references blacklisted pubkey"
    fails=$((fails+1))
  else
    echo "  ✅ topology clean"
  fi

  # ---------------------------------------------------------------------------
  # §10.2 — DB retain
  # ---------------------------------------------------------------------------
  echo "=== §10.2 verify packets retained in DB ==="
  if ! read_retain_count; then
    # read_retain_count already printed the classified reason. Counting here and
    # nowhere else, so one failure is never counted twice.
    fails=$((fails+1))
  elif [[ "$RETAIN_COUNT" =~ ^[0-9]+$ ]] && (( RETAIN_COUNT > 0 )); then
    echo "  ✅ DB retains $RETAIN_COUNT packets from $TEST_PUBKEY"
  else
    echo "  ❌ retain-failed: count=$RETAIN_COUNT (expected > 0)"
    fails=$((fails+1))
  fi

  echo "=== summary: $fails failure(s) before teardown ==="
  # trap handles teardown + exit
  exit "$fails"
}

# Only run main when executed directly (not when sourced by tests).
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
  main "$@"
fi
