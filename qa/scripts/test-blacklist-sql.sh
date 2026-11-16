#!/usr/bin/env bash
# test-blacklist-sql.sh — unit tests for the §10.2 SQL construction in
# qa/scripts/blacklist-test.sh (issue #1977). Sources the script and exercises
# its pure helpers, plus a real psql against throwaway Postgres databases.
#
# Run: bash qa/scripts/test-blacklist-sql.sh
# Exits non-zero if any case fails.
#
# The Postgres group runs only when psql is on PATH, TEST_PG_DSN names a server
# the test may create databases on (e.g. postgres://postgres:postgres@localhost:5432/postgres),
# and the migrate binary exists (go build -o bin/migrate ./cmd/migrate). Without
# them the alphabet and query-text assertions still run and the group is skipped.
#
# The point of the Postgres group is that BOTH directions are asserted. A test
# that only checks "the injection payload returns 0" passes just as happily when
# the query is silently broken and returns 0 for everything, so the legitimate
# pubkey must be shown to still return the row it should.
#
# The fixture schema is NOT hand-written: it is built by running the real goose
# migrations (bin/migrate) against a throwaway database, so it always matches
# whatever the migrations currently define. An earlier version invented a
# `from_node` column that no CoreScope database has, and so passed while the
# real probe could never succeed; this is the fix for that same class of bug.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
MIGRATE_BIN="${MIGRATE_BIN:-$REPO_ROOT/bin/migrate}"
# shellcheck source=blacklist-test.sh
. "$SCRIPT_DIR/blacklist-test.sh"

PASS=0
FAIL=0

assert_eq() {
    local label="$1" expected="$2" actual="$3"
    if [ "$expected" = "$actual" ]; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "FAIL: $label — expected '$expected' got '$actual'" >&2
    fi
}

assert_match() {
    local label="$1" pattern="$2" actual="$3"
    if [[ "$actual" =~ $pattern ]]; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "FAIL: $label — '$actual' does not match /$pattern/" >&2
    fi
}

assert_true() {
    local label="$1"; shift
    if "$@"; then PASS=$((PASS + 1)); else
        FAIL=$((FAIL + 1)); echo "FAIL: $label" >&2
    fi
}

contains() { [[ "$1" == *"$2"* ]]; }
lacks()    { [[ "$1" != *"$2"* ]]; }

# ----- pg_hex_literal ---------------------------------------------------------
# The security property: whatever goes in, the SQL text it produces is drawn
# from [0-9a-f] only. No caller-supplied byte can close a string literal or
# change the statement. Needs no Postgres, so this group always runs.
assert_eq "hex of deadbeef" "'6465616462656566'" "$(pg_hex_literal deadbeef)"
assert_eq "hex of empty"    "''"                  "$(pg_hex_literal "")"

HEX_ONLY="^'[0-9a-f]*'\$"
assert_match "alphabet: sql quote payload" "$HEX_ONLY" "$(pg_hex_literal "' OR 1=1 --")"
assert_match "alphabet: drop table"        "$HEX_ONLY" "$(pg_hex_literal '"; DROP TABLE transmissions; --')"
assert_match "alphabet: backslash"         "$HEX_ONLY" "$(pg_hex_literal 'a\b')"
assert_match "alphabet: dollar and backtick" "$HEX_ONLY" "$(pg_hex_literal '$(id) `id`')"
assert_match "alphabet: embedded newline"  "$HEX_ONLY" "$(pg_hex_literal "$(printf 'a\nb')")"
assert_match "alphabet: multibyte"         "$HEX_ONLY" "$(pg_hex_literal 'héllo')"

# `od` without -v collapses runs of identical lines to '*'. A long repetitive
# value is the case that catches losing the flag.
LONG=$(printf 'x%.0s' $(seq 1 4096))
LONG_HEX=$(pg_hex_literal "$LONG")
assert_match "alphabet: 4096 repeated bytes" "$HEX_ONLY" "$LONG_HEX"
# 4096 bytes → 8192 hex digits, plus the 2 quotes. A collapsed run would be far
# shorter and would also fail the alphabet check on '*'.
assert_eq "no od line-collapse in 4096-byte value" "8192" "$(( ${#LONG_HEX} - 2 ))"

# ----- query text ---------------------------------------------------------------
# Synthetic 32-byte pubkeys in the ingestor's form (hex.EncodeToString → lowercase).
PK_A="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
PK_B="fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
PK_ABSENT="00000000000000000000000000000000000000000000000000000000000000ff"
PK_A_UPPER=$(printf '%s' "$PK_A" | tr 'a-f' 'A-F')

COUNT_SQL=$(transmission_count_sql "$PK_A")
assert_true "query targets transmissions.from_pubkey" contains "$COUNT_SQL" "WHERE from_pubkey = lower(convert_from(decode("
assert_true "query does not reference from_node"     lacks "$COUNT_SQL" "from_node"
assert_true "query text does not carry the raw pubkey" lacks "$COUNT_SQL" "$PK_A"
assert_true "query carries the pubkey hex-encoded" contains "$COUNT_SQL" "$(pg_hex_literal "$PK_A")"
assert_eq "probe decodes its token" "SELECT convert_from(decode($(pg_hex_literal "$PSQL_PROBE_TOKEN"), 'hex'), 'UTF8');" "$(pg_probe_sql)"

# ----- against a real Postgres ----------------------------------------------------
if ! command -v psql >/dev/null 2>&1; then
    echo "SKIP: psql not on PATH — skipping the Postgres-backed query group" >&2
    echo "      (the alphabet and query-text assertions above still ran)" >&2
elif [ -z "${TEST_PG_DSN:-}" ]; then
    echo "SKIP: TEST_PG_DSN not set — skipping the Postgres-backed query group" >&2
    echo "      (the alphabet and query-text assertions above still ran)" >&2
elif [ ! -x "$MIGRATE_BIN" ]; then
    echo "SKIP: migrate binary not found/executable at $MIGRATE_BIN — skipping the Postgres-backed query group" >&2
    echo "      (build it first, e.g. go build -o bin/migrate ./cmd/migrate)" >&2
else
    FIXTURE_DIR=$(mktemp -d)
    SUFFIX="$$_$RANDOM"
    DB_REAL="blacklist_real_$SUFFIX"
    DB_LEGACY="blacklist_legacy_$SUFFIX"
    DB_EMPTY="blacklist_empty_$SUFFIX"
    cleanup() {
        for d in "$DB_REAL" "$DB_LEGACY" "$DB_EMPTY"; do
            psql "$TEST_PG_DSN" -X -q -c "DROP DATABASE IF EXISTS $d WITH (FORCE)" >/dev/null 2>&1
        done
        rm -rf "$FIXTURE_DIR"
    }
    trap cleanup EXIT

    # The DSN of a sibling database: same server and credentials, new database name.
    dsn_for() { printf '%s' "$TEST_PG_DSN" | sed -E "s#(://[^/]*)/[^?]*#\\1/$1#"; }
    run_local() { psql "$(dsn_for "$1")" "${PSQL_ARGS[@]}"; }
    count() { transmission_count_sql "$1" | run_local "$2"; }
    sql() { run_local "$1" <<<"$2"; }

    for d in "$DB_REAL" "$DB_LEGACY" "$DB_EMPTY"; do
        if ! psql "$TEST_PG_DSN" -X -q -v ON_ERROR_STOP=1 -c "CREATE DATABASE $d" >"$FIXTURE_DIR/create.log" 2>&1; then
            FAIL=$((FAIL + 1)); echo "FAIL: creating database $d" >&2; cat "$FIXTURE_DIR/create.log" >&2
        fi
    done

    # Schema-realistic fixture: built by running the real migrations. Three
    # ADVERTs from A, one from B, and two non-ADVERT rows whose from_pubkey is
    # NULL (the ingestor only attributes ADVERTs), so a structural injection has
    # 6 rows to leak.
    if ! DATABASE_URL="$(dsn_for "$DB_REAL")" "$MIGRATE_BIN" >"$FIXTURE_DIR/migrate.log" 2>&1; then
        FAIL=$((FAIL + 1))
        echo "FAIL: building fixture via $MIGRATE_BIN" >&2
        cat "$FIXTURE_DIR/migrate.log" >&2
    fi
    sql "$DB_REAL" "
INSERT INTO nodes(public_key, first_seen, last_seen) VALUES
  ('$PK_A', now(), now()), ('$PK_B', now(), now());
INSERT INTO transmissions(raw_hex, hash, first_seen, last_seen, payload_type, from_pubkey) VALUES
  ('\\x00', 'h1', now(), now(), 4, '$PK_A'),
  ('\\x00', 'h2', now(), now(), 4, '$PK_A'),
  ('\\x00', 'h3', now(), now(), 4, '$PK_A'),
  ('\\x00', 'h4', now(), now(), 4, '$PK_B'),
  ('\\x00', 'h5', now(), now(), 5, NULL),
  ('\\x00', 'h6', now(), now(), 2, NULL);" >/dev/null
    # The fixture the first version of this test used: an invented column.
    # Hand-rolled deliberately — it simulates a broken/wrong schema, not a real
    # one, so it stays independent of the migrate tool.
    sql "$DB_LEGACY" "CREATE TABLE transmissions(from_node TEXT); INSERT INTO transmissions VALUES('$PK_A');" >/dev/null
    sql "$DB_EMPTY" "CREATE TABLE unrelated(x int);" >/dev/null

    # The probe must round-trip on this server, or the assertions below would
    # be testing nothing.
    assert_eq "probe round-trips" "$PSQL_PROBE_TOKEN" "$(pg_probe_sql | run_local "$DB_REAL")"
    assert_eq "fixture really holds 6 rows" "6" "$(sql "$DB_REAL" 'SELECT COUNT(*) FROM transmissions;')"

    # POSITIVE CONTROL: a legitimate pubkey still returns its rows. Without this,
    # a silently broken query looks like a passing security fix.
    out=$(count "$PK_A" "$DB_REAL"); rc=$?
    assert_eq "legit pubkey → its 3 rows"  "3" "$out"
    assert_eq "legit pubkey → exit 0"      "0" "$rc"
    assert_eq "other legit pubkey → 1 row" "1" "$(count "$PK_B" "$DB_REAL")"
    assert_eq "upper-case pubkey matches (blacklist is case-insensitive)" "3" "$(count "$PK_A_UPPER" "$DB_REAL")"
    assert_eq "absent pubkey → 0"          "0" "$(count "$PK_ABSENT" "$DB_REAL")"
    assert_eq "prefix of a pubkey → 0 (exact match only)" "0" "$(count "${PK_A:0:16}" "$DB_REAL")"

    # NEGATIVE: payloads bind as literals that match nothing. A structural
    # injection would return 6 (or 4, the non-NULL rows), not 0.
    for payload in "' OR 1=1 --" "') OR 1=1 --" '" OR 1=1 --' \
                   "x' OR from_pubkey IS NOT NULL --" "$PK_A' OR '1'='1" \
                   '"); DROP TABLE transmissions; --' "$(printf "a\n\\\\! id\nSELECT 99;")"; do
        out=$(count "$payload" "$DB_REAL" 2>&1); rc=$?
        assert_eq "payload binds literally: $(printf %q "$payload")" "0" "$out"
        assert_eq "payload exit 0: $(printf %q "$payload")" "0" "$rc"
    done
    assert_eq "table survived the DROP payload" "6" "$(sql "$DB_REAL" 'SELECT COUNT(*) FROM transmissions;')"

    # Interpolating the same payload the old way returns the whole table. This is
    # the behaviour the change removes; asserting it keeps the test honest about
    # what "0" above is worth.
    legacy="SELECT COUNT(*) FROM transmissions WHERE from_pubkey = '' OR 1=1 --';"
    assert_eq "interpolated form leaks the table" "6" "$(sql "$DB_REAL" "$legacy")"

    # Multibyte, whitespace, empty and long values bind as themselves. The FK
    # needs a node for each stored value.
    LONG_PK=$(printf 'ab%.0s' $(seq 1 5000))
    sql "$DB_REAL" "
INSERT INTO nodes(public_key, first_seen, last_seen) VALUES
  ('héllo wörld', now(), now()), ('', now(), now()), ('  ', now(), now()), ('$LONG_PK', now(), now());
INSERT INTO transmissions(raw_hex, hash, first_seen, last_seen, from_pubkey) VALUES
  ('\\x00', 'm1', now(), now(), 'héllo wörld'), ('\\x00', 'm2', now(), now(), ''),
  ('\\x00', 'm3', now(), now(), '  '), ('\\x00', 'm4', now(), now(), '$LONG_PK');" >/dev/null
    assert_eq "multibyte value with a space binds" "1" "$(count 'héllo wörld' "$DB_REAL")"
    assert_eq "empty value binds as ''"            "1" "$(count '' "$DB_REAL")"
    assert_eq "whitespace value binds as itself"   "1" "$(count '  ' "$DB_REAL")"
    assert_eq "10000-byte value binds"             "1" "$(count "$LONG_PK" "$DB_REAL")"

    # ERROR SURFACING: a broken query must be distinguishable from an empty
    # result — non-zero exit and something on stderr, not a silent "" or "0".
    for bad in "$DB_LEGACY:column \"from_pubkey\" does not exist" "$DB_EMPTY:relation \"transmissions\" does not exist"; do
        bad_db=${bad%%:*}; want=${bad#*:}
        err_file="$FIXTURE_DIR/err"
        out=$(count "$PK_A" "$bad_db" 2>"$err_file"); rc=$?
        assert_true "$want → non-zero exit (got $rc)" test "$rc" -ne 0
        assert_true "$want → named on stderr" contains "$(cat "$err_file")" "$want"
        assert_eq   "$want → no count on stdout" "" "$out"
    done

    # ----- transport, with ssh stubbed -------------------------------------------
    # ssh_t is replaced by a stub that logs its argv, captures stdin, and runs the
    # remote command string through a real `bash -c`, so the script's own quoting
    # is what gets parsed. "docker exec -i NAME" is peeled off, and the role and
    # database flags are swapped for the local DSN of the database under test.
    STUB_ARGV="$FIXTURE_DIR/ssh.argv"; STUB_STDIN="$FIXTURE_DIR/ssh.stdin"
    STUB_DB="$DB_REAL"
    ssh_t() {
        local cmd="$1" in
        printf '%s\n' "$*" >>"$STUB_ARGV"
        in=$(cat); printf '%s\n' "$in" >>"$STUB_STDIN"
        case "$cmd" in
            "docker exec -i "*)
                cmd=${cmd#docker exec -i }; cmd=${cmd#* }
                cmd=${cmd%% -U *}
                cmd="$cmd \"\$STUB_DSN\"" ;;
        esac
        printf '%s\n' "$in" | STUB_DSN="$(dsn_for "$STUB_DB")" bash -c "$cmd"
    }
    reset_stub() { : >"$STUB_ARGV"; : >"$STUB_STDIN"; }
    TMP="$FIXTURE_DIR"; TARGET_DB_CONTAINER="corescope-pg-stub"
    TARGET_DB_USER="corescope"; TARGET_DB_NAME="corescope"
    TARGET_SSH_HOST="stub-host"; TEST_PUBKEY="$PK_A"

    reset_stub
    read_retain_count >/dev/null 2>&1; rc=$?
    assert_eq "stubbed transport: rc"    "0" "$rc"
    assert_eq "stubbed transport: count" "3" "$RETAIN_COUNT"
    argv=$(cat "$STUB_ARGV"); stdin=$(cat "$STUB_STDIN")
    assert_true "transport: container targeted" contains "$(head -1 "$STUB_ARGV")" "docker exec -i corescope-pg-stub psql"
    assert_true "transport: role and database named" contains "$(head -1 "$STUB_ARGV")" "-U corescope -d corescope"
    assert_true "transport: pubkey not in remote argv" lacks "$argv" "$PK_A"
    assert_true "transport: SQL not in remote argv"    lacks "$argv" "SELECT"
    assert_true "transport: column not in remote argv" lacks "$argv" "from_pubkey"
    assert_true "transport: SQL arrives on stdin"      contains "$stdin" "SELECT COUNT(*) FROM transmissions WHERE from_pubkey = lower(convert_from(decode("
    assert_true "transport: pubkey arrives hex-encoded on stdin" contains "$stdin" "$(pg_hex_literal "$PK_A")"
    assert_true "transport: raw pubkey never on stdin" lacks "$stdin" "$PK_A"

    # No container configured: a classified failure with no count, never "0".
    reset_stub; TARGET_DB_CONTAINER=""
    read_retain_count >/dev/null 2>&1; rc=$?
    assert_eq "no container: rc"    "1" "$rc"
    assert_eq "no container: count" ""  "$RETAIN_COUNT"
    TARGET_DB_CONTAINER="corescope-pg-stub"

    # A server that cannot answer the probe is rejected, not trusted.
    reset_stub; STUB_DB="$DB_LEGACY"
    read_retain_count >/dev/null 2>&1; rc=$?
    assert_eq "schema error on the target: rc"    "1" "$rc"
    assert_eq "schema error on the target: count" ""  "$RETAIN_COUNT"
    assert_true "schema error on the target: stderr kept" contains "$(cat "$TMP/psql.err")" "from_pubkey"
    STUB_DB="$DB_REAL"
fi

echo "test-blacklist-sql.sh: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
