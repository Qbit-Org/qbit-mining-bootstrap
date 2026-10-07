#!/bin/bash
# The go-live gate for a frontend that admits traffic while `qbit-prism-server import-audits` is
# still storing legacy audit bodies.
#
# In production, `self-check` stops at legacy audit completeness and skips its local checks until
# the import has finished (#734). This script checks every fact `self-check` checks
# (`self_check()` and `self_check_local()` in crates/qbit-prism-server/src/tools.rs), with one
# exception: legacy completeness, which it reports as pending, together with whether an
# import-audits session can be seen. It still fails on any native audit row that self-check would
# count as incomplete.
#
# Run it on each frontend host, as the frontend's service user, with that frontend's operator
# environment exported (the environment `self-check` would read):
#   set -a; . <the frontend's environment file>; set +a
#   bash prism_legacy_import_admission_gate.sh
# It needs `qbit-prism-server`, `psql` and `python3` on PATH. Every query runs in a read-only
# transaction. psql is given the database URL without its password, which it reads from
# PGPASSWORD, so no credential appears in a command line or in the output. Exit 0 only when
# every check passes.
#
# Every query runs under statement_timeout 15 s and lock_timeout 5 s, except check 5's
# carry-forward integrity report, which takes about a minute at production size: 120 s on the
# writer, 600 s on a standby. The report holds a snapshot for that long, and on a primary with
# live miners every share updates the cluster row, so a long snapshot there slows the share
# append's lock (#738). Above about 200 shares/s, run the report elsewhere:
#   PRISM_INTEGRITY_REPORT_DATABASE_URL  run check 5 on this hot standby instead, which must
#       have hot_standby_feedback off (or the report holds back the writer anyway). Pause its
#       replay first (SELECT pg_wal_replay_pause(); until pg_get_wal_replay_pause_state() is
#       'paused'), or set max_standby_streaming_delay and max_standby_archive_delay to -1
#       there, or a recovery conflict can cancel the report. Never use a standby the writer
#       waits on for apply (synchronous_commit = remote_apply): pausing it stalls every commit.
#       The found-block offer standby (#529) waits for flush, which a pause doesn't stop. The
#       gate refuses a URL that is not in recovery, is not a replica of this writer's database
#       (system identifier and database name) on the writer's timeline, has hot_standby_feedback
#       on, serves a writer that waits for apply, or is stale: its last replayed transaction is
#       older than PRISM_INTEGRITY_REPORT_MAX_AGE_SECONDS (default 300). The live frontends'
#       heartbeats keep the writer committing, so a following standby stays well inside that.
#       Right after a failover a following standby can show its previous timeline until its
#       next restartpoint; rerun then. The gate records both WAL positions.
#   PRISM_GATE_INTEGRITY=skip  skip check 5 and say so (WARN). Record the report from wherever
#       it ran instead.
# These limits are the gate's own. PRISM_DATABASE_STATEMENT_TIMEOUT_MS sets the frontends'
# 15 s, which is too short for the report at production size (#737).
set -uo pipefail
: "${PRISM_DATABASE_URL:?export the frontend environment first}"
fail=0
skipped=""
gate_tmp=$(mktemp -d) || exit 2
trap 'rm -rf "$gate_tmp"' EXIT
caller_pgpassword_set=${PGPASSWORD+x}
caller_pgpassword=${PGPASSWORD-}
pass() { printf 'PASS  %s\n' "$1"; }
bad() { printf 'FAIL  %s\n' "$1"; fail=1; }
info() { printf 'INFO  %s\n' "$1"; }
warn() { printf 'WARN  %s\n' "$1"; }
trim() {
  local value=$1
  value=${value#"${value%%[![:space:]]*}"}
  printf '%s' "${value%"${value##*[![:space:]]}"}"
}

# The URL without its password, and the password: from the user info or a `password` query
# parameter, percent-decoded as libpq decodes them. split_url NAME reads the variable NAME.
split_url() {
  GATE_URL=${!1} python3 - <<'PY'
import os, sys, urllib.parse
parts = urllib.parse.urlsplit(os.environ["GATE_URL"].strip())
if parts.scheme not in ("postgres", "postgresql"):
    sys.exit(1)
userinfo, at, hosts = parts.netloc.rpartition("@")
user, _, password = userinfo.partition(":")
password = urllib.parse.unquote(password)
kept = []
for item in parts.query.split("&") if parts.query else []:
    key, _, value = item.partition("=")
    if urllib.parse.unquote(key) == "password":
        password = urllib.parse.unquote(value)
    else:
        kept.append(item)
if "\0" in password:
    sys.exit(1)
netloc = f"{user}@{hosts}" if at else hosts
url = urllib.parse.urlunsplit(parts._replace(netloc=netloc, query="&".join(kept)))
sys.stdout.write(url + "\0" + password + "\0")
PY
}
if ! { IFS= read -r -d '' database && IFS= read -r -d '' password; } < <(split_url PRISM_DATABASE_URL); then
  echo "PRISM_DATABASE_URL must be a postgres:// or postgresql:// URL" >&2
  exit 2
fi
integrity_database=""
integrity_password=""
integrity_url=$(trim "${PRISM_INTEGRITY_REPORT_DATABASE_URL:-}")
if [ -n "$integrity_url" ] && ! {
  IFS= read -r -d '' integrity_database && IFS= read -r -d '' integrity_password
} < <(split_url integrity_url); then
  echo "PRISM_INTEGRITY_REPORT_DATABASE_URL must be a postgres:// or postgresql:// URL" >&2
  exit 2
fi
max_age=$(trim "${PRISM_INTEGRITY_REPORT_MAX_AGE_SECONDS:-}")
max_age=${max_age:-300}
if ! [[ $max_age =~ ^[0-9]{1,9}$ ]] || (( 10#$max_age < 1 )); then
  echo "PRISM_INTEGRITY_REPORT_MAX_AGE_SECONDS must be a whole number of seconds from 1 to 999999999" >&2
  exit 2
fi
max_age=$(( 10#$max_age ))
if [ -n "$password" ]; then export PGPASSWORD=$password; fi
unset password
# psqlq SQL [psql -v name=value ...]: one read-only transaction against the frontend's
# database, or, with q_target=integrity, against PRISM_INTEGRITY_REPORT_DATABASE_URL.
# q_timeout overrides the 15 s statement timeout. psql's stderr is kept apart and only ever
# classified by last_error: it can name the host and user.
psqlq() {
  local sql=$1
  shift
  ( # A subshell, so the standby's credential stays in this process's environment, never argv.
    target=$database
    if [ "${q_target:-}" = integrity ]; then
      target=$integrity_database
      if [ -n "$integrity_password" ]; then
        export PGPASSWORD=$integrity_password
      elif [ -n "$caller_pgpassword_set" ]; then
        export PGPASSWORD=$caller_pgpassword
      else
        unset PGPASSWORD
      fi
    fi
    printf "SET default_transaction_read_only = on;\nSET statement_timeout = '%s';\nSET lock_timeout = '5s';\n%s;\n" \
      "${q_timeout:-15s}" "$sql" |
      psql "$target" -XAtq -v ON_ERROR_STOP=1 "$@" 2>"$gate_tmp/psql.err"
  )
}
# Why the last psqlq failed, when psql said something recognizable, without echoing it.
last_error() {
  local error
  error=$(cat "$gate_tmp/psql.err" 2>/dev/null)
  case $error in
    *"canceling statement due to statement timeout"*) printf ' (statement timeout)' ;;
    *"conflict with recovery"*)
      printf ' (cancelled by a recovery conflict: pause the standby'"'"'s replay or set max_standby_streaming_delay = -1 there)' ;;
    *"lock timeout"*) printf ' (lock timeout)' ;;
  esac
}

# 1. Configuration under production rules, the pool fee settling dust (#525) and the block
#    submission state (#291). `check-config` runs all three, plus every other settings loader.
if out=$(qbit-prism-server check-config 2>&1); then
  pass "check-config"
else
  bad "check-config: $(tail -1 <<<"$out")"
fi
grep -E '^WARNING' <<<"$out" | sed 's/^/INFO  check-config /'

# 2. The live-instance census, classified as `live_instances()` classifies it. self-check fails
#    only when the heartbeat read fails, and warns when HA is unknown or fewer than two
#    frontends are live. Freshness is 3 health refreshes, at least 15 s.
refresh=$(trim "${PRISM_HEALTH_REFRESH_SECONDS:-}")
if [[ ${refresh:-2} =~ ^[0-9]+$ ]] && (( 10#${refresh:-2} >= 1 )); then
  fresh=$(( 10#${refresh:-2} * 3 > 15 ? 10#${refresh:-2} * 3 : 15 ))
else
  fresh=15
fi
if census=$(psqlq "WITH sample AS (SELECT clock_timestamp() AS observed_at),
heartbeats AS (
  SELECT instance_id, extract(epoch FROM (observed_at - heartbeat_at)) AS age, status,
    coalesce(jsonb_typeof(status) = 'object'
      AND status->>'schema' = 'qbit.prism.audit-health.v1'
      AND jsonb_typeof(status->'ready') = 'boolean', false) AS health
  FROM qbit_prism_instances, sample)
SELECT count(*) FILTER (WHERE age BETWEEN 0 AND :fresh AND health) || '|'
  || count(*) FILTER (WHERE age > :fresh) || '|'
  || count(*) FILTER (WHERE age BETWEEN 0 AND :fresh AND NOT health
       AND coalesce(status->>'state', '') IN ('starting', 'stopped')) || '|'
  || count(*) FILTER (WHERE age < 0 OR (age <= :fresh AND NOT health
       AND coalesce(status->>'state', '') NOT IN ('starting', 'stopped'))) || '|'
  || coalesce(jsonb_agg(jsonb_build_object('id', instance_id, 'age_s', round(age::numeric, 1),
       'status', left(status::text, 80)) ORDER BY instance_id)::text, '[]')
FROM heartbeats" -v fresh="$fresh"); then
  IFS='|' read -r live stale inactive unknown rows <<<"$census"
  pass "heartbeat census read: $rows"
  info "HA: $live live, $stale stale, $inactive inactive, $unknown unknown (freshness ${fresh}s)"
  if [ "$unknown" != 0 ]; then
    warn "Unrecognized or future-dated heartbeats; HA is unknown"
  elif [ "$live" -lt 2 ]; then
    warn "Fewer than two live frontends observed; do not present this deployment as HA"
  fi
else
  bad "heartbeat census read failed$(last_error)"
fi

# 3. The cluster's block submission hold (#664): reported, never a failure.
info "submission hold: $(qbit-prism-server submission-hold show 2>&1 | tr '\n' ' ' | cut -c1-300)"

# 4. Audit completeness, as `audit_completeness()` counts it, split by kind. Legacy rows (no
#    snapshot) are what import-audits is storing. A native row (with a snapshot) that has no
#    bytes and either no object body or no snapshot row is incomplete, and fails here as it
#    would fail self-check.
if c=$(psqlq "SELECT count(*) FILTER (WHERE canonical_audit_bytes IS NULL AND share_snapshot_sha256 IS NULL) || '|' || count(*) FILTER (WHERE canonical_audit_bytes IS NULL AND share_snapshot_sha256 IS NOT NULL AND (jsonb_typeof(audit_bundle) IS DISTINCT FROM 'object' OR NOT EXISTS (SELECT 1 FROM qbit_prism_audit_snapshots s WHERE s.snapshot_sha256 = a.share_snapshot_sha256))) FROM qbit_pool_audit_bundles a"); then
  IFS='|' read -r legacy native <<<"$c"
  info "audit completeness: ${legacy} legacy rows pending"
  if [ "$native" = 0 ]; then
    pass "native audit rows complete"
  else
    bad "${native} native audit rows are incomplete"
  fi
  # Whether import-audits is running: a session whose last statement is the import's row read
  # or write. Other sessions' statements need pg_read_all_stats to be seen.
  if [ "${legacy:-0}" != 0 ]; then
    if s=$(psqlq "SELECT count(*) FILTER (WHERE importing) || '|' || coalesce(round(extract(epoch FROM clock_timestamp() - max(query_start) FILTER (WHERE importing)))::text, '-') || '|' || count(*) FILTER (WHERE query = '<insufficient privilege>') FROM (SELECT query, query_start, (query LIKE 'UPDATE qbit_pool_audit_bundles SET schema_version=%' OR query LIKE 'SELECT block_hash,body_uri,audit_bundle,audit_bundle_sha256,coinbase_tx_hex FROM qbit_pool_audit_bundles%') AS importing FROM pg_stat_activity WHERE pid <> pg_backend_pid()) t"); then
      IFS='|' read -r sessions age hidden <<<"$s"
      if [ "$sessions" != 0 ]; then
        info "import-audits: $sessions session(s), last statement ${age}s ago"
      elif [ "$hidden" != 0 ]; then
        info "import-audits progress not visible to this role (pg_read_all_stats shows it)"
      else
        warn "no import-audits session found: legacy completeness will not progress until it runs"
      fi
    else
      warn "could not read pg_stat_activity for import-audits progress"
    fi
  fi
else
  bad "audit completeness read failed$(last_error)"
fi

# 5. Carry-forward integrity: mismatch_count and current_drift_count are 0. On the writer by
#    default; on a hot standby with PRISM_INTEGRITY_REPORT_DATABASE_URL; skipped, with a WARN,
#    with PRISM_GATE_INTEGRITY=skip. See the header for the standby's requirements.
integrity() { # integrity [q_target]
  local r mismatch drift where="" limit=120s
  if [ -n "${1:-}" ]; then where=" on the standby"; limit=600s; fi
  if r=$(q_target=${1:-} q_timeout=$limit psqlq "SELECT (x->>'mismatch_count') || '|' || (x->>'current_drift_count') FROM (SELECT qbit_carry_forward_integrity_report() AS x) t"); then
    IFS='|' read -r mismatch drift <<<"$r"
    if [ "$mismatch" = 0 ] && [ "$drift" = 0 ]; then
      pass "carry-forward integrity$where (mismatch 0, drift 0)"
    else
      bad "carry-forward integrity$where: mismatch_count=$mismatch current_drift_count=$drift"
    fi
  else
    bad "carry-forward integrity report failed$where$(last_error)"
  fi
}
integrity_mode=$(trim "${PRISM_GATE_INTEGRITY:-}")
identity_sql="(SELECT system_identifier FROM pg_control_system())::text || '|' || current_database()"
if [ -n "$integrity_mode" ] && [ "$integrity_mode" != skip ]; then
  bad "PRISM_GATE_INTEGRITY must be empty or skip"
elif [ "$integrity_mode" = skip ]; then
  warn "carry-forward integrity skipped (PRISM_GATE_INTEGRITY=skip): record the report from where it ran"
  if [ -n "$integrity_database" ]; then
    warn "PRISM_INTEGRITY_REPORT_DATABASE_URL is set but not used: PRISM_GATE_INTEGRITY=skip"
  fi
  skipped=" (carry-forward integrity skipped)"
elif [ -n "$integrity_database" ]; then
  if ! w=$(psqlq "SELECT current_setting('synchronous_commit') || '|' || current_setting('synchronous_standby_names') || '|' || pg_current_wal_lsn()::text || '|' || $identity_sql || '|' || substring(pg_walfile_name(pg_current_wal_lsn()), 1, 8)"); then
    bad "writer position read failed$(last_error)"
  else
    IFS='|' read -r commit_mode sync_names writer_lsn writer_system writer_db writer_tli <<<"$w"
    # The standby's timeline: the WAL receiver's, when this role may read it, else its last
    # restartpoint's (which can trail a timeline switch until the next restartpoint). WAL
    # positions on different timelines aren't comparable, so a mismatch refuses. The age is
    # clamped at 0 against clock skew between the hosts.
    if ! r=$(q_target=integrity psqlq "SELECT pg_is_in_recovery()::text || '|' || coalesce(pg_last_wal_replay_lsn()::text, '-') || '|' || CASE WHEN pg_is_in_recovery() THEN pg_get_wal_replay_pause_state() ELSE '-' END || '|' || coalesce(greatest(0, round(extract(epoch FROM clock_timestamp() - pg_last_xact_replay_timestamp())))::text, '-') || '|' || current_setting('hot_standby_feedback') || '|' || current_setting('max_standby_streaming_delay') || '|' || current_setting('max_standby_archive_delay') || '|' || $identity_sql || '|' || lpad(upper(to_hex(coalesce((SELECT received_tli FROM pg_stat_wal_receiver), (SELECT timeline_id FROM pg_control_checkpoint())))), 8, '0')"); then
      bad "integrity standby read failed$(last_error)"
    else
      IFS='|' read -r recovery lsn pause age feedback stream_delay archive_delay system db tli <<<"$r"
      if [ "$recovery" != true ]; then
        bad "PRISM_INTEGRITY_REPORT_DATABASE_URL is not a hot standby (in recovery: $recovery); unset it to run the report on the writer"
      elif [ "$system" != "$writer_system" ] || [ "$db" != "$writer_db" ]; then
        bad "the integrity standby is not a replica of this writer's database (its system identifier or database name differs)"
      elif [ "$tli" != "$writer_tli" ]; then
        bad "the integrity standby is on timeline $tli but the writer is on $writer_tli: after a failover, let it follow the writer's timeline (a follower shows it from its next restartpoint), or unset the URL"
      elif [ "$commit_mode" = remote_apply ] && [ -n "$sync_names" ]; then
        bad "the writer waits for standby apply (synchronous_commit=remote_apply): pausing one of its synchronous standbys stalls every commit, so resume this standby's replay now (SELECT pg_wal_replay_resume();) and use an asynchronous standby, or skip"
      elif [ "$feedback" = on ]; then
        bad "the integrity standby has hot_standby_feedback on: its report would hold back the writer's horizon all the same (#738); turn it off there, or unset the URL"
      elif ! [[ $age =~ ^[0-9]+$ ]]; then
        bad "the integrity standby has replayed no transaction yet; let it catch up, then pause it and rerun"
      elif (( age > max_age )); then
        bad "the integrity standby's last replayed transaction is ${age}s old (limit ${max_age}s): resume its replay until it catches up, then pause it and rerun"
      else
        info "integrity report on a standby: replayed to $lsn on timeline $tli (writer at $writer_lsn), replay state '$pause', last replayed transaction ${age}s ago"
        if [ "$pause" != paused ] && ! { [ "$stream_delay" = -1 ] && [ "$archive_delay" = -1 ]; }; then
          warn "the integrity standby's replay state is '$pause', not 'paused': a recovery conflict can cancel the report; pause it (SELECT pg_wal_replay_pause(); until pg_get_wal_replay_pause_state() is 'paused') or set max_standby_streaming_delay and max_standby_archive_delay to -1 there"
        fi
        integrity integrity
      fi
    fi
  fi
else
  integrity
fi

# 6. Durability: fsync, full_page_writes and synchronous_commit are not off.
if d=$(psqlq "SELECT string_agg(name || '=' || setting, ' ' ORDER BY name) FROM pg_settings WHERE name IN ('fsync', 'full_page_writes', 'synchronous_commit')"); then
  if grep -q '=off' <<<"$d"; then bad "durability: $d"; else pass "durability: $d"; fi
else
  bad "durability read failed$(last_error)"
fi

# 7. The found-block offer's standby (#529), on when PRISM_OFFER_STANDBY_APPLICATION_NAME is set
#    and PRISM_OFFER_STANDBY_FLUSH_WAIT_MS is not 0 (default 250): the role can read replication
#    positions, and exactly one streaming or catching-up standby by that name reports a flush
#    position.
standby=$(trim "${PRISM_OFFER_STANDBY_APPLICATION_NAME:-}")
wait_ms=$(trim "${PRISM_OFFER_STANDBY_FLUSH_WAIT_MS:-}")
if [ -n "$standby" ] && ! [[ $wait_ms =~ ^\+?0+$ ]]; then
  s=$(psqlq "SELECT pg_has_role(current_user, 'pg_read_all_stats', 'USAGE')::text || '|' || count(*) || '|' || count(flush_lsn) FROM pg_catalog.pg_stat_replication WHERE application_name = :'app' AND state IN ('streaming', 'catchup')" -v app="$standby")
  if [ "$s" = "true|1|1" ]; then
    pass "offer standby usable"
  else
    bad "offer standby not usable (readable|standbys|flushing = ${s:-error})"
  fi
else
  info "offer standby not configured"
fi

# 8. The frontend's own readiness, through the same `healthcheck` self-check calls: HTTP 2xx and
#    ok=true. It also stands for self-check's tool coordinator start and `refresh_once()`: this
#    frontend passed the same startup gates and has published work on the observed tip.
if out=$(qbit-prism-server healthcheck 2>&1); then
  pass "healthcheck (frontend ready on the observed tip)"
else
  bad "healthcheck: $(tail -1 <<<"$out")"
fi

# 9. The highdiff listener's floor, when PRISM_STRATUM_HIGHDIFF_PORT is set: within 15 s, the
#    first difficulty it advertises is at least PRISM_STRATUM_HIGHDIFF_MIN_DIFF (default 500000).
#    The probe authorizes as self-check does: PRISM_SELF_CHECK_ADDRESS, the username fallback
#    (PRISM_USERNAME_FALLBACK_ADDRESS, or the built-in test-network address), the pool fee
#    address, then the most recent miner. Like config::optional, a blank variable counts as
#    unset and a set one is used verbatim.
highdiff_port=$(trim "${PRISM_STRATUM_HIGHDIFF_PORT:-}")
if [ -n "$highdiff_port" ]; then
  user=""
  for name in PRISM_SELF_CHECK_ADDRESS PRISM_USERNAME_FALLBACK_ADDRESS; do
    if [ -z "$user" ] && [ -n "$(trim "${!name:-}")" ]; then user=${!name}; fi
  done
  if [ -z "$user" ]; then
    case "$(trim "${QBIT_CHAIN:-regtest}" | tr '[:upper:]' '[:lower:]')" in
      test | testnet | testnet3 | testnet4 | signet)
        user=tq1zlsq9dpxz8mennhdpr9nf9s0f2tjtq6gxs9m84k6xglhkfp92q2zszzu4m3 ;;
    esac
  fi
  if [ -z "$user" ] && [ -n "$(trim "${PRISM_POOL_FEE_ADDRESS:-}")" ]; then
    user=$PRISM_POOL_FEE_ADDRESS
  fi
  [ -n "$user" ] || user=$(psqlq "SELECT miner_id FROM qbit_share_ledger ORDER BY share_seq DESC LIMIT 1")
  bind=${PRISM_STRATUM_HIGHDIFF_BIND:-}
  [ -n "$(trim "$bind")" ] || bind=${PRISM_STRATUM_BIND-127.0.0.1}
  floor=$(trim "${PRISM_STRATUM_HIGHDIFF_MIN_DIFF:-}")
  if [ -z "$user" ]; then
    bad "highdiff probe: set PRISM_SELF_CHECK_ADDRESS to a valid P2MR address to probe highdiff on an empty pool"
  elif res=$(python3 - "$bind" "$highdiff_port" "$user" "${floor:-500000}" <<'PY' 2>&1
import json, math, socket, sys, time
host, port, user, floor = sys.argv[1], int(sys.argv[2]), sys.argv[3], float(sys.argv[4])
host = {"0.0.0.0": "127.0.0.1", "::": "::1"}.get(host, host).strip("[]")
deadline = time.monotonic() + 15

def remaining():
    left = deadline - time.monotonic()
    if left <= 0:
        raise socket.timeout()
    return left

try:
    conn = socket.create_connection((host, port), timeout=remaining())
    for message in ({"id": 1, "method": "mining.subscribe", "params": ["prism-admission-gate"]},
                    {"id": 2, "method": "mining.authorize", "params": [user, "x"]}):
        conn.settimeout(remaining())
        conn.sendall((json.dumps(message) + "\n").encode())
    pending = b""
    while True:
        conn.settimeout(remaining())
        chunk = conn.recv(65536)
        if not chunk:
            sys.exit("Stratum probe disconnected before difficulty")
        pending += chunk
        while b"\n" in pending:
            line, pending = pending.split(b"\n", 1)
            if len(line) > 1024 * 1024:
                sys.exit("oversized Stratum probe response")
            value = json.loads(line)
            if not isinstance(value, dict):
                continue
            if value.get("error") is not None:
                sys.exit(f"Stratum probe rejected: {value['error']}")
            if value.get("method") == "mining.set_difficulty":
                params = value.get("params")
                difficulty = params[0] if isinstance(params, list) and params else None
                if (isinstance(difficulty, bool) or not isinstance(difficulty, (int, float))
                        or not math.isfinite(difficulty) or difficulty <= 0):
                    sys.exit("invalid advertised difficulty")
                difficulty = float(difficulty)
                print(f"{difficulty} {'>=' if difficulty >= floor else '<'} {floor}")
                sys.exit(0 if difficulty >= floor else 1)
        if len(pending) > 1024 * 1024:
            sys.exit("oversized Stratum probe response")
except socket.timeout:
    sys.exit("Stratum difficulty probe timed out")
except OSError as error:
    sys.exit(f"Stratum probe failed: {error}")
PY
  ); then
    pass "highdiff first difficulty $res"
  else
    bad "highdiff probe: $res"
  fi
else
  info "highdiff not configured on this host"
fi

if [ "$fail" = 0 ]; then
  echo "ADMISSION GATE: PASS (legacy audit import pending, reported above)$skipped"
else
  echo "ADMISSION GATE: FAIL"
fi
exit "$fail"
