#!/bin/bash
# The go-live gate for a frontend that admits traffic while `qbit-prism-server import-audits` is
# still storing legacy audit bodies.
#
# In production, `self-check` stops at legacy audit completeness and skips its local checks until
# the import has finished (#734). This script checks every fact `self-check` checks
# (`self_check()` and `self_check_local()` in crates/qbit-prism-server/src/tools.rs), with one
# exception: legacy completeness, which it reports as "N legacy rows pending, import running".
# It still fails on any native audit row that self-check would count as incomplete.
#
# Run it on each frontend host, as the frontend's service user, with that frontend's operator
# environment exported (the environment `self-check` would read):
#   set -a; . <the frontend's environment file>; set +a
#   bash prism_legacy_import_admission_gate.sh
# It needs `qbit-prism-server` and `psql` on PATH. Every query runs in a read-only transaction.
# Nothing prints a DSN or a token. Exit 0 only when every check passes.
set -uo pipefail
: "${PRISM_DATABASE_URL:?export the frontend environment first}"
fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad() { printf 'FAIL  %s\n' "$1"; fail=1; }
info() { printf 'INFO  %s\n' "$1"; }
psqlq() {
  psql "$PRISM_DATABASE_URL" -XAtq -v ON_ERROR_STOP=1 \
    -c "SET default_transaction_read_only = on" -c "$1" 2>/dev/null
}

# 1. Configuration under production rules, the pool fee settling dust (#525) and the block
#    submission state (#291). `check-config` runs all three, plus every other settings loader.
if out=$(qbit-prism-server check-config 2>&1); then
  pass "check-config"
else
  bad "check-config: $(tail -1 <<<"$out")"
fi
grep -E '^WARNING' <<<"$out" | sed 's/^/INFO  check-config /'

# 2. The live-instance census. self-check fails only when the heartbeat read fails, and warns
#    below two live frontends.
if rows=$(psqlq "SELECT jsonb_agg(jsonb_build_object('id', instance_id, 'age_s', round(extract(epoch FROM clock_timestamp() - heartbeat_at)::numeric, 1), 'status', left(status::text, 80)) ORDER BY instance_id) FROM qbit_prism_instances"); then
  pass "heartbeat census read: ${rows:-[]}"
else
  bad "heartbeat census read failed"
fi

# 3. The cluster's block submission hold (#664): reported, never a failure.
info "submission hold: $(qbit-prism-server submission-hold show 2>&1 | tr '\n' ' ' | cut -c1-300)"

# 4. Audit completeness, as `audit_completeness()` counts it, split by kind. Legacy rows (no
#    snapshot) are what import-audits is storing. A native row (with a snapshot) that has no
#    bytes and either no object body or no snapshot row is incomplete, and fails here as it
#    would fail self-check.
if c=$(psqlq "SELECT count(*) FILTER (WHERE canonical_audit_bytes IS NULL AND share_snapshot_sha256 IS NULL), count(*) FILTER (WHERE canonical_audit_bytes IS NULL AND share_snapshot_sha256 IS NOT NULL AND (jsonb_typeof(audit_bundle) IS DISTINCT FROM 'object' OR NOT EXISTS (SELECT 1 FROM qbit_prism_audit_snapshots s WHERE s.snapshot_sha256 = a.share_snapshot_sha256))) FROM qbit_pool_audit_bundles a"); then
  IFS='|' read -r legacy native <<<"$c"
  info "audit completeness: ${legacy} legacy rows pending, import running"
  if [ "$native" = 0 ]; then
    pass "native audit rows complete"
  else
    bad "${native} native audit rows are incomplete"
  fi
else
  bad "audit completeness read failed"
fi

# 5. Carry-forward integrity: mismatch_count and current_drift_count are 0.
if r=$(psqlq "SELECT (x->>'mismatch_count') || '|' || (x->>'current_drift_count') FROM (SELECT qbit_carry_forward_integrity_report() AS x) t"); then
  IFS='|' read -r mismatch drift <<<"$r"
  if [ "$mismatch" = 0 ] && [ "$drift" = 0 ]; then
    pass "carry-forward integrity (mismatch 0, drift 0)"
  else
    bad "carry-forward integrity: mismatch_count=$mismatch current_drift_count=$drift"
  fi
else
  bad "carry-forward integrity report failed"
fi

# 6. Durability: fsync, full_page_writes and synchronous_commit are not off.
if d=$(psqlq "SELECT string_agg(name || '=' || setting, ' ' ORDER BY name) FROM pg_settings WHERE name IN ('fsync', 'full_page_writes', 'synchronous_commit')"); then
  if grep -q '=off' <<<"$d"; then bad "durability: $d"; else pass "durability: $d"; fi
else
  bad "durability read failed"
fi

# 7. The found-block offer's standby (#529), when PRISM_OFFER_STANDBY_APPLICATION_NAME is set: the
#    role can read replication positions, and exactly one streaming or catching-up standby by that
#    name reports a flush position.
if [ -n "${PRISM_OFFER_STANDBY_APPLICATION_NAME:-}" ]; then
  s=$(psql "$PRISM_DATABASE_URL" -XAtq -v ON_ERROR_STOP=1 \
    -v app="$PRISM_OFFER_STANDBY_APPLICATION_NAME" <<'SQL' 2>/dev/null
SET default_transaction_read_only = on;
SELECT pg_has_role(current_user, 'pg_read_all_stats', 'USAGE')::text || '|' || count(*) || '|' || count(flush_lsn)
FROM pg_catalog.pg_stat_replication
WHERE application_name = :'app' AND state IN ('streaming', 'catchup');
SQL
  )
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

# 9. The highdiff listener's floor, when highdiff is configured: the first difficulty it
#    advertises is at least PRISM_STRATUM_HIGHDIFF_MIN_DIFF (default 500000).
if [ -n "${PRISM_STRATUM_HIGHDIFF_PORT:-}${PRISM_STRATUM_HIGHDIFF_BIND:-}" ]; then
  user=${PRISM_SELF_CHECK_ADDRESS:-${PRISM_USERNAME_FALLBACK:-${PRISM_FEE_ADDRESS:-}}}
  [ -n "$user" ] || user=$(psqlq "SELECT miner_id FROM qbit_share_ledger ORDER BY share_seq DESC LIMIT 1")
  if res=$(python3 - "${PRISM_STRATUM_HIGHDIFF_BIND:-${PRISM_STRATUM_BIND:-127.0.0.1}}" \
    "${PRISM_STRATUM_HIGHDIFF_PORT:-4334}" "$user" "${PRISM_STRATUM_HIGHDIFF_MIN_DIFF:-500000}" <<'PY' 2>&1
import json, socket, sys
host, port, user, floor = sys.argv[1], int(sys.argv[2]), sys.argv[3], float(sys.argv[4])
host = {"0.0.0.0": "127.0.0.1", "": "127.0.0.1", "::": "::1"}.get(host, host)
conn = socket.create_connection((host, port), timeout=15)
conn.settimeout(15)
for message in ({"id": 1, "method": "mining.subscribe", "params": ["prism-admission-gate"]},
                {"id": 2, "method": "mining.authorize", "params": [user, "x"]}):
    conn.sendall((json.dumps(message) + "\n").encode())
pending = b""
while True:
    chunk = conn.recv(65536)
    if not chunk:
        sys.exit("disconnected before a difficulty")
    pending += chunk
    while b"\n" in pending:
        line, pending = pending.split(b"\n", 1)
        value = json.loads(line)
        if value.get("error"):
            sys.exit(f"rejected: {value['error']}")
        if value.get("method") == "mining.set_difficulty":
            difficulty = float(value["params"][0])
            print(f"{difficulty} {'>=' if difficulty >= floor else '<'} {floor}")
            sys.exit(0 if difficulty >= floor else 1)
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
  echo "ADMISSION GATE: PASS (legacy audit import pending, reported above)"
else
  echo "ADMISSION GATE: FAIL"
fi
exit "$fail"
