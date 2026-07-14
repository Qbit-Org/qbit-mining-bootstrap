#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/test/test-lib.sh"
DATADIR="${DATADIR:-$(mktemp -d -t qbit-prism-live.XXXXXX)}"
RPC_USER="${RPC_USER:-qbitrpc}"
RPC_PASSWORD="${RPC_PASSWORD:-change-this}"
RPC_PORT="${RPC_PORT:-18455}"
WALLET_NAME="${WALLET_NAME:-prism-live}"
STRATUM_PORT="${PRISM_STRATUM_PORT:-3340}"
AUDIT_PORT="${PRISM_AUDIT_PORT:-3341}"
HIGHDIFF_PORT="${PRISM_STRATUM_HIGHDIFF_PORT:-4334}"
HIGHDIFF_START_DIFF="${PRISM_STRATUM_HIGHDIFF_START_DIFF:-500000}"
POWER_LAW_ENABLED="${QBIT_PRISM_LIVE_POWER_LAW:-0}"
if [[ "${POWER_LAW_ENABLED}" == "1" ]]; then
  MINER_COUNT="${QBIT_PRISM_LIVE_MINERS:-6}"
else
  MINER_COUNT="${QBIT_PRISM_LIVE_MINERS:-3}"
fi
TARGET_BLOCKS="${QBIT_PRISM_LIVE_BLOCKS:-1}"
EVIDENCE_PATH="${DATADIR}/prism-live-evidence.json"
COORDINATOR_LOG="${DATADIR}/prism-coordinator.log"
MINER_TIMEOUT_SECONDS="${QBIT_PRISM_LIVE_MINER_TIMEOUT_SECONDS:-90}"
AUDIT_API_ENABLED="${QBIT_PRISM_LIVE_AUDIT_API:-0}"
POSTGRES_ENABLED="${QBIT_PRISM_LIVE_POSTGRES:-0}"
POSTGRES_IMAGE="${QBIT_PRISM_POSTGRES_IMAGE:-postgres:16-alpine}"
POSTGRES_CONTAINER="${QBIT_PRISM_POSTGRES_CONTAINER:-qbit-prism-pg-$$}"
MANIFEST_SIGNING_SEED_HEX="${QBIT_PRISM_MANIFEST_SIGNING_SEED_HEX:-4242424242424242424242424242424242424242424242424242424242424242}"
LEDGER_ATTESTATION_SIGNING_SEED_HEX="${QBIT_PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX:-4343434343434343434343434343434343434343434343434343434343434343}"
PSQL_COMMAND=""

resolve_qbit_binaries
read -r -a CARGO_CMD <<< "${CARGO:-cargo}"
require_executable "${CARGO_CMD[0]}"
require_executable python3
# When QBIT_PRISM_EXTERNAL_PSQL_COMMAND is set, use an already-running Postgres
# (e.g. a local cluster) instead of provisioning a Docker container.
EXTERNAL_PSQL="${QBIT_PRISM_EXTERNAL_PSQL_COMMAND:-}"
if [[ "${POSTGRES_ENABLED}" == "1" && -z "${EXTERNAL_PSQL}" ]]; then
  require_executable docker
fi
run_cargo() {
  "${CARGO_CMD[@]}" "$@"
}
if [[ "${POSTGRES_ENABLED}" == "1" && "${AUDIT_API_ENABLED}" != "1" ]]; then
  echo "QBIT_PRISM_LIVE_POSTGRES=1 requires QBIT_PRISM_LIVE_AUDIT_API=1 for DB/API proof checks" >&2
  exit 1
fi
if [[ "${TARGET_BLOCKS}" -gt 1 && "${AUDIT_API_ENABLED}" != "1" ]]; then
  echo "QBIT_PRISM_LIVE_BLOCKS>1 requires QBIT_PRISM_LIVE_AUDIT_API=1" >&2
  exit 1
fi
if [[ "${POWER_LAW_ENABLED}" == "1" && "${MINER_COUNT}" -lt 6 ]]; then
  echo "QBIT_PRISM_LIVE_POWER_LAW=1 requires at least 6 miners" >&2
  exit 1
fi
require_qbit_src_helper "test/functional/test_framework/blocktools.py" "the live PRISM Stratum regtest smoke"

coordinator_pid=""
miner_pids=()
postgres_started=0

cleanup() {
  for pid in "${miner_pids[@]:-}"; do
    kill "${pid}" >/dev/null 2>&1 || true
  done
  if [[ -n "${coordinator_pid}" ]]; then
    kill "${coordinator_pid}" >/dev/null 2>&1 || true
  fi
  if [[ "${postgres_started}" == "1" ]]; then
    docker rm -f "${POSTGRES_CONTAINER}" >/dev/null 2>&1 || true
  fi
  stop_qbitd
  remove_datadir "${DATADIR}"
}
trap cleanup EXIT

if [[ "$(ulimit -n)" == "unlimited" ]]; then
  ulimit -n 1024 || true
fi

if [[ "${POSTGRES_ENABLED}" == "1" ]]; then
  if [[ -n "${EXTERNAL_PSQL}" ]]; then
    PSQL_COMMAND="${EXTERNAL_PSQL}"
    deadline=$((SECONDS + 60))
    until echo 'SELECT 1;' | ${PSQL_COMMAND} >/dev/null 2>&1; do
      if [[ "${SECONDS}" -ge "${deadline}" ]]; then
        echo "timed out waiting for external PRISM Postgres" >&2
        exit 1
      fi
      sleep 1
    done
  else
    docker rm -f "${POSTGRES_CONTAINER}" >/dev/null 2>&1 || true
    docker run \
      --rm \
      --detach \
      --name "${POSTGRES_CONTAINER}" \
      -e POSTGRES_USER=qbit \
      -e POSTGRES_PASSWORD=qbit \
      -e POSTGRES_DB=qbit \
      "${POSTGRES_IMAGE}" >/dev/null
    postgres_started=1
    PSQL_COMMAND="docker exec -i ${POSTGRES_CONTAINER} psql -U qbit -d qbit"
    deadline=$((SECONDS + 60))
    until docker exec "${POSTGRES_CONTAINER}" pg_isready -U qbit -d qbit >/dev/null 2>&1; do
      if [[ "${SECONDS}" -ge "${deadline}" ]]; then
        echo "timed out waiting for PRISM Postgres container" >&2
        docker logs "${POSTGRES_CONTAINER}" >&2 || true
        exit 1
      fi
      sleep 1
    done
  fi
fi

"${QBITD_BIN}" \
  -regtest \
  -asert \
  -p2mronly=1 \
  -daemonwait \
  -server \
  -listen=0 \
  -maxconnections=0 \
  -dnsseed=0 \
  -fixedseeds=0 \
  -rpcbind=127.0.0.1 \
  -rpcallowip=127.0.0.1 \
  -rpcport="${RPC_PORT}" \
  -rpcuser="${RPC_USER}" \
  -rpcpassword="${RPC_PASSWORD}" \
  -fallbackfee=0.00001000 \
  -datadir="${DATADIR}" >/dev/null

wait_for_qbit_rpc_state ready 60
qbit_rpc createwallet "${WALLET_NAME}" >/dev/null

miner_usernames=()
for miner_index in $(seq 1 "${MINER_COUNT}"); do
  miner_usernames+=("$(qbit_rpc -rpcwallet="${WALLET_NAME}" getnewaddress "" p2mr)")
done

# A virgin regtest chain reports initialblockdownload=true until its first
# block, and the coordinator's chain-trust gate blocks job issuance during
# IBD, so a fresh chain could never mine its own first block through the
# pool. Pre-mine one block to exit IBD; the PRISM ledger stays empty, so the
# fresh-ledger (collection-mode) path is still exercised by the miners.
qbit_rpc generatetoaddress 1 "${miner_usernames[0]}" >/dev/null
before_height="$(qbit_rpc getblockcount)"

(
  cd "${ROOT_DIR}"
  export QBIT_RPC_HOST=127.0.0.1
  export QBIT_RPC_PORT="${RPC_PORT}"
  export QBIT_RPC_USER="${RPC_USER}"
  export QBIT_RPC_PASSWORD="${RPC_PASSWORD}"
  export PRISM_STRATUM_BIND=127.0.0.1
  export PRISM_STRATUM_PORT="${STRATUM_PORT}"
  export PRISM_STRATUM_HIGHDIFF_PORT="${HIGHDIFF_PORT}"
  export PRISM_STRATUM_HIGHDIFF_START_DIFF="${HIGHDIFF_START_DIFF}"
  export PRISM_STRATUM_HIGHDIFF_MIN_DIFF="${HIGHDIFF_START_DIFF}"
  export PRISM_MIN_READY_MINERS="${MINER_COUNT}"
  export PRISM_EVIDENCE_PATH="${EVIDENCE_PATH}"
  export PRISM_AUDIT_DIR="${DATADIR}"
  export PRISM_MANIFEST_SIGNING_SEED_HEX="${MANIFEST_SIGNING_SEED_HEX}"
  export PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX="${LEDGER_ATTESTATION_SIGNING_SEED_HEX}"
  export PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY=1
  export PRISM_STRATUM_SHARE_DIFF=0.000000001
  export PRISM_STOP_AFTER_BLOCK=1
  if [[ "${AUDIT_API_ENABLED}" == "1" ]]; then
    export PRISM_AUDIT_BIND=127.0.0.1
    export PRISM_AUDIT_PORT="${AUDIT_PORT}"
    export PRISM_STOP_AFTER_BLOCK=0
    export PRISM_MAX_BLOCKS="${QBIT_PRISM_COORDINATOR_MAX_BLOCKS:-99}"
  else
    export PRISM_MAX_BLOCKS="${TARGET_BLOCKS}"
  fi
  if [[ "${POSTGRES_ENABLED}" == "1" ]]; then
    export PRISM_POSTGRES_PSQL_COMMAND="${PSQL_COMMAND}"
    export PRISM_POSTGRES_INIT_SCHEMA=1
  else
    export PRISM_ALLOW_MEMORY_LEDGER=1
  fi
  python3 lab/prism/prism_coordinator.py
) >"${COORDINATOR_LOG}" 2>&1 &
coordinator_pid="$!"

STRATUM_PORT="${STRATUM_PORT}" python3 <<'PY'
import os
import socket
import time

deadline = time.time() + 30
port = int(os.environ["STRATUM_PORT"])
while time.time() < deadline:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.5):
            raise SystemExit(0)
    except OSError:
        time.sleep(0.25)
raise SystemExit("timed out waiting for PRISM Stratum port")
PY
if ! kill -0 "${coordinator_pid}" >/dev/null 2>&1; then
  echo "PRISM coordinator exited before opening Stratum port" >&2
  cat "${COORDINATOR_LOG}" >&2 || true
  exit 1
fi

STRATUM_PORT="${HIGHDIFF_PORT}" python3 <<'PY'
import os
import socket
import time

deadline = time.time() + 30
port = int(os.environ["STRATUM_PORT"])
while time.time() < deadline:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.5):
            raise SystemExit(0)
    except OSError:
        time.sleep(0.25)
raise SystemExit("timed out waiting for PRISM high-diff Stratum port")
PY
if ! kill -0 "${coordinator_pid}" >/dev/null 2>&1; then
  echo "PRISM coordinator exited before opening high-diff Stratum port" >&2
  cat "${COORDINATOR_LOG}" >&2 || true
  exit 1
fi

# Handshake-only difficulty probes: the first mining.set_difficulty must be the
# listener's start difficulty (the high-diff floor is what rental marketplaces
# verify), and a password d= request must override it within listener bounds.
probe_first_difficulty() {
  PROBE_PORT="$1" PROBE_USERNAME="$2" PROBE_PASSWORD="$3" PROBE_EXPECTED_DIFF="$4" python3 <<'PY'
import json
import os
import socket

port = int(os.environ["PROBE_PORT"])
username = os.environ["PROBE_USERNAME"]
password = os.environ["PROBE_PASSWORD"]
expected = float(os.environ["PROBE_EXPECTED_DIFF"])

with socket.create_connection(("127.0.0.1", port), timeout=15) as sock:
    stream = sock.makefile("rw", encoding="utf-8", newline="\n")

    def send(payload):
        stream.write(json.dumps(payload) + "\n")
        stream.flush()

    send({"id": 1, "method": "mining.subscribe", "params": ["difficulty-probe/1.0"]})
    send({"id": 2, "method": "mining.authorize", "params": [username, password]})
    difficulty = None
    saw_notify_before_difficulty = False
    for _ in range(50):
        line = stream.readline()
        if not line:
            break
        message = json.loads(line)
        if message.get("method") == "mining.set_difficulty":
            difficulty = float(message["params"][0])
            break
        if message.get("method") == "mining.notify":
            saw_notify_before_difficulty = True

if saw_notify_before_difficulty:
    raise SystemExit(f"port {port}: mining.notify arrived before any mining.set_difficulty")
if difficulty is None:
    raise SystemExit(f"port {port}: no mining.set_difficulty received")
if abs(difficulty - expected) > expected * 1e-9:
    raise SystemExit(f"port {port}: first difficulty {difficulty} != expected {expected}")
print(f"difficulty probe PASS port={port} password={password!r} difficulty={difficulty}")
PY
}

# The stamped job floor clamps the advertised share difficulty to the network
# difficulty (a share is never required to be harder than a block), while an
# explicit md= minimum and the high-diff wire floor still hold above it. On a
# fresh regtest chain the network difficulty sits below every configured
# value, so the first probe expects the network clamp.
network_difficulty="$(qbit_rpc getdifficulty)"
probe_first_difficulty "${STRATUM_PORT}" "${miner_usernames[0]}" "x" "${network_difficulty}"
probe_first_difficulty "${HIGHDIFF_PORT}" "${miner_usernames[0]}" "x" "${HIGHDIFF_START_DIFF}"
# Password d=/md= bound vardiff but do not lift the first advertised job
# above the network clamp; only the high-diff listener floor is a wire
# guarantee that holds above it.
probe_first_difficulty "${STRATUM_PORT}" "${miner_usernames[0]}" "d=0.5,md=0.25" "${network_difficulty}"

if [[ "${AUDIT_API_ENABLED}" == "1" ]]; then
  AUDIT_PORT="${AUDIT_PORT}" python3 <<'PY'
import os
import socket
import time

deadline = time.time() + 30
port = int(os.environ["AUDIT_PORT"])
while time.time() < deadline:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.5):
            raise SystemExit(0)
    except OSError:
        time.sleep(0.25)
raise SystemExit("timed out waiting for PRISM audit API port")
PY
  if ! kill -0 "${coordinator_pid}" >/dev/null 2>&1; then
    echo "PRISM coordinator exited before opening audit API port" >&2
    cat "${COORDINATOR_LOG}" >&2 || true
    exit 1
  fi
fi

for miner_index in $(seq 1 "${MINER_COUNT}"); do
  miner_username="${miner_usernames[$((miner_index - 1))]}"
  (
    cd "${ROOT_DIR}"
    STRATUM_HOST=127.0.0.1 \
    STRATUM_PORT="${STRATUM_PORT}" \
    MINER_USERNAME="${miner_username}" \
    MINER_WALLET_NAME="prism-live-miner-${miner_index}" \
    MINER_PASSWORD=x \
    MINER_TIMEOUT_SECONDS="${MINER_TIMEOUT_SECONDS}" \
    MINER_USERNAME_TIMEOUT_SECONDS=30 \
    QBIT_RPC_HOST=127.0.0.1 \
    QBIT_RPC_PORT="${RPC_PORT}" \
    QBIT_RPC_USER="${RPC_USER}" \
    QBIT_RPC_PASSWORD="${RPC_PASSWORD}" \
    python3 lab/miner-sim/miner_sim.py
  ) >"${DATADIR}/miner-${miner_index}.log" 2>&1 &
  miner_pids+=("$!")
done

deadline=$((SECONDS + 30))
while [[ ! -f "${EVIDENCE_PATH}" && "${SECONDS}" -lt "${deadline}" ]]; do
  accepted_count="$(grep -c 'prism coordinator: accepted share' "${COORDINATOR_LOG}" 2>/dev/null || true)"
  if [[ "${accepted_count}" -ge "${MINER_COUNT}" ]]; then
    miner_username="${miner_usernames[0]}"
    (
      cd "${ROOT_DIR}"
      STRATUM_HOST=127.0.0.1 \
      STRATUM_PORT="${STRATUM_PORT}" \
      MINER_USERNAME="${miner_username}" \
      MINER_WALLET_NAME="prism-live-miner-block" \
      MINER_PASSWORD=x \
      MINER_TIMEOUT_SECONDS="${MINER_TIMEOUT_SECONDS}" \
      MINER_USERNAME_TIMEOUT_SECONDS=30 \
      QBIT_RPC_HOST=127.0.0.1 \
      QBIT_RPC_PORT="${RPC_PORT}" \
      QBIT_RPC_USER="${RPC_USER}" \
      QBIT_RPC_PASSWORD="${RPC_PASSWORD}" \
      python3 lab/miner-sim/miner_sim.py
    ) >"${DATADIR}/miner-block.log" 2>&1 &
    miner_pids+=("$!")
    break
  fi
  if ! kill -0 "${coordinator_pid}" >/dev/null 2>&1; then
    echo "PRISM coordinator exited before accepting ready shares" >&2
    cat "${COORDINATOR_LOG}" >&2 || true
    exit 1
  fi
  sleep 1
done

deadline=$((SECONDS + MINER_TIMEOUT_SECONDS))
while [[ ! -f "${EVIDENCE_PATH}" && "${SECONDS}" -lt "${deadline}" ]]; do
  if ! kill -0 "${coordinator_pid}" >/dev/null 2>&1; then
    echo "PRISM coordinator exited before writing evidence" >&2
    cat "${COORDINATOR_LOG}" >&2 || true
    exit 1
  fi
  sleep 1
done

if [[ ! -f "${EVIDENCE_PATH}" ]]; then
  echo "timed out waiting for live PRISM evidence" >&2
  cat "${COORDINATOR_LOG}" >&2 || true
    for miner_index in $(seq 1 "${MINER_COUNT}"); do
      echo "--- miner ${miner_index} ---" >&2
      cat "${DATADIR}/miner-${miner_index}.log" >&2 || true
    done
    echo "--- miner block ---" >&2
    cat "${DATADIR}/miner-block.log" >&2 || true
    exit 1
fi

for pid in "${miner_pids[@]}"; do
  wait "${pid}" || {
    echo "a live PRISM miner exited unsuccessfully" >&2
    cat "${COORDINATOR_LOG}" >&2 || true
    for miner_index in $(seq 1 "${MINER_COUNT}"); do
      echo "--- miner ${miner_index} ---" >&2
      cat "${DATADIR}/miner-${miner_index}.log" >&2 || true
    done
    echo "--- miner block ---" >&2
    cat "${DATADIR}/miner-block.log" >&2 || true
    exit 1
  }
done

if [[ "${AUDIT_API_ENABLED}" == "1" ]]; then
  EVIDENCE_PATH="${EVIDENCE_PATH}" \
  DATADIR="${DATADIR}" \
  AUDIT_PORT="${AUDIT_PORT}" \
  POSTGRES_ENABLED="${POSTGRES_ENABLED}" \
  POSTGRES_CONTAINER="${POSTGRES_CONTAINER}" \
  PRISM_PSQL_COMMAND="${PSQL_COMMAND}" \
  python3 <<'PY'
import json
import os
import subprocess
import urllib.request
from pathlib import Path

base_url = f"http://127.0.0.1:{os.environ['AUDIT_PORT']}"
evidence_path = Path(os.environ["EVIDENCE_PATH"])
datadir = Path(os.environ["DATADIR"])
evidence = json.loads(evidence_path.read_text(encoding="utf-8"))

def local_audit_payload(path):
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if payload.get("schema") == "qbit.prism.live-audit-bundle-envelope.v1":
        return local_audit_payload(payload["body_uri"])
    if payload.get("schema") == "qbit.prism.audit-body-ref.v1":
        return payload["bundle_without_shares"]
    if payload.get("schema") == "qbit.prism.audit-bundle.v2":
        return payload["bundle_without_shares"]
    return payload

bundle = local_audit_payload(evidence["audit_bundle_path"])

def get_json(path):
    with urllib.request.urlopen(base_url + path, timeout=10) as response:
        return json.loads(response.read())

def get_text(path):
    with urllib.request.urlopen(base_url + path, timeout=10) as response:
        return response.read().decode()

latest = get_json("/audit/latest")
if latest["block_hash"] != evidence["block_hash"]:
    raise SystemExit("audit API latest evidence block hash mismatch")

owed = get_json("/owed-balances")
if owed["schema"] != "qbit.prism.owed-balances.v1":
    raise SystemExit("unexpected owed-balances schema")

metrics = get_text("/metrics")
for expected in (
    f"qbit_prism_accepted_shares_total {evidence['accepted_share_count']}",
    "qbit_prism_blocks_accepted_total 1",
):
    if expected not in metrics:
        raise SystemExit(f"missing metrics line: {expected}")

if os.environ["POSTGRES_ENABLED"] == "1":
    import shlex

    psql_base = shlex.split(os.environ["PRISM_PSQL_COMMAND"])

    def psql_json(sql):
        raw = subprocess.check_output(
            psql_base
            + [
                "--no-psqlrc",
                "--set",
                "ON_ERROR_STOP=1",
                "--tuples-only",
                "--no-align",
                "--quiet",
                "--command",
                sql,
            ],
            text=True,
        ).strip()
        return json.loads(raw.splitlines()[-1])

    counts = psql_json(
        """
        SELECT json_build_object(
          'shares', (SELECT count(*) FROM qbit_share_ledger WHERE accepted),
          'contiguous', (
            SELECT COALESCE(bool_and(share_seq = expected_seq), true)
            FROM (
              SELECT share_seq, row_number() OVER (ORDER BY share_seq) AS expected_seq
              FROM qbit_share_ledger
              WHERE accepted
            ) ordered
          ),
          'blocks', (SELECT count(*) FROM qbit_pool_blocks),
          'payout_entries', (SELECT count(*) FROM qbit_pool_payout_entries),
          'bundles', (SELECT count(*) FROM qbit_pool_audit_bundles),
          'writer_rows', (
            SELECT count(*)
            FROM qbit_share_ledger
            WHERE writer_id = 'prism-coordinator' AND writer_epoch = 1
          )
        );
        """
    )
    if counts["shares"] != evidence["accepted_share_count"]:
        raise SystemExit(f"persisted share count mismatch: {counts['shares']} != {evidence['accepted_share_count']}")
    if counts["contiguous"] is not True:
        raise SystemExit("persisted share_seq values are not contiguous")
    if counts["blocks"] != 1 or counts["bundles"] != 1:
        raise SystemExit(f"expected one persisted block and bundle, got {counts}")
    if counts["writer_rows"] != evidence["accepted_share_count"]:
        raise SystemExit("persisted shares do not all carry the expected writer identity")

    expected_payout_entries = len(bundle["payout_policy_manifest"]["accounts"])
    if counts["payout_entries"] != expected_payout_entries:
        raise SystemExit(f"payout row count mismatch: {counts['payout_entries']} != {expected_payout_entries}")

    found = bundle["found_block"]
    sql_window = psql_json(
        f"""
        SELECT COALESCE(json_agg(json_build_object(
          'share_seq', share_seq,
          'share_id', share_id,
          'miner_id', miner_id,
          'order_key', payout_order_key,
          'p2mr_program_hex', encode(p2mr_program, 'hex'),
          'share_difficulty', share_difficulty::text,
          'counted_difficulty', counted_difficulty::text
        ) ORDER BY share_seq DESC), '[]'::json)
        FROM qbit_audit_share_window(
          to_timestamp({int(found['anchor_job_issued_at_ms'])}::double precision / 1000.0),
          {int(found['network_difficulty'])}::numeric
        );
        """
    )
    expected_window = [
        {
            "share_seq": share["share_seq"],
            "share_id": share["share_id"],
            "miner_id": share["miner_id"],
            "order_key": share["order_key"],
            "p2mr_program_hex": share["p2mr_program_hex"],
            "share_difficulty": str(share["share_difficulty"]),
            "counted_difficulty": str(share["counted_difficulty"]),
        }
        for share in bundle["reward_manifest"]["shares"]
    ]
    if sql_window != expected_window:
        raise SystemExit("persisted audit share window does not match submitted bundle")

    api_window = get_json(
        "/audit/share-window"
        f"?anchor_job_issued_at_ms={int(found['anchor_job_issued_at_ms'])}"
        f"&network_difficulty={int(found['network_difficulty'])}"
    )
    if [row["share_id"] for row in api_window["rows"]] != [row["share_id"] for row in bundle["reward_manifest"]["shares"]]:
        raise SystemExit("audit API share-window rows do not match submitted bundle")
    expected_api_window = [
        {
            "share_seq": share["share_seq"],
            "share_id": share["share_id"],
            "miner_id": share["miner_id"],
            "order_key": share["order_key"],
            "p2mr_program_hex": share["p2mr_program_hex"],
            "share_difficulty": share["share_difficulty"],
            "counted_difficulty": share["counted_difficulty"],
        }
        for share in bundle["reward_manifest"]["shares"]
    ]
    actual_api_window = [
        {
            "share_seq": row["share_seq"],
            "share_id": row["share_id"],
            "miner_id": row["miner_id"],
            "order_key": row["order_key"],
            "p2mr_program_hex": row["p2mr_program_hex"],
            "share_difficulty": row["share_difficulty"],
            "counted_difficulty": row["counted_difficulty"],
        }
        for row in api_window["rows"]
    ]
    if actual_api_window != expected_api_window:
        raise SystemExit("audit API share-window row contents do not match submitted bundle")

    payouts = get_json(f"/audit/blocks/{evidence['block_hash']}/payouts")
    if len(payouts["rows"]) != expected_payout_entries:
        raise SystemExit("audit API payout row count does not match submitted bundle")
    expected_payouts = [
        {
            "miner_id": account["recipient_id"],
            "order_key": account["order_key"],
            "p2mr_program_hex": account["p2mr_program_hex"],
            "onchain_amount_sats": account["onchain_amount_sats"],
            "carry_forward_balance_sats": account["carry_forward_balance_sats"],
            "action": account["action"],
            "maturity_state": "immature",
        }
        for account in bundle["payout_policy_manifest"]["accounts"]
    ]
    actual_payouts = [
        {
            "miner_id": row["miner_id"],
            "order_key": row["order_key"],
            "p2mr_program_hex": row["p2mr_program_hex"],
            "onchain_amount_sats": row["onchain_amount_sats"],
            "carry_forward_balance_sats": row["carry_forward_balance_sats"],
            "action": row["action"],
            "maturity_state": row["maturity_state"],
        }
        for row in payouts["rows"]
    ]
    if actual_payouts != expected_payouts:
        raise SystemExit("audit API payout rows do not match submitted payout manifest")

    api_bundle = get_json(f"/audit/blocks/{evidence['block_hash']}/bundle")
    if api_bundle["audit_bundle_sha256"] != evidence["audit_report"]["audit_bundle_sha256_hex"]:
        raise SystemExit("audit API bundle digest does not match verifier report")
    api_bundle_path = datadir / "prism-api-audit-bundle.json"
    api_bundle_path.write_text(json.dumps(api_bundle["audit_bundle"], indent=2), encoding="utf-8")
PY

  if [[ "${POSTGRES_ENABLED}" == "1" ]]; then
    run_cargo run --quiet -p qbit-prism --bin qbit-prism-audit-verify -- \
      "${DATADIR}/prism-api-audit-bundle.json" \
      --coinbase-tx-hex "$(EVIDENCE_PATH="${EVIDENCE_PATH}" python3 -c 'import json,os; print(json.load(open(os.environ["EVIDENCE_PATH"], encoding="utf-8"))["coinbase_tx_hex"])')" \
      --ledger-writer-public-key-hex "$(DATADIR="${DATADIR}" python3 -c 'import json,os; print(json.load(open(os.path.join(os.environ["DATADIR"], "prism-api-audit-bundle.json"), encoding="utf-8"))["ledger_window_attestation"]["signature"]["public_key_hex"])')" \
      --expected-coinbase-value-sats "$(DATADIR="${DATADIR}" python3 -c 'import json,os; print(json.load(open(os.path.join(os.environ["DATADIR"], "prism-api-audit-bundle.json"), encoding="utf-8"))["found_block"]["coinbase_value_sats"])')" >/dev/null
  fi

  kill "${coordinator_pid}" >/dev/null 2>&1 || true
  wait "${coordinator_pid}" >/dev/null 2>&1 || true
  coordinator_pid=""
else
  wait "${coordinator_pid}" || {
    echo "PRISM coordinator exited unsuccessfully" >&2
    cat "${COORDINATOR_LOG}" >&2 || true
    exit 1
  }
  coordinator_pid=""
fi

after_height="$(qbit_rpc getblockcount)"
# With the advertised difficulty clamped to the network difficulty, every
# accepted share is also a block, so a share already in flight when the
# coordinator begins its stop-after-block shutdown can land one extra block.
# Require at least the target; never fewer.
if [[ "${after_height}" -lt $((before_height + 1)) ]]; then
  echo "expected live PRISM blockcount >= ${before_height}+1, got ${after_height}" >&2
  exit 1
fi

EVIDENCE_PATH="${EVIDENCE_PATH}" MINER_COUNT="${MINER_COUNT}" python3 <<'PY'
import json
import os

evidence = json.load(open(os.environ["EVIDENCE_PATH"], encoding="utf-8"))
miner_count = int(os.environ["MINER_COUNT"])
if evidence["schema"] != "qbit.prism.live-stratum-evidence.v1":
    raise SystemExit("unexpected live evidence schema")
if evidence["distinct_miner_count"] < miner_count:
    raise SystemExit(f"expected >= {miner_count} distinct miners, got {evidence['distinct_miner_count']}")
if evidence["accepted_share_count"] < miner_count:
    raise SystemExit("live ledger did not record enough accepted shares")
if evidence["job_share_count"] < miner_count:
    raise SystemExit("submitted block did not use a 3+ miner PRISM share snapshot")
if evidence["audit_report"]["schema"] != "qbit.prism.audit-verification-report.v1":
    raise SystemExit("unexpected audit report schema")
if evidence["audit_report"]["onchain_output_count"] < 1:
    raise SystemExit("live audit report has no on-chain outputs")
PY

block_hash="$(qbit_rpc getblockhash "${after_height}")"
raw_block="$(qbit_rpc getblock "${block_hash}" 0)"
RAW_BLOCK="${raw_block}" \
EVIDENCE_PATH="${EVIDENCE_PATH}" \
QBIT_SRC_DIR="${QBIT_SRC_DIR}" \
python3 <<'PY'
import io
import json
import os
import sys
from pathlib import Path

qbit_src = Path(os.environ["QBIT_SRC_DIR"])
sys.path.insert(0, str(qbit_src / "test/functional"))

from test_framework.messages import CBlock

evidence = json.load(open(os.environ["EVIDENCE_PATH"], encoding="utf-8"))
block = CBlock()
block.deserialize(io.BytesIO(bytes.fromhex(os.environ["RAW_BLOCK"])))
coinbase_hex = block.vtx[0].serialize_with_witness().hex()
if coinbase_hex != evidence["coinbase_tx_hex"]:
    raise SystemExit("on-chain live coinbase bytes do not match coordinator evidence")
PY

printf 'prism live stratum regtest PASS height=%s block=%s evidence=%s\n' \
  "${after_height}" "${block_hash}" "${EVIDENCE_PATH}"
