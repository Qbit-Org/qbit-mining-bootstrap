#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/test/test-lib.sh"

DATADIR="${DATADIR:-$(mktemp -d -t qbit-prism-combined.XXXXXX)}"
RPC_USER="${RPC_USER:-qbitrpc}"
RPC_PASSWORD="${RPC_PASSWORD:-change-this}"
RPC_PORT="${RPC_PORT:-18456}"
WALLET_NAME="${WALLET_NAME:-prism-combined}"
STRATUM_PORT="${PRISM_STRATUM_PORT:-3342}"
AUDIT_PORT="${PRISM_AUDIT_PORT:-3343}"
MINER_COUNT=6
MINER_TIMEOUT_SECONDS="${QBIT_PRISM_LIVE_MINER_TIMEOUT_SECONDS:-120}"
EVIDENCE_PATH="${DATADIR}/prism-live-evidence.json"
COORDINATOR_LOG="${DATADIR}/prism-coordinator.log"
POSTGRES_IMAGE="${QBIT_PRISM_POSTGRES_IMAGE:-postgres:16-alpine}"
POSTGRES_CONTAINER="${QBIT_PRISM_POSTGRES_CONTAINER:-qbit-prism-combined-pg-$$}"
MANIFEST_SIGNING_SEED_HEX="${QBIT_PRISM_MANIFEST_SIGNING_SEED_HEX:-4242424242424242424242424242424242424242424242424242424242424242}"
LEDGER_ATTESTATION_SIGNING_SEED_HEX="${QBIT_PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX:-4343434343434343434343434343434343434343434343434343434343434343}"
LEDGER_WRITER_SESSION_TOKEN="${QBIT_PRISM_LEDGER_WRITER_SESSION_TOKEN:-prism-combined-regtest-session}"
ASERT_MOCKTIME_BASE="${QBIT_PRISM_LIVE_MOCKTIME_BASE:-1738713603}"
ASERT_TIME_STEP="${QBIT_PRISM_LIVE_ASERT_TIME_STEP:-1}"

resolve_qbit_binaries
read -r -a CARGO_CMD <<< "${CARGO:-cargo}"
require_executable "${CARGO_CMD[0]}"
# When QBIT_PRISM_EXTERNAL_PSQL_COMMAND is set, use an already-running Postgres
# (e.g. a local cluster) instead of provisioning a Docker container.
EXTERNAL_PSQL="${QBIT_PRISM_EXTERNAL_PSQL_COMMAND:-}"
[[ -n "${EXTERNAL_PSQL}" ]] || require_executable docker
require_executable python3
require_qbit_src_helper "test/functional/test_framework/blocktools.py" "the combined PRISM regtest proof"

coordinator_pid=""
postgres_started=0
miner_pids=()

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
qbit_rpc setmocktime "${ASERT_MOCKTIME_BASE}" >/dev/null

miner_usernames=()
for miner_index in $(seq 1 "${MINER_COUNT}"); do
  miner_usernames+=("$(qbit_rpc -rpcwallet="${WALLET_NAME}" getnewaddress "" p2mr)")
done

initial_template_json="$(qbit_rpc getblocktemplate '{"rules":["segwit"]}')"
share_weights_json="$(
  TEMPLATE_JSON="${initial_template_json}" \
  MINERS_JSON="$(printf '%s\n' "${miner_usernames[@]}" | python3 -c 'import json,sys; print(json.dumps([line.strip() for line in sys.stdin if line.strip()]))')" \
  python3 <<'PY'
import json
import os

template = json.loads(os.environ["TEMPLATE_JSON"])
miners = json.loads(os.environ["MINERS_JSON"])

def target_from_compact(bits_hex: str) -> int:
    compact = int(bits_hex, 16)
    size = compact >> 24
    mantissa = compact & 0x007fffff
    if size <= 3:
        return mantissa >> (8 * (3 - size))
    return mantissa << (8 * (size - 3))

pow_limit_target = target_from_compact("207fffff")
template_target = target_from_compact(template["bits"])
network_difficulty = max(1, (pow_limit_target * 1_000_000) // template_target)
weights = [
    network_difficulty * 5,
    network_difficulty,
    max(1, network_difficulty // 4),
    10,
    1,
    1,
]
print(json.dumps(dict(zip(miners, weights)), separators=(",", ":")))
PY
)"

(
  cd "${ROOT_DIR}"
  export QBIT_RPC_HOST=127.0.0.1
  export QBIT_RPC_PORT="${RPC_PORT}"
  export QBIT_RPC_USER="${RPC_USER}"
  export QBIT_RPC_PASSWORD="${RPC_PASSWORD}"
  export PRISM_STRATUM_BIND=127.0.0.1
  export PRISM_STRATUM_PORT="${STRATUM_PORT}"
  export PRISM_AUDIT_BIND=127.0.0.1
  export PRISM_AUDIT_PORT="${AUDIT_PORT}"
  export PRISM_MIN_READY_MINERS="${MINER_COUNT}"
  export PRISM_EVIDENCE_PATH="${EVIDENCE_PATH}"
  export PRISM_AUDIT_DIR="${DATADIR}"
  export PRISM_STRATUM_SHARE_DIFF=0.000000001
  export PRISM_STRATUM_SHARE_WEIGHTS_JSON="${share_weights_json}"
  export PRISM_STOP_AFTER_BLOCK=0
  export PRISM_MAX_BLOCKS=99
  export PRISM_POSTGRES_PSQL_COMMAND="${PSQL_COMMAND}"
  export PRISM_POSTGRES_INIT_SCHEMA=1
  export PRISM_LEDGER_WRITER_SESSION_TOKEN="${LEDGER_WRITER_SESSION_TOKEN}"
  export PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN=1
  export PRISM_MANIFEST_SIGNING_SEED_HEX="${MANIFEST_SIGNING_SEED_HEX}"
  export PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX="${LEDGER_ATTESTATION_SIGNING_SEED_HEX}"
  export PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY=1
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

before_height="$(qbit_rpc getblockcount)"
for miner_index in $(seq 1 "${MINER_COUNT}"); do
  miner_username="${miner_usernames[$((miner_index - 1))]}"
  (
    cd "${ROOT_DIR}"
    STRATUM_HOST=127.0.0.1 \
    STRATUM_PORT="${STRATUM_PORT}" \
    MINER_USERNAME="${miner_username}" \
    MINER_WALLET_NAME="prism-combined-miner-${miner_index}" \
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
    exit 1
  }
done
miner_pids=()

after_height="$(qbit_rpc getblockcount)"
if [[ "${after_height}" -ne $((before_height + 1)) ]]; then
  echo "expected first live PRISM block at ${before_height}+1, got ${after_height}" >&2
  exit 1
fi
live_hash="$(qbit_rpc getblockhash "${after_height}")"

EVIDENCE_PATH="${EVIDENCE_PATH}" MINER_COUNT="${MINER_COUNT}" python3 <<'PY'
import json
import os
from pathlib import Path

evidence = json.loads(Path(os.environ["EVIDENCE_PATH"]).read_text(encoding="utf-8"))

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
if evidence["ledger_backend"] != "postgres-psql":
    raise SystemExit("combined proof did not use the Postgres ledger")
if evidence["distinct_miner_count"] < int(os.environ["MINER_COUNT"]):
    raise SystemExit("combined proof did not include all skewed miners")
if evidence["job_share_count"] < int(os.environ["MINER_COUNT"]):
    raise SystemExit("live block did not use the 6-miner PRISM snapshot")
if evidence["audit_report"]["accrued_account_count"] < 1:
    raise SystemExit("live skewed payout did not create an accrued account")
if not any(account["action"] == "accrued" for account in bundle["payout_policy_manifest"]["accounts"]):
    raise SystemExit("live payout policy manifest has no accrued account")
PY

build_submit_persist_direct_block() {
  local phase="$1"
  local suffix_hex="$2"
  local mocktime="$3"
  qbit_rpc setmocktime "${mocktime}" >/dev/null
  (
    cd "${ROOT_DIR}"
    PHASE="${phase}" \
    SUFFIX_HEX="${suffix_hex}" \
    DATADIR="${DATADIR}" \
    QBIT_RPC_HOST=127.0.0.1 \
    QBIT_RPC_PORT="${RPC_PORT}" \
	    QBIT_RPC_USER="${RPC_USER}" \
	    QBIT_RPC_PASSWORD="${RPC_PASSWORD}" \
	    QBIT_SRC_DIR="${QBIT_SRC_DIR}" \
	    PRISM_PSQL_COMMAND="${PSQL_COMMAND}" \
	    LEDGER_WRITER_SESSION_TOKEN="${LEDGER_WRITER_SESSION_TOKEN}" \
	    MANIFEST_SIGNING_SEED_HEX="${MANIFEST_SIGNING_SEED_HEX}" \
	    LEDGER_ATTESTATION_SIGNING_SEED_HEX="${LEDGER_ATTESTATION_SIGNING_SEED_HEX}" \
	    python3 <<'PY'
from __future__ import annotations

import base64
import io
import json
import os
import shlex
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(os.environ["QBIT_SRC_DIR"]) / "test/functional"))
from test_framework.blocktools import create_block
from test_framework.messages import CBlock, CTransaction

from lab.prism.share_ledger import PsqlShareLedger


def rpc(method: str, params: list[object] | None = None) -> object:
    body = json.dumps({"jsonrpc": "1.0", "id": method, "method": method, "params": params or []}).encode()
    credentials = f"{os.environ['QBIT_RPC_USER']}:{os.environ['QBIT_RPC_PASSWORD']}".encode()
    request = urllib.request.Request(
        f"http://{os.environ['QBIT_RPC_HOST']}:{os.environ['QBIT_RPC_PORT']}",
        data=body,
        headers={
            "Authorization": "Basic " + base64.b64encode(credentials).decode(),
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        payload = json.loads(response.read())
    if payload["error"] is not None:
        raise RuntimeError(f"qbit RPC {method} failed: {payload['error']}")
    return payload["result"]


def target_from_compact(bits_hex: str) -> int:
    compact = int(bits_hex, 16)
    size = compact >> 24
    mantissa = compact & 0x007fffff
    if size <= 3:
        return mantissa >> (8 * (3 - size))
    return mantissa << (8 * (size - 3))


phase = os.environ["PHASE"]
suffix_hex = os.environ["SUFFIX_HEX"]
datadir = Path(os.environ["DATADIR"])
ledger = PsqlShareLedger(
    psql_command=os.environ["PRISM_PSQL_COMMAND"],
    writer_id="prism-coordinator",
    writer_epoch=1,
    writer_session_token=os.environ["LEDGER_WRITER_SESSION_TOKEN"],
)
template = rpc("getblocktemplate", [{"rules": ["segwit"]}])
if template.get("transactions"):
    raise SystemExit("combined proof expects an empty regtest mempool")
anchor_ms = int(time.time() * 1000)
pow_limit_target = target_from_compact("207fffff")
template_target = target_from_compact(template["bits"])
network_difficulty = max(1, (pow_limit_target * 1_000_000) // template_target)
shares = [record.to_prism_json() for record in ledger.snapshot_at_job_issue(anchor_ms)]
prior_balances = ledger.current_prior_balances()
payload = {
    "shares": shares,
    "found_block": {
        "block_height": int(template["height"]),
        "coinbase_value_sats": int(template["coinbasevalue"]),
        "network_difficulty": network_difficulty,
        "anchor_job_issued_at_ms": anchor_ms,
    },
    "prior_balances": prior_balances,
    "coinbase_script_sig_suffix_hex": suffix_hex,
}
cargo = shlex.split(os.environ.get("PRISM_CARGO", os.environ.get("CARGO", "cargo")))
bundle_json = subprocess.check_output(
    cargo
    + [
        "run",
        "--quiet",
        "-p",
        "qbit-prism",
        "--bin",
        "qbit-prism-build-audit-bundle",
        "--",
        "--input",
        "-",
        "--signing-key-seed-hex",
        os.environ["MANIFEST_SIGNING_SEED_HEX"],
        "--ledger-signing-key-seed-hex",
        os.environ["LEDGER_ATTESTATION_SIGNING_SEED_HEX"],
    ],
    input=json.dumps(payload),
    text=True,
)
bundle = json.loads(bundle_json)
bundle_path = datadir / f"prism-combined-{phase}-bundle.json"
bundle_path.write_text(json.dumps(bundle, indent=2), encoding="utf-8")

manifest = bundle["signed_coinbase_manifest"]["manifest"]
coinbase = CTransaction()
coinbase.deserialize(io.BytesIO(bytes.fromhex(manifest["coinbase_tx_hex"])))
block = create_block(
    hashprev=int(template["previousblockhash"], 16),
    coinbase=coinbase,
    ntime=int(template["curtime"]),
    version=int(template["version"]),
    tmpl=template,
    txlist=[tx["data"] for tx in template.get("transactions", [])],
)
block.solve()
before_height = int(rpc("getblockcount"))
result = rpc("submitblock", [block.serialize().hex()])
after_height = int(rpc("getblockcount"))
if result not in (None, "duplicate"):
    raise SystemExit(f"submitblock rejected {phase}: {result}")
if after_height != before_height + 1:
    raise SystemExit(f"submitblock did not advance height for {phase}: {before_height}->{after_height}")
block_hash = str(rpc("getblockhash", [after_height]))
raw_block = str(rpc("getblock", [block_hash, 0]))
onchain = CBlock()
onchain.deserialize(io.BytesIO(bytes.fromhex(raw_block)))
coinbase_hex = onchain.vtx[0].serialize_with_witness().hex()
if coinbase_hex != manifest["coinbase_tx_hex"]:
    raise SystemExit(f"on-chain coinbase mismatch for {phase}")
report_json = subprocess.check_output(
    cargo
    + [
        "run",
        "--quiet",
        "-p",
        "qbit-prism",
        "--bin",
        "qbit-prism-audit-verify",
        "--",
        str(bundle_path),
        "--coinbase-tx-hex",
        coinbase_hex,
        "--ledger-writer-public-key-hex",
        bundle["ledger_window_attestation"]["signature"]["public_key_hex"],
        "--expected-coinbase-value-sats",
        str(bundle["found_block"]["coinbase_value_sats"]),
    ],
    text=True,
)
report = json.loads(report_json)
ledger.persist_accepted_block(
    block_hash=block_hash,
    block_height=after_height,
    parent_hash=str(template["previousblockhash"]),
    final_bundle=bundle,
    audit_report=report,
)
print(json.dumps({
    "phase": phase,
    "block_hash": block_hash,
    "block_height": after_height,
    "bits": template["bits"],
    "bundle_path": str(bundle_path),
    "audit_report": report,
    "prior_balance_count": len(prior_balances),
}, separators=(",", ":")))
PY
  )
}

second_json="$(build_submit_persist_direct_block second 222222223333333333333333 "$((ASERT_MOCKTIME_BASE + ASERT_TIME_STEP))")"
second_hash="$(SECOND_JSON="${second_json}" python3 -c 'import json,os; print(json.loads(os.environ["SECOND_JSON"])["block_hash"])')"
second_bits="$(SECOND_JSON="${second_json}" python3 -c 'import json,os; print(json.loads(os.environ["SECOND_JSON"])["bits"])')"
first_bits="$(TEMPLATE_JSON="${initial_template_json}" python3 -c 'import json,os; print(json.loads(os.environ["TEMPLATE_JSON"])["bits"])')"
if [[ "${first_bits}" == "${second_bits}" ]]; then
  echo "expected ASERT compact target to change; saw ${first_bits},${second_bits}" >&2
  exit 1
fi

AUDIT_PORT="${AUDIT_PORT}" SECOND_HASH="${second_hash}" python3 <<'PY'
import json
import os
import urllib.request

base = f"http://127.0.0.1:{os.environ['AUDIT_PORT']}"
with urllib.request.urlopen(base + "/owed-balances", timeout=10) as response:
    owed = json.loads(response.read())
if not owed["balances"]:
    raise SystemExit("expected positive owed balances after skewed persisted blocks")
with urllib.request.urlopen(base + f"/audit/blocks/{os.environ['SECOND_HASH']}/bundle", timeout=10) as response:
    bundle = json.loads(response.read())
if bundle["block_hash"] != os.environ["SECOND_HASH"]:
    raise SystemExit("second persisted bundle API mismatch")
PY

qbit_rpc invalidateblock "${second_hash}" >/dev/null
reorg_height="$(qbit_rpc getblockcount)"
if [[ "${reorg_height}" -ne 1 ]]; then
  echo "expected height 1 after invalidating second block, got ${reorg_height}" >&2
  exit 1
fi

PRISM_PSQL_COMMAND="${PSQL_COMMAND}" LEDGER_WRITER_SESSION_TOKEN="${LEDGER_WRITER_SESSION_TOKEN}" SECOND_HASH="${second_hash}" REORG_HEIGHT="${reorg_height}" python3 <<'PY'
import os
from lab.prism.share_ledger import PsqlShareLedger

ledger = PsqlShareLedger(
    psql_command=os.environ["PRISM_PSQL_COMMAND"],
    writer_id="prism-coordinator",
    writer_epoch=1,
    writer_session_token=os.environ["LEDGER_WRITER_SESSION_TOKEN"],
)
result = ledger.reverse_immature_block(
    block_hash=os.environ["SECOND_HASH"],
    active_tip_height=int(os.environ["REORG_HEIGHT"]),
)
if result["reversed_count"] <= 0:
    raise SystemExit("reverse_immature_pool_block did not reverse persisted rows")
PY

AUDIT_PORT="${AUDIT_PORT}" SECOND_HASH="${second_hash}" python3 <<'PY'
import json
import os
import urllib.request

base = f"http://127.0.0.1:{os.environ['AUDIT_PORT']}"
with urllib.request.urlopen(base + f"/audit/blocks/{os.environ['SECOND_HASH']}/payouts", timeout=10) as response:
    payouts = json.loads(response.read())
if not payouts["rows"] or {row["maturity_state"] for row in payouts["rows"]} != {"reversed"}:
    raise SystemExit("disconnected block payout rows were not all reversed")
with urllib.request.urlopen(base + "/owed-balances", timeout=10) as response:
    owed = json.loads(response.read())
if not owed["balances"]:
    raise SystemExit("expected first-block owed balances to remain after second-block reversal")
PY

replacement_json="$(build_submit_persist_direct_block replacement 222222224444444444444444 "$((ASERT_MOCKTIME_BASE + ASERT_TIME_STEP))")"
replacement_hash="$(REPLACEMENT_JSON="${replacement_json}" python3 -c 'import json,os; print(json.loads(os.environ["REPLACEMENT_JSON"])["block_hash"])')"
if [[ "${replacement_hash}" == "${second_hash}" ]]; then
  echo "replacement block unexpectedly matched disconnected block ${second_hash}" >&2
  exit 1
fi
active_tip="$(qbit_rpc getbestblockhash)"
if [[ "${active_tip}" != "${replacement_hash}" ]]; then
  echo "expected replacement ${replacement_hash} to be active tip, got ${active_tip}" >&2
  exit 1
fi

AUDIT_PORT="${AUDIT_PORT}" REPLACEMENT_HASH="${replacement_hash}" python3 <<'PY'
import json
import os
import urllib.request

base = f"http://127.0.0.1:{os.environ['AUDIT_PORT']}"
with urllib.request.urlopen(base + f"/audit/blocks/{os.environ['REPLACEMENT_HASH']}/bundle", timeout=10) as response:
    bundle = json.loads(response.read())
if bundle["block_hash"] != os.environ["REPLACEMENT_HASH"]:
    raise SystemExit("replacement persisted bundle API mismatch")
PY

kill "${coordinator_pid}" >/dev/null 2>&1 || true
wait "${coordinator_pid}" >/dev/null 2>&1 || true
coordinator_pid=""

printf 'prism combined regtest PASS live=%s second=%s replacement=%s bits=%s,%s\n' \
  "${live_hash}" "${second_hash}" "${replacement_hash}" "${first_bits}" "${second_bits}"
