#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/test/test-lib.sh"
DATADIR="${DATADIR:-$(mktemp -d -t qbit-builder-regtest.XXXXXX)}"
RPC_USER="${RPC_USER:-qbitrpc}"
RPC_PASSWORD="${RPC_PASSWORD:-change-this}"
RPC_PORT="${RPC_PORT:-18453}"
WALLET_NAME="${WALLET_NAME:-builder-regtest}"
BUILDER_COUNTS="${QBIT_BUILDER_REGTEST_COUNTS:-1,50,500}"
MANIFEST_SIGNING_SEED_HEX="${QBIT_BUILDER_MANIFEST_SIGNING_SEED_HEX:-000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f}"

resolve_qbit_binaries
read -r -a CARGO_CMD <<< "${CARGO:-cargo}"
require_executable "${CARGO_CMD[0]}"
require_executable python3
require_qbit_src_helper "test/functional/test_framework/blocktools.py" "the Rust builder regtest smoke"
run_cargo() {
  "${CARGO_CMD[@]}" "$@"
}

cleanup() {
  stop_qbitd
  remove_datadir "${DATADIR}"
}
trap cleanup EXIT

if [[ "$(ulimit -n)" == "unlimited" ]]; then
  ulimit -n 1024 || true
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

IFS=',' read -r -a COUNTS <<<"${BUILDER_COUNTS}"
for raw_count in "${COUNTS[@]}"; do
  output_count="$(echo "${raw_count}" | xargs)"
  if [[ -z "${output_count}" ]]; then
    continue
  fi

  before_height="$(qbit_rpc getblockcount)"
  template_json="$(qbit_rpc getblocktemplate '{"rules":["segwit"]}')"
  request_json="$(
    TEMPLATE_JSON="${template_json}" \
    OUTPUT_COUNT="${output_count}" \
    WALLET_NAME="${WALLET_NAME}" \
    QBIT_CLI_BIN="${QBIT_CLI_BIN}" \
    DATADIR="${DATADIR}" \
    RPC_USER="${RPC_USER}" \
    RPC_PASSWORD="${RPC_PASSWORD}" \
    RPC_PORT="${RPC_PORT}" \
    python3 <<'PY'
import hashlib
import json
import os
import subprocess

template = json.loads(os.environ["TEMPLATE_JSON"])
count = int(os.environ["OUTPUT_COUNT"])
if template.get("transactions"):
    raise SystemExit("builder regtest smoke expects an empty regtest mempool")

def qbit_rpc(*args: str) -> str:
    command = [
        os.environ["QBIT_CLI_BIN"],
        "-regtest",
        f"-datadir={os.environ['DATADIR']}",
        f"-rpcuser={os.environ['RPC_USER']}",
        f"-rpcpassword={os.environ['RPC_PASSWORD']}",
        f"-rpcport={os.environ['RPC_PORT']}",
        *args,
    ]
    return subprocess.check_output(command, text=True).strip()

entitlements = []
seen = set()
for _ in range(count):
    address = qbit_rpc(f"-rpcwallet={os.environ['WALLET_NAME']}", "getnewaddress", "", "p2mr")
    if address in seen:
        raise SystemExit(f"duplicate generated payout address: {address}")
    seen.add(address)
    info = json.loads(qbit_rpc("getaddressinfo", address))
    script = info["scriptPubKey"]
    if not script.startswith("5220") or len(script) != 68:
        raise SystemExit(f"non-P2MR script for {address}: {script}")
    entitlements.append({
        "recipient_id": address,
        "order_key": hashlib.sha256(address.encode("ascii")).hexdigest(),
        "p2mr_program_hex": script[4:],
        "weight": 1,
    })

print(json.dumps({
    "block_height": template["height"],
    "coinbase_value_sats": template["coinbasevalue"],
    "entitlements": entitlements,
}, separators=(",", ":")))
PY
  )"

  manifest_json="$(
    run_cargo run --quiet -p qbit-pool-builder -- \
      --signing-key-seed-hex "${MANIFEST_SIGNING_SEED_HEX}" <<<"${request_json}"
  )"
  block_hex="$(
    TEMPLATE_JSON="${template_json}" \
    MANIFEST_JSON="${manifest_json}" \
    QBIT_SRC_DIR="${QBIT_SRC_DIR}" \
    python3 <<'PY'
import io
import json
import os
import sys
from pathlib import Path

qbit_src = Path(os.environ["QBIT_SRC_DIR"])
sys.path.insert(0, str(qbit_src / "test/functional"))

from test_framework.blocktools import create_block
from test_framework.messages import CTransaction

template = json.loads(os.environ["TEMPLATE_JSON"])
manifest = json.loads(os.environ["MANIFEST_JSON"])["manifest"]
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
print(block.serialize().hex())
PY
  )"

  result="$(qbit_rpc submitblock "${block_hex}")"
  after_height="$(qbit_rpc getblockcount)"
  if [[ -n "${result}" && "${result}" != "null" ]]; then
    echo "submitblock returned failure for N=${output_count}: ${result}" >&2
    exit 1
  fi
  if [[ "${after_height}" -ne $((before_height + 1)) ]]; then
    echo "expected blockcount ${before_height}+1 after submitblock, got ${after_height}" >&2
    exit 1
  fi

  block_hash="$(qbit_rpc getblockhash "${after_height}")"
  raw_block="$(qbit_rpc getblock "${block_hash}" 0)"
  RAW_BLOCK="${raw_block}" \
  MANIFEST_JSON="${manifest_json}" \
  OUTPUT_COUNT="${output_count}" \
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

block = CBlock()
block.deserialize(io.BytesIO(bytes.fromhex(os.environ["RAW_BLOCK"])))
manifest = json.loads(os.environ["MANIFEST_JSON"])["manifest"]
expected_count = int(os.environ["OUTPUT_COUNT"])
coinbase = block.vtx[0]
if coinbase.serialize_with_witness().hex() != manifest["coinbase_tx_hex"]:
    raise SystemExit("on-chain coinbase bytes do not match manifest coinbase_tx_hex")
if len(coinbase.vout) != expected_count + 1:
    raise SystemExit(f"expected {expected_count} payouts plus witness commitment, got {len(coinbase.vout)}")

actual = [
    (txout.nValue, bytes(txout.scriptPubKey).hex())
    for txout in coinbase.vout[:expected_count]
]
expected = [
    (output["amount_sats"], output["script_pubkey_hex"])
    for output in manifest["outputs"]
]
if actual != expected:
    raise SystemExit("on-chain payout outputs do not match manifest")
if sum(value for value, _script in actual) != manifest["coinbase_value_sats"]:
    raise SystemExit("on-chain payout sum does not equal coinbase value")
commitment = coinbase.vout[-1]
if commitment.nValue != 0 or bytes(commitment.scriptPubKey).hex() != manifest["witness_commitment_script_hex"]:
    raise SystemExit("on-chain witness commitment does not match manifest")
PY

  printf 'builder regtest PASS N=%s height=%s block=%s\n' "${output_count}" "${after_height}" "${block_hash}"
done
