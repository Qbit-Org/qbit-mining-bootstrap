#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/test/test-lib.sh"
DATADIR="${DATADIR:-$(mktemp -d -t qbit-guide-permissionless.XXXXXX)}"
RPC_USER="${RPC_USER:-qbitrpc}"
RPC_PASSWORD="${RPC_PASSWORD:-change-this}"
RPC_PORT="${RPC_PORT:-18452}"
WALLET_NAME="${WALLET_NAME:-miner}"

resolve_qbit_binaries
require_executable python3
require_qbit_src_helper "test/functional/test_framework/blocktools.py" "the permissionless smoke test"

cleanup() {
  stop_qbitd
  remove_datadir "${DATADIR}"
}
trap cleanup EXIT

"${QBITD_BIN}" \
  -regtest \
  -asert \
  -daemonwait \
  -server \
  -listen=0 \
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
PAYOUT_ADDRESS="$(qbit_rpc -rpcwallet="${WALLET_NAME}" getnewaddress)"
PAYOUT_SCRIPT="$(qbit_rpc getaddressinfo "${PAYOUT_ADDRESS}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["scriptPubKey"])')"
TEMPLATE_JSON="$(qbit_rpc getblocktemplate '{"rules":["segwit"]}')"

BLOCK_HEX="$(
  TEMPLATE_JSON="${TEMPLATE_JSON}" PAYOUT_SCRIPT="${PAYOUT_SCRIPT}" QBIT_SRC_DIR="${QBIT_SRC_DIR}" python3 <<'PY'
import json
import os
import sys
from pathlib import Path

qbit_src = Path(os.environ["QBIT_SRC_DIR"])
sys.path.insert(0, str(qbit_src / "test/functional"))

from test_framework.blocktools import add_witness_commitment, create_block, create_coinbase
from test_framework.script import CScript

tmpl = json.loads(os.environ["TEMPLATE_JSON"])
coinbase = create_coinbase(tmpl["height"], script_pubkey=CScript(bytes.fromhex(os.environ["PAYOUT_SCRIPT"])))
coinbase.vout[0].nValue = tmpl["coinbasevalue"]
block = create_block(
    hashprev=int(tmpl["previousblockhash"], 16),
    coinbase=coinbase,
    ntime=tmpl["curtime"],
    version=tmpl["version"],
    tmpl=tmpl,
    txlist=[tx["data"] for tx in tmpl["transactions"]],
)
add_witness_commitment(block)
block.solve()
print(block.serialize().hex())
PY
)"

BEFORE="$(qbit_rpc getblockcount)"
RESULT="$(qbit_rpc submitblock "${BLOCK_HEX}")"
AFTER="$(qbit_rpc getblockcount)"

if [[ -n "${RESULT}" && "${RESULT}" != "null" ]]; then
  echo "submitblock returned failure: ${RESULT}" >&2
  exit 1
fi

if [[ "${AFTER}" -ne $((BEFORE + 1)) ]]; then
  echo "expected blockcount ${BEFORE}+1 after submitblock, got ${AFTER}" >&2
  exit 1
fi

printf 'permissionless smoke test passed: height %s -> %s\n' "${BEFORE}" "${AFTER}"
