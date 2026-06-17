#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/test/test-lib.sh"
DATADIR="${DATADIR:-$(mktemp -d -t qbit-guide-mergemining.XXXXXX)}"
RPC_USER="${RPC_USER:-qbitrpc}"
RPC_PASSWORD="${RPC_PASSWORD:-change-this}"
RPC_PORT="${RPC_PORT:-18453}"
WALLET_NAME="${WALLET_NAME:-miner}"

resolve_qbit_binaries
require_executable python3
require_qbit_src_helper "test/functional/test_framework/auxpow.py" "the merge-mining smoke test"

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
AUX_TEMPLATE_JSON="$(qbit_rpc createauxblock "${PAYOUT_ADDRESS}")"
AUX_HASH="$(AUX_TEMPLATE_JSON="${AUX_TEMPLATE_JSON}" python3 -c 'import json,os; print(json.loads(os.environ["AUX_TEMPLATE_JSON"])["hash"])')"
AUXPOW_HEX="$(
  printf '%s\n' "${AUX_TEMPLATE_JSON}" | \
    python3 "${ROOT_DIR}/examples/python-auxpow-payload.py" --qbit-src "${QBIT_SRC_DIR}" --parent-time "$(date +%s)"
)"

BEFORE="$(qbit_rpc getblockcount)"
RESULT="$(qbit_rpc submitauxblock "${AUX_HASH}" "${AUXPOW_HEX}")"
AFTER="$(qbit_rpc getblockcount)"

if [[ -n "${RESULT}" && "${RESULT}" != "null" ]]; then
  echo "submitauxblock returned failure: ${RESULT}" >&2
  exit 1
fi

if [[ "${AFTER}" -ne $((BEFORE + 1)) ]]; then
  echo "expected blockcount ${BEFORE}+1 after submitauxblock, got ${AFTER}" >&2
  exit 1
fi

printf 'merge-mining smoke test passed: height %s -> %s\n' "${BEFORE}" "${AFTER}"
