#!/usr/bin/env bash
set -euo pipefail

: "${QBIT_RPC_USER:?QBIT_RPC_USER is required}"
: "${QBIT_RPC_PASSWORD:?QBIT_RPC_PASSWORD is required}"
: "${QBIT_RPC_HOST:=qbitd}"
: "${QBIT_RPC_PORT:=18452}"
: "${QBIT_MINER_WALLET_NAME:=ckpool-real}"
: "${QBIT_MINER_ADDRESS_FILE:=/var/lib/qbit-lab/miner-address.txt}"

mkdir -p "$(dirname "${QBIT_MINER_ADDRESS_FILE}")"

qbit_cli() {
  qbit-cli \
    -rpcconnect="${QBIT_RPC_HOST}" \
    -rpcport="${QBIT_RPC_PORT}" \
    -rpcuser="${QBIT_RPC_USER}" \
    -rpcpassword="${QBIT_RPC_PASSWORD}" \
    "$@"
}

ready=0
for _ in $(seq 1 60); do
  if qbit_cli getblockchaininfo >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done

if [[ "${ready}" -ne 1 ]]; then
  echo "timed out waiting for qbit RPC at ${QBIT_RPC_HOST}:${QBIT_RPC_PORT}" >&2
  exit 1
fi

if ! qbit_cli createwallet "${QBIT_MINER_WALLET_NAME}" >/dev/null 2>&1; then
  qbit_cli loadwallet "${QBIT_MINER_WALLET_NAME}" >/dev/null 2>&1 || true
fi

resolve_qbit_address() {
  local address

  address="$(
    qbit_cli \
      -rpcwallet="${QBIT_MINER_WALLET_NAME}" \
      getnewaddress 2>/dev/null || true
  )"
  if [[ -n "${address}" ]]; then
    printf '%s\n' "${address}"
    return 0
  fi

  qbit_cli \
    -rpcwallet="${QBIT_MINER_WALLET_NAME}" \
    getnewaddress "" p2mr
}

address="$(
  resolve_qbit_address
)"

printf '%s\n' "${address}" > "${QBIT_MINER_ADDRESS_FILE}"
printf 'bootstrap miner address: %s\n' "${address}"
exec tail -f /dev/null
