#!/usr/bin/env bash

set -euo pipefail

require_executable() {
  local binary="$1"

  if [[ "${binary}" == */* ]]; then
    [[ -x "${binary}" ]] && return 0
  else
    command -v "${binary}" >/dev/null 2>&1 && return 0
  fi

  echo "Required executable not found: ${binary}" >&2
  return 1
}

require_qbit_src_helper() {
  local helper_rel="$1"
  local purpose="$2"
  local qbit_src="${QBIT_SRC_DIR:-}"

  if [[ -z "${qbit_src}" ]]; then
    echo "Set QBIT_SRC_DIR to a separate qbit source checkout for ${purpose}." >&2
    return 1
  fi

  if [[ "${qbit_src}" == "~"* ]]; then
    qbit_src="${HOME}${qbit_src#\~}"
  fi

  if [[ ! -f "${qbit_src}/${helper_rel}" ]]; then
    echo "QBIT_SRC_DIR does not contain ${helper_rel}: ${qbit_src}" >&2
    return 1
  fi

  QBIT_SRC_DIR="$(cd "${qbit_src}" && pwd)"
}

resolve_qbit_binaries() {
  if [[ -n "${QBIT_BIN_DIR:-}" ]]; then
    if [[ "${QBIT_BIN_DIR}" == "~"* ]]; then
      QBIT_BIN_DIR="${HOME}${QBIT_BIN_DIR#\~}"
    fi
    QBIT_BIN_DIR="$(cd "${QBIT_BIN_DIR}" && pwd)"
    QBITD_BIN="${QBITD_BIN:-${QBIT_BIN_DIR}/qbitd}"
    QBIT_CLI_BIN="${QBIT_CLI_BIN:-${QBIT_BIN_DIR}/qbit-cli}"
  else
    QBITD_BIN="${QBITD_BIN:-qbitd}"
    QBIT_CLI_BIN="${QBIT_CLI_BIN:-qbit-cli}"
  fi

  require_executable "${QBITD_BIN}"
  require_executable "${QBIT_CLI_BIN}"
}

qbit_rpc() {
  "${QBIT_CLI_BIN}" \
    -regtest \
    -datadir="${DATADIR}" \
    -rpcuser="${RPC_USER}" \
    -rpcpassword="${RPC_PASSWORD}" \
    -rpcport="${RPC_PORT}" \
    "$@"
}

wait_for_qbit_rpc_state() {
  local desired="$1"
  local attempts="${2:-60}"
  local ok=1

  for ((i = 0; i < attempts; i++)); do
    if qbit_rpc getblockchaininfo >/dev/null 2>&1; then
      ok=0
    else
      ok=1
    fi

    if [[ "${desired}" == "ready" && "${ok}" -eq 0 ]]; then
      return 0
    fi

    if [[ "${desired}" == "stopped" && "${ok}" -eq 1 ]]; then
      return 0
    fi

    sleep 1
  done

  echo "Timed out waiting for qbit RPC state '${desired}' on port ${RPC_PORT}" >&2
  return 1
}

stop_qbitd() {
  qbit_rpc stop >/dev/null 2>&1 || true
  wait_for_qbit_rpc_state stopped 60 || true
}

remove_datadir() {
  local path="$1"

  for ((i = 0; i < 10; i++)); do
    rm -rf "${path}" >/dev/null 2>&1 && [[ ! -e "${path}" ]] && return 0
    sleep 1
  done

  echo "Failed to remove datadir after qbit shutdown: ${path}" >&2
  return 1
}
