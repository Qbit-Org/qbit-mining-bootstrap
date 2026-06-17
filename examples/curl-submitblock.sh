#!/usr/bin/env bash
set -euo pipefail

RPC_USER="${RPC_USER:-qbitrpc}"
RPC_PASSWORD="${RPC_PASSWORD:-change-this}"
RPC_HOST="${RPC_HOST:-127.0.0.1}"
RPC_PORT="${RPC_PORT:-18452}"
BLOCK_HEX="${BLOCK_HEX:?set BLOCK_HEX to the serialized block returned by your block builder}"

# submitblock returns JSON null on acceptance and a BIP22 rejection string on failure.
# qbit ignores the optional second BIP22 argument exactly like Bitcoin Core.

curl --user "${RPC_USER}:${RPC_PASSWORD}" \
  --header 'content-type: text/plain;' \
  --data-binary "{\"jsonrpc\":\"1.0\",\"id\":\"qbit-mining-guide\",\"method\":\"submitblock\",\"params\":[\"${BLOCK_HEX}\"]}" \
  "http://${RPC_HOST}:${RPC_PORT}/"
