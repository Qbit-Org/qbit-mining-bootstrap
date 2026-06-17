#!/usr/bin/env bash
set -euo pipefail

RPC_USER="${RPC_USER:-qbitrpc}"
RPC_PASSWORD="${RPC_PASSWORD:-change-this}"
RPC_HOST="${RPC_HOST:-127.0.0.1}"
RPC_PORT="${RPC_PORT:-18452}"
AUX_HASH="${AUX_HASH:?set AUX_HASH to the hash returned by createauxblock}"
AUXPOW_HEX="${AUXPOW_HEX:?set AUXPOW_HEX to the serialized CAuxPow payload}"

# submitauxblock returns JSON null on acceptance, "stale-prevblk" if the cached
# candidate expired, or a qbit reject reason like "bad-auxpow-parent-hash".

curl --user "${RPC_USER}:${RPC_PASSWORD}" \
  --header 'content-type: text/plain;' \
  --data-binary "{\"jsonrpc\":\"1.0\",\"id\":\"qbit-mining-guide\",\"method\":\"submitauxblock\",\"params\":[\"${AUX_HASH}\",\"${AUXPOW_HEX}\"]}" \
  "http://${RPC_HOST}:${RPC_PORT}/"
