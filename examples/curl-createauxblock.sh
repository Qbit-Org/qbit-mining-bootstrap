#!/usr/bin/env bash
set -euo pipefail

RPC_USER="${RPC_USER:-qbitrpc}"
RPC_PASSWORD="${RPC_PASSWORD:-change-this}"
RPC_HOST="${RPC_HOST:-127.0.0.1}"
RPC_PORT="${RPC_PORT:-18452}"
PAYOUT_ADDRESS="${PAYOUT_ADDRESS:?set PAYOUT_ADDRESS to a valid qbit payout address (public chains require P2MR)}"

# Expected response keys:
#   hash, chainid, previousblockhash, coinbasevalue, bits, height, target
#
# The returned "hash" is the cache key you must pass back to submitauxblock.

curl --user "${RPC_USER}:${RPC_PASSWORD}" \
  --header 'content-type: text/plain;' \
  --data-binary "{\"jsonrpc\":\"1.0\",\"id\":\"qbit-mining-guide\",\"method\":\"createauxblock\",\"params\":[\"${PAYOUT_ADDRESS}\"]}" \
  "http://${RPC_HOST}:${RPC_PORT}/"
