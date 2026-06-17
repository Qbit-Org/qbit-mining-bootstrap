#!/usr/bin/env bash
set -euo pipefail

RPC_USER="${RPC_USER:-qbitrpc}"
RPC_PASSWORD="${RPC_PASSWORD:-change-this}"
RPC_HOST="${RPC_HOST:-127.0.0.1}"
RPC_PORT="${RPC_PORT:-18452}"

# Default qbit RPC ports are 8352 on mainnet and 18452 on regtest.
# qbit keeps standard BIP22/BIP23 GBT semantics and requires segwit support
# to be declared in the "rules" array.
#
# Expected response keys include:
#   version, rules, previousblockhash, transactions, coinbasevalue,
#   target, mintime, mutable, noncerange, sigoplimit, sizelimit,
#   weightlimit, curtime, bits, height, default_witness_commitment

curl --user "${RPC_USER}:${RPC_PASSWORD}" \
  --header 'content-type: text/plain;' \
  --data-binary '{"jsonrpc":"1.0","id":"qbit-mining-guide","method":"getblocktemplate","params":[{"rules":["segwit"]}]}' \
  "http://${RPC_HOST}:${RPC_PORT}/"
