#!/bin/sh
set -eu

# BITCOIN_NODE_EXTRA_ARGS is intentionally split on shell whitespace so
# deployment tooling can pass several bitcoind flags through one env var.
set -f
if [ -n "${BITCOIN_NODE_EXTRA_ARGS:-}" ]; then
  # shellcheck disable=SC2086
  set -- "$@" ${BITCOIN_NODE_EXTRA_ARGS}
fi

exec bitcoind "$@"
