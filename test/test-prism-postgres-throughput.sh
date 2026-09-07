#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# The native builder benchmark measures CPU work; use capacity-evidence to
# validate separately captured full Stratum-to-PostgreSQL qualification runs.
exec cargo run --locked --release -p qbit-prism-server -- benchmark --shares "${PRISM_BENCHMARK_SHARES:-100000}" --miners "${PRISM_BENCHMARK_MINERS:-100}" --iterations "${PRISM_BENCHMARK_ITERATIONS:-10}" "$@"
