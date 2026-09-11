#!/usr/bin/env bash
# Full-size share-append throughput floor for the native ledger (issue #271).
#
# This runs the real production share path: every share is one `Ledger::append`
# transaction that takes the cluster-wide ORDER_LOCK advisory lock, so the rate
# it reports is the rate at which that serialized transaction retires, measured
# at 1, 2 and 4 concurrent appenders.
#
# It replaces the CPU-only audit-builder benchmark this script used to run. That
# benchmark still exists and is still worth running; it just answers a different
# question (builder CPU cost, no database):
#
#     cargo run --release -p qbit-prism-server -- benchmark \
#         --shares 100000 --miners 100 --iterations 10
#
# The numbers here are also NOT comparable with the 2.x.x
# `qbit.prism.postgres-throughput.v1` report, which timed one bulk
# `INSERT ... SELECT ... FROM generate_series`: one statement, one commit, no
# advisory lock, no concurrency. The report this script writes carries the
# schema `qbit.prism.postgres-throughput.v2` for that reason.
#
# Environment:
#   PRISM_TEST_DATABASE_URL        an existing PostgreSQL 16; when unset, a
#                                  disposable cluster is started from the local
#                                  server binaries and torn down afterwards.
#   QBIT_PRISM_MIN_SHARES_PER_SEC  the floor; unset uses the test's compiled-in
#                                  constant.
#   QBIT_PRISM_THROUGHPUT_*        window size, shares per level, appender
#                                  counts, lock sampling interval, report path.
#                                  Every one of these is inherited by the test
#                                  process unchanged; this script reads none of
#                                  them except to print where the report landed.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Release, unlike the CI-sized run: this is the "how fast can this host go"
# measurement, and a debug build would report the compiler's overhead as the
# ledger's. The CI floor is calibrated separately, in debug.
cargo_args=(
  test --locked --release -p qbit-prism-server --test throughput_floor --
  --ignored --exact share_append_throughput_floor_full_size --nocapture
)

# The test writes its report before it asserts the floor, so a run that fails
# the floor still leaves the evidence behind. `set -e` is suspended around the
# test for exactly that reason: the path is worth printing on a red run, and the
# status is re-raised immediately afterwards.
status=0
if [[ -n "${PRISM_TEST_DATABASE_URL:-}" ]]; then
  cargo "${cargo_args[@]}" || status=$?
else
  # prism-native-tests.sh initdb's a throwaway cluster on a random loopback
  # port, exports PRISM_TEST_DATABASE_URL for the command it runs, and stops
  # and removes it on exit.
  test/prism-native-tests.sh cargo-args "${cargo_args[@]}" || status=$?
fi

report="${QBIT_PRISM_THROUGHPUT_REPORT:-$(pwd)/target/prism-postgres-throughput.json}"
echo "PRISM share-append throughput report: ${report}"
exit "${status}"
