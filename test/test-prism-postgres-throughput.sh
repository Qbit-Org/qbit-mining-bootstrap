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
# RUN THIS AGAINST A DATABASE NOTHING ELSE IS APPENDING TO.
#   ORDER_LOCK is a PostgreSQL advisory lock, and an advisory lock is scoped to
#   the database, not to the per-run schema the test creates. Any other backend
#   appending shares into the same database serializes against this run and
#   roughly halves the rate reported here. The test does not trust the operator
#   to get that right: every connection it opens carries a per-run
#   `application_name`, the ORDER_LOCK sampler separates this run's waiters from
#   everyone else's, and a level during which a foreign backend held or waited
#   on the lock is marked `contaminated: true` in the report and in the failure
#   message. A contaminated level is still measured and still gated, but its
#   number describes two workloads sharing one lock and must not be carried
#   forward as a property of the append path. Leaving PRISM_TEST_DATABASE_URL
#   unset gives you a disposable cluster of your own, which is the safe default.
#
# A CANCELLED RUN LEAVES ITS SCHEMA BEHIND.
#   The test drops its `prism_throughput_<uuid>` schema on every exit path it
#   controls, but Ctrl-C kills the process before the drop runs. Adding a signal
#   handler would mean a new dependency the test is not allowed to take, so the
#   leftovers are swept by hand. On a server you keep, after cancelling a run:
#
#     SELECT nspname FROM pg_namespace WHERE nspname LIKE 'prism\_throughput\_%';
#     DROP SCHEMA prism_throughput_0ee059ccc0314cbb8a814c0319f846d5 CASCADE;
#
#   Do that only when no run is in flight; a live run's schema matches the same
#   pattern. A disposable cluster (PRISM_TEST_DATABASE_URL unset) needs none of
#   this: it is thrown away whole.
#
# Environment:
#   PRISM_TEST_DATABASE_URL        an existing PostgreSQL 16; when unset, a
#                                  disposable cluster is started from the local
#                                  server binaries and torn down afterwards.
#   QBIT_PRISM_MIN_SHARES_PER_SEC  the floor; unset uses the test's compiled-in
#                                  constant.
#   QBIT_PRISM_THROUGHPUT_*        window size, shares per level, appender
#                                  counts, lock sampling interval (1..=1000 ms),
#                                  report path. Every one of these is inherited
#                                  by the test process unchanged; this script
#                                  reads none of them except to work out where
#                                  the report should have landed.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Each test writes its own report, so the two floor tests cannot overwrite each
# other. This script runs the full-size one.
test_name=share_append_throughput_floor_full_size

# Release, unlike the CI-sized run: this is the "how fast can this host go"
# measurement, and a debug build would report the compiler's overhead as the
# ledger's. The CI floor is calibrated separately, in debug.
cargo_args=(
  test --locked --release -p qbit-prism-server --test throughput_floor --
  --ignored --exact "${test_name}" --nocapture
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

# Announce the report only if there is one. A run that died before the test
# started — no database, no server binaries, a rejected variable — writes
# nothing, and printing a path to a file that does not exist sends the reader
# looking for evidence that was never produced.
report="${QBIT_PRISM_THROUGHPUT_REPORT:-$(pwd)/target/prism-postgres-throughput-${test_name}.json}"
if [[ -f "${report}" ]]; then
  echo "PRISM share-append throughput report: ${report}"
else
  echo "PRISM share-append throughput: no report was written (expected it at ${report})"
fi
exit "${status}"
