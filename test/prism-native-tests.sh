#!/usr/bin/env bash
# Run the native PRISM test suites against a disposable PostgreSQL.
#
#   test/prism-native-tests.sh              every workspace test, the two
#                                           explicit #[ignore] database runs,
#                                           and the gate manifest check: the
#                                           prism-native-postgres CI job
#   test/prism-native-tests.sh live         the real qbitd regtest suite
#   test/prism-native-tests.sh replica      the physical replica and failover suite
#   test/prism-native-tests.sh cargo-args <cargo test arguments...>
#
# Inputs: PRISM_TEST_DATABASE_URL (otherwise a private cluster is started from
# PRISM_TEST_PG_BIN_DIR or `pg_config --bindir`), PRISM_TEST_PG_BIN_DIR, and
# QBITD_BIN (otherwise QBIT_BIN_DIR/qbitd, otherwise qbitd on PATH). Once every
# input a mode needs is present, PRISM_TEST_REQUIRE_INTEGRATION=1 is exported
# so a gated test that cannot run fails instead of skipping, and the gate's
# execution manifest is written to PRISM_TEST_GATE_MANIFEST (default: a file in
# this run's scratch directory). See docs/prism-integration-test-gate.md.
set -euo pipefail

PRISM_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PRISM_ROOT}"
mode="${1:-database}"
case "${mode}" in
  database|live|replica|cargo-args) ;;
  *)
    echo "Usage: test/prism-native-tests.sh [database|live|replica|cargo-args <cargo test arguments...>]" >&2
    exit 1
    ;;
esac

prism_test_tmp="$(mktemp -d -t prism-native-tests.XXXXXX)"
prism_pg_bin=""
prism_cluster_started=0
cleanup() {
  if [[ "${prism_cluster_started}" == 1 ]]; then
    "${prism_pg_bin}/pg_ctl" -D "${prism_test_tmp}/data" -m immediate stop >/dev/null 2>&1 || true
  fi
  rm -rf "${prism_test_tmp}"
}
trap cleanup EXIT

prism_pg_bin="${PRISM_TEST_PG_BIN_DIR:-}"
if [[ -z "${prism_pg_bin}" ]] && command -v pg_config >/dev/null 2>&1; then
  prism_pg_bin="$(pg_config --bindir)"
fi
if [[ -x "${prism_pg_bin}/initdb" && -x "${prism_pg_bin}/pg_ctl" ]]; then
  export PRISM_TEST_PG_BIN_DIR="${prism_pg_bin}"
else
  unset PRISM_TEST_PG_BIN_DIR
  if [[ "${mode}" == replica ]]; then
    echo 'Install PostgreSQL server tools and set PRISM_TEST_PG_BIN_DIR for physical replica tests.' >&2
    exit 1
  fi
fi

if [[ -z "${PRISM_TEST_DATABASE_URL:-}" ]]; then
  [[ -n "${PRISM_TEST_PG_BIN_DIR:-}" ]] || {
    echo 'Set PRISM_TEST_DATABASE_URL or install PostgreSQL server tools for isolated native tests.' >&2
    exit 1
  }
  "${prism_pg_bin}/initdb" -D "${prism_test_tmp}/data" -A trust --no-locale -E UTF8 > "${prism_test_tmp}/initdb.log"
  # A private socket directory and random loopback port avoid production DBs.
  started=0
  for _ in 1 2 3 4 5; do
    prism_test_port=$((20000 + RANDOM % 20000))
    if "${prism_pg_bin}/pg_ctl" -D "${prism_test_tmp}/data" -l "${prism_test_tmp}/postgres.log" \
      -o "-h 127.0.0.1 -p ${prism_test_port} -k ${prism_test_tmp}" start >/dev/null 2>&1; then
      started=1
      prism_cluster_started=1
      break
    fi
  done
  [[ "${started}" == 1 ]] || { cat "${prism_test_tmp}/postgres.log" >&2; exit 1; }
  prism_test_user="$(id -un)"
  export PRISM_TEST_DATABASE_URL="postgresql://${prism_test_user}@127.0.0.1:${prism_test_port}/postgres"
fi

# qbitd: QBITD_BIN, then QBIT_BIN_DIR, then PATH. Only the live suite refuses
# to go on without it; the other modes note its absence below.
qbitd="${QBITD_BIN:-${QBIT_BIN_DIR:+${QBIT_BIN_DIR}/qbitd}}"
if [[ -z "${qbitd}" ]]; then qbitd="$(command -v qbitd || true)"; fi
if [[ -n "${qbitd}" && -x "${qbitd}" ]]; then
  export QBITD_BIN="${qbitd}"
else
  unset QBITD_BIN
  if [[ "${mode}" == live ]]; then
    echo 'Set QBITD_BIN to a qbitd executable for native regtest integration.' >&2
    exit 1
  fi
fi

# Required mode: once every input the mode needs is guaranteed, a gated test
# that cannot run fails instead of skipping, exactly as in CI. The default mode
# runs the whole workspace, so it needs all three inputs; the explicit modes
# have already refused to start without theirs, and a cargo-args run names its
# own tests, so a missing input there should fail loudly too.
required=1
if [[ "${mode}" == database ]] && [[ -z "${PRISM_TEST_PG_BIN_DIR:-}" || -z "${QBITD_BIN:-}" ]]; then
  required=0
fi
if [[ "${required}" == 1 ]]; then
  export PRISM_TEST_REQUIRE_INTEGRATION=1
else
  unset PRISM_TEST_REQUIRE_INTEGRATION
  echo 'prism-native-tests: QBITD_BIN or PostgreSQL server tools are missing, so the gated tests that need them will skip; the manifest below is printed but not checked. Provide both for a run identical to CI.' >&2
fi
export PRISM_TEST_GATE_MANIFEST="${PRISM_TEST_GATE_MANIFEST:-${prism_test_tmp}/gate-manifest.txt}"
# The manifest describes this run only.
: > "${PRISM_TEST_GATE_MANIFEST}"
log="${prism_test_tmp}/tests.log"

run_tests() {
  "$@" 2>&1 | tee -a "${log}"
}

case "${mode}" in
  live)
    run_tests cargo test --locked -p qbit-prism-server --test live_regtest -- --nocapture
    ;;
  replica)
    run_tests cargo test --locked -p qbit-prism-server --test postgres_failover -- --nocapture
    ;;
  cargo-args)
    # Run an arbitrary cargo test invocation against the cluster this script
    # prepared, so long or #[ignore]d runs can use the disposable database too.
    shift
    [[ $# -gt 0 ]] || {
      echo 'Usage: test/prism-native-tests.sh cargo-args <cargo test arguments...>' >&2
      exit 1
    }
    run_tests cargo test "$@"
    ;;
  database)
    run_tests cargo test --locked --workspace --all-targets -- --nocapture
    run_tests cargo test --locked -p qbit-prism-server --test stratum_admission_postgres \
      -- --ignored --nocapture --exact ten_thousand_unsubscribed_connections_do_not_advance_postgres_sequence
    run_tests cargo test --locked -p qbit-prism-server --test observability_database -- --ignored --nocapture
    ;;
esac

if [[ "${mode}" == database && "${required}" == 1 ]]; then
  python3 scripts/check_gate_manifest.py \
    --manifest "${PRISM_TEST_GATE_MANIFEST}" \
    --expected test/prism-gated-tests.txt \
    --log "${log}"
else
  echo "prism-native-tests: gate manifest for this run (${PRISM_TEST_GATE_MANIFEST}):"
  if [[ -s "${PRISM_TEST_GATE_MANIFEST}" ]]; then
    sort "${PRISM_TEST_GATE_MANIFEST}"
  else
    echo '(no gated test recorded a decision)'
  fi
fi
