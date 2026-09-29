#!/usr/bin/env bash
# The PRISM 2.x.x -> 3.x.x cutover rehearsal (#575), for operators.
#
#   test/prism-cutover-rehearsal.sh rehearse   rehearse the cutover on a pg_dump
#   test/prism-cutover-rehearsal.sh dump       write a mainnet-shaped 2.x.x dump
#   test/prism-cutover-rehearsal.sh generated  both: write a mainnet-shaped dump
#                                              under WORKDIR and rehearse it
#
# rehearse restores DUMP (custom, directory, tar or plain format; a plain dump
# must be taken with --no-owner --no-privileges) into a
# private PostgreSQL cluster it creates under WORKDIR and removes afterwards,
# runs check-config, migrate and import-audits exactly as
# docs/prism-rust-migration.md does, checks the recovery evidence, balances,
# payout window and table sums against the source, starts one frontend
# (self-check, API balances, first Stratum job) and prints a PASS/FAIL report.
# It connects to no database but the one it creates.
#
#   DUMP=<file>            required: the pg_dump to rehearse
#   LEDGER_KEY=<hex>       required: the trusted PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX
#   AUDIT_ROOT=<dir>       external audit bodies and canonical sidecars, mounted
#                          at their recorded paths (default: none)
#   SCHEMA=<name>          the ledger schema (default: the one holding qbit_share_ledger)
#   ENV_FILE=<file>        the reviewed production environment the real cutover
#                          uses (NAME=value lines): its synced node, chain,
#                          genesis pin, signing keys and pool fee. Only the
#                          database URL, listen ports and instance ID are
#                          replaced. Default: a lab configuration with test
#                          keys and a node serving the dump's own chain
#   STATEMENT_TIMEOUT_MS=<ms>  the PRISM_DATABASE_STATEMENT_TIMEOUT_MS the real
#                          migrate will run with (default: the server's 15000)
#   REPORT=<file>          also write the report as JSON
#   WORKDIR=<dir>          where the private cluster lives (default: TMPDIR)
#   PG_BIN_DIR=<dir>       PostgreSQL 16 server binaries (default: pg_config --bindir)
#
# dump writes OUT (custom format) and OUT's audit bodies to OUT.audits:
#
#   OUT=<file>             required
#   SCALE=<n>              1 (mainnet 2.x.x, Jul 15 - Sep 28 2026) or more
#   DENSITY=<fraction>     share rows kept, each carrying 1/DENSITY of the
#                          difficulty (default 0.0625)
#
# generated takes SCALE, DENSITY, STATEMENT_TIMEOUT_MS, REPORT and WORKDIR
# (default: a new directory under TMPDIR, removed afterwards unless KEEP=1),
# writes the dump there and rehearses it with the generator's own ledger key:
# one command to rerun the rehearsal on any branch.
set -euo pipefail

PRISM_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PRISM_ROOT}"
mode="${1:-}"

fail() {
  echo "prism-cutover-rehearsal: $*" >&2
  exit 2
}

pg_bin="${PG_BIN_DIR:-}"
if [[ -z "${pg_bin}" ]] && command -v pg_config >/dev/null; then
  pg_bin="$(pg_config --bindir)"
fi
[[ -n "${pg_bin}" && -x "${pg_bin}/initdb" ]] || fail "set PG_BIN_DIR to the PostgreSQL 16 server binaries (initdb not found)"

# Only this run's own variables reach the test: nothing inherited selects
# another database.
export PRISM_TEST_PG_BIN_DIR="${pg_bin}"
export PRISM_TEST_REQUIRE_INTEGRATION=1
unset PRISM_TEST_DATABASE_URL

export_optional() {
  local name="$1" value="$2"
  if [[ -n "${value}" ]]; then
    export "${name}=${value}"
  else
    unset "${name}"
  fi
}

# The ledger key the generator signs its history with (seed 22...22).
GENERATED_LEDGER_KEY=a09aa5f47a6759802ff955f8dc2d2a14a5c99d23be97f864127ff9383455a4f0

if [[ "${mode}" == generated ]]; then
  workdir="${WORKDIR:+$(realpath -m "${WORKDIR}")}"
  if [[ -z "${workdir}" ]]; then
    workdir="$(mktemp -d "${TMPDIR:-/tmp}/prism-cutover-rehearsal.XXXXXX")"
    if [[ "${KEEP:-0}" != 1 ]]; then
      trap 'rm -rf -- "${workdir:?}"' EXIT
    fi
  fi
  mkdir -p "${workdir}"
  OUT="${workdir}/generated.dump" bash "$0" dump
  DUMP="${workdir}/generated.dump" AUDIT_ROOT="${workdir}/generated.audits" \
    LEDGER_KEY="${GENERATED_LEDGER_KEY}" WORKDIR="${workdir}" bash "$0" rehearse
  exit
fi

case "${mode}" in
  rehearse)
    [[ -n "${DUMP:-}" ]] || fail "set DUMP to the pg_dump to rehearse"
    [[ -e "${DUMP}" ]] || fail "DUMP ${DUMP} does not exist"
    [[ -n "${LEDGER_KEY:-}" ]] || fail "set LEDGER_KEY to the trusted ledger public key (64 hex digits)"
    PRISM_REHEARSAL_DUMP="$(realpath "${DUMP}")"
    export PRISM_REHEARSAL_DUMP
    export PRISM_REHEARSAL_LEDGER_PUBLIC_KEY_HEX="${LEDGER_KEY}"
    # cargo runs the test from the crate directory: every path is absolute.
    export_optional PRISM_REHEARSAL_AUDIT_ROOT "${AUDIT_ROOT:+$(realpath -m "${AUDIT_ROOT}")}"
    export_optional PRISM_REHEARSAL_SCHEMA "${SCHEMA:-}"
    export_optional PRISM_REHEARSAL_ENV_FILE "${ENV_FILE:+$(realpath -m "${ENV_FILE}")}"
    report_path=""
    if [[ -n "${REPORT:-}" ]]; then
      report_path="$(realpath -m "${REPORT}")"
    fi
    export_optional PRISM_REHEARSAL_REPORT "${report_path}"
    export_optional PRISM_REHEARSAL_WORKDIR "${WORKDIR:+$(realpath -m "${WORKDIR}")}"
    export_optional PRISM_REHEARSAL_STATEMENT_TIMEOUT_MS "${STATEMENT_TIMEOUT_MS:-}"
    test_name=cutover_rehearsal_tests::operator_dump_rehearsal
    ;;
  dump)
    [[ -n "${OUT:-}" ]] || fail "set OUT to the dump file to write"
    PRISM_REHEARSAL_GENERATE_DUMP="$(realpath -m "${OUT}")"
    export PRISM_REHEARSAL_GENERATE_DUMP
    export_optional PRISM_REHEARSAL_SCALE "${SCALE:-}"
    export_optional PRISM_REHEARSAL_DENSITY "${DENSITY:-}"
    test_name=cutover_rehearsal_tests::generate_mainnet_shaped_dump
    ;;
  *)
    fail "usage: test/prism-cutover-rehearsal.sh rehearse|dump|generated (see the header)"
    ;;
esac

cargo test --locked --release -p qbit-prism-server --test migration_rollback -- \
  --ignored --exact --nocapture --test-threads=1 "${test_name}"
