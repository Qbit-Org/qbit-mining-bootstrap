#!/usr/bin/env bash
set -euo pipefail

PRISM_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PRISM_ROOT}"
mode="${1:-database}"
prism_test_tmp=""
prism_pg_bin=""
cleanup() {
  if [[ -n "${prism_test_tmp}" ]]; then
    "${prism_pg_bin}/pg_ctl" -D "${prism_test_tmp}/data" -m immediate stop >/dev/null 2>&1 || true
    rm -rf "${prism_test_tmp}"
  fi
}
trap cleanup EXIT

prism_pg_bin="${PRISM_TEST_PG_BIN_DIR:-}"
if [[ -z "${prism_pg_bin}" ]] && command -v pg_config >/dev/null 2>&1; then
  prism_pg_bin="$(pg_config --bindir)"
fi
if [[ -x "${prism_pg_bin}/initdb" && -x "${prism_pg_bin}/pg_ctl" ]]; then
  export PRISM_TEST_PG_BIN_DIR="${prism_pg_bin}"
elif [[ "${mode}" == replica ]]; then
  echo 'Install PostgreSQL server tools and set PRISM_TEST_PG_BIN_DIR for physical replica tests.' >&2
  exit 1
fi

if [[ -z "${PRISM_TEST_DATABASE_URL:-}" ]]; then
  [[ -x "${prism_pg_bin}/initdb" && -x "${prism_pg_bin}/pg_ctl" ]] || {
    echo 'Set PRISM_TEST_DATABASE_URL or install PostgreSQL server tools for isolated native tests.' >&2
    exit 1
  }
  prism_test_tmp="$(mktemp -d -t prism-native-tests.XXXXXX)"
  "${prism_pg_bin}/initdb" -D "${prism_test_tmp}/data" -A trust --no-locale -E UTF8 > "${prism_test_tmp}/initdb.log"
  # A private socket directory and random loopback port avoid production DBs.
  started=0
  for _ in 1 2 3 4 5; do
    prism_test_port=$((20000 + RANDOM % 20000))
    if "${prism_pg_bin}/pg_ctl" -D "${prism_test_tmp}/data" -l "${prism_test_tmp}/postgres.log" \
      -o "-h 127.0.0.1 -p ${prism_test_port} -k ${prism_test_tmp}" start >/dev/null 2>&1; then
      started=1
      break
    fi
  done
  [[ "${started}" == 1 ]] || { cat "${prism_test_tmp}/postgres.log" >&2; exit 1; }
  prism_test_user="$(id -un)"
  export PRISM_TEST_DATABASE_URL="postgresql://${prism_test_user}@127.0.0.1:${prism_test_port}/postgres"
  export PRISM_TEST_PG_BIN_DIR="${PRISM_TEST_PG_BIN_DIR:-${prism_pg_bin}}"
fi

if [[ "${mode}" == live ]]; then
  export QBITD_BIN="${QBITD_BIN:-${QBIT_BIN_DIR:+${QBIT_BIN_DIR}/qbitd}}"
  if [[ -z "${QBITD_BIN}" ]]; then QBITD_BIN="$(command -v qbitd || true)"; fi
  [[ -x "${QBITD_BIN}" ]] || { echo 'Set QBITD_BIN to a qbitd executable for native regtest integration.' >&2; exit 1; }
  cargo test --locked -p qbit-prism-server --test live_regtest -- --nocapture
elif [[ "${mode}" == replica ]]; then
  cargo test --locked -p qbit-prism-server --test postgres_failover -- --nocapture
else
  cargo test --locked -p qbit-prism-server --all-targets
fi
