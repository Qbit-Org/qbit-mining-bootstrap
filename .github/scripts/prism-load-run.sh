#!/usr/bin/env bash
# Run one checked-in load-harness preset and gate it (#521). The nightly
# workflow runs exactly this, and so can anyone reproducing a nightly result.
#
# Usage: prism-load-run.sh <preset name> <output directory> [operational flags]
#
# Any further arguments go to the harness after the preset's, so only flags
# the preset does not set are accepted: `--allow-dirty-tree` to reproduce a
# run from a modified checkout, say. A preset flag given again is refused.
#
# Expects release builds of qbit-prism-server, qbit-prism-load and
# qbit-prism-load-gate in ${TARGET_DIR:-target}/release, and PostgreSQL 16
# server binaries in PG_BIN_DIR (default /usr/lib/postgresql/16/bin).
#
# Writes into the output directory:
#   pg_test_fsync.txt        the WAL volume's commit cost, on the filesystem
#                            the harness builds its cluster on
#   harness-exit-code        the harness's exit code
#   gate.md                  the gate's verdict table
#   everything the harness writes (load-harness-report.json, logs/, ...)
#
# Optional overrides of the preset's gates: PRISM_LOAD_MAX_SHORTFALL and
# PRISM_LOAD_TIP_LAST_NOTIFY_P99_BUDGET_MS (empty keeps the preset's). The
# gate's table is also appended to GITHUB_STEP_SUMMARY when that is set.
# Exits with the gate's status: 0 pass, 1 fail, 2 unreadable inputs.
set -euo pipefail

preset="${1:?usage: prism-load-run.sh <preset> <out> [flags]}"
out="${2:?usage: prism-load-run.sh <preset> <out> [flags]}"
shift 2
root="$(git rev-parse --show-toplevel)"
target="${TARGET_DIR:-${root}/target}/release"
pg_bin="${PG_BIN_DIR:-/usr/lib/postgresql/16/bin}"
preset_file="${root}/crates/qbit-prism-load/presets/${preset}.json"
test -f "${preset_file}" || { echo "no preset ${preset_file}" >&2; exit 2; }

mkdir -p "${out}"
# The harness builds its cluster under TMPDIR, and PostgreSQL refuses a
# socket path over 107 bytes, so the directory is kept short.
export TMPDIR="${PRISM_LOAD_TMPDIR:-${RUNNER_TEMP:-/tmp}/pload}"
mkdir -p "${TMPDIR}"

{
  echo "# pg_test_fsync on the harness's cluster filesystem (${TMPDIR})"
  df -h "${TMPDIR}" || true
  "${pg_bin}/pg_test_fsync" -s 2 -f "${TMPDIR}/pg_test_fsync.out"
} > "${out}/pg_test_fsync.txt" 2>&1 || echo "pg_test_fsync failed; see above" >> "${out}/pg_test_fsync.txt"
rm -f "${TMPDIR}/pg_test_fsync.out"

set +e
"${target}/qbit-prism-load" \
  --preset "${preset_file}" \
  --server-bin "${target}/qbit-prism-server" \
  --pg-bin-dir "${pg_bin}" \
  --out "${out}" \
  "$@"
code=$?
set -e
echo "${code}" > "${out}/harness-exit-code"

gate=("${target}/qbit-prism-load-gate"
  --report "${out}/load-harness-report.json"
  --preset "${preset_file}"
  --exit-code "${code}")
if [[ -n "${PRISM_LOAD_MAX_SHORTFALL:-}" ]]; then
  gate+=(--max-shortfall "${PRISM_LOAD_MAX_SHORTFALL}")
fi
if [[ -n "${PRISM_LOAD_TIP_LAST_NOTIFY_P99_BUDGET_MS:-}" ]]; then
  gate+=(--tip-last-notify-p99-budget-ms "${PRISM_LOAD_TIP_LAST_NOTIFY_P99_BUDGET_MS}")
fi
set +e
"${gate[@]}" > "${out}/gate.md"
status=$?
set -e
cat "${out}/gate.md"
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  {
    cat "${out}/gate.md"
    echo
    echo "<details><summary>pg_test_fsync</summary>"
    echo
    echo '```'
    cat "${out}/pg_test_fsync.txt"
    echo '```'
    echo
    echo "</details>"
    echo
  } >> "${GITHUB_STEP_SUMMARY}"
fi
exit "${status}"
