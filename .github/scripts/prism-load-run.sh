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
#   host.json                the host fingerprint from scripts/prism_load_probe.py
#                            host: the runner label (RUNNER_LABEL, when set),
#                            CPU, memory, kernel, and the filesystem and block
#                            device under the cluster (mount options, write
#                            cache, FUA)
#   pg_test_fsync.txt        the WAL volume's commit cost, on the filesystem
#                            the harness builds its cluster on
#   harness-exit-code        the harness's exit code
#   gate.md                  the gate's verdict table (a soak preset's soak
#                            gates included)
#   gate-exit-code           the gate's exit code: 0 or 1 is a verdict, anything
#                            else (2, a panic, a binary that would not start) is not
#   everything the harness writes (load-harness-report.json, logs/, and for
#   a soak preset soak-samples.jsonl, soak-events.jsonl, soak-report.md)
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
presets="${root}/crates/qbit-prism-load/presets"
# A deprecated name (presets/aliases.txt) runs the preset it was renamed to.
if [[ ! -f "${presets}/${preset}.json" && -f "${presets}/aliases.txt" ]]; then
  renamed="$(awk -v old="${preset}" '$1 == old && $1 !~ /^#/ { print $2 }' "${presets}/aliases.txt")"
  if [[ -n "${renamed}" ]]; then
    echo "prism-load-run: ${preset} is deprecated; running ${renamed}" >&2
    preset="${renamed}"
  fi
fi
preset_file="${presets}/${preset}.json"
test -f "${preset_file}" || { echo "no preset ${preset_file}" >&2; exit 2; }

mkdir -p "${out}"
# The harness builds its cluster under TMPDIR, and PostgreSQL refuses a
# socket path over 107 bytes, so the directory is kept short.
export TMPDIR="${PRISM_LOAD_TMPDIR:-${RUNNER_TEMP:-/tmp}/pload}"
mkdir -p "${TMPDIR}"

# #549: the fingerprint is evidence beside the result, so failing to take it
# is noted in the file and does not stop the run.
python3 "${root}/scripts/prism_load_probe.py" host --out "${out}/host.json" --dir "${TMPDIR}" \
  ${RUNNER_LABEL:+--runner "${RUNNER_LABEL}"} \
  || echo '{"error": "prism_load_probe.py host failed"}' > "${out}/host.json"

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
echo "${status}" > "${out}/gate-exit-code"
cat "${out}/gate.md"
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  {
    cat "${out}/gate.md"
    echo
    if [[ -f "${out}/soak-report.md" ]]; then
      echo "<details><summary>soak samples</summary>"
      echo
      sed -n '/^#### Samples/,$p' "${out}/soak-report.md"
      echo
      echo "</details>"
      echo
    fi
    echo "<details><summary>Host</summary>"
    echo
    echo '```json'
    cat "${out}/host.json"
    echo '```'
    echo
    echo "</details>"
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
