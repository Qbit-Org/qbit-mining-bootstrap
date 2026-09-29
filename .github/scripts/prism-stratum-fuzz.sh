#!/usr/bin/env bash
# Run the Stratum cargo-fuzz targets for a fixed time and gate on no crash
# (#575). The nightly workflow runs exactly this, and so can anyone
# reproducing a nightly result.
#
# Usage: prism-stratum-fuzz.sh <corpus directory> <output directory> [seconds]
#
# Installs the pinned nightly toolchain and the pinned cargo-fuzz release
# (checked against its sha256), builds the targets in
# crates/qbit-prism-server/fuzz, and runs all three at once for the given
# seconds each (default 1200): stratum_session, stratum_lines and
# codec_parsers. Each starts from the checked-in seeds and
# <corpus directory>/<target>, which it grows; afterwards each target's corpus
# is merged down to the inputs that add coverage, so a cached corpus stays
# small from night to night.
#
# Writes into the output directory:
#   <target>.log          libFuzzer's log, final stats included
#   artifacts/<target>/   crash-*, leak-*, timeout-* and oom-* inputs
#   summary.md            one row per target, also appended to
#                         GITHUB_STEP_SUMMARY when that is set
# Exits 1 when any target found a crash, leak, timeout or out-of-memory
# input or exited abnormally, after every target has run; 0 otherwise.
# Reproduce an artifact with:
#   cargo +<nightly> fuzz run --fuzz-dir crates/qbit-prism-server/fuzz <target> <artifact>
# and PRISM_FUZZ_TRACE=1 to print the conversation.
set -euo pipefail

corpus="${1:?usage: prism-stratum-fuzz.sh <corpus dir> <output dir> [seconds]}"
out="${2:?usage: prism-stratum-fuzz.sh <corpus dir> <output dir> [seconds]}"
seconds="${3:-1200}"
[[ "${seconds}" =~ ^[1-9][0-9]*$ ]] || { echo "prism-stratum-fuzz: seconds must be a positive integer" >&2; exit 2; }

# The toolchain the targets were last run and measured on. cargo-fuzz needs
# nightly for its sanitizer flags; the workspace itself stays on the stable
# pin in rust-toolchain.toml.
NIGHTLY=nightly-2026-09-24
CARGO_FUZZ_VERSION=0.13.2
CARGO_FUZZ_SHA256=b5b704018b63e0f151c17a057ac53b5111e1db545d1b9f72fee79f08a545931c
TARGETS=(stratum_session stratum_lines codec_parsers)
declare -A MAX_LEN=([stratum_session]=16384 [stratum_lines]=20000 [codec_parsers]=8192)

root="$(git rev-parse --show-toplevel)"
fuzz_dir="${root}/crates/qbit-prism-server/fuzz"
mkdir -p "${corpus}" "${out}/artifacts"
corpus="$(cd -- "${corpus}" && pwd)"
out="$(cd -- "${out}" && pwd)"
tools="$(mktemp -d)"
trap 'rm -rf "${tools}"' EXIT

rustup toolchain install "${NIGHTLY}" --profile minimal --no-self-update

asset="cargo-fuzz-${CARGO_FUZZ_VERSION}-x86_64-unknown-linux-musl.tar.gz"
curl --fail --location --silent --show-error --retry 12 --retry-delay 15 \
  --retry-max-time 180 --connect-timeout 10 --max-time 60 \
  --output "${tools}/${asset}" \
  "https://github.com/rust-fuzz/cargo-fuzz/releases/download/${CARGO_FUZZ_VERSION}/${asset}"
printf '%s  %s\n' "${CARGO_FUZZ_SHA256}" "${tools}/${asset}" | sha256sum --check --quiet
tar -xzf "${tools}/${asset}" -C "${tools}" cargo-fuzz
export PATH="${tools}:${PATH}"
cargo fuzz --version

cd "${fuzz_dir}"
# The release binary is a musl build, whose default target would be musl too.
cargo "+${NIGHTLY}" fuzz build -O --target x86_64-unknown-linux-gnu
bin_dir="${CARGO_TARGET_DIR:-${fuzz_dir}/target}/x86_64-unknown-linux-gnu/release"

pids=()
for target in "${TARGETS[@]}"; do
  mkdir -p "${corpus}/${target}" "${out}/artifacts/${target}"
  "${bin_dir}/${target}" "${corpus}/${target}" "${fuzz_dir}/seeds/${target}" \
    -dict="${fuzz_dir}/stratum.dict" -max_len="${MAX_LEN[${target}]}" \
    -max_total_time="${seconds}" -rss_limit_mb=2048 -malloc_limit_mb=512 \
    -timeout=30 -print_final_stats=1 \
    -artifact_prefix="${out}/artifacts/${target}/" \
    > "${out}/${target}.log" 2>&1 &
  pids+=("$!")
done

status=0
declare -A EXIT MERGE
for i in "${!TARGETS[@]}"; do
  code=0
  wait "${pids[${i}]}" || code=$?
  EXIT[${TARGETS[${i}]}]="${code}"
  (( code == 0 )) || status=1
done

{
  echo "### Stratum fuzzing, ${seconds} s per target on ${NIGHTLY}"
  echo
  echo "| target | exit | runs | exec/s | coverage (edges) | features | corpus | findings |"
  echo "| --- | --- | --- | --- | --- | --- | --- | --- |"
} > "${out}/summary.md"
for target in "${TARGETS[@]}"; do
  log="${out}/${target}.log"
  final_stat() { sed -n "s/^stat::$1: *//p" "${log}" | tail -n 1; }
  last="$(grep -E '^#[0-9]+' "${log}" | tail -n 1 || true)"
  cov="$(sed -nE 's/.* cov: ([0-9]+).*/\1/p' <<< "${last}")"
  ft="$(sed -nE 's/.* ft: ([0-9]+).*/\1/p' <<< "${last}")"
  findings="$(find "${out}/artifacts/${target}" -type f | wc -l)"
  # Keep only inputs that add coverage, so the cache does not grow nightly.
  # A merge that fails keeps the whole grown corpus instead of a partial one.
  merged="$(mktemp -d)"
  MERGE[${target}]=0
  "${bin_dir}/${target}" -merge=1 -max_len="${MAX_LEN[${target}]}" -rss_limit_mb=2048 \
    -malloc_limit_mb=512 -timeout=30 -artifact_prefix="${out}/artifacts/${target}/merge-" \
    "${merged}" "${corpus}/${target}" > "${out}/${target}.merge.log" 2>&1 || MERGE[${target}]=$?
  if [[ "${MERGE[${target}]}" == 0 ]]; then
    rm -rf "${corpus:?}/${target}"
    mv "${merged}" "${corpus}/${target}"
  else
    status=1
    rm -rf "${merged}"
  fi
  size="$(find "${corpus}/${target}" -type f | wc -l)"
  echo "| ${target} | ${EXIT[${target}]} | $(final_stat number_of_executed_units) | $(final_stat average_exec_per_sec) | ${cov:-?} | ${ft:-?} | ${size} | ${findings} |" \
    >> "${out}/summary.md"
done
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  cat "${out}/summary.md" >> "${GITHUB_STEP_SUMMARY}"
fi
cat "${out}/summary.md"
for target in "${TARGETS[@]}"; do
  if [[ "${EXIT[${target}]}" != 0 ]]; then
    echo "::error::${target} failed; its input is under artifacts/${target} and its log is ${target}.log"
    grep -m 5 -E 'invariant violated|panicked at|SUMMARY|ERROR' "${out}/${target}.log" || true
  fi
  if [[ "${MERGE[${target}]}" != 0 ]]; then
    echo "::error::${target}'s corpus merge failed (exit ${MERGE[${target}]}); its corpus was kept unmerged, see ${target}.merge.log and artifacts/${target}/merge-*"
    grep -m 5 -E 'invariant violated|panicked at|SUMMARY|ERROR' "${out}/${target}.merge.log" || true
  fi
done
exit "${status}"
