#!/usr/bin/env bash
# Install the pinned qbit release the PRISM native integration tests run
# against, checking its sha256 before anything is extracted.
#
# Usage: install-prism-qbit.sh <destination directory>
#
# The tarball is taken from the first of these that yields the pinned digest:
#   1. PRISM_QBIT_CACHE_DIR, when set (a cached copy whose digest mismatches is
#      deleted, not used);
#   2. the release's browser download route on github.com;
#   3. the release-asset API route on api.github.com, which fronts the same
#      storage through a different tier, so a github.com gateway error alone
#      does not fail the install (no token: the repository is public).
# Each route is fetched with a bounded retry budget. A copy that verified is
# left in the cache directory so the next run needs no network at all. The
# script fails only when every source is unusable.
#
# The version, asset id and digest below are the pin; bump them together. The
# digest is the sha256 GitHub publishes for the asset
# (gh api repos/Qbit-Org/qbit/releases/assets/<id> --jq .digest).
set -euo pipefail

destination="${1:?usage: install-prism-qbit.sh <destination directory>}"

version=1.0.0
asset_id=478569140
asset="qbit-${version}-x86_64-linux-gnu.tar.gz"
sha256=ae121af03263b55d530e3f3e8719a71362d0950cba82f54a2bc2d6c437a029b5

release_url="https://github.com/Qbit-Org/qbit/releases/download/v${version}/${asset}"
asset_api_url="https://api.github.com/repos/Qbit-Org/qbit/releases/assets/${asset_id}"
retries=5
retry_max_time=90
cache_dir="${PRISM_QBIT_CACHE_DIR:-}"

scratch="$(mktemp -d)"
trap 'rm -rf "${scratch}"' EXIT

log() {
  printf 'install-prism-qbit: %s\n' "$*" >&2
}

# verify <file>: succeed when the file's sha256 is the pin; log both on mismatch.
verify() {
  local actual
  actual="$(sha256sum "$1" | cut -d ' ' -f 1)"
  if [[ "${actual}" != "${sha256}" ]]; then
    log "digest mismatch for $1: expected ${sha256}, got ${actual}"
    return 1
  fi
}

# fetch <route name> <url> <output> [curl args...]: download with a bounded
# retry budget and verify; on any failure remove the output and fail.
fetch() {
  local route="$1" url="$2" output="$3"
  shift 3
  log "fetching ${asset} from the ${route} (${url})"
  if ! curl --fail --location --silent --show-error \
      --connect-timeout 30 --speed-limit 1024 --speed-time 60 \
      --retry "${retries}" --retry-max-time "${retry_max_time}" --retry-all-errors \
      "$@" --output "${output}" "${url}"; then
    log "the ${route} did not deliver ${asset}"
    rm -f "${output}"
    return 1
  fi
  if ! verify "${output}"; then
    rm -f "${output}"
    return 1
  fi
}

tarball=""
if [[ -n "${cache_dir}" ]] && [[ -f "${cache_dir}/${asset}" ]]; then
  if verify "${cache_dir}/${asset}"; then
    log "using the cached ${asset} from ${cache_dir}"
    tarball="${cache_dir}/${asset}"
  else
    log "discarding the cached ${asset} from ${cache_dir}"
    rm -f "${cache_dir}/${asset}"
  fi
fi

if [[ -z "${tarball}" ]]; then
  if fetch 'release download route' "${release_url}" "${scratch}/${asset}"; then
    tarball="${scratch}/${asset}"
  elif fetch 'release-asset API route' "${asset_api_url}" "${scratch}/${asset}" \
      --header 'Accept: application/octet-stream'; then
    tarball="${scratch}/${asset}"
  else
    log "no source delivered ${asset} with digest ${sha256}: the release download route and the release-asset API route both failed"
    exit 1
  fi
  if [[ -n "${cache_dir}" ]]; then
    mkdir -p "${cache_dir}"
    cp "${tarball}" "${cache_dir}/${asset}.partial"
    mv "${cache_dir}/${asset}.partial" "${cache_dir}/${asset}"
    tarball="${cache_dir}/${asset}"
  fi
fi

mkdir -p "${destination}"
tar -xzf "${tarball}" -C "${destination}"
qbitd="${destination}/qbit-${version}/bin/qbitd"
if [[ ! -x "${qbitd}" ]]; then
  log "${asset} did not contain an executable qbit-${version}/bin/qbitd"
  exit 1
fi
if [[ -n "${GITHUB_ENV:-}" ]]; then
  printf 'QBITD_BIN=%s\n' "${qbitd}" >> "${GITHUB_ENV}"
fi
log "qbit ${version} -> ${qbitd}"
