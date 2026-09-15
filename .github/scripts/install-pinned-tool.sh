#!/usr/bin/env bash
# Install one pinned CI tool from its GitHub release after checking its sha256.
#
# Usage: install-pinned-tool.sh <cargo-deny|hadolint> <destination directory>
#        install-pinned-tool.sh --cache-metadata <cargo-deny|hadolint>
#
# The versions and digests below are the pins; bump them together. Each digest
# is the sha256 of the release asset as published (cargo-deny also publishes a
# .sha256 file next to its tarball, which matched when the pin was set).
#
# Downloads ride out GitHub release outages: curl retries transient failures
# (timeouts, 408, 429, 5xx) every 15 seconds for up to 180 seconds, and the
# whole download is killed (SIGKILL) at 210 seconds. Other failures, such as a
# 404, fail at once.
#
# With PINNED_TOOL_CACHE_DIR set, the release asset as published is kept under
# <cache dir>/<tool>/<version>/<sha256>/ and installed from there on the next
# run without any download. A cached asset must be a regular file; it is copied
# to private scratch space and checked against the pinned sha256 before anything
# is extracted, installed or run. A bad entry fails without refetching, so
# remove it by hand.
# Unset means no cache.
#
# --cache-metadata writes the cache key and paths for actions/cache to
# GITHUB_OUTPUT (cache-dir, path and key), so the pins stay in this file. It
# needs PINNED_TOOL_CACHE_DIR, RUNNER_OS and RUNNER_ARCH.
set -euo pipefail

usage='usage: install-pinned-tool.sh <cargo-deny|hadolint> <destination directory>
       install-pinned-tool.sh --cache-metadata <cargo-deny|hadolint>'

metadata_only=0
if [[ "${1:-}" == --cache-metadata ]]; then
  metadata_only=1
  shift
  if [[ $# -ne 1 ]]; then
    printf '%s\n' "${usage}" >&2
    exit 2
  fi
fi

tool="${1:?${usage}}"
if (( metadata_only == 0 )); then
  destination="${2:?${usage}}"
fi

case "${tool}" in
  cargo-deny)
    version=0.20.2
    asset="cargo-deny-${version}-x86_64-unknown-linux-musl"
    url="https://github.com/EmbarkStudios/cargo-deny/releases/download/${version}/${asset}.tar.gz"
    sha256=9f12ed4c49936e09b48bf862b595cde2fe64fcbd9d74dfacac6131ca824c8d5f
    ;;
  hadolint)
    version=2.15.1
    url="https://github.com/hadolint/hadolint/releases/download/v${version}/hadolint-linux-x86_64"
    sha256=c7187db94eeeeca956519a6af171adc31453941a1e777961f6e680f697c8c507
    ;;
  *)
    echo "install-pinned-tool: unknown tool ${tool}; expected cargo-deny or hadolint" >&2
    exit 2
    ;;
esac

if [[ -n "${PINNED_TOOL_CACHE_DIR+set}" && -z "${PINNED_TOOL_CACHE_DIR}" ]]; then
  echo "install-pinned-tool: PINNED_TOOL_CACHE_DIR is set but empty; unset it to disable the cache" >&2
  exit 2
fi
cache_dir="${PINNED_TOOL_CACHE_DIR:-}"
cache_entry=""
cached_asset=""
if [[ -n "${cache_dir}" ]]; then
  cache_entry="${cache_dir}/${tool}/${version}/${sha256}"
  cached_asset="${cache_entry}/${url##*/}"
fi

if (( metadata_only == 1 )); then
  for name in PINNED_TOOL_CACHE_DIR RUNNER_OS RUNNER_ARCH GITHUB_OUTPUT; do
    if [[ -z "${!name:-}" ]]; then
      echo "install-pinned-tool: --cache-metadata needs ${name}" >&2
      exit 2
    fi
  done
  {
    printf 'cache-dir=%s\n' "${cache_dir}"
    printf 'path=%s\n' "${cache_entry}"
    printf 'key=pinned-tool-v1-%s-%s-%s-%s-%s\n' \
      "${RUNNER_OS}" "${RUNNER_ARCH}" "${tool}" "${version}" "${sha256}"
  } >> "${GITHUB_OUTPUT}"
  exit 0
fi

scratch="$(mktemp -d)"
publishing=""
cleanup() {
  rm -rf "${scratch}"
  if [[ -n "${publishing}" ]]; then
    rm -f "${publishing}"
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

verify() {
  printf '%s  %s\n' "${sha256}" "$1" | sha256sum --check --quiet
}

if [[ -n "${cached_asset}" && ( -e "${cached_asset}" || -L "${cached_asset}" ) ]]; then
  # Only a plain file is read: a link to a FIFO or device could block or fill
  # the disk before the check.
  if [[ -L "${cached_asset}" || ! -f "${cached_asset}" ]]; then
    echo "install-pinned-tool: cached ${cached_asset} is not a regular file; not using or refetching it, remove it and retry" >&2
    exit 1
  fi
  # Check the private copy, not the shared entry, so nothing can change what
  # was checked before it is used.
  cp -- "${cached_asset}" "${scratch}/download"
  if ! verify "${scratch}/download"; then
    echo "install-pinned-tool: cached ${cached_asset} does not match the pinned sha256; not using or refetching it, remove it and retry" >&2
    exit 1
  fi
  printf 'install-pinned-tool: using cached %s\n' "${cached_asset}"
else
  status=0
  timeout --signal=KILL 210 \
    curl --fail --location --silent --show-error \
      --retry 12 --retry-delay 15 --retry-max-time 180 \
      --connect-timeout 10 --max-time 30 \
      --output "${scratch}/download" "${url}" || status=$?
  if (( status == 137 )); then
    echo "install-pinned-tool: download of ${url} did not finish within 210 seconds" >&2
    exit 1
  elif (( status != 0 )); then
    echo "install-pinned-tool: download of ${url} failed (curl exit ${status}); transient errors were retried for up to 180 seconds" >&2
    exit 1
  fi
  if ! verify "${scratch}/download"; then
    echo "install-pinned-tool: ${url} does not match the pinned sha256" >&2
    exit 1
  fi
  if [[ -n "${cache_entry}" ]]; then
    # Publish under a temporary name in the same directory, check that copy,
    # then rename it into place, so readers and concurrent writers only ever
    # see a whole, checked asset.
    mkdir -p "${cache_entry}"
    publishing="$(mktemp "${cache_entry}/.publish.XXXXXX")"
    cp -- "${scratch}/download" "${publishing}"
    if ! verify "${publishing}"; then
      echo "install-pinned-tool: copy of ${url} into ${cache_entry} does not match the pinned sha256" >&2
      exit 1
    fi
    chmod 0644 "${publishing}"
    mv -fT -- "${publishing}" "${cached_asset}"
    publishing=""
    printf 'install-pinned-tool: cached %s\n' "${cached_asset}"
  fi
fi

mkdir -p "${destination}"
case "${tool}" in
  cargo-deny)
    tar -xzf "${scratch}/download" -C "${scratch}"
    install -m 0755 "${scratch}/${asset}/cargo-deny" "${destination}/cargo-deny"
    ;;
  hadolint)
    install -m 0755 "${scratch}/download" "${destination}/hadolint"
    ;;
esac
printf 'install-pinned-tool: %s %s -> %s\n' "${tool}" "${version}" "${destination}/${tool}"
"${destination}/${tool}" --version
