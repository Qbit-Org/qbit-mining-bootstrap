#!/usr/bin/env bash
# Install one pinned CI tool from its GitHub release after checking its sha256.
#
# Usage: install-pinned-tool.sh <cargo-deny|hadolint> <destination directory>
#
# The versions and digests below are the pins; bump them together. Each digest
# is the sha256 of the release asset as published (cargo-deny also publishes a
# .sha256 file next to its tarball, which matched when the pin was set).
set -euo pipefail

tool="${1:?usage: install-pinned-tool.sh <cargo-deny|hadolint> <destination directory>}"
destination="${2:?usage: install-pinned-tool.sh <cargo-deny|hadolint> <destination directory>}"

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

scratch="$(mktemp -d)"
trap 'rm -rf "${scratch}"' EXIT
curl --fail --location --retry 3 --silent --show-error \
  --output "${scratch}/download" "${url}"
printf '%s  %s\n' "${sha256}" "${scratch}/download" | sha256sum --check --quiet
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
