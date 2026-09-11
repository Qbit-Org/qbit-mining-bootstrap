#!/usr/bin/env bash
set -euo pipefail

# Install Ubuntu packages on a CI runner and ride out short mirror outages.
#
# apt retries each download itself (Acquire::Retries). If the whole
# update-and-install still fails, for example because the archive refused
# connections or a mirror was mid-sync ("Hash Sum mismatch"), this clears the
# downloaded package lists, waits, and tries again. Every attempt but the last
# treats a partially failed `apt-get update` as a failure, instead of silently
# installing from stale indexes. The last attempt keeps apt's default leniency,
# so this is never stricter than a plain update-and-install.
#
# Usage: bash .github/scripts/apt-install.sh package [package...]

ATTEMPTS="${APT_INSTALL_ATTEMPTS:-4}"
PAUSE_SECONDS="${APT_INSTALL_PAUSE_SECONDS:-30}"

if [[ $# -eq 0 ]]; then
  printf 'apt-install: no packages given\n' >&2
  exit 2
fi

SUDO=()
if [[ "$(id -u)" -ne 0 ]]; then
  SUDO=(sudo)
fi

APT_OPTIONS=(
  -o Acquire::Retries=5
  -o Acquire::http::Timeout=30
  -o Acquire::https::Timeout=30
)

attempt=1
while true; do
  update_options=()
  if (( attempt < ATTEMPTS )); then
    update_options=(--error-on=any)
  fi

  if "${SUDO[@]}" apt-get "${APT_OPTIONS[@]}" "${update_options[@]}" update &&
    "${SUDO[@]}" env DEBIAN_FRONTEND=noninteractive \
      apt-get "${APT_OPTIONS[@]}" install -y --no-install-recommends "$@"; then
    exit 0
  fi

  if (( attempt >= ATTEMPTS )); then
    printf '::error::apt-install: installing %s failed after %d attempts\n' "$*" "${ATTEMPTS}"
    exit 1
  fi

  pause=$(( PAUSE_SECONDS * attempt ))
  printf '::warning::apt-install: attempt %d of %d failed; clearing package lists and retrying in %ds\n' \
    "${attempt}" "${ATTEMPTS}" "${pause}"
  "${SUDO[@]}" find /var/lib/apt/lists -mindepth 1 -maxdepth 1 ! -name lock -exec rm -rf {} +
  sleep "${pause}"
  attempt=$(( attempt + 1 ))
done
