#!/usr/bin/env bash
set -euo pipefail

# Install Ubuntu packages on a CI runner and ride out Ubuntu mirror outages.
#
# - apt retries each download itself (Acquire::Retries).
# - If the whole update-and-install still fails, for example because the
#   archive refused connections or a mirror was mid-sync ("Hash Sum mismatch"),
#   this clears the downloaded package lists, waits, and tries again. After the
#   first failure it also replaces the runner's Ubuntu sources with the pinned
#   fallback mirrors in .github/apt/ubuntu-mirrors.txt, which apt tries in order.
# - Every attempt but the last treats a partially failed `apt-get update` as a
#   failure, instead of silently installing from stale indexes. The last attempt
#   keeps apt's default leniency, so this is never stricter than a plain
#   update-and-install.
# - With APT_INSTALL_CACHE_DIR set (the apt-install composite action sets it and
#   keeps it with actions/cache), the packages a successful install downloaded
#   are kept there. They seed the next install, and if every online attempt
#   fails they are installed directly with dpkg instead. Nothing the runner
#   already has at the same or a newer version is reinstalled or downgraded.
#
# Usage: bash .github/scripts/apt-install.sh package [package...]
#
# Settings:
#   APT_INSTALL_ATTEMPTS           default 4
#   APT_INSTALL_PAUSE_SECONDS      default 30, multiplied by the attempt number
#   APT_INSTALL_DEADLINE_SECONDS   default 480; after this, the next attempt is
#                                  the last
#   APT_INSTALL_FALLBACK_MIRRORS   default .github/apt/ubuntu-mirrors.txt
#   APT_INSTALL_CACHE_DIR          unset: no package cache

ATTEMPTS="${APT_INSTALL_ATTEMPTS:-4}"
PAUSE_SECONDS="${APT_INSTALL_PAUSE_SECONDS:-30}"
DEADLINE_SECONDS="${APT_INSTALL_DEADLINE_SECONDS:-480}"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
FALLBACK_MIRRORS="${APT_INSTALL_FALLBACK_MIRRORS:-${SCRIPT_DIR}/../apt/ubuntu-mirrors.txt}"
CACHE_DIR="${APT_INSTALL_CACHE_DIR:-}"
# apt downloads into a directory of its own, so the packages outlive the
# image's post-install clean-up and can be copied into the cache.
ARCHIVES=/var/cache/apt-install/archives

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
  -o "Dir::Cache::archives=${ARCHIVES}/"
  -o APT::Keep-Downloaded-Packages=true
)

shopt -s nullglob

# Print the installed version of a package, or nothing if it is not installed.
installed_version() {
  local status="" version=""
  # shellcheck disable=SC2016 # dpkg-query's own ${field} syntax, not a shell expansion
  read -r status version < <(dpkg-query --show \
    --showformat='${db:Status-Status} ${Version}\n' "$1" 2>/dev/null) || true
  if [[ "${status}" == installed ]]; then
    printf '%s' "${version}"
  fi
}

fallback_active=0

# Replace the runner's Ubuntu sources with the pinned fallback mirror list.
use_fallback_mirrors() {
  local arch codename source
  if (( fallback_active == 1 )); then
    return 0
  fi
  fallback_active=1
  if [[ ! -r "${FALLBACK_MIRRORS}" ]]; then
    printf '::warning::apt-install: no fallback mirror list at %s\n' "${FALLBACK_MIRRORS}"
    return 0
  fi
  arch="$(dpkg --print-architecture)"
  if [[ "${arch}" != amd64 && "${arch}" != i386 ]]; then
    printf '::warning::apt-install: the fallback mirrors carry amd64 and i386 only, not %s\n' "${arch}"
    return 0
  fi
  # shellcheck source=/dev/null
  codename="$(. /etc/os-release && printf '%s' "${VERSION_CODENAME:-}")"
  if [[ -z "${codename}" ]]; then
    printf '::warning::apt-install: cannot tell the Ubuntu release, so the fallback mirrors are not used\n'
    return 0
  fi
  for source in /etc/apt/sources.list /etc/apt/sources.list.d/*.list /etc/apt/sources.list.d/*.sources; do
    if [[ -f "${source}" ]] &&
      grep -q -E '(archive|security)\.ubuntu\.com|mirror\+file:[^[:space:]]*ubuntu' "${source}"; then
      "${SUDO[@]}" mv -- "${source}" "${source}.apt-install-disabled"
    fi
  done
  "${SUDO[@]}" install -m 0644 -- "${FALLBACK_MIRRORS}" /etc/apt/apt-install-ubuntu-mirrors.txt
  printf 'Types: deb\nURIs: mirror+file:/etc/apt/apt-install-ubuntu-mirrors.txt\nSuites: %s %s-updates %s-backports %s-security\nComponents: main restricted universe multiverse\nSigned-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg\n' \
    "${codename}" "${codename}" "${codename}" "${codename}" |
    "${SUDO[@]}" tee /etc/apt/sources.list.d/apt-install-fallback.sources >/dev/null
  # apt retries a failed file on the same mirror before moving to the next one,
  # so its growing retry delay would keep an unreachable mirror blocking every
  # download for minutes. Retry immediately instead.
  APT_OPTIONS+=(-o Acquire::Retries::Delay=false)
  printf '::warning::apt-install: switched the Ubuntu sources to the fallback mirrors in .github/apt/ubuntu-mirrors.txt\n'
}

# Replace the cache with exactly the downloaded packages this install used.
save_cache() {
  local fresh="${CACHE_DIR}.new" deb package kept=0
  rm -rf -- "${fresh}"
  mkdir -p -- "${fresh}/archives"
  for deb in "${ARCHIVES}"/*.deb; do
    package="$(dpkg-deb --field "${deb}" Package)"
    if [[ "$(installed_version "${package}")" == "$(dpkg-deb --field "${deb}" Version)" ]]; then
      cp -- "${deb}" "${fresh}/archives/"
      kept=$(( kept + 1 ))
    fi
  done
  printf 'saved %s: %d packages for %s\n' "$(date -u +%Y-%m-%dT%H:%MZ)" "${kept}" "$*" \
    > "${fresh}/manifest.txt"
  rm -rf -- "${CACHE_DIR}"
  mv -- "${fresh}" "${CACHE_DIR}"
}

# Install the cached packages with dpkg, skipping any the runner already has
# at the same or a newer version, then check every requested package is there.
install_from_cache() {
  local debs=() deb package version current
  for deb in "${CACHE_DIR}"/archives/*.deb; do
    package="$(dpkg-deb --field "${deb}" Package)"
    version="$(dpkg-deb --field "${deb}" Version)"
    current="$(installed_version "${package}")"
    if [[ -n "${current}" ]] && dpkg --compare-versions "${current}" ge "${version}"; then
      continue
    fi
    debs+=("${deb}")
  done
  printf '::warning::apt-install: the Ubuntu mirrors are unreachable; installing %d cached packages (%s)\n' \
    "${#debs[@]}" "$(head -n 1 "${CACHE_DIR}/manifest.txt" 2>/dev/null || printf 'no manifest')"
  if (( ${#debs[@]} > 0 )); then
    "${SUDO[@]}" env DEBIAN_FRONTEND=noninteractive dpkg --install "${debs[@]}" || return 1
  fi
  for package in "$@"; do
    if [[ -z "$(installed_version "${package%%=*}")" ]]; then
      return 1
    fi
  done
}

"${SUDO[@]}" mkdir -p "${ARCHIVES}/partial"
if [[ -n "${CACHE_DIR}" ]]; then
  cached=("${CACHE_DIR}"/archives/*.deb)
  if (( ${#cached[@]} > 0 )); then
    # apt reuses a cached package whose checksum still matches the index.
    "${SUDO[@]}" cp -- "${cached[@]}" "${ARCHIVES}/"
  fi
fi

attempt=1
while true; do
  last=0
  if (( attempt >= ATTEMPTS || SECONDS >= DEADLINE_SECONDS )); then
    last=1
  fi
  update_options=()
  if (( last == 0 )); then
    update_options=(--error-on=any)
  fi

  if "${SUDO[@]}" apt-get "${APT_OPTIONS[@]}" "${update_options[@]}" update &&
    "${SUDO[@]}" env DEBIAN_FRONTEND=noninteractive \
      apt-get "${APT_OPTIONS[@]}" install -y --no-install-recommends "$@"; then
    if [[ -n "${CACHE_DIR}" ]]; then
      save_cache "$@" ||
        printf '::warning::apt-install: could not update the package cache in %s\n' "${CACHE_DIR}"
    fi
    exit 0
  fi

  if (( last == 1 )); then
    break
  fi

  pause=$(( PAUSE_SECONDS * attempt ))
  printf '::warning::apt-install: attempt %d of %d failed; clearing package lists and retrying in %ds\n' \
    "${attempt}" "${ATTEMPTS}" "${pause}"
  use_fallback_mirrors
  "${SUDO[@]}" find /var/lib/apt/lists -mindepth 1 -maxdepth 1 ! -name lock -exec rm -rf {} +
  sleep "${pause}"
  attempt=$(( attempt + 1 ))
done

if [[ -n "${CACHE_DIR}" && -d "${CACHE_DIR}/archives" ]] && install_from_cache "$@"; then
  exit 0
fi

printf '::error::apt-install: installing %s failed after %d attempts%s\n' \
  "$*" "${attempt}" "${CACHE_DIR:+, and the package cache could not supply it}"
exit 1
