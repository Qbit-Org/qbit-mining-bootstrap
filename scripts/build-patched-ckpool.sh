#!/usr/bin/env bash
# Stage the pinned upstream ckpool, apply the qbit patches in Dockerfile
# order, and build the ckpool binary for the live rejects-observability
# tests. Prints the built binary path as the last stdout line.
#
# The staging tree is cached and rebuilt only when the upstream pin or any
# patch content changes. Override locations with:
#   CKPOOL_BUILD_DIR   staging/cache directory (default: generated/ckpool-build)
#   CKPOOL_GIT_URL/CKPOOL_GIT_REF   upstream pin (default: config/upstream.env)

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

UPSTREAM_ENV_FILE="${ROOT_DIR}/config/upstream.env"
if [[ ! -f "${UPSTREAM_ENV_FILE}" ]]; then
    UPSTREAM_ENV_FILE="${ROOT_DIR}/config/upstream.env.example"
fi

upstream_value() {
    local key="$1"
    sed -n "s/^${key}=//p" "${UPSTREAM_ENV_FILE}" | tail -n 1
}

CKPOOL_GIT_URL="${CKPOOL_GIT_URL:-$(upstream_value CKPOOL_GIT_URL)}"
CKPOOL_GIT_REF="${CKPOOL_GIT_REF:-$(upstream_value CKPOOL_GIT_REF)}"
CKPOOL_BUILD_DIR="${CKPOOL_BUILD_DIR:-${ROOT_DIR}/generated/ckpool-build}"

if [[ -z "${CKPOOL_GIT_URL}" || -z "${CKPOOL_GIT_REF}" ]]; then
    echo "error: CKPOOL_GIT_URL/CKPOOL_GIT_REF not resolvable from ${UPSTREAM_ENV_FILE}" >&2
    exit 1
fi

PATCHES=(
    "${ROOT_DIR}/docker/ckpool/qbit-regtest.patch"
    "${ROOT_DIR}/docker/ckpool/qbit-signet-gbt.patch"
    "${ROOT_DIR}/docker/ckpool/qbit-rejects-observability.patch"
)

SRC_DIR="${CKPOOL_BUILD_DIR}/ckpool"
STAMP_FILE="${CKPOOL_BUILD_DIR}/build.stamp"
BINARY="${SRC_DIR}/src/ckpool"

stamp() {
    {
        printf '%s\n%s\n' "${CKPOOL_GIT_URL}" "${CKPOOL_GIT_REF}"
        cat "${PATCHES[@]}"
    } | sha256sum | cut -d' ' -f1
}

WANT_STAMP="$(stamp)"
if [[ -x "${BINARY}" && -f "${STAMP_FILE}" && "$(cat "${STAMP_FILE}")" == "${WANT_STAMP}" ]]; then
    echo "ckpool build up to date: ${BINARY}" >&2
    echo "${BINARY}"
    exit 0
fi

echo "staging ckpool ${CKPOOL_GIT_REF} from ${CKPOOL_GIT_URL}" >&2
rm -rf "${SRC_DIR}"
mkdir -p "${SRC_DIR}"
git -C "${SRC_DIR}" init --quiet .
git -C "${SRC_DIR}" remote add origin "${CKPOOL_GIT_URL}"
git -C "${SRC_DIR}" fetch --quiet --depth 1 origin "${CKPOOL_GIT_REF}"
git -C "${SRC_DIR}" checkout --quiet FETCH_HEAD

for patch in "${PATCHES[@]}"; do
    echo "applying $(basename "${patch}")" >&2
    git -C "${SRC_DIR}" apply "${patch}"
done

(
    cd "${SRC_DIR}"
    ./autogen.sh
    ./configure
    make -C src/jansson-2.14/src -j"$(nproc)"
    make -C src -j"$(nproc)" ckpool
) >&2

test -x "${BINARY}"
printf '%s' "${WANT_STAMP}" > "${STAMP_FILE}"
echo "built ${BINARY}" >&2
echo "${BINARY}"
