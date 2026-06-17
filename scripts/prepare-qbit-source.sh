#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_QBIT_PROVIDER="${QBIT_PROVIDER:-}"
ENV_QBIT_SRC_DIR="${QBIT_SRC_DIR:-}"
ENV_QBIT_GIT_URL="${QBIT_GIT_URL:-}"
ENV_QBIT_GIT_REF="${QBIT_GIT_REF:-}"
ENV_QBIT_GIT_COMMIT="${QBIT_GIT_COMMIT:-}"
source "${ROOT_DIR}/.env.example"
if [[ -f "${ROOT_DIR}/config/upstream.env" ]]; then
  source "${ROOT_DIR}/config/upstream.env"
else
  source "${ROOT_DIR}/config/upstream.env.example"
fi
if [[ -f "${ROOT_DIR}/.env" ]]; then
  source "${ROOT_DIR}/.env"
fi
if [[ -n "${ENV_QBIT_PROVIDER}" ]]; then
  QBIT_PROVIDER="${ENV_QBIT_PROVIDER}"
fi
if [[ -n "${ENV_QBIT_SRC_DIR}" ]]; then
  QBIT_SRC_DIR="${ENV_QBIT_SRC_DIR}"
fi
if [[ -n "${ENV_QBIT_GIT_URL}" ]]; then
  QBIT_GIT_URL="${ENV_QBIT_GIT_URL}"
fi
if [[ -n "${ENV_QBIT_GIT_REF}" ]]; then
  QBIT_GIT_REF="${ENV_QBIT_GIT_REF}"
fi
if [[ -n "${ENV_QBIT_GIT_COMMIT}" ]]; then
  QBIT_GIT_COMMIT="${ENV_QBIT_GIT_COMMIT}"
fi

fail() {
  printf 'prepare-qbit-source: %s\n' "$1" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "$1 is required"
}

check_absolute_dir() {
  local path="$1"
  local label="$2"

  [[ -n "${path}" ]] || fail "${label} is required"
  [[ "${path}" = /* ]] || fail "${label} must be an absolute path: ${path}"
  [[ -d "${path}" ]] || fail "${label} does not exist: ${path}"
}

check_qbit_tree() {
  local path="$1"

  [[ -f "${path}/CMakeLists.txt" ]] || fail "qbit source is missing CMakeLists.txt: ${path}"
  [[ -f "${path}/src/CMakeLists.txt" ]] || fail "qbit source is missing src/CMakeLists.txt: ${path}"
  [[ -f "${path}/test/functional/test_framework/auxpow.py" ]] || fail "qbit source is missing functional AuxPoW helpers: ${path}"
}

stage_tree() {
  local source_dir="$1"
  local dest_dir="${ROOT_DIR}/generated/qbit-src"

  require_cmd rsync
  mkdir -p "${dest_dir}"
  rsync -a --delete --delete-excluded \
    --exclude='.git/' \
    --exclude='build/' \
    --exclude='build*/' \
    --exclude='cmake-build*/' \
    --exclude='depends/' \
    --exclude='.cache/' \
    --exclude='__pycache__/' \
    --exclude='*.pyc' \
    --exclude='.DS_Store' \
    "${source_dir}/" "${dest_dir}/"
  printf '%s\n' "${dest_dir}"
}

resolve_git_source() {
  local cache_dir="${ROOT_DIR}/generated/qbit-git-cache"
  mkdir -p "${ROOT_DIR}/generated"

  require_cmd git
  [[ -n "${QBIT_GIT_URL:-}" ]] || fail "QBIT_GIT_URL must be set when QBIT_PROVIDER=git"
  [[ -n "${QBIT_GIT_REF:-}" ]] || fail "QBIT_GIT_REF must be set when QBIT_PROVIDER=git"

  if [[ ! -d "${cache_dir}/.git" ]]; then
    git clone "${QBIT_GIT_URL}" "${cache_dir}" >/dev/null 2>&1
  fi

  git -C "${cache_dir}" remote set-url origin "${QBIT_GIT_URL}"
  git -C "${cache_dir}" fetch --tags origin "${QBIT_GIT_REF}" >/dev/null 2>&1

  if [[ -n "${QBIT_GIT_COMMIT:-}" ]]; then
    git -C "${cache_dir}" fetch --depth 1 origin "${QBIT_GIT_COMMIT}" >/dev/null 2>&1 || true
    git -C "${cache_dir}" checkout --force --detach "${QBIT_GIT_COMMIT}" >/dev/null 2>&1
  else
    git -C "${cache_dir}" checkout --force --detach FETCH_HEAD >/dev/null 2>&1
  fi

  printf '%s\n' "${cache_dir}"
}

case "${QBIT_PROVIDER}" in
  source)
    check_absolute_dir "${QBIT_SRC_DIR}" "QBIT_SRC_DIR"
    source_dir="${QBIT_SRC_DIR}"
    ;;
  git)
    source_dir="$(resolve_git_source)"
    ;;
  *)
    fail "unsupported QBIT_PROVIDER '${QBIT_PROVIDER}'; expected 'source' or 'git'"
    ;;
esac

check_qbit_tree "${source_dir}"
stage_tree "${source_dir}"
