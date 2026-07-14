#!/usr/bin/env bash
# shellcheck disable=SC1090
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_DEPLOY_ENV_FILE="${DEPLOY_ENV_FILE:-}"
ENV_QBIT_PROVIDER="${QBIT_PROVIDER:-}"
ENV_QBIT_PRODUCTION="${QBIT_PRODUCTION:-}"
ENV_QBIT_SRC_DIR="${QBIT_SRC_DIR:-}"
ENV_QBIT_GIT_URL="${QBIT_GIT_URL:-}"
ENV_QBIT_GIT_REF="${QBIT_GIT_REF:-}"
ENV_QBIT_GIT_COMMIT="${QBIT_GIT_COMMIT:-}"
ENV_QBIT_CHAIN="${QBIT_CHAIN:-}"
source "${ROOT_DIR}/.env.example"
if [[ -f "${ROOT_DIR}/config/upstream.env" ]]; then
  source "${ROOT_DIR}/config/upstream.env"
else
  source "${ROOT_DIR}/config/upstream.env.example"
fi
DEPLOY_ENV_FILE="${ENV_DEPLOY_ENV_FILE}"
if [[ -n "${DEPLOY_ENV_FILE:-}" ]]; then
  if [[ "${DEPLOY_ENV_FILE}" != /* ]]; then
    DEPLOY_ENV_FILE="${ROOT_DIR}/${DEPLOY_ENV_FILE}"
  fi
  if [[ ! -f "${DEPLOY_ENV_FILE}" ]]; then
    printf 'prepare-qbit-source: DEPLOY_ENV_FILE does not exist: %s\n' "${DEPLOY_ENV_FILE}" >&2
    exit 1
  fi
  source "${DEPLOY_ENV_FILE}"
elif [[ -f "${ROOT_DIR}/.env" ]]; then
  source "${ROOT_DIR}/.env"
fi
if [[ -n "${ENV_QBIT_PROVIDER}" ]]; then
  QBIT_PROVIDER="${ENV_QBIT_PROVIDER}"
fi
if [[ -n "${ENV_QBIT_PRODUCTION}" ]]; then
  QBIT_PRODUCTION="${ENV_QBIT_PRODUCTION}"
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
if [[ -n "${ENV_QBIT_CHAIN}" ]]; then
  QBIT_CHAIN="${ENV_QBIT_CHAIN}"
fi

fail() {
  printf 'prepare-qbit-source: %s\n' "$1" >&2
  exit 1
}

ascii_lower() {
  printf '%s' "${1:-}" | LC_ALL=C tr '[:upper:]' '[:lower:]'
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
  local source_commit="${2:-}"
  local dest_dir="${ROOT_DIR}/generated/qbit-src"
  local staged_source="${source_dir}"
  local archive_dir=""

  require_cmd rsync
  mkdir -p "${ROOT_DIR}/generated"
  if [[ -n "${source_commit}" ]]; then
    require_cmd git
    require_cmd tar
    archive_dir="$(mktemp -d "${ROOT_DIR}/generated/qbit-archive.XXXXXX")"
    trap 'rm -rf "${archive_dir}"' RETURN
    git -C "${source_dir}" archive --format=tar "${source_commit}" | tar -xf - -C "${archive_dir}"
    staged_source="${archive_dir}"
    check_qbit_tree "${staged_source}"
  fi
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
    "${staged_source}/" "${dest_dir}/"
  if [[ -n "${source_commit}" ]]; then
    printf '%s\n' "$(ascii_lower "${source_commit}")" > "${dest_dir}/.qbit-source-commit"
  fi
  printf '%s\n' "${dest_dir}"
}

requires_pinned_source() {
  case "${QBIT_CHAIN:-regtest}" in
    main|mainnet) return 0 ;;
  esac
  case "${QBIT_PRODUCTION:-0}" in
    1|true|TRUE|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

resolve_commit() {
  local source_dir="$1"
  local requested_commit="$2"
  local requested_commit_normalized
  local resolved_commit

  [[ -d "${source_dir}/.git" ]] || fail "pinned qbit source must be a Git checkout: ${source_dir}"
  if ! resolved_commit="$(git -C "${source_dir}" rev-parse --verify "${requested_commit}^{commit}" 2>/dev/null)"; then
    fail "QBIT_GIT_COMMIT is not present in qbit source: ${requested_commit}"
  fi
  resolved_commit="$(ascii_lower "${resolved_commit}")"
  requested_commit_normalized="$(ascii_lower "${requested_commit}")"
  [[ "${resolved_commit}" == "${requested_commit_normalized}" ]] || fail \
    "qbit source resolved ${resolved_commit}, expected QBIT_GIT_COMMIT ${requested_commit_normalized}"
  printf '%s\n' "${resolved_commit}"
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
if requires_pinned_source && [[ ! "${QBIT_GIT_COMMIT:-}" =~ ^[[:xdigit:]]{40}$ ]]; then
  fail "production source staging requires QBIT_GIT_COMMIT as exactly 40 hex characters"
fi

resolved_commit=""
if [[ -n "${QBIT_GIT_COMMIT:-}" ]]; then
  resolved_commit="$(resolve_commit "${source_dir}" "${QBIT_GIT_COMMIT}")"
elif [[ "${QBIT_PROVIDER}" == "git" ]]; then
  resolved_commit="$(git -C "${source_dir}" rev-parse --verify HEAD)"
fi
stage_tree "${source_dir}" "${resolved_commit}"
