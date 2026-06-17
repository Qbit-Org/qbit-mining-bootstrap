#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_QBIT_PROVIDER="${QBIT_PROVIDER:-}"
ENV_QBIT_SRC_DIR="${QBIT_SRC_DIR:-}"
ENV_QBIT_BIN_DIR="${QBIT_BIN_DIR:-}"
ENV_QBIT_GIT_URL="${QBIT_GIT_URL:-}"
ENV_QBIT_GIT_REF="${QBIT_GIT_REF:-}"
ENV_QBIT_GIT_COMMIT="${QBIT_GIT_COMMIT:-}"
ENV_QBIT_CHAIN="${QBIT_CHAIN:-}"
ENV_QBIT_CHAIN_FLAG="${QBIT_CHAIN_FLAG:-}"
ENV_QBIT_NODE_EXTRA_ARG="${QBIT_NODE_EXTRA_ARG:-}"
ENV_CKPOOL_GIT_URL="${CKPOOL_GIT_URL:-}"
ENV_CKPOOL_GIT_REF="${CKPOOL_GIT_REF:-}"
ENV_CKPOOL_MINDIFF="${CKPOOL_MINDIFF:-}"
ENV_CKPOOL_STARTDIFF="${CKPOOL_STARTDIFF:-}"
ENV_CKPOOL_PUBLIC_DIFF_POLICY="${CKPOOL_PUBLIC_DIFF_POLICY:-}"
ENV_CPUMINER_GIT_URL="${CPUMINER_GIT_URL:-}"
ENV_CPUMINER_GIT_REF="${CPUMINER_GIT_REF:-}"
ENV_BITCOIN_RELEASE_VERSION="${BITCOIN_RELEASE_VERSION:-}"
ENV_BITCOIN_RELEASE_BASE_URL="${BITCOIN_RELEASE_BASE_URL:-}"
ENV_BITCOIN_RELEASE_URL="${BITCOIN_RELEASE_URL:-}"
ENV_BITCOIN_CHAIN_FLAG="${BITCOIN_CHAIN_FLAG:-}"
ENV_BITCOIN_RPC_PORT="${BITCOIN_RPC_PORT:-}"
ENV_BITCOIN_P2P_PORT="${BITCOIN_P2P_PORT:-}"
ENV_BITCOIN_DNSSEED="${BITCOIN_DNSSEED:-}"
ENV_BITCOIN_DISCOVER="${BITCOIN_DISCOVER:-}"
ENV_BITCOIN_NODE_EXTRA_ARGS="${BITCOIN_NODE_EXTRA_ARGS:-}"
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
if [[ -n "${ENV_QBIT_BIN_DIR}" ]]; then
  QBIT_BIN_DIR="${ENV_QBIT_BIN_DIR}"
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
if [[ -n "${ENV_QBIT_CHAIN_FLAG}" ]]; then
  QBIT_CHAIN_FLAG="${ENV_QBIT_CHAIN_FLAG}"
fi
if [[ -n "${ENV_QBIT_NODE_EXTRA_ARG}" ]]; then
  QBIT_NODE_EXTRA_ARG="${ENV_QBIT_NODE_EXTRA_ARG}"
fi
if [[ -n "${ENV_CKPOOL_GIT_URL}" ]]; then
  CKPOOL_GIT_URL="${ENV_CKPOOL_GIT_URL}"
fi
if [[ -n "${ENV_CKPOOL_GIT_REF}" ]]; then
  CKPOOL_GIT_REF="${ENV_CKPOOL_GIT_REF}"
fi
if [[ -n "${ENV_CKPOOL_MINDIFF}" ]]; then
  CKPOOL_MINDIFF="${ENV_CKPOOL_MINDIFF}"
fi
if [[ -n "${ENV_CKPOOL_STARTDIFF}" ]]; then
  CKPOOL_STARTDIFF="${ENV_CKPOOL_STARTDIFF}"
fi
if [[ -n "${ENV_CKPOOL_PUBLIC_DIFF_POLICY}" ]]; then
  CKPOOL_PUBLIC_DIFF_POLICY="${ENV_CKPOOL_PUBLIC_DIFF_POLICY}"
fi
if [[ -n "${ENV_CPUMINER_GIT_URL}" ]]; then
  CPUMINER_GIT_URL="${ENV_CPUMINER_GIT_URL}"
fi
if [[ -n "${ENV_CPUMINER_GIT_REF}" ]]; then
  CPUMINER_GIT_REF="${ENV_CPUMINER_GIT_REF}"
fi
if [[ -n "${ENV_BITCOIN_RELEASE_VERSION}" ]]; then
  BITCOIN_RELEASE_VERSION="${ENV_BITCOIN_RELEASE_VERSION}"
fi
if [[ -n "${ENV_BITCOIN_RELEASE_BASE_URL}" ]]; then
  BITCOIN_RELEASE_BASE_URL="${ENV_BITCOIN_RELEASE_BASE_URL}"
fi
if [[ -n "${ENV_BITCOIN_RELEASE_URL}" ]]; then
  BITCOIN_RELEASE_URL="${ENV_BITCOIN_RELEASE_URL}"
fi
if [[ -n "${ENV_BITCOIN_CHAIN_FLAG}" ]]; then
  BITCOIN_CHAIN_FLAG="${ENV_BITCOIN_CHAIN_FLAG}"
fi
if [[ -n "${ENV_BITCOIN_RPC_PORT}" ]]; then
  BITCOIN_RPC_PORT="${ENV_BITCOIN_RPC_PORT}"
fi
if [[ -n "${ENV_BITCOIN_P2P_PORT}" ]]; then
  BITCOIN_P2P_PORT="${ENV_BITCOIN_P2P_PORT}"
fi
if [[ -n "${ENV_BITCOIN_DNSSEED}" ]]; then
  BITCOIN_DNSSEED="${ENV_BITCOIN_DNSSEED}"
fi
if [[ -n "${ENV_BITCOIN_DISCOVER}" ]]; then
  BITCOIN_DISCOVER="${ENV_BITCOIN_DISCOVER}"
fi
if [[ -n "${ENV_BITCOIN_NODE_EXTRA_ARGS}" ]]; then
  BITCOIN_NODE_EXTRA_ARGS="${ENV_BITCOIN_NODE_EXTRA_ARGS}"
fi
if [[ -n "${QBIT_SRC_DIR_OVERRIDE:-}" ]]; then
  QBIT_SRC_DIR="${QBIT_SRC_DIR_OVERRIDE}"
fi

fail() {
  printf 'doctor: %s\n' "$1" >&2
  exit 1
}

canonical_dir() {
  (
    cd "$1" >/dev/null 2>&1
    pwd -P
  )
}

print_git_ref() {
  local path="$1"
  local label="$2"
  local top

  if ! top="$(git -C "${path}" rev-parse --show-toplevel 2>/dev/null)"; then
    printf 'doctor: %s is not a git worktree, continuing\n' "${label}"
    return
  fi

  if [[ "$(canonical_dir "${path}")" != "$(canonical_dir "${top}")" ]]; then
    printf 'doctor: %s is not a git worktree root, continuing\n' "${label}"
    return
  fi

  printf 'doctor: %s ref %s\n' "${label}" "$(git -C "${path}" rev-parse HEAD)"
}

check_absolute_dir() {
  local path="$1"
  local label="$2"

  [[ -n "${path}" ]] || fail "${label} is required"
  [[ "${path}" = /* ]] || fail "${label} must be an absolute path: ${path}"
  [[ -d "${path}" ]] || fail "${label} does not exist: ${path}"
}

check_bool_env() {
  local name="$1"
  local value="${!name:-}"

  case "${value}" in
    ""|0|1) ;;
    *) fail "${name} must be 0 or 1, got '${value}'" ;;
  esac
}

is_public_qbit_chain() {
  case "${1}" in
    mainnet|testnet|testnet3|testnet4|signet) return 0 ;;
    *) return 1 ;;
  esac
}

command -v docker >/dev/null 2>&1 || fail "docker is required"
docker info >/dev/null 2>&1 || fail "docker daemon is not reachable"

case "${QBIT_PROVIDER}" in
  source)
    check_absolute_dir "${QBIT_SRC_DIR}" "QBIT_SRC_DIR"
    ;;
  git)
    command -v git >/dev/null 2>&1 || fail "git is required when QBIT_PROVIDER=git"
    [[ -n "${QBIT_GIT_URL:-}" ]] || fail "QBIT_GIT_URL must be set when QBIT_PROVIDER=git"
    [[ -n "${QBIT_GIT_REF:-}" ]] || fail "QBIT_GIT_REF must be set when QBIT_PROVIDER=git"
    ;;
  *)
    fail "QBIT_PROVIDER must be 'source' or 'git', got '${QBIT_PROVIDER}'"
    ;;
esac

case "${QBIT_CHAIN:-regtest}" in
  mainnet|testnet|testnet3|testnet4|signet|regtest) ;;
  *) fail "QBIT_CHAIN must be mainnet, testnet, testnet3, testnet4, signet, or regtest; got '${QBIT_CHAIN}'" ;;
esac

case "${QBIT_CHAIN:-regtest}" in
  regtest)
    [[ "${QBIT_CHAIN_FLAG:--regtest}" == "-regtest" ]] || fail "QBIT_CHAIN=regtest requires QBIT_CHAIN_FLAG=-regtest"
    ;;
  testnet)
    [[ "${QBIT_CHAIN_FLAG:-}" == "-testnet" ]] || fail "QBIT_CHAIN=testnet requires QBIT_CHAIN_FLAG=-testnet"
    ;;
  testnet3)
    [[ "${QBIT_CHAIN_FLAG:-}" == "-testnet3" ]] || fail "QBIT_CHAIN=testnet3 requires QBIT_CHAIN_FLAG=-testnet3"
    ;;
  testnet4)
    [[ "${QBIT_CHAIN_FLAG:-}" == "-testnet4" ]] || fail "QBIT_CHAIN=testnet4 requires QBIT_CHAIN_FLAG=-testnet4"
    ;;
  signet)
    [[ "${QBIT_CHAIN_FLAG:-}" == "-signet" ]] || fail "QBIT_CHAIN=signet requires QBIT_CHAIN_FLAG=-signet"
    ;;
  mainnet)
    case "${QBIT_CHAIN_FLAG:-}" in
      -regtest|-testnet|-testnet3|-testnet4|-signet)
        fail "QBIT_CHAIN=mainnet cannot use test-chain QBIT_CHAIN_FLAG=${QBIT_CHAIN_FLAG}"
        ;;
    esac
    ;;
esac

case "${CKPOOL_PUBLIC_DIFF_POLICY:-explicit}" in
  explicit|require|required|permissive|allow-defaults|defaults) ;;
  *) fail "CKPOOL_PUBLIC_DIFF_POLICY must be explicit or permissive, got '${CKPOOL_PUBLIC_DIFF_POLICY}'" ;;
esac

if is_public_qbit_chain "${QBIT_CHAIN:-regtest}"; then
  case "${CKPOOL_PUBLIC_DIFF_POLICY:-explicit}" in
    explicit|require|required)
      [[ -n "${CKPOOL_MINDIFF:-}" ]] || fail "QBIT_CHAIN=${QBIT_CHAIN} requires explicit CKPOOL_MINDIFF"
      [[ -n "${CKPOOL_STARTDIFF:-}" ]] || fail "QBIT_CHAIN=${QBIT_CHAIN} requires explicit CKPOOL_STARTDIFF"
      ;;
  esac
fi

if [[ -n "${QBIT_SRC_DIR:-}" ]]; then
  check_absolute_dir "${QBIT_SRC_DIR}" "QBIT_SRC_DIR"
  [[ -f "${QBIT_SRC_DIR}/CMakeLists.txt" ]] || fail "QBIT_SRC_DIR does not look like a qbit checkout: missing CMakeLists.txt"
  [[ -f "${QBIT_SRC_DIR}/src/CMakeLists.txt" ]] || fail "QBIT_SRC_DIR does not contain src/CMakeLists.txt"
  [[ -f "${QBIT_SRC_DIR}/test/functional/test_framework/auxpow.py" ]] || fail "QBIT_SRC_DIR does not contain qbit functional-test helpers"
fi

if [[ -n "${QBIT_BIN_DIR:-}" ]]; then
  check_absolute_dir "${QBIT_BIN_DIR}" "QBIT_BIN_DIR"
  [[ -x "${QBIT_BIN_DIR}/qbitd" ]] || fail "QBIT_BIN_DIR must contain qbitd"
  [[ -x "${QBIT_BIN_DIR}/qbit-cli" ]] || fail "QBIT_BIN_DIR must contain qbit-cli"
fi

[[ -n "${CKPOOL_GIT_URL:-}" ]] || fail "CKPOOL_GIT_URL must be configured"
[[ -n "${CKPOOL_GIT_REF:-}" ]] || fail "CKPOOL_GIT_REF must be configured"
[[ -n "${CPUMINER_GIT_URL:-}" ]] || fail "CPUMINER_GIT_URL must be configured"
[[ -n "${CPUMINER_GIT_REF:-}" ]] || fail "CPUMINER_GIT_REF must be configured"
if [[ -z "${BITCOIN_RELEASE_URL:-}" ]]; then
  [[ -n "${BITCOIN_RELEASE_VERSION:-}" ]] || fail "BITCOIN_RELEASE_VERSION must be set when BITCOIN_RELEASE_URL is unset"
  [[ -n "${BITCOIN_RELEASE_BASE_URL:-}" ]] || fail "BITCOIN_RELEASE_BASE_URL must be set when BITCOIN_RELEASE_URL is unset"
fi
check_bool_env BITCOIN_DNSSEED
check_bool_env BITCOIN_DISCOVER

if [[ "${BITCOIN_CHAIN_FLAG:--regtest}" == "-testnet4" ]]; then
  bitcoin_has_dnsseed=0
  bitcoin_has_explicit_peer=0
  if [[ "${BITCOIN_DNSSEED:-0}" == "1" || "${BITCOIN_NODE_EXTRA_ARGS:-}" == *"-dnsseed=1"* ]]; then
    bitcoin_has_dnsseed=1
  fi
  if [[ "${BITCOIN_NODE_EXTRA_ARGS:-}" == *"-addnode="* || "${BITCOIN_NODE_EXTRA_ARGS:-}" == *"-connect="* || "${BITCOIN_NODE_EXTRA_ARGS:-}" == *"-seednode="* ]]; then
    bitcoin_has_explicit_peer=1
  fi
  if [[ "${bitcoin_has_explicit_peer}" != "1" && "${bitcoin_has_dnsseed}" != "1" ]]; then
    fail "BITCOIN_CHAIN_FLAG=-testnet4 needs parent peer bootstrap; set BITCOIN_DNSSEED=1, or set BITCOIN_NODE_EXTRA_ARGS with -addnode/-connect/-seednode"
  fi
fi

if [[ -n "${QBIT_SRC_DIR:-}" ]]; then
  print_git_ref "${QBIT_SRC_DIR}" "qbit source checkout"
fi
if [[ "${QBIT_PROVIDER}" == "git" && -d "${ROOT_DIR}/generated/qbit-git-cache/.git" ]]; then
  print_git_ref "${ROOT_DIR}/generated/qbit-git-cache" "qbit cache"
fi
printf 'doctor: provider=%s\n' "${QBIT_PROVIDER}"
if [[ -n "${QBIT_SRC_DIR:-}" ]]; then
  printf 'doctor: qbit source=%s\n' "${QBIT_SRC_DIR}"
fi
if [[ -n "${QBIT_GIT_URL:-}" || -n "${QBIT_GIT_REF:-}" ]]; then
  printf 'doctor: qbit git=%s ref=%s\n' "${QBIT_GIT_URL:-<unset>}" "${QBIT_GIT_REF:-<unset>}"
fi
if [[ -n "${QBIT_GIT_COMMIT:-}" ]]; then
  printf 'doctor: qbit pinned commit=%s\n' "${QBIT_GIT_COMMIT}"
fi
printf 'doctor: ckpool ref=%s\n' "${CKPOOL_GIT_REF}"
printf 'doctor: cpuminer ref=%s\n' "${CPUMINER_GIT_REF}"
if [[ -n "${BITCOIN_RELEASE_URL:-}" ]]; then
  printf 'doctor: bitcoin release=%s\n' "${BITCOIN_RELEASE_URL}"
else
  printf 'doctor: bitcoin release version=%s base=%s\n' "${BITCOIN_RELEASE_VERSION}" "${BITCOIN_RELEASE_BASE_URL}"
fi
printf 'doctor: bitcoin parent flag=%s rpc_port=%s p2p_port=%s dnsseed=%s discover=%s\n' \
  "${BITCOIN_CHAIN_FLAG:--regtest}" \
  "${BITCOIN_RPC_PORT:-18443}" \
  "${BITCOIN_P2P_PORT:-18444}" \
  "${BITCOIN_DNSSEED:-0}" \
  "${BITCOIN_DISCOVER:-0}"
printf 'doctor: bitcoin parent extra args=%s\n' "${BITCOIN_NODE_EXTRA_ARGS:-<none>}"
