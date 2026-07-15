#!/usr/bin/env bash
# shellcheck disable=SC1090,SC2034
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_DEPLOY_ENV_FILE="${DEPLOY_ENV_FILE:-}"
ENV_QBIT_PROVIDER="${QBIT_PROVIDER:-}"
ENV_MINING_LANES="${MINING_LANES:-}"
ENV_QBIT_PRODUCTION="${QBIT_PRODUCTION:-}"
ENV_QBIT_TOOLS_PRODUCTION="${QBIT_TOOLS_PRODUCTION:-}"
ENV_QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="${QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED:-}"
ENV_QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS_IS_SET="${QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS+x}"
ENV_QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS="${QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS-}"
ENV_QBIT_REQUIRE_RELEASE_PROVENANCE="${QBIT_REQUIRE_RELEASE_PROVENANCE:-}"
ENV_QBIT_SRC_DIR="${QBIT_SRC_DIR:-}"
ENV_QBIT_BIN_DIR="${QBIT_BIN_DIR:-}"
ENV_QBIT_GIT_URL="${QBIT_GIT_URL:-}"
ENV_QBIT_GIT_REF="${QBIT_GIT_REF:-}"
ENV_QBIT_GIT_COMMIT="${QBIT_GIT_COMMIT:-}"
ENV_QBITD_IMAGE="${QBITD_IMAGE:-}"
ENV_CKPOOL_IMAGE="${CKPOOL_IMAGE:-}"
ENV_BITCOIND_IMAGE="${BITCOIND_IMAGE:-}"
ENV_AUXPOW_COORDINATOR_IMAGE="${AUXPOW_COORDINATOR_IMAGE:-}"
ENV_PRISM_COORDINATOR_IMAGE="${PRISM_COORDINATOR_IMAGE:-}"
ENV_PRISM_POSTGRES_IMAGE="${PRISM_POSTGRES_IMAGE:-}"
ENV_QBIT_DATA_SOURCE="${QBIT_DATA_SOURCE:-}"
ENV_BITCOIN_DATA_SOURCE="${BITCOIN_DATA_SOURCE:-}"
ENV_PRISM_POSTGRES_DATA_SOURCE="${PRISM_POSTGRES_DATA_SOURCE:-}"
ENV_PRISM_POSTGRES_WAL_SOURCE="${PRISM_POSTGRES_WAL_SOURCE:-}"
ENV_PRISM_AUDIT_DATA_SOURCE="${PRISM_AUDIT_DATA_SOURCE:-}"
ENV_QBIT_CHAIN="${QBIT_CHAIN:-}"
ENV_QBIT_CHAIN_FLAG="${QBIT_CHAIN_FLAG:-}"
ENV_QBIT_EXPECTED_GENESIS_HASH="${QBIT_EXPECTED_GENESIS_HASH:-}"
ENV_QBIT_NODE_EXTRA_ARG="${QBIT_NODE_EXTRA_ARG:-}"
ENV_QBIT_MINER_ADDRESS="${QBIT_MINER_ADDRESS:-}"
ENV_CKPOOL_GIT_URL="${CKPOOL_GIT_URL:-}"
ENV_CKPOOL_GIT_REF="${CKPOOL_GIT_REF:-}"
ENV_CKPOOL_MINDIFF="${CKPOOL_MINDIFF:-}"
ENV_CKPOOL_STARTDIFF="${CKPOOL_STARTDIFF:-}"
ENV_CKPOOL_PUBLIC_DIFF_POLICY="${CKPOOL_PUBLIC_DIFF_POLICY:-}"
ENV_CKPOOL_NON_TEST_READINESS_GATE="${CKPOOL_NON_TEST_READINESS_GATE:-}"
ENV_CKPOOL_VALIDATE_QBIT_ASSUMPTIONS="${CKPOOL_VALIDATE_QBIT_ASSUMPTIONS:-}"
ENV_CKPOOL_REQUIRE_P2MR_PAYOUT="${CKPOOL_REQUIRE_P2MR_PAYOUT:-}"
ENV_CKPOOL_STRATUM_PORT_HOST="${CKPOOL_STRATUM_PORT_HOST:-}"
ENV_CPUMINER_GIT_URL="${CPUMINER_GIT_URL:-}"
ENV_CPUMINER_GIT_REF="${CPUMINER_GIT_REF:-}"
ENV_BITCOIN_RELEASE_VERSION="${BITCOIN_RELEASE_VERSION:-}"
ENV_BITCOIN_RELEASE_BASE_URL="${BITCOIN_RELEASE_BASE_URL:-}"
ENV_BITCOIN_RELEASE_URL="${BITCOIN_RELEASE_URL:-}"
ENV_BITCOIN_CHAIN="${BITCOIN_CHAIN:-}"
ENV_BITCOIN_CHAIN_FLAG="${BITCOIN_CHAIN_FLAG:-}"
ENV_BITCOIN_EXPECTED_GENESIS_HASH="${BITCOIN_EXPECTED_GENESIS_HASH:-}"
ENV_BITCOIN_RPC_PORT="${BITCOIN_RPC_PORT:-}"
ENV_BITCOIN_P2P_PORT="${BITCOIN_P2P_PORT:-}"
ENV_BITCOIN_DNSSEED="${BITCOIN_DNSSEED:-}"
ENV_BITCOIN_DISCOVER="${BITCOIN_DISCOVER:-}"
ENV_BITCOIN_NODE_EXTRA_ARGS="${BITCOIN_NODE_EXTRA_ARGS:-}"
ENV_BITCOIN_MINER_ADDRESS="${BITCOIN_MINER_ADDRESS:-}"
ENV_QBIT_RPC_USER="${QBIT_RPC_USER:-}"
ENV_QBIT_RPC_PASSWORD="${QBIT_RPC_PASSWORD:-}"
ENV_QBIT_RPC_PORT_HOST="${QBIT_RPC_PORT_HOST:-}"
ENV_BITCOIN_RPC_USER="${BITCOIN_RPC_USER:-}"
ENV_BITCOIN_RPC_PASSWORD="${BITCOIN_RPC_PASSWORD:-}"
ENV_BITCOIN_RPC_PORT_HOST="${BITCOIN_RPC_PORT_HOST:-}"
ENV_PRISM_DATABASE_URL="${PRISM_DATABASE_URL:-}"
ENV_PRISM_POSTGRES_PASSWORD="${PRISM_POSTGRES_PASSWORD:-}"
ENV_PRISM_POSTGRES_PSQL_COMMAND="${PRISM_POSTGRES_PSQL_COMMAND:-}"
ENV_PRISM_LEDGER_WRITER_ID="${PRISM_LEDGER_WRITER_ID:-}"
ENV_PRISM_LEDGER_WRITER_EPOCH="${PRISM_LEDGER_WRITER_EPOCH:-}"
ENV_PRISM_LEDGER_WRITER_SESSION_TOKEN="${PRISM_LEDGER_WRITER_SESSION_TOKEN:-}"
ENV_PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX="${PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX:-}"
ENV_PRISM_MANIFEST_SIGNING_SEED_HEX="${PRISM_MANIFEST_SIGNING_SEED_HEX:-}"
ENV_PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX="${PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX:-}"
ENV_PRISM_AUDIT_DIR="${PRISM_AUDIT_DIR:-}"
ENV_PRISM_EVIDENCE_PATH="${PRISM_EVIDENCE_PATH:-}"
ENV_PRISM_ALLOW_MEMORY_LEDGER="${PRISM_ALLOW_MEMORY_LEDGER:-}"
ENV_PRISM_ALLOW_TEST_SIGNING_SEEDS="${PRISM_ALLOW_TEST_SIGNING_SEEDS:-}"
ENV_PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY="${PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY:-}"
ENV_PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN="${PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN:-}"
ENV_PRISM_CTV_SETTLEMENT_ENABLED="${PRISM_CTV_SETTLEMENT_ENABLED:-}"
ENV_PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT="${PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT:-}"
ENV_PRISM_CTV_FANOUT_FEE_PREMIUM_BPS="${PRISM_CTV_FANOUT_FEE_PREMIUM_BPS:-}"
ENV_PRISM_STRATUM_SHARE_DIFF="${PRISM_STRATUM_SHARE_DIFF:-}"
ENV_PRISM_STRATUM_VARDIFF="${PRISM_STRATUM_VARDIFF:-}"
ENV_PRISM_STRATUM_VARDIFF_TARGET_SECONDS="${PRISM_STRATUM_VARDIFF_TARGET_SECONDS:-}"
ENV_PRISM_STRATUM_VARDIFF_MIN_DIFF="${PRISM_STRATUM_VARDIFF_MIN_DIFF:-}"
ENV_PRISM_STRATUM_VARDIFF_START_DIFF="${PRISM_STRATUM_VARDIFF_START_DIFF:-}"
ENV_PRISM_STRATUM_VARDIFF_MAX_DIFF="${PRISM_STRATUM_VARDIFF_MAX_DIFF:-}"
ENV_PRISM_STRATUM_VARDIFF_RETARGET_SECONDS="${PRISM_STRATUM_VARDIFF_RETARGET_SECONDS:-}"
ENV_PRISM_STRATUM_VARDIFF_MAX_STEP_UP="${PRISM_STRATUM_VARDIFF_MAX_STEP_UP:-}"
ENV_PRISM_STRATUM_VARDIFF_MAX_STEP_DOWN="${PRISM_STRATUM_VARDIFF_MAX_STEP_DOWN:-}"
ENV_PRISM_STRATUM_VARDIFF_EWMA_ALPHA="${PRISM_STRATUM_VARDIFF_EWMA_ALPHA:-}"
ENV_PRISM_STRATUM_VARDIFF_RETARGET_TOLERANCE="${PRISM_STRATUM_VARDIFF_RETARGET_TOLERANCE:-}"
ENV_PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS="${PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS:-}"
ENV_PRISM_SHARE_COMMIT_BATCH_SIZE="${PRISM_SHARE_COMMIT_BATCH_SIZE:-}"
ENV_PRISM_SHARE_COMMIT_LINGER_MILLISECONDS="${PRISM_SHARE_COMMIT_LINGER_MILLISECONDS:-}"
ENV_PRISM_SHARE_COMMIT_TIMEOUT_SECONDS="${PRISM_SHARE_COMMIT_TIMEOUT_SECONDS:-}"
ENV_PRISM_STRATUM_SEND_TIMEOUT_SECONDS="${PRISM_STRATUM_SEND_TIMEOUT_SECONDS:-}"
ENV_PRISM_STRATUM_STALE_GRACE_SECONDS="${PRISM_STRATUM_STALE_GRACE_SECONDS:-}"
ENV_AUXPOW_STRATUM_ACCEPT_DIAGNOSTIC_VARIANT="${AUXPOW_STRATUM_ACCEPT_DIAGNOSTIC_VARIANT:-}"
ENV_AUXPOW_STRATUM_HEADER_VARIANT="${AUXPOW_STRATUM_HEADER_VARIANT:-}"
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
    printf 'doctor: DEPLOY_ENV_FILE does not exist: %s\n' "${DEPLOY_ENV_FILE}" >&2
    exit 1
  fi
  source "${DEPLOY_ENV_FILE}"
elif [[ -f "${ROOT_DIR}/.env" ]]; then
  source "${ROOT_DIR}/.env"
fi
if [[ -n "${ENV_QBIT_PROVIDER}" ]]; then
  QBIT_PROVIDER="${ENV_QBIT_PROVIDER}"
fi
if [[ -n "${ENV_MINING_LANES}" ]]; then
  MINING_LANES="${ENV_MINING_LANES}"
fi
if [[ -n "${ENV_QBIT_PRODUCTION}" ]]; then
  QBIT_PRODUCTION="${ENV_QBIT_PRODUCTION}"
fi
if [[ -n "${ENV_QBIT_TOOLS_PRODUCTION}" ]]; then
  QBIT_TOOLS_PRODUCTION="${ENV_QBIT_TOOLS_PRODUCTION}"
fi
if [[ -n "${ENV_QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED}" ]]; then
  QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="${ENV_QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED}"
fi
if [[ "${ENV_QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS_IS_SET}" == "x" ]]; then
  QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS="${ENV_QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS}"
fi
if [[ -n "${ENV_QBIT_REQUIRE_RELEASE_PROVENANCE}" ]]; then
  QBIT_REQUIRE_RELEASE_PROVENANCE="${ENV_QBIT_REQUIRE_RELEASE_PROVENANCE}"
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
for name in \
  QBITD_IMAGE \
  CKPOOL_IMAGE \
  BITCOIND_IMAGE \
  AUXPOW_COORDINATOR_IMAGE \
  PRISM_COORDINATOR_IMAGE \
  PRISM_POSTGRES_IMAGE \
  QBIT_DATA_SOURCE \
  BITCOIN_DATA_SOURCE \
  PRISM_POSTGRES_DATA_SOURCE \
  PRISM_POSTGRES_WAL_SOURCE \
  PRISM_AUDIT_DATA_SOURCE; do
  override_name="ENV_${name}"
  if [[ -n "${!override_name}" ]]; then
    printf -v "${name}" '%s' "${!override_name}"
  fi
done
if [[ -n "${ENV_QBIT_CHAIN}" ]]; then
  QBIT_CHAIN="${ENV_QBIT_CHAIN}"
fi
if [[ -n "${ENV_QBIT_CHAIN_FLAG}" ]]; then
  QBIT_CHAIN_FLAG="${ENV_QBIT_CHAIN_FLAG}"
fi
if [[ -n "${ENV_QBIT_EXPECTED_GENESIS_HASH}" ]]; then
  QBIT_EXPECTED_GENESIS_HASH="${ENV_QBIT_EXPECTED_GENESIS_HASH}"
fi
if [[ -n "${ENV_QBIT_NODE_EXTRA_ARG}" ]]; then
  QBIT_NODE_EXTRA_ARG="${ENV_QBIT_NODE_EXTRA_ARG}"
fi
if [[ -n "${ENV_QBIT_MINER_ADDRESS}" ]]; then
  QBIT_MINER_ADDRESS="${ENV_QBIT_MINER_ADDRESS}"
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
if [[ -n "${ENV_CKPOOL_NON_TEST_READINESS_GATE}" ]]; then
  CKPOOL_NON_TEST_READINESS_GATE="${ENV_CKPOOL_NON_TEST_READINESS_GATE}"
fi
if [[ -n "${ENV_CKPOOL_VALIDATE_QBIT_ASSUMPTIONS}" ]]; then
  CKPOOL_VALIDATE_QBIT_ASSUMPTIONS="${ENV_CKPOOL_VALIDATE_QBIT_ASSUMPTIONS}"
fi
if [[ -n "${ENV_CKPOOL_REQUIRE_P2MR_PAYOUT}" ]]; then
  CKPOOL_REQUIRE_P2MR_PAYOUT="${ENV_CKPOOL_REQUIRE_P2MR_PAYOUT}"
fi
if [[ -n "${ENV_CKPOOL_STRATUM_PORT_HOST}" ]]; then
  CKPOOL_STRATUM_PORT_HOST="${ENV_CKPOOL_STRATUM_PORT_HOST}"
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
if [[ -n "${ENV_BITCOIN_CHAIN}" ]]; then
  BITCOIN_CHAIN="${ENV_BITCOIN_CHAIN}"
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
if [[ -n "${ENV_BITCOIN_MINER_ADDRESS}" ]]; then
  BITCOIN_MINER_ADDRESS="${ENV_BITCOIN_MINER_ADDRESS}"
fi
if [[ -n "${ENV_BITCOIN_EXPECTED_GENESIS_HASH}" ]]; then
  BITCOIN_EXPECTED_GENESIS_HASH="${ENV_BITCOIN_EXPECTED_GENESIS_HASH}"
fi
if [[ -n "${ENV_QBIT_RPC_USER}" ]]; then
  QBIT_RPC_USER="${ENV_QBIT_RPC_USER}"
fi
if [[ -n "${ENV_QBIT_RPC_PASSWORD}" ]]; then
  QBIT_RPC_PASSWORD="${ENV_QBIT_RPC_PASSWORD}"
fi
if [[ -n "${ENV_QBIT_RPC_PORT_HOST}" ]]; then
  QBIT_RPC_PORT_HOST="${ENV_QBIT_RPC_PORT_HOST}"
fi
if [[ -n "${ENV_BITCOIN_RPC_USER}" ]]; then
  BITCOIN_RPC_USER="${ENV_BITCOIN_RPC_USER}"
fi
if [[ -n "${ENV_BITCOIN_RPC_PASSWORD}" ]]; then
  BITCOIN_RPC_PASSWORD="${ENV_BITCOIN_RPC_PASSWORD}"
fi
if [[ -n "${ENV_BITCOIN_RPC_PORT_HOST}" ]]; then
  BITCOIN_RPC_PORT_HOST="${ENV_BITCOIN_RPC_PORT_HOST}"
fi
if [[ -n "${ENV_PRISM_DATABASE_URL}" ]]; then
  PRISM_DATABASE_URL="${ENV_PRISM_DATABASE_URL}"
fi
if [[ -n "${ENV_PRISM_POSTGRES_PSQL_COMMAND}" ]]; then
  PRISM_POSTGRES_PSQL_COMMAND="${ENV_PRISM_POSTGRES_PSQL_COMMAND}"
fi
if [[ -n "${ENV_PRISM_POSTGRES_PASSWORD}" ]]; then
  PRISM_POSTGRES_PASSWORD="${ENV_PRISM_POSTGRES_PASSWORD}"
fi
if [[ -n "${ENV_PRISM_LEDGER_WRITER_ID}" ]]; then
  PRISM_LEDGER_WRITER_ID="${ENV_PRISM_LEDGER_WRITER_ID}"
fi
if [[ -n "${ENV_PRISM_LEDGER_WRITER_EPOCH}" ]]; then
  PRISM_LEDGER_WRITER_EPOCH="${ENV_PRISM_LEDGER_WRITER_EPOCH}"
fi
if [[ -n "${ENV_PRISM_LEDGER_WRITER_SESSION_TOKEN}" ]]; then
  PRISM_LEDGER_WRITER_SESSION_TOKEN="${ENV_PRISM_LEDGER_WRITER_SESSION_TOKEN}"
fi
if [[ -n "${ENV_PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX}" ]]; then
  PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX="${ENV_PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX}"
fi
if [[ -n "${ENV_PRISM_MANIFEST_SIGNING_SEED_HEX}" ]]; then
  PRISM_MANIFEST_SIGNING_SEED_HEX="${ENV_PRISM_MANIFEST_SIGNING_SEED_HEX}"
fi
if [[ -n "${ENV_PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX}" ]]; then
  PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX="${ENV_PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX}"
fi
if [[ -n "${ENV_PRISM_AUDIT_DIR}" ]]; then
  PRISM_AUDIT_DIR="${ENV_PRISM_AUDIT_DIR}"
fi
if [[ -n "${ENV_PRISM_EVIDENCE_PATH}" ]]; then
  PRISM_EVIDENCE_PATH="${ENV_PRISM_EVIDENCE_PATH}"
fi
if [[ -n "${ENV_PRISM_ALLOW_MEMORY_LEDGER}" ]]; then
  PRISM_ALLOW_MEMORY_LEDGER="${ENV_PRISM_ALLOW_MEMORY_LEDGER}"
fi
if [[ -n "${ENV_PRISM_ALLOW_TEST_SIGNING_SEEDS}" ]]; then
  PRISM_ALLOW_TEST_SIGNING_SEEDS="${ENV_PRISM_ALLOW_TEST_SIGNING_SEEDS}"
fi
if [[ -n "${ENV_PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY}" ]]; then
  PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY="${ENV_PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY}"
fi
if [[ -n "${ENV_PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN}" ]]; then
  PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN="${ENV_PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN}"
fi
if [[ -n "${ENV_PRISM_CTV_SETTLEMENT_ENABLED}" ]]; then
  PRISM_CTV_SETTLEMENT_ENABLED="${ENV_PRISM_CTV_SETTLEMENT_ENABLED}"
fi
if [[ -n "${ENV_PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT}" ]]; then
  PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT="${ENV_PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT}"
fi
if [[ -n "${ENV_PRISM_CTV_FANOUT_FEE_PREMIUM_BPS}" ]]; then
  PRISM_CTV_FANOUT_FEE_PREMIUM_BPS="${ENV_PRISM_CTV_FANOUT_FEE_PREMIUM_BPS}"
fi
for name in \
  PRISM_STRATUM_VARDIFF \
  PRISM_STRATUM_VARDIFF_TARGET_SECONDS \
  PRISM_STRATUM_VARDIFF_MAX_DIFF \
  PRISM_STRATUM_VARDIFF_RETARGET_SECONDS \
  PRISM_STRATUM_VARDIFF_MAX_STEP_UP \
  PRISM_STRATUM_VARDIFF_MAX_STEP_DOWN \
  PRISM_STRATUM_VARDIFF_EWMA_ALPHA \
  PRISM_STRATUM_VARDIFF_RETARGET_TOLERANCE \
  PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS \
  PRISM_SHARE_COMMIT_TIMEOUT_SECONDS \
  PRISM_STRATUM_SEND_TIMEOUT_SECONDS; do
  override_name="ENV_${name}"
  if [[ -n "${!override_name}" ]]; then
    printf -v "${name}" '%s' "${!override_name}"
  fi
done
if [[ -n "${ENV_PRISM_STRATUM_SHARE_DIFF}" ]]; then
  PRISM_STRATUM_SHARE_DIFF="${ENV_PRISM_STRATUM_SHARE_DIFF}"
fi
if [[ -n "${ENV_PRISM_STRATUM_VARDIFF_MIN_DIFF}" ]]; then
  PRISM_STRATUM_VARDIFF_MIN_DIFF="${ENV_PRISM_STRATUM_VARDIFF_MIN_DIFF}"
fi
if [[ -n "${ENV_PRISM_STRATUM_VARDIFF_START_DIFF}" ]]; then
  PRISM_STRATUM_VARDIFF_START_DIFF="${ENV_PRISM_STRATUM_VARDIFF_START_DIFF}"
fi
if [[ -n "${ENV_PRISM_SHARE_COMMIT_BATCH_SIZE}" ]]; then
  PRISM_SHARE_COMMIT_BATCH_SIZE="${ENV_PRISM_SHARE_COMMIT_BATCH_SIZE}"
fi
if [[ -n "${ENV_PRISM_SHARE_COMMIT_LINGER_MILLISECONDS}" ]]; then
  PRISM_SHARE_COMMIT_LINGER_MILLISECONDS="${ENV_PRISM_SHARE_COMMIT_LINGER_MILLISECONDS}"
fi
if [[ -n "${ENV_PRISM_STRATUM_STALE_GRACE_SECONDS}" ]]; then
  PRISM_STRATUM_STALE_GRACE_SECONDS="${ENV_PRISM_STRATUM_STALE_GRACE_SECONDS}"
fi
if [[ -n "${ENV_AUXPOW_STRATUM_ACCEPT_DIAGNOSTIC_VARIANT}" ]]; then
  AUXPOW_STRATUM_ACCEPT_DIAGNOSTIC_VARIANT="${ENV_AUXPOW_STRATUM_ACCEPT_DIAGNOSTIC_VARIANT}"
fi
if [[ -n "${ENV_AUXPOW_STRATUM_HEADER_VARIANT}" ]]; then
  AUXPOW_STRATUM_HEADER_VARIANT="${ENV_AUXPOW_STRATUM_HEADER_VARIANT}"
fi
if [[ -n "${QBIT_SRC_DIR_OVERRIDE:-}" ]]; then
  QBIT_SRC_DIR="${QBIT_SRC_DIR_OVERRIDE}"
fi

fail() {
  printf 'doctor: %s\n' "$1" >&2
  exit 1
}

ascii_lower() {
  printf '%s' "${1:-}" | LC_ALL=C tr '[:upper:]' '[:lower:]'
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

is_true_env() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

parse_bool_env() {
  local name="$1"
  local value="${2:-}"
  local normalized

  normalized="$(ascii_lower "${value}" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"

  case "${normalized}" in
    1|true|yes|on) PARSED_BOOL_ENV=1 ;;
    0|false|no|off) PARSED_BOOL_ENV=0 ;;
    *) fail "${name} must be a true/false style value, got '${value}'" ;;
  esac
}

production_mode_enabled() {
  local qbit_production
  local qbit_tools_production

  parse_bool_env QBIT_PRODUCTION "${QBIT_PRODUCTION:-0}"
  qbit_production="${PARSED_BOOL_ENV}"
  parse_bool_env QBIT_TOOLS_PRODUCTION "${QBIT_TOOLS_PRODUCTION:-0}"
  qbit_tools_production="${PARSED_BOOL_ENV}"
  [[ "${qbit_production}" == "1" || "${qbit_tools_production}" == "1" ]]
}

# Release provenance (digest-qualified images and absolute host state paths) is
# unconditional on mainnet: no environment variable may waive it there. Other
# chains can run production mode while building images from the pinned source,
# so they enforce provenance only when QBIT_REQUIRE_RELEASE_PROVENANCE opts in.
release_provenance_required() {
  if [[ "${QBIT_CHAIN:-regtest}" == "mainnet" ]]; then
    return 0
  fi
  is_true_env "${QBIT_REQUIRE_RELEASE_PROVENANCE:-0}"
}

is_public_qbit_chain() {
  case "${1}" in
    mainnet|testnet|testnet3|testnet4|signet) return 0 ;;
    *) return 1 ;;
  esac
}

mining_lane_enabled() {
  local requested="$1"
  local configured=",${MINING_LANES:-all},"
  [[ "${configured}" == *,all,* || "${configured}" == *,"${requested}",* ]]
}

validate_mining_lanes() {
  local lane
  IFS=',' read -r -a lanes <<< "${MINING_LANES:-all}"
  [[ "${#lanes[@]}" -gt 0 ]] || fail "MINING_LANES must select ckpool, prism, auxpow, or all"
  for lane in "${lanes[@]}"; do
    case "${lane}" in
      all|ckpool|prism|auxpow) ;;
      *) fail "MINING_LANES contains unsupported lane '${lane}'" ;;
    esac
  done
}

check_digest_image() {
  local name="$1"
  local value="${!name:-}"

  [[ "${value}" =~ ^[^[:space:]@]+@sha256:[0-9a-f]{64}$ ]] || \
    fail "release provenance requires ${name} as a digest-qualified image reference"
}

check_private_rpc_binding() {
  local name="$1"
  local value="${!name:-}"
  local host port lower_host
  local -a octets

  if [[ "${value}" =~ ^\[([^][]+)\]:([0-9]+)$ ]]; then
    host="${BASH_REMATCH[1]}"
    port="${BASH_REMATCH[2]}"
  elif [[ "${value}" =~ ^([^:[:space:]]+):([0-9]+)$ ]]; then
    host="${BASH_REMATCH[1]}"
    port="${BASH_REMATCH[2]}"
  else
    fail "production mode requires ${name} as an explicit loopback or private host:port binding"
  fi

  if ((10#${port} < 1 || 10#${port} > 65535)); then
    fail "production mode requires ${name} with a port between 1 and 65535"
  fi

  lower_host="$(ascii_lower "${host}")"
  if [[ "${lower_host}" == *:* ]]; then
    if [[ "${lower_host}" == "::1" || "${lower_host}" =~ ^f[cd][0-9a-f]{2}: || "${lower_host}" =~ ^fe[89ab][0-9a-f]: ]]; then
      return 0
    fi
    fail "production mode requires ${name} to bind a loopback or private interface"
  fi

  IFS='.' read -r -a octets <<< "${host}"
  if [[ "${#octets[@]}" -ne 4 ]]; then
    fail "production mode requires ${name} to bind a loopback or private interface"
  fi
  for octet in "${octets[@]}"; do
    if [[ ! "${octet}" =~ ^[0-9]+$ ]] || ((10#${octet} > 255)); then
      fail "production mode requires ${name} to contain a valid IP address"
    fi
  done
  if ((
    10#${octets[0]} == 127 ||
    10#${octets[0]} == 10 ||
    (10#${octets[0]} == 100 && 10#${octets[1]} >= 64 && 10#${octets[1]} <= 127) ||
    (10#${octets[0]} == 172 && 10#${octets[1]} >= 16 && 10#${octets[1]} <= 31) ||
    (10#${octets[0]} == 192 && 10#${octets[1]} == 168)
  )); then
    return 0
  fi
  fail "production mode requires ${name} to bind a loopback or private interface"
}

check_node_extra_arg_selectors() {
  local name value token normalized option

  for name in QBIT_NODE_EXTRA_ARG BITCOIN_NODE_EXTRA_ARGS; do
    value="${!name:-}"
    for token in ${value}; do
      normalized="$(ascii_lower "${token}")"
      [[ "${normalized}" == -* ]] || continue
      option="${normalized#-}"
      option="${option#-}"
      case "${option}" in
        main|main=*|mainnet|mainnet=*|testnet|testnet=*|testnet3|testnet3=*|testnet4|testnet4=*|signet|signet=*|regtest|regtest=*|nomain|nomain=*|nomainnet|nomainnet=*|notestnet|notestnet=*|notestnet3|notestnet3=*|notestnet4|notestnet4=*|nosignet|nosignet=*|noregtest|noregtest=*|chain|chain=*)
          fail "network selector ${token} is not allowed in ${name}; use the dedicated chain settings"
          ;;
      esac
    done
  done
}

check_production_bitcoin_extra_args() {
  local token normalized option

  for token in ${BITCOIN_NODE_EXTRA_ARGS:-}; do
    normalized="$(ascii_lower "${token}")"
    [[ "${normalized}" == -* ]] || continue
    option="${normalized#-}"
    option="${option#-}"
    case "${option}" in
      dnsseed|dnsseed=*|discover|discover=*)
        fail "production mode rejects ${token} in BITCOIN_NODE_EXTRA_ARGS; use BITCOIN_DNSSEED and BITCOIN_DISCOVER"
        ;;
    esac
  done
}

check_absolute_storage_source() {
  local name="$1"
  local value="${!name:-}"

  [[ -n "${value}" && "${value}" == /* ]] || \
    fail "release provenance requires ${name} as an absolute host path"
}

check_distinct_storage_sources() {
  local names=("$@")
  local left right
  local left_name right_name
  local left_value right_value

  for ((left = 0; left < ${#names[@]}; left++)); do
    left_name="${names[left]}"
    left_value="${!left_name}"
    left_value="${left_value%/}"
    for ((right = left + 1; right < ${#names[@]}; right++)); do
      right_name="${names[right]}"
      right_value="${!right_name}"
      right_value="${right_value%/}"
      [[ "${left_value}" != "${right_value}" ]] || \
        fail "release provenance storage sources ${left_name} and ${right_name} must be distinct"
    done
  done
}

check_production_deployment_inputs() {
  local storage_names=(QBIT_DATA_SOURCE)

  check_digest_image QBITD_IMAGE
  check_absolute_storage_source QBIT_DATA_SOURCE
  if mining_lane_enabled ckpool; then
    check_digest_image CKPOOL_IMAGE
  fi
  if mining_lane_enabled auxpow; then
    check_digest_image BITCOIND_IMAGE
    check_digest_image AUXPOW_COORDINATOR_IMAGE
    check_absolute_storage_source BITCOIN_DATA_SOURCE
    storage_names+=(BITCOIN_DATA_SOURCE)
  fi
  if mining_lane_enabled prism; then
    check_digest_image PRISM_COORDINATOR_IMAGE
    check_digest_image PRISM_POSTGRES_IMAGE
    check_absolute_storage_source PRISM_POSTGRES_DATA_SOURCE
    check_absolute_storage_source PRISM_POSTGRES_WAL_SOURCE
    check_absolute_storage_source PRISM_AUDIT_DATA_SOURCE
    storage_names+=(
      PRISM_POSTGRES_DATA_SOURCE
      PRISM_POSTGRES_WAL_SOURCE
      PRISM_AUDIT_DATA_SOURCE
    )
  fi
  check_distinct_storage_sources "${storage_names[@]}"
}

require_lab_mode() {
  if production_mode_enabled; then
    fail "lab-only target refuses a production configuration selected by QBIT_PRODUCTION"
  fi
  case "${QBIT_CHAIN:-regtest}" in
    main|mainnet) fail "lab-only target refuses main-chain QBIT_CHAIN" ;;
  esac
  case "${BITCOIN_CHAIN:-regtest}" in
    main|mainnet) fail "lab-only target refuses main-chain BITCOIN_CHAIN" ;;
  esac
  check_node_extra_arg_selectors
  check_qbit_chain_selection
  check_bitcoin_chain_selection
  printf 'doctor: lab-only target confirmed non-production chains\n'
}

check_prism_production_difficulty() {
  local output

  command -v python3 >/dev/null 2>&1 || fail "python3 is required to validate production PRISM difficulty"
  if ! output="$(python3 - \
    "${PRISM_STRATUM_SHARE_DIFF:-}" \
    "${PRISM_STRATUM_VARDIFF_MIN_DIFF:-}" \
    "${PRISM_STRATUM_VARDIFF_START_DIFF:-}" \
    "${PRISM_STRATUM_VARDIFF_MAX_DIFF:-}" 2>&1 <<'PY'
from decimal import Decimal, InvalidOperation
import sys

names = (
    "PRISM_STRATUM_SHARE_DIFF",
    "PRISM_STRATUM_VARDIFF_MIN_DIFF",
    "PRISM_STRATUM_VARDIFF_START_DIFF",
    "PRISM_STRATUM_VARDIFF_MAX_DIFF",
)
values = {}
for name, raw_value in zip(names, sys.argv[1:]):
    if not raw_value:
        raise SystemExit(f"production mode requires an explicit {name}")
    try:
        value = Decimal(raw_value)
    except InvalidOperation:
        raise SystemExit(f"{name} must be a decimal number")
    if not value.is_finite() or value <= 0:
        raise SystemExit(f"{name} must be positive")
    if value == Decimal("0.000000001"):
        raise SystemExit(f"{name} cannot use the lab-only 1e-9 difficulty")
    values[name] = value
if values["PRISM_STRATUM_VARDIFF_MIN_DIFF"] > values["PRISM_STRATUM_VARDIFF_START_DIFF"]:
    raise SystemExit("production vardiff minimum exceeds its start difficulty")
if values["PRISM_STRATUM_VARDIFF_START_DIFF"] > values["PRISM_STRATUM_VARDIFF_MAX_DIFF"]:
    raise SystemExit("production vardiff start exceeds its maximum difficulty")
PY
  )"; then
    fail "${output}"
  fi
}

check_production_gate() {
  local production_enabled=0
  local qbit_production=0
  local qbit_tools_production=0
  local readiness_gate
  local launch_readiness_checks="unset"

  if production_mode_enabled; then
    production_enabled=1
  fi
  [[ "${QBIT_CHAIN:-regtest}" == "mainnet" || "${production_enabled}" == "1" ]] || return 0

  parse_bool_env QBIT_PRODUCTION "${QBIT_PRODUCTION:-0}"
  qbit_production="${PARSED_BOOL_ENV}"
  parse_bool_env QBIT_TOOLS_PRODUCTION "${QBIT_TOOLS_PRODUCTION:-0}"
  qbit_tools_production="${PARSED_BOOL_ENV}"
  parse_bool_env CKPOOL_NON_TEST_READINESS_GATE "${CKPOOL_NON_TEST_READINESS_GATE:-1}"
  readiness_gate="${PARSED_BOOL_ENV}"
  if [[ -n "${QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED:-}" ]]; then
    parse_bool_env QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED "${QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED}"
    launch_readiness_checks="${PARSED_BOOL_ENV}"
    [[ "${QBIT_CHAIN:-regtest}" == "mainnet" ]] || fail "QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED is valid only for QBIT_CHAIN=mainnet"
  fi
  if [[ "${readiness_gate}" == "0" || "${launch_readiness_checks}" == "0" ]]; then
    [[ \
      "${QBIT_CHAIN:-regtest}" == "mainnet" && \
      "${qbit_production}" == "1" && \
      "${qbit_tools_production}" == "1" && \
      "${readiness_gate}" == "0" && \
      "${launch_readiness_checks}" == "0" \
    ]] || fail "mainnet prelaunch requires the explicitly authorized mainnet prelaunch combination: QBIT_PRODUCTION=1, QBIT_TOOLS_PRODUCTION=1, CKPOOL_NON_TEST_READINESS_GATE=0, and QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED=0"
  fi

  [[ "${QBIT_CHAIN:-regtest}" != "regtest" ]] || fail "production mode rejects regtest QBIT_CHAIN"

  if mining_lane_enabled prism; then
    for name in \
      PRISM_ALLOW_MEMORY_LEDGER \
      PRISM_ALLOW_TEST_SIGNING_SEEDS \
      PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY \
      PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN; do
      if is_true_env "${!name:-0}"; then
        fail "production mode rejects ${name}=1"
      fi
    done
    if [[ "${QBIT_CHAIN:-regtest}" == "mainnet" ]]; then
      [[ "${PRISM_STRATUM_STALE_GRACE_SECONDS:-3}" == "0" ]] || fail "mainnet requires PRISM_STRATUM_STALE_GRACE_SECONDS=0"
    fi
    [[ -n "${PRISM_DATABASE_URL:-}" || -n "${PRISM_POSTGRES_PSQL_COMMAND:-}" ]] || fail "production mode requires PRISM_DATABASE_URL or PRISM_POSTGRES_PSQL_COMMAND"
    [[ "${PRISM_POSTGRES_PASSWORD:-}" != "change-this" ]] || fail "production mode requires a non-default PRISM_POSTGRES_PASSWORD"
    [[ "${PRISM_DATABASE_URL:-}" != *"change-this"* ]] || fail "production mode requires a non-default PRISM_DATABASE_URL"
    [[ -n "${PRISM_MANIFEST_SIGNING_SEED_HEX:-}" ]] || fail "production mode requires PRISM_MANIFEST_SIGNING_SEED_HEX"
    [[ -n "${PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX:-}" ]] || fail "production mode requires PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX"
    [[ -n "${PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX:-}" ]] || fail "production mode requires PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX"
    [[ -n "${PRISM_LEDGER_WRITER_ID:-}" ]] || fail "production mode requires PRISM_LEDGER_WRITER_ID"
    [[ -n "${PRISM_LEDGER_WRITER_EPOCH:-}" ]] || fail "production mode requires PRISM_LEDGER_WRITER_EPOCH"
    [[ -z "${PRISM_LEDGER_WRITER_SESSION_TOKEN:-}" ]] || fail "production mode requires managed ledger session tokens; unset PRISM_LEDGER_WRITER_SESSION_TOKEN"
    [[ -n "${PRISM_AUDIT_DIR:-}" ]] || fail "production mode requires PRISM_AUDIT_DIR"
    [[ -n "${PRISM_EVIDENCE_PATH:-}" ]] || fail "production mode requires PRISM_EVIDENCE_PATH"
  fi

  if mining_lane_enabled ckpool; then
    [[ "${CKPOOL_PUBLIC_DIFF_POLICY:-explicit}" != "permissive" ]] || fail "production mode rejects CKPOOL_PUBLIC_DIFF_POLICY=permissive"
    [[ "${CKPOOL_PUBLIC_DIFF_POLICY:-explicit}" != "allow-defaults" ]] || fail "production mode rejects CKPOOL_PUBLIC_DIFF_POLICY=allow-defaults"
    [[ "${CKPOOL_PUBLIC_DIFF_POLICY:-explicit}" != "defaults" ]] || fail "production mode rejects CKPOOL_PUBLIC_DIFF_POLICY=defaults"
    [[ "${CKPOOL_VALIDATE_QBIT_ASSUMPTIONS:-1}" != "0" ]] || fail "production mode rejects CKPOOL_VALIDATE_QBIT_ASSUMPTIONS=0"
    if is_public_qbit_chain "${QBIT_CHAIN:-regtest}"; then
      [[ "${CKPOOL_REQUIRE_P2MR_PAYOUT:-1}" != "0" ]] || fail "production mode rejects public-chain CKPOOL_REQUIRE_P2MR_PAYOUT=0"
    fi
    [[ -n "${QBIT_MINER_ADDRESS:-}" && "$(ascii_lower "${QBIT_MINER_ADDRESS:-}")" != "auto" ]] || fail "production CKPool requires an explicit QBIT_MINER_ADDRESS"
  fi

  if mining_lane_enabled auxpow; then
    check_production_bitcoin_extra_args
    [[ "${AUXPOW_STRATUM_ACCEPT_DIAGNOSTIC_VARIANT:-0}" != "1" ]] || fail "production mode rejects AUXPOW_STRATUM_ACCEPT_DIAGNOSTIC_VARIANT=1"
    [[ "${AUXPOW_STRATUM_HEADER_VARIANT:-canonical}" == "canonical" ]] || fail "production mode requires AUXPOW_STRATUM_HEADER_VARIANT=canonical"
    if [[ "${QBIT_CHAIN:-regtest}" == "mainnet" && "${BITCOIN_CHAIN:-regtest}" != "mainnet" ]]; then
      fail "qbit mainnet AuxPoW requires BITCOIN_CHAIN=mainnet"
    fi
    [[ -n "${QBIT_MINER_ADDRESS:-}" && "${QBIT_MINER_ADDRESS}" != "auto" ]] || fail "production AuxPoW requires an explicit QBIT_MINER_ADDRESS"
    [[ -n "${BITCOIN_MINER_ADDRESS:-}" && "${BITCOIN_MINER_ADDRESS}" != "auto" ]] || fail "production AuxPoW requires an explicit BITCOIN_MINER_ADDRESS"
  fi

  [[ -n "${QBIT_RPC_USER:-}" ]] || fail "production mode requires QBIT_RPC_USER"
  [[ -n "${QBIT_RPC_PASSWORD:-}" && "${QBIT_RPC_PASSWORD:-}" != "change-this" ]] || fail "production mode requires a non-default QBIT_RPC_PASSWORD"
  if mining_lane_enabled auxpow; then
    [[ -n "${BITCOIN_RPC_USER:-}" ]] || fail "production mode requires BITCOIN_RPC_USER"
    [[ -n "${BITCOIN_RPC_PASSWORD:-}" && "${BITCOIN_RPC_PASSWORD:-}" != "change-this" ]] || fail "production mode requires a non-default BITCOIN_RPC_PASSWORD"
    check_private_rpc_binding BITCOIN_RPC_PORT_HOST
  fi
  if mining_lane_enabled ckpool; then
    [[ -n "${CKPOOL_STRATUM_PORT_HOST:-}" ]] || fail "production mode requires CKPOOL_STRATUM_PORT_HOST"
  fi
  check_private_rpc_binding QBIT_RPC_PORT_HOST

  [[ "${QBIT_GIT_COMMIT:-}" =~ ^[[:xdigit:]]{40}$ ]] || fail "production mode requires QBIT_GIT_COMMIT as exactly 40 hex characters"
  if mining_lane_enabled ckpool; then
    [[ "${CKPOOL_GIT_REF:-}" =~ ^[[:xdigit:]]{40}$ ]] || fail "production mode requires CKPOOL_GIT_REF as exactly 40 hex characters"
  fi
  if mining_lane_enabled prism; then
    check_prism_production_difficulty
  fi
}

check_release_provenance_gate() {
  if ! release_provenance_required; then
    if production_mode_enabled; then
      printf 'doctor: release provenance not enforced for QBIT_CHAIN=%s; set QBIT_REQUIRE_RELEASE_PROVENANCE=1 to require digest-pinned deployment inputs\n' \
        "${QBIT_CHAIN:-regtest}"
    fi
    return 0
  fi
  check_production_deployment_inputs
}

verify_git_checkout_commit() {
  local checkout="$1"
  local expected_commit
  local label="$3"
  local actual_commit

  expected_commit="$(ascii_lower "$2")"
  if ! actual_commit="$(git -C "${checkout}" rev-parse --verify HEAD 2>/dev/null)"; then
    fail "cannot resolve ${label} HEAD: ${checkout}"
  fi
  actual_commit="$(ascii_lower "${actual_commit}")"
  [[ "${actual_commit}" == "${expected_commit}" ]] || fail "${label} HEAD ${actual_commit} does not match QBIT_GIT_COMMIT ${expected_commit}"
  printf 'doctor: %s verified at %s\n' "${label}" "${actual_commit}"
}

verify_staged_source_commit() {
  local checkout="$1"
  local expected_commit
  local label="$3"
  local marker="${checkout}/.qbit-source-commit"
  local actual_commit

  expected_commit="$(ascii_lower "$2")"
  if [[ -f "${marker}" ]]; then
    actual_commit="$(tr -d '[:space:]' < "${marker}")"
    actual_commit="$(ascii_lower "${actual_commit}")"
    [[ "${actual_commit}" =~ ^[[:xdigit:]]{40}$ ]] || fail "${label} has an invalid source commit marker"
    [[ "${actual_commit}" == "${expected_commit}" ]] || fail "${label} commit ${actual_commit} does not match QBIT_GIT_COMMIT ${expected_commit}"
    printf 'doctor: %s verified at %s\n' "${label}" "${actual_commit}"
    return
  fi
  verify_git_checkout_commit "${checkout}" "${expected_commit}" "${label}"
}

check_qbit_chain_selection() {
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
      [[ "${QBIT_CHAIN_FLAG:-}" == "-chain=main" ]] || fail "QBIT_CHAIN=mainnet requires explicit QBIT_CHAIN_FLAG=-chain=main"
      [[ "${QBIT_EXPECTED_GENESIS_HASH:-}" =~ ^[[:xdigit:]]{64}$ ]] || fail "QBIT_CHAIN=mainnet requires QBIT_EXPECTED_GENESIS_HASH as 64 hex characters"
      ;;
    *)
      fail "QBIT_CHAIN must be mainnet, testnet, testnet3, testnet4, signet, or regtest; got '${QBIT_CHAIN}'"
      ;;
  esac
}

check_bitcoin_chain_selection() {
  case "${BITCOIN_CHAIN:-regtest}" in
    regtest)
      [[ "${BITCOIN_CHAIN_FLAG:--regtest}" == "-regtest" ]] || fail "BITCOIN_CHAIN=regtest requires BITCOIN_CHAIN_FLAG=-regtest"
      ;;
    testnet|testnet3)
      [[ "${BITCOIN_CHAIN_FLAG:-}" == "-testnet" ]] || fail "BITCOIN_CHAIN=${BITCOIN_CHAIN} requires BITCOIN_CHAIN_FLAG=-testnet"
      ;;
    testnet4)
      [[ "${BITCOIN_CHAIN_FLAG:-}" == "-testnet4" ]] || fail "BITCOIN_CHAIN=testnet4 requires BITCOIN_CHAIN_FLAG=-testnet4"
      ;;
    signet)
      [[ "${BITCOIN_CHAIN_FLAG:-}" == "-signet" ]] || fail "BITCOIN_CHAIN=signet requires BITCOIN_CHAIN_FLAG=-signet"
      ;;
    mainnet)
      [[ "${BITCOIN_CHAIN_FLAG:-}" == "-chain=main" ]] || fail "BITCOIN_CHAIN=mainnet requires explicit BITCOIN_CHAIN_FLAG=-chain=main"
      [[ "${BITCOIN_EXPECTED_GENESIS_HASH:-}" =~ ^[[:xdigit:]]{64}$ ]] || fail "BITCOIN_CHAIN=mainnet requires BITCOIN_EXPECTED_GENESIS_HASH as 64 hex characters"
      [[ "$(ascii_lower "${BITCOIN_EXPECTED_GENESIS_HASH:-}")" == "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f" ]] || fail "BITCOIN_EXPECTED_GENESIS_HASH must equal the canonical Bitcoin mainnet genesis"
      ;;
    *)
      fail "BITCOIN_CHAIN must be mainnet, testnet, testnet3, testnet4, signet, or regtest; got '${BITCOIN_CHAIN}'"
      ;;
  esac
}

check_ctv_fee_config() {
  mining_lane_enabled prism || return 0
  is_true_env "${PRISM_CTV_SETTLEMENT_ENABLED:-0}" || return 0

  local rate="${PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT:-}"
  local premium="${PRISM_CTV_FANOUT_FEE_PREMIUM_BPS:-12000}"
  if [[ -n "${rate}" && ( ! "${rate}" =~ ^[0-9]+$ || "${rate}" == "0" ) ]]; then
    fail "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT must be a positive integer when set"
  fi
  [[ "${premium}" =~ ^[0-9]+$ && "${premium}" != "0" ]] || fail "PRISM_CTV_FANOUT_FEE_PREMIUM_BPS must be a positive integer"
  if [[ -z "${rate}" ]]; then
    if [[ "${QBIT_CHAIN:-regtest}" == "mainnet" ]]; then
      fail "mainnet CTV settlement requires PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT"
    fi
    printf 'doctor: CTV fanout fee rate will use estimatesmartfee; run prism-self-check to preflight it before accepting miners\n'
  fi
}

check_bitcoin_peer_bootstrap() {
  mining_lane_enabled auxpow || return 0
  local chain="${BITCOIN_CHAIN:-regtest}"
  local has_dnsseed=0
  local has_explicit_peer=0

  [[ "${chain}" != "regtest" ]] || return 0
  if [[ "${BITCOIN_DNSSEED:-0}" == "1" || "${BITCOIN_NODE_EXTRA_ARGS:-}" == *"-dnsseed=1"* ]]; then
    has_dnsseed=1
  fi
  if [[ "${BITCOIN_NODE_EXTRA_ARGS:-}" == *"-addnode="* || "${BITCOIN_NODE_EXTRA_ARGS:-}" == *"-connect="* || "${BITCOIN_NODE_EXTRA_ARGS:-}" == *"-seednode="* ]]; then
    has_explicit_peer=1
  fi
  if [[ "${has_explicit_peer}" != "1" && "${has_dnsseed}" != "1" ]]; then
    fail "BITCOIN_CHAIN=${chain} needs parent peer bootstrap; set BITCOIN_DNSSEED=1, or set BITCOIN_NODE_EXTRA_ARGS with -addnode/-connect/-seednode"
  fi
}

case "${1:-}" in
  "") ;;
  --require-lab)
    validate_mining_lanes
    require_lab_mode
    exit 0
    ;;
  *) fail "unknown argument: ${1}" ;;
esac

export QBIT_PRODUCTION QBIT_TOOLS_PRODUCTION QBIT_CHAIN CKPOOL_NON_TEST_READINESS_GATE
export QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED
export QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS
qbit_validation_args=(
  --validate-only
  "${QBIT_CHAIN_FLAG:--regtest}"
  "${QBIT_NODE_EXTRA_ARG:--asert}"
  "${QBIT_P2MR_ONLY_ARG:--p2mronly=0}"
)
bash "${ROOT_DIR}/docker/qbit/qbit-entrypoint.sh" "${qbit_validation_args[@]}"

validate_mining_lanes
check_node_extra_arg_selectors
check_qbit_chain_selection
if mining_lane_enabled auxpow; then
  check_bitcoin_chain_selection
  check_bool_env BITCOIN_DNSSEED
  check_bool_env BITCOIN_DISCOVER
fi
check_ctv_fee_config

check_production_gate
check_release_provenance_gate
if mining_lane_enabled auxpow; then
  check_bitcoin_peer_bootstrap
fi

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

if [[ "${QBIT_CHAIN:-regtest}" == "mainnet" ]] || production_mode_enabled; then
  if [[ -n "${QBIT_SRC_DIR:-}" && ( -f "${QBIT_SRC_DIR}/.qbit-source-commit" || -d "${QBIT_SRC_DIR}/.git" ) ]]; then
    verify_staged_source_commit "${QBIT_SRC_DIR}" "${QBIT_GIT_COMMIT}" "qbit source checkout"
  elif [[ "${QBIT_PROVIDER}" == "git" ]] && git -C "${ROOT_DIR}/generated/qbit-git-cache" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    verify_git_checkout_commit "${ROOT_DIR}/generated/qbit-git-cache" "${QBIT_GIT_COMMIT}" "qbit cache"
  else
    fail "production mode requires a resolved, commit-pinned qbit source; run the source preparation step first"
  fi
fi

if mining_lane_enabled ckpool; then
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

if mining_lane_enabled ckpool; then
  [[ -n "${CKPOOL_GIT_URL:-}" ]] || fail "CKPOOL_GIT_URL must be configured"
  [[ -n "${CKPOOL_GIT_REF:-}" ]] || fail "CKPOOL_GIT_REF must be configured"
fi
if mining_lane_enabled auxpow && [[ -z "${BITCOIN_RELEASE_URL:-}" ]]; then
  [[ -n "${BITCOIN_RELEASE_VERSION:-}" ]] || fail "BITCOIN_RELEASE_VERSION must be set when BITCOIN_RELEASE_URL is unset"
  [[ -n "${BITCOIN_RELEASE_BASE_URL:-}" ]] || fail "BITCOIN_RELEASE_BASE_URL must be set when BITCOIN_RELEASE_URL is unset"
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
printf 'doctor: bitcoin parent chain=%s flag=%s rpc_port=%s p2p_port=%s dnsseed=%s discover=%s\n' \
  "${BITCOIN_CHAIN:-regtest}" \
  "${BITCOIN_CHAIN_FLAG:--regtest}" \
  "${BITCOIN_RPC_PORT:-18443}" \
  "${BITCOIN_P2P_PORT:-18444}" \
  "${BITCOIN_DNSSEED:-0}" \
  "${BITCOIN_DISCOVER:-0}"
printf 'doctor: bitcoin parent extra args=%s\n' "${BITCOIN_NODE_EXTRA_ARGS:-<none>}"
