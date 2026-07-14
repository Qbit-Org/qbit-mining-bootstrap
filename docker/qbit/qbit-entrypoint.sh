#!/usr/bin/env bash
set -euo pipefail

MAX_SIGNED_INT64=9223372036854775807
PRELAUNCH_TIP_AGE_ARG=""

fail() {
  printf 'qbit entrypoint: FAIL: %s\n' "$*" >&2
  exit 1
}

parse_bool() {
  local name="$1"
  local value="$2"
  local normalized

  normalized="$(printf '%s' "${value}" | tr '[:upper:]' '[:lower:]' | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
  case "${normalized}" in
    1|true|yes|on) PARSED_BOOL=1 ;;
    0|false|no|off) PARSED_BOOL=0 ;;
    *) fail "${name} must be a true/false style value" ;;
  esac
}

validate_positive_int64() {
  local name="$1"
  local value="$2"
  local normalized="${value}"

  [[ -n "${value}" ]] || fail "${name} must not be empty"
  [[ "${value}" =~ ^[0-9]+$ ]] || fail "${name} must be a positive integer"
  while [[ "${#normalized}" -gt 1 && "${normalized:0:1}" == "0" ]]; do
    normalized="${normalized:1}"
  done
  [[ "${normalized}" != "0" ]] || fail "${name} must be greater than zero"
  # Equal-width ASCII decimal strings can be compared lexically.
  # shellcheck disable=SC2071
  if [[ "${#normalized}" -gt "${#MAX_SIGNED_INT64}" ]] ||
    { [[ "${#normalized}" -eq "${#MAX_SIGNED_INT64}" ]] && [[ "${normalized}" > "${MAX_SIGNED_INT64}" ]]; }; then
    fail "${name} exceeds qbitd's signed 64-bit integer range"
  fi
}

validate_mainnet_qbitd_args() {
  local arg
  local normalized

  for arg in "$@"; do
    normalized="$(printf '%s' "${arg}" | tr '[:upper:]' '[:lower:]')"
    case "${normalized}" in
      -regtest=0|-regtest=false|-signet=0|-signet=false|-testnet=0|-testnet=false|-testnet3=0|-testnet3=false|-testnet4=0|-testnet4=false)
        ;;
      -regtest|-regtest=*|-signet|-signet=*|-testnet|-testnet=*|-testnet3|-testnet3=*|-testnet4|-testnet4=*|-chain=regtest|-chain=signet|-chain=testnet|-chain=testnet3|-chain=testnet4)
        fail "QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS cannot be used when qbitd selects a test chain with ${arg}"
        ;;
    esac
  done
}

reject_caller_tip_age_args() {
  local arg
  local normalized

  for arg in "$@"; do
    normalized="$(printf '%s' "${arg}" | tr '[:upper:]' '[:lower:]')"
    case "${normalized}" in
      -maxtipage|-maxtipage=*|--maxtipage|--maxtipage=*)
        fail "caller-provided ${arg} is not allowed; use QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS for an explicitly authorized mainnet prelaunch"
        ;;
    esac
  done
}

configure_prelaunch_tip_age() {
  local production_value="${QBIT_PRODUCTION:-0}"
  local tools_production_value="${QBIT_TOOLS_PRODUCTION:-0}"
  local readiness_gate_value="${CKPOOL_NON_TEST_READINESS_GATE:-1}"
  local launch_value="${QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED:-}"
  local production_enabled
  local tools_production_enabled
  local readiness_gate_enabled
  local launch_checks_enabled=1
  local duration_name="QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS"
  local duration

  parse_bool QBIT_PRODUCTION "${production_value}"
  production_enabled="${PARSED_BOOL}"
  if [[ -n "${launch_value}" ]]; then
    parse_bool QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED "${launch_value}"
    launch_checks_enabled="${PARSED_BOOL}"
  fi
  parse_bool QBIT_TOOLS_PRODUCTION "${tools_production_value}"
  tools_production_enabled="${PARSED_BOOL}"
  parse_bool CKPOOL_NON_TEST_READINESS_GATE "${readiness_gate_value}"
  readiness_gate_enabled="${PARSED_BOOL}"

  if [[ "${QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS+x}" != "x" ]]; then
    return
  fi

  duration="${QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS}"
  validate_positive_int64 "${duration_name}" "${duration}"
  [[ "${production_enabled}" == "1" ]] ||
    fail "${duration_name} is valid only with QBIT_PRODUCTION=1"
  [[ "${QBIT_CHAIN:-regtest}" == "mainnet" ]] ||
    fail "${duration_name} is valid only with QBIT_CHAIN=mainnet"

  if [[ "${launch_checks_enabled}" == "0" ]]; then
    [[ "${tools_production_enabled}" == "1" ]] ||
      fail "${duration_name} requires QBIT_TOOLS_PRODUCTION=1"
    [[ "${readiness_gate_enabled}" == "0" ]] ||
      fail "${duration_name} requires CKPOOL_NON_TEST_READINESS_GATE=0"
    validate_mainnet_qbitd_args "$@"
    PRELAUNCH_TIP_AGE_ARG="-maxtipage=${duration}"
  fi
}

reject_caller_tip_age_args "$@"
configure_prelaunch_tip_age "$@"

if [[ "${1:-}" == "--validate-only" ]]; then
  exit 0
fi

qbitd_args=("$@")
if [[ -n "${PRELAUNCH_TIP_AGE_ARG}" ]]; then
  qbitd_args+=("${PRELAUNCH_TIP_AGE_ARG}")
fi
exec qbitd "${qbitd_args[@]}"
