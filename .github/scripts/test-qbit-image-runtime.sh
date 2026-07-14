#!/usr/bin/env bash
set -euo pipefail

QBIT_IMAGE="${QBIT_IMAGE:-${QBIT_CI_IMAGE:-}}"
TIP_AGE_SECONDS=123456789
CONTAINER_PREFIX="qbit-runtime-smoke-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}-$$"
REGTEST_CONTAINER="${CONTAINER_PREFIX}-regtest"
PRELAUNCH_CONTAINER="${CONTAINER_PREFIX}-prelaunch"
LAUNCHED_CONTAINER="${CONTAINER_PREFIX}-launched"
CONTAINERS=(
  "${REGTEST_CONTAINER}"
  "${PRELAUNCH_CONTAINER}"
  "${LAUNCHED_CONTAINER}"
)

fail() {
  printf 'qbit runtime image smoke: FAIL: %s\n' "$*" >&2
  exit 1
}

cleanup() {
  local container
  for container in "${CONTAINERS[@]}"; do
    if docker container inspect "${container}" >/dev/null 2>&1; then
      docker stop --timeout 10 "${container}" >/dev/null 2>&1 || true
      docker rm --force "${container}" >/dev/null 2>&1 || true
    fi
  done
}
trap cleanup EXIT

require_clean_exit() {
  local container="$1"
  local exit_code

  docker stop --timeout 30 "${container}" >/dev/null
  exit_code="$(docker container inspect --format '{{.State.ExitCode}}' "${container}")"
  [[ "${exit_code}" == "0" ]] || {
    docker logs "${container}" >&2 || true
    fail "${container} exited with status ${exit_code} after SIGTERM"
  }
  docker rm "${container}" >/dev/null
}

wait_for_rpc() {
  local container="$1"
  local chain_arg="$2"
  local expected_chain="$3"
  local rpc_output=""
  local _attempt

  for _attempt in {1..30}; do
    if rpc_output="$(
      docker exec "${container}" \
        qbit-cli "${chain_arg}" -datadir=/var/lib/qbit getblockchaininfo 2>/dev/null
    )"; then
      [[ "${rpc_output}" == *"\"chain\": \"${expected_chain}\""* ]] ||
        fail "${container} RPC reported an unexpected chain: ${rpc_output}"
      return
    fi
    if [[ "$(docker container inspect --format '{{.State.Running}}' "${container}")" != "true" ]]; then
      docker logs "${container}" >&2 || true
      fail "${container} exited before its RPC endpoint became ready"
    fi
    sleep 1
  done

  docker logs "${container}" >&2 || true
  fail "${container} RPC endpoint did not become ready"
}

wait_for_qbitd_argv() {
  local container="$1"
  local argv=""
  local _attempt

  for _attempt in {1..30}; do
    if argv="$(
      docker exec "${container}" sh -c '
        for comm in /proc/[0-9]*/comm; do
          if [ "$(cat "${comm}")" = qbitd ]; then
            tr "\000" "\n" < "${comm%/comm}/cmdline"
            exit 0
          fi
        done
        exit 1
      ' 2>/dev/null
    )"; then
      printf '%s\n' "${argv}"
      return
    fi
    if [[ "$(docker container inspect --format '{{.State.Running}}' "${container}")" != "true" ]]; then
      docker logs "${container}" >&2 || true
      fail "${container} exited before qbitd argv could be inspected"
    fi
    sleep 1
  done

  docker logs "${container}" >&2 || true
  fail "${container} did not expose a running qbitd process"
}

count_exact_arg() {
  local argv="$1"
  local expected="$2"
  local arg
  local count=0

  while IFS= read -r arg; do
    if [[ "${arg}" == "${expected}" ]]; then
      ((count += 1))
    fi
  done <<< "${argv}"
  printf '%s\n' "${count}"
}

assert_no_tip_age_arg() {
  local argv="$1"
  local arg
  local normalized

  while IFS= read -r arg; do
    normalized="$(printf '%s' "${arg}" | tr '[:upper:]' '[:lower:]')"
    case "${normalized}" in
      -maxtipage|--maxtipage|-maxtipage=*|--maxtipage=*)
        fail "launched qbitd retained a tip-age argument: ${arg}"
        ;;
    esac
  done <<< "${argv}"
}

start_mainnet_container() {
  local container="$1"
  local readiness_gate="$2"
  local launch_checks="$3"

  docker run --detach \
    --name "${container}" \
    --network none \
    --tmpfs /var/lib/qbit:rw,noexec,nosuid,size=512m \
    --env QBIT_PRODUCTION=1 \
    --env QBIT_TOOLS_PRODUCTION=1 \
    --env QBIT_CHAIN=mainnet \
    --env "CKPOOL_NON_TEST_READINESS_GATE=${readiness_gate}" \
    --env "QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED=${launch_checks}" \
    --env "QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS=${TIP_AGE_SECONDS}" \
    "${QBIT_IMAGE}" \
    -chain=main \
    -datadir=/var/lib/qbit \
    -printtoconsole \
    -server=1 \
    -listen=0 \
    -connect=0 \
    -dnsseed=0 \
    -fixedseeds=0 \
    -discover=0 >/dev/null
}

[[ -n "${QBIT_IMAGE}" ]] || fail "QBIT_IMAGE or QBIT_CI_IMAGE must name the image to test"
docker image inspect "${QBIT_IMAGE}" >/dev/null || fail "image does not exist: ${QBIT_IMAGE}"

entrypoint="$(docker image inspect --format '{{json .Config.Entrypoint}}' "${QBIT_IMAGE}")"
[[ "${entrypoint}" == '["/usr/bin/tini","--","/usr/local/bin/qbit-entrypoint.sh"]' ]] ||
  fail "unexpected image entrypoint: ${entrypoint}"

version_output="$(docker run --rm "${QBIT_IMAGE}" -version)"
[[ "${version_output}" == *"qbit daemon version"* ]] ||
  fail "packaged qbitd did not report its version"

set +e
caller_output="$(docker run --rm "${QBIT_IMAGE}" -maxtipage=1 -version 2>&1)"
caller_status=$?
set -e
[[ "${caller_status}" -ne 0 ]] || fail "caller-provided -maxtipage unexpectedly succeeded"
[[ "${caller_output}" == *"caller-provided -maxtipage=1 is not allowed"* ]] ||
  fail "caller-provided -maxtipage failed without the entrypoint rejection"

docker run --detach \
  --name "${REGTEST_CONTAINER}" \
  --network none \
  --tmpfs /var/lib/qbit:rw,noexec,nosuid,size=512m \
  "${QBIT_IMAGE}" \
  -regtest \
  -asert \
  -datadir=/var/lib/qbit \
  -printtoconsole \
  -server=1 \
  -listen=0 \
  -connect=0 \
  -dnsseed=0 \
  -fixedseeds=0 \
  -discover=0 >/dev/null
wait_for_rpc "${REGTEST_CONTAINER}" -regtest regtest
processes="$(docker top "${REGTEST_CONTAINER}" -eo pid,comm)"
[[ "${processes}" == *"tini"* && "${processes}" == *"qbitd"* ]] ||
  fail "runtime process chain does not contain both tini and qbitd: ${processes}"
require_clean_exit "${REGTEST_CONTAINER}"

start_mainnet_container "${PRELAUNCH_CONTAINER}" 0 0
prelaunch_argv="$(wait_for_qbitd_argv "${PRELAUNCH_CONTAINER}")"
managed_arg="-maxtipage=${TIP_AGE_SECONDS}"
managed_count="$(count_exact_arg "${prelaunch_argv}" "${managed_arg}")"
[[ "${managed_count}" == "1" ]] ||
  fail "prelaunch qbitd expected one ${managed_arg}, found ${managed_count}: ${prelaunch_argv}"
wait_for_rpc "${PRELAUNCH_CONTAINER}" -chain=main main
require_clean_exit "${PRELAUNCH_CONTAINER}"

start_mainnet_container "${LAUNCHED_CONTAINER}" 0 1
launched_argv="$(wait_for_qbitd_argv "${LAUNCHED_CONTAINER}")"
assert_no_tip_age_arg "${launched_argv}"
wait_for_rpc "${LAUNCHED_CONTAINER}" -chain=main main
require_clean_exit "${LAUNCHED_CONTAINER}"

printf 'qbit runtime image smoke: PASS\n'
