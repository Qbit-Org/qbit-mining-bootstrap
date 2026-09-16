#!/usr/bin/env bash
# Prove the PRISM runtime image runs as a non-root user with core dumps
# disabled, and that its health probes pass as that user. The public API role
# needs only PostgreSQL, so it carries the live health check; the operator
# probe's mounted bearer-token handling is exercised against the same listener.
set -euo pipefail

PRISM_IMAGE="${PRISM_IMAGE:-qbit-lab-prism:ci}"
POSTGRES_IMAGE="${PRISM_SMOKE_POSTGRES_IMAGE:-postgres:16}"
CONTAINER_PREFIX="prism-image-smoke-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}-$$"
NETWORK="${CONTAINER_PREFIX}-network"
POSTGRES_CONTAINER="${CONTAINER_PREFIX}-postgres"
PUBLIC_CONTAINER="${CONTAINER_PREFIX}-public-api"
SECRETS_VOLUME="${CONTAINER_PREFIX}-secrets"
DATABASE_URL="postgresql://prism_smoke:prism_smoke@${POSTGRES_CONTAINER}:5432/prism_smoke"
SECRETS_DIR=/run/secrets/qbit-prism

fail() {
  printf 'prism image smoke: FAIL: %s\n' "$*" >&2
  exit 1
}

cleanup() {
  docker rm --force "${PUBLIC_CONTAINER}" "${POSTGRES_CONTAINER}" >/dev/null 2>&1 || true
  docker volume rm "${SECRETS_VOLUME}" >/dev/null 2>&1 || true
  docker network rm "${NETWORK}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

run_tool() {
  docker run --rm --network none --no-healthcheck "$@"
}

# Image metadata: a numeric non-root user and the operator health probe.
image_user="$(docker image inspect --format '{{.Config.User}}' "${PRISM_IMAGE}")"
[[ "${image_user}" =~ ^[0-9]+:[0-9]+$ ]] || fail "image USER must be numeric uid:gid, got '${image_user}'"
[[ "${image_user%%:*}" != "0" ]] || fail "image USER must not be root"
image_healthcheck="$(docker image inspect --format '{{json .Config.Healthcheck.Test}}' "${PRISM_IMAGE}")"
[[ "${image_healthcheck}" == '["CMD","qbit-prism-server","healthcheck"]' ]] ||
  fail "unexpected image HEALTHCHECK: ${image_healthcheck}"

# The entrypoint lowers the core limit even when the runtime grants unlimited.
uid="$(run_tool "${PRISM_IMAGE}" id -u)"
[[ "${uid}" =~ ^[0-9]+$ && "${uid}" != "0" ]] || fail "container runs as uid '${uid}'"
# shellcheck disable=SC2016 # the container shell expands the limits
core_limits="$(run_tool --ulimit core=-1:-1 "${PRISM_IMAGE}" sh -c 'printf "%s %s" "$(ulimit -Sc)" "$(ulimit -Hc)"')"
[[ "${core_limits}" == "0 0" ]] || fail "core limit is '${core_limits}', expected '0 0'"
for binary in qbit-prism-server qbit-prism-miner; do
  run_tool "${PRISM_IMAGE}" "${binary}" --help >/dev/null || fail "${binary} --help failed as uid ${uid}"
done
printf 'prism image smoke: user %s, core limit %s\n' "${image_user}" "${core_limits}"

docker network create "${NETWORK}" >/dev/null
docker run --detach --name "${POSTGRES_CONTAINER}" --network "${NETWORK}" \
  --env POSTGRES_DB=prism_smoke \
  --env POSTGRES_USER=prism_smoke \
  --env POSTGRES_PASSWORD=prism_smoke \
  "${POSTGRES_IMAGE}" >/dev/null
# Wait on TCP: initdb's temporary server listens on the socket only.
postgres_ready=0
for _attempt in {1..60}; do
  if docker exec "${POSTGRES_CONTAINER}" pg_isready -h 127.0.0.1 -U prism_smoke -d prism_smoke >/dev/null 2>&1; then
    postgres_ready=1
    break
  fi
  sleep 1
done
[[ "${postgres_ready}" == "1" ]] || fail "PostgreSQL did not become ready"

if ! migrate_output="$(docker run --rm --network "${NETWORK}" --no-healthcheck \
  --env PRISM_DATABASE_URL="${DATABASE_URL}" \
  "${PRISM_IMAGE}" qbit-prism-server migrate 2>&1)"; then
  printf '%s\n' "${migrate_output}" >&2
  fail "migrate failed as uid ${uid}"
fi

# An operator-provided token owned by the image user, and one only root can read.
docker volume create "${SECRETS_VOLUME}" >/dev/null
run_tool --user 0:0 --volume "${SECRETS_VOLUME}:/secrets" "${PRISM_IMAGE}" sh -c "
  printf 'smoke-operator-token\n' > /secrets/operator-bearer-token
  printf 'root-only\n' > /secrets/root-only-token
  chown ${uid}:${uid} /secrets/operator-bearer-token
  chmod 0400 /secrets/operator-bearer-token /secrets/root-only-token
  chmod 0555 /secrets
"

docker run --detach --name "${PUBLIC_CONTAINER}" --network "${NETWORK}" \
  --ulimit core=-1:-1 \
  --health-cmd 'qbit-prism-server healthcheck --public-api' \
  --health-interval 2s --health-timeout 5s --health-retries 3 --health-start-period 30s \
  --volume "${SECRETS_VOLUME}:${SECRETS_DIR}:ro" \
  --env PRISM_DATABASE_URL="${DATABASE_URL}" \
  --env PRISM_PUBLIC_REPLICA_MODE=off \
  --env PRISM_PUBLIC_STRATUM_URL=stratum+tcp://pool.example.invalid:3340 \
  "${PRISM_IMAGE}" qbit-prism-server public-api >/dev/null

health=""
for _attempt in {1..45}; do
  health="$(docker inspect --format '{{.State.Health.Status}}' "${PUBLIC_CONTAINER}")"
  [[ "${health}" != "healthy" ]] || break
  if [[ "$(docker inspect --format '{{.State.Running}}' "${PUBLIC_CONTAINER}")" != "true" ]]; then
    docker logs "${PUBLIC_CONTAINER}" >&2 || true
    fail "public API exited before becoming healthy"
  fi
  sleep 2
done
if [[ "${health}" != "healthy" ]]; then
  docker logs "${PUBLIC_CONTAINER}" >&2 || true
  fail "public API health is '${health}'"
fi

pid1_uid="$(docker exec "${PUBLIC_CONTAINER}" awk '/^Uid:/ { print $2 }' /proc/1/status)"
[[ "${pid1_uid}" == "${uid}" ]] || fail "public API PID 1 runs as uid '${pid1_uid}'"
pid1_core="$(docker exec "${PUBLIC_CONTAINER}" awk '/^Max core file size/ { print $5, $6 }' /proc/1/limits)"
[[ "${pid1_core}" == "0 0" ]] || fail "public API PID 1 core limit is '${pid1_core}'"

# The operator probe reads its mounted token as the image user and sends it;
# an unreadable token fails closed instead of probing without credentials.
public_url=http://127.0.0.1:3342/healthz
docker exec --env PRISM_OPERATOR_BEARER_TOKEN_FILE="${SECRETS_DIR}/operator-bearer-token" \
  "${PUBLIC_CONTAINER}" qbit-prism-server healthcheck --url "${public_url}" ||
  fail "operator healthcheck could not use a token file owned by uid ${uid}"
if unreadable="$(docker exec --env PRISM_OPERATOR_BEARER_TOKEN_FILE="${SECRETS_DIR}/root-only-token" \
  "${PUBLIC_CONTAINER}" qbit-prism-server healthcheck --url "${public_url}" 2>&1)"; then
  fail "operator healthcheck read a root-only token file"
fi
[[ "${unreadable}" == *"cannot open PRISM_OPERATOR_BEARER_TOKEN_FILE"* ]] ||
  fail "unexpected unreadable-token diagnostic: ${unreadable}"
# Without an operator listener the image's default probe must not pass.
if docker exec "${PUBLIC_CONTAINER}" qbit-prism-server healthcheck >/dev/null 2>&1; then
  fail "default operator healthcheck passed without an operator listener"
fi

printf 'prism image smoke: public API healthy as uid %s with core dumps disabled\n' "${uid}"

# Exercise dedicated-reader authentication through the real Compose merges,
# image entrypoint and HTTP healthcheck, using disposable synthetic databases.
PRISM_CREDENTIAL_TEST_IMAGE="${PRISM_IMAGE}" python3 -m unittest -v tests.test_prism_public_credentials
