#!/usr/bin/env bash
# Real-PostgreSQL gate for chunked candidate bodies (issue #255). Provisions a
# disposable postgres container and drives tests/prism_postgres_candidate_gate.py
# through the psql subprocess backend and, when the host python has psycopg,
# the native pooled client too.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
POSTGRES_IMAGE="${QBIT_PRISM_POSTGRES_IMAGE:-postgres:16-alpine}"
POSTGRES_CONTAINER="${QBIT_PRISM_POSTGRES_CONTAINER:-qbit-prism-candidate-pg-$$}"

require_executable() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required executable: $1" >&2
    exit 1
  }
}

cleanup() {
  docker rm -f "${POSTGRES_CONTAINER}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

require_executable docker
require_executable python3

docker rm -f "${POSTGRES_CONTAINER}" >/dev/null 2>&1 || true
docker run \
  --rm \
  --detach \
  --name "${POSTGRES_CONTAINER}" \
  -p 127.0.0.1:0:5432 \
  -e POSTGRES_USER=qbit \
  -e POSTGRES_PASSWORD=qbit \
  -e POSTGRES_DB=qbit \
  "${POSTGRES_IMAGE}" >/dev/null

deadline=$((SECONDS + 60))
until docker exec "${POSTGRES_CONTAINER}" pg_isready -U qbit -d qbit >/dev/null 2>&1; do
  if [[ "${SECONDS}" -ge "${deadline}" ]]; then
    echo "timed out waiting for PRISM Postgres container" >&2
    docker logs "${POSTGRES_CONTAINER}" >&2 || true
    exit 1
  fi
  sleep 1
done

HOST_PORT="$(docker port "${POSTGRES_CONTAINER}" 5432/tcp | head -n 1 | awk -F: '{print $NF}')"
DATABASE_URL=""
if [[ -n "${HOST_PORT}" ]]; then
  DATABASE_URL="postgresql://qbit:qbit@127.0.0.1:${HOST_PORT}/qbit"
fi

(
  cd "${ROOT_DIR}"
  PRISM_TEST_PSQL_COMMAND="docker exec -i ${POSTGRES_CONTAINER} psql -U qbit -d qbit" \
  PRISM_TEST_DATABASE_URL="${DATABASE_URL}" \
    python3 -m tests.prism_postgres_candidate_gate
)

echo "test-prism-postgres-candidate-storage: PASS"
