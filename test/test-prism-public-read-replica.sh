#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
POSTGRES_IMAGE="${QBIT_PRISM_POSTGRES_IMAGE:-postgres:16-alpine}"
NETWORK="qbit-prism-pubrep-net-$$"
PRIMARY="qbit-prism-pubrep-primary-$$"
REPLICA="qbit-prism-pubrep-replica-$$"

require_executable() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required executable: $1" >&2
    exit 1
  }
}

cleanup() {
  docker rm -f "${PRIMARY}" "${REPLICA}" >/dev/null 2>&1 || true
  docker network rm "${NETWORK}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

require_executable python3
require_executable docker

docker network create "${NETWORK}" >/dev/null

# The primary runs the same mounted pg_hba.conf the compose service uses, so
# the replication rules (and the local trust the healthcheck depends on) are
# the production-shaped ones.
docker run \
  --rm \
  --detach \
  --name "${PRIMARY}" \
  --network "${NETWORK}" \
  -e POSTGRES_USER=qbit \
  -e POSTGRES_PASSWORD=qbit \
  -e POSTGRES_DB=qbit \
  -v "${ROOT_DIR}/config/prism-postgres/pg_hba.conf:/etc/postgresql/pg_hba.conf:ro" \
  "${POSTGRES_IMAGE}" \
  postgres -c hba_file=/etc/postgresql/pg_hba.conf >/dev/null

deadline=$((SECONDS + 60))
until docker exec "${PRIMARY}" pg_isready -h 127.0.0.1 -p 5432 -U qbit -d qbit >/dev/null 2>&1 \
  && sleep 1 \
  && docker exec "${PRIMARY}" pg_isready -h 127.0.0.1 -p 5432 -U qbit -d qbit >/dev/null 2>&1; do
  if [ "${SECONDS}" -ge "${deadline}" ]; then
    echo "timed out waiting for the primary Postgres container" >&2
    docker logs "${PRIMARY}" >&2 || true
    exit 1
  fi
  sleep 1
done

# The standby reuses the compose entrypoint: pg_basebackup with slot creation,
# then the stock image entrypoint. The schema lands later, through streaming.
docker run \
  --rm \
  --detach \
  --name "${REPLICA}" \
  --network "${NETWORK}" \
  -e POSTGRES_USER=qbit \
  -e POSTGRES_PASSWORD=qbit \
  -e POSTGRES_DB=qbit \
  -e PRISM_POSTGRES_PRIMARY_HOST="${PRIMARY}" \
  -e PRISM_POSTGRES_PRIMARY_PORT=5432 \
  -e PRISM_POSTGRES_REPLICATION_SLOT=prism_public_replica_gate \
  -v "${ROOT_DIR}/config/prism-postgres/replica-entrypoint.sh:/usr/local/bin/prism-replica-entrypoint.sh:ro" \
  --entrypoint /usr/local/bin/prism-replica-entrypoint.sh \
  "${POSTGRES_IMAGE}" >/dev/null

deadline=$((SECONDS + 120))
until docker exec "${REPLICA}" pg_isready -h 127.0.0.1 -p 5432 -U qbit -d qbit >/dev/null 2>&1 \
  && [ "$(docker exec "${REPLICA}" psql -U qbit -d qbit -tAc 'SELECT pg_is_in_recovery()' 2>/dev/null | tr -d '[:space:]')" = "t" ]; do
  if [ "${SECONDS}" -ge "${deadline}" ]; then
    echo "timed out waiting for the replica standby container" >&2
    docker logs "${REPLICA}" >&2 || true
    exit 1
  fi
  sleep 1
done

(
  cd "${ROOT_DIR}"
  GATE_PRIMARY_PSQL_COMMAND="docker exec -i ${PRIMARY} psql -U qbit -d qbit" \
  GATE_REPLICA_PSQL_COMMAND="docker exec -i ${REPLICA} psql -U qbit -d qbit" \
  PYTHONPATH="${ROOT_DIR}" \
    python3 tests/prism_public_read_replica_gate.py
)
