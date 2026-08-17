#!/usr/bin/env bash

set -euo pipefail

# Readiness gate for the throwaway Postgres containers the PRISM tests spin up.
#
# The official Postgres image initialises a fresh data directory by running a
# temporary server that listens on the unix socket only (listen_addresses=''),
# then shuts it down and starts the real one. `pg_isready` over that socket
# therefore reports "accepting connections" during initialisation, and the very
# next client call can hit `FATAL: database "qbit" does not exist`, `FATAL: the
# database system is shutting down`, or -- once the socket file is gone --
# `connection to server on socket "/var/run/postgresql/.s.PGSQL.5432" failed:
# No such file or directory`. The window is short, so the race only bites when
# the image is already cached and the first probe lands inside it.
#
# Gate on the TCP listener instead (absent until the real server is up) and
# confirm with a real query over the socket the tests actually use.
wait_for_prism_postgres_container() {
  local container="$1"
  local user="${2:-qbit}"
  local database="${3:-qbit}"
  local timeout_seconds="${4:-120}"
  local deadline=$((SECONDS + timeout_seconds))

  while :; do
    if docker exec "${container}" \
      pg_isready -h 127.0.0.1 -U "${user}" -d "${database}" >/dev/null 2>&1 &&
      echo 'SELECT 1;' | docker exec -i "${container}" \
        psql --no-psqlrc --set ON_ERROR_STOP=1 -U "${user}" -d "${database}" \
        >/dev/null 2>&1; then
      return 0
    fi

    if [[ "${SECONDS}" -ge "${deadline}" ]]; then
      echo "timed out waiting for PRISM Postgres container: ${container}" >&2
      docker logs "${container}" >&2 || true
      return 1
    fi

    sleep 1
  done
}
