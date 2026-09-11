#!/usr/bin/env bash
# PRISM public-read standby bootstrap (issue #145).
#
# The prism-postgres-replica data directory is never initdb'd: it is cloned
# from prism-postgres with pg_basebackup and then handed to the stock postgres
# image entrypoint, which sees a populated PGDATA, skips initialization, and
# starts the server. Recovery is driven by the standby.signal and
# primary_conninfo that `pg_basebackup -R` writes.
#
# See docs/prism-postgres-replica.md.
set -euo pipefail

PGDATA="${PGDATA:-/var/lib/postgresql/data}"
PRIMARY_HOST="${PRISM_POSTGRES_PRIMARY_HOST:-prism-postgres}"
PRIMARY_PORT="${PRISM_POSTGRES_PRIMARY_PORT:-5432}"
REPLICATION_USER="${PRISM_POSTGRES_USER:-${POSTGRES_USER:-qbit}}"
REPLICATION_DB="${PRISM_POSTGRES_DB:-${POSTGRES_DB:-postgres}}"
REPLICATION_SLOT="${PRISM_POSTGRES_REPLICATION_SLOT:-prism_public_replica}"
ATTEMPTS="${PRISM_POSTGRES_REPLICA_BASEBACKUP_ATTEMPTS:-60}"
RETRY_SECONDS="${PRISM_POSTGRES_REPLICA_BASEBACKUP_RETRY_SECONDS:-5}"

export PGPASSWORD="${PGPASSWORD:-${PRISM_POSTGRES_PASSWORD:-${POSTGRES_PASSWORD:-}}}"

log() {
  printf 'prism-postgres-replica: %s\n' "$*" >&2
}

# postgres refuses to run as root and pg_basebackup would leave root-owned
# files the server cannot read, so drop to the image's postgres account for
# both. gosu and su-exec both ship in the official images; PGPASSWORD is
# exported, and both tools preserve the environment.
as_postgres() {
  if [ "$(id -u)" = '0' ]; then
    if command -v gosu >/dev/null 2>&1; then
      gosu postgres "$@"
    elif command -v su-exec >/dev/null 2>&1; then
      su-exec postgres "$@"
    else
      log "no gosu or su-exec in this image; cannot drop privileges"
      return 1
    fi
  else
    "$@"
  fi
}

slot_exists() {
  local found
  found="$(as_postgres psql \
    --host="$PRIMARY_HOST" \
    --port="$PRIMARY_PORT" \
    --username="$REPLICATION_USER" \
    --dbname="$REPLICATION_DB" \
    --no-align --tuples-only \
    --command="SELECT 1 FROM pg_replication_slots WHERE slot_name = '${REPLICATION_SLOT}'" \
    2>/dev/null)" || return 1
  [ "$found" = "1" ]
}

# standby.signal is the completion marker, not PG_VERSION: pg_basebackup writes
# PG_VERSION early in the stream but standby.signal only once the backup has
# finished, so an interrupted bootstrap is distinguishable from a usable
# standby. This volume only ever holds a standby produced by this script, and
# the replica is never promoted, so leftovers without standby.signal are debris
# and are cleared before retrying.
if [ ! -f "$PGDATA/standby.signal" ]; then
  # Refuse to clear a directory that holds a complete cluster. The compose
  # volume only ever holds standbys made here, but
  # PRISM_POSTGRES_REPLICA_DATA_SOURCE is a production bind mount: aimed by
  # mistake at the primary's data directory, the clear below would destroy it.
  #
  # pg_controldata is the discriminator rather than PG_VERSION, because
  # PG_VERSION is also present in the debris of an interrupted base backup,
  # which this script must still be free to clear. A readable pg_control means
  # a cluster that finished being built; anything less is debris. No marker
  # file can serve here -- pg_basebackup refuses a target directory that is
  # not empty, dot files included.
  if as_postgres pg_controldata "$PGDATA" >/dev/null 2>&1; then
    log "refusing to clear ${PGDATA}: it holds a complete Postgres cluster that is not a standby"
    log "point PRISM_POSTGRES_REPLICA_DATA_SOURCE at a dedicated volume, or empty it deliberately"
    exit 1
  fi
  attempt=1
  while :; do
    if [ -n "$(ls -A "$PGDATA" 2>/dev/null)" ]; then
      log "clearing incomplete data directory ${PGDATA} before base backup"
      find "$PGDATA" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
    fi

    if ! pg_isready --host="$PRIMARY_HOST" --port="$PRIMARY_PORT" --quiet; then
      log "primary ${PRIMARY_HOST}:${PRIMARY_PORT} not accepting connections (attempt ${attempt}/${ATTEMPTS})"
    else
      basebackup=(pg_basebackup
        --host="$PRIMARY_HOST"
        --port="$PRIMARY_PORT"
        --username="$REPLICATION_USER"
        --pgdata="$PGDATA"
        --format=plain
        --wal-method=stream
        --progress
        --write-recovery-conf
        --slot="$REPLICATION_SLOT")
      # -C creates the physical slot as part of the backup. A slot left behind
      # by an earlier replica (the primary keeps it while the standby is gone)
      # would make -C fail, so reuse it instead of recreating it.
      if slot_exists; then
        log "reusing existing replication slot ${REPLICATION_SLOT}"
      else
        basebackup+=(--create-slot)
      fi

      log "base backup from ${PRIMARY_HOST}:${PRIMARY_PORT} (attempt ${attempt}/${ATTEMPTS})"
      if as_postgres "${basebackup[@]}"; then
        log "base backup complete; starting standby"
        break
      fi
      log "base backup failed (attempt ${attempt}/${ATTEMPTS})"
    fi

    if [ "$attempt" -ge "$ATTEMPTS" ]; then
      log "giving up after ${ATTEMPTS} attempts"
      exit 1
    fi
    attempt=$((attempt + 1))
    sleep "$RETRY_SECONDS"
  done
fi

# The stock entrypoint chowns/chmods PGDATA, drops to the postgres user, sees a
# populated data directory, and skips initialization. It also picks up the
# pg_hba.conf pg_basebackup copied from the primary's data directory, which
# keeps the loopback healthcheck connections working.
exec docker-entrypoint.sh postgres
