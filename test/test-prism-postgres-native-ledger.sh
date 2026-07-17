#!/usr/bin/env bash
# End-to-end validation of the persistent pooled psycopg share-ledger backend
# (PRISM_POSTGRES_NATIVE_CLIENT=1) against a real Postgres: schema init, lease
# acquisition, batched synchronous share appends, duplicate handling, cached
# accepted stats, and cross-backend read consistency with the psql fallback.
#
# Requires docker and a host python3 with psycopg installed
# (python3 -m pip install 'psycopg[binary]').
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
POSTGRES_IMAGE="${QBIT_PRISM_POSTGRES_IMAGE:-postgres:16-alpine}"
POSTGRES_CONTAINER="${QBIT_PRISM_POSTGRES_CONTAINER:-qbit-prism-native-ledger-pg-$$}"

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

python3 -c 'import psycopg' 2>/dev/null || {
  echo "host python3 lacks psycopg; install with: python3 -m pip install 'psycopg[binary]'" >&2
  exit 1
}

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
if [[ -z "${HOST_PORT}" ]]; then
  echo "unable to resolve published Postgres port" >&2
  exit 1
fi
DATABASE_URL="postgresql://qbit:qbit@127.0.0.1:${HOST_PORT}/qbit"

# Wait for the published listener too (pg_isready above ran inside the
# container over the unix socket).
deadline=$((SECONDS + 60))
until python3 - "$DATABASE_URL" <<'PY' >/dev/null 2>&1
import sys
import psycopg

with psycopg.connect(sys.argv[1], connect_timeout=3) as conn:
    conn.execute("SELECT 1")
PY
do
  if [[ "${SECONDS}" -ge "${deadline}" ]]; then
    echo "timed out waiting for published PRISM Postgres port" >&2
    exit 1
  fi
  sleep 1
done

(
  cd "${ROOT_DIR}"
  PRISM_TEST_DATABASE_URL="${DATABASE_URL}" \
  PRISM_TEST_PSQL_COMMAND="docker exec -i ${POSTGRES_CONTAINER} psql -U qbit -d qbit" \
    python3 <<'PY'
from __future__ import annotations

import os
import threading

from lab.prism.share_ledger import PendingShare, PsqlShareLedger


def pending(index: int, *, share_id: str | None = None) -> PendingShare:
    return PendingShare(
        share_id=share_id or f"share-{index}",
        miner_id=f"miner-{index % 4}",
        order_key=f"{index:04d}",
        p2mr_program_hex=f"{index % 256:02x}" * 32,
        share_difficulty=100 + index,
        network_difficulty=1_000,
        template_height=10,
        job_id=f"job-{index}",
        job_issued_at_ms=1_700_000_000_000 + index,
        accepted_at_ms=1_700_000_001_000 + index,
        ntime=1_700_000_000 + index,
    )


def assert_equal(actual: object, expected: object, message: str) -> None:
    if actual != expected:
        raise SystemExit(f"{message}: expected {expected!r}, got {actual!r}")


database_url = os.environ["PRISM_TEST_DATABASE_URL"]
psql_command = os.environ["PRISM_TEST_PSQL_COMMAND"]

ledger = PsqlShareLedger(
    psql_command=psql_command,
    database_url=database_url,
    native_client_mode="1",
    writer_id="writer-native",
    writer_epoch=1,
    initialize_schema=True,
    accepted_stats_cache_seconds=60.0,
)
assert_equal(ledger.execution_backend, "psycopg-pool", "native backend selected")

# Concurrent per-share appends exercise the pooled client under the writer
# lock from many threads, each caller getting its own canonical record back.
single_count = 16
records: dict[int, object] = {}
errors: list[BaseException] = []
barrier = threading.Barrier(8)


def run(worker_index: int) -> None:
    try:
        barrier.wait()
        for offset in range(single_count // 8):
            index = worker_index * (single_count // 8) + offset
            records[index] = ledger.append(pending(index))
    except BaseException as exc:  # noqa: BLE001 - surfaced below
        errors.append(exc)


threads = [threading.Thread(target=run, args=(index,)) for index in range(8)]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join(timeout=60)

assert_equal(errors, [], "concurrent appends succeed")
assert_equal(len(records), single_count, "all appends returned records")
assert_equal(
    sorted(record.share_seq for record in records.values()),
    list(range(1, single_count + 1)),
    "canonical share sequence is contiguous",
)
for index, record in records.items():
    assert_equal(record.share_id, f"share-{index}", "each caller got its own share back")
assert_equal(
    ledger.accepted_share_stats()["accepted_share_count"],
    single_count,
    "cached share count before batch",
)

# The coordinator's share-writer group commit lands one atomic append_batch
# statement; on the pooled client that is a single synchronously committed
# round trip.
share_count = 32
batch_entries = [(pending(index), None) for index in range(single_count, share_count)]
batch_records = ledger.append_batch(batch_entries)
assert_equal(
    [record.share_id for record in batch_records],
    [f"share-{index}" for index in range(single_count, share_count)],
    "batch preserves submission order",
)
assert_equal(
    sorted(record.share_seq for record in batch_records),
    list(range(single_count + 1, share_count + 1)),
    "batch extends the canonical sequence",
)
stats_before_replay = ledger.accepted_share_stats()
assert_equal(stats_before_replay["accepted_share_count"], share_count, "cached share count before replay")

# Replaying the exact same batch is idempotent: the same records come back
# and neither the database nor the accepted-share cache double-counts it.
replayed_records = ledger.append_batch(batch_entries)
assert_equal(
    [(record.share_seq, record.share_id) for record in replayed_records],
    [(record.share_seq, record.share_id) for record in batch_records],
    "batch replay returns the originally committed records",
)
assert_equal(
    ledger.accepted_share_stats()["accepted_share_count"],
    share_count,
    "batch replay leaves cached share count unchanged",
)

try:
    ledger.append(pending(999, share_id="share-0"))
except RuntimeError as exc:
    if "duplicate share_id" not in str(exc):
        raise
else:
    raise SystemExit("duplicate share replay unexpectedly appended")

# Stats reconcile against the database (the cache was advanced by the replay
# too, so force a fresh aggregate to prove the committed state is exact).
ledger._accepted_stats_cache_seconds = 0.0
stats = ledger.accepted_share_stats()
assert_equal(stats["accepted_share_count"], share_count, "accepted share count")
assert_equal(stats["distinct_miner_count"], 4, "distinct miner count")
ledger._accepted_stats_cache_seconds = 60.0

snapshot = ledger.snapshot_at_job_issue(1_700_000_002_000)
assert_equal(len(snapshot), share_count, "snapshot returns all committed shares")

metrics = ledger.metrics()
assert_equal(metrics["shares"], share_count, "metrics share count from cached stats")

try:
    PsqlShareLedger(
        psql_command=psql_command,
        database_url=database_url,
        native_client_mode="1",
        writer_id="writer-other",
        writer_epoch=1,
    )
except RuntimeError as exc:
    if "writer-native" not in str(exc):
        raise
else:
    raise SystemExit("second writer stole an unexpired lease over the native client")

released = ledger.release_writer_lease()
assert_equal(released, True, "writer lease released")
ledger.close()

# The psql subprocess fallback must see exactly the rows the native client
# committed (same schema, same data, interchangeable backends).
fallback = PsqlShareLedger(
    psql_command=psql_command,
    native_client_mode="0",
    writer_id="writer-native",
    writer_epoch=1,
)
assert_equal(fallback.execution_backend, "psql-subprocess", "fallback backend selected")
fallback_shares = fallback.all_shares()
assert_equal(len(fallback_shares), share_count, "fallback sees committed shares")
assert_equal(
    sorted(share.share_seq for share in fallback_shares),
    list(range(1, share_count + 1)),
    "fallback sees the same canonical sequence",
)
fallback.release_writer_lease()
fallback.close()

print("prism postgres native ledger: OK")
PY
)

echo "test-prism-postgres-native-ledger: PASS"
