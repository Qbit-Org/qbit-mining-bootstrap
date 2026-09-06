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
  PRISM_TEST_PSQL_COMMAND_WITH_ENV="docker exec -i -e PGOPTIONS ${POSTGRES_CONTAINER} psql -U qbit -d qbit" \
    python3 <<'PY'
from __future__ import annotations

import os
import threading
import time

from lab.prism.share_ledger import (
    LedgerOperationTimeout,
    PendingShare,
    PsqlShareLedger,
)
from tests.prism_window_rows_reference import assert_rows_match_aggregate


def pending(
    index: int,
    *,
    share_id: str | None = None,
    miner_id: str | None = None,
) -> PendingShare:
    return PendingShare(
        share_id=share_id or f"share-{index}",
        miner_id=miner_id or f"miner-{index % 4}",
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
# Session GUCs reach a psql subprocess through PGOPTIONS, and "docker exec"
# does not forward the caller's environment, so the read-only assertions below
# use a command that asks docker to pass the variable through.
psql_command_with_env = os.environ["PRISM_TEST_PSQL_COMMAND_WITH_ENV"]

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
batch_entries[0] = (pending(single_count, miner_id="miner-new"), None)
batch_entries[1] = (pending(single_count + 1, miner_id="miner-new"), None)
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
assert_equal(
    stats_before_replay["distinct_miner_count"],
    5,
    "batch increments a new miner exactly once",
)

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
assert_equal(stats["distinct_miner_count"], 5, "distinct miner count")
ledger._accepted_stats_cache_seconds = 60.0

snapshot = ledger.snapshot_at_job_issue(1_700_000_002_000)
assert_equal(len(snapshot), share_count, "snapshot returns all committed shares")


metrics = ledger.metrics()
assert_equal(metrics["shares"], share_count, "metrics share count from cached stats")

# Issue #211: the replay enumeration is a read and must not queue on the
# coordinator-local writer gate. This is the production call shape -- the real
# statement, over the real pooled psycopg client, against a real server -- run
# while an accounting-shaped writer holds that gate for longer than the whole
# fast-call budget. Before the fix the page spent that budget on admission and
# never reached PostgreSQL.
candidate_hash = "ab" * 32
second_candidate_hash = "cd" * 32
candidate_intent = {
    "schema": "qbit.prism.block-candidate-intent.v1",
    "block_hash_hex": candidate_hash,
    "block_hex": "00",
}
assert ledger.persist_block_candidate_intent(candidate_intent)
assert ledger.persist_block_candidate_intent(
    {**candidate_intent, "block_hash_hex": second_candidate_hash}
)

FAST_CALL_BUDGET_SECONDS = 1.0
gate_taken = threading.Event()
release_gate = threading.Event()
holder_error: list[BaseException] = []


def hold_writer_gate() -> None:
    try:
        with ledger._operation_gate(ledger._lock, "writer lock"):
            gate_taken.set()
            release_gate.wait(timeout=60)
    except BaseException as exc:  # noqa: BLE001 - surfaced below
        holder_error.append(exc)
        gate_taken.set()


holder = threading.Thread(target=hold_writer_gate, daemon=True)
holder.start()
if not gate_taken.wait(timeout=30):
    raise SystemExit("the writer gate holder never acquired the gate")
if holder_error:
    raise holder_error[0]

try:
    started = time.monotonic()
    with ledger.operation_timeout(FAST_CALL_BUDGET_SECONDS):
        page = ledger.pending_block_candidate_rows(limit=1)
    enumeration_seconds = time.monotonic() - started

    # It completed, inside its own budget, with the gate still held.
    assert_equal(
        [row["block_hash"] for row in page],
        [candidate_hash],
        "pending page enumerated while the writer gate was held",
    )
    assert_equal(
        [row["pool_block_exists"] for row in page],
        [False],
        "pending page carried the landed-block fact from the same snapshot",
    )
    if enumeration_seconds >= FAST_CALL_BUDGET_SECONDS:
        raise SystemExit(
            "pending page took "
            f"{enumeration_seconds:.3f}s of a {FAST_CALL_BUDGET_SECONDS:g}s budget"
        )

    # Pagination stays exact across the same held gate: the cursor resumes
    # strictly after its own row and the short page proves the end.
    with ledger.operation_timeout(FAST_CALL_BUDGET_SECONDS):
        second_page = ledger.pending_block_candidate_rows(
            limit=1,
            after_cursor=page[0]["cursor"],
        )
    assert_equal(
        [row["block_hash"] for row in second_page],
        [second_candidate_hash],
        "pending page cursor resumed strictly after its own row",
    )
    with ledger.operation_timeout(FAST_CALL_BUDGET_SECONDS):
        assert_equal(
            ledger.pending_block_candidate_rows(
                limit=1,
                after_cursor=second_page[0]["cursor"],
            ),
            [],
            "a cursor past every pending row proves the walk complete",
        )

    # The control. The same gate, the same budget, asked for writer admission
    # instead: it times out, which is what the enumeration used to do.
    try:
        with ledger.operation_timeout(FAST_CALL_BUDGET_SECONDS):
            ledger._acquire_operation_gate(ledger._lock, "writer lock")
    except LedgerOperationTimeout as exc:
        if "writer lock" not in str(exc):
            raise
    else:
        ledger._lock.release()
        raise SystemExit("the writer gate was not actually held")
finally:
    release_gate.set()
    holder.join(timeout=30)
if holder.is_alive():
    raise SystemExit("the writer gate holder never released the gate")
if holder_error:
    raise holder_error[0]

# The attribution the next budget exhaustion will be read from: no time on
# local admission, real time in PostgreSQL, and neither timeout counter armed.
read_gate_stats = ledger.ledger_read_gate_stats()["pending_block_candidate_rows"]
assert_equal(int(read_gate_stats["calls_total"]), 3, "read-slot calls counted")
assert_equal(int(read_gate_stats["gate_timeouts_total"]), 0, "no admission expiry")
assert_equal(int(read_gate_stats["execute_timeouts_total"]), 0, "no statement expiry")
if float(read_gate_stats["execute_seconds_total"]) <= 0.0:
    raise SystemExit("pending page recorded no PostgreSQL execution time")
if float(read_gate_stats["gate_wait_seconds_max"]) >= FAST_CALL_BUDGET_SECONDS:
    raise SystemExit(
        "pending page charged "
        f"{read_gate_stats['gate_wait_seconds_max']}s to local admission"
    )

ledger._run_sql("DELETE FROM qbit_block_candidate_outbox;")

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

# Issue #236: the pooled client consumes the payout-window reads one JSON
# object per row through fetchmany, in bounded batches, from one buffered
# SELECT. A 3000-row fixture crosses several 512-row batch boundaries; the
# pre-#236 json_agg statement over the same CTE is the oracle for every
# variant, exactly as on the psql backend.
ledger._run_script(
    """
INSERT INTO qbit_share_ledger (
    share_id, miner_id, payout_order_key, p2mr_program,
    share_difficulty, network_difficulty, template_height, job_id,
    job_issued_at, ntime, accepted_at, accepted, writer_id, writer_epoch
)
SELECT
    'bulk-' || g,
    'miner-' || (g % 7),
    lpad((g % 7)::text, 4, '0'),
    decode(md5(g::text) || md5((g + 7)::text), 'hex'),
    1 + (g % 3),
    1000,
    10,
    'job-bulk',
    to_timestamp(1700001000) + (g * interval '1 millisecond'),
    1700001000,
    to_timestamp(1700001000) + (g * interval '1 millisecond'),
    TRUE,
    'writer-native',
    1
FROM generate_series(1, 3000) AS g;
ANALYZE qbit_share_ledger;
"""
)
bulk_anchor_ms = 1_700_001_010_000
assert_equal(ledger.execution_backend, "psycopg-pool", "oracle check runs on the pooled client")
for weight in (1, 512, 1023, 1024, 1025, 3000, 6000, 10**9):
    assert_rows_match_aggregate(
        ledger,
        lambda: ledger.snapshot_at_job_issue(bulk_anchor_ms, window_weight=weight),
        label=f"native per-row bounded snapshot at window weight {weight}",
    )
full_history = assert_rows_match_aggregate(
    ledger,
    lambda: ledger.snapshot_at_job_issue(bulk_anchor_ms),
    label="native per-row unbounded snapshot",
    expected_len=share_count + 3000,
)
assert_rows_match_aggregate(
    ledger,
    lambda: ledger.snapshot_at_job_issue(0, window_weight=64),
    label="native per-row empty window",
    expected_len=0,
)
assert_rows_match_aggregate(
    ledger,
    lambda: ledger.snapshot_between_job_issues(1_700_001_001_000, 1_700_001_002_000),
    label="native per-row delta inside the bulk fixture",
    expected_len=1000,
)
with ledger.operation_timeout(30.0):
    assert_rows_match_aggregate(
        ledger,
        lambda: ledger.snapshot_at_job_issue(bulk_anchor_ms, window_weight=2048),
        label="native per-row bounded snapshot under an armed deadline",
    )

# One SELECT, one MVCC snapshot: a share appended while the rows are still
# being converted (between the first and second 512-row batches) is not in
# any part of that snapshot, and is in the next one. The between-batch hook
# is the ledger's own; a second pooled connection lands the append.
late_share = pending(share_count + 5000, share_id="appended-mid-decode")
late_share = PendingShare(
    **{
        **late_share.__dict__,
        "job_issued_at_ms": 1_700_001_005_000,
        "accepted_at_ms": 1_700_001_005_000,
    }
)
appended_during: list[object] = []
original_hook = ledger._note_json_row_batch


def append_on_first_batch() -> None:
    if not appended_during:
        appended_during.append(ledger.append(late_share))
    original_hook()


ledger._note_json_row_batch = append_on_first_batch  # type: ignore[method-assign]
try:
    during = ledger.snapshot_at_job_issue(bulk_anchor_ms)
finally:
    del ledger._note_json_row_batch
assert_equal(len(appended_during), 1, "the append landed during row conversion")
assert_equal(
    [record.share_id for record in during],
    [record.share_id for record in full_history],
    "a share appended mid-decode is absent from the whole in-flight snapshot",
)
after = ledger.snapshot_at_job_issue(bulk_anchor_ms)
assert_equal(len(after), len(full_history) + 1, "the next snapshot includes the appended share")
assert_equal(after[-1].share_id, "appended-mid-decode", "the appended share is the newest row")
assert_equal(
    ledger.ledger_read_gate_stats()["payout_window_snapshot"]["execute_timeouts_total"],
    0,
    "no window read timed out",
)
print("prism postgres native ledger: OK window-rows=oracle mvcc=single-select", flush=True)

# Restore the 32-share state the fallback and read-only checks below assert
# on: the bulk fixture and the mid-decode append were this section's only
# writes, and their sequence numbers sit above the contiguous 1..32 range.
ledger._run_script(
    """
DELETE FROM qbit_share_ledger
WHERE share_id LIKE 'bulk-%' OR share_id = 'appended-mid-decode';
ANALYZE qbit_share_ledger;
"""
)
assert_equal(
    len(ledger.snapshot_at_job_issue(bulk_anchor_ms)),
    share_count,
    "window fixture rows removed again",
)

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

# A read-only ledger against this writable primary. The database has accepted
# every write above, which is what makes it the right target: a standby would
# refuse these writes whoever asked, so it could never show that read_only=True
# is what refused them.
read_only_ledger = PsqlShareLedger(
    psql_command=psql_command_with_env,
    database_url=database_url,
    native_client_mode="1",
    writer_id="public-read",
    writer_epoch=1,
    read_only=True,
)
assert_equal(
    read_only_ledger.execution_backend,
    "psycopg-pool",
    "read-only ledger uses the pooled native client",
)

# One instance, two kinds of connection. The pooled psycopg session runs the
# writer-lease upsert -- production's own gate-free write, reached here
# directly because a read-only ledger deliberately never runs it at startup.
try:
    read_only_ledger._try_acquire_writer_lease()
except Exception as exc:
    if "read-only transaction" not in str(exc):
        raise
else:
    raise SystemExit("read-only pooled session wrote to a writable primary")

# ...and the one-shot psql connection runs the lease release, which takes
# neither the writer gate nor the pool, so nothing in-process can refuse it.
try:
    read_only_ledger.release_writer_lease_fresh_connection()
except RuntimeError as exc:
    if "read-only transaction" not in str(exc):
        raise
else:
    raise SystemExit("read-only psql connection wrote to a writable primary")

# Reads still work: the session is read-only, not unusable. A read slot is the
# gate every public route takes; the O(n) reads take the writer gate and are
# refused in-process, which is the pre-existing half of the promise.
assert_equal(
    read_only_ledger.accepted_share_stats()["accepted_share_count"],
    share_count,
    "read-only ledger still serves reads over the pool",
)
read_only_ledger.close()

# The same two statements, the same identity, differing only in read_only.
control_writer = PsqlShareLedger(
    psql_command=psql_command_with_env,
    database_url=database_url,
    native_client_mode="1",
    writer_id="public-read",
    writer_epoch=1,
)
assert_equal(
    control_writer._try_acquire_writer_lease()["acquired"],
    True,
    "writable pooled session still acquires the lease",
)
assert_equal(
    control_writer.release_writer_lease_fresh_connection(),
    True,
    "writable psql connection still releases the lease",
)
control_writer.close()

print("prism postgres native ledger: OK read-only-session")
PY
)

echo "test-prism-postgres-native-ledger: PASS"
