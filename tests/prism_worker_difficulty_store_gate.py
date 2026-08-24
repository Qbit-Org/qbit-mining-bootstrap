"""Non-discovered PostgreSQL parity gate for the worker-difficulty store.

Drives :class:`MemoryWorkerDifficultyStore` and
:class:`PostgresWorkerDifficultyStore` through the same operation sequence and
requires identical results after every step, then checks the PostgreSQL-only
properties (schema reapply safety, CHECK constraint enforcement, index-backed
plans). It lives outside unittest discovery because it needs an explicitly
provisioned PostgreSQL target; run it from the repository root:

    PRISM_PSQL_COMMAND="psql -p 5432 -d some_scratch_db" \\
        python3 -m tests.prism_worker_difficulty_store_gate

The target database is written to (the monolithic ledger schema is applied
into the default schema); use a scratch database.
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

from lab.prism.worker_difficulty_store import (
    MemoryWorkerDifficultyStore,
    PostgresWorkerDifficultyStore,
    WorkerDifficultyRecord,
)

ROOT = Path(__file__).resolve().parents[1]
LEDGER_SCHEMA = ROOT / "crates" / "qbit-prism" / "sql" / "001_share_ledger.sql"
PSQL_TIMEOUT_SECONDS = 60.0

BASE_PSQL_COMMAND = os.environ.get("PRISM_PSQL_COMMAND", "")
if not BASE_PSQL_COMMAND:
    raise SystemExit("PRISM_PSQL_COMMAND is required")
BASE_PSQL_ARGV = shlex.split(BASE_PSQL_COMMAND)
if not BASE_PSQL_ARGV:
    raise SystemExit("PRISM_PSQL_COMMAND is empty")

BASE_MS = 1_756_000_000_000


class GateFailure(RuntimeError):
    pass


def assert_equal(actual: object, expected: object, message: str) -> None:
    if actual != expected:
        raise GateFailure(f"{message}: expected {expected!r}, got {actual!r}")


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise GateFailure(message)


def run_psql(sql: str, *, expect_failure: bool = False) -> str:
    command = [
        *BASE_PSQL_ARGV,
        "--no-psqlrc",
        "--set",
        "ON_ERROR_STOP=1",
        "--tuples-only",
        "--no-align",
        "--quiet",
    ]
    completed = subprocess.run(
        command,
        input=sql,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=PSQL_TIMEOUT_SECONDS,
        check=False,
    )
    if expect_failure:
        if completed.returncode == 0:
            raise GateFailure(f"psql unexpectedly succeeded for: {sql.strip()[:200]}")
        return completed.stderr
    if completed.returncode != 0:
        raise GateFailure(
            f"psql failed with exit {completed.returncode}: {completed.stderr.strip()}"
        )
    return completed.stdout


def run_json(sql: str) -> object:
    output = run_psql(sql).strip()
    if not output:
        raise GateFailure(f"psql query returned no JSON: {sql.strip()[:200]}")
    return json.loads(output.splitlines()[-1])


def run_script(sql: str) -> None:
    run_psql(sql)


def record_key(record: WorkerDifficultyRecord) -> tuple[str, str]:
    return (record.listener, record.worker_username)


def check_parity(
    memory: MemoryWorkerDifficultyStore,
    postgres: PostgresWorkerDifficultyStore,
    message: str,
) -> None:
    """Full-table equality via the bounded load with a generous limit."""
    memory_rows = memory.load_recent(evidence_after_ms=0, limit=10_000)
    postgres_rows = postgres.load_recent(evidence_after_ms=0, limit=10_000)
    assert_equal(postgres_rows, memory_rows, message)


def main() -> None:
    schema_sql = LEDGER_SCHEMA.read_text(encoding="utf-8")

    # Idempotent schema application: the monolithic file applies cleanly
    # twice, and the focused module DDL reapplies over it a third time.
    run_psql(schema_sql)
    run_psql(schema_sql)

    memory = MemoryWorkerDifficultyStore()
    postgres = PostgresWorkerDifficultyStore(run_json, run_script=run_script)
    postgres.ensure_schema()
    postgres.ensure_schema()

    # Clean slate in case the scratch database was reused.
    run_psql("DELETE FROM qbit_worker_difficulty;")

    def upsert_both(**kwargs: object) -> None:
        memory_outcome = memory.upsert(**kwargs)  # type: ignore[arg-type]
        postgres_outcome = postgres.upsert(**kwargs)  # type: ignore[arg-type]
        assert_equal(
            postgres_outcome,
            memory_outcome,
            f"upsert outcome parity for {kwargs!r}",
        )

    # Insert across two lanes, same username in both; lookalike usernames in
    # one lane; a quote-bearing username to exercise literal quoting.
    upsert_both(
        listener="stratum-public",
        worker_username="miner.worker1",
        difficulty=Decimal("16"),
        evidence_at_ms=BASE_MS + 1_000,
        now_ms=BASE_MS + 1_000,
    )
    upsert_both(
        listener="stratum-highdiff",
        worker_username="miner.worker1",
        difficulty=Decimal("65536"),
        evidence_at_ms=BASE_MS + 2_000,
        now_ms=BASE_MS + 2_000,
    )
    upsert_both(
        listener="stratum-public",
        worker_username="Miner.worker1",
        difficulty=Decimal("32"),
        evidence_at_ms=BASE_MS + 3_000,
        now_ms=BASE_MS + 3_000,
    )
    upsert_both(
        listener="stratum-public",
        worker_username=" miner.worker1",
        difficulty=Decimal("64"),
        evidence_at_ms=BASE_MS + 4_000,
        now_ms=BASE_MS + 4_000,
    )
    upsert_both(
        listener="stratum-public",
        worker_username="o'brien.worker1",
        difficulty=Decimal("128"),
        evidence_at_ms=BASE_MS + 5_000,
        now_ms=BASE_MS + 5_000,
    )
    check_parity(memory, postgres, "parity after inserts")

    # Update with newer evidence applies; the lane sibling is untouched.
    upsert_both(
        listener="stratum-public",
        worker_username="miner.worker1",
        difficulty=Decimal("24"),
        evidence_at_ms=BASE_MS + 6_000,
        now_ms=BASE_MS + 6_500,
    )
    # Older evidence is refused even with a later wall-clock write time.
    upsert_both(
        listener="stratum-public",
        worker_username="miner.worker1",
        difficulty=Decimal("1"),
        evidence_at_ms=BASE_MS + 5_999,
        now_ms=BASE_MS + 9_000,
    )
    # Equal evidence is deterministic last-write-wins.
    upsert_both(
        listener="stratum-public",
        worker_username="miner.worker1",
        difficulty=Decimal("48"),
        evidence_at_ms=BASE_MS + 6_000,
        now_ms=BASE_MS + 9_500,
    )
    check_parity(memory, postgres, "parity after evidence-guarded updates")

    def lower_both(**kwargs: object) -> None:
        memory_outcome = memory.apply_downward(**kwargs)  # type: ignore[arg-type]
        postgres_outcome = postgres.apply_downward(**kwargs)  # type: ignore[arg-type]
        assert_equal(
            postgres_outcome,
            memory_outcome,
            f"apply_downward outcome parity for {kwargs!r}",
        )

    # The lower-only correction: reduces a higher row, refuses to raise one,
    # is a no-op on an equal value or a missing key, and never touches
    # evidence_at -- so a later share-backed write whose evidence predates the
    # correction still applies.
    lower_both(
        listener="stratum-public",
        worker_username="miner.worker1",
        difficulty=Decimal("8"),
        now_ms=BASE_MS + 20_000,
    )
    lower_both(
        listener="stratum-public",
        worker_username="miner.worker1",
        difficulty=Decimal("4096"),
        now_ms=BASE_MS + 20_100,
    )
    lower_both(
        listener="stratum-public",
        worker_username="miner.worker1",
        difficulty=Decimal("8.000"),
        now_ms=BASE_MS + 20_200,
    )
    lower_both(
        listener="stratum-public",
        worker_username="never.seen",
        difficulty=Decimal("8"),
        now_ms=BASE_MS + 20_300,
    )
    check_parity(memory, postgres, "parity after lower-only corrections")
    assert_true(
        not any(
            record.worker_username == "never.seen"
            for record in postgres.load_recent(evidence_after_ms=0, limit=10_000)
        ),
        "apply_downward must never insert a row for an unknown worker",
    )
    # Evidence older than the correction's wall clock, newer than the row's:
    # the correction wrote no evidence, so this must still apply.
    upsert_both(
        listener="stratum-public",
        worker_username="miner.worker1",
        difficulty=Decimal("512"),
        evidence_at_ms=BASE_MS + 6_001,
        now_ms=BASE_MS + 21_000,
    )
    check_parity(
        memory,
        postgres,
        "a lower-only correction must not suppress later share evidence",
    )

    # Decimal exactness through the durable round trip.
    exact_values = [
        Decimal("0.000030517578125"),  # 2**-15
        Decimal("123456789012345678901234567890.5"),
        Decimal("1E-12"),
        Decimal("8192.000"),
    ]
    for index, difficulty in enumerate(exact_values):
        upsert_both(
            listener="decimal-lane",
            worker_username=f"miner.exact{index}",
            difficulty=difficulty,
            evidence_at_ms=BASE_MS + 10_000 + index,
            now_ms=BASE_MS + 10_000 + index,
        )
    loaded = postgres.load_recent(evidence_after_ms=BASE_MS + 9_999, limit=10)
    by_user = {entry.worker_username: entry.difficulty for entry in loaded}
    for index, difficulty in enumerate(exact_values):
        assert_equal(
            by_user[f"miner.exact{index}"],
            difficulty,
            f"decimal round trip for {difficulty}",
        )
    check_parity(memory, postgres, "parity after decimal round-trip rows")

    # Bounded newest-first load: limit and deterministic ordering pinned,
    # cutoff strictly exclusive. Both stores must agree element-for-element.
    for cutoff, limit in [
        (0, 3),
        (BASE_MS + 4_000, 2),
        (BASE_MS + 6_000, 10),
        (BASE_MS + 10_003, 10),
    ]:
        assert_equal(
            postgres.load_recent(evidence_after_ms=cutoff, limit=limit),
            memory.load_recent(evidence_after_ms=cutoff, limit=limit),
            f"bounded load parity (cutoff={cutoff}, limit={limit})",
        )
    top = postgres.load_recent(evidence_after_ms=0, limit=3)
    assert_equal(
        [record_key(entry) for entry in top],
        [
            ("decimal-lane", "miner.exact3"),
            ("decimal-lane", "miner.exact2"),
            ("decimal-lane", "miner.exact1"),
        ],
        "newest-first pinned ordering",
    )
    boundary = postgres.load_recent(evidence_after_ms=BASE_MS + 10_003, limit=10)
    assert_equal(boundary, [], "strict cutoff excludes the boundary row")

    # Bounded prune removes the deterministic oldest set; both back ends
    # agree on the count and on every survivor.
    memory_deleted = memory.prune(evidence_cutoff_ms=BASE_MS + 5_000, limit=2)
    postgres_deleted = postgres.prune(evidence_cutoff_ms=BASE_MS + 5_000, limit=2)
    assert_equal(postgres_deleted, memory_deleted, "bounded prune count parity")
    assert_equal(postgres_deleted, 2, "bounded prune removes exactly the limit")
    check_parity(memory, postgres, "parity after bounded prune")

    # Unbounded prune clears the rest of the old range store-side.
    memory_deleted = memory.prune(evidence_cutoff_ms=BASE_MS + 6_000)
    postgres_deleted = postgres.prune(evidence_cutoff_ms=BASE_MS + 6_000)
    assert_equal(postgres_deleted, memory_deleted, "prune count parity")
    check_parity(memory, postgres, "parity after prune")

    # A no-op prune below every remaining row deletes nothing.
    assert_equal(postgres.prune(evidence_cutoff_ms=1), memory.prune(evidence_cutoff_ms=1), "no-op prune parity")

    # Ordering ties: several keys share one evidence timestamp, so the
    # (listener, worker_username) byte-order tie-break itself is what both
    # back ends must agree on (COLLATE "C" pins the PostgreSQL side to byte
    # order regardless of the database's locale collation).
    tie_usernames = ["zeta", "Alpha", " alpha", "alpha", "ALPHA.9"]
    for tie_index, username in enumerate(tie_usernames):
        upsert_both(
            listener="tie-lane",
            worker_username=username,
            difficulty=Decimal(tie_index + 1),
            evidence_at_ms=BASE_MS + 20_000,
            now_ms=BASE_MS + 20_000,
        )
    tie_memory = memory.load_recent(evidence_after_ms=BASE_MS + 19_999, limit=10)
    tie_postgres = postgres.load_recent(evidence_after_ms=BASE_MS + 19_999, limit=10)
    assert_equal(tie_postgres, tie_memory, "tie-break ordering parity")
    assert_equal(
        [entry.worker_username for entry in tie_postgres],
        sorted(tie_usernames),
        "tie-break is byte order",
    )
    check_parity(memory, postgres, "parity with the tie group present")

    # Schema-level validation: the CHECK constraints refuse what the Python
    # validation refuses, so a writer bypassing this module cannot poison the
    # preload path with NaN/Infinity/non-positive difficulties or empty keys.
    for bad_row in (
        "('lane', 'user-nan', 'NaN'::numeric, now(), now())",
        "('lane', 'user-inf', 'Infinity'::numeric, now(), now())",
        "('lane', 'user-zero', 0, now(), now())",
        "('lane', 'user-negative', -5, now(), now())",
        "('', 'user-empty-lane', 1, now(), now())",
        "('lane', '', 1, now(), now())",
    ):
        stderr = run_psql(
            "INSERT INTO qbit_worker_difficulty "
            "(listener, worker_username, difficulty, evidence_at, updated_at) "
            f"VALUES {bad_row};",
            expect_failure=True,
        )
        assert_true(
            "violates check constraint" in stderr or "not-null constraint" in stderr,
            f"unexpected rejection reason for {bad_row}: {stderr.strip()}",
        )

    # The bounded newest-first load plans through the evidence index rather
    # than a sequential scan once the planner is told scans are expensive
    # (the table is tiny here, so seqscan is otherwise legitimately cheaper).
    plan = run_psql(
        """
BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
SELECT listener, worker_username, difficulty
FROM qbit_worker_difficulty
WHERE evidence_at > now() - interval '1 hour'
ORDER BY evidence_at DESC, listener, worker_username
LIMIT 100;
ROLLBACK;
"""
    )
    assert_true(
        "qbit_worker_difficulty_evidence_idx" in plan,
        f"bounded load did not use the evidence index:\n{plan}",
    )
    assert_true("Sort" not in plan, f"bounded load required an explicit sort:\n{plan}")

    # Canonical share/payout tables are untouched by this gate's traffic.
    counts = run_json(
        """
SELECT json_build_object(
    'shares', (SELECT count(*) FROM qbit_share_ledger),
    'blocks', (SELECT count(*) FROM qbit_pool_blocks),
    'payouts', (SELECT count(*) FROM qbit_pool_payout_entries),
    'carry', (SELECT count(*) FROM qbit_payout_carry_forward)
);
"""
    )
    assert_equal(
        counts,
        {"shares": 0, "blocks": 0, "payouts": 0, "carry": 0},
        "canonical tables must stay empty",
    )

    print("prism worker-difficulty store gate: all checks passed")


if __name__ == "__main__":
    try:
        main()
    except GateFailure as failure:
        print(f"GATE FAILURE: {failure}", file=sys.stderr)
        raise SystemExit(1)
