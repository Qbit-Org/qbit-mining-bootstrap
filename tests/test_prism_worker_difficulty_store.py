#!/usr/bin/env python3
"""Focused tests for the durable worker-difficulty storage seam.

Everything here runs without a database: the in-memory back end is exercised
directly, the PostgreSQL back end through query capture and canned results,
and the schema through string contracts against the monolithic SQL file.
Real-PostgreSQL parity for the same contract lives in
``tests/prism_worker_difficulty_store_gate.py``.
"""

from __future__ import annotations

import unittest

from decimal import Decimal
from pathlib import Path
from typing import Any

from lab.prism.worker_difficulty_store import (
    MemoryWorkerDifficultyStore,
    PostgresWorkerDifficultyStore,
    WORKER_DIFFICULTY_SCHEMA_SQL,
    WorkerDifficultyRecord,
    WorkerDifficultyStorePort,
    WorkerDifficultyUpsertResult,
)


ROOT = Path(__file__).resolve().parents[1]
LEDGER_SCHEMA = ROOT / "crates" / "qbit-prism" / "sql" / "001_share_ledger.sql"

BASE_MS = 1_756_000_000_000


def record(
    *,
    listener: str = "default",
    worker_username: str = "miner.worker1",
    difficulty: str = "8192",
    evidence_at_ms: int = BASE_MS,
    updated_at_ms: int = BASE_MS,
) -> WorkerDifficultyRecord:
    return WorkerDifficultyRecord(
        listener=listener,
        worker_username=worker_username,
        difficulty=Decimal(difficulty),
        evidence_at_ms=evidence_at_ms,
        updated_at_ms=updated_at_ms,
    )


class QueryCaptureStore(PostgresWorkerDifficultyStore):
    """Postgres store whose runner records SQL and replays canned results."""

    def __init__(self, results: list[Any] | None = None) -> None:
        self.queries: list[str] = []
        self._results = list(results or [])
        super().__init__(self._capture)

    def _capture(self, sql: str) -> Any:
        self.queries.append(sql)
        if self._results:
            return self._results.pop(0)
        return {"applied": True, "stored": None}


class SchemaContractTests(unittest.TestCase):
    def test_module_schema_block_matches_monolithic_file_verbatim(self) -> None:
        schema = LEDGER_SCHEMA.read_text(encoding="utf-8")
        self.assertIn(WORKER_DIFFICULTY_SCHEMA_SQL.strip(), schema)
        # Exactly once, inside the file's single BEGIN/COMMIT transaction.
        self.assertEqual(schema.count("CREATE TABLE IF NOT EXISTS qbit_worker_difficulty"), 1)
        self.assertLess(
            schema.index("CREATE TABLE IF NOT EXISTS qbit_worker_difficulty"),
            schema.rindex("COMMIT;"),
        )

    def test_schema_block_is_idempotent_and_scoped(self) -> None:
        # Idempotent reapply: every statement in the block tolerates the
        # object already existing.
        for statement in WORKER_DIFFICULTY_SCHEMA_SQL.split(";"):
            statement = statement.strip()
            if not statement:
                continue
            self.assertIn("IF NOT EXISTS", statement)
        # Primary key covers listener plus exact username; the preload/prune
        # index leads on the evidence timestamp and matches the preload
        # ORDER BY column-for-column.
        self.assertIn("PRIMARY KEY (listener, worker_username)", WORKER_DIFFICULTY_SCHEMA_SQL)
        self.assertIn(
            "ON qbit_worker_difficulty (evidence_at DESC, listener, worker_username)",
            WORKER_DIFFICULTY_SCHEMA_SQL,
        )
        # Positive finite difficulty: NaN and Infinity both compare greater
        # than zero in PostgreSQL numeric, so the finite guard is load-bearing.
        self.assertIn("difficulty > 0 AND difficulty < 'Infinity'::numeric", WORKER_DIFFICULTY_SCHEMA_SQL)
        # The decimal wire value must not be forced through an integer typmod.
        self.assertIn("difficulty numeric NOT NULL", WORKER_DIFFICULTY_SCHEMA_SQL)
        self.assertNotIn("numeric(", WORKER_DIFFICULTY_SCHEMA_SQL)
        # Evidence and ordinary-update timestamps are separate columns.
        self.assertIn("evidence_at timestamptz NOT NULL", WORKER_DIFFICULTY_SCHEMA_SQL)
        self.assertIn("updated_at timestamptz NOT NULL", WORKER_DIFFICULTY_SCHEMA_SQL)
        # Tie-breaks are byte order in every deployment and match the
        # in-memory store's code-point ordering.
        self.assertIn('listener text COLLATE "C" NOT NULL', WORKER_DIFFICULTY_SCHEMA_SQL)
        self.assertIn('worker_username text COLLATE "C" NOT NULL', WORKER_DIFFICULTY_SCHEMA_SQL)

    def test_schema_touches_no_canonical_share_or_payout_tables(self) -> None:
        for name in (
            "qbit_share_ledger",
            "qbit_pool_blocks",
            "qbit_pool_payout_entries",
            "qbit_payout_carry_forward",
        ):
            self.assertNotIn(name, WORKER_DIFFICULTY_SCHEMA_SQL)


class StorePortTests(unittest.TestCase):
    def test_port_exposes_no_per_key_read(self) -> None:
        # Reconnect authorization must never block on a synchronous
        # database read, so the contract is upsert + bounded batch preload +
        # prune, and nothing key-addressed to read.
        for store_type in (MemoryWorkerDifficultyStore, PostgresWorkerDifficultyStore):
            self.assertTrue(issubclass(store_type, WorkerDifficultyStorePort))
            for forbidden in ("get", "lookup", "load_one", "fetch"):
                self.assertFalse(
                    hasattr(store_type, forbidden),
                    f"{store_type.__name__} must not expose {forbidden}",
                )


class MemoryStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = MemoryWorkerDifficultyStore()

    def test_insert_then_update(self) -> None:
        first = self.store.upsert(
            listener="default",
            worker_username="miner.worker1",
            difficulty=Decimal("4096"),
            evidence_at_ms=BASE_MS,
            now_ms=BASE_MS,
        )
        self.assertTrue(first.applied)
        self.assertEqual(
            first.stored,
            record(difficulty="4096"),
        )
        second = self.store.upsert(
            listener="default",
            worker_username="miner.worker1",
            difficulty=Decimal("8192"),
            evidence_at_ms=BASE_MS + 1_000,
            now_ms=BASE_MS + 1_500,
        )
        self.assertTrue(second.applied)
        self.assertEqual(
            second.stored,
            record(
                difficulty="8192",
                evidence_at_ms=BASE_MS + 1_000,
                updated_at_ms=BASE_MS + 1_500,
            ),
        )
        self.assertEqual(len(self.store), 1)

    def test_older_evidence_cannot_replace_newer_evidence(self) -> None:
        self.store.upsert(
            listener="default",
            worker_username="miner.worker1",
            difficulty=Decimal("8192"),
            evidence_at_ms=BASE_MS + 5_000,
            now_ms=BASE_MS + 5_000,
        )
        stale = self.store.upsert(
            listener="default",
            worker_username="miner.worker1",
            difficulty=Decimal("1"),
            evidence_at_ms=BASE_MS + 4_999,
            now_ms=BASE_MS + 6_000,
        )
        self.assertFalse(stale.applied)
        self.assertEqual(
            stale.stored,
            record(
                difficulty="8192",
                evidence_at_ms=BASE_MS + 5_000,
                updated_at_ms=BASE_MS + 5_000,
            ),
        )
        loaded = self.store.load_recent(evidence_after_ms=0, limit=10)
        self.assertEqual(loaded[0].difficulty, Decimal("8192"))
        self.assertEqual(loaded[0].updated_at_ms, BASE_MS + 5_000)

    def test_equal_evidence_is_deterministic_last_write_wins(self) -> None:
        self.store.upsert(
            listener="default",
            worker_username="miner.worker1",
            difficulty=Decimal("8192"),
            evidence_at_ms=BASE_MS,
            now_ms=BASE_MS,
        )
        rewrite = self.store.upsert(
            listener="default",
            worker_username="miner.worker1",
            difficulty=Decimal("2048"),
            evidence_at_ms=BASE_MS,
            now_ms=BASE_MS + 100,
        )
        self.assertTrue(rewrite.applied)
        self.assertEqual(
            rewrite.stored,
            record(difficulty="2048", updated_at_ms=BASE_MS + 100),
        )

    def test_lane_separation(self) -> None:
        self.store.upsert(
            listener="stratum-public",
            worker_username="miner.worker1",
            difficulty=Decimal("16"),
            evidence_at_ms=BASE_MS + 1,
            now_ms=BASE_MS + 1,
        )
        self.store.upsert(
            listener="stratum-highdiff",
            worker_username="miner.worker1",
            difficulty=Decimal("65536"),
            evidence_at_ms=BASE_MS + 2,
            now_ms=BASE_MS + 2,
        )
        loaded = self.store.load_recent(evidence_after_ms=0, limit=10)
        self.assertEqual(len(loaded), 2)
        by_lane = {entry.listener: entry.difficulty for entry in loaded}
        self.assertEqual(by_lane["stratum-public"], Decimal("16"))
        self.assertEqual(by_lane["stratum-highdiff"], Decimal("65536"))

    def test_exact_username_separation(self) -> None:
        # Case, surrounding whitespace and lookalikes are distinct identities:
        # exact byte match only, no normalization.
        usernames = ["miner.worker1", "Miner.worker1", " miner.worker1", "miner.worker1 "]
        for index, username in enumerate(usernames):
            outcome = self.store.upsert(
                listener="default",
                worker_username=username,
                difficulty=Decimal(2 ** (index + 1)),
                evidence_at_ms=BASE_MS + index,
                now_ms=BASE_MS + index,
            )
            self.assertTrue(outcome.applied)
        loaded = self.store.load_recent(evidence_after_ms=0, limit=10)
        self.assertEqual(len(loaded), len(usernames))
        self.assertEqual(
            {entry.worker_username for entry in loaded},
            set(usernames),
        )

    def test_load_recent_is_bounded_cutoff_exclusive_and_ordered(self) -> None:
        for index in range(5):
            self.store.upsert(
                listener="default",
                worker_username=f"miner.worker{index}",
                difficulty=Decimal("64"),
                evidence_at_ms=BASE_MS + index * 1_000,
                now_ms=BASE_MS + index * 1_000,
            )
        # Tie on the evidence timestamp to pin the (listener, username)
        # tie-break.
        self.store.upsert(
            listener="alpha-lane",
            worker_username="miner.workerZ",
            difficulty=Decimal("64"),
            evidence_at_ms=BASE_MS + 4_000,
            now_ms=BASE_MS + 4_000,
        )
        loaded = self.store.load_recent(evidence_after_ms=BASE_MS + 1_000, limit=3)
        self.assertEqual(
            [
                (entry.evidence_at_ms, entry.listener, entry.worker_username)
                for entry in loaded
            ],
            [
                (BASE_MS + 4_000, "alpha-lane", "miner.workerZ"),
                (BASE_MS + 4_000, "default", "miner.worker4"),
                (BASE_MS + 3_000, "default", "miner.worker3"),
            ],
        )
        # The cutoff is strict: an entry exactly at the cutoff is excluded.
        cutoff_hits = self.store.load_recent(evidence_after_ms=BASE_MS + 4_000, limit=10)
        self.assertEqual(cutoff_hits, [])

    def test_prune_is_cutoff_inclusive_and_complements_load(self) -> None:
        for index in range(4):
            self.store.upsert(
                listener="default",
                worker_username=f"miner.worker{index}",
                difficulty=Decimal("64"),
                evidence_at_ms=BASE_MS + index * 1_000,
                now_ms=BASE_MS + index * 1_000,
            )
        deleted = self.store.prune(evidence_cutoff_ms=BASE_MS + 1_000)
        self.assertEqual(deleted, 2)
        survivors = self.store.load_recent(evidence_after_ms=BASE_MS + 1_000, limit=10)
        self.assertEqual(len(survivors), 2)
        self.assertEqual(len(self.store), 2)

    def test_bounded_prune_removes_oldest_first_deterministically(self) -> None:
        entries = [
            ("default", "miner.workerB", BASE_MS),
            ("default", "miner.workerA", BASE_MS),
            ("alpha-lane", "miner.workerA", BASE_MS),
            ("default", "miner.workerC", BASE_MS + 1_000),
        ]
        for listener, username, evidence_ms in entries:
            self.store.upsert(
                listener=listener,
                worker_username=username,
                difficulty=Decimal("64"),
                evidence_at_ms=evidence_ms,
                now_ms=evidence_ms,
            )
        deleted = self.store.prune(evidence_cutoff_ms=BASE_MS + 1_000, limit=2)
        self.assertEqual(deleted, 2)
        remaining = self.store.load_recent(evidence_after_ms=0, limit=10)
        # Oldest evidence first, ties broken by (listener, username): the
        # alpha-lane row and default/workerA go, workerB and workerC stay.
        self.assertEqual(
            {(entry.listener, entry.worker_username) for entry in remaining},
            {("default", "miner.workerB"), ("default", "miner.workerC")},
        )

    def test_decimal_values_round_trip_exactly(self) -> None:
        difficulties = [
            Decimal("0.000030517578125"),  # 2**-15
            Decimal("123456789012345678901234567890.5"),
            Decimal("1E-12"),
            Decimal("8192.000"),
        ]
        for index, difficulty in enumerate(difficulties):
            self.store.upsert(
                listener="default",
                worker_username=f"miner.worker{index}",
                difficulty=difficulty,
                evidence_at_ms=BASE_MS + index,
                now_ms=BASE_MS + index,
            )
        loaded = self.store.load_recent(evidence_after_ms=0, limit=10)
        by_user = {entry.worker_username: entry.difficulty for entry in loaded}
        for index, difficulty in enumerate(difficulties):
            self.assertEqual(by_user[f"miner.worker{index}"], difficulty)

    def test_input_validation(self) -> None:
        good = dict(
            listener="default",
            worker_username="miner.worker1",
            difficulty=Decimal("64"),
            evidence_at_ms=BASE_MS,
            now_ms=BASE_MS,
        )
        rejected = [
            {**good, "difficulty": Decimal("NaN")},
            {**good, "difficulty": Decimal("Infinity")},
            {**good, "difficulty": Decimal("0")},
            {**good, "difficulty": Decimal("-1")},
            {**good, "difficulty": 64},
            {**good, "difficulty": 64.0},
            {**good, "difficulty": Decimal("1E+1000")},
            {**good, "difficulty": Decimal("1E-1000")},
            {**good, "listener": ""},
            {**good, "listener": "bad\x00lane"},
            {**good, "worker_username": ""},
            {**good, "evidence_at_ms": -1},
            {**good, "evidence_at_ms": 1.5},
            {**good, "now_ms": True},
        ]
        for kwargs in rejected:
            with self.assertRaises(ValueError, msg=repr(kwargs)):
                self.store.upsert(**kwargs)
        with self.assertRaises(ValueError):
            self.store.load_recent(evidence_after_ms=0, limit=0)
        with self.assertRaises(ValueError):
            self.store.prune(evidence_cutoff_ms=0, limit=-1)
        self.assertEqual(len(self.store), 0)


class PostgresStoreSqlTests(unittest.TestCase):
    def test_upsert_sql_is_one_guarded_atomic_statement(self) -> None:
        store = QueryCaptureStore(
            results=[
                {
                    "applied": True,
                    "stored": {
                        "listener": "default",
                        "worker_username": "miner.worker1",
                        "difficulty": "8192.5",
                        "evidence_at_ms": BASE_MS,
                        "updated_at_ms": BASE_MS + 1,
                    },
                }
            ]
        )
        outcome = store.upsert(
            listener="default",
            worker_username="o'brien.worker1",
            difficulty=Decimal("8192.5"),
            evidence_at_ms=BASE_MS,
            now_ms=BASE_MS + 1,
        )
        self.assertEqual(len(store.queries), 1)
        sql = store.queries[0]
        self.assertIn("INSERT INTO qbit_worker_difficulty", sql)
        self.assertIn("ON CONFLICT (listener, worker_username) DO UPDATE", sql)
        # The evidence guard is the only thing standing between older and
        # newer evidence; pin it verbatim.
        self.assertIn("WHERE EXCLUDED.evidence_at >= existing.evidence_at", sql)
        # Values are inlined with quoting; the embedded quote must be doubled.
        self.assertIn("'o''brien.worker1'", sql)
        self.assertIn("'8192.5'::numeric", sql)
        self.assertIn(f"to_timestamp(({BASE_MS}::double precision / 1000.0))", sql)
        # Difficulty travels back as text, never as a JSON float.
        self.assertIn("difficulty::text", sql)
        self.assertEqual(
            outcome,
            WorkerDifficultyUpsertResult(
                applied=True,
                stored=record(difficulty="8192.5", updated_at_ms=BASE_MS + 1),
            ),
        )

    def test_refused_upsert_parses_standing_row(self) -> None:
        store = QueryCaptureStore(
            results=[
                {
                    "applied": False,
                    "stored": {
                        "listener": "default",
                        "worker_username": "miner.worker1",
                        "difficulty": "8192",
                        "evidence_at_ms": BASE_MS + 5_000,
                        "updated_at_ms": BASE_MS + 5_000,
                    },
                }
            ]
        )
        outcome = store.upsert(
            listener="default",
            worker_username="miner.worker1",
            difficulty=Decimal("1"),
            evidence_at_ms=BASE_MS,
            now_ms=BASE_MS + 6_000,
        )
        self.assertFalse(outcome.applied)
        assert outcome.stored is not None
        self.assertEqual(outcome.stored.difficulty, Decimal("8192"))
        self.assertEqual(outcome.stored.evidence_at_ms, BASE_MS + 5_000)

    def test_load_recent_sql_pins_bound_order_and_cutoff(self) -> None:
        store = QueryCaptureStore(results=[[]])
        loaded = store.load_recent(evidence_after_ms=BASE_MS, limit=250)
        self.assertEqual(loaded, [])
        sql = store.queries[0]
        self.assertIn(
            f"WHERE evidence_at > to_timestamp(({BASE_MS}::double precision / 1000.0))",
            sql,
        )
        self.assertIn("ORDER BY evidence_at DESC, listener, worker_username", sql)
        self.assertIn("LIMIT 250", sql)
        self.assertIn(
            "ORDER BY recent.evidence_at DESC, recent.listener, recent.worker_username",
            sql,
        )

    def test_prune_sql_is_store_side_and_boundable(self) -> None:
        store = QueryCaptureStore(results=[{"deleted": 3}, {"deleted": 2}])
        self.assertEqual(store.prune(evidence_cutoff_ms=BASE_MS), 3)
        self.assertEqual(store.prune(evidence_cutoff_ms=BASE_MS, limit=2), 2)
        unbounded, bounded = store.queries
        self.assertIn("DELETE FROM qbit_worker_difficulty", unbounded)
        self.assertIn(
            f"WHERE evidence_at <= to_timestamp(({BASE_MS}::double precision / 1000.0))",
            unbounded,
        )
        self.assertIn("ORDER BY evidence_at ASC, listener, worker_username", bounded)
        self.assertIn("LIMIT 2", bounded)

    def test_statements_touch_only_the_worker_difficulty_table(self) -> None:
        store = QueryCaptureStore(
            results=[
                {"applied": True, "stored": None},
                {"applied": True, "stored": None},
                [],
                {"deleted": 0},
            ]
        )
        store.upsert(
            listener="default",
            worker_username="miner.worker1",
            difficulty=Decimal("64"),
            evidence_at_ms=BASE_MS,
            now_ms=BASE_MS,
        )
        store.apply_downward(
            listener="default",
            worker_username="miner.worker1",
            difficulty=Decimal("32"),
            now_ms=BASE_MS,
        )
        store.load_recent(evidence_after_ms=0, limit=1)
        store.prune(evidence_cutoff_ms=0)
        for sql in store.queries:
            for name in (
                "qbit_share_ledger",
                "qbit_pool_blocks",
                "qbit_pool_payout_entries",
                "qbit_payout_carry_forward",
                "qbit_ledger_writer_lease",
            ):
                self.assertNotIn(name, sql)


class LowerOnlyCorrectionTests(unittest.TestCase):
    """The atomic downward correction, on the in-memory reference back end.

    The PostgreSQL twin is driven through the same sequence by
    tests/prism_worker_difficulty_store_gate.py.
    """

    def setUp(self) -> None:
        self.store = MemoryWorkerDifficultyStore()
        self.key = {"listener": "default", "worker_username": "miner.worker1"}

    def seed(self, difficulty: str, *, evidence_at_ms: int = BASE_MS) -> None:
        self.store.upsert(
            **self.key,
            difficulty=Decimal(difficulty),
            evidence_at_ms=evidence_at_ms,
            now_ms=evidence_at_ms,
        )

    def test_lowers_a_higher_row_and_preserves_its_evidence(self) -> None:
        self.seed("1048576")

        result = self.store.apply_downward(
            **self.key,
            difficulty=Decimal("65536"),
            now_ms=BASE_MS + 5_000,
        )

        self.assertTrue(result.applied)
        assert result.stored is not None
        self.assertEqual(result.stored.difficulty, Decimal("65536"))
        # The correction is not evidence, so it must not re-stamp the clock
        # the TTL and the write ordering both read.
        self.assertEqual(result.stored.evidence_at_ms, BASE_MS)
        self.assertEqual(result.stored.updated_at_ms, BASE_MS + 5_000)

    def test_never_raises_a_stored_difficulty(self) -> None:
        self.seed("65536")

        result = self.store.apply_downward(
            **self.key,
            difficulty=Decimal("1048576"),
            now_ms=BASE_MS + 5_000,
        )

        self.assertFalse(result.applied)
        assert result.stored is not None
        self.assertEqual(result.stored.difficulty, Decimal("65536"))
        self.assertEqual(result.stored.updated_at_ms, BASE_MS)

    def test_equal_difficulty_is_a_no_op(self) -> None:
        self.seed("65536")

        result = self.store.apply_downward(
            **self.key,
            difficulty=Decimal("65536.0"),
            now_ms=BASE_MS + 5_000,
        )

        self.assertFalse(result.applied)
        self.assertEqual(len(self.store), 1)

    def test_missing_row_is_a_no_op_and_never_an_insert(self) -> None:
        result = self.store.apply_downward(
            **self.key,
            difficulty=Decimal("65536"),
            now_ms=BASE_MS,
        )

        # Nothing stored means nothing a restart could resurrect, so there is
        # no value to write and no evidence to invent.
        self.assertFalse(result.applied)
        self.assertIsNone(result.stored)
        self.assertEqual(len(self.store), 0)

    def test_a_correction_cannot_suppress_later_share_backed_evidence(self) -> None:
        self.seed("1048576", evidence_at_ms=BASE_MS)
        self.store.apply_downward(
            **self.key,
            difficulty=Decimal("65536"),
            now_ms=BASE_MS + 60_000,
        )

        # Share evidence recorded after the seed but BEFORE the correction's
        # wall clock still has to win: the correction wrote no evidence.
        result = self.store.upsert(
            **self.key,
            difficulty=Decimal("500000"),
            evidence_at_ms=BASE_MS + 50,
            now_ms=BASE_MS + 70_000,
        )

        self.assertTrue(result.applied)
        assert result.stored is not None
        self.assertEqual(result.stored.difficulty, Decimal("500000"))

    def test_correction_is_scoped_to_lane_and_exact_username(self) -> None:
        self.seed("1048576")
        self.store.upsert(
            listener="highdiff",
            worker_username="miner.worker1",
            difficulty=Decimal("1048576"),
            evidence_at_ms=BASE_MS,
            now_ms=BASE_MS,
        )
        self.store.upsert(
            listener="default",
            worker_username="miner.worker10",
            difficulty=Decimal("1048576"),
            evidence_at_ms=BASE_MS,
            now_ms=BASE_MS,
        )

        self.store.apply_downward(
            **self.key,
            difficulty=Decimal("65536"),
            now_ms=BASE_MS + 1,
        )

        untouched = {
            (record.listener, record.worker_username): record.difficulty
            for record in self.store.load_recent(evidence_after_ms=0, limit=10)
        }
        self.assertEqual(untouched[("default", "miner.worker1")], Decimal("65536"))
        self.assertEqual(
            untouched[("highdiff", "miner.worker1")], Decimal("1048576")
        )
        self.assertEqual(
            untouched[("default", "miner.worker10")], Decimal("1048576")
        )

    def test_input_validation_matches_the_upsert_path(self) -> None:
        with self.assertRaises(ValueError):
            self.store.apply_downward(
                **self.key, difficulty=Decimal("0"), now_ms=BASE_MS
            )
        with self.assertRaises(ValueError):
            self.store.apply_downward(
                **self.key, difficulty=Decimal("1"), now_ms=-1
            )
        with self.assertRaises(ValueError):
            self.store.apply_downward(
                listener="",
                worker_username="miner.worker1",
                difficulty=Decimal("1"),
                now_ms=BASE_MS,
            )


class LowerOnlySqlTests(unittest.TestCase):
    def test_correction_is_one_guarded_atomic_statement(self) -> None:
        store = QueryCaptureStore(results=[{"applied": False, "stored": None}])

        store.apply_downward(
            listener="default",
            worker_username="miner.worker1",
            difficulty=Decimal("65536"),
            now_ms=BASE_MS,
        )

        self.assertEqual(len(store.queries), 1)
        sql = store.queries[0]
        self.assertEqual(sql.count("UPDATE qbit_worker_difficulty"), 1)
        # The guard is what makes the correction lower-only...
        self.assertIn("AND difficulty > ", sql)
        # ...and evidence_at is never on the SET list, which is what stops it
        # suppressing later share-backed evidence.
        self.assertNotIn("evidence_at =", sql)
        self.assertIn("SET difficulty = ", sql)
        self.assertIn("updated_at = ", sql)
        self.assertNotIn("INSERT", sql)

if __name__ == "__main__":
    unittest.main()
