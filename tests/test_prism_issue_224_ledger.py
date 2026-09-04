#!/usr/bin/env python3
"""Issue #224 Wave 1: attribution for the ledger reads a preview waits on.

``PrismAcceptedPreviewPublicationLatencyHigh`` fired while the coordinator
stayed healthy, and nothing on ``/metrics`` said which stretch of the landing
owned the sample. Four ledger reads sit in that stretch -- the payout-window
snapshot and its incremental delta at job issue, and the two prior-balances
reads -- and each of them can be slow for either of two unrelated reasons:
coordinator-local admission (a convoy behind an accounting write) or
PostgreSQL execution. One duration covering both cannot tell an operator
which, which is exactly how #211 was misdiagnosed on the enumeration path
before it was split.

This wave routes those four through the same attributed read helper the
enumeration already uses, under the closed operation vocabulary
``lab.prism.accepted_preview_telemetry`` froze in Wave 0. The scenarios here
pin the two things that could go wrong while doing it:

* the *measurement* is right -- the fixed operation name, gate wait apart
  from execution, an admission expiry counted as a gate timeout with no
  execution sample, a statement expiry counted as an execution timeout;
* the *behavior* is unchanged -- each read keeps the admission primitive it
  already had (the writer lock stays the writer lock, the read slot stays the
  read slot), sends byte-identical SQL, returns identical records, gives the
  primitive back on every failure path, and preserves the original exception.

The gates below are real ``Lock``/``BoundedSemaphore`` objects behind a
recording wrapper, so "the read released what it took" is a fact about an
actual primitive rather than about a counter. The clock is scripted, so the
recorded seconds are exact.
"""

from __future__ import annotations

import copy
import re
import sys
import threading
import unittest
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lab.prism.accepted_preview_telemetry import (  # noqa: E402
    PRISM_LEDGER_READ_OPERATIONS,
    fold_ledger_read_stats,
)
from lab.prism.share_ledger import (  # noqa: E402
    AcceptedShareRecord,
    LedgerOperationTimeout,
    PsqlShareLedger,
    ReadOnlyLedgerError,
    _RefusingWriterGate,
)

REPO_ROOT = Path(__file__).resolve().parents[1]

BLOCK_HASH = "aa" * 32

# The operation each method must record under, and the admission primitive it
# must keep. Both halves are the contract: an attribution that renamed the
# operation would break the join with the Wave 0 vocabulary, and one that
# moved a read between primitives would change which writes it serializes
# against -- a concurrency change wearing a metrics change's clothes.
READ_SLOT = "read slot"
WRITER_LOCK = "writer lock"

# The longest a scenario will wait for admission before calling the gate
# stranded. Every legitimate acquisition below is uncontended and returns
# immediately, so this is never reached by a passing run -- it exists so a
# helper that stopped releasing its gate fails a scenario instead of parking
# the suite on a primitive nobody will ever give back.
STRANDED_GATE_SECONDS = 2.0


class _Clock:
    """A monotonic clock these scenarios advance explicitly.

    Every recorded duration below comes from this clock, so a gate wait of
    0.25s is 0.25s in the assertion whether or not the primitive was really
    contended and whatever the host was doing at the time.
    """

    def __init__(self, start: float = 1_000.0) -> None:
        self.now = float(start)

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += float(seconds)


class _RecordingGate:
    """A real admission primitive that also reports who took it, and when.

    Deliberately a wrapper rather than a stub. The failure scenarios assert
    that a read which raises still hands its gate back, and a counter-only
    fake cannot tell a released primitive from a leaked one -- the next
    acquisition would succeed either way. Wrapping a real ``Lock`` /
    ``BoundedSemaphore`` makes a leak show up where it would in production,
    as the next caller failing to be admitted (bounded here by
    ``STRANDED_GATE_SECONDS`` so the failure is a report, not a hang).
    """

    def __init__(
        self,
        name: str,
        primitive: Any,
        journal: list[str],
        clock: _Clock,
        *,
        wait_seconds: float = 0.0,
    ) -> None:
        self.name = name
        self.primitive = primitive
        self.journal = journal
        self.clock = clock
        self.wait_seconds = wait_seconds
        self.admissions = 0
        self.releases = 0

    def acquire(self, timeout: float | None = None) -> bool:
        self.journal.append(f"{self.name}:acquire")
        acquired = self.primitive.acquire(
            timeout=STRANDED_GATE_SECONDS if timeout is None else timeout
        )
        # Admission wall-clock is scripted, and is charged whether admission
        # succeeded or expired: a caller that waited out its budget waited.
        self.clock.advance(self.wait_seconds)
        if acquired:
            self.admissions += 1
        self.journal.append(f"{self.name}:{'admitted' if acquired else 'expired'}")
        return acquired

    def release(self) -> None:
        self.releases += 1
        self.journal.append(f"{self.name}:release")
        self.primitive.release()

    @property
    def outstanding(self) -> int:
        return self.admissions - self.releases

    def is_free(self) -> bool:
        """Whether the wrapped primitive can still be taken."""
        if not self.primitive.acquire(timeout=0):
            return False
        self.primitive.release()
        return True


class _AttributedReadLedger(PsqlShareLedger):
    """A ledger carrying only the read plumbing these scenarios touch.

    Built through ``__new__`` in the style the rest of the ledger suite uses
    for single-statement scenarios: the class under test is the shipped
    ``PsqlShareLedger``, and only the seams a read actually reaches are
    supplied.
    """

    def __init__(
        self,
        results: list[Any],
        *,
        gate_wait_seconds: float = 0.0,
        execute_seconds: float = 0.0,
        read_concurrency: int = 1,
    ) -> None:
        self.clock = _Clock()
        self.journal: list[str] = []
        self.queries: list[str] = []
        self.results = list(results)
        self.execute_seconds = execute_seconds
        self._monotonic = self.clock
        self._native = None
        self.writer_gate = _RecordingGate(
            WRITER_LOCK,
            threading.Lock(),
            self.journal,
            self.clock,
            wait_seconds=gate_wait_seconds,
        )
        self.read_gate = _RecordingGate(
            READ_SLOT,
            threading.BoundedSemaphore(read_concurrency),
            self.journal,
            self.clock,
            wait_seconds=gate_wait_seconds,
        )
        self._lock = self.writer_gate
        self._read_semaphore = self.read_gate
        self._prior_balances_read_stats_lock = threading.Lock()
        self._prior_balances_reads_total = 0
        self._prior_balances_read_last_seconds = 0.0
        self._prior_balances_read_max_seconds = 0.0
        self._ledger_read_timings: dict[str, dict[str, float | int]] = {}
        self._ledger_read_timings_lock = threading.Lock()

    def arm_deadline(self, remaining_seconds: float) -> None:
        """Give the calling thread a bounded operation budget."""
        deadline_local = threading.local()
        deadline_local.deadline = self.clock.now + remaining_seconds
        self._operation_timeout_local = deadline_local

    def _run_json(self, sql: str) -> Any:
        self.queries.append(sql)
        self.journal.append("statement")
        self.clock.advance(self.execute_seconds)
        if not self.results:
            raise AssertionError(f"unexpected extra query: {sql[:120]}")
        result = self.results.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result


class _UnattributedReadLedger(_AttributedReadLedger):
    """The same ledger with the pre-#224 admission body restored.

    ``_run_attributed_read_json`` is replaced by exactly what each call site
    used to do: take the gate through ``_operation_gate`` and run the same
    retry-safe statement, recording nothing. It is the differential control
    for "the SQL and the results did not change" -- both ledgers run the
    shipped method bodies, and only the read helper differs.
    """

    def _run_attributed_read_json(
        self,
        sql: str,
        *,
        operation: str,
        gate: Any,
        gate_name: str,
    ) -> Any:
        with self._operation_gate(gate, gate_name):
            return self._run_retry_safe_read_json(sql)


def share_row(share_seq: int, *, miner: str = "miner-a") -> dict[str, Any]:
    """One accepted-share payload in the shape the snapshot SQL returns."""
    return {
        "share_seq": share_seq,
        "share_id": f"share-{share_seq}",
        "miner_id": miner,
        "order_key": miner,
        "p2mr_program_hex": "11" * 32,
        "share_difficulty": "4096",
        "network_difficulty": "1048576",
        "template_height": 800_000 + share_seq,
        "job_id": f"job-{share_seq}",
        "job_issued_at_ms": 1_000 + share_seq,
        "accepted_at_ms": 1_001 + share_seq,
        "ntime": 1_700_000_000 + share_seq,
        "credit_policy": None,
    }


def balance_row(miner: str = "miner-a") -> dict[str, Any]:
    """One carry-forward balance in the shape both balance reads return."""
    return {
        "recipient_id": miner,
        "order_key": miner,
        "p2mr_program_hex": "22" * 32,
        "balance_sats": "2500",
    }


# Each attributed read, as (operation, gate name, canned result, call). The
# scenarios iterate this so a fifth read added to the contract without a
# scenario is a missing row here rather than a silently untested path.
def read_cases() -> tuple[tuple[str, str, Any, Any], ...]:
    return (
        (
            "payout_window_snapshot",
            READ_SLOT,
            [share_row(1), share_row(2)],
            lambda ledger: ledger.snapshot_at_job_issue(1_005, window_weight=64),
        ),
        (
            "payout_window_delta",
            READ_SLOT,
            [share_row(3)],
            lambda ledger: ledger.snapshot_between_job_issues(1_005, 1_006),
        ),
        (
            "prior_balances_after_pool_block",
            READ_SLOT,
            [balance_row()],
            lambda ledger: ledger.prior_balances_after_pool_block(
                block_hash=BLOCK_HASH
            ),
        ),
        (
            "current_prior_balances",
            WRITER_LOCK,
            [balance_row()],
            lambda ledger: ledger.current_prior_balances(),
        ),
    )


class LedgerReadAttributionTests(unittest.TestCase):
    def test_each_read_records_under_its_contract_operation(self) -> None:
        """The four names are the Wave 0 ones, spelled exactly."""
        for operation, _gate_name, result, call in read_cases():
            with self.subTest(operation=operation):
                ledger = _AttributedReadLedger([result])

                call(ledger)

                self.assertIn(operation, PRISM_LEDGER_READ_OPERATIONS)
                stats = ledger.ledger_read_gate_stats()
                # Exactly one operation, exactly once: a read that recorded
                # twice would double-count the landing it is meant to explain.
                self.assertEqual(list(stats), [operation])
                self.assertEqual(stats[operation]["calls_total"], 1)

    def test_the_shipped_enumeration_attribution_is_untouched(self) -> None:
        """``pending_block_candidate_rows`` keeps its name and its read slot."""
        ledger = _AttributedReadLedger(
            [[{"block_hash": BLOCK_HASH, "candidate": {}, "pool_block_exists": False}]]
        )

        rows = ledger.pending_block_candidate_rows(limit=8)

        self.assertEqual([row["block_hash"] for row in rows], [BLOCK_HASH])
        self.assertEqual(rows[0]["pool_block_exists"], False)
        self.assertEqual(
            list(ledger.ledger_read_gate_stats()),
            ["pending_block_candidate_rows"],
        )
        self.assertEqual(ledger.read_gate.admissions, 1)
        self.assertEqual(ledger.writer_gate.admissions, 0)

    def test_each_read_keeps_the_admission_primitive_it_had(self) -> None:
        """The writer-lock read stays a writer-lock read, and vice versa.

        This is the invariant most at risk while adding attribution: the
        attributed helper used to be hard-wired to the read semaphore, so
        routing the prior-balances reread through it is exactly where that
        read could have been silently moved off the writer lock it needs.
        """
        for operation, gate_name, result, call in read_cases():
            with self.subTest(operation=operation):
                ledger = _AttributedReadLedger([result])

                call(ledger)

                writer_lock = gate_name == WRITER_LOCK
                taken = ledger.writer_gate if writer_lock else ledger.read_gate
                untaken = ledger.read_gate if writer_lock else ledger.writer_gate
                self.assertEqual(taken.admissions, 1)
                self.assertEqual(taken.releases, 1)
                # Not merely "the right gate was taken": the other one was
                # never touched, so no read acquired both or swapped classes.
                self.assertEqual(untaken.admissions, 0)
                self.assertEqual(untaken.releases, 0)
                # And admission precedes the statement it admits.
                self.assertEqual(
                    ledger.journal,
                    [
                        f"{gate_name}:acquire",
                        f"{gate_name}:admitted",
                        "statement",
                        f"{gate_name}:release",
                    ],
                )

    def test_read_slot_reads_never_reach_the_writer_lock(self) -> None:
        """A refusing writer gate is invisible to the three read-slot reads.

        The read-only ledger substitutes ``_RefusingWriterGate`` for the
        writer lock, so any path that quietly took it fails loudly. The three
        read-slot reads must be unaffected; the prior-balances reread must
        still be refused, which is the same statement made from the other
        side: it really is on the writer lock.
        """
        for operation, gate_name, result, call in read_cases():
            with self.subTest(operation=operation):
                ledger = _AttributedReadLedger([result])
                ledger._lock = _RefusingWriterGate()

                if gate_name == WRITER_LOCK:
                    with self.assertRaises(ReadOnlyLedgerError):
                        call(ledger)
                    # Refused before any statement was sent.
                    self.assertEqual(ledger.queries, [])
                    self.assertEqual(ledger.read_gate.admissions, 0)
                else:
                    self.assertEqual(len(call(ledger)), len(result))
                    self.assertEqual(len(ledger.queries), 1)
                    self.assertEqual(ledger.read_gate.admissions, 1)

    def test_gate_wait_and_execution_are_separate_samples(self) -> None:
        """The whole point: which half of a slow read was slow."""
        for operation, _gate_name, result, call in read_cases():
            with self.subTest(operation=operation):
                ledger = _AttributedReadLedger(
                    [result],
                    gate_wait_seconds=0.25,
                    execute_seconds=1.5,
                )

                call(ledger)

                stats = ledger.ledger_read_gate_stats()[operation]
                self.assertAlmostEqual(stats["gate_wait_seconds_total"], 0.25)
                self.assertAlmostEqual(stats["gate_wait_seconds_max"], 0.25)
                self.assertAlmostEqual(stats["execute_seconds_total"], 1.5)
                self.assertAlmostEqual(stats["execute_seconds_max"], 1.5)
                # Neither timeout counter fires on a read that completed.
                self.assertEqual(stats["gate_timeouts_total"], 0)
                self.assertEqual(stats["execute_timeouts_total"], 0)

    def test_admission_timeout_is_a_gate_timeout_with_no_execution_sample(
        self,
    ) -> None:
        """A budget spent queueing is never charged to PostgreSQL.

        The gate is genuinely held by another holder, so the expiry is a real
        admission expiry rather than a scripted one, and it names the
        primitive the caller was waiting for.
        """
        for operation, gate_name, result, call in read_cases():
            with self.subTest(operation=operation):
                ledger = _AttributedReadLedger([result], gate_wait_seconds=4.0)
                held = (
                    ledger.writer_gate if gate_name == WRITER_LOCK else ledger.read_gate
                )
                self.assertTrue(held.primitive.acquire(timeout=0))
                self.addCleanup(held.primitive.release)
                ledger.arm_deadline(0.01)

                with self.assertRaises(LedgerOperationTimeout) as raised:
                    call(ledger)

                self.assertIn(gate_name, str(raised.exception))
                # No statement was ever sent, so there is nothing to charge to
                # the server -- not even a zero-length execution sample.
                self.assertEqual(ledger.queries, [])
                stats = ledger.ledger_read_gate_stats()[operation]
                self.assertEqual(stats["calls_total"], 1)
                self.assertEqual(stats["gate_timeouts_total"], 1)
                self.assertEqual(stats["execute_timeouts_total"], 0)
                self.assertEqual(stats["execute_seconds_total"], 0.0)
                self.assertEqual(stats["execute_seconds_max"], 0.0)
                self.assertAlmostEqual(stats["gate_wait_seconds_max"], 4.0)
                # A failed admission releases nothing, because it took nothing.
                self.assertEqual(held.admissions, 0)
                self.assertEqual(held.releases, 0)

    def test_execution_timeout_is_an_execution_timeout(self) -> None:
        """A deadline that expired inside the statement belongs to the server.

        Including the tail a server-cancelled statement spends returning: the
        elapsed execution is recorded, not discarded, so the operator sees how
        long PostgreSQL actually held the budget.
        """
        for operation, _gate_name, _result, call in read_cases():
            with self.subTest(operation=operation):
                ledger = _AttributedReadLedger(
                    [LedgerOperationTimeout("postgres operation exceeded 5s")],
                    gate_wait_seconds=0.1,
                    execute_seconds=5.0,
                )

                with self.assertRaisesRegex(LedgerOperationTimeout, "exceeded 5s"):
                    call(ledger)

                stats = ledger.ledger_read_gate_stats()[operation]
                self.assertEqual(stats["calls_total"], 1)
                self.assertEqual(stats["gate_timeouts_total"], 0)
                self.assertEqual(stats["execute_timeouts_total"], 1)
                self.assertAlmostEqual(stats["execute_seconds_max"], 5.0)
                self.assertAlmostEqual(stats["gate_wait_seconds_max"], 0.1)

    def test_failing_reads_release_the_primitive_and_keep_the_error(self) -> None:
        """A raising statement leaks nothing and masks nothing.

        Attribution runs in a ``finally``; a version of it that recorded
        before releasing, or that swallowed the statement's exception into a
        bookkeeping one, would strand the coordinator's writer lock or hide
        the failure that mattered.
        """
        for operation, gate_name, _result, call in read_cases():
            with self.subTest(operation=operation):
                failure = RuntimeError("postgres query failed: connection reset")
                ledger = _AttributedReadLedger([failure])
                gate = (
                    ledger.writer_gate if gate_name == WRITER_LOCK else ledger.read_gate
                )

                with self.assertRaises(RuntimeError) as raised:
                    call(ledger)

                # The original exception object, not a wrapper and not a
                # timeout misclassification.
                self.assertIs(raised.exception, failure)
                self.assertEqual(gate.admissions, 1)
                self.assertEqual(gate.releases, 1)
                self.assertEqual(gate.outstanding, 0)
                self.assertTrue(gate.is_free())
                # A failure is still a call, and still not a timeout.
                stats = ledger.ledger_read_gate_stats()[operation]
                self.assertEqual(stats["calls_total"], 1)
                self.assertEqual(stats["gate_timeouts_total"], 0)
                self.assertEqual(stats["execute_timeouts_total"], 0)

    def test_a_released_gate_admits_the_next_read(self) -> None:
        """The proof that release was real: the same gate serves again.

        Run against a read slot of one and a single writer lock, so a read
        that failed to give its primitive back cannot be admitted a second
        time (see ``STRANDED_GATE_SECONDS``).
        """
        for operation, _gate_name, result, call in read_cases():
            with self.subTest(operation=operation):
                ledger = _AttributedReadLedger(
                    [RuntimeError("first attempt failed"), result],
                    read_concurrency=1,
                )

                with self.assertRaises(RuntimeError):
                    call(ledger)
                second = call(ledger)

                self.assertEqual(len(second), len(result))
                self.assertEqual(
                    ledger.ledger_read_gate_stats()[operation]["calls_total"], 2
                )


class UnchangedBehaviorTests(unittest.TestCase):
    """Everything except the bookkeeping must be byte-for-byte what it was."""

    def test_sql_and_results_match_the_unattributed_path(self) -> None:
        """Differential against the pre-#224 admission body.

        Both ledgers run the shipped method; only the read helper differs --
        one attributes, one reproduces the old ``_operation_gate`` +
        ``_run_retry_safe_read_json`` pair. Identical SQL text and identical
        return values is the statement that this wave added a measurement and
        nothing else.
        """
        for operation, _gate_name, result, call in read_cases():
            with self.subTest(operation=operation):
                # Deep copies: both balance reads convert ``balance_sats``
                # in place, and a shared row would let one ledger's parsing
                # feed the other's.
                attributed = _AttributedReadLedger([copy.deepcopy(result)])
                legacy = _UnattributedReadLedger([copy.deepcopy(result)])

                new_value = call(attributed)
                old_value = call(legacy)

                self.assertEqual(len(attributed.queries), 1)
                self.assertEqual(attributed.queries, legacy.queries)
                self.assertEqual(new_value, old_value)
                # The legacy path records nothing, so equal results cannot be
                # an artifact of both sides sharing the new helper.
                self.assertEqual(legacy.ledger_read_gate_stats(), {})

    def test_snapshot_records_parse_exactly_as_before(self) -> None:
        """Result parsing is untouched: same fields, same types, same order."""
        ledger = _AttributedReadLedger([[share_row(1), share_row(2, miner="miner-b")]])

        records = ledger.snapshot_at_job_issue(1_005, window_weight=64)

        self.assertEqual([type(record) for record in records], [AcceptedShareRecord] * 2)
        self.assertEqual([record.share_seq for record in records], [1, 2])
        self.assertEqual([record.miner_id for record in records], ["miner-a", "miner-b"])
        self.assertEqual([record.share_difficulty for record in records], [4096, 4096])
        self.assertIsNone(records[0].credit_policy)

    def test_balances_are_still_returned_as_integers(self) -> None:
        """Both balance reads keep converting ``balance_sats`` out of text."""
        for call in (
            lambda ledger: ledger.prior_balances_after_pool_block(block_hash=BLOCK_HASH),
            lambda ledger: ledger.current_prior_balances(),
        ):
            with self.subTest(call=call):
                ledger = _AttributedReadLedger([[balance_row()]])

                balances = call(ledger)

                self.assertEqual(
                    balances,
                    [{**balance_row(), "balance_sats": 2500}],
                )

    def test_snapshot_sql_still_carries_its_window_bound(self) -> None:
        """The statements themselves are the ones the audit path depends on."""
        ledger = _AttributedReadLedger([[share_row(1)], [share_row(2)]])

        ledger.snapshot_at_job_issue(1_005, window_weight=64)
        ledger.snapshot_between_job_issues(1_005, 1_006)

        snapshot_sql, delta_sql = ledger.queries
        self.assertIn("WITH RECURSIVE pages AS", snapshot_sql)
        self.assertIn("cumulative_difficulty - share_difficulty < 64::numeric", snapshot_sql)
        self.assertIn("ORDER BY share_seq ASC", snapshot_sql)
        # The delta's two disjoint branches are what keep a share from being
        # credited twice across consecutive anchors.
        self.assertIn("UNION ALL", delta_sql)
        self.assertIn("ledger.accepted_at > to_timestamp", delta_sql)
        self.assertIn("ledger.job_issued_at > to_timestamp", delta_sql)

    def test_empty_delta_still_short_circuits_without_a_gate_or_a_statement(
        self,
    ) -> None:
        """An unmoved anchor reads nothing, so it records nothing.

        A sample here would be a lie about work that never happened, and would
        inflate the delta operation's call count on every idle rebuild.
        """
        ledger = _AttributedReadLedger([])

        self.assertEqual(ledger.snapshot_between_job_issues(1_005, 1_005), [])

        self.assertEqual(ledger.queries, [])
        self.assertEqual(ledger.journal, [])
        self.assertEqual(ledger.ledger_read_gate_stats(), {})

    def test_backwards_anchor_is_still_rejected_before_admission(self) -> None:
        ledger = _AttributedReadLedger([])

        with self.assertRaisesRegex(ValueError, "snapshot anchor moved backwards"):
            ledger.snapshot_between_job_issues(1_006, 1_005)

        self.assertEqual(ledger.journal, [])

    def test_legacy_prior_balances_aggregate_is_preserved(self) -> None:
        """``qbit_prism_prior_balances_*`` keeps its old meaning.

        It still counts only successful reads and still covers admission and
        execution together. The new per-operation sample duplicates part of it
        on purpose: an operator comparing the existing series across this
        upgrade must not find it silently redefined.
        """
        ledger = _AttributedReadLedger(
            [[balance_row()], RuntimeError("postgres query failed")],
            gate_wait_seconds=0.5,
            execute_seconds=2.0,
        )

        ledger.current_prior_balances()
        with self.assertRaises(RuntimeError):
            ledger.current_prior_balances()

        legacy = ledger.prior_balances_read_stats()
        # One success, one failure: the failure is absent from the legacy
        # aggregate, exactly as before, and present in the new one.
        self.assertEqual(legacy["reads_total"], 1)
        self.assertGreaterEqual(legacy["last_seconds"], 0.0)
        self.assertGreaterEqual(legacy["max_seconds"], legacy["last_seconds"])
        self.assertEqual(
            ledger.ledger_read_gate_stats()["current_prior_balances"]["calls_total"],
            2,
        )


class LabelBoundednessTests(unittest.TestCase):
    def test_every_attributed_call_site_names_a_contract_operation(self) -> None:
        """The literals in the module are the closed vocabulary, and all of it.

        ``tests.test_prism_accepted_preview_telemetry`` already fails on a
        literal outside the contract. This is the other direction: the four
        names this wave was asked for are actually present, so a lane that
        wired up only some of them cannot pass.
        """
        source = (REPO_ROOT / "lab" / "prism" / "share_ledger.py").read_text()
        literals = set(re.findall(r'operation="([^"]+)"', source))

        self.assertEqual(
            literals,
            {
                "pending_block_candidate_rows",
                "payout_window_snapshot",
                "payout_window_delta",
                "current_prior_balances",
                "prior_balances_after_pool_block",
            },
        )
        self.assertEqual(sorted(literals - set(PRISM_LEDGER_READ_OPERATIONS)), [])

    def test_an_out_of_contract_name_folds_to_other_before_rendering(self) -> None:
        """A drifted call site costs an attribution, never a new series.

        The recorder itself keeps whatever name it is handed -- it is a plain
        dict, and rejecting there would turn a metrics mistake into a failed
        ledger read. Boundedness is the renderer's job, and this is where the
        two meet.
        """
        ledger = _AttributedReadLedger([[balance_row()], [balance_row()]])

        ledger.current_prior_balances()
        ledger._run_attributed_read_json(
            "SELECT json_build_object('ok', true);",
            operation="block_hash=" + BLOCK_HASH,
            gate=ledger._read_semaphore,
            gate_name=READ_SLOT,
        )

        raw = ledger.ledger_read_gate_stats()
        self.assertEqual(len(raw), 2)
        folded = fold_ledger_read_stats(raw)
        self.assertEqual(
            sorted(folded),
            ["current_prior_balances", "other"],
        )
        for operation in folded:
            self.assertIn(operation, PRISM_LEDGER_READ_OPERATIONS)

    def test_many_unknown_names_still_render_as_one_series(self) -> None:
        """Unbounded cardinality at the call site stays one label at the edge."""
        ledger = _AttributedReadLedger([[] for _ in range(8)])

        for index in range(8):
            ledger._run_attributed_read_json(
                "SELECT json_build_object('ok', true);",
                operation=f"snapshot_at_job_issue_{index}",
                gate=ledger._read_semaphore,
                gate_name=READ_SLOT,
            )

        folded = fold_ledger_read_stats(ledger.ledger_read_gate_stats())
        self.assertEqual(list(folded), ["other"])
        self.assertEqual(folded["other"]["calls_total"], 8)


if __name__ == "__main__":
    unittest.main()
