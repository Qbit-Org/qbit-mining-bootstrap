#!/usr/bin/env python3
"""Replay enumeration must not queue behind the coordinator's writer convoy.

Issue #211. ``PsqlShareLedger.pending_block_candidate_rows()`` is one
read-only, single-snapshot statement, but it used to take
``_operation_gate(self._lock, "writer lock")`` before sending it. During
accepted-block accounting that put a *read* in the same coordinator-local
convoy as the accounting write, and on ``union-mainnet`` the read spent most
of its bounded budget there: the outer call reported
``replay-outbox-query exceeded 5s`` while the inner PostgreSQL deadline still
had ~0.4-1.3s left, PostgreSQL showed zero blocked backends, and the host was
~96% idle. Accepted candidates still converged, but in 79-186s, and
``qbit_prism_accepted_parent_unresolved_oldest_seconds`` reached ~101s.

These scenarios drive that interleaving deterministically. The writer gate is
genuinely held by a fenced write that has sent its statement and not yet
committed -- the shape a long accounting write has -- and the enumeration runs
against it. The evidence is not merely "the read returned": the same held gate,
under the same budget, is shown to *fail* writer admission on another actor, so
a run cannot pass by accident of a gate that happened to be free.

The harness is the #128 deterministic one, extended here with the pending-page
statement (``LandingOp.OUTBOX_PENDING_PAGE``); the code under test is the
shipped ``PsqlShareLedger``, reached through its real ports.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lab.prism.share_ledger import LedgerOperationTimeout  # noqa: E402
from tests.prism_concurrency_harness import (  # noqa: E402
    LeaseHarness,
    assert_deterministic,
)
from tests.prism_landing_harness import PARENT_HASH, LandingHarness  # noqa: E402

BLOCK_A = "aa" * 32
BLOCK_B = "bb" * 32

# The production fast-call budget the incident ran with
# (PRISM_BLOCK_SUBMIT_DB_TIMEOUT_SECONDS=5). Every enumeration below is armed
# with exactly this, and every writer hold outlasts it.
FAST_CALL_BUDGET_SECONDS = 5.0

# How long the fenced write stays parked mid-statement. Deliberately longer
# than the fast-call budget: that gap is the whole defect.
WRITER_HOLD_SECONDS = 12.0

# Statement checkpoints FakePostgres offers, tagged with the coordinator name.
FENCED_WRITE_PRECOMMIT = "writer.precommit"
PAGE_STATEMENT_BEGIN = "writer.begin:outbox_pending_page"


def _intent(block_hash: str) -> dict[str, Any]:
    return {
        "schema": "qbit.prism.block-candidate-intent.v1",
        "block_hash_hex": block_hash,
        "block_hex": "00",
    }


def _booted(harness: LeaseHarness) -> Any:
    """Boot one coordinator and make its ledger gates schedulable.

    The gates are the subject, so they have to be visible to the scheduler:
    a real ``threading.Lock`` would block an actor thread outside the baton
    and read as a harness stall rather than as the contention it is. These
    are the same substitutions the landing harness makes, and they change how
    a waiter blocks, never what the ledger decides.
    """
    coordinator = harness.coordinator("writer")
    coordinator.start()
    harness.run_until(coordinator.actor, "done:startup")
    ledger = coordinator.ledger
    ledger._lock = harness.lock("ledger-writer")
    ledger._read_semaphore = harness.semaphore("ledger-read-slot", 4)
    return ledger


def _park_a_fenced_write(harness: LeaseHarness, ledger: Any, block_hash: str) -> Any:
    """Leave a fenced outbox write in flight, holding the writer gate.

    ``persist_block_candidate_intent`` goes through ``_run_fenced_json``, so
    the writer gate is held for the whole statement. Arming a deadline is what
    makes the backend run BEGIN / statement / COMMIT rather than autocommit,
    which gives the scenario a checkpoint *inside* the held gate -- the
    accounting write's own shape, and the only place a read can observe the
    convoy.
    """
    writer = harness.scheduler.actor("long-write")

    def run() -> Any:
        with ledger.operation_timeout(WRITER_HOLD_SECONDS * 4):
            return ledger.persist_block_candidate_intent(_intent(block_hash))

    call = writer.submit(run, label=f"fenced-write:{block_hash[:4]}")
    harness.run_until(writer, FENCED_WRITE_PRECOMMIT)
    return writer, call


def _enumerate(ledger: Any) -> list[dict[str, Any]]:
    """One replay enumeration under the incident's own fast-call budget."""
    with ledger.operation_timeout(FAST_CALL_BUDGET_SECONDS):
        return ledger.pending_block_candidate_rows(limit=32)


def _take_writer_gate(ledger: Any) -> None:
    """Ask for writer admission on the same budget: the control."""
    with ledger.operation_timeout(FAST_CALL_BUDGET_SECONDS):
        ledger._acquire_operation_gate(ledger._lock, "writer lock")
        ledger._lock.release()


class EnumerationOutsideTheWriterConvoyTests(unittest.TestCase):
    def _scenario(self, harness: LeaseHarness) -> dict[str, Any]:
        """Enumerate a durable backlog while a fenced write holds the gate."""
        ledger = _booted(harness)

        # A durable pending candidate exists before the convoy forms, so the
        # enumeration has something real to return.
        setup = harness.scheduler.actor("setup")
        setup.submit(
            lambda: ledger.persist_block_candidate_intent(_intent(BLOCK_A)),
            label="seed",
        )
        harness.run_until(setup, "done:seed")

        # A second candidate's fenced write is now in flight and parked: its
        # statement has been sent, its COMMIT has not, and the writer gate is
        # held for the duration.
        long_write, long_call = _park_a_fenced_write(harness, ledger, BLOCK_B)
        gate_held_during_read = ledger._lock.locked()

        # The replay enumeration, under the incident's own budget.
        replay = harness.scheduler.actor("replay")
        page_call = replay.submit(
            lambda: _enumerate(ledger),
            label="replay-outbox-query",
        )

        # A second reader asks for *writer* admission under the same budget
        # against the same held gate. It is the control: it proves the hold is
        # real and outlasts the budget, so the enumeration's success below
        # cannot be a gate that happened to be free.
        contender = harness.scheduler.actor("writer-admission")
        admission_call = contender.submit(
            lambda: _take_writer_gate(ledger),
            label="writer-admission",
        )
        harness.run_until_blocked(contender, "lock:ledger-writer")

        # The enumeration reaches PostgreSQL with the gate still held...
        harness.run_until(replay, PAGE_STATEMENT_BEGIN)
        gate_held_at_statement = ledger._lock.locked()
        # ...and spends measurable time in the server rather than in the gate.
        harness.advance(0.75)
        harness.run_until(replay, "done:replay-outbox-query")

        # Only now does the hold outlast the fast-call budget. The contender
        # gives up; the fenced write is still parked.
        harness.advance(WRITER_HOLD_SECONDS)
        harness.drain([contender])

        harness.drain([long_write, contender, replay])
        return {
            "page": page_call.value(),
            "gate_held_during_read": gate_held_during_read,
            "gate_held_at_statement": gate_held_at_statement,
            "admission_error": type(admission_call.error).__name__,
            "admission_message": str(admission_call.error),
            "long_write_result": (
                type(long_call.error).__name__
                if long_call.error is not None
                else str(long_call.value())
            ),
            "stats": ledger.ledger_read_gate_stats()[
                "pending_block_candidate_rows"
            ],
        }

    def test_enumeration_completes_through_the_read_path(self) -> None:
        with LeaseHarness() as harness:
            result = self._scenario(harness)

        # The gate really was held, both when the read started and when its
        # statement reached the server.
        self.assertTrue(result["gate_held_during_read"])
        self.assertTrue(result["gate_held_at_statement"])
        # The control proves the hold outlasted the identical budget.
        self.assertEqual(result["admission_error"], "LedgerOperationTimeout")
        self.assertIn("writer lock", result["admission_message"])
        # And the enumeration returned the durable backlog anyway.
        #
        # Exactly the durable backlog: BLOCK_B's row is staged inside the
        # parked write's uncommitted transaction, and the page -- on its own
        # pooled session, at its own snapshot -- does not see it. That is the
        # commit-before-wakeup ordering the outbox relies on, stated from the
        # reader's side: enumeration never surfaces a candidate before the row
        # that makes it durable exists.
        self.assertEqual(
            [row["block_hash"] for row in result["page"]],
            [BLOCK_A],
        )
        self.assertEqual(
            [row["pool_block_exists"] for row in result["page"]],
            [False],
        )
        # The long write is untouched by any of it: it commits when it resumes.
        self.assertEqual(
            result["long_write_result"],
            "BlockCandidateIntentPersistResult(inserted=True, state='pending')",
        )

    def test_no_time_is_charged_to_local_admission(self) -> None:
        """The attribution says database, because the database is where it went.

        The incident was unattributable from ``/metrics``: one duration
        covered both admission and execution, so "exceeded 5s" could not be
        told apart from a slow query. These two fields are that distinction.
        """
        with LeaseHarness() as harness:
            stats = self._scenario(harness)["stats"]

        self.assertEqual(stats["calls_total"], 1)
        self.assertEqual(stats["gate_wait_seconds_max"], 0.0)
        self.assertEqual(stats["gate_timeouts_total"], 0)
        self.assertEqual(stats["execute_timeouts_total"], 0)
        # The 0.75s advanced while the statement was in flight lands on the
        # execution series, not the gate series.
        self.assertAlmostEqual(stats["execute_seconds_max"], 0.75)
        self.assertAlmostEqual(stats["execute_seconds_total"], 0.75)

    def test_the_interleaving_is_deterministic(self) -> None:
        assert_deterministic(self, self._scenario)

    def test_enumeration_still_queues_on_the_bounded_read_slot(self) -> None:
        """Bounded, not unbounded: the read slot is still the admission limit.

        Removing the writer gate must not mean removing every gate. A read
        semaphore of one admits one enumeration; the second waits for a slot
        rather than opening a connection or starting a thread of its own, and
        it names the read slot when its own budget expires.
        """
        with LeaseHarness() as harness:
            ledger = _booted(harness)
            ledger._read_semaphore = harness.semaphore("ledger-read-slot", 1)

            setup = harness.scheduler.actor("setup")
            setup.submit(
                lambda: ledger.persist_block_candidate_intent(_intent(BLOCK_A)),
                label="seed",
            )
            harness.run_until(setup, "done:seed")

            first = harness.scheduler.actor("replay-a")
            second = harness.scheduler.actor("replay-b")
            first_call = first.submit(
                lambda: _enumerate(ledger),
                label="page-a",
            )
            second_call = second.submit(
                lambda: _enumerate(ledger),
                label="page-b",
            )

            # The first holds the only slot, parked inside its statement.
            harness.run_until(first, PAGE_STATEMENT_BEGIN)
            harness.run_until_blocked(second, "semaphore:ledger-read-slot")

            # The second's own budget expires while it waits for admission.
            harness.advance(FAST_CALL_BUDGET_SECONDS * 2)
            harness.drain([second])
            harness.drain([first, second])

            self.assertIsNone(first_call.error)
            self.assertEqual(
                [row["block_hash"] for row in first_call.value()],
                [BLOCK_A],
            )
            self.assertIsInstance(second_call.error, LedgerOperationTimeout)
            self.assertIn("read slot", str(second_call.error))

            # An admission expiry is counted as one, and never as a statement
            # that PostgreSQL was slow to answer.
            stats = ledger.ledger_read_gate_stats()[
                "pending_block_candidate_rows"
            ]
            self.assertEqual(stats["calls_total"], 2)
            self.assertEqual(stats["gate_timeouts_total"], 1)
            self.assertEqual(stats["execute_timeouts_total"], 0)
            self.assertGreaterEqual(
                stats["gate_wait_seconds_max"],
                FAST_CALL_BUDGET_SECONDS,
            )

    def test_no_extra_connection_or_thread_is_taken(self) -> None:
        """The page read borrows a pooled connection, as it always did.

        The fix must not buy latency with a connection per call: the pool is
        sized ``read_concurrency + 1`` precisely so the read slots and the
        single writer cannot together outrun it.
        """
        with LeaseHarness() as harness:
            ledger = _booted(harness)
            backend = harness.server
            pool = ledger._native

            setup = harness.scheduler.actor("setup")
            setup.submit(
                lambda: ledger.persist_block_candidate_intent(_intent(BLOCK_A)),
                label="seed",
            )
            harness.run_until(setup, "done:seed")
            sessions_before = len(backend.live_backends())

            replay = harness.scheduler.actor("replay")
            replay.submit(lambda: _enumerate(ledger), label="page")
            harness.run_until(replay, "done:page")

            self.assertEqual(len(backend.live_backends()), sessions_before)
            self.assertEqual(
                pool.pool_size,
                ledger._read_semaphore.value + len(pool._in_use) + 1,
            )


class StalePageAgainstALandingWriteTests(unittest.TestCase):
    """The production landing topology, with the enumeration reading across it.

    Class one proves the gate is no longer taken. This one proves that not
    taking it is *safe*: the page a caller now gets can be stale about a
    candidate that lands while the read is in flight, and every terminal
    decision built on that page must still refuse the landed row. The refusal
    is the production ``mark_block_candidates_abandoned`` statement, whose
    pool-block re-check sits inside the same fenced UPDATE that transitions
    the rows.
    """

    def _scenario(self, harness: LandingHarness) -> dict[str, Any]:
        ledger = harness.boot()
        block_a = harness.found_block(BLOCK_A, height=10, parent_hash=PARENT_HASH)
        block_b = harness.found_block(BLOCK_B, height=11, parent_hash=BLOCK_A)

        # Both candidates are durable before either lands: closely spaced
        # found blocks, which is the population #211 measured converging in
        # 79-186s.
        harness.appender.submit(
            lambda: harness._persist_intent(block_b),
            label="seed-b",
        )
        harness.run_until(harness.appender, "done:seed-b")

        # A's accepted-block accounting write is in flight: the statement has
        # been sent, its COMMIT has not, and the writer gate is held for the
        # whole of it.
        harness.land_on_accounting_tail(block_a)
        harness.run_until(harness.accounting, "ledger.done:persist_block")
        harness.step(harness.accounting)
        gate_held_during_read = ledger._lock.locked()

        # Writer admission under the fast-call budget cannot get through that
        # hold -- the control for everything below.
        contender = harness.scheduler.actor("writer-admission")
        admission_call = contender.submit(
            lambda: _take_writer_gate(ledger),
            label="writer-admission",
        )
        harness.run_until_blocked(contender, "lock:ledger-writer")

        # The enumeration does, on the same budget.
        replay = harness.scheduler.actor("replay")
        page_call = replay.submit(
            lambda: _enumerate(ledger),
            label="replay-outbox-query",
        )
        harness.run_until(replay, "done:replay-outbox-query")
        page = page_call.value()

        harness.advance(FAST_CALL_BUDGET_SECONDS * 2)
        harness.drain([contender])

        # A's write commits. Its pool-block row is now durable while its
        # outbox row is still pending, and the writer gate is free again --
        # the exact window in which a caller holding the stale page acts.
        harness.run_until(harness.accounting, "ledger.begin:prior_balances_as_of")
        landed = harness.pool_block(BLOCK_A) is not None

        terminal = harness.scheduler.actor("collapse")
        abandon_call = terminal.submit(
            lambda: ledger.mark_block_candidates_abandoned(
                block_hashes=[str(row["block_hash"]) for row in page],
                error="superseded by a decided height",
            ),
            label="batch-abandon",
        )
        harness.run_until(terminal, "done:batch-abandon")

        harness.drain([harness.accounting, harness.appender, terminal])
        return {
            "gate_held_during_read": gate_held_during_read,
            "admission_error": type(admission_call.error).__name__,
            "page": [
                (str(row["block_hash"]), bool(row["pool_block_exists"]))
                for row in page
            ],
            "landed_before_the_terminal_write": landed,
            "abandoned": list(abandon_call.value()),
            "outbox_a": harness.outbox_state(block_a),
            "outbox_b": harness.outbox_state(block_b),
        }

    def test_enumeration_crosses_a_landing_write_and_stays_advisory(self) -> None:
        with LandingHarness() as harness:
            result = self._scenario(harness)

        # The gate was genuinely held, and genuinely impassable on the budget.
        self.assertTrue(result["gate_held_during_read"])
        self.assertEqual(result["admission_error"], "LedgerOperationTimeout")

        # The page came back, and it is stale about A: A's pool-block row
        # exists only inside the uncommitted landing transaction, so this
        # snapshot cannot see it. That staleness is allowed -- it is what
        # "advisory" means -- and it is exactly the input the next assertion
        # feeds to a terminal write.
        #
        # The order is A then B even though B was persisted first: both rows
        # carry the same creation stamp here, and the total order's block-hash
        # tiebreak decides. That tiebreak is the reason a cursor cannot replay
        # or skip a colliding group.
        self.assertEqual(
            result["page"],
            [(BLOCK_A, False), (BLOCK_B, False)],
        )

    def test_a_landed_candidate_is_refused_by_the_fence_not_the_page(self) -> None:
        with LandingHarness() as harness:
            result = self._scenario(harness)

        # By the time the terminal write runs, A has landed and is still
        # pending -- both halves matter, or the row would be excluded by the
        # ordinary state predicate and prove nothing about the re-check.
        self.assertTrue(result["landed_before_the_terminal_write"])

        # The caller asked to abandon both hashes on the strength of a page
        # that said neither had landed. The fenced UPDATE re-asked
        # qbit_pool_blocks and returned only the row that had not.
        self.assertEqual(result["abandoned"], [BLOCK_B])
        self.assertEqual(result["outbox_b"], "abandoned")
        # A survived the stale page and finished its landing normally.
        self.assertEqual(result["outbox_a"], "submitted")

    def test_the_interleaving_is_deterministic(self) -> None:
        assert_deterministic(
            self,
            self._scenario,
            harness_factory=LandingHarness,
        )


if __name__ == "__main__":  # pragma: no cover - direct invocation
    unittest.main()
