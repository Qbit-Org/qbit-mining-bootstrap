#!/usr/bin/env python3
"""Definitive-acceptance priority at the block-accounting handoff (#181).

The 2026-08-21 testnet4 run left a tail of accepted-parent preview waits --
the first two at only 0.32 PH/s -- behind stale same-height replay work that
the accounting lane served first.  The task that carries the decided block
already knows it is decided: its ``_BlockCandidateNodeSubmission`` is an
attempted offer that raised nothing and returned nothing.

These tests own that classification and the lane it selects:

* the discriminator itself, including the two shapes that are easy to
  misread as acceptance (a transport failure, which also leaves ``result``
  at ``None``, and a ``"duplicate"`` rejection);
* the issue's own counterexample -- three durable same-height replay tasks
  queued before one definitively accepted task -- now draining accepted
  first;
* the burst case, where the primary handoff queue is at its bound and
  spillover has already begun.  This is the case a priority *key* on the
  primary queue cannot reach: once the overflow queue is non-empty every
  later handoff joins it, and the loop drains the primary in full before it
  ever looks at the overflow, so the accepted task would be served last
  exactly when it matters most;
* the fairness bound that keeps the accepted lane from becoming a
  starvation lane, stated as a bound rather than as an argument; and
* the spillover put rule's own anti-starvation property, which this change
  leaves byte-for-byte alone and which is asserted here so it stays that
  way.
"""

from __future__ import annotations

import itertools
import sys
import threading
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lab.prism.block_candidates import (  # noqa: E402
    BLOCK_ACCOUNTING_ACCEPTED_DISPATCH_QUOTA,
    DEFAULT_BLOCK_ACCOUNTING_QUEUE_DEPTH,
    _BlockCandidateAccountingTask,
    _BlockCandidateNodeSubmission,
    _is_definitive_node_acceptance,
)
from lab.prism.share_ledger import (  # noqa: E402
    PendingShare,
    SingleWriterShareLedger,
)
from tests.prism_vardiff_test_support import (  # noqa: E402
    RejectingSubmitTipRpc,
    block_candidate,
    submit_coordinator,
)


# The parent every seeded job builds on, and the tip the chain moved to
# while the stale candidate sat in the durable outbox.
PARENT_TIP = "00" * 32
MOVED_TIP = "77" * 32


class AccountingLaneRig:
    """Drive the shipped accounting loop over controllable lane traffic.

    Only the three sinks the loop reaches are replaced -- the accounting task
    runner, the invalid-candidate quarantine drain and the deferred-cleanup
    retry -- so the lane selection under test, the three queues, the handoff
    routing and the dispatch quota all stay exactly as shipped.  A real
    :class:`_BlockCandidateAccountingTask` is still run through the shipped
    ``_run_block_accounting_task``; only the synthetic placeholders are
    counted and dropped.

    The loop runs on the calling thread, which makes dispatch order a plain
    list rather than a race.  It is bounded twice over: the quarantine stub
    counts the loop's empty passes and stops it after a couple of them, and
    a watchdog timer sets the stop event if the loop somehow never reaches
    an empty pass.  A starved lane therefore ends the test with a report
    instead of an unbounded wait.
    """

    def __init__(
        self,
        *,
        quota: int | None = None,
        idle_passes_before_stop: int = 2,
        ledger: Any = None,
        rpc: Any = None,
    ) -> None:
        server, state, _recording = submit_coordinator()
        server.max_blocks = 1 << 30
        server.stop_after_block = False
        server.block_candidate_retry_initial_seconds = 0.0
        if quota is not None:
            server.block_accounting_accepted_dispatch_quota = int(quota)
        if ledger is not None:
            server.ledger = ledger
        if rpc is not None:
            server.rpc = rpc
        self.server = server
        self.state = state
        self.ledger = server.ledger
        self.rpc = server.rpc
        self.service = server._ensure_block_candidate_service()
        self.service._ensure_block_accounting_state()
        self.service._ensure_block_replay_state()
        self.service._ensure_block_candidate_disposition_state()
        # Dispatch order, one label per task the lane selected.
        self.dispatched: list[str] = []
        # Called after each dispatch so a scenario can hand off more work
        # mid-drain, which is what the submitter thread does in production.
        self.on_dispatch: Callable[[Any], None] | None = None
        self._shipped_run = self.service._run_block_accounting_task
        self._idle_passes = 0
        self._idle_ceiling = int(idle_passes_before_stop)
        self._labels = itertools.count()
        server._run_block_accounting_task = self._run_task
        server._run_one_invalid_block_candidate_quarantine = self._idle_pass
        self.service._run_one_collapsed_block_candidate_cleanup_retry = (
            self._no_cleanup
        )

    # -- the replaced sinks ------------------------------------------------

    def _run_task(self, task: Any) -> None:
        self.dispatched.append(self.label(task))
        if isinstance(task, _BlockCandidateAccountingTask):
            self._shipped_run(task)
        if self.on_dispatch is not None:
            self.on_dispatch(task)

    def _idle_pass(self) -> bool:
        self._idle_passes += 1
        if self._idle_passes >= self._idle_ceiling:
            self.server.stop_event.set()
        return False

    @staticmethod
    def _no_cleanup() -> bool:
        return False

    # -- traffic -----------------------------------------------------------

    @staticmethod
    def label(task: Any) -> str:
        return str(
            getattr(task, "label", None)
            or task.candidate.submission.block_hash_hex
        )

    def enqueue(
        self,
        label: str,
        *,
        accepted: bool,
        height: int = 10,
    ) -> Any:
        """Hand one synthetic task to the shipped routing.

        The task carries only what ``_enqueue_block_accounting_task`` reads:
        the template height it keys on, the hash its spill log names, and the
        node submission whose classification chooses the lane.
        """
        node_submission = (
            _BlockCandidateNodeSubmission(attempted=True)
            if accepted
            else _BlockCandidateNodeSubmission(attempted=False)
        )
        task = SimpleNamespace(
            label=label,
            candidate=SimpleNamespace(
                context=SimpleNamespace(template={"height": int(height)}),
                submission=SimpleNamespace(block_hash_hex=label),
            ),
            node_submission=node_submission,
        )
        # The spill log is part of the rule this scenario exercises; it is
        # captured rather than printed so the suite stays readable.
        with redirect_stdout(StringIO()):
            assert self.server._enqueue_block_accounting_task(task)
        return task

    def accepted_stream(self, *, until: int) -> Callable[[Any], None]:
        """Keep the accepted lane non-empty for the whole drain.

        Every accepted dispatch immediately hands off another accepted task,
        so the lane is never observed empty while stale work waits -- which
        is the only condition under which the fairness bound says anything.
        The stream stops after ``until`` total dispatches so the drain ends.
        """

        def rearm(task: Any) -> None:
            if len(self.dispatched) >= until:
                return
            if _is_definitive_node_acceptance(task.node_submission):
                self.enqueue(
                    f"accepted-{next(self._labels)}",
                    accepted=True,
                )

        return rearm

    # -- the drain ---------------------------------------------------------

    def drain(self, *, timeout: float = 15.0) -> list[str]:
        watchdog = threading.Timer(timeout, self.server.stop_event.set)
        watchdog.start()
        try:
            with redirect_stdout(StringIO()):
                self.service.block_accounting_loop()
        finally:
            watchdog.cancel()
        return list(self.dispatched)

    # -- queue inspection --------------------------------------------------

    @property
    def accepted_queue(self) -> Any:
        return self.service._block_accounting_accepted_queue

    @property
    def primary_queue(self) -> Any:
        return self.service._block_accounting_queue

    @property
    def overflow_queue(self) -> Any:
        return self.service._block_accounting_overflow_queue


def durable_candidate(
    rig: AccountingLaneRig,
    block_hash: str,
    *,
    tag: str = "aa",
) -> Any:
    """Seed one durable pending outbox row and return its candidate."""
    pending = PendingShare(
        share_id=f"miner-a:{block_hash}",
        miner_id="miner-a",
        order_key="miner-a",
        p2mr_program_hex="11" * 32,
        share_difficulty=1,
        network_difficulty=1,
        template_height=9,
        job_id="job-1",
        job_issued_at_ms=1,
        accepted_at_ms=1,
        ntime=1,
    )
    candidate = block_candidate(
        rig.server,
        rig.state,
        SimpleNamespace(
            coinbase_tx_hex="00",
            block_hash_hex=block_hash,
            block_hex=tag,
            share_pass=True,
            block_pass=True,
        ),
        pending_share=pending,
    )
    rig.ledger.append_batch(
        [(pending, rig.server.block_candidate_intent(candidate))]
    )
    return candidate


def durable_accounting_task(
    rig: AccountingLaneRig,
    candidate: Any,
) -> _BlockCandidateAccountingTask:
    """Queue the durable candidate the way the deferred handoff leaves it.

    ``attempted=False`` is the replay shape: the row was adopted from the
    outbox, so no offer has been made for it yet and it is not accepted
    evidence.  The disposition lease travels with the task until the
    accounting lane releases it.
    """
    block_hash = str(candidate.submission.block_hash_hex)
    lease = rig.server._claim_block_candidate_disposition(
        block_hash,
        blocking=False,
    )
    assert lease is not None
    task = _BlockCandidateAccountingTask(
        candidate=candidate,
        node_submission=_BlockCandidateNodeSubmission(attempted=False),
        disposition_lease=lease,
    )
    assert rig.server._enqueue_block_accounting_task(task)
    return task


class DefinitiveNodeAcceptanceTests(unittest.TestCase):
    """The discriminator, and the two sites that must never disagree."""

    def test_only_an_attempted_silent_offer_is_definitive_acceptance(
        self,
    ) -> None:
        self.assertTrue(
            _is_definitive_node_acceptance(
                _BlockCandidateNodeSubmission(attempted=True)
            )
        )
        # A transport failure builds attempted=True with error set, which
        # leaves result at its None default: a two-clause test that dropped
        # `error is None` would read this failed offer as an acceptance.
        self.assertFalse(
            _is_definitive_node_acceptance(
                _BlockCandidateNodeSubmission(
                    attempted=True,
                    error=RuntimeError("submitblock timed out"),
                )
            )
        )
        # A rejection string is a result, so it is not silent acceptance.
        self.assertFalse(
            _is_definitive_node_acceptance(
                _BlockCandidateNodeSubmission(attempted=True, result="duplicate")
            )
        )
        # No offer has been made at all.
        self.assertFalse(
            _is_definitive_node_acceptance(
                _BlockCandidateNodeSubmission(attempted=False)
            )
        )
        self.assertFalse(_is_definitive_node_acceptance(None))

    def test_the_handoff_routes_exactly_the_definitive_acceptances(
        self,
    ) -> None:
        rig = AccountingLaneRig()
        rig.enqueue("accepted", accepted=True)
        self.assertEqual(rig.accepted_queue.qsize(), 1)
        self.assertTrue(rig.primary_queue.empty())
        for label, node_submission in (
            ("errored", _BlockCandidateNodeSubmission(
                attempted=True,
                error=RuntimeError("boom"),
            )),
            ("duplicate", _BlockCandidateNodeSubmission(
                attempted=True,
                result="duplicate",
            )),
            ("unattempted", _BlockCandidateNodeSubmission(attempted=False)),
        ):
            task = SimpleNamespace(
                label=label,
                candidate=SimpleNamespace(
                    context=SimpleNamespace(template={"height": 10}),
                    submission=SimpleNamespace(block_hash_hex=label),
                ),
                node_submission=node_submission,
            )
            self.assertTrue(rig.server._enqueue_block_accounting_task(task))
        # None of the three joined the accepted lane.
        self.assertEqual(rig.accepted_queue.qsize(), 1)
        self.assertEqual(rig.primary_queue.qsize(), 3)

    def test_the_retained_offer_stash_shares_the_one_predicate(self) -> None:
        rig = AccountingLaneRig()
        service = rig.service

        def retained() -> dict[str, Any]:
            return dict(
                getattr(
                    service,
                    "_block_candidate_retained_node_submissions",
                    {},
                )
            )

        for block_hash, node_submission in (
            ("11" * 32, _BlockCandidateNodeSubmission(
                attempted=True,
                error=RuntimeError("boom"),
            )),
            ("22" * 32, _BlockCandidateNodeSubmission(
                attempted=True,
                result="duplicate",
            )),
            ("33" * 32, _BlockCandidateNodeSubmission(attempted=False)),
            ("44" * 32, None),
        ):
            service._stash_retained_block_candidate_node_submission(
                block_hash,
                node_submission,
            )
        self.assertEqual(retained(), {})
        definitive = _BlockCandidateNodeSubmission(attempted=True)
        service._stash_retained_block_candidate_node_submission(
            "55" * 32,
            definitive,
        )
        self.assertEqual(retained(), {"55" * 32: definitive})


class AcceptedAccountingPriorityTests(unittest.TestCase):
    """Dispatch order: the issue's counterexample and the burst it hides in."""

    def test_accepted_evidence_drains_before_queued_same_height_replay(
        self,
    ) -> None:
        """#181's BLOCKED integration review, now passing.

        Three durable same-height replay tasks are queued and a definitively
        accepted task is queued afterward.  The reported drain order was
        replay 1, replay 2, replay 3, accepted -- every one of those replay
        tasks costing ~6 RPCs, two ledger writes and a payout-balance
        mutation before the decided block's accounting could start.
        """
        rig = AccountingLaneRig()
        for index in (1, 2, 3):
            rig.enqueue(f"replay-{index}", accepted=False, height=10)
        rig.enqueue("accepted", accepted=True, height=10)
        self.assertEqual(
            rig.drain(),
            ["accepted", "replay-1", "replay-2", "replay-3"],
        )

    def test_accepted_evidence_overtakes_a_spilled_stale_backlog(
        self,
    ) -> None:
        """The burst case, which a priority key cannot reach.

        The primary queue is at its bound and spillover has begun before the
        accepted task is handed off.  A key on the primary heap is inert
        here: the accepted task would be *put* into the overflow queue by
        the unchanged spillover rule, and the loop drains the bounded
        primary in full before it looks at the overflow at all.  Ordering by
        lane rather than by key is what makes the accepted class
        structurally un-spillable.

        The accepted task is deliberately given the highest height and the
        last arrival sequence in the whole batch, so nothing about its key
        can explain it draining first.
        """
        rig = AccountingLaneRig()
        depth = DEFAULT_BLOCK_ACCOUNTING_QUEUE_DEPTH
        self.assertEqual(rig.primary_queue.maxsize, depth)
        for index in range(depth):
            rig.enqueue(f"stale-{index}", accepted=False, height=10)
        self.assertTrue(rig.primary_queue.full())
        # The burst state itself: this handoff finds the primary full and
        # spills, and the one after it joins the spill by the put rule.
        rig.enqueue("spilled-0", accepted=False, height=10)
        self.assertFalse(rig.overflow_queue.empty())
        rig.enqueue("spilled-1", accepted=False, height=10)
        self.assertEqual(rig.overflow_queue.qsize(), 2)
        rig.enqueue("accepted", accepted=True, height=99)
        self.assertEqual(rig.accepted_queue.qsize(), 1)

        order = rig.drain()
        self.assertEqual(order[0], "accepted")
        self.assertEqual(
            order,
            ["accepted"]
            + [f"stale-{index}" for index in range(depth)]
            + ["spilled-0", "spilled-1"],
        )

    def test_a_newer_stale_handoff_never_jumps_an_older_spill_entry(
        self,
    ) -> None:
        """The spillover put rule's anti-starvation property, unchanged.

        Membership is what this change moved, not the rule.  Once the
        overflow queue is non-empty, a later non-accepted handoff still
        joins it rather than refilling the bounded primary in front of the
        older spill entries -- even when the primary has since drained and
        has room again, which is exactly when refilling would starve them.
        """
        rig = AccountingLaneRig(quota=1)
        rig.server.block_accounting_queue_depth = 1
        del rig.service._block_accounting_queue
        rig.service._ensure_block_accounting_state()
        self.assertEqual(rig.primary_queue.maxsize, 1)

        rig.enqueue("primary", accepted=False, height=10)
        rig.enqueue("spilled-old", accepted=False, height=10)
        self.assertEqual(rig.overflow_queue.qsize(), 1)

        def hand_off_newer(task: Any) -> None:
            if AccountingLaneRig.label(task) != "primary":
                return
            # The primary queue has just been emptied by this dispatch, so
            # it has room; the put rule must still send the newer handoff
            # behind the older spill entry.
            self.assertTrue(rig.primary_queue.empty())
            rig.enqueue("newer", accepted=False, height=10)
            self.assertTrue(rig.primary_queue.empty())
            self.assertEqual(rig.overflow_queue.qsize(), 2)

        rig.on_dispatch = hand_off_newer
        self.assertEqual(rig.drain(), ["primary", "spilled-old", "newer"])


class AcceptedDispatchQuotaTests(unittest.TestCase):
    """Bounded fairness (D3): stale replay is delayed, never starved."""

    QUOTAS = (1, BLOCK_ACCOUNTING_ACCEPTED_DISPATCH_QUOTA)

    def test_the_default_quota_is_four(self) -> None:
        self.assertEqual(BLOCK_ACCOUNTING_ACCEPTED_DISPATCH_QUOTA, 4)

    def test_a_stale_task_waits_behind_at_most_the_quota(self) -> None:
        """The acceptance criterion, stated the way D3 states it.

        With a continuous stream of accepted tasks and at least one stale
        task pending, the stale task is dispatched after at most ``quota``
        accepted services -- never behind an unbounded stream of them,
        whatever the accepted arrival rate.
        """
        for quota in self.QUOTAS:
            with self.subTest(quota=quota):
                rig = AccountingLaneRig(quota=quota)
                rig.enqueue("stale", accepted=False, height=10)
                rig.on_dispatch = rig.accepted_stream(until=8 * quota)
                rig.enqueue("accepted-seed", accepted=True, height=10)

                order = rig.drain()
                self.assertIn("stale", order)
                waited_behind = order.index("stale")
                self.assertLessEqual(waited_behind, quota)
                # Every dispatch it waited behind was an accepted one, so
                # the count above really is the accepted-service bound.
                self.assertTrue(
                    all(
                        label.startswith("accepted")
                        for label in order[:waited_behind]
                    ),
                    order,
                )
                # And the quota is a bound on delay, not a ban on accepted
                # work: the stream kept running afterwards.
                self.assertGreater(len(order) - waited_behind, 1)

    def test_the_quota_only_yields_to_work_that_exists(self) -> None:
        """An exhausted quota with no stale work still dispatches accepted.

        The bound is defined against *available* non-accepted work.  With
        the stale lanes empty there is nothing to yield to, so an accepted
        backlog keeps draining instead of stalling behind a spent counter.
        """
        rig = AccountingLaneRig(quota=1)
        for index in range(5):
            rig.enqueue(f"accepted-{index}", accepted=True, height=10)
        self.assertEqual(
            rig.drain(),
            [f"accepted-{index}" for index in range(5)],
        )

    def test_a_delayed_stale_task_still_reaches_terminal_disposition(
        self,
    ) -> None:
        """Delay is the fix; starvation would be a different bug.

        A stale accounting task that never runs leaves its durable outbox
        row pending, its offer-time accepted-block payout-preview barrier
        armed and its disposition lease held.  The stale task here is a real
        one with a real pending outbox row whose parent tip has since moved,
        run through the shipped ``_run_block_accounting_task``, while the
        accepted lane is kept continuously non-empty around it.  Delayed by
        the quota, it still reaches its terminal abandonment.
        """
        for quota in self.QUOTAS:
            with self.subTest(quota=quota):
                block_hash = "e7" * 32
                rig = AccountingLaneRig(
                    quota=quota,
                    ledger=SingleWriterShareLedger(),
                    rpc=RejectingSubmitTipRpc(MOVED_TIP),
                )
                candidate = durable_candidate(rig, block_hash, tag="e7")
                self.assertEqual(
                    rig.ledger._block_candidate_outbox[block_hash]["state"],
                    "pending",
                )
                durable_accounting_task(rig, candidate)
                rig.on_dispatch = rig.accepted_stream(until=8 * quota)
                rig.enqueue("accepted-seed", accepted=True, height=10)

                order = rig.drain()
                self.assertIn(block_hash, order)
                self.assertLessEqual(order.index(block_hash), quota)
                self.assertEqual(
                    rig.ledger._block_candidate_outbox[block_hash]["state"],
                    "abandoned",
                )
                with rig.server.lock:
                    self.assertIn(
                        block_hash,
                        rig.service._block_candidate_terminal_outcomes,
                    )
                # The lease travelled with the task and the lane released it.
                with rig.service._block_candidate_disposition_registry_lock:
                    self.assertNotIn(
                        block_hash,
                        rig.service._block_candidate_disposition_flights,
                    )


class AcceptedLaneShapeTests(unittest.TestCase):
    """The lane's own registration and unboundedness."""

    def test_the_accepted_lane_is_unbounded(self) -> None:
        """A maxsize here would need a spill target, and any queue-state
        dependent spill is the inversion this lane removes.  The offer has
        already happened, so it may never be converted back into a
        raw-submit retry; max-block admission bounds how many unresolved
        real offers can exist at once.
        """
        rig = AccountingLaneRig()
        self.assertEqual(rig.accepted_queue.maxsize, 0)
        self.assertEqual(rig.overflow_queue.maxsize, 0)
        self.assertEqual(
            rig.primary_queue.maxsize,
            DEFAULT_BLOCK_ACCOUNTING_QUEUE_DEPTH,
        )
        for index in range(4 * DEFAULT_BLOCK_ACCOUNTING_QUEUE_DEPTH):
            rig.enqueue(f"accepted-{index}", accepted=True, height=10)
        self.assertEqual(
            rig.accepted_queue.qsize(),
            4 * DEFAULT_BLOCK_ACCOUNTING_QUEUE_DEPTH,
        )
        self.assertTrue(rig.primary_queue.empty())
        self.assertTrue(rig.overflow_queue.empty())

    def test_the_accepted_lane_is_reachable_through_the_coordinator(
        self,
    ) -> None:
        """The state-field registration, which only a delegate-backed
        coordinator exercises: the lane must be readable and writable under
        its historical coordinator attribute name like the other two.
        """
        rig = AccountingLaneRig()
        self.assertIs(
            rig.server._block_accounting_accepted_queue,
            rig.service._block_accounting_accepted_queue,
        )
        rig.server._block_accounting_accepted_queue.put_nowait(
            (10, 0, SimpleNamespace())
        )
        self.assertEqual(rig.service._block_accounting_accepted_queue.qsize(), 1)


if __name__ == "__main__":
    unittest.main()
