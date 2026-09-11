#!/usr/bin/env python3
"""False lease-heartbeat exits under normal tail latency (issue #212).

What could not be tested before
-------------------------------
``union-mainnet`` restarted 69 times during one rapid-block burst while
PostgreSQL, qbitd, the RAID array, CPU, memory and disk were all healthy and
the coordinator still held its PostgreSQL advisory guard.  The exits were
real protective exits: the heartbeat's server-proven envelope had genuinely
elapsed.  It had elapsed because the envelope was sized for an idle interval
and not for the tail of a lawful verification.

Reproducing that needs three things at once — a guard statement running near
its server-side timeout, the coordinator's own threads being descheduled
behind the burst, and a fenced write holding the lease tuple — and needs them
in a fixed order.  Running the coordinator and hoping produces the schedule
by accident at best.  This module puts the real
:class:`LedgerLeaseHeartbeatService` loops on the #128 deterministic harness:
its heartbeat and monitor become two harness actors, every interval they
measure reads the harness's virtual clock, and modelled server time is
charged per statement.  Nothing sleeps.

What is proved
--------------
``RapidBlockBurstTests``
    A production-shaped burst — near-bound guard statements, repeated
    scheduler stalls, an in-flight fenced write on the lease tuple, and
    external-effect fence callers competing for the guard's query slot —
    produces no hard exit, while the peak staleness it reaches is above
    what the pre-#212 policy tolerated.  The regression is anchored to the
    old bounds, so a future retune that reintroduces them fails here.

``ProofRenewalSplitTests``
    The frequent statement is the cheap ownership proof, renewal escalates
    on the same beat the committed row enters the authority margin, and
    neither ever queues on the lease tuple's row lock.

``FailClosedTests``
    The safety half.  A lost guard session, a wedged proof that never
    answers, and a guarded statement cancelled by its own statement timeout
    each terminate this coordinator's authority strictly before a successor
    becomes eligible to adopt — measured against the successor's real
    guard-acquisition edge, not against a constant.
"""

from __future__ import annotations

from typing import Callable
import unittest

from lab.prism.background_services import (
    LedgerLeaseHeartbeatPorts,
    LedgerLeaseHeartbeatService,
)
from lab.prism.writer_lease_timing import (
    DEFAULT_WRITER_LEASE_HEARTBEAT_POLICY,
    LEASE_HEARTBEAT_MODE_FENCE,
    WriterLeaseHeartbeatPolicy,
    WriterLeaseVerificationAttempt,
)
from tests.prism_concurrency_harness import (
    GUARD_STATEMENT_TIMEOUT_SECONDS,
    LeaseHarness,
)
from tests.test_prism_writer_lease_policy import PRE_212_POLICY

# The shipped policy, used verbatim: these scenarios exist to judge the
# defaults operators actually run, not a scaled-down stand-in.
POLICY = DEFAULT_WRITER_LEASE_HEARTBEAT_POLICY

# Guard statements run just inside their server-side ceiling — the tail the
# burst produced, not a pathological value. Anything above the ceiling is
# cancelled by PostgreSQL and is a different scenario (see FailClosedTests).
TAIL_STATEMENT_SECONDS = 0.45

# One descheduling of the coordinator's threads behind the burst. Well inside
# the policy's scheduler-slack allowance and far outside what the pre-#212
# envelope could absorb.
SCHEDULER_STALL_SECONDS = 0.35

# Virtual seconds of burst. Long enough to cross the own-write authority
# margin (half the 60s lease TTL) and prove the renewal escalation fires on
# its own, rather than only on the first beat.
BURST_SECONDS = 40.0

LEASE_TTL_SECONDS = 60.0

# A fenced write's caller deadline. Arming one is what makes the ledger wrap
# the statement in an explicit transaction whose COMMIT is a separate client
# message, which is how the lease tuple stays row-locked across the burst.
FENCED_WRITE_BUDGET_SECONDS = 600.0


class HeartbeatUnderTest:
    """The real heartbeat service, wired onto the deterministic harness.

    Substitution goes through ports, exactly as the harness does for the
    ledger: the loops, the fail-closed bookkeeping, the phase attribution
    and the monitor's arithmetic are the shipped implementations. Only the
    clock, the synchronisation primitives and the process-exit seam are
    supplied by the harness, so what a scenario observes is what production
    would do on the same schedule.
    """

    def __init__(
        self,
        harness: LeaseHarness,
        coordinator,
        *,
        policy: WriterLeaseHeartbeatPolicy = POLICY,
        name: str = "hb",
    ) -> None:
        self.harness = harness
        self.policy = policy
        self.exits: list[tuple[float, str, str | None]] = []
        service: LedgerLeaseHeartbeatService

        def lease_hard_exit(message: str, *, include_traceback: bool) -> None:
            # The coordinator's delegate, spelled the same way: the real
            # fail-closed bookkeeping (single-arming, exit-thread exemption,
            # failure reason) has to run for the assertions to mean anything.
            service.hard_exit(message, include_traceback=include_traceback)

        def watchdog_hard_exit(reason: str, *, timeout_seconds: float) -> None:
            # Stands in for os._exit: record when the process would have
            # gone, and stop both loops, because a dead process runs nothing.
            self.exits.append(
                (harness.clock.monotonic(), reason, service.failure_reason)
            )
            service.stop_event.set()

        ports = LedgerLeaseHeartbeatPorts(
            ledger=lambda: coordinator.ledger,
            heartbeat_seconds=lambda: policy.heartbeat_interval_seconds,
            failure_seconds=lambda: policy.failure_budget_seconds,
            monitor_seconds=lambda: policy.monitor_interval_seconds,
            exit_timeout_seconds=lambda: policy.exit_margin_seconds,
            scheduler_slack_seconds=lambda: policy.scheduler_slack_seconds,
            external_fence_timeout_seconds=lambda: 0.5,
            lease_hard_exit=lease_hard_exit,
            watchdog_hard_exit=watchdog_hard_exit,
            heartbeat_loop=lambda: service.heartbeat_loop(),
            monitor_loop=lambda: service.monitor_loop(),
            monotonic=harness.clock.monotonic,
        )
        service = LedgerLeaseHeartbeatService(ports)
        # The freshness and failure locks stay real: the service never
        # yields while holding either, and under the baton scheduler no two
        # actors run at once, so neither can be contended. The stop/ready/
        # failed events do have to be scheduler-visible, because the loops
        # wait on them with a timeout and that wait is what makes the
        # heartbeat interval and the monitor poll observable.
        service.stop_event = harness.event(f"{name}.stop")
        service.ready = harness.event(f"{name}.ready")
        service.failed = harness.event(f"{name}.failed")
        armed = harness.clock.monotonic()
        service.last_success_monotonic = armed
        service.last_server_proven_monotonic = armed
        self.service = service
        self.armed_monotonic = armed
        self.heartbeat = harness.scheduler.actor(f"{name}-heartbeat")
        self.monitor = harness.scheduler.actor(f"{name}-monitor")
        self.heartbeat.submit(service.heartbeat_loop, label="heartbeat-loop")
        self.monitor.submit(service.monitor_loop, label="monitor-loop")
        self.peak_server_proven_age = 0.0
        self.peak_activity_age = 0.0

    # -- observation -------------------------------------------------------

    @property
    def exited(self) -> bool:
        return bool(self.exits)

    @property
    def exit_monotonic(self) -> float:
        if not self.exits:
            raise AssertionError("the heartbeat did not hard-exit")
        return self.exits[0][0]

    @property
    def exit_reason(self) -> str:
        if not self.exits:
            raise AssertionError("the heartbeat did not hard-exit")
        return self.exits[0][2] or ""

    def observe(self) -> None:
        """Sample staleness the way the monitor sees it, for peak reporting."""
        self.peak_server_proven_age = max(
            self.peak_server_proven_age,
            self.service.server_proven_age_seconds(),
        )
        self.peak_activity_age = max(
            self.peak_activity_age,
            self.service.activity_age_seconds(),
        )

    # -- driving -----------------------------------------------------------

    def fence_call(self, ledger) -> Callable[[], object]:
        """The external-effect fence's guard call, minus its worker thread.

        ``require_fresh_lease_for_external_side_effect`` runs the
        verification on a daemon thread the baton scheduler cannot drive,
        so a scenario calls the same shipped verification with the same two
        callbacks wired the same way. What is under test through this seam
        is the guard's single query slot and the marks a fence leaves
        behind, both of which are identical either way — and leaving the
        marks out would model a guard caller that does not exist.
        """
        attempt = WriterLeaseVerificationAttempt(
            LEASE_HEARTBEAT_MODE_FENCE,
            monotonic=self.harness.clock.monotonic,
        )

        def run() -> object:
            return ledger.verify_writer_lease_guard_session(
                on_query_start=attempt.slot_acquired,
                on_statement_end=lambda: self.service.record_server_proven(
                    attempt.statement_completed()
                ),
            )

        return run

    def advance_until_beat(self, others: list) -> None:
        """Jump forward until the heartbeat's next beat is due.

        Other actors keep their own turns as their timers come up, so the
        heartbeat is not privileged by the fast-forward.
        """
        while not self.heartbeat.runnable():
            if self.harness.advance_to_next_deadline() is None:
                raise AssertionError("nothing bounds the heartbeat's idle wait")
            for actor in others:
                while actor.runnable():
                    self.harness.step(actor)

    def run_monitor_maximally_late(
        self,
        *,
        rounds: int,
        first_poll_monotonic: float | None = None,
    ) -> None:
        """Drive only the monitor, each poll as late as the policy allows.

        The worst schedule the safety inequality claims to survive: the
        heartbeat thread gets no turn at all, and the monitor — the thread
        that has to notice and exit — wakes ``monitor_interval +
        scheduler_slack`` after its previous poll every single time.

        ``first_poll_monotonic`` pins the phase of that grid. Lateness
        alone is not the worst case: a poll that happens to land just after
        a bound elapses notices immediately, and the scenario then proves
        nothing about the term being tested. Placing one poll immediately
        *before* the bound forces the next one to arrive a full late
        interval after it, which is the case the envelope has to cover.
        """
        late_by = (
            self.policy.monitor_interval_seconds
            + self.policy.scheduler_slack_seconds
        )
        if first_poll_monotonic is not None:
            self.harness.clock.advance_to(
                max(first_poll_monotonic, self.harness.clock.monotonic())
            )
            while self.monitor.runnable():
                self.harness.step(self.monitor)
            self.observe()
        for _ in range(rounds):
            if self.exited:
                return
            self.harness.clock.advance(late_by)
            while self.monitor.runnable():
                self.harness.step(self.monitor)
            self.observe()

    def stall_heartbeat(self, seconds: float) -> None:
        """Deschedule the coordinator's heartbeat thread for ``seconds``.

        Only the monitor runs, in its own poll steps, which is what a GIL
        stall behind a rapid-block burst looks like from the monitor's side:
        time passes, no new activity mark appears, and the decision to
        hard-exit or not is taken on the policy alone.
        """
        target = self.harness.clock.monotonic() + seconds
        while self.harness.clock.monotonic() < target and not self.exited:
            self.harness.clock.advance_to(
                min(
                    target,
                    self.harness.clock.monotonic()
                    + self.policy.monitor_interval_seconds,
                )
            )
            while self.monitor.runnable():
                self.harness.step(self.monitor)
            self.observe()

    def run(
        self,
        actors: list,
        *,
        seconds: float,
        stall_every: int = 0,
        stall_seconds: float = SCHEDULER_STALL_SECONDS,
        limit: int = 8000,
    ) -> None:
        """Drain every runnable actor, then jump to the next pending timer."""
        started = self.harness.clock.monotonic()
        rounds = 0
        while (
            self.harness.clock.monotonic() - started < seconds
            and rounds < limit
            and not self.exited
        ):
            self.harness.drain(actors)
            self.observe()
            if stall_every and rounds % stall_every == stall_every - 1:
                self.stall_heartbeat(stall_seconds)
            if self.harness.advance_to_next_deadline() is None:
                break
            rounds += 1

    def quiesce(self, actors: list) -> None:
        self.service.stop_event.set()
        self.harness.drain(actors)


def guard_statement_latency(seconds: float):
    """Charge ``seconds`` of modelled server time to each guarded statement."""

    def latency(statement) -> float:
        return seconds if statement.kind.value in {"prove", "verify"} else 0.0

    return latency


def statement_counts(harness: LeaseHarness) -> dict[str, int]:
    kinds = harness.statement_kinds()
    return {kind: kinds.count(kind) for kind in set(kinds)}


class RapidBlockBurstTests(unittest.TestCase):
    def test_burst_with_tail_latency_and_stalls_never_hard_exits(self) -> None:
        """The regression, on the schedule that produced it.

        Guard statements run just under their server-side ceiling, the
        coordinator's threads are descheduled repeatedly, a fenced write
        holds the lease tuple for the whole burst, and external-effect
        fences compete for the guard's single query slot. Nothing here is
        a fault; every restart the burst produced was a healthy
        coordinator being told its own session was gone.
        """
        with LeaseHarness(
            lease_ttl_seconds=LEASE_TTL_SECONDS,
            capture_output=True,
        ) as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "done:startup")
            heartbeat = HeartbeatUnderTest(harness, alpha)
            harness.server.statement_latency_seconds = guard_statement_latency(
                TAIL_STATEMENT_SECONDS
            )

            # A long fenced write parked mid-statement: PostgreSQL holds the
            # lease tuple's row lock and the COMMIT has not been sent. This
            # is the shape persist_accepted_block leaves behind.
            def fenced_write() -> object:
                with alpha.ledger.operation_timeout(FENCED_WRITE_BUDGET_SECONDS):
                    return alpha.ledger.renew_writer_lease()

            alpha.submit(fenced_write, label="fenced-write")
            harness.run_until(alpha, "alpha.done:renew")

            fence = harness.scheduler.actor("external-fence")
            actors = [heartbeat.heartbeat, heartbeat.monitor, fence]
            heartbeat.run(
                actors,
                seconds=BURST_SECONDS,
                stall_every=7,
            )
            heartbeat.quiesce(actors)

            self.assertFalse(
                heartbeat.exited,
                f"healthy coordinator hard-exited: {heartbeat.exits}",
            )
            # The burst really did produce the staleness the issue reported:
            # both pre-#212 bounds are crossed by a coordinator that never
            # stopped holding its guard.
            self.assertGreaterEqual(
                heartbeat.peak_server_proven_age,
                PRE_212_POLICY.server_proven_cap_seconds,
            )
            self.assertLess(
                heartbeat.peak_server_proven_age,
                POLICY.server_proven_cap_seconds,
            )
            self.assertLess(
                heartbeat.peak_activity_age,
                POLICY.failure_budget_seconds,
            )
            # The lease tuple was locked throughout, and no guarded
            # statement ever queued for it.
            self.assertIsNotNone(harness.server.lease_lock_holder)
            self.assertEqual(
                [stop for stop in harness.trace if "guard" in stop and "lockwait" in stop],
                [],
            )

    def test_monitor_lateness_is_attributed_and_survived(self) -> None:
        """A stalled monitor thread spends envelope like a slow statement.

        The monitor decides the exit, so its own lateness has to be
        measurable separately from database latency — otherwise an operator
        reading an exit cannot tell "PostgreSQL was slow" from "this
        process could not get scheduled".
        """
        with LeaseHarness(lease_ttl_seconds=LEASE_TTL_SECONDS) as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "done:startup")
            heartbeat = HeartbeatUnderTest(harness, alpha)
            actors = [heartbeat.heartbeat, heartbeat.monitor]
            heartbeat.run(actors, seconds=1.0)

            # Deschedule the monitor alone: the heartbeat keeps proving.
            late_by = 0.4
            harness.advance(heartbeat.policy.monitor_interval_seconds + late_by)
            while heartbeat.monitor.runnable():
                harness.step(heartbeat.monitor)
            heartbeat.run(actors, seconds=1.0)
            heartbeat.quiesce(actors)

            self.assertFalse(heartbeat.exited)
            self.assertGreaterEqual(
                heartbeat.service.monitor_wake_delay_seconds,
                late_by,
            )
            snapshot = heartbeat.service.snapshot()
            self.assertGreaterEqual(
                float(snapshot["monitor_wake_delay_seconds"]),
                late_by,
            )


class ProofRenewalSplitTests(unittest.TestCase):
    def test_beats_prove_ownership_and_escalate_only_when_renewal_is_due(
        self,
    ) -> None:
        """Frequent ownership proof, infrequent TTL renewal — same guarantees.

        The first beat renews, because startup readiness must prove the
        renewing path works. After that every beat asks the cheap ownership
        question until the committed row falls inside the own-write
        authority margin (half the 60s TTL), at which point renewal
        escalates on that same beat.
        """
        with LeaseHarness(lease_ttl_seconds=LEASE_TTL_SECONDS) as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "done:startup")
            heartbeat = HeartbeatUnderTest(harness, alpha)
            harness.server.statement_latency_seconds = guard_statement_latency(
                TAIL_STATEMENT_SECONDS
            )
            actors = [heartbeat.heartbeat, heartbeat.monitor]
            heartbeat.run(actors, seconds=BURST_SECONDS)
            heartbeat.quiesce(actors)

            counts = statement_counts(harness)
            self.assertFalse(heartbeat.exited)
            # Proof dominates by a wide margin: renewal runs on the first
            # beat and once more when half the TTL has eroded.
            self.assertGreater(counts["prove"], 20)
            self.assertEqual(counts["verify"], 2)
            self.assertGreater(counts["prove"], 10 * counts["verify"])

            attempts = heartbeat.service.attempt_counts
            outcomes = heartbeat.service.outcome_counts
            self.assertEqual(attempts["renew"], counts["verify"])
            self.assertEqual(attempts["proof"], counts["prove"])
            # Exactly one proof reported the row renewal-due; that beat
            # escalated immediately rather than waiting for the next one.
            self.assertEqual(outcomes["renewal_due"], 1)
            self.assertEqual(outcomes["renewed"], counts["verify"])
            self.assertEqual(outcomes["failed"], 0)

    def test_proof_never_queues_behind_an_in_flight_fenced_write(self) -> None:
        """The block-39416 property, preserved across the split.

        The proof takes no row lock at all — it asks two ``EXISTS``
        questions — so a fenced write holding the lease tuple for the whole
        of its transaction cannot make a beat queue and die on the guard's
        statement timeout.
        """
        with LeaseHarness(lease_ttl_seconds=LEASE_TTL_SECONDS) as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "done:startup")

            def fenced_write() -> object:
                with alpha.ledger.operation_timeout(FENCED_WRITE_BUDGET_SECONDS):
                    return alpha.ledger.renew_writer_lease()

            alpha.submit(fenced_write, label="fenced-write")
            harness.run_until(alpha, "alpha.done:renew")
            self.assertIsNotNone(harness.server.lease_lock_holder)

            heartbeat = HeartbeatUnderTest(harness, alpha)
            actors = [heartbeat.heartbeat, heartbeat.monitor]
            heartbeat.run(actors, seconds=2.0)
            heartbeat.quiesce(actors)

            self.assertFalse(heartbeat.exited)
            self.assertGreater(statement_counts(harness)["prove"], 1)
            self.assertEqual(
                [stop for stop in harness.trace if "lockwait" in stop],
                [],
            )

    def test_guard_slot_contention_is_attributed_to_queue_wait(self) -> None:
        """A beat that queued behind a fence must not look like slow SQL.

        The guard session is one connection with one query slot, so an
        external-effect fence and the heartbeat serialize. Charging that
        wait to the database would send an operator to PostgreSQL for a
        problem that is in this process.
        """
        with LeaseHarness(lease_ttl_seconds=LEASE_TTL_SECONDS) as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "done:startup")
            heartbeat = HeartbeatUnderTest(harness, alpha)
            harness.server.statement_latency_seconds = guard_statement_latency(
                TAIL_STATEMENT_SECONDS
            )
            actors = [heartbeat.heartbeat, heartbeat.monitor]
            heartbeat.run(actors, seconds=1.0)

            # Park a fence caller inside the guard's slot, then let the
            # heartbeat's next beat arrive behind it.
            fence = harness.scheduler.actor("external-fence")
            fence.submit(
                heartbeat.fence_call(alpha.ledger),
                label="submitblock-fence",
            )
            harness.run_until(fence, "alpha.guard.begin:verify")
            heartbeat.advance_until_beat([heartbeat.monitor])
            harness.run_until_blocked(
                heartbeat.heartbeat,
                "alpha.guard.slot:wait",
            )
            # Only now does the fence's statement spend its server time, so
            # every second of it is queue wait for the heartbeat.
            harness.drain([fence] + actors)
            heartbeat.quiesce(actors + [fence])

            self.assertFalse(heartbeat.exited)
            queued = [
                phases
                for phases in (heartbeat.service.worst_phases,)
                if phases.slot_wait_seconds > 0.0
            ]
            self.assertTrue(
                queued,
                "a beat queued behind the fence but recorded no slot wait",
            )
            self.assertGreaterEqual(
                heartbeat.service.worst_phases.slot_wait_seconds,
                TAIL_STATEMENT_SECONDS,
            )


class FailClosedTests(unittest.TestCase):
    """Losing authority must still be fatal, and fatal early enough."""

    def takeover_deadline(self, harness: LeaseHarness, successor) -> float:
        """Earliest virtual instant the successor may compare-and-swap.

        Measured from the instant the successor actually took the writer
        advisory guard, because that is the edge
        ``_writer_lease_adoption_wait_seconds`` counts its silence from —
        deliberately, so a predecessor that just lost the guard always gets
        its whole self-fencing budget however stale its lease row looks.
        """
        successor.start()
        return self.step_until_guard_held(harness, successor)

    def step_until_guard_held(self, harness: LeaseHarness, successor) -> float:
        for _ in range(50):
            if successor.guard is not None and successor.guard.held:
                return harness.clock.monotonic() + POLICY.adoption_silence_seconds
            if not successor.actor.runnable():
                break
            harness.step(successor.actor)
        raise AssertionError("the successor never acquired the writer guard")

    def assert_guard_withheld(self, harness: LeaseHarness, successor) -> None:
        """A successor cannot even start its silence while we hold the guard.

        This is the first fence, ahead of any timing argument: the advisory
        lock is a session lock, so a predecessor that is wedged — rather
        than dead — keeps it, and the successor's adoption clock has not
        begun.
        """
        successor.start()
        for _ in range(20):
            if not successor.actor.runnable():
                break
            harness.step(successor.actor)
        self.assertFalse(
            successor.guard is not None and successor.guard.held,
            "a successor took the writer guard while the predecessor held it",
        )

    def test_lost_guard_session_exits_before_takeover_eligibility(self) -> None:
        """An injected guard-session loss fences the old coordinator first."""
        with LeaseHarness(lease_ttl_seconds=LEASE_TTL_SECONDS) as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "done:startup")
            heartbeat = HeartbeatUnderTest(harness, alpha)
            harness.server.statement_latency_seconds = guard_statement_latency(
                TAIL_STATEMENT_SECONDS
            )
            actors = [heartbeat.heartbeat, heartbeat.monitor]
            heartbeat.run(actors, seconds=1.0)
            self.assertFalse(heartbeat.exited)

            # The guard connection drops: the advisory lock is released
            # server-side and this coordinator no longer owns anything.
            assert alpha.guard is not None
            alpha.guard.close()
            lost_at = harness.clock.monotonic()

            beta = harness.coordinator("beta")
            deadline = self.takeover_deadline(harness, beta)
            heartbeat.run(
                actors + [beta.actor],
                seconds=POLICY.adoption_silence_seconds,
            )

            self.assertTrue(heartbeat.exited, "a lost guard did not fence")
            self.assertLess(heartbeat.exit_monotonic, deadline)
            self.assertLess(
                heartbeat.exit_monotonic - lost_at,
                POLICY.adoption_silence_seconds,
            )
            self.assertIn("ledger lease heartbeat failed", heartbeat.exit_reason)
            # The exit names the phase breakdown of the attempt that failed.
            self.assertIn("mode=proof", heartbeat.exit_reason)
            self.assertIn("outcome=failed", heartbeat.exit_reason)
            self.assertEqual(heartbeat.service.outcome_counts["failed"], 1)

    def test_wedged_proof_exits_before_takeover_eligibility(self) -> None:
        """A statement that never answers still terminates authority.

        A wedged beat stamps nothing at all after its attempt start, so the
        liveness budget is the bound that fires first and the server-proven
        edge — which no client-side mark can move — is stale behind it. The
        exit has to land inside the adoption silence, and the reason has to
        say which of the two bounds decided it.
        """
        with LeaseHarness(lease_ttl_seconds=LEASE_TTL_SECONDS) as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "done:startup")
            heartbeat = HeartbeatUnderTest(harness, alpha)
            actors = [heartbeat.heartbeat, heartbeat.monitor]
            heartbeat.run(actors, seconds=1.0)
            wedged_at = harness.clock.monotonic()

            def wedge(statement) -> float:
                if statement.kind.value in {"prove", "verify"}:
                    # No answer, no error: the beat is abandoned mid
                    # statement exactly as a black-holed session leaves it.
                    harness.scheduler.park_forever("alpha.guard.wedged")
                return 0.0

            harness.server.statement_latency_seconds = wedge

            beta = harness.coordinator("beta")
            # A wedged coordinator still holds the session advisory lock, so
            # the successor cannot even begin its silence window yet.
            self.assert_guard_withheld(harness, beta)
            heartbeat.run(
                actors + [beta.actor],
                seconds=POLICY.adoption_silence_seconds,
            )

            self.assertTrue(heartbeat.exited, "a wedged proof did not fence")
            self.assertLess(
                heartbeat.exit_monotonic - wedged_at,
                POLICY.adoption_silence_seconds,
            )
            # The process is gone, which is what finally drops the guard.
            # Only then does the successor's full silence window start, so
            # the old coordinator's authority ended strictly first.
            assert alpha.guard is not None
            alpha.guard.close()
            deadline = self.step_until_guard_held(harness, beta)
            self.assertLess(heartbeat.exit_monotonic, deadline)
            reason = heartbeat.exit_reason
            self.assertIn("stopped making progress", reason)
            # The server-proven edge is the bound that decides a wedge: it
            # is a whole idle interval staler than the client-side activity
            # mark the abandoned beat left behind, so it reaches its cap
            # first. That ordering is the point of measuring the adoption
            # envelope from completed round trips.
            self.assertIn("Tripped: server-proven", reason)
            self.assertIn("adoption envelope cap", reason)
            self.assertGreater(
                heartbeat.service.server_proven_age_seconds(),
                heartbeat.service.activity_age_seconds(),
            )
            # Every term of the policy that produced the decision is in the
            # exit, so an operator never has to guess which bound tripped.
            self.assertIn("Last attempt:", reason)
            self.assertIn("Monitor wake delay:", reason)
            self.assertIn("server_proven_cap=", reason)

    def test_statement_cancelled_by_its_own_timeout_fails_closed(self) -> None:
        """A guarded statement PostgreSQL cancels is loss, not latency.

        The guard session's statement_timeout is the ceiling the whole
        envelope is sized against. A statement that reaches it produces an
        error, and an error on the guarded session is loss of the guarded
        session — never something to retry into the next beat.
        """
        with LeaseHarness(lease_ttl_seconds=LEASE_TTL_SECONDS) as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "done:startup")
            heartbeat = HeartbeatUnderTest(harness, alpha)
            actors = [heartbeat.heartbeat, heartbeat.monitor]
            heartbeat.run(actors, seconds=1.0)
            self.assertFalse(heartbeat.exited)

            harness.server.statement_latency_seconds = guard_statement_latency(
                GUARD_STATEMENT_TIMEOUT_SECONDS * 2
            )
            beta = harness.coordinator("beta")
            self.assert_guard_withheld(harness, beta)
            heartbeat.run(
                actors + [beta.actor],
                seconds=POLICY.adoption_silence_seconds,
            )

            self.assertTrue(heartbeat.exited)
            self.assertIn("ledger lease heartbeat failed", heartbeat.exit_reason)
            assert alpha.guard is not None
            alpha.guard.close()
            self.assertLess(
                heartbeat.exit_monotonic,
                self.step_until_guard_held(harness, beta),
            )

    def assert_exit_precedes_eligibility(
        self,
        harness: LeaseHarness,
        heartbeat: HeartbeatUnderTest,
        alpha,
        successor,
        *,
        authority_lost_monotonic: float,
    ) -> None:
        """The whole safety claim, measured rather than assumed.

        Three things have to hold together: the old coordinator exits, it
        exits within one adoption silence of losing authority, and the
        successor's own eligibility edge — its real guard acquisition plus
        the silence — is strictly later than that exit.
        """
        self.assertTrue(heartbeat.exited, "authority was lost without an exit")
        # The recorded instant is when the monitor *decided*; the process is
        # gone at most one exit margin later, and that is what has to beat
        # the adoption edge.
        self.assertLess(
            heartbeat.exit_monotonic
            + POLICY.exit_margin_seconds
            - authority_lost_monotonic,
            POLICY.adoption_silence_seconds,
        )
        if alpha.guard is not None and alpha.guard.held:
            # The process is gone; that is what finally drops the session
            # advisory lock and lets any successor start its silence.
            alpha.guard.close()
        eligible_at = self.step_until_guard_held(harness, successor)
        self.assertLess(
            heartbeat.exit_monotonic + POLICY.exit_margin_seconds,
            eligible_at,
        )

    def test_wedged_proof_and_maximally_late_monitor_still_exit_in_time(
        self,
    ) -> None:
        """The combined worst case the exit envelope is sized for.

        Authority is gone (the guard session answers nothing), the thread
        that would notice by raising is descheduled entirely, and the
        monitor — the only thread left that can act — wakes as late as the
        policy's scheduler slack permits on every poll. This is the
        schedule that made the first cut of the envelope unsafe: reserving
        only the poll intervals, a maximally late monitor observed the
        stale edge after the successor's adoption edge.
        """
        with LeaseHarness(lease_ttl_seconds=LEASE_TTL_SECONDS) as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "done:startup")
            heartbeat = HeartbeatUnderTest(harness, alpha)
            # Beats run their statements at the guard's server-side ceiling.
            # That maximises the gap between the newest completed round trip
            # and the client-side marks the next beat leaves before it
            # wedges, which is exactly when the envelope cap — not the
            # liveness budget — is the bound that has to carry the
            # guarantee.
            harness.server.statement_latency_seconds = guard_statement_latency(
                GUARD_STATEMENT_TIMEOUT_SECONDS
            )
            actors = [heartbeat.heartbeat, heartbeat.monitor]
            heartbeat.run(actors, seconds=2.0)
            self.assertFalse(heartbeat.exited)

            # The last instant this coordinator's authority was provable.
            # The guarded session cannot have died before it, so a
            # successor's guard acquisition — and therefore its silence
            # window — cannot start earlier either. Measuring from here is
            # conservative in the direction that matters.
            proven_at = float(heartbeat.service.last_server_proven_monotonic)

            def wedge(statement) -> float:
                if statement.kind.value in {"prove", "verify"}:
                    harness.scheduler.park_forever("alpha.guard.wedged")
                return 0.0

            harness.server.statement_latency_seconds = wedge
            # Let one beat reach the wedge — leaving its attempt-start and
            # slot-acquisition marks behind, which is what makes the
            # client-side liveness clock look healthier than the session
            # is — then abandon the heartbeat thread: from here only the
            # monitor can save the deployment.
            heartbeat.advance_until_beat([heartbeat.monitor])
            while heartbeat.heartbeat.runnable():
                harness.step(heartbeat.heartbeat)

            beta = harness.coordinator("beta")
            self.assert_guard_withheld(harness, beta)
            # Worst phase: one poll lands a hair before the cap elapses, so
            # the poll that can actually notice is a full late interval
            # after it.
            heartbeat.run_monitor_maximally_late(
                rounds=200,
                first_poll_monotonic=(
                    proven_at + POLICY.server_proven_cap_seconds - 0.001
                ),
            )

            # A precondition, not a bound: the point is that the monitor
            # really was at least as late as the policy budgets for, so a
            # lower bound is what this checks.
            self.assertGreaterEqual(
                heartbeat.service.monitor_wake_delay_seconds,
                POLICY.scheduler_slack_seconds - 1e-6,
                "the monitor was not actually driven at maximum lateness",
            )
            self.assertTrue(heartbeat.exited, "a wedged proof did not fence")
            # The whole safety claim, at the worst schedule the policy
            # tolerates: gone before the silence window — which starts no
            # earlier than the last provable instant — can elapse. Asserted
            # before anything about wording, so a regression in the envelope
            # reports the breach rather than a changed message.
            self.assertLess(
                heartbeat.exit_monotonic
                + POLICY.exit_margin_seconds
                - proven_at,
                POLICY.adoption_silence_seconds,
                "the old coordinator was still exiting after a successor "
                "became eligible to adopt",
            )
            # The envelope cap is the bound that fired, not the liveness
            # budget the fresh client marks were still inside.
            self.assertIn("Tripped: server-proven", heartbeat.exit_reason)
            self.assert_exit_precedes_eligibility(
                harness,
                heartbeat,
                alpha,
                beta,
                authority_lost_monotonic=proven_at,
            )

    def test_lost_guard_with_stalled_heartbeat_and_late_monitor_exits_in_time(
        self,
    ) -> None:
        """The same worst case, entered through guard-session loss.

        The advisory lock is genuinely gone, so this coordinator has no
        authority at all — and its heartbeat thread, which would normally
        raise on the next beat, never gets scheduled again. The monitor
        has to carry the exit alone, maximally late, and still finish
        before the successor that can now take the guard becomes eligible.
        """
        with LeaseHarness(lease_ttl_seconds=LEASE_TTL_SECONDS) as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "done:startup")
            heartbeat = HeartbeatUnderTest(harness, alpha)
            actors = [heartbeat.heartbeat, heartbeat.monitor]
            heartbeat.run(actors, seconds=1.0)
            self.assertFalse(heartbeat.exited)

            assert alpha.guard is not None
            alpha.guard.close()
            authority_lost = harness.clock.monotonic()

            # The successor may take the guard immediately — the silence
            # window it must then wait out is the only thing standing
            # between it and the lease row.
            beta = harness.coordinator("beta")
            eligible_at = self.takeover_deadline(harness, beta)

            # From here the heartbeat thread never runs again.
            heartbeat.run_monitor_maximally_late(rounds=200)

            self.assertTrue(heartbeat.exited, "a lost guard did not fence")
            self.assertGreaterEqual(
                heartbeat.service.monitor_wake_delay_seconds,
                POLICY.scheduler_slack_seconds - 1e-6,
            )
            self.assertLess(
                heartbeat.exit_monotonic
                + POLICY.exit_margin_seconds
                - authority_lost,
                POLICY.adoption_silence_seconds,
            )
            self.assertLess(
                heartbeat.exit_monotonic + POLICY.exit_margin_seconds,
                eligible_at,
            )

    def test_unsafe_policy_refuses_to_arm_the_heartbeat(self) -> None:
        """Startup rejection, on the real service.

        A policy whose failure budget cannot fit inside the adoption
        silence alongside the exit envelope is a double-writer hazard. The
        service must refuse to start rather than run with a broken
        exit-before-adoption argument.
        """
        with LeaseHarness(lease_ttl_seconds=LEASE_TTL_SECONDS) as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "done:startup")
            unsafe = WriterLeaseHeartbeatPolicy(
                adoption_silence_seconds=POLICY.adoption_silence_seconds,
                heartbeat_interval_seconds=0.25,
                failure_budget_seconds=POLICY.adoption_silence_seconds,
                monitor_interval_seconds=0.05,
                exit_margin_seconds=0.1,
            )
            heartbeat = HeartbeatUnderTest(harness, alpha, policy=unsafe)
            starter = harness.scheduler.actor("serve")
            started = starter.submit(heartbeat.service.start, label="start")
            harness.run_until(starter, "done:start")

            self.assertIsNone(started.value())
            self.assertTrue(heartbeat.exited)
            self.assertIn(
                "refusing to start the ledger lease heartbeat",
                heartbeat.exit_reason,
            )
            self.assertIn("adoption silence", heartbeat.exit_reason)
            # Nothing was armed: no guarded statement ran.
            self.assertNotIn("prove", statement_counts(harness))


if __name__ == "__main__":
    unittest.main()
