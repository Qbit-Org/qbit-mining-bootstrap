#!/usr/bin/env python3
"""Issue #123: an orphaned lease-tuple lock blocking writer failover.

#123 describes a coordinator that vanishes mid-statement — a network
partition, a SIGSTOP, a VM pause — after PostgreSQL has finished executing a
deadline-scoped landing statement but before the client's COMMIT arrives. The
server keeps the transaction open and keeps the ``qbit_ledger_writer_lease``
tuple lock. Before the fix nothing bounded how long it held them:
``idle_in_transaction_session_timeout`` was set nowhere, server-side TCP
keepalives were configured nowhere, and the startup lease upsert ran with no
``lock_timeout``, so a successor coordinator waited inside
``PsqlShareLedger.__init__`` until TCP keepalive teardown — hours at OS
defaults.

The issue was written as a hypothesis with a hand-derived trigger sequence
because no test could express it: reproducing the interleaving meant a live
PostgreSQL, a partition, and luck. This module drives it deterministically
against the harness instead, with the successor running the shipped
acquisition path — the real retry loop, the real CAS SQL, the real
``_run_json`` transaction shape.

Two tests, doing two different jobs:

``OrphanedLeaseLockInterleavingTests``
    Proves the harness still expresses the interleaving: the schedule is
    stable across repeated runs, the orphan really does hold the tuple lock,
    and the successor really does park on it. That evidence is what #128
    asked for, and the fix does not remove any of it — the successor still
    queues behind an orphan it cannot clear. What changed is that the wait is
    now bounded, so the tests pin the bound instead of its absence.

``OrphanedLeaseLockFailoverBoundTests``
    Asserts the behaviour #123's fix must produce: a successor's startup
    finishes, one way or the other, within a bounded time.

    That assertion was authored against the fixed behaviour while 2.x.x was
    still unfixed, and carried an ``expectedFailure`` marker so the suite
    would go red the day it started passing. It now passes, so the marker is
    gone — removed by the commit that fixed #123, exactly as this file
    demanded. The assertion itself is unchanged and is the strongest
    evidence the fix works: it was written by someone who had not seen the
    fix, against a model of PostgreSQL that knows nothing about it.
"""

from __future__ import annotations

import unittest

from lab.prism.share_ledger import (
    DEFAULT_LEASE_ACQUIRE_ATTEMPTS,
    DEFAULT_LEASE_ACQUIRE_LOCK_TIMEOUT_SECONDS,
    LedgerOperationTimeout,
)
from tests.prism_concurrency_harness import (
    LeaseHarness,
    assert_deterministic,
)

# How long the vanished client's landing statement was budgeted for. #120's
# escalated landing budget tops out at 120s; the exact value does not matter
# to the interleaving, only that a deadline is armed, because that is what
# makes _NativePostgresClient wrap the statement in an explicit transaction
# whose COMMIT is a separate client message.
LANDING_BUDGET_SECONDS = 120.0

# Well past any plausible watchdog, restart or retry interval, and still far
# short of the "on the order of hours" TCP keepalive teardown #123 describes.
FAILOVER_HORIZON_SECONDS = 6 * 60 * 60

# PsqlShareLedger's floor between acquisition attempts (_lease_retry_sleep is
# called with _lease_retry_min_sleep_seconds, which is min(0.25, max sleep)).
# Not a tunable of this fix; named so the elapsed-time assertion below can be
# exact rather than approximate.
RETRY_SLEEP_SECONDS = 0.25


def drive_orphaned_lock_interleaving(harness: LeaseHarness) -> dict[str, object]:
    """Run #123's trigger sequence and report what the successor sees.

    1. ``alpha`` starts and takes the writer lease.
    2. ``alpha`` runs a deadline-scoped fenced statement, which PostgreSQL
       executes inside an explicit transaction holding the lease tuple lock.
    3. ``alpha`` vanishes in the gap between the statement completing and its
       COMMIT — no RST reaches the server, so the transaction stays open.
    4. ``beta``, a failover twin at the next writer epoch, starts up. Its
       advisory-guard key differs from ``alpha``'s, so it is not held off
       there and reaches the lease upsert, which is where #123 bites.
    """
    alpha = harness.coordinator("alpha", writer_epoch=1)
    alpha.start()
    harness.run_until(alpha, "done:startup")

    def fenced_landing_statement() -> object:
        # Any deadline-scoped fenced write has this shape; renewal is the
        # smallest one that touches the lease tuple, and it is real shipped
        # code rather than a stand-in.
        with alpha.ledger.operation_timeout(LANDING_BUDGET_SECONDS):
            return alpha.ledger.renew_writer_lease()

    alpha.submit(fenced_landing_statement, label="landing")
    # Stop in the gap PostgreSQL is left in: statement executed, COMMIT not
    # sent. This is the exact instant #123 names.
    harness.run_until(alpha, "alpha.precommit")
    alpha.vanish()

    orphan = harness.server.lease_lock_holder

    beta = harness.coordinator("beta", writer_epoch=2)
    beta.start()
    harness.run_until_blocked(beta, "beta.lockwait:acquire")

    return {
        "orphan_open": orphan is not None and orphan.orphaned,
        "orphan_holds_lease_lock": harness.server.lease_lock_holder is orphan,
        "beta_blocked_at": beta.actor.block_reason,
        "beta_parked_at": harness.clock.monotonic(),
        "beta_wake_at": beta.actor.wake_at,
        "scheduler_next_deadline": harness.scheduler.next_deadline(),
        "lease_holder_session": harness.lease_holder_session(),
        "statements": harness.statement_kinds(),
    }


def drive_successor_to_quiescence(
    harness: LeaseHarness,
    successor: object,
    *,
    horizon_seconds: float = FAILOVER_HORIZON_SECONDS,
) -> bool:
    """Run the successor until it finishes or nothing can move it again.

    Draining once is not enough to decide "did the wait end". A fix built
    from ``lock_timeout`` plus a bounded retry — which is what #123 proposes
    — parks the successor on a succession of short deadlines, and a single
    ``advance`` followed by a single ``drain`` would leave it stopped at the
    first of them and report a wait that never ended. Advancing to each
    pending deadline in turn asks the question the assertion actually means:
    is there any sequence of timeouts that gets this coordinator out?

    Returns True when startup finished, by acquiring or by raising. Returns
    False when the successor is stuck with no deadline left to reach, which
    is the state ``2.x.x`` produces.
    """
    startup = successor.actor.calls[0]  # type: ignore[attr-defined]
    deadline = harness.clock.monotonic() + horizon_seconds
    while not startup.done and harness.clock.monotonic() < deadline:
        harness.drain([successor])
        if startup.done:
            break
        if harness.advance_to_next_deadline() is None:
            # Nothing in the system is waiting on time, so no amount of
            # further waiting changes anything.
            break
    return startup.done


class OrphanedLeaseLockInterleavingTests(unittest.TestCase):
    """The harness can express and reproduce #123's interleaving."""

    def test_successor_parks_on_the_orphaned_lease_tuple_lock(self) -> None:
        with LeaseHarness() as harness:
            observed = drive_orphaned_lock_interleaving(harness)

            self.assertTrue(
                observed["orphan_open"],
                "the vanished client's transaction must still be open server-side",
            )
            self.assertTrue(
                observed["orphan_holds_lease_lock"],
                "the orphaned transaction must still hold the lease tuple lock",
            )
            self.assertEqual(observed["beta_blocked_at"], "beta.lockwait:acquire")
            # The interleaving #123 names is intact: the orphan holds the
            # tuple lock and the successor is parked on it. What the fix
            # changes is the only thing that was ever wrong with it — the
            # wait now ends. _run_lease_acquisition_json arms
            # lock_timeout/statement_timeout on every attempt, so the
            # successor parks with a wake time rather than forever.
            self.assertEqual(
                observed["beta_wake_at"],
                observed["beta_parked_at"]
                + DEFAULT_LEASE_ACQUIRE_LOCK_TIMEOUT_SECONDS,
                "the successor must park under the configured acquisition "
                "lock deadline, not indefinitely",
            )
            self.assertEqual(
                observed["scheduler_next_deadline"],
                observed["beta_wake_at"],
                "and that deadline must be the thing the whole system is "
                "waiting on: nothing else is pending, so it is what releases "
                "the successor",
            )
            # The predecessor's renewal never committed, so the committed row
            # still names its session: the successor is queued behind a write
            # whose effect will never land.
            self.assertEqual(
                observed["lease_holder_session"],
                "heartbeat-v1:alpha-1",
            )
            self.assertEqual(
                observed["statements"],
                ["acquire", "renew", "acquire"],
            )

    def test_wait_ends_in_bounded_attempts_and_a_visible_failure(self) -> None:
        """How the wait ends, now that it ends.

        This test used to assert ``finished is False``: six virtual hours
        passed and the successor was still parked, because nothing bounded
        the wait. The fix inverts that premise outright, and simply flipping
        the assertion would duplicate
        ``OrphanedLeaseLockFailoverBoundTests``, which already pins *that*
        the wait ends.

        So it pins *how* it ends instead, which nothing else covers: the
        successor spends exactly its configured retry budget on the lock,
        each attempt bounded by the configured deadline, and then fails
        visibly with a ``RuntimeError`` that chains the underlying
        ``LedgerOperationTimeout``. Retrying matters because an orphan is
        often reaped partway through the budget; giving up visibly matters
        because a coordinator that cannot take the lease must exit for its
        supervisor rather than hang in ``__init__``. The evidence the old
        assertion carried — that nothing releases the orphan's lock, so the
        successor never actually starts — is kept below.
        """
        with LeaseHarness() as harness:
            drive_orphaned_lock_interleaving(harness)
            beta = harness.coordinators["beta"]
            before = harness.clock.monotonic()

            finished = drive_successor_to_quiescence(harness, beta)

            self.assertTrue(finished, "the bounded retry must terminate")
            self.assertFalse(
                beta.started,
                "and it must not have started: the orphan still holds the "
                "lease lock, so there was never a lease to take",
            )
            # The orphan is untouched — this is still #123's interleaving,
            # not a scenario that quietly resolved itself.
            orphan = harness.server.lease_lock_holder
            self.assertIsNotNone(orphan)
            assert orphan is not None
            self.assertTrue(orphan.orphaned)

            # Exactly the configured budget of attempts, no more and no
            # fewer: one attempt would abandon orphans that clear on their
            # own, and an unbounded count is the outage this fix removes.
            self.assertEqual(
                harness.statement_kinds().count("acquire"),
                1 + DEFAULT_LEASE_ACQUIRE_ATTEMPTS,
                "alpha's successful acquire plus beta's full retry budget",
            )
            # Every attempt ran the deadline out, so the wall time is the
            # budget itself rather than an accident of scheduling.
            self.assertEqual(
                harness.clock.monotonic() - before,
                DEFAULT_LEASE_ACQUIRE_ATTEMPTS
                * DEFAULT_LEASE_ACQUIRE_LOCK_TIMEOUT_SECONDS
                + (DEFAULT_LEASE_ACQUIRE_ATTEMPTS - 1) * RETRY_SLEEP_SECONDS,
            )

            with self.assertRaisesRegex(
                RuntimeError,
                "did not complete within its .*deadline on any of "
                f"{DEFAULT_LEASE_ACQUIRE_ATTEMPTS} attempts",
            ) as raised:
                beta.actor.calls[0].value()
            # Chained, not swallowed: the operator diagnoses from the cause
            # rather than from this layer's guess at one.
            self.assertIsInstance(
                raised.exception.__cause__,
                LedgerOperationTimeout,
            )

    def test_interleaving_is_order_stable_across_repeated_runs(self) -> None:
        """A flaky concurrency test is worse than none; prove this one is not."""
        run = assert_deterministic(
            self,
            drive_orphaned_lock_interleaving,
            repeats=25,
        )

        self.assertEqual(
            list(run.trace),
            [
                "alpha@alpha.begin:acquire",
                "alpha@alpha.done:acquire",
                # New since the fix: the startup acquire is deadline-scoped,
                # so it runs in an explicit transaction with its own COMMIT.
                # docs/prism-ledger-ops.md records what that costs.
                "alpha@alpha.precommit",
                "alpha@done:startup",
                "alpha@alpha.begin:renew",
                "alpha@alpha.done:renew",
                "alpha@alpha.precommit",
                "alpha@vanished",
                "beta@beta.begin:acquire",
                "beta@beta.lockwait:acquire",
            ],
        )

    def test_committing_the_predecessor_releases_the_successor(self) -> None:
        """Control: the successor is blocked by the lock, not by the harness.

        If the vanished client's transaction commits after all, the successor
        wakes on the next step and its upsert proceeds. That is what makes the
        parked state above attributable to the orphaned lock rather than to a
        scenario that simply never scheduled the successor.
        """
        with LeaseHarness() as harness:
            drive_orphaned_lock_interleaving(harness)
            beta = harness.coordinators["beta"]
            orphan = harness.server.lease_lock_holder
            assert orphan is not None

            harness.server._commit(orphan)

            self.assertTrue(beta.actor.runnable())
            harness.run_until(beta, "done:startup")

            # Once the lock is gone the twin gets to read the row and reaches
            # its decision immediately. A different epoch cannot wait on
            # another identity's unexpired lease, so it refuses loudly. That
            # is the shape #123's fix should produce within a bounded time:
            # the twin has no useful work to do behind that lock, and every
            # second it spends queued for it is pure outage.
            with self.assertRaisesRegex(
                RuntimeError,
                "qbit ledger writer lease is held by prism-coordinator epoch=1",
            ):
                beta.actor.calls[0].value()
            self.assertEqual(
                harness.lease_holder_session(),
                "heartbeat-v1:alpha-1",
            )


class OrphanedLeaseLockFailoverBoundTests(unittest.TestCase):
    """The bound #123's fix must establish. Does not hold until #123 is fixed."""

    def test_successor_startup_must_not_wait_unboundedly(self) -> None:
        """A successor must fail fast and visibly, not queue for hours.

        #123's proposed fix runs the startup lease upsert with
        ``SET LOCAL lock_timeout`` and a bounded retry. Under that fix the
        successor's acquisition ends within a bounded time — by acquiring, or
        by raising something an operator can see — rather than waiting on a
        lock nobody will ever release.

        The successor is driven through every deadline it reaches rather than
        drained once, so a fix whose bounded wait spans several short
        timeouts satisfies this as readily as one that fails on the first.
        Asserting anything narrower would leave the person who lands #123
        looking at a red test their correct fix did not turn green, and
        reaching for the same weakening this test exists to refuse.

        This assertion was deliberately written against the fixed behaviour
        rather than against the behaviour of the day. It carried an
        ``expectedFailure`` marker until #123 was fixed; the marker is gone
        because the bound now holds, which is the removal this docstring
        asked the fixing commit to make. The assertion is otherwise
        untouched.
        """
        with LeaseHarness() as harness:
            drive_orphaned_lock_interleaving(harness)
            beta = harness.coordinators["beta"]

            finished = drive_successor_to_quiescence(harness, beta)

            self.assertTrue(
                finished,
                "the successor's PsqlShareLedger.__init__ is still waiting on "
                f"the orphaned lease-tuple lock after "
                f"{FAILOVER_HORIZON_SECONDS / 3600:g} virtual hours, with no "
                "deadline left anywhere that could release it. #123 has "
                "regressed: the startup lease upsert is reaching the tuple "
                "lock without a lock_timeout again, so nothing bounds the "
                "wait.",
            )


if __name__ == "__main__":
    unittest.main()
