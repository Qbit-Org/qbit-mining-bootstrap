#!/usr/bin/env python3
"""Issue #123: an orphaned lease-tuple lock blocking writer failover.

#123 describes a coordinator that vanishes mid-statement — a network
partition, a SIGSTOP, a VM pause — after PostgreSQL has finished executing a
deadline-scoped landing statement but before the client's COMMIT arrives. The
server keeps the transaction open, keeps the ``qbit_ledger_writer_lease``
tuple lock, and nothing in this repository bounds how long it holds them:
``idle_in_transaction_session_timeout`` is set nowhere, server-side TCP
keepalives are configured nowhere, and the startup lease upsert runs with no
``lock_timeout``. A successor coordinator then waits inside
``PsqlShareLedger.__init__`` until TCP keepalive teardown, which with default
settings is hours.

The issue was written as a hypothesis with a hand-derived trigger sequence
because no test could express it: reproducing the interleaving meant a live
PostgreSQL, a partition, and luck. This module drives it deterministically
against the harness instead, with the successor running the shipped
acquisition path — the real retry loop, the real CAS SQL, the real
``_run_json`` transaction shape.

Two tests, doing two different jobs:

``OrphanedLeaseLockInterleavingTests``
    Proves the harness expresses the interleaving: the schedule is stable
    across repeated runs, the orphan really does hold the tuple lock, and the
    successor really does park on it with nothing that can ever wake it.
    These pass today and are the evidence #128 asked for.

``OrphanedLeaseLockFailoverBoundTests``
    Asserts the behaviour #123's fix must produce: a successor's startup
    finishes, one way or the other, within a bounded time. **This test fails
    on 2.x.x, because #123 is not fixed on 2.x.x.** It is written against the
    fix rather than against today's behaviour deliberately; weakening it to
    green would delete the only executable statement of what #123 asks for.
    It turns green when the successor's lease acquisition gains a bounded
    lock wait.
"""

from __future__ import annotations

import unittest

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
        "beta_wake_at": beta.actor.wake_at,
        "scheduler_next_deadline": harness.scheduler.next_deadline(),
        "lease_holder_session": harness.lease_holder_session(),
        "statements": harness.statement_kinds(),
    }


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
            # The successor's startup upsert runs with no caller deadline, so
            # _NativePostgresClient sets neither statement_timeout nor
            # lock_timeout. There is therefore no time at which the wait ends
            # on its own.
            self.assertIsNone(
                observed["beta_wake_at"],
                "the successor's lease upsert must be waiting with no deadline "
                "for this to be #123 rather than a bounded retry",
            )
            self.assertIsNone(
                observed["scheduler_next_deadline"],
                "nothing anywhere in the system bounds the wait",
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

    def test_wait_does_not_end_within_a_failover_horizon(self) -> None:
        """Six virtual hours pass and the successor is still not runnable."""
        with LeaseHarness() as harness:
            drive_orphaned_lock_interleaving(harness)
            beta = harness.coordinators["beta"]

            harness.advance(FAILOVER_HORIZON_SECONDS)
            harness.drain([beta])

            self.assertFalse(
                beta.actor.runnable(),
                "the successor is still parked on the orphaned tuple lock",
            )
            self.assertFalse(
                beta.started,
                "the successor never finished PsqlShareLedger.__init__",
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
    """The bound #123's fix must establish. Fails until #123 is fixed."""

    def test_successor_startup_must_not_wait_unboundedly(self) -> None:
        """A successor must fail fast and visibly, not queue for hours.

        #123's proposed fix runs the startup lease upsert with
        ``SET LOCAL lock_timeout`` and a bounded retry. Under that fix the
        successor's acquisition ends within a bounded time — by acquiring, or
        by raising something an operator can see — rather than waiting on a
        lock nobody will ever release.

        This assertion is deliberately written against the fixed behaviour.
        On 2.x.x it fails, and that failure is the point: it is the first
        executable statement of what #123 asks for.
        """
        with LeaseHarness() as harness:
            drive_orphaned_lock_interleaving(harness)
            beta = harness.coordinators["beta"]

            harness.advance(FAILOVER_HORIZON_SECONDS)
            harness.drain([beta])

            startup = beta.actor.calls[0]
            self.assertTrue(
                startup.done,
                "the successor's PsqlShareLedger.__init__ is still waiting on "
                f"the orphaned lease-tuple lock after "
                f"{FAILOVER_HORIZON_SECONDS / 3600:g} virtual hours. #123 is "
                "unfixed on this line: the startup lease upsert runs with no "
                "lock_timeout, so nothing bounds the wait.",
            )


if __name__ == "__main__":
    unittest.main()
