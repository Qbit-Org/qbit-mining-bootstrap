#!/usr/bin/env python3
"""The lease heartbeat self-fencing shape: a heartbeat queued behind our own write.

Every fenced write this coordinator performs — a share append,
``persist_accepted_block`` — row-locks the singleton ``qbit_ledger_writer_lease``
tuple for the whole of its transaction. The pre-fix heartbeat renewed the lease
with a plain ``UPDATE``, which queues for that tuple. On the guard connection,
which carries a server-side ``statement_timeout``, queueing is fatal: the
heartbeat's own statement is cancelled, the coordinator reads that as loss of
its guarded session, and a perfectly healthy writer hard-exits because it was
briefly busy writing. The coordinator fenced itself out with its own success.

The fix made the heartbeat's renewal non-blocking —
``FOR NO KEY UPDATE SKIP LOCKED`` — and proved liveness by reads instead: the
session answers, the guard advisory lock is still held by this backend, and
the last committed lease row still names this exact session. A skipped renewal
is harmless while that committed row is unexpired, because the fenced write
that skipped it refreshes the TTL when it commits.

Nothing could interleave a real heartbeat with a real in-flight fenced write
before the deterministic harness existed: the fenced write has to be parked
*inside* its statement, after PostgreSQL has taken the tuple lock and before
the client's COMMIT, and no amount of running the coordinator reliably lands
there. This module parks it there on purpose and drives the shipped
``verify_writer_lease_guard_session`` across the gap.

Four things are proved, and one of them is the reason the others mean anything:

``HeartbeatUnderFencedWriteTests``
    The heartbeat survives an in-flight fenced write, skipping renewal
    without ever queueing, and renewal resumes the moment that write commits.
    ``test_the_queueing_renewal_spelling_dies_where_the_heartbeat_survives``
    is the control: it runs the *pre-fix* spelling into the same lock and
    watches it die on the guard's statement timeout. Without that control,
    ``renewed_count == 0`` would be equally consistent with a heartbeat that
    never tried.

``OwnWriteRenewalDeferralTests``
    The one lock-blocked-and-expired case that must not fail closed: this
    process's own fenced write outlasting the TTL. The locker is attributable
    through ``pg_stat_activity`` to this process's pool ``application_name``,
    so the verification defers rather than self-fencing — and the same
    expired row locked by anyone else still fails closed.

``GuardQuerySlotTests``
    The guard connection is one session, so its callers serialize on one
    query slot rather than pipelining onto the same backend.
"""

from __future__ import annotations

import threading
import unittest

from lab.prism import share_ledger as share_ledger_module
from tests.prism_concurrency_harness import (
    LeaseHarness,
    LockTimeout,
    assert_deterministic,
)

# Lease TTL for every scenario here. The own-write authority margin defaults
# to half the TTL (``_resolve_lease_authority_margin_seconds``), so a freshly
# acquired row at 60s is 30s clear of the margin: a renewal skipped over it
# defers nothing, which is what makes scenario 1's `renewal_deferred_to_own_write`
# assertion distinguishable from scenario 4's.
LEASE_TTL_SECONDS = 60.0

# The caller deadline the fenced write runs under. A slow
# ``persist_accepted_block`` is the real shape; the value only has to be a
# deadline, because arming one is what makes _NativePostgresClient wrap the
# statement in an explicit transaction whose COMMIT is a separate client
# message — the gap this module parks in. Chosen well beyond every clock
# advance below so the write's own budget never explains what we observe.
FENCED_WRITE_BUDGET_SECONDS = 600.0

# Past LEASE_TTL_SECONDS, so the committed row the heartbeat reads has lapsed
# while the fenced write that would have refreshed it is still in flight.
TTL_LAPSE_SECONDS = 70.0

# Three passes, because one is not a heartbeat. The point is that the guarded
# session keeps answering for the whole span of the fenced write, not that it
# answered once.
HEARTBEAT_PASSES = 3


def drive_heartbeat_under_fenced_write(harness: LeaseHarness) -> dict[str, object]:
    """Park a fenced write mid-statement and heartbeat across it.

    1. ``alpha`` starts and takes the writer lease.
    2. ``alpha`` begins a deadline-scoped fenced statement. Stopping at
       ``alpha.done:renew`` leaves PostgreSQL exactly where a long fenced
       write leaves it: the statement has executed and holds the lease tuple
       lock, and the COMMIT has not been sent.
    3. A second actor runs the shipped heartbeat verification on ``alpha``'s
       guard connection, repeatedly. It has to be a second actor: ``alpha``
       is parked inside its own statement and also holds the ledger's writer
       lock, so a heartbeat submitted to ``alpha`` could never run — which is
       precisely the production shape, where the heartbeat is a separate
       thread from whatever is writing.
    """
    alpha = harness.coordinator("alpha")
    alpha.start()
    harness.run_until(alpha, "done:startup")

    def fenced_write() -> object:
        # Renewal is the smallest fenced write that touches the lease tuple,
        # and it is shipped code rather than a stand-in. Any fenced write has
        # this shape: deadline armed, tuple locked, COMMIT still to come.
        with alpha.ledger.operation_timeout(FENCED_WRITE_BUDGET_SECONDS):
            return alpha.ledger.renew_writer_lease()

    alpha.submit(fenced_write, label="fenced-write")
    harness.run_until(alpha, "alpha.done:renew")

    heartbeat = harness.scheduler.actor("heartbeat")
    verifications = []
    for pass_number in range(1, HEARTBEAT_PASSES + 1):
        label = f"heartbeat-{pass_number}"
        call = heartbeat.submit(
            alpha.ledger.verify_writer_lease_guard_session,
            label=label,
        )
        harness.run_until(heartbeat, f"done:{label}")
        verifications.append(call.value())

    return {
        "verifications": verifications,
        "fenced_write_still_holds_lease_lock": (
            harness.server.lease_lock_holder is not None
        ),
        "fenced_write_finished": alpha.actor.calls[-1].done,
        "lockwait_stops": [stop for stop in harness.trace if "lockwait" in stop],
        "statements": harness.statement_kinds(),
        "lease_holder_session": harness.lease_holder_session(),
    }


def drive_own_write_outlasting_the_ttl(harness: LeaseHarness) -> dict[str, object]:
    """Let the TTL lapse under this coordinator's own in-flight fenced write.

    Same park as above, then the virtual clock runs past the committed row's
    ``lease_expires_at``. The renewal that would have refreshed it is the very
    write holding the lock, so the heartbeat now reads a row that is expired,
    locked, and unrenewable — the shape that must fail closed when the locker
    is a competing claimant, and must not when it is us.
    """
    alpha = harness.coordinator("alpha")
    alpha.start()
    harness.run_until(alpha, "done:startup")

    def fenced_write() -> object:
        with alpha.ledger.operation_timeout(FENCED_WRITE_BUDGET_SECONDS):
            return alpha.ledger.renew_writer_lease()

    alpha.submit(fenced_write, label="fenced-write")
    harness.run_until(alpha, "alpha.done:renew")

    harness.advance(TTL_LAPSE_SECONDS)

    heartbeat = harness.scheduler.actor("heartbeat")
    call = heartbeat.submit(
        alpha.ledger.verify_writer_lease_guard_session,
        label="heartbeat",
    )
    harness.run_until(heartbeat, "done:heartbeat")

    row = harness.lease_row()
    return {
        "verification": call.value(),
        "committed_row_expired": (
            row is not None and row.lease_expires_at <= harness.clock.now()
        ),
        "committed_row_locked": row is not None and row.xmax is not None,
        "lockwait_stops": [stop for stop in harness.trace if "lockwait" in stop],
        "statements": harness.statement_kinds(),
    }


def drive_foreign_locker_over_the_expired_row(
    harness: LeaseHarness,
) -> dict[str, object]:
    """The same expired-and-lock-blocked read, with the lock held by someone else.

    ``beta`` is a different-epoch claimant, so it has a different advisory
    guard key and is not held off by ``alpha``'s guard. Parking it at
    ``beta.done:acquire`` stops it in the gap its expiry CAS opens: it holds
    the tuple lock and has staged a takeover that has not committed, so the
    committed row ``alpha``'s heartbeat reads still names ``alpha`` and is
    still expired. That stale read is exactly what the queueing renewal used
    to catch by waiting, and what the non-blocking spelling has to catch by
    failing closed.
    """
    alpha = harness.coordinator("alpha", writer_epoch=1)
    alpha.start()
    harness.run_until(alpha, "done:startup")

    harness.advance(TTL_LAPSE_SECONDS)

    beta = harness.coordinator("beta", writer_epoch=2)
    beta.start()
    harness.run_until(beta, "beta.done:acquire")

    heartbeat = harness.scheduler.actor("heartbeat")
    call = heartbeat.submit(
        alpha.ledger.verify_writer_lease_guard_session,
        label="heartbeat",
    )
    harness.run_until(heartbeat, "done:heartbeat")

    row = harness.lease_row()
    return {
        "error": None if call.error is None else str(call.error),
        "error_type": None if call.error is None else type(call.error).__name__,
        "committed_row_session": None if row is None else row.writer_session_token,
        "committed_row_expired": (
            row is not None and row.lease_expires_at <= harness.clock.now()
        ),
        "lockwait_stops": [stop for stop in harness.trace if "lockwait" in stop],
        "statements": harness.statement_kinds(),
    }


class HeartbeatUnderFencedWriteTests(unittest.TestCase):
    """A heartbeat crossing an in-flight fenced write, and the control that dates it."""

    def test_heartbeat_survives_the_whole_fenced_write_without_queueing(self) -> None:
        """Skipped, never queued, never raised — for every pass of the write.

        ``renewed_count == 0`` on its own is only half the claim. The other
        half is ``lockwait_stops``: the harness gives a statement that queues
        for the lease tuple a named checkpoint, so an empty list is positive
        evidence that the verification declined the lock rather than waiting
        on it and getting lucky. ``verified_count == 1`` alongside it says the
        session was proven live by reads while the renewal was skipped, which
        is the whole substance of the fix.
        """
        with LeaseHarness(lease_ttl_seconds=LEASE_TTL_SECONDS) as harness:
            observed = drive_heartbeat_under_fenced_write(harness)

            self.assertTrue(
                observed["fenced_write_still_holds_lease_lock"],
                "the fenced write must still be holding the lease tuple lock, "
                "or the heartbeat had nothing to survive",
            )
            self.assertFalse(observed["fenced_write_finished"])
            self.assertEqual(len(observed["verifications"]), HEARTBEAT_PASSES)
            for pass_number, result in enumerate(observed["verifications"], start=1):
                with self.subTest(heartbeat_pass=pass_number):
                    self.assertEqual(result["verified_count"], 1)
                    self.assertEqual(result["renewed_count"], 0)
                    # The committed row is a full TTL fresh and the margin is
                    # half a TTL, so this skip is over a row with plenty of
                    # runway: liveness *and* authority, no deferral.
                    self.assertFalse(result["renewal_deferred_to_own_write"])
            self.assertEqual(
                observed["lockwait_stops"],
                [],
                "a verification that stopped at a lockwait checkpoint queued "
                "for the lease tuple, which is the self-fencing bug itself",
            )
            self.assertEqual(
                observed["statements"],
                ["acquire", "renew", "verify", "verify", "verify"],
                "each heartbeat must be one non-blocking statement; a second "
                "statement would mean the attribution recheck fired, and this "
                "row is not ambiguous",
            )

    def test_renewal_resumes_once_the_fenced_write_commits(self) -> None:
        """The skip is deferral to the write, not a heartbeat that stopped renewing.

        The fenced write refreshes the TTL itself when it commits, so the
        renewal the heartbeat skipped was never lost. Proving that the very
        next verification renews is what separates "declined the lock" from
        "silently gave up on renewing".
        """
        with LeaseHarness(lease_ttl_seconds=LEASE_TTL_SECONDS) as harness:
            drive_heartbeat_under_fenced_write(harness)
            alpha = harness.coordinators["alpha"]
            heartbeat = harness.scheduler.actors["heartbeat"]

            harness.run_until(alpha, "alpha.precommit")
            harness.run_until(alpha, "done:fenced-write")
            self.assertIsNone(
                harness.server.lease_lock_holder,
                "the committed fenced write must have released the tuple lock",
            )

            call = heartbeat.submit(
                alpha.ledger.verify_writer_lease_guard_session,
                label="heartbeat-after-commit",
            )
            harness.run_until(heartbeat, "done:heartbeat-after-commit")

            self.assertEqual(
                call.value(),
                {
                    "backend": "postgres-psql",
                    "verified_count": 1,
                    "renewed_count": 1,
                    "renewal_deferred_to_own_write": False,
                },
            )

    def test_the_queueing_renewal_spelling_dies_where_the_heartbeat_survives(
        self,
    ) -> None:
        """Control: the pre-fix renewal, run into the same lock, is cancelled.

        ``_renew_writer_lease_with`` is the plain locking ``UPDATE`` the
        heartbeat used to run. Handed the guard's ``run_json`` it becomes the
        pre-fix heartbeat exactly, and the guard session's server-side
        ``statement_timeout`` is what turns its queue into a hard failure —
        which the coordinator reads as loss of its guarded session.

        This test is what makes ``renewed_count == 0`` above mean "declined a
        lock that was really there" rather than "nothing was attempted".
        """
        with LeaseHarness(lease_ttl_seconds=LEASE_TTL_SECONDS) as harness:
            drive_heartbeat_under_fenced_write(harness)
            alpha = harness.coordinators["alpha"]
            heartbeat = harness.scheduler.actors["heartbeat"]
            guard = alpha.guard
            assert guard is not None

            call = heartbeat.submit(
                lambda: alpha.ledger._renew_writer_lease_with(guard.run_json),
                label="queueing-renewal",
            )
            # The pre-fix spelling does what the fixed one refuses to: it
            # queues on the tuple the fenced write holds.
            harness.run_until_blocked(heartbeat, "alpha.guard.lockwait:renew")

            self.assertIsNotNone(
                heartbeat.wake_at,
                "the guard session's statement timeout must bound this wait; "
                "an unbounded one would be a different defect",
            )
            self.assertIsNotNone(
                harness.advance_to_next_deadline(),
                "the guard statement timeout is the only pending deadline",
            )
            harness.run_until(heartbeat, "done:queueing-renewal")

            # Specifically the guard connection's server-side
            # statement_timeout, not a caller-armed lock_timeout: the
            # heartbeat runs on a session whose bound it cannot widen, which
            # is why queueing there is fatal rather than merely slow.
            with self.assertRaisesRegex(
                LockTimeout,
                "canceling statement due to statement timeout",
            ):
                call.value()
            self.assertTrue(
                harness.server.lease_lock_holder is not None,
                "the fenced write still holds the lock it queued for; the "
                "renewal died, the write did not",
            )

    def test_interleaving_is_order_stable_across_repeated_runs(self) -> None:
        """A flaky concurrency test is worse than none; prove this one is not."""
        run = assert_deterministic(
            self,
            drive_heartbeat_under_fenced_write,
            repeats=25,
            lease_ttl_seconds=LEASE_TTL_SECONDS,
        )

        self.assertEqual(
            list(run.trace),
            [
                "alpha@alpha.begin:acquire",
                "alpha@alpha.done:acquire",
                "alpha@done:startup",
                "alpha@alpha.begin:renew",
                "alpha@alpha.done:renew",
                "heartbeat@alpha.guard.begin:verify",
                "heartbeat@alpha.guard.snapshot:verify",
                "heartbeat@alpha.guard.done:verify",
                "heartbeat@done:heartbeat-1",
                "heartbeat@alpha.guard.begin:verify",
                "heartbeat@alpha.guard.snapshot:verify",
                "heartbeat@alpha.guard.done:verify",
                "heartbeat@done:heartbeat-2",
                "heartbeat@alpha.guard.begin:verify",
                "heartbeat@alpha.guard.snapshot:verify",
                "heartbeat@alpha.guard.done:verify",
                "heartbeat@done:heartbeat-3",
            ],
        )


class OwnWriteRenewalDeferralTests(unittest.TestCase):
    """Expired, lock-blocked, and ours: defer. Expired and someone else's: fail closed."""

    def test_own_write_outlasting_the_ttl_defers_instead_of_self_fencing(self) -> None:
        """Raising here would roll back a valid write and restart-loop on the next one.

        The locker is this process's own fenced write, so no competing expiry
        claim can be in flight behind it — the tuple's exclusive lock is the
        proof — and its commit refreshes ``lease_expires_at`` before any
        queued claimant re-evaluates its CAS. The session provably survives,
        so the verification must not raise. It survives as a *process* only:
        the argument assumes the write commits, and a rollback would hand the
        expired row straight to a queued claimant, so the same result carries
        ``renewal_deferred_to_own_write`` to tell external-side-effect fences
        to withhold their RPC.
        """
        with LeaseHarness(lease_ttl_seconds=LEASE_TTL_SECONDS) as harness:
            observed = drive_own_write_outlasting_the_ttl(harness)

            self.assertTrue(
                observed["committed_row_expired"],
                "the committed row must have lapsed for this to be the "
                "fail-closed shape at all",
            )
            self.assertTrue(observed["committed_row_locked"])
            self.assertEqual(
                observed["verification"],
                {
                    "backend": "postgres-psql",
                    "verified_count": 1,
                    "renewed_count": 0,
                    "renewal_deferred_to_own_write": True,
                },
            )
            self.assertEqual(observed["lockwait_stops"], [])
            self.assertEqual(
                observed["statements"],
                ["acquire", "renew", "verify"],
                "the locker was attributable on the first statement, so no "
                "attribution recheck was warranted",
            )

    def test_an_expired_row_locked_by_a_foreign_application_fails_closed(self) -> None:
        """The contrast that gives the deferral its meaning.

        Identical read — expired committed row, renewal skipped over a held
        lock — and the only difference is who holds the lock. A locker that
        cannot be attributed to this process's pool ``application_name`` may
        be a different-identity expiry claim whose commit lands right after
        this snapshot, and the stale token read would name us until it does.
        A skipped renewal is not evidence of liveness there, so it raises.
        """
        with LeaseHarness(lease_ttl_seconds=LEASE_TTL_SECONDS) as harness:
            observed = drive_foreign_locker_over_the_expired_row(harness)

            self.assertEqual(
                observed["committed_row_session"],
                "heartbeat-v1:alpha-1",
                "the claimant's takeover must still be uncommitted, or the "
                "verification would fail on identity instead",
            )
            self.assertTrue(observed["committed_row_expired"])
            self.assertEqual(observed["error_type"], "RuntimeError")
            self.assertIsNotNone(observed["error"])
            self.assertIn(
                "expired and its renewal was lock-blocked",
                str(observed["error"]),
            )
            self.assertEqual(
                observed["lockwait_stops"],
                [],
                "failing closed must still be non-blocking; the point of the "
                "fix is that the guard never queues, not that it never raises",
            )
            self.assertEqual(
                observed["statements"],
                ["acquire", "acquire", "verify", "verify"],
                "an unattributable locker over an expired row is the one "
                "ambiguous shape, so it earns exactly one recheck before the "
                "fail-closed raise",
            )

    def test_own_write_deferral_is_order_stable_across_repeated_runs(self) -> None:
        """The TTL-lapse interleaving pins a schedule too, not just an outcome."""
        run = assert_deterministic(
            self,
            drive_own_write_outlasting_the_ttl,
            repeats=25,
            lease_ttl_seconds=LEASE_TTL_SECONDS,
        )

        self.assertEqual(
            list(run.trace),
            [
                "alpha@alpha.begin:acquire",
                "alpha@alpha.done:acquire",
                "alpha@done:startup",
                "alpha@alpha.begin:renew",
                "alpha@alpha.done:renew",
                "heartbeat@alpha.guard.begin:verify",
                "heartbeat@alpha.guard.snapshot:verify",
                "heartbeat@alpha.guard.done:verify",
                "heartbeat@done:heartbeat",
            ],
        )


class GuardQuerySlotTests(unittest.TestCase):
    """One guard connection, one statement at a time."""

    def test_production_guard_serializes_callers_on_one_query_slot(self) -> None:
        """The shipped guard, not the harness's model of it.

        The scheduler test below drives ``FakeLeaseGuard``, whose slot is a
        reimplementation: deleting ``with self._query_lock:`` from
        ``_NativePostgresLeaseGuard.run_json`` would leave every assertion
        there green. This one holds the production lock and shows the
        production method waiting for it, which is the claim the module
        actually wants to make. It needs no server — the serialization is a
        plain lock around the statement, above the connection.
        """
        guard = share_ledger_module._NativePostgresLeaseGuard.__new__(
            share_ledger_module._NativePostgresLeaseGuard
        )
        guard._advisory_lock_key = 0
        guard._query_lock = threading.Lock()
        guard._closed = False
        guard._held = True
        guard._connection = None

        entered = threading.Event()
        arrived = threading.Event()
        released = threading.Event()

        def second_caller() -> None:
            entered.set()
            # Reaching the statement requires the slot; `held` is True and
            # `_connection` is None, so arrival past the slot is observable as
            # the AttributeError the statement itself then raises. Catching
            # that exact type is what makes this evidence: any other outcome
            # means run_json did not get where this test claims it got.
            try:
                guard.run_json("SELECT 1")
            except AttributeError:
                arrived.set()
            released.set()

        guard._query_lock.acquire()
        caller = threading.Thread(target=second_caller, daemon=True)
        caller.start()
        self.assertTrue(entered.wait(2.0))
        self.assertFalse(
            released.wait(0.2),
            "run_json entered the statement while another caller held the "
            "query slot; the guard session is not serialized",
        )
        guard._query_lock.release()
        self.assertTrue(
            released.wait(2.0),
            "releasing the slot must let the queued caller proceed",
        )
        self.assertTrue(
            arrived.is_set(),
            "the queued caller must have reached the statement itself, not "
            "merely returned",
        )
        caller.join(timeout=2.0)

    def test_a_second_caller_parks_on_the_guard_query_slot(self) -> None:
        """The harness models that serialization as an interleaving point.

        Both callers run the non-blocking verification, so neither can be
        queued on the lease tuple; the only thing that can hold one up is the
        other's occupancy of the single guard connection. This pins the
        harness's rendering of it — that the second caller stops at a named
        checkpoint and is released by the holder rather than by a timeout —
        so scenarios can schedule around it. The production serialization
        itself is pinned by the test above.
        """
        with LeaseHarness(lease_ttl_seconds=LEASE_TTL_SECONDS) as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "done:startup")

            first = harness.scheduler.actor("hb-first")
            second = harness.scheduler.actor("hb-second")
            first_call = first.submit(
                alpha.ledger.verify_writer_lease_guard_session,
                label="first",
            )
            second_call = second.submit(
                alpha.ledger.verify_writer_lease_guard_session,
                label="second",
            )

            harness.run_until(first, "alpha.guard.begin:verify")
            harness.run_until_blocked(second, "alpha.guard.slot:wait")
            self.assertIsNone(
                second.wake_at,
                "the slot wait is released by the holder, not by a timeout",
            )

            harness.run_until(first, "done:first")
            self.assertTrue(
                second.runnable(),
                "releasing the slot must be what makes the second caller "
                "runnable again",
            )
            harness.run_until(second, "done:second")

            self.assertEqual(first_call.value()["verified_count"], 1)
            self.assertEqual(second_call.value()["verified_count"], 1)
            self.assertEqual(
                harness.trace,
                [
                    "alpha@alpha.begin:acquire",
                    "alpha@alpha.done:acquire",
                    "alpha@done:startup",
                    "hb-first@alpha.guard.begin:verify",
                    "hb-second@alpha.guard.slot:wait",
                    "hb-first@alpha.guard.snapshot:verify",
                    "hb-first@alpha.guard.done:verify",
                    "hb-first@done:first",
                    "hb-second@alpha.guard.begin:verify",
                    "hb-second@alpha.guard.snapshot:verify",
                    "hb-second@alpha.guard.done:verify",
                    "hb-second@done:second",
                ],
            )


if __name__ == "__main__":
    unittest.main()
