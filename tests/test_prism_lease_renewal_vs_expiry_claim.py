#!/usr/bin/env python3
"""Writer-lease heartbeat renewal racing a competing expiry claim.

``PsqlShareLedger.verify_writer_lease_guard_session`` renews the lease TTL
with ``FOR NO KEY UPDATE SKIP LOCKED`` so a heartbeat never queues behind a
fenced write holding the ``qbit_ledger_writer_lease`` tuple lock. Declining
to queue buys liveness but loses the re-evaluation the old locking renewal
performed after a contending transaction committed: once the committed row's
TTL has lapsed, the lock the heartbeat skipped may be a different-identity
``_try_acquire_writer_lease`` taking the row through its expiry CAS, and the
stale committed snapshot would keep naming this session right up until that
claim commits. The defect shape this module covers is that gap: a heartbeat
that reads its own stale token off an expired, foreign-locked row and calls
itself alive would authorize a fenced-out writer.

The production docstring states four invariants, and each gets a scenario:

1. Uncontended, the heartbeat renews: ``renewed_count == 1`` and the
   committed row's ``lease_expires_at`` moves a full TTL ahead of the
   virtual clock.
2. Expired row plus a lock held by a different-identity claimant fails
   closed ("expired and its renewal was lock-blocked"), and it is the
   *combination* that raises: the same foreign lock over an unexpired row
   verifies fine with ``renewed_count == 0``.
3. The one ambiguous result shape — expired, renewal skipped, no
   attributable locker — earns exactly one recheck on a fresh snapshot,
   because this coordinator's own fenced write committing between the first
   statement's MVCC snapshot and its live ``pg_stat_activity`` probe
   presents identically to a competing claim. The recheck resolves it and
   the verification returns instead of restart-looping a healthy writer.
4. A claim that commits wins: the next verification raises "writer lease is
   not active" instead of reporting a renewal.

The competing claim is a real second coordinator at the next writer epoch
running the shipped startup acquisition. Its expiry CAS executes and takes
the tuple lock, and the harness's ``done:acquire`` checkpoint sits between
statement execution and the transaction's commit, so parking the claimant
there holds the claim in flight — executed, locking, uncommitted — for as
long as the scenario needs.
"""

from __future__ import annotations

import unittest
from datetime import timedelta

from tests.prism_concurrency_harness import (
    LeaseHarness,
    assert_deterministic,
)

LEASE_TTL_SECONDS = 60.0

# Push the virtual clock just past lease_expires_at. The exact overshoot is
# irrelevant; the fail-closed branch triggers on `lease_expires_at <=
# clock_timestamp()`, not on how stale the row is.
EXPIRY_LAPSE_SECONDS = LEASE_TTL_SECONDS + 1.0

# For the unexpired control: far enough in that time visibly passed, far
# enough out that the row is neither expired nor inside the authority margin
# (TTL/2 = 30s), so the success path exercised is plain "skip and trust the
# committed row" with no deferral semantics in play.
UNEXPIRED_ADVANCE_SECONDS = 20.0

# Budget for the coordinator's own fenced write in the recheck scenario. Any
# armed deadline works; a deadline is what makes the client wrap the
# statement in an explicit transaction whose COMMIT is a separate message,
# giving the scenario a precommit stop inside the snapshot-to-probe window.
LANDING_BUDGET_SECONDS = 30.0


def drive_expired_lock_blocked_renewal(harness: LeaseHarness) -> dict[str, object]:
    """Scenario 2's raise: expired row, tuple lock held by a foreign claim.

    1. ``alpha`` starts and holds the lease; the clock then lapses the TTL.
    2. ``claimant``, at the next writer epoch, starts up. Its advisory-guard
       key differs from ``alpha``'s, so it reaches the lease upsert, whose
       expiry CAS executes and takes the tuple lock. It is parked at
       ``done:acquire``: the claim is in flight and uncommitted.
    3. ``alpha``'s heartbeat verification runs. Its ``SKIP LOCKED`` renewal
       skips, the committed snapshot still names ``alpha``, and the row is
       expired — the shape whose meaning one statement cannot decide, so the
       verification spends its one recheck on a fresh snapshot, sees the
       same contended expiry, and fails closed.
    """
    alpha = harness.coordinator("alpha", writer_epoch=1)
    alpha.start()
    harness.run_until(alpha, "done:startup")
    row_at_startup = harness.lease_row()
    assert row_at_startup is not None

    harness.advance(EXPIRY_LAPSE_SECONDS)

    claimant = harness.coordinator("claimant", writer_epoch=2)
    claimant.start()
    harness.run_until(claimant, "claimant.begin:acquire")
    harness.run_until(claimant, "claimant.done:acquire")
    claim_tx = harness.server.lease_lock_holder

    verify = alpha.submit(
        alpha.ledger.verify_writer_lease_guard_session,
        label="verify",
    )
    harness.run_until(alpha, "done:verify")

    row = harness.lease_row()
    assert row is not None
    return {
        "claim_took_lock": claim_tx is not None,
        "claim_still_holds_lock": harness.server.lease_lock_holder is claim_tx,
        "claim_uncommitted": not claimant.started,
        "verify_raised": verify.error is not None,
        "verify_error": str(verify.error),
        "committed_row_session": row.writer_session_token,
        "committed_row_expired": row.lease_expires_at <= harness.clock.now(),
        "committed_row_unrenewed": (
            row.lease_expires_at == row_at_startup.lease_expires_at
        ),
        "statement_kinds": harness.statement_kinds(),
    }


def drive_unexpired_lock_blocked_renewal(harness: LeaseHarness) -> dict[str, object]:
    """Scenario 2's control: the identical foreign lock over a live row.

    Same claimant, same parked uncommitted acquire holding the tuple lock;
    the only difference is that the committed row has not expired (the
    claimant's upsert therefore observed the holder instead of running the
    expiry CAS — either way its statement locks the row until commit). The
    skipped renewal must be trusted on the committed row's own validity.
    """
    alpha = harness.coordinator("alpha", writer_epoch=1)
    alpha.start()
    harness.run_until(alpha, "done:startup")
    row_at_startup = harness.lease_row()
    assert row_at_startup is not None

    harness.advance(UNEXPIRED_ADVANCE_SECONDS)

    claimant = harness.coordinator("claimant", writer_epoch=2)
    claimant.start()
    harness.run_until(claimant, "claimant.begin:acquire")
    harness.run_until(claimant, "claimant.done:acquire")
    claim_tx = harness.server.lease_lock_holder

    verify = alpha.submit(
        alpha.ledger.verify_writer_lease_guard_session,
        label="verify",
    )
    harness.run_until(alpha, "done:verify")

    row = harness.lease_row()
    assert row is not None
    return {
        "claim_still_holds_lock": (
            claim_tx is not None
            and harness.server.lease_lock_holder is claim_tx
        ),
        "verify_raised": verify.error is not None,
        "result": verify.result,
        "committed_row_session": row.writer_session_token,
        "committed_row_unrenewed": (
            row.lease_expires_at == row_at_startup.lease_expires_at
        ),
        "statement_kinds": harness.statement_kinds(),
    }


def drive_own_write_attribution_recheck(harness: LeaseHarness) -> dict[str, object]:
    """Scenario 3: the coordinator's own commit lands inside the probe gap.

    ``alpha``'s deadline-scoped fenced write has executed its statement —
    holding the tuple lock, its renewal staged — but has not yet sent
    COMMIT. The heartbeat verification's first statement takes its MVCC
    snapshot in that window: expired row, renewal skipped. The scenario then
    commits the fenced write at the ``guard.snapshot:verify`` checkpoint,
    exactly between that snapshot and the statement's live
    ``pg_stat_activity`` probe, so the probe finds the locker's
    ``backend_xid`` already cleared: an own write that just landed is now
    indistinguishable from a competing claim. The verification must resolve
    this with one recheck, not a raise.
    """
    alpha = harness.coordinator("alpha", writer_epoch=1)
    alpha.start()
    harness.run_until(alpha, "done:startup")

    harness.advance(EXPIRY_LAPSE_SECONDS)

    def fenced_landing() -> object:
        # Renewal under an armed deadline: the smallest shipped statement
        # with the fenced-write transaction shape (BEGIN / statement /
        # separate COMMIT) that locks and rewrites the lease tuple.
        with alpha.ledger.operation_timeout(LANDING_BUDGET_SECONDS):
            return alpha.ledger.renew_writer_lease()

    alpha.submit(fenced_landing, label="landing")
    harness.run_until(alpha, "alpha.precommit")
    landing_tx = harness.server.lease_lock_holder
    row_in_snapshot = harness.lease_row()
    assert row_in_snapshot is not None
    statements_before = len(harness.server.statements)

    # The heartbeat runs on its own thread inside the coordinator process;
    # alpha's actor is parked inside the fenced write, so the verification
    # needs its own actor to interleave with it.
    progress: list[str] = []
    heartbeat = harness.scheduler.actor("heartbeat")
    verify = heartbeat.submit(
        lambda: alpha.ledger.verify_writer_lease_guard_session(
            on_statement_progress=lambda: progress.append("round-trip"),
        ),
        label="verify",
    )
    harness.run_until(heartbeat, "alpha.guard.snapshot:verify")

    # What the first statement's snapshot saw: the documented ambiguous
    # inputs, minus the locker attribution the commit is about to spoil.
    first_statement_shape = {
        "row_expired": row_in_snapshot.lease_expires_at <= harness.clock.now(),
        "row_names_this_session": (
            row_in_snapshot.writer_session_token == alpha.session_token
        ),
        "renewal_lock_blocked": (
            landing_tx is not None
            and harness.server.lease_lock_holder is landing_tx
        ),
        "row_xmax_is_own_write": (
            landing_tx is not None and row_in_snapshot.xmax == landing_tx.xid
        ),
    }

    # The misfire: the own fenced write commits between the snapshot and the
    # pg_stat_activity probe, clearing its backend_xid while the snapshot
    # still shows the expired row that write had locked.
    harness.run_until(alpha, "done:landing")
    harness.run_until(heartbeat, "done:verify")

    row = harness.lease_row()
    assert row is not None
    return {
        "first_statement_shape": first_statement_shape,
        "verify_raised": verify.error is not None,
        "result": verify.result,
        "progress_calls": len(progress),
        "verify_statements_run": (
            len(harness.server.statements) - statements_before
        ),
        "statement_kinds": harness.statement_kinds(),
        "lease_holder_session": harness.lease_holder_session(),
        "lease_unexpired_after": (
            row.lease_expires_at > harness.clock.now()
        ),
    }


def drive_committed_expiry_claim(harness: LeaseHarness) -> dict[str, object]:
    """Scenario 4: the competing claim commits and this session is fenced."""
    alpha = harness.coordinator("alpha", writer_epoch=1)
    alpha.start()
    harness.run_until(alpha, "done:startup")

    harness.advance(EXPIRY_LAPSE_SECONDS)

    claimant = harness.coordinator("claimant", writer_epoch=2)
    claimant.start()
    # Run the claimant's startup to completion: its expiry CAS commits and
    # the singleton row now names the claimant's session.
    harness.run_until(claimant, "done:startup")

    verify = alpha.submit(
        alpha.ledger.verify_writer_lease_guard_session,
        label="verify",
    )
    harness.run_until(alpha, "done:verify")

    return {
        "claimant_started": claimant.started,
        "verify_raised": verify.error is not None,
        "verify_error": str(verify.error),
        "lease_holder_session": harness.lease_holder_session(),
        "statement_kinds": harness.statement_kinds(),
    }


class UncontendedRenewalTests(unittest.TestCase):
    """Scenario 1: a healthy heartbeat renews without contention."""

    def test_heartbeat_renews_a_full_ttl_ahead_of_the_clock(self) -> None:
        """The committed row, not just the return value, must move.

        The return value alone could report a renewal whose UPDATE matched
        zero rows; asserting ``lease_expires_at`` and ``updated_at`` against
        the virtual clock proves the renewal landed on the singleton row and
        pushed the TTL a full interval ahead of *now*, not ahead of the old
        expiry.
        """
        with LeaseHarness(lease_ttl_seconds=LEASE_TTL_SECONDS) as harness:
            alpha = harness.coordinator("alpha", writer_epoch=1)
            alpha.start()
            harness.run_until(alpha, "done:startup")

            # Half a TTL of virtual time passes so the renewed expiry is
            # visibly different from the acquisition's.
            harness.advance(LEASE_TTL_SECONDS / 2)

            result = alpha.call(
                alpha.ledger.verify_writer_lease_guard_session,
                label="verify",
            )
            self.assertEqual(
                result,
                {
                    "backend": "postgres-psql",
                    "verified_count": 1,
                    "renewed_count": 1,
                    "renewal_deferred_to_own_write": False,
                },
            )

            row = harness.lease_row()
            assert row is not None
            self.assertEqual(row.writer_session_token, "heartbeat-v1:alpha-1")
            self.assertEqual(
                row.lease_expires_at,
                harness.clock.now() + timedelta(seconds=LEASE_TTL_SECONDS),
            )
            self.assertEqual(row.updated_at, harness.clock.now())
            self.assertIsNone(row.xmax, "the renewal's transaction committed")


class ExpiryClaimFailClosedTests(unittest.TestCase):
    """Scenario 2: expired + foreign-locked raises; either alone does not."""

    def test_expired_row_with_foreign_lock_fails_closed(self) -> None:
        with LeaseHarness(lease_ttl_seconds=LEASE_TTL_SECONDS) as harness:
            observed = drive_expired_lock_blocked_renewal(harness)

            self.assertTrue(observed["claim_took_lock"])
            self.assertTrue(
                observed["claim_still_holds_lock"],
                "the competing claim must still be in flight when the "
                "heartbeat verifies; a committed claim is scenario 4",
            )
            self.assertTrue(observed["claim_uncommitted"])
            self.assertTrue(observed["verify_raised"])
            self.assertRegex(
                str(observed["verify_error"]),
                "expired and its renewal was lock-blocked",
            )
            # The stale committed snapshot is exactly what makes the raise
            # necessary: it still names this session, so a token read alone
            # would have called the fenced-out writer alive.
            self.assertEqual(
                observed["committed_row_session"],
                "heartbeat-v1:alpha-1",
            )
            self.assertTrue(observed["committed_row_expired"])
            self.assertTrue(observed["committed_row_unrenewed"])
            # Two verify statements: the ambiguous shape (the claimant's
            # backend is not attributable to alpha's pool) earns the one
            # recheck, whose fresh snapshot sees the same contended expiry
            # and still fails closed — the docstring's residual outcome.
            self.assertEqual(
                observed["statement_kinds"],
                ["acquire", "acquire", "verify", "verify"],
            )

    def test_same_lock_over_an_unexpired_row_verifies_clean(self) -> None:
        """The control that pins the raise on the *combination*.

        If the foreign lock alone raised, every heartbeat racing an ordinary
        fenced write would kill a healthy coordinator; the non-blocking
        renewal is only allowed to distrust a skip once the committed row's
        own validity has lapsed. Identical claimant, identical held lock,
        unexpired row: the verification must succeed, renewing nothing.
        """
        with LeaseHarness(lease_ttl_seconds=LEASE_TTL_SECONDS) as harness:
            observed = drive_unexpired_lock_blocked_renewal(harness)

            self.assertTrue(observed["claim_still_holds_lock"])
            self.assertFalse(observed["verify_raised"])
            self.assertEqual(
                observed["result"],
                {
                    "backend": "postgres-psql",
                    "verified_count": 1,
                    "renewed_count": 0,
                    "renewal_deferred_to_own_write": False,
                },
            )
            self.assertEqual(
                observed["committed_row_session"],
                "heartbeat-v1:alpha-1",
            )
            self.assertTrue(
                observed["committed_row_unrenewed"],
                "a skipped renewal must leave the committed expiry alone",
            )
            # Unexpired means unambiguous: no recheck, one verify statement.
            self.assertEqual(
                observed["statement_kinds"],
                ["acquire", "acquire", "verify"],
            )

    def test_fail_closed_interleaving_is_order_stable(self) -> None:
        """A fail-closed path that only usually triggers is a split brain.

        The exact schedule is the assertion: the claim executes and parks
        uncommitted before the verification starts, and the verification's
        two statements (first read, one recheck) both run against the still
        contended row. Any drift in this trace means the scenario stopped
        testing the interleaving it claims to test.
        """
        run = assert_deterministic(
            self,
            drive_expired_lock_blocked_renewal,
            repeats=25,
            lease_ttl_seconds=LEASE_TTL_SECONDS,
        )

        self.assertEqual(
            list(run.trace),
            [
                "alpha@alpha.begin:acquire",
                "alpha@alpha.done:acquire",
                "alpha@done:startup",
                "claimant@claimant.begin:acquire",
                "claimant@claimant.done:acquire",
                "alpha@alpha.guard.begin:verify",
                "alpha@alpha.guard.snapshot:verify",
                "alpha@alpha.guard.done:verify",
                "alpha@alpha.guard.begin:verify",
                "alpha@alpha.guard.snapshot:verify",
                "alpha@alpha.guard.done:verify",
                "alpha@done:verify",
            ],
        )


class AttributionRecheckTests(unittest.TestCase):
    """Scenario 3: the ambiguous shape earns exactly one fresh-snapshot look."""

    def test_own_commit_in_probe_window_resolves_by_recheck(self) -> None:
        with LeaseHarness(lease_ttl_seconds=LEASE_TTL_SECONDS) as harness:
            observed = drive_own_write_attribution_recheck(harness)

            # The first statement really produced the ambiguous shape: the
            # snapshot row was expired, still named this session, and its
            # locker was this coordinator's own in-flight write — which the
            # live probe could no longer attribute after the commit.
            self.assertEqual(
                observed["first_statement_shape"],
                {
                    "row_expired": True,
                    "row_names_this_session": True,
                    "renewal_lock_blocked": True,
                    "row_xmax_is_own_write": True,
                },
            )
            # The verification returned; raising here is the misfire the
            # recheck exists to prevent — it would roll back nothing (the
            # write already committed) and restart-loop a healthy writer on
            # every fenced write slow enough to outlast the TTL.
            self.assertFalse(observed["verify_raised"])
            self.assertEqual(
                observed["result"],
                {
                    "backend": "postgres-psql",
                    "verified_count": 1,
                    "renewed_count": 1,
                    "renewal_deferred_to_own_write": False,
                },
            )
            # WRITER_LEASE_VERIFICATION_MAX_STATEMENTS is 2: exactly one
            # recheck ran, and it announced the first statement's completed
            # round trip to the liveness monitor exactly once.
            self.assertEqual(observed["progress_calls"], 1)
            self.assertEqual(observed["verify_statements_run"], 2)
            self.assertEqual(
                observed["statement_kinds"],
                ["acquire", "renew", "verify", "verify"],
            )
            # The recheck's fresh snapshot saw the committed refresh and the
            # second statement's renewal landed on the uncontended row.
            self.assertEqual(
                observed["lease_holder_session"],
                "heartbeat-v1:alpha-1",
            )
            self.assertTrue(observed["lease_unexpired_after"])

    def test_recheck_interleaving_is_order_stable(self) -> None:
        """The misfire window is two checkpoints wide; pin the schedule.

        The own write must commit after ``guard.snapshot:verify`` (the first
        statement's MVCC snapshot is taken) and before that statement's live
        probe runs. If the trace drifts — the commit landing before the
        snapshot, or the recheck not running — the test would silently stop
        covering the one-direction misfire the production docstring names.
        """
        run = assert_deterministic(
            self,
            drive_own_write_attribution_recheck,
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
                "alpha@alpha.precommit",
                "heartbeat@alpha.guard.begin:verify",
                "heartbeat@alpha.guard.snapshot:verify",
                "alpha@done:landing",
                "heartbeat@alpha.guard.done:verify",
                "heartbeat@alpha.guard.begin:verify",
                "heartbeat@alpha.guard.snapshot:verify",
                "heartbeat@alpha.guard.done:verify",
                "heartbeat@done:verify",
            ],
        )


class CommittedExpiryClaimTests(unittest.TestCase):
    """Scenario 4: a claim that wins fences this session outright."""

    def test_committed_claim_fences_the_session(self) -> None:
        """Losing the row must read as fencing, not as a lock-blocked retry.

        Once the claimant's CAS commits, the committed row no longer names
        this session, which is conclusive — not the ambiguous shape — so
        the verification raises the identity failure in one statement
        rather than spending its recheck or, worse, reporting a renewal
        against a row it no longer owns.
        """
        with LeaseHarness(lease_ttl_seconds=LEASE_TTL_SECONDS) as harness:
            observed = drive_committed_expiry_claim(harness)

            self.assertTrue(observed["claimant_started"])
            self.assertEqual(
                observed["lease_holder_session"],
                "heartbeat-v1:claimant-2",
            )
            self.assertTrue(observed["verify_raised"])
            self.assertRegex(
                str(observed["verify_error"]),
                "writer lease is not active",
            )
            self.assertEqual(
                observed["statement_kinds"],
                ["acquire", "acquire", "verify"],
            )


if __name__ == "__main__":
    unittest.main()
