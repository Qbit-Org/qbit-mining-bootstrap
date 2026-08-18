#!/usr/bin/env python3
"""Writer-lease takeover: the proof fast adoption owes before it fences a peer.

Fast adoption is the only path by which one PRISM coordinator takes the
writer lease from a same-identity predecessor *before* the TTL has run out,
and therefore the only path that can hand two live processes overlapping
authority if its proof is wrong. The proof has three independent parts, and
each one fails in a way that is invisible to a single-process test:

``_initialize_writer_lease_guard``
    The successor must hold the predecessor's PostgreSQL session advisory
    lock. Two coordinators sharing ``(writer_id, writer_epoch)`` hash to the
    same key, so a live predecessor keeps its successor out of the lease
    upsert entirely: the successor never observes the row, never runs a CAS,
    and spins. A guard that failed open would not change any *outcome* a
    functional test can see -- adoption would still work -- so the only
    executable statement of it is that no statement reaches the server.

``_writer_lease_adoption_wait_seconds``
    Silence is measured from two edges and the later one wins: the lease
    row's ``updated_at`` age, and the time since *this* process acquired the
    guard. The guard edge is the one that is easy to drop and impossible to
    notice, because dropping it is only wrong when the row is *already*
    stale: a long fenced transaction such as ``persist_accepted_block``
    withholds ``updated_at`` refreshes, so the row can look minutes silent at
    the instant the predecessor dies. A row-only implementation adopts
    instantly in exactly that case and gives the dying predecessor none of
    its heartbeat failure budget to self-fence. The second scenario here
    constructs precisely that state -- a row far staler than the silence
    interval -- and requires a full interval of waiting anyway.

``_try_adopt_writer_lease``
    The CAS is the final database fence. Winning it rewrites the session
    token, which fences every later predecessor mutation. Losing it must not
    be terminal: the renewal that beat it may have been the predecessor's
    final act before dying, so refusing that token permanently would recreate
    the whole-TTL outage adoption exists to avoid. The successor must
    re-observe and wait a *fresh* full silence interval instead.

The last scenario covers the other half of the identity rule. Fast adoption
is deliberately unavailable across writer epochs -- ``_can_wait_for_writer_lease``
requires an exact ``(writer_id, writer_epoch)`` match, and a different epoch
hashes to a different advisory key, so nothing in this process can prove
anything about the holder. A different epoch must therefore refuse loudly
while the lease is live, and take it through the ordinary expiry CAS once it
has lapsed.

Every scenario is driven through the shipped code: the real retry loop, the
real adoption arithmetic, the real CAS SQL. Nothing is patched; the harness
substitutes PostgreSQL, the clock and the retry sleep at the constructor
seams ``PsqlShareLedger`` already exposes.
"""

from __future__ import annotations

import unittest

from tests.prism_concurrency_harness import (
    Coordinator,
    LeaseHarness,
    assert_deterministic,
)

# A long TTL keeps every scenario inside the *unexpired* half of the lease
# lifecycle, which is the only half where adoption is reachable at all: an
# expired row is taken by the ordinary expiry CAS in `_try_acquire_writer_lease`
# and never consults the silence arithmetic.
LEASE_TTL_SECONDS = 600.0

# Small and explicit so the waiting arithmetic below is readable as arithmetic
# rather than taken on trust. Production defaults to 1.0s
# (`DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS`); the mechanism does not
# depend on the value, only on which edge it is measured from.
ADOPTION_SILENCE_SECONDS = 5.0

# How stale the lease row is made before the successor ever starts. Chosen to
# be far larger than the silence interval, so a row-edge-only implementation
# would compute a wait of zero and adopt on its first observation.
PREDECESSOR_SILENT_SECONDS = 120.0

# `_lease_retry_min_sleep_seconds` is `min(0.25, lease_retry_max_sleep_seconds)`,
# and the guard spin sleeps exactly that between attempts.
GUARD_SPIN_SLEEP_SECONDS = 0.25

# How many times the gated successor is made to go round the guard spin. Any
# number above one works; more than one is the point, because the warning is
# asserted to be printed once for the whole wait rather than once per lap.
GUARD_SPIN_LAPS = 4

GUARD_HELD_WARNING = (
    "prism ledger writer guard held by a live same-identity coordinator; "
    "waiting before lease acquisition"
)

HARNESS_KWARGS = {
    "lease_ttl_seconds": LEASE_TTL_SECONDS,
    "lease_adoption_silence_seconds": ADOPTION_SILENCE_SECONDS,
}


def start_predecessor(harness: LeaseHarness) -> Coordinator:
    """Bring up the lease holder every scenario takes over from."""
    alpha = harness.coordinator("alpha", writer_epoch=1)
    alpha.start()
    harness.run_until(alpha, "done:startup")
    return alpha


def drive_guard_gated_successor(harness: LeaseHarness) -> dict[str, object]:
    """Start a same-identity successor while the predecessor is alive.

    ``beta`` shares ``alpha``'s ``(writer_id, writer_epoch)``, so
    ``_writer_lease_advisory_lock_key`` hands both the same key and ``beta``
    cannot get past ``_initialize_writer_lease_guard``. It is stepped round
    the spin several times to show the wait is a loop rather than a single
    stall, and that the loop reaches the server not at all.
    """
    alpha = start_predecessor(harness)
    statements_before = harness.statement_kinds()

    beta = harness.coordinator("beta", writer_epoch=1)
    beta.start()
    harness.run_until_blocked(beta, f"sleep:{GUARD_SPIN_SLEEP_SECONDS:g}")
    for _ in range(GUARD_SPIN_LAPS - 1):
        harness.advance_to_next_deadline()
        harness.run_until_blocked(beta, f"sleep:{GUARD_SPIN_SLEEP_SECONDS:g}")

    return {
        "statements_before": statements_before,
        "statements_after": harness.statement_kinds(),
        "successor_stops": [
            entry.split("@", 1)[1]
            for entry in harness.trace
            if entry.startswith(f"{beta.name}@")
        ],
        "successor_started": beta.started,
        "sleeps": list(harness.sleeps),
        "same_advisory_key": (
            beta.guard.advisory_lock_key == alpha.guard.advisory_lock_key
        ),
        "predecessor_guard_held": alpha.guard.held,
        "successor_guard_held": beta.guard.held,
        "warning_count": harness.output.count(GUARD_HELD_WARNING),
    }


def drive_fast_adoption(harness: LeaseHarness) -> dict[str, object]:
    """Adopt from a predecessor whose lease row is already long stale.

    The clock is advanced ``PREDECESSOR_SILENT_SECONDS`` *before* the
    predecessor's guard is released, which is the shape a long fenced
    transaction produces: the row has not been refreshed for minutes, yet the
    predecessor only stopped being able to act at the instant the guard went.
    Everything the successor may conclude about silence therefore has to be
    measured from its own guard acquisition.
    """
    alpha = start_predecessor(harness)
    harness.advance(PREDECESSOR_SILENT_SECONDS)
    alpha.guard.close()

    beta = harness.coordinator("beta", writer_epoch=1)
    beta.start()
    # The successor takes the guard, observes the row, and finds it cannot
    # adopt yet. This is the first moment it could have adopted if silence
    # were read off the row alone.
    harness.run_until_blocked(beta, f"sleep:{ADOPTION_SILENCE_SECONDS:g}")
    observed_row = harness.lease_row()
    row_age_at_first_observation = (
        harness.clock.now() - observed_row.updated_at
    ).total_seconds()
    statements_at_first_observation = harness.statement_kinds()
    monotonic_at_first_observation = harness.clock.monotonic()

    harness.advance_to_next_deadline()
    harness.run_until(beta, "done:startup")

    adopted_row = harness.lease_row()
    return {
        "row_age_at_first_observation": row_age_at_first_observation,
        "statements_at_first_observation": statements_at_first_observation,
        "waited_seconds": (
            harness.clock.monotonic() - monotonic_at_first_observation
        ),
        "sleeps": list(harness.sleeps),
        "statements": harness.statement_kinds(),
        "session_token": adopted_row.writer_session_token,
        "predecessor_token": alpha.session_token,
        "successor_token": beta.session_token,
        "lease_expires_at": str(adopted_row.lease_expires_at),
        "ttl_ahead_seconds": (
            adopted_row.lease_expires_at - harness.clock.now()
        ).total_seconds(),
        "adoption_announced": (
            "adopted from same-identity predecessor" in harness.output
        ),
    }


def drive_lost_adoption_cas(harness: LeaseHarness) -> dict[str, object]:
    """Land a predecessor renewal between the successor's observation and CAS.

    The interleaving is forced by PostgreSQL's own ordering rather than by
    scheduling luck. At ``beta.done:acquire`` the successor's transaction
    still holds the lease tuple lock, so the predecessor's renewal queues
    behind it; letting the successor run on to its COMMIT releases the lock,
    at which point the renewal lands and changes ``updated_at`` -- the exact
    column the adoption CAS compares. The successor is then at
    ``beta.begin:adopt`` with a stale observation and no way to know it.

    Since #123's fix the observation is deadline-scoped, so that COMMIT is an
    explicit message and reaching it takes a ``beta.precommit`` stop and then
    a step, where an autocommit statement committed itself in one. The
    interleaving is the same one and lands at the same place; only the number
    of scheduler steps needed to reach it changed.
    """
    alpha = start_predecessor(harness)
    harness.advance(PREDECESSOR_SILENT_SECONDS)
    alpha.guard.close()

    beta = harness.coordinator("beta", writer_epoch=1)
    beta.start()
    harness.run_until_blocked(beta, f"sleep:{ADOPTION_SILENCE_SECONDS:g}")
    harness.advance_to_next_deadline()

    # Re-observation: adoption is now permitted, and the successor holds the
    # tuple lock until this statement's autocommit COMMIT.
    harness.run_until(beta, "beta.done:acquire")
    renewal = alpha.submit(alpha.ledger.renew_writer_lease, label="renew")
    harness.run_until_blocked(alpha, "alpha.lockwait:renew")

    # Commit the observation and carry the successor to the CAS statement,
    # which has begun a transaction but not yet taken the lock.
    harness.run_until(beta, "beta.precommit")
    harness.step(beta)
    stop_before_cas = beta.actor.stop
    harness.run_until(alpha, "done:renew")

    # The CAS now runs against a row the renewal moved out from under it.
    harness.run_until_blocked(beta, f"sleep:{ADOPTION_SILENCE_SECONDS:g}")
    statements_after_lost_cas = harness.statement_kinds()
    sleeps_after_lost_cas = list(harness.sleeps)
    monotonic_after_lost_cas = harness.clock.monotonic()

    harness.advance_to_next_deadline()
    harness.run_until(beta, "done:startup")

    adopted_row = harness.lease_row()
    return {
        "stop_before_cas": stop_before_cas,
        "renewal": renewal.value(),
        "statements_after_lost_cas": statements_after_lost_cas,
        "sleeps_after_lost_cas": sleeps_after_lost_cas,
        "waited_after_lost_cas": (
            harness.clock.monotonic() - monotonic_after_lost_cas
        ),
        "sleeps": list(harness.sleeps),
        "statements": harness.statement_kinds(),
        "session_token": adopted_row.writer_session_token,
        "successor_token": beta.session_token,
    }


class WriterGuardGatesTakeoverTests(unittest.TestCase):
    """A live same-identity predecessor keeps its successor out entirely."""

    def test_successor_never_reaches_the_lease_upsert(self) -> None:
        """Not "does not adopt" -- does not *observe*.

        The advisory guard is the only part of the proof that speaks to what
        the predecessor can still do outside the database, so it has to hold
        before any lease statement runs, not merely before the CAS. Asserting
        that no statement reached the server is the difference between a
        guard that gates takeover and one that only delays it.
        """
        with LeaseHarness(**HARNESS_KWARGS) as harness:
            observed = drive_guard_gated_successor(harness)

            self.assertTrue(
                observed["same_advisory_key"],
                "two coordinators at the same (writer_id, writer_epoch) must "
                "contend for one advisory key, or the guard proves nothing",
            )
            self.assertTrue(observed["predecessor_guard_held"])
            self.assertFalse(observed["successor_guard_held"])

            self.assertEqual(
                observed["statements_after"],
                observed["statements_before"],
                "the gated successor must not have executed any lease "
                "statement; it is held off before the upsert, not at it",
            )
            self.assertEqual(observed["statements_after"], ["acquire"])
            self.assertNotIn(
                "beta.begin:acquire",
                observed["successor_stops"],
                "reaching begin:acquire would mean the successor observed the "
                "lease row while its predecessor was still able to act",
            )
            self.assertEqual(
                observed["successor_stops"],
                [f"sleep:{GUARD_SPIN_SLEEP_SECONDS:g}"] * GUARD_SPIN_LAPS,
            )
            self.assertEqual(
                observed["sleeps"],
                [("beta", GUARD_SPIN_SLEEP_SECONDS)] * GUARD_SPIN_LAPS,
            )
            self.assertFalse(
                observed["successor_started"],
                "PsqlShareLedger.__init__ has not returned",
            )

    def test_guard_contention_is_reported_once_not_once_per_retry(self) -> None:
        """The spin is unbounded; the log line must not be.

        ``_initialize_writer_lease_guard`` retries every 0.25s for as long as
        the predecessor lives, so a per-attempt warning would emit four lines
        a second for the whole of a healthy coordinator's life and bury the
        one line an operator needs. The ``warned`` latch is the behaviour, and
        a spin of several laps producing one line is the only way to see it.
        """
        with LeaseHarness(**HARNESS_KWARGS) as harness:
            observed = drive_guard_gated_successor(harness)

            self.assertEqual(len(observed["sleeps"]), GUARD_SPIN_LAPS)
            self.assertEqual(observed["warning_count"], 1)

    def test_releasing_the_guard_lets_the_gated_successor_proceed(self) -> None:
        """Control: the successor is held by the guard, not by the schedule.

        Without this, the parked state above is equally consistent with a
        scenario that simply never gave the successor a chance to run. Closing
        the predecessor's guard and doing nothing else must be enough to carry
        the same successor all the way through adoption.
        """
        with LeaseHarness(**HARNESS_KWARGS) as harness:
            drive_guard_gated_successor(harness)
            alpha = harness.coordinators["alpha"]
            beta = harness.coordinators["beta"]

            alpha.guard.close()

            harness.advance_to_next_deadline()
            harness.run_until_blocked(
                beta, f"sleep:{ADOPTION_SILENCE_SECONDS:g}"
            )
            self.assertTrue(
                beta.guard.held,
                "the successor took the guard on its next lap",
            )
            harness.advance_to_next_deadline()
            harness.run_until(beta, "done:startup")

            self.assertTrue(beta.started)
            self.assertEqual(
                harness.lease_holder_session(),
                beta.session_token,
            )


class AdoptionSilenceIsMeasuredFromGuardAcquisitionTests(unittest.TestCase):
    """The guard edge, not the row edge, is what bounds the wait."""

    def test_successor_waits_a_full_interval_after_taking_the_guard(self) -> None:
        """A row that is already minutes stale buys the successor nothing.

        This is the case the two-edge rule exists for. The predecessor's
        ``updated_at`` is ``PREDECESSOR_SILENT_SECONDS`` old -- far past the
        silence interval -- when the successor takes the guard, so the row
        edge contributes a wait of zero. If the row edge were the only one,
        the successor would adopt on its first observation and the dying
        predecessor would get none of its heartbeat failure budget to
        self-fence. The wait must be a full interval measured from this
        process's own guard acquisition instead.
        """
        with LeaseHarness(**HARNESS_KWARGS) as harness:
            observed = drive_fast_adoption(harness)

            self.assertGreater(
                observed["row_age_at_first_observation"],
                ADOPTION_SILENCE_SECONDS,
                "the row must already be staler than the silence interval, or "
                "this scenario is not testing the guard edge at all",
            )
            self.assertEqual(
                observed["row_age_at_first_observation"],
                PREDECESSOR_SILENT_SECONDS,
            )
            self.assertEqual(
                observed["sleeps"],
                [("beta", ADOPTION_SILENCE_SECONDS)],
                "exactly one wait, of exactly one full silence interval",
            )
            self.assertEqual(
                observed["waited_seconds"],
                ADOPTION_SILENCE_SECONDS,
            )

    def test_observation_precedes_the_cas_by_a_whole_interval(self) -> None:
        """The statement order is the executable form of the arithmetic.

        A wait is only a fence if nothing was written during it. The successor
        must observe, wait, re-observe, and only then run the CAS -- so every
        statement before the first ``adopt`` is a read-only ``acquire``
        attempt, and there are two of them: one either side of the interval.
        """
        with LeaseHarness(**HARNESS_KWARGS) as harness:
            observed = drive_fast_adoption(harness)

            self.assertEqual(
                observed["statements_at_first_observation"],
                ["acquire", "acquire"],
                "the predecessor's startup acquire, then the successor's "
                "first observation -- and no adopt",
            )
            self.assertEqual(
                observed["statements"],
                ["acquire", "acquire", "acquire", "adopt"],
            )
            statements = observed["statements"]
            self.assertEqual(
                set(statements[: statements.index("adopt")]),
                {"acquire"},
            )

    def test_adoption_schedule_is_order_stable_across_repeated_runs(self) -> None:
        """The wait is a property of the schedule, so pin the schedule.

        Everything this class asserts is a claim about *when* statements ran
        relative to one another. That claim is only worth as much as the
        reproducibility of the interleaving that produced it.
        """
        run = assert_deterministic(
            self,
            drive_fast_adoption,
            repeats=25,
            **HARNESS_KWARGS,
        )

        self.assertEqual(
            list(run.trace),
            [
                "alpha@alpha.begin:acquire",
                "alpha@alpha.done:acquire",
                # Since #123 the acquire/adopt statements carry an acquisition
                # deadline, so each commits explicitly and offers a precommit stop.
                "alpha@alpha.precommit",
                "alpha@done:startup",
                "beta@beta.begin:acquire",
                "beta@beta.done:acquire",
                "beta@beta.precommit",
                "beta@sleep:5",
                "beta@beta.begin:acquire",
                "beta@beta.done:acquire",
                "beta@beta.precommit",
                "beta@beta.begin:adopt",
                "beta@beta.done:adopt",
                "beta@beta.precommit",
                "beta@done:startup",
            ],
        )


class AdoptionRewritesTheSessionTokenTests(unittest.TestCase):
    """What a won CAS leaves behind, and what it takes away."""

    def test_adoption_installs_the_successor_for_a_full_ttl(self) -> None:
        with LeaseHarness(**HARNESS_KWARGS) as harness:
            observed = drive_fast_adoption(harness)

            self.assertEqual(
                observed["session_token"],
                observed["successor_token"],
            )
            self.assertNotEqual(
                observed["session_token"],
                observed["predecessor_token"],
            )
            self.assertEqual(
                observed["ttl_ahead_seconds"],
                LEASE_TTL_SECONDS,
                "adoption installs a full fresh TTL, not the remainder of the "
                "predecessor's",
            )
            self.assertTrue(observed["adoption_announced"])
            self.assertIn(
                "prism ledger writer lease adopted from same-identity "
                "predecessor session=heartbeat-v1:alpha-1",
                harness.output,
            )

    def test_predecessor_is_fenced_by_the_rewritten_token(self) -> None:
        """The CAS is the fence; nothing tells the predecessor it lost.

        A replaced predecessor is still a running process holding a ledger
        object that believes it owns the lease. Its renewal must fail the same
        way a fenced write does, so it fails fast rather than continuing to
        act, and its release must decline to touch a row it no longer owns --
        expiring a successor's live lease would open the window adoption just
        closed.
        """
        with LeaseHarness(**HARNESS_KWARGS) as harness:
            drive_fast_adoption(harness)
            alpha = harness.coordinators["alpha"]
            beta = harness.coordinators["beta"]

            with self.assertRaisesRegex(
                RuntimeError,
                "writer lease is not active",
            ):
                alpha.call(alpha.ledger.renew_writer_lease, label="renew")

            self.assertFalse(
                alpha.call(alpha.ledger.release_writer_lease, label="release"),
                "a lease already reassigned must not be expired by its "
                "predecessor",
            )
            self.assertEqual(
                harness.lease_holder_session(),
                beta.session_token,
            )
            row = harness.lease_row()
            self.assertGreater(row.lease_expires_at, harness.clock.now())
            self.assertEqual(
                harness.statement_kinds(),
                ["acquire", "acquire", "acquire", "adopt", "renew", "release"],
                "both fenced calls really did reach the server and were "
                "refused by the row, not short-circuited in the client",
            )


class LostAdoptionCasTests(unittest.TestCase):
    """A CAS loss re-observes; it never refuses the token permanently."""

    def test_lost_cas_re_observes_after_a_fresh_silence_interval(self) -> None:
        """Neither give up nor retry immediately -- both are unsafe.

        The renewal that beat the CAS proves the predecessor was alive
        moments ago, so retrying straight away would adopt from a process
        that had just demonstrated it can still act. Giving up instead would
        recreate the whole-TTL outage fast adoption exists to avoid, because
        that renewal may equally have been the predecessor's final act. The
        comment in ``_ensure_writer_lease`` picks the third option, and this
        is it: re-observe, and require a fresh full silence interval -- which
        the renewal's own ``updated_at`` now supplies through the row edge.
        """
        with LeaseHarness(**HARNESS_KWARGS) as harness:
            observed = drive_lost_adoption_cas(harness)

            self.assertEqual(observed["stop_before_cas"], "beta.begin:adopt")
            self.assertEqual(observed["renewal"]["renewed_count"], 1)

            self.assertEqual(
                observed["statements_after_lost_cas"],
                [
                    "acquire",  # predecessor startup
                    "acquire",  # successor's first observation
                    "acquire",  # successor re-observes after the guard wait
                    "renew",    # predecessor's renewal wins the tuple lock
                    "adopt",    # the CAS, now against a moved updated_at
                ],
                "the renewal must land between the observation and the CAS, "
                "or the CAS was never actually contested",
            )
            self.assertEqual(
                observed["sleeps_after_lost_cas"],
                [
                    ("beta", ADOPTION_SILENCE_SECONDS),
                    ("beta", ADOPTION_SILENCE_SECONDS),
                ],
                "a second full interval, not a shortened one and not none",
            )
            self.assertEqual(
                observed["waited_after_lost_cas"],
                ADOPTION_SILENCE_SECONDS,
            )

            self.assertEqual(
                observed["statements"],
                [
                    "acquire",
                    "acquire",
                    "acquire",
                    "renew",
                    "adopt",
                    "acquire",
                    "adopt",
                ],
                "re-observe before the second CAS: adopting on the first "
                "observation's token would be adopting on stale evidence",
            )
            self.assertEqual(
                observed["session_token"],
                observed["successor_token"],
                "the successor still ends up holding the lease",
            )

    def test_interleaving_is_order_stable_across_repeated_runs(self) -> None:
        """A flaky concurrency test is worse than none; prove this one is not."""
        run = assert_deterministic(
            self,
            drive_lost_adoption_cas,
            repeats=25,
            **HARNESS_KWARGS,
        )

        self.assertEqual(
            list(run.trace),
            [
                "alpha@alpha.begin:acquire",
                "alpha@alpha.done:acquire",
                "alpha@alpha.precommit",
                "alpha@done:startup",
                "beta@beta.begin:acquire",
                "beta@beta.done:acquire",
                "beta@beta.precommit",
                "beta@sleep:5",
                "beta@beta.begin:acquire",
                "beta@beta.done:acquire",
                "alpha@alpha.begin:renew",
                "alpha@alpha.lockwait:renew",
                "beta@beta.precommit",
                "beta@beta.begin:adopt",
                "alpha@alpha.done:renew",
                "alpha@done:renew",
                "beta@beta.done:adopt",
                "beta@beta.precommit",
                "beta@sleep:5",
                "beta@beta.begin:acquire",
                "beta@beta.done:acquire",
                "beta@beta.precommit",
                "beta@beta.begin:adopt",
                "beta@beta.done:adopt",
                "beta@beta.precommit",
                "beta@done:startup",
            ],
        )


class WriterEpochFencingTests(unittest.TestCase):
    """A different epoch gets the TTL and nothing else."""

    def test_different_epoch_refuses_an_unexpired_lease(self) -> None:
        """Refusing loudly is the only safe answer, and it must be immediate.

        A different epoch hashes to a different advisory key, so it is not
        held off by the holder's guard and does reach the lease upsert -- and
        having reached it, it can prove nothing at all about whether the
        holder is alive. ``_can_wait_for_writer_lease`` therefore requires an
        exact identity match, which makes every different-epoch outcome
        against a live lease a raise rather than a wait or a CAS.
        """
        with LeaseHarness(**HARNESS_KWARGS) as harness:
            alpha = start_predecessor(harness)

            gamma = harness.coordinator("gamma", writer_epoch=2)
            gamma.start()
            harness.run_until(gamma, "done:startup")

            self.assertNotEqual(
                gamma.guard.advisory_lock_key,
                alpha.guard.advisory_lock_key,
                "different epochs must not share an advisory key, or this "
                "coordinator would have been gated instead of refused",
            )
            self.assertIn(
                "gamma@gamma.begin:acquire",
                harness.trace,
                "the different-epoch guard was acquired and the upsert was "
                "reached, so the refusal comes from the identity check and "
                "not from guard contention",
            )
            # `PsqlShareLedger.__init__` closes what it opened when it raises.
            # A refused claimant that kept its advisory lock would gate the
            # legitimate holder's own successor for the life of the process.
            self.assertFalse(gamma.guard.held)
            self.assertFalse(
                gamma.started,
                "PsqlShareLedger.__init__ must not return",
            )
            with self.assertRaisesRegex(
                RuntimeError,
                "qbit ledger writer lease is held by prism-coordinator epoch=1",
            ):
                gamma.actor.calls[0].value()

            self.assertEqual(
                harness.statement_kinds(),
                ["acquire", "acquire"],
                "one observation and no adopt: fast adoption is unreachable "
                "across epochs",
            )
            self.assertEqual(harness.sleeps, [])
            self.assertEqual(
                harness.lease_holder_session(),
                alpha.session_token,
            )

    def test_different_epoch_takes_an_expired_lease_immediately(self) -> None:
        """The expiry CAS is the different-epoch path, and it does not wait."""
        with LeaseHarness(**HARNESS_KWARGS) as harness:
            alpha = start_predecessor(harness)

            gamma = harness.coordinator("gamma", writer_epoch=2)
            gamma.start()
            harness.run_until(gamma, "done:startup")
            self.assertIsNotNone(gamma.actor.calls[0].error)

            row = harness.lease_row()
            remaining = (
                row.lease_expires_at - harness.clock.now()
            ).total_seconds()
            self.assertEqual(remaining, LEASE_TTL_SECONDS)
            harness.advance(remaining + 1.0)

            delta = harness.coordinator("delta", writer_epoch=2)
            delta.start()
            harness.run_until(delta, "done:startup")

            self.assertTrue(delta.started)
            adopted = harness.lease_row()
            self.assertEqual(adopted.writer_epoch, 2)
            self.assertEqual(adopted.writer_id, alpha.writer_id)
            self.assertEqual(
                adopted.writer_session_token,
                delta.session_token,
            )
            self.assertEqual(
                harness.statement_kinds(),
                ["acquire", "acquire", "acquire"],
                "the expiry CAS lands on the first attempt; no adopt "
                "statement is involved",
            )
            self.assertEqual(
                harness.sleeps,
                [],
                "an expired lease is taken without waiting at all",
            )
            self.assertEqual(
                adopted.lease_expires_at - harness.clock.now(),
                row.lease_expires_at - row.updated_at,
                "the claimant installs a full fresh TTL",
            )


if __name__ == "__main__":
    unittest.main()
