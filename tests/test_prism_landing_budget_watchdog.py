#!/usr/bin/env python3
"""One landing step's budget against the watchdog that is timing it.

Issue #125: a landing-class ledger step is two waits, not one. The caller
first queues for the ledger's writer lock -- no statement has been sent, so
neither ``statement_timeout`` nor any server-side cancellation bounds it --
and only then runs the statement it was queuing for, under its own full
budget. Both are spent on the block-work thread the watchdog monitors, and
before ``49f04c3`` both were heartbeat-silent. At the reviewed 120s
escalation cap that is ~240s of silence against a 120s tolerance: the
watchdog hard-exits mid-landing, the in-memory escalation counter dies with
the process, and the restart comes back at the 30s base to ride the same
cycle again. No accounting is lost -- the outbox row stays pending and
replays -- but found-block settlement is deferred behind a restart loop.

``49f04c3`` answers it in two halves, and these scenarios are shaped around
them:

* the admission wait is served in slices whenever a progress hook is
  installed, so a queued landing stamps its heartbeat instead of going
  quiet -- while the caller's own deadline stays the sole authority on when
  the wait ends;
* every configured landing budget, base and escalated cap alike, is clamped
  to a fraction of the watchdog tolerance, so admission plus statement
  cannot outrun the monitor timing them.

These pass on ``2.x.x``. Their evidence is not that the fix is correct --
its own PR argued that against unit-level doubles -- but that the
interleaving #125 describes can now be *driven*: a real landing tail queuing
on the real ledger writer lock, escalating its real budget across real
timed-out attempts, on a clock that moves only when this file says so.
"""

from __future__ import annotations

import sys
import threading
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.prism_concurrency_harness import Actor, assert_deterministic  # noqa: E402
from tests.prism_landing_harness import (  # noqa: E402
    PARENT_HASH,
    LandingHarness,
)

BLOCK_A = "aa" * 32
BLOCK_B = "bb" * 32

# The landing names its own boundaries; these are the two it names that this
# file turns on. ``phase:accounting`` is the last stamp before the accounting
# tail opens its landing-class budget scope, so an actor parked there is
# parked exactly one step short of the wait #125 is about.
# ``wait-ledger-admission:landing`` is the stamp the fix added *inside* that
# wait -- arming it turns every heartbeat into a checkpoint, which makes the
# stamps part of the order-stable trace rather than something counted on the
# side.
LANDING_SCOPE = "phase:accounting"
ADMISSION_STAMP = "phase:wait-ledger-admission:landing"

WRITER_LOCK_HELD = "writer-lock-held"
CLAMP_LINE = "prism coordinator: landing db budget clamped by watchdog"
GATE_TIMEOUT = "timed out waiting for postgres writer lock"


# --------------------------------------------------------------------------
# Driving
# --------------------------------------------------------------------------


def _own_block_work(harness: LandingHarness) -> None:
    """Let the accounting actor stand in for the block_accounting thread.

    Block-work stamps are gated to the two threads that own a heartbeat slot
    (``_block_work_heartbeat_owner``), which is the whole reason a client
    connection thread cannot refresh the accounting thread's liveness budget
    on its behalf. ``block_accounting_loop`` claims that slot by stamping its
    own ident; the harness dequeues nothing and drives
    ``_run_block_accounting_task`` directly, so the claim has to be made
    here. Without it the admission stamps still fire -- the fix does not
    depend on ownership -- but they land on the unowned fallback slot with no
    phase attached, which is not the shape production runs and not the shape
    #125 describes.
    """
    service = harness.pool._ensure_block_candidate_service()

    def claim() -> None:
        service._block_accounting_thread_ident = threading.get_ident()

    harness.accounting.submit(claim, label="own-block-work")
    harness.run_until(harness.accounting, "done:own-block-work")


def _pump(
    harness: LandingHarness,
    actor: Actor,
    label: str,
    *,
    on_stop: Callable[[str, float], None] | None = None,
    limit: int = 20_000,
) -> None:
    """Step ``actor`` to ``label``, moving virtual time when it waits on it.

    ``run_until`` cannot drive a sliced wait: an actor parked on a slice
    deadline is not runnable until the clock reaches it, and only the
    controller can move a virtual clock. Advancing to the *next* pending
    deadline rather than by a fixed step is what keeps the schedule a
    function of the scenario -- every second that passes here is a second
    some deadline in the code under test asked for, and an actor parked with
    no deadline at all is reported rather than waited on, because nothing
    would ever release it.
    """
    for _ in range(limit):
        if not actor.runnable():
            if harness.advance_to_next_deadline() is None:
                raise AssertionError(
                    f"actor {actor.name!r} is parked at {actor.stop!r} with no "
                    "deadline; nothing bounds this wait"
                )
            continue
        stop = harness.step(actor)
        if on_stop is not None:
            on_stop(stop, harness.clock.monotonic())
        if stop == label:
            return
    raise AssertionError(
        f"actor {actor.name!r} did not reach {label!r} within {limit} steps; "
        f"last stop {actor.stop!r}"
    )


def _hold_writer_lock(harness: LandingHarness) -> None:
    """Park the client actor holding the ledger's writer lock.

    A landing queues on admission only if something else is already inside
    the gate. The client tail is the honest holder: in production it is the
    other thread that lands blocks, and a share append on it takes the same
    lock. Parking it at a checkpoint rather than blocking it means the
    controller decides exactly when the queue clears.
    """
    lock = harness.pool.ledger._lock

    def hold() -> None:
        lock.acquire()
        harness.scheduler.checkpoint(WRITER_LOCK_HELD)
        lock.release()

    harness.client.submit(hold, label="hold-writer-lock")
    harness.run_until(harness.client, WRITER_LOCK_HELD)


def _release_writer_lock(harness: LandingHarness) -> None:
    harness.run_until(harness.client, "done:hold-writer-lock")


def _granted_budget(harness: LandingHarness, block_hash: str | None) -> float:
    """What production would actually grant this hash's next landing step.

    Read through the coordinator rather than recomputed here, because the
    clamp arithmetic is the thing under test; a second copy of it in the
    test would agree with itself and prove nothing. It runs on an actor
    because it takes the coordinator lock, which is a harness lock.
    """
    call = harness.accounting.submit(
        lambda: harness.pool._block_landing_db_timeout(block_hash),
        label="landing-budget",
    )
    harness.run_until(harness.accounting, "done:landing-budget")
    return float(call.value())


def _wait_slice(harness: LandingHarness) -> float:
    service = harness.pool._ensure_block_candidate_service()
    return float(service._block_work_wait_slice())


def _timeout_count(harness: LandingHarness, block_hash: str) -> int:
    service = harness.pool._ensure_block_candidate_service()
    counts = getattr(service, "_block_landing_timeout_counts", None) or {}
    return int(counts.get(block_hash, 0))


def _land_while_queued(
    harness: LandingHarness,
    found: Any,
    *,
    release_after_stamps: int | None = None,
) -> dict[str, Any]:
    """Run one accounting-tail landing attempt against a held writer lock.

    The tail is stopped at the last boundary before its landing-class scope
    opens, the lock is taken there, and the tail is then let go: everything
    from that point on -- the admission wait, its stamps, the deadline that
    ends it -- is the real landing running under its real budget.

    ``release_after_stamps`` hands the lock back mid-wait, which is the only
    way to get a landing that queues *and then still runs*; leaving it held
    can only ever produce the timing-out half of the story.
    """
    harness.break_at(LANDING_SCOPE)
    call = harness.land_on_accounting_tail(found)
    harness.run_until(harness.accounting, LANDING_SCOPE)
    started = harness.clock.monotonic()
    _hold_writer_lock(harness)

    stamps: list[float] = []
    after_admission: list[float] = []
    queued = True

    def observe(stop: str, when: float) -> None:
        nonlocal queued
        if stop != ADMISSION_STAMP:
            return
        # The same phase covers both reports the fix added: the slices of an
        # admission wait, and the round trip a gate-holding step completes
        # between two statements it cannot release the gate between. Which
        # one a stamp is depends only on whether this landing is still
        # outside the gate, so they are split on that rather than the label.
        if not queued:
            after_admission.append(when)
            return
        stamps.append(when)
        if release_after_stamps is not None and len(stamps) == release_after_stamps:
            _release_writer_lock(harness)
            queued = False

    _pump(
        harness,
        harness.accounting,
        f"done:accounting-land:{found.block_hash[:4]}",
        on_stop=observe,
    )
    finished = harness.clock.monotonic()
    # Admission ends when the lock is handed over -- the last stamp taken
    # while it was still shut -- or, when it never is, at the deadline one
    # slice past the last stamp. Nothing else in a landing consumes virtual
    # time, so the tail's own completion pins the timing-out case exactly.
    admitted_at = stamps[-1] if release_after_stamps is not None else finished
    return {
        "started": started,
        "stamps": stamps,
        "after_admission": after_admission,
        "admitted_at": admitted_at,
        "finished": finished,
        "admission_seconds": admitted_at - started,
        "call": call,
    }


def _silent_gaps(started: float, stamps: list[float], ended: float) -> list[float]:
    """Every interval the block-work heartbeat went unstamped, in order."""
    marks = [started, *stamps, ended]
    return [round(later - earlier, 6) for earlier, later in zip(marks, marks[1:])]


def _prepared_harness(
    harness: LandingHarness,
    *,
    configured_max_seconds: float,
) -> None:
    """Boot, claim the block-work slot, and make every admission stamp visible.

    ``block_landing_db_timeout_max_seconds`` is the escalation cap an
    operator configures; ``LandingHarness`` exposes the base budget and the
    watchdog tolerance as constructor arguments but not the cap, so it is
    set here before anything can read it.
    """
    harness.pool.block_landing_db_timeout_max_seconds = float(configured_max_seconds)
    harness.boot()
    _own_block_work(harness)
    harness.break_at(ADMISSION_STAMP)


# --------------------------------------------------------------------------
# 1. The admission wait is no longer silent
# --------------------------------------------------------------------------


def _stamped_admission_wait(harness: LandingHarness) -> dict[str, Any]:
    """A landing that queues on the writer lock until its budget runs out."""
    _prepared_harness(harness, configured_max_seconds=120.0)
    found = harness.found_block(BLOCK_A, height=10, parent_hash=PARENT_HASH)
    budget = _granted_budget(harness, BLOCK_A)

    attempt = _land_while_queued(harness, found)
    _release_writer_lock(harness)

    return {
        "budget": budget,
        "slice": _wait_slice(harness),
        "stamps": len(attempt["stamps"]),
        "waited": round(attempt["admission_seconds"], 6),
        "silent_gaps": sorted(
            set(
                _silent_gaps(
                    attempt["started"],
                    attempt["stamps"],
                    attempt["admitted_at"],
                )
            )
        ),
        # The tail swallows the timeout and leaves the row for replay, which
        # is the behaviour #125 says keeps accounting correct. The gate error
        # itself reaches stderr with the traceback.
        "tail_error": (
            type(attempt["call"].error).__name__ if attempt["call"].error else None
        ),
        "gate_timed_out": GATE_TIMEOUT in harness.errors,
        "escalation_armed": _timeout_count(harness, BLOCK_A),
        "outbox": harness.outbox_state(found),
        "summary": harness.landing_summary(),
    }


def _sliced_and_unsliced_wait(harness: LandingHarness) -> dict[str, Any]:
    """The same gate, the same budget, once with the hook and once without.

    Slicing is only safe if it is invisible to the caller's deadline, so the
    claim needs a control rather than an argument: one landing-class step
    under the production scope that installs the hook, and one bare
    ``statement_timeout`` at the same budget, both queued behind the same
    held lock on the same actor. Anything the slicing changed about *when*
    the wait ends would show up as a difference between the two.
    """
    _prepared_harness(harness, configured_max_seconds=120.0)
    ledger = harness.pool.ledger
    budget = _granted_budget(harness, BLOCK_B)
    _hold_writer_lock(harness)

    def queue(scope: Callable[[], Any], label: str) -> dict[str, Any]:
        def run() -> None:
            with scope():
                # A landing-class read that takes the writer lock, so the
                # wait under test is the ledger's own admission gate rather
                # than something the harness invented.
                ledger.current_prior_balances()

        call = harness.accounting.submit(run, label=label)
        started = harness.clock.monotonic()
        stamps: list[float] = []
        _pump(
            harness,
            harness.accounting,
            f"done:{label}",
            on_stop=lambda stop, when: (
                stamps.append(when) if stop == ADMISSION_STAMP else None
            ),
        )
        return {
            "stamps": len(stamps),
            "waited": round(harness.clock.monotonic() - started, 6),
            "error": type(call.error).__name__ if call.error else None,
            "message": str(call.error) if call.error else None,
        }

    hooked = queue(
        lambda: harness.pool._block_landing_ledger_statement_timeout_scope(BLOCK_B),
        "hooked-admission",
    )
    unhooked = queue(lambda: ledger.statement_timeout(budget), "unhooked-admission")
    _release_writer_lock(harness)

    return {"budget": budget, "hooked": hooked, "unhooked": unhooked}


class AdmissionWaitStampTests(unittest.TestCase):
    """#125, first half: a queued landing must not go quiet."""

    def test_a_queued_landing_stamps_its_heartbeat_while_it_waits(self) -> None:
        outcome = assert_deterministic(
            self,
            _stamped_admission_wait,
            harness_factory=LandingHarness,
        ).outcome
        assert isinstance(outcome, dict)

        # The default deployment: a 120s watchdog grants 30s to a first
        # landing attempt, and the wait is served in watchdog-sized slices.
        self.assertEqual(outcome["budget"], 30.0)
        self.assertEqual(outcome["slice"], 0.25)

        # The assertion the fix exists for. Before it, this wait was one
        # unbroken silence; now every slice boundary reports liveness, and
        # the longest gap the watchdog could observe is one slice.
        self.assertEqual(outcome["stamps"], 119)
        self.assertEqual(outcome["silent_gaps"], [0.25])

        # ...and the slicing bought that without lengthening the wait: the
        # caller's own deadline still ends it, to the microsecond.
        self.assertEqual(outcome["waited"], 30.0)

        # The landing failed the way #125 says it does: the gate deadline
        # fires, the tail keeps the durable row for replay, and the next
        # attempt for this hash is armed to escalate.
        self.assertTrue(outcome["gate_timed_out"])
        self.assertIsNone(outcome["tail_error"])
        self.assertEqual(outcome["outbox"], "pending")
        self.assertEqual(outcome["escalation_armed"], 1)

    def test_slicing_the_wait_does_not_lengthen_it(self) -> None:
        outcome = assert_deterministic(
            self,
            _sliced_and_unsliced_wait,
            harness_factory=LandingHarness,
        ).outcome
        assert isinstance(outcome, dict)

        hooked = outcome["hooked"]
        unhooked = outcome["unhooked"]

        # One wait reports 119 times, the other not once...
        self.assertEqual(hooked["stamps"], 119)
        self.assertEqual(unhooked["stamps"], 0)

        # ...and they end at the same instant, with the same error. The
        # deadline is the authority; the hook only decides how loudly the
        # caller waits for it.
        self.assertEqual(hooked["waited"], outcome["budget"])
        self.assertEqual(unhooked["waited"], outcome["budget"])
        self.assertEqual(hooked["error"], "LedgerOperationTimeout")
        self.assertEqual(unhooked["error"], "LedgerOperationTimeout")
        self.assertEqual(hooked["message"], unhooked["message"])
        self.assertIn(GATE_TIMEOUT, hooked["message"])


# --------------------------------------------------------------------------
# 2. The escalation ladder cannot outrun the watchdog
# --------------------------------------------------------------------------


def _escalation_ladder(
    harness: LandingHarness,
    *,
    configured_max_seconds: float,
    attempts: int,
) -> dict[str, Any]:
    """Time out ``attempts`` landings for one hash and read the ladder off.

    The escalation counter is keyed by block hash and moved only by an
    observed landing-class timeout, so the ladder is driven the way
    production climbs it -- one genuinely timed-out attempt per rung --
    rather than by writing the counter.
    """
    _prepared_harness(harness, configured_max_seconds=configured_max_seconds)
    found = harness.found_block(BLOCK_A, height=10, parent_hash=PARENT_HASH)
    watchdog = float(harness.pool.watchdog_timeout_seconds)

    rungs: list[dict[str, Any]] = []
    for _ in range(attempts):
        granted = _granted_budget(harness, BLOCK_A)
        attempt = _land_while_queued(harness, found)
        _release_writer_lock(harness)
        rungs.append(
            {
                "granted": granted,
                "waited": round(attempt["admission_seconds"], 6),
                "stamps": len(attempt["stamps"]),
                # #125's arithmetic: admission and statement each receive a
                # full budget, so this is what one landing step can cost the
                # heartbeat before the watchdog is entitled to give up.
                "admission_plus_statement": 2.0 * granted,
                "fits_watchdog": 2.0 * granted <= watchdog,
            }
        )

    return {
        "watchdog": watchdog,
        "configured_base": float(harness.pool.block_landing_db_timeout_seconds),
        "configured_max": float(harness.pool.block_landing_db_timeout_max_seconds),
        "rungs": rungs,
        "clamped": harness.output.count(CLAMP_LINE),
        "escalations": _timeout_count(harness, BLOCK_A),
        "outbox": harness.outbox_state(found),
    }


# ``assert_deterministic`` takes a one-argument scenario, so each
# configuration under test is a named binding of the ladder above rather
# than a parameter the test method passes in.


def _clamped_cap_ladder(harness: LandingHarness) -> dict[str, Any]:
    return _escalation_ladder(harness, configured_max_seconds=120.0, attempts=3)


def _clamped_base_ladder(harness: LandingHarness) -> dict[str, Any]:
    return _escalation_ladder(harness, configured_max_seconds=120.0, attempts=2)


def _unclamped_ladder(harness: LandingHarness) -> dict[str, Any]:
    return _escalation_ladder(harness, configured_max_seconds=60.0, attempts=3)


class EscalatedBudgetCeilingTests(unittest.TestCase):
    """#125, second half: escalation stops below the watchdog, and says so."""

    def test_the_reviewed_cap_is_clamped_to_the_default_watchdog(self) -> None:
        """The exact deployment #125 was reported against.

        A 120s tolerance with the reviewed 120s escalation cap is the
        configuration that produces the ~240s heartbeat gap: two full
        budgets, one monitor, and no slack at all between them.
        """
        outcome = assert_deterministic(
            self,
            _clamped_cap_ladder,
            harness_factory=LandingHarness,
        ).outcome
        assert isinstance(outcome, dict)

        self.assertEqual(outcome["watchdog"], 120.0)
        self.assertEqual(outcome["configured_base"], 30.0)
        self.assertEqual(outcome["configured_max"], 120.0)

        # What #125 measured: the configured cap alone is the whole watchdog
        # tolerance, so admission plus statement is twice it.
        self.assertGreater(2.0 * outcome["configured_max"], outcome["watchdog"])

        granted = [rung["granted"] for rung in outcome["rungs"]]
        self.assertEqual(granted, [30.0, 60.0, 60.0])

        # Every rung, including the exhausted one, leaves the watchdog able
        # to tell a slow landing from a wedged one.
        for rung in outcome["rungs"]:
            self.assertTrue(rung["fits_watchdog"], rung)
            self.assertLessEqual(rung["admission_plus_statement"], outcome["watchdog"])

        # The ladder was climbed by real timed-out landings, not asserted:
        # each rung spent its whole granted budget queuing, and stamped
        # every slice of it.
        self.assertEqual([rung["waited"] for rung in outcome["rungs"]], granted)
        self.assertEqual([rung["stamps"] for rung in outcome["rungs"]], [119, 239, 239])
        self.assertEqual(outcome["escalations"], 3)
        self.assertEqual(outcome["outbox"], "pending")

        # A reduced budget is never silent -- and it is said once, not on
        # every budget read, of which this scenario made several per rung.
        self.assertEqual(outcome["clamped"], 1)

    def test_a_tight_watchdog_clamps_the_base_budget_too(self) -> None:
        """The ceiling is not a cap-only rule.

        An operator who tightens the watchdog without touching the landing
        budgets moves the ceiling below the *base*, and the first attempt --
        the one that never escalated -- is already the one that would outrun
        the monitor. Escalation then has nowhere to climb, which is the
        correct answer: a budget that cannot be spent safely is not a budget.
        """
        outcome = assert_deterministic(
            self,
            _clamped_base_ladder,
            harness_factory=LandingHarness,
            watchdog_timeout_seconds=40.0,
        ).outcome
        assert isinstance(outcome, dict)

        self.assertEqual(outcome["watchdog"], 40.0)
        self.assertEqual(outcome["configured_base"], 30.0)
        self.assertGreater(2.0 * outcome["configured_base"], outcome["watchdog"])

        granted = [rung["granted"] for rung in outcome["rungs"]]
        self.assertEqual(granted, [20.0, 20.0])
        for rung in outcome["rungs"]:
            self.assertTrue(rung["fits_watchdog"], rung)

        self.assertEqual([rung["waited"] for rung in outcome["rungs"]], granted)
        self.assertEqual([rung["stamps"] for rung in outcome["rungs"]], [79, 79])
        self.assertEqual(outcome["clamped"], 1)

    def test_a_budget_the_watchdog_can_afford_is_left_alone(self) -> None:
        """The clamp is a ceiling, not a policy.

        A 300s tolerance with a 60s cap has room for both halves of a
        landing step, so nothing is reduced and nothing is logged. This is
        the case that keeps the ceiling honest: if it lowered budgets it did
        not need to, the reported clamp would stop meaning anything.
        """
        outcome = assert_deterministic(
            self,
            _unclamped_ladder,
            harness_factory=LandingHarness,
            watchdog_timeout_seconds=300.0,
        ).outcome
        assert isinstance(outcome, dict)

        self.assertEqual(outcome["watchdog"], 300.0)
        self.assertEqual(outcome["configured_max"], 60.0)

        granted = [rung["granted"] for rung in outcome["rungs"]]
        self.assertEqual(granted, [30.0, 60.0, 60.0])
        for rung in outcome["rungs"]:
            self.assertTrue(rung["fits_watchdog"], rung)

        self.assertEqual([rung["waited"] for rung in outcome["rungs"]], granted)
        self.assertEqual(outcome["clamped"], 0)


# --------------------------------------------------------------------------
# 3. Both halves at once
# --------------------------------------------------------------------------


def _queue_then_land_at_the_escalated_budget(
    harness: LandingHarness,
) -> dict[str, Any]:
    """Burn a whole escalated admission budget, then land on what is left.

    #125's worst case is not a landing that times out -- that one merely
    retries. It is a landing that queues for almost everything admission
    will give it and *then* starts a statement with a fresh full budget,
    because that is the pair the watchdog sees back to back. So the first
    attempt is spent to arm the escalation, and the second is handed the
    lock one slice before its own deadline: the longest admission wait that
    still ends in a landing.
    """
    _prepared_harness(harness, configured_max_seconds=120.0)
    found = harness.found_block(BLOCK_A, height=10, parent_hash=PARENT_HASH)
    watchdog = float(harness.pool.watchdog_timeout_seconds)
    slice_seconds = _wait_slice(harness)

    # Rung one: a full base budget spent queuing, which is what escalates.
    base = _granted_budget(harness, BLOCK_A)
    _land_while_queued(harness, found)
    _release_writer_lock(harness)

    # Rung two: the escalated budget, released with one slice to spare.
    escalated = _granted_budget(harness, BLOCK_A)
    attempt = _land_while_queued(
        harness,
        found,
        release_after_stamps=int(escalated / slice_seconds) - 1,
    )

    admission = attempt["admission_seconds"]
    return {
        "watchdog": watchdog,
        "base": base,
        "escalated": escalated,
        "admission": round(admission, 6),
        "stamps": len(attempt["stamps"]),
        "stamps_after_admission": len(attempt["after_admission"]),
        "longest_silence": max(
            _silent_gaps(
                attempt["started"],
                attempt["stamps"],
                attempt["admitted_at"],
            )
        ),
        # The quantity #125 is about: what this one landing step can still
        # cost the block-work heartbeat, counting the admission it actually
        # spent plus the untouched statement budget waiting on the far side
        # of the gate.
        "worst_case": round(admission + escalated, 6),
        "landed": attempt["call"].error is None,
        "outbox": harness.outbox_state(found),
        "sequence": harness.publication_sequence(found),
        "envelope": harness.envelope_written(found),
        "summary": harness.landing_summary(),
    }


class CombinedWorstCaseTests(unittest.TestCase):
    """#125 end to end: queue on admission, then spend the escalated budget."""

    def test_a_queued_escalated_landing_stays_inside_the_watchdog(self) -> None:
        outcome = assert_deterministic(
            self,
            _queue_then_land_at_the_escalated_budget,
            harness_factory=LandingHarness,
        ).outcome
        assert isinstance(outcome, dict)

        self.assertEqual(outcome["base"], 30.0)
        self.assertEqual(outcome["escalated"], 60.0)

        # The landing queued for all but one slice of its escalated budget.
        self.assertEqual(outcome["admission"], 59.75)
        self.assertEqual(outcome["stamps"], 239)

        # Admission plus a fresh statement budget -- the exact pair that read
        # as ~240s of silence at the unclamped cap -- now fits the tolerance.
        self.assertEqual(outcome["worst_case"], 119.75)
        self.assertLessEqual(outcome["worst_case"], outcome["watchdog"])

        # And the watchdog never had to infer any of that: the longest stretch
        # it saw without a stamp was one slice.
        self.assertEqual(outcome["longest_silence"], 0.25)

        # Past the gate the same contract holds one level down. A landing
        # step that must issue two statements without releasing the writer
        # lock reports the first one's completed round trip, so the pair
        # costs the monitor one budget of silence rather than two -- the
        # same doubling #125 found on the other side of the gate.
        self.assertEqual(outcome["stamps_after_admission"], 1)

        # The point of staying alive: the block lands. Nearly the whole
        # escalated budget went to queuing and the landing still completed,
        # published its ordinal and wrote its envelope.
        self.assertTrue(outcome["landed"])
        self.assertEqual(outcome["outbox"], "submitted")
        self.assertEqual(outcome["sequence"], 1)
        self.assertTrue(outcome["envelope"])


if __name__ == "__main__":
    unittest.main()
