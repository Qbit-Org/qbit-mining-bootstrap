#!/usr/bin/env python3
"""The append-invalidation epoch, asked per anchor instead of per counter.

Issue #126: the epoch is one global counter, so a landing that compares its
own baseline against the live value learns only that *somebody's* payout
window was invalidated. A share that becomes durable stamped later than this
candidate's declared anchor -- but early enough to predate some newer exposed
window -- bumps that counter without touching this candidate's window. On the
fallback submit lane, where nothing has been offered yet, the pre-offer fence
read the inequality and terminally abandoned the block as
``append_epoch_stale``: it never reached qbitd at all, and a valid block's
reward was forfeited.

What the fix may not do is forgive a window that genuinely was invalidated,
so these scenarios are written in pairs. The same interleaving, at the same
fence, differs only in the stamp of the appended row: one stamp that cannot
predate the candidate's declared anchor must land, and one that can must
still abandon with nothing submitted. The boundary itself (a stamp exactly
equal to the anchor) is on the terminal side, because
``_pending_share_predates_anchor`` treats an equal stamp as predating.

Both landing fences are covered. The pre-offer one is advisory -- it reads
the epoch under the job-cache lock and releases it -- so its pair drains the
appender before the landing starts. The authoritative one holds
``_payout_append_landing_fence_lock`` across the read and ``submitblock``, so
its pair parks the accounting tail at ``phase:tip-height-rpc`` (past the
advisory read, before the fence arms) and lets the appender commit its bump
into exactly that gap.

The two fail-closed cases that have nothing to do with stamps are here too:
a baseline epoch older than the retained stamp history cannot be answered,
and neither can a candidate that declares no anchor. Both abandon, which is
what the bare counter did for every candidate.

The scoped predicate reads recorded bumps, so it is only as good as the
guarantee that every predating append bumps. The disarm-gap scenario pins
that guarantee at its one weak point: a seeded window's anchor is exposed
only by the armed artifact, and the bump that consults it also disarms it,
so without the watermark fold a second append could predate the candidate's
window with the anchor set empty -- no bump, no stamp, and a fence that
reads only harmless history submits an invalidated window.

Finally, the recorded stamp itself is pinned. A row predates an anchor iff
BOTH its stamps do -- iff their max does -- and every equal-stamp scenario
above records the same number under max(), min(), or either stamp alone.
The straddling pair pulls the stamps apart so only max() survives it.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lab.prism.payout_state import (  # noqa: E402
    PRISM_PAYOUT_APPEND_INVALIDATION_STAMP_HISTORY,
)
from tests.prism_concurrency_harness import assert_deterministic  # noqa: E402
from tests.prism_landing_harness import LandingHarness  # noqa: E402

BLOCK = "cc" * 32

# The candidate's own window. Every share stamp below is placed relative to
# this: at or before it invalidates the candidate, after it does not.
ANCHOR_MS = 12_000
# A newer published window, live while this candidate lands. It is what makes
# an append that misses the candidate's window still bump the global epoch --
# without it there would be no defect to scope, because nothing would predate
# a live anchor at all.
NEWER_WINDOW_ANCHOR_MS = 20_000
# Stamped after the candidate's anchor and before the newer window's: it
# invalidates that newer window and nothing of this candidate's.
LATER_THAN_ANCHOR_MS = 15_000
# Stamped inside the candidate's own window, and on its exact boundary.
INSIDE_ANCHOR_MS = 9_000
AT_ANCHOR_MS = ANCHOR_MS
# A newer SEEDED window. Unlike NEWER_WINDOW_ANCHOR_MS it is never handed
# to the published-window watermark: its anchor is exposed only by the
# armed payout-ledger artifact, which the first bump disarms -- the
# precondition for the disarm gap.
NEWER_SEEDED_ANCHOR_MS = 20_000
# The acceptance stamp of a row whose two stamps differ while both sit
# inside the candidate's window.
ALSO_INSIDE_ANCHOR_MS = 11_000

# The last phase the landing names before the fallback lane's authoritative
# fence. Stopping here puts a tail past the advisory pre-offer read, with no
# fence lock held and ``submitblock`` not yet reached -- the one gap in which
# an append-side bump can still overtake a landing that already checked.
BEFORE_AUTHORITATIVE_FENCE = "phase:tip-height-rpc"


def _landing_outcome(harness: LandingHarness, found: Any) -> dict[str, object]:
    """The durable verdict on one candidate, as a comparable digest."""
    row = harness.pool_block(found.block_hash)
    return {
        "submitted": list(harness.rpc.submitted),
        "abandoned": dict(harness.pool.block_candidate_abandoned_counts),
        "stale_classes": dict(
            getattr(harness.pool, "stale_job_abandon_counts", None) or {}
        ),
        "outbox": harness.outbox_state(found),
        "chain_state": None if row is None else row.chain_state,
        "envelope_written": harness.envelope_written(found),
        "live_epoch": harness.live_append_epoch(),
        "summary": harness.landing_summary(),
    }


def _append_then_land(
    harness: LandingHarness,
    *,
    stamp_ms: int,
) -> dict[str, object]:
    """The reproduction from #126, parameterised by the appended stamp.

    The append is fully drained before the landing starts, so the epoch has
    already moved when the pre-offer fence takes its advisory read. Which
    verdict that fence reaches is then a question about ``stamp_ms`` alone.
    """
    harness.boot()
    found = harness.found_block(
        BLOCK,
        height=10,
        anchor_job_issued_at_ms=ANCHOR_MS,
    )
    harness.publish_window_anchor(NEWER_WINDOW_ANCHOR_MS)
    appended = harness.append_late_visible_share(stamp_ms=stamp_ms)
    harness.drain([harness.appender])

    harness.land_on_accounting_tail(found)
    harness.drain([harness.accounting, harness.appender])

    outcome = _landing_outcome(harness, found)
    # Evidence that the premise held: the row really did advance the global
    # epoch. A scenario where it did not would prove nothing about scoping.
    outcome["bumped_to"] = appended.value()
    return outcome


def _land_across_the_authoritative_fence(
    harness: LandingHarness,
    *,
    stamp_ms: int,
) -> dict[str, object]:
    """Commit the bump between the advisory read and the fence-held one.

    The accounting tail parks past the pre-offer fence, which therefore saw
    an unmoved epoch; the appender then commits its bump into that gap and
    the tail resumes into the authoritative fence. ``epoch_at_advisory_read``
    is recorded so a scenario cannot silently degenerate into the drained
    shape above.
    """
    harness.boot()
    harness.break_at(BEFORE_AUTHORITATIVE_FENCE)
    found = harness.found_block(
        BLOCK,
        height=10,
        anchor_job_issued_at_ms=ANCHOR_MS,
    )
    harness.publish_window_anchor(NEWER_WINDOW_ANCHOR_MS)

    harness.land_on_accounting_tail(found)
    harness.run_until(harness.accounting, BEFORE_AUTHORITATIVE_FENCE)
    epoch_at_advisory_read = harness.live_append_epoch()

    appended = harness.append_late_visible_share(stamp_ms=stamp_ms)
    harness.drain([harness.appender])
    harness.drain([harness.accounting, harness.appender])

    outcome = _landing_outcome(harness, found)
    outcome["epoch_at_advisory_read"] = epoch_at_advisory_read
    outcome["bumped_to"] = appended.value()
    return outcome


def _silent_append_after_disarm(harness: LandingHarness) -> dict[str, object]:
    """The disarm gap: a predating append that leaves no bump at all.

    A newer SEEDED window's anchor lives only on the armed artifact --
    deliberately NOT ``publish_window_anchor()``, whose seedless watermark
    is immortal and would hide the gap. The first append predates only that
    newer window, so it bumps; the bump also disarms the artifact, and with
    it the window's only anchor exposure. The second append genuinely
    predates the candidate's declared anchor, but if the disarm emptied the
    anchor set it records nothing: no bump, no stamp, and the landing's
    anchor-scoped fences read only the first bump's harmless stamp and
    submit a window a durable share invalidated. The fold of the armed
    artifact's anchor into the published-window watermark is what closes
    this: the second append still finds an anchor to predate and leaves the
    stamp the fences then refuse.
    """
    harness.boot()
    found = harness.found_block(
        BLOCK,
        height=10,
        anchor_job_issued_at_ms=ANCHOR_MS,
    )
    # A staging liberty: production arms a full PayoutLedgerArtifact, but
    # the anchor-set arithmetic under test reads only snapshot_anchor_ms.
    harness.pool._payout_ledger_artifact = SimpleNamespace(
        snapshot_anchor_ms=NEWER_SEEDED_ANCHOR_MS
    )
    bump = harness.append_late_visible_share(stamp_ms=LATER_THAN_ANCHOR_MS)
    harness.drain([harness.appender])
    silent = harness.append_late_visible_share(
        stamp_ms=INSIDE_ANCHOR_MS,
        share_id="appender:silent",
    )
    harness.drain([harness.appender])

    harness.land_on_accounting_tail(found)
    harness.drain([harness.accounting, harness.appender])

    outcome = _landing_outcome(harness, found)
    outcome["bumped_to"] = bump.value()
    outcome["silent_bump"] = silent.value()
    outcome["artifact_disarmed"] = harness.pool._payout_ledger_artifact is None
    return outcome


def _append_with_distinct_stamps_then_land(
    harness: LandingHarness,
    *,
    job_issued_at_ms: int,
    accepted_at_ms: int,
) -> dict[str, object]:
    """The drained shape above, with the row's two stamps pulled apart.

    The recorded bump stamp is ``max(job_issued_at_ms, accepted_at_ms)``: a
    row predates an anchor iff BOTH stamps do, i.e. iff that max does. The
    equal-stamp scenarios in this file cannot tell that choice from
    ``min()`` or either stamp alone. A straddling row -- issued at or
    before the candidate's anchor, accepted after -- is where they part
    ways: it does not predate the candidate's window, so recording anything
    but the max would abandon a block whose window was never invalidated,
    which is #126's over-abandon back again.
    """
    harness.boot()
    found = harness.found_block(
        BLOCK,
        height=10,
        anchor_job_issued_at_ms=ANCHOR_MS,
    )
    harness.publish_window_anchor(NEWER_WINDOW_ANCHOR_MS)
    appended = harness.append_late_visible_share(
        stamp_ms=job_issued_at_ms,
        accepted_at_ms=accepted_at_ms,
    )
    harness.drain([harness.appender])

    harness.land_on_accounting_tail(found)
    harness.drain([harness.accounting, harness.appender])

    outcome = _landing_outcome(harness, found)
    outcome["bumped_to"] = appended.value()
    return outcome


def _land_after_pruned_history(harness: LandingHarness) -> dict[str, object]:
    """Push the candidate's baseline off the end of the retained stamps.

    Every append here misses the candidate's window, so an unbounded history
    would land the block. The point is that a bounded one must not: once the
    baseline epoch is older than the oldest retained bump, the landing cannot
    prove anything about the bumps in between and has to abandon.
    """
    harness.boot()
    found = harness.found_block(
        BLOCK,
        height=10,
        anchor_job_issued_at_ms=ANCHOR_MS,
    )
    harness.publish_window_anchor(NEWER_WINDOW_ANCHOR_MS)
    bumps = PRISM_PAYOUT_APPEND_INVALIDATION_STAMP_HISTORY + 2
    for index in range(bumps):
        harness.append_late_visible_share(
            stamp_ms=LATER_THAN_ANCHOR_MS,
            share_id=f"appender:pruned-{index}",
        )
    # One drain for the whole queue: the appends are independent of each
    # other and of the landing, which has not started.
    harness.drain([harness.appender], limit=4 * bumps)

    harness.land_on_accounting_tail(found)
    harness.drain([harness.accounting, harness.appender])

    outcome = _landing_outcome(harness, found)
    outcome["bumps"] = bumps
    return outcome


def _land_without_a_declared_anchor(harness: LandingHarness) -> dict[str, object]:
    """Land a candidate whose job declared no payout window anchor.

    Nothing scopes the epoch question for such a candidate -- the landing
    cannot even expose an anchor for the append side to bump against -- so a
    moved epoch stays as terminal as it was before #126.
    """
    harness.boot()
    found = harness.found_block(
        BLOCK,
        height=10,
        anchor_job_issued_at_ms=ANCHOR_MS,
    )
    found.candidate.context.found_block.pop("anchor_job_issued_at_ms")
    harness.publish_window_anchor(NEWER_WINDOW_ANCHOR_MS)
    appended = harness.append_late_visible_share(stamp_ms=LATER_THAN_ANCHOR_MS)
    harness.drain([harness.appender])

    harness.land_on_accounting_tail(found)
    harness.drain([harness.accounting, harness.appender])

    outcome = _landing_outcome(harness, found)
    outcome["bumped_to"] = appended.value()
    return outcome


class AppendEpochScopedToTheDeclaredAnchorTests(unittest.TestCase):
    """#126: only an append that predates *this* anchor may abandon."""

    def _deterministic(self, scenario: Any) -> dict[str, object]:
        outcome = assert_deterministic(
            self,
            scenario,
            harness_factory=LandingHarness,
        ).outcome
        assert isinstance(outcome, dict)
        return outcome

    def _assert_landed(self, outcome: dict[str, object]) -> None:
        """The block reached qbitd, confirmed durably, and was not abandoned."""
        self.assertEqual(outcome["submitted"], [f"block:{BLOCK}"])
        self.assertEqual(outcome["chain_state"], "confirmed")
        self.assertEqual(outcome["outbox"], "submitted")
        self.assertTrue(outcome["envelope_written"])
        self.assertEqual(outcome["abandoned"], {})
        self.assertEqual(outcome["stale_classes"], {})

    def _assert_abandoned_as_append_epoch_stale(
        self,
        outcome: dict[str, object],
    ) -> None:
        """The block was withheld from qbitd and terminally abandoned."""
        self.assertEqual(outcome["submitted"], [])
        self.assertIsNone(outcome["chain_state"])
        self.assertEqual(outcome["outbox"], "abandoned")
        self.assertFalse(outcome["envelope_written"])
        self.assertEqual(outcome["abandoned"], {"stale-job": 1})
        self.assertEqual(
            outcome["stale_classes"],
            {"tip_moved": 0, "balance_stale": 0, "append_epoch_stale": 1},
        )

    # -- the over-abandon ---------------------------------------------------

    def test_an_append_that_misses_this_window_lands_the_block(self) -> None:
        """#126's reproduction: the block must reach qbitd and confirm.

        Before the fix this abandoned with ``submitted == []`` and
        ``block_candidate_abandoned_counts == {"stale-job": 1}`` -- the
        forfeited reward the issue is about.
        """
        outcome = self._deterministic(
            lambda harness: _append_then_land(
                harness,
                stamp_ms=LATER_THAN_ANCHOR_MS,
            )
        )
        # The premise: the append really did move the global epoch, so the
        # bare comparison this fence used to make would have fired.
        self.assertEqual(outcome["bumped_to"], 1)
        self.assertEqual(outcome["live_epoch"], 1)
        self._assert_landed(outcome)

    # -- fail-closed, preserved --------------------------------------------

    def test_an_append_inside_this_window_still_abandons(self) -> None:
        outcome = self._deterministic(
            lambda harness: _append_then_land(harness, stamp_ms=INSIDE_ANCHOR_MS)
        )
        self.assertEqual(outcome["bumped_to"], 1)
        self._assert_abandoned_as_append_epoch_stale(outcome)

    def test_an_append_exactly_at_the_declared_anchor_still_abandons(self) -> None:
        """The boundary belongs to the terminal side.

        A row predates an anchor when *both* its stamps are at or before it,
        so a share stamped exactly at the anchor is inside the candidate's
        window and its coinbase would omit a durable share.
        """
        outcome = self._deterministic(
            lambda harness: _append_then_land(harness, stamp_ms=AT_ANCHOR_MS)
        )
        self.assertEqual(outcome["bumped_to"], 1)
        self._assert_abandoned_as_append_epoch_stale(outcome)

    # -- the disarm gap ------------------------------------------------------

    def test_a_predating_append_after_the_seeded_anchor_disarms_still_abandons(
        self,
    ) -> None:
        """A bump that disarms the artifact must not blind the next append.

        Before the watermark fold this submitted: the second append
        returned ``None`` (no live anchor covered it), the fences saw only
        the first bump's harmless stamp, and ``submitted`` held the block
        -- a coinbase whose window omitted a durable share.
        """
        outcome = self._deterministic(_silent_append_after_disarm)
        # The premise: the first append moved the epoch by predating the
        # seeded window, and doing so disarmed the artifact that was that
        # window's only anchor exposure.
        self.assertEqual(outcome["bumped_to"], 1)
        self.assertTrue(outcome["artifact_disarmed"])
        # The fix: the second append still finds an anchor to predate --
        # the disarm folded the seeded anchor into the watermark -- so its
        # bump and stamp exist for the fences to refuse.
        self.assertEqual(outcome["silent_bump"], 2)
        self.assertEqual(outcome["live_epoch"], 2)
        self._assert_abandoned_as_append_epoch_stale(outcome)

    # -- the recorded stamp is the max of the row's two ----------------------

    def test_a_straddling_append_lands_the_block(self) -> None:
        """Issued at or before the anchor, accepted after: not predating.

        The row invalidates the newer published window (both stamps are at
        or before its anchor), so the epoch moves -- but the candidate's
        own window is intact, and only a recorded stamp of
        ``max(job_issued_at_ms, accepted_at_ms)`` says so. ``min()`` or the
        issue stamp alone records 9_000 here and abandons a valid block.
        """
        outcome = self._deterministic(
            lambda harness: _append_with_distinct_stamps_then_land(
                harness,
                job_issued_at_ms=INSIDE_ANCHOR_MS,
                accepted_at_ms=LATER_THAN_ANCHOR_MS,
            )
        )
        self.assertEqual(outcome["bumped_to"], 1)
        self._assert_landed(outcome)

    def test_an_append_with_distinct_stamps_inside_the_window_still_abandons(
        self,
    ) -> None:
        """And the same distinct-stamp row is terminal once both predate."""
        outcome = self._deterministic(
            lambda harness: _append_with_distinct_stamps_then_land(
                harness,
                job_issued_at_ms=INSIDE_ANCHOR_MS,
                accepted_at_ms=ALSO_INSIDE_ANCHOR_MS,
            )
        )
        self.assertEqual(outcome["bumped_to"], 1)
        self._assert_abandoned_as_append_epoch_stale(outcome)

    # -- the authoritative fence -------------------------------------------

    def test_a_bump_after_the_advisory_read_is_terminal_when_it_predates(
        self,
    ) -> None:
        """The fence-held check still catches what the advisory one missed."""
        outcome = self._deterministic(
            lambda harness: _land_across_the_authoritative_fence(
                harness,
                stamp_ms=INSIDE_ANCHOR_MS,
            )
        )
        # The interleaving is the whole point: the pre-offer fence read an
        # unmoved epoch, so this verdict can only come from the fence-held
        # comparison that runs after it.
        self.assertEqual(outcome["epoch_at_advisory_read"], 0)
        self.assertEqual(outcome["bumped_to"], 1)
        self._assert_abandoned_as_append_epoch_stale(outcome)

    def test_a_bump_after_the_advisory_read_is_harmless_when_it_does_not(
        self,
    ) -> None:
        """And forgives the same interleaving when the stamp cannot predate."""
        outcome = self._deterministic(
            lambda harness: _land_across_the_authoritative_fence(
                harness,
                stamp_ms=LATER_THAN_ANCHOR_MS,
            )
        )
        self.assertEqual(outcome["epoch_at_advisory_read"], 0)
        self.assertEqual(outcome["bumped_to"], 1)
        self._assert_landed(outcome)

    # -- what cannot be answered -------------------------------------------

    def test_a_baseline_older_than_the_retained_history_abandons(self) -> None:
        """Pruning narrows acceptance; it never widens it."""
        outcome = self._deterministic(_land_after_pruned_history)
        self.assertEqual(
            outcome["live_epoch"],
            PRISM_PAYOUT_APPEND_INVALIDATION_STAMP_HISTORY + 2,
        )
        # Every one of those appends missed the candidate's window, so this
        # abandon is the retention bound talking and nothing else.
        self._assert_abandoned_as_append_epoch_stale(outcome)

    def test_a_candidate_without_a_declared_anchor_abandons(self) -> None:
        outcome = self._deterministic(_land_without_a_declared_anchor)
        self.assertEqual(outcome["bumped_to"], 1)
        self._assert_abandoned_as_append_epoch_stale(outcome)


if __name__ == "__main__":
    unittest.main()
