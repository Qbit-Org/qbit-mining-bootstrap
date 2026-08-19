#!/usr/bin/env python3
"""Two distinct found blocks interleaved across PRISM's two landing tails.

Issue #133: when two *distinct* found blocks land inside one confirm→publish
window and one of them lands on the synchronous client-thread route, the
earlier block's live audit envelope was silently and permanently never
written. Nothing was lost but the published evidence pointer — the block was
mined, paid and audited correctly — which is exactly why no error appeared
and no test caught it. The reachable case was untested because the per-hash
disposition lease provides no mutual exclusion across hashes, and no test
interleaved two hashes.

That defect is fixed on ``2.x.x`` by ``03e733e``, so these scenarios pass.
The evidence they carry is not that the fix works — its own PR asserted that
against a hand-built store — but that the *interleaving* can now be driven
end to end, through the real landing tails, the real publication-ordinal
allocator and a real audit root. Nothing could do that before.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.prism_concurrency_harness import assert_deterministic  # noqa: E402
from tests.prism_landing_harness import (  # noqa: E402
    PARENT_HASH,
    LandingHarness,
)

BLOCK_A = "aa" * 32
BLOCK_B = "bb" * 32

# The top of the publish step, named by the landing itself. Every tail stops
# here having confirmed durably and having released the payout-balance
# serializer, the publication order guard and the ledger's stats
# single-flight — which is precisely the window #133 describes as open to a
# descendant.
PUBLISH_BOUNDARY = "progress:evidence-write"


def _two_tail_landing(harness: LandingHarness) -> dict[str, object]:
    """Land A on the client tail and B on the accounting tail, interleaved.

    Block B is A's child: #133's trigger is explicit that the window is open
    to descendants because A clears its accepted-block payout preview — the
    child-deferral gate — before it publishes.
    """
    harness.boot()
    harness.break_at(PUBLISH_BOUNDARY)
    block_a = harness.found_block(BLOCK_A, height=10, parent_hash=PARENT_HASH)
    block_b = harness.found_block(BLOCK_B, height=11, parent_hash=BLOCK_A)

    landed_a = harness.land_on_client_tail(block_a)
    landed_b = harness.land_on_accounting_tail(block_b)

    # A confirms and stops at the top of its publish step.
    harness.run_until(harness.client, PUBLISH_BOUNDARY)
    a_sequence = harness.publication_sequence(block_a)
    floor_before_b = harness.publication_floor()

    # B lands entirely inside that window: confirm, ordinal, publish.
    harness.run_until(harness.accounting, PUBLISH_BOUNDARY)
    harness.drain([harness.accounting])
    b_sequence = harness.publication_sequence(block_b)

    # A resumes and publishes against a floor B has already raised.
    harness.drain([harness.client, harness.accounting])

    return {
        "a_sequence": a_sequence,
        "b_sequence": b_sequence,
        "floor_before_b": floor_before_b,
        "floor_after": harness.publication_floor(),
        "a_landed": landed_a.error is None and landed_a.result is True,
        "b_landed": landed_b.error is None,
        "a_envelope_written": harness.envelope_written(block_a),
        "b_envelope_written": harness.envelope_written(block_b),
        "a_outbox": harness.outbox_state(block_a),
        "b_outbox": harness.outbox_state(block_b),
        "summary": harness.landing_summary(),
    }


class TwoDistinctHashLandingTests(unittest.TestCase):
    """#133: the earlier ordinal must keep its own published evidence."""

    def test_harness_drives_the_two_distinct_hash_interleaving(self) -> None:
        outcome = assert_deterministic(
            self,
            _two_tail_landing,
            harness_factory=LandingHarness,
        ).outcome
        assert isinstance(outcome, dict)

        # The interleaving actually happened: A took the lower ordinal, B
        # took the next one and raised the durable floor past A while A was
        # still between its confirmation and its publication.
        self.assertEqual(outcome["a_sequence"], 1)
        self.assertEqual(outcome["b_sequence"], 2)
        self.assertEqual(outcome["floor_before_b"], 1)
        self.assertEqual(outcome["floor_after"], 2)

        # Both blocks land, and neither is abandoned by the race.
        self.assertTrue(outcome["a_landed"])
        self.assertTrue(outcome["b_landed"])
        self.assertEqual(outcome["a_outbox"], "submitted")
        self.assertEqual(outcome["b_outbox"], "submitted")

        # The assertion #133 exists for. A published at sequence 1 against a
        # floor of 2, so it is superseded on the evidence pointer and never
        # becomes the current reference — but its own height+hash envelope is
        # its only published evidence, it competes with nothing, and it must
        # be on disk. Before 03e733e this file was silently absent.
        self.assertTrue(outcome["a_envelope_written"])
        self.assertTrue(outcome["b_envelope_written"])

    def test_the_superseded_publication_is_reported_not_silent(self) -> None:
        """A superseded envelope write must leave a trace an operator can find.

        The original defect's whole character was silence: no error, no log,
        a permanent gap in the public audit trail. A fix that restored the
        file but stayed quiet would still leave nobody able to tell the
        interleaving had happened.
        """
        with LandingHarness() as harness:
            _two_tail_landing(harness)
            # Audit-path diagnostics go to stderr: worker processes reserve
            # stdout for a strict JSON-lines protocol.
            self.assertIn(
                "wrote live envelope for superseded publication",
                harness.errors,
            )
            self.assertIn("sequence=1 floor_sequence=2", harness.errors)

    def test_restore_authority_is_gated_on_a_fresh_lease_proof(self) -> None:
        """Withheld authority degrades to the old behaviour, not to a failure.

        Only the live writer for a landing may mutate the shared audit root.
        A process whose writer lease was taken over inside the confirm→publish
        gap reaches the same call site with stale reads behind it, and
        publishing a stale report as a block's public evidence is worse than
        leaving the envelope missing. So the restore is gated — and when the
        gate refuses, the publication must still succeed, because failing a
        found-block landing over a lease hiccup would be a live regression.
        """

        def scenario(harness: LandingHarness) -> dict[str, object]:
            harness.withheld_lease_components = {
                "superseded_audit_envelope_restore"
            }
            outcome = _two_tail_landing(harness)
            return {
                "a_envelope_written": outcome["a_envelope_written"],
                "b_envelope_written": outcome["b_envelope_written"],
                "a_landed": outcome["a_landed"],
                "b_landed": outcome["b_landed"],
            }

        outcome = assert_deterministic(
            self,
            scenario,
            harness_factory=LandingHarness,
        ).outcome
        assert isinstance(outcome, dict)
        # The landing still succeeds on both tails.
        self.assertTrue(outcome["a_landed"])
        self.assertTrue(outcome["b_landed"])
        # B is unaffected: it is not superseded, so it never asks.
        self.assertTrue(outcome["b_envelope_written"])
        # A degrades to the pre-fix behaviour rather than failing.
        self.assertFalse(outcome["a_envelope_written"])

    def test_the_disposition_lease_does_not_serialise_distinct_hashes(self) -> None:
        """The topology the defect needs, asserted rather than assumed.

        If the per-hash disposition lease did serialise across hashes, this
        whole scenario would be unreachable and the tests above would be
        proving nothing. The claim is load-bearing, so it is checked: with A
        holding its own disposition and parked mid-landing, B's tail claims
        its disposition and completes a full landing.
        """
        with LandingHarness() as harness:
            harness.boot()
            harness.break_at(PUBLISH_BOUNDARY)
            block_a = harness.found_block(BLOCK_A, height=10, parent_hash=PARENT_HASH)
            block_b = harness.found_block(BLOCK_B, height=11, parent_hash=BLOCK_A)
            harness.land_on_client_tail(block_a)
            landed_b = harness.land_on_accounting_tail(block_b)

            harness.run_until(harness.client, PUBLISH_BOUNDARY)
            self.assertIsNone(harness.publication_sequence(block_b))

            # A is mid-landing and holds its hash's disposition. B proceeds
            # anyway, all the way through its own durable confirmation.
            harness.run_until(harness.accounting, PUBLISH_BOUNDARY)
            harness.drain([harness.accounting])
            self.assertIsNone(landed_b.error)
            self.assertEqual(harness.publication_sequence(block_b), 2)


if __name__ == "__main__":
    unittest.main()
