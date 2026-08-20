#!/usr/bin/env python3
"""Regression coverage for the reusable PRISM candidate-storm rig."""

from __future__ import annotations

import unittest

from lab.prism.block_candidates import (
    MAX_BLOCK_REPLAY_ENUMERATION_ROWS,
    MAX_PENDING_BLOCK_CANDIDATES,
)
from tests.prism_candidate_storm import (
    OBSERVED_TESTNET_CANDIDATE_STORM,
    CandidateStormRig,
)


class CandidateStormInstrumentTests(unittest.TestCase):
    def test_observed_storm_exposes_live_and_restart_marker_cardinality(
        self,
    ) -> None:
        """Pin the ownership semantics that falsified the first #183 design.

        Outstanding covers all live admissions, not just the bounded queue,
        and replay-inflight covers every adopted restart row.  Neither marker
        is evidence that the candidate has already reached qbitd.
        """

        self.assertGreater(
            OBSERVED_TESTNET_CANDIDATE_STORM,
            MAX_BLOCK_REPLAY_ENUMERATION_ROWS,
        )
        self.assertGreater(
            OBSERVED_TESTNET_CANDIDATE_STORM,
            MAX_PENDING_BLOCK_CANDIDATES,
        )
        rig = CandidateStormRig()

        live = rig.seed_live()
        self.assertEqual(live.durable_pending, OBSERVED_TESTNET_CANDIDATE_STORM)
        self.assertEqual(live.live_queue_capacity, MAX_PENDING_BLOCK_CANDIDATES)
        self.assertEqual(live.live_queued, MAX_PENDING_BLOCK_CANDIDATES)
        self.assertEqual(
            live.live_coalesced,
            OBSERVED_TESTNET_CANDIDATE_STORM - MAX_PENDING_BLOCK_CANDIDATES,
        )
        self.assertEqual(
            live.outstanding_marked,
            OBSERVED_TESTNET_CANDIDATE_STORM,
        )
        self.assertTrue(live.outstanding_covers_all_durable)
        self.assertEqual(live.replay_queued, 0)
        self.assertEqual(live.replay_inflight_marked, 0)

        restarted = rig.restart_and_enumerate()
        self.assertEqual(
            restarted.durable_pending,
            OBSERVED_TESTNET_CANDIDATE_STORM,
        )
        self.assertEqual(restarted.live_queued, 0)
        self.assertEqual(restarted.live_coalesced, 0)
        self.assertEqual(restarted.outstanding_marked, 0)
        self.assertEqual(
            restarted.replay_queued,
            OBSERVED_TESTNET_CANDIDATE_STORM,
        )
        self.assertEqual(
            restarted.replay_inflight_marked,
            OBSERVED_TESTNET_CANDIDATE_STORM,
        )
        self.assertTrue(restarted.replay_inflight_covers_all_durable)
        self.assertEqual(
            restarted.accepted_parent_previews,
            OBSERVED_TESTNET_CANDIDATE_STORM,
        )
        self.assertFalse(restarted.replay_enumeration_owed)


if __name__ == "__main__":
    unittest.main()
