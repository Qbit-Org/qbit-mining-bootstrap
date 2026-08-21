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
    PERTURBATIONS,
    STORM_PARENT_HASH,
    CandidateStormRig,
)

# A storm large enough to overflow the live queue by 2x and to exercise the
# replay enumeration's legacy widening window more than once, small enough
# to drain through the per-row path in well under a second.
DECIDED_STORM_CANDIDATES = 100


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


class DecidedCandidateStormTests(unittest.TestCase):
    """Pin what the rig reports once the storm height is decided.

    These are component facts about the shipped tree, not a selector: they
    say which in-memory and durable facts exist for never-offered siblings
    versus candidates with a real or ambiguous node offer, and what the
    per-row disposition path costs to drain the sibling set.
    """

    def test_decided_height_leaves_siblings_without_offer_evidence(self) -> None:
        rig = CandidateStormRig(candidates=DECIDED_STORM_CANDIDATES)
        rig.seed_live()
        winner = rig.decide_height()
        self.assertEqual(winner, rig.block_hashes[-1])
        self.assertEqual(rig.decided_rpc.call("getbestblockhash"), winner)
        self.assertEqual(
            rig.decided_rpc.call("getblockhash", [rig.storm_height]),
            winner,
        )

        live = rig.snapshot(rig.live_server)
        self.assertEqual(live.outstanding_marked, DECIDED_STORM_CANDIDATES)
        self.assertEqual(live.offer_evidence_marked, 0)
        ownership = rig.ownership(rig.live_server)
        self.assertEqual(len(ownership), DECIDED_STORM_CANDIDATES)
        decided_header = rig.decided_rpc.call("getblockheader", [winner])
        for record in ownership:
            self.assertEqual(record.outbox_state, "pending")
            self.assertEqual(record.attempt_count, 0)
            self.assertEqual(record.parent_hash, STORM_PARENT_HASH)
            self.assertEqual(record.expected_height, rig.storm_height)
            # Equal work: the decided block and every sibling were built at
            # the same difficulty, so height occupancy is decisive here.
            self.assertEqual(record.network_difficulty, decided_header["difficulty"])
            self.assertTrue(record.outstanding)
            self.assertFalse(record.node_offer_evidence)
        self.assertEqual(
            sum(1 for record in ownership if record.live_queued),
            MAX_PENDING_BLOCK_CANDIDATES,
        )
        # The winner is the last seeded candidate: a coalesced wakeup in the
        # live view, so the queue holds only siblings.
        self.assertFalse(ownership[-1].live_queued)

        restarted = rig.restart_and_enumerate()
        self.assertEqual(restarted.replay_inflight_marked, DECIDED_STORM_CANDIDATES)
        self.assertEqual(restarted.accepted_parent_previews, DECIDED_STORM_CANDIDATES)
        self.assertEqual(restarted.offer_evidence_marked, 0)
        self.assertIs(rig.restarted_server.rpc, rig.decided_rpc)
        self.assertTrue(
            all(record.replay_queued for record in rig.ownership(rig.restarted_server))
        )

    def test_offer_evidence_gauge_counts_each_evidence_kind_once(self) -> None:
        rig = CandidateStormRig(candidates=DECIDED_STORM_CANDIDATES)
        rig.seed_live()
        rig.decide_height()
        server = rig.live_server
        hashes = rig.block_hashes
        undo = [
            rig.perturb(server, kind, hashes[index])
            for index, kind in enumerate(PERTURBATIONS)
        ]
        try:
            by_hash = {record.block_hash: record for record in rig.ownership(server)}
            self.assertTrue(by_hash[hashes[0]].disposition_held)
            self.assertTrue(by_hash[hashes[1]].node_acceptance_retained)
            self.assertTrue(by_hash[hashes[2]].tip_observed)
            self.assertTrue(by_hash[hashes[3]].retry_held)
            self.assertEqual(by_hash[hashes[4]].pool_block_chain_state, "prepared")
            self.assertIs(by_hash[hashes[5]].terminal_outcome, False)
            for block_hash in hashes[:len(PERTURBATIONS)]:
                self.assertTrue(by_hash[block_hash].node_offer_evidence)
                # Process ownership is unchanged by evidence: still outstanding.
                self.assertTrue(by_hash[block_hash].outstanding)
            self.assertEqual(
                sum(1 for record in by_hash.values() if record.node_offer_evidence),
                len(PERTURBATIONS),
            )
            # The union is conservative, but the rig still says which rows
            # actually attest a node offer. Only the parked wakeup does not:
            # the retry holder is reachable without any submitblock.
            self.assertFalse(by_hash[hashes[3]].node_offer_attested)
            self.assertTrue(by_hash[hashes[3]].retry_held_without_offer)
            for index in (0, 1, 2, 4, 5):
                with self.subTest(perturbation=PERTURBATIONS[index]):
                    self.assertTrue(by_hash[hashes[index]].node_offer_attested)
                    self.assertFalse(by_hash[hashes[index]].retry_held_without_offer)
            snapshot = rig.snapshot(server)
            self.assertEqual(snapshot.offer_evidence_marked, len(PERTURBATIONS))
            self.assertEqual(
                snapshot.node_offer_attested_marked,
                len(PERTURBATIONS) - 1,
            )
            self.assertEqual(snapshot.unoffered_retry_marked, 1)
        finally:
            for release in undo:
                if release is not None:
                    release()

    def test_retry_retention_alone_is_not_node_offer_evidence(self) -> None:
        """Parking a wakeup in the retry holder must not read as an offer.

        ``_retain_block_candidate_for_retry`` runs on shipped paths that
        never reached qbitd -- most plainly when
        ``_reserve_block_fast_lane_slot`` declines and ``submit_next`` parks
        the candidate *before* any submitblock -- so a rig that folded the
        holder into its offer evidence would credit a node call that never
        happened.  The #183 must-not-abandon union still has to cover the
        row, because the holder cannot distinguish that path from a genuine
        offer retrying its tail.
        """

        rig = CandidateStormRig(candidates=DECIDED_STORM_CANDIDATES)
        rig.seed_live()
        rig.decide_height()
        server = rig.live_server
        parked = rig.block_hashes[0]

        before = {record.block_hash: record for record in rig.ownership(server)}
        self.assertTrue(before[parked].live_queued)
        self.assertFalse(before[parked].retry_held)
        self.assertFalse(before[parked].node_offer_evidence)
        calls_before = dict(rig.decided_rpc.calls)

        # Move a queued, never-offered wakeup into the retry slot, exactly as
        # a declined fast-lane reservation does.
        rig.perturb(server, "retry_slot", parked)
        record = {r.block_hash: r for r in rig.ownership(server)}[parked]
        self.assertTrue(record.retry_held)

        # Nothing was offered: no RPC happened, and no fact downstream of an
        # offer exists for the row.
        self.assertEqual(rig.decided_rpc.calls, calls_before)
        self.assertFalse(record.node_offer_attested)
        self.assertTrue(record.retry_held_without_offer)
        self.assertFalse(record.disposition_held)
        self.assertFalse(record.node_acceptance_retained)
        self.assertFalse(record.tip_observed)
        self.assertFalse(record.accounted_accepted)
        self.assertIsNone(record.pool_block_chain_state)
        # Nor is the row reported terminal: retry retention is not a
        # disposition, so the durable row is untouched and unattempted.
        self.assertIsNone(record.terminal_outcome)
        self.assertEqual(record.outbox_state, "pending")
        self.assertEqual(record.attempt_count, 0)
        # The conservative contract still covers it.
        self.assertTrue(record.node_offer_evidence)

        snapshot = rig.snapshot(server)
        self.assertEqual(snapshot.offer_evidence_marked, 1)
        self.assertEqual(snapshot.node_offer_attested_marked, 0)
        self.assertEqual(snapshot.unoffered_retry_marked, 1)

    def test_replay_adoption_alone_drops_a_tip_observation(self) -> None:
        """Why ``perturb('tip_observed')`` also registers the hash outstanding.

        Restart adoption never marks a hash outstanding -- only the node
        offer does -- so a blockwait observation of a replayed candidate is
        dropped until it is dequeued and offered.  A selector that reads the
        observation set alone therefore cannot see a replayed candidate's
        acceptance, which is why the #181 dequeue-time skip has to read the
        chain for replayed rows.
        """

        rig = CandidateStormRig(candidates=DECIDED_STORM_CANDIDATES)
        rig.seed_live()
        rig.restart_and_enumerate()
        rig.decide_height()
        server = rig.restarted_server
        replayed = rig.block_hashes[0]
        self.assertEqual(rig.snapshot(server).outstanding_marked, 0)

        server._note_tip_observation_for_candidates(replayed)
        by_hash = {record.block_hash: record for record in rig.ownership(server)}
        self.assertFalse(by_hash[replayed].tip_observed)
        self.assertFalse(by_hash[replayed].node_offer_evidence)
        self.assertEqual(rig.snapshot(server).offer_evidence_marked, 0)

        rig.perturb(server, "tip_observed", replayed)
        by_hash = {record.block_hash: record for record in rig.ownership(server)}
        self.assertTrue(by_hash[replayed].tip_observed)
        self.assertEqual(rig.snapshot(server).offer_evidence_marked, 1)

    def test_per_row_drain_cost_is_linear_in_siblings_live_and_after_restart(
        self,
    ) -> None:
        siblings = DECIDED_STORM_CANDIDATES - 1
        expected_rpc = {
            # One fast-lane offer per sibling, then the admission tip read plus
            # the abandon seal's pre- and post-withdrawal chain probes: three
            # getbestblockhash/getblockheader pairs per sibling.
            "submitblock": siblings,
            "getbestblockhash": 3 * siblings,
            "getblockheader": 3 * siblings,
        }

        live_rig = CandidateStormRig(candidates=DECIDED_STORM_CANDIDATES)
        live_rig.seed_live()
        winner = live_rig.decide_height()
        drain = live_rig.drain_per_row(live_rig.live_server, stop_before_hash=winner)
        self.assertEqual(drain.rounds, siblings)
        # The exact set, not just its size: this is what a selector is
        # compared against.
        self.assertEqual(
            drain.abandoned_hashes,
            frozenset(live_rig.block_hashes) - {winner},
        )
        self.assertEqual(drain.pending_hashes, frozenset({winner}))
        self.assertEqual(drain.submitted_hashes, frozenset())
        self.assertEqual(drain.withheld_hashes, frozenset())
        # Every offer was handed to the real accounting queue and run there.
        self.assertEqual(drain.accounting_tasks, siblings)
        self.assertEqual(drain.ledger_attempt_marks, siblings)
        self.assertEqual(drain.rpc_calls, expected_rpc)
        # 32 queued wakeups drain first; the coalesced remainder is recovered
        # through the outbox query in 32-row windows: 32 + 32 + 4 rows.
        self.assertEqual(drain.replay_enumerations, 3)
        after = {record.block_hash: record for record in live_rig.ownership(live_rig.live_server)}
        self.assertEqual(after[winner].outbox_state, "pending")
        self.assertFalse(after[winner].node_offer_evidence)
        self.assertTrue(
            all(
                record.outbox_state == "abandoned" and record.terminal_outcome is False
                for block_hash, record in after.items()
                if block_hash != winner
            )
        )
        # Every sibling's offer-time barrier was withdrawn by its abandonment;
        # the one left is the winner's, armed when the live process's own
        # recovery enumeration adopted it from the outbox (adoption arms a
        # barrier per row exactly as restart adoption does).
        self.assertEqual(live_rig.snapshot(live_rig.live_server).accepted_parent_previews, 1)

        restart_rig = CandidateStormRig(candidates=DECIDED_STORM_CANDIDATES)
        restart_rig.seed_live()
        restart_rig.restart_and_enumerate()
        winner = restart_rig.decide_height()
        drain = restart_rig.drain_per_row(
            restart_rig.restarted_server,
            stop_before_hash=winner,
        )
        self.assertEqual(drain.rounds, siblings)
        self.assertEqual(
            drain.abandoned_hashes,
            frozenset(restart_rig.block_hashes) - {winner},
        )
        self.assertEqual(drain.pending_hashes, frozenset({winner}))
        self.assertEqual(drain.accounting_tasks, siblings)
        self.assertEqual(drain.ledger_attempt_marks, siblings)
        self.assertEqual(drain.rpc_calls, expected_rpc)
        # Restart adoption already queued every row; no enumeration is needed.
        self.assertEqual(drain.replay_enumerations, 0)
        # Only the undrained winner's adoption-time barrier remains.
        self.assertEqual(
            restart_rig.snapshot(restart_rig.restarted_server).accepted_parent_previews,
            1,
        )

    def test_per_row_drain_runs_the_production_submitter_split(self) -> None:
        """The drain must defer accounting, as ``BlockCandidateService.run`` does.

        Two shipped behaviours exist only on that split, and both decide
        whether a row is abandoned: ``submit_next`` claims the disposition
        lease *blocking* when accounting is inline, so one candidate whose
        lease is held elsewhere would stop the drain forever; and
        ``_restore_replayed_candidate_acceptance_evidence`` runs only inside
        the accounting task, so an inline drain abandons a replayed candidate
        with a prepared pool-block row that production preserves.
        """

        rig = CandidateStormRig(candidates=DECIDED_STORM_CANDIDATES)
        rig.seed_live()
        winner = rig.decide_height()
        server = rig.live_server
        held = rig.block_hashes[0]
        release = rig.perturb(server, "lease_held", held)
        try:
            drain = rig.drain_per_row(server, stop_before_hash=winner)
        finally:
            release()
        # Bounded: the held lease parked one wakeup instead of blocking, and
        # every other sibling still drained.
        self.assertEqual(drain.lease_blocked_hashes, frozenset({held}))
        self.assertNotIn(held, drain.abandoned_hashes)
        self.assertIn(held, drain.pending_hashes)
        self.assertEqual(
            drain.abandoned_hashes,
            frozenset(rig.block_hashes) - {winner, held},
        )
        # The lease-held row was never offered, so it cost no submitblock.
        self.assertEqual(
            drain.rpc_calls["submitblock"],
            DECIDED_STORM_CANDIDATES - 2,
        )

    def test_per_row_drain_preserves_every_offer_evidence_perturbation(
        self,
    ) -> None:
        """What the shipped path does with each evidence shape, in both views.

        Only the two shapes that are *not* live node-offer evidence at
        disposition time reach a terminal abandonment: a wakeup sitting in
        the retry slot (dequeued and re-offered like any other), and, in the
        live view only, a prepared pool-block row (rejected first, then
        abandoned). The same prepared row after restart is preserved, because
        the accounting task restores its acceptance evidence.
        """

        for view in ("live", "restart"):
            with self.subTest(view=view):
                rig = CandidateStormRig(candidates=DECIDED_STORM_CANDIDATES)
                rig.seed_live()
                if view == "restart":
                    rig.restart_and_enumerate()
                winner = rig.decide_height()
                server = rig.restarted_server if view == "restart" else rig.live_server
                hashes = dict(zip(PERTURBATIONS, rig.block_hashes))
                undo = [
                    rig.perturb(server, kind, block_hash)
                    for kind, block_hash in hashes.items()
                ]
                self.assertEqual(
                    rig.snapshot(server).offer_evidence_marked,
                    len(PERTURBATIONS),
                )
                try:
                    drain = rig.drain_per_row(
                        server,
                        stop_before_hash=winner,
                        # A retained definitive success re-runs the landing
                        # tail rather than the abandon path, and that tail is
                        # outside this instrument.
                        preserve_hashes=[hashes["retained_success"]],
                    )
                finally:
                    for release in undo:
                        if release is not None:
                            release()

                abandoned = {"retry_slot"}
                if view == "live":
                    abandoned.add("prepared_pool_block")
                for kind, block_hash in hashes.items():
                    with self.subTest(perturbation=kind):
                        if kind in abandoned:
                            self.assertIn(block_hash, drain.abandoned_hashes)
                        else:
                            self.assertNotIn(block_hash, drain.abandoned_hashes)
                            self.assertIn(block_hash, drain.pending_hashes)
                self.assertEqual(
                    drain.lease_blocked_hashes,
                    frozenset({hashes["lease_held"]}),
                )
                expected_deferred = {hashes["tip_observed"]}
                if view == "restart":
                    expected_deferred.add(hashes["prepared_pool_block"])
                self.assertEqual(drain.deferred_hashes, frozenset(expected_deferred))
                self.assertEqual(
                    drain.abandoned_rows,
                    DECIDED_STORM_CANDIDATES - 1 - len(hashes) + len(abandoned),
                )
                self.assertEqual(drain.submitted_hashes, frozenset())
                self.assertIn(winner, drain.pending_hashes)

    def test_bounded_drains_report_only_their_own_transitions(self) -> None:
        """``max_rounds`` reports this call's work, not the rig's history.

        A bounded drain is how a caller steps the oracle one row at a time.
        If the report named every terminal row standing in the outbox instead
        of the ones this call moved, the second step would claim the first
        step's abandonment, and no per-step comparison against a selector
        would mean anything.
        """

        steps = 3
        rig = CandidateStormRig(candidates=DECIDED_STORM_CANDIDATES)
        rig.seed_live()
        winner = rig.decide_height()
        server = rig.live_server

        seen: frozenset[str] = frozenset()
        for step in range(steps):
            with self.subTest(step=step):
                report = rig.drain_per_row(
                    server,
                    stop_before_hash=winner,
                    max_rounds=1,
                )
                # One row offered, one accounted, one durable attempt marked.
                self.assertEqual(report.rounds, 1)
                self.assertEqual(report.accounting_tasks, 1)
                self.assertEqual(report.ledger_attempt_marks, 1)
                self.assertEqual(report.rpc_calls["submitblock"], 1)
                # The first 32 wakeups are already queued, so no step needs
                # the recovery enumeration.
                self.assertEqual(report.replay_enumerations, 0)
                # ...and exactly one transition, never an earlier step's.
                self.assertEqual(len(report.abandoned_hashes), 1)
                self.assertEqual(report.submitted_hashes, frozenset())
                self.assertEqual(report.withheld_hashes, frozenset())
                self.assertTrue(report.abandoned_hashes.isdisjoint(seen))
                seen |= report.abandoned_hashes
                # pending_hashes is the complementary residue of the same
                # outbox, so it shrinks by exactly the rows drained so far.
                self.assertEqual(
                    report.pending_hashes,
                    frozenset(rig.block_hashes) - seen,
                )
        self.assertEqual(len(seen), steps)

        # The bounded steps and the unbounded remainder partition the sibling
        # set exactly: no row is reported twice and none is dropped.
        final = rig.drain_per_row(server, stop_before_hash=winner)
        self.assertEqual(final.rounds, DECIDED_STORM_CANDIDATES - 1 - steps)
        self.assertTrue(final.abandoned_hashes.isdisjoint(seen))
        self.assertEqual(
            seen | final.abandoned_hashes,
            frozenset(rig.block_hashes) - {winner},
        )
        self.assertEqual(final.submitted_hashes, frozenset())
        self.assertEqual(final.withheld_hashes, frozenset())
        self.assertEqual(final.pending_hashes, frozenset({winner}))

    def test_per_row_drain_at_the_observed_cardinality(self) -> None:
        """The same facts at the incident's 3,120 rows, live and after restart.

        3,120 is above both the 32-entry live queue and the 1,024-row replay
        page, so the live view exercises the recovery enumeration 97 times
        and the restart view exercises multi-page adoption.
        """

        siblings = OBSERVED_TESTNET_CANDIDATE_STORM - 1
        expected_rpc = {
            "submitblock": siblings,
            "getbestblockhash": 3 * siblings,
            "getblockheader": 3 * siblings,
        }
        for view, enumerations in (("live", 97), ("restart", 0)):
            with self.subTest(view=view):
                rig = CandidateStormRig()
                rig.seed_live()
                if view == "restart":
                    rig.restart_and_enumerate()
                winner = rig.decide_height()
                server = rig.restarted_server if view == "restart" else rig.live_server
                drain = rig.drain_per_row(server, stop_before_hash=winner)
                self.assertEqual(
                    drain.abandoned_hashes,
                    frozenset(rig.block_hashes) - {winner},
                )
                self.assertEqual(drain.pending_hashes, frozenset({winner}))
                self.assertEqual(drain.submitted_hashes, frozenset())
                self.assertEqual(drain.withheld_hashes, frozenset())
                self.assertEqual(drain.rounds, siblings)
                self.assertEqual(drain.accounting_tasks, siblings)
                self.assertEqual(drain.ledger_attempt_marks, siblings)
                self.assertEqual(drain.rpc_calls, expected_rpc)
                self.assertEqual(sum(drain.rpc_calls.values()), 21_833)
                self.assertEqual(drain.replay_enumerations, enumerations)
                # Only the undrained winner's barrier is left standing.
                self.assertEqual(
                    rig.snapshot(server).accepted_parent_previews,
                    1,
                )

    def test_parent_transition_lookup_probes_once_per_distinct_height(self) -> None:
        rig = CandidateStormRig(candidates=DECIDED_STORM_CANDIDATES)
        rig.seed_live()
        rig.restart_and_enumerate()
        rig.decide_height()
        server = rig.restarted_server
        self.assertEqual(
            rig.snapshot(server).accepted_parent_previews,
            DECIDED_STORM_CANDIDATES,
        )
        before = dict(rig.decided_rpc.calls)
        # A parent with no exact transition: the lookup falls through to the
        # ancestor scan, which #193 deduplicates by candidate height.
        selected = server._accepted_block_payout_transition_for_parent(
            "22" * 32,
            parent_height=rig.storm_height,
        )
        self.assertEqual(
            rig.decided_rpc.calls.get("getblockhash", 0) - before.get("getblockhash", 0),
            1,
        )
        self.assertEqual(selected, (rig.block_hashes[-1], False))


if __name__ == "__main__":
    unittest.main()
