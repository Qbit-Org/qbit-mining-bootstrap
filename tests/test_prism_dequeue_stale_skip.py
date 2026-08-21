#!/usr/bin/env python3
"""Regression coverage for the dequeue-time stale sibling skip (#181 item 2).

The 2026-08-21 testnet4 validation showed that #196 removed the
restart/replay candidate population but left the live/fast-path one: the
coordinator log still carried bursts of individual
``block candidate abandoned reason=stale-job: tip moved before submit``.
Each of those rows costs one ``submitblock``, three ``getbestblockhash``,
three ``getblockheader`` and two ledger writes -- plus an accounting task, a
fast-lane reservation and an accepted-block payout-preview barrier armed and
withdrawn -- to learn a fact the chain could have answered before the offer.

These tests pin what the skip does about that, against two references it may
never disagree with: #194's per-row oracle (``drain_per_row``, the shipped
disposition path) and #196's predicate S (whose clauses, evidence set,
fencing and cleanup the skip reuses rather than restates).

The invariant every efficacy test asserts is one-directional:
``skip_set - oracle_abandoned`` must be empty. Conservative oracle-only
deltas are acceptable -- the skip is an optimisation over a path that still
runs -- while a single selector-only hash is a block the per-row path would
have kept.
"""

from __future__ import annotations

import queue
import sys
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from typing import Any

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lab.prism.block_candidates import (  # noqa: E402
    MAX_PENDING_BLOCK_CANDIDATES,
    PRISM_BLOCK_CANDIDATE_COLLAPSE_OUTCOMES,
    _BlockCandidateNodeSubmission,
    _dequeued_candidate_collapse_row,
    block_candidate_intent,
)
from tests.prism_candidate_storm import (  # noqa: E402
    PERTURBATIONS,
    CandidateStormRig,
)
from tests.test_prism_block_candidate_collapse import (  # noqa: E402
    DECIDED_HEIGHT,
    DECIDED_WINNER,
    MIN_WORK_BITS,
    STORM_PARENT,
    CollapseChainRpc,
    CollapseFixture,
    CollapseLedger,
    _hash,
    _scaled,
)


class _NoBatchLedger(CollapseLedger):
    """A compatibility ledger with no fenced batch abandonment at all."""

    def __getattr__(self, name: str) -> Any:
        if name == "mark_block_candidates_abandoned":
            raise AttributeError(name)
        return super().__getattr__(name)

    mark_block_candidates_abandoned = None  # type: ignore[assignment]


class _NoPoolBlockLedger(CollapseLedger):
    """A compatibility ledger that cannot answer clause 2 at all."""

    def __getattr__(self, name: str) -> Any:
        if name == "pool_block_state":
            raise AttributeError(name)
        return super().__getattr__(name)

    pool_block_state = None  # type: ignore[assignment]


def _offerable(fixture: CollapseFixture) -> CollapseFixture:
    """Let the fixture's chain answer ``submitblock`` instead of raising.

    ``FakeRpc`` refuses every unknown method, which would turn a preserved
    candidate's node offer into an error submission. These tests care
    whether the offer *happened*, so the chain answers the way qbitd does
    for a valid block that did not become the tip.
    """
    fixture.rpc.results["submitblock"] = "inconclusive"
    return fixture


def _drive(fixture: CollapseFixture, candidate: Any) -> str:
    """Admit one candidate and run exactly one shipped ``submit_next`` over it.

    ``defer_accounting=True`` is the split ``BlockCandidateService.run``
    uses. No accounting lane is drained here, so a candidate the skip
    preserved is left with its offer made and its durable row still pending
    -- which is precisely the observable difference from one it skipped.
    """
    buffer = StringIO()
    with redirect_stdout(buffer):
        fixture.server.enqueue_block_candidate(candidate)
        fixture.server.submit_next_block_candidate(defer_accounting=True)
    return buffer.getvalue()


def _record_skips(server: Any) -> set[str]:
    """Capture the hashes the *skip itself* terminalized, and only those.

    ``PerRowDrainReport.abandoned_hashes`` names every durable transition a
    drain made, which under ``dequeue_skip=True`` is the union of the skip's
    own set and whatever the per-row path went on to abandon for candidates
    the skip preserved. The contract's invariant is about the skip's set, so
    it has to be read where the decision is made.
    """
    service = server._ensure_block_candidate_service()
    shipped = service._skip_superseded_block_candidate_at_dequeue
    seen: set[str] = set()

    def recording(candidate: Any, **kwargs: Any) -> bool:
        skipped = shipped(candidate, **kwargs)
        if skipped:
            seen.add(str(candidate.submission.block_hash_hex).lower())
        return skipped

    service._skip_superseded_block_candidate_at_dequeue = recording
    return seen


def _accounting_depth(fixture: CollapseFixture) -> int:
    service = fixture.service
    service._ensure_block_accounting_state()
    return (
        service._block_accounting_queue.qsize()
        + service._block_accounting_overflow_queue.qsize()
        + service._block_accounting_accepted_queue.qsize()
    )


def _skipped(fixture: CollapseFixture, candidate: Any) -> bool:
    """Drive one candidate and report whether the skip terminalized it."""
    before = fixture.pending()
    _drive(fixture, candidate)
    block_hash = str(candidate.submission.block_hash_hex).lower()
    gone = block_hash in before and block_hash not in fixture.pending()
    skipped = fixture.counts()["dequeue_skipped"] > 0
    assert gone == skipped, "durable state and the skip counter disagree"
    return skipped


class DequeueSkipRowShapeTests(unittest.TestCase):
    """The skip judges an in-memory candidate; it must present the durable row."""

    def test_the_dequeue_row_matches_the_durable_intent_field_by_field(
        self,
    ) -> None:
        """Predicate S must read the same facts whichever side built the row.

        ``_dequeued_candidate_collapse_row`` restates a subset of
        ``block_candidate_intent`` rather than calling it, because the full
        intent re-serializes the block and recomputes every witness merkle
        leaf. This is what keeps the restatement honest.
        """
        fixture = CollapseFixture()
        candidate = fixture.seed([_hash(1)], network_difficulty=7)[0]
        intent = block_candidate_intent(candidate)
        row = _dequeued_candidate_collapse_row(candidate, pool_block_exists=False)
        self.assertEqual(row["block_hash"], intent["block_hash_hex"])
        built = row["candidate"]
        for field in ("block_hash_hex", "parent_hash", "expected_height"):
            self.assertEqual(built[field], intent[field], field)
        for field in ("previousblockhash", "height"):
            self.assertEqual(
                built["template"][field],
                intent["template"][field],
                field,
            )
        self.assertEqual(built["found_block"], intent["found_block"])
        self.assertEqual(
            built["pending_share"]["job_id"],
            intent["pending_share"]["job_id"],
        )
        self.assertIs(row["pool_block_exists"], False)
        self.assertIs(
            _dequeued_candidate_collapse_row(candidate, pool_block_exists=True)[
                "pool_block_exists"
            ],
            True,
        )

    def test_the_outcome_label_set_stays_closed(self) -> None:
        fixture = CollapseFixture()
        self.assertEqual(
            tuple(fixture.counts()),
            PRISM_BLOCK_CANDIDATE_COLLAPSE_OUTCOMES,
        )
        for outcome in ("dequeue_considered", "dequeue_skipped", "dequeue_preserved"):
            self.assertIn(outcome, PRISM_BLOCK_CANDIDATE_COLLAPSE_OUTCOMES)


class DequeueSkipEfficacyTests(unittest.TestCase):
    """The skip against #194's per-row oracle, live and after restart."""

    def _rig(self, candidates: int, view: str) -> tuple[CandidateStormRig, Any, str]:
        rig = CandidateStormRig(candidates=candidates)
        rig.seed_live()
        if view == "restart":
            rig.restart_and_enumerate()
        server = rig.restarted_server if view == "restart" else rig.live_server
        winner = rig.decide_height()
        return rig, server, winner

    def _oracle(self, candidates: int, view: str) -> tuple[set[str], Any]:
        rig, server, winner = self._rig(candidates, view)
        # drain_per_row suppresses both shipped set-oriented dispositions by
        # default, so this stays the per-row path with no patching here.
        report = rig.drain_per_row(server, stop_before_hash=winner)
        return set(report.abandoned_hashes), report

    def _skip(self, candidates: int, view: str) -> tuple[set[str], Any, Any, str]:
        rig, server, winner = self._rig(candidates, view)
        skip_set = _record_skips(server)
        report = rig.drain_per_row(
            server,
            stop_before_hash=winner,
            dequeue_skip=True,
        )
        return skip_set, report, server, winner

    def _compare(self, candidates: int, view: str) -> tuple[set[str], Any, Any]:
        oracle, oracle_report = self._oracle(candidates, view)
        skipped, skip_report, _server, winner = self._skip(candidates, view)
        self.assertEqual(
            skipped - oracle,
            set(),
            f"the skip abandoned {len(skipped - oracle)} hash(es) the shipped "
            f"per-row path refused ({candidates} candidates, {view} view)",
        )
        # And the drain as a whole -- skip plus whatever per-row disposed of
        # afterwards -- terminalized nothing the oracle kept either.
        self.assertEqual(set(skip_report.abandoned_hashes) - oracle, set())
        self.assertNotIn(winner, skipped)
        return skipped, skip_report, oracle_report

    def test_every_stale_sibling_is_skipped_and_the_winner_is_not(self) -> None:
        for view in ("live", "restart"):
            with self.subTest(view=view):
                skipped, _report, _oracle = self._compare(100, view)
                self.assertEqual(len(skipped), 99)

    def test_agreement_holds_at_the_observed_storm_cardinality(self) -> None:
        for view in ("live", "restart"):
            with self.subTest(view=view):
                skipped, _report, _oracle = self._compare(3_120, view)
                self.assertEqual(len(skipped), 3_119)

    def test_the_skip_costs_no_submitblock_and_fewer_chain_reads(self) -> None:
        """The cost claim, measured against the oracle on the same population."""
        for view in ("live", "restart"):
            with self.subTest(view=view):
                oracle, oracle_report = self._oracle(100, view)
                skipped, skip_report, _server, _winner = self._skip(100, view)
                self.assertEqual(len(oracle), 99)
                self.assertEqual(len(skipped), 99)
                self.assertEqual(set(skip_report.abandoned_hashes), skipped)
                # The dominant per-sibling cost is the node offer itself.
                self.assertEqual(oracle_report.rpc_calls["submitblock"], 99)
                self.assertNotIn("submitblock", skip_report.rpc_calls)
                oracle_rpcs = sum(oracle_report.rpc_calls.values())
                skip_rpcs = sum(skip_report.rpc_calls.values())
                self.assertEqual(oracle_rpcs, 693)
                self.assertLess(skip_rpcs, oracle_rpcs)
                # Four reads per skipped sibling: the clause-3 tip, the
                # selector's tip height, and the pre-write revalidation's
                # own fresh tip and occupant. The decided height and its
                # header are read once for the whole burst, because the
                # burst shares one probe budget for as long as the best tip
                # holds.
                self.assertEqual(skip_rpcs, 4 * 99 + 2)
                self.assertEqual(skip_report.rpc_calls["getblockheader"], 1)
                # Two durable writes per sibling become one: the attempt
                # mark the offer stamps is never made at all.
                self.assertEqual(oracle_report.ledger_attempt_marks, 99)
                self.assertEqual(skip_report.ledger_attempt_marks, 0)
                # And no stale sibling reaches per-row accounting.
                self.assertEqual(oracle_report.accounting_tasks, 99)
                self.assertEqual(skip_report.accounting_tasks, 0)

    def test_a_replay_adopted_row_is_classified_from_the_chain(self) -> None:
        """The restart-specific fact: replay adoption marks nothing outstanding.

        A skip that consulted only the observation set would get both halves
        of this wrong -- it would have no evidence that the winner is the
        winner, and none that a sibling is superseded.
        """
        rig = CandidateStormRig(candidates=8)
        rig.seed_live()
        rig.restart_and_enumerate()
        server = rig.restarted_server
        winner = rig.decide_height()
        service = server._ensure_block_candidate_service()
        with server.lock:
            self.assertEqual(
                set(service._outstanding_block_candidate_hashes),
                set(),
            )
            self.assertEqual(
                set(service._tip_observed_accepted_block_hashes),
                set(),
            )
            self.assertEqual(
                len(service._block_replay_inflight_hashes),
                8,
            )
        report = rig.drain_per_row(
            server,
            stop_before_hash=winner,
            dequeue_skip=True,
        )
        self.assertEqual(len(report.abandoned_hashes), 7)
        self.assertNotIn(winner, report.abandoned_hashes)
        # Now drive the winner's own dequeue. Nothing in memory says it was
        # accepted; only the chain does, through clause 4's "the occupant is
        # this candidate".
        offers_before = int(rig.decided_rpc.calls.get("submitblock", 0))
        with redirect_stdout(StringIO()):
            server.submit_next_block_candidate(defer_accounting=True)
        self.assertEqual(
            int(rig.decided_rpc.calls.get("submitblock", 0)) - offers_before,
            1,
        )
        self.assertIn(
            winner,
            {
                str(row["block_hash"]).lower()
                for row in rig.ledger.pending_block_candidate_rows(limit=64)
            },
        )
        counts = service.block_candidate_collapse_snapshot()
        self.assertEqual(counts["dequeue_skipped"], 7)
        self.assertEqual(counts["dequeue_preserved"], 1)


class DequeueSkipEvidenceTests(unittest.TestCase):
    """Every member of #196's evidence set still rejects at dequeue."""

    def test_the_baseline_sibling_is_skipped(self) -> None:
        fixture = _offerable(CollapseFixture())
        candidate = fixture.seed([_hash(1)])[0]
        self.assertTrue(_skipped(fixture, candidate))
        self.assertEqual(_accounting_depth(fixture), 0)
        self.assertNotIn("submitblock", fixture.rpc.counts())

    def test_a_candidate_building_on_the_best_tip_is_never_skipped(self) -> None:
        """Clause 3, read from the chain: this is the block waiting to land."""
        fixture = _offerable(
            CollapseFixture(
                rpc=CollapseChainRpc(
                    tip=STORM_PARENT,
                    tip_height=DECIDED_HEIGHT - 1,
                    active={DECIDED_HEIGHT - 1: STORM_PARENT},
                )
            )
        )
        candidate = fixture.seed([_hash(1)])[0]
        self.assertFalse(_skipped(fixture, candidate))
        self.assertEqual(fixture.counts()["dequeue_preserved"], 1)
        # The acceptance path pays exactly one extra chain read for the
        # clause-3 answer, and never a durable one.
        self.assertEqual(fixture.rpc.counts()["getbestblockhash"], 1)
        self.assertNotIn("getblockcount", fixture.rpc.counts())
        self.assertEqual(fixture.rpc.counts()["submitblock"], 1)

    def test_ownership_markers_alone_do_not_preserve_a_stale_sibling(self) -> None:
        """Outstanding/replay-inflight mean owned, not offered (#183's note)."""
        fixture = _offerable(CollapseFixture())
        candidate = fixture.seed([_hash(1)])[0]
        fixture.server._register_outstanding_block_candidate(_hash(1))
        with fixture.server.lock:
            fixture.service._block_replay_inflight_hashes.add(_hash(1))
        self.assertTrue(_skipped(fixture, candidate))

    def test_each_evidence_source_preserves_its_hash(self) -> None:
        target = _hash(1)

        def deferred_accounting_retry(fixture: CollapseFixture) -> None:
            duplicate = fixture.seed([target])[0]
            with fixture.server.lock:
                fixture.service._block_accounting_deferred_retry_candidate = (
                    duplicate
                )

        def waiting_retry(fixture: CollapseFixture) -> None:
            duplicate = fixture.seed([target])[0]
            with fixture.server.lock:
                fixture.service._block_disposition_waiting_retries[target] = (
                    duplicate
                )

        def retained_submission(fixture: CollapseFixture) -> None:
            fixture.service._stash_retained_block_candidate_node_submission(
                target,
                _BlockCandidateNodeSubmission(attempted=True, result=None),
            )

        def tip_observed(fixture: CollapseFixture) -> None:
            fixture.server._register_outstanding_block_candidate(target)
            with redirect_stdout(StringIO()):
                fixture.server._note_tip_observation_for_candidates(target)

        def accounted_accepted(fixture: CollapseFixture) -> None:
            fixture.server._ensure_job_cache_state()
            with fixture.server.lock:
                fixture.server._accounted_accepted_block_hashes.add(target)

        def prepared_pool_block(fixture: CollapseFixture) -> None:
            fixture.ledger.inner.persist_accepted_block(
                block_hash=target,
                block_height=DECIDED_HEIGHT,
                parent_hash=STORM_PARENT,
                final_bundle={},
                audit_report={},
            )

        for name, install in (
            ("deferred_accounting_retry", deferred_accounting_retry),
            ("waiting_retry", waiting_retry),
            ("retained_submission", retained_submission),
            ("tip_observed", tip_observed),
            ("accounted_accepted", accounted_accepted),
            ("prepared_pool_block", prepared_pool_block),
        ):
            with self.subTest(evidence=name):
                fixture = _offerable(CollapseFixture())
                candidate = fixture.seed([target])[0]
                install(fixture)
                self.assertFalse(_skipped(fixture, candidate))
                self.assertIn(target, fixture.pending())
                self.assertEqual(fixture.counts()["dequeue_skipped"], 0)
                if name != "prepared_pool_block":
                    # In-memory evidence is probed before any round trip, so
                    # an excluded candidate leaves the skip having spent
                    # neither a chain read nor the durable pool-block probe.
                    # ``prepared_pool_block`` is the one clause that is a
                    # durable fact, so it is answered after the tip read.
                    self.assertNotIn(
                        "getbestblockhash",
                        fixture.rpc.counts(),
                    )
                else:
                    self.assertEqual(
                        fixture.rpc.counts()["getbestblockhash"],
                        1,
                    )

    def test_a_recorded_terminal_outcome_never_reaches_the_skip(self) -> None:
        """The duplicate-drop fence runs first, and abandons nothing."""
        fixture = _offerable(CollapseFixture())
        candidate = fixture.seed([_hash(1)])[0]
        fixture.service._record_block_candidate_terminal_outcome(
            _hash(1),
            accepted=False,
        )
        _drive(fixture, candidate)
        self.assertEqual(fixture.counts()["dequeue_considered"], 0)
        self.assertIn(_hash(1), fixture.pending())

    def test_a_finalize_retry_never_reaches_the_skip(self) -> None:
        """A finalize-only replay is a different branch of ``submit_next``."""
        fixture = _offerable(CollapseFixture())
        candidate = fixture.seed([_hash(1)])[0]
        with fixture.server.lock:
            fixture.service.finalize_retries[_hash(1)] = (False, "boom")
        _drive(fixture, candidate)
        self.assertEqual(fixture.counts()["dequeue_considered"], 0)
        self.assertEqual(fixture.counts()["dequeue_skipped"], 0)

    def test_a_lease_held_elsewhere_parks_the_wakeup_before_the_skip(self) -> None:
        """``submit_next`` claims the lease first; an unclaimable one defers."""
        fixture = _offerable(CollapseFixture())
        candidate = fixture.seed([_hash(1)])[0]
        lease = fixture.server._claim_block_candidate_disposition(
            _hash(1),
            blocking=False,
        )
        self.assertIsNotNone(lease)
        self.addCleanup(
            fixture.server._release_block_candidate_disposition,
            lease,
        )
        _drive(fixture, candidate)
        self.assertEqual(fixture.counts()["dequeue_considered"], 0)
        self.assertIn(_hash(1), fixture.pending())
        self.assertIn(
            _hash(1),
            fixture.service._block_disposition_waiting_retries,
        )

    def test_the_passs_own_lease_is_the_only_flight_exempted(self) -> None:
        """The self-exemption is what makes the skip reachable at all.

        ``submit_next`` holds this hash's disposition lease by the time the
        skip runs, so #196's clause 1 would reject the row forever without
        it -- and any exemption wider than this one hash would let a
        genuinely mid-offer sibling through.
        """
        fixture = _offerable(CollapseFixture())
        target, other = _hash(1), _hash(2)
        first = fixture.seed([target])[0]
        second = fixture.seed([other])[0]
        held = fixture.server._claim_block_candidate_disposition(
            other,
            blocking=False,
        )
        self.assertIsNotNone(held)
        self.addCleanup(
            fixture.server._release_block_candidate_disposition,
            held,
        )
        # The pass's own flight does not count; the sibling's does.
        self.assertTrue(_skipped(fixture, first))
        self.assertNotIn(target, fixture.pending())
        _drive(fixture, second)
        self.assertIn(other, fixture.pending())


class DequeueSkipClauseTests(unittest.TestCase):
    """The chain clauses, at the dequeue boundary."""

    def test_a_lighter_occupant_never_destroys_a_heavier_sibling(self) -> None:
        """Clause 4b, and the per-row path re-decides the height instead.

        Under testnet4's 20-minute minimum-difficulty rule a light foreign
        block can occupy a height our full-difficulty sibling outweighs.
        Skipping it pre-offer would destroy a block the per-row path wins.
        """
        fixture = _offerable(
            CollapseFixture(
                rpc=CollapseChainRpc(
                    tip=DECIDED_WINNER,
                    tip_height=DECIDED_HEIGHT,
                    active={DECIDED_HEIGHT: DECIDED_WINNER},
                    bits={DECIDED_WINNER: MIN_WORK_BITS},
                )
            )
        )
        occupant = _scaled(MIN_WORK_BITS)
        heavier = fixture.seed([_hash(1)], network_difficulty=occupant * 8)[0]
        self.assertFalse(_skipped(fixture, heavier))
        self.assertIn(_hash(1), fixture.pending())
        # Still offered, and still handed to per-row accounting to decide.
        self.assertEqual(fixture.rpc.counts()["submitblock"], 1)
        self.assertEqual(_accounting_depth(fixture), 1)

    def test_an_equal_work_occupant_still_supersedes(self) -> None:
        fixture = _offerable(
            CollapseFixture(
                rpc=CollapseChainRpc(
                    tip=DECIDED_WINNER,
                    tip_height=DECIDED_HEIGHT,
                    active={DECIDED_HEIGHT: DECIDED_WINNER},
                    bits={DECIDED_WINNER: MIN_WORK_BITS},
                )
            )
        )
        occupant = _scaled(MIN_WORK_BITS)
        sibling = fixture.seed([_hash(2)], network_difficulty=occupant)[0]
        self.assertTrue(_skipped(fixture, sibling))

    def test_an_undecided_height_is_never_skipped(self) -> None:
        """Clause 4: a height above the tip is still ours to win."""
        fixture = _offerable(
            CollapseFixture(
                rpc=CollapseChainRpc(
                    tip=DECIDED_WINNER,
                    tip_height=DECIDED_HEIGHT - 1,
                    active={DECIDED_HEIGHT - 1: DECIDED_WINNER},
                )
            )
        )
        candidate = fixture.seed([_hash(1)])[0]
        self.assertFalse(_skipped(fixture, candidate))
        self.assertEqual(fixture.rpc.counts()["submitblock"], 1)

    def test_a_candidate_that_is_itself_the_occupant_is_never_skipped(self) -> None:
        fixture = _offerable(
            CollapseFixture(
                rpc=CollapseChainRpc(
                    tip=_hash(1),
                    tip_height=DECIDED_HEIGHT,
                    active={DECIDED_HEIGHT: _hash(1)},
                )
            )
        )
        candidate = fixture.seed([_hash(1)])[0]
        self.assertFalse(_skipped(fixture, candidate))


class DequeueSkipFailClosedTests(unittest.TestCase):
    """Every unreadable fact falls through to the offer path."""

    def _preserved_on(self, method: str) -> None:
        fixture = _offerable(CollapseFixture())
        candidate = fixture.seed([_hash(1)])[0]
        fixture.rpc.failures.add(method)
        self.assertFalse(_skipped(fixture, candidate))
        self.assertIn(_hash(1), fixture.pending())
        self.assertEqual(fixture.counts()["dequeue_preserved"], 1)
        self.assertGreaterEqual(fixture.counts()["fail_closed"], 1)
        self.assertEqual(fixture.rpc.counts()["submitblock"], 1)

    def test_a_failed_best_tip_read_preserves_the_candidate(self) -> None:
        self._preserved_on("getbestblockhash")

    def test_a_failed_active_block_read_preserves_the_candidate(self) -> None:
        self._preserved_on("getblockhash")

    def test_a_failed_header_read_preserves_the_candidate(self) -> None:
        self._preserved_on("getblockheader")

    def test_a_failed_tip_height_read_preserves_the_candidate(self) -> None:
        self._preserved_on("getblockcount")

    def test_a_failed_pool_block_probe_preserves_the_candidate(self) -> None:
        fixture = _offerable(CollapseFixture())
        candidate = fixture.seed([_hash(1)])[0]

        def explode(**_kwargs: object) -> object:
            raise RuntimeError("pool block state unavailable")

        fixture.ledger.inner.pool_block_state = explode
        self.assertFalse(_skipped(fixture, candidate))
        self.assertEqual(fixture.counts()["dequeue_preserved"], 1)
        self.assertGreaterEqual(fixture.counts()["fail_closed"], 1)
        self.assertEqual(fixture.rpc.counts()["submitblock"], 1)

    def test_a_ledger_without_a_pool_block_probe_never_skips(self) -> None:
        """Clause 2 has no durable answer, so nothing may be concluded."""
        fixture = _offerable(CollapseFixture(ledger=_NoPoolBlockLedger()))
        candidate = fixture.seed([_hash(1)])[0]
        self.assertFalse(_skipped(fixture, candidate))
        self.assertEqual(fixture.counts()["dequeue_considered"], 0)
        self.assertEqual(fixture.rpc.counts()["submitblock"], 1)

    def test_a_ledger_without_a_fenced_batch_write_never_skips(self) -> None:
        fixture = _offerable(CollapseFixture(ledger=_NoBatchLedger()))
        candidate = fixture.seed([_hash(1)])[0]
        self.assertFalse(_skipped(fixture, candidate))
        self.assertEqual(fixture.counts()["dequeue_considered"], 0)
        # Not even a chain read is spent on a structural absence.
        self.assertNotIn("getbestblockhash", fixture.rpc.counts())
        self.assertEqual(fixture.rpc.counts()["submitblock"], 1)

    def test_a_lost_fenced_write_preserves_the_candidate(self) -> None:
        """The write is the authority: a row it did not return is not ours."""
        fixture = _offerable(CollapseFixture())
        candidate = fixture.seed([_hash(1)])[0]
        fixture.ledger.abandon_hook = lambda _hashes, _error: ()
        self.assertFalse(_skipped(fixture, candidate))
        self.assertIn(_hash(1), fixture.pending())
        self.assertEqual(fixture.counts()["write_lost"], 1)
        self.assertEqual(fixture.counts()["dequeue_preserved"], 1)
        self.assertEqual(fixture.rpc.counts()["submitblock"], 1)

    def test_a_failed_fenced_write_preserves_the_candidate(self) -> None:
        def explode(_hashes: object, _error: object) -> object:
            raise RuntimeError("fenced write unavailable")

        fixture = _offerable(CollapseFixture())
        candidate = fixture.seed([_hash(1)])[0]
        fixture.ledger.abandon_hook = explode
        self.assertFalse(_skipped(fixture, candidate))
        self.assertIn(_hash(1), fixture.pending())
        self.assertEqual(fixture.rpc.counts()["submitblock"], 1)

    def test_a_tip_that_moved_under_the_lease_drops_the_write(self) -> None:
        """The pre-write revalidation runs at dequeue exactly as it does on a page."""
        fixture = _offerable(CollapseFixture())
        candidate = fixture.seed([_hash(1)])[0]
        seen: list[str] = []

        def move_tip(rpc: Any, method: str, _params: object) -> None:
            if method != "getbestblockhash":
                return
            seen.append(method)
            if len(seen) == 2:
                # Between selection and the fenced write, the candidate's
                # own parent became the tip again.
                rpc.tip = STORM_PARENT

        fixture.rpc.before_call = move_tip
        self.assertFalse(_skipped(fixture, candidate))
        self.assertIn(_hash(1), fixture.pending())
        self.assertEqual(fixture.counts()["revalidation_dropped"], 1)
        self.assertEqual(fixture.rpc.counts()["submitblock"], 1)


class DequeueSkipGuardTests(unittest.TestCase):
    """An escape from the skip must not strand the lease or the object."""

    def test_a_raising_skip_retains_the_candidate_and_frees_the_lease(
        self,
    ) -> None:
        fixture = _offerable(CollapseFixture())
        candidate = fixture.seed([_hash(1)], credit_share_on_accept=True)[0]

        def explode(_candidate: Any, **_kwargs: Any) -> bool:
            raise RuntimeError("skip exploded")

        fixture.service._skip_superseded_block_candidate_at_dequeue = explode
        with self.assertRaises(RuntimeError):
            with redirect_stdout(StringIO()):
                fixture.server.enqueue_block_candidate(candidate)
                fixture.server.submit_next_block_candidate(defer_accounting=True)
        # The lease is free again, so the next pass for this hash is not
        # parked forever...
        lease = fixture.server._claim_block_candidate_disposition(
            _hash(1),
            blocking=False,
        )
        self.assertIsNotNone(lease)
        fixture.server._release_block_candidate_disposition(lease)
        # ...and the object that owns the floor holder is still reachable.
        with fixture.server.lock:
            self.assertIs(fixture.service.retry_candidate, candidate)
        self.assertIn(_hash(1), fixture.pending())


class DequeueSkipCleanupTests(unittest.TestCase):
    """A skipped candidate is terminalized with #196's full cleanup."""

    def test_the_dequeued_objects_floor_holder_is_released(self) -> None:
        """Asserted directly: the holder is keyed by the object's identity.

        The apply's own pending-share step indexes holders by scanning the
        live and replay queues, and a dequeued object is in neither, so the
        skip has to release this one itself.
        """
        fixture = CollapseFixture()
        candidate = fixture.seed([_hash(1)], credit_share_on_accept=True)[0]
        writer = fixture.server._ensure_share_writer_service()
        writer.adopt_pending_share(candidate.pending_share)
        self.assertIn(id(candidate.pending_share), fixture.floor())
        self.assertTrue(_skipped(fixture, candidate))
        self.assertNotIn(id(candidate.pending_share), fixture.floor())

    def test_every_skipped_candidate_in_a_burst_releases_its_floor(self) -> None:
        fixture = CollapseFixture()
        hashes = [_hash(index) for index in range(1, 6)]
        candidates = fixture.seed(hashes, credit_share_on_accept=True)
        writer = fixture.server._ensure_share_writer_service()
        for candidate in candidates:
            writer.adopt_pending_share(candidate.pending_share)
        self.assertEqual(len(fixture.floor()), len(candidates))
        for candidate in candidates:
            _drive(fixture, candidate)
        self.assertEqual(fixture.counts()["dequeue_skipped"], len(candidates))
        self.assertEqual(fixture.floor(), {})
        self.assertEqual(fixture.pending(), set())

    def test_the_preview_barrier_is_withdrawn_and_left_untombstoned(self) -> None:
        fixture = CollapseFixture()
        candidate = fixture.seed([_hash(1)])[0]
        server = fixture.server
        server._register_outstanding_block_candidate(_hash(1))
        server._begin_accepted_block_payout_preview(
            _hash(1),
            block_height=DECIDED_HEIGHT,
        )
        server._mark_accepted_block_payout_landed(
            _hash(1),
            block_height=DECIDED_HEIGHT,
        )
        with server._accepted_block_payout_preview_condition:
            self.assertIn(_hash(1), server._accepted_block_payout_previews)
        self.assertTrue(_skipped(fixture, candidate))
        with server._accepted_block_payout_preview_condition:
            # Withdrawn *and* untombstoned: the durable row is terminal, so
            # nothing is left to fence, and a retained tombstone is exactly
            # the preview wait this work exists to end.
            self.assertNotIn(_hash(1), server._accepted_block_payout_previews)
            self.assertEqual(
                server._invalidated_accepted_block_payout_previews,
                {},
            )

    def test_the_skip_publishes_the_terminal_fence_and_the_counters(self) -> None:
        fixture = CollapseFixture()
        candidate = fixture.seed([_hash(1)])[0]
        self.assertTrue(_skipped(fixture, candidate))
        service = fixture.service
        self.assertIs(service._block_candidate_terminal_outcome(_hash(1)), False)
        with fixture.server.lock:
            self.assertNotIn(
                _hash(1),
                service._outstanding_block_candidate_hashes,
            )
            self.assertNotIn(_hash(1), service._block_fast_lane_reservations)
        self.assertEqual(
            fixture.server.block_candidate_abandoned_counts,
            {"stale-job": 1},
        )
        self.assertEqual(fixture.cleanup_backlog(), {})

    def test_a_skip_reserves_no_max_block_capacity(self) -> None:
        """A stale sibling must not consume capacity on its way to abandonment."""
        fixture = CollapseFixture()
        fixture.server.max_blocks = 1
        fixture.server.stop_after_block = False
        candidates = fixture.seed(
            [_hash(1), _hash(2), _hash(3)],
        )
        for candidate in candidates:
            _drive(fixture, candidate)
        self.assertEqual(fixture.counts()["dequeue_skipped"], 3)
        self.assertEqual(fixture.pending(), set())
        with fixture.server.lock:
            self.assertEqual(fixture.service._block_fast_lane_reservations, set())


class DequeueSkipPerturbationTests(unittest.TestCase):
    """No evidence shape ever produces a skip-only hash, in either view."""

    def test_no_perturbation_ever_produces_a_skip_only_hash(self) -> None:
        candidates = 20
        target = _hash(7)
        for view in ("live", "restart"):
            for kind in PERTURBATIONS:
                with self.subTest(view=view, perturbation=kind):
                    oracle = self._drain(candidates, view, kind, target, False)
                    skipped = self._drain(candidates, view, kind, target, True)
                    self.assertEqual(
                        skipped - oracle,
                        set(),
                        f"{kind} produced a skip-only hash in the {view} view",
                    )

    def test_five_of_six_shapes_exclude_the_perturbed_hash_outright(self) -> None:
        """And the sixth is the retry slot, which the dequeue itself consumes.

        ``perturb('retry_slot')`` moves a never-offered wakeup into the
        global retry holder -- what the shipped path does when the fast-lane
        reservation declines. ``submit_next`` clears that holder as it
        dequeues, so by the time the skip judges the object, the holder no
        longer names it and cannot be evidence about it. The per-row oracle
        abandons that row too (it re-offers and finds the tip moved), so the
        subset invariant above still holds and no block the per-row path
        would have won is destroyed; it is simply a place where the skip is
        no more conservative than the path it replaces.
        """
        candidates = 20
        target = _hash(7)
        for view in ("live", "restart"):
            for kind in PERTURBATIONS:
                with self.subTest(view=view, perturbation=kind):
                    skipped = self._drain(candidates, view, kind, target, True)
                    if kind == "retry_slot":
                        self.assertIn(target, skipped)
                    else:
                        self.assertNotIn(target, skipped)

    def _drain(
        self,
        candidates: int,
        view: str,
        kind: str,
        target: str,
        dequeue_skip: bool,
    ) -> set[str]:
        rig = CandidateStormRig(candidates=candidates)
        rig.seed_live()
        if view == "restart":
            rig.restart_and_enumerate()
        server = rig.restarted_server if view == "restart" else rig.live_server
        winner = rig.decide_height()
        with redirect_stdout(StringIO()):
            undo = rig.perturb(server, kind, target)
        skip_set = _record_skips(server) if dequeue_skip else set()
        try:
            report = rig.drain_per_row(
                server,
                stop_before_hash=winner,
                dequeue_skip=dequeue_skip,
            )
        finally:
            if undo is not None:
                undo()
        return skip_set if dequeue_skip else set(report.abandoned_hashes)


class DequeueAdmissionUnchangedTests(unittest.TestCase):
    """D5: the live queue stays FIFO and stays bounded at 32."""

    def test_the_live_queue_is_still_a_bounded_fifo_of_thirty_two(self) -> None:
        self.assertEqual(MAX_PENDING_BLOCK_CANDIDATES, 32)
        fixture = CollapseFixture()
        service = fixture.service
        service.candidate_queue = None
        candidates = fixture.seed([_hash(index) for index in range(1, 40)])
        with redirect_stdout(StringIO()):
            admitted = [
                fixture.server.enqueue_block_candidate(candidate)
                for candidate in candidates
            ]
        queue_obj = service.candidate_queue
        self.assertIsInstance(queue_obj, queue.Queue)
        self.assertNotIsInstance(queue_obj, queue.PriorityQueue)
        self.assertEqual(queue_obj.maxsize, 32)
        self.assertEqual(queue_obj.qsize(), 32)
        # The first 32 wakeups were admitted; the rest coalesced, and their
        # durable rows are what keeps them replayable.
        self.assertEqual(admitted, [True] * 32 + [False] * 7)
        self.assertEqual(int(service.wakeups_coalesced), 7)
        self.assertEqual(
            [
                str(item.submission.block_hash_hex).lower()
                for item in list(queue_obj.queue)
            ],
            [_hash(index) for index in range(1, 33)],
        )


if __name__ == "__main__":
    unittest.main()
