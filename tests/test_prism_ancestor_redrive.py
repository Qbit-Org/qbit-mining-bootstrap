#!/usr/bin/env python3
"""In-process targeted ancestor re-drive for stuck payout transitions (#190).

The production wedge these tests reproduce (2026-08-24 smoke test): a found
block's finalization deferred on an ancestor whose accepted payout
transition was armed and landed but whose preview never published. The
retained-retry loop could only re-check the transition -- 37 cycles of
``ledger-confirmation-failed`` over 906 seconds -- while the durable-replay
pass, the one path that provably resolves the ancestor (startup replay
resolved the same state in ~19s after the watchdog restart), was
short-circuited by the very retry candidate it needed to unblock.

The fix under test: a per-ancestor streak of finalization deferrals arms a
targeted re-drive; the next ``replay_pending`` pass bypasses the
retry/live/replay-queue short-circuits, re-enumerates the durable outbox
through the same adoption startup replay uses (same pagination, same
landing-class budget), and either re-queues the ancestor's pending row for
ordinary replay finalization or -- when the completed enumeration proves no
pending row exists, nothing in-process owns the hash, the block is durably
confirmed, and a fresh chain probe proves it active on the node's chain --
clears the stale transition. The attempt cap bounds the mechanism per
ancestor (each attempt needs a fresh deferral streak); on exhaustion
deferrals fall back to the pre-#190 behavior and the publication-progress
watchdog remains the backstop.
"""
# ruff: noqa: E402

from __future__ import annotations

import dataclasses
import sys
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lab.prism.share_ledger import PendingShare, SingleWriterShareLedger
from tests.prism_coordinator_test_support import (
    coordinator as light_coordinator,
)
from tests.prism_vardiff_test_support import block_candidate, submit_coordinator


ANCESTOR_HASH = "aa" * 32
CHILD_HASH = "cc" * 32
PARENT_TIP = "00" * 32


def _pending_share(block_hash: str, stamp: int) -> PendingShare:
    return PendingShare(
        share_id=f"miner-a:{block_hash}",
        miner_id="miner-a",
        order_key="miner-a",
        p2mr_program_hex="11" * 32,
        share_difficulty=1,
        network_difficulty=1,
        template_height=9,
        job_id="job-1",
        job_issued_at_ms=1,
        accepted_at_ms=stamp,
        ntime=1,
    )


def _wedged_coordinator():
    """The incident shape: retained child, armed+landed ancestor, no preview.

    The ancestor's durable outbox row stays pending (its replay source), its
    in-memory transition is armed and landed with the preview never
    published, and the child found on top of it is retained in the retry
    slot -- exactly the state the 2026-08-24 run wedged in.
    """
    server, state, _recording = submit_coordinator()
    ledger = SingleWriterShareLedger()
    server.ledger = ledger
    server.block_candidate_retry_initial_seconds = 0.0
    server.block_candidate_retry_max_seconds = 0.0
    ancestor = block_candidate(
        server,
        state,
        SimpleNamespace(
            coinbase_tx_hex="00",
            block_hash_hex=ANCESTOR_HASH,
            block_hex="00",
            share_pass=False,
            block_pass=True,
        ),
        pending_share=_pending_share(ANCESTOR_HASH, 1),
    )
    ledger.persist_block_candidate_intent(server.block_candidate_intent(ancestor))
    server._begin_accepted_block_payout_preview(ANCESTOR_HASH, block_height=10)
    server._mark_accepted_block_payout_landed(ANCESTOR_HASH, block_height=10)
    child_context = SimpleNamespace(
        job=SimpleNamespace(
            job_id="job-2",
            share_target=server.jobs["job-1"].job.share_target,
            share_difficulty=server.jobs["job-1"].job.share_difficulty,
            transaction_hexes=(),
        ),
        template={
            "previousblockhash": ANCESTOR_HASH,
            "height": 11,
            "coinbasevalue": 50_00000000,
        },
        found_block={"network_difficulty": 1},
        issued_at_ms=12346,
        collection_only=False,
        worker=state.worker,
        shares_json=[],
        prior_balances=[],
    )
    server.jobs["job-2"] = child_context
    state.active_job_ids.add("job-2")
    child = block_candidate(
        server,
        state,
        SimpleNamespace(
            coinbase_tx_hex="00",
            block_hash_hex=CHILD_HASH,
            block_hex="00",
            share_pass=False,
            block_pass=True,
        ),
        job_id="job-2",
        pending_share=_pending_share(CHILD_HASH, 2),
    )
    # The child's intent is durable too (persisted at admission in the
    # incident), so a forced enumeration sees a pending row for a hash the
    # retry holder still owns -- adoption must skip it.
    ledger.persist_block_candidate_intent(server.block_candidate_intent(child))
    server._retain_block_candidate_for_retry(child)
    return server, state, ledger, child


def _script_chain_probe(server, result):
    """Pin the sweep's node-side probe; returns the recorded calls."""
    service = server._ensure_block_candidate_service()
    calls: list[tuple[str, int | None]] = []

    def probe(block_hash, *, expected_height=None):
        calls.append((str(block_hash).lower(), expected_height))
        return result

    service._block_candidate_chain_probe = probe
    return calls


class SweepLedger(SingleWriterShareLedger):
    """Empty outbox plus a caller-scripted durable pool-block verdict."""

    def __init__(self, block_state: dict[str, object] | None) -> None:
        super().__init__()
        self.scripted_block_state = block_state
        self.pool_block_state_calls: list[str] = []

    def pool_block_state(self, *, block_hash: str) -> dict[str, object] | None:
        self.pool_block_state_calls.append(block_hash)
        return self.scripted_block_state


class AncestorRedriveWedgeTests(unittest.TestCase):
    """Issue #190 finding 1: the wedge, and the re-drive that resolves it."""

    def _child_defers(self, server) -> bool:
        return server._defer_for_pending_parent_payout_transition(
            block_hash=CHILD_HASH,
            parent_hash=ANCESTOR_HASH,
            parent_height=10,
            worker=None,
        )

    def test_deferral_streak_redrives_the_stuck_ancestor(self) -> None:
        server, _state, _ledger, _child = _wedged_coordinator()
        server.accepted_parent_redrive_defer_threshold = 3
        server.accepted_parent_redrive_attempt_max = 3
        service = server._ensure_block_candidate_service()

        # The pre-fix wedge: every retry cycle re-checks the transition and
        # defers again, while the durable replay pass -- the only code that
        # can resolve the ancestor -- short-circuits on the retained retry
        # candidate it exists to unblock.
        with patch("builtins.print"):
            for _cycle in range(2):
                self.assertTrue(self._child_defers(server))
                self.assertEqual(server.replay_pending_block_candidates(), 0)
            self.assertFalse(service._ancestor_redrive_owed())

            # Crossing the deferral threshold arms the targeted re-drive.
            self.assertTrue(self._child_defers(server))
        self.assertTrue(service._ancestor_redrive_owed())
        self.assertEqual(int(server.accepted_parent_redrive_attempt_count), 1)

        # The armed pass bypasses the retry short-circuit and re-adopts the
        # ancestor's pending row through the same durable-replay code
        # startup replay runs. The child's pending row is durable too, but
        # its live copy sits in the retry holder, so adoption must skip it
        # rather than install a second in-process copy under the holder.
        with patch("builtins.print"):
            self.assertEqual(server.replay_pending_block_candidates(), 1)
        self.assertFalse(service._ancestor_redrive_owed())
        with server.lock:
            self.assertIn(ANCESTOR_HASH, server._block_replay_inflight_hashes)
            self.assertNotIn(CHILD_HASH, server._block_replay_inflight_hashes)

        # The submitter's next pass runs the re-driven ancestor while the
        # child's retry is parked in backoff. The finalization tail itself
        # is owned by the block-finalization suites; the double here
        # resolves the transition through the same production payout-state
        # seam that tail uses on completion.
        landed: list[str] = []

        def finalize(candidate, **_kwargs) -> bool:
            block_hash = str(candidate.submission.block_hash_hex).lower()
            landed.append(block_hash)
            server._clear_accepted_block_payout_preview(block_hash)
            return True

        server.submit_block_candidate = finalize
        # Post-accept job fanout is the tip-refresh owner's machinery, not
        # this wedge's; the replay-reconstructed client is not a full
        # ClientState in this fixture.
        server.refresh_jobs_after_pending_accepted_block = (
            lambda *_args, **_kwargs: 0
        )
        server._block_candidate_retry_not_before = {
            CHILD_HASH: time.monotonic() + 300.0
        }
        with patch("builtins.print"):
            self.assertTrue(server.submit_next_block_candidate())
        self.assertEqual(landed, [ANCESTOR_HASH])
        with server._accepted_block_payout_preview_condition:
            self.assertNotIn(
                ANCESTOR_HASH, server._accepted_block_payout_previews
            )

        # With the ancestor resolved, the retained child's next retry passes
        # the fence and its finalization completes.
        with patch("builtins.print"):
            self.assertFalse(self._child_defers(server))
        self.assertEqual(int(server.accepted_parent_redrive_resolved_count), 1)
        server._clear_block_candidate_retry_state(CHILD_HASH)
        with patch("builtins.print"):
            self.assertTrue(server.submit_next_block_candidate())
        self.assertEqual(landed, [ANCESTOR_HASH, CHILD_HASH])

    def test_streak_below_threshold_never_arms_a_redrive(self) -> None:
        server, _state, _ledger, _child = _wedged_coordinator()
        server.accepted_parent_redrive_defer_threshold = 5
        service = server._ensure_block_candidate_service()

        with patch("builtins.print"):
            for _cycle in range(4):
                self.assertTrue(self._child_defers(server))
                self.assertEqual(server.replay_pending_block_candidates(), 0)
        self.assertFalse(service._ancestor_redrive_owed())
        self.assertEqual(int(server.accepted_parent_redrive_attempt_count), 0)

    def test_redrive_respects_the_attempt_cap_and_falls_back(self) -> None:
        server, _rpc = light_coordinator()
        server.accepted_parent_redrive_defer_threshold = 1
        server.accepted_parent_redrive_attempt_max = 2
        service = server._ensure_block_candidate_service()
        server._begin_accepted_block_payout_preview(ANCESTOR_HASH, block_height=9)
        server._mark_accepted_block_payout_landed(ANCESTOR_HASH, block_height=9)

        def child_defers() -> bool:
            return server._defer_for_pending_parent_payout_transition(
                block_hash=CHILD_HASH,
                parent_hash=ANCESTOR_HASH,
                parent_height=9,
                worker=None,
            )

        with patch("builtins.print"):
            for expected_attempts in (1, 2):
                self.assertTrue(child_defers())
                self.assertEqual(
                    int(server.accepted_parent_redrive_attempt_count),
                    expected_attempts,
                )
                self.assertTrue(service._ancestor_redrive_owed())
                self.assertEqual(
                    service._consume_ancestor_redrive_requests(),
                    (ANCESTOR_HASH,),
                )
            # The cap is exhausted: the wedge keeps deferring exactly as it
            # did before the fix, no further pass is armed, and exhaustion
            # is counted once for the ancestor.
            for _cycle in range(3):
                self.assertTrue(child_defers())
                self.assertFalse(service._ancestor_redrive_owed())
        self.assertEqual(int(server.accepted_parent_redrive_attempt_count), 2)
        self.assertEqual(int(server.accepted_parent_redrive_exhausted_count), 1)
        server._clear_accepted_block_payout_preview(ANCESTOR_HASH)

    def test_armed_request_does_not_double_arm_before_consumption(self) -> None:
        server, _rpc = light_coordinator()
        server.accepted_parent_redrive_defer_threshold = 1
        server.accepted_parent_redrive_attempt_max = 5
        service = server._ensure_block_candidate_service()
        server._begin_accepted_block_payout_preview(ANCESTOR_HASH, block_height=9)
        server._mark_accepted_block_payout_landed(ANCESTOR_HASH, block_height=9)

        with patch("builtins.print"):
            for _cycle in range(3):
                self.assertTrue(
                    server._defer_for_pending_parent_payout_transition(
                        block_hash=CHILD_HASH,
                        parent_hash=ANCESTOR_HASH,
                        parent_height=9,
                        worker=None,
                    )
                )
        # Deferrals while a pass is already armed count toward nothing new.
        self.assertEqual(int(server.accepted_parent_redrive_attempt_count), 1)
        self.assertEqual(
            service._consume_ancestor_redrive_requests(), (ANCESTOR_HASH,)
        )
        server._clear_accepted_block_payout_preview(ANCESTOR_HASH)


class AncestorRedriveSweepTests(unittest.TestCase):
    """The no-pending-row wedge shape: converge with durable state, or stand
    down for the watchdog."""

    def _defer_once(self, server) -> None:
        with patch("builtins.print"):
            self.assertTrue(
                server._defer_for_pending_parent_payout_transition(
                    block_hash=CHILD_HASH,
                    parent_hash=ANCESTOR_HASH,
                    parent_height=10,
                    worker=None,
                )
            )

    def _armed_sweep_coordinator(self, block_state, *, chain_probe=True):
        server, _state, _recording = submit_coordinator()
        ledger = SweepLedger(block_state)
        server.ledger = ledger
        server.accepted_parent_redrive_defer_threshold = 1
        probe_calls = _script_chain_probe(server, chain_probe)
        server._begin_accepted_block_payout_preview(
            ANCESTOR_HASH, block_height=10
        )
        server._mark_accepted_block_payout_landed(ANCESTOR_HASH, block_height=10)
        return server, ledger, probe_calls

    def test_confirmed_stale_transition_is_cleared(self) -> None:
        server, ledger, probe_calls = self._armed_sweep_coordinator(
            {
                "block_hash": ANCESTOR_HASH,
                "block_height": 10,
                "parent_hash": PARENT_TIP,
                "chain_state": "confirmed",
                "maturity_state": "immature",
            }
        )
        self._defer_once(server)
        with patch("builtins.print"):
            self.assertEqual(server.replay_pending_block_candidates(), 0)
        self.assertEqual(ledger.pool_block_state_calls, [ANCESTOR_HASH])
        # The clear demanded the node-side proof at the transition's height.
        self.assertEqual(probe_calls, [(ANCESTOR_HASH, 10)])
        with server._accepted_block_payout_preview_condition:
            self.assertNotIn(
                ANCESTOR_HASH, server._accepted_block_payout_previews
            )
            self.assertNotIn(
                ANCESTOR_HASH,
                server._invalidated_accepted_block_payout_previews,
            )
        with patch("builtins.print"):
            self.assertFalse(
                server._defer_for_pending_parent_payout_transition(
                    block_hash=CHILD_HASH,
                    parent_hash=ANCESTOR_HASH,
                    parent_height=10,
                    worker=None,
                )
            )
        self.assertEqual(int(server.accepted_parent_redrive_resolved_count), 1)

    def test_unconfirmed_transition_is_left_for_the_watchdog(self) -> None:
        server, ledger, _probe_calls = self._armed_sweep_coordinator(None)
        self._defer_once(server)
        with patch("builtins.print"):
            self.assertEqual(server.replay_pending_block_candidates(), 0)
        self.assertEqual(ledger.pool_block_state_calls, [ANCESTOR_HASH])
        with server._accepted_block_payout_preview_condition:
            self.assertIn(ANCESTOR_HASH, server._accepted_block_payout_previews)
        self._defer_once(server)
        self.assertEqual(int(server.accepted_parent_redrive_resolved_count), 0)

    def test_reversed_pool_block_transition_is_left_alone(self) -> None:
        server, ledger, _probe_calls = self._armed_sweep_coordinator(
            {
                "block_hash": ANCESTOR_HASH,
                "block_height": 10,
                "parent_hash": PARENT_TIP,
                "chain_state": "confirmed",
                "maturity_state": "reversed",
            }
        )
        self._defer_once(server)
        with patch("builtins.print"):
            self.assertEqual(server.replay_pending_block_candidates(), 0)
        with server._accepted_block_payout_preview_condition:
            self.assertIn(ANCESTOR_HASH, server._accepted_block_payout_previews)

    def test_transition_owned_by_an_in_process_lane_is_left_alone(self) -> None:
        server, ledger, _probe_calls = self._armed_sweep_coordinator(
            {
                "block_hash": ANCESTOR_HASH,
                "block_height": 10,
                "parent_hash": PARENT_TIP,
                "chain_state": "confirmed",
                "maturity_state": "immature",
            }
        )
        service = server._ensure_block_candidate_service()
        service._ensure_block_replay_state()
        with server.lock:
            server._block_replay_inflight_hashes.add(ANCESTOR_HASH)
        self._defer_once(server)
        with patch("builtins.print"):
            self.assertEqual(server.replay_pending_block_candidates(), 0)
        # An inflight hash means ordinary replay finalization still owns the
        # resolution; the sweep never consults the ledger for it.
        self.assertEqual(ledger.pool_block_state_calls, [])
        with server._accepted_block_payout_preview_condition:
            self.assertIn(ANCESTOR_HASH, server._accepted_block_payout_previews)

    def test_unprovable_chain_activity_is_left_for_the_watchdog(self) -> None:
        """A durably confirmed row alone is not proof against a fresh reorg.

        Reconciliation fails closed while a landed transition exists, so a
        block qbitd has already reorged out can still read confirmed in the
        durable row. The sweep must demand the node-side probe the landing
        demands and stand down on anything short of a definite True.
        """
        for probe_verdict in (None, False):
            with self.subTest(probe_verdict=probe_verdict):
                server, ledger, probe_calls = self._armed_sweep_coordinator(
                    {
                        "block_hash": ANCESTOR_HASH,
                        "block_height": 10,
                        "parent_hash": PARENT_TIP,
                        "chain_state": "confirmed",
                        "maturity_state": "immature",
                    },
                    chain_probe=probe_verdict,
                )
                self._defer_once(server)
                with patch("builtins.print"):
                    self.assertEqual(
                        server.replay_pending_block_candidates(), 0
                    )
                self.assertEqual(
                    ledger.pool_block_state_calls, [ANCESTOR_HASH]
                )
                self.assertEqual(probe_calls, [(ANCESTOR_HASH, 10)])
                with server._accepted_block_payout_preview_condition:
                    self.assertIn(
                        ANCESTOR_HASH, server._accepted_block_payout_previews
                    )
                self.assertEqual(
                    int(server.accepted_parent_redrive_resolved_count), 0
                )

    def test_retry_held_ancestor_is_left_to_its_owner(self) -> None:
        """The in-process-ownership guard itself, reached with nothing adopted.

        The ancestor's own candidate sits in the retry holder while its
        outbox row is already terminal, so the forced pass enumerates
        nothing, the adopted short-circuit cannot fire, and the sweep runs.
        The guard must see the held copy and stand down before the durable
        read or the chain probe -- a guard that wrongly answered "not owned"
        would clear the transition out from under the live lane and fails
        every assertion below.
        """
        server, state, _recording = submit_coordinator()
        ledger = SweepLedger(
            {
                "block_hash": ANCESTOR_HASH,
                "block_height": 10,
                "parent_hash": PARENT_TIP,
                "chain_state": "confirmed",
                "maturity_state": "immature",
            }
        )
        server.ledger = ledger
        server.accepted_parent_redrive_defer_threshold = 1
        probe_calls = _script_chain_probe(server, True)
        ancestor = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex=ANCESTOR_HASH,
                block_hex="00",
                share_pass=False,
                block_pass=True,
            ),
            pending_share=_pending_share(ANCESTOR_HASH, 1),
        )
        server._begin_accepted_block_payout_preview(
            ANCESTOR_HASH, block_height=10
        )
        server._mark_accepted_block_payout_landed(ANCESTOR_HASH, block_height=10)
        server._retain_block_candidate_for_retry(ancestor)
        self._defer_once(server)
        with patch("builtins.print"):
            self.assertEqual(server.replay_pending_block_candidates(), 0)
        with server.lock:
            self.assertNotIn(
                ANCESTOR_HASH, server._block_replay_inflight_hashes
            )
        self.assertEqual(ledger.pool_block_state_calls, [])
        self.assertEqual(probe_calls, [])
        with server._accepted_block_payout_preview_condition:
            self.assertIn(ANCESTOR_HASH, server._accepted_block_payout_previews)


class AncestorRedriveBookkeepingTests(unittest.TestCase):
    """The per-ancestor record's cap, streak, eviction, and counter rules."""

    def _tracker(self, *, threshold: int, cap: int):
        server, _rpc = light_coordinator()
        server.accepted_parent_redrive_defer_threshold = threshold
        server.accepted_parent_redrive_attempt_max = cap
        return server, server._ensure_block_candidate_service()

    def test_each_attempt_needs_a_fresh_deferral_streak(self) -> None:
        """Deferrals behind an armed request must not bank toward the next.

        Otherwise attempts 2..N arm on single deferrals right after
        consumption and the cap exhausts before the first pass's adoption
        had any chance to resolve the ancestor.
        """
        server, service = self._tracker(threshold=3, cap=3)
        with patch("builtins.print"):
            for _cycle in range(3):
                service.note_pending_parent_transition_deferral(
                    CHILD_HASH, ANCESTOR_HASH
                )
            self.assertEqual(int(server.accepted_parent_redrive_attempt_count), 1)
            # Deferrals while armed bank nothing.
            for _cycle in range(4):
                service.note_pending_parent_transition_deferral(
                    CHILD_HASH, ANCESTOR_HASH
                )
            self.assertEqual(
                service._consume_ancestor_redrive_requests(), (ANCESTOR_HASH,)
            )
            # A fresh threshold of deferrals is required for attempt 2.
            for _cycle in range(2):
                service.note_pending_parent_transition_deferral(
                    CHILD_HASH, ANCESTOR_HASH
                )
                self.assertEqual(
                    int(server.accepted_parent_redrive_attempt_count), 1
                )
            service.note_pending_parent_transition_deferral(
                CHILD_HASH, ANCESTOR_HASH
            )
        self.assertEqual(int(server.accepted_parent_redrive_attempt_count), 2)

    def test_zero_attempt_cap_disables_the_mechanism_silently(self) -> None:
        """Cap 0 means off: no arming, no exhaustion signal, no bookkeeping.

        The exhausted counter is the alert-worthy pre-restart signal; an
        installation that disabled the feature must not emit it.
        """
        server, service = self._tracker(threshold=1, cap=0)
        with patch("builtins.print"):
            for _cycle in range(5):
                service.note_pending_parent_transition_deferral(
                    CHILD_HASH, ANCESTOR_HASH
                )
        self.assertFalse(service._ancestor_redrive_owed())
        self.assertEqual(int(server.accepted_parent_redrive_attempt_count), 0)
        self.assertEqual(int(server.accepted_parent_redrive_exhausted_count), 0)
        self.assertEqual(service._ancestor_redrive_records, {})
        self.assertEqual(service._ancestor_redrive_last_blocking, {})

    def test_resolution_without_a_consumed_pass_is_not_counted(self) -> None:
        """An armed-but-unconsumed request that resolves on its own is the
        mechanism standing by, not succeeding; the resolved counter must not
        inflate against attempts."""
        server, service = self._tracker(threshold=1, cap=3)
        with patch("builtins.print"):
            service.note_pending_parent_transition_deferral(
                CHILD_HASH, ANCESTOR_HASH
            )
            self.assertTrue(service._ancestor_redrive_owed())
            service.note_pending_parent_transition_resolved(CHILD_HASH)
        self.assertEqual(int(server.accepted_parent_redrive_resolved_count), 0)
        self.assertEqual(service._ancestor_redrive_records, {})
        self.assertEqual(service._ancestor_redrive_last_blocking, {})

    def test_child_entry_eviction_never_cancels_an_ancestor_record(self) -> None:
        """The two registries are different hash spaces; trimming one must
        not reach into the other.

        Hash B is both a child (B deferred on A once) and an armed ancestor
        (C's deferrals armed B's re-drive). Flooding the child-keyed index
        past its bound evicts B's stale child entry; B's armed record must
        survive that eviction untouched.
        """
        server, service = self._tracker(threshold=3, cap=3)
        ancestor_b = "bb" * 32
        with patch("builtins.print"):
            service.note_pending_parent_transition_deferral(
                ancestor_b, ANCESTOR_HASH
            )
            for _cycle in range(3):
                service.note_pending_parent_transition_deferral(
                    CHILD_HASH, ancestor_b
                )
            self.assertEqual(int(server.accepted_parent_redrive_attempt_count), 1)
            for index in range(70):
                service.note_pending_parent_transition_deferral(
                    "%064x" % (0x1000 + index), ancestor_b
                )
        with server.lock:
            self.assertNotIn(ancestor_b, service._ancestor_redrive_last_blocking)
        self.assertEqual(
            service._consume_ancestor_redrive_requests(), (ancestor_b,)
        )
        self.assertEqual(int(server.accepted_parent_redrive_attempt_count), 1)

    def test_eviction_never_drops_the_actively_wedged_ancestor(self) -> None:
        """The lifetime cap must survive the pathological many-ancestor
        stream the eviction bound exists for.

        An exhausted ancestor that keeps deferring re-inserts its whole
        record every deferral, so a storm of distinct arming ancestors must
        never evict it -- eviction would discard the exhaustion latch and
        re-arm re-drives past the cap.
        """
        server, service = self._tracker(threshold=1, cap=1)
        with patch("builtins.print"):
            service.note_pending_parent_transition_deferral(
                CHILD_HASH, ANCESTOR_HASH
            )
            self.assertEqual(
                service._consume_ancestor_redrive_requests(), (ANCESTOR_HASH,)
            )
            service.note_pending_parent_transition_deferral(
                CHILD_HASH, ANCESTOR_HASH
            )
            self.assertEqual(
                int(server.accepted_parent_redrive_exhausted_count), 1
            )
            for index in range(70):
                service.note_pending_parent_transition_deferral(
                    "%064x" % (0x2000 + index),
                    "%064x" % (0x3000 + index),
                )
                # The wedge keeps deferring throughout the storm.
                service.note_pending_parent_transition_deferral(
                    CHILD_HASH, ANCESTOR_HASH
                )
            attempts_after_storm = int(server.accepted_parent_redrive_attempt_count)
            service.note_pending_parent_transition_deferral(
                CHILD_HASH, ANCESTOR_HASH
            )
        self.assertEqual(
            int(server.accepted_parent_redrive_attempt_count),
            attempts_after_storm,
        )
        self.assertEqual(int(server.accepted_parent_redrive_exhausted_count), 1)
        with server.lock:
            record = service._ancestor_redrive_records.get(ANCESTOR_HASH)
        self.assertIsNotNone(record)
        self.assertTrue(record.exhausted)


class AncestorRedriveRunLoopTests(unittest.TestCase):
    """The submitter loop's fall-through: an armed re-drive must reach the
    replay entrypoint even when immediate live work keeps succeeding --
    sustained live traffic taking the ``continue`` every pass is exactly how
    the incident starved the one path that resolves a stuck ancestor."""

    def _run_one_iteration(self, *, arm_redrive: bool):
        server, _rpc = light_coordinator()
        server.accepted_parent_redrive_defer_threshold = 1
        server.accepted_parent_redrive_attempt_max = 3
        service = server._ensure_block_candidate_service()
        if arm_redrive:
            with patch("builtins.print"):
                service.note_pending_parent_transition_deferral(
                    CHILD_HASH, ANCESTOR_HASH
                )
            self.assertTrue(service._ancestor_redrive_owed())
        server._start_block_accounting_thread = lambda: None
        submits: list[float | None] = []

        def submit_next(timeout=None, *, defer_accounting=False):
            submits.append(timeout)
            return True

        server.submit_next_block_candidate = submit_next
        replays: list[bool] = []
        stop_states = iter([False, True])
        stop = SimpleNamespace(is_set=lambda: next(stop_states))
        service.ports = dataclasses.replace(
            service.ports,
            stop_event=lambda: stop,
            replay_entrypoint=lambda: (replays.append(True), 0)[1],
        )
        # Immediate work is available and keeps succeeding.
        service.retry_candidate = object()
        with patch("builtins.print"):
            service.run()
        return replays, submits

    def test_armed_redrive_falls_through_to_the_replay_entrypoint(self) -> None:
        replays, submits = self._run_one_iteration(arm_redrive=True)
        self.assertEqual(replays, [True])
        self.assertEqual(submits, [None, 1.0])

    def test_without_a_redrive_immediate_success_short_circuits(self) -> None:
        replays, submits = self._run_one_iteration(arm_redrive=False)
        self.assertEqual(replays, [])
        self.assertEqual(submits, [None])


class AncestorRedriveForcedPassTests(unittest.TestCase):
    """A forced re-drive pass runs the startup enumeration's code: keyset
    pagination when the ledger supports it, on the landing-class budget."""

    def test_forced_pass_paginates_on_the_landing_budget(self) -> None:
        server, _state, _recording = submit_coordinator()
        ledger = SweepLedger(None)
        server.ledger = ledger
        server.accepted_parent_redrive_defer_threshold = 1
        _script_chain_probe(server, True)
        server._begin_accepted_block_payout_preview(
            ANCESTOR_HASH, block_height=10
        )
        server._mark_accepted_block_payout_landed(ANCESTOR_HASH, block_height=10)
        original_call = server._run_block_submitter_ledger_call
        outbox_queries: list[tuple[object, object, object]] = []

        def spying_call(key, label, fn, **kwargs):
            if label == "replay-outbox-query":
                outbox_queries.append(
                    (key, kwargs.get("call_class"), kwargs.get("timeout_seconds"))
                )
            return original_call(key, label, fn, **kwargs)

        server._run_block_submitter_ledger_call = spying_call
        with patch("builtins.print"):
            self.assertTrue(
                server._defer_for_pending_parent_payout_transition(
                    block_hash=CHILD_HASH,
                    parent_hash=ANCESTOR_HASH,
                    parent_height=10,
                    worker=None,
                )
            )
            self.assertEqual(server.replay_pending_block_candidates(), 0)
        self.assertEqual(len(outbox_queries), 1)
        key, call_class, timeout_seconds = outbox_queries[0]
        # The paginated fetch keys its dedupe on (label, limit, page,
        # cursor); the legacy widening window keys on (label, limit) alone.
        self.assertEqual(len(key), 4)
        self.assertEqual(call_class, "landing")
        self.assertIsNotNone(timeout_seconds)


if __name__ == "__main__":
    unittest.main()
