#!/usr/bin/env python3
"""Focused PRISM coordinator retained jobs tests."""
# ruff: noqa: F403, F405

from __future__ import annotations

import unittest
from tests.prism_vardiff_test_support import *


class PrismCoordinatorVardiffTests(unittest.TestCase):
    def test_clean_job_prunes_previous_active_prism_job(self) -> None:
        server = coordinator()
        state = client()
        state.worker = WorkerIdentity(
            username="miner-a",
            payout_address="miner-a",
            worker_name=None,
            script_pubkey_hex="5220" + "11" * 32,
            p2mr_program_hex="11" * 32,
        )
        server.jobs = {}
        counter = {"value": 0}

        def build_context(client: ClientState, *, clean_jobs: bool) -> object:
            counter["value"] += 1
            return SimpleNamespace(
                job=SimpleNamespace(job_id=f"job-{counter['value']}", share_difficulty=Decimal("1")),
                template={"previousblockhash": "00" * 32},
                collection_only=False,
            )

        server.build_job_for_client = build_context  # type: ignore[method-assign]
        server.send_difficulty = lambda *args, **kwargs: None
        server.send_job = lambda *args, **kwargs: None
        server.apply_job_difficulty = lambda *args, **kwargs: None

        server.maybe_send_job(state, clean_jobs=True)
        first_job_id = next(iter(state.active_job_ids))
        server.maybe_send_job(state, clean_jobs=True)
        second_job_id = next(iter(state.active_job_ids))

        self.assertNotEqual(first_job_id, second_job_id)
        self.assertNotIn(first_job_id, server.jobs)
        self.assertIn(second_job_id, server.jobs)

        state.sock = SimpleNamespace(shutdown=lambda *_args: None, close=lambda: None)
        server.disconnect_client(state)
        self.assertNotIn(second_job_id, server.jobs)
        self.assertEqual(state.active_job_ids, set())
    def test_non_clean_job_retention_caps_previous_active_prism_jobs(self) -> None:
        server = coordinator()
        state = client()
        state.worker = WorkerIdentity(
            username="miner-a",
            payout_address="miner-a",
            worker_name=None,
            script_pubkey_hex="5220" + "11" * 32,
            p2mr_program_hex="11" * 32,
        )
        server.jobs = {}
        counter = {"value": 0}

        def build_context(client: ClientState, *, clean_jobs: bool) -> object:
            counter["value"] += 1
            return SimpleNamespace(
                job=SimpleNamespace(job_id=f"job-{counter['value']}", share_difficulty=Decimal("1")),
                template={"previousblockhash": "00" * 32},
                collection_only=False,
            )

        server.build_job_for_client = build_context  # type: ignore[method-assign]
        server.send_difficulty = lambda *args, **kwargs: None
        server.send_job = lambda *args, **kwargs: None
        server.apply_job_difficulty = lambda *args, **kwargs: None

        total_jobs = MAX_ACTIVE_PRISM_JOBS_PER_CLIENT + 3
        for _ in range(total_jobs):
            server.maybe_send_job(state, clean_jobs=False)

        retained_ids = {
            f"job-{index}"
            for index in range(4, total_jobs + 1)
        }
        self.assertEqual(state.active_job_ids, retained_ids)
        self.assertEqual(set(server.jobs), retained_ids)
        self.assertNotIn("job-1", server.jobs)
        self.assertEqual(state.active_job.job.job_id, f"job-{total_jobs}")
    def test_normal_accepted_share_does_not_close_client(self) -> None:
        server, state, ledger = submit_coordinator()
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="bb" * 32,
            share_pass=True,
            block_pass=False,
        )

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            should_close = server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertFalse(should_close)
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(ledger.pending[0].share_id, "miner-a:" + "bb" * 32)
    def test_prior_tip_share_inside_grace_is_credited_without_submitblock(self) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        server, state, ledger = submit_coordinator(tip=old_tip)
        rpc = ParentTipRpc(tip=new_tip, parent=old_tip)
        server.rpc = rpc
        server.current_tip_first_seen = (new_tip, time.monotonic())
        server.stale_grace_seconds = 3
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="cc" * 32,
            share_pass=True,
            block_pass=True,
        )

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            should_close = server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertFalse(should_close)
        self.assertEqual(rpc.submitblock_calls, 0)
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(ledger.pending[0].credit_policy, PRISM_CREDIT_POLICY_STALE_GRACE)
        self.assertEqual(server.grace_credited_share_count, 1)
        self.assertEqual(server.worker_share_counts["miner-a"]["grace"], 1)
    def test_evicted_prior_tip_share_inside_grace_is_credited(self) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        server, state, ledger = submit_coordinator(tip=old_tip)
        rpc = ParentTipRpc(tip=new_tip, parent=old_tip)
        server.rpc = rpc
        server.current_tip_first_seen = (new_tip, time.monotonic())
        server.bury_evicted_job(state, "job-1")
        server.jobs.pop("job-1")
        state.active_job_ids.clear()
        submission = SimpleNamespace(
            header_hex="ad" * 80,
            block_hash_hex="cd" * 32,
            share_pass=True,
            block_pass=False,
        )

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            should_close = server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertFalse(should_close)
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(ledger.pending[0].credit_policy, PRISM_CREDIT_POLICY_STALE_GRACE)
    def test_evicted_same_tip_share_is_credited_without_stale_grace_policy(self) -> None:
        tip = "00" * 32
        server, state, ledger = submit_coordinator(tip=tip)
        server.bury_evicted_job(state, "job-1")
        server.jobs.pop("job-1")
        state.active_job_ids.clear()
        submission = SimpleNamespace(
            header_hex="af" * 80,
            block_hash_hex="cf" * 32,
            share_pass=True,
            block_pass=False,
        )

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            should_close = server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertFalse(should_close)
        self.assertEqual(len(ledger.pending), 1)
        self.assertIsNone(ledger.pending[0].credit_policy)
    def test_retained_share_dedup_uses_original_worker_after_reauthorization(self) -> None:
        tip = "00" * 32
        server, state, ledger = submit_coordinator(tip=tip)
        server.bury_evicted_job(state, "job-1")
        server.jobs.pop("job-1")
        state.active_job_ids.clear()
        submission = SimpleNamespace(
            header_hex="af" * 80,
            block_hash_hex="cf" * 32,
            share_pass=True,
            block_pass=False,
        )

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            self.assertFalse(
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )
            )
            state.username = "miner-b"
            state.worker = worker_identity("miner-b")
            with self.assertRaises(StratumError) as raised:
                server.handle_submit(
                    state,
                    ["miner-b", "job-1", "00" * 8, "00000001", "00000002"],
                )

        self.assertEqual(raised.exception.reason, PRISM_REJECTION_DUPLICATE_SHARE)
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(ledger.pending[0].share_id, "miner-a:" + "cf" * 32)
        self.assertEqual(server.worker_share_counts["miner-a"]["accepted"], 1)
        self.assertEqual(server.worker_share_counts["miner-b"]["accepted"], 0)
    def test_evicted_same_tip_share_survives_beyond_legacy_one_second_floor(self) -> None:
        tip = "00" * 32
        server, state, ledger = submit_coordinator(tip=tip)
        server.current_tip_first_seen = (tip, None)
        server.same_tip_job_retention_seconds = 30
        server.bury_evicted_job(state, "job-1", now=100.0)
        server.jobs.pop("job-1")
        state.active_job_ids.clear()
        submission = SimpleNamespace(
            header_hex="a1" * 80,
            block_hash_hex="c1" * 32,
            share_pass=True,
            block_pass=False,
        )

        with patch(
            "lab.prism.prism_coordinator.time.monotonic",
            return_value=102.0,
        ), patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            self.assertFalse(
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )
            )

        self.assertEqual(len(ledger.pending), 1)
        self.assertIsNone(ledger.pending[0].credit_policy)
        self.assertIn("job-1", server.evicted_job_graveyard)
        self.assertEqual(server.evicted_job_submit_counts["accepted_same_tip"], 1)
    def test_evicted_same_tip_submit_uses_original_job_difficulty(self) -> None:
        tip = "00" * 32
        server, state, ledger = submit_coordinator(tip=tip)
        server.current_tip_first_seen = (tip, None)
        original_context = server.jobs["job-1"]
        original_context.job.share_difficulty = Decimal("2")
        state.share_difficulty = Decimal("32")
        server.bury_evicted_job(state, "job-1")
        server.jobs.pop("job-1")
        state.active_job_ids.clear()
        submission = SimpleNamespace(
            header_hex="a2" * 80,
            block_hash_hex="c2" * 32,
            share_pass=True,
            block_pass=False,
        )

        def assemble(job: object, **_kwargs: object) -> object:
            self.assertIs(job, original_context.job)
            self.assertEqual(job.share_difficulty, Decimal("2"))
            return submission

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            side_effect=assemble,
        ):
            self.assertFalse(
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )
            )

        self.assertEqual(len(ledger.pending), 1)
        self.assertIsNone(ledger.pending[0].credit_policy)
    def test_same_tip_retention_ttl_and_capacity_are_bounded(self) -> None:
        tip = "00" * 32
        server, state, _ledger = submit_coordinator(tip=tip)
        server.current_tip_first_seen = (tip, None)
        server.same_tip_job_retention_seconds = 30
        server.same_tip_job_retention_per_connection = 2
        identity = state.worker
        for index in range(3):
            job_id = f"job-{index + 1}"
            server.jobs[job_id] = prism_context(job_id, tip, worker=identity)
            server.bury_evicted_job(state, job_id, now=100.0 + index)

        self.assertNotIn("job-1", server.evicted_job_graveyard)
        self.assertEqual(
            list(server.evicted_job_graveyard),
            ["job-2", "job-3"],
        )
        self.assertEqual(server.evicted_job_capacity_eviction_counts["connection"], 1)

        server.prune_evicted_job_graveyard(now=133.1)
        self.assertEqual(server.evicted_job_graveyard, {})
        self.assertEqual(server.evicted_job_expiration_counts["same_tip"], 2)
    def test_evicted_job_hit_is_constant_work_in_large_graveyard(self) -> None:
        tip = "00" * 32
        server, state, _ledger = submit_coordinator(tip=tip)
        server.current_tip_first_seen = (tip, None)
        server.same_tip_job_retention_seconds = 30
        server.same_tip_job_retention_per_connection = 4_096
        for index in range(4_096):
            job_id = f"retained-{index}"
            server.jobs[job_id] = prism_context(job_id, tip, worker=state.worker)
            server.bury_evicted_job(state, job_id, now=100.0, prune=False)

        self.assertEqual(len(server.evicted_job_graveyard), 4_096)
        self.assertEqual(len(server.evicted_same_tip_job_ids), 4_096)
        classify_calls = 0
        original_classify = server._evicted_job_class_locked

        def counted_classify(entry: object) -> str:
            nonlocal classify_calls
            classify_calls += 1
            return original_classify(entry)

        server._evicted_job_class_locked = counted_classify  # type: ignore[method-assign]
        with patch("lab.prism.prism_coordinator.time.monotonic", return_value=101.0):
            for _ in range(100):
                self.assertIsNotNone(
                    server.evicted_job_entry(state, "retained-2048")
                )

        self.assertEqual(classify_calls, 100)
    def test_pool_width_does_not_evict_other_connections_retained_jobs(self) -> None:
        tip = "00" * 32
        server, _state, _ledger = submit_coordinator(tip=tip)
        server.current_tip_first_seen = (tip, None)
        server.same_tip_job_retention_seconds = 30
        server.same_tip_job_retention_per_connection = 1
        clients: list[ClientState] = []
        for index in range(4_097):
            state = client()
            state.connection_id = index + 1
            state.worker = worker_identity(f"miner-{index}")
            clients.append(state)
            job_id = f"wide-{index}"
            server.jobs[job_id] = prism_context(job_id, tip, worker=state.worker)
            server.bury_evicted_job(state, job_id, now=100.0, prune=False)

        self.assertEqual(len(server.evicted_job_graveyard), 4_097)
        self.assertIn("wide-0", server.evicted_job_graveyard)
        self.assertIn("wide-4096", server.evicted_job_graveyard)

        replacement_id = "wide-0-replacement"
        server.jobs[replacement_id] = prism_context(
            replacement_id,
            tip,
            worker=clients[0].worker,
        )
        server.bury_evicted_job(
            clients[0],
            replacement_id,
            now=101.0,
            prune=False,
        )

        self.assertNotIn("wide-0", server.evicted_job_graveyard)
        self.assertIn("wide-1", server.evicted_job_graveyard)
        self.assertIn(replacement_id, server.evicted_job_graveyard)
        self.assertEqual(len(server.evicted_job_graveyard), 4_097)
        self.assertEqual(server.evicted_job_capacity_eviction_counts["connection"], 1)
    def test_tip_change_and_disconnect_remove_retained_contexts(self) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        server, state, _ledger = submit_coordinator(tip=old_tip)
        server.current_tip_first_seen = (old_tip, None)
        server.stale_grace_seconds = 0
        server.bury_evicted_job(state, "job-1")
        self.assertIn("job-1", server.evicted_job_graveyard)

        server.observe_tip_first_seen(new_tip)
        self.assertNotIn("job-1", server.evicted_job_graveyard)

        server.current_tip_first_seen = (new_tip, None)
        server.jobs["job-2"] = prism_context("job-2", new_tip, worker=state.worker)
        server.bury_evicted_job(state, "job-2")
        server.clients = {state}
        state.close = lambda: None  # type: ignore[method-assign]
        server.disconnect_client(state)
        self.assertEqual(server.evicted_job_graveyard, {})
    def test_tip_flip_reanchors_retained_job_grace_to_client_delivery(self) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        server, state, _ledger = submit_coordinator(tip=old_tip)
        server.clients = {state}
        server.current_tip_first_seen = (old_tip, None)
        server.stale_grace_seconds = 3
        server.bury_evicted_job(state, "job-1", now=100.0)

        with patch("lab.prism.prism_coordinator.time.monotonic", return_value=120.0):
            server.observe_tip_first_seen(new_tip)

        # Burial predates the flip by twenty seconds, but grace does not begin
        # until this connection actually receives replacement work.
        server.prune_evicted_job_graveyard(now=130.0)
        self.assertIn("job-1", server.evicted_job_graveyard)

        state.tip_work_delivered = (new_tip, 130.0)
        server.prune_evicted_job_graveyard(now=132.9)
        self.assertIn("job-1", server.evicted_job_graveyard)
        server.prune_evicted_job_graveyard(now=133.1)
        self.assertNotIn("job-1", server.evicted_job_graveyard)
        self.assertEqual(server.evicted_job_expiration_counts["stale_grace"], 1)
    def test_tip_flip_prunes_by_chain_parent_when_poller_skips_observed_tip(self) -> None:
        observed_tip = "00" * 32
        intermediate_tip = "11" * 32
        current_tip = "22" * 32
        server, state, _ledger = submit_coordinator(tip=intermediate_tip)
        server.clients = {state}
        server.current_tip_first_seen = (observed_tip, None)
        server.stale_grace_seconds = 3
        server.jobs["older-job"] = prism_context(
            "older-job",
            observed_tip,
            worker=state.worker,
        )
        server.bury_evicted_job(state, "older-job", now=100.0, prune=False)
        server.bury_evicted_job(state, "job-1", now=110.0, prune=False)
        server.rpc = ParentTipRpc(tip=current_tip, parent=intermediate_tip)

        with patch("lab.prism.prism_coordinator.time.monotonic", return_value=120.0):
            server.observe_tip_first_seen(current_tip)

        # The poller's previous observation is not authoritative. Tip
        # observation proactively loads the actual parent, drops older work,
        # and preserves the intermediate-tip context that submit can credit.
        self.assertNotIn("older-job", server.evicted_job_graveyard)
        self.assertIn("job-1", server.evicted_job_graveyard)
        self.assertEqual(
            server.current_tip_parent_hash(current_tip),
            intermediate_tip,
        )
        entry = server.evicted_job_entry(state, "job-1")
        self.assertIsNotNone(entry)
        assert entry is not None
        self.assertEqual(
            server.evicted_submit_context(state, entry, current_tip),
            (entry.context, PRISM_CREDIT_POLICY_STALE_GRACE),
        )
    def test_slow_parent_lookup_cannot_overwrite_newer_tip_parent_cache(self) -> None:
        old_tip = "00" * 32
        old_parent = "ff" * 32
        new_tip = "11" * 32
        new_parent = old_tip
        server = coordinator()
        server.current_tip_first_seen = (old_tip, None)
        server.current_tip_observation_sequence = 1
        server.current_tip_parent = None

        def overtake_parent_lookup(tip_hash: str) -> str:
            self.assertEqual(tip_hash, old_tip)
            with server.lock:
                server.current_tip_first_seen = (new_tip, 100.0)
                server.current_tip_observation_sequence = 2
                server.current_tip_parent = (new_tip, new_parent)
            return old_parent

        server._fetch_tip_parent_hash = overtake_parent_lookup  # type: ignore[method-assign]

        self.assertEqual(server.current_tip_parent_hash(old_tip), old_parent)
        self.assertEqual(server.current_tip_parent, (new_tip, new_parent))
    def test_retained_same_tip_duplicate_remains_duplicate_share(self) -> None:
        tip = "00" * 32
        server, state, ledger = submit_coordinator(tip=tip)
        server.current_tip_first_seen = (tip, None)
        server.bury_evicted_job(state, "job-1")
        server.jobs.pop("job-1")
        state.active_job_ids.clear()
        submission = SimpleNamespace(
            header_hex="a3" * 80,
            block_hash_hex="c3" * 32,
            share_pass=True,
            block_pass=False,
        )
        params = ["miner-a", "job-1", "00" * 8, "00000001", "00000002"]

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            self.assertFalse(server.handle_submit(state, params))
            with self.assertRaises(StratumError) as raised:
                server.handle_submit(state, params)

        self.assertEqual(raised.exception.reason, PRISM_REJECTION_DUPLICATE_SHARE)
        self.assertEqual(len(ledger.pending), 1)
        self.assertIn("job-1", server.evicted_job_graveyard)
        metrics = server.metrics_payload()
        self.assertIn('qbit_prism_evicted_job_contexts{class="same_tip"} 1', metrics)
        self.assertIn(
            'qbit_prism_evicted_job_submits_total{outcome="accepted_same_tip"} 1',
            metrics,
        )
    def test_pool_closed_submit_rejects_before_any_share_accounting(self) -> None:
        # Post-close submits must not inflate submitted totals (the
        # stale-percent denominator), per-worker submitted counters, or the
        # vardiff window; only the pool-closed rejection itself is recorded.
        server, state, ledger = submit_coordinator()
        server.accepted_block_count = 1
        server.max_blocks = 1
        state.vardiff_config = SimpleNamespace(enabled=True)
        submitted_before = server.submitted_share_count

        with self.assertRaises(StratumError) as raised:
            server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertEqual(raised.exception.reason, PRISM_REJECTION_POOL_CLOSED)
        self.assertEqual(server.submitted_share_count, submitted_before)
        # The rejection itself may admit the label, but no submission counted.
        self.assertEqual(server.worker_share_counts["miner-a"]["submitted"], 0)
        self.assertEqual(state.vardiff_window_submitted, 0)
        self.assertEqual(len(ledger.pending), 0)
        self.assertEqual(
            server.worker_rejection_counts[("miner-a", PRISM_REJECTION_POOL_CLOSED)], 1
        )
    def test_malformed_submit_does_not_diverge_worker_and_aggregate_submitted(self) -> None:
        # A malformed-ntime submit must count identically in the per-worker and
        # aggregate submitted counters (i.e. not at all) so the two never drift.
        server, state, _ledger = submit_coordinator()
        submitted_before = server.submitted_share_count

        with self.assertRaises(StratumError) as raised:
            server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "bad-ntime", "00000002"],
            )

        self.assertEqual(raised.exception.reason, PRISM_REJECTION_INVALID_NTIME_OR_NONCE)
        self.assertEqual(server.submitted_share_count, submitted_before)
        self.assertEqual(server.worker_share_counts["miner-a"]["submitted"], 0)
        self.assertEqual(
            server.worker_rejection_counts[("miner-a", PRISM_REJECTION_INVALID_NTIME_OR_NONCE)],
            1,
        )
    def test_stale_grace_closed_when_refresh_path_has_not_observed_tip(self) -> None:
        # Only blockpoll/blockwait may open the grace window. If the refresh path
        # has not anchored the new tip (current_tip_first_seen is None) and only
        # this submit's getbestblockhash sees it, the prior-tip share must reject
        # as stale-job -- not get credited from a submit-anchored window.
        old_tip = "00" * 32
        new_tip = "11" * 32
        server, state, ledger = submit_coordinator(tip=old_tip)
        rpc = ParentTipRpc(tip=new_tip, parent=old_tip)
        server.rpc = rpc
        server.current_tip_first_seen = None
        server.stale_grace_seconds = 3
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="cc" * 32,
            share_pass=True,
            block_pass=False,
        )

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            with self.assertRaises(StratumError) as raised:
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )

        self.assertEqual(raised.exception.reason, PRISM_REJECTION_STALE_JOB)
        self.assertEqual(len(ledger.pending), 0)
        self.assertEqual(rpc.submitblock_calls, 0)
        # The submit must not have anchored the window either.
        self.assertIsNone(server.current_tip_first_seen)
    def test_stale_grace_rejected_after_window_expires(self) -> None:
        # This connection received current-tip work well outside the grace
        # window; a prior-tip share arriving now must reject rather than be
        # credited late.
        old_tip = "00" * 32
        new_tip = "11" * 32
        server, state, ledger = submit_coordinator(tip=old_tip)
        rpc = ParentTipRpc(tip=new_tip, parent=old_tip)
        server.rpc = rpc
        server.stale_grace_seconds = 3
        server.current_tip_first_seen = (new_tip, time.monotonic() - 10)
        state.tip_work_delivered = (new_tip, time.monotonic() - 10)
        submission = SimpleNamespace(
            header_hex="ab" * 80,
            block_hash_hex="ce" * 32,
            share_pass=True,
            block_pass=False,
        )

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            with self.assertRaises(StratumError) as raised:
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )

        self.assertEqual(raised.exception.reason, PRISM_REJECTION_STALE_JOB)
        self.assertEqual(len(ledger.pending), 0)
        self.assertEqual(rpc.submitblock_calls, 0)
    def test_stale_grace_open_until_connection_receives_new_tip_work(self) -> None:
        # The refresh pass may be slow or aborted (reorg reconcile failure,
        # transient build errors). Until THIS connection is sent current-tip
        # work, its prior-tip shares are still in flight and must stay
        # creditable even after the global first-seen stamp ages past the
        # grace window.
        old_tip = "00" * 32
        new_tip = "11" * 32
        server, state, ledger = submit_coordinator(tip=old_tip)
        rpc = ParentTipRpc(tip=new_tip, parent=old_tip)
        server.rpc = rpc
        server.stale_grace_seconds = 3
        server.current_tip_first_seen = (new_tip, time.monotonic() - 10)
        state.tip_work_delivered = (old_tip, time.monotonic() - 60)
        submission = SimpleNamespace(
            header_hex="ac" * 80,
            block_hash_hex="cd" * 32,
            share_pass=True,
            block_pass=False,
        )

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            should_close = server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertFalse(should_close)
        self.assertEqual(rpc.submitblock_calls, 0)
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(ledger.pending[0].credit_policy, PRISM_CREDIT_POLICY_STALE_GRACE)
    def test_stale_grace_window_runs_from_per_connection_delivery_not_first_seen(self) -> None:
        # A slow refresh pass can deliver current-tip work to a connection
        # after the global first-seen stamp has already aged past the grace
        # window. The window for that connection runs from ITS delivery.
        old_tip = "00" * 32
        new_tip = "11" * 32
        server, state, ledger = submit_coordinator(tip=old_tip)
        rpc = ParentTipRpc(tip=new_tip, parent=old_tip)
        server.rpc = rpc
        server.stale_grace_seconds = 3
        server.current_tip_first_seen = (new_tip, time.monotonic() - 10)
        state.tip_work_delivered = (new_tip, time.monotonic() - 1)
        submission = SimpleNamespace(
            header_hex="ae" * 80,
            block_hash_hex="cb" * 32,
            share_pass=True,
            block_pass=False,
        )

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            should_close = server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertFalse(should_close)
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(ledger.pending[0].credit_policy, PRISM_CREDIT_POLICY_STALE_GRACE)
    def test_startup_baseline_tip_does_not_open_stale_grace_window(self) -> None:
        # The first tip observed after process start is a baseline, not a tip
        # flip: it must not open the grace window. A later real flip must.
        old_tip = "00" * 32
        new_tip = "11" * 32
        server, state, _ledger = submit_coordinator(tip=old_tip)
        server.stale_grace_seconds = 3
        server.current_tip_first_seen = None

        server.observe_tip_first_seen(new_tip)
        self.assertEqual(server.current_tip_first_seen, (new_tip, None))
        self.assertFalse(server.stale_grace_deadline_open(state, new_tip))

        # A change away from the observed baseline is a real flip and opens
        # the window for connections that have not yet received the new work.
        flip_tip = "22" * 32
        server.observe_tip_first_seen(flip_tip)
        self.assertIsNotNone(server.current_tip_first_seen[1])
        self.assertTrue(server.stale_grace_deadline_open(state, flip_tip))
    def test_note_tip_work_delivered_keeps_first_delivery_per_tip(self) -> None:
        # Same-tip template refreshes must not slide the grace anchor forward.
        server, state, _ledger = submit_coordinator()
        tip = "11" * 32

        server.note_tip_work_delivered(state, tip)
        first = state.tip_work_delivered
        self.assertEqual(first[0], tip)
        server.note_tip_work_delivered(state, tip)
        self.assertEqual(state.tip_work_delivered, first)

        # A new tip re-anchors.
        server.note_tip_work_delivered(state, "22" * 32)
        self.assertEqual(state.tip_work_delivered[0], "22" * 32)
        self.assertGreaterEqual(state.tip_work_delivered[1], first[1])
    def test_evicted_graveyard_keeps_unexpired_entries_above_previous_cap_for_grace_credit(self) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        server, state, ledger = submit_coordinator(tip=old_tip)
        rpc = ParentTipRpc(tip=new_tip, parent=old_tip)
        server.rpc = rpc
        server.current_tip_first_seen = (new_tip, time.monotonic())
        context = server.jobs["job-1"]
        evicted_at = time.monotonic()
        server.evicted_job_graveyard = {
            "job-1": (context, state.connection_id, evicted_at),
        }
        previous_hard_cap = 512
        for index in range(previous_hard_cap):
            server.evicted_job_graveyard[f"filler-{index}"] = (
                context,
                state.connection_id,
                evicted_at + 0.001 + (index / 1_000_000),
            )
        server.prune_evicted_job_graveyard(now=evicted_at + 0.5)
        self.assertIn("job-1", server.evicted_job_graveyard)
        server.jobs.pop("job-1")
        state.active_job_ids.clear()
        submission = SimpleNamespace(
            header_hex="ae" * 80,
            block_hash_hex="ce" * 32,
            share_pass=True,
            block_pass=False,
        )

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            should_close = server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertFalse(should_close)
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(ledger.pending[0].credit_policy, PRISM_CREDIT_POLICY_STALE_GRACE)
    def test_stale_grace_parent_rpc_failure_rejects_as_backend_unavailable(self) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        server, state, _ledger = submit_coordinator(tip=old_tip)
        server.rpc = TipRpc(new_tip)
        server.current_tip_first_seen = (new_tip, time.monotonic())

        with self.assertRaises(StratumError) as raised:
            server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertEqual(raised.exception.reason, PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE)
    def test_unknown_job_rejects_before_getbestblockhash_rpc(self) -> None:
        class CountingTipRpc(TipRpc):
            def __init__(self, tip: str) -> None:
                super().__init__(tip)
                self.getbest_calls = 0

            def call(self, method: str, params: list[object] | None = None) -> object:
                if method == "getbestblockhash":
                    self.getbest_calls += 1
                return super().call(method, params)

        server, state, _ledger = submit_coordinator()
        rpc = CountingTipRpc("00" * 32)
        server.rpc = rpc

        with self.assertRaises(StratumError) as raised:
            server.handle_submit(
                state,
                ["miner-a", "garbage-job", "00" * 8, "00000001", "00000002"],
            )

        self.assertEqual(raised.exception.reason, PRISM_REJECTION_UNKNOWN_JOB)
        self.assertEqual(rpc.getbest_calls, 0)
    def test_submit_passes_negotiated_version_bits_and_mask_to_stratum_assembly(self) -> None:
        server, state, _ledger = submit_coordinator()
        state.version_mask = 0x1FFFE000
        submission = SimpleNamespace(
            header_hex="ac" * 80,
            block_hash_hex="ba" * 32,
            share_pass=True,
            block_pass=False,
        )

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ) as assemble_submission:
            server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "00000001", "00000002", "00002000"],
            )

        self.assertEqual(assemble_submission.call_args.kwargs["version_bits_hex"], "00002000")
        self.assertEqual(assemble_submission.call_args.kwargs["version_mask"], 0x1FFFE000)
    def test_address_worker_submit_accrues_to_base_payout_address(self) -> None:
        server, state, ledger = submit_coordinator()
        username = f"{PAYOUT_ADDRESS}.rig-a"
        worker = WorkerIdentity(
            username=username,
            payout_address=PAYOUT_ADDRESS,
            worker_name="rig-a",
            script_pubkey_hex="5220" + "44" * 32,
            p2mr_program_hex="44" * 32,
        )
        state.username = username
        state.worker = worker
        server.jobs["job-1"].worker = worker
        server.share_weights_by_username = {PAYOUT_ADDRESS: 9}
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="bc" * 32,
            share_pass=True,
            block_pass=False,
        )

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            should_close = server.handle_submit(
                state,
                [username, "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertFalse(should_close)
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(ledger.pending[0].share_id, username + ":" + "bc" * 32)
        self.assertEqual(ledger.pending[0].miner_id, PAYOUT_ADDRESS)
        self.assertEqual(ledger.pending[0].order_key, PAYOUT_ADDRESS)
        self.assertEqual(ledger.pending[0].share_difficulty, 9)
    def test_address_worker_submit_still_requires_authorized_full_username(self) -> None:
        server, state, ledger = submit_coordinator()
        username = f"{PAYOUT_ADDRESS}.rig-a"
        worker = WorkerIdentity(
            username=username,
            payout_address=PAYOUT_ADDRESS,
            worker_name="rig-a",
            script_pubkey_hex="5220" + "44" * 32,
            p2mr_program_hex="44" * 32,
        )
        state.username = username
        state.worker = worker
        server.jobs["job-1"].worker = worker

        with self.assertRaises(StratumError) as raised:
            server.handle_submit(
                state,
                [PAYOUT_ADDRESS, "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertEqual(raised.exception.code, 20)
        self.assertEqual(len(ledger.pending), 0)
    def test_stale_tip_rejects_without_appending_share(self) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        server, state, ledger = submit_coordinator(tip=old_tip)
        server.rpc = ParentTipRpc(tip=new_tip, parent="22" * 32)

        with self.assertRaises(StratumError) as raised:
            server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertEqual(raised.exception.code, 21)
        self.assertEqual(raised.exception.reason, PRISM_REJECTION_STALE_JOB)
        self.assertEqual(len(ledger.pending), 0)
        self.assertEqual(server.stale_share_count, 1)
        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_STALE_JOB], 1)
    def _pending_append(self, tag: str, accepted_at_ms: int = 2) -> PendingShareAppend:
        from lab.prism.share_ledger import PendingShare

        return PendingShareAppend(
            pending_share=PendingShare(
                share_id=f"miner-a:{tag}",
                miner_id="miner-a",
                order_key="miner-a",
                p2mr_program_hex="11" * 32,
                share_difficulty=1,
                network_difficulty=1,
                template_height=10,
                job_id="job-1",
                job_issued_at_ms=1,
                accepted_at_ms=accepted_at_ms,
                ntime=1_700_000_000,
            ),
            username="miner-a",
            job_id="job-1",
            block_hash_hex=tag * 32,
            collection_only=False,
            credit_policy=None,
        )
