#!/usr/bin/env python3
"""Focused PRISM coordinator tip refresh delivery tests."""
# ruff: noqa: F821

from __future__ import annotations

import unittest
from tests import prism_coordinator_test_support as _job_support
from tests import prism_vardiff_test_support as _vardiff_support


class _VardiffSupportTestCase(unittest.TestCase):
    def setUp(self) -> None:
        globals().update(
            {name: getattr(_vardiff_support, name) for name in _vardiff_support.__all__}
        )


class _JobSupportTestCase(unittest.TestCase):
    def setUp(self) -> None:
        globals().update(
            {name: getattr(_job_support, name) for name in _job_support.__all__}
        )


class PrismCoordinatorVardiffTests(_VardiffSupportTestCase):
    def test_tip_change_refreshes_clean_job_and_old_job_becomes_stale_without_reconnect(self) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        server = coordinator()
        server.accepted_block_count = 0
        server.max_blocks = 1
        server.stop_after_block = True
        server.stale_grace_seconds = 0
        server.jobs = {}
        server.recent_share_keys = set()
        server.share_weights_by_username = {}
        ledger = RecordingLedger()
        server.ledger = ledger
        worker = worker_identity()
        state = client()
        state.username = worker.username
        state.worker = worker
        state.share_difficulty = Decimal("1")
        sent: list[dict[str, object]] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]
        server.clients = {state}

        old_context = prism_context("old-job", old_tip, worker=worker)
        state.active_job = old_context
        state.active_job_ids = {"old-job"}
        server.jobs["old-job"] = old_context
        server.tip_template_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=old_tip,
            previousblockhash=old_tip,
            template_fingerprint=qbit_template_fingerprint(old_context.template),
        )
        server.rpc = TipTemplateRpc(tip=new_tip, template=gbt_template(new_tip, height=11))

        def build_fresh_job(client: ClientState, *, clean_jobs: bool) -> object:
            self.assertIs(client, state)
            return prism_context(
                "fresh-job",
                new_tip,
                worker=worker,
                difficulty=client.pending_share_difficulty or client.share_difficulty,
                clean_jobs=clean_jobs,
            )

        server.build_job_for_client = build_fresh_job  # type: ignore[method-assign]

        refreshed = server.poll_qbit_tip_template_once()

        self.assertEqual(refreshed, 1)
        self.assertEqual(server.tip_refresh_job_count, 1)
        self.assertIn(state, server.clients)
        self.assertNotIn("old-job", server.jobs)
        self.assertEqual(state.active_job_ids, {"fresh-job"})
        self.assertIn("fresh-job", server.jobs)
        self.assertEqual([payload["method"] for payload in sent], ["mining.set_difficulty", "mining.notify"])
        self.assertEqual(sent[1]["params"][0], "fresh-job")
        self.assertTrue(sent[1]["params"][8])

        with self.assertRaises(StratumError) as raised:
            server.handle_submit(
                state,
                ["miner-a", "old-job", "00" * 8, "00000001", "00000002"],
            )
        self.assertEqual(raised.exception.code, 21)
        self.assertEqual(raised.exception.reason, PRISM_REJECTION_UNKNOWN_JOB)
        self.assertEqual(server.stale_share_count, 1)
        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_UNKNOWN_JOB], 1)
        self.assertEqual(len(ledger.pending), 0)

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
                ["miner-a", "fresh-job", "00" * 8, "00000001", "00000002"],
            )

        self.assertFalse(should_close)
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(ledger.pending[0].job_id, "fresh-job")
        self.assertIn(state, server.clients)
    def test_tip_refresh_rpc_race_blocks_mismatched_tip_template_snapshot(self) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        server = coordinator()
        server.jobs = {}
        worker = worker_identity()
        state = client()
        state.username = worker.username
        state.worker = worker
        sent: list[dict[str, object]] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]
        server.clients = {state}

        old_context = prism_context("old-job", old_tip, worker=worker)
        state.active_job = old_context
        state.active_job_ids = {"old-job"}
        server.jobs["old-job"] = old_context
        server.tip_template_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=old_tip,
            previousblockhash=old_tip,
            template_fingerprint=qbit_template_fingerprint(old_context.template),
        )
        server.rpc = TipTemplateRpc(tip=old_tip, template=gbt_template(new_tip, height=11))

        with self.assertRaisesRegex(
            TemplateRefreshBlocked,
            "tip changed while fetching block template",
        ):
            server.poll_qbit_tip_template_once()

        self.assertIs(state.active_job, old_context)
        self.assertEqual(server.jobs, {"old-job": old_context})
        self.assertEqual(state.active_job_ids, {"old-job"})
        self.assertEqual(sent, [])
        # Tip observation is recorded as a detection before expensive template
        # construction so obsolete builders can be cancelled immediately, but
        # submit authority is published only alongside a coherent snapshot.
        # The incoherent template never enters the artifact cache, client job
        # maps, or the published tip state.
        self.assertEqual(server.latest_detected_tip[0], old_tip)
        self.assertIsNone(server.current_tip_first_seen)
        self.assertIsNone(server._template_artifacts)
    def test_slow_tip_poll_cannot_regress_newer_blockwait_observation(self) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        server = coordinator()
        old_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=old_tip,
            previousblockhash=old_tip,
            template_fingerprint="22" * 32,
        )
        server.rpc = TipRpc(old_tip)
        self.assertTrue(server.observe_tip_first_seen(old_tip))

        def overtake_poll() -> QbitTipTemplateSnapshot:
            self.assertTrue(server.observe_tip_for_refresh(new_tip))
            return old_snapshot

        server.fetch_qbit_tip_template_snapshot = overtake_poll  # type: ignore[method-assign]

        with self.assertRaisesRegex(
            TemplateRefreshSuperseded,
            "tip/template poll was superseded during template fetch",
        ):
            server.poll_qbit_tip_template_once()

        self.assertEqual(server.latest_detected_tip[0], new_tip)
        self.assertEqual(server.current_tip_first_seen[0], old_tip)
        self.assertIsNone(server.tip_template_snapshot)
        self.assertIsNone(
            getattr(server, "template_refresh_failure_started_monotonic", None)
        )
        self.assertTrue(server._consume_tip_refresh_retry())
        self.assertFalse(server._consume_tip_refresh_retry())
    def test_same_tip_template_refresh_sends_non_clean_job_and_keeps_old_job_submittable(self) -> None:
        tip = "00" * 32
        server = coordinator()
        server.accepted_block_count = 0
        server.max_blocks = 1
        server.stop_after_block = True
        server.jobs = {}
        server.recent_share_keys = set()
        server.share_weights_by_username = {}
        ledger = RecordingLedger()
        server.ledger = ledger
        worker = worker_identity()
        state = client()
        state.username = worker.username
        state.worker = worker
        state.share_difficulty = Decimal("1")
        sent: list[dict[str, object]] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]
        server.clients = {state}

        old_context = prism_context("old-job", tip, worker=worker)
        state.active_job = old_context
        state.active_job_ids = {"old-job"}
        server.jobs["old-job"] = old_context
        server.tip_template_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=tip,
            previousblockhash=tip,
            template_fingerprint=qbit_template_fingerprint(old_context.template),
        )
        refreshed_template = gbt_template(tip, height=10, coinbasevalue=50_00000001)
        server.rpc = TipTemplateRpc(tip=tip, template=refreshed_template)

        def build_fresh_job(client: ClientState, *, clean_jobs: bool) -> object:
            self.assertIs(client, state)
            self.assertFalse(clean_jobs)
            fresh_context = prism_context(
                "fresh-job",
                tip,
                worker=worker,
                difficulty=client.pending_share_difficulty or client.share_difficulty,
                clean_jobs=clean_jobs,
            )
            fresh_context.template["coinbasevalue"] = refreshed_template["coinbasevalue"]
            fresh_context.template_fingerprint = qbit_template_fingerprint(fresh_context.template)
            return fresh_context

        server.build_job_for_client = build_fresh_job  # type: ignore[method-assign]

        refreshed = server.poll_qbit_tip_template_once()

        self.assertEqual(refreshed, 1)
        self.assertEqual(server.tip_refresh_job_count, 1)
        self.assertIn(state, server.clients)
        self.assertIn("old-job", server.jobs)
        self.assertIn("fresh-job", server.jobs)
        self.assertEqual(state.active_job_ids, {"old-job", "fresh-job"})
        self.assertEqual([payload["method"] for payload in sent], ["mining.set_difficulty", "mining.notify"])
        self.assertEqual(sent[1]["params"][0], "fresh-job")
        self.assertFalse(sent[1]["params"][8])
        self.assertIn("qbit_prism_active_job_contexts 2", server.metrics_payload())

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
                ["miner-a", "old-job", "00" * 8, "00000001", "00000002"],
            )

        self.assertFalse(should_close)
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(ledger.pending[0].job_id, "old-job")
        self.assertEqual(server.stale_share_count, 0)
        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_UNKNOWN_JOB], 0)
        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_STALE_JOB], 0)
    def test_tip_refresh_uses_pending_vardiff_difficulty_for_consistent_pair(self) -> None:
        old_tip = "00" * 32
        new_tip = "22" * 32
        server = coordinator()
        server.jobs = {}
        worker = worker_identity()
        state = client()
        state.username = worker.username
        state.worker = worker
        state.share_difficulty = Decimal("1")
        state.pending_share_difficulty = Decimal("8")
        sent: list[dict[str, object]] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]
        server.clients = {state}
        old_context = prism_context("old-job", old_tip, worker=worker, difficulty=Decimal("1"))
        state.active_job = old_context
        state.active_job_ids = {"old-job"}
        server.jobs["old-job"] = old_context
        server.tip_template_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=old_tip,
            previousblockhash=old_tip,
            template_fingerprint=qbit_template_fingerprint(old_context.template),
        )
        server.rpc = TipTemplateRpc(tip=new_tip, template=gbt_template(new_tip, height=11))

        def build_fresh_job(client: ClientState, *, clean_jobs: bool) -> object:
            return prism_context(
                "fresh-vardiff-job",
                new_tip,
                worker=worker,
                difficulty=server.desired_client_share_difficulty(client),
                clean_jobs=clean_jobs,
            )

        server.build_job_for_client = build_fresh_job  # type: ignore[method-assign]

        refreshed = server.poll_qbit_tip_template_once()

        self.assertEqual(refreshed, 1)
        self.assertEqual(sent[0]["method"], "mining.set_difficulty")
        self.assertEqual(sent[0]["params"], [8.0])
        self.assertEqual(sent[1]["method"], "mining.notify")
        self.assertEqual(sent[1]["params"][0], "fresh-vardiff-job")
        self.assertTrue(sent[1]["params"][8])
        self.assertEqual(state.share_difficulty, Decimal("8"))
        self.assertIsNone(state.pending_share_difficulty)
        self.assertEqual(server.jobs["fresh-vardiff-job"].job.share_difficulty, Decimal("8"))
    def test_tip_refresh_build_failure_keeps_client_connected_and_old_job_registered(self) -> None:
        old_tip = "00" * 32
        new_tip = "33" * 32
        server = coordinator()
        server.jobs = {}
        worker = worker_identity()
        state = client()
        state.username = worker.username
        state.worker = worker
        sent: list[dict[str, object]] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]
        server.clients = {state}
        old_context = prism_context("old-job", old_tip, worker=worker)
        state.active_job = old_context
        state.active_job_ids = {"old-job"}
        server.jobs["old-job"] = old_context
        server.tip_template_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=old_tip,
            previousblockhash=old_tip,
            template_fingerprint=qbit_template_fingerprint(old_context.template),
        )
        server.rpc = TipTemplateRpc(tip=new_tip, template=gbt_template(new_tip, height=11))

        def failing_build(client: ClientState, *, clean_jobs: bool) -> object:
            raise RuntimeError("transient getblocktemplate failure")

        server.build_job_for_client = failing_build  # type: ignore[method-assign]

        with self.assertRaisesRegex(TemplateRefreshBlocked, "no refreshed work was issued"):
            server.poll_qbit_tip_template_once()

        self.assertEqual(server.job_build_failure_count, 1)
        self.assertEqual(server.tip_refresh_job_count, 0)
        self.assertIn(state, server.clients)
        self.assertEqual(state.active_job_ids, {"old-job"})
        self.assertIn("old-job", server.jobs)
        self.assertEqual(sent, [])
    def test_tip_refresh_build_failure_is_not_masked_by_disconnected_client(self) -> None:
        old_tip = "00" * 32
        new_tip = "33" * 32
        server = coordinator()
        server.jobs = {}

        build_failed = client()
        build_failed.worker = worker_identity("miner-build-failed")
        build_failed.username = build_failed.worker.username
        build_failed.active_job = prism_context(
            "old-build-failed-job", old_tip, worker=build_failed.worker
        )
        build_failed.active_job_ids = {"old-build-failed-job"}

        disconnected = client()
        disconnected.connection_id = 2
        disconnected.worker = worker_identity("miner-disconnected")
        disconnected.username = disconnected.worker.username
        disconnected.active_job = prism_context(
            "old-disconnected-job", old_tip, worker=disconnected.worker
        )
        disconnected.active_job_ids = {"old-disconnected-job"}

        def disconnect_on_send(_payload: object) -> None:
            raise OSError("socket closed")

        disconnected.send = disconnect_on_send  # type: ignore[method-assign]
        server.clients = {build_failed, disconnected}
        server.jobs = {
            "old-build-failed-job": build_failed.active_job,
            "old-disconnected-job": disconnected.active_job,
        }
        server.tip_template_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=old_tip,
            previousblockhash=old_tip,
            template_fingerprint=qbit_template_fingerprint(build_failed.active_job.template),
        )
        server.rpc = TipTemplateRpc(tip=new_tip, template=gbt_template(new_tip, height=11))

        def mixed_build(state: ClientState, *, clean_jobs: bool) -> object:
            if state is build_failed:
                raise RuntimeError("template build unavailable")
            return prism_context(
                "disconnected-fresh-job",
                new_tip,
                worker=state.worker,
                clean_jobs=clean_jobs,
            )

        disconnected_clients: list[ClientState] = []
        server.build_job_for_client = mixed_build  # type: ignore[method-assign]
        server.disconnect_client = disconnected_clients.append  # type: ignore[method-assign]

        with self.assertRaisesRegex(TemplateRefreshBlocked, "no refreshed work was issued"):
            server.poll_qbit_tip_template_once()

        self.assertEqual(server.job_build_failure_count, 1)
        self.assertEqual(disconnected_clients, [disconnected])
        self.assertIn(build_failed, server.clients)
    def test_tip_reconciliation_quarantines_disconnected_block_before_refresh_job(self) -> None:
        old_tip = "00" * 32
        new_tip = "44" * 32
        pool_block_hash = "aa" * 32
        server = coordinator()
        server.reorg_reconciler_enabled = True
        server.jobs = {}
        worker = worker_identity()
        state = client()
        state.username = worker.username
        state.worker = worker
        state.share_difficulty = Decimal("1")
        sent: list[dict[str, object]] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]
        server.clients = {state}
        old_context = prism_context("old-job", old_tip, worker=worker)
        state.active_job = old_context
        state.active_job_ids = {"old-job"}
        server.jobs["old-job"] = old_context
        server.tip_template_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=old_tip,
            previousblockhash=old_tip,
            template_fingerprint=qbit_template_fingerprint(old_context.template),
        )
        ledger = ReorgLedger(
            [
                {
                    "block_hash": pool_block_hash,
                    "block_height": 10,
                    "chain_state": "confirmed",
                    "maturity_state": "immature",
                }
            ]
        )
        server.ledger = ledger
        server.rpc = ReorgRpc(
            tip=new_tip,
            template=gbt_template(new_tip, height=11),
            height=10,
            block_hashes={10: "bb" * 32},
        )

        def build_fresh_job(client: ClientState, *, clean_jobs: bool) -> object:
            self.assertIn(("inactive", pool_block_hash, 10), ledger.events)
            ledger.events.append(("build", client.connection_id))
            return prism_context("fresh-job", new_tip, worker=worker, clean_jobs=clean_jobs)

        server.build_job_for_client = build_fresh_job  # type: ignore[method-assign]

        refreshed = server.poll_qbit_tip_template_once()

        self.assertEqual(refreshed, 1)
        self.assertLess(
            ledger.events.index(("inactive", pool_block_hash, 10)),
            ledger.events.index(("build", state.connection_id)),
        )
        self.assertEqual(server.reorg_inactive_block_count, 1)
        self.assertEqual(ledger.rows[0]["chain_state"], "inactive")
        self.assertEqual(sent[1]["params"][0], "fresh-job")
    def test_reconciliation_quarantines_confirmed_block_above_shortened_tip(self) -> None:
        pool_block_hash = "af" * 32
        server = coordinator()
        server.reorg_reconciler_enabled = True
        ledger = ReorgLedger(
            [
                {
                    "block_hash": pool_block_hash,
                    "block_height": 12,
                    "chain_state": "confirmed",
                    "maturity_state": "immature",
                }
            ]
        )
        server.ledger = ledger
        server.rpc = ReorgRpc(
            tip="77" * 32,
            template=gbt_template("77" * 32, height=11),
            height=10,
            block_hashes={},
        )

        summary = server.reconcile_prism_pool_blocks_once(tip_hash="77" * 32)

        self.assertEqual(summary["inactive_blocks"], 1)
        self.assertEqual(server.reorg_inactive_block_count, 1)
        self.assertEqual(ledger.rows[0]["chain_state"], "inactive")
    def test_tip_reconciliation_skips_jobs_when_qbit_chain_view_is_untrusted(self) -> None:
        old_tip = "00" * 32
        new_tip = "55" * 32
        server = coordinator()
        server.reorg_reconciler_enabled = True
        server.jobs = {}
        worker = worker_identity()
        state = client()
        state.username = worker.username
        state.worker = worker
        state.active_job = prism_context("old-job", old_tip, worker=worker)
        state.active_job_ids = {"old-job"}
        server.jobs["old-job"] = state.active_job
        server.clients = {state}
        server.tip_template_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=old_tip,
            previousblockhash=old_tip,
            template_fingerprint=qbit_template_fingerprint(state.active_job.template),
        )
        ledger = ReorgLedger([])
        server.ledger = ledger
        server.rpc = ReorgRpc(
            tip=new_tip,
            template=gbt_template(new_tip, height=11),
            height=10,
            block_hashes={10: new_tip},
            initialblockdownload=True,
        )

        def unexpected_build(client: ClientState, *, clean_jobs: bool) -> object:
            raise AssertionError("job build should be skipped while qbitd is in IBD")

        server.build_job_for_client = unexpected_build  # type: ignore[method-assign]

        with self.assertRaisesRegex(TemplateRefreshBlocked, "chain view remained untrusted"):
            server.poll_qbit_tip_template_once()

        self.assertEqual(server.reorg_reconcile_skip_count, 1)
        self.assertEqual(ledger.events, [])
        self.assertEqual(state.active_job_ids, {"old-job"})
        self.assertEqual(server.tip_template_snapshot.bestblockhash, old_tip)
    def test_reconciliation_error_before_job_build_is_not_counted_as_build_failure(self) -> None:
        tip = "59" * 32
        server = coordinator()
        server.reorg_reconciler_enabled = True
        state = client()
        state.username = "miner-a"
        state.worker = worker_identity()
        state.share_difficulty = Decimal("1")
        server.rpc = ReorgRpc(
            tip=tip,
            template=gbt_template(tip, height=11),
            height=10,
            block_hashes={10: tip},
        )

        class FailingReorgLedger(FakeLedger):
            def reorg_watch_blocks(self, *, active_tip_height: int) -> list[dict[str, object]]:
                raise RuntimeError("ledger unavailable")

        server.ledger = FailingReorgLedger()
        server.build_job_for_client = lambda _client, *, clean_jobs: (_ for _ in ()).throw(  # type: ignore[method-assign]
            AssertionError("job build should not run after reconcile failure")
        )

        sent_job = server.maybe_send_job(state, clean_jobs=True)

        self.assertFalse(sent_job)
        self.assertEqual(server.reorg_reconcile_error_count, 1)
        self.assertEqual(server.job_build_failure_count, 0)
    def test_reconciliation_runs_again_for_same_tip_hash(self) -> None:
        tip = "5a" * 32
        server = coordinator()
        server.reorg_reconciler_enabled = True
        ledger = ReorgLedger([])
        server.ledger = ledger
        server.rpc = ReorgRpc(
            tip=tip,
            template=gbt_template(tip, height=11),
            height=10,
            block_hashes={10: tip},
        )

        self.assertTrue(server.ensure_reorg_reconciled_for_tip(tip))
        self.assertTrue(server.ensure_reorg_reconciled_for_tip(tip))

        self.assertEqual(ledger.events, [("watch", 10), ("mature", 10), ("watch", 10), ("mature", 10)])
        self.assertEqual(server._payout_state_generation, 1)
        self.assertEqual(server._payout_state_source[0], 1)
    def test_reconciliation_reactivates_inactive_block_that_returns_to_active_chain(self) -> None:
        pool_block_hash = "cc" * 32
        server = coordinator()
        server.reorg_reconciler_enabled = True
        ledger = ReorgLedger(
            [
                {
                    "block_hash": pool_block_hash,
                    "block_height": 12,
                    "chain_state": "inactive",
                    "maturity_state": "immature",
                }
            ]
        )
        server.ledger = ledger
        server.rpc = ReorgRpc(
            tip=pool_block_hash,
            template=gbt_template(pool_block_hash, height=13),
            height=12,
            block_hashes={12: pool_block_hash},
        )

        summary = server.reconcile_prism_pool_blocks_once(tip_hash=pool_block_hash)

        self.assertEqual(summary["reactivated_blocks"], 1)
        self.assertEqual(server.reorg_reactivated_block_count, 1)
        self.assertEqual(ledger.rows[0]["chain_state"], "confirmed")
        self.assertIn(("mature", 12), ledger.events)
    def test_maybe_send_job_reconciles_before_direct_job_build(self) -> None:
        tip = "66" * 32
        pool_block_hash = "dd" * 32
        server = coordinator()
        server.reorg_reconciler_enabled = True
        server.jobs = {}
        worker = worker_identity()
        state = client()
        state.username = worker.username
        state.worker = worker
        state.share_difficulty = Decimal("1")
        sent: list[dict[str, object]] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]
        ledger = ReorgLedger(
            [
                {
                    "block_hash": pool_block_hash,
                    "block_height": 20,
                    "chain_state": "confirmed",
                    "maturity_state": "immature",
                }
            ]
        )
        server.ledger = ledger
        server.rpc = ReorgRpc(
            tip=tip,
            template=gbt_template(tip, height=21),
            height=20,
            block_hashes={20: "ee" * 32},
        )

        def build_direct_job(client: ClientState, *, clean_jobs: bool) -> object:
            self.assertIn(("inactive", pool_block_hash, 20), ledger.events)
            ledger.events.append(("build", client.connection_id))
            return prism_context("direct-job", tip, worker=worker, clean_jobs=clean_jobs)

        server.build_job_for_client = build_direct_job  # type: ignore[method-assign]

        sent_job = server.maybe_send_job(state, clean_jobs=True)

        self.assertTrue(sent_job)
        self.assertLess(
            ledger.events.index(("inactive", pool_block_hash, 20)),
            ledger.events.index(("build", state.connection_id)),
        )
        self.assertEqual(sent[1]["params"][0], "direct-job")
    def test_template_refresh_failure_budget_starts_at_first_failure(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.template_refresh_failure_exit_seconds = 120
        server.last_successful_template_refresh_monotonic = 100.0

        server._record_template_refresh_failure(500.0)
        self.assertFalse(server.template_refresh_failure_expired(500.0))
        self.assertEqual(server.template_refresh_failure_started_monotonic, 500.0)
        self.assertFalse(server.template_refresh_failure_expired(619.999))
        self.assertTrue(server.template_refresh_failure_expired(620.0))
    def test_disabled_template_refresh_failure_budget_does_not_start_or_expire(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.template_refresh_failure_exit_seconds = 0

        server._record_template_refresh_failure(500.0)
        self.assertFalse(server.template_refresh_failure_expired(500.0))
        self.assertFalse(hasattr(server, "template_refresh_failure_started_monotonic"))
    def test_coordination_blocked_refresh_does_not_start_failure_budget(self) -> None:
        for blocked_error in (
            TemplateRefreshSuperseded("qbit tip changed during sequential refresh"),
            _PayoutStatePublicationBlocked("payout state invalidation is pending publication"),
        ):
            with self.subTest(blocked=type(blocked_error).__name__):
                server = coordinator()
                server.template_refresh_failure_exit_seconds = 10.0
                server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
                server.rpc = TipRpc("11" * 32)

                def raise_blocked(error: Exception = blocked_error) -> QbitTipTemplateSnapshot:
                    raise error

                server.fetch_qbit_tip_template_snapshot = raise_blocked  # type: ignore[method-assign]
                with self.assertRaises(type(blocked_error)):
                    server.poll_qbit_tip_template_once()

                self.assertIsNone(
                    getattr(server, "template_refresh_failure_started_monotonic", None)
                )
                self.assertFalse(server.template_refresh_failure_expired(10_000.0))
    def test_non_coordination_blocked_refresh_still_starts_failure_budget(self) -> None:
        # Plain TemplateRefreshBlocked also wraps genuine failures (malformed
        # template artifacts, job builds failing, untrusted chain views); only
        # the TemplateRefreshSuperseded/payout-fence subclasses are exempt.
        server = coordinator()
        server.template_refresh_failure_exit_seconds = 10.0
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
        server.rpc = TipRpc("11" * 32)

        def raise_blocked() -> QbitTipTemplateSnapshot:
            raise TemplateRefreshBlocked(
                "unable to derive exact artifacts for observed qbit template"
            )

        server.fetch_qbit_tip_template_snapshot = raise_blocked  # type: ignore[method-assign]
        with (
            patch("lab.prism.prism_coordinator.time.monotonic", return_value=100.0),
            self.assertRaises(TemplateRefreshBlocked),
        ):
            server.poll_qbit_tip_template_once()

        self.assertEqual(server.template_refresh_failure_started_monotonic, 100.0)
        self.assertTrue(server.template_refresh_failure_expired(110.0))
    def test_sustained_blocked_refresh_storm_never_exhausts_failure_budget(self) -> None:
        server = coordinator()
        server.blockpoll_seconds = 0
        server.template_refresh_failure_exit_seconds = 10.0
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
        server.rpc = TipRpc("11" * 32)
        clock = {"now": 100.0}
        blocked_polls = 0

        def blocked_fetch() -> QbitTipTemplateSnapshot:
            nonlocal blocked_polls
            blocked_polls += 1
            clock["now"] += 6.0
            if blocked_polls >= 4:
                server.stop_event.set()
            if blocked_polls % 2:
                raise TemplateRefreshSuperseded(
                    "qbit tip changed during sequential refresh; immediate retry scheduled"
                )
            raise _PayoutStatePublicationBlocked(
                "payout state invalidation is pending publication"
            )

        server.fetch_qbit_tip_template_snapshot = blocked_fetch  # type: ignore[method-assign]
        with (
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                side_effect=lambda: clock["now"],
            ),
            patch("lab.prism.prism_coordinator.traceback.print_exc"),
            patch("lab.prism.prism_coordinator.os._exit", side_effect=SystemExit(1)) as exit_process,
        ):
            server.blockpoll_loop()

        self.assertEqual(blocked_polls, 4)
        self.assertGreater(clock["now"] - 100.0, server.template_refresh_failure_exit_seconds)
        exit_process.assert_not_called()
        self.assertIsNone(
            getattr(server, "template_refresh_failure_started_monotonic", None)
        )
    def test_armed_budget_is_not_fired_by_coordination_blocked_refresh(self) -> None:
        # A transient budgeted failure armed the clock, qbitd recovered, and
        # only coordination churn follows. Blocked attempts must not trip the
        # armed budget in blockpoll_loop: the exit is reserved for the next
        # budgeted failure, and the clock clears on the next completed refresh.
        server = coordinator()
        server.blockpoll_seconds = 0
        server.template_refresh_failure_exit_seconds = 10.0
        server.template_refresh_failure_started_monotonic = 100.0
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
        server.rpc = TipRpc("11" * 32)
        clock = {"now": 100.0}
        blocked_polls = 0

        def blocked_fetch() -> QbitTipTemplateSnapshot:
            nonlocal blocked_polls
            blocked_polls += 1
            clock["now"] += 6.0
            if blocked_polls >= 4:
                server.stop_event.set()
            raise TemplateRefreshSuperseded(
                "qbit tip changed during sequential refresh; immediate retry scheduled"
            )

        server.fetch_qbit_tip_template_snapshot = blocked_fetch  # type: ignore[method-assign]
        with (
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                side_effect=lambda: clock["now"],
            ),
            patch("lab.prism.prism_coordinator.traceback.print_exc"),
            patch("lab.prism.prism_coordinator.os._exit", side_effect=SystemExit(1)) as exit_process,
        ):
            server.blockpoll_loop()

        self.assertEqual(blocked_polls, 4)
        self.assertGreater(
            clock["now"],
            server.template_refresh_failure_started_monotonic
            + server.template_refresh_failure_exit_seconds,
        )
        exit_process.assert_not_called()
        self.assertEqual(server.template_refresh_failure_started_monotonic, 100.0)
    def test_rpc_outage_arms_and_exhausts_failure_budget(self) -> None:
        server = coordinator()
        server.blockpoll_seconds = 0
        server.template_refresh_failure_exit_seconds = 10.0
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
        clock = {"now": 100.0}

        class OutageRpc:
            def call(self, method: str, params: list[object] | None = None, **_kwargs: object) -> object:
                clock["now"] += 6.0
                raise ConnectionError("qbitd unreachable")

        server.rpc = OutageRpc()
        with (
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                side_effect=lambda: clock["now"],
            ),
            patch("lab.prism.prism_coordinator.traceback.print_exc"),
            patch("lab.prism.prism_coordinator.os._exit", side_effect=SystemExit(1)) as exit_process,
            self.assertRaises(SystemExit),
        ):
            server.blockpoll_loop()

        exit_process.assert_called_once_with(1)
        self.assertIsNotNone(server.template_refresh_failure_started_monotonic)
    def test_persistent_blocked_template_derivation_arms_and_exhausts_failure_budget(self) -> None:
        # Top-of-poll RPCs stay healthy, but every refresh fails with plain
        # TemplateRefreshBlocked (e.g. malformed template artifacts, all job
        # builds failing). A fresh process must still arm the budget from its
        # first such failure and take the budgeted restart path.
        server = coordinator()
        server.blockpoll_seconds = 0
        server.template_refresh_failure_exit_seconds = 10.0
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
        server.rpc = TipRpc("11" * 32)
        clock = {"now": 100.0}

        def blocked_fetch() -> QbitTipTemplateSnapshot:
            clock["now"] += 6.0
            raise TemplateRefreshBlocked(
                "unable to derive exact artifacts for observed qbit template"
            )

        server.fetch_qbit_tip_template_snapshot = blocked_fetch  # type: ignore[method-assign]
        with (
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                side_effect=lambda: clock["now"],
            ),
            patch("lab.prism.prism_coordinator.traceback.print_exc"),
            patch("lab.prism.prism_coordinator.os._exit", side_effect=SystemExit(1)) as exit_process,
            self.assertRaises(SystemExit),
        ):
            server.blockpoll_loop()

        exit_process.assert_called_once_with(1)
        self.assertIsNotNone(server.template_refresh_failure_started_monotonic)
    def test_healthy_noop_template_poll_resets_refresh_failure_clock(self) -> None:
        server = coordinator()
        server.blockpoll_seconds = 0
        server.last_successful_template_refresh_monotonic = 100.0
        server.template_refresh_failure_started_monotonic = 190.0
        server.template_refresh_failure_exit_seconds = 10.0
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
        snapshot = QbitTipTemplateSnapshot(
            bestblockhash="11" * 32,
            previousblockhash="11" * 32,
            template_fingerprint="22" * 32,
        )
        server.tip_template_snapshot = snapshot
        server.rpc = TipRpc(snapshot.bestblockhash)
        server.fetch_qbit_tip_template_snapshot = lambda: snapshot  # type: ignore[method-assign]

        def trusted_chain_view(_tip: str) -> bool:
            server.stop_event.set()
            return True

        server.ensure_reorg_reconciled_for_tip = trusted_chain_view  # type: ignore[method-assign]
        with patch("lab.prism.prism_coordinator.time.monotonic", return_value=200.0):
            server.blockpoll_loop()

        self.assertEqual(server.last_successful_template_refresh_monotonic, 200.0)
        self.assertIsNone(server.template_refresh_failure_started_monotonic)
        self.assertFalse(server.template_refresh_failure_expired(300.0))
        self.assertIsNone(server.template_refresh_failure_started_monotonic)
        server._record_template_refresh_failure(300.0)
        self.assertEqual(server.template_refresh_failure_started_monotonic, 300.0)
    def test_shared_template_poll_records_success_for_blockwait_callers(self) -> None:
        server = coordinator()
        server.last_successful_template_refresh_monotonic = 100.0
        snapshot = QbitTipTemplateSnapshot(
            bestblockhash="11" * 32,
            previousblockhash="11" * 32,
            template_fingerprint="22" * 32,
        )
        server.rpc = TipRpc(snapshot.bestblockhash)
        server.fetch_qbit_tip_template_snapshot = lambda: snapshot  # type: ignore[method-assign]
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]

        with patch("lab.prism.prism_coordinator.time.monotonic", return_value=200.0):
            refreshed = server.poll_qbit_tip_template_once(heartbeat_name="qbit_blockwait")

        self.assertEqual(refreshed, 0)
        self.assertEqual(server.last_successful_template_refresh_monotonic, 200.0)
    def test_untrusted_reconciliation_exhausts_template_refresh_failure_budget(self) -> None:
        server = coordinator()
        server.blockpoll_seconds = 0
        server.last_successful_template_refresh_monotonic = 100.0
        server.template_refresh_failure_started_monotonic = 100.0
        server.template_refresh_failure_exit_seconds = 10.0
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
        snapshot = QbitTipTemplateSnapshot(
            bestblockhash="11" * 32,
            previousblockhash="11" * 32,
            template_fingerprint="22" * 32,
        )
        server.rpc = TipRpc(snapshot.bestblockhash)
        server.fetch_qbit_tip_template_snapshot = lambda: snapshot  # type: ignore[method-assign]
        server.ensure_reorg_reconciled_for_tip = lambda _tip: False  # type: ignore[method-assign]

        with (
            patch("lab.prism.prism_coordinator.time.monotonic", return_value=110.0),
            patch("lab.prism.prism_coordinator.traceback.print_exc"),
            patch("lab.prism.prism_coordinator.os._exit", side_effect=SystemExit(1)) as exit_process,
            self.assertRaises(SystemExit),
        ):
            server.blockpoll_loop()

        exit_process.assert_called_once_with(1)
        self.assertEqual(server.last_successful_template_refresh_monotonic, 100.0)
    def test_all_refresh_job_builds_failing_exhausts_failure_budget(self) -> None:
        old_tip = "00" * 32
        new_tip = "33" * 32
        server = coordinator()
        server.blockpoll_seconds = 0
        server.last_successful_template_refresh_monotonic = 100.0
        server.template_refresh_failure_started_monotonic = 100.0
        server.template_refresh_failure_exit_seconds = 10.0
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
        state = client()
        state.username = "miner-a"
        state.worker = worker_identity()
        state.active_job = prism_context("old-job", old_tip, worker=state.worker)
        state.active_job_ids = {"old-job"}
        server.clients = {state}
        server.jobs = {"old-job": state.active_job}
        server.tip_template_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=old_tip,
            previousblockhash=old_tip,
            template_fingerprint=qbit_template_fingerprint(state.active_job.template),
        )
        snapshot = QbitTipTemplateSnapshot(
            bestblockhash=new_tip,
            previousblockhash=new_tip,
            template_fingerprint="44" * 32,
        )
        server.rpc = TipRpc(new_tip)
        server.fetch_qbit_tip_template_snapshot = lambda: snapshot  # type: ignore[method-assign]
        server.build_job_for_client = lambda *_args, **_kwargs: (_ for _ in ()).throw(  # type: ignore[method-assign]
            RuntimeError("template build unavailable")
        )

        with (
            patch("lab.prism.prism_coordinator.time.monotonic", return_value=110.0),
            patch("lab.prism.prism_coordinator.traceback.print_exc"),
            patch("lab.prism.prism_coordinator.os._exit", side_effect=SystemExit(1)) as exit_process,
            self.assertRaises(SystemExit),
        ):
            server.blockpoll_loop()

        exit_process.assert_called_once_with(1)
        self.assertEqual(server.job_build_failure_count, 1)
        self.assertEqual(server.last_successful_template_refresh_monotonic, 100.0)
    def test_guarded_sequential_build_supersession_does_not_arm_failure_budget(self) -> None:
        # Non-ready/collection mode: the sequential loop's guarded client
        # build detects a superseded snapshot inside maybe_send_job. That is
        # coordination churn, not template unhealthiness -- the real raise
        # site must carry TemplateRefreshSuperseded so sustained pre-ready
        # churn neither arms nor fires the budget.
        old_tip = "00" * 32
        new_tip = "33" * 32
        server = coordinator()
        server.blockpoll_seconds = 0
        server.template_refresh_failure_exit_seconds = 10.0
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
        state = client()
        state.username = "miner-a"
        state.worker = worker_identity()
        state.active_job = prism_context("old-job", old_tip, worker=state.worker)
        state.active_job_ids = {"old-job"}
        server.clients = {state}
        server.jobs = {"old-job": state.active_job}
        server.tip_template_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=old_tip,
            previousblockhash=old_tip,
            template_fingerprint=qbit_template_fingerprint(state.active_job.template),
        )
        snapshot = QbitTipTemplateSnapshot(
            bestblockhash=new_tip,
            previousblockhash=new_tip,
            template_fingerprint="44" * 32,
        )
        server.rpc = TipRpc(new_tip)
        clock = {"now": 100.0}
        fetches = 0

        def fetch_snapshot() -> QbitTipTemplateSnapshot:
            nonlocal fetches
            fetches += 1
            clock["now"] += 6.0
            if fetches >= 4:
                server.stop_event.set()
            return snapshot

        server.fetch_qbit_tip_template_snapshot = fetch_snapshot  # type: ignore[method-assign]
        # The guarded pre-build currency check loses the race on every pass.
        server._tip_refresh_snapshot_current_locked = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: False
        )

        with (
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                side_effect=lambda: clock["now"],
            ),
            patch("lab.prism.prism_coordinator.traceback.print_exc"),
            patch("lab.prism.prism_coordinator.os._exit", side_effect=SystemExit(1)) as exit_process,
        ):
            server.blockpoll_loop()

        self.assertEqual(fetches, 4)
        self.assertGreater(clock["now"] - 100.0, server.template_refresh_failure_exit_seconds)
        exit_process.assert_not_called()
        self.assertIsNone(
            getattr(server, "template_refresh_failure_started_monotonic", None)
        )
    def test_transient_template_refresh_failure_recovers_on_healthy_noop(self) -> None:
        server = coordinator()
        server.blockpoll_seconds = 0
        server.last_successful_template_refresh_monotonic = 100.0
        server.template_refresh_failure_exit_seconds = 10.0
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
        poll_count = 0

        def fail_then_noop() -> int:
            nonlocal poll_count
            poll_count += 1
            if poll_count == 1:
                raise RuntimeError("transient RPC failure")
            server.last_successful_template_refresh_monotonic = time.monotonic()
            server.stop_event.set()
            return 0

        server.poll_qbit_tip_template_once = fail_then_noop  # type: ignore[method-assign]
        with (
            patch("lab.prism.prism_coordinator.time.monotonic", side_effect=[105.0, 106.0]),
            patch("lab.prism.prism_coordinator.traceback.print_exc"),
        ):
            server.blockpoll_loop()

        self.assertEqual(poll_count, 2)
        self.assertEqual(server.last_successful_template_refresh_monotonic, 106.0)
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

class PrismCoordinatorReliabilityTests(_VardiffSupportTestCase):
    def _bare_coordinator(self) -> PrismCoordinator:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.stop_event = threading.Event()
        server._heartbeats = {}
        server._watchdog_pauses = {}
        server._heartbeats_lock = threading.Lock()
        server.watchdog_timeout_seconds = 120.0
        server.watchdog_interval_seconds = 15.0
        return server
    def test_blockwait_parameter_mismatch_is_treated_as_unsupported(self) -> None:
        self.assertTrue(
            PrismCoordinator._blockwait_unsupported(
                RuntimeError("RPC error -32602: invalid params: wrong number of parameters")
            )
        )
    def test_blockwait_advances_known_tip_before_notification_failure(self) -> None:
        server = self._bare_coordinator()
        tip_a = "aa" * 32
        tip_b = "bb" * 32
        server.rpc = SimpleNamespace(call=lambda method: tip_a)
        server.blockpoll_seconds = 1.0
        known_tips: list[str] = []
        detected_tips: list[str] = []

        def blockwait_once(known_tip: str) -> str:
            known_tips.append(known_tip)
            if len(known_tips) == 2:
                server.stop_event.set()
            return tip_b

        server.blockwait_once = blockwait_once  # type: ignore[method-assign]

        def observe_tip_for_refresh(tip_hash: str, **_kwargs: object) -> bool:
            detected_tips.append(tip_hash)
            if tip_hash == tip_b:
                raise TemplateRefreshBlocked("notification failed")
            return True

        def reject_premature_publication(*_args: object, **_kwargs: object) -> bool:
            raise AssertionError("blockwait must not publish submit authority")

        server.observe_tip_for_refresh = observe_tip_for_refresh  # type: ignore[method-assign]
        server.observe_tip_first_seen = reject_premature_publication  # type: ignore[method-assign]

        with patch("builtins.print"), patch(
            "lab.prism.prism_coordinator.traceback.print_exc"
        ), patch.object(
            server.stop_event,
            "wait",
            side_effect=lambda _timeout: server.stop_event.is_set(),
        ):
            server.blockwait_loop()

        self.assertEqual(known_tips, [tip_a, tip_b])
        self.assertEqual(detected_tips, [tip_a, tip_b])
    def test_blockwait_unsupported_removes_watchdog_heartbeat(self) -> None:
        server = coordinator()
        server.rpc = UnsupportedBlockwaitRpc("00" * 32)
        server._record_heartbeat("qbit_blockwait")

        server.blockwait_loop()

        self.assertNotIn("qbit_blockwait", server._heartbeats)
        self.assertNotIn("qbit_blockwait", server._watchdog_pauses)

class PrismStampedJobFloorTests(_VardiffSupportTestCase):
    """The listener floor must hold on the wire, not just in vardiff policy.

    Stamped jobs are the single choke point for every mining.set_difficulty
    the coordinator sends, and marketplace verification judges the first one.
    The regression here is a young chain: qbit network difficulty below the
    high-diff floor used to drag the advertised difficulty down with it.
    """
    def stamp_coordinator(self) -> PrismCoordinator:
        server = coordinator()
        server.job_counter = 0
        server.share_weights_by_username = {}
        server.default_share_weight = 1
        return server
    def cached_bundle(self) -> CachedJobBundle:
        # bits 207fffff: regtest-grade network difficulty (~4.7e-10), far
        # below the 500k marketplace floor.
        qbit_target = target_from_compact("207fffff")
        base_job = direct_stratum.DirectQbitStratumJob(
            job_id="prism-template-base",
            previousblockhash_display="00" * 32,
            prevhash="00" * 32,
            coinb1="",
            coinb2="",
            full_coinbase_prefix="",
            full_coinbase_suffix="",
            merkle_branch=(),
            transaction_hexes=(),
            version="20000000",
            nbits="207fffff",
            ntime="6553f100",
            qbit_target=qbit_target,
            share_target=qbit_target,
            share_difficulty=Decimal("1"),
            extranonce1_hex="ffffffff",
            extranonce2_size=8,
            clean_jobs=True,
        )
        return CachedJobBundle(
            key=("test",),
            template=gbt_template("00" * 32),
            template_fingerprint="fp",
            coinbase_manifest={},
            shares_json=[],
            prior_balances=[],
            found_block={"network_difficulty": 1},
            collection_only=False,
            issued_at_ms=12345,
            base_job=base_job,
            built_monotonic=time.monotonic(),
        )
    def highdiff_client(self) -> ClientState:
        state = client()
        state.worker = worker_identity()
        state.listener_vardiff_config = highdiff_vardiff_config()
        state.minimum_advertised_difficulty = Decimal("500000")
        state.share_difficulty = Decimal("500000")
        return state
    def test_ready_pool_refreshes_clients_left_on_collection_jobs(self) -> None:
        # Once the pool crosses min_ready_miners, the poller must replace
        # collection jobs with windowed work even when the template snapshot
        # is otherwise unchanged -- readiness itself is invisible to the
        # template fingerprint.
        server, state, _ledger = submit_coordinator()
        tip = "00" * 32
        snapshot = SimpleNamespace(
            bestblockhash=tip,
            previousblockhash=tip,
            template_fingerprint="fp",
        )
        context = SimpleNamespace(
            template={"previousblockhash": tip},
            template_fingerprint="fp",
            collection_only=True,
        )
        state.active_job = context

        server.min_ready_miners = 1
        server.accepted_share_stats = lambda: (0, 0)  # type: ignore[method-assign]
        self.assertFalse(server.pool_readiness_latched())
        self.assertFalse(server.client_needs_tip_template_refresh(state, snapshot))

        server.accepted_share_stats = lambda: (1, 1)  # type: ignore[method-assign]
        self.assertTrue(server.pool_readiness_latched())
        self.assertTrue(server.client_needs_tip_template_refresh(state, snapshot))

        # Readiness is monotonic: once latched the ledger is never consulted
        # again, and ready (non-collection) jobs still need no refresh.
        server.accepted_share_stats = None  # type: ignore[assignment]
        self.assertTrue(server.pool_readiness_latched())
        context.collection_only = False
        self.assertFalse(server.client_needs_tip_template_refresh(state, snapshot))

class JobBundleCacheTests(_JobSupportTestCase):
    @staticmethod
    def _capture_error(
        errors: list[BaseException],
        operation: object,
    ) -> None:
        try:
            operation()  # type: ignore[operator]
        except BaseException as exc:  # noqa: BLE001 - test thread handoff
            errors.append(exc)
    def test_ready_tip_refresh_builds_once_and_stamps_every_client(self) -> None:
        server, _ = coordinator()
        recorded = install_fake_bundle_builder(server)
        clients = [client(1), client(2), client(3)]
        clients[1].pending_share_difficulty = Decimal("8")
        sent: dict[int, list[dict[str, object]]] = {state.connection_id: [] for state in clients}
        for state in clients:
            state.send = (  # type: ignore[method-assign]
                lambda payload, connection_id=state.connection_id: sent[connection_id].append(payload)
            )
        server.clients = set(clients)

        try:
            refreshed = server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(refreshed, 3)
        self.assertEqual(recorded["calls"], 1)
        contexts = [state.active_job for state in clients]
        self.assertEqual(len({context.job.job_id for context in contexts}), 3)
        self.assertEqual(
            [context.job.extranonce1_hex for context in contexts],
            [state.extranonce1_hex for state in clients],
        )
        self.assertEqual(contexts[1].job.share_difficulty, Decimal("8"))
        self.assertEqual(
            [payload["method"] for payload in sent[2]],
            ["mining.set_difficulty", "mining.notify"],
        )
        metrics = server.metrics_payload()
        self.assertIn('qbit_prism_tip_refresh_clients_total{result="sent"} 3', metrics)
        self.assertIn("qbit_prism_tip_refresh_first_delivery_seconds_count 1", metrics)
        self.assertIn("qbit_prism_tip_refresh_last_delivery_seconds_count 1", metrics)
    def test_ready_tip_refresh_shares_one_bundle_across_250_clients(self) -> None:
        server, rpc = coordinator()
        recorded = install_fake_bundle_builder(server)
        server.reorg_reconciler_enabled = True
        reconciled: list[str] = []
        trust_checks = 0

        def reconcile_once(tip_hash: str) -> bool:
            reconciled.append(tip_hash)
            return True

        def chain_view_untrusted() -> bool:
            nonlocal trust_checks
            trust_checks += 1
            return False

        server.ensure_reorg_reconciled_for_tip = reconcile_once  # type: ignore[method-assign]
        server.qbit_chain_view_untrusted = chain_view_untrusted  # type: ignore[method-assign]
        server.ensure_reorg_reconciled_for_current_tip = (  # type: ignore[method-assign]
            lambda **_kwargs: self.fail("fanout repeated current-tip validation")
        )
        clients = [client(index + 1) for index in range(250)]
        sent: dict[int, list[dict[str, object]]] = {
            state.connection_id: [] for state in clients
        }
        for state in clients:
            state.send = (  # type: ignore[method-assign]
                lambda payload, connection_id=state.connection_id: sent[
                    connection_id
                ].append(payload)
            )
        server.clients = set(clients)

        try:
            refreshed = server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(refreshed, 250)
        self.assertEqual(recorded["calls"], 1)
        cached = next(iter(server._job_bundle_cache.values()))
        self.assertEqual(
            len({id(state.active_job.shares_json) for state in clients}),
            1,
        )
        self.assertTrue(
            all(state.active_job.shares_json is cached.shares_json for state in clients)
        )
        self.assertEqual(reconciled, [rpc.tip])
        self.assertEqual(trust_checks, 2)
        # The early priority probe, snapshot coherence, pre-fanout validation,
        # and post-fanout detection are each constant-cost regardless of
        # client count.
        self.assertEqual(rpc.count("getbestblockhash"), 4)
        self.assertTrue(all(len(payloads) == 2 for payloads in sent.values()))
        fingerprints = {state.active_job.template_fingerprint for state in clients}
        self.assertEqual(len(fingerprints), 1)
    def test_supersession_retry_wakes_blockpoll_without_full_interval(self) -> None:
        server, _ = coordinator()
        server.blockpoll_seconds = 60.0
        server._ensure_tip_refresh_state()
        poll_called = threading.Event()

        def poll_once() -> int:
            poll_called.set()
            server.stop_event.set()
            return 0

        server.poll_qbit_tip_template_once = poll_once  # type: ignore[method-assign]
        thread = threading.Thread(target=server.blockpoll_loop)
        thread.start()
        try:
            server._schedule_tip_refresh_retry()
            self.assertTrue(poll_called.wait(1))
        finally:
            server.stop_event.set()
            server._schedule_tip_refresh_retry()
            thread.join(1)

        self.assertFalse(thread.is_alive())
    def test_ready_tip_refresh_respects_executor_bound(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_max_workers = 2
        clients = [client(index + 1) for index in range(6)]
        server.clients = set(clients)
        release = threading.Event()
        two_started = threading.Event()
        counter_lock = threading.Lock()
        active = 0
        maximum = 0

        def send(payload: dict[str, object]) -> None:
            nonlocal active, maximum
            if payload["method"] != "mining.notify":
                return
            with counter_lock:
                active += 1
                maximum = max(maximum, active)
                if active == 2:
                    two_started.set()
            try:
                self.assertTrue(release.wait(5))
            finally:
                with counter_lock:
                    active -= 1

        for state in clients:
            state.send = send  # type: ignore[method-assign]
        result: list[int] = []
        thread = threading.Thread(target=lambda: result.append(server.poll_qbit_tip_template_once()))
        thread.start()
        try:
            self.assertTrue(two_started.wait(5))
            self.assertLessEqual(maximum, 2)
        finally:
            release.set()
            thread.join(5)
            server.shutdown_tip_refresh_executor()

        self.assertFalse(thread.is_alive())
        self.assertEqual(result, [6])
        self.assertEqual(maximum, 2)
    def test_blocked_socket_does_not_delay_another_client(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_max_workers = 2
        blocked = client(1)
        healthy = client(2)
        server.clients = {blocked, healthy}
        blocked_started = threading.Event()
        healthy_delivered = threading.Event()
        release = threading.Event()

        def blocked_send(payload: dict[str, object]) -> None:
            if payload["method"] == "mining.notify":
                blocked_started.set()
                self.assertTrue(release.wait(5))

        def healthy_send(payload: dict[str, object]) -> None:
            if payload["method"] == "mining.notify":
                healthy_delivered.set()

        blocked.send = blocked_send  # type: ignore[method-assign]
        healthy.send = healthy_send  # type: ignore[method-assign]
        result: list[int] = []
        thread = threading.Thread(target=lambda: result.append(server.poll_qbit_tip_template_once()))
        thread.start()
        try:
            self.assertTrue(blocked_started.wait(5))
            self.assertTrue(healthy_delivered.wait(5))
        finally:
            release.set()
            thread.join(5)
            server.shutdown_tip_refresh_executor()

        self.assertEqual(result, [2])
    def test_shutdown_drains_inflight_tip_refresh_worker(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_max_workers = 1
        state = client(1)
        server.clients = {state}
        worker_started = threading.Event()
        worker_send_finished = threading.Event()
        release_worker = threading.Event()
        shutdown_complete = threading.Event()
        poll_errors: list[BaseException] = []

        def blocked_send(payload: dict[str, object]) -> None:
            if payload["method"] != "mining.notify":
                return
            worker_started.set()
            try:
                self.assertTrue(release_worker.wait(5))
            finally:
                worker_send_finished.set()

        def poll() -> None:
            try:
                server.poll_qbit_tip_template_once()
            except BaseException as exc:  # noqa: BLE001 - surface to the test
                poll_errors.append(exc)

        def shutdown() -> None:
            server.shutdown_tip_refresh_executor()
            shutdown_complete.set()

        state.send = blocked_send  # type: ignore[method-assign]
        poll_thread = threading.Thread(target=poll)
        shutdown_thread = threading.Thread(target=shutdown)
        poll_thread.start()
        try:
            self.assertTrue(worker_started.wait(5))
            server.stop_event.set()
            shutdown_thread.start()
            self.assertFalse(shutdown_complete.wait(0.05))
            self.assertFalse(worker_send_finished.is_set())
        finally:
            release_worker.set()
            shutdown_thread.join(5)
            poll_thread.join(5)

        self.assertFalse(shutdown_thread.is_alive())
        self.assertFalse(poll_thread.is_alive())
        self.assertTrue(shutdown_complete.is_set())
        self.assertTrue(worker_send_finished.is_set())
        self.assertEqual(poll_errors, [])
        self.assertEqual(server.tip_refresh_inflight, 0)
        with self.assertRaisesRegex(RuntimeError, "executor is shut down"):
            server.tip_refresh_executor()
    def test_queued_fanout_stops_when_chain_view_becomes_untrusted(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        server.reorg_reconciler_enabled = True
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
        trust_checks = 0

        def chain_view_untrusted() -> bool:
            nonlocal trust_checks
            trust_checks += 1
            return True

        server.qbit_chain_view_untrusted = chain_view_untrusted  # type: ignore[method-assign]
        first = client(1)
        second = client(2)
        server.clients = [first, second]  # type: ignore[assignment]
        sent: list[dict[str, object]] = []
        first.send = sent.append  # type: ignore[method-assign]
        second.send = sent.append  # type: ignore[method-assign]
        try:
            with self.assertRaisesRegex(TemplateRefreshBlocked, "became untrusted"):
                server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(trust_checks, 1)
        self.assertEqual(sent, [])
        self.assertIsNone(first.active_job)
        self.assertIsNone(second.active_job)
    def test_queued_fanout_stops_when_live_tip_changes(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_max_workers = 1
        first = client(1)
        second = client(2)
        server.clients = [first, second]  # type: ignore[assignment]
        first_blocked = threading.Event()
        release_first = threading.Event()
        second_sent: list[dict[str, object]] = []

        def first_send(payload: dict[str, object]) -> None:
            if payload["method"] == "mining.notify":
                first_blocked.set()
                self.assertTrue(release_first.wait(5))

        first.send = first_send  # type: ignore[method-assign]
        second.send = second_sent.append  # type: ignore[method-assign]
        refreshed: list[int] = []
        errors: list[BaseException] = []

        def poll() -> None:
            try:
                refreshed.append(server.poll_qbit_tip_template_once())
            except BaseException as exc:  # noqa: BLE001 - surface to the test
                errors.append(exc)

        thread = threading.Thread(target=poll)
        thread.start()
        try:
            self.assertTrue(first_blocked.wait(5))
            rpc.tip = "33" * 32
        finally:
            release_first.set()
            thread.join(5)
            server.shutdown_tip_refresh_executor()

        self.assertFalse(thread.is_alive())
        self.assertEqual(refreshed, [])
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], TemplateRefreshBlocked)
        self.assertIn("immediate retry scheduled", str(errors[0]))
        self.assertTrue(server._tip_refresh_retry.is_set())
        self.assertIsNotNone(first.active_job)
        self.assertIsNotNone(second.active_job)
        self.assertEqual(
            [payload["method"] for payload in second_sent],
            ["mining.set_difficulty", "mining.notify"],
        )
    def test_multiworker_cancel_releases_client_lock_while_draining_peer(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_max_workers = 1
        admitted = client(1)
        queued = client(2)
        server.clients = [admitted, queued]  # type: ignore[assignment]
        admitted_send_started = threading.Event()
        release_admitted_send = threading.Event()
        queued_sent: list[dict[str, object]] = []

        def admitted_send(payload: dict[str, object]) -> None:
            if payload["method"] == "mining.notify":
                admitted_send_started.set()
                self.assertTrue(release_admitted_send.wait(5))

        admitted.send = admitted_send  # type: ignore[method-assign]
        queued.send = queued_sent.append  # type: ignore[method-assign]
        refreshed: list[int] = []
        errors: list[BaseException] = []

        def poll() -> None:
            try:
                refreshed.append(server.poll_qbit_tip_template_once())
            except BaseException as exc:  # noqa: BLE001 - surface to the test
                errors.append(exc)

        thread = threading.Thread(target=poll)
        thread.start()
        try:
            self.assertTrue(admitted_send_started.wait(5))
            server.observe_tip_for_refresh("33" * 32)
            lock_acquired = queued.job_update_lock.acquire(timeout=0.1)
            self.assertTrue(lock_acquired)
            if lock_acquired:
                queued.job_update_lock.release()
            # The coordinator still waits for the admitted peer delivery, but
            # queued workers observe cancellation without taking client state.
            self.assertTrue(thread.is_alive())
            self.assertIsNone(queued.active_job)
        finally:
            release_admitted_send.set()
            thread.join(5)
            server.shutdown_tip_refresh_executor()

        self.assertFalse(thread.is_alive())
        self.assertEqual(refreshed, [])
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], TemplateRefreshBlocked)
        self.assertIsNotNone(admitted.active_job)
        self.assertIsNone(queued.active_job)
        self.assertEqual(queued_sent, [])
    def test_same_tip_cache_refresh_during_fanout_does_not_abort(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_max_workers = 1
        first = client(1)
        second = client(2)
        # Preserve task order so the cache replacement happens after one
        # delivery and before the next worker task starts.
        server.clients = [first, second]  # type: ignore[assignment]
        first_blocked = threading.Event()
        release_first = threading.Event()
        second_sent: list[dict[str, object]] = []

        def first_send(payload: dict[str, object]) -> None:
            if payload["method"] == "mining.notify":
                first_blocked.set()
                self.assertTrue(release_first.wait(5))

        first.send = first_send  # type: ignore[method-assign]
        second.send = second_sent.append  # type: ignore[method-assign]
        refreshed: list[int] = []
        errors: list[BaseException] = []

        def poll() -> None:
            try:
                refreshed.append(server.poll_qbit_tip_template_once())
            except BaseException as exc:  # noqa: BLE001 - surface to the test
                errors.append(exc)

        thread = threading.Thread(target=poll)
        thread.start()
        try:
            self.assertTrue(first_blocked.wait(5))
            replacement = dict(rpc.template)
            replacement["coinbasevalue"] = int(replacement["coinbasevalue"]) + 1
            replacement_artifacts = server.store_template_artifacts(replacement)
            self.assertIsNotNone(replacement_artifacts)
            self.assertNotEqual(
                replacement_artifacts.fingerprint,
                first.active_job.template_fingerprint,
            )
        finally:
            release_first.set()
            thread.join(5)
            server.shutdown_tip_refresh_executor()

        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(refreshed, [2])
        self.assertIsNotNone(server.last_successful_template_refresh_monotonic)
        self.assertEqual(
            [payload["method"] for payload in second_sent],
            ["mining.set_difficulty", "mining.notify"],
        )
    def test_queued_fanout_does_not_overwrite_intervening_job(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_max_workers = 1
        first = client(1)
        second = client(2)
        server.clients = [first, second]  # type: ignore[assignment]
        first_blocked = threading.Event()
        release_first = threading.Event()
        second_sent: list[dict[str, object]] = []

        def first_send(payload: dict[str, object]) -> None:
            if payload["method"] == "mining.notify":
                first_blocked.set()
                self.assertTrue(release_first.wait(5))

        first.send = first_send  # type: ignore[method-assign]
        second.send = second_sent.append  # type: ignore[method-assign]
        refreshed: list[int] = []
        errors: list[BaseException] = []

        def poll() -> None:
            try:
                refreshed.append(server.poll_qbit_tip_template_once())
            except BaseException as exc:  # noqa: BLE001 - surface to the test
                errors.append(exc)

        thread = threading.Thread(target=poll)
        thread.start()
        try:
            self.assertTrue(first_blocked.wait(5))
            replacement = dict(rpc.template)
            replacement["coinbasevalue"] = int(replacement["coinbasevalue"]) + 1
            replacement_artifacts = server.store_template_artifacts(replacement)
            self.assertIsNotNone(replacement_artifacts)
            self.assertTrue(server.maybe_send_job(second, clean_jobs=False))
            intervening_job = second.active_job
            self.assertEqual(
                intervening_job.template_fingerprint,
                replacement_artifacts.fingerprint,
            )
            self.assertGreater(
                intervening_job.template_generation,
                first.active_job.template_generation,
            )
        finally:
            release_first.set()
            thread.join(5)
            server.shutdown_tip_refresh_executor()

        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(refreshed, [1])
        self.assertIs(second.active_job, intervening_job)
        self.assertEqual(
            [payload["method"] for payload in second_sent],
            ["mining.set_difficulty", "mining.notify"],
        )
    def test_queued_fanout_replaces_stale_intervening_job(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_max_workers = 1
        # The poll must observe the same-tip template rotation immediately for
        # the queued-fanout replacement race below; disable the same-tip
        # template reuse window so the rotation is fetched, not deferred.
        server.template_cache_seconds = 0.0
        first = client(1)
        second = client(2)
        server.clients = [first, second]  # type: ignore[assignment]
        old_artifacts = server.store_template_artifacts(dict(rpc.template))
        self.assertIsNotNone(old_artifacts)
        assert old_artifacts is not None
        assert second.worker is not None
        old_bundle = server.shared_job_bundle(old_artifacts, second.worker)
        refreshed_template = dict(rpc.template)
        refreshed_template["coinbasevalue"] = int(
            refreshed_template["coinbasevalue"]
        ) + 1
        rpc.template = refreshed_template
        first_blocked = threading.Event()
        release_first = threading.Event()
        second_sent: list[dict[str, object]] = []

        def first_send(payload: dict[str, object]) -> None:
            if payload["method"] == "mining.notify":
                first_blocked.set()
                self.assertTrue(release_first.wait(5))

        first.send = first_send  # type: ignore[method-assign]
        second.send = second_sent.append  # type: ignore[method-assign]
        refreshed: list[int] = []
        errors: list[BaseException] = []

        def poll() -> None:
            try:
                refreshed.append(server.poll_qbit_tip_template_once())
            except BaseException as exc:  # noqa: BLE001 - surface to the test
                errors.append(exc)

        thread = threading.Thread(target=poll)
        thread.start()
        try:
            self.assertTrue(first_blocked.wait(5))
            with second.job_update_lock, server.lock:
                stale_intervening_job = server.stamp_job_for_client(
                    second,
                    old_bundle,
                    clean_jobs=False,
                )
                second.active_job = stale_intervening_job
                second.active_job_ids.add(stale_intervening_job.job.job_id)
                server.jobs[stale_intervening_job.job.job_id] = stale_intervening_job
        finally:
            release_first.set()
            thread.join(5)
            server.shutdown_tip_refresh_executor()

        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(refreshed, [2])
        self.assertIsNot(second.active_job, stale_intervening_job)
        self.assertEqual(
            second.active_job.template_fingerprint,
            qbit_template_fingerprint(refreshed_template),
        )
        self.assertGreater(
            second.active_job.template_generation,
            stale_intervening_job.template_generation,
        )
        self.assertEqual(
            [payload["method"] for payload in second_sent],
            ["mining.set_difficulty", "mining.notify"],
        )
    def test_newer_template_does_not_supersede_current_payout_refresh(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        self.assertIsNotNone(artifacts)
        assert artifacts is not None and state.worker is not None
        stale_bundle = server.shared_job_bundle(artifacts, state.worker)
        snapshot = server.fetch_qbit_tip_template_snapshot()
        stale_intervening_job = dataclass_replace(
            server.stamp_job_for_client(
                state,
                stale_bundle,
                clean_jobs=False,
            ),
            template_generation=snapshot.template_generation + 1,
        )
        server._advance_payout_state_generation()

        self.assertFalse(
            server.intervening_job_supersedes_snapshot(
                stale_intervening_job,
                None,
                snapshot,
            )
        )
        current_intervening_job = dataclass_replace(
            stale_intervening_job,
            payout_state_generation=server._payout_state_generation,
        )
        self.assertTrue(
            server.intervening_job_supersedes_snapshot(
                current_intervening_job,
                None,
                snapshot,
            )
        )
    def test_higher_generation_old_tip_does_not_supersede_new_tip_snapshot(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        old_artifacts = server.store_template_artifacts(dict(rpc.template))
        self.assertIsNotNone(old_artifacts)
        assert old_artifacts is not None and state.worker is not None
        old_bundle = server.shared_job_bundle(old_artifacts, state.worker)

        new_tip = "22" * 32
        rpc.tip = new_tip
        rpc.template = base_template(height=11, prevhash=new_tip)
        snapshot = server.fetch_qbit_tip_template_snapshot()
        old_tip_job = dataclass_replace(
            server.stamp_job_for_client(
                state,
                old_bundle,
                clean_jobs=False,
            ),
            template_generation=snapshot.template_generation + 1,
        )

        self.assertNotEqual(
            old_tip_job.template_fingerprint,
            snapshot.template_fingerprint,
        )
        self.assertFalse(
            server.intervening_job_supersedes_snapshot(
                old_tip_job,
                None,
                snapshot,
            )
        )
    def test_broken_socket_disconnects_only_that_client(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        broken = client(1)
        healthy = client(2)
        server.clients = {broken, healthy}
        healthy_sent: list[dict[str, object]] = []
        disconnected: list[ClientState] = []
        broken.send = lambda _payload: (_ for _ in ()).throw(OSError("closed"))  # type: ignore[method-assign]
        healthy.send = healthy_sent.append  # type: ignore[method-assign]
        server.disconnect_client = disconnected.append  # type: ignore[method-assign]

        try:
            refreshed = server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(refreshed, 1)
        self.assertEqual(disconnected, [broken])
        self.assertEqual(
            [payload["method"] for payload in healthy_sent],
            ["mining.set_difficulty", "mining.notify"],
        )
    def test_client_removed_before_pending_task_runs_is_skipped(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_max_workers = 1
        first = client(1)
        removed = client(2)
        clients = [first, removed]
        server.clients = set(clients)
        snapshot = server.fetch_qbit_tip_template_snapshot()
        server.observe_tip_first_seen(snapshot.bestblockhash)
        server.pool_readiness_latched()
        server.tip_template_snapshot = snapshot
        bundle = server.prepare_tip_refresh_bundle(snapshot)
        blocked = threading.Event()
        release = threading.Event()

        def first_send(payload: dict[str, object]) -> None:
            if payload["method"] == "mining.notify":
                blocked.set()
                self.assertTrue(release.wait(5))

        first.send = first_send  # type: ignore[method-assign]
        removed.send = lambda _payload: self.fail("removed client received a job")  # type: ignore[method-assign]
        result: list[tuple[int, float | None, float | None, int]] = []
        thread = threading.Thread(
            target=lambda: result.append(
                server._fanout_prepared_tip_refresh(
                    clients,
                    bundle,
                    snapshot,
                    heartbeat_name="qbit_blockpoll",
                )
            )
        )
        thread.start()
        try:
            self.assertTrue(blocked.wait(5))
            with server.lock:
                server.clients.remove(removed)
        finally:
            release.set()
            thread.join(5)
            server.shutdown_tip_refresh_executor()

        self.assertEqual(result[0][0], 1)
        self.assertIsNone(removed.active_job)
        self.assertEqual(removed.active_job_ids, set())
    def test_template_fingerprint_race_uses_snapshot_owned_artifacts(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        states = [client(1), client(2)]
        sent: list[dict[str, object]] = []
        for state in states:
            state.send = sent.append  # type: ignore[method-assign]
        server.clients = set(states)
        original_shared_job_bundle = server.shared_job_bundle
        race_calls = 0

        def race_artifacts(
            artifacts: object,
            identity: WorkerIdentity | None = None,
            **kwargs: object,
        ) -> object:
            nonlocal race_calls
            race_calls += 1
            bundle = original_shared_job_bundle(artifacts, identity, **kwargs)
            with server._job_cache_lock:
                server._template_artifacts = dataclass_replace(
                    server._template_artifacts,
                    fingerprint="ff" * 32,
                )
            return bundle

        server.shared_job_bundle = race_artifacts  # type: ignore[method-assign]

        refreshed = server.poll_qbit_tip_template_once()

        self.assertEqual(race_calls, 1)
        self.assertEqual(refreshed, 2)
        self.assertEqual(len(sent), 4)
        self.assertIsNotNone(server.tip_template_snapshot)
        snapshot = server.tip_template_snapshot
        assert snapshot is not None and snapshot.template_artifacts is not None
        for state in states:
            self.assertIs(state.active_job.template, snapshot.template_artifacts.template)
            self.assertEqual(
                state.active_job.template_fingerprint,
                snapshot.template_fingerprint,
            )

class ClientCleanupTests(_JobSupportTestCase):
    def test_disconnect_retires_and_closes_before_job_lock_cleanup(self) -> None:
        server, _ = coordinator()
        state = client(1)
        server.clients = {state}
        socket_closed = threading.Event()
        state.close = socket_closed.set  # type: ignore[method-assign]
        state.job_update_lock.acquire()
        disconnect = threading.Thread(target=server.disconnect_client, args=(state,))
        try:
            disconnect.start()
            self.assertTrue(socket_closed.wait(5))
            with server.lock:
                self.assertNotIn(state, server.clients)
                self.assertTrue(state.closing)
            self.assertTrue(disconnect.is_alive())
        finally:
            state.job_update_lock.release()
            disconnect.join(5)

        self.assertFalse(disconnect.is_alive())
    def test_disconnect_during_prepared_refresh_skips_without_job_state(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        observed_lock = ObservedRLock()
        state.job_update_lock = observed_lock  # type: ignore[assignment]
        server.clients = {state}
        snapshot = server.fetch_qbit_tip_template_snapshot()
        server.observe_tip_first_seen(snapshot.bestblockhash)
        server.pool_readiness_latched()
        server.tip_template_snapshot = snapshot
        bundle = server.prepare_tip_refresh_bundle(snapshot)
        state.send = lambda _payload: self.fail(  # type: ignore[method-assign]
            "disconnected client received prepared work"
        )
        socket_closed = threading.Event()
        state.close = socket_closed.set  # type: ignore[method-assign]
        results: list[tuple[int, float | None, float | None, int]] = []
        errors: list[BaseException] = []

        def refresh() -> None:
            try:
                results.append(
                    server._fanout_prepared_tip_refresh(
                        [state],
                        bundle,
                        snapshot,
                        heartbeat_name="qbit_blockpoll",
                    )
                )
            except BaseException as exc:  # noqa: BLE001 - surface thread failures
                errors.append(exc)

        observed_lock.acquire()
        observed_lock.observe_acquires = True
        refresh_thread = threading.Thread(target=refresh)
        disconnect_thread = threading.Thread(
            target=server.disconnect_client,
            args=(state,),
        )
        try:
            refresh_thread.start()
            self.assertTrue(observed_lock.acquire_attempted.wait(5))
            disconnect_thread.start()
            self.assertTrue(socket_closed.wait(5))
            refresh_thread.join(5)
            self.assertFalse(refresh_thread.is_alive())
            self.assertTrue(disconnect_thread.is_alive())
        finally:
            observed_lock.release()
            refresh_thread.join(5)
            disconnect_thread.join(5)
            server.shutdown_tip_refresh_executor()

        self.assertFalse(disconnect_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(results[0][0], 0)
        self.assertIsNone(state.active_job)
        self.assertEqual(state.active_job_ids, set())
        self.assertEqual(server.jobs, {})
    def test_mass_disconnect_releases_active_connection_accounting(self) -> None:
        server, _ = coordinator()
        states = [client(index) for index in range(1, 129)]
        for state in states:
            state.close = lambda: None  # type: ignore[method-assign]
        server.clients = set(states)

        for state in states:
            server.disconnect_client(state)

        with server.lock:
            self.assertEqual(len(server.clients), 0)
        self.assertTrue(all(state.closing for state in states))
    def test_concurrent_disconnect_is_idempotent_and_deadlock_free(self) -> None:
        server, _ = coordinator()
        state = client(1)
        server.clients = {state}
        close_count = 0
        close_count_lock = threading.Lock()
        caller_count = 16
        start = threading.Barrier(caller_count + 1)
        errors: list[BaseException] = []

        def close() -> None:
            nonlocal close_count
            with close_count_lock:
                close_count += 1

        def disconnect() -> None:
            try:
                start.wait()
                server.disconnect_client(state)
            except BaseException as exc:  # noqa: BLE001 - surface thread failures
                errors.append(exc)

        state.close = close  # type: ignore[method-assign]
        callers = [threading.Thread(target=disconnect) for _ in range(caller_count)]
        for caller in callers:
            caller.start()
        start.wait()
        for caller in callers:
            caller.join(5)

        self.assertTrue(all(not caller.is_alive() for caller in callers))
        self.assertEqual(errors, [])
        self.assertEqual(close_count, 1)
        self.assertNotIn(state, server.clients)
    def test_disconnect_removes_active_and_evicted_job_contexts(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        active = server.build_job_for_client(state, clean_jobs=True)
        evicted = server.build_job_for_client(state, clean_jobs=False)
        active_id = active.job.job_id
        evicted_id = evicted.job.job_id
        state.active_job = active
        state.active_job_ids = {active_id}
        server.jobs = {active_id: active, evicted_id: evicted}
        server.bury_evicted_job(state, evicted_id)
        server.jobs.pop(evicted_id)
        server.clients = {state}
        state.close = lambda: None  # type: ignore[method-assign]

        server.disconnect_client(state)

        self.assertIsNone(state.active_job)
        self.assertEqual(state.active_job_ids, set())
        self.assertNotIn(active_id, server.jobs)
        self.assertNotIn(evicted_id, server.evicted_job_graveyard)
        self.assertNotIn(state.connection_id, server.evicted_jobs_by_connection)
    def test_reconnect_storm_leaves_no_handler_threads_or_ghost_clients(self) -> None:
        server, _ = coordinator()
        connection_count = 32
        start = threading.Barrier(connection_count + 1)
        peers: list[socket.socket] = []
        handlers: list[threading.Thread] = []

        def handle(state: ClientState) -> None:
            start.wait()
            server.handle_client(state)

        for connection_id in range(1, connection_count + 1):
            coordinator_socket, peer_socket = socket.socketpair()
            state = client(connection_id)
            state.sock = coordinator_socket
            server.clients.add(state)
            peers.append(peer_socket)
            handler = threading.Thread(
                target=handle,
                args=(state,),
                name=f"prism-test-handler-{connection_id}",
            )
            handlers.append(handler)
            handler.start()

        start.wait()
        for peer in peers:
            peer.close()
        for handler in handlers:
            handler.join(5)

        self.assertTrue(all(not handler.is_alive() for handler in handlers))
        with server.lock:
            self.assertEqual(server.clients, set())
