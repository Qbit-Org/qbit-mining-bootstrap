#!/usr/bin/env python3
"""Focused PRISM coordinator job builder tests."""
# ruff: noqa: F403, F405

from __future__ import annotations

import unittest
from tests.prism_coordinator_test_support import *


class SnapshotAnchorFloorTests(unittest.TestCase):
    def _hold_floor(self, server: PrismCoordinator, share: PendingShare) -> None:
        server._ensure_pending_share_commit_state()
        with server._pending_share_commit_lock:
            server._pending_share_commit_floor[id(share)] = [
                share,
                time.monotonic(),
                False,
            ]
    def test_job_bundle_anchor_clamps_below_pending_share_commit(self) -> None:
        # The issued snapshot must be reproducible from the durable ledger:
        # while a stamped share's commit is pending, the job anchor (which the
        # bundle declares as anchor_job_issued_at_ms) has to predate it, or
        # qbit_audit_share_window at the declared anchor would include a share
        # the published window omitted.
        ledger = AnchorRecordingLedger()
        server, _rpc = coordinator(ledger=ledger)
        install_fake_bundle_builder(server)
        stamped_ms = now_ms() - 5
        share = stamped_pending_share(stamped_ms)
        self._hold_floor(server, share)

        bundle = server.build_shared_job_bundle(
            server.current_template_artifacts(),
            worker(),
        )
        self.assertEqual(ledger.anchors[-1], stamped_ms - 1)
        self.assertEqual(
            bundle.found_block["anchor_job_issued_at_ms"], stamped_ms - 1
        )
        self.assertEqual(bundle.issued_at_ms, stamped_ms - 1)

        server._finish_pending_share_commit(share)
        # The issued time is frozen per template generation; drop the frozen
        # entry so the rebuild stamps a fresh anchor now that no commit is
        # pending.
        with server._ensure_job_bundle_service()._cache_lock:
            server._ensure_job_bundle_service()._issued_at_ms.clear()
        rebuilt = server.build_shared_job_bundle(
            server.current_template_artifacts(),
            worker(),
        )
        self.assertGreaterEqual(ledger.anchors[-1], stamped_ms)
        self.assertGreaterEqual(
            int(rebuilt.found_block["anchor_job_issued_at_ms"]), stamped_ms
        )
    def test_payout_artifact_declares_its_own_snapshot_anchor(self) -> None:
        # An artifact snapshot is taken at its own (possibly clamped) anchor.
        # A bundle reusing the artifact must declare that anchor rather than
        # the fresher job-issue time: a share that was already durable at
        # artifact build time but stamped above the artifact's clamped anchor
        # is excluded from the artifact by construction, yet a re-derivation
        # at the job-issue anchor would include it.
        ledger = AnchorRecordingLedger()
        server, _rpc = coordinator(ledger=ledger)
        install_fake_bundle_builder(server)
        artifacts = server.current_template_artifacts()
        stamped_ms = now_ms() - 5
        share = stamped_pending_share(stamped_ms)
        self._hold_floor(server, share)

        artifact = server._build_payout_ledger_artifact(
            0, 0, artifacts.network_difficulty
        )
        assert artifact is not None
        self.assertEqual(artifact.snapshot_anchor_ms, stamped_ms - 1)
        self.assertEqual(ledger.anchors[-1], stamped_ms - 1)

        server._finish_pending_share_commit(share)
        # Construction re-validates that the passed artifact is the installed
        # current one.
        with server._payout_state_service._lock:
            server._payout_state_service._ledger_artifact = artifact
        bundle = server.build_shared_job_bundle(
            artifacts,
            worker(),
            payout_artifact=artifact,
        )
        self.assertEqual(
            bundle.found_block["anchor_job_issued_at_ms"],
            artifact.snapshot_anchor_ms,
        )
        self.assertGreater(bundle.issued_at_ms, int(artifact.snapshot_anchor_ms))

class JobBundleCacheTests(unittest.TestCase):
    def test_tip_template_snapshot_stays_coherent_across_tip_transition(self) -> None:
        old_tip = "11" * 32
        new_tip = "22" * 32
        server, rpc = coordinator(template=base_template(prevhash=old_tip))
        new_template = base_template(height=11, prevhash=new_tip)
        original_call = rpc.call

        def transition_during_template_fetch(
            method: str,
            params: list[object] | None = None,
        ) -> object:
            if method == "getblocktemplate":
                rpc.tip = new_tip
                rpc.template = new_template
            return original_call(method, params)

        rpc.call = transition_during_template_fetch  # type: ignore[method-assign]

        snapshot = server.fetch_qbit_tip_template_snapshot()

        self.assertEqual(snapshot.bestblockhash, new_tip)
        self.assertEqual(snapshot.previousblockhash, new_tip)
        self.assertEqual(
            snapshot.template_fingerprint,
            qbit_template_fingerprint(new_template),
        )
        self.assertEqual(rpc.calls[:2], ["getblocktemplate", "getbestblockhash"])
    def test_one_heavy_build_shared_across_clients_with_per_client_stamping(self) -> None:
        server, rpc = coordinator()
        recorded = install_fake_bundle_builder(server)
        clients = [client(1), client(2), client(3)]

        contexts = [server.build_job_for_client(c, clean_jobs=True) for c in clients]

        self.assertEqual(recorded["calls"], 1)
        self.assertEqual(rpc.count("getblocktemplate"), 1)
        self.assertEqual(server.ledger.snapshot_calls, 1)
        # The heavy build uses the placeholder extranonce1, never a client's.
        self.assertEqual(
            recorded["suffixes"],
            [
                server.coinbase_tag_hex
                + PRISM_JOB_EXTRANONCE1_PLACEHOLDER_HEX
                + "00" * EXTRANONCE2_SIZE
            ],
        )
        job_ids = {context.job.job_id for context in contexts}
        self.assertEqual(len(job_ids), 3)
        self.assertEqual(
            [context.job.extranonce1_hex for context in contexts],
            [c.extranonce1_hex for c in clients],
        )
        # coinb1/coinb2 exclude the extranonce window entirely, so the shared
        # split is byte-identical for every client.
        self.assertEqual(len({context.job.coinb1 for context in contexts}), 1)
        self.assertEqual(len({context.job.coinb2 for context in contexts}), 1)
        self.assertTrue(all(not hasattr(context, "bundle") for context in contexts))
        self.assertTrue(
            all(context.prospective_prior_balances == () for context in contexts)
        )
        cached = next(iter(server._ensure_job_bundle_service()._bundle_cache.values()))
        self.assertFalse(hasattr(cached, "bundle"))
        self.assertEqual(cached.prospective_prior_balances, ())
        self.assertEqual(
            cached.coinbase_manifest["coinbase_tx_hex"],
            synthetic_manifest_coinbase_hex(recorded["suffixes"][0]),
        )
        self.assertIs(contexts[0].shares_json, contexts[1].shares_json)
    def test_latest_wins_scheduler_preserves_synchronous_builder_output(self) -> None:
        server, rpc = coordinator()
        recorded = install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        identity = worker()
        cache_key = server._job_bundle_key(
            artifacts,
            mode="ready",
            payout_state_generation=0,
            worker=identity,
        )

        with patch("lab.prism.prism_coordinator.now_ms", return_value=1_700_000_001_000):
            direct_request = server._new_job_build_request(
                artifacts,
                identity,
                mode="ready",
                payout_state_generation=0,
                cache_key=cache_key,
            )
            direct = server.build_shared_job_bundle(
                artifacts,
                identity,
                mode="ready",
                payout_state_generation=0,
                key=cache_key,
                build_request=direct_request,
            )
            scheduled_request = server._new_job_build_request(
                artifacts,
                identity,
                mode="ready",
                payout_state_generation=0,
                cache_key=cache_key,
            )
            scheduled = server._request_job_build(scheduled_request).result(5)

        server.shutdown_job_build_executor()
        self.assertEqual(recorded["calls"], 2)
        self.assertEqual(scheduled.key, direct.key)
        self.assertEqual(scheduled.template, direct.template)
        self.assertEqual(scheduled.template_fingerprint, direct.template_fingerprint)
        self.assertEqual(scheduled.coinbase_manifest, direct.coinbase_manifest)
        self.assertEqual(scheduled.shares_json, direct.shares_json)
        self.assertEqual(scheduled.prior_balances, direct.prior_balances)
        self.assertEqual(scheduled.found_block, direct.found_block)
        self.assertEqual(scheduled.collection_only, direct.collection_only)
        self.assertEqual(scheduled.issued_at_ms, direct.issued_at_ms)
        self.assertEqual(scheduled.base_job, direct.base_job)
        self.assertEqual(scheduled.build_key, direct.build_key)
    def test_stamped_job_reassembles_coinbase_with_client_extranonce(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        state = client(0x2A)

        context = server.build_job_for_client(state, clean_jobs=True)
        extranonce2_hex = "11" * EXTRANONCE2_SIZE
        submission = direct_stratum.assemble_submission(
            context.job,
            extranonce2_hex=extranonce2_hex,
            ntime_hex="65000000",
            nonce_hex="00000001",
        )

        expected_suffix = server.coinbase_tag_hex + state.extranonce1_hex + extranonce2_hex
        coinbase = bytes.fromhex(submission.coinbase_tx_hex)
        script_start, script_len = direct_stratum.coinbase_scriptsig_span(
            coinbase, field_name="stamped coinbase"
        )
        script_sig_hex = coinbase[script_start : script_start + script_len].hex()
        self.assertTrue(script_sig_hex.endswith(expected_suffix))
        self.assertNotIn(PRISM_JOB_EXTRANONCE1_PLACEHOLDER_HEX, script_sig_hex[len("03aabbcc") :])
    def test_template_fingerprint_change_invalidates_bundle_cache(self) -> None:
        server, rpc = coordinator()
        recorded = install_fake_bundle_builder(server)
        server.build_job_for_client(client(1), clean_jobs=True)

        new_template = base_template(height=11, prevhash="22" * 32)
        rpc.template = new_template
        rpc.tip = str(new_template["previousblockhash"])
        server.store_template_artifacts(dict(new_template))

        context = server.build_job_for_client(client(2), clean_jobs=True)

        self.assertEqual(recorded["calls"], 2)
        self.assertEqual(context.template_fingerprint, qbit_template_fingerprint(new_template))
        # Bundles for the old fingerprint are evicted.
        self.assertEqual(
            {entry.template_fingerprint for entry in server._ensure_job_bundle_service()._bundle_cache.values()},
            {qbit_template_fingerprint(new_template)},
        )
    def test_bundle_cache_ttl_expiry_rebuilds(self) -> None:
        server, _ = coordinator()
        recorded = install_fake_bundle_builder(server)
        server.job_bundle_cache_seconds = 0.05
        server._ensure_job_bundle_service().set_cache_seconds_for_test(0.05)

        server.build_job_for_client(client(1), clean_jobs=True)
        time.sleep(0.06)
        server.build_job_for_client(client(2), clean_jobs=True)

        self.assertEqual(recorded["calls"], 2)
    def test_bundle_cache_lookup_prunes_every_expired_snapshot(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(base_template())
        assert artifacts is not None
        current = server.shared_job_bundle(artifacts, worker())
        expired_key = ("expired-template", "ready")
        expired = dataclass_replace(
            current,
            key=expired_key,
            template_fingerprint="expired-template",
            built_monotonic=time.monotonic() - 60,
        )
        server._ensure_job_bundle_service()._bundle_cache[expired_key] = expired

        looked_up = server._lookup_job_bundle(current.key)

        self.assertIs(looked_up, current)
        self.assertNotIn(expired_key, server._ensure_job_bundle_service()._bundle_cache)
        self.assertEqual(list(server._ensure_job_bundle_service()._bundle_cache.values()), [current])
    def test_zero_ttl_disables_bundle_cache(self) -> None:
        server, _ = coordinator()
        recorded = install_fake_bundle_builder(server)
        server.job_bundle_cache_seconds = 0.0
        server._ensure_job_bundle_service().set_cache_seconds_for_test(0.0)

        server.build_job_for_client(client(1), clean_jobs=True)
        server.build_job_for_client(client(2), clean_jobs=True)

        self.assertEqual(recorded["calls"], 2)
    def test_payout_state_change_during_build_retries_before_cache_or_return(self) -> None:
        server, rpc = coordinator()
        recorded = install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        self.assertIsNotNone(artifacts)
        assert artifacts is not None
        identity = worker()
        service = server._ensure_job_bundle_service()
        original_build = service.build_shared_job_bundle
        built_generations: list[int] = []

        def mutate_after_first_build(
            build_artifacts: object,
            build_worker: WorkerIdentity | None,
            **kwargs: object,
        ) -> object:
            bundle = original_build(  # type: ignore[arg-type]
                build_artifacts,
                build_worker,
                **kwargs,
            )
            built_generations.append(bundle.payout_state_generation)
            if len(built_generations) == 1:
                server._advance_payout_state_generation()
            return bundle

        service.build_shared_job_bundle = mutate_after_first_build  # type: ignore[method-assign]

        bundle = server.shared_job_bundle(artifacts, identity)
        cached = server.shared_job_bundle(artifacts, identity)

        self.assertEqual(built_generations, [0, 1])
        self.assertEqual(recorded["calls"], 2)
        self.assertEqual(bundle.payout_state_generation, 1)
        self.assertIs(cached, bundle)
    def test_escaped_stale_bundle_is_rejected_before_direct_delivery(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        server._ensure_tip_refresh_state()
        state = client(1)
        sent: list[dict[str, object]] = []
        state.send = sent.append  # type: ignore[method-assign]
        original_shared_job_bundle = server.shared_job_bundle
        advanced = False

        def advance_after_bundle(*args: object, **kwargs: object) -> object:
            nonlocal advanced
            bundle = original_shared_job_bundle(*args, **kwargs)  # type: ignore[arg-type]
            if not advanced:
                advanced = True
                server._advance_payout_state_generation()
            return bundle

        server.shared_job_bundle = advance_after_bundle  # type: ignore[method-assign]

        self.assertFalse(server.maybe_send_job(state, clean_jobs=True))
        self.assertEqual(sent, [])
        self.assertIsNone(state.active_job)
        self.assertEqual(server._payout_state_service._generation, 1)
        self.assertTrue(server._ensure_tip_refresh_service().snapshot().retry_requested)

        self.assertTrue(server.maybe_send_job(state, clean_jobs=True))
        self.assertIsNotNone(state.active_job)
        self.assertEqual(state.active_job.payout_state_generation, 1)
        self.assertEqual(
            [payload["method"] for payload in sent],
            ["mining.set_difficulty", "mining.notify"],
        )
    def test_priority_decision_uses_one_publication_snapshot(self) -> None:
        server, _rpc = coordinator()
        state = client(1)
        context = SimpleNamespace(
            payout_state_generation=0,
            template={"previousblockhash": "11" * 32},
        )
        server.ensure_reorg_reconciled_for_current_tip = (  # type: ignore[method-assign]
            lambda **_kwargs: True
        )
        server.build_job_for_client = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: context
        )
        original_lock = server._payout_state_service._lock

        class PublishAfterPrioritySnapshot:
            advanced = False

            def __enter__(self) -> object:
                original_lock.acquire()
                return self

            def __exit__(
                self,
                _exc_type: object,
                _exc: object,
                _traceback: object,
            ) -> None:
                original_lock.release()
                if not self.advanced:
                    self.advanced = True
                    server._payout_state_service._generation = 1

        priorities: list[bool] = []

        class RecordingGate:
            @contextmanager
            def delivery_cancelable(
                self,
                _cancelled: object,
                *,
                priority: bool,
                **_kwargs: object,
            ) -> object:
                priorities.append(priority)
                yield False

        server._payout_state_service._lock = PublishAfterPrioritySnapshot()  # type: ignore[assignment]
        server._payout_state_service._delivery_gate = RecordingGate()  # type: ignore[assignment]

        self.assertFalse(server.maybe_send_job(state, clean_jobs=True))
        self.assertEqual(priorities, [True])
        self.assertEqual(server._payout_state_service._generation, 1)
    def test_zero_template_ttl_fetches_template_per_build(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.template_cache_seconds = 0.0
        server._ensure_job_bundle_service().template_repository.set_cache_seconds_for_test(0.0)

        server.build_job_for_client(client(1), clean_jobs=True)
        server.build_job_for_client(client(2), clean_jobs=True)

        self.assertEqual(rpc.count("getblocktemplate"), 2)
    def test_late_stale_template_fetch_cannot_replace_newer_artifacts(self) -> None:
        server, rpc = coordinator()
        server.template_cache_seconds = 0.0
        server._ensure_job_bundle_service().template_repository.set_cache_seconds_for_test(0.0)
        stale_template = dict(rpc.template)
        current_template = base_template(height=11, prevhash="22" * 32)
        fetch_started = threading.Event()
        release_fetch = threading.Event()
        results: list[object] = []
        errors: list[BaseException] = []
        original_call = rpc.call
        thread: threading.Thread

        def blocking_call(
            method: str,
            params: list[object] | None = None,
        ) -> object:
            if method == "getblocktemplate" and threading.current_thread() is thread:
                fetch_started.set()
                if not release_fetch.wait(5):
                    raise AssertionError("stale template fetch was not released")
                return dict(stale_template)
            return original_call(method, params)

        def fetch_stale_artifacts() -> None:
            try:
                results.append(server.current_template_artifacts())
            except BaseException as exc:  # noqa: BLE001 - surface to the test
                errors.append(exc)

        rpc.call = blocking_call  # type: ignore[method-assign]
        thread = threading.Thread(target=fetch_stale_artifacts)
        thread.start()
        try:
            self.assertTrue(fetch_started.wait(5))
            current_artifacts = server.store_template_artifacts(current_template)
            self.assertIsNotNone(current_artifacts)
            assert current_artifacts is not None
            self.assertGreater(current_artifacts.generation, 1)
        finally:
            release_fetch.set()
            thread.join(5)

        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(results, [current_artifacts])
        self.assertIs(
            server._ensure_job_bundle_service().template_repository.current_artifacts(),
            current_artifacts,
        )
        self.assertEqual(
            current_artifacts.fingerprint,
            qbit_template_fingerprint(current_template),
        )
    def test_collection_mode_bundles_are_keyed_per_worker(self) -> None:
        server, _ = coordinator(ledger=FakeLedger(miners=["solo"]))
        recorded = install_fake_bundle_builder(server)
        server.min_ready_miners = 3
        server._ensure_job_bundle_service().set_min_ready_miners_for_test(3)

        worker_a = worker(payout="tq1worker-a")
        worker_b = worker(payout="tq1worker-b")
        context_a1 = server.build_job_for_client(client(1, worker_a), clean_jobs=True)
        context_a2 = server.build_job_for_client(client(2, worker_a), clean_jobs=True)
        context_b = server.build_job_for_client(client(3, worker_b), clean_jobs=True)

        self.assertTrue(context_a1.collection_only)
        self.assertTrue(context_b.collection_only)
        self.assertEqual(recorded["calls"], 2)
        self.assertTrue(
            all(not hasattr(context, "bundle") for context in (context_a1, context_a2, context_b))
        )
        self.assertIs(context_a1.shares_json, context_a2.shares_json)
        self.assertIsNot(context_a1.shares_json, context_b.shares_json)
    def test_collection_bundle_cache_rebuilds_when_pool_becomes_ready(self) -> None:
        ledger = FakeLedger(miners=["solo"])
        server, _ = coordinator(ledger=ledger)
        recorded = install_fake_bundle_builder(server)
        state = client(1)

        collection_context = server.build_job_for_client(state, clean_jobs=True)
        ledger.miners = ["miner-a", "miner-b", "miner-c"]
        ready_context = server.build_job_for_client(state, clean_jobs=True)

        self.assertTrue(collection_context.collection_only)
        self.assertFalse(ready_context.collection_only)
        self.assertEqual(recorded["calls"], 2)
        self.assertEqual(ledger.snapshot_calls, 1)
    def test_ready_empty_snapshot_does_not_fall_back_to_worker_collection(self) -> None:
        ledger = ReadyLedgerWithEmptyFirstSnapshot()
        server, _ = coordinator(ledger=ledger)
        recorded = install_fake_bundle_builder(server)
        state = client(1)

        with self.assertRaisesRegex(
            RuntimeError,
            "ready-pool ledger snapshot contained no payout shares",
        ):
            server.build_job_for_client(state, clean_jobs=True)
        ready_context = server.build_job_for_client(state, clean_jobs=True)

        self.assertFalse(ready_context.collection_only)
        self.assertEqual(recorded["calls"], 1)
        self.assertEqual(ledger.snapshot_calls, 2)
    def test_vardiff_difficulty_is_stamped_per_client(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        easy = client(1)
        hard = client(2)
        hard.pending_share_difficulty = Decimal("512")

        easy_context = server.build_job_for_client(easy, clean_jobs=True)
        hard_context = server.build_job_for_client(hard, clean_jobs=True)

        self.assertEqual(easy_context.job.coinb1, hard_context.job.coinb1)
        self.assertGreater(easy_context.job.share_target, hard_context.job.share_target)
        self.assertEqual(hard_context.job.share_difficulty, Decimal("512"))
    def test_template_artifacts_reuse_derivations_when_fingerprint_unchanged(self) -> None:
        server, _ = coordinator()
        template = base_template()
        first = server.store_template_artifacts(dict(template))
        refreshed_template = dict(template)
        refreshed_template["curtime"] = int(template["curtime"]) + 30
        second = server.store_template_artifacts(refreshed_template)

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        assert first is not None and second is not None
        self.assertEqual(first.fingerprint, second.fingerprint)
        self.assertIs(first.transaction_hexes, second.transaction_hexes)
        self.assertIs(first.witness_merkle_leaves_hex, second.witness_merkle_leaves_hex)
    def test_poll_seeds_template_cache_for_client_builds(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)

        refreshed = server.poll_qbit_tip_template_once()
        self.assertEqual(refreshed, 0)
        self.assertEqual(rpc.count("getblocktemplate"), 1)

        server.build_job_for_client(client(1), clean_jobs=True)
        self.assertEqual(rpc.count("getblocktemplate"), 1)
    def test_reorg_reconciliation_cached_per_tip(self) -> None:
        server, rpc = coordinator()
        server.reorg_reconciler_enabled = True
        reconcile_calls: list[str | None] = []

        def fake_reconcile(*, tip_hash: str | None = None) -> dict[str, object]:
            reconcile_calls.append(tip_hash)
            with server.lock:
                server.last_reorg_reconciled_tip_hash = tip_hash
                server.last_reorg_reconciled_trusted = True
                server.last_reorg_reconciled_monotonic = time.monotonic()
            return {"untrusted": False}

        server.reconcile_prism_pool_blocks_once = fake_reconcile  # type: ignore[method-assign]

        self.assertTrue(server.ensure_reorg_reconciled_for_current_tip())
        self.assertTrue(server.ensure_reorg_reconciled_for_current_tip())
        self.assertEqual(len(reconcile_calls), 1)

        rpc.tip = "33" * 32
        self.assertTrue(server.ensure_reorg_reconciled_for_current_tip())
        self.assertEqual(len(reconcile_calls), 2)
        self.assertEqual(reconcile_calls[-1], "33" * 32)
    def test_reorg_cache_rechecks_chain_view_before_reuse(self) -> None:
        server, rpc = coordinator()
        server.reorg_reconciler_enabled = True
        rpc.blockchain_info["headers"] = 101
        with server.lock:
            server.last_reorg_reconciled_tip_hash = rpc.tip
            server.last_reorg_reconciled_trusted = True
            server.last_reorg_reconciled_monotonic = time.monotonic()
        reconcile_calls: list[str | None] = []

        def fake_reconcile(*, tip_hash: str | None = None) -> dict[str, object]:
            reconcile_calls.append(tip_hash)
            with server.lock:
                server.last_reorg_reconciled_tip_hash = tip_hash
                server.last_reorg_reconciled_trusted = False
                server.last_reorg_reconciled_monotonic = time.monotonic()
            return {"untrusted": True}

        server.reconcile_prism_pool_blocks_once = fake_reconcile  # type: ignore[method-assign]

        self.assertFalse(server.ensure_reorg_reconciled_for_current_tip())
        self.assertEqual(rpc.count("getblockchaininfo"), 1)
        self.assertEqual(reconcile_calls, [rpc.tip])
        self.assertFalse(server.last_reorg_reconciled_trusted)
    def test_single_flight_builds_once_under_concurrency(self) -> None:
        server, _ = coordinator()
        recorded = install_fake_bundle_builder(server)
        original_builder = server.build_audit_bundle
        build_started = threading.Event()

        def slow_builder(**kwargs: object) -> dict[str, object]:
            build_started.set()
            time.sleep(0.05)
            return original_builder(**kwargs)

        server.build_audit_bundle = slow_builder  # type: ignore[method-assign]
        server._ensure_bundle_compiler().build_audit_bundle = (  # type: ignore[method-assign]
            slow_builder
        )
        errors: list[BaseException] = []

        def build(connection_id: int) -> None:
            try:
                server.build_job_for_client(client(connection_id), clean_jobs=True)
            except BaseException as exc:  # noqa: BLE001 - surface to the test
                errors.append(exc)

        threads = [threading.Thread(target=build, args=(index + 1,)) for index in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])
        self.assertEqual(recorded["calls"], 1)
    def test_observed_tip_change_rejects_stale_cached_bundle_delivery(self) -> None:
        old_tip = "11" * 32
        new_tip = "22" * 32
        server, rpc = coordinator(template=base_template(prevhash=old_tip))
        install_fake_bundle_builder(server)
        state = client(1)
        sent: list[dict[str, object]] = []
        state.send = sent.append  # type: ignore[method-assign]
        server.clients = {state}
        server.observe_tip_first_seen(old_tip, observation_sequence=1)

        self.assertTrue(server.maybe_send_job(state, clean_jobs=True))
        self.assertEqual(len(sent), 2)
        sent.clear()

        rpc.tip = new_tip
        server.observe_tip_first_seen(new_tip, observation_sequence=2)

        self.assertFalse(server.maybe_send_job(state, clean_jobs=True))
        self.assertEqual(sent, [])
    def test_ready_ledger_snapshot_holds_payout_mutation_lock(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        entered_snapshot = threading.Event()
        release_snapshot = threading.Event()
        original_snapshot = server.ledger.snapshot_at_job_issue

        def blocked_snapshot(*args: object, **kwargs: object) -> object:
            entered_snapshot.set()
            self.assertTrue(release_snapshot.wait(2))
            return original_snapshot(*args, **kwargs)

        server.ledger.snapshot_at_job_issue = blocked_snapshot  # type: ignore[method-assign]
        errors: list[BaseException] = []
        build_thread = threading.Thread(
            target=lambda: self._capture_error(
                errors,
                lambda: server.shared_job_bundle(artifacts, mode="ready"),
            )
        )
        build_thread.start()
        try:
            self.assertTrue(entered_snapshot.wait(2))
            mutation_acquired = server._payout_state_service._prepare_lock.acquire(
                blocking=False
            )
            if mutation_acquired:
                server._payout_state_service._prepare_lock.release()
            self.assertFalse(mutation_acquired)
        finally:
            release_snapshot.set()
        build_thread.join(2)

        self.assertFalse(build_thread.is_alive())
        self.assertEqual(errors, [])
    def test_ready_build_identity_separates_clock_only_generations(self) -> None:
        server, rpc = coordinator()
        first = server.store_template_artifacts(dict(rpc.template))
        second_template = dict(rpc.template)
        second_template["curtime"] = int(second_template["curtime"]) + 1
        second = server.store_template_artifacts(second_template)
        assert first is not None and second is not None
        self.assertEqual(first.fingerprint, second.fingerprint)
        self.assertNotEqual(first.generation, second.generation)
        payout_generation = server._payout_state_service._generation

        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=[1_700_000_000_000, 1_700_000_001_000],
        ):
            first_request = server._new_job_build_request(
                first,
                None,
                mode="ready",
                payout_state_generation=payout_generation,
                cache_key=server._job_bundle_key(
                    first,
                    mode="ready",
                    payout_state_generation=payout_generation,
                    worker=None,
                ),
            )
            second_request = server._new_job_build_request(
                second,
                None,
                mode="ready",
                payout_state_generation=payout_generation,
                cache_key=server._job_bundle_key(
                    second,
                    mode="ready",
                    payout_state_generation=payout_generation,
                    worker=None,
                ),
            )

        self.assertNotEqual(
            first_request.equivalence_key,
            second_request.equivalence_key,
        )
        self.assertNotEqual(
            first_request.key.issued_at_ms,
            second_request.key.issued_at_ms,
        )
    def test_precomputed_payout_artifact_matches_inline_output_exactly(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None

        with patch("lab.prism.prism_coordinator.now_ms", return_value=1_700_000_123_000):
            inline = server.shared_job_bundle(artifacts, mode="ready")
            with server._ensure_job_bundle_service()._cache_lock:
                server._ensure_job_bundle_service()._bundle_cache.clear()
            server._prepare_payout_ledger_artifact(
                server._payout_state_service._generation,
                artifacts.network_difficulty,
            )
            prepared = server.shared_job_bundle(artifacts, mode="ready")

        self.assertEqual(prepared.base_job, inline.base_job)
        self.assertEqual(prepared.coinbase_manifest, inline.coinbase_manifest)
        self.assertEqual(prepared.shares_json, inline.shares_json)
        self.assertEqual(prepared.prior_balances, inline.prior_balances)
        self.assertEqual(prepared.found_block, inline.found_block)
        self.assertGreater(prepared.payout_artifact_generation, 0)
    def test_accepted_preview_patches_artifact_across_normal_clear(self) -> None:
        class CountingBalanceLedger(FakeLedger):
            def __init__(self) -> None:
                super().__init__()
                self.prior_balance_reads = 0
                self.database_balances = [
                    {
                        "recipient_id": "stale-miner",
                        "order_key": "stale-miner",
                        "p2mr_program_hex": "22" * 32,
                        "balance_sats": 1,
                    }
                ]

            def current_prior_balances(self) -> list[dict[str, object]]:
                self.prior_balance_reads += 1
                return [dict(balance) for balance in self.database_balances]

        ledger = CountingBalanceLedger()
        server, rpc = coordinator(ledger=ledger)
        recorded = install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._ensure_job_bundle_service().set_ready_for_test(True)
        parent_hash = str(rpc.template["previousblockhash"])
        parent_height = int(rpc.template["height"]) - 1
        preview = [
            {
                "recipient_id": "miner-a",
                "order_key": "miner-a",
                "p2mr_program_hex": "11" * 32,
                "balance_sats": 25,
            }
        ]

        server._begin_accepted_block_payout_preview(
            parent_hash,
            block_height=parent_height,
        )
        server._publish_accepted_block_payout_preview(parent_hash, preview)

        artifact = server._payout_state_service._ledger_artifact
        self.assertIsNotNone(artifact)
        assert artifact is not None
        self.assertEqual(artifact.prior_balances, tuple(preview))
        self.assertEqual(
            artifact.payout_state_generation,
            server._payout_state_service._generation,
        )
        self.assertGreater(artifact.generation, 0)

        ledger.prior_balance_reads = 0
        preview_bundle = server.shared_job_bundle(artifacts, mode="ready")

        self.assertEqual(preview_bundle.prior_balances, preview)
        self.assertEqual(recorded["last_kwargs"]["prior_balances"], preview)  # type: ignore[index]
        self.assertEqual(
            preview_bundle.payout_artifact_generation,
            artifact.generation,
        )
        self.assertEqual(ledger.prior_balance_reads, 0)

        payout_generation = server._payout_state_service._generation
        ledger.database_balances = [dict(balance) for balance in preview]
        server._clear_accepted_block_payout_preview(parent_hash)
        self.assertEqual(server._payout_state_service._generation, payout_generation)
        self.assertIs(server._payout_state_service._ledger_artifact, artifact)
        self.assertNotIn(parent_hash, server._payout_state_service._previews)
        self.assertNotIn(
            parent_hash,
            server._payout_state_service._invalidated_previews,
        )
        with server._ensure_job_bundle_service()._cache_lock:
            server._ensure_job_bundle_service()._bundle_cache.clear()

        post_clear_bundle = server.shared_job_bundle(artifacts, mode="ready")

        self.assertEqual(post_clear_bundle.prior_balances, preview)
        self.assertEqual(recorded["last_kwargs"]["prior_balances"], preview)  # type: ignore[index]
        self.assertEqual(
            post_clear_bundle.payout_artifact_generation,
            artifact.generation,
        )
        self.assertEqual(ledger.prior_balance_reads, 0)
    def test_valid_precomputed_artifact_skips_tip_path_ledger_snapshot(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        snapshot = server.fetch_qbit_tip_template_snapshot()
        assert snapshot.template_artifacts is not None
        server._prepare_payout_ledger_artifact(
            server._payout_state_service._generation,
            snapshot.template_artifacts.network_difficulty,
        )
        server.ledger.snapshot_calls = 0

        bundle = server.prepare_tip_refresh_bundle(snapshot)

        self.assertFalse(bundle.collection_only)
        self.assertGreater(bundle.payout_artifact_generation, 0)
        self.assertEqual(server.ledger.snapshot_calls, 0)
    def test_mismatched_precomputed_artifact_falls_back_to_inline_snapshot(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._current_payout_state_artifact()
        server._prepare_payout_ledger_artifact(
            server._payout_state_service._generation,
            artifacts.network_difficulty,
        )
        with server._payout_state_service._lock:
            published = server._payout_state_service._published
            assert published.artifact is not None
            changed_balances = [{"miner_id": "miner-z", "balance_sats": 1}]
            server._payout_state_service._published = dataclass_replace(
                published,
                artifact=dataclass_replace(
                    published.artifact,
                    prior_balances_json=canonical_json_text(changed_balances),
                    prior_balances_sha256=canonical_json_sha256(changed_balances),
                ),
            )

        self.assertIsNone(
            server._usable_payout_ledger_artifact(
                server._payout_state_service._generation,
                artifacts.network_difficulty,
            )
        )
        self.assertIsNone(server._payout_state_service._ledger_artifact)
        server.ledger.snapshot_calls = 0

        bundle = server.shared_job_bundle(artifacts, mode="ready")

        self.assertEqual(bundle.payout_artifact_generation, 0)
        self.assertEqual(server.ledger.snapshot_calls, 1)
    def test_new_tip_cancels_blocked_old_bundle_without_publication(self) -> None:
        old_tip = "11" * 32
        new_tip = "22" * 32
        server, rpc = coordinator(template=base_template(prevhash=old_tip))
        recorded = install_fake_bundle_builder(server)
        original_builder = server.build_audit_bundle
        build_started = threading.Event()

        def cancelable_builder(**kwargs: object) -> dict[str, object]:
            control = server._ensure_job_bundle_service()._phase_local.bundle_build_control
            build_started.set()
            self.assertTrue(control.cancel_event.wait(2))
            return original_builder(**kwargs)

        server.build_audit_bundle = cancelable_builder  # type: ignore[method-assign]
        server._ensure_bundle_compiler().build_audit_bundle = (  # type: ignore[method-assign]
            cancelable_builder
        )
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server.observe_tip_first_seen(old_tip, observation_sequence=1)
        errors: list[BaseException] = []
        thread = threading.Thread(
            target=lambda: self._capture_error(
                errors,
                lambda: server.shared_job_bundle(artifacts, mode="ready"),
            )
        )
        thread.start()
        self.assertTrue(build_started.wait(2))

        server.observe_tip_first_seen(new_tip, observation_sequence=2)
        thread.join(2)

        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], TemplateRefreshBlocked)
        self.assertEqual(recorded["calls"], 1)
        self.assertEqual(server._ensure_job_bundle_service()._active_bundle_builds, {})
        self.assertEqual(server._ensure_tip_refresh_service().metrics_snapshot()["build_inflight"], 0)
        self.assertFalse(any(
            entry.template_fingerprint == artifacts.fingerprint
            for entry in server._ensure_job_bundle_service()._bundle_cache.values()
        ))
        self.assertEqual(server._ensure_tip_refresh_service().metrics_snapshot()["superseded_results"], 1)
    def test_builder_crash_and_timeout_fail_closed_then_recover(self) -> None:
        server, _rpc = coordinator()
        server.prism_ctv_settlement_config = lambda **_kwargs: None  # type: ignore[method-assign]
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        build_kwargs = {
            "shares": [],
            "found_block": {
                "block_height": 10,
                "coinbase_value_sats": 50_00000000,
                "network_difficulty": 1,
                "anchor_job_issued_at_ms": 1_700_000_000_000,
            },
            "prior_balances": [],
            "coinbase_script_sig_suffix_hex": "00",
        }

        with patch(
            "lab.prism.bundle_compiler.prism_tool_command",
            return_value=[sys.executable, "-c", "raise SystemExit(7)"],
        ):
            with self.assertRaisesRegex(RuntimeError, "failed"):
                server.build_audit_bundle(**build_kwargs)

        server.bundle_build_timeout_seconds = 0.01
        with patch(
            "lab.prism.bundle_compiler.prism_tool_command",
            return_value=[
                sys.executable,
                "-c",
                "import time; time.sleep(5)",
            ],
        ):
            with self.assertRaisesRegex(RuntimeError, "timed out"):
                server.build_audit_bundle(**build_kwargs)

        server.bundle_build_timeout_seconds = 1.0
        recovery_script = (
            "import json,sys; json.load(sys.stdin); "
            "json.dump({'recovered': True}, sys.stdout)"
        )
        with patch(
            "lab.prism.bundle_compiler.prism_tool_command",
            return_value=[sys.executable, "-c", recovery_script],
        ):
            recovered = server.build_audit_bundle(**build_kwargs)
        self.assertEqual(recovered, {"recovered": True})
    def test_audit_builder_child_does_not_inherit_open_socket(self) -> None:
        server, _rpc = coordinator()
        server.prism_ctv_settlement_config = lambda **_kwargs: None  # type: ignore[method-assign]
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        probe_script = (
            "import json,os,sys; json.load(sys.stdin); fd=int(sys.argv[1]); "
            "inherited=True; "
            "\ntry: os.fstat(fd)"
            "\nexcept OSError: inherited=False"
            "\njson.dump({'inherited_socket': inherited}, sys.stdout)"
        )
        with socket.socket() as parent_socket:
            parent_socket.set_inheritable(True)
            with patch(
                "lab.prism.bundle_compiler.prism_tool_command",
                return_value=[
                    sys.executable,
                    "-c",
                    probe_script,
                    str(parent_socket.fileno()),
                ],
            ):
                result = server.build_audit_bundle(
                    shares=[],
                    found_block={
                        "block_height": 10,
                        "coinbase_value_sats": 50_00000000,
                        "network_difficulty": 1,
                        "anchor_job_issued_at_ms": 1_700_000_000_000,
                    },
                    prior_balances=[],
                    coinbase_script_sig_suffix_hex="00",
                )
        self.assertEqual(result, {"inherited_socket": False})
    def test_repeated_superseded_builds_leave_state_bounded(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        original_builder = server.build_audit_bundle
        starts: queue.Queue[None] = queue.Queue()

        def cancelable_builder(**kwargs: object) -> dict[str, object]:
            control = server._ensure_job_bundle_service()._phase_local.bundle_build_control
            starts.put(None)
            self.assertTrue(control.cancel_event.wait(2))
            return original_builder(**kwargs)

        server.build_audit_bundle = cancelable_builder  # type: ignore[method-assign]
        server._ensure_bundle_compiler().build_audit_bundle = (  # type: ignore[method-assign]
            cancelable_builder
        )
        current_tip = str(rpc.tip)
        server.observe_tip_first_seen(current_tip, observation_sequence=1)
        errors: list[BaseException] = []
        for index in range(8):
            rpc.template = base_template(height=10 + index, prevhash=current_tip)
            artifacts = server.store_template_artifacts(dict(rpc.template))
            assert artifacts is not None
            thread = threading.Thread(
                target=lambda current=artifacts: self._capture_error(
                    errors,
                    lambda: server.shared_job_bundle(current, mode="ready"),
                )
            )
            thread.start()
            starts.get(timeout=2)
            current_tip = f"{index + 2:064x}"
            rpc.tip = current_tip
            server.observe_tip_first_seen(
                current_tip,
                observation_sequence=index + 2,
            )
            thread.join(2)
            self.assertFalse(thread.is_alive())

        self.assertEqual(len(errors), 8)
        self.assertTrue(all(isinstance(exc, TemplateRefreshBlocked) for exc in errors))
        self.assertEqual(server._ensure_job_bundle_service()._active_bundle_builds, {})
        self.assertEqual(server._ensure_tip_refresh_service().metrics_snapshot()["build_inflight"], 0)
        self.assertEqual(server._ensure_tip_refresh_service().metrics_snapshot()["build_queue_depth"], 0)
        self.assertLessEqual(
            len(server._ensure_job_bundle_service()._bundle_cache),
            MAX_PRISM_JOB_BUNDLE_CACHE_ENTRIES,
        )
        self.assertEqual(server._ensure_tip_refresh_service().metrics_snapshot()["superseded_results"], 8)
    @staticmethod
    def _capture_error(
        errors: list[BaseException],
        operation: object,
    ) -> None:
        try:
            operation()  # type: ignore[operator]
        except BaseException as exc:  # noqa: BLE001 - test thread handoff
            errors.append(exc)
    def test_same_fingerprint_bundle_rebinds_exact_template_observation(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        identity = worker()
        first = server.store_template_artifacts(base_template())
        assert first is not None
        original = server.shared_job_bundle(first, identity)
        updated_template = dict(first.template)
        updated_template["curtime"] = int(updated_template["curtime"]) + 30
        second = server.store_template_artifacts(updated_template)
        assert second is not None

        rebound = server.shared_job_bundle(second, identity)

        self.assertEqual(first.fingerprint, second.fingerprint)
        self.assertIs(rebound.template, second.template)
        self.assertIsNot(rebound.template, original.template)
        self.assertEqual(rebound.template_generation, second.generation)
        self.assertEqual(rebound.base_job.ntime, f'{updated_template["curtime"]:08x}')
    def test_clock_only_refresh_does_not_discard_inflight_ready_build(self) -> None:
        server, rpc = coordinator()
        recorded = install_fake_bundle_builder(server)
        first = server.store_template_artifacts(dict(rpc.template))
        assert first is not None
        build_entered = threading.Event()
        release_build = threading.Event()
        service = server._ensure_job_bundle_service()
        original_build = service.build_shared_job_bundle
        build_calls = 0
        build_calls_lock = threading.Lock()

        def blocking_build(*args: object, **kwargs: object) -> object:
            nonlocal build_calls
            with build_calls_lock:
                build_calls += 1
            build_entered.set()
            self.assertTrue(release_build.wait(5))
            return original_build(*args, **kwargs)  # type: ignore[arg-type]

        service.build_shared_job_bundle = blocking_build  # type: ignore[method-assign]
        results: list[list[object]] = [[], []]
        errors: list[list[BaseException]] = [[], []]

        def build(index: int, build_artifacts: object) -> None:
            try:
                results[index].append(
                    server.shared_job_bundle(  # type: ignore[arg-type]
                        build_artifacts,
                        mode="ready",
                    )
                )
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors[index].append(exc)

        first_thread = threading.Thread(target=build, args=(0, first))
        second_thread: threading.Thread | None = None
        first_thread.start()
        try:
            self.assertTrue(build_entered.wait(5))
            updated_template = dict(first.template)
            updated_template["curtime"] = int(updated_template["curtime"]) + 30
            second = server.store_template_artifacts(updated_template)
            assert second is not None
            second_thread = threading.Thread(target=build, args=(1, second))
            second_thread.start()
            time.sleep(0.05)
            with build_calls_lock:
                self.assertEqual(build_calls, 1)
        finally:
            release_build.set()
            first_thread.join(5)
            if second_thread is not None:
                second_thread.join(5)

        self.assertFalse(first_thread.is_alive())
        assert second_thread is not None
        self.assertFalse(second_thread.is_alive())
        self.assertEqual(errors, [[], []])
        self.assertEqual([len(items) for items in results], [1, 1])
        built = results[0][0]
        self.assertIs(built.template, first.template)  # type: ignore[union-attr]
        rebound = results[1][0]
        self.assertIs(rebound.template, second.template)
        self.assertEqual(rebound.template_generation, second.generation)
        self.assertEqual(recorded["calls"], 1)
    def test_same_fingerprint_collection_bundle_rebuilds_exact_observation(self) -> None:
        server, _ = coordinator(ledger=FakeLedger(miners=["miner-a"]))
        recorded = install_fake_bundle_builder(server)
        identity = worker()
        first = server.store_template_artifacts(base_template())
        assert first is not None
        original = server.shared_job_bundle(first, identity)
        updated_template = dict(first.template)
        updated_template["curtime"] = int(updated_template["curtime"]) + 30
        second = server.store_template_artifacts(updated_template)
        assert second is not None

        rebuilt = server.shared_job_bundle(second, identity)

        self.assertTrue(original.collection_only)
        self.assertTrue(rebuilt.collection_only)
        self.assertEqual(first.fingerprint, second.fingerprint)
        self.assertEqual(recorded["calls"], 2)
        self.assertIsNot(rebuilt.coinbase_manifest, original.coinbase_manifest)
        self.assertIs(rebuilt.template, second.template)
        self.assertEqual(rebuilt.template_generation, second.generation)
    def test_job_bundle_cache_is_bounded(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(base_template())
        assert artifacts is not None
        bundle = server.shared_job_bundle(artifacts, worker())

        for index in range(MAX_PRISM_JOB_BUNDLE_CACHE_ENTRIES + 5):
            candidate = dataclass_replace(
                bundle,
                key=(artifacts.fingerprint, "test", index),
            )
            server._cache_job_bundle_if_current(candidate, artifacts)

        self.assertEqual(
            len(server._ensure_job_bundle_service()._bundle_cache),
            MAX_PRISM_JOB_BUNDLE_CACHE_ENTRIES,
        )
        self.assertNotIn(
            (artifacts.fingerprint, "test", 0),
            server._ensure_job_bundle_service()._bundle_cache,
        )
    def test_job_bundle_cache_preserves_coordinator_lock_order(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(base_template())
        assert artifacts is not None
        bundle = server.shared_job_bundle(artifacts, mode="ready")
        observed_cache_lock = ObservedRLock()
        server._ensure_job_bundle_service()._cache_lock = observed_cache_lock  # type: ignore[assignment]
        errors: list[BaseException] = []

        def cache_bundle() -> None:
            try:
                server._cache_job_bundle_if_current(bundle, artifacts)
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors.append(exc)

        observed_cache_lock.acquire()
        observed_cache_lock.observe_acquires = True
        cache_thread = threading.Thread(target=cache_bundle)
        coordinator_lock_acquired = False
        try:
            cache_thread.start()
            self.assertTrue(observed_cache_lock.acquire_attempted.wait(5))
            coordinator_lock_acquired = server.lock.acquire(timeout=0.25)
            if coordinator_lock_acquired:
                server.lock.release()
        finally:
            observed_cache_lock.release()
            cache_thread.join(5)

        self.assertTrue(coordinator_lock_acquired)
        self.assertFalse(cache_thread.is_alive())
        self.assertEqual(errors, [])
    def test_active_gap_replaces_older_pending_job_build(self) -> None:
        server, rpc = coordinator()
        first = server.store_template_artifacts(dict(rpc.template))
        second_template = dict(rpc.template)
        second_template["coinbasevalue"] = int(second_template["coinbasevalue"]) + 1
        second = server.store_template_artifacts(second_template)
        assert first is not None and second is not None
        payout_generation = server._payout_state_service._generation

        def request_for(artifacts: object) -> object:
            return server._new_job_build_request(
                artifacts,  # type: ignore[arg-type]
                None,
                mode="ready",
                payout_state_generation=payout_generation,
                cache_key=server._job_bundle_key(
                    artifacts,  # type: ignore[arg-type]
                    mode="ready",
                    payout_state_generation=payout_generation,
                    worker=None,
                ),
            )

        pending = request_for(first)
        newest = request_for(second)
        server._ensure_job_bundle_service()._active = None
        server._ensure_job_bundle_service()._retiring = SimpleNamespace(request=pending)
        server._ensure_job_bundle_service()._pending = pending
        server._ensure_job_bundle_service()._start_locked = (  # type: ignore[method-assign]
            lambda request: SimpleNamespace(request=request, future=None)
        )
        server._ensure_job_bundle_service()._arm_locked = lambda _flight: None  # type: ignore[method-assign]

        promise = server._request_job_build(newest)  # type: ignore[arg-type]

        self.assertIs(promise, newest.promise)  # type: ignore[union-attr]
        self.assertIsNone(server._ensure_job_bundle_service()._pending)
        assert server._ensure_job_bundle_service()._active is not None
        self.assertIs(server._ensure_job_bundle_service()._active.request, newest)
        self.assertTrue(pending.promise.done())  # type: ignore[union-attr]
        self.assertIsInstance(
            pending.promise.exception(),  # type: ignore[union-attr]
            JobBuildSuperseded,
        )
    def test_cancelled_ready_does_not_block_collection_promotion(self) -> None:
        for placement in ("active", "retiring"):
            with self.subTest(placement=placement):
                server, rpc = coordinator(ledger=FakeLedger(miners=["miner-a"]))
                artifacts = server.store_template_artifacts(dict(rpc.template))
                assert artifacts is not None
                payout_generation = server._payout_state_service._generation

                def request_for(
                    mode: str,
                    identity: WorkerIdentity | None,
                ) -> object:
                    return server._new_job_build_request(
                        artifacts,
                        identity,
                        mode=mode,
                        payout_state_generation=payout_generation,
                        cache_key=server._job_bundle_key(
                            artifacts,
                            mode=mode,
                            payout_state_generation=payout_generation,
                            worker=identity,
                        ),
                    )

                ready = request_for("ready", None)
                collection = request_for(
                    "collection",
                    worker("tq1collection", "tq1collection.rig"),
                )
                self.assertTrue(  # type: ignore[union-attr]
                    ready.cancellation.cancel("superseded")
                )
                ready_flight = SimpleNamespace(request=ready)
                if placement == "active":
                    server._ensure_job_bundle_service()._active = ready_flight
                    server._ensure_job_bundle_service()._retiring = None
                else:
                    server._ensure_job_bundle_service()._active = None
                    server._ensure_job_bundle_service()._retiring = ready_flight
                server._ensure_job_bundle_service()._pending = collection
                armed: list[object] = []
                server._ensure_job_bundle_service()._start_locked = (  # type: ignore[method-assign]
                    lambda request: SimpleNamespace(request=request, future=None)
                )
                server._ensure_job_bundle_service()._arm_locked = armed.append  # type: ignore[method-assign]

                server._promote_pending_job_build_locked()

                self.assertIsNone(server._ensure_job_bundle_service()._pending)
                assert server._ensure_job_bundle_service()._active is not None
                self.assertIs(server._ensure_job_bundle_service()._active.request, collection)
                self.assertIs(server._ensure_job_bundle_service()._retiring, ready_flight)
                self.assertEqual(armed, [server._ensure_job_bundle_service()._active])
    def test_immediate_collection_completion_does_not_reoccupy_slot(self) -> None:
        server, rpc = coordinator(ledger=FakeLedger(miners=["miner-a"]))
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        payout_generation = server._payout_state_service._generation
        identities = [
            worker(f"tq1worker-{index}", f"tq1worker-{index}.rig")
            for index in range(2)
        ]

        def request_for(identity: WorkerIdentity) -> object:
            return server._new_job_build_request(
                artifacts,
                identity,
                mode="collection",
                payout_state_generation=payout_generation,
                cache_key=server._job_bundle_key(
                    artifacts,
                    mode="collection",
                    payout_state_generation=payout_generation,
                    worker=identity,
                ),
            )

        pending = request_for(identities[0])
        incoming = request_for(identities[1])
        results: dict[int, object] = {}

        def completed_flight(request: object) -> object:
            result = SimpleNamespace(request=request)
            results[id(request)] = result
            future: Future[object] = Future()
            future.set_result(result)
            return SimpleNamespace(request=request, future=future)

        server._ensure_job_bundle_service()._pending = pending
        server._ensure_job_bundle_service()._start_locked = completed_flight  # type: ignore[method-assign]

        promise = server._request_job_build(incoming)  # type: ignore[arg-type]

        self.assertIs(promise.result(), results[id(incoming)])
        self.assertIs(  # type: ignore[union-attr]
            pending.promise.result(),
            results[id(pending)],
        )
        self.assertIsNone(server._ensure_job_bundle_service()._active)
        self.assertIsNone(server._ensure_job_bundle_service()._retiring)
        self.assertIsNone(server._ensure_job_bundle_service()._pending)
    def test_independent_collection_workers_do_not_supersede_each_other(self) -> None:
        server, rpc = coordinator(ledger=FakeLedger(miners=["miner-a"]))
        recorded = install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        identities = [
            worker(f"tq1worker-{index}", f"tq1worker-{index}.rig")
            for index in range(4)
        ]
        entered = [threading.Event() for _identity in identities]
        releases = [threading.Event() for _identity in identities]
        service = server._ensure_job_bundle_service()
        original_build = service.build_shared_job_bundle
        active_builds = 0
        max_active_builds = 0
        active_lock = threading.Lock()

        def blocking_build(
            build_artifacts: object,
            identity: WorkerIdentity,
            **kwargs: object,
        ) -> object:
            nonlocal active_builds, max_active_builds
            index = identities.index(identity)
            with active_lock:
                active_builds += 1
                max_active_builds = max(max_active_builds, active_builds)
            entered[index].set()
            try:
                self.assertTrue(releases[index].wait(5))
                return original_build(
                    build_artifacts,  # type: ignore[arg-type]
                    identity,
                    **kwargs,
                )
            finally:
                with active_lock:
                    active_builds -= 1

        service.build_shared_job_bundle = blocking_build  # type: ignore[method-assign]
        results: list[list[object]] = [[] for _identity in identities]
        errors: list[list[BaseException]] = [[] for _identity in identities]

        def build(index: int) -> None:
            try:
                results[index].append(
                    server.shared_job_bundle(
                        artifacts,
                        identities[index],
                        mode="collection",
                    )
                )
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors[index].append(exc)

        threads = [
            threading.Thread(target=build, args=(index,))
            for index in range(len(identities))
        ]
        threads[0].start()
        try:
            self.assertTrue(entered[0].wait(5))
            threads[1].start()
            self.assertTrue(entered[1].wait(5))
            threads[2].start()
            pending_deadline = time.monotonic() + 5
            while time.monotonic() < pending_deadline:
                with server._ensure_job_bundle_service()._scheduler_lock:
                    pending = server._ensure_job_bundle_service()._pending
                    if pending is not None and pending.worker == identities[2]:
                        break
                time.sleep(0.01)
            else:
                self.fail("third collection build was not queued")
            threads[3].start()
            self.assertEqual(server._ensure_job_bundle_service()._scheduler_counts["starts"], 2)
            releases[0].set()
            self.assertTrue(entered[2].wait(5))
            releases[1].set()
            self.assertTrue(entered[3].wait(5))
            self.assertEqual(server._ensure_job_bundle_service()._scheduler_counts["starts"], 4)
        finally:
            for release in releases:
                release.set()
            for thread in threads:
                thread.join(5)

        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(errors, [[], [], [], []])
        self.assertEqual([len(items) for items in results], [1, 1, 1, 1])
        self.assertEqual(recorded["calls"], 4)
        self.assertLessEqual(max_active_builds, 2)
        self.assertEqual(server._ensure_job_bundle_service()._scheduler_counts["supersessions"], 0)
    def test_collection_independence_requires_one_immutable_cohort(self) -> None:
        server, rpc = coordinator(ledger=FakeLedger(miners=["miner-a"]))
        first = server.store_template_artifacts(dict(rpc.template))
        assert first is not None
        second_template = dict(rpc.template)
        second_template["curtime"] = int(second_template["curtime"]) + 1
        second = server.store_template_artifacts(second_template)
        assert second is not None
        payout_generation = server._payout_state_service._generation
        first_worker = worker("tq1worker-1", "tq1worker-1.rig")
        second_worker = worker("tq1worker-2", "tq1worker-2.rig")

        def request_for(
            build_artifacts: object,
            identity: WorkerIdentity,
        ) -> object:
            return server._new_job_build_request(
                build_artifacts,  # type: ignore[arg-type]
                identity,
                mode="collection",
                payout_state_generation=payout_generation,
                cache_key=server._job_bundle_key(
                    build_artifacts,  # type: ignore[arg-type]
                    mode="collection",
                    payout_state_generation=payout_generation,
                    worker=identity,
                ),
            )

        first_request = request_for(first, first_worker)
        peer_request = request_for(first, second_worker)
        newer_request = request_for(second, first_worker)

        self.assertTrue(
            server._collection_job_builds_are_independent(
                first_request,  # type: ignore[arg-type]
                peer_request,  # type: ignore[arg-type]
            )
        )
        self.assertFalse(
            server._collection_job_builds_are_independent(
                first_request,  # type: ignore[arg-type]
                newer_request,  # type: ignore[arg-type]
            )
        )
    def test_ready_build_cancels_both_live_collection_flights(self) -> None:
        server, rpc = coordinator(ledger=FakeLedger(miners=["miner-a"]))
        server._ensure_tip_refresh_state()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        identities = [
            worker(f"tq1worker-{index}", f"tq1worker-{index}.rig")
            for index in range(2)
        ]
        entered = [threading.Event(), threading.Event()]
        cancellation_observed = [threading.Event(), threading.Event()]
        release_cancelled = threading.Event()
        service = server._ensure_job_bundle_service()
        original_build = service.build_shared_job_bundle

        def blocking_build(
            build_artifacts: object,
            identity: WorkerIdentity | None,
            **kwargs: object,
        ) -> object:
            request = kwargs["build_request"]
            if identity in identities:
                index = identities.index(identity)
                entered[index].set()
                while not request.cancellation.is_set():  # type: ignore[union-attr]
                    if release_cancelled.wait(0.01):
                        raise AssertionError("collection build was not cancelled")
                cancellation_observed[index].set()
                release_cancelled.wait(5)
                request.cancellation.raise_if_cancelled(  # type: ignore[union-attr]
                    "test collection hold"
                )
            return original_build(
                build_artifacts,  # type: ignore[arg-type]
                identity,
                **kwargs,
            )

        service.build_shared_job_bundle = blocking_build  # type: ignore[method-assign]
        payout_generation = server._payout_state_service._generation

        def request_for(
            mode: str,
            identity: WorkerIdentity | None,
        ) -> object:
            return server._new_job_build_request(
                artifacts,
                identity,
                mode=mode,
                payout_state_generation=payout_generation,
                cache_key=server._job_bundle_key(
                    artifacts,
                    mode=mode,
                    payout_state_generation=payout_generation,
                    worker=identity,
                ),
            )

        collection_requests = [
            request_for("collection", identity) for identity in identities
        ]
        collection_promises = []
        ready_promise = None
        try:
            collection_promises.append(
                server._request_job_build(collection_requests[0])  # type: ignore[arg-type]
            )
            self.assertTrue(entered[0].wait(5))
            collection_promises.append(
                server._request_job_build(collection_requests[1])  # type: ignore[arg-type]
            )
            self.assertTrue(entered[1].wait(5))

            ready_request = request_for("ready", None)
            ready_promise = server._request_job_build(  # type: ignore[arg-type]
                ready_request
            )
            self.assertTrue(cancellation_observed[0].wait(5))
            self.assertTrue(cancellation_observed[1].wait(5))
            self.assertFalse(collection_promises[0].done())
            self.assertFalse(collection_promises[1].done())
            self.assertFalse(ready_promise.done())
            self.assertEqual(server._ensure_job_bundle_service()._scheduler_counts["starts"], 2)
        finally:
            release_cancelled.set()

        assert ready_promise is not None
        ready_bundle = ready_promise.result(timeout=5)
        self.assertFalse(ready_bundle.collection_only)
        for request, promise in zip(collection_requests, collection_promises):
            self.assertTrue(request.cancellation.is_set())  # type: ignore[union-attr]
            self.assertIsInstance(
                promise.exception(timeout=5),
                JobBuildSuperseded,
            )
        self.assertEqual(server._ensure_job_bundle_service()._scheduler_counts["starts"], 3)
    def test_ready_build_cancels_retiring_only_collection_flight(self) -> None:
        server, rpc = coordinator(ledger=FakeLedger(miners=["miner-a"]))
        server._ensure_tip_refresh_state()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        identities = [
            worker(f"tq1worker-{index}", f"tq1worker-{index}.rig")
            for index in range(2)
        ]
        entered = [threading.Event(), threading.Event()]
        release_active = threading.Event()
        retiring_cancelled = threading.Event()
        release_retiring = threading.Event()
        service = server._ensure_job_bundle_service()
        original_build = service.build_shared_job_bundle

        def blocking_build(
            build_artifacts: object,
            identity: WorkerIdentity | None,
            **kwargs: object,
        ) -> object:
            request = kwargs["build_request"]
            if identity == identities[0]:
                entered[0].set()
                while not request.cancellation.is_set():  # type: ignore[union-attr]
                    if release_retiring.wait(0.01):
                        raise AssertionError("retiring build was not cancelled")
                retiring_cancelled.set()
                release_retiring.wait(5)
                request.cancellation.raise_if_cancelled(  # type: ignore[union-attr]
                    "test retiring-only hold"
                )
            elif identity == identities[1]:
                entered[1].set()
                self.assertTrue(release_active.wait(5))
            return original_build(
                build_artifacts,  # type: ignore[arg-type]
                identity,
                **kwargs,
            )

        service.build_shared_job_bundle = blocking_build  # type: ignore[method-assign]
        payout_generation = server._payout_state_service._generation

        def request_for(
            mode: str,
            identity: WorkerIdentity | None,
        ) -> object:
            return server._new_job_build_request(
                artifacts,
                identity,
                mode=mode,
                payout_state_generation=payout_generation,
                cache_key=server._job_bundle_key(
                    artifacts,
                    mode=mode,
                    payout_state_generation=payout_generation,
                    worker=identity,
                ),
            )

        first_request = request_for("collection", identities[0])
        second_request = request_for("collection", identities[1])
        first_promise = server._request_job_build(  # type: ignore[arg-type]
            first_request
        )
        self.assertTrue(entered[0].wait(5))
        second_promise = server._request_job_build(  # type: ignore[arg-type]
            second_request
        )
        self.assertTrue(entered[1].wait(5))
        try:
            release_active.set()
            second_bundle = second_promise.result(timeout=5)
            self.assertTrue(second_bundle.collection_only)
            with server._ensure_job_bundle_service()._scheduler_lock:
                self.assertIsNone(server._ensure_job_bundle_service()._active)
                assert server._ensure_job_bundle_service()._retiring is not None
                self.assertIs(
                    server._ensure_job_bundle_service()._retiring.request,
                    first_request,
                )

            ready_request = request_for("ready", None)
            ready_promise = server._request_job_build(  # type: ignore[arg-type]
                ready_request
            )
            self.assertTrue(retiring_cancelled.wait(5))
            ready_bundle = ready_promise.result(timeout=5)
            self.assertFalse(ready_bundle.collection_only)
            self.assertTrue(first_request.cancellation.is_set())  # type: ignore[union-attr]
        finally:
            release_active.set()
            release_retiring.set()

        self.assertIsInstance(
            first_promise.exception(timeout=5),
            JobBuildSuperseded,
        )
    def test_collection_retries_do_not_supersede_ready_build(self) -> None:
        server, rpc = coordinator(ledger=FakeLedger(miners=["miner-a"]))
        server._ensure_tip_refresh_state()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        identities = [
            worker(f"tq1worker-{index}", f"tq1worker-{index}.rig")
            for index in range(2)
        ]
        collection_entered = [threading.Event(), threading.Event()]
        collection_cancelled = [threading.Event(), threading.Event()]
        release_cancelled = threading.Event()
        ready_entered = threading.Event()
        release_ready = threading.Event()
        stop_collections = threading.Event()
        ready_requests: list[object] = []
        collection_requests: list[object | None] = [None, None]
        service = server._ensure_job_bundle_service()
        original_build = service.build_shared_job_bundle

        def blocking_build(
            build_artifacts: object,
            identity: WorkerIdentity | None,
            **kwargs: object,
        ) -> object:
            request = kwargs["build_request"]
            if request.mode == "ready":  # type: ignore[union-attr]
                ready_requests.append(request)
                ready_entered.set()
                self.assertTrue(release_ready.wait(5))
            elif identity in identities:
                index = identities.index(identity)
                collection_requests[index] = request
                collection_entered[index].set()
                while not request.cancellation.is_set():  # type: ignore[union-attr]
                    if release_cancelled.wait(0.01):
                        raise AssertionError("collection build was not cancelled")
                collection_cancelled[index].set()
                release_cancelled.wait(5)
                request.cancellation.raise_if_cancelled(  # type: ignore[union-attr]
                    "test collection retry hold"
                )
            return original_build(
                build_artifacts,  # type: ignore[arg-type]
                identity,
                **kwargs,
            )

        service.build_shared_job_bundle = blocking_build  # type: ignore[method-assign]
        collection_results: list[list[object]] = [[], []]
        collection_errors: list[list[BaseException]] = [[], []]
        ready_results: list[object] = []
        ready_errors: list[BaseException] = []

        def build_collection(index: int) -> None:
            try:
                collection_results[index].append(
                    server.shared_job_bundle(
                        artifacts,
                        identities[index],
                        mode="collection",
                        cancelled=stop_collections.is_set,
                    )
                )
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                collection_errors[index].append(exc)

        def build_ready() -> None:
            try:
                ready_results.append(
                    server.shared_job_bundle(
                        artifacts,
                        mode="ready",
                        retry_superseded=False,
                    )
                )
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                ready_errors.append(exc)

        collection_threads = [
            threading.Thread(target=build_collection, args=(index,))
            for index in range(2)
        ]
        ready_thread = threading.Thread(target=build_ready)
        try:
            collection_threads[0].start()
            self.assertTrue(collection_entered[0].wait(5))
            collection_threads[1].start()
            self.assertTrue(collection_entered[1].wait(5))
            ready_thread.start()
            self.assertTrue(collection_cancelled[0].wait(5))
            self.assertTrue(collection_cancelled[1].wait(5))
            self.assertEqual(server._ensure_job_bundle_service()._scheduler_counts["starts"], 2)

            release_cancelled.set()
            self.assertTrue(ready_entered.wait(5))
            retry_deadline = time.monotonic() + 5
            while (
                server._ensure_job_bundle_service()._scheduler_counts["requests"] < 5
                and time.monotonic() < retry_deadline
            ):
                time.sleep(0.01)
            self.assertGreaterEqual(
                server._ensure_job_bundle_service()._scheduler_counts["requests"],
                5,
            )
            self.assertEqual(server._ensure_job_bundle_service()._scheduler_counts["starts"], 3)
            self.assertEqual(len(ready_requests), 1)
            self.assertFalse(
                ready_requests[0].cancellation.is_set()  # type: ignore[union-attr]
            )

            stop_collections.set()
            for thread in collection_threads:
                thread.join(2)
            release_ready.set()
            ready_thread.join(5)
        finally:
            stop_collections.set()
            release_cancelled.set()
            release_ready.set()
            for thread in collection_threads:
                if thread.ident is not None:
                    thread.join(5)
            if ready_thread.ident is not None:
                ready_thread.join(5)

        self.assertTrue(all(not thread.is_alive() for thread in collection_threads))
        self.assertFalse(ready_thread.is_alive())
        self.assertEqual(collection_results, [[], []])
        self.assertEqual([len(errors) for errors in collection_errors], [1, 1])
        self.assertEqual(ready_errors, [])
        self.assertEqual(len(ready_results), 1)
        self.assertFalse(ready_results[0].collection_only)  # type: ignore[union-attr]
        for request in collection_requests:
            assert request is not None
            self.assertTrue(request.cancellation.is_set())  # type: ignore[union-attr]
    def test_shutdown_cancels_builder_with_full_helper_input_pipe(self) -> None:
        server, rpc = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        payout_generation = server._payout_state_service._generation
        request = server._new_job_build_request(
            artifacts,
            None,
            mode="ready",
            payout_state_generation=payout_generation,
            cache_key=server._job_bundle_key(
                artifacts,
                mode="ready",
                payout_state_generation=payout_generation,
                worker=None,
            ),
        )
        helper_started = threading.Event()
        helper_processes: list[subprocess.Popen[str]] = []
        real_popen = subprocess.Popen

        def capture_popen(*args: object, **kwargs: object) -> subprocess.Popen[str]:
            process = real_popen(*args, **kwargs)  # type: ignore[arg-type]
            helper_processes.append(process)
            helper_started.set()
            return process

        def fill_helper_pipe(*_args: object, **kwargs: object) -> object:
            build_request = kwargs["build_request"]
            return server.build_audit_bundle(
                shares=[],
                found_block={
                    "block_height": 10,
                    "coinbase_value_sats": 50_00000000,
                    "network_difficulty": 1,
                    "anchor_job_issued_at_ms": 1_700_000_000_000,
                },
                prior_balances=[
                    {
                        "miner_id": "pipe-filler",
                        "balance_sats": 1,
                        "padding": "x" * (4 * 1024 * 1024),
                    }
                ],
                coinbase_script_sig_suffix_hex="00",
                cancellation=build_request.cancellation,  # type: ignore[union-attr]
            )

        server._ensure_job_bundle_service().build_shared_job_bundle = fill_helper_pipe  # type: ignore[method-assign]
        shutdown_finished = threading.Event()
        shutdown_errors: list[BaseException] = []

        def shutdown() -> None:
            try:
                server.shutdown_job_build_executor()
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                shutdown_errors.append(exc)
            finally:
                shutdown_finished.set()

        with patch(
            "lab.prism.bundle_compiler.prism_tool_command",
            return_value=[sys.executable, "-c", "import time; time.sleep(30)"],
        ), patch(
            "lab.prism.bundle_compiler.subprocess.Popen",
            side_effect=capture_popen,
        ):
            promise = server._request_job_build(request)
            self.assertTrue(
                helper_started.wait(5),
                repr(promise.exception(timeout=1)) if promise.done() else None,
            )
            time.sleep(0.1)
            self.assertFalse(promise.done())
            shutdown_thread = threading.Thread(target=shutdown)
            shutdown_thread.start()
            shutdown_returned = shutdown_finished.wait(2)
            if not shutdown_returned:
                for process in helper_processes:
                    if process.poll() is None:
                        process.kill()
            shutdown_thread.join(5)

        self.assertTrue(shutdown_returned)
        self.assertFalse(shutdown_thread.is_alive())
        self.assertEqual(shutdown_errors, [])
        self.assertIsInstance(promise.exception(timeout=1), JobBuildSuperseded)
    def test_control_cancel_during_serialization_is_supersession(self) -> None:
        server, rpc = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        helper_started = threading.Event()
        helper_processes: list[subprocess.Popen[str]] = []
        real_popen = subprocess.Popen

        def capture_popen(*args: object, **kwargs: object) -> subprocess.Popen[str]:
            process = real_popen(*args, **kwargs)  # type: ignore[arg-type]
            helper_processes.append(process)
            helper_started.set()
            return process

        def fill_helper_pipe(*_args: object, **kwargs: object) -> object:
            build_request = kwargs["build_request"]
            return server.build_audit_bundle(
                shares=[],
                found_block={
                    "block_height": 10,
                    "coinbase_value_sats": 50_00000000,
                    "network_difficulty": 1,
                    "anchor_job_issued_at_ms": 1_700_000_000_000,
                },
                prior_balances=[
                    {
                        "miner_id": "pipe-filler",
                        "balance_sats": 1,
                        "padding": "x" * (4 * 1024 * 1024),
                    }
                ],
                coinbase_script_sig_suffix_hex="00",
                cancellation=build_request.cancellation,  # type: ignore[union-attr]
            )

        server._ensure_job_bundle_service().build_shared_job_bundle = fill_helper_pipe  # type: ignore[method-assign]
        errors: list[BaseException] = []

        def build() -> None:
            try:
                server.shared_job_bundle(
                    artifacts,
                    mode="ready",
                    retry_superseded=False,
                )
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors.append(exc)

        with patch(
            "lab.prism.bundle_compiler.prism_tool_command",
            return_value=[sys.executable, "-c", "import time; time.sleep(30)"],
        ), patch(
            "lab.prism.bundle_compiler.subprocess.Popen",
            side_effect=capture_popen,
        ):
            build_thread = threading.Thread(target=build)
            build_thread.start()
            self.assertTrue(helper_started.wait(5))
            with server._ensure_job_bundle_service()._cache_lock:
                controls = list(server._ensure_job_bundle_service()._active_bundle_builds.values())
            self.assertEqual(len(controls), 1)
            controls[0].cancel_event.set()
            build_thread.join(2)
            if build_thread.is_alive():
                for process in helper_processes:
                    if process.poll() is None:
                        process.kill()
                build_thread.join(5)

        self.assertFalse(build_thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], JobBuildSuperseded)
        build_counts = server._ensure_job_bundle_service().shared_preparation_metrics()[
            "build_counts"
        ]
        self.assertEqual(build_counts["superseded"], 1)
        self.assertEqual(build_counts["failed"], 0)
        self.assertEqual(server._ensure_tip_refresh_service().metrics_snapshot()["superseded_results"], 1)

    def test_externally_terminated_superseded_builder_is_not_a_crash(self) -> None:
        server, rpc = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        real_popen = subprocess.Popen

        def build_with_control(*_args: object, **kwargs: object) -> object:
            build_request = kwargs["build_request"]
            return server.build_audit_bundle(
                shares=[],
                found_block={
                    "block_height": 10,
                    "coinbase_value_sats": 50_00000000,
                    "network_difficulty": 1,
                    "anchor_job_issued_at_ms": 1_700_000_000_000,
                },
                prior_balances=[],
                coinbase_script_sig_suffix_hex="00",
                cancellation=build_request.cancellation,  # type: ignore[union-attr]
            )

        server._ensure_job_bundle_service().build_shared_job_bundle = (  # type: ignore[method-assign]
            build_with_control
        )

        def cancel_then_exit(
            *args: object,
            **kwargs: object,
        ) -> subprocess.Popen[str]:
            process = real_popen(*args, **kwargs)  # type: ignore[arg-type]
            original_poll = process.poll
            original_wait = process.wait
            first_poll = True

            def poll() -> int | None:
                nonlocal first_poll
                if not first_poll:
                    return original_poll()
                first_poll = False
                service = server._ensure_job_bundle_service()
                with service._cache_lock:
                    controls = list(service._active_bundle_builds.values())
                self.assertEqual(len(controls), 1)
                controls[0].cancel_event.set()
                process.kill()
                return original_wait()

            process.poll = poll  # type: ignore[method-assign]
            return process

        with patch(
            "lab.prism.bundle_compiler.prism_tool_command",
            return_value=[sys.executable, "-c", "import time; time.sleep(30)"],
        ), patch(
            "lab.prism.bundle_compiler.subprocess.Popen",
            side_effect=cancel_then_exit,
        ):
            with self.assertRaises(JobBuildSuperseded):
                server.shared_job_bundle(
                    artifacts,
                    mode="ready",
                    retry_superseded=False,
                )

        metrics = server._ensure_job_bundle_service().shared_preparation_metrics()
        self.assertEqual(metrics["build_counts"]["superseded"], 1)
        self.assertEqual(metrics["build_counts"]["failed"], 0)
        worker_counts = server._ensure_job_bundle_service().metrics_snapshot()[
            "worker_counts"
        ]
        self.assertEqual(worker_counts["crashes"], 0)

    def test_full_helper_input_pipe_obeys_builder_timeout(self) -> None:
        server, _rpc = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        server.bundle_build_timeout_seconds = 0.05
        helper_started = threading.Event()
        helper_processes: list[subprocess.Popen[str]] = []
        real_popen = subprocess.Popen

        def capture_popen(*args: object, **kwargs: object) -> subprocess.Popen[str]:
            process = real_popen(*args, **kwargs)  # type: ignore[arg-type]
            helper_processes.append(process)
            helper_started.set()
            return process

        errors: list[BaseException] = []

        def build() -> None:
            try:
                server.build_audit_bundle(
                    shares=[],
                    found_block={
                        "block_height": 10,
                        "coinbase_value_sats": 50_00000000,
                        "network_difficulty": 1,
                        "anchor_job_issued_at_ms": 1_700_000_000_000,
                    },
                    prior_balances=[
                        {
                            "miner_id": "pipe-filler",
                            "balance_sats": 1,
                            "padding": "x" * (4 * 1024 * 1024),
                        }
                    ],
                    coinbase_script_sig_suffix_hex="00",
                )
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors.append(exc)

        with patch(
            "lab.prism.bundle_compiler.prism_tool_command",
            return_value=[sys.executable, "-c", "import time; time.sleep(30)"],
        ), patch(
            "lab.prism.bundle_compiler.subprocess.Popen",
            side_effect=capture_popen,
        ):
            build_thread = threading.Thread(target=build)
            build_thread.start()
            self.assertTrue(helper_started.wait(5))
            build_thread.join(2)
            if build_thread.is_alive():
                for process in helper_processes:
                    if process.poll() is None:
                        process.kill()
                build_thread.join(5)

        self.assertFalse(build_thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], RuntimeError)
        self.assertIn("timed out", str(errors[0]))
