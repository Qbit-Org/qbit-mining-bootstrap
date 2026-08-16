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
        with server._job_cache_lock:
            server._job_build_issued_at_ms.clear()
        rebuilt = server.build_shared_job_bundle(
            server.current_template_artifacts(),
            worker(),
        )
        self.assertGreaterEqual(ledger.anchors[-1], stamped_ms)
        self.assertGreaterEqual(
            int(rebuilt.found_block["anchor_job_issued_at_ms"]), stamped_ms
        )

    def test_payout_artifact_declares_its_own_snapshot_anchor(self) -> None:
        # An artifact snapshot is taken at its own earlier anchor. A bundle
        # reusing the artifact must declare that anchor rather than the
        # fresher job-issue time: a share stamped between the two anchors is
        # excluded from the artifact by construction, yet a re-derivation at
        # the job-issue anchor would include it.
        ledger = AnchorRecordingLedger()
        server, _rpc = coordinator(ledger=ledger)
        install_fake_bundle_builder(server)
        artifacts = server.current_template_artifacts()
        clamp_now_ms = now_ms() - 6
        # Anchor selection sits strictly below the clamp instant so a share
        # stamped in the same millisecond can never tie the anchor.
        artifact_anchor_ms = clamp_now_ms - 1
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            return_value=clamp_now_ms,
        ):
            artifact = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
        assert artifact is not None
        self.assertEqual(artifact.snapshot_anchor_ms, artifact_anchor_ms)
        self.assertEqual(ledger.anchors[-1], artifact_anchor_ms)

        # Construction re-validates that the passed artifact is the installed
        # current one.
        with server._job_cache_lock:
            server._payout_ledger_artifact = artifact
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

    def test_artifact_paths_proceed_at_the_clamped_anchor_floor(
        self,
    ) -> None:
        # The pending-commit clamp is anchor selection, not a fence: every
        # share stamped at or below the clamped anchor is already durable,
        # so the window read at that anchor is exact and reproducible, and
        # both artifact producers publish it. Shares stamped above the
        # anchor deterministically belong to the next window.
        ledger = AnchorRecordingLedger()
        server, _rpc = coordinator(ledger=ledger)
        install_fake_bundle_builder(server)
        artifacts = server.current_template_artifacts()
        stamped_ms = now_ms() - 5
        share = stamped_pending_share(stamped_ms)
        self._hold_floor(server, share)
        try:
            artifact = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            self.assertIsNotNone(artifact)
            assert artifact is not None
            self.assertEqual(artifact.snapshot_anchor_ms, stamped_ms - 1)
            self.assertEqual(ledger.anchors[-1], stamped_ms - 1)

            # The synchronous build proceeds at the clamped anchor and seeds
            # its window for reuse under that same anchor; cache publication
            # installs the seed.
            bundle = server.build_shared_job_bundle(artifacts, worker())
            self.assertEqual(bundle.issued_at_ms, stamped_ms - 1)
            seeded = bundle.prepared_ledger_artifact
            self.assertIsNotNone(seeded)
            assert seeded is not None
            self.assertEqual(seeded.snapshot_anchor_ms, stamped_ms - 1)
            server._install_payout_ledger_artifact(seeded)
            with server._job_cache_lock:
                published = server._payout_ledger_artifact
            self.assertIsNotNone(published)
            assert published is not None
            self.assertEqual(published.snapshot_anchor_ms, stamped_ms - 1)
        finally:
            server._finish_pending_share_commit(share)

    def test_background_build_refuses_a_pathologically_old_floor(self) -> None:
        # A floor held further below now than the audit anchor ceiling (a
        # wedged writer or leaked release) would arm an artifact that is
        # born expired; refuse before paying the window walk so the
        # re-arm backoff paces retries.
        ledger = AnchorRecordingLedger()
        server, _rpc = coordinator(ledger=ledger)
        install_fake_bundle_builder(server)
        artifacts = server.current_template_artifacts()
        share = stamped_pending_share(now_ms() - 400_000)
        self._hold_floor(server, share)
        try:
            self.assertIsNone(
                server._build_payout_ledger_artifact(
                    0, 0, artifacts.network_difficulty
                )
            )
            self.assertEqual(ledger.snapshot_calls, 0)
        finally:
            server._finish_pending_share_commit(share)

    def test_anchor_selection_excludes_same_millisecond_stamps(self) -> None:
        # Millisecond granularity can hand a share stamped right after
        # anchor selection the same accepted_at_ms as the clamp instant,
        # and such a share is never protected by the pending floor. The
        # window predicate is anchor-inclusive, so the anchor must sit
        # strictly below the clamp-time millisecond or that share would
        # join audit replays of a window that never contained it.
        server, _rpc = coordinator(ledger=AnchorRecordingLedger())
        clamp_now = now_ms()
        self.assertEqual(
            server._job_snapshot_anchor_ms(clamp_now),
            clamp_now - 1,
        )

        share = stamped_pending_share(clamp_now - 7)
        self._hold_floor(server, share)
        try:
            self.assertEqual(
                server._job_snapshot_anchor_ms(clamp_now),
                clamp_now - 8,
            )
        finally:
            server._finish_pending_share_commit(share)


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
        cached = next(iter(server._job_bundle_cache.values()))
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
            {entry.template_fingerprint for entry in server._job_bundle_cache.values()},
            {qbit_template_fingerprint(new_template)},
        )

    def test_bundle_cache_ttl_expiry_rebuilds(self) -> None:
        server, _ = coordinator()
        recorded = install_fake_bundle_builder(server)
        server.job_bundle_cache_seconds = 0.05

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
        server._job_bundle_cache[expired_key] = expired

        looked_up = server._lookup_job_bundle(current.key)

        self.assertIs(looked_up, current)
        self.assertNotIn(expired_key, server._job_bundle_cache)
        self.assertEqual(list(server._job_bundle_cache.values()), [current])

    def test_zero_ttl_disables_bundle_cache(self) -> None:
        server, _ = coordinator()
        recorded = install_fake_bundle_builder(server)
        server.job_bundle_cache_seconds = 0.0

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
        original_build = server.build_shared_job_bundle
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

        server.build_shared_job_bundle = mutate_after_first_build  # type: ignore[method-assign]

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
        self.assertEqual(server._payout_state_generation, 1)
        self.assertTrue(server._tip_refresh_retry.is_set())

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
        original_lock = server._job_cache_lock

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
                    server._payout_state_generation = 1

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

        server._job_cache_lock = PublishAfterPrioritySnapshot()  # type: ignore[assignment]
        server._payout_state_delivery_gate = RecordingGate()  # type: ignore[assignment]

        self.assertFalse(server.maybe_send_job(state, clean_jobs=True))
        self.assertEqual(priorities, [True])
        self.assertEqual(server._payout_state_generation, 1)

    def test_zero_template_ttl_fetches_template_per_build(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.template_cache_seconds = 0.0

        server.build_job_for_client(client(1), clean_jobs=True)
        server.build_job_for_client(client(2), clean_jobs=True)

        self.assertEqual(rpc.count("getblocktemplate"), 2)

    def test_late_stale_template_fetch_cannot_replace_newer_artifacts(self) -> None:
        server, rpc = coordinator()
        server.template_cache_seconds = 0.0
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
        self.assertIs(server._template_artifacts, current_artifacts)
        self.assertEqual(
            current_artifacts.fingerprint,
            qbit_template_fingerprint(current_template),
        )

    def test_collection_mode_bundles_are_keyed_per_worker(self) -> None:
        server, _ = coordinator(ledger=FakeLedger(miners=["solo"]))
        recorded = install_fake_bundle_builder(server)
        server.min_ready_miners = 3

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
            server._note_reorg_reconcile_outcome(tip_hash, trusted=True)
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
        server._note_reorg_reconcile_outcome(rpc.tip, trusted=True)
        reconcile_calls: list[str | None] = []

        def fake_reconcile(*, tip_hash: str | None = None) -> dict[str, object]:
            reconcile_calls.append(tip_hash)
            server._note_reorg_reconcile_outcome(tip_hash, trusted=False)
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
            mutation_acquired = server._payout_state_prepare_lock.acquire(
                blocking=False
            )
            if mutation_acquired:
                server._payout_state_prepare_lock.release()
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
        payout_generation = server._payout_state_generation

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
            with server._job_cache_lock:
                server._job_bundle_cache.clear()
            server._prepare_payout_ledger_artifact(
                server._payout_state_generation,
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
        server._pool_ready_latched = True
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

        artifact = server._payout_ledger_artifact
        self.assertIsNotNone(artifact)
        assert artifact is not None
        self.assertEqual(artifact.prior_balances, tuple(preview))
        self.assertEqual(
            artifact.payout_state_generation,
            server._payout_state_generation,
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

        payout_generation = server._payout_state_generation
        ledger.database_balances = [dict(balance) for balance in preview]
        server._clear_accepted_block_payout_preview(parent_hash)
        self.assertEqual(server._payout_state_generation, payout_generation)
        self.assertIs(server._payout_ledger_artifact, artifact)
        self.assertNotIn(parent_hash, server._accepted_block_payout_previews)
        self.assertNotIn(
            parent_hash,
            server._invalidated_accepted_block_payout_previews,
        )
        with server._job_cache_lock:
            server._job_bundle_cache.clear()

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
            server._payout_state_generation,
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
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        with server._job_cache_lock:
            published = server._published_payout_state
            assert published.artifact is not None
            changed_balances = [{"miner_id": "miner-z", "balance_sats": 1}]
            server._published_payout_state = dataclass_replace(
                published,
                artifact=dataclass_replace(
                    published.artifact,
                    prior_balances_json=canonical_json_text(changed_balances),
                    prior_balances_sha256=canonical_json_sha256(changed_balances),
                ),
            )

        self.assertIsNone(
            server._usable_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
        )
        self.assertIsNone(server._payout_ledger_artifact)
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
            control = server._job_build_phase_local.bundle_build_control
            build_started.set()
            self.assertTrue(control.cancel_event.wait(2))
            return original_builder(**kwargs)

        server.build_audit_bundle = cancelable_builder  # type: ignore[method-assign]
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
        self.assertEqual(server._active_job_bundle_builds, {})
        self.assertEqual(server.tip_refresh_build_inflight, 0)
        self.assertFalse(any(
            entry.template_fingerprint == artifacts.fingerprint
            for entry in server._job_bundle_cache.values()
        ))
        self.assertEqual(server.tip_refresh_superseded_results, 1)

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
            "lab.prism.prism_coordinator.prism_tool_command",
            return_value=[sys.executable, "-c", "raise SystemExit(7)"],
        ):
            with self.assertRaisesRegex(RuntimeError, "failed"):
                server.build_audit_bundle(**build_kwargs)

        server.bundle_build_timeout_seconds = 0.01
        with patch(
            "lab.prism.prism_coordinator.prism_tool_command",
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
            "lab.prism.prism_coordinator.prism_tool_command",
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
                "lab.prism.prism_coordinator.prism_tool_command",
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
            control = server._job_build_phase_local.bundle_build_control
            starts.put(None)
            self.assertTrue(control.cancel_event.wait(2))
            return original_builder(**kwargs)

        server.build_audit_bundle = cancelable_builder  # type: ignore[method-assign]
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
        self.assertEqual(server._active_job_bundle_builds, {})
        self.assertEqual(server.tip_refresh_build_inflight, 0)
        self.assertEqual(server.tip_refresh_build_queue_depth, 0)
        self.assertLessEqual(
            len(server._job_bundle_cache),
            MAX_PRISM_JOB_BUNDLE_CACHE_ENTRIES,
        )
        self.assertEqual(server.tip_refresh_superseded_results, 8)

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
        original_build = server.build_shared_job_bundle
        build_calls = 0
        build_calls_lock = threading.Lock()

        def blocking_build(*args: object, **kwargs: object) -> object:
            nonlocal build_calls
            with build_calls_lock:
                build_calls += 1
            build_entered.set()
            self.assertTrue(release_build.wait(5))
            return original_build(*args, **kwargs)  # type: ignore[arg-type]

        server.build_shared_job_bundle = blocking_build  # type: ignore[method-assign]
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
            len(server._job_bundle_cache),
            MAX_PRISM_JOB_BUNDLE_CACHE_ENTRIES,
        )
        self.assertNotIn(
            (artifacts.fingerprint, "test", 0),
            server._job_bundle_cache,
        )

    def test_job_bundle_cache_preserves_coordinator_lock_order(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(base_template())
        assert artifacts is not None
        bundle = server.shared_job_bundle(artifacts, mode="ready")
        observed_cache_lock = ObservedRLock()
        server._job_cache_lock = observed_cache_lock  # type: ignore[assignment]
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
        payout_generation = server._payout_state_generation

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
        server._job_build_active = None
        server._job_build_retiring = SimpleNamespace(request=pending)
        server._job_build_pending = pending
        server._start_job_build_locked = (  # type: ignore[method-assign]
            lambda request: SimpleNamespace(request=request, future=None)
        )
        server._arm_job_build_locked = lambda _flight: None  # type: ignore[method-assign]

        promise = server._request_job_build(newest)  # type: ignore[arg-type]

        self.assertIs(promise, newest.promise)  # type: ignore[union-attr]
        self.assertIsNone(server._job_build_pending)
        assert server._job_build_active is not None
        self.assertIs(server._job_build_active.request, newest)
        self.assertTrue(pending.promise.done())  # type: ignore[union-attr]
        self.assertIsInstance(
            pending.promise.exception(),  # type: ignore[union-attr]
            JobBuildSuperseded,
        )

    def test_publication_critical_build_cannot_be_displaced_by_initial_work(
        self,
    ) -> None:
        server, rpc = coordinator()
        old_artifacts = server.store_template_artifacts(dict(rpc.template))
        new_artifacts = server.store_template_artifacts(
            base_template(height=11, prevhash="22" * 32)
        )
        assert old_artifacts is not None and new_artifacts is not None
        payout_generation = server._payout_state_generation

        def request_for(
            artifacts: object,
            *,
            publication_critical: bool,
            request_source: str,
        ) -> object:
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
                publication_critical=publication_critical,
                request_source=request_source,
            )

        latest = request_for(
            new_artifacts,
            publication_critical=True,
            request_source="tip_refresh",
        )
        reconnect = request_for(
            old_artifacts,
            publication_critical=False,
            request_source="initial",
        )
        same_tip_reconnect = request_for(
            new_artifacts,
            publication_critical=False,
            request_source="initial",
        )
        latest_flight = SimpleNamespace(request=latest)
        server._job_build_active = latest_flight
        server._job_build_retiring = None
        server._job_build_pending = None

        deferred = server._request_job_build(reconnect)  # type: ignore[arg-type]
        coalesced = server._request_job_build(  # type: ignore[arg-type]
            same_tip_reconnect
        )

        self.assertIs(server._job_build_active, latest_flight)
        self.assertIs(coalesced, latest.promise)  # type: ignore[union-attr]
        self.assertFalse(latest.cancellation.is_set())  # type: ignore[union-attr]
        self.assertFalse(deferred.done())
        self.assertEqual(
            server.job_build_priority_counts["routine_deferred"],
            1,
        )
        self.assertEqual(
            server.initial_job_prepared_work_counts["deferred"],
            1,
        )
        self.assertEqual(
            server.initial_job_prepared_work_counts["singleflight"],
            1,
        )

        latest.promise.set_result(object())  # type: ignore[union-attr]
        with self.assertRaises(JobBuildSuperseded):
            deferred.result()

        metrics = "\n".join(server.job_build_metrics_lines())
        self.assertIn(
            'qbit_prism_job_build_priority_events_total{result="routine_deferred"} 1',
            metrics,
        )
        self.assertIn(
            'qbit_prism_initial_job_prepared_work_total{result="deferred"} 1',
            metrics,
        )
        self.assertIn(
            'qbit_prism_initial_job_prepared_work_total{result="singleflight"} 1',
            metrics,
        )

    def test_publication_critical_build_preempts_routine_builder_capacity(
        self,
    ) -> None:
        server, rpc = coordinator()
        old_artifacts = server.store_template_artifacts(dict(rpc.template))
        new_artifacts = server.store_template_artifacts(
            base_template(height=11, prevhash="22" * 32)
        )
        assert old_artifacts is not None and new_artifacts is not None
        payout_generation = server._payout_state_generation

        def request_for(
            artifacts: object,
            *,
            publication_critical: bool,
            request_source: str,
        ) -> object:
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
                publication_critical=publication_critical,
                request_source=request_source,
            )

        routine = request_for(
            old_artifacts,
            publication_critical=False,
            request_source="initial",
        )
        latest = request_for(
            new_artifacts,
            publication_critical=True,
            request_source="tip_refresh",
        )
        routine_flight = SimpleNamespace(request=routine)
        server._job_build_active = routine_flight
        server._job_build_retiring = None
        server._job_build_pending = None
        server._start_job_build_locked = (  # type: ignore[method-assign]
            lambda request: SimpleNamespace(request=request, future=None)
        )
        server._arm_job_build_locked = lambda _flight: None  # type: ignore[method-assign]

        promise = server._request_job_build(latest)  # type: ignore[arg-type]

        self.assertIs(promise, latest.promise)  # type: ignore[union-attr]
        self.assertTrue(routine.cancellation.is_set())  # type: ignore[union-attr]
        self.assertIs(server._job_build_retiring, routine_flight)
        assert server._job_build_active is not None
        self.assertIs(server._job_build_active.request, latest)
        self.assertEqual(
            server.job_build_priority_counts["routine_preempted"],
            1,
        )

    def test_publication_critical_build_restarts_unhealthy_exact_flight(
        self,
    ) -> None:
        for unhealthy in ("almost_expired", "stalled"):
            with self.subTest(unhealthy=unhealthy):
                server, rpc = coordinator()
                artifacts = server.store_template_artifacts(dict(rpc.template))
                assert artifacts is not None
                payout_generation = server._payout_state_generation

                def request_for(
                    *,
                    publication_critical: bool,
                    priority_requested_monotonic: float | None = None,
                ) -> object:
                    return server._new_job_build_request(
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
                        publication_critical=publication_critical,
                        request_source=(
                            "tip_refresh"
                            if publication_critical
                            else "initial"
                        ),
                        priority_requested_monotonic=(
                            priority_requested_monotonic
                        ),
                    )

                routine = request_for(publication_critical=False)
                now = time.monotonic()
                if unhealthy == "almost_expired":
                    routine.cancellation.started_monotonic = now - 59.99
                    routine.cancellation.deadline_monotonic = now + 0.01
                    routine.cancellation.last_checkpoint_monotonic = now
                else:
                    routine.cancellation.started_monotonic = now - 1.0
                    routine.cancellation.deadline_monotonic = now + 59.0
                    routine.cancellation.last_checkpoint_monotonic = (
                        now - server.job_build_cancel_grace_seconds - 0.01
                    )
                priority_requested = now - 59.0
                latest = request_for(
                    publication_critical=True,
                    priority_requested_monotonic=priority_requested,
                )
                routine_flight = SimpleNamespace(request=routine)
                server._job_build_active = routine_flight
                server._job_build_retiring = None
                server._job_build_pending = None
                server._start_job_build_locked = (  # type: ignore[method-assign]
                    lambda request: SimpleNamespace(request=request, future=None)
                )
                server._arm_job_build_locked = lambda _flight: None  # type: ignore[method-assign]

                promise = server._request_job_build(latest)  # type: ignore[arg-type]

                self.assertIs(promise, latest.promise)
                self.assertIsNot(promise, routine.promise)
                self.assertTrue(routine.cancellation.is_set())
                self.assertIs(server._job_build_retiring, routine_flight)
                assert server._job_build_active is not None
                self.assertIs(server._job_build_active.request, latest)
                self.assertEqual(
                    latest.requested_monotonic,
                    priority_requested,
                )
                self.assertGreater(
                    latest.cancellation.deadline_monotonic
                    - time.monotonic(),
                    server.job_build_timeout_seconds - 1.0,
                )
                self.assertEqual(
                    server.job_build_priority_counts["routine_preempted"],
                    1,
                )

    def test_publication_priority_precedes_immutable_request_preparation(
        self,
    ) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        critical_preparation_entered = threading.Event()
        release_critical_preparation = threading.Event()
        initial_preparation_entered = threading.Event()
        original_new_request = server._new_job_build_request

        def observed_new_request(*args: object, **kwargs: object) -> object:
            request_source = str(kwargs.get("request_source", "routine"))
            if request_source == "tip_refresh":
                critical_preparation_entered.set()
                if not release_critical_preparation.wait(5):
                    raise AssertionError("test did not release priority preparation")
            elif request_source == "initial":
                initial_preparation_entered.set()
            return original_new_request(*args, **kwargs)  # type: ignore[arg-type]

        server._new_job_build_request = observed_new_request  # type: ignore[method-assign]
        results: dict[str, list[object]] = {"critical": [], "initial": []}
        errors: list[BaseException] = []

        def build(label: str, *, publication_critical: bool) -> None:
            try:
                results[label].append(
                    server.shared_job_bundle(
                        artifacts,
                        mode="ready",
                        publication_critical=publication_critical,
                        request_source=(
                            "tip_refresh" if publication_critical else "initial"
                        ),
                    )
                )
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors.append(exc)

        critical_thread = threading.Thread(
            target=build,
            args=("critical",),
            kwargs={"publication_critical": True},
        )
        initial_thread = threading.Thread(
            target=build,
            args=("initial",),
            kwargs={"publication_critical": False},
        )
        critical_thread.start()
        try:
            self.assertTrue(critical_preparation_entered.wait(5))
            initial_thread.start()
            self.assertFalse(initial_preparation_entered.wait(0.1))
            metrics = "\n".join(server.job_build_metrics_lines())
            self.assertIn("qbit_prism_job_build_priority_active 1", metrics)
        finally:
            release_critical_preparation.set()
            critical_thread.join(5)
            initial_thread.join(5)

        self.assertFalse(critical_thread.is_alive())
        self.assertFalse(initial_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual([len(results[label]) for label in results], [1, 1])
        self.assertIs(results["critical"][0], results["initial"][0])
        self.assertFalse(initial_preparation_entered.is_set())
        self.assertEqual(
            server.initial_job_prepared_work_counts["deferred"],
            1,
        )
        self.assertEqual(
            server.initial_job_prepared_work_counts["cache_hit"],
            1,
        )
        self.assertEqual(
            server.job_build_priority_admission_seconds["count"],
            1,
        )
        self.assertGreaterEqual(
            server.job_build_priority_admission_seconds["sum"],
            0.1,
        )

    def test_priority_reservation_cancels_admitted_routine_preparation(
        self,
    ) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        routine_in_payout_lookup = threading.Event()
        release_routine_lookup = threading.Event()
        routine_request_constructed = threading.Event()
        original_usable_artifact = server._usable_payout_ledger_artifact
        original_new_request = server._new_job_build_request
        routine_thread: threading.Thread

        def blocked_usable_artifact(*args: object, **kwargs: object) -> object:
            if threading.current_thread() is routine_thread:
                # The pre-admission cache probe also consults the payout
                # artifact; only the lookup made under an admitted routine
                # preparation exercises the cancellation contract here.
                with server._job_build_scheduler_lock:
                    admitted = bool(server._job_build_routine_preparations)
                if admitted:
                    routine_in_payout_lookup.set()
                    if not release_routine_lookup.wait(5):
                        raise AssertionError("test did not release payout lookup")
            return original_usable_artifact(*args, **kwargs)  # type: ignore[arg-type]

        def observed_new_request(*args: object, **kwargs: object) -> object:
            if kwargs.get("request_source") == "initial":
                routine_request_constructed.set()
            return original_new_request(*args, **kwargs)  # type: ignore[arg-type]

        server._usable_payout_ledger_artifact = blocked_usable_artifact  # type: ignore[method-assign]
        server._new_job_build_request = observed_new_request  # type: ignore[method-assign]
        errors: list[BaseException] = []

        def build_routine() -> None:
            try:
                server.shared_job_bundle(
                    artifacts,
                    mode="ready",
                    retry_superseded=False,
                    request_source="initial",
                )
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors.append(exc)

        routine_thread = threading.Thread(target=build_routine)
        routine_thread.start()
        priority_token: int | None = None
        try:
            self.assertTrue(routine_in_payout_lookup.wait(5))
            with server._job_build_scheduler_lock:
                routine_cancellations = tuple(
                    cancellation_ref()
                    for cancellation_ref in (
                        server._job_build_routine_preparations.values()
                    )
                )
            self.assertEqual(len(routine_cancellations), 1)
            self.assertIsNotNone(routine_cancellations[0])
            priority_token, _requested = (
                server._begin_job_build_priority_preparation()
            )
            assert routine_cancellations[0] is not None
            self.assertTrue(routine_cancellations[0].is_set())
        finally:
            release_routine_lookup.set()
            routine_thread.join(5)
            if priority_token is not None:
                server._finish_job_build_priority_preparation(priority_token)

        self.assertFalse(routine_thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], JobBuildSuperseded)
        self.assertFalse(routine_request_constructed.is_set())
        with server._job_build_scheduler_lock:
            self.assertEqual(len(server._job_build_routine_preparations), 0)

    def test_publication_critical_collection_promotes_past_ready_work(self) -> None:
        server, rpc = coordinator(ledger=FakeLedger(miners=["miner-a"]))
        old_artifacts = server.store_template_artifacts(dict(rpc.template))
        new_artifacts = server.store_template_artifacts(
            base_template(height=11, prevhash="22" * 32)
        )
        assert old_artifacts is not None and new_artifacts is not None
        payout_generation = server._payout_state_generation
        ready = server._new_job_build_request(
            old_artifacts,
            None,
            mode="ready",
            payout_state_generation=payout_generation,
            cache_key=server._job_bundle_key(
                old_artifacts,
                mode="ready",
                payout_state_generation=payout_generation,
                worker=None,
            ),
            request_source="initial",
        )
        latest_identity = worker("tq1latest", "tq1latest.rig")
        latest = server._new_job_build_request(
            new_artifacts,
            latest_identity,
            mode="collection",
            payout_state_generation=payout_generation,
            cache_key=server._job_bundle_key(
                new_artifacts,
                mode="collection",
                payout_state_generation=payout_generation,
                worker=latest_identity,
            ),
            publication_critical=True,
            request_source="tip_refresh",
        )
        ready_flight = SimpleNamespace(request=ready)
        server._job_build_active = ready_flight
        server._job_build_retiring = None
        server._job_build_pending = latest
        armed: list[object] = []
        server._start_job_build_locked = (  # type: ignore[method-assign]
            lambda request: SimpleNamespace(request=request, future=None)
        )
        server._arm_job_build_locked = armed.append  # type: ignore[method-assign]

        server._promote_pending_job_build_locked()

        self.assertTrue(ready.cancellation.is_set())
        self.assertIs(server._job_build_retiring, ready_flight)
        self.assertIsNone(server._job_build_pending)
        assert server._job_build_active is not None
        self.assertIs(server._job_build_active.request, latest)
        self.assertEqual(armed, [server._job_build_active])
        self.assertEqual(
            server.job_build_priority_counts["routine_preempted"],
            1,
        )

    def test_routine_pending_never_promotes_over_publication_critical_flight(
        self,
    ) -> None:
        for placement in ("active", "retiring"):
            with self.subTest(placement=placement):
                server, rpc = coordinator()
                critical_artifacts = server.store_template_artifacts(
                    dict(rpc.template)
                )
                routine_artifacts = server.store_template_artifacts(
                    base_template(height=11, prevhash="22" * 32)
                )
                assert critical_artifacts is not None
                assert routine_artifacts is not None
                payout_generation = server._payout_state_generation

                def request_for(
                    artifacts: object,
                    *,
                    publication_critical: bool,
                ) -> object:
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
                        publication_critical=publication_critical,
                        request_source=(
                            "tip_refresh" if publication_critical else "initial"
                        ),
                    )

                critical = request_for(
                    critical_artifacts,
                    publication_critical=True,
                )
                routine = request_for(
                    routine_artifacts,
                    publication_critical=False,
                )
                critical_flight = SimpleNamespace(request=critical)
                server._job_build_active = (
                    critical_flight if placement == "active" else None
                )
                server._job_build_retiring = (
                    critical_flight if placement == "retiring" else None
                )
                server._job_build_pending = routine
                server._start_job_build_locked = (  # type: ignore[method-assign]
                    lambda _request: self.fail(
                        "routine pending work displaced publication-critical work"
                    )
                )

                server._promote_pending_job_build_locked()

                self.assertIs(server._job_build_pending, routine)
                self.assertFalse(critical.cancellation.is_set())
                self.assertIs(
                    (
                        server._job_build_active
                        if placement == "active"
                        else server._job_build_retiring
                    ),
                    critical_flight,
                )

    def test_cancelled_ready_does_not_block_collection_promotion(self) -> None:
        for placement in ("active", "retiring"):
            with self.subTest(placement=placement):
                server, rpc = coordinator(ledger=FakeLedger(miners=["miner-a"]))
                artifacts = server.store_template_artifacts(dict(rpc.template))
                assert artifacts is not None
                payout_generation = server._payout_state_generation

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
                    server._job_build_active = ready_flight
                    server._job_build_retiring = None
                else:
                    server._job_build_active = None
                    server._job_build_retiring = ready_flight
                server._job_build_pending = collection
                armed: list[object] = []
                server._start_job_build_locked = (  # type: ignore[method-assign]
                    lambda request: SimpleNamespace(request=request, future=None)
                )
                server._arm_job_build_locked = armed.append  # type: ignore[method-assign]

                server._promote_pending_job_build_locked()

                self.assertIsNone(server._job_build_pending)
                assert server._job_build_active is not None
                self.assertIs(server._job_build_active.request, collection)
                self.assertIs(server._job_build_retiring, ready_flight)
                self.assertEqual(armed, [server._job_build_active])

    def test_immediate_collection_completion_does_not_reoccupy_slot(self) -> None:
        server, rpc = coordinator(ledger=FakeLedger(miners=["miner-a"]))
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        payout_generation = server._payout_state_generation
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

        server._job_build_pending = pending
        server._start_job_build_locked = completed_flight  # type: ignore[method-assign]

        promise = server._request_job_build(incoming)  # type: ignore[arg-type]

        self.assertIs(promise.result(), results[id(incoming)])
        self.assertIs(  # type: ignore[union-attr]
            pending.promise.result(),
            results[id(pending)],
        )
        self.assertIsNone(server._job_build_active)
        self.assertIsNone(server._job_build_retiring)
        self.assertIsNone(server._job_build_pending)

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
        original_build = server.build_shared_job_bundle
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

        server.build_shared_job_bundle = blocking_build  # type: ignore[method-assign]
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
                with server._job_build_scheduler_lock:
                    pending = server._job_build_pending
                    if pending is not None and pending.worker == identities[2]:
                        break
                time.sleep(0.01)
            else:
                self.fail("third collection build was not queued")
            threads[3].start()
            self.assertEqual(server.job_build_scheduler_counts["starts"], 2)
            releases[0].set()
            self.assertTrue(entered[2].wait(5))
            releases[1].set()
            self.assertTrue(entered[3].wait(5))
            self.assertEqual(server.job_build_scheduler_counts["starts"], 4)
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
        self.assertEqual(server.job_build_scheduler_counts["supersessions"], 0)

    def test_collection_independence_requires_one_immutable_cohort(self) -> None:
        server, rpc = coordinator(ledger=FakeLedger(miners=["miner-a"]))
        first = server.store_template_artifacts(dict(rpc.template))
        assert first is not None
        second_template = dict(rpc.template)
        second_template["curtime"] = int(second_template["curtime"]) + 1
        second = server.store_template_artifacts(second_template)
        assert second is not None
        payout_generation = server._payout_state_generation
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
        original_build = server.build_shared_job_bundle

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

        server.build_shared_job_bundle = blocking_build  # type: ignore[method-assign]
        payout_generation = server._payout_state_generation

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
            self.assertEqual(server.job_build_scheduler_counts["starts"], 2)
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
        self.assertEqual(server.job_build_scheduler_counts["starts"], 3)

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
        original_build = server.build_shared_job_bundle

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

        server.build_shared_job_bundle = blocking_build  # type: ignore[method-assign]
        payout_generation = server._payout_state_generation

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
            with server._job_build_scheduler_lock:
                self.assertIsNone(server._job_build_active)
                assert server._job_build_retiring is not None
                self.assertIs(
                    server._job_build_retiring.request,
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
        original_build = server.build_shared_job_bundle

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

        server.build_shared_job_bundle = blocking_build  # type: ignore[method-assign]
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
            self.assertEqual(server.job_build_scheduler_counts["starts"], 2)

            release_cancelled.set()
            self.assertTrue(ready_entered.wait(5))
            retry_deadline = time.monotonic() + 5
            while (
                server.job_build_scheduler_counts["requests"] < 5
                and time.monotonic() < retry_deadline
            ):
                time.sleep(0.01)
            self.assertGreaterEqual(
                server.job_build_scheduler_counts["requests"],
                5,
            )
            self.assertEqual(server.job_build_scheduler_counts["starts"], 3)
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
        payout_generation = server._payout_state_generation
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

        server.build_shared_job_bundle = fill_helper_pipe  # type: ignore[method-assign]
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
            "lab.prism.prism_coordinator.prism_tool_command",
            return_value=[sys.executable, "-c", "import time; time.sleep(30)"],
        ), patch(
            "lab.prism.prism_coordinator.subprocess.Popen",
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

        server.build_shared_job_bundle = fill_helper_pipe  # type: ignore[method-assign]
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
            "lab.prism.prism_coordinator.prism_tool_command",
            return_value=[sys.executable, "-c", "import time; time.sleep(30)"],
        ), patch(
            "lab.prism.prism_coordinator.subprocess.Popen",
            side_effect=capture_popen,
        ):
            build_thread = threading.Thread(target=build)
            build_thread.start()
            self.assertTrue(helper_started.wait(5))
            with server._job_cache_lock:
                controls = list(server._active_job_bundle_builds.values())
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
        self.assertEqual(server.shared_bundle_build_counts["superseded"], 1)
        self.assertEqual(server.shared_bundle_build_counts["failed"], 0)
        self.assertEqual(server.tip_refresh_superseded_results, 1)

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
            "lab.prism.prism_coordinator.prism_tool_command",
            return_value=[sys.executable, "-c", "import time; time.sleep(30)"],
        ), patch(
            "lab.prism.prism_coordinator.subprocess.Popen",
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

    def test_untrusted_outcome_for_other_tip_keeps_memo_armed(self) -> None:
        server, rpc = coordinator()
        server.reorg_reconciler_enabled = True
        server._note_reorg_reconcile_outcome(rpc.tip, trusted=True)
        reconcile_calls: list[str | None] = []

        def fake_reconcile(*, tip_hash: str | None = None) -> dict[str, object]:
            reconcile_calls.append(tip_hash)
            server._note_reorg_reconcile_outcome(tip_hash, trusted=True)
            return {"untrusted": False}

        server.reconcile_prism_pool_blocks_once = fake_reconcile  # type: ignore[method-assign]

        # A superseded/untrusted pass for a different tip must not unarm the
        # cached trusted outcome for the current tip.
        server._note_reorg_reconcile_outcome("44" * 32, trusted=False)
        self.assertTrue(server.ensure_reorg_reconciled_for_current_tip())
        self.assertEqual(reconcile_calls, [])

        # An untrusted outcome for the current tip itself unarms only it.
        server._note_reorg_reconcile_outcome(rpc.tip, trusted=False)
        self.assertTrue(server.ensure_reorg_reconciled_for_current_tip())
        self.assertEqual(reconcile_calls, [rpc.tip])

    def test_reconcile_error_outcome_clears_every_memo_entry(self) -> None:
        server, rpc = coordinator()
        server.reorg_reconciler_enabled = True
        server._note_reorg_reconcile_outcome(rpc.tip, trusted=True)
        server._note_reorg_reconcile_outcome("55" * 32, trusted=True)
        reconcile_calls: list[str | None] = []

        def fake_reconcile(*, tip_hash: str | None = None) -> dict[str, object]:
            reconcile_calls.append(tip_hash)
            server._note_reorg_reconcile_outcome(tip_hash, trusted=True)
            return {"untrusted": False}

        server.reconcile_prism_pool_blocks_once = fake_reconcile  # type: ignore[method-assign]

        server._note_reorg_reconcile_outcome(
            "55" * 32,
            trusted=False,
            clear_memo=True,
        )

        self.assertTrue(server.ensure_reorg_reconciled_for_current_tip())
        self.assertEqual(reconcile_calls, [rpc.tip])

    def test_tip_flip_back_within_ttl_requires_fresh_reconcile(self) -> None:
        server, rpc = coordinator()
        server.reorg_reconciler_enabled = True
        tip_a = rpc.tip
        self.addCleanup(server.shutdown_tip_refresh_executor)
        server.observe_tip_for_refresh(tip_a)
        server._note_reorg_reconcile_outcome(tip_a, trusted=True)
        reconcile_calls: list[str | None] = []

        def fake_reconcile(*, tip_hash: str | None = None) -> dict[str, object]:
            reconcile_calls.append(tip_hash)
            return {"untrusted": False}

        server.reconcile_prism_pool_blocks_once = fake_reconcile  # type: ignore[method-assign]

        self.assertTrue(server.ensure_reorg_reconciled_for_current_tip())
        self.assertEqual(reconcile_calls, [])

        # Detecting an intervening tip ends tip A's memo epoch: when the
        # chain flips back to A within the cache TTL, pool-block chain state
        # must be re-proven by a fresh pass, not assumed from the pre-flip
        # outcome.
        server.observe_tip_for_refresh("bb" * 32)

        self.assertTrue(server.ensure_reorg_reconciled_for_current_tip())
        self.assertEqual(reconcile_calls, [tip_a])

    def test_stale_pass_cannot_rearm_memo_after_newer_tip_detected(self) -> None:
        server, rpc = coordinator()
        server.reorg_reconciler_enabled = True
        tip_a = rpc.tip
        self.addCleanup(server.shutdown_tip_refresh_executor)
        server.observe_tip_for_refresh(tip_a)
        server.observe_tip_for_refresh("bb" * 32)

        # A pass for tip A finishing after tip B was detected must not arm
        # the memo for A: its epoch already ended at the newer observation.
        server._note_reorg_reconcile_outcome(tip_a, trusted=True)
        reconcile_calls: list[str | None] = []

        def fake_reconcile(*, tip_hash: str | None = None) -> dict[str, object]:
            reconcile_calls.append(tip_hash)
            return {"untrusted": False}

        server.reconcile_prism_pool_blocks_once = fake_reconcile  # type: ignore[method-assign]

        self.assertTrue(server.ensure_reorg_reconciled_for_current_tip())
        self.assertEqual(reconcile_calls, [tip_a])

    def test_rearmed_artifact_is_not_shadowed_by_stale_cached_bundle(
        self,
    ) -> None:
        server, rpc = coordinator()
        recorded = install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None

        stale = server.shared_job_bundle(artifacts, mode="ready")
        self.assertEqual(recorded["calls"], 1)
        self.assertEqual(len(stale.shares_json), 3)

        # A share lands and the debounced re-arm rebuilds a fresher window.
        server.ledger.miners = [*server.ledger.miners, "late-share"]
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        with server._job_cache_lock:
            fresh_artifact = server._payout_ledger_artifact
        assert fresh_artifact is not None
        self.assertEqual(fresh_artifact.accepted_share_count, 4)

        rebuilt = server.shared_job_bundle(artifacts, mode="ready")

        # The pre-re-arm bundle is still inside its cache TTL under the
        # no-artifact key; its older window must not shadow the fresher
        # artifact.
        self.assertEqual(
            rebuilt.payout_artifact_generation,
            fresh_artifact.generation,
        )
        self.assertEqual(len(rebuilt.shares_json), 4)
        self.assertEqual(recorded["calls"], 2)

    def test_in_flight_build_keeps_aged_artifact_it_already_selected(
        self,
    ) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        server.payout_artifact_max_anchor_age_seconds = 5.0
        with server._job_cache_lock:
            armed = server._payout_ledger_artifact
            assert armed is not None
            assert armed.snapshot_anchor_ms is not None
            aged = dataclass_replace(
                armed,
                snapshot_anchor_ms=int(armed.snapshot_anchor_ms) - 6_000,
            )
            server._payout_ledger_artifact = aged

        # New reuse decisions reject a window past the audit ceiling...
        self.assertIsNone(
            server._usable_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
                rearm_on_fence_failure=False,
            )
        )
        # ...but a build that selected it while fresh completes with it: the
        # in-build re-validation checks supersession and the balances fence
        # only, so queue delay past the bound cannot scrap the reuse into
        # the full snapshot it was armed to avoid.
        server.ledger.snapshot_calls = 0
        bundle = server.build_shared_job_bundle(
            artifacts,
            worker(),
            payout_artifact=aged,
        )
        self.assertEqual(server.ledger.snapshot_calls, 0)
        self.assertEqual(
            bundle.found_block["anchor_job_issued_at_ms"],
            aged.snapshot_anchor_ms,
        )

    def test_in_flight_build_survives_same_window_anchor_refresh(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        with server._job_cache_lock:
            armed = server._payout_ledger_artifact
            assert armed is not None
            assert armed.snapshot_anchor_ms is not None
            # An in-flight build selected the armed artifact while it
            # carried an older anchor.
            selected = dataclass_replace(
                armed,
                snapshot_anchor_ms=int(armed.snapshot_anchor_ms) - 5,
            )
            server._payout_ledger_artifact = selected

        # A same-window rebuild refreshes the anchor in place: the stored
        # instance swaps while the generation -- the re-key authority --
        # and the window bytes stay identical.
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        with server._job_cache_lock:
            refreshed = server._payout_ledger_artifact
        assert refreshed is not None
        self.assertIsNot(refreshed, selected)
        self.assertEqual(refreshed.generation, selected.generation)
        self.assertEqual(
            refreshed.share_snapshot_sha256,
            selected.share_snapshot_sha256,
        )

        # The build holding the pre-refresh instance completes its reuse
        # instead of being scrapped into a full snapshot by the swap.
        server.ledger.snapshot_calls = 0
        bundle = server.build_shared_job_bundle(
            artifacts,
            worker(),
            payout_artifact=selected,
        )
        self.assertEqual(server.ledger.snapshot_calls, 0)
        self.assertEqual(
            bundle.found_block["anchor_job_issued_at_ms"],
            selected.snapshot_anchor_ms,
        )

    def test_generation_current_cached_bundle_serves_past_reanchor_floor(
        self,
    ) -> None:
        server, rpc = coordinator()
        recorded = install_fake_bundle_builder(server)
        first = server.store_template_artifacts(dict(rpc.template))
        assert first is not None
        with server._payout_artifact_executor_lock:
            server._payout_artifact_executor_shutdown = True
        # Re-anchor floor below the bundle-cache TTL so the two gates are
        # distinguishable inside the TTL window.
        server.payout_artifact_reanchor_seconds = 5.0

        server.shared_job_bundle(first, mode="ready")
        self.assertEqual(recorded["calls"], 1)
        with server._job_cache_lock:
            armed = server._payout_ledger_artifact
        assert armed is not None

        # A bundle keyed to the currently armed artifact generation carries
        # exactly the armed window: it keeps serving past the re-anchor
        # floor (its window is as fresh as reuse itself), bounded upstream
        # by the cache TTL.
        second = server.store_template_artifacts(
            base_template(height=11, prevhash="22" * 32)
        )
        assert second is not None
        reused = server.shared_job_bundle(second, mode="ready")
        self.assertEqual(reused.payout_artifact_generation, armed.generation)
        self.assertEqual(recorded["calls"], 2)
        with server._job_cache_lock:
            for key, entry in list(server._job_bundle_cache.items()):
                if entry.payout_artifact_generation == armed.generation:
                    server._job_bundle_cache[key] = dataclass_replace(
                        entry,
                        built_monotonic=float(entry.built_monotonic) - 7.0,
                    )
        server.shared_job_bundle(second, mode="ready")
        self.assertEqual(recorded["calls"], 2)

    def test_cached_no_artifact_bundle_gates_on_build_age_not_anchor(
        self,
    ) -> None:
        # 2026-07-29 regression companion: the gate must follow build age
        # (freshness of the work) inside the audit ceiling, never the
        # wall-clock anchor, which is frozen per template generation.
        server, rpc = coordinator()
        recorded = install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        with server._payout_artifact_executor_lock:
            server._payout_artifact_executor_shutdown = True
        server.payout_artifact_reanchor_seconds = 5.0

        seeded = server.shared_job_bundle(artifacts, mode="ready")
        self.assertEqual(recorded["calls"], 1)

        # Fresh build age serves from cache once the armed artifact is
        # gone.
        with server._job_cache_lock:
            server._payout_ledger_artifact = None
        server.shared_job_bundle(artifacts, mode="ready")
        self.assertEqual(recorded["calls"], 1)

        # Build age past the re-anchor floor rebuilds even though the
        # bundle-cache TTL has not lapsed.
        with server._job_cache_lock:
            server._payout_ledger_artifact = None
            for key, entry in list(server._job_bundle_cache.items()):
                server._job_bundle_cache[key] = dataclass_replace(
                    entry,
                    built_monotonic=float(entry.built_monotonic) - 7.0,
                )
        server.shared_job_bundle(artifacts, mode="ready")
        self.assertEqual(recorded["calls"], 2)

        # The audit ceiling on the declared anchor stays as the backstop
        # even for a freshly built entry.
        aged_anchor_ms = (
            int(seeded.found_block["anchor_job_issued_at_ms"]) - 400_000
        )
        with server._job_cache_lock:
            server._payout_ledger_artifact = None
            for key, entry in list(server._job_bundle_cache.items()):
                aged_found_block = dict(entry.found_block)
                aged_found_block["anchor_job_issued_at_ms"] = aged_anchor_ms
                server._job_bundle_cache[key] = dataclass_replace(
                    entry,
                    found_block=aged_found_block,
                )
        server.shared_job_bundle(artifacts, mode="ready")
        self.assertEqual(recorded["calls"], 3)

    def test_generation_bump_does_not_scrap_in_flight_build(self) -> None:
        # 2026-07-29 regression: every differing window bumps the armed
        # artifact generation, and the in-build re-validation scrapped any
        # build whose selected artifact lost the slot -- a rebuild storm at
        # production share rates. A build must finish on the copy it
        # selected: its window stays audit-reproducible at its declared
        # anchor while the payout generation and published balances hold.
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        with server._job_cache_lock:
            selected = server._payout_ledger_artifact
        assert selected is not None
        assert selected.snapshot_anchor_ms is not None

        fresher_window = [
            {"share_seq": seq, "miner_id": "miner-a"} for seq in (1, 2, 3, 4)
        ]
        fresher = dataclass_replace(
            selected,
            generation=0,
            accepted_share_count=4,
            shares_json=tuple(fresher_window),
            snapshot_anchor_ms=int(selected.snapshot_anchor_ms) + 5,
            share_snapshot_sha256=canonical_json_sha256(fresher_window),
        )
        self.assertTrue(server._install_payout_ledger_artifact(fresher))
        with server._job_cache_lock:
            bumped = server._payout_ledger_artifact
        assert bumped is not None
        self.assertGreater(bumped.generation, selected.generation)

        server.ledger.snapshot_calls = 0
        bundle = server.build_shared_job_bundle(
            artifacts,
            worker(),
            payout_artifact=selected,
        )
        self.assertEqual(server.ledger.snapshot_calls, 0)
        self.assertEqual(
            bundle.found_block["anchor_job_issued_at_ms"],
            selected.snapshot_anchor_ms,
        )
        self.assertEqual(bundle.shares_json, list(selected.shares_json))

    def test_rebuilt_bundle_serves_under_aged_template_generation(
        self,
    ) -> None:
        # 2026-07-29 regression: issued_at_ms is frozen per template
        # generation and predates the walk, so a wall-clock anchor gate on
        # cached bundles declared every rebuilt bundle dead on arrival once
        # its template generation outlived the bound. Below the audit
        # ceiling, cache service must follow build age, not anchor age.
        server, rpc = coordinator()
        recorded = install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        with server._payout_artifact_executor_lock:
            server._payout_artifact_executor_shutdown = True
        with server._job_cache_lock:
            server._job_build_issued_at_ms[artifacts.generation] = (
                now_ms() - 20_000
            )

        built = server.shared_job_bundle(artifacts, mode="ready")
        self.assertEqual(recorded["calls"], 1)
        self.assertEqual(
            built.found_block["anchor_job_issued_at_ms"],
            built.issued_at_ms,
        )

        served = server.shared_job_bundle(artifacts, mode="ready")
        self.assertEqual(recorded["calls"], 1)
        self.assertEqual(served.issued_at_ms, built.issued_at_ms)

    def test_admission_deadline_fails_fast_and_visibly(self) -> None:
        server, _rpc = coordinator()
        server.routine_admission_deadline_seconds = 0.05
        server._publication_priority_scheduled_locked = (  # type: ignore[method-assign]
            lambda: True
        )

        with self.assertRaises(JobBuildAdmissionDeadlineExceeded) as raised:
            server._begin_routine_job_build_preparation(
                request_source="initial",
                cancelled=None,
            )

        # The deadline is coordination churn, not a failure budget event:
        # callers retry with their normal pacing.
        self.assertIsInstance(raised.exception, TemplateRefreshBlocked)
        with server._job_build_scheduler_lock:
            self.assertEqual(
                server.initial_job_prepared_work_counts["admission_deadline"],
                1,
            )


class ShareWindowSpoolTests(unittest.TestCase):
    """Spool-file handoff of the serialized share window to the builder."""

    def _coordinator(self) -> PrismCoordinator:
        server, _rpc = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        self.addCleanup(server.retire_share_window_spool)
        return server

    def _ledger_artifact(
        self,
        shares: list[dict[str, object]],
        *,
        generation: int,
    ) -> PayoutLedgerArtifact:
        return PayoutLedgerArtifact(
            generation=generation,
            payout_state_generation=0,
            network_difficulty=1,
            accepted_share_count=len(shares),
            shares_json=tuple(shares),
            prior_balances=(),
            prepared_monotonic=time.monotonic(),
            snapshot_anchor_ms=None,
        )

    def _build_with_echo_builder(
        self,
        server: PrismCoordinator,
        shares: list[dict[str, object]],
        serialization: object,
        *,
        height: int,
    ) -> dict[str, object]:
        # These tests cover the one-shot spool handoff; pin the persistent
        # builder off so the echo helper is never spawned as a daemon.
        with patch.dict(os.environ, {"PRISM_BUILDER_SERVE": "0"}), patch(
            "lab.prism.prism_coordinator.prism_tool_command",
            return_value=list(ECHO_BUILDER_COMMAND),
        ):
            return server.build_audit_bundle(
                shares=shares,
                found_block={
                    "block_height": height,
                    "coinbase_value_sats": 50_00000000,
                    "network_difficulty": 1,
                    "anchor_job_issued_at_ms": 1_700_000_000_000,
                },
                prior_balances=[],
                coinbase_script_sig_suffix_hex="00",
                summary_only=True,
                payout_policy={"policy": "day-one"},
                share_serialization=serialization,  # type: ignore[arg-type]
            )

    def _expected_payload(
        self,
        shares: list[dict[str, object]],
        *,
        height: int,
    ) -> dict[str, object]:
        identities, compact_shares = _compact_share_payload(shares)
        return {
            "found_block": {
                "block_height": height,
                "coinbase_value_sats": 50_00000000,
                "network_difficulty": 1,
                "anchor_job_issued_at_ms": 1_700_000_000_000,
            },
            "prior_balances": [],
            "payout_policy": {"policy": "day-one"},
            "coinbase_script_sig_suffix_hex": "00",
            "witness_merkle_leaves_hex": [],
            "compact_share_identities": [
                list(identity) for identity in identities
            ],
            "compact_shares": [list(share) for share in compact_shares],
        }

    def test_spool_feeds_builder_and_is_written_once_per_generation(
        self,
    ) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = server._share_window_serialization_for_artifact(
            self._ledger_artifact(shares, generation=1),
            shares,
        )
        spool_creations: list[int] = []
        real_spool_file = prism_coordinator_module._share_window_spool_file

        def counting_spool_file() -> object:
            spool_creations.append(1)
            return real_spool_file()

        with patch(
            "lab.prism.prism_coordinator._share_window_spool_file",
            side_effect=counting_spool_file,
        ):
            first = self._build_with_echo_builder(
                server,
                shares,
                serialization,
                height=10,
            )
            second = self._build_with_echo_builder(
                server,
                shares,
                serialization,
                height=11,
            )

        self.assertEqual(
            first["received"],
            self._expected_payload(shares, height=10),
        )
        self.assertEqual(
            second["received"],
            self._expected_payload(shares, height=11),
        )
        self.assertEqual(len(spool_creations), 1)

    def test_spool_creation_failure_falls_back_to_pipe_writes(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = server._share_window_serialization_for_artifact(
            self._ledger_artifact(shares, generation=1),
            shares,
        )

        with patch(
            "lab.prism.prism_coordinator._share_window_spool_file",
            side_effect=OSError("temp filesystem unavailable"),
        ) as spool_factory:
            first = self._build_with_echo_builder(
                server,
                shares,
                serialization,
                height=10,
            )
            second = self._build_with_echo_builder(
                server,
                shares,
                serialization,
                height=11,
            )

        self.assertEqual(
            first["received"],
            self._expected_payload(shares, height=10),
        )
        self.assertEqual(
            second["received"],
            self._expected_payload(shares, height=11),
        )
        # The failure is sticky: a broken temp filesystem is not retried on
        # every subsequent build of the same generation.
        self.assertEqual(spool_factory.call_count, 1)

    def test_splice_failure_poisons_spool_and_falls_back_to_pipe(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = server._share_window_serialization_for_artifact(
            self._ledger_artifact(shares, generation=1),
            shares,
        )

        with patch(
            "lab.prism.prism_coordinator.os.splice",
            side_effect=OSError("splice unsupported"),
        ):
            first = self._build_with_echo_builder(
                server,
                shares,
                serialization,
                height=10,
            )
        second = self._build_with_echo_builder(
            server,
            shares,
            serialization,
            height=11,
        )

        self.assertEqual(
            first["received"],
            self._expected_payload(shares, height=10),
        )
        self.assertEqual(
            second["received"],
            self._expected_payload(shares, height=11),
        )
        self.assertTrue(serialization._spool_failed)
        self.assertIsNone(serialization._spool_file)

    def test_mid_stream_splice_failure_poisons_spool_for_later_builds(
        self,
    ) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = server._share_window_serialization_for_artifact(
            self._ledger_artifact(shares, generation=1),
            shares,
        )

        # The first chunk moves and the second fails: this build is
        # unrepairable (partial tail already in the pipe), but the spool must
        # be poisoned so later builds stream the cached fragments instead of
        # repeating the failure.
        with patch(
            "lab.prism.prism_coordinator.os.splice",
            side_effect=[16, OSError("splice io error")],
        ):
            with self.assertRaises(OSError):
                self._build_with_echo_builder(
                    server,
                    shares,
                    serialization,
                    height=10,
                )
        self.assertTrue(serialization._spool_failed)

        recovered = self._build_with_echo_builder(
            server,
            shares,
            serialization,
            height=11,
        )
        self.assertEqual(
            recovered["received"],
            self._expected_payload(shares, height=11),
        )

    def test_content_rotation_retires_spool_after_last_lease(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        first = server._share_window_serialization_for_artifact(
            self._ledger_artifact(shares, generation=1),
            shares,
        )
        lease = first.acquire_spooled_tail(shares)
        self.assertIsNotNone(lease)
        assert lease is not None
        spool_file, spool_size = lease
        self.assertGreater(spool_size, 0)

        changed_shares = [*shares, spool_share(4)]
        second = server._share_window_serialization_for_artifact(
            self._ledger_artifact(changed_shares, generation=2),
            changed_shares,
        )
        self.assertIsNot(second, first)
        # Retired: no new leases, but the in-flight transfer keeps the
        # descriptor alive until it releases.
        self.assertIsNone(first.acquire_spooled_tail(shares))
        self.assertFalse(spool_file.closed)
        first.release_spooled_tail()
        self.assertTrue(spool_file.closed)

    def test_generation_retag_reuses_content_keyed_serialization(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]

        first = server._share_window_serialization_for_artifact(
            self._ledger_artifact(shares, generation=1),
            shares,
        )
        second = server._share_window_serialization_for_artifact(
            self._ledger_artifact(shares, generation=2),
            shares,
        )

        self.assertIs(second, first)

    def test_shutdown_retires_current_spool(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = server._share_window_serialization_for_artifact(
            self._ledger_artifact(shares, generation=1),
            shares,
        )
        lease = serialization.acquire_spooled_tail(shares)
        assert lease is not None
        serialization.release_spooled_tail()
        self.assertFalse(lease[0].closed)

        server.retire_share_window_spool()

        self.assertTrue(lease[0].closed)
        self.assertIsNone(serialization.acquire_spooled_tail(shares))


class ServeBuilderTests(unittest.TestCase):
    """Persistent --serve builder client, its fallback, and supersession."""

    def _coordinator(self) -> PrismCoordinator:
        server, _rpc = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        self.addCleanup(server.shutdown_serve_builder)
        self.addCleanup(server.retire_share_window_spool)
        return server

    def _serialization(
        self,
        server: PrismCoordinator,
        shares: list[dict[str, object]],
        *,
        generation: int = 1,
    ) -> object:
        return server._share_window_serialization_for_artifact(
            PayoutLedgerArtifact(
                generation=generation,
                payout_state_generation=0,
                network_difficulty=1,
                accepted_share_count=len(shares),
                shares_json=tuple(shares),
                prior_balances=(),
                prepared_monotonic=time.monotonic(),
                snapshot_anchor_ms=None,
            ),
            shares,
        )

    def _build(
        self,
        server: PrismCoordinator,
        shares: list[dict[str, object]],
        serialization: object,
        *,
        height: int,
        mode: str = "ok",
        cancellation: object = None,
    ) -> dict[str, object]:
        with patch.dict(
            os.environ,
            {"FAKE_SERVE_BUILDER_MODE": mode},
        ), patch(
            "lab.prism.prism_coordinator.prism_tool_command",
            return_value=list(FAKE_SERVE_BUILDER_COMMAND),
        ):
            return server.build_audit_bundle(
                shares=shares,
                found_block={
                    "block_height": height,
                    "coinbase_value_sats": 50_00000000,
                    "network_difficulty": 1,
                    "anchor_job_issued_at_ms": 1_700_000_000_000,
                },
                prior_balances=[],
                coinbase_script_sig_suffix_hex="00",
                summary_only=True,
                payout_policy={"policy": "day-one"},
                share_serialization=serialization,  # type: ignore[arg-type]
                cancellation=cancellation,  # type: ignore[arg-type]
            )

    def test_cache_miss_uploads_window_then_hits_on_next_request(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = self._serialization(server, shares)
        identities, compact_shares = _compact_share_payload(shares)
        expected_window = {
            "compact_share_identities": [
                list(identity) for identity in identities
            ],
            "compact_shares": [list(share) for share in compact_shares],
        }
        try:
            first = self._build(server, shares, serialization, height=10)
            second = self._build(server, shares, serialization, height=11)

            self.assertEqual(first["transport"], "serve")
            self.assertTrue(first["request_had_window"])
            self.assertEqual(first["window"], expected_window)
            self.assertEqual(first["found_block"]["block_height"], 10)

            self.assertEqual(second["transport"], "serve")
            self.assertFalse(second["request_had_window"])
            self.assertEqual(second["window"], expected_window)
            self.assertEqual(second["found_block"]["block_height"], 11)

            with server._serve_builder_metrics_lock:
                counts = dict(server.serve_builder_counts)
                window_counts = dict(server.serve_builder_window_cache_counts)
            self.assertEqual(counts["requests"], 2)
            self.assertEqual(counts["spawns"], 1)
            self.assertEqual(counts["fallbacks"], 0)
            self.assertEqual(counts["window_uploads"], 1)
            self.assertEqual(window_counts, {"hits": 1, "misses": 1})
        finally:
            server.shutdown_serve_builder()

    def test_needs_window_bounce_reuploads_and_succeeds(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = self._serialization(server, shares)
        try:
            first = self._build(server, shares, serialization, height=10)
            self.assertTrue(first["request_had_window"])
            client = server._serve_builder
            assert client is not None
            second_shares = [spool_share(seq) for seq in range(1, 5)]
            second_serialization = self._serialization(
                server,
                second_shares,
                generation=2,
            )
            # Pretend this window was already uploaded: the daemon's
            # needs_window bounce must repair the divergence transparently.
            client.note_uploaded_window(
                second_serialization.share_snapshot_sha256  # type: ignore[attr-defined]
            )

            server._ensure_tip_refresh_state()
            server._job_build_phase_local.tip_refresh_metrics = True
            try:
                second = self._build(
                    server,
                    second_shares,
                    second_serialization,
                    height=11,
                )
            finally:
                server._job_build_phase_local.tip_refresh_metrics = False

            self.assertEqual(second["transport"], "serve")
            self.assertTrue(second["request_had_window"])
            self.assertEqual(second["found_block"]["block_height"], 11)
            with server._serve_builder_metrics_lock:
                counts = dict(server.serve_builder_counts)
            self.assertEqual(counts["requests"], 2)
            self.assertEqual(counts["fallbacks"], 0)
            # One build observes serialization_copy exactly three times --
            # fragment precompose, the accumulated transport writes, and the
            # daemon's reported timings -- matching the one-shot transport
            # even when a needs_window bounce re-sent the request.
            histogram = server.tip_refresh_build_phase_histograms[
                "serialization_copy"
            ]
            self.assertEqual(histogram["count"], 3)
        finally:
            server.shutdown_serve_builder()

    def test_hit_promotion_keeps_mirror_and_daemon_lru_aligned(self) -> None:
        server = self._coordinator()
        shares_a = [spool_share(seq) for seq in range(1, 4)]
        shares_b = [spool_share(seq) for seq in range(1, 5)]
        shares_c = [spool_share(seq) for seq in range(1, 6)]
        ser_a = self._serialization(server, shares_a, generation=1)
        ser_b = self._serialization(server, shares_b, generation=2)
        ser_c = self._serialization(server, shares_c, generation=3)
        try:
            self.assertTrue(
                self._build(server, shares_a, ser_a, height=10)[
                    "request_had_window"
                ]
            )
            self.assertTrue(
                self._build(server, shares_b, ser_b, height=11)[
                    "request_had_window"
                ]
            )
            # The hit promotes window A on both sides; uploading a third
            # window must therefore evict B everywhere.
            self.assertFalse(
                self._build(server, shares_a, ser_a, height=12)[
                    "request_had_window"
                ]
            )
            self.assertTrue(
                self._build(server, shares_c, ser_c, height=13)[
                    "request_had_window"
                ]
            )

            aligned = self._build(server, shares_a, ser_a, height=14)

            self.assertEqual(aligned["transport"], "serve")
            self.assertFalse(aligned["request_had_window"])
            with server._serve_builder_metrics_lock:
                counts = dict(server.serve_builder_counts)
            self.assertEqual(counts["requests"], 5)
            self.assertEqual(counts["window_uploads"], 3)
            self.assertEqual(counts["fallbacks"], 0)
        finally:
            server.shutdown_serve_builder()

    def test_daemon_crash_falls_back_to_one_shot(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = self._serialization(server, shares)
        try:
            result = self._build(
                server,
                shares,
                serialization,
                height=10,
                mode="crash-before-response",
            )

            self.assertEqual(result["transport"], "one-shot")
            self.assertEqual(
                result["received"]["found_block"]["block_height"],
                10,
            )
            self.assertIn("compact_shares", result["received"])
            with server._serve_builder_metrics_lock:
                counts = dict(server.serve_builder_counts)
                self.assertIsNone(server._serve_builder)
            self.assertEqual(counts["fallbacks"], 1)
            self.assertEqual(counts["requests"], 0)
        finally:
            server.shutdown_serve_builder()

    def test_daemon_splice_failure_poisons_spool_and_one_shot_recovers(
        self,
    ) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = self._serialization(server, shares)
        try:
            # The daemon upload dies mid-splice; the spool is poisoned before
            # falling back, so the fresh one-shot subprocess streams the
            # cached fragments instead of retrying the same failing transfer.
            with patch(
                "lab.prism.prism_coordinator.os.splice",
                side_effect=[16, OSError("splice io error")],
            ):
                result = self._build(server, shares, serialization, height=10)

            self.assertEqual(result["transport"], "one-shot")
            self.assertIn("compact_shares", result["received"])
            self.assertTrue(serialization._spool_failed)  # type: ignore[attr-defined]
            with server._serve_builder_metrics_lock:
                counts = dict(server.serve_builder_counts)
            self.assertEqual(counts["fallbacks"], 1)
        finally:
            server.shutdown_serve_builder()

    def test_protocol_mismatch_falls_back_to_one_shot(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = self._serialization(server, shares)
        try:
            result = self._build(
                server,
                shares,
                serialization,
                height=10,
                mode="protocol-mismatch",
            )

            self.assertEqual(result["transport"], "one-shot")
            with server._serve_builder_metrics_lock:
                counts = dict(server.serve_builder_counts)
                self.assertIsNone(server._serve_builder)
            self.assertEqual(counts["fallbacks"], 1)
            # The mismatched daemon was a real worker launch and must appear
            # in the lifecycle totals alongside the one-shot fallback.
            with server._job_build_scheduler_lock:
                worker_counts = dict(server.job_build_worker_counts)
            self.assertEqual(worker_counts["starts"], 2)
        finally:
            server.shutdown_serve_builder()

    def test_disabled_serve_builder_uses_one_shot_directly(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = self._serialization(server, shares)
        with patch.dict(os.environ, {"PRISM_BUILDER_SERVE": "0"}):
            result = self._build(server, shares, serialization, height=10)

        self.assertEqual(result["transport"], "one-shot")
        with server._serve_builder_metrics_lock:
            counts = dict(server.serve_builder_counts)
        self.assertEqual(counts["spawns"], 0)

    def test_daemon_timeout_fallback_shares_the_build_deadline(self) -> None:
        server = self._coordinator()
        # An already-exhausted budget: the daemon attempt must not grant the
        # one-shot fallback a fresh full deadline, and the timeout is counted
        # as one worker failure, not two.
        server.bundle_build_timeout_seconds = 0.0
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = self._serialization(server, shares)

        with self.assertRaisesRegex(
            RuntimeError,
            "qbit-prism-build-audit-bundle timed out",
        ):
            self._build(
                server,
                shares,
                serialization,
                height=10,
                mode="hang-after-request",
            )

        with server._serve_builder_metrics_lock:
            counts = dict(server.serve_builder_counts)
        self.assertEqual(counts["fallbacks"], 1)
        self.assertEqual(counts["spawns"], 0)
        self.assertEqual(server.tip_refresh_worker_failures, 1)
        # The live daemon killed at the handshake deadline is a recorded
        # termination, and the one-shot replacement consumes the pending
        # restart: two launches, one termination, one restart.
        with server._job_build_scheduler_lock:
            worker_counts = dict(server.job_build_worker_counts)
            self.assertFalse(server._job_build_worker_restart_pending)
        self.assertEqual(worker_counts["starts"], 2)
        self.assertEqual(worker_counts["terminations"], 1)
        self.assertEqual(worker_counts["restarts"], 1)

    def test_idle_daemon_death_counts_crash_and_restart(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = self._serialization(server, shares)
        try:
            first = self._build(server, shares, serialization, height=10)
            self.assertEqual(first["transport"], "serve")
            client = server._serve_builder
            assert client is not None
            client.process.kill()
            client.process.wait(timeout=5)

            second = self._build(server, shares, serialization, height=11)

            self.assertEqual(second["transport"], "serve")
            with server._job_build_scheduler_lock:
                counts = dict(server.job_build_worker_counts)
                self.assertFalse(server._job_build_worker_restart_pending)
            self.assertEqual(counts["crashes"], 1)
            self.assertEqual(counts["restarts"], 1)
            self.assertEqual(server.tip_refresh_worker_restarts, 1)
            with server._serve_builder_metrics_lock:
                serve_counts = dict(server.serve_builder_counts)
            self.assertEqual(serve_counts["spawns"], 2)
            self.assertEqual(serve_counts["fallbacks"], 0)
        finally:
            server.shutdown_serve_builder()

    def test_daemon_respawn_counts_as_worker_restart(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = self._serialization(server, shares)
        try:
            # A prior worker ended abnormally (crash or supersession kill)
            # with no immediate fallback consuming the flag; the replacement
            # daemon spawn must claim the restart instead of leaking it to
            # an unrelated later one-shot build.
            server._ensure_tip_refresh_state()
            with server._job_build_scheduler_lock:
                server._job_build_worker_restart_pending = True

            result = self._build(server, shares, serialization, height=10)

            self.assertEqual(result["transport"], "serve")
            with server._job_build_scheduler_lock:
                counts = dict(server.job_build_worker_counts)
                self.assertFalse(server._job_build_worker_restart_pending)
            self.assertEqual(counts["restarts"], 1)
            self.assertGreaterEqual(counts["starts"], 1)
            self.assertEqual(server.tip_refresh_worker_restarts, 1)
        finally:
            server.shutdown_serve_builder()

    def test_daemon_detaches_from_build_control_after_request(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = self._serialization(server, shares)
        control = prism_coordinator_module._JobBundleBuildControl(
            key=("serve-detach",),
            previousblockhash="11" * 32,
            payout_state_generation=0,
            payout_artifact_generation=1,
        )
        with server._job_cache_lock:
            server._active_job_bundle_builds[control.key] = control
        server._job_build_phase_local.bundle_build_control = control
        try:
            result = self._build(server, shares, serialization, height=10)

            self.assertEqual(result["transport"], "serve")
            # The completed request detached the daemon: a late supersession
            # of this build has no process reference left to terminate.
            self.assertIsNone(control.process)
            control.cancel_event.set()
            client = server._serve_builder
            assert client is not None
            self.assertIsNone(client.process.poll())
        finally:
            server._job_build_phase_local.bundle_build_control = None
            control.cancel_event.set()
            with server._job_cache_lock:
                server._active_job_bundle_builds.pop(control.key, None)
            server.shutdown_serve_builder()

    def test_supersession_cancels_in_flight_daemon_request(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = self._serialization(server, shares)
        control = prism_coordinator_module._JobBundleBuildControl(
            key=("serve-test",),
            previousblockhash="11" * 32,
            payout_state_generation=0,
            payout_artifact_generation=1,
        )
        with server._job_cache_lock:
            server._active_job_bundle_builds[control.key] = control
        errors: list[BaseException] = []
        results: list[object] = []

        def build() -> None:
            # The bundle-build control travels on the builder thread's local
            # state, exactly as _execute_job_build installs it.
            server._job_build_phase_local.bundle_build_control = control
            try:
                results.append(
                    self._build(
                        server,
                        shares,
                        serialization,
                        height=10,
                        mode="hang-after-request",
                    )
                )
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors.append(exc)
            finally:
                server._job_build_phase_local.bundle_build_control = None

        thread = threading.Thread(target=build)
        thread.start()
        try:
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                if control.process is not None:
                    break
                time.sleep(0.005)
            self.assertIsNotNone(control.process)

            control.cancel_event.set()
            thread.join(5.0)
        finally:
            control.cancel_event.set()
            thread.join(1.0)
            server.shutdown_serve_builder()

        self.assertFalse(thread.is_alive())
        self.assertEqual(results, [])
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], JobBuildSuperseded)
        self.assertIsNone(server._serve_builder)


class ReorgReconcileRefreshPathTests(unittest.TestCase):
    """The tip-refresh reconcile stage: memo reuse and fetch overlap."""

    def test_poll_memo_hit_skips_full_reconcile_pass(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.reorg_reconciler_enabled = True
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}
        with server.lock:
            server._ensure_reorg_reconciler_service()._reorg_reconcile_trusted_memo[rpc.tip] = time.monotonic()

        def unexpected_pass(**_kwargs: object) -> dict[str, object]:
            raise AssertionError(
                "memo-armed poll must not queue a serialized reconcile pass"
            )

        server.reconcile_prism_pool_blocks_once = (  # type: ignore[method-assign]
            unexpected_pass
        )

        try:
            refreshed = server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(refreshed, 1)
        # A memo hit never needs the prefetch worker.
        self.assertIsNone(server._ensure_reorg_reconciler_service()._reconcile_prefetch_executor)
        metrics = server.metrics_payload()
        self.assertIn(
            'qbit_prism_reorg_reconcile_lookups_total{path="tip_refresh",source="memo_hit"} 1',
            metrics,
        )
        self.assertIn(
            'qbit_prism_tip_refresh_bundle_phase_seconds_count{phase="reorg_reconcile"} 1',
            metrics,
        )

    def test_poll_overlaps_reconcile_with_template_fetch(self) -> None:
        # Deadlock-free only when the reconcile pass and the template fetch
        # genuinely run concurrently: the fetch refuses to return before the
        # reconcile starts, and the reconcile refuses to return before the
        # fetch starts.
        fetch_started = threading.Event()
        reconcile_running = threading.Event()

        class OverlapProbeRpc(FakeRpc):
            def call(
                self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblocktemplate":
                    fetch_started.set()
                    assert reconcile_running.wait(5.0), (
                        "reconcile prefetch did not start while the template "
                        "fetch was in flight"
                    )
                return super().call(method, params)

        template = base_template()
        rpc = OverlapProbeRpc(template, tip=str(template["previousblockhash"]))
        server, _ = coordinator(template=template)
        server.rpc = rpc
        install_fake_bundle_builder(server)
        server.reorg_reconciler_enabled = True
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}
        reconciled_tips: list[str] = []

        def fake_ensure(tip_hash: str) -> bool:
            reconciled_tips.append(tip_hash)
            reconcile_running.set()
            assert fetch_started.wait(5.0), (
                "template fetch did not start while the reconcile pass was "
                "in flight"
            )
            # A real trusted pass arms the per-tip memo; the join validates
            # the overlapped result against that armed entry.
            with server.lock:
                server._ensure_reorg_reconciler_service()._reorg_reconcile_trusted_memo[tip_hash] = time.monotonic()
            return True

        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            fake_ensure
        )

        try:
            refreshed = server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(refreshed, 1)
        self.assertEqual(reconciled_tips, [rpc.tip])
        metrics = server.metrics_payload()
        self.assertIn(
            'qbit_prism_reorg_reconcile_lookups_total{path="tip_refresh",source="overlap"} 1',
            metrics,
        )

    def test_poll_reconciles_snapshot_tip_serially_when_tip_moves_before_fetch(
        self,
    ) -> None:
        # The cheap probe observed a tip that was replaced before the
        # template fetch. The prefetched pass proved the superseded hash, so
        # the poll must reconcile the snapshot's newer tip on its own thread.
        template = base_template()
        new_tip = str(template["previousblockhash"])
        old_tip = "ab" * 32

        class MovingTipRpc(FakeRpc):
            def __init__(self) -> None:
                super().__init__(template, tip=new_tip)
                self.best_tip_calls = 0

            def call(
                self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getbestblockhash":
                    self.best_tip_calls += 1
                    if self.best_tip_calls == 1:
                        self.calls.append(method)
                        return old_tip
                return super().call(method, params)

        rpc = MovingTipRpc()
        server, _ = coordinator(template=template)
        server.rpc = rpc
        install_fake_bundle_builder(server)
        server.reorg_reconciler_enabled = True
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}
        reconcile_calls: list[str] = []
        prefetch_drained = threading.Event()

        def fake_ensure(tip_hash: str) -> bool:
            reconcile_calls.append(tip_hash)
            if tip_hash == old_tip:
                prefetch_drained.set()
            return True

        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            fake_ensure
        )

        try:
            refreshed = server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(refreshed, 1)
        self.assertTrue(prefetch_drained.wait(5.0))
        self.assertIn(new_tip, reconcile_calls)
        self.assertEqual(
            sorted(reconcile_calls),
            sorted([old_tip, new_tip]),
        )
        metrics = server.metrics_payload()
        self.assertIn(
            'qbit_prism_reorg_reconcile_lookups_total{path="tip_refresh",source="serial"} 1',
            metrics,
        )

    def test_poll_reproves_when_memo_evicted_during_template_fetch(self) -> None:
        # A tip that flips away and back during the template fetch evicts
        # the memo entry so post-flip pool-block state is re-proven. The
        # join must judge the memo at join time, never the pre-fetch probe.
        template = base_template()
        server, _ = coordinator(template=template)
        server.reorg_reconciler_enabled = True
        tip = str(template["previousblockhash"])

        class EvictingRpc(FakeRpc):
            def call(
                self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblocktemplate":
                    # Models a detection during the fetch unarming the memo.
                    with server.lock:
                        server._ensure_reorg_reconciler_service()._reorg_reconcile_trusted_memo.clear()
                return super().call(method, params)

        rpc = EvictingRpc(template, tip=tip)
        server.rpc = rpc
        install_fake_bundle_builder(server)
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}
        with server.lock:
            server._ensure_reorg_reconciler_service()._reorg_reconcile_trusted_memo[tip] = time.monotonic()
        reconcile_calls: list[str] = []

        def fake_ensure(tip_hash: str) -> bool:
            reconcile_calls.append(tip_hash)
            return True

        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            fake_ensure
        )

        try:
            refreshed = server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(refreshed, 1)
        self.assertEqual(reconcile_calls, [tip])
        metrics = server.metrics_payload()
        self.assertIn(
            'qbit_prism_reorg_reconcile_lookups_total{path="tip_refresh",source="serial"} 1',
            metrics,
        )
        self.assertIn(
            'qbit_prism_reorg_reconcile_lookups_total{path="tip_refresh",source="memo_hit"} 0',
            metrics,
        )

    def test_poll_reproves_when_memo_rearmed_across_detection_epoch(self) -> None:
        # A pass whose execution straddles a flip away and back can re-arm
        # the memo after the flip-back (the latest detected hash matches
        # again), but its proof belongs to the closed epoch. The join must
        # refuse memo entries when the detection epoch moved mid-fetch.
        template = base_template()
        server, _ = coordinator(template=template)
        server.reorg_reconciler_enabled = True
        tip = str(template["previousblockhash"])

        class FlipBackRpc(FakeRpc):
            def call(
                self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblocktemplate":
                    # Models a flip away and back detected during the fetch
                    # (two epoch bumps) with a straddling pass re-arming the
                    # memo for the same hash afterwards.
                    with server.lock:
                        server.tip_detection_epoch = (
                            int(getattr(server, "tip_detection_epoch", 0)) + 2
                        )
                        server._ensure_reorg_reconciler_service()._reorg_reconcile_trusted_memo[tip] = (
                            time.monotonic()
                        )
                return super().call(method, params)

        rpc = FlipBackRpc(template, tip=tip)
        server.rpc = rpc
        install_fake_bundle_builder(server)
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}
        with server.lock:
            server._ensure_reorg_reconciler_service()._reorg_reconcile_trusted_memo[tip] = time.monotonic()
        reconcile_calls: list[str] = []

        def fake_ensure(tip_hash: str) -> bool:
            reconcile_calls.append(tip_hash)
            return True

        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            fake_ensure
        )

        try:
            refreshed = server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(refreshed, 1)
        self.assertEqual(reconcile_calls, [tip])
        metrics = server.metrics_payload()
        self.assertIn(
            'qbit_prism_reorg_reconcile_lookups_total{path="tip_refresh",source="serial"} 1',
            metrics,
        )

    def test_overlap_join_blocks_when_trust_flips_after_prefetch(self) -> None:
        # The prefetched pass proves trust when it runs, up to a fetch
        # window earlier. Headers running ahead without a best-hash change
        # (an arriving reorg, no detection) must still block the refresh at
        # the join, matching the memo branch's live trust check.
        template = base_template()
        server, _ = coordinator(template=template)
        server.reorg_reconciler_enabled = True
        tip = str(template["previousblockhash"])

        class TrustFlipRpc(FakeRpc):
            def call(
                self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblocktemplate":
                    # Headers run ahead after the fetch: untrusted view with
                    # no tip detection (epoch unchanged).
                    self.blockchain_info["headers"] = 101
                return super().call(method, params)

        rpc = TrustFlipRpc(template, tip=tip)
        server.rpc = rpc
        install_fake_bundle_builder(server)
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}
        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            lambda _tip: True
        )

        try:
            with self.assertRaises(TemplateRefreshBlocked) as raised:
                server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertIn("untrusted", str(raised.exception))
        self.assertIsNone(state.active_job)

    def test_tip_refresh_join_bounds_a_stalled_reconcile_prefetch(self) -> None:
        # The overlap join must never park the poll loop on a crawling
        # prefetched pass: it times out into the normal blocked-retry path
        # within the CONFIGURED budget (not merely eventually), retries
        # re-join the identical still-running future without starting
        # another pass, and the pass completing unblocks the next poll.
        server, rpc = coordinator()
        server.reorg_reconciler_enabled = True
        server.reconcile_prefetch_join_timeout_seconds = 0.05
        started = threading.Event()
        release = threading.Event()
        pass_calls = [0]

        def slow_ensure(tip_hash: str) -> bool:
            pass_calls[0] += 1
            started.set()
            assert release.wait(10.0)
            return True

        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            slow_ensure
        )
        try:
            join_started = time.monotonic()
            with self.assertRaises(TemplateRefreshBlocked):
                server.poll_qbit_tip_template_once()
            # Generous scheduler tolerance, but far below any hardcoded
            # multi-second budget: the configured 0.05s must be what fired.
            self.assertLess(time.monotonic() - join_started, 1.0)
            self.assertTrue(started.is_set())
            with server._ensure_reorg_reconciler_service()._reconcile_prefetch_executor_lock:
                pending = server._ensure_reorg_reconciler_service()._reconcile_prefetch_pending
            self.assertIsNotNone(pending)
            assert pending is not None
            first_future = pending[1]
            self.assertFalse(first_future.done())

            # A retry re-joins the same running future: same slot identity,
            # no additional pass started.
            with self.assertRaises(TemplateRefreshBlocked):
                server.poll_qbit_tip_template_once()
            with server._ensure_reorg_reconciler_service()._reconcile_prefetch_executor_lock:
                pending_again = server._ensure_reorg_reconciler_service()._reconcile_prefetch_pending
            assert pending_again is not None
            self.assertIs(pending_again[1], first_future)
            self.assertEqual(pass_calls[0], 1)

            release.set()
            self.assertTrue(first_future.result(5.0))
            server.poll_qbit_tip_template_once()
        finally:
            release.set()
            server.shutdown_reconcile_prefetch_executor()

    def test_serial_reconcile_branch_uses_the_bounded_join(self) -> None:
        # With the memo disabled no overlap prefetch exists, so the pass
        # lands in the serial re-prove branch -- which must route through
        # the same prefetch slot and bounded join instead of parking the
        # poll loop synchronously inside a crawling pass.
        server, rpc = coordinator()
        server.reorg_reconciler_enabled = True
        server.reorg_reconcile_cache_seconds = 0.0
        server.reconcile_prefetch_join_timeout_seconds = 0.05
        started = threading.Event()
        release = threading.Event()

        def slow_ensure(tip_hash: str) -> bool:
            started.set()
            assert release.wait(10.0)
            return True

        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            slow_ensure
        )
        try:
            join_started = time.monotonic()
            with self.assertRaises(TemplateRefreshBlocked):
                server.poll_qbit_tip_template_once()
            self.assertLess(time.monotonic() - join_started, 1.0)
            self.assertTrue(started.is_set())
            with server._ensure_reorg_reconciler_service()._reconcile_prefetch_executor_lock:
                pending = server._ensure_reorg_reconciler_service()._reconcile_prefetch_pending
            self.assertIsNotNone(pending)
            assert pending is not None
            self.assertFalse(pending[1].done())

            release.set()
            self.assertTrue(pending[1].result(5.0))
            server.poll_qbit_tip_template_once()
        finally:
            release.set()
            server.shutdown_reconcile_prefetch_executor()

    def test_pass_raised_timeout_is_not_logged_as_join_expiry(self) -> None:
        # socket.timeout IS builtins.TimeoutError: a pass that raises it
        # completes the future exceptionally and must surface as an ordinary
        # blocked pass -- not as a join expiry, which would misdirect
        # operators toward the join while nothing is running.
        server, rpc = coordinator()
        server.reorg_reconciler_enabled = True
        server.reorg_reconcile_cache_seconds = 0.0
        server.reconcile_prefetch_join_timeout_seconds = 5.0

        def timing_out_ensure(tip_hash: str) -> bool:
            raise TimeoutError("pass-owned timeout")

        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            timing_out_ensure
        )
        try:
            captured = io.StringIO()
            join_started = time.monotonic()
            with contextlib.redirect_stdout(captured):
                with self.assertRaises(TemplateRefreshBlocked):
                    server.poll_qbit_tip_template_once()
            self.assertLess(time.monotonic() - join_started, 2.0)
            self.assertNotIn("join exceeded", captured.getvalue())
        finally:
            server.shutdown_reconcile_prefetch_executor()

    def test_reconcile_prefetch_slot_is_reused_across_failed_attempts(self) -> None:
        # A refresh attempt that dies before its join (template-RPC outage)
        # must not queue another serialized pass per retry: the slot holds
        # at most one outstanding prefetch, reused for the same tip and
        # replaced (with the queued task cancelled) on a tip change.
        server, _ = coordinator()
        server.reorg_reconciler_enabled = True
        started = threading.Event()
        release = threading.Event()

        def slow_ensure(tip_hash: str) -> bool:
            started.set()
            assert release.wait(5.0)
            return True

        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            slow_ensure
        )
        try:
            first = server._submit_reconcile_prefetch("aa" * 32)
            assert first is not None
            self.assertTrue(started.wait(5.0))
            second = server._submit_reconcile_prefetch("aa" * 32)
            self.assertIs(second, first)
            third = server._submit_reconcile_prefetch("bb" * 32)
            assert third is not None
            self.assertIsNot(third, first)
            fourth = server._submit_reconcile_prefetch("cc" * 32)
            assert fourth is not None
            self.assertIsNot(fourth, third)
            self.assertTrue(third.cancelled())
            release.set()
            self.assertTrue(first.result(5.0))
            self.assertTrue(fourth.result(5.0))
        finally:
            release.set()
            server.shutdown_reconcile_prefetch_executor()

    def test_job_build_memo_lookups_are_counted(self) -> None:
        server, rpc = coordinator()
        server.reorg_reconciler_enabled = True
        with server.lock:
            server._ensure_reorg_reconciler_service()._reorg_reconcile_trusted_memo[rpc.tip] = time.monotonic()

        self.assertTrue(server.ensure_reorg_reconciled_for_current_tip())

        with server.lock:
            server._ensure_reorg_reconciler_service()._reorg_reconcile_trusted_memo[rpc.tip] = (
                time.monotonic() - 100.0
            )
        serial_tips: list[str] = []

        def fake_ensure(tip_hash: str) -> bool:
            serial_tips.append(tip_hash)
            return True

        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            fake_ensure
        )

        self.assertTrue(server.ensure_reorg_reconciled_for_current_tip())

        self.assertEqual(serial_tips, [rpc.tip])
        metrics = server.metrics_payload()
        self.assertIn(
            'qbit_prism_reorg_reconcile_lookups_total{path="job_build",source="memo_hit"} 1',
            metrics,
        )
        self.assertIn(
            'qbit_prism_reorg_reconcile_lookups_total{path="job_build",source="serial"} 1',
            metrics,
        )

    def test_poll_falls_back_to_serial_reconcile_after_prefetch_shutdown(
        self,
    ) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.reorg_reconciler_enabled = True
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}
        server.shutdown_reconcile_prefetch_executor()
        reconcile_calls: list[str] = []

        def fake_ensure(tip_hash: str) -> bool:
            reconcile_calls.append(tip_hash)
            return True

        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            fake_ensure
        )

        try:
            refreshed = server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(refreshed, 1)
        self.assertEqual(reconcile_calls, [rpc.tip])
        self.assertIsNone(server._ensure_reorg_reconciler_service()._reconcile_prefetch_executor)
        metrics = server.metrics_payload()
        self.assertIn(
            'qbit_prism_reorg_reconcile_lookups_total{path="tip_refresh",source="serial"} 1',
            metrics,
        )


class JobContextStampTests(unittest.TestCase):
    def test_job_context_stamps_client_version_mask(self) -> None:
        # Cross-connection resumes validate version bits against the mask
        # negotiated on the delivering connection, so the context must
        # record it at stamp time.
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        state.version_mask = 0x1FFFE000

        context = server.build_job_for_client(state, clean_jobs=True)

        self.assertEqual(context.version_mask, 0x1FFFE000)


class ExternallyTerminatedBuilderTests(unittest.TestCase):
    """A build superseded while its helper runs terminates without a crash."""

    def test_superseded_running_builder_terminates_without_crash(self) -> None:
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

        server.build_shared_job_bundle = build_with_control  # type: ignore[method-assign]

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
                with server._job_cache_lock:
                    controls = list(server._active_job_bundle_builds.values())
                self.assertEqual(len(controls), 1)
                # Supersession lands while the helper is still running; the
                # compiler owns the terminate/kill sequence itself, so this
                # never counts as a worker crash.
                controls[0].cancel_event.set()
                return None

            process.poll = poll  # type: ignore[method-assign]
            del original_wait
            return process

        with patch(
            "lab.prism.prism_coordinator.prism_tool_command",
            return_value=[sys.executable, "-c", "import time; time.sleep(30)"],
        ), patch(
            "lab.prism.prism_coordinator.subprocess.Popen",
            side_effect=cancel_then_exit,
        ):
            with self.assertRaises(JobBuildSuperseded):
                server.shared_job_bundle(
                    artifacts,
                    mode="ready",
                    retry_superseded=False,
                )

        service = server._ensure_job_bundle_service()
        self.assertEqual(service.shared_bundle_build_counts["superseded"], 1)
        self.assertEqual(service.shared_bundle_build_counts["failed"], 0)
        self.assertEqual(server.job_build_worker_counts["terminations"], 1)
        self.assertEqual(server.job_build_worker_counts["crashes"], 0)
        self.assertEqual(server.job_build_failure_count, 0)
