#!/usr/bin/env python3
"""Deterministic disconnect races for representative-free PRISM refreshes."""

from __future__ import annotations

import threading
import time
import unittest

from lab.prism.prism_coordinator import (
    CachedJobBundle,
    CachedTemplateArtifacts,
    CollectionIdentityUnavailable,
    StratumError,
    TemplateRefreshBlocked,
    WorkerIdentity,
)
from tests.prism_coordinator_test_support import (
    FakeLedger,
    base_template,
    client,
    coordinator,
    install_fake_bundle_builder,
    worker,
)


class RepresentativeIndependentRefreshTests(unittest.TestCase):
    def _run_ready_disconnect_race(self, stage: str) -> None:
        server, rpc = coordinator()
        recorded = install_fake_bundle_builder(server)
        disconnected = client(1)
        survivor = client(2)
        server.clients = {disconnected, survivor}
        survivor_payloads: list[dict[str, object]] = []
        survivor.send = survivor_payloads.append  # type: ignore[method-assign]
        disconnected.send = lambda _payload: self.fail(  # type: ignore[method-assign]
            "disconnected target received prepared work"
        )

        def remove_target() -> None:
            with server.lock:
                server.clients.discard(disconnected)

        if stage == "before_build":
            original_shared_job_bundle = server.shared_job_bundle

            def disconnect_before_build(
                artifacts: CachedTemplateArtifacts,
                identity: WorkerIdentity | None = None,
                **kwargs: object,
            ) -> CachedJobBundle:
                remove_target()
                return original_shared_job_bundle(artifacts, identity, **kwargs)

            server.shared_job_bundle = disconnect_before_build  # type: ignore[method-assign]
        elif stage == "during_build":
            original_build_audit_bundle = server.build_audit_bundle

            def disconnect_during_build(**kwargs: object) -> dict[str, object]:
                remove_target()
                return original_build_audit_bundle(**kwargs)

            server.build_audit_bundle = disconnect_during_build  # type: ignore[method-assign]
        elif stage == "before_fanout":
            original_fanout = server._fanout_prepared_tip_refresh

            def disconnect_before_fanout(*args: object, **kwargs: object) -> object:
                remove_target()
                return original_fanout(*args, **kwargs)

            server._fanout_prepared_tip_refresh = disconnect_before_fanout  # type: ignore[method-assign]
        else:  # pragma: no cover - test helper guard
            raise AssertionError(f"unknown race stage {stage}")

        try:
            refreshed = server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(refreshed, 1)
        self.assertEqual(recorded["calls"], 1)
        self.assertEqual(rpc.count("getblocktemplate"), 1)
        self.assertEqual(server.ledger.snapshot_calls, 1)
        self.assertEqual(
            [payload["method"] for payload in survivor_payloads],
            ["mining.set_difficulty", "mining.notify"],
        )
        self.assertEqual(server.tip_refresh_client_counts["disconnected"], 1)
        self.assertIsNotNone(survivor.active_job)
        assert survivor.active_job is not None
        self.assertFalse(survivor.active_job.collection_only)

    def test_target_disconnect_immediately_before_ready_bundle_construction(self) -> None:
        self._run_ready_disconnect_race("before_build")

    def test_target_disconnect_during_ready_bundle_construction(self) -> None:
        self._run_ready_disconnect_race("during_build")

    def test_target_disconnect_after_ready_construction_before_fanout(self) -> None:
        self._run_ready_disconnect_race("before_fanout")

    def test_reselected_poll_start_target_keeps_expected_job_snapshot(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        stable = client(1)
        reselected = client(2)
        server.clients = {stable, reselected}
        original_fetch = server.fetch_qbit_tip_template_snapshot

        def make_target_temporarily_ineligible() -> object:
            snapshot = original_fetch()
            reselected.authorized = False
            return snapshot

        def restore_target(_tip_hash: str) -> bool:
            reselected.authorized = True
            return True

        captured_expected: dict[object, object] = {}

        def capture_fanout(
            clients: list[object],
            *_args: object,
            expected_active_jobs: dict[object, object],
            **_kwargs: object,
        ) -> tuple[int, float, float, int]:
            captured_expected.update(expected_active_jobs)
            delivered = time.monotonic()
            return len(clients), delivered, delivered, 0

        server.fetch_qbit_tip_template_snapshot = make_target_temporarily_ineligible  # type: ignore[method-assign]
        server.ensure_reorg_reconciled_for_tip = restore_target  # type: ignore[method-assign]
        server._fanout_prepared_tip_refresh = capture_fanout  # type: ignore[method-assign]
        try:
            refreshed = server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(refreshed, 2)
        self.assertIn(stable, captured_expected)
        self.assertIn(reselected, captured_expected)

    def test_ready_bundle_builds_without_clients_or_worker_identity(self) -> None:
        server, _rpc = coordinator()
        recorded = install_fake_bundle_builder(server)
        snapshot = server.fetch_qbit_tip_template_snapshot()
        server.clients = set()

        bundle = server.prepare_tip_refresh_bundle(snapshot)
        cached = server.prepare_tip_refresh_bundle(snapshot)

        self.assertFalse(bundle.collection_only)
        self.assertIsNone(bundle.collection_identity)
        self.assertIs(cached, bundle)
        self.assertEqual(recorded["calls"], 1)
        self.assertEqual(server.ledger.snapshot_calls, 1)

    def test_collection_ineligible_connected_target_is_skipped(self) -> None:
        server, _rpc = coordinator(ledger=FakeLedger(miners=["solo"]))
        state = client(1)
        server.clients = {state}
        original_observe_tip = server.observe_tip_first_seen

        def make_target_ineligible(*args: object, **kwargs: object) -> bool:
            observed = original_observe_tip(*args, **kwargs)
            state.authorized = False
            return observed

        server.observe_tip_first_seen = make_target_ineligible  # type: ignore[method-assign]
        server.maybe_send_job = lambda *_args, **_kwargs: self.fail(  # type: ignore[method-assign]
            "ineligible collection target received work"
        )

        try:
            refreshed = server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(refreshed, 0)
        self.assertEqual(server.tip_refresh_client_counts["skipped"], 1)
        self.assertEqual(server.tip_refresh_client_counts["disconnected"], 0)

    def test_collection_identity_absence_has_a_distinct_temporary_result(self) -> None:
        server, _rpc = coordinator(ledger=FakeLedger(miners=["solo"]))
        artifacts = server.current_template_artifacts()

        with self.assertRaisesRegex(
            CollectionIdentityUnavailable,
            "temporarily unavailable",
        ):
            server.shared_job_bundle(artifacts, mode="collection")

    def test_all_collection_identities_disappear_then_authorization_reuses_artifacts(
        self,
    ) -> None:
        server, rpc = coordinator(ledger=FakeLedger(miners=["solo"]))
        recorded = install_fake_bundle_builder(server)
        server.template_cache_seconds = 0.0
        original = client(1)
        original.send = lambda _payload: None  # type: ignore[method-assign]
        original.close = lambda: None  # type: ignore[method-assign]
        server.clients = {original}

        self.assertEqual(server.poll_qbit_tip_template_once(), 1)
        self.assertIsNone(server._retained_collection_refresh)
        server.disconnect_client(original)
        retained = server._retained_collection_refresh
        self.assertIsNotNone(retained)
        self.assertEqual(rpc.count("getblocktemplate"), 1)
        self.assertEqual(recorded["calls"], 1)

        authorized = client(3, original.worker)
        authorized.authorized = False
        sent: list[dict[str, object]] = []
        authorized.send = sent.append  # type: ignore[method-assign]
        server.clients.add(authorized)
        authorized.authorized = True
        server._note_collection_identity_available(authorized)

        self.assertTrue(server.tip_refresh_is_pending())
        self.assertTrue(server._tip_refresh_retry.is_set())
        self.assertTrue(server.maybe_send_job(authorized, clean_jobs=True))
        self.assertEqual(rpc.count("getblocktemplate"), 1)
        self.assertEqual(recorded["calls"], 1)
        self.assertEqual(
            [payload["method"] for payload in sent],
            ["mining.set_difficulty", "mining.notify"],
        )
        self.assertIsNotNone(authorized.active_job)
        assert authorized.active_job is not None and retained is not None
        self.assertTrue(authorized.active_job.collection_only)
        self.assertIs(
            authorized.active_job.template,
            retained.snapshot.template_artifacts.template,
        )
        self.assertIsNone(server._retained_collection_refresh)

        pending_token = server._tip_refresh_pending_token
        if pending_token is not None:
            server._clear_tip_refresh_pending(pending_token)
        server._tip_refresh_retry.clear()
        server._note_collection_identity_available(authorized)

        self.assertFalse(server.tip_refresh_is_pending())
        self.assertFalse(server._tip_refresh_retry.is_set())

    def test_authorization_during_same_tip_publication_keeps_retained_wake(
        self,
    ) -> None:
        server, rpc = coordinator(ledger=FakeLedger(miners=["solo"]))
        recorded = install_fake_bundle_builder(server)
        server.template_cache_seconds = 0.0
        server.clients = set()

        self.assertEqual(server.poll_qbit_tip_template_once(), 0)
        retained = server._retained_collection_refresh
        self.assertIsNotNone(retained)
        self.assertEqual(rpc.count("getblocktemplate"), 1)
        server._tip_refresh_retry.clear()

        publication_window = threading.Event()
        release_publication = threading.Event()
        original_observe_tip = server.observe_tip_first_seen

        def pause_after_same_tip_observation(
            *args: object,
            **kwargs: object,
        ) -> bool:
            observed = original_observe_tip(*args, **kwargs)
            if kwargs.get("publish_refresh_observation"):
                publication_window.set()
                self.assertTrue(release_publication.wait(5.0))
            return observed

        server.observe_tip_first_seen = pause_after_same_tip_observation  # type: ignore[method-assign]
        poll_results: list[int] = []
        poll_errors: list[BaseException] = []

        def poll_same_tip() -> None:
            try:
                poll_results.append(server.poll_qbit_tip_template_once())
            except BaseException as exc:  # pragma: no cover - asserted below
                poll_errors.append(exc)

        try:
            poll_thread = threading.Thread(target=poll_same_tip)
            poll_thread.start()
            authorized = client(2)
            sent: list[dict[str, object]] = []
            authorized.send = sent.append  # type: ignore[method-assign]
            try:
                self.assertTrue(publication_window.wait(5.0))
                with server.lock:
                    server.clients.add(authorized)
                server._note_collection_identity_available(authorized)

                self.assertTrue(server.tip_refresh_is_pending())
                self.assertTrue(server._tip_refresh_retry.is_set())
                self.assertTrue(server.maybe_send_job(authorized, clean_jobs=True))
                self.assertEqual(rpc.count("getblocktemplate"), 2)
                self.assertEqual(recorded["calls"], 1)
                self.assertIsNotNone(authorized.active_job)
                with server.lock:
                    published_snapshot = server.tip_template_snapshot
                assert (
                    authorized.active_job is not None
                    and published_snapshot is not None
                    and published_snapshot.template_artifacts is not None
                )
                self.assertIs(
                    authorized.active_job.template,
                    published_snapshot.template_artifacts.template,
                )
            finally:
                release_publication.set()
                poll_thread.join(5.0)

            self.assertFalse(poll_thread.is_alive())
            self.assertEqual(poll_results, [])
            self.assertEqual(len(poll_errors), 1)
            self.assertIsInstance(poll_errors[0], TemplateRefreshBlocked)
            # Authorization minted a newer wake token while this poll owned
            # the previous state. The first poll must not clear that work.
            self.assertTrue(server.tip_refresh_is_pending())
            self.assertTrue(server._tip_refresh_retry.is_set())
            self.assertEqual(server.poll_qbit_tip_template_once(), 0)
            self.assertFalse(server.tip_refresh_is_pending())
            self.assertIsNone(server._retained_collection_refresh)
            self.assertEqual(
                [payload["method"] for payload in sent],
                ["mining.set_difficulty", "mining.notify"],
            )
            self.assertIsNotNone(authorized.active_job)
        finally:
            release_publication.set()
            server.shutdown_tip_refresh_executor()

    def test_ready_latch_discards_retained_collection_marker(self) -> None:
        ledger = FakeLedger(miners=["solo"])
        server, _rpc = coordinator(ledger=ledger)
        server.clients = set()

        self.assertEqual(server.poll_qbit_tip_template_once(), 0)
        self.assertIsNotNone(server._retained_collection_refresh)
        ledger.miners = ["miner-a", "miner-b", "miner-c"]

        self.assertTrue(server.pool_readiness_latched())
        self.assertIsNone(server._retained_collection_refresh)

    def test_collection_reauthorization_reselects_identity_without_template_refetch(
        self,
    ) -> None:
        server, rpc = coordinator(ledger=FakeLedger(miners=["solo"]))
        recorded = install_fake_bundle_builder(server)
        worker_a = worker(payout="tq1worker-a", username="worker-a")
        worker_b = worker(payout="tq1worker-b", username="worker-b")
        state = client(1, worker_a)
        artifacts = server.current_template_artifacts()
        original_shared_job_bundle = server.shared_job_bundle
        calls = 0

        def reauthorize_after_first_bundle(
            build_artifacts: CachedTemplateArtifacts,
            identity: WorkerIdentity | None = None,
            **kwargs: object,
        ) -> CachedJobBundle:
            nonlocal calls
            calls += 1
            bundle = original_shared_job_bundle(
                build_artifacts,
                identity,
                **kwargs,
            )
            if calls == 1:
                state.worker = worker_b
                state.username = worker_b.username
            return bundle

        server.shared_job_bundle = reauthorize_after_first_bundle  # type: ignore[method-assign]

        context = server.build_job_for_client_from_artifacts(
            state,
            artifacts,
            clean_jobs=True,
        )

        self.assertEqual(calls, 2)
        self.assertEqual(recorded["calls"], 2)
        self.assertEqual(rpc.count("getblocktemplate"), 1)
        self.assertIs(context.worker, worker_b)
        self.assertEqual(
            context.found_block["coinbase_value_sats"],
            base_template()["coinbasevalue"],
        )
        self.assertEqual(
            recorded["last_kwargs"]["shares"][0]["miner_id"],
            worker_b.payout_address,
        )

    def test_collection_bundle_cannot_be_stamped_across_worker_identities(self) -> None:
        server, _rpc = coordinator(ledger=FakeLedger(miners=["solo"]))
        install_fake_bundle_builder(server)
        worker_a = worker(payout="tq1worker-a")
        worker_b = worker(payout="tq1worker-b")
        state = client(1, worker_a)
        artifacts = server.current_template_artifacts()
        bundle_a = server.shared_job_bundle(artifacts, worker_a)

        state.worker = worker_b
        state.username = worker_b.username

        with self.assertRaisesRegex(StratumError, "no longer matches"):
            server.stamp_job_for_client(state, bundle_a, clean_jobs=True)

    def test_new_tip_supersedes_retained_collection_preparation(self) -> None:
        old_tip = "11" * 32
        new_tip = "22" * 32
        server, rpc = coordinator(
            ledger=FakeLedger(miners=["solo"]),
            template=base_template(height=10, prevhash=old_tip),
        )
        install_fake_bundle_builder(server)
        server.clients = set()

        self.assertEqual(server.poll_qbit_tip_template_once(), 0)
        old_retained = server._retained_collection_refresh
        self.assertIsNotNone(old_retained)

        rpc.tip = new_tip
        rpc.template = base_template(height=11, prevhash=new_tip)
        self.assertTrue(server.observe_tip_first_seen(new_tip))
        self.assertIsNone(server._retained_collection_refresh)

        self.assertEqual(server.poll_qbit_tip_template_once(), 0)
        current = server._retained_collection_refresh
        self.assertIsNotNone(current)
        assert current is not None and old_retained is not None
        self.assertIsNot(current.snapshot, old_retained.snapshot)
        self.assertEqual(current.snapshot.bestblockhash, new_tip)
        self.assertEqual(current.snapshot.previousblockhash, new_tip)
        self.assertEqual(rpc.count("getblocktemplate"), 2)


if __name__ == "__main__":
    unittest.main()
