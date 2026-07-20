#!/usr/bin/env python3
"""Startup prewarm and reconnect-storm initial job delivery regressions."""

from __future__ import annotations

import threading
import time
import unittest

from lab.prism.prism_coordinator import (
    PendingInitialJob,
    PRISM_JOB_EXTRANONCE1_PLACEHOLDER_HEX,
    TemplateRefreshBlocked,
)
from tests.prism_coordinator_test_support import (
    EXTRANONCE2_SIZE,
    ObservedRLock,
    base_template,
    client,
    coordinator,
    install_fake_bundle_builder,
    worker,
)


def wait_until(predicate: object, *, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():  # type: ignore[operator]
            return
        time.sleep(0.01)
    raise AssertionError("condition did not become true before the deadline")


class PrismInitialJobDeliveryTests(unittest.TestCase):
    def test_blocked_startup_prewarm_defers_to_background_refresh(self) -> None:
        server, _rpc = coordinator()
        def blocked() -> object:
            raise TemplateRefreshBlocked("template raced startup")

        tip_refresh = server._ensure_tip_refresh_service()
        tip_refresh.prewarm_current_tip_ready_bundle = blocked  # type: ignore[method-assign]

        self.assertIsNone(server.prewarm_startup_jobs())
        self.assertTrue(tip_refresh.snapshot().retry_requested)

    def test_startup_prewarm_builds_worker_independent_ready_bundle(self) -> None:
        server, rpc = coordinator()
        recorded = install_fake_bundle_builder(server)

        bundle = server.prewarm_current_tip_ready_bundle()

        self.assertIsNotNone(bundle)
        assert bundle is not None
        self.assertFalse(bundle.collection_only)
        self.assertEqual(
            bundle.key,
            (
                bundle.template_fingerprint,
                bundle.template["previousblockhash"],
                "ready",
                0,
                0,
            ),
        )
        self.assertEqual(recorded["calls"], 1)
        self.assertEqual(server.ledger.snapshot_calls, 1)
        self.assertEqual(server.tip_template_snapshot.bestblockhash, rpc.tip)
        self.assertIs(server._ensure_job_bundle_service()._prepared_ready_bundle, bundle)
        self.assertEqual(
            recorded["suffixes"],
            [
                server.coinbase_tag_hex
                + PRISM_JOB_EXTRANONCE1_PLACEHOLDER_HEX
                + "00" * EXTRANONCE2_SIZE
            ],
        )

    def test_initial_delivery_vardiff_wait_does_not_hold_coordinator_lock(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        bundle = server.prewarm_current_tip_ready_bundle()
        assert bundle is not None
        artifacts = server.current_template_artifacts()
        state = client(1)
        state.authorization_generation = 1
        state.difficulty_generation = 0
        state.authorized_monotonic = time.monotonic()
        state.send = lambda _payload: None  # type: ignore[method-assign]
        vardiff_lock = ObservedRLock()
        state.vardiff_lock = vardiff_lock  # type: ignore[assignment]
        server.clients = {state}
        server._ensure_initial_job_state()
        request = PendingInitialJob(
            client=state,
            authorization_generation=1,
            worker=state.worker,
            requested_monotonic=time.monotonic(),
            deadline_monotonic=None,
            connection_id=state.connection_id,
            difficulty_generation=0,
        )
        server.pending_initial_jobs[state] = request
        results: list[bool | None] = []

        with vardiff_lock:
            vardiff_lock.acquire_attempted.clear()
            vardiff_lock.observe_acquires = True
            delivery = threading.Thread(
                target=lambda: results.append(
                    server._deliver_initial_bundle(request, artifacts, bundle)
                )
            )
            delivery.start()
            self.assertTrue(vardiff_lock.acquire_attempted.wait(2))
            acquired = server.lock.acquire(timeout=0.25)
            self.assertTrue(acquired)
            if acquired:
                server.lock.release()

        delivery.join(2)
        self.assertFalse(delivery.is_alive())
        self.assertEqual(results, [True])

    def test_initial_delivery_retries_transient_reorg_and_build_failures(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        server.prewarm_current_tip_ready_bundle()
        state = client(1)
        state.authorization_generation = 1
        state.authorized_monotonic = time.monotonic()
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}

        reorg_attempts = 0

        def transient_reorg() -> bool:
            nonlocal reorg_attempts
            reorg_attempts += 1
            return reorg_attempts > 1

        artifact_attempts = 0
        repository = server._ensure_job_bundle_service().template_repository
        original_artifacts = repository.current

        def transient_artifacts() -> object:
            nonlocal artifact_attempts
            artifact_attempts += 1
            if artifact_attempts == 1:
                raise RuntimeError("temporary template failure")
            return original_artifacts()

        server.ensure_reorg_reconciled_for_current_tip = transient_reorg  # type: ignore[method-assign]
        repository.current = transient_artifacts  # type: ignore[method-assign]

        server.request_initial_job_delivery(state)
        try:
            wait_until(lambda: state.active_job is not None)
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(reorg_attempts, 3)
        self.assertEqual(artifact_attempts, 2)
        self.assertEqual(server._ensure_job_bundle_service().metrics_snapshot()["failure_count"], 1)
        self.assertIn(state, server.clients)
        self.assertEqual(
            server.progress_health_snapshot()["eligible_clients_requiring_refresh"],
            0,
        )

    def test_initial_delivery_backs_off_after_superseded_work(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        bundle = server.prewarm_current_tip_ready_bundle()
        state = client(1)
        state.authorization_generation = 1
        state.difficulty_generation = 0
        server.clients = {state}
        server._ensure_initial_job_state()
        # Lapse the published authority entirely: age the freshness stamp and
        # close the divergence lease so the live RPC read owns classification
        # again, which is the only state where prepared published-tip work
        # must be dropped instead of delivered.
        server.current_tip_observed_monotonic = (
            time.monotonic()
            - float(getattr(server, "submit_tip_max_age_seconds", 10.0))
            - 1.0
        )
        server.template_refresh_failure_exit_seconds = 0.0
        server._ensure_tip_refresh_service().reconfigure_for_test(
            failure_exit_seconds=server.template_refresh_failure_exit_seconds
        )
        request = PendingInitialJob(
            client=state,
            authorization_generation=1,
            worker=state.worker,
            requested_monotonic=0.0,
            deadline_monotonic=None,
            connection_id=state.connection_id,
            difficulty_generation=0,
        )
        server.pending_initial_jobs[state] = request
        artifacts = server.current_template_artifacts()
        waits: list[float] = []
        request.cancelled.wait = (  # type: ignore[method-assign]
            lambda timeout: waits.append(timeout) or False
        )
        original_call = rpc.call
        best_tip_calls = 0

        def tip_churn_once(
            method: str,
            params: list[object] | None = None,
        ) -> object:
            nonlocal best_tip_calls
            if method == "getbestblockhash":
                best_tip_calls += 1
                if best_tip_calls == 1:
                    return "ff" * 32
                return artifacts.previousblockhash
            return original_call(method, params)

        deliveries = iter((None, True))
        rpc.call = tip_churn_once  # type: ignore[method-assign]
        service = server._ensure_job_bundle_service()
        service.template_repository.current = lambda: artifacts  # type: ignore[method-assign]
        service.shared_job_bundle = lambda *_args, **_kwargs: bundle  # type: ignore[method-assign]
        server._deliver_initial_bundle = (  # type: ignore[method-assign]
            lambda *_args: next(deliveries)
        )

        self.assertTrue(server._run_initial_job(request))
        self.assertEqual(best_tip_calls, 3)
        self.assertEqual(waits, [0.05, 0.1])

    def test_initial_delivery_keeps_authoritative_published_work_on_tip_churn(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        bundle = server.prewarm_current_tip_ready_bundle()
        state = client(1)
        state.authorization_generation = 1
        state.difficulty_generation = 0
        server.clients = {state}
        server._ensure_initial_job_state()
        request = PendingInitialJob(
            client=state,
            authorization_generation=1,
            worker=state.worker,
            requested_monotonic=0.0,
            deadline_monotonic=None,
            connection_id=state.connection_id,
            difficulty_generation=0,
        )
        server.pending_initial_jobs[state] = request
        artifacts = server.current_template_artifacts()
        waits: list[float] = []
        request.cancelled.wait = (  # type: ignore[method-assign]
            lambda timeout: waits.append(timeout) or False
        )
        original_call = rpc.call
        detected_tip = "ee" * 32
        best_tip_calls = 0

        def churned_tip(
            method: str,
            params: list[object] | None = None,
        ) -> object:
            nonlocal best_tip_calls
            if method == "getbestblockhash":
                best_tip_calls += 1
                return detected_tip
            return original_call(method, params)

        rpc.call = churned_tip  # type: ignore[method-assign]
        service = server._ensure_job_bundle_service()
        service.template_repository.current = lambda: artifacts  # type: ignore[method-assign]
        service.shared_job_bundle = lambda *_args, **_kwargs: bundle  # type: ignore[method-assign]
        server._deliver_initial_bundle = (  # type: ignore[method-assign]
            lambda *_args: True
        )

        # The published tip is fresh, so a live read racing ahead of the
        # refresh must not drop deliverable published-tip work; it is only
        # recorded as a detection for the refresh path to act on.
        self.assertTrue(server._run_initial_job(request))
        self.assertEqual(best_tip_calls, 1)
        self.assertEqual(waits, [])
        self.assertEqual(server.latest_detected_tip[0], detected_tip)
        published = server.current_tip_first_seen
        assert published is not None
        self.assertEqual(published[0], artifacts.previousblockhash)

    def test_250_client_reconnect_storm_uses_one_build_without_client_locks(self) -> None:
        server, _rpc = coordinator()
        recorded = install_fake_bundle_builder(server)
        server.tip_refresh_max_workers = 8
        server._ensure_tip_refresh_service().reconfigure_for_test(
            max_workers=server.tip_refresh_max_workers
        )
        server.stratum_max_pending_initial_jobs = 250
        server.prewarm_current_tip_ready_bundle()
        service = server._ensure_job_bundle_service()
        with service._cache_lock:
            service._bundle_cache.clear()
        service.clear_prepared_ready()

        build_entered = threading.Event()
        release_build = threading.Event()
        original_build = service.build_shared_job_bundle
        build_calls = 0
        build_calls_lock = threading.Lock()

        def blocked_build(*args: object, **kwargs: object) -> object:
            nonlocal build_calls
            with build_calls_lock:
                build_calls += 1
            build_entered.set()
            self.assertTrue(release_build.wait(10))
            return original_build(*args, **kwargs)  # type: ignore[arg-type]

        service.build_shared_job_bundle = blocked_build  # type: ignore[method-assign]
        clients = [client(index + 1) for index in range(250)]
        now = time.monotonic()
        for state in clients:
            state.authorization_generation = 1
            state.authorized_monotonic = now
            state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = set(clients)

        barrier = threading.Barrier(len(clients) + 1)
        request_threads = [
            threading.Thread(
                target=lambda state=state: (
                    barrier.wait(),
                    server.request_initial_job_delivery(state),
                )
            )
            for state in clients
        ]
        for thread in request_threads:
            thread.start()
        barrier.wait()
        try:
            self.assertTrue(build_entered.wait(10))
            for thread in request_threads:
                thread.join(5)
            self.assertTrue(all(not thread.is_alive() for thread in request_threads))
            executor = server.initial_job_executor()
            _queued, active_workers = executor.stats()
            self.assertLessEqual(active_workers, 4)
            self.assertLessEqual(
                len(server.pending_initial_jobs),
                250,
            )
            for state in clients:
                acquired = state.job_update_lock.acquire(timeout=0.2)
                self.assertTrue(acquired)
                if acquired:
                    state.job_update_lock.release()
        finally:
            release_build.set()

        try:
            wait_until(lambda: all(state.active_job is not None for state in clients))
            wait_until(lambda: not server.pending_initial_jobs)
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(build_calls, 1)
        self.assertEqual(recorded["calls"], 2)
        self.assertEqual(server.ledger.snapshot_calls, 2)
        self.assertIsNotNone(server.last_initial_job_delivery_monotonic)
        expected_tip = server.tip_template_snapshot.bestblockhash
        self.assertTrue(
            all(
                state.active_job.template["previousblockhash"] == expected_tip
                for state in clients
            )
        )

    def test_reauthorization_supersedes_pending_identity(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        server.prewarm_current_tip_ready_bundle()
        with server._ensure_job_bundle_service()._cache_lock:
            server._ensure_job_bundle_service()._bundle_cache.clear()

        build_entered = threading.Event()
        release_build = threading.Event()
        service = server._ensure_job_bundle_service()
        original_build = service.build_shared_job_bundle

        def blocked_build(*args: object, **kwargs: object) -> object:
            build_entered.set()
            self.assertTrue(release_build.wait(5))
            return original_build(*args, **kwargs)  # type: ignore[arg-type]

        service.build_shared_job_bundle = blocked_build  # type: ignore[method-assign]
        state = client(1, worker(username="old-worker"))
        state.authorization_generation = 1
        state.authorized_monotonic = time.monotonic()
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}
        server.request_initial_job_delivery(state)
        self.assertTrue(build_entered.wait(5))

        replacement = worker(username="new-worker")
        with state.job_update_lock:
            state.worker = replacement
            state.username = replacement.username
            state.authorization_generation += 1
            state.authorized_monotonic = time.monotonic()
        server.request_initial_job_delivery(state)
        queued_replacement = server.pending_initial_jobs[state]
        self.assertIsNone(queued_replacement.future)
        self.assertIsNotNone(queued_replacement.predecessor)
        release_build.set()
        try:
            wait_until(lambda: state.active_job is not None)
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(state.active_job.worker, replacement)
        self.assertEqual(state.active_job.authorization_generation, 2)

    def test_payout_generation_supersedes_pending_initial_bundle(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        server.prewarm_current_tip_ready_bundle()
        with server._ensure_job_bundle_service()._cache_lock:
            server._ensure_job_bundle_service()._bundle_cache.clear()

        build_entered = threading.Event()
        release_build = threading.Event()
        service = server._ensure_job_bundle_service()
        original_build = service.build_shared_job_bundle
        build_calls = 0

        def blocked_build(*args: object, **kwargs: object) -> object:
            nonlocal build_calls
            build_calls += 1
            if build_calls == 1:
                build_entered.set()
                self.assertTrue(release_build.wait(5))
            return original_build(*args, **kwargs)  # type: ignore[arg-type]

        service.build_shared_job_bundle = blocked_build  # type: ignore[method-assign]
        state = client(1)
        state.authorization_generation = 1
        state.authorized_monotonic = time.monotonic()
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}
        server.request_initial_job_delivery(state)
        self.assertTrue(build_entered.wait(5))

        self.assertEqual(server._advance_payout_state_generation(), 1)
        release_build.set()
        try:
            wait_until(
                lambda: state.active_job is not None
                and state.active_job.payout_state_generation == 1
            )
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(state.active_job.payout_state_generation, 1)
        self.assertNotEqual(state.active_job.payout_state_generation, 0)

    def test_collection_mode_initial_bundles_remain_identity_specific(self) -> None:
        from tests.prism_coordinator_test_support import FakeLedger

        server, _rpc = coordinator(ledger=FakeLedger(miners=["solo"]))
        install_fake_bundle_builder(server)
        server.prewarm_current_tip_ready_bundle()
        first = client(1, worker(payout="tq1worker-a", username="worker-a"))
        second = client(2, worker(payout="tq1worker-b", username="worker-b"))
        for state in (first, second):
            state.authorization_generation = 1
            state.authorized_monotonic = time.monotonic()
            state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {first, second}

        server.request_initial_job_delivery(first)
        server.request_initial_job_delivery(second)
        try:
            wait_until(
                lambda: first.active_job is not None
                and second.active_job is not None
            )
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertTrue(first.active_job.collection_only)
        self.assertTrue(second.active_job.collection_only)
        self.assertNotEqual(first.active_job.worker, second.active_job.worker)
        collection_keys = {
            key
            for key in server._ensure_job_bundle_service()._bundle_cache
            if len(key) >= 8 and key[2] == "collection"
        }
        self.assertEqual(
            {key[6] for key in collection_keys},
            {"tq1worker-a", "tq1worker-b"},
        )

    def test_new_tip_supersedes_blocked_build_without_duplicate_client_task(self) -> None:
        tip_a = "11" * 32
        tip_b = "22" * 32
        server, rpc = coordinator(template=base_template(prevhash=tip_a))
        install_fake_bundle_builder(server)
        server.prewarm_current_tip_ready_bundle()
        with server._ensure_job_bundle_service()._cache_lock:
            server._ensure_job_bundle_service()._bundle_cache.clear()

        first_build_entered = threading.Event()
        release_first_build = threading.Event()
        service = server._ensure_job_bundle_service()
        original_build = service.build_shared_job_bundle
        build_count = 0
        build_lock = threading.Lock()

        def block_first_build(*args: object, **kwargs: object) -> object:
            nonlocal build_count
            with build_lock:
                build_count += 1
                current = build_count
            if current == 1:
                first_build_entered.set()
                self.assertTrue(release_first_build.wait(10))
            return original_build(*args, **kwargs)  # type: ignore[arg-type]

        service.build_shared_job_bundle = block_first_build  # type: ignore[method-assign]
        state = client(1)
        state.authorization_generation = 1
        state.authorized_monotonic = time.monotonic()
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}
        server.request_initial_job_delivery(state)
        self.assertTrue(first_build_entered.wait(5))

        rpc.tip = tip_b
        rpc.template = base_template(height=11, prevhash=tip_b)
        server.observe_tip_first_seen(tip_b)
        time.sleep(0.05)
        self.assertEqual(build_count, 1)
        self.assertIsNone(state.active_job)
        release_first_build.set()
        try:
            wait_until(
                lambda: state.active_job is not None
                and state.active_job.template["previousblockhash"] == tip_b,
                timeout=5,
            )
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertGreaterEqual(build_count, 2)
        self.assertEqual(state.active_job.template["previousblockhash"], tip_b)
        build_counts = server._ensure_job_bundle_service().shared_preparation_metrics()[
            "build_counts"
        ]
        self.assertGreaterEqual(build_counts["superseded"], 1)

    def test_health_turns_non_green_after_stalled_delivery_deadline(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        server.prewarm_current_tip_ready_bundle()
        state = client(1)
        state.authorization_generation = 1
        state.authorized_monotonic = time.monotonic() - 60
        server.clients = {state}
        server.started_monotonic = time.monotonic() - 60
        server.mining_health_startup_grace_seconds = 5
        server.stratum_initial_job_timeout_seconds = 5

        status, payload = server.cached_health_payload()

        self.assertEqual(status, 503)
        self.assertFalse(payload["ok"])
        self.assertFalse(payload["mining_ready"])
        self.assertEqual(payload["authorized_clients"], 1)
        self.assertEqual(payload["clients_with_current_tip_job"], 0)

        server.clients.clear()
        server._health_snapshot = None
        status, payload = server.cached_health_payload()
        self.assertEqual(status, 200)
        self.assertTrue(payload["ok"])

    def test_zero_initial_timeout_disables_delivery_stall_health_deadline(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        server.prewarm_current_tip_ready_bundle()
        state = client(1)
        state.authorization_generation = 1
        state.authorized_monotonic = time.monotonic() - 60
        server.clients = {state}
        server.started_monotonic = time.monotonic() - 60
        server.mining_health_startup_grace_seconds = 5
        server.stratum_initial_job_timeout_seconds = 0

        status, payload = server.cached_health_payload()

        self.assertEqual(status, 200)
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["mining_ready"])
        self.assertFalse(payload["initial_delivery_stalled"])
        self.assertNotIn("initial-delivery-stalled", payload["unhealthy_reasons"])

    def test_health_flags_sustained_eighty_seven_percent_coverage_loss(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        bundle = server.prewarm_current_tip_ready_bundle()
        assert bundle is not None
        clients = [client(index + 1) for index in range(8)]
        old = time.monotonic() - 60
        for state in clients:
            state.authorization_generation = 1
            state.authorized_monotonic = old
            state.tip_work_delivered = ("ff" * 32, old)
        clients[0].active_job = server.stamp_job_for_client(
            clients[0], bundle, clean_jobs=True
        )
        server.clients = set(clients)
        server.started_monotonic = old
        server.mining_health_startup_grace_seconds = 5
        server.stratum_initial_job_timeout_seconds = 5
        server._mining_delivery_failure_started_monotonic = time.monotonic() - 5

        status, payload = server.cached_health_payload()

        self.assertEqual(status, 503)
        self.assertEqual(payload["clients_with_current_tip_job"], 1)
        self.assertEqual(payload["current_tip_job_coverage_ratio"], 0.125)
        self.assertIn("initial-delivery-stalled", payload["unhealthy_reasons"])

    def test_same_tip_obsolete_template_does_not_count_as_current(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        bundle = server.prewarm_current_tip_ready_bundle()
        assert bundle is not None
        state = client(1)
        state.authorization_generation = 1
        state.active_job = server.stamp_job_for_client(
            state, bundle, clean_jobs=True
        )
        server.clients = {state}
        self.assertEqual(
            server.mining_delivery_snapshot()["clients_with_current_tip_jobs"],
            1,
        )

        rpc.template = base_template(height=11, prevhash=rpc.tip)
        rpc.template["coinbasevalue"] = int(rpc.template["coinbasevalue"]) + 1
        server.tip_template_snapshot = server.fetch_qbit_tip_template_snapshot()

        self.assertEqual(
            server.mining_delivery_snapshot()["clients_with_current_tip_jobs"],
            0,
        )


if __name__ == "__main__":
    unittest.main()
