#!/usr/bin/env python3
"""Startup prewarm and reconnect-storm initial job delivery regressions."""

from __future__ import annotations

import threading
import time
import unittest

from lab.prism.prism_coordinator import PRISM_JOB_EXTRANONCE1_PLACEHOLDER_HEX
from tests.test_prism_coordinator_job_cache import (
    EXTRANONCE2_SIZE,
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
    def test_startup_prewarm_builds_worker_independent_ready_bundle(self) -> None:
        server, rpc = coordinator()
        recorded = install_fake_bundle_builder(server)

        bundle = server.prewarm_current_tip_ready_bundle()

        self.assertIsNotNone(bundle)
        assert bundle is not None
        self.assertFalse(bundle.collection_only)
        self.assertEqual(bundle.key, (bundle.template_fingerprint, "ready"))
        self.assertEqual(recorded["calls"], 1)
        self.assertEqual(server.ledger.snapshot_calls, 1)
        self.assertEqual(server.tip_template_snapshot.bestblockhash, rpc.tip)
        self.assertIs(server._prepared_ready_bundle, bundle)
        self.assertEqual(
            recorded["suffixes"],
            [
                server.coinbase_tag_hex
                + PRISM_JOB_EXTRANONCE1_PLACEHOLDER_HEX
                + "00" * EXTRANONCE2_SIZE
            ],
        )

    def test_250_client_reconnect_storm_uses_one_build_without_client_locks(self) -> None:
        server, _rpc = coordinator()
        recorded = install_fake_bundle_builder(server)
        server.tip_refresh_max_workers = 8
        server.prewarm_current_tip_ready_bundle()
        with server._job_cache_lock:
            server._job_bundle_cache.clear()
            server._prepared_ready_bundle = None
            server._prepared_ready_snapshot = None

        build_entered = threading.Event()
        release_build = threading.Event()
        original_build = server.build_shared_job_bundle
        build_calls = 0
        build_calls_lock = threading.Lock()

        def blocked_build(*args: object, **kwargs: object) -> object:
            nonlocal build_calls
            with build_calls_lock:
                build_calls += 1
            build_entered.set()
            self.assertTrue(release_build.wait(10))
            return original_build(*args, **kwargs)  # type: ignore[arg-type]

        server.build_shared_job_bundle = blocked_build  # type: ignore[method-assign]
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
            self.assertLessEqual(server._delivery_slots_in_use, 8)
            self.assertLessEqual(
                len(server._initial_delivery_pending),
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
            wait_until(
                lambda: not server._initial_delivery_pending
                and not server._initial_delivery_active
            )
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(build_calls, 1)
        self.assertEqual(recorded["calls"], 2)
        self.assertEqual(server.ledger.snapshot_calls, 2)
        self.assertEqual(server.initial_delivery_counts["sent"], 250)
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
        with server._job_cache_lock:
            server._job_bundle_cache.clear()

        build_entered = threading.Event()
        release_build = threading.Event()
        original_build = server.build_shared_job_bundle

        def blocked_build(*args: object, **kwargs: object) -> object:
            build_entered.set()
            self.assertTrue(release_build.wait(5))
            return original_build(*args, **kwargs)  # type: ignore[arg-type]

        server.build_shared_job_bundle = blocked_build  # type: ignore[method-assign]
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
        with server._job_cache_lock:
            server._job_bundle_cache.clear()

        stats_entered = threading.Event()
        release_stats = threading.Event()
        original_stats = server.accepted_share_stats
        stats_calls = 0

        def blocked_stats() -> tuple[int, int]:
            nonlocal stats_calls
            stats_calls += 1
            if stats_calls == 1:
                stats_entered.set()
                self.assertTrue(release_stats.wait(5))
            return original_stats()

        server.accepted_share_stats = blocked_stats  # type: ignore[method-assign]
        state = client(1)
        state.authorization_generation = 1
        state.authorized_monotonic = time.monotonic()
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}
        server.request_initial_job_delivery(state)
        self.assertTrue(stats_entered.wait(5))

        self.assertEqual(server._advance_payout_state_generation(), 1)
        release_stats.set()
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
        from tests.test_prism_coordinator_job_cache import FakeLedger

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
        self.assertIsNot(first.active_job.bundle, second.active_job.bundle)
        collection_keys = {
            key
            for key in server._job_bundle_cache
            if len(key) >= 4 and key[1] == "collection"
        }
        self.assertEqual(
            {key[2] for key in collection_keys},
            {"tq1worker-a", "tq1worker-b"},
        )

    def test_new_tip_build_overtakes_blocked_obsolete_preparation(self) -> None:
        tip_a = "11" * 32
        tip_b = "22" * 32
        server, rpc = coordinator(template=base_template(prevhash=tip_a))
        install_fake_bundle_builder(server)
        server.prewarm_current_tip_ready_bundle()
        with server._job_cache_lock:
            server._job_bundle_cache.clear()

        first_build_entered = threading.Event()
        release_first_build = threading.Event()
        original_build = server.build_shared_job_bundle
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

        server.build_shared_job_bundle = block_first_build  # type: ignore[method-assign]
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
        try:
            wait_until(
                lambda: state.active_job is not None
                and state.active_job.template["previousblockhash"] == tip_b,
                timeout=5,
            )
            self.assertFalse(release_first_build.is_set())
        finally:
            release_first_build.set()
            server.shutdown_tip_refresh_executor()

        self.assertGreaterEqual(build_count, 2)
        self.assertEqual(state.active_job.template["previousblockhash"], tip_b)
        self.assertGreaterEqual(server.shared_bundle_build_counts["superseded"], 1)

    def test_health_turns_non_green_after_stalled_delivery_deadline(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        server.prewarm_current_tip_ready_bundle()
        state = client(1)
        state.authorization_generation = 1
        state.authorized_monotonic = time.monotonic() - 60
        server.clients = {state}
        server.started_monotonic = time.monotonic() - 60
        server.mining_startup_grace_seconds = 5
        server.initial_job_delivery_deadline_seconds = 5

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


if __name__ == "__main__":
    unittest.main()
