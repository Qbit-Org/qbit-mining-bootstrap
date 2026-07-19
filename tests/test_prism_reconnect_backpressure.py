#!/usr/bin/env python3
"""Deterministic reconnect-storm admission, backpressure, and health tests."""

from __future__ import annotations

import socket
import threading
import time
import unittest
from concurrent.futures import Future
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from lab.auxpow import vardiff
from lab.prism.prism_coordinator import (
    ClientState,
    PendingInitialJob,
    PRISM_DELIVERY_PRIORITY_INITIAL,
    PRISM_DELIVERY_PRIORITY_NEW_TIP,
    PRISM_DELIVERY_PRIORITY_SAME_TIP,
    PrismCoordinator,
    StratumError,
    StratumListenerProfile,
    WorkerIdentity,
    _BoundedPriorityExecutor,
    _JobBuildCancellation,
)


class FakeSocket:
    def __init__(self) -> None:
        self.closed = False

    def settimeout(self, _timeout: object) -> None:
        return

    def setsockopt(self, *_args: object) -> None:
        return

    def shutdown(self, _how: object) -> None:
        return

    def close(self) -> None:
        self.closed = True


class AcceptSequence:
    def __init__(self, server: PrismCoordinator, count: int, *, port: int) -> None:
        self.server = server
        self.sockets = [FakeSocket() for _ in range(count)]
        self.port = port
        self.index = 0

    def accept(self) -> tuple[FakeSocket, tuple[str, int]]:
        if self.index == len(self.sockets):
            self.server.stop_event.set()
            raise socket.timeout
        sock = self.sockets[self.index]
        address = ("127.0.0.1", self.port + self.index)
        self.index += 1
        return sock, address


class DormantThread:
    created = 0

    def __init__(self, *args: object, **kwargs: object) -> None:
        type(self).created += 1

    def start(self) -> None:
        return


def worker(name: str) -> WorkerIdentity:
    return WorkerIdentity(
        username=name,
        payout_address=name,
        worker_name=None,
        script_pubkey_hex="5220" + "11" * 32,
        p2mr_program_hex="11" * 32,
    )


def listener(name: str, port: int) -> StratumListenerProfile:
    config = vardiff.VardiffConfig(
        enabled=False,
        target_share_interval_seconds=Decimal("15"),
        min_difficulty=Decimal("1"),
        max_difficulty=Decimal("1024"),
        retarget_interval_seconds=Decimal("90"),
        max_step_factor=Decimal("4"),
        startup_difficulty=Decimal("1"),
        max_step_down_factor=Decimal("4"),
        ewma_alpha=Decimal("0.4"),
        retarget_tolerance=Decimal("0.25"),
    )
    return StratumListenerProfile(
        name=name,
        bind="127.0.0.1",
        port=port,
        share_difficulty=Decimal("1"),
        vardiff_config=config,
        heartbeat_name=f"accept-{name}",
    )


def coordinator(*, connection_limit: int = 3, pending_limit: int = 2) -> PrismCoordinator:
    server = PrismCoordinator.__new__(PrismCoordinator)
    server.lock = threading.RLock()
    server.stop_event = threading.Event()
    server.clients = set()
    server.jobs = {}
    server.connection_counter = 0
    server.job_counter = 0
    server.stratum_max_connections = connection_limit
    server.stratum_max_connections_per_username = 0
    server.stratum_max_pending_initial_jobs = pending_limit
    server.stratum_initial_job_timeout_seconds = 30.0
    server.mining_health_startup_grace_seconds = 30.0
    server.tip_refresh_max_workers = 1
    server.tip_template_snapshot = None
    server.started_monotonic = 0.0
    server.submitted_share_count = 0
    server.rejection_counts_by_reason = {}
    server.tip_template_snapshot = None
    server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
    server.apply_stratum_send_timeout = lambda _sock: None  # type: ignore[method-assign]
    server.client_startup_difficulty = lambda _profile: Decimal("1")  # type: ignore[method-assign]
    server._ensure_job_cache_state()
    server._ensure_tip_refresh_state()
    server._ensure_initial_job_state()
    return server


def client(server: PrismCoordinator, connection_id: int, *, with_job: bool = False) -> ClientState:
    state = ClientState(
        sock=FakeSocket(),
        address=("127.0.0.1", 40_000 + connection_id),
        connection_id=connection_id,
        extranonce1_hex=f"{connection_id:08x}",
    )
    state.subscribed = True
    state.authorized = True
    state.authorization_generation = 1
    state.worker = worker(f"miner-{connection_id}")
    state.username = state.worker.username
    if with_job:
        state.active_job = SimpleNamespace(
            template={"previousblockhash": "aa" * 32},
            payout_state_generation=0,
        )
    server.clients.add(state)
    return state


class PrismReconnectBackpressureTests(unittest.TestCase):
    def tearDown(self) -> None:
        DormantThread.created = 0

    def test_global_cap_precedes_client_and_handler_allocation_during_storm(self) -> None:
        server = coordinator(connection_limit=3)
        accepted = AcceptSequence(server, 1_000, port=10_000)

        with patch("lab.prism.prism_coordinator.threading.Thread", DormantThread):
            server.accept_loop(accepted, listener("default", 3340))  # type: ignore[arg-type]

        self.assertEqual(len(server.clients), 3)
        self.assertEqual(server.handler_thread_count, 3)
        self.assertEqual(DormantThread.created, 3)
        self.assertEqual(server.connection_limit_rejection_counts["global"], 997)
        self.assertTrue(all(sock.closed for sock in accepted.sockets[3:]))
        self.assertTrue(all(not sock.closed for sock in accepted.sockets[:3]))

    def test_cross_listener_accounting_uses_one_global_cap(self) -> None:
        server = coordinator(connection_limit=3)
        default_accepts = AcceptSequence(server, 2, port=20_000)
        with patch("lab.prism.prism_coordinator.threading.Thread", DormantThread):
            server.accept_loop(default_accepts, listener("default", 3340))  # type: ignore[arg-type]
            server.stop_event.clear()
            high_accepts = AcceptSequence(server, 2, port=30_000)
            server.accept_loop(high_accepts, listener("highdiff", 3342))  # type: ignore[arg-type]

        self.assertEqual(len(server.clients), 3)
        self.assertEqual(
            sorted(state.listener_name for state in server.clients),
            ["default", "default", "highdiff"],
        )
        self.assertTrue(high_accepts.sockets[1].closed)

    def test_current_job_coverage_requires_an_observed_matching_tip(self) -> None:
        server = coordinator(connection_limit=2)
        client(server, 1, with_job=True)

        missing_tip = server.mining_delivery_snapshot(now=1.0)
        self.assertEqual(missing_tip["clients_with_current_tip_jobs"], 0)
        self.assertEqual(missing_tip["current_tip_job_coverage"], 0.0)

        server.current_tip_first_seen = ("aa" * 32, None)
        matching_tip = server.mining_delivery_snapshot(now=2.0)
        self.assertEqual(matching_tip["clients_with_current_tip_jobs"], 1)
        self.assertEqual(matching_tip["current_tip_job_coverage"], 1.0)

        server.current_tip_first_seen = ("bb" * 32, time.monotonic())
        advanced_tip = server.mining_delivery_snapshot(now=3.0)
        self.assertEqual(advanced_tip["clients_with_current_tip_jobs"], 0)
        self.assertEqual(advanced_tip["current_tip_job_coverage"], 0.0)

    def test_pending_initial_jobs_are_bounded_and_duplicate_requests_coalesce(self) -> None:
        server = coordinator(connection_limit=5, pending_limit=2)
        server.initial_job_max_workers = 1
        started = threading.Event()
        release = threading.Event()

        def blocked(request: PendingInitialJob) -> bool:
            started.set()
            release.wait(5)
            return not server._initial_request_cancelled(request)

        server._run_initial_job = blocked  # type: ignore[method-assign]
        first, second, excess = (client(server, index) for index in (1, 2, 3))

        self.assertTrue(server.schedule_initial_job(first))
        self.assertTrue(started.wait(5))
        self.assertTrue(server.schedule_initial_job(first))
        self.assertTrue(server.schedule_initial_job(second))
        self.assertFalse(server.schedule_initial_job(excess))

        executor = server.initial_job_executor()
        self.assertEqual(executor.max_queue_size, 2)
        self.assertEqual(executor.stats(), (1, 1))
        self.assertEqual(len(server.pending_initial_jobs), 2)
        self.assertEqual(server.initial_job_coalesced_count, 1)
        self.assertEqual(server.initial_job_queue_rejection_count, 1)
        self.assertNotIn(excess, server.clients)
        self.assertTrue(excess.sock.closed)
        release.set()
        server.shutdown_tip_refresh_executor()

    def test_cancelled_queued_requests_immediately_reclaim_admission(self) -> None:
        server = coordinator(connection_limit=6, pending_limit=2)
        server.initial_job_max_workers = 1
        started = threading.Event()
        release = threading.Event()

        def blocked(_request: PendingInitialJob) -> bool:
            started.set()
            release.wait(5)
            return False

        server._run_initial_job = blocked  # type: ignore[method-assign]
        running = client(server, 1)
        cancelled = [client(server, index) for index in (2, 3)]
        replacement = client(server, 4)

        try:
            self.assertTrue(server.schedule_initial_job(running))
            self.assertTrue(started.wait(5))
            for state in cancelled:
                self.assertTrue(server.schedule_initial_job(state))
                self.assertEqual(server.initial_job_executor().stats(), (1, 1))
                server.cancel_initial_job_delivery(state)
            server.cancel_initial_job_delivery(running)

            executor = server.initial_job_executor()
            self.assertEqual(executor.stats(), (0, 1))
            health = server.mining_delivery_snapshot()
            self.assertEqual(health["pending_initial_jobs"], 0)
            self.assertFalse(health["pending_initial_jobs_saturated"])
            self.assertEqual(server.initial_job_queue_capacity_reclaimed_count, 2)
            metrics = "\n".join(server.initial_delivery_metrics_lines())
            self.assertIn("qbit_prism_initial_job_delivery_queue_depth 0", metrics)
            self.assertIn(
                "qbit_prism_initial_job_queue_capacity_reclaimed_total 2",
                metrics,
            )

            # Before cancellation-aware removal, the two tombstones filled the
            # physical queue and this valid replacement was rejected.
            self.assertTrue(server.schedule_initial_job(replacement))
            self.assertEqual(executor.stats(), (1, 1))
            self.assertIn(replacement, server.clients)
            self.assertFalse(replacement.sock.closed)
        finally:
            release.set()
            server.shutdown_tip_refresh_executor()

    def test_disconnect_and_timeout_reclaim_queued_admission(self) -> None:
        server = coordinator(connection_limit=6, pending_limit=2)
        server.initial_job_max_workers = 1
        started = threading.Event()
        release = threading.Event()

        def blocked(_request: PendingInitialJob) -> bool:
            started.set()
            release.wait(5)
            return False

        server._run_initial_job = blocked  # type: ignore[method-assign]
        running = client(server, 1)
        disconnected = client(server, 2)
        timed_out = client(server, 3)
        replacement = client(server, 4)

        try:
            self.assertTrue(server.schedule_initial_job(running))
            self.assertTrue(started.wait(5))
            server.pending_initial_jobs[running].deadline_monotonic = None

            self.assertTrue(server.schedule_initial_job(disconnected))
            server.disconnect_client(disconnected)
            self.assertEqual(server.initial_job_executor().stats(), (0, 1))
            self.assertTrue(disconnected.sock.closed)

            self.assertTrue(server.schedule_initial_job(timed_out))
            request = server.pending_initial_jobs[timed_out]
            assert request.deadline_monotonic is not None
            self.assertEqual(
                server.sweep_initial_job_timeouts(
                    now=request.deadline_monotonic,
                ),
                1,
            )
            self.assertEqual(server.initial_job_executor().stats(), (0, 1))
            self.assertTrue(timed_out.sock.closed)
            self.assertEqual(server.initial_job_timeout_count, 1)

            self.assertTrue(server.schedule_initial_job(replacement))
            self.assertEqual(server.initial_job_executor().stats(), (1, 1))
            self.assertEqual(server.initial_job_queue_capacity_reclaimed_count, 2)
        finally:
            release.set()
            server.shutdown_tip_refresh_executor()

    def test_saturated_queue_reauthorization_preserves_working_session(self) -> None:
        server = coordinator(connection_limit=4, pending_limit=1)
        server.initial_job_max_workers = 1
        server.current_tip_first_seen = ("aa" * 32, time.monotonic())
        started = threading.Event()
        release = threading.Event()

        def blocked(_request: PendingInitialJob) -> bool:
            started.set()
            release.wait(5)
            return False

        server._run_initial_job = blocked  # type: ignore[method-assign]
        waiting = client(server, 1)
        live = client(server, 2, with_job=True)
        original_worker = live.worker
        server.resolve_worker = lambda username: worker(username)  # type: ignore[method-assign]

        try:
            self.assertTrue(server.schedule_initial_job(waiting))
            self.assertTrue(started.wait(5))
            with self.assertRaises(StratumError) as raised:
                server.handle_request(
                    live,
                    {
                        "id": 7,
                        "method": "mining.authorize",
                        "params": ["replacement-worker", "x"],
                    },
                )

            self.assertFalse(raised.exception.disconnect)
            self.assertEqual(
                raised.exception.message,
                "initial job delivery capacity unavailable",
            )
            self.assertEqual(live.authorization_generation, 1)
            self.assertIs(live.worker, original_worker)
            self.assertIn(live, server.clients)
            self.assertFalse(live.sock.closed)
            self.assertIsNotNone(live.active_job)
            self.assertIs(server.pending_initial_jobs[waiting].client, waiting)
            self.assertEqual(server.initial_job_queue_rejection_count, 0)
        finally:
            release.set()
            server.shutdown_tip_refresh_executor()

    def test_executor_cancel_racing_dequeue_does_not_leak_or_double_release(self) -> None:
        for _ in range(10):
            executor = _BoundedPriorityExecutor(max_workers=1, max_queue_size=1)
            blocker_started = threading.Event()
            release_blocker = threading.Event()
            raced_started = threading.Event()
            release_raced = threading.Event()
            cancel_now = threading.Event()
            cancel_results: list[bool] = []

            def blocker() -> None:
                blocker_started.set()
                release_blocker.wait(5)

            def raced() -> None:
                raced_started.set()
                release_raced.wait(5)

            first = executor.submit(blocker)
            self.assertTrue(blocker_started.wait(5))
            second = executor.submit(raced)

            def cancel_raced() -> None:
                cancel_now.wait(5)
                cancel_results.append(executor.cancel(second))

            cancel_thread = threading.Thread(target=cancel_raced)
            cancel_thread.start()
            cancel_now.set()
            release_blocker.set()
            release_raced.set()
            cancel_thread.join(5)
            self.assertFalse(cancel_thread.is_alive())
            first.result(5)
            if not second.cancelled():
                second.result(5)

            # A second cancellation cannot release the same queue entry, and
            # fresh bounded work remains admissible whichever side won.
            self.assertFalse(executor.cancel(second))
            probe = executor.submit(lambda: "admitted")
            self.assertEqual(probe.result(5), "admitted")
            executor._queue.join()
            self.assertEqual(executor.stats(), (0, 0))
            self.assertEqual(executor._queue.unfinished_tasks, 0)
            self.assertEqual(len(cancel_results), 1)
            executor.shutdown(wait=True, cancel_futures=True)

    def test_initial_job_deadline_starts_only_after_protocol_handshake(self) -> None:
        server = coordinator(connection_limit=3, pending_limit=2)
        release = threading.Event()
        server._run_initial_job = lambda _request: release.wait(5)  # type: ignore[method-assign]
        state = client(server, 1)
        state.subscribed = False

        self.assertTrue(server.schedule_initial_job(state))
        self.assertNotIn(state, server.pending_initial_jobs)

        state.subscribed = True
        self.assertTrue(server.schedule_initial_job(state))
        self.assertIn(state, server.pending_initial_jobs)
        release.set()
        server.shutdown_tip_refresh_executor()

    def test_expired_initial_request_is_fenced_before_preparation(self) -> None:
        server = coordinator(connection_limit=2, pending_limit=1)
        state = client(server, 1)
        preparation_started = threading.Event()
        request = PendingInitialJob(
            client=state,
            connection_id=state.connection_id,
            authorization_generation=state.authorization_generation,
            difficulty_generation=state.difficulty_generation,
            worker=state.worker,
            requested_monotonic=time.monotonic() - 2,
            deadline_monotonic=time.monotonic() - 1,
        )
        server.pending_initial_jobs[state] = request
        server.ensure_reorg_reconciled_for_current_tip = (  # type: ignore[method-assign]
            lambda: preparation_started.set() or True
        )

        self.assertFalse(server._run_initial_job(request))
        self.assertFalse(preparation_started.is_set())

    def test_failed_initial_job_releases_capacity_and_disconnects_client(self) -> None:
        server = coordinator(connection_limit=3, pending_limit=1)
        server._run_initial_job = lambda _request: False  # type: ignore[method-assign]
        state = client(server, 1)

        self.assertTrue(server.schedule_initial_job(state))
        deadline = time.monotonic() + 5
        while state in server.clients and time.monotonic() < deadline:
            time.sleep(0.01)

        self.assertNotIn(state, server.clients)
        self.assertNotIn(state, server.pending_initial_jobs)
        self.assertTrue(state.sock.closed)
        server.shutdown_tip_refresh_executor()

    def test_reauthorize_schedules_current_work_after_stale_direct_delivery(self) -> None:
        server = coordinator(connection_limit=3, pending_limit=2)
        server.current_tip_first_seen = ("bb" * 32, time.monotonic())
        state = client(server, 1, with_job=True)
        old_request = PendingInitialJob(
            client=state,
            authorization_generation=state.authorization_generation,
            worker=state.worker,
            requested_monotonic=0.0,
            deadline_monotonic=30.0,
        )
        server.pending_initial_jobs[state] = old_request
        release = threading.Event()
        server._run_initial_job = lambda _request: release.wait(5)  # type: ignore[method-assign]
        server.resolve_worker = lambda username: worker(username)  # type: ignore[method-assign]

        def reserve(current: ClientState, identity: WorkerIdentity) -> bool:
            current.worker = identity
            current.username = identity.username
            return True

        server.reserve_client_username = reserve  # type: ignore[method-assign]
        server.apply_client_difficulty_requests = lambda _client: Decimal("2")  # type: ignore[method-assign]
        server.advertise_client_difficulty = lambda _client, _target: True  # type: ignore[method-assign]
        server.send_result = lambda *_args: None  # type: ignore[method-assign]

        server.handle_request(
            state,
            {
                "id": 7,
                "method": "mining.authorize",
                "params": ["miner-new", "d=2"],
            },
        )

        replacement = server.pending_initial_jobs[state]
        self.assertIsNot(replacement, old_request)
        self.assertTrue(old_request.cancelled.is_set())
        self.assertEqual(replacement.authorization_generation, 2)
        release.set()
        server.shutdown_tip_refresh_executor()

    def test_difficulty_change_replaces_pending_first_job_generation(self) -> None:
        server = coordinator(connection_limit=3, pending_limit=2)
        server.current_tip_first_seen = ("aa" * 32, time.monotonic())
        state = client(server, 1)
        started = threading.Event()
        release = threading.Event()

        def deliver(request: PendingInitialJob) -> bool:
            if request.difficulty_generation == 0:
                started.set()
                release.wait(5)
            if server._initial_request_cancelled(request):
                return False
            request.client.active_job = SimpleNamespace(
                template={"previousblockhash": "aa" * 32},
                payout_state_generation=0,
                connection_id=request.connection_id,
                authorization_generation=request.authorization_generation,
                difficulty_generation=request.difficulty_generation,
            )
            server.note_initial_job_delivered(request.client)
            return True

        server._run_initial_job = deliver  # type: ignore[method-assign]
        self.assertTrue(server.request_initial_job_delivery(state))
        self.assertTrue(started.wait(5))
        original = server.pending_initial_jobs[state]

        self.assertFalse(
            server.advertise_client_difficulty(state, Decimal("4"))
        )

        replacement = server.pending_initial_jobs[state]
        self.assertIsNot(replacement, original)
        self.assertTrue(original.cancelled.is_set())
        self.assertEqual(replacement.difficulty_generation, 1)
        self.assertEqual(state.difficulty_generation, 1)
        self.assertEqual(state.share_difficulty, Decimal("4"))
        self.assertIsNone(state.pending_share_difficulty)

        release.set()
        deadline = time.monotonic() + 5
        while server.pending_initial_jobs and time.monotonic() < deadline:
            time.sleep(0.01)
        server.shutdown_tip_refresh_executor()

        self.assertEqual(server.pending_initial_jobs, {})
        self.assertIn(state, server.clients)
        self.assertFalse(state.sock.closed)
        self.assertIsNotNone(state.active_job)
        self.assertEqual(state.active_job.difficulty_generation, 1)

    def test_timeout_cleans_state_and_prevents_late_delivery(self) -> None:
        server = coordinator(connection_limit=3, pending_limit=2)
        release = threading.Event()
        sent: list[int] = []

        def blocked(request: PendingInitialJob) -> bool:
            release.wait(5)
            if not server._initial_request_cancelled(request):
                sent.append(request.client.connection_id)
            return False

        server._run_initial_job = blocked  # type: ignore[method-assign]
        state = client(server, 1)
        self.assertTrue(server.schedule_initial_job(state))
        request = server.pending_initial_jobs[state]

        expired = server.sweep_initial_job_timeouts(
            now=request.requested_monotonic + 31,
        )
        release.set()
        server.shutdown_tip_refresh_executor()

        self.assertEqual(expired, 1)
        self.assertEqual(server.initial_job_timeout_count, 1)
        self.assertNotIn(state, server.clients)
        self.assertNotIn(state, server.pending_initial_jobs)
        self.assertIsNone(state.worker)
        self.assertEqual(state.active_job_ids, set())
        self.assertTrue(state.sock.closed)
        self.assertEqual(sent, [])

    def test_timeout_commits_closing_before_disconnect_handoff(self) -> None:
        server = coordinator(connection_limit=3, pending_limit=2)
        state = client(server, 1)
        request = PendingInitialJob(
            client=state,
            authorization_generation=state.authorization_generation,
            worker=state.worker,
            requested_monotonic=0.0,
            deadline_monotonic=1.0,
        )
        server.pending_initial_jobs[state] = request
        disconnect_client = server.disconnect_client
        rescheduled: list[bool] = []

        def observe_handoff(current: ClientState) -> None:
            self.assertTrue(current.closing)
            rescheduled.append(server.schedule_initial_job(current))
            self.assertNotIn(current, server.pending_initial_jobs)
            disconnect_client(current)

        server.disconnect_client = observe_handoff  # type: ignore[method-assign]

        self.assertEqual(server.sweep_initial_job_timeouts(now=2.0), 1)

        self.assertEqual(rescheduled, [True])
        self.assertNotIn(state, server.clients)
        self.assertTrue(state.sock.closed)

    def test_overload_rejection_never_evicts_current_work(self) -> None:
        server = coordinator(connection_limit=4, pending_limit=1)
        healthy = client(server, 1, with_job=True)
        waiting = client(server, 2)
        excess = client(server, 3)
        request = PendingInitialJob(
            client=waiting,
            authorization_generation=1,
            worker=waiting.worker,
            requested_monotonic=10.0,
            deadline_monotonic=40.0,
        )
        server.pending_initial_jobs[waiting] = request

        self.assertFalse(server.schedule_initial_job(excess))

        self.assertIn(healthy, server.clients)
        self.assertIsNotNone(healthy.active_job)
        self.assertIn(waiting, server.clients)
        self.assertNotIn(excess, server.clients)

    def test_recovery_releases_pending_capacity_without_restart(self) -> None:
        server = coordinator(connection_limit=4, pending_limit=2)
        server.current_tip_first_seen = ("aa" * 32, None)
        bundle_ready = threading.Event()

        def deliver(request: PendingInitialJob) -> bool:
            bundle_ready.wait(5)
            if server._initial_request_cancelled(request):
                return False
            request.client.active_job = SimpleNamespace(
                template={"previousblockhash": "aa" * 32},
                payout_state_generation=0,
            )
            server.note_initial_job_delivered(request.client)
            return True

        server._run_initial_job = deliver  # type: ignore[method-assign]
        first, second = client(server, 1), client(server, 2)
        self.assertTrue(server.schedule_initial_job(first))
        self.assertTrue(server.schedule_initial_job(second))
        self.assertEqual(len(server.pending_initial_jobs), 2)

        bundle_ready.set()
        deadline = time.monotonic() + 5
        while server.pending_initial_jobs and time.monotonic() < deadline:
            time.sleep(0.01)

        self.assertEqual(server.pending_initial_jobs, {})
        self.assertIsNotNone(first.active_job)
        self.assertIsNotNone(second.active_job)
        newcomer = client(server, 3)
        self.assertTrue(server.schedule_initial_job(newcomer))
        server.shutdown_tip_refresh_executor()

    def test_delivery_executor_prioritizes_new_tip_then_initial_then_same_tip(self) -> None:
        executor = _BoundedPriorityExecutor(max_workers=1, max_queue_size=4)
        blocker_started = threading.Event()
        release = threading.Event()
        order: list[str] = []

        def blocker() -> None:
            blocker_started.set()
            release.wait(5)

        executor.submit(blocker, priority=PRISM_DELIVERY_PRIORITY_SAME_TIP)
        self.assertTrue(blocker_started.wait(5))
        routine = executor.submit(
            lambda: order.append("routine"),
            priority=PRISM_DELIVERY_PRIORITY_SAME_TIP,
        )
        initial = executor.submit(
            lambda: order.append("initial"),
            priority=PRISM_DELIVERY_PRIORITY_INITIAL,
        )
        new_tip = executor.submit(
            lambda: order.append("new-tip"),
            priority=PRISM_DELIVERY_PRIORITY_NEW_TIP,
        )
        release.set()
        new_tip.result(5)
        initial.result(5)
        routine.result(5)
        executor.shutdown(wait=True, cancel_futures=True)

        self.assertEqual(order, ["new-tip", "initial", "routine"])

    def test_blocked_initial_workers_do_not_starve_new_tip_delivery(self) -> None:
        server = coordinator(connection_limit=8, pending_limit=8)
        server.initial_job_max_workers = 4
        started = 0
        started_lock = threading.Lock()
        all_started = threading.Event()

        def blocked_retry(request: PendingInitialJob) -> bool:
            nonlocal started
            with started_lock:
                started += 1
                if started == server.initial_job_max_workers:
                    all_started.set()
            while not request.cancelled.wait(0.01):
                pass
            return False

        server._run_initial_job = blocked_retry  # type: ignore[method-assign]
        clients = [client(server, index) for index in range(1, 9)]
        for state in clients:
            self.assertTrue(server.schedule_initial_job(state))

        try:
            self.assertTrue(all_started.wait(5))
            tip_started = threading.Event()

            def publish_new_tip() -> str:
                tip_started.set()
                return "published"

            future = server._submit_delivery_task(
                server.tip_refresh_executor(),
                publish_new_tip,
                priority=PRISM_DELIVERY_PRIORITY_NEW_TIP,
            )
            self.assertTrue(tip_started.wait(0.5))
            self.assertEqual(future.result(1), "published")
            self.assertEqual(len(server.pending_initial_jobs), 8)
            self.assertTrue(
                server.mining_delivery_snapshot()["pending_initial_jobs_saturated"]
            )
            self.assertEqual(server.initial_job_executor().stats(), (4, 4))
            metrics = "\n".join(server.initial_delivery_metrics_lines())
            self.assertIn("qbit_prism_initial_job_delivery_queue_depth 4", metrics)
            self.assertIn("qbit_prism_initial_job_delivery_active_workers 4", metrics)
            self.assertIn("qbit_prism_initial_job_delivery_configured_workers 4", metrics)
        finally:
            server.shutdown_tip_refresh_executor()

    def test_640_client_storm_cannot_displace_latest_tip_builder(self) -> None:
        server = coordinator(connection_limit=800, pending_limit=128)
        server.initial_job_max_workers = 4
        workers_ready = 0
        workers_contending = 0
        worker_lock = threading.Lock()
        all_workers_ready = threading.Event()
        all_workers_contending = threading.Event()
        contend = threading.Event()
        release_workers = threading.Event()

        def build_request(
            key: str,
            *,
            publication_critical: bool,
            source: str,
        ) -> SimpleNamespace:
            return SimpleNamespace(
                idle_retarget=False,
                mode="ready",
                equivalence_key=(key,),
                cache_key=(key,),
                cancellation=_JobBuildCancellation(timeout_seconds=30.0),
                promise=Future(),
                publication_critical=publication_critical,
                request_source=source,
                requested_monotonic=time.monotonic(),
                priority_admission_recorded=False,
            )

        def contending_initial(_request: PendingInitialJob) -> bool:
            nonlocal workers_ready, workers_contending
            with worker_lock:
                workers_ready += 1
                if workers_ready == server.initial_job_max_workers:
                    all_workers_ready.set()
            if not contend.wait(5):
                raise AssertionError("test did not release initial builders")
            deferred = server._request_job_build(  # type: ignore[arg-type]
                build_request(
                    "published-tip",
                    publication_critical=False,
                    source="initial",
                )
            )
            self.assertFalse(deferred.done())
            with worker_lock:
                workers_contending += 1
                if workers_contending == server.initial_job_max_workers:
                    all_workers_contending.set()
            release_workers.wait(5)
            return False

        server._run_initial_job = contending_initial  # type: ignore[method-assign]
        clients = [client(server, index) for index in range(1, 641)]
        admitted = 0
        for state in clients:
            admitted += int(server.schedule_initial_job(state))

        self.assertTrue(all_workers_ready.wait(5))
        self.assertEqual(admitted, 128)
        self.assertEqual(len(server.pending_initial_jobs), 128)
        self.assertEqual(server.initial_job_executor().stats(), (124, 4))

        latest = build_request(
            "latest-tip",
            publication_critical=True,
            source="tip_refresh",
        )

        def start_without_execution(request: object) -> SimpleNamespace:
            server.job_build_scheduler_counts["starts"] += 1
            server._record_priority_admission_locked(  # type: ignore[arg-type]
                request,
                "started",
            )
            return SimpleNamespace(request=request, future=Future())

        server._start_job_build_locked = start_without_execution  # type: ignore[method-assign]
        server._arm_job_build_locked = lambda _flight: None  # type: ignore[method-assign]
        admission_started = time.monotonic()
        latest_promise = server._request_job_build(latest)  # type: ignore[arg-type]
        admission_elapsed = time.monotonic() - admission_started
        contend.set()

        try:
            self.assertTrue(all_workers_contending.wait(5))
            self.assertLess(admission_elapsed, 0.1)
            self.assertIs(latest_promise, latest.promise)
            self.assertFalse(latest.cancellation.is_set())
            assert server._job_build_active is not None
            self.assertIs(server._job_build_active.request, latest)
            self.assertEqual(
                server.job_build_priority_counts["routine_deferred"],
                4,
            )
            self.assertEqual(
                server.initial_job_prepared_work_counts["deferred"],
                4,
            )
            self.assertEqual(
                server.job_build_priority_admission_seconds["count"],
                1,
            )
        finally:
            release_workers.set()
            server.shutdown_initial_job_executor()
            with server._job_build_scheduler_lock:
                server._job_build_active = None

    def test_shutdown_cancels_queued_initial_work_and_joins_both_executors(self) -> None:
        server = coordinator(connection_limit=4, pending_limit=3)
        server.initial_job_max_workers = 1
        initial_started = threading.Event()
        initial_stopped = threading.Event()
        initial_runs: list[int] = []
        queued_tip_ran = threading.Event()

        def blocked_initial(request: PendingInitialJob) -> bool:
            initial_runs.append(request.client.connection_id)
            initial_started.set()
            request.cancelled.wait(5)
            initial_stopped.set()
            return False

        server._run_initial_job = blocked_initial  # type: ignore[method-assign]
        first, second = (client(server, index) for index in (1, 2))
        self.assertTrue(server.schedule_initial_job(first))
        self.assertTrue(initial_started.wait(5))
        self.assertTrue(server.schedule_initial_job(second))
        second_request = server.pending_initial_jobs[second]
        assert second_request.future is not None
        second_future = second_request.future

        tip_started = threading.Event()
        release_tip = threading.Event()

        def blocked_tip() -> None:
            tip_started.set()
            release_tip.wait(5)

        tip_executor = server.tip_refresh_executor()
        tip_executor.submit(
            blocked_tip,
            priority=PRISM_DELIVERY_PRIORITY_NEW_TIP,
        )
        self.assertTrue(tip_started.wait(5))
        queued_tip = tip_executor.submit(
            queued_tip_ran.set,
            priority=PRISM_DELIVERY_PRIORITY_SAME_TIP,
        )
        initial_executor = server.initial_job_executor()
        initial_threads = tuple(initial_executor._threads)
        tip_threads = tuple(tip_executor._threads)
        shutdown_complete = threading.Event()

        def shutdown_executors() -> None:
            server.shutdown_tip_refresh_executor()
            shutdown_complete.set()

        shutdown_thread = threading.Thread(target=shutdown_executors)
        shutdown_thread.start()
        try:
            self.assertTrue(initial_stopped.wait(1))
            self.assertFalse(shutdown_complete.wait(0.05))
            self.assertTrue(second_future.cancelled())
            self.assertTrue(queued_tip.cancelled())
            self.assertFalse(queued_tip_ran.is_set())
        finally:
            release_tip.set()
            shutdown_thread.join(5)

        self.assertFalse(shutdown_thread.is_alive())
        self.assertTrue(shutdown_complete.is_set())
        self.assertEqual(server.pending_initial_jobs, {})
        self.assertEqual(initial_runs, [first.connection_id])
        self.assertEqual(initial_executor.stats(), (0, 0))
        self.assertEqual(server.initial_job_queue_capacity_reclaimed_count, 1)
        self.assertTrue(all(not thread.is_alive() for thread in initial_threads))
        self.assertTrue(all(not thread.is_alive() for thread in tip_threads))
        with self.assertRaisesRegex(RuntimeError, "initial job executor is shut down"):
            server.initial_job_executor()
        with self.assertRaisesRegex(RuntimeError, "tip refresh executor is shut down"):
            server.tip_refresh_executor()

    def test_health_grace_failure_and_recovery_follow_job_coverage(self) -> None:
        server = coordinator(connection_limit=2, pending_limit=2)
        server.started_monotonic = 0.0
        first, second = client(server, 1), client(server, 2)
        for state in (first, second):
            server.pending_initial_jobs[state] = PendingInitialJob(
                client=state,
                authorization_generation=1,
                worker=state.worker,
                requested_monotonic=0.0,
                deadline_monotonic=30.0,
            )

        startup = server.mining_delivery_snapshot(now=10.0)
        failed = server.mining_delivery_snapshot(now=31.0)
        self.assertTrue(startup["mining_ready"])
        self.assertFalse(failed["mining_ready"])
        self.assertIn("initial-delivery-stalled", failed["unhealthy_reasons"])

        server.current_tip_first_seen = ("aa" * 32, None)
        for state in (first, second):
            state.active_job = SimpleNamespace(
                template={"previousblockhash": "aa" * 32},
                payout_state_generation=0,
            )
            server.note_initial_job_delivered(state)
        recovered = server.mining_delivery_snapshot(now=32.0)

        self.assertTrue(recovered["mining_ready"])
        self.assertEqual(recovered["current_tip_job_coverage"], 1.0)
        self.assertTrue(recovered["connection_capacity_saturated"])


if __name__ == "__main__":
    unittest.main()
