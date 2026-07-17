#!/usr/bin/env python3
"""Deterministic reconnect-storm admission, backpressure, and health tests."""

from __future__ import annotations

import socket
import threading
import time
import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from lab.auxpow import vardiff
from lab.prism.prism_coordinator import (
    ClientState,
    PendingInitialJob,
    PrismCoordinator,
    StratumListenerProfile,
    WorkerIdentity,
    _BoundedPriorityExecutor,
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
        sock = self.sockets[self.index]
        address = ("127.0.0.1", self.port + self.index)
        self.index += 1
        if self.index == len(self.sockets):
            self.server.stop_event.set()
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
        state = client(server, 1, with_job=True)

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
        release = threading.Event()

        def blocked(request: PendingInitialJob) -> bool:
            release.wait(5)
            return not server._initial_request_cancelled(request)

        server._run_initial_job = blocked  # type: ignore[method-assign]
        first, second, excess = (client(server, index) for index in (1, 2, 3))

        self.assertTrue(server.schedule_initial_job(first))
        self.assertTrue(server.schedule_initial_job(first))
        self.assertTrue(server.schedule_initial_job(second))
        self.assertFalse(server.schedule_initial_job(excess))

        self.assertEqual(len(server.pending_initial_jobs), 2)
        self.assertEqual(server.initial_job_coalesced_count, 1)
        self.assertEqual(server.initial_job_queue_rejection_count, 1)
        self.assertNotIn(excess, server.clients)
        self.assertTrue(excess.sock.closed)
        release.set()
        server.shutdown_tip_refresh_executor()

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

    def test_delivery_executor_prioritizes_initial_over_routine_work(self) -> None:
        executor = _BoundedPriorityExecutor(max_workers=1, max_queue_size=4)
        blocker_started = threading.Event()
        release = threading.Event()
        order: list[str] = []

        def blocker() -> None:
            blocker_started.set()
            release.wait(5)

        executor.submit(blocker, priority=2)
        self.assertTrue(blocker_started.wait(5))
        routine = executor.submit(lambda: order.append("routine"), priority=2)
        initial = executor.submit(lambda: order.append("initial"), priority=0)
        release.set()
        initial.result(5)
        routine.result(5)
        executor.shutdown(wait=True, cancel_futures=True)

        self.assertEqual(order, ["initial", "routine"])

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
