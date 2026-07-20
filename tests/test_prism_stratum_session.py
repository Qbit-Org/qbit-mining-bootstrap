#!/usr/bin/env python3
"""Deterministic tests for the extracted PRISM Stratum session boundary."""

from __future__ import annotations

import socket
import threading
import unittest
from collections import OrderedDict
from decimal import Decimal
from types import SimpleNamespace

from lab.auxpow import vardiff
from lab.prism import stratum_session
from lab.prism.prism_coordinator import (
    ClientState as FacadeClientState,
    PrismCoordinator,
    StratumError as FacadeStratumError,
    WorkerIdentity as FacadeWorkerIdentity,
)
from lab.prism.stratum_session import (
    ClientState,
    P2mrAddressValidator,
    SessionRegistry,
    StratumError,
    StratumListenerProfile,
    StratumSessionService,
    WorkerIdentity,
    client_can_receive_jobs,
    error_payload,
    parse_stratum_password_options,
    result_payload,
)


def listener() -> StratumListenerProfile:
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
        name="default",
        bind="127.0.0.1",
        port=3340,
        share_difficulty=Decimal("1"),
        vardiff_config=config,
        heartbeat_name="stratum_accept",
    )


def worker(username: str) -> WorkerIdentity:
    return WorkerIdentity(
        username=username,
        payout_address=username,
        worker_name=None,
        script_pubkey_hex="5220" + "11" * 32,
        p2mr_program_hex="11" * 32,
    )


class FakeSocket:
    def __init__(self) -> None:
        self.closed = threading.Event()
        self.sent: list[dict[str, object]] = []

    def settimeout(self, _timeout: object) -> None:
        return

    def setsockopt(self, *_args: object) -> None:
        return

    def shutdown(self, _how: object) -> None:
        self.closed.set()

    def close(self) -> None:
        self.closed.set()

    def sendall(self, _data: bytes) -> None:
        return


class FakeJobs:
    def __init__(self, registry: SessionRegistry) -> None:
        self.registry = registry
        self.cancelled: list[ClientState] = []
        self.cleaned: list[ClientState] = []
        self.retained = 0

    def note_collection_identity_available(self, _client: ClientState) -> None:
        return

    def request_initial_job_delivery(self, _client: ClientState) -> None:
        return

    def apply_client_difficulty_requests(self, _client: ClientState) -> Decimal | None:
        return None

    def advertise_client_difficulty(self, _client: ClientState, _target: Decimal) -> bool:
        return False

    def handle_submit(self, _client: ClientState, _params: list[object]) -> bool:
        return False

    def refresh_jobs_after_pending_accepted_block(self, _client: ClientState) -> None:
        return

    def cancel_pending_initial_job_locked(self, client: ClientState) -> None:
        self.cancelled.append(client)

    def cleanup_disconnected_client(self, client: ClientState) -> None:
        self.cleaned.append(client)
        with self.registry.lock:
            self.registry.clear_active_jobs_locked(client)
            client.authorized = False
            client.worker = None
            client.username = ""

    def retain_current_collection_refresh_if_unrepresented(self) -> None:
        self.retained += 1


class FakeProgress:
    def __init__(self, registry: SessionRegistry) -> None:
        self.registry = registry
        self.deliveries: list[tuple[int, object, float]] = []
        self.registry_had_proof_at_callback: list[bool] = []
        self.reconciles = 0

    def record_delivery(
        self, client: ClientState, context: object, delivered_monotonic: float
    ) -> None:
        snapshot = self.registry.eligible_snapshot()
        delivered = snapshot.get(client.connection_id)
        self.registry_had_proof_at_callback.append(
            delivered is not None
            and delivered.delivered is not None
            and delivered.delivered.context is context
        )
        self.deliveries.append(
            (client.connection_id, context, delivered_monotonic)
        )

    def reconcile_eligibility(self) -> None:
        self.reconciles += 1


class FailingThread:
    def start(self) -> None:
        raise RuntimeError("thread unavailable")


class FakeRuntime:
    def __init__(self) -> None:
        self.is_running = True
        self.setup_failures = 0
        self.registry_metrics: list[tuple[int, int]] = []

    def running(self) -> bool:
        return self.is_running

    def record_heartbeat(self, _name: str) -> None:
        return

    def wait_after_resource_failure(self, _heartbeat_name: str) -> None:
        return

    def record_resource_exhaustion(self, **_kwargs: object) -> None:
        return

    def record_setup_failure(self) -> int:
        self.setup_failures += 1
        return self.setup_failures

    def sync_registry_metrics(self, registry: SessionRegistry) -> None:
        self.registry_metrics.append(
            (len(registry.clients), registry.handler_thread_count)  # type: ignore[arg-type]
        )

    def max_connections(self) -> int:
        return 8

    def max_connections_per_username(self) -> int:
        return 1

    def client_startup_difficulty(self, _profile: StratumListenerProfile) -> Decimal:
        return Decimal("1")

    def apply_send_timeout(self, _sock: socket.socket) -> None:
        return

    def make_client_thread(self, _client: ClientState) -> FailingThread:
        return FailingThread()

    def extranonce2_size(self) -> int:
        return 8

    def version_mask(self) -> int:
        return 0x1FFFE000

    def username_fallback_address(self) -> str | None:
        return None

    def resolve_worker(self, _username: str, fallback: object) -> WorkerIdentity:
        return fallback()  # type: ignore[operator]

    def reserve_client_username(
        self,
        _client: ClientState,
        _worker: WorkerIdentity,
        fallback: object,
    ) -> bool:
        return bool(fallback())  # type: ignore[operator]

    def send_result(
        self, client: ClientState, request_id: object, result: object
    ) -> None:
        client.send(result_payload(request_id, result))

    def send_error(
        self,
        client: ClientState,
        request_id: object,
        code: int,
        message: str,
        *,
        reason: str | None,
    ) -> None:
        client.send(error_payload(request_id, code, message, reason=reason))

    def disconnect_client(self, _client: ClientState, fallback: object) -> None:
        fallback()  # type: ignore[operator]


def service_fixture(
    clients: object | None = None,
) -> tuple[StratumSessionService, SessionRegistry, FakeRuntime, FakeJobs, FakeProgress]:
    registry = SessionRegistry(
        lock=threading.RLock(),
        clients=set() if clients is None else clients,
    )
    runtime = FakeRuntime()
    jobs = FakeJobs(registry)
    progress = FakeProgress(registry)
    validator = P2mrAddressValidator(
        rpc_call=lambda _method, _params: {
            "isvalid": True,
            "scriptPubKey": "5220" + "11" * 32,
        },
        max_entries=lambda: 16,
        ttl_seconds=lambda: 60.0,
        cache=OrderedDict(),
    )
    service = StratumSessionService(
        registry=registry,
        runtime=runtime,
        jobs=jobs,
        progress=progress,
        address_validator=validator,
        pool_closed_reason="pool-closed",
    )
    return service, registry, runtime, jobs, progress


class SessionRegistryTests(unittest.TestCase):
    def test_admission_generation_and_global_capacity_are_one_atomic_registry_step(self) -> None:
        registry = SessionRegistry(lock=threading.RLock())
        profile = listener()

        first, first_rejection = registry.admit(
            sock=FakeSocket(),  # type: ignore[arg-type]
            address=("127.0.0.1", 1),
            profile=profile,
            share_difficulty=Decimal("1"),
            max_connections=1,
        )
        second, second_rejection = registry.admit(
            sock=FakeSocket(),  # type: ignore[arg-type]
            address=("127.0.0.1", 2),
            profile=profile,
            share_difficulty=Decimal("1"),
            max_connections=1,
        )

        assert first is not None
        self.assertEqual(first.connection_id, 1)
        self.assertEqual(first_rejection, 0)
        self.assertIsNone(second)
        self.assertEqual(second_rejection, 1)
        self.assertEqual(registry.connection_generation, 1)
        self.assertEqual(registry.clients, {first})

    def test_reauthorization_limit_preserves_prior_live_identity(self) -> None:
        first = ClientState(FakeSocket(), ("127.0.0.1", 1), 1, "00000001")  # type: ignore[arg-type]
        occupant = ClientState(FakeSocket(), ("127.0.0.1", 2), 2, "00000002")  # type: ignore[arg-type]
        registry = SessionRegistry(lock=threading.RLock(), clients={first, occupant})
        original = worker("original")
        full = worker("full")
        self.assertEqual(
            registry.reserve_username(
                first, original, max_connections_per_username=1
            ),
            (True, 0),
        )
        self.assertEqual(
            registry.reserve_username(
                occupant, full, max_connections_per_username=1
            ),
            (True, 0),
        )

        accepted, count = registry.reserve_username(
            first, full, max_connections_per_username=1
        )

        self.assertFalse(accepted)
        self.assertEqual(count, 1)
        self.assertIs(first.worker, original)
        self.assertEqual(first.username, "original")

    def test_eligibility_snapshot_is_immutable_exact_and_uses_delivered_context(self) -> None:
        current = ClientState(FakeSocket(), ("127.0.0.1", 1), 1, "00000001")  # type: ignore[arg-type]
        current.subscribed = current.authorized = True
        current.worker = worker("miner")
        current.username = "miner"
        idle = ClientState(FakeSocket(), ("127.0.0.1", 2), 2, "00000002")  # type: ignore[arg-type]
        clients = [current, idle]
        registry = SessionRegistry(lock=threading.RLock(), clients=clients)
        delivered = SimpleNamespace(name="delivered")
        current.active_job = SimpleNamespace(name="registered-before-send")
        registry.record_delivery(current, delivered, 42.0)
        current.active_job = SimpleNamespace(name="newer-unsent")

        snapshot = registry.eligible_snapshot()

        self.assertEqual(tuple(snapshot), (1,))
        self.assertIs(snapshot[1].delivered.context, delivered)  # type: ignore[union-attr]
        self.assertTrue(client_can_receive_jobs(current))
        with self.assertRaises(TypeError):
            snapshot[3] = snapshot[1]  # type: ignore[index]

        # Reauthorization does not erase a valid socket-delivery proof.
        current.worker = worker("replacement")
        self.assertIs(
            registry.eligible_snapshot()[1].delivered.context,  # type: ignore[union-attr]
            delivered,
        )
        with registry.lock:
            self.assertTrue(registry.begin_retirement_locked(current))
        self.assertFalse(registry.eligible_snapshot())

    def test_active_job_registration_is_registry_owned(self) -> None:
        state = ClientState(FakeSocket(), ("127.0.0.1", 1), 1, "00000001")  # type: ignore[arg-type]
        state.active_job_ids.update({"old-a", "old-b"})
        registry = SessionRegistry(lock=threading.RLock(), clients={state})
        context = SimpleNamespace(job=SimpleNamespace(job_id="new"))

        with registry.lock:
            retired = registry.register_active_job_locked(
                state, context, job_id="new", clean_jobs=True
            )

        self.assertEqual(set(retired), {"old-a", "old-b"})
        self.assertIs(state.active_job, context)
        self.assertEqual(state.active_job_ids, {"new"})

    def test_ordered_membership_can_be_adopted_before_and_after_use(self) -> None:
        first = ClientState(FakeSocket(), ("127.0.0.1", 1), 1, "00000001")  # type: ignore[arg-type]
        second = ClientState(FakeSocket(), ("127.0.0.1", 2), 2, "00000002")  # type: ignore[arg-type]
        registry = SessionRegistry(lock=threading.RLock(), clients=[first, second])
        self.assertIsInstance(registry.clients, list)
        self.assertEqual(registry.clients, [first, second])

        replacement = [second, first]
        registry.adopt_clients(replacement)

        self.assertIs(registry.clients, replacement)
        self.assertEqual(registry.clients, [second, first])

    def test_coordinator_registry_adopts_ordered_membership_replacement(self) -> None:
        first = ClientState(FakeSocket(), ("127.0.0.1", 1), 1, "00000001")  # type: ignore[arg-type]
        second = ClientState(FakeSocket(), ("127.0.0.1", 2), 2, "00000002")  # type: ignore[arg-type]
        coordinator = PrismCoordinator.__new__(PrismCoordinator)
        coordinator.lock = threading.RLock()
        coordinator.clients = [first, second]  # type: ignore[assignment]
        coordinator.connection_counter = 0

        registry = coordinator._ensure_session_registry()
        self.assertIs(registry.clients, coordinator.clients)
        self.assertEqual(registry.connection_generation, 2)
        first.handler_thread_registered = True
        replacement = [second, first]
        coordinator.clients = replacement  # type: ignore[assignment]

        self.assertIs(coordinator._ensure_session_registry(), registry)
        self.assertIs(registry.clients, replacement)
        self.assertEqual(registry.clients, [second, first])
        self.assertEqual(registry.handler_thread_count, 1)
        self.assertEqual(coordinator.connection_counter, 2)
        self.assertEqual(coordinator.handler_thread_count, 1)


class SessionLifecycleTests(unittest.TestCase):
    def test_handler_thread_failure_rolls_back_membership_and_socket(self) -> None:
        service, registry, runtime, _jobs, _progress = service_fixture()
        accepted = FakeSocket()

        class Listener:
            calls = 0

            def accept(self) -> tuple[FakeSocket, tuple[str, int]]:
                self.calls += 1
                if self.calls == 1:
                    return accepted, ("127.0.0.1", 1)
                runtime.is_running = False
                raise socket.timeout

        service.accept_loop(Listener(), listener())  # type: ignore[arg-type]

        self.assertTrue(accepted.closed.is_set())
        self.assertFalse(registry.clients)
        self.assertEqual(registry.handler_thread_count, 0)
        self.assertEqual(runtime.setup_failures, 1)

    def test_disconnect_closes_socket_before_waiting_for_job_update_lock(self) -> None:
        service, registry, _runtime, jobs, progress = service_fixture()
        sock = FakeSocket()
        state = ClientState(sock, ("127.0.0.1", 1), 1, "00000001")  # type: ignore[arg-type]
        registry._add_client_locked(state)
        state.job_update_lock.acquire()
        finished = threading.Event()

        def disconnect() -> None:
            service.disconnect_client(state)
            finished.set()

        thread = threading.Thread(target=disconnect)
        thread.start()
        self.assertTrue(sock.closed.wait(1.0))
        self.assertFalse(finished.is_set())
        self.assertNotIn(state, registry.clients)
        state.job_update_lock.release()
        thread.join(1.0)

        self.assertTrue(finished.is_set())
        self.assertEqual(jobs.cancelled, [state])
        self.assertEqual(jobs.cleaned, [state])
        self.assertEqual(progress.reconciles, 1)
        service.disconnect_client(state)
        self.assertEqual(jobs.cleaned, [state])

    def test_successful_delivery_records_registry_proof_before_health_callback(self) -> None:
        service, registry, _runtime, _jobs, progress = service_fixture()
        state = ClientState(FakeSocket(), ("127.0.0.1", 1), 1, "00000001")  # type: ignore[arg-type]
        state.subscribed = state.authorized = True
        state.worker = worker("miner")
        registry._add_client_locked(state)
        context = SimpleNamespace(name="sent")

        service.record_successful_delivery(state, context, 12.5)

        self.assertIs(
            registry.eligible_snapshot()[1].delivered.context,  # type: ignore[union-attr]
            context,
        )
        self.assertEqual(progress.deliveries, [(1, context, 12.5)])
        self.assertEqual(progress.registry_had_proof_at_callback, [True])
        self.assertEqual(progress.reconciles, 1)

    def test_retired_client_delivery_cannot_reach_progress_health(self) -> None:
        service, registry, _runtime, _jobs, progress = service_fixture()
        state = ClientState(FakeSocket(), ("127.0.0.1", 1), 1, "00000001")  # type: ignore[arg-type]
        state.subscribed = state.authorized = True
        state.worker = worker("miner")
        registry._add_client_locked(state)
        with registry.lock:
            self.assertTrue(registry.begin_retirement_locked(state))

        service.record_successful_delivery(
            state,
            SimpleNamespace(name="sent-before-retirement-won"),
            12.5,
        )

        self.assertFalse(registry.eligible_snapshot())
        self.assertEqual(progress.deliveries, [])
        self.assertEqual(progress.registry_had_proof_at_callback, [])
        self.assertEqual(progress.reconciles, 0)


class CompatibilityTests(unittest.TestCase):
    def test_coordinator_reexports_exact_session_model_identities(self) -> None:
        self.assertIs(FacadeClientState, ClientState)
        self.assertIs(FacadeWorkerIdentity, WorkerIdentity)
        self.assertIs(FacadeStratumError, StratumError)
        self.assertFalse(hasattr(stratum_session, "PrismCoordinator"))

    def test_protocol_helpers_preserve_payload_and_password_behavior(self) -> None:
        self.assertEqual(result_payload(1, True), {"id": 1, "result": True, "error": None})
        self.assertEqual(
            error_payload(2, 21, "stale", reason="stale-job"),
            {
                "id": 2,
                "result": None,
                "error": [21, "stale", {"reason_id": "stale-job"}],
            },
        )
        self.assertEqual(
            parse_stratum_password_options("x,md=4,d=8,bad=1"),
            (Decimal("8"), Decimal("4")),
        )


if __name__ == "__main__":
    unittest.main()
