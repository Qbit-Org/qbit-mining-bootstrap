#!/usr/bin/env python3
"""Deterministic tests for the extracted PRISM Stratum session boundary.

The current service resolves the coordinator through one call-time
``SessionRuntime`` typed port (not the old per-seam collaborator objects), so
these tests drive :class:`StratumSessionService` with a recording runtime and
exercise :class:`SessionRegistry` directly where the contract is registry-owned.
"""

from __future__ import annotations

import socket
import threading
import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from lab.auxpow import vardiff
from lab.prism import stratum_session
from lab.prism.coordinator_config import StratumListenerProfile
from lab.prism.prism_coordinator import (
    ClientState as FacadeClientState,
    PrismCoordinator,
    StratumError as FacadeStratumError,
    WorkerIdentity as FacadeWorkerIdentity,
)
from lab.prism.stratum_session import (
    ClientState,
    SessionRegistry,
    StratumError,
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
        self.sent: list[bytes] = []

    def settimeout(self, _timeout: object) -> None:
        return

    def setsockopt(self, *_args: object) -> None:
        return

    def shutdown(self, _how: object) -> None:
        self.closed.set()

    def close(self) -> None:
        self.closed.set()

    def sendall(self, data: bytes) -> None:
        self.sent.append(data)


def client_state(connection_id: int) -> ClientState:
    return ClientState(
        FakeSocket(),  # type: ignore[arg-type]
        ("127.0.0.1", connection_id),
        connection_id,
        f"{connection_id:08x}",
    )


class FakeProgressService:
    """G1 stand-in that records callback-time registry proof visibility."""

    def __init__(self, registry: SessionRegistry) -> None:
        self.registry = registry
        self.proofs: list[object] = []
        self.registry_had_proof_at_callback: list[bool] = []

    def record_delivery(
        self,
        proof: object,
        ready_mode_required: bool,
        *,
        matches_latest_tip: bool,
    ) -> bool:
        snapshot = self.registry.eligible_snapshot()
        delivered = snapshot.get(proof.connection_id)  # type: ignore[union-attr]
        self.registry_had_proof_at_callback.append(
            delivered is not None and delivered.delivered is not None
        )
        self.proofs.append(proof)
        return True


class FakeSessionRuntime:
    """Recording call-time SessionRuntime port for the session service."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.stratum_max_connections = 8
        self.stratum_max_connections_per_username = 1
        self._pool_ready_latched = False
        self.latest_detected_tip = None
        self.rpc = SimpleNamespace(
            call=lambda _method, _params: {
                "isvalid": True,
                "scriptPubKey": "5220" + "11" * 32,
            }
        )
        self.service: StratumSessionService | None = None
        self.progress: FakeProgressService | None = None
        self.cancelled: list[ClientState] = []
        self.cleaned: list[ClientState] = []
        self.retained = 0
        self.activity: list[float] = []
        self.reconciles: list[float] = []
        self.coverage: list[object] = []
        self.send_timeout_error: BaseException | None = None
        self._controller = SimpleNamespace(phase="running")

    # -- lifecycle plumbing -------------------------------------------------

    def _record_heartbeat(self, _name: str, *, phase: str | None = None) -> None:
        return None

    def _ensure_shutdown_controller(self) -> object:
        return self._controller

    def _ensure_initial_job_state(self) -> None:
        return None

    def client_startup_difficulty(self, _profile: StratumListenerProfile) -> Decimal:
        return Decimal("1")

    def apply_stratum_send_timeout(self, _sock: object) -> None:
        if self.send_timeout_error is not None:
            raise self.send_timeout_error

    def handle_client(self, _client: ClientState) -> None:
        return None

    def disconnect_client(self, client: ClientState) -> None:
        assert self.service is not None
        self.service.disconnect_client(client)

    def _cancel_pending_initial_job_locked(
        self, client: ClientState, *, count: bool
    ) -> None:
        self.cancelled.append(client)

    def _ensure_job_delivery_service(self) -> object:
        return SimpleNamespace(cleanup_disconnected_client=self.cleaned.append)

    def _retain_current_collection_refresh_if_unrepresented(self) -> None:
        self.retained += 1

    def _record_stratum_resource_exhaustion(self, **_kwargs: object) -> None:
        return None

    def _wait_after_stratum_resource_failure(self, _heartbeat_name: str) -> None:
        return None

    # -- delivery/progress plumbing ----------------------------------------

    def _ensure_job_cache_state(self) -> None:
        return None

    def _ensure_progress_health_service(self) -> object:
        assert self.progress is not None
        return self.progress

    def _tip_refresh_epoch_coverage_reached_locked(
        self,
        _client: ClientState,
        _context: object,
        _delivered_monotonic: float,
    ) -> list[tuple[str, float]]:
        return []

    def _progress_note_refresh_activity(self, delivered_monotonic: float) -> None:
        self.activity.append(delivered_monotonic)

    def _progress_reconcile_pending(self, *, now: float) -> None:
        self.reconciles.append(now)

    def _record_tip_refresh_epoch_coverage(self, coverage: object) -> None:
        self.coverage.append(coverage)

    def _handle_request(self, _client: ClientState, _request: dict[str, object]) -> None:
        raise _DispatchShutdown("writer admission closed")


class _DispatchShutdown(RuntimeError):
    pass


def service_fixture() -> tuple[StratumSessionService, SessionRegistry, FakeSessionRuntime]:
    runtime = FakeSessionRuntime()
    service = StratumSessionService(
        runtime,
        shutdown_error=_DispatchShutdown,
        pool_closed_reason="pool-closed",
    )
    runtime.service = service
    runtime.progress = FakeProgressService(service.registry)
    return service, service.registry, runtime


class SessionRegistryTests(unittest.TestCase):
    def test_registry_adopts_seeded_clients_and_derives_generation_and_counts(
        self,
    ) -> None:
        early = client_state(3)
        late = client_state(7)
        late.handler_thread_registered = True

        registry = SessionRegistry(clients={early, late})

        self.assertEqual(registry.connection_generation, 7)
        self.assertEqual(registry.handler_thread_count, 1)
        self.assertEqual(registry.peak_active_connections, 2)
        self.assertEqual(registry.note_rejection_locked("global"), 1)
        self.assertEqual(registry.note_rejection_locked("global"), 2)
        self.assertEqual(registry.rejection_counts["global"], 2)

    def test_eligibility_snapshot_is_immutable_exact_and_uses_delivered_context(
        self,
    ) -> None:
        current = client_state(1)
        current.subscribed = current.authorized = True
        current.worker = worker("miner")
        current.username = "miner"
        idle = client_state(2)
        registry = SessionRegistry(clients=[current, idle])
        delivered = SimpleNamespace(name="delivered")
        current.active_job = SimpleNamespace(name="registered-before-send")
        registry.record_delivery_locked(current, delivered, 42.0)
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
        registry.clients = set(registry.clients)
        self.assertTrue(registry.begin_retirement_locked(current))
        self.assertFalse(registry.eligible_snapshot())

    def test_retirement_claim_is_single_owner_and_clears_delivery_proof(self) -> None:
        state = client_state(1)
        registry = SessionRegistry(clients={state})
        registry.record_delivery_locked(state, SimpleNamespace(name="sent"), 1.0)
        self.assertIn(1, registry._delivered_by_connection)

        self.assertTrue(registry.begin_retirement_locked(state))
        self.assertNotIn(state, registry.clients)
        self.assertEqual(registry._delivered_by_connection, {})
        self.assertFalse(registry.begin_retirement_locked(state))

    def test_ordered_membership_can_be_adopted_before_and_after_use(self) -> None:
        first = client_state(1)
        second = client_state(2)
        registry = SessionRegistry(clients=[first, second])
        self.assertIsInstance(registry.clients, list)
        self.assertEqual(registry.clients, [first, second])

        replacement = [second, first]
        registry.adopt_clients(replacement)

        self.assertIs(registry.clients, replacement)
        self.assertEqual(registry.clients, [second, first])

    def test_coordinator_registry_adopts_ordered_membership_replacement(self) -> None:
        first = client_state(1)
        second = client_state(2)
        coordinator = PrismCoordinator.__new__(PrismCoordinator)
        coordinator.lock = threading.RLock()
        coordinator.connection_counter = 0
        coordinator.clients = [first, second]  # type: ignore[assignment]

        registry = coordinator._ensure_stratum_session_service().registry
        self.assertIs(registry.clients, coordinator.clients)
        self.assertEqual(registry.connection_generation, 2)
        first.handler_thread_registered = True
        replacement = [second, first]
        coordinator.clients = replacement  # type: ignore[assignment]

        self.assertIs(
            coordinator._ensure_stratum_session_service().registry, registry
        )
        self.assertIs(registry.clients, replacement)
        self.assertEqual(registry.clients, [second, first])
        self.assertEqual(registry.handler_thread_count, 1)
        self.assertEqual(coordinator.connection_counter, 2)
        self.assertEqual(coordinator.handler_thread_count, 1)


class SessionLifecycleTests(unittest.TestCase):
    def test_username_reservation_enforces_capacity_and_preserves_identity(
        self,
    ) -> None:
        service, registry, _runtime = service_fixture()
        first = client_state(1)
        occupant = client_state(2)
        registry.clients.update({first, occupant})
        original = worker("original")
        full = worker("full")

        self.assertTrue(service.reserve_client_username(first, original))
        self.assertTrue(service.reserve_client_username(occupant, full))

        with patch("builtins.print"):
            accepted = service.reserve_client_username(first, full)

        self.assertFalse(accepted)
        self.assertIs(first.worker, original)
        self.assertEqual(first.username, "original")
        self.assertEqual(registry.rejection_counts["username"], 1)

    def test_handler_setup_failure_rolls_back_membership_and_socket(self) -> None:
        service, registry, runtime = service_fixture()
        runtime.send_timeout_error = RuntimeError("socket setup unavailable")
        accepted = FakeSocket()

        class Listener:
            calls = 0

            def accept(self) -> tuple[FakeSocket, tuple[str, int]]:
                self.calls += 1
                if self.calls == 1:
                    return accepted, ("127.0.0.1", 1)
                runtime.stop_event.set()
                raise socket.timeout

        with patch("builtins.print"):
            service.accept_loop(Listener(), listener())  # type: ignore[arg-type]

        self.assertTrue(accepted.closed.is_set())
        self.assertFalse(registry.clients)
        self.assertEqual(registry.handler_thread_count, 0)
        self.assertEqual(service.connection_setup_failure_count, 1)

    def test_disconnect_closes_socket_before_waiting_for_job_update_lock(self) -> None:
        service, registry, runtime = service_fixture()
        state = client_state(1)
        sock = state.sock
        registry.clients.add(state)
        state.job_update_lock.acquire()
        finished = threading.Event()

        def disconnect() -> None:
            service.disconnect_client(state)
            finished.set()

        thread = threading.Thread(target=disconnect)
        thread.start()
        self.assertTrue(sock.closed.wait(1.0))  # type: ignore[union-attr]
        self.assertFalse(finished.is_set())
        self.assertNotIn(state, registry.clients)
        state.job_update_lock.release()
        thread.join(1.0)

        self.assertTrue(finished.is_set())
        self.assertEqual(runtime.cancelled, [state])
        self.assertEqual(runtime.cleaned, [state])
        self.assertEqual(runtime.retained, 1)
        # Only the first caller owns socket close and final cleanup.
        service.disconnect_client(state)
        self.assertEqual(runtime.cleaned, [state])

    def test_successful_delivery_records_registry_proof_before_health_callback(
        self,
    ) -> None:
        service, registry, runtime = service_fixture()
        progress = runtime.progress
        assert progress is not None
        state = client_state(1)
        state.subscribed = state.authorized = True
        state.worker = worker("miner")
        state.username = "miner"
        registry.clients.add(state)
        context = SimpleNamespace(
            name="sent",
            template={"previousblockhash": "00" * 32},
            template_fingerprint="fp-1",
            template_generation=4,
            payout_state_generation=2,
            collection_only=False,
        )

        service.record_successful_delivery(state, context, 12.5)

        self.assertIs(
            registry.eligible_snapshot()[1].delivered.context,  # type: ignore[union-attr]
            context,
        )
        self.assertEqual(progress.registry_had_proof_at_callback, [True])
        self.assertEqual(len(progress.proofs), 1)
        proof = progress.proofs[0]
        self.assertEqual(proof.connection_id, 1)  # type: ignore[union-attr]
        self.assertEqual(proof.delivered_monotonic, 12.5)  # type: ignore[union-attr]
        # The staged-split compatibility mirrors advance in the same critical
        # section as the registry proof.
        self.assertIs(state._progress_delivered_context, context)
        self.assertEqual(state._progress_delivered_template_generation, 4)
        self.assertEqual(runtime.activity, [12.5])
        self.assertEqual(runtime.reconciles, [12.5])
        self.assertEqual(runtime.coverage, [[]])

    def test_retired_client_leaves_no_eligible_snapshot_entry(self) -> None:
        service, registry, _runtime = service_fixture()
        state = client_state(1)
        state.subscribed = state.authorized = True
        state.worker = worker("miner")
        registry.clients.add(state)
        self.assertTrue(registry.begin_retirement_locked(state))

        registry.record_delivery_locked(
            state, SimpleNamespace(name="sent-before-retirement-won"), 12.5
        )

        # A retired connection is invisible to fanout eligibility even if a
        # racing delivery commits proof after retirement claimed the client.
        self.assertFalse(registry.eligible_snapshot())

    def test_shutdown_race_in_dispatch_maps_to_pool_closed_stratum_error(self) -> None:
        service, _registry, _runtime = service_fixture()
        state = client_state(1)

        with self.assertRaises(StratumError) as caught:
            service.handle_request(state, {"id": 1, "method": "mining.submit"})

        error = caught.exception
        self.assertEqual(error.code, 20)
        self.assertEqual(error.reason, "pool-closed")
        self.assertTrue(error.disconnect)
        self.assertIsInstance(error.__cause__, _DispatchShutdown)


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
