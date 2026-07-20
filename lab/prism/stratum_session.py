"""PRISM Stratum listener and session lifecycle.

This module owns miner-facing connection admission, protocol dispatch, worker
resolution, and the live-session registry.  It deliberately knows nothing
about :class:`PrismCoordinator`: construction-root adapters provide the few
job-delivery, progress-health, and runtime operations required by a session.
"""

from __future__ import annotations

from collections import OrderedDict
from contextlib import ExitStack
from dataclasses import dataclass, field
from decimal import Decimal
import errno
import json
import socket
import struct
import threading
import time
import traceback
from types import MappingProxyType
from typing import Callable, Mapping, Protocol

from lab.auxpow import stratum_codec, vardiff
from lab.prism import direct_stratum
from lab.prism.coordinator_config import (
    StratumListenerProfile,
    load_prism_highdiff_listener,  # noqa: F401 - compatibility re-export
)


_VARDIFF_LOCK_INITIALIZATION_LOCK = threading.Lock()
from lab.prism.coordinator_shutdown import ShutdownInProgress


@dataclass(frozen=True)
class WorkerIdentity:
    username: str
    payout_address: str
    worker_name: str | None
    script_pubkey_hex: str
    p2mr_program_hex: str


@dataclass(eq=False)
class ClientState:
    sock: socket.socket
    address: tuple[str, int]
    connection_id: int
    extranonce1_hex: str
    subscribed: bool = False
    authorized: bool = False
    authorization_generation: int = 0
    difficulty_generation: int = 0
    authorized_monotonic: float | None = None
    username: str = ""
    worker: WorkerIdentity | None = None
    version_mask: int = 0
    active_job: object | None = None
    # Compatibility mirrors. SessionRegistry is authoritative for delivery
    # proof, but these fields remain available throughout the staged split.
    _progress_delivered_context: object | None = None
    _progress_delivered_template_fingerprint: str | None = None
    _progress_delivered_template_generation: int = 0
    _progress_delivered_payout_generation: int = -1
    _progress_delivered_monotonic: float | None = None
    listener_name: str = "default"
    listener_vardiff_config: vardiff.VardiffConfig | None = None
    minimum_advertised_difficulty: Decimal = Decimal("0")
    vardiff_config: vardiff.VardiffConfig | None = None
    requested_difficulty: Decimal | None = None
    requested_min_difficulty: Decimal | None = None
    suggested_difficulty: Decimal | None = None
    share_difficulty: Decimal = Decimal("1")
    pending_share_difficulty: Decimal | None = None
    vardiff_window_started_monotonic: float = field(default_factory=time.monotonic)
    vardiff_window_accepted: int = 0
    vardiff_window_submitted: int = 0
    vardiff_window_work: Decimal = Decimal("0")
    vardiff_difficulty_estimate: Decimal | None = None
    # Serializes vardiff/request state without involving the coordinator's
    # control-plane lock. Delivery paths take this before publication locks.
    vardiff_lock: threading.RLock = field(default_factory=threading.RLock)
    active_job_ids: set[str] = field(default_factory=set)
    post_accept_refresh_block: tuple[int, str] | None = None
    tip_work_delivered: tuple[str, float] | None = None
    closing: bool = False
    job_update_lock: threading.RLock = field(default_factory=threading.RLock)
    send_lock: threading.Lock = field(default_factory=threading.Lock)
    handler_thread_registered: bool = False

    def send(self, payload: dict[str, object]) -> None:
        data = json.dumps(payload).encode() + b"\n"
        with self.send_lock:
            self.sock.sendall(data)

    def send_batch(self, payloads: list[dict[str, object]]) -> None:
        # Focused tests and embedders replace send with a recorder. Preserve
        # that seam while normal sockets write a paired update atomically.
        if "send" in self.__dict__:
            for payload in payloads:
                self.send(payload)
            return
        data = b"".join(json.dumps(payload).encode() + b"\n" for payload in payloads)
        with self.send_lock:
            self.sock.sendall(data)

    def close(self) -> None:
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass


def client_vardiff_lock(client: ClientState) -> threading.RLock:
    """Return the per-client vardiff lock, including lightweight embedders."""
    lock = getattr(client, "vardiff_lock", None)
    if lock is not None:
        return lock
    with _VARDIFF_LOCK_INITIALIZATION_LOCK:
        lock = getattr(client, "vardiff_lock", None)
        if lock is None:
            lock = threading.RLock()
            client.vardiff_lock = lock
    return lock


class StratumError(RuntimeError):
    def __init__(
        self,
        code: int,
        message: str,
        *,
        reason: str | None = None,
        disconnect: bool = False,
    ):
        super().__init__(message)
        self.code = code
        self.message = message
        self.reason = reason
        self.disconnect = disconnect


def parse_stratum_password_options(password: str) -> tuple[Decimal | None, Decimal | None]:
    """Extract pool-side d=N / md=N options, ignoring unknown miner tokens."""
    requested: Decimal | None = None
    requested_min: Decimal | None = None
    for token in password.split(","):
        key, separator, raw_value = token.strip().partition("=")
        if not separator:
            continue
        key = key.strip().lower()
        if key not in {"d", "md"}:
            continue
        try:
            value = Decimal(raw_value.strip())
        except Exception:
            continue
        if not value.is_finite() or value <= 0:
            continue
        if key == "d":
            requested = value
        else:
            requested_min = value
    return requested, requested_min


def parse_worker_username(username: str) -> tuple[str, str | None]:
    payout_address, worker_name = split_worker_username(username)
    if not payout_address:
        raise StratumError(20, "username base is empty")
    return payout_address, worker_name


def split_worker_username(username: str) -> tuple[str, str | None]:
    payout_address, separator, worker_name = username.partition(".")
    return payout_address, worker_name if separator else None


def result_payload(request_id: object, result: object) -> dict[str, object]:
    return {"id": request_id, "result": result, "error": None}


def error_payload(
    request_id: object,
    code: int,
    message: str,
    *,
    reason: str | None = None,
) -> dict[str, object]:
    data = {"reason_id": reason} if reason is not None else None
    return {"id": request_id, "result": None, "error": [code, message, data]}


def difficulty_payload(difficulty: Decimal) -> dict[str, object]:
    return {
        "id": None,
        "method": "mining.set_difficulty",
        "params": [float(difficulty)],
    }


def job_payload(job: direct_stratum.DirectQbitStratumJob) -> dict[str, object]:
    return {
        "id": None,
        "method": "mining.notify",
        "params": [
            job.job_id,
            job.prevhash,
            job.coinb1,
            job.coinb2,
            list(job.merkle_branch),
            job.version,
            job.nbits,
            job.ntime,
            job.clean_jobs,
        ],
    }


def client_can_receive_jobs(client: ClientState) -> bool:
    return (
        not getattr(client, "closing", False)
        and client.subscribed
        and client.authorized
        and client.worker is not None
    )


def stratum_accept_heartbeat_names(
    profiles: list[StratumListenerProfile] | tuple[StratumListenerProfile, ...] | None,
) -> tuple[str, ...]:
    if not profiles:
        return ("stratum_accept",)
    return tuple(profile.heartbeat_name for profile in profiles)


@dataclass(frozen=True)
class DeliveredSessionContext:
    context: object
    delivered_monotonic: float


@dataclass(frozen=True)
class EligibleSession:
    connection_id: int
    delivered: DeliveredSessionContext | None


class SessionRegistry:
    """Atomic owner of live membership and connection-scoped session facts."""

    def __init__(
        self,
        *,
        lock: threading.RLock,
        clients: object | None = None,
        connection_generation: int = 0,
        rejection_counts: dict[str, int] | None = None,
    ) -> None:
        self.lock = lock
        self.clients = clients if clients is not None else set()
        self.connection_generation = max(
            int(connection_generation),
            max(
                (int(client.connection_id) for client in self.clients),  # type: ignore[union-attr]
                default=0,
            ),
        )
        self.rejection_counts = (
            rejection_counts
            if rejection_counts is not None
            else {"global": 0, "username": 0}
        )
        self.peak_active_connections = len(self.clients)
        self.handler_thread_count = sum(
            int(getattr(client, "handler_thread_registered", False))
            for client in self.clients
        )
        self._delivered_by_connection: dict[int, DeliveredSessionContext] = {}

    def adopt_clients(self, clients: object) -> None:
        """Adopt a compatibility replacement without changing its order/type."""
        with self.lock:
            self.clients = clients
            live_ids = {client.connection_id for client in clients}  # type: ignore[union-attr]
            self._delivered_by_connection = {
                connection_id: delivered
                for connection_id, delivered in self._delivered_by_connection.items()
                if connection_id in live_ids
            }
            self.peak_active_connections = max(
                self.peak_active_connections,
                len(clients),  # type: ignore[arg-type]
            )
            self.connection_generation = max(
                self.connection_generation,
                max(
                    (int(client.connection_id) for client in clients),  # type: ignore[union-attr]
                    default=0,
                ),
            )
            self.handler_thread_count = sum(
                int(getattr(client, "handler_thread_registered", False))
                for client in clients  # type: ignore[union-attr]
            )

    def _add_client_locked(self, client: ClientState) -> None:
        add = getattr(self.clients, "add", None)
        if callable(add):
            add(client)
            return
        append = getattr(self.clients, "append", None)
        if callable(append):
            append(client)
            return
        raise TypeError("session membership must support add() or append()")

    def _discard_client_locked(self, client: ClientState) -> None:
        discard = getattr(self.clients, "discard", None)
        if callable(discard):
            discard(client)
            return
        remove = getattr(self.clients, "remove", None)
        if callable(remove):
            try:
                remove(client)
            except ValueError:
                pass
            return
        raise TypeError("session membership must support discard() or remove()")

    def _note_rejection_locked(self, scope: str) -> int:
        count = int(self.rejection_counts.get(scope, 0)) + 1
        self.rejection_counts[scope] = count
        return count

    def admit(
        self,
        *,
        sock: socket.socket,
        address: tuple[str, int],
        profile: StratumListenerProfile,
        share_difficulty: Decimal,
        max_connections: int,
    ) -> tuple[ClientState | None, int]:
        with self.lock:
            if max_connections > 0 and len(self.clients) >= max_connections:
                return None, self._note_rejection_locked("global")
            self.connection_generation += 1
            connection_id = self.connection_generation
            client = ClientState(
                sock=sock,
                address=address,
                connection_id=connection_id,
                extranonce1_hex=f"{connection_id & 0xFFFFFFFF:08x}",
                listener_name=profile.name,
                listener_vardiff_config=profile.vardiff_config,
                minimum_advertised_difficulty=profile.minimum_advertised_difficulty,
                share_difficulty=share_difficulty,
            )
            self._add_client_locked(client)
            self.peak_active_connections = max(
                self.peak_active_connections, len(self.clients)
            )
            return client, 0

    def reserve_username(
        self,
        client: ClientState,
        worker: WorkerIdentity,
        *,
        max_connections_per_username: int,
    ) -> tuple[bool, int]:
        with self.lock:
            active_for_username = sum(
                1
                for other in self.clients
                if (
                    other is not client
                    and other.worker is not None
                    and other.username == worker.username
                )
            )
            if (
                max_connections_per_username > 0
                and active_for_username >= max_connections_per_username
            ):
                return False, self._note_rejection_locked("username")
            # Commit the replacement only after capacity validation. A failed
            # reauthorization therefore leaves the prior live identity intact.
            client.worker = worker
            client.username = worker.username
            return True, 0

    def register_handler(self, client: ClientState) -> None:
        with self.lock:
            if client.handler_thread_registered:
                return
            client.handler_thread_registered = True
            self.handler_thread_count += 1

    def unregister_handler(self, client: ClientState) -> None:
        with self.lock:
            if not client.handler_thread_registered:
                return
            client.handler_thread_registered = False
            self.handler_thread_count = max(0, self.handler_thread_count - 1)

    def begin_retirement_locked(self, client: ClientState) -> bool:
        if getattr(client, "closing", False) and client not in self.clients:
            return False
        client.closing = True
        self._discard_client_locked(client)
        self._delivered_by_connection.pop(client.connection_id, None)
        return True

    def register_active_job_locked(
        self,
        client: ClientState,
        context: object,
        *,
        job_id: str,
        clean_jobs: bool,
    ) -> tuple[str, ...]:
        retired = tuple(client.active_job_ids) if clean_jobs else ()
        if clean_jobs:
            client.active_job_ids.clear()
        client.active_job = context
        client.active_job_ids.add(job_id)
        return retired

    def clear_active_jobs_locked(self, client: ClientState) -> tuple[str, ...]:
        retired = tuple(client.active_job_ids)
        client.active_job_ids.clear()
        client.active_job = None
        return retired

    def record_delivery(
        self,
        client: ClientState,
        context: object,
        delivered_monotonic: float,
    ) -> bool:
        with self.lock:
            return self.record_delivery_locked(
                client,
                context,
                delivered_monotonic,
            )

    def record_delivery_locked(
        self,
        client: ClientState,
        context: object,
        delivered_monotonic: float,
    ) -> bool:
        """Commit proof while the shared registry lock is already held."""
        if client not in self.clients or client.closing:
            return False
        delivered = DeliveredSessionContext(context, delivered_monotonic)
        self._delivered_by_connection[client.connection_id] = delivered
        # Compatibility mirrors for staged callers/tests.
        client._progress_delivered_context = context
        client._progress_delivered_monotonic = delivered_monotonic
        return True

    def eligible_snapshot(self) -> Mapping[int, EligibleSession]:
        """Return an immutable exact client_can_receive_jobs population."""
        with self.lock:
            captured: dict[int, EligibleSession] = {}
            for client in self.clients:
                if not client_can_receive_jobs(client):
                    continue
                delivered = self._delivered_by_connection.get(client.connection_id)
                if delivered is None and client._progress_delivered_context is not None:
                    delivered = DeliveredSessionContext(
                        client._progress_delivered_context,
                        float(client._progress_delivered_monotonic or 0.0),
                    )
                captured[client.connection_id] = EligibleSession(
                    connection_id=client.connection_id,
                    delivered=delivered,
                )
            return MappingProxyType(captured)


@dataclass
class _P2mrAddressValidationFlight:
    event: threading.Event = field(default_factory=threading.Event)
    result: tuple[str, str] | None = None
    error: BaseException | None = None
    waiters: int = 0


class P2mrAddressValidator:
    """Bounded LRU and singleflight wrapper around validateaddress RPC."""

    def __init__(
        self,
        *,
        rpc_call: Callable[[str, list[object]], object],
        max_entries: Callable[[], int],
        ttl_seconds: Callable[[], float],
        cache_lock: threading.Lock | None = None,
        cache: OrderedDict[str, tuple[float, tuple[str, str]]] | None = None,
        inflight: dict[str, _P2mrAddressValidationFlight] | None = None,
    ) -> None:
        self.rpc_call = rpc_call
        self.max_entries = max_entries
        self.ttl_seconds = ttl_seconds
        self.cache_lock = cache_lock if cache_lock is not None else threading.Lock()
        self.cache = cache if cache is not None else OrderedDict()
        self.inflight = inflight if inflight is not None else {}

    def validate(self, address: str, *, label: str) -> tuple[str, str]:
        with self.cache_lock:
            cached = self.cache.get(address)
            if cached is not None:
                expires_monotonic, cached_result = cached
                if expires_monotonic > time.monotonic():
                    self.cache.move_to_end(address)
                    return cached_result
                self.cache.pop(address, None)
            pending = self.inflight.get(address)
            is_leader = pending is None
            if pending is None:
                pending = _P2mrAddressValidationFlight()
                self.inflight[address] = pending
            else:
                pending.waiters += 1

        if not is_leader:
            pending.event.wait()
            if pending.result is not None:
                return pending.result
            if pending.error is not None:
                self._raise_shared_error(pending.error)
            raise RuntimeError("payout address validation completed without a result")

        try:
            validation = self.rpc_call("validateaddress", [address])
            if not isinstance(validation, dict) or not validation.get("isvalid"):
                raise StratumError(20, f"{label} is not a valid qbit address: {address}")
            script = str(validation.get("scriptPubKey") or "")
            if not script.startswith("5220") or len(script) != 68:
                raise StratumError(20, f"{label} does not resolve to a P2MR script: {address}")
            result = (script, script[4:])
            with self.cache_lock:
                max_entries = int(self.max_entries())
                ttl_seconds = float(self.ttl_seconds())
                if max_entries > 0 and ttl_seconds > 0:
                    self.cache[address] = (time.monotonic() + ttl_seconds, result)
                    self.cache.move_to_end(address)
                    while len(self.cache) > max_entries:
                        self.cache.popitem(last=False)
                pending.result = result
            return result
        except BaseException as exc:
            with self.cache_lock:
                pending.error = exc
            raise
        finally:
            with self.cache_lock:
                if self.inflight.get(address) is pending:
                    self.inflight.pop(address, None)
                pending.event.set()

    @staticmethod
    def _raise_shared_error(error: BaseException) -> None:
        if isinstance(error, StratumError):
            raise StratumError(
                error.code,
                error.message,
                reason=error.reason,
                disconnect=error.disconnect,
            ) from error
        raise RuntimeError(str(error)) from error


class JobDeliveryPort(Protocol):
    """Session-facing seam to job construction/delivery and submit handling."""

    def note_collection_identity_available(self, client: ClientState) -> None: ...
    def request_initial_job_delivery(self, client: ClientState) -> None: ...
    def reauthorization_has_capacity(self, client: ClientState) -> bool: ...
    def apply_client_difficulty_requests(self, client: ClientState) -> Decimal | None: ...
    def advertise_client_difficulty(self, client: ClientState, target: Decimal) -> bool: ...
    def handle_submit(self, client: ClientState, params: list[object]) -> bool: ...
    def refresh_jobs_after_pending_accepted_block(self, client: ClientState) -> None: ...
    def cancel_pending_initial_job_locked(
        self,
        client: ClientState,
    ) -> Callable[[], object] | None: ...
    def cleanup_disconnected_client(self, client: ClientState) -> None: ...
    def retain_current_collection_refresh_if_unrepresented(self) -> None: ...


class ProgressHealthPort(Protocol):
    """G1 integration; session code stores proof but owns no health policy."""

    def record_delivery(
        self,
        client: ClientState,
        context: object,
        delivered_monotonic: float,
    ) -> None: ...

    def reconcile_eligibility(self) -> None: ...


class SessionRuntimePort(Protocol):
    def running(self) -> bool: ...
    def record_heartbeat(self, name: str) -> None: ...
    def wait_after_resource_failure(self, heartbeat_name: str) -> None: ...
    def record_resource_exhaustion(
        self, *, listener_name: str, location: str, error_number: int | None
    ) -> None: ...
    def record_setup_failure(self) -> int: ...
    def sync_registry_metrics(self, registry: SessionRegistry) -> None: ...
    def max_connections(self) -> int: ...
    def max_connections_per_username(self) -> int: ...
    def client_startup_difficulty(self, profile: StratumListenerProfile) -> Decimal: ...
    def apply_send_timeout(self, sock: socket.socket) -> None: ...
    def make_client_thread(self, client: ClientState) -> threading.Thread: ...
    def extranonce2_size(self) -> int: ...
    def version_mask(self) -> int: ...
    def username_fallback_address(self) -> str | None: ...
    def resolve_worker(
        self, username: str, fallback: Callable[[], WorkerIdentity]
    ) -> WorkerIdentity: ...
    def reserve_client_username(
        self, client: ClientState, worker: WorkerIdentity, fallback: Callable[[], bool]
    ) -> bool: ...
    def send_result(
        self, client: ClientState, request_id: object, result: object
    ) -> None: ...
    def send_error(
        self,
        client: ClientState,
        request_id: object,
        code: int,
        message: str,
        *,
        reason: str | None,
    ) -> None: ...
    def disconnect_client(
        self, client: ClientState, fallback: Callable[[], None]
    ) -> None: ...


class StratumSessionService:
    def __init__(
        self,
        *,
        registry: SessionRegistry,
        runtime: SessionRuntimePort,
        jobs: JobDeliveryPort,
        progress: ProgressHealthPort,
        address_validator: P2mrAddressValidator,
        pool_closed_reason: str,
    ) -> None:
        self.registry = registry
        self.runtime = runtime
        self.jobs = jobs
        self.progress = progress
        self.address_validator = address_validator
        self.pool_closed_reason = pool_closed_reason

    @staticmethod
    def open_stratum_listeners(
        listener_stack: ExitStack,
        profiles: list[StratumListenerProfile],
        *,
        backlog: int,
        retry_seconds: float,
        stop_event: threading.Event | None,
        socket_factory: Callable[[int, int], socket.socket] = socket.socket,
    ) -> list[tuple[socket.socket, StratumListenerProfile]] | None:
        listeners: list[tuple[socket.socket, StratumListenerProfile]] = []
        for profile in profiles:
            server = listener_stack.enter_context(
                socket_factory(socket.AF_INET, socket.SOCK_STREAM)
            )
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            bind_deadline = time.monotonic() + retry_seconds
            warned = False
            while True:
                try:
                    server.bind((profile.bind, profile.port))
                    break
                except OSError as exc:
                    if exc.errno != errno.EADDRINUSE or time.monotonic() >= bind_deadline:
                        raise
                    if stop_event is not None and stop_event.is_set():
                        print(
                            "prism coordinator: shutdown requested while waiting "
                            f"to bind {profile.bind}:{profile.port}; aborting startup",
                            flush=True,
                        )
                        return None
                    if not warned:
                        print(
                            f"prism coordinator: {profile.name} listener port "
                            f"{profile.bind}:{profile.port} is busy; retrying bind "
                            f"for up to {retry_seconds:g}s",
                            flush=True,
                        )
                        warned = True
                    time.sleep(0.1)
            server.listen(backlog)
            server.settimeout(1)
            listeners.append((server, profile))
        return listeners

    def accept_loop(self, server: socket.socket, profile: StratumListenerProfile) -> None:
        while self.runtime.running():
            self.runtime.record_heartbeat(profile.heartbeat_name)
            try:
                sock, address = server.accept()
            except socket.timeout:
                continue
            except OSError as exc:
                if not self.runtime.running():
                    return
                if exc.errno in {errno.EMFILE, errno.ENFILE}:
                    self.runtime.record_resource_exhaustion(
                        listener_name=profile.name,
                        location="accept",
                        error_number=exc.errno,
                    )
                    self.runtime.wait_after_resource_failure(profile.heartbeat_name)
                    continue
                raise

            if not self.runtime.running():
                try:
                    sock.close()
                except OSError:
                    pass
                return

            # Recheck shutdown and membership admission in one registry
            # critical section. Socket close remains outside the lock.
            with self.registry.lock:
                admission_open = self.runtime.running()
                if admission_open:
                    client, rejection_count = self.registry.admit(
                        sock=sock,
                        address=address,
                        profile=profile,
                        share_difficulty=self.runtime.client_startup_difficulty(
                            profile
                        ),
                        max_connections=self.runtime.max_connections(),
                    )
                else:
                    client, rejection_count = None, 0
            if not admission_open:
                try:
                    sock.close()
                except OSError:
                    pass
                return
            self.runtime.sync_registry_metrics(self.registry)
            if client is None:
                try:
                    sock.close()
                except OSError:
                    pass
                if rejection_count == 1 or rejection_count % 100 == 0:
                    print(
                        "prism coordinator: rejected stratum connection at global limit "
                        f"limit={self.runtime.max_connections()} count={rejection_count}",
                        flush=True,
                    )
                continue
            try:
                sock.settimeout(None)
                self.runtime.apply_send_timeout(sock)
                thread = self.runtime.make_client_thread(client)
                self.registry.register_handler(client)
                self.runtime.sync_registry_metrics(self.registry)
                thread.start()
            except (OSError, RuntimeError) as exc:
                self.registry.unregister_handler(client)
                self.runtime.sync_registry_metrics(self.registry)
                try:
                    self.disconnect_client(client)
                except Exception:
                    print(
                        "prism coordinator: failed to fully close rejected stratum client "
                        f"address={address}",
                        flush=True,
                    )
                    traceback.print_exc()
                setup_failure_count = self.runtime.record_setup_failure()
                if isinstance(exc, OSError) and exc.errno in {errno.EMFILE, errno.ENFILE}:
                    self.runtime.record_resource_exhaustion(
                        listener_name=profile.name,
                        location="connection-setup",
                        error_number=exc.errno,
                    )
                if setup_failure_count == 1 or setup_failure_count % 100 == 0:
                    print(
                        "prism coordinator: stratum connection setup failed; backing off "
                        f"listener={profile.name} address={address} "
                        f"error={exc!r} count={setup_failure_count}",
                        flush=True,
                    )
                self.runtime.wait_after_resource_failure(profile.heartbeat_name)

    def reserve_client_username(
        self, client: ClientState, worker: WorkerIdentity
    ) -> bool:
        accepted, rejection_count = self.registry.reserve_username(
            client,
            worker,
            max_connections_per_username=self.runtime.max_connections_per_username(),
        )
        self.runtime.sync_registry_metrics(self.registry)
        if not accepted and (rejection_count == 1 or rejection_count % 100 == 0):
            print(
                "prism coordinator: rejected stratum authorization at username limit "
                f"username={worker.username!r} "
                f"limit={self.runtime.max_connections_per_username()} "
                f"count={rejection_count}",
                flush=True,
            )
        return accepted

    def handle_client(self, client: ClientState) -> None:
        reader = None
        try:
            reader = client.sock.makefile("r", encoding="utf-8", newline="\n")
            for line in reader:
                if not self.runtime.running():
                    break
                line = line.strip()
                if not line:
                    continue
                request_id: object = None
                try:
                    request = json.loads(line)
                    if not isinstance(request, dict):
                        raise StratumError(20, "request must be an object")
                    request_id = request.get("id")
                    self.handle_request(client, request)
                except json.JSONDecodeError as exc:
                    self._send_error(client, request_id, 20, f"invalid JSON: {exc.msg}")
                except StratumError as exc:
                    self._send_error(
                        client,
                        request_id,
                        exc.code,
                        exc.message,
                        reason=exc.reason,
                    )
                    if exc.disconnect:
                        break
                except Exception:
                    print(
                        f"prism coordinator: client thread failed address={client.address}",
                        flush=True,
                    )
                    traceback.print_exc()
                    break
        except (OSError, ValueError) as exc:
            if isinstance(exc, OSError) and exc.errno in {errno.EMFILE, errno.ENFILE}:
                self.runtime.record_resource_exhaustion(
                    listener_name=client.listener_name,
                    location="client-reader",
                    error_number=exc.errno,
                )
            print(
                "prism coordinator: stratum client socket failed "
                f"address={client.address} error={exc!r}",
                flush=True,
            )
        finally:
            try:
                if reader is not None:
                    reader.close()
            except (OSError, ValueError):
                pass
            finally:
                self.runtime.disconnect_client(
                    client, lambda: self.disconnect_client(client)
                )
                self.registry.unregister_handler(client)
                self.runtime.sync_registry_metrics(self.registry)

    def disconnect_client(self, client: ClientState) -> None:
        cancel_pending: Callable[[], object] | None = None
        with self.registry.lock:
            if not self.registry.begin_retirement_locked(client):
                return
            cancel_pending = self.jobs.cancel_pending_initial_job_locked(client)

        if cancel_pending is not None:
            cancel_pending()

        # Socket closure interrupts recv/send before waiting for job delivery.
        try:
            client.close()
        finally:
            with client.job_update_lock:
                self.jobs.cleanup_disconnected_client(client)
            self.jobs.retain_current_collection_refresh_if_unrepresented()
            self.progress.reconcile_eligibility()

    def handle_request(self, client: ClientState, request: dict[str, object]) -> None:
        try:
            self._handle_request(client, request)
        except ShutdownInProgress as exc:
            raise StratumError(
                20,
                "coordinator is shutting down",
                reason=self.pool_closed_reason,
                disconnect=True,
            ) from exc

    def _handle_request(self, client: ClientState, request: dict[str, object]) -> None:
        if not self.runtime.running():
            raise StratumError(
                20,
                "coordinator is shutting down",
                reason=self.pool_closed_reason,
                disconnect=True,
            )
        method = request.get("method")
        params = request.get("params", [])
        request_id = request.get("id")
        if not isinstance(method, str):
            raise StratumError(20, "missing method")
        if not isinstance(params, list):
            raise StratumError(20, "params must be an array")

        if method == "mining.configure":
            self.handle_configure(client, request_id, params)
            return
        if method == "mining.subscribe":
            with client.job_update_lock:
                client.subscribed = True
                self._send_result(
                    client,
                    request_id,
                    [[], client.extranonce1_hex, self.runtime.extranonce2_size()],
                )
                self.jobs.note_collection_identity_available(client)
                needs_initial_job = client.authorized
            if needs_initial_job:
                self.jobs.request_initial_job_delivery(client)
            return
        if method == "mining.authorize":
            username = str(params[0]) if params else ""
            password = str(params[1]) if len(params) > 1 and params[1] is not None else ""
            # validateaddress RPC stays outside registry and job-update locks.
            worker = self.runtime.resolve_worker(
                username, lambda: self.resolve_worker(username)
            )
            with client.job_update_lock:
                was_authorized = client.authorized
                if (
                    was_authorized
                    and not self.jobs.reauthorization_has_capacity(client)
                ):
                    raise StratumError(
                        20,
                        "initial job delivery capacity unavailable",
                        disconnect=False,
                    )
                if not self.runtime.reserve_client_username(
                    client,
                    worker,
                    lambda: self.reserve_client_username(client, worker),
                ):
                    raise StratumError(
                        20,
                        "too many connections for username",
                        disconnect=not client.authorized,
                    )
                client.requested_difficulty, client.requested_min_difficulty = (
                    parse_stratum_password_options(password)
                )
                target = self.jobs.apply_client_difficulty_requests(client)
                if target is not None:
                    current = client.pending_share_difficulty or client.share_difficulty
                    if target != current:
                        if not was_authorized:
                            client.share_difficulty = target
                            client.pending_share_difficulty = None
                        else:
                            client.pending_share_difficulty = target
                        client.difficulty_generation = int(client.difficulty_generation) + 1
                client.authorization_generation = int(client.authorization_generation) + 1
                client.authorized = True
                client.authorized_monotonic = time.monotonic()
                self._send_result(client, request_id, True)
                self.jobs.note_collection_identity_available(client)
            self.jobs.request_initial_job_delivery(client)
            return
        if method == "mining.extranonce.subscribe":
            self._send_result(client, request_id, True)
            return
        if method == "mining.suggest_difficulty":
            self.handle_suggest_difficulty(client, request_id, params)
            return
        if method == "mining.submit":
            accepted_and_closed = self.jobs.handle_submit(client, params)
            try:
                self._send_result(client, request_id, True)
            finally:
                self.jobs.refresh_jobs_after_pending_accepted_block(client)
            if accepted_and_closed:
                client.close()
            return
        raise StratumError(20, f"unsupported method {method}")

    def handle_suggest_difficulty(
        self, client: ClientState, request_id: object, params: list[object]
    ) -> None:
        with client.job_update_lock:
            suggested: Decimal | None = None
            if params:
                try:
                    suggested = Decimal(str(params[0]))
                except Exception:
                    suggested = None
                if suggested is not None and (
                    not suggested.is_finite() or suggested <= 0
                ):
                    suggested = None
            if suggested is not None:
                client.suggested_difficulty = suggested
                target = self.jobs.apply_client_difficulty_requests(client)
                if target is not None:
                    self.jobs.advertise_client_difficulty(client, target)
            self._send_result(client, request_id, True)

    def handle_configure(
        self, client: ClientState, request_id: object, params: list[object]
    ) -> None:
        extensions = params[0] if params else []
        extension_params = (
            params[1] if len(params) > 1 and isinstance(params[1], dict) else {}
        )
        result: dict[str, object] = {}
        if isinstance(extensions, list):
            for extension in extensions:
                if extension == "version-rolling":
                    miner_mask = 0xFFFFFFFF
                    if "version-rolling.mask" in extension_params:
                        miner_mask = stratum_codec.parse_mask_hex(
                            extension_params["version-rolling.mask"],
                            field_name="version-rolling.mask",
                        )
                    client.version_mask = self.runtime.version_mask() & miner_mask
                    result["version-rolling"] = client.version_mask != 0
                    result["version-rolling.mask"] = stratum_codec.format_mask_hex(
                        client.version_mask
                    )
                else:
                    result[str(extension)] = False
        self._send_result(client, request_id, result)

    @staticmethod
    def send_result(client: ClientState, request_id: object, result: object) -> None:
        client.send(result_payload(request_id, result))

    def _send_result(
        self, client: ClientState, request_id: object, result: object
    ) -> None:
        self.runtime.send_result(client, request_id, result)

    @staticmethod
    def send_error(
        client: ClientState,
        request_id: object,
        code: int,
        message: str,
        *,
        reason: str | None = None,
    ) -> None:
        client.send(error_payload(request_id, code, message, reason=reason))

    def _send_error(
        self,
        client: ClientState,
        request_id: object,
        code: int,
        message: str,
        *,
        reason: str | None = None,
    ) -> None:
        self.runtime.send_error(
            client,
            request_id,
            code,
            message,
            reason=reason,
        )

    def resolve_worker(self, username: str) -> WorkerIdentity:
        payout_address, worker_name = split_worker_username(username)
        try:
            if not payout_address:
                raise StratumError(20, "username base is empty")
            script, p2mr_program_hex = self.address_validator.validate(
                payout_address, label="username base"
            )
        except StratumError as username_error:
            fallback_address = self.runtime.username_fallback_address()
            if fallback_address is None:
                raise username_error
            print(
                f"prism coordinator: username {username!r} cannot be used as a payout "
                f"({username_error.message}); using fallback payout {fallback_address}",
                flush=True,
            )
            payout_address = fallback_address
            script, p2mr_program_hex = self.address_validator.validate(
                fallback_address,
                label="PRISM_USERNAME_FALLBACK_ADDRESS",
            )
        return WorkerIdentity(
            username=username,
            payout_address=payout_address,
            worker_name=worker_name,
            script_pubkey_hex=script,
            p2mr_program_hex=p2mr_program_hex,
        )

    def record_successful_delivery(
        self,
        client: ClientState,
        context: object,
        delivered_monotonic: float,
    ) -> None:
        if not self.registry.record_delivery(
            client, context, delivered_monotonic
        ):
            return
        self.progress.record_delivery(client, context, delivered_monotonic)
        self.progress.reconcile_eligibility()


def apply_stratum_send_timeout(sock: socket.socket, timeout_seconds: float) -> None:
    """Apply a send-only timeout without changing blocking receive behavior."""
    if timeout_seconds <= 0:
        return
    seconds = int(timeout_seconds)
    microseconds = int((timeout_seconds - seconds) * 1_000_000)
    try:
        sock.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_SNDTIMEO,
            struct.pack("ll", seconds, microseconds),
        )
    except (AttributeError, OSError, struct.error):
        return
