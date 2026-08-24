#!/usr/bin/env python3
"""PRISM Stratum listener and session lifecycle.

This module owns the S1 domain: miner-facing connection admission, protocol
dispatch, worker resolution, payout-address validation, and the live-session
registry (membership, connection generations, handler-thread accounting, and
exact per-connection delivery proof).

It never imports ``prism_coordinator``.  Job delivery, share submission,
progress health, shutdown state, and live configuration attributes are
reached through the :class:`SessionRuntime` typed port, resolved at call
time so the historical coordinator monkeypatch seams (including the
instance-level facade patches used by the current test suite) keep
intercepting exactly as before the extraction.  Share-ACK timing state
remains coordinator-owned at this layer (it moves with the PR 77 submission
owner); the session code stamps ``request_received_monotonic`` and reports
the response boundary through the runtime seam.
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
from typing import Any, Callable, Mapping, Protocol

from lab.auxpow import stratum_codec, vardiff
from lab.prism import direct_stratum
from lab.prism.coordinator_config import (
    DEFAULT_PRISM_PAYOUT_ADDRESS_CACHE_MAX_ENTRIES,
    DEFAULT_PRISM_PAYOUT_ADDRESS_CACHE_TTL_SECONDS,
    DEFAULT_PRISM_STRATUM_ACCEPT_RESOURCE_EXHAUSTION_BACKOFF_SECONDS,
    DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS,
    DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS_PER_USERNAME,
    StratumListenerProfile,
    default_prism_username_fallback_address,
)
from lab.prism.progress_health import DeliveryProof, WorkGeneration
from lab.prism.template_artifacts import qbit_template_fingerprint


_VARDIFF_LOCK_INITIALIZATION_LOCK = threading.Lock()


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
    # active_job is registered before the potentially blocking socket write.
    # These delivery-proof fields advance only after send_job_update succeeds.
    _progress_delivered_context: object | None = None
    _progress_delivered_template_fingerprint: str | None = None
    _progress_delivered_template_generation: int = 0
    _progress_delivered_payout_generation: int = -1
    _progress_delivered_monotonic: float | None = None
    # Admission ordering only. Delivery proof remains in the progress fields
    # above and advances exclusively after the socket write succeeds.
    _tip_refresh_admitted_epoch_sequence: int = 0
    listener_name: str = "default"
    # Pristine difficulty policy of the accepting listener; never mutated.
    listener_vardiff_config: vardiff.VardiffConfig | None = None
    # Floor below which stamped jobs never advertise, copied from the
    # accepting listener profile. Zero (default listener) keeps the network
    # cap authoritative.
    minimum_advertised_difficulty: Decimal = Decimal("0")
    # Per-client specialization of the listener policy (password d=/md= or
    # mining.suggest_difficulty); recomputed from the pristine base on every
    # request so repeat applications cannot compound.
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
    # Per-connection evidence that this session's difficulty is real: set on
    # the first accepted share of the connection and never reset within it.
    # Reconnect retention reads it to decide whether re-recording an
    # unchanged difficulty may refresh its TTL; a session that resumed a
    # retained value and submitted nothing must let that value keep ageing.
    vardiff_accepted_any: bool = False
    # Most recent accepted-share evidence. Durable reconnect retention uses
    # the exact stamped difficulty plus its wall-clock evidence time so a
    # later explicit difficulty request cannot accidentally be persisted as
    # safe merely because this connection accepted an older, easier share.
    vardiff_last_accepted_difficulty: Decimal | None = None
    vardiff_last_accepted_wall_ms: int | None = None
    # Fast-arrival initial convergence. True until this connection's first
    # share-driven retarget actually commits; while set, an accepted share
    # may trigger a retarget before the configured interval and that
    # retarget may take the larger initial step bound. Cleared exactly once,
    # at the paired-send commit point, so a build/send failure cannot spend
    # the relaxation without the miner ever seeing the new difficulty, and
    # so every later retarget is back on the ordinary configured bound.
    vardiff_initial_convergence_pending: bool = True
    # The statistical gate is evaluated once at the first sample that meets
    # both its share-count and elapsed-time floors. A failed delivery may
    # re-arm it, but an ordinary on-cadence window is never tested after every
    # subsequent share (which would compound the false-positive probability).
    vardiff_initial_convergence_evaluated: bool = False
    # Connection-scoped arrival evidence for the bounded high-difficulty
    # arrival histograms: when this session's vardiff accounting began, how
    # many accepted shares it has produced since, and whether its one
    # crossing of the high-diff threshold has already been observed.
    vardiff_session_started_monotonic: float = field(default_factory=time.monotonic)
    vardiff_session_accepted: int = 0
    vardiff_high_diff_arrival_recorded: bool = False
    # Owns all per-client difficulty policy, estimates, and Vardiff windows.
    # Share handlers release it before acquiring job_update_lock or the
    # coordinator lock; job delivery may acquire it while job_update_lock is
    # held. Keeping this lock per connection prevents one miner's accounting
    # or slow socket from convoying tip publication for the whole coordinator.
    vardiff_lock: threading.RLock = field(default_factory=threading.RLock)
    active_job_ids: set[str] = field(default_factory=set)
    post_accept_refresh_block: tuple[int, str] | None = None
    # (job previousblockhash, monotonic) of the FIRST job this connection was
    # sent for that tip. Anchors the per-connection stale-grace window: a
    # prior-tip share is in flight until shortly after this connection
    # received replacement work, however long the refresh pass took to reach
    # it. See stale_grace_deadline_open.
    tip_work_delivered: tuple[str, float] | None = None
    # Protected by the coordinator lock. Disconnect retirement sets this before
    # waiting for any per-client job update so queued work can reject the client.
    closing: bool = False
    # Stamped by the handler thread when a request line arrives, before JSON
    # parsing. Only that thread reads it; mining.submit uses it to observe
    # read-to-ack latency (the share-ingest saturation instrument).
    request_received_monotonic: float | None = None
    # Serializes every job build/register/send transition for this connection.
    # The coordinator lock may be acquired while this lock is held, never in
    # the reverse order. RLock permits authorize/retarget helpers to call the
    # common maybe_send_job path while retaining the same serialization scope.
    job_update_lock: threading.RLock = field(default_factory=threading.RLock)
    send_lock: threading.Lock = field(default_factory=threading.Lock)
    handler_thread_registered: bool = False

    def send(self, payload: dict[str, object]) -> None:
        data = json.dumps(payload).encode() + b"\n"
        with self.send_lock:
            self.sock.sendall(data)

    def send_batch(
        self,
        payloads: list[dict[str, object]],
        *,
        preserialized: bytes | None = None,
    ) -> None:
        # Tests and embedders may replace ``send`` with an in-memory recorder;
        # retain that seam while production sockets write the whole difficulty
        # + notify pair under one send lock with no response interleaving.
        # ``preserialized`` must be the exact serialization of ``payloads``;
        # callers with a shared precomposed fragment pass it so a fleet-wide
        # wave does not re-serialize identical coinbase parts per client.
        if "send" in self.__dict__:
            for payload in payloads:
                self.send(payload)
            return
        data = (
            preserialized
            if preserialized is not None
            else b"".join(
                json.dumps(payload).encode() + b"\n" for payload in payloads
            )
        )
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
    """Return the per-client vardiff lock, including lightweight embedders.

    Production ClientState instances always have this field. The lazy
    fallback preserves focused tests and embedders that construct a
    ClientState with __new__ and populate only the attributes they use.
    """
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
    """Extract the pool-side d=N / md=N difficulty convention from a password.

    Unknown tokens and malformed values are ignored: miners routinely send
    junk passwords ("x") and rejecting them would break every such rig.
    Returns (requested_difficulty, requested_min_difficulty).
    """

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


def apply_stratum_send_timeout(sock: socket.socket, timeout_seconds: float) -> None:
    """Bound blocking sends to miners without touching receive semantics.

    Job refreshes use a bounded executor, but an unresponsive peer whose
    TCP buffer is full must still release its worker eventually.
    SO_SNDTIMEO turns that into an OSError, which the refresh path treats
    as a dead client without failing delivery to other miners.
    A plain socket timeout is not usable here: it would also apply to
    recv, disconnecting idle-but-healthy miners.
    """
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
        # Platform without SO_SNDTIMEO support: keep legacy blocking sends.
        return


@dataclass(frozen=True)
class DeliveredSessionContext:
    context: object
    delivered_monotonic: float


@dataclass(frozen=True)
class EligibleSession:
    connection_id: int
    delivered: DeliveredSessionContext | None


class SessionRegistry:
    """Atomic owner of live membership and connection-scoped session facts.

    Callers hold the shared coordinator control-plane lock around the
    ``*_locked`` helpers exactly as the pre-extraction coordinator did; the
    registry itself adds no second lock.  Compatibility containers seeded by
    ``__new__`` embedders are adopted, never copied, so test-held references
    remain live.
    """

    def __init__(
        self,
        *,
        clients: object | None = None,
        connection_generation: int = 0,
        rejection_counts: dict[str, int] | None = None,
    ) -> None:
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
        self.peak_active_connections = len(self.clients)  # type: ignore[arg-type]
        self.handler_thread_count = sum(
            int(getattr(client, "handler_thread_registered", False))
            for client in self.clients
        )
        self._delivered_by_connection: dict[int, DeliveredSessionContext] = {}

    def adopt_clients(self, clients: object) -> None:
        """Adopt a compatibility replacement without changing its order/type."""
        self.clients = clients
        live_ids = {
            int(getattr(client, "connection_id", 0)) for client in clients  # type: ignore[union-attr]
        }
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
                (
                    int(getattr(client, "connection_id", 0))
                    for client in clients  # type: ignore[union-attr]
                ),
                default=0,
            ),
        )
        self.handler_thread_count = sum(
            int(getattr(client, "handler_thread_registered", False))
            for client in clients  # type: ignore[union-attr]
        )

    def note_rejection_locked(self, scope: str) -> int:
        count = int(self.rejection_counts.get(scope, 0)) + 1
        self.rejection_counts[scope] = count
        return count

    def begin_retirement_locked(self, client: ClientState) -> bool:
        """Claim retirement ownership; only the first claimer cleans up."""
        if getattr(client, "closing", False) and client not in self.clients:
            return False
        client.closing = True
        self.clients.discard(client)
        self._delivered_by_connection.pop(client.connection_id, None)
        return True

    def record_delivery_locked(
        self,
        client: ClientState,
        context: object,
        delivered_monotonic: float,
    ) -> None:
        """Commit exact delivery proof while the shared lock is held."""
        self._delivered_by_connection[client.connection_id] = (
            DeliveredSessionContext(context, delivered_monotonic)
        )

    def eligible_snapshot(self) -> Mapping[int, EligibleSession]:
        """Return an immutable exact client_can_receive_jobs population.

        Caller holds the shared lock; the compatibility mirrors on the
        client remain the staged-split fallback for embedder-seeded state.
        """
        captured: dict[int, EligibleSession] = {}
        for client in self.clients:
            if not client_can_receive_jobs(client):
                continue
            delivered = self._delivered_by_connection.get(client.connection_id)
            if (
                delivered is None
                and getattr(client, "_progress_delivered_context", None) is not None
            ):
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
                    self.cache[address] = (
                        time.monotonic() + ttl_seconds,
                        result,
                    )
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


class SessionRuntime(Protocol):
    """Typed port over the coordinator, resolved at call time.

    Every member is looked up on the live coordinator object when used, so
    instance monkeypatches (``server.send_result = ...``,
    ``server.validate_p2mr_address = ...`` and friends) and coordinator-owned
    live configuration attributes keep working exactly as before the
    extraction.  Owner facades route back into this service; that round trip
    is deliberate: it preserves the current white-box patch surface until
    later stack layers repoint those tests.  The legacy S1 field names
    (``clients``, ``connection_counter``, the connection counters, and the
    payout-address cache trio) also resolve here -- coordinator class
    descriptors route them to this service's single mutable copy.
    """

    # Cross-domain objects and live configuration attributes.
    _pool_ready_latched: Any
    clients: Any
    extranonce2_size: Any
    handler_thread_count: Any
    latest_detected_tip: Any
    listener_profiles: Any
    lock: Any
    pending_initial_jobs: Any
    rpc: Any
    stop_event: Any
    stratum_max_pending_initial_jobs: Any
    version_mask: Any

    def _cancel_pending_initial_job_locked(self, *args: Any, **kwargs: Any) -> Any: ...

    def _client_has_current_tip_job_locked(self, *args: Any, **kwargs: Any) -> Any: ...

    def _client_vardiff_lock(self, *args: Any, **kwargs: Any) -> Any: ...

    def _ensure_initial_job_state(self, *args: Any, **kwargs: Any) -> Any: ...

    def _ensure_job_cache_state(self, *args: Any, **kwargs: Any) -> Any: ...

    def _ensure_job_delivery_service(self, *args: Any, **kwargs: Any) -> Any: ...

    def _ensure_progress_health_service(self, *args: Any, **kwargs: Any) -> Any: ...

    def _ensure_shutdown_controller(self, *args: Any, **kwargs: Any) -> Any: ...

    def _handle_request(self, *args: Any, **kwargs: Any) -> Any: ...

    def _note_collection_identity_available(self, *args: Any, **kwargs: Any) -> Any: ...

    def _observe_share_ack_seconds(self, *args: Any, **kwargs: Any) -> Any: ...

    def _progress_note_refresh_activity(self, *args: Any, **kwargs: Any) -> Any: ...

    def _progress_reconcile_pending(self, *args: Any, **kwargs: Any) -> Any: ...

    def _record_heartbeat(self, *args: Any, **kwargs: Any) -> Any: ...

    def _record_stratum_resource_exhaustion(self, *args: Any, **kwargs: Any) -> Any: ...

    def _record_tip_refresh_epoch_coverage(self, *args: Any, **kwargs: Any) -> Any: ...

    def _retain_current_collection_refresh_if_unrepresented(
        self, *args: Any, **kwargs: Any
    ) -> Any: ...

    def _tip_refresh_epoch_coverage_reached_locked(
        self, *args: Any, **kwargs: Any
    ) -> Any: ...

    def _wait_after_stratum_resource_failure(self, *args: Any, **kwargs: Any) -> Any: ...

    def apply_client_difficulty_requests(self, *args: Any, **kwargs: Any) -> Any: ...

    def apply_stratum_send_timeout(self, *args: Any, **kwargs: Any) -> Any: ...

    def advertise_client_difficulty(self, *args: Any, **kwargs: Any) -> Any: ...

    def client_startup_difficulty(self, *args: Any, **kwargs: Any) -> Any: ...

    def disconnect_client(self, *args: Any, **kwargs: Any) -> Any: ...

    def handle_client(self, *args: Any, **kwargs: Any) -> Any: ...

    def handle_configure(self, *args: Any, **kwargs: Any) -> Any: ...

    def handle_submit(self, *args: Any, **kwargs: Any) -> Any: ...

    def handle_suggest_difficulty(self, *args: Any, **kwargs: Any) -> Any: ...

    def note_vardiff_resume_overridden(self, *args: Any, **kwargs: Any) -> Any: ...

    def record_session_difficulty(self, *args: Any, **kwargs: Any) -> Any: ...

    def refresh_jobs_after_pending_accepted_block(
        self, *args: Any, **kwargs: Any
    ) -> Any: ...

    def request_initial_job_delivery(self, *args: Any, **kwargs: Any) -> Any: ...

    def reserve_client_username(self, *args: Any, **kwargs: Any) -> Any: ...

    def resume_client_difficulty(self, *args: Any, **kwargs: Any) -> Any: ...

    def send_error(self, *args: Any, **kwargs: Any) -> Any: ...

    def send_result(self, *args: Any, **kwargs: Any) -> Any: ...

    def validate_p2mr_address(self, *args: Any, **kwargs: Any) -> Any: ...


class StratumSessionService:
    """Sole owner of S1 session admission, dispatch, and registry proof."""

    def __init__(
        self,
        runtime: SessionRuntime,
        *,
        shutdown_error: type[BaseException],
        pool_closed_reason: str,
    ) -> None:
        self._runtime = runtime
        # The shutdown exception type remains coordinator-owned until the
        # shutdown controller is extracted; inject it so this leaf module
        # never imports prism_coordinator.
        self._shutdown_error = shutdown_error
        self.pool_closed_reason = pool_closed_reason
        self.registry = SessionRegistry()
        self.accept_resource_exhaustion_count = 0
        self.connection_setup_failure_count = 0
        self.address_validator = P2mrAddressValidator(
            rpc_call=lambda method, params: runtime.rpc.call(method, params),
            max_entries=lambda: int(
                getattr(
                    runtime,
                    "payout_address_cache_max_entries",
                    DEFAULT_PRISM_PAYOUT_ADDRESS_CACHE_MAX_ENTRIES,
                )
            ),
            ttl_seconds=lambda: float(
                getattr(
                    runtime,
                    "payout_address_cache_ttl_seconds",
                    DEFAULT_PRISM_PAYOUT_ADDRESS_CACHE_TTL_SECONDS,
                )
            ),
        )

    # -- legacy coordinator field names routed by class descriptors --------

    @property
    def clients(self) -> object:
        return self.registry.clients

    @clients.setter
    def clients(self, value: object) -> None:
        self.registry.adopt_clients(value)

    @property
    def connection_counter(self) -> int:
        return self.registry.connection_generation

    @connection_counter.setter
    def connection_counter(self, value: int) -> None:
        self.registry.connection_generation = int(value)

    @property
    def connection_limit_rejection_counts(self) -> dict[str, int]:
        return self.registry.rejection_counts

    @connection_limit_rejection_counts.setter
    def connection_limit_rejection_counts(self, value: dict[str, int]) -> None:
        self.registry.rejection_counts = value

    @property
    def peak_active_connection_count(self) -> int:
        return self.registry.peak_active_connections

    @peak_active_connection_count.setter
    def peak_active_connection_count(self, value: int) -> None:
        self.registry.peak_active_connections = int(value)

    @property
    def handler_thread_count(self) -> int:
        return self.registry.handler_thread_count

    @handler_thread_count.setter
    def handler_thread_count(self, value: int) -> None:
        self.registry.handler_thread_count = int(value)

    @property
    def _p2mr_address_cache_lock(self) -> threading.Lock:
        return self.address_validator.cache_lock

    @_p2mr_address_cache_lock.setter
    def _p2mr_address_cache_lock(self, value: threading.Lock) -> None:
        self.address_validator.cache_lock = value

    @property
    def _p2mr_address_cache(self) -> OrderedDict[str, tuple[float, tuple[str, str]]]:
        return self.address_validator.cache

    @_p2mr_address_cache.setter
    def _p2mr_address_cache(
        self, value: OrderedDict[str, tuple[float, tuple[str, str]]]
    ) -> None:
        self.address_validator.cache = value

    @property
    def _p2mr_address_validation_inflight(
        self,
    ) -> dict[str, _P2mrAddressValidationFlight]:
        return self.address_validator.inflight

    @_p2mr_address_validation_inflight.setter
    def _p2mr_address_validation_inflight(
        self, value: dict[str, _P2mrAddressValidationFlight]
    ) -> None:
        self.address_validator.inflight = value

    # -- listener factory --------------------------------------------------

    @staticmethod
    def open_stratum_listeners(
        listener_stack: ExitStack,
        profiles: list[StratumListenerProfile] | tuple[StratumListenerProfile, ...],
        *,
        backlog: int,
        retry_seconds: float,
        stop_event: threading.Event | None,
    ) -> list[tuple[socket.socket, StratumListenerProfile]] | None:
        """Bind and listen on every stratum listener profile.

        Called before the slow parts of startup (qbit readiness, policy
        validation, block-work recovery) so miners reconnecting through a
        restart park in the kernel accept backlog instead of getting
        connection refused, which sends firmware into reconnect backoff or
        failover and costs hashrate. bind() retries EADDRINUSE for a bounded
        window because a predecessor process may still hold the port while
        draining its shutdown. Returns None when shutdown is requested during
        the retry, so startup can abort gracefully.
        """
        listeners: list[tuple[socket.socket, StratumListenerProfile]] = []
        for profile in profiles:
            server = listener_stack.enter_context(
                socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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
                        # Shutting down mid-startup: stop contending for a
                        # port this process will never serve.
                        print(
                            f"prism coordinator: shutdown requested while waiting "
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

    # -- accept/admission --------------------------------------------------

    def accept_loop(
        self, server: socket.socket, profile: StratumListenerProfile
    ) -> None:
        runtime = self._runtime
        registry = self.registry
        while not runtime.stop_event.is_set():
            runtime._record_heartbeat(profile.heartbeat_name)
            try:
                sock, address = server.accept()
            except socket.timeout:
                continue
            except OSError as exc:
                # The listener socket is torn down by serve()'s ExitStack on
                # shutdown while secondary accept threads may still be blocked
                # in accept(). Descriptor exhaustion is recoverable: keep the
                # accept loop alive and refresh its watchdog heartbeat while
                # waiting for client/RPC descriptors to drain.
                if runtime.stop_event.is_set():
                    return
                if exc.errno in {errno.EMFILE, errno.ENFILE}:
                    runtime._record_stratum_resource_exhaustion(
                        listener_name=profile.name,
                        location="accept",
                        error_number=exc.errno,
                    )
                    runtime._wait_after_stratum_resource_failure(profile.heartbeat_name)
                    continue
                raise

            if (
                runtime.stop_event.is_set()
                or runtime._ensure_shutdown_controller().phase != "running"
            ):
                try:
                    sock.close()
                except OSError:
                    pass
                return

            with runtime.lock:
                if (
                    runtime.stop_event.is_set()
                    or runtime._ensure_shutdown_controller().phase != "running"
                ):
                    try:
                        sock.close()
                    except OSError:
                        pass
                    return
                max_connections = int(
                    getattr(
                        runtime,
                        "stratum_max_connections",
                        DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS,
                    )
                )
                if max_connections > 0 and len(registry.clients) >= max_connections:
                    rejection_count = registry.note_rejection_locked("global")
                    client = None
                else:
                    registry.connection_generation += 1
                    connection_id = registry.connection_generation
                    client = ClientState(
                        sock=sock,
                        address=address,
                        connection_id=connection_id,
                        extranonce1_hex=f"{connection_id & 0xFFFFFFFF:08x}",
                        listener_name=profile.name,
                        listener_vardiff_config=profile.vardiff_config,
                        minimum_advertised_difficulty=profile.minimum_advertised_difficulty,
                        share_difficulty=runtime.client_startup_difficulty(profile),
                    )
                    registry.clients.add(client)
                    runtime._ensure_initial_job_state()
                    registry.peak_active_connections = max(
                        registry.peak_active_connections,
                        len(registry.clients),
                    )
            if client is None:
                try:
                    sock.close()
                except OSError:
                    pass
                if rejection_count == 1 or rejection_count % 100 == 0:
                    print(
                        "prism coordinator: rejected stratum connection at global limit "
                        f"limit={max_connections} count={rejection_count}",
                        flush=True,
                    )
                continue
            try:
                sock.settimeout(None)
                runtime.apply_stratum_send_timeout(sock)
                thread = threading.Thread(
                    target=runtime.handle_client, args=(client,), daemon=True
                )
                with runtime.lock:
                    client.handler_thread_registered = True
                    registry.handler_thread_count += 1
                thread.start()
            except (OSError, RuntimeError) as exc:
                # Admission is atomic with the global count. Undo it if socket
                # setup or thread creation fails before a handler owns cleanup,
                # then keep this listener alive for the next connection.
                try:
                    with runtime.lock:
                        if client.handler_thread_registered:
                            client.handler_thread_registered = False
                            registry.handler_thread_count = max(
                                0,
                                registry.handler_thread_count - 1,
                            )
                    runtime.disconnect_client(client)
                except Exception:
                    print(
                        "prism coordinator: failed to fully close rejected stratum client "
                        f"address={address}",
                        flush=True,
                    )
                    traceback.print_exc()
                with runtime.lock:
                    self.connection_setup_failure_count = int(
                        getattr(self, "connection_setup_failure_count", 0)
                    ) + 1
                    setup_failure_count = self.connection_setup_failure_count
                if isinstance(exc, OSError) and exc.errno in {errno.EMFILE, errno.ENFILE}:
                    runtime._record_stratum_resource_exhaustion(
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
                runtime._wait_after_stratum_resource_failure(profile.heartbeat_name)
                continue

    def record_stratum_resource_exhaustion(
        self,
        *,
        listener_name: str,
        location: str,
        error_number: int | None,
    ) -> int:
        runtime = self._runtime
        with runtime.lock:
            self.accept_resource_exhaustion_count = int(
                getattr(self, "accept_resource_exhaustion_count", 0)
            ) + 1
            exhaustion_count = self.accept_resource_exhaustion_count
        if exhaustion_count == 1 or exhaustion_count % 100 == 0:
            print(
                "prism coordinator: stratum resource exhaustion "
                f"listener={listener_name} location={location} errno={error_number} "
                f"count={exhaustion_count}",
                flush=True,
            )
        return exhaustion_count

    def wait_after_stratum_resource_failure(self, heartbeat_name: str) -> None:
        runtime = self._runtime
        backoff_seconds = getattr(
            runtime,
            "stratum_accept_resource_exhaustion_backoff_seconds",
            DEFAULT_PRISM_STRATUM_ACCEPT_RESOURCE_EXHAUSTION_BACKOFF_SECONDS,
        )
        remaining_seconds = max(0.0, float(backoff_seconds))
        watchdog_timeout_seconds = max(
            0.001,
            float(getattr(runtime, "watchdog_timeout_seconds", 120.0)),
        )
        heartbeat_interval_seconds = max(
            0.001,
            min(1.0, watchdog_timeout_seconds / 2.0),
        )
        deadline = time.monotonic() + remaining_seconds
        while not runtime.stop_event.is_set():
            runtime._record_heartbeat(heartbeat_name)
            remaining_seconds = deadline - time.monotonic()
            if remaining_seconds <= 0:
                return
            if runtime.stop_event.wait(
                min(remaining_seconds, heartbeat_interval_seconds)
            ):
                return

    def reserve_client_username(
        self, client: ClientState, worker: WorkerIdentity
    ) -> bool:
        """Atomically reserve an exact Stratum username for one connection."""
        runtime = self._runtime
        registry = self.registry
        with runtime.lock:
            limit = int(
                getattr(
                    runtime,
                    "stratum_max_connections_per_username",
                    DEFAULT_PRISM_STRATUM_MAX_CONNECTIONS_PER_USERNAME,
                )
            )
            active_for_username = sum(
                1
                for other in registry.clients
                if (
                    other is not client
                    and other.worker is not None
                    and other.username == worker.username
                )
            )
            if limit > 0 and active_for_username >= limit:
                rejection_count = registry.note_rejection_locked("username")
                if rejection_count == 1 or rejection_count % 100 == 0:
                    print(
                        "prism coordinator: rejected stratum authorization at username limit "
                        f"username={worker.username!r} limit={limit} count={rejection_count}",
                        flush=True,
                    )
                return False
            # Commit the replacement only after capacity validation. A failed
            # reauthorization therefore leaves the prior live identity intact.
            client.worker = worker
            client.username = worker.username
            return True

    # -- session lifecycle -------------------------------------------------

    def handle_client(self, client: ClientState) -> None:
        runtime = self._runtime
        registry = self.registry
        reader = None
        try:
            reader = client.sock.makefile("r", encoding="utf-8", newline="\n")
            for line in reader:
                if runtime.stop_event.is_set():
                    break
                line = line.strip()
                if not line:
                    continue
                client.request_received_monotonic = time.monotonic()
                request_id: object = None
                request: object = None
                try:
                    request = json.loads(line)
                    if not isinstance(request, dict):
                        raise StratumError(20, "request must be an object")
                    request_id = request.get("id")
                    self.handle_request(client, request)
                except json.JSONDecodeError as exc:
                    runtime.send_error(client, request_id, 20, f"invalid JSON: {exc.msg}")
                except StratumError as exc:
                    runtime.send_error(
                        client, request_id, exc.code, exc.message, reason=exc.reason
                    )
                    received_monotonic = client.request_received_monotonic
                    if (
                        received_monotonic is not None
                        and isinstance(request, dict)
                        and request.get("method") == "mining.submit"
                    ):
                        # Symmetric with the accepted-path observation after
                        # send_result: both sides include the response write,
                        # so the accepted-vs-rejected comparison separates
                        # commit pressure from transport/thread saturation.
                        runtime._observe_share_ack_seconds(
                            "rejected",
                            time.monotonic() - received_monotonic,
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
                runtime._record_stratum_resource_exhaustion(
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
                runtime.disconnect_client(client)
                with runtime.lock:
                    runtime._ensure_initial_job_state()
                    if getattr(client, "handler_thread_registered", False):
                        client.handler_thread_registered = False
                        registry.handler_thread_count = max(
                            0,
                            registry.handler_thread_count - 1,
                        )

    def disconnect_client(self, client: ClientState) -> None:
        # Retire admission and fanout eligibility without waiting behind job
        # delivery. Only the first caller owns socket close and final cleanup.
        runtime = self._runtime
        registry = self.registry
        with runtime.lock:
            # The timeout sweeper marks closing while leaving membership in
            # place as its atomic handoff token. Whichever disconnect caller
            # first removes that membership owns socket close and cleanup.
            if not registry.begin_retirement_locked(client):
                return
            runtime._cancel_pending_initial_job_locked(client, count=True)

        # Retirement was just claimed, so this runs once per connection and
        # outside the coordinator lock: retain the session's converged
        # difficulty for a reconnect resume. Reached through the runtime seam
        # so monkeypatched coordinators keep intercepting; a retention
        # failure must never break disconnect cleanup.
        record_session_difficulty = getattr(
            runtime, "record_session_difficulty", None
        )
        if record_session_difficulty is not None:
            try:
                record_session_difficulty(client)
            except Exception:
                print(
                    "prism coordinator: session difficulty retention failed "
                    f"address={client.address}",
                    flush=True,
                )
                traceback.print_exc()

        # Do not take send_lock here: shutdown must interrupt an in-flight
        # sendall as well as the handler's blocking reader.
        try:
            client.close()
        finally:
            # Every mixed lock path uses job_update_lock -> coordinator lock.
            # Retirement above holds neither while this potentially waits.
            with client.job_update_lock:
                runtime._ensure_job_delivery_service().cleanup_disconnected_client(
                    client
                )
            runtime._retain_current_collection_refresh_if_unrepresented()

    # -- protocol dispatch -------------------------------------------------

    def handle_request(self, client: ClientState, request: dict[str, object]) -> None:
        """Dispatch one request, translating shutdown races to Stratum errors."""
        try:
            self._runtime._handle_request(client, request)
        except self._shutdown_error as exc:
            # A request can pass the initial shutdown check immediately before
            # writer admission closes. Preserve the normal protocol response
            # instead of surfacing a generic client-thread failure.
            raise StratumError(
                20,
                "coordinator is shutting down",
                reason=self.pool_closed_reason,
                disconnect=True,
            ) from exc

    def _handle_request(self, client: ClientState, request: dict[str, object]) -> None:
        runtime = self._runtime
        if (
            runtime.stop_event.is_set()
            or runtime._ensure_shutdown_controller().phase != "running"
        ):
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
            runtime.handle_configure(client, request_id, params)
            return
        if method == "mining.subscribe":
            with client.job_update_lock:
                client.subscribed = True
                runtime.send_result(
                    client,
                    request_id,
                    [[], client.extranonce1_hex, runtime.extranonce2_size],
                )
                runtime._note_collection_identity_available(client)
                needs_initial_job = client.authorized
            if needs_initial_job:
                runtime.request_initial_job_delivery(client)
            return
        if method == "mining.authorize":
            username = str(params[0]) if params else ""
            password = str(params[1]) if len(params) > 1 and params[1] is not None else ""
            # Address validation may use RPC; it is unrelated to client job
            # state and therefore stays outside the job-update lock.
            worker = runtime.resolve_worker(username)
            with client.job_update_lock:
                was_authorized = client.authorized
                if was_authorized:
                    with runtime.lock:
                        runtime._ensure_initial_job_state()
                        if (
                            client not in runtime.pending_initial_jobs
                            and len(runtime.pending_initial_jobs)
                            >= runtime.stratum_max_pending_initial_jobs
                            and runtime._client_has_current_tip_job_locked(client)
                        ):
                            # Reject before mutating identity or difficulty. A
                            # working session keeps its current authorization
                            # when there is no live first-job slot for the
                            # superseding generation.
                            raise StratumError(
                                20,
                                "initial job delivery capacity unavailable",
                                disconnect=False,
                            )
                if not runtime.reserve_client_username(client, worker):
                    raise StratumError(
                        20,
                        "too many connections for username",
                        # A new connection has no useful session to preserve. A
                        # live miner re-authorizing to a full username does: keep
                        # its prior worker/session active after returning the
                        # capacity error.
                        disconnect=not client.authorized,
                    )
                # The password is authoritative for password-derived options: a
                # re-authorize without d=/md= clears any prior override (a stored
                # suggest_difficulty still applies via the request resolution).
                with runtime._client_vardiff_lock(client):
                    # Only the FIRST authorize of a connection may resume the
                    # worker's retained difficulty (a live session already
                    # holds a converged value), and it must land before the
                    # password options: an explicit d= computes its target
                    # from requested_difficulty and outranks the resume,
                    # while an md=-only password clamps the just-resumed
                    # value through the share_difficulty fallback.
                    resumed: Decimal | None = None
                    if not was_authorized:
                        resumed = runtime.resume_client_difficulty(client)
                    client.requested_difficulty, client.requested_min_difficulty = (
                        parse_stratum_password_options(password)
                    )
                    target = runtime.apply_client_difficulty_requests(client)
                    if target is not None:
                        current = client.pending_share_difficulty or client.share_difficulty
                        if resumed is not None and target != resumed:
                            # The resume applied a value and this authorize
                            # immediately replaced it, so it never stuck.
                            # resumed/clamped stay attempt counters; this
                            # subtracts the ones an explicit difficulty
                            # request (d=/md=, or a suggestion sent before
                            # authorize) overrode.
                            runtime.note_vardiff_resume_overridden()
                        if target != current:
                            if not was_authorized:
                                client.share_difficulty = target
                                client.pending_share_difficulty = None
                            else:
                                client.pending_share_difficulty = target
                            client.difficulty_generation = int(
                                getattr(client, "difficulty_generation", 0)
                            ) + 1
                client.authorization_generation = int(
                    getattr(client, "authorization_generation", 0)
                ) + 1
                client.authorized = True
                client.authorized_monotonic = time.monotonic()
                runtime.send_result(client, request_id, True)
                runtime._note_collection_identity_available(client)
            # Exactly one coalesced delivery represents this authorization,
            # including a password-derived difficulty change.
            runtime.request_initial_job_delivery(client)
            return
        if method == "mining.extranonce.subscribe":
            runtime.send_result(client, request_id, True)
            return
        if method == "mining.suggest_difficulty":
            runtime.handle_suggest_difficulty(client, request_id, params)
            return
        if method == "mining.submit":
            received_monotonic = getattr(
                client,
                "request_received_monotonic",
                None,
            )
            accepted_and_closed = runtime.handle_submit(client, params)
            try:
                runtime.send_result(client, request_id, True)
                if received_monotonic is not None:
                    # Rejects are observed symmetrically in handle_client
                    # after their error response write.
                    runtime._observe_share_ack_seconds(
                        "accepted",
                        time.monotonic() - received_monotonic,
                    )
            finally:
                runtime.refresh_jobs_after_pending_accepted_block(client)
            if accepted_and_closed:
                client.close()
            return
        raise StratumError(20, f"unsupported method {method}")

    def handle_suggest_difficulty(
        self, client: ClientState, request_id: object, params: list[object]
    ) -> None:
        runtime = self._runtime
        with client.job_update_lock:
            suggested: Decimal | None = None
            if params:
                try:
                    suggested = Decimal(str(params[0]))
                except Exception:
                    suggested = None
                if suggested is not None and (not suggested.is_finite() or suggested <= 0):
                    suggested = None
            if suggested is not None:
                with runtime._client_vardiff_lock(client):
                    client.suggested_difficulty = suggested
                    target = runtime.apply_client_difficulty_requests(client)
                if target is not None:
                    runtime.advertise_client_difficulty(client, target)
            runtime.send_result(client, request_id, True)

    def handle_configure(
        self, client: ClientState, request_id: object, params: list[object]
    ) -> None:
        runtime = self._runtime
        extensions = params[0] if params else []
        extension_params = params[1] if len(params) > 1 and isinstance(params[1], dict) else {}
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
                    client.version_mask = runtime.version_mask & miner_mask
                    result["version-rolling"] = client.version_mask != 0
                    result["version-rolling.mask"] = stratum_codec.format_mask_hex(client.version_mask)
                else:
                    result[str(extension)] = False
        runtime.send_result(client, request_id, result)

    # -- worker resolution -------------------------------------------------

    def resolve_worker(self, username: str) -> WorkerIdentity:
        runtime = self._runtime
        payout_address, worker_name = split_worker_username(username)
        try:
            if not payout_address:
                raise StratumError(20, "username base is empty")
            script, p2mr_program_hex = runtime.validate_p2mr_address(
                payout_address, label="username base"
            )
        except StratumError as username_error:
            fallback_address = getattr(
                runtime,
                "username_fallback_address",
                default_prism_username_fallback_address(),
            )
            if fallback_address is None:
                raise username_error
            print(
                f"prism coordinator: username {username!r} cannot be used as a payout "
                f"({username_error.message}); using fallback payout {fallback_address}",
                flush=True,
            )
            payout_address = fallback_address
            script, p2mr_program_hex = runtime.validate_p2mr_address(
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

    # -- registry proof ----------------------------------------------------

    def record_successful_delivery(
        self,
        client: ClientState,
        context: object,
        delivered_monotonic: float,
    ) -> None:
        """Commit exact registry delivery proof, then report to G1.

        Proof-before-health ordering is a lost-fix detector: within the one
        shared critical section the S1 registry proof (and its staged-split
        compatibility mirrors on the client object) commits first, and only
        then does the progress-health owner observe the delivery.
        """
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        service = runtime._ensure_progress_health_service()
        fingerprint = getattr(context, "template_fingerprint", None)
        if fingerprint is None:
            fingerprint = qbit_template_fingerprint(context.template)
        payout_generation = int(getattr(context, "payout_state_generation", 0))
        delivered_tip = str(context.template.get("previousblockhash", ""))
        recorded = False
        coverage_reached: list[tuple[str, float]] = []
        with runtime.lock:
            self.registry.record_delivery_locked(
                client, context, delivered_monotonic
            )
            client._progress_delivered_context = context
            client._progress_delivered_template_fingerprint = fingerprint
            client._progress_delivered_template_generation = int(
                getattr(context, "template_generation", 0)
            )
            client._progress_delivered_payout_generation = payout_generation
            client._progress_delivered_monotonic = delivered_monotonic
            ready_mode_required = bool(
                getattr(runtime, "_pool_ready_latched", False)
            )
            latest_detected = getattr(runtime, "latest_detected_tip", None)
            recorded = service.record_delivery(
                DeliveryProof(
                    connection_id=int(getattr(client, "connection_id", 0)),
                    delivered_work=WorkGeneration(
                        template_generation=int(
                            getattr(context, "template_generation", 0)
                        ),
                        template_fingerprint=fingerprint,
                        payout_generation=payout_generation,
                    ),
                    collection_only=bool(
                        getattr(context, "collection_only", False)
                    ),
                    delivered_monotonic=delivered_monotonic,
                ),
                ready_mode_required,
                matches_latest_tip=bool(
                    latest_detected is None
                    or latest_detected[0] == delivered_tip
                ),
            )
            coverage_reached = (
                runtime._tip_refresh_epoch_coverage_reached_locked(
                    client,
                    context,
                    delivered_monotonic,
                )
            )
        if recorded:
            runtime._progress_note_refresh_activity(delivered_monotonic)
        runtime._progress_reconcile_pending(now=delivered_monotonic)
        runtime._record_tip_refresh_epoch_coverage(coverage_reached)


__all__ = [
    "ClientState",
    "DeliveredSessionContext",
    "EligibleSession",
    "P2mrAddressValidator",
    "SessionRegistry",
    "SessionRuntime",
    "StratumError",
    "StratumSessionService",
    "WorkerIdentity",
    "_P2mrAddressValidationFlight",
    "apply_stratum_send_timeout",
    "client_can_receive_jobs",
    "client_vardiff_lock",
    "difficulty_payload",
    "error_payload",
    "job_payload",
    "parse_stratum_password_options",
    "parse_worker_username",
    "result_payload",
    "split_worker_username",
    "stratum_accept_heartbeat_names",
]
