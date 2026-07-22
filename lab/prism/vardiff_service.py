"""PRISM vardiff windows, retarget delivery, and bounded idle work."""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from decimal import Decimal
import threading
import time
import traceback
from typing import Any, Callable, Protocol

from lab.auxpow import vardiff
from lab.prism.coordinator_config import StratumListenerProfile
from lab.prism.job_bundle import CachedJobBundle, JobBuildSuperseded
from lab.prism.job_delivery import IdleDeliveryAuthority, PrismJobContext
from lab.prism.stratum_session import (
    ClientState,
    WorkerIdentity,
    client_vardiff_lock,
)


PRISM_VARDIFF_IDLE_RETARGET_MAX_WORKERS = 2
MAX_PENDING_VARDIFF_IDLE_RETARGETS = 8
PRISM_VARDIFF_IDLE_SECONDS_BUCKETS = (
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
)
PRISM_VARDIFF_IDLE_SKIP_REASONS = (
    "busy",
    "disconnected",
    "not_idle",
    "cache_miss",
    "queue_full",
    "superseded",
)


@dataclass(frozen=True)
class IdleRetargetRequest:
    """Immutable idle-window identity captured by one bounded sweep."""

    client: ClientState
    connection_id: int
    worker: WorkerIdentity
    active_job: PrismJobContext
    window_started_monotonic: float
    current_difficulty: Decimal
    elapsed_seconds: Decimal


class VardiffRuntime(Protocol):
    """Coordinator operations needed at vardiff's ownership boundary."""

    lock: Any
    clients: set[ClientState]
    stop_event: threading.Event
    vardiff_config: vardiff.VardiffConfig
    share_difficulty: Decimal
    vardiff_idle_sweep_seconds: float

    def client_can_receive_jobs(self, client: ClientState) -> bool: ...

    def _record_heartbeat(self, name: str) -> None: ...

    def _record_vardiff_idle_skip(self, reason: str) -> None: ...

    def _cached_idle_job_bundle(self, client: ClientState) -> CachedJobBundle | None: ...

    def _build_idle_job_bundle(
        self,
        request: IdleRetargetRequest,
    ) -> CachedJobBundle: ...

    def _idle_bundle_current_locked(
        self,
        client: ClientState,
        bundle: CachedJobBundle,
        *,
        allow_uncached: bool = False,
    ) -> bool: ...

    def pool_readiness_latched(self) -> bool: ...

    def ensure_reorg_reconciled_for_current_tip(self) -> bool: ...

    def disconnect_client(self, client: ClientState) -> None: ...

    def _maybe_send_job_locked(self, client: ClientState, **kwargs: object) -> bool: ...

    def maybe_send_job(self, client: ClientState, *, clean_jobs: bool) -> bool: ...

    def retarget_client(self, client: ClientState, **kwargs: object) -> bool: ...


class VardiffCompatibilityField:
    """Temporary coordinator view over state owned by ``VardiffService``."""

    def __init__(self, name: str, default: Callable[[], Any]) -> None:
        self.name = name
        self.default = default

    def __get__(self, instance: Any, owner: type[Any]) -> Any:
        if instance is None:
            return self
        init_lock = instance.__dict__.setdefault(
            "_vardiff_service_init_lock",
            threading.RLock(),
        )
        with init_lock:
            service = instance.__dict__.get("_vardiff_service")
            if service is not None:
                return getattr(service, self.name)
            if self.name not in instance.__dict__:
                instance.__dict__[self.name] = self.default()
            return instance.__dict__[self.name]

    def __set__(self, instance: Any, value: Any) -> None:
        init_lock = instance.__dict__.setdefault(
            "_vardiff_service_init_lock",
            threading.RLock(),
        )
        with init_lock:
            service = instance.__dict__.get("_vardiff_service")
            if service is not None:
                setattr(service, self.name, value)
            else:
                instance.__dict__[self.name] = value


VARDIFF_COMPATIBILITY_FIELDS = (
    "idle_retarget_count",
    "_vardiff_idle_lock",
    "_vardiff_idle_executor",
    "_vardiff_idle_executor_shutdown",
    "_vardiff_idle_pending",
    "vardiff_idle_queue_depth",
    "vardiff_idle_inflight",
    "vardiff_idle_clients_inspected",
    "vardiff_idle_skip_counts",
    "vardiff_idle_task_failures",
    "vardiff_idle_sweep_histogram",
    "vardiff_idle_task_histogram",
)


def _new_histogram() -> dict[str, Any]:
    return {
        "buckets": {bucket: 0 for bucket in PRISM_VARDIFF_IDLE_SECONDS_BUCKETS},
        "sum": 0.0,
        "count": 0,
    }


class VardiffService:
    """Own vardiff client windows and its bounded idle-retarget machinery."""

    def __init__(self, runtime: VardiffRuntime) -> None:
        self.runtime = runtime
        self.idle_retarget_count = 0
        self._vardiff_idle_lock = threading.Lock()
        self._vardiff_idle_executor: ThreadPoolExecutor | None = None
        self._vardiff_idle_executor_shutdown = False
        self._vardiff_idle_pending: set[tuple[ClientState, int]] = set()
        self.vardiff_idle_queue_depth = 0
        self.vardiff_idle_inflight = 0
        self.vardiff_idle_clients_inspected = 0
        self.vardiff_idle_skip_counts = {
            reason: 0 for reason in PRISM_VARDIFF_IDLE_SKIP_REASONS
        }
        self.vardiff_idle_task_failures = 0
        self.vardiff_idle_sweep_histogram = _new_histogram()
        self.vardiff_idle_task_histogram = _new_histogram()

    def client_config(self, client: ClientState) -> vardiff.VardiffConfig:
        with client_vardiff_lock(client):
            return (
                client.vardiff_config
                or client.listener_vardiff_config
                or self.runtime.vardiff_config
            )

    def startup_difficulty(
        self,
        profile: StratumListenerProfile | None = None,
    ) -> Decimal:
        config = (
            profile.vardiff_config
            if profile is not None
            else self.runtime.vardiff_config
        )
        fixed_difficulty = (
            profile.share_difficulty
            if profile is not None
            else self.runtime.share_difficulty
        )
        if not config.enabled:
            return fixed_difficulty
        return vardiff.clamp(
            config.startup_difficulty,
            config.min_difficulty,
            config.max_difficulty,
        )

    @staticmethod
    def desired_difficulty(client: ClientState) -> Decimal:
        with client_vardiff_lock(client):
            return client.pending_share_difficulty or client.share_difficulty

    def minimum_advertised_difficulty(self, client: ClientState) -> Decimal:
        with client_vardiff_lock(client):
            if client.minimum_advertised_difficulty <= 0:
                return Decimal("0")
            return max(
                client.minimum_advertised_difficulty,
                self.client_config(client).min_difficulty,
            )

    def note_submitted(self, client: ClientState) -> None:
        with client_vardiff_lock(client):
            if not self.client_config(client).enabled:
                return
            client.vardiff_window_submitted += 1

    def note_accepted(self, client: ClientState, share_difficulty: Decimal) -> None:
        now = time.monotonic()
        with client_vardiff_lock(client):
            config = self.client_config(client)
            if not config.enabled:
                return
            client.vardiff_window_accepted += 1
            client.vardiff_window_work += share_difficulty
            elapsed_seconds = Decimal(
                str(max(0.001, now - client.vardiff_window_started_monotonic))
            )
            if elapsed_seconds < config.retarget_interval_seconds:
                return
            accepted_shares = client.vardiff_window_accepted
            submitted_shares = client.vardiff_window_submitted
            accepted_difficulty = client.vardiff_window_work
            current_difficulty = (
                client.pending_share_difficulty or client.share_difficulty
            )
            client.vardiff_window_started_monotonic = now
            client.vardiff_window_accepted = 0
            client.vardiff_window_submitted = 0
            client.vardiff_window_work = Decimal("0")
        self.runtime.retarget_client(
            client,
            current_difficulty=current_difficulty,
            accepted_shares=accepted_shares,
            submitted_shares=submitted_shares,
            accepted_difficulty=accepted_difficulty,
            elapsed_seconds=elapsed_seconds,
        )

    def record_idle_skip(self, reason: str) -> None:
        if reason not in PRISM_VARDIFF_IDLE_SKIP_REASONS:
            raise ValueError(f"unknown vardiff idle skip reason: {reason}")
        with self._vardiff_idle_lock:
            self.vardiff_idle_skip_counts[reason] += 1

    def observe_idle_seconds(self, name: str, elapsed_seconds: float) -> None:
        if name not in {"sweep", "task"}:
            raise ValueError(f"unknown vardiff idle histogram: {name}")
        with self._vardiff_idle_lock:
            histogram = getattr(self, f"vardiff_idle_{name}_histogram")
            histogram["count"] = int(histogram["count"]) + 1
            histogram["sum"] = float(histogram["sum"]) + elapsed_seconds
            buckets = histogram["buckets"]
            for bucket in PRISM_VARDIFF_IDLE_SECONDS_BUCKETS:
                if elapsed_seconds <= bucket:
                    buckets[bucket] = int(buckets.get(bucket, 0)) + 1

    def idle_tip_diverged_locked(self) -> bool:
        published = getattr(self.runtime, "current_tip_first_seen", None)
        latest_detected = getattr(self.runtime, "latest_detected_tip", None)
        return bool(
            latest_detected is not None
            and (published is None or latest_detected[0] != published[0])
        )

    def _request_skip_reason_locked(
        self,
        request: IdleRetargetRequest,
    ) -> str | None:
        client = request.client
        if self.idle_tip_diverged_locked():
            return "superseded"
        if (
            client not in self.runtime.clients
            or getattr(client, "closing", False)
            or not self.runtime.client_can_receive_jobs(client)
        ):
            return "disconnected"
        if (
            client.connection_id != request.connection_id
            or client.worker != request.worker
            or client.active_job is not request.active_job
            or self.desired_difficulty(client) != request.current_difficulty
        ):
            return "superseded"
        if (
            client.vardiff_window_started_monotonic
            != request.window_started_monotonic
            or client.vardiff_window_accepted != 0
            or client.vardiff_window_submitted != 0
        ):
            return "not_idle"
        return None

    def request_skip_reason(self, request: IdleRetargetRequest) -> str | None:
        """Validate one idle request without inverting vardiff/control locks."""
        with client_vardiff_lock(request.client), self.runtime.lock:
            return self._request_skip_reason_locked(request)

    def _request_pending(self, request: IdleRetargetRequest) -> bool:
        with self._vardiff_idle_lock:
            return (
                request.client,
                request.connection_id,
            ) in self._vardiff_idle_pending

    def _finish_idle_task(
        self,
        key: tuple[ClientState, int],
        queued_monotonic: float,
        *,
        started: bool,
    ) -> None:
        with self._vardiff_idle_lock:
            if key not in self._vardiff_idle_pending:
                return
            self._vardiff_idle_pending.discard(key)
            if started:
                self.vardiff_idle_inflight = max(0, self.vardiff_idle_inflight - 1)
            else:
                self.vardiff_idle_queue_depth = max(
                    0,
                    self.vardiff_idle_queue_depth - 1,
                )
        self.observe_idle_seconds(
            "task",
            max(0.0, time.monotonic() - queued_monotonic),
        )

    def _run_idle_task(
        self,
        request: IdleRetargetRequest,
        bundle: CachedJobBundle | None,
        queued_monotonic: float,
    ) -> None:
        key = (request.client, request.connection_id)
        with self._vardiff_idle_lock:
            if key not in self._vardiff_idle_pending:
                return
            self.vardiff_idle_queue_depth = max(
                0,
                self.vardiff_idle_queue_depth - 1,
            )
            self.vardiff_idle_inflight += 1
        client = request.client
        delivery_attempted = False
        try:
            with client_vardiff_lock(client), self.runtime.lock:
                reason = self._request_skip_reason_locked(request)
            if reason is not None:
                self.runtime._record_vardiff_idle_skip(reason)
                return
            self.runtime.pool_readiness_latched()
            bundle = self.runtime._build_idle_job_bundle(request)
            with client_vardiff_lock(client), self.runtime.lock:
                reason = self._request_skip_reason_locked(request)
            if reason is not None:
                self.runtime._record_vardiff_idle_skip(reason)
                return
            if not self.runtime.ensure_reorg_reconciled_for_current_tip():
                self.runtime._record_vardiff_idle_skip("superseded")
                return
            if not client.job_update_lock.acquire(blocking=False):
                self.runtime._record_vardiff_idle_skip("busy")
                return
            try:
                with client_vardiff_lock(client), self.runtime.lock:
                    reason = self._request_skip_reason_locked(request)
                if reason is not None:
                    self.runtime._record_vardiff_idle_skip(reason)
                    return
                if not self.runtime._idle_bundle_current_locked(
                    client,
                    bundle,
                    allow_uncached=True,
                ):
                    self.runtime._record_vardiff_idle_skip("superseded")
                    return
                delivery_attempted = True
                retargeted = self.retarget_locked(
                    client,
                    current_difficulty=request.current_difficulty,
                    accepted_shares=0,
                    submitted_shares=0,
                    accepted_difficulty=Decimal("0"),
                    elapsed_seconds=request.elapsed_seconds,
                    require_idle=True,
                    prepared_bundle=bundle,
                    expected_connection_id=request.connection_id,
                    expected_worker=request.worker,
                    expected_active_job=request.active_job,
                    expected_window_started=request.window_started_monotonic,
                    prepared_bundle_allow_uncached=True,
                )
            finally:
                client.job_update_lock.release()
            if retargeted:
                with self._vardiff_idle_lock:
                    self.idle_retarget_count += 1
                return
            with client_vardiff_lock(client), self.runtime.lock:
                reason = self._request_skip_reason_locked(request)
            if reason is not None:
                self.runtime._record_vardiff_idle_skip(reason)
                return
            if not self.runtime._idle_bundle_current_locked(
                client,
                bundle,
                allow_uncached=True,
            ):
                self.runtime._record_vardiff_idle_skip("superseded")
        except JobBuildSuperseded:
            self.runtime._record_vardiff_idle_skip("superseded")
        except OSError:
            with self._vardiff_idle_lock:
                self.vardiff_idle_task_failures += 1
            if delivery_attempted:
                self.runtime.disconnect_client(client)
                return
            print(
                "prism coordinator: idle vardiff retarget preparation failed; "
                "keeping client connected",
                flush=True,
            )
            traceback.print_exc()
        except Exception:
            with self._vardiff_idle_lock:
                self.vardiff_idle_task_failures += 1
            print("prism coordinator: idle vardiff retarget task failed", flush=True)
            traceback.print_exc()

    def _enqueue_idle(
        self,
        request: IdleRetargetRequest,
        bundle: CachedJobBundle | None,
    ) -> str | None:
        key = (request.client, request.connection_id)
        queued_monotonic = time.monotonic()
        with self._vardiff_idle_lock:
            if (
                self._vardiff_idle_executor_shutdown
                or key in self._vardiff_idle_pending
            ):
                return "superseded"
            if len(self._vardiff_idle_pending) >= MAX_PENDING_VARDIFF_IDLE_RETARGETS:
                return "queue_full"
            executor = self._vardiff_idle_executor
            if executor is None:
                executor = ThreadPoolExecutor(
                    max_workers=PRISM_VARDIFF_IDLE_RETARGET_MAX_WORKERS,
                    thread_name_prefix="prism-vardiff-idle",
                )
                self._vardiff_idle_executor = executor
            self._vardiff_idle_pending.add(key)
            self.vardiff_idle_queue_depth += 1
            try:
                future = executor.submit(
                    self._run_idle_task,
                    request,
                    bundle,
                    queued_monotonic,
                )
            except RuntimeError:
                self._vardiff_idle_pending.discard(key)
                self.vardiff_idle_queue_depth = max(
                    0,
                    self.vardiff_idle_queue_depth - 1,
                )
                return "queue_full"

        def finish_task(completed: Future[None]) -> None:
            self._finish_idle_task(
                key,
                queued_monotonic,
                started=not completed.cancelled(),
            )

        future.add_done_callback(finish_task)
        return None

    def shutdown_idle_executor(self) -> None:
        with self._vardiff_idle_lock:
            executor = self._vardiff_idle_executor
            self._vardiff_idle_executor = None
            self._vardiff_idle_executor_shutdown = True
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)

    def idle_sweep_loop(self) -> None:
        while not self.runtime.stop_event.wait(
            self.runtime.vardiff_idle_sweep_seconds
        ):
            self.runtime._record_heartbeat("vardiff_idle_sweep")
            try:
                queued = self.idle_sweep_once()
                if queued:
                    print(
                        "prism coordinator: idle vardiff sweep queued "
                        f"{queued} client(s)",
                        flush=True,
                    )
            except Exception:
                print("prism coordinator: idle vardiff sweep failed", flush=True)
                traceback.print_exc()

    def idle_sweep_once(self) -> int:
        sweep_started = time.monotonic()
        now = time.monotonic()
        queued = 0
        try:
            with self.runtime.lock:
                clients = tuple(self.runtime.clients)
            with self._vardiff_idle_lock:
                self.vardiff_idle_clients_inspected += len(clients)
            for client in clients:
                self.runtime._record_heartbeat("vardiff_idle_sweep")
                with client_vardiff_lock(client), self.runtime.lock:
                    if self.idle_tip_diverged_locked():
                        reason = "superseded"
                        request = None
                    elif (
                        client not in self.runtime.clients
                        or not self.runtime.client_can_receive_jobs(client)
                    ):
                        reason = "disconnected"
                        request = None
                    else:
                        config = self.client_config(client)
                        active_job = client.active_job
                        worker = client.worker
                        if not config.enabled:
                            continue
                        if active_job is None or worker is None:
                            reason = "superseded"
                            request = None
                        else:
                            elapsed = Decimal(
                                str(
                                    max(
                                        0.001,
                                        now
                                        - client.vardiff_window_started_monotonic,
                                    )
                                )
                            )
                            if (
                                elapsed < config.retarget_interval_seconds
                                or client.vardiff_window_accepted != 0
                                or client.vardiff_window_submitted != 0
                            ):
                                reason = "not_idle"
                                request = None
                            else:
                                reason = None
                                request = IdleRetargetRequest(
                                    client=client,
                                    connection_id=client.connection_id,
                                    worker=worker,
                                    active_job=active_job,
                                    window_started_monotonic=(
                                        client.vardiff_window_started_monotonic
                                    ),
                                    current_difficulty=self.desired_difficulty(
                                        client
                                    ),
                                    elapsed_seconds=elapsed,
                                )
                if reason is not None:
                    self.runtime._record_vardiff_idle_skip(reason)
                    continue
                assert request is not None
                if self._request_pending(request):
                    self.runtime._record_vardiff_idle_skip("superseded")
                    continue
                if not client.job_update_lock.acquire(blocking=False):
                    self.runtime._record_vardiff_idle_skip("busy")
                    continue
                try:
                    with client_vardiff_lock(client), self.runtime.lock:
                        reason = self._request_skip_reason_locked(request)
                finally:
                    client.job_update_lock.release()
                if reason is not None:
                    self.runtime._record_vardiff_idle_skip(reason)
                    continue
                bundle = self.runtime._cached_idle_job_bundle(client)
                if bundle is None:
                    self.runtime._record_vardiff_idle_skip("cache_miss")
                reason = self._enqueue_idle(request, bundle)
                if reason is not None:
                    self.runtime._record_vardiff_idle_skip(reason)
                    continue
                queued += 1
            return queued
        finally:
            self.runtime._record_heartbeat("vardiff_idle_sweep")
            self.observe_idle_seconds(
                "sweep",
                max(0.0, time.monotonic() - sweep_started),
            )

    def retarget(
        self,
        client: ClientState,
        *,
        current_difficulty: Decimal,
        accepted_shares: int,
        submitted_shares: int,
        accepted_difficulty: Decimal,
        elapsed_seconds: Decimal,
        require_idle: bool = False,
        prepared_bundle: CachedJobBundle | None = None,
        expected_connection_id: int | None = None,
        expected_worker: WorkerIdentity | None = None,
        expected_active_job: PrismJobContext | None = None,
        expected_window_started: float | None = None,
    ) -> bool:
        acquired = client.job_update_lock.acquire(blocking=not require_idle)
        if not acquired:
            return False
        try:
            if require_idle and prepared_bundle is None:
                prepared_bundle = self.runtime._cached_idle_job_bundle(client)
                if prepared_bundle is None:
                    return False
            return self.retarget_locked(
                client,
                current_difficulty=current_difficulty,
                accepted_shares=accepted_shares,
                submitted_shares=submitted_shares,
                accepted_difficulty=accepted_difficulty,
                elapsed_seconds=elapsed_seconds,
                require_idle=require_idle,
                prepared_bundle=prepared_bundle,
                expected_connection_id=expected_connection_id,
                expected_worker=expected_worker,
                expected_active_job=expected_active_job,
                expected_window_started=expected_window_started,
            )
        finally:
            client.job_update_lock.release()

    def retarget_locked(
        self,
        client: ClientState,
        *,
        current_difficulty: Decimal,
        accepted_shares: int,
        submitted_shares: int,
        accepted_difficulty: Decimal,
        elapsed_seconds: Decimal,
        require_idle: bool = False,
        prepared_bundle: CachedJobBundle | None = None,
        expected_connection_id: int | None = None,
        expected_worker: WorkerIdentity | None = None,
        expected_active_job: PrismJobContext | None = None,
        expected_window_started: float | None = None,
        prepared_bundle_allow_uncached: bool = False,
    ) -> bool:
        config = self.client_config(client)
        if not config.enabled:
            return False
        if require_idle:
            if prepared_bundle is None:
                return False
            with client_vardiff_lock(client), self.runtime.lock:
                if expected_connection_id is None:
                    expected_connection_id = client.connection_id
                if expected_worker is None:
                    expected_worker = client.worker
                if expected_active_job is None:
                    expected_active_job = client.active_job
                if expected_window_started is None:
                    expected_window_started = (
                        client.vardiff_window_started_monotonic
                    )
                if not self._idle_authority_current_locked(
                    client,
                    expected_connection_id=expected_connection_id,
                    expected_worker=expected_worker,
                    expected_active_job=expected_active_job,
                    expected_window_started=expected_window_started,
                ):
                    return False
        observed_difficulty = vardiff.observed_difficulty(
            accepted_difficulty=accepted_difficulty,
            elapsed_seconds=elapsed_seconds,
            target_share_interval_seconds=config.target_share_interval_seconds,
        )
        with client_vardiff_lock(client):
            previous_estimate = client.vardiff_difficulty_estimate
        if observed_difficulty is None:
            difficulty_estimate = None
            with client_vardiff_lock(client):
                client.vardiff_difficulty_estimate = None
        else:
            difficulty_estimate = vardiff.smooth_difficulty_estimate(
                observed=observed_difficulty,
                previous=previous_estimate,
                config=config,
            )
            with client_vardiff_lock(client):
                client.vardiff_difficulty_estimate = difficulty_estimate
        next_difficulty = vardiff.calculate_next_difficulty(
            current_difficulty=current_difficulty,
            accepted_shares=accepted_shares,
            elapsed_seconds=elapsed_seconds,
            config=config,
            accepted_difficulty=accepted_difficulty,
            difficulty_estimate=difficulty_estimate,
        )
        if not vardiff.should_retarget(
            current_difficulty,
            next_difficulty,
            config.retarget_tolerance,
        ):
            return False
        idle_window_state: tuple[float, int, int, Decimal] | None = None
        idle_window_reset_at: float | None = None
        with client_vardiff_lock(client), self.runtime.lock:
            previous_difficulty = (
                client.pending_share_difficulty or client.share_difficulty
            )
            if previous_difficulty != current_difficulty:
                return False
            if require_idle and not self._idle_authority_current_locked(
                client,
                expected_connection_id=expected_connection_id,
                expected_worker=expected_worker,
                expected_active_job=expected_active_job,
                expected_window_started=expected_window_started,
            ):
                return False
            if require_idle:
                idle_window_state = (
                    client.vardiff_window_started_monotonic,
                    client.vardiff_window_accepted,
                    client.vardiff_window_submitted,
                    client.vardiff_window_work,
                )
            prior_pending = client.pending_share_difficulty
            client.pending_share_difficulty = next_difficulty
        idle_authority = (
            IdleDeliveryAuthority(
                connection_id=expected_connection_id,
                worker=expected_worker,
                expected_active_job=expected_active_job,
                expected_window_started=expected_window_started,
                pending_difficulty=next_difficulty,
            )
            if require_idle
            else None
        )

        def restore_speculative_retarget() -> None:
            reset_at = idle_window_reset_at
            if reset_at is None and idle_authority is not None:
                reset_at = idle_authority.committed_reset_monotonic
            with client_vardiff_lock(client):
                if client.pending_share_difficulty == next_difficulty:
                    client.pending_share_difficulty = prior_pending
                self.restore_idle_window_state(
                    client,
                    idle_window_state,
                    reset_at,
                )

        try:
            if require_idle:
                sent = self.runtime._maybe_send_job_locked(
                    client,
                    clean_jobs=True,
                    raise_on_build_failure=True,
                    prepared_bundle=prepared_bundle,
                    idle_authority=idle_authority,
                    prepared_bundle_allow_uncached=prepared_bundle_allow_uncached,
                )
                if idle_authority is not None:
                    idle_window_reset_at = (
                        idle_authority.committed_reset_monotonic
                    )
            else:
                sent = bool(
                    client.authorized
                    and client.subscribed
                    and not self.runtime.stop_event.is_set()
                    and self.runtime.maybe_send_job(client, clean_jobs=True)
                )
            if sent:
                return True
        except Exception:
            restore_speculative_retarget()
            raise
        restore_speculative_retarget()
        return False

    def _idle_authority_current_locked(
        self,
        client: ClientState,
        *,
        expected_connection_id: int | None,
        expected_worker: WorkerIdentity | None,
        expected_active_job: PrismJobContext | None,
        expected_window_started: float | None,
    ) -> bool:
        return bool(
            client in self.runtime.clients
            and not getattr(client, "closing", False)
            and self.runtime.client_can_receive_jobs(client)
            and client.connection_id == expected_connection_id
            and client.worker == expected_worker
            and client.active_job is expected_active_job
            and client.vardiff_window_started_monotonic
            == expected_window_started
            and client.vardiff_window_accepted == 0
            and client.vardiff_window_submitted == 0
        )

    @staticmethod
    def restore_idle_window_state(
        client: ClientState,
        idle_window_state: tuple[float, int, int, Decimal] | None,
        idle_window_reset_at: float | None,
    ) -> None:
        if idle_window_reset_at is None or idle_window_state is None:
            return
        if (
            client.vardiff_window_started_monotonic == idle_window_reset_at
            and client.vardiff_window_accepted == 0
            and client.vardiff_window_submitted == 0
            and client.vardiff_window_work == 0
        ):
            (
                client.vardiff_window_started_monotonic,
                client.vardiff_window_accepted,
                client.vardiff_window_submitted,
                client.vardiff_window_work,
            ) = idle_window_state

    def metrics_lines(self) -> list[str]:
        with self._vardiff_idle_lock:
            sweep = {
                "buckets": dict(self.vardiff_idle_sweep_histogram["buckets"]),
                "sum": float(self.vardiff_idle_sweep_histogram["sum"]),
                "count": int(self.vardiff_idle_sweep_histogram["count"]),
            }
            task = {
                "buckets": dict(self.vardiff_idle_task_histogram["buckets"]),
                "sum": float(self.vardiff_idle_task_histogram["sum"]),
                "count": int(self.vardiff_idle_task_histogram["count"]),
            }
            inspected = self.vardiff_idle_clients_inspected
            skip_counts = dict(self.vardiff_idle_skip_counts)
            queue_depth = self.vardiff_idle_queue_depth
            inflight = self.vardiff_idle_inflight
            failures = self.vardiff_idle_task_failures

        lines = [
            "# HELP qbit_prism_vardiff_idle_clients_inspected_total Clients inspected by bounded vardiff idle sweeps.",
            "# TYPE qbit_prism_vardiff_idle_clients_inspected_total counter",
            f"qbit_prism_vardiff_idle_clients_inspected_total {inspected}",
            "# HELP qbit_prism_vardiff_idle_skips_total Idle retargets skipped by a bounded reason.",
            "# TYPE qbit_prism_vardiff_idle_skips_total counter",
            *[
                f'qbit_prism_vardiff_idle_skips_total{{reason="{reason}"}} {int(skip_counts.get(reason, 0))}'
                for reason in PRISM_VARDIFF_IDLE_SKIP_REASONS
            ],
            "# HELP qbit_prism_vardiff_idle_queue_depth Cache-only idle retarget tasks waiting for a dedicated worker.",
            "# TYPE qbit_prism_vardiff_idle_queue_depth gauge",
            f"qbit_prism_vardiff_idle_queue_depth {queue_depth}",
            "# HELP qbit_prism_vardiff_idle_inflight Cache-only idle retarget tasks currently running.",
            "# TYPE qbit_prism_vardiff_idle_inflight gauge",
            f"qbit_prism_vardiff_idle_inflight {inflight}",
            "# HELP qbit_prism_vardiff_idle_task_failures_total Idle retarget tasks that failed during cached delivery.",
            "# TYPE qbit_prism_vardiff_idle_task_failures_total counter",
            f"qbit_prism_vardiff_idle_task_failures_total {failures}",
        ]
        for metric_name, description, histogram in (
            (
                "qbit_prism_vardiff_idle_sweep_seconds",
                "Wall time of one bounded vardiff idle sweep.",
                sweep,
            ),
            (
                "qbit_prism_vardiff_idle_retarget_task_seconds",
                "Queue plus execution latency for cache-only idle retarget tasks.",
                task,
            ),
        ):
            lines.extend(
                [
                    f"# HELP {metric_name} {description}",
                    f"# TYPE {metric_name} histogram",
                    *[
                        f'{metric_name}_bucket{{le="{bucket:g}"}} {histogram["buckets"].get(bucket, 0)}'
                        for bucket in PRISM_VARDIFF_IDLE_SECONDS_BUCKETS
                    ],
                    f'{metric_name}_bucket{{le="+Inf"}} {histogram["count"]}',
                    f'{metric_name}_sum {float(histogram["sum"]):.6f}',
                    f'{metric_name}_count {histogram["count"]}',
                ]
            )
        return lines
