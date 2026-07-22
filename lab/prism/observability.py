"""Cached PRISM base health with a fresh monotonic progress overlay."""

from __future__ import annotations

from dataclasses import dataclass
import threading
from typing import Callable, Mapping, Protocol

from lab.prism.progress_health import overlay_progress_health


HEALTH_SCHEMA = "qbit.prism.audit-health.v1"
MINIMUM_HEALTH_STALE_SECONDS = 15.0
METRICS_FAILURE_CLASSES = ("exception", "invalid_payload")


@dataclass(frozen=True)
class MiningDeliveryInputs:
    """One immutable collection of coordinator facts used for health policy."""

    active_connections: int
    connection_capacity: int
    peak_active_connections: int
    subscribed_connections: int
    authorized_connections: int
    pending_initial_jobs: int
    pending_initial_job_capacity: int
    oldest_pending_initial_job_age_seconds: float
    oldest_genuinely_pending_initial_job_age_seconds: float
    clients_with_current_tip_jobs: int
    clients_with_no_active_job: int
    last_initial_job_delivery_monotonic: float | None
    initial_job_timeout_seconds: float
    initial_job_queue_rejections: int
    initial_job_timeout_disconnects: int
    initial_job_cancelled_tasks: int
    initial_job_coalesced_tasks: int
    initial_job_queue_capacity_reclaimed: int
    handler_threads: int
    delivery_executor_queue_depth: int
    delivery_executor_active_workers: int
    started_monotonic: float
    startup_grace_seconds: float
    stale_unknown_rejections: int
    submitted_shares: int
    job_preparation_pending: bool
    current_observed_tip: str | None
    prepared_bundle_current: bool
    prepared_bundle_tip: str | None
    prepared_bundle_template_generation: int | None
    prepared_bundle_payout_generation: int | None


class ObservabilityPort(Protocol):
    """Narrow coordinator adapter required by health collection."""

    def monotonic(self) -> float: ...

    def mining_delivery_inputs(self, now: float) -> MiningDeliveryInputs: ...

    def accepted_share_stats(self) -> tuple[int, int]: ...

    def ledger_backend(self) -> str: ...

    def block_counts(self) -> tuple[int, int]: ...

    def progress_health(self) -> Mapping[str, object]: ...

    def health_refresh_seconds(self) -> float: ...

    def render_metrics_payload(self) -> str: ...

    def metrics_refresh_seconds(self) -> float: ...

    def stop_requested(self) -> bool: ...

    def wait_for_stop(self, timeout: float) -> bool: ...

    def log(self, message: str) -> None: ...

    def log_exception(self) -> None: ...


@dataclass(frozen=True)
class ObservabilityState:
    health_snapshot: dict[str, object] | None
    health_snapshot_monotonic: float | None
    health_refresh_loop_running: bool
    health_snapshot_refresh_failure_count: int
    mining_overload_started_monotonic: float | None
    mining_delivery_failure_started_monotonic: float | None


@dataclass(frozen=True)
class MetricsObservabilityState:
    metrics_snapshot: str | None
    metrics_snapshot_monotonic: float | None
    metrics_refresh_loop_running: bool
    metrics_collection_success_count: int
    metrics_collection_failure_count: int
    metrics_collection_generation: int
    metrics_failure_exception_count: int
    metrics_failure_invalid_payload_count: int
    metrics_last_failure_class: str | None


class _InvalidMetricsPayload(ValueError):
    pass


class ObservabilityService:
    """Own health policy, the cached base snapshot, and refresher lifecycle."""

    def __init__(
        self,
        port: ObservabilityPort,
        *,
        lock_factory: Callable[[], threading.RLock] = threading.RLock,
    ) -> None:
        self.port = port
        self._lock: threading.RLock = lock_factory()
        self._health_snapshot: dict[str, object] | None = None
        self._health_snapshot_monotonic: float | None = None
        self._health_refresh_loop_running = False
        self._health_snapshot_refresh_failure_count = 0
        self._mining_overload_started_monotonic: float | None = None
        self._mining_delivery_failure_started_monotonic: float | None = None
        self._mining_delivery_lock = threading.Lock()
        self._metrics_lock = threading.RLock()
        self._metrics_collection_lock = threading.Lock()
        self._metrics_snapshot: str | None = None
        self._metrics_snapshot_monotonic: float | None = None
        self._metrics_refresh_loop_running = False
        self._metrics_collection_success_count = 0
        self._metrics_collection_failure_count = 0
        self._metrics_collection_generation = 0
        self._metrics_failure_counts = {
            failure_class: 0 for failure_class in METRICS_FAILURE_CLASSES
        }
        self._metrics_last_failure_class: str | None = None

    def state(self) -> ObservabilityState:
        with self._lock:
            return ObservabilityState(
                health_snapshot=(
                    None
                    if self._health_snapshot is None
                    else dict(self._health_snapshot)
                ),
                health_snapshot_monotonic=self._health_snapshot_monotonic,
                health_refresh_loop_running=self._health_refresh_loop_running,
                health_snapshot_refresh_failure_count=(
                    self._health_snapshot_refresh_failure_count
                ),
                mining_overload_started_monotonic=(
                    self._mining_overload_started_monotonic
                ),
                mining_delivery_failure_started_monotonic=(
                    self._mining_delivery_failure_started_monotonic
                ),
            )

    def metrics_state(self) -> MetricsObservabilityState:
        with self._metrics_lock:
            return MetricsObservabilityState(
                metrics_snapshot=self._metrics_snapshot,
                metrics_snapshot_monotonic=self._metrics_snapshot_monotonic,
                metrics_refresh_loop_running=self._metrics_refresh_loop_running,
                metrics_collection_success_count=(
                    self._metrics_collection_success_count
                ),
                metrics_collection_failure_count=(
                    self._metrics_collection_failure_count
                ),
                metrics_collection_generation=self._metrics_collection_generation,
                metrics_failure_exception_count=self._metrics_failure_counts[
                    "exception"
                ],
                metrics_failure_invalid_payload_count=self._metrics_failure_counts[
                    "invalid_payload"
                ],
                metrics_last_failure_class=self._metrics_last_failure_class,
            )

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def replace_lock_for_test(self, lock: threading.RLock) -> None:
        self._lock = lock

    def set_health_snapshot_for_compatibility(
        self,
        snapshot: dict[str, object] | None,
    ) -> None:
        with self._lock:
            self._health_snapshot = snapshot

    def set_health_snapshot_monotonic_for_compatibility(
        self,
        value: float | None,
    ) -> None:
        with self._lock:
            self._health_snapshot_monotonic = value

    def set_loop_running_for_compatibility(self, value: bool) -> None:
        with self._lock:
            self._health_refresh_loop_running = bool(value)

    def set_refresh_failure_count_for_compatibility(self, value: int) -> None:
        with self._lock:
            self._health_snapshot_refresh_failure_count = int(value)

    def set_delivery_failure_started_monotonic_for_test(
        self,
        value: float | None,
    ) -> None:
        with self._lock:
            self._mining_delivery_failure_started_monotonic = value

    def reset_delivery_failure(self) -> None:
        """Start the next current-tip coverage gap with a fresh grace window."""

        with self._lock:
            self._mining_delivery_failure_started_monotonic = None

    def mining_delivery_snapshot(
        self,
        *,
        now: float | None = None,
    ) -> dict[str, object]:
        with self._mining_delivery_lock:
            return self._mining_delivery_snapshot_serialized(now=now)

    def _mining_delivery_snapshot_serialized(
        self,
        *,
        now: float | None,
    ) -> dict[str, object]:
        now = self.port.monotonic() if now is None else now
        inputs = self.port.mining_delivery_inputs(now)
        authorized = inputs.authorized_connections
        current = inputs.clients_with_current_tip_jobs
        coverage = current / authorized if authorized else 1.0
        cap_saturated = (
            inputs.connection_capacity > 0
            and inputs.active_connections >= inputs.connection_capacity
        )
        pending_saturated = (
            inputs.pending_initial_jobs >= inputs.pending_initial_job_capacity
        )
        # Any sustained loss of at least five percent of current-job coverage
        # is operationally degraded during a reconnect incident.
        poor_coverage = authorized > 0 and coverage < 0.95
        overload_now = pending_saturated or (cap_saturated and poor_coverage)
        with self._lock:
            if poor_coverage:
                if self._mining_delivery_failure_started_monotonic is None:
                    self._mining_delivery_failure_started_monotonic = now
            else:
                self._mining_delivery_failure_started_monotonic = None
            delivery_failure_started = (
                self._mining_delivery_failure_started_monotonic
            )
            if overload_now:
                if self._mining_overload_started_monotonic is None:
                    self._mining_overload_started_monotonic = now
            else:
                self._mining_overload_started_monotonic = None
            overload_started = self._mining_overload_started_monotonic
        delivery_failure_age = (
            max(0.0, now - delivery_failure_started)
            if delivery_failure_started is not None
            else 0.0
        )
        overload_age = (
            max(0.0, now - overload_started)
            if overload_started is not None
            else 0.0
        )
        deadline = (
            inputs.initial_job_timeout_seconds
            if inputs.initial_job_timeout_seconds > 0
            else None
        )
        startup_age = max(0.0, now - inputs.started_monotonic)
        in_startup_grace = startup_age < inputs.startup_grace_seconds
        initial_job_starved = bool(
            deadline is not None
            and inputs.oldest_genuinely_pending_initial_job_age_seconds >= deadline
        )
        current_tip_coverage_stalled = bool(
            deadline is not None
            and poor_coverage
            and delivery_failure_age >= deadline
        )
        no_delivery_progress = bool(
            initial_job_starved or current_tip_coverage_stalled
        )
        reject_storm = (
            poor_coverage
            and inputs.submitted_shares > 0
            and inputs.stale_unknown_rejections / inputs.submitted_shares >= 0.95
        )
        persistent_overload = deadline is not None and overload_age >= deadline
        unhealthy_reasons: list[str] = []
        if not in_startup_grace:
            if no_delivery_progress:
                unhealthy_reasons.append("initial-delivery-stalled")
            if pending_saturated and persistent_overload:
                unhealthy_reasons.append("pending-initial-jobs-saturated")
            if cap_saturated and poor_coverage and persistent_overload:
                unhealthy_reasons.append("connection-capacity-saturated")
            if reject_storm:
                unhealthy_reasons.append("stale-unknown-rejection-storm")
        mining_ready = not unhealthy_reasons
        return {
            "mining_ready": mining_ready,
            "mining_delivery_healthy": mining_ready,
            "mining_health_startup_grace": in_startup_grace,
            "active_connections": inputs.active_connections,
            "connection_capacity": inputs.connection_capacity,
            "peak_active_connections": inputs.peak_active_connections,
            "subscribed_connections": inputs.subscribed_connections,
            "authorized_connections": authorized,
            "pending_initial_jobs": inputs.pending_initial_jobs,
            "pending_initial_job_capacity": inputs.pending_initial_job_capacity,
            "oldest_pending_initial_job_age_seconds": round(
                inputs.oldest_pending_initial_job_age_seconds,
                3,
            ),
            "oldest_genuinely_pending_initial_job_age_seconds": round(
                inputs.oldest_genuinely_pending_initial_job_age_seconds,
                3,
            ),
            "clients_with_current_tip_jobs": current,
            "current_tip_job_coverage": round(coverage, 6),
            "current_tip_coverage_gap_age_seconds": round(
                delivery_failure_age,
                3,
            ),
            "connection_capacity_saturated": cap_saturated,
            "pending_initial_jobs_saturated": pending_saturated,
            "initial_delivery_stalled": no_delivery_progress,
            "overload": bool(overload_now or reject_storm),
            "overload_age_seconds": round(overload_age, 3),
            "unhealthy_reasons": unhealthy_reasons,
            "initial_job_queue_rejections": inputs.initial_job_queue_rejections,
            "initial_job_timeout_disconnects": (
                inputs.initial_job_timeout_disconnects
            ),
            "initial_job_cancelled_tasks": inputs.initial_job_cancelled_tasks,
            "initial_job_coalesced_tasks": inputs.initial_job_coalesced_tasks,
            "initial_job_queue_capacity_reclaimed": (
                inputs.initial_job_queue_capacity_reclaimed
            ),
            "handler_threads": inputs.handler_threads,
            "delivery_executor_queue_depth": (
                inputs.delivery_executor_queue_depth
            ),
            "delivery_executor_active_workers": (
                inputs.delivery_executor_active_workers
            ),
            # Compatibility aliases retained for existing dashboards.
            "subscribed_clients": inputs.subscribed_connections,
            "authorized_clients": authorized,
            "clients_with_no_active_job": inputs.clients_with_no_active_job,
            "clients_without_current_tip_job": authorized - current,
            "clients_with_current_tip_job": current,
            "clients_pending_initial_job": inputs.pending_initial_jobs,
            "current_tip_job_coverage_ratio": coverage,
            # Compatibility alias: only genuine first-job starvation lives here.
            "oldest_initial_job_pending_seconds": round(
                inputs.oldest_genuinely_pending_initial_job_age_seconds,
                3,
            ),
            "job_preparation_pending": inputs.job_preparation_pending,
            "current_observed_tip": inputs.current_observed_tip,
            "prepared_bundle_current": inputs.prepared_bundle_current,
            "prepared_bundle_tip": inputs.prepared_bundle_tip,
            "prepared_bundle_template_generation": (
                inputs.prepared_bundle_template_generation
            ),
            "prepared_bundle_payout_generation": (
                inputs.prepared_bundle_payout_generation
            ),
        }

    def base_health_payload(self) -> dict[str, object]:
        accepted_share_count, ready_miner_count = self.port.accepted_share_stats()
        mining = self.mining_delivery_snapshot()
        accepted_block_count, max_blocks = self.port.block_counts()
        return {
            "ok": bool(mining["mining_ready"]),
            "schema": HEALTH_SCHEMA,
            "ledger_backend": self.port.ledger_backend(),
            "accepted_share_count": accepted_share_count,
            "ready_miner_count": ready_miner_count,
            "accepted_block": accepted_block_count > 0,
            "accepted_block_count": accepted_block_count,
            "max_blocks": max_blocks,
            **mining,
        }

    def _with_current_progress(
        self,
        base_health: Mapping[str, object],
    ) -> dict[str, object]:
        return self.apply_progress_health(
            base_health,
            self.port.progress_health(),
        )

    @staticmethod
    def apply_progress_health(
        base_health: Mapping[str, object],
        progress: Mapping[str, object],
    ) -> dict[str, object]:
        """Retain the existing overlay contract behind the new owner."""

        return dict(overlay_progress_health(base_health, progress))

    def health_payload(self) -> dict[str, object]:
        return self._with_current_progress(self.base_health_payload())

    def refresh_health_snapshot(self) -> dict[str, object]:
        # Cache only ledger/session-backed base health. Progress is monotonic,
        # in-memory state and is deliberately re-read for every response.
        base_health = self.base_health_payload()
        with self._lock:
            self._health_snapshot = base_health
            self._health_snapshot_monotonic = self.port.monotonic()
        return self._with_current_progress(base_health)

    def cached_health_payload(self) -> tuple[int, dict[str, object]]:
        with self._lock:
            snapshot = (
                None
                if self._health_snapshot is None
                else dict(self._health_snapshot)
            )
            snapshot_monotonic = self._health_snapshot_monotonic
            loop_running = self._health_refresh_loop_running
        if snapshot is None or snapshot_monotonic is None:
            if not loop_running:
                payload = self.refresh_health_snapshot()
                return (200 if payload.get("ok") else 503), payload
            payload = self._with_current_progress(
                {
                    "ok": False,
                    "schema": HEALTH_SCHEMA,
                    "error": "health snapshot is not available yet",
                }
            )
            payload["ok"] = False
            return 503, payload
        age_seconds = self.port.monotonic() - snapshot_monotonic
        stale_after = max(
            3 * self.port.health_refresh_seconds(),
            MINIMUM_HEALTH_STALE_SECONDS,
        )
        if age_seconds > stale_after:
            payload = self._with_current_progress(
                {
                    "ok": False,
                    "schema": HEALTH_SCHEMA,
                    "error": "health snapshot is stale",
                    "snapshot_age_seconds": round(age_seconds, 3),
                }
            )
            payload["ok"] = False
            return 503, payload
        payload = self._with_current_progress(snapshot)
        payload["snapshot_age_seconds"] = round(age_seconds, 3)
        return (200 if payload.get("ok") else 503), payload

    def begin_refresh_loop(self) -> bool:
        with self._lock:
            if self._health_refresh_loop_running:
                return False
            self._health_refresh_loop_running = True
            return True

    def health_snapshot_loop(self) -> None:
        try:
            while not self.port.stop_requested():
                try:
                    self.refresh_health_snapshot()
                except Exception:
                    with self._lock:
                        self._health_snapshot_refresh_failure_count += 1
                    self.port.log("prism coordinator: health snapshot refresh failed")
                    self.port.log_exception()
                if self.port.wait_for_stop(self.port.health_refresh_seconds()):
                    break
        finally:
            with self._lock:
                self._health_refresh_loop_running = False

    @staticmethod
    def _metrics_failure_class(exc: Exception) -> str:
        if isinstance(exc, _InvalidMetricsPayload):
            return "invalid_payload"
        return "exception"

    def refresh_metrics_snapshot(self) -> str:
        """Collect and atomically publish one complete Prometheus document."""

        try:
            with self._metrics_collection_lock:
                payload = self.port.render_metrics_payload()
                if not isinstance(payload, str) or not payload or not payload.endswith(
                    "\n"
                ):
                    raise _InvalidMetricsPayload(
                        "metrics renderer must return non-empty newline-terminated text"
                    )
                collected_monotonic = self.port.monotonic()
                with self._metrics_lock:
                    self._metrics_snapshot = payload
                    self._metrics_snapshot_monotonic = collected_monotonic
                    self._metrics_collection_success_count += 1
                    self._metrics_collection_generation += 1
                    self._metrics_last_failure_class = None
            return payload
        except Exception as exc:
            failure_class = self._metrics_failure_class(exc)
            with self._metrics_lock:
                self._metrics_collection_failure_count += 1
                self._metrics_failure_counts[failure_class] += 1
                self._metrics_last_failure_class = failure_class
            raise

    def _metrics_snapshot_response(
        self,
        *,
        now: float,
        refresh_seconds: float,
    ) -> tuple[int, str]:
        with self._metrics_lock:
            snapshot = self._metrics_snapshot
            snapshot_monotonic = self._metrics_snapshot_monotonic
            running = self._metrics_refresh_loop_running
            successes = self._metrics_collection_success_count
            generation = self._metrics_collection_generation
            failure_counts = dict(self._metrics_failure_counts)
        age_seconds = (
            -1.0
            if snapshot_monotonic is None
            else max(0.0, now - snapshot_monotonic)
        )
        stale_after = max(3 * refresh_seconds, MINIMUM_HEALTH_STALE_SECONDS)
        stale = snapshot is None or age_seconds > stale_after
        diagnostic_lines = [
            "# HELP qbit_prism_metrics_snapshot_available Whether a complete metrics snapshot is available.",
            "# TYPE qbit_prism_metrics_snapshot_available gauge",
            f"qbit_prism_metrics_snapshot_available {1 if snapshot is not None else 0}",
            "# HELP qbit_prism_metrics_snapshot_stale Whether the complete metrics snapshot exceeded its staleness budget.",
            "# TYPE qbit_prism_metrics_snapshot_stale gauge",
            f"qbit_prism_metrics_snapshot_stale {1 if stale else 0}",
            "# HELP qbit_prism_metrics_snapshot_age_seconds Age of the complete metrics snapshot, or -1 before first success.",
            "# TYPE qbit_prism_metrics_snapshot_age_seconds gauge",
            f"qbit_prism_metrics_snapshot_age_seconds {age_seconds:.3f}",
            "# HELP qbit_prism_metrics_collector_running Whether the background metrics collector is running.",
            "# TYPE qbit_prism_metrics_collector_running gauge",
            f"qbit_prism_metrics_collector_running {1 if running else 0}",
            "# HELP qbit_prism_metrics_collection_successes_total Complete metrics snapshots published.",
            "# TYPE qbit_prism_metrics_collection_successes_total counter",
            f"qbit_prism_metrics_collection_successes_total {successes}",
            "# HELP qbit_prism_metrics_collection_failures_total Metrics collection failures by bounded class.",
            "# TYPE qbit_prism_metrics_collection_failures_total counter",
            *(
                f'qbit_prism_metrics_collection_failures_total{{class="{failure_class}"}} {failure_counts[failure_class]}'
                for failure_class in METRICS_FAILURE_CLASSES
            ),
            "# HELP qbit_prism_metrics_snapshot_generation Complete metrics snapshot generation.",
            "# TYPE qbit_prism_metrics_snapshot_generation gauge",
            f"qbit_prism_metrics_snapshot_generation {generation}",
        ]
        prefix = "" if snapshot is None else snapshot
        return (503 if stale else 200), prefix + "\n".join(diagnostic_lines) + "\n"

    def cached_metrics_payload(self) -> tuple[int, str]:
        """Return cached text using only monotonic time and metrics state."""

        now = self.port.monotonic()
        refresh_seconds = self.port.metrics_refresh_seconds()
        return self._metrics_snapshot_response(
            now=now,
            refresh_seconds=refresh_seconds,
        )

    def begin_metrics_refresh_loop(self) -> bool:
        with self._metrics_lock:
            if self._metrics_refresh_loop_running:
                return False
            self._metrics_refresh_loop_running = True
            return True

    def metrics_snapshot_loop(self) -> None:
        try:
            while not self.port.stop_requested():
                try:
                    self.refresh_metrics_snapshot()
                except Exception:
                    self.port.log("prism coordinator: metrics snapshot refresh failed")
                    self.port.log_exception()
                if self.port.wait_for_stop(self.port.metrics_refresh_seconds()):
                    break
        finally:
            with self._metrics_lock:
                self._metrics_refresh_loop_running = False


__all__ = [
    "HEALTH_SCHEMA",
    "MINIMUM_HEALTH_STALE_SECONDS",
    "METRICS_FAILURE_CLASSES",
    "MetricsObservabilityState",
    "MiningDeliveryInputs",
    "ObservabilityPort",
    "ObservabilityService",
    "ObservabilityState",
]
