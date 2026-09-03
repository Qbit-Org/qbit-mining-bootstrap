"""Cached PRISM base health with a fresh monotonic progress overlay."""

from __future__ import annotations

from dataclasses import dataclass
import threading
from typing import Callable, Iterator, Mapping, Protocol

from lab.prism.progress_health import (
    MINING_READINESS_REASON_REFRESH_PENDING_TOO_LONG,
    MINING_READINESS_REASON_WARMING_UP,
    MINING_READINESS_SCHEMA,
    MINING_READINESS_STATE_DEGRADED,
    MiningReadinessConfig,
    MiningReadinessSample,
    MiningReadinessSnapshot,
    MiningReadinessTracker,
    overlay_progress_health,
)


HEALTH_SCHEMA = "qbit.prism.audit-health.v1"
MINIMUM_HEALTH_STALE_SECONDS = 15.0
METRICS_FAILURE_CLASSES = ("exception", "invalid_payload")

# What /metrics says about the payload it just returned, so a consumer can tell
# fresh from stale from unavailable without parsing the Prometheus body (issue
# #184). Same shape as #164's database-state header: one bounded value an
# operator greps and a dashboard frontend switches a banner on, rather than a
# warn-code line to parse or a gauge to scrape out of the body it qualifies.
METRICS_STATE_HEADER = "X-Prism-Metrics-State"
METRICS_STATE_FRESH = "fresh"
METRICS_STATE_STALE = "stale"
METRICS_STATE_UNAVAILABLE = "unavailable"
METRICS_STATES = (
    METRICS_STATE_FRESH,
    METRICS_STATE_STALE,
    METRICS_STATE_UNAVAILABLE,
)

# Warning 110 is the registered "Response is Stale" warn-code (RFC 7234 5.5.1),
# with this service named as the agent that added it, exactly as #164 says it
# on a cached public response. Said only on a stale payload this endpoint
# actually served: the warm-up refusal has no cached body to qualify.
METRICS_STALE_WARNING_HEADER = "Warning"
METRICS_STALE_WARNING = (
    '110 qbit-prism "metrics snapshot is stale; serving last complete payload"'
)


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
    clients_with_semantically_current_work: int
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

    def mining_readiness_config(self) -> MiningReadinessConfig: ...

    def accepted_parent_preview_wait_timeouts(self) -> int: ...

    def render_metrics_payload(self) -> str: ...

    def metrics_refresh_seconds(self) -> float: ...

    def record_startup_phase(self, phase: str) -> None: ...

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
    mining_semantic_work_gap_started_monotonic: float | None


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


@dataclass(frozen=True)
class MetricsSnapshotResponse:
    """One /metrics answer: the bytes to send, and what they are (issue #184).

    Iterable as the ``(status, body)`` pair this read used to return, so the
    callers that only ever needed those two keep working unchanged while the
    HTTP layer gets the freshness facts it must publish out of band.
    """

    status: int
    body: str
    state: str
    age_seconds: int | None

    def __iter__(self) -> Iterator[object]:
        yield self.status
        yield self.body

    def response_headers(self) -> dict[str, str]:
        """State this answer's freshness where no body parsing is needed."""

        headers = {
            # A scrape answer is never re-servable: an intermediary holding a
            # stale body would republish it under this response's own Age.
            "Cache-Control": "no-store",
            METRICS_STATE_HEADER: self.state,
        }
        if self.age_seconds is not None:
            headers["Age"] = str(self.age_seconds)
        if self.state == METRICS_STATE_STALE:
            headers[METRICS_STALE_WARNING_HEADER] = METRICS_STALE_WARNING
        return headers


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
        self._mining_semantic_work_gap_started_monotonic: float | None = None
        # Issue #186: the latched router-facing readiness signal. The tracker
        # is driven only by the health refresher (serialized from collection
        # through publication by its own lock so a test driving refreshes
        # concurrently cannot publish an older observation after a newer
        # one); the published snapshot and the candidate age the metrics
        # renderer hands over live under the owner lock like every other
        # cached answer.
        self._mining_readiness_tracker = MiningReadinessTracker(
            port.mining_readiness_config()
        )
        self._mining_readiness_observe_lock = threading.Lock()
        self._mining_readiness_snapshot: MiningReadinessSnapshot | None = None
        self._oldest_durable_candidate_age_seconds: float | None = None
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
                mining_semantic_work_gap_started_monotonic=(
                    self._mining_semantic_work_gap_started_monotonic
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

    def replace_lock_for_test(self, lock: threading.RLock) -> None:
        self._lock = lock

    def clear_health_snapshot_for_test(self) -> None:
        with self._lock:
            self._health_snapshot = None

    def set_health_snapshot_monotonic_for_test(
        self,
        value: float | None,
    ) -> None:
        with self._lock:
            self._health_snapshot_monotonic = value

    def set_loop_running_for_test(self, value: bool) -> None:
        with self._lock:
            self._health_refresh_loop_running = bool(value)

    def set_delivery_failure_started_monotonic_for_test(
        self,
        value: float | None,
    ) -> None:
        # A test that starts past the deadline means the whole delivery gap:
        # seed the semantic timer alongside the strict tip-hash one.
        with self._lock:
            self._mining_delivery_failure_started_monotonic = value
            self._mining_semantic_work_gap_started_monotonic = value

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
        semantic_current = inputs.clients_with_semantically_current_work
        coverage = current / authorized if authorized else 1.0
        semantic_coverage = (
            semantic_current / authorized if authorized else 1.0
        )
        cap_saturated = (
            inputs.connection_capacity > 0
            and inputs.active_connections >= inputs.connection_capacity
        )
        pending_saturated = (
            inputs.pending_initial_jobs >= inputs.pending_initial_job_capacity
        )
        # A reconnect incident is operationally significant well before
        # nearly every miner is missing work. Treat any sustained loss of
        # at least five percent of current-job coverage as degraded.
        poor_coverage = authorized > 0 and coverage < 0.95
        # Exact-tip churn alone must not fail health (#216): a fleet holding
        # work for the current template content is being served even while
        # the strict tip-hash gauge lags the observed tip, so the delivery
        # stall is timed on semantic coverage. Boundary: overload_now,
        # reject_storm, and the cap-saturated reason deliberately stay on
        # exact-tip coverage. Each already needs independent corroboration
        # (queue/cap saturation or an observed rejection storm) and
        # diagnoses strict-tip operational pressure; semantic coverage
        # changes only the delivery-stalled predicate.
        poor_semantic_coverage = authorized > 0 and semantic_coverage < 0.95
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
            if poor_semantic_coverage:
                if self._mining_semantic_work_gap_started_monotonic is None:
                    self._mining_semantic_work_gap_started_monotonic = now
            else:
                self._mining_semantic_work_gap_started_monotonic = None
            semantic_gap_started = (
                self._mining_semantic_work_gap_started_monotonic
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
        semantic_gap_age = (
            max(0.0, now - semantic_gap_started)
            if semantic_gap_started is not None
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
        # A nonzero genuine pending age implies at least one genuinely
        # pending authorized client existed at capture time, so the age
        # threshold alone is the starvation predicate.
        initial_job_starved = bool(
            deadline is not None
            and inputs.oldest_genuinely_pending_initial_job_age_seconds >= deadline
        )
        # The semantic gap must be sustained for a full deadline, and the
        # strict gap alone can never fire it. Exact current-tip work is
        # checked against the same template fingerprint, payout generation,
        # and client-session generations whenever a template snapshot is
        # published, so in production it is a subset of the semantic count
        # and this reduces to the semantic gap; an embedder that publishes
        # only the observed tip has no semantic evidence, and its exact-tip
        # work must still count as delivered.
        current_work_coverage_stalled = bool(
            deadline is not None
            and poor_semantic_coverage
            and semantic_gap_age >= deadline
            and poor_coverage
            and delivery_failure_age >= deadline
        )
        no_delivery_progress = bool(
            initial_job_starved or current_work_coverage_stalled
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
            "clients_with_semantically_current_work": semantic_current,
            "semantic_current_work_ratio": round(semantic_coverage, 6),
            "current_tip_coverage_gap_age_seconds": round(
                delivery_failure_age,
                3,
            ),
            "semantic_current_work_gap_age_seconds": round(
                semantic_gap_age,
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
            # Compatibility aliases and preparation visibility introduced by
            # the prewarm work. These retain the original bounded-pipeline
            # names above for existing dashboards.
            "subscribed_clients": inputs.subscribed_connections,
            "authorized_clients": authorized,
            "clients_with_no_active_job": inputs.clients_with_no_active_job,
            "clients_without_current_tip_job": authorized - current,
            "clients_with_current_tip_job": current,
            "clients_pending_initial_job": inputs.pending_initial_jobs,
            "current_tip_job_coverage_ratio": coverage,
            # Compatibility alias: this now reports only genuine first-job
            # starvation. Current-tip fanout lag has its own age above.
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
        # Production has one health-refresher thread, but the compatibility
        # path can refresh inline when no loop is running. Serialize the whole
        # collection -> observation -> publication transaction so overlapping
        # inline callers cannot publish an older snapshot after a newer one.
        with self._mining_readiness_observe_lock:
            # Cache only ledger/session-backed base health. Progress is
            # monotonic, in-memory state and is deliberately re-read for every
            # response.
            base_health = self.base_health_payload()
            # The readiness sample is taken from this same refresh's delivery
            # snapshot and progress mapping, so the latched signal and
            # /healthz never disagree about what the fleet looked like at one
            # instant.
            progress = self.port.progress_health()
            readiness = self._observe_mining_readiness(base_health, progress)
            with self._lock:
                self._health_snapshot = base_health
                self._health_snapshot_monotonic = self.port.monotonic()
                self._mining_readiness_snapshot = readiness
        self.port.record_startup_phase("health_snapshot_warm")
        return self.apply_progress_health(base_health, progress)

    # --- Mining readiness (issue #186) -----------------------------------

    def record_oldest_durable_candidate_age(
        self,
        age_seconds: float | None,
    ) -> None:
        """Accept the candidate-outbox age the metrics renderer already read.

        The Postgres pending-candidate aggregate is fenced behind the writer
        lock. The metrics refresher already pays for it every cycle; the
        health refresher must not, or a block landing holding the writer
        lock would stall /healthz. So the value crosses owners here, once
        per metrics cycle, and the readiness sample reads the last one. A
        negative age is the gauge's "unavailable" and is stored as None.
        """

        value = (
            None
            if age_seconds is None or float(age_seconds) < 0.0
            else float(age_seconds)
        )
        with self._lock:
            self._oldest_durable_candidate_age_seconds = value

    def _observe_mining_readiness(
        self,
        base_health: Mapping[str, object],
        progress: Mapping[str, object],
    ) -> MiningReadinessSnapshot:
        reasons = progress.get("reasons") or ()
        pending_age = progress.get("pending_refresh_age_seconds")
        requiring_refresh = progress.get("eligible_clients_requiring_refresh")
        timeouts = int(self.port.accepted_parent_preview_wait_timeouts())
        with self._lock:
            candidate_age = self._oldest_durable_candidate_age_seconds
        sample = MiningReadinessSample(
            monotonic=self.port.monotonic(),
            semantic_current_work_ratio=float(
                base_health.get("semantic_current_work_ratio", 1.0)
            ),
            refresh_pending=bool(progress.get("pending_refresh", False)),
            refresh_pending_age_seconds=(
                None if pending_age is None else float(pending_age)
            ),
            refresh_pending_too_long=(
                MINING_READINESS_REASON_REFRESH_PENDING_TOO_LONG in reasons
            ),
            eligible_clients_requiring_refresh=int(requiring_refresh or 0),
            oldest_durable_candidate_age_seconds=candidate_age,
            accepted_parent_preview_wait_timeouts=timeouts,
        )
        return self._mining_readiness_tracker.observe(sample)

    def mining_readiness_snapshot(self) -> MiningReadinessSnapshot | None:
        """The cached latched answer, or None before the first refresh."""

        with self._lock:
            return self._mining_readiness_snapshot

    def _mining_readiness_stale_after(self) -> float:
        return max(
            3 * self.port.health_refresh_seconds(),
            MINIMUM_HEALTH_STALE_SECONDS,
        )

    def cached_mining_readiness_payload(self) -> tuple[int, dict[str, object]]:
        """Answer /readyz/mining from the cache alone.

        This copies the published snapshot and reads the clock. It never
        takes the coordinator lock, never touches job delivery or candidate
        processing, and never queries the ledger; before the first complete
        refresh it fails closed with a warming-up 503 rather than sampling
        anything on the request thread.
        """

        with self._lock:
            snapshot = self._mining_readiness_snapshot
        now = self.port.monotonic()
        stale_after = self._mining_readiness_stale_after()
        if snapshot is None:
            return 503, {
                "schema": MINING_READINESS_SCHEMA,
                "ready": False,
                "state": MINING_READINESS_STATE_DEGRADED,
                "reasons": [MINING_READINESS_REASON_WARMING_UP],
                "state_age_seconds": 0.0,
                "transitions": 0,
                "entry_streak_seconds": 0.0,
                "recovery_streak_seconds": 0.0,
                "semantic_current_work_ratio": None,
                "refresh_pending": None,
                "refresh_pending_age_seconds": None,
                "refresh_pending_too_long": None,
                "eligible_clients_requiring_refresh": None,
                "oldest_durable_candidate_age_seconds": None,
                "accepted_parent_preview_timeout_rate_per_second": 0.0,
                "entry_dwell_seconds": (
                    self._mining_readiness_tracker.config.entry_dwell_seconds
                ),
                "recovery_window_seconds": (
                    self._mining_readiness_tracker.config.recovery_window_seconds
                ),
                "sample_age_seconds": None,
                "sample_stale": True,
                "sample_stale_after_seconds": round(stale_after, 3),
                "error": "mining readiness warm-up has not completed yet",
            }
        sample_age = snapshot.sample_age_seconds(now)
        payload: dict[str, object] = {
            "schema": MINING_READINESS_SCHEMA,
            "ready": snapshot.ready,
            "state": snapshot.state,
            "reasons": list(snapshot.reasons),
            "state_age_seconds": round(snapshot.state_age_seconds(now), 3),
            "transitions": snapshot.transitions,
            "entry_streak_seconds": round(snapshot.entry_streak_seconds, 3),
            "recovery_streak_seconds": round(snapshot.recovery_streak_seconds, 3),
            "semantic_current_work_ratio": round(
                snapshot.semantic_current_work_ratio,
                6,
            ),
            "refresh_pending": snapshot.refresh_pending,
            "refresh_pending_age_seconds": (
                None
                if snapshot.refresh_pending_age_seconds is None
                else round(snapshot.refresh_pending_age_seconds, 3)
            ),
            "refresh_pending_too_long": snapshot.refresh_pending_too_long,
            "eligible_clients_requiring_refresh": (
                snapshot.eligible_clients_requiring_refresh
            ),
            "oldest_durable_candidate_age_seconds": (
                None
                if snapshot.oldest_durable_candidate_age_seconds is None
                else round(snapshot.oldest_durable_candidate_age_seconds, 3)
            ),
            "accepted_parent_preview_timeout_rate_per_second": round(
                snapshot.accepted_parent_preview_timeout_rate_per_second,
                6,
            ),
            "entry_dwell_seconds": snapshot.entry_dwell_seconds,
            "recovery_window_seconds": snapshot.recovery_window_seconds,
            "sample_age_seconds": round(sample_age, 3),
            # Diagnostic only: the latched state is served either way, so a
            # wedged refresher cannot make the signal flap. A consumer that
            # wants to treat a stale sample as not-ready has the facts to.
            "sample_stale": sample_age > stale_after,
            "sample_stale_after_seconds": round(stale_after, 3),
        }
        return (200 if snapshot.ready else 503), payload

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
                # No refresher (tests, or audit HTTP without serve()): compute
                # inline like the legacy endpoint did.
                payload = self.refresh_health_snapshot()
                return (200 if payload.get("ok") else 503), payload
            # The refresher owns the cold accepted-share warm-up; until its
            # first snapshot lands, report an explicit starting state instead
            # of running ledger aggregates on the handler thread (issue #188
            # fix 4).
            payload = self._with_current_progress(
                {
                    "ok": False,
                    "schema": HEALTH_SCHEMA,
                    "state": "starting",
                    "error": "health snapshot warm-up has not completed yet",
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
        # Ledger-backed fields stay cached, but progress state is an in-memory
        # monotonic snapshot and must be overlaid on every request. Otherwise a
        # cached ok=true response can mask a known failed refresh for another
        # full cache cycle (the production incident this endpoint must expose).
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
        # The running flag is a one-shot latch (upstream #120): it is armed
        # before the refresher thread can be dispatched and deliberately
        # never cleared, so a request racing loop exit (or arriving during
        # shutdown drain) fails closed through the cached path instead of
        # re-opening cached_health_payload's legacy inline aggregate.
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
    ) -> MetricsSnapshotResponse:
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
        body = prefix + "\n".join(diagnostic_lines) + "\n"
        if snapshot is None:
            # Nothing complete has ever been published, so there is no payload
            # to serve and no truthful age to publish beside it. Refuse, and
            # say so out of band too: this body is diagnostics only, and a
            # scraper that stored it would be storing a metrics document that
            # is missing every metric the process actually reports.
            return MetricsSnapshotResponse(
                status=503,
                body=body,
                state=METRICS_STATE_UNAVAILABLE,
                age_seconds=None,
            )
        # A complete payload exists, so it is served -- stale or not (#184).
        # Prometheus discards the body of a non-200, so answering 503 here threw
        # the last known-good document away at exactly the moment it was the
        # only thing left to look at, and took the diagnostic gauges above with
        # it. The staleness is not hidden: it travels in the response metadata
        # and in snapshot_stale/snapshot_age_seconds. Nothing is rendered or
        # collected on this thread either way -- the published generation is
        # returned as it stands, so a collector wedged on the GIL or on the
        # renderer lock cannot delay or re-enter this path.
        return MetricsSnapshotResponse(
            status=200,
            body=body,
            state=METRICS_STATE_STALE if stale else METRICS_STATE_FRESH,
            age_seconds=int(age_seconds),
        )

    def cached_metrics_payload(self) -> MetricsSnapshotResponse:
        """Return the cached answer using only monotonic time and metrics state."""

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
    "METRICS_STALE_WARNING",
    "METRICS_STALE_WARNING_HEADER",
    "METRICS_STATES",
    "METRICS_STATE_FRESH",
    "METRICS_STATE_HEADER",
    "METRICS_STATE_STALE",
    "METRICS_STATE_UNAVAILABLE",
    "MetricsObservabilityState",
    "MetricsSnapshotResponse",
    "MiningDeliveryInputs",
    "ObservabilityPort",
    "ObservabilityService",
    "ObservabilityState",
]
