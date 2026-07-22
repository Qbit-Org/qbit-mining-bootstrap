"""PRISM tip observation, publication, and bounded refresh ownership.

The service deliberately has no dependency on :mod:`prism_coordinator`.
Construction wires narrow ports for qbit RPC, payout/job-bundle invalidation,
progress-health events, and the temporary client-delivery boundary that S2
will replace.
"""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, wait
from contextlib import contextmanager
from dataclasses import dataclass, field, replace as dataclass_replace
import random
import threading
import time
import traceback
from typing import Any, Callable, Mapping, Protocol

from lab.prism.bounded_executor import _BoundedPriorityExecutor
from lab.prism.coordinator_shutdown import ShutdownInProgress
from lab.prism.job_bundle import CachedJobBundle, JobBuildKey, JobBuildSuperseded
from lab.prism.payout_state import (
    DEFAULT_PRISM_PAYOUT_RECONCILE_SUPERSESSION_RETRIES,
    PayoutStatePublicationBlocked,
    PayoutStateSnapshot,
    TemplateRefreshBlocked,
    TemplateRefreshSuperseded,
)
from lab.prism.template_artifacts import (
    CachedTemplateArtifacts,
    QbitTipTemplateSnapshot,
    qbit_template_fingerprint,
)


PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS = 0.05
PRISM_TIP_REFRESH_FAILURE_HOLDOFF_JITTER_FRACTION = 0.25
PRISM_TIP_REFRESH_SECONDS_BUCKETS = (
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
PRISM_TIP_REFRESH_BUILD_PHASES = (
    "ledger_snapshot",
    "payout_state_derivation",
    "ctv_manifest_construction",
    "coinbase_bundle_construction",
    "signing_verification",
    "serialization_copy",
    "singleflight_wait",
)
PRISM_TIP_REFRESH_RESULTS = ("sent", "skipped", "disconnected", "failed")
PRISM_TIP_REFRESH_CANCELLATION_STAGES = (
    "executor_queue",
    "client_lock",
    "payout_gate",
)
PRISM_TIP_REFRESH_TRIGGER_REASONS = (
    "blockpoll",
    "blockwait",
    "payout",
    "post_accept",
    "readiness",
    "retained_collection",
    "template",
)
PRISM_TIP_REFRESH_TRIGGER_PENDING_CAPACITY = 1

_UNSET = object()


class RefreshActivityPort(Protocol):
    def note_activity(self, observed_monotonic: float | None = None) -> None: ...

    def finish(self) -> None: ...


class DeliveryAdmissionPort(Protocol):
    def __bool__(self) -> bool: ...


class PayoutDeliveryGatePort(Protocol):
    def delivery_cancelable(
        self,
        cancelled: Callable[[], bool],
        *,
        generation: int,
        priority: bool = False,
    ) -> Any: ...


class PayoutStatePort(Protocol):
    @property
    def delivery_gate(self) -> PayoutDeliveryGatePort: ...

    def snapshot(self) -> PayoutStateSnapshot: ...

    def reserve_source_for_tip_change(
        self,
        tip_hash: str,
        *,
        cause: str,
        invalidated_monotonic: float,
    ) -> int: ...


class JobBundlePort(Protocol):
    def begin_priority_preparation(
        self,
        requested_monotonic: float | None = None,
    ) -> tuple[int, float]: ...

    def finish_priority_preparation(self, token: int) -> None: ...

    def ready_latched(self) -> bool: ...

    def clear_prepared_ready(self) -> None: ...

    def record_failure(self) -> None: ...

    def pool_readiness_latched(self) -> bool: ...

    def shared_job_bundle(
        self,
        artifacts: CachedTemplateArtifacts,
        worker: object | None = None,
        *,
        mode: str | None = None,
        retry_superseded: bool = True,
        publication_critical: bool = False,
        request_source: str = "routine",
        priority_requested_monotonic: float | None = None,
    ) -> CachedJobBundle: ...

    def set_preparation_pending(self, pending: bool) -> None: ...

    def set_prepared_ready(
        self,
        snapshot: QbitTipTemplateSnapshot | None,
        bundle: CachedJobBundle | None,
    ) -> None: ...


@dataclass(frozen=True)
class RefreshClientTarget:
    client: object = field(repr=False)
    expected_active_job: object | None = field(default=None, repr=False)


class TipRefreshDeliveryPort(Protocol):
    """Temporary R1-to-S2 boundary; S2 replaces this adapter outright."""

    def eligible_clients(self) -> tuple[object, ...]: ...

    def client_can_receive_jobs(self, client: object) -> bool: ...

    def client_needs_refresh(
        self,
        client: object,
        snapshot: QbitTipTemplateSnapshot,
    ) -> bool: ...

    def active_job(self, client: object) -> object | None: ...

    def connection_id(self, client: object) -> int: ...

    def delivery_priority(
        self,
        client: object,
        snapshot: QbitTipTemplateSnapshot,
        expected_active_job: object | None,
    ) -> int: ...

    def submit_task(
        self,
        executor: object,
        fn: Callable[..., RefreshResult],
        *args: object,
        priority: int,
    ) -> Future[RefreshResult]: ...

    def send_prepared_job(
        self,
        client: object,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        validation_token: TipRefreshValidationToken,
        expected_connection_id: int,
        expected_active_job: object | None,
        cancel_event: FanoutCancellation,
        submitted_monotonic: float,
    ) -> RefreshResult: ...

    def disconnect(self, client: object) -> None: ...

    def log_identity(self, client: object) -> str: ...

    def select_targets(
        self,
        snapshot: QbitTipTemplateSnapshot,
        *,
        refresh_all: bool,
    ) -> tuple[RefreshClientTarget, ...]: ...

    def merge_poll_start_targets(
        self,
        targets: tuple[RefreshClientTarget, ...],
        poll_start_clients: tuple[object, ...],
        snapshot: QbitTipTemplateSnapshot,
        *,
        refresh_all: bool,
    ) -> tuple[RefreshClientTarget, ...]: ...

    def revalidate_targets(
        self,
        targets: tuple[RefreshClientTarget, ...],
        snapshot: QbitTipTemplateSnapshot,
    ) -> tuple[tuple[RefreshClientTarget, ...], tuple[str, ...]]: ...

    def deliver_collection(
        self,
        client: object,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
    ) -> RefreshResult: ...

    def take_post_accept_refresh(
        self,
        client: object,
    ) -> tuple[int, str] | None: ...


@dataclass(frozen=True)
class TipRefreshConfig:
    blockpoll_seconds: float
    blockwait_timeout_seconds: float
    failure_holdoff_seconds: float
    max_workers: int
    submit_tip_max_age_seconds: float
    failure_exit_seconds: float
    watchdog_timeout_seconds: float
    payout_reconcile_supersession_retries: int = (
        DEFAULT_PRISM_PAYOUT_RECONCILE_SUPERSESSION_RETRIES
    )


@dataclass(frozen=True)
class PublishedTipSnapshot:
    """Atomic share-validation authority and its exact template artifact."""

    first_seen: tuple[str, float | None] | None
    parent: tuple[str, str] | None
    observation_sequence: int
    observed_monotonic: float | None
    template: QbitTipTemplateSnapshot | None = field(repr=False)

    @property
    def tip_hash(self) -> str | None:
        return None if self.first_seen is None else self.first_seen[0]


@dataclass(frozen=True)
class TipRefreshStateSnapshot:
    published: PublishedTipSnapshot
    latest_detected_tip: tuple[str, int] | None
    divergence_started_monotonic: float | None
    observation_sequence: int
    pending: bool
    pending_counter: int
    pending_token: int | None
    retry_requested: bool
    last_successful_refresh_monotonic: float | None
    failure_started_monotonic: float | None
    refresh_job_count: int
    post_accept_refresh_failure_count: int


@dataclass(frozen=True)
class TipRefreshTrigger:
    """Immutable refresh authority submitted to the latest-wins scheduler."""

    admission_sequence: int
    observation_sequence: int
    tip_hash: str | None
    template_fingerprint: str | None
    template_generation: int | None
    payout_state_generation: int
    ready_required: bool
    reasons: tuple[str, ...]
    submitted_monotonic: float
    pending_signal_token: int | None
    snapshot: QbitTipTemplateSnapshot | None = field(default=None, repr=False)
    poll_start_clients: tuple[object, ...] = field(default=(), repr=False)
    initial_targets: tuple[RefreshClientTarget, ...] = field(default=(), repr=False)
    snapshot_changed: bool = False
    post_accept_block: tuple[int, str] | None = None
    post_accept_admission_sequence: int | None = None
    fresh_capture_required: bool = False


@dataclass(frozen=True)
class TipObservationAdmission:
    """Result of admitting one live tip observation to the scheduler."""

    accepted: bool
    refresh_needed: bool
    observation_sequence: int
    completion: Future[int] | None = field(default=None, repr=False)


@dataclass(frozen=True)
class TipRefreshSchedulerSnapshot:
    admission_open: bool
    worker_alive: bool
    active: TipRefreshTrigger | None
    pending: TipRefreshTrigger | None
    pending_capacity: int


@dataclass
class _ScheduledTipRefresh:
    trigger: TipRefreshTrigger
    completion: Future[int]
    reporting_trigger: TipRefreshTrigger | None = None


@dataclass(frozen=True)
class RetainedCollectionRefresh:
    snapshot: QbitTipTemplateSnapshot
    observation_sequence: int
    payout_state_generation: int


@dataclass(frozen=True)
class RefreshResult:
    result: str
    delivered_monotonic: float | None = None


@dataclass(frozen=True, eq=False)
class TipRefreshValidationToken:
    """Immutable proof that one prepared refresh passed its expensive guard."""

    tip_hash: str
    template_fingerprint: str
    template_generation: int
    payout_state_generation: int
    observation_sequence: int
    build_key: JobBuildKey
    snapshot: QbitTipTemplateSnapshot = field(repr=False)


@dataclass(frozen=True)
class ActiveRefreshSnapshot:
    token: TipRefreshValidationToken
    cancelling: bool


class FanoutCancellation:
    """Close fanout admission, then drain already-admitted deliveries."""

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._cancelling = False
        self._active_deliveries = 0

    def is_set(self) -> bool:
        with self._condition:
            return self._cancelling

    def begin_delivery(self) -> bool:
        with self._condition:
            if self._cancelling:
                return False
            self._active_deliveries += 1
            return True

    def end_delivery(self) -> None:
        with self._condition:
            if self._active_deliveries <= 0:
                raise RuntimeError("fanout delivery gate released without admission")
            self._active_deliveries -= 1
            if self._active_deliveries == 0:
                self._condition.notify_all()

    def cancel(self) -> None:
        with self._condition:
            self._cancelling = True

    def set(self) -> None:
        self.cancel()
        with self._condition:
            while self._active_deliveries:
                self._condition.wait()


@dataclass(frozen=True)
class TipRefreshPorts:
    """Narrow cross-domain operations used by tip refresh ownership."""

    rpc_call: Callable[[str, list[object] | None], object]
    rpc_call_with_timeout: Callable[[str, list[object] | None, float], object]
    payout_state: Callable[[], PayoutStatePort]
    job_bundles: Callable[[], JobBundlePort]
    delivery: TipRefreshDeliveryPort
    mark_progress_pending: Callable[[float | None], None]
    observe_progress_tip_poll: Callable[[QbitTipTemplateSnapshot], None]
    publish_progress_work: Callable[[QbitTipTemplateSnapshot, int], None]
    start_progress_refresh: Callable[[], RefreshActivityPort]
    cancel_obsolete_bundle_builds: Callable[[str | None, int | None], None]
    cancel_obsolete_job_builds: Callable[[str], None]
    prune_evicted_jobs: Callable[[float | None, bool], None]
    delivery_queue_limit: Callable[[], int]
    stop_requested: Callable[[], bool]
    heartbeat: Callable[[str], None]
    remove_heartbeat: Callable[[str], None]
    chain_view_untrusted: Callable[[], bool]
    ensure_reorg_current: Callable[[str], bool]
    observe_job_build_elapsed: Callable[[float, Mapping[str, float]], None]
    fetch_snapshot: Callable[[], QbitTipTemplateSnapshot]
    ensure_reorg_tip: Callable[[str], bool]
    wait_for_execution_permit: Callable[[float], bool]
    wait_for_stop: Callable[[float], bool]
    hard_exit: Callable[[int], None]
    fetch_snapshot_for_tip: (
        Callable[[str], QbitTipTemplateSnapshot] | None
    ) = None


class TipRefreshService:
    """Own tip detection/publication state, pending work, and refresh runtime."""

    def __init__(
        self,
        config: TipRefreshConfig,
        ports: TipRefreshPorts,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        state_lock: threading.RLock | None = None,
    ) -> None:
        self.config = config
        self._ports = ports
        self._monotonic = monotonic
        # Observation state and its cross-domain consequences must retain one
        # total order. The RLock lets us reject accidental synchronous port
        # reentry explicitly instead of deadlocking; production ports never
        # reenter tip observation.
        self._observation_effects_lock = threading.RLock()
        self._observation_effects_active = False
        # The coordinator injects the S1 registry RLock so R1 publication
        # authority and delivery proof share one atomic boundary. Standalone
        # R1 users retain a private lock.
        self._state_lock = state_lock or threading.RLock()
        self._publication_lock = threading.Lock()
        self._refresh_lock = threading.Lock()
        self._executor_lock = threading.Lock()
        self._metrics_lock = threading.Lock()
        self._scheduler_condition = threading.Condition(threading.Lock())
        self._scheduler_admission_sequence = 0
        self._scheduler_admission_open = True
        self._scheduler_active: _ScheduledTipRefresh | None = None
        self._scheduler_pending: _ScheduledTipRefresh | None = None
        self._scheduler_worker: threading.Thread | None = None
        self._scheduler_cancel_active = False
        self._trigger_capture_local = threading.local()
        self._executor: _BoundedPriorityExecutor | None = None
        self._executor_shutdown = False
        self._pending_event = threading.Event()
        self._retry_event = threading.Event()
        self._retry_counter = 0
        self._retry_consumed = 0
        self._failure_holdoff_until: float | None = None
        self._failure_tip: str | None = None
        self._pending_counter = 0
        self._pending_token: int | None = None
        self._active_refresh: tuple[
            TipRefreshValidationToken,
            FanoutCancellation,
        ] | None = None
        self._observation_sequence = 0
        self._latest_detected_tip: tuple[str, int] | None = None
        self._published = PublishedTipSnapshot(None, None, 0, None, None)
        self._divergence_started_monotonic: float | None = None
        self._retained_collection_refresh: RetainedCollectionRefresh | None = None
        self._last_successful_refresh_monotonic: float | None = None
        self._failure_started_monotonic: float | None = None
        self._refresh_job_count = 0
        self._post_accept_refresh_failure_count = 0
        self._histograms = {
            name: {
                "buckets": {bucket: 0 for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS},
                "sum": 0.0,
                "count": 0,
            }
            for name in ("refresh", "bundle_build", "first_delivery", "last_delivery")
        }
        self._phase_histograms = {
            phase: {
                "buckets": {bucket: 0 for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS},
                "sum": 0.0,
                "count": 0,
            }
            for phase in PRISM_TIP_REFRESH_BUILD_PHASES
        }
        self._client_counts = {result: 0 for result in PRISM_TIP_REFRESH_RESULTS}
        self._cancellation_counts = {
            stage: 0 for stage in PRISM_TIP_REFRESH_CANCELLATION_STAGES
        }
        self._inflight = 0
        self._build_inflight = 0
        self._build_queue_depth = 0
        self._singleflight_hits = 0
        self._superseded_results = 0
        self._worker_failures = 0
        self._worker_restarts = 0
        self._ipc_bytes = {"input": 0, "output": 0}
        self._trigger_coalesces = 0
        self._trigger_supersessions = 0
        self._trigger_latency = {
            "buckets": {bucket: 0 for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS},
            "sum": 0.0,
            "count": 0,
        }

    def reconfigure_for_test(
        self,
        *,
        blockpoll_seconds: float | None = None,
        blockwait_timeout_seconds: float | None = None,
        failure_holdoff_seconds: float | None = None,
        max_workers: int | None = None,
        submit_tip_max_age_seconds: float | None = None,
        failure_exit_seconds: float | None = None,
    ) -> None:
        """Explicit fixture hook; production configuration stays immutable."""
        updates: dict[str, object] = {}
        if blockpoll_seconds is not None:
            updates["blockpoll_seconds"] = float(blockpoll_seconds)
        if blockwait_timeout_seconds is not None:
            updates["blockwait_timeout_seconds"] = float(blockwait_timeout_seconds)
        if failure_holdoff_seconds is not None:
            updates["failure_holdoff_seconds"] = float(failure_holdoff_seconds)
        if max_workers is not None:
            updates["max_workers"] = int(max_workers)
        if submit_tip_max_age_seconds is not None:
            updates["submit_tip_max_age_seconds"] = float(
                submit_tip_max_age_seconds
            )
        if failure_exit_seconds is not None:
            updates["failure_exit_seconds"] = float(failure_exit_seconds)
        self.config = dataclass_replace(self.config, **updates)

    def reconfigure_ports_for_test(
        self,
        *,
        rpc_call: Callable[[str, list[object] | None], object] | None = None,
        rpc_call_with_timeout: (
            Callable[[str, list[object] | None, float], object] | None
        ) = None,
        fetch_snapshot: Callable[[], QbitTipTemplateSnapshot] | None = None,
        heartbeat: Callable[[str], None] | None = None,
        remove_heartbeat: Callable[[str], None] | None = None,
        wait_for_stop: Callable[[float], bool] | None = None,
        wait_for_execution_permit: Callable[[float], bool] | None = None,
        stop_requested: Callable[[], bool] | None = None,
    ) -> None:
        """Replace explicit runtime seams in deterministic fixtures only."""
        updates: dict[str, object] = {}
        if rpc_call is not None:
            updates["rpc_call"] = rpc_call
        if rpc_call_with_timeout is not None:
            updates["rpc_call_with_timeout"] = rpc_call_with_timeout
        if fetch_snapshot is not None:
            updates["fetch_snapshot"] = fetch_snapshot
            updates["fetch_snapshot_for_tip"] = lambda _tip: fetch_snapshot()
        if heartbeat is not None:
            updates["heartbeat"] = heartbeat
        if remove_heartbeat is not None:
            updates["remove_heartbeat"] = remove_heartbeat
        if wait_for_stop is not None:
            updates["wait_for_stop"] = wait_for_stop
        if wait_for_execution_permit is not None:
            updates["wait_for_execution_permit"] = wait_for_execution_permit
        if stop_requested is not None:
            updates["stop_requested"] = stop_requested
        self._ports = dataclass_replace(self._ports, **updates)

    @staticmethod
    def _trigger_axis_key(trigger: TipRefreshTrigger) -> tuple[int, int]:
        return (
            trigger.observation_sequence,
            -1 if trigger.template_generation is None else trigger.template_generation,
        )

    @staticmethod
    def _trigger_authority_is_older(
        candidate: TipRefreshTrigger,
        current: TipRefreshTrigger,
    ) -> bool:
        """Compare only authority axes actually carried by the candidate."""
        if candidate.observation_sequence != current.observation_sequence:
            return candidate.observation_sequence < current.observation_sequence
        return bool(
            candidate.template_generation is not None
            and current.template_generation is not None
            and candidate.template_generation < current.template_generation
        )

    @classmethod
    def _trigger_requirement_supersedes(
        cls,
        current: TipRefreshTrigger,
        candidate: TipRefreshTrigger,
    ) -> bool:
        return bool(
            cls._trigger_axis_key(candidate) > cls._trigger_axis_key(current)
            or candidate.payout_state_generation > current.payout_state_generation
            or (candidate.ready_required and not current.ready_required)
            or (
                candidate.fresh_capture_required
                and not current.fresh_capture_required
            )
            or (
                candidate.pending_signal_token is not None
                and not cls._trigger_authority_is_older(candidate, current)
                and (
                    current.pending_signal_token is None
                    or candidate.pending_signal_token > current.pending_signal_token
                )
            )
        )

    @classmethod
    def _merge_triggers(
        cls,
        current: TipRefreshTrigger,
        candidate: TipRefreshTrigger,
    ) -> TipRefreshTrigger:
        current_axis = cls._trigger_axis_key(current)
        candidate_axis = cls._trigger_axis_key(candidate)
        latest = current
        if candidate_axis > current_axis or (
            candidate_axis == current_axis
            and candidate.snapshot is not None
            and (
                current.snapshot is None
                or (
                    candidate.pending_signal_token is not None
                    and (
                        current.pending_signal_token is None
                        or candidate.pending_signal_token
                        > current.pending_signal_token
                    )
                )
            )
        ):
            latest = candidate
        current_post_accept_sequence = (
            -1
            if current.post_accept_admission_sequence is None
            else current.post_accept_admission_sequence
        )
        candidate_post_accept_sequence = (
            -1
            if candidate.post_accept_admission_sequence is None
            else candidate.post_accept_admission_sequence
        )
        candidate_owns_post_accept = bool(
            candidate.post_accept_block is not None
            and candidate_post_accept_sequence >= current_post_accept_sequence
        )
        newest_post_accept = (
            candidate.post_accept_block
            if candidate_owns_post_accept
            else current.post_accept_block
        )
        newest_post_accept_sequence = (
            candidate.post_accept_admission_sequence
            if candidate_owns_post_accept
            else current.post_accept_admission_sequence
        )
        merged = dataclass_replace(
            latest,
            payout_state_generation=max(
                current.payout_state_generation,
                candidate.payout_state_generation,
            ),
            ready_required=current.ready_required or candidate.ready_required,
            reasons=tuple(
                reason
                for reason in PRISM_TIP_REFRESH_TRIGGER_REASONS
                if reason in current.reasons or reason in candidate.reasons
            ),
            submitted_monotonic=min(
                current.submitted_monotonic,
                candidate.submitted_monotonic,
            ),
            pending_signal_token=(
                candidate.pending_signal_token
                if candidate.pending_signal_token is not None
                and not cls._trigger_authority_is_older(candidate, current)
                and (
                    current.pending_signal_token is None
                    or candidate.pending_signal_token > current.pending_signal_token
                )
                else current.pending_signal_token
            ),
            post_accept_block=newest_post_accept,
            post_accept_admission_sequence=newest_post_accept_sequence,
            fresh_capture_required=(
                current.fresh_capture_required
                or candidate.fresh_capture_required
            ),
        )
        if not merged.fresh_capture_required:
            return merged
        return dataclass_replace(
            merged,
            template_fingerprint=None,
            template_generation=None,
            snapshot=None,
            poll_start_clients=(),
            initial_targets=(),
            snapshot_changed=False,
        )

    @staticmethod
    def _trigger_cancels_active(
        current: TipRefreshTrigger,
        candidate: TipRefreshTrigger,
    ) -> bool:
        return bool(
            (
                current.tip_hash is not None
                and candidate.tip_hash is not None
                and current.tip_hash != candidate.tip_hash
            )
            or candidate.payout_state_generation > current.payout_state_generation
            or (candidate.ready_required and not current.ready_required)
        )

    def _new_trigger(
        self,
        *,
        observation_sequence: int,
        tip_hash: str | None,
        payout_state_generation: int,
        ready_required: bool,
        reasons: tuple[str, ...],
        pending_signal_token: int | None,
        snapshot: QbitTipTemplateSnapshot | None = None,
        template_fingerprint: str | None = None,
        template_generation: int | None = None,
        poll_start_clients: tuple[object, ...] = (),
        initial_targets: tuple[RefreshClientTarget, ...] = (),
        snapshot_changed: bool = False,
        post_accept_block: tuple[int, str] | None = None,
        fresh_capture_required: bool = False,
    ) -> TipRefreshTrigger:
        if not reasons or any(
            reason not in PRISM_TIP_REFRESH_TRIGGER_REASONS for reason in reasons
        ):
            raise ValueError("tip refresh trigger has an unknown reason")
        with self._scheduler_condition:
            self._scheduler_admission_sequence += 1
            admission_sequence = self._scheduler_admission_sequence
        return TipRefreshTrigger(
            admission_sequence=admission_sequence,
            observation_sequence=observation_sequence,
            tip_hash=tip_hash,
            template_fingerprint=(
                template_fingerprint
                if snapshot is None
                else snapshot.template_fingerprint
            ),
            template_generation=(
                template_generation if snapshot is None else snapshot.template_generation
            ),
            payout_state_generation=int(payout_state_generation),
            ready_required=bool(ready_required),
            reasons=tuple(
                reason for reason in PRISM_TIP_REFRESH_TRIGGER_REASONS if reason in reasons
            ),
            submitted_monotonic=self._monotonic(),
            pending_signal_token=pending_signal_token,
            snapshot=snapshot,
            poll_start_clients=poll_start_clients,
            initial_targets=initial_targets,
            snapshot_changed=snapshot_changed,
            post_accept_block=post_accept_block,
            post_accept_admission_sequence=(
                admission_sequence if post_accept_block is not None else None
            ),
            fresh_capture_required=fresh_capture_required,
        )

    def _ensure_scheduler_worker_locked(self) -> None:
        worker = self._scheduler_worker
        if worker is not None and worker.is_alive():
            return
        worker = threading.Thread(
            target=self._scheduler_loop,
            name="prism-tip-refresh-scheduler",
            daemon=True,
        )
        self._scheduler_worker = worker
        worker.start()

    def submit_trigger(self, trigger: TipRefreshTrigger) -> Future[int]:
        """Admit one immutable observation into active plus one pending slot."""
        cancel_active = False
        coalesces = 0
        supersessions = 0
        with self._scheduler_condition:
            if not self._scheduler_admission_open:
                raise ShutdownInProgress("tip refresh trigger admission is closed")
            active = self._scheduler_active
            pending = self._scheduler_pending
            force_fresh_followup = bool(
                active is not None
                and trigger.fresh_capture_required
            )
            if (
                active is not None
                and not force_fresh_followup
                and not self._trigger_requirement_supersedes(
                    active.trigger,
                    trigger,
                )
            ):
                active.reporting_trigger = self._merge_triggers(
                    active.reporting_trigger or active.trigger,
                    trigger,
                )
                completion = active.completion
                coalesces = 1
            else:
                pending_advanced = pending is None
                if pending is None:
                    pending_trigger = (
                        trigger
                        if active is None
                        else dataclass_replace(
                            self._merge_triggers(active.trigger, trigger),
                            reasons=trigger.reasons,
                            submitted_monotonic=trigger.submitted_monotonic,
                        )
                    )
                    pending = _ScheduledTipRefresh(
                        pending_trigger,
                        Future(),
                        pending_trigger,
                    )
                    self._scheduler_pending = pending
                else:
                    if self._trigger_requirement_supersedes(
                        pending.trigger,
                        trigger,
                    ):
                        supersessions = 1
                        pending_advanced = True
                    pending.trigger = self._merge_triggers(pending.trigger, trigger)
                    pending.reporting_trigger = self._merge_triggers(
                        pending.reporting_trigger or pending.trigger,
                        trigger,
                    )
                    coalesces = 1
                if (
                    pending_advanced
                    and active is not None
                    and self._trigger_cancels_active(
                        active.trigger,
                        pending.trigger,
                    )
                ):
                    cancel_active = True
                    supersessions = 1
                    active_reporting = active.reporting_trigger or active.trigger
                    pending_reporting = (
                        pending.reporting_trigger or pending.trigger
                    )
                    active_post_accept_sequence = (
                        -1
                        if active_reporting.post_accept_admission_sequence is None
                        else active_reporting.post_accept_admission_sequence
                    )
                    pending_post_accept_sequence = (
                        -1
                        if pending_reporting.post_accept_admission_sequence is None
                        else pending_reporting.post_accept_admission_sequence
                    )
                    if (
                        active_reporting.post_accept_block is not None
                        and pending_reporting.post_accept_block is not None
                        and pending_post_accept_sequence
                        >= active_post_accept_sequence
                    ):
                        # Expected supersession is not a post-accept failure
                        # once the canceled pass's reporting context belongs
                        # to an equal-or-newer pending successor. Same-tip
                        # draining work is not canceled and retains ownership.
                        active.reporting_trigger = dataclass_replace(
                            active_reporting,
                            post_accept_block=None,
                            post_accept_admission_sequence=None,
                        )
                self._ensure_scheduler_worker_locked()
                self._scheduler_condition.notify()
                completion = pending.completion
        if coalesces or supersessions:
            with self._metrics_lock:
                self._trigger_coalesces += coalesces
                self._trigger_supersessions += supersessions
        if cancel_active:
            self._cancel_active_fanout()
        return completion

    def scheduler_snapshot(self) -> TipRefreshSchedulerSnapshot:
        with self._scheduler_condition:
            worker = self._scheduler_worker
            return TipRefreshSchedulerSnapshot(
                admission_open=self._scheduler_admission_open,
                worker_alive=bool(worker is not None and worker.is_alive()),
                active=(
                    None
                    if self._scheduler_active is None
                    else self._scheduler_active.trigger
                ),
                pending=(
                    None
                    if self._scheduler_pending is None
                    else self._scheduler_pending.trigger
                ),
                pending_capacity=PRISM_TIP_REFRESH_TRIGGER_PENDING_CAPACITY,
            )

    def wait_for_scheduler_idle_for_test(self, timeout_seconds: float = 1.0) -> bool:
        """Wait for active and pending scheduler slots in deterministic tests."""
        deadline = time.monotonic() + max(0.0, timeout_seconds)
        with self._scheduler_condition:
            while (
                self._scheduler_active is not None
                or self._scheduler_pending is not None
            ):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._scheduler_condition.wait(remaining)
            return True

    def _cancel_active_fanout(self) -> None:
        with self._state_lock:
            active = self._active_refresh
        if active is not None:
            active[1].cancel()

    def _scheduler_owns_current_thread(self) -> bool:
        with self._scheduler_condition:
            return bool(
                self._scheduler_worker is threading.current_thread()
                and self._scheduler_active is not None
            )

    def _scheduler_trigger_current(self, trigger: TipRefreshTrigger) -> bool:
        with self._scheduler_condition:
            active = self._scheduler_active
            pending = self._scheduler_pending
            return bool(
                active is not None
                and active.trigger is trigger
                and not self._scheduler_cancel_active
                and (
                    pending is None
                    or not self._trigger_cancels_active(
                        trigger,
                        pending.trigger,
                    )
                )
            )

    def _scheduler_has_followup(self, trigger: TipRefreshTrigger) -> bool:
        with self._scheduler_condition:
            active = self._scheduler_active
            pending = self._scheduler_pending
            return bool(
                active is not None
                and active.trigger is trigger
                and pending is not None
                and not self._trigger_cancels_active(trigger, pending.trigger)
            )

    def _replace_active_trigger(
        self,
        current: TipRefreshTrigger,
        replacement: TipRefreshTrigger,
    ) -> TipRefreshTrigger:
        with self._scheduler_condition:
            active = self._scheduler_active
            if active is not None and active.trigger is current:
                active.trigger = replacement
                pending = self._scheduler_pending
                if (
                    pending is not None
                    and pending.trigger.fresh_capture_required
                ):
                    pending_trigger = pending.trigger
                    pending_reporting = (
                        pending.reporting_trigger or pending_trigger
                    )
                    pending.trigger = self._merge_triggers(
                        pending_trigger,
                        replacement,
                    )
                    pending.reporting_trigger = self._merge_triggers(
                        pending_reporting,
                        replacement,
                    )
        return replacement

    def _raise_if_scheduler_superseded(self, trigger: TipRefreshTrigger) -> None:
        if not self._scheduler_trigger_current(trigger):
            raise TemplateRefreshSuperseded(
                "refresh trigger was superseded by newer queued authority"
            )

    def _observe_trigger_latency(self, elapsed_seconds: float) -> None:
        elapsed_seconds = max(0.0, elapsed_seconds)
        with self._metrics_lock:
            self._trigger_latency["count"] = int(self._trigger_latency["count"]) + 1
            self._trigger_latency["sum"] = (
                float(self._trigger_latency["sum"]) + elapsed_seconds
            )
            buckets = self._trigger_latency["buckets"]
            assert isinstance(buckets, dict)
            for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS:
                if elapsed_seconds <= bucket:
                    buckets[bucket] = int(buckets.get(bucket, 0)) + 1

    def _scheduler_loop(self) -> None:
        while True:
            with self._scheduler_condition:
                if self._scheduler_pending is None:
                    scheduled = None
                else:
                    scheduled = self._scheduler_pending
                    self._scheduler_pending = None
                    self._scheduler_active = scheduled
                    self._scheduler_cancel_active = False
                    trigger = scheduled.trigger
            if scheduled is None:
                self._ports.remove_heartbeat("tip_refresh_scheduler")
                with self._scheduler_condition:
                    # Admission may have arrived while the external heartbeat
                    # port ran. This worker remains published until that port
                    # returns, so it can consume the new pending slot without
                    # racing a second scheduler thread.
                    if self._scheduler_pending is not None:
                        continue
                    if self._scheduler_worker is threading.current_thread():
                        self._scheduler_worker = None
                    self._scheduler_condition.notify_all()
                return
            self._observe_trigger_latency(
                self._monotonic() - trigger.submitted_monotonic
            )
            try:
                result = self._execute_refresh_trigger(trigger)
            except BaseException as exc:
                if not scheduled.completion.done():
                    scheduled.completion.set_exception(exc)
                self._handle_scheduled_failure(
                    scheduled.reporting_trigger or trigger,
                    exc,
                )
            else:
                if not scheduled.completion.done():
                    scheduled.completion.set_result(result)
                self._handle_scheduled_success(
                    scheduled.reporting_trigger or trigger,
                    result,
                )
            finally:
                with self._scheduler_condition:
                    if self._scheduler_active is scheduled:
                        self._scheduler_active = None
                        self._scheduler_cancel_active = False
                    self._scheduler_condition.notify_all()

    def _handle_scheduled_failure(
        self,
        trigger: TipRefreshTrigger,
        exc: BaseException,
    ) -> None:
        if isinstance(exc, ShutdownInProgress) or self._ports.stop_requested():
            return
        if trigger.post_accept_block is not None:
            self.schedule_retry()
            if isinstance(
                exc,
                (TemplateRefreshSuperseded, PayoutStatePublicationBlocked),
            ):
                return
            block_height, block_hash = trigger.post_accept_block
            with self._state_lock:
                self._post_accept_refresh_failure_count += 1
            print(
                "prism coordinator: post-accept clean job refresh failed after "
                "direct PRISM block "
                f"height={block_height} hash={block_hash}",
                flush=True,
            )
            traceback.print_exception(exc)
            return
        if isinstance(exc, (TemplateRefreshSuperseded, PayoutStatePublicationBlocked)):
            print(
                f"prism coordinator: tip/template refresh superseded; retrying: {exc}",
                flush=True,
            )
            return
        print("prism coordinator: qbit tip/template poll failed", flush=True)
        traceback.print_exception(exc)

    @staticmethod
    def _handle_scheduled_success(trigger: TipRefreshTrigger, refreshed: int) -> None:
        if trigger.post_accept_block is not None:
            block_height, block_hash = trigger.post_accept_block
            if refreshed:
                print(
                    "prism coordinator: refreshed "
                    f"{refreshed} client job(s) after direct PRISM block "
                    f"height={block_height} hash={block_hash}",
                    flush=True,
                )
        elif refreshed:
            print(
                f"prism coordinator: refreshed {refreshed} client job(s) "
                "after qbit tip/template change",
                flush=True,
            )

    def snapshot(self) -> TipRefreshStateSnapshot:
        with self._state_lock:
            return TipRefreshStateSnapshot(
                published=self._published,
                latest_detected_tip=self._latest_detected_tip,
                divergence_started_monotonic=self._divergence_started_monotonic,
                observation_sequence=self._observation_sequence,
                pending=self._pending_event.is_set(),
                pending_counter=self._pending_counter,
                pending_token=self._pending_token,
                retry_requested=self._retry_event.is_set(),
                last_successful_refresh_monotonic=(
                    self._last_successful_refresh_monotonic
                ),
                failure_started_monotonic=self._failure_started_monotonic,
                refresh_job_count=self._refresh_job_count,
                post_accept_refresh_failure_count=(
                    self._post_accept_refresh_failure_count
                ),
            )

    def seed_state_for_test(
        self,
        *,
        published: PublishedTipSnapshot | object = _UNSET,
        latest_detected_tip: tuple[str, int] | None | object = _UNSET,
        divergence_started_monotonic: float | None | object = _UNSET,
        observation_sequence: int | object = _UNSET,
        last_successful_refresh_monotonic: float | None | object = _UNSET,
        failure_started_monotonic: float | None | object = _UNSET,
        refresh_job_count: int | object = _UNSET,
        post_accept_refresh_failure_count: int | object = _UNSET,
    ) -> None:
        """Seed explicitly named state for focused deterministic fixtures."""
        with self._state_lock:
            if published is not _UNSET:
                self._published = published  # type: ignore[assignment]
            if latest_detected_tip is not _UNSET:
                self._latest_detected_tip = latest_detected_tip  # type: ignore[assignment]
            if divergence_started_monotonic is not _UNSET:
                self._divergence_started_monotonic = divergence_started_monotonic  # type: ignore[assignment]
            if observation_sequence is not _UNSET:
                self._observation_sequence = int(observation_sequence)
            if last_successful_refresh_monotonic is not _UNSET:
                self._last_successful_refresh_monotonic = last_successful_refresh_monotonic  # type: ignore[assignment]
            if failure_started_monotonic is not _UNSET:
                self._failure_started_monotonic = failure_started_monotonic  # type: ignore[assignment]
            if refresh_job_count is not _UNSET:
                self._refresh_job_count = int(refresh_job_count)
            if post_accept_refresh_failure_count is not _UNSET:
                self._post_accept_refresh_failure_count = int(
                    post_accept_refresh_failure_count
                )

    def seed_published_for_test(
        self,
        *,
        first_seen: tuple[str, float | None] | None | object = _UNSET,
        parent: tuple[str, str] | None | object = _UNSET,
        observation_sequence: int | object = _UNSET,
        observed_monotonic: float | None | object = _UNSET,
        template: QbitTipTemplateSnapshot | None | object = _UNSET,
    ) -> None:
        with self._state_lock:
            current = self._published
            self._published = PublishedTipSnapshot(
                current.first_seen if first_seen is _UNSET else first_seen,  # type: ignore[arg-type]
                current.parent if parent is _UNSET else parent,  # type: ignore[arg-type]
                (
                    current.observation_sequence
                    if observation_sequence is _UNSET
                    else int(observation_sequence)
                ),
                (
                    current.observed_monotonic
                    if observed_monotonic is _UNSET
                    else observed_monotonic
                ),  # type: ignore[arg-type]
                current.template if template is _UNSET else template,  # type: ignore[arg-type]
            )

    def replace_executor_for_test(self, executor: object) -> None:
        with self._executor_lock:
            self._executor = executor  # type: ignore[assignment]

    def replace_refresh_lock_for_test(self, lock: threading.Lock) -> None:
        self._refresh_lock = lock

    def clear_retry_for_test(self) -> None:
        """Clear a scheduled retry in deterministic fixtures only."""
        with self._state_lock:
            self._retry_consumed = self._retry_counter
            self._retry_event.clear()

    @contextmanager
    def suppress_trigger_callbacks_for_test(self) -> Any:
        """Suppress synchronous producer callbacks during manual test setup."""
        previous = bool(getattr(self._trigger_capture_local, "active", False))
        self._trigger_capture_local.active = True
        try:
            yield
        finally:
            self._trigger_capture_local.active = previous

    def retained_collection_refresh_snapshot(
        self,
    ) -> RetainedCollectionRefresh | None:
        with self._state_lock:
            return self._retained_collection_refresh

    def active_refresh_snapshot(self) -> ActiveRefreshSnapshot | None:
        with self._state_lock:
            active = self._active_refresh
        if active is None:
            return None
        return ActiveRefreshSnapshot(active[0], active[1].is_set())

    def seed_active_refresh_for_test(
        self,
        token: TipRefreshValidationToken,
        cancellation: FanoutCancellation,
    ) -> None:
        with self._state_lock:
            self._active_refresh = (token, cancellation)

    def executor_stats(self) -> tuple[int, int]:
        with self._executor_lock:
            executor = self._executor
        return (0, 0) if executor is None else executor.stats()

    def _submit_requirement_trigger(
        self,
        reason: str,
        *,
        payout_state_generation: int | None = None,
        ready_required: bool | None = None,
        pending_signal_token: int | None = None,
        tip_hash: str | None = None,
        template_fingerprint: str | None = None,
        template_generation: int | None = None,
        post_accept_block: tuple[int, str] | None = None,
        observation_sequence: int | None = None,
        fresh_capture_required: bool = False,
    ) -> Future[int]:
        authority_tip, authority_sequence = self._current_observation_authority()
        if tip_hash is None:
            tip_hash = authority_tip
        if observation_sequence is None:
            observation_sequence = authority_sequence
        if payout_state_generation is None:
            payout_state_generation = int(
                self._ports.payout_state().snapshot().generation
            )
        if ready_required is None:
            ready_required = self._ports.job_bundles().ready_latched()
        trigger = self._new_trigger(
            observation_sequence=observation_sequence,
            tip_hash=tip_hash,
            template_fingerprint=template_fingerprint,
            template_generation=template_generation,
            payout_state_generation=payout_state_generation,
            ready_required=ready_required,
            reasons=(reason,),
            pending_signal_token=pending_signal_token,
            post_accept_block=post_accept_block,
            fresh_capture_required=fresh_capture_required,
        )
        return self.submit_trigger(trigger)

    def submit_post_accept_trigger(
        self,
        *,
        block_height: int,
        block_hash: str,
    ) -> Future[int]:
        # The accepted candidate hash is reporting context, not a validated
        # qbit best-tip observation. The scheduler's coherent live capture
        # establishes authority for this post-accept refresh.
        tip_hash, observation_sequence = self._current_observation_authority()
        return self._submit_requirement_trigger(
            "post_accept",
            tip_hash=tip_hash,
            pending_signal_token=self.claim_pending(),
            post_accept_block=(block_height, block_hash),
            observation_sequence=observation_sequence,
            fresh_capture_required=True,
        )

    def submit_tip_observation_admission(
        self,
        tip_hash: str,
        *,
        reason: str,
    ) -> TipObservationAdmission:
        """Record one live observation and return its exact scheduler future."""
        observation_sequence = self.reserve_observation_sequence()
        before = self.newest_observed_tip()
        accepted = self.observe_tip(
            tip_hash,
            observation_sequence=observation_sequence,
            mark_pending=False,
        )
        published = self.published_snapshot()
        published_tip = published.tip_hash
        completion: Future[int] | None = None
        if (
            accepted
            and published.template is not None
            and (before != tip_hash or published_tip != tip_hash)
        ):
            pending_signal_token = self.claim_pending()
            if before != tip_hash or pending_signal_token is None:
                pending_signal_token = self.mark_pending(observation_sequence)
            trigger = self._new_trigger(
                observation_sequence=observation_sequence,
                tip_hash=tip_hash,
                payout_state_generation=int(
                    self._ports.payout_state().snapshot().generation
                ),
                ready_required=self._ports.job_bundles().ready_latched(),
                reasons=(reason,),
                pending_signal_token=pending_signal_token,
            )
            completion = self.submit_trigger(trigger)
        return TipObservationAdmission(
            accepted=accepted,
            refresh_needed=completion is not None,
            observation_sequence=observation_sequence,
            completion=completion,
        )

    def submit_tip_observation(self, tip_hash: str, *, reason: str) -> bool:
        return self.submit_tip_observation_admission(
            tip_hash,
            reason=reason,
        ).accepted

    def payout_generation_invalidated(self, generation: int) -> None:
        """Fence active work now; publication admits the runnable trigger."""
        if self._scheduler_owns_current_thread() or bool(
            getattr(self._trigger_capture_local, "active", False)
        ):
            return
        with self._state_lock:
            active = self._active_refresh
        if active is not None and active[0].payout_state_generation >= generation:
            return
        if active is not None:
            active[1].cancel()
        self.mark_pending(generation)

    def payout_generation_changed(self, generation: int) -> None:
        """Admit a runnable payout requirement after atomic publication."""
        # Reconciliation/publication performed by the active scheduler pass is
        # re-snapshotted before target selection and publication below. A
        # recursive payout trigger would only obsolete its own owner.
        if self._scheduler_owns_current_thread() or bool(
            getattr(self._trigger_capture_local, "active", False)
        ):
            return
        # Direct block acceptance guarantees a fresh post-accept capture after
        # its writer scope closes. Keep the invalidation token pending until
        # that marker is admitted instead of racing a separate payout pass
        # that can deliver duplicate clean work. If acceptance aborts before
        # producing the marker, the periodic poll still consumes the token.
        if not self._ports.wait_for_execution_permit(0.0):
            return
        # Startup consumes the first published payout state together with its
        # initial coherent tip/template authority.
        if self.published_snapshot().template is None:
            return
        with self._state_lock:
            active = self._active_refresh
        if active is not None and active[0].payout_state_generation >= generation:
            return
        if active is not None:
            active[1].cancel()
        pending_token = self.claim_pending()
        if pending_token is None:
            pending_token = self.mark_pending(generation)
        try:
            self._submit_requirement_trigger(
                "payout",
                payout_state_generation=generation,
                pending_signal_token=pending_token,
            )
        except ShutdownInProgress:
            if not self._ports.stop_requested():
                raise
            return

    def readiness_promoted(self) -> None:
        # The active scheduler pass called the one-way readiness latch before
        # selecting/building work and therefore already owns this requirement.
        # Re-enqueueing synchronously from that callback would obsolete the
        # pass that is about to satisfy it.
        if self._scheduler_owns_current_thread() or bool(
            getattr(self._trigger_capture_local, "active", False)
        ):
            return
        # Startup owns readiness promotion until it has published the initial
        # coherent tip/template authority.
        if self.published_snapshot().template is None:
            return
        pending_token = self.mark_pending("readiness")
        try:
            self._submit_requirement_trigger(
                "readiness",
                ready_required=True,
                pending_signal_token=pending_token,
            )
        except ShutdownInProgress:
            if not self._ports.stop_requested():
                raise
            return

    def template_artifacts_changed(
        self,
        artifacts: CachedTemplateArtifacts,
    ) -> None:
        # A scheduler fetch owns the exact returned artifacts. Its immutable
        # trigger is rebound immediately after the callback returns, so a
        # recursive trigger would only supersede identical in-flight work.
        if self._scheduler_owns_current_thread() or bool(
            getattr(self._trigger_capture_local, "active", False)
        ):
            return
        # The initial repository fill happens before the tip/template authority
        # is published. Startup owns that first delivery; admitting a scheduler
        # pass here would race the caller that is still assembling it.
        with self._state_lock:
            published = self._published
            if published.template is None:
                return
            latest = self._latest_detected_tip
            if latest is not None and latest[1] >= published.observation_sequence:
                authority_tip, observation_sequence = latest
            else:
                authority_tip = published.tip_hash
                observation_sequence = published.observation_sequence
            # Repository callback order is not chain-tip authority. A template
            # is admitted only after a live observation owns its parent axis.
            if artifacts.previousblockhash != authority_tip:
                return
            pending_token = self.mark_pending(artifacts.generation)
        snapshot = QbitTipTemplateSnapshot(
            bestblockhash=artifacts.previousblockhash,
            previousblockhash=artifacts.previousblockhash,
            template_fingerprint=artifacts.fingerprint,
            template_generation=artifacts.generation,
            template_artifacts=artifacts,
        )
        snapshot_changed = bool(
            published.template.bestblockhash != snapshot.bestblockhash
            or published.template.previousblockhash != snapshot.previousblockhash
            or published.template.template_fingerprint
            != snapshot.template_fingerprint
        )
        poll_start_clients = self._ports.delivery.eligible_clients()
        targets = self._ports.delivery.select_targets(
            snapshot,
            refresh_all=snapshot_changed,
        )
        try:
            self.submit_trigger(
                self._new_trigger(
                    observation_sequence=observation_sequence,
                    tip_hash=snapshot.bestblockhash,
                    payout_state_generation=int(
                        self._ports.payout_state().snapshot().generation
                    ),
                    ready_required=self._ports.job_bundles().ready_latched(),
                    reasons=("template",),
                    snapshot=snapshot,
                    poll_start_clients=poll_start_clients,
                    initial_targets=targets,
                    snapshot_changed=snapshot_changed,
                    template_fingerprint=artifacts.fingerprint,
                    template_generation=artifacts.generation,
                    pending_signal_token=pending_token,
                )
            )
        except ShutdownInProgress:
            if not self._ports.stop_requested():
                raise
            return

    def record_successful_refresh(self, observed_monotonic: float) -> None:
        with self._state_lock:
            self._last_successful_refresh_monotonic = observed_monotonic
            self._failure_started_monotonic = None

    def published_snapshot(self) -> PublishedTipSnapshot:
        with self._state_lock:
            return self._published

    def artifacts_parent_current_locked(
        self,
        artifacts: CachedTemplateArtifacts,
        *,
        now: float,
    ) -> bool:
        """Validate repository artifacts against R1's newest chain axis.

        The caller owns ``_state_lock``.  R1 deliberately validates only the
        immutable parent here: a repository callback may advance the exact
        same-tip template generation without first replacing the published
        refresh snapshot.  S2 separately proves the artifact object,
        fingerprint, generation, and payout generation at delivery commit.

        A different parent is admitted only after a live observation advances
        ``_latest_detected_tip``.  The sole fallback is the exact artifact
        owned by the published snapshot while its bounded authority lease is
        open.  Repository callback order therefore cannot manufacture chain
        authority, reuse a generation, or revive arbitrary older artifacts.
        """
        published = self._published
        latest = self._latest_detected_tip
        published_snapshot = published.template
        published_artifacts = (
            None
            if published_snapshot is None
            else published_snapshot.template_artifacts
        )
        latest_is_newest = bool(
            latest is not None
            and latest[1] >= published.observation_sequence
        )
        newest_tip = latest[0] if latest_is_newest else published.tip_hash
        if artifacts.previousblockhash == newest_tip:
            if published.tip_hash != newest_tip or published_snapshot is None:
                return True
            # On the published parent, repository generations may advance but
            # cannot move backward or reuse the published generation with a
            # different artifact identity.
            return bool(
                artifacts is published_artifacts
                or artifacts.generation > published_snapshot.template_generation
            )
        return bool(
            artifacts is published_artifacts
            and artifacts.previousblockhash == published.tip_hash
            and self._published_tip_authoritative_locked(now)
        )

    def ensure_artifacts_parent_observed(
        self,
        artifacts: CachedTemplateArtifacts,
    ) -> bool:
        """Bootstrap missing R1 chain authority from one live observation.

        Existing observed/published authority is never replaced to make an
        artifact fit.  When authority is wholly absent, R1 releases its state
        lock, fetches the live tip, records that observation through the normal
        R2 admission path, and then rechecks the persisted state.  RPC and
        observation effects therefore run without the shared S1/R1 lock.
        """
        now = self._monotonic()
        with self._state_lock:
            if self.artifacts_parent_current_locked(artifacts, now=now):
                return True
            if (
                self._latest_detected_tip is not None
                or self._published.tip_hash is not None
            ):
                return False
        live_tip = str(self._ports.rpc_call("getbestblockhash", None))
        if not self.submit_tip_observation(live_tip, reason="template"):
            return False
        now = self._monotonic()
        with self._state_lock:
            return self.artifacts_parent_current_locked(artifacts, now=now)

    @staticmethod
    def artifacts(
        snapshot: QbitTipTemplateSnapshot,
    ) -> CachedTemplateArtifacts:
        artifacts = snapshot.template_artifacts
        if (
            artifacts is None
            or artifacts.fingerprint != snapshot.template_fingerprint
            or artifacts.previousblockhash != snapshot.previousblockhash
            or artifacts.generation != snapshot.template_generation
            or snapshot.bestblockhash != snapshot.previousblockhash
            or qbit_template_fingerprint(artifacts.template) != artifacts.fingerprint
            or str(artifacts.template.get("previousblockhash", ""))
            != artifacts.previousblockhash
        ):
            raise TemplateRefreshBlocked(
                "tip/template snapshot does not own matching exact artifacts"
            )
        return artifacts

    def prepare_bundle(
        self,
        snapshot: QbitTipTemplateSnapshot,
        *,
        priority_requested_monotonic: float | None = None,
    ) -> CachedJobBundle:
        artifacts = self.artifacts(snapshot)
        build_started = self._monotonic()
        job_bundles = self._ports.job_bundles()
        priority_token, priority_requested_monotonic = (
            job_bundles.begin_priority_preparation(
                priority_requested_monotonic
            )
        )
        try:
            max_retries = max(
                0,
                int(self.config.payout_reconcile_supersession_retries),
            )
            for attempt in range(max_retries + 1):
                payout_before = self._ports.payout_state().snapshot()
                try:
                    bundle = job_bundles.shared_job_bundle(
                        artifacts,
                        mode="ready",
                        retry_superseded=False,
                        publication_critical=True,
                        request_source="tip_refresh",
                        priority_requested_monotonic=(
                            priority_requested_monotonic
                        ),
                    )
                    break
                except JobBuildSuperseded:
                    payout_after = self._ports.payout_state().snapshot()
                    if (
                        attempt >= max_retries
                        or payout_after.publication_blocked
                        or payout_after.generation == payout_before.generation
                    ):
                        raise
            else:  # pragma: no cover - range always runs at least once
                raise TemplateRefreshBlocked(
                    "payout generation did not stabilize during preparation"
                )
        except TemplateRefreshBlocked:
            raise
        except Exception as exc:
            self._ports.job_bundles().record_failure()
            raise TemplateRefreshBlocked("prepared refresh bundle build failed") from exc
        finally:
            job_bundles.finish_priority_preparation(priority_token)
            self.observe_seconds("bundle_build", self._monotonic() - build_started)
        artifacts = self.artifacts(snapshot)
        if (
            bundle.template is not artifacts.template
            or bundle.template_fingerprint != artifacts.fingerprint
            or bundle.template_generation != artifacts.generation
            or str(bundle.template.get("previousblockhash", ""))
            != artifacts.previousblockhash
        ):
            raise TemplateRefreshBlocked(
                "prepared refresh bundle does not match exact template artifacts"
            )
        if bundle.collection_only:
            raise TemplateRefreshBlocked(
                "ready-pool prepared refresh unexpectedly produced a collection bundle"
            )
        return bundle

    def prewarm_current_tip_ready_bundle(self) -> CachedJobBundle | None:
        job_bundles = self._ports.job_bundles()
        job_bundles.set_preparation_pending(True)
        try:
            observation_sequence = self.reserve_observation_sequence()
            snapshot = self._ports.fetch_snapshot()
            self._ports.observe_progress_tip_poll(snapshot)
            try:
                reconciled = self._ports.ensure_reorg_tip(snapshot.bestblockhash)
            except Exception as exc:
                raise TemplateRefreshBlocked(
                    "startup reorg reconciliation failed before job preparation"
                ) from exc
            if not reconciled:
                raise TemplateRefreshBlocked(
                    "startup chain view remained untrusted during job preparation"
                )
            ready = job_bundles.pool_readiness_latched()
            bundle: CachedJobBundle | None = None
            if ready:
                bundle = job_bundles.shared_job_bundle(
                    self.artifacts(snapshot),
                    None,
                    publication_critical=True,
                    request_source="tip_refresh",
                )
                if bundle.collection_only:
                    raise TemplateRefreshBlocked(
                        "startup ready preparation produced collection work"
                    )
                if bundle.payout_state_generation != int(
                    self._ports.payout_state().snapshot().generation
                ):
                    raise TemplateRefreshSuperseded(
                        "payout state changed during startup job preparation"
                    )
            if str(self._ports.rpc_call("getbestblockhash", None)) != snapshot.bestblockhash:
                raise TemplateRefreshSuperseded(
                    "qbit tip changed during startup job preparation"
                )
            if not self.publish_tip(
                snapshot.bestblockhash,
                observation_sequence=observation_sequence,
                publish_refresh_observation=True,
                published_snapshot=snapshot,
            ):
                raise TemplateRefreshSuperseded(
                    "startup job preparation was superseded before publication"
                )
            job_bundles.set_prepared_ready(
                snapshot if bundle is not None else None,
                bundle,
            )
            payout_generation = (
                bundle.payout_state_generation
                if bundle is not None
                else int(self._ports.payout_state().snapshot().generation)
            )
            self._ports.publish_progress_work(snapshot, payout_generation)
            with self._state_lock:
                self._last_successful_refresh_monotonic = self._monotonic()
            self._ports.observe_progress_tip_poll(snapshot)
            return bundle
        finally:
            job_bundles.set_preparation_pending(False)

    def prewarm_startup_jobs(self) -> CachedJobBundle | None:
        try:
            return self.prewarm_current_tip_ready_bundle()
        except TemplateRefreshBlocked as exc:
            self.schedule_retry()
            print(
                "prism coordinator: startup job preparation deferred "
                f"reason={exc}",
                flush=True,
            )
            return None

    def snapshot_current(
        self,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
    ) -> bool:
        with self._state_lock:
            return self._snapshot_current_locked(snapshot, observation_sequence)

    def _snapshot_current_locked(
        self,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
    ) -> bool:
        return bool(
            self._published.template is snapshot
            and self._published.tip_hash == snapshot.bestblockhash
            and self._published.observation_sequence == observation_sequence
            and not self._detected_tip_supersedes_locked(
                snapshot.bestblockhash,
                observation_sequence,
            )
        )

    def retain_collection_refresh(
        self,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
        payout_state_generation: int,
    ) -> None:
        retained = RetainedCollectionRefresh(
            snapshot,
            observation_sequence,
            payout_state_generation,
        )
        should_log = False
        has_eligible_clients = bool(self._ports.delivery.eligible_clients())
        with self._state_lock:
            if not self.snapshot_current(snapshot, observation_sequence):
                return
            if has_eligible_clients:
                return
            previous = self._retained_collection_refresh
            self._retained_collection_refresh = retained
            should_log = previous is None or (
                previous.snapshot.bestblockhash != snapshot.bestblockhash
                or previous.payout_state_generation != payout_state_generation
            )
        if should_log:
            print(
                "prism coordinator: collection refresh retained while no "
                "authorized worker identity is available",
                flush=True,
            )

    def retained_collection_artifacts(self) -> CachedTemplateArtifacts | None:
        payout_generation = self._ports.payout_state().snapshot().generation
        with self._state_lock:
            retained = self._retained_collection_refresh
            published = self._published
            if retained is None or retained.payout_state_generation != payout_generation:
                return None
            if (
                published.template is None
                or published.tip_hash != published.template.bestblockhash
            ):
                return None
            return self.artifacts(published.template)

    def retain_current_collection_refresh_if_unrepresented(self) -> None:
        if self._ports.job_bundles().ready_latched():
            return
        if self._ports.delivery.eligible_clients():
            return
        with self._state_lock:
            snapshot = self._published.template
            observation_sequence = self._published.observation_sequence
        if snapshot is None:
            return
        self.retain_collection_refresh(
            snapshot,
            observation_sequence,
            self._ports.payout_state().snapshot().generation,
        )

    def note_collection_identity_available(self, client: object) -> None:
        if not self._ports.delivery.client_can_receive_jobs(client):
            return
        retained_artifacts = self.retained_collection_artifacts()
        if retained_artifacts is None:
            return
        with self._state_lock:
            published = self._published
            snapshot = published.template
            latest = self._latest_detected_tip
            if (
                snapshot is None
                or snapshot.template_artifacts is not retained_artifacts
                or (
                    latest is not None
                    and (
                        latest[1] > published.observation_sequence
                        or latest[0] != published.tip_hash
                    )
                )
            ):
                return
            observation_sequence = published.observation_sequence
            pending_token = self.mark_pending(
                getattr(client, "connection_id", None)
            )
        poll_start_clients = self._ports.delivery.eligible_clients()
        targets = self._ports.delivery.select_targets(
            snapshot,
            refresh_all=False,
        )
        try:
            self.submit_trigger(
                self._new_trigger(
                    observation_sequence=observation_sequence,
                    tip_hash=snapshot.bestblockhash,
                    payout_state_generation=int(
                        self._ports.payout_state().snapshot().generation
                    ),
                    ready_required=self._ports.job_bundles().ready_latched(),
                    reasons=("retained_collection",),
                    pending_signal_token=pending_token,
                    snapshot=snapshot,
                    poll_start_clients=poll_start_clients,
                    initial_targets=targets,
                    snapshot_changed=False,
                )
            )
        except ShutdownInProgress:
            if not self._ports.stop_requested():
                raise

    def consume_retained_collection_refresh(self, context: object) -> None:
        if not bool(getattr(context, "collection_only", False)):
            return
        with self._state_lock:
            retained = self._retained_collection_refresh
            snapshot = self._published.template
            artifacts = None if snapshot is None else snapshot.template_artifacts
            if (
                retained is not None
                and retained.payout_state_generation
                == int(getattr(context, "payout_state_generation"))
                and artifacts is not None
                and getattr(context, "template") is artifacts.template
                and getattr(context, "template_fingerprint") == artifacts.fingerprint
                and int(getattr(context, "template_generation")) == artifacts.generation
            ):
                self._retained_collection_refresh = None

    def clear_retained_collection_refresh(self) -> None:
        with self._state_lock:
            self._retained_collection_refresh = None

    def newest_observed_tip(self) -> str | None:
        with self._state_lock:
            if self._latest_detected_tip is not None:
                return self._latest_detected_tip[0]
            return self._published.tip_hash

    def _current_observation_authority(self) -> tuple[str | None, int]:
        """Snapshot the newest live-detected or published chain authority."""
        with self._state_lock:
            latest = self._latest_detected_tip
            published = self._published
            if latest is not None and latest[1] >= published.observation_sequence:
                return latest
            return published.tip_hash, published.observation_sequence

    def reserve_observation_sequence(self) -> int:
        with self._state_lock:
            self._observation_sequence = max(
                self._observation_sequence,
                self._published.observation_sequence,
            ) + 1
            return self._observation_sequence

    def observation_sequence(self) -> int:
        with self._state_lock:
            return self._observation_sequence

    def pending(self) -> bool:
        return self._pending_event.is_set()

    def mark_pending(self, _observation: object = None) -> int:
        with self._state_lock:
            self._pending_counter += 1
            token = self._pending_counter
            self._pending_token = token
            self._pending_event.set()
            return token

    def claim_pending(self) -> int | None:
        with self._state_lock:
            return self._pending_token if self._pending_event.is_set() else None

    def mark_pending_for_poll(
        self,
        owned_token: int | None,
        _observation: object = None,
    ) -> int | None:
        with self._state_lock:
            if self._pending_token != owned_token:
                return owned_token
            if owned_token is not None:
                self._pending_event.set()
                return owned_token
            self._pending_counter += 1
            token = self._pending_counter
            self._pending_token = token
            self._pending_event.set()
            return token

    def clear_pending(self, token: int) -> None:
        with self._state_lock:
            if self._pending_token == token:
                self._pending_token = None
                self._pending_event.clear()

    def schedule_retry(self) -> None:
        # Pair the event with a monotonic generation so a producer cannot set
        # it between a waiter's wake and clear and lose the newest retry.
        with self._state_lock:
            self._retry_counter += 1
            self._retry_event.set()

    def consume_retry(self) -> bool:
        """Consume all retry signals visible at one atomic wake boundary."""
        with self._state_lock:
            generation = self._retry_counter
            if generation == self._retry_consumed:
                return False
            self._retry_consumed = generation
            self._retry_event.clear()
            return True

    def note_attempt_failed(self, observed_tip: str | None = None) -> None:
        """Space another failed attempt while its observed tip is unchanged."""
        holdoff = float(self.config.failure_holdoff_seconds)
        if holdoff <= 0:
            return
        holdoff += random.uniform(
            0.0,
            holdoff * PRISM_TIP_REFRESH_FAILURE_HOLDOFF_JITTER_FRACTION,
        )
        with self._state_lock:
            if observed_tip is None:
                observed_tip = (
                    self._latest_detected_tip[0]
                    if self._latest_detected_tip is not None
                    else self._published.tip_hash
                )
            self._failure_tip = observed_tip
            self._failure_holdoff_until = self._monotonic() + holdoff

    def clear_failure_holdoff(self) -> None:
        with self._state_lock:
            self._failure_holdoff_until = None
            self._failure_tip = None

    def failure_holdoff_remaining(self) -> float:
        """Return zero immediately when a newer observed tip re-arms refresh."""
        with self._state_lock:
            deadline = self._failure_holdoff_until
            failed_tip = self._failure_tip
            current_tip = (
                self._latest_detected_tip[0]
                if self._latest_detected_tip is not None
                else self._published.tip_hash
            )
        if deadline is None or current_tip != failed_tip:
            return 0.0
        return max(0.0, deadline - self._monotonic())

    def _detected_tip_supersedes_locked(
        self,
        tip_hash: str,
        observation_sequence: int,
    ) -> bool:
        latest = self._latest_detected_tip
        return bool(
            latest is not None
            and latest[0] != tip_hash
            and latest[1] > observation_sequence
        )

    def observe_tip(
        self,
        tip_hash: str,
        *,
        observation_sequence: int | None = None,
        mark_pending: bool = True,
    ) -> bool:
        """Record detection without moving published submit authority."""
        if observation_sequence is None:
            observation_sequence = self.reserve_observation_sequence()
        with self._observation_effects_lock:
            if self._observation_effects_active:
                raise RuntimeError(
                    "tip observation ports must not synchronously reenter observation"
                )
            self._observation_effects_active = True
            try:
                now = self._monotonic()
                active_to_cancel: FanoutCancellation | None = None
                with self._state_lock:
                    latest = self._latest_detected_tip
                    if latest is not None and observation_sequence < latest[1]:
                        return latest[0] == tip_hash
                    published_tip = self._published.tip_hash
                    prior_detected_hash = (
                        latest[0]
                        if latest is not None
                        else self._published.tip_hash
                    )
                    detection_changed = (
                        prior_detected_hash is not None
                        and prior_detected_hash != tip_hash
                    )
                    self._latest_detected_tip = (tip_hash, observation_sequence)
                    replacement_needed = (
                        published_tip is None or published_tip != tip_hash
                    )
                    if published_tip == tip_hash:
                        self._published = PublishedTipSnapshot(
                            self._published.first_seen,
                            self._published.parent,
                            self._published.observation_sequence,
                            now,
                            self._published.template,
                        )
                    elif (
                        published_tip is not None
                        and self._divergence_started_monotonic is None
                    ):
                        # First departure owns the bounded lease; later churn
                        # cannot renew it.
                        self._divergence_started_monotonic = now
                    active = self._active_refresh
                    if (
                        active is not None
                        and active[0].tip_hash != tip_hash
                        and active[0].observation_sequence < observation_sequence
                    ):
                        active_to_cancel = active[1]
                    should_mark = bool(
                        mark_pending
                        and (
                            detection_changed
                            or (
                                replacement_needed
                                and not self._pending_event.is_set()
                            )
                        )
                    )

                if detection_changed:
                    # The effects lock preserves observation order while the
                    # R1 state lock stays released across domain callbacks.
                    self._ports.payout_state().reserve_source_for_tip_change(
                        tip_hash,
                        cause="external_tip",
                        invalidated_monotonic=now,
                    )
                if active_to_cancel is not None:
                    active_to_cancel.cancel()
                if detection_changed:
                    self._ports.mark_progress_pending(now)
                    self._ports.cancel_obsolete_bundle_builds(tip_hash, None)
                    self._ports.cancel_obsolete_job_builds("chain tip superseded")
                if should_mark:
                    self.mark_pending(observation_sequence)
                    self.schedule_retry()
                return True
            finally:
                self._observation_effects_active = False

    def publication_failure_expired(
        self,
        now: float | None = None,
        *,
        budget_seconds: float | None = None,
    ) -> bool:
        """Return whether coherent publication has diverged past its budget."""
        current = self._monotonic() if now is None else now
        budget = (
            self.config.failure_exit_seconds
            if budget_seconds is None
            else float(budget_seconds)
        )
        if budget <= 0:
            return False
        with self._state_lock:
            started = self._divergence_started_monotonic
        return bool(started is not None and current - started >= budget)

    def _fetch_parent_hash(self, tip_hash: str) -> str | None:
        block = self._ports.rpc_call("getblock", [tip_hash])
        if not isinstance(block, Mapping):
            return None
        parent = str(block.get("previousblockhash", "") or "")
        return parent or None

    def publish_tip(
        self,
        tip_hash: str,
        *,
        observation_sequence: int | None = None,
        publish_refresh_observation: bool = False,
        published_snapshot: QbitTipTemplateSnapshot | None = None,
    ) -> bool:
        """Publish exact coherent work only after caller validation."""
        if published_snapshot is not None and published_snapshot.bestblockhash != tip_hash:
            raise ValueError("published snapshot does not match tip hash")
        if observation_sequence is None:
            observation_sequence = self.reserve_observation_sequence()
        if not self.observe_tip(
            tip_hash,
            observation_sequence=observation_sequence,
            mark_pending=False,
        ):
            return False
        now = self._monotonic()
        with self._state_lock:
            if (
                observation_sequence < self._published.observation_sequence
                or self._detected_tip_supersedes_locked(tip_hash, observation_sequence)
            ):
                return False
            if self._published.tip_hash == tip_hash:
                active = self._active_refresh
                sequence = self._published.observation_sequence
                if publish_refresh_observation and (
                    active is None or active[0].tip_hash != tip_hash
                ):
                    sequence = observation_sequence
                self._published = PublishedTipSnapshot(
                    self._published.first_seen,
                    self._published.parent,
                    sequence,
                    now,
                    published_snapshot or self._published.template,
                )
                self._divergence_started_monotonic = None
                return True

        try:
            parent_hash = self._fetch_parent_hash(tip_hash)
        except Exception:
            parent_hash = None

        with self._state_lock:
            if (
                observation_sequence < self._published.observation_sequence
                or self._detected_tip_supersedes_locked(tip_hash, observation_sequence)
            ):
                return False
            if self._published.tip_hash == tip_hash:
                active = self._active_refresh
                sequence = self._published.observation_sequence
                if publish_refresh_observation and (
                    active is None or active[0].tip_hash != tip_hash
                ):
                    sequence = observation_sequence
                self._published = PublishedTipSnapshot(
                    self._published.first_seen,
                    self._published.parent,
                    sequence,
                    now,
                    published_snapshot or self._published.template,
                )
                self._divergence_started_monotonic = None
                return True
            tip_changed = self._published.first_seen is not None
            self._published = PublishedTipSnapshot(
                (tip_hash, now if tip_changed else None),
                None if parent_hash is None else (tip_hash, parent_hash),
                observation_sequence,
                now,
                published_snapshot,
            )
            self._divergence_started_monotonic = None
            self._retained_collection_refresh = None

        self._ports.prune_evicted_jobs(now, True)
        if tip_changed:
            self._ports.job_bundles().clear_prepared_ready()
        return True

    def current_tip_parent_hash(self, tip_hash: str) -> str | None:
        with self._state_lock:
            published = self._published
            if published.parent is not None and published.parent[0] == tip_hash:
                return published.parent[1]
            observed_sequence = (
                published.observation_sequence if published.tip_hash == tip_hash else None
            )
        parent = self._fetch_parent_hash(tip_hash)
        if parent is None:
            return None
        with self._state_lock:
            published = self._published
            if (
                observed_sequence is not None
                and published.tip_hash == tip_hash
                and published.observation_sequence == observed_sequence
            ):
                self._published = PublishedTipSnapshot(
                    published.first_seen,
                    (tip_hash, parent),
                    published.observation_sequence,
                    published.observed_monotonic,
                    published.template,
                )
        return parent

    def published_tip_authoritative(self, now: float | None = None) -> bool:
        now = self._monotonic() if now is None else now
        with self._state_lock:
            return self._published_tip_authoritative_locked(now)

    def _published_tip_authoritative_locked(self, now: float) -> bool:
        published = self._published
        if self.config.submit_tip_max_age_seconds <= 0 or published.first_seen is None:
            return False
        if (
            published.observed_monotonic is not None
            and now - published.observed_monotonic
            <= self.config.submit_tip_max_age_seconds
        ):
            return True
        return bool(
            self._latest_detected_tip is not None
            and self._latest_detected_tip[0] != published.first_seen[0]
            and self._divergence_started_monotonic is not None
            and self.config.failure_exit_seconds > 0
            and now - self._divergence_started_monotonic
            <= self.config.failure_exit_seconds
        )

    def submit_authority(self) -> str:
        with self._state_lock:
            if self.published_tip_authoritative(self._monotonic()):
                assert self._published.first_seen is not None
                return self._published.first_seen[0]
        return str(self._ports.rpc_call("getbestblockhash", None))

    def clear_pending_for_completed_refresh(
        self,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
        payout_state_generation: int,
        pending_signal_token: int | None,
    ) -> bool:
        payout = self._ports.payout_state()
        with payout.delivery_gate.delivery_cancelable(
            lambda: payout.snapshot().generation != payout_state_generation,
            generation=payout_state_generation,
            priority=True,
        ) as admission:
            if not admission:
                return False
            if payout.snapshot().generation != payout_state_generation:
                return False
            with self._state_lock:
                published = self._published
                current = bool(
                    published.template is snapshot
                    and published.tip_hash == snapshot.bestblockhash
                    and published.observation_sequence == observation_sequence
                    and not self._detected_tip_supersedes_locked(
                        snapshot.bestblockhash,
                        observation_sequence,
                    )
                )
                if not current or self._pending_token != pending_signal_token:
                    return False
                self._pending_token = None
                self._pending_event.clear()
                return True

    def token_prepublication_current(
        self,
        token: TipRefreshValidationToken,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
    ) -> bool:
        payout = self._ports.payout_state().snapshot()
        with self._state_lock:
            return self._token_prepublication_current_locked(
                token,
                bundle,
                snapshot,
                payout,
            )

    def _token_prepublication_current_locked(
        self,
        token: TipRefreshValidationToken,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        payout: PayoutStateSnapshot,
    ) -> bool:
        published_payout = payout.published
        return bool(
            token.snapshot is snapshot
            and token.tip_hash == snapshot.bestblockhash
            and token.template_fingerprint == snapshot.template_fingerprint
            and token.template_generation == snapshot.template_generation
            and bundle.template_fingerprint == token.template_fingerprint
            and bundle.template_generation == token.template_generation
            and bundle.payout_state_generation == token.payout_state_generation
            and bundle.build_key is token.build_key
            and token.build_key.best_tip_hash == snapshot.bestblockhash
            and token.build_key.previous_block_hash == snapshot.previousblockhash
            and token.build_key.template_fingerprint == snapshot.template_fingerprint
            and token.build_key.template_generation == snapshot.template_generation
            and token.build_key.payout_state_generation == token.payout_state_generation
            and token.payout_state_generation == int(payout.generation)
            and published_payout is not None
            and published_payout.artifact is not None
            and token.build_key.payout_artifact_sha256
            == published_payout.artifact.prior_balances_sha256
            and snapshot.template_artifacts is not None
            and bundle.template is snapshot.template_artifacts.template
            and not self._detected_tip_supersedes_locked(
                snapshot.bestblockhash,
                token.observation_sequence,
            )
        )

    def token_current(
        self,
        token: TipRefreshValidationToken,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
    ) -> bool:
        payout = self._ports.payout_state().snapshot()
        with self._state_lock:
            return bool(
                self._token_prepublication_current_locked(
                    token,
                    bundle,
                    snapshot,
                    payout,
                )
                and self._snapshot_current_locked(
                    snapshot,
                    token.observation_sequence,
                )
            )

    def token_current_for_payout_snapshot(
        self,
        token: TipRefreshValidationToken,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        payout: PayoutStateSnapshot,
    ) -> bool:
        """Validate a delivery token without acquiring the P1 state lock.

        S2 captures ``payout`` while its P1 delivery admission is active, then
        invokes this method under the shared S1/R1 authority lock.
        """
        with self._state_lock:
            return bool(
                self._token_prepublication_current_locked(
                    token,
                    bundle,
                    snapshot,
                    payout,
                )
                and self._snapshot_current_locked(
                    snapshot,
                    token.observation_sequence,
                )
            )

    def validate_prepared(
        self,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
    ) -> TipRefreshValidationToken:
        artifacts = self.artifacts(snapshot)
        if (
            bundle.template is not artifacts.template
            or bundle.template_fingerprint != artifacts.fingerprint
            or bundle.template_generation != artifacts.generation
            or bundle.build_key is None
            or bundle.build_key.best_tip_hash != snapshot.bestblockhash
            or bundle.build_key.previous_block_hash != snapshot.previousblockhash
            or bundle.build_key.template_fingerprint != artifacts.fingerprint
            or bundle.build_key.template_generation != artifacts.generation
            or bundle.build_key.mode != "ready"
        ):
            raise TemplateRefreshBlocked(
                "prepared refresh bundle changed before final validation"
            )
        try:
            current_tip = str(self._ports.rpc_call("getbestblockhash", None))
        except Exception as exc:
            self.schedule_retry()
            raise TemplateRefreshBlocked(
                "qbit tip validation failed before prepared fanout"
            ) from exc
        if current_tip != snapshot.bestblockhash:
            self.schedule_retry()
            raise TemplateRefreshSuperseded(
                "qbit tip changed before prepared fanout "
                f"expected={snapshot.bestblockhash} current={current_tip}"
            )
        try:
            chain_view_untrusted = self._ports.chain_view_untrusted()
        except Exception as exc:
            self.schedule_retry()
            raise TemplateRefreshBlocked(
                "qbit chain trust check failed before prepared fanout"
            ) from exc
        if chain_view_untrusted:
            self.schedule_retry()
            raise TemplateRefreshBlocked(
                "qbit chain view became untrusted before prepared fanout"
            )
        token = TipRefreshValidationToken(
            tip_hash=snapshot.bestblockhash,
            template_fingerprint=artifacts.fingerprint,
            template_generation=artifacts.generation,
            payout_state_generation=bundle.payout_state_generation,
            observation_sequence=observation_sequence,
            build_key=bundle.build_key,
            snapshot=snapshot,
        )
        if not self.token_prepublication_current(token, bundle, snapshot):
            self.schedule_retry()
            raise TemplateRefreshSuperseded(
                "prepared refresh was superseded before tip publication"
            )
        return token

    def activate(
        self,
        token: TipRefreshValidationToken,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        cancel_event: FanoutCancellation,
    ) -> None:
        payout = self._ports.payout_state().snapshot()
        prior_cancel: FanoutCancellation | None = None
        with self._state_lock:
            if not (
                self._token_prepublication_current_locked(
                    token,
                    bundle,
                    snapshot,
                    payout,
                )
                and self._snapshot_current_locked(
                    snapshot,
                    token.observation_sequence,
                )
            ):
                self.schedule_retry()
                raise TemplateRefreshSuperseded(
                    "prepared refresh was superseded before cancellation registration"
                )
            active = self._active_refresh
            if active is not None:
                prior_cancel = active[1]
            self._active_refresh = (token, cancel_event)
        if prior_cancel is not None:
            prior_cancel.cancel()

    def publish_prepared(
        self,
        token: TipRefreshValidationToken,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        *,
        parent_hash: str | None,
    ) -> FanoutCancellation:
        now = self._monotonic()
        cancel_event = FanoutCancellation()
        payout = self._ports.payout_state()
        tip_changed = False
        prior_cancel: FanoutCancellation | None = None
        with payout.delivery_gate.delivery_cancelable(
            lambda: False,
            generation=token.payout_state_generation,
            priority=True,
        ) as admitted:
            if not admitted:
                self.schedule_retry()
                raise TemplateRefreshSuperseded(
                    "prepared refresh was superseded before atomic publication"
                )
            with self._publication_lock:
                payout_snapshot = payout.snapshot()
                with self._state_lock:
                    if (
                        payout_snapshot.publication_blocked
                        or self._published.observation_sequence > token.observation_sequence
                        or not self._token_prepublication_current_locked(
                            token,
                            bundle,
                            snapshot,
                            payout_snapshot,
                        )
                    ):
                        self.schedule_retry()
                        raise TemplateRefreshSuperseded(
                            "prepared refresh was superseded before atomic publication"
                        )
                    first_seen = self._published.first_seen
                    tip_changed = first_seen is not None and first_seen[0] != token.tip_hash
                    flip_stamp = (
                        now
                        if tip_changed
                        else first_seen[1]
                        if first_seen is not None
                        else None
                    )
                    prior_parent = self._published.parent
                    published_parent = (
                        (token.tip_hash, parent_hash)
                        if parent_hash is not None
                        else prior_parent
                        if prior_parent is not None and prior_parent[0] == token.tip_hash
                        else None
                    )
                    self._published = PublishedTipSnapshot(
                        (token.tip_hash, flip_stamp),
                        published_parent,
                        token.observation_sequence,
                        now,
                        snapshot,
                    )
                    self._divergence_started_monotonic = None
                    if tip_changed:
                        self._retained_collection_refresh = None
                    if not (
                        self._token_prepublication_current_locked(
                            token,
                            bundle,
                            snapshot,
                            payout_snapshot,
                        )
                        and self._snapshot_current_locked(
                            snapshot,
                            token.observation_sequence,
                        )
                    ):
                        raise TemplateRefreshBlocked(
                            "prepared refresh publication did not produce a current token"
                        )
                    active = self._active_refresh
                    if active is not None:
                        prior_cancel = active[1]
                    self._active_refresh = (token, cancel_event)
        if prior_cancel is not None:
            prior_cancel.cancel()
        if tip_changed:
            self._ports.job_bundles().clear_prepared_ready()
            self._ports.prune_evicted_jobs(now, True)
        return cancel_event

    def clear_active(
        self,
        token: TipRefreshValidationToken,
        cancel_event: FanoutCancellation,
    ) -> None:
        with self._state_lock:
            active = self._active_refresh
            if active is not None and active[0] is token and active[1] is cancel_event:
                self._active_refresh = None

    def prepared_obsolete(
        self,
        token: TipRefreshValidationToken,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        cancel_event: FanoutCancellation | None,
    ) -> bool:
        if self._ports.stop_requested() or (
            cancel_event is not None and cancel_event.is_set()
        ):
            return True
        current = self.token_current(token, bundle, snapshot)
        if not current and cancel_event is not None:
            cancel_event.cancel()
        return not current

    def fanout_prepared(
        self,
        clients: list[object],
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        *,
        observation_sequence: int | None = None,
        validation_token: TipRefreshValidationToken | None = None,
        preactivated_cancel_event: FanoutCancellation | None = None,
        executor: object | None = None,
        expected_active_jobs: dict[object, object | None] | None = None,
        heartbeat_name: str,
    ) -> tuple[int, float | None, float | None, int]:
        executor = executor or self.executor()
        cancel_event = preactivated_cancel_event or FanoutCancellation()
        if observation_sequence is None:
            observation_sequence = self.published_snapshot().observation_sequence
        if validation_token is None:
            validation_token = self.validate_prepared(
                bundle,
                snapshot,
                observation_sequence,
            )
        if preactivated_cancel_event is None:
            self.activate(validation_token, bundle, snapshot, cancel_event)
        futures: dict[Future[RefreshResult], object] = {}
        submitted_at: dict[Future[RefreshResult], float] = {}
        queued_cancellations: set[Future[RefreshResult]] = set()
        if expected_active_jobs is None:
            expected_active_jobs = {
                client: self._ports.delivery.active_job(client) for client in clients
            }
        clients_iter = iter(clients)
        max_inflight = max(1, self.config.max_workers)

        def record_queued_cancellation(future: Future[RefreshResult]) -> None:
            if future in queued_cancellations:
                return
            queued_cancellations.add(future)
            elapsed = max(0.0, self._monotonic() - submitted_at[future])
            self._ports.observe_job_build_elapsed(
                elapsed,
                {"executor_queue": elapsed},
            )
            self.record_cancellation("executor_queue")

        def cancel_pending_futures(pending: set[Future[RefreshResult]]) -> None:
            cancel_event.cancel()
            for future in pending:
                if future.cancel():
                    record_queued_cancellation(future)

        def submit_available(pending: set[Future[RefreshResult]]) -> None:
            while (
                len(pending) < max_inflight
                and not self._ports.stop_requested()
                and not cancel_event.is_set()
            ):
                if not self.token_current(validation_token, bundle, snapshot):
                    cancel_event.cancel()
                    return
                try:
                    client = next(clients_iter)
                except StopIteration:
                    return
                submitted = self._monotonic()
                expected = expected_active_jobs.get(client)
                future = self._ports.delivery.submit_task(
                    executor,
                    self._ports.delivery.send_prepared_job,
                    client,
                    bundle,
                    snapshot,
                    validation_token,
                    self._ports.delivery.connection_id(client),
                    expected,
                    cancel_event,
                    submitted,
                    priority=self._ports.delivery.delivery_priority(
                        client,
                        snapshot,
                        expected,
                    ),
                )
                self.future_started()
                future.add_done_callback(self.future_finished)
                futures[future] = client
                submitted_at[future] = submitted
                pending.add(future)

        pending: set[Future[RefreshResult]] = set()
        try:
            sent = 0
            failed = 0
            first_delivery: float | None = None
            last_delivery: float | None = None
            invalidation: TemplateRefreshBlocked | None = None
            last_live_trust_check = self._monotonic()
            try:
                submit_available(pending)
            except RuntimeError:
                cancel_pending_futures(pending)
                cancel_event.set()
                if pending:
                    wait(pending)
                if not self._ports.stop_requested():
                    self.schedule_retry()
                    raise
            while pending:
                self._ports.heartbeat(heartbeat_name)
                if self._ports.stop_requested() or cancel_event.is_set():
                    cancel_pending_futures(pending)
                done, pending = wait(
                    pending,
                    timeout=PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS,
                    return_when=FIRST_COMPLETED,
                )
                for future in done:
                    client = futures[future]
                    if future.cancelled():
                        if future not in queued_cancellations:
                            record_queued_cancellation(future)
                        self.record_client_result("skipped")
                        continue
                    try:
                        result = future.result()
                    except OSError:
                        self.record_client_result("disconnected")
                        self._ports.delivery.disconnect(client)
                        continue
                    except TemplateRefreshBlocked as exc:
                        self.record_client_result("skipped")
                        invalidation = exc
                        cancel_pending_futures(pending)
                        continue
                    except Exception:
                        failed += 1
                        self.record_client_result("failed")
                        self._ports.job_bundles().record_failure()
                        print(
                            "prism coordinator: prepared job fanout failed "
                            + self._ports.delivery.log_identity(client),
                            flush=True,
                        )
                        import traceback

                        traceback.print_exc()
                        continue
                    self.record_client_result(result.result)
                    if result.result == "sent":
                        sent += 1
                        delivered = result.delivered_monotonic
                        if delivered is not None:
                            first_delivery = (
                                delivered
                                if first_delivery is None
                                else min(first_delivery, delivered)
                            )
                            last_delivery = (
                                delivered
                                if last_delivery is None
                                else max(last_delivery, delivered)
                            )
                if (
                    pending
                    and invalidation is None
                    and not self._ports.stop_requested()
                    and self._monotonic() - last_live_trust_check >= 1.0
                ):
                    try:
                        if not self._ports.ensure_reorg_current(snapshot.bestblockhash):
                            raise TemplateRefreshBlocked(
                                "qbit chain view became untrusted during prepared fanout"
                            )
                        last_live_trust_check = self._monotonic()
                    except ShutdownInProgress:
                        cancel_pending_futures(pending)
                        raise
                    except TemplateRefreshBlocked as exc:
                        invalidation = exc
                    except Exception as exc:
                        invalidation = TemplateRefreshBlocked(
                            "qbit chain trust check failed during prepared fanout"
                        )
                        invalidation.__cause__ = exc
                    if invalidation is not None:
                        cancel_pending_futures(pending)
                if invalidation is None:
                    try:
                        submit_available(pending)
                    except RuntimeError:
                        cancel_pending_futures(pending)
                        cancel_event.set()
                        if pending:
                            wait(pending)
                        if not self._ports.stop_requested():
                            self.schedule_retry()
                            raise
            if invalidation is not None:
                cancel_event.set()
                self.schedule_retry()
                raise invalidation
            if not self.token_current(validation_token, bundle, snapshot):
                self.schedule_retry()
                raise TemplateRefreshSuperseded(
                    "prepared refresh was superseded during fanout; immediate retry scheduled"
                )
            try:
                post_fanout_tip = str(self._ports.rpc_call("getbestblockhash", None))
            except Exception as exc:
                self.schedule_retry()
                raise TemplateRefreshBlocked(
                    "qbit tip validation failed after prepared fanout; "
                    "immediate retry scheduled"
                ) from exc
            if post_fanout_tip != snapshot.bestblockhash:
                cancel_event.set()
                self.schedule_retry()
                raise TemplateRefreshSuperseded(
                    "qbit tip changed during prepared fanout; immediate retry scheduled "
                    f"expected={snapshot.bestblockhash} current={post_fanout_tip}"
                )
            try:
                post_fanout_untrusted = self._ports.chain_view_untrusted()
            except Exception as exc:
                cancel_event.set()
                self.schedule_retry()
                raise TemplateRefreshBlocked(
                    "qbit chain trust check failed after prepared fanout; "
                    "immediate retry scheduled"
                ) from exc
            if post_fanout_untrusted:
                cancel_event.set()
                self.schedule_retry()
                raise TemplateRefreshBlocked(
                    "qbit chain view became untrusted during prepared fanout; "
                    "immediate retry scheduled"
                )
            if not self.token_current(validation_token, bundle, snapshot):
                cancel_event.set()
                self.schedule_retry()
                raise TemplateRefreshSuperseded(
                    "prepared refresh payout state changed during post-fanout "
                    "validation; immediate retry scheduled"
                )
            return sent, first_delivery, last_delivery, failed
        finally:
            self.clear_active(validation_token, cancel_event)

    def _raise_if_superseded(
        self,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
    ) -> None:
        with self._state_lock:
            superseded = self._detected_tip_supersedes_locked(
                snapshot.bestblockhash,
                observation_sequence,
            )
        if superseded:
            self.schedule_retry()
            raise TemplateRefreshSuperseded(
                "tip/template poll was superseded by a newer tip observation "
                "before refresh preparation"
            )

    def _probe_tip_while_waiting(self) -> None:
        observation_sequence = self.reserve_observation_sequence()
        try:
            observed_tip = str(self._ports.rpc_call("getbestblockhash", None))
        except Exception:
            return
        self.observe_tip(observed_tip, observation_sequence=observation_sequence)

    def _capture_poll_trigger(
        self,
        *,
        observation_sequence: int,
        reasons: tuple[str, ...],
        pending_signal_token: int | None,
        post_accept_block: tuple[int, str] | None = None,
        existing: TipRefreshTrigger | None = None,
    ) -> TipRefreshTrigger:
        poll_start_clients = self._ports.delivery.eligible_clients()
        observed_best_tip = str(self._ports.rpc_call("getbestblockhash", None))
        if not self.observe_tip(
            observed_best_tip,
            observation_sequence=observation_sequence,
            mark_pending=False,
        ):
            self.schedule_retry()
            raise TemplateRefreshSuperseded(
                "tip/template poll was superseded before template fetch"
            )
        published_tip = self.published_snapshot().tip_hash
        if published_tip is not None and published_tip != observed_best_tip:
            pending_signal_token = self.mark_pending_for_poll(
                pending_signal_token,
                observation_sequence,
            )
        capture_was_active = bool(
            getattr(self._trigger_capture_local, "active", False)
        )
        self._trigger_capture_local.active = True
        try:
            fetch_for_tip = self._ports.fetch_snapshot_for_tip
            snapshot = (
                self._ports.fetch_snapshot()
                if fetch_for_tip is None
                else fetch_for_tip(observed_best_tip)
            )
        except Exception:
            self.note_attempt_failed(observed_best_tip)
            raise
        finally:
            self._trigger_capture_local.active = capture_was_active
        if not self.observe_tip(
            snapshot.bestblockhash,
            observation_sequence=observation_sequence,
            mark_pending=False,
        ):
            self.schedule_retry()
            raise TemplateRefreshSuperseded(
                "tip/template poll was superseded during template fetch"
            )
        published_after_fetch = self.published_snapshot().tip_hash
        if (
            published_after_fetch is not None
            and published_after_fetch != snapshot.bestblockhash
        ):
            pending_signal_token = self.mark_pending_for_poll(
                pending_signal_token,
                observation_sequence,
            )
        self._ports.observe_progress_tip_poll(snapshot)
        job_bundles = self._ports.job_bundles()
        ready_required = job_bundles.ready_latched()
        payout_generation = int(self._ports.payout_state().snapshot().generation)
        previous_snapshot = self.published_snapshot().template
        snapshot_changed = previous_snapshot is not None and (
            previous_snapshot.bestblockhash != snapshot.bestblockhash
            or previous_snapshot.previousblockhash != snapshot.previousblockhash
            or previous_snapshot.template_fingerprint != snapshot.template_fingerprint
        )
        targets = self._ports.delivery.select_targets(
            snapshot,
            refresh_all=snapshot_changed,
        )
        if targets and snapshot_changed:
            pending_signal_token = self.mark_pending_for_poll(
                pending_signal_token,
                observation_sequence,
            )
        if existing is None:
            return self._new_trigger(
                observation_sequence=observation_sequence,
                tip_hash=snapshot.bestblockhash,
                payout_state_generation=payout_generation,
                ready_required=ready_required,
                reasons=reasons,
                pending_signal_token=pending_signal_token,
                snapshot=snapshot,
                poll_start_clients=poll_start_clients,
                initial_targets=targets,
                snapshot_changed=snapshot_changed,
                post_accept_block=post_accept_block,
            )
        return dataclass_replace(
            existing,
            observation_sequence=observation_sequence,
            tip_hash=snapshot.bestblockhash,
            template_fingerprint=snapshot.template_fingerprint,
            template_generation=snapshot.template_generation,
            payout_state_generation=max(
                existing.payout_state_generation,
                payout_generation,
            ),
            ready_required=existing.ready_required or ready_required,
            pending_signal_token=(
                pending_signal_token
                if pending_signal_token is not None
                else existing.pending_signal_token
            ),
            snapshot=snapshot,
            poll_start_clients=poll_start_clients,
            initial_targets=targets,
            snapshot_changed=snapshot_changed,
            fresh_capture_required=False,
        )

    def detect_poll_trigger(
        self,
        *,
        reason: str = "blockpoll",
        post_accept_block: tuple[int, str] | None = None,
    ) -> TipRefreshTrigger:
        observation_sequence = self.reserve_observation_sequence()
        return self._capture_poll_trigger(
            observation_sequence=observation_sequence,
            reasons=(reason,),
            pending_signal_token=self.claim_pending(),
            post_accept_block=post_accept_block,
        )

    def submit_poll_trigger(
        self,
        *,
        reason: str = "blockpoll",
        post_accept_block: tuple[int, str] | None = None,
    ) -> Future[int]:
        return self.submit_trigger(
            self.detect_poll_trigger(
                reason=reason,
                post_accept_block=post_accept_block,
            )
        )

    def poll_once(self, *, heartbeat_name: str = "qbit_blockpoll") -> int:
        reason = "blockwait" if heartbeat_name == "qbit_blockwait" else "blockpoll"
        with self._scheduler_condition:
            owner_already_active = self._scheduler_active is not None
        try:
            completion = self.submit_poll_trigger(reason=reason)
        except (TemplateRefreshSuperseded, PayoutStatePublicationBlocked):
            raise
        except Exception:
            self.record_template_refresh_failure(self._monotonic())
            raise
        if owner_already_active:
            # Contending producers only contribute their newest immutable
            # requirement. The scheduler owner drains that coalesced follow-up
            # without tying another poll/blockwait caller to the heavy lane.
            return 0
        return self._await_scheduler_result(completion, heartbeat_name)

    def _await_scheduler_result(
        self,
        completion: Future[int],
        heartbeat_name: str,
        *,
        stop_result: int | None = None,
    ) -> int:
        try:
            while True:
                self._ports.heartbeat(heartbeat_name)
                try:
                    return completion.result(
                        timeout=PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS
                    )
                except TimeoutError:
                    if stop_result is not None and self._ports.stop_requested():
                        return stop_result
        finally:
            self._wait_for_scheduler_completion(completion, heartbeat_name)

    def _wait_for_scheduler_completion(
        self,
        completion: Future[int],
        heartbeat_name: str,
    ) -> None:
        while True:
            self._ports.heartbeat(heartbeat_name)
            with self._scheduler_condition:
                if (
                    self._scheduler_active is None
                    or self._scheduler_active.completion is not completion
                ):
                    return
                self._scheduler_condition.wait(
                    PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS
                )

    def _execute_refresh_trigger(self, trigger: TipRefreshTrigger) -> int:
        while True:
            self._raise_if_scheduler_superseded(trigger)
            self._ports.heartbeat("tip_refresh_scheduler")
            if self._ports.wait_for_execution_permit(
                PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS
            ):
                break
            if self._ports.stop_requested():
                raise ShutdownInProgress(
                    "tip refresh stopped while awaiting writer quiescence"
                )
        self._raise_if_scheduler_superseded(trigger)
        try:
            if trigger.snapshot is None:
                original_trigger = trigger
                capture_sequence = (
                    self.reserve_observation_sequence()
                    if trigger.fresh_capture_required
                    else trigger.observation_sequence
                )
                trigger = self._capture_poll_trigger(
                    observation_sequence=capture_sequence,
                    reasons=trigger.reasons,
                    pending_signal_token=trigger.pending_signal_token,
                    post_accept_block=trigger.post_accept_block,
                    existing=trigger,
                )
                trigger = self._replace_active_trigger(original_trigger, trigger)
        except (
            ShutdownInProgress,
            TemplateRefreshSuperseded,
            PayoutStatePublicationBlocked,
        ):
            raise
        except Exception:
            self.record_template_refresh_failure(self._monotonic())
            raise
        self._raise_if_scheduler_superseded(trigger)
        refresh_started = self._monotonic()
        publication_lock_acquired = False
        progress_refresh: RefreshActivityPort | None = None
        heartbeat_name = "tip_refresh_scheduler"
        observation_sequence = trigger.observation_sequence
        pending_signal_token = trigger.pending_signal_token
        snapshot = trigger.snapshot
        assert snapshot is not None
        poll_start_clients = trigger.poll_start_clients
        targets = trigger.initial_targets
        snapshot_changed = trigger.snapshot_changed
        try:
            progress_refresh = self._ports.start_progress_refresh()
            job_bundles = self._ports.job_bundles()
            ready_required = job_bundles.pool_readiness_latched()
            if ready_required and not trigger.ready_required:
                trigger = self._replace_active_trigger(
                    trigger,
                    dataclass_replace(trigger, ready_required=True),
                )
            payout = self._ports.payout_state()
            payout_generation_before = int(payout.snapshot().generation)
            refreshed = 0
            build_failures = 0
            first_delivery: float | None = None
            last_delivery: float | None = None
            self._raise_if_scheduler_superseded(trigger)
            self._raise_if_superseded(snapshot, observation_sequence)
            try:
                payout_only_same_tip = bool(
                    trigger.reasons == ("payout",)
                    and self.published_snapshot().tip_hash
                    == snapshot.bestblockhash
                )
                reconciled = (
                    not self._ports.chain_view_untrusted()
                    if payout_only_same_tip
                    else self._ports.ensure_reorg_tip(snapshot.bestblockhash)
                )
            except ShutdownInProgress:
                return 0
            except Exception as exc:
                raise TemplateRefreshBlocked(
                    "qbit reorg reconciliation failed before refresh preparation"
                ) from exc
            if not reconciled:
                raise TemplateRefreshBlocked(
                    "qbit chain view remained untrusted after reorg reconciliation"
                )
            payout_generation_after = int(payout.snapshot().generation)
            if payout_generation_after > trigger.payout_state_generation:
                trigger = self._replace_active_trigger(
                    trigger,
                    dataclass_replace(
                        trigger,
                        payout_state_generation=payout_generation_after,
                    ),
                )
            if payout_generation_after != payout_generation_before:
                targets = self._ports.delivery.select_targets(
                    snapshot,
                    refresh_all=False,
                )
                pending_signal_token = self.claim_pending()
            targets = self._ports.delivery.merge_poll_start_targets(
                targets,
                poll_start_clients,
                snapshot,
                refresh_all=snapshot_changed,
            )
            selected_clients = [target.client for target in targets]
            expected_active_jobs = {
                target.client: target.expected_active_job for target in targets
            }
            use_prepared_fanout = bool(
                selected_clients and job_bundles.ready_latched()
            )
            ready_mode = job_bundles.ready_latched()
            bundle: CachedJobBundle | None = None
            validation_token: TipRefreshValidationToken | None = None
            preactivated_cancel_event: FanoutCancellation | None = None
            prepared_executor: object | None = None
            if use_prepared_fanout:
                self._raise_if_scheduler_superseded(trigger)
                self._raise_if_superseded(snapshot, observation_sequence)
                try:
                    bundle = self.prepare_bundle(
                        snapshot,
                        priority_requested_monotonic=(
                            trigger.submitted_monotonic
                        ),
                    )
                except PayoutStatePublicationBlocked:
                    for _client in selected_clients:
                        self.record_client_result("skipped")
                    self.schedule_retry()
                    raise
                except TemplateRefreshBlocked:
                    for _client in selected_clients:
                        self.record_client_result("failed")
                    raise
                if bundle.payout_state_generation != payout_generation_after:
                    latest_payout = payout.snapshot()
                    if (
                        latest_payout.publication_blocked
                        or bundle.payout_state_generation
                        != latest_payout.generation
                    ):
                        self.schedule_retry()
                        raise TemplateRefreshSuperseded(
                            "payout state changed after refresh client selection; "
                            "immediate retry scheduled"
                        )
                    payout_generation_after = int(latest_payout.generation)
                    trigger = self._replace_active_trigger(
                        trigger,
                        dataclass_replace(
                            trigger,
                            payout_state_generation=payout_generation_after,
                        ),
                    )
                    targets = self._ports.delivery.select_targets(
                        snapshot,
                        refresh_all=False,
                    )
                    targets = self._ports.delivery.merge_poll_start_targets(
                        targets,
                        poll_start_clients,
                        snapshot,
                        refresh_all=snapshot_changed,
                    )
                    selected_clients = [target.client for target in targets]
                    expected_active_jobs = {
                        target.client: target.expected_active_job
                        for target in targets
                    }
                    pending_signal_token = self.claim_pending()

            self._raise_if_scheduler_superseded(trigger)
            self._raise_if_superseded(snapshot, observation_sequence)
            while not self._refresh_lock.acquire(
                timeout=PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS
            ):
                self._ports.heartbeat(heartbeat_name)
                if self._ports.stop_requested():
                    return 0
                self._probe_tip_while_waiting()
                self._raise_if_scheduler_superseded(trigger)
                self._raise_if_superseded(snapshot, observation_sequence)
            publication_lock_acquired = True
            payout_snapshot = payout.snapshot()
            current_payout_artifact = payout_snapshot.published.artifact
            if (
                payout_snapshot.generation != payout_generation_after
                or (
                    bundle is not None
                    and (
                        current_payout_artifact is None
                        or bundle.build_key is None
                        or bundle.build_key.payout_artifact_sha256
                        != current_payout_artifact.prior_balances_sha256
                    )
                )
            ):
                self.schedule_retry()
                raise TemplateRefreshBlocked(
                    "complete build key changed before refresh publication"
                )

            if use_prepared_fanout:
                assert bundle is not None
                self._raise_if_scheduler_superseded(trigger)
                prepared_executor = self.executor()
                try:
                    parent_hash = self._fetch_parent_hash(snapshot.bestblockhash)
                except Exception:
                    parent_hash = None
                validation_token = self.validate_prepared(
                    bundle,
                    snapshot,
                    observation_sequence,
                )
                preactivated_cancel_event = self.publish_prepared(
                    validation_token,
                    bundle,
                    snapshot,
                    parent_hash=parent_hash,
                )
            else:
                if not self.publish_tip(
                    snapshot.bestblockhash,
                    observation_sequence=observation_sequence,
                    publish_refresh_observation=True,
                    published_snapshot=snapshot,
                ):
                    raise TemplateRefreshSuperseded(
                        "tip/template poll was superseded by a newer tip observation"
                    )
                self._ports.prune_evicted_jobs(None, False)
                if not self.snapshot_current(snapshot, observation_sequence):
                    raise TemplateRefreshSuperseded(
                        "tip/template poll was superseded before snapshot publication"
                    )

            targets, dropped = self._ports.delivery.revalidate_targets(
                targets,
                snapshot,
            )
            selected_clients = [target.client for target in targets]
            for result in dropped:
                self.record_client_result(result)

            if bundle is not None and not bundle.collection_only:
                if bundle.payout_state_generation == payout.snapshot().generation:
                    job_bundles.set_prepared_ready(snapshot, bundle)

            if (
                not use_prepared_fanout
                and selected_clients
                and job_bundles.ready_latched()
            ):
                self.mark_pending(observation_sequence)
                self.schedule_retry()
                raise TemplateRefreshBlocked(
                    "current clients appeared after build selection; retry scheduled"
                )
            self._refresh_lock.release()
            publication_lock_acquired = False

            progress_eligible_client = bool(self._ports.delivery.eligible_clients())
            if use_prepared_fanout or not progress_eligible_client:
                self._ports.publish_progress_work(snapshot, payout_generation_after)

            if not ready_mode and not progress_eligible_client:
                self.retain_collection_refresh(
                    snapshot,
                    observation_sequence,
                    payout_generation_after,
                )

            if use_prepared_fanout:
                assert bundle is not None
                (
                    refreshed,
                    first_delivery,
                    last_delivery,
                    build_failures,
                ) = self.fanout_prepared(
                    selected_clients,
                    bundle,
                    snapshot,
                    observation_sequence=observation_sequence,
                    validation_token=validation_token,
                    preactivated_cancel_event=preactivated_cancel_event,
                    executor=prepared_executor,
                    expected_active_jobs=expected_active_jobs,
                    heartbeat_name=heartbeat_name,
                )
            else:
                for client in selected_clients:
                    if self._ports.stop_requested():
                        break
                    self._ports.heartbeat(heartbeat_name)
                    result = self._ports.delivery.deliver_collection(
                        client,
                        snapshot,
                        observation_sequence,
                    )
                    self.record_client_result(result.result)
                    if result.result == "sent":
                        refreshed += 1
                        assert result.delivered_monotonic is not None
                        first_delivery = (
                            result.delivered_monotonic
                            if first_delivery is None
                            else min(first_delivery, result.delivered_monotonic)
                        )
                        last_delivery = (
                            result.delivered_monotonic
                            if last_delivery is None
                            else max(last_delivery, result.delivered_monotonic)
                        )
                    elif result.result == "failed":
                        build_failures += 1

                if not ready_mode and not self._ports.delivery.eligible_clients():
                    self.retain_collection_refresh(
                        snapshot,
                        observation_sequence,
                        payout_generation_after,
                    )
                    self._ports.publish_progress_work(
                        snapshot,
                        payout_generation_after,
                    )

                if selected_clients:
                    try:
                        post_fanout_tip = str(
                            self._ports.rpc_call("getbestblockhash", None)
                        )
                    except Exception as exc:
                        self.schedule_retry()
                        raise TemplateRefreshBlocked(
                            "qbit tip validation failed after sequential refresh; "
                            "immediate retry scheduled"
                        ) from exc
                    if post_fanout_tip != snapshot.bestblockhash:
                        self.schedule_retry()
                        raise TemplateRefreshSuperseded(
                            "qbit tip changed during sequential refresh; "
                            "immediate retry scheduled "
                            f"expected={snapshot.bestblockhash} current={post_fanout_tip}"
                        )
                    if int(payout.snapshot().generation) != payout_generation_after:
                        self.schedule_retry()
                        raise TemplateRefreshSuperseded(
                            "payout state changed during sequential refresh; "
                            "immediate retry scheduled"
                        )

            if refreshed == 0 and build_failures:
                raise TemplateRefreshBlocked(
                    f"job builds failed for {build_failures} client(s); "
                    "no refreshed work was issued"
                )
            if refreshed:
                self.add_refresh_jobs(refreshed)
                assert first_delivery is not None and last_delivery is not None
                self.observe_seconds(
                    "first_delivery",
                    first_delivery - refresh_started,
                )
                self.observe_seconds(
                    "last_delivery",
                    last_delivery - refresh_started,
                )
            has_followup = self._scheduler_has_followup(trigger)
            pending_cleared = has_followup or self.clear_pending_for_completed_refresh(
                snapshot,
                observation_sequence,
                payout_generation_after,
                pending_signal_token,
            )
            if not pending_cleared:
                pending_signal_token = None
                self.schedule_retry()
                raise TemplateRefreshSuperseded(
                    "tip or payout state changed before refresh completion; "
                    "immediate retry scheduled"
                )
            with self._state_lock:
                self._last_successful_refresh_monotonic = self._monotonic()
                self._failure_started_monotonic = None
            self.clear_failure_holdoff()
            self._ports.observe_progress_tip_poll(snapshot)
            return refreshed
        except (TemplateRefreshSuperseded, PayoutStatePublicationBlocked):
            self.note_attempt_failed(snapshot.bestblockhash)
            raise
        except Exception:
            self.record_template_refresh_failure(self._monotonic())
            self.note_attempt_failed(snapshot.bestblockhash)
            raise
        finally:
            if publication_lock_acquired:
                self._refresh_lock.release()
            if progress_refresh is not None:
                progress_refresh.finish()
            self.observe_seconds("refresh", self._monotonic() - refresh_started)

    def refresh_after_pending_accepted_block(
        self,
        client: object,
        *,
        heartbeat_name: str = "qbit_blockpoll",
    ) -> int:
        block = self._ports.delivery.take_post_accept_refresh(client)
        if block is None:
            return 0
        block_height, block_hash = block
        return self.refresh_after_accepted_block(
            block_height=block_height,
            block_hash=block_hash,
            heartbeat_name=heartbeat_name,
        )

    def refresh_after_accepted_block(
        self,
        *,
        block_height: int,
        block_hash: str,
        heartbeat_name: str = "qbit_blockpoll",
    ) -> int:
        try:
            self._ports.heartbeat(heartbeat_name)
            completion = self.submit_post_accept_trigger(
                block_height=block_height,
                block_hash=block_hash,
            )
        except (TemplateRefreshSuperseded, PayoutStatePublicationBlocked):
            self.schedule_retry()
            return 0
        except Exception:
            self.schedule_retry()
            with self._state_lock:
                self._post_accept_refresh_failure_count += 1
            print(
                "prism coordinator: post-accept clean job refresh failed after "
                f"direct PRISM block height={block_height} hash={block_hash}",
                flush=True,
            )
            traceback.print_exc()
            return 0
        try:
            return self._await_scheduler_result(
                completion,
                heartbeat_name,
                stop_result=0,
            )
        except (
            ShutdownInProgress,
            TemplateRefreshSuperseded,
            PayoutStatePublicationBlocked,
        ):
            # The scheduler owns retry and failure classification after
            # admission; coordination churn is not a failed notification.
            return 0
        except Exception:
            # The scheduler already recorded and logged this post-accept
            # failure against the merged reporting trigger.
            return 0

    def template_refresh_failure_expired(self, now: float) -> bool:
        if self.config.failure_exit_seconds <= 0:
            return False
        with self._state_lock:
            started = self._failure_started_monotonic
        return started is not None and now - started >= self.config.failure_exit_seconds

    def record_template_refresh_failure(self, now: float) -> None:
        if self.config.failure_exit_seconds <= 0:
            return
        with self._state_lock:
            if self._failure_started_monotonic is None:
                self._failure_started_monotonic = now

    def blockwait_once(self, known_tip: str) -> str:
        max_rpc_timeout = max(1.0, self.config.watchdog_timeout_seconds * 0.8)
        timeout_seconds = min(
            self.config.blockwait_timeout_seconds,
            max(1.0, max_rpc_timeout - 1.0),
        )
        result = self._ports.rpc_call_with_timeout(
            "waitfornewblock",
            [max(1, int(timeout_seconds * 1000)), known_tip],
            timeout_seconds + 10.0,
        )
        if isinstance(result, Mapping):
            new_tip = str(result.get("hash", "") or "")
            if new_tip:
                return new_tip
        return known_tip

    def wait_for_blockpoll_trigger(self) -> bool:
        remaining = self.config.blockpoll_seconds
        while remaining > 0:
            if self._ports.stop_requested():
                return False
            holdoff = self.failure_holdoff_remaining()
            if holdoff <= 0 and self.consume_retry():
                return not self._ports.stop_requested()
            wait_seconds = min(remaining, 0.25)
            if holdoff > 0:
                self._ports.heartbeat("qbit_blockpoll")
                wait_seconds = min(wait_seconds, holdoff, 0.05)
                self._ports.wait_for_stop(wait_seconds)
            else:
                self._retry_event.wait(wait_seconds)
            remaining -= wait_seconds
        while not self._ports.stop_requested():
            holdoff = self.failure_holdoff_remaining()
            if holdoff <= 0:
                break
            self._ports.heartbeat("qbit_blockpoll")
            self._ports.wait_for_stop(min(holdoff, 0.05))
        self.consume_retry()
        return not self._ports.stop_requested()

    def blockpoll_loop(self) -> None:
        while self.wait_for_blockpoll_trigger():
            self._ports.heartbeat("qbit_blockpoll")
            try:
                self.poll_once()
            except ShutdownInProgress:
                return
            except (TemplateRefreshSuperseded, PayoutStatePublicationBlocked) as exc:
                print(
                    f"prism coordinator: tip/template refresh superseded; retrying: {exc}",
                    flush=True,
                )
            except Exception:
                print("prism coordinator: qbit tip/template poll failed", flush=True)
                traceback.print_exc()
                if self.template_refresh_failure_expired(self._monotonic()):
                    print(
                        "prism coordinator: template refresh failure budget exhausted; "
                        "exiting non-zero so the restart policy recovers the process",
                        flush=True,
                    )
                    self._ports.hard_exit(1)

    def blockwait_loop(self) -> None:
        known_tip: str | None = None
        while not self._ports.stop_requested():
            self._ports.heartbeat("qbit_blockwait")
            try:
                if known_tip is None:
                    observed_tip = str(
                        self._ports.rpc_call("getbestblockhash", None)
                    )
                    self.observe_tip(
                        observed_tip,
                    )
                    known_tip = observed_tip
                new_tip = self.blockwait_once(known_tip)
                if new_tip == known_tip:
                    if self._ports.wait_for_stop(0.25):
                        return
                    continue
                # Advance the cursor before notification. A bookkeeping
                # failure must not rediscover this transition in a loop.
                known_tip = new_tip
                try:
                    self.observe_tip(new_tip)
                finally:
                    self.schedule_retry()
                print(
                    f"prism coordinator: blockwait saw new tip {new_tip}; "
                    "single-flight refresh scheduled",
                    flush=True,
                )
            except Exception as exc:
                if known_tip is not None and self.blockwait_unsupported(exc):
                    print(
                        "prism coordinator: waitfornewblock unavailable on this qbitd; "
                        "tip detection falls back to blockpoll only",
                        flush=True,
                    )
                    self._ports.remove_heartbeat("qbit_blockwait")
                    return
                print("prism coordinator: blockwait pass failed", flush=True)
                traceback.print_exc()
                if self._ports.wait_for_stop(
                    min(5.0, self.config.blockpoll_seconds)
                ):
                    return

    @staticmethod
    def blockwait_unsupported(exc: Exception) -> bool:
        detail = str(exc).lower()
        return any(
            marker in detail
            for marker in (
                "-32601",
                "-32602",
                "method not found",
                "unknown method",
                "invalid params",
                "invalid parameter",
                "wrong number of",
                "too many parameters",
                "incorrect number of",
            )
        )

    def executor(self) -> _BoundedPriorityExecutor:
        with self._executor_lock:
            if self._executor_shutdown:
                raise RuntimeError("tip refresh executor is shut down")
            if self._executor is None:
                self._executor = _BoundedPriorityExecutor(
                    max_workers=self.config.max_workers,
                    max_queue_size=self._ports.delivery_queue_limit(),
                )
            return self._executor

    def cancel_active(self) -> None:
        pending: _ScheduledTipRefresh | None
        with self._scheduler_condition:
            self._scheduler_admission_open = False
            self._scheduler_cancel_active = True
            pending = self._scheduler_pending
            self._scheduler_pending = None
            self._scheduler_condition.notify_all()
        if pending is not None and not pending.completion.done():
            pending.completion.set_exception(
                ShutdownInProgress("pending tip refresh cancelled by shutdown")
            )
        self._cancel_active_fanout()

    def shutdown(self) -> bool:
        self.cancel_active()
        with self._scheduler_condition:
            worker = self._scheduler_worker
        if worker is not None and worker is not threading.current_thread():
            worker.join(timeout=1.0)
        if worker is not None and worker.is_alive():
            return False
        with self._executor_lock:
            executor = self._executor
            self._executor = None
            self._executor_shutdown = True
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)
        return True

    def observe_seconds(self, name: str, elapsed_seconds: float) -> None:
        with self._metrics_lock:
            histogram = self._histograms[name]
            histogram["count"] = int(histogram["count"]) + 1
            histogram["sum"] = float(histogram["sum"]) + elapsed_seconds
            buckets = histogram["buckets"]
            assert isinstance(buckets, dict)
            for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS:
                if elapsed_seconds <= bucket:
                    buckets[bucket] = int(buckets.get(bucket, 0)) + 1

    def observe_build_phase(self, phase: str, elapsed_seconds: float) -> None:
        if phase not in PRISM_TIP_REFRESH_BUILD_PHASES:
            raise ValueError(f"unknown tip refresh build phase: {phase}")
        with self._metrics_lock:
            histogram = self._phase_histograms[phase]
            histogram["count"] = int(histogram["count"]) + 1
            histogram["sum"] = float(histogram["sum"]) + max(0.0, elapsed_seconds)
            buckets = histogram["buckets"]
            assert isinstance(buckets, dict)
            for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS:
                if elapsed_seconds <= bucket:
                    buckets[bucket] = int(buckets.get(bucket, 0)) + 1

    def record_ipc_bytes(self, direction: str, byte_count: int) -> None:
        if direction not in self._ipc_bytes:
            raise ValueError(f"unknown tip refresh IPC direction: {direction}")
        with self._metrics_lock:
            self._ipc_bytes[direction] += max(0, int(byte_count))

    def record_client_result(self, result: str) -> None:
        if result not in PRISM_TIP_REFRESH_RESULTS:
            raise ValueError(f"unknown tip refresh result: {result}")
        with self._metrics_lock:
            self._client_counts[result] += 1

    def record_cancellation(self, stage: str) -> None:
        if stage not in PRISM_TIP_REFRESH_CANCELLATION_STAGES:
            raise ValueError(f"unknown tip refresh cancellation stage: {stage}")
        with self._metrics_lock:
            self._cancellation_counts[stage] += 1

    def future_started(self) -> None:
        with self._metrics_lock:
            self._inflight += 1

    def future_finished(self, _future: Future[RefreshResult] | None = None) -> None:
        with self._metrics_lock:
            self._inflight = max(0, self._inflight - 1)

    def record_superseded_result(self) -> None:
        with self._metrics_lock:
            self._superseded_results += 1

    def record_worker_failure(self) -> None:
        with self._metrics_lock:
            self._worker_failures += 1

    def record_worker_restart(self) -> None:
        with self._metrics_lock:
            self._worker_restarts += 1

    def record_singleflight_hit(self) -> None:
        with self._metrics_lock:
            self._singleflight_hits += 1

    def add_refresh_jobs(self, count: int) -> None:
        with self._state_lock:
            self._refresh_job_count += max(0, int(count))

    def set_build_gauges(self, *, inflight: int, queue_depth: int) -> None:
        with self._metrics_lock:
            self._build_inflight = int(inflight)
            self._build_queue_depth = int(queue_depth)

    def metrics_snapshot(self) -> dict[str, object]:
        with self._scheduler_condition:
            trigger_queue_depth = int(self._scheduler_pending is not None)
        with self._executor_lock:
            executor_workers = self.config.max_workers if self._executor is not None else 0
        with self._metrics_lock:
            return {
                "histograms": {
                    name: {
                        "buckets": dict(value["buckets"]),
                        "sum": float(value["sum"]),
                        "count": int(value["count"]),
                    }
                    for name, value in self._histograms.items()
                },
                "phase_histograms": {
                    name: {
                        "buckets": dict(value["buckets"]),
                        "sum": float(value["sum"]),
                        "count": int(value["count"]),
                    }
                    for name, value in self._phase_histograms.items()
                },
                "client_counts": dict(self._client_counts),
                "cancellation_counts": dict(self._cancellation_counts),
                "inflight": self._inflight,
                "executor_workers": executor_workers,
                "build_inflight": self._build_inflight,
                "build_queue_depth": self._build_queue_depth,
                "singleflight_hits": self._singleflight_hits,
                "superseded_results": self._superseded_results,
                "worker_failures": self._worker_failures,
                "worker_restarts": self._worker_restarts,
                "ipc_bytes": dict(self._ipc_bytes),
                "trigger_queue_depth": trigger_queue_depth,
                "trigger_queue_capacity": PRISM_TIP_REFRESH_TRIGGER_PENDING_CAPACITY,
                "trigger_coalesces": self._trigger_coalesces,
                "trigger_supersessions": self._trigger_supersessions,
                "trigger_latency": {
                    "buckets": dict(self._trigger_latency["buckets"]),
                    "sum": float(self._trigger_latency["sum"]),
                    "count": int(self._trigger_latency["count"]),
                },
            }

    def metrics_lines(self) -> list[str]:
        snapshot = self.metrics_snapshot()
        histograms = snapshot["histograms"]
        phase_histograms = snapshot["phase_histograms"]
        client_counts = snapshot["client_counts"]
        cancellation_counts = snapshot["cancellation_counts"]
        ipc_bytes = snapshot["ipc_bytes"]
        trigger_latency = snapshot["trigger_latency"]
        assert isinstance(histograms, dict)
        assert isinstance(phase_histograms, dict)
        assert isinstance(client_counts, dict)
        assert isinstance(cancellation_counts, dict)
        assert isinstance(ipc_bytes, dict)
        assert isinstance(trigger_latency, dict)

        metric_names = {
            "refresh": "qbit_prism_tip_refresh_seconds",
            "bundle_build": "qbit_prism_tip_refresh_bundle_build_seconds",
            "first_delivery": "qbit_prism_tip_refresh_first_delivery_seconds",
            "last_delivery": "qbit_prism_tip_refresh_last_delivery_seconds",
        }
        descriptions = {
            "refresh": "Full qbit tip/template refresh pass wall time.",
            "bundle_build": "Shared ready-pool refresh bundle preparation wall time.",
            "first_delivery": "Tip observation to first successful client delivery.",
            "last_delivery": "Tip observation to last successful client delivery.",
        }
        lines: list[str] = []
        for name, metric_name in metric_names.items():
            histogram = histograms[name]
            assert isinstance(histogram, dict)
            buckets = histogram["buckets"]
            assert isinstance(buckets, dict)
            lines.extend(
                [
                    f"# HELP {metric_name} {descriptions[name]}",
                    f"# TYPE {metric_name} histogram",
                    *[
                        f'{metric_name}_bucket{{le="{bucket:g}"}} {int(buckets.get(bucket, 0))}'
                        for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS
                    ],
                    f'{metric_name}_bucket{{le="+Inf"}} {histogram["count"]}',
                    f'{metric_name}_sum {float(histogram["sum"]):.6f}',
                    f'{metric_name}_count {histogram["count"]}',
                ]
            )
        phase_metric_name = "qbit_prism_tip_refresh_bundle_phase_seconds"
        lines.extend(
            [
                "# HELP qbit_prism_tip_refresh_bundle_phase_seconds Shared bundle-build phase wall time.",
                "# TYPE qbit_prism_tip_refresh_bundle_phase_seconds histogram",
            ]
        )
        for phase in PRISM_TIP_REFRESH_BUILD_PHASES:
            histogram = phase_histograms[phase]
            assert isinstance(histogram, dict)
            buckets = histogram["buckets"]
            assert isinstance(buckets, dict)
            lines.extend(
                [
                    *[
                        f'{phase_metric_name}_bucket{{phase="{phase}",le="{bucket:g}"}} {int(buckets.get(bucket, 0))}'
                        for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS
                    ],
                    f'{phase_metric_name}_bucket{{phase="{phase}",le="+Inf"}} {histogram["count"]}',
                    f'{phase_metric_name}_sum{{phase="{phase}"}} {float(histogram["sum"]):.6f}',
                    f'{phase_metric_name}_count{{phase="{phase}"}} {histogram["count"]}',
                ]
            )
        lines.extend(
            [
                "# HELP qbit_prism_tip_refresh_clients_total Client outcomes from tip/template refresh passes.",
                "# TYPE qbit_prism_tip_refresh_clients_total counter",
                *[
                    f'qbit_prism_tip_refresh_clients_total{{result="{result}"}} {int(client_counts.get(result, 0))}'
                    for result in PRISM_TIP_REFRESH_RESULTS
                ],
                "# HELP qbit_prism_tip_refresh_cancellations_total Obsolete prepared refresh tasks canceled before delivery admission.",
                "# TYPE qbit_prism_tip_refresh_cancellations_total counter",
                *[
                    f'qbit_prism_tip_refresh_cancellations_total{{stage="{stage}"}} {int(cancellation_counts.get(stage, 0))}'
                    for stage in PRISM_TIP_REFRESH_CANCELLATION_STAGES
                ],
                "# HELP qbit_prism_tip_refresh_inflight Prepared refresh client tasks currently queued or running.",
                "# TYPE qbit_prism_tip_refresh_inflight gauge",
                f'qbit_prism_tip_refresh_inflight {int(snapshot["inflight"])}',
                "# HELP qbit_prism_tip_refresh_executor_workers Configured persistent refresh executor workers, or zero before creation.",
                "# TYPE qbit_prism_tip_refresh_executor_workers gauge",
                f'qbit_prism_tip_refresh_executor_workers {int(snapshot["executor_workers"])}',
                "# HELP qbit_prism_tip_refresh_bundle_inflight Shared bundle builds currently running.",
                "# TYPE qbit_prism_tip_refresh_bundle_inflight gauge",
                f'qbit_prism_tip_refresh_bundle_inflight {int(snapshot["build_inflight"])}',
                "# HELP qbit_prism_tip_refresh_bundle_queue_depth Shared bundle callers waiting on bounded build admission or an identical single-flight.",
                "# TYPE qbit_prism_tip_refresh_bundle_queue_depth gauge",
                f'qbit_prism_tip_refresh_bundle_queue_depth {int(snapshot["build_queue_depth"])}',
                "# HELP qbit_prism_tip_refresh_bundle_singleflight_hits_total Shared bundle callers coalesced behind an identical build.",
                "# TYPE qbit_prism_tip_refresh_bundle_singleflight_hits_total counter",
                f'qbit_prism_tip_refresh_bundle_singleflight_hits_total {int(snapshot["singleflight_hits"])}',
                "# HELP qbit_prism_tip_refresh_bundle_superseded_results_total Completed or canceled shared bundles discarded after supersession.",
                "# TYPE qbit_prism_tip_refresh_bundle_superseded_results_total counter",
                f'qbit_prism_tip_refresh_bundle_superseded_results_total {int(snapshot["superseded_results"])}',
                "# HELP qbit_prism_tip_refresh_builder_worker_failures_total Audit-builder subprocess failures.",
                "# TYPE qbit_prism_tip_refresh_builder_worker_failures_total counter",
                f'qbit_prism_tip_refresh_builder_worker_failures_total {int(snapshot["worker_failures"])}',
                "# HELP qbit_prism_tip_refresh_builder_worker_restarts_total Long-lived builder worker restarts; zero for the inline subprocess design.",
                "# TYPE qbit_prism_tip_refresh_builder_worker_restarts_total counter",
                f'qbit_prism_tip_refresh_builder_worker_restarts_total {int(snapshot["worker_restarts"])}',
                "# HELP qbit_prism_tip_refresh_builder_ipc_bytes_total Bytes copied across audit-builder subprocess IPC.",
                "# TYPE qbit_prism_tip_refresh_builder_ipc_bytes_total counter",
                *[
                    f'qbit_prism_tip_refresh_builder_ipc_bytes_total{{direction="{direction}"}} {int(ipc_bytes.get(direction, 0))}'
                    for direction in ("input", "output")
                ],
            ]
        )
        trigger_latency_buckets = trigger_latency["buckets"]
        assert isinstance(trigger_latency_buckets, dict)
        lines.extend(
            [
                "# HELP qbit_prism_tip_refresh_trigger_queue_depth Immutable refresh triggers waiting in the fixed latest-wins pending slot.",
                "# TYPE qbit_prism_tip_refresh_trigger_queue_depth gauge",
                f'qbit_prism_tip_refresh_trigger_queue_depth {int(snapshot["trigger_queue_depth"])}',
                "# HELP qbit_prism_tip_refresh_trigger_queue_capacity Fixed pending trigger capacity, excluding the active refresh.",
                "# TYPE qbit_prism_tip_refresh_trigger_queue_capacity gauge",
                f'qbit_prism_tip_refresh_trigger_queue_capacity {int(snapshot["trigger_queue_capacity"])}',
                "# HELP qbit_prism_tip_refresh_trigger_coalesces_total Trigger admissions merged with already active or pending work.",
                "# TYPE qbit_prism_tip_refresh_trigger_coalesces_total counter",
                f'qbit_prism_tip_refresh_trigger_coalesces_total {int(snapshot["trigger_coalesces"])}',
                "# HELP qbit_prism_tip_refresh_trigger_supersessions_total Trigger admissions that replaced older pending or active requirements.",
                "# TYPE qbit_prism_tip_refresh_trigger_supersessions_total counter",
                f'qbit_prism_tip_refresh_trigger_supersessions_total {int(snapshot["trigger_supersessions"])}',
                "# HELP qbit_prism_tip_refresh_trigger_latency_seconds Trigger admission to scheduler execution latency.",
                "# TYPE qbit_prism_tip_refresh_trigger_latency_seconds histogram",
                *[
                    'qbit_prism_tip_refresh_trigger_latency_seconds_bucket'
                    f'{{le="{bucket:g}"}} {int(trigger_latency_buckets.get(bucket, 0))}'
                    for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS
                ],
                "qbit_prism_tip_refresh_trigger_latency_seconds_bucket"
                f'{{le="+Inf"}} {trigger_latency["count"]}',
                "qbit_prism_tip_refresh_trigger_latency_seconds_sum "
                f'{float(trigger_latency["sum"]):.6f}',
                "qbit_prism_tip_refresh_trigger_latency_seconds_count "
                f'{trigger_latency["count"]}',
            ]
        )
        return lines
