#!/usr/bin/env python3
"""PRISM tip observation, publication, and bounded refresh ownership.

The service owns the R1 domain: tip observation/publication state, the
latest-wins refresh waves and their epoch convergence authority (#101),
refresh pending/retry signaling, failure holdoff pacing, fanout delivery
coordination, blockpoll/blockwait loops, retained collection refreshes, and
the tip-refresh metric state.

It never imports ``prism_coordinator``.  Every cross-domain fact -- payout
state, job-bundle preparation, session membership, delivery submission,
reorg reconciliation, progress health, watchdog heartbeats, live
configuration attributes -- is reached through the :class:`TipRefreshRuntime`
typed port, resolved at call time so the historical coordinator monkeypatch
seams (including the instance-level facade patches used by the current test
suite) keep intercepting exactly as before the extraction.  Reconcile
flights/memo/prefetch and share-ack timing remain coordinator-owned at this
layer (they belong to later stack owners); R1 reaches them only through
call-time runtime ports.
"""

from __future__ import annotations

from concurrent.futures import (
    FIRST_COMPLETED,
    CancelledError as FuturesCancelledError,
    Future,
    ThreadPoolExecutor,
    wait,
)
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
import os
import random
import threading
import time
import traceback
from typing import Any, Protocol

from lab.prism.bounded_executor import _BoundedPriorityExecutor
from lab.prism.coordinator_config import (
    DEFAULT_PRISM_BLOCKPOLL_SECONDS,
    DEFAULT_PRISM_BLOCKWAIT_TIMEOUT_SECONDS,
    DEFAULT_PRISM_REORG_RECONCILE_CACHE_SECONDS,
    DEFAULT_PRISM_SUBMIT_TIP_MAX_AGE_SECONDS,
    DEFAULT_PRISM_TEMPLATE_MAX_AGE_SECONDS,
    DEFAULT_PRISM_TIP_REFRESH_FAILURE_HOLDOFF_SECONDS,
)
from lab.prism.job_bundle import CachedJobBundle, JobBuildKey, JobBuildSuperseded
from lab.prism.template_artifacts import (
    CachedTemplateArtifacts,
    PayoutStatePublicationBlocked,
    QbitTipTemplateSnapshot,
    TemplateRefreshBlocked,
    TemplateRefreshSuperseded,
    qbit_template_fingerprint,
)


# Owner-local copies of still-coordinator-visible constants; compatibility
# duplicates by design (the coordinator's import source for the admission
# poll interval is job_delivery.py, and the payout supersession-retry
# default remains a coordinator/P1 definition at this layer).
PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS = 0.05
DEFAULT_PRISM_PAYOUT_RECONCILE_SUPERSESSION_RETRIES = 8
# Fraction of the holdoff added as random jitter so coordinators sharing one
# qbitd do not phase-lock their blocked-refresh re-attempts.
PRISM_TIP_REFRESH_FAILURE_HOLDOFF_JITTER_FRACTION = 0.25
PRISM_TIP_REFRESH_REENTRY_BACKOFF_SECONDS = 0.05
PRISM_TIP_REFRESH_WAVE_PASS_BUDGET = 64
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
    "reorg_reconcile",
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
PRISM_TIP_REFRESH_WAVE_OUTCOMES = (
    "completed",
    "fanout_superseded",
    "build_superseded",
    "payout_blocked",
    "trust_blocked",
    "shutdown",
    "error",
)
PRISM_TIP_REFRESH_COVERAGE_TARGETS = (
    ("50", Decimal("0.50")),
    ("95", Decimal("0.95")),
    ("99", Decimal("0.99")),
)


@dataclass(frozen=True)
class RetainedCollectionRefresh:
    """Current immutable preparation waiting for a collection identity."""

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
    epoch_sequence: int = 0


@dataclass
class _TipRefreshEpochCoverage:
    """Client cohort snapshot used to observe fixed-cardinality coverage latency."""

    sequence: int
    tip_hash: str
    payout_state_generation: int
    started_monotonic: float
    client_weights: dict[int, Decimal]
    total_weight: Decimal
    delivered_weight: Decimal = Decimal(0)
    delivered_clients: set[int] = field(default_factory=set)
    recorded_targets: set[str] = field(default_factory=set)


class FanoutCancellation:
    """Cancel a fanout without racing already-admitted deliveries.

    ``cancel`` closes admission without waiting, so workers can call it while
    holding a client lock. The fanout coordinator calls ``set`` outside client
    locks to wait for deliveries that already passed the final gate.
    """

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


class _TipRefreshFanoutSuperseded(TemplateRefreshSuperseded):
    """A published fanout lost to a newer tip-refresh epoch."""


class _TipRefreshTrustBlocked(TemplateRefreshBlocked):
    """Refresh work stopped because the live chain view was not trusted."""


class TipRefreshRuntime(Protocol):
    """Typed port over the coordinator, resolved at call time.

    Every member is looked up on the live coordinator object when used, so
    instance monkeypatches (``server.maybe_send_job = ...`` and friends) and
    coordinator-owned live configuration attributes keep working exactly as
    before the extraction.  Owner facades route back into this service; that
    round trip is deliberate: it is what preserves the current white-box
    patch surface until later stack layers repoint those tests.  The legacy
    R1 field names also resolve here -- coordinator class descriptors route
    them to this service's single mutable copy.
    """

    # Cross-domain objects and live configuration attributes.
    _job_cache_lock: Any
    _payout_artifact_executor_lock: Any
    _payout_ledger_append_invalidation_epoch: Any
    _payout_state_delivery_gate: Any
    _payout_state_generation: Any
    _payout_state_publication_blocked: Any
    _payout_state_source: Any
    _pool_ready_latched: Any
    _prepared_ready_bundle: Any
    _prepared_ready_snapshot: Any
    _progress_has_published_work: Any
    _progress_health_lock: Any
    _progress_publication_divergence_since_monotonic: Any
    _progress_published_payout_generation: Any
    _progress_published_template_fingerprint: Any
    _progress_refresh_signal_pending: Any
    _published_payout_state: Any
    _serve_builder_metrics_lock: Any
    _template_artifacts: Any
    blockpoll_seconds: Any
    blockwait_timeout_seconds: Any
    clients: Any
    job_build_failure_count: Any
    job_preparation_pending: Any
    lock: Any
    payout_artifact_event_counts: Any
    payout_reconcile_supersession_retries: Any
    reorg_reconcile_cache_seconds: Any
    reorg_reconciler_enabled: Any
    rpc: Any
    serve_builder_counts: Any
    serve_builder_window_cache_counts: Any
    stop_event: Any
    submit_tip_max_age_seconds: Any
    template_cache_seconds: Any
    template_refresh_failure_exit_seconds: Any
    tip_refresh_epoch_fanout: Any
    tip_refresh_failure_holdoff_seconds: Any
    tip_refresh_max_workers: Any
    watchdog_timeout_seconds: Any

    def _artifacts_buildable_locked(self, *args: Any, **kwargs: Any) -> Any: ...

    def _begin_job_build_priority_preparation(self, *args: Any, **kwargs: Any) -> Any: ...

    def _blockwait_unsupported(self, *args: Any, **kwargs: Any) -> Any: ...

    def _cancel_obsolete_job_builds(self, *args: Any, **kwargs: Any) -> Any: ...

    def _cancel_obsolete_job_bundle_builds(self, *args: Any, **kwargs: Any) -> Any: ...

    def _clear_coordination_blocked_streak(self, *args: Any, **kwargs: Any) -> Any: ...

    def _discard_stale_reconcile_prefetch(self, *args: Any, **kwargs: Any) -> Any: ...

    def _ensure_job_cache_state(self, *args: Any, **kwargs: Any) -> Any: ...

    def _ensure_tip_refresh_state(self, *args: Any, **kwargs: Any) -> Any: ...

    def _evict_reorg_reconcile_memo_for_new_tip_locked(self, *args: Any, **kwargs: Any) -> Any: ...

    def _finish_job_build_priority_preparation(self, *args: Any, **kwargs: Any) -> Any: ...

    def _join_reconcile_prefetch_bounded(self, *args: Any, **kwargs: Any) -> Any: ...

    def _newest_observed_tip_locked(self, *args: Any, **kwargs: Any) -> Any: ...

    def _note_tip_observation_for_candidates(self, *args: Any, **kwargs: Any) -> Any: ...

    def _progress_bundle_build_finished(self, *args: Any, **kwargs: Any) -> Any: ...

    def _progress_bundle_build_started(self, *args: Any, **kwargs: Any) -> Any: ...

    def _progress_note_refresh_pending(self, *args: Any, **kwargs: Any) -> Any: ...

    def _progress_refresh_finished(self, *args: Any, **kwargs: Any) -> Any: ...

    def _progress_refresh_started(self, *args: Any, **kwargs: Any) -> Any: ...

    def _reconcile_snapshot_tip_bounded(self, *args: Any, **kwargs: Any) -> Any: ...

    def _record_coordination_blocked_refresh(self, *args: Any, **kwargs: Any) -> Any: ...

    def _record_heartbeat(self, *args: Any, **kwargs: Any) -> Any: ...

    def _record_job_cache_event(self, *args: Any, **kwargs: Any) -> Any: ...

    def _record_progress_publication(self, *args: Any, **kwargs: Any) -> Any: ...

    def _record_progress_tip_poll(self, *args: Any, **kwargs: Any) -> Any: ...

    def _record_reorg_reconcile_lookup(self, *args: Any, **kwargs: Any) -> Any: ...

    def _record_startup_phase_once(self, *args: Any, **kwargs: Any) -> Any: ...

    def _remove_watchdog_heartbeat(self, *args: Any, **kwargs: Any) -> Any: ...

    def _reorg_reconcile_memo_fresh(self, *args: Any, **kwargs: Any) -> Any: ...

    def _submit_delivery_task(self, *args: Any, **kwargs: Any) -> Any: ...

    def _submit_reconcile_prefetch(self, *args: Any, **kwargs: Any) -> Any: ...

    def _submit_stale_check_tip_locked(self, *args: Any, **kwargs: Any) -> Any: ...

    def delivery_queue_limit(self, *args: Any, **kwargs: Any) -> Any: ...

    def disconnect_client(self, *args: Any, **kwargs: Any) -> Any: ...

    def ensure_reorg_reconciled_for_current_tip(self, *args: Any, **kwargs: Any) -> Any: ...

    def ensure_reorg_reconciled_for_tip(self, *args: Any, **kwargs: Any) -> Any: ...

    def fetch_qbit_tip_template_snapshot(self, *args: Any, **kwargs: Any) -> Any: ...

    def observe_job_build_elapsed(self, *args: Any, **kwargs: Any) -> Any: ...

    def pool_readiness_latched(self, *args: Any, **kwargs: Any) -> Any: ...

    def qbit_chain_view_untrusted(self, *args: Any, **kwargs: Any) -> Any: ...

    def retire_share_window_spool(self, *args: Any, **kwargs: Any) -> Any: ...

    def shutdown_job_build_executor(self, *args: Any, **kwargs: Any) -> Any: ...

    def shutdown_payout_artifact_executor(self, *args: Any, **kwargs: Any) -> Any: ...

    def shutdown_reconcile_prefetch_executor(self, *args: Any, **kwargs: Any) -> Any: ...

    def shutdown_serve_builder(self, *args: Any, **kwargs: Any) -> Any: ...

    def send_prepared_job(self, *args: Any, **kwargs: Any) -> Any: ...

    def shared_job_bundle(self, *args: Any, **kwargs: Any) -> Any: ...


class TipRefreshService:
    """Sole owner of R1 tip observation, refresh scheduling, and metrics."""

    def __init__(
        self,
        runtime: TipRefreshRuntime,
        *,
        shutdown_error: type[BaseException],
        job_build_failed_error: type[BaseException],
        delivery_priority_initial: int,
        delivery_priority_new_tip: int,
        delivery_priority_same_tip: int,
    ) -> None:
        self._runtime = runtime
        # Coordinator-owned exception types and S2 delivery priorities are
        # injected so this leaf module never imports prism_coordinator (or
        # job_delivery, which sits above it in the owner layering).
        self._shutdown_error = shutdown_error
        self._job_build_failed_error = job_build_failed_error
        self._delivery_priority_initial = delivery_priority_initial
        self._delivery_priority_new_tip = delivery_priority_new_tip
        self._delivery_priority_same_tip = delivery_priority_same_tip
        self._tip_refresh_lock = threading.Lock()
        self._tip_refresh_singleflight_lock = threading.Lock()
        self._tip_refresh_executor_lock = threading.Lock()
        self._tip_refresh_executor: _BoundedPriorityExecutor | None = None
        self._tip_refresh_executor_shutdown = False
        self._tip_refresh_metrics_lock = threading.Lock()
        self.tip_refresh_histograms = {
            name: {
                "buckets": {bucket: 0 for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS},
                "sum": 0.0,
                "count": 0,
            }
            for name in (
                "refresh",
                "bundle_build",
                "first_delivery",
                "last_delivery",
                "fanout_wave",
            )
        }
        self.tip_refresh_build_phase_histograms = {
            phase: {
                "buckets": {
                    bucket: 0 for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS
                },
                "sum": 0.0,
                "count": 0,
            }
            for phase in PRISM_TIP_REFRESH_BUILD_PHASES
        }
        self.tip_refresh_client_counts = {
            result: 0 for result in PRISM_TIP_REFRESH_RESULTS
        }
        self.tip_refresh_cancellation_counts = {
            stage: 0 for stage in PRISM_TIP_REFRESH_CANCELLATION_STAGES
        }
        self.tip_refresh_wave_outcome_counts = {
            outcome: 0 for outcome in PRISM_TIP_REFRESH_WAVE_OUTCOMES
        }
        self.tip_refresh_coverage_histograms = {
            target: {
                "buckets": {
                    bucket: 0 for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS
                },
                "sum": 0.0,
                "count": 0,
            }
            for target, _ratio in PRISM_TIP_REFRESH_COVERAGE_TARGETS
        }
        self.tip_refresh_inflight = 0
        self.tip_refresh_build_inflight = 0
        self.tip_refresh_build_queue_depth = 0
        self.tip_refresh_singleflight_hits = 0
        self.tip_refresh_superseded_results = 0
        self.tip_refresh_worker_failures = 0
        self.tip_refresh_worker_restarts = 0
        self.tip_refresh_ipc_bytes = {"input": 0, "output": 0}
        self._tip_refresh_pending_event = threading.Event()
        self._tip_refresh_pending_counter = 0
        self._tip_refresh_pending_token: int | None = None
        self._tip_refresh_retry = threading.Event()
        self._tip_refresh_retry_counter = 0
        self._tip_refresh_retry_consumed = 0
        self._tip_refresh_failure_holdoff_until: float | None = None
        self._tip_refresh_failure_tip: str | None = None
        self._active_tip_refresh: tuple[
            TipRefreshValidationToken,
            FanoutCancellation,
        ] | None = None
        self._tip_refresh_epoch_sequence = 0
        self._tip_refresh_epoch_tip_hash: str | None = None
        self._tip_refresh_epoch_payout_generation = 0
        self._tip_refresh_epoch_coverage: _TipRefreshEpochCoverage | None = None
        self._published_tip_refresh_epoch_identity: tuple[
            int,
            str,
            int,
            str,
            int,
        ] | None = None
        self._retained_collection_refresh: RetainedCollectionRefresh | None = None
        # The twelve historical plain coordinator fields (plus the
        # tip-detection epoch) are deliberately NOT initialized here: they
        # come into existence on first write exactly as the pre-extraction
        # lazy fields did, so ``hasattr``/``getattr``-with-default legacy
        # semantics (and the coordinator descriptor shims that route the old
        # names to this service) stay byte-for-byte compatible:
        #   current_tip_first_seen, current_tip_parent,
        #   current_tip_observation_sequence, current_tip_observed_monotonic,
        #   tip_template_snapshot, latest_detected_tip,
        #   tip_refresh_divergence_started_monotonic, tip_observation_sequence,
        #   last_successful_template_refresh_monotonic,
        #   template_refresh_failure_started_monotonic, tip_refresh_job_count,
        #   post_accept_refresh_failure_count, tip_detection_epoch.

    def _retain_collection_refresh(
        self,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
        payout_state_generation: int,
    ) -> None:
        """Retain reusable current work until an eligible identity appears."""
        runtime = self._runtime
        retained = RetainedCollectionRefresh(
            snapshot=snapshot,
            observation_sequence=observation_sequence,
            payout_state_generation=payout_state_generation,
        )
        should_log = False
        with runtime.lock:
            if not runtime._tip_refresh_snapshot_current_locked(
                snapshot,
                observation_sequence,
            ):
                return
            if any(
                runtime.client_can_receive_jobs(client)
                for client in runtime.clients
            ):
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

    def _retained_collection_artifacts(self) -> CachedTemplateArtifacts | None:
        """Return retained artifacts while their published work stays current.

        A same-tip poll advances its observation sequence before atomically
        replacing ``tip_template_snapshot``. The published snapshot remains
        reusable on both sides of that handoff even if the retained marker has
        not yet been updated; a new tip or payout generation still invalidates
        it immediately.
        """
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        runtime._ensure_tip_refresh_state()
        with runtime._job_cache_lock:
            payout_state_generation = runtime._payout_state_generation
        with runtime.lock:
            retained = self._retained_collection_refresh
            if retained is None:
                return None
            if retained.payout_state_generation != payout_state_generation:
                return None
            current_tip = getattr(self, "current_tip_first_seen", None)
            published_snapshot = self.tip_template_snapshot
            if (
                published_snapshot is None
                or current_tip is None
                or current_tip[0] != published_snapshot.bestblockhash
            ):
                return None
            return runtime._tip_refresh_artifacts(published_snapshot)

    def _retain_current_collection_refresh_if_unrepresented(self) -> None:
        """Keep the last published collection work when the fleet empties."""
        runtime = self._runtime
        runtime._ensure_tip_refresh_state()
        if getattr(runtime, "_pool_ready_latched", False):
            return
        with runtime.lock:
            if any(
                runtime.client_can_receive_jobs(client)
                for client in runtime.clients
            ):
                return
            snapshot = self.tip_template_snapshot
            observation_sequence = int(
                getattr(self, "current_tip_observation_sequence", 0)
            )
        if snapshot is None:
            return
        runtime._ensure_job_cache_state()
        with runtime._job_cache_lock:
            payout_state_generation = runtime._payout_state_generation
        runtime._retain_collection_refresh(
            snapshot,
            observation_sequence,
            payout_state_generation,
        )

    def _note_collection_identity_available(self, client: ClientState) -> None:
        """Wake a retained collection refresh as soon as a client is eligible."""
        runtime = self._runtime
        if not runtime.client_can_receive_jobs(client):
            return
        if runtime._retained_collection_artifacts() is None:
            return
        runtime._mark_tip_refresh_pending(client.connection_id)
        runtime._schedule_tip_refresh_retry()

    def _consume_retained_collection_refresh(
        self,
        context: PrismJobContext,
    ) -> None:
        """Consume retention only after its collection work was delivered."""
        runtime = self._runtime
        if not context.collection_only:
            return
        with runtime.lock:
            retained = self._retained_collection_refresh
            published_snapshot = self.tip_template_snapshot
            artifacts = (
                published_snapshot.template_artifacts
                if published_snapshot is not None
                else None
            )
            if (
                retained is not None
                and retained.payout_state_generation
                == context.payout_state_generation
                and artifacts is not None
                and context.template is artifacts.template
                and context.template_fingerprint == artifacts.fingerprint
                and context.template_generation == artifacts.generation
            ):
                self._retained_collection_refresh = None

    def _tip_refresh_pending(self) -> bool:
        runtime = self._runtime
        runtime._ensure_tip_refresh_state()
        return self._tip_refresh_pending_event.is_set()

    def _mark_tip_refresh_pending(self, _observation: object) -> int:
        runtime = self._runtime
        runtime._ensure_tip_refresh_state()
        with runtime.lock:
            self._tip_refresh_pending_counter += 1
            token = self._tip_refresh_pending_counter
            self._tip_refresh_pending_token = token
            self._tip_refresh_pending_event.set()
            return token

    def _claim_tip_refresh_pending(self) -> int | None:
        """Snapshot pending work without replacing a newer producer's token."""
        runtime = self._runtime
        runtime._ensure_tip_refresh_state()
        with runtime.lock:
            if not self._tip_refresh_pending_event.is_set():
                return None
            return self._tip_refresh_pending_token

    def _mark_tip_refresh_pending_for_poll(
        self,
        owned_token: int | None,
        _observation: object,
    ) -> int | None:
        """Mark poll-owned work only while no newer producer has superseded it."""
        runtime = self._runtime
        runtime._ensure_tip_refresh_state()
        with runtime.lock:
            if self._tip_refresh_pending_token != owned_token:
                return owned_token
            if owned_token is not None:
                self._tip_refresh_pending_event.set()
                return owned_token
            self._tip_refresh_pending_counter += 1
            token = self._tip_refresh_pending_counter
            self._tip_refresh_pending_token = token
            self._tip_refresh_pending_event.set()
            return token

    def _clear_tip_refresh_pending(self, token: int) -> None:
        runtime = self._runtime
        runtime._ensure_tip_refresh_state()
        with runtime.lock:
            if self._tip_refresh_pending_token == token:
                self._tip_refresh_pending_token = None
                self._tip_refresh_pending_event.clear()

    def _clear_tip_refresh_pending_for_completed_refresh(
        self,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
        payout_state_generation: int,
        pending_signal_token: int | None = None,
    ) -> bool:
        """Atomically acknowledge pending work handled by a completed poll."""
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        with runtime._payout_state_delivery_gate.delivery_cancelable(
            lambda: runtime._payout_state_generation != payout_state_generation,
            generation=payout_state_generation,
            priority=True,
        ) as admission:
            if not admission:
                return False
            with runtime._job_cache_lock:
                payout_state_current = (
                    runtime._payout_state_generation == payout_state_generation
                )
            with runtime.lock:
                refresh_current = runtime._tip_refresh_snapshot_current_locked(
                    snapshot,
                    observation_sequence,
                )
                pending_owned = (
                    self._tip_refresh_pending_token == pending_signal_token
                )
                if (
                    not payout_state_current
                    or not refresh_current
                    or not pending_owned
                ):
                    return False
                self._tip_refresh_pending_token = None
                self._tip_refresh_pending_event.clear()
                with runtime._progress_health_lock:
                    published_current = bool(
                        runtime._progress_has_published_work
                        and runtime._progress_published_template_fingerprint
                        == snapshot.template_fingerprint
                        and runtime._progress_published_payout_generation
                        == payout_state_generation
                    )
                    if published_current:
                        # The completion guard above proves this coherent
                        # snapshot still represents the latest detected and
                        # published tip. A transient A -> B -> A observation
                        # therefore closes its publication-divergence epoch
                        # even when existing A work needed no new delivery.
                        runtime._progress_refresh_signal_pending = False
                        runtime._progress_publication_divergence_since_monotonic = None
                return True

    def _schedule_tip_refresh_retry(self) -> None:
        runtime = self._runtime
        runtime._ensure_tip_refresh_state()
        # Pair the Event with a monotonic generation so a producer cannot set
        # it between a waiter's wake and clear and lose the newest retry. The
        # event remains the blocking primitive; the generation is the durable
        # coalesced work marker.
        with runtime.lock:
            self._tip_refresh_retry_counter += 1
            self._tip_refresh_retry.set()

    def _consume_tip_refresh_retry(self) -> bool:
        """Consume all retry signals visible at one atomic wake boundary."""
        runtime = self._runtime
        runtime._ensure_tip_refresh_state()
        with runtime.lock:
            generation = self._tip_refresh_retry_counter
            if generation == self._tip_refresh_retry_consumed:
                return False
            self._tip_refresh_retry_consumed = generation
            self._tip_refresh_retry.clear()
            return True

    def _note_tip_refresh_attempt_failed(
        self,
        observed_tip: str | None = None,
    ) -> None:
        """Stamp a failed refresh pass so the poller spaces its re-attempt.

        ``observed_tip`` is the tip the failed pass worked against; a pass
        that failed before learning one stamps the current observation so an
        RPC outage with a static tip still gets spaced.
        """
        runtime = self._runtime
        runtime._ensure_tip_refresh_state()
        holdoff = float(
            getattr(
                runtime,
                "tip_refresh_failure_holdoff_seconds",
                DEFAULT_PRISM_TIP_REFRESH_FAILURE_HOLDOFF_SECONDS,
            )
        )
        if holdoff <= 0:
            return
        holdoff += random.uniform(
            0.0,
            holdoff * PRISM_TIP_REFRESH_FAILURE_HOLDOFF_JITTER_FRACTION,
        )
        with runtime.lock:
            if observed_tip is None:
                observed_tip = runtime._newest_observed_tip_locked()
            self._tip_refresh_failure_tip = observed_tip
            self._tip_refresh_failure_holdoff_until = time.monotonic() + holdoff

    def _tip_refresh_failure_holdoff_remaining(self) -> float:
        """Seconds the poller must still wait before re-running a failed pass.

        Zero as soon as the newest known tip differs from the one the failed
        pass worked against: spacing throttles re-attempts against an
        unchanged, blocked or churning view, never the reaction to a
        genuinely new tip (including one detected but not yet published).
        """
        runtime = self._runtime
        runtime._ensure_tip_refresh_state()
        with runtime.lock:
            deadline = self._tip_refresh_failure_holdoff_until
            failed_tip = self._tip_refresh_failure_tip
            current_hash = runtime._newest_observed_tip_locked()
        if deadline is None:
            return 0.0
        if current_hash != failed_tip:
            return 0.0
        return max(0.0, deadline - time.monotonic())

    def _observe_tip_refresh_seconds(self, name: str, elapsed_seconds: float) -> None:
        runtime = self._runtime
        runtime._ensure_tip_refresh_state()
        with self._tip_refresh_metrics_lock:
            histogram = self.tip_refresh_histograms[name]
            histogram["count"] = int(histogram["count"]) + 1
            histogram["sum"] = float(histogram["sum"]) + elapsed_seconds
            buckets = histogram["buckets"]
            assert isinstance(buckets, dict)
            for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS:
                if elapsed_seconds <= bucket:
                    buckets[bucket] = int(buckets.get(bucket, 0)) + 1

    def _observe_tip_refresh_build_phase(
        self,
        phase: str,
        elapsed_seconds: float,
    ) -> None:
        runtime = self._runtime
        if phase not in PRISM_TIP_REFRESH_BUILD_PHASES:
            raise ValueError(f"unknown tip refresh build phase: {phase}")
        runtime._ensure_tip_refresh_state()
        with self._tip_refresh_metrics_lock:
            histogram = self.tip_refresh_build_phase_histograms[phase]
            histogram["count"] = int(histogram["count"]) + 1
            histogram["sum"] = float(histogram["sum"]) + max(
                0.0, elapsed_seconds
            )
            buckets = histogram["buckets"]
            assert isinstance(buckets, dict)
            for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS:
                if elapsed_seconds <= bucket:
                    buckets[bucket] = int(buckets.get(bucket, 0)) + 1

    def _record_tip_refresh_ipc_bytes(self, direction: str, byte_count: int) -> None:
        runtime = self._runtime
        if direction not in {"input", "output"}:
            raise ValueError(f"unknown tip refresh IPC direction: {direction}")
        runtime._ensure_tip_refresh_state()
        with self._tip_refresh_metrics_lock:
            self.tip_refresh_ipc_bytes[direction] += max(0, int(byte_count))

    def _record_tip_refresh_client_result(self, result: str) -> None:
        runtime = self._runtime
        if result not in PRISM_TIP_REFRESH_RESULTS:
            raise ValueError(f"unknown tip refresh result: {result}")
        runtime._ensure_tip_refresh_state()
        with self._tip_refresh_metrics_lock:
            self.tip_refresh_client_counts[result] += 1

    def _record_tip_refresh_cancellation(self, stage: str) -> None:
        runtime = self._runtime
        if stage not in PRISM_TIP_REFRESH_CANCELLATION_STAGES:
            raise ValueError(f"unknown tip refresh cancellation stage: {stage}")
        runtime._ensure_tip_refresh_state()
        with self._tip_refresh_metrics_lock:
            self.tip_refresh_cancellation_counts[stage] += 1

    def _record_tip_refresh_wave_outcome(self, outcome: str) -> None:
        runtime = self._runtime
        runtime._ensure_tip_refresh_state()
        if outcome not in PRISM_TIP_REFRESH_WAVE_OUTCOMES:
            raise ValueError(f"unknown tip refresh wave outcome: {outcome}")
        with self._tip_refresh_metrics_lock:
            self.tip_refresh_wave_outcome_counts[outcome] = (
                int(self.tip_refresh_wave_outcome_counts.get(outcome, 0)) + 1
            )

    def _tip_refresh_future_started(self) -> None:
        runtime = self._runtime
        runtime._ensure_tip_refresh_state()
        with self._tip_refresh_metrics_lock:
            self.tip_refresh_inflight += 1

    def _tip_refresh_future_finished(self, _future: Future[RefreshResult]) -> None:
        runtime = self._runtime
        runtime._ensure_tip_refresh_state()
        with self._tip_refresh_metrics_lock:
            self.tip_refresh_inflight = max(0, self.tip_refresh_inflight - 1)

    def tip_refresh_executor(self) -> _BoundedPriorityExecutor:
        runtime = self._runtime
        runtime._ensure_tip_refresh_state()
        with self._tip_refresh_executor_lock:
            if self._tip_refresh_executor_shutdown:
                raise RuntimeError("tip refresh executor is shut down")
            executor = self._tip_refresh_executor
            if executor is None:
                executor = _BoundedPriorityExecutor(
                    max_workers=runtime.tip_refresh_max_workers,
                    max_queue_size=runtime.delivery_queue_limit(),
                    thread_name_prefix="prism-tip-refresh-delivery",
                )
                self._tip_refresh_executor = executor
            return executor

    def shutdown_tip_refresh_executor(self) -> None:
        runtime = self._runtime
        runtime._ensure_tip_refresh_state()
        with self._tip_refresh_executor_lock:
            executor = self._tip_refresh_executor
            self._tip_refresh_executor = None
            self._tip_refresh_executor_shutdown = True
        if executor is not None:
            # Stop queued publication work before waiting on reconnect workers.
            executor.shutdown(wait=False, cancel_futures=True)
        runtime.shutdown_initial_job_executor()
        if executor is not None:
            # Running workers may already hold client/job state or be inside a
            # socket send. Drain them before serve returns and the writer lease
            # is released; queued workers are cancelled without starting.
            executor.shutdown(wait=True)
        runtime.shutdown_job_build_executor()
        runtime.shutdown_payout_artifact_executor()
        runtime.shutdown_reconcile_prefetch_executor()
        runtime.retire_share_window_spool()
        runtime.shutdown_serve_builder()

    def publication_progress_failure_expired(self, now: float) -> bool:
        """Bound sustained detected/current publication divergence.

        Retry-loop heartbeats prove only that the driver is scheduled. This
        dedicated deadline is independent of client-delivery health and is
        cleared only by current work publication/delivery, so old delivery
        backlog cannot make a later legitimate divergence expire immediately.
        """
        runtime = self._runtime
        budget = float(
            getattr(
                runtime,
                "template_refresh_failure_exit_seconds",
                DEFAULT_PRISM_TEMPLATE_MAX_AGE_SECONDS,
            )
        )
        if budget <= 0:
            return False
        runtime._ensure_job_cache_state()
        with runtime._progress_health_lock:
            divergence_since = (
                runtime._progress_publication_divergence_since_monotonic
            )
        return bool(
            divergence_since is not None
            and now - divergence_since >= budget
        )

    def _cancel_active_tip_refresh_for_shutdown(self) -> None:
        runtime = self._runtime
        runtime._ensure_tip_refresh_state()
        with runtime.lock:
            active = self._active_tip_refresh
            if active is not None:
                active[1].cancel()
            self._tip_refresh_retry.clear()

    def _wait_for_blockpoll_trigger(self) -> bool:
        """Wait for the normal interval or an immediate coalesced retry."""
        runtime = self._runtime
        remaining = float(runtime.blockpoll_seconds)
        while remaining > 0:
            if runtime.stop_event.is_set():
                return False
            # After a failed pass, leave retry signals unconsumed until the
            # spacing window closes. Re-checked per slice: a newly observed
            # tip zeroes the holdoff immediately, and signals arriving during
            # the hold stay coalesced for the attempt that eventually runs.
            holdoff = runtime._tip_refresh_failure_holdoff_remaining()
            if holdoff <= 0 and runtime._consume_tip_refresh_retry():
                return not runtime.stop_event.is_set()
            wait_seconds = min(remaining, 0.25)
            if holdoff > 0:
                # The retry event may already be set; pace on the stop event
                # so the spacing window cannot be spun through instantly.
                # Short slices keep a newly detected tip's release prompt.
                # Beat per slice: a deliberately held poller is idle, not
                # hung, and must not trip the watchdog when the configured
                # holdoff exceeds its timeout.
                runtime._record_heartbeat("qbit_blockpoll")
                wait_seconds = min(wait_seconds, holdoff, 0.05)
                runtime.stop_event.wait(wait_seconds)
            else:
                self._tip_refresh_retry.wait(wait_seconds)
            remaining -= wait_seconds
        # Interval-driven attempts respect the same spacing so a sub-holdoff
        # blockpoll interval cannot re-run a failed pass early.
        while not runtime.stop_event.is_set():
            holdoff = runtime._tip_refresh_failure_holdoff_remaining()
            if holdoff <= 0:
                break
            runtime._record_heartbeat("qbit_blockpoll")
            runtime.stop_event.wait(min(holdoff, 0.05))
        runtime._consume_tip_refresh_retry()
        return not runtime.stop_event.is_set()

    def blockpoll_loop(self) -> None:
        runtime = self._runtime
        runtime._ensure_tip_refresh_state()
        while runtime._wait_for_blockpoll_trigger():
            # A superseding observation or post-fanout tip change wakes this
            # fallback loop immediately. The event coalesces repeated signals;
            # ordinary same-tip polling retains its configured interval.
            # Heartbeat at the top of each iteration: reaching here proves the
            # loop is alive. A transient qbit RPC error still loops and beats; a
            # hung RPC call never returns, so the beat goes stale and the
            # watchdog restarts the process.
            runtime._record_heartbeat("qbit_blockpoll")
            try:
                refreshed = runtime.poll_qbit_tip_template_once()
                if refreshed:
                    print(
                        f"prism coordinator: refreshed {refreshed} client job(s) after qbit tip/template change",
                        flush=True,
                    )
            except self._shutdown_error:
                # Admission can close after the loop condition but before a
                # nested reconciliation enters the writer gate. That is an
                # intentional shutdown stop, not a template-health failure.
                return
            except (TemplateRefreshSuperseded, PayoutStatePublicationBlocked) as exc:
                # Coordination-blocked attempts remain outside the ordinary
                # template failure budget. Their separate continuous-streak
                # budget is recorded by poll_qbit_tip_template_once and
                # enforced by the mandatory publication-progress watchdog.
                # The retry is already scheduled by the raise site.
                print(
                    f"prism coordinator: tip/template refresh superseded; retrying: {exc}",
                    flush=True,
                )
            except Exception:
                print("prism coordinator: qbit tip/template poll failed", flush=True)
                traceback.print_exc()
                if runtime.template_refresh_failure_expired(time.monotonic()):
                    print(
                        "prism coordinator: template refresh failure budget exhausted; "
                        "exiting non-zero so the restart policy recovers the process",
                        flush=True,
                    )
                    os._exit(1)

    def template_refresh_failure_expired(self, now: float) -> bool:
        runtime = self._runtime
        budget = float(
            getattr(
                runtime,
                "template_refresh_failure_exit_seconds",
                DEFAULT_PRISM_TEMPLATE_MAX_AGE_SECONDS,
            )
        )
        if budget <= 0:
            return False
        failure_started = getattr(self, "template_refresh_failure_started_monotonic", None)
        return failure_started is not None and now - failure_started >= budget

    def _record_template_refresh_failure(self, now: float) -> None:
        runtime = self._runtime
        budget = float(
            getattr(
                runtime,
                "template_refresh_failure_exit_seconds",
                DEFAULT_PRISM_TEMPLATE_MAX_AGE_SECONDS,
            )
        )
        if (
            budget > 0
            and getattr(self, "template_refresh_failure_started_monotonic", None) is None
        ):
            self.template_refresh_failure_started_monotonic = now

    def blockwait_once(self, known_tip: str) -> str:
        """One waitfornewblock round: returns the tip after the wait.

        qbitd returns as soon as its tip differs from ``known_tip`` (or after
        the server-side timeout, echoing the current tip), so a tip observed
        between our last poll and this call is reported immediately rather
        than being missed for a cycle.
        """
        runtime = self._runtime
        timeout_seconds = getattr(
            runtime,
            "blockwait_timeout_seconds",
            DEFAULT_PRISM_BLOCKWAIT_TIMEOUT_SECONDS,
        )
        watchdog_timeout = float(getattr(runtime, "watchdog_timeout_seconds", 120.0))
        max_rpc_timeout = max(1.0, watchdog_timeout * 0.8)
        timeout_seconds = min(float(timeout_seconds), max(1.0, max_rpc_timeout - 1.0))
        result = runtime.rpc.call(
            "waitfornewblock",
            [max(1, int(timeout_seconds * 1000)), known_tip],
            timeout=timeout_seconds + 10.0,
        )
        if isinstance(result, dict):
            new_tip = str(result.get("hash", "") or "")
            if new_tip:
                return new_tip
        return known_tip

    def blockwait_loop(self) -> None:
        """Push-style tip detection alongside the interval poller.

        Stale rejects are dominated by the window between a block connecting
        and miners receiving fresh work; the poller alone leaves up to a full
        PRISM_BLOCKPOLL_SECONDS of that window. This loop parks inside
        waitfornewblock and triggers the same refresh path within milliseconds
        of a new tip. The poller stays on as the fallback and still owns
        same-tip template refreshes, which waitfornewblock does not signal.
        Disabled cleanly when qbitd does not support the RPC.
        """
        runtime = self._runtime
        known_tip: str | None = None
        while not runtime.stop_event.is_set():
            runtime._record_heartbeat("qbit_blockwait")
            try:
                if known_tip is None:
                    known_tip = str(runtime.rpc.call("getbestblockhash"))
                    runtime.observe_tip_for_refresh(known_tip)
                new_tip = runtime.blockwait_once(known_tip)
                if new_tip == known_tip:
                    if runtime.stop_event.wait(0.25):
                        return
                    continue
                # Advance the wait cursor before observation/logging. If either
                # operation raises, the next wait must not rediscover the same
                # already-seen transition and create a notification storm.
                known_tip = new_tip
                # Detection can supersede/cancel obsolete heavy work, but only
                # the blockpoll single-flight owner may fetch, build, and
                # publish replacement work.
                try:
                    runtime.observe_tip_for_refresh(new_tip)
                finally:
                    # Preserve the wake even if observation-side cancellation
                    # or bookkeeping raises after known_tip advanced.
                    runtime._schedule_tip_refresh_retry()
                print(
                    f"prism coordinator: blockwait saw new tip {new_tip}; "
                    "single-flight refresh scheduled",
                    flush=True,
                )
            except Exception as exc:
                if known_tip is not None and runtime._blockwait_unsupported(exc):
                    print(
                        "prism coordinator: waitfornewblock unavailable on this qbitd; "
                        "tip detection falls back to blockpoll only",
                        flush=True,
                    )
                    runtime._remove_watchdog_heartbeat("qbit_blockwait")
                    return
                print("prism coordinator: blockwait pass failed", flush=True)
                traceback.print_exc()
                if runtime.stop_event.wait(min(5.0, runtime.blockpoll_seconds)):
                    return

    def tip_refresh_metrics_lines(self) -> list[str]:
        runtime = self._runtime
        runtime._ensure_tip_refresh_state()
        with self._tip_refresh_executor_lock:
            executor_workers = (
                runtime.tip_refresh_max_workers
                if self._tip_refresh_executor is not None
                else 0
            )
        with self._tip_refresh_metrics_lock:
            histograms = {
                name: {
                    "buckets": dict(histogram["buckets"]),
                    "sum": float(histogram["sum"]),
                    "count": int(histogram["count"]),
                }
                for name, histogram in self.tip_refresh_histograms.items()
            }
            phase_histograms = {
                phase: {
                    "buckets": dict(histogram["buckets"]),
                    "sum": float(histogram["sum"]),
                    "count": int(histogram["count"]),
                }
                for phase, histogram in self.tip_refresh_build_phase_histograms.items()
            }
            client_counts = dict(self.tip_refresh_client_counts)
            cancellation_counts = dict(self.tip_refresh_cancellation_counts)
            wave_outcome_counts = dict(self.tip_refresh_wave_outcome_counts)
            coverage_histograms = {
                target: {
                    "buckets": dict(histogram["buckets"]),
                    "sum": float(histogram["sum"]),
                    "count": int(histogram["count"]),
                }
                for target, histogram in (
                    self.tip_refresh_coverage_histograms.items()
                )
            }
            inflight = self.tip_refresh_inflight
            build_inflight = self.tip_refresh_build_inflight
            build_queue_depth = self.tip_refresh_build_queue_depth
            singleflight_hits = self.tip_refresh_singleflight_hits
            superseded_results = self.tip_refresh_superseded_results
            worker_failures = self.tip_refresh_worker_failures
            worker_restarts = self.tip_refresh_worker_restarts
            ipc_bytes = dict(self.tip_refresh_ipc_bytes)

        metric_names = {
            "refresh": "qbit_prism_tip_refresh_seconds",
            "bundle_build": "qbit_prism_tip_refresh_bundle_build_seconds",
            "first_delivery": "qbit_prism_tip_refresh_first_delivery_seconds",
            "last_delivery": "qbit_prism_tip_refresh_last_delivery_seconds",
            "fanout_wave": "qbit_prism_tip_refresh_fanout_wave_seconds",
        }
        descriptions = {
            "refresh": "Full qbit tip/template refresh pass wall time.",
            "bundle_build": "Shared ready-pool refresh bundle preparation wall time.",
            "first_delivery": "Tip observation to first successful client delivery.",
            "last_delivery": "Tip observation to last successful client delivery.",
            "fanout_wave": "First task submission to last successful delivery of one completed prepared fanout wave.",
        }
        lines: list[str] = []
        for name, metric_name in metric_names.items():
            histogram = histograms[name]
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
                "# HELP qbit_prism_tip_refresh_wave_outcomes_total Singleflight refresh waves by terminal or recovered supersession outcome.",
                "# TYPE qbit_prism_tip_refresh_wave_outcomes_total counter",
                *[
                    f'qbit_prism_tip_refresh_wave_outcomes_total{{outcome="{outcome}"}} {int(wave_outcome_counts.get(outcome, 0))}'
                    for outcome in PRISM_TIP_REFRESH_WAVE_OUTCOMES
                ],
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
                f"qbit_prism_tip_refresh_inflight {inflight}",
                "# HELP qbit_prism_tip_refresh_executor_workers Configured persistent refresh executor workers, or zero before creation.",
                "# TYPE qbit_prism_tip_refresh_executor_workers gauge",
                f"qbit_prism_tip_refresh_executor_workers {executor_workers}",
                "# HELP qbit_prism_tip_refresh_bundle_inflight Shared bundle builds currently running.",
                "# TYPE qbit_prism_tip_refresh_bundle_inflight gauge",
                f"qbit_prism_tip_refresh_bundle_inflight {build_inflight}",
                "# HELP qbit_prism_tip_refresh_bundle_queue_depth Shared bundle callers waiting on bounded build admission or an identical single-flight.",
                "# TYPE qbit_prism_tip_refresh_bundle_queue_depth gauge",
                f"qbit_prism_tip_refresh_bundle_queue_depth {build_queue_depth}",
                "# HELP qbit_prism_tip_refresh_bundle_singleflight_hits_total Shared bundle callers coalesced behind an identical build.",
                "# TYPE qbit_prism_tip_refresh_bundle_singleflight_hits_total counter",
                f"qbit_prism_tip_refresh_bundle_singleflight_hits_total {singleflight_hits}",
                "# HELP qbit_prism_tip_refresh_bundle_superseded_results_total Completed or canceled shared bundles discarded after supersession.",
                "# TYPE qbit_prism_tip_refresh_bundle_superseded_results_total counter",
                f"qbit_prism_tip_refresh_bundle_superseded_results_total {superseded_results}",
                "# HELP qbit_prism_tip_refresh_builder_worker_failures_total Audit-builder subprocess failures.",
                "# TYPE qbit_prism_tip_refresh_builder_worker_failures_total counter",
                f"qbit_prism_tip_refresh_builder_worker_failures_total {worker_failures}",
                "# HELP qbit_prism_tip_refresh_builder_worker_restarts_total Long-lived builder worker restarts (persistent --serve daemon respawns after a crash or supersession).",
                "# TYPE qbit_prism_tip_refresh_builder_worker_restarts_total counter",
                f"qbit_prism_tip_refresh_builder_worker_restarts_total {worker_restarts}",
                "# HELP qbit_prism_tip_refresh_builder_ipc_bytes_total Bytes copied across audit-builder subprocess IPC.",
                "# TYPE qbit_prism_tip_refresh_builder_ipc_bytes_total counter",
                *[
                    f'qbit_prism_tip_refresh_builder_ipc_bytes_total{{direction="{direction}"}} {int(ipc_bytes.get(direction, 0))}'
                    for direction in ("input", "output")
                ],
            ]
        )
        coverage_metric_name = "qbit_prism_tip_refresh_hashrate_coverage_seconds"
        lines.extend(
            [
                "# HELP qbit_prism_tip_refresh_hashrate_coverage_seconds Time from epoch detection until a fixed share of the snapshotted hashrate proxy received current work.",
                "# TYPE qbit_prism_tip_refresh_hashrate_coverage_seconds histogram",
            ]
        )
        for target, _ratio in PRISM_TIP_REFRESH_COVERAGE_TARGETS:
            histogram = coverage_histograms[target]
            buckets = histogram["buckets"]
            assert isinstance(buckets, dict)
            lines.extend(
                [
                    *[
                        f'{coverage_metric_name}_bucket{{coverage="{target}",le="{bucket:g}"}} {int(buckets.get(bucket, 0))}'
                        for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS
                    ],
                    f'{coverage_metric_name}_bucket{{coverage="{target}",le="+Inf"}} {histogram["count"]}',
                    f'{coverage_metric_name}_sum{{coverage="{target}"}} {float(histogram["sum"]):.6f}',
                    f'{coverage_metric_name}_count{{coverage="{target}"}} {histogram["count"]}',
                ]
            )
        runtime._ensure_job_cache_state()
        with runtime._serve_builder_metrics_lock:
            serve_counts = dict(runtime.serve_builder_counts)
            serve_window_counts = dict(runtime.serve_builder_window_cache_counts)
        with runtime._payout_artifact_executor_lock:
            payout_artifact_events = dict(runtime.payout_artifact_event_counts)
        lines.extend(
            [
                "# HELP qbit_prism_serve_builder_events_total Persistent audit-builder daemon lifecycle and request outcomes.",
                "# TYPE qbit_prism_serve_builder_events_total counter",
                *[
                    f'qbit_prism_serve_builder_events_total{{event="{event}"}} {int(serve_counts.get(event, 0))}'
                    for event in (
                        "requests",
                        "fallbacks",
                        "spawns",
                        "window_uploads",
                        "window_prepares",
                    )
                ],
                "# HELP qbit_prism_payout_artifact_events_total Payout-ledger-artifact lifecycle outcomes (build, install, reuse pacing).",
                "# TYPE qbit_prism_payout_artifact_events_total counter",
                *[
                    f'qbit_prism_payout_artifact_events_total{{event="{event}"}} {int(payout_artifact_events.get(event, 0))}'
                    for event in (
                        "built",
                        "build_aborted",
                        "debounced",
                        "incremental",
                        "full_rescan",
                        "self_check_match",
                        "self_check_mismatch",
                        "self_check_failed",
                        "balance_check_mismatch",
                        "found_block_cached",
                        "installed",
                        "refreshed",
                        "already_current",
                        "discarded",
                        "born_expired",
                        "rearm_scheduled",
                        "served_reuse",
                        "probe_rejected_ceiling",
                        "probe_past_floor",
                        "window_mirror_divergence",
                    )
                ],
                "# HELP qbit_prism_serve_builder_window_cache_total Daemon parsed share-window cache outcomes.",
                "# TYPE qbit_prism_serve_builder_window_cache_total counter",
                *[
                    f'qbit_prism_serve_builder_window_cache_total{{result="{result}"}} {int(serve_window_counts.get(counter_key, 0))}'
                    for result, counter_key in (
                        ("hit", "hits"),
                        ("miss", "misses"),
                    )
                ],
            ]
        )
        return lines

    def _tip_refresh_artifacts(
        self,
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

    def prepare_tip_refresh_bundle(
        self,
        snapshot: QbitTipTemplateSnapshot,
        *,
        priority_requested_monotonic: float | None = None,
    ) -> CachedJobBundle:
        """Build ready-pool work from immutable shared inputs only.

        Client selection belongs exclusively to fanout. In particular, a
        connection disappearing before or during this build cannot affect the
        signed payout bundle or its cache lifetime.
        """
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        runtime._ensure_tip_refresh_state()
        artifacts = runtime._tip_refresh_artifacts(snapshot)
        build_started = time.monotonic()
        progress_build_token = runtime._progress_bundle_build_started()
        priority_token, priority_requested_monotonic = (
            runtime._begin_job_build_priority_preparation(
                priority_requested_monotonic
            )
        )
        try:
            max_payout_retries = max(
                0,
                int(
                    getattr(
                        runtime,
                        "payout_reconcile_supersession_retries",
                        DEFAULT_PRISM_PAYOUT_RECONCILE_SUPERSESSION_RETRIES,
                    )
                ),
            )
            for attempt in range(max_payout_retries + 1):
                with runtime._job_cache_lock:
                    payout_generation_before_build = (
                        runtime._payout_state_generation
                    )
                try:
                    bundle = runtime.shared_job_bundle(
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
                    if getattr(runtime, "tip_refresh_epoch_fanout", False):
                        raise
                    with runtime._job_cache_lock:
                        payout_generation_after_build = (
                            runtime._payout_state_generation
                        )
                        payout_publication_blocked = (
                            runtime._payout_state_publication_blocked
                        )
                    with runtime.lock:
                        artifacts_buildable = runtime._artifacts_buildable_locked(
                            artifacts
                        )
                    if (
                        attempt >= max_payout_retries
                        or payout_publication_blocked
                        or not artifacts_buildable
                        or payout_generation_after_build
                        == payout_generation_before_build
                    ):
                        raise
                    # Coalesce a completed payout generation into this same
                    # tip owner. Every abandoned attempt remains fenced by its
                    # immutable generation; only the latest coherent retry can
                    # reach final publication.
                    continue
            else:  # pragma: no cover - range always runs at least once
                raise TemplateRefreshBlocked(
                    "payout generation did not stabilize during preparation"
                )
        except TemplateRefreshBlocked:
            raise
        except Exception as exc:
            with runtime.lock:
                runtime.job_build_failure_count += 1
            raise TemplateRefreshBlocked("prepared refresh bundle build failed") from exc
        finally:
            runtime._finish_job_build_priority_preparation(priority_token)
            runtime._progress_bundle_build_finished(progress_build_token)
            runtime._observe_tip_refresh_seconds(
                "bundle_build",
                time.monotonic() - build_started,
            )
        # Revalidate only the snapshot-owned object. A concurrent cache fill is
        # unrelated to this refresh and cannot replace its exact artifacts.
        artifacts = runtime._tip_refresh_artifacts(snapshot)
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
        """Publish one exact current-tip ready bundle before Stratum accepts."""
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        with runtime._job_cache_lock:
            runtime.job_preparation_pending = True
        try:
            observation_sequence = runtime._reserve_tip_observation_sequence()
            snapshot = runtime.fetch_qbit_tip_template_snapshot()
            runtime._record_progress_tip_poll(snapshot)
            try:
                reconciled = runtime.ensure_reorg_reconciled_for_tip(
                    snapshot.bestblockhash
                )
            except Exception as exc:
                raise TemplateRefreshBlocked(
                    "startup reorg reconciliation failed before job preparation"
                ) from exc
            if not reconciled:
                raise TemplateRefreshBlocked(
                    "startup chain view remained untrusted during job preparation"
                )

            ready = runtime.pool_readiness_latched()
            bundle: CachedJobBundle | None = None
            if ready:
                progress_build_token = runtime._progress_bundle_build_started()
                try:
                    bundle = runtime.shared_job_bundle(
                        runtime._tip_refresh_artifacts(snapshot),
                        None,
                        publication_critical=True,
                        request_source="tip_refresh",
                    )
                finally:
                    runtime._progress_bundle_build_finished(progress_build_token)
                if bundle.collection_only:
                    raise TemplateRefreshBlocked(
                        "startup ready preparation produced collection work"
                    )
                if bundle.payout_state_generation != int(
                    getattr(runtime, "_payout_state_generation", 0)
                ):
                    raise TemplateRefreshSuperseded(
                        "payout state changed during startup job preparation"
                    )

            if str(runtime.rpc.call("getbestblockhash")) != snapshot.bestblockhash:
                raise TemplateRefreshSuperseded(
                    "qbit tip changed during startup job preparation"
                )
            if not runtime.observe_tip_first_seen(
                snapshot.bestblockhash,
                observation_sequence=observation_sequence,
                publish_refresh_observation=True,
                published_snapshot=snapshot,
            ):
                raise TemplateRefreshSuperseded(
                    "startup job preparation was superseded before publication"
                )
            with runtime._job_cache_lock:
                runtime._prepared_ready_snapshot = snapshot if bundle is not None else None
                runtime._prepared_ready_bundle = bundle
            runtime._record_progress_publication(
                snapshot,
                (
                    bundle.payout_state_generation
                    if bundle is not None
                    else int(getattr(runtime, "_payout_state_generation", 0))
                ),
            )
            self.last_successful_template_refresh_monotonic = time.monotonic()
            runtime._record_progress_tip_poll(snapshot)
            return bundle
        finally:
            with runtime._job_cache_lock:
                runtime.job_preparation_pending = False

    def prewarm_startup_jobs(self) -> CachedJobBundle | None:
        """Best-effort startup prewarm; transient blocking defers to blockpoll."""
        runtime = self._runtime
        try:
            return runtime.prewarm_current_tip_ready_bundle()
        except TemplateRefreshBlocked as exc:
            # Startup prewarming is an optimization. A transient reconciliation,
            # payout-generation, or tip race must not prevent Stratum listeners
            # from opening; blockpoll and the bounded initial-job queue retry it.
            runtime._schedule_tip_refresh_retry()
            print(
                "prism coordinator: startup job preparation deferred "
                f"reason={exc}",
                flush=True,
            )
            return None

    def _tip_refresh_token_current_locked(
        self,
        token: TipRefreshValidationToken,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
    ) -> bool:
        runtime = self._runtime
        return bool(
            runtime._tip_refresh_token_prepublication_current_locked(
                token,
                bundle,
                snapshot,
            )
            and runtime._tip_refresh_snapshot_current_locked(
                snapshot,
                token.observation_sequence,
            )
        )

    def _tip_refresh_token_prepublication_current_locked(
        self,
        token: TipRefreshValidationToken,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
    ) -> bool:
        runtime = self._runtime
        published = getattr(runtime, "_published_payout_state", None)
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
            and token.build_key.template_fingerprint
            == snapshot.template_fingerprint
            and token.build_key.template_generation == snapshot.template_generation
            and token.build_key.payout_state_generation
            == token.payout_state_generation
            and token.build_key.payout_append_invalidation_epoch
            == int(
                getattr(
                    runtime,
                    "_payout_ledger_append_invalidation_epoch",
                    0,
                )
            )
            and token.payout_state_generation
            == int(getattr(runtime, "_payout_state_generation", 0))
            and published is not None
            and published.artifact is not None
            and token.build_key.payout_artifact_sha256
            == published.artifact.prior_balances_sha256
            and snapshot.template_artifacts is not None
            and bundle.template is snapshot.template_artifacts.template
            and not runtime._detected_tip_supersedes_locked(
                snapshot.bestblockhash,
                token.observation_sequence,
            )
            and (
                not getattr(runtime, "tip_refresh_epoch_fanout", False)
                or (
                    token.epoch_sequence
                    == int(getattr(self, "_tip_refresh_epoch_sequence", 0))
                    and token.tip_hash
                    == getattr(self, "_tip_refresh_epoch_tip_hash", None)
                    and token.payout_state_generation
                    == int(
                        getattr(
                            self,
                            "_tip_refresh_epoch_payout_generation",
                            0,
                        )
                    )
                )
            )
        )

    def _tip_refresh_snapshot_current_locked(
        self,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
    ) -> bool:
        runtime = self._runtime
        current_tip = getattr(self, "current_tip_first_seen", None)
        return bool(
            self.tip_template_snapshot is snapshot
            and current_tip is not None
            and current_tip[0] == snapshot.bestblockhash
            and int(getattr(self, "current_tip_observation_sequence", 0))
            == observation_sequence
            and not runtime._detected_tip_supersedes_locked(
                snapshot.bestblockhash,
                observation_sequence,
            )
        )

    def _validate_prepared_tip_refresh(
        self,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
    ) -> TipRefreshValidationToken:
        """Validate prepared work before publishing submit authority."""
        runtime = self._runtime
        artifacts = runtime._tip_refresh_artifacts(snapshot)
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
            current_tip = str(runtime.rpc.call("getbestblockhash"))
        except Exception as exc:
            runtime._schedule_tip_refresh_retry()
            raise TemplateRefreshBlocked(
                "qbit tip validation failed before prepared fanout"
            ) from exc
        if current_tip != snapshot.bestblockhash:
            runtime._schedule_tip_refresh_retry()
            raise TemplateRefreshSuperseded(
                "qbit tip changed before prepared fanout "
                f"expected={snapshot.bestblockhash} current={current_tip}"
            )
        try:
            chain_view_untrusted = bool(
                getattr(runtime, "reorg_reconciler_enabled", True)
                and runtime.qbit_chain_view_untrusted()
            )
        except Exception as exc:
            runtime._schedule_tip_refresh_retry()
            raise _TipRefreshTrustBlocked(
                "qbit chain trust check failed before prepared fanout"
            ) from exc
        if chain_view_untrusted:
            runtime._schedule_tip_refresh_retry()
            raise _TipRefreshTrustBlocked(
                "qbit chain view became untrusted before prepared fanout"
            )
        with runtime.lock:
            epoch_sequence = (
                int(getattr(self, "_tip_refresh_epoch_sequence", 0))
                if getattr(runtime, "tip_refresh_epoch_fanout", False)
                else 0
            )
            token = TipRefreshValidationToken(
                tip_hash=snapshot.bestblockhash,
                template_fingerprint=artifacts.fingerprint,
                template_generation=artifacts.generation,
                payout_state_generation=bundle.payout_state_generation,
                observation_sequence=observation_sequence,
                epoch_sequence=epoch_sequence,
                build_key=bundle.build_key,
                snapshot=snapshot,
            )
            if not runtime._tip_refresh_token_prepublication_current_locked(
                token,
                bundle,
                snapshot,
            ):
                runtime._schedule_tip_refresh_retry()
                raise TemplateRefreshSuperseded(
                    "prepared refresh was superseded before tip publication"
                )
        return token

    def _activate_tip_refresh(
        self,
        token: TipRefreshValidationToken,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        cancel_event: FanoutCancellation,
    ) -> None:
        runtime = self._runtime
        with runtime.lock:
            if not runtime._tip_refresh_token_current_locked(token, bundle, snapshot):
                runtime._schedule_tip_refresh_retry()
                raise TemplateRefreshSuperseded(
                    "prepared refresh was superseded before cancellation registration"
                )
            active = self._active_tip_refresh
            if active is not None:
                active[1].cancel()
            self._active_tip_refresh = (token, cancel_event)

    def _publish_prepared_tip_refresh(
        self,
        token: TipRefreshValidationToken,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        *,
        parent_hash: str | None,
    ) -> FanoutCancellation:
        """Atomically publish prepared work and register its cancellation token."""
        runtime = self._runtime
        now = time.monotonic()
        cancel_event = FanoutCancellation()
        # Admit a synchronization-only reader of this payout generation. Do
        # not mark it delivered: the gate's priority reservation belongs to
        # the first current-tip socket delivery, not this publication fence.
        with runtime._payout_state_delivery_gate.delivery_cancelable(
            lambda: False,
            generation=token.payout_state_generation,
            priority=True,
        ) as payout_admitted:
            if not payout_admitted:
                runtime._schedule_tip_refresh_retry()
                raise TemplateRefreshSuperseded(
                    "prepared refresh was superseded before atomic publication"
                )
            # Match payout publication's cache -> coordinator lock order. A
            # payout fence can close admission after this reader entered, so
            # recheck its blocked marker inside the same atomic section.
            with runtime._job_cache_lock:
                with runtime.lock:
                    current_sequence = int(
                        getattr(self, "current_tip_observation_sequence", 0)
                    )
                    if (
                        runtime._payout_state_publication_blocked
                        or current_sequence > token.observation_sequence
                        or not runtime._tip_refresh_token_prepublication_current_locked(
                            token,
                            bundle,
                            snapshot,
                        )
                    ):
                        runtime._schedule_tip_refresh_retry()
                        raise TemplateRefreshSuperseded(
                            "prepared refresh was superseded before atomic publication"
                        )

                    first_seen = getattr(self, "current_tip_first_seen", None)
                    tip_changed = (
                        first_seen is not None and first_seen[0] != token.tip_hash
                    )
                    flip_stamp = (
                        now
                        if tip_changed
                        else first_seen[1]
                        if first_seen is not None
                        else None
                    )
                    self.current_tip_first_seen = (
                        token.tip_hash,
                        flip_stamp,
                    )
                    self.current_tip_observation_sequence = token.observation_sequence
                    self.current_tip_observed_monotonic = now
                    if parent_hash is not None:
                        self.current_tip_parent = (token.tip_hash, parent_hash)
                    else:
                        # The parent lookup is best-effort cleanup metadata. A
                        # transient RPC failure during a same-tip republication
                        # must not wipe the still-valid cached parent; only a
                        # parent belonging to a different tip is stale here.
                        prior_parent = getattr(self, "current_tip_parent", None)
                        if prior_parent is None or prior_parent[0] != token.tip_hash:
                            self.current_tip_parent = None
                    self.tip_template_snapshot = snapshot
                    runtime._record_startup_phase_once("first_tip_template_published")
                    self.tip_refresh_divergence_started_monotonic = None
                    if tip_changed:
                        # Retained collection work, the retained ready bundle,
                        # and graveyard classification all belong to the
                        # previously published tip.
                        self._retained_collection_refresh = None
                        runtime._prepared_ready_bundle = None
                        runtime._prepared_ready_snapshot = None
                        runtime.prune_evicted_job_graveyard(now=now, force=True)

                    if not runtime._tip_refresh_token_current_locked(
                        token,
                        bundle,
                        snapshot,
                    ):
                        # The payout admission and both coordinator locks
                        # stabilize every field used by this predicate.
                        raise TemplateRefreshBlocked(
                            "prepared refresh publication did not produce a current token"
                        )
                    runtime._publish_tip_refresh_epoch_identity_locked(snapshot)
                    active = self._active_tip_refresh
                    if active is not None:
                        active[1].cancel()
                    self._active_tip_refresh = (token, cancel_event)
        return cancel_event

    def _clear_active_tip_refresh(
        self,
        token: TipRefreshValidationToken,
        cancel_event: FanoutCancellation,
    ) -> None:
        runtime = self._runtime
        with runtime.lock:
            active = self._active_tip_refresh
            if active is not None and active[0] is token and active[1] is cancel_event:
                self._active_tip_refresh = None

    def _prepared_tip_refresh_obsolete(
        self,
        validation_token: TipRefreshValidationToken,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        cancel_event: FanoutCancellation | None,
    ) -> bool:
        runtime = self._runtime
        if runtime.stop_event.is_set() or (
            cancel_event is not None and cancel_event.is_set()
        ):
            return True
        with runtime.lock:
            current = runtime._tip_refresh_token_current_locked(
                validation_token,
                bundle,
                snapshot,
            )
        if not current and cancel_event is not None:
            cancel_event.cancel()
        return not current

    def _fanout_prepared_tip_refresh(
        self,
        clients: list[ClientState],
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        *,
        observation_sequence: int | None = None,
        validation_token: TipRefreshValidationToken | None = None,
        preactivated_cancel_event: FanoutCancellation | None = None,
        executor: ThreadPoolExecutor | None = None,
        expected_active_jobs: dict[ClientState, PrismJobContext | None] | None = None,
        heartbeat_name: str,
    ) -> tuple[int, float | None, float | None, int]:
        runtime = self._runtime
        executor = executor or runtime.tip_refresh_executor()
        cancel_event = preactivated_cancel_event or FanoutCancellation()
        if observation_sequence is None:
            with runtime.lock:
                observation_sequence = int(
                    getattr(self, "current_tip_observation_sequence", 0)
                )
        if validation_token is None:
            validation_token = runtime._validate_prepared_tip_refresh(
                bundle,
                snapshot,
                observation_sequence,
            )
        if preactivated_cancel_event is None:
            runtime._activate_tip_refresh(
                validation_token,
                bundle,
                snapshot,
                cancel_event,
            )
        futures: dict[Future[RefreshResult], ClientState] = {}
        submitted_at: dict[Future[RefreshResult], float] = {}
        queued_cancellations: set[Future[RefreshResult]] = set()
        with runtime.lock:
            if expected_active_jobs is None:
                expected_active_jobs = {
                    client: client.active_job
                    for client in clients
                }
            # Vardiff drives every client toward the same share interval, so
            # the difficulty a client sustains is proportional to its hashrate.
            # Snapshot the proxies in the same locked pass as the job snapshot.
            hashrate_proxies = {
                client: (
                    client.vardiff_difficulty_estimate or client.share_difficulty
                )
                for client in clients
            }
        # Bounded submission admits at most max_inflight tasks at a time, so
        # queue priority alone cannot lift one client over a fleet wave that
        # has not been submitted yet. Order admission itself: stale-job
        # clients by descending hashrate, job-less clients last. A stale fast
        # client burns its full rate on old-tip work every second of refresh
        # lag, while a job-less client burns nothing while it waits regardless
        # of its configured difficulty (it also keeps its initial-delivery
        # queue priority once admitted).
        clients = sorted(
            clients,
            key=lambda ordered: (
                expected_active_jobs.get(ordered) is not None,
                hashrate_proxies.get(ordered, Decimal(0)),
            ),
            reverse=True,
        )
        clients_iter = iter(clients)
        max_inflight = max(1, int(runtime.tip_refresh_max_workers))

        def record_queued_cancellation(future: Future[RefreshResult]) -> None:
            if future in queued_cancellations:
                return
            queued_cancellations.add(future)
            elapsed = max(0.0, time.monotonic() - submitted_at[future])
            runtime.observe_job_build_elapsed(elapsed, {"executor_queue": elapsed})
            runtime._record_tip_refresh_cancellation("executor_queue")

        def cancel_pending_futures(pending: set[Future[RefreshResult]]) -> None:
            cancel_event.cancel()
            for future in tuple(pending):
                if future.cancel():
                    record_queued_cancellation(future)
                    runtime._record_tip_refresh_client_result("skipped")
                    pending.discard(future)

        def submit_available(pending: set[Future[RefreshResult]]) -> None:
            while (
                len(pending) < max_inflight
                and not runtime.stop_event.is_set()
                and not cancel_event.is_set()
            ):
                with runtime.lock:
                    token_current = runtime._tip_refresh_token_current_locked(
                        validation_token,
                        bundle,
                        snapshot,
                    )
                if not token_current:
                    cancel_event.cancel()
                    return
                try:
                    client = next(clients_iter)
                except StopIteration:
                    return
                submitted = time.monotonic()
                active_job = expected_active_jobs.get(client)
                if active_job is None:
                    priority = self._delivery_priority_initial
                elif runtime.client_tip_changed_for_snapshot(client, snapshot):
                    priority = self._delivery_priority_new_tip
                else:
                    priority = self._delivery_priority_same_tip
                future = runtime._submit_delivery_task(
                    executor,
                    runtime.send_prepared_job,
                    client,
                    bundle,
                    snapshot,
                    validation_token,
                    client.connection_id,
                    expected_active_jobs.get(client),
                    cancel_event,
                    submitted,
                    priority=priority,
                )
                runtime._tip_refresh_future_started()
                future.add_done_callback(runtime._tip_refresh_future_finished)
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
            last_live_trust_check = time.monotonic()
            try:
                submit_available(pending)
            except RuntimeError:
                cancel_pending_futures(pending)
                cancel_event.set()
                if pending:
                    wait(pending)
                if not runtime.stop_event.is_set():
                    runtime._schedule_tip_refresh_retry()
                    raise
            while pending:
                runtime._record_heartbeat(heartbeat_name)
                if runtime.stop_event.is_set() or cancel_event.is_set():
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
                        runtime._record_tip_refresh_client_result("skipped")
                        continue
                    try:
                        result = future.result()
                    except OSError:
                        runtime._record_tip_refresh_client_result("disconnected")
                        runtime.disconnect_client(client)
                        continue
                    except TemplateRefreshBlocked as exc:
                        runtime._record_tip_refresh_client_result("skipped")
                        invalidation = exc
                        cancel_pending_futures(pending)
                        continue
                    except Exception:
                        failed += 1
                        runtime._record_tip_refresh_client_result("failed")
                        with runtime.lock:
                            runtime.job_build_failure_count += 1
                        print(
                            "prism coordinator: prepared job fanout failed "
                            f"connection={client.connection_id} username={client.username}",
                            flush=True,
                        )
                        traceback.print_exc()
                        continue
                    runtime._record_tip_refresh_client_result(result.result)
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
                    and not runtime.stop_event.is_set()
                    and time.monotonic() - last_live_trust_check >= 1.0
                ):
                    # Validation tokens keep queued per-client deliveries
                    # RPC-free, but they cannot observe headers advancing
                    # ahead of blocks while the best-block hash stays fixed.
                    # Recheck the live chain view from the fanout driver about
                    # once per second and cancel every delivery still queued
                    # if the view becomes untrusted.
                    try:
                        trusted = runtime.ensure_reorg_reconciled_for_current_tip(
                            expected_tip_hash=snapshot.bestblockhash,
                        )
                        if not trusted:
                            raise _TipRefreshTrustBlocked(
                                "qbit chain view became untrusted during prepared fanout"
                            )
                        last_live_trust_check = time.monotonic()
                    except self._shutdown_error:
                        # Admission can close after the stop-event check above
                        # but before reconciliation enters its writer scope.
                        # Preserve the intentional shutdown signal so the
                        # poller cannot consume its template-failure budget.
                        cancel_pending_futures(pending)
                        raise
                    except TemplateRefreshBlocked as exc:
                        invalidation = exc
                    except Exception as exc:
                        invalidation = _TipRefreshTrustBlocked(
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
                        if not runtime.stop_event.is_set():
                            runtime._schedule_tip_refresh_retry()
                            raise
            if invalidation is not None:
                cancel_event.set()
                runtime._schedule_tip_refresh_retry()
                raise invalidation
            with runtime.lock:
                token_current = runtime._tip_refresh_token_current_locked(
                    validation_token,
                    bundle,
                    snapshot,
                )
            if not token_current:
                runtime._schedule_tip_refresh_retry()
                raise _TipRefreshFanoutSuperseded(
                    "prepared refresh was superseded during fanout; immediate retry scheduled"
                )
            try:
                post_fanout_tip = str(runtime.rpc.call("getbestblockhash"))
            except Exception as exc:
                runtime._schedule_tip_refresh_retry()
                raise TemplateRefreshBlocked(
                    "qbit tip validation failed after prepared fanout; "
                    "immediate retry scheduled"
                ) from exc
            if post_fanout_tip != snapshot.bestblockhash:
                cancel_event.set()
                runtime._schedule_tip_refresh_retry()
                raise _TipRefreshFanoutSuperseded(
                    "qbit tip changed during prepared fanout; immediate retry scheduled "
                    f"expected={snapshot.bestblockhash} current={post_fanout_tip}"
                )
            try:
                post_fanout_untrusted = bool(
                    getattr(runtime, "reorg_reconciler_enabled", True)
                    and runtime.qbit_chain_view_untrusted()
                )
            except Exception as exc:
                cancel_event.set()
                runtime._schedule_tip_refresh_retry()
                raise _TipRefreshTrustBlocked(
                    "qbit chain trust check failed after prepared fanout; "
                    "immediate retry scheduled"
                ) from exc
            if post_fanout_untrusted:
                cancel_event.set()
                runtime._schedule_tip_refresh_retry()
                raise _TipRefreshTrustBlocked(
                    "qbit chain view became untrusted during prepared fanout; "
                    "immediate retry scheduled"
                )
            with runtime.lock:
                token_current = runtime._tip_refresh_token_current_locked(
                    validation_token,
                    bundle,
                    snapshot,
                )
            if not token_current:
                cancel_event.set()
                runtime._schedule_tip_refresh_retry()
                raise _TipRefreshFanoutSuperseded(
                    "prepared refresh payout state changed during post-fanout "
                    "validation; immediate retry scheduled"
                )
            if last_delivery is not None and submitted_at:
                # Wall-clock span of the wave itself (first task submission
                # to last successful delivery), independent of the reconcile
                # and bundle-build stages the first/last_delivery histograms
                # include. At fleet scale this is the dominant staleness term
                # bounded by the delivery worker pool.
                runtime._observe_tip_refresh_seconds(
                    "fanout_wave",
                    max(0.0, last_delivery - min(submitted_at.values())),
                )
            return sent, first_delivery, last_delivery, failed
        finally:
            runtime._clear_active_tip_refresh(validation_token, cancel_event)

    def poll_qbit_tip_template_once(
        self,
        *,
        heartbeat_name: str = "qbit_blockpoll",
    ) -> int:
        """Drive one latest-wins refresh wave as its singleflight owner."""
        runtime = self._runtime
        runtime._ensure_tip_refresh_state()
        runtime._ensure_job_cache_state()
        refresh_started = time.monotonic()
        observed_best_tip: str | None = None
        try:
            observation_sequence = runtime._reserve_tip_observation_sequence()
            observed_best_tip = str(runtime.rpc.call("getbestblockhash"))
            if not runtime.observe_tip_for_refresh(
                observed_best_tip,
                observation_sequence=observation_sequence,
                mark_pending=False,
            ):
                runtime._schedule_tip_refresh_retry()
                raise TemplateRefreshSuperseded(
                    "tip/template poll was superseded before template fetch"
                )
        except TemplateRefreshSuperseded:
            if not getattr(runtime, "tip_refresh_epoch_fanout", False):
                runtime._record_coordination_blocked_refresh(time.monotonic())
                runtime._note_tip_refresh_attempt_failed(observed_best_tip)
            raise
        except Exception:
            runtime._clear_coordination_blocked_streak()
            runtime._record_template_refresh_failure(time.monotonic())
            runtime._note_tip_refresh_attempt_failed(observed_best_tip)
            raise
        assert observed_best_tip is not None
        if not self._tip_refresh_singleflight_lock.acquire(blocking=False):
            # A losing observer still publishes newer detection to cancellation
            # state, but it does not own (or count) a refresh wave.
            runtime.observe_tip_for_refresh(
                observed_best_tip,
                observation_sequence=observation_sequence,
                mark_pending=True,
            )
            runtime._schedule_tip_refresh_retry()
            return 0

        total_refreshed = 0
        saw_build_supersession = False
        saw_fanout_supersession = False
        outcome = "error"
        first_pass = True
        completed_passes = 0
        try:
            while True:
                try:
                    total_refreshed += runtime._poll_qbit_tip_template_pass_once(
                        heartbeat_name=heartbeat_name,
                        refresh_started=refresh_started,
                        observation_sequence=(
                            observation_sequence if first_pass else None
                        ),
                        observed_best_tip=(
                            observed_best_tip if first_pass else None
                        ),
                    )
                    first_pass = False
                    completed_passes += 1
                except PayoutStatePublicationBlocked:
                    outcome = "payout_blocked"
                    raise
                except _TipRefreshTrustBlocked:
                    outcome = "trust_blocked"
                    raise
                except JobBuildSuperseded:
                    saw_build_supersession = True
                    first_pass = False
                    completed_passes += 1
                    if runtime._tip_refresh_wave_reenters(completed_passes):
                        continue
                    outcome = runtime._interrupted_wave_outcome(
                        "build_superseded"
                    )
                    raise
                except TemplateRefreshSuperseded:
                    saw_fanout_supersession = True
                    first_pass = False
                    completed_passes += 1
                    if runtime._tip_refresh_wave_reenters(completed_passes):
                        continue
                    outcome = runtime._interrupted_wave_outcome(
                        "fanout_superseded"
                    )
                    raise
                except Exception:
                    outcome = "error"
                    raise

                if (
                    getattr(runtime, "tip_refresh_epoch_fanout", False)
                    and not runtime._tip_refresh_epoch_fixpoint_reached()
                ):
                    saw_fanout_supersession = True
                    if runtime._tip_refresh_wave_reenters(completed_passes):
                        continue
                    outcome = runtime._interrupted_wave_outcome(
                        "fanout_superseded"
                    )
                    return total_refreshed
                outcome = (
                    "build_superseded"
                    if saw_build_supersession
                    else "fanout_superseded"
                    if saw_fanout_supersession
                    else "completed"
                )
                return total_refreshed
        finally:
            try:
                runtime._record_tip_refresh_wave_outcome(outcome)
                wave_elapsed = time.monotonic() - refresh_started
                runtime._observe_tip_refresh_seconds(
                    "refresh",
                    wave_elapsed,
                )
                if wave_elapsed > 1.0:
                    # One line per slow wave (~1/min at current cadence).
                    # Reconstructing wave latency by pairing blockwait lines
                    # with "refreshed N client job(s)" lines cannot
                    # attribute time or see superseded passes; this can.
                    print(
                        "prism coordinator: tip refresh wave "
                        f"outcome={outcome} refreshed={total_refreshed} "
                        f"passes={completed_passes} "
                        f"elapsed={wave_elapsed:.3f}s",
                        flush=True,
                    )
            finally:
                self._tip_refresh_singleflight_lock.release()

    def _interrupted_wave_outcome(self, superseded_outcome: str) -> str:
        """Classify a wave that declined re-entry.

        With epoch fanout active, a wave that would have re-entered but for
        shutdown was terminated by shutdown, not by the supersession it
        recovered; reporting it superseded would pollute the terminal
        supersession counts that rollout evaluation watches. Legacy waves
        never re-enter, so their supersession outcome stands.
        """
        runtime = self._runtime
        if (
            getattr(runtime, "tip_refresh_epoch_fanout", False)
            and runtime.stop_event.is_set()
        ):
            return "shutdown"
        return superseded_outcome

    def _tip_refresh_wave_reenters(self, completed_passes: int) -> bool:
        """Gate owner-wave re-entry so tip churn cannot spin at RPC speed.

        Every re-entry pass costs template and reconcile RPC work against
        qbitd exactly when block cadence is fastest. The inter-pass backoff
        bounds that poll rate, and a wave that cannot converge within its
        pass budget hands the remainder to the scheduled retry, whose fresh
        wave restarts from the newest observed state.
        """
        runtime = self._runtime
        if not getattr(runtime, "tip_refresh_epoch_fanout", False):
            return False
        if runtime.stop_event.is_set():
            return False
        if completed_passes >= PRISM_TIP_REFRESH_WAVE_PASS_BUDGET:
            runtime._schedule_tip_refresh_retry()
            return False
        return not runtime.stop_event.wait(
            PRISM_TIP_REFRESH_REENTRY_BACKOFF_SECONDS
        )

    def _poll_qbit_tip_template_pass_once(
        self,
        *,
        heartbeat_name: str,
        refresh_started: float,
        observation_sequence: int | None = None,
        observed_best_tip: str | None = None,
    ) -> int:
        runtime = self._runtime
        runtime._ensure_tip_refresh_state()
        runtime._ensure_job_cache_state()
        publication_lock_acquired = False
        progress_refresh_active = False
        pending_signal_token: int | None = None
        try:
            observation_prevalidated = (
                observation_sequence is not None
                and observed_best_tip is not None
            )
            if observation_sequence is None:
                observation_sequence = runtime._reserve_tip_observation_sequence()
            # The interval poller has no push notification to mark priority for
            # it. Probe the cheap best-tip RPC before fetching and deriving the
            # template so CTV maintenance can yield as soon as a changed tip is
            # observed, rather than after reconciliation or bundle preparation.
            if observed_best_tip is None:
                observed_best_tip = str(runtime.rpc.call("getbestblockhash"))
            if (
                not observation_prevalidated
                and not runtime.observe_tip_for_refresh(
                    observed_best_tip,
                    observation_sequence=observation_sequence,
                    mark_pending=False,
                )
            ):
                runtime._schedule_tip_refresh_retry()
                raise TemplateRefreshSuperseded(
                    "tip/template poll was superseded before template fetch"
                )
            pending_signal_token = runtime._claim_tip_refresh_pending()
            with runtime.lock:
                poll_start_clients = tuple(
                    client
                    for client in runtime.clients
                    if runtime.client_can_receive_jobs(client)
                )
            with runtime.lock:
                current_tip = getattr(self, "current_tip_first_seen", None)
            if current_tip is not None and current_tip[0] != observed_best_tip:
                pending_signal_token = runtime._mark_tip_refresh_pending_for_poll(
                    pending_signal_token,
                    observation_sequence,
                )
            # Captured before reconciliation can begin: the pass may run on
            # the prefetch worker while this thread fetches the template, so
            # the mutation bracket must open before either starts.
            payout_generation_before_reconciliation = int(
                getattr(runtime, "_payout_state_generation", 0)
            )
            # Reconciliation is keyed by tip hash and the probe above already
            # observed the best hash, so a memo miss can run its full pass on
            # the prefetch worker while this thread fetches and derives the
            # template. Whether the pass can be skipped is decided at the
            # join below against the memo's state AT THAT TIME -- a tip that
            # flips away and back during the fetch evicts the entry so
            # post-flip pool-block state is re-proven -- and publication
            # still re-proves chain trust in _validate_prepared_tip_refresh
            # before any fanout. With the memo disabled (TTL 0) there is no
            # way to validate an overlapped pass across the fetch window, so
            # the poll keeps its historical serial pass.
            reconciler_enabled = bool(
                getattr(runtime, "reorg_reconciler_enabled", True)
            )
            reconcile_memo_enabled = reconciler_enabled and (
                float(
                    getattr(
                        runtime,
                        "reorg_reconcile_cache_seconds",
                        DEFAULT_PRISM_REORG_RECONCILE_CACHE_SECONDS,
                    )
                )
                > 0
            )
            reconcile_prefetch: Future[bool] | None = None
            reconcile_probe_started = time.monotonic()
            reconcile_detection_epoch = int(
                getattr(self, "tip_detection_epoch", 0)
            )
            if reconcile_memo_enabled and not runtime._reorg_reconcile_memo_fresh(
                observed_best_tip
            ):
                reconcile_prefetch = runtime._submit_reconcile_prefetch(
                    observed_best_tip
                )
            reconcile_probe_seconds = time.monotonic() - reconcile_probe_started
            snapshot = self._reuse_current_tip_template_snapshot(observed_best_tip)
            if snapshot is None:
                runtime._record_job_cache_event("template", hit=False)
                snapshot = runtime.fetch_qbit_tip_template_snapshot()
            if not runtime.observe_tip_for_refresh(
                snapshot.bestblockhash,
                observation_sequence=observation_sequence,
                mark_pending=False,
            ):
                runtime._schedule_tip_refresh_retry()
                raise TemplateRefreshSuperseded(
                    "tip/template poll was superseded during template fetch"
                )
            with runtime.lock:
                published_after_fetch = getattr(
                    self,
                    "current_tip_first_seen",
                    None,
                )
            if (
                published_after_fetch is not None
                and published_after_fetch[0] != snapshot.bestblockhash
            ):
                pending_signal_token = runtime._mark_tip_refresh_pending_for_poll(
                    pending_signal_token,
                    observation_sequence,
                )
            runtime._record_progress_tip_poll(snapshot)
            runtime._progress_refresh_started()
            progress_refresh_active = True
            runtime.pool_readiness_latched()
            with runtime.lock:
                previous_snapshot = self.tip_template_snapshot
                # Generation orders concurrent observations but is not itself
                # a template change. Repeated observations of identical work
                # must not trigger a clean fanout on every poll.
                snapshot_changed = previous_snapshot is not None and (
                    previous_snapshot.bestblockhash != snapshot.bestblockhash
                    or previous_snapshot.previousblockhash != snapshot.previousblockhash
                    or previous_snapshot.template_fingerprint
                    != snapshot.template_fingerprint
                )
                if snapshot_changed:
                    clients = [
                        client
                        for client in runtime.clients
                        if runtime.client_can_receive_jobs(client)
                    ]
                else:
                    clients = [
                        client
                        for client in runtime.clients
                        if runtime.client_can_receive_jobs(client)
                        and runtime.client_needs_tip_template_refresh(client, snapshot)
                    ]
                # Capture the exact job each client had when this refresh pass
                # selected it. A Vardiff/authorize path may install intervening
                # work while the shared bundle is prepared or while its task
                # waits in the executor queue. Artifact generations let the
                # task replace stale intervening work while preserving work
                # produced from a template stored after this snapshot.
                expected_active_jobs = {
                    client: client.active_job
                    for client in clients
                }

            if clients and snapshot_changed:
                pending_signal_token = runtime._mark_tip_refresh_pending_for_poll(
                    pending_signal_token,
                    observation_sequence,
                )

            refreshed = 0
            build_failures = 0
            first_delivery: float | None = None
            last_delivery: float | None = None
            runtime._raise_if_tip_refresh_superseded(
                snapshot,
                observation_sequence,
            )
            reconcile_source = "serial"
            reconcile_join_started = time.monotonic()
            try:
                if (
                    reconcile_memo_enabled
                    and int(getattr(self, "tip_detection_epoch", 0))
                    == reconcile_detection_epoch
                    and runtime._reorg_reconcile_memo_fresh(
                        snapshot.bestblockhash
                    )
                ):
                    # Fresh at join time AND no detection interleaved the
                    # fetch. Both are required: a flip away and back evicts
                    # the entry, but a pass whose execution straddled the
                    # flip can re-arm it afterwards (the latest detected
                    # hash matches again), and its proof belongs to the
                    # closed epoch. Any epoch movement lands in the serial
                    # re-prove branches below.
                    reconcile_source = (
                        "overlap" if reconcile_prefetch is not None else "memo_hit"
                    )
                    if reconcile_prefetch is not None:
                        runtime._discard_stale_reconcile_prefetch(
                            reconcile_prefetch
                        )
                        reconcile_prefetch = None
                    reorg_reconciled = True
                elif (
                    reconcile_prefetch is not None
                    and snapshot.bestblockhash == observed_best_tip
                ):
                    reconcile_source = "overlap"
                    reorg_reconciled = runtime._join_reconcile_prefetch_bounded(
                        reconcile_prefetch
                    )
                    if reorg_reconciled and (
                        int(getattr(self, "tip_detection_epoch", 0))
                        != reconcile_detection_epoch
                    ):
                        # A detection interleaved the fetch (a flip away, or
                        # away and back to this same hash): cached proofs
                        # were evicted and the overlapped pass may have run
                        # in the closed epoch. Re-prove off-thread with the
                        # same bounded join -- the crawl that slows the
                        # overlap join slows this re-prove identically.
                        reconcile_source = "serial"
                        reorg_reconciled = runtime._reconcile_snapshot_tip_bounded(
                            snapshot.bestblockhash
                        )
                    # A trust flip after the pass completed (headers running
                    # ahead with no detection) is deliberately NOT re-checked
                    # here: prepared fanout re-proves the live chain view in
                    # _validate_prepared_tip_refresh, and sequential issuance
                    # re-proves it per client in
                    # ensure_reorg_reconciled_for_current_tip, whose trust
                    # check is never cached. A second live check here would
                    # break the one-trust-validation-per-refresh economy.
                else:
                    if reconcile_prefetch is not None:
                        # The tip moved between the probe and the template
                        # fetch; the prefetched pass proved a superseded
                        # hash. Reconcile the snapshot tip off-thread with
                        # the same bounded join: the crawl that slows the
                        # overlap join reaches this branch through exactly
                        # the churn that moves tips mid-fetch, and the poll
                        # loop must never park on it here either.
                        runtime._discard_stale_reconcile_prefetch(
                            reconcile_prefetch
                        )
                        reconcile_prefetch = None
                    reorg_reconciled = runtime._reconcile_snapshot_tip_bounded(
                        snapshot.bestblockhash
                    )
            except (self._shutdown_error, FuturesCancelledError):
                # Shutdown may close writer admission (or cancel the queued
                # prefetch) after this refresh has fetched a snapshot. Leave
                # the refresh incomplete and let the controlled shutdown
                # proceed without consuming the template failure budget or
                # taking the hard-exit path.
                return 0
            except (TemplateRefreshSuperseded, PayoutStatePublicationBlocked):
                raise
            except Exception as exc:
                raise _TipRefreshTrustBlocked(
                    "qbit reorg reconciliation failed before refresh preparation"
                ) from exc
            finally:
                # The effective serial reconcile cost of this refresh: the
                # memo probe plus whatever wait the template fetch did not
                # absorb. Observe failures too so a slow reconcile that
                # blocks the refresh still shows up in the histogram.
                runtime._observe_tip_refresh_build_phase(
                    "reorg_reconcile",
                    reconcile_probe_seconds
                    + (time.monotonic() - reconcile_join_started),
                )
                if reconciler_enabled:
                    runtime._record_reorg_reconcile_lookup(
                        "tip_refresh",
                        reconcile_source,
                    )
            if not reorg_reconciled:
                raise _TipRefreshTrustBlocked(
                    "qbit chain view remained untrusted after reorg reconciliation"
                )
            payout_generation_after_reconciliation = int(
                getattr(runtime, "_payout_state_generation", 0)
            )
            if (
                payout_generation_after_reconciliation
                != payout_generation_before_reconciliation
            ):
                # A same-tip reconciliation can invalidate signed payout state
                # even when no client needed template work at initial
                # selection. Reselect after the ledger mutation so every old-
                # generation job is replaced from the post-reorg snapshot.
                with runtime.lock:
                    clients = [
                        client
                        for client in runtime.clients
                        if runtime.client_can_receive_jobs(client)
                        and runtime.client_needs_tip_template_refresh(client, snapshot)
                    ]
                    expected_active_jobs = {
                        client: client.active_job
                        for client in clients
                    }
                # Reconciliation itself minted the payout-pending token and
                # this pass has deliberately reselected clients from that new
                # generation. Adopt exactly that token; completion still checks
                # both token ownership and payout/detected-tip currentness, so
                # a later producer cannot be cleared accidentally.
                pending_signal_token = runtime._claim_tip_refresh_pending()
            with runtime.lock:
                selected_clients_list = list(clients)
                selected_client_set = set(clients)
                for client in poll_start_clients:
                    if client in selected_client_set:
                        continue
                    if snapshot_changed or runtime.client_needs_tip_template_refresh(
                        client,
                        snapshot,
                    ):
                        selected_clients_list.append(client)
                        selected_client_set.add(client)
                        expected_active_jobs[client] = client.active_job
                selected_clients = tuple(selected_clients_list)
            # Prepared-mode selection must cover every client this pass can
            # end up serving. Revalidation only filters selected_clients, so
            # deriving from the narrower initial list could publish authority
            # through the sequential path with no shared bundle while
            # poll-start targets still need prepared ready work.
            use_prepared_fanout = bool(
                selected_clients
                and getattr(runtime, "_pool_ready_latched", False)
            )
            ready_mode = bool(getattr(runtime, "_pool_ready_latched", False))
            bundle: CachedJobBundle | None = None
            validation_token: TipRefreshValidationToken | None = None
            preactivated_cancel_event: FanoutCancellation | None = None
            prepared_executor: ThreadPoolExecutor | None = None
            if use_prepared_fanout:
                runtime._raise_if_tip_refresh_superseded(
                    snapshot,
                    observation_sequence,
                )
                try:
                    bundle = runtime.prepare_tip_refresh_bundle(
                        snapshot,
                        priority_requested_monotonic=refresh_started,
                    )
                except PayoutStatePublicationBlocked:
                    for _client in selected_clients:
                        runtime._record_tip_refresh_client_result("skipped")
                    runtime._schedule_tip_refresh_retry()
                    raise
                except TemplateRefreshBlocked:
                    for _client in selected_clients:
                        runtime._record_tip_refresh_client_result("failed")
                    raise
                if (
                    bundle.payout_state_generation
                    != payout_generation_after_reconciliation
                ):
                    # Request construction can observe a payout generation
                    # published after reconciliation and still return a fully
                    # coherent newest-generation bundle. Adopt it only while
                    # it remains the current immutable payout pointer, then
                    # reselect the complete fleet so no old-generation client
                    # is omitted merely because selection preceded the build.
                    with runtime._job_cache_lock:
                        current_payout_generation = (
                            runtime._payout_state_generation
                        )
                        current_payout_artifact = (
                            runtime._published_payout_state.artifact
                        )
                    if (
                        bundle.payout_state_generation
                        != current_payout_generation
                        or bundle.build_key is None
                        or current_payout_artifact is None
                        or bundle.build_key.payout_artifact_sha256
                        != current_payout_artifact.prior_balances_sha256
                    ):
                        runtime._schedule_tip_refresh_retry()
                        raise TemplateRefreshSuperseded(
                            "payout state changed after refresh preparation; "
                            "immediate retry scheduled"
                        )
                    payout_generation_after_reconciliation = (
                        current_payout_generation
                    )
                    with runtime.lock:
                        selected_clients = tuple(
                            client
                            for client in runtime.clients
                            if runtime.client_can_receive_jobs(client)
                            and runtime.client_needs_tip_template_refresh(
                                client,
                                snapshot,
                            )
                        )
                        expected_active_jobs = {
                            client: client.active_job
                            for client in selected_clients
                        }
                    pending_signal_token = runtime._claim_tip_refresh_pending()

            runtime._raise_if_tip_refresh_superseded(
                snapshot,
                observation_sequence,
            )
            # Construction is complete. Acquire the short publication lane
            # only for final validation, snapshot publication, and current
            # client selection. Other observations and replacement builds can
            # progress while an obsolete builder is still unwinding.
            while not self._tip_refresh_lock.acquire(
                timeout=PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS
            ):
                runtime._record_heartbeat(heartbeat_name)
                if runtime.stop_event.is_set():
                    return 0
                runtime._probe_tip_while_refresh_waiting()
                runtime._raise_if_tip_refresh_superseded(
                    snapshot,
                    observation_sequence,
                )
            publication_lock_acquired = True
            with runtime._job_cache_lock:
                current_payout_generation = runtime._payout_state_generation
                current_payout_artifact = runtime._published_payout_state.artifact
            if (
                current_payout_generation
                != payout_generation_after_reconciliation
            ):
                runtime._schedule_tip_refresh_retry()
                raise TemplateRefreshSuperseded(
                    "payout state changed before refresh publication; "
                    "immediate retry scheduled"
                )
            if (
                bundle is not None
                and (
                    current_payout_artifact is None
                    or bundle.build_key is None
                    or bundle.build_key.payout_artifact_sha256
                    != current_payout_artifact.prior_balances_sha256
                )
            ):
                runtime._schedule_tip_refresh_retry()
                raise TemplateRefreshBlocked(
                    "complete build key changed before refresh publication"
                )

            # A ready-pool pass must build and validate its immutable shared
            # bundle before publishing submit authority. Otherwise a cache,
            # derivation, or final chain-validation failure can invalidate
            # retained work without a deliverable replacement. Sequential /
            # collection work has no shared preparation stage, so it commits
            # here immediately before its worker-specific builds.
            if use_prepared_fanout:
                assert bundle is not None
                # Acquire infrastructure before publishing authority. Once the
                # active token is installed, every subsequent fanout exit is
                # covered by its cancellation/cleanup finally block.
                prepared_executor = runtime.tip_refresh_executor()
                # Parent metadata is cleanup-only, but fetch it before the
                # final live/trust guard so validation remains the last RPC
                # boundary before atomic publication.
                try:
                    prepared_parent_hash = runtime._fetch_tip_parent_hash(
                        snapshot.bestblockhash
                    )
                except Exception:
                    prepared_parent_hash = None
                validation_token = runtime._validate_prepared_tip_refresh(
                    bundle,
                    snapshot,
                    observation_sequence,
                )
                preactivated_cancel_event = runtime._publish_prepared_tip_refresh(
                    validation_token,
                    bundle,
                    snapshot,
                    parent_hash=prepared_parent_hash,
                )
            else:
                if not runtime.observe_tip_first_seen(
                    snapshot.bestblockhash,
                    observation_sequence=observation_sequence,
                    publish_refresh_observation=True,
                    published_snapshot=snapshot,
                ):
                    raise TemplateRefreshSuperseded(
                        "tip/template poll was superseded by a newer tip observation"
                    )
                runtime.prune_evicted_job_graveyard(force=False)
                with runtime.lock:
                    current_tip = getattr(self, "current_tip_first_seen", None)
                    if (
                        current_tip is None
                        or current_tip[0] != snapshot.bestblockhash
                        or int(getattr(self, "current_tip_observation_sequence", 0))
                        != observation_sequence
                    ):
                        raise TemplateRefreshSuperseded(
                            "tip/template poll was superseded before snapshot publication"
                        )
                    if self.tip_template_snapshot is not snapshot:
                        raise TemplateRefreshBlocked(
                            "tip/template snapshot was not atomically published"
                        )

            dropped_client_results: list[str] = []
            with runtime.lock:
                # Revalidate the originally selected targets at publication.
                # New connections keep their own pending/retained wake; this
                # pass must not consume work that appeared after construction.
                # Preserve the pre-build expected job pointer so an intervening
                # authorize/Vardiff delivery is never overwritten.
                current_clients: list[ClientState] = []
                for client in selected_clients:
                    if client not in runtime.clients:
                        dropped_client_results.append("disconnected")
                    elif not runtime.client_can_receive_jobs(client):
                        dropped_client_results.append("skipped")
                    elif not runtime.client_needs_tip_template_refresh(client, snapshot):
                        dropped_client_results.append("skipped")
                    else:
                        current_clients.append(client)
                clients = current_clients

            for result in dropped_client_results:
                runtime._record_tip_refresh_client_result(result)

            if bundle is not None and not bundle.collection_only:
                with runtime._job_cache_lock:
                    if (
                        bundle.payout_state_generation
                        == runtime._payout_state_generation
                    ):
                        runtime._prepared_ready_snapshot = snapshot
                        runtime._prepared_ready_bundle = bundle

            if (
                not use_prepared_fanout
                and clients
                and getattr(runtime, "_pool_ready_latched", False)
            ):
                # Publication already committed through the sequential path,
                # so a ready-pool target that appeared after build selection
                # has no shared bundle to receive. Leave the pending marker
                # armed and let the immediate retry build for it.
                runtime._mark_tip_refresh_pending(observation_sequence)
                runtime._schedule_tip_refresh_retry()
                raise TemplateRefreshBlocked(
                    "current clients appeared after build selection; retry scheduled"
                )
            self._tip_refresh_lock.release()
            publication_lock_acquired = False

            with runtime.lock:
                progress_eligible_client = any(
                    runtime.client_can_receive_jobs(client)
                    for client in runtime.clients
                )
            if use_prepared_fanout or not progress_eligible_client:
                # Ready-mode work was prepared above. With no eligible clients,
                # publishing the reconciled snapshot is sufficient; collection
                # work with a live identity is not published until its worker-
                # specific bundle is actually delivered below.
                runtime._record_progress_publication(
                    snapshot,
                    payout_generation_after_reconciliation,
                )

            if not ready_mode:
                with runtime.lock:
                    eligible_collection_client = any(
                        runtime.client_can_receive_jobs(client)
                        for client in runtime.clients
                    )
                if not eligible_collection_client:
                    runtime._retain_collection_refresh(
                        snapshot,
                        observation_sequence,
                        payout_generation_after_reconciliation,
                    )

            if use_prepared_fanout:
                assert bundle is not None
                (
                    refreshed,
                    first_delivery,
                    last_delivery,
                    build_failures,
                ) = runtime._fanout_prepared_tip_refresh(
                    clients,
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
                for client in clients:
                    if runtime.stop_event.is_set():
                        break
                    # Collection bundles are worker-specific, so build and
                    # validate each selected target independently.
                    runtime._record_heartbeat(heartbeat_name)
                    with runtime.lock:
                        target_connected = client in runtime.clients
                        target_eligible = (
                            target_connected
                            and runtime.client_can_receive_jobs(client)
                        )
                    if not target_connected:
                        runtime._record_tip_refresh_client_result("disconnected")
                        continue
                    if not target_eligible:
                        runtime._record_tip_refresh_client_result("skipped")
                        continue
                    try:
                        if runtime.maybe_send_job(
                            client,
                            clean_jobs=runtime.client_tip_changed_for_snapshot(client, snapshot),
                            raise_on_reorg_failure=True,
                            raise_on_build_failure=True,
                            tip_refresh_snapshot=snapshot,
                            tip_refresh_observation_sequence=observation_sequence,
                        ):
                            delivered = time.monotonic()
                            refreshed += 1
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
                            runtime._record_tip_refresh_client_result("sent")
                        else:
                            runtime._record_tip_refresh_client_result("skipped")
                    except self._job_build_failed_error:
                        build_failures += 1
                        runtime._record_tip_refresh_client_result("failed")
                    except OSError:
                        runtime._record_tip_refresh_client_result("disconnected")
                        runtime.disconnect_client(client)

                if not ready_mode:
                    with runtime.lock:
                        eligible_collection_client = any(
                            runtime.client_can_receive_jobs(client)
                            for client in runtime.clients
                        )
                    if not eligible_collection_client:
                        runtime._retain_collection_refresh(
                            snapshot,
                            observation_sequence,
                            payout_generation_after_reconciliation,
                        )
                        runtime._record_progress_publication(
                            snapshot,
                            payout_generation_after_reconciliation,
                        )

                if clients:
                    try:
                        post_fanout_tip = str(runtime.rpc.call("getbestblockhash"))
                    except Exception as exc:
                        runtime._schedule_tip_refresh_retry()
                        raise TemplateRefreshBlocked(
                            "qbit tip validation failed after sequential refresh; "
                            "immediate retry scheduled"
                        ) from exc
                    if post_fanout_tip != snapshot.bestblockhash:
                        runtime._schedule_tip_refresh_retry()
                        raise TemplateRefreshSuperseded(
                            "qbit tip changed during sequential refresh; "
                            "immediate retry scheduled "
                            f"expected={snapshot.bestblockhash} current={post_fanout_tip}"
                        )
                    if int(getattr(runtime, "_payout_state_generation", 0)) != (
                        payout_generation_after_reconciliation
                    ):
                        runtime._schedule_tip_refresh_retry()
                        raise TemplateRefreshSuperseded(
                            "payout state changed during sequential refresh; "
                            "immediate retry scheduled"
                        )

            if refreshed == 0 and build_failures:
                raise TemplateRefreshBlocked(
                    f"job builds failed for {build_failures} client(s); no refreshed work was issued"
                )
            if refreshed:
                with runtime.lock:
                    self.tip_refresh_job_count += refreshed
                assert first_delivery is not None and last_delivery is not None
                runtime._observe_tip_refresh_seconds(
                    "first_delivery",
                    first_delivery - refresh_started,
                )
                runtime._observe_tip_refresh_seconds(
                    "last_delivery",
                    last_delivery - refresh_started,
                )
            if not runtime._clear_tip_refresh_pending_for_completed_refresh(
                snapshot,
                observation_sequence,
                payout_generation_after_reconciliation,
                pending_signal_token,
            ):
                # A newer tip or payout mutation won after the last delivery
                # guard. Preserve its pending token and retry immediately.
                pending_signal_token = None
                runtime._schedule_tip_refresh_retry()
                raise TemplateRefreshSuperseded(
                    "tip or payout state changed before refresh completion; "
                    "immediate retry scheduled"
                )
            self.last_successful_template_refresh_monotonic = time.monotonic()
            self.template_refresh_failure_started_monotonic = None
            runtime._clear_coordination_blocked_streak()
            self._clear_tip_refresh_failure_holdoff()
            # A completed pass reconfirms that the coherent snapshot remained
            # current through publication and fanout. Refresh the liveness
            # stamp so a legitimately long, progressing pass does not become
            # stale the instant its active marker is cleared.
            runtime._record_progress_tip_poll(snapshot)
            return refreshed
        except TemplateRefreshSuperseded:
            # Coordination-blocked refreshes -- a superseded tip, a pending
            # payout publication fence, a refresh raced by payout mutation --
            # are churn between healthy components, not qbitd unhealthiness.
            # They do not arm the ordinary failure budget, but their oldest
            # continuous streak has its own longer restart deadline. Re-raise
            # so callers still schedule their immediate retry. Plain
            # TemplateRefreshBlocked stays budgeted below: it also wraps
            # genuine failures (job builds failing, malformed template
            # artifacts, untrusted chain views) whose persistence must still
            # take the ordinary restart path.
            # The retry-spacing stamp still applies: churn against an
            # unchanged tip re-arms the poller no faster than the holdoff,
            # while a genuinely newer tip zeroes it immediately.
            if not getattr(runtime, "tip_refresh_epoch_fanout", False):
                runtime._record_coordination_blocked_refresh(time.monotonic())
                runtime._note_tip_refresh_attempt_failed(observed_best_tip)
            raise
        except PayoutStatePublicationBlocked:
            runtime._record_coordination_blocked_refresh(time.monotonic())
            runtime._note_tip_refresh_attempt_failed(observed_best_tip)
            raise
        except Exception:
            runtime._clear_coordination_blocked_streak()
            runtime._record_template_refresh_failure(time.monotonic())
            runtime._note_tip_refresh_attempt_failed(observed_best_tip)
            raise
        finally:
            if publication_lock_acquired:
                self._tip_refresh_lock.release()
            if progress_refresh_active:
                runtime._progress_refresh_finished()

    def _probe_tip_while_refresh_waiting(self) -> None:
        """Detect a changed live tip without entering the heavy refresh lane."""
        runtime = self._runtime
        observation_sequence = runtime._reserve_tip_observation_sequence()
        try:
            observed_tip = str(runtime.rpc.call("getbestblockhash"))
        except Exception:
            # The owning refresh still has to unwind or complete. Preserve its
            # pending state and let the next bounded lock wait probe again.
            return
        runtime.observe_tip_for_refresh(
            observed_tip,
            observation_sequence=observation_sequence,
        )

    def _detected_tip_supersedes_locked(
        self,
        tip_hash: str,
        observation_sequence: int,
    ) -> bool:
        latest = getattr(self, "latest_detected_tip", None)
        return bool(
            latest is not None
            and latest[0] != tip_hash
            and latest[1] > observation_sequence
        )

    def _raise_if_tip_refresh_superseded(
        self,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
    ) -> None:
        """Stop obsolete work before entering another expensive phase."""
        runtime = self._runtime
        with runtime.lock:
            superseded = runtime._detected_tip_supersedes_locked(
                snapshot.bestblockhash,
                observation_sequence,
            )
        if superseded:
            runtime._schedule_tip_refresh_retry()
            raise TemplateRefreshSuperseded(
                "tip/template poll was superseded by a newer tip observation "
                "before refresh preparation"
            )

    def _reserve_tip_observation_sequence(self) -> int:
        runtime = self._runtime
        with runtime.lock:
            sequence = int(getattr(self, "tip_observation_sequence", 0)) + 1
            self.tip_observation_sequence = sequence
            return sequence

    def observe_tip_for_refresh(
        self,
        tip_hash: str,
        *,
        observation_sequence: int | None = None,
        mark_pending: bool = True,
    ) -> bool:
        """Record tip detection without publishing share-validation authority.

        A waiter outside ``_tip_refresh_lock`` must be able to cancel obsolete
        bundle construction and fanout promptly. It must not update
        ``current_tip_first_seen``: that value invalidates old jobs, so the
        winning refresh publishes it only after replacement work is prepared
        and validated.
        """
        runtime = self._runtime
        if observation_sequence is None:
            observation_sequence = runtime._reserve_tip_observation_sequence()
        runtime._ensure_job_cache_state()
        runtime._ensure_tip_refresh_state()
        # Before any sequencing decision: an own-candidate hash arriving as
        # the tip is acceptance evidence (blockwait can see it before -- or
        # instead of -- the direct submitblock ack) and must be registered
        # even when this observation loses the sequence race below.
        runtime._note_tip_observation_for_candidates(tip_hash)
        now = time.monotonic()
        active_to_cancel: FanoutCancellation | None = None
        should_mark_pending = False
        with runtime.lock:
            latest = getattr(self, "latest_detected_tip", None)
            if latest is not None and observation_sequence < latest[1]:
                return latest[0] == tip_hash
            published = getattr(self, "current_tip_first_seen", None)
            prior_detected_hash = (
                latest[0]
                if latest is not None
                else published[0]
                if published is not None
                else None
            )
            detection_changed = (
                prior_detected_hash is not None
                and prior_detected_hash != tip_hash
            )
            if detection_changed:
                runtime._evict_reorg_reconcile_memo_for_new_tip_locked(tip_hash)
                # Bumped exactly when cached reconcile proofs are dropped:
                # a refresh's join compares this epoch to tell whether any
                # flip (away, or away and back) interleaved its template
                # fetch and invalidated a pass that ran concurrently.
                self.tip_detection_epoch = (
                    int(getattr(self, "tip_detection_epoch", 0)) + 1
                )
            if (
                detection_changed
                and runtime._payout_state_source[1] != tip_hash
            ):
                # Supersede an in-progress immutable payout candidate as soon
                # as the newer tip is detected, without publishing that tip as
                # submit authority before replacement work is ready.
                source_generation = runtime._payout_state_source[0] + 1
                runtime._payout_state_source = (
                    source_generation,
                    tip_hash,
                    "external_tip",
                    now,
                )
            if latest is None or detection_changed:
                runtime._mint_tip_refresh_epoch_locked(
                    tip_hash=tip_hash,
                    payout_state_generation=int(
                        getattr(runtime, "_payout_state_generation", 0)
                    ),
                    started_monotonic=now,
                )
            self.latest_detected_tip = (tip_hash, observation_sequence)
            replacement_needed = published is None or published[0] != tip_hash
            if published is not None and published[0] == tip_hash:
                # A live observation has returned to (or reconfirmed) the
                # published generation. This closes any unpublished divergence
                # epoch without changing the published snapshot itself.
                self.current_tip_observed_monotonic = now
                self.tip_refresh_divergence_started_monotonic = None
            elif (
                published is not None
                and getattr(
                    self,
                    "tip_refresh_divergence_started_monotonic",
                    None,
                )
                is None
            ):
                # Anchor the lease to the first departure from the published
                # tip. Further B -> C observations deliberately do not renew it.
                self.tip_refresh_divergence_started_monotonic = now
            pending_already = bool(
                getattr(self, "_tip_refresh_pending_event", None)
                and self._tip_refresh_pending_event.is_set()
            )
            active = getattr(self, "_active_tip_refresh", None)
            if (
                active is not None
                and active[0].tip_hash != tip_hash
                and active[0].observation_sequence < observation_sequence
            ):
                active_to_cancel = active[1]
            should_mark_pending = bool(
                mark_pending
                and (
                    detection_changed
                    or (replacement_needed and not pending_already)
                )
            )

        if active_to_cancel is not None:
            active_to_cancel.cancel()
        if detection_changed:
            runtime._progress_note_refresh_pending(now)
            # In-flight constructions for older parents can no longer win;
            # stop them at detection so replacement preparation starts
            # immediately, well before publication moves submit authority.
            runtime._cancel_obsolete_job_bundle_builds(current_tip=tip_hash)
            runtime._cancel_obsolete_job_builds("chain tip superseded")
        if should_mark_pending:
            runtime._mark_tip_refresh_pending(observation_sequence)
            runtime._schedule_tip_refresh_retry()
        return True

    def observe_tip_first_seen(
        self,
        tip_hash: str,
        *,
        observation_sequence: int | None = None,
        publish_refresh_observation: bool = False,
        published_snapshot: QbitTipTemplateSnapshot | None = None,
    ) -> bool:
        """Publish a tip (and, when supplied, its coherent template snapshot).

        Detection and publication are deliberately separate. Callers doing a
        prepared refresh must finish bundle construction and final validation
        before invoking this method.
        """
        runtime = self._runtime
        if (
            published_snapshot is not None
            and published_snapshot.bestblockhash != tip_hash
        ):
            raise ValueError("published snapshot does not match tip hash")
        if observation_sequence is None:
            observation_sequence = runtime._reserve_tip_observation_sequence()
        if not runtime.observe_tip_for_refresh(
            tip_hash,
            observation_sequence=observation_sequence,
            mark_pending=False,
        ):
            return False
        now = time.monotonic()
        with runtime.lock:
            current_sequence = int(
                getattr(self, "current_tip_observation_sequence", 0)
            )
            if (
                observation_sequence < current_sequence
                or runtime._detected_tip_supersedes_locked(
                    tip_hash,
                    observation_sequence,
                )
            ):
                return False
            first_seen = getattr(self, "current_tip_first_seen", None)
            if first_seen is not None and first_seen[0] == tip_hash:
                # A same-tip re-observation proves the tip view is live; the
                # freshness stamp bounds submit_stale_check_tip reuse.
                self.current_tip_observed_monotonic = now
                active = getattr(self, "_active_tip_refresh", None)
                # A routine blockwait/poll observation of the same hash carries
                # no newer template. While that hash is actively fanning out,
                # do not invalidate its token merely by advancing the global
                # observation sequence. The next real refresh observation can
                # advance it after the active fanout clears.
                if publish_refresh_observation and (
                    active is None or active[0].tip_hash != tip_hash
                ):
                    self.current_tip_observation_sequence = observation_sequence
                if published_snapshot is not None:
                    self.tip_template_snapshot = published_snapshot
                    runtime._publish_tip_refresh_epoch_identity_locked(
                        published_snapshot
                    )
                self.tip_refresh_divergence_started_monotonic = None
                return True

        # Fetch optional cleanup metadata before the atomic publication. A slow
        # parent RPC must not create a window where submits see the new tip but
        # the winning refresh has not yet published its coherent snapshot.
        try:
            parent_hash = runtime._fetch_tip_parent_hash(tip_hash)
        except Exception:
            parent_hash = None

        with runtime.lock:
            current_sequence = int(
                getattr(self, "current_tip_observation_sequence", 0)
            )
            if (
                observation_sequence < current_sequence
                or runtime._detected_tip_supersedes_locked(
                    tip_hash,
                    observation_sequence,
                )
            ):
                return False
            first_seen = getattr(self, "current_tip_first_seen", None)
            if first_seen is not None and first_seen[0] == tip_hash:
                self.current_tip_observed_monotonic = now
                active = getattr(self, "_active_tip_refresh", None)
                if publish_refresh_observation and (
                    active is None or active[0].tip_hash != tip_hash
                ):
                    self.current_tip_observation_sequence = observation_sequence
                if published_snapshot is not None:
                    self.tip_template_snapshot = published_snapshot
                    runtime._publish_tip_refresh_epoch_identity_locked(
                        published_snapshot
                    )
                self.tip_refresh_divergence_started_monotonic = None
                return True

            tip_changed = first_seen is not None
            # The first tip this process publishes is a startup baseline, not
            # a tip flip: a None stamp keeps stale grace closed.
            self.current_tip_first_seen = (
                tip_hash,
                now if tip_changed else None,
            )
            self.current_tip_observation_sequence = observation_sequence
            self.current_tip_observed_monotonic = now
            if published_snapshot is not None:
                self.tip_template_snapshot = published_snapshot
                runtime._publish_tip_refresh_epoch_identity_locked(
                    published_snapshot
                )
            self.tip_refresh_divergence_started_monotonic = None
            # Retained collection work is reusable throughout detection and
            # preparation, but never after authority moves to a different tip.
            self._retained_collection_refresh = None
            if parent_hash is None:
                self.current_tip_parent = None
            else:
                self.current_tip_parent = (tip_hash, parent_hash)
            # Reclassify formerly same-tip entries immediately. On mainnet the
            # zero stale-grace TTL removes them in this pass; on other chains
            # the actual chain parent removes multi-tip-behind entries while
            # the independently configured grace lifetime protects one-back.
            runtime.prune_evicted_job_graveyard(now=now, force=True)
        if tip_changed:
            # A retained ready bundle belongs to the previously published tip
            # and can never satisfy the consumer's published-snapshot identity
            # check again; release it as soon as authority moves.
            with runtime._job_cache_lock:
                runtime._prepared_ready_bundle = None
                runtime._prepared_ready_snapshot = None
        return True

    def reorg_proof_snapshot(self) -> tuple[str | None, int]:
        """Atomic (latest detected tip hash, detection epoch) reorg proof.

        The reorg reconciler remains coordinator-owned at this layer; its
        later owner consumes this snapshot instead of reading the two fields
        separately, so a detection between the reads can never pair a new tip
        with a stale epoch.
        """
        runtime = self._runtime
        with runtime.lock:
            latest = getattr(self, "latest_detected_tip", None)
            return (
                latest[0] if latest is not None else None,
                int(getattr(self, "tip_detection_epoch", 0)),
            )

    def _fetch_tip_parent_hash(self, tip_hash: str) -> str | None:
        runtime = self._runtime
        block = runtime.rpc.call("getblock", [tip_hash])
        if not isinstance(block, dict):
            return None
        parent = str(block.get("previousblockhash", "") or "")
        if not parent:
            return None
        return parent

    def current_tip_parent_hash(self, tip_hash: str) -> str | None:
        runtime = self._runtime
        with runtime.lock:
            cached = getattr(self, "current_tip_parent", None)
            if cached is not None and cached[0] == tip_hash:
                return cached[1]
            first_seen = getattr(self, "current_tip_first_seen", None)
            observed_sequence = (
                int(getattr(self, "current_tip_observation_sequence", 0))
                if first_seen is not None and first_seen[0] == tip_hash
                else None
            )
        parent = runtime._fetch_tip_parent_hash(tip_hash)
        if parent is None:
            return None
        with runtime.lock:
            current = getattr(self, "current_tip_first_seen", None)
            if (
                observed_sequence is not None
                and current is not None
                and current[0] == tip_hash
                and int(getattr(self, "current_tip_observation_sequence", 0))
                == observed_sequence
            ):
                self.current_tip_parent = (tip_hash, parent)
        return parent

    def submit_stale_check_tip(self) -> str:
        """Best-known chain tip for per-share submit classification.

        Prefers the tip for which the refresh path already published coherent
        work (reconfirmed at least every PRISM_BLOCKPOLL_SECONDS while healthy)
        so mining.submit never blocks on a getbestblockhash RPC per share. This
        also removes the submit-races-ahead-of-the-refresh failure mode: a
        submit-path RPC can observe a new tip seconds before jobs refresh, and
        with PRISM_STRATUM_STALE_GRACE_SECONDS=0 (mainnet-forced) that
        rejected every in-flight share on the old tip. Classifying against the
        published tip keeps shares valid until the coordinator has prepared,
        validated, and published the flip, and it is the same tip source the
        stale-grace window and evicted-job classification are anchored to.

        During a detected-but-unpublished replacement, the published tip stays
        authoritative beyond the ordinary freshness age so a large healthy
        build cannot recreate the reject outage. That extension is bounded by
        PRISM_TEMPLATE_REFRESH_FAILURE_EXIT_SECONDS and is anchored to the
        first unpublished divergence; failed refreshes therefore still fall
        back to the live RPC instead of accepting frozen work indefinitely.
        """
        runtime = self._runtime
        with runtime.lock:
            published_tip = runtime._submit_stale_check_tip_locked(time.monotonic())
        if published_tip is not None:
            return published_tip
        return str(runtime.rpc.call("getbestblockhash"))

    def _published_tip_authoritative_locked(self, now: float) -> bool:
        """True while the published tip still owns share classification.

        Either the ordinary freshness window (reconfirmed within
        PRISM_SUBMIT_TIP_MAX_AGE_SECONDS) or the bounded detected-but-
        unpublished replacement lease is open. Job issuance uses the same
        predicate so work handed to miners is never classified against a
        different tip than the one it was issued for.
        """
        runtime = self._runtime
        max_age = float(
            getattr(
                runtime,
                "submit_tip_max_age_seconds",
                DEFAULT_PRISM_SUBMIT_TIP_MAX_AGE_SECONDS,
            )
        )
        if max_age <= 0:
            return False
        observed = getattr(self, "current_tip_first_seen", None)
        if observed is None:
            return False
        observed_at = getattr(self, "current_tip_observed_monotonic", None)
        if observed_at is not None and now - observed_at <= max_age:
            return True
        latest_detected = getattr(self, "latest_detected_tip", None)
        divergence_started = getattr(
            self,
            "tip_refresh_divergence_started_monotonic",
            None,
        )
        divergence_budget = float(
            getattr(
                runtime,
                "template_refresh_failure_exit_seconds",
                DEFAULT_PRISM_TEMPLATE_MAX_AGE_SECONDS,
            )
        )
        return bool(
            latest_detected is not None
            and latest_detected[0] != observed[0]
            and divergence_started is not None
            and divergence_budget > 0
            and now - divergence_started <= divergence_budget
        )

    def refresh_jobs_after_pending_accepted_block(
        self,
        client: ClientState,
        *,
        heartbeat_name: str = "qbit_blockpoll",
    ) -> int:
        runtime = self._runtime
        with runtime.lock:
            block = client.post_accept_refresh_block
            client.post_accept_refresh_block = None
        if block is None:
            return 0
        block_height, block_hash = block
        return runtime.refresh_jobs_after_accepted_block(
            block_height=block_height,
            block_hash=block_hash,
            heartbeat_name=heartbeat_name,
        )

    def refresh_jobs_after_accepted_block(
        self, *, block_height: int, block_hash: str, heartbeat_name: str = "qbit_blockpoll"
    ) -> int:
        runtime = self._runtime
        try:
            runtime._record_heartbeat(heartbeat_name)
            observation_sequence = runtime._reserve_tip_observation_sequence()
            observed_tip = str(runtime.rpc.call("getbestblockhash"))
            if not runtime.observe_tip_for_refresh(
                observed_tip,
                observation_sequence=observation_sequence,
            ):
                raise TemplateRefreshSuperseded(
                    "post-accept tip observation was superseded"
                )
        except TemplateRefreshSuperseded:
            # A newer observation won before this notification could publish
            # its tip. The shared driver is woken below; this is coordination
            # churn, not a post-accept refresh failure.
            return 0
        except Exception:
            with runtime.lock:
                self.post_accept_refresh_failure_count += 1
            print(
                "prism coordinator: post-accept clean job refresh failed after direct PRISM block "
                f"height={block_height} hash={block_hash}",
                flush=True,
            )
            traceback.print_exc()
            return 0
        finally:
            # Accepted-block payout/template changes can require a same-tip
            # rebuild. Wake the driver even when the immediate best-tip RPC or
            # observation fails; the owner will refetch a coherent snapshot.
            runtime._schedule_tip_refresh_retry()
        print(
            "prism coordinator: scheduled single-flight job refresh after "
            f"direct PRISM block height={block_height} hash={block_hash} "
            f"observed_tip={observed_tip}",
            flush=True,
        )
        return 0

    @staticmethod
    def _tip_refresh_hashrate_proxy(client: ClientState) -> Decimal:
        proxy = (
            getattr(client, "vardiff_difficulty_estimate", None)
            or getattr(client, "share_difficulty", Decimal(0))
        )
        try:
            resolved = Decimal(proxy)
        except (InvalidOperation, TypeError, ValueError):
            return Decimal(0)
        return resolved if resolved.is_finite() and resolved > 0 else Decimal(0)

    def _payout_epoch_tip_hash_locked(self) -> str | None:
        """Label a payout-minted epoch with the current refresh target tip.

        A payout publication advances the payout generation of the target
        the fleet is already converging on; it must not move or invent the
        tip. Candidate source hashes are unusable here: an accepted-block
        preview stores the accepted block's own hash, which is not a
        template parent and can trail an already-observed newer tip, so
        labeling the epoch with it strands token currency, identity
        publication, and the fixpoint until a later tip observation
        remints.
        """
        current_epoch_tip = getattr(self, "_tip_refresh_epoch_tip_hash", None)
        if current_epoch_tip is not None:
            return str(current_epoch_tip)
        latest_detected = getattr(self, "latest_detected_tip", None)
        if latest_detected is not None:
            return str(latest_detected[0])
        published_tip = getattr(self, "current_tip_first_seen", None)
        if published_tip is not None:
            return str(published_tip[0])
        return None

    def _mint_tip_refresh_epoch_locked(
        self,
        *,
        tip_hash: str,
        payout_state_generation: int,
        started_monotonic: float,
    ) -> int:
        """Mint the next target epoch while the coordinator lock is held."""
        runtime = self._runtime
        if not getattr(runtime, "tip_refresh_epoch_fanout", False):
            return 0
        sequence = int(self._tip_refresh_epoch_sequence) + 1
        self._tip_refresh_epoch_sequence = sequence
        self._tip_refresh_epoch_tip_hash = tip_hash
        self._tip_refresh_epoch_payout_generation = int(payout_state_generation)
        client_weights = {
            int(client.connection_id): runtime._tip_refresh_hashrate_proxy(client)
            for client in runtime.clients
            if runtime.client_can_receive_jobs(client)
        }
        total_weight = sum(
            (
                weight
                for weight in client_weights.values()
                if weight > 0
            ),
            Decimal(0),
        )
        self._tip_refresh_epoch_coverage = _TipRefreshEpochCoverage(
            sequence=sequence,
            tip_hash=tip_hash,
            payout_state_generation=int(payout_state_generation),
            started_monotonic=float(started_monotonic),
            client_weights=client_weights,
            total_weight=total_weight,
        )
        if total_weight <= 0:
            tracker = self._tip_refresh_epoch_coverage
            with self._tip_refresh_metrics_lock:
                for target, _ratio in PRISM_TIP_REFRESH_COVERAGE_TARGETS:
                    tracker.recorded_targets.add(target)
                    histogram = self.tip_refresh_coverage_histograms[target]
                    histogram["count"] = int(histogram["count"]) + 1
                    buckets = histogram["buckets"]
                    assert isinstance(buckets, dict)
                    for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS:
                        buckets[bucket] = int(buckets.get(bucket, 0)) + 1
        return sequence

    def _tip_refresh_epoch_for_bundle_locked(
        self,
        cached: CachedJobBundle,
    ) -> int:
        runtime = self._runtime
        if not getattr(runtime, "tip_refresh_epoch_fanout", False):
            return 0
        identity = getattr(
            self,
            "_published_tip_refresh_epoch_identity",
            None,
        )
        if identity is None:
            return 0
        (
            sequence,
            tip_hash,
            payout_generation,
            template_fingerprint,
            template_generation,
        ) = identity
        parent_hash = str(cached.template.get("previousblockhash", ""))
        if (
            parent_hash != tip_hash
            or int(cached.payout_state_generation)
            != payout_generation
            or cached.template_fingerprint != template_fingerprint
            or int(cached.template_generation)
            != template_generation
        ):
            return 0
        return sequence

    def _publish_tip_refresh_epoch_identity_locked(
        self,
        snapshot: QbitTipTemplateSnapshot,
    ) -> None:
        runtime = self._runtime
        if not getattr(runtime, "tip_refresh_epoch_fanout", False):
            return
        sequence = int(getattr(self, "_tip_refresh_epoch_sequence", 0))
        tip_hash = getattr(self, "_tip_refresh_epoch_tip_hash", None)
        payout_generation = int(
            getattr(self, "_tip_refresh_epoch_payout_generation", 0)
        )
        if (
            sequence <= 0
            or tip_hash != snapshot.bestblockhash
            or payout_generation
            != int(getattr(runtime, "_payout_state_generation", 0))
        ):
            return
        self._published_tip_refresh_epoch_identity = (
            sequence,
            tip_hash,
            payout_generation,
            snapshot.template_fingerprint,
            snapshot.template_generation,
        )

    def _admit_client_tip_refresh_epoch_locked(
        self,
        client: ClientState,
        epoch_sequence: int,
    ) -> bool:
        """Fence per-client registration so epochs cannot regress on the wire."""
        runtime = self._runtime
        if not getattr(runtime, "tip_refresh_epoch_fanout", False):
            return True
        if runtime._client_tip_refresh_epoch_blocked_locked(client, epoch_sequence):
            return False
        admitted_epoch = int(
            getattr(client, "_tip_refresh_admitted_epoch_sequence", 0)
        )
        client._tip_refresh_admitted_epoch_sequence = max(
            int(epoch_sequence),
            admitted_epoch,
        )
        return True

    @staticmethod
    def _client_tip_refresh_epoch_blocked_locked(
        client: ClientState,
        epoch_sequence: int,
    ) -> bool:
        active_epoch = int(
            getattr(
                getattr(client, "active_job", None),
                "tip_refresh_epoch_sequence",
                0,
            )
        )
        admitted_epoch = int(
            getattr(client, "_tip_refresh_admitted_epoch_sequence", 0)
        )
        return int(epoch_sequence) < max(active_epoch, admitted_epoch)

    def _tip_refresh_epoch_coverage_reached_locked(
        self,
        client: ClientState,
        context: PrismJobContext,
        delivered_monotonic: float,
    ) -> list[tuple[str, float]]:
        """Advance the active cohort while the coordinator lock is held."""
        runtime = self._runtime
        if not getattr(runtime, "tip_refresh_epoch_fanout", False):
            return []
        reached: list[tuple[str, float]] = []
        tracker = self._tip_refresh_epoch_coverage
        connection_id = int(getattr(client, "connection_id", 0))
        if (
            tracker is None
            or tracker.sequence != int(self._tip_refresh_epoch_sequence)
            or tracker.total_weight <= 0
            or connection_id in tracker.delivered_clients
        ):
            return reached
        weight = tracker.client_weights.get(connection_id)
        if weight is None:
            return reached
        if (
            int(getattr(context, "connection_id", 0)) != connection_id
            or int(getattr(context, "tip_refresh_epoch_sequence", 0))
            != tracker.sequence
            or str(context.template.get("previousblockhash", ""))
            != tracker.tip_hash
            or int(getattr(context, "payout_state_generation", 0))
            != tracker.payout_state_generation
        ):
            return reached
        tracker.delivered_clients.add(connection_id)
        if weight <= 0:
            return reached
        tracker.delivered_weight += weight
        coverage = tracker.delivered_weight / tracker.total_weight
        elapsed = max(
            0.0,
            float(delivered_monotonic) - tracker.started_monotonic,
        )
        for target, ratio in PRISM_TIP_REFRESH_COVERAGE_TARGETS:
            if target not in tracker.recorded_targets and coverage >= ratio:
                tracker.recorded_targets.add(target)
                reached.append((target, elapsed))
        return reached

    def _record_tip_refresh_epoch_coverage(
        self,
        reached: list[tuple[str, float]],
    ) -> None:
        """Observe fixed cohort thresholds without per-connection series."""
        if not reached:
            return
        with self._tip_refresh_metrics_lock:
            for target, elapsed in reached:
                histogram = self.tip_refresh_coverage_histograms[target]
                histogram["count"] = int(histogram["count"]) + 1
                histogram["sum"] = float(histogram["sum"]) + elapsed
                buckets = histogram["buckets"]
                assert isinstance(buckets, dict)
                for bucket in PRISM_TIP_REFRESH_SECONDS_BUCKETS:
                    if elapsed <= bucket:
                        buckets[bucket] = int(buckets.get(bucket, 0)) + 1

    def _tip_refresh_epoch_fixpoint_reached(self) -> bool:
        """Return whether every eligible connection has newest delivered work."""
        runtime = self._runtime
        if not getattr(runtime, "tip_refresh_epoch_fanout", False):
            return True
        with runtime.lock:
            sequence = int(getattr(self, "_tip_refresh_epoch_sequence", 0))
            tip_hash = getattr(self, "_tip_refresh_epoch_tip_hash", None)
            payout_generation = int(
                getattr(self, "_tip_refresh_epoch_payout_generation", 0)
            )
            snapshot = getattr(self, "tip_template_snapshot", None)
            ready_mode = bool(getattr(runtime, "_pool_ready_latched", False))
            for client in runtime.clients:
                if not runtime.client_can_receive_jobs(client):
                    continue
                context = getattr(client, "_progress_delivered_context", None)
                if (
                    context is None
                    or int(getattr(context, "connection_id", 0))
                    != int(client.connection_id)
                ):
                    return False
                context_epoch = int(
                    getattr(context, "tip_refresh_epoch_sequence", 0)
                )
                if context_epoch > sequence:
                    continue
                if context_epoch < sequence:
                    return False
                if str(context.template.get("previousblockhash", "")) != tip_hash:
                    return False
                if (
                    int(getattr(context, "payout_state_generation", 0))
                    != payout_generation
                ):
                    return False
                if ready_mode and bool(getattr(context, "collection_only", False)):
                    return False
                if (
                    snapshot is not None
                    and snapshot.bestblockhash == tip_hash
                    and (
                        getattr(context, "template_fingerprint", None)
                        != snapshot.template_fingerprint
                    )
                ):
                    return False
            # Every eligible connection holds newest delivered work, so any
            # refresh retry armed before this verification is satisfied.
            # Consume it inside the same lock hold that proved convergence:
            # signals armed afterwards stay armed, and blockpoll no longer
            # re-runs a full wave against work a recovered supersession
            # already finished.
            self._tip_refresh_retry_consumed = self._tip_refresh_retry_counter
            self._tip_refresh_retry.clear()
        return True

    def _clear_tip_refresh_failure_holdoff(self) -> None:
        runtime = self._runtime
        runtime._ensure_tip_refresh_state()
        with runtime.lock:
            self._tip_refresh_failure_holdoff_until = None
            self._tip_refresh_failure_tip = None

    def _reuse_current_tip_template_snapshot(
        self,
        observed_best_tip: str,
    ) -> QbitTipTemplateSnapshot | None:
        """Rebuild a snapshot from cached artifacts while their tip holds.

        Honors the PRISM_TEMPLATE_CACHE_SECONDS window that per-client job
        builds already use: within it, a poll pass whose observed best tip
        still equals the cached template's parent issues no getblocktemplate.
        Rapid failed-refresh re-attempts then cost one cheap best-hash probe
        each, while a changed tip or an expired window falls through to the
        full fetch, so same-tip template rotation still lands on the normal
        cadence.
        """
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        ttl = float(
            getattr(runtime, "template_cache_seconds", DEFAULT_PRISM_BLOCKPOLL_SECONDS)
        )
        if ttl <= 0:
            return None
        with runtime._job_cache_lock:
            cached = runtime._template_artifacts
        if cached is None:
            return None
        if time.monotonic() - cached.fetched_monotonic > ttl:
            return None
        if str(observed_best_tip) != cached.previousblockhash:
            return None
        runtime._record_job_cache_event("template", hit=True)
        return QbitTipTemplateSnapshot(
            bestblockhash=cached.previousblockhash,
            previousblockhash=cached.previousblockhash,
            template_fingerprint=cached.fingerprint,
            template_generation=cached.generation,
            template_artifacts=cached,
        )


__all__ = [
    "DEFAULT_PRISM_PAYOUT_RECONCILE_SUPERSESSION_RETRIES",
    "FanoutCancellation",
    "PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS",
    "PRISM_TIP_REFRESH_BUILD_PHASES",
    "PRISM_TIP_REFRESH_CANCELLATION_STAGES",
    "PRISM_TIP_REFRESH_COVERAGE_TARGETS",
    "PRISM_TIP_REFRESH_FAILURE_HOLDOFF_JITTER_FRACTION",
    "PRISM_TIP_REFRESH_REENTRY_BACKOFF_SECONDS",
    "PRISM_TIP_REFRESH_RESULTS",
    "PRISM_TIP_REFRESH_SECONDS_BUCKETS",
    "PRISM_TIP_REFRESH_WAVE_OUTCOMES",
    "PRISM_TIP_REFRESH_WAVE_PASS_BUDGET",
    "RefreshResult",
    "RetainedCollectionRefresh",
    "TipRefreshRuntime",
    "TipRefreshService",
    "TipRefreshValidationToken",
]
