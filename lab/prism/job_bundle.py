#!/usr/bin/env python3
"""Immutable PRISM job bundles, their cache, and the bounded build scheduler.

The service owns the shared job-bundle cache, the bounded latest-wins build
scheduler (active/retiring/pending flights, priority and routine admission,
promise cleanup, orphan sweep), the per-generation issued-at anchors, the
share-window serialization cache slot, and the template-artifact repository.

It never imports ``prism_coordinator``.  Every cross-domain fact -- payout
state, tip-refresh authority, ledger snapshots, refresh metrics, live
configuration attributes -- is reached through the :class:`JobBundleRuntime`
typed port, resolved at call time so the historical coordinator monkeypatch
seams (including the instance-level facade patches used by the current test
suite) keep intercepting exactly as before the extraction.
"""

from __future__ import annotations

from collections import OrderedDict
from concurrent.futures import (
    FIRST_COMPLETED,
    Future,
    InvalidStateError,
    ThreadPoolExecutor,
    wait,
)
from dataclasses import dataclass, field, replace as dataclass_replace
from decimal import Context, getcontext, localcontext
import hashlib
import json
import subprocess
import threading
import time
import weakref
from typing import Any, Callable, Protocol

from lab.prism import direct_stratum
from lab.prism.bundle_compiler import _ShareWindowSerialization
from lab.prism.coordinator_config import (
    DEFAULT_PRISM_JOB_BUILD_CANCEL_GRACE_SECONDS,
    DEFAULT_PRISM_JOB_BUILD_EXECUTOR_WORKERS,
    DEFAULT_PRISM_JOB_BUILD_TIMEOUT_SECONDS,
    DEFAULT_PRISM_JOB_BUNDLE_CACHE_SECONDS,
    DEFAULT_PRISM_ROUTINE_ADMISSION_DEADLINE_SECONDS,
)
from lab.prism.template_artifacts import (
    CachedTemplateArtifacts,
    PayoutStatePublicationBlocked,
    QbitTipTemplateSnapshot,
    TemplateArtifactRepository,
    TemplateRefreshBlocked,
    TemplateRefreshSuperseded,
)


# Owner-local copies of the still-coordinator-visible constants; these are
# compatibility duplicates by design, not missed removals.
MAX_PRISM_JOB_BUNDLE_CACHE_ENTRIES = 128
PRISM_JOB_EXTRANONCE1_PLACEHOLDER_HEX = "00000000"
PRISM_REWARD_WINDOW_MULTIPLIER = 8
PRISM_SNAPSHOT_WINDOW_MARGIN = 2
PRISM_JOB_BUILD_SECONDS_BUCKETS = (
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
PRISM_JOB_BUILD_PHASES = (
    "reorg",
    "template",
    "merkle",
    "ledger",
    "payout_artifact",
    "payout",
    "ctv",
    "input_serialization",
    "worker",
    "output_serialization",
    "assembly",
    "bundle",
    "preparation_wait",
    "executor_queue",
    "client_lock",
    "payout_gate",
    "stamp",
    "socket_send",
    "send",
)
PRISM_JOB_CACHE_KINDS = ("template", "bundle")
# A flight whose executor future is finished normally leaves its slot inside
# the future's done callback. The admission sweep only treats such a flight
# as orphaned after this grace, so a callback that is merely between future
# completion and slot cleanup is never mistaken for a dead one.
PRISM_JOB_BUILD_ORPHAN_SWEEP_GRACE_SECONDS = 1.0
# Cancellation-check slice while an initial request rides a subscribed
# publication-priority build promise. Promise completion wakes the waiter
# immediately; this only bounds how stale a cancellation can go unnoticed.
PRISM_INITIAL_JOB_SUBSCRIBE_POLL_SECONDS = 0.25


def now_ms() -> int:
    return int(time.time() * 1000)


def canonical_json_text(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def canonical_json_sha256(value: object) -> str:
    incremental_digest = getattr(value, "canonical_json_sha256", None)
    if callable(incremental_digest):
        return str(incremental_digest())
    return hashlib.sha256(canonical_json_text(value).encode()).hexdigest()


class WorkerIdentityPort(Protocol):
    """The exact worker facts a collection-mode build binds into its work."""

    payout_address: str
    p2mr_program_hex: str


class JobBuildCancelled(TemplateRefreshBlocked):
    """An immutable build was cancelled or timed out."""


class JobBuildSuperseded(JobBuildCancelled, TemplateRefreshSuperseded):
    """A coordination race cooperatively cancelled immutable construction."""


class JobBundleBuildSuperseded(JobBuildSuperseded):
    """A newer tip or payout generation canceled this deterministic build."""


class CollectionIdentityUnavailable(TemplateRefreshBlocked):
    """Current collection work is waiting for an authorized worker identity."""


class JobBuildAdmissionDeadlineExceeded(TemplateRefreshBlocked):
    """A routine request parked behind publication priority past its bound.

    Subclasses TemplateRefreshBlocked so callers retry with their normal
    pacing; the raise site logs and counts the expiry so parked admission is
    visible instead of silently riding the client's own longer timeout.
    """


class JobBuildWaiterCancelled(RuntimeError):
    """A bundle waiter became obsolete before it acquired preparation."""


class JobBuildCancellation:
    def __init__(self, *, timeout_seconds: float) -> None:
        self._event = threading.Event()
        self._lock = threading.Lock()
        self.started_monotonic = time.monotonic()
        self.deadline_monotonic = self.started_monotonic + timeout_seconds
        self.last_checkpoint_monotonic = self.started_monotonic
        self.cancelled_monotonic: float | None = None
        self.reason: str | None = None

    def cancel(self, reason: str) -> bool:
        with self._lock:
            if self._event.is_set():
                return False
            self.reason = reason
            self.cancelled_monotonic = time.monotonic()
            self._event.set()
            return True

    def is_set(self) -> bool:
        if self._event.is_set():
            return True
        if time.monotonic() >= self.deadline_monotonic:
            self.cancel("timeout")
            return True
        return False

    def raise_if_cancelled(self, phase: str) -> None:
        if self.is_set():
            reason = self.reason or "cancelled"
            if reason == "timeout":
                raise JobBuildCancelled(
                    f"job build timeout at {phase}; immediate retry scheduled"
                )
            raise JobBuildSuperseded(
                f"job build {reason} at {phase}; immediate retry scheduled"
            )
        with self._lock:
            self.last_checkpoint_monotonic = time.monotonic()


@dataclass(frozen=True)
class JobBuildKey:
    """Every immutable input that can affect one constructed mining job."""

    best_tip_hash: str
    previous_block_hash: str
    template_fingerprint: str
    template_generation: int
    payout_state_generation: int
    payout_artifact_sha256: str
    mode: str
    collection_identity: tuple[str, str] | None
    block_height: int
    coinbase_value_sats: int
    network_difficulty: int
    issued_at_ms: int
    payout_policy_sha256: str
    ctv_settlement_sha256: str | None
    witness_merkle_sha256: str
    transaction_set_sha256: str
    coinbase_suffix_hex: str
    signing_key_sha256: str
    ledger_signing_key_sha256: str
    numeric_context_sha256: str
    share_snapshot_sha256: str = ""
    payout_append_invalidation_epoch: int = 0


@dataclass(frozen=True)
class CachedJobBundle:
    """One heavy job build (ledger snapshot + signed manifest + base job)
    shared across every client on the same template.

    The base job is built with the extranonce1 placeholder; per-client jobs
    are stamped from it by swapping job_id, extranonce1, difficulty, and the
    clean_jobs flag. All other fields are byte-identical across clients
    because the stratum coinbase split excludes the extranonce window.
    """

    key: tuple[object, ...]
    template: dict[str, Any]
    template_fingerprint: str
    coinbase_manifest: dict[str, Any]
    shares_json: list[dict[str, object]]
    prior_balances: list[dict[str, object]]
    found_block: dict[str, object]
    collection_only: bool
    issued_at_ms: int
    base_job: direct_stratum.DirectQbitStratumJob
    built_monotonic: float
    template_generation: int = 0
    payout_state_generation: int = 0
    payout_artifact_generation: int = 0
    # Collection coinbases commit a synthetic share to this exact payout
    # identity. Ready bundles have no worker-specific inputs and keep this
    # unset, which makes accidental cross-worker stamping fail closed.
    collection_identity: tuple[str, str] | None = None
    # Compact carry-forward state derived from the immutable job summary. It
    # lets an accepted block publish child payout state without retaining the
    # full audit bundle (and its duplicate shares tree) in every cached job.
    prospective_prior_balances: tuple[tuple[str, str, str, int], ...] | None = None
    build_key: JobBuildKey | None = None
    # A synchronous ready build publishes its fenced ledger window for reuse
    # only once this bundle wins cache publication (or is served as pinned
    # snapshot work). Installing mid-build would re-key concurrent lookups
    # away from the in-flight single flight and fork redundant builds.
    prepared_ledger_artifact: "PayoutLedgerArtifact | None" = field(
        default=None,
        compare=False,
        repr=False,
    )


@dataclass
class JobBuildRequest:
    key: JobBuildKey
    cache_key: tuple[object, ...]
    equivalence_key: tuple[object, ...]
    artifacts: CachedTemplateArtifacts
    template_json: str
    transaction_hexes: tuple[str, ...]
    witness_merkle_leaves_hex: tuple[str, ...]
    worker: "WorkerIdentityPort | None"
    mode: str
    payout_artifact: "PayoutStateArtifact"
    payout_ledger_artifact: "PayoutLedgerArtifact | None"
    payout_policy_json: str
    ctv_settlement_json: str | None
    decimal_context: Context = field(repr=False)
    cancellation: JobBuildCancellation
    idle_retarget: bool = False
    publication_critical: bool = False
    request_source: str = "routine"
    priority_admission_recorded: bool = False
    promise: Future[CachedJobBundle] = field(default_factory=Future)
    requested_monotonic: float = field(default_factory=time.monotonic)
    superseded_monotonic: float | None = None
    # Exposed in-flight scan anchor owned by this build (see
    # _expose_inflight_scan_anchor). Set by the synchronous ledger read so
    # the executor wrapper can retire the exposure when the build dies
    # before handing the token to its seeded artifact.
    inflight_scan_anchor_token: int | None = None


@dataclass(eq=False)
class JobBuildFlight:
    request: JobBuildRequest
    future: Future[CachedJobBundle] | None = None
    # First time the admission sweep saw this flight finished but still
    # occupying its slot; eviction waits out the sweep grace from here.
    orphan_observed_monotonic: float | None = None


@dataclass
class JobBundleBuildControl:
    key: tuple[object, ...]
    previousblockhash: str
    payout_state_generation: int
    payout_artifact_generation: int
    cancel_event: threading.Event = field(default_factory=threading.Event)
    process: subprocess.Popen[str] | None = None


@dataclass
class _SharedBundlePreparationFlight:
    event: threading.Event = field(default_factory=threading.Event)
    result: CachedJobBundle | None = None
    error: BaseException | None = None
    waiters: int = 0


class JobBundleRuntime(Protocol):
    """Typed port over the coordinator, resolved at call time.

    Every member is looked up on the live coordinator object when used, so
    instance monkeypatches (``server.build_audit_bundle = ...`` and friends)
    and coordinator-owned live configuration attributes keep working exactly
    as before the extraction.  Owner facades route back into this service;
    that round trip is deliberate: it is what preserves the current
    white-box patch surface until later stack layers repoint those tests.
    """

    # Cross-domain objects and live configuration attributes.
    lock: Any
    rpc: Any
    ledger: Any
    stop_event: Any
    extranonce2_size: int
    share_difficulty: Any
    min_ready_miners: int
    job_build_timeout_seconds: float
    job_build_cancel_grace_seconds: float

    # First-touch initializers.
    def _ensure_job_cache_state(self) -> None: ...

    def _ensure_tip_refresh_state(self) -> None: ...

    # Payout-state facades (owner extracted by a later chunk).
    def _current_payout_state_artifact(self, cancellation: Any = None) -> Any: ...

    def _usable_payout_ledger_artifact(
        self,
        payout_state_generation: int,
        network_difficulty: int,
    ) -> Any: ...

    def _await_pending_parent_payout_preview(
        self,
        parent_hash: str,
        *,
        parent_height: int,
    ) -> Any: ...

    def _prior_balances_for_job_parent(
        self,
        parent_hash: str,
        *,
        parent_height: int,
        fallback_balances: list[dict[str, object]],
    ) -> list[dict[str, object]]: ...

    def _serialize_prior_balance_preview(self, balances: Any) -> Any: ...

    def _accepted_block_payout_preview_from_bundle(
        self,
        bundle: dict[str, Any],
        *,
        prior_balances: list[dict[str, object]],
    ) -> Any: ...

    def _install_payout_ledger_artifact(self, artifact: Any) -> Any: ...

    def _payout_artifact_reuse_active(self) -> bool: ...

    def _payout_artifact_max_anchor_age_ms(self) -> float: ...

    def _payout_artifact_reanchor_seconds(self) -> float: ...

    def _expose_inflight_scan_anchor(self, anchor_ms: int) -> int: ...

    def _retire_inflight_scan_anchor(self, token: int | None) -> None: ...

    def _publish_seedless_job_window_anchor_locked(self, anchor_ms: int) -> None: ...

    # Tip-refresh facades (owner extracted by a later chunk).
    def _newest_observed_tip_locked(self) -> str | None: ...

    def _artifacts_buildable_locked(self, artifacts: CachedTemplateArtifacts) -> bool: ...

    def _published_snapshot_artifacts_locked(
        self,
        artifacts: CachedTemplateArtifacts,
    ) -> bool: ...

    def _published_tip_authoritative_locked(self, now: float) -> bool: ...

    def _schedule_tip_refresh_retry(self) -> None: ...

    def _observe_tip_refresh_build_phase(self, name: str, elapsed: float) -> None: ...

    def _vardiff_idle_tip_divergence_locked(self) -> bool: ...

    # Shared readiness/stats and builder facades.
    def pool_readiness_latched(self) -> bool: ...

    def accepted_share_stats(self) -> tuple[int, int]: ...

    def prism_payout_policy(self) -> dict[str, object]: ...

    def prism_ctv_settlement_config(
        self,
        *,
        block_height: int,
        parent_hash: str | None,
    ) -> dict[str, object] | None: ...

    def coinbase_script_sig_suffix_hex(
        self,
        extranonce1_hex: str,
        extranonce2_hex: str,
    ) -> str: ...

    def _job_snapshot_anchor_ms(self, issued_at_ms: int) -> int: ...

    def _job_build_checkpoint(self, phase: str, cancellation: Any) -> None: ...

    def build_audit_bundle(self, **kwargs: Any) -> dict[str, Any]: ...


class JobBundleService:
    """Sole owner of the job-bundle cache, scheduler, and readiness seams."""

    def __init__(
        self,
        runtime: JobBundleRuntime,
        *,
        spool_factory: Callable[[], Any] | None = None,
        payout_ledger_artifact_type: type | None = None,
        now_ms: Callable[[], int] | None = None,
        canonical_json_sha256_override: Callable[[object], str] | None = None,
    ) -> None:
        self._runtime = runtime
        # Call-time-resolved wall clock: the coordinator wires a lambda over
        # its own module global so the historical ``prism_coordinator.now_ms``
        # patch seam keeps steering anchor selection.
        self._now_ms = now_ms if now_ms is not None else globals()["now_ms"]
        # Same seam for the canonical digest: white-box tests intercept it
        # through the coordinator module global.
        self._canonical_json_sha256 = (
            canonical_json_sha256
            if canonical_json_sha256_override is None
            else canonical_json_sha256_override
        )
        # Call-time-resolved spool-file factory (see bundle_compiler); the
        # coordinator wires its own module global through here so the
        # historical patch seam keeps steering spool creation.
        self._spool_factory = spool_factory
        # The payout-ledger artifact remains coordinator-owned until the
        # payout-state owner is extracted; the class is injected so seeded
        # artifacts stay type-identical with coordinator-built ones.
        self._payout_ledger_artifact_type = payout_ledger_artifact_type
        self._job_cache_lock = threading.Lock()
        self._active_job_bundle_builds: dict[
            tuple[object, ...], JobBundleBuildControl
        ] = {}
        # Compatibility seam for embedders/tests. Expensive construction is
        # coordinated by the bounded latest-wins scheduler below, not held
        # under this lock.
        self._job_build_lock = threading.Lock()
        self._job_build_scheduler_lock = threading.RLock()
        self._job_build_priority_preparations: dict[int, float] = {}
        self._job_build_priority_preparation_sequence = 0
        self._job_build_routine_preparations: dict[
            int,
            weakref.ReferenceType[JobBuildCancellation],
        ] = {}
        self._job_build_routine_preparation_sequence = 0
        self._job_build_priority_changed = threading.Event()
        self._job_build_executor: ThreadPoolExecutor | None = None
        self._job_build_executor_shutdown = False
        self._job_build_active: JobBuildFlight | None = None
        self._job_build_retiring: JobBuildFlight | None = None
        self._job_build_pending: JobBuildRequest | None = None
        self._job_build_issued_at_ms: OrderedDict[int, int] = OrderedDict()
        self._job_bundle_cache: OrderedDict[
            tuple[object, ...], CachedJobBundle
        ] = OrderedDict()
        self._job_build_phase_local = threading.local()
        self.job_cache_hit_counts = {kind: 0 for kind in PRISM_JOB_CACHE_KINDS}
        self.job_cache_miss_counts = {kind: 0 for kind in PRISM_JOB_CACHE_KINDS}
        self.job_build_seconds_bucket_counts = {
            bucket: 0 for bucket in PRISM_JOB_BUILD_SECONDS_BUCKETS
        }
        self.job_build_seconds_sum = 0.0
        self.job_build_count = 0
        self.job_build_phase_seconds = {phase: 0.0 for phase in PRISM_JOB_BUILD_PHASES}
        self.job_build_scheduler_counts = {
            "requests": 0,
            "starts": 0,
            "completions": 0,
            "supersessions": 0,
            "obsolete_results": 0,
            "orphan_evicted": 0,
        }
        self.job_build_priority_counts = {
            result: 0
            for result in (
                "started",
                "coalesced",
                "queued",
                "routine_deferred",
                "routine_preempted",
            )
        }
        self.job_build_priority_admission_seconds = {
            "sum": 0.0,
            "count": 0,
        }
        self.initial_job_prepared_work_counts = {
            result: 0
            for result in (
                "cache_hit",
                "singleflight",
                "deferred",
                "subscribed",
                "admission_deadline",
            )
        }
        self._admission_deadline_last_log_monotonic: float | None = None
        self.job_build_cancellation_seconds = {
            "sum": 0.0,
            "count": 0,
        }
        self.job_build_replacement_start_seconds = {
            "sum": 0.0,
            "count": 0,
        }
        self.job_build_worker_counts = {
            "starts": 0,
            "terminations": 0,
            "crashes": 0,
            "restarts": 0,
        }
        self._job_build_worker_restart_pending = False
        self._share_window_serialization_lock = threading.Lock()
        self._share_window_serialization: _ShareWindowSerialization | None = None
        # Keyed shared-preparation flights absorbed from the historical
        # coordinator field pair; retained for the still-coordinator-side
        # metrics reader until later chunks retire them.
        self._bundle_preparation_lock = threading.Lock()
        self._bundle_preparation_flights: dict[
            tuple[object, ...], _SharedBundlePreparationFlight
        ] = {}
        self.shared_bundle_build_counts = {
            outcome: 0
            for outcome in ("started", "completed", "superseded", "failed")
        }
        self.shared_bundle_preparation_seconds_sum = 0.0
        self.shared_bundle_preparation_count = 0
        self.shared_bundle_preparation_waiters = 0
        self._prepared_ready_bundle: CachedJobBundle | None = None
        self._prepared_ready_snapshot: QbitTipTemplateSnapshot | None = None
        self.job_preparation_pending = False
        # The template-artifact repository shares the job-cache lock: the
        # historical layout guarded template currency and bundle admission
        # with one lock and remaining coordinator domains still rely on it.
        self.template_repository = TemplateArtifactRepository(
            runtime,
            lock=self._job_cache_lock,
        )

    # -- phase bookkeeping -------------------------------------------------

    def _job_build_phases(self) -> dict[str, float]:
        """Per-thread scratch dict of phase timings for the current build."""
        self._runtime._ensure_job_cache_state()
        phases = getattr(self._job_build_phase_local, "phases", None)
        if phases is None:
            phases = {}
            self._job_build_phase_local.phases = phases
        return phases

    def observe_job_build_elapsed(
        self,
        elapsed_seconds: float,
        phases: dict[str, float],
    ) -> None:
        self._runtime._ensure_job_cache_state()
        with self._job_cache_lock:
            self.job_build_count += 1
            self.job_build_seconds_sum += elapsed_seconds
            for bucket in PRISM_JOB_BUILD_SECONDS_BUCKETS:
                if elapsed_seconds <= bucket:
                    self.job_build_seconds_bucket_counts[bucket] += 1
            for phase, duration in phases.items():
                if phase in self.job_build_phase_seconds:
                    self.job_build_phase_seconds[phase] += duration

    def _flush_job_build_phases(self, phases: dict[str, float]) -> None:
        """Fold a worker thread's phase accruals into the exported counters.

        Only the per-client delivery entry points clear and flush the
        thread-local phase dict; the job-build executor and payout-artifact
        preparation workers never passed through them, so the template/
        ledger/assembly/bundle seconds they accrued were exported as
        exactly 0.0 while production paid multi-second walks -- the blind
        spot that misdirected the 2026-07-31 incident review. Build counts
        and the duration histogram stay per-delivery; this folds phase
        seconds only, and clears the dict so a long-lived worker thread
        cannot double-report an earlier request's accruals.
        """
        if not phases:
            return
        self._runtime._ensure_job_cache_state()
        with self._job_cache_lock:
            for phase, duration in phases.items():
                if phase in self.job_build_phase_seconds:
                    self.job_build_phase_seconds[phase] += duration
        phases.clear()

    def _record_job_cache_event(self, kind: str, *, hit: bool) -> None:
        self._runtime._ensure_job_cache_state()
        with self._job_cache_lock:
            counts = self.job_cache_hit_counts if hit else self.job_cache_miss_counts
            counts[kind] = int(counts.get(kind, 0)) + 1

    # -- build-control registry --------------------------------------------

    def _cancel_obsolete_job_bundle_builds(
        self,
        *,
        current_tip: str | None = None,
        payout_state_generation: int | None = None,
    ) -> None:
        """Cancel only builds proven obsolete by a newer exact generation."""
        self._runtime._ensure_job_cache_state()
        processes: list[subprocess.Popen[str]] = []
        with self._job_cache_lock:
            for control in self._active_job_bundle_builds.values():
                obsolete = (
                    current_tip is not None
                    and control.previousblockhash != current_tip
                ) or (
                    payout_state_generation is not None
                    and control.payout_state_generation
                    != int(payout_state_generation)
                )
                if not obsolete or control.cancel_event.is_set():
                    continue
                control.cancel_event.set()
                if control.process is not None:
                    processes.append(control.process)
        for process in processes:
            if process.poll() is not None:
                continue
            try:
                process.terminate()
            except ProcessLookupError:
                pass

    def _register_job_bundle_process(
        self,
        control: JobBundleBuildControl,
        process: subprocess.Popen[str],
    ) -> None:
        terminate = False
        with self._job_cache_lock:
            if (
                self._active_job_bundle_builds.get(control.key) is not control
                or control.cancel_event.is_set()
            ):
                terminate = True
            else:
                control.process = process
        if terminate and process.poll() is None:
            try:
                process.terminate()
            except ProcessLookupError:
                pass

    def _unregister_job_bundle_process(
        self,
        control: JobBundleBuildControl,
        process: Any,
    ) -> None:
        """Detach a shared daemon from a build control after its request.

        Registration makes supersession terminate the in-flight process.
        Once this build's daemon request has completed, a later supersession
        of the build must not kill the shared daemon out from under
        whichever request owns it next.
        """
        with self._job_cache_lock:
            if control.process is process:
                control.process = None

    # -- bounded latest-wins scheduler -------------------------------------

    def _job_build_executor_locked(self) -> ThreadPoolExecutor:
        runtime = self._runtime
        if self._job_build_executor_shutdown:
            raise RuntimeError("job build executor is shut down")
        executor = self._job_build_executor
        if executor is None:
            executor = ThreadPoolExecutor(
                max_workers=int(
                    getattr(
                        runtime,
                        "job_build_executor_workers",
                        DEFAULT_PRISM_JOB_BUILD_EXECUTOR_WORKERS,
                    )
                ),
                thread_name_prefix="prism-job-build",
            )
            self._job_build_executor = executor
        return executor

    def _start_job_build_locked(self, request: JobBuildRequest) -> JobBuildFlight:
        runtime = self._runtime
        executor = runtime._job_build_executor_locked()
        flight = JobBuildFlight(request=request)
        self.job_build_scheduler_counts["starts"] += 1
        self.shared_bundle_build_counts["started"] += 1
        if request.superseded_monotonic is not None:
            elapsed = max(0.0, time.monotonic() - request.superseded_monotonic)
            self.job_build_replacement_start_seconds["sum"] += elapsed
            self.job_build_replacement_start_seconds["count"] += 1
        future = executor.submit(runtime._execute_job_build_request, request)
        flight.future = future
        runtime._record_priority_admission_locked(request, "started")
        return flight

    def _arm_job_build_locked(self, flight: JobBuildFlight) -> None:
        runtime = self._runtime
        future = flight.future
        assert future is not None
        future.add_done_callback(
            lambda completed, build_flight=flight: runtime._job_build_done(
                build_flight,
                completed,
            )
        )

    def _execute_job_build_request(
        self,
        request: JobBuildRequest,
    ) -> CachedJobBundle:
        runtime = self._runtime
        request.cancellation.raise_if_cancelled("start")
        control = JobBundleBuildControl(
            key=request.equivalence_key,
            previousblockhash=request.key.previous_block_hash,
            payout_state_generation=request.key.payout_state_generation,
            payout_artifact_generation=(
                request.payout_ledger_artifact.generation
                if request.payout_ledger_artifact is not None
                else 0
            ),
        )
        with self._job_cache_lock:
            self._active_job_bundle_builds[control.key] = control
        previous_control = getattr(
            self._job_build_phase_local,
            "bundle_build_control",
            None,
        )
        self._job_build_phase_local.bundle_build_control = control
        phases = runtime._job_build_phases()
        phases.clear()
        try:
            with localcontext(request.decimal_context):
                return runtime.build_shared_job_bundle(
                    request.artifacts,
                    request.worker,
                    mode=request.mode,
                    payout_state_generation=request.key.payout_state_generation,
                    payout_artifact=request.payout_ledger_artifact,
                    key=request.cache_key,
                    build_request=request,
                )
        except BaseException:
            # A build that died past its synchronous ledger read can no
            # longer hand its exposed scan anchor to a seeded artifact;
            # retire it here so a dead walk does not keep advancing the
            # append-invalidation epoch. Successful builds either retired
            # the token at the publication fence or handed it to the seed,
            # whose install fence retires it.
            runtime._retire_inflight_scan_anchor(
                request.inflight_scan_anchor_token
            )
            raise
        finally:
            runtime._flush_job_build_phases(phases)
            self._job_build_phase_local.bundle_build_control = previous_control
            with self._job_cache_lock:
                if self._active_job_bundle_builds.get(control.key) is control:
                    self._active_job_bundle_builds.pop(control.key, None)
                control.process = None

    @staticmethod
    def _collection_job_builds_are_independent(
        first: JobBuildRequest,
        second: JobBuildRequest,
    ) -> bool:
        """Distinct workers in one immutable collection cohort are peers."""

        return (
            first.mode == "collection"
            and second.mode == "collection"
            and first.key.collection_identity != second.key.collection_identity
            and dataclass_replace(first.key, collection_identity=None)
            == dataclass_replace(second.key, collection_identity=None)
        )

    @staticmethod
    def _job_build_requests_can_share(
        first: JobBuildRequest,
        second: JobBuildRequest,
    ) -> bool:
        """Share exact builds plus ready work stable across clock-only refreshes."""

        return first.equivalence_key == second.equivalence_key or (
            first.mode == "ready"
            and second.mode == "ready"
            and first.cache_key == second.cache_key
        )

    @staticmethod
    def _ready_job_build_precedes_collection(
        first: JobBuildRequest,
        second: JobBuildRequest,
    ) -> bool:
        """Live ready work cannot be displaced by a collection-mode retry."""

        return (
            first.mode == "ready"
            and second.mode == "collection"
            and not first.cancellation.is_set()
        )

    @staticmethod
    def _defer_job_build_locked(
        *blockers: Future[CachedJobBundle],
    ) -> Future[CachedJobBundle]:
        """Wake a bounded lower-priority waiter when occupied capacity exits."""

        deferred: Future[CachedJobBundle] = Future()
        wake_lock = threading.Lock()

        def wake_for_retry(_completed: Future[CachedJobBundle]) -> None:
            with wake_lock:
                if not deferred.done():
                    deferred.set_exception(
                        JobBuildSuperseded(
                            "job build capacity became available; retrying"
                        )
                    )

        for blocker in blockers:
            blocker.add_done_callback(wake_for_retry)
        return deferred

    @staticmethod
    def _job_build_is_publication_critical(request: object) -> bool:
        return bool(getattr(request, "publication_critical", False))

    @staticmethod
    def _job_build_promise_done(request: object) -> bool:
        """Whether a request's shared promise has already been resolved.

        Reads defensively like the other request predicates: embedders and
        tests plant lightweight request doubles without a promise, which are
        never resolved-and-abandoned flights.
        """

        promise = getattr(request, "promise", None)
        return promise is not None and promise.done()

    def _record_priority_admission_locked(
        self,
        request: JobBuildRequest,
        result: str,
    ) -> None:
        """Observe first builder admission from publication-priority reservation."""

        runtime = self._runtime
        if not runtime._job_build_is_publication_critical(request):
            return
        self.job_build_priority_counts[result] += 1
        if result not in {"started", "coalesced"}:
            return
        if request.priority_admission_recorded:
            return
        request.priority_admission_recorded = True
        elapsed = max(0.0, time.monotonic() - request.requested_monotonic)
        self.job_build_priority_admission_seconds["sum"] += elapsed
        self.job_build_priority_admission_seconds["count"] += 1

    def _record_initial_prepared_work_locked(self, result: str) -> None:
        self.initial_job_prepared_work_counts[result] += 1

    def _new_job_build_cancellation(self) -> JobBuildCancellation:
        return JobBuildCancellation(
            timeout_seconds=max(
                0.001,
                float(
                    getattr(
                        self._runtime,
                        "job_build_timeout_seconds",
                        DEFAULT_PRISM_JOB_BUILD_TIMEOUT_SECONDS,
                    )
                ),
            )
        )

    def _begin_job_build_priority_preparation(
        self,
        requested_monotonic: float | None = None,
    ) -> tuple[int, float]:
        """Reserve publication priority before immutable request construction."""

        started = (
            time.monotonic()
            if requested_monotonic is None
            else requested_monotonic
        )
        with self._job_build_scheduler_lock:
            self._job_build_priority_preparation_sequence += 1
            token = self._job_build_priority_preparation_sequence
            self._job_build_priority_preparations[token] = started
            for routine_cancellation_ref in tuple(
                self._job_build_routine_preparations.values()
            ):
                routine_cancellation = routine_cancellation_ref()
                if routine_cancellation is not None:
                    routine_cancellation.cancel("publication priority")
            self._job_build_routine_preparations.clear()
            self._job_build_priority_changed.set()
        return token, started

    def _finish_job_build_priority_preparation(self, token: int) -> None:
        with self._job_build_scheduler_lock:
            self._job_build_priority_preparations.pop(token, None)
            self._job_build_priority_changed.set()

    def _begin_routine_job_build_preparation(
        self,
        *,
        request_source: str,
        cancelled: Callable[[], bool] | None,
    ) -> tuple[int, JobBuildCancellation, CachedJobBundle | None]:
        """Atomically admit cancellable routine request construction.

        Initial requests do not poll out the publication-priority window:
        they subscribe to the in-flight priority build's promise, wake when
        it completes, and hand its bundle back to the caller. Riding the
        result that displaced them replaces the old defer-then-rebuild cycle
        (and its wakeup storm) with one shared build.
        """

        runtime = self._runtime
        deferred_recorded = False
        subscription_recorded = False
        subscribed_bundle: CachedJobBundle | None = None
        admission_started_monotonic = time.monotonic()
        admission_deadline_seconds = float(
            getattr(
                runtime,
                "routine_admission_deadline_seconds",
                DEFAULT_PRISM_ROUTINE_ADMISSION_DEADLINE_SECONDS,
            )
        )
        while True:
            self._job_build_priority_changed.clear()
            priority_promises: tuple[Future[CachedJobBundle], ...] = ()
            with self._job_build_scheduler_lock:
                if not runtime._publication_priority_scheduled_locked():
                    self._job_build_routine_preparation_sequence += 1
                    token = self._job_build_routine_preparation_sequence
                    preparation_cancellation = (
                        runtime._new_job_build_cancellation()
                    )
                    coordinator_ref = weakref.ref(runtime)

                    def remove_dead_preparation(
                        dead_ref: weakref.ReferenceType[
                            JobBuildCancellation
                        ],
                        *,
                        preparation_token: int = token,
                    ) -> None:
                        coordinator = coordinator_ref()
                        if coordinator is None:
                            return
                        with coordinator._job_build_scheduler_lock:
                            if (
                                coordinator._job_build_routine_preparations.get(
                                    preparation_token
                                )
                                is dead_ref
                            ):
                                coordinator._job_build_routine_preparations.pop(
                                    preparation_token,
                                    None,
                                )

                    self._job_build_routine_preparations[token] = weakref.ref(
                        preparation_cancellation,
                        remove_dead_preparation,
                    )
                    return token, preparation_cancellation, subscribed_bundle
                if not deferred_recorded:
                    self.job_build_priority_counts["routine_deferred"] += 1
                    if request_source == "initial":
                        runtime._record_initial_prepared_work_locked("deferred")
                    deferred_recorded = True
                if request_source == "initial":
                    priority_promises = (
                        runtime._publication_priority_promises_locked()
                    )
                    if priority_promises and not subscription_recorded:
                        runtime._record_initial_prepared_work_locked("subscribed")
                        subscription_recorded = True
            if cancelled is not None and cancelled():
                raise JobBuildWaiterCancelled(
                    "job bundle request was cancelled behind publication priority"
                )
            stop_event = getattr(runtime, "stop_event", None)
            if stop_event is not None and stop_event.is_set():
                raise JobBuildWaiterCancelled(
                    "coordinator stopped behind publication priority"
                )
            parked_seconds = time.monotonic() - admission_started_monotonic
            if parked_seconds > admission_deadline_seconds:
                # Unbounded parking here is how the 2026-07-29 incident hid:
                # initial jobs rode the client's own 90s timeout with zero
                # tracebacks. Fail fast and visibly instead; callers retry
                # with their normal pacing.
                with self._job_build_scheduler_lock:
                    if request_source == "initial":
                        runtime._record_initial_prepared_work_locked(
                            "admission_deadline"
                        )
                    now_monotonic = time.monotonic()
                    should_log = (
                        self._admission_deadline_last_log_monotonic is None
                        or now_monotonic
                        - self._admission_deadline_last_log_monotonic >= 5.0
                    )
                    if should_log:
                        self._admission_deadline_last_log_monotonic = (
                            now_monotonic
                        )
                if should_log:
                    print(
                        "prism coordinator: job admission deadline exceeded "
                        f"source={request_source} "
                        f"parked_seconds={parked_seconds:.1f}",
                        flush=True,
                    )
                raise JobBuildAdmissionDeadlineExceeded(
                    "job bundle admission parked behind publication priority "
                    f"for {parked_seconds:.1f}s"
                )
            if priority_promises:
                for promise in priority_promises:
                    if (
                        promise.done()
                        and not promise.cancelled()
                        and promise.exception() is None
                    ):
                        subscribed_bundle = promise.result()
                pending_promises = tuple(
                    promise
                    for promise in priority_promises
                    if not promise.done()
                )
                if pending_promises:
                    done, _pending = wait(
                        pending_promises,
                        timeout=PRISM_INITIAL_JOB_SUBSCRIBE_POLL_SECONDS,
                        return_when=FIRST_COMPLETED,
                    )
                    for completed in done:
                        if (
                            completed.cancelled()
                            or completed.exception() is not None
                        ):
                            continue
                        subscribed_bundle = completed.result()
                else:
                    # Every live priority promise has resolved; the scheduler
                    # is about to sweep the finished flights. Waiting on the
                    # event instead of the done promises avoids a hot loop.
                    self._job_build_priority_changed.wait(0.05)
            else:
                # Reservation-only window: the priority request object (and
                # its promise) does not exist yet. This window only spans
                # immutable request construction, never the build itself.
                self._job_build_priority_changed.wait(0.05)

    def _publication_priority_promises_locked(
        self,
    ) -> tuple[Future[CachedJobBundle], ...]:
        """Promises of live publication-critical build requests."""

        runtime = self._runtime
        promises: list[Future[CachedJobBundle]] = []
        pending = self._job_build_pending
        if (
            pending is not None
            and not pending.cancellation.is_set()
            and runtime._job_build_is_publication_critical(pending)
        ):
            promises.append(pending.promise)
        for flight in (self._job_build_active, self._job_build_retiring):
            if (
                flight is not None
                and not flight.request.cancellation.is_set()
                and runtime._job_build_is_publication_critical(flight.request)
            ):
                promises.append(flight.request.promise)
        return tuple(promises)

    def _finish_routine_job_build_preparation(self, token: int) -> None:
        with self._job_build_scheduler_lock:
            self._job_build_routine_preparations.pop(token, None)

    def _publication_priority_scheduled_locked(self) -> bool:
        runtime = self._runtime
        if self._job_build_priority_preparations:
            return True
        pending = self._job_build_pending
        if (
            pending is not None
            and not pending.cancellation.is_set()
            and not runtime._job_build_promise_done(pending)
            and runtime._job_build_is_publication_critical(pending)
        ):
            return True
        return any(
            flight is not None
            and not flight.request.cancellation.is_set()
            and not runtime._job_build_promise_done(flight.request)
            and runtime._job_build_is_publication_critical(flight.request)
            for flight in (
                self._job_build_active,
                self._job_build_retiring,
            )
        )

    def _job_build_can_inherit_publication_priority(
        self,
        existing: JobBuildRequest,
        incoming: JobBuildRequest,
    ) -> bool:
        """Reject an almost-expired or stalled routine flight as a critical owner."""

        runtime = self._runtime
        if (
            not runtime._job_build_is_publication_critical(incoming)
            or runtime._job_build_is_publication_critical(existing)
        ):
            return True
        cancellation = existing.cancellation
        total_budget = max(
            0.001,
            cancellation.deadline_monotonic - cancellation.started_monotonic,
        )
        remaining_budget = cancellation.deadline_monotonic - time.monotonic()
        progress_budget = max(
            0.001,
            float(
                getattr(
                    runtime,
                    "job_build_cancel_grace_seconds",
                    DEFAULT_PRISM_JOB_BUILD_CANCEL_GRACE_SECONDS,
                )
            ),
        )
        progress_age = (
            time.monotonic()
            - float(
                getattr(
                    cancellation,
                    "last_checkpoint_monotonic",
                    cancellation.started_monotonic,
                )
            )
        )
        return (
            remaining_budget >= total_budget / 2.0
            and progress_age <= progress_budget
        )

    @staticmethod
    def _resolve_cancelled_job_build_promise(
        request: JobBuildRequest,
        reason: str,
    ) -> None:
        """Wake every waiter on a terminated flight's promise immediately.

        Safe against the build's own completion: the done callback checks
        ``promise.done()`` before resolving, so the first resolution wins and
        a late build result for a cancelled flight is dropped exactly as it
        was when the callback produced the exception itself.
        """

        promise = getattr(request, "promise", None)
        if promise is None or promise.done():
            return
        if reason == "timeout":
            promise.set_exception(
                JobBuildCancelled("job build flight cancelled by timeout")
            )
            return
        promise.set_exception(
            JobBuildSuperseded(f"job build flight cancelled: {reason}")
        )

    @staticmethod
    def _job_build_flight_outcome(
        request: JobBuildRequest,
        future: Future[CachedJobBundle],
    ) -> tuple[CachedJobBundle | None, BaseException | None]:
        """Map a finished executor future onto the shared promise outcome."""

        result: CachedJobBundle | None = None
        error: BaseException | None = None
        try:
            result = future.result()
            if request.cancellation.is_set():
                if request.cancellation.reason == "timeout":
                    error = JobBuildCancelled(
                        "job build completed after its timeout"
                    )
                else:
                    error = JobBuildSuperseded(
                        "obsolete job build completed after cancellation"
                    )
        except BaseException as exc:  # noqa: BLE001 - delivered to all waiters
            error = exc
        return result, error

    def _evict_orphaned_job_build_flights_locked(self) -> list[str]:
        """Evict finished flights whose completion never released their slot.

        A flight's done callback resolves its promise, vacates its slot, and
        promotes pending work. Done callbacks swallow exceptions, so if one
        dies mid-pass the flight wedges: admission keeps treating the slot as
        occupied, deferred requesters chain onto a promise nobody will ever
        resolve, and promotion never runs again even though the executor is
        idle. Sweep such flights out at admission time: resolve the promise
        from the finished future so parked waiters wake, free the slot so the
        next requester becomes the new owner, and promote any parked pending
        request. A flight whose future is still queued or executing is never
        touched; its own completion performs the normal cleanup.

        Returns the eviction log lines instead of printing them: the caller
        holds the scheduler lock, and a blocked stdout must not stall
        admission (the admission-deadline log follows the same discipline).
        """

        runtime = self._runtime
        eviction_logs: list[str] = []
        grace_seconds = float(
            getattr(
                runtime,
                "job_build_orphan_sweep_grace_seconds",
                PRISM_JOB_BUILD_ORPHAN_SWEEP_GRACE_SECONDS,
            )
        )
        now = time.monotonic()
        evicted = False
        for slot_name in ("_job_build_active", "_job_build_retiring"):
            flight: JobBuildFlight | None = getattr(self, slot_name)
            if flight is None:
                continue
            # Embedders and tests plant lightweight flight doubles; read the
            # lifecycle fields defensively like the rest of the scheduler.
            future = getattr(flight, "future", None)
            if future is not None and not future.done():
                continue
            observed = getattr(flight, "orphan_observed_monotonic", None)
            if observed is None:
                try:
                    flight.orphan_observed_monotonic = now
                except AttributeError:
                    pass
                continue
            if now - observed < grace_seconds:
                continue
            request = flight.request
            promise = getattr(request, "promise", None)
            if promise is not None and not promise.done():
                if future is None:
                    runtime._resolve_cancelled_job_build_promise(
                        request,
                        "superseded",
                    )
                else:
                    result, error = runtime._job_build_flight_outcome(
                        request,
                        future,
                    )
                    try:
                        if error is not None:
                            promise.set_exception(error)
                        else:
                            assert result is not None
                            promise.set_result(result)
                    except InvalidStateError:
                        # The flight's own done callback won the resolution
                        # race between our done() check and this set.
                        pass
            setattr(self, slot_name, None)
            self.job_build_scheduler_counts["orphan_evicted"] += 1
            evicted = True
            eviction_logs.append(
                "prism coordinator: evicted orphaned job build flight "
                f"slot={slot_name.removeprefix('_job_build_')} "
                f"cancelled={flight.request.cancellation.is_set()} "
                f"started={future is not None}"
            )
        if evicted:
            runtime._promote_pending_job_build_locked()
            self._job_build_priority_changed.set()
        return eviction_logs

    def _cancel_job_build_flight_locked(
        self,
        flight: JobBuildFlight,
        reason: str,
        *,
        now: float | None = None,
    ) -> bool:
        runtime = self._runtime
        if not flight.request.cancellation.cancel(reason):
            # Already cancelled (possibly by its own deadline). A flight that
            # never reached the executor still has no other resolver, so
            # settle its waiters here instead of leaving them to the sweep.
            if getattr(flight, "future", None) is None:
                runtime._resolve_cancelled_job_build_promise(
                    flight.request,
                    flight.request.cancellation.reason or reason,
                )
            return False
        try:
            flight.request.superseded_monotonic = (
                time.monotonic() if now is None else now
            )
            self.job_build_scheduler_counts["supersessions"] += 1
            if reason == "publication priority":
                self.job_build_priority_counts["routine_preempted"] += 1
        finally:
            # A submitted future delivers the promise from its done callback
            # when the build observes the cancellation and drains -- refresh
            # drivers rely on that ordering to keep one heavy build in
            # flight, and a completed-but-unswept future is the admission
            # sweep's to resolve. A flight that never reached the executor
            # has no callback at all: cancellation must wake its waiters
            # itself or they burn their full wait deadline.
            if getattr(flight, "future", None) is None:
                runtime._resolve_cancelled_job_build_promise(
                    flight.request,
                    reason,
                )
            self._job_build_priority_changed.set()
        return True

    def _promote_pending_job_build_locked(self) -> None:
        runtime = self._runtime
        pending = self._job_build_pending
        if pending is None:
            return
        if runtime._job_build_promise_done(pending):
            # Every waiter has already been answered (eviction or a raced
            # resolution); building it would spend a bounded executor slot on
            # work nobody consumes.
            self._job_build_pending = None
            return
        active = self._job_build_active
        retiring = self._job_build_retiring
        if (
            not runtime._job_build_is_publication_critical(pending)
            and any(
                flight is not None
                and not flight.request.cancellation.is_set()
                and not runtime._job_build_promise_done(flight.request)
                and runtime._job_build_is_publication_critical(flight.request)
                for flight in (active, retiring)
            )
        ):
            # The pending request may predate an exact-flight priority upgrade.
            # Preserve the same admission invariant as _request_job_build:
            # routine work cannot displace scheduled publication work.
            return
        if active is not None:
            if retiring is not None:
                return
            if runtime._ready_job_build_precedes_collection(
                active.request,
                pending,
            ) and not runtime._job_build_is_publication_critical(pending):
                return
            if not runtime._collection_job_builds_are_independent(
                active.request,
                pending,
            ):
                reason = (
                    "publication priority"
                    if runtime._job_build_is_publication_critical(pending)
                    and not runtime._job_build_is_publication_critical(active.request)
                    else "superseded"
                )
                runtime._cancel_job_build_flight_locked(active, reason)
            self._job_build_retiring = active
            self._job_build_active = None
        elif retiring is not None:
            if runtime._ready_job_build_precedes_collection(
                retiring.request,
                pending,
            ) and not runtime._job_build_is_publication_critical(pending):
                return
            if not runtime._collection_job_builds_are_independent(
                retiring.request,
                pending,
            ):
                reason = (
                    "publication priority"
                    if runtime._job_build_is_publication_critical(pending)
                    and not runtime._job_build_is_publication_critical(retiring.request)
                    else "superseded"
                )
                runtime._cancel_job_build_flight_locked(retiring, reason)
        self._job_build_pending = None
        try:
            flight = runtime._start_job_build_locked(pending)
        except BaseException:
            # The pending slot is already vacated: with no flight there is no
            # done callback left that could ever resolve this promise for its
            # waiters.
            runtime._resolve_cancelled_job_build_promise(pending, "superseded")
            raise
        self._job_build_active = flight
        runtime._arm_job_build_locked(flight)

    def _job_build_done(
        self,
        flight: JobBuildFlight,
        future: Future[CachedJobBundle],
    ) -> None:
        runtime = self._runtime
        request = flight.request
        result, error = runtime._job_build_flight_outcome(request, future)
        try:
            with self._job_build_scheduler_lock:
                self.job_build_scheduler_counts["completions"] += 1
                self.shared_bundle_preparation_count += 1
                self.shared_bundle_preparation_seconds_sum += max(
                    0.0,
                    time.monotonic() - request.cancellation.started_monotonic,
                )
                if request.cancellation.cancelled_monotonic is not None:
                    elapsed = max(
                        0.0,
                        time.monotonic()
                        - request.cancellation.cancelled_monotonic,
                    )
                    self.job_build_cancellation_seconds["sum"] += elapsed
                    self.job_build_cancellation_seconds["count"] += 1
                coordination_cancelled = isinstance(
                    error, JobBuildSuperseded
                ) or (
                    request.cancellation.is_set()
                    and request.cancellation.reason != "timeout"
                )
                if error is not None and coordination_cancelled:
                    self.job_build_scheduler_counts["obsolete_results"] += 1
                    self.shared_bundle_build_counts["superseded"] += 1
                    with runtime._tip_refresh_metrics_lock:
                        runtime.tip_refresh_superseded_results += 1
                elif error is not None:
                    self.shared_bundle_build_counts["failed"] += 1
                else:
                    self.shared_bundle_build_counts["completed"] += 1
                if self._job_build_active is flight:
                    self._job_build_active = None
                if self._job_build_retiring is flight:
                    self._job_build_retiring = None
                runtime._promote_pending_job_build_locked()
        finally:
            # Done callbacks swallow exceptions, so a raise anywhere in the
            # bookkeeping above must not skip promise resolution: an orphaned
            # promise strands every joiner until its wait deadline and leaves
            # a finished flight wedged in its slot with the executor idle.
            try:
                if not request.promise.done():
                    if error is not None:
                        request.promise.set_exception(error)
                    else:
                        assert result is not None
                        request.promise.set_result(result)
            except InvalidStateError:
                # The admission sweep can resolve a wedged flight between the
                # done() check and this set; either resolution wakes waiters.
                pass
            self._job_build_priority_changed.set()

    def _request_job_build(self, request: JobBuildRequest) -> Future[CachedJobBundle]:
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        with self._job_build_scheduler_lock:
            eviction_logs = runtime._evict_orphaned_job_build_flights_locked()
        for eviction_log in eviction_logs:
            print(eviction_log, flush=True)
        with self._job_build_scheduler_lock:
            if getattr(request, "idle_retarget", False):
                with runtime.lock:
                    defer_idle = runtime._vardiff_idle_tip_divergence_locked()
                if defer_idle:
                    request.cancellation.cancel(
                        "idle retarget deferred during unpublished tip refresh"
                    )
                    if not request.promise.done():
                        request.promise.set_exception(
                            JobBuildSuperseded(
                                "idle retarget deferred during unpublished tip refresh"
                            )
                        )
                    return request.promise
            self.job_build_scheduler_counts["requests"] += 1
            active = self._job_build_active
            retiring = self._job_build_retiring
            pending = self._job_build_pending
            publication_critical = runtime._job_build_is_publication_critical(
                request
            )
            if (
                active is not None
                and not active.request.cancellation.is_set()
                and not runtime._job_build_promise_done(active.request)
                and runtime._job_build_requests_can_share(active.request, request)
                and runtime._job_build_can_inherit_publication_priority(
                    active.request,
                    request,
                )
            ):
                if publication_critical:
                    active.request.publication_critical = True
                    active.request.request_source = request.request_source
                    active.request.requested_monotonic = request.requested_monotonic
                    active.request.priority_admission_recorded = True
                    runtime._record_priority_admission_locked(request, "coalesced")
                if request.request_source == "initial":
                    runtime._record_initial_prepared_work_locked("singleflight")
                return active.request.promise
            if (
                retiring is not None
                and not retiring.request.cancellation.is_set()
                and not runtime._job_build_promise_done(retiring.request)
                and runtime._job_build_requests_can_share(retiring.request, request)
                and runtime._job_build_can_inherit_publication_priority(
                    retiring.request,
                    request,
                )
            ):
                if publication_critical:
                    retiring.request.publication_critical = True
                    retiring.request.request_source = request.request_source
                    retiring.request.requested_monotonic = request.requested_monotonic
                    retiring.request.priority_admission_recorded = True
                    runtime._record_priority_admission_locked(request, "coalesced")
                if request.request_source == "initial":
                    runtime._record_initial_prepared_work_locked("singleflight")
                return retiring.request.promise
            if (
                pending is not None
                and not pending.cancellation.is_set()
                and not runtime._job_build_promise_done(pending)
                and runtime._job_build_requests_can_share(pending, request)
                and runtime._job_build_can_inherit_publication_priority(
                    pending,
                    request,
                )
            ):
                if publication_critical:
                    pending.publication_critical = True
                    pending.request_source = request.request_source
                    pending.requested_monotonic = request.requested_monotonic
                    runtime._record_priority_admission_locked(request, "queued")
                    now = time.monotonic()
                    for occupied in (active, retiring):
                        if (
                            occupied is not None
                            and not runtime._job_build_requests_can_share(
                                occupied.request,
                                pending,
                            )
                            and not runtime._job_build_is_publication_critical(
                                occupied.request
                            )
                        ):
                            runtime._cancel_job_build_flight_locked(
                                occupied,
                                "publication priority",
                                now=now,
                            )
                    runtime._promote_pending_job_build_locked()
                if request.request_source == "initial":
                    runtime._record_initial_prepared_work_locked("singleflight")
                return pending.promise

            if not publication_critical:
                priority_blockers = tuple(
                    blocker
                    for blocker in (
                        active.request if active is not None else None,
                        retiring.request if retiring is not None else None,
                        pending,
                    )
                    if blocker is not None
                    and not blocker.cancellation.is_set()
                    # A resolved promise is not live priority work: deferring
                    # on it wakes instantly and spins until the sweep evicts.
                    and not runtime._job_build_promise_done(blocker)
                    and runtime._job_build_is_publication_critical(blocker)
                )
                if priority_blockers:
                    self.job_build_priority_counts["routine_deferred"] += 1
                    if request.request_source == "initial":
                        runtime._record_initial_prepared_work_locked("deferred")
                    return runtime._defer_job_build_locked(
                        *(blocker.promise for blocker in priority_blockers)
                    )

            if request.mode == "collection":
                possible_blockers = (
                    pending,
                    active.request if active is not None else None,
                    retiring.request if retiring is not None else None,
                )
                for blocker in possible_blockers:
                    if (
                        blocker is not None
                        and not blocker.cancellation.is_set()
                        and not runtime._job_build_promise_done(blocker)
                        and runtime._ready_job_build_precedes_collection(
                            blocker,
                            request,
                        )
                        and not (
                            publication_critical
                            and not runtime._job_build_is_publication_critical(
                                blocker
                            )
                        )
                    ):
                        return runtime._defer_job_build_locked(blocker.promise)
            if active is None:
                if pending is not None:
                    if runtime._collection_job_builds_are_independent(
                        pending,
                        request,
                    ):
                        self._job_build_pending = None
                        try:
                            flight = runtime._start_job_build_locked(pending)
                        except BaseException:
                            # The pending slot is already vacated: with no
                            # flight there is no done callback left that
                            # could resolve this promise for its waiters.
                            runtime._resolve_cancelled_job_build_promise(
                                pending,
                                "superseded",
                            )
                            raise
                        if retiring is None:
                            try:
                                replacement = runtime._start_job_build_locked(
                                    request
                                )
                            except BaseException:
                                # The first start succeeded: slot and arm it
                                # so its completion still resolves and sweeps
                                # normally before this failure propagates.
                                self._job_build_active = flight
                                runtime._arm_job_build_locked(flight)
                                raise
                            self._job_build_retiring = flight
                            self._job_build_active = replacement
                            runtime._arm_job_build_locked(flight)
                            runtime._arm_job_build_locked(replacement)
                            return request.promise
                        self._job_build_active = flight
                        runtime._arm_job_build_locked(flight)
                        return runtime._defer_job_build_locked(
                            flight.request.promise,
                            retiring.request.promise,
                        )
                    pending.cancellation.cancel("superseded while pending")
                    if not pending.promise.done():
                        pending.promise.set_exception(
                            JobBuildSuperseded("pending job build was superseded")
                        )
                    self._job_build_pending = None
                    self.job_build_scheduler_counts["supersessions"] += 1
                if (
                    retiring is not None
                    and not runtime._collection_job_builds_are_independent(
                        retiring.request,
                        request,
                    )
                ):
                    now = time.monotonic()
                    reason = (
                        "publication priority"
                        if publication_critical
                        and not runtime._job_build_is_publication_critical(
                            retiring.request
                        )
                        else "superseded"
                    )
                    if runtime._cancel_job_build_flight_locked(
                        retiring,
                        reason,
                        now=now,
                    ):
                        request.superseded_monotonic = now
                flight = runtime._start_job_build_locked(request)
                self._job_build_active = flight
                runtime._arm_job_build_locked(flight)
                return request.promise

            if runtime._collection_job_builds_are_independent(
                active.request,
                request,
            ):
                if self._job_build_retiring is None:
                    self._job_build_retiring = active
                    flight = runtime._start_job_build_locked(request)
                    self._job_build_active = flight
                    runtime._arm_job_build_locked(flight)
                    return request.promise
                if pending is None:
                    self._job_build_pending = request
                    if publication_critical:
                        runtime._record_priority_admission_locked(request, "queued")
                        now = time.monotonic()
                        for occupied in (active, self._job_build_retiring):
                            if (
                                occupied is not None
                                and not runtime._job_build_is_publication_critical(
                                    occupied.request
                                )
                            ):
                                runtime._cancel_job_build_flight_locked(
                                    occupied,
                                    "publication priority",
                                    now=now,
                                )
                    return request.promise
                assert retiring is not None
                if publication_critical:
                    pending.cancellation.cancel("superseded while pending")
                    if not pending.promise.done():
                        pending.promise.set_exception(
                            JobBuildSuperseded(
                                "pending job build was superseded by publication priority"
                            )
                        )
                    self.job_build_scheduler_counts["supersessions"] += 1
                    self._job_build_pending = request
                    runtime._record_priority_admission_locked(request, "queued")
                    now = time.monotonic()
                    for occupied in (active, retiring):
                        if not runtime._job_build_is_publication_critical(
                            occupied.request
                        ):
                            runtime._cancel_job_build_flight_locked(
                                occupied,
                                "publication priority",
                                now=now,
                            )
                    return request.promise
                return runtime._defer_job_build_locked(
                    active.request.promise,
                    retiring.request.promise,
                )

            now = time.monotonic()
            for obsolete in (active, retiring):
                if obsolete is not None:
                    reason = (
                        "publication priority"
                        if publication_critical
                        and not runtime._job_build_is_publication_critical(
                            obsolete.request
                        )
                        else "superseded"
                    )
                    runtime._cancel_job_build_flight_locked(
                        obsolete,
                        reason,
                        now=now,
                    )
            request.superseded_monotonic = now
            if retiring is None:
                self._job_build_retiring = active
                flight = runtime._start_job_build_locked(request)
                self._job_build_active = flight
                runtime._arm_job_build_locked(flight)
                return request.promise

            previous_pending = self._job_build_pending
            if previous_pending is not None:
                previous_pending.cancellation.cancel("superseded while pending")
                if not previous_pending.promise.done():
                    previous_pending.promise.set_exception(
                        JobBuildSuperseded("pending job build was superseded")
                    )
                self.job_build_scheduler_counts["supersessions"] += 1
            self._job_build_pending = request
            if publication_critical:
                runtime._record_priority_admission_locked(request, "queued")
            return request.promise

    def _cancel_obsolete_job_builds(
        self,
        reason: str,
        *,
        keep_published_snapshot: bool = False,
    ) -> None:
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        published_parent: str | None = None
        published_fingerprint: str | None = None
        if keep_published_snapshot:
            # A per-client build for exactly the published snapshot is still
            # valid, creditable work when a same-tip template bump sweeps old
            # fingerprints. Detection-time sweeps deliberately do not use this:
            # once a replacement tip is detected, even published-tip builds
            # must vacate the single-flight lane for the replacement's work.
            # Snapshot the identity outside the scheduler lock so the keep
            # check stays lock-free below.
            with runtime.lock:
                published = getattr(runtime, "current_tip_first_seen", None)
                snapshot = getattr(runtime, "tip_template_snapshot", None)
                snapshot_tip = getattr(snapshot, "bestblockhash", None)
                snapshot_fingerprint = getattr(
                    snapshot, "template_fingerprint", None
                )
                if (
                    published is not None
                    and snapshot_tip == published[0]
                    and snapshot_fingerprint is not None
                    and runtime._published_tip_authoritative_locked(time.monotonic())
                ):
                    published_parent = published[0]
                    published_fingerprint = snapshot_fingerprint

        def keep(request: JobBuildRequest) -> bool:
            return bool(
                published_fingerprint is not None
                and request.artifacts.previousblockhash == published_parent
                and request.artifacts.fingerprint == published_fingerprint
            )

        with self._job_build_scheduler_lock:
            for flight in (self._job_build_active, self._job_build_retiring):
                if flight is not None and not keep(flight.request):
                    runtime._cancel_job_build_flight_locked(flight, reason)
            pending = self._job_build_pending
            if pending is not None and not keep(pending):
                pending.cancellation.cancel(reason)
                if not pending.promise.done():
                    pending.promise.set_exception(
                        JobBuildSuperseded(f"pending job build {reason}")
                    )
                self._job_build_pending = None
                self.job_build_scheduler_counts["supersessions"] += 1

    def shutdown_job_build_executor(self) -> None:
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        with self._job_build_scheduler_lock:
            for flight in (self._job_build_active, self._job_build_retiring):
                if flight is None:
                    continue
                flight.request.cancellation.cancel("shutdown")
                future = getattr(flight, "future", None)
                if future is None or future.done():
                    # A terminal future's callback has already run (or died)
                    # and executor shutdown will not fire it again: this is
                    # the last chance to settle the flight's waiters.
                    runtime._resolve_cancelled_job_build_promise(
                        flight.request,
                        "shutdown",
                    )
            pending = self._job_build_pending
            if pending is not None:
                pending.cancellation.cancel("shutdown")
                if not pending.promise.done():
                    pending.promise.set_exception(
                        JobBuildSuperseded("pending job build cancelled by shutdown")
                    )
            self._job_build_pending = None
            executor = self._job_build_executor
            self._job_build_executor = None
            self._job_build_executor_shutdown = True
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)

    # -- cache identity and admission --------------------------------------

    def _job_bundle_payout_state_current(self, bundle: CachedJobBundle) -> bool:
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        with self._job_cache_lock:
            artifact = runtime._published_payout_state.artifact
            return bool(
                bundle.payout_state_generation == runtime._payout_state_generation
                and bundle.build_key is not None
                and (
                    bundle.collection_only
                    or int(
                        getattr(
                            bundle.build_key,
                            "payout_append_invalidation_epoch",
                            0,
                        )
                    )
                    == runtime._payout_ledger_append_invalidation_epoch
                )
                and artifact is not None
                and bundle.build_key.payout_artifact_sha256
                == artifact.prior_balances_sha256
            )

    @staticmethod
    def _collection_bundle_identity(worker: WorkerIdentityPort) -> tuple[str, str]:
        return worker.payout_address, worker.p2mr_program_hex

    def _job_bundle_key(
        self,
        artifacts: CachedTemplateArtifacts,
        *,
        mode: str,
        payout_state_generation: int,
        payout_artifact_generation: int = 0,
        worker: WorkerIdentityPort | None,
    ) -> tuple[object, ...]:
        runtime = self._runtime
        if mode == "ready":
            return (
                artifacts.fingerprint,
                artifacts.previousblockhash,
                "ready",
                payout_state_generation,
                payout_artifact_generation,
            )
        if mode != "collection":
            raise ValueError(f"unknown PRISM job-bundle mode: {mode}")
        if worker is None:
            raise CollectionIdentityUnavailable(
                "collection-mode worker identity is temporarily unavailable"
            )
        return (
            artifacts.fingerprint,
            artifacts.previousblockhash,
            "collection",
            artifacts.generation,
            payout_state_generation,
            payout_artifact_generation,
            *runtime._collection_bundle_identity(worker),
        )

    def _job_bundle_mode(self, requested_mode: str | None) -> str:
        runtime = self._runtime
        if requested_mode is not None:
            if requested_mode not in {"ready", "collection"}:
                raise ValueError(
                    f"unknown PRISM job-bundle mode: {requested_mode}"
                )
            return requested_mode
        return "ready" if runtime.pool_readiness_latched() else "collection"

    def _lookup_job_bundle(
        self,
        key: tuple[object, ...],
    ) -> CachedJobBundle | None:
        runtime = self._runtime
        ttl = getattr(runtime, "job_bundle_cache_seconds", DEFAULT_PRISM_JOB_BUNDLE_CACHE_SECONDS)
        now = time.monotonic()
        with self._job_cache_lock:
            if ttl <= 0:
                self._job_bundle_cache.clear()
                return None
            # The entry-count cap is not a memory bound: one production entry
            # can reference more than 100k shares.  Expired entries must release
            # their snapshots instead of remaining resident until count eviction.
            expired = [
                cache_key
                for cache_key, entry in self._job_bundle_cache.items()
                if now - entry.built_monotonic > ttl
            ]
            for cache_key in expired:
                self._job_bundle_cache.pop(cache_key, None)
            return self._job_bundle_cache.get(key)
        return None

    def _no_artifact_job_bundle_key(
        self,
        artifacts: CachedTemplateArtifacts,
        *,
        mode: str,
        payout_state_generation: int,
        worker: WorkerIdentityPort | None,
    ) -> tuple[object, ...]:
        """Fallback cache identity for work built before an artifact re-key.

        A synchronous build caches its bundle under the no-artifact key and
        then publishes its anchored window as the payout ledger artifact. The
        next lookup is keyed to that artifact and would miss the still-fresh
        bundle and rebuild identical work: within one payout generation and
        template identity the no-artifact bundle binds the same balances,
        window anchor, and policy inputs, so it remains a correct cache
        identity to serve -- but only while its window digest matches the
        armed artifact (see _no_artifact_bundle_matches_artifact); a fence
        re-arm can install a fresher window that cached work must not shadow.
        """
        return self._runtime._job_bundle_key(
            artifacts,
            mode=mode,
            payout_state_generation=payout_state_generation,
            payout_artifact_generation=0,
            worker=worker,
        )

    @staticmethod
    def _no_artifact_bundle_matches_artifact(
        cached: CachedJobBundle,
        payout_artifact: Any,
    ) -> bool:
        """Whether pre-re-key cached work carries exactly the artifact's window.

        A speculative rebuild can arm a fresher artifact while an older
        no-artifact bundle is still inside its cache TTL. Serving that bundle
        would shadow the fresher window behind cached work, so the fallback
        identity is honored only when the window digests are byte-identical
        -- which the bundle that published the artifact satisfies by
        construction.
        """
        return (
            payout_artifact is not None
            and payout_artifact.share_snapshot_sha256 is not None
            and cached.build_key is not None
            and cached.build_key.share_snapshot_sha256
            == payout_artifact.share_snapshot_sha256
        )

    def _job_bundle_entry_usable(
        self,
        cached: CachedJobBundle | None,
        artifacts: CachedTemplateArtifacts,
    ) -> bool:
        """Re-validate freshness and readiness for cached bundles.

        Readiness is monotonic in practice (the distinct accepted-miner count
        only grows), so submit-capable ready bundles are served as-is while
        their declared window anchor stays inside the artifact staleness
        bound. A cached collection bundle is re-checked against the cheap
        aggregate stats: once the pool is ready it must stop being served, or
        jobs would keep collecting winning shares without submitting blocks
        for up to the cache TTL.
        """
        runtime = self._runtime
        if cached is None:
            return False
        # Usability follows the same split-authority rule as construction: a
        # cached bundle for the newest detected tip stays reusable across
        # refresh retries while the previous tip is still published, and the
        # published snapshot's own bundle stays servable for pinned issuance.
        with runtime.lock:
            parent_usable = runtime._artifacts_buildable_locked(artifacts)
        if not parent_usable:
            return False
        with self._job_cache_lock:
            if runtime._payout_state_publication_blocked:
                return False
        if not runtime._job_bundle_payout_state_current(cached):
            return False
        if not cached.collection_only:
            # Cached-ready-bundle validity follows the same ceiling/floor
            # split as artifact reuse. The declared anchor is gated only by
            # the audit CEILING: the anchor is frozen per template
            # generation and predates the window walk, so a tight
            # wall-clock gate here declared every rebuilt bundle dead on
            # arrival once its template generation outlived the bound
            # (2026-07-29 rollback).
            found_block = getattr(cached, "found_block", None)
            declared_anchor_ms = (
                found_block.get("anchor_job_issued_at_ms")
                if isinstance(found_block, dict)
                else None
            )
            if declared_anchor_ms is not None and (
                self._now_ms() - int(declared_anchor_ms)
                > runtime._payout_artifact_max_anchor_age_ms()
            ):
                return False
            # A bundle keyed to the currently armed artifact generation
            # carries exactly the armed window, so it serves for as long as
            # reuse itself does (the armed artifact is event-invalidated by
            # the payout fences and re-anchored in the background). Every
            # other ready bundle (no-artifact builds and survivors of a
            # re-key) carries a window nothing will re-anchor, so it keeps
            # serving only inside the re-anchor floor -- its declared
            # anchor is already ceiling-gated above, and a wall-clock
            # budget tighter than the build cadence is exactly the
            # 2026-07-30 brownout (every consecutive build fell back to the
            # synchronous reward-window walk).
            if int(getattr(cached, "payout_artifact_generation", 0)) > 0:
                with self._job_cache_lock:
                    armed = runtime._payout_ledger_artifact
                if (
                    armed is not None
                    and int(armed.generation)
                    == int(cached.payout_artifact_generation)
                ):
                    return True
            return (
                time.monotonic() - float(cached.built_monotonic)
                <= runtime._payout_artifact_reanchor_seconds()
            )
        # Collection bundles sign a synthetic bootstrap share containing the
        # exact template ntime. A clock-only observation keeps the stable work
        # fingerprint, but it must rebuild this signed bundle instead of
        # rebinding the old manifest to a new template generation.
        if (
            cached.template is not artifacts.template
            or cached.template_generation != artifacts.generation
        ):
            return False
        try:
            _, ready_miner_count = runtime.accepted_share_stats()
        except Exception:
            # If readiness cannot be proven, force the normal build path. That
            # path will either build an up-to-date bundle or surface the ledger
            # failure instead of continuing to issue no-submit collection jobs.
            return False
        return ready_miner_count < runtime.min_ready_miners

    def _bind_cached_bundle_to_artifacts(
        self,
        cached: CachedJobBundle,
        artifacts: CachedTemplateArtifacts,
    ) -> CachedJobBundle:
        """Return the cached heavy bundle bound to this exact observation.

        Clock-only template changes intentionally keep the stable fingerprint.
        Ready bundles may reuse their ledger snapshot and signed manifest, but
        the Stratum base job must still carry the observing template's exact
        ntime and generation. Collection bundles are filtered before this point
        because their signed synthetic share contains the template ntime.
        """
        runtime = self._runtime
        if (
            cached.template is artifacts.template
            and cached.template_generation == artifacts.generation
        ):
            return cached
        manifest = cached.coinbase_manifest
        base_job = direct_stratum.make_job_from_builder_manifest(
            job_id="prism-template-base",
            template=artifacts.template,
            manifest=manifest,
            extranonce1_hex=PRISM_JOB_EXTRANONCE1_PLACEHOLDER_HEX,
            extranonce2_size=runtime.extranonce2_size,
            desired_share_difficulty=runtime.share_difficulty,
            clean_jobs=True,
            transaction_hexes=artifacts.transaction_hexes,
        )
        return dataclass_replace(
            cached,
            template=artifacts.template,
            base_job=base_job,
            template_generation=artifacts.generation,
            build_key=(
                dataclass_replace(
                    cached.build_key,
                    best_tip_hash=artifacts.previousblockhash,
                    previous_block_hash=artifacts.previousblockhash,
                    template_generation=artifacts.generation,
                    block_height=int(artifacts.template["height"]),
                    coinbase_value_sats=int(artifacts.template["coinbasevalue"]),
                )
                if cached.build_key is not None
                else None
            ),
        )

    def _new_job_build_request(
        self,
        artifacts: CachedTemplateArtifacts,
        worker: WorkerIdentityPort | None,
        *,
        mode: str,
        payout_state_generation: int,
        cache_key: tuple[object, ...],
        payout_ledger_artifact: Any = None,
        idle_retarget: bool = False,
        publication_critical: bool = False,
        request_source: str = "routine",
        priority_requested_monotonic: float | None = None,
        preparation_cancellation: JobBuildCancellation | None = None,
    ) -> JobBuildRequest:
        runtime = self._runtime
        cancellation = (
            runtime._new_job_build_cancellation()
            if preparation_cancellation is None
            else preparation_cancellation
        )
        cancellation.raise_if_cancelled("immutable snapshot")
        # A pending accepted-parent transition owns the balances children must
        # observe. Wait for its preview before acquiring any confirmed payout
        # input so a child build cannot take -- or cache into the published
        # artifact -- confirmed state that omits its new parent. The preview
        # itself is re-resolved consistently during construction.
        runtime._await_pending_parent_payout_preview(
            artifacts.previousblockhash,
            parent_height=int(artifacts.template["height"]) - 1,
        )
        payout_artifact = runtime._current_payout_state_artifact(cancellation)
        if payout_artifact.generation != payout_state_generation:
            raise JobBuildSuperseded(
                "payout artifact generation changed before build request"
            )

        phases = runtime._job_build_phases()
        payout_started = time.monotonic()
        payout_policy_json = canonical_json_text(runtime.prism_payout_policy())
        phases["payout"] = phases.get("payout", 0.0) + (
            time.monotonic() - payout_started
        )
        cancellation.raise_if_cancelled("payout policy")
        ctv_started = time.monotonic()
        ctv_settlement = runtime.prism_ctv_settlement_config(
            block_height=int(artifacts.template["height"]),
            parent_hash=artifacts.previousblockhash,
        )
        ctv_settlement_json = (
            canonical_json_text(ctv_settlement)
            if ctv_settlement is not None
            else None
        )
        phases["ctv"] = phases.get("ctv", 0.0) + (
            time.monotonic() - ctv_started
        )
        cancellation.raise_if_cancelled("CTV configuration")

        suffix_hex = runtime.coinbase_script_sig_suffix_hex(
            PRISM_JOB_EXTRANONCE1_PLACEHOLDER_HEX,
            "00" * runtime.extranonce2_size,
        )
        collection_identity = (
            runtime._collection_bundle_identity(worker)
            if mode == "collection" and worker is not None
            else None
        )
        decimal_context = getcontext().copy()
        numeric_context_sha256 = self._canonical_json_sha256(
            {
                "precision": decimal_context.prec,
                "rounding": decimal_context.rounding,
                "minimum_exponent": decimal_context.Emin,
                "maximum_exponent": decimal_context.Emax,
                "capitals": decimal_context.capitals,
                "clamp": decimal_context.clamp,
            }
        )
        with self._job_cache_lock:
            issued_at_ms = self._job_build_issued_at_ms.get(artifacts.generation)
            current_append_invalidation_epoch = int(
                runtime._payout_ledger_append_invalidation_epoch
            )
        if issued_at_ms is None:
            # The issued time doubles as the audit window anchor, so it
            # must not cover a stamped share whose commit is still in
            # flight: the frozen anchor stays reproducible from the
            # durable ledger for every rebuild of this generation. The
            # pending-commit clamp is pure anchor selection -- it freezes
            # the highest anchor whose covered shares are all durable, and
            # shares stamped above it deterministically belong to the next
            # window -- so nothing else needs to be captured with it.
            candidate_anchor_ms = runtime._job_snapshot_anchor_ms(self._now_ms())
            with self._job_cache_lock:
                issued_at_ms = self._job_build_issued_at_ms.get(
                    artifacts.generation
                )
                if issued_at_ms is None:
                    issued_at_ms = candidate_anchor_ms
                    self._job_build_issued_at_ms[artifacts.generation] = (
                        issued_at_ms
                    )
                    while len(self._job_build_issued_at_ms) > 128:
                        self._job_build_issued_at_ms.popitem(last=False)
        build_append_invalidation_epoch = (
            int(payout_ledger_artifact.append_invalidation_epoch)
            if mode == "ready" and payout_ledger_artifact is not None
            else current_append_invalidation_epoch
            if mode == "ready"
            else 0
        )
        build_key = JobBuildKey(
            best_tip_hash=artifacts.previousblockhash,
            previous_block_hash=artifacts.previousblockhash,
            template_fingerprint=artifacts.fingerprint,
            template_generation=artifacts.generation,
            payout_state_generation=payout_state_generation,
            payout_artifact_sha256=payout_artifact.prior_balances_sha256,
            mode=mode,
            collection_identity=collection_identity,
            block_height=int(artifacts.template["height"]),
            coinbase_value_sats=int(artifacts.template["coinbasevalue"]),
            network_difficulty=int(artifacts.network_difficulty),
            issued_at_ms=issued_at_ms,
            payout_policy_sha256=hashlib.sha256(
                payout_policy_json.encode()
            ).hexdigest(),
            ctv_settlement_sha256=(
                hashlib.sha256(ctv_settlement_json.encode()).hexdigest()
                if ctv_settlement_json is not None
                else None
            ),
            witness_merkle_sha256=self._canonical_json_sha256(
                artifacts.witness_merkle_leaves_hex
            ),
            transaction_set_sha256=self._canonical_json_sha256(
                artifacts.transaction_hexes
            ),
            coinbase_suffix_hex=suffix_hex,
            signing_key_sha256=hashlib.sha256(
                str(getattr(runtime, "signing_seed_hex", "")).encode()
            ).hexdigest(),
            ledger_signing_key_sha256=hashlib.sha256(
                str(
                    getattr(
                        runtime,
                        "ledger_attestation_signing_seed_hex",
                        "",
                    )
                ).encode()
            ).hexdigest(),
            numeric_context_sha256=numeric_context_sha256,
            payout_append_invalidation_epoch=(
                build_append_invalidation_epoch
            ),
        )
        immutable_identity: tuple[object, ...] = (
            cache_key,
            artifacts.generation,
            issued_at_ms,
            payout_artifact.prior_balances_sha256,
            build_key.payout_policy_sha256,
            build_key.ctv_settlement_sha256,
            build_key.witness_merkle_sha256,
            build_key.transaction_set_sha256,
            build_key.coinbase_suffix_hex,
            build_key.signing_key_sha256,
            build_key.ledger_signing_key_sha256,
            build_key.numeric_context_sha256,
            build_key.payout_append_invalidation_epoch,
        )
        return JobBuildRequest(
            key=build_key,
            cache_key=cache_key,
            equivalence_key=immutable_identity,
            artifacts=artifacts,
            template_json=canonical_json_text(artifacts.template),
            transaction_hexes=artifacts.transaction_hexes,
            witness_merkle_leaves_hex=artifacts.witness_merkle_leaves_hex,
            worker=worker,
            mode=mode,
            payout_artifact=payout_artifact,
            payout_ledger_artifact=payout_ledger_artifact,
            payout_policy_json=payout_policy_json,
            ctv_settlement_json=ctv_settlement_json,
            decimal_context=decimal_context,
            cancellation=cancellation,
            idle_retarget=idle_retarget,
            publication_critical=publication_critical,
            request_source=request_source,
            requested_monotonic=(
                cancellation.started_monotonic
                if priority_requested_monotonic is None
                else priority_requested_monotonic
            ),
        )

    def _cache_job_bundle_if_current(
        self,
        built: CachedJobBundle,
        artifacts: CachedTemplateArtifacts,
    ) -> bool:
        """Cache only current state; report whether payout state stayed valid."""
        runtime = self._runtime
        with self._job_cache_lock:
            with runtime.lock:
                buildable = runtime._artifacts_buildable_locked(artifacts)
                snapshot_pinned = runtime._published_snapshot_artifacts_locked(
                    artifacts
                )
            published_artifact = runtime._published_payout_state.artifact
            if not buildable:
                return False
            if (
                built.payout_state_generation != runtime._payout_state_generation
                or built.build_key is None
                or (
                    not built.collection_only
                    and built.build_key.payout_append_invalidation_epoch
                    != runtime._payout_ledger_append_invalidation_epoch
                )
                or published_artifact is None
                or built.build_key.payout_artifact_sha256
                != published_artifact.prior_balances_sha256
            ):
                return False
            current = runtime._template_artifacts
            globally_current = (
                current is not None
                and current.fingerprint == artifacts.fingerprint
                and current.previousblockhash == artifacts.previousblockhash
                and (
                    not built.collection_only
                    or current.generation == artifacts.generation
                )
            )
            if not globally_current and not snapshot_pinned:
                # Only the current template observation may win the cache
                # race; the sole exception is a pinned rebuild of exactly the
                # published snapshot, which repeated direct issuance would
                # otherwise rebuild for the rest of the unpublished window.
                return False
            self._job_bundle_cache[built.key] = built
            self._job_bundle_cache.move_to_end(built.key)
            while len(self._job_bundle_cache) > MAX_PRISM_JOB_BUNDLE_CACHE_ENTRIES:
                oldest_key = next(iter(self._job_bundle_cache))
                self._job_bundle_cache.pop(oldest_key, None)
        # Arm the build's fenced ledger window only after the bundle itself
        # won cache publication: lookups re-keyed by the fresh artifact fall
        # back to exactly this cached no-artifact entry instead of forking a
        # redundant build.
        if built.prepared_ledger_artifact is not None:
            runtime._install_payout_ledger_artifact(built.prepared_ledger_artifact)
        return True

    def _probe_initial_job_bundle(
        self,
        artifacts: CachedTemplateArtifacts,
        worker: WorkerIdentityPort | None,
        mode: str | None,
    ) -> CachedJobBundle | None:
        """Serve an initial request straight from the bundle cache.

        Routine admission exists to keep CPU-heavy request construction from
        competing with a publication-critical build; a cache probe does none
        of that work. Probing before admission lets a first job ship the
        already-published bundle immediately instead of waiting out an entire
        priority build it would never have contended with.
        """
        runtime = self._runtime
        resolved_mode = runtime._job_bundle_mode(mode)
        if resolved_mode == "collection" and worker is None:
            raise CollectionIdentityUnavailable(
                "collection-mode worker identity is temporarily unavailable"
            )
        with self._job_cache_lock:
            payout_state_generation = runtime._payout_state_generation
        payout_artifact = (
            runtime._usable_payout_ledger_artifact(
                payout_state_generation,
                artifacts.network_difficulty,
            )
            if resolved_mode == "ready"
            else None
        )
        key = runtime._job_bundle_key(
            artifacts,
            mode=resolved_mode,
            payout_state_generation=payout_state_generation,
            payout_artifact_generation=(
                payout_artifact.generation if payout_artifact is not None else 0
            ),
            worker=worker,
        )
        cached = runtime._lookup_job_bundle(key)
        if cached is None and payout_artifact is not None:
            fallback = runtime._lookup_job_bundle(
                runtime._no_artifact_job_bundle_key(
                    artifacts,
                    mode=resolved_mode,
                    payout_state_generation=payout_state_generation,
                    worker=worker,
                )
            )
            if fallback is not None and runtime._no_artifact_bundle_matches_artifact(
                fallback,
                payout_artifact,
            ):
                cached = fallback
        if not runtime._job_bundle_entry_usable(cached, artifacts):
            return None
        runtime._record_job_cache_event("bundle", hit=True)
        with self._job_build_scheduler_lock:
            self.initial_job_prepared_work_counts["cache_hit"] += 1
        assert cached is not None
        return runtime._bind_cached_bundle_to_artifacts(cached, artifacts)

    def shared_job_bundle(
        self,
        artifacts: CachedTemplateArtifacts,
        worker: WorkerIdentityPort | None = None,
        *,
        mode: str | None = None,
        cancelled: Callable[[], bool] | None = None,
        retry_superseded: bool = True,
        idle_retarget: bool = False,
        publication_critical: bool = False,
        request_source: str = "routine",
        priority_requested_monotonic: float | None = None,
    ) -> CachedJobBundle:
        """Return one immutable heavy build through a work-identity flight.

        Equivalent callers share one future. Publication-critical refreshes
        can cancel routine work and start in the bounded replacement slot;
        reconnect, Vardiff, authorization, and same-tip work must defer while
        that priority work is active. Only the latest pending request is
        retained during repeated supersession.
        """
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        runtime._ensure_tip_refresh_state()
        priority_token: int | None = None
        if publication_critical:
            (
                priority_token,
                priority_requested_monotonic,
            ) = runtime._begin_job_build_priority_preparation(
                priority_requested_monotonic
            )
        try:
            return runtime._shared_job_bundle_after_priority_admission(
                artifacts,
                worker,
                mode=mode,
                cancelled=cancelled,
                retry_superseded=retry_superseded,
                idle_retarget=idle_retarget,
                publication_critical=publication_critical,
                request_source=request_source,
                priority_requested_monotonic=priority_requested_monotonic,
            )
        finally:
            if priority_token is not None:
                runtime._finish_job_build_priority_preparation(priority_token)

    def _shared_job_bundle_after_priority_admission(
        self,
        artifacts: CachedTemplateArtifacts,
        worker: WorkerIdentityPort | None = None,
        *,
        mode: str | None = None,
        cancelled: Callable[[], bool] | None = None,
        retry_superseded: bool = True,
        idle_retarget: bool = False,
        publication_critical: bool = False,
        request_source: str = "routine",
        priority_requested_monotonic: float | None = None,
    ) -> CachedJobBundle:
        runtime = self._runtime
        while True:
            routine_preparation_token: int | None = None
            preparation_cancellation: JobBuildCancellation | None = None
            subscribed_bundle: CachedJobBundle | None = None
            if not publication_critical:
                if request_source == "initial":
                    probed = runtime._probe_initial_job_bundle(
                        artifacts,
                        worker,
                        mode,
                    )
                    if probed is not None:
                        return probed
                (
                    routine_preparation_token,
                    preparation_cancellation,
                    subscribed_bundle,
                ) = runtime._begin_routine_job_build_preparation(
                    request_source=request_source,
                    cancelled=cancelled,
                )
            resolved_mode = runtime._job_bundle_mode(mode)
            if preparation_cancellation is not None:
                preparation_cancellation.raise_if_cancelled(
                    "request preparation admission"
                )
            if resolved_mode == "collection" and worker is None:
                raise CollectionIdentityUnavailable(
                    "collection-mode worker identity is temporarily unavailable"
                )
            with self._job_cache_lock:
                payout_state_generation = runtime._payout_state_generation
            payout_artifact = (
                runtime._usable_payout_ledger_artifact(
                    payout_state_generation,
                    artifacts.network_difficulty,
                )
                if resolved_mode == "ready"
                else None
            )
            if preparation_cancellation is not None:
                preparation_cancellation.raise_if_cancelled(
                    "payout artifact lookup"
                )
            payout_artifact_generation = (
                payout_artifact.generation if payout_artifact is not None else 0
            )
            key = runtime._job_bundle_key(
                artifacts,
                mode=resolved_mode,
                payout_state_generation=payout_state_generation,
                payout_artifact_generation=payout_artifact_generation,
                worker=worker,
            )
            no_artifact_key = (
                runtime._no_artifact_job_bundle_key(
                    artifacts,
                    mode=resolved_mode,
                    payout_state_generation=payout_state_generation,
                    worker=worker,
                )
                if payout_artifact_generation != 0
                else None
            )
            cached = runtime._lookup_job_bundle(key)
            if (
                cached is None
                and subscribed_bundle is not None
                and (
                    subscribed_bundle.key == key
                    or (
                        no_artifact_key is not None
                        and subscribed_bundle.key == no_artifact_key
                        and runtime._no_artifact_bundle_matches_artifact(
                            subscribed_bundle,
                            payout_artifact,
                        )
                    )
                )
            ):
                # The subscribed priority result may not have won the global
                # cache-publication race yet; consume it directly. Usability
                # below applies exactly the checks a cache entry would face.
                cached = subscribed_bundle
            if cached is None and no_artifact_key is not None:
                fallback = runtime._lookup_job_bundle(no_artifact_key)
                if (
                    fallback is not None
                    and runtime._no_artifact_bundle_matches_artifact(
                        fallback,
                        payout_artifact,
                    )
                ):
                    cached = fallback
            if runtime._job_bundle_entry_usable(cached, artifacts):
                if preparation_cancellation is not None:
                    preparation_cancellation.raise_if_cancelled(
                        "bundle cache lookup"
                    )
                if routine_preparation_token is not None:
                    runtime._finish_routine_job_build_preparation(
                        routine_preparation_token
                    )
                runtime._record_job_cache_event("bundle", hit=True)
                if request_source == "initial":
                    with self._job_build_scheduler_lock:
                        self.initial_job_prepared_work_counts["cache_hit"] += 1
                assert cached is not None
                return runtime._bind_cached_bundle_to_artifacts(cached, artifacts)
            if runtime._job_bundle_mode(mode) != resolved_mode:
                if routine_preparation_token is not None:
                    runtime._finish_routine_job_build_preparation(
                        routine_preparation_token
                    )
                continue
            runtime._record_job_cache_event("bundle", hit=False)
            try:
                request = runtime._new_job_build_request(
                    artifacts,
                    worker,
                    mode=resolved_mode,
                    payout_state_generation=payout_state_generation,
                    cache_key=key,
                    payout_ledger_artifact=payout_artifact,
                    idle_retarget=idle_retarget,
                    publication_critical=publication_critical,
                    request_source=request_source,
                    priority_requested_monotonic=(
                        priority_requested_monotonic
                    ),
                    preparation_cancellation=preparation_cancellation,
                )
                # Preserve the historical readiness handoff without holding a
                # lock across construction: only admission and the final mode
                # re-selection are serialized here.
                with self._job_build_lock:
                    with self._job_build_scheduler_lock:
                        if routine_preparation_token is not None:
                            runtime._finish_routine_job_build_preparation(
                                routine_preparation_token
                            )
                            routine_preparation_token = None
                        request.cancellation.raise_if_cancelled(
                            "scheduler admission"
                        )
                        if runtime._job_bundle_mode(mode) != resolved_mode:
                            request.cancellation.cancel(
                                "worker mode superseded"
                            )
                            continue
                        if cancelled is not None and cancelled():
                            raise JobBuildWaiterCancelled(
                                "job bundle request was cancelled before preparation"
                            )
                        promise = runtime._request_job_build(request)
                wait_deadline = time.monotonic() + max(
                    0.001,
                    float(runtime.job_build_timeout_seconds)
                    + float(runtime.job_build_cancel_grace_seconds)
                    + 1.0,
                )
                while True:
                    if cancelled is not None and cancelled():
                        raise JobBuildWaiterCancelled(
                            "job bundle waiter was cancelled during preparation"
                        )
                    try:
                        built = promise.result(
                            timeout=min(
                                0.1,
                                max(0.001, wait_deadline - time.monotonic()),
                            )
                        )
                        break
                    except TimeoutError:
                        if time.monotonic() >= wait_deadline:
                            raise
            except TimeoutError as exc:
                request.cancellation.cancel("timeout")
                runtime._schedule_tip_refresh_retry()
                raise JobBuildCancelled(
                    "job build timed out; immediate retry scheduled"
                ) from exc
            except JobBuildCancelled:
                if routine_preparation_token is not None:
                    runtime._finish_routine_job_build_preparation(
                        routine_preparation_token
                    )
                runtime._schedule_tip_refresh_retry()
                if not retry_superseded:
                    raise
                with runtime.lock:
                    buildable = runtime._artifacts_buildable_locked(artifacts)
                if not buildable:
                    raise
                with self._job_cache_lock:
                    current = runtime._template_artifacts
                    payout_current = (
                        payout_state_generation == runtime._payout_state_generation
                    )
                if current is artifacts and payout_current:
                    continue
                if current is artifacts:
                    continue
                raise
            built = runtime._bind_cached_bundle_to_artifacts(built, artifacts)
            if not runtime._cache_job_bundle_if_current(built, artifacts):
                with self._job_build_scheduler_lock:
                    self.job_build_scheduler_counts["obsolete_results"] += 1
                with runtime.lock:
                    buildable = runtime._artifacts_buildable_locked(artifacts)
                if not buildable:
                    with runtime._tip_refresh_metrics_lock:
                        runtime.tip_refresh_superseded_results += 1
                    raise JobBuildSuperseded(
                        "observed tip changed before cache publication"
                    )
                with self._job_cache_lock:
                    payout_current = (
                        built.payout_state_generation
                        == runtime._payout_state_generation
                        and (
                            built.collection_only
                            or (
                                built.build_key is not None
                                and built.build_key.payout_append_invalidation_epoch
                                == runtime._payout_ledger_append_invalidation_epoch
                            )
                        )
                    )
                with runtime.lock:
                    published_snapshot = getattr(
                        runtime,
                        "tip_template_snapshot",
                        None,
                    )
                    published_artifacts = (
                        published_snapshot.template_artifacts
                        if published_snapshot is not None
                        else None
                    )
                if (
                    payout_current
                    and built.template is artifacts.template
                    and built.template_generation == artifacts.generation
                    and (
                        not retry_superseded
                        or published_artifacts is artifacts
                    )
                ):
                    # Snapshot-owned work may outlive an unrelated global
                    # cache fill. Return it only to its refresh validator or
                    # retained collection delivery; never retain it globally.
                    if built.prepared_ledger_artifact is not None:
                        runtime._install_payout_ledger_artifact(
                            built.prepared_ledger_artifact
                        )
                    return built
                if retry_superseded:
                    with self._job_cache_lock:
                        current = runtime._template_artifacts
                    if current is artifacts:
                        continue
                raise JobBuildSuperseded(
                    "job build key changed before cache publication"
                )
            return built

    def _share_window_serialization_for_artifact(
        self,
        payout_artifact: Any,
        shares: list[dict[str, object]],
    ) -> _ShareWindowSerialization:
        """Cached digest and builder fragments for the artifact's share window.

        Single-slot cache keyed by the canonical content digest, share count,
        and window weight. A debounced payout generation can retag an
        unchanged window, so generation identity would unnecessarily rebuild
        and re-spool the same multi-megabyte compact payload. The compute
        deliberately takes no cancellation checkpoints -- the result outlives
        any one requester and is served to whichever build wins next.
        """
        self._runtime._ensure_job_cache_state()
        window_weight = (
            PRISM_REWARD_WINDOW_MULTIPLIER
            * PRISM_SNAPSHOT_WINDOW_MARGIN
            * int(payout_artifact.network_difficulty)
        )
        # The digest is computed under the slot lock on purpose: the two
        # bounded build flights can request the same window concurrently, and
        # letting both walk the share tree would duplicate exactly the
        # GIL-heavy pass this cache removes. The loser waits briefly and
        # reuses the winner's entry; a different-key waiter is serialized
        # behind at most one O(window) pass.
        with self._share_window_serialization_lock:
            cached = self._share_window_serialization
            if (
                payout_artifact.share_snapshot_sha256 is None
                and cached is not None
                and cached._source_artifact is payout_artifact
                and cached.share_count == len(shares)
                and cached.key[2] == window_weight
            ):
                return cached
            share_snapshot_sha256 = (
                payout_artifact.share_snapshot_sha256
                or self._canonical_json_sha256(shares)
            )
            key = (share_snapshot_sha256, len(shares), window_weight)
            if (
                cached is not None
                and cached.key == key
                and cached.share_count == len(shares)
            ):
                cached._source_artifact = payout_artifact
                return cached
            serialization = _ShareWindowSerialization(
                key=key,
                share_count=len(shares),
                share_snapshot_sha256=share_snapshot_sha256,
                _source_artifact=payout_artifact,
                _spool_factory=self._spool_factory,
            )
            self._share_window_serialization = serialization
        if cached is not None:
            # Generation rotation retires the replaced spool; a build still
            # holding a lease keeps the descriptor alive until it releases.
            cached.retire_spool()
        return serialization

    def retire_share_window_spool(self) -> None:
        """Release the cached share-window spool during shutdown.

        Runs after the build executors quiesce, so any lease still held by a
        draining transfer defers the close to its own release.
        """
        self._runtime._ensure_job_cache_state()
        with self._share_window_serialization_lock:
            serialization = self._share_window_serialization
        if serialization is not None:
            serialization.retire_spool()

    def build_shared_job_bundle(
        self,
        artifacts: CachedTemplateArtifacts,
        worker: WorkerIdentityPort | None = None,
        *,
        mode: str | None = None,
        payout_state_generation: int | None = None,
        payout_artifact: Any = None,
        key: tuple[object, ...] | None = None,
        build_request: JobBuildRequest | None = None,
    ) -> CachedJobBundle:
        runtime = self._runtime
        phases = runtime._job_build_phases()
        resolved_mode = runtime._job_bundle_mode(mode)
        if resolved_mode == "collection" and worker is None:
            raise CollectionIdentityUnavailable(
                "collection-mode worker identity is temporarily unavailable"
            )
        with self._job_cache_lock:
            publication_blocked = runtime._payout_state_publication_blocked
            if payout_state_generation is None:
                payout_state_generation = runtime._payout_state_generation
        if publication_blocked:
            raise PayoutStatePublicationBlocked(
                "payout state invalidation is pending publication"
            )
        if key is None:
            key = runtime._job_bundle_key(
                artifacts,
                mode=resolved_mode,
                payout_state_generation=payout_state_generation,
                payout_artifact_generation=(
                    payout_artifact.generation
                    if payout_artifact is not None
                    else 0
                ),
                worker=worker,
            )
        if build_request is None:
            build_request = runtime._new_job_build_request(
                artifacts,
                worker,
                mode=resolved_mode,
                payout_state_generation=payout_state_generation,
                cache_key=key,
                payout_ledger_artifact=payout_artifact,
            )
        else:
            payout_artifact = build_request.payout_ledger_artifact
        cancellation = build_request.cancellation
        runtime._job_build_checkpoint("ledger_snapshot", cancellation)
        template_value = json.loads(build_request.template_json)
        if not isinstance(template_value, dict):
            raise RuntimeError("immutable job template is not an object")
        template: dict[str, Any] = template_value
        issued_at_ms = build_request.key.issued_at_ms
        started = time.monotonic()
        snapshot_window_weight = (
            PRISM_REWARD_WINDOW_MULTIPLIER
            * PRISM_SNAPSHOT_WINDOW_MARGIN
            * int(build_request.key.network_difficulty)
        )
        prepared_ledger_artifact: Any = None
        snapshot_accepted_count: int | None = None
        inflight_scan_anchor_token: int | None = None
        if payout_artifact is not None:
            # The reuse decision was made at request preparation. Nothing
            # that happens to the ARMED slot afterwards -- a fresher window
            # installing (generation re-key), a same-window anchor refresh
            # swapping the instance, or the artifact simply aging out of new
            # reuse decisions -- invalidates the copy this build already
            # selected: its window stays audit-reproducible at its declared
            # anchor. Scrapping admitted work on any of those (as the first
            # anchor-scoped deploy did on the generation re-key) turns
            # routine churn into a rebuild storm. Only two fences gate
            # completion: the payout generation and the published balances
            # this artifact binds.
            with self._job_cache_lock:
                current_payout_state_generation = runtime._payout_state_generation
                published_artifact = runtime._published_payout_state.artifact
                current_append_invalidation_epoch = (
                    runtime._payout_ledger_append_invalidation_epoch
                )
            if current_payout_state_generation != payout_state_generation:
                raise JobBuildSuperseded(
                    "payout generation changed before construction"
                )
            if (
                payout_artifact.append_invalidation_epoch
                != current_append_invalidation_epoch
            ):
                raise JobBuildSuperseded(
                    "payout window invalidated before construction"
                )
            if published_artifact is None:
                try:
                    published_artifact = runtime._current_payout_state_artifact()
                except Exception as exc:
                    raise JobBuildSuperseded(
                        "published payout state unavailable before construction"
                    ) from exc
            artifact_balances_sha256 = (
                payout_artifact.prior_balances_sha256
                or self._canonical_json_sha256(payout_artifact.prior_balances)
            )
            if (
                artifact_balances_sha256
                != published_artifact.prior_balances_sha256
            ):
                raise JobBuildSuperseded(
                    "precomputed payout artifact changed before construction"
                )
            prior_balances = list(payout_artifact.prior_balances)
            if (
                artifact_balances_sha256
                != build_request.key.payout_artifact_sha256
            ):
                raise JobBuildSuperseded(
                    "precomputed payout artifact does not match payout generation"
                )
            shares = list(payout_artifact.shares_json)
            # The artifact binds the key above; the balances actually used may
            # still be an accepted parent's prospective carry state.
            prior_balances = runtime._prior_balances_for_job_parent(
                str(template["previousblockhash"]),
                parent_height=int(template["height"]) - 1,
                fallback_balances=prior_balances,
            )
        else:
            with runtime._payout_state_prepare_lock:
                with self._job_cache_lock:
                    published_artifact = runtime._published_payout_state.artifact
                    if runtime._payout_state_publication_blocked:
                        raise PayoutStatePublicationBlocked(
                            "payout state invalidation is pending publication"
                        )
                    if (
                        payout_state_generation != runtime._payout_state_generation
                        or build_request.key.payout_append_invalidation_epoch
                        != runtime._payout_ledger_append_invalidation_epoch
                        or published_artifact is None
                        or published_artifact.prior_balances_sha256
                        != build_request.key.payout_artifact_sha256
                    ):
                        raise JobBuildSuperseded(
                            "payout generation changed before ledger snapshot"
                        )
                if resolved_mode == "ready":
                    try:
                        accepted_now, _ = runtime.accepted_share_stats()
                    except Exception:
                        # Artifact seeding is speculative; snapshot errors
                        # stay owned by the bundle build itself. Without an
                        # observed count the seed below is skipped rather
                        # than armed with a fabricated one.
                        accepted_now = None
                    if accepted_now is not None:
                        # Informational only: the window is scoped by the
                        # frozen anchor (shares committing during the read
                        # land above it and belong to the next window), so
                        # no bracket around the read is needed.
                        snapshot_accepted_count = int(accepted_now)
                if resolved_mode == "ready":
                    # Publish the read's frozen anchor: a late-visible
                    # append committing mid-read predates it without
                    # holding the pending floor, and this read's database
                    # snapshot may already exclude the row. The recorded
                    # invalidation epoch keeps the resulting bundle and
                    # its seeded artifact from arming or serving. The
                    # exposure must outlive this read: nothing else exposes
                    # this window's anchor until the seeded artifact passes
                    # its install fence, so an append committing during
                    # bundle construction or cache publication would
                    # otherwise advance no epoch and the seed would arm a
                    # window that delta reads can never rediscover the row
                    # from. The token therefore rides the seeded artifact
                    # (retired by its install fence); a build that raises,
                    # or seeds nothing, retires it on the spot.
                    inflight_scan_anchor_token = (
                        runtime._expose_inflight_scan_anchor(issued_at_ms)
                    )
                    # Mirrored onto the request so the executor wrapper can
                    # retire the exposure when this build dies anywhere past
                    # the read (checkpoint cancellation, manifest assembly,
                    # the publication fence).
                    build_request.inflight_scan_anchor_token = (
                        inflight_scan_anchor_token
                    )
                    try:
                        records = runtime.ledger.snapshot_at_job_issue(
                            issued_at_ms,
                            window_weight=snapshot_window_weight,
                        )
                    except BaseException:
                        runtime._retire_inflight_scan_anchor(
                            inflight_scan_anchor_token
                        )
                        raise
                else:
                    records = []
                # An accepted parent's prospective carry state supersedes the
                # published artifact for children built on that parent; the
                # published balances remain the fallback for ordinary tips.
                prior_balances = runtime._prior_balances_for_job_parent(
                    str(template["previousblockhash"]),
                    parent_height=int(template["height"]) - 1,
                    fallback_balances=build_request.payout_artifact.prior_balances(),
                )
            runtime._job_build_checkpoint("ledger_snapshot_complete", cancellation)
            shares = []
            for index, record in enumerate(records):
                if index % 256 == 0:
                    runtime._job_build_checkpoint(
                        "ledger_snapshot_conversion",
                        cancellation,
                    )
                shares.append(record.to_prism_json())
        # A reused artifact carries shares snapshotted at its own earlier
        # anchor. The bundle must declare that anchor: replaying the audit
        # window at this job's fresher anchor could include a share that was
        # already durable at artifact build time but stamped after the
        # artifact's clamped anchor, and the artifact's share set excludes it
        # by construction.
        bundle_anchor_ms = (
            payout_artifact.snapshot_anchor_ms
            if payout_artifact is not None
            and payout_artifact.snapshot_anchor_ms is not None
            else issued_at_ms
        )
        ledger_elapsed = time.monotonic() - started
        phases["ledger"] = phases.get("ledger", 0.0) + ledger_elapsed
        if resolved_mode == "ready":
            runtime._observe_tip_refresh_build_phase(
                "ledger_snapshot",
                ledger_elapsed,
            )
        share_serialization: _ShareWindowSerialization | None = None
        if resolved_mode == "ready" and payout_artifact is not None:
            share_serialization = runtime._share_window_serialization_for_artifact(
                payout_artifact,
                shares,
            )
            share_snapshot_sha256 = share_serialization.share_snapshot_sha256
        else:
            share_snapshot_sha256 = self._canonical_json_sha256(shares)
        if (
            payout_artifact is None
            and snapshot_accepted_count is not None
            and shares
            and runtime._payout_artifact_reuse_active()
        ):
            # This build already paid the full ledger read; carry the window
            # it produced so cache publication can arm it for builds arriving
            # while its anchor stays inside the staleness bound. The artifact
            # carries the published payout-state balances, not this bundle's
            # possibly parent-adjusted view: reuse re-applies the parent
            # override itself and the reuse fence hashes the artifact
            # balances against the published payout artifact. Skipped
            # entirely under the kill-switch: nothing may arm while reuse
            # is disabled.
            assert self._payout_ledger_artifact_type is not None, (
                "job bundle service requires the payout-ledger artifact type "
                "to seed reusable windows"
            )
            seed_prior_balances = tuple(
                build_request.payout_artifact.prior_balances()
            )
            prepared_ledger_artifact = self._payout_ledger_artifact_type(
                generation=0,
                payout_state_generation=payout_state_generation,
                network_difficulty=int(build_request.key.network_difficulty),
                accepted_share_count=snapshot_accepted_count,
                shares_json=tuple(shares),
                prior_balances=seed_prior_balances,
                prepared_monotonic=time.monotonic(),
                snapshot_anchor_ms=issued_at_ms,
                share_snapshot_sha256=share_snapshot_sha256,
                prior_balances_sha256=self._canonical_json_sha256(
                    seed_prior_balances
                ),
                append_invalidation_epoch=(
                    build_request.key.payout_append_invalidation_epoch
                ),
                inflight_scan_anchor_token=inflight_scan_anchor_token,
            )
        final_build_key = dataclass_replace(
            build_request.key,
            share_snapshot_sha256=share_snapshot_sha256,
        )
        runtime._job_build_checkpoint("payout_derivation", cancellation)
        started = time.monotonic()
        placeholder_suffix_hex = final_build_key.coinbase_suffix_hex
        collection_identity: tuple[str, str] | None = None
        previous_metrics_scope = bool(
            getattr(self._job_build_phase_local, "tip_refresh_metrics", False)
        )
        self._job_build_phase_local.tip_refresh_metrics = resolved_mode == "ready"
        try:
            if resolved_mode == "ready":
                if not shares:
                    raise RuntimeError(
                        "ready-pool ledger snapshot contained no payout shares"
                    )
                runtime._job_build_checkpoint("ctv_manifest", cancellation)
                runtime._job_build_checkpoint("signing_verification", cancellation)
                bundle = runtime.build_audit_bundle(
                    shares=shares,
                    found_block={
                        "block_height": int(template["height"]),
                        "coinbase_value_sats": int(template["coinbasevalue"]),
                        "network_difficulty": artifacts.network_difficulty,
                        "anchor_job_issued_at_ms": bundle_anchor_ms,
                    },
                    prior_balances=prior_balances,
                    coinbase_script_sig_suffix_hex=placeholder_suffix_hex,
                    witness_merkle_leaves_hex=list(
                        build_request.witness_merkle_leaves_hex
                    ),
                    ctv_fee_parent_hash=str(template["previousblockhash"]),
                    summary_only=True,
                    payout_policy=json.loads(build_request.payout_policy_json),
                    ctv_settlement=(
                        json.loads(build_request.ctv_settlement_json)
                        if build_request.ctv_settlement_json is not None
                        else None
                    ),
                    cancellation=cancellation,
                    share_serialization=share_serialization,
                )
                collection_only = False
            else:
                assert worker is not None
                runtime._job_build_checkpoint("ctv_manifest", cancellation)
                runtime._job_build_checkpoint("signing_verification", cancellation)
                bundle = runtime.build_collection_bundle(
                    template=template,
                    transaction_hexes=build_request.transaction_hexes,
                    worker=worker,
                    network_difficulty=final_build_key.network_difficulty,
                    issued_at_ms=issued_at_ms,
                    suffix_hex=placeholder_suffix_hex,
                    summary_only=True,
                    payout_policy=json.loads(build_request.payout_policy_json),
                    ctv_settlement=(
                        json.loads(build_request.ctv_settlement_json)
                        if build_request.ctv_settlement_json is not None
                        else None
                    ),
                    cancellation=cancellation,
                )
                shares = []
                collection_only = True
                collection_identity = runtime._collection_bundle_identity(worker)
        finally:
            self._job_build_phase_local.tip_refresh_metrics = previous_metrics_scope
        manifest = bundle["signed_coinbase_manifest"]["manifest"]
        prospective_prior_balances: (
            tuple[tuple[str, str, str, int], ...] | None
        ) = None
        payout_policy_manifest = bundle.get("payout_policy_manifest")
        if isinstance(payout_policy_manifest, dict) and isinstance(
            payout_policy_manifest.get("accounts"),
            list,
        ):
            prospective_prior_balances = runtime._serialize_prior_balance_preview(
                runtime._accepted_block_payout_preview_from_bundle(
                    bundle,
                    prior_balances=prior_balances,
                )
            )
        runtime._job_build_checkpoint("bundle_assembly", cancellation)
        assembly_started = time.monotonic()
        base_job = direct_stratum.make_job_from_builder_manifest(
            job_id="prism-template-base",
            template=template,
            manifest=manifest,
            extranonce1_hex=PRISM_JOB_EXTRANONCE1_PLACEHOLDER_HEX,
            extranonce2_size=runtime.extranonce2_size,
            desired_share_difficulty=runtime.share_difficulty,
            clean_jobs=True,
            transaction_hexes=build_request.transaction_hexes,
        )
        phases["assembly"] = phases.get("assembly", 0.0) + (
            time.monotonic() - assembly_started
        )
        runtime._job_build_checkpoint("serialization", cancellation)
        runtime._job_build_checkpoint("bundle_publication", cancellation)
        if resolved_mode == "ready":
            with self._job_cache_lock:
                if (
                    final_build_key.payout_append_invalidation_epoch
                    != runtime._payout_ledger_append_invalidation_epoch
                ):
                    raise JobBuildSuperseded(
                        "payout window invalidated before bundle publication"
                    )
                if (
                    inflight_scan_anchor_token is not None
                    and prepared_ledger_artifact is None
                ):
                    # The fence above just proved, under this same lock hold,
                    # that no append predated this build's window. Seeded
                    # builds keep their exposure until the artifact's
                    # install fence settles; with no seed, nothing else
                    # exposes this window's anchor once the walk exposure
                    # retires, yet jobs stamped from this bundle keep
                    # serving the window until they retire. Hand the anchor
                    # to the published-window watermark under this same lock
                    # hold, so a replay-shaped append committing after this
                    # return still predates a live anchor and advances the
                    # epoch those jobs' landing fences check.
                    runtime._publish_seedless_job_window_anchor_locked(
                        issued_at_ms
                    )
                    runtime._payout_window_inflight_scan_anchors.pop(
                        inflight_scan_anchor_token,
                        None,
                    )
                    inflight_scan_anchor_token = None
                    build_request.inflight_scan_anchor_token = None
        phases["bundle"] = phases.get("bundle", 0.0) + (time.monotonic() - started)
        return CachedJobBundle(
            key=key,
            # Construction consumed the detached canonical template above;
            # publication retains the exact snapshot-owned object identity used
            # by the existing validation token.
            template=artifacts.template,
            template_fingerprint=artifacts.fingerprint,
            # Only this manifest is needed to bind later clock-only template
            # observations.  Retaining the returned logical bundle duplicated
            # the entire shares tree already held in shares_json.
            coinbase_manifest=manifest,
            shares_json=shares,
            prior_balances=prior_balances,
            found_block=bundle["found_block"],
            collection_only=collection_only,
            issued_at_ms=issued_at_ms,
            base_job=base_job,
            built_monotonic=time.monotonic(),
            template_generation=artifacts.generation,
            payout_state_generation=payout_state_generation,
            payout_artifact_generation=(
                payout_artifact.generation if payout_artifact is not None else 0
            ),
            collection_identity=collection_identity,
            prospective_prior_balances=prospective_prior_balances,
            build_key=final_build_key,
            prepared_ledger_artifact=prepared_ledger_artifact,
        )

    def build_collection_bundle(
        self,
        *,
        template: dict[str, Any],
        transaction_hexes: tuple[str, ...],
        worker: WorkerIdentityPort,
        network_difficulty: int,
        issued_at_ms: int,
        suffix_hex: str,
        summary_only: bool = False,
        payout_policy: dict[str, object] | None = None,
        ctv_settlement: dict[str, object] | None = None,
        cancellation: JobBuildCancellation | None = None,
    ) -> dict[str, Any]:
        runtime = self._runtime
        if cancellation is not None:
            cancellation.raise_if_cancelled("collection payout derivation")
        share = {
            "share_seq": 1,
            "share_id": "bootstrap-share",
            "miner_id": worker.payout_address,
            "order_key": worker.payout_address,
            "p2mr_program_hex": worker.p2mr_program_hex,
            "share_difficulty": network_difficulty,
            "network_difficulty": network_difficulty,
            "template_height": int(template["height"]) - 1,
            "job_id": "bootstrap-job",
            "job_issued_at_ms": issued_at_ms,
            "accepted_at_ms": issued_at_ms,
            "ntime": int(template["curtime"]),
        }
        return runtime.build_audit_bundle(
            shares=[share],
            found_block={
                "block_height": int(template["height"]),
                "coinbase_value_sats": int(template["coinbasevalue"]),
                "network_difficulty": network_difficulty,
                "anchor_job_issued_at_ms": issued_at_ms,
            },
            prior_balances=[],
            coinbase_script_sig_suffix_hex=suffix_hex,
            witness_merkle_leaves_hex=direct_stratum.witness_merkle_leaves_hex(transaction_hexes),
            ctv_fee_parent_hash=str(template["previousblockhash"]),
            summary_only=summary_only,
            payout_policy=payout_policy,
            ctv_settlement=ctv_settlement,
            cancellation=cancellation,
        )


__all__ = [
    "CachedJobBundle",
    "CollectionIdentityUnavailable",
    "JobBuildAdmissionDeadlineExceeded",
    "JobBuildCancellation",
    "JobBuildCancelled",
    "JobBuildFlight",
    "JobBuildKey",
    "JobBuildRequest",
    "JobBuildSuperseded",
    "JobBuildWaiterCancelled",
    "JobBundleBuildControl",
    "JobBundleBuildSuperseded",
    "JobBundleRuntime",
    "JobBundleService",
    "MAX_PRISM_JOB_BUNDLE_CACHE_ENTRIES",
    "PRISM_INITIAL_JOB_SUBSCRIBE_POLL_SECONDS",
    "PRISM_JOB_BUILD_ORPHAN_SWEEP_GRACE_SECONDS",
    "PRISM_JOB_BUILD_PHASES",
    "PRISM_JOB_BUILD_SECONDS_BUCKETS",
    "PRISM_JOB_CACHE_KINDS",
    "PRISM_JOB_EXTRANONCE1_PLACEHOLDER_HEX",
    "PRISM_REWARD_WINDOW_MULTIPLIER",
    "PRISM_SNAPSHOT_WINDOW_MARGIN",
    "WorkerIdentityPort",
    "canonical_json_sha256",
    "canonical_json_text",
    "now_ms",
]
