"""Immutable PRISM job bundles, cache, and bounded latest-wins scheduler."""

from __future__ import annotations

from collections import OrderedDict
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field, replace as dataclass_replace
from decimal import Context, Decimal, getcontext, localcontext
import hashlib
import json
import subprocess
import threading
import time
import weakref
from typing import Any, Callable, ContextManager, Iterator, Protocol, Sequence

from lab.prism import direct_stratum
from lab.prism.payout_state import (
    PayoutLedgerArtifact,
    PayoutStateArtifact,
    PayoutStatePublicationBlocked,
    PayoutStateSnapshot,
    TemplateRefreshBlocked,
    TemplateRefreshSuperseded,
)
from lab.prism.template_artifacts import (
    CachedTemplateArtifacts,
    QbitTipTemplateSnapshot,
    TemplateArtifactRepository,
    freeze_json,
)


MAX_PRISM_JOB_BUNDLE_CACHE_ENTRIES = 128
PRISM_JOB_BUILD_EXECUTOR_WORKERS = 2
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


def canonical_json_text(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def canonical_json_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json_text(value).encode()).hexdigest()


class WorkerIdentityPort(Protocol):
    payout_address: str
    p2mr_program_hex: str


class PayoutStatePort(Protocol):
    @property
    def prepare_lock(self) -> threading.RLock: ...

    def cache_publication_admission(self) -> ContextManager[None]: ...

    def snapshot(self) -> PayoutStateSnapshot: ...

    def current_artifact(
        self,
        cancellation: object | None = None,
    ) -> PayoutStateArtifact: ...

    def usable_ledger_artifact(
        self,
        payout_state_generation: int,
        network_difficulty: int,
    ) -> PayoutLedgerArtifact | None: ...


@dataclass(frozen=True)
class JobBuildKey:
    """Every immutable input capable of changing one constructed job."""

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


@dataclass(frozen=True)
class CachedJobBundle:
    """One immutable heavy job build reusable across client stamping."""

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
    collection_identity: tuple[str, str] | None = None
    prospective_prior_balances: tuple[tuple[str, str, str, int], ...] | None = None
    build_key: JobBuildKey | None = None

    def __post_init__(self) -> None:
        for name in (
            "template",
            "coinbase_manifest",
            "shares_json",
            "prior_balances",
            "found_block",
        ):
            object.__setattr__(self, name, freeze_json(getattr(self, name)))


class JobBuildCancelled(TemplateRefreshBlocked):
    """An immutable build was cancelled or timed out."""


class JobBuildSuperseded(JobBuildCancelled, TemplateRefreshSuperseded):
    """A coordination race cooperatively cancelled construction."""


class JobBundleBuildSuperseded(JobBuildSuperseded):
    """A newer tip or payout generation canceled deterministic construction."""


class CollectionIdentityUnavailable(TemplateRefreshBlocked):
    """Collection work is waiting for an authorized worker identity."""


class JobBuildWaiterCancelled(RuntimeError):
    """A bundle waiter became obsolete before acquiring preparation."""


class JobBuildCancellation:
    def __init__(
        self,
        *,
        timeout_seconds: float,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._event = threading.Event()
        self._lock = threading.Lock()
        self._monotonic = monotonic
        self.started_monotonic = monotonic()
        self.deadline_monotonic = self.started_monotonic + timeout_seconds
        self.last_checkpoint_monotonic = self.started_monotonic
        self.cancelled_monotonic: float | None = None
        self.reason: str | None = None

    def cancel(self, reason: str) -> bool:
        with self._lock:
            if self._event.is_set():
                return False
            self.reason = reason
            self.cancelled_monotonic = self._monotonic()
            self._event.set()
            return True

    def is_set(self) -> bool:
        if self._event.is_set():
            return True
        if self._monotonic() >= self.deadline_monotonic:
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
            self.last_checkpoint_monotonic = self._monotonic()


@dataclass
class JobBundleBuildControl:
    key: tuple[object, ...]
    previousblockhash: str
    payout_state_generation: int
    payout_artifact_generation: int
    cancel_event: threading.Event = field(default_factory=threading.Event)
    process: subprocess.Popen[str] | None = None


@dataclass
class JobBuildRequest:
    key: JobBuildKey
    cache_key: tuple[object, ...]
    equivalence_key: tuple[object, ...]
    artifacts: CachedTemplateArtifacts
    template_json: str
    transaction_hexes: tuple[str, ...]
    witness_merkle_leaves_hex: tuple[str, ...]
    worker: WorkerIdentityPort | None
    mode: str
    payout_artifact: PayoutStateArtifact
    payout_ledger_artifact: PayoutLedgerArtifact | None
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


@dataclass(eq=False)
class JobBuildFlight:
    request: JobBuildRequest
    future: Future[CachedJobBundle] | None = None


@dataclass(frozen=True)
class JobBundleConfig:
    cache_seconds: float
    build_timeout_seconds: float
    cancel_grace_seconds: float
    min_ready_miners: int
    extranonce2_size: int
    share_difficulty: Decimal


class BundleCompilerPort(Protocol):
    def build_audit_bundle(self, **kwargs: object) -> dict[str, Any]: ...


@dataclass(frozen=True)
class JobBundlePorts:
    payout_state: Callable[[], PayoutStatePort]
    accepted_share_stats: Callable[[], tuple[int, int]]
    snapshot_at_job_issue: Callable[[int, int], Sequence[object]]
    snapshot_anchor_ms: Callable[[int], int]
    payout_policy: Callable[[], dict[str, object]]
    ctv_settlement: Callable[[int, str], dict[str, object] | None]
    coinbase_suffix: Callable[[str, str], str]
    signing_seed_hex: Callable[[], str]
    ledger_signing_seed_hex: Callable[[], str]
    await_parent_preview: Callable[[str, int], object]
    prior_balances_for_parent: Callable[
        [str, int, Sequence[dict[str, object]]], list[dict[str, object]]
    ]
    serialize_prior_balance_preview: Callable[
        [list[dict[str, object]]], tuple[tuple[str, str, str, int], ...]
    ]
    accepted_block_preview_from_bundle: Callable[
        [dict[str, Any], list[dict[str, object]]], list[dict[str, object]]
    ]
    schedule_refresh_retry: Callable[[], None]
    idle_tip_diverged: Callable[[], bool]
    artifacts_buildable: Callable[[CachedTemplateArtifacts], bool]
    published_snapshot_artifacts: Callable[[CachedTemplateArtifacts], bool]
    published_artifacts: Callable[[], CachedTemplateArtifacts | None]
    note_tip_refresh_superseded: Callable[[], None]
    record_tip_refresh_phase: Callable[[str, float], None]
    clear_retained_collection_refresh: Callable[[], None]
    readiness_promoted: Callable[[], None]
    start_bundle_build: Callable[[], ContextManager[object]]
    wall_time_ms: Callable[[], int]


class JobBundleService:
    """Sole owner of shared bundle state, scheduler, cache, and readiness."""

    def __init__(
        self,
        config: JobBundleConfig,
        ports: JobBundlePorts,
        template_repository: TemplateArtifactRepository,
        *,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._config = config
        self._ports = ports
        self.template_repository = template_repository
        self._monotonic = monotonic
        self._dependency_lock = threading.Lock()
        self._bundle_compiler: BundleCompilerPort | None = None
        self._cache_lock = threading.Lock()
        self._active_bundle_builds: dict[
            tuple[object, ...], JobBundleBuildControl
        ] = {}
        self._admission_lock = threading.Lock()
        self._scheduler_lock = threading.RLock()
        self._priority_preparations: dict[int, float] = {}
        self._priority_preparation_sequence = 0
        self._routine_preparations: dict[
            int,
            weakref.ReferenceType[JobBuildCancellation],
        ] = {}
        self._routine_preparation_sequence = 0
        self._priority_changed = threading.Event()
        self._executor: ThreadPoolExecutor | None = None
        self._executor_shutdown = False
        self._active: JobBuildFlight | None = None
        self._retiring: JobBuildFlight | None = None
        self._pending: JobBuildRequest | None = None
        self._issued_at_ms: OrderedDict[int, int] = OrderedDict()
        self._bundle_cache: OrderedDict[
            tuple[object, ...], CachedJobBundle
        ] = OrderedDict()
        self._phase_local = threading.local()
        self._readiness_lock = threading.Lock()
        self._ready_latched = False
        self._prepared_lock = threading.Lock()
        self._prepared_ready_bundle: CachedJobBundle | None = None
        self._prepared_ready_snapshot: QbitTipTemplateSnapshot | None = None
        self._preparation_pending = False
        self._failure_count = 0
        self._cache_hits = {kind: 0 for kind in PRISM_JOB_CACHE_KINDS}
        self._cache_misses = {kind: 0 for kind in PRISM_JOB_CACHE_KINDS}
        self._build_bucket_counts = {
            bucket: 0 for bucket in PRISM_JOB_BUILD_SECONDS_BUCKETS
        }
        self._build_seconds_sum = 0.0
        self._build_count = 0
        self._phase_seconds = {phase: 0.0 for phase in PRISM_JOB_BUILD_PHASES}
        self._scheduler_counts = {
            "requests": 0,
            "starts": 0,
            "completions": 0,
            "supersessions": 0,
            "obsolete_results": 0,
        }
        self._priority_counts = {
            result: 0
            for result in (
                "started",
                "coalesced",
                "queued",
                "routine_deferred",
                "routine_preempted",
            )
        }
        self._priority_admission_seconds = {"sum": 0.0, "count": 0}
        self._initial_prepared_work_counts = {
            result: 0 for result in ("cache_hit", "singleflight", "deferred")
        }
        self._cancellation_seconds = {"sum": 0.0, "count": 0}
        self._replacement_start_seconds = {"sum": 0.0, "count": 0}
        self._worker_counts = {
            "starts": 0,
            "terminations": 0,
            "crashes": 0,
            "restarts": 0,
        }
        self._worker_restart_pending = False
        self._shared_build_counts = {
            outcome: 0
            for outcome in ("started", "completed", "superseded", "failed")
        }
        self._preparation_seconds_sum = 0.0
        self._preparation_count = 0
        self._preparation_waiters = 0

    def bind_bundle_compiler(self, compiler: BundleCompilerPort) -> None:
        """Bind the compiler once after constructing both leaf services."""
        with self._dependency_lock:
            if self._bundle_compiler is not None:
                raise RuntimeError("job bundle compiler is already bound")
            self._bundle_compiler = compiler

    def bundle_compiler(self) -> BundleCompilerPort:
        with self._dependency_lock:
            compiler = self._bundle_compiler
        if compiler is None:
            raise RuntimeError("job bundle compiler is not bound")
        return compiler

    @staticmethod
    def collection_identity(worker: WorkerIdentityPort) -> tuple[str, str]:
        return worker.payout_address, worker.p2mr_program_hex

    def phases(self) -> dict[str, float]:
        phases = getattr(self._phase_local, "phases", None)
        if phases is None:
            phases = {}
            self._phase_local.phases = phases
        return phases

    def record_phase(self, phase: str, elapsed: float) -> None:
        phases = self.phases()
        phases[phase] = phases.get(phase, 0.0) + elapsed

    def observe_elapsed(self, elapsed_seconds: float, phases: dict[str, float]) -> None:
        with self._cache_lock:
            self._build_count += 1
            self._build_seconds_sum += elapsed_seconds
            for bucket in PRISM_JOB_BUILD_SECONDS_BUCKETS:
                if elapsed_seconds <= bucket:
                    self._build_bucket_counts[bucket] += 1
            for phase, duration in phases.items():
                if phase in self._phase_seconds:
                    self._phase_seconds[phase] += duration

    def record_cache_event(self, kind: str, *, hit: bool) -> None:
        with self._cache_lock:
            counts = self._cache_hits if hit else self._cache_misses
            counts[kind] = int(counts.get(kind, 0)) + 1

    def pool_readiness_latched(self) -> bool:
        with self._readiness_lock:
            if self._ready_latched:
                return True
        try:
            _, ready_miner_count = self._ports.accepted_share_stats()
        except Exception:
            return False
        if ready_miner_count < self._config.min_ready_miners:
            return False
        became_ready = False
        with self._readiness_lock:
            if not self._ready_latched:
                self._ready_latched = True
                became_ready = True
        if became_ready:
            self._ports.clear_retained_collection_refresh()
            self._ports.readiness_promoted()
        return True

    def job_bundle_mode(self, requested_mode: str | None) -> str:
        if requested_mode is not None:
            if requested_mode not in {"ready", "collection"}:
                raise ValueError(
                    f"unknown PRISM job-bundle mode: {requested_mode}"
                )
            return requested_mode
        return "ready" if self.pool_readiness_latched() else "collection"

    def job_bundle_key(
        self,
        artifacts: CachedTemplateArtifacts,
        *,
        mode: str,
        payout_state_generation: int,
        payout_artifact_generation: int = 0,
        worker: WorkerIdentityPort | None,
    ) -> tuple[object, ...]:
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
            *self.collection_identity(worker),
        )

    def lookup_bundle(self, key: tuple[object, ...]) -> CachedJobBundle | None:
        now = self._monotonic()
        with self._cache_lock:
            if self._config.cache_seconds <= 0:
                self._bundle_cache.clear()
                return None
            expired = [
                cache_key
                for cache_key, entry in self._bundle_cache.items()
                if now - entry.built_monotonic > self._config.cache_seconds
            ]
            for cache_key in expired:
                self._bundle_cache.pop(cache_key, None)
            return self._bundle_cache.get(key)

    def bundle_payout_state_current(self, bundle: CachedJobBundle) -> bool:
        payout = self._ports.payout_state().snapshot()
        artifact = payout.published.artifact
        return bool(
            bundle.payout_state_generation == payout.generation
            and bundle.build_key is not None
            and artifact is not None
            and bundle.build_key.payout_artifact_sha256
            == artifact.prior_balances_sha256
        )

    def bundle_entry_usable(
        self,
        cached: CachedJobBundle | None,
        artifacts: CachedTemplateArtifacts,
    ) -> bool:
        if cached is None or not self._ports.artifacts_buildable(artifacts):
            return False
        if self._ports.payout_state().snapshot().publication_blocked:
            return False
        if not self.bundle_payout_state_current(cached):
            return False
        if not cached.collection_only:
            return True
        if (
            cached.template is not artifacts.template
            or cached.template_generation != artifacts.generation
        ):
            return False
        try:
            _, ready_miner_count = self._ports.accepted_share_stats()
        except Exception:
            return False
        return ready_miner_count < self._config.min_ready_miners

    def bind_cached_bundle(
        self,
        cached: CachedJobBundle,
        artifacts: CachedTemplateArtifacts,
    ) -> CachedJobBundle:
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
            extranonce2_size=self._config.extranonce2_size,
            desired_share_difficulty=self._config.share_difficulty,
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

    def new_build_request(
        self,
        artifacts: CachedTemplateArtifacts,
        worker: WorkerIdentityPort | None,
        *,
        mode: str,
        payout_state_generation: int,
        cache_key: tuple[object, ...],
        payout_ledger_artifact: PayoutLedgerArtifact | None = None,
        idle_retarget: bool = False,
        publication_critical: bool = False,
        request_source: str = "routine",
        priority_requested_monotonic: float | None = None,
        preparation_cancellation: JobBuildCancellation | None = None,
    ) -> JobBuildRequest:
        cancellation = (
            JobBuildCancellation(
                timeout_seconds=max(0.001, self._config.build_timeout_seconds),
                monotonic=self._monotonic,
            )
            if preparation_cancellation is None
            else preparation_cancellation
        )
        cancellation.raise_if_cancelled("immutable snapshot")
        self._ports.await_parent_preview(
            artifacts.previousblockhash,
            int(artifacts.template["height"]) - 1,
        )
        payout_artifact = self._ports.payout_state().current_artifact(cancellation)
        if payout_artifact.generation != payout_state_generation:
            raise JobBuildSuperseded(
                "payout artifact generation changed before build request"
            )
        payout_started = self._monotonic()
        payout_policy_json = canonical_json_text(self._ports.payout_policy())
        self.record_phase("payout", self._monotonic() - payout_started)
        cancellation.raise_if_cancelled("payout policy")
        ctv_started = self._monotonic()
        ctv_settlement = self._ports.ctv_settlement(
            int(artifacts.template["height"]),
            artifacts.previousblockhash,
        )
        ctv_settlement_json = (
            canonical_json_text(ctv_settlement)
            if ctv_settlement is not None
            else None
        )
        self.record_phase("ctv", self._monotonic() - ctv_started)
        cancellation.raise_if_cancelled("CTV configuration")
        suffix_hex = self._ports.coinbase_suffix(
            PRISM_JOB_EXTRANONCE1_PLACEHOLDER_HEX,
            "00" * self._config.extranonce2_size,
        )
        collection_identity = (
            self.collection_identity(worker)
            if mode == "collection" and worker is not None
            else None
        )
        decimal_context = getcontext().copy()
        numeric_context_sha256 = canonical_json_sha256(
            {
                "precision": decimal_context.prec,
                "rounding": decimal_context.rounding,
                "minimum_exponent": decimal_context.Emin,
                "maximum_exponent": decimal_context.Emax,
                "capitals": decimal_context.capitals,
                "clamp": decimal_context.clamp,
            }
        )
        with self._cache_lock:
            issued_at_ms = self._issued_at_ms.get(artifacts.generation)
            if issued_at_ms is None:
                issued_at_ms = self._ports.snapshot_anchor_ms(
                    self._ports.wall_time_ms()
                )
                self._issued_at_ms[artifacts.generation] = issued_at_ms
                while len(self._issued_at_ms) > 128:
                    self._issued_at_ms.popitem(last=False)
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
            witness_merkle_sha256=canonical_json_sha256(
                artifacts.witness_merkle_leaves_hex
            ),
            transaction_set_sha256=canonical_json_sha256(
                artifacts.transaction_hexes
            ),
            coinbase_suffix_hex=suffix_hex,
            signing_key_sha256=hashlib.sha256(
                self._ports.signing_seed_hex().encode()
            ).hexdigest(),
            ledger_signing_key_sha256=hashlib.sha256(
                self._ports.ledger_signing_seed_hex().encode()
            ).hexdigest(),
            numeric_context_sha256=numeric_context_sha256,
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

    def _executor_locked(self) -> ThreadPoolExecutor:
        if self._executor_shutdown:
            raise RuntimeError("job build executor is shut down")
        executor = self._executor
        if executor is None:
            executor = ThreadPoolExecutor(
                max_workers=PRISM_JOB_BUILD_EXECUTOR_WORKERS,
                thread_name_prefix="prism-job-build",
            )
            self._executor = executor
        return executor

    def _start_locked(self, request: JobBuildRequest) -> JobBuildFlight:
        executor = self._executor_locked()
        flight = JobBuildFlight(request=request)
        self._scheduler_counts["starts"] += 1
        self._shared_build_counts["started"] += 1
        if request.superseded_monotonic is not None:
            elapsed = max(
                0.0,
                self._monotonic() - request.superseded_monotonic,
            )
            self._replacement_start_seconds["sum"] += elapsed
            self._replacement_start_seconds["count"] += 1
        flight.future = executor.submit(self._execute_request, request)
        self.record_priority_admission_locked(request, "started")
        return flight

    def _arm_locked(self, flight: JobBuildFlight) -> None:
        future = flight.future
        assert future is not None
        future.add_done_callback(
            lambda completed, build_flight=flight: self._build_done(
                build_flight,
                completed,
            )
        )

    def _execute_request(self, request: JobBuildRequest) -> CachedJobBundle:
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
        with self._cache_lock:
            self._active_bundle_builds[control.key] = control
        previous_control = getattr(
            self._phase_local,
            "bundle_build_control",
            None,
        )
        self._phase_local.bundle_build_control = control
        try:
            with localcontext(request.decimal_context):
                return self.build_shared_job_bundle(
                    request.artifacts,
                    request.worker,
                    mode=request.mode,
                    payout_state_generation=request.key.payout_state_generation,
                    payout_artifact=request.payout_ledger_artifact,
                    key=request.cache_key,
                    build_request=request,
                )
        finally:
            self._phase_local.bundle_build_control = previous_control
            with self._cache_lock:
                if self._active_bundle_builds.get(control.key) is control:
                    self._active_bundle_builds.pop(control.key, None)
                control.process = None

    @staticmethod
    def collection_builds_independent(
        first: JobBuildRequest,
        second: JobBuildRequest,
    ) -> bool:
        return (
            first.mode == "collection"
            and second.mode == "collection"
            and first.key.collection_identity != second.key.collection_identity
            and dataclass_replace(first.key, collection_identity=None)
            == dataclass_replace(second.key, collection_identity=None)
        )

    @staticmethod
    def requests_can_share(
        first: JobBuildRequest,
        second: JobBuildRequest,
    ) -> bool:
        return first.equivalence_key == second.equivalence_key or (
            first.mode == "ready"
            and second.mode == "ready"
            and first.cache_key == second.cache_key
        )

    @staticmethod
    def ready_precedes_collection(
        first: JobBuildRequest,
        second: JobBuildRequest,
    ) -> bool:
        return (
            first.mode == "ready"
            and second.mode == "collection"
            and not first.cancellation.is_set()
        )

    @staticmethod
    def defer_collection(
        *blockers: Future[CachedJobBundle],
    ) -> Future[CachedJobBundle]:
        deferred: Future[CachedJobBundle] = Future()
        wake_lock = threading.Lock()

        def wake_for_retry(_completed: Future[CachedJobBundle]) -> None:
            with wake_lock:
                if not deferred.done():
                    deferred.set_exception(
                        JobBuildSuperseded(
                            "collection build capacity became available; retrying"
                        )
                    )

        for blocker in blockers:
            blocker.add_done_callback(wake_for_retry)
        return deferred

    @staticmethod
    def is_publication_critical(request: object) -> bool:
        return bool(getattr(request, "publication_critical", False))

    def record_priority_admission_locked(
        self,
        request: JobBuildRequest,
        result: str,
    ) -> None:
        if not self.is_publication_critical(request):
            return
        self._priority_counts[result] += 1
        if result not in {"started", "coalesced"}:
            return
        if request.priority_admission_recorded:
            return
        request.priority_admission_recorded = True
        elapsed = max(0.0, self._monotonic() - request.requested_monotonic)
        self._priority_admission_seconds["sum"] += elapsed
        self._priority_admission_seconds["count"] += 1

    def record_initial_prepared_work_locked(self, result: str) -> None:
        self._initial_prepared_work_counts[result] += 1

    def new_cancellation(self) -> JobBuildCancellation:
        return JobBuildCancellation(
            timeout_seconds=max(0.001, self._config.build_timeout_seconds),
            monotonic=self._monotonic,
        )

    def begin_priority_preparation(
        self,
        requested_monotonic: float | None = None,
    ) -> tuple[int, float]:
        """Reserve publication priority before immutable request construction."""
        started = (
            self._monotonic()
            if requested_monotonic is None
            else requested_monotonic
        )
        with self._scheduler_lock:
            self._priority_preparation_sequence += 1
            token = self._priority_preparation_sequence
            self._priority_preparations[token] = started
            for routine_ref in tuple(self._routine_preparations.values()):
                routine = routine_ref()
                if routine is not None:
                    routine.cancel("publication priority")
            self._routine_preparations.clear()
            self._priority_changed.set()
        return token, started

    def finish_priority_preparation(self, token: int) -> None:
        with self._scheduler_lock:
            self._priority_preparations.pop(token, None)
            self._priority_changed.set()

    def begin_routine_preparation(
        self,
        *,
        request_source: str,
        cancelled: Callable[[], bool] | None,
    ) -> tuple[int, JobBuildCancellation]:
        """Atomically admit cancellable routine request construction."""
        deferred_recorded = False
        while True:
            self._priority_changed.clear()
            with self._scheduler_lock:
                if not self.publication_priority_scheduled_locked():
                    self._routine_preparation_sequence += 1
                    token = self._routine_preparation_sequence
                    preparation = self.new_cancellation()
                    service_ref = weakref.ref(self)

                    def remove_dead_preparation(
                        dead_ref: weakref.ReferenceType[JobBuildCancellation],
                        *,
                        preparation_token: int = token,
                    ) -> None:
                        service = service_ref()
                        if service is None:
                            return
                        with service._scheduler_lock:
                            if (
                                service._routine_preparations.get(
                                    preparation_token
                                )
                                is dead_ref
                            ):
                                service._routine_preparations.pop(
                                    preparation_token,
                                    None,
                                )

                    self._routine_preparations[token] = weakref.ref(
                        preparation,
                        remove_dead_preparation,
                    )
                    return token, preparation
                if not deferred_recorded:
                    self._priority_counts["routine_deferred"] += 1
                    if request_source == "initial":
                        self.record_initial_prepared_work_locked("deferred")
                    deferred_recorded = True
                stopped = self._executor_shutdown
            if cancelled is not None and cancelled():
                raise JobBuildWaiterCancelled(
                    "job bundle request was cancelled behind publication priority"
                )
            if stopped:
                raise JobBuildWaiterCancelled(
                    "job builder stopped behind publication priority"
                )
            self._priority_changed.wait(0.05)

    def finish_routine_preparation(self, token: int) -> None:
        with self._scheduler_lock:
            self._routine_preparations.pop(token, None)

    def publication_priority_scheduled_locked(self) -> bool:
        if self._priority_preparations:
            return True
        pending = self._pending
        if (
            pending is not None
            and not pending.cancellation.is_set()
            and self.is_publication_critical(pending)
        ):
            return True
        return any(
            flight is not None
            and not flight.request.cancellation.is_set()
            and self.is_publication_critical(flight.request)
            for flight in (self._active, self._retiring)
        )

    def can_inherit_publication_priority(
        self,
        existing: JobBuildRequest,
        incoming: JobBuildRequest,
    ) -> bool:
        if (
            not self.is_publication_critical(incoming)
            or self.is_publication_critical(existing)
        ):
            return True
        cancellation = existing.cancellation
        total_budget = max(
            0.001,
            cancellation.deadline_monotonic - cancellation.started_monotonic,
        )
        remaining_budget = cancellation.deadline_monotonic - self._monotonic()
        progress_age = self._monotonic() - cancellation.last_checkpoint_monotonic
        return bool(
            remaining_budget >= total_budget / 2.0
            and progress_age <= max(0.001, self._config.cancel_grace_seconds)
        )

    def _cancel_flight_locked(
        self,
        flight: JobBuildFlight,
        reason: str,
        *,
        now: float | None = None,
    ) -> bool:
        if not flight.request.cancellation.cancel(reason):
            return False
        flight.request.superseded_monotonic = (
            self._monotonic() if now is None else now
        )
        self._scheduler_counts["supersessions"] += 1
        if reason == "publication priority":
            self._priority_counts["routine_preempted"] += 1
        self._priority_changed.set()
        return True

    def _promote_pending_locked(self) -> None:
        pending = self._pending
        if pending is None:
            return
        active = self._active
        retiring = self._retiring
        if (
            not self.is_publication_critical(pending)
            and any(
                flight is not None
                and not flight.request.cancellation.is_set()
                and self.is_publication_critical(flight.request)
                for flight in (active, retiring)
            )
        ):
            return
        if active is not None:
            if retiring is not None:
                return
            if self.ready_precedes_collection(
                active.request,
                pending,
            ) and not self.is_publication_critical(pending):
                return
            if not self.collection_builds_independent(active.request, pending):
                reason = (
                    "publication priority"
                    if self.is_publication_critical(pending)
                    and not self.is_publication_critical(active.request)
                    else "superseded"
                )
                self._cancel_flight_locked(active, reason)
            self._retiring = active
            self._active = None
        elif retiring is not None:
            if self.ready_precedes_collection(
                retiring.request,
                pending,
            ) and not self.is_publication_critical(pending):
                return
            if not self.collection_builds_independent(retiring.request, pending):
                reason = (
                    "publication priority"
                    if self.is_publication_critical(pending)
                    and not self.is_publication_critical(retiring.request)
                    else "superseded"
                )
                self._cancel_flight_locked(retiring, reason)
        self._pending = None
        flight = self._start_locked(pending)
        self._active = flight
        self._arm_locked(flight)

    def _build_done(
        self,
        flight: JobBuildFlight,
        future: Future[CachedJobBundle],
    ) -> None:
        request = flight.request
        result: CachedJobBundle | None = None
        error: BaseException | None = None
        try:
            result = future.result()
        except BaseException as exc:
            error = exc
        with self._scheduler_lock:
            if error is None and request.cancellation.is_set():
                if request.cancellation.reason == "timeout":
                    error = JobBuildCancelled(
                        "job build completed after its timeout"
                    )
                else:
                    error = JobBuildSuperseded(
                        "obsolete job build completed after cancellation"
                    )
            self._scheduler_counts["completions"] += 1
            self._preparation_count += 1
            self._preparation_seconds_sum += max(
                0.0,
                self._monotonic() - request.cancellation.started_monotonic,
            )
            if request.cancellation.cancelled_monotonic is not None:
                elapsed = max(
                    0.0,
                    self._monotonic()
                    - request.cancellation.cancelled_monotonic,
                )
                self._cancellation_seconds["sum"] += elapsed
                self._cancellation_seconds["count"] += 1
            coordination_cancelled = isinstance(error, JobBuildSuperseded) or (
                request.cancellation.is_set()
                and request.cancellation.reason != "timeout"
            )
            if error is not None and coordination_cancelled:
                self._scheduler_counts["obsolete_results"] += 1
                self._shared_build_counts["superseded"] += 1
                self._ports.note_tip_refresh_superseded()
            elif error is not None:
                self._shared_build_counts["failed"] += 1
            else:
                self._shared_build_counts["completed"] += 1
            if self._active is flight:
                self._active = None
            if self._retiring is flight:
                self._retiring = None
            self._promote_pending_locked()
        if not request.promise.done():
            if error is not None:
                request.promise.set_exception(error)
            else:
                assert result is not None
                request.promise.set_result(result)
        self._priority_changed.set()

    def request_build(self, request: JobBuildRequest) -> Future[CachedJobBundle]:
        with self._scheduler_lock:
            if request.idle_retarget and self._ports.idle_tip_diverged():
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
            self._scheduler_counts["requests"] += 1
            active = self._active
            retiring = self._retiring
            pending = self._pending
            publication_critical = self.is_publication_critical(request)
            if (
                active is not None
                and not active.request.cancellation.is_set()
                and self.requests_can_share(active.request, request)
                and self.can_inherit_publication_priority(active.request, request)
            ):
                if publication_critical:
                    active.request.publication_critical = True
                    active.request.request_source = request.request_source
                    active.request.requested_monotonic = request.requested_monotonic
                    active.request.priority_admission_recorded = True
                    self.record_priority_admission_locked(request, "coalesced")
                if request.request_source == "initial":
                    self.record_initial_prepared_work_locked("singleflight")
                return active.request.promise
            if (
                retiring is not None
                and not retiring.request.cancellation.is_set()
                and self.requests_can_share(retiring.request, request)
                and self.can_inherit_publication_priority(retiring.request, request)
            ):
                if publication_critical:
                    retiring.request.publication_critical = True
                    retiring.request.request_source = request.request_source
                    retiring.request.requested_monotonic = request.requested_monotonic
                    retiring.request.priority_admission_recorded = True
                    self.record_priority_admission_locked(request, "coalesced")
                if request.request_source == "initial":
                    self.record_initial_prepared_work_locked("singleflight")
                return retiring.request.promise
            if (
                pending is not None
                and not pending.cancellation.is_set()
                and self.requests_can_share(pending, request)
                and self.can_inherit_publication_priority(pending, request)
            ):
                if publication_critical:
                    pending.publication_critical = True
                    pending.request_source = request.request_source
                    pending.requested_monotonic = request.requested_monotonic
                    self.record_priority_admission_locked(request, "queued")
                    now = self._monotonic()
                    for occupied in (active, retiring):
                        if (
                            occupied is not None
                            and not self.requests_can_share(
                                occupied.request,
                                pending,
                            )
                            and not self.is_publication_critical(occupied.request)
                        ):
                            self._cancel_flight_locked(
                                occupied,
                                "publication priority",
                                now=now,
                            )
                    self._promote_pending_locked()
                if request.request_source == "initial":
                    self.record_initial_prepared_work_locked("singleflight")
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
                    and self.is_publication_critical(blocker)
                )
                if priority_blockers:
                    self._priority_counts["routine_deferred"] += 1
                    if request.request_source == "initial":
                        self.record_initial_prepared_work_locked("deferred")
                    return self.defer_collection(
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
                        and self.ready_precedes_collection(blocker, request)
                        and not (
                            publication_critical
                            and not self.is_publication_critical(blocker)
                        )
                    ):
                        return self.defer_collection(blocker.promise)
            if active is None:
                if pending is not None:
                    if self.collection_builds_independent(pending, request):
                        self._pending = None
                        flight = self._start_locked(pending)
                        if retiring is None:
                            replacement = self._start_locked(request)
                            self._retiring = flight
                            self._active = replacement
                            self._arm_locked(flight)
                            self._arm_locked(replacement)
                            return request.promise
                        self._active = flight
                        self._arm_locked(flight)
                        return self.defer_collection(
                            flight.request.promise,
                            retiring.request.promise,
                        )
                    pending.cancellation.cancel("superseded while pending")
                    if not pending.promise.done():
                        pending.promise.set_exception(
                            JobBuildSuperseded(
                                "pending job build was superseded"
                            )
                        )
                    self._pending = None
                    self._scheduler_counts["supersessions"] += 1
                if (
                    retiring is not None
                    and not self.collection_builds_independent(
                        retiring.request,
                        request,
                    )
                ):
                    now = self._monotonic()
                    reason = (
                        "publication priority"
                        if publication_critical
                        and not self.is_publication_critical(retiring.request)
                        else "superseded"
                    )
                    if self._cancel_flight_locked(
                        retiring,
                        reason,
                        now=now,
                    ):
                        request.superseded_monotonic = now
                flight = self._start_locked(request)
                self._active = flight
                self._arm_locked(flight)
                return request.promise
            if self.collection_builds_independent(active.request, request):
                if self._retiring is None:
                    self._retiring = active
                    flight = self._start_locked(request)
                    self._active = flight
                    self._arm_locked(flight)
                    return request.promise
                if pending is None:
                    self._pending = request
                    if publication_critical:
                        self.record_priority_admission_locked(request, "queued")
                        now = self._monotonic()
                        for occupied in (active, self._retiring):
                            if (
                                occupied is not None
                                and not self.is_publication_critical(
                                    occupied.request
                                )
                            ):
                                self._cancel_flight_locked(
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
                    self._scheduler_counts["supersessions"] += 1
                    self._pending = request
                    self.record_priority_admission_locked(request, "queued")
                    now = self._monotonic()
                    for occupied in (active, retiring):
                        if not self.is_publication_critical(occupied.request):
                            self._cancel_flight_locked(
                                occupied,
                                "publication priority",
                                now=now,
                            )
                    return request.promise
                return self.defer_collection(
                    active.request.promise,
                    retiring.request.promise,
                )
            now = self._monotonic()
            for obsolete in (active, retiring):
                if obsolete is not None:
                    reason = (
                        "publication priority"
                        if publication_critical
                        and not self.is_publication_critical(obsolete.request)
                        else "superseded"
                    )
                    self._cancel_flight_locked(
                        obsolete,
                        reason,
                        now=now,
                    )
            request.superseded_monotonic = now
            if retiring is None:
                self._retiring = active
                flight = self._start_locked(request)
                self._active = flight
                self._arm_locked(flight)
                return request.promise
            previous_pending = self._pending
            if previous_pending is not None:
                previous_pending.cancellation.cancel("superseded while pending")
                if not previous_pending.promise.done():
                    previous_pending.promise.set_exception(
                        JobBuildSuperseded(
                            "pending job build was superseded"
                        )
                    )
                self._scheduler_counts["supersessions"] += 1
            self._pending = request
            if publication_critical:
                self.record_priority_admission_locked(request, "queued")
            return request.promise

    def cancel_obsolete_builds(
        self,
        reason: str,
        *,
        keep_published_snapshot: bool = False,
    ) -> None:
        def keep(request: JobBuildRequest) -> bool:
            return bool(
                keep_published_snapshot
                and self._published_snapshot_matches(
                    request.artifacts,
                    exact_generation=(
                        getattr(request, "mode", "ready") == "collection"
                    ),
                )
            )

        with self._scheduler_lock:
            for flight in (self._active, self._retiring):
                if (
                    flight is not None
                    and not keep(flight.request)
                    and flight.request.cancellation.cancel(reason)
                ):
                    flight.request.superseded_monotonic = self._monotonic()
                    self._scheduler_counts["supersessions"] += 1
            pending = self._pending
            if pending is not None and not keep(pending):
                pending.cancellation.cancel(reason)
                if not pending.promise.done():
                    pending.promise.set_exception(
                        JobBuildSuperseded(f"pending job build {reason}")
                    )
                self._pending = None
                self._scheduler_counts["supersessions"] += 1

    def cancel_obsolete_bundle_processes(
        self,
        *,
        current_tip: str | None = None,
        payout_state_generation: int | None = None,
    ) -> None:
        processes: list[subprocess.Popen[str]] = []
        with self._cache_lock:
            for control in self._active_bundle_builds.values():
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

    def register_process(
        self,
        control: JobBundleBuildControl,
        process: subprocess.Popen[str],
    ) -> None:
        terminate = False
        with self._cache_lock:
            if (
                self._active_bundle_builds.get(control.key) is not control
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

    def active_build_control(self) -> JobBundleBuildControl | None:
        value = getattr(self._phase_local, "bundle_build_control", None)
        return value if isinstance(value, JobBundleBuildControl) else None

    def shutdown(self) -> None:
        with self._scheduler_lock:
            for flight in (self._active, self._retiring):
                if flight is not None:
                    flight.request.cancellation.cancel("shutdown")
            pending = self._pending
            if pending is not None:
                pending.cancellation.cancel("shutdown")
                if not pending.promise.done():
                    pending.promise.set_exception(
                        JobBuildSuperseded(
                            "pending job build cancelled by shutdown"
                        )
                    )
            self._pending = None
            executor = self._executor
            self._executor = None
            self._executor_shutdown = True
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)

    def on_template_artifacts_changed(
        self,
        artifacts: CachedTemplateArtifacts,
        fingerprint_changed: bool,
    ) -> None:
        published_artifacts = self._ports.published_artifacts()

        def keep(entry: CachedJobBundle) -> bool:
            current = entry.template_fingerprint == artifacts.fingerprint and (
                not entry.collection_only
                or entry.template_generation == artifacts.generation
            )
            published = (
                published_artifacts is not None
                and entry.template_fingerprint == published_artifacts.fingerprint
                and (
                    not entry.collection_only
                    or entry.template_generation == published_artifacts.generation
                )
            )
            return current or published

        with self._cache_lock:
            self._bundle_cache = OrderedDict(
                (key, entry)
                for key, entry in self._bundle_cache.items()
                if keep(entry)
            )
        if fingerprint_changed:
            self.cancel_obsolete_builds(
                "template fingerprint superseded",
                keep_published_snapshot=True,
            )

    def on_template_artifacts_cleared(
        self,
        _artifacts: CachedTemplateArtifacts,
    ) -> None:
        published_artifacts = self._ports.published_artifacts()
        keep_published = bool(
            published_artifacts is not None
            and self._ports.published_snapshot_artifacts(published_artifacts)
        )

        def keep(entry: CachedJobBundle) -> bool:
            return bool(
                keep_published
                and published_artifacts is not None
                and entry.template_fingerprint == published_artifacts.fingerprint
                and (
                    not entry.collection_only
                    or entry.template_generation == published_artifacts.generation
                )
            )

        with self._cache_lock:
            self._bundle_cache = OrderedDict(
                (key, entry)
                for key, entry in self._bundle_cache.items()
                if keep(entry)
            )
        self.cancel_obsolete_builds(
            "template artifacts cleared",
            keep_published_snapshot=keep_published,
        )

    def clear_cache(self) -> None:
        with self._cache_lock:
            self._bundle_cache.clear()

    def cache_bundle_if_current(
        self,
        built: CachedJobBundle,
        artifacts: CachedTemplateArtifacts,
    ) -> bool:
        payout_state = self._ports.payout_state()
        if not self._bundle_cache_admissible(built, artifacts, payout_state):
            return False
        # P1 publication never takes the template fence. Keep the one-way
        # payout -> template -> cache order so neither invalidation path can
        # invert admission.
        with payout_state.cache_publication_admission():
            with self.template_repository.publication_admission():
                if not self._bundle_cache_admissible(
                    built,
                    artifacts,
                    payout_state,
                ):
                    return False
                with self._cache_lock:
                    self._bundle_cache[built.key] = built
                    self._bundle_cache.move_to_end(built.key)
                    while len(self._bundle_cache) > MAX_PRISM_JOB_BUNDLE_CACHE_ENTRIES:
                        self._bundle_cache.popitem(last=False)
                return True

    def _published_snapshot_matches(
        self,
        artifacts: CachedTemplateArtifacts,
        *,
        exact_generation: bool,
    ) -> bool:
        if not self._ports.published_snapshot_artifacts(artifacts):
            return False
        if not exact_generation:
            return True
        published = self._ports.published_artifacts()
        return bool(
            published is not None
            and published.generation == artifacts.generation
            and published.fingerprint == artifacts.fingerprint
            and published.previousblockhash == artifacts.previousblockhash
        )

    def _bundle_cache_admissible(
        self,
        built: CachedJobBundle,
        artifacts: CachedTemplateArtifacts,
        payout_state: PayoutStatePort,
    ) -> bool:
        if not self._ports.artifacts_buildable(artifacts):
            return False
        snapshot_pinned = self._published_snapshot_matches(
            artifacts,
            exact_generation=built.collection_only,
        )
        payout = payout_state.snapshot()
        published_artifact = payout.published.artifact
        if (
            payout.publication_blocked
            or built.payout_state_generation != payout.generation
            or built.build_key is None
            or published_artifact is None
            or built.build_key.payout_artifact_sha256
            != published_artifact.prior_balances_sha256
        ):
            return False
        current = self.template_repository.current_artifacts()
        globally_current = (
            current is not None
            and current.fingerprint == artifacts.fingerprint
            and current.previousblockhash == artifacts.previousblockhash
            and (
                not built.collection_only
                or current.generation == artifacts.generation
            )
        )
        return globally_current or snapshot_pinned

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
        with self._ports.start_bundle_build():
            priority_token: int | None = None
            if publication_critical:
                (
                    priority_token,
                    priority_requested_monotonic,
                ) = self.begin_priority_preparation(
                    priority_requested_monotonic
                )
            try:
                return self._shared_job_bundle(
                    artifacts,
                    worker,
                    mode=mode,
                    cancelled=cancelled,
                    retry_superseded=retry_superseded,
                    idle_retarget=idle_retarget,
                    publication_critical=publication_critical,
                    request_source=request_source,
                    priority_requested_monotonic=(
                        priority_requested_monotonic
                    ),
                )
            finally:
                if priority_token is not None:
                    self.finish_priority_preparation(priority_token)

    def _shared_job_bundle(
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
        while True:
            routine_token: int | None = None
            preparation_cancellation: JobBuildCancellation | None = None
            payout_state_generation: int | None = None
            request: JobBuildRequest | None = None
            try:
                if not publication_critical:
                    (
                        routine_token,
                        preparation_cancellation,
                    ) = self.begin_routine_preparation(
                        request_source=request_source,
                        cancelled=cancelled,
                    )
                resolved_mode = self.job_bundle_mode(mode)
                if preparation_cancellation is not None:
                    preparation_cancellation.raise_if_cancelled(
                        "request preparation admission"
                    )
                if resolved_mode == "collection" and worker is None:
                    raise CollectionIdentityUnavailable(
                        "collection-mode worker identity is temporarily unavailable"
                    )
                payout = self._ports.payout_state().snapshot()
                payout_state_generation = payout.generation
                payout_artifact = (
                    self._ports.payout_state().usable_ledger_artifact(
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
                    payout_artifact.generation
                    if payout_artifact is not None
                    else 0
                )
                key = self.job_bundle_key(
                    artifacts,
                    mode=resolved_mode,
                    payout_state_generation=payout_state_generation,
                    payout_artifact_generation=payout_artifact_generation,
                    worker=worker,
                )
                cached = self.lookup_bundle(key)
                if self.bundle_entry_usable(cached, artifacts):
                    if preparation_cancellation is not None:
                        preparation_cancellation.raise_if_cancelled(
                            "bundle cache lookup"
                        )
                    if routine_token is not None:
                        self.finish_routine_preparation(routine_token)
                        routine_token = None
                    self.record_cache_event("bundle", hit=True)
                    if request_source == "initial":
                        with self._scheduler_lock:
                            self._initial_prepared_work_counts["cache_hit"] += 1
                    assert cached is not None
                    return self.bind_cached_bundle(cached, artifacts)
                if self.job_bundle_mode(mode) != resolved_mode:
                    continue
                self.record_cache_event("bundle", hit=False)
                request = self.new_build_request(
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
                with self._admission_lock:
                    with self._scheduler_lock:
                        if routine_token is not None:
                            self.finish_routine_preparation(routine_token)
                            routine_token = None
                        request.cancellation.raise_if_cancelled(
                            "scheduler admission"
                        )
                        if self.job_bundle_mode(mode) != resolved_mode:
                            request.cancellation.cancel("worker mode superseded")
                            continue
                        if cancelled is not None and cancelled():
                            raise JobBuildWaiterCancelled(
                                "job bundle request was cancelled before preparation"
                            )
                        promise = self.request_build(request)
                wait_deadline = self._monotonic() + max(
                    0.001,
                    self._config.build_timeout_seconds
                    + self._config.cancel_grace_seconds
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
                                max(0.001, wait_deadline - self._monotonic()),
                            )
                        )
                        break
                    except TimeoutError:
                        if self._monotonic() >= wait_deadline:
                            raise
            except TimeoutError as exc:
                assert request is not None
                request.cancellation.cancel("timeout")
                self._ports.schedule_refresh_retry()
                raise JobBuildCancelled(
                    "job build timed out; immediate retry scheduled"
                ) from exc
            except JobBuildCancelled:
                self._ports.schedule_refresh_retry()
                if not retry_superseded:
                    raise
                if payout_state_generation is None:
                    raise
                if not self._ports.artifacts_buildable(artifacts):
                    raise
                current = self.template_repository.current_artifacts()
                payout_current = (
                    payout_state_generation
                    == self._ports.payout_state().snapshot().generation
                )
                if current is artifacts and payout_current:
                    continue
                if current is artifacts:
                    continue
                raise
            finally:
                if routine_token is not None:
                    self.finish_routine_preparation(routine_token)
            built = self.bind_cached_bundle(built, artifacts)
            if not self.cache_bundle_if_current(built, artifacts):
                with self._scheduler_lock:
                    self._scheduler_counts["obsolete_results"] += 1
                if not self._ports.artifacts_buildable(artifacts):
                    self._ports.note_tip_refresh_superseded()
                    raise JobBuildSuperseded(
                        "observed tip changed before cache publication"
                    )
                payout_current = (
                    built.payout_state_generation
                    == self._ports.payout_state().snapshot().generation
                )
                published_artifacts = self._ports.published_artifacts()
                if (
                    payout_current
                    and built.template is artifacts.template
                    and built.template_generation == artifacts.generation
                    and (
                        not retry_superseded
                        or published_artifacts is artifacts
                    )
                ):
                    return built
                if (
                    retry_superseded
                    and self.template_repository.current_artifacts() is artifacts
                ):
                    continue
                raise JobBuildSuperseded(
                    "job build key changed before cache publication"
                )
            return built

    def build_shared_job_bundle(
        self,
        artifacts: CachedTemplateArtifacts,
        worker: WorkerIdentityPort | None = None,
        *,
        mode: str | None = None,
        payout_state_generation: int | None = None,
        payout_artifact: PayoutLedgerArtifact | None = None,
        key: tuple[object, ...] | None = None,
        build_request: JobBuildRequest | None = None,
    ) -> CachedJobBundle:
        resolved_mode = self.job_bundle_mode(mode)
        if resolved_mode == "collection" and worker is None:
            raise CollectionIdentityUnavailable(
                "collection-mode worker identity is temporarily unavailable"
            )
        payout_snapshot = self._ports.payout_state().snapshot()
        if payout_state_generation is None:
            payout_state_generation = payout_snapshot.generation
        if payout_snapshot.publication_blocked:
            raise PayoutStatePublicationBlocked(
                "payout state invalidation is pending publication"
            )
        if key is None:
            key = self.job_bundle_key(
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
            build_request = self.new_build_request(
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
        cancellation.raise_if_cancelled("ledger_snapshot")
        template_value = json.loads(build_request.template_json)
        if not isinstance(template_value, dict):
            raise RuntimeError("immutable job template is not an object")
        template: dict[str, Any] = template_value
        issued_at_ms = build_request.key.issued_at_ms
        started = self._monotonic()
        snapshot_window_weight = (
            PRISM_REWARD_WINDOW_MULTIPLIER
            * PRISM_SNAPSHOT_WINDOW_MARGIN
            * int(build_request.key.network_difficulty)
        )
        if payout_artifact is not None:
            if (
                self._ports.payout_state().usable_ledger_artifact(
                    payout_state_generation,
                    build_request.key.network_difficulty,
                )
                is not payout_artifact
            ):
                raise JobBuildSuperseded(
                    "precomputed payout artifact changed before construction"
                )
            prior_balances = list(payout_artifact.prior_balances)
            if (
                canonical_json_sha256(prior_balances)
                != build_request.key.payout_artifact_sha256
            ):
                raise JobBuildSuperseded(
                    "precomputed payout artifact does not match payout generation"
                )
            shares = list(payout_artifact.shares_json)
            prior_balances = self._ports.prior_balances_for_parent(
                str(template["previousblockhash"]),
                int(template["height"]) - 1,
                prior_balances,
            )
        else:
            payout_service = self._ports.payout_state()
            with payout_service.prepare_lock:
                payout_snapshot = payout_service.snapshot()
                published_artifact = payout_snapshot.published.artifact
                if payout_snapshot.publication_blocked:
                    raise PayoutStatePublicationBlocked(
                        "payout state invalidation is pending publication"
                    )
                if (
                    payout_state_generation != payout_snapshot.generation
                    or published_artifact is None
                    or published_artifact.prior_balances_sha256
                    != build_request.key.payout_artifact_sha256
                ):
                    raise JobBuildSuperseded(
                        "payout generation changed before ledger snapshot"
                    )
                records = (
                    self._ports.snapshot_at_job_issue(
                        issued_at_ms,
                        snapshot_window_weight,
                    )
                    if resolved_mode == "ready"
                    else []
                )
                prior_balances = self._ports.prior_balances_for_parent(
                    str(template["previousblockhash"]),
                    int(template["height"]) - 1,
                    build_request.payout_artifact.prior_balances(),
                )
            cancellation.raise_if_cancelled("ledger_snapshot_complete")
            shares = []
            for index, record in enumerate(records):
                if index % 256 == 0:
                    cancellation.raise_if_cancelled(
                        "ledger_snapshot_conversion"
                    )
                shares.append(record.to_prism_json())
        bundle_anchor_ms = (
            payout_artifact.snapshot_anchor_ms
            if payout_artifact is not None
            and payout_artifact.snapshot_anchor_ms is not None
            else issued_at_ms
        )
        ledger_elapsed = self._monotonic() - started
        self.record_phase("ledger", ledger_elapsed)
        if resolved_mode == "ready":
            self._ports.record_tip_refresh_phase(
                "ledger_snapshot",
                ledger_elapsed,
            )
        final_build_key = dataclass_replace(
            build_request.key,
            share_snapshot_sha256=canonical_json_sha256(shares),
        )
        cancellation.raise_if_cancelled("payout_derivation")
        started = self._monotonic()
        placeholder_suffix_hex = final_build_key.coinbase_suffix_hex
        collection_identity: tuple[str, str] | None = None
        previous_metrics_scope = bool(
            getattr(self._phase_local, "tip_refresh_metrics", False)
        )
        self._phase_local.tip_refresh_metrics = resolved_mode == "ready"
        try:
            if resolved_mode == "ready":
                if not shares:
                    raise RuntimeError(
                        "ready-pool ledger snapshot contained no payout shares"
                    )
                cancellation.raise_if_cancelled("ctv_manifest")
                cancellation.raise_if_cancelled("signing_verification")
                bundle = self.bundle_compiler().build_audit_bundle(
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
                )
                collection_only = False
            else:
                assert worker is not None
                cancellation.raise_if_cancelled("ctv_manifest")
                cancellation.raise_if_cancelled("signing_verification")
                bundle = self.build_collection_bundle(
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
                collection_identity = self.collection_identity(worker)
        finally:
            self._phase_local.tip_refresh_metrics = previous_metrics_scope
        manifest = bundle["signed_coinbase_manifest"]["manifest"]
        prospective_prior_balances: (
            tuple[tuple[str, str, str, int], ...] | None
        ) = None
        payout_policy_manifest = bundle.get("payout_policy_manifest")
        if isinstance(payout_policy_manifest, dict) and isinstance(
            payout_policy_manifest.get("accounts"),
            list,
        ):
            prospective_prior_balances = (
                self._ports.serialize_prior_balance_preview(
                    self._ports.accepted_block_preview_from_bundle(
                        bundle,
                        prior_balances,
                    )
                )
            )
        cancellation.raise_if_cancelled("bundle_assembly")
        assembly_started = self._monotonic()
        base_job = direct_stratum.make_job_from_builder_manifest(
            job_id="prism-template-base",
            template=template,
            manifest=manifest,
            extranonce1_hex=PRISM_JOB_EXTRANONCE1_PLACEHOLDER_HEX,
            extranonce2_size=self._config.extranonce2_size,
            desired_share_difficulty=self._config.share_difficulty,
            clean_jobs=True,
            transaction_hexes=build_request.transaction_hexes,
        )
        self.record_phase("assembly", self._monotonic() - assembly_started)
        cancellation.raise_if_cancelled("serialization")
        cancellation.raise_if_cancelled("bundle_publication")
        self.record_phase("bundle", self._monotonic() - started)
        return CachedJobBundle(
            key=key,
            template=artifacts.template,
            template_fingerprint=artifacts.fingerprint,
            coinbase_manifest=manifest,
            shares_json=shares,
            prior_balances=prior_balances,
            found_block=bundle["found_block"],
            collection_only=collection_only,
            issued_at_ms=issued_at_ms,
            base_job=base_job,
            built_monotonic=self._monotonic(),
            template_generation=artifacts.generation,
            payout_state_generation=payout_state_generation,
            payout_artifact_generation=(
                payout_artifact.generation if payout_artifact is not None else 0
            ),
            collection_identity=collection_identity,
            prospective_prior_balances=prospective_prior_balances,
            build_key=final_build_key,
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
        return self.bundle_compiler().build_audit_bundle(
            shares=[share],
            found_block={
                "block_height": int(template["height"]),
                "coinbase_value_sats": int(template["coinbasevalue"]),
                "network_difficulty": network_difficulty,
                "anchor_job_issued_at_ms": issued_at_ms,
            },
            prior_balances=[],
            coinbase_script_sig_suffix_hex=suffix_hex,
            witness_merkle_leaves_hex=(
                direct_stratum.witness_merkle_leaves_hex(transaction_hexes)
            ),
            ctv_fee_parent_hash=str(template["previousblockhash"]),
            summary_only=summary_only,
            payout_policy=payout_policy,
            ctv_settlement=ctv_settlement,
            cancellation=cancellation,
        )

    def set_preparation_pending(self, pending: bool) -> None:
        with self._prepared_lock:
            self._preparation_pending = bool(pending)

    def set_prepared_ready(
        self,
        snapshot: QbitTipTemplateSnapshot | None,
        bundle: CachedJobBundle | None,
    ) -> None:
        with self._prepared_lock:
            self._prepared_ready_snapshot = snapshot
            self._prepared_ready_bundle = bundle

    def clear_prepared_ready(self) -> None:
        self.set_prepared_ready(None, None)

    def prepared_ready_snapshot(
        self,
    ) -> tuple[
        CachedJobBundle | None,
        QbitTipTemplateSnapshot | None,
        bool,
    ]:
        with self._prepared_lock:
            return (
                self._prepared_ready_bundle,
                self._prepared_ready_snapshot,
                self._preparation_pending,
            )

    def record_failure(self) -> None:
        with self._cache_lock:
            self._failure_count += 1

    def record_worker_event(self, event: str) -> None:
        with self._scheduler_lock:
            if event == "start":
                if self._worker_restart_pending:
                    self._worker_counts["restarts"] += 1
                    self._worker_restart_pending = False
                self._worker_counts["starts"] += 1
                return
            if event == "termination":
                self._worker_counts["terminations"] += 1
                self._worker_restart_pending = True
                return
            if event == "crash":
                self._worker_counts["crashes"] += 1
                self._worker_restart_pending = True
                return
            raise ValueError(f"unknown job builder worker event: {event}")

    def tip_refresh_metrics_enabled(self) -> bool:
        return bool(getattr(self._phase_local, "tip_refresh_metrics", False))

    def shared_preparation_metrics(self) -> dict[str, object]:
        with self._scheduler_lock:
            return {
                "build_counts": dict(self._shared_build_counts),
                "preparation_sum": self._preparation_seconds_sum,
                "preparation_count": self._preparation_count,
                "waiters": self._preparation_waiters,
            }

    def metrics_snapshot(self) -> dict[str, object]:
        with self._cache_lock:
            cache = {
                "bucket_counts": dict(self._build_bucket_counts),
                "build_sum": self._build_seconds_sum,
                "build_count": self._build_count,
                "phase_seconds": dict(self._phase_seconds),
                "hit_counts": dict(self._cache_hits),
                "miss_counts": dict(self._cache_misses),
                "failure_count": self._failure_count,
            }
        with self._scheduler_lock:
            now = self._monotonic()
            priority_requests = tuple(
                request
                for request in (
                    self._active.request if self._active is not None else None,
                    (
                        self._retiring.request
                        if self._retiring is not None
                        else None
                    ),
                    self._pending,
                )
                if request is not None
                and not request.cancellation.is_set()
                and self.is_publication_critical(request)
            )
            priority_preparations = tuple(self._priority_preparations.values())
            scheduler = {
                "scheduler_counts": dict(self._scheduler_counts),
                "priority_counts": dict(self._priority_counts),
                "priority_admission_seconds": dict(
                    self._priority_admission_seconds
                ),
                "initial_prepared_counts": dict(
                    self._initial_prepared_work_counts
                ),
                "cancellation_seconds": dict(self._cancellation_seconds),
                "replacement_seconds": dict(self._replacement_start_seconds),
                "worker_counts": dict(self._worker_counts),
                "active_builds": int(self._active is not None),
                "pending_builds": int(self._pending is not None),
                "priority_active": int(
                    bool(priority_requests or priority_preparations)
                ),
                "priority_age_seconds": max(
                    (
                        *(now - request.requested_monotonic for request in priority_requests),
                        *(now - started for started in priority_preparations),
                        0.0,
                    )
                ),
            }
        return {**cache, **scheduler}

    def bundle_cache_snapshot(self) -> tuple[CachedJobBundle, ...]:
        with self._cache_lock:
            return tuple(self._bundle_cache.values())

    def cached_bundle_for_key(
        self,
        key: tuple[object, ...],
    ) -> CachedJobBundle | None:
        with self._cache_lock:
            return self._bundle_cache.get(key)

    @contextmanager
    def cache_admission(
        self,
        key: tuple[object, ...],
        bundle: CachedJobBundle,
        *,
        allow_uncached: bool,
    ) -> Iterator[bool]:
        """Hold exact cache identity through an external final commit guard.

        V1/S2 still perform the client/tip guard while this admission is held.
        This preserves the existing cache-before-client lock order until those
        domains move; no callback is invoked while the service lock is held.
        """
        with self._cache_lock:
            if self._config.cache_seconds <= 0:
                yield bool(allow_uncached and bundle.key == key)
                return
            cached = self._bundle_cache.get(key)
            matches = bool(
                cached is not None
                and (
                    cached is bundle
                    or (
                        bundle.key == cached.key
                        and bundle.coinbase_manifest is cached.coinbase_manifest
                        and bundle.shares_json is cached.shares_json
                        and bundle.prior_balances is cached.prior_balances
                        and bundle.found_block is cached.found_block
                        and bundle.collection_only == cached.collection_only
                        and bundle.issued_at_ms == cached.issued_at_ms
                        and bundle.built_monotonic == cached.built_monotonic
                        and bundle.payout_state_generation
                        == cached.payout_state_generation
                        and bundle.payout_artifact_generation
                        == cached.payout_artifact_generation
                        and bundle.collection_identity == cached.collection_identity
                    )
                )
                and self._monotonic() - cached.built_monotonic
                <= self._config.cache_seconds
            )
            yield matches

    def clear_issued_at_for_test(self) -> None:
        with self._cache_lock:
            self._issued_at_ms.clear()

    def set_ready_for_test(self, ready: bool) -> None:
        with self._readiness_lock:
            self._ready_latched = bool(ready)

    def ready_latched(self) -> bool:
        with self._readiness_lock:
            return self._ready_latched

    def active_bundle_builds_for_test(
        self,
    ) -> dict[tuple[object, ...], JobBundleBuildControl]:
        with self._cache_lock:
            return dict(self._active_bundle_builds)

    def scheduler_state_for_test(
        self,
    ) -> tuple[JobBuildFlight | None, JobBuildFlight | None, JobBuildRequest | None]:
        with self._scheduler_lock:
            return self._active, self._retiring, self._pending

    def replace_config_for_test(self, config: JobBundleConfig) -> None:
        self._config = config

    def set_cache_seconds_for_test(self, seconds: float) -> None:
        self._config = dataclass_replace(
            self._config,
            cache_seconds=float(seconds),
        )

    def set_min_ready_miners_for_test(self, count: int) -> None:
        self._config = dataclass_replace(
            self._config,
            min_ready_miners=int(count),
        )
