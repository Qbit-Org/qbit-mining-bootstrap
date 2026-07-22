"""Per-session PRISM job delivery and retained-job ownership.

This module deliberately has no dependency on :mod:`prism_coordinator`.  The
coordinator is the construction root and supplies narrow runtime operations;
session membership remains authoritative in :class:`SessionRegistry` and the
heavy shared bundle build remains in J1's job-bundle service.
"""

from __future__ import annotations

from collections import OrderedDict
from concurrent.futures import Future
from dataclasses import dataclass, field, replace as dataclass_replace
from decimal import Decimal
import threading
import time
import traceback
from typing import Any, Callable, Mapping, MutableMapping, Protocol

from lab.auxpow import vardiff
from lab.prism import direct_stratum
from lab.prism.bounded_executor import _BoundedPriorityExecutor, _DeliveryQueueFull
from lab.prism.coordinator_config import (
    DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_PER_CONNECTION,
    DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_SECONDS,
    DEFAULT_PRISM_STALE_GRACE_SECONDS,
)
from lab.prism.job_bundle import CachedJobBundle, JobBuildWaiterCancelled
from lab.prism.payout_state import TemplateRefreshBlocked, TemplateRefreshSuperseded
from lab.prism.stratum_session import (
    ClientState,
    SessionRegistry,
    StratumError,
    WorkerIdentity,
    client_vardiff_lock,
    client_can_receive_jobs as session_client_can_receive_jobs,
)
from lab.prism.template_artifacts import CachedTemplateArtifacts, QbitTipTemplateSnapshot
from lab.prism.tip_refresh import (
    FanoutCancellation,
    RefreshClientTarget,
    RefreshResult,
    TipRefreshValidationToken,
)


MAX_ACTIVE_PRISM_JOBS_PER_CLIENT = 16
DEFAULT_PRISM_EVICTED_JOB_PRUNE_INTERVAL_SECONDS = 1.0
PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS = 0.05
PRISM_DELIVERY_PRIORITY_NEW_TIP = 0
PRISM_DELIVERY_PRIORITY_INITIAL = 1
PRISM_DELIVERY_PRIORITY_SAME_TIP = 2
PRISM_EVICTED_JOB_CLASSES = ("same_tip", "stale_grace")
PRISM_EVICTED_JOB_SUBMIT_OUTCOMES = (
    "accepted_same_tip",
    "credited_stale_grace",
)
PRISM_EVICTED_JOB_CAPACITY_SCOPES = ("connection",)
PRISM_CREDIT_POLICY_STALE_GRACE = "stale-grace"
DEFAULT_PRISM_INITIAL_JOB_MAX_WORKERS = 4


@dataclass(frozen=True)
class PrismJobContext:
    job: direct_stratum.DirectQbitStratumJob
    template: dict[str, Any]
    shares_json: list[dict[str, object]]
    prior_balances: list[dict[str, object]]
    found_block: dict[str, object]
    share_weight: int
    collection_only: bool
    worker: WorkerIdentity
    issued_at_ms: int
    template_fingerprint: str | None = None
    template_generation: int = 0
    payout_state_generation: int = 0
    prospective_prior_balances: tuple[tuple[str, str, str, int], ...] | None = None
    payout_artifact_generation: int = 0
    connection_id: int = 0
    authorization_generation: int = 0
    difficulty_generation: int = 0


@dataclass(frozen=True)
class EvictedJobEntry:
    context: PrismJobContext
    connection_id: int
    evicted_monotonic: float
    previousblockhash: str
    client: ClientState | None = None


@dataclass(eq=False)
class PendingInitialJob:
    client: ClientState
    authorization_generation: int
    worker: WorkerIdentity
    requested_monotonic: float
    deadline_monotonic: float | None
    connection_id: int | None = None
    difficulty_generation: int | None = None
    cancelled: threading.Event = field(default_factory=threading.Event)
    future: Future[bool] | None = None
    predecessor: Future[bool] | None = None


@dataclass(frozen=True)
class InitialJobConfig:
    max_pending: int
    timeout_seconds: float
    max_workers: int = DEFAULT_PRISM_INITIAL_JOB_MAX_WORKERS


@dataclass(frozen=True)
class InitialJobSnapshot:
    max_workers: int
    pending_count: int
    queue_rejection_count: int
    timeout_count: int
    cancelled_count: int
    coalesced_count: int
    sent_count: int
    failed_count: int
    superseded_count: int
    queue_capacity_reclaimed_count: int
    delivery_latency_seconds_sum: float
    delivery_latency_count: int
    last_delivery_monotonic: float | None


@dataclass(frozen=True)
class CurrentJobSource:
    payout_generation: int
    current_tip: str | None
    published_template: QbitTipTemplateSnapshot | None


@dataclass(frozen=True)
class RefreshSource:
    ready_latched: bool
    payout_generation: int


@dataclass(frozen=True)
class DeliverySourceAuthority:
    """Immutable source identity revalidated under the shared S1/R1 lock."""

    kind: str
    payout_generation: int
    template_generation: int
    observation_sequence: int
    template_fingerprint: str | None = None
    artifacts: CachedTemplateArtifacts | None = None
    snapshot: QbitTipTemplateSnapshot | None = None
    token: TipRefreshValidationToken | None = None
    bundle: CachedJobBundle | None = None
    payout_snapshot: Any = None
    context_parent: str = ""
    lapsed_live_validated: bool = False


@dataclass(frozen=True)
class AdmittedIdleBundleSource:
    """Owner-issued J1 lease for one exact immutable idle bundle source.

    J1 fixes cache freshness at admission and returns the exact artifact,
    bundle, and cache identity.  S2 may finish delivery after cache eviction
    without reacquiring a J1 lock; R1, P1, and client authority remain final
    commit checks.
    """

    artifacts: CachedTemplateArtifacts
    bundle: CachedJobBundle
    cache_identity: tuple[object, ...]
    allow_uncached: bool


@dataclass
class IdleDeliveryAuthority:
    """Typed V1 state expected at an idle-retarget delivery commit."""

    connection_id: int
    worker: WorkerIdentity
    expected_active_job: PrismJobContext | None
    expected_window_started: float
    pending_difficulty: Decimal
    committed_reset_monotonic: float | None = None


class JobBuildFailed(RuntimeError):
    """A client job could not be built, without implying socket failure."""


# Compatibility name used during the staged extraction.
_JobBuildFailed = JobBuildFailed


class InitialJobTracker:
    """Own pending first-job identities independently of build/executor policy."""

    def __init__(
        self,
        pending: MutableMapping[ClientState, PendingInitialJob] | None = None,
    ) -> None:
        self.pending = pending if pending is not None else {}

    def adopt(
        self,
        pending: MutableMapping[ClientState, PendingInitialJob],
    ) -> None:
        self.pending = pending

    def request_current_locked(
        self,
        request: PendingInitialJob,
        *,
        clients: object,
        stopping: bool,
    ) -> bool:
        client = request.client
        return bool(
            not stopping
            and self.pending.get(client) is request
            and client in clients  # type: ignore[operator]
            and (
                request.connection_id is None
                or client.connection_id == request.connection_id
            )
            and client.authorized
            and client.subscribed
            and client.worker == request.worker
            and int(client.authorization_generation)
            == request.authorization_generation
            and (
                request.difficulty_generation is None
                or int(client.difficulty_generation)
                == request.difficulty_generation
            )
            and not client.closing
            and (
                request.deadline_monotonic is None
                or time.monotonic() < request.deadline_monotonic
            )
            and not request.cancelled.is_set()
        )

    def cancel_locked(
        self,
        client: ClientState,
    ) -> PendingInitialJob | None:
        request = self.pending.pop(client, None)
        if request is None:
            return None
        request.cancelled.set()
        return request

    def expire_locked(self, now: float) -> tuple[PendingInitialJob, ...]:
        expired: list[PendingInitialJob] = []
        for request in tuple(self.pending.values()):
            if (
                request.deadline_monotonic is None
                or request.deadline_monotonic > now
                or self.pending.get(request.client) is not request
            ):
                continue
            self.pending.pop(request.client, None)
            request.cancelled.set()
            # This state transition is the admission fence for a concurrent
            # reauthorization before the coordinator performs socket close.
            request.client.closing = True
            expired.append(request)
        return tuple(expired)

    def shutdown_locked(self) -> tuple[PendingInitialJob, ...]:
        pending = tuple(self.pending.values())
        self.pending.clear()
        for request in pending:
            request.cancelled.set()
        return pending


class InitialJobState:
    """S2-owned first-job configuration, lifecycle, and metrics state."""

    def __init__(
        self,
        config: InitialJobConfig,
        pending: MutableMapping[ClientState, PendingInitialJob] | None = None,
    ) -> None:
        self.config = config
        self.tracker = InitialJobTracker(pending)
        self.queue_rejection_count = 0
        self.timeout_count = 0
        self.cancelled_count = 0
        self.coalesced_count = 0
        self.sent_count = 0
        self.failed_count = 0
        self.superseded_count = 0
        self.queue_capacity_reclaimed_count = 0
        self.delivery_latency_seconds_sum = 0.0
        self.delivery_latency_count = 0
        self.last_delivery_monotonic: float | None = None

    @property
    def pending(self) -> MutableMapping[ClientState, PendingInitialJob]:
        return self.tracker.pending

    def adopt_pending(
        self,
        pending: MutableMapping[ClientState, PendingInitialJob],
    ) -> None:
        self.tracker.adopt(pending)

    def reconfigure(
        self,
        *,
        max_pending: int | None = None,
        timeout_seconds: float | None = None,
        max_workers: int | None = None,
    ) -> None:
        self.config = InitialJobConfig(
            max_pending=(
                self.config.max_pending if max_pending is None else int(max_pending)
            ),
            timeout_seconds=(
                self.config.timeout_seconds
                if timeout_seconds is None
                else float(timeout_seconds)
            ),
            max_workers=(
                self.config.max_workers
                if max_workers is None
                else int(max_workers)
            ),
        )

    def snapshot(self) -> InitialJobSnapshot:
        return InitialJobSnapshot(
            max_workers=self.config.max_workers,
            pending_count=len(self.pending),
            queue_rejection_count=self.queue_rejection_count,
            timeout_count=self.timeout_count,
            cancelled_count=self.cancelled_count,
            coalesced_count=self.coalesced_count,
            sent_count=self.sent_count,
            failed_count=self.failed_count,
            superseded_count=self.superseded_count,
            queue_capacity_reclaimed_count=self.queue_capacity_reclaimed_count,
            delivery_latency_seconds_sum=self.delivery_latency_seconds_sum,
            delivery_latency_count=self.delivery_latency_count,
            last_delivery_monotonic=self.last_delivery_monotonic,
        )


@dataclass(frozen=True)
class DeliveryAuthority:
    """Immutable identity that every delivery path must revalidate.

    ``expected_active_job`` is the context observed before registration.  Once
    registration has happened, ``registered_context`` becomes the final send
    guard.  Authorization and difficulty generations are both intentional:
    unlike health snapshots, delivery authority is generation-sensitive.
    """

    connection_id: int
    authorization_generation: int
    difficulty_generation: int
    worker: WorkerIdentity
    expected_active_job: object | None
    template_fingerprint: str | None
    template_generation: int
    payout_state_generation: int

    @classmethod
    def capture(
        cls,
        client: ClientState,
        *,
        context: object,
        expected_active_job: object | None,
    ) -> DeliveryAuthority:
        worker = client.worker
        if worker is None:
            raise StratumError(20, "client is not authorized")
        return cls(
            connection_id=int(client.connection_id),
            authorization_generation=int(client.authorization_generation),
            difficulty_generation=int(client.difficulty_generation),
            worker=worker,
            expected_active_job=expected_active_job,
            template_fingerprint=getattr(context, "template_fingerprint", None),
            template_generation=int(getattr(context, "template_generation", 0)),
            payout_state_generation=int(
                getattr(context, "payout_state_generation", 0)
            ),
        )

    def client_matches(
        self,
        client: ClientState,
        *,
        active_job: object | None,
    ) -> bool:
        return bool(
            not client.closing
            and int(client.connection_id) == self.connection_id
            and client.subscribed
            and client.authorized
            and client.worker == self.worker
            and int(client.authorization_generation)
            == self.authorization_generation
            and int(client.difficulty_generation) == self.difficulty_generation
            and active_job is self.expected_active_job
        )


class RetainedJobIndex:
    """Bounded, indexed ownership for same-tip and stale-grace contexts.

    The caller follows ``job_update_lock -> SessionRegistry.lock -> lock``.
    This class never performs RPC, socket I/O, or callbacks while ``lock`` is
    held.  Tip-parent information is supplied as an immutable cached value.
    """

    def __init__(
        self,
        *,
        lock: threading.RLock | None = None,
        graveyard: OrderedDict[str, EvictedJobEntry] | None = None,
        by_connection: dict[int, OrderedDict[str, None]] | None = None,
        same_tip_by_connection: dict[int, OrderedDict[str, None]] | None = None,
        same_tip_job_ids: OrderedDict[str, None] | None = None,
        same_tip_ttl_seconds: float = DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_SECONDS,
        same_tip_per_connection: int = (
            DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_PER_CONNECTION
        ),
        stale_grace_seconds: float = DEFAULT_PRISM_STALE_GRACE_SECONDS,
    ) -> None:
        self.lock = lock if lock is not None else threading.RLock()
        converted_graveyard: OrderedDict[str, EvictedJobEntry] = OrderedDict()
        graveyard_requires_conversion = False
        for job_id, entry in (graveyard or {}).items():
            if not isinstance(entry, EvictedJobEntry):
                graveyard_requires_conversion = True
                context, connection_id, evicted_monotonic = entry  # type: ignore[misc]
                entry = EvictedJobEntry(
                    context=context,
                    connection_id=connection_id,
                    evicted_monotonic=evicted_monotonic,
                    previousblockhash=str(context.template["previousblockhash"]),
                )
            converted_graveyard[job_id] = entry
        self.graveyard = (
            graveyard
            if isinstance(graveyard, OrderedDict) and not graveyard_requires_conversion
            else converted_graveyard
        )
        self.by_connection = by_connection if by_connection is not None else {}
        self.same_tip_by_connection = (
            same_tip_by_connection if same_tip_by_connection is not None else {}
        )
        self.same_tip_job_ids = (
            same_tip_job_ids if same_tip_job_ids is not None else OrderedDict()
        )
        self.same_tip_ttl_seconds = float(same_tip_ttl_seconds)
        self.same_tip_per_connection = int(same_tip_per_connection)
        self.stale_grace_seconds = float(stale_grace_seconds)
        self.index_tip_hash: str | None = None
        self.next_prune_monotonic = 0.0
        self.expiration_counts = {name: 0 for name in PRISM_EVICTED_JOB_CLASSES}
        self.capacity_eviction_counts = {
            name: 0 for name in PRISM_EVICTED_JOB_CAPACITY_SCOPES
        }
        self.submit_counts = {
            name: 0 for name in PRISM_EVICTED_JOB_SUBMIT_OUTCOMES
        }

    def adopt(
        self,
        *,
        graveyard: MutableMapping[str, EvictedJobEntry] | None,
        by_connection: dict[int, OrderedDict[str, None]] | None,
        same_tip_by_connection: dict[int, OrderedDict[str, None]] | None,
        same_tip_job_ids: OrderedDict[str, None] | None,
        current_tip: str | None,
    ) -> None:
        """Adopt focused-test/embedding replacements without stale aliases."""
        with self.lock:
            graveyard_replaced = graveyard is not self.graveyard
            if graveyard_replaced:
                converted: OrderedDict[str, EvictedJobEntry] = OrderedDict()
                requires_conversion = False
                for job_id, entry in (graveyard or {}).items():
                    if not isinstance(entry, EvictedJobEntry):
                        requires_conversion = True
                        context, connection_id, evicted_monotonic = entry  # type: ignore[misc]
                        entry = EvictedJobEntry(
                            context=context,
                            connection_id=connection_id,
                            evicted_monotonic=evicted_monotonic,
                            previousblockhash=str(
                                context.template["previousblockhash"]
                            ),
                        )
                    converted[job_id] = entry
                self.graveyard = (
                    graveyard
                    if isinstance(graveyard, OrderedDict) and not requires_conversion
                    else converted
                )
            maps_replaced = False
            if by_connection is not None and by_connection is not self.by_connection:
                self.by_connection = by_connection
                maps_replaced = True
            if (
                same_tip_by_connection is not None
                and same_tip_by_connection is not self.same_tip_by_connection
            ):
                self.same_tip_by_connection = same_tip_by_connection
                maps_replaced = True
            if same_tip_job_ids is not None and same_tip_job_ids is not self.same_tip_job_ids:
                self.same_tip_job_ids = same_tip_job_ids
                maps_replaced = True
            if (
                graveyard_replaced
                or maps_replaced
                or self.index_tip_hash != current_tip
            ):
                self._rebuild_locked(current_tip)

    def _job_class_locked(
        self, entry: EvictedJobEntry, current_tip: str | None
    ) -> str:
        if current_tip is None or entry.previousblockhash == current_tip:
            return "same_tip"
        return "stale_grace"

    def _coerce_entry_locked(
        self,
        job_id: str,
        entry: EvictedJobEntry | object,
    ) -> EvictedJobEntry:
        if isinstance(entry, EvictedJobEntry):
            return entry
        context, connection_id, evicted_monotonic = entry  # type: ignore[misc]
        converted = EvictedJobEntry(
            context=context,
            connection_id=connection_id,
            evicted_monotonic=evicted_monotonic,
            previousblockhash=str(context.template["previousblockhash"]),
        )
        self.graveyard[job_id] = converted
        return converted

    def _remove_locked(self, job_id: str) -> EvictedJobEntry | None:
        entry = self.graveyard.pop(job_id, None)
        if entry is None:
            return None
        for mapping in (self.by_connection, self.same_tip_by_connection):
            connection_jobs = mapping.get(entry.connection_id)
            if connection_jobs is not None:
                connection_jobs.pop(job_id, None)
                if not connection_jobs:
                    mapping.pop(entry.connection_id, None)
        self.same_tip_job_ids.pop(job_id, None)
        return entry

    def _index_locked(
        self,
        job_id: str,
        entry: EvictedJobEntry,
        current_tip: str | None,
    ) -> None:
        self.by_connection.setdefault(entry.connection_id, OrderedDict())[job_id] = None
        if self._job_class_locked(entry, current_tip) != "same_tip":
            return
        self.same_tip_by_connection.setdefault(
            entry.connection_id, OrderedDict()
        )[job_id] = None
        self.same_tip_job_ids[job_id] = None

    def _rebuild_locked(self, current_tip: str | None) -> None:
        self.by_connection.clear()
        self.same_tip_by_connection.clear()
        self.same_tip_job_ids.clear()
        for job_id, entry in tuple(self.graveyard.items()):
            entry = self._coerce_entry_locked(job_id, entry)
            self._index_locked(job_id, entry, current_tip)
        self.index_tip_hash = current_tip
        self._enforce_capacity_locked()

    def _enforce_capacity_locked(self, connection_id: int | None = None) -> None:
        connection_ids = (
            (connection_id,)
            if connection_id is not None
            else tuple(self.same_tip_by_connection)
        )
        for candidate in connection_ids:
            job_ids = self.same_tip_by_connection.get(candidate)
            while job_ids is not None and len(job_ids) > self.same_tip_per_connection:
                self._remove_locked(next(iter(job_ids)))
                self.capacity_eviction_counts["connection"] += 1
                job_ids = self.same_tip_by_connection.get(candidate)

    def retain(
        self,
        client: ClientState,
        job_id: str,
        context: PrismJobContext,
        *,
        current_tip: str | None,
        now: float | None = None,
    ) -> None:
        with self.lock:
            if self.index_tip_hash != current_tip:
                self._rebuild_locked(current_tip)
            self._remove_locked(job_id)
            entry = EvictedJobEntry(
                context=context,
                connection_id=int(client.connection_id),
                evicted_monotonic=time.monotonic() if now is None else now,
                previousblockhash=str(context.template["previousblockhash"]),
                client=client,
            )
            self.graveyard[job_id] = entry
            self._index_locked(job_id, entry, current_tip)
            self._enforce_capacity_locked(client.connection_id)

    def _expired_locked(
        self,
        entry: EvictedJobEntry,
        *,
        now: float,
        current_tip: str | None,
        current_tip_first_delivery: float | None,
        cached_parent: str | None,
    ) -> tuple[str, bool]:
        job_class = self._job_class_locked(entry, current_tip)
        if job_class == "same_tip":
            return (
                job_class,
                self.same_tip_ttl_seconds <= 0
                or now - entry.evicted_monotonic > self.same_tip_ttl_seconds,
            )
        if self.stale_grace_seconds <= 0 or current_tip is None:
            return job_class, True
        if cached_parent is not None and entry.previousblockhash != cached_parent:
            return job_class, True
        client = entry.client
        if client is not None:
            delivered = client.tip_work_delivered
            if delivered is None or delivered[0] != current_tip:
                return job_class, False
            anchor = float(delivered[1])
        elif current_tip_first_delivery is None:
            return job_class, True
        else:
            anchor = current_tip_first_delivery
        return job_class, now - anchor > self.stale_grace_seconds

    def prune(
        self,
        *,
        current_tip: str | None,
        current_tip_first_delivery: float | None,
        cached_parent: str | None,
        now: float | None = None,
        force: bool = True,
    ) -> None:
        with self.lock:
            if self.index_tip_hash != current_tip:
                self._rebuild_locked(current_tip)
            if not self.graveyard:
                return
            now = time.monotonic() if now is None else now
            if not force and now < self.next_prune_monotonic:
                return
            self.next_prune_monotonic = (
                now + DEFAULT_PRISM_EVICTED_JOB_PRUNE_INTERVAL_SECONDS
            )
            for job_id, entry in tuple(self.graveyard.items()):
                entry = self._coerce_entry_locked(job_id, entry)
                job_class, expired = self._expired_locked(
                    entry,
                    now=now,
                    current_tip=current_tip,
                    current_tip_first_delivery=current_tip_first_delivery,
                    cached_parent=cached_parent,
                )
                if expired:
                    self._remove_locked(job_id)
                    self.expiration_counts[job_class] += 1

    def lookup(
        self,
        client: ClientState,
        job_id: str,
        *,
        current_tip: str | None,
        current_tip_first_delivery: float | None,
        cached_parent: str | None,
        now: float | None = None,
    ) -> EvictedJobEntry | None:
        """Constant-time lookup; no scan of the graveyard or client pool."""
        with self.lock:
            if self.index_tip_hash != current_tip:
                self._rebuild_locked(current_tip)
            entry = self.graveyard.get(job_id)
            if entry is None:
                return None
            entry = self._coerce_entry_locked(job_id, entry)
            if entry.connection_id != client.connection_id:
                return None
            job_class, expired = self._expired_locked(
                entry,
                now=time.monotonic() if now is None else now,
                current_tip=current_tip,
                current_tip_first_delivery=current_tip_first_delivery,
                cached_parent=cached_parent,
            )
            if expired:
                self._remove_locked(job_id)
                self.expiration_counts[job_class] += 1
                return None
            return entry

    def peek(self, job_id: str) -> EvictedJobEntry | None:
        """Return one retained entry for explicit compatibility observation."""
        with self.lock:
            entry = self.graveyard.get(job_id)
            if entry is None:
                return None
            return self._coerce_entry_locked(job_id, entry)

    def retire_connection(self, connection_id: int) -> tuple[str, ...]:
        with self.lock:
            retired = tuple(self.by_connection.get(connection_id, ()))
            for job_id in retired:
                self._remove_locked(job_id)
            return retired

    def note_submit(self, credit_policy: str | None) -> None:
        outcome = (
            "credited_stale_grace"
            if credit_policy == PRISM_CREDIT_POLICY_STALE_GRACE
            else "accepted_same_tip"
        )
        with self.lock:
            self.submit_counts[outcome] += 1

    def job_class(
        self,
        entry: EvictedJobEntry,
        *,
        current_tip: str | None,
    ) -> str:
        with self.lock:
            return self._job_class_locked(entry, current_tip)


class JobDeliveryRuntimePort(Protocol):
    def desired_share_difficulty(self, client: ClientState) -> Decimal: ...
    def minimum_advertised_difficulty(self, client: ClientState) -> Decimal: ...
    def share_weight(self, worker: WorkerIdentity) -> int: ...
    def vardiff_config(self, client: ClientState) -> vardiff.VardiffConfig: ...
    def send_difficulty(
        self, client: ClientState, job: direct_stratum.DirectQbitStratumJob
    ) -> None: ...
    def send_job(
        self, client: ClientState, job: direct_stratum.DirectQbitStratumJob
    ) -> None: ...
    def send_job_batch(
        self, client: ClientState, job: direct_stratum.DirectQbitStratumJob
    ) -> None: ...


@dataclass(frozen=True)
class JobDeliveryRuntime(JobDeliveryRuntimePort):
    """Callable-backed narrow port used by the coordinator construction root."""

    desired_share_difficulty_fn: Callable[[ClientState], Decimal]
    minimum_advertised_difficulty_fn: Callable[[ClientState], Decimal]
    share_weight_fn: Callable[[WorkerIdentity], int]
    vardiff_config_fn: Callable[[ClientState], vardiff.VardiffConfig]
    send_difficulty_fn: Callable[
        [ClientState, direct_stratum.DirectQbitStratumJob], None
    ]
    send_job_fn: Callable[[ClientState, direct_stratum.DirectQbitStratumJob], None]
    send_job_batch_fn: Callable[
        [ClientState, direct_stratum.DirectQbitStratumJob], None
    ]

    def desired_share_difficulty(self, client: ClientState) -> Decimal:
        return self.desired_share_difficulty_fn(client)

    def minimum_advertised_difficulty(self, client: ClientState) -> Decimal:
        return self.minimum_advertised_difficulty_fn(client)

    def share_weight(self, worker: WorkerIdentity) -> int:
        return self.share_weight_fn(worker)

    def vardiff_config(self, client: ClientState) -> vardiff.VardiffConfig:
        return self.vardiff_config_fn(client)

    def send_difficulty(
        self,
        client: ClientState,
        job: direct_stratum.DirectQbitStratumJob,
    ) -> None:
        self.send_difficulty_fn(client, job)

    def send_job(
        self,
        client: ClientState,
        job: direct_stratum.DirectQbitStratumJob,
    ) -> None:
        self.send_job_fn(client, job)

    def send_job_batch(
        self,
        client: ClientState,
        job: direct_stratum.DirectQbitStratumJob,
    ) -> None:
        self.send_job_batch_fn(client, job)


@dataclass(frozen=True)
class RetentionAuthority:
    current_tip: str | None
    current_tip_first_delivery: float | None
    cached_parent: str | None


class JobPreparationPort(Protocol):
    def ensure_reorg_current(self) -> bool: ...
    def issuance_artifacts(self) -> CachedTemplateArtifacts: ...
    def shared_bundle(
        self,
        artifacts: CachedTemplateArtifacts,
        worker: WorkerIdentity,
        *,
        cancelled: Callable[[], bool] | None = None,
        request_source: str = "routine",
    ) -> CachedJobBundle: ...
    def artifacts_current(self, artifacts: CachedTemplateArtifacts) -> bool: ...
    def clear_artifacts(self, artifacts: CachedTemplateArtifacts) -> None: ...
    def record_failure(self) -> None: ...
    def phases(self) -> dict[str, float]: ...
    def retained_artifacts(self) -> CachedTemplateArtifacts | None: ...
    def chain_view_untrusted(self) -> bool: ...
    def admit_idle_bundle_source(
        self,
        client: ClientState,
        bundle: CachedJobBundle,
        *,
        allow_uncached: bool,
    ) -> AdmittedIdleBundleSource | None: ...
    def observe_elapsed(
        self,
        elapsed_seconds: float,
        phases: Mapping[str, float],
    ) -> None: ...
    def collection_identity(self, worker: WorkerIdentity) -> object: ...
    def ready_latched(self) -> bool: ...
    def template_fingerprint(self, template: Mapping[str, object]) -> str: ...


class TipAuthorityPort(Protocol):
    def live_tip(self) -> str: ...
    def observe_tip(self, tip_hash: str) -> object: ...
    def published_authority(self) -> tuple[str, float | None] | None: ...
    def published_authoritative(self, now: float) -> bool: ...
    def current_tip_locked(self) -> str | None: ...
    def published_template_locked(self) -> QbitTipTemplateSnapshot | None: ...
    def snapshot_current_locked(
        self,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
    ) -> bool: ...
    def artifacts_parent_current_locked(
        self,
        artifacts: CachedTemplateArtifacts,
    ) -> bool: ...
    def ensure_artifacts_parent_observed(
        self,
        artifacts: CachedTemplateArtifacts,
    ) -> bool: ...
    def schedule_retry(self) -> None: ...
    def prepared_obsolete(
        self,
        validation_token: TipRefreshValidationToken,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        cancel_event: FanoutCancellation | None,
    ) -> bool: ...
    def prepared_token_current_locked(
        self,
        validation_token: TipRefreshValidationToken,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        payout_snapshot: Any,
    ) -> bool: ...
    def record_cancellation(self, stage: str) -> None: ...
    def retention_authority_locked(self) -> RetentionAuthority: ...
    def consume_retained_refresh(self, context: PrismJobContext) -> None: ...
    def published_current_locked(
        self,
        context_parent: str,
        *,
        template_fingerprint: str | None,
        template_generation: int,
        lapsed_live_validated: bool,
        payout_generation: int,
    ) -> bool: ...


class PayoutDeliveryPort(Protocol):
    def snapshot(self) -> Any: ...
    def generation(self) -> int: ...
    def initial_admission(
        self,
        cancelled: Callable[[], bool],
        *,
        generation: int,
    ) -> Any: ...
    def admission(
        self,
        cancelled: Callable[[], bool],
        *,
        generation: int,
        priority: bool,
    ) -> Any: ...
    def observe_admission(
        self,
        admission: object,
        *,
        generation: int,
        fallback_wait_seconds: float,
    ) -> None: ...
    def record_first_delivery(
        self,
        generation: int,
        delivered_monotonic: float,
    ) -> None: ...
class InitialJobRuntimePort(Protocol):
    def stopping(self) -> bool: ...
    def wait(self, timeout: float) -> bool: ...
    def disconnect(self, client: ClientState) -> None: ...
    def submit_initial(
        self,
        function: Callable[[PendingInitialJob], bool],
        request: PendingInitialJob,
        *,
        priority: int,
    ) -> Future[Any]: ...


@dataclass(frozen=True)
class DeliveryCompatibilityHooks:
    """Named legacy monkeypatch resolvers; never a domain dependency port."""

    run_initial_override: Callable[[], Callable[[PendingInitialJob], bool] | None]
    deliver_initial_override: Callable[[], Callable[..., bool | None] | None]
    submit_initial_override: Callable[
        [], Callable[[PendingInitialJob], bool] | None
    ]
    maybe_send_override: Callable[[], Callable[..., bool] | None]
    send_prepared_override: Callable[[], Callable[..., RefreshResult] | None]
    build_job_override: Callable[[], Callable[..., PrismJobContext] | None]
    stamp_job_override: Callable[[], Callable[..., PrismJobContext] | None]
    apply_difficulty_override: Callable[[], Callable[..., None] | None]
    send_update_override: Callable[[], Callable[..., None] | None]
    needs_refresh_override: Callable[[], Callable[..., bool] | None]
    retained_classify_override: Callable[[], Callable[..., str] | None]
    split_send_enabled: Callable[[], bool]
    hot_path_logging_enabled: Callable[[], bool]
    reorg_reconciler_enabled: Callable[[], bool]


class ProgressDeliveryPort(Protocol):
    def record_health_delivery(
        self,
        client: ClientState,
        context: PrismJobContext,
        delivered_monotonic: float,
    ) -> None: ...
    def reconcile_health_eligibility(self) -> None: ...


class JobDeliveryService:
    """Coordinator-free owner of stamping, mutations, guards, and wire pairs."""

    def __init__(
        self,
        *,
        registry: SessionRegistry,
        runtime: JobDeliveryRuntimePort,
        jobs: MutableMapping[str, PrismJobContext],
        retained: RetainedJobIndex,
        preparation: JobPreparationPort | None = None,
        tip_authority: TipAuthorityPort | None = None,
        payout: PayoutDeliveryPort | None = None,
        initial_runtime: InitialJobRuntimePort | None = None,
        hooks: DeliveryCompatibilityHooks | None = None,
        progress: ProgressDeliveryPort | None = None,
        initial_state: InitialJobState | None = None,
        job_counter: int = 0,
        delivery_health_updated: Callable[[str], None] | None = None,
    ) -> None:
        self.registry = registry
        self.runtime = runtime
        self.jobs = jobs
        self.retained = retained
        self.preparation = preparation
        self.tip_authority = tip_authority
        self.payout = payout
        self.initial_runtime = initial_runtime
        self.hooks = hooks
        self.progress = progress
        self.delivery_health_updated = delivery_health_updated
        self.initial_state = initial_state or InitialJobState(
            InitialJobConfig(max_pending=0, timeout_seconds=0.0)
        )
        self._job_counter_lock = threading.Lock()
        self._job_counter = int(job_counter)
        self._send_override_local = threading.local()
        self._initial_executor_lock = threading.Lock()
        self._initial_executor: _BoundedPriorityExecutor | None = None
        self._initial_executor_shutdown = False

    def adopt_ports(
        self,
        *,
        preparation: JobPreparationPort,
        tip_authority: TipAuthorityPort,
        payout: PayoutDeliveryPort,
        initial_runtime: InitialJobRuntimePort,
        hooks: DeliveryCompatibilityHooks,
        progress: ProgressDeliveryPort,
        initial_state: InitialJobState,
        delivery_health_updated: Callable[[str], None] | None = None,
    ) -> None:
        self.preparation = preparation
        self.tip_authority = tip_authority
        self.payout = payout
        self.initial_runtime = initial_runtime
        self.hooks = hooks
        self.progress = progress
        self.initial_state = initial_state
        self.delivery_health_updated = delivery_health_updated

    @staticmethod
    def _required(port: object | None, name: str) -> Any:
        if port is None:
            raise RuntimeError(f"job delivery {name} port is not configured")
        return port

    def initial_snapshot(self) -> InitialJobSnapshot:
        with self.registry.lock:
            return self.initial_state.snapshot()

    def initial_executor(self) -> _BoundedPriorityExecutor:
        with self._initial_executor_lock:
            if self._initial_executor_shutdown:
                raise RuntimeError("initial job executor is shut down")
            executor = self._initial_executor
            if executor is None:
                executor = _BoundedPriorityExecutor(
                    max_workers=self.initial_state.config.max_workers,
                    max_queue_size=self.initial_state.config.max_pending,
                    thread_name_prefix="prism-initial-job-delivery",
                )
                self._initial_executor = executor
            return executor

    def initial_executor_stats(self) -> tuple[int, int]:
        with self._initial_executor_lock:
            executor = self._initial_executor
        return (0, 0) if executor is None else executor.stats()

    def cancel_initial_future(self, future: Future[Any]) -> bool:
        with self._initial_executor_lock:
            executor = self._initial_executor
        reclaimed = bool(executor is not None and executor.cancel(future))
        if executor is None:
            future.cancel()
        if reclaimed:
            with self.registry.lock:
                self.initial_state.queue_capacity_reclaimed_count += 1
        return reclaimed

    def shutdown_initial_executor(self) -> None:
        self.shutdown_initial_jobs()
        with self._initial_executor_lock:
            executor = self._initial_executor
            self._initial_executor = None
            self._initial_executor_shutdown = True
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)

    @property
    def job_counter(self) -> int:
        with self._job_counter_lock:
            return self._job_counter

    def adopt_job_counter(self, value: int) -> None:
        with self._job_counter_lock:
            self._job_counter = int(value)

    def next_job_id(self) -> str:
        with self._job_counter_lock:
            self._job_counter += 1
            return f"prism-{self._job_counter}"

    def initial_request_current_locked(self, request: PendingInitialJob) -> bool:
        initial_runtime = self._required(self.initial_runtime, "initial runtime")
        return self.initial_state.tracker.request_current_locked(
            request,
            clients=self.registry.clients,
            stopping=initial_runtime.stopping(),
        )

    def initial_request_cancelled(self, request: PendingInitialJob) -> bool:
        initial_runtime = self._required(self.initial_runtime, "initial runtime")
        if request.cancelled.is_set() or initial_runtime.stopping():
            return True
        with self.registry.lock:
            return not self.initial_request_current_locked(request)

    def cancel_initial_job_locked(
        self,
        client: ClientState,
        *,
        count: bool,
    ) -> PendingInitialJob | None:
        request = self.initial_state.tracker.cancel_locked(client)
        if request is not None and count:
            self.initial_state.cancelled_count += 1
        return request

    def cancel_initial_job(
        self,
        client: ClientState,
        *,
        count: bool,
    ) -> PendingInitialJob | None:
        with self.registry.lock:
            request = self.cancel_initial_job_locked(client, count=count)
        if request is not None and request.future is not None:
            self.cancel_initial_future(request.future)
        return request

    def shutdown_initial_jobs_locked(self) -> tuple[PendingInitialJob, ...]:
        return self.initial_state.tracker.shutdown_locked()

    def shutdown_initial_jobs(self) -> tuple[PendingInitialJob, ...]:
        with self.registry.lock:
            pending = self.shutdown_initial_jobs_locked()
            self.initial_state.cancelled_count += len(pending)
        for request in pending:
            if request.future is not None:
                self.cancel_initial_future(request.future)
        return pending

    def current_job_source(self) -> CurrentJobSource:
        payout = self._required(self.payout, "payout")
        tip = self._required(self.tip_authority, "tip authority")
        return CurrentJobSource(
            payout_generation=payout.generation(),
            current_tip=tip.current_tip_locked(),
            published_template=tip.published_template_locked(),
        )

    def reauthorization_has_capacity(self, client: ClientState) -> bool:
        """Preserve live work when no superseding initial-job slot exists."""
        source = self.current_job_source()
        with self.registry.lock:
            return not (
                client not in self.initial_state.pending
                and len(self.initial_state.pending)
                >= self.initial_state.config.max_pending
                and self.client_has_current_tip_job_locked(client, source)
            )

    @staticmethod
    def client_has_current_tip_job_locked(
        client: ClientState,
        source: CurrentJobSource,
    ) -> bool:
        context = client.active_job
        if context is None:
            return False
        generation = source.payout_generation
        if int(getattr(context, "payout_state_generation", generation)) != generation:
            return False
        current_tip = source.current_tip
        if current_tip is None:
            return False
        snapshot = source.published_template
        if snapshot is None:
            return str(context.template.get("previousblockhash", "")) == current_tip
        if snapshot.bestblockhash != current_tip or snapshot.template_artifacts is None:
            return False
        return bool(
            str(context.template.get("previousblockhash", "")) == current_tip
            and getattr(context, "template_fingerprint", None)
            == snapshot.template_fingerprint
            and int(getattr(context, "template_generation", 0))
            == snapshot.template_generation
            and context.template is snapshot.template_artifacts.template
            and int(getattr(context, "connection_id", client.connection_id))
            == client.connection_id
            and int(getattr(context, "authorization_generation", 0))
            == int(client.authorization_generation)
            and int(getattr(context, "difficulty_generation", 0))
            == int(client.difficulty_generation)
        )

    @staticmethod
    def client_has_delivered_work_locked(client: ClientState) -> bool:
        """Return whether a socket write completed for any usable job."""

        return bool(
            client.tip_work_delivered is not None
            or client._progress_delivered_context is not None
        )

    def note_initial_job_delivered(
        self,
        client: ClientState,
        *,
        validated_current: bool = False,
    ) -> None:
        source = None if validated_current else self.current_job_source()
        future: Future[bool] | None = None
        with self.registry.lock:
            if (
                source is not None
                and not self.client_has_current_tip_job_locked(client, source)
            ):
                return
            request = self.initial_state.pending.pop(client, None)
            if request is None:
                return
            request.cancelled.set()
            future = request.future
            delivered = time.monotonic()
            self.initial_state.sent_count += 1
            self.initial_state.delivery_latency_seconds_sum += max(
                0.0,
                delivered - request.requested_monotonic,
            )
            self.initial_state.delivery_latency_count += 1
            self.initial_state.last_delivery_monotonic = delivered
        if future is not None:
            self.cancel_initial_future(future)

    def schedule_initial_job(self, client: ClientState) -> bool:
        initial_runtime = self._required(self.initial_runtime, "initial runtime")
        hooks = self._required(self.hooks, "compatibility hooks")
        maybe_send_override = hooks.maybe_send_override()
        if callable(maybe_send_override):
            return bool(maybe_send_override(client, clean_jobs=True))

        now = time.monotonic()
        reject = False
        deferred = False
        request: PendingInitialJob | None = None
        superseded_future: Future[bool] | None = None
        already_current = False
        source = self.current_job_source()
        with self.registry.lock:
            if (
                not client.subscribed
                or not client.authorized
                or client.worker is None
                or client.closing
            ):
                return True
            generation = int(client.authorization_generation)
            difficulty_generation = int(client.difficulty_generation)
            existing = self.initial_state.pending.get(client)
            if (
                existing is not None
                and existing.connection_id == client.connection_id
                and existing.authorization_generation == generation
                and existing.difficulty_generation == difficulty_generation
                and existing.worker == client.worker
            ):
                self.initial_state.coalesced_count += 1
                return True
            if existing is not None:
                existing.cancelled.set()
                superseded_future = existing.future
                self.initial_state.cancelled_count += 1
                self.initial_state.superseded_count += 1
            if self.client_has_current_tip_job_locked(client, source):
                if existing is not None:
                    self.initial_state.pending.pop(client, None)
                already_current = True
            if not already_current:
                if (
                    existing is None
                    and len(self.initial_state.pending)
                    >= self.initial_state.config.max_pending
                ):
                    self.initial_state.queue_rejection_count += 1
                    reject = True
                else:
                    timeout = self.initial_state.config.timeout_seconds
                    predecessor = None
                    if existing is not None:
                        for candidate in (existing.future, existing.predecessor):
                            if candidate is not None and not candidate.done():
                                predecessor = candidate
                                break
                    request = PendingInitialJob(
                        client=client,
                        connection_id=client.connection_id,
                        authorization_generation=generation,
                        difficulty_generation=difficulty_generation,
                        worker=client.worker,
                        requested_monotonic=now,
                        deadline_monotonic=now + timeout if timeout > 0 else None,
                        predecessor=predecessor,
                    )
                    self.initial_state.pending[client] = request
                    deferred = predecessor is not None
        if superseded_future is not None:
            self.cancel_initial_future(superseded_future)
        if already_current:
            return True
        if reject or request is None:
            initial_runtime.disconnect(client)
            return False
        if deferred:
            return True
        submit_override = hooks.submit_initial_override()
        if callable(submit_override):
            return bool(submit_override(request))
        return self.submit_initial_job_request(request)

    def submit_initial_job_request(self, request: PendingInitialJob) -> bool:
        initial_runtime = self._required(self.initial_runtime, "initial runtime")
        hooks = self._required(self.hooks, "compatibility hooks")
        client = request.client
        run_override = hooks.run_initial_override()
        run = (
            run_override
            if callable(run_override)
            else lambda pending: self.run_initial_job(pending)
        )
        try:
            future = self.initial_executor().submit(
                run,
                request,
                priority=PRISM_DELIVERY_PRIORITY_INITIAL,
            )
        except (_DeliveryQueueFull, RuntimeError):
            disconnect = False
            with self.registry.lock:
                if self.initial_state.pending.get(client) is request:
                    self.initial_state.pending.pop(client, None)
                    request.cancelled.set()
                    self.initial_state.queue_rejection_count += 1
                    disconnect = True
            if disconnect:
                initial_runtime.disconnect(client)
            return not disconnect
        cancel_future = False
        with self.registry.lock:
            if self.initial_state.pending.get(client) is request:
                request.future = future
            else:
                cancel_future = True
        future.add_done_callback(
            lambda completed: self.initial_job_future_finished(request, completed)
        )
        if cancel_future:
            self.cancel_initial_future(future)
        return True

    def initial_job_future_finished(
        self,
        request: PendingInitialJob,
        future: Future[bool],
    ) -> None:
        initial_runtime = self._required(self.initial_runtime, "initial runtime")
        preparation = self._required(self.preparation, "preparation")
        hooks = self._required(self.hooks, "compatibility hooks")
        delivered = False
        if not future.cancelled():
            try:
                delivered = bool(future.result())
            except Exception:
                preparation.record_failure()
                print(
                    "prism coordinator: initial job task failed "
                    f"connection={request.client.connection_id}",
                    flush=True,
                )
                traceback.print_exc()

        disconnect = False
        replacement: PendingInitialJob | None = None
        source = self.current_job_source() if delivered else None
        with self.registry.lock:
            current = self.initial_state.pending.get(request.client)
            if current is not request:
                if (
                    current is not None
                    and current.future is None
                    and current.predecessor is future
                ):
                    current.predecessor = None
                    replacement = current
            elif (
                source is not None
                and self.initial_request_current_locked(request)
                and self.client_has_current_tip_job_locked(request.client, source)
            ):
                self.initial_state.pending.pop(request.client, None)
                request.cancelled.set()
                self.initial_state.last_delivery_monotonic = time.monotonic()
            else:
                self.initial_state.pending.pop(request.client, None)
                request.cancelled.set()
                if (
                    request.deadline_monotonic is not None
                    and request.deadline_monotonic <= time.monotonic()
                ):
                    request.client.closing = True
                    self.initial_state.timeout_count += 1
                    self.initial_state.cancelled_count += 1
                else:
                    self.initial_state.failed_count += 1
                disconnect = True
        if replacement is not None:
            submit_override = hooks.submit_initial_override()
            if callable(submit_override):
                submit_override(replacement)
            else:
                self.submit_initial_job_request(replacement)
        if disconnect:
            initial_runtime.disconnect(request.client)

    def run_initial_job(self, request: PendingInitialJob) -> bool:
        initial_runtime = self._required(self.initial_runtime, "initial runtime")
        preparation = self._required(self.preparation, "preparation")
        tip = self._required(self.tip_authority, "tip authority")
        hooks = self._required(self.hooks, "compatibility hooks")
        retry_delay = 0.05
        last_failure_log_monotonic: float | None = None

        def retry_later() -> bool:
            nonlocal retry_delay
            request.cancelled.wait(retry_delay)
            retry_delay = min(1.0, retry_delay * 2)
            return not self.initial_request_cancelled(request)

        try:
            while not self.initial_request_cancelled(request):
                try:
                    if not preparation.ensure_reorg_current():
                        if not retry_later():
                            return False
                        continue
                    artifacts = preparation.issuance_artifacts()
                    if self.initial_request_cancelled(request):
                        return False
                    bundle = preparation.shared_bundle(
                        artifacts,
                        request.worker,
                        cancelled=lambda: (
                            self.initial_request_cancelled(request)
                            or not preparation.artifacts_current(artifacts)
                        ),
                        request_source="initial",
                    )
                    live_tip = tip.live_tip()
                    if artifacts.previousblockhash != live_tip:
                        tip.observe_tip(live_tip)
                        published = tip.published_authority()
                        pinned_authoritative = bool(
                            published is not None
                            and artifacts.previousblockhash == published[0]
                            and tip.published_authoritative(time.monotonic())
                        )
                        if not pinned_authoritative:
                            preparation.clear_artifacts(artifacts)
                            if not retry_later():
                                return False
                            continue
                except (JobBuildWaiterCancelled, TemplateRefreshBlocked):
                    if self.initial_request_cancelled(request):
                        return False
                    if not retry_later():
                        return False
                    continue
                except Exception:
                    preparation.record_failure()
                    now = time.monotonic()
                    if (
                        last_failure_log_monotonic is None
                        or now - last_failure_log_monotonic >= 5.0
                    ):
                        last_failure_log_monotonic = now
                        print(
                            "prism coordinator: initial job preparation failed "
                            f"connection={request.client.connection_id}; retrying",
                            flush=True,
                        )
                        traceback.print_exc()
                    if not retry_later():
                        return False
                    continue
                deliver_override = hooks.deliver_initial_override()
                if callable(deliver_override):
                    delivered = deliver_override(request, artifacts, bundle)
                else:
                    delivered = self.deliver_initial_bundle(request, artifacts, bundle)
                if delivered is None:
                    if not retry_later():
                        return False
                    continue
                return bool(delivered)
            return False
        except OSError:
            initial_runtime.disconnect(request.client)
            return False

    @staticmethod
    def _acquire_client_job_lock(
        client: ClientState,
        cancelled: Callable[[], bool],
    ) -> bool:
        while not cancelled():
            if client.job_update_lock.acquire(timeout=0.1):
                return True
        return False

    def _prune_retained_locked(
        self,
        *,
        authority: RetentionAuthority,
        now: float | None = None,
        force: bool,
    ) -> None:
        self.retained.prune(
            current_tip=authority.current_tip,
            current_tip_first_delivery=authority.current_tip_first_delivery,
            cached_parent=authority.cached_parent,
            now=time.monotonic() if now is None else now,
            force=force,
        )

    def prune_retained(
        self,
        *,
        now: float | None = None,
        force: bool = True,
    ) -> None:
        tip = self._required(self.tip_authority, "tip authority")
        authority = tip.retention_authority_locked()
        with self.registry.lock:
            self._prune_retained_locked(
                authority=authority,
                now=now,
                force=force,
            )

    def bury_retained(
        self,
        client: ClientState,
        job_id: str,
        *,
        now: float | None = None,
        prune: bool = True,
    ) -> None:
        tip = self._required(self.tip_authority, "tip authority")
        authority = tip.retention_authority_locked()
        with self.registry.lock:
            context = self.jobs.get(job_id)
            if context is None:
                return
            self.retained.retain(
                client,
                job_id,
                context,
                current_tip=authority.current_tip,
                now=now,
            )
            if prune:
                self._prune_retained_locked(
                    authority=authority,
                    now=now,
                    force=False,
                )

    def retained_entry(
        self,
        client: ClientState,
        job_id: str,
        *,
        now: float | None = None,
    ) -> EvictedJobEntry | None:
        tip = self._required(self.tip_authority, "tip authority")
        hooks = self._required(self.hooks, "compatibility hooks")
        existing = self.retained.peek(job_id)
        classify_override = hooks.retained_classify_override()
        if existing is not None and callable(classify_override):
            classify_override(existing)
        authority = tip.retention_authority_locked()
        with self.registry.lock:
            return self.retained.lookup(
                client,
                job_id,
                current_tip=authority.current_tip,
                current_tip_first_delivery=authority.current_tip_first_delivery,
                cached_parent=authority.cached_parent,
                now=time.monotonic() if now is None else now,
            )

    def note_retained_submit(self, credit_policy: str | None) -> None:
        self.retained.note_submit(credit_policy)

    def retained_job_class(self, entry: EvictedJobEntry) -> str:
        tip = self._required(self.tip_authority, "tip authority")
        authority = tip.retention_authority_locked()
        return self.retained.job_class(entry, current_tip=authority.current_tip)

    def deliver_initial_bundle(
        self,
        request: PendingInitialJob,
        artifacts: CachedTemplateArtifacts,
        bundle: CachedJobBundle,
    ) -> bool | None:
        preparation = self._required(self.preparation, "preparation")
        payout = self._required(self.payout, "payout")
        tip = self._required(self.tip_authority, "tip authority")
        client = request.client

        def cancelled() -> bool:
            return self.initial_request_cancelled(request)

        if not self._acquire_client_job_lock(client, cancelled):
            return False
        try:
            if cancelled():
                return False
            if not preparation.artifacts_current(artifacts):
                return None
            gate_started = time.monotonic()
            with payout.initial_admission(
                cancelled,
                generation=bundle.payout_state_generation,
            ) as admitted:
                payout.observe_admission(
                    admitted,
                    generation=bundle.payout_state_generation,
                    fallback_wait_seconds=time.monotonic() - gate_started,
                )
                if not admitted or cancelled():
                    return False
                if (
                    bundle.payout_state_generation != payout.generation()
                    or not preparation.artifacts_current(artifacts)
                ):
                    return None
                with client_vardiff_lock(client):
                    context = self._stamp_for_client(
                        client,
                        bundle,
                        clean_jobs=True,
                    )
                retention_authority = tip.retention_authority_locked()
                source_authority = DeliverySourceAuthority(
                    kind="artifacts",
                    payout_generation=bundle.payout_state_generation,
                    template_generation=artifacts.generation,
                    observation_sequence=0,
                    template_fingerprint=artifacts.fingerprint,
                    artifacts=artifacts,
                )
                with client_vardiff_lock(client), self.registry.lock:
                    if not self.source_authority_current_locked(
                        source_authority,
                        context,
                    ):
                        return None
                    if not self.initial_request_current_locked(request):
                        return False
                    if not self.context_matches_client_locked(client, context):
                        return False
                    authority = self.capture_authority(
                        client,
                        context,
                        expected_active_job=client.active_job,
                    )
                    if not self.authority_current_locked(
                        client,
                        authority,
                        expected_active_job=client.active_job,
                    ):
                        return False
                    self.register_locked(
                        client,
                        context,
                        clean_jobs=True,
                        current_tip=retention_authority.current_tip,
                    )
                    self._prune_retained_locked(
                        authority=retention_authority,
                        force=False,
                    )

                self.send_update(client, context.job, split_send=self._split_send())
                delivered_monotonic = time.monotonic()
                if not self.complete_delivery(
                    client,
                    authority,
                    context,
                    delivered_monotonic,
                    initial_request=request,
                    source_authorities=(source_authority,),
                ):
                    return False
                mark_delivered = getattr(admitted, "mark_delivered", None)
                if callable(mark_delivered):
                    mark_delivered()
                self._apply_delivered_difficulty(client, context.job)
                self.note_tip_work_delivered(
                    client,
                    str(context.template["previousblockhash"]),
                )
                payout.record_first_delivery(
                    bundle.payout_state_generation,
                    delivered_monotonic,
                )
                self.note_initial_job_delivered(client, validated_current=True)
                return True
        finally:
            client.job_update_lock.release()

    def sweep_initial_job_timeouts(self, *, now: float | None = None) -> int:
        initial_runtime = self._required(self.initial_runtime, "initial runtime")
        now = time.monotonic() if now is None else now
        with self.registry.lock:
            timed_out = self.initial_state.tracker.expire_locked(now)
            self.initial_state.timeout_count += len(timed_out)
            self.initial_state.cancelled_count += len(timed_out)
        for request in timed_out:
            if request.future is not None:
                self.cancel_initial_future(request.future)
            initial_runtime.disconnect(request.client)
        return len(timed_out)

    def initial_job_timeout_loop(self) -> None:
        initial_runtime = self._required(self.initial_runtime, "initial runtime")
        while not initial_runtime.wait(1.0):
            self.sweep_initial_job_timeouts()

    def _split_send(self) -> bool:
        hooks = self._required(self.hooks, "compatibility hooks")
        return hooks.split_send_enabled()

    def _apply_delivered_difficulty(
        self,
        client: ClientState,
        job: direct_stratum.DirectQbitStratumJob,
    ) -> None:
        hooks = self._required(self.hooks, "compatibility hooks")
        override = hooks.apply_difficulty_override()
        if callable(override):
            override(client, job)
            return
        self.apply_job_difficulty(
            client,
            job,
            config=self.runtime.vardiff_config(client),
        )

    def _stamp_for_client(
        self,
        client: ClientState,
        bundle: CachedJobBundle,
        *,
        clean_jobs: bool,
    ) -> PrismJobContext:
        hooks = self._required(self.hooks, "compatibility hooks")
        override = hooks.stamp_job_override()
        if callable(override):
            return override(client, bundle, clean_jobs=clean_jobs)
        return self.stamp(client, bundle, clean_jobs=clean_jobs)

    def build_job_for_client(
        self,
        client: ClientState,
        *,
        clean_jobs: bool,
    ) -> PrismJobContext:
        preparation = self._required(self.preparation, "preparation")
        if client.worker is None:
            raise StratumError(20, "client is not authorized")
        artifacts = (
            preparation.retained_artifacts()
            or preparation.issuance_artifacts()
        )
        return self.build_job_for_client_from_artifacts(
            client,
            artifacts,
            clean_jobs=clean_jobs,
        )

    def build_job_for_client_from_artifacts(
        self,
        client: ClientState,
        artifacts: CachedTemplateArtifacts,
        *,
        clean_jobs: bool,
    ) -> PrismJobContext:
        preparation = self._required(self.preparation, "preparation")
        phases = preparation.phases()
        while True:
            worker = client.worker
            if worker is None:
                raise StratumError(20, "client is not authorized")
            cached_bundle = preparation.shared_bundle(artifacts, worker)
            current_worker = client.worker
            if not cached_bundle.collection_only or current_worker == worker:
                break
        stamp_started = time.monotonic()
        context = self._stamp_for_client(
            client,
            cached_bundle,
            clean_jobs=clean_jobs,
        )
        phases["stamp"] = phases.get("stamp", 0.0) + (
            time.monotonic() - stamp_started
        )
        return context

    def maybe_send_job(
        self,
        client: ClientState,
        *,
        clean_jobs: bool,
        raise_on_reorg_failure: bool = False,
        raise_on_build_failure: bool = False,
        tip_refresh_snapshot: QbitTipTemplateSnapshot | None = None,
        tip_refresh_observation_sequence: int | None = None,
    ) -> bool:
        with client.job_update_lock:
            return self.maybe_send_job_locked(
                client,
                clean_jobs=clean_jobs,
                raise_on_reorg_failure=raise_on_reorg_failure,
                raise_on_build_failure=raise_on_build_failure,
                tip_refresh_snapshot=tip_refresh_snapshot,
                tip_refresh_observation_sequence=tip_refresh_observation_sequence,
            )

    def idle_authority_current_locked(
        self,
        client: ClientState,
        authority: IdleDeliveryAuthority,
    ) -> bool:
        return bool(
            client in self.registry.clients
            and not client.closing
            and self.client_can_receive_jobs(client)
            and client.connection_id == authority.connection_id
            and client.worker == authority.worker
            and client.active_job is authority.expected_active_job
            and client.vardiff_window_started_monotonic
            == authority.expected_window_started
            and client.vardiff_window_accepted == 0
            and client.vardiff_window_submitted == 0
            and client.pending_share_difficulty == authority.pending_difficulty
        )

    @staticmethod
    def commit_idle_authority_locked(
        client: ClientState,
        authority: IdleDeliveryAuthority,
    ) -> None:
        reset_at = time.monotonic()
        client.vardiff_window_started_monotonic = reset_at
        client.vardiff_window_accepted = 0
        client.vardiff_window_submitted = 0
        client.vardiff_window_work = Decimal("0")
        authority.committed_reset_monotonic = reset_at

    def maybe_send_job_locked(
        self,
        client: ClientState,
        *,
        clean_jobs: bool,
        raise_on_reorg_failure: bool = False,
        raise_on_build_failure: bool = False,
        tip_refresh_snapshot: QbitTipTemplateSnapshot | None = None,
        tip_refresh_observation_sequence: int | None = None,
        prepared_bundle: CachedJobBundle | None = None,
        idle_authority: IdleDeliveryAuthority | None = None,
        prepared_bundle_allow_uncached: bool = False,
    ) -> bool:
        preparation = self._required(self.preparation, "preparation")
        tip = self._required(self.tip_authority, "tip authority")
        payout = self._required(self.payout, "payout")
        hooks = self._required(self.hooks, "compatibility hooks")
        if not client.subscribed or not client.authorized or client.worker is None:
            return False
        started = time.monotonic()
        phases = preparation.phases()
        phases.clear()
        if hooks.hot_path_logging_enabled():
            print(
                "prism coordinator: building job "
                f"connection={client.connection_id} username={client.username}",
                flush=True,
            )
        phase_started = time.monotonic()
        guarded_refresh = tip_refresh_snapshot is not None
        if guarded_refresh != (tip_refresh_observation_sequence is not None):
            raise ValueError("tip refresh snapshot and observation sequence must be paired")
        if prepared_bundle is not None and guarded_refresh:
            raise ValueError("prepared idle bundles cannot be combined with tip refresh guards")
        if prepared_bundle_allow_uncached and prepared_bundle is None:
            raise ValueError("uncached prepared delivery requires a prepared bundle")
        if prepared_bundle is not None:
            pass
        elif guarded_refresh:
            assert tip_refresh_snapshot is not None
            assert tip_refresh_observation_sequence is not None
            with self.registry.lock:
                refresh_current = tip.snapshot_current_locked(
                    tip_refresh_snapshot,
                    tip_refresh_observation_sequence,
                )
            if not refresh_current:
                tip.schedule_retry()
                raise TemplateRefreshSuperseded(
                    "tip refresh snapshot was superseded before client job build"
                )
            try:
                chain_view_untrusted = bool(
                    hooks.reorg_reconciler_enabled()
                    and preparation.chain_view_untrusted()
                )
            except Exception as exc:
                tip.schedule_retry()
                raise TemplateRefreshBlocked(
                    "qbit chain trust check failed before sequential client job build"
                ) from exc
            if chain_view_untrusted:
                tip.schedule_retry()
                raise TemplateRefreshBlocked(
                    "qbit chain view became untrusted before sequential client job build"
                )
        else:
            try:
                if not preparation.ensure_reorg_current():
                    if raise_on_reorg_failure:
                        raise TemplateRefreshBlocked(
                            "qbit chain view became untrusted before client job build"
                        )
                    return False
            except TemplateRefreshBlocked:
                raise
            except Exception as exc:
                print(
                    "prism coordinator: reorg reconciliation failed before job build "
                    f"connection={client.connection_id} username={client.username}; "
                    "skipping this job",
                    flush=True,
                )
                traceback.print_exc()
                if raise_on_reorg_failure:
                    raise TemplateRefreshBlocked(
                        "reorg reconciliation failed before client job build"
                    ) from exc
                return False
        phases["reorg"] = time.monotonic() - phase_started
        build_override = hooks.build_job_override()
        built_from_guarded_artifacts = bool(
            guarded_refresh
            and tip_refresh_snapshot.template_artifacts is not None
            and build_override is None
        )
        selected_source_artifacts: CachedTemplateArtifacts | None = None
        try:
            if prepared_bundle is not None:
                context = self._stamp_for_client(
                    client,
                    prepared_bundle,
                    clean_jobs=clean_jobs,
                )
            elif built_from_guarded_artifacts:
                assert tip_refresh_snapshot is not None
                assert tip_refresh_snapshot.template_artifacts is not None
                context = self.build_job_for_client_from_artifacts(
                    client,
                    tip_refresh_snapshot.template_artifacts,
                    clean_jobs=clean_jobs,
                )
            elif callable(build_override):
                context = build_override(client, clean_jobs=clean_jobs)
            else:
                selected_source_artifacts = (
                    preparation.retained_artifacts()
                    or preparation.issuance_artifacts()
                )
                context = self.build_job_for_client_from_artifacts(
                    client,
                    selected_source_artifacts,
                    clean_jobs=clean_jobs,
                )
        except TemplateRefreshBlocked:
            tip.schedule_retry()
            if guarded_refresh or raise_on_reorg_failure or raise_on_build_failure:
                raise
            return False
        except Exception as exc:
            preparation.record_failure()
            print(
                "prism coordinator: job build failed "
                f"connection={client.connection_id} username={client.username}; "
                "keeping client connected and skipping this template",
                flush=True,
            )
            traceback.print_exc()
            if raise_on_build_failure:
                raise JobBuildFailed(
                    f"job build failed for connection {client.connection_id}"
                ) from exc
            return False

        payout_snapshot = payout.snapshot()
        current_payout_generation = payout_snapshot.generation
        published_tip = payout_snapshot.published.source_tip_hash
        publication_blocked = payout_snapshot.publication_blocked
        context_payout_generation = int(
            getattr(context, "payout_state_generation", current_payout_generation)
        )
        context_template = getattr(context, "template", None)
        context_parent = (
            str(context_template.get("previousblockhash", ""))
            if isinstance(context_template, dict)
            else ""
        )
        published_authority = tip.published_authority()
        published_authoritative = tip.published_authoritative(time.monotonic())
        pinned_published_delivery = bool(
            context_parent
            and published_authority is not None
            and context_parent == published_authority[0]
            and published_authoritative
        )
        lapsed_live_validated = False
        if (
            not guarded_refresh
            and context_parent
            and published_authority is not None
            and not published_authoritative
        ):
            try:
                lapsed_live_tip = tip.live_tip()
            except Exception:
                lapsed_live_tip = None
            if lapsed_live_tip is not None:
                tip.observe_tip(lapsed_live_tip)
                if context_parent != lapsed_live_tip:
                    tip.schedule_retry()
                    return False
                lapsed_live_validated = True
        priority_delivery = (
            not publication_blocked
            and context_payout_generation == current_payout_generation
            and (
                published_tip is None
                or context_parent == published_tip
                or pinned_published_delivery
            )
        )
        payout_gate_started = time.monotonic()
        with payout.admission(
            lambda: context_payout_generation
            != payout.generation(),
            generation=context_payout_generation,
            priority=priority_delivery,
        ) as payout_admitted:
            payout_gate_wait = max(0.0, time.monotonic() - payout_gate_started)
            phases["payout_gate"] = phases.get("payout_gate", 0.0) + payout_gate_wait
            payout.observe_admission(
                payout_admitted,
                generation=context_payout_generation,
                fallback_wait_seconds=payout_gate_wait,
            )
            if not payout_admitted:
                tip.schedule_retry()
                if guarded_refresh:
                    raise TemplateRefreshSuperseded(
                        "payout state changed during client job build"
                    )
                return False

            authority: DeliveryAuthority | None = None
            retention_authority = tip.retention_authority_locked()
            published_commit = tip.published_authority()
            published_commit_authoritative = (
                published_commit is not None
                and tip.published_authoritative(time.monotonic())
            )
            if guarded_refresh:
                assert tip_refresh_snapshot is not None
                assert tip_refresh_observation_sequence is not None
                guarded_commit_current = tip.snapshot_current_locked(
                    tip_refresh_snapshot,
                    tip_refresh_observation_sequence,
                )
                if not guarded_commit_current:
                    tip.schedule_retry()
                    raise TemplateRefreshSuperseded(
                        "tip refresh snapshot was superseded during client job build"
                    )
            elif (
                published_commit is not None
                and context_parent
                and (
                    (
                        published_commit_authoritative
                        and context_parent != published_commit[0]
                    )
                    or (
                        not published_commit_authoritative
                        and not lapsed_live_validated
                    )
                )
            ):
                tip.schedule_retry()
                return False

            source_authority: DeliverySourceAuthority | None
            if prepared_bundle is not None:
                source_authority = None
            elif guarded_refresh:
                assert tip_refresh_snapshot is not None
                assert tip_refresh_observation_sequence is not None
                source_authority = DeliverySourceAuthority(
                    kind="tip_snapshot",
                    payout_generation=context_payout_generation,
                    template_generation=tip_refresh_snapshot.template_generation,
                    observation_sequence=tip_refresh_observation_sequence,
                    template_fingerprint=(
                        tip_refresh_snapshot.template_fingerprint
                    ),
                    snapshot=tip_refresh_snapshot,
                )
            elif selected_source_artifacts is not None:
                source_authority = DeliverySourceAuthority(
                    kind="artifacts",
                    payout_generation=context_payout_generation,
                    template_generation=selected_source_artifacts.generation,
                    observation_sequence=0,
                    template_fingerprint=selected_source_artifacts.fingerprint,
                    artifacts=selected_source_artifacts,
                )
            else:
                source_authority = DeliverySourceAuthority(
                    kind="published_tip",
                    payout_generation=context_payout_generation,
                    template_generation=int(
                        getattr(context, "template_generation", 0)
                    ),
                    observation_sequence=0,
                    template_fingerprint=getattr(
                        context,
                        "template_fingerprint",
                        None,
                    ),
                    context_parent=context_parent,
                    lapsed_live_validated=lapsed_live_validated,
                )

            def commit_context_locked() -> bool:
                nonlocal authority
                if client not in self.registry.clients or client.closing:
                    return False
                if not self.context_matches_client_locked(client, context):
                    return False
                if isinstance(context, PrismJobContext):
                    if source_authority is None or not (
                        self.source_authority_current_locked(
                            source_authority,
                            context,
                        )
                    ):
                        return False
                if (
                    idle_authority is not None
                    and not self.idle_authority_current_locked(
                        client,
                        idle_authority,
                    )
                ):
                    return False
                if guarded_refresh:
                    assert tip_refresh_snapshot is not None
                    artifacts = tip_refresh_snapshot.template_artifacts
                    if built_from_guarded_artifacts and artifacts is not None and (
                        context.template is not artifacts.template
                        or context.template_fingerprint != artifacts.fingerprint
                        or context.template_generation != artifacts.generation
                    ):
                        raise TemplateRefreshBlocked(
                            "client job build did not use the guarded refresh artifacts"
                        )
                authority = self.capture_authority(
                    client,
                    context,
                    expected_active_job=client.active_job,
                )
                self.register_locked(
                    client,
                    context,
                    clean_jobs=clean_jobs,
                    current_tip=retention_authority.current_tip,
                )
                if clean_jobs:
                    self._prune_retained_locked(
                        authority=retention_authority,
                        force=False,
                    )
                if idle_authority is not None:
                    self.commit_idle_authority_locked(
                        client,
                        idle_authority,
                    )
                return True

            if prepared_bundle is not None:
                admitted_source = preparation.admit_idle_bundle_source(
                    client,
                    prepared_bundle,
                    allow_uncached=prepared_bundle_allow_uncached,
                )
                if (
                    admitted_source is None
                    or admitted_source.bundle is not prepared_bundle
                    or admitted_source.cache_identity != prepared_bundle.key
                    or admitted_source.allow_uncached
                    != prepared_bundle_allow_uncached
                ):
                    return False
                selected_source_artifacts = admitted_source.artifacts
                if not tip.ensure_artifacts_parent_observed(
                    selected_source_artifacts
                ):
                    return False
                source_authority = DeliverySourceAuthority(
                    kind="artifacts",
                    payout_generation=context_payout_generation,
                    template_generation=selected_source_artifacts.generation,
                    observation_sequence=0,
                    template_fingerprint=selected_source_artifacts.fingerprint,
                    artifacts=selected_source_artifacts,
                )
                with client_vardiff_lock(client), self.registry.lock:
                    if not commit_context_locked():
                        return False
            else:
                with client_vardiff_lock(client), self.registry.lock:
                    if not commit_context_locked():
                        return False
            phase_started = time.monotonic()
            self.send_update(client, context.job, split_send=self._split_send())
            delivered_monotonic = time.monotonic()
            if authority is None or not self.complete_delivery(
                client,
                authority,
                context,
                delivered_monotonic,
                source_authorities=(
                    (source_authority,)
                    if (
                        isinstance(context, PrismJobContext)
                        and source_authority is not None
                    )
                    else ()
                ),
            ):
                return False
            mark_delivered = getattr(payout_admitted, "mark_delivered", None)
            if callable(mark_delivered):
                mark_delivered()
            self._apply_delivered_difficulty(client, context.job)
            self.note_tip_work_delivered(
                client,
                str(context.template["previousblockhash"]),
            )
            payout.record_first_delivery(
                context_payout_generation,
                delivered_monotonic,
            )
            tip.consume_retained_refresh(context)
            self.note_initial_job_delivered(
                client,
                validated_current=guarded_refresh,
            )
            phases["send"] = delivered_monotonic - phase_started
            elapsed = time.monotonic() - started
            preparation.observe_elapsed(elapsed, phases)
            if hooks.hot_path_logging_enabled():
                phase_report = ",".join(
                    f"{phase}:{seconds:.3f}" for phase, seconds in phases.items()
                )
                print(
                    "prism coordinator: sent job "
                    f"connection={client.connection_id} username={client.username} "
                    f"job={context.job.job_id} collection={context.collection_only} "
                    f"elapsed={elapsed:.3f}s phases={phase_report}",
                    flush=True,
                )
            return True

    def send_prepared_job(
        self,
        client: ClientState,
        bundle: CachedJobBundle,
        snapshot: QbitTipTemplateSnapshot,
        validation_token: TipRefreshValidationToken,
        expected_connection_id: int,
        expected_active_job: PrismJobContext | None,
        cancel_event: FanoutCancellation | None = None,
        submitted_monotonic: float | None = None,
    ) -> RefreshResult:
        preparation = self._required(self.preparation, "preparation")
        tip = self._required(self.tip_authority, "tip authority")
        payout = self._required(self.payout, "payout")
        hooks = self._required(self.hooks, "compatibility hooks")
        worker_started = time.monotonic()
        started = worker_started if submitted_monotonic is None else submitted_monotonic
        phases = preparation.phases()
        phases.clear()

        def cancelled() -> bool:
            return tip.prepared_obsolete(
                validation_token,
                bundle,
                snapshot,
                cancel_event,
            ) or client.closing

        phases["executor_queue"] = max(0.0, worker_started - started)
        client_lock_started = worker_started
        client_lock_acquired = False
        client_lock_attempted = False
        try:
            while True:
                with self.registry.lock:
                    if (
                        client not in self.registry.clients
                        or client.connection_id != expected_connection_id
                        or client.closing
                    ):
                        return RefreshResult("disconnected")
                if cancelled():
                    phases["client_lock"] = max(
                        0.0,
                        time.monotonic() - client_lock_started,
                    )
                    tip.record_cancellation(
                        "client_lock" if client_lock_attempted else "executor_queue"
                    )
                    return RefreshResult("skipped")
                client_lock_attempted = True
                client_lock_acquired = client.job_update_lock.acquire(
                    timeout=PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS
                )
                if client_lock_acquired:
                    break
            phases["client_lock"] = max(
                0.0,
                time.monotonic() - client_lock_started,
            )
            if cancelled():
                tip.record_cancellation("client_lock")
                return RefreshResult("skipped")
            refresh_source = self.refresh_source()
            with self.registry.lock:
                if (
                    client not in self.registry.clients
                    or client.connection_id != expected_connection_id
                ):
                    return RefreshResult("disconnected")
                if (
                    not self.client_can_receive_jobs(client)
                    or self.intervening_supersedes(
                        client.active_job,
                        expected_active_job,
                        snapshot,
                        refresh_source,
                    )
                    or not self.client_needs_refresh_locked(
                        client, snapshot, refresh_source
                    )
                ):
                    return RefreshResult("skipped")
            payout_gate_started = time.monotonic()
            with payout.admission(
                cancelled,
                generation=bundle.payout_state_generation,
                priority=True,
            ) as payout_admitted:
                phases["payout_gate"] = max(
                    0.0,
                    time.monotonic() - payout_gate_started,
                )
                payout.observe_admission(
                    payout_admitted,
                    generation=bundle.payout_state_generation,
                    fallback_wait_seconds=phases["payout_gate"],
                )
                if not payout_admitted or cancelled():
                    tip.record_cancellation("payout_gate")
                    return RefreshResult("skipped")
                fanout_admitted = cancel_event is None or cancel_event.begin_delivery()
                if not fanout_admitted:
                    tip.record_cancellation("payout_gate")
                    return RefreshResult("skipped")
                try:
                    payout_snapshot = payout.snapshot()
                    token_current = tip.prepared_token_current_locked(
                        validation_token,
                        bundle,
                        snapshot,
                        payout_snapshot,
                    )
                    if not token_current:
                        if cancel_event is not None:
                            cancel_event.cancel()
                        return RefreshResult("skipped")
                    refresh_source = self.refresh_source()
                    with self.registry.lock:
                        if (
                            client not in self.registry.clients
                            or client.connection_id != expected_connection_id
                        ):
                            return RefreshResult("disconnected")
                        if (
                            not self.client_can_receive_jobs(client)
                            or self.intervening_supersedes(
                                client.active_job,
                                expected_active_job,
                                snapshot,
                                refresh_source,
                            )
                            or not self.client_needs_refresh_locked(
                                client, snapshot, refresh_source
                            )
                        ):
                            return RefreshResult("skipped")
                        clean_jobs = self.tip_changed(
                            client, snapshot, refresh_source
                        )

                    stamp_started = time.monotonic()
                    with client_vardiff_lock(client):
                        context = self._stamp_for_client(
                            client,
                            bundle,
                            clean_jobs=clean_jobs,
                        )
                    phases["stamp"] = time.monotonic() - stamp_started
                    retention_authority = tip.retention_authority_locked()
                    refresh_source = self.refresh_source()
                    source_authority = DeliverySourceAuthority(
                        kind="tip_token",
                        payout_generation=validation_token.payout_state_generation,
                        template_generation=snapshot.template_generation,
                        observation_sequence=validation_token.observation_sequence,
                        template_fingerprint=snapshot.template_fingerprint,
                        snapshot=snapshot,
                        token=validation_token,
                        bundle=bundle,
                        payout_snapshot=payout_snapshot,
                    )
                    authority: DeliveryAuthority | None = None

                    def register_current() -> str:
                        nonlocal authority
                        with client_vardiff_lock(client), self.registry.lock:
                            if not self.source_authority_current_locked(
                                source_authority,
                                context,
                            ):
                                return "source"
                            if (
                                client not in self.registry.clients
                                or client.connection_id != expected_connection_id
                            ):
                                return "disconnected"
                            if (
                                not self.client_can_receive_jobs(client)
                                or self.intervening_supersedes(
                                    client.active_job,
                                    expected_active_job,
                                    snapshot,
                                    refresh_source,
                                )
                                or not self.client_needs_refresh_locked(
                                    client, snapshot, refresh_source
                                )
                                or not self.context_matches_client_locked(
                                    client, context
                                )
                            ):
                                return "skipped"
                            authority = self.capture_authority(
                                client,
                                context,
                                expected_active_job=client.active_job,
                            )
                            self.register_locked(
                                client,
                                context,
                                clean_jobs=clean_jobs,
                                current_tip=retention_authority.current_tip,
                            )
                            if clean_jobs:
                                self._prune_retained_locked(
                                    authority=retention_authority,
                                    force=False,
                                )
                            return "registered"

                    registration = register_current()
                    if registration == "source":
                        if cancel_event is not None:
                            cancel_event.cancel()
                        return RefreshResult("skipped")
                    if registration != "registered":
                        return RefreshResult(registration)

                    socket_send_started = time.monotonic()
                    try:
                        self.send_update(
                            client,
                            context.job,
                            split_send=self._split_send(),
                        )
                    finally:
                        socket_send_finished = time.monotonic()
                        phases["socket_send"] = max(
                            0.0,
                            socket_send_finished - socket_send_started,
                        )
                    delivered_monotonic = time.monotonic()
                    if not self.complete_delivery(
                        client,
                        authority,
                        context,
                        delivered_monotonic,
                        source_authorities=(source_authority,),
                    ):
                        with self.registry.lock:
                            connected = (
                                client in self.registry.clients
                                and client.connection_id == expected_connection_id
                                and not client.closing
                            )
                        return RefreshResult(
                            "skipped" if connected else "disconnected"
                        )
                    mark_delivered = getattr(payout_admitted, "mark_delivered", None)
                    if callable(mark_delivered):
                        mark_delivered()
                    self._apply_delivered_difficulty(client, context.job)
                    self.note_tip_work_delivered(
                        client,
                        str(context.template["previousblockhash"]),
                    )
                    self.note_initial_job_delivered(
                        client,
                        validated_current=True,
                    )
                    payout.record_first_delivery(
                        context.payout_state_generation,
                        delivered_monotonic,
                    )
                    if hooks.hot_path_logging_enabled():
                        print(
                            "prism coordinator: sent prepared job "
                            f"connection={client.connection_id} "
                            f"username={client.username} job={context.job.job_id} "
                            f"elapsed={delivered_monotonic - started:.3f}s",
                            flush=True,
                        )
                    return RefreshResult("sent", delivered_monotonic)
                finally:
                    if cancel_event is not None:
                        cancel_event.end_delivery()
        finally:
            if client_lock_acquired:
                client.job_update_lock.release()
            preparation.observe_elapsed(
                max(0.0, time.monotonic() - started),
                phases,
            )

    def advertise_client_difficulty(
        self,
        client: ClientState,
        target: Decimal,
    ) -> bool:
        with client.job_update_lock:
            return self.advertise_client_difficulty_locked(client, target)

    def advertise_client_difficulty_locked(
        self,
        client: ClientState,
        target: Decimal,
    ) -> bool:
        hooks = self._required(self.hooks, "compatibility hooks")
        initial_runtime = self._required(self.initial_runtime, "initial runtime")
        applied_directly = False
        schedule_initial = False
        with client_vardiff_lock(client):
            current = client.pending_share_difficulty or client.share_difficulty
            if target == current:
                return False
            if not (client.subscribed and client.authorized) or (
                client.active_job is None
                and hooks.maybe_send_override() is None
            ):
                client.share_difficulty = target
                client.pending_share_difficulty = None
                client.difficulty_generation = int(client.difficulty_generation) + 1
                applied_directly = True
                schedule_initial = bool(
                    client.subscribed
                    and client.authorized
                    and client.worker is not None
                )
            else:
                prior_pending = client.pending_share_difficulty
                prior_generation = int(client.difficulty_generation)
                advertised_generation = prior_generation + 1
                client.pending_share_difficulty = target
                client.difficulty_generation = advertised_generation
        if applied_directly:
            if schedule_initial:
                self.schedule_initial_job(client)
            return False
        with self.registry.lock:
            initial_pending = client in self.initial_state.pending
        if initial_pending:
            self.schedule_initial_job(client)
            return False
        maybe_send_override = hooks.maybe_send_override()
        if callable(maybe_send_override):
            sent = bool(maybe_send_override(client, clean_jobs=True))
        else:
            sent = bool(
                not initial_runtime.stopping()
                and self.maybe_send_job(client, clean_jobs=True)
            )
        if sent:
            return True
        with client_vardiff_lock(client):
            if (
                client.pending_share_difficulty == target
                and int(client.difficulty_generation) == advertised_generation
            ):
                client.pending_share_difficulty = prior_pending
                client.difficulty_generation = prior_generation
        return False

    def adopt_jobs(self, jobs: MutableMapping[str, PrismJobContext]) -> None:
        self.jobs = jobs

    def active_context_locked(
        self,
        client: ClientState,
        job_id: str,
    ) -> PrismJobContext | None:
        if job_id not in client.active_job_ids:
            return None
        return self.jobs.get(job_id)

    @staticmethod
    def client_can_receive_jobs(client: ClientState) -> bool:
        return session_client_can_receive_jobs(client)

    def refresh_source(self) -> RefreshSource:
        preparation = self._required(self.preparation, "preparation")
        payout = self._required(self.payout, "payout")
        return RefreshSource(
            ready_latched=preparation.ready_latched(),
            payout_generation=payout.generation(),
        )

    def client_needs_refresh(
        self,
        client: ClientState,
        snapshot: QbitTipTemplateSnapshot,
    ) -> bool:
        return self.client_needs_refresh_locked(
            client,
            snapshot,
            self.refresh_source(),
        )

    def client_needs_refresh_locked(
        self,
        client: ClientState,
        snapshot: QbitTipTemplateSnapshot,
        source: RefreshSource,
    ) -> bool:
        context = client.active_job
        if context is None:
            return True
        if getattr(context, "collection_only", False) and source.ready_latched:
            return True
        template = context.template
        previousblockhash = str(template.get("previousblockhash", ""))
        context_fingerprint = getattr(context, "template_fingerprint", None)
        if context_fingerprint is None:
            preparation = self._required(self.preparation, "preparation")
            context_fingerprint = preparation.template_fingerprint(template)
        return bool(
            previousblockhash != snapshot.bestblockhash
            or previousblockhash != snapshot.previousblockhash
            or context_fingerprint != snapshot.template_fingerprint
            or int(getattr(context, "payout_state_generation", 0))
            != source.payout_generation
        )

    def intervening_supersedes(
        self,
        active_job: PrismJobContext | None,
        expected_active_job: PrismJobContext | None,
        snapshot: QbitTipTemplateSnapshot,
        source: RefreshSource | None = None,
    ) -> bool:
        if source is None:
            source = self.refresh_source()
        if active_job is expected_active_job or active_job is None:
            return False
        if int(active_job.payout_state_generation) < source.payout_generation:
            return False
        active_parent = str(active_job.template.get("previousblockhash", ""))
        if (
            active_parent != snapshot.bestblockhash
            or active_parent != snapshot.previousblockhash
        ):
            return False
        active_generation = int(active_job.template_generation)
        snapshot_generation = int(snapshot.template_generation)
        if active_generation <= 0 or snapshot_generation <= 0:
            return True
        return active_generation >= snapshot_generation

    def tip_changed(
        self,
        client: ClientState,
        snapshot: QbitTipTemplateSnapshot,
        source: RefreshSource | None = None,
    ) -> bool:
        if source is None:
            source = self.refresh_source()
        context = client.active_job
        if context is None:
            return True
        previousblockhash = str(context.template.get("previousblockhash", ""))
        return bool(
            previousblockhash != snapshot.bestblockhash
            or previousblockhash != snapshot.previousblockhash
            or int(getattr(context, "payout_state_generation", 0))
            != source.payout_generation
        )

    def stamp(
        self,
        client: ClientState,
        cached: CachedJobBundle,
        *,
        clean_jobs: bool,
    ) -> PrismJobContext:
        worker = client.worker
        if worker is None:
            raise StratumError(20, "client is not authorized")
        preparation = self._required(self.preparation, "preparation")
        collection_identity = preparation.collection_identity(worker)
        if cached.collection_only and cached.collection_identity != collection_identity:
            raise StratumError(
                20,
                "collection bundle payout identity no longer matches client authorization",
            )
        share_target = direct_stratum.effective_share_target(
            self.runtime.desired_share_difficulty(client),
            cached.base_job.qbit_target,
            minimum_advertised_difficulty=(
                self.runtime.minimum_advertised_difficulty(client)
            ),
        )
        job = dataclass_replace(
            cached.base_job,
            job_id=self.next_job_id(),
            extranonce1_hex=client.extranonce1_hex,
            share_target=share_target,
            share_difficulty=direct_stratum.target_difficulty(share_target),
            clean_jobs=clean_jobs,
        )
        return PrismJobContext(
            job=job,
            template=cached.template,
            shares_json=cached.shares_json,
            prior_balances=cached.prior_balances,
            found_block=cached.found_block,
            share_weight=self.runtime.share_weight(worker),
            collection_only=cached.collection_only,
            worker=worker,
            issued_at_ms=cached.issued_at_ms,
            template_fingerprint=cached.template_fingerprint,
            template_generation=cached.template_generation,
            payout_state_generation=cached.payout_state_generation,
            prospective_prior_balances=cached.prospective_prior_balances,
            payout_artifact_generation=cached.payout_artifact_generation,
            connection_id=client.connection_id,
            authorization_generation=int(client.authorization_generation),
            difficulty_generation=int(client.difficulty_generation),
        )

    def send_update(
        self,
        client: ClientState,
        job: direct_stratum.DirectQbitStratumJob,
        *,
        split_send: bool,
    ) -> None:
        """Send one adjacent set_difficulty/mining.notify pair."""
        if split_send:
            self.runtime.send_difficulty(client, job)
            self.runtime.send_job(client, job)
        else:
            override = (
                None
                if self.hooks is None
                or bool(getattr(self._send_override_local, "active", False))
                else self.hooks.send_update_override()
            )
            if callable(override):
                self._send_override_local.active = True
                try:
                    override(client, job)
                finally:
                    self._send_override_local.active = False
            else:
                self.runtime.send_job_batch(client, job)

    @staticmethod
    def apply_job_difficulty(
        client: ClientState,
        job: direct_stratum.DirectQbitStratumJob,
        *,
        config: vardiff.VardiffConfig,
    ) -> None:
        with client_vardiff_lock(client):
            if not config.enabled:
                client.share_difficulty = job.share_difficulty
                client.pending_share_difficulty = None
                return
            pending = client.pending_share_difficulty
            client.share_difficulty = job.share_difficulty
            if pending is not None and job.share_difficulty == pending:
                client.pending_share_difficulty = None

    @staticmethod
    def apply_client_difficulty_requests(
        client: ClientState,
        *,
        base: vardiff.VardiffConfig,
    ) -> Decimal | None:
        """Resolve d=/md=/suggest inputs without owning vardiff policy."""
        with client_vardiff_lock(client):
            requested = (
                client.requested_difficulty
                if client.requested_difficulty is not None
                else client.suggested_difficulty
            )
            if requested is None and client.requested_min_difficulty is None:
                client.vardiff_config = None
                return None
            floor = base.min_difficulty
            if client.requested_min_difficulty is not None:
                floor = vardiff.clamp(
                    client.requested_min_difficulty,
                    base.min_difficulty,
                    base.max_difficulty,
                )
            if requested is None:
                requested = client.share_difficulty
            target = vardiff.clamp(requested, floor, base.max_difficulty)
            client.vardiff_config = dataclass_replace(
                base,
                min_difficulty=floor,
                startup_difficulty=target,
            )
            return target

    def capture_authority(
        self,
        client: ClientState,
        context: PrismJobContext,
        *,
        expected_active_job: object | None,
    ) -> DeliveryAuthority:
        return DeliveryAuthority.capture(
            client,
            context=context,
            expected_active_job=expected_active_job,
        )

    def authority_current_locked(
        self,
        client: ClientState,
        authority: DeliveryAuthority,
        *,
        expected_active_job: object | None,
    ) -> bool:
        return bool(
            client in self.registry.clients
            and authority.client_matches(client, active_job=expected_active_job)
        )

    @staticmethod
    def context_matches_client_locked(
        client: ClientState,
        context: PrismJobContext,
    ) -> bool:
        return bool(
            getattr(context, "worker", client.worker) == client.worker
            and int(getattr(context, "connection_id", client.connection_id))
            == int(client.connection_id)
            and int(
                getattr(
                    context,
                    "authorization_generation",
                    client.authorization_generation,
                )
            )
            == int(client.authorization_generation)
            and int(
                getattr(
                    context,
                    "difficulty_generation",
                    client.difficulty_generation,
                )
            )
            == int(client.difficulty_generation)
        )

    def final_delivery_guard_locked(
        self,
        client: ClientState,
        authority: DeliveryAuthority,
        context: PrismJobContext,
    ) -> bool:
        return bool(
            client in self.registry.clients
            and not client.closing
            and int(client.connection_id) == authority.connection_id
            and client.subscribed
            and client.authorized
            and client.worker == authority.worker
            and int(client.authorization_generation)
            == authority.authorization_generation
            and int(client.difficulty_generation) == authority.difficulty_generation
            and client.active_job is context
            and getattr(context, "template_fingerprint", None)
            == authority.template_fingerprint
            and int(getattr(context, "template_generation", 0))
            == authority.template_generation
            and int(getattr(context, "payout_state_generation", 0))
            == authority.payout_state_generation
        )

    def register_locked(
        self,
        client: ClientState,
        context: PrismJobContext,
        *,
        clean_jobs: bool,
        current_tip: str | None,
    ) -> tuple[str, ...]:
        retired = self.registry.register_active_job_locked(
            client,
            context,
            job_id=context.job.job_id,
            clean_jobs=clean_jobs,
        )
        if clean_jobs:
            for job_id in retired:
                retired_context = self.jobs.pop(job_id, None)
                if retired_context is not None:
                    self.retained.retain(
                        client,
                        job_id,
                        retired_context,
                        current_tip=current_tip,
                    )
        self.jobs[context.job.job_id] = context
        self.prune_active_locked(client, current_tip=current_tip)
        return retired

    def prune_active_locked(
        self,
        client: ClientState,
        *,
        current_tip: str | None,
    ) -> None:
        for job_id in tuple(client.active_job_ids):
            if job_id not in self.jobs:
                client.active_job_ids.discard(job_id)
        ordered = [job_id for job_id in self.jobs if job_id in client.active_job_ids]
        while len(ordered) > MAX_ACTIVE_PRISM_JOBS_PER_CLIENT:
            job_id = ordered.pop(0)
            client.active_job_ids.remove(job_id)
            context = self.jobs.pop(job_id, None)
            if context is not None:
                self.retained.retain(
                    client,
                    job_id,
                    context,
                    current_tip=current_tip,
                )

    def prune_active(self, client: ClientState) -> None:
        tip = self._required(self.tip_authority, "tip authority")
        authority = tip.retention_authority_locked()
        with self.registry.lock:
            self.prune_active_locked(
                client,
                current_tip=authority.current_tip,
            )

    def record_successful_delivery(
        self,
        client: ClientState,
        authority: DeliveryAuthority,
        context: PrismJobContext,
        delivered_monotonic: float,
    ) -> bool:
        """Final guard and S1 proof commit, after a successful socket send."""
        with self.registry.lock:
            return self._record_successful_delivery_locked(
                client,
                authority,
                context,
                delivered_monotonic,
            )

    def _record_successful_delivery_locked(
        self,
        client: ClientState,
        authority: DeliveryAuthority,
        context: PrismJobContext,
        delivered_monotonic: float,
    ) -> bool:
        if not self.final_delivery_guard_locked(client, authority, context):
            return False
        if client._progress_delivered_context is context:
            return False
        return self.registry.record_delivery_locked(
            client,
            context,
            delivered_monotonic,
        )

    def source_authority_current_locked(
        self,
        source: DeliverySourceAuthority,
        context: PrismJobContext,
    ) -> bool:
        """Validate an immutable source identity under the S1/R1 lock."""
        tip = self._required(self.tip_authority, "tip authority")
        context_payout_generation = int(
            getattr(
                context,
                "payout_state_generation",
                source.payout_generation,
            )
        )
        context_template_generation = int(
            getattr(context, "template_generation", source.template_generation)
        )
        context_template_fingerprint = getattr(
            context,
            "template_fingerprint",
            source.template_fingerprint,
        )
        if (
            context_payout_generation != source.payout_generation
            or context_template_generation != source.template_generation
            or (
                source.template_fingerprint is not None
                and context_template_fingerprint != source.template_fingerprint
            )
        ):
            return False
        if source.kind == "artifacts":
            artifacts = source.artifacts
            if artifacts is None:
                return False
            return bool(
                context.template is artifacts.template
                and context_template_fingerprint == artifacts.fingerprint
                and context_template_generation == artifacts.generation
                and tip.artifacts_parent_current_locked(artifacts)
            )
        if source.kind == "tip_snapshot":
            snapshot = source.snapshot
            if snapshot is None:
                return False
            return bool(
                context_template_fingerprint == snapshot.template_fingerprint
                and context_template_generation == snapshot.template_generation
                and str(context.template.get("previousblockhash", ""))
                == snapshot.previousblockhash
                and tip.snapshot_current_locked(
                    snapshot,
                    source.observation_sequence,
                )
            )
        if source.kind == "tip_token":
            if (
                source.token is None
                or source.bundle is None
                or source.snapshot is None
                or source.payout_snapshot is None
            ):
                return False
            return tip.prepared_token_current_locked(
                source.token,
                source.bundle,
                source.snapshot,
                source.payout_snapshot,
            )
        if source.kind == "published_tip":
            return tip.published_current_locked(
                source.context_parent,
                template_fingerprint=source.template_fingerprint,
                template_generation=source.template_generation,
                lapsed_live_validated=source.lapsed_live_validated,
                payout_generation=source.payout_generation,
            )
        return False

    def complete_delivery(
        self,
        client: ClientState,
        authority: DeliveryAuthority,
        context: PrismJobContext,
        delivered_monotonic: float,
        *,
        initial_request: PendingInitialJob | None = None,
        source_authorities: tuple[DeliverySourceAuthority, ...] = (),
    ) -> bool:
        """Commit exact S1 proof, then notify G1 after releasing the lock."""
        progress = self._required(self.progress, "progress")
        with self.registry.lock:
            if any(
                not self.source_authority_current_locked(source, context)
                for source in source_authorities
            ):
                return False
            if (
                initial_request is not None
                and not self.initial_request_current_locked(initial_request)
            ):
                return False
            if not self._record_successful_delivery_locked(
                client,
                authority,
                context,
                delivered_monotonic,
            ):
                return False
        progress.record_health_delivery(client, context, delivered_monotonic)
        progress.reconcile_health_eligibility()
        return True

    def note_tip_work_delivered(
        self,
        client: ClientState,
        job_parent_hash: str,
    ) -> None:
        """Anchor stale grace at the first successful delivery for each tip."""
        now = time.monotonic()
        with self.registry.lock:
            delivered = client.tip_work_delivered
            if delivered is None or delivered[0] != job_parent_hash:
                client.tip_work_delivered = (job_parent_hash, now)
            if self.delivery_health_updated is not None:
                self.delivery_health_updated(job_parent_hash)

    def retire_client_locked(self, client: ClientState) -> tuple[str, ...]:
        retired_active = self.registry.clear_active_jobs_locked(client)
        for job_id in retired_active:
            self.jobs.pop(job_id, None)
        self.retained.retire_connection(client.connection_id)
        return retired_active


class JobDeliveryTipRefreshPort:
    """R1 delivery interface backed by S2 ownership, without coordinator context."""

    def __init__(
        self,
        *,
        registry: SessionRegistry | Callable[[], SessionRegistry],
        delivery: JobDeliveryService,
        submit_task: Callable[..., Future[RefreshResult]],
        disconnect: Callable[[ClientState], None],
    ) -> None:
        self._registry = registry
        self.delivery = delivery
        self._submit_task = submit_task
        self._disconnect = disconnect

    def _refresh_source(self) -> RefreshSource:
        return self.delivery.refresh_source()

    def _needs_refresh(
        self,
        client: ClientState,
        snapshot: QbitTipTemplateSnapshot,
        source: RefreshSource,
    ) -> bool:
        hooks = self.delivery.hooks
        override = None if hooks is None else hooks.needs_refresh_override()
        if callable(override):
            return bool(override(client, snapshot))
        return self.delivery.client_needs_refresh_locked(client, snapshot, source)

    def _tip_changed_snapshot(
        self,
        client: ClientState,
        snapshot: QbitTipTemplateSnapshot,
        source: RefreshSource,
    ) -> bool:
        return self.delivery.tip_changed(client, snapshot, source)

    @property
    def registry(self) -> SessionRegistry:
        if callable(self._registry):
            return self._registry()
        return self._registry

    def eligible_clients(self) -> tuple[object, ...]:
        with self.registry.lock:
            return tuple(
                client
                for client in self.registry.clients
                if self.delivery.client_can_receive_jobs(client)
            )

    def client_can_receive_jobs(self, client: object) -> bool:
        return self.delivery.client_can_receive_jobs(client)  # type: ignore[arg-type]

    def client_needs_refresh(
        self,
        client: object,
        snapshot: QbitTipTemplateSnapshot,
    ) -> bool:
        source = self._refresh_source()
        return self._needs_refresh(client, snapshot, source)  # type: ignore[arg-type]

    def active_job(self, client: object) -> object | None:
        with self.registry.lock:
            return client.active_job  # type: ignore[attr-defined]

    def connection_id(self, client: object) -> int:
        return int(client.connection_id)  # type: ignore[attr-defined]

    def delivery_priority(
        self,
        client: object,
        snapshot: QbitTipTemplateSnapshot,
        expected_active_job: object | None,
    ) -> int:
        if expected_active_job is None:
            return PRISM_DELIVERY_PRIORITY_INITIAL
        source = self._refresh_source()
        if self._tip_changed_snapshot(client, snapshot, source):  # type: ignore[arg-type]
            return PRISM_DELIVERY_PRIORITY_NEW_TIP
        return PRISM_DELIVERY_PRIORITY_SAME_TIP

    def submit_task(
        self,
        executor: object,
        fn: Callable[..., RefreshResult],
        *args: object,
        priority: int,
    ) -> Future[RefreshResult]:
        return self._submit_task(executor, fn, *args, priority=priority)

    def send_prepared_job(self, *args: object) -> RefreshResult:
        hooks = self.delivery.hooks
        override = None if hooks is None else hooks.send_prepared_override()
        if callable(override):
            return override(*args)
        return self.delivery.send_prepared_job(*args)  # type: ignore[arg-type]

    def disconnect(self, client: object) -> None:
        self._disconnect(client)  # type: ignore[arg-type]

    @staticmethod
    def log_identity(client: object) -> str:
        return (
            f"connection={getattr(client, 'connection_id', 'unknown')} "
            f"username={getattr(client, 'username', '')}"
        )

    def select_targets(
        self,
        snapshot: QbitTipTemplateSnapshot,
        *,
        refresh_all: bool,
    ) -> tuple[RefreshClientTarget, ...]:
        source = self._refresh_source()
        with self.registry.lock:
            candidates = tuple(
                (client, client.active_job)
                for client in self.registry.clients
                if self.delivery.client_can_receive_jobs(client)
            )
        return tuple(
            RefreshClientTarget(client, active_job)
            for client, active_job in candidates
            if refresh_all or self._needs_refresh(client, snapshot, source)
        )

    def merge_poll_start_targets(
        self,
        targets: tuple[RefreshClientTarget, ...],
        poll_start_clients: tuple[object, ...],
        snapshot: QbitTipTemplateSnapshot,
        *,
        refresh_all: bool,
    ) -> tuple[RefreshClientTarget, ...]:
        source = self._refresh_source()
        with self.registry.lock:
            connected = {
                client: getattr(client, "active_job", None)
                for client in self.registry.clients
            }
        merged = list(targets)
        selected = {target.client for target in targets}
        for candidate in poll_start_clients:
            client = candidate
            if client in selected or client not in connected:
                continue
            if refresh_all or self._needs_refresh(  # type: ignore[arg-type]
                client, snapshot, source
            ):
                merged.append(RefreshClientTarget(client, connected[client]))
                selected.add(client)
        return tuple(merged)

    def revalidate_targets(
        self,
        targets: tuple[RefreshClientTarget, ...],
        snapshot: QbitTipTemplateSnapshot,
    ) -> tuple[tuple[RefreshClientTarget, ...], tuple[str, ...]]:
        current: list[RefreshClientTarget] = []
        dropped: list[str] = []
        source = self._refresh_source()
        with self.registry.lock:
            connected = set(self.registry.clients)
        for target in targets:
            client = target.client
            if client not in connected:
                dropped.append("disconnected")
            elif not self.delivery.client_can_receive_jobs(  # type: ignore[arg-type]
                client
            ):
                dropped.append("skipped")
            elif not self._needs_refresh(  # type: ignore[arg-type]
                client, snapshot, source
            ):
                dropped.append("skipped")
            else:
                current.append(target)
        return tuple(current), tuple(dropped)

    def deliver_collection(
        self,
        client: object,
        snapshot: QbitTipTemplateSnapshot,
        observation_sequence: int,
    ) -> RefreshResult:
        source = self._refresh_source()
        with self.registry.lock:
            connected = client in self.registry.clients
        if not connected:
            return RefreshResult("disconnected")
        eligible = self.delivery.client_can_receive_jobs(  # type: ignore[arg-type]
            client
        )
        if not eligible:
            return RefreshResult("skipped")
        try:
            hooks = self.delivery.hooks
            override = None if hooks is None else hooks.maybe_send_override()
            send = override if callable(override) else self.delivery.maybe_send_job
            if send(
                client,
                clean_jobs=self._tip_changed_snapshot(  # type: ignore[arg-type]
                    client, snapshot, source
                ),
                raise_on_reorg_failure=True,
                raise_on_build_failure=True,
                tip_refresh_snapshot=snapshot,
                tip_refresh_observation_sequence=observation_sequence,
            ):
                return RefreshResult("sent", time.monotonic())
            return RefreshResult("skipped")
        except JobBuildFailed:
            return RefreshResult("failed")
        except OSError:
            self._disconnect(client)  # type: ignore[arg-type]
            return RefreshResult("disconnected")

    def take_post_accept_refresh(
        self,
        client: object,
    ) -> tuple[int, str] | None:
        with self.registry.lock:
            block = getattr(client, "post_accept_refresh_block", None)
            client.post_accept_refresh_block = None  # type: ignore[attr-defined]
            return block


__all__ = [
    "AdmittedIdleBundleSource",
    "DEFAULT_PRISM_EVICTED_JOB_PRUNE_INTERVAL_SECONDS",
    "DeliveryAuthority",
    "DeliverySourceAuthority",
    "EvictedJobEntry",
    "IdleDeliveryAuthority",
    "JobBuildFailed",
    "JobDeliveryRuntimePort",
    "JobDeliveryRuntime",
    "JobDeliveryService",
    "JobDeliveryTipRefreshPort",
    "InitialJobTracker",
    "MAX_ACTIVE_PRISM_JOBS_PER_CLIENT",
    "PRISM_CREDIT_POLICY_STALE_GRACE",
    "PRISM_DELIVERY_PRIORITY_INITIAL",
    "PRISM_DELIVERY_PRIORITY_NEW_TIP",
    "PRISM_DELIVERY_PRIORITY_SAME_TIP",
    "PRISM_EVICTED_JOB_CAPACITY_SCOPES",
    "PRISM_EVICTED_JOB_CLASSES",
    "PRISM_EVICTED_JOB_SUBMIT_OUTCOMES",
    "PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS",
    "PendingInitialJob",
    "PrismJobContext",
    "RetainedJobIndex",
    "_JobBuildFailed",
]
