#!/usr/bin/env python3
"""Per-session PRISM job delivery, initial jobs, and retained-job ownership.

The service owns the S2 domain: job stamping/registration, initial-job
scheduling and its bounded executor, prepared-job delivery admission (epoch
admission floor and post-write proof), difficulty adjacency, and all
retained/evicted-job state including the #100 cross-connection disconnected
retention authority.

It never imports ``prism_coordinator``.  Session membership, Stratum socket
writes, payout delivery gates, job-bundle preparation, and live configuration
attributes are reached through the :class:`JobDeliveryRuntime` typed port,
resolved at call time so the historical coordinator monkeypatch seams
(including the instance-level facade patches used by the current test suite)
keep intercepting exactly as before the extraction.  ``ClientState`` remains
coordinator-owned at this layer (it moves with the session owner); client
objects are used strictly through duck typing, exactly as the current code
does.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping
from concurrent.futures import Future
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, field, replace as dataclass_replace
from decimal import Decimal
import json
import threading
import time
import traceback
from typing import Any, Callable, Iterator, Protocol

from lab.auxpow import vardiff
from lab.prism import direct_stratum
from lab.prism.bounded_executor import _BoundedPriorityExecutor, _DeliveryQueueFull
from lab.prism.coordinator_config import (
    DEFAULT_PRISM_DISCONNECTED_JOB_RETENTION,
    DEFAULT_PRISM_INITIAL_JOB_MAX_WORKERS,  # noqa: F401 - compatibility re-export
    DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_PER_CONNECTION,
    DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_SECONDS,
    DEFAULT_PRISM_STALE_GRACE_SECONDS,
)
from lab.prism.job_bundle import (
    CachedJobBundle,
    JobBuildWaiterCancelled,
    PRISM_JOB_BUILD_PHASES,  # noqa: F401 - owner-shared phase registry
)
from lab.prism.template_artifacts import (
    CachedTemplateArtifacts,
    QbitTipTemplateSnapshot,
    TemplateRefreshBlocked,
    TemplateRefreshSuperseded,
    qbit_template_fingerprint,
)
from lab.prism.tip_refresh import (
    FanoutCancellation,
    RefreshResult,
    TipRefreshValidationToken,
    _TipRefreshTrustBlocked,
)


DEFAULT_PRISM_EVICTED_JOB_PRUNE_INTERVAL_SECONDS = 1.0
# Owner-local copy of the still-coordinator-visible constant; a compatibility
# duplicate by design.  This module is the coordinator's compatibility import
# source for the constant.
PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS = 0.05
MAX_ACTIVE_PRISM_JOBS_PER_CLIENT = 16
# Never-served clients outrank the fleet's tip-refresh wave: a client with no
# active job is producing nothing until its first notify, while a client with
# stale-tip work keeps mining (stale-grace credits it) for the wave's duration.
PRISM_DELIVERY_PRIORITY_INITIAL = 0
PRISM_DELIVERY_PRIORITY_NEW_TIP = 1
PRISM_DELIVERY_PRIORITY_SAME_TIP = 2
PRISM_EVICTED_JOB_CLASSES = ("same_tip", "stale_grace")
PRISM_EVICTED_JOB_SUBMIT_OUTCOMES = (
    "accepted_same_tip",
    "credited_stale_grace",
    "accepted_same_tip_cross_connection",
)
PRISM_EVICTED_JOB_CAPACITY_SCOPES = ("connection", "disconnected")
# Credit policies recorded on accepted ledger rows. Normal shares carry no
# policy; a policy marks a share that was credited by an explicit pool rule
# (documented in docs/prism-rejections.md) so audits can distinguish them.
PRISM_CREDIT_POLICY_STALE_GRACE = "stale-grace"


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
    payout_append_invalidation_epoch: int = 0
    # Digest of the published prior-balance snapshot this job's payouts are
    # keyed to. A periodic self-check repair replaces the published snapshot
    # in place -- the payout generation and append epoch both survive the
    # swap -- so this digest is the only fence that identifies an active job
    # still carrying refuted balances.
    payout_artifact_sha256: str | None = None
    connection_id: int = 0
    authorization_generation: int = 0
    difficulty_generation: int = 0
    tip_refresh_epoch_sequence: int = 0
    # Version-rolling mask negotiated on the connection this job was
    # delivered to. Cross-connection resumes validate in-flight version bits
    # against this mask, not the replacement connection's (often still 0).
    version_mask: int = 0


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
class InitialJobAdmissionSnapshot:
    """One internally consistent copy of the first-job admission domain.

    Metrics and health readers take this instead of reaching into the live
    admission state, so an observability refresh never has to hold the
    coordinator lock and the admission lock at the same time (#159).
    """

    pending: dict[Any, PendingInitialJob]
    queue_rejections: int
    timeout_disconnects: int
    cancelled: int
    coalesced: int
    queue_capacity_reclaimed: int
    last_delivery_monotonic: float | None


class PendingInitialJobsView(Mapping):
    """Read-only, lock-protected view of the first-job admission map.

    The mutable mapping is private to :class:`JobDeliveryService` and is only
    ever read or written under the admission lock.  Every compatibility reader
    -- the session owner's reauthorization capacity pre-check, health and
    metrics, focused tests -- reaches the queue through this view, so an
    external ``len()``, ``in``, lookup, or iteration is itself taken under the
    owning lock.  Such a read can therefore never observe a replacement or a
    cancellation half applied, and can never mutate the queue.

    ``len`` and ``in`` stay O(1) under the lock rather than copying the map:
    the reauthorization pre-check runs once per re-authorize, and a snapshot
    copy there would put O(N) work back inside a lock a reconnect herd
    contends on -- the exact shape #159 exists to remove.  Bulk materialization
    (``copy``, ``items``, ``values``, ``keys``, iteration) takes one locked
    copy, so it cannot tear against concurrent admission.

    These reads are exact at the instant they are taken, but they are not a
    reservation: the authoritative capacity decision is always re-taken under
    the same lock in :meth:`JobDeliveryService.schedule_initial_job`.

    The view is strictly immutable: it exposes no ``__setitem__``,
    ``__delitem__``, ``pop``, ``clear``, ``update``, or ``setdefault``, so no
    caller outside this service can reach admission state to write it.
    Embedders and focused tests that need to install a request seed the whole
    mapping through the ``pending_initial_jobs`` setter, which adopts its
    contents under the admission lock.
    """

    __slots__ = ("_service",)

    def __init__(self, service: "JobDeliveryService") -> None:
        self._service = service

    def copy(self) -> dict[Any, PendingInitialJob]:
        """Return one locked, internally consistent copy of the queue."""
        service = self._service
        with service._initial_job_admission_lock:
            return dict(service._pending_initial_jobs)

    def __getitem__(self, key: Any) -> PendingInitialJob:
        service = self._service
        with service._initial_job_admission_lock:
            return service._pending_initial_jobs[key]

    def __contains__(self, key: object) -> bool:
        service = self._service
        with service._initial_job_admission_lock:
            return key in service._pending_initial_jobs

    def __len__(self) -> int:
        service = self._service
        with service._initial_job_admission_lock:
            return len(service._pending_initial_jobs)

    def get(
        self,
        key: Any,
        default: Any = None,
    ) -> PendingInitialJob | None:
        service = self._service
        with service._initial_job_admission_lock:
            return service._pending_initial_jobs.get(key, default)

    def __iter__(self) -> Iterator[Any]:
        return iter(self.copy())

    def keys(self) -> Any:
        return self.copy().keys()

    def values(self) -> Any:
        return self.copy().values()

    def items(self) -> Any:
        return self.copy().items()

    def __eq__(self, other: object) -> bool:
        return self.copy() == other

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    __hash__ = None  # type: ignore[assignment]

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.copy()!r})"


class JobBuildFailed(RuntimeError):
    """Internal signal used to distinguish a skipped build from a no-op."""


class JobDeliveryRuntime(Protocol):
    """Typed port over the coordinator, resolved at call time.

    Every member is looked up on the live coordinator object when used, so
    instance monkeypatches (``server.maybe_send_job = ...`` and friends) and
    coordinator-owned live configuration attributes keep working exactly as
    before the extraction.  Owner facades route back into this service; that
    round trip is deliberate: it is what preserves the current white-box
    patch surface until later stack layers repoint those tests.  The legacy
    S2 field names also resolve here -- coordinator class descriptors route
    them to this service's single mutable copy -- and the R1 published-tip
    fields resolve through the tip-refresh owner's descriptors the same way.
    """

    # Cross-domain objects and live configuration attributes.
    _job_cache_lock: Any
    _payout_ledger_append_invalidation_epoch: Any
    _payout_state_delivery_gate: Any
    _payout_state_generation: Any
    _payout_state_publication_blocked: Any
    _pool_ready_latched: Any
    _published_payout_state: Any
    _template_artifacts: Any
    _tip_refresh_epoch_payout_generation: Any
    _tip_refresh_epoch_sequence: Any
    _tip_refresh_epoch_tip_hash: Any
    clients: Any
    current_tip_first_seen: Any
    current_tip_parent: Any
    disconnected_job_retention: Any
    hot_path_log_enabled: Any
    initial_job_max_workers: Any
    job_build_failure_count: Any
    lock: Any
    reorg_reconciler_enabled: Any
    rpc: Any
    same_tip_job_retention_per_connection: Any
    same_tip_job_retention_seconds: Any
    stale_grace_seconds: Any
    stop_event: Any
    stratum_initial_job_timeout_seconds: Any
    stratum_max_pending_initial_jobs: Any
    tip_refresh_epoch_fanout: Any
    tip_template_snapshot: Any
    vardiff_config: Any

    def _client_vardiff_lock(self, *args: Any, **kwargs: Any) -> Any: ...

    def _collection_bundle_identity(self, *args: Any, **kwargs: Any) -> Any: ...

    def _current_published_tip_hash_locked(self, *args: Any, **kwargs: Any) -> Any: ...

    def _ensure_evicted_job_state(self, *args: Any, **kwargs: Any) -> Any: ...

    def _ensure_initial_job_state(self, *args: Any, **kwargs: Any) -> Any: ...

    def _ensure_job_cache_state(self, *args: Any, **kwargs: Any) -> Any: ...

    def _idle_bundle_current_locked(self, *args: Any, **kwargs: Any) -> Any: ...

    def _issuance_artifacts_current(self, *args: Any, **kwargs: Any) -> Any: ...

    def _job_build_phases(self, *args: Any, **kwargs: Any) -> Any: ...

    def _observe_payout_gate_admission(self, *args: Any, **kwargs: Any) -> Any: ...

    def _payout_delivery(self, *args: Any, **kwargs: Any) -> Any: ...

    def _progress_bundle_build_finished(self, *args: Any, **kwargs: Any) -> Any: ...

    def _progress_bundle_build_started(self, *args: Any, **kwargs: Any) -> Any: ...

    def _record_first_payout_delivery(self, *args: Any, **kwargs: Any) -> Any: ...

    def _record_progress_delivery(self, *args: Any, **kwargs: Any) -> Any: ...

    def _record_startup_phase_once(self, *args: Any, **kwargs: Any) -> Any: ...

    def _submit_delivery_task(self, *args: Any, **kwargs: Any) -> Any: ...

    def bury_evicted_job(self, *args: Any, **kwargs: Any) -> Any: ...

    def client_minimum_advertised_difficulty(self, *args: Any, **kwargs: Any) -> Any: ...

    def desired_client_share_difficulty(self, *args: Any, **kwargs: Any) -> Any: ...

    def difficulty_payload(self, *args: Any, **kwargs: Any) -> Any: ...

    def disconnect_client(self, *args: Any, **kwargs: Any) -> Any: ...

    def ensure_reorg_reconciled_for_current_tip(self, *args: Any, **kwargs: Any) -> Any: ...

    def job_issuance_template_artifacts(self, *args: Any, **kwargs: Any) -> Any: ...

    def job_payload(self, *args: Any, **kwargs: Any) -> Any: ...

    def observe_job_build_elapsed(self, *args: Any, **kwargs: Any) -> Any: ...

    def qbit_chain_view_untrusted(self, *args: Any, **kwargs: Any) -> Any: ...

    def send_difficulty(self, *args: Any, **kwargs: Any) -> Any: ...

    def send_job(self, *args: Any, **kwargs: Any) -> Any: ...

    def share_weight_for_worker(self, *args: Any, **kwargs: Any) -> Any: ...

    def shared_job_bundle(self, *args: Any, **kwargs: Any) -> Any: ...


class JobDeliveryService:
    """Sole owner of S2 job delivery, initial jobs, and retained jobs."""

    def __init__(
        self,
        runtime: JobDeliveryRuntime,
        *,
        stratum_error: type[BaseException],
    ) -> None:
        self._runtime = runtime
        # The Stratum error type remains coordinator-owned until the session
        # owner is extracted; it is injected so this leaf module never
        # imports prism_coordinator.
        self._stratum_error = stratum_error
        self.jobs: dict[str, PrismJobContext] = {}
        self.job_counter = 0
        # Private: every read and write of this mapping happens under the
        # admission lock below.  Callers outside this service reach it only
        # through the read-only ``pending_initial_jobs`` view.
        self._pending_initial_jobs: dict[Any, PendingInitialJob] = {}
        # Sole owner of the first-job admission domain: the admission map
        # itself, its exact capacity decision, request replacement/coalescing/
        # cancellation, and every admission counter and timestamp below.
        #
        # It is the innermost lock in the process.  Nothing acquires another
        # lock while holding it, and it is never held across a socket write, a
        # disconnect, a job build, an executor shutdown wait, or a future
        # cancellation (``Future.cancel`` runs done-callbacks inline, and this
        # module's callback re-enters admission).  Cancellation callbacks are
        # therefore always invoked after the admission state is committed and
        # the lock is released.  Splitting this domain out of ``runtime.lock``
        # is what stops a reconnect herd's queue bookkeeping from convoying
        # behind fleet-wide coordinator work (#159).
        self._initial_job_admission_lock = threading.Lock()
        self._pending_initial_jobs_view = PendingInitialJobsView(self)
        self._initial_job_executor_lock = threading.Lock()
        self._initial_job_executor: _BoundedPriorityExecutor | None = None
        self._initial_job_executor_shutdown = False
        self.initial_job_queue_rejection_count = 0
        self.initial_job_timeout_count = 0
        self.initial_job_cancelled_count = 0
        self.initial_job_coalesced_count = 0
        self.initial_job_queue_capacity_reclaimed_count = 0
        self.initial_job_sent_count = 0
        self.initial_job_failed_count = 0
        self.initial_job_superseded_count = 0
        self.initial_job_delivery_latency_seconds_sum = 0.0
        self.initial_job_delivery_latency_count = 0
        self.last_initial_job_delivery_monotonic: float | None = None
        # Globally insertion ordered, with per-connection indexes for the
        # independent TTL and capacity limits. Prior-tip entries never consume
        # the same-tip cap while stale-grace still protects them.
        self.evicted_job_graveyard: OrderedDict[str, EvictedJobEntry] = OrderedDict()
        self.evicted_jobs_by_connection: dict[int, OrderedDict[str, None]] = {}
        self.evicted_same_tip_by_connection: dict[int, OrderedDict[str, None]] = {}
        self.evicted_same_tip_job_ids: OrderedDict[str, None] = OrderedDict()
        self.evicted_job_index_tip_hash: str | None = None
        self.evicted_job_next_prune_monotonic = 0.0
        self.evicted_job_expiration_counts = {
            job_class: 0 for job_class in PRISM_EVICTED_JOB_CLASSES
        }
        self.evicted_job_capacity_eviction_counts = {
            scope: 0 for scope in PRISM_EVICTED_JOB_CAPACITY_SCOPES
        }
        self.evicted_job_submit_counts = {
            outcome: 0 for outcome in PRISM_EVICTED_JOB_SUBMIT_OUTCOMES
        }
        # Insertion-ordered ids of graveyard entries whose connection is
        # gone (entry.client is None); bounds cross-connection retention
        # independently of live connections' per-connection caps.
        self._disconnected_evicted_job_ids: OrderedDict[str, None] = OrderedDict()

    # -- state adoption ----------------------------------------------------

    @property
    def pending_initial_jobs(self) -> PendingInitialJobsView:
        """Read-only compatibility surface over the admission map.

        The legacy attribute name is coordinator-visible (and reaches the
        session owner's reauthorization capacity pre-check through the
        coordinator descriptor), so it must not hand out the mutable mapping:
        that is what left a capacity read outside the lock that owns it.
        """
        return self._pending_initial_jobs_view

    @pending_initial_jobs.setter
    def pending_initial_jobs(self, value: Any) -> None:
        """Adopt a mapping assigned through the legacy name.

        Construction and focused tests assign through this name; adopt the
        contents into the private map instead of retaining the caller's
        mutable object, which would put admission state back outside the lock.
        """
        with self._initial_job_admission_lock:
            self._pending_initial_jobs.clear()
            if value:
                self._pending_initial_jobs.update(value)

    def ensure_evicted_job_state(self) -> None:
        """Ensure/adopt the retained-job index, converting legacy seeded state."""
        runtime = self._runtime
        graveyard = getattr(self, "evicted_job_graveyard", None)
        rebuild_indexes = False
        if not isinstance(graveyard, OrderedDict):
            converted: OrderedDict[str, EvictedJobEntry] = OrderedDict()
            for job_id, entry in (graveyard or {}).items():
                if isinstance(entry, EvictedJobEntry):
                    converted[job_id] = entry
                    continue
                context, connection_id, evicted_monotonic = entry
                client = next(
                    (
                        candidate
                        for candidate in getattr(runtime, "clients", ())
                        if candidate.connection_id == connection_id
                    ),
                    None,
                )
                converted[job_id] = EvictedJobEntry(
                    context=context,
                    connection_id=connection_id,
                    evicted_monotonic=evicted_monotonic,
                    previousblockhash=str(context.template["previousblockhash"]),
                    client=client,
                )
            self.evicted_job_graveyard = converted
            rebuild_indexes = True
        if not hasattr(runtime, "same_tip_job_retention_seconds"):
            runtime.same_tip_job_retention_seconds = (
                DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_SECONDS
            )
        if not hasattr(runtime, "same_tip_job_retention_per_connection"):
            runtime.same_tip_job_retention_per_connection = (
                DEFAULT_PRISM_SAME_TIP_JOB_RETENTION_PER_CONNECTION
            )
        if not hasattr(runtime, "disconnected_job_retention"):
            runtime.disconnected_job_retention = (
                DEFAULT_PRISM_DISCONNECTED_JOB_RETENTION
            )
        current_tip = runtime._current_published_tip_hash_locked()
        if self.evicted_job_index_tip_hash != current_tip:
            rebuild_indexes = True
        if rebuild_indexes:
            self._rebuild_evicted_job_indexes_locked()

    # -- narrow adapters for other owners ----------------------------------

    def next_job_id(self) -> str:
        """Allocate the next delivery job id under the coordinator lock."""
        runtime = self._runtime
        with runtime.lock:
            self.job_counter += 1
            return f"prism-{self.job_counter}"

    def complete_delivery(
        self,
        client: Any,
        context: PrismJobContext,
        delivered_monotonic: float,
    ) -> None:
        """Commit S2 delivery bookkeeping after a successful socket write."""
        runtime = self._runtime
        runtime.note_tip_work_delivered(
            client,
            str(context.template["previousblockhash"]),
        )
        runtime._record_progress_delivery(client, context, delivered_monotonic)

    def stamp_job_for_client(
        self,
        client: ClientState,
        cached: CachedJobBundle,
        *,
        clean_jobs: bool,
        tip_refresh_epoch_sequence: int | None = None,
    ) -> PrismJobContext:
        runtime = self._runtime
        if client.worker is None:
            raise self._stratum_error(20, "client is not authorized")
        if cached.collection_only and cached.collection_identity != (
            runtime._collection_bundle_identity(client.worker)
        ):
            raise self._stratum_error(
                20,
                "collection bundle payout identity no longer matches client authorization",
            )
        with runtime.lock:
            self.job_counter += 1
            job_id = f"prism-{self.job_counter}"
            if tip_refresh_epoch_sequence is None:
                tip_refresh_epoch_sequence = (
                    runtime._tip_refresh_epoch_for_bundle_locked(cached)
                )
        share_target = direct_stratum.effective_share_target(
            runtime.desired_client_share_difficulty(client),
            cached.base_job.qbit_target,
            minimum_advertised_difficulty=runtime.client_minimum_advertised_difficulty(client),
        )
        job = dataclass_replace(
            cached.base_job,
            job_id=job_id,
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
            share_weight=runtime.share_weight_for_worker(client.worker),
            collection_only=cached.collection_only,
            worker=client.worker,
            issued_at_ms=cached.issued_at_ms,
            template_fingerprint=cached.template_fingerprint,
            template_generation=cached.template_generation,
            payout_state_generation=cached.payout_state_generation,
            prospective_prior_balances=cached.prospective_prior_balances,
            payout_artifact_generation=cached.payout_artifact_generation,
            payout_append_invalidation_epoch=(
                int(
                    getattr(
                        cached.build_key,
                        "payout_append_invalidation_epoch",
                        0,
                    )
                )
                if cached.build_key is not None
                else 0
            ),
            payout_artifact_sha256=(
                cached.build_key.payout_artifact_sha256
                if cached.build_key is not None
                else None
            ),
            connection_id=client.connection_id,
            authorization_generation=int(
                getattr(client, "authorization_generation", 0)
            ),
            difficulty_generation=int(
                getattr(client, "difficulty_generation", 0)
            ),
            tip_refresh_epoch_sequence=int(tip_refresh_epoch_sequence),
            version_mask=int(getattr(client, "version_mask", 0)),
        )

    def initial_job_executor(self) -> _BoundedPriorityExecutor:
        runtime = self._runtime
        runtime._ensure_initial_job_state()
        with self._initial_job_executor_lock:
            if self._initial_job_executor_shutdown:
                raise RuntimeError("initial job executor is shut down")
            executor = self._initial_job_executor
            if executor is None:
                executor = _BoundedPriorityExecutor(
                    max_workers=runtime.initial_job_max_workers,
                    max_queue_size=runtime.stratum_max_pending_initial_jobs,
                    thread_name_prefix="prism-initial-job-delivery",
                )
                self._initial_job_executor = executor
            return executor

    def shutdown_initial_job_executor(self) -> None:
        runtime = self._runtime
        runtime._ensure_initial_job_state()
        # Reclaim the whole admission domain in one constant-shape critical
        # section, then cancel the futures outside it: every cancellation runs
        # this module's done-callback inline, and that callback re-enters
        # admission.
        with self._initial_job_admission_lock:
            reclaimed = tuple(self._pending_initial_jobs.values())
            self._pending_initial_jobs.clear()
            futures = []
            for request in reclaimed:
                request.cancelled.set()
                if request.future is not None:
                    futures.append(request.future)
                self.initial_job_cancelled_count += 1
        for future in futures:
            runtime._cancel_initial_job_future(future)
        with self._initial_job_executor_lock:
            executor = self._initial_job_executor
            self._initial_job_executor = None
            self._initial_job_executor_shutdown = True
        if executor is not None:
            # Running workers observe request cancellation before shutdown
            # returns; queued reconnect work is cancelled without starting.
            executor.shutdown(wait=True, cancel_futures=True)

    def initial_job_admission_snapshot(self) -> InitialJobAdmissionSnapshot:
        """Return one internally consistent copy of the admission domain.

        Health and metrics readers call this instead of walking the live
        admission state under the coordinator lock, so a refresh never holds
        two locks at once and never observes a half-committed replacement.
        """
        with self._initial_job_admission_lock:
            return InitialJobAdmissionSnapshot(
                pending=dict(self._pending_initial_jobs),
                queue_rejections=self.initial_job_queue_rejection_count,
                timeout_disconnects=self.initial_job_timeout_count,
                cancelled=self.initial_job_cancelled_count,
                coalesced=self.initial_job_coalesced_count,
                queue_capacity_reclaimed=(
                    self.initial_job_queue_capacity_reclaimed_count
                ),
                last_delivery_monotonic=self.last_initial_job_delivery_monotonic,
            )

    def _cancel_initial_job_future(self, future: Future[bool]) -> bool:
        """Cancel one initial-job future and account physical queue removal."""
        executor = getattr(self, "_initial_job_executor", None)
        reclaimed = bool(
            executor is not None
            and executor.cancel(future)
        )
        if executor is None:
            future.cancel()
        if reclaimed:
            # Admission accounting only; the cancellation callbacks that
            # ``executor.cancel`` ran inline above have already completed, so
            # taking the admission lock here cannot re-enter it.
            with self._initial_job_admission_lock:
                self.initial_job_queue_capacity_reclaimed_count += 1
        return reclaimed

    def _initial_request_current_locked(self, request: PendingInitialJob) -> bool:
        """Coordinator-domain currency for one first-job request.

        Called with ``runtime.lock`` held (session membership and the
        published-tip identity are read here).  The pending-slot term is read
        under the admission lock -- taken and released before the rest of the
        predicate, and innermost as always -- so it cannot observe a
        replacement or cancellation half applied.
        """
        runtime = self._runtime
        client = request.client
        deadline = request.deadline_monotonic
        with self._initial_job_admission_lock:
            owns_pending_slot = self._pending_initial_jobs.get(client) is request
        return (
            owns_pending_slot
            and client in runtime.clients
            and not getattr(client, "closing", False)
            and (
                request.connection_id is None
                or client.connection_id == request.connection_id
            )
            and client.authorized
            and client.subscribed
            and client.worker == request.worker
            and int(getattr(client, "authorization_generation", 0))
            == request.authorization_generation
            and (
                request.difficulty_generation is None
                or int(getattr(client, "difficulty_generation", 0))
                == request.difficulty_generation
            )
            and (deadline is None or time.monotonic() < deadline)
            and not request.cancelled.is_set()
        )

    def _initial_request_cancelled(self, request: PendingInitialJob) -> bool:
        runtime = self._runtime
        if request.cancelled.is_set() or runtime.stop_event.is_set():
            return True
        with runtime.lock:
            runtime._ensure_initial_job_state()
            return not runtime._initial_request_current_locked(request)

    def _cancel_pending_initial_job_locked(
        self,
        client: ClientState,
        *,
        count: bool,
    ) -> PendingInitialJob | None:
        """Release this client's pending first-job slot, if it owns one.

        The historical ``_locked`` name records that the disconnect path calls
        this with ``runtime.lock`` held; the admission state it mutates is now
        owned by the admission lock, which is taken (and released) inside.  The
        future is cancelled after the slot is committed so the inline
        done-callback observes the released slot and cannot deadlock.
        """
        runtime = self._runtime
        runtime._ensure_initial_job_state()
        with self._initial_job_admission_lock:
            request = self._pending_initial_jobs.pop(client, None)
            if request is None:
                return None
            request.cancelled.set()
            future = request.future
            if count:
                self.initial_job_cancelled_count += 1
        if future is not None:
            runtime._cancel_initial_job_future(future)
        return request

    def _client_has_current_tip_job_locked(self, client: ClientState) -> bool:
        runtime = self._runtime
        context = client.active_job
        if context is None:
            return False
        payout_generation = int(getattr(runtime, "_payout_state_generation", 0))
        if int(getattr(context, "payout_state_generation", payout_generation)) != payout_generation:
            return False
        current_tip = runtime._current_published_tip_hash_locked()
        if current_tip is None:
            # An active job is not proof that tip observation is alive. Keep
            # coverage fail-closed until blockpoll/blockwait has published the
            # tip that makes the job current.
            return False
        snapshot = getattr(runtime, "tip_template_snapshot", None)
        if snapshot is None:
            # Focused embedders may only publish the observed tip. Production
            # startup and blockpoll publish a full snapshot, in which case the
            # exact template identity checks below are mandatory.
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
            == int(getattr(client, "authorization_generation", 0))
            and int(getattr(context, "difficulty_generation", 0))
            == int(getattr(client, "difficulty_generation", 0))
        )

    def note_initial_job_delivered(
        self,
        client: ClientState,
        *,
        validated_current: bool = False,
    ) -> None:
        runtime = self._runtime
        runtime._ensure_initial_job_state()
        if not validated_current:
            # Coordinator-domain question (published tip identity, payout
            # generation): a short constant-time read, taken and released
            # before any admission state is touched.
            with runtime.lock:
                if not runtime._client_has_current_tip_job_locked(client):
                    return
        future: Future[bool] | None = None
        completed = False
        with self._initial_job_admission_lock:
            request = self._pending_initial_jobs.pop(client, None)
            if request is not None:
                request.cancelled.set()
                future = request.future
                delivered = time.monotonic()
                self.initial_job_sent_count += 1
                self.initial_job_delivery_latency_seconds_sum += max(
                    0.0, delivered - request.requested_monotonic
                )
                self.initial_job_delivery_latency_count += 1
                self.last_initial_job_delivery_monotonic = delivered
                completed = True
        if future is not None:
            runtime._cancel_initial_job_future(future)
        if completed:
            runtime._record_startup_phase_once("first_job_delivered")

    def schedule_initial_job(self, client: ClientState) -> bool:
        """Coalesce and enqueue one first-job request without blocking its handler."""
        runtime = self._runtime
        # Focused tests and embedders replace maybe_send_job on the instance as
        # a synchronous seam. Preserve it without affecting the production
        # class path, which always uses the bounded executor below.
        if "maybe_send_job" in runtime.__dict__:
            return bool(runtime.maybe_send_job(client, clean_jobs=True))

        now = time.monotonic()
        runtime._ensure_initial_job_state()
        if (
            not client.subscribed
            or not client.authorized
            or client.worker is None
            or getattr(client, "closing", False)
        ):
            return True
        generation = int(getattr(client, "authorization_generation", 0))
        difficulty_generation = int(getattr(client, "difficulty_generation", 0))
        worker = client.worker
        connection_id = client.connection_id

        def represents_this_authorization(candidate: PendingInitialJob) -> bool:
            return (
                candidate.connection_id == connection_id
                and candidate.authorization_generation == generation
                and candidate.difficulty_generation == difficulty_generation
                and candidate.worker == worker
            )

        # Pure queue bookkeeping: a live request already represents exactly
        # this authorization.  A reconnect herd's repeat requests settle here
        # without ever reaching the coordinator lock.
        with self._initial_job_admission_lock:
            existing = self._pending_initial_jobs.get(client)
            if existing is not None and represents_this_authorization(existing):
                self.initial_job_coalesced_count += 1
                return True

        # Coordinator-domain snapshot: whether this connection already holds
        # current published-tip work is global tip identity, so it stays under
        # runtime.lock -- taken alone, for a constant-time read, and released
        # before the admission commit below.
        with runtime.lock:
            has_current_tip_job = runtime._client_has_current_tip_job_locked(client)

        # Configuration is read outside the admission lock so nothing but
        # local state is touched inside it.
        capacity = int(runtime.stratum_max_pending_initial_jobs)
        timeout = float(runtime.stratum_initial_job_timeout_seconds)

        reject = False
        deferred = False
        superseded_future: Future[bool] | None = None
        request: PendingInitialJob | None = None
        with self._initial_job_admission_lock:
            if getattr(client, "closing", False):
                # Re-read under the admission lock: retirement and the timeout
                # sweep both commit ``closing`` before releasing this client's
                # slot, so an installation that observed a stale False here
                # would strand a request behind a session already being torn
                # down.
                return True
            existing = self._pending_initial_jobs.get(client)
            if existing is not None and represents_this_authorization(existing):
                self.initial_job_coalesced_count += 1
                return True
            if existing is not None:
                existing.cancelled.set()
                superseded_future = existing.future
                self.initial_job_cancelled_count += 1
                self.initial_job_superseded_count += 1
                if has_current_tip_job:
                    self._pending_initial_jobs.pop(client, None)
            if not has_current_tip_job:
                if (
                    existing is None
                    and len(self._pending_initial_jobs) >= capacity
                ):
                    self.initial_job_queue_rejection_count += 1
                    reject = True
                else:
                    predecessor = None
                    if existing is not None:
                        for candidate in (existing.future, existing.predecessor):
                            if candidate is not None and not candidate.done():
                                predecessor = candidate
                                break
                    request = PendingInitialJob(
                        client=client,
                        connection_id=connection_id,
                        authorization_generation=generation,
                        difficulty_generation=difficulty_generation,
                        worker=worker,
                        requested_monotonic=now,
                        deadline_monotonic=now + timeout if timeout > 0 else None,
                        predecessor=predecessor,
                    )
                    # Install the replacement before releasing the lock: the
                    # superseded future's cancellation callback runs inline
                    # below and must observe the replacement already holding
                    # the client slot.
                    self._pending_initial_jobs[client] = request
                    deferred = predecessor is not None
        if superseded_future is not None:
            # Cancellation callbacks run inline here, with the admission state
            # already committed and the admission lock released; the
            # predecessor callback then hands off exactly one client slot
            # instead of mistaking the obsolete request for a terminal failure.
            runtime._cancel_initial_job_future(superseded_future)
        if has_current_tip_job:
            return True
        if reject or request is None:
            runtime.disconnect_client(client)
            return False
        if deferred:
            return True

        return runtime._submit_initial_job_request(request)

    def request_initial_job_delivery(self, client: ClientState) -> bool:
        """Compatibility name for the single bounded initial-job pipeline."""
        runtime = self._runtime
        return runtime.schedule_initial_job(client)

    def cancel_initial_job_delivery(self, client: ClientState) -> None:
        runtime = self._runtime
        runtime._ensure_initial_job_state()
        runtime._cancel_pending_initial_job_locked(client, count=True)

    def _submit_initial_job_request(self, request: PendingInitialJob) -> bool:
        runtime = self._runtime
        client = request.client
        try:
            future = runtime._submit_delivery_task(
                runtime.initial_job_executor(),
                runtime._run_initial_job,
                request,
                priority=PRISM_DELIVERY_PRIORITY_INITIAL,
            )
        except (_DeliveryQueueFull, RuntimeError):
            disconnect = False
            with self._initial_job_admission_lock:
                if self._pending_initial_jobs.get(client) is request:
                    self._pending_initial_jobs.pop(client, None)
                    request.cancelled.set()
                    self.initial_job_queue_rejection_count += 1
                    disconnect = True
            if disconnect:
                runtime.disconnect_client(client)
            # An obsolete submit can race its already-installed replacement.
            # It owns neither the client slot nor the right to retire the live
            # session when admission fails.
            return not disconnect
        orphan: Future[bool] | None = None
        with self._initial_job_admission_lock:
            if self._pending_initial_jobs.get(client) is request:
                request.future = future
            else:
                orphan = future
        if orphan is not None:
            # A replacement already owns the client slot. Reclaim the queue
            # entry outside the admission lock; the done-callback installed
            # just below then fires immediately and hands the slot on.
            runtime._cancel_initial_job_future(orphan)
        future.add_done_callback(
            lambda completed: runtime._initial_job_future_finished(request, completed)
        )
        return True

    def _initial_job_future_finished(
        self,
        request: PendingInitialJob,
        future: Future[bool],
    ) -> None:
        """Release failed first-job requests instead of stranding capacity."""
        runtime = self._runtime
        delivered = False
        if not future.cancelled():
            try:
                delivered = bool(future.result())
            except Exception:
                with runtime.lock:
                    runtime.job_build_failure_count = int(
                        getattr(runtime, "job_build_failure_count", 0)
                    ) + 1
                print(
                    "prism coordinator: initial job task failed "
                    f"connection={request.client.connection_id}",
                    flush=True,
                )
                traceback.print_exc()

        # Coordinator-domain question, asked only on the delivered path and
        # only for as long as the constant-time read takes.
        proven_delivered = False
        if delivered:
            with runtime.lock:
                runtime._ensure_initial_job_state()
                proven_delivered = bool(
                    runtime._initial_request_current_locked(request)
                    and runtime._client_has_current_tip_job_locked(request.client)
                )

        disconnect = False
        first_delivery = False
        replacement: PendingInitialJob | None = None
        with self._initial_job_admission_lock:
            current = self._pending_initial_jobs.get(request.client)
            if current is not request:
                if (
                    current is not None
                    and current.future is None
                    and current.predecessor is future
                ):
                    current.predecessor = None
                    replacement = current
            elif proven_delivered:
                self._pending_initial_jobs.pop(request.client, None)
                request.cancelled.set()
                self.last_initial_job_delivery_monotonic = time.monotonic()
                first_delivery = True
            else:
                self._pending_initial_jobs.pop(request.client, None)
                request.cancelled.set()
                if (
                    request.deadline_monotonic is not None
                    and request.deadline_monotonic <= time.monotonic()
                ):
                    # Commit teardown while this request still owns the pending
                    # slot: a concurrent reauthorization re-reads ``closing``
                    # under this lock and cannot install a replacement between
                    # expiry and disconnect.
                    request.client.closing = True
                    self.initial_job_timeout_count += 1
                    self.initial_job_cancelled_count += 1
                else:
                    self.initial_job_failed_count += 1
                disconnect = True
        if first_delivery:
            runtime._record_startup_phase_once("first_job_delivered")
        if replacement is not None:
            runtime._submit_initial_job_request(replacement)
        if disconnect:
            runtime.disconnect_client(request.client)

    def _run_initial_job(self, request: PendingInitialJob) -> bool:
        """Prepare outside client locks, then atomically stamp and send current work."""
        runtime = self._runtime
        retry_delay = 0.05
        last_failure_log_monotonic: float | None = None

        def retry_later() -> bool:
            nonlocal retry_delay
            request.cancelled.wait(retry_delay)
            retry_delay = min(1.0, retry_delay * 2)
            return not runtime._initial_request_cancelled(request)

        try:
            while not runtime._initial_request_cancelled(request):
                try:
                    if not runtime.ensure_reorg_reconciled_for_current_tip():
                        if not retry_later():
                            return False
                        continue
                    artifacts = runtime.job_issuance_template_artifacts()
                    if runtime._initial_request_cancelled(request):
                        return False
                    bundle = runtime.shared_job_bundle(
                        artifacts,
                        request.worker,
                        request_source="initial",
                        cancelled=lambda: (
                            runtime._initial_request_cancelled(request)
                            or not runtime._issuance_artifacts_current(artifacts)
                        ),
                    )
                    live_tip = str(runtime.rpc.call("getbestblockhash"))
                    if artifacts.previousblockhash != live_tip:
                        # Feed the observation into detection like every other
                        # live-tip reader; the refresh path owns publication.
                        runtime.observe_tip_for_refresh(live_tip)
                        with runtime.lock:
                            published = getattr(runtime, "current_tip_first_seen", None)
                            pinned_authoritative = bool(
                                published is not None
                                and artifacts.previousblockhash == published[0]
                                and runtime._published_tip_authoritative_locked(
                                    time.monotonic()
                                )
                            )
                        if not pinned_authoritative:
                            # Authority lapsed: submit classification is on the
                            # live RPC read now, so published-tip work would be
                            # rejected. Drop it and rebuild from live state.
                            with runtime._job_cache_lock:
                                if runtime._template_artifacts is artifacts:
                                    runtime._template_artifacts = None
                            if not retry_later():
                                return False
                            continue
                except JobBuildWaiterCancelled:
                    if runtime._initial_request_cancelled(request):
                        return False
                    if not retry_later():
                        return False
                    continue
                except TemplateRefreshBlocked:
                    if runtime._initial_request_cancelled(request):
                        return False
                    if not retry_later():
                        return False
                    continue
                except Exception:
                    with runtime.lock:
                        runtime.job_build_failure_count = int(
                            getattr(runtime, "job_build_failure_count", 0)
                        ) + 1
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
                delivered = runtime._deliver_initial_bundle(request, artifacts, bundle)
                if delivered is None:
                    if not retry_later():
                        return False
                    continue
                return delivered
            return False
        except OSError:
            runtime.disconnect_client(request.client)
            return False

    def _deliver_initial_bundle(
        self,
        request: PendingInitialJob,
        artifacts: CachedTemplateArtifacts,
        bundle: CachedJobBundle,
    ) -> bool | None:
        runtime = self._runtime
        client = request.client

        def cancelled() -> bool:
            return runtime._initial_request_cancelled(request)

        def bundle_payout_stale() -> bool:
            # Unlocked at the gate (a cancellation hint, exactly like direct
            # delivery's callback); authoritative only when re-read under
            # _job_cache_lock at the commit boundary below.
            published_artifact = runtime._published_payout_state.artifact
            return (
                bundle.payout_state_generation != runtime._payout_state_generation
                or bundle.build_key is None
                # A periodic self-check repair replaces the published balance
                # snapshot in place: the generation and append epoch both
                # survive the swap, so only the digest identifies work keyed
                # to the refuted balances. Every other serving decision
                # (cache usability, idle issuance, cache publication) already
                # applies this fence; the initial-delivery commit must too,
                # or a bundle probed before the repair becomes the miner's
                # active job with the refuted balances.
                or published_artifact is None
                or bundle.build_key.payout_artifact_sha256
                != published_artifact.prior_balances_sha256
                or (
                    not bundle.collection_only
                    and int(
                        getattr(
                            bundle.build_key,
                            "payout_append_invalidation_epoch",
                            0,
                        )
                    )
                    != runtime._payout_ledger_append_invalidation_epoch
                )
            )

        if not self._acquire_client_job_lock(client, cancelled):
            return False
        try:
            if cancelled():
                return False
            if not runtime._issuance_artifacts_current(artifacts):
                return None
            runtime._ensure_job_cache_state()
            gate_started = time.monotonic()
            with runtime._payout_delivery(
                lambda: cancelled() or bundle_payout_stale(),
                generation=bundle.payout_state_generation,
            ) as admitted:
                runtime._observe_payout_gate_admission(
                    admitted,
                    generation=bundle.payout_state_generation,
                    fallback_wait_seconds=time.monotonic() - gate_started,
                )
                if cancelled():
                    return False
                if not admitted:
                    # Non-admission with a bundle in hand is transient gate
                    # state: a payout invalidation pending publication, or
                    # this bundle's generation going stale while waiting.
                    # Rebuild and retry within the request deadline; treating
                    # it as terminal turned every unlucky join during a payout
                    # publication into a disconnect-reconnect-rebuild loop.
                    return None
                with runtime._job_cache_lock:
                    payout_current = not bundle_payout_stale()
                if not payout_current or not runtime._issuance_artifacts_current(artifacts):
                    return None
                # Difficulty state precedes coordinator admission everywhere.
                # A share holding this client's Vardiff lock may delay only
                # this delivery; it must never make initial-job delivery hold
                # the global control-plane lock while waiting.
                with runtime._client_vardiff_lock(client):
                    # The Vardiff wait happens after the payout check above; a
                    # late-visible append can invalidate the bundle during it.
                    # Mirror direct delivery: the live epoch re-read and the
                    # stamp are one commit boundary under _job_cache_lock.
                    with runtime._job_cache_lock:
                        if bundle_payout_stale():
                            return None
                        with runtime.lock:
                            if not runtime._initial_request_current_locked(request):
                                return False
                            context = runtime.stamp_job_for_client(
                                client,
                                bundle,
                                clean_jobs=True,
                            )
                            if not runtime._admit_client_tip_refresh_epoch_locked(
                                client,
                                context.tip_refresh_epoch_sequence,
                            ):
                                # The fence blocks only strictly older epochs, so
                                # this connection already registered newer refresh
                                # work whose delivery the epoch machinery owns.
                                # Retrying here can spin until the initial-job
                                # deadline disconnects an already-served client;
                                # complete a request that newer current-tip work
                                # satisfies instead.
                                runtime.note_initial_job_delivered(client)
                                with self._initial_job_admission_lock:
                                    still_owns_slot = (
                                        self._pending_initial_jobs.get(client)
                                        is request
                                    )
                                if not still_owns_slot:
                                    return False
                                return None
                            client.active_job = context
                            for job_id in tuple(client.active_job_ids):
                                runtime.bury_evicted_job(client, job_id, prune=False)
                                self.jobs.pop(job_id, None)
                            client.active_job_ids.clear()
                            runtime.prune_evicted_job_graveyard(force=False)
                            self.jobs[context.job.job_id] = context
                            client.active_job_ids.add(context.job.job_id)
                            runtime.prune_client_active_jobs(client)

                runtime.send_job_update(client, context.job)
                mark_delivered = getattr(admitted, "mark_delivered", None)
                if callable(mark_delivered):
                    mark_delivered()
                runtime.apply_job_difficulty(client, context.job)
                runtime.note_tip_work_delivered(
                    client,
                    str(context.template["previousblockhash"]),
                )
                delivered_monotonic = time.monotonic()
                runtime._record_first_payout_delivery(
                    bundle.payout_state_generation,
                    delivered_monotonic,
                )
                runtime._record_progress_delivery(
                    client,
                    context,
                    delivered_monotonic,
                )
                runtime.note_initial_job_delivered(client, validated_current=True)
                return True
        finally:
            client.job_update_lock.release()

    def sweep_initial_job_timeouts(self, *, now: float | None = None) -> int:
        runtime = self._runtime
        runtime._ensure_initial_job_state()
        now = time.monotonic() if now is None else now
        timed_out: list[PendingInitialJob] = []
        futures: list[Future[bool]] = []
        with self._initial_job_admission_lock:
            expired = [
                request
                for request in self._pending_initial_jobs.values()
                if request.deadline_monotonic is not None
                and request.deadline_monotonic <= now
            ]
            for request in expired:
                self._pending_initial_jobs.pop(request.client, None)
                request.cancelled.set()
                if request.future is not None:
                    futures.append(request.future)
                # Commit teardown while this request still owns the pending
                # slot. A concurrent reauthorization re-reads closing under
                # this lock and cannot install a replacement between expiry
                # and disconnect.
                request.client.closing = True
                self.initial_job_timeout_count += 1
                self.initial_job_cancelled_count += 1
                timed_out.append(request)
        # Queue reclamation runs cancellation callbacks inline; both it and
        # the disconnects below stay outside the admission lock.
        for future in futures:
            runtime._cancel_initial_job_future(future)
        for request in timed_out:
            runtime.disconnect_client(request.client)
        return len(timed_out)

    def initial_job_timeout_loop(self) -> None:
        runtime = self._runtime
        while not runtime.stop_event.wait(1.0):
            runtime.sweep_initial_job_timeouts()

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
        runtime = self._runtime
        worker_started = time.monotonic()
        started = worker_started if submitted_monotonic is None else submitted_monotonic
        phases = runtime._job_build_phases()
        phases.clear()

        def cancelled() -> bool:
            return runtime._prepared_tip_refresh_obsolete(
                validation_token,
                bundle,
                snapshot,
                cancel_event,
            ) or getattr(client, "closing", False)

        phases["executor_queue"] = max(0.0, worker_started - started)
        client_lock_started = worker_started
        client_lock_acquired = False
        client_lock_attempted = False
        try:
            while True:
                with runtime.lock:
                    if (
                        client not in runtime.clients
                        or client.connection_id != expected_connection_id
                        or getattr(client, "closing", False)
                    ):
                        return RefreshResult("disconnected")
                if cancelled():
                    phases["client_lock"] = max(
                        0.0,
                        time.monotonic() - client_lock_started,
                    )
                    runtime._record_tip_refresh_cancellation(
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
                runtime._record_tip_refresh_cancellation("client_lock")
                return RefreshResult("skipped")
            with runtime.lock:
                if (
                    client not in runtime.clients
                    or client.connection_id != expected_connection_id
                ):
                    return RefreshResult("disconnected")
                active_epoch = int(
                    getattr(
                        getattr(client, "active_job", None),
                        "tip_refresh_epoch_sequence",
                        0,
                    )
                )
                epoch_fanout = bool(
                    getattr(runtime, "tip_refresh_epoch_fanout", False)
                )
                if (
                    not runtime.client_can_receive_jobs(client)
                    or (
                        epoch_fanout
                        and runtime._client_tip_refresh_epoch_blocked_locked(
                            client,
                            validation_token.epoch_sequence,
                        )
                    )
                    or (
                        (
                            not epoch_fanout
                            or active_epoch >= validation_token.epoch_sequence
                        )
                        and runtime.intervening_job_supersedes_snapshot(
                            client.active_job,
                            expected_active_job,
                            snapshot,
                        )
                    )
                    or not runtime.client_needs_tip_template_refresh(client, snapshot)
                ):
                    return RefreshResult("skipped")
            runtime._ensure_job_cache_state()
            payout_gate_started = time.monotonic()
            with runtime._payout_state_delivery_gate.delivery_cancelable(
                cancelled,
                generation=bundle.payout_state_generation,
                priority=True,
            ) as payout_admitted:
                phases["payout_gate"] = max(
                    0.0,
                    time.monotonic() - payout_gate_started,
                )
                runtime._observe_payout_gate_admission(
                    payout_admitted,
                    generation=bundle.payout_state_generation,
                    fallback_wait_seconds=phases["payout_gate"],
                )
                if not payout_admitted or cancelled():
                    runtime._record_tip_refresh_cancellation("payout_gate")
                    return RefreshResult("skipped")
                fanout_admitted = (
                    cancel_event is None or cancel_event.begin_delivery()
                )
                if not fanout_admitted:
                    runtime._record_tip_refresh_cancellation("payout_gate")
                    return RefreshResult("skipped")
                try:
                    # The validation token binds this immutable bundle to the
                    # exact observed artifact object. Fanout tasks consult only
                    # in-memory publication/cancellation state, never RPC or
                    # the mutable cache.
                    # Preserve one coherent difficulty snapshot through stamp
                    # and commit, but acquire it before global control-plane
                    # admission so a concurrent share cannot form the inverse
                    # coordinator -> Vardiff edge.
                    with runtime._client_vardiff_lock(client):
                        with runtime.lock:
                            token_current = runtime._tip_refresh_token_current_locked(
                                validation_token,
                                bundle,
                                snapshot,
                            )
                            if not token_current:
                                if cancel_event is not None:
                                    cancel_event.cancel()
                                return RefreshResult("skipped")
                            if (
                                client not in runtime.clients
                                or client.connection_id != expected_connection_id
                            ):
                                return RefreshResult("disconnected")
                            active_epoch = int(
                                getattr(
                                    getattr(client, "active_job", None),
                                    "tip_refresh_epoch_sequence",
                                    0,
                                )
                            )
                            epoch_fanout = bool(
                                getattr(runtime, "tip_refresh_epoch_fanout", False)
                            )
                            if (
                                not runtime.client_can_receive_jobs(client)
                                or (
                                    epoch_fanout
                                    and runtime._client_tip_refresh_epoch_blocked_locked(
                                        client,
                                        validation_token.epoch_sequence,
                                    )
                                )
                                or (
                                    (
                                        not epoch_fanout
                                        or active_epoch
                                        >= validation_token.epoch_sequence
                                    )
                                    and runtime.intervening_job_supersedes_snapshot(
                                        client.active_job,
                                        expected_active_job,
                                        snapshot,
                                    )
                                )
                                or not runtime.client_needs_tip_template_refresh(
                                    client,
                                    snapshot,
                                )
                            ):
                                return RefreshResult("skipped")
                            if not runtime._admit_client_tip_refresh_epoch_locked(
                                client,
                                validation_token.epoch_sequence,
                            ):
                                return RefreshResult("skipped")
                            clean_jobs = runtime.client_tip_changed_for_snapshot(
                                client,
                                snapshot,
                            )
                            stamp_started = time.monotonic()
                            context = runtime.stamp_job_for_client(
                                client,
                                bundle,
                                clean_jobs=clean_jobs,
                                tip_refresh_epoch_sequence=(
                                    validation_token.epoch_sequence
                                ),
                            )
                            phases["stamp"] = time.monotonic() - stamp_started
                            client.active_job = context
                            if clean_jobs:
                                for job_id in tuple(client.active_job_ids):
                                    runtime.bury_evicted_job(client, job_id, prune=False)
                                    self.jobs.pop(job_id, None)
                                client.active_job_ids.clear()
                                runtime.prune_evicted_job_graveyard(force=False)
                            self.jobs[context.job.job_id] = context
                            client.active_job_ids.add(context.job.job_id)
                            runtime.prune_client_active_jobs(client)

                    socket_send_started = time.monotonic()
                    try:
                        runtime.send_job_update(client, context.job)
                        payout_admitted.mark_delivered()
                    finally:
                        socket_send_finished = time.monotonic()
                        phases["socket_send"] = max(
                            0.0,
                            socket_send_finished - socket_send_started,
                        )
                    runtime.apply_job_difficulty(client, context.job)
                    runtime.note_tip_work_delivered(
                        client,
                        str(context.template["previousblockhash"]),
                    )
                    runtime.note_initial_job_delivered(client, validated_current=True)
                    delivered_monotonic = time.monotonic()
                    runtime._record_first_payout_delivery(
                        context.payout_state_generation,
                        delivered_monotonic,
                    )
                    runtime._record_progress_delivery(
                        client,
                        context,
                        delivered_monotonic,
                    )
                    if getattr(runtime, "hot_path_log_enabled", False):
                        print(
                            "prism coordinator: sent prepared job "
                            f"connection={client.connection_id} username={client.username} "
                            f"job={context.job.job_id} "
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
            runtime.observe_job_build_elapsed(
                max(0.0, time.monotonic() - started),
                phases,
            )

    def note_tip_work_delivered(self, client: ClientState, job_parent_hash: str) -> None:
        """Record the first time this connection was sent work for a tip.

        First delivery wins per tip: same-tip template refreshes must not slide
        the connection's stale-grace anchor forward.
        """
        now = time.monotonic()
        # Per-connection state only. The coordinator lock is deliberately
        # absent: a delivery to one connection must never wait on fleet-wide
        # control-plane work, and the O(N) current-tip coverage census that
        # used to run here now runs once per observability refresh instead of
        # once per delivery (#159).
        #
        # Anchoring is serialized by this connection's job-update lock. Every
        # production delivery path already holds it across send-then-anchor, so
        # re-entering it costs nothing and extends the same serialization to
        # the direct compatibility callers. Lightweight embedders and focused
        # doubles may supply a minimal lock exposing only the timed
        # acquire/release pair the delivery path uses; those callers hold it
        # already, so anchor without re-entering rather than failing on the
        # missing protocol.
        lock = getattr(client, "job_update_lock", None)
        with lock if hasattr(lock, "__enter__") else nullcontext():
            delivered = client.tip_work_delivered
            if delivered is None or delivered[0] != job_parent_hash:
                client.tip_work_delivered = (job_parent_hash, now)

    def _evicted_job_class_locked(self, entry: EvictedJobEntry) -> str:
        runtime = self._runtime
        current_tip = runtime._current_published_tip_hash_locked()
        if current_tip is None or entry.previousblockhash == current_tip:
            return "same_tip"
        return "stale_grace"

    def bury_evicted_job(
        self,
        client: ClientState,
        job_id: str,
        *,
        now: float | None = None,
        prune: bool = True,
    ) -> None:
        runtime = self._runtime
        with runtime.lock:
            runtime._ensure_evicted_job_state()
            context = self.jobs.get(job_id)
            if context is None:
                return
            self._remove_evicted_job_locked(job_id)
            self.evicted_job_graveyard[job_id] = EvictedJobEntry(
                context=context,
                connection_id=client.connection_id,
                evicted_monotonic=time.monotonic() if now is None else now,
                previousblockhash=str(context.template["previousblockhash"]),
                client=client,
            )
            self._index_evicted_job_locked(job_id, self.evicted_job_graveyard[job_id])
            self._enforce_evicted_same_tip_capacity_locked(client.connection_id)
            if prune:
                runtime.prune_evicted_job_graveyard(now=now, force=False)

    def prune_evicted_job_graveyard(
        self,
        *,
        now: float | None = None,
        force: bool = True,
    ) -> None:
        runtime = self._runtime
        with runtime.lock:
            runtime._ensure_evicted_job_state()
            if not self.evicted_job_graveyard:
                return
            now = time.monotonic() if now is None else now
            if not force and now < self.evicted_job_next_prune_monotonic:
                return
            self.evicted_job_next_prune_monotonic = (
                now + DEFAULT_PRISM_EVICTED_JOB_PRUNE_INTERVAL_SECONDS
            )
            for job_id, entry in tuple(self.evicted_job_graveyard.items()):
                job_class, expired = self._evicted_job_expired_locked(entry, now=now)
                if expired:
                    self._remove_evicted_job_locked(job_id)
                    self.evicted_job_expiration_counts[job_class] += 1

    def evicted_job_entry(
        self,
        client: ClientState,
        job_id: str,
    ) -> EvictedJobEntry | None:
        runtime = self._runtime
        with runtime.lock:
            runtime._ensure_evicted_job_state()
            entry = getattr(self, "evicted_job_graveyard", {}).get(job_id)
            if entry is None:
                return None
            if (
                entry.connection_id != client.connection_id
                and not self._cross_connection_evicted_entry_allowed_locked(
                    entry,
                    client,
                )
            ):
                return None
            job_class, expired = self._evicted_job_expired_locked(
                entry,
                now=time.monotonic(),
            )
            if expired:
                self._remove_evicted_job_locked(job_id)
                self.evicted_job_expiration_counts[job_class] += 1
                return None
            return entry

    def note_evicted_job_submit(
        self,
        credit_policy: str | None,
        *,
        cross_connection: bool = False,
    ) -> None:
        runtime = self._runtime
        if credit_policy == PRISM_CREDIT_POLICY_STALE_GRACE:
            outcome = "credited_stale_grace"
        elif cross_connection:
            outcome = "accepted_same_tip_cross_connection"
        else:
            outcome = "accepted_same_tip"
        with runtime.lock:
            runtime._ensure_evicted_job_state()
            self.evicted_job_submit_counts[outcome] += 1

    def client_can_receive_jobs(self, client: ClientState) -> bool:
        return (
            not getattr(client, "closing", False)
            and client.subscribed
            and client.authorized
            and client.worker is not None
        )

    def client_needs_tip_template_refresh(
        self,
        client: ClientState,
        snapshot: QbitTipTemplateSnapshot,
    ) -> bool:
        runtime = self._runtime
        context = client.active_job
        if context is None:
            return True
        if getattr(runtime, "tip_refresh_epoch_fanout", False):
            current_epoch = int(
                getattr(runtime, "_tip_refresh_epoch_sequence", 0)
            )
            context_epoch = int(
                getattr(context, "tip_refresh_epoch_sequence", 0)
            )
            if context_epoch > current_epoch:
                return False
            if context_epoch < current_epoch:
                return True
            current_epoch_tip = getattr(
                runtime,
                "_tip_refresh_epoch_tip_hash",
                None,
            )
            if (
                current_epoch_tip is not None
                and str(context.template.get("previousblockhash", ""))
                != current_epoch_tip
            ):
                return True
        if getattr(context, "collection_only", False) and getattr(runtime, "_pool_ready_latched", False
        ):
            # The pool crossed min_ready_miners after this job was issued.
            # A collection job keeps settling solved blocks solver-pays-all,
            # so replace it with windowed work on the next poller pass.
            return True
        template = context.template
        previousblockhash = str(template.get("previousblockhash", ""))
        context_fingerprint = getattr(context, "template_fingerprint", None)
        if context_fingerprint is None:
            context_fingerprint = qbit_template_fingerprint(template)
        context_payout_generation = int(
            getattr(context, "payout_state_generation", 0)
        )
        context_payout_artifact_sha256 = getattr(
            context,
            "payout_artifact_sha256",
            None,
        )
        published_payout_state = getattr(runtime, "_published_payout_state", None)
        published_payout_artifact = (
            published_payout_state.artifact
            if published_payout_state is not None
            else None
        )
        active_needs_refresh = (
            previousblockhash != snapshot.bestblockhash
            or previousblockhash != snapshot.previousblockhash
            or context_fingerprint != snapshot.template_fingerprint
            or context_payout_generation
            != int(getattr(runtime, "_payout_state_generation", 0))
            # A self-check repair swaps the published balance snapshot in
            # place: the generation and append epoch both survive, so only
            # the digest identifies an active job still mining the refuted
            # balances. Jobs stamped before the digest existed (or while no
            # artifact is published mid-publication) defer to the generation
            # fence above.
            or (
                context_payout_artifact_sha256 is not None
                and published_payout_artifact is not None
                and context_payout_artifact_sha256
                != published_payout_artifact.prior_balances_sha256
            )
            # A late-visible append advances only the epoch: the generation
            # and digest survive, yet the job's window omitted the late row
            # (possibly a replayed winning share). Collection jobs settle
            # solver-pays-all and are exempt, as at every other epoch fence.
            or (
                not getattr(context, "collection_only", False)
                and int(
                    getattr(context, "payout_append_invalidation_epoch", 0)
                )
                != int(
                    getattr(
                        runtime,
                        "_payout_ledger_append_invalidation_epoch",
                        0,
                    )
                )
            )
        )
        if active_needs_refresh:
            return True
        if not getattr(runtime, "tip_refresh_epoch_fanout", False):
            return False
        delivered = getattr(client, "_progress_delivered_context", None)
        if delivered is None:
            return True
        delivered_fingerprint = getattr(
            delivered,
            "template_fingerprint",
            None,
        )
        if delivered_fingerprint is None:
            delivered_fingerprint = qbit_template_fingerprint(
                delivered.template
            )
        # Reselection must cover every condition the epoch fixpoint counts
        # as undelivered, or the owning wave re-enters forever with no
        # candidate to serve. A delivered collection job after the pool
        # latches ready is one such condition even when its epoch, tip,
        # payout generation, and fingerprint all remain current, because
        # registration can advance past a delivery whose send failed.
        # A client whose current-epoch delivery is registered but unproven
        # is deliberately reselected here (the fence admits equal epochs,
        # so the re-serve is idempotent on the wire). Deduplicating on the
        # admitted epoch instead would be wrong: admission is monotonic
        # and survives a failed send, so an admitted-equal skip would
        # deselect the client while the fixpoint still counts it
        # undelivered.
        return (
            int(getattr(delivered, "connection_id", 0))
            != int(client.connection_id)
            or int(getattr(delivered, "tip_refresh_epoch_sequence", 0))
            != current_epoch
            or str(delivered.template.get("previousblockhash", ""))
            != current_epoch_tip
            or int(getattr(delivered, "payout_state_generation", 0))
            != int(getattr(runtime, "_tip_refresh_epoch_payout_generation", 0))
            or delivered_fingerprint != snapshot.template_fingerprint
            or (
                bool(getattr(delivered, "collection_only", False))
                and bool(getattr(runtime, "_pool_ready_latched", False))
            )
        )

    def intervening_job_supersedes_snapshot(
        self,
        active_job: PrismJobContext | None,
        expected_active_job: PrismJobContext | None,
        snapshot: QbitTipTemplateSnapshot,
    ) -> bool:
        runtime = self._runtime
        if active_job is expected_active_job or active_job is None:
            return False
        active_payout_generation = int(
            getattr(active_job, "payout_state_generation", 0)
        )
        if active_payout_generation < int(
            getattr(runtime, "_payout_state_generation", 0)
        ):
            # Template ordering cannot make a payout-stale intervening job
            # authoritative. Let the reconciled refresh replace it.
            return False
        active_parent_hash = str(
            getattr(active_job, "template", {}).get("previousblockhash", "")
        )
        if (
            active_parent_hash != snapshot.bestblockhash
            or active_parent_hash != snapshot.previousblockhash
        ):
            # Artifact generations order fetch starts, not chain tips. A fetch
            # for the old tip can start after this exact new-tip observation
            # and therefore carry a larger generation; it must not prevent
            # the new-tip snapshot from replacing that stale work.
            return False
        active_generation = int(getattr(active_job, "template_generation", 0))
        snapshot_generation = int(getattr(snapshot, "template_generation", 0))
        if active_generation <= 0 or snapshot_generation <= 0:
            # Legacy/test contexts without ordering metadata retain the safe
            # behavior: never overwrite an unclassified intervening job.
            return True
        return active_generation >= snapshot_generation

    def client_tip_changed_for_snapshot(
        self,
        client: ClientState,
        snapshot: QbitTipTemplateSnapshot,
    ) -> bool:
        runtime = self._runtime
        context = client.active_job
        if context is None:
            return True
        previousblockhash = str(context.template.get("previousblockhash", ""))
        context_payout_artifact_sha256 = getattr(
            context,
            "payout_artifact_sha256",
            None,
        )
        published_payout_state = getattr(runtime, "_published_payout_state", None)
        published_payout_artifact = (
            published_payout_state.artifact
            if published_payout_state is not None
            else None
        )
        return (
            previousblockhash != snapshot.bestblockhash
            or previousblockhash != snapshot.previousblockhash
            or int(getattr(context, "payout_state_generation", 0))
            != int(getattr(runtime, "_payout_state_generation", 0))
            # A refresh selected because a self-check repair replaced the
            # published balance digest must also retire the refuted job: a
            # clean_jobs=False replacement leaves the old job ID admissible,
            # so a late block submission could still commit the refuted
            # allocation. Same None guards as the reselection predicate.
            or (
                context_payout_artifact_sha256 is not None
                and published_payout_artifact is not None
                and context_payout_artifact_sha256
                != published_payout_artifact.prior_balances_sha256
            )
            # Likewise for append-epoch skew: the replacement window covers
            # the late-visible row, so the job mining the pre-append window
            # must not stay admissible beside it.
            or (
                not getattr(context, "collection_only", False)
                and int(
                    getattr(context, "payout_append_invalidation_epoch", 0)
                )
                != int(
                    getattr(
                        runtime,
                        "_payout_ledger_append_invalidation_epoch",
                        0,
                    )
                )
            )
        )

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
        runtime = self._runtime
        with client.job_update_lock:
            return runtime._maybe_send_job_locked(
                client,
                clean_jobs=clean_jobs,
                raise_on_reorg_failure=raise_on_reorg_failure,
                raise_on_build_failure=raise_on_build_failure,
                tip_refresh_snapshot=tip_refresh_snapshot,
                tip_refresh_observation_sequence=tip_refresh_observation_sequence,
            )

    def _maybe_send_job_locked(
        self,
        client: ClientState,
        *,
        clean_jobs: bool,
        raise_on_reorg_failure: bool = False,
        raise_on_build_failure: bool = False,
        tip_refresh_snapshot: QbitTipTemplateSnapshot | None = None,
        tip_refresh_observation_sequence: int | None = None,
        prepared_bundle: CachedJobBundle | None = None,
        commit_guard: Callable[[], bool] | None = None,
        commit_guard_lock: threading.RLock | None = None,
        prepared_bundle_allow_uncached: bool = False,
    ) -> bool:
        runtime = self._runtime
        if not client.subscribed or not client.authorized or client.worker is None:
            return False
        runtime._ensure_job_cache_state()
        started = time.monotonic()
        phases = runtime._job_build_phases()
        phases.clear()
        if getattr(runtime, "hot_path_log_enabled", False):
            print(
                f"prism coordinator: building job connection={client.connection_id} username={client.username}",
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
            with runtime.lock:
                refresh_current = runtime._tip_refresh_snapshot_current_locked(
                    tip_refresh_snapshot,
                    tip_refresh_observation_sequence,
                )
            if not refresh_current:
                runtime._schedule_tip_refresh_retry()
                raise TemplateRefreshSuperseded(
                    "tip refresh snapshot was superseded before client job build"
                )
            try:
                chain_view_untrusted = bool(
                    getattr(runtime, "reorg_reconciler_enabled", True)
                    and runtime.qbit_chain_view_untrusted()
                )
            except Exception as exc:
                runtime._schedule_tip_refresh_retry()
                raise _TipRefreshTrustBlocked(
                    "qbit chain trust check failed before sequential client job build"
                ) from exc
            if chain_view_untrusted:
                runtime._schedule_tip_refresh_retry()
                raise _TipRefreshTrustBlocked(
                    "qbit chain view became untrusted before sequential client job build"
                )
        else:
            try:
                if not runtime.ensure_reorg_reconciled_for_current_tip():
                    if raise_on_reorg_failure:
                        raise _TipRefreshTrustBlocked(
                            "qbit chain view became untrusted before client job build"
                        )
                    return False
            except TemplateRefreshBlocked:
                raise
            except Exception as exc:
                print(
                    f"prism coordinator: reorg reconciliation failed before job build "
                    f"connection={client.connection_id} username={client.username}; skipping this job",
                    flush=True,
                )
                traceback.print_exc()
                if raise_on_reorg_failure:
                    raise TemplateRefreshBlocked(
                        "reorg reconciliation failed before client job build"
                    ) from exc
                return False
        phases["reorg"] = time.monotonic() - phase_started
        built_from_guarded_artifacts = bool(
            guarded_refresh
            and tip_refresh_snapshot.template_artifacts is not None
            and "build_job_for_client" not in runtime.__dict__
        )
        try:
            if prepared_bundle is not None:
                context = runtime.stamp_job_for_client(
                    client,
                    prepared_bundle,
                    clean_jobs=clean_jobs,
                )
            elif built_from_guarded_artifacts:
                assert tip_refresh_snapshot is not None
                assert tip_refresh_snapshot.template_artifacts is not None
                context = runtime.build_job_for_client_from_artifacts(
                    client,
                    tip_refresh_snapshot.template_artifacts,
                    clean_jobs=clean_jobs,
                    publication_critical=True,
                    request_source="tip_refresh",
                )
            else:
                context = runtime.build_job_for_client(client, clean_jobs=clean_jobs)
        except TemplateRefreshBlocked:
            runtime._schedule_tip_refresh_retry()
            if guarded_refresh or raise_on_reorg_failure or raise_on_build_failure:
                raise
            return False
        except Exception as exc:
            # A single bad template (e.g. a coinbase whose bytes collide with the
            # extranonce placeholder, or a transient getblocktemplate failure) must
            # never tear down the miner's connection. Log it, count it, and skip
            # this job; the next share/retarget or block change rebuilds a fresh one.
            # Only the build is isolated: nothing has been registered or sent yet, so
            # there is no stale job state. Downstream send failures still surface to
            # handle_client, which disconnects the (now dead) socket and cleans up.
            with runtime.lock:
                runtime.job_build_failure_count += 1
            print(
                f"prism coordinator: job build failed connection={client.connection_id} "
                f"username={client.username}; keeping client connected and skipping this template",
                flush=True,
            )
            traceback.print_exc()
            if raise_on_build_failure:
                raise JobBuildFailed(
                    f"job build failed for connection {client.connection_id}"
                ) from exc
            return False
        # Linearize direct delivery against the immutable publication pointer.
        # Expensive build and ledger reads happened under the preparation lock,
        # outside this admission boundary.
        with runtime._job_cache_lock:
            current_payout_generation = runtime._payout_state_generation
            current_append_invalidation_epoch = (
                runtime._payout_ledger_append_invalidation_epoch
            )
            published_tip = runtime._published_payout_state.source_tip_hash
            publication_blocked = runtime._payout_state_publication_blocked
            context_payout_generation = int(
                getattr(
                    context,
                    "payout_state_generation",
                    current_payout_generation,
                )
            )
            context_append_invalidation_epoch = int(
                getattr(context, "payout_append_invalidation_epoch", 0)
            )
            context_collection_only = bool(
                getattr(context, "collection_only", False)
            )
            context_payout_artifact_sha256 = getattr(
                context,
                "payout_artifact_sha256",
                None,
            )

        def context_payout_digest_stale() -> bool:
            # A periodic self-check repair swaps the published balance
            # snapshot in place: the payout generation and append epoch both
            # survive the swap, so only the digest identifies a context still
            # keyed to the refuted balances. Contexts without a digest defer
            # to the generation fence. Unlocked at the gate (a cancellation
            # hint); authoritative when re-read under _job_cache_lock at the
            # commit boundary below.
            if context_payout_artifact_sha256 is None:
                return False
            published_artifact = runtime._published_payout_state.artifact
            return (
                published_artifact is None
                or context_payout_artifact_sha256
                != published_artifact.prior_balances_sha256
            )

        context_template = getattr(context, "template", None)
        context_parent = (
            str(context_template.get("previousblockhash", ""))
            if isinstance(context_template, dict)
            else ""
        )
        with runtime.lock:
            published_authority = getattr(runtime, "current_tip_first_seen", None)
            published_authoritative = runtime._published_tip_authoritative_locked(
                time.monotonic()
            )
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
            # The published authority lapsed, so per-share classification has
            # fallen back to the live RPC read. Mirror the initial-job path:
            # revalidate against the live tip (outside every lock), record
            # the observation for the refresh machinery, and drop work that
            # would be stale on arrival.
            try:
                lapsed_live_tip = str(runtime.rpc.call("getbestblockhash"))
            except Exception:
                lapsed_live_tip = None
            if lapsed_live_tip is not None:
                runtime.observe_tip_for_refresh(lapsed_live_tip)
                if context_parent != lapsed_live_tip:
                    runtime._schedule_tip_refresh_retry()
                    return False
                lapsed_live_validated = True
        priority_delivery = (
            not publication_blocked
            and context_payout_generation == current_payout_generation
            and (
                context_collection_only
                or context_append_invalidation_epoch
                == current_append_invalidation_epoch
            )
            and (
                published_tip is None
                or context_parent == published_tip
                # Reconciliation can source payout state at the detected tip
                # before submit authority flips. Pinned published-tip work is
                # still the currently creditable work, so it must keep the
                # priority lane instead of tripping the gate's non-priority
                # same-generation rejection for the whole unpublished window.
                or pinned_published_delivery
            )
        )
        payout_gate_started = time.monotonic()
        with runtime._payout_state_delivery_gate.delivery_cancelable(
            lambda: (
                context_payout_generation != runtime._payout_state_generation
                or context_payout_digest_stale()
                or (
                    not context_collection_only
                    and context_append_invalidation_epoch
                    != runtime._payout_ledger_append_invalidation_epoch
                )
            ),
            generation=context_payout_generation,
            priority=priority_delivery,
        ) as payout_admitted:
            payout_gate_wait = max(0.0, time.monotonic() - payout_gate_started)
            phases["payout_gate"] = phases.get("payout_gate", 0.0) + payout_gate_wait
            runtime._observe_payout_gate_admission(
                payout_admitted,
                generation=context_payout_generation,
                fallback_wait_seconds=payout_gate_wait,
            )
            if not payout_admitted:
                runtime._schedule_tip_refresh_retry()
                if guarded_refresh:
                    raise TemplateRefreshSuperseded(
                        "payout state changed during client job build"
                    )
                return False
            def commit_context_locked() -> bool:
                if getattr(client, "closing", False):
                    return False
                if commit_guard is not None and not commit_guard():
                    return False
                if guarded_refresh:
                    assert tip_refresh_snapshot is not None
                    assert tip_refresh_observation_sequence is not None
                    if not runtime._tip_refresh_snapshot_current_locked(
                        tip_refresh_snapshot,
                        tip_refresh_observation_sequence,
                    ):
                        runtime._schedule_tip_refresh_retry()
                        raise TemplateRefreshSuperseded(
                            "tip refresh snapshot was superseded during client job build"
                        )
                    artifacts = tip_refresh_snapshot.template_artifacts
                    if built_from_guarded_artifacts and artifacts is not None and (
                        context.template is not artifacts.template
                        or context.template_fingerprint != artifacts.fingerprint
                        or context.template_generation != artifacts.generation
                    ):
                        raise TemplateRefreshBlocked(
                            "client job build did not use the guarded refresh artifacts"
                        )
                else:
                    published_now = getattr(runtime, "current_tip_first_seen", None)
                    if published_now is None:
                        # Bootstrap: nothing published yet, so there is no
                        # authority for this work to contradict.
                        pass
                    elif runtime._published_tip_authoritative_locked(
                        time.monotonic()
                    ):
                        if context_parent and context_parent != published_now[0]:
                            # Authority moved while this direct build waited
                            # on the payout gate or client lock. Sending now
                            # would advertise work that classifies stale-job
                            # on arrival; skip and let the refresh fanout (or
                            # the pending retry) deliver current work.
                            runtime._schedule_tip_refresh_retry()
                            return False
                    elif context_parent and not lapsed_live_validated:
                        # The lease lapsed during the wait and this context
                        # never passed a live-tip revalidation: cached
                        # observations may be blind to the tip mining.submit
                        # now classifies against. Defer to a fresh pass whose
                        # pre-wait live check settles it without holding the
                        # coordinator lock across an RPC.
                        runtime._schedule_tip_refresh_retry()
                        return False
                if not runtime._admit_client_tip_refresh_epoch_locked(
                    client,
                    int(getattr(context, "tip_refresh_epoch_sequence", 0)),
                ):
                    runtime._schedule_tip_refresh_retry()
                    return False
                client.active_job = context
                if clean_jobs:
                    for job_id in client.active_job_ids:
                        runtime.bury_evicted_job(client, job_id, prune=False)
                        self.jobs.pop(job_id, None)
                    client.active_job_ids.clear()
                    runtime.prune_evicted_job_graveyard(force=False)
                self.jobs[context.job.job_id] = context
                client.active_job_ids.add(context.job.job_id)
                runtime.prune_client_active_jobs(client)
                return True

            def commit_context() -> bool:
                if prepared_bundle is not None:
                    # Exact cache identity and the client/window guard are one
                    # commit boundary. A tip or payout publication that wins
                    # this race makes the idle task a no-op instead of
                    # delivering stale work.
                    with runtime._job_cache_lock:
                        if not runtime._idle_bundle_current_locked(
                            client,
                            prepared_bundle,
                            allow_uncached=prepared_bundle_allow_uncached,
                        ):
                            return False
                        with runtime.lock:
                            return commit_context_locked()
                with runtime._job_cache_lock:
                    if context_payout_digest_stale() or (
                        not context_collection_only
                        and context_append_invalidation_epoch
                        != runtime._payout_ledger_append_invalidation_epoch
                    ):
                        return False
                    with runtime.lock:
                        return commit_context_locked()

            if commit_guard_lock is None:
                committed = commit_context()
            else:
                # Per-client state is acquired before coordinator admission.
                # A busy share can delay only this delivery; it cannot hold
                # runtime.lock while the delivery waits for Vardiff state.
                with commit_guard_lock:
                    committed = commit_context()
            if not committed:
                return False
            phase_started = time.monotonic()
            runtime.send_job_update(client, context.job)
            payout_admitted.mark_delivered()
            runtime.apply_job_difficulty(client, context.job)
            runtime.note_tip_work_delivered(client, str(context.template["previousblockhash"]))
            delivered_monotonic = time.monotonic()
            runtime._record_first_payout_delivery(
                context_payout_generation,
                delivered_monotonic,
            )
            runtime._record_progress_delivery(
                client,
                context,
                delivered_monotonic,
            )
            runtime._consume_retained_collection_refresh(context)
            runtime.note_initial_job_delivered(
                client,
                validated_current=guarded_refresh,
            )
            phases["send"] = delivered_monotonic - phase_started
            elapsed = time.monotonic() - started
            runtime.observe_job_build_elapsed(elapsed, phases)
            if getattr(runtime, "hot_path_log_enabled", False):
                phase_report = ",".join(
                    f"{phase}:{phases[phase]:.3f}"
                    for phase in PRISM_JOB_BUILD_PHASES
                    if phase in phases
                )
                print(
                    f"prism coordinator: sent job connection={client.connection_id} username={client.username} "
                    f"job={context.job.job_id} collection={context.collection_only} elapsed={elapsed:.3f}s "
                    f"phases={phase_report}",
                    flush=True,
                )
            return True

    def prune_client_active_jobs(self, client: ClientState) -> None:
        runtime = self._runtime
        for job_id in tuple(client.active_job_ids):
            if job_id not in self.jobs:
                client.active_job_ids.discard(job_id)
        ordered_active_job_ids = [
            job_id for job_id in self.jobs if job_id in client.active_job_ids
        ]
        while len(ordered_active_job_ids) > MAX_ACTIVE_PRISM_JOBS_PER_CLIENT:
            oldest_job_id = ordered_active_job_ids.pop(0)
            client.active_job_ids.remove(oldest_job_id)
            runtime.bury_evicted_job(client, oldest_job_id)
            self.jobs.pop(oldest_job_id, None)

    def apply_job_difficulty(self, client: ClientState, job: direct_stratum.DirectQbitStratumJob) -> None:
        runtime = self._runtime
        with runtime._client_vardiff_lock(client):
            config = (
                client.vardiff_config
                or client.listener_vardiff_config
                or runtime.vardiff_config
            )
            if not config.enabled:
                client.share_difficulty = job.share_difficulty
                client.pending_share_difficulty = None
                return
            pending = client.pending_share_difficulty
            client.share_difficulty = job.share_difficulty
            if pending is not None and job.share_difficulty == pending:
                client.pending_share_difficulty = None

    def apply_client_difficulty_requests(self, client: ClientState) -> Decimal | None:
        """Specialize the client's difficulty policy from its recorded requests
        (password ``d=``/``md=`` and ``mining.suggest_difficulty``), clamped to
        the pristine listener bounds. The listener floor always wins: on a
        high-diff listener no request can drop a client below the configured
        minimum. Explicit ``d=`` outranks a suggestion. Returns the resolved
        target difficulty, or None when the client requested nothing."""
        runtime = self._runtime
        with runtime._client_vardiff_lock(client):
            base = client.listener_vardiff_config or runtime.vardiff_config
            requested = (
                client.requested_difficulty
                if client.requested_difficulty is not None
                else client.suggested_difficulty
            )
            if requested is None and client.requested_min_difficulty is None:
                # No live requests: drop any stale specialization so the client
                # falls back to the pristine listener policy.
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

    def advertise_client_difficulty(self, client: ClientState, target: Decimal) -> bool:
        """Move a client to an explicitly requested difficulty.

        Before the client can receive jobs the value is applied directly (the
        first set_difficulty/notify pair picks it up). Afterwards it uses the
        same job-gated pending mechanism as vardiff retargets: the difficulty
        is advertised together with the job it applies to, or not at all.
        Returns True only when a fresh set_difficulty/notify pair went out, so
        callers about to send their own job can skip a duplicate pair."""
        runtime = self._runtime
        with client.job_update_lock:
            return runtime._advertise_client_difficulty_locked(client, target)

    def _advertise_client_difficulty_locked(
        self,
        client: ClientState,
        target: Decimal,
    ) -> bool:
        runtime = self._runtime
        applied_directly = False
        schedule_initial = False
        with runtime._client_vardiff_lock(client):
            current = client.pending_share_difficulty or client.share_difficulty
            if target == current:
                return False
            if not (client.subscribed and client.authorized) or (
                client.active_job is None and "maybe_send_job" not in runtime.__dict__
            ):
                client.share_difficulty = target
                client.pending_share_difficulty = None
                client.difficulty_generation = int(
                    getattr(client, "difficulty_generation", 0)
                ) + 1
                applied_directly = True
                schedule_initial = bool(
                    client.subscribed
                    and client.authorized
                    and client.worker is not None
                )
            else:
                prior_pending = client.pending_share_difficulty
                prior_generation = int(
                    getattr(client, "difficulty_generation", 0)
                )
                advertised_generation = prior_generation + 1
                client.pending_share_difficulty = target
                client.difficulty_generation = advertised_generation
        if applied_directly:
            if schedule_initial:
                # A pending first-job request captured the previous difficulty
                # generation. Replace it atomically so its cancellation callback
                # hands the client slot to current work instead of disconnecting.
                runtime.request_initial_job_delivery(client)
            return False
        runtime._ensure_initial_job_state()
        # Lock-protected membership read; the replacement decision itself is
        # re-taken exactly under the same lock in schedule_initial_job.
        with self._initial_job_admission_lock:
            initial_pending = client in self._pending_initial_jobs
        if initial_pending:
            runtime.request_initial_job_delivery(client)
            return False
        if not runtime.stop_event.is_set() and runtime.maybe_send_job(client, clean_jobs=True):
            return True
        with runtime._client_vardiff_lock(client):
            if (
                client.pending_share_difficulty == target
                and int(getattr(client, "difficulty_generation", 0))
                == advertised_generation
            ):
                client.pending_share_difficulty = prior_pending
                client.difficulty_generation = prior_generation
        return False

    def send_job_update(
        self,
        client: ClientState,
        job: direct_stratum.DirectQbitStratumJob,
    ) -> None:
        runtime = self._runtime
        # Preserve instance-level send method replacements used by focused
        # tests; normal coordinators use the atomic socket batch below.
        if "send_difficulty" in runtime.__dict__ or "send_job" in runtime.__dict__:
            runtime.send_difficulty(client, job)
            runtime.send_job(client, job)
            return
        payloads = [
            runtime.difficulty_payload(job.share_difficulty),
            runtime.job_payload(job),
        ]
        shared_params = getattr(job, "notify_shared_params_json", None)
        if shared_params is None:
            client.send_batch(payloads)
            return
        # Splice the per-client fields around the fragment serialized once
        # per bundle build; at fleet scale the coinb1/coinb2/merkle parts
        # dominate the notify and are byte-identical for every client.
        notify_line = (
            '{"id": null, "method": "mining.notify", "params": ['
            + json.dumps(job.job_id)
            + ", "
            + shared_params
            + ", "
            + ("true" if job.clean_jobs else "false")
            + "]}"
        )
        client.send_batch(
            payloads,
            preserialized=(
                json.dumps(payloads[0]).encode()
                + b"\n"
                + notify_line.encode()
                + b"\n"
            ),
        )

    def build_job_for_client(self, client: ClientState, *, clean_jobs: bool) -> PrismJobContext:
        runtime = self._runtime
        if client.worker is None:
            raise self._stratum_error(20, "client is not authorized")
        runtime._ensure_job_cache_state()
        artifacts = (
            runtime._retained_collection_artifacts()
            or runtime.job_issuance_template_artifacts()
        )
        return runtime.build_job_for_client_from_artifacts(
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
        publication_critical: bool = False,
        request_source: str = "routine",
    ) -> PrismJobContext:
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        phases = runtime._job_build_phases()
        while True:
            worker = client.worker
            if worker is None:
                raise self._stratum_error(20, "client is not authorized")
            progress_build_token = runtime._progress_bundle_build_started()
            try:
                cached_bundle = runtime.shared_job_bundle(
                    artifacts,
                    worker,
                    publication_critical=publication_critical,
                    request_source=request_source,
                )
            finally:
                runtime._progress_bundle_build_finished(progress_build_token)
            current_worker = client.worker
            if not cached_bundle.collection_only or current_worker == worker:
                break
            # Reauthorization changed a genuine collection input while the
            # worker-specific bundle was being built. Re-select the latest
            # identity without refetching or discarding the exact artifacts.
        stamp_started = time.monotonic()
        context = runtime.stamp_job_for_client(client, cached_bundle, clean_jobs=clean_jobs)
        phases["stamp"] = phases.get("stamp", 0.0) + (time.monotonic() - stamp_started)
        return context

    def _acquire_client_job_lock(
        self,
        client: ClientState,
        cancelled: Callable[[], bool],
    ) -> bool:
        while not cancelled():
            if client.job_update_lock.acquire(timeout=0.1):
                return True
        return False

    @contextmanager
    def _cancellable_client_job_lock(
        self,
        client: ClientState,
        cancelled: Callable[[], bool],
    ) -> Iterator[bool]:
        acquired = self._acquire_client_job_lock(client, cancelled)
        try:
            yield acquired
        finally:
            if acquired:
                client.job_update_lock.release()

    @staticmethod
    def _client_has_delivered_work_locked(client: ClientState) -> bool:
        """Return whether a socket write completed for any usable job."""

        return bool(
            client.tip_work_delivered is not None
            or client._progress_delivered_context is not None
        )

    def _enforce_evicted_same_tip_capacity_locked(
        self,
        connection_id: int | None = None,
    ) -> None:
        runtime = self._runtime
        connection_ids = (
            (connection_id,)
            if connection_id is not None
            else tuple(self.evicted_same_tip_by_connection)
        )
        per_connection_cap = int(runtime.same_tip_job_retention_per_connection)
        for candidate_connection_id in connection_ids:
            job_ids = self.evicted_same_tip_by_connection.get(candidate_connection_id)
            while job_ids is not None and len(job_ids) > per_connection_cap:
                oldest_job_id = next(iter(job_ids))
                self._remove_evicted_job_locked(oldest_job_id)
                self.evicted_job_capacity_eviction_counts["connection"] += 1
                job_ids = self.evicted_same_tip_by_connection.get(candidate_connection_id)

    def _evicted_job_expired_locked(
        self,
        entry: EvictedJobEntry,
        *,
        now: float,
    ) -> tuple[str, bool]:
        runtime = self._runtime
        job_class = runtime._evicted_job_class_locked(entry)
        if job_class == "same_tip":
            ttl = float(runtime.same_tip_job_retention_seconds)
            return job_class, ttl <= 0 or now - entry.evicted_monotonic > ttl
        return job_class, self._stale_grace_entry_expired_locked(
            entry,
            now=now,
            ttl=float(
                getattr(
                    runtime,
                    "stale_grace_seconds",
                    DEFAULT_PRISM_STALE_GRACE_SECONDS,
                )
            ),
        )

    def _index_evicted_job_locked(self, job_id: str, entry: EvictedJobEntry) -> None:
        runtime = self._runtime
        self.evicted_jobs_by_connection.setdefault(
            entry.connection_id,
            OrderedDict(),
        )[job_id] = None
        if runtime._evicted_job_class_locked(entry) != "same_tip":
            return
        self.evicted_same_tip_by_connection.setdefault(
            entry.connection_id,
            OrderedDict(),
        )[job_id] = None
        self.evicted_same_tip_job_ids[job_id] = None

    def _rebuild_evicted_job_indexes_locked(self) -> None:
        runtime = self._runtime
        self.evicted_jobs_by_connection = {}
        self.evicted_same_tip_by_connection = {}
        self.evicted_same_tip_job_ids = OrderedDict()
        for job_id, entry in self.evicted_job_graveyard.items():
            self._index_evicted_job_locked(job_id, entry)
        self.evicted_job_index_tip_hash = runtime._current_published_tip_hash_locked()
        self._enforce_evicted_same_tip_capacity_locked()

    def _remove_evicted_job_locked(self, job_id: str) -> EvictedJobEntry | None:
        entry = self.evicted_job_graveyard.pop(job_id, None)
        if entry is None:
            return None
        connection_jobs = self.evicted_jobs_by_connection.get(entry.connection_id)
        if connection_jobs is not None:
            connection_jobs.pop(job_id, None)
            if not connection_jobs:
                self.evicted_jobs_by_connection.pop(entry.connection_id, None)
        connection_jobs = self.evicted_same_tip_by_connection.get(entry.connection_id)
        if connection_jobs is not None:
            connection_jobs.pop(job_id, None)
            if not connection_jobs:
                self.evicted_same_tip_by_connection.pop(entry.connection_id, None)
        self.evicted_same_tip_job_ids.pop(job_id, None)
        self._disconnected_evicted_job_ids.pop(job_id, None)
        return entry

    def _stale_grace_entry_expired_locked(
        self,
        entry: EvictedJobEntry,
        *,
        now: float,
        ttl: float,
    ) -> bool:
        runtime = self._runtime
        current_tip = runtime._current_published_tip_hash_locked()
        first_seen = getattr(runtime, "current_tip_first_seen", None)
        if (
            ttl <= 0
            or current_tip is None
            or first_seen is None
            or str(first_seen[0]) != current_tip
            or first_seen[1] is None
        ):
            return True

        # Submit eligibility is exactly one chain parent behind, so pruning
        # must use that same relationship. The prior poll observation can lag
        # (for example when authorize/vardiff issued work on an intermediate
        # tip), and using it here would drop work submit would still credit.
        # Until the parent RPC has populated the cache, retain conservatively;
        # submit classification fetches it before granting stale grace.
        cached_parent = getattr(runtime, "current_tip_parent", None)
        if (
            cached_parent is not None
            and cached_parent[0] == current_tip
            and entry.previousblockhash != cached_parent[1]
        ):
            return True

        client = entry.client
        if client is not None:
            delivered = client.tip_work_delivered
            if delivered is None or delivered[0] != current_tip:
                # Match stale_grace_deadline_open: prior-tip shares stay in
                # flight until this connection receives replacement work.
                return False
            anchor = delivered[1]
        else:
            # Disconnect normally removes these entries. Keep legacy/test
            # orphan state bounded from the refresh path's tip-flip anchor.
            anchor = float(first_seen[1])
        return now - anchor > ttl

    def _enforce_disconnected_evicted_capacity_locked(self) -> None:
        runtime = self._runtime
        cap = int(
            getattr(
                runtime,
                "disconnected_job_retention",
                DEFAULT_PRISM_DISCONNECTED_JOB_RETENTION,
            )
        )
        while len(self._disconnected_evicted_job_ids) > max(0, cap):
            oldest_job_id = next(iter(self._disconnected_evicted_job_ids))
            self._remove_evicted_job_locked(oldest_job_id)
            self.evicted_job_capacity_eviction_counts["disconnected"] += 1

    def cleanup_disconnected_client(self, client: ClientState) -> None:
        """Retire a disconnected client's job/retention state (S1 port).

        The caller (the session owner's disconnect path) already holds
        ``client.job_update_lock``; every mixed lock path uses
        ``job_update_lock -> coordinator lock`` and this method takes the
        coordinator lock itself.
        """
        runtime = self._runtime
        with runtime.lock:
            runtime._ensure_evicted_job_state()
            retention_cap = int(
                getattr(
                    runtime,
                    "disconnected_job_retention",
                    DEFAULT_PRISM_DISCONNECTED_JOB_RETENTION,
                )
            )
            if retention_cap > 0:
                # Devices behind a flapping proxy keep mining the
                # jobs this connection delivered; bury the active
                # ones so their in-flight shares can resume against
                # the retained context after the reconnect.
                for job_id in tuple(client.active_job_ids):
                    runtime.bury_evicted_job(client, job_id, prune=False)
            for job_id in tuple(client.active_job_ids):
                self.jobs.pop(job_id, None)
            client.active_job_ids.clear()
            client.active_job = None
            for job_id in tuple(
                self.evicted_jobs_by_connection.get(client.connection_id, ())
            ):
                entry = self.evicted_job_graveyard.get(job_id)
                if entry is None:
                    continue
                if (
                    retention_cap <= 0
                    or self._evicted_job_class_locked(entry) != "same_tip"
                ):
                    # Stale-grace work stays tied to the connection
                    # that received it (its expiry anchors on that
                    # connection's delivered work); only fully valid
                    # same-tip contexts survive the disconnect.
                    self._remove_evicted_job_locked(job_id)
                    continue
                # Detach from the dead connection object: expiry then
                # anchors on the entry's own eviction time (same-tip
                # TTL) or the published tip flip, never on delivery
                # state this connection can no longer advance.
                self.evicted_job_graveyard[job_id] = dataclass_replace(
                    entry,
                    client=None,
                )
                self._disconnected_evicted_job_ids[job_id] = None
            self._enforce_disconnected_evicted_capacity_locked()
            client.authorized = False
            client.worker = None
            client.username = ""

    def _cross_connection_evicted_entry_allowed_locked(
        self,
        entry: EvictedJobEntry,
        client: ClientState,
    ) -> bool:
        """Whether a retained context of a dead connection may serve this
        submitter.

        Only same-tip contexts qualify (they are fully valid current work;
        stale-grace expiry anchors on the original connection's delivery
        state, which a dead connection can never advance), and only for an
        authorized reconnect of the same username -- the credit goes to the
        context's original worker identity either way, so a foreign
        submitter could at most donate work, but refusing keeps job ids
        unguessable-by-effect and the accounting per miner honest. Replays
        stay rejected by the (username, header) dedup key.
        """
        runtime = self._runtime
        if entry.client is not None:
            # The owning connection is still alive; job ids stay scoped to it.
            return False
        if (
            int(
                getattr(
                    runtime,
                    "disconnected_job_retention",
                    DEFAULT_PRISM_DISCONNECTED_JOB_RETENTION,
                )
            )
            <= 0
        ):
            return False
        if not client.authorized or not client.username:
            return False
        worker = getattr(entry.context, "worker", None)
        username = getattr(worker, "username", None)
        if not username or username != client.username:
            return False
        return runtime._evicted_job_class_locked(entry) == "same_tip"


__all__ = [
    "DEFAULT_PRISM_EVICTED_JOB_PRUNE_INTERVAL_SECONDS",
    "DEFAULT_PRISM_INITIAL_JOB_MAX_WORKERS",
    "EvictedJobEntry",
    "JobBuildFailed",
    "JobDeliveryRuntime",
    "JobDeliveryService",
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
]
