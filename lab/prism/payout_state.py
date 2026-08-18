#!/usr/bin/env python3
"""PRISM payout-state ownership: generations, artifacts, previews, fencing.

This module owns the P1 domain: the payout-state generation and its
publication/delivery gate, the payout ledger artifact (incremental windows,
reuse/debounce/re-anchor policy and its background preparation executor),
accepted-block payout previews and their landing transitions, and the #112
append/anchor fencing (inflight scan anchors, the published seedless job
window anchor, the append invalidation epoch, the rare append-vs-submit
landing fence, and the unfenced-append registry/drain).

It never imports ``prism_coordinator``.  The ledger handle, J1 job cache and
scheduler, R1 refresh scheduling, G1 progress recording, S3's pending-commit
floor, the shutdown controller, and live configuration attributes are reached
through the :class:`PayoutStateRuntime` typed port, resolved at call time so
the historical coordinator monkeypatch seams (including the instance-level
facade patches used by the current test suite) keep intercepting exactly as
before the extraction.  Owner facades route back into this service; that
round trip is deliberate.

Cross-owner lock order is unchanged by the extraction: payout publication and
delivery use this owner's delivery gate and balance-mutation lock; append
classification briefly takes and releases the state lock, then takes the
append-landing fence around the ledger commit; landing drains unfenced
appends without the fence, then holds fence -> state for the final epoch
check; no socket or RPC work runs while registry/retained locks are held.
"""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field, replace as dataclass_replace
import hashlib
import json
import threading
import time
from typing import Any, Callable, Iterator, Protocol, Sequence

from lab.prism.coordinator_config import (
    DEFAULT_ACCEPTED_PARENT_UNRESOLVED_DEPTH_MAX,
    DEFAULT_PRISM_PAYOUT_ARTIFACT_FULL_RESCAN_SECONDS,
    DEFAULT_PRISM_PAYOUT_ARTIFACT_MAX_ANCHOR_AGE_SECONDS,
    DEFAULT_PRISM_PAYOUT_ARTIFACT_REANCHOR_SECONDS,
    DEFAULT_PRISM_PAYOUT_ARTIFACT_REARM_MIN_SECONDS,
)
from lab.prism.coordinator_shutdown import (
    BLOCK_SUBMITTER_WAIT_HEARTBEAT_SLICE_SECONDS,
)
from lab.prism.share_ledger import (
    IncrementalShareJsonSequence,  # noqa: F401 - annotation/compatibility export
    IncrementalShareWindow,
    IncrementalWindowAdvanceStats,
    IncrementalWindowFallback,
    PendingShare,  # noqa: F401 - annotation/compatibility export
)


DEFAULT_ACCEPTED_BLOCK_PAYOUT_PREVIEW_WAIT_SECONDS = 5.0
DEFAULT_PRISM_PAYOUT_RECONCILE_SUPERSESSION_RETRIES = 8
PRISM_PAYOUT_DELIVERY_GENERATIONS = ("current", "stale", "future")
# Consecutive aborted speculative rebuilds double the re-arm interval up to
# this multiplier (80s at the default floor). Rebuilds abort on generation
# supersession, snapshot errors, empty windows, or a pathologically old
# pending-commit floor; backing off stops database preparation from cycling --
# and from holding the payout preparation lock tip builds also need --
# while the underlying condition persists. Any successful install or
# event-driven preparation resets the backoff.
PRISM_PAYOUT_ARTIFACT_REARM_BACKOFF_CAP = 16
# Owner-local copies of the still-coordinator-visible reward-window constants
# and the admission poll cadence; compatibility duplicates by design so this
# leaf module never imports upward.
PRISM_REWARD_WINDOW_MULTIPLIER = 8
PRISM_SNAPSHOT_WINDOW_MARGIN = 2
PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS = 0.05
# Payout preparation/publication/first-delivery latency buckets; value-equal
# to the tip-refresh owner's seconds buckets so exposition is unchanged.
PRISM_PAYOUT_SECONDS_BUCKETS = (
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


def canonical_json_text(value: object) -> str:
    """Owner-local canonical serialization (no reverse imports by design)."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def canonical_json_sha256(value: object) -> str:
    """Owner-local canonical digest (no reverse imports by design)."""
    incremental_digest = getattr(value, "canonical_json_sha256", None)
    if callable(incremental_digest):
        return str(incremental_digest())
    return hashlib.sha256(canonical_json_text(value).encode()).hexdigest()


class TemplateRefreshBlocked(RuntimeError):
    """A live template was fetched, but safe work could not be issued."""


class TemplateRefreshSuperseded(TemplateRefreshBlocked):
    """Concurrent tip/payout progress invalidated this refresh attempt.

    Raised only for coordination races that a scheduled retry resolves on its
    own: the tip advanced mid-refresh, the payout-state generation moved, or a
    newer observation superseded the prepared work. Unlike its parent, this
    subclass never arms the ordinary template-refresh failure budget. Repeated
    coordination blocks are tracked by their own longer deadline; a genuine
    RPC/build/trust failure must raise plain TemplateRefreshBlocked so it still
    takes the ordinary budgeted restart path.
    """


class PayoutStatePublicationBlocked(TemplateRefreshBlocked):
    """Job construction is waiting for a prepared payout publication."""


@dataclass(frozen=True)
class PayoutStateArtifact:
    """Immutable ledger-backed inputs published with one payout generation."""

    generation: int
    source_generation: int
    prior_balances_json: str = field(repr=False)
    prior_balances_sha256: str
    prepared_monotonic: float

    def prior_balances(self) -> list[dict[str, object]]:
        value = json.loads(self.prior_balances_json)
        if not isinstance(value, list):
            raise RuntimeError("published payout artifact is not a balance list")
        return value


@dataclass(frozen=True)
class AcceptedBlockPayoutTransition:
    """Prospective balances for one durable candidate across its landing seam."""

    block_height: int | None = None
    landed: bool = False
    preview: tuple[tuple[str, str, str, int], ...] | None = None
    published_generation: int | None = None
    landed_monotonic: float | None = None


@dataclass(frozen=True)
class PayoutStateCandidate:
    """Immutable result of payout work prepared outside delivery admission."""

    base_generation: int
    source_generation: int
    source_tip_hash: str | None
    cause: str
    invalidated_monotonic: float
    prepared_monotonic: float
    accepted_block_hash: str | None = None
    accepted_block_preview: tuple[tuple[str, str, str, int], ...] | None = None
    accepted_block_withdrawal: bool = False
    accepted_block_height: int | None = None
    ledger_artifact: PayoutLedgerArtifact | None = field(
        default=None,
        compare=False,
        repr=False,
    )


@dataclass(frozen=True)
class PublishedPayoutState:
    """The payout snapshot identity to which cached jobs are stamped."""

    generation: int
    source_generation: int
    source_tip_hash: str | None
    published_monotonic: float
    artifact: PayoutStateArtifact | None = field(default=None, repr=False)


@dataclass(frozen=True)
class PayoutLedgerArtifact:
    """Immutable ledger input prepared independently of a qbit template."""

    generation: int
    payout_state_generation: int
    network_difficulty: int
    # Durable accepted-share total observed near the snapshot. Informational
    # (diagnostics only): validity is scoped to snapshot_anchor_ms, and the
    # count cannot be scoped to a clamped anchor because share stamping and
    # writer enqueue are not atomic.
    accepted_share_count: int
    shares_json: Sequence[dict[str, object]] = field(repr=False)
    prior_balances: tuple[dict[str, object], ...] = field(repr=False)
    prepared_monotonic: float
    # The anchor the share snapshot was actually taken at. Bundles built from
    # this artifact must declare it as anchor_job_issued_at_ms: an auditor
    # replaying qbit_audit_share_window at the declared anchor must reproduce
    # exactly these shares, which only holds at the snapshot's own anchor.
    # That declaration is also what makes reuse valid while shares keep
    # landing: a share stamped after this anchor deterministically belongs to
    # the next window (it is never lost), so the artifact stays
    # audit-reproducible for as long as it serves. Validity is event-driven
    # (the payout-generation and balances fences); wall-clock time only
    # enters through the loose anchor ceiling that backstops how far a
    # served window may trail the live ledger, never through install age.
    snapshot_anchor_ms: int | None = None
    # Canonical digest of shares_json. Cached bundles built before this
    # artifact was armed may only keep serving re-keyed lookups when their
    # own window digest matches; anything else must rebuild from the
    # artifact so a fresher window is never shadowed by older cached work.
    share_snapshot_sha256: str | None = None
    # Canonical digest of prior_balances, memoized at construction. The
    # reuse probe hashes the balances on every serving decision and the
    # reused-build fences hash them twice more; the tuple is immutable, so
    # the O(accounts) canonicalization is paid once here instead.
    prior_balances_sha256: str | None = None
    # Internal coordination token. A late-visible append can invalidate a
    # previously exact anchored window without changing payout generation.
    # Artifacts prepared before that append may never be armed again.
    append_invalidation_epoch: int = field(
        default=0,
        compare=False,
        repr=False,
    )
    # Exposed in-flight scan anchor carried from the synchronous bundle
    # build that seeded this artifact (see _expose_inflight_scan_anchor).
    # Nothing else exposes this window's anchor between the ledger read and
    # the install fence, so the exposure rides here and the install retires
    # it once the fence settles either way.
    inflight_scan_anchor_token: int | None = field(
        default=None,
        compare=False,
        repr=False,
    )
    # Build-path diagnostics only. They do not participate in artifact
    # equality or any signed/wire representation.
    window_build_mode: str | None = field(default=None, compare=False, repr=False)
    window_delta_rows: int = field(default=0, compare=False, repr=False)
    window_expired_rows: int = field(default=0, compare=False, repr=False)
    window_touched_pages: int = field(default=0, compare=False, repr=False)
    window_build_seconds: float = field(default=0.0, compare=False, repr=False)
    window_full_rescan_reason: str | None = field(
        default=None,
        compare=False,
        repr=False,
    )


@dataclass(frozen=True)
class _IncrementalPayoutArtifactWindow:
    """Coordinator-owned exact window plus its canonical materialization."""

    window: IncrementalShareWindow
    shares_json: IncrementalShareJsonSequence = field(repr=False)
    share_snapshot_sha256: str
    refreshed_monotonic: float
    full_rescan_monotonic: float
    full_rescan_attempt_monotonic: float
    append_invalidation_epoch: int = 0


@dataclass(frozen=True)
class _PayoutWindowMaterialization:
    """One exact share-window materialization and bounded-work metadata."""

    shares_json: Sequence[dict[str, object]] = field(repr=False)
    share_snapshot_sha256: str
    snapshot_anchor_ms: int
    mode: str
    record_count: int
    stats: IncrementalWindowAdvanceStats
    full_rescan_reason: str | None = None
    balance_check_prior_balances: tuple[dict[str, object], ...] | None = field(
        default=None,
        repr=False,
    )
    balance_check_mismatch: bool = False


@dataclass
class PayoutDeliveryAdmission:
    admitted: bool
    wait_seconds: float
    generation: int
    published_generation: int
    relation: str
    delivered: bool = False

    def __bool__(self) -> bool:
        return self.admitted

    def mark_delivered(self) -> None:
        if not self.admitted:
            raise RuntimeError("payout delivery completed without admission")
        self.delivered = True


class PayoutStateDeliveryGate:
    """Order delivery admission around a very short payout publication.

    A publisher first closes admission and drains sends that already crossed
    the boundary.  It does not own the atomic publication section while that
    drain is in progress.  Once drained, publication ownership is transferred
    to the caller for the generation/cache pointer swap only.
    """

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._active_deliveries = 0
        self._publisher_waiting = False
        self._mutation_owner: int | None = None
        self._mutation_depth = 0
        self._published_generation = 0
        self._priority_generation: int | None = None
        self._delivery_blocked = False

    @staticmethod
    def _generation_relation(generation: int, published_generation: int) -> str:
        if generation < published_generation:
            return "stale"
        if generation > published_generation:
            return "future"
        return "current"

    @contextmanager
    def delivery(self) -> Iterator[None]:
        with self.delivery_cancelable(lambda: False, priority=True) as admission:
            if not admission:
                raise RuntimeError("uncancelled payout delivery was not admitted")
            yield
            admission.mark_delivered()

    @contextmanager
    def delivery_cancelable(
        self,
        cancelled: Callable[[], bool],
        *,
        generation: int | None = None,
        priority: bool = False,
        poll_seconds: float = PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS,
    ) -> Iterator[PayoutDeliveryAdmission]:
        """Admit a delivery unless cancellation wins while mutation owns the gate."""

        started = time.monotonic()
        admitted = False
        with self._condition:
            if generation is None:
                generation = self._published_generation
            while True:
                if cancelled():
                    break
                if self._delivery_blocked:
                    break
                if generation < self._published_generation:
                    break
                publication_blocked = (
                    self._publisher_waiting or self._mutation_owner is not None
                )
                priority_blocked = (
                    self._priority_generation is not None
                    and generation == self._priority_generation
                    and not priority
                )
                if priority_blocked:
                    # A same-generation job that is not for the published tip
                    # must not occupy a waiter slot indefinitely. Reject it so
                    # its caller can rebuild current-tip work; the reserved
                    # first-delivery lane remains available to priority work.
                    break
                future_blocked = generation > self._published_generation
                if (
                    not publication_blocked
                    and not future_blocked
                ):
                    self._active_deliveries += 1
                    admitted = True
                    break
                self._condition.wait(timeout=poll_seconds)
            published_generation = self._published_generation
            relation = self._generation_relation(generation, published_generation)
            admission = PayoutDeliveryAdmission(
                admitted=admitted,
                wait_seconds=max(0.0, time.monotonic() - started),
                generation=generation,
                published_generation=published_generation,
                relation=relation,
            )
        try:
            yield admission
        finally:
            if admitted:
                with self._condition:
                    if self._active_deliveries <= 0:
                        raise RuntimeError("payout delivery gate released without admission")
                    self._active_deliveries -= 1
                    if (
                        priority
                        and admission.delivered
                        and generation == self._priority_generation
                    ):
                        # Keep routine same-generation sends queued until the
                        # first prioritized current-tip socket delivery exits.
                        # Privileged synchronization admissions do not consume
                        # this reservation merely by leaving the gate.
                        self._priority_generation = None
                    if self._active_deliveries == 0:
                        self._condition.notify_all()
                    elif self._priority_generation is None:
                        self._condition.notify_all()

    @contextmanager
    def publication(self) -> Iterator[None]:
        owner = threading.get_ident()
        with self._condition:
            if self._mutation_owner == owner:
                self._mutation_depth += 1
            else:
                while self._mutation_owner is not None or self._publisher_waiting:
                    self._condition.wait()
                self._publisher_waiting = True
                while self._active_deliveries:
                    self._condition.wait()
                self._mutation_owner = owner
                self._mutation_depth = 1
                self._publisher_waiting = False
        try:
            yield
        finally:
            with self._condition:
                if self._mutation_owner != owner or self._mutation_depth <= 0:
                    raise RuntimeError("payout mutation gate released by non-owner")
                self._mutation_depth -= 1
                if self._mutation_depth == 0:
                    self._mutation_owner = None
                    self._condition.notify_all()

    def publish_generation(self, generation: int, *, prioritize_delivery: bool) -> None:
        owner = threading.get_ident()
        with self._condition:
            if self._mutation_owner != owner:
                raise RuntimeError("payout generation published outside atomic section")
            if generation <= self._published_generation:
                raise RuntimeError("payout generation did not advance")
            self._published_generation = generation
            self._priority_generation = generation if prioritize_delivery else None
            self._delivery_blocked = False

    def block_delivery(
        self,
        mark_blocked: Callable[[], bool] | None = None,
    ) -> bool:
        """Reject admission until publication, atomically with caller state."""

        with self._condition:
            # A publisher drops the condition while swapping its immutable
            # pointer. Wait only for that short section, never for admitted
            # socket sends or fanout cancellation.
            while self._mutation_owner is not None:
                self._condition.wait()
            if mark_blocked is not None and not mark_blocked():
                return False
            self._delivery_blocked = True
            self._condition.notify_all()
            return True

    @contextmanager
    def mutation(self) -> Iterator[None]:
        """Compatibility alias for tests and callers that only need exclusion."""

        with self.publication():
            yield


@dataclass(frozen=True)
class PayoutStateSnapshot:
    """Copied identity facts for cross-owner delivery decisions."""

    generation: int
    published: PublishedPayoutState
    publication_blocked: bool


class PayoutStateRuntime(Protocol):
    """Typed port over the coordinator, resolved at call time.

    Every member is looked up on the live coordinator object when used, so
    instance monkeypatches (``server._prepared_payout_state_candidate = ...``
    and friends) and coordinator-owned live configuration attributes keep
    working exactly as before the extraction.  Owner facades route back into
    this service; that round trip is deliberate.  The legacy P1 field names
    (``_payout_state_generation`` through ``payout_artifact_event_counts``)
    also resolve here -- coordinator class descriptors route them to this
    service's single mutable copy.
    """

    # Cross-domain objects and live configuration attributes.
    _job_cache_lock: Any
    ledger: Any
    lock: Any
    stop_event: Any

    def _ensure_job_cache_state(self, *args: Any, **kwargs: Any) -> Any: ...

    def _ensure_shutdown_controller(self, *args: Any, **kwargs: Any) -> Any: ...

    def _record_heartbeat(self, *args: Any, **kwargs: Any) -> Any: ...

    def _record_block_submitter_phase(self, *args: Any, **kwargs: Any) -> Any: ...

    def _block_work_wait_slice(self, *args: Any, **kwargs: Any) -> Any: ...

    def _record_progress_payout_generation(self, *args: Any, **kwargs: Any) -> Any: ...

    def _writer_operation(self, *args: Any, **kwargs: Any) -> Any: ...


class PayoutStateService:
    """Sole owner of P1 payout state, artifacts, previews, and fencing."""

    def __init__(
        self,
        runtime: PayoutStateRuntime,
        *,
        shutdown_error: type[BaseException],
        now_ms: Callable[[], int],
        canonical_json_text_override: Callable[[object], str] = canonical_json_text,
        canonical_json_sha256_override: Callable[[object], str] = canonical_json_sha256,
    ) -> None:
        self._runtime = runtime
        # The shutdown exception type stays coordinator-owned until the
        # shutdown controller is extracted, the wall-clock stamp and the
        # canonical serialization resolve the coordinator module's globals at
        # call time (tests patch them there); all are injected so this leaf
        # module never imports prism_coordinator.
        self._shutdown_error = shutdown_error
        self._now_ms = now_ms
        self._canonical_json_text = canonical_json_text_override
        self._canonical_json_sha256 = canonical_json_sha256_override
        self._payout_state_generation = 0
        # Ledger mutations and the ledger reads used to build signed jobs
        # share this lock. They may be expensive, but they never block
        # delivery of an already-published immutable generation.
        self._payout_state_prepare_lock = threading.RLock()
        self._payout_state_source: tuple[int, str | None, str, float] = (
            0,
            None,
            "startup",
            time.monotonic(),
        )
        self._published_payout_state = PublishedPayoutState(
            generation=self._payout_state_generation,
            source_generation=0,
            source_tip_hash=None,
            published_monotonic=time.monotonic(),
        )
        self._payout_ledger_artifact: PayoutLedgerArtifact | None = None
        self._payout_ledger_artifact_generation = 0
        # Guarded by _job_cache_lock. Unlike payout generation, this
        # advances only when a newly visible row predates an anchored
        # share window and makes pre-append work unsafe to publish.
        self._payout_ledger_append_invalidation_epoch = 0
        # Serializes the append-side epoch bump against the landing's
        # final epoch-check-and-submitblock boundary. Ordinary share
        # commits never touch it: the append side acquires it only for
        # rows that predate a live anchor (the rare replay-shaped
        # append), and the landing holds it across exactly one RPC, so
        # an epoch advance can never slip between the landing's last
        # epoch read and the block entering qbitd. Ordered strictly
        # outside _job_cache_lock.
        self._payout_append_landing_fence_lock = threading.Lock()
        # Guarded by _job_cache_lock. The anchors of ledger window walks
        # whose results are not yet visible anywhere else, published
        # before each read begins. A late-visible append can predate such
        # an anchor while no completed window or armed artifact exposes
        # one, and the walk's database snapshot may already exclude the
        # row; the append-side invalidation must still advance the epoch
        # so the walk's result cannot arm or serve. A synchronous bundle
        # build keeps its entry exposed past its own return -- until the
        # seeded artifact's install fence settles -- because nothing else
        # exposes that window's anchor in the meantime.
        self._payout_window_inflight_scan_anchors: dict[int, int] = {}
        self._payout_window_inflight_scan_anchor_token = 0
        # Guarded by _job_cache_lock. One entry per durable append
        # currently committing OUTSIDE the landing fence, holding the
        # batch's most predating row stamp (min over rows of
        # max(job_issued_at_ms, accepted_at_ms)). The unfenced
        # classification is a one-time predicate: an anchor exposed
        # after it cannot retroactively fence the in-flight commit, so
        # a landing that just exposed its declared anchor drains
        # matching entries before its epoch fences arm. The condition
        # shares _job_cache_lock and signals every deregistration.
        self._payout_unfenced_append_inflight_stamps: dict[int, int] = {}
        self._payout_unfenced_append_inflight_token = 0
        self._payout_unfenced_append_drained = threading.Condition(
            runtime._job_cache_lock
        )
        # Guarded by _job_cache_lock. The highest declared anchor among
        # published job windows whose walk exposure retired without a
        # seeded artifact to carry it (a build under the reuse
        # kill-switch, or one with nothing to seed). Jobs stamped from
        # such a bundle keep serving the window until they retire, and
        # nothing else exposes its anchor between publication and the
        # landing's own exposure -- so the anchor must stay visible to
        # the append-side predates() checks: a replay-shaped append
        # committing in that gap has to advance the epoch those jobs'
        # landing fences compare against. Monotonic and never retired;
        # ordinary share commits can never match it because every job
        # anchor is clamped below the pending-commit floor.
        self._payout_published_job_window_anchor_ms: int | None = None
        # Guarded by _payout_state_prepare_lock. It is independent of the
        # published artifact generation: normal generation bumps retag
        # the same immutable share window without another ledger walk.
        self._incremental_payout_artifact_window: (
            _IncrementalPayoutArtifactWindow | None
        ) = None
        # Guarded by _payout_state_prepare_lock. Explicit append-side
        # invalidations leave a one-shot diagnostic for the oracle build
        # that replaces the discarded incremental window.
        self._incremental_payout_artifact_window_invalidation_reason: (
            str | None
        ) = None
        # Guarded by _payout_state_prepare_lock. A periodic balance oracle
        # mismatch keeps the stale published digest off the reuse path
        # until a later publication installs a different digest.
        self._payout_prior_balances_reuse_invalidated_sha256: str | None = None
        self._payout_artifact_executor_lock = threading.Lock()
        self._payout_artifact_executor: ThreadPoolExecutor | None = None
        self._payout_artifact_future: Future[None] | None = None
        self._payout_artifact_requested: tuple[int, int] | None = None
        self._payout_artifact_requested_bypass = False
        self._payout_artifact_executor_shutdown = False
        # Guarded by _payout_artifact_executor_lock. Stamped by every
        # accepted preparation request so fence-failure re-arms measure
        # their debounce from the newest scheduled rebuild, whichever
        # path requested it.
        self._payout_artifact_last_schedule_monotonic: float | None = None
        # Guarded by _payout_artifact_executor_lock.
        self._payout_artifact_rearm_backoff = 1
        # Orders reconciliation mutations against final job-delivery
        # admission while preserving parallel sends to different miners.
        self._payout_state_delivery_gate = PayoutStateDeliveryGate()
        # Keep durable payout-balance transitions serialized even when their
        # preparation intentionally runs outside the delivery gate.  The
        # accepted-block path can hold this lock across expensive writes
        # without preventing replacement jobs from reaching miners.
        self._payout_balance_mutation_lock = threading.RLock()
        self._accepted_block_payout_preview_condition = threading.Condition()
        # Durable replay registers an unlanded transition. Once its block
        # is active, reconciliation is barred and child/descendant builders
        # wait for or consume its verified prospective balance snapshot.
        self._accepted_block_payout_previews: dict[
            str, AcceptedBlockPayoutTransition
        ] = {}
        # A landed transition can be withdrawn before durability catches
        # up. Keep a height-bearing tombstone so a new exact/descendant
        # build cannot fall through to database balances that omit it in
        # the gap before durable retry re-registers the candidate.
        self._invalidated_accepted_block_payout_previews: dict[
            str, int | None
        ] = {}
        self._payout_state_metrics_lock = threading.Lock()
        self.payout_state_histograms = {
            name: {
                "buckets": {
                    bucket: 0 for bucket in PRISM_PAYOUT_SECONDS_BUCKETS
                },
                "sum": 0.0,
                "count": 0,
            }
            for name in ("preparation", "publish", "first_delivery")
        }
        self.payout_gate_wait_histograms = {
            relation: {
                "buckets": {
                    bucket: 0 for bucket in PRISM_PAYOUT_SECONDS_BUCKETS
                },
                "sum": 0.0,
                "count": 0,
            }
            for relation in PRISM_PAYOUT_DELIVERY_GENERATIONS
        }
        self.payout_state_candidates_discarded = 0
        self._payout_first_delivery_pending: tuple[int, float] | None = None
        self._payout_state_publication_blocked = False
        # Artifact lifecycle observability (the 2026-07-29 incident was
        # invisible precisely here). Guarded by
        # _payout_artifact_executor_lock.
        self.payout_artifact_event_counts = {
            event: 0
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
            )
        }

    def snapshot(self) -> PayoutStateSnapshot:
        """Copied cross-owner identity facts (memo rule: no shared mutables)."""
        with self._runtime.lock:
            return PayoutStateSnapshot(
                generation=int(self._payout_state_generation),
                published=self._published_payout_state,
                publication_blocked=bool(self._payout_state_publication_blocked),
            )

    @staticmethod
    def _incremental_window_records_supported(records: Sequence[object]) -> bool:
        """Whether a ledger snapshot exposes the append-only window contract."""

        required = (
            "share_seq",
            "share_id",
            "share_difficulty",
            "job_issued_at_ms",
            "accepted_at_ms",
            "to_prism_json",
        )
        return bool(records) and all(
            all(hasattr(record, attribute) for attribute in required)
            for record in records
        )

    def _full_payout_window_materialization(
        self,
        *,
        snapshot_anchor_ms: int,
        snapshot_window_weight: int,
        reason: str,
        observed_monotonic: float,
        append_invalidation_epoch: int,
    ) -> _PayoutWindowMaterialization:
        """Run the exact ledger oracle and atomically replace cached pages."""
        runtime = self._runtime

        records = list(
            runtime.ledger.snapshot_at_job_issue(
                snapshot_anchor_ms,
                window_weight=snapshot_window_weight,
            )
        )
        zero_stats = IncrementalWindowAdvanceStats(
            added_rows=0,
            expired_rows=0,
            touched_pages=0,
        )
        if not runtime._incremental_window_records_supported(records):
            # Small test/custom ledgers predating the delta contract retain the
            # legacy full-read behavior. Production ledger records always
            # expose the complete AcceptedShareRecord shape.
            runtime._incremental_payout_artifact_window = None
            shares_json = tuple(record.to_prism_json() for record in records)
            return _PayoutWindowMaterialization(
                shares_json=shares_json,
                share_snapshot_sha256=self._canonical_json_sha256(shares_json),
                snapshot_anchor_ms=int(snapshot_anchor_ms),
                mode="full_rescan",
                record_count=len(records),
                stats=zero_stats,
                full_rescan_reason=reason,
            )

        window = IncrementalShareWindow.from_full_snapshot(
            records,
            anchor_job_issued_at_ms=snapshot_anchor_ms,
            window_weight=snapshot_window_weight,
        )
        shares_json = window.json_records()
        digest = self._canonical_json_sha256(shares_json)
        runtime._incremental_payout_artifact_window = (
            _IncrementalPayoutArtifactWindow(
                window=window,
                shares_json=shares_json,
                share_snapshot_sha256=digest,
                refreshed_monotonic=observed_monotonic,
                full_rescan_monotonic=observed_monotonic,
                full_rescan_attempt_monotonic=observed_monotonic,
                append_invalidation_epoch=append_invalidation_epoch,
            )
        )
        return _PayoutWindowMaterialization(
            shares_json=shares_json,
            share_snapshot_sha256=digest,
            snapshot_anchor_ms=int(snapshot_anchor_ms),
            mode="full_rescan",
            record_count=len(shares_json),
            stats=zero_stats,
            full_rescan_reason=reason,
        )

    def _incremental_payout_window_materialization(
        self,
        *,
        snapshot_anchor_ms: int,
        snapshot_window_weight: int,
        force_full_rescan: bool,
        bypass_build_interval: bool,
        append_invalidation_epoch: int,
        reused_prior_balances_sha256: str | None = None,
    ) -> _PayoutWindowMaterialization:
        """Return an exact window using debounce, delta folding, or the oracle.

        Must run under ``_payout_state_prepare_lock``. The cache replacement
        happens only after a complete materialization, except an explicit
        forced invalidation clears it before the oracle so a failed reorg
        check cannot later resume from unverified append-only state.
        """
        runtime = self._runtime

        observed = time.monotonic()
        cached = runtime._incremental_payout_artifact_window
        full_reason: str | None = None
        if force_full_rescan:
            runtime._incremental_payout_artifact_window = None
            runtime._incremental_payout_artifact_window_invalidation_reason = None
            cached = None
            full_reason = "reconcile_invalidation"
        elif cached is None:
            full_reason = (
                runtime._incremental_payout_artifact_window_invalidation_reason
                or "cold_start"
            )
        elif cached.append_invalidation_epoch != append_invalidation_epoch:
            # Append invalidation is published without waiting for this
            # preparation lock. A build that acquires the lock first must not
            # advance the pre-append cache under the new epoch.
            runtime._incremental_payout_artifact_window = None
            cached = None
            full_reason = "late_visible_append"
        elif cached.window.window_weight != int(snapshot_window_weight):
            runtime._incremental_payout_artifact_window = None
            runtime._incremental_payout_artifact_window_invalidation_reason = None
            cached = None
            full_reason = "network_difficulty_changed"
        elif snapshot_anchor_ms < cached.window.anchor_job_issued_at_ms:
            runtime._incremental_payout_artifact_window = None
            runtime._incremental_payout_artifact_window_invalidation_reason = None
            cached = None
            full_reason = "anchor_regression"

        if cached is None:
            materialized = runtime._full_payout_window_materialization(
                snapshot_anchor_ms=snapshot_anchor_ms,
                snapshot_window_weight=snapshot_window_weight,
                reason=full_reason or "cache_invalidated",
                observed_monotonic=observed,
                append_invalidation_epoch=append_invalidation_epoch,
            )
            runtime._incremental_payout_artifact_window_invalidation_reason = None
            return materialized

        min_interval = runtime._payout_artifact_min_build_interval_seconds()
        full_rescan_seconds = float(
            getattr(
                runtime,
                "payout_artifact_full_rescan_seconds",
                DEFAULT_PRISM_PAYOUT_ARTIFACT_FULL_RESCAN_SECONDS,
            )
        )
        # Bypass builds (accepted-block previews) refresh
        # refreshed_monotonic on every block yet never run the periodic
        # oracle themselves, so a sustained sub-interval block cadence
        # would otherwise send every routine build back from this
        # debounce and postpone the configured runtime-check indefinitely.
        # An overdue runtime-check disarms the debounce for routine builds;
        # the urgent preview path stays debounce-free and oracle-free
        # either way.
        self_check_overdue = (
            observed - cached.full_rescan_attempt_monotonic
            >= full_rescan_seconds
        )
        if (
            not bypass_build_interval
            and not self_check_overdue
            and observed - cached.refreshed_monotonic < min_interval
        ):
            return _PayoutWindowMaterialization(
                shares_json=cached.shares_json,
                share_snapshot_sha256=cached.share_snapshot_sha256,
                snapshot_anchor_ms=cached.window.anchor_job_issued_at_ms,
                mode="debounced",
                record_count=sum(
                    len(page.records) for page in cached.window.pages
                ),
                stats=IncrementalWindowAdvanceStats(0, 0, 0),
            )

        delta_reader = getattr(runtime.ledger, "snapshot_between_job_issues", None)
        if not callable(delta_reader):
            return runtime._full_payout_window_materialization(
                snapshot_anchor_ms=snapshot_anchor_ms,
                snapshot_window_weight=snapshot_window_weight,
                reason="delta_api_unavailable",
                observed_monotonic=observed,
                append_invalidation_epoch=append_invalidation_epoch,
            )

        try:
            delta_records = list(
                delta_reader(
                    cached.window.anchor_job_issued_at_ms,
                    snapshot_anchor_ms,
                )
            )
            advanced_window, stats = cached.window.advance(
                delta_records,
                anchor_job_issued_at_ms=snapshot_anchor_ms,
            )
        except (IncrementalWindowFallback, ValueError):
            runtime._incremental_payout_artifact_window = None
            return runtime._full_payout_window_materialization(
                snapshot_anchor_ms=snapshot_anchor_ms,
                snapshot_window_weight=snapshot_window_weight,
                reason="incremental_invariant_failed",
                observed_monotonic=observed,
                append_invalidation_epoch=append_invalidation_epoch,
            )

        if delta_records or stats.expired_rows:
            # Each window page owns its immutable JSON representation. Page
            # identities survive normal advancement, so this view and its
            # digest convert only append/head boundary pages; retained share
            # records are never flattened or re-encoded here.
            shares_json = advanced_window.json_records()
            digest = self._canonical_json_sha256(shares_json)
        else:
            shares_json = cached.shares_json
            digest = cached.share_snapshot_sha256

        advanced = _IncrementalPayoutArtifactWindow(
            window=advanced_window,
            shares_json=shares_json,
            share_snapshot_sha256=digest,
            refreshed_monotonic=observed,
            full_rescan_monotonic=cached.full_rescan_monotonic,
            full_rescan_attempt_monotonic=(
                cached.full_rescan_attempt_monotonic
            ),
            append_invalidation_epoch=append_invalidation_epoch,
        )
        mode = "incremental"
        check_reason: str | None = None
        balance_check_prior_balances: tuple[dict[str, object], ...] | None = None
        balance_check_mismatch = False
        if not bypass_build_interval and self_check_overdue:
            try:
                full_records = list(
                    runtime.ledger.snapshot_at_job_issue(
                        snapshot_anchor_ms,
                        window_weight=snapshot_window_weight,
                    )
                )
                full_window = IncrementalShareWindow.from_full_snapshot(
                    full_records,
                    anchor_job_issued_at_ms=snapshot_anchor_ms,
                    window_weight=snapshot_window_weight,
                )
                full_retained = full_window.records()
                matched = full_retained == advanced_window.records()
                shares_json = full_window.json_records()
                digest = self._canonical_json_sha256(shares_json)
            except Exception:
                # The already-validated delta remains usable. Space failed
                # oracle attempts by the configured runtime-check interval so a
                # saturated/unavailable database cannot turn a periodic
                # safety check into one full CTE retry every minute.
                advanced = dataclass_replace(
                    advanced,
                    full_rescan_attempt_monotonic=observed,
                )
                mode = "incremental_self_check_failed"
                check_reason = "periodic_self_check_failed"
            else:
                advanced = _IncrementalPayoutArtifactWindow(
                    window=full_window,
                    shares_json=shares_json,
                    share_snapshot_sha256=digest,
                    refreshed_monotonic=observed,
                    full_rescan_monotonic=observed,
                    full_rescan_attempt_monotonic=observed,
                    append_invalidation_epoch=append_invalidation_epoch,
                )
                mode = "self_check_match" if matched else "self_check_mismatch"
                check_reason = "periodic_self_check"
                if reused_prior_balances_sha256 is not None:
                    try:
                        checked_balances = tuple(
                            runtime.ledger.current_prior_balances()
                        )
                        checked_balances_sha256 = self._canonical_json_sha256(
                            checked_balances
                        )
                        if (
                            checked_balances_sha256
                            != reused_prior_balances_sha256
                        ):
                            balance_check_prior_balances = checked_balances
                            balance_check_mismatch = True
                            runtime._payout_prior_balances_reuse_invalidated_sha256 = (
                                reused_prior_balances_sha256
                            )
                            # Equal-window refreshes intentionally retain
                            # their armed balances. Drop the stale artifact so
                            # the fresh balance read cannot be discarded as a
                            # window-only no-op when this build is installed.
                            with runtime._job_cache_lock:
                                armed = runtime._payout_ledger_artifact
                                if armed is not None:
                                    armed_balances_sha256 = (
                                        armed.prior_balances_sha256
                                        or self._canonical_json_sha256(
                                            armed.prior_balances
                                        )
                                    )
                                    if (
                                        armed_balances_sha256
                                        == reused_prior_balances_sha256
                                    ):
                                        runtime._payout_ledger_artifact = None
                    except Exception:
                        # The carry backstop is independent of the window
                        # oracle that just succeeded. Reverting here would
                        # serve a window the oracle may have refuted for a
                        # whole rescan interval, so keep the full window and
                        # leave the reused balances unverified until the next
                        # runtime-check.
                        check_reason = (
                            "periodic_self_check_balance_check_failed"
                        )

        runtime._incremental_payout_artifact_window = advanced
        return _PayoutWindowMaterialization(
            shares_json=advanced.shares_json,
            share_snapshot_sha256=advanced.share_snapshot_sha256,
            snapshot_anchor_ms=advanced.window.anchor_job_issued_at_ms,
            mode=mode,
            record_count=sum(len(page.records) for page in advanced.window.pages),
            stats=stats,
            full_rescan_reason=check_reason,
            balance_check_prior_balances=balance_check_prior_balances,
            balance_check_mismatch=balance_check_mismatch,
        )

    def _build_payout_ledger_artifact(
        self,
        expected_payout_state_generation: int,
        artifact_payout_state_generation: int,
        network_difficulty: int,
        force_full_rescan: bool = False,
        bypass_build_interval: bool = False,
        during_publication: bool = False,
    ) -> PayoutLedgerArtifact | None:
        """Build a stable ledger snapshot without publishing it.

        Validity is anchor-scoped, not count-scoped. The pending-commit clamp
        selects the highest clean anchor: every share stamped at or below it
        is already durable, so the window read at that anchor is exact and
        reproducible no matter how many shares commit while the walk runs.
        Shares stamped above the anchor deterministically belong to the next
        window; concurrent writers therefore never invalidate this attempt.
        """
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        build_started = time.monotonic()
        ledger_started = time.monotonic()
        materialized: _PayoutWindowMaterialization
        try:
            with runtime._payout_state_prepare_lock:
                with runtime._job_cache_lock:
                    if (
                        expected_payout_state_generation
                        != runtime._payout_state_generation
                    ):
                        return None
                    append_invalidation_epoch = int(
                        runtime._payout_ledger_append_invalidation_epoch
                    )
                    published_payout_artifact = (
                        runtime._published_payout_state.artifact
                    )
                invalidated_balances_sha256 = (
                    runtime._payout_prior_balances_reuse_invalidated_sha256
                )
                if (
                    invalidated_balances_sha256 is not None
                    and published_payout_artifact is not None
                    and published_payout_artifact.prior_balances_sha256
                    != invalidated_balances_sha256
                ):
                    # A later publication installed a different balance
                    # snapshot, so the digest that failed its oracle check can
                    # no longer enter this generation's reuse path.
                    runtime._payout_prior_balances_reuse_invalidated_sha256 = None
                    invalidated_balances_sha256 = None
                reuse_published_balances = bool(
                    not force_full_rescan
                    and published_payout_artifact is not None
                    and published_payout_artifact.generation
                    == expected_payout_state_generation
                    and published_payout_artifact.prior_balances_sha256
                    != invalidated_balances_sha256
                )
                snapshot_window_weight = (
                    PRISM_REWARD_WINDOW_MULTIPLIER
                    * PRISM_SNAPSHOT_WINDOW_MARGIN
                    * int(network_difficulty)
                )
                clamp_now_ms = self._now_ms()
                snapshot_anchor_ms = runtime._job_snapshot_anchor_ms(clamp_now_ms)
                if (
                    clamp_now_ms - snapshot_anchor_ms
                    > runtime._payout_artifact_max_anchor_age_ms()
                ):
                    # The floor is held this far below now only by a wedged
                    # writer or a leaked release; an artifact anchored there
                    # would already be past the audit ceiling -- born
                    # expired -- on arrival. Abort before paying the window
                    # walk: the re-arm backoff paces retries and the
                    # share-commit liveness watchdog owns recovery.
                    return None
                window_started = time.monotonic()
                # Publish the walk's anchor before the ledger read: a
                # late-visible append committing mid-walk can predate it
                # while a cold or forcibly cleared cache exposes no other
                # anchor, and the walk's database snapshot may already
                # exclude the row. Retiring in the finally is safe here
                # because the materialization installs its incremental
                # pointer -- the walk's durable visibility -- before this
                # exposure is withdrawn.
                inflight_anchor_token = runtime._expose_inflight_scan_anchor(
                    snapshot_anchor_ms
                )
                try:
                    materialized = runtime._incremental_payout_window_materialization(
                        snapshot_anchor_ms=snapshot_anchor_ms,
                        snapshot_window_weight=snapshot_window_weight,
                        force_full_rescan=force_full_rescan,
                        bypass_build_interval=bypass_build_interval,
                        append_invalidation_epoch=append_invalidation_epoch,
                        reused_prior_balances_sha256=(
                            published_payout_artifact.prior_balances_sha256
                            if reuse_published_balances
                            and published_payout_artifact is not None
                            else None
                        ),
                    )
                finally:
                    runtime._retire_inflight_scan_anchor(inflight_anchor_token)
                if materialized.balance_check_mismatch:
                    # Emit at detection time: a later, unrelated artifact
                    # serialization or stats failure must not hide the oracle
                    # alarm that already invalidated reuse.
                    runtime._record_payout_artifact_event(
                        "balance_check_mismatch"
                    )
                    runtime._payout_artifact_log(
                        "payout_artifact_balance_check_mismatch",
                        payout_state_generation=int(
                            artifact_payout_state_generation
                        ),
                        balance_check_mismatch=True,
                    )
                if (
                    materialized.balance_check_mismatch
                    and materialized.balance_check_prior_balances is not None
                    and published_payout_artifact is not None
                ):
                    # The oracle refuted the published balance snapshot, so
                    # invalidating reuse alone is not enough: consumers keyed
                    # to the published digest (the armed-artifact fence, new
                    # build keys, and the synchronous fallback) would keep
                    # serving the refuted bytes until an unrelated generation
                    # change. Publish the repaired balances for the same
                    # generation so this build's artifact can arm and serve.
                    runtime._publish_self_check_repaired_balances(
                        expected_payout_state_generation,
                        stale_prior_balances_sha256=(
                            published_payout_artifact.prior_balances_sha256
                        ),
                        balances=materialized.balance_check_prior_balances,
                    )
                window_build_seconds = time.monotonic() - window_started
                if materialized.balance_check_prior_balances is not None:
                    # The periodic oracle already paid for the live aggregate.
                    # Use that exact result rather than issuing a duplicate
                    # carry query in the mismatch build.
                    prior_balances = (
                        materialized.balance_check_prior_balances
                    )
                    prior_balances_source = "ledger"
                elif reuse_published_balances:
                    assert published_payout_artifact is not None
                    # Prior balances are generation-owned immutable state.
                    # Normal share-window refreshes cannot change them, so
                    # reuse the exact published bytes instead of aggregating
                    # the 1.3M-row carry tables on every debounced generation.
                    # Reconcile/withdrawal mutations force a ledger read.
                    prior_balances = (
                        published_payout_artifact.prior_balances()
                    )
                    prior_balances_source = "published"
                else:
                    prior_balances = runtime.ledger.current_prior_balances()
                    prior_balances_source = "ledger"
            accepted_share_count, _ = runtime.accepted_share_stats()
        except Exception as exc:
            # Artifact preparation is speculative. The synchronous bundle path
            # still owns errors when current work actually requires a snapshot.
            if during_publication:
                runtime._record_payout_artifact_event("build_aborted")
                runtime._payout_artifact_log(
                    "payout_artifact_build_aborted",
                    payout_state_generation=int(
                        artifact_payout_state_generation
                    ),
                    duration_seconds=round(
                        time.monotonic() - build_started,
                        3,
                    ),
                    during_publication=True,
                    build_interval_bypassed=bool(bypass_build_interval),
                    abort_reason=type(exc).__name__,
                )
            return None
        finally:
            runtime._observe_tip_refresh_build_phase(
                "ledger_snapshot",
                time.monotonic() - ledger_started,
            )
        if not materialized.shares_json:
            return None
        copy_started = time.monotonic()
        shares_json = materialized.shares_json
        frozen_balances = tuple(prior_balances)
        runtime._observe_tip_refresh_build_phase(
            "serialization_copy",
            time.monotonic() - copy_started,
        )
        artifact = PayoutLedgerArtifact(
            generation=0,
            payout_state_generation=artifact_payout_state_generation,
            network_difficulty=int(network_difficulty),
            accepted_share_count=accepted_share_count,
            shares_json=shares_json,
            prior_balances=frozen_balances,
            prepared_monotonic=time.monotonic(),
            snapshot_anchor_ms=materialized.snapshot_anchor_ms,
            share_snapshot_sha256=materialized.share_snapshot_sha256,
            prior_balances_sha256=self._canonical_json_sha256(frozen_balances),
            append_invalidation_epoch=append_invalidation_epoch,
            window_build_mode=materialized.mode,
            window_delta_rows=materialized.stats.added_rows,
            window_expired_rows=materialized.stats.expired_rows,
            window_touched_pages=materialized.stats.touched_pages,
            window_build_seconds=window_build_seconds,
            window_full_rescan_reason=materialized.full_rescan_reason,
        )
        build_seconds = time.monotonic() - build_started
        log_fields: dict[str, object] = {
            "payout_state_generation": int(artifact_payout_state_generation),
            "duration_seconds": round(build_seconds, 3),
            "window_build_seconds": round(window_build_seconds, 3),
            "window_build_mode": materialized.mode,
            "window_shares": materialized.record_count,
            "delta_rows": materialized.stats.added_rows,
            "expired_rows": materialized.stats.expired_rows,
            "touched_pages": materialized.stats.touched_pages,
            "anchor_age_ms": self._now_ms() - materialized.snapshot_anchor_ms,
            "during_publication": bool(during_publication),
            "build_interval_bypassed": bool(bypass_build_interval),
            "prior_balances_source": prior_balances_source,
            "balance_check_mismatch": materialized.balance_check_mismatch,
        }
        if materialized.full_rescan_reason is not None:
            log_fields["full_rescan_reason"] = materialized.full_rescan_reason
        if materialized.mode == "debounced":
            runtime._record_payout_artifact_event("debounced")
            runtime._payout_artifact_log(
                "payout_artifact_build_debounced",
                **log_fields,
            )
        else:
            runtime._record_payout_artifact_event("built")
            if materialized.mode == "full_rescan":
                runtime._record_payout_artifact_event("full_rescan")
            elif materialized.mode == "self_check_match":
                runtime._record_payout_artifact_event("self_check_match")
            elif materialized.mode == "self_check_mismatch":
                runtime._record_payout_artifact_event("self_check_mismatch")
            elif materialized.mode == "incremental_self_check_failed":
                runtime._record_payout_artifact_event("incremental")
                runtime._record_payout_artifact_event("self_check_failed")
            else:
                runtime._record_payout_artifact_event("incremental")
            runtime._payout_artifact_log("payout_artifact_built", **log_fields)
        return artifact

    def _prepare_payout_ledger_artifact(
        self,
        payout_state_generation: int,
        network_difficulty: int,
        *,
        bypass_build_interval: bool = False,
    ) -> None:
        """Prepare and atomically publish an artifact for a current generation."""
        runtime = self._runtime
        build_started = time.monotonic()
        artifact = runtime._build_payout_ledger_artifact(
            payout_state_generation,
            payout_state_generation,
            network_difficulty,
            False,
            bypass_build_interval,
        )
        build_seconds = time.monotonic() - build_started
        if artifact is None:
            # A superseding generation, a snapshot error, an empty window, or
            # a pathologically old pending-commit floor discarded this
            # attempt; that condition can persist, and each attempt performs
            # ledger preparation under the shared lock. Back the
            # fence-failure re-arm off until something arms.
            runtime._record_payout_artifact_event("build_aborted")
            runtime._payout_artifact_log(
                "payout_artifact_build_aborted",
                payout_state_generation=int(payout_state_generation),
                duration_seconds=round(build_seconds, 3),
            )
            with runtime._payout_artifact_executor_lock:
                runtime._payout_artifact_rearm_backoff = min(
                    runtime._payout_artifact_rearm_backoff * 2,
                    PRISM_PAYOUT_ARTIFACT_REARM_BACKOFF_CAP,
                )
            return
        if not runtime._install_payout_ledger_artifact(artifact):
            # A discarded install -- born expired after a slow walk, an
            # ordering loss to a fresher window, or a generation change --
            # armed nothing. It must not collapse the re-arm interval: an
            # unconditional reset here is exactly the reset-then-rewalk
            # livelock that rolled back the first anchor-scoped deploy.
            with runtime._payout_artifact_executor_lock:
                runtime._payout_artifact_rearm_backoff = min(
                    runtime._payout_artifact_rearm_backoff * 2,
                    PRISM_PAYOUT_ARTIFACT_REARM_BACKOFF_CAP,
                )

    def _install_payout_ledger_artifact(
        self,
        artifact: PayoutLedgerArtifact,
    ) -> bool:
        """Atomically publish a prepared artifact for its own generation.

        Snapshot-freshness-ordered and idempotent: every snapshot is taken at
        a clean anchor (strictly below all pending commits), and the durable
        window at or below an anchor is immutable, so the anchor orders
        snapshots even when a build delayed in window conversion finishes
        after a later snapshot installed (completion time cannot order
        snapshots). Equal-anchor installs with an identical window -- every
        flight waiter re-runs cache publication with the same prepared
        artifact, and a same-window speculative rebuild re-reads unchanged
        state under an equal anchor -- keep the installed generation instead
        of re-keying bundle lookups for nothing.

        Returns True when the window is armed (installed fresh, refreshed in
        place, or confirmed already current); only that outcome resets the
        re-arm backoff. A discarded install returns False and leaves the
        backoff alone -- in particular a BORN-EXPIRED artifact, whose anchor
        aged past the audit ceiling during its own window walk. Arming (or
        crediting) such an install would collapse the re-arm interval and
        rewalk the reward window continuously: the livelock behind the
        2026-07-29 rollback.
        """
        runtime = self._runtime
        try:
            return runtime._install_payout_ledger_artifact_outcome(artifact)
        finally:
            # A seeded artifact carries the exposed scan anchor of the
            # synchronous read that produced it. The install decision above
            # is atomic under _job_cache_lock: either the window armed (its
            # own anchor is now visible to append invalidation) or it was
            # discarded and can never serve, so the exposure has done its
            # job either way.
            runtime._retire_inflight_scan_anchor(
                artifact.inflight_scan_anchor_token
            )

    def _install_payout_ledger_artifact_outcome(
        self,
        artifact: PayoutLedgerArtifact,
    ) -> bool:
        runtime = self._runtime
        if not runtime._payout_artifact_reuse_active():
            # Kill-switch: never arm. An armed artifact would churn install
            # events while disabled and re-key the idle-bundle fast path to
            # a generation the reuse probe refuses anyway.
            return False
        anchor_age_ms = (
            None
            if artifact.snapshot_anchor_ms is None
            else self._now_ms() - int(artifact.snapshot_anchor_ms)
        )
        if (
            anchor_age_ms is None
            or anchor_age_ms > runtime._payout_artifact_max_anchor_age_ms()
        ):
            runtime._record_payout_artifact_event("born_expired")
            runtime._payout_artifact_log(
                "payout_artifact_born_expired",
                payout_state_generation=int(artifact.payout_state_generation),
                anchor_age_ms=anchor_age_ms,
                window_shares=len(artifact.shares_json),
            )
            return False
        with runtime._job_cache_lock:
            outcome, armed_generation = (
                runtime._install_payout_ledger_artifact_locked(artifact)
            )
        runtime._record_payout_artifact_event(outcome)
        if outcome == "discarded":
            runtime._payout_artifact_log(
                "payout_artifact_discarded",
                payout_state_generation=int(artifact.payout_state_generation),
                anchor_age_ms=anchor_age_ms,
                window_shares=len(artifact.shares_json),
            )
            return False
        if outcome in ("installed", "refreshed"):
            runtime._payout_artifact_log(
                "payout_artifact_" + outcome,
                generation=armed_generation,
                payout_state_generation=int(artifact.payout_state_generation),
                anchor_age_ms=anchor_age_ms,
                window_shares=len(artifact.shares_json),
            )
        # An armed artifact -- installed fresh or confirmed already current
        # -- proves preparation can succeed again; let the next fence-failure
        # re-arm run at the configured floor.
        with runtime._payout_artifact_executor_lock:
            runtime._payout_artifact_rearm_backoff = 1
        return True

    def _install_payout_ledger_artifact_locked(
        self,
        artifact: PayoutLedgerArtifact,
    ) -> tuple[str, int | None]:
        """Ordering core of the install; returns (outcome, armed generation)."""
        runtime = self._runtime
        if (
            artifact.payout_state_generation != runtime._payout_state_generation
            or artifact.append_invalidation_epoch
            != runtime._payout_ledger_append_invalidation_epoch
        ):
            return "discarded", None
        current = runtime._payout_ledger_artifact
        if (
            current is not None
            and current.payout_state_generation
            == artifact.payout_state_generation
        ):
            current_anchor_ms = (
                -1
                if current.snapshot_anchor_ms is None
                else int(current.snapshot_anchor_ms)
            )
            artifact_anchor_ms = (
                -1
                if artifact.snapshot_anchor_ms is None
                else int(artifact.snapshot_anchor_ms)
            )
            if current_anchor_ms > artifact_anchor_ms:
                return "discarded", None
            if (
                current.network_difficulty == artifact.network_difficulty
                and current.share_snapshot_sha256 is not None
                and current.share_snapshot_sha256
                == artifact.share_snapshot_sha256
            ):
                # The armed artifact already carries exactly this
                # window; keep its generation (no lookup re-key) but
                # still treat the preparation as a success, and advance
                # the freshness clock in place either way -- otherwise a
                # quiet share stream, or a pending-commit floor pinning
                # the anchor so every re-prove lands at the SAME anchor,
                # would cycle identical rebuilds forever without ever
                # un-staling the armed artifact. No fresher window is
                # constructible while the window bytes are unchanged, so
                # the re-walk that just re-proved them earns the credit.
                # The armed balances are kept: they are what the reuse
                # fence hashes against the published payout state
                # (including an accepted parent's preview patch).
                anchor_advanced = artifact_anchor_ms > current_anchor_ms
                runtime._payout_ledger_artifact = dataclass_replace(
                    current,
                    accepted_share_count=artifact.accepted_share_count,
                    prepared_monotonic=time.monotonic(),
                    snapshot_anchor_ms=(
                        artifact.snapshot_anchor_ms
                        if anchor_advanced
                        else current.snapshot_anchor_ms
                    ),
                )
                if anchor_advanced:
                    return "refreshed", int(current.generation)
                return "already_current", int(current.generation)
            template_artifacts = getattr(
                runtime,
                "_template_artifacts",
                None,
            )
            if (
                current.network_difficulty != artifact.network_difficulty
                and template_artifacts is not None
                and current.network_difficulty
                == int(template_artifacts.network_difficulty)
                and artifact.network_difficulty
                != int(template_artifacts.network_difficulty)
            ):
                # Whether a delayed pre-retarget build's anchor ties or
                # leads, keep the artifact the live template difficulty
                # can actually reuse: a wrong-difficulty install would
                # fail every reuse probe on the difficulty check, which
                # never re-arms, until the next synchronous build
                # re-seeded the live window.
                return "discarded", None
        runtime._payout_ledger_artifact_generation += 1
        # Freshness runs from the moment the window becomes reusable, not
        # from build completion: a sync-seeded artifact is constructed
        # mid-bundle-build and reaches cache publication only after the
        # audit builder, so stamping here keeps the budget honest at every
        # install site.
        runtime._payout_ledger_artifact = dataclass_replace(
            artifact,
            generation=runtime._payout_ledger_artifact_generation,
            prepared_monotonic=time.monotonic(),
        )
        return "installed", int(runtime._payout_ledger_artifact_generation)

    def _payout_artifact_preparation_loop(self) -> None:
        runtime = self._runtime
        while True:
            with runtime._payout_artifact_executor_lock:
                request = runtime._payout_artifact_requested
                bypass_build_interval = (
                    runtime._payout_artifact_requested_bypass
                )
                runtime._payout_artifact_requested = None
                runtime._payout_artifact_requested_bypass = False
                if request is None:
                    runtime._payout_artifact_future = None
                    return
            phases = runtime._job_build_phases()
            phases.clear()
            try:
                runtime._prepare_payout_ledger_artifact(
                    *request,
                    bypass_build_interval=bypass_build_interval,
                )
            finally:
                runtime._flush_job_build_phases(phases)

    def _schedule_payout_ledger_artifact_preparation(
        self,
        payout_state_generation: int,
        network_difficulty: int,
        *,
        min_interval_seconds: float | None = None,
        bypass_build_interval: bool = False,
    ) -> None:
        """Latest-generation-wins scheduling with one worker and one slot.

        With min_interval_seconds, the debounce check, timestamp update, and
        request enqueue are one atomic step under the executor lock: a
        concurrent dequeue or a racing probe cannot slip a duplicate request
        into the emptied slot and run the reward-window snapshot twice.
        """
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        if not runtime._payout_artifact_reuse_active():
            # Kill-switch: no background reward-window walks either. Off
            # disables the reuse line -- probe refusals, no walks, no
            # arming -- restoring pre-reuse delivery economics; the
            # synchronous build path keeps the re-landed anchor-selection
            # semantics rather than the pre-#102 count fences.
            return
        with runtime._payout_artifact_executor_lock:
            if runtime._payout_artifact_executor_shutdown:
                return
            if min_interval_seconds is not None:
                # Consecutive aborted rebuilds stretch the re-arm interval;
                # event-driven preparations below reset the backoff because
                # they mark a real state change worth retrying promptly for.
                effective_interval = (
                    min_interval_seconds * runtime._payout_artifact_rearm_backoff
                )
                if (
                    runtime._payout_artifact_last_schedule_monotonic is not None
                    and time.monotonic()
                    - runtime._payout_artifact_last_schedule_monotonic
                    < effective_interval
                ):
                    return
                rearm_backoff = runtime._payout_artifact_rearm_backoff
            else:
                runtime._payout_artifact_rearm_backoff = 1
                rearm_backoff = None
            runtime._payout_artifact_requested = (
                int(payout_state_generation),
                int(network_difficulty),
            )
            runtime._payout_artifact_requested_bypass = (
                runtime._payout_artifact_requested_bypass
                or bool(bypass_build_interval)
            )
            runtime._payout_artifact_last_schedule_monotonic = time.monotonic()
            if runtime._payout_artifact_future is None:
                executor = runtime._payout_artifact_executor
                if executor is None:
                    executor = ThreadPoolExecutor(
                        max_workers=1,
                        thread_name_prefix="prism-payout-artifact",
                    )
                    runtime._payout_artifact_executor = executor
                runtime._payout_artifact_future = executor.submit(
                    runtime._payout_artifact_preparation_loop
                )
        if rearm_backoff is not None:
            # Only debounced fence-failure re-arms log here; event-driven
            # preparations ride payout publications, which are already
            # visible. Recorded outside the executor lock the counter
            # shares.
            runtime._record_payout_artifact_event("rearm_scheduled")
            runtime._payout_artifact_log(
                "payout_artifact_rearm_scheduled",
                payout_state_generation=int(payout_state_generation),
                backoff=int(rearm_backoff),
            )

    def _payout_artifact_max_anchor_age_ms(self) -> float:
        """Audit ceiling on a served window's wall-clock anchor age."""
        runtime = self._runtime
        return (
            float(
                getattr(
                    runtime,
                    "payout_artifact_max_anchor_age_seconds",
                    DEFAULT_PRISM_PAYOUT_ARTIFACT_MAX_ANCHOR_AGE_SECONDS,
                )
            )
            * 1000.0
        )

    def _payout_artifact_reanchor_seconds(self) -> float:
        """Anchor age that triggers a background re-anchor while reuse keeps serving."""
        runtime = self._runtime
        return float(
            getattr(
                runtime,
                "payout_artifact_reanchor_seconds",
                DEFAULT_PRISM_PAYOUT_ARTIFACT_REANCHOR_SECONDS,
            )
        )

    def _payout_artifact_min_build_interval_seconds(self) -> float:
        """Minimum normal cadence; found-block settlement bypasses it."""
        runtime = self._runtime

        return float(
            getattr(
                runtime,
                "payout_artifact_min_build_interval_seconds",
                runtime._payout_artifact_reanchor_seconds(),
            )
        )

    def _payout_artifact_reuse_active(self) -> bool:
        """Master kill-switch (PRISM_PAYOUT_ARTIFACT_REUSE) for the reuse line."""
        runtime = self._runtime
        return bool(getattr(runtime, "payout_artifact_reuse_enabled", True))

    @staticmethod
    def _payout_artifact_log(event: str, **fields: object) -> None:
        """Single-line JSON lifecycle log; the incident was invisible here."""
        print(
            "prism coordinator: "
            + json.dumps({"event": event, **fields}, sort_keys=True),
            flush=True,
        )

    def _record_payout_artifact_event(self, event: str) -> None:
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        with runtime._payout_artifact_executor_lock:
            counts = runtime.payout_artifact_event_counts
            counts[event] = int(counts.get(event, 0)) + 1

    def _usable_payout_ledger_artifact(
        self,
        payout_state_generation: int,
        network_difficulty: int,
        *,
        rearm_on_fence_failure: bool = True,
    ) -> PayoutLedgerArtifact | None:
        """Return the armed artifact when reuse is valid for NEW work.

        Validity is anchor-scoped and EVENT-DRIVEN: the artifact's window is
        exact at its own snapshot anchor (which reused bundles declare), so
        shares landing after the anchor never invalidate it -- they belong
        to the next window by construction -- and the artifact stays valid
        until a payout event actually changes the state it snapshots (the
        generation and balances fences below). Wall-clock time is NOT a
        validity input below the audit ceiling: the 2026-07-29 rollback was
        a 10s anchor-age budget rejecting every slow build on arrival, and
        the 2026-07-30 brownout was the same budget moved to install time --
        at production cadence (a shared build every 10-14s, a 4-8s window
        walk) a 10s budget expires between consecutive builds, so every
        build fell back to the synchronous reward-window walk while the
        re-arm worker walked the same window in parallel. Past the
        REANCHOR floor the probe schedules a debounced background re-anchor
        but KEEPS SERVING the armed window; only past the audit ceiling
        (the bound on how far a paid window may trail the live ledger --
        post-anchor shares settle in the next window, none are lost) does
        reuse fail closed. The ceiling governs NEW reuse and issuance
        decisions; an in-flight build keeps the window it already selected,
        so a declared anchor slightly past the ceiling can still publish.
        """
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        if not runtime._payout_artifact_reuse_active():
            return None
        with runtime._job_cache_lock:
            artifact = runtime._payout_ledger_artifact
            published_artifact = runtime._published_payout_state.artifact
            append_invalidation_epoch = (
                runtime._payout_ledger_append_invalidation_epoch
            )
        if (
            artifact is None
            or artifact.payout_state_generation != payout_state_generation
            or artifact.network_difficulty != int(network_difficulty)
            or artifact.append_invalidation_epoch
            != append_invalidation_epoch
        ):
            return None
        if artifact.snapshot_anchor_ms is None:
            # Without a recorded anchor the artifact cannot declare the
            # anchor reused bundles must stamp; fail closed.
            return None
        anchor_age_ms = self._now_ms() - int(artifact.snapshot_anchor_ms)
        if anchor_age_ms > runtime._payout_artifact_max_anchor_age_ms():
            # The declared anchor crossed the audit ceiling; the window may
            # no longer be served to new work. Queue a bounded speculative
            # rebuild that re-anchors it; in-flight builds keep the copy
            # they selected.
            if rearm_on_fence_failure:
                runtime._rearm_payout_ledger_artifact_after_fence_failure(
                    payout_state_generation,
                    network_difficulty,
                )
            runtime._record_payout_artifact_event("probe_rejected_ceiling")
            return None
        if anchor_age_ms > runtime._payout_artifact_reanchor_seconds() * 1000.0:
            # Aging but valid: schedule the debounced background re-anchor
            # and keep serving. The delivery path must never pay the
            # synchronous reward-window walk while a correct armed window
            # exists; the re-anchor swaps in a fresher window off the
            # critical path. Counted unconditionally so the canary can
            # distinguish "serving past the floor, re-anchor pending" from
            # "never crossing the floor" -- rearm_scheduled alone
            # under-counts through the debounce and suppression paths.
            runtime._record_payout_artifact_event("probe_past_floor")
            if rearm_on_fence_failure:
                runtime._rearm_payout_ledger_artifact_after_fence_failure(
                    payout_state_generation,
                    network_difficulty,
                )
        if published_artifact is None:
            try:
                published_artifact = runtime._current_payout_state_artifact()
            except Exception:
                return None
        balances_sha256 = artifact.prior_balances_sha256 or self._canonical_json_sha256(
            artifact.prior_balances
        )
        with runtime._job_cache_lock:
            if (
                runtime._payout_state_generation != payout_state_generation
                or runtime._published_payout_state.artifact is not published_artifact
                or runtime._payout_ledger_append_invalidation_epoch
                != artifact.append_invalidation_epoch
            ):
                return None
            latest = runtime._payout_ledger_artifact
            if latest is not artifact:
                # An equal-window freshness restamp (already_current /
                # refreshed) replaces the armed object while this probe was
                # hashing balances. The restamp keeps the generation, the
                # window bytes, and the balances object, and only moves the
                # freshness stamp (and possibly the anchor) forward, so the
                # validity established above still holds for the restamped
                # copy; failing closed here would turn the intentional
                # pinned-floor recovery path into a spurious synchronous
                # reward-window walk. A real re-key changes the generation
                # or the window sha and still fails closed.
                if (
                    latest is None
                    or latest.generation != artifact.generation
                    or latest.payout_state_generation
                    != artifact.payout_state_generation
                    or latest.network_difficulty != artifact.network_difficulty
                    or latest.share_snapshot_sha256 is None
                    or latest.share_snapshot_sha256
                    != artifact.share_snapshot_sha256
                    or latest.snapshot_anchor_ms is None
                    or latest.prior_balances is not artifact.prior_balances
                    or latest.append_invalidation_epoch
                    != artifact.append_invalidation_epoch
                ):
                    return None
                artifact = latest
            if balances_sha256 != published_artifact.prior_balances_sha256:
                # A candidate can carry a ledger snapshot prepared before its
                # payout state is published. Never keep retrying that stale
                # shortcut; the synchronous path will take a fresh snapshot.
                runtime._payout_ledger_artifact = None
                return None
            served = artifact
        # Recorded outside the cache lock the event counter's own lock
        # ordering forbids nesting under.
        runtime._record_payout_artifact_event("served_reuse")
        return served

    def _rearm_payout_ledger_artifact_after_fence_failure(
        self,
        payout_state_generation: int,
        network_difficulty: int,
    ) -> None:
        """Debounced rebuild scheduling for an artifact past the re-anchor floor or ceiling.

        The interval floor keeps delta/carry reads (and any full fallback)
        from running continuously when artifacts age out faster than rebuilds
        complete.
        Landed accepted-block previews suppress the re-arm entirely: a
        speculative rebuild in that window would read database balances the
        published prospective state has already superseded, and the
        durable-confirmation path resumes preparation itself once the gap
        closes.
        """
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        with runtime._job_cache_lock:
            if (
                runtime._payout_state_publication_blocked
                or payout_state_generation != runtime._payout_state_generation
            ):
                return
        with runtime._accepted_block_payout_preview_condition:
            if any(
                transition.landed
                for transition in runtime._accepted_block_payout_previews.values()
            ):
                return
        cached_window = getattr(
            runtime,
            "_incremental_payout_artifact_window",
            None,
        )
        if (
            cached_window is not None
            and time.monotonic() - cached_window.refreshed_monotonic
            < runtime._payout_artifact_min_build_interval_seconds()
        ):
            # A pinned/old declared anchor can cross the re-anchor floor while
            # the window itself was just re-proved. Do not enqueue a worker
            # every five seconds merely to retag the same pages and re-read
            # carry balances; the next probe after the real cadence expires
            # will schedule it.
            return
        runtime._schedule_payout_ledger_artifact_preparation(
            payout_state_generation,
            network_difficulty,
            min_interval_seconds=float(
                getattr(
                    runtime,
                    "payout_artifact_rearm_min_seconds",
                    DEFAULT_PRISM_PAYOUT_ARTIFACT_REARM_MIN_SECONDS,
                )
            ),
        )

    def _schedule_current_payout_ledger_artifact_if_missing(self) -> None:
        """Resume speculative preparation after a durable preview catches up."""
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        with runtime._job_cache_lock:
            payout_state_generation = runtime._payout_state_generation
            template_artifacts = runtime._template_artifacts
        if template_artifacts is None:
            return
        if runtime._usable_payout_ledger_artifact(
            payout_state_generation,
            template_artifacts.network_difficulty,
            # This call site enqueues unconditionally below; letting the
            # probe also re-arm would race the one-slot worker's dequeue and
            # run the reward-window snapshot twice for one resumption.
            rearm_on_fence_failure=False,
        ) is not None:
            return
        runtime._schedule_payout_ledger_artifact_preparation(
            payout_state_generation,
            template_artifacts.network_difficulty,
        )

    def shutdown_payout_artifact_executor(self) -> None:
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        with runtime._payout_artifact_executor_lock:
            executor = runtime._payout_artifact_executor
            runtime._payout_artifact_executor = None
            runtime._payout_artifact_executor_shutdown = True
            runtime._payout_artifact_requested = None
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)

    def _prepare_payout_state_artifact(
        self,
        *,
        generation: int,
        source_generation: int,
        cancellation: _JobBuildCancellation | None = None,
    ) -> PayoutStateArtifact:
        """Snapshot carry-forward balances once for a payout generation."""
        runtime = self._runtime

        started = time.monotonic()
        if cancellation is not None:
            cancellation.raise_if_cancelled("payout artifact read")
        with runtime._payout_state_prepare_lock:
            balances = runtime.ledger.current_prior_balances()
        if cancellation is not None:
            cancellation.raise_if_cancelled("payout artifact serialization")
        artifact = runtime._payout_state_artifact_from_balances(
            generation=generation,
            source_generation=source_generation,
            balances=balances,
        )
        phases = runtime._job_build_phases()
        phases["payout_artifact"] = phases.get("payout_artifact", 0.0) + (
            time.monotonic() - started
        )
        return artifact

    def _payout_state_artifact_from_balances(
        self,
        *,
        generation: int,
        source_generation: int,
        balances: list[dict[str, object]],
    ) -> PayoutStateArtifact:
        balances_json = self._canonical_json_text(balances)
        return PayoutStateArtifact(
            generation=generation,
            source_generation=source_generation,
            prior_balances_json=balances_json,
            prior_balances_sha256=hashlib.sha256(balances_json.encode()).hexdigest(),
            prepared_monotonic=time.monotonic(),
        )

    def _publish_self_check_repaired_balances(
        self,
        expected_payout_state_generation: int,
        *,
        stale_prior_balances_sha256: str,
        balances: Sequence[dict[str, object]],
    ) -> bool:
        """Replace the published balance snapshot the periodic oracle refuted.

        The published artifact is otherwise immutable for its generation, but
        a confirmed drift means its bytes no longer match the durable ledger
        (a payout mutation whose response was lost). Swapping in the repaired
        balances re-keys new work to the corrected digest, and every serving
        decision for NOT-yet-stamped work re-checks that digest. Jobs already
        stamped for miners have no such fence -- their admission compares
        only the payout generation and append epoch, which both survive this
        swap -- so the repair also schedules the same refresh wave a payout
        publication schedules; digest-aware reselection then replaces exactly
        the active jobs keyed to the refuted snapshot. Only the exact refuted
        snapshot is replaced: any concurrent publication wins the guarded
        swap (and drives its own wave).
        """
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        with runtime._job_cache_lock:
            source_generation = runtime._published_payout_state.source_generation
        repaired = runtime._payout_state_artifact_from_balances(
            generation=int(expected_payout_state_generation),
            source_generation=int(source_generation),
            balances=list(balances),
        )
        with runtime._job_cache_lock:
            published = runtime._published_payout_state
            current = published.artifact
            if (
                runtime._payout_state_publication_blocked
                or runtime._payout_state_generation
                != expected_payout_state_generation
                or published.generation != expected_payout_state_generation
                or published.source_generation != source_generation
                or current is None
                or current.generation != expected_payout_state_generation
                or current.prior_balances_sha256
                != stale_prior_balances_sha256
            ):
                return False
            runtime._published_payout_state = dataclass_replace(
                published,
                artifact=repaired,
            )
        runtime._record_payout_artifact_event("balance_repair_published")
        runtime._payout_artifact_log(
            "payout_artifact_balance_repair_published",
            payout_state_generation=int(expected_payout_state_generation),
            stale_prior_balances_sha256=stale_prior_balances_sha256,
            repaired_prior_balances_sha256=repaired.prior_balances_sha256,
        )
        # Active jobs keyed to the refuted digest remain mineable until a
        # wave replaces them -- a block solved from one would commit the
        # stale allocation. The pending mark survives a superseded wave and
        # the digest reselection is idempotent, so a wave that raced this
        # repair converges on the next pass.
        runtime._mark_tip_refresh_pending(int(expected_payout_state_generation))
        runtime._schedule_tip_refresh_retry()
        return True

    def _current_payout_state_artifact(
        self,
        cancellation: _JobBuildCancellation | None = None,
    ) -> PayoutStateArtifact:
        """Return the immutable artifact published for the current generation."""
        runtime = self._runtime

        runtime._ensure_job_cache_state()
        while True:
            with runtime._job_cache_lock:
                if runtime._payout_state_publication_blocked:
                    raise PayoutStatePublicationBlocked(
                        "payout state invalidation is pending publication"
                    )
                published = runtime._published_payout_state
                if (
                    published.generation == runtime._payout_state_generation
                    and published.artifact is not None
                    and published.artifact.generation == published.generation
                ):
                    return published.artifact
                generation = runtime._payout_state_generation
                source_generation = published.source_generation
            artifact = runtime._prepare_payout_state_artifact(
                generation=generation,
                source_generation=source_generation,
                cancellation=cancellation,
            )
            with runtime._job_cache_lock:
                published = runtime._published_payout_state
                if (
                    runtime._payout_state_publication_blocked
                    or runtime._payout_state_generation != generation
                    or published.source_generation != source_generation
                ):
                    if cancellation is not None:
                        cancellation.raise_if_cancelled(
                            "payout artifact publication race"
                        )
                    continue
                runtime._published_payout_state = dataclass_replace(
                    published,
                    artifact=artifact,
                )
                return artifact

    @contextmanager
    def _payout_balance_mutation(self) -> Iterator[None]:
        """Serialize durable balance changes without excluding delivery."""
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        with runtime._block_submitter_lock(
            runtime._payout_balance_mutation_lock,
            "payout-balance-mutation",
        ):
            with runtime._accepted_block_payout_preview_condition:
                landed_transition = any(
                    transition.landed
                    for transition in runtime._accepted_block_payout_previews.values()
                )
            if landed_transition:
                raise PayoutStatePublicationBlocked(
                    "accepted block payout confirmation is still pending"
                )
            yield

    def _begin_accepted_block_payout_preview(
        self,
        block_hash: str,
        *,
        block_height: int | None = None,
    ) -> None:
        """Prevent child work from snapshotting pre-accept balances."""
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        key = block_hash.lower()
        with runtime._accepted_block_payout_preview_condition:
            runtime._invalidated_accepted_block_payout_previews.pop(key, None)
            existing = runtime._accepted_block_payout_previews.get(key)
            if existing is None:
                runtime._accepted_block_payout_previews[key] = (
                    AcceptedBlockPayoutTransition(block_height=block_height)
                )
            elif (
                block_height is not None
                and existing.block_height is not None
                and existing.block_height != block_height
            ):
                raise RuntimeError("accepted block payout transition height changed")
            elif existing.block_height is None and block_height is not None:
                runtime._accepted_block_payout_previews[key] = dataclass_replace(
                    existing,
                    block_height=block_height,
                )

    def _mark_accepted_block_payout_landed(
        self,
        block_hash: str,
        *,
        block_height: int,
    ) -> None:
        """Bar reconciliation after submitblock makes a candidate active."""
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        key = block_hash.lower()
        with runtime._accepted_block_payout_preview_condition:
            existing = runtime._accepted_block_payout_previews.get(
                key,
                AcceptedBlockPayoutTransition(block_height=block_height),
            )
            if existing.block_height not in {None, block_height}:
                raise RuntimeError("accepted block payout transition height changed")
            runtime._accepted_block_payout_previews[key] = dataclass_replace(
                existing,
                block_height=block_height,
                landed=True,
                landed_monotonic=(
                    existing.landed_monotonic
                    if existing.landed_monotonic is not None
                    else time.monotonic()
                ),
            )
            runtime._accepted_block_payout_preview_condition.notify_all()

    def _unmark_accepted_block_payout_landed(self, block_hash: str) -> None:
        """Withdraw a landed bar armed for an attempt that never reached RPC.

        Only sound while the submit outcome is NOT uncertain: the caller
        must know submitblock was never invoked under this arming (a
        preflight refusal such as ``WriterLeaseRenewalDeferred``), so there
        is no possibly-active coinbase for the barrier to preserve and no
        reason to keep reconciliation and payout-state publication barred
        while the candidate waits for its retry. The pending preview entry
        survives — child work keeps waiting instead of snapshotting
        pre-accept balances, matching the startup-replay state a durable
        candidate restores — and the next attempt re-arms ``landed`` before
        its own RPC.
        """
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        key = block_hash.lower()
        with runtime._accepted_block_payout_preview_condition:
            existing = runtime._accepted_block_payout_previews.get(key)
            if existing is None or not existing.landed:
                return
            runtime._accepted_block_payout_previews[key] = dataclass_replace(
                existing,
                landed=False,
                landed_monotonic=None,
            )
            runtime._accepted_block_payout_preview_condition.notify_all()

    def _publish_accepted_block_payout_preview(
        self,
        block_hash: str,
        balances: list[dict[str, object]],
    ) -> list[dict[str, object]]:
        """Publish the balances child work must observe after confirmation.

        Canonicalization happens before the delivery boundary. The gate only
        installs an immutable pointer and advances the generation, so no job
        bound to the pre-accept balances can land after publication.
        """
        runtime = self._runtime
        normalized = runtime.normalized_prior_balances(balances)
        serialized = runtime._serialize_prior_balance_preview(normalized)
        key = block_hash.lower()
        with runtime._block_submitter_lock(
            runtime._payout_balance_mutation_lock,
            "payout-balance-mutation",
        ):
            with runtime._accepted_block_payout_preview_condition:
                existing = runtime._accepted_block_payout_previews.get(key)
                existing_preview = existing.preview if existing is not None else None
                if existing_preview is not None:
                    if existing_preview != serialized:
                        raise RuntimeError(
                            "accepted block payout preview changed during retry"
                        )
                    if (
                        existing is not None
                        and existing.published_generation is not None
                    ):
                        return runtime._materialize_prior_balance_preview(
                            existing_preview
                        )
                    # A bounded publication loss retains the compact preview
                    # locally while delivery remains fenced. Matching retries
                    # must still cross the atomic publication boundary so they
                    # install a generation and reopen admission.

            published = None
            try:
                captured = runtime._capture_payout_state_source()
                reserved = runtime._reserve_payout_state_source_if_current(
                    captured[1],
                    "accepted_block_preview",
                    tip_hash=key,
                    invalidated_monotonic=time.monotonic(),
                )
                if reserved is None:
                    candidate = runtime._prepared_payout_state_candidate(
                        runtime._capture_payout_state_source(),
                        bypass_build_interval=True,
                    )
                else:
                    candidate = runtime._prepared_payout_state_candidate(reserved)
                candidate = runtime._accepted_block_preview_candidate(
                    candidate,
                    block_hash=key,
                    preview=serialized,
                )
                # Close delivery admission before exposing the preview. The
                # generation publication below performs the only atomic pointer
                # swap and reopens admission; no preparation lock is involved.
                runtime._block_payout_state_publication(force=True)
                published = runtime._publish_payout_state_candidate(candidate)
                if published is None:
                    max_retries = max(
                        0,
                        int(
                            getattr(
                                runtime,
                                "payout_reconcile_supersession_retries",
                                DEFAULT_PRISM_PAYOUT_RECONCILE_SUPERSESSION_RETRIES,
                            )
                        ),
                    )
                    for _attempt in range(max_retries):
                        candidate = runtime._accepted_block_preview_candidate(
                            runtime._prepared_payout_state_candidate(
                                runtime._capture_payout_state_source(),
                                bypass_build_interval=True,
                            ),
                            block_hash=key,
                            preview=serialized,
                        )
                        published = runtime._publish_payout_state_candidate(candidate)
                        if published is not None:
                            break
            except Exception as exc:
                # Last-resort degraded retention (issue #188): candidate
                # preparation already degrades a ledger-timeout window build
                # to the cached armed window, so reaching here means an
                # in-memory publication step itself failed. Fall through to
                # the fenced local-retention branch below exactly like a
                # lost atomic publication: admission is force-blocked, the
                # immutable compact preview remains visible to children
                # already waiting on the transition, and the tip-refresh
                # retry loop must publish a fresh source before new job
                # admission resumes.
                published = None
                print(
                    "prism coordinator: accepted block payout preview "
                    f"publication degraded hash={key}: {exc}",
                    flush=True,
                )
            if published is None:
                runtime._block_payout_state_publication(force=True)
                # Admission is now globally fenced, so retaining the compact
                # preview locally is safe even though it did not win an atomic
                # generation publication. Finalization may finish; the normal
                # retry loop will publish the newest source before delivery
                # resumes.
                with runtime._accepted_block_payout_preview_condition:
                    transition = runtime._accepted_block_payout_previews.get(
                        key,
                        AcceptedBlockPayoutTransition(landed=True),
                    )
                    runtime._accepted_block_payout_previews[key] = dataclass_replace(
                        transition,
                        landed=True,
                        preview=serialized,
                        published_generation=None,
                    )
                    runtime._accepted_block_payout_preview_condition.notify_all()
        return runtime._materialize_prior_balance_preview(serialized)

    def _accepted_block_preview_candidate(
        self,
        candidate: PayoutStateCandidate,
        *,
        block_hash: str,
        preview: tuple[tuple[str, str, str, int], ...],
    ) -> PayoutStateCandidate:
        """Bind a compact preview to its prepared artifact before gating."""
        runtime = self._runtime
        ledger_artifact = candidate.ledger_artifact
        if ledger_artifact is not None:
            # The artifact was prepared before accepted-block persistence, so
            # replace only its balance view with the verified prospective
            # state. This allocation must stay outside delivery publication.
            # The memoized balances digest must be re-derived with the patch:
            # a stale digest would fail the reuse probe's balances fence
            # against the very state this patch installs.
            patched_balances = tuple(
                runtime._materialize_prior_balance_preview(preview)
            )
            ledger_artifact = dataclass_replace(
                ledger_artifact,
                prior_balances=patched_balances,
                prior_balances_sha256=self._canonical_json_sha256(patched_balances),
            )
        return dataclass_replace(
            candidate,
            accepted_block_hash=block_hash,
            accepted_block_preview=preview,
            ledger_artifact=ledger_artifact,
        )

    @staticmethod
    def _serialize_prior_balance_preview(
        balances: list[dict[str, object]],
    ) -> tuple[tuple[str, str, str, int], ...]:
        return tuple(
            (
                str(balance["recipient_id"]),
                str(balance["order_key"]),
                str(balance["p2mr_program_hex"]),
                int(balance["balance_sats"]),
            )
            for balance in balances
        )

    def _accepted_block_payout_preview_from_bundle(
        self,
        final_bundle: dict[str, Any],
        *,
        prior_balances: list[dict[str, object]] | None = None,
    ) -> list[dict[str, object]]:
        """Derive the confirmed carry-forward view from a verified bundle."""
        runtime = self._runtime
        manifest = final_bundle.get("payout_policy_manifest")
        if not isinstance(manifest, dict) or not isinstance(manifest.get("accounts"), list):
            raise RuntimeError("accepted block payout manifest is missing accounts")
        prior_identities: dict[str, tuple[str, str]] = {}
        for balance in prior_balances or []:
            program = str(balance.get("p2mr_program_hex", "")).lower()
            identity = (
                str(balance.get("order_key", "")),
                str(balance.get("recipient_id", "")),
            )
            prior_identities[program] = min(
                identity,
                prior_identities.get(program, identity),
            )
        balances: list[dict[str, object]] = []
        for account in manifest["accounts"]:
            if not isinstance(account, dict):
                continue
            if str(account.get("account_type", "miner")) == "pool_fee":
                continue
            balance_sats = int(account.get("carry_forward_balance_sats", 0))
            if balance_sats == 0:
                continue
            program = str(account.get("p2mr_program_hex", "")).lower()
            account_identity = (
                str(account.get("order_key", "")),
                str(account.get("recipient_id", "")),
            )
            order_key, recipient_id = min(
                account_identity,
                prior_identities.get(program, account_identity),
            )
            balances.append(
                {
                    "recipient_id": recipient_id,
                    "order_key": order_key,
                    "p2mr_program_hex": program,
                    "balance_sats": balance_sats,
                }
            )
        return runtime.normalized_prior_balances(balances)

    @staticmethod
    def _materialize_prior_balance_preview(
        preview: tuple[tuple[str, str, str, int], ...],
    ) -> list[dict[str, object]]:
        return [
            {
                "recipient_id": recipient_id,
                "order_key": order_key,
                "p2mr_program_hex": p2mr_program_hex,
                "balance_sats": balance_sats,
            }
            for recipient_id, order_key, p2mr_program_hex, balance_sats in preview
        ]

    def _clear_accepted_block_payout_preview(
        self,
        block_hash: str,
        *,
        invalidate_published: bool = False,
    ) -> None:
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        key = block_hash.lower()
        with runtime._block_submitter_lock(
            runtime._payout_balance_mutation_lock,
            "payout-balance-mutation",
        ):
            with runtime._accepted_block_payout_preview_condition:
                existing = runtime._accepted_block_payout_previews.get(key)
                if existing is None:
                    if not invalidate_published:
                        runtime._invalidated_accepted_block_payout_previews.pop(
                            key,
                            None,
                        )
                    runtime._accepted_block_payout_preview_condition.notify_all()
                    return
                if not invalidate_published:
                    # Durable state now equals the published prospective view;
                    # removing the override changes no logical payout state.
                    runtime._accepted_block_payout_previews.pop(key, None)
                    runtime._invalidated_accepted_block_payout_previews.pop(key, None)
                    runtime._accepted_block_payout_preview_condition.notify_all()
                    return
                if existing.preview is None:
                    # Nothing crossed a generation boundary. A landed
                    # transition still becomes a tombstone so descendants
                    # cannot fall through to an uncertain database snapshot.
                    runtime._accepted_block_payout_previews.pop(key, None)
                    if existing.landed:
                        runtime._invalidated_accepted_block_payout_previews[key] = (
                            existing.block_height
                        )
                    runtime._accepted_block_payout_preview_condition.notify_all()
                    return

            captured = runtime._capture_payout_state_source()
            reserved = runtime._reserve_payout_state_source_if_current(
                captured[1],
                "accepted_block_preview_withdrawn",
                tip_hash=captured[2],
                invalidated_monotonic=time.monotonic(),
            )
            candidate = runtime._prepared_payout_state_candidate(
                reserved if reserved is not None else runtime._capture_payout_state_source(),
                force_full_window_rescan=True,
                bypass_build_interval=True,
            )
            candidate = dataclass_replace(
                candidate,
                accepted_block_hash=key,
                accepted_block_withdrawal=True,
                accepted_block_height=existing.block_height,
            )
            runtime._block_payout_state_publication(force=True)
            published = runtime._publish_payout_state_candidate(candidate)
            if published is None:
                max_retries = max(
                    0,
                    int(
                        getattr(
                            runtime,
                            "payout_reconcile_supersession_retries",
                            DEFAULT_PRISM_PAYOUT_RECONCILE_SUPERSESSION_RETRIES,
                        )
                    ),
                )
                for _attempt in range(max_retries):
                    candidate = dataclass_replace(
                        runtime._prepared_payout_state_candidate(
                            runtime._capture_payout_state_source(),
                            force_full_window_rescan=True,
                            bypass_build_interval=True,
                        ),
                        accepted_block_hash=key,
                        accepted_block_withdrawal=True,
                        accepted_block_height=existing.block_height,
                    )
                    published = runtime._publish_payout_state_candidate(candidate)
                    if published is not None:
                        break
            if published is None:
                # Delivery remains fenced. Install the tombstone immediately
                # so even local builders cannot fall through while the newest
                # source waits for a retry publication.
                with runtime._accepted_block_payout_preview_condition:
                    runtime._accepted_block_payout_previews.pop(key, None)
                    runtime._invalidated_accepted_block_payout_previews[key] = (
                        existing.block_height
                    )
                    runtime._accepted_block_payout_preview_condition.notify_all()

    def _accepted_block_payout_transition_landed(self, block_hash: str) -> bool:
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        with runtime._accepted_block_payout_preview_condition:
            transition = runtime._accepted_block_payout_previews.get(block_hash.lower())
            return transition is not None and transition.landed

    def _accepted_block_payout_transition_for_parent(
        self,
        parent_hash: str,
        *,
        parent_height: int | None = None,
    ) -> tuple[str, bool] | None:
        """Select the highest active exact/ancestor payout transition.

        The boolean result is true for an invalidated landed transition. The
        caller decides whether to wait for a live preview or fail closed on its
        tombstone.
        """
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        key = parent_hash.lower()
        with runtime._accepted_block_payout_preview_condition:
            exact_transition = runtime._accepted_block_payout_previews.get(key)
            exact_invalidated = (
                key in runtime._invalidated_accepted_block_payout_previews
            )
            fail_closed_candidate_hashes = {
                candidate_hash
                for candidate_hash, transition in (
                    runtime._accepted_block_payout_previews.items()
                )
                if transition.landed
            }
            fail_closed_candidate_hashes.update(
                runtime._invalidated_accepted_block_payout_previews
            )
            ancestor_candidates = [
                (candidate_hash, transition.block_height, False)
                for candidate_hash, transition in runtime._accepted_block_payout_previews.items()
                if exact_transition is None
                and not exact_invalidated
                and transition.block_height is not None
                and parent_height is not None
                and transition.block_height <= parent_height
            ]
            ancestor_candidates.extend(
                (
                    candidate_hash,
                    candidate_height,
                    True,
                )
                for candidate_hash, candidate_height in (
                    runtime._invalidated_accepted_block_payout_previews.items()
                )
                if exact_transition is None
                and not exact_invalidated
                and candidate_height is not None
                and parent_height is not None
                and candidate_height <= parent_height
            )
        if exact_transition is not None or exact_invalidated:
            return key, exact_invalidated
        if not ancestor_candidates:
            return None

        active_ancestors: list[tuple[int, str, bool]] = []
        try:
            for (
                candidate_hash,
                candidate_height,
                candidate_invalidated,
            ) in ancestor_candidates:
                assert candidate_height is not None
                active_hash = str(
                    runtime.rpc.call("getblockhash", [candidate_height])
                ).lower()
                if active_hash == candidate_hash:
                    active_ancestors.append(
                        (
                            candidate_height,
                            candidate_hash,
                            candidate_invalidated,
                        )
                    )
        except Exception as exc:
            runtime._schedule_tip_refresh_retry()
            raise TemplateRefreshBlocked(
                "could not validate an accepted payout preview on the active chain"
            ) from exc
        if not active_ancestors:
            if any(
                candidate_hash in fail_closed_candidate_hashes
                for candidate_hash, _candidate_height, _invalidated in (
                    ancestor_candidates
                )
            ):
                # A prepared artifact published with an accepted preview carries
                # that prospective balance view. If its block left the active
                # chain between reconciliation and this build, falling through
                # to the artifact would stamp unrelated work with those balances.
                # Wait for withdrawal/reconciliation to publish a new payout
                # generation instead of using either the artifact or live DB.
                runtime._schedule_tip_refresh_retry()
                raise PayoutStatePublicationBlocked(
                    "accepted payout transition is no longer active"
                )
            return None
        _, selected_key, selected_invalidated = max(active_ancestors)
        return selected_key, selected_invalidated

    def _accepted_parent_unresolved_depth_cap(self) -> int:
        runtime = self._runtime
        return max(
            1,
            int(
                getattr(
                    runtime,
                    "accepted_parent_unresolved_depth_max",
                    DEFAULT_ACCEPTED_PARENT_UNRESOLVED_DEPTH_MAX,
                )
            ),
        )

    def accepted_parent_unresolved_ages_seconds(self) -> list[float]:
        """Ages of landed transitions whose bookkeeping is still unresolved."""
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        now = time.monotonic()
        with runtime._accepted_block_payout_preview_condition:
            return [
                max(0.0, now - transition.landed_monotonic)
                for transition in runtime._accepted_block_payout_previews.values()
                if transition.landed and transition.landed_monotonic is not None
            ]

    def _accepted_parent_unresolved_depth(self) -> int:
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        with runtime._accepted_block_payout_preview_condition:
            return sum(
                1
                for transition in runtime._accepted_block_payout_previews.values()
                if transition.landed
            )

    def _await_pending_parent_payout_preview(
        self,
        parent_hash: str,
        *,
        parent_height: int | None = None,
    ) -> list[dict[str, object]] | None:
        """Wait out a pending accepted-parent transition, returning its preview.

        Returns None when no landed transition governs this parent, without
        touching any confirmed payout input. Ordering this wait before every
        confirmed read lets children of a pending parent block here instead of
        taking (or caching) balances that omit their new parent.
        """
        runtime = self._runtime
        # Startup-replay window (issue #188 fix 4): before the durable outbox
        # has been enumerated, an unarmed pending parent may exist. A child
        # built now could pay balances that omit that parent's carry, so job
        # builds fail closed until enumeration succeeds.
        if runtime._block_replay_enumeration_owed():
            runtime._schedule_tip_refresh_retry()
            raise TemplateRefreshBlocked(
                "durable block candidate replay has not enumerated pending "
                "candidates yet"
            )
        selected = runtime._accepted_block_payout_transition_for_parent(
            parent_hash,
            parent_height=parent_height,
        )
        if selected is None:
            return None
        # Fail closed once too many landed transitions remain unresolved:
        # every published preview a child consumes stacks another prospective
        # balance chain on unfinished bookkeeping, so the depth of that stack
        # must stay bounded even while degraded previews keep jobs flowing.
        # Issuance stops at the cap, not past it: a child issued while
        # exactly depth_cap transitions are unresolved could land and create
        # a (cap + 1)th, so the configured maximum would not actually bound
        # the chain.
        unresolved_depth = runtime._accepted_parent_unresolved_depth()
        depth_cap = runtime._accepted_parent_unresolved_depth_cap()
        if unresolved_depth >= depth_cap:
            runtime._schedule_tip_refresh_retry()
            raise TemplateRefreshBlocked(
                "unresolved accepted-parent depth "
                f"{unresolved_depth} meets or exceeds cap {depth_cap}"
            )
        selected_key, selected_invalidated = selected
        if selected_invalidated:
            runtime._schedule_tip_refresh_retry()
            raise TemplateRefreshBlocked(
                "accepted parent payout preview was withdrawn"
            )

        wait_seconds = max(
            0.0,
            float(
                getattr(
                    runtime,
                    "accepted_block_payout_preview_wait_seconds",
                    DEFAULT_ACCEPTED_BLOCK_PAYOUT_PREVIEW_WAIT_SECONDS,
                )
            ),
        )
        deadline = time.monotonic() + wait_seconds
        timed_out = False
        invalidated = False
        with runtime._accepted_block_payout_preview_condition:
            while selected_key in runtime._accepted_block_payout_previews:
                transition = runtime._accepted_block_payout_previews[selected_key]
                if transition.preview is not None:
                    return runtime._materialize_prior_balance_preview(
                        transition.preview
                    )
                if runtime.stop_event.is_set():
                    raise RuntimeError(
                        "coordinator stopped while accepted payout preview was pending"
                    )
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    timed_out = True
                    break
                runtime._accepted_block_payout_preview_condition.wait(
                    timeout=min(0.25, remaining)
                )
            invalidated = (
                selected_key in runtime._invalidated_accepted_block_payout_previews
            )
        if invalidated:
            runtime._schedule_tip_refresh_retry()
            raise TemplateRefreshBlocked(
                "accepted parent payout preview was withdrawn"
            )
        if timed_out:
            with runtime.lock:
                runtime._accepted_parent_preview_wait_timeouts = (
                    int(getattr(runtime, "_accepted_parent_preview_wait_timeouts", 0))
                    + 1
                )
            runtime._schedule_tip_refresh_retry()
            raise TemplateRefreshBlocked(
                "accepted parent payout preview is not ready yet"
            )
        # The transition reached a terminal durable state while waiting; the
        # caller's confirmed fallback now includes the parent.
        return None

    def _prior_balances_for_job_parent(
        self,
        parent_hash: str,
        *,
        parent_height: int | None = None,
        fallback_balances: Sequence[dict[str, object]] | None = None,
    ) -> list[dict[str, object]]:
        """Return prospective balances, otherwise a prepared/ledger fallback."""
        runtime = self._runtime
        preview = runtime._await_pending_parent_payout_preview(
            parent_hash,
            parent_height=parent_height,
        )
        if preview is not None:
            return preview
        return (
            list(fallback_balances)
            if fallback_balances is not None
            else runtime.ledger.current_prior_balances()
        )

    def _observe_payout_state_seconds(
        self,
        name: str,
        elapsed_seconds: float,
        *,
        relation: str | None = None,
    ) -> None:
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        with runtime._payout_state_metrics_lock:
            if name == "gate_wait":
                if relation not in PRISM_PAYOUT_DELIVERY_GENERATIONS:
                    raise ValueError(f"unknown payout delivery generation: {relation}")
                histogram = runtime.payout_gate_wait_histograms[str(relation)]
            else:
                histogram = runtime.payout_state_histograms[name]
            histogram["count"] = int(histogram["count"]) + 1
            histogram["sum"] = float(histogram["sum"]) + elapsed_seconds
            buckets = histogram["buckets"]
            assert isinstance(buckets, dict)
            for bucket in PRISM_PAYOUT_SECONDS_BUCKETS:
                if elapsed_seconds <= bucket:
                    buckets[bucket] = int(buckets.get(bucket, 0)) + 1

    def _observe_payout_gate_admission(
        self,
        admission: object,
        *,
        generation: int,
        fallback_wait_seconds: float,
    ) -> None:
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        with runtime._job_cache_lock:
            published_generation = runtime._payout_state_generation
        relation = getattr(admission, "relation", None)
        if relation not in PRISM_PAYOUT_DELIVERY_GENERATIONS:
            relation = PayoutStateDeliveryGate._generation_relation(
                generation,
                published_generation,
            )
        wait_seconds = float(
            getattr(admission, "wait_seconds", fallback_wait_seconds)
        )
        runtime._observe_payout_state_seconds(
            "gate_wait",
            max(0.0, wait_seconds),
            relation=relation,
        )

    def _reserve_payout_state_source(
        self,
        cause: str,
        *,
        tip_hash: str | None = None,
        invalidated_monotonic: float | None = None,
    ) -> int:
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        invalidated = (
            time.monotonic()
            if invalidated_monotonic is None
            else invalidated_monotonic
        )
        with runtime.lock:
            generation = runtime._payout_state_source[0] + 1
            runtime._payout_state_source = (
                generation,
                tip_hash,
                cause,
                invalidated,
            )
            return generation

    def _reserve_payout_state_source_if_current(
        self,
        expected_source_generation: int,
        cause: str,
        *,
        tip_hash: str | None = None,
        invalidated_monotonic: float | None = None,
    ) -> tuple[int, int, str | None, str, float] | None:
        """Reserve and capture a source only if preparation was not superseded."""
        runtime = self._runtime

        runtime._ensure_job_cache_state()
        invalidated = (
            time.monotonic()
            if invalidated_monotonic is None
            else invalidated_monotonic
        )
        # Match publication's lock order so the returned base generation and
        # newly reserved source form one atomic candidate identity.
        with runtime._job_cache_lock:
            with runtime.lock:
                if runtime._payout_state_source[0] != expected_source_generation:
                    return None
                source_generation = expected_source_generation + 1
                runtime._payout_state_source = (
                    source_generation,
                    tip_hash,
                    cause,
                    invalidated,
                )
                return (
                    runtime._payout_state_generation,
                    source_generation,
                    tip_hash,
                    cause,
                    invalidated,
                )

    def _capture_payout_state_source(
        self,
    ) -> tuple[int, int, str | None, str, float]:
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        with runtime.lock:
            source_generation, source_tip, cause, invalidated = (
                runtime._payout_state_source
            )
        with runtime._job_cache_lock:
            base_generation = runtime._payout_state_generation
        return (
            base_generation,
            source_generation,
            source_tip,
            cause,
            invalidated,
        )

    def _prepared_payout_state_candidate(
        self,
        captured: tuple[int, int, str | None, str, float],
        *,
        force_full_window_rescan: bool = False,
        bypass_build_interval: bool = False,
    ) -> PayoutStateCandidate:
        runtime = self._runtime
        base_generation, source_generation, source_tip, cause, invalidated = captured
        ledger_artifact: PayoutLedgerArtifact | None = None
        with runtime._job_cache_lock:
            template_artifacts = runtime._template_artifacts
        if (
            template_artifacts is not None
            and getattr(runtime, "_pool_ready_latched", False)
        ):
            force_full = (
                force_full_window_rescan
                or cause == "accepted_block_preview_withdrawn"
            )
            bypass_interval = (
                bypass_build_interval
                or cause
                in {
                    "accepted_block_preview",
                    "accepted_block_preview_withdrawn",
                }
            )
            immediate_preview = (
                cause == "accepted_block_preview"
                or (
                    bypass_build_interval
                    and not force_full_window_rescan
                    and cause != "accepted_block_preview_withdrawn"
                )
            )
            prepare_lock_acquired = True
            if immediate_preview:
                # A routine periodic oracle may already own this lock and its
                # Postgres query cannot be cancelled safely. Found-block
                # publication must not queue behind that 11--36 second scan:
                # use the last exact, still-valid armed window and patch its
                # balances with the verified preview below.
                prepare_lock_acquired = runtime._payout_state_prepare_lock.acquire(
                    blocking=False
                )
            if prepare_lock_acquired:
                try:
                    ledger_artifact = runtime._build_payout_ledger_artifact(
                        base_generation,
                        base_generation + 1,
                        template_artifacts.network_difficulty,
                        force_full,
                        bypass_interval,
                        True,
                    )
                except self._shutdown_error:
                    raise
                except Exception as exc:
                    if not immediate_preview:
                        raise
                    # Found-block publication exists to decouple child job
                    # issuance from slow landing bookkeeping (issue #188). A
                    # window build that dies with the ledger must not abort
                    # the atomic publication that reopens admission: degrade
                    # to the cached armed window below, exactly like a busy
                    # prepare lock. The preview itself never depends on this
                    # build; it is the verified balance state either way.
                    print(
                        "prism coordinator: found-block payout window build "
                        f"failed; degrading to cached armed window: {exc}",
                        flush=True,
                    )
                    ledger_artifact = None
                finally:
                    if immediate_preview:
                        runtime._payout_state_prepare_lock.release()
            else:
                ledger_artifact = None
            if immediate_preview and ledger_artifact is None:
                ledger_artifact = runtime._cached_found_block_payout_artifact(
                    base_generation=base_generation,
                    artifact_payout_state_generation=base_generation + 1,
                    network_difficulty=template_artifacts.network_difficulty,
                    fallback_reason=(
                        "build_failed"
                        if prepare_lock_acquired
                        else "prepare_lock_busy"
                    ),
                )
        return PayoutStateCandidate(
            base_generation=base_generation,
            source_generation=source_generation,
            source_tip_hash=source_tip,
            cause=cause,
            invalidated_monotonic=invalidated,
            prepared_monotonic=time.monotonic(),
            ledger_artifact=ledger_artifact,
        )

    def _cached_found_block_payout_artifact(
        self,
        *,
        base_generation: int,
        artifact_payout_state_generation: int,
        network_difficulty: int,
        fallback_reason: str,
    ) -> PayoutLedgerArtifact | None:
        """Retag an exact armed window when immediate preparation is unavailable.

        The caller always replaces its balance view with the accepted block's
        verified prospective balances before publication. Only the share
        window and its declared anchor are reused here.
        """
        runtime = self._runtime

        with runtime._job_cache_lock:
            artifact = runtime._payout_ledger_artifact
            generation_current = base_generation == runtime._payout_state_generation
            append_epoch_current = bool(
                artifact is not None
                and artifact.append_invalidation_epoch
                == runtime._payout_ledger_append_invalidation_epoch
            )
        anchor_age_ms = (
            None
            if artifact is None or artifact.snapshot_anchor_ms is None
            else self._now_ms() - int(artifact.snapshot_anchor_ms)
        )
        if (
            not generation_current
            or not append_epoch_current
            or artifact is None
            or artifact.payout_state_generation != base_generation
            or artifact.network_difficulty != int(network_difficulty)
            or anchor_age_ms is None
            or anchor_age_ms > runtime._payout_artifact_max_anchor_age_ms()
        ):
            runtime._record_payout_artifact_event("build_aborted")
            runtime._payout_artifact_log(
                "payout_artifact_build_aborted",
                payout_state_generation=int(artifact_payout_state_generation),
                duration_seconds=0.0,
                during_publication=True,
                build_interval_bypassed=True,
                abort_reason=f"{fallback_reason}_without_usable_window",
            )
            return None
        reused = dataclass_replace(
            artifact,
            generation=0,
            payout_state_generation=int(artifact_payout_state_generation),
            prepared_monotonic=time.monotonic(),
            window_build_mode="found_block_cached",
            window_delta_rows=0,
            window_expired_rows=0,
            window_touched_pages=0,
            window_build_seconds=0.0,
            window_full_rescan_reason=fallback_reason,
        )
        runtime._record_payout_artifact_event("found_block_cached")
        runtime._payout_artifact_log(
            "payout_artifact_found_block_cached",
            payout_state_generation=int(artifact_payout_state_generation),
            duration_seconds=0.0,
            window_build_mode="found_block_cached",
            window_shares=len(reused.shares_json),
            anchor_age_ms=anchor_age_ms,
            during_publication=True,
            build_interval_bypassed=True,
            fallback_reason=fallback_reason,
        )
        return reused

    def _current_payout_state_candidate(self) -> PayoutStateCandidate:
        runtime = self._runtime
        return runtime._prepared_payout_state_candidate(
            runtime._capture_payout_state_source()
        )

    def _record_discarded_payout_candidate(self) -> None:
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        with runtime._payout_state_metrics_lock:
            runtime.payout_state_candidates_discarded += 1

    def _block_payout_state_publication(
        self,
        *,
        force: bool = False,
        supersede_with: tuple[int, str | None, str, float] | None = None,
    ) -> None:
        """Atomically close delivery, optionally reserving a newer source."""
        runtime = self._runtime

        runtime._ensure_job_cache_state()

        pending_source: int | None = None

        def mark_blocked() -> bool:
            nonlocal pending_source
            with runtime._job_cache_lock:
                with runtime.lock:
                    if supersede_with is not None:
                        (
                            expected_source,
                            fallback_tip,
                            cause,
                            invalidated,
                        ) = supersede_with
                        current_source, current_tip, _, _ = (
                            runtime._payout_state_source
                        )
                        # A newer tip/source wins its identity, but it must be
                        # superseded so no candidate prepared before an
                        # uncertain durable commit can publish afterward.
                        source_tip = (
                            fallback_tip
                            if current_source == expected_source
                            else current_tip
                        )
                        pending_source = current_source + 1
                        runtime._payout_state_source = (
                            pending_source,
                            source_tip,
                            cause,
                            invalidated,
                        )
                    else:
                        pending_source = runtime._payout_state_source[0]
                    if (
                        not force
                        and supersede_with is None
                        and pending_source
                        == runtime._published_payout_state.source_generation
                    ):
                        return False
                    runtime._payout_state_publication_blocked = True
                    runtime._job_bundle_cache.clear()
                    return True

        # Close fleet admission atomically with the cache fence. Escaped
        # immutable bundles remain stamped with the old generation, but cannot
        # cross the boundary while the ledger has newer unpublished state.
        if not runtime._payout_state_delivery_gate.block_delivery(mark_blocked):
            return
        # Construction cancellation is independent of fanout activation. An
        # obsolete builder may still be in its ledger or subprocess phase and
        # must be preempted before a replacement request is submitted.
        runtime._cancel_obsolete_job_builds("payout generation superseded")
        with runtime._job_cache_lock:
            next_payout_generation = runtime._payout_state_generation + 1
        with runtime.lock:
            payout_invalidated_monotonic = runtime._payout_state_source[3]
        runtime._record_progress_payout_generation(
            next_payout_generation,
            payout_invalidated_monotonic,
        )
        with runtime.lock:
            active = getattr(runtime, "_active_tip_refresh", None)
        if (
            active is not None
            and active[0].payout_state_generation < next_payout_generation
        ):
            active[1].cancel()
        elif active is not None:
            return
        assert pending_source is not None
        runtime._mark_tip_refresh_pending(next_payout_generation)
        runtime._schedule_tip_refresh_retry()

    def _payout_state_publication_fenced(self) -> bool:
        """Report whether delivery is still blocked awaiting a publication."""
        runtime = self._runtime

        runtime._ensure_job_cache_state()
        with runtime._job_cache_lock:
            return runtime._payout_state_publication_blocked

    def _payout_source_requires_publication(
        self,
        candidate: PayoutStateCandidate | None = None,
    ) -> bool:
        """Report whether an invalidation source still lacks a publication."""
        runtime = self._runtime

        runtime._ensure_job_cache_state()
        with runtime._job_cache_lock:
            published_source = runtime._published_payout_state.source_generation
            if candidate is not None:
                return candidate.source_generation != published_source
            with runtime.lock:
                return runtime._payout_state_source[0] != published_source

    def _captured_payout_source_requires_publication(
        self,
        captured: tuple[int, int, str | None, str, float],
    ) -> bool:
        """Publication check for a captured source, without a candidate.

        Answers exactly what _payout_source_requires_publication would answer
        for a candidate prepared from ``captured``. Candidate preparation can
        include a multi-second ledger snapshot, so decision sites must be able
        to ask this first and prepare only when publication will follow.
        """
        runtime = self._runtime

        runtime._ensure_job_cache_state()
        with runtime._job_cache_lock:
            return captured[1] != runtime._published_payout_state.source_generation

    def _publish_payout_state_candidate(
        self,
        candidate: PayoutStateCandidate,
    ) -> int | None:
        """Publish a prepared candidate, or reject it if its source moved."""
        runtime = self._runtime

        runtime._ensure_job_cache_state()
        runtime._ensure_tip_refresh_state()
        published_generation: int | None = None
        schedule_retry = False
        active_to_cancel: _FanoutCancellation | None = None
        publication_born_expired: tuple[int | None, int] | None = None
        publication_installed: tuple[int, int, int] | None = None
        publish_started = 0.0
        with runtime._job_cache_lock:
            with runtime.lock:
                if (
                    candidate.source_generation != runtime._payout_state_source[0]
                    or candidate.base_generation != runtime._payout_state_generation
                ):
                    runtime._record_discarded_payout_candidate()
                    return None
        try:
            if (
                candidate.accepted_block_preview is not None
                and not candidate.accepted_block_withdrawal
            ):
                # The preview IS the balance state this generation publishes.
                # The durable ledger only catches up after persistence, so a
                # ledger read here would cache pre-parent balances against the
                # post-parent generation and outlive the transition override.
                artifact = runtime._payout_state_artifact_from_balances(
                    generation=candidate.base_generation + 1,
                    source_generation=candidate.source_generation,
                    balances=runtime._materialize_prior_balance_preview(
                        candidate.accepted_block_preview
                    ),
                )
            elif candidate.ledger_artifact is not None:
                # Candidate preparation captured balances under the same
                # ledger fence as its share window. Reuse that immutable copy
                # instead of issuing the carry-forward aggregate twice during
                # every publication.
                artifact = runtime._payout_state_artifact_from_balances(
                    generation=candidate.base_generation + 1,
                    source_generation=candidate.source_generation,
                    balances=list(candidate.ledger_artifact.prior_balances),
                )
            else:
                artifact = runtime._prepare_payout_state_artifact(
                    generation=candidate.base_generation + 1,
                    source_generation=candidate.source_generation,
                )
        except Exception:
            # The delivery/cache fence remains closed. A later reconciliation
            # retry can prepare and publish the artifact; no generation lacking
            # immutable payout inputs is ever exposed to builders.
            runtime._schedule_tip_refresh_retry()
            raise
        with runtime._job_cache_lock:
            with runtime.lock:
                if (
                    candidate.source_generation != runtime._payout_state_source[0]
                    or candidate.base_generation != runtime._payout_state_generation
                ):
                    runtime._record_discarded_payout_candidate()
                    return None
        with runtime._payout_state_delivery_gate.publication():
            # publication() has already drained admitted old sends. Start the
            # critical-section timer only now; drain latency is delivery wait,
            # not time spent holding the atomic payout mutation section.
            publish_started = time.monotonic()
            with runtime._job_cache_lock:
                with runtime.lock:
                    source_generation = runtime._payout_state_source[0]
                    if (
                        candidate.source_generation == source_generation
                        and candidate.base_generation
                        == runtime._payout_state_generation
                    ):
                        published_generation = runtime._payout_state_generation + 1
                        if candidate.accepted_block_hash is not None:
                            key = candidate.accepted_block_hash
                            with runtime._accepted_block_payout_preview_condition:
                                transition = runtime._accepted_block_payout_previews.get(
                                    key,
                                    AcceptedBlockPayoutTransition(
                                        block_height=candidate.accepted_block_height,
                                        landed=True,
                                    ),
                                )
                                if candidate.accepted_block_withdrawal:
                                    runtime._accepted_block_payout_previews.pop(key, None)
                                    runtime._invalidated_accepted_block_payout_previews[
                                        key
                                    ] = (
                                        transition.block_height
                                        if transition.block_height is not None
                                        else candidate.accepted_block_height
                                    )
                                else:
                                    existing_preview = transition.preview
                                    if (
                                        existing_preview is not None
                                        and existing_preview
                                        != candidate.accepted_block_preview
                                    ):
                                        raise RuntimeError(
                                            "accepted block payout preview changed "
                                            "during atomic publication"
                                        )
                                    runtime._invalidated_accepted_block_payout_previews.pop(
                                        key,
                                        None,
                                    )
                                    runtime._accepted_block_payout_previews[key] = (
                                        dataclass_replace(
                                            transition,
                                            landed=True,
                                            preview=candidate.accepted_block_preview,
                                            published_generation=published_generation,
                                        )
                                    )
                                runtime._accepted_block_payout_preview_condition.notify_all()
                        runtime._payout_state_generation = published_generation
                        prepared_artifact = candidate.ledger_artifact
                        candidate_anchor_age_ms = (
                            None
                            if prepared_artifact is None
                            or prepared_artifact.snapshot_anchor_ms is None
                            else self._now_ms()
                            - int(prepared_artifact.snapshot_anchor_ms)
                        )
                        if (
                            prepared_artifact is not None
                            and runtime._payout_artifact_reuse_active()
                            and prepared_artifact.payout_state_generation
                            == published_generation
                            and prepared_artifact.append_invalidation_epoch
                            == runtime._payout_ledger_append_invalidation_epoch
                            and candidate_anchor_age_ms is not None
                            and candidate_anchor_age_ms
                            <= runtime._payout_artifact_max_anchor_age_ms()
                        ):
                            runtime._payout_ledger_artifact_generation += 1
                            # Restamp freshness at the install: the candidate
                            # artifact was built before publication, and the
                            # delivery-gate drain between the two can outlive
                            # the reuse budget -- a freshly published payout
                            # generation must never arm an already-stale
                            # artifact and force the next builds back through
                            # the synchronous reward-window walk.
                            runtime._payout_ledger_artifact = dataclass_replace(
                                prepared_artifact,
                                generation=runtime._payout_ledger_artifact_generation,
                                prepared_monotonic=time.monotonic(),
                            )
                            publication_installed = (
                                runtime._payout_ledger_artifact_generation,
                                candidate_anchor_age_ms,
                                len(prepared_artifact.shares_json),
                            )
                        else:
                            if (
                                prepared_artifact is not None
                                and runtime._payout_artifact_reuse_active()
                                and prepared_artifact.payout_state_generation
                                == published_generation
                                and prepared_artifact.append_invalidation_epoch
                                == runtime._payout_ledger_append_invalidation_epoch
                            ):
                                # Same admission rule as
                                # _install_payout_ledger_artifact: candidate
                                # construction plus the delivery-gate drain
                                # can push the declared anchor past the
                                # audit ceiling, and arming such a
                                # BORN-EXPIRED artifact would fail every
                                # reuse probe on anchor age -- with the
                                # re-arm suppressed while an accepted
                                # preview awaits durability. Discard it; the
                                # post-publication probe schedules
                                # preparation (or the durable-confirmation
                                # path resumes it once the preview lands).
                                publication_born_expired = (
                                    candidate_anchor_age_ms,
                                    len(prepared_artifact.shares_json),
                                )
                            runtime._payout_ledger_artifact = None
                        runtime._published_payout_state = PublishedPayoutState(
                            generation=published_generation,
                            source_generation=candidate.source_generation,
                            source_tip_hash=candidate.source_tip_hash,
                            published_monotonic=publish_started,
                            artifact=artifact,
                        )
                        epoch_tip_hash = runtime._payout_epoch_tip_hash_locked()
                        if epoch_tip_hash is not None:
                            runtime._mint_tip_refresh_epoch_locked(
                                tip_hash=epoch_tip_hash,
                                payout_state_generation=published_generation,
                                started_monotonic=candidate.invalidated_monotonic,
                            )
                            # Re-anchor the stamping identity to the minted
                            # epoch in the same lock hold. A stale identity
                            # stamps rebuilt bundles at epoch 0, which the
                            # fence blocks for every previously admitted
                            # connection until the next wave republishes.
                            # The publish helper no-ops when the epoch tip
                            # is not the published snapshot tip; the owning
                            # wave handles that transition.
                            published_snapshot = getattr(
                                runtime,
                                "tip_template_snapshot",
                                None,
                            )
                            if published_snapshot is not None:
                                runtime._publish_tip_refresh_epoch_identity_locked(
                                    published_snapshot
                                )
                        runtime._payout_state_publication_blocked = False
                        runtime._job_bundle_cache.clear()
                        runtime._retained_collection_refresh = None
                        active = getattr(runtime, "_active_tip_refresh", None)
                        if active is None:
                            schedule_retry = True
                        elif active[0].payout_state_generation < published_generation:
                            # The payout gate itself rejects this old generation.
                            # Signal its fanout only after atomic publication.
                            active_to_cancel = active[1]
                            schedule_retry = True
                        with runtime._payout_state_metrics_lock:
                            runtime._payout_first_delivery_pending = (
                                published_generation,
                                candidate.invalidated_monotonic,
                            )
            if published_generation is not None:
                # The mutation owner still blocks every delivery admission,
                # so the pointer swap and gate generation remain one atomic
                # publication boundary. Do not acquire the gate condition
                # while holding coordinator locks: cancellation callbacks take
                # those locks after entering the gate wait loop.
                runtime._payout_state_delivery_gate.publish_generation(
                    published_generation,
                    prioritize_delivery=True,
                )
        runtime._observe_payout_state_seconds(
            "publish",
            max(0.0, time.monotonic() - publish_started),
        )
        if published_generation is None:
            runtime._record_discarded_payout_candidate()
            return None
        if publication_born_expired is not None:
            born_expired_age_ms, born_expired_shares = publication_born_expired
            runtime._record_payout_artifact_event("born_expired")
            runtime._payout_artifact_log(
                "payout_artifact_born_expired",
                payout_state_generation=int(published_generation),
                anchor_age_ms=born_expired_age_ms,
                window_shares=born_expired_shares,
                during_publication=True,
            )
        if publication_installed is not None:
            # The pointer swap above is an install site that bypasses
            # _install_payout_ledger_artifact; record it in the same event
            # family or the publication path stays invisible to the
            # lifecycle observability this counter exists for.
            installed_generation, installed_age_ms, installed_shares = (
                publication_installed
            )
            runtime._record_payout_artifact_event("installed")
            runtime._payout_artifact_log(
                "payout_artifact_installed",
                generation=int(installed_generation),
                payout_state_generation=int(published_generation),
                anchor_age_ms=installed_age_ms,
                window_shares=installed_shares,
                during_publication=True,
            )
        # A publication is a fresh start for speculative preparation: the
        # candidate artifact installs through the atomic pointer swap above
        # (not _install_payout_ledger_artifact), so whatever re-arm backoff
        # pre-publication share traffic accumulated must be released here or
        # a share landing right after publication would stay suppressed for
        # the whole scaled interval.
        with runtime._payout_artifact_executor_lock:
            runtime._payout_artifact_rearm_backoff = 1
        runtime._cancel_obsolete_job_bundle_builds(
            payout_state_generation=published_generation
        )
        runtime._record_progress_payout_generation(
            published_generation,
            candidate.invalidated_monotonic,
        )
        if active_to_cancel is not None:
            active_to_cancel.cancel()
        runtime._cancel_obsolete_job_builds("payout generation published")
        if schedule_retry:
            runtime._mark_tip_refresh_pending(published_generation)
            runtime._schedule_tip_refresh_retry()
        with runtime._job_cache_lock:
            current_artifacts = runtime._template_artifacts
        published_artifact_usable = (
            runtime._usable_payout_ledger_artifact(
                published_generation,
                current_artifacts.network_difficulty,
            )
            if current_artifacts is not None
            else None
        )
        accepted_preview_pending_durability = (
            candidate.accepted_block_hash is not None
            and not candidate.accepted_block_withdrawal
        )
        if (
            current_artifacts is not None
            and published_artifact_usable is None
            and not accepted_preview_pending_durability
        ):
            # A background artifact built before accepted-block confirmation
            # would carry the old database balances under the new payout
            # generation. Child builders use the compact preview until the
            # durable state catches up; artifact preparation resumes then.
            runtime._schedule_payout_ledger_artifact_preparation(
                published_generation,
                current_artifacts.network_difficulty,
            )
        return published_generation

    def _record_first_payout_delivery(
        self,
        generation: int,
        delivered_monotonic: float,
    ) -> None:
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        elapsed: float | None = None
        with runtime._payout_state_metrics_lock:
            pending = runtime._payout_first_delivery_pending
            if pending is not None and pending[0] == generation:
                elapsed = max(0.0, delivered_monotonic - pending[1])
                runtime._payout_first_delivery_pending = None
        if elapsed is not None:
            runtime._observe_payout_state_seconds("first_delivery", elapsed)

    def _advance_payout_state_generation(self) -> int:
        """Publish a payout-only invalidation with no expensive gate work."""
        runtime = self._runtime
        # No production caller remains: direct-block finalization publishes
        # through submit_block_candidate's reserve/publish path instead. Tests
        # keep using this as the smallest complete reserve -> block -> publish
        # invalidation cycle.
        runtime._ensure_job_cache_state()
        runtime._reserve_payout_state_source("payout_only")
        prepared_started = time.monotonic()
        with runtime._payout_state_prepare_lock:
            # Close build/delivery admission before releasing snapshot readers.
            # Publication may then drain already-admitted sends without holding
            # the preparation lock needed by later ledger work.
            runtime._block_payout_state_publication(force=True)
            runtime._observe_payout_state_seconds(
                "preparation",
                max(0.0, time.monotonic() - prepared_started),
            )
        generation = runtime._publish_current_payout_state_with_retry_budget()
        if generation is None:
            raise TemplateRefreshSuperseded(
                "payout-only invalidation was superseded; immediate retry scheduled"
            )
        return generation

    def _publish_current_payout_state_with_retry_budget(
        self,
        *,
        initial_attempted: bool = False,
    ) -> int | None:
        """Publish the current source with a bounded supersession budget."""
        runtime = self._runtime

        max_retries = max(
            0,
            int(
                getattr(
                    runtime,
                    "payout_reconcile_supersession_retries",
                    DEFAULT_PRISM_PAYOUT_RECONCILE_SUPERSESSION_RETRIES,
                )
            ),
        )
        attempts = max_retries + (0 if initial_attempted else 1)
        for _attempt in range(attempts):
            candidate = runtime._current_payout_state_candidate()
            published = runtime._publish_payout_state_candidate(candidate)
            if published is not None:
                return published
        runtime._block_payout_state_publication()
        return None

    def _payout_delivery(
        self,
        cancelled: Callable[[], bool],
        *,
        generation: int,
    ) -> Any:
        """Use cancellable admission while retaining focused gate test seams."""
        runtime = self._runtime
        gate = runtime._payout_state_delivery_gate
        delivery_cancelable = getattr(gate, "delivery_cancelable", None)
        if callable(delivery_cancelable):
            return delivery_cancelable(
                cancelled,
                generation=generation,
                priority=True,
            )
        delivery = gate.delivery
        try:
            return delivery(cancelled)
        except TypeError:
            return delivery()

    def _expose_inflight_scan_anchor(self, anchor_ms: int) -> int:
        """Publish a ledger walk's anchor to the append-side invalidation.

        The entry must stay exposed for as long as the walk's result is
        invisible to ``_invalidate_incremental_payout_window_for_append``:
        from before the database read (whose snapshot may already exclude a
        row committing mid-read) until either the result is discarded or its
        window becomes visible through the incremental cache or the armed
        artifact's own anchor. Entries older than the audit ceiling are
        pruned lazily here: ``_install_payout_ledger_artifact`` rejects any
        artifact whose anchor aged past that same ceiling as born-expired,
        so a leaked exposure (a built bundle every waiter abandoned) can
        never outlive the last install that could have used it.
        """
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        max_age_ms = runtime._payout_artifact_max_anchor_age_ms()
        stale_before_ms = self._now_ms() - max_age_ms
        with runtime._job_cache_lock:
            for token, exposed_anchor_ms in tuple(
                runtime._payout_window_inflight_scan_anchors.items()
            ):
                if exposed_anchor_ms < stale_before_ms:
                    runtime._payout_window_inflight_scan_anchors.pop(token, None)
            runtime._payout_window_inflight_scan_anchor_token += 1
            token = runtime._payout_window_inflight_scan_anchor_token
            runtime._payout_window_inflight_scan_anchors[token] = int(anchor_ms)
            return token

    def _retire_inflight_scan_anchor(self, token: int | None) -> None:
        """Withdraw an exposed walk anchor once its result became visible
        (or provably never will). Idempotent; ``None`` is a no-op."""
        runtime = self._runtime
        if token is None:
            return
        runtime._ensure_job_cache_state()
        with runtime._job_cache_lock:
            runtime._payout_window_inflight_scan_anchors.pop(int(token), None)

    def _publish_seedless_job_window_anchor_locked(self, anchor_ms: int) -> None:
        """Keep a seedless published window's anchor visible to appends.

        Caller holds _job_cache_lock, at the bundle publication fence of a
        build whose walk exposure retires with no seeded artifact to carry
        it (the reuse kill-switch, or nothing to seed). Jobs stamped from
        that bundle keep serving the window until they retire, so the
        declared anchor must stay in the predates() anchor set past the
        exposure: a replay-shaped append that starts and finishes entirely
        between publication and a landing's own anchor exposure would
        otherwise see no live anchor, skip its epoch bump, and leave no
        registry entry for the landing's drain to find. Monotonic and never
        retired; the pending-commit floor keeps every ordinary share commit
        above it.
        """
        runtime = self._runtime
        runtime._payout_published_job_window_anchor_ms = max(
            int(anchor_ms),
            runtime._payout_published_job_window_anchor_ms or 0,
        )

    @staticmethod
    def _pending_share_predates_anchor(
        pending_share: PendingShare,
        anchor_ms: int | None,
    ) -> bool:
        return bool(
            anchor_ms is not None
            and int(pending_share.job_issued_at_ms) <= anchor_ms
            and int(pending_share.accepted_at_ms) <= anchor_ms
        )

    def _pending_share_predates_live_anchor_locked(
        self,
        pending_share: PendingShare,
    ) -> bool:
        # Caller holds _job_cache_lock. The incremental pointer is
        # immutable and replaced atomically; its owning lock is
        # deliberately not acquired here, because an urgent append must
        # disarm the separately guarded artifact even while a long
        # oracle build owns _payout_state_prepare_lock. Reading it under
        # _job_cache_lock orders the read against the in-flight scan
        # anchors: a window walk makes its result visible (the
        # incremental pointer, or the armed artifact's install fence)
        # before retiring its exposed anchor under this lock, so a walk
        # that could have missed this row always exposes an anchor to
        # the predates checks below.
        runtime = self._runtime
        predates = runtime._pending_share_predates_anchor
        cached = getattr(runtime, "_incremental_payout_artifact_window", None)
        artifact = runtime._payout_ledger_artifact
        inflight_anchors_ms = tuple(
            runtime._payout_window_inflight_scan_anchors.values()
        )
        cached_anchor_ms = (
            None
            if cached is None
            else int(cached.window.anchor_job_issued_at_ms)
        )
        artifact_anchor_ms = (
            None
            if artifact is None or artifact.snapshot_anchor_ms is None
            else int(artifact.snapshot_anchor_ms)
        )
        published_job_anchor_ms = getattr(
            runtime, "_payout_published_job_window_anchor_ms", None
        )
        return bool(
            predates(pending_share, cached_anchor_ms)
            or predates(pending_share, artifact_anchor_ms)
            or predates(pending_share, published_job_anchor_ms)
            or any(
                predates(pending_share, anchor_ms)
                for anchor_ms in inflight_anchors_ms
            )
        )

    @contextmanager
    def _landing_fence_for_predating_append(
        self,
        pending_shares: list[PendingShare],
    ) -> Iterator[bool]:
        """Hold the landing fence across a durable append of predating rows.

        An epoch bump that only starts after its row is durable leaves a
        gap: a landing can acquire the fence, verify the pre-bump epoch,
        and enter submitblock after the row committed but before the bump
        starts, still landing a coinbase whose window omits the durable
        share. Any append whose rows predate a live anchor therefore takes
        the fence BEFORE the ledger commit and holds it through the epoch
        bump, so the durable append and its invalidation land on the same
        side of a landing's fence-guarded epoch-check-and-submit boundary.
        Ordinary share commits stay off the lock entirely: the unfenced
        peek here fires only for the rare replay-shaped append. Yields
        whether the fence is held so the caller can thread it into
        _record_late_visible_payout_append.

        The unfenced classification is a one-time predicate, so an anchor
        exposed after it (a landing publishing its declared anchor) cannot
        retroactively fence this commit. An unfenced batch therefore
        registers itself, atomically with the classification, in
        _payout_unfenced_append_inflight_stamps and deregisters only after
        the caller's post-commit epoch-bump attempt ran inside this block;
        _await_unfenced_appends_predating_anchor lets a landing wait those
        commits out before its epoch fences arm.
        """
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        unfenced_token: int | None = None
        with runtime._job_cache_lock:
            fence_needed = any(
                runtime._pending_share_predates_live_anchor_locked(pending)
                for pending in pending_shares
            )
            if not fence_needed and pending_shares:
                runtime._payout_unfenced_append_inflight_token += 1
                unfenced_token = runtime._payout_unfenced_append_inflight_token
                runtime._payout_unfenced_append_inflight_stamps[
                    unfenced_token
                ] = min(
                    max(
                        int(pending.job_issued_at_ms),
                        int(pending.accepted_at_ms),
                    )
                    for pending in pending_shares
                )
        if not fence_needed:
            try:
                yield False
            finally:
                if unfenced_token is not None:
                    with runtime._payout_unfenced_append_drained:
                        runtime._payout_unfenced_append_inflight_stamps.pop(
                            unfenced_token, None
                        )
                        runtime._payout_unfenced_append_drained.notify_all()
            return
        with runtime._payout_append_landing_fence_lock:
            yield True

    def _block_work_wait_slice(self) -> float:
        """Slice length for a condition wait taken on the block-work thread.

        The block-candidate owner derives this from the configured watchdog
        tolerance, so reusing it keeps every sliced wait on that thread in
        step with the watchdog through operator overrides instead of pinning
        a second literal that goes stale. Duck-typed runtimes in focused
        tests do not carry the helper; they fall back to the same
        admission-wait slice the shutdown controller keeps its own copy of.
        """
        wait_slice = getattr(self._runtime, "_block_work_wait_slice", None)
        if callable(wait_slice):
            return max(0.001, float(wait_slice()))
        return BLOCK_SUBMITTER_WAIT_HEARTBEAT_SLICE_SECONDS

    def _record_block_work_phase(self, phase: str) -> None:
        """Stamp a block-work liveness phase when the runtime records them.

        The recorder itself decides whether the calling thread is a
        registered block-work owner and records nothing otherwise, which is
        what keeps a share-writer or client thread from refreshing the
        budget of an owner that is genuinely wedged. Duck-typed runtimes
        without the recorder simply do not stamp.
        """
        recorder = getattr(self._runtime, "_record_block_submitter_phase", None)
        if callable(recorder):
            recorder(phase)

    def _await_unfenced_appends_predating_anchor(self, anchor_ms: int) -> None:
        """Wait until no unfenced in-flight append can predate ``anchor_ms``.

        Called by a landing right after exposing its declared anchor. An
        append classified as unfenced before that exposure commits outside
        the landing fence, so holding the fence across submitblock would
        not exclude its durable commit -- and its post-commit epoch bump
        would queue behind the very fence the landing holds, arriving only
        after the block entered qbitd. Draining here restores the fence
        contract: each matching append finishes its commit and its bump
        attempt against the now-exposed anchor before this returns, so a
        predating row advances the epoch the landing's fences check, and
        every append classified after the exposure sees the anchor and
        takes the fence itself. Appends whose rows cannot predate the
        anchor never block this wait, and the registry holds entries only
        for the duration of a ledger commit, so the common path is one
        locked emptiness check. Commit-scoped retention is sound because an
        append that completed before this drain cannot have predated
        ``anchor_ms`` silently: every landable declared anchor is exposed
        from publication onward (the armed artifact's anchor, or the
        published-window watermark a seedless build hands its anchor to at
        the publication fence), so such an append classified as fenced and
        advanced the epoch this landing's fences compare against.
        """
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        anchor = int(anchor_ms)
        # The drain is unbounded by design and stays that way: it enforces
        # the fencing contract above, so a deadline here could let a landing
        # proceed past appends whose epoch bumps its own fences must see.
        # What an untimed wait may not do is run heartbeat-silent -- this
        # call happens inline on the watchdog-monitored block-work thread,
        # where an unbroken silence longer than the tolerance is a hard exit
        # mid-landing (issue #125). Waiting in watchdog-sized slices and
        # stamping each one that still finds the predicate true keeps the
        # wait exactly as long as it was while making it visible; the
        # ``while`` re-check is what makes a timed wait semantically
        # transparent. The common path -- nothing in flight predates the
        # anchor -- never enters the loop and so stamps nothing at all.
        slice_seconds = self._block_work_wait_slice()
        with runtime._payout_unfenced_append_drained:
            while any(
                stamp_ms <= anchor
                for stamp_ms in (
                    runtime._payout_unfenced_append_inflight_stamps.values()
                )
            ):
                self._record_block_work_phase("wait-unfenced-append-drain")
                runtime._payout_unfenced_append_drained.wait(
                    timeout=slice_seconds
                )

    def _record_late_visible_payout_append(
        self,
        pending_share: PendingShare,
        *,
        landing_fence_owned: bool = False,
    ) -> int | None:
        """Advance the append-invalidation epoch for a newly durable row.

        Two-phase bump. The unfenced peek keeps ordinary share commits off
        the landing fence lock entirely; only a row that predates a live
        anchor re-checks and bumps under it. Holding that lock for the bump
        is what makes the landing's final epoch fence authoritative: a bump
        cannot commit while a landing sits between its last epoch read and
        submitblock, so the invalidation lands either before that read
        (candidate abandoned) or after the RPC returns (the refresh wave
        retires the job). The predicate re-runs under the lock because the
        anchor set may have moved during the unfenced gap. A caller that
        already holds the fence around the durable commit itself
        (landing_fence_owned) bumps without re-acquiring.

        Returns the advanced epoch, or None when no live anchor predates
        the row. The caller must follow a non-None return with
        _retire_payout_windows_for_late_append OUTSIDE the fence: that step
        waits on _payout_state_prepare_lock, which a long oracle build can
        hold, and the wait must not block landings.
        """
        runtime = self._runtime
        runtime._ensure_job_cache_state()

        def bump_if_predating_locked() -> int | None:
            with runtime._job_cache_lock:
                if not runtime._pending_share_predates_live_anchor_locked(
                    pending_share
                ):
                    return None
                runtime._payout_ledger_append_invalidation_epoch += 1
                runtime._payout_ledger_artifact = None
                return int(runtime._payout_ledger_append_invalidation_epoch)

        if landing_fence_owned:
            return bump_if_predating_locked()
        with runtime._job_cache_lock:
            if not runtime._pending_share_predates_live_anchor_locked(
                pending_share
            ):
                return None
        with runtime._payout_append_landing_fence_lock:
            return bump_if_predating_locked()

    def _invalidate_incremental_payout_window_for_append(
        self,
        pending_share: PendingShare,
    ) -> None:
        """Disarm payout work that predates a newly visible eligible row.

        Normal submissions hold the pending-share commit floor, so their
        accepted timestamp remains above the cached anchor until the row is
        durable. Durable replay reconstructs the original timestamps without
        that process-local floor. If both stamps are already covered by an
        incremental or armed-artifact anchor, the snapshot cannot discover
        the late-visible row. Publish the invalidation without waiting for an
        in-flight oracle walk, then clear the incremental cache under its own
        lock; the epoch prevents pre-append work from re-arming meanwhile.

        This wrapper serves post-commit callers that do not hold the landing
        fence; the durable append paths take the fence around the commit via
        _landing_fence_for_predating_append and call the bump and retire
        steps directly.
        """
        runtime = self._runtime
        invalidation_epoch = runtime._record_late_visible_payout_append(
            pending_share
        )
        if invalidation_epoch is None:
            return
        runtime._retire_payout_windows_for_late_append(
            pending_share, invalidation_epoch
        )

    def _retire_payout_windows_for_late_append(
        self,
        pending_share: PendingShare,
        invalidation_epoch: int,
    ) -> None:
        """Disarm cached payout windows after a late-append epoch bump.

        Runs outside the landing fence. Active jobs stamped against the
        pre-append window still pass the generation and digest fences; only
        epoch-aware reselection replaces them. Schedule the same refresh wave
        a repair publication schedules, and do it before the prepare-lock
        wait below so a long oracle build holding that lock cannot delay
        superseding the stale jobs.
        """
        runtime = self._runtime

        def predates(anchor_ms: int | None) -> bool:
            return runtime._pending_share_predates_anchor(
                pending_share, anchor_ms
            )

        runtime._mark_tip_refresh_pending(invalidation_epoch)
        runtime._schedule_tip_refresh_retry()

        with runtime._payout_state_prepare_lock:
            cached = runtime._incremental_payout_artifact_window
            if cached is None:
                runtime._incremental_payout_artifact_window_invalidation_reason = (
                    "late_visible_append"
                )
                return
            anchor_ms = int(cached.window.anchor_job_issued_at_ms)
            if cached.append_invalidation_epoch >= invalidation_epoch:
                # A post-invalidation build already replaced the cache.
                return
            if predates(anchor_ms):
                runtime._incremental_payout_artifact_window = None
                runtime._incremental_payout_artifact_window_invalidation_reason = (
                    "late_visible_append"
                )
            else:
                # The armed artifact was affected but this older incremental
                # anchor was not. Retag it so a later delta build can preserve
                # the exact append-only window instead of forcing an oracle.
                runtime._incremental_payout_artifact_window = dataclass_replace(
                    cached,
                    append_invalidation_epoch=invalidation_epoch,
                )

    @contextmanager
    def _payout_balance_serializer_released(self) -> Iterator[None]:
        """Temporarily release the balance serializer around candidate audit
        construction.

        The caller is the landing, which holds the serializer exactly once
        (its single outer acquisition; no caller of
        _land_and_confirm_block_candidate holds it). The landed-transition
        fence armed before the release keeps reconciliation from mutating
        balances while the serializer is free, so job delivery proceeds
        during the expensive builder/verifier subprocess work instead of
        queueing for its whole duration.
        """
        runtime = self._runtime
        runtime._payout_balance_mutation_lock.release()
        try:
            yield
        finally:
            runtime._acquire_block_submitter_lock(
                runtime._payout_balance_mutation_lock,
                "payout-balance-mutation",
            )

    def _replayed_payout_window_reproducible(
        self,
        context: PrismJobContext,
    ) -> bool:
        """Whether a reconstructed candidate's payout window replays intact.

        Append-invalidation epochs are process-local, so the landing epoch
        fence stands down for a candidate rebuilt from durable intent. The
        durable ledger is the surviving authority instead: the reward-window
        contract keeps the share window replayable at the artifact's declared
        anchor, so a share row that became durably visible only after the
        recorded window walk -- on either side of a restart -- appears in the
        replayed window while the recorded coinbase omits it. Only omissions
        fail the candidate: an appended row leaves the tip, height, and carry
        balances untouched, which is exactly why no other landing fence can
        see it, while a recorded row absent from the replay is not this
        hazard (share rows are append-only, and drift from settled payout
        state is governed by the reorg and prior-balance fences).
        """
        runtime = self._runtime
        found_block = getattr(context, "found_block", None)
        anchor_ms = (
            found_block.get("anchor_job_issued_at_ms")
            if isinstance(found_block, dict)
            else None
        )
        network_difficulty = (
            found_block.get("network_difficulty")
            if isinstance(found_block, dict)
            else None
        )
        audit_share_window = getattr(runtime.ledger, "audit_share_window", None)
        if (
            anchor_ms is None
            or network_difficulty is None
            or not callable(audit_share_window)
        ):
            # Fail closed: a candidate whose window cannot be replayed at a
            # declared anchor cannot prove its coinbase pays the window the
            # durable ledger requires.
            return False
        durable_rows = audit_share_window(
            anchor_job_issued_at_ms=int(anchor_ms),
            network_difficulty=int(network_difficulty),
        )
        recorded_share_ids = {
            str(row.get("share_id"))
            for row in context.shares_json
            if isinstance(row, dict)
        }
        return all(
            str(row.get("share_id")) in recorded_share_ids
            for row in durable_rows
            if isinstance(row, dict)
        )

    def normalized_prior_balances(self, balances: list[dict[str, object]]) -> list[dict[str, object]]:
        rows = [
            {
                "recipient_id": str(balance.get("recipient_id", "")),
                "order_key": str(balance.get("order_key", "")),
                "p2mr_program_hex": str(balance.get("p2mr_program_hex", "")),
                "balance_sats": int(balance.get("balance_sats", 0)),
            }
            for balance in balances
        ]
        rows.sort(
            key=lambda row: (
                row["order_key"],
                row["recipient_id"],
                row["p2mr_program_hex"],
                row["balance_sats"],
            )
        )
        return rows

    @staticmethod
    def settlement_balances_by_program(
        balances: list[dict[str, object]],
    ) -> dict[str, int]:
        """Aggregate owed balances by P2MR program for settlement equality.

        A miner may re-authorize the same payout address under any valid
        spelling (bech32 permits an all-uppercase form), which changes the
        recipient/order-key identity strings while committing to the same
        payout program. Ledger and preview views can then label one program
        with different identity strings while agreeing on every settled
        amount, so settlement equality must be judged on the program and
        amount alone. Identity strings stay display metadata. Zero totals
        are dropped to match both the ledger reader (nonzero HAVING filter)
        and the preview builder (zero balances skipped).
        """
        aggregated: dict[str, int] = {}
        for balance in balances:
            program = str(balance.get("p2mr_program_hex", "")).lower()
            aggregated[program] = aggregated.get(program, 0) + int(
                balance.get("balance_sats", 0)
            )
        return {program: total for program, total in aggregated.items() if total != 0}

    def prior_balances_match_current(self, prior_balances: list[dict[str, object]]) -> bool:
        runtime = self._runtime
        return runtime.settlement_balances_by_program(
            prior_balances
        ) == runtime.settlement_balances_by_program(runtime.ledger.current_prior_balances())

    def payout_state_metrics_lines(self) -> list[str]:
        runtime = self._runtime
        runtime._ensure_job_cache_state()
        with runtime._payout_state_metrics_lock:
            state_histograms = {
                name: {
                    "buckets": dict(histogram["buckets"]),
                    "sum": float(histogram["sum"]),
                    "count": int(histogram["count"]),
                }
                for name, histogram in runtime.payout_state_histograms.items()
            }
            gate_histograms = {
                relation: {
                    "buckets": dict(histogram["buckets"]),
                    "sum": float(histogram["sum"]),
                    "count": int(histogram["count"]),
                }
                for relation, histogram in runtime.payout_gate_wait_histograms.items()
            }
            discarded = runtime.payout_state_candidates_discarded

        metric_names = {
            "preparation": "qbit_prism_payout_preparation_seconds",
            "publish": "qbit_prism_payout_publish_seconds",
            "first_delivery": "qbit_prism_payout_invalidation_first_delivery_seconds",
        }
        descriptions = {
            "preparation": "Payout reconciliation and candidate preparation outside delivery publication.",
            "publish": "Atomic payout generation/cache publication gate-hold time.",
            "first_delivery": "Payout invalidation to first delivery of the published generation.",
        }
        lines: list[str] = []
        for name, metric_name in metric_names.items():
            histogram = state_histograms[name]
            buckets = histogram["buckets"]
            assert isinstance(buckets, dict)
            lines.extend(
                [
                    f"# HELP {metric_name} {descriptions[name]}",
                    f"# TYPE {metric_name} histogram",
                    *[
                        f'{metric_name}_bucket{{le="{bucket:g}"}} {int(buckets.get(bucket, 0))}'
                        for bucket in PRISM_PAYOUT_SECONDS_BUCKETS
                    ],
                    f'{metric_name}_bucket{{le="+Inf"}} {histogram["count"]}',
                    f'{metric_name}_sum {float(histogram["sum"]):.6f}',
                    f'{metric_name}_count {histogram["count"]}',
                ]
            )

        gate_name = "qbit_prism_payout_gate_wait_seconds"
        lines.extend(
            [
                "# HELP qbit_prism_payout_gate_wait_seconds Delivery admission wait by generation relationship to the published payout state.",
                "# TYPE qbit_prism_payout_gate_wait_seconds histogram",
            ]
        )
        for relation in PRISM_PAYOUT_DELIVERY_GENERATIONS:
            histogram = gate_histograms[relation]
            buckets = histogram["buckets"]
            assert isinstance(buckets, dict)
            lines.extend(
                [
                    *[
                        f'{gate_name}_bucket{{generation="{relation}",le="{bucket:g}"}} {int(buckets.get(bucket, 0))}'
                        for bucket in PRISM_PAYOUT_SECONDS_BUCKETS
                    ],
                    f'{gate_name}_bucket{{generation="{relation}",le="+Inf"}} {histogram["count"]}',
                    f'{gate_name}_sum{{generation="{relation}"}} {float(histogram["sum"]):.6f}',
                    f'{gate_name}_count{{generation="{relation}"}} {histogram["count"]}',
                ]
            )
        lines.extend(
            [
                "# HELP qbit_prism_payout_candidates_discarded_total Prepared payout candidates discarded after source supersession.",
                "# TYPE qbit_prism_payout_candidates_discarded_total counter",
                f"qbit_prism_payout_candidates_discarded_total {discarded}",
            ]
        )
        return lines


__all__ = [
    "AcceptedBlockPayoutTransition",
    "DEFAULT_ACCEPTED_BLOCK_PAYOUT_PREVIEW_WAIT_SECONDS",
    "DEFAULT_PRISM_PAYOUT_RECONCILE_SUPERSESSION_RETRIES",
    "PRISM_PAYOUT_ARTIFACT_REARM_BACKOFF_CAP",
    "PRISM_PAYOUT_DELIVERY_GENERATIONS",
    "PRISM_PAYOUT_SECONDS_BUCKETS",
    "PayoutDeliveryAdmission",
    "PayoutLedgerArtifact",
    "PayoutStateArtifact",
    "PayoutStateCandidate",
    "PayoutStateDeliveryGate",
    "PayoutStatePublicationBlocked",
    "PayoutStateRuntime",
    "PayoutStateService",
    "PayoutStateSnapshot",
    "PublishedPayoutState",
    "TemplateRefreshBlocked",
    "TemplateRefreshSuperseded",
    "_IncrementalPayoutArtifactWindow",
    "_PayoutWindowMaterialization",
    "canonical_json_sha256",
    "canonical_json_text",
]
