"""Payout generation, artifact, preview, and delivery-gate ownership.

This module deliberately has no dependency on ``prism_coordinator``.  The
coordinator wires the service to ledger, job-build, tip-refresh, and progress
health domains through :class:`PayoutStatePorts`.
"""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field, replace as dataclass_replace
import hashlib
import json
import threading
import time
from typing import Callable, Iterator, Mapping, Protocol, Sequence


DEFAULT_ACCEPTED_BLOCK_PAYOUT_PREVIEW_WAIT_SECONDS = 5.0
DEFAULT_PRISM_PAYOUT_RECONCILE_SUPERSESSION_RETRIES = 8
PRISM_PAYOUT_DELIVERY_GENERATIONS = ("current", "stale", "future")
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
PRISM_REWARD_WINDOW_MULTIPLIER = 8
PRISM_SNAPSHOT_WINDOW_MARGIN = 2
PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS = 0.05


def canonical_json_text(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def canonical_json_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json_text(value).encode()).hexdigest()


class TemplateRefreshBlocked(RuntimeError):
    """A live template was fetched, but safe work could not be issued."""


class TemplateRefreshSuperseded(TemplateRefreshBlocked):
    """Concurrent tip or payout progress invalidated a refresh attempt."""


class PayoutStatePublicationBlocked(TemplateRefreshBlocked):
    """Job construction is waiting for a prepared payout publication."""


class CancellationPort(Protocol):
    def raise_if_cancelled(self, phase: str) -> None: ...


class _FrozenJsonDict(dict[str, object]):
    """A JSON object that retains dict serialization without mutation."""

    __slots__ = ()

    @staticmethod
    def _immutable(*_args: object, **_kwargs: object) -> None:
        raise TypeError("payout ledger JSON is immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    clear = _immutable
    pop = _immutable
    popitem = _immutable
    setdefault = _immutable
    update = _immutable
    __ior__ = _immutable


class _FrozenJsonList(list[object]):
    """A JSON array that retains list equality/serialization semantics."""

    __slots__ = ()

    @staticmethod
    def _immutable(*_args: object, **_kwargs: object) -> None:
        raise TypeError("payout ledger JSON is immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    __iadd__ = _immutable
    __imul__ = _immutable
    append = _immutable
    clear = _immutable
    extend = _immutable
    insert = _immutable
    pop = _immutable
    remove = _immutable
    reverse = _immutable
    sort = _immutable


def _freeze_json_value(value: object) -> object:
    if isinstance(value, (_FrozenJsonDict, _FrozenJsonList)):
        return value
    if isinstance(value, Mapping):
        frozen = _FrozenJsonDict()
        dict.update(
            frozen,
            ((str(key), _freeze_json_value(item)) for key, item in value.items()),
        )
        return frozen
    if isinstance(value, (list, tuple)):
        frozen = _FrozenJsonList()
        list.extend(frozen, (_freeze_json_value(item) for item in value))
        return frozen
    return value


def _freeze_json_rows(
    rows: Sequence[Mapping[str, object]],
) -> tuple[dict[str, object], ...]:
    frozen_rows: list[dict[str, object]] = []
    for row in rows:
        frozen = _freeze_json_value(row)
        if not isinstance(frozen, dict):
            raise TypeError("payout ledger JSON row must be an object")
        frozen_rows.append(frozen)
    return tuple(frozen_rows)


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
class PayoutLedgerArtifact:
    """Immutable ledger input prepared independently of a qbit template."""

    generation: int
    payout_state_generation: int
    network_difficulty: int
    accepted_share_count: int
    shares_json: tuple[dict[str, object], ...] = field(repr=False)
    prior_balances: tuple[dict[str, object], ...] = field(repr=False)
    prepared_monotonic: float
    snapshot_anchor_ms: int | None = None

    def __post_init__(self) -> None:
        # These artifacts are cached and shared across snapshots/candidates.
        # Frozen JSON containers retain equality and encoding semantics while
        # protecting the accepted-count/balance fence from caller mutation.
        object.__setattr__(
            self,
            "shares_json",
            _freeze_json_rows(self.shares_json),
        )
        object.__setattr__(
            self,
            "prior_balances",
            _freeze_json_rows(self.prior_balances),
        )


@dataclass(frozen=True)
class AcceptedBlockPayoutTransition:
    """Prospective balances for one durable candidate across its landing seam."""

    block_height: int | None = None
    landed: bool = False
    preview: tuple[tuple[str, str, str, int], ...] | None = None
    published_generation: int | None = None


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
class PayoutStateSnapshot:
    generation: int
    source: tuple[int, str | None, str, float]
    published: PublishedPayoutState
    ledger_artifact: PayoutLedgerArtifact | None
    publication_blocked: bool


@dataclass(frozen=True)
class PayoutStateConfig:
    accepted_block_preview_wait_seconds: float = (
        DEFAULT_ACCEPTED_BLOCK_PAYOUT_PREVIEW_WAIT_SECONDS
    )
    reconcile_supersession_retries: int = (
        DEFAULT_PRISM_PAYOUT_RECONCILE_SUPERSESSION_RETRIES
    )


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
    """Order delivery admission around a short payout publication."""

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
        started = time.monotonic()
        admitted = False
        with self._condition:
            if generation is None:
                generation = self._published_generation
            while True:
                if cancelled() or self._delivery_blocked:
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
                    break
                future_blocked = generation > self._published_generation
                if not publication_blocked and not future_blocked:
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
                        raise RuntimeError(
                            "payout delivery gate released without admission"
                        )
                    self._active_deliveries -= 1
                    if (
                        priority
                        and admission.delivered
                        and generation == self._priority_generation
                    ):
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
        with self._condition:
            while self._mutation_owner is not None:
                self._condition.wait()
            if mark_blocked is not None and not mark_blocked():
                return False
            self._delivery_blocked = True
            self._condition.notify_all()
            return True

    @contextmanager
    def mutation(self) -> Iterator[None]:
        with self.publication():
            yield


@dataclass(frozen=True)
class PayoutStatePorts:
    """Narrow callbacks into the ledger, job, refresh, and health domains."""

    accepted_share_stats: Callable[[], tuple[int, int]]
    snapshot_at_job_issue: Callable[[int, int], Sequence[object]]
    current_prior_balances: Callable[[], list[dict[str, object]]]
    snapshot_anchor_ms: Callable[[int], int]
    current_template_network_difficulty: Callable[[], int | None]
    pool_ready: Callable[[], bool]
    record_build_phase: Callable[[str, float], None]
    invalidate_job_cache: Callable[[], None]
    clear_retained_collection_refresh: Callable[[], None]
    cancel_obsolete_job_builds: Callable[[str], None]
    cancel_obsolete_bundle_builds: Callable[[int], None]
    payout_invalidated: Callable[[int, float], None]
    payout_published: Callable[[int, float], None]
    schedule_refresh_retry: Callable[[], None]
    chain_block_hash: Callable[[int], str]
    stop_requested: Callable[[], bool]


class PayoutStateService:
    """Single owner of payout state and its publication boundary."""

    def __init__(
        self,
        ports: PayoutStatePorts,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        wall_time_ms: Callable[[], int] | None = None,
        histogram_buckets: Sequence[float] = PRISM_PAYOUT_SECONDS_BUCKETS,
        config: PayoutStateConfig | None = None,
    ) -> None:
        self._ports = ports
        self._config = config or PayoutStateConfig()
        self._monotonic = monotonic
        self._wall_time_ms = wall_time_ms or (lambda: int(time.time() * 1000))
        self._histogram_buckets = tuple(float(value) for value in histogram_buckets)
        self._lock = threading.RLock()
        self._prepare_lock = threading.RLock()
        self._cache_publication_lock = threading.RLock()
        self._balance_mutation_lock = threading.RLock()
        self._preview_condition = threading.Condition()
        self._metrics_lock = threading.Lock()
        self._executor_lock = threading.Lock()
        started = self._monotonic()
        self._generation = 0
        self._source: tuple[int, str | None, str, float] = (
            0,
            None,
            "startup",
            started,
        )
        self._published = PublishedPayoutState(0, 0, None, started)
        self._ledger_artifact: PayoutLedgerArtifact | None = None
        self._ledger_artifact_generation = 0
        self._delivery_gate = PayoutStateDeliveryGate()
        self._previews: dict[str, AcceptedBlockPayoutTransition] = {}
        self._invalidated_previews: dict[str, int | None] = {}
        self._publication_blocked = False
        self._artifact_executor: ThreadPoolExecutor | None = None
        self._artifact_future: Future[None] | None = None
        self._artifact_requested: tuple[int, int] | None = None
        self._artifact_executor_shutdown = False
        self._first_delivery_pending: tuple[int, float] | None = None
        self._state_histograms = {
            name: self._new_histogram()
            for name in ("preparation", "publish", "first_delivery")
        }
        self._gate_histograms = {
            relation: self._new_histogram()
            for relation in PRISM_PAYOUT_DELIVERY_GENERATIONS
        }
        self._discarded_candidates = 0

    def _new_histogram(self) -> dict[str, object]:
        return {
            "buckets": {bucket: 0 for bucket in self._histogram_buckets},
            "sum": 0.0,
            "count": 0,
        }

    def snapshot(self) -> PayoutStateSnapshot:
        with self._lock:
            return PayoutStateSnapshot(
                generation=self._generation,
                source=self._source,
                published=self._published,
                ledger_artifact=self._ledger_artifact,
                publication_blocked=self._publication_blocked,
            )

    def current_artifact(
        self,
        cancellation: CancellationPort | None = None,
    ) -> PayoutStateArtifact:
        while True:
            with self._lock:
                if self._publication_blocked:
                    raise PayoutStatePublicationBlocked(
                        "payout state invalidation is pending publication"
                    )
                published = self._published
                if (
                    published.generation == self._generation
                    and published.artifact is not None
                    and published.artifact.generation == published.generation
                ):
                    return published.artifact
                generation = self._generation
                source_generation = published.source_generation
            artifact = self.prepare_artifact(
                generation=generation,
                source_generation=source_generation,
                cancellation=cancellation,
            )
            with self._lock:
                published = self._published
                if (
                    self._publication_blocked
                    or self._generation != generation
                    or published.source_generation != source_generation
                ):
                    if cancellation is not None:
                        cancellation.raise_if_cancelled(
                            "payout artifact publication race"
                        )
                    continue
                self._published = dataclass_replace(published, artifact=artifact)
                return artifact

    def prepare_artifact(
        self,
        *,
        generation: int,
        source_generation: int,
        cancellation: CancellationPort | None = None,
    ) -> PayoutStateArtifact:
        started = self._monotonic()
        if cancellation is not None:
            cancellation.raise_if_cancelled("payout artifact read")
        with self._prepare_lock:
            balances = self._ports.current_prior_balances()
        if cancellation is not None:
            cancellation.raise_if_cancelled("payout artifact serialization")
        artifact = self.artifact_from_balances(
            generation=generation,
            source_generation=source_generation,
            balances=balances,
        )
        self._ports.record_build_phase(
            "payout_artifact",
            self._monotonic() - started,
        )
        return artifact

    def artifact_from_balances(
        self,
        *,
        generation: int,
        source_generation: int,
        balances: list[dict[str, object]],
    ) -> PayoutStateArtifact:
        balances_json = canonical_json_text(balances)
        return PayoutStateArtifact(
            generation=generation,
            source_generation=source_generation,
            prior_balances_json=balances_json,
            prior_balances_sha256=hashlib.sha256(balances_json.encode()).hexdigest(),
            prepared_monotonic=self._monotonic(),
        )

    def build_ledger_artifact(
        self,
        expected_payout_state_generation: int,
        artifact_payout_state_generation: int,
        network_difficulty: int,
    ) -> PayoutLedgerArtifact | None:
        ledger_started = self._monotonic()
        try:
            accepted_before, _ = self._ports.accepted_share_stats()
            with self._prepare_lock:
                with self._lock:
                    if expected_payout_state_generation != self._generation:
                        return None
                snapshot_window_weight = (
                    PRISM_REWARD_WINDOW_MULTIPLIER
                    * PRISM_SNAPSHOT_WINDOW_MARGIN
                    * int(network_difficulty)
                )
                snapshot_anchor_ms = self._ports.snapshot_anchor_ms(
                    self._wall_time_ms()
                )
                records = list(
                    self._ports.snapshot_at_job_issue(
                        snapshot_anchor_ms,
                        snapshot_window_weight,
                    )
                )
                prior_balances = self._ports.current_prior_balances()
            accepted_after, _ = self._ports.accepted_share_stats()
        except Exception:
            return None
        finally:
            self._ports.record_build_phase(
                "ledger_snapshot",
                self._monotonic() - ledger_started,
            )
        if accepted_before != accepted_after or not records:
            return None
        copy_started = self._monotonic()
        shares_json = tuple(record.to_prism_json() for record in records)
        frozen_balances = tuple(prior_balances)
        self._ports.record_build_phase(
            "serialization_copy",
            self._monotonic() - copy_started,
        )
        return PayoutLedgerArtifact(
            generation=0,
            payout_state_generation=artifact_payout_state_generation,
            network_difficulty=int(network_difficulty),
            accepted_share_count=accepted_after,
            shares_json=shares_json,
            prior_balances=frozen_balances,
            prepared_monotonic=self._monotonic(),
            snapshot_anchor_ms=snapshot_anchor_ms,
        )

    def prepare_ledger_artifact(
        self,
        payout_state_generation: int,
        network_difficulty: int,
    ) -> None:
        artifact = self.build_ledger_artifact(
            payout_state_generation,
            payout_state_generation,
            network_difficulty,
        )
        if artifact is None:
            return
        with self._lock:
            if payout_state_generation != self._generation:
                return
            self._ledger_artifact_generation += 1
            self._ledger_artifact = dataclass_replace(
                artifact,
                generation=self._ledger_artifact_generation,
            )

    def _artifact_preparation_loop(self) -> None:
        while True:
            with self._executor_lock:
                request = self._artifact_requested
                self._artifact_requested = None
                if request is None:
                    self._artifact_future = None
                    return
            self.prepare_ledger_artifact(*request)

    def schedule_ledger_artifact_preparation(
        self,
        payout_state_generation: int,
        network_difficulty: int,
    ) -> None:
        with self._executor_lock:
            if self._artifact_executor_shutdown:
                return
            self._artifact_requested = (
                int(payout_state_generation),
                int(network_difficulty),
            )
            if self._artifact_future is not None:
                return
            executor = self._artifact_executor
            if executor is None:
                executor = ThreadPoolExecutor(
                    max_workers=1,
                    thread_name_prefix="prism-payout-artifact",
                )
                self._artifact_executor = executor
            self._artifact_future = executor.submit(self._artifact_preparation_loop)

    def usable_ledger_artifact(
        self,
        payout_state_generation: int,
        network_difficulty: int,
    ) -> PayoutLedgerArtifact | None:
        with self._lock:
            artifact = self._ledger_artifact
            published_artifact = self._published.artifact
        if (
            artifact is None
            or artifact.payout_state_generation != payout_state_generation
            or artifact.network_difficulty != int(network_difficulty)
        ):
            return None
        if published_artifact is None:
            try:
                published_artifact = self.current_artifact()
            except Exception:
                return None
        try:
            accepted_share_count, _ = self._ports.accepted_share_stats()
        except Exception:
            return None
        if accepted_share_count != artifact.accepted_share_count:
            return None
        balances_sha256 = canonical_json_sha256(artifact.prior_balances)
        with self._lock:
            if (
                self._ledger_artifact is not artifact
                or self._generation != payout_state_generation
                or self._published.artifact is not published_artifact
            ):
                return None
            if balances_sha256 != published_artifact.prior_balances_sha256:
                self._ledger_artifact = None
                return None
            return artifact

    def schedule_current_ledger_artifact_if_missing(self) -> None:
        snapshot = self.snapshot()
        network_difficulty = self._ports.current_template_network_difficulty()
        if network_difficulty is None:
            return
        if (
            self.usable_ledger_artifact(snapshot.generation, network_difficulty)
            is not None
        ):
            return
        self.schedule_ledger_artifact_preparation(
            snapshot.generation,
            network_difficulty,
        )

    def shutdown(self) -> None:
        with self._executor_lock:
            executor = self._artifact_executor
            self._artifact_executor = None
            self._artifact_executor_shutdown = True
            self._artifact_requested = None
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)

    @contextmanager
    def balance_mutation(self) -> Iterator[None]:
        with self._balance_mutation_lock:
            with self._preview_condition:
                landed_transition = any(
                    transition.landed for transition in self._previews.values()
                )
            if landed_transition:
                raise TemplateRefreshBlocked(
                    "accepted block payout confirmation is still pending"
                )
            yield

    def begin_accepted_block_preview(
        self,
        block_hash: str,
        *,
        block_height: int | None = None,
    ) -> None:
        key = block_hash.lower()
        with self._preview_condition:
            self._invalidated_previews.pop(key, None)
            existing = self._previews.get(key)
            if existing is None:
                self._previews[key] = AcceptedBlockPayoutTransition(
                    block_height=block_height
                )
            elif (
                block_height is not None
                and existing.block_height is not None
                and existing.block_height != block_height
            ):
                raise RuntimeError("accepted block payout transition height changed")
            elif existing.block_height is None and block_height is not None:
                self._previews[key] = dataclass_replace(
                    existing,
                    block_height=block_height,
                )

    def mark_accepted_block_landed(
        self,
        block_hash: str,
        *,
        block_height: int,
    ) -> None:
        key = block_hash.lower()
        with self._preview_condition:
            existing = self._previews.get(
                key,
                AcceptedBlockPayoutTransition(block_height=block_height),
            )
            if existing.block_height not in {None, block_height}:
                raise RuntimeError("accepted block payout transition height changed")
            self._previews[key] = dataclass_replace(
                existing,
                block_height=block_height,
                landed=True,
            )
            self._preview_condition.notify_all()

    def publish_accepted_block_preview(
        self,
        block_hash: str,
        balances: list[dict[str, object]],
    ) -> list[dict[str, object]]:
        normalized = self.normalized_prior_balances(balances)
        serialized = self.serialize_prior_balance_preview(normalized)
        key = block_hash.lower()
        with self._balance_mutation_lock:
            with self._preview_condition:
                existing = self._previews.get(key)
                existing_preview = existing.preview if existing is not None else None
                if existing_preview is not None:
                    if existing_preview != serialized:
                        raise RuntimeError(
                            "accepted block payout preview changed during retry"
                        )
                    if existing.published_generation is not None:
                        return self.materialize_prior_balance_preview(existing_preview)
            captured = self.capture_source()
            reserved = self.reserve_source_if_current(
                captured[1],
                "accepted_block_preview",
                tip_hash=key,
                invalidated_monotonic=self._monotonic(),
            )
            candidate = (
                self.current_candidate()
                if reserved is None
                else self.prepared_candidate(reserved)
            )
            candidate = self.accepted_block_preview_candidate(
                candidate,
                block_hash=key,
                preview=serialized,
            )
            self.block_publication(force=True)
            published = self.publish_candidate(candidate)
            if published is None:
                for _attempt in range(self._reconcile_retries()):
                    candidate = self.accepted_block_preview_candidate(
                        self.current_candidate(),
                        block_hash=key,
                        preview=serialized,
                    )
                    published = self.publish_candidate(candidate)
                    if published is not None:
                        break
            if published is None:
                self.block_publication(force=True)
                with self._preview_condition:
                    transition = self._previews.get(
                        key,
                        AcceptedBlockPayoutTransition(landed=True),
                    )
                    self._previews[key] = dataclass_replace(
                        transition,
                        landed=True,
                        preview=serialized,
                        published_generation=None,
                    )
                    self._preview_condition.notify_all()
        return self.materialize_prior_balance_preview(serialized)

    def accepted_block_preview_candidate(
        self,
        candidate: PayoutStateCandidate,
        *,
        block_hash: str,
        preview: tuple[tuple[str, str, str, int], ...],
    ) -> PayoutStateCandidate:
        ledger_artifact = candidate.ledger_artifact
        if ledger_artifact is not None:
            ledger_artifact = dataclass_replace(
                ledger_artifact,
                prior_balances=tuple(self.materialize_prior_balance_preview(preview)),
            )
        return dataclass_replace(
            candidate,
            accepted_block_hash=block_hash,
            accepted_block_preview=preview,
            ledger_artifact=ledger_artifact,
        )

    @staticmethod
    def serialize_prior_balance_preview(
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

    @staticmethod
    def materialize_prior_balance_preview(
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

    def accepted_block_preview_from_bundle(
        self,
        final_bundle: Mapping[str, object],
        *,
        prior_balances: list[dict[str, object]] | None = None,
    ) -> list[dict[str, object]]:
        manifest = final_bundle.get("payout_policy_manifest")
        if not isinstance(manifest, dict) or not isinstance(
            manifest.get("accounts"), list
        ):
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
        return self.normalized_prior_balances(balances)

    def clear_accepted_block_preview(
        self,
        block_hash: str,
        *,
        invalidate_published: bool = False,
    ) -> None:
        key = block_hash.lower()
        existing: AcceptedBlockPayoutTransition | None = None
        with self._balance_mutation_lock:
            with self._preview_condition:
                existing = self._previews.get(key)
                if existing is None:
                    if not invalidate_published:
                        self._invalidated_previews.pop(key, None)
                    self._preview_condition.notify_all()
                    return
                if not invalidate_published:
                    self._previews.pop(key, None)
                    self._invalidated_previews.pop(key, None)
                    self._preview_condition.notify_all()
                    return
                if existing.preview is None:
                    self._previews.pop(key, None)
                    if existing.landed:
                        self._invalidated_previews[key] = existing.block_height
                    self._preview_condition.notify_all()
                    return
            captured = self.capture_source()
            reserved = self.reserve_source_if_current(
                captured[1],
                "accepted_block_preview_withdrawn",
                tip_hash=captured[2],
                invalidated_monotonic=self._monotonic(),
            )
            candidate = self.prepared_candidate(
                reserved if reserved is not None else self.capture_source()
            )
            candidate = dataclass_replace(
                candidate,
                accepted_block_hash=key,
                accepted_block_withdrawal=True,
                accepted_block_height=existing.block_height,
            )
            self.block_publication(force=True)
            published = self.publish_candidate(candidate)
            if published is None:
                for _attempt in range(self._reconcile_retries()):
                    candidate = dataclass_replace(
                        self.current_candidate(),
                        accepted_block_hash=key,
                        accepted_block_withdrawal=True,
                        accepted_block_height=existing.block_height,
                    )
                    published = self.publish_candidate(candidate)
                    if published is not None:
                        break
            if published is None:
                self.block_publication(force=True)
                with self._preview_condition:
                    self._previews.pop(key, None)
                    self._invalidated_previews[key] = existing.block_height
                    self._preview_condition.notify_all()

    def accepted_block_transition_landed(self, block_hash: str) -> bool:
        with self._preview_condition:
            transition = self._previews.get(block_hash.lower())
            return transition is not None and transition.landed

    def accepted_block_transition_for_parent(
        self,
        parent_hash: str,
        *,
        parent_height: int | None = None,
    ) -> tuple[str, bool] | None:
        key = parent_hash.lower()
        with self._preview_condition:
            exact_transition = self._previews.get(key)
            exact_invalidated = key in self._invalidated_previews
            fail_closed_candidate_hashes = {
                candidate_hash
                for candidate_hash, transition in self._previews.items()
                if transition.landed
            }
            fail_closed_candidate_hashes.update(self._invalidated_previews)
            ancestor_candidates = [
                (candidate_hash, transition.block_height, False)
                for candidate_hash, transition in self._previews.items()
                if exact_transition is None
                and not exact_invalidated
                and transition.block_height is not None
                and parent_height is not None
                and transition.block_height <= parent_height
            ]
            ancestor_candidates.extend(
                (candidate_hash, candidate_height, True)
                for candidate_hash, candidate_height in self._invalidated_previews.items()
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
            for candidate_hash, candidate_height, candidate_invalidated in ancestor_candidates:
                assert candidate_height is not None
                active_hash = self._ports.chain_block_hash(candidate_height).lower()
                if active_hash == candidate_hash:
                    active_ancestors.append(
                        (candidate_height, candidate_hash, candidate_invalidated)
                    )
        except Exception as exc:
            self._ports.schedule_refresh_retry()
            raise TemplateRefreshBlocked(
                "could not validate an accepted payout preview on the active chain"
            ) from exc
        if not active_ancestors:
            if any(
                candidate_hash in fail_closed_candidate_hashes
                for candidate_hash, _height, _invalidated in ancestor_candidates
            ):
                self._ports.schedule_refresh_retry()
                raise PayoutStatePublicationBlocked(
                    "accepted payout transition is no longer active"
                )
            return None
        _, selected_key, selected_invalidated = max(active_ancestors)
        return selected_key, selected_invalidated

    def await_pending_parent_preview(
        self,
        parent_hash: str,
        *,
        parent_height: int | None = None,
    ) -> list[dict[str, object]] | None:
        selected = self.accepted_block_transition_for_parent(
            parent_hash,
            parent_height=parent_height,
        )
        if selected is None:
            return None
        selected_key, selected_invalidated = selected
        if selected_invalidated:
            self._ports.schedule_refresh_retry()
            raise TemplateRefreshBlocked(
                "accepted parent payout preview was withdrawn"
            )
        wait_seconds = max(
            0.0,
            float(self._config.accepted_block_preview_wait_seconds),
        )
        deadline = self._monotonic() + wait_seconds
        timed_out = False
        invalidated = False
        with self._preview_condition:
            while selected_key in self._previews:
                transition = self._previews[selected_key]
                if transition.preview is not None:
                    return self.materialize_prior_balance_preview(transition.preview)
                if self._ports.stop_requested():
                    raise RuntimeError(
                        "coordinator stopped while accepted payout preview was pending"
                    )
                remaining = deadline - self._monotonic()
                if remaining <= 0:
                    timed_out = True
                    break
                self._preview_condition.wait(timeout=min(0.25, remaining))
            invalidated = selected_key in self._invalidated_previews
        if invalidated:
            self._ports.schedule_refresh_retry()
            raise TemplateRefreshBlocked(
                "accepted parent payout preview was withdrawn"
            )
        if timed_out:
            self._ports.schedule_refresh_retry()
            raise TemplateRefreshBlocked(
                "accepted parent payout preview is not ready yet"
            )
        return None

    def prior_balances_for_parent(
        self,
        parent_hash: str,
        *,
        parent_height: int | None = None,
        fallback_balances: Sequence[dict[str, object]] | None = None,
    ) -> list[dict[str, object]]:
        preview = self.await_pending_parent_preview(
            parent_hash,
            parent_height=parent_height,
        )
        if preview is not None:
            return preview
        return (
            list(fallback_balances)
            if fallback_balances is not None
            else self._ports.current_prior_balances()
        )

    @staticmethod
    def normalized_prior_balances(
        balances: list[dict[str, object]],
    ) -> list[dict[str, object]]:
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

    def prior_balances_match_current(
        self,
        prior_balances: list[dict[str, object]],
    ) -> bool:
        return self.normalized_prior_balances(
            prior_balances
        ) == self.normalized_prior_balances(self._ports.current_prior_balances())

    def reserve_source(
        self,
        cause: str,
        *,
        tip_hash: str | None = None,
        invalidated_monotonic: float | None = None,
    ) -> int:
        invalidated = (
            self._monotonic()
            if invalidated_monotonic is None
            else invalidated_monotonic
        )
        with self._lock:
            generation = self._source[0] + 1
            self._source = (generation, tip_hash, cause, invalidated)
            return generation

    def reserve_source_for_tip_change(
        self,
        tip_hash: str,
        *,
        cause: str,
        invalidated_monotonic: float,
    ) -> int | None:
        """Reserve a source only when the observed tip identity changed."""

        with self._lock:
            if self._source[1] == tip_hash:
                return None
            generation = self._source[0] + 1
            self._source = (
                generation,
                tip_hash,
                cause,
                invalidated_monotonic,
            )
            return generation

    def reserve_source_if_current(
        self,
        expected_source_generation: int,
        cause: str,
        *,
        tip_hash: str | None = None,
        invalidated_monotonic: float | None = None,
    ) -> tuple[int, int, str | None, str, float] | None:
        invalidated = (
            self._monotonic()
            if invalidated_monotonic is None
            else invalidated_monotonic
        )
        with self._lock:
            if self._source[0] != expected_source_generation:
                return None
            source_generation = expected_source_generation + 1
            self._source = (source_generation, tip_hash, cause, invalidated)
            return (
                self._generation,
                source_generation,
                tip_hash,
                cause,
                invalidated,
            )

    def capture_source(self) -> tuple[int, int, str | None, str, float]:
        with self._lock:
            source_generation, source_tip, cause, invalidated = self._source
            return (
                self._generation,
                source_generation,
                source_tip,
                cause,
                invalidated,
            )

    def prepared_candidate(
        self,
        captured: tuple[int, int, str | None, str, float],
    ) -> PayoutStateCandidate:
        base_generation, source_generation, source_tip, cause, invalidated = captured
        ledger_artifact: PayoutLedgerArtifact | None = None
        network_difficulty = self._ports.current_template_network_difficulty()
        if network_difficulty is not None and self._ports.pool_ready():
            ledger_artifact = self.build_ledger_artifact(
                base_generation,
                base_generation + 1,
                network_difficulty,
            )
        return PayoutStateCandidate(
            base_generation=base_generation,
            source_generation=source_generation,
            source_tip_hash=source_tip,
            cause=cause,
            invalidated_monotonic=invalidated,
            prepared_monotonic=self._monotonic(),
            ledger_artifact=ledger_artifact,
        )

    def current_candidate(self) -> PayoutStateCandidate:
        return self.prepared_candidate(self.capture_source())

    def _record_discarded_candidate(self) -> None:
        with self._metrics_lock:
            self._discarded_candidates += 1

    def block_publication(
        self,
        *,
        force: bool = False,
        supersede_with: tuple[int, str | None, str, float] | None = None,
    ) -> None:
        pending_source: int | None = None

        def mark_blocked() -> bool:
            nonlocal pending_source
            with self._lock:
                if supersede_with is not None:
                    expected_source, fallback_tip, cause, invalidated = supersede_with
                    current_source, current_tip, _, _ = self._source
                    source_tip = (
                        fallback_tip if current_source == expected_source else current_tip
                    )
                    pending_source = current_source + 1
                    self._source = (
                        pending_source,
                        source_tip,
                        cause,
                        invalidated,
                    )
                else:
                    pending_source = self._source[0]
                if (
                    not force
                    and supersede_with is None
                    and pending_source == self._published.source_generation
                ):
                    return False
                self._publication_blocked = True
            # Publish the blocked state before entering the narrow cache
            # fence. Admission either completes before the following clear or
            # observes publication_blocked and fails closed.
            with self._cache_publication_lock:
                self._ports.invalidate_job_cache()
            return True

        if not self._delivery_gate.block_delivery(mark_blocked):
            return
        self._ports.cancel_obsolete_job_builds("payout generation superseded")
        with self._lock:
            next_generation = self._generation + 1
            invalidated = self._source[3]
        self._ports.payout_invalidated(next_generation, invalidated)

    def publication_fenced(self) -> bool:
        with self._lock:
            return self._publication_blocked

    def source_requires_publication(
        self,
        candidate: PayoutStateCandidate | None = None,
    ) -> bool:
        with self._lock:
            if candidate is not None:
                return candidate.source_generation != self._published.source_generation
            return self._source[0] != self._published.source_generation

    def publish_candidate(self, candidate: PayoutStateCandidate) -> int | None:
        with self._lock:
            if (
                candidate.source_generation != self._source[0]
                or candidate.base_generation != self._generation
            ):
                self._record_discarded_candidate()
                return None
        try:
            if (
                candidate.accepted_block_preview is not None
                and not candidate.accepted_block_withdrawal
            ):
                artifact = self.artifact_from_balances(
                    generation=candidate.base_generation + 1,
                    source_generation=candidate.source_generation,
                    balances=self.materialize_prior_balance_preview(
                        candidate.accepted_block_preview
                    ),
                )
            else:
                artifact = self.prepare_artifact(
                    generation=candidate.base_generation + 1,
                    source_generation=candidate.source_generation,
                )
        except Exception:
            self._ports.schedule_refresh_retry()
            raise
        with self._lock:
            if (
                candidate.source_generation != self._source[0]
                or candidate.base_generation != self._generation
            ):
                self._record_discarded_candidate()
                return None
        published_generation: int | None = None
        publish_started = 0.0
        invalidate_job_cache = False
        with self._delivery_gate.publication(), self._cache_publication_lock:
            publish_started = self._monotonic()
            with self._lock:
                source_generation = self._source[0]
                if (
                    candidate.source_generation == source_generation
                    and candidate.base_generation == self._generation
                ):
                    published_generation = self._generation + 1
                    if candidate.accepted_block_hash is not None:
                        key = candidate.accepted_block_hash
                        with self._preview_condition:
                            transition = self._previews.get(
                                key,
                                AcceptedBlockPayoutTransition(
                                    block_height=candidate.accepted_block_height,
                                    landed=True,
                                ),
                            )
                            if candidate.accepted_block_withdrawal:
                                self._previews.pop(key, None)
                                self._invalidated_previews[key] = (
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
                                self._invalidated_previews.pop(key, None)
                                self._previews[key] = dataclass_replace(
                                    transition,
                                    landed=True,
                                    preview=candidate.accepted_block_preview,
                                    published_generation=published_generation,
                                )
                            self._preview_condition.notify_all()
                    self._generation = published_generation
                    prepared_artifact = candidate.ledger_artifact
                    if (
                        prepared_artifact is not None
                        and prepared_artifact.payout_state_generation
                        == published_generation
                    ):
                        self._ledger_artifact_generation += 1
                        self._ledger_artifact = dataclass_replace(
                            prepared_artifact,
                            generation=self._ledger_artifact_generation,
                        )
                    else:
                        self._ledger_artifact = None
                    self._published = PublishedPayoutState(
                        generation=published_generation,
                        source_generation=candidate.source_generation,
                        source_tip_hash=candidate.source_tip_hash,
                        published_monotonic=publish_started,
                        artifact=artifact,
                    )
                    self._publication_blocked = False
                    invalidate_job_cache = True
                    with self._metrics_lock:
                        self._first_delivery_pending = (
                            published_generation,
                            candidate.invalidated_monotonic,
                        )
            if invalidate_job_cache:
                # Publication still owns the delivery gate, but the payout
                # lock is released before callbacks enter J1-owned locks.
                self._ports.invalidate_job_cache()
                self._ports.clear_retained_collection_refresh()
            if published_generation is not None:
                self._delivery_gate.publish_generation(
                    published_generation,
                    prioritize_delivery=True,
                )
        self.observe_seconds(
            "publish",
            max(0.0, self._monotonic() - publish_started),
        )
        if published_generation is None:
            self._record_discarded_candidate()
            return None
        self._ports.cancel_obsolete_bundle_builds(published_generation)
        self._ports.payout_published(
            published_generation,
            candidate.invalidated_monotonic,
        )
        self._ports.cancel_obsolete_job_builds("payout generation published")
        network_difficulty = self._ports.current_template_network_difficulty()
        accepted_preview_pending_durability = (
            candidate.accepted_block_hash is not None
            and not candidate.accepted_block_withdrawal
        )
        if (
            network_difficulty is not None
            and self.usable_ledger_artifact(
                published_generation,
                network_difficulty,
            )
            is None
            and not accepted_preview_pending_durability
        ):
            self.schedule_ledger_artifact_preparation(
                published_generation,
                network_difficulty,
            )
        return published_generation

    def record_first_delivery(
        self,
        generation: int,
        delivered_monotonic: float,
    ) -> None:
        elapsed: float | None = None
        with self._metrics_lock:
            pending = self._first_delivery_pending
            if pending is not None and pending[0] == generation:
                elapsed = max(0.0, delivered_monotonic - pending[1])
                self._first_delivery_pending = None
        if elapsed is not None:
            self.observe_seconds("first_delivery", elapsed)

    def advance_generation(self) -> int:
        self.reserve_source("payout_only")
        prepared_started = self._monotonic()
        with self._prepare_lock:
            self.block_publication(force=True)
            self.observe_seconds(
                "preparation",
                max(0.0, self._monotonic() - prepared_started),
            )
        generation = self.publish_current_with_retry_budget(initial_attempted=False)
        if generation is None:
            raise TemplateRefreshSuperseded(
                "payout-only invalidation was superseded; immediate retry scheduled"
            )
        return generation

    def publish_current_with_retry_budget(
        self,
        *,
        initial_attempted: bool = False,
    ) -> int | None:
        attempts = self._reconcile_retries() + (0 if initial_attempted else 1)
        for _attempt in range(attempts):
            candidate = self.current_candidate()
            published = self.publish_candidate(candidate)
            if published is not None:
                return published
        self.block_publication()
        return None

    def _reconcile_retries(self) -> int:
        return max(0, int(self._config.reconcile_supersession_retries))

    @property
    def reconcile_supersession_retries(self) -> int:
        return self._reconcile_retries()

    def replace_config_for_test(self, config: PayoutStateConfig) -> None:
        self._config = config

    def set_preview_wait_seconds_for_test(self, seconds: float) -> None:
        self._config = dataclass_replace(
            self._config,
            accepted_block_preview_wait_seconds=float(seconds),
        )

    def set_reconcile_retries_for_test(self, retries: int) -> None:
        self._config = dataclass_replace(
            self._config,
            reconcile_supersession_retries=int(retries),
        )

    @property
    def delivery_gate(self) -> PayoutStateDeliveryGate:
        return self._delivery_gate

    @contextmanager
    def cache_publication_admission(self) -> Iterator[None]:
        """Fence J1 cache insertion against payout generation mutation."""
        with self._cache_publication_lock:
            yield

    @contextmanager
    def delivery(
        self,
        generation: int,
        *,
        cancelled: Callable[[], bool],
        priority: bool,
    ) -> Iterator[PayoutDeliveryAdmission]:
        with self._delivery_gate.delivery_cancelable(
            cancelled,
            generation=generation,
            priority=priority,
        ) as admission:
            yield admission

    def observe_gate_admission(
        self,
        admission: object,
        *,
        generation: int,
        fallback_wait_seconds: float,
    ) -> None:
        published_generation = self.snapshot().generation
        relation = getattr(admission, "relation", None)
        if relation not in PRISM_PAYOUT_DELIVERY_GENERATIONS:
            relation = PayoutStateDeliveryGate._generation_relation(
                generation,
                published_generation,
            )
        wait_seconds = float(
            getattr(admission, "wait_seconds", fallback_wait_seconds)
        )
        self.observe_seconds(
            "gate_wait",
            max(0.0, wait_seconds),
            relation=relation,
        )

    def observe_seconds(
        self,
        name: str,
        elapsed_seconds: float,
        *,
        relation: str | None = None,
    ) -> None:
        with self._metrics_lock:
            if name == "gate_wait":
                if relation not in PRISM_PAYOUT_DELIVERY_GENERATIONS:
                    raise ValueError(
                        f"unknown payout delivery generation: {relation}"
                    )
                histogram = self._gate_histograms[str(relation)]
            else:
                histogram = self._state_histograms[name]
            histogram["count"] = int(histogram["count"]) + 1
            histogram["sum"] = float(histogram["sum"]) + elapsed_seconds
            buckets = histogram["buckets"]
            assert isinstance(buckets, dict)
            for bucket in self._histogram_buckets:
                if elapsed_seconds <= bucket:
                    buckets[bucket] = int(buckets.get(bucket, 0)) + 1

    def metrics_snapshot(self) -> dict[str, object]:
        with self._metrics_lock:
            return {
                "state_histograms": {
                    name: {
                        "buckets": dict(histogram["buckets"]),
                        "sum": float(histogram["sum"]),
                        "count": int(histogram["count"]),
                    }
                    for name, histogram in self._state_histograms.items()
                },
                "gate_histograms": {
                    relation: {
                        "buckets": dict(histogram["buckets"]),
                        "sum": float(histogram["sum"]),
                        "count": int(histogram["count"]),
                    }
                    for relation, histogram in self._gate_histograms.items()
                },
                "discarded_candidates": self._discarded_candidates,
            }

    def metrics_lines(self) -> list[str]:
        metrics = self.metrics_snapshot()
        state_histograms = metrics["state_histograms"]
        gate_histograms = metrics["gate_histograms"]
        assert isinstance(state_histograms, dict)
        assert isinstance(gate_histograms, dict)
        metric_names = {
            "preparation": "qbit_prism_payout_preparation_seconds",
            "publish": "qbit_prism_payout_publish_seconds",
            "first_delivery": (
                "qbit_prism_payout_invalidation_first_delivery_seconds"
            ),
        }
        descriptions = {
            "preparation": (
                "Payout reconciliation and candidate preparation outside "
                "delivery publication."
            ),
            "publish": "Atomic payout generation/cache publication gate-hold time.",
            "first_delivery": (
                "Payout invalidation to first delivery of the published generation."
            ),
        }
        lines: list[str] = []
        for name, metric_name in metric_names.items():
            histogram = state_histograms[name]
            assert isinstance(histogram, dict)
            buckets = histogram["buckets"]
            assert isinstance(buckets, dict)
            lines.extend(
                [
                    f"# HELP {metric_name} {descriptions[name]}",
                    f"# TYPE {metric_name} histogram",
                    *[
                        f'{metric_name}_bucket{{le="{bucket:g}"}} '
                        f'{int(buckets.get(bucket, 0))}'
                        for bucket in self._histogram_buckets
                    ],
                    f'{metric_name}_bucket{{le="+Inf"}} {histogram["count"]}',
                    f'{metric_name}_sum {float(histogram["sum"]):.6f}',
                    f'{metric_name}_count {histogram["count"]}',
                ]
            )
        gate_name = "qbit_prism_payout_gate_wait_seconds"
        lines.extend(
            [
                "# HELP qbit_prism_payout_gate_wait_seconds Delivery admission "
                "wait by generation relationship to the published payout state.",
                "# TYPE qbit_prism_payout_gate_wait_seconds histogram",
            ]
        )
        for relation in PRISM_PAYOUT_DELIVERY_GENERATIONS:
            histogram = gate_histograms[relation]
            assert isinstance(histogram, dict)
            buckets = histogram["buckets"]
            assert isinstance(buckets, dict)
            lines.extend(
                [
                    *[
                        f'{gate_name}_bucket{{generation="{relation}",'
                        f'le="{bucket:g}"}} {int(buckets.get(bucket, 0))}'
                        for bucket in self._histogram_buckets
                    ],
                    f'{gate_name}_bucket{{generation="{relation}",le="+Inf"}} '
                    f'{histogram["count"]}',
                    f'{gate_name}_sum{{generation="{relation}"}} '
                    f'{float(histogram["sum"]):.6f}',
                    f'{gate_name}_count{{generation="{relation}"}} '
                    f'{histogram["count"]}',
                ]
            )
        lines.extend(
            [
                "# HELP qbit_prism_payout_candidates_discarded_total Prepared "
                "payout candidates discarded after source supersession.",
                "# TYPE qbit_prism_payout_candidates_discarded_total counter",
                "qbit_prism_payout_candidates_discarded_total "
                f'{metrics["discarded_candidates"]}',
            ]
        )
        return lines

    # The following accessors keep temporary facade/test compatibility without
    # duplicating mutable state on the coordinator. X1 removes these seams.
    @property
    def prepare_lock(self) -> threading.RLock:
        return self._prepare_lock

    @property
    def balance_mutation_lock(self) -> threading.RLock:
        return self._balance_mutation_lock

    @property
    def preview_condition(self) -> threading.Condition:
        return self._preview_condition

    @preview_condition.setter
    def preview_condition(self, value: threading.Condition) -> None:
        self._preview_condition = value

    @property
    def previews(self) -> dict[str, AcceptedBlockPayoutTransition]:
        return self._previews

    @property
    def invalidated_previews(self) -> dict[str, int | None]:
        return self._invalidated_previews

    def replace_generation_for_test(self, generation: int) -> None:
        with self._lock:
            self._generation = int(generation)

    def replace_published_for_test(self, published: PublishedPayoutState) -> None:
        with self._lock:
            self._published = published

    def replace_ledger_artifact_for_test(
        self,
        artifact: PayoutLedgerArtifact | None,
    ) -> None:
        with self._lock:
            self._ledger_artifact = artifact

    def replace_delivery_gate_for_test(self, gate: PayoutStateDeliveryGate) -> None:
        self._delivery_gate = gate
