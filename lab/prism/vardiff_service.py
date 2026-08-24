"""PRISM vardiff windows, retarget delivery, and bounded idle work."""

from __future__ import annotations

from collections import OrderedDict
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from decimal import Decimal
import threading
import time
import traceback
from typing import Any, Callable, Iterable, Protocol

from lab.auxpow import vardiff
from lab.prism.coordinator_config import (
    DEFAULT_PRISM_VARDIFF_RESUME_MAX_ENTRIES,
    DEFAULT_PRISM_VARDIFF_RESUME_MAX_START_FACTOR,
    DEFAULT_PRISM_VARDIFF_RESUME_TTL_SECONDS,
    StratumListenerProfile,
)
from lab.prism.job_bundle import CachedJobBundle, JobBuildSuperseded
from lab.prism.job_delivery import PrismJobContext
from lab.prism.stratum_session import (
    ClientState,
    WorkerIdentity,
    client_vardiff_lock,
)
from lab.prism.worker_difficulty_store import (
    MemoryWorkerDifficultyStore,
    PostgresWorkerDifficultyStore,
    WorkerDifficultyStorePort,
)


PRISM_VARDIFF_IDLE_RETARGET_MAX_WORKERS = 2
MAX_PENDING_VARDIFF_IDLE_RETARGETS = 8
# Hard bound on the durable persistence lane. Entries coalesce by worker
# identity, so this bounds DISTINCT pending workers, not the event rate.
MAX_PENDING_VARDIFF_DURABLE_WRITES = 256
PRISM_VARDIFF_DURABLE_PRUNE_INTERVAL_SECONDS = 300.0
# TTL pruning deletes in bounded batches rather than one unbounded statement,
# so a large inherited backlog cannot produce a single delete that outruns the
# ledger's operation deadline and then never converges. Several batches run per
# pass; whatever is left waits for the next interval.
PRISM_VARDIFF_DURABLE_PRUNE_BATCH = 1024
PRISM_VARDIFF_DURABLE_PRUNE_MAX_BATCHES = 8
# How long the lane's worker parks between wake-ups. Work arrives by event;
# this only bounds how long a stop request waits for the loop to notice.
PRISM_VARDIFF_DURABLE_IDLE_WAKE_SECONDS = 1.0
# Mirrors the coordinator's PRISM_JOB_BUILD_SECONDS_BUCKETS values; owned
# here so metric formatting does not import the coordinator module.
PRISM_VARDIFF_IDLE_SECONDS_BUCKETS = (
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
PRISM_VARDIFF_IDLE_SKIP_REASONS = (
    "busy",
    "disconnected",
    "not_idle",
    "cache_miss",
    "queue_full",
    "superseded",
)
# Bounded outcomes for one retarget taken under the fast-arrival initial
# convergence policy; also the metric label order for
# qbit_prism_vardiff_initial_retargets_total. Their sum is the attempt count
# published alongside them.
PRISM_VARDIFF_INITIAL_RETARGET_OUTCOMES = (
    "applied",     # committed together with its paired job
    "suppressed",  # the computed step landed inside the retarget tolerance
    "superseded",  # difficulty or job state moved before the paired send
    "failed",      # the build or send raised; speculative state was restored
)
# Seconds and accepted-share buckets for the high-difficulty arrival
# histograms. Fixed bucket sets, no per-worker labels: the whole point is to
# see how long a rental-scale session spends below the threshold it should
# be credited at, fleet-wide.
PRISM_VARDIFF_HIGH_DIFF_ARRIVAL_SECONDS_BUCKETS = (
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
    120.0,
    300.0,
    600.0,
)
PRISM_VARDIFF_HIGH_DIFF_ARRIVAL_SHARES_BUCKETS = (
    1,
    2,
    4,
    8,
    16,
    32,
    64,
    128,
    256,
    512,
)
# Bounded outcomes for one reconnect difficulty resume attempt; also the
# metric label order for qbit_prism_vardiff_resume_total.
PRISM_VARDIFF_RESUME_OUTCOMES = (
    "resumed",     # retained value adopted unchanged
    "clamped",     # retained value adopted after being pulled into bounds
    "overridden",  # adopted, then superseded by an explicit difficulty request
    "expired",     # entry existed, TTL had passed
    "miss",        # no entry for this key
    "rejected",    # entry present but unusable (non-finite / non-positive)
    "disabled",    # retention off, or vardiff disabled for this client
)
# Bounded outcomes for one durable worker-difficulty write. The first two are
# evidence-carrying upserts; the next two are the lower-only correction, whose
# "unchanged" means there was no higher stored row left to correct.
PRISM_VARDIFF_DURABLE_WRITE_OUTCOMES = (
    "applied",
    "stale",
    "lowered",
    "unchanged",
    "failed",
    "dropped",
)


@dataclass(frozen=True)
class _PendingDurableWrite:
    """One queued durable worker-difficulty write, keyed by worker identity.

    ``downward_only`` selects the atomic lower-only correction, which carries
    no evidence timestamp: it reduces an existing row without touching
    ``evidence_at``, so it can neither refresh the TTL of a value no share
    proved nor refuse a later share-backed write whose evidence predates it.
    """

    key: tuple[str, str]
    downward_only: bool
    difficulty: Decimal
    evidence_at_ms: int | None
    now_ms: int

    def merged_with(self, later: "_PendingDurableWrite") -> "_PendingDurableWrite":
        """Collapse this entry and a later one for the same worker.

        The result is exactly what applying both in order would leave behind:

        * a later evidence-carrying write supersedes anything queued for the
          key -- it names the difficulty the session actually ended up at, and
          its evidence is what the store arbitrates on;
        * a later downward correction cannot discard a queued evidence-carrying
          write, so it clamps that write's difficulty instead. ``lower`` sets
          the row only when the stored value is greater, so upsert-then-lower
          and a single upsert at the minimum leave the same difficulty and the
          same ``evidence_at``;
        * two downward corrections collapse to the smaller: ``lower`` is
          idempotent under minimum.
        """
        if not later.downward_only:
            return later
        if self.downward_only:
            return _PendingDurableWrite(
                key=self.key,
                downward_only=True,
                difficulty=min(self.difficulty, later.difficulty),
                evidence_at_ms=None,
                now_ms=max(self.now_ms, later.now_ms),
            )
        return _PendingDurableWrite(
            key=self.key,
            downward_only=False,
            difficulty=min(self.difficulty, later.difficulty),
            evidence_at_ms=self.evidence_at_ms,
            now_ms=max(self.now_ms, later.now_ms),
        )


@dataclass(frozen=True)
class RetainedSessionDifficulty:
    difficulty: Decimal
    recorded_monotonic: float


def session_difficulty_key(client: ClientState) -> tuple[str, str] | None:
    """The retention identity for one session: (listener name, exact username).

    Scoping by lane keeps the two lanes' resume policies independent: a
    reconnect to the same port (the reconnect-storm case) always hits, while
    a lane switch is simply a miss that behaves exactly like today. ``None``
    means the connection has no reserved worker yet, so nothing is retained.
    """
    worker = getattr(client, "worker", None)
    if worker is None:
        return None
    return (str(getattr(client, "listener_name", "default")), worker.username)


class SessionDifficultyStore:
    """Bounded, TTL'd, in-process map of last-converged share difficulty.

    Keyed by :func:`session_difficulty_key`. The internal lock is a leaf
    lock: nothing here calls back into the coordinator or client locks while
    holding it. Retention is disabled entirely (``record`` never stores,
    ``lookup`` always answers ``(None, "disabled")``) when ``max_entries`` or
    ``ttl_seconds`` is non-positive.

    In-memory only: the store does not survive a coordinator restart,
    including the liveness watchdog's terminal ``exit_process(1)`` in
    ``background_services.py``. The mass-reconnect wave right after such a
    restart therefore finds an empty store and every session starts at lane
    start, unsmoothed by retention.
    """

    def __init__(self, *, max_entries: int, ttl_seconds: float) -> None:
        self.max_entries = int(max_entries)
        self.ttl_seconds = float(ttl_seconds)
        self._lock = threading.Lock()
        self._entries: OrderedDict[tuple[str, str], RetainedSessionDifficulty] = (
            OrderedDict()
        )
        self.record_count = 0
        self.hit_count = 0
        self.expired_count = 0
        self.miss_count = 0
        self.evicted_count = 0

    @property
    def enabled(self) -> bool:
        return self.max_entries > 0 and self.ttl_seconds > 0

    def record(
        self,
        key: tuple[str, str],
        difficulty: Decimal,
        *,
        now: float,
        share_backed: bool,
    ) -> None:
        """Retain ``difficulty`` for ``key``, ageing the entry honestly.

        The stored difficulty is always updated, but ``recorded_monotonic``
        (what the TTL measures) is refreshed only when the value was
        genuinely re-validated: a first record, a value that actually moved,
        or an unchanged value backed by accepted shares. Re-recording an
        unchanged value with no share evidence -- a session that resumed a
        retained difficulty, submitted nothing and disconnected -- keeps the
        original timestamp, so a reconnect loop shorter than the TTL can no
        longer keep a stale value alive forever. LRU recency is independent
        of the TTL and still moves on every record, exactly as on ``lookup``.
        """
        if not self.enabled:
            return
        if (
            not isinstance(difficulty, Decimal)
            or not difficulty.is_finite()
            or difficulty <= 0
        ):
            return
        with self._lock:
            existing = self._entries.get(key)
            if (
                existing is None
                or difficulty != existing.difficulty
                or share_backed
            ):
                recorded_monotonic = float(now)
            else:
                recorded_monotonic = existing.recorded_monotonic
            self._entries[key] = RetainedSessionDifficulty(
                difficulty=difficulty,
                recorded_monotonic=recorded_monotonic,
            )
            self._entries.move_to_end(key)
            self.record_count += 1
            while len(self._entries) > self.max_entries:
                self._entries.popitem(last=False)
                self.evicted_count += 1

    def lookup(
        self,
        key: tuple[str, str],
        *,
        now: float,
    ) -> tuple[Decimal | None, str]:
        if not self.enabled:
            return None, "disabled"
        with self._lock:
            retained = self._entries.get(key)
            if retained is None:
                self.miss_count += 1
                return None, "miss"
            if now - retained.recorded_monotonic > self.ttl_seconds:
                self._entries.pop(key, None)
                self.expired_count += 1
                return None, "expired"
            # A hit refreshes LRU recency only. The TTL measures the age of
            # the retained value, not of the last access, so
            # recorded_monotonic stays untouched; and the entry survives the
            # hit because a miner may re-authorize.
            self._entries.move_to_end(key)
            self.hit_count += 1
            return retained.difficulty, "hit"

    def preload(
        self,
        entries: Iterable[
            tuple[tuple[str, str], Decimal, float]
        ],
    ) -> int:
        """Seed bounded retained values without counting them as live writes.

        ``recorded_monotonic`` is reconstructed by the durable integration
        layer from each wall-clock evidence timestamp. Callers provide oldest
        first so the resulting LRU order still has the newest evidence at the
        end. Invalid rows are ignored defensively; the durable stores already
        validate them on write and parse.
        """
        if not self.enabled:
            return 0
        loaded = 0
        with self._lock:
            for key, difficulty, recorded_monotonic in entries:
                if (
                    not isinstance(difficulty, Decimal)
                    or not difficulty.is_finite()
                    or difficulty <= 0
                ):
                    continue
                self._entries[key] = RetainedSessionDifficulty(
                    difficulty=difficulty,
                    recorded_monotonic=float(recorded_monotonic),
                )
                self._entries.move_to_end(key)
                loaded += 1
                while len(self._entries) > self.max_entries:
                    self._entries.popitem(last=False)
                    self.evicted_count += 1
        return loaded

    def prune(self, *, now: float) -> int:
        if not self.enabled:
            return 0
        with self._lock:
            expired_keys = [
                key
                for key, retained in self._entries.items()
                if now - retained.recorded_monotonic > self.ttl_seconds
            ]
            for key in expired_keys:
                del self._entries[key]
            self.expired_count += len(expired_keys)
            return len(expired_keys)

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {
                "entries": len(self._entries),
                "records": self.record_count,
                "hits": self.hit_count,
                "expired": self.expired_count,
                "misses": self.miss_count,
                "evicted": self.evicted_count,
            }

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)


@dataclass(frozen=True)
class IdleRetargetRequest:
    """Immutable idle-window identity captured by one bounded sweep."""

    client: ClientState
    connection_id: int
    worker: WorkerIdentity
    active_job: PrismJobContext
    window_started_monotonic: float
    current_difficulty: Decimal
    elapsed_seconds: Decimal


class VardiffRuntime(Protocol):
    """Coordinator operations needed at vardiff's ownership boundary.

    Adjacent reconcile, retained-job, tip-epoch, payout-fence, and heartbeat
    state stays with its owning modules; the service reaches all of it
    through these call-time coordinator seams so monkeypatched instances
    keep intercepting.
    """

    lock: Any
    clients: set[ClientState]
    stop_event: threading.Event
    vardiff_config: vardiff.VardiffConfig
    share_difficulty: Decimal
    vardiff_idle_sweep_seconds: float
    vardiff_resume_enabled: bool
    vardiff_resume_ttl_seconds: float
    vardiff_resume_max_entries: int
    vardiff_resume_max_start_factor: Decimal

    def client_can_receive_jobs(self, client: ClientState) -> bool: ...

    def _record_heartbeat(self, name: str) -> None: ...

    def _record_vardiff_idle_skip(self, reason: str) -> None: ...

    def _cached_idle_job_bundle(self, client: ClientState) -> CachedJobBundle | None: ...

    def _build_idle_job_bundle(
        self,
        request: IdleRetargetRequest,
    ) -> CachedJobBundle: ...

    def _idle_bundle_current_locked(
        self,
        client: ClientState,
        bundle: CachedJobBundle,
        *,
        allow_uncached: bool = False,
    ) -> bool: ...

    def pool_readiness_latched(self) -> bool: ...

    def ensure_reorg_reconciled_for_current_tip(self) -> bool: ...

    def disconnect_client(self, client: ClientState) -> None: ...

    def _maybe_send_job_locked(self, client: ClientState, **kwargs: object) -> bool: ...

    def maybe_send_job(self, client: ClientState, *, clean_jobs: bool) -> bool: ...

    def retarget_client(self, client: ClientState, **kwargs: object) -> bool: ...


class VardiffCompatibilityField:
    """Temporary coordinator view over state owned by ``VardiffService``."""

    def __init__(self, name: str, default: Callable[[], Any]) -> None:
        self.name = name
        self.default = default

    def __get__(self, instance: Any, owner: type[Any]) -> Any:
        if instance is None:
            return self
        init_lock = instance.__dict__.setdefault(
            "_vardiff_service_init_lock",
            threading.RLock(),
        )
        with init_lock:
            service = instance.__dict__.get("_vardiff_service")
            if service is not None:
                return getattr(service, self.name)
            if self.name not in instance.__dict__:
                instance.__dict__[self.name] = self.default()
            return instance.__dict__[self.name]

    def __set__(self, instance: Any, value: Any) -> None:
        init_lock = instance.__dict__.setdefault(
            "_vardiff_service_init_lock",
            threading.RLock(),
        )
        with init_lock:
            service = instance.__dict__.get("_vardiff_service")
            if service is not None:
                setattr(service, self.name, value)
            else:
                instance.__dict__[self.name] = value


VARDIFF_COMPATIBILITY_FIELDS = (
    "idle_retarget_count",
    "_vardiff_idle_lock",
    "_vardiff_idle_executor",
    "_vardiff_idle_executor_shutdown",
    "_vardiff_idle_pending",
    "vardiff_idle_queue_depth",
    "vardiff_idle_inflight",
    "vardiff_idle_clients_inspected",
    "vardiff_idle_skip_counts",
    "vardiff_idle_task_failures",
    "vardiff_idle_sweep_histogram",
    "vardiff_idle_task_histogram",
)


def _new_bucket_histogram(buckets: tuple[float, ...]) -> dict[str, Any]:
    return {
        "buckets": {bucket: 0 for bucket in buckets},
        "sum": 0.0,
        "count": 0,
    }


def _new_histogram() -> dict[str, Any]:
    return _new_bucket_histogram(PRISM_VARDIFF_IDLE_SECONDS_BUCKETS)


def _observe_bucket_histogram(
    histogram: dict[str, Any],
    buckets: tuple[float, ...],
    value: float,
) -> None:
    """Record one observation. Caller holds the histogram's owning lock."""
    histogram["count"] = int(histogram["count"]) + 1
    histogram["sum"] = float(histogram["sum"]) + float(value)
    counts = histogram["buckets"]
    for bucket in buckets:
        if value <= bucket:
            counts[bucket] = int(counts.get(bucket, 0)) + 1


def _histogram_lines(
    metric_name: str,
    description: str,
    histogram: dict[str, Any],
    buckets: tuple[float, ...],
) -> list[str]:
    return [
        f"# HELP {metric_name} {description}",
        f"# TYPE {metric_name} histogram",
        *[
            f'{metric_name}_bucket{{le="{bucket:g}"}} {histogram["buckets"].get(bucket, 0)}'
            for bucket in buckets
        ],
        f'{metric_name}_bucket{{le="+Inf"}} {histogram["count"]}',
        f'{metric_name}_sum {float(histogram["sum"]):.6f}',
        f'{metric_name}_count {histogram["count"]}',
    ]


def _worker_difficulty_store(runtime: VardiffRuntime) -> WorkerDifficultyStorePort:
    """Resolve one process-wide durable-store adapter for the runtime.

    Focused embedders may inject ``worker_difficulty_store`` directly. In
    production the adapter is attached to the ledger so recreating the
    vardiff service in-process does not discard the memory reference store,
    while PostgreSQL remains the cross-process source of truth.
    """
    injected = getattr(runtime, "worker_difficulty_store", None)
    if injected is not None:
        return injected
    ledger = getattr(runtime, "ledger", None)
    owner = ledger if ledger is not None else runtime
    existing = getattr(owner, "_worker_difficulty_store", None)
    if existing is not None:
        return existing
    if (
        ledger is not None
        and getattr(ledger, "backend_name", "") == "postgres-psql"
    ):
        store: WorkerDifficultyStorePort = (
            PostgresWorkerDifficultyStore.from_share_ledger(
                ledger,
                timeout_seconds=float(
                    getattr(runtime, "share_commit_timeout_seconds", 15.0)
                ),
            )
        )
    else:
        store = MemoryWorkerDifficultyStore()
    setattr(owner, "_worker_difficulty_store", store)
    return store


class VardiffService:
    """Own vardiff client windows and its bounded idle-retarget machinery."""

    def __init__(self, runtime: VardiffRuntime) -> None:
        self.runtime = runtime
        self.idle_retarget_count = 0
        self._vardiff_idle_lock = threading.Lock()
        self._vardiff_idle_executor: ThreadPoolExecutor | None = None
        self._vardiff_idle_executor_shutdown = False
        self._vardiff_idle_pending: set[tuple[ClientState, int]] = set()
        self.vardiff_idle_queue_depth = 0
        self.vardiff_idle_inflight = 0
        self.vardiff_idle_clients_inspected = 0
        self.vardiff_idle_skip_counts = {
            reason: 0 for reason in PRISM_VARDIFF_IDLE_SKIP_REASONS
        }
        self.vardiff_idle_task_failures = 0
        self.vardiff_idle_sweep_histogram = _new_histogram()
        self.vardiff_idle_task_histogram = _new_histogram()
        # Reconnect difficulty retention. Sized once from the runtime's
        # loaded configuration (getattr keeps lightweight embedder runtimes
        # working); vardiff_resume_enabled=False behaves exactly as
        # max_entries=0, so the store never records and always answers
        # "disabled".
        resume_enabled = bool(getattr(runtime, "vardiff_resume_enabled", True))
        self.session_difficulty_store = SessionDifficultyStore(
            max_entries=(
                int(
                    getattr(
                        runtime,
                        "vardiff_resume_max_entries",
                        DEFAULT_PRISM_VARDIFF_RESUME_MAX_ENTRIES,
                    )
                )
                if resume_enabled
                else 0
            ),
            ttl_seconds=float(
                getattr(
                    runtime,
                    "vardiff_resume_ttl_seconds",
                    DEFAULT_PRISM_VARDIFF_RESUME_TTL_SECONDS,
                )
            ),
        )
        # Leaf lock for the convergence counters below (resume outcomes and
        # per-lane accepted shares); never call out of this module while
        # holding it, and never take it under the store's lock.
        self._vardiff_convergence_lock = threading.Lock()
        self.vardiff_resume_outcome_counts = {
            outcome: 0 for outcome in PRISM_VARDIFF_RESUME_OUTCOMES
        }
        self.vardiff_lane_accepted_counts: dict[str, int] = {}
        self.vardiff_initial_retarget_attempts = 0
        self.vardiff_initial_retarget_outcome_counts = {
            outcome: 0 for outcome in PRISM_VARDIFF_INITIAL_RETARGET_OUTCOMES
        }
        self.vardiff_high_diff_arrival_seconds_histogram = _new_bucket_histogram(
            PRISM_VARDIFF_HIGH_DIFF_ARRIVAL_SECONDS_BUCKETS
        )
        self.vardiff_high_diff_arrival_shares_histogram = _new_bucket_histogram(
            PRISM_VARDIFF_HIGH_DIFF_ARRIVAL_SHARES_BUCKETS
        )
        self.worker_difficulty_store = _worker_difficulty_store(runtime)
        self.vardiff_durable_preload_records = 0
        self.vardiff_durable_preload_failures = 0
        self.vardiff_durable_prune_failures = 0
        self.vardiff_durable_pruned_records = 0
        self.vardiff_durable_coalesced = 0
        self.vardiff_durable_downward_dropped = 0
        self.vardiff_durable_write_outcome_counts = {
            outcome: 0 for outcome in PRISM_VARDIFF_DURABLE_WRITE_OUTCOMES
        }
        # Durable writes leave the Stratum/client lock path through this
        # bounded, coalescing, single-worker lane. Nothing here reads client
        # state or waits on a client lock: every payload is snapshotted by the
        # caller, so a slow database can never reach back into share
        # accounting or job delivery. The queue is capped by DISTINCT worker
        # identity, and safety-critical downward corrections outrank optional
        # resume hints for both eviction and shutdown drain order.
        self._vardiff_durable_lock = threading.Lock()
        self._vardiff_durable_pending: OrderedDict[
            tuple[str, str], _PendingDurableWrite
        ] = OrderedDict()
        self._vardiff_durable_thread: threading.Thread | None = None
        self._vardiff_durable_wake = threading.Event()
        self._vardiff_durable_stopping = False
        self._vardiff_durable_prune_requested = False
        self._vardiff_durable_inflight = 0
        self._vardiff_durable_operation_timeout_seconds = float(
            getattr(runtime, "share_commit_timeout_seconds", 15.0)
        )
        self._vardiff_durable_next_prune_monotonic = (
            time.monotonic() + PRISM_VARDIFF_DURABLE_PRUNE_INTERVAL_SECONDS
        )
        self._preload_worker_difficulties()

    def _preload_worker_difficulties(self) -> None:
        """Hydrate the authorization-time map with one bounded durable read."""
        store = self.session_difficulty_store
        if not store.enabled:
            return
        now_ms = max(0, int(time.time() * 1000))
        ttl_ms = max(0, int(store.ttl_seconds * 1000))
        cutoff_ms = max(0, now_ms - ttl_ms)
        try:
            recent = self.worker_difficulty_store.load_recent(
                evidence_after_ms=cutoff_ms,
                limit=store.max_entries,
            )
        except Exception:
            self.vardiff_durable_preload_failures += 1
            return
        now_monotonic = time.monotonic()
        # load_recent is newest-first; preload oldest-first so LRU recency
        # ends in the same order as evidence freshness.
        entries = [
            (
                (record.listener, record.worker_username),
                record.difficulty,
                now_monotonic
                - max(0.0, (now_ms - record.evidence_at_ms) / 1000.0),
            )
            for record in reversed(recent)
        ]
        self.vardiff_durable_preload_records = store.preload(entries)
        # Retention is an optimization over the safe cold-start path, so a
        # prune failure must not prevent mining or discard the records already
        # loaded into the process-local map; the periodic prune armed from the
        # idle sweep retries it on the ordinary interval.
        self.vardiff_durable_pruned_records = self._prune_expired_rows(cutoff_ms)

    def client_config(self, client: ClientState) -> vardiff.VardiffConfig:
        """The difficulty policy for one client: its per-client specialization
        if any, else its listener profile, else the default listener's config
        (clients created without one: tests, legacy callers)."""
        with client_vardiff_lock(client):
            return (
                client.vardiff_config
                or client.listener_vardiff_config
                or self.runtime.vardiff_config
            )

    def startup_difficulty(
        self,
        profile: StratumListenerProfile | None = None,
    ) -> Decimal:
        config = (
            profile.vardiff_config
            if profile is not None
            else self.runtime.vardiff_config
        )
        fixed_difficulty = (
            profile.share_difficulty
            if profile is not None
            else self.runtime.share_difficulty
        )
        if not config.enabled:
            return fixed_difficulty
        return vardiff.clamp(
            config.startup_difficulty,
            config.min_difficulty,
            config.max_difficulty,
        )

    @staticmethod
    def desired_difficulty(client: ClientState) -> Decimal:
        # pending_share_difficulty is set by vardiff retargets and by explicit
        # difficulty requests (d=/suggest_difficulty); either way it applies to
        # the next stamped job regardless of whether vardiff is enabled.
        with client_vardiff_lock(client):
            return client.pending_share_difficulty or client.share_difficulty

    def minimum_advertised_difficulty(self, client: ClientState) -> Decimal:
        """The difficulty stamped jobs never advertise below for this client.

        Zero everywhere except floor-bearing listeners (the high-diff port),
        where the effective policy floor governs: the listener minimum, raised
        by any md= specialization. The floor overrides the network-difficulty
        cap because the listener's marketplace contract is checked against the
        first advertised difficulty, even while qbit network difficulty sits
        below the floor.
        """
        with client_vardiff_lock(client):
            if client.minimum_advertised_difficulty <= 0:
                return Decimal("0")
            config = (
                client.vardiff_config
                or client.listener_vardiff_config
                or self.runtime.vardiff_config
            )
            return max(client.minimum_advertised_difficulty, config.min_difficulty)

    def note_submitted(self, client: ClientState) -> None:
        with client_vardiff_lock(client):
            config = (
                client.vardiff_config
                or client.listener_vardiff_config
                or self.runtime.vardiff_config
            )
            if not config.enabled:
                return
            client.vardiff_window_submitted += 1

    def note_accepted(self, client: ClientState, share_difficulty: Decimal) -> None:
        now = time.monotonic()
        # Lane share-rate accounting must stay observable even where vardiff
        # is off, so count before the enabled early-out below.
        lane = str(getattr(client, "listener_name", "default"))
        with self._vardiff_convergence_lock:
            self.vardiff_lane_accepted_counts[lane] = (
                self.vardiff_lane_accepted_counts.get(lane, 0) + 1
            )
        with client_vardiff_lock(client):
            # Per-connection evidence for reconnect retention, set before the
            # enabled early-out so it holds where vardiff is disabled too.
            # Never reset within a connection: one accepted share is enough to
            # prove the difficulty this session is running at is real, which
            # is what lets disconnect refresh the retained value's TTL.
            client.vardiff_accepted_any = True
            client.vardiff_last_accepted_difficulty = share_difficulty
            client.vardiff_last_accepted_wall_ms = max(
                0,
                int(time.time() * 1000),
            )
            config = (
                client.vardiff_config
                or client.listener_vardiff_config
                or self.runtime.vardiff_config
            )
            if not config.enabled:
                return
            client.vardiff_window_accepted += 1
            client.vardiff_window_work += share_difficulty
            client.vardiff_session_accepted = int(
                getattr(client, "vardiff_session_accepted", 0)
            ) + 1
            elapsed_seconds = Decimal(
                str(max(0.001, now - client.vardiff_window_started_monotonic))
            )
            current_difficulty = (
                client.pending_share_difficulty or client.share_difficulty
            )
            # Fast arrival: a session that has not yet had a retarget commit
            # may cross the ordinary interval early, but only on decisive
            # evidence. Both the early trigger and the larger step bound come
            # from the same predicate, so a session can never take the wider
            # step on evidence that was not good enough to trigger it.
            initial_evaluation = bool(
                getattr(client, "vardiff_initial_convergence_pending", True)
                and not getattr(
                    client,
                    "vardiff_initial_convergence_evaluated",
                    False,
                )
                and client.vardiff_window_accepted
                >= config.initial_min_accepted_shares
                and elapsed_seconds >= config.initial_min_elapsed_seconds
            )
            if initial_evaluation:
                client.vardiff_initial_convergence_evaluated = True
            initial_convergence = initial_evaluation and vardiff.initial_convergence_ready(
                current_difficulty=current_difficulty,
                accepted_shares=client.vardiff_window_accepted,
                accepted_difficulty=client.vardiff_window_work,
                elapsed_seconds=elapsed_seconds,
                config=config,
            )
            if (
                elapsed_seconds < config.retarget_interval_seconds
                and not initial_convergence
            ):
                return
            accepted_shares = client.vardiff_window_accepted
            submitted_shares = client.vardiff_window_submitted
            accepted_difficulty = client.vardiff_window_work
            client.vardiff_window_started_monotonic = now
            client.vardiff_window_accepted = 0
            client.vardiff_window_submitted = 0
            client.vardiff_window_work = Decimal("0")
        applied = False
        try:
            applied = bool(
                self.runtime.retarget_client(
                    client,
                    current_difficulty=current_difficulty,
                    accepted_shares=accepted_shares,
                    submitted_shares=submitted_shares,
                    accepted_difficulty=accepted_difficulty,
                    elapsed_seconds=elapsed_seconds,
                    initial_convergence=initial_convergence,
                )
            )
        finally:
            if initial_convergence and not applied:
                with client_vardiff_lock(client):
                    if getattr(
                        client,
                        "vardiff_initial_convergence_pending",
                        True,
                    ):
                        client.vardiff_initial_convergence_evaluated = False

    def _count_resume_outcome(self, outcome: str) -> None:
        if outcome not in PRISM_VARDIFF_RESUME_OUTCOMES:
            raise ValueError(f"unknown vardiff resume outcome: {outcome}")
        with self._vardiff_convergence_lock:
            self.vardiff_resume_outcome_counts[outcome] += 1

    def _count_initial_retarget_attempt(self) -> None:
        with self._vardiff_convergence_lock:
            self.vardiff_initial_retarget_attempts += 1

    def _count_initial_retarget_outcome(self, outcome: str) -> None:
        if outcome not in PRISM_VARDIFF_INITIAL_RETARGET_OUTCOMES:
            raise ValueError(f"unknown vardiff initial retarget outcome: {outcome}")
        with self._vardiff_convergence_lock:
            self.vardiff_initial_retarget_outcome_counts[outcome] += 1

    def observe_high_diff_arrival(
        self,
        client: ClientState,
        *,
        config: vardiff.VardiffConfig,
        previous_difficulty: Decimal,
        next_difficulty: Decimal,
    ) -> bool:
        """Observe this connection's first vardiff crossing of the high-diff
        threshold, in seconds and in accepted shares.

        Observation only -- the threshold never gates or clamps a retarget.
        Recorded at most once per connection, and only for a retarget that
        carried the session from below the threshold to at or above it, so a
        lane that already starts above it (the high-diff listener) never
        contributes and a step-down followed by a re-crossing does not
        double count. Returns whether an observation was recorded.

        The client's vardiff lock is released before the counter lock is
        taken: this module never nests the two in either direction.
        """
        threshold = getattr(
            config,
            "high_diff_arrival_threshold",
            Decimal("0"),
        )
        if (
            not threshold.is_finite()
            or threshold <= 0
            or previous_difficulty >= threshold
            or next_difficulty < threshold
        ):
            return False
        with client_vardiff_lock(client):
            if getattr(client, "vardiff_high_diff_arrival_recorded", False):
                return False
            client.vardiff_high_diff_arrival_recorded = True
            started = getattr(client, "vardiff_session_started_monotonic", None)
            accepted = int(getattr(client, "vardiff_session_accepted", 0))
        with self._vardiff_convergence_lock:
            if started is not None:
                _observe_bucket_histogram(
                    self.vardiff_high_diff_arrival_seconds_histogram,
                    PRISM_VARDIFF_HIGH_DIFF_ARRIVAL_SECONDS_BUCKETS,
                    max(0.0, time.monotonic() - float(started)),
                )
            _observe_bucket_histogram(
                self.vardiff_high_diff_arrival_shares_histogram,
                PRISM_VARDIFF_HIGH_DIFF_ARRIVAL_SHARES_BUCKETS,
                float(accepted),
            )
        return True

    def record_session_difficulty(
        self,
        client: ClientState,
        difficulty: Decimal | None = None,
        *,
        share_backed: bool,
        previous_difficulty: Decimal | None = None,
        initial_convergence: bool = False,
    ) -> None:
        """Retain a session's converged difficulty for reconnect resume.

        Callers pass the just-committed retarget difficulty explicitly; the
        disconnect seam omits it and the client's DELIVERED difficulty is
        read under its vardiff lock instead. pending_share_difficulty is
        deliberately not consulted here: it is advertised with a future job,
        so a disconnect racing an in-flight retarget would otherwise retain a
        value the miner was never given. Nothing is lost -- a retarget whose
        paired send completed records its own value at that commit point.

        ``share_backed`` says whether this difficulty is backed by accepted
        shares; the process-local store uses it to decide whether re-recording
        an unchanged value may refresh its TTL. Durable writes are stricter:
        a committed retarget may carry forward the share evidence that drove
        its estimate, while a disconnect is durable only when the last
        accepted share was stamped at the exact delivered difficulty. This
        prevents an unproven explicit step-up from surviving a restart.

        A committed retarget that LOWERS the difficulty is durable even with
        no accepted shares behind it, but it goes out as an atomic lower-only
        correction rather than a write of new evidence: leaving a stale higher
        row standing would let a restart resurrect a difficulty the live
        session had already abandoned, while re-stamping evidence_at would let
        the correction suppress later genuine share-backed evidence.
        """
        key = session_difficulty_key(client)
        if key is None:
            return
        if not self.session_difficulty_store.enabled:
            return
        committed_retarget = difficulty is not None
        with client_vardiff_lock(client):
            if difficulty is None:
                difficulty = client.share_difficulty
            evidence_difficulty = getattr(
                client,
                "vardiff_last_accepted_difficulty",
                None,
            )
            evidence_at_ms = getattr(
                client,
                "vardiff_last_accepted_wall_ms",
                None,
            )
        self.session_difficulty_store.record(
            key,
            difficulty,
            now=time.monotonic(),
            share_backed=share_backed,
        )
        now_ms = max(0, int(time.time() * 1000))
        downward_commit = bool(
            committed_retarget
            and previous_difficulty is not None
            and difficulty < previous_difficulty
        )
        durably_share_backed = bool(
            share_backed
            and evidence_at_ms is not None
            and (
                (committed_retarget and not initial_convergence)
                or evidence_difficulty == difficulty
            )
        )
        # A share-backed write already carries this exact difficulty plus the
        # evidence that proves it, so it subsumes the downward correction; the
        # lower-only path is for the moves no accepted share backs, above all
        # the idle step-down.
        if durably_share_backed:
            self._enqueue_durable_write(
                _PendingDurableWrite(
                    key=key,
                    downward_only=False,
                    difficulty=difficulty,
                    evidence_at_ms=int(evidence_at_ms),
                    now_ms=now_ms,
                )
            )
        elif downward_commit:
            self._enqueue_durable_write(
                _PendingDurableWrite(
                    key=key,
                    downward_only=True,
                    difficulty=difficulty,
                    evidence_at_ms=None,
                    now_ms=now_ms,
                )
            )

    def _count_durable_write(self, outcome: str) -> None:
        if outcome not in PRISM_VARDIFF_DURABLE_WRITE_OUTCOMES:
            raise ValueError(f"unknown vardiff durable write outcome: {outcome}")
        with self._vardiff_convergence_lock:
            self.vardiff_durable_write_outcome_counts[outcome] += 1

    # -- bounded coalescing persistence lane -------------------------------

    def _enqueue_durable_write(self, pending: _PendingDurableWrite) -> None:
        """Hand one durable write to the lane. Never touches a client lock.

        The payload is fully snapshotted by the caller, so nothing here reads
        client state and nothing waits on ``job_update_lock``: a slow database
        can never reach back into share accounting or job delivery.

        Entries coalesce by worker identity, which is what keeps the queue
        bounded by DISTINCT workers rather than by event rate. The merge is
        order-equivalent to applying both writes in sequence -- see
        :meth:`_PendingDurableWrite.merged_with`.

        After the lane is closed a step-down is applied on the calling thread
        rather than queued, because the shutdown drain has already run and a
        queued entry would simply be stranded.
        """
        lost: _PendingDurableWrite | None = None
        apply_inline = False
        with self._vardiff_durable_lock:
            if self._vardiff_durable_stopping:
                # The lane is closed: its drain has already run, so queueing
                # here would strand the entry. An optional resume hint is
                # dropped, but a step-down is safety critical -- losing it
                # leaves a restart resuming an abandoned difficulty -- so it
                # is applied on this thread instead, bounded by the store's
                # own per-statement operation deadline.
                if pending.downward_only:
                    apply_inline = True
                else:
                    lost = pending
            else:
                existing = self._vardiff_durable_pending.get(pending.key)
                if existing is not None:
                    self._vardiff_durable_pending[pending.key] = (
                        existing.merged_with(pending)
                    )
                    self.vardiff_durable_coalesced += 1
                else:
                    lost = self._evict_for_locked(pending)
                    # _evict_for_locked returns the incoming entry itself when
                    # it is the one refused, in which case it must not also be
                    # queued -- that would push the lane past its bound.
                    if lost is not pending:
                        self._vardiff_durable_pending[pending.key] = pending
                self._ensure_durable_worker_locked()
        if apply_inline:
            # Outside the lane lock: _apply_durable_write reaches the store.
            self._apply_durable_write(pending)
            return
        if lost is not None:
            if lost.downward_only:
                self._count_downward_drop(lost)
            else:
                self._count_durable_write("dropped")
        self._vardiff_durable_wake.set()

    def _evict_for_locked(
        self,
        incoming: _PendingDurableWrite,
    ) -> _PendingDurableWrite | None:
        """Make room for ``incoming`` under the hard queue bound.

        Caller holds ``_vardiff_durable_lock``. Optional resume hints are
        evicted before safety-critical downward corrections, and the incoming
        entry itself is refused only when every queued entry is already a
        downward correction -- which needs MAX_PENDING distinct workers
        stepping down while the database is unavailable.
        """
        if len(self._vardiff_durable_pending) < MAX_PENDING_VARDIFF_DURABLE_WRITES:
            return None
        for key, queued in self._vardiff_durable_pending.items():
            if not queued.downward_only:
                del self._vardiff_durable_pending[key]
                return queued
        if not incoming.downward_only:
            return incoming
        oldest_key = next(iter(self._vardiff_durable_pending))
        return self._vardiff_durable_pending.pop(oldest_key)

    def _count_downward_drop(self, pending: _PendingDurableWrite) -> None:
        """Record -- loudly -- a downward correction the lane could not keep.

        This is the one durable-write loss that can leave a restart resuming a
        difficulty the live session abandoned, so it gets its own counter and
        a log line instead of folding into the generic drop count.
        """
        self._count_durable_write("dropped")
        with self._vardiff_convergence_lock:
            self.vardiff_durable_downward_dropped += 1
        print(
            "prism coordinator: durable vardiff step-down dropped "
            f"listener={pending.key[0]} worker={pending.key[1]} "
            f"difficulty={pending.difficulty} "
            "(reconnect may resume a higher difficulty until its TTL expires)",
            flush=True,
        )

    def _ensure_durable_worker_locked(self) -> None:
        """Start the lane's single worker thread. Caller holds the lane lock."""
        if self._vardiff_durable_thread is not None:
            return
        if self._vardiff_durable_stopping:
            return
        thread = threading.Thread(
            target=self._durable_worker_loop,
            name="prism-vardiff-durable",
            daemon=True,
        )
        self._vardiff_durable_thread = thread
        thread.start()

    def _durable_worker_loop(self) -> None:
        while True:
            self._vardiff_durable_wake.wait(
                PRISM_VARDIFF_DURABLE_IDLE_WAKE_SECONDS
            )
            self._vardiff_durable_wake.clear()
            with self._vardiff_durable_lock:
                stopping = self._vardiff_durable_stopping
            self._drain_durable_pending(
                deadline=None if not stopping else (
                    time.monotonic() + self._vardiff_durable_operation_timeout_seconds
                ),
                final=stopping,
            )
            self._prune_durable_worker_difficulties_if_requested()
            if stopping:
                return

    def _next_pending_locked(self) -> _PendingDurableWrite | None:
        """Pop the next entry, safety-critical corrections first.

        Caller holds ``_vardiff_durable_lock``. Downward-first ordering is
        what lets a deadline-bounded shutdown drain finish the writes that
        matter even when optional resume hints are still queued behind them.
        """
        chosen_key: tuple[str, str] | None = None
        for key, queued in self._vardiff_durable_pending.items():
            if queued.downward_only:
                chosen_key = key
                break
        if chosen_key is None:
            chosen_key = next(iter(self._vardiff_durable_pending), None)
        if chosen_key is None:
            return None
        return self._vardiff_durable_pending.pop(chosen_key)

    def _drain_durable_pending(
        self,
        *,
        deadline: float | None,
        final: bool,
    ) -> None:
        """Apply queued writes until the queue empties or ``deadline`` passes.

        ``deadline`` bounds the whole drain with the same budget the ledger
        gives one operation; each individual statement is separately bounded
        by the store's own operation timeout. On a final drain anything still
        queued when the budget runs out is accounted for explicitly.
        """
        while True:
            if deadline is not None and time.monotonic() >= deadline:
                break
            with self._vardiff_durable_lock:
                pending = self._next_pending_locked()
                # Counted as in-flight while the lane lock is held, so a
                # concurrent flush cannot observe an empty queue between the
                # pop and the write landing.
                self._vardiff_durable_inflight += 1 if pending is not None else 0
            if pending is None:
                return
            try:
                self._apply_durable_write(pending)
            finally:
                with self._vardiff_durable_lock:
                    self._vardiff_durable_inflight -= 1
        if not final:
            return
        with self._vardiff_durable_lock:
            abandoned = list(self._vardiff_durable_pending.values())
            self._vardiff_durable_pending.clear()
        for pending in abandoned:
            if pending.downward_only:
                self._count_downward_drop(pending)
            else:
                self._count_durable_write("dropped")

    def _apply_durable_write(self, pending: _PendingDurableWrite) -> None:
        try:
            if pending.downward_only:
                lowered = self.worker_difficulty_store.apply_downward(
                    listener=pending.key[0],
                    worker_username=pending.key[1],
                    difficulty=pending.difficulty,
                    now_ms=pending.now_ms,
                )
                outcome = "lowered" if lowered.applied else "unchanged"
            else:
                result = self.worker_difficulty_store.upsert(
                    listener=pending.key[0],
                    worker_username=pending.key[1],
                    difficulty=pending.difficulty,
                    evidence_at_ms=int(pending.evidence_at_ms or 0),
                    now_ms=pending.now_ms,
                )
                outcome = "applied" if result.applied else "stale"
        except Exception:
            outcome = "failed"
        self._count_durable_write(outcome)

    def request_durable_prune_if_due(self) -> bool:
        """Arm a TTL prune when one is due, without doing database work here.

        Called from the idle sweep, so pruning is driven by the clock rather
        than by successful write traffic: a lane whose writes all fail, or a
        process that inherits rows and writes none of its own, still prunes.
        Returns whether a prune was armed.
        """
        if not self.session_difficulty_store.enabled:
            return False
        now_monotonic = time.monotonic()
        with self._vardiff_durable_lock:
            if now_monotonic < self._vardiff_durable_next_prune_monotonic:
                return False
            self._vardiff_durable_next_prune_monotonic = (
                now_monotonic + PRISM_VARDIFF_DURABLE_PRUNE_INTERVAL_SECONDS
            )
            if self._vardiff_durable_stopping:
                return False
            self._vardiff_durable_prune_requested = True
            self._ensure_durable_worker_locked()
        self._vardiff_durable_wake.set()
        return True

    def _prune_expired_rows(self, cutoff_ms: int) -> int:
        """Delete expired rows in bounded batches; never one open-ended DELETE.

        Every statement carries a finite ``limit`` so it stays inside the
        ledger's operation deadline, and batching continues only while a full
        batch comes back, so an inherited backlog drains over successive
        intervals instead of stalling on a delete it can never finish.
        """
        pruned = 0
        for _ in range(PRISM_VARDIFF_DURABLE_PRUNE_MAX_BATCHES):
            try:
                deleted = self.worker_difficulty_store.prune(
                    evidence_cutoff_ms=cutoff_ms,
                    limit=PRISM_VARDIFF_DURABLE_PRUNE_BATCH,
                )
            except Exception:
                with self._vardiff_convergence_lock:
                    self.vardiff_durable_prune_failures += 1
                break
            pruned += int(deleted)
            if int(deleted) < PRISM_VARDIFF_DURABLE_PRUNE_BATCH:
                break
        return pruned

    def _prune_durable_worker_difficulties_if_requested(self) -> None:
        with self._vardiff_durable_lock:
            if not self._vardiff_durable_prune_requested:
                return
            self._vardiff_durable_prune_requested = False
        now_ms = max(0, int(time.time() * 1000))
        cutoff_ms = max(
            0,
            now_ms - int(self.session_difficulty_store.ttl_seconds * 1000),
        )
        pruned = self._prune_expired_rows(cutoff_ms)
        if not pruned:
            return
        with self._vardiff_convergence_lock:
            self.vardiff_durable_pruned_records += pruned

    def durable_pending_depth(self) -> int:
        with self._vardiff_durable_lock:
            return len(self._vardiff_durable_pending)

    def durable_coalesced_count(self) -> int:
        """Coalesced-entry count, read under the lane lock that maintains it."""
        with self._vardiff_durable_lock:
            return int(self.vardiff_durable_coalesced)

    def flush_durable_writes(self, *, timeout: float | None = None) -> bool:
        """Wait until the lane has nothing queued or in flight.

        No production caller needs this -- durable retention is deliberately
        fire-and-forget so mining never waits on it. It exists so tests and
        the shutdown drain can observe the lane deterministically. Returns
        whether the lane went quiet within the budget.
        """
        budget = (
            self._vardiff_durable_operation_timeout_seconds
            if timeout is None
            else float(timeout)
        )
        deadline = time.monotonic() + max(0.0, budget)
        self._vardiff_durable_wake.set()
        while True:
            with self._vardiff_durable_lock:
                quiet = (
                    not self._vardiff_durable_pending
                    and self._vardiff_durable_inflight == 0
                )
                worker_running = self._vardiff_durable_thread is not None
            if quiet:
                return True
            if not worker_running:
                # Nothing will drain this; do it on the caller's thread rather
                # than spin until the budget expires.
                self._drain_durable_pending(deadline=deadline, final=False)
                continue
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.001)

    def shutdown_durable_executor(self) -> None:
        """Close the lane, draining safety-critical corrections first.

        Queued work is drained, never cancelled: the whole point of the
        downward correction is that losing it leaves a restart resuming an
        abandoned difficulty. The drain is bounded by the ledger's operation
        timeout so shutdown cannot stall on an unreachable database, and
        anything the budget cannot cover is counted and logged.
        """
        with self._vardiff_durable_lock:
            already_stopped = self._vardiff_durable_stopping
            self._vardiff_durable_stopping = True
            thread = self._vardiff_durable_thread
            self._vardiff_durable_thread = None
        if already_stopped and thread is None:
            return
        deadline = (
            time.monotonic() + self._vardiff_durable_operation_timeout_seconds
        )
        self._vardiff_durable_wake.set()
        if thread is not None:
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
        # The worker owns the drain while it lives; this covers a lane whose
        # worker never started, already exited, or ran out of budget.
        self._drain_durable_pending(deadline=deadline, final=True)

    def note_resume_overridden(self) -> None:
        """Count a resumed difficulty that an explicit request superseded.

        resumed/clamped stay attempt counters, so
        ``resumed + clamped - overridden`` is the number of resumes that
        actually stuck. The caller is the authorize path: it is the only
        place that knows both the value the resume applied and the target a
        password ``d=``/``md=`` or a pending suggestion resolved to.
        """
        self._count_resume_outcome("overridden")

    def resume_client_difficulty(self, client: ClientState) -> Decimal | None:
        """The difficulty a reconnecting worker should resume at, or None.

        A retained value is only adopted inside the lane's plausibility
        bounds: the ceiling is min(lane max, lane start * resume factor), so
        a stale or absurd entry can never be adopted as-is. A retained value
        BELOW the lane start is adopted unchanged on purpose -- vardiff
        targets a share rate, so a session at its converged difficulty is on
        target by definition, and cold-starting it higher would give the pool
        fewer shares, not more.
        """
        with client_vardiff_lock(client):
            config = (
                client.vardiff_config
                or client.listener_vardiff_config
                or self.runtime.vardiff_config
            )
        if not config.enabled:
            self._count_resume_outcome("disabled")
            return None
        key = session_difficulty_key(client)
        if key is None:
            return None
        retained, outcome = self.session_difficulty_store.lookup(
            key,
            now=time.monotonic(),
        )
        if retained is None:
            self._count_resume_outcome(outcome)
            return None
        if not retained.is_finite() or retained <= 0:
            self._count_resume_outcome("rejected")
            return None
        factor = getattr(
            self.runtime,
            "vardiff_resume_max_start_factor",
            DEFAULT_PRISM_VARDIFF_RESUME_MAX_START_FACTOR,
        )
        ceiling = min(config.max_difficulty, config.startup_difficulty * factor)
        floor = config.min_difficulty
        resumed = vardiff.clamp(retained, floor, max(floor, ceiling))
        self._count_resume_outcome(
            "resumed" if resumed == retained else "clamped"
        )
        return resumed

    def apply_resumed_difficulty(self, client: ClientState) -> Decimal | None:
        """Adopt the retained difficulty onto a freshly authorizing client."""
        with client_vardiff_lock(client):
            resumed = self.resume_client_difficulty(client)
            if resumed is None:
                return None
            current = client.pending_share_difficulty or client.share_difficulty
            if resumed != current:
                client.share_difficulty = resumed
                client.pending_share_difficulty = None
                client.difficulty_generation = int(
                    getattr(client, "difficulty_generation", 0)
                ) + 1
            return resumed

    def convergence_snapshot(self) -> dict[str, object]:
        """Copied convergence/retention counters plus the at-ceiling census.

        Lock order in this codebase is client vardiff lock -> coordinator
        lock, never the reverse; membership is therefore snapshotted under
        the coordinator lock alone, released, and only then is each client's
        vardiff lock taken to read its effective policy and difficulty.

        The at-ceiling census is therefore O(connections) lock acquisitions
        per call, bounded by the deployed 4096-connection cap. That cost is
        acceptable because this runs on the background metrics refresher and
        /metrics serves its cached payload, and because each vardiff lock is
        held for a few field reads only -- no client vardiff lock is ever
        held across a socket write, so a slow miner cannot convoy the census.
        """
        with self.runtime.lock:
            clients = tuple(self.runtime.clients)
        sessions_at_max = 0
        for client in clients:
            with client_vardiff_lock(client):
                config = (
                    client.vardiff_config
                    or client.listener_vardiff_config
                    or self.runtime.vardiff_config
                )
                if not config.enabled:
                    continue
                current = (
                    client.pending_share_difficulty or client.share_difficulty
                )
                if current >= config.max_difficulty:
                    sessions_at_max += 1
        with self._vardiff_convergence_lock:
            lane_accepted = dict(self.vardiff_lane_accepted_counts)
            resume_outcomes = dict(self.vardiff_resume_outcome_counts)
            durable_writes = dict(self.vardiff_durable_write_outcome_counts)
            durable_pruned = int(self.vardiff_durable_pruned_records)
            durable_prune_failures = int(self.vardiff_durable_prune_failures)
            durable_downward_dropped = int(self.vardiff_durable_downward_dropped)
        # Prune first so the retained-sessions gauge counts only entries a
        # reconnect could still adopt.
        self.session_difficulty_store.prune(now=time.monotonic())
        store = self.session_difficulty_store.snapshot()
        return {
            "sessions_at_max_difficulty": sessions_at_max,
            "lane_accepted_shares": lane_accepted,
            "resume_outcomes": resume_outcomes,
            "retained_sessions": int(store["entries"]),
            "store": store,
            "durable_store": {
                "preloaded": self.vardiff_durable_preload_records,
                "preload_failures": self.vardiff_durable_preload_failures,
                "pruned": durable_pruned,
                "prune_failures": durable_prune_failures,
                "pending": self.durable_pending_depth(),
                "coalesced": self.durable_coalesced_count(),
                "downward_dropped": durable_downward_dropped,
                "writes": durable_writes,
            },
        }

    def record_idle_skip(self, reason: str) -> None:
        if reason not in PRISM_VARDIFF_IDLE_SKIP_REASONS:
            raise ValueError(f"unknown vardiff idle skip reason: {reason}")
        with self._vardiff_idle_lock:
            self.vardiff_idle_skip_counts[reason] += 1

    def observe_idle_seconds(self, name: str, elapsed_seconds: float) -> None:
        if name not in {"sweep", "task"}:
            raise ValueError(f"unknown vardiff idle histogram: {name}")
        with self._vardiff_idle_lock:
            histogram = getattr(self, f"vardiff_idle_{name}_histogram")
            histogram["count"] = int(histogram["count"]) + 1
            histogram["sum"] = float(histogram["sum"]) + elapsed_seconds
            buckets = histogram["buckets"]
            for bucket in PRISM_VARDIFF_IDLE_SECONDS_BUCKETS:
                if elapsed_seconds <= bucket:
                    buckets[bucket] = int(buckets.get(bucket, 0)) + 1

    def idle_tip_diverged_locked(self) -> bool:
        """Whether detected tip work still lacks published submit authority."""
        published = getattr(self.runtime, "current_tip_first_seen", None)
        latest_detected = getattr(self.runtime, "latest_detected_tip", None)
        return bool(
            latest_detected is not None
            and (published is None or latest_detected[0] != published[0])
        )

    def request_skip_reason(
        self,
        request: IdleRetargetRequest,
    ) -> str | None:
        client = request.client
        # Take the per-client lock before coordinator admission. A share can
        # delay this client's idle retarget, but it can never make the retarget
        # hold the coordinator lock while waiting and convoy tip publication.
        with client_vardiff_lock(client):
            with self.runtime.lock:
                if self.idle_tip_diverged_locked():
                    return "superseded"
                if (
                    client not in self.runtime.clients
                    or getattr(client, "closing", False)
                    or not self.runtime.client_can_receive_jobs(client)
                ):
                    return "disconnected"
                if (
                    client.connection_id != request.connection_id
                    or client.worker != request.worker
                    or client.active_job is not request.active_job
                    or (client.pending_share_difficulty or client.share_difficulty)
                    != request.current_difficulty
                ):
                    return "superseded"
                if (
                    client.vardiff_window_started_monotonic
                    != request.window_started_monotonic
                    or client.vardiff_window_accepted != 0
                    or client.vardiff_window_submitted != 0
                ):
                    return "not_idle"
        return None

    def _request_pending(self, request: IdleRetargetRequest) -> bool:
        with self._vardiff_idle_lock:
            return (
                request.client,
                request.connection_id,
            ) in self._vardiff_idle_pending

    def _finish_idle_task(
        self,
        key: tuple[ClientState, int],
        queued_monotonic: float,
        *,
        started: bool,
    ) -> None:
        with self._vardiff_idle_lock:
            if key not in self._vardiff_idle_pending:
                return
            self._vardiff_idle_pending.discard(key)
            if started:
                self.vardiff_idle_inflight = max(0, self.vardiff_idle_inflight - 1)
            else:
                self.vardiff_idle_queue_depth = max(
                    0,
                    self.vardiff_idle_queue_depth - 1,
                )
        self.observe_idle_seconds(
            "task",
            max(0.0, time.monotonic() - queued_monotonic),
        )

    def _run_idle_task(
        self,
        request: IdleRetargetRequest,
        bundle: CachedJobBundle | None,
        queued_monotonic: float,
    ) -> None:
        key = (request.client, request.connection_id)
        with self._vardiff_idle_lock:
            if key not in self._vardiff_idle_pending:
                return
            self.vardiff_idle_queue_depth = max(
                0,
                self.vardiff_idle_queue_depth - 1,
            )
            self.vardiff_idle_inflight += 1
        client = request.client
        delivery_attempted = False
        try:
            reason = self.request_skip_reason(request)
            if reason is not None:
                self.runtime._record_vardiff_idle_skip(reason)
                return
            # Readiness may have crossed in the ledger after the sweep's
            # cache-only snapshot. Refresh it on this bounded worker so a
            # cached collection bundle cannot be delivered after the pool is
            # ready for normal payout work.
            self.runtime.pool_readiness_latched()
            # Canonicalize the sweep's cache-only snapshot on the dedicated
            # worker. shared_job_bundle() selects the current payout-artifact
            # key and rebinds a ready heavy bundle to the latest same-tip
            # template observation; a miss may build here, never on the sweep.
            bundle = self.runtime._build_idle_job_bundle(request)
            reason = self.request_skip_reason(request)
            if reason is not None:
                self.runtime._record_vardiff_idle_skip(reason)
                return
            # Prepared bundles bypass _maybe_send_job_locked's normal build
            # admission, so preserve its live reorg/headers/IBD trust guard on
            # the dedicated worker before taking the client lock or sending.
            if not self.runtime.ensure_reorg_reconciled_for_current_tip():
                self.runtime._record_vardiff_idle_skip("superseded")
                return
            if not client.job_update_lock.acquire(blocking=False):
                self.runtime._record_vardiff_idle_skip("busy")
                return
            try:
                reason = self.request_skip_reason(request)
                if reason is not None:
                    self.runtime._record_vardiff_idle_skip(reason)
                    return
                with self.runtime._job_cache_lock:
                    bundle_current = self.runtime._idle_bundle_current_locked(
                        client,
                        bundle,
                        allow_uncached=True,
                    )
                if not bundle_current:
                    self.runtime._record_vardiff_idle_skip("superseded")
                    return
                # Everything above this point is coordinator preparation. An
                # OSError there belongs to qbit RPC/ledger I/O, not the miner
                # socket. Only retire the connection after entering the paired
                # client delivery path below.
                delivery_attempted = True
                retargeted = self.retarget_locked(
                    client,
                    current_difficulty=request.current_difficulty,
                    accepted_shares=0,
                    submitted_shares=0,
                    accepted_difficulty=Decimal("0"),
                    elapsed_seconds=request.elapsed_seconds,
                    require_idle=True,
                    prepared_bundle=bundle,
                    expected_connection_id=request.connection_id,
                    expected_worker=request.worker,
                    expected_active_job=request.active_job,
                    expected_window_started=request.window_started_monotonic,
                    prepared_bundle_allow_uncached=True,
                )
            finally:
                client.job_update_lock.release()
            if retargeted:
                with self.runtime.lock:
                    self.idle_retarget_count = int(
                        getattr(self, "idle_retarget_count", 0)
                    ) + 1
                return
            reason = self.request_skip_reason(request)
            if reason is not None:
                self.runtime._record_vardiff_idle_skip(reason)
                return
            with self.runtime._job_cache_lock:
                bundle_current = self.runtime._idle_bundle_current_locked(
                    client,
                    bundle,
                    allow_uncached=True,
                )
            if not bundle_current:
                self.runtime._record_vardiff_idle_skip("superseded")
        except JobBuildSuperseded:
            self.runtime._record_vardiff_idle_skip("superseded")
        except OSError:
            with self._vardiff_idle_lock:
                self.vardiff_idle_task_failures += 1
            if delivery_attempted:
                self.runtime.disconnect_client(client)
                return
            print(
                "prism coordinator: idle vardiff retarget preparation failed; "
                "keeping client connected",
                flush=True,
            )
            traceback.print_exc()
        except Exception:
            with self._vardiff_idle_lock:
                self.vardiff_idle_task_failures += 1
            print("prism coordinator: idle vardiff retarget task failed", flush=True)
            traceback.print_exc()

    def _enqueue_idle(
        self,
        request: IdleRetargetRequest,
        bundle: CachedJobBundle | None,
    ) -> str | None:
        key = (request.client, request.connection_id)
        queued_monotonic = time.monotonic()
        with self._vardiff_idle_lock:
            if (
                self._vardiff_idle_executor_shutdown
                or key in self._vardiff_idle_pending
            ):
                return "superseded"
            if len(self._vardiff_idle_pending) >= MAX_PENDING_VARDIFF_IDLE_RETARGETS:
                return "queue_full"
            executor = self._vardiff_idle_executor
            if executor is None:
                executor = ThreadPoolExecutor(
                    max_workers=PRISM_VARDIFF_IDLE_RETARGET_MAX_WORKERS,
                    thread_name_prefix="prism-vardiff-idle",
                )
                self._vardiff_idle_executor = executor
            self._vardiff_idle_pending.add(key)
            self.vardiff_idle_queue_depth += 1
            try:
                future = executor.submit(
                    self._run_idle_task,
                    request,
                    bundle,
                    queued_monotonic,
                )
            except RuntimeError:
                self._vardiff_idle_pending.discard(key)
                self.vardiff_idle_queue_depth = max(
                    0,
                    self.vardiff_idle_queue_depth - 1,
                )
                return "queue_full"

        def finish_task(completed: Future[None]) -> None:
            self._finish_idle_task(
                key,
                queued_monotonic,
                started=not completed.cancelled(),
            )

        future.add_done_callback(finish_task)
        return None

    def shutdown_idle_executor(self) -> None:
        with self._vardiff_idle_lock:
            executor = self._vardiff_idle_executor
            self._vardiff_idle_executor = None
            self._vardiff_idle_executor_shutdown = True
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)
        self.shutdown_durable_executor()

    def idle_sweep_loop(self) -> None:
        while not self.runtime.stop_event.wait(
            self.runtime.vardiff_idle_sweep_seconds
        ):
            self.runtime._record_heartbeat("vardiff_idle_sweep")
            try:
                queued = self.idle_sweep_once()
                if queued:
                    print(
                        "prism coordinator: idle vardiff sweep queued "
                        f"{queued} client(s)",
                        flush=True,
                    )
            except Exception:
                print("prism coordinator: idle vardiff sweep failed", flush=True)
                traceback.print_exc()

    def idle_sweep_once(self) -> int:
        sweep_started = time.monotonic()
        now = time.monotonic()
        queued = 0
        # Arm the durable TTL prune from the clock, not from write traffic:
        # this only flips a flag and wakes the lane, so no database work
        # happens on the sweep thread.
        self.request_durable_prune_if_due()
        try:
            with self.runtime.lock:
                clients = tuple(self.runtime.clients)
            with self._vardiff_idle_lock:
                self.vardiff_idle_clients_inspected += len(clients)
            for client in clients:
                self.runtime._record_heartbeat("vardiff_idle_sweep")
                with client_vardiff_lock(client), self.runtime.lock:
                    if self.idle_tip_diverged_locked():
                        reason = "superseded"
                        request = None
                    elif (
                        client not in self.runtime.clients
                        or not self.runtime.client_can_receive_jobs(client)
                    ):
                        reason = "disconnected"
                        request = None
                    else:
                        active_job = client.active_job
                        worker = client.worker
                        config = (
                            client.vardiff_config
                            or client.listener_vardiff_config
                            or self.runtime.vardiff_config
                        )
                        if not config.enabled:
                            continue
                        if active_job is None or worker is None:
                            reason = "superseded"
                            request = None
                        else:
                            elapsed = Decimal(
                                str(
                                    max(
                                        0.001,
                                        now
                                        - client.vardiff_window_started_monotonic,
                                    )
                                )
                            )
                            if (
                                elapsed < config.retarget_interval_seconds
                                or client.vardiff_window_accepted != 0
                                or client.vardiff_window_submitted != 0
                            ):
                                reason = "not_idle"
                                request = None
                            else:
                                reason = None
                                request = IdleRetargetRequest(
                                    client=client,
                                    connection_id=client.connection_id,
                                    worker=worker,
                                    active_job=active_job,
                                    window_started_monotonic=(
                                        client.vardiff_window_started_monotonic
                                    ),
                                    current_difficulty=(
                                        client.pending_share_difficulty
                                        or client.share_difficulty
                                    ),
                                    elapsed_seconds=elapsed,
                                )
                if reason is not None:
                    self.runtime._record_vardiff_idle_skip(reason)
                    continue
                assert request is not None
                if self._request_pending(request):
                    self.runtime._record_vardiff_idle_skip("superseded")
                    continue
                if not client.job_update_lock.acquire(blocking=False):
                    self.runtime._record_vardiff_idle_skip("busy")
                    continue
                try:
                    reason = self.request_skip_reason(request)
                finally:
                    client.job_update_lock.release()
                if reason is not None:
                    self.runtime._record_vardiff_idle_skip(reason)
                    continue
                bundle = self.runtime._cached_idle_job_bundle(client)
                if bundle is None:
                    # The sweep itself stays cache-only. A missing/expired
                    # bundle is rebuilt only by the dedicated bounded worker,
                    # so the client still makes eventual vardiff progress.
                    self.runtime._record_vardiff_idle_skip("cache_miss")
                reason = self._enqueue_idle(request, bundle)
                if reason is not None:
                    self.runtime._record_vardiff_idle_skip(reason)
                    continue
                queued += 1
            return queued
        finally:
            self.runtime._record_heartbeat("vardiff_idle_sweep")
            self.observe_idle_seconds(
                "sweep",
                max(0.0, time.monotonic() - sweep_started),
            )

    def retarget(
        self,
        client: ClientState,
        *,
        current_difficulty: Decimal,
        accepted_shares: int,
        submitted_shares: int,
        accepted_difficulty: Decimal,
        elapsed_seconds: Decimal,
        require_idle: bool = False,
        prepared_bundle: CachedJobBundle | None = None,
        expected_connection_id: int | None = None,
        expected_worker: WorkerIdentity | None = None,
        expected_active_job: PrismJobContext | None = None,
        expected_window_started: float | None = None,
        initial_convergence: bool = False,
    ) -> bool:
        acquired = client.job_update_lock.acquire(blocking=not require_idle)
        if not acquired:
            return False
        try:
            if require_idle and prepared_bundle is None:
                prepared_bundle = self.runtime._cached_idle_job_bundle(client)
                if prepared_bundle is None:
                    return False
            return self.retarget_locked(
                client,
                current_difficulty=current_difficulty,
                accepted_shares=accepted_shares,
                submitted_shares=submitted_shares,
                accepted_difficulty=accepted_difficulty,
                elapsed_seconds=elapsed_seconds,
                require_idle=require_idle,
                prepared_bundle=prepared_bundle,
                expected_connection_id=expected_connection_id,
                expected_worker=expected_worker,
                expected_active_job=expected_active_job,
                expected_window_started=expected_window_started,
                initial_convergence=initial_convergence,
            )
        finally:
            client.job_update_lock.release()

    def retarget_locked(
        self,
        client: ClientState,
        *,
        current_difficulty: Decimal,
        accepted_shares: int,
        submitted_shares: int,
        accepted_difficulty: Decimal,
        elapsed_seconds: Decimal,
        require_idle: bool = False,
        prepared_bundle: CachedJobBundle | None = None,
        expected_connection_id: int | None = None,
        expected_worker: WorkerIdentity | None = None,
        expected_active_job: PrismJobContext | None = None,
        expected_window_started: float | None = None,
        prepared_bundle_allow_uncached: bool = False,
        initial_convergence: bool = False,
    ) -> bool:
        config = self.client_config(client)
        if not config.enabled:
            return False
        # Idle step-downs carry no accepted shares, so they can never be the
        # decisive evidence the initial relaxation is granted for.
        with client_vardiff_lock(client):
            initial_convergence = bool(
                initial_convergence
                and not require_idle
                and getattr(
                    client,
                    "vardiff_initial_convergence_pending",
                    True,
                )
            )
        if initial_convergence:
            self._count_initial_retarget_attempt()
        if require_idle:
            if prepared_bundle is None:
                return False
            with client_vardiff_lock(client), self.runtime.lock:
                if expected_connection_id is None:
                    expected_connection_id = client.connection_id
                if expected_worker is None:
                    expected_worker = client.worker
                if expected_active_job is None:
                    expected_active_job = client.active_job
                if expected_window_started is None:
                    expected_window_started = client.vardiff_window_started_monotonic
                if (
                    client not in self.runtime.clients
                    or getattr(client, "closing", False)
                    or not self.runtime.client_can_receive_jobs(client)
                    or client.connection_id != expected_connection_id
                    or client.worker != expected_worker
                    or client.active_job is not expected_active_job
                    or client.vardiff_window_started_monotonic
                    != expected_window_started
                    or client.vardiff_window_accepted != 0
                    or client.vardiff_window_submitted != 0
                ):
                    return False
        observed_difficulty = vardiff.observed_difficulty(
            accepted_difficulty=accepted_difficulty,
            elapsed_seconds=elapsed_seconds,
            target_share_interval_seconds=config.target_share_interval_seconds,
        )
        with client_vardiff_lock(client):
            previous_estimate = client.vardiff_difficulty_estimate
        if observed_difficulty is None:
            difficulty_estimate = None
            with client_vardiff_lock(client):
                client.vardiff_difficulty_estimate = None
        else:
            difficulty_estimate = vardiff.smooth_difficulty_estimate(
                observed=observed_difficulty,
                previous=previous_estimate,
                config=config,
            )
            with client_vardiff_lock(client):
                client.vardiff_difficulty_estimate = difficulty_estimate
        next_difficulty = vardiff.calculate_next_difficulty(
            current_difficulty=current_difficulty,
            accepted_shares=accepted_shares,
            elapsed_seconds=elapsed_seconds,
            config=config,
            accepted_difficulty=accepted_difficulty,
            difficulty_estimate=difficulty_estimate,
            initial_convergence=initial_convergence,
        )
        if not vardiff.should_retarget(
            current_difficulty,
            next_difficulty,
            config.retarget_tolerance,
        ):
            if initial_convergence:
                self._count_initial_retarget_outcome("suppressed")
            return False
        idle_window_state: tuple[float, int, int, Decimal] | None = None
        idle_window_reset_at: float | None = None
        difficulty_superseded = False
        prior_pending: Decimal | None = None
        with client_vardiff_lock(client), self.runtime.lock:
            previous_difficulty = (
                client.pending_share_difficulty or client.share_difficulty
            )
            if previous_difficulty != current_difficulty:
                # An explicit d=/md= request (or another retarget) moved this
                # client while the step was being computed. It keeps its
                # precedence: this retarget yields rather than overriding it.
                difficulty_superseded = True
            else:
                if require_idle and (
                    client not in self.runtime.clients
                    or getattr(client, "closing", False)
                    or not self.runtime.client_can_receive_jobs(client)
                    or client.connection_id != expected_connection_id
                    or client.worker != expected_worker
                    or client.active_job is not expected_active_job
                    or client.vardiff_window_started_monotonic
                    != expected_window_started
                    or client.vardiff_window_accepted != 0
                    or client.vardiff_window_submitted != 0
                ):
                    # A share landed since the idle snapshot; the accept path
                    # owns this window. Abort the speculative step-down rather
                    # than overriding a client that just resumed submitting.
                    return False
                if require_idle:
                    idle_window_state = (
                        client.vardiff_window_started_monotonic,
                        client.vardiff_window_accepted,
                        client.vardiff_window_submitted,
                        client.vardiff_window_work,
                    )
                prior_pending = client.pending_share_difficulty
                client.pending_share_difficulty = next_difficulty
        if difficulty_superseded:
            if initial_convergence:
                self._count_initial_retarget_outcome("superseded")
            return False
        # Advertise the new difficulty only with its corresponding job. Idle
        # retargets stamp an already-cached bundle; normal share-driven
        # retargets retain the existing build path. Either path sends the pair
        # together or restores the prior pending difficulty/window state.
        def idle_commit_guard() -> bool:
            nonlocal idle_window_reset_at
            if not require_idle:
                return True
            # _maybe_send_job_locked holds vardiff_lock before entering the
            # coordinator commit section, so this guard never waits under
            # the coordinator lock.
            if (
                self.runtime.stop_event.is_set()
                or client not in self.runtime.clients
                or getattr(client, "closing", False)
                or not self.runtime.client_can_receive_jobs(client)
                or client.connection_id != expected_connection_id
                or client.worker != expected_worker
                or client.active_job is not expected_active_job
                or client.vardiff_window_started_monotonic
                != expected_window_started
                or client.vardiff_window_accepted != 0
                or client.vardiff_window_submitted != 0
                or client.pending_share_difficulty != next_difficulty
            ):
                return False
            idle_window_reset_at = time.monotonic()
            client.vardiff_window_started_monotonic = idle_window_reset_at
            client.vardiff_window_accepted = 0
            client.vardiff_window_submitted = 0
            client.vardiff_window_work = Decimal("0")
            return True

        def restore_speculative_retarget() -> None:
            with client_vardiff_lock(client), self.runtime.lock:
                if client.pending_share_difficulty == next_difficulty:
                    client.pending_share_difficulty = prior_pending
                self.restore_idle_window_state(
                    client,
                    idle_window_state,
                    idle_window_reset_at,
                )

        # A same-tip retarget only needs to flush in-flight work on a
        # step-down: firmware that applies mining.set_difficulty retroactively
        # would otherwise submit sub-target shares against the old job. On a
        # step-up the old job stays valid at its own stamped share_target, so
        # keeping it avoids discarding the miner's in-flight work.
        clean_jobs = next_difficulty < current_difficulty
        try:
            if require_idle:
                sent = self.runtime._maybe_send_job_locked(
                    client,
                    clean_jobs=clean_jobs,
                    raise_on_build_failure=True,
                    prepared_bundle=prepared_bundle,
                    commit_guard=idle_commit_guard,
                    commit_guard_lock=client_vardiff_lock(client),
                    prepared_bundle_allow_uncached=(
                        prepared_bundle_allow_uncached
                    ),
                )
            else:
                sent = bool(
                    client.authorized
                    and client.subscribed
                    and not self.runtime.stop_event.is_set()
                    and self.runtime.maybe_send_job(client, clean_jobs=clean_jobs)
                )
            # A completed paired send is the commit point. Shutdown may race
            # immediately afterward, but it cannot make already-delivered work
            # speculative again.
            if sent:
                if not require_idle:
                    # Commit point for the fast-arrival relaxation: it is
                    # spent only once a share-driven retarget has actually
                    # reached the miner, so a skipped build or a failed send
                    # cannot consume it silently. Every later retarget on
                    # this connection is back on the ordinary step bound.
                    with client_vardiff_lock(client):
                        client.vardiff_initial_convergence_pending = False
                if initial_convergence:
                    self._count_initial_retarget_outcome("applied")
                self.observe_high_diff_arrival(
                    client,
                    config=config,
                    previous_difficulty=current_difficulty,
                    next_difficulty=next_difficulty,
                )
                # Retain the committed difficulty here, not only on
                # disconnect, so sessions that die without a clean
                # disconnect still resume at their converged value. An idle
                # step-down carries no accepted shares, but it also moves the
                # value, so the store refreshes its TTL on the changed-value
                # branch instead.
                self.record_session_difficulty(
                    client,
                    next_difficulty,
                    share_backed=accepted_shares > 0,
                    previous_difficulty=current_difficulty,
                    initial_convergence=initial_convergence,
                )
                return True
        except Exception:
            # Cached stamping can surface _JobBuildFailed before delivery, and
            # socket errors can surface during the paired send. Both must undo
            # every speculative client mutation before the task reports failure.
            restore_speculative_retarget()
            if initial_convergence:
                self._count_initial_retarget_outcome("failed")
            raise
        restore_speculative_retarget()
        if initial_convergence:
            self._count_initial_retarget_outcome("superseded")
        return False

    @staticmethod
    def restore_idle_window_state(
        client: ClientState,
        idle_window_state: tuple[float, int, int, Decimal] | None,
        idle_window_reset_at: float | None,
    ) -> None:
        """Un-restart the idle vardiff window after a step-down that never
        reached the miner (skipped build/send), so the next sweep can retry
        immediately instead of waiting out another full retarget interval.
        Caller must hold the client's vardiff_lock. No-op unless this retarget did the reset
        and nothing else has restarted the window since."""
        if idle_window_reset_at is None or idle_window_state is None:
            return
        if (
            client.vardiff_window_started_monotonic == idle_window_reset_at
            and client.vardiff_window_accepted == 0
            and client.vardiff_window_submitted == 0
            and client.vardiff_window_work == 0
        ):
            (
                client.vardiff_window_started_monotonic,
                client.vardiff_window_accepted,
                client.vardiff_window_submitted,
                client.vardiff_window_work,
            ) = idle_window_state

    def metrics_lines(self) -> list[str]:
        with self._vardiff_idle_lock:
            sweep = {
                "buckets": dict(self.vardiff_idle_sweep_histogram["buckets"]),
                "sum": float(self.vardiff_idle_sweep_histogram["sum"]),
                "count": int(self.vardiff_idle_sweep_histogram["count"]),
            }
            task = {
                "buckets": dict(self.vardiff_idle_task_histogram["buckets"]),
                "sum": float(self.vardiff_idle_task_histogram["sum"]),
                "count": int(self.vardiff_idle_task_histogram["count"]),
            }
            inspected = self.vardiff_idle_clients_inspected
            skip_counts = dict(self.vardiff_idle_skip_counts)
            queue_depth = self.vardiff_idle_queue_depth
            inflight = self.vardiff_idle_inflight
            failures = self.vardiff_idle_task_failures
        with self._vardiff_convergence_lock:
            initial_attempts = self.vardiff_initial_retarget_attempts
            initial_outcomes = dict(self.vardiff_initial_retarget_outcome_counts)
            durable_writes = dict(self.vardiff_durable_write_outcome_counts)
            durable_pruned = int(self.vardiff_durable_pruned_records)
            durable_prune_failures = int(self.vardiff_durable_prune_failures)
            durable_downward_dropped = int(self.vardiff_durable_downward_dropped)
            durable_backend = (
                "postgres"
                if isinstance(
                    self.worker_difficulty_store,
                    PostgresWorkerDifficultyStore,
                )
                else "memory"
            )
            arrival_seconds = {
                "buckets": dict(
                    self.vardiff_high_diff_arrival_seconds_histogram["buckets"]
                ),
                "sum": float(self.vardiff_high_diff_arrival_seconds_histogram["sum"]),
                "count": int(self.vardiff_high_diff_arrival_seconds_histogram["count"]),
            }
            arrival_shares = {
                "buckets": dict(
                    self.vardiff_high_diff_arrival_shares_histogram["buckets"]
                ),
                "sum": float(self.vardiff_high_diff_arrival_shares_histogram["sum"]),
                "count": int(self.vardiff_high_diff_arrival_shares_histogram["count"]),
            }

        lines = [
            "# HELP qbit_prism_vardiff_idle_clients_inspected_total Clients inspected by bounded vardiff idle sweeps.",
            "# TYPE qbit_prism_vardiff_idle_clients_inspected_total counter",
            f"qbit_prism_vardiff_idle_clients_inspected_total {inspected}",
            "# HELP qbit_prism_vardiff_idle_skips_total Idle retargets skipped by a bounded reason.",
            "# TYPE qbit_prism_vardiff_idle_skips_total counter",
            *[
                f'qbit_prism_vardiff_idle_skips_total{{reason="{reason}"}} {int(skip_counts.get(reason, 0))}'
                for reason in PRISM_VARDIFF_IDLE_SKIP_REASONS
            ],
            "# HELP qbit_prism_vardiff_idle_queue_depth Cache-only idle retarget tasks waiting for a dedicated worker.",
            "# TYPE qbit_prism_vardiff_idle_queue_depth gauge",
            f"qbit_prism_vardiff_idle_queue_depth {queue_depth}",
            "# HELP qbit_prism_vardiff_idle_inflight Cache-only idle retarget tasks currently running.",
            "# TYPE qbit_prism_vardiff_idle_inflight gauge",
            f"qbit_prism_vardiff_idle_inflight {inflight}",
            "# HELP qbit_prism_vardiff_idle_task_failures_total Idle retarget tasks that failed during cached delivery.",
            "# TYPE qbit_prism_vardiff_idle_task_failures_total counter",
            f"qbit_prism_vardiff_idle_task_failures_total {failures}",
            "# HELP qbit_prism_vardiff_initial_retarget_attempts_total Retargets attempted under the fast-arrival initial convergence policy: a connection whose first retarget has not yet committed produced enough accepted shares, over enough elapsed time, to show an observed difficulty far above the one it is stamped at. Equals the sum of qbit_prism_vardiff_initial_retargets_total.",
            "# TYPE qbit_prism_vardiff_initial_retarget_attempts_total counter",
            f"qbit_prism_vardiff_initial_retarget_attempts_total {int(initial_attempts)}",
            "# HELP qbit_prism_vardiff_initial_retargets_total Fast-arrival initial retargets by outcome; only applied reached the miner as a paired difficulty and job, and only applied spends the connection's one initial relaxation.",
            "# TYPE qbit_prism_vardiff_initial_retargets_total counter",
            *[
                f'qbit_prism_vardiff_initial_retargets_total{{outcome="{outcome}"}} {int(initial_outcomes.get(outcome, 0))}'
                for outcome in PRISM_VARDIFF_INITIAL_RETARGET_OUTCOMES
            ],
            "# HELP qbit_prism_vardiff_durable_preloaded Worker difficulties restored into the bounded authorization-time cache during service initialization.",
            "# TYPE qbit_prism_vardiff_durable_preloaded gauge",
            f"qbit_prism_vardiff_durable_preloaded {self.vardiff_durable_preload_records}",
            "# HELP qbit_prism_vardiff_durable_preload_failures_total Durable startup preload failures; mining safely falls back to lane startup difficulty.",
            "# TYPE qbit_prism_vardiff_durable_preload_failures_total counter",
            f"qbit_prism_vardiff_durable_preload_failures_total {self.vardiff_durable_preload_failures}",
            "# HELP qbit_prism_vardiff_durable_pruned_total Worker-difficulty rows deleted as expired, across the startup prune and every periodic TTL prune since.",
            "# TYPE qbit_prism_vardiff_durable_pruned_total counter",
            f"qbit_prism_vardiff_durable_pruned_total {durable_pruned}",
            "# HELP qbit_prism_vardiff_durable_prune_failures_total TTL prune attempts that raised; rows stay until the next interval retries.",
            "# TYPE qbit_prism_vardiff_durable_prune_failures_total counter",
            f"qbit_prism_vardiff_durable_prune_failures_total {durable_prune_failures}",
            "# HELP qbit_prism_vardiff_durable_pending Worker-difficulty writes queued on the bounded persistence lane right now.",
            "# TYPE qbit_prism_vardiff_durable_pending gauge",
            f"qbit_prism_vardiff_durable_pending {self.durable_pending_depth()}",
            "# HELP qbit_prism_vardiff_durable_coalesced_total Queued writes absorbed into an existing entry for the same worker; the lane is bounded by distinct workers, not by event rate.",
            "# TYPE qbit_prism_vardiff_durable_coalesced_total counter",
            f"qbit_prism_vardiff_durable_coalesced_total {self.durable_coalesced_count()}",
            "# HELP qbit_prism_vardiff_durable_downward_dropped_total Safe step-down corrections the lane could not persist. Non-zero means a reconnect may resume a difficulty the live session had abandoned, until its TTL expires; alert on it.",
            "# TYPE qbit_prism_vardiff_durable_downward_dropped_total counter",
            f"qbit_prism_vardiff_durable_downward_dropped_total {durable_downward_dropped}",
            "# HELP qbit_prism_vardiff_durable_backend Active worker-difficulty retention backend.",
            "# TYPE qbit_prism_vardiff_durable_backend gauge",
            f'qbit_prism_vardiff_durable_backend{{backend="{durable_backend}"}} 1',
            "# HELP qbit_prism_vardiff_durable_writes_total Share-backed worker-difficulty persistence attempts by bounded outcome.",
            "# TYPE qbit_prism_vardiff_durable_writes_total counter",
            *[
                f'qbit_prism_vardiff_durable_writes_total{{outcome="{outcome}"}} {int(durable_writes.get(outcome, 0))}'
                for outcome in PRISM_VARDIFF_DURABLE_WRITE_OUTCOMES
            ],
        ]
        lines.extend(
            _histogram_lines(
                "qbit_prism_vardiff_high_diff_arrival_seconds",
                "Seconds from the start of a session's vardiff accounting until a retarget first carried it to the configured high-difficulty arrival threshold. Observed once per connection, and only for sessions that started below the threshold, so a lane already starting above it never contributes.",
                arrival_seconds,
                PRISM_VARDIFF_HIGH_DIFF_ARRIVAL_SECONDS_BUCKETS,
            )
        )
        lines.extend(
            _histogram_lines(
                "qbit_prism_vardiff_high_diff_arrival_shares",
                "Accepted shares a session produced before a retarget first carried it to the configured high-difficulty arrival threshold. Shares credited at the pre-arrival stamped target are exactly the under-credited ones, so this is the share-denominated cost of the climb.",
                arrival_shares,
                PRISM_VARDIFF_HIGH_DIFF_ARRIVAL_SHARES_BUCKETS,
            )
        )
        for metric_name, description, histogram in (
            (
                "qbit_prism_vardiff_idle_sweep_seconds",
                "Wall time of one bounded vardiff idle sweep.",
                sweep,
            ),
            (
                "qbit_prism_vardiff_idle_retarget_task_seconds",
                "Queue plus execution latency for cache-only idle retarget tasks.",
                task,
            ),
        ):
            lines.extend(
                _histogram_lines(
                    metric_name,
                    description,
                    histogram,
                    PRISM_VARDIFF_IDLE_SECONDS_BUCKETS,
                )
            )
        return lines
