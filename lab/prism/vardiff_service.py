"""PRISM vardiff windows, retarget delivery, and bounded idle work."""

from __future__ import annotations

from collections import OrderedDict
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from decimal import Decimal
import threading
import time
import traceback
from typing import Any, Callable, Protocol

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


PRISM_VARDIFF_IDLE_RETARGET_MAX_WORKERS = 2
MAX_PENDING_VARDIFF_IDLE_RETARGETS = 8
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


def _new_histogram() -> dict[str, Any]:
    return {
        "buckets": {bucket: 0 for bucket in PRISM_VARDIFF_IDLE_SECONDS_BUCKETS},
        "sum": 0.0,
        "count": 0,
    }


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
            config = (
                client.vardiff_config
                or client.listener_vardiff_config
                or self.runtime.vardiff_config
            )
            if not config.enabled:
                return
            client.vardiff_window_accepted += 1
            client.vardiff_window_work += share_difficulty
            elapsed_seconds = Decimal(
                str(max(0.001, now - client.vardiff_window_started_monotonic))
            )
            if elapsed_seconds < config.retarget_interval_seconds:
                return
            accepted_shares = client.vardiff_window_accepted
            submitted_shares = client.vardiff_window_submitted
            accepted_difficulty = client.vardiff_window_work
            current_difficulty = (
                client.pending_share_difficulty or client.share_difficulty
            )
            client.vardiff_window_started_monotonic = now
            client.vardiff_window_accepted = 0
            client.vardiff_window_submitted = 0
            client.vardiff_window_work = Decimal("0")
        self.runtime.retarget_client(
            client,
            current_difficulty=current_difficulty,
            accepted_shares=accepted_shares,
            submitted_shares=submitted_shares,
            accepted_difficulty=accepted_difficulty,
            elapsed_seconds=elapsed_seconds,
        )

    def _count_resume_outcome(self, outcome: str) -> None:
        if outcome not in PRISM_VARDIFF_RESUME_OUTCOMES:
            raise ValueError(f"unknown vardiff resume outcome: {outcome}")
        with self._vardiff_convergence_lock:
            self.vardiff_resume_outcome_counts[outcome] += 1

    def record_session_difficulty(
        self,
        client: ClientState,
        difficulty: Decimal | None = None,
        *,
        share_backed: bool,
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
        shares; the store uses it to decide whether re-recording an unchanged
        value may refresh its TTL.
        """
        key = session_difficulty_key(client)
        if key is None:
            return
        if difficulty is None:
            with client_vardiff_lock(client):
                difficulty = client.share_difficulty
        self.session_difficulty_store.record(
            key,
            difficulty,
            now=time.monotonic(),
            share_backed=share_backed,
        )

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
    ) -> bool:
        config = self.client_config(client)
        if not config.enabled:
            return False
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
        )
        if not vardiff.should_retarget(
            current_difficulty,
            next_difficulty,
            config.retarget_tolerance,
        ):
            return False
        idle_window_state: tuple[float, int, int, Decimal] | None = None
        idle_window_reset_at: float | None = None
        with client_vardiff_lock(client), self.runtime.lock:
            previous_difficulty = (
                client.pending_share_difficulty or client.share_difficulty
            )
            if previous_difficulty != current_difficulty:
                return False
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
                # A share landed since the idle snapshot; the accept path owns
                # this window. Abort the speculative step-down rather than
                # overriding a client that just resumed submitting.
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
                )
                return True
        except Exception:
            # Cached stamping can surface _JobBuildFailed before delivery, and
            # socket errors can surface during the paired send. Both must undo
            # every speculative client mutation before the task reports failure.
            restore_speculative_retarget()
            raise
        restore_speculative_retarget()
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
        ]
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
                [
                    f"# HELP {metric_name} {description}",
                    f"# TYPE {metric_name} histogram",
                    *[
                        f'{metric_name}_bucket{{le="{bucket:g}"}} {histogram["buckets"].get(bucket, 0)}'
                        for bucket in PRISM_VARDIFF_IDLE_SECONDS_BUCKETS
                    ],
                    f'{metric_name}_bucket{{le="+Inf"}} {histogram["count"]}',
                    f'{metric_name}_sum {float(histogram["sum"]):.6f}',
                    f'{metric_name}_count {histogram["count"]}',
                ]
            )
        return lines
