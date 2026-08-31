"""Named lifecycle registry for PRISM process-level background loops.

The registry deliberately has no ``start_all`` operation.  The coordinator
starts named services at the existing recovery boundaries, while this module
owns the start-once state and the exact thread handles used during shutdown.

This module also owns :class:`LedgerLeaseHeartbeatService`, the sole owner of
the ledger writer-lease heartbeat/monitor threads, their freshness state, and
the synchronous exact-session external-effect fence.  Because its startup is
conditional, creates two coupled threads, and waits for the first proof, it is
started directly at the serve boundary rather than modeled as two independent
registry specifications.

:class:`WatchdogService` owns watchdog policy: the coordination-blocked
streak, coordination-versus-publication classification, the liveness and
fatal-stop decisions, failure detail/diagnostics, and the bounded fresh-thread
shutdown/lease-release attempt followed by an unconditional process exit.
Generic liveness heartbeat registration/record/pause maps stay coordinator
runtime state and reach the watchdog through ports.  Both WatchdogService.run
and the lease service trigger the dynamic coordinator ``_watchdog_hard_exit``
seam so per-instance monkeypatches intercept every exit path; the coordinator
delegate routes back into :meth:`WatchdogService.hard_exit`.
"""

from __future__ import annotations

from dataclasses import dataclass
import inspect
import queue
import threading
import time
from typing import Any, Callable, Iterable

from lab.prism.coordinator_shutdown import ShutdownInProgress
from lab.prism.share_ledger import (
    DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS,
    WRITER_LEASE_VERIFICATION_MAX_STATEMENTS,
    WriterLeaseRenewalDeferred,
)
from lab.prism.writer_lease_timing import (
    WRITER_LEASE_HEARTBEAT_SCHEDULER_SLACK_SECONDS,
    LEASE_HEARTBEAT_MODE_FENCE,
    LEASE_HEARTBEAT_MODE_PROOF,
    LEASE_HEARTBEAT_MODE_RENEW,
    LEASE_HEARTBEAT_MODES,
    LEASE_HEARTBEAT_OUTCOME_DEFERRED,
    LEASE_HEARTBEAT_OUTCOME_FAILED,
    LEASE_HEARTBEAT_OUTCOME_PROVEN,
    LEASE_HEARTBEAT_OUTCOME_RENEWAL_DUE,
    LEASE_HEARTBEAT_OUTCOME_RENEWED,
    LEASE_HEARTBEAT_OUTCOMES,
    UNATTRIBUTED_PHASES,
    WriterLeaseHeartbeatPolicy,
    WriterLeaseVerificationAttempt,
    WriterLeaseVerificationPhases,
)


@dataclass(frozen=True, slots=True)
class BackgroundServiceSpec:
    """Immutable construction and shutdown policy for one background loop."""

    name: str
    thread_name: str
    target: Callable[[], None]
    daemon: bool
    join_timeout: float
    watchdog_monitored: bool
    registration_identity: object | None = None

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("background service name must not be empty")
        if not self.thread_name:
            raise ValueError("background service thread name must not be empty")
        if self.join_timeout < 0:
            raise ValueError("background service join timeout must be nonnegative")


@dataclass(frozen=True, slots=True)
class BackgroundServiceSnapshot:
    """Read-only lifecycle state returned without exposing registry records."""

    specification: BackgroundServiceSpec
    started: bool
    thread: threading.Thread | None


@dataclass(slots=True)
class _BackgroundServiceRecord:
    specification: BackgroundServiceSpec
    started: bool = False
    thread: threading.Thread | None = None
    start_hook_completed: bool = False


ThreadFactory = Callable[..., threading.Thread]


class BackgroundServiceRegistry:
    """Start named process services once and retain their drain handles."""

    def __init__(
        self,
        specifications: Iterable[BackgroundServiceSpec] = (),
        *,
        thread_factory: ThreadFactory = threading.Thread,
    ) -> None:
        self._lock = threading.Lock()
        self._thread_factory = thread_factory
        self._records: dict[str, _BackgroundServiceRecord] = {}
        self._thread_names: set[str] = set()
        for specification in specifications:
            self.register(specification)

    def register(self, specification: BackgroundServiceSpec) -> None:
        """Register one service without starting it."""
        with self._lock:
            self._register_locked(specification)

    def register_if_absent(self, specification: BackgroundServiceSpec) -> bool:
        """Atomically install an equivalent dynamic service at most once.

        Returns true when this call registered the service. Concurrent callers
        describing the same lifecycle and registration identity receive false;
        a conflicting reuse of either name still fails explicitly.
        """
        with self._lock:
            existing = self._records.get(specification.name)
            if existing is not None:
                if self._equivalent(existing.specification, specification):
                    return False
                raise ValueError(
                    "incompatible background service registration for name: "
                    f"{specification.name}"
                )
            self._register_locked(specification)
            return True

    def _register_locked(self, specification: BackgroundServiceSpec) -> None:
        if specification.name in self._records:
            raise ValueError(
                f"background service is already registered: {specification.name}"
            )
        if specification.thread_name in self._thread_names:
            raise ValueError(
                "background service thread name is already registered: "
                f"{specification.thread_name}"
            )
        self._records[specification.name] = _BackgroundServiceRecord(
            specification=specification
        )
        self._thread_names.add(specification.thread_name)

    @staticmethod
    def _equivalent(
        existing: BackgroundServiceSpec,
        candidate: BackgroundServiceSpec,
    ) -> bool:
        target_equivalent = (
            existing.target is candidate.target
            if existing.registration_identity is None
            and candidate.registration_identity is None
            else existing.registration_identity == candidate.registration_identity
            and existing.registration_identity is not None
            and candidate.registration_identity is not None
        )
        return bool(
            target_equivalent
            and existing.name == candidate.name
            and existing.thread_name == candidate.thread_name
            and existing.daemon == candidate.daemon
            and existing.join_timeout == candidate.join_timeout
            and existing.watchdog_monitored == candidate.watchdog_monitored
        )

    def contains(self, name: str) -> bool:
        with self._lock:
            return name in self._records

    def service_names(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(self._records)

    def snapshot(self, name: str) -> BackgroundServiceSnapshot:
        with self._lock:
            record = self._records[name]
            return BackgroundServiceSnapshot(
                specification=record.specification,
                started=record.started,
                thread=record.thread,
            )

    def start(
        self,
        name: str,
        *,
        on_started: Callable[[BackgroundServiceSpec], None] | None = None,
    ) -> threading.Thread:
        """Start a named service once, returning the same thread thereafter."""
        with self._lock:
            record = self._records[name]
            if record.thread is not None:
                if not record.start_hook_completed and on_started is not None:
                    on_started(record.specification)
                    record.start_hook_completed = True
                return record.thread
            specification = record.specification
            thread = self._thread_factory(
                target=specification.target,
                name=specification.thread_name,
                daemon=specification.daemon,
            )
            thread.start()
            record.thread = thread
            record.started = True
            if on_started is None:
                record.start_hook_completed = True
            else:
                # The live thread remains registered for shutdown if this
                # nonblocking side-effect fails. A later start call retries
                # only the hook and never creates a second worker.
                on_started(specification)
                record.start_hook_completed = True
            return thread

    def threads_to_drain(self) -> tuple[tuple[threading.Thread, float], ...]:
        """Return started threads in stable registration/shutdown order."""
        with self._lock:
            return tuple(
                (record.thread, record.specification.join_timeout)
                for record in self._records.values()
                if record.started and record.thread is not None
            )

    def watchdog_service_names(self, *, started_only: bool = False) -> tuple[str, ...]:
        """Derive watchdog keys from the same records used to start loops."""
        with self._lock:
            return tuple(
                record.specification.name
                for record in self._records.values()
                if record.specification.watchdog_monitored
                and (record.started or not started_only)
            )


@dataclass(frozen=True, slots=True)
class WatchdogPorts:
    """Dynamic process-supervision capabilities used by the watchdog owner.

    ``publication_state``, ``hard_exit``, ``overdue_heartbeats``, and
    ``publication_failure_expired`` resolve coordinator methods at call time
    so per-instance monkeypatches intercept them; the interval/budget/enabled
    reads stay live per iteration for focused timing overrides.
    """

    wait_for_stop: Callable[[float], bool]
    interval_seconds: Callable[[], float]
    fatal_exit_requested: Callable[[], bool]
    publication_state: Callable[[float], tuple[str | None, float, float, float]]
    hard_exit: Callable[[str], None]
    liveness_enabled: Callable[[], bool]
    overdue_heartbeats: Callable[[float], list[str]]
    liveness_timeout_seconds: Callable[[], float]
    coordination_budget_seconds: Callable[[], float]
    publication_budget_seconds: Callable[[], float]
    ensure_job_cache_state: Callable[[], None]
    publication_failure_expired: Callable[[float], bool]
    publication_divergence_since: Callable[[], float | None]
    lease_release_timeout_seconds: Callable[[], float]
    shutdown_controller: Callable[[], Any]
    request_shutdown: Callable[[], None]
    release_ledger_lease: Callable[[float], bool]
    lease_failure_reason: Callable[[], str | None]
    exit_process: Callable[[int], None]
    log: Callable[[str], None]


class WatchdogService:
    """Own watchdog classification, streak state, and bounded emergency exit.

    Progress-health continues to own the publication-divergence timestamps;
    the classification reads a locked snapshot through a port and rechecks it
    while claiming an exit.  Lock order is the service's coordination lock,
    then the progress-health lock — progress-health must never call the
    watchdog while holding its own lock.
    """

    def __init__(self, ports: WatchdogPorts) -> None:
        self._ports = ports
        self._coordination_blocked_lock = threading.Lock()
        self._coordination_blocked_since_monotonic: float | None = None
        self._publication_watchdog_exit_claimed = False
        self.failure_detail: str | None = None

    def record_coordination_blocked_refresh(self, now: float) -> None:
        with self._coordination_blocked_lock:
            if self._publication_watchdog_exit_claimed:
                return
            if self._coordination_blocked_since_monotonic is None:
                self._coordination_blocked_since_monotonic = now

    def clear_coordination_blocked_streak(self) -> None:
        with self._coordination_blocked_lock:
            # Once the watchdog claims an expired generation, its hard exit is
            # the terminal action. Otherwise a refresh completing between the
            # deadline check and os._exit could appear to cancel an exit that
            # already won the streak lock.
            if not self._publication_watchdog_exit_claimed:
                self._coordination_blocked_since_monotonic = None

    def coordination_blocked_streak_age_seconds(
        self,
        now: float | None = None,
    ) -> float:
        now = time.monotonic() if now is None else now
        with self._coordination_blocked_lock:
            started = self._coordination_blocked_since_monotonic
        return 0.0 if started is None else max(0.0, now - started)

    def coordination_blocked_streak_expired(self, now: float) -> bool:
        budget = float(self._ports.coordination_budget_seconds())
        with self._coordination_blocked_lock:
            started = self._coordination_blocked_since_monotonic
        return bool(
            started is not None
            and (budget <= 0 or now - started >= budget)
        )

    def publication_watchdog_state(
        self,
        now: float,
    ) -> tuple[str | None, float, float, float]:
        """Arbitrate coordination and ordinary publication deadlines.

        The final decision is serialized with coordination streak changes.
        A streak recorded while the ordinary publication check is in flight
        therefore owns the longer coordination deadline; once either deadline
        is claimed, later refresh results cannot cancel the terminal exit.
        """
        coordination_budget = float(self._ports.coordination_budget_seconds())
        publication_budget = float(self._ports.publication_budget_seconds())
        self._ports.ensure_job_cache_state()

        # This preflight keeps the common healthy case cheap. Its result is
        # revalidated under both state locks before an ordinary exit is
        # claimed, so a concurrent publication cannot leave a stale verdict.
        publication_expired = self._ports.publication_failure_expired(now)
        with self._coordination_blocked_lock:
            started = self._coordination_blocked_since_monotonic
            if started is not None:
                age = max(0.0, now - started)
                if coordination_budget <= 0 or age >= coordination_budget:
                    self._publication_watchdog_exit_claimed = True
                    return (
                        "coordination",
                        age,
                        coordination_budget,
                        publication_budget,
                    )
                return None, age, coordination_budget, publication_budget

            if publication_expired:
                # Recheck the divergence timestamp under the progress-health
                # lock (inside the port) while the coordination lock is held.
                divergence_since = self._ports.publication_divergence_since()
                publication_expired = bool(
                    publication_budget > 0
                    and divergence_since is not None
                    and now - divergence_since >= publication_budget
                )
                if publication_expired:
                    self._publication_watchdog_exit_claimed = True
                    return (
                        "publication",
                        0.0,
                        coordination_budget,
                        publication_budget,
                    )
            return None, 0.0, coordination_budget, publication_budget

    def run(self) -> None:
        while True:
            if self._ports.wait_for_stop(self._ports.interval_seconds()):
                if self._ports.fatal_exit_requested():
                    self._ports.log(
                        "prism coordinator: fatal block-work restart requested; "
                        "exiting non-zero even if the main thread is blocked"
                    )
                    self._ports.exit_process(1)
                return
            now = time.monotonic()
            (
                publication_failure,
                coordination_age,
                coordination_budget,
                publication_budget,
            ) = self._ports.publication_state(now)
            if publication_failure == "coordination":
                self.failure_detail = (
                    "prism coordinator: publication-progress watchdog firing; "
                    "template refresh remained coordination-blocked past the "
                    f"coordination budget={coordination_budget:g}s "
                    f"streak_age={coordination_age:.3f}s"
                )
                self._ports.hard_exit("coordination")
            if publication_failure == "publication":
                self.failure_detail = (
                    "prism coordinator: publication-progress watchdog firing; "
                    "current tip/generation remained unpublished past the "
                    f"template refresh failure budget={publication_budget:g}s"
                )
                self._ports.hard_exit("publication")
            overdue = (
                self._ports.overdue_heartbeats(now)
                if self._ports.liveness_enabled()
                else []
            )
            if overdue:
                self.failure_detail = (
                    "prism coordinator: liveness watchdog firing; unresponsive "
                    f"subsystems={overdue} "
                    f"timeout={self._ports.liveness_timeout_seconds():g}s"
                )
                # Queued shares have not been acknowledged. Miners reconnect
                # and retry them after restart; exact-payload replay is
                # idempotent if Postgres committed just before this exit.
                self._ports.hard_exit("liveness")

    def hard_exit(
        self,
        reason: str,
        *,
        timeout_seconds: float | None = None,
    ) -> None:
        """Bound a fresh-thread lease release, then terminate unconditionally."""
        try:
            if timeout_seconds is None:
                timeout_seconds = float(
                    self._ports.lease_release_timeout_seconds()
                )
            deadline = time.monotonic() + max(0.0, timeout_seconds)
            release_thread = threading.Thread(
                target=self._release_ledger_lease,
                args=(reason, deadline),
                name="prism-watchdog-lease-release",
                daemon=True,
            )
            release_thread.start()
            release_thread.join(max(0.0, deadline - time.monotonic()))
        finally:
            # Nothing, including timeout logging or thread-start failure, may
            # extend or suppress the watchdog's terminal action. Exiting also
            # discards the in-memory vardiff retention store (see
            # vardiff_service.SessionDifficultyStore), so the post-restart
            # reconnect wave is not smoothed by difficulty resume.
            self._ports.exit_process(1)

    def _release_ledger_lease(self, reason: str, deadline: float) -> bool:
        """Use only the shutdown controller and a fresh DB connection.

        Do not call ``shutdown`` here: cancellation takes the coordinator
        control-plane lock, which may belong to the subsystem that triggered
        the watchdog. Closing writer admission plus the controller's
        tracked-writer barrier retains the graceful path's release-withheld
        invariant without that lock.

        Any best-effort diagnostic is emitted only after lease handling on this
        daemon worker. A blocked container log pipe may park the worker, but it
        cannot precede the release attempt or extend the caller's hard deadline.
        """
        controller = self._ports.shutdown_controller()
        self._ports.request_shutdown()
        if not controller.begin_shutdown(f"watchdog_{reason}"):
            handled = controller.wait_for_lease_handling()
            self._exit_diagnostic(reason, lease_handled=handled)
            return handled

        quiesced, _elapsed, _blockers = controller.wait_for_writer_quiescence(
            max(0.0, deadline - time.monotonic())
        )
        if not quiesced:
            self._exit_diagnostic(reason, lease_handled=False)
            return False
        if time.monotonic() >= deadline:
            self._exit_diagnostic(reason, lease_handled=False)
            return False
        handled = self._ports.release_ledger_lease(deadline)
        self._exit_diagnostic(reason, lease_handled=handled)
        return handled

    def _exit_diagnostic(self, reason: str, *, lease_handled: bool) -> None:
        """Best-effort logging after the watchdog's safety-critical work."""
        detail = (
            self._ports.lease_failure_reason()
            if reason == "lease_heartbeat"
            else self.failure_detail
        )
        try:
            self._ports.log(
                (detail or f"prism coordinator: {reason} watchdog firing")
                + ". Exiting non-zero so the restart policy recovers the process. "
                + f"lease_handled={lease_handled}"
            )
        except Exception:
            pass


def guard_session_verifier(ledger: object) -> Callable[..., object] | None:
    """Return the guarded-session liveness check for ``ledger``.

    Prefer ``verify_writer_lease_guard_session``: it proves the dedicated
    guard session is live and renews the lease TTL only via SKIP LOCKED,
    so it can never queue behind the row lock a long fenced transaction
    (``persist_accepted_block``) holds until commit and then die on the
    guard's statement timeout, yet an idle coordinator still keeps its
    lease from expiring under different-identity claimants.
    ``renew_writer_lease_heartbeat`` is the legacy embedder spelling and
    does wait on the lease tuple; it is only a fallback for ledgers that
    predate the non-blocking verification.
    """
    verify = getattr(ledger, "verify_writer_lease_guard_session", None)
    if verify is not None:
        return verify
    return getattr(ledger, "renew_writer_lease_heartbeat", None)


def guard_session_prover(ledger: object) -> Callable[..., object] | None:
    """Return the cheap read-only ownership proof for ``ledger``, if any.

    ``prove_writer_lease_guard_session`` answers only "does this backend
    still hold the advisory guard, and does the committed lease row still
    name this exact session?" on one non-blocking statement.  It is the
    frequent half of the issue #212 split: the heartbeat needs ownership
    several times per adoption-silence window, but the lease TTL only needs
    a writer-side refresh well before it lapses, and paying for renewal
    (a ``SKIP LOCKED`` row lock, a ``pg_stat_activity`` locker attribution,
    and sometimes a second statement) on every beat is what pushed the
    frequent statement toward the guard's 500ms statement timeout during a
    rapid-block burst.

    A ledger that does not offer it — an embedder's, or a test fake —
    simply keeps running the full verification on every beat, which is the
    behavior that predates the split.
    """
    return getattr(ledger, "prove_writer_lease_guard_session", None)


def verifier_callbacks(verify: Callable[..., object]) -> frozenset[str]:
    """Which progress callbacks ``verify`` accepts.

    Resolved once per loop rather than per beat: a ledger's verification
    signature cannot change under a running heartbeat, and
    ``inspect.signature`` is far too expensive for a 0.25s cadence.
    """
    try:
        parameters = inspect.signature(verify).parameters
    except (TypeError, ValueError):
        return frozenset()
    return frozenset(
        name
        for name in (
            "on_query_start",
            "on_statement_progress",
            "on_statement_end",
        )
        if name in parameters
    )


def _default_monotonic() -> float:
    """Read ``time.monotonic`` at call time, not at class-definition time.

    The lease lifecycle's documented test seam is patching ``monotonic`` on
    the shared ``time`` module object. A dataclass default captured when this
    module is imported would keep reading the real clock behind such a patch,
    which is exactly the kind of silent divergence between test and
    production that the phase attribution exists to prevent.
    """
    return time.monotonic()


@dataclass(frozen=True)
class LedgerLeaseHeartbeatPorts:
    """Call-time-resolved coordinator seams for the lease heartbeat owner.

    Every callable resolves live coordinator state (ledger identity, test
    timing overrides, patched hard-exit methods) when invoked, preserving the
    documented monkeypatch seams.  ``lease_hard_exit`` routes through the
    coordinator's ``_ledger_lease_heartbeat_hard_exit`` delegate so instance
    patches intercept every internal failure path; the real exit body then
    reaches process termination only through the dynamic coordinator
    ``_watchdog_hard_exit`` port (PR 80 redirects that seam).
    """

    ledger: Callable[[], object]
    heartbeat_seconds: Callable[[], float]
    failure_seconds: Callable[[], float]
    monitor_seconds: Callable[[], float]
    exit_timeout_seconds: Callable[[], float]
    external_fence_timeout_seconds: Callable[[], float]
    lease_hard_exit: Callable[..., None]
    watchdog_hard_exit: Callable[..., None]
    heartbeat_loop: Callable[[], None]
    monitor_loop: Callable[[], None]
    # Every interval this service measures about itself reads this clock, so
    # a deterministic test can drive the whole envelope — idle waits, phase
    # attribution, staleness, the monitor's own lateness — by advancing a
    # virtual clock instead of sleeping and hoping. Production passes
    # nothing and gets time.monotonic.
    monotonic: Callable[[], float] = _default_monotonic
    # The process-side scheduling allowance, resolved like every other
    # timing term rather than read from a module constant: it appears on
    # both sides of the safety inequality (the heartbeat's stamping path
    # and the monitor's own poll), so a deployment or a scaled-down test
    # that changes it must change what the policy validates.
    scheduler_slack_seconds: Callable[[], float] = (
        lambda: WRITER_LEASE_HEARTBEAT_SCHEDULER_SLACK_SECONDS
    )


class LedgerLeaseHeartbeatService:
    """Sole owner of ledger-lease heartbeat state, threads, and fencing.

    Owns the success/progress/server-proven timestamps and their timing
    envelope, guarded-session verifier selection, the heartbeat and monitor
    threads, startup readiness/fail-closed state, the stop event, the failure
    reason, the exit-thread exemption, and the synchronous exact-session
    external-effect fence.  It owns no writer-admission or lease-release-claim
    state; that remains with ``CoordinatorShutdownController``.
    """

    def __init__(self, ports: LedgerLeaseHeartbeatPorts) -> None:
        self._ports = ports
        self.freshness_lock = threading.Lock()
        self.failure_lock = threading.Lock()
        self.last_success_monotonic: float | None = None
        self.last_progress_monotonic: float | None = None
        self.last_server_proven_monotonic: float | None = None
        self.failed: threading.Event | None = None
        self.ready: threading.Event | None = None
        self.stop_event: threading.Event | None = None
        self.thread: threading.Thread | None = None
        self.monitor_thread: threading.Thread | None = None
        self.exit_thread: threading.Thread | None = None
        self.failure_reason: str | None = None
        self.failure_has_traceback = False
        # Phase attribution (issue #212). Written by whichever thread ran the
        # attempt and published as one immutable snapshot, so the monitor can
        # quote the last attempt in a hard-exit reason without locking.
        self.last_phases: WriterLeaseVerificationPhases = UNATTRIBUTED_PHASES
        self.worst_phases: WriterLeaseVerificationPhases = UNATTRIBUTED_PHASES
        self.attempt_counts: dict[str, int] = {
            mode: 0 for mode in LEASE_HEARTBEAT_MODES
        }
        self.outcome_counts: dict[str, int] = {
            outcome: 0 for outcome in LEASE_HEARTBEAT_OUTCOMES
        }
        # The monitor's own lateness. It decides the hard exit, so a stalled
        # monitor thread consumes envelope exactly like a stalled heartbeat
        # thread and must be attributable separately from database latency.
        self.monitor_wake_delay_seconds = 0.0

    def record_success(self, renewal_started_monotonic: float) -> None:
        """Advance the conservative freshness edge without regression."""
        with self.freshness_lock:
            last_success = self.last_success_monotonic
            if (
                last_success is None
                or renewal_started_monotonic > last_success
            ):
                self.last_success_monotonic = renewal_started_monotonic

    def record_phases(self, phases: WriterLeaseVerificationPhases) -> None:
        """Publish one attempt's phase breakdown for exits and metrics."""
        self.last_phases = phases
        if phases.total_seconds >= self.worst_phases.total_seconds:
            self.worst_phases = phases
        counts = self.attempt_counts
        if phases.mode in counts:
            counts[phases.mode] += 1
        outcomes = self.outcome_counts
        if phases.outcome in outcomes:
            outcomes[phases.outcome] += 1

    def record_progress(self) -> None:
        """Stamp mid-verification progress for the staleness monitor only.

        Fires from inside verify_writer_lease_guard_session when its
        attribution recheck runs a second statement: the first statement's
        round trip completed, proving the heartbeat machinery is making
        progress. Deliberately separate from the success timestamp — the
        verification has not yet reached a verdict, so nothing that treats
        success as authorization freshness may observe this mark. Only the
        heartbeat monitor reads it, so a lawful two-statement verification
        is not mistaken for a wedged heartbeat while a genuinely stuck
        statement still ages toward the failure budget unimpeded.
        """
        self.last_progress_monotonic = self._ports.monotonic()

    def record_server_proven(
        self,
        proven_monotonic: float | None = None,
    ) -> None:
        """Stamp a completed server round trip: progress plus the envelope edge.

        Client-side progress marks (attempt start, query-slot acquisition)
        keep a lawfully slow verification alive under the monitor, but they
        can postdate a silent guard-session death by up to one idle
        interval, so they cannot carry the adoption-envelope guarantee
        (hard exit completes before a replacement's silence window can
        elapse). Only a response actually received from PostgreSQL — a
        completed statement round trip or a whole verification — proves
        the session was alive approximately now, so only these sites stamp
        the server-proven edge the monitor's envelope cap measures from.

        ``proven_monotonic`` is the *send* edge of the statement whose
        response just arrived: a response proves the session was alive at
        some instant between the statement leaving this process and the
        answer arriving, and the conservative end of that interval is when
        it left. Stamping receipt time instead would let scheduler delay
        between the response and this call make the session look fresher
        than PostgreSQL proved, spending adoption envelope that the exit
        ordering argument budgets for the monitor's own poll. Omitting the
        argument keeps the old receipt-time behavior for callers that
        cannot observe the send edge.

        The stamp never regresses: the heartbeat and a concurrent external
        fence both publish here, and an older send edge arriving second
        must not undo a newer proof.
        """
        now = self._ports.monotonic()
        self.last_progress_monotonic = now
        proven = now if proven_monotonic is None else float(proven_monotonic)
        with self.freshness_lock:
            last_proven = self.last_server_proven_monotonic
            if last_proven is None or proven > last_proven:
                self.last_server_proven_monotonic = proven

    def adoption_silence_seconds(self) -> float:
        """Resolve the ledger's adoption-silence window for timing checks.

        PsqlShareLedger keeps the value on a private attribute with no
        public alias; read both spellings so operator overrides actually
        reach the misconfiguration warning and the monitor's envelope cap
        instead of silently comparing against the compiled default.
        """
        ledger = self._ports.ledger()
        for name in (
            "lease_adoption_silence_seconds",
            "_lease_adoption_silence_seconds",
        ):
            value = getattr(ledger, name, None)
            if value:
                return float(value)
        return float(DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS)

    def activity_age_seconds(self) -> float:
        """Age of the newest monitor-visible heartbeat activity mark.

        The success timestamp is deliberately the call-start edge (authority
        freshness must never look newer than what PostgreSQL proved), so it
        alone cannot answer "is the heartbeat advancing?": back-to-back
        verifications that each lawfully approach the statement budget age
        the freshest success edge by two call durations plus the idle
        interval, and a budget sized for one statement would hard-exit a
        healthy sole writer. Staleness enforcers therefore measure silence
        from the newest of the success edge and the progress mark, which
        carries attempt starts, query-slot acquisition, statement round
        trips, and completions. A verification that stops advancing stops
        stamping, so a wedged statement still ages out on the same budget.
        """
        last_success = float(
            self.last_success_monotonic
            if self.last_success_monotonic is not None
            else self._ports.monotonic()
        )
        last_progress = float(
            self.last_progress_monotonic
            if self.last_progress_monotonic is not None
            else last_success
        )
        return max(0.0, self._ports.monotonic() - max(last_success, last_progress))

    def server_proven_age_seconds(self) -> float:
        """Age of the newest completed guard round trip.

        The envelope edge: unlike the client-side activity marks this can
        never postdate a silent guard-session death, so it is what the
        monitor's adoption cap measures from.
        """
        last_server_proven = float(
            self.last_server_proven_monotonic
            if self.last_server_proven_monotonic is not None
            else self._ports.monotonic()
        )
        return max(0.0, self._ports.monotonic() - last_server_proven)

    def policy(self) -> WriterLeaseHeartbeatPolicy:
        """Resolve the five coupled timings as one validated policy.

        Built from live ports and the ledger's own adoption silence, so an
        operator override reaches the same inequality the compiled defaults
        satisfy rather than being compared against a stale constant.
        """
        return WriterLeaseHeartbeatPolicy(
            adoption_silence_seconds=self.adoption_silence_seconds(),
            heartbeat_interval_seconds=float(self._ports.heartbeat_seconds()),
            failure_budget_seconds=float(self._ports.failure_seconds()),
            monitor_interval_seconds=float(self._ports.monitor_seconds()),
            exit_margin_seconds=float(self._ports.exit_timeout_seconds()),
            scheduler_slack_seconds=float(
                self._ports.scheduler_slack_seconds()
            ),
        )

    def snapshot(self) -> dict[str, object]:
        """Read-only heartbeat attribution for the metrics surface.

        Every key is fixed and every nested map has a fixed key set, so the
        rendered metric cardinality cannot grow with traffic.
        """
        policy = self.policy()
        last = self.last_phases
        worst = self.worst_phases
        thread = self.thread
        return {
            "running": bool(thread is not None and thread.is_alive()),
            "activity_age_seconds": self.activity_age_seconds(),
            "server_proven_age_seconds": self.server_proven_age_seconds(),
            "monitor_wake_delay_seconds": self.monitor_wake_delay_seconds,
            "attempts": dict(self.attempt_counts),
            "outcomes": dict(self.outcome_counts),
            "last_phase_seconds": last.phase_seconds(),
            "worst_phase_seconds": worst.phase_seconds(),
            "last_statement_count": last.statement_count,
            "policy_seconds": {
                "adoption_silence": policy.adoption_silence_seconds,
                "heartbeat_interval": policy.heartbeat_interval_seconds,
                "failure_budget": policy.failure_budget_seconds,
                "monitor_interval": policy.monitor_interval_seconds,
                "exit_margin": policy.exit_margin_seconds,
                "server_proven_cap": policy.server_proven_cap_seconds,
                "max_healthy_server_gap": policy.max_healthy_server_gap_seconds,
                "stability_surplus": policy.stability_surplus_seconds,
            },
        }

    def start(self) -> threading.Thread | None:
        """Arm both coupled threads and wait for the first liveness proof."""
        existing = self.thread
        if existing is not None and existing.is_alive():
            return existing
        ledger = self._ports.ledger()
        guard_required = bool(
            getattr(ledger, "writer_lease_guard_required", False)
        )
        if not bool(
            getattr(ledger, "writer_lease_fast_adoption_capable", False)
        ):
            if guard_required:
                # The session token published to the lease row promises a
                # guarded heartbeat session, but the advisory guard is already
                # gone (for example the dedicated connection dropped between
                # ledger construction and serve()). Continuing would let a
                # replacement adopt the lease after one silence window while
                # this process keeps running as a fenced zombie.
                self._ports.lease_hard_exit(
                    "prism coordinator: writer lease guard was lost before "
                    "the lease heartbeat started; hard-exiting so a "
                    "replacement does not adopt while this process is live",
                    include_traceback=False,
                )
            return None
        verify = guard_session_verifier(ledger)
        if verify is None:
            if guard_required:
                self._ports.lease_hard_exit(
                    "prism coordinator: writer lease guard requires a "
                    "heartbeat but the ledger cannot verify the guarded "
                    "session; hard-exiting instead of running unguarded",
                    include_traceback=False,
                )
            return None
        # One validated policy, resolved before anything is armed. An unsafe
        # combination is refused outright rather than warned about and run:
        # the whole content of a violation is "this process may still be
        # live when a replacement adopts the lease", which is the split-brain
        # the guard exists to prevent, and running anyway trades a loud
        # startup failure for a silent double-writer window. Refusing here —
        # before the threads exist and before serve() admits a writer — is
        # the fail-closed reading.
        policy = self.policy()
        violations = policy.violations()
        if violations:
            self._ports.lease_hard_exit(
                "prism coordinator: refusing to start the ledger lease "
                "heartbeat; its timing policy cannot guarantee hard exit "
                "before replacement adoption ("
                + "; ".join(violations)
                + f"). Policy: {policy.describe()}",
                include_traceback=False,
            )
            return None
        for advisory in policy.advisories():
            # Safe but unstable: no double-writer window, but ordinary
            # verification tail latency will hard-exit a healthy
            # coordinator. A lab or test policy is allowed to make that
            # trade deliberately, so this stays a warning.
            print(
                "prism coordinator: ledger lease heartbeat timing has no "
                f"tail-latency headroom: {advisory}. Policy: "
                f"{policy.describe()}",
                flush=True,
            )
        armed_started_monotonic = self._ports.monotonic()
        self.last_success_monotonic = armed_started_monotonic
        self.last_server_proven_monotonic = armed_started_monotonic
        self.failed = threading.Event()
        self.ready = threading.Event()
        self.stop_event = threading.Event()
        thread = threading.Thread(
            target=self._ports.heartbeat_loop,
            name="prism-ledger-lease-heartbeat",
            daemon=True,
        )
        self.thread = thread
        thread.start()
        monitor_thread = threading.Thread(
            target=self._ports.monitor_loop,
            name="prism-ledger-lease-heartbeat-monitor",
            daemon=True,
        )
        monitor_thread.start()
        self.monitor_thread = monitor_thread
        failure_seconds = policy.failure_budget_seconds
        # The startup fencing deadline measures silence from the newest
        # monitor-visible activity, not from arming: a first verification
        # that is legally slower than the whole budget end-to-end but
        # demonstrably advancing (queue wait, then a statement round trip
        # near the guard's statement timeout) is not a wedged first beat.
        # A genuinely stuck first statement stamps nothing and ages out on
        # the same budget the monitor enforces.
        while not self.ready.wait(0.01):
            if self.failed.is_set():
                return None
            if self.activity_age_seconds() < failure_seconds:
                continue
            self._ports.lease_hard_exit(
                "prism coordinator: initial ledger lease heartbeat did not "
                "complete before startup fencing deadline "
                f"({self.last_phases.summary()})",
                include_traceback=False,
            )
            return None
        return thread

    def hard_exit(self, message: str, *, include_traceback: bool) -> None:
        with self.failure_lock:
            failed = self.failed
            if failed is None:
                failed = threading.Event()
                self.failed = failed
            if failed.is_set():
                return
            failed.set()
            # This thread now proceeds to _watchdog_hard_exit and stays parked
            # there until os._exit; it can never issue another renewal. The
            # release worker must not join it, or a heartbeat/monitor-driven
            # exit deadlocks against its own release and burns the exit budget
            # before the fresh-connection lease release can run.
            self.exit_thread = threading.current_thread()
        # Never write to stdout/stderr before arming the hard-exit path. A full
        # container log pipe can block a flush forever, leaving writer
        # admission open and the old process alive. Keep the detail in memory
        # for tests/embedders whose patched hard-exit returns; the real process
        # exits and Docker's die event is the authoritative diagnostic.
        self.failure_reason = message
        self.failure_has_traceback = include_traceback
        self._ports.watchdog_hard_exit(
            "lease_heartbeat",
            timeout_seconds=float(self._ports.exit_timeout_seconds()),
        )

    def run_guard_attempt(
        self,
        call: Callable[..., object],
        callbacks: frozenset[str],
        mode: str,
    ) -> object:
        """Run one guarded call, attributing its phases, and publish them.

        Every progress mark this installs feeds the same two clocks the
        monitor reads, and the attempt object turns the same marks into the
        phase breakdown an operator needs to see in the exit reason:
        queue wait for the guard's serialized slot, guard SQL across the
        round trips, and the residual — Python scheduling and GIL
        contention — that neither of those explains.

        Progress marks feed only the staleness monitor; authority freshness
        still moves exclusively on the conservative success edge the caller
        records. The attempt-start and slot-acquisition marks are
        client-side and can postdate a silent guard-session death by at
        most one idle interval, so they carry only the liveness budget. The
        adoption envelope is enforced separately by the monitor's
        server-proven cap, which measures from completed round trips at
        their conservative send edge. During any hang the external-effect
        fence also queues behind the hung statement and fails closed, so no
        new external effect authorizes while the monitor ages out.

        Raises whatever the guarded call raises, after publishing the
        phases of the failed attempt — the exit reason is built from them.
        """
        attempt = WriterLeaseVerificationAttempt(
            mode,
            monotonic=self._ports.monotonic,
        )

        def on_query_start() -> None:
            attempt.slot_acquired()
            self.record_progress()

        def on_statement_end() -> None:
            self.record_server_proven(attempt.statement_completed())

        kwargs: dict[str, Callable[[], None]] = {}
        if "on_query_start" in callbacks:
            kwargs["on_query_start"] = on_query_start
        if "on_statement_end" in callbacks:
            kwargs["on_statement_end"] = on_statement_end
        elif "on_statement_progress" in callbacks:
            # The legacy spelling: fires only between a verification's two
            # statements, which is enough to keep a lawful attribution
            # recheck from looking wedged but carries no phase timing.
            kwargs["on_statement_progress"] = self.record_server_proven
        try:
            result = call(**kwargs)
        except BaseException:
            self.record_phases(attempt.finish(LEASE_HEARTBEAT_OUTCOME_FAILED))
            raise
        self.record_phases(attempt.finish(self._attempt_outcome(mode, result)))
        # Completion is a real server response even when the call reported
        # no per-statement marks; stamp the conservative edge so a verifier
        # without callbacks still keeps the envelope fed.
        self.record_server_proven(attempt.proven_edge_monotonic)
        return result

    @staticmethod
    def _attempt_outcome(mode: str, result: object) -> str:
        """Classify one guarded call for attribution and metrics."""
        if not isinstance(result, dict):
            return (
                LEASE_HEARTBEAT_OUTCOME_PROVEN
                if mode == LEASE_HEARTBEAT_MODE_PROOF
                else LEASE_HEARTBEAT_OUTCOME_RENEWED
            )
        if mode == LEASE_HEARTBEAT_MODE_PROOF:
            if result.get("lease_renewal_due"):
                return LEASE_HEARTBEAT_OUTCOME_RENEWAL_DUE
            return LEASE_HEARTBEAT_OUTCOME_PROVEN
        if result.get("renewal_deferred_to_own_write"):
            return LEASE_HEARTBEAT_OUTCOME_DEFERRED
        try:
            renewed = int(result.get("renewed_count", 0))
        except (TypeError, ValueError):
            renewed = 0
        if renewed > 0:
            return LEASE_HEARTBEAT_OUTCOME_RENEWED
        return LEASE_HEARTBEAT_OUTCOME_PROVEN

    def heartbeat_loop(self) -> None:
        """Keep proving the guarded session live on an isolated DB path.

        The heartbeat must never wait on the lease tuple that fenced writes
        hold for whole transactions, or a long ``persist_accepted_block``
        would time out the heartbeat statement and hard-exit a healthy
        coordinator (the block-39416 restart loop). It still renews the
        lease TTL whenever that tuple is uncontended, so an idle coordinator
        (no fenced writes, CTV broadcaster disabled) does not let
        ``lease_expires_at`` lapse into different-identity expiry claims.

        Ownership and renewal run at different rates (issue #212). Each beat
        asks the cheap read-only ownership question — one non-blocking
        statement, no row lock and no ``pg_stat_activity`` scan — and the
        full renewing verification runs on the first beat and thereafter
        only on a beat whose proof reports the committed row inside the
        own-write authority margin. That escalation happens on the *same*
        beat, so renewal is never later than it was before the split, and
        while renewals are actually being skipped (a long own fenced write)
        every beat runs the same fail-closed verification it always did.
        A ledger with no proof method keeps running the full verification
        on every beat.
        """
        ledger = self._ports.ledger()
        verify = guard_session_verifier(ledger)
        if verify is None:
            return
        prove = guard_session_prover(ledger)
        verify_callbacks = verifier_callbacks(verify)
        prove_callbacks = (
            verifier_callbacks(prove) if prove is not None else frozenset()
        )
        interval_seconds = float(self._ports.heartbeat_seconds())
        heartbeat_stop = self.stop_event
        if heartbeat_stop is None:
            heartbeat_stop = threading.Event()
            self.stop_event = heartbeat_stop
        # The first beat renews: startup readiness must prove the renewing
        # path works, not merely that the session answers.
        renew_this_beat = True
        while not heartbeat_stop.is_set():
            beat_started_monotonic = self._ports.monotonic()
            # An advancing verification is monitor-visible activity, and the
            # attempt start is its first client-side mark.
            self.record_progress()
            try:
                if prove is not None and not renew_this_beat:
                    proof = self.run_guard_attempt(
                        prove,
                        prove_callbacks,
                        LEASE_HEARTBEAT_MODE_PROOF,
                    )
                    renew_this_beat = not isinstance(proof, dict) or bool(
                        proof.get("lease_renewal_due")
                    )
                if renew_this_beat or prove is None:
                    self.record_progress()
                    self.run_guard_attempt(
                        verify,
                        verify_callbacks,
                        LEASE_HEARTBEAT_MODE_RENEW,
                    )
                    # A landed renewal (or a deferral behind this writer's
                    # own in-flight write) puts the answer back in the
                    # committed row: the next beat's proof re-reads it and
                    # escalates again if renewal is still outstanding.
                    renew_this_beat = False
            except Exception:
                self._ports.lease_hard_exit(
                    "prism coordinator: ledger lease heartbeat failed; "
                    "hard-exiting so this process cannot outlive its "
                    "fast-adoptable session "
                    f"({self.last_phases.summary()})",
                    include_traceback=True,
                )
                return
            # The session was proven live no earlier than the beat's start.
            # Using that conservative edge prevents a delayed response from
            # making local freshness look newer than what PostgreSQL proved.
            self.record_success(beat_started_monotonic)
            ready = self.ready
            if ready is not None:
                ready.set()
            if heartbeat_stop.wait(interval_seconds):
                return

    def monitor_loop(self) -> None:
        # One resolved policy for the whole loop: the monitor's two bounds
        # and the exit reason all quote the same inequality.
        policy = self.policy()
        failure_seconds = policy.failure_budget_seconds
        monitor_seconds = policy.monitor_interval_seconds
        heartbeat_stop = self.stop_event
        if heartbeat_stop is None:
            heartbeat_stop = threading.Event()
            self.stop_event = heartbeat_stop
        # The adoption-envelope cap: the client-side progress marks that
        # keep a lawfully slow verification alive can postdate a silent
        # guard-session death by up to one idle interval, which with a
        # silence sized at interval + budget would let the hard exit land
        # at or after a replacement's CAS eligibility. Server-proven marks
        # cannot postdate the death, so a second bound measured from them
        # restores exit-before-adoption regardless of client marks. What
        # the cap must leave free inside the silence is the whole exit
        # envelope: the hard-exit budget, one poll of granularity, this
        # thread's own budgeted lateness, and one further poll of strict
        # reserve (see WriterLeaseHeartbeatPolicy.exit_envelope_seconds).
        # Reserving only the poll intervals — as this did before the
        # scheduler-slack term was added — lets a maximally late monitor
        # begin the exit after the successor is already eligible. The cap
        # is floored at the failure budget so a deliberately tiny (advised)
        # silence degrades to the plain liveness bound instead of killing
        # lawful single-statement verifications.
        server_cap_seconds = policy.server_proven_cap_seconds
        while True:
            wait_started_monotonic = self._ports.monotonic()
            if heartbeat_stop.wait(monitor_seconds):
                return
            # The monitor decides the hard exit, so its own lateness is
            # spent envelope exactly like a slow statement is. Attributing
            # it separately is what lets an operator tell "PostgreSQL was
            # slow" from "this process could not get scheduled".
            wake_delay = max(
                0.0,
                self._ports.monotonic() - wait_started_monotonic - monitor_seconds,
            )
            if wake_delay > self.monitor_wake_delay_seconds:
                self.monitor_wake_delay_seconds = wake_delay
            # Mid-verification progress counts: the budget — sized for one
            # statement plus one idle wait plus scheduler slack so it stays
            # under the adoption silence — cannot absorb a whole multi-step
            # verification, and each demonstrable step (attempt start, slot
            # acquisition, statement round trip, completion) is the
            # advancing-heartbeat evidence this monitor watches for. A
            # wedged statement records nothing and still ages out.
            age_seconds = self.activity_age_seconds()
            server_age_seconds = self.server_proven_age_seconds()
            if (
                age_seconds < failure_seconds
                and server_age_seconds < server_cap_seconds
            ):
                continue
            # Name the bound that tripped and the phase that consumed it.
            # Issue #212 was unresolvable in production precisely because
            # the exit said only how stale the heartbeat was, never where
            # the time went.
            if age_seconds >= failure_seconds:
                tripped = (
                    f"activity {age_seconds:.3f}s >= failure budget "
                    f"{failure_seconds:.3f}s"
                )
            else:
                tripped = (
                    f"server-proven {server_age_seconds:.3f}s >= adoption "
                    f"envelope cap {server_cap_seconds:.3f}s"
                )
            self._ports.lease_hard_exit(
                "prism coordinator: ledger lease heartbeat stopped making "
                f"progress for {age_seconds:.3f}s "
                f"(server-proven {server_age_seconds:.3f}s); hard-exiting "
                "before its session becomes fast-adoptable. "
                f"Tripped: {tripped}. Last attempt: "
                f"{self.last_phases.summary()}. Worst attempt: "
                f"{self.worst_phases.summary()}. Monitor wake delay: "
                f"{self.monitor_wake_delay_seconds:.3f}s. Policy: "
                f"{policy.describe()}",
                include_traceback=False,
            )
            return

    def stop(self, *, deadline: float | None = None) -> bool:
        heartbeat_stop = self.stop_event
        if heartbeat_stop is None:
            return True
        heartbeat_stop.set()
        threads = (self.thread, self.monitor_thread)
        # A heartbeat or monitor thread that armed the hard exit is blocked in
        # _watchdog_hard_exit joining the very release worker that runs this
        # method. It has already stopped renewing forever, so treat it as
        # stopped instead of deadlocking against it until the deadline.
        exit_thread = self.exit_thread
        if deadline is None:
            deadline = (
                self._ports.monotonic()
                + DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS
            )
        for thread in threads:
            if (
                thread is None
                or thread is threading.current_thread()
                or thread is exit_thread
            ):
                continue
            thread.join(max(0.0, deadline - self._ports.monotonic()))
        return all(
            thread is None
            or thread is threading.current_thread()
            or thread is exit_thread
            or not thread.is_alive()
            for thread in threads
        )

    def require_fresh_lease_for_external_side_effect(self, component: str) -> None:
        """Synchronously verify the exact guarded session before an RPC effect.

        The periodic heartbeat is an early process-liveness detector, not an
        authorization oracle: CLOCK_MONOTONIC may not advance across host
        suspend, and a PostgreSQL connection object does not necessarily know
        its server session died until the next I/O. Every external mutation
        therefore performs a bounded non-blocking verification on the session
        holding the advisory guard. It must not wait on the lease row: a long
        fenced transaction (``persist_accepted_block``) holds that tuple's
        row lock until commit, and queueing behind it would time out and
        fence a healthy coordinator (the TTL is renewed opportunistically via
        SKIP LOCKED instead). A daemon worker bounds a dead network path even
        when the driver's server-side statement timeout cannot.

        PostgreSQL and qbitd are independent systems, so this remains a
        preflight fence rather than an atomic transaction with the subsequent
        RPC. The advisory session remains held across the RPC in the ordinary
        case; see the deployment guide for the residual post-check pause risk.

        A verification that reports ``renewal_deferred_to_own_write`` proves
        liveness but not authority: the writer's own fenced write holds the
        expired lease row, and only its commit guarantees the lease survives.
        That is enough for the heartbeat, which merely keeps a healthy
        process from self-fencing, but not for an RPC — a rollback would let
        a queued claimant take the lease while the effect is in flight. This
        fence then raises :class:`WriterLeaseRenewalDeferred` without arming
        the hard exit; callers already retry deferred work on their own
        cadence.
        """
        ledger = self._ports.ledger()
        if not bool(
            getattr(ledger, "writer_lease_guard_required", False)
        ):
            return
        verify = guard_session_verifier(ledger)
        if verify is None:
            self._ports.lease_hard_exit(
                "prism coordinator: refusing external side effect from "
                f"{component}; exact-session verification is unavailable",
                include_traceback=False,
            )
            raise ShutdownInProgress(
                f"writer lease guard verification is unavailable before {component}"
            )

        # The configured fence timeout budgets one statement; the
        # verification may lawfully run a second inside the same guarded
        # slot (the attribution recheck), and killing that recheck at the
        # single-statement deadline would hard-exit a coordinator the
        # recheck was about to prove healthy. Two moderately slow but
        # in-deadline statements must both fit.
        timeout_seconds = WRITER_LEASE_VERIFICATION_MAX_STATEMENTS * max(
            0.0,
            float(self._ports.external_fence_timeout_seconds()),
        )
        # The guarded session serializes this verification behind the periodic
        # heartbeat and any concurrent fence, and the execution budget above
        # tracks the guard's statement timeout. Waiting for the serialized
        # query slot must not consume that budget, or a fence arriving during
        # an in-flight renewal hard-exits a coordinator whose session is still
        # healthy. The queue wait gets the heartbeat failure budget instead: a
        # guard session that cannot complete queued work within it is already
        # being declared dead by the heartbeat monitor.
        queue_timeout_seconds = max(
            timeout_seconds,
            float(self._ports.failure_seconds()),
        )
        query_started = threading.Event()
        callbacks = verifier_callbacks(verify)
        verify_reports_query_start = "on_query_start" in callbacks
        # This verification holds the guard's serialized slot for its whole
        # statement sequence, so the periodic heartbeat queues behind it
        # and records nothing meanwhile. Its lawful second statement (the
        # attribution recheck) can therefore age the heartbeat's last
        # success past the monitor's failure budget — the same hazard the
        # heartbeat loop's own recheck had — so this caller must feed the
        # monitor its completed round trips too.
        verify_reports_statement_end = "on_statement_end" in callbacks
        verify_reports_statement_progress = (
            not verify_reports_statement_end
            and "on_statement_progress" in callbacks
        )
        # The fence is a guard-slot caller like the heartbeat, so its phases
        # belong in the same attribution: a fence that spent its budget
        # queueing behind an in-flight renewal reads very differently from
        # one that spent it inside PostgreSQL.
        attempt = WriterLeaseVerificationAttempt(
            LEASE_HEARTBEAT_MODE_FENCE,
            monotonic=self._ports.monotonic,
        )
        if not verify_reports_query_start:
            # A verification that cannot report when its query slot was
            # acquired keeps the previous behavior: the whole budget covers
            # both.
            query_started.set()

        outcome: queue.Queue[BaseException | None] = queue.Queue(maxsize=1)
        verification_result: list[object] = []
        verification_started_monotonic = attempt.started_monotonic

        def on_query_start() -> None:
            attempt.slot_acquired()
            query_started.set()

        def on_statement_end() -> None:
            self.record_server_proven(attempt.statement_completed())

        def verify_exact_session() -> None:
            try:
                verify_kwargs: dict[str, Callable[[], None]] = {}
                if verify_reports_query_start:
                    verify_kwargs["on_query_start"] = on_query_start
                if verify_reports_statement_end:
                    verify_kwargs["on_statement_end"] = on_statement_end
                elif verify_reports_statement_progress:
                    verify_kwargs["on_statement_progress"] = (
                        self.record_server_proven
                    )
                result = verify(**verify_kwargs)
            except BaseException as exc:
                outcome.put(exc)
            else:
                verification_result.append(result)
                outcome.put(None)

        try:
            thread = threading.Thread(
                target=verify_exact_session,
                name="prism-ledger-lease-external-fence",
                daemon=True,
            )
            thread.start()
        except BaseException as exc:
            self._ports.lease_hard_exit(
                "prism coordinator: refusing external side effect from "
                f"{component}; could not start exact-session verification",
                include_traceback=False,
            )
            raise ShutdownInProgress(
                f"writer lease guard verification could not start before {component}"
            ) from exc

        def refuse(detail: str) -> str:
            """Fail closed, naming the phase that consumed the fence budget."""
            phases = attempt.finish(LEASE_HEARTBEAT_OUTCOME_FAILED)
            self.record_phases(phases)
            return (
                "prism coordinator: refusing external side effect from "
                f"{component}; {detail} ({phases.summary()})"
            )

        queue_deadline = verification_started_monotonic + queue_timeout_seconds
        while not query_started.is_set() and thread.is_alive():
            remaining = queue_deadline - self._ports.monotonic()
            if remaining <= 0.0:
                self._ports.lease_hard_exit(
                    refuse(
                        "exact-session verification could not start within "
                        f"{queue_timeout_seconds:g}s"
                    ),
                    include_traceback=False,
                )
                raise ShutdownInProgress(
                    f"writer lease guard verification timed out before {component}"
                )
            thread.join(min(0.01, remaining))
        thread.join(timeout_seconds)
        if thread.is_alive():
            self._ports.lease_hard_exit(
                refuse(
                    "exact-session verification exceeded "
                    f"{timeout_seconds:g}s"
                ),
                include_traceback=False,
            )
            raise ShutdownInProgress(
                f"writer lease guard verification timed out before {component}"
            )

        try:
            error = outcome.get_nowait()
        except queue.Empty as exc:
            self._ports.lease_hard_exit(
                refuse("exact-session verification returned no result"),
                include_traceback=False,
            )
            raise ShutdownInProgress(
                f"writer lease guard verification failed before {component}"
            ) from exc
        if error is not None:
            self._ports.lease_hard_exit(
                refuse("exact-session verification failed"),
                include_traceback=False,
            )
            raise ShutdownInProgress(
                f"writer lease guard verification failed before {component}"
            ) from error

        # Use the call-start time so scheduler delay never makes this success
        # appear fresher than the database response actually proves. The
        # envelope edge takes the conservative send edge of the newest
        # completed round trip for the same reason.
        self.record_success(verification_started_monotonic)
        self.record_server_proven(attempt.proven_edge_monotonic)

        # A verification that proved liveness only because the writer's own
        # fenced write holds the expired lease row is not authority for an
        # external side effect: that survival argument assumes the write
        # commits, and a rollback would hand the row to a queued
        # different-identity claimant while the RPC is in flight. Refuse the
        # side effect without fencing the process — the session is healthy
        # and the write's commit will land the next renewal; callers retry
        # on their own cadence (broadcast interval, candidate outbox replay).
        result = verification_result[0] if verification_result else None
        self.record_phases(
            attempt.finish(
                self._attempt_outcome(LEASE_HEARTBEAT_MODE_FENCE, result)
            )
        )
        if isinstance(result, dict) and result.get(
            "renewal_deferred_to_own_write"
        ):
            raise WriterLeaseRenewalDeferred(
                f"withholding {component}: writer lease renewal is deferred "
                "behind this coordinator's own in-flight fenced write; "
                "retry after that transaction commits and a renewal lands"
            )


class _LeaseHeartbeatStateField:
    """Descriptor routing a legacy coordinator attribute to lease ownership.

    The lease service holds the only copy of each mutable field; the
    coordinator class keeps the historical private attribute names readable
    and writable for tests and embedders. Reads and writes construct the
    lazy service on first touch so pre-service fixture assignments are
    adopted rather than duplicated.
    """

    def __init__(self, attribute: str):
        self.attribute = attribute

    def __get__(self, instance: Any, owner: type[Any] | None = None) -> Any:
        if instance is None:
            return self
        return getattr(
            instance._ensure_lease_heartbeat_service(), self.attribute
        )

    def __set__(self, instance: Any, value: Any) -> None:
        setattr(
            instance._ensure_lease_heartbeat_service(), self.attribute, value
        )
