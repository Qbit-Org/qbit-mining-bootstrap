"""Named lifecycle registry for PRISM process-level background loops.

The registry deliberately has no ``start_all`` operation.  The coordinator
starts named services at the existing recovery boundaries, while this module
owns the start-once state and the exact thread handles used during shutdown.

This module also owns :class:`LedgerLeaseHeartbeatService`, the sole owner of
the ledger writer-lease heartbeat/monitor threads, their freshness state, and
the synchronous exact-session external-effect fence.  Because its startup is
conditional, creates two coupled threads, and waits for the first proof, it is
started directly at the serve boundary rather than modeled as two independent
registry specifications.  Watchdog policy itself (classification, coordination
streak, emergency exit) remains coordinator-owned at this layer; the lease
service reaches process exit only through a dynamic coordinator
``_watchdog_hard_exit`` port.
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

    def record_success(self, renewal_started_monotonic: float) -> None:
        """Advance the conservative freshness edge without regression."""
        with self.freshness_lock:
            last_success = self.last_success_monotonic
            if (
                last_success is None
                or renewal_started_monotonic > last_success
            ):
                self.last_success_monotonic = renewal_started_monotonic

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
        self.last_progress_monotonic = time.monotonic()

    def record_server_proven(self) -> None:
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
        """
        now = time.monotonic()
        self.last_progress_monotonic = now
        self.last_server_proven_monotonic = now

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
            else time.monotonic()
        )
        last_progress = float(
            self.last_progress_monotonic
            if self.last_progress_monotonic is not None
            else last_success
        )
        return max(0.0, time.monotonic() - max(last_success, last_progress))

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
        armed_started_monotonic = time.monotonic()
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
        failure_seconds = float(self._ports.failure_seconds())
        interval_seconds = float(self._ports.heartbeat_seconds())
        silence_seconds = self.adoption_silence_seconds()
        monitor_seconds = float(self._ports.monitor_seconds())
        exit_seconds = float(self._ports.exit_timeout_seconds())
        if (
            failure_seconds <= interval_seconds
            or silence_seconds
            <= failure_seconds + exit_seconds + 2.0 * monitor_seconds
        ):
            # The hard-exit ordering argument needs interval < budget (one
            # idle wait must not exhaust the budget) and enough headroom
            # between the budget and the adoption silence for the monitor's
            # server-proven envelope cap to fit its poll and exit costs
            # (this process must be gone before a replacement may CAS).
            # The derived defaults satisfy both; only operator overrides
            # can break them, so warn rather than refuse.
            print(
                "prism coordinator: ledger lease heartbeat timing is "
                f"misconfigured: failure budget {failure_seconds:g}s must "
                f"exceed the heartbeat interval {interval_seconds:g}s and "
                f"leave the adoption silence {silence_seconds:g}s at least "
                f"{exit_seconds + 2.0 * monitor_seconds:g}s of envelope "
                "headroom; continuing, but hard-exit is no longer "
                "guaranteed to precede replacement adoption",
                flush=True,
            )
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
                "complete before startup fencing deadline",
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

    def heartbeat_loop(self) -> None:
        """Keep proving the guarded session live on an isolated DB path.

        The heartbeat must never wait on the lease tuple that fenced writes
        hold for whole transactions, or a long ``persist_accepted_block``
        would time out the heartbeat statement and hard-exit a healthy
        coordinator (the block-39416 restart loop). It still renews the
        lease TTL whenever that tuple is uncontended, so an idle coordinator
        (no fenced writes, CTV broadcaster disabled) does not let
        ``lease_expires_at`` lapse into different-identity expiry claims.
        """
        ledger = self._ports.ledger()
        verify = guard_session_verifier(ledger)
        if verify is None:
            return
        interval_seconds = float(self._ports.heartbeat_seconds())
        heartbeat_stop = self.stop_event
        if heartbeat_stop is None:
            heartbeat_stop = threading.Event()
            self.stop_event = heartbeat_stop
        # The verification may lawfully run a second statement (the
        # attribution recheck) whose duration the monitor's failure budget
        # cannot absorb — that budget must stay under the adoption silence,
        # so it is sized for one statement. A verification that reports
        # per-statement progress lets the monitor count the completed first
        # round trip instead of hard-exiting a healthy coordinator mid-way
        # through a lawful recheck.
        try:
            verify_parameters = inspect.signature(verify).parameters
        except (TypeError, ValueError):
            verify_parameters = {}
        verify_reports_query_start = "on_query_start" in verify_parameters
        verify_reports_statement_progress = (
            "on_statement_progress" in verify_parameters
        )
        while not heartbeat_stop.is_set():
            verify_started_monotonic = time.monotonic()
            # An advancing verification is monitor-visible activity. The
            # failure budget is sized for one statement round trip, so the
            # monitor must measure silence between demonstrable steps of a
            # verification (attempt start, query-slot acquisition, statement
            # progress, completion), never between success edges: those are
            # call-start times, so back-to-back verifications that each
            # lawfully approach the statement budget age the freshest edge
            # by two call durations plus the idle interval and the monitor
            # would hard-exit a healthy sole writer. Progress marks feed
            # only the staleness monitor — authority freshness still moves
            # exclusively on the conservative success edge below. The
            # attempt-start and slot-acquisition marks are client-side and
            # can postdate a silent guard-session death by at most one idle
            # interval, so they carry only the liveness budget; the
            # adoption envelope (hard exit before a replacement's silence
            # window elapses) is enforced separately by the monitor's
            # server-proven cap, which measures from completed round trips
            # alone. During any such hang the external-effect fence also
            # queues behind the hung statement and fails closed, so no new
            # external effect authorizes while the monitor ages out.
            self.record_progress()
            verify_kwargs: dict[str, Callable[[], None]] = {}
            if verify_reports_query_start:
                verify_kwargs["on_query_start"] = self.record_progress
            if verify_reports_statement_progress:
                verify_kwargs["on_statement_progress"] = (
                    self.record_server_proven
                )
            try:
                verify(**verify_kwargs)
            except Exception:
                self._ports.lease_hard_exit(
                    "prism coordinator: ledger lease heartbeat failed; "
                    "hard-exiting so this process cannot outlive its "
                    "fast-adoptable session",
                    include_traceback=True,
                )
                return
            # The session was proven live no earlier than call start. Using
            # that conservative edge prevents a delayed response from making
            # local freshness look newer than what PostgreSQL actually proved.
            self.record_success(verify_started_monotonic)
            # Completion is the newest demonstrable activity and a real
            # server response; the monitor may observe it even though
            # authority freshness stays pinned to the call-start edge
            # recorded above.
            self.record_server_proven()
            ready = self.ready
            if ready is not None:
                ready.set()
            if heartbeat_stop.wait(interval_seconds):
                return

    def monitor_loop(self) -> None:
        failure_seconds = float(self._ports.failure_seconds())
        monitor_seconds = float(self._ports.monitor_seconds())
        heartbeat_stop = self.stop_event
        if heartbeat_stop is None:
            heartbeat_stop = threading.Event()
            self.stop_event = heartbeat_stop
        # The adoption-envelope cap: the client-side progress marks that
        # keep a lawfully slow verification alive can postdate a silent
        # guard-session death by up to one idle interval, which with the
        # derived defaults (interval + budget = silence) would let the
        # hard exit land at or after a replacement's CAS eligibility.
        # Server-proven marks cannot postdate the death, so a second bound
        # measured from them — sized to leave room for one monitor poll on
        # each side plus the hard-exit budget inside the silence window —
        # restores exit-before-adoption regardless of client marks. It is
        # floored at the failure budget so a misconfigured (warned) tiny
        # silence degrades to the plain liveness bound instead of killing
        # lawful single-statement verifications.
        exit_seconds = float(self._ports.exit_timeout_seconds())
        server_cap_seconds = max(
            failure_seconds,
            self.adoption_silence_seconds()
            - exit_seconds
            - 2.0 * monitor_seconds,
        )
        while not heartbeat_stop.wait(monitor_seconds):
            # Mid-verification progress counts: the budget — sized for one
            # statement so it stays under the adoption silence — cannot
            # absorb a whole multi-step verification, and each demonstrable
            # step (attempt start, slot acquisition, statement round trip,
            # completion) is the advancing-heartbeat evidence this monitor
            # watches for. A wedged statement records nothing and still
            # ages out.
            age_seconds = self.activity_age_seconds()
            last_server_proven = float(
                self.last_server_proven_monotonic
                if self.last_server_proven_monotonic is not None
                else time.monotonic()
            )
            server_age_seconds = max(
                0.0,
                time.monotonic() - last_server_proven,
            )
            if (
                age_seconds < failure_seconds
                and server_age_seconds < server_cap_seconds
            ):
                continue
            self._ports.lease_hard_exit(
                "prism coordinator: ledger lease heartbeat stopped making "
                f"progress for {age_seconds:.3f}s "
                f"(server-proven {server_age_seconds:.3f}s); hard-exiting "
                "before its session becomes fast-adoptable",
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
                time.monotonic()
                + DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS
            )
        for thread in threads:
            if (
                thread is None
                or thread is threading.current_thread()
                or thread is exit_thread
            ):
                continue
            thread.join(max(0.0, deadline - time.monotonic()))
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
        try:
            verify_parameters = inspect.signature(verify).parameters
        except (TypeError, ValueError):
            verify_parameters = {}
        verify_reports_query_start = "on_query_start" in verify_parameters
        # This verification holds the guard's serialized slot for its whole
        # statement sequence, so the periodic heartbeat queues behind it
        # and records nothing meanwhile. Its lawful second statement (the
        # attribution recheck) can therefore age the heartbeat's last
        # success past the monitor's single-statement failure budget — the
        # same hazard the heartbeat loop's own recheck had — so this
        # caller must feed the monitor the completed first round trip too.
        verify_reports_statement_progress = (
            "on_statement_progress" in verify_parameters
        )
        if not verify_reports_query_start:
            # A verification that cannot report when its query slot was
            # acquired keeps the previous behavior: the whole budget covers
            # both.
            query_started.set()

        outcome: queue.Queue[BaseException | None] = queue.Queue(maxsize=1)
        verification_result: list[object] = []
        verification_started_monotonic = time.monotonic()

        def verify_exact_session() -> None:
            try:
                verify_kwargs: dict[str, Callable[[], None]] = {}
                if verify_reports_query_start:
                    verify_kwargs["on_query_start"] = query_started.set
                if verify_reports_statement_progress:
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

        queue_deadline = verification_started_monotonic + queue_timeout_seconds
        while not query_started.is_set() and thread.is_alive():
            remaining = queue_deadline - time.monotonic()
            if remaining <= 0.0:
                self._ports.lease_hard_exit(
                    "prism coordinator: refusing external side effect from "
                    f"{component}; exact-session verification could not "
                    f"start within {queue_timeout_seconds:g}s",
                    include_traceback=False,
                )
                raise ShutdownInProgress(
                    f"writer lease guard verification timed out before {component}"
                )
            thread.join(min(0.01, remaining))
        thread.join(timeout_seconds)
        if thread.is_alive():
            self._ports.lease_hard_exit(
                "prism coordinator: refusing external side effect from "
                f"{component}; exact-session verification exceeded "
                f"{timeout_seconds:g}s",
                include_traceback=False,
            )
            raise ShutdownInProgress(
                f"writer lease guard verification timed out before {component}"
            )

        try:
            error = outcome.get_nowait()
        except queue.Empty as exc:
            self._ports.lease_hard_exit(
                "prism coordinator: refusing external side effect from "
                f"{component}; exact-session verification returned no result",
                include_traceback=False,
            )
            raise ShutdownInProgress(
                f"writer lease guard verification failed before {component}"
            ) from exc
        if error is not None:
            self._ports.lease_hard_exit(
                "prism coordinator: refusing external side effect from "
                f"{component}; exact-session verification failed",
                include_traceback=False,
            )
            raise ShutdownInProgress(
                f"writer lease guard verification failed before {component}"
            ) from error

        # Use the call-start time so scheduler delay never makes this success
        # appear fresher than the database response actually proves. The
        # response itself is a fresh server round trip, so the monitor's
        # envelope cap may take it at receipt time.
        self.record_success(verification_started_monotonic)
        self.record_server_proven()

        # A verification that proved liveness only because the writer's own
        # fenced write holds the expired lease row is not authority for an
        # external side effect: that survival argument assumes the write
        # commits, and a rollback would hand the row to a queued
        # different-identity claimant while the RPC is in flight. Refuse the
        # side effect without fencing the process — the session is healthy
        # and the write's commit will land the next renewal; callers retry
        # on their own cadence (broadcast interval, candidate outbox replay).
        result = verification_result[0] if verification_result else None
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
