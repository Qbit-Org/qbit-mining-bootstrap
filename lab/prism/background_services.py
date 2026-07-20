"""Named lifecycle registry for PRISM process-level background loops.

The registry deliberately has no ``start_all`` operation.  The coordinator
starts named services at the existing recovery boundaries, while this module
owns the start-once state and the exact thread handles used during shutdown.
"""

from __future__ import annotations

from dataclasses import dataclass
import threading
from typing import Callable, Iterable


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


@dataclass(frozen=True, slots=True)
class WatchdogPorts:
    """Dynamic process-supervision capabilities used by the watchdog loop."""

    wait_for_stop: Callable[[float], bool]
    interval_seconds: Callable[[], float]
    monotonic: Callable[[], float]
    publication_failure_expired: Callable[[float], bool]
    publication_budget_seconds: Callable[[], float]
    liveness_enabled: Callable[[], bool]
    overdue_heartbeats: Callable[[float], list[str]]
    liveness_timeout_seconds: Callable[[], float]
    log: Callable[[str], None]
    exit_process: Callable[[int], None]


class WatchdogService:
    """Own the bounded-wait process supervision loop."""

    def __init__(self, ports: WatchdogPorts) -> None:
        self.ports = ports

    def run(self) -> None:
        while not self.ports.wait_for_stop(self.ports.interval_seconds()):
            now = self.ports.monotonic()
            if self.ports.publication_failure_expired(now):
                publication_budget = self.ports.publication_budget_seconds()
                self.ports.log(
                    "prism coordinator: publication-progress watchdog firing; "
                    "current tip/generation remained unpublished past the "
                    "template refresh failure budget="
                    f"{publication_budget:g}s. "
                    "Exiting non-zero so the restart policy recovers the process."
                )
                self.ports.exit_process(1)
                return
            overdue = (
                self.ports.overdue_heartbeats(now)
                if self.ports.liveness_enabled()
                else []
            )
            if overdue:
                self.ports.log(
                    "prism coordinator: liveness watchdog firing; unresponsive "
                    f"subsystems={overdue} "
                    f"timeout={self.ports.liveness_timeout_seconds():g}s. "
                    "Exiting non-zero so the restart policy recovers the process."
                )
                self.ports.exit_process(1)
                return


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
