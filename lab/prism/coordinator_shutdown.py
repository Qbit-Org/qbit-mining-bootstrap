"""Writer admission and shutdown state for the PRISM coordinator."""

from __future__ import annotations

from contextlib import AbstractContextManager
from functools import wraps
import signal
import threading
import time
from typing import Any, Callable, Protocol


class ShutdownInProgress(RuntimeError):
    """Raised when work that could mutate the ledger arrives after shutdown."""


class _WriterOperationToken:
    """One transferable writer admission held until durable work completes."""

    def __init__(self, controller: "CoordinatorShutdownController", component: str):
        self.controller = controller
        self.component = component
        self.finished = False

    def finish(self) -> None:
        self.controller.finish_token(self)


class CoordinatorShutdownController:
    """Coordinates the writer barrier, one-shot lease release, and final drain.

    Writer operations enter through :meth:`enter_writer` before shutdown or
    inherit an already-admitted operation on the same thread. Queue admissions
    use transferable tokens so a share remains visible to the barrier while it
    moves from a client thread to the group-commit writer.
    """

    def __init__(self, writer_quiescence_timeout_seconds: float):
        self.writer_quiescence_timeout_seconds = writer_quiescence_timeout_seconds
        self.condition = threading.Condition(threading.RLock())
        self.local = threading.local()
        self.phase = "running"
        self.reason: str | None = None
        self.signal_number: int | None = None
        self.sigterm_monotonic: float | None = None
        self.shutdown_started_monotonic: float | None = None
        self.active_writers: dict[str, int] = {}
        self.shutdowns_total = 0
        self.writer_quiescence_outcomes = {"success": 0, "timeout": 0}
        self.writer_quiescence_seconds = 0.0
        self.lease_release_attempts_total = 0
        self.lease_release_outcomes = {
            "success": 0,
            "not_held": 0,
            "unsupported": 0,
            "failure": 0,
        }
        self.lease_release_seconds = 0.0
        self.lease_release_attempted = False
        self.lease_release_succeeded = False
        self.lease_release_withheld = False
        self.sigterm_to_lease_release_seconds = 0.0
        self.sigterm_release_observed = False
        self.release_withheld_total = 0
        self.non_writer_drain_seconds = 0.0
        self.non_writer_drains_total = 0
        self._drain_claimed = False

    def request_shutdown(self, signum: int | None) -> None:
        """Close admission atomically; the caller only needs to set its event."""
        now = time.monotonic()
        with self.condition:
            if signum == signal.SIGTERM and self.sigterm_monotonic is None:
                self.sigterm_monotonic = now
            if self.signal_number is None and signum is not None:
                self.signal_number = signum
            if self.phase == "running":
                self.phase = "requested"
            self.condition.notify_all()

    def begin_shutdown(self, reason: str) -> bool:
        with self.condition:
            if self.phase not in {"running", "requested"}:
                return False
            self.phase = "quiescing_writers"
            self.reason = reason
            self.shutdown_started_monotonic = time.monotonic()
            self.shutdowns_total += 1
            self.condition.notify_all()
            return True

    def wait_for_lease_handling(self) -> bool:
        """Wait for the one shutdown owner to release or safely withhold."""
        in_progress = {
            "requested",
            "quiescing_writers",
            "writers_quiesced",
            "releasing_lease",
        }
        with self.condition:
            while self.phase in in_progress:
                self.condition.wait()
            return self.lease_release_succeeded

    def _thread_writer_depth(self) -> int:
        return int(getattr(self.local, "writer_depth", 0))

    def _admit_writer_locked(self, component: str, *, inherited: bool) -> _WriterOperationToken:
        if self.lease_release_attempted:
            raise ShutdownInProgress("PRISM writer lease release has already started")
        if self.phase != "running" and not inherited:
            raise ShutdownInProgress("PRISM coordinator is shutting down")
        self.active_writers[component] = self.active_writers.get(component, 0) + 1
        return _WriterOperationToken(self, component)

    def enter_writer(self, component: str) -> _WriterOperationToken:
        depth = self._thread_writer_depth()
        with self.condition:
            token = self._admit_writer_locked(component, inherited=depth > 0)
        self.local.writer_depth = depth + 1
        return token

    def exit_writer(self, token: _WriterOperationToken) -> None:
        depth = self._thread_writer_depth()
        self.local.writer_depth = max(0, depth - 1)
        token.finish()

    def reserve_writer(self, component: str) -> _WriterOperationToken:
        """Reserve work that will finish on another thread."""
        with self.condition:
            return self._admit_writer_locked(
                component,
                inherited=self._thread_writer_depth() > 0,
            )

    def finish_token(self, token: _WriterOperationToken) -> None:
        with self.condition:
            if token.finished:
                return
            token.finished = True
            remaining = self.active_writers.get(token.component, 0) - 1
            if remaining > 0:
                self.active_writers[token.component] = remaining
            else:
                self.active_writers.pop(token.component, None)
            self.condition.notify_all()

    def has_active_writer(self, components: set[str]) -> bool:
        with self.condition:
            return any(self.active_writers.get(component, 0) for component in components)

    def wait_for_no_active_writer(
        self,
        components: set[str],
        timeout_seconds: float,
    ) -> bool:
        """Wait once for the selected writer classes to become quiescent."""
        with self.condition:
            if not any(
                self.active_writers.get(component, 0) for component in components
            ):
                return True
            self.condition.wait(max(0.0, timeout_seconds))
            return not any(
                self.active_writers.get(component, 0) for component in components
            )

    def writer_admission_closed(self) -> bool:
        with self.condition:
            return self.phase != "running"

    def wait_for_writer_quiescence(self) -> tuple[bool, float, dict[str, int]]:
        started = time.monotonic()
        deadline = started + self.writer_quiescence_timeout_seconds
        with self.condition:
            while self.active_writers:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self.condition.wait(remaining)
            elapsed = max(0.0, time.monotonic() - started)
            quiesced = not self.active_writers
            blockers = dict(sorted(self.active_writers.items()))
            outcome = "success" if quiesced else "timeout"
            self.writer_quiescence_outcomes[outcome] += 1
            self.writer_quiescence_seconds = elapsed
            if quiesced:
                self.phase = "writers_quiesced"
            else:
                self.phase = "release_withheld"
                self.lease_release_withheld = True
                self.release_withheld_total += 1
            self.condition.notify_all()
            return quiesced, elapsed, blockers

    def claim_lease_release(self) -> tuple[bool, dict[str, int]]:
        with self.condition:
            if self.lease_release_attempted or self.lease_release_withheld:
                return False, {}
            if self.active_writers:
                return False, dict(sorted(self.active_writers.items()))
            self.lease_release_attempted = True
            self.lease_release_attempts_total += 1
            self.phase = "releasing_lease"
            self.condition.notify_all()
            return True, {}

    def finish_lease_release(self, outcome: str, elapsed: float) -> None:
        with self.condition:
            self.lease_release_outcomes[outcome] += 1
            self.lease_release_seconds = elapsed
            self.lease_release_succeeded = outcome != "failure"
            self.phase = "lease_released" if outcome != "failure" else "lease_release_failed"
            if outcome != "failure" and self.sigterm_monotonic is not None:
                self.sigterm_to_lease_release_seconds = max(
                    0.0,
                    time.monotonic() - self.sigterm_monotonic,
                )
                self.sigterm_release_observed = True
            self.condition.notify_all()

    def claim_non_writer_drain(self) -> bool:
        with self.condition:
            if self._drain_claimed:
                return False
            if self.phase not in {
                "lease_released",
                "lease_release_failed",
                "release_withheld",
            }:
                return False
            self._drain_claimed = True
            self.phase = "draining_non_writers"
            return True

    def finish_non_writer_drain(self, elapsed: float) -> None:
        with self.condition:
            self.non_writer_drain_seconds = elapsed
            self.non_writer_drains_total += 1
            self.phase = "complete"
            self.condition.notify_all()

    def snapshot(self) -> dict[str, Any]:
        with self.condition:
            return {
                "phase": self.phase,
                "active_writers": dict(self.active_writers),
                "shutdowns_total": self.shutdowns_total,
                "writer_quiescence_outcomes": dict(self.writer_quiescence_outcomes),
                "writer_quiescence_seconds": self.writer_quiescence_seconds,
                "lease_release_attempts_total": self.lease_release_attempts_total,
                "lease_release_outcomes": dict(self.lease_release_outcomes),
                "lease_release_seconds": self.lease_release_seconds,
                "lease_release_withheld": self.lease_release_withheld,
                "sigterm_to_lease_release_seconds": self.sigterm_to_lease_release_seconds,
                "sigterm_release_observed": self.sigterm_release_observed,
                "release_withheld_total": self.release_withheld_total,
                "non_writer_drain_seconds": self.non_writer_drain_seconds,
                "non_writer_drains_total": self.non_writer_drains_total,
            }


class _WriterOperationOwner(Protocol):
    def _writer_operation(self, component: str) -> AbstractContextManager[object]: ...


def ledger_writer_operation(component: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorate an entry point that can mutate the PRISM ledger."""

    def decorate(method: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(method)
        def guarded(self: _WriterOperationOwner, *args: Any, **kwargs: Any) -> Any:
            with self._writer_operation(component):
                return method(self, *args, **kwargs)

        return guarded

    return decorate
