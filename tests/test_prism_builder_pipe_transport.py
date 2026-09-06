"""Builder pipe transport: readiness waits in place of fixed polling (#236).

The --serve daemon transport and the one-shot writer drive non-blocking
pipes so cancellation, supersession and the absolute deadline are checked
between syscalls. They used to sleep a fixed 20 ms after every EAGAIN; a
readiness wait bounded by the deadline and the same check cadence now takes
its place. These tests pin the transport contract that must survive that
swap -- exact bytes and framing, exactly-once failure accounting, the
timeout/cancellation/supersession classifications, spool poisoning without
prefix replay, and selector cleanup on every path -- first with scripted
syscalls (deterministic EAGAIN/EINTR/spurious-readiness/short-write/EOF
sequences) and then over real pipes with a peer thread or subprocess.

No timing threshold here is tight enough to be a performance claim; the
throughput measurement lives in ``tests/perf/window_builder_pipe_latency.py``.
"""

from __future__ import annotations

import hashlib
import json
import os
import selectors
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable
from unittest.mock import patch

from lab.prism import bundle_compiler as compiler_module
from lab.prism.bundle_compiler import (
    PRISM_BUILDER_PIPE_READ_CHUNK_BYTES,
    PRISM_BUILDER_PIPE_WAIT_SLICE_SECONDS,
    PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS,
    BundleCompiler,
    _PipeReadinessWaiter,
    _ServeBuilderClient,
    _ServeBuilderUnavailable,
)
from tests.prism_coordinator_test_support import (
    FAKE_SERVE_BUILDER_COMMAND,
    PayoutLedgerArtifact,
    coordinator,
    spool_share,
)


FAR_DEADLINE_SECONDS = 30.0


class _Superseded(RuntimeError):
    """Stands in for JobBuildSuperseded."""


class _Cancelled(RuntimeError):
    """Stands in for the coordinator's cancellation error."""


class _Control:
    """Minimal BundleBuildControlPort."""

    def __init__(self) -> None:
        self.cancel_event = threading.Event()
        self.process = None


class _Cancellation:
    """Minimal CancellationPort backed by an event."""

    def __init__(self) -> None:
        self.event = threading.Event()
        self.phases: list[str] = []

    def is_set(self) -> bool:
        return self.event.is_set()

    def raise_if_cancelled(self, phase: str) -> None:
        self.phases.append(phase)
        if self.event.is_set():
            raise _Cancelled(phase)


class _Runtime:
    """Only the scheduler counters the transport helpers touch."""

    def __init__(self) -> None:
        self._job_build_scheduler_lock = threading.Lock()
        self.job_build_worker_counts = {
            "starts": 0,
            "restarts": 0,
            "crashes": 0,
            "terminations": 0,
        }
        self._job_build_worker_restart_pending = False


class _Serialization:
    """Only the spool-poisoning surface the splice helper touches."""

    def __init__(self) -> None:
        self.spool_failures = 0

    def mark_spool_failed(self) -> None:
        self.spool_failures += 1


class _PipeEnd:
    def __init__(self, file_descriptor: int) -> None:
        self._file_descriptor = file_descriptor

    def fileno(self) -> int:
        return self._file_descriptor


class _FakeClock:
    """A monotonic clock the scripted tests advance by hand."""

    def __init__(self, start: float = 1_000.0) -> None:
        self.now = start

    def monotonic(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds

    def sleep(self, seconds: float) -> None:
        self.advance(seconds)


class _ScriptedWaiter:
    """Drop-in for _PipeReadinessWaiter that never blocks.

    Records every wait with the deadline slack it was given (against the
    scripted fake clock) and answers readiness from a class-level script
    (True when the script is empty). Each wait may advance the fake clock,
    so a deadline is exhausted by construction rather than by real time
    passing: the transport loops can be walked through EAGAIN, spurious
    readiness and timeouts deterministically.
    """

    instances: list[_ScriptedWaiter] = []
    readiness: list[bool] = []
    clock: _FakeClock = _FakeClock()
    advance_per_wait: float = 0.0

    def __init__(self, file_descriptor: int, events: int) -> None:
        self.file_descriptor = file_descriptor
        self.events = events
        self.waits = 0
        self.slack: list[float] = []
        self.closed = 0
        type(self).instances.append(self)

    def __enter__(self) -> _ScriptedWaiter:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def wait(self, deadline: float) -> bool:
        self.waits += 1
        cls = type(self)
        self.slack.append(deadline - cls.clock.monotonic())
        cls.clock.advance(cls.advance_per_wait)
        script = cls.readiness
        if script:
            return script.pop(0)
        return True

    def close(self) -> None:
        self.closed += 1


class _RecordingWaiter(_PipeReadinessWaiter):
    """The real waiter, with its instances and waits observable."""

    instances: list[_RecordingWaiter] = []

    def __init__(self, file_descriptor: int, events: int) -> None:
        super().__init__(file_descriptor, events)
        self.closed = 0
        type(self).instances.append(self)

    def close(self) -> None:
        self.closed += 1
        super().close()


class _GatedWaiter(_RecordingWaiter):
    """The real waiter, releasing a held-back peer when the transport blocks.

    Tests that need the transport to block by construction hold their peer
    back until the transport has actually entered a wait: at that moment
    the pipe is empty (reads) or full (writes) whatever the scheduler did,
    so a wait-count assertion is not a timing assumption. The release is an
    in-process event for thread peers and a marker file for child processes.
    """

    instances: list[_GatedWaiter] = []
    released = threading.Event()
    marker: str | None = None

    @classmethod
    def arm(cls, marker: str | None = None) -> None:
        cls.instances = []
        cls.released = threading.Event()
        cls.marker = marker

    def wait(self, deadline: float) -> bool:
        gate = type(self)
        if not gate.released.is_set():
            if gate.marker is not None:
                Path(gate.marker).touch()
            gate.released.set()
        return super().wait(deadline)


class _PollFailingSelector:
    """A DefaultSelector stand-in that registers, then cannot poll.

    Models SelectSelector over a Windows pipe handle (registration succeeds,
    ``select`` raises), with a hook to spend fake time inside the failed
    poll. Lifecycle counts are class-level so a test can assert the
    selector was built and closed exactly once.
    """

    built = 0
    closes = 0
    selects: list[float | None] = []
    error: BaseException = OSError(10038, "not a socket")
    on_select: Callable[[], None] | None = None

    @classmethod
    def reset(
        cls,
        *,
        error: BaseException | None = None,
        on_select: Callable[[], None] | None = None,
    ) -> None:
        cls.built = 0
        cls.closes = 0
        cls.selects = []
        cls.error = OSError(10038, "not a socket") if error is None else error
        cls.on_select = on_select

    def __init__(self) -> None:
        type(self).built += 1
        self.registered: list[tuple[int, int]] = []

    def register(self, file_descriptor: int, events: int, data: Any = None) -> None:
        self.registered.append((file_descriptor, events))

    def select(self, timeout: float | None = None) -> list[Any]:
        cls = type(self)
        cls.selects.append(timeout)
        if cls.on_select is not None:
            cls.on_select()
        raise cls.error

    def close(self) -> None:
        type(self).closes += 1


class _Do:
    """A scripted syscall step: run a side effect, then answer or raise."""

    def __init__(self, effect: Callable[[], None], answer: Any) -> None:
        self.effect = effect
        self.answer = answer


class _SyscallScript:
    """Scripted os.read / os.write / os.splice answers for one descriptor.

    Answers are consumed in order: bytes (read) or an int (write/splice)
    are returned, an exception instance is raised, and a _Do runs its
    effect first. Every other descriptor reaches the real syscall.
    """

    def __init__(self, file_descriptor: int) -> None:
        self.file_descriptor = file_descriptor
        self.reads: list[Any] = []
        self.writes: list[Any] = []
        self.splices: list[Any] = []
        self.read_sizes: list[int] = []
        self.write_log: list[bytes] = []
        self.splice_log: list[tuple[int, int]] = []

    def _answer(self, script: list[Any], name: str) -> Any:
        if not script:
            raise AssertionError(f"unscripted os.{name} call")
        action = script.pop(0)
        if isinstance(action, _Do):
            action.effect()
            action = action.answer
        if isinstance(action, BaseException):
            raise action
        return action

    def install(self, case: unittest.TestCase) -> None:
        real_read = os.read
        real_write = os.write
        real_splice = getattr(os, "splice", None)

        def read(file_descriptor: int, size: int) -> bytes:
            if file_descriptor != self.file_descriptor:
                return real_read(file_descriptor, size)
            self.read_sizes.append(size)
            chunk = self._answer(self.reads, "read")
            if len(chunk) > size:
                raise AssertionError("scripted read exceeds the requested size")
            return chunk

        def write(file_descriptor: int, data: Any) -> int:
            if file_descriptor != self.file_descriptor:
                return real_write(file_descriptor, data)
            written = self._answer(self.writes, "write")
            self.write_log.append(bytes(data[:written]))
            return written

        def splice(src: int, dst: int, count: int, **kwargs: Any) -> int:
            if dst != self.file_descriptor:
                assert real_splice is not None
                return real_splice(src, dst, count, **kwargs)
            self.splice_log.append((int(kwargs.get("offset_src", -1)), count))
            return self._answer(self.splices, "splice")

        for name, replacement in (("read", read), ("write", write)):
            patcher = patch.object(os, name, replacement)
            patcher.start()
            case.addCleanup(patcher.stop)
        splice_patcher = patch.object(os, "splice", splice, create=True)
        splice_patcher.start()
        case.addCleanup(splice_patcher.stop)


class _TransportCase(unittest.TestCase):
    """Shared fixtures: a compiler over a counter-only runtime and pipes."""

    def setUp(self) -> None:
        self.runtime = _Runtime()
        self.compiler = BundleCompiler(
            self.runtime,  # type: ignore[arg-type]
            superseded_error=_Superseded,
            cancellation_error_types=(_Cancelled,),
            build_control_type=_Control,
        )
        self._open_fds: list[int] = []

    def tearDown(self) -> None:
        for file_descriptor in self._open_fds:
            try:
                os.close(file_descriptor)
            except OSError:
                pass

    def _close_fd(self, file_descriptor: int) -> None:
        try:
            os.close(file_descriptor)
        except OSError:
            pass
        if file_descriptor in self._open_fds:
            self._open_fds.remove(file_descriptor)

    def _client(self) -> tuple[_ServeBuilderClient, int, int]:
        """A client over two real pipes.

        Returns the client plus the peer ends: the descriptor a fake daemon
        writes its stdout into, and the one it reads its stdin from.
        """
        stdout_read, stdout_write = os.pipe()
        stdin_read, stdin_write = os.pipe()
        self._open_fds += [stdout_read, stdout_write, stdin_read, stdin_write]
        os.set_blocking(stdout_read, False)
        os.set_blocking(stdin_write, False)
        client = _ServeBuilderClient(
            process=SimpleNamespace(  # type: ignore[arg-type]
                stdin=_PipeEnd(stdin_write),
                stdout=_PipeEnd(stdout_read),
                poll=lambda: None,
            )
        )
        return client, stdout_write, stdin_read

    def _scripted(self) -> None:
        """Scripted waiter plus a hand-advanced clock for the module's deadlines."""
        self.clock = _FakeClock()
        _ScriptedWaiter.instances = []
        _ScriptedWaiter.readiness = []
        _ScriptedWaiter.clock = self.clock
        _ScriptedWaiter.advance_per_wait = 0.0
        for target, replacement in (
            ("_PipeReadinessWaiter", _ScriptedWaiter),
            (
                "time",
                SimpleNamespace(
                    monotonic=self.clock.monotonic,
                    sleep=self.clock.sleep,
                ),
            ),
        ):
            patcher = patch.object(compiler_module, target, replacement)
            patcher.start()
            self.addCleanup(patcher.stop)

    def _gated(self, marker: str | None = None) -> None:
        _GatedWaiter.arm(marker)
        patcher = patch.object(
            compiler_module,
            "_PipeReadinessWaiter",
            _GatedWaiter,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def _recording(self) -> None:
        _RecordingWaiter.instances = []
        patcher = patch.object(
            compiler_module,
            "_PipeReadinessWaiter",
            _RecordingWaiter,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def assertNoFailureAccounted(self) -> None:
        self.assertEqual(
            self.runtime.job_build_worker_counts,
            {"starts": 0, "restarts": 0, "crashes": 0, "terminations": 0},
        )
        self.assertFalse(self.runtime._job_build_worker_restart_pending)

    def assertOneCrashAccounted(self) -> None:
        self.assertEqual(self.runtime.job_build_worker_counts["crashes"], 1)
        self.assertEqual(self.runtime.job_build_worker_counts["terminations"], 0)
        self.assertTrue(self.runtime._job_build_worker_restart_pending)


class ReadinessWaiterTests(_TransportCase):
    """The helper itself: laziness, bounds, degradation and cleanup."""

    def test_wait_slice_matches_the_retired_polling_cadence(self) -> None:
        self.assertEqual(
            PRISM_BUILDER_PIPE_WAIT_SLICE_SECONDS,
            min(0.02, PRISM_TIP_REFRESH_ADMISSION_POLL_SECONDS),
        )

    def test_registration_is_lazy_and_close_is_idempotent(self) -> None:
        read_end, write_end = os.pipe()
        self._open_fds += [read_end, write_end]
        with patch.object(
            selectors,
            "DefaultSelector",
            side_effect=AssertionError("selector built without a wait"),
        ):
            waiter = _PipeReadinessWaiter(read_end, selectors.EVENT_READ)
            self.assertEqual(waiter.waits, 0)
            waiter.close()
            waiter.close()
        with waiter:
            pass

    def test_ready_descriptor_returns_true_without_sleeping(self) -> None:
        read_end, write_end = os.pipe()
        self._open_fds += [read_end, write_end]
        os.write(write_end, b"x")
        with _PipeReadinessWaiter(read_end, selectors.EVENT_READ) as waiter:
            started = time.monotonic()
            ready = waiter.wait(time.monotonic() + FAR_DEADLINE_SECONDS)
            elapsed = time.monotonic() - started
        self.assertTrue(ready)
        self.assertEqual(waiter.waits, 1)
        self.assertLess(elapsed, 1.0)

    def test_idle_descriptor_wakes_on_the_cadence_not_the_deadline(self) -> None:
        read_end, write_end = os.pipe()
        self._open_fds += [read_end, write_end]
        with _PipeReadinessWaiter(read_end, selectors.EVENT_READ) as waiter:
            started = time.monotonic()
            ready = waiter.wait(time.monotonic() + FAR_DEADLINE_SECONDS)
            elapsed = time.monotonic() - started
        self.assertFalse(ready)
        # Bounded by the cancellation-check slice (20 ms), not by the far
        # deadline; the upper bound is generous for a loaded runner.
        self.assertGreaterEqual(elapsed, PRISM_BUILDER_PIPE_WAIT_SLICE_SECONDS / 2)
        self.assertLess(elapsed, 1.0)

    def test_wait_is_bounded_by_the_remaining_deadline(self) -> None:
        read_end, write_end = os.pipe()
        self._open_fds += [read_end, write_end]
        with _PipeReadinessWaiter(read_end, selectors.EVENT_READ) as waiter:
            started = time.monotonic()
            ready = waiter.wait(time.monotonic() + 0.005)
            elapsed = time.monotonic() - started
        self.assertFalse(ready)
        self.assertLess(elapsed, 1.0)

    def test_exhausted_deadline_returns_false_without_a_selector(self) -> None:
        read_end, write_end = os.pipe()
        self._open_fds += [read_end, write_end]
        with patch.object(
            selectors,
            "DefaultSelector",
            side_effect=AssertionError("selector built for a dead deadline"),
        ), patch.object(compiler_module.time, "sleep") as sleep:
            with _PipeReadinessWaiter(read_end, selectors.EVENT_READ) as waiter:
                self.assertFalse(waiter.wait(time.monotonic() - 1.0))
        self.assertEqual(waiter.waits, 1)
        sleep.assert_not_called()

    def test_unwatchable_descriptor_degrades_to_one_bounded_sleep(self) -> None:
        class _Refusing:
            built = 0

            def __init__(self) -> None:
                type(self).built += 1

            def register(self, *args: object) -> None:
                raise PermissionError("regular files cannot be polled")

            def close(self) -> None:
                pass

        read_end, write_end = os.pipe()
        self._open_fds += [read_end, write_end]
        with patch.object(selectors, "DefaultSelector", _Refusing), patch.object(
            compiler_module.time,
            "sleep",
        ) as sleep:
            with _PipeReadinessWaiter(read_end, selectors.EVENT_READ) as waiter:
                self.assertFalse(waiter.wait(time.monotonic() + FAR_DEADLINE_SECONDS))
                self.assertFalse(waiter.wait(time.monotonic() + FAR_DEADLINE_SECONDS))
        self.assertEqual(_Refusing.built, 1)
        self.assertEqual(sleep.call_count, 2)
        for call in sleep.call_args_list:
            self.assertLessEqual(call.args[0], PRISM_BUILDER_PIPE_WAIT_SLICE_SECONDS)
            self.assertGreater(call.args[0], 0.0)

    def test_close_releases_the_registration(self) -> None:
        closed: list[object] = []
        real_selector = selectors.DefaultSelector

        class _Observed(real_selector):  # type: ignore[misc,valid-type]
            def close(self) -> None:
                closed.append(self)
                super().close()

        read_end, write_end = os.pipe()
        self._open_fds += [read_end, write_end]
        with patch.object(selectors, "DefaultSelector", _Observed):
            waiter = _PipeReadinessWaiter(read_end, selectors.EVENT_READ)
            waiter.wait(time.monotonic() + 0.001)
            self.assertEqual(closed, [])
            waiter.close()
        self.assertEqual(len(closed), 1)
        self.assertIsNone(waiter._selector)

    def _fake_clock_module(self) -> tuple[_FakeClock, list[float]]:
        """Drive the waiter's clock and record its sleeps."""
        clock = _FakeClock()
        sleeps: list[float] = []

        def sleep(seconds: float) -> None:
            sleeps.append(seconds)
            clock.advance(seconds)

        patcher = patch.object(
            compiler_module,
            "time",
            SimpleNamespace(monotonic=clock.monotonic, sleep=sleep),
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        return clock, sleeps

    def test_poll_failure_detaches_once_and_falls_back_to_bounded_sleep(self) -> None:
        read_end, write_end = os.pipe()
        self._open_fds += [read_end, write_end]
        clock, sleeps = self._fake_clock_module()
        _PollFailingSelector.reset()
        with patch.object(selectors, "DefaultSelector", _PollFailingSelector):
            waiter = _PipeReadinessWaiter(read_end, selectors.EVENT_READ)
            first = waiter.wait(clock.now + FAR_DEADLINE_SECONDS)
            second = waiter.wait(clock.now + FAR_DEADLINE_SECONDS)
            waiter.close()
            waiter.close()

        self.assertEqual((first, second), (False, False))
        self.assertEqual(waiter.waits, 2)
        # Built and polled once; released exactly once at the failed poll,
        # never rebuilt for the next wait, and not closed again on exit.
        self.assertEqual(_PollFailingSelector.built, 1)
        self.assertEqual(len(_PollFailingSelector.selects), 1)
        self.assertEqual(_PollFailingSelector.closes, 1)
        self.assertIsNone(waiter._selector)
        self.assertTrue(waiter._unwatchable)
        # The failed poll spent no clock, so the first wait sleeps its whole
        # slice; the second, unwatchable, sleeps one full slice too.
        self.assertEqual(
            [round(seconds, 6) for seconds in sleeps],
            [PRISM_BUILDER_PIPE_WAIT_SLICE_SECONDS] * 2,
        )

    def test_poll_failure_sleeps_only_the_remainder_of_the_slice(self) -> None:
        read_end, write_end = os.pipe()
        self._open_fds += [read_end, write_end]
        clock, sleeps = self._fake_clock_module()
        spent = 0.005
        _PollFailingSelector.reset(on_select=lambda: clock.advance(spent))
        with patch.object(selectors, "DefaultSelector", _PollFailingSelector):
            with _PipeReadinessWaiter(read_end, selectors.EVENT_READ) as waiter:
                ready = waiter.wait(clock.now + FAR_DEADLINE_SECONDS)
                after_first = clock.now
                again = waiter.wait(clock.now + FAR_DEADLINE_SECONDS)

        self.assertEqual((ready, again), (False, False))
        # The failed poll consumed 5 ms of the 20 ms slice: the fallback
        # sleeps the 15 ms left, so the wait as a whole still spans exactly
        # one slice; the next (unwatchable) wait sleeps one full slice.
        self.assertEqual(
            [round(seconds, 6) for seconds in sleeps],
            [
                round(PRISM_BUILDER_PIPE_WAIT_SLICE_SECONDS - spent, 6),
                PRISM_BUILDER_PIPE_WAIT_SLICE_SECONDS,
            ],
        )
        self.assertEqual(round(after_first - 1_000.0, 6), PRISM_BUILDER_PIPE_WAIT_SLICE_SECONDS)
        self.assertEqual(_PollFailingSelector.built, 1)
        self.assertEqual(_PollFailingSelector.closes, 1)

    def test_poll_failure_after_spending_the_slice_sleeps_no_further(self) -> None:
        read_end, write_end = os.pipe()
        self._open_fds += [read_end, write_end]
        clock, sleeps = self._fake_clock_module()
        _PollFailingSelector.reset(
            on_select=lambda: clock.advance(PRISM_BUILDER_PIPE_WAIT_SLICE_SECONDS * 3)
        )
        with patch.object(selectors, "DefaultSelector", _PollFailingSelector):
            with _PipeReadinessWaiter(read_end, selectors.EVENT_READ) as waiter:
                ready = waiter.wait(clock.now + FAR_DEADLINE_SECONDS)

        self.assertFalse(ready)
        self.assertEqual(sleeps, [])
        self.assertEqual(_PollFailingSelector.closes, 1)
        self.assertTrue(waiter._unwatchable)

    def test_poll_failure_never_overruns_the_absolute_deadline(self) -> None:
        read_end, write_end = os.pipe()
        self._open_fds += [read_end, write_end]
        clock, sleeps = self._fake_clock_module()
        _PollFailingSelector.reset(on_select=lambda: clock.advance(0.004))
        with patch.object(selectors, "DefaultSelector", _PollFailingSelector):
            with _PipeReadinessWaiter(read_end, selectors.EVENT_READ) as waiter:
                deadline = clock.now + 0.010
                ready = waiter.wait(deadline)
                overshoot = clock.now - deadline

        self.assertFalse(ready)
        # 10 ms of budget, 4 ms spent by the failed poll: the fallback sleeps
        # the 6 ms to the deadline, not the 16 ms left of the slice.
        self.assertEqual([round(seconds, 6) for seconds in sleeps], [0.006])
        self.assertLessEqual(round(overshoot, 9), 0.0)

    def test_poll_failure_with_a_spent_deadline_returns_without_sleeping(self) -> None:
        read_end, write_end = os.pipe()
        self._open_fds += [read_end, write_end]
        clock, sleeps = self._fake_clock_module()
        _PollFailingSelector.reset(on_select=lambda: clock.advance(0.050))
        with patch.object(selectors, "DefaultSelector", _PollFailingSelector):
            with _PipeReadinessWaiter(read_end, selectors.EVENT_READ) as waiter:
                ready = waiter.wait(clock.now + 0.010)
                # Subsequent waits against a dead deadline stay free too.
                later = waiter.wait(clock.now - 1.0)

        self.assertEqual((ready, later), (False, False))
        self.assertEqual(sleeps, [])
        self.assertEqual(_PollFailingSelector.built, 1)
        self.assertEqual(_PollFailingSelector.closes, 1)

    def test_interrupted_poll_is_a_plain_wakeup_and_keeps_the_selector(self) -> None:
        read_end, write_end = os.pipe()
        self._open_fds += [read_end, write_end]
        _PollFailingSelector.reset(error=InterruptedError())
        with patch.object(selectors, "DefaultSelector", _PollFailingSelector), patch.object(
            compiler_module.time,
            "sleep",
        ) as sleep:
            waiter = _PipeReadinessWaiter(read_end, selectors.EVENT_READ)
            first = waiter.wait(time.monotonic() + FAR_DEADLINE_SECONDS)
            second = waiter.wait(time.monotonic() + FAR_DEADLINE_SECONDS)
            self.assertEqual(_PollFailingSelector.closes, 0)
            waiter.close()

        self.assertEqual((first, second), (False, False))
        # EINTR is not a fault: no sleep, the same selector polled again,
        # released only by close().
        sleep.assert_not_called()
        self.assertEqual(_PollFailingSelector.built, 1)
        self.assertEqual(len(_PollFailingSelector.selects), 2)
        self.assertEqual(_PollFailingSelector.closes, 1)
        self.assertFalse(waiter._unwatchable)


class UnpollableDescriptorTransportTests(_TransportCase):
    """A transport helper over a descriptor the selector cannot poll."""

    def setUp(self) -> None:
        super().setUp()
        self._recording()
        self.client, _stdout_write, _stdin_read = self._client()
        self.script = _SyscallScript(self.client.process.stdout.fileno())
        self.script.install(self)
        self.clock = _FakeClock()
        self.sleeps: list[float] = []

        def sleep(seconds: float) -> None:
            self.sleeps.append(seconds)
            self.clock.advance(seconds)

        for target, replacement in (
            ("time", SimpleNamespace(monotonic=self.clock.monotonic, sleep=sleep)),
        ):
            patcher = patch.object(compiler_module, target, replacement)
            patcher.start()
            self.addCleanup(patcher.stop)
        selector_patch = patch.object(selectors, "DefaultSelector", _PollFailingSelector)
        selector_patch.start()
        self.addCleanup(selector_patch.stop)

    def test_read_line_completes_through_the_bounded_sleep_fallback(self) -> None:
        _PollFailingSelector.reset()
        self.script.reads = [BlockingIOError(), BlockingIOError(), b"ok\n"]

        line = self.compiler._serve_builder_read_line(
            self.client,
            self.clock.now + FAR_DEADLINE_SECONDS,
            None,
            None,
        )

        self.assertEqual(line, b"ok")
        self.assertEqual(self.script.reads, [])
        # First EAGAIN: the poll fails and the wait finishes on the sleep;
        # second EAGAIN: unwatchable, straight to the sleep. One selector,
        # released once at the failed poll, nothing left for close().
        self.assertEqual(
            [round(seconds, 6) for seconds in self.sleeps],
            [PRISM_BUILDER_PIPE_WAIT_SLICE_SECONDS] * 2,
        )
        self.assertEqual(_PollFailingSelector.built, 1)
        self.assertEqual(len(_PollFailingSelector.selects), 1)
        self.assertEqual(_PollFailingSelector.closes, 1)
        (waiter,) = _RecordingWaiter.instances
        self.assertEqual(waiter.waits, 2)
        self.assertEqual(waiter.closed, 1)
        self.assertIsNone(waiter._selector)
        self.assertNoFailureAccounted()

    def test_failed_poll_that_spends_the_deadline_times_out_without_sleeping(self) -> None:
        _PollFailingSelector.reset(on_select=lambda: self.clock.advance(0.050))
        self.script.reads = [BlockingIOError(), b"never\n"]

        with self.assertRaisesRegex(_ServeBuilderUnavailable, "timed out"):
            self.compiler._serve_builder_read_line(
                self.client,
                self.clock.now + 0.010,
                None,
                None,
            )

        self.assertEqual(self.sleeps, [])
        self.assertEqual(self.script.reads, [b"never\n"])
        self.assertEqual(_PollFailingSelector.closes, 1)
        (waiter,) = _RecordingWaiter.instances
        self.assertEqual(waiter.waits, 1)
        self.assertEqual(waiter.closed, 1)
        self.assertNoFailureAccounted()


class ScriptedReadLineTests(_TransportCase):
    """Envelope line reads walked through scripted syscalls."""

    def setUp(self) -> None:
        super().setUp()
        self._scripted()
        self.client, _stdout_write, _stdin_read = self._client()
        self.script = _SyscallScript(self.client.process.stdout.fileno())
        self.script.install(self)
        self.deadline = self.clock.now + FAR_DEADLINE_SECONDS

    def _read_line(self, cancellation=None, control=None) -> bytes:
        return self.compiler._serve_builder_read_line(
            self.client,
            self.deadline,
            cancellation,
            control,
        )

    def test_eagain_waits_once_then_returns_line_and_keeps_tail(self) -> None:
        self.script.reads = [BlockingIOError(), b'{"ok":true}\nTAIL']

        line = self._read_line()

        self.assertEqual(line, b'{"ok":true}')
        self.assertEqual(bytes(self.client.stdout_buffer), b"TAIL")
        self.assertEqual(self.script.reads, [])
        (waiter,) = _ScriptedWaiter.instances
        self.assertEqual(waiter.events, selectors.EVENT_READ)
        self.assertEqual(waiter.file_descriptor, self.script.file_descriptor)
        self.assertEqual(waiter.waits, 1)
        self.assertEqual(waiter.closed, 1)
        self.assertEqual(waiter.slack, [FAR_DEADLINE_SECONDS])
        self.assertEqual(self.script.read_sizes, [PRISM_BUILDER_PIPE_READ_CHUNK_BYTES] * 2)
        self.assertNoFailureAccounted()

    def test_line_already_buffered_takes_no_syscall_and_no_wait(self) -> None:
        self.client.stdout_buffer += b"first\nsecond"

        self.assertEqual(self._read_line(), b"first")

        self.assertEqual(bytes(self.client.stdout_buffer), b"second")
        self.assertEqual(self.script.read_sizes, [])
        (waiter,) = _ScriptedWaiter.instances
        self.assertEqual(waiter.waits, 0)
        self.assertEqual(waiter.closed, 1)

    def test_line_assembled_across_chunks(self) -> None:
        self.script.reads = [b"ab", BlockingIOError(), b"cd", b"ef\n"]

        self.assertEqual(self._read_line(), b"abcdef")

        self.assertEqual(bytes(self.client.stdout_buffer), b"")
        self.assertEqual(_ScriptedWaiter.instances[0].waits, 1)

    def test_eintr_retries_immediately_without_waiting(self) -> None:
        self.script.reads = [InterruptedError(), InterruptedError(), b"line\n"]

        self.assertEqual(self._read_line(), b"line")

        (waiter,) = _ScriptedWaiter.instances
        self.assertEqual(waiter.waits, 0)
        self.assertEqual(waiter.closed, 1)

    def test_spurious_readiness_retries_and_waits_again(self) -> None:
        self.script.reads = [BlockingIOError(), BlockingIOError(), b"x\n"]
        _ScriptedWaiter.readiness = [True, True]

        self.assertEqual(self._read_line(), b"x")

        self.assertEqual(_ScriptedWaiter.instances[0].waits, 2)

    def test_cadence_wakeup_without_readiness_retries(self) -> None:
        self.script.reads = [BlockingIOError(), BlockingIOError(), b"x\n"]
        _ScriptedWaiter.readiness = [False, False]

        self.assertEqual(self._read_line(), b"x")

        self.assertEqual(_ScriptedWaiter.instances[0].waits, 2)

    def test_eof_mid_frame_is_one_crash(self) -> None:
        self.script.reads = [b"partial", b""]

        with self.assertRaisesRegex(_ServeBuilderUnavailable, "exited mid-request"):
            self._read_line()

        self.assertOneCrashAccounted()
        self.assertEqual(_ScriptedWaiter.instances[0].closed, 1)

    def test_eof_after_supersession_is_superseded_not_a_crash(self) -> None:
        control = _Control()
        self.script.reads = [
            b"partial",
            _Do(control.cancel_event.set, b""),
        ]

        with self.assertRaisesRegex(_Superseded, "terminated after supersession"):
            self._read_line(control=control)

        self.assertNoFailureAccounted()
        self.assertEqual(_ScriptedWaiter.instances[0].closed, 1)

    def test_deadline_while_blocked_times_out_without_accounting(self) -> None:
        self.deadline = self.clock.now + 1.0
        self.script.reads = [BlockingIOError()] * 3
        _ScriptedWaiter.readiness = [False, False]
        _ScriptedWaiter.advance_per_wait = 0.6

        with self.assertRaisesRegex(_ServeBuilderUnavailable, "timed out"):
            self._read_line()

        self.assertNoFailureAccounted()
        (waiter,) = _ScriptedWaiter.instances
        # Two waits, each handed the true remaining slack: the second
        # exhausts the deadline and the loop's own check raises before any
        # further syscall.
        self.assertEqual(waiter.waits, 2)
        self.assertEqual([round(slack, 6) for slack in waiter.slack], [1.0, 0.4])
        self.assertEqual(len(self.script.reads), 1)
        self.assertEqual(waiter.closed, 1)

    def test_cancellation_while_blocked_raises_and_cleans_up(self) -> None:
        cancellation = _Cancellation()
        self.script.reads = [
            _Do(cancellation.event.set, BlockingIOError()),
            b"never\n",
        ]

        with self.assertRaises(_Cancelled):
            self._read_line(cancellation=cancellation)

        self.assertEqual(self.script.reads, [b"never\n"])
        self.assertNoFailureAccounted()
        self.assertEqual(_ScriptedWaiter.instances[0].closed, 1)

    def test_supersession_while_blocked_raises_and_cleans_up(self) -> None:
        control = _Control()
        self.script.reads = [
            _Do(control.cancel_event.set, BlockingIOError()),
            b"never\n",
        ]

        with self.assertRaisesRegex(_Superseded, "canceled after supersession"):
            self._read_line(control=control)

        self.assertEqual(self.script.reads, [b"never\n"])
        self.assertNoFailureAccounted()
        self.assertEqual(_ScriptedWaiter.instances[0].closed, 1)

    def test_read_error_is_a_daemon_anomaly(self) -> None:
        self.script.reads = [OSError(5, "io error")]

        with self.assertRaisesRegex(_ServeBuilderUnavailable, "read failed"):
            self._read_line()

        self.assertNoFailureAccounted()
        self.assertEqual(_ScriptedWaiter.instances[0].closed, 1)


class ScriptedReadExactTests(_TransportCase):
    """Raw-section reads walked through scripted syscalls."""

    def setUp(self) -> None:
        super().setUp()
        self._scripted()
        self.client, _stdout_write, _stdin_read = self._client()
        self.script = _SyscallScript(self.client.process.stdout.fileno())
        self.script.install(self)
        self.deadline = self.clock.now + FAR_DEADLINE_SECONDS

    def _read_exact(self, byte_count: int, cancellation=None, control=None) -> bytes:
        return self.compiler._serve_builder_read_exact(
            self.client,
            byte_count,
            self.deadline,
            cancellation,
            control,
        )

    def test_drains_buffer_then_reads_exactly_across_short_reads(self) -> None:
        self.client.stdout_buffer += b"ABC"
        self.script.reads = [b"DE", BlockingIOError(), b"FGH", b"UNREAD"]

        self.assertEqual(self._read_exact(8), b"ABCDEFGH")

        self.assertEqual(bytes(self.client.stdout_buffer), b"")
        self.assertEqual(self.script.reads, [b"UNREAD"])
        # Never asks the pipe for more than the frame still owes.
        self.assertEqual(self.script.read_sizes, [5, 3, 3])
        (waiter,) = _ScriptedWaiter.instances
        self.assertEqual(waiter.waits, 1)
        self.assertEqual(waiter.closed, 1)
        self.assertNoFailureAccounted()

    def test_satisfied_from_buffer_leaves_the_rest_for_the_next_frame(self) -> None:
        self.client.stdout_buffer += b"RAW\nnext-line\n"

        self.assertEqual(self._read_exact(3), b"RAW")
        self.assertEqual(self._read_exact(1), b"\n")

        self.assertEqual(bytes(self.client.stdout_buffer), b"next-line\n")
        self.assertEqual(self.script.read_sizes, [])
        self.assertTrue(all(w.waits == 0 for w in _ScriptedWaiter.instances))
        self.assertTrue(all(w.closed == 1 for w in _ScriptedWaiter.instances))

    def test_large_frame_reads_in_pipe_sized_chunks(self) -> None:
        payload = bytes(range(256)) * 1024  # 256 KiB
        chunk = PRISM_BUILDER_PIPE_READ_CHUNK_BYTES
        self.script.reads = [
            payload[:chunk],
            BlockingIOError(),
            payload[chunk : 2 * chunk],
            payload[2 * chunk : 2 * chunk + 10],
            payload[2 * chunk + 10 : 3 * chunk],
            payload[3 * chunk :],
        ]

        self.assertEqual(self._read_exact(len(payload)), payload)

        # Every read asks for one pipe capacity while more than that is
        # owed, whatever the pipe actually delivered last time.
        self.assertEqual(self.script.read_sizes, [chunk] * 6)
        self.assertEqual(_ScriptedWaiter.instances[0].waits, 1)

    def test_zero_length_frame_returns_empty_without_io(self) -> None:
        self.assertEqual(self._read_exact(0), b"")
        self.assertEqual(self.script.read_sizes, [])

    def test_eof_mid_frame_is_one_crash(self) -> None:
        self.script.reads = [b"12", b""]

        with self.assertRaisesRegex(_ServeBuilderUnavailable, "exited mid-request"):
            self._read_exact(4)

        self.assertOneCrashAccounted()
        self.assertEqual(_ScriptedWaiter.instances[0].closed, 1)

    def test_eintr_and_spurious_readiness(self) -> None:
        self.script.reads = [
            InterruptedError(),
            BlockingIOError(),
            BlockingIOError(),
            b"ok",
        ]
        _ScriptedWaiter.readiness = [True, False]

        self.assertEqual(self._read_exact(2), b"ok")

        self.assertEqual(_ScriptedWaiter.instances[0].waits, 2)

    def test_deadline_times_out_without_accounting(self) -> None:
        self.deadline = self.clock.now + 1.0
        self.script.reads = [BlockingIOError()] * 3
        _ScriptedWaiter.readiness = [False, False]
        _ScriptedWaiter.advance_per_wait = 0.6

        with self.assertRaisesRegex(_ServeBuilderUnavailable, "timed out"):
            self._read_exact(1)

        self.assertNoFailureAccounted()
        (waiter,) = _ScriptedWaiter.instances
        self.assertEqual(waiter.waits, 2)
        self.assertEqual([round(slack, 6) for slack in waiter.slack], [1.0, 0.4])
        self.assertEqual(len(self.script.reads), 1)
        self.assertEqual(waiter.closed, 1)

    def test_cancellation_and_supersession_while_blocked(self) -> None:
        cancellation = _Cancellation()
        self.script.reads = [_Do(cancellation.event.set, BlockingIOError())]
        with self.assertRaises(_Cancelled):
            self._read_exact(1, cancellation=cancellation)

        control = _Control()
        self.script.reads = [_Do(control.cancel_event.set, BlockingIOError())]
        with self.assertRaisesRegex(_Superseded, "canceled after supersession"):
            self._read_exact(1, control=control)

        self.assertNoFailureAccounted()
        self.assertTrue(all(w.closed == 1 for w in _ScriptedWaiter.instances))


class ScriptedWriteTests(_TransportCase):
    """Request writes walked through scripted syscalls."""

    def setUp(self) -> None:
        super().setUp()
        self._scripted()
        self.client, _stdout_write, _stdin_read = self._client()
        self.script = _SyscallScript(self.client.process.stdin.fileno())
        self.script.install(self)
        self.deadline = self.clock.now + FAR_DEADLINE_SECONDS

    def _write(self, data: bytes, cancellation=None, control=None) -> int:
        return self.compiler._serve_builder_write(
            self.client,
            data,
            self.deadline,
            cancellation,
            control,
        )

    def test_short_writes_and_eagain_deliver_every_byte_once_in_order(self) -> None:
        self.script.writes = [3, BlockingIOError(), InterruptedError(), 4, 3]

        self.assertEqual(self._write(b"0123456789"), 10)

        self.assertEqual(b"".join(self.script.write_log), b"0123456789")
        self.assertEqual(self.script.write_log, [b"012", b"3456", b"789"])
        (waiter,) = _ScriptedWaiter.instances
        self.assertEqual(waiter.events, selectors.EVENT_WRITE)
        self.assertEqual(waiter.file_descriptor, self.script.file_descriptor)
        self.assertEqual(waiter.waits, 1)
        self.assertEqual(waiter.closed, 1)
        self.assertNoFailureAccounted()

    def test_empty_write_takes_no_syscall(self) -> None:
        self.assertEqual(self._write(b""), 0)
        self.assertEqual(self.script.write_log, [])

    def test_broken_pipe_is_closed_input(self) -> None:
        self.script.writes = [2, BrokenPipeError()]

        with self.assertRaisesRegex(_ServeBuilderUnavailable, "input pipe closed"):
            self._write(b"abcd")

        self.assertEqual(self.script.write_log, [b"ab"])
        self.assertNoFailureAccounted()
        self.assertEqual(_ScriptedWaiter.instances[0].closed, 1)

    def test_broken_pipe_after_supersession_is_superseded(self) -> None:
        control = _Control()
        self.script.writes = [_Do(control.cancel_event.set, BrokenPipeError())]

        with self.assertRaisesRegex(_Superseded, "terminated after supersession"):
            self._write(b"abcd", control=control)

        self.assertNoFailureAccounted()

    def test_zero_progress_is_closed_input(self) -> None:
        self.script.writes = [0]

        with self.assertRaisesRegex(_ServeBuilderUnavailable, "input pipe closed"):
            self._write(b"abcd")

        self.assertNoFailureAccounted()

    def test_write_error_is_a_daemon_anomaly(self) -> None:
        self.script.writes = [OSError(5, "io error")]

        with self.assertRaisesRegex(_ServeBuilderUnavailable, "write failed"):
            self._write(b"abcd")

        self.assertNoFailureAccounted()

    def test_deadline_times_out_without_accounting(self) -> None:
        self.deadline = self.clock.now + 1.0
        self.script.writes = [BlockingIOError()] * 3
        _ScriptedWaiter.readiness = [False, False]
        _ScriptedWaiter.advance_per_wait = 0.6

        with self.assertRaisesRegex(_ServeBuilderUnavailable, "timed out"):
            self._write(b"abcd")

        self.assertNoFailureAccounted()
        (waiter,) = _ScriptedWaiter.instances
        self.assertEqual(waiter.waits, 2)
        self.assertEqual([round(slack, 6) for slack in waiter.slack], [1.0, 0.4])
        self.assertEqual(len(self.script.writes), 1)
        self.assertEqual(self.script.write_log, [])
        self.assertEqual(waiter.closed, 1)

    def test_cancellation_and_supersession_while_blocked(self) -> None:
        cancellation = _Cancellation()
        self.script.writes = [_Do(cancellation.event.set, BlockingIOError())]
        with self.assertRaises(_Cancelled):
            self._write(b"abcd", cancellation=cancellation)

        control = _Control()
        self.script.writes = [_Do(control.cancel_event.set, BlockingIOError())]
        with self.assertRaisesRegex(_Superseded, "canceled after supersession"):
            self._write(b"abcd", control=control)

        self.assertNoFailureAccounted()
        self.assertTrue(all(w.closed == 1 for w in _ScriptedWaiter.instances))


class ScriptedSpliceTests(_TransportCase):
    """Spool splices walked through scripted syscalls."""

    def setUp(self) -> None:
        super().setUp()
        self._scripted()
        self.client, _stdout_write, _stdin_read = self._client()
        self.script = _SyscallScript(self.client.process.stdin.fileno())
        self.script.install(self)
        self.deadline = self.clock.now + FAR_DEADLINE_SECONDS
        self.serialization = _Serialization()
        self.spool = tempfile.TemporaryFile()
        self.addCleanup(self.spool.close)

    def _splice(self, spool_size: int, cancellation=None, control=None) -> int:
        return self.compiler._serve_builder_splice_spool(
            self.client,
            self.serialization,  # type: ignore[arg-type]
            self.spool,
            spool_size,
            self.deadline,
            cancellation,
            control,
        )

    def test_eagain_waits_for_the_pipe_and_resumes_from_the_exact_offset(self) -> None:
        self.script.splices = [4, BlockingIOError(), InterruptedError(), 6]

        self.assertEqual(self._splice(10), 10)

        self.assertEqual(
            self.script.splice_log,
            [(0, 10), (4, 6), (4, 6), (4, 6)],
        )
        (waiter,) = _ScriptedWaiter.instances
        self.assertEqual(waiter.events, selectors.EVENT_WRITE)
        self.assertEqual(waiter.file_descriptor, self.script.file_descriptor)
        self.assertEqual(waiter.waits, 1)
        self.assertEqual(waiter.closed, 1)
        self.assertEqual(self.serialization.spool_failures, 0)
        self.assertNoFailureAccounted()

    def test_failure_after_partial_progress_poisons_spool_and_never_replays(self) -> None:
        self.script.splices = [4, OSError(22, "unsupported")]

        with self.assertRaisesRegex(_ServeBuilderUnavailable, "spool transfer failed"):
            self._splice(10)

        self.assertEqual(self.script.splice_log, [(0, 10), (4, 6)])
        self.assertEqual(self.script.write_log, [])
        self.assertEqual(self.serialization.spool_failures, 1)
        self.assertNoFailureAccounted()
        self.assertEqual(_ScriptedWaiter.instances[0].closed, 1)

    def test_broken_pipe_is_closed_input_and_keeps_the_spool(self) -> None:
        self.script.splices = [BrokenPipeError()]

        with self.assertRaisesRegex(_ServeBuilderUnavailable, "input pipe closed"):
            self._splice(10)

        self.assertEqual(self.serialization.spool_failures, 0)
        self.assertNoFailureAccounted()

    def test_errors_after_supersession_are_superseded(self) -> None:
        control = _Control()
        self.script.splices = [_Do(control.cancel_event.set, BrokenPipeError())]
        with self.assertRaisesRegex(_Superseded, "terminated after supersession"):
            self._splice(10, control=control)

        control = _Control()
        self.script.splices = [_Do(control.cancel_event.set, OSError(5, "io"))]
        with self.assertRaisesRegex(_Superseded, "terminated after supersession"):
            self._splice(10, control=control)

        self.assertEqual(self.serialization.spool_failures, 0)
        self.assertNoFailureAccounted()

    def test_zero_progress_is_closed_input(self) -> None:
        self.script.splices = [0]

        with self.assertRaisesRegex(_ServeBuilderUnavailable, "input pipe closed"):
            self._splice(10)

    def test_deadline_times_out_without_accounting(self) -> None:
        self.deadline = self.clock.now + 1.0
        self.script.splices = [BlockingIOError()] * 3
        _ScriptedWaiter.readiness = [False, False]
        _ScriptedWaiter.advance_per_wait = 0.6

        with self.assertRaisesRegex(_ServeBuilderUnavailable, "timed out"):
            self._splice(10)

        self.assertNoFailureAccounted()
        self.assertEqual(self.serialization.spool_failures, 0)
        (waiter,) = _ScriptedWaiter.instances
        self.assertEqual(waiter.waits, 2)
        self.assertEqual([round(slack, 6) for slack in waiter.slack], [1.0, 0.4])
        self.assertEqual(len(self.script.splices), 1)
        self.assertEqual(waiter.closed, 1)

    def test_cancellation_while_blocked(self) -> None:
        cancellation = _Cancellation()
        self.script.splices = [_Do(cancellation.event.set, BlockingIOError())]

        with self.assertRaises(_Cancelled):
            self._splice(10, cancellation=cancellation)

        self.assertNoFailureAccounted()


def _slow_reader(file_descriptor: int, sink: bytearray, *, chunk: int, pause: float) -> None:
    """Drain a blocking pipe end slowly so the writer side fills the pipe."""
    while True:
        try:
            data = os.read(file_descriptor, chunk)
        except OSError:
            break
        if not data:
            break
        sink += data
        time.sleep(pause)


def _bursty_writer(file_descriptor: int, payload: bytes, *, burst: int, pause: float) -> None:
    """Write a payload in paced bursts, then close the pipe end."""
    view = memoryview(payload)
    try:
        while view:
            written = os.write(file_descriptor, view[:burst])
            view = view[written:]
            time.sleep(pause)
    finally:
        os.close(file_descriptor)


class RealPipeTransportTests(_TransportCase):
    """The transport over real pipes with a peer thread."""

    def setUp(self) -> None:
        super().setUp()
        self._recording()
        self.client, self.stdout_write, self.stdin_read = self._client()
        self.deadline = time.monotonic() + FAR_DEADLINE_SECONDS
        self.threads: list[threading.Thread] = []

    def tearDown(self) -> None:
        for thread in self.threads:
            thread.join(10.0)
        super().tearDown()

    def _start(self, target: Callable[[], None]) -> None:
        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        self.threads.append(thread)

    def test_read_exact_streams_a_multi_megabyte_frame_exactly(self) -> None:
        self._gated()
        payload = os.urandom(4 << 20)
        stdout_write = self.stdout_write
        self._open_fds.remove(stdout_write)

        def writer() -> None:
            # Nothing is written until the reader has blocked on the empty
            # pipe, so the first read meets EAGAIN by construction.
            _GatedWaiter.released.wait(10.0)
            _bursty_writer(stdout_write, payload, burst=256 << 10, pause=0.0)

        self._start(writer)

        received = self.compiler._serve_builder_read_exact(
            self.client,
            len(payload),
            self.deadline,
            None,
            None,
        )

        self.assertEqual(len(received), len(payload))
        self.assertEqual(
            hashlib.sha256(received).hexdigest(),
            hashlib.sha256(payload).hexdigest(),
        )
        self.assertEqual(bytes(self.client.stdout_buffer), b"")
        (waiter,) = _GatedWaiter.instances
        self.assertTrue(_GatedWaiter.released.is_set())
        self.assertGreaterEqual(waiter.waits, 1)
        self.assertEqual(waiter.closed, 1)
        self.assertNoFailureAccounted()

    def test_line_then_raw_section_then_line_keeps_framing(self) -> None:
        raw = os.urandom(200_000)
        envelope = json.dumps({"ok": True, "window_items_len": len(raw)}).encode()
        stream = envelope + b"\n" + raw + b"\n" + b'{"ok":true,"next":1}\n'
        stdout_write = self.stdout_write
        self._open_fds.remove(stdout_write)
        self._start(lambda: _bursty_writer(stdout_write, stream, burst=7_000, pause=0.0))

        line = self.compiler._serve_builder_read_line(
            self.client, self.deadline, None, None
        )
        self.assertEqual(json.loads(line)["window_items_len"], len(raw))
        section = self.compiler._serve_builder_read_exact(
            self.client, len(raw), self.deadline, None, None
        )
        self.assertEqual(section, raw)
        self.assertEqual(
            self.compiler._serve_builder_read_exact(
                self.client, 1, self.deadline, None, None
            ),
            b"\n",
        )
        self.assertEqual(
            json.loads(
                self.compiler._serve_builder_read_line(
                    self.client, self.deadline, None, None
                )
            ),
            {"ok": True, "next": 1},
        )
        self.assertEqual(bytes(self.client.stdout_buffer), b"")
        self.assertTrue(all(w.closed == 1 for w in _RecordingWaiter.instances))

    def test_write_streams_to_a_slow_reader_exactly(self) -> None:
        self._gated()
        payload = os.urandom(2 << 20)
        received = bytearray()
        stdin_read = self.stdin_read

        def reader() -> None:
            # Nothing is drained until the writer has blocked on the full
            # pipe, so the transfer meets EAGAIN by construction.
            _GatedWaiter.released.wait(10.0)
            _slow_reader(stdin_read, received, chunk=32 << 10, pause=0.0)

        self._start(reader)

        written = self.compiler._serve_builder_write(
            self.client, payload, self.deadline, None, None
        )
        self._close_fd(self.client.process.stdin.fileno())
        self.threads[-1].join(10.0)

        self.assertEqual(written, len(payload))
        self.assertEqual(bytes(received), payload)
        (waiter,) = _GatedWaiter.instances
        self.assertTrue(_GatedWaiter.released.is_set())
        self.assertGreaterEqual(waiter.waits, 1)
        self.assertEqual(waiter.closed, 1)
        self.assertNoFailureAccounted()

    @unittest.skipUnless(hasattr(os, "splice"), "os.splice is Linux-only")
    def test_splice_streams_the_spool_to_a_slow_reader_exactly(self) -> None:
        payload = os.urandom(2 << 20)
        spool = tempfile.TemporaryFile()
        self.addCleanup(spool.close)
        spool.write(payload)
        spool.flush()
        serialization = _Serialization()
        received = bytearray()
        stdin_read = self.stdin_read
        self._gated()

        def reader() -> None:
            _GatedWaiter.released.wait(10.0)
            _slow_reader(stdin_read, received, chunk=32 << 10, pause=0.0)

        self._start(reader)

        moved = self.compiler._serve_builder_splice_spool(
            self.client,
            serialization,  # type: ignore[arg-type]
            spool,
            len(payload),
            self.deadline,
            None,
            None,
        )
        self._close_fd(self.client.process.stdin.fileno())
        self.threads[-1].join(10.0)

        self.assertEqual(moved, len(payload))
        self.assertEqual(bytes(received), payload)
        self.assertEqual(serialization.spool_failures, 0)
        (waiter,) = _GatedWaiter.instances
        self.assertTrue(_GatedWaiter.released.is_set())
        self.assertGreaterEqual(waiter.waits, 1)
        self.assertEqual(waiter.closed, 1)
        self.assertNoFailureAccounted()

    def test_idle_daemon_times_out_at_the_deadline_without_accounting(self) -> None:
        deadline = time.monotonic() + 0.1
        started = time.monotonic()

        with self.assertRaisesRegex(_ServeBuilderUnavailable, "timed out"):
            self.compiler._serve_builder_read_line(self.client, deadline, None, None)

        elapsed = time.monotonic() - started
        self.assertGreaterEqual(elapsed, 0.05)
        self.assertLess(elapsed, 5.0)
        # The wait path itself is pinned by the scripted timeout tests; a
        # real deadline cannot guarantee the loop reached a wait first.
        (waiter,) = _RecordingWaiter.instances
        self.assertEqual(waiter.closed, 1)
        self.assertNoFailureAccounted()

    def test_supersession_interrupts_a_blocked_read_on_the_cadence(self) -> None:
        control = _Control()
        threading.Timer(0.05, control.cancel_event.set).start()
        started = time.monotonic()

        with self.assertRaisesRegex(_Superseded, "canceled after supersession"):
            self.compiler._serve_builder_read_line(
                self.client, self.deadline, None, control
            )

        self.assertLess(time.monotonic() - started, 5.0)
        self.assertNoFailureAccounted()
        self.assertEqual(_RecordingWaiter.instances[0].closed, 1)

    def test_cancellation_interrupts_a_blocked_write_on_the_cadence(self) -> None:
        cancellation = _Cancellation()
        stdin_write = self.client.process.stdin.fileno()
        # Fill the pipe so the transport must wait for the peer to drain.
        filler = b"\0" * (64 << 10)
        while True:
            try:
                os.write(stdin_write, filler)
            except BlockingIOError:
                break
        threading.Timer(0.05, cancellation.event.set).start()
        started = time.monotonic()

        with self.assertRaises(_Cancelled):
            self.compiler._serve_builder_write(
                self.client, b"more", self.deadline, cancellation, None
            )

        self.assertLess(time.monotonic() - started, 5.0)
        self.assertNoFailureAccounted()
        self.assertEqual(_RecordingWaiter.instances[0].closed, 1)

    def test_daemon_exit_mid_frame_is_one_crash(self) -> None:
        os.write(self.stdout_write, b"only-part")
        self._close_fd(self.stdout_write)

        with self.assertRaisesRegex(_ServeBuilderUnavailable, "exited mid-request"):
            self.compiler._serve_builder_read_exact(
                self.client, 200, self.deadline, None, None
            )

        self.assertOneCrashAccounted()
        self.assertEqual(_RecordingWaiter.instances[0].closed, 1)

    def test_daemon_reader_gone_is_closed_input(self) -> None:
        self._close_fd(self.stdin_read)

        with self.assertRaisesRegex(_ServeBuilderUnavailable, "input pipe closed"):
            self.compiler._serve_builder_write(
                self.client, b"request\n", self.deadline, None, None
            )

        self.assertNoFailureAccounted()
        self.assertEqual(_RecordingWaiter.instances[0].closed, 1)

    def test_daemon_reader_gone_while_blocked_wakes_the_writer(self) -> None:
        stdin_write = self.client.process.stdin.fileno()
        filler = b"\0" * (64 << 10)
        while True:
            try:
                os.write(stdin_write, filler)
            except BlockingIOError:
                break
        stdin_read = self.stdin_read
        threading.Timer(0.05, lambda: self._close_fd(stdin_read)).start()
        started = time.monotonic()

        with self.assertRaisesRegex(_ServeBuilderUnavailable, "input pipe closed"):
            self.compiler._serve_builder_write(
                self.client, b"more", self.deadline, None, None
            )

        self.assertLess(time.monotonic() - started, 5.0)
        self.assertNoFailureAccounted()


# Echoes its stdin payload like the echo builder, but reads nothing until
# the marker file named by PRISM_TEST_READER_GATE exists: the coordinator's
# writer therefore fills the pipe and blocks by construction, and the gated
# waiter creates the marker on that first wait.
GATED_READER_BUILDER_COMMAND = [
    sys.executable,
    "-c",
    (
        "import json, os, sys, time\n"
        "marker = os.environ.get('PRISM_TEST_READER_GATE')\n"
        "while marker and not os.path.exists(marker):\n"
        "    time.sleep(0.001)\n"
        "data = bytearray()\n"
        "while True:\n"
        "    chunk = sys.stdin.buffer.read(65536)\n"
        "    if not chunk:\n"
        "        break\n"
        "    data += chunk\n"
        "json.dump({'received': json.loads(bytes(data)), 'transport': 'one-shot'},"
        " sys.stdout)\n"
    ),
]


class OneShotWriterTests(unittest.TestCase):
    """The one-shot subprocess writer waits for readiness like the daemon."""

    def _coordinator(self):
        server, _rpc = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        self.addCleanup(server.shutdown_serve_builder)
        self.addCleanup(server.retire_share_window_spool)
        return server

    def _serialization(self, server, shares):
        return server._share_window_serialization_for_artifact(
            PayoutLedgerArtifact(
                generation=1,
                payout_state_generation=0,
                network_difficulty=1,
                accepted_share_count=len(shares),
                shares_json=tuple(shares),
                prior_balances=(),
                prepared_monotonic=time.monotonic(),
                snapshot_anchor_ms=None,
            ),
            shares,
        )

    def _gate(self) -> str:
        directory = tempfile.mkdtemp(prefix="prism-reader-gate-")
        self.addCleanup(lambda: __import__("shutil").rmtree(directory, ignore_errors=True))
        return os.path.join(directory, "go")

    def _build(self, server, shares, *, serialization=None, command=None, gate=None):
        environment = {"PRISM_BUILDER_SERVE": "0"}
        if gate is not None:
            environment["PRISM_TEST_READER_GATE"] = gate
            _GatedWaiter.arm(gate)
            waiter_class: type = _GatedWaiter
        else:
            _RecordingWaiter.instances = []
            waiter_class = _RecordingWaiter
        with patch.dict(os.environ, environment), patch(
            "lab.prism.prism_coordinator.prism_tool_command",
            return_value=list(command or GATED_READER_BUILDER_COMMAND),
        ), patch.object(compiler_module, "_PipeReadinessWaiter", waiter_class):
            return server.build_audit_bundle(
                shares=shares,
                found_block={
                    "block_height": 10,
                    "coinbase_value_sats": 50_00000000,
                    "network_difficulty": 1,
                    "anchor_job_issued_at_ms": 1_700_000_000_000,
                },
                prior_balances=[],
                coinbase_script_sig_suffix_hex="00",
                summary_only=True,
                payout_policy={"policy": "day-one"},
                share_serialization=serialization,
            )

    def test_buffered_writer_streams_to_a_gated_child_exactly(self) -> None:
        server = self._coordinator()
        server.bundle_build_timeout_seconds = 30.0
        # Well over one pipe capacity, so the fragment writer must block
        # before the child is released to read anything.
        shares = [spool_share(seq) for seq in range(1, 6_001)]
        gate = self._gate()

        result = self._build(server, shares, gate=gate)

        self.assertEqual(result["transport"], "one-shot")
        self.assertEqual(len(result["received"]["compact_shares"]), 6_000)
        self.assertEqual(result["received"]["found_block"]["block_height"], 10)
        self.assertTrue(os.path.exists(gate))
        (waiter,) = _GatedWaiter.instances
        self.assertEqual(waiter._events, selectors.EVENT_WRITE)
        self.assertGreaterEqual(waiter.waits, 1)
        self.assertEqual(waiter.closed, 1)
        with server._job_build_scheduler_lock:
            counts = dict(server.job_build_worker_counts)
        self.assertEqual(counts["crashes"], 0)
        self.assertEqual(counts["terminations"], 0)

    @unittest.skipUnless(hasattr(os, "splice"), "os.splice is Linux-only")
    def test_spool_splice_streams_to_a_gated_child_exactly(self) -> None:
        server = self._coordinator()
        server.bundle_build_timeout_seconds = 30.0
        shares = [spool_share(seq) for seq in range(1, 6_001)]
        serialization = self._serialization(server, shares)
        gate = self._gate()

        result = self._build(server, shares, serialization=serialization, gate=gate)

        self.assertEqual(result["transport"], "one-shot")
        self.assertEqual(len(result["received"]["compact_shares"]), 6_000)
        self.assertFalse(serialization._spool_failed)
        self.assertTrue(os.path.exists(gate))
        (waiter,) = _GatedWaiter.instances
        self.assertGreaterEqual(waiter.waits, 1)
        self.assertEqual(waiter.closed, 1)

    def test_child_that_stops_reading_fails_the_build_without_hanging(self) -> None:
        server = self._coordinator()
        server.bundle_build_timeout_seconds = 0.5
        shares = [spool_share(seq) for seq in range(1, 6_001)]
        stalled = [
            sys.executable,
            "-c",
            "import sys, time\nsys.stdin.buffer.read(1024)\ntime.sleep(30)\n",
        ]
        started = time.monotonic()

        with self.assertRaisesRegex(RuntimeError, "timed out"):
            self._build(server, shares, command=stalled)

        self.assertLess(time.monotonic() - started, 10.0)
        (waiter,) = _RecordingWaiter.instances
        self.assertGreaterEqual(waiter.waits, 1)
        self.assertEqual(waiter.closed, 1)
        self.assertEqual(server.tip_refresh_worker_failures, 1)


class DaemonRoundTripTests(unittest.TestCase):
    """A large window through the real daemon protocol over real pipes."""

    def test_large_window_upload_and_hit_over_subprocess_pipes(self) -> None:
        server, _rpc = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        self.addCleanup(server.shutdown_serve_builder)
        self.addCleanup(server.retire_share_window_spool)
        shares = [spool_share(seq) for seq in range(1, 20_001)]
        serialization = server._share_window_serialization_for_artifact(
            PayoutLedgerArtifact(
                generation=1,
                payout_state_generation=0,
                network_difficulty=1,
                accepted_share_count=len(shares),
                shares_json=tuple(shares),
                prior_balances=(),
                prepared_monotonic=time.monotonic(),
                snapshot_anchor_ms=None,
            ),
            shares,
        )
        _RecordingWaiter.instances = []

        def build(height: int):
            with patch.dict(os.environ, {"FAKE_SERVE_BUILDER_MODE": "ok"}), patch(
                "lab.prism.prism_coordinator.prism_tool_command",
                return_value=list(FAKE_SERVE_BUILDER_COMMAND),
            ), patch.object(
                compiler_module,
                "_PipeReadinessWaiter",
                _RecordingWaiter,
            ):
                return server.build_audit_bundle(
                    shares=shares,
                    found_block={
                        "block_height": height,
                        "coinbase_value_sats": 50_00000000,
                        "network_difficulty": 1,
                        "anchor_job_issued_at_ms": 1_700_000_000_000,
                    },
                    prior_balances=[],
                    coinbase_script_sig_suffix_hex="00",
                    summary_only=True,
                    payout_policy={"policy": "day-one"},
                    share_serialization=serialization,
                )

        first = build(10)
        second = build(11)

        self.assertEqual(first["transport"], "serve")
        self.assertTrue(first["request_had_window"])
        self.assertEqual(len(first["window"]["compact_shares"]), 20_000)
        self.assertEqual(second["transport"], "serve")
        self.assertFalse(second["request_had_window"])
        self.assertEqual(len(second["window"]["compact_shares"]), 20_000)
        with server._serve_builder_metrics_lock:
            counts = dict(server.serve_builder_counts)
        self.assertEqual(counts["requests"], 2)
        self.assertEqual(counts["fallbacks"], 0)
        # Every transport operation registered and released its waiter; how
        # often the fake daemon's parse outran the coordinator is scheduling
        # and is not asserted.
        self.assertTrue(_RecordingWaiter.instances)
        self.assertTrue(all(w.closed == 1 for w in _RecordingWaiter.instances))


if __name__ == "__main__":
    unittest.main()
