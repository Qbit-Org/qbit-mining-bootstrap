"""Ordinary job-build failures: diagnostics, races and prompt ownership release."""

from __future__ import annotations

import gc
import subprocess
import threading
import unittest
import weakref
from concurrent.futures import CancelledError, Future

from lab.prism.job_bundle import (
    JobBuildSuperseded, _await_job_build_promise, _job_build_error_for_waiter,
)
from tests import test_prism_job_build_exception_retention as harness


class KeywordFailure(RuntimeError):
    __slots__ = ("phase",)

    def __init__(self, message, *, phase):
        super().__init__(message)
        self.phase = phase

    def __copy__(self):
        raise AssertionError("copy hooks must not run during failure delivery")


class Payload:
    pass


class UnexpectedJobBuildOwnershipTests(unittest.TestCase):
    def setUp(self):
        self.case = harness.JobBuildExceptionRetentionTests()
        self.case.setUp()
        self.addCleanup(self.case.tearDown)

    def finish(self, threads):
        case = self.case
        case.join_all(threads)
        for slot in ("_job_build_active", "_job_build_retiring", "_job_build_pending"):
            self.assertIsNone(getattr(case.service, slot))
        self.assertEqual(case.server._payout_window_inflight_scan_anchors, {})
        case.assert_released(case.tracked_references())

    def test_healthy_build_keeps_cached_rows_until_cache_retires(self):
        case = self.case
        recorded = harness.install_fake_bundle_builder(case.server)
        case.server._payout_artifact_reuse_active = lambda: False
        entered, release = case.hold_first_conversion()
        threads, outcomes = case.run_waiters(3)
        self.assertTrue(entered.wait(harness.WAIT_SECONDS))
        case.await_joined_flight(3)
        release.set()
        case.join_all(threads)
        self.assertTrue(all(o == {"kind": "bundle"} for o in outcomes))
        case.assert_released({k: v for k, v in case.tracked_references().items()
                              if not k.startswith("first_row")})
        self.assertIsNotNone(case.ledger.first_rows[0]())
        recorded.clear()  # The compiler test double deliberately records kwargs.
        with case.service._job_cache_lock:
            case.service._job_bundle_cache.clear()
        case.assert_released(case.tracked_references())

    def test_executor_queued_cancellation_releases_real_scheduler_waiters(self):
        case = self.case
        case.server.job_build_executor_workers = 1
        entered, release = threading.Event(), threading.Event()

        def occupy_worker():
            entered.set()
            if not release.wait(harness.WAIT_SECONDS):
                raise AssertionError("worker not released")

        with case.service._job_build_scheduler_lock:
            executor = case.service._job_build_executor_locked()
        blocker = executor.submit(occupy_worker)
        self.assertTrue(entered.wait(harness.WAIT_SECONDS))
        threads, outcomes = case.run_waiters(3)
        harness.wait_until(lambda: case.service.job_build_scheduler_counts["requests"] == 3)
        with case.service._job_build_scheduler_lock:
            flight = case.service._job_build_active
            case.flight_promise = weakref.ref(flight.request.promise)
            queued = flight.future
        self.assertTrue(queued.cancel())
        release.set()
        blocker.result(harness.WAIT_SECONDS)
        del flight, queued, blocker
        self.finish(threads)
        self.assertTrue(all(o["type"] is CancelledError for o in outcomes))
        self.assertEqual(case.ledger.snapshot_calls, 0)

    def test_worker_timeout_retains_existing_wait_deadline_and_retry_semantics(self):
        case = self.case
        # The shared waiter historically retries TimeoutError until its wait
        # deadline, then translates it to JobBuildCancelled. Keep that policy.
        case.server.job_build_timeout_seconds = 0.01
        case.server.job_build_cancel_grace_seconds = 0.01

        def fail(**kwargs):
            raise TimeoutError("compiler timeout")

        case.server.build_audit_bundle = fail
        threads, outcomes = case.run_waiters(1)
        self.finish(threads)
        self.assertEqual(outcomes[0]["kind"], "cancelled")
        self.assertEqual(outcomes[0]["cause"][:2], (TimeoutError, ("compiler timeout",)))
        self.assertEqual(case.service.shared_bundle_build_counts["failed"], 1)

    def test_compiler_failure_preserves_nested_diagnostics(self):
        case = self.case
        reached, proceed = case.hold_first_conversion()

        def compile_bundle(**kwargs):
            try:
                raise OSError(5, "ledger read failed", "test-input")
            except OSError as cause:
                error = KeywordFailure("compiler failed", phase="audit")
                error.add_note("diagnostic note")
                error.status = 17
                raise error from cause

        case.server.build_audit_bundle = compile_bundle
        threads, outcomes = case.run_waiters(3)
        self.assertTrue(reached.wait(harness.WAIT_SECONDS))
        case.await_joined_flight(3)
        proceed.set()
        self.finish(threads)
        for outcome in outcomes:
            self.assertIs(outcome["type"], KeywordFailure)
            self.assertEqual(outcome["args"], ("compiler failed",))
            self.assertEqual(outcome["notes"], ["diagnostic note"])
            self.assertEqual(outcome["attributes"], {"status": 17})
            self.assertEqual(outcome["cause"][:2], (OSError, (5, "ledger read failed")))
            self.assertEqual(outcome["context"], outcome["cause"])
            self.assertIn("compile_bundle", outcome["names"])
            self.assertFalse(outcome["stored_is_error"])
        self.assertEqual(case.service.shared_bundle_build_counts["failed"], 1)

    def test_ledger_read_failure_releases_snapshot_before_conversion(self):
        case = self.case
        read = case.ledger.snapshot_at_job_issue
        entered, release = threading.Event(), threading.Event()

        def snapshot(*args, **kwargs):
            records = read(*args, **kwargs)
            entered.set()
            if not release.wait(harness.WAIT_SECONDS):
                raise AssertionError("read not released")
            raise OSError(5, "snapshot failed", "test-ledger")

        case.ledger.snapshot_at_job_issue = snapshot
        threads, outcomes = case.run_waiters(3)
        self.assertTrue(entered.wait(harness.WAIT_SECONDS))
        case.await_joined_flight(3)
        release.set()
        self.finish(threads)
        self.assertTrue(all(outcome["type"] is OSError for outcome in outcomes))
        self.assertEqual(len(case.ledger.first_rows), 0)
        self.assertEqual(len(case.ledger.snapshots), 1)

    def test_ordinary_failure_does_not_auto_retry_but_next_request_can_build(self):
        case = self.case
        case.server.job_build_executor_workers = 1
        case.server._payout_artifact_reuse_active = lambda: False

        def reject(index):
            raise ValueError("one failed attempt")

        entered, release = case.hold_first_conversion(after=reject)
        threads, outcomes = case.run_waiters(1, retry_superseded=True)
        self.assertTrue(entered.wait(harness.WAIT_SECONDS))
        case.await_joined_flight(1)
        release.set()
        for thread in threads:
            thread.join(harness.WAIT_SECONDS)
            self.assertFalse(thread.is_alive())
        case.service._job_build_executor.submit(lambda: None).result(5)
        case.assert_released(case.tracked_references())
        self.assertEqual(outcomes[0]["kind"], "unexpected")
        self.assertEqual(case.service.job_build_scheduler_counts["starts"], 1)
        recorded = harness.install_fake_bundle_builder(case.server)
        threads, outcomes = case.run_waiters(1)
        case.join_all(threads)
        self.assertEqual(outcomes, [{"kind": "bundle"}])
        self.assertEqual(case.service.job_build_scheduler_counts["starts"], 2)
        self.assertEqual(case.service.shared_bundle_build_counts["failed"], 1)
        self.assertEqual(case.service.shared_bundle_build_counts["completed"], 1)
        recorded.clear()
        with case.service._job_cache_lock:
            case.service._job_bundle_cache.clear()
        case.assert_released(case.tracked_references())

    def test_late_waiters_leave_stored_traceback_and_producer_retired(self):
        case = self.case

        def reject(index):
            raise ValueError("ledger rejected")

        entered, release = case.hold_first_conversion(after=reject)
        threads, _ = case.run_waiters(1)
        self.assertTrue(entered.wait(harness.WAIT_SECONDS))
        case.await_joined_flight(1)
        promise = case.flight_promise()
        release.set()
        case.join_all(threads)
        original = promise.exception()
        before = harness.traceback_names(original)
        original_traceback = original.__traceback__
        # Keeping the completed promise for late subscribers must not retain
        # the retired request or any part of the converted window.
        case.assert_released({k: v for k, v in case.tracked_references().items()
                              if not k.startswith("promise")})
        for _ in range(3):
            try:
                _await_job_build_promise(promise, 1)
            except ValueError as error:
                self.assertIsNot(error, original)
                self.assertEqual(error.args, original.args)
                self.assertIsNot(error.__traceback__.tb_next.tb_next, original_traceback)
            else:
                self.fail("late waiter did not fail")
        self.assertIs(original.__traceback__, original_traceback)
        self.assertEqual(harness.traceback_names(original), before)
        del promise, original, original_traceback
        case.assert_released(case.tracked_references())

    def test_failure_with_cyclic_cause_does_not_cycle_back_to_waiter(self):
        case = self.case

        def reject(index):
            error = ValueError("cyclic diagnostics")
            error.__cause__ = error
            raise error

        entered, release = case.hold_first_conversion(after=reject)
        threads, outcomes = case.run_waiters(3)
        self.assertTrue(entered.wait(harness.WAIT_SECONDS))
        case.await_joined_flight(3)
        release.set()
        self.finish(threads)
        self.assertTrue(all(o["type"] is ValueError for o in outcomes))

    def test_grouped_failure_releases_nested_producer(self):
        case = self.case
        payloads = []

        def reject(index):
            payload = Payload()
            payloads.append(weakref.ref(payload))
            error = ValueError("inner")
            raise error

        entered, release = case.hold_first_conversion(after=reject)
        execute = case.server._execute_job_build_request

        def _execute_job_build_request(request):
            try:
                return execute(request)
            except ValueError as error:
                raise ExceptionGroup("build failures", [error]) from None

        case.server._execute_job_build_request = _execute_job_build_request
        # The generic harness records args; group args legitimately own child
        # errors. Record only the classification for this payload assertion.
        outcomes = []

        def wait():
            try:
                case.server.shared_job_bundle(case.artifacts, mode="ready", retry_superseded=False)
            except ExceptionGroup as error:
                outcomes.append((error.message, error.exceptions[0].args))

        thread = threading.Thread(target=wait)
        thread.start()
        self.assertTrue(entered.wait(harness.WAIT_SECONDS))
        case.await_joined_flight(1)
        release.set()
        self.finish([thread])
        self.assertEqual(outcomes, [("build failures", ("inner",))])
        self.assertIsNone(payloads[0]())

    def test_failure_shutdown_race_keeps_failure_and_releases(self):
        case = self.case

        def reject(index):
            raise ValueError("failure while shutting down")

        entered, release = case.hold_first_conversion(after=reject)
        threads, outcomes = case.run_waiters(3)
        self.assertTrue(entered.wait(harness.WAIT_SECONDS))
        case.await_joined_flight(3)
        shutdown = threading.Thread(target=case.server.shutdown_job_build_executor)
        shutdown.start()
        harness.wait_until(lambda: case.service._job_build_executor_shutdown)
        release.set()
        self.finish(threads + [shutdown])
        self.assertTrue(all(o["type"] is ValueError for o in outcomes))
        # Cancellation already won classification, but it does not swallow
        # the producer's actual error delivered on the shared promise.
        self.assertEqual(case.service.shared_bundle_build_counts["superseded"], 1)
        self.assertEqual(case.service.shared_bundle_build_counts["failed"], 0)

    def test_orphan_completion_race_releases_and_resolves_once(self):
        case = self.case
        case.server.job_build_orphan_sweep_grace_seconds = 0
        callback_entered, callback_release = threading.Event(), threading.Event()
        done = case.server._job_build_done

        def parked_done(flight, future):
            callback_entered.set()
            if not callback_release.wait(harness.WAIT_SECONDS):
                raise AssertionError("callback not released")
            done(flight, future)

        case.server._job_build_done = parked_done

        def reject(index):
            raise ValueError("orphan failure")

        entered, release = case.hold_first_conversion(after=reject)
        threads, outcomes = case.run_waiters(3)
        self.assertTrue(entered.wait(harness.WAIT_SECONDS))
        case.await_joined_flight(3)
        release.set()
        self.assertTrue(callback_entered.wait(harness.WAIT_SECONDS))
        with case.service._job_build_scheduler_lock:
            self.assertEqual(case.service._evict_orphaned_job_build_flights_locked(), [])
            self.assertEqual(len(case.service._evict_orphaned_job_build_flights_locked()), 1)
        for thread in threads:
            thread.join(harness.WAIT_SECONDS)
            self.assertFalse(thread.is_alive())
        callback_release.set()
        self.finish(threads)
        self.assertTrue(all(o["type"] is ValueError for o in outcomes))
        self.assertEqual(case.service.job_build_scheduler_counts["orphan_evicted"], 1)
        self.assertEqual(case.service.job_build_scheduler_counts["completions"], 1)

    def test_foreign_finished_and_running_frames_are_untouched(self):
        case = self.case
        foreign_payloads = []

        def foreign():
            payload = Payload()
            foreign_payloads.append(weakref.ref(payload))
            raise ValueError("foreign producer")

        try:
            foreign()
        except ValueError as error:
            shared = error
        original_traceback = shared.__traceback__
        original_next = original_traceback.tb_next

        def reject(index):
            raise RuntimeError("build failed") from shared

        entered, release = case.hold_first_conversion(after=reject)
        threads, _ = case.run_waiters(3)
        self.assertTrue(entered.wait(harness.WAIT_SECONDS))
        case.await_joined_flight(3)
        release.set()
        self.finish(threads)
        # This test's running frame and the finished foreign frame remain
        # owned by the externally held error; neither belongs to the build.
        self.assertIsNotNone(foreign_payloads[0]())
        self.assertIs(shared.__traceback__, original_traceback)
        self.assertIs(original_traceback.tb_next, original_next)


class JobBuildErrorCopyTests(unittest.TestCase):
    def test_builtin_custom_and_group_state(self):
        custom = KeywordFailure("custom", phase="compiler")
        custom.add_note("note")
        custom.status = 17
        for original in (
            OSError("ledger failed"),
            TimeoutError("compiler timeout"),
            MemoryError("allocation failed"),
            OSError(5, "io", "input"),
            OSError(5, "io", "input", None, "output"),
            BlockingIOError(11, "blocked", 123),
            subprocess.CalledProcessError(7, ["test-helper"], output=b"out", stderr=b"err"),
            UnicodeDecodeError("utf-8", b"\xff", 0, 1, "bad byte"),
            SyntaxError("invalid", ("input", 2, 3, "bad line")),
            ImportError("missing", name="test-module", path="test-path"),
            custom,
        ):
            with self.subTest(kind=type(original).__name__):
                private = _job_build_error_for_waiter(original)
                self.assertIsNot(private, original)
                self.assertIs(type(private), type(original))
                self.assertEqual(private.args, original.args)
                self.assertEqual(str(private), str(original))
                self.assertEqual(vars(private), vars(original))
                for name in ("filename", "errno", "strerror", "output", "stderr", "encoding",
                             "object", "start", "end", "reason", "phase", "characters_written",
                             "name", "path", "lineno", "offset", "text"):
                    if hasattr(original, name):
                        self.assertEqual(getattr(private, name), getattr(original, name))
        child = ValueError("child")
        group = ExceptionGroup("group", [child])
        group.__cause__ = child
        child.__context__ = group
        private = _job_build_error_for_waiter(group)
        self.assertIs(private.__cause__, private.exceptions[0])
        self.assertIsNot(private.exceptions[0], child)
        self.assertEqual(private.exceptions[0].__context__.message, "group")

    def test_concurrent_consumers_can_edit_tracebacks_and_notes_independently(self):
        promise = Future()
        try:
            raise ValueError("inner")
        except ValueError as inner:
            original = RuntimeError("failed")
            original.__cause__ = inner
        original.add_note("original note")
        promise.set_exception(original)
        consumers = []

        def wait():
            try:
                _await_job_build_promise(promise, 1)
            except RuntimeError as error:
                consumers.append(error)

        threads = [threading.Thread(target=wait) for _ in range(3)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(5)
            self.assertFalse(thread.is_alive())
        self.assertEqual(len(consumers), 3)
        self.assertEqual(len({id(error) for error in consumers}), 3)
        self.assertEqual(len({id(error.__cause__) for error in consumers}), 3)
        original_cause_tb = original.__cause__.__traceback__
        other_tb = consumers[1].__cause__.__traceback__
        consumers[0].__cause__.__traceback__.tb_next = None
        consumers[0].__cause__.__traceback__ = None
        consumers[0].add_note("private note")
        self.assertIs(original.__cause__.__traceback__, original_cause_tb)
        self.assertIs(consumers[1].__cause__.__traceback__, other_tb)
        self.assertEqual(consumers[1].__notes__, ["original note"])
        self.assertEqual(original.__notes__, ["original note"])
        self.assertIsNone(original.__traceback__)
        consumers.clear()


if __name__ == "__main__":
    unittest.main()
