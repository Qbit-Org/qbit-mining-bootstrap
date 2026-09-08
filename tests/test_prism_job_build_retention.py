#!/usr/bin/env python3
"""Completed job flights must release their windows without cyclic GC."""

from __future__ import annotations

import gc
import unittest
import weakref
from concurrent.futures import CancelledError, Future
from types import SimpleNamespace
from unittest.mock import patch

from lab.prism.job_bundle import JobBuildFlight, JobBundleService


class _Payload:
    pass


class JobBuildCallbackRetentionTests(unittest.TestCase):
    def setUp(self) -> None:
        gc.collect()
        self.gc_enabled = gc.isenabled()
        gc.disable()
        self.completions = 0
        self.fail_completion = False
        self.owner = SimpleNamespace(_runtime=self)

    def tearDown(self) -> None:
        if self.gc_enabled:
            gc.enable()
        gc.collect()

    def _job_build_done(self, flight: JobBuildFlight, future: Future) -> None:
        self.assertIs(flight.future, future)
        self.assertTrue(future.done())
        self.completions += 1
        if self.fail_completion:
            raise RuntimeError("injected completion bookkeeping failure")

    def arm(self, *, already_finished: bool = False):
        request = _Payload()
        result = _Payload()
        future = Future()
        flight = JobBuildFlight(request=request, future=future)
        references = {
            "request": weakref.ref(request),
            "result": weakref.ref(result),
            "flight": weakref.ref(flight),
            "future": weakref.ref(future),
        }
        if already_finished:
            future.set_result(result)
        JobBundleService._arm_job_build_locked(self.owner, flight)
        return future, result, references

    def assert_released(self, references) -> None:
        self.assertFalse(gc.isenabled())
        self.assertEqual(
            {name: ref() is not None for name, ref in references.items()},
            dict.fromkeys(references, False),
        )

    def test_completed_flights_and_results_release_without_collection(self) -> None:
        for _ in range(10):
            future, result, references = self.arm()
            future.set_result(result)
            del future, result
            self.assert_released(references)
        self.assertEqual(self.completions, 10)

    def test_callback_owns_pending_flight_until_completion(self) -> None:
        future, result, references = self.arm()
        self.assertIsNotNone(references["flight"]())
        self.assertIsNotNone(references["request"]())
        self.assertEqual(self.completions, 0)
        future.set_result(result)
        self.assertIsNone(references["flight"]())
        self.assertIsNone(references["request"]())
        self.assertIs(future.result(), result)
        del future, result
        self.assert_released(references)
        self.assertEqual(self.completions, 1)

    def test_already_finished_future_completes_inline(self) -> None:
        future, result, references = self.arm(already_finished=True)
        self.assertEqual(self.completions, 1)
        self.assertIs(future.result(), result)
        del future, result
        self.assert_released(references)

    def test_cancelled_future_releases_flight(self) -> None:
        future, result, references = self.arm()
        self.assertTrue(future.cancel())
        del future, result
        self.assert_released(references)
        self.assertEqual(self.completions, 1)

    def test_exceptional_future_releases_callback_ownership(self) -> None:
        future, result, references = self.arm()
        error = RuntimeError("build failed")
        future.set_exception(error)
        self.assertIs(future.exception(), error)
        del future, result
        self.assert_released(references)
        self.assertEqual(self.completions, 1)

    def test_bookkeeping_failure_releases_callback_ownership(self) -> None:
        future, result, references = self.arm()
        self.fail_completion = True
        # Capturing logging records here would itself retain the callback's
        # traceback and flight, masking the ownership under test.
        with patch("concurrent.futures._base.LOGGER.exception", lambda *a, **kw: None):
            future.set_result(result)
        self.assertEqual(self.completions, 1)
        self.assertIs(future.result(), result)
        del future, result
        self.assert_released(references)

    def check_failed_outcome_releases_request(self, *, cancelled: bool) -> None:
        request = _Payload()
        request.cancellation = SimpleNamespace(is_set=lambda: False)
        future = Future()
        references = {"request": weakref.ref(request), "future": weakref.ref(future)}
        if cancelled:
            future.cancel()
        else:
            future.set_exception(RuntimeError("build failed"))
        result, error = JobBundleService._job_build_flight_outcome(request, future)
        self.assertIsNone(result)
        self.assertIsInstance(error, CancelledError if cancelled else RuntimeError)
        self.assertIsNone(error.__traceback__)
        del request, future, result, error
        self.assert_released(references)

    def test_failed_outcome_does_not_capture_request_in_traceback(self) -> None:
        self.check_failed_outcome_releases_request(cancelled=False)

    def test_cancelled_outcome_does_not_capture_request_in_traceback(self) -> None:
        self.check_failed_outcome_releases_request(cancelled=True)


if __name__ == "__main__":
    unittest.main()
