#!/usr/bin/env python3
"""A completed future must not keep obsolete callback owners alive."""

from __future__ import annotations

from concurrent.futures import Future
from contextlib import redirect_stderr, redirect_stdout
import gc
import io
import threading
from types import SimpleNamespace
import unittest
import weakref

from lab.prism.future_callbacks import add_releasing_done_callback
from lab.prism.job_delivery import JobDeliveryService, PendingInitialJob
from lab.prism.reorg_reconciler import ReorgReconcilerService


class _Callback:
    def __init__(self, calls: list[str], name: str) -> None:
        self.calls = calls
        self.name = name

    def __call__(self, future: Future) -> None:
        assert future.done()
        self.calls.append(self.name)


class CompletionCallbackTests(unittest.TestCase):
    def test_pending_callbacks_stay_alive_and_complete_in_registration_order(self) -> None:
        future = Future()
        calls: list[str] = []
        references = []
        for name in ("first", "second"):
            callback = _Callback(calls, name)
            references.append(weakref.ref(callback))
            add_releasing_done_callback(future, callback)
            del callback
        self.assertTrue(all(ref() is not None for ref in references))
        future.set_result(17)
        self.assertEqual(calls, ["first", "second"])
        self.assertTrue(all(ref() is None for ref in references))
        # The caller can keep the result future without keeping its callbacks.
        self.assertEqual(future.result(), 17)

    def test_finished_future_invokes_and_releases_callback_inline(self) -> None:
        future = Future()
        future.set_result(17)
        calls: list[str] = []
        callback = _Callback(calls, "inline")
        reference = weakref.ref(callback)
        add_releasing_done_callback(future, callback)
        del callback
        self.assertEqual(calls, ["inline"])
        self.assertIsNone(reference())


class InitialDeliveryCallbackRetentionTests(unittest.TestCase):
    def test_failure_handler_preserves_error_without_capturing_request(self) -> None:
        gc.collect()
        was_enabled = gc.isenabled()
        gc.disable()
        try:
            references = self.finish_failed_request()
            self.assertTrue(all(ref() is None for ref in references))
        finally:
            if was_enabled:
                gc.enable()
            gc.collect()

    def finish_failed_request(self):
        class Client:
            connection_id = 17

        client = Client()
        request = PendingInitialJob(
            client=client,
            authorization_generation=1,
            worker=None,
            requested_monotonic=0,
            deadline_monotonic=None,
        )
        future = Future()
        error = RuntimeError("initial build failed")
        future.set_exception(error)
        request.future = future
        pending = {client: request}
        disconnected = []
        runtime = SimpleNamespace(
            lock=threading.Lock(),
            job_build_failure_count=0,
            disconnect_client=lambda peer: disconnected.append(peer.connection_id),
        )
        owner = SimpleNamespace(
            _runtime=runtime,
            _initial_job_admission_lock=threading.Lock(),
            _pending_initial_jobs=pending,
            initial_job_failed_count=0,
        )
        output, errors = io.StringIO(), io.StringIO()
        with redirect_stdout(output), redirect_stderr(errors):
            JobDeliveryService._initial_job_future_finished(owner, request, future)
        self.assertEqual(runtime.job_build_failure_count, 1)
        self.assertEqual(owner.initial_job_failed_count, 1)
        self.assertEqual(disconnected, [17])
        self.assertFalse(pending)
        self.assertIn("connection=17", output.getvalue())
        self.assertIn("RuntimeError: initial build failed", errors.getvalue())
        self.assertIs(future.exception(), error)
        self.assertIsNone(error.__traceback__)
        return [weakref.ref(obj) for obj in (client, request, future)]

    def check_releases_request(self, *, cancel: bool, already_done: bool) -> None:
        gc.collect()
        was_enabled = gc.isenabled()
        gc.disable()
        try:
            references = self.complete_request(cancel=cancel, already_done=already_done)
            self.assertEqual(
                {name: ref() is not None for name, ref in references.items()},
                dict.fromkeys(references, False),
            )
        finally:
            if was_enabled:
                gc.enable()
            gc.collect()

    def complete_request(self, *, cancel: bool, already_done: bool):
        class Client:
            pass

        client = Client()
        request = PendingInitialJob(
            client=client,
            authorization_generation=1,
            worker=None,
            requested_monotonic=0,
            deadline_monotonic=None,
        )
        future = Future()
        pending = {client: request}
        calls = []

        def finish(completed_request, completed_future):
            self.assertIs(completed_request, pending.pop(completed_request.client))
            self.assertIs(completed_request.future, completed_future)
            calls.append(True)

        runtime = SimpleNamespace(
            _submit_delivery_task=lambda *args, **kwargs: future,
            initial_job_executor=lambda: None,
            _run_initial_job=lambda *args: None,
            _initial_job_future_finished=finish,
        )
        owner = SimpleNamespace(
            _runtime=runtime,
            _initial_job_admission_lock=threading.Lock(),
            _pending_initial_jobs=pending,
        )
        references = {
            "client": weakref.ref(client),
            "request": weakref.ref(request),
            "future": weakref.ref(future),
        }
        if already_done:
            future.set_result(True)
        self.assertTrue(JobDeliveryService._submit_initial_job_request(owner, request))
        if cancel:
            self.assertTrue(future.cancel())
        elif not already_done:
            future.set_result(True)
        self.assertEqual(calls, [True])
        self.assertFalse(pending)
        return references

    def test_completed_request_releases_disconnected_client(self) -> None:
        self.check_releases_request(cancel=False, already_done=False)

    def test_cancelled_request_releases_disconnected_client(self) -> None:
        self.check_releases_request(cancel=True, already_done=False)

    def test_already_completed_request_releases_disconnected_client(self) -> None:
        self.check_releases_request(cancel=False, already_done=True)


class StalePrefetchCallbackRetentionTests(unittest.TestCase):
    def check_releases_prefetch(self, *, already_done: bool, outcome: str) -> None:
        gc.collect()
        was_enabled = gc.isenabled()
        gc.disable()
        try:
            reference = self.discard_prefetch(already_done=already_done, outcome=outcome)
            self.assertIsNone(reference())
        finally:
            if was_enabled:
                gc.enable()
            gc.collect()

    def discard_prefetch(self, *, already_done: bool, outcome: str):
        future = Future()
        error = KeyboardInterrupt("prefetch failed")

        def complete():
            if outcome == "failed":
                future.set_exception(error)
            elif outcome == "cancelled":
                future.cancel()
            else:
                future.set_result(True)

        if already_done:
            complete()
        ReorgReconcilerService.discard_stale_prefetch(future)
        if not already_done:
            complete()
        if outcome == "failed":
            self.assertIs(future.exception(), error)
            # Observing an obsolete error must not add a new traceback whose
            # callback frame points back to the discarded future.
            self.assertIsNone(error.__traceback__)
        return weakref.ref(future)

    def test_failed_pending_prefetch_does_not_capture_consumer_frame(self) -> None:
        self.check_releases_prefetch(already_done=False, outcome="failed")

    def test_failed_finished_prefetch_does_not_capture_consumer_frame(self) -> None:
        self.check_releases_prefetch(already_done=True, outcome="failed")

    def test_cancelled_prefetch_is_discarded(self) -> None:
        self.check_releases_prefetch(already_done=False, outcome="cancelled")

    def test_successful_prefetch_is_discarded(self) -> None:
        self.check_releases_prefetch(already_done=False, outcome="succeeded")


if __name__ == "__main__":
    unittest.main()
