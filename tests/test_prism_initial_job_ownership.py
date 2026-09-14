"""Real initial producer/delivery/completion ownership, independent of #340."""

from dataclasses import replace
import io
import threading
from contextlib import redirect_stderr

from lab.prism.bounded_executor import _BoundedPriorityExecutor
from tests.prism_async_ownership_support import LifetimeCase, RecordingExecutor, retire_jobs, WAIT, held_worker
from tests.prism_coordinator_test_support import coordinator, client, install_fake_bundle_builder


def noted(error):
    error.add_note("initial diagnostic note")
    return error


class InitialJobOwnershipTests(LifetimeCase):
    def exercise(self, outcome, *, inline=False):
        server, _ = coordinator()
        self.server = server  # service remains alive through assertions
        install_fake_bundle_builder(server)
        base = server.prewarm_current_tip_ready_bundle()
        bundle = self.owner.watch("bundle", replace(base, shares_json=self.owner.window(parsed=True)))
        service = server._ensure_job_delivery_service()
        executor = RecordingExecutor(self.owner, _BoundedPriorityExecutor(
            max_workers=1, max_queue_size=4), inline=inline)
        service._initial_job_executor = executor
        self.addCleanup(server.shutdown_initial_job_executor)
        self.addCleanup(server.shutdown_job_build_executor)
        state = self.owner.watch("client", client(1))
        state.close = lambda: None
        state.send = lambda payload: None
        server.clients.add(state)
        # Build has already succeeded. Inject at delivery, outside #340.
        server.shared_job_bundle = lambda *a, **kw: bundle
        reached, proceed = threading.Event(), threading.Event()
        methods = []

        def send(payload):
            methods.append(payload["method"])
            if outcome in ("failure", "timeout", "disconnect"):
                reached.set()
                if not inline and not proceed.wait(WAIT):
                    raise AssertionError("test did not release send")
                error_type = {"failure": ValueError, "timeout": TimeoutError,
                              "disconnect": OSError}[outcome]
                raise noted(error_type("injected initial delivery")) from LookupError("initial diagnostic cause")

        state.send = send
        self.addCleanup(proceed.set)
        diagnostics = io.StringIO()
        with redirect_stderr(diagnostics):
            self.assertTrue(server.schedule_initial_job(state))
            if outcome in ("failure", "timeout", "disconnect") and not inline:
                self.wait(reached)
                proceed.set()
            executor.drain()
        self.assertFalse(server.pending_initial_jobs)
        if outcome == "failure":
            self.assertIn("initial diagnostic cause", diagnostics.getvalue())
            self.assertIn("initial diagnostic note", diagnostics.getvalue())
            self.assertIn("_deliver_initial_bundle", diagnostics.getvalue())
            self.assertEqual(server.job_build_failure_count, 1)
        elif outcome in ("timeout", "disconnect"):
            # TimeoutError is an OSError: the socket path disconnects.
            self.assertTrue(state.closing)
            self.assertEqual(server.job_build_failure_count, 0)
        elif outcome == "success":
            self.assertIn("mining.notify", methods)
            self.assertIs(state.active_job.shares_json, bundle.shares_json)
            self.assertEqual(server.job_build_failure_count, 0)
        # Retained active/graveyard work is legitimate until this boundary.
        retire_jobs(server, state)
        server.shared_job_bundle = None
        executor.futures.clear()
        del state, bundle, base

    def test_success_releases_after_job_history_retires(self):
        self.exercise("success")
        self.released()
    def test_delivery_failure_releases_producer_request(self):
        self.exercise("failure")
        self.released()

    def test_worker_timeout_releases_without_becoming_join_expiry(self):
        self.exercise("timeout")
        self.released()

    def test_socket_disconnect_releases(self):
        self.exercise("disconnect")
        self.released()

    def test_completion_before_registration(self):
        self.exercise("failure", inline=True)
        self.released()

    def queued(self, action):
        server, _ = coordinator()
        self.server = server
        install_fake_bundle_builder(server)
        server.prewarm_current_tip_ready_bundle()
        state = self.owner.watch("client", client(1))
        state.close = lambda: None
        state.send = lambda payload: None
        state.authorization_generation = 0
        server.clients.add(state)
        service = server._ensure_job_delivery_service()
        executor = RecordingExecutor(self.owner, _BoundedPriorityExecutor(
            max_workers=1, max_queue_size=1))
        service._initial_job_executor = executor
        self.addCleanup(server.shutdown_job_build_executor)
        self.addCleanup(server.shutdown_initial_job_executor)
        release = held_worker(executor.executor)
        self.addCleanup(release.set)
        self.assertTrue(server.schedule_initial_job(state))
        predecessor = executor.futures[0]
        if action == "replace":
            state.authorization_generation = 1
            self.assertTrue(server.schedule_initial_job(state))
            self.assertTrue(predecessor.cancelled())
            self.assertEqual(len(executor.futures), 2)
            self.assertIs(server.pending_initial_jobs[state].future, executor.futures[1])
            self.assertEqual(executor.executor.stats(), (1, 1))
        elif action == "deadline":
            self.assertEqual(server.sweep_initial_job_timeouts(now=1e30), 1)
            self.assertTrue(state.closing)
            self.assertTrue(predecessor.cancelled())
        elif action == "cancel":
            server.cancel_initial_job_delivery(state)
            self.assertTrue(predecessor.cancelled())
            self.assertEqual(executor.executor.stats(), (0, 1))
        else:
            server.stop_event.set()
        release.set()
        if action == "shutdown":
            server.shutdown_initial_job_executor()
        else:
            executor.drain()
        self.assertFalse(server.pending_initial_jobs)
        if action == "replace":
            self.assertIsNotNone(state.active_job)
            self.assertFalse(state.closing)
            self.assertEqual(state.active_job.authorization_generation, 1)
        retire_jobs(server, state)
        executor.futures.clear()
        del predecessor, state

    def test_queued_replacement_reclaims_admission_before_submit(self):
        self.queued("replace")
        self.released()

    def test_queued_cancellation(self):
        self.queued("cancel")
        self.released()

    def test_deadline_disconnects_and_retires_queued_work(self):
        self.queued("deadline")
        self.released()

    def test_shutdown(self):
        self.queued("shutdown")
        self.released()

    def running(self, action):
        server, _ = coordinator()
        self.server = server
        install_fake_bundle_builder(server)
        base = server.prewarm_current_tip_ready_bundle()
        bundle = self.owner.watch("bundle", replace(base, shares_json=self.owner.window()))
        state = self.owner.watch("client", client(1))
        state.close = lambda: None
        state.authorization_generation = 0
        sent = []
        state.send = sent.append
        server.clients.add(state)
        service = server._ensure_job_delivery_service()
        executor = RecordingExecutor(self.owner, _BoundedPriorityExecutor(max_workers=1, max_queue_size=1))
        service._initial_job_executor = executor
        self.addCleanup(server.shutdown_job_build_executor)
        self.addCleanup(server.shutdown_initial_job_executor)
        reached, proceed = threading.Event(), threading.Event()
        calls = []

        def build(*args, **kwargs):
            calls.append(1)
            if len(calls) == 1:
                reached.set()
                if not proceed.wait(WAIT):
                    raise AssertionError("test did not release initial preparation")
                if action == "retry":
                    raise ValueError("ordinary preparation error; retry")
            return bundle

        server.shared_job_bundle = build
        self.addCleanup(proceed.set)
        self.assertTrue(server.schedule_initial_job(state))
        self.wait(reached)
        with self.assertRaises(TimeoutError):
            executor.futures[0].exception(timeout=0)
        self.assertFalse(executor.futures[0].done())
        if action == "replace":
            state.authorization_generation = 1
            self.assertTrue(server.schedule_initial_job(state))
            self.assertIsNone(server.pending_initial_jobs[state].future)
            self.assertIs(server.pending_initial_jobs[state].predecessor, executor.futures[0])
        elif action == "reconnect":
            server.disconnect_client(state)
            replacement = self.owner.watch("replacement", client(2))
            replacement.close = lambda: None
            replacement.send = sent.append
            server.clients.add(replacement)
            self.assertTrue(server.schedule_initial_job(replacement))
        elif action == "cancel":
            server.cancel_initial_job_delivery(state)
        # A tail marker is insufficient: the completion callback can enqueue
        # a replacement after it. Wait on the replacement's actual notify.
        delivered = threading.Event()
        original_send = server.send_job_update

        def send(client, job):
            original_send(client, job)
            delivered.set()

        server.send_job_update = send
        proceed.set()
        if action != "cancel":
            self.wait(delivered)
        executor.drain()
        self.assertFalse(server.pending_initial_jobs)
        self.assertEqual(len(calls), 1 if action == "cancel" else 2)
        if action == "replace":
            self.assertFalse(state.closing)
            self.assertEqual(state.active_job.authorization_generation, 1)
        elif action == "cancel":
            self.assertEqual(sent, [])
        elif action == "reconnect":
            self.assertIsNotNone(replacement.active_job)
            retire_jobs(server, replacement)
            del replacement
        elif action == "retry":
            self.assertEqual(server.job_build_failure_count, 1)
            self.assertIsNotNone(state.active_job)
        retire_jobs(server, state)
        server.shared_job_bundle = None
        executor.futures.clear()
        del bundle, base, state

    def test_running_replacement_waits_for_predecessor(self):
        self.running("replace")
        self.released()

    def test_running_cancellation(self):
        self.running("cancel")
        self.released()

    def test_disconnect_reconnect_during_preparation(self):
        self.running("reconnect")
        self.released()

    def test_caught_preparation_failure_retries(self):
        self.running("retry")
        self.released()
