"""Real idle sweep/task/completion; retained client keys versus retired work."""

from dataclasses import replace
from decimal import Decimal
import threading

from lab.prism.job_bundle import JobBuildSuperseded
from tests.prism_async_ownership_support import LifetimeCase, RecordingExecutor, retire_jobs, WAIT, held_worker
from tests.prism_vardiff_test_support import coordinator, client, prepare_idle_client, install_idle_job_cache


class FatalIdle(BaseException):
    pass


class VardiffIdleOwnershipTests(LifetimeCase):
    def exercise(self, outcome, *, inline=False):
        server = coordinator()
        self.server = server
        state = self.owner.watch("client", client())
        state.close = lambda: None
        prepare_idle_client(server, state)
        base = install_idle_job_cache(server)
        bundle = self.owner.watch("bundle", replace(base, shares_json=self.owner.window(parsed=True)))
        server._job_bundle_cache[bundle.key] = bundle
        service = server._ensure_vardiff_service()
        executor = RecordingExecutor(self.owner, inline=inline)
        service._vardiff_idle_executor = executor
        self.addCleanup(server.shutdown_vardiff_idle_executor)
        reached, proceed = threading.Event(), threading.Event()
        original = server._build_idle_job_bundle
        sent = []

        def build(request):
            prepared = original(request)
            reached.set()
            if not inline and not proceed.wait(WAIT):
                raise AssertionError("test did not release idle build")
            if outcome in ("failure", "timeout", "superseded", "prepare_oserror", "fatal"):
                error_type = {"failure": ValueError, "timeout": TimeoutError,
                              "superseded": JobBuildSuperseded, "prepare_oserror": OSError,
                              "fatal": FatalIdle}[outcome]
                raise error_type("injected idle preparation") from LookupError("idle diagnostic cause")
            return prepared

        def send(payload):
            sent.append(payload["method"])
            if outcome == "disconnect":
                raise OSError("injected idle send failure")

        server._build_idle_job_bundle = build
        state.send = send
        self.addCleanup(proceed.set)
        self.assertEqual(server.vardiff_idle_sweep_once(), 1)
        if not inline:
            self.wait(reached)
            self.assertEqual(service.vardiff_idle_inflight, 1)
            self.assertEqual(server.vardiff_idle_sweep_once(), 0)
            if outcome == "reconnect":
                server.disconnect_client(state)
                replacement = self.owner.watch("replacement", client())
                replacement.close = lambda: None
                prepare_idle_client(server, replacement, connection_id=2)
                replacement.send = sent.append
            proceed.set()
        executor.drain()
        self.assertEqual(service._vardiff_idle_pending, set())
        self.assertEqual((service.vardiff_idle_queue_depth, service.vardiff_idle_inflight), (0, 0))
        if outcome == "success":
            self.assertEqual(sent, ["mining.set_difficulty", "mining.notify"])
            self.assertEqual(state.share_difficulty, Decimal("4"))
        elif outcome in ("failure", "timeout", "prepare_oserror"):
            self.assertEqual(service.vardiff_idle_task_failures, 1)
            self.assertFalse(state.closing)
            self.assertEqual(state.share_difficulty, Decimal("16"))
        elif outcome == "disconnect":
            self.assertTrue(state.closing)
        elif outcome == "fatal":
            self.assertIsInstance(executor.futures[0].exception(), FatalIdle)
            self.assertEqual(executor.futures[0].exception().args, ("injected idle preparation",))
        elif outcome == "reconnect":
            self.assertEqual(sent, [])
            self.assertIn(replacement, server.clients)
            retire_jobs(server, replacement)
            del replacement
        retire_jobs(server, state)
        server._build_idle_job_bundle = original
        executor.futures.clear()
        del bundle, base, state

    def test_success(self):
        self.exercise("success")
        self.released()

    def test_caught_failure(self):
        self.exercise("failure")
        self.released()

    def test_worker_timeout(self):
        self.exercise("timeout")
        self.released()

    def test_superseded(self):
        self.exercise("superseded")
        self.released()

    def test_preparation_oserror_preserves_client(self):
        self.exercise("prepare_oserror")
        self.released()

    def test_send_disconnect(self):
        self.exercise("disconnect")
        self.released()

    def test_reconnect_during_preparation(self):
        self.exercise("reconnect")
        self.released()

    def test_completion_before_callback_registration(self):
        self.exercise("success", inline=True)
        self.released()

    def test_fatal_exit_completes_and_releases(self):
        self.exercise("fatal")
        self.released()

    def test_queued_cancellation_on_shutdown(self):
        server = coordinator()
        self.server = server
        state = self.owner.watch("client", client())
        state.close = lambda: None
        prepare_idle_client(server, state)
        bundle = install_idle_job_cache(server)
        service = server._ensure_vardiff_service()
        executor = RecordingExecutor(self.owner)
        service._vardiff_idle_executor = executor
        release = held_worker(executor.executor)
        self.addCleanup(server.shutdown_vardiff_idle_executor)
        self.addCleanup(release.set)
        self.assertEqual(server.vardiff_idle_sweep_once(), 1)
        executor.shutdown(wait=False, cancel_futures=True)
        self.assertTrue(executor.futures[0].cancelled())
        self.assertEqual(service._vardiff_idle_pending, set())
        self.assertEqual(service.vardiff_idle_queue_depth, 0)
        release.set()
        executor.drain()
        retire_jobs(server, state)
        executor.futures.clear()
        del state, bundle
        self.released()
