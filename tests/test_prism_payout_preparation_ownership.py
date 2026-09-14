"""Real preparation loop: caught failures, escaped failures and slot retirement."""

from concurrent.futures import TimeoutError
from dataclasses import replace
from contextlib import redirect_stderr
import io
import threading
from unittest.mock import patch
from tests.prism_async_ownership_support import LifetimeCase, RecordingExecutor, WAIT, held_worker
from tests.prism_coordinator_test_support import coordinator
from tests.test_prism_job_build_exception_retention import WindowLedger


class PayoutPreparationOwnershipTests(LifetimeCase):
    def setUp(self):
        super().setUp()
        self.server, rpc = coordinator()
        self.service = self.server._ensure_payout_state_service()
        self.difficulty = self.server.store_template_artifacts(dict(rpc.template)).network_difficulty
        self.executor = RecordingExecutor(self.owner)
        self.server._payout_artifact_executor = self.executor
        self.addCleanup(self.server.shutdown_payout_artifact_executor)

    def schedule(self, generation=0):
        self.server._schedule_payout_ledger_artifact_preparation(generation, self.difficulty)

    def retire(self):
        self.server._payout_ledger_artifact = None
        self.server._incremental_payout_artifact_window = None
        self.executor.futures.clear()

    def test_healthy_artifacts_release(self):
        original = self.server._build_payout_ledger_artifact

        def build(*args, **kwargs):
            artifact = original(*args, **kwargs)
            if artifact is not None:
                artifact = replace(artifact, shares_json=self.owner.window())
                self.owner.watch("artifact", artifact)
            return artifact

        self.server._build_payout_ledger_artifact = build
        self.schedule()
        self.executor.drain()
        self.assertIsNotNone(self.server._payout_ledger_artifact)
        self.assertIsNone(self.server._payout_artifact_future)
        self.assertIsNone(self.server._payout_artifact_requested)
        self.retire()
        self.released()

    def test_caught_materialization_failure_releases_and_backs_off(self):
        def materialize(**kwargs):
            payload = self.owner.window(parsed=True)
            raise ValueError("injected materialization failure")

        self.server._incremental_payout_window_materialization = materialize
        self.schedule()
        self.executor.drain()
        self.assertIsNone(self.executor.futures[0].exception())
        self.assertEqual(self.server._payout_artifact_rearm_backoff, 2)
        self.assertEqual(self.server._payout_window_inflight_scan_anchors, {})
        self.assertIsNone(self.server._payout_artifact_future)
        self.retire()
        self.released()

    def test_real_compatibility_conversion_failure_releases_rows(self):
        ledger = WindowLedger()
        self.server.ledger = ledger
        original = ledger.snapshot_at_job_issue

        def snapshot(*args, **kwargs):
            records = original(*args, **kwargs)
            self.owner.watch("record", records[0])
            return records

        def convert(snapshot_index, index):
            if index == 300:
                raise ValueError("partial payout conversion")

        ledger.snapshot_at_job_issue = snapshot
        ledger.on_convert = convert
        self.schedule()
        self.executor.drain()
        self.assertEqual(ledger.snapshot_calls, 1)
        self.assertEqual(len(ledger.first_rows), 1)
        self.owner.refs.extend(("converted_row", ref) for ref in ledger.first_rows)
        self.assertIsNone(self.executor.futures[0].exception())
        self.assertEqual(self.server._payout_artifact_rearm_backoff, 2)
        self.retire()
        self.released()

    def test_escaped_install_failure_releases_and_drains_latest_request(self):
        reached, proceed = threading.Event(), threading.Event()
        calls = []
        original = self.server._install_payout_ledger_artifact

        def install(artifact):
            calls.append(artifact.payout_state_generation)
            if len(calls) == 1:
                payload = self.owner.window(parsed=True)
                reached.set()
                if not proceed.wait(WAIT):
                    raise AssertionError("test did not release install")
                raise ValueError("injected install failure") from LookupError("diagnostic cause")
            return original(artifact)

        self.server._install_payout_ledger_artifact = install
        self.addCleanup(proceed.set)
        self.schedule()
        self.wait(reached)
        # Supersede the queued scalar request twice while the worker is held.
        self.schedule(41)
        self.schedule(0)
        diagnostics = io.StringIO()
        with redirect_stderr(diagnostics):
            proceed.set()
            self.executor.drain()
        self.assertIn("ValueError: injected install failure", diagnostics.getvalue())
        self.assertIn("LookupError: diagnostic cause", diagnostics.getvalue())
        self.assertIn("_prepare_payout_ledger_artifact", diagnostics.getvalue())
        self.assertIsNone(self.server._payout_artifact_future)
        self.assertIsNone(self.server._payout_artifact_requested)
        self.assertEqual(calls, [0, 0])
        self.assertIsNotNone(self.server._payout_ledger_artifact)
        self.retire()
        self.released()

    def test_worker_timeout_is_caught_but_wait_expiry_keeps_worker_owned(self):
        reached, proceed = threading.Event(), threading.Event()

        def materialize(**kwargs):
            payload = self.owner.window()
            reached.set()
            if not proceed.wait(WAIT):
                raise AssertionError("test did not release materializer")
            raise TimeoutError("worker deadline")

        self.server._incremental_payout_window_materialization = materialize
        self.addCleanup(proceed.set)
        self.schedule()
        self.wait(reached)
        with self.assertRaises(TimeoutError):
            self.executor.futures[0].exception(timeout=0)
        self.assertEqual(self.owner.counts()["buffers"], 1)
        self.assertFalse(self.executor.futures[0].done())
        proceed.set()
        self.executor.drain()
        self.assertIsNone(self.executor.futures[0].exception())
        self.retire()
        self.released()

    def test_exceptional_phase_flush_retires_future(self):
        def flush(phases):
            payload = self.owner.window()
            raise RuntimeError("injected phase flush failure")

        self.server._flush_job_build_phases = flush
        self.schedule()
        self.executor.drain()
        self.retire()
        self.assertIsNone(self.server._payout_artifact_future)
        self.released()

    def test_escaped_install_failure_lifetime_after_join(self):
        original = self.server._build_payout_ledger_artifact

        def build(*args, **kwargs):
            return self.owner.watch("artifact", replace(
                original(*args, **kwargs), shares_json=self.owner.window(parsed=True)))

        def install(artifact):
            raise ValueError("escaped install payload")

        self.server._build_payout_ledger_artifact = build
        self.server._install_payout_ledger_artifact = install
        self.schedule()
        self.executor.drain()
        self.retire()
        # Intentionally do not clear the service Future: it is the owner
        # under audit, and must retire itself on every exit.
        self.released()

    def test_generation_supersession_discards_built_window(self):
        original = self.server._build_payout_ledger_artifact

        def build(*args, **kwargs):
            artifact = replace(original(*args, **kwargs), shares_json=self.owner.window())
            self.owner.watch("artifact", artifact)
            with self.server._job_cache_lock:
                self.server._payout_state_generation += 1
            return artifact

        self.server._build_payout_ledger_artifact = build
        self.schedule()
        self.executor.drain()
        self.assertIsNone(self.server._payout_ledger_artifact)
        self.assertEqual(self.server._payout_artifact_rearm_backoff, 2)
        self.retire()
        self.released()

    def test_queued_shutdown_retires_future_and_latest_slot(self):
        release = held_worker(self.executor.executor)
        self.addCleanup(release.set)
        self.schedule()
        self.schedule(42)
        self.executor.shutdown(wait=False, cancel_futures=True)
        self.assertTrue(self.executor.futures[0].cancelled())
        release.set()
        self.server.shutdown_payout_artifact_executor()
        self.assertIsNone(self.server._payout_artifact_future)
        self.assertIsNone(self.server._payout_artifact_requested)
        self.assertFalse(self.server._payout_artifact_requested_bypass)
        self.schedule()  # Shutdown cannot reopen admission.
        self.assertEqual(len(self.executor.futures), 1)
        self.retire()
        self.released()

    def test_fatal_worker_exit_releases_slot_and_can_readmit(self):
        class FatalPreparation(BaseException):
            pass

        original = self.server._install_payout_ledger_artifact

        def install(artifact):
            payload = self.owner.window()
            raise FatalPreparation("fatal preparation")

        self.server._install_payout_ledger_artifact = install
        self.schedule()
        self.assertIsInstance(self.executor.futures[0].exception(WAIT), FatalPreparation)
        self.assertIsNone(self.server._payout_artifact_future)
        self.server._install_payout_ledger_artifact = original
        self.schedule()
        self.executor.drain()
        self.assertEqual(len(self.executor.futures), 2)
        self.assertIsNotNone(self.server._payout_ledger_artifact)
        self.retire()
        self.released()

    def test_fatal_worker_exit_drains_latest_queued_request(self):
        self._assert_fatal_exit_drains_latest_request(diagnostic_failure=False)

    def test_diagnostic_failure_drains_latest_queued_request(self):
        self._assert_fatal_exit_drains_latest_request(diagnostic_failure=True)

    def test_fatal_worker_exit_does_not_resubmit_after_shutdown(self):
        class FatalPreparation(BaseException):
            pass

        reached, proceed, shutting_down = (
            threading.Event(), threading.Event(), threading.Event())
        original_shutdown = self.executor.shutdown
        shutdown_errors = []

        def install(artifact):
            payload = self.owner.window(parsed=True)
            reached.set()
            if not proceed.wait(WAIT):
                raise AssertionError("test did not release install")
            raise FatalPreparation("fatal preparation during shutdown")

        def shutdown(**kwargs):
            shutting_down.set()  # Service has closed admission under its lock.
            return original_shutdown(**kwargs)

        def shutdown_service():
            try:
                self.server.shutdown_payout_artifact_executor()
            except BaseException as error:
                shutdown_errors.append(error)

        self.server._install_payout_ledger_artifact = install
        self.executor.shutdown = shutdown
        self.addCleanup(proceed.set)
        self.schedule()
        self.wait(reached)
        self.server._schedule_payout_ledger_artifact_preparation(
            42, self.difficulty, bypass_build_interval=True)
        thread = threading.Thread(target=shutdown_service)
        thread.start()
        try:
            self.wait(shutting_down)
        finally:
            proceed.set()
            thread.join(WAIT)
        self.assertFalse(thread.is_alive())
        self.assertEqual(shutdown_errors, [])
        self.assertEqual(len(self.executor.futures), 1)
        self.assertIsInstance(self.executor.futures[0].exception(WAIT), FatalPreparation)
        self.assertIsNone(self.server._payout_artifact_future)
        self.assertIsNone(self.server._payout_artifact_requested)
        self.assertFalse(self.server._payout_artifact_requested_bypass)
        self.retire()
        self.released()

    def _assert_fatal_exit_drains_latest_request(self, *, diagnostic_failure):
        class FatalPreparation(BaseException):
            pass

        reached, proceed = threading.Event(), threading.Event()
        calls = []
        original_prepare = self.server._prepare_payout_ledger_artifact
        original_install = self.server._install_payout_ledger_artifact

        def prepare(generation, difficulty, *, bypass_build_interval=False):
            calls.append((generation, bypass_build_interval))
            return original_prepare(
                generation, difficulty, bypass_build_interval=bypass_build_interval)

        def install(artifact):
            if len(calls) == 1:
                payload = self.owner.window(parsed=True)
                reached.set()
                if not proceed.wait(WAIT):
                    raise AssertionError("test did not release install")
                if diagnostic_failure:
                    raise ValueError("preparation failure before diagnostic failure")
                raise FatalPreparation("fatal preparation with queued work")
            return original_install(artifact)

        def fail_diagnostics():
            raise OSError("diagnostic sink failed")

        self.server._prepare_payout_ledger_artifact = prepare
        self.server._install_payout_ledger_artifact = install
        self.addCleanup(proceed.set)
        with patch("lab.prism.payout_state.traceback.print_exc", new=fail_diagnostics):
            self.schedule()
            self.wait(reached)
            self.schedule(41)
            self.server._schedule_payout_ledger_artifact_preparation(
                0, self.difficulty, bypass_build_interval=True)
            self.schedule(0)  # Coalescing must preserve the pending bypass bit.
            self.assertEqual(len(self.executor.futures), 1)
            proceed.set()
            self.assertIsInstance(
                self.executor.futures[0].exception(WAIT),
                OSError if diagnostic_failure else FatalPreparation,
            )
            self.assertEqual(len(self.executor.futures), 2)
            # Wait before drain shuts down the executor: the failed worker
            # must submit its successor while the executor still accepts work.
            self.assertIsNone(self.executor.futures[1].exception(WAIT))
            self.executor.drain()
        self.assertEqual(calls, [(0, False), (0, True)])
        self.assertIsNotNone(self.server._payout_ledger_artifact)
        self.assertIsNone(self.server._payout_artifact_future)
        self.assertIsNone(self.server._payout_artifact_requested)
        self.assertFalse(self.server._payout_artifact_requested_bypass)
        self.retire()
        self.released()
