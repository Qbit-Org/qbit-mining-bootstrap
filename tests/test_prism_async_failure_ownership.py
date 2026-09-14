"""Issue #332: producer and consumer retirement must not wait for cyclic GC."""

import gc
import io
import threading
import unittest
import weakref
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor
from contextlib import redirect_stdout
from dataclasses import replace

from lab.prism.detached_failure import capture_failure, detached_future_result
from lab.prism.tip_refresh import RefreshResult, TemplateRefreshBlocked
from tests.prism_coordinator_test_support import coordinator, client, install_fake_bundle_builder


class Payload(list):
    pass


class AsyncFailureOwnershipTests(unittest.TestCase):
    def setUp(self):
        gc.collect()
        enabled = gc.isenabled()
        gc.disable()
        self.addCleanup(gc.enable if enabled else gc.disable)

    def _fanout(self, mode, epoch):
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        server.clients = [client(1), client(2)]
        server.tip_refresh_max_workers = 1
        server.tip_refresh_epoch_fanout = epoch
        server._tip_refresh_wave_reenters = lambda _passes: False
        references = []
        prepare = server.prepare_tip_refresh_bundle

        def tracked(*args, **kwargs):
            original = prepare(*args, **kwargs)
            window = Payload(original.shares_json)
            bundle = replace(original, shares_json=window)
            references.extend((weakref.ref(window), weakref.ref(bundle)))
            return bundle

        def deliver(*args, **kwargs):
            if mode == "blocked":
                raise TemplateRefreshBlocked("injected invalidation")
            if mode == "error":
                raise ValueError("injected error")
            if mode == "disconnect":
                raise OSError("injected disconnect")
            if mode == "superseded":
                server.current_tip_observation_sequence += 1
            if mode == "shutdown":
                server.stop_event.set()
            return RefreshResult("skipped")

        server.prepare_tip_refresh_bundle = tracked
        server.send_prepared_job = deliver
        server.disconnect_client = lambda state: None
        try:
            server.poll_qbit_tip_template_once()
        except TemplateRefreshBlocked:
            pass
        finally:
            server.shutdown_tip_refresh_executor()
            server.shutdown_job_build_executor()
            server._prepared_ready_bundle = None
        return server, references

    def test_fanout_releases_retired_payloads_before_gc(self):
        for epoch in (False, True):
            for mode in ("success", "superseded", "blocked", "error", "disconnect", "shutdown"):
                with self.subTest(epoch=epoch, mode=mode):
                    server, references = self._fanout(mode, epoch)
                    self.assertTrue(references)
                    self.assertTrue(all(ref() is None for ref in references))

    def _join(self, mode):
        server, _ = coordinator()
        service = server._ensure_reorg_reconciler_service()
        references = []

        def caller():
            payload = Payload()
            future = Future()
            references.extend((weakref.ref(payload), weakref.ref(future)))
            if mode == "success":
                future.set_result(True)
            elif mode == "cancelled":
                future.cancel()
            else:
                future.set_exception((TimeoutError if mode == "worker_timeout" else ValueError)(mode))
            return service.join_prefetch_bounded(future)

        output = io.StringIO()
        with redirect_stdout(output):
            try:
                caller()
            except (ValueError, TimeoutError, CancelledError):
                pass
        self.assertNotIn("join exceeded", output.getvalue())
        return references

    def test_prefetch_consumers_retire_without_gc(self):
        for mode in ("success", "failed", "worker_timeout", "cancelled"):
            with self.subTest(mode=mode):
                self.assertTrue(all(ref() is None for ref in self._join(mode)))

    def test_true_join_expiry_keeps_work_running_for_retry(self):
        server, _ = coordinator()
        server.reconcile_prefetch_join_timeout_seconds = 0.001
        service = server._ensure_reorg_reconciler_service()
        future = Future()
        with self.assertRaises(TimeoutError):
            service.join_prefetch_bounded(future)
        self.assertFalse(future.done())
        future.set_result(True)
        self.assertTrue(service.join_prefetch_bounded(future))

    def test_multiple_consumers_get_private_diagnostics_and_release_producer(self):
        references = []

        def producer():
            payload = Payload()
            references.append(weakref.ref(payload))
            try:
                raise OSError(5, "cause", "input")
            except OSError as cause:
                error = TemplateRefreshBlocked("blocked", 17)
                error.add_note("diagnostic note")
                raise error from cause

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(capture_failure, producer)
            future.result(5)
        self.assertIsNone(references[0]())
        errors = []

        def consume():
            try:
                detached_future_result(future)
            except TemplateRefreshBlocked as error:
                errors.append(error)

        threads = [threading.Thread(target=consume) for _ in range(3)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(5)
        self.assertEqual(len({id(error) for error in errors}), 3)
        for error in errors:
            self.assertEqual(error.args, ("blocked", 17))
            self.assertEqual(error.__cause__.filename, "input")
            self.assertIn("diagnostic note", error.__notes__)
            self.assertTrue(any("producer" in note for note in error.__notes__))
        self.assertIsNone(future.result()._error.__traceback__)


if __name__ == "__main__":
    unittest.main()
