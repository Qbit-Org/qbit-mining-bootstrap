"""Deferred capacity is a wake signal; verify completed consumer ownership."""

from concurrent.futures import Future, ThreadPoolExecutor, CancelledError
import threading

from lab.prism.job_bundle import JobBuildSuperseded, _await_job_build_promise
from tests.prism_async_ownership_support import LifetimeCase, WAIT
from tests.prism_coordinator_test_support import coordinator


class DeferredCapacityOwnershipTests(LifetimeCase):
    def setUp(self):
        super().setUp()
        self.server, _ = coordinator()
        self.service = self.server._ensure_job_bundle_service()

    def consume(self, deferred, outcomes):
        payload = self.owner.window(parsed=True)
        try:
            _await_job_build_promise(deferred, WAIT)
        except JobBuildSuperseded as error:
            outcomes.append((type(error), error.args, error is deferred.exception()))

    def test_multiple_and_late_consumers_release_after_all_blockers(self):
        blockers = [self.owner.watch("blocker", Future()) for _ in range(2)]
        deferred = self.owner.watch("deferred", self.service._defer_job_build_locked(*blockers))
        outcomes = []
        ready = threading.Barrier(4)

        def consume():
            ready.wait(WAIT)
            self.consume(deferred, outcomes)

        threads = [threading.Thread(target=consume) for _ in range(3)]
        for thread in threads:
            thread.start()
        ready.wait(WAIT)
        blockers[0].set_result(self.owner.window())
        for thread in threads:
            thread.join(WAIT)
            self.assertFalse(thread.is_alive())
        self.consume(deferred, outcomes)  # late consumer, same stored signal
        self.assertEqual(len(outcomes), 4)
        self.assertTrue(all(kind is JobBuildSuperseded and not shared
                            for kind, args, shared in outcomes))
        self.assertTrue(all(args == ("job build capacity became available; retrying",)
                            for kind, args, shared in outcomes))
        self.assertIsNone(deferred.exception().__traceback__)
        # The second callback still legitimately awaits its blocker. Once it
        # runs, neither it nor the first callback may keep consumer payloads.
        blockers[1].set_exception(ValueError("blocker failed"))
        del blockers, deferred
        self.released()

    def test_already_complete_blockers_and_all_outcomes(self):
        for outcome in ("success", "failure", "timeout", "cancelled"):
            with self.subTest(outcome=outcome):
                blocker = self.owner.watch("blocker", Future())
                if outcome == "success":
                    blocker.set_result(self.owner.window())
                elif outcome == "cancelled":
                    blocker.cancel()
                else:
                    blocker.set_exception((TimeoutError if outcome == "timeout" else ValueError)(outcome))
                deferred = self.owner.watch("deferred", self.service._defer_job_build_locked(blocker))
                outcomes = []
                self.consume(deferred, outcomes)
                self.assertEqual(len(outcomes), 1)
                del blocker, deferred
                self.released()

    def test_true_join_expiry_and_cancelled_deferred(self):
        blocker = self.owner.watch("blocker", Future())
        deferred = self.owner.watch("deferred", self.service._defer_job_build_locked(blocker))
        with self.assertRaises(TimeoutError):
            _await_job_build_promise(deferred, 0)
        self.assertFalse(blocker.done())
        self.assertFalse(deferred.done())
        deferred.cancel()
        blocker.set_result(self.owner.window())
        self.assertTrue(deferred.cancelled())
        # Cancellation consumers throw their own CancelledError, not a
        # traceback-bearing stored error. All observer owners then retire.
        with self.assertRaises(CancelledError):
            _await_job_build_promise(deferred, WAIT)
        del blocker, deferred
        self.released()

    def test_shutdown_cancels_real_queued_blocker_and_wakes_waiter(self):
        executor = ThreadPoolExecutor(max_workers=1)
        started, proceed = threading.Event(), threading.Event()

        def hold():
            started.set()
            proceed.wait(WAIT)

        executor.submit(hold)
        self.wait(started)
        blocker = self.owner.watch("blocker", executor.submit(lambda: None))
        deferred = self.owner.watch("deferred", self.service._defer_job_build_locked(blocker))
        executor.shutdown(wait=False, cancel_futures=True)
        self.assertTrue(blocker.cancelled())
        outcomes = []
        self.consume(deferred, outcomes)
        proceed.set()
        executor.shutdown(wait=True)
        self.assertEqual(len(outcomes), 1)
        del blocker, deferred
        self.released()
