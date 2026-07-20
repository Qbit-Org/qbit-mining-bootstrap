#!/usr/bin/env python3
"""Focused tests for PRISM's bounded priority executor."""

from __future__ import annotations

import threading
import unittest

from lab.prism import prism_coordinator
from lab.prism.bounded_executor import (
    _BoundedPriorityExecutor,
    _DeliveryQueueFull,
)


class BoundedPriorityExecutorTests(unittest.TestCase):
    def test_compatibility_reexports_reference_executor_owner(self) -> None:
        self.assertIs(
            prism_coordinator._BoundedPriorityExecutor,
            _BoundedPriorityExecutor,
        )
        self.assertIs(prism_coordinator._DeliveryQueueFull, _DeliveryQueueFull)

    def test_queue_bound_excludes_active_worker_and_reports_active_count(self) -> None:
        executor = _BoundedPriorityExecutor(max_workers=1, max_queue_size=1)
        blocker_started = threading.Event()
        release = threading.Event()

        def blocker() -> None:
            blocker_started.set()
            release.wait(5)

        try:
            executor.submit(blocker)
            self.assertTrue(blocker_started.wait(5))
            queued = executor.submit(lambda: None)
            self.assertEqual(executor.stats(), (1, 1))
            with self.assertRaisesRegex(_DeliveryQueueFull, "queue is full"):
                executor.submit(lambda: None)
            release.set()
            queued.result(5)
        finally:
            release.set()
            executor.shutdown(wait=True, cancel_futures=True)

    def test_shutdown_cancels_queued_future_and_joins_named_workers(self) -> None:
        executor = _BoundedPriorityExecutor(max_workers=2, max_queue_size=2)
        blocker_started = [threading.Event(), threading.Event()]
        release = threading.Event()

        def blocker(index: int) -> None:
            blocker_started[index].set()
            release.wait(5)

        executor.submit(blocker, 0)
        executor.submit(blocker, 1)
        self.assertTrue(all(started.wait(5) for started in blocker_started))
        queued = executor.submit(lambda: None)

        executor.shutdown(wait=False, cancel_futures=True)
        self.assertTrue(queued.cancelled())
        self.assertEqual(
            [thread.name for thread in executor._threads],
            ["prism-job-delivery-1", "prism-job-delivery-2"],
        )
        release.set()
        executor.shutdown(wait=True)

        self.assertTrue(all(not thread.is_alive() for thread in executor._threads))

    def test_wait_false_shutdown_does_not_block_when_queue_is_smaller_than_pool(
        self,
    ) -> None:
        executor = _BoundedPriorityExecutor(max_workers=4, max_queue_size=1)
        blocker_started = [threading.Event() for _index in range(4)]
        release = threading.Event()

        def blocker(index: int) -> None:
            blocker_started[index].set()
            release.wait(5)

        for index in range(4):
            executor.submit(blocker, index)
            self.assertTrue(blocker_started[index].wait(5))
        queued = executor.submit(lambda: "drained")

        shutdown_returned = threading.Event()
        shutdown = threading.Thread(
            target=lambda: (
                executor.shutdown(wait=False),
                shutdown_returned.set(),
            )
        )
        shutdown.start()
        self.assertTrue(shutdown_returned.wait(0.5))
        shutdown.join(0.5)
        self.assertFalse(shutdown.is_alive())

        release.set()
        self.assertEqual(queued.result(5), "drained")
        executor.shutdown(wait=True)
        self.assertTrue(all(not thread.is_alive() for thread in executor._threads))


if __name__ == "__main__":
    unittest.main()
