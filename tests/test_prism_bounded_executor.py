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

    def test_cancel_discards_queued_entry_and_frees_bounded_admission(self) -> None:
        executor = _BoundedPriorityExecutor(max_workers=1, max_queue_size=1)
        blocker_started = threading.Event()
        release = threading.Event()

        def blocker() -> None:
            blocker_started.set()
            release.wait(5)

        try:
            executor.submit(blocker)
            self.assertTrue(blocker_started.wait(5))
            queued = executor.submit(lambda: "victim")
            with self.assertRaises(_DeliveryQueueFull):
                executor.submit(lambda: "rejected")

            self.assertTrue(executor.cancel(queued))
            self.assertTrue(queued.cancelled())
            # The cancelled entry is removed from the priority queue itself,
            # so bounded admission is immediately available again.
            replacement = executor.submit(lambda: "replacement")
            self.assertEqual(executor.stats()[0], 1)

            release.set()
            self.assertEqual(replacement.result(5), "replacement")
            # A worker that already dequeued an entry owns the task_done
            # path; cancelling a running/finished future removes nothing.
            self.assertFalse(executor.cancel(replacement))
        finally:
            release.set()
            executor.shutdown(wait=True, cancel_futures=True)


if __name__ == "__main__":
    unittest.main()
