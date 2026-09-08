"""Bounded priority executor used for PRISM job delivery."""

from __future__ import annotations

from concurrent.futures import Future
import heapq
import math
import queue
import threading
from typing import Any, Callable


class _DeliveryQueueFull(RuntimeError):
    """The bounded delivery executor cannot admit another task."""


class _BoundedPriorityExecutor:
    """Small Future-compatible executor with bounded, priority-ordered work."""

    def __init__(
        self,
        *,
        max_workers: int,
        max_queue_size: int,
        thread_name_prefix: str = "prism-job-delivery",
    ) -> None:
        self.max_workers = max_workers
        self.max_queue_size = max_queue_size
        self._queue: queue.PriorityQueue[tuple[object, ...]] = queue.PriorityQueue(
            maxsize=max_queue_size
        )
        self._lock = threading.Lock()
        self._sequence = 0
        self._active_workers = 0
        self._shutdown = False
        self._threads = [
            threading.Thread(
                target=self._worker,
                name=f"{thread_name_prefix}-{index + 1}",
                daemon=True,
            )
            for index in range(max_workers)
        ]
        for thread in self._threads:
            thread.start()

    def submit(
        self,
        function: Callable[..., Any],
        /,
        *args: object,
        # Same-tip refreshes are the lowest-priority delivery class; the
        # numeric default stays owner-local so this executor never imports
        # the delivery-priority registry.
        priority: int = 2,
        **kwargs: object,
    ) -> Future[Any]:
        future: Future[Any] = Future()
        with self._lock:
            if self._shutdown:
                raise RuntimeError("delivery executor is shut down")
            self._sequence += 1
            item = (
                int(priority),
                self._sequence,
                future,
                function,
                args,
                kwargs,
            )
            try:
                self._queue.put_nowait(item)
            except queue.Full as exc:
                raise _DeliveryQueueFull("delivery executor queue is full") from exc
        return future

    def _worker(self) -> None:
        while True:
            item = self._queue.get()
            _, _, future, function, args, kwargs = item
            if function is None:
                self._queue.task_done()
                return
            assert isinstance(future, Future)
            if not future.set_running_or_notify_cancel():
                self._queue.task_done()
                continue
            with self._lock:
                self._active_workers += 1
            try:
                result = function(*args, **kwargs)
            except BaseException as exc:
                future.set_exception(exc)
            else:
                future.set_result(result)
            finally:
                with self._lock:
                    self._active_workers -= 1
                self._queue.task_done()

    def cancel(self, future: Future[Any]) -> bool:
        """Cancel ``future`` and immediately discard it when still queued.

        ``Future.cancel`` alone leaves the cancelled entry in PriorityQueue
        until a worker dequeues it. Removing the exact entry while holding the
        queue mutex makes bounded admission available to a replacement at the
        cancellation boundary. A worker that already dequeued the entry owns
        the normal ``task_done`` path, so the two paths cannot double-release.
        """
        removed = False
        with self._queue.mutex:
            queued_items = self._queue.queue
            for index, item in enumerate(queued_items):
                if item[2] is not future:
                    continue
                queued_items.pop(index)
                heapq.heapify(queued_items)
                self._queue.unfinished_tasks -= 1
                if self._queue.unfinished_tasks == 0:
                    self._queue.all_tasks_done.notify_all()
                self._queue.not_full.notify()
                removed = True
                break
        # Invoke callbacks only after releasing the queue mutex. Initial-job
        # cancellation callbacks may submit a replacement to this executor.
        future.cancel()
        return removed

    def stats(self) -> tuple[int, int]:
        with self._lock:
            return self._queue.qsize(), self._active_workers

    def shutdown(self, *, wait: bool = True, cancel_futures: bool = False) -> None:
        with self._lock:
            if self._shutdown:
                threads = list(self._threads)
                already_shutdown = True
            else:
                self._shutdown = True
                threads = list(self._threads)
                already_shutdown = False
        if already_shutdown:
            if wait:
                for thread in threads:
                    thread.join()
            return
        if cancel_futures:
            while True:
                try:
                    item = self._queue.get_nowait()
                except queue.Empty:
                    break
                future = item[2]
                if isinstance(future, Future):
                    future.cancel()
                self._queue.task_done()
        for index in range(len(threads)):
            self._queue.put((math.inf, index, None, None, (), {}))
        if wait:
            for thread in threads:
                thread.join()
