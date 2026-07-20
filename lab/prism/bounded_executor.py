"""Bounded priority executor used for PRISM job delivery."""

from __future__ import annotations

from concurrent.futures import Future
import heapq
import queue
import threading
from typing import Any, Callable


class _DeliveryQueueFull(RuntimeError):
    """The bounded delivery executor cannot admit another task."""


class _BoundedPriorityExecutor:
    """Small Future-compatible executor with bounded, priority-ordered work."""

    # A short poll keeps idle workers promptly joinable without requiring one
    # poison-pill queue slot per worker (the queue may be smaller than the
    # worker pool).
    _WORKER_POLL_SECONDS = 0.01

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
        self._cancel_futures_on_shutdown = False
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
            try:
                item = self._queue.get(timeout=self._WORKER_POLL_SECONDS)
            except queue.Empty:
                with self._lock:
                    if self._shutdown:
                        return
                continue
            _, _, future, function, args, kwargs = item
            assert isinstance(future, Future)
            with self._lock:
                cancel_for_shutdown = (
                    self._shutdown and self._cancel_futures_on_shutdown
                )
            if cancel_for_shutdown:
                future.cancel()
                self._queue.task_done()
                continue
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

        Cancelling a ``Future`` alone does not release bounded queue capacity
        until a worker dequeues it. Removing the exact entry under the queue
        mutex makes that capacity available to a replacement immediately.
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
        # Invoke callbacks only after releasing the queue mutex. A cancellation
        # callback may submit the replacement that consumes the reclaimed slot.
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
                self._cancel_futures_on_shutdown = cancel_futures
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
        if wait:
            for thread in threads:
                thread.join()
