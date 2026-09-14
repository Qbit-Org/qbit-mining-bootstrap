"""Bounded weak-reference instrumentation for isolated #341 tests/replays."""

from __future__ import annotations

import gc
import threading
import unittest
import weakref
from concurrent.futures import ThreadPoolExecutor

from lab.prism.share_ledger import DaemonShareJsonSequence

WAIT = 10


class Window(DaemonShareJsonSequence):
    """Weak-referenceable on the pre-#335 base too; same bytes/parse methods."""


class Ownership:
    def __init__(self):
        self.refs = []

    def watch(self, kind, value):
        self.refs.append((kind, weakref.ref(value)))
        return value

    def window(self, rows=32, *, parsed=False):
        # 623 bytes/record including separator: 249.2 MB at 400k records.
        row = b'{"blob":"' + b'x' * 600 + b'","weight":1}'
        value = self.watch("window", Window(b','.join([row] * rows), rows))
        if parsed:
            value[0]
        return value

    def counts(self):
        live = {}
        buffers = {}
        parsed_rows = 0
        for kind, reference in self.refs:
            value = reference()
            if value is None:
                continue
            live[kind] = live.get(kind, 0) + 1
            if isinstance(value, DaemonShareJsonSequence):
                buffers[id(value.canonical_items)] = len(value.canonical_items)
                parsed_rows += len(value._parsed or ())
        return dict(live=live, buffers=len(buffers),
                    canonical_bytes=sum(buffers.values()), parsed_rows=parsed_rows)


class RecordingExecutor:
    """Real workers; the observer releases its Futures explicitly after join."""

    def __init__(self, ownership, executor=None, *, inline=False):
        self.owner = ownership
        self.executor = executor or ThreadPoolExecutor(max_workers=1)
        self.inline = inline
        self.futures = []

    def submit(self, function, *args, **kwargs):
        for value in args:
            if type(value).__name__ in ("PendingInitialJob", "IdleRetargetRequest"):
                self.owner.watch("request", value)
        future = self.executor.submit(function, *args, **kwargs)
        self.owner.watch("future", future)
        self.futures.append(future)
        if self.inline:
            register = future.add_done_callback

            def after_completion(callback):
                future.exception(timeout=WAIT)  # no stored-error rethrow
                register(callback)

            future.add_done_callback = after_completion
        return future

    def cancel(self, future):
        return self.executor.cancel(future)

    def shutdown(self, **kwargs):
        self.executor.shutdown(**kwargs)

    def drain(self):
        self.executor.shutdown(wait=True)
        if self.inline:
            for future in self.futures:
                # Retire this test-only closure's reference back to Future.
                del future.add_done_callback


class LifetimeCase(unittest.TestCase):
    def setUp(self):
        self.was_enabled = gc.isenabled()
        gc.disable()
        self.owner = Ownership()
        self.addCleanup(self.restore_gc)

    def restore_gc(self):
        if self.was_enabled:
            gc.enable()
        # Only teardown, after all lifetime assertions (even failed ones).
        gc.collect()

    def released(self):
        self.assertFalse(gc.isenabled())
        self.assertEqual(self.owner.counts(), dict(
            live={}, buffers=0, canonical_bytes=0, parsed_rows=0))

    def wait(self, event):
        self.assertTrue(event.wait(WAIT), "worker did not reach event")


def retire_jobs(server, *clients):
    """Fixture's declared end of all mining work, after worker join.

    First use real disconnect/history expiry. The client object itself also
    retains its last context, so release that fixture owner after disconnect.
    No live service code uses this artificial end-of-history operation.
    """
    for state in clients:
        server.disconnect_client(state)
        state.active_job = None
    server.prune_evicted_job_graveyard(now=1e30)
    server._job_bundle_cache.clear()
    server._prepared_ready_bundle = None
    server._payout_ledger_artifact = None
    server._incremental_payout_artifact_window = None


def held_worker(executor):
    started, release = threading.Event(), threading.Event()

    def hold():
        started.set()
        if not release.wait(WAIT):
            raise AssertionError("test did not release worker")

    executor.submit(hold)
    if not started.wait(WAIT):
        raise AssertionError("test worker did not start")
    return release
