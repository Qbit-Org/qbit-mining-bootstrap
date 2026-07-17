#!/usr/bin/env python3

from __future__ import annotations

import queue
import signal
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from lab.prism import prism_coordinator
from lab.prism.prism_coordinator import (
    CoordinatorShutdownController,
    PendingShareAppend,
    PrismCoordinator,
    ShutdownInProgress,
)


class RecordingLeaseLedger:
    backend_name = "recording"

    def __init__(self) -> None:
        self.release_calls = 0
        self.released = threading.Event()

    def release_writer_lease(self) -> bool:
        self.release_calls += 1
        self.released.set()
        return True


def coordinator(
    ledger: object | None = None,
    *,
    timeout: float = 0.5,
) -> PrismCoordinator:
    server = PrismCoordinator.__new__(PrismCoordinator)
    server.lock = threading.RLock()
    server.stop_event = threading.Event()
    server.writer_quiescence_timeout_seconds = timeout
    server._shutdown_controller = CoordinatorShutdownController(timeout)
    server.ledger = ledger or RecordingLeaseLedger()
    return server


class PrismCoordinatorShutdownTests(unittest.TestCase):
    def test_normal_shutdown_releases_lease_promptly_and_exports_metrics(self) -> None:
        ledger = RecordingLeaseLedger()
        server = coordinator(ledger)

        started = time.monotonic()
        with patch("builtins.print"):
            self.assertTrue(server.shutdown(reason="normal_return"))
        elapsed = time.monotonic() - started

        self.assertLess(elapsed, 0.2)
        self.assertEqual(ledger.release_calls, 1)
        metrics = "\n".join(server.shutdown_metrics_lines())
        self.assertIn("qbit_prism_shutdowns_total 1", metrics)
        self.assertIn(
            'qbit_prism_shutdown_writer_quiescence_total{outcome="success"} 1',
            metrics,
        )
        self.assertIn(
            'qbit_prism_shutdown_lease_release_total{outcome="success"} 1',
            metrics,
        )

    def test_blocked_non_writer_drain_starts_only_after_lease_release(self) -> None:
        ledger = RecordingLeaseLedger()
        server = coordinator(ledger)
        drain_started = threading.Event()
        unblock_drain = threading.Event()

        def blocked_executor_drain() -> None:
            drain_started.set()
            unblock_drain.wait(1)

        server.shutdown_tip_refresh_executor = blocked_executor_drain  # type: ignore[method-assign]
        with patch("builtins.print"):
            self.assertTrue(server.shutdown())
        self.assertTrue(ledger.released.is_set())

        drain_thread = threading.Thread(target=server.drain_non_writer_components)
        with patch("builtins.print"):
            drain_thread.start()
            self.assertTrue(drain_started.wait(0.2))
            self.assertTrue(ledger.released.is_set())
            self.assertTrue(drain_thread.is_alive())
            unblock_drain.set()
            drain_thread.join(1)
        self.assertFalse(drain_thread.is_alive())

    def test_pending_share_batch_flushes_before_release(self) -> None:
        append_started = threading.Event()
        allow_flush = threading.Event()
        release_saw_ack: list[bool] = []
        timeline: list[str] = []

        class Ledger(RecordingLeaseLedger):
            def append_batch(self, entries: object) -> list[object]:
                append_started.set()
                allow_flush.wait(1)
                timeline.append("share_flush")
                return [SimpleNamespace(share_seq=1)]

            def release_writer_lease(self) -> bool:
                release_saw_ack.append(entry.committed.is_set())
                timeline.append("lease_release")
                return super().release_writer_lease()

        ledger = Ledger()
        server = coordinator(ledger, timeout=1)
        server.share_append_queue = queue.Queue(maxsize=2)
        entry = PendingShareAppend(
            pending_share=SimpleNamespace(),
            username="miner-a",
            job_id="job-a",
            block_hash_hex="aa" * 32,
            collection_only=False,
            credit_policy=None,
        )
        server.enqueue_share_append(entry)

        def flush_one() -> None:
            queued = server.share_append_queue.get_nowait()
            server._append_share_batch([queued])

        writer_thread = threading.Thread(target=flush_one)
        writer_thread.start()
        self.assertTrue(append_started.wait(0.2))

        shutdown_result: list[bool] = []
        shutdown_thread = threading.Thread(
            target=lambda: shutdown_result.append(server.shutdown())
        )
        with patch("builtins.print"):
            shutdown_thread.start()
            time.sleep(0.03)
            self.assertEqual(ledger.release_calls, 0)
            allow_flush.set()
            writer_thread.join(1)
            shutdown_thread.join(1)

        self.assertEqual(shutdown_result, [True])
        self.assertEqual(timeline, ["share_flush", "lease_release"])
        self.assertEqual(release_saw_ack, [True])

    def test_share_writer_stays_alive_for_admitted_not_yet_queued_submit(self) -> None:
        class Ledger(RecordingLeaseLedger):
            def append_batch(self, entries: object) -> list[object]:
                return [SimpleNamespace(share_seq=1)]

        ledger = Ledger()
        server = coordinator(ledger, timeout=1)
        server.share_append_queue = queue.Queue(maxsize=2)
        server.share_commit_batch_size = 1
        server.share_commit_linger_seconds = 0
        server.share_commit_timeout_seconds = 1
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
        producer_admitted = threading.Event()
        allow_enqueue = threading.Event()

        entry = PendingShareAppend(
            pending_share=SimpleNamespace(),
            username="miner-a",
            job_id="job-a",
            block_hash_hex="aa" * 32,
            collection_only=False,
            credit_policy=None,
        )

        def producer() -> None:
            with server._writer_operation("share_submission"):
                producer_admitted.set()
                allow_enqueue.wait(1)
                server.enqueue_share_append(entry, wait=True)

        producer_thread = threading.Thread(target=producer)
        writer_thread = threading.Thread(target=server.share_append_loop)
        producer_thread.start()
        writer_thread.start()
        self.assertTrue(producer_admitted.wait(0.2))

        shutdown_thread = threading.Thread(target=server.shutdown)
        with patch("builtins.print"):
            shutdown_thread.start()
            time.sleep(0.03)
            self.assertTrue(writer_thread.is_alive())
            self.assertEqual(ledger.release_calls, 0)
            allow_enqueue.set()
            producer_thread.join(1)
            writer_thread.join(1)
            shutdown_thread.join(1)

        self.assertTrue(entry.committed.is_set())
        self.assertEqual(ledger.release_calls, 1)

    def test_blocked_writer_withholds_release_and_names_component(self) -> None:
        ledger = RecordingLeaseLedger()
        server = coordinator(ledger, timeout=0.03)
        entered = threading.Event()
        unblock = threading.Event()

        def blocked_writer() -> None:
            with server._writer_operation("accepted_block_handling"):
                entered.set()
                unblock.wait(1)

        writer = threading.Thread(target=blocked_writer)
        writer.start()
        self.assertTrue(entered.wait(0.2))
        with patch("builtins.print") as printed:
            self.assertFalse(server.shutdown())

        self.assertEqual(ledger.release_calls, 0)
        snapshot = server._ensure_shutdown_controller().snapshot()
        self.assertEqual(snapshot["release_withheld_total"], 1)
        rendered = " ".join(str(call) for call in printed.call_args_list)
        self.assertIn("accepted_block_handling", rendered)
        unblock.set()
        writer.join(1)
        with patch("builtins.print"):
            self.assertFalse(server.shutdown(reason="finally"))
        self.assertEqual(ledger.release_calls, 0)

    def test_repeated_shutdown_and_finally_release_at_most_once(self) -> None:
        ledger = RecordingLeaseLedger()
        server = coordinator(ledger)
        with patch("builtins.print"):
            self.assertTrue(server.shutdown(reason="serve_exit"))
            self.assertTrue(server.shutdown(reason="main_finally"))
            self.assertTrue(server.release_ledger_lease())
        self.assertEqual(ledger.release_calls, 1)

    def test_sigterm_closes_writer_admission_and_records_release_latency(self) -> None:
        ledger = RecordingLeaseLedger()
        server = coordinator(ledger)
        server.request_shutdown(signal.SIGTERM)

        with self.assertRaises(ShutdownInProgress):
            with server._writer_operation("share_submission"):
                pass
        with patch("builtins.print"):
            self.assertTrue(server.shutdown(reason="signal"))

        snapshot = server._ensure_shutdown_controller().snapshot()
        self.assertTrue(snapshot["sigterm_release_observed"])
        self.assertGreaterEqual(snapshot["sigterm_to_lease_release_seconds"], 0)

    def test_replacement_can_acquire_immediately_after_graceful_release(self) -> None:
        lease_lock = threading.Lock()
        holder: list[str | None] = [None]

        class LeaseLedger(RecordingLeaseLedger):
            def __init__(self, session: str) -> None:
                super().__init__()
                self.session = session

            def acquire(self) -> bool:
                with lease_lock:
                    if holder[0] is not None:
                        return False
                    holder[0] = self.session
                    return True

            def release_writer_lease(self) -> bool:
                with lease_lock:
                    if holder[0] != self.session:
                        return False
                    holder[0] = None
                return super().release_writer_lease()

        old = LeaseLedger("old")
        replacement = LeaseLedger("replacement")
        self.assertTrue(old.acquire())
        server = coordinator(old)
        with patch("builtins.print"):
            self.assertTrue(server.shutdown())
        self.assertTrue(replacement.acquire())

    def test_no_ledger_mutation_is_admitted_after_release(self) -> None:
        ledger = RecordingLeaseLedger()
        server = coordinator(ledger)
        with patch("builtins.print"):
            self.assertTrue(server.shutdown())

        with self.assertRaises(ShutdownInProgress):
            server.replay_recovered_shares()
        self.assertEqual(ledger.release_calls, 1)

    def test_shutdown_race_preserves_single_writer_invariant(self) -> None:
        ledger = RecordingLeaseLedger()
        server = coordinator(ledger, timeout=1)
        admitted = threading.Event()
        finish_writer = threading.Event()
        mutation_after_release: list[bool] = []

        def existing_writer() -> None:
            with server._writer_operation("payout_reconciliation"):
                admitted.set()
                finish_writer.wait(1)
                mutation_after_release.append(ledger.released.is_set())

        writer = threading.Thread(target=existing_writer)
        writer.start()
        self.assertTrue(admitted.wait(0.2))
        server.request_shutdown(signal.SIGTERM)
        with self.assertRaises(ShutdownInProgress):
            with server._writer_operation("ctv_broadcast_state"):
                pass

        shutdown_thread = threading.Thread(target=server.shutdown)
        with patch("builtins.print"):
            shutdown_thread.start()
            time.sleep(0.03)
            self.assertFalse(ledger.released.is_set())
            finish_writer.set()
            writer.join(1)
            shutdown_thread.join(1)

        self.assertEqual(mutation_after_release, [False])
        self.assertTrue(ledger.released.is_set())

    def test_main_normal_sigterm_and_exception_paths_run_controlled_finally(self) -> None:
        handlers: dict[int, object] = {}

        class FakeCoordinator:
            def __init__(self, mode: str) -> None:
                self.mode = mode
                self.events: list[object] = []

            def request_shutdown(self, signum: int | None = None) -> None:
                self.events.append(("request", signum))

            def serve(self) -> None:
                self.events.append("serve")
                if self.mode == "sigterm":
                    handler = handlers[signal.SIGTERM]
                    assert callable(handler)
                    handler(signal.SIGTERM, None)
                if self.mode == "exception":
                    raise RuntimeError("serve failed")

            def shutdown(self, *, reason: str) -> bool:
                self.events.append(("shutdown", reason))
                return True

            def drain_non_writer_components(self) -> None:
                self.events.append("drain")

        for mode in ("normal", "sigterm", "exception"):
            with self.subTest(mode=mode):
                fake = FakeCoordinator(mode)
                handlers.clear()
                with patch.object(
                    prism_coordinator,
                    "PrismCoordinator",
                    return_value=fake,
                ), patch.object(
                    prism_coordinator.signal,
                    "signal",
                    side_effect=lambda signum, handler: handlers.__setitem__(signum, handler),
                ):
                    if mode == "exception":
                        with self.assertRaisesRegex(RuntimeError, "serve failed"):
                            prism_coordinator.main()
                    else:
                        self.assertEqual(prism_coordinator.main(), 0)
                if mode == "sigterm":
                    self.assertIn(("request", signal.SIGTERM), fake.events)
                self.assertEqual(fake.events[-2:], [("shutdown", "main_finally"), "drain"])


if __name__ == "__main__":
    unittest.main()
