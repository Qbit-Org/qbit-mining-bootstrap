#!/usr/bin/env python3

from __future__ import annotations

import queue
import re
import signal
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from lab.prism import prism_coordinator
from lab.prism.coordinator_shutdown import (
    CoordinatorShutdownController,
    ShutdownInProgress,
)
from lab.prism.prism_coordinator import (
    PendingShareAppend,
    PRISM_REJECTION_POOL_CLOSED,
    PrismCoordinator,
    StratumError,
    WriterLeaseRenewalDeferred,
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


class WatchdogLeaseLedger(RecordingLeaseLedger):
    def __init__(self) -> None:
        super().__init__()
        self.release_thread: threading.Thread | None = None

    def release_writer_lease_fresh_connection(self) -> bool:
        self.release_thread = threading.current_thread()
        return self.release_writer_lease()


class HeartbeatLeaseLedger(RecordingLeaseLedger):
    writer_lease_fast_adoption_capable = True
    writer_lease_guard_required = True

    def __init__(self) -> None:
        super().__init__()
        self.renew_calls = 0
        self.renew_thread: threading.Thread | None = None
        self.renewed = threading.Event()

    def renew_writer_lease_heartbeat(
        self,
        *,
        on_query_start=None,
    ) -> dict[str, int | str]:
        if on_query_start is not None:
            on_query_start()
        self.renew_calls += 1
        self.renew_thread = threading.current_thread()
        self.renewed.set()
        return {"backend": "recording", "renewed_count": 1}


class DeferredRenewalLeaseLedger(RecordingLeaseLedger):
    """Verification proves liveness with renewal deferred to an own write.

    Models a fenced write that outlasted the lease TTL: the committed row is
    expired, the SKIP LOCKED renewal skipped the tuple lock, and PostgreSQL
    attributes that lock to this process's own pooled backend. The session
    is live only as long as that write eventually commits.
    """

    writer_lease_fast_adoption_capable = True
    writer_lease_guard_required = True

    def __init__(self) -> None:
        super().__init__()
        self.verify_calls = 0
        self.verified = threading.Event()

    def verify_writer_lease_guard_session(
        self,
        *,
        on_query_start=None,
    ) -> dict[str, int | str | bool]:
        if on_query_start is not None:
            on_query_start()
        self.verify_calls += 1
        self.verified.set()
        return {
            "backend": "recording",
            "verified_count": 1,
            "renewed_count": 0,
            "renewal_deferred_to_own_write": True,
        }


class GuardVerifyLeaseLedger(RecordingLeaseLedger):
    """Fake ledger with the non-locking guarded-session verification.

    ``lease_row_lock`` models the qbit_ledger_writer_lease tuple lock a
    fenced write holds for its entire transaction. The verification never
    needs it; the legacy locking renewal would (and in production died on
    the guard's statement timeout — the block-39416 restart loop), so this
    fake fails the test outright if the coordinator still calls it.
    """

    writer_lease_fast_adoption_capable = True
    writer_lease_guard_required = True

    def __init__(self) -> None:
        super().__init__()
        self.lease_row_lock = threading.Lock()
        self.fenced_write_started = threading.Event()
        self.verified = threading.Event()
        self.verify_calls = 0
        self.verify_calls_during_fenced_write = 0
        self.renew_calls = 0
        self.guard_lost = False

    def verify_writer_lease_guard_session(
        self,
        *,
        on_query_start=None,
    ) -> dict[str, int | str]:
        if on_query_start is not None:
            on_query_start()
        if self.guard_lost:
            raise RuntimeError("postgres writer lease guard is not held")
        self.verify_calls += 1
        if self.lease_row_lock.locked():
            self.verify_calls_during_fenced_write += 1
        self.verified.set()
        return {"backend": "recording", "verified_count": 1}

    def renew_writer_lease_heartbeat(
        self,
        *,
        on_query_start=None,
    ) -> dict[str, int | str]:
        self.renew_calls += 1
        raise AssertionError(
            "lease-row renewal must not run on the guarded session"
        )

    def persist_accepted_block(
        self,
        *,
        duration_seconds: float,
    ) -> dict[str, int | str]:
        with self.lease_row_lock:
            self.fenced_write_started.set()
            time.sleep(duration_seconds)
        return {"backend": "recording", "block_row_count": 1}


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


class CoordinatorShutdownControllerTests(unittest.TestCase):
    def test_compatibility_reexports_reference_shutdown_owner(self) -> None:
        self.assertIs(
            prism_coordinator.CoordinatorShutdownController,
            CoordinatorShutdownController,
        )
        self.assertIs(prism_coordinator.ShutdownInProgress, ShutdownInProgress)

    def test_nested_writer_inherits_admission_after_shutdown_request(self) -> None:
        controller = CoordinatorShutdownController(0.5)
        outer = controller.enter_writer("outer")
        controller.request_shutdown(signal.SIGTERM)
        inner = controller.enter_writer("inner")

        controller.exit_writer(inner)
        controller.exit_writer(outer)

        self.assertEqual(controller.snapshot()["active_writers"], {})
        with self.assertRaisesRegex(ShutdownInProgress, "coordinator is shutting down"):
            controller.enter_writer("late")

    def test_transferable_writer_token_finishes_idempotently_on_another_thread(
        self,
    ) -> None:
        controller = CoordinatorShutdownController(0.5)
        token = controller.reserve_writer("share_persistence")

        finisher = threading.Thread(target=lambda: (token.finish(), token.finish()))
        finisher.start()
        finisher.join(1)

        self.assertFalse(finisher.is_alive())
        self.assertTrue(token.finished)
        self.assertEqual(controller.snapshot()["active_writers"], {})


class PrismCoordinatorShutdownTests(unittest.TestCase):
    def test_refresh_timeout_still_drains_build_executors(self) -> None:
        server = coordinator()
        calls: list[str] = []
        server.shutdown_initial_job_executor = (  # type: ignore[method-assign]
            lambda: calls.append("initial")
        )
        server.shutdown_job_build_executor = (  # type: ignore[method-assign]
            lambda: calls.append("job_build")
        )
        server.shutdown_payout_artifact_executor = (  # type: ignore[method-assign]
            lambda: calls.append("payout_artifact")
        )
        server.shutdown_reconcile_prefetch_executor = (  # type: ignore[method-assign]
            lambda: calls.append("reconcile_prefetch")
        )
        server.retire_share_window_spool = (  # type: ignore[method-assign]
            lambda: calls.append("spool")
        )
        server.shutdown_serve_builder = (  # type: ignore[method-assign]
            lambda: calls.append("serve_builder")
        )

        server.shutdown_tip_refresh_executor()

        self.assertEqual(
            calls,
            [
                "initial",
                "job_build",
                "payout_artifact",
                "reconcile_prefetch",
                "spool",
                "serve_builder",
            ],
        )

    def test_startup_replay_shutdown_stops_cleanly_and_releases_lease_once(
        self,
    ) -> None:
        ledger = RecordingLeaseLedger()
        server = coordinator(ledger)

        def rejected_replay() -> int:
            server.request_shutdown(signal.SIGTERM)
            raise ShutdownInProgress("PRISM coordinator is shutting down")

        started = time.monotonic()
        with patch("builtins.print"):
            self.assertFalse(
                server._run_startup_writer_replay(
                    rejected_replay,
                    drain_threads=[],
                )
            )
        elapsed = time.monotonic() - started

        # No writer is active, so the replay exit never waits out the
        # quiescence budget before releasing the lease exactly once.
        self.assertLess(elapsed, 0.45)
        self.assertEqual(ledger.release_calls, 1)
        snapshot = server._ensure_shutdown_controller().snapshot()
        self.assertEqual(snapshot["active_writers"], {})
        self.assertFalse(snapshot["lease_release_withheld"])
        self.assertEqual(snapshot["release_withheld_total"], 0)
        self.assertEqual(snapshot["lease_release_outcomes"]["success"], 1)
        with patch("builtins.print"):
            server.shutdown(reason="main_finally")
        self.assertEqual(ledger.release_calls, 1)

    def test_lease_heartbeat_start_without_ledger_is_noop(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)

        with patch.object(prism_coordinator.threading, "Thread") as thread:
            self.assertIsNone(server._start_ledger_lease_heartbeat())

        thread.assert_not_called()

    def test_lease_heartbeat_start_fails_closed_when_required_guard_lost(
        self,
    ) -> None:
        class LostGuardAtStartupLedger(HeartbeatLeaseLedger):
            writer_lease_fast_adoption_capable = False

        server = coordinator(LostGuardAtStartupLedger())

        with patch.object(
            server,
            "_ledger_lease_heartbeat_hard_exit",
        ) as hard_exit:
            self.assertIsNone(server._start_ledger_lease_heartbeat())

        hard_exit.assert_called_once()

    def test_lease_heartbeat_start_skips_guardless_ledger_without_exit(
        self,
    ) -> None:
        server = coordinator(RecordingLeaseLedger())

        with patch.object(
            server,
            "_ledger_lease_heartbeat_hard_exit",
        ) as hard_exit:
            self.assertIsNone(server._start_ledger_lease_heartbeat())

        hard_exit.assert_not_called()

    def test_external_side_effect_gate_fails_closed_after_guard_loss(self) -> None:
        class LostGuardLedger(HeartbeatLeaseLedger):
            def renew_writer_lease_heartbeat(self) -> dict[str, int | str]:
                raise RuntimeError("postgres session was lost during host suspend")

        server = coordinator(LostGuardLedger())

        with patch.object(
            server,
            "_ledger_lease_heartbeat_hard_exit",
        ) as hard_exit, self.assertRaisesRegex(
            ShutdownInProgress,
            "writer lease guard verification failed",
        ):
            server._require_fresh_ledger_lease_for_external_side_effect(
                "submitblock"
            )

        hard_exit.assert_called_once()

    def test_external_side_effect_gate_allows_fresh_guarded_session(self) -> None:
        ledger = HeartbeatLeaseLedger()
        server = coordinator(ledger)

        server._require_fresh_ledger_lease_for_external_side_effect(
            "submitblock"
        )

        self.assertEqual(ledger.renew_calls, 1)
        self.assertIsNot(ledger.renew_thread, threading.current_thread())

    def test_external_side_effect_gate_does_not_regress_freshness_stamp(
        self,
    ) -> None:
        ledger = HeartbeatLeaseLedger()
        server = coordinator(ledger)
        verification_started_monotonic = 100.0
        fresher_heartbeat_monotonic = 101.0
        original_join = threading.Thread.join

        def join_then_publish_fresher_heartbeat(
            thread: threading.Thread,
            timeout: float | None = None,
        ) -> None:
            original_join(thread, timeout)
            self.assertFalse(thread.is_alive())
            self.assertTrue(ledger.renewed.is_set())
            server._ledger_lease_heartbeat_last_success_monotonic = (
                fresher_heartbeat_monotonic
            )

        with patch.object(
            prism_coordinator.time,
            "monotonic",
            return_value=verification_started_monotonic,
        ), patch.object(
            prism_coordinator.threading.Thread,
            "join",
            new=join_then_publish_fresher_heartbeat,
        ), patch(
            "lab.prism.prism_coordinator.os._exit",
            side_effect=AssertionError("unexpected hard exit"),
        ) as process_exit, patch.object(
            server,
            "_watchdog_hard_exit",
        ) as watchdog_hard_exit:
            server._require_fresh_ledger_lease_for_external_side_effect(
                "submitblock"
            )

        self.assertEqual(
            server._ledger_lease_heartbeat_last_success_monotonic,
            fresher_heartbeat_monotonic,
        )
        process_exit.assert_not_called()
        watchdog_hard_exit.assert_not_called()

    def test_external_side_effect_gate_defers_renewal_blocked_by_own_write(
        self,
    ) -> None:
        """Own-write lease deferral withholds the RPC without fencing the process.

        The guarded session is live — the heartbeat exemption keeps a slow
        fenced write from restart-looping the coordinator — but that
        survival argument assumes the write commits. A rollback would hand
        the expired row to a queued different-identity claimant while the
        external effect is in flight, so the fence must refuse the RPC. It
        must refuse without the hard exit: the process is healthy, and
        callers retry deferred work on their own cadence (broadcast pass
        interval, block-candidate outbox replay), succeeding once the
        commit lands a renewal.
        """
        ledger = DeferredRenewalLeaseLedger()
        server = coordinator(ledger)

        with patch.object(
            server,
            "_ledger_lease_heartbeat_hard_exit",
        ) as hard_exit, self.assertRaisesRegex(
            WriterLeaseRenewalDeferred,
            "renewal is deferred",
        ):
            server._require_fresh_ledger_lease_for_external_side_effect(
                "submitblock"
            )

        hard_exit.assert_not_called()
        self.assertEqual(ledger.verify_calls, 1)
        # The verification still proved the session live; the freshness
        # stamp must advance so the heartbeat monitor sees progress.
        self.assertIsNotNone(
            getattr(
                server,
                "_ledger_lease_heartbeat_last_success_monotonic",
                None,
            )
        )

    def test_lease_heartbeat_tolerates_renewal_deferred_to_own_write(self) -> None:
        """The heartbeat keeps running while renewal defers to an own write.

        Hard-exiting here would roll back the very write the lease defers
        to and restart-loop on every similarly slow block; only the
        external-side-effect fence treats the deferral as disqualifying.
        """
        ledger = DeferredRenewalLeaseLedger()
        server = coordinator(ledger)
        server.ledger_lease_heartbeat_seconds = 0.01

        with patch.object(
            server,
            "_ledger_lease_heartbeat_hard_exit",
        ) as hard_exit:
            thread = server._start_ledger_lease_heartbeat()
            self.assertIsNotNone(thread)
            self.assertTrue(ledger.verified.wait(0.2))
            server._ledger_lease_heartbeat_stop_event.set()
            assert thread is not None
            thread.join(1.0)

        hard_exit.assert_not_called()
        self.assertGreaterEqual(ledger.verify_calls, 1)

    def test_external_side_effect_gate_bounds_blocked_guard_verification(self) -> None:
        verification_started = threading.Event()
        release_verification = threading.Event()

        class BlockingGuardLedger(HeartbeatLeaseLedger):
            def renew_writer_lease_heartbeat(self) -> dict[str, int | str]:
                verification_started.set()
                release_verification.wait(1)
                return {"backend": "recording", "renewed_count": 1}

        ledger = BlockingGuardLedger()
        server = coordinator(ledger)
        server.ledger_lease_external_fence_timeout_seconds = 0.01

        try:
            started = time.monotonic()
            with patch.object(
                server,
                "_ledger_lease_heartbeat_hard_exit",
            ) as hard_exit, self.assertRaisesRegex(
                ShutdownInProgress,
                "writer lease guard verification timed out",
            ):
                server._require_fresh_ledger_lease_for_external_side_effect(
                    "ctv_submitpackage"
                )
            elapsed = time.monotonic() - started
        finally:
            release_verification.set()

        self.assertTrue(verification_started.is_set())
        self.assertLess(elapsed, 0.1)
        hard_exit.assert_called_once()
        if ledger.renew_thread is not None:
            ledger.renew_thread.join(0.2)

    def test_external_fence_survives_guard_queue_contention(self) -> None:
        guard_query_lock = threading.Lock()
        holder_ready = threading.Event()
        release_holder = threading.Event()

        class ContendedGuardLedger(HeartbeatLeaseLedger):
            def renew_writer_lease_heartbeat(
                self,
                *,
                on_query_start=None,
            ) -> dict[str, int | str]:
                with guard_query_lock:
                    return super().renew_writer_lease_heartbeat(
                        on_query_start=on_query_start
                    )

        def hold_guard_like_inflight_heartbeat() -> None:
            with guard_query_lock:
                holder_ready.set()
                release_holder.wait(1)

        ledger = ContendedGuardLedger()
        server = coordinator(ledger)
        server.ledger_lease_external_fence_timeout_seconds = 0.05
        server.ledger_lease_heartbeat_failure_seconds = 0.75
        holder = threading.Thread(target=hold_guard_like_inflight_heartbeat)
        holder.start()
        self.assertTrue(holder_ready.wait(0.2))
        unblock = threading.Timer(0.15, release_holder.set)
        unblock.start()

        try:
            with patch.object(
                server,
                "_ledger_lease_heartbeat_hard_exit",
            ) as hard_exit:
                server._require_fresh_ledger_lease_for_external_side_effect(
                    "submitblock"
                )
        finally:
            release_holder.set()
            unblock.cancel()
            holder.join(1.0)

        hard_exit.assert_not_called()
        self.assertEqual(ledger.renew_calls, 1)

    def test_external_fence_bounds_queue_wait_by_failure_budget(self) -> None:
        release_queue_slot = threading.Event()

        class QueuedForeverLedger(HeartbeatLeaseLedger):
            def renew_writer_lease_heartbeat(
                self,
                *,
                on_query_start=None,
            ) -> dict[str, int | str]:
                release_queue_slot.wait(1)
                return super().renew_writer_lease_heartbeat(
                    on_query_start=on_query_start
                )

        server = coordinator(QueuedForeverLedger())
        server.ledger_lease_external_fence_timeout_seconds = 0.02
        server.ledger_lease_heartbeat_failure_seconds = 0.05

        started = time.monotonic()
        try:
            with patch.object(
                server,
                "_ledger_lease_heartbeat_hard_exit",
            ) as hard_exit, self.assertRaisesRegex(
                ShutdownInProgress,
                "writer lease guard verification timed out",
            ):
                server._require_fresh_ledger_lease_for_external_side_effect(
                    "ctv_submitpackage"
                )
            elapsed = time.monotonic() - started
        finally:
            release_queue_slot.set()

        self.assertLess(elapsed, 0.5)
        hard_exit.assert_called_once()

    def test_fast_adoptable_lease_renews_on_dedicated_heartbeat_thread(self) -> None:
        ledger = HeartbeatLeaseLedger()
        server = coordinator(ledger)
        server.ledger_lease_heartbeat_seconds = 0.01

        thread = server._start_ledger_lease_heartbeat()
        self.assertIsNotNone(thread)
        self.assertTrue(ledger.renewed.wait(0.2))
        server.stop_event.set()
        assert thread is not None
        time.sleep(0.02)
        self.assertTrue(thread.is_alive())
        self.assertTrue(server._stop_ledger_lease_heartbeat())
        thread.join(0.2)

        self.assertFalse(thread.is_alive())
        self.assertGreaterEqual(ledger.renew_calls, 1)
        self.assertIs(ledger.renew_thread, thread)
        self.assertIsNot(ledger.renew_thread, threading.current_thread())

    def test_heartbeat_prefers_non_locking_guard_verification(self) -> None:
        ledger = GuardVerifyLeaseLedger()
        server = coordinator(ledger)
        server.ledger_lease_heartbeat_seconds = 0.01

        thread = server._start_ledger_lease_heartbeat()
        self.assertIsNotNone(thread)
        self.assertTrue(ledger.verified.wait(0.2))
        self.assertTrue(server._stop_ledger_lease_heartbeat())
        assert thread is not None
        thread.join(0.2)

        self.assertGreaterEqual(ledger.verify_calls, 1)
        self.assertEqual(ledger.renew_calls, 0)

    def test_heartbeat_survives_fenced_write_holding_lease_row_past_timeout(self) -> None:
        """Regression: block-39416 restart loop.

        persist_accepted_block holds the lease tuple for several multiples of
        the guard's statement timeout and of the heartbeat failure budget
        (production: 1.7s versus 0.5s/0.75s, scaled here). The heartbeat must
        keep passing throughout, no hard exit fires, and the fenced write
        completes.
        """
        ledger = GuardVerifyLeaseLedger()
        server = coordinator(ledger)
        server.ledger_lease_heartbeat_seconds = 0.01
        server.ledger_lease_heartbeat_failure_seconds = 0.1
        server.ledger_lease_heartbeat_monitor_seconds = 0.005

        with patch(
            "lab.prism.prism_coordinator.os._exit",
            side_effect=AssertionError("unexpected hard exit"),
        ) as process_exit, patch.object(
            server,
            "_watchdog_hard_exit",
        ) as hard_exit:
            thread = server._start_ledger_lease_heartbeat()
            self.assertIsNotNone(thread)
            self.assertTrue(ledger.verified.wait(0.2))

            result = ledger.persist_accepted_block(duration_seconds=0.3)

            assert thread is not None
            self.assertTrue(thread.is_alive())
            self.assertTrue(server._stop_ledger_lease_heartbeat())
            thread.join(0.2)

        self.assertEqual(result, {"backend": "recording", "block_row_count": 1})
        self.assertGreaterEqual(ledger.verify_calls_during_fenced_write, 1)
        self.assertEqual(ledger.renew_calls, 0)
        process_exit.assert_not_called()
        hard_exit.assert_not_called()

    def test_heartbeat_hard_exits_promptly_after_guard_connection_loss(self) -> None:
        ledger = GuardVerifyLeaseLedger()
        server = coordinator(ledger)
        server.ledger_lease_heartbeat_seconds = 0.01
        exited = threading.Event()

        with patch("builtins.print"), patch("traceback.print_exc"), patch.object(
            server,
            "_watchdog_hard_exit",
            side_effect=lambda *_args, **_kwargs: exited.set(),
        ) as hard_exit:
            thread = server._start_ledger_lease_heartbeat()
            self.assertIsNotNone(thread)
            self.assertTrue(ledger.verified.wait(0.2))

            ledger.guard_lost = True
            self.assertTrue(exited.wait(0.5))
            assert thread is not None
            thread.join(0.5)
            server._ledger_lease_heartbeat_stop_event.set()

        hard_exit.assert_called_once_with("lease_heartbeat", timeout_seconds=0.1)

    def test_external_fence_is_non_locking_during_accepted_block_persistence(self) -> None:
        ledger = GuardVerifyLeaseLedger()
        server = coordinator(ledger)
        # Tighter than the fenced write's duration: a fence that queued on
        # the lease tuple would time out and hard-exit here.
        server.ledger_lease_external_fence_timeout_seconds = 0.05
        persist_thread = threading.Thread(
            target=ledger.persist_accepted_block,
            kwargs={"duration_seconds": 0.3},
        )
        persist_thread.start()
        self.assertTrue(ledger.fenced_write_started.wait(0.5))

        try:
            with patch.object(
                server,
                "_ledger_lease_heartbeat_hard_exit",
            ) as hard_exit:
                server._require_fresh_ledger_lease_for_external_side_effect(
                    "submitblock"
                )
        finally:
            persist_thread.join(1.0)

        self.assertFalse(persist_thread.is_alive())
        hard_exit.assert_not_called()
        self.assertGreaterEqual(ledger.verify_calls_during_fenced_write, 1)
        self.assertEqual(ledger.renew_calls, 0)

    def test_external_fence_fails_closed_after_guard_loss_with_verification(self) -> None:
        ledger = GuardVerifyLeaseLedger()
        ledger.guard_lost = True
        server = coordinator(ledger)

        with patch.object(
            server,
            "_ledger_lease_heartbeat_hard_exit",
        ) as hard_exit, self.assertRaisesRegex(
            ShutdownInProgress,
            "writer lease guard verification failed",
        ):
            server._require_fresh_ledger_lease_for_external_side_effect(
                "submitblock"
            )

        hard_exit.assert_called_once()

    def test_lease_heartbeat_failure_uses_bounded_hard_exit_path(self) -> None:
        class FailingHeartbeatLedger(HeartbeatLeaseLedger):
            def renew_writer_lease_heartbeat(self) -> dict[str, int | str]:
                raise RuntimeError("database unavailable")

        server = coordinator(FailingHeartbeatLedger())

        with patch("builtins.print"), patch("traceback.print_exc"), patch.object(
            server,
            "_watchdog_hard_exit",
        ) as hard_exit:
            server.ledger_lease_heartbeat_loop()

        hard_exit.assert_called_once_with("lease_heartbeat", timeout_seconds=0.1)

    def test_heartbeat_hard_exit_does_not_deadlock_lease_release(self) -> None:
        released = threading.Event()
        exited = threading.Event()

        class FailingReleaseLedger(HeartbeatLeaseLedger):
            def renew_writer_lease_heartbeat(self) -> dict[str, int | str]:
                raise RuntimeError("database unavailable")

            def release_writer_lease_fresh_connection(self) -> bool:
                released.set()
                return True

        server = coordinator(FailingReleaseLedger())

        with patch("builtins.print"), patch("traceback.print_exc"), patch(
            "lab.prism.prism_coordinator.os._exit",
            side_effect=lambda _code: exited.set(),
        ):
            thread = threading.Thread(target=server.ledger_lease_heartbeat_loop)
            server._ledger_lease_heartbeat_thread = thread
            thread.start()
            self.assertTrue(exited.wait(1.0))
            thread.join(1.0)

        self.assertFalse(thread.is_alive())
        # The heartbeat thread that armed the exit is parked joining the
        # release worker; the release must still reach the database instead
        # of burning the exit budget joining that same thread.
        self.assertTrue(released.is_set())

    def test_lease_heartbeat_hard_exit_never_waits_on_blocked_output(self) -> None:
        server = coordinator(HeartbeatLeaseLedger())
        release_print = threading.Event()
        hard_exit_called = threading.Event()

        def blocking_print(*_args: object, **_kwargs: object) -> None:
            release_print.wait(1)

        with patch("builtins.print", side_effect=blocking_print) as print_call, patch.object(
            server,
            "_watchdog_hard_exit",
            side_effect=lambda *_args, **_kwargs: hard_exit_called.set(),
        ) as hard_exit:
            caller = threading.Thread(
                target=server._ledger_lease_heartbeat_hard_exit,
                args=("heartbeat failed",),
                kwargs={"include_traceback": False},
            )
            caller.start()
            reached_hard_exit = hard_exit_called.wait(0.1)
            release_print.set()
            caller.join(0.2)

        self.assertTrue(reached_hard_exit)
        self.assertFalse(caller.is_alive())
        print_call.assert_not_called()
        hard_exit.assert_called_once_with("lease_heartbeat", timeout_seconds=0.1)

    def test_heartbeat_start_waits_for_first_exact_session_renewal(self) -> None:
        renew_started = threading.Event()
        release_renew = threading.Event()

        class ArmingHeartbeatLedger(HeartbeatLeaseLedger):
            def renew_writer_lease_heartbeat(self) -> dict[str, int | str]:
                renew_started.set()
                release_renew.wait(1)
                return {"backend": "recording", "renewed_count": 1}

        server = coordinator(ArmingHeartbeatLedger())
        result: list[threading.Thread | None] = []
        starter = threading.Thread(
            target=lambda: result.append(server._start_ledger_lease_heartbeat())
        )
        starter.start()
        self.assertTrue(renew_started.wait(0.2))
        time.sleep(0.02)
        self.assertTrue(starter.is_alive())

        release_renew.set()
        starter.join(0.2)
        self.assertFalse(starter.is_alive())
        self.assertEqual(len(result), 1)
        self.assertIsNotNone(result[0])
        self.assertTrue(server._stop_ledger_lease_heartbeat())

    def test_stalled_lease_heartbeat_exits_before_adoption_silence(self) -> None:
        renew_started = threading.Event()
        release_renew = threading.Event()

        class BlockingHeartbeatLedger(HeartbeatLeaseLedger):
            def renew_writer_lease_heartbeat(self) -> dict[str, int | str]:
                renew_started.set()
                release_renew.wait(1)
                return {"backend": "recording", "renewed_count": 1}

        server = coordinator(BlockingHeartbeatLedger())
        server.ledger_lease_heartbeat_failure_seconds = 0.03
        server.ledger_lease_heartbeat_monitor_seconds = 0.005
        server.ledger_lease_heartbeat_exit_timeout_seconds = 0.01
        thread: threading.Thread | None = None

        try:
            with patch("builtins.print"), patch.object(
                server,
                "_watchdog_hard_exit",
            ) as hard_exit:
                thread = server._start_ledger_lease_heartbeat()
                self.assertTrue(renew_started.wait(0.2))
                deadline = time.monotonic() + 0.2
                while not hard_exit.called and time.monotonic() < deadline:
                    time.sleep(0.001)

                hard_exit.assert_called_once_with(
                    "lease_heartbeat",
                    timeout_seconds=0.01,
                )
        finally:
            release_renew.set()
            heartbeat_stop = getattr(
                server,
                "_ledger_lease_heartbeat_stop_event",
                None,
            )
            if heartbeat_stop is not None:
                heartbeat_stop.set()
            if thread is not None:
                thread.join(0.2)

    def test_heartbeat_survives_steady_slow_but_successful_verifications(
        self,
    ) -> None:
        """Regression: healthy-writer hard exit at legal verification latency.

        The success timestamp is the call-start edge, so with steady
        verification duration D the freshest edge reaches age 2D plus the
        idle interval just before the next success lands. Keyed on success
        edges alone (D=0.15, interval=0.05, budget=0.25 here), the monitor
        fires ~0.35s into a run in which every verification succeeds. The
        attempt-start and completion progress marks keep the observable
        silence at D, so a sole writer whose statements stay inside their
        legal budgets is never hard-exited.
        """

        class SlowVerifyLedger(RecordingLeaseLedger):
            writer_lease_fast_adoption_capable = True
            writer_lease_guard_required = True

            def __init__(self) -> None:
                super().__init__()
                self.verify_calls = 0

            def verify_writer_lease_guard_session(
                self,
            ) -> dict[str, int | str]:
                time.sleep(0.15)
                self.verify_calls += 1
                return {"backend": "recording", "verified_count": 1}

        ledger = SlowVerifyLedger()
        server = coordinator(ledger)
        server.ledger_lease_heartbeat_seconds = 0.05
        server.ledger_lease_heartbeat_failure_seconds = 0.25
        server.ledger_lease_heartbeat_monitor_seconds = 0.01

        with patch(
            "lab.prism.prism_coordinator.os._exit",
            side_effect=AssertionError("unexpected hard exit"),
        ) as process_exit, patch.object(
            server,
            "_watchdog_hard_exit",
        ) as hard_exit:
            thread = server._start_ledger_lease_heartbeat()
            self.assertIsNotNone(thread)
            deadline = time.monotonic() + 2.0
            while ledger.verify_calls < 4 and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertGreaterEqual(ledger.verify_calls, 4)
            assert thread is not None
            self.assertTrue(thread.is_alive())
            self.assertTrue(server._stop_ledger_lease_heartbeat())
            thread.join(0.5)

        process_exit.assert_not_called()
        hard_exit.assert_not_called()

    def test_slow_but_advancing_first_heartbeat_does_not_startup_exit(
        self,
    ) -> None:
        """The startup fencing deadline keys on activity, not arming age.

        A first verification whose end-to-end duration exceeds the whole
        failure budget while every observable segment stays inside it
        (queue wait, then a statement round trip) must arm the heartbeat
        rather than hard-exit at startup. A genuinely wedged first beat
        still ages out because it stamps nothing.
        """

        class AdvancingFirstBeatLedger(RecordingLeaseLedger):
            writer_lease_fast_adoption_capable = True
            writer_lease_guard_required = True

            def __init__(self) -> None:
                super().__init__()
                self.saw_query_start = threading.Event()

            def verify_writer_lease_guard_session(
                self,
                *,
                on_query_start=None,
            ) -> dict[str, int | str]:
                time.sleep(0.10)
                if on_query_start is not None:
                    on_query_start()
                    self.saw_query_start.set()
                time.sleep(0.10)
                return {"backend": "recording", "verified_count": 1}

        ledger = AdvancingFirstBeatLedger()
        server = coordinator(ledger)
        server.ledger_lease_heartbeat_seconds = 0.05
        server.ledger_lease_heartbeat_failure_seconds = 0.16
        server.ledger_lease_heartbeat_monitor_seconds = 0.01

        with patch(
            "lab.prism.prism_coordinator.os._exit",
            side_effect=AssertionError("unexpected hard exit"),
        ) as process_exit, patch.object(
            server,
            "_watchdog_hard_exit",
        ) as hard_exit:
            thread = server._start_ledger_lease_heartbeat()
            self.assertIsNotNone(thread)
            self.assertTrue(ledger.saw_query_start.is_set())
            assert thread is not None
            self.assertTrue(server._stop_ledger_lease_heartbeat())
            thread.join(0.5)

        process_exit.assert_not_called()
        hard_exit.assert_not_called()

    def test_server_proven_cap_preserves_adoption_envelope_after_death(
        self,
    ) -> None:
        """Client-side marks must not stretch hard exit past adoption.

        A silent guard-session death leaves one attempt-start and one
        query-slot mark up to an idle interval after the death, so the
        plain activity budget alone would fire only at
        death + interval + budget — past a replacement's CAS eligibility
        when interval + budget reaches the adoption silence. The
        server-proven cap measures from completed round trips, which
        cannot postdate the death, and must fire first (cap 0.265s here
        versus interval + budget = 0.30s).
        """
        first_done = threading.Event()
        release_hang = threading.Event()
        calls: list[int] = []

        class DyingSessionLedger(RecordingLeaseLedger):
            writer_lease_fast_adoption_capable = True
            writer_lease_guard_required = True
            _lease_adoption_silence_seconds = 0.28

            def verify_writer_lease_guard_session(
                self,
                *,
                on_query_start=None,
            ) -> dict[str, int | str]:
                calls.append(len(calls))
                if on_query_start is not None:
                    on_query_start()
                if len(calls) == 1:
                    first_done.set()
                    return {"backend": "recording", "verified_count": 1}
                # The guard session died silently between beats: this
                # statement black-holes with no error and no response.
                release_hang.wait(5.0)
                return {"backend": "recording", "verified_count": 1}

        ledger = DyingSessionLedger()
        server = coordinator(ledger)
        server.ledger_lease_heartbeat_seconds = 0.05
        server.ledger_lease_heartbeat_failure_seconds = 0.25
        server.ledger_lease_heartbeat_monitor_seconds = 0.005
        server.ledger_lease_heartbeat_exit_timeout_seconds = 0.005
        thread: threading.Thread | None = None

        try:
            with patch("builtins.print"), patch.object(
                server,
                "_watchdog_hard_exit",
            ) as hard_exit:
                thread = server._start_ledger_lease_heartbeat()
                self.assertIsNotNone(thread)
                self.assertTrue(first_done.wait(0.5))
                deadline = time.monotonic() + 1.0
                while not hard_exit.called and time.monotonic() < deadline:
                    time.sleep(0.002)
                self.assertTrue(hard_exit.called)
            reason = str(
                getattr(server, "_ledger_lease_heartbeat_failure_reason", "")
            )
            match = re.search(
                r"progress for ([0-9.]+)s \(server-proven ([0-9.]+)s\)",
                reason,
            )
            self.assertIsNotNone(match, reason)
            assert match is not None
            activity_age = float(match.group(1))
            server_age = float(match.group(2))
            # The cap fired on stale server evidence while the client
            # marks were still inside the plain activity budget.
            self.assertLess(activity_age, 0.25)
            self.assertGreaterEqual(server_age, 0.26)
        finally:
            release_hang.set()
            heartbeat_stop = getattr(
                server,
                "_ledger_lease_heartbeat_stop_event",
                None,
            )
            if heartbeat_stop is not None:
                heartbeat_stop.set()
            if thread is not None:
                thread.join(0.5)

    def test_heartbeat_warning_honors_private_silence_attribute(self) -> None:
        class ShortSilenceLedger(GuardVerifyLeaseLedger):
            _lease_adoption_silence_seconds = 0.5

        ledger = ShortSilenceLedger()
        server = coordinator(ledger)
        server.ledger_lease_heartbeat_seconds = 0.01

        with patch("builtins.print") as printed:
            thread = server._start_ledger_lease_heartbeat()
            self.assertIsNotNone(thread)
            self.assertTrue(server._stop_ledger_lease_heartbeat())
        assert thread is not None
        thread.join(0.2)
        warnings = [
            call.args[0]
            for call in printed.call_args_list
            if call.args
            and "heartbeat timing is misconfigured" in str(call.args[0])
        ]
        # The default 0.75s budget leaves no envelope headroom under a
        # 0.5s operator silence override stored on the ledger's private
        # attribute; the warning must see it rather than the 1.0s default.
        self.assertEqual(len(warnings), 1)

    def test_heartbeat_timing_misconfiguration_warns_at_start(self) -> None:
        ledger = GuardVerifyLeaseLedger()
        server = coordinator(ledger)
        server.ledger_lease_heartbeat_seconds = 0.01
        server.ledger_lease_heartbeat_failure_seconds = 1.5

        with patch("builtins.print") as printed:
            thread = server._start_ledger_lease_heartbeat()
            self.assertIsNotNone(thread)
            self.assertTrue(server._stop_ledger_lease_heartbeat())
        assert thread is not None
        thread.join(0.2)
        warnings = [
            call.args[0]
            for call in printed.call_args_list
            if call.args
            and "heartbeat timing is misconfigured" in str(call.args[0])
        ]
        self.assertEqual(len(warnings), 1)

    def test_heartbeat_default_timing_does_not_warn(self) -> None:
        ledger = GuardVerifyLeaseLedger()
        server = coordinator(ledger)
        server.ledger_lease_heartbeat_seconds = 0.01

        with patch("builtins.print") as printed:
            thread = server._start_ledger_lease_heartbeat()
            self.assertIsNotNone(thread)
            self.assertTrue(server._stop_ledger_lease_heartbeat())
        assert thread is not None
        thread.join(0.2)
        for call in printed.call_args_list:
            if call.args:
                self.assertNotIn(
                    "heartbeat timing is misconfigured",
                    str(call.args[0]),
                )

    def test_external_fence_reports_recheck_progress_to_the_monitor(
        self,
    ) -> None:
        """The fence's verification feeds the monitor's progress mark too.

        The verification holds the guard's serialized slot for its whole
        statement sequence, so the periodic heartbeat queues behind it and
        records nothing meanwhile. Without this caller passing the
        progress callback, a fence verification's lawful second statement
        ages the heartbeat's last success past the single-statement
        failure budget and the monitor hard-exits a healthy coordinator —
        the hazard already closed for the heartbeat loop's own recheck.
        """

        class RecheckFenceLedger(RecordingLeaseLedger):
            writer_lease_fast_adoption_capable = True
            writer_lease_guard_required = True

            def __init__(self) -> None:
                super().__init__()
                self.saw_progress_callback = threading.Event()

            def verify_writer_lease_guard_session(
                self,
                *,
                on_query_start=None,
                on_statement_progress=None,
            ) -> dict[str, int | str]:
                if on_query_start is not None:
                    on_query_start()
                if on_statement_progress is not None:
                    # The attribution recheck's first statement completed.
                    on_statement_progress()
                    self.saw_progress_callback.set()
                return {
                    "backend": "recording",
                    "verified_count": 1,
                    "renewed_count": 1,
                }

        ledger = RecheckFenceLedger()
        server = coordinator(ledger)

        server._require_fresh_ledger_lease_for_external_side_effect(
            "submitblock"
        )

        self.assertTrue(ledger.saw_progress_callback.is_set())
        self.assertIsNotNone(
            getattr(
                server,
                "_ledger_lease_heartbeat_last_progress_monotonic",
                None,
            )
        )

    def test_recheck_statement_progress_keeps_monitor_from_hard_exiting(
        self,
    ) -> None:
        """A lawful two-statement verification outlasts the failure budget.

        The monitor's budget must stay under the adoption silence, so it is
        sized for one statement; the attribution recheck's second statement
        would age the last success past it (two in-deadline statements plus
        the heartbeat interval exceed the budget below). The completed
        first round trip is the same session-answers evidence a success
        proves, so its progress mark must keep the monitor from restarting
        a healthy coordinator, while a wedged statement — which reports no
        progress — still ages out (previous test).
        """
        first_statement_seconds = 0.2
        second_statement_seconds = 0.2

        class RecheckingLedger(HeartbeatLeaseLedger):
            def __init__(self) -> None:
                super().__init__()
                self.verify_calls = 0
                self.recheck_completed = threading.Event()

            def verify_writer_lease_guard_session(
                self,
                *,
                on_query_start=None,
                on_statement_progress=None,
            ) -> dict[str, int | str]:
                if on_query_start is not None:
                    on_query_start()
                self.verify_calls += 1
                if self.verify_calls == 1:
                    # Seed the success stamp with a fast verification.
                    return {"backend": "recording", "renewed_count": 1}
                # The ambiguous shape: both statements individually within
                # the guard's statement deadline, together past the
                # monitor's failure budget.
                time.sleep(first_statement_seconds)
                if on_statement_progress is not None:
                    on_statement_progress()
                time.sleep(second_statement_seconds)
                self.recheck_completed.set()
                return {"backend": "recording", "renewed_count": 1}

        ledger = RecheckingLedger()
        server = coordinator(ledger)
        server.ledger_lease_heartbeat_seconds = 0.01
        server.ledger_lease_heartbeat_failure_seconds = 0.3
        server.ledger_lease_heartbeat_monitor_seconds = 0.005
        thread: threading.Thread | None = None

        try:
            with patch(
                "lab.prism.prism_coordinator.os._exit",
                side_effect=AssertionError("unexpected hard exit"),
            ) as process_exit, patch.object(
                server,
                "_watchdog_hard_exit",
            ) as hard_exit:
                thread = server._start_ledger_lease_heartbeat()
                self.assertIsNotNone(thread)
                self.assertTrue(ledger.recheck_completed.wait(2))
                process_exit.assert_not_called()
                hard_exit.assert_not_called()
        finally:
            heartbeat_stop = getattr(
                server,
                "_ledger_lease_heartbeat_stop_event",
                None,
            )
            if heartbeat_stop is not None:
                heartbeat_stop.set()
            if thread is not None:
                thread.join(1)

    def test_watchdog_exit_releases_on_fresh_thread_within_deadline(self) -> None:
        ledger = WatchdogLeaseLedger()
        server = coordinator(ledger)
        server.watchdog_lease_release_timeout_seconds = 0.2
        watchdog_thread = threading.current_thread()

        started = time.monotonic()
        with patch("builtins.print"), patch(
            "lab.prism.prism_coordinator.os._exit",
            side_effect=SystemExit(1),
        ) as hard_exit, self.assertRaises(SystemExit):
            server._watchdog_hard_exit("liveness")
        elapsed = time.monotonic() - started

        hard_exit.assert_called_once_with(1)
        self.assertLess(elapsed, 0.2)
        self.assertEqual(ledger.release_calls, 1)
        self.assertIsNotNone(ledger.release_thread)
        self.assertIsNot(ledger.release_thread, watchdog_thread)
        self.assertEqual(ledger.release_thread.name, "prism-watchdog-lease-release")

    def test_all_watchdog_branches_arm_exit_without_blocking_output(self) -> None:
        cases = (
            ("coordination", ("coordination", 2.0, 1.0, 1.0), []),
            ("publication", ("publication", 0.0, 1.0, 1.0), []),
            ("liveness", (None, 0.0, 1.0, 1.0), ["block_submitter"]),
        )
        for expected_reason, publication_state, overdue in cases:
            with self.subTest(reason=expected_reason):
                server = coordinator()
                server.watchdog_interval_seconds = 0.0
                server.watchdog_timeout_seconds = 300.0
                server.watchdog_enabled = True
                server._publication_watchdog_state = (  # type: ignore[method-assign]
                    lambda _now, state=publication_state: state
                )
                server._overdue_heartbeats = (  # type: ignore[method-assign]
                    lambda _now, value=overdue: value
                )
                release_print = threading.Event()
                hard_exit_called = threading.Event()

                def blocking_print(*_args: object, **_kwargs: object) -> None:
                    release_print.wait(1)

                def hard_exit(reason: str) -> None:
                    self.assertEqual(reason, expected_reason)
                    hard_exit_called.set()
                    raise SystemExit(1)

                with patch(
                    "builtins.print",
                    side_effect=blocking_print,
                ) as print_call, patch.object(
                    server,
                    "_watchdog_hard_exit",
                    side_effect=hard_exit,
                ):
                    caller = threading.Thread(target=server.watchdog_loop)
                    caller.start()
                    reached_hard_exit = hard_exit_called.wait(0.1)
                    release_print.set()
                    caller.join(0.2)

                self.assertTrue(reached_hard_exit)
                self.assertFalse(caller.is_alive())
                print_call.assert_not_called()

    def test_watchdog_exit_hard_exits_when_fresh_db_release_hangs(self) -> None:
        release_started = threading.Event()
        unblock_release = threading.Event()
        release_finished = threading.Event()

        class BlockingLedger(RecordingLeaseLedger):
            def release_writer_lease_fresh_connection(self) -> bool:
                release_started.set()
                unblock_release.wait(1)
                release_finished.set()
                return True

        server = coordinator(BlockingLedger())
        server.watchdog_lease_release_timeout_seconds = 0.02

        started = time.monotonic()
        try:
            with patch("builtins.print"), patch(
                "lab.prism.prism_coordinator.os._exit",
                side_effect=SystemExit(1),
            ) as hard_exit, self.assertRaises(SystemExit):
                server._watchdog_hard_exit("publication")
        finally:
            unblock_release.set()
        elapsed = time.monotonic() - started

        hard_exit.assert_called_once_with(1)
        self.assertTrue(release_started.is_set())
        self.assertLess(elapsed, 0.2)
        self.assertTrue(release_finished.wait(0.2))

    def test_watchdog_release_never_waits_on_shutdown_logging(self) -> None:
        ledger = WatchdogLeaseLedger()
        server = coordinator(ledger)
        server.watchdog_lease_release_timeout_seconds = 0.05
        release_log = threading.Event()

        def blocking_log(*_args: object, **_kwargs: object) -> None:
            release_log.wait(1)

        try:
            with patch.object(
                server,
                "_shutdown_log",
                side_effect=blocking_log,
            ) as shutdown_log, patch("builtins.print"), patch(
                "lab.prism.prism_coordinator.os._exit",
                side_effect=SystemExit(1),
            ), self.assertRaises(SystemExit):
                server._watchdog_hard_exit("liveness")
            released_before_log_unblocked = ledger.released.is_set()
        finally:
            release_log.set()

        self.assertTrue(released_before_log_unblocked)
        shutdown_log.assert_not_called()

    def test_watchdog_blocked_diagnostic_cannot_extend_exit_deadline(self) -> None:
        ledger = WatchdogLeaseLedger()
        server = coordinator(ledger)
        server.watchdog_lease_release_timeout_seconds = 0.02
        diagnostic_started = threading.Event()
        release_diagnostic = threading.Event()

        def blocking_print(*_args: object, **_kwargs: object) -> None:
            diagnostic_started.set()
            release_diagnostic.wait(1)

        started = time.monotonic()
        try:
            with patch("builtins.print", side_effect=blocking_print), patch(
                "lab.prism.prism_coordinator.os._exit",
                side_effect=SystemExit(1),
            ), self.assertRaises(SystemExit):
                server._watchdog_hard_exit("liveness")
            elapsed = time.monotonic() - started
        finally:
            release_diagnostic.set()
            if ledger.release_thread is not None:
                ledger.release_thread.join(0.2)

        self.assertTrue(ledger.released.is_set())
        self.assertTrue(diagnostic_started.is_set())
        self.assertLess(elapsed, 0.2)

    def test_watchdog_exit_hard_exits_when_release_thread_cannot_start(self) -> None:
        server = coordinator()

        with patch(
            "lab.prism.prism_coordinator.threading.Thread",
            side_effect=RuntimeError("cannot start new thread"),
        ), patch(
            "lab.prism.prism_coordinator.os._exit",
            side_effect=SystemExit(1),
        ) as hard_exit, self.assertRaises(SystemExit):
            server._watchdog_hard_exit("liveness")

        hard_exit.assert_called_once_with(1)

    def test_watchdog_exit_withholds_release_while_writer_is_active(self) -> None:
        ledger = WatchdogLeaseLedger()
        server = coordinator(ledger)
        server.watchdog_lease_release_timeout_seconds = 0.02
        controller = server._ensure_shutdown_controller()
        active_writer = controller.reserve_writer("block_submitter")

        try:
            with patch("builtins.print"), patch(
                "lab.prism.prism_coordinator.os._exit",
                side_effect=SystemExit(1),
            ), self.assertRaises(SystemExit):
                server._watchdog_hard_exit("liveness")
            deadline = time.monotonic() + 0.2
            while (
                not controller.snapshot()["lease_release_withheld"]
                and time.monotonic() < deadline
            ):
                time.sleep(0.001)
            self.assertTrue(controller.snapshot()["lease_release_withheld"])
        finally:
            active_writer.finish()

        self.assertEqual(ledger.release_calls, 0)

    def test_withheld_shutdown_keeps_lease_heartbeat_alive(self) -> None:
        ledger = HeartbeatLeaseLedger()
        server = coordinator(ledger, timeout=0.02)
        server.ledger_lease_heartbeat_seconds = 0.005
        heartbeat_thread = server._start_ledger_lease_heartbeat()
        self.assertIsNotNone(heartbeat_thread)
        controller = server._ensure_shutdown_controller()
        active_writer = controller.reserve_writer("accepted_block_handling")
        renewals_before_shutdown = ledger.renew_calls

        try:
            with patch("builtins.print"):
                self.assertFalse(server.shutdown(reason="active_writer"))
            deadline = time.monotonic() + 0.1
            while (
                ledger.renew_calls <= renewals_before_shutdown
                and time.monotonic() < deadline
            ):
                time.sleep(0.001)

            self.assertGreater(ledger.renew_calls, renewals_before_shutdown)
            assert heartbeat_thread is not None
            self.assertTrue(heartbeat_thread.is_alive())
            self.assertEqual(ledger.release_calls, 0)
        finally:
            active_writer.finish()
            self.assertTrue(server._stop_ledger_lease_heartbeat())

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

    def test_safe_release_stops_lease_heartbeat_before_database_handoff(self) -> None:
        release_saw_heartbeat_alive: list[bool] = []

        class OrderedHeartbeatLedger(HeartbeatLeaseLedger):
            def release_writer_lease(self) -> bool:
                heartbeat = getattr(server, "_ledger_lease_heartbeat_thread", None)
                release_saw_heartbeat_alive.append(
                    bool(heartbeat is not None and heartbeat.is_alive())
                )
                return super().release_writer_lease()

        ledger = OrderedHeartbeatLedger()
        server = coordinator(ledger)
        self.assertIsNotNone(server._start_ledger_lease_heartbeat())

        with patch("builtins.print"):
            self.assertTrue(server.shutdown(reason="ordered_release"))

        self.assertEqual(release_saw_heartbeat_alive, [False])
        self.assertEqual(ledger.release_calls, 1)

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
            pending_share=SimpleNamespace(
                job_issued_at_ms=0,
                accepted_at_ms=0,
            ),
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
            pending_share=SimpleNamespace(
                job_issued_at_ms=0,
                accepted_at_ms=0,
            ),
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

        self.assertFalse(producer_thread.is_alive(), "producer thread leaked")
        self.assertFalse(writer_thread.is_alive(), "share writer thread leaked")
        self.assertFalse(shutdown_thread.is_alive(), "shutdown thread leaked")
        self.assertTrue(entry.committed.is_set())
        self.assertEqual(ledger.release_calls, 1)

    def test_event_only_stop_cannot_strand_late_admitted_share(self) -> None:
        class Ledger(RecordingLeaseLedger):
            def append_batch(self, entries: object) -> list[object]:
                return [SimpleNamespace(share_seq=1)]

        server = coordinator(Ledger())
        server.share_append_queue = queue.Queue(maxsize=2)
        server.share_commit_batch_size = 1
        server.share_commit_linger_seconds = 0
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
        entry = PendingShareAppend(
            pending_share=SimpleNamespace(
                job_issued_at_ms=0,
                accepted_at_ms=0,
            ),
            username="miner-a",
            job_id="job-a",
            block_hash_hex="aa" * 32,
            collection_only=False,
            credit_policy=None,
        )

        # Model a request that passed admission immediately before a legacy
        # event-only stop. The writer must remain available until admission is
        # actually closed, so the share cannot be stranded behind its exit.
        server.stop_event.set()
        writer_thread = threading.Thread(target=server.share_append_loop)
        writer_thread.start()
        time.sleep(0.25)
        self.assertTrue(writer_thread.is_alive())

        server.enqueue_share_append(entry)
        self.assertTrue(entry.committed.wait(1))
        server.request_shutdown(signal.SIGTERM)
        writer_thread.join(1)

        self.assertFalse(writer_thread.is_alive())
        self.assertIsNotNone(entry.record)

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
        controller = server._ensure_shutdown_controller()
        self.assertTrue(controller.claim_non_writer_drain())
        controller.finish_non_writer_drain(0.0)
        with patch("builtins.print"):
            self.assertFalse(server.shutdown(reason="finally"))
            self.assertFalse(server.release_ledger_lease())
        self.assertEqual(ledger.release_calls, 0)
        self.assertTrue(controller.snapshot()["lease_release_withheld"])

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

    def test_submit_admission_race_returns_pool_closed_stratum_error(self) -> None:
        server = coordinator()

        def rejected_submit(_client: object, _params: object) -> bool:
            server.request_shutdown(signal.SIGTERM)
            raise ShutdownInProgress("PRISM coordinator is shutting down")

        server.handle_submit = rejected_submit  # type: ignore[method-assign]
        with self.assertRaises(StratumError) as raised:
            server.handle_request(
                SimpleNamespace(),
                {"id": 1, "method": "mining.submit", "params": []},
            )

        self.assertEqual(raised.exception.code, 20)
        self.assertEqual(raised.exception.reason, PRISM_REJECTION_POOL_CLOSED)
        self.assertTrue(raised.exception.disconnect)

    def test_block_submit_loop_exits_if_shutdown_wins_admission_race(self) -> None:
        server = coordinator()
        submit_calls: list[bool] = []
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]

        def rejected_replay() -> int:
            server.request_shutdown(signal.SIGTERM)
            raise ShutdownInProgress("PRISM coordinator is shutting down")

        server.replay_pending_block_candidates = rejected_replay  # type: ignore[method-assign]
        server.submit_next_block_candidate = (  # type: ignore[method-assign]
            lambda **_kwargs: submit_calls.append(True) or False
        )

        server.block_submit_loop()

        self.assertEqual(submit_calls, [])

    def test_blockpoll_shutdown_race_does_not_take_hard_exit_path(self) -> None:
        server = coordinator()
        server.blockpoll_seconds = 0
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]

        def rejected_poll() -> int:
            server.request_shutdown(signal.SIGTERM)
            raise ShutdownInProgress("PRISM coordinator is shutting down")

        server.poll_qbit_tip_template_once = rejected_poll  # type: ignore[method-assign]
        with patch("lab.prism.prism_coordinator.os._exit") as hard_exit:
            server.blockpoll_loop()

        hard_exit.assert_not_called()

    def test_ctv_loop_exits_cleanly_if_shutdown_wins_admission_race(self) -> None:
        server = coordinator()
        pass_observations: list[float] = []
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
        server.observe_ctv_fanout_broadcaster_pass = (  # type: ignore[method-assign]
            pass_observations.append
        )

        def rejected_pass(**_kwargs: object) -> object:
            server.request_shutdown(signal.SIGTERM)
            raise ShutdownInProgress("PRISM coordinator is shutting down")

        server.run_ctv_fanout_broadcaster_once = rejected_pass  # type: ignore[method-assign]
        with patch("builtins.print") as printed, patch(
            "lab.prism.prism_coordinator.traceback.print_exc"
        ) as traceback_printed:
            server.ctv_fanout_broadcaster_loop()

        self.assertEqual(pass_observations, [])
        printed.assert_not_called()
        traceback_printed.assert_not_called()

    def test_startup_replays_stop_cleanly_after_shutdown_admission_closes(self) -> None:
        server = coordinator()
        server.request_shutdown(signal.SIGTERM)

        for replay in (
            server.replay_pending_block_candidates,
            server.replay_recovered_shares,
        ):
            with self.subTest(replay=replay.__name__):
                self.assertFalse(server._run_startup_writer_replay(replay))

        drain_threads = [(threading.current_thread(), 0.0)]
        timeline: list[object] = []
        server.shutdown = (  # type: ignore[method-assign]
            lambda *, reason: timeline.append(("shutdown", reason)) or True
        )
        server.drain_non_writer_components = (  # type: ignore[method-assign]
            lambda threads: timeline.append(("drain", threads))
        )
        self.assertFalse(
            server._run_startup_writer_replay(
                server.replay_recovered_shares,
                drain_threads=drain_threads,
            )
        )
        self.assertEqual(
            timeline,
            [
                ("shutdown", "serve_startup_exit"),
                ("drain", drain_threads),
            ],
        )

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
