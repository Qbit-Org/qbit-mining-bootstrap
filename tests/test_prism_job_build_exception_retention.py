#!/usr/bin/env python3
"""Cancelled job builds must release producer and waiter references.

A build that observes supersession or timeout raises from inside the executor
task. The stored error's traceback reached the request through the finished
producer frames (and, on Python 3.14, through the executor context frame that
still owned the task's argument tuple), and every waiter that re-raised the
stored instance appended its own frame owning the request and promise. Both
halves formed request -> promise -> error -> traceback -> request cycles that
only cyclic GC reclaimed.

These regressions drive the real coordinator facade, scheduler, executor
callback, builder checkpoint, and waiter loop with automatic collection
disabled, and assert release by reference counting alone. Threads are
synchronized with events; nothing inspects frame locals or referrers.
"""

from __future__ import annotations

import gc
import threading
import time
import unittest
import weakref
from concurrent.futures import Future
from typing import Callable

from lab.prism.job_bundle import (
    JobBuildCancelled,
    JobBuildSuperseded,
    _await_job_build_promise,
)
from tests.prism_coordinator_test_support import (
    FakeLedger,
    coordinator,
    install_fake_bundle_builder,
)

# The builder checkpoints ledger conversion every 256 rows. Cancelling while
# row CANCEL_ROW converts lets the next checkpoint raise from the real builder
# frame while the ledger snapshot and the converted rows are still live.
WINDOW_ROWS = 1024
CANCEL_ROW = 300
WAIT_SECONDS = 20.0
WAITER_FRAME = "_shared_job_bundle_after_priority_admission"
PRODUCER_FRAMES = (
    "_execute_job_build_request",
    "build_shared_job_bundle",
    "raise_if_cancelled",
)


def wait_until(predicate: Callable[[], bool], *, timeout: float = WAIT_SECONDS) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    raise AssertionError("condition did not become true before the deadline")


def traceback_names(error: BaseException | None) -> list[str]:
    """Function names along a traceback, retaining no frame."""

    names: list[str] = []
    entry = error.__traceback__ if error is not None else None
    while entry is not None:
        names.append(entry.tb_frame.f_code.co_name)
        entry = entry.tb_next
    return names


def describe(error: BaseException | None) -> tuple[type, tuple, list[str]] | None:
    if error is None:
        return None
    return type(error), error.args, traceback_names(error)


class ShareRow(dict):
    """dict subclass so a converted share row is weakref-able."""


class Snapshot(list):
    """list subclass so a ledger snapshot is weakref-able."""


class LedgerRecord:
    def __init__(self, ledger: "WindowLedger", snapshot_index: int, index: int) -> None:
        self.ledger = ledger
        self.snapshot_index = snapshot_index
        self.index = index
        self.payload = {
            "miner_id": f"miner-{index % 3}",
            "share_seq": index + 1,
            "blob": "x" * 64,
        }

    def to_prism_json(self) -> ShareRow:
        self.ledger.on_convert(self.snapshot_index, self.index)
        row = ShareRow(self.payload)
        if self.index == 0:
            self.ledger.first_rows.append(weakref.ref(row))
        return row


class WindowLedger(FakeLedger):
    """A ledger whose snapshot rows call back into the test as they convert."""

    def __init__(self) -> None:
        super().__init__(miners=["miner-0", "miner-1", "miner-2"])
        self.on_convert: Callable[[int, int], None] = lambda snapshot_index, index: None
        self.snapshots: list[weakref.ref] = []
        self.first_rows: list[weakref.ref] = []

    def accepted_share_stats(self) -> dict[str, int]:
        self.stats_calls += 1
        return {"accepted_share_count": WINDOW_ROWS, "distinct_miner_count": 3}

    def snapshot_at_job_issue(
        self,
        anchor_job_issued_at_ms: int,
        *,
        window_weight: int | None = None,
    ) -> Snapshot:
        self.snapshot_calls += 1
        snapshot_index = self.snapshot_calls
        snapshot = Snapshot(
            LedgerRecord(self, snapshot_index, index) for index in range(WINDOW_ROWS)
        )
        self.snapshots.append(weakref.ref(snapshot))
        return snapshot


class _Payload:
    pass


class JobBuildExceptionRetentionTests(unittest.TestCase):
    def setUp(self) -> None:
        gc.collect()
        self.gc_enabled = gc.isenabled()
        gc.disable()
        self.ledger = WindowLedger()
        self.server, rpc = coordinator(ledger=self.ledger)
        install_fake_bundle_builder(self.server)
        self.service = self.server._ensure_job_bundle_service()
        self.artifacts = self.server.store_template_artifacts(dict(rpc.template))
        self.assertIsNotNone(self.artifacts)
        self.requests: list[weakref.ref] = []
        self.promises: list[weakref.ref] = []
        self.flight_promise: weakref.ref | None = None
        original_new_request = self.server._new_job_build_request

        def observe_new_request(*args: object, **kwargs: object) -> object:
            request = original_new_request(*args, **kwargs)  # type: ignore[arg-type]
            self.requests.append(weakref.ref(request))
            self.promises.append(weakref.ref(request.promise))
            return request

        self.server._new_job_build_request = observe_new_request  # type: ignore[method-assign]

    def tearDown(self) -> None:
        self.server.shutdown_job_build_executor()
        if self.gc_enabled:
            gc.enable()
        gc.collect()

    # -- harness -----------------------------------------------------------

    def hold_first_conversion(
        self,
        after: Callable[[int], None] | None = None,
    ) -> tuple[threading.Event, threading.Event]:
        """Pause the first snapshot's conversion at CANCEL_ROW until released."""

        reached = threading.Event()
        proceed = threading.Event()

        def on_convert(snapshot_index: int, index: int) -> None:
            if snapshot_index == 1 and index == CANCEL_ROW:
                reached.set()
                if not proceed.wait(WAIT_SECONDS):
                    raise TimeoutError("test never released the paused build")
                if after is not None:
                    after(index)

        self.ledger.on_convert = on_convert
        return reached, proceed

    def run_waiters(
        self,
        count: int,
        *,
        retry_superseded: bool = False,
    ) -> tuple[list[threading.Thread], list[dict[str, object]]]:
        """Run real bundle waiters, recording only plain data about outcomes."""

        outcomes: list[dict[str, object]] = [{} for _ in range(count)]

        def stored_error() -> BaseException | None:
            reference = self.flight_promise or self.promises[0]
            promise = reference()
            return None if promise is None else promise.exception(timeout=WAIT_SECONDS)

        def wait(slot: int) -> None:
            outcome = outcomes[slot]
            try:
                self.server.shared_job_bundle(
                    self.artifacts,
                    mode="ready",
                    retry_superseded=retry_superseded,
                )
            except JobBuildCancelled as error:
                stored = stored_error()
                outcome.update(
                    kind="cancelled",
                    type=type(error),
                    args=error.args,
                    names=traceback_names(error),
                    notes=list(getattr(error, "__notes__", [])),
                    attributes={
                        key: value
                        for key, value in vars(error).items()
                        if key != "__notes__"
                    },
                    cause=describe(error.__cause__),
                    context=describe(error.__context__),
                    suppress_context=error.__suppress_context__,
                    stored_is_error=stored is error,
                    stored_names=traceback_names(stored),
                )
            except BaseException as error:  # noqa: BLE001 - asserted below
                stored = stored_error()
                outcome.update(
                    kind="unexpected",
                    type=type(error),
                    args=error.args,
                    stored_is_error=stored is error,
                )
            else:
                outcome.update(kind="bundle")

        threads = [
            threading.Thread(target=wait, args=(slot,), name=f"bundle-waiter-{slot}")
            for slot in range(count)
        ]
        for thread in threads:
            thread.start()
        return threads, outcomes

    def await_joined_flight(self, waiters: int) -> None:
        """Every waiter must have joined the paused flight before it is cancelled."""

        wait_until(
            lambda: self.service.job_build_scheduler_counts["requests"] == waiters
        )
        with self.service._job_build_scheduler_lock:
            flight = self.service._job_build_active
            self.assertIsNotNone(flight)
            self.flight_promise = weakref.ref(flight.request.promise)
        self.assertEqual(self.ledger.snapshot_calls, 1)

    def cancel_active_flight(self, reason: str) -> None:
        with self.service._job_build_scheduler_lock:
            flight = self.service._job_build_active
            self.assertIsNotNone(flight)
            self.assertTrue(self.service._cancel_job_build_flight_locked(flight, reason))
            self.assertEqual(flight.request.cancellation.reason, reason)

    def join_all(self, threads: list[threading.Thread]) -> None:
        for thread in threads:
            thread.join(WAIT_SECONDS)
            self.assertFalse(thread.is_alive(), f"{thread.name} did not finish")
        # Joining the executor thread drops the finished work item and the
        # task argument tuple it still holds between completion and exit.
        self.server.shutdown_job_build_executor()

    def tracked_references(self) -> dict[str, weakref.ref]:
        references: dict[str, weakref.ref] = {}
        for index, reference in enumerate(self.requests):
            references[f"request[{index}]"] = reference
        for index, reference in enumerate(self.promises):
            references[f"promise[{index}]"] = reference
        for index, reference in enumerate(self.ledger.snapshots):
            references[f"snapshot[{index}]"] = reference
        for index, reference in enumerate(self.ledger.first_rows):
            references[f"first_row[{index}]"] = reference
        return references

    def assert_released(self, references: dict[str, weakref.ref]) -> None:
        self.assertFalse(gc.isenabled())
        self.assertEqual(
            {name: reference() is not None for name, reference in references.items()},
            dict.fromkeys(references, False),
        )

    def assert_cancelled_outcome(
        self,
        outcome: dict[str, object],
        error_type: type[JobBuildCancelled],
        message_prefix: str,
    ) -> None:
        self.assertEqual(outcome["kind"], "cancelled", outcome)
        self.assertIs(outcome["type"], error_type)
        args = outcome["args"]
        assert isinstance(args, tuple)
        self.assertEqual(len(args), 1)
        self.assertTrue(str(args[0]).startswith(message_prefix), args)
        names = outcome["names"]
        assert isinstance(names, list)
        self.assertIn(WAITER_FRAME, names)
        self.assertFalse(outcome["stored_is_error"])
        stored_names = outcome["stored_names"]
        assert isinstance(stored_names, list)
        self.assertNotIn(WAITER_FRAME, stored_names)
        self.assertIn("_execute_job_build_request", stored_names)

    # -- supersession and timeout through the real checkpoint --------------

    def check_cancelled_flight_releases_for_every_waiter(
        self,
        reason: str,
        error_type: type[JobBuildCancelled],
        message_prefix: str,
    ) -> None:
        reached, proceed = self.hold_first_conversion()
        threads, outcomes = self.run_waiters(3)
        self.assertTrue(reached.wait(WAIT_SECONDS))
        self.await_joined_flight(3)
        self.cancel_active_flight(reason)
        proceed.set()
        self.join_all(threads)
        for outcome in outcomes:
            self.assert_cancelled_outcome(outcome, error_type, message_prefix)
            names = outcome["names"]
            assert isinstance(names, list)
            for producer_frame in PRODUCER_FRAMES:
                self.assertIn(producer_frame, names)
            self.assertEqual(outcome["cause"], None)
            self.assertEqual(outcome["context"], None)
        self.assertEqual(len(self.requests), 3)
        self.assertEqual(self.ledger.snapshot_calls, 1)
        # Scheduler accounting is unchanged: supersession is an obsolete
        # result, a timeout is a failed build.
        superseded = reason != "timeout"
        self.assertEqual(
            self.service.job_build_scheduler_counts["obsolete_results"],
            1 if superseded else 0,
        )
        self.assertEqual(
            self.service.shared_bundle_build_counts["superseded"],
            1 if superseded else 0,
        )
        self.assertEqual(
            self.service.shared_bundle_build_counts["failed"],
            0 if superseded else 1,
        )
        self.assert_released(self.tracked_references())

    def test_superseded_flight_releases_window_for_every_waiter(self) -> None:
        self.check_cancelled_flight_releases_for_every_waiter(
            "superseded",
            JobBuildSuperseded,
            "job build superseded at ledger_snapshot_conversion",
        )

    def test_timed_out_flight_releases_window_for_every_waiter(self) -> None:
        self.check_cancelled_flight_releases_for_every_waiter(
            "timeout",
            JobBuildCancelled,
            "job build timeout at ledger_snapshot_conversion",
        )

    def test_in_frame_retry_releases_the_superseded_build(self) -> None:
        reached, proceed = self.hold_first_conversion()
        threads, outcomes = self.run_waiters(1, retry_superseded=True)
        self.assertTrue(reached.wait(WAIT_SECONDS))
        self.await_joined_flight(1)
        self.cancel_active_flight("superseded")
        proceed.set()
        self.join_all(threads)
        self.assertEqual(outcomes[0], {"kind": "bundle"})
        self.assertEqual(self.ledger.snapshot_calls, 2)
        self.assertEqual(len(self.requests), 2)
        # The retry's cached bundle legitimately keeps its converted rows;
        # everything the superseded build owned must be gone.
        self.assert_released(
            {
                "superseded_request": self.requests[0],
                "superseded_promise": self.promises[0],
                "superseded_snapshot": self.ledger.snapshots[0],
                "superseded_first_row": self.ledger.first_rows[0],
                "retry_request": self.requests[1],
                "retry_promise": self.promises[1],
                "retry_snapshot": self.ledger.snapshots[1],
            }
        )

    # -- orphan sweep resolves under the scheduler lock --------------------

    def test_orphan_sweep_resolves_wedged_flight_and_releases_window(self) -> None:
        self.server.job_build_orphan_sweep_grace_seconds = 0.0  # type: ignore[attr-defined]
        flights: list[weakref.ref] = []

        def never_arm(flight: object) -> None:
            # A done callback that died before resolving leaves the flight
            # wedged in its slot; the admission sweep must settle it.
            flights.append(weakref.ref(flight))

        self.server._arm_job_build_locked = never_arm  # type: ignore[method-assign]
        reached, proceed = self.hold_first_conversion()
        threads, outcomes = self.run_waiters(2)
        self.assertTrue(reached.wait(WAIT_SECONDS))
        self.await_joined_flight(2)
        self.cancel_active_flight("superseded")
        proceed.set()
        wait_until(lambda: flights[0]().future.done())
        with self.service._job_build_scheduler_lock:
            self.assertEqual(self.service._evict_orphaned_job_build_flights_locked(), [])
            logs = self.service._evict_orphaned_job_build_flights_locked()
        self.assertEqual(len(logs), 1)
        self.assertIn("slot=active", logs[0])
        self.assertIn("cancelled=True", logs[0])
        self.assertEqual(self.service.job_build_scheduler_counts["orphan_evicted"], 1)
        self.join_all(threads)
        for outcome in outcomes:
            self.assert_cancelled_outcome(
                outcome,
                JobBuildSuperseded,
                "job build superseded at ledger_snapshot_conversion",
            )
        references = self.tracked_references()
        references["flight"] = flights[0]
        self.assert_released(references)

    # -- chained producer failures reported as supersession -----------------

    def check_chained_cancellation_releases_nested_frames(self, chaining: str) -> None:
        def reject_row(index: int) -> None:
            raise ValueError(f"ledger row {index} rejected")

        reached, proceed = self.hold_first_conversion(after=reject_row)
        coordinator_entry = self.server._execute_job_build_request

        def _execute_job_build_request(request: object) -> object:
            # Stand-in for an executor entry that reports a nested producer
            # failure as supersession, as the bundle compiler does with a
            # subprocess failure. Like the real entry, its frame owns the
            # request while the nested frames own the payout window.
            try:
                return coordinator_entry(request)  # type: ignore[arg-type]
            except ValueError as failure:
                error = JobBuildSuperseded(
                    "job build superseded at ledger_snapshot_conversion; row rejected"
                )
                error.add_note("rejected row converts on the next build")
                error.phase = "ledger_snapshot_conversion"  # type: ignore[attr-defined]
                if chaining == "cause":
                    raise error from failure
                if chaining == "suppressed":
                    raise error from None
                raise error

        self.server._execute_job_build_request = _execute_job_build_request  # type: ignore[method-assign]
        threads, outcomes = self.run_waiters(2)
        self.assertTrue(reached.wait(WAIT_SECONDS))
        self.await_joined_flight(2)
        proceed.set()
        self.join_all(threads)
        failure = (ValueError, (f"ledger row {CANCEL_ROW} rejected",))
        for outcome in outcomes:
            self.assert_cancelled_outcome(
                outcome,
                JobBuildSuperseded,
                "job build superseded at ledger_snapshot_conversion; row rejected",
            )
            self.assertEqual(outcome["notes"], ["rejected row converts on the next build"])
            self.assertEqual(outcome["attributes"], {"phase": "ledger_snapshot_conversion"})
            cause = outcome["cause"]
            context = outcome["context"]
            if chaining == "cause":
                self.assertTrue(outcome["suppress_context"])
                self.assertIsNotNone(cause)
                chained = cause
            else:
                self.assertIsNone(cause)
                self.assertEqual(outcome["suppress_context"], chaining == "suppressed")
                chained = context
            assert chained is not None
            self.assertEqual(chained[:2], failure)
            for producer_frame in ("_execute_job_build_request", "build_shared_job_bundle", "to_prism_json"):
                self.assertIn(producer_frame, chained[2])
        self.assertEqual(len(self.requests), 2)
        self.assertEqual(self.ledger.snapshot_calls, 1)
        self.assert_released(self.tracked_references())

    def test_cancellation_with_cause_releases_nested_producer_frames(self) -> None:
        self.check_chained_cancellation_releases_nested_frames("cause")

    def test_cancellation_with_context_releases_nested_producer_frames(self) -> None:
        self.check_chained_cancellation_releases_nested_frames("context")

    def test_cancellation_with_suppressed_context_releases_nested_frames(self) -> None:
        self.check_chained_cancellation_releases_nested_frames("suppressed")

    # -- unexpected failures keep their behavior ----------------------------

    def test_unexpected_build_failure_still_raises_the_stored_instance(self) -> None:
        def reject_row(index: int) -> None:
            raise ValueError(f"ledger row {index} rejected")

        reached, proceed = self.hold_first_conversion(after=reject_row)
        threads, outcomes = self.run_waiters(1)
        self.assertTrue(reached.wait(WAIT_SECONDS))
        self.await_joined_flight(1)
        proceed.set()
        self.join_all(threads)
        self.assertEqual(outcomes[0]["kind"], "unexpected")
        self.assertIs(outcomes[0]["type"], ValueError)
        self.assertEqual(outcomes[0]["args"], (f"ledger row {CANCEL_ROW} rejected",))
        self.assertTrue(outcomes[0]["stored_is_error"])
        self.assertEqual(self.service.shared_bundle_build_counts["failed"], 1)
        # An unexpected failure keeps its full traceback for post-mortem
        # inspection, so only collection reclaims that build.
        gc.collect()
        self.assert_released(self.tracked_references())


class WaiterCopyTests(unittest.TestCase):
    def setUp(self) -> None:
        gc.collect()
        self.gc_enabled = gc.isenabled()
        gc.disable()

    def tearDown(self) -> None:
        if self.gc_enabled:
            gc.enable()
        gc.collect()

    def test_waiter_copy_preserves_metadata_and_leaves_stored_traceback_alone(self) -> None:
        payload = _Payload()
        promise: Future[object] = Future()
        try:
            raise ValueError("compiler exited 137")
        except ValueError as failure:
            stored = JobBuildSuperseded("job build superseded at bundle_assembly")
            stored.add_note("compiler exit 137")
            stored.phase = "bundle_assembly"  # type: ignore[attr-defined]
            stored.__cause__ = failure
            stored.__suppress_context__ = True
        promise.set_exception(stored)
        references = {"payload": weakref.ref(payload), "promise": weakref.ref(promise)}

        def waiter(owned_payload: _Payload, owned_promise: Future[object]) -> None:
            _await_job_build_promise(owned_promise, 1.0)

        try:
            waiter(payload, promise)
        except JobBuildSuperseded as error:
            self.assertIsNot(error, stored)
            self.assertIs(type(error), JobBuildSuperseded)
            self.assertEqual(error.args, stored.args)
            self.assertEqual(error.__notes__, ["compiler exit 137"])  # type: ignore[attr-defined]
            self.assertEqual(error.phase, "bundle_assembly")  # type: ignore[attr-defined]
            self.assertIs(error.__cause__, stored.__cause__)
            self.assertIs(error.__context__, stored.__context__)
            self.assertTrue(error.__suppress_context__)
            self.assertEqual(
                traceback_names(error)[-2:],
                ["waiter", "_await_job_build_promise"],
            )
        else:
            self.fail("waiter did not observe the cancellation")
        # The stored instance was never raised: no waiter frame reached it.
        self.assertIsNone(stored.__traceback__)
        self.assertIs(promise.exception(), stored)
        del stored, payload, promise
        self.assertFalse(gc.isenabled())
        self.assertEqual(
            {name: reference() is not None for name, reference in references.items()},
            dict.fromkeys(references, False),
        )

    def test_waiter_keeps_future_result_semantics_for_other_outcomes(self) -> None:
        pending: Future[object] = Future()
        with self.assertRaises(TimeoutError):
            _await_job_build_promise(pending, 0.001)
        completed: Future[object] = Future()
        result = _Payload()
        completed.set_result(result)
        self.assertIs(_await_job_build_promise(completed, 1.0), result)
        failed: Future[object] = Future()
        failure = RuntimeError("build failed")
        failed.set_exception(failure)
        try:
            _await_job_build_promise(failed, 1.0)
        except RuntimeError as error:
            self.assertIs(error, failure)
        else:
            self.fail("unexpected failure was not re-raised")


if __name__ == "__main__":
    unittest.main()
